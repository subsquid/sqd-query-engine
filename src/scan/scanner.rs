use super::pairs::{pack16, PairSet};
use crate::engine_err;
use crate::error::ErrorKind;
use crate::integers::IntColumn;
use crate::scan::chunk::ParquetTable;
use crate::scan::predicate::RowPredicate;
use crate::scan::rows::{Scanned, ScannedBatch};
use crate::text::StringColumn;
use anyhow::{Context, Result};
use arrow::array::builder::BooleanBufferBuilder;
use arrow::array::*;
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::boolean::and;
use arrow::compute::kernels::cmp::{gt_eq, lt_eq};
use arrow::datatypes::{DataType, Schema, SchemaRef, UInt64Type};
use arrow::error::ArrowError;
use arrow::row::{RowConverter, SortField};
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
use parquet::arrow::ProjectionMask;
use parquet::basic::Encoding;
use parquet::file::metadata::ColumnChunkMetaData;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet as HashSet};
use std::sync::Arc;

/// A scan request: which columns to read, what predicates to apply.
#[derive(Clone)]
pub struct ScanRequest<'a> {
    /// Columns to include in the output.
    pub output_columns: Vec<&'a str>,
    /// Row predicates (multiple items ORed together).
    pub predicates: Vec<&'a RowPredicate>,
    /// Block range filter: only include rows where block_number >= from_block.
    pub from_block: Option<u64>,
    /// Block range filter: only include rows where block_number <= to_block.
    pub to_block: Option<u64>,
    /// The column name that holds the block number (for block range filtering).
    pub block_number_column: Option<&'a str>,
    /// Max rows per batch during reading.
    pub batch_size: usize,
    /// Optional key filter for join pushdown (relation scans only).
    pub key_filter: Option<&'a KeyFilter>,
    /// Optional hierarchical filter for Children/Parents relations.
    pub hierarchical_filter: Option<&'a HierarchicalFilter>,
    /// Columns that the user explicitly requested and which MUST exist in the
    /// parquet file. A missing one is a hard error (matches legacy
    /// `ColumnDoesNotExist`), as opposed to engine-internal columns that are
    /// tolerated when absent.
    pub required_columns: Vec<&'a str>,
    /// Record each returned row's absolute physical position. Only readers
    /// that support row positions can.
    pub positions: bool,
    /// Read these physical rows directly. Positions must be sorted and unique;
    /// predicates and block bounds must already have been applied by the caller.
    pub row_indices: Option<&'a [u64]>,
    /// Lists of items, by index into `predicates`: for each list the scan
    /// reports which rows one of its items matched, so that a relation follows
    /// the rows its own items matched without evaluating them again.
    pub item_tags: Vec<&'a [usize]>,
    /// Where to keep this scan's columns decoded whole, for a table several
    /// scans of the query filter: each then filters them in memory instead of
    /// decoding them again. Used when the scan's items match every row.
    pub column_cache: Option<&'a super::ColumnCache>,
    /// The block range of the query pass this scan belongs to, which holds
    /// every row the scan can return. Decoded columns keep only its rows.
    pub window: Option<super::Window>,
}

impl<'a> ScanRequest<'a> {
    pub fn new(output_columns: Vec<&'a str>) -> Self {
        Self {
            output_columns,
            predicates: Vec::new(),
            from_block: None,
            to_block: None,
            block_number_column: None,
            batch_size: usize::MAX,
            key_filter: None,
            hierarchical_filter: None,
            required_columns: Vec::new(),
            positions: false,
            row_indices: None,
            item_tags: Vec::new(),
            column_cache: None,
            window: None,
        }
    }
}

/// Composite-key set with an inline fast path. The common join key is two
/// integer columns (block_number + transaction_index = 16 bytes); packing it
/// into a `u128` avoids a per-key heap allocation on build and a slice hash on
/// probe. Wider or string/list keys fall back to serialized `Vec<u8>`.
enum CompositeKeySet {
    /// Two integer key columns.
    Fixed16(PairSet),
    /// Two integer columns and a path of item indices: an instruction's or a
    /// call's identity.
    PairPath(SourceAddresses),
    /// Arbitrary key: serialized bytes (see `TypedKeyColumn::append_to`).
    Wide(HashSet<Vec<u8>>),
    /// Row identity during materialization, including null key components.
    Rows {
        converter: RowConverter,
        values: HashSet<Vec<u8>>,
    },
}

impl CompositeKeySet {
    #[inline]
    fn is_empty(&self) -> bool {
        match self {
            Self::Fixed16(s) => s.is_empty(),
            Self::PairPath(s) => s.paths.is_empty(),
            Self::Wide(s) => s.is_empty(),
            Self::Rows { values, .. } => values.is_empty(),
        }
    }
}

/// A set-based filter for join key pushdown during relation scans.
/// Filters rows to only those matching specific composite keys from a primary scan.
/// Uses Arc-wrapped sets for cheap cloning into RowFilter closures.
pub struct KeyFilter {
    /// Column names forming the composite key (in the target/relation table).
    pub columns: Vec<String>,
    /// Pre-built set of composite keys (Arc for cheap clone into closures).
    key_set: Arc<CompositeKeySet>,
    /// Sorted unique block numbers for efficient row group pruning.
    sorted_blocks: Vec<u64>,
    /// Block number column name in the target table.
    block_number_column: String,
    /// Apply the cheap block predicate before decoding a complete row identity.
    materialization: bool,
}

impl KeyFilter {
    /// Select rows from the same physical table. Unlike a relation join, null
    /// components are part of a row's identity and must match themselves.
    pub(crate) fn for_rows(
        batches: &[RecordBatch],
        columns: &[String],
        block_column: &str,
    ) -> Result<Self> {
        let first = batches.first().ok_or_else(|| {
            engine_err!(ErrorKind::MalformedChunkData, "row selection has no schema")
        })?;
        let indices = columns
            .iter()
            .map(|name| first.schema().index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if batches.iter().all(|batch| {
            columns.iter().all(|name| {
                batch.column_by_name(name).is_some_and(|column| {
                    column.null_count() == 0
                        && (crate::integers::is_integer(column.data_type())
                            || matches!(column.data_type(), arrow::datatypes::DataType::Utf8))
                })
            })
        }) {
            let keys: Vec<_> = columns.iter().map(String::as_str).collect();
            let mut filter = Self::build(batches, &keys, &keys, block_column, block_column);
            filter.materialization = true;
            return Ok(filter);
        }
        let converter = RowConverter::new(
            indices
                .iter()
                .map(|&i| SortField::new(first.column(i).data_type().clone()))
                .collect(),
        )?;
        let mut values = HashSet::default();
        let mut blocks = HashSet::default();
        for batch in batches {
            let arrays: Vec<_> = columns
                .iter()
                .map(|name| {
                    batch
                        .schema()
                        .index_of(name)
                        .map(|i| batch.column(i).clone())
                })
                .collect::<std::result::Result<_, _>>()?;
            let rows = converter.convert_columns(&arrays)?;
            values.extend(rows.iter().map(|row| row.as_ref().to_vec()));
            if let Some(column) = batch.column_by_name(block_column) {
                extract_block_numbers(column.as_ref(), &mut blocks);
            }
        }
        let mut sorted_blocks: Vec<_> = blocks.into_iter().collect();
        sorted_blocks.sort_unstable();
        Ok(Self {
            columns: columns.to_vec(),
            key_set: Arc::new(CompositeKeySet::Rows { converter, values }),
            sorted_blocks,
            block_number_column: block_column.to_owned(),
            materialization: true,
        })
    }

    /// Build a key filter from primary scan results.
    ///
    /// - `primary_batches`: results from the primary table scan
    /// - `left_keys`: column names in primary_batches
    /// - `right_keys`: column names in the target/relation table
    /// - `primary_bn_col`: block number column name in primary_batches
    /// - `target_bn_col`: block number column name in the target table
    pub fn build(
        primary_batches: &[RecordBatch],
        left_keys: &[&str],
        right_keys: &[&str],
        primary_bn_col: &str,
        target_bn_col: &str,
    ) -> Self {
        KeySet::build(primary_batches, left_keys, primary_bn_col).filter(right_keys, target_bn_col)
    }

    pub fn is_empty(&self) -> bool {
        self.key_set.is_empty()
    }
}

/// The keys of some source rows, built once and matched against any target
/// whose key columns line up with them.
pub struct KeySet {
    key_set: Arc<CompositeKeySet>,
    /// Sorted unique block numbers for efficient row group pruning.
    sorted_blocks: Vec<u64>,
    /// How many columns a key has.
    width: usize,
}

impl KeySet {
    /// The `left_keys` values of every row of `primary_batches`.
    pub fn build(
        primary_batches: &[RecordBatch],
        left_keys: &[&str],
        primary_bn_col: &str,
    ) -> Self {
        let mut block_numbers = HashSet::default();

        // Fast path when the key is exactly two integer columns (block_number +
        // transaction_index): pack into a u128, avoiding a per-key heap alloc.
        let use_fixed16 = left_keys.len() == 2
            && primary_batches
                .iter()
                .find(|b| b.num_rows() > 0)
                .map(|b| {
                    left_keys.iter().all(|name| {
                        b.column_by_name(name)
                            .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
                            .map(|tc| tc.is_integer())
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false);

        let use_pair_path = left_keys.len() == 3
            && primary_batches
                .iter()
                .find(|b| b.num_rows() > 0)
                .is_some_and(|b| {
                    let int = |name: &str| {
                        b.column_by_name(name)
                            .is_some_and(|c| IntColumn::resolve(c.as_ref()).is_some())
                    };
                    int(left_keys[0])
                        && int(left_keys[1])
                        && address_list(b, left_keys[2]).is_some()
                });

        let key_set = if use_pair_path {
            for batch in primary_batches {
                if let Some(col) = batch.column_by_name(primary_bn_col) {
                    extract_block_numbers(col.as_ref(), &mut block_numbers);
                }
            }
            CompositeKeySet::PairPath(SourceAddresses::build(
                primary_batches,
                &left_keys[..2],
                left_keys[2],
            ))
        } else if use_fixed16 {
            let mut pairs = Vec::new();
            for batch in primary_batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                if let Some(col) = batch.column_by_name(primary_bn_col) {
                    extract_block_numbers(col.as_ref(), &mut block_numbers);
                }
                if let Some((first, second, nulls)) = pair_keys(batch, left_keys) {
                    let present = |row: &usize| nulls.as_ref().is_none_or(|n| n.is_valid(*row));
                    pairs.extend(
                        (0..batch.num_rows())
                            .filter(present)
                            .map(|row| (first[row], second[row])),
                    );
                }
            }
            CompositeKeySet::Fixed16(PairSet::new(pairs))
        } else {
            let mut set: HashSet<Vec<u8>> = HashSet::default();
            for batch in primary_batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                if let Some(col) = batch.column_by_name(primary_bn_col) {
                    extract_block_numbers(col.as_ref(), &mut block_numbers);
                }
                let typed_cols: Vec<Option<TypedKeyColumn>> = left_keys
                    .iter()
                    .map(|name| {
                        batch
                            .column_by_name(name)
                            .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
                    })
                    .collect();
                let mut key_buf = Vec::with_capacity(left_keys.len() * 8);
                for row in 0..batch.num_rows() {
                    key_buf.clear();
                    let complete = typed_cols
                        .iter()
                        .all(|tc| matches!(tc, Some(tc) if tc.append_to(&mut key_buf, row)));
                    if complete {
                        set.insert(key_buf.clone());
                    }
                }
            }
            CompositeKeySet::Wide(set)
        };

        let mut sorted_blocks: Vec<u64> = block_numbers.iter().copied().collect();
        sorted_blocks.sort_unstable();

        KeySet {
            key_set: Arc::new(key_set),
            sorted_blocks,
            width: left_keys.len(),
        }
    }

    /// Target rows whose `right_keys` columns hold one of the keys, paired with
    /// `left_keys` in order.
    pub fn filter(&self, right_keys: &[&str], target_bn_col: &str) -> KeyFilter {
        assert_eq!(self.width, right_keys.len());

        KeyFilter {
            columns: right_keys.iter().map(|s| s.to_string()).collect(),
            key_set: self.key_set.clone(),
            sorted_blocks: self.sorted_blocks.clone(),
            block_number_column: target_bn_col.to_string(),
            materialization: false,
        }
    }
}

/// Mode for hierarchical address filtering.
#[derive(Clone, Copy)]
pub enum HierarchicalMode {
    /// Keep rows whose address is a strict extension of a source address (children).
    Children,
    /// Keep rows whose address is a strict prefix of a source address (parents).
    Parents,
}

/// A filter for hierarchical joins (find_children / find_parents) that can be
/// applied as a RowFilter stage, avoiding decode of data columns for non-matching rows.
pub struct HierarchicalFilter {
    /// Every source address, by the group it belongs to.
    sources: Arc<SourceAddresses>,
    /// Group key column names in the target table (e.g., ["block_number", "transaction_index"]).
    pub group_key_columns: Vec<String>,
    /// Address column name in target table (e.g., "instruction_address", "call_address").
    pub address_column: String,
    /// Whether to find children or parents.
    mode: HierarchicalMode,
    /// When `true`, same-depth addresses count as a match (cross-table relations).
    /// When `false`, only strictly deeper/shallower addresses match (self-join).
    /// See `find_children` in `hierarchical.rs` for full explanation.
    inclusive: bool,
}

impl HierarchicalFilter {
    /// Build from primary scan results.
    ///
    /// - `source_address_column`: address column name in source (primary) batches
    /// - `target_address_column`: address column name in target batches (stored for scan-time use)
    /// - `inclusive`: see `find_children` in `hierarchical.rs`
    pub fn build(
        primary_batches: &[RecordBatch],
        group_key_columns: &[&str],
        source_address_column: &str,
        target_address_column: &str,
        mode: HierarchicalMode,
        inclusive: bool,
    ) -> Self {
        AddressIndex::build(primary_batches, group_key_columns, source_address_column).filter(
            target_address_column,
            mode,
            inclusive,
        )
    }

    pub fn is_empty(&self) -> bool {
        self.sources.paths.is_empty()
    }
}

/// The addresses of some source rows by group, built once and related to any
/// target by [`AddressIndex::filter`].
pub struct AddressIndex {
    sources: Arc<SourceAddresses>,
    group_key_columns: Vec<String>,
}

impl AddressIndex {
    pub fn build(
        primary_batches: &[RecordBatch],
        group_key_columns: &[&str],
        source_address_column: &str,
    ) -> Self {
        let sources =
            SourceAddresses::build(primary_batches, group_key_columns, source_address_column);

        AddressIndex {
            sources: Arc::new(sources),
            group_key_columns: group_key_columns.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Target rows whose `target_address_column` relates to a source address
    /// as `mode` and `inclusive` say.
    pub fn filter(
        &self,
        target_address_column: &str,
        mode: HierarchicalMode,
        inclusive: bool,
    ) -> HierarchicalFilter {
        HierarchicalFilter {
            sources: self.sources.clone(),
            group_key_columns: self.group_key_columns.clone(),
            address_column: target_address_column.to_string(),
            mode,
            inclusive,
        }
    }
}

/// Group ids by group key. Two integer columns, the key of every bundled
/// relation, pack into one `u128`; any other key is the bytes
/// [`TypedKeyColumn::append_to`] writes.
enum GroupIds {
    Pair(FxHashMap<u128, u32>),
    Bytes(FxHashMap<Vec<u8>, u32>),
}

impl GroupIds {
    fn len(&self) -> usize {
        match self {
            Self::Pair(ids) => ids.len(),
            Self::Bytes(ids) => ids.len(),
        }
    }

    /// The id of the group `row` belongs to, assigned on first sight.
    fn insert(
        &mut self,
        keys: &[Option<TypedKeyColumn>],
        row: usize,
        buf: &mut Vec<u8>,
    ) -> Option<u32> {
        let next = self.len() as u32;
        match self {
            Self::Pair(ids) => Some(*ids.entry(pair_key(keys, row)?).or_insert(next)),
            Self::Bytes(ids) => {
                let key = bytes_key(keys, row, buf)?;
                if let Some(&id) = ids.get(key) {
                    return Some(id);
                }
                ids.insert(key.to_vec(), next);
                Some(next)
            }
        }
    }

    /// The id of the group `row` belongs to, if any source does.
    fn get(&self, keys: &[Option<TypedKeyColumn>], row: usize, buf: &mut Vec<u8>) -> Option<u32> {
        match self {
            Self::Pair(ids) => ids.get(&pair_key(keys, row)?).copied(),
            Self::Bytes(ids) => ids.get(bytes_key(keys, row, buf)?).copied(),
        }
    }
}

/// A two-integer group key, or `None` when the row has none.
#[inline]
fn pair_key(keys: &[Option<TypedKeyColumn>], row: usize) -> Option<u128> {
    let [Some(TypedKeyColumn::Int(first)), Some(TypedKeyColumn::Int(second))] = keys else {
        return None;
    };
    let present = !first.is_null(row) && !second.is_null(row);

    present.then(|| pack16(first.join_key(row), second.join_key(row)))
}

/// A group key as bytes, or `None` when a component is missing or null.
#[inline]
fn bytes_key<'b>(
    keys: &[Option<TypedKeyColumn>],
    row: usize,
    buf: &'b mut Vec<u8>,
) -> Option<&'b [u8]> {
    buf.clear();
    let complete = keys
        .iter()
        .all(|key| matches!(key, Some(key) if key.append_to(buf, row)));

    complete.then_some(buf.as_slice())
}

/// The relation's source addresses, sorted within each group, so a target row
/// finds its relatives by binary search instead of by comparing itself with
/// every source of its transaction.
///
/// Address elements are kept as join keys, so a path compares by value
/// whatever width each side stored it at, and big-endian, so a deep path
/// compares as one run of bytes.
struct SourceAddresses {
    groups: GroupIds,
    /// Per group id, the range of `paths` holding the group's addresses.
    ranges: Vec<std::ops::Range<u32>>,
    /// `(start, len)` into `elements`, sorted by group and then by path, with
    /// no path twice in one group.
    paths: Vec<(u32, u32)>,
    /// Each element's join key, big-endian, so that comparing two paths'
    /// bytes orders them as comparing their elements does.
    elements: Vec<u8>,
    /// For deep paths, where a binary search compares long shared prefixes:
    /// every source path, and every strict prefix of one, by hash.
    hashed: Option<(PathIndex, PathIndex)>,
}

/// Paths a lookup finds by a hash of their group and elements, then confirms
/// by comparing the elements.
struct PathIndex {
    /// The first entry with a hash.
    first: FxHashMap<u64, u32>,
    /// `(hash, group, start, len)`, sorted by hash.
    entries: Vec<(u64, u32, u32, u32)>,
}

impl PathIndex {
    fn new(mut entries: Vec<(u64, u32, u32, u32)>) -> Self {
        entries.sort_unstable_by_key(|entry| entry.0);
        let mut first = FxHashMap::default();
        for (index, entry) in entries.iter().enumerate().rev() {
            first.insert(entry.0, index as u32);
        }

        Self { first, entries }
    }

    fn contains(&self, hash: u64, group: u32, path: &[u8], elements: &[u8]) -> bool {
        let Some(&first) = self.first.get(&hash) else {
            return false;
        };

        self.entries[first as usize..]
            .iter()
            .take_while(|entry| entry.0 == hash)
            .any(|&(_, g, start, len)| {
                g == group && &elements[start as usize * 8..(start + len) as usize * 8] == path
            })
    }
}

/// Paths deeper than this on average are looked up by hash.
const HASHED_DEPTH: usize = 4;

/// The hash of a group's empty path; [`path_hash_step`] extends it by one
/// element.
#[inline]
fn path_hash_start(group: u32) -> u64 {
    (group as u64 ^ 0x9e37_79b9_7f4a_7c15).wrapping_mul(0x517c_c1b7_2722_0a95)
}

#[inline]
fn path_hash_step(hash: u64, element: &[u8]) -> u64 {
    let element = u64::from_be_bytes(element.try_into().expect("eight bytes"));
    (hash.rotate_left(5) ^ element).wrapping_mul(0x517c_c1b7_2722_0a95)
}

impl SourceAddresses {
    fn build(batches: &[RecordBatch], group_key_columns: &[&str], address_column: &str) -> Self {
        let pair = group_key_columns.len() == 2
            && batches.iter().filter(|b| b.num_rows() > 0).all(|batch| {
                group_key_columns.iter().all(|name| {
                    batch
                        .column_by_name(name)
                        .is_some_and(|c| IntColumn::resolve(c.as_ref()).is_some())
                })
            });
        let mut groups = if pair {
            GroupIds::Pair(Default::default())
        } else {
            GroupIds::Bytes(Default::default())
        };

        let mut entries: Vec<(u32, u32, u32)> = Vec::new();
        let mut elements: Vec<u8> = Vec::new();
        let mut buf = Vec::new();
        for batch in batches {
            let keys = typed_key_columns(batch, group_key_columns);
            let Some((addresses, values)) = address_list(batch, address_column) else {
                continue;
            };
            let offsets = addresses.value_offsets();
            let element_bytes = values.join_key_bytes();
            let pairs = pair_keys(batch, group_key_columns);
            let group_of =
                |groups: &mut GroupIds, row: usize, buf: &mut Vec<u8>| match (&pairs, groups) {
                    (Some((first, second, nulls)), GroupIds::Pair(ids)) => {
                        let present = nulls.as_ref().is_none_or(|n| n.is_valid(row));
                        let next = ids.len() as u32;
                        present.then(|| *ids.entry(pack16(first[row], second[row])).or_insert(next))
                    }
                    (_, groups) => groups.insert(&keys, row, buf),
                };

            for row in 0..batch.num_rows() {
                // A null address is not the empty one, which is the root call.
                if addresses.is_null(row) {
                    continue;
                }
                let Some(group) = group_of(&mut groups, row, &mut buf) else {
                    continue;
                };

                let start = (elements.len() / 8) as u32;
                let path = offsets[row] as usize * 8..offsets[row + 1] as usize * 8;
                elements.extend_from_slice(&element_bytes[path]);
                entries.push((group, start, (elements.len() / 8) as u32 - start));
            }
        }

        // Group ids are dense, so one counting pass orders the paths by group,
        // and only each group's few paths are compared.
        let mut starts = vec![0u32; groups.len() + 1];
        for &(group, ..) in &entries {
            starts[group as usize + 1] += 1;
        }
        for group in 0..groups.len() {
            starts[group + 1] += starts[group];
        }
        let mut ordered = vec![(0u32, 0u32); entries.len()];
        let mut next = starts.clone();
        for &(group, start, len) in &entries {
            ordered[next[group as usize] as usize] = (start, len);
            next[group as usize] += 1;
        }

        let path =
            |&(start, len): &(u32, u32)| &elements[start as usize * 8..(start + len) as usize * 8];
        let mut ranges = Vec::with_capacity(groups.len());
        let mut paths = Vec::with_capacity(ordered.len());
        for group in 0..groups.len() {
            let own = &mut ordered[starts[group] as usize..starts[group + 1] as usize];
            own.sort_unstable_by(|a, b| path(a).cmp(path(b)));
            let first = paths.len() as u32;
            for &entry in own.iter() {
                let repeated =
                    paths.len() as u32 > first && path(paths.last().unwrap()) == path(&entry);
                if !repeated {
                    paths.push(entry);
                }
            }
            ranges.push(first..paths.len() as u32);
        }

        let deep =
            paths.iter().map(|&(_, len)| len as usize).sum::<usize>() > HASHED_DEPTH * paths.len();
        let hashed = deep.then(|| Self::index(&ranges, &paths, &elements));

        SourceAddresses {
            groups,
            ranges,
            paths,
            elements,
            hashed,
        }
    }

    /// Every source path, and every distinct strict prefix of one. A prefix
    /// shared by several paths is the prefix of each in a run of the sorted
    /// paths, so it is new only where it is longer than what the path shares
    /// with the one before, or is that whole path.
    fn index(
        ranges: &[std::ops::Range<u32>],
        paths: &[(u32, u32)],
        elements: &[u8],
    ) -> (PathIndex, PathIndex) {
        let bytes =
            |(start, len): (u32, u32)| &elements[start as usize * 8..(start + len) as usize * 8];
        let mut exact = Vec::with_capacity(paths.len());
        let mut prefixes = Vec::new();

        for (group, range) in ranges.iter().enumerate() {
            let group = group as u32;
            let mut previous: Option<&[u8]> = None;
            for &(start, len) in &paths[range.start as usize..range.end as usize] {
                let path = bytes((start, len));
                let new_from = previous.map_or(0, |before| {
                    let shared = before
                        .chunks_exact(8)
                        .zip(path.chunks_exact(8))
                        .take_while(|(a, b)| a == b)
                        .count();
                    if shared * 8 == before.len() {
                        shared
                    } else {
                        shared + 1
                    }
                });

                let mut hash = path_hash_start(group);
                for (depth, element) in path.chunks_exact(8).enumerate() {
                    if depth >= new_from {
                        prefixes.push((hash, group, start, depth as u32));
                    }
                    hash = path_hash_step(hash, element);
                }
                exact.push((hash, group, start, len));
                previous = Some(path);
            }
        }

        (PathIndex::new(exact), PathIndex::new(prefixes))
    }

    fn path(&self, (start, len): (u32, u32)) -> &[u8] {
        &self.elements[start as usize * 8..(start + len) as usize * 8]
    }

    /// Whether `target` is a source address of `group`.
    fn holds(&self, group: u32, target: &[u8]) -> bool {
        if let Some((exact, _)) = &self.hashed {
            let hash = target
                .chunks_exact(8)
                .fold(path_hash_start(group), path_hash_step);
            return exact.contains(hash, group, target, &self.elements);
        }

        let range = &self.ranges[group as usize];
        let paths = &self.paths[range.start as usize..range.end as usize];

        paths
            .binary_search_by(|&path| self.path(path).cmp(target))
            .is_ok()
    }

    /// Whether `target` is related to a source address of `group`.
    fn relates(&self, group: u32, target: &[u8], mode: HierarchicalMode, inclusive: bool) -> bool {
        if let Some((exact, prefixes)) = &self.hashed {
            let elements = &self.elements;
            return match mode {
                // A source that is a prefix of the target, strict unless inclusive.
                HierarchicalMode::Children => {
                    let depth = target.len() / 8;
                    let mut hash = path_hash_start(group);
                    let mut found = false;
                    for d in 0..=depth {
                        if d == depth && !inclusive {
                            break;
                        }
                        if exact.contains(hash, group, &target[..d * 8], elements) {
                            found = true;
                            break;
                        }
                        if d < depth {
                            hash = path_hash_step(hash, &target[d * 8..(d + 1) * 8]);
                        }
                    }
                    found
                }
                // A source the target is a strict prefix of, or equal to when
                // inclusive.
                HierarchicalMode::Parents => {
                    let hash = target
                        .chunks_exact(8)
                        .fold(path_hash_start(group), path_hash_step);
                    prefixes.contains(hash, group, target, elements)
                        || (inclusive && exact.contains(hash, group, target, elements))
                }
            };
        }

        let range = &self.ranges[group as usize];
        let paths = &self.paths[range.start as usize..range.end as usize];

        match mode {
            // A parent is one of the target's prefixes, one lookup per depth.
            HierarchicalMode::Children => {
                let depth = target.len() / 8;
                let depths = if inclusive { depth + 1 } else { depth };
                (0..depths).any(|depth| {
                    paths
                        .binary_search_by(|&path| self.path(path).cmp(&target[..depth * 8]))
                        .is_ok()
                })
            }
            // The addresses that extend the target sort right after it.
            HierarchicalMode::Parents => {
                let first = paths.partition_point(|&path| self.path(path) < target);
                let mut extensions = paths[first..].iter().map(|&path| self.path(path));

                match extensions.next() {
                    Some(path) if path == target => {
                        inclusive || extensions.next().is_some_and(|p| p.starts_with(target))
                    }
                    Some(path) => path.starts_with(target),
                    None => false,
                }
            }
        }
    }
}

fn typed_key_columns<'a>(
    batch: &'a RecordBatch,
    names: &[impl AsRef<str>],
) -> Vec<Option<TypedKeyColumn<'a>>> {
    names
        .iter()
        .map(|name| {
            batch
                .column_by_name(name.as_ref())
                .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
        })
        .collect()
}

/// A hierarchical address column and its elements. `None` where the column is
/// absent or its elements are not integers: an address is a path of item
/// indices, so anything else is a chunk that disagrees with its catalog, and
/// nothing matches.
fn address_list<'a>(
    batch: &'a RecordBatch,
    column: &str,
) -> Option<(&'a GenericListArray<i32>, IntColumn<'a>)> {
    let addresses = batch
        .column_by_name(column)?
        .as_any()
        .downcast_ref::<GenericListArray<i32>>()?;
    let values = IntColumn::resolve(addresses.values().as_ref())?;

    Some((addresses, values))
}

/// Build a boolean mask for hierarchical filtering.
/// Also performs key-in-set check inline, so this can replace a separate KeyFilter stage.
fn hierarchical_mask(
    batch: &RecordBatch,
    sources: &SourceAddresses,
    group_key_columns: &[String],
    address_column: &str,
    mode: HierarchicalMode,
    inclusive: bool,
    candidates: Option<&BooleanBuffer>,
) -> BooleanArray {
    let len = batch.num_rows();
    let mut builder = BooleanBufferBuilder::new(len);

    let Some((addresses, values)) = address_list(batch, address_column) else {
        builder.append_n(len, false);
        return BooleanArray::new(builder.finish(), None);
    };
    let keys = typed_key_columns(batch, group_key_columns);
    let pairs = match &sources.groups {
        GroupIds::Pair(_) => pair_keys(batch, group_key_columns),
        GroupIds::Bytes(_) => None,
    };
    let offsets = addresses.value_offsets();
    let element_bytes = values.join_key_bytes();

    let mut buf = Vec::new();
    for row in 0..len {
        let candidate = candidates.is_none_or(|c| c.value(row)) && !addresses.is_null(row);
        let group = match (&pairs, &sources.groups) {
            _ if !candidate => None,
            (Some((first, second, nulls)), GroupIds::Pair(ids)) => nulls
                .as_ref()
                .is_none_or(|n| n.is_valid(row))
                .then(|| ids.get(&pack16(first[row], second[row])).copied())
                .flatten(),
            (_, groups) => groups.get(&keys, row, &mut buf),
        };
        let related = group.is_some_and(|group| {
            let target = &element_bytes[offsets[row] as usize * 8..offsets[row + 1] as usize * 8];
            sources.relates(group, target, mode, inclusive)
        });
        builder.append(related);
    }

    BooleanArray::new(builder.finish(), None)
}

/// Extract all block number values from a column into a HashSet.
///
/// A column that is not an integer contributes nothing, which is what it has:
/// the block-number readers that must not fail silently are the ones the
/// assembly uses, and they raise `UnsupportedKeyType` on the same input.
fn extract_block_numbers(col: &dyn Array, out: &mut HashSet<u64>) {
    let Some(reader) = IntColumn::resolve(col) else {
        return;
    };

    for row in 0..reader.len() {
        out.insert(reader.block_number(row));
    }
}

/// A typed column extractor that avoids per-row type dispatch.
enum TypedKeyColumn<'a> {
    Int(IntColumn<'a>),
    Str(StringColumn<'a>),
    /// A path of item indices. `None` elements are not integers, so no row of
    /// the list has a key.
    List(&'a GenericListArray<i32>, Option<IntColumn<'a>>),
}

impl<'a> TypedKeyColumn<'a> {
    fn resolve(col: &'a dyn Array) -> Option<Self> {
        if let Some(ints) = IntColumn::resolve(col) {
            return Some(Self::Int(ints));
        }
        if let Some(text) = StringColumn::resolve(col) {
            return Some(Self::Str(text));
        }
        if let Some(a) = col.as_any().downcast_ref::<GenericListArray<i32>>() {
            return Some(Self::List(a, IntColumn::resolve(a.values().as_ref())));
        }

        None
    }

    #[inline(always)]
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Int(a) => a.is_null(row),
            Self::Str(a) => a.value(row).is_none(),
            Self::List(a, _) => a.is_null(row),
        }
    }

    /// Append this column's value at `row`, or report that there is none. A null
    /// list serializes byte-for-byte like an empty one, so without this a row
    /// that says "no call" joins to the call at the empty address.
    #[inline(always)]
    fn append_to(&self, buf: &mut Vec<u8>, row: usize) -> bool {
        if self.is_null(row) {
            return false;
        }

        match self {
            Self::Int(a) => buf.extend_from_slice(&a.join_key(row).to_le_bytes()),
            Self::Str(a) => {
                let v = a.value(row).expect("checked above");
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            Self::List(a, elements) => {
                // A list key is a path of item indices, so its elements are
                // integers at whatever width the writer chose, and each is
                // written the fixed eight bytes the scalar arm writes.
                let Some(elements) = elements else {
                    return false;
                };
                let offsets = a.value_offsets();
                let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);

                buf.extend_from_slice(&((end - start) as u32).to_le_bytes());
                for i in start..end {
                    buf.extend_from_slice(&elements.join_key(i).to_le_bytes());
                }
            }
        }

        true
    }

    /// True for integer key columns (eligible for the packed-u128 fast path).
    #[inline(always)]
    fn is_integer(&self) -> bool {
        matches!(self, Self::Int(_))
    }
}

/// The join keys of a batch's two integer key columns, and the rows where
/// either is null; `None` when they are not two integer columns.
fn pair_keys<S: AsRef<str>>(
    batch: &RecordBatch,
    columns: &[S],
) -> Option<(Vec<u64>, Vec<u64>, Option<arrow::buffer::NullBuffer>)> {
    let [first, second] = columns else {
        return None;
    };
    let column = |name: &S| IntColumn::resolve(batch.column_by_name(name.as_ref())?.as_ref());
    let (first, second) = (column(first)?, column(second)?);
    let nulls = arrow::buffer::NullBuffer::union(first.nulls(), second.nulls());

    Some((first.join_keys(), second.join_keys(), nulls))
}

/// Build a boolean mask: true for rows where composite key is in the set.
/// Resolves column types once per batch, then uses tight typed loops.
fn composite_key_in_set_mask(
    batch: &RecordBatch,
    key_columns: &[String],
    key_set: &CompositeKeySet,
    candidates: Option<&BooleanBuffer>,
) -> std::result::Result<BooleanArray, ArrowError> {
    let candidate = |row: usize| candidates.is_none_or(|c| c.value(row));
    let len = batch.num_rows();
    if let CompositeKeySet::Rows { converter, values } = key_set {
        let arrays: Vec<_> = key_columns
            .iter()
            .map(|name| {
                batch
                    .schema()
                    .index_of(name)
                    .map(|i| batch.column(i).clone())
            })
            .collect::<std::result::Result<_, _>>()?;
        let rows = converter.convert_columns(&arrays)?;
        return Ok(BooleanArray::from_iter(
            rows.iter().map(|row| Some(values.contains(row.as_ref()))),
        ));
    }
    let mut builder = BooleanBufferBuilder::new(len);

    // Resolve column types once (avoids per-row type dispatch)
    let typed_cols: Vec<Option<TypedKeyColumn>> = key_columns
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
        })
        .collect();

    match key_set {
        // Fast path: exactly two integer columns packed as u128 (matches
        // `KeyFilter::build`'s `pack16`). No per-row allocation, no slice hash.
        CompositeKeySet::Fixed16(set) => {
            let Some((first, second, nulls)) = pair_keys(batch, key_columns) else {
                return Ok(BooleanArray::new(BooleanBuffer::new_unset(len), None));
            };
            let found = BooleanBuffer::collect_bool(len, |row| {
                candidate(row) && set.contains(first[row], second[row])
            });
            let found = match nulls {
                Some(nulls) => &found & nulls.inner(),
                None => found,
            };
            return Ok(BooleanArray::new(found, None));
        }
        // General path: serialize each key column with `append_to` (matches
        // build), reusing one scratch buffer. Correct for string/list keys.
        CompositeKeySet::Wide(set) => {
            let mut key_buf = Vec::with_capacity(key_columns.len() * 8);
            for row in 0..len {
                if !candidate(row) {
                    builder.append(false);
                    continue;
                }
                key_buf.clear();
                let complete = typed_cols
                    .iter()
                    .all(|tc| matches!(tc, Some(tc) if tc.append_to(&mut key_buf, row)));
                builder.append(complete && set.contains(key_buf.as_slice()));
            }
        }
        CompositeKeySet::PairPath(sources) => {
            let Some((addresses, values)) = address_list(batch, &key_columns[2]) else {
                return Ok(BooleanArray::new(BooleanBuffer::new_unset(len), None));
            };
            let groups = &key_columns[..2];
            let keys = typed_key_columns(batch, groups);
            let pairs = pair_keys(batch, groups);
            let offsets = addresses.value_offsets();
            let element_bytes = values.join_key_bytes();
            let mut buf = Vec::new();
            let found = BooleanBuffer::collect_bool(len, |row| {
                if !candidate(row) || addresses.is_null(row) {
                    return false;
                }
                let group = match (&pairs, &sources.groups) {
                    (Some((first, second, nulls)), GroupIds::Pair(ids)) => nulls
                        .as_ref()
                        .is_none_or(|n| n.is_valid(row))
                        .then(|| ids.get(&pack16(first[row], second[row])).copied())
                        .flatten(),
                    (_, groups) => groups.get(&keys, row, &mut buf),
                };
                group.is_some_and(|group| {
                    let path = offsets[row] as usize * 8..offsets[row + 1] as usize * 8;
                    sources.holds(group, &element_bytes[path])
                })
            });
            return Ok(BooleanArray::new(found, None));
        }
        CompositeKeySet::Rows { .. } => unreachable!(),
    }

    Ok(BooleanArray::new(builder.finish(), None))
}

/// Determine all columns a scan must read: requested output, predicate columns,
/// the block-number column and any key or hierarchical filter columns —
/// restricted to those that actually exist in the table.
fn collect_read_columns<'a, 'b>(table: &ParquetTable, request: &'b ScanRequest<'a>) -> Vec<&'b str>
where
    'a: 'b,
{
    let mut all_columns: HashSet<&str> = HashSet::default();
    for col in &request.output_columns {
        all_columns.insert(col);
    }
    for pred in &request.predicates {
        for col in pred.required_columns() {
            all_columns.insert(col);
        }
    }
    if let Some(bn_col) = request.block_number_column {
        all_columns.insert(bn_col);
    }
    // Key filter columns must be available for RowFilter
    if let Some(kf) = &request.key_filter {
        all_columns.insert(&kf.block_number_column);
        for col in &kf.columns {
            all_columns.insert(col);
        }
    }
    // Hierarchical filter columns
    if let Some(hf) = &request.hierarchical_filter {
        for col in &hf.group_key_columns {
            all_columns.insert(col);
        }
        all_columns.insert(&hf.address_column);
    }

    all_columns
        .into_iter()
        .filter(|c| table.column_index(c).is_some())
        .collect()
}

/// A user-requested column declared in metadata but absent from this parquet
/// file is a hard error (matches legacy `ColumnDoesNotExist`).
///
/// The same applies to a *filtered* column. Reading it is what makes the filter
/// mean anything, and a filter that cannot be evaluated does not narrow the scan
/// — it widens it to everything, which no client can detect in the response
/// (INV-X3). Every scan entry point runs this, over every filter kind: a
/// relation's join key is as load-bearing as a predicate, and an unresolvable
/// one makes the pushdown drop itself while assembly still skips the join that
/// would have corrected it.
/// Refuse a chunk whose block-number column cannot place a row, before anything
/// reads it.
///
/// This runs once per scan, at the entry both scan paths share, off metadata
/// where the metadata answers and off the column where it does not. Later is not
/// good enough, and the reason is the shape of the bug rather than an ordering
/// detail: a check that sits where the rows are produced misses the rows a
/// predicate excluded, misses the hierarchical scan that returns before reaching
/// it, and finds an absent column only in the batches that happened to project
/// it. What a per-reader check gives is one chunk erroring on a direct scan and
/// answering, short, on a relation pull — the divergence
/// [gap 31](../../spec/GAPS.md) calls terminal, arrived at from the other
/// direction.
///
/// Every row group is checked, not the ones this query selects, for the same
/// reason: a narrow range must not answer where a wide one fails.
fn ensure_block_numbers_readable(table: &ParquetTable, request: &ScanRequest) -> Result<()> {
    let Some(bn_column) = request.block_number_column else {
        return Ok(());
    };

    let Some(index) = table.column_index(bn_column) else {
        crate::engine_bail!(
            crate::error::ErrorKind::ColumnNotFound,
            "block-number column '{}' is not found in '{}'",
            bn_column,
            table.name()
        );
    };

    let field = table.schema().field(index);
    crate::engine_ensure!(
        crate::integers::is_integer(field.data_type()),
        crate::error::ErrorKind::MalformedChunkData,
        "block-number column '{}' of '{}' is stored as {}, which is not an integer",
        bn_column,
        table.name(),
        field.data_type()
    );

    // A column parquet marks REQUIRED cannot hold a null, whatever its
    // statistics say or fail to say.
    if !field.is_nullable() {
        return Ok(());
    }

    let mut unstated: Vec<usize> = Vec::new();

    for rg in 0..table.num_row_groups() {
        match table.column_stats(rg, bn_column).and_then(|s| s.null_count) {
            Some(nulls) => crate::engine_ensure!(
                nulls <= 0,
                crate::error::ErrorKind::MalformedChunkData,
                "block-number column '{}' of '{}' leaves {} row(s) of row group {} without a block",
                bn_column,
                table.name(),
                nulls,
                rg
            ),
            None => unstated.push(rg),
        }
    }

    if unstated.is_empty() {
        return Ok(());
    }

    // A file that states no null count has not said there are none, and this is
    // the only place that can tell the two apart before a row is acted on.
    // Reading the column costs one narrow column of the groups that were silent,
    // and buys the promise the paragraphs above make: the same chunk answers, or
    // refuses, whatever the query. Left to the readers behind here it would
    // depend on the query — on the rows a predicate leaves, and on whether the
    // plan takes the hierarchical path, which returns before reaching any of
    // them.
    for batch in table.read(&[bn_column], Some(&unstated), 8192)? {
        let column = batch.column(0);

        crate::engine_ensure!(
            column.null_count() == 0,
            crate::error::ErrorKind::MalformedChunkData,
            "block-number column '{}' of '{}' leaves {} of {} rows without a block, \
             and the file states no null count",
            bn_column,
            table.name(),
            column.null_count(),
            column.len()
        );
    }

    Ok(())
}

fn ensure_columns_present(table: &ParquetTable, request: &ScanRequest) -> Result<()> {
    let mut required: Vec<&str> = request.required_columns.clone();

    for pred in &request.predicates {
        required.extend(pred.required_columns());
    }

    if let Some(kf) = &request.key_filter {
        required.push(&kf.block_number_column);
        required.extend(kf.columns.iter().map(String::as_str));
    }

    if let Some(hf) = &request.hierarchical_filter {
        required.extend(hf.group_key_columns.iter().map(String::as_str));
        required.push(&hf.address_column);
    }

    for col in required {
        if table.column_index(col).is_none() {
            crate::engine_bail!(
                crate::error::ErrorKind::ColumnNotFound,
                "column '{}' is not found in '{}'",
                col,
                table.name()
            );
        }
    }

    Ok(())
}

/// Refuse a filter whose values cannot be compared against the column as this
/// chunk stores it, before any row is read (INV-E7).
///
/// Here rather than in the row filter for the same reason the block-number
/// check is: a row filter's callback can only fail with an `ArrowError`, which
/// carries no kind, and a chunk that answers or refuses depending on how many
/// rows a predicate happened to reach is the same bug from the other side.
fn ensure_predicates_comparable(table: &ParquetTable, request: &ScanRequest) -> Result<()> {
    for pred in &request.predicates {
        for col_pred in pred.column_predicates() {
            let Some(index) = table.column_index(&col_pred.column) else {
                continue;
            };
            let stored = table.schema().field(index).data_type();

            if let Err(e) =
                crate::scan::predicate::check_stored_type(col_pred.predicate.as_ref(), stored)
            {
                crate::engine_bail!(
                    ErrorKind::UnsupportedKeyType,
                    "filter on column '{}' of '{}': {}",
                    col_pred.column,
                    table.name(),
                    e
                );
            }
        }
    }

    Ok(())
}

/// Execute a scan against a parquet table: read, filter, project.
/// Returns the filtered rows with only the output columns.
pub fn scan(table: &ParquetTable, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
    Ok(scan_rows(table, request)?.rows.into_batches())
}

/// [`scan`], with what the request asked to learn about the rows.
pub fn scan_rows(table: &ParquetTable, request: &ScanRequest) -> Result<Scanned> {
    let batches = scan_batches(table, request)?;
    Ok(Scanned::collect(request, batches))
}

fn scan_batches(table: &ParquetTable, request: &ScanRequest) -> Result<Vec<ScannedBatch>> {
    ensure_columns_present(table, request)?;
    ensure_predicates_comparable(table, request)?;
    ensure_block_numbers_readable(table, request)?;

    if let Some(rows) = request.row_indices {
        return super::positions::read_rows(table, request, rows);
    }

    // 1. Determine all columns we need to read (output + predicate + block range)
    let all_columns = collect_read_columns(table, request);

    // 2. Determine which row groups to scan (skip via statistics)
    let row_groups_to_scan = select_row_groups(table, request)?;

    if row_groups_to_scan.is_empty() {
        return Ok(Vec::new());
    }

    // 3. Read and filter across row groups
    let output_schema = build_output_schema(table.schema(), &request.output_columns);

    let in_memory =
        request.row_indices.is_none() && request.predicates.iter().all(|p| p.matches_every_row());
    if let Some(cache) = request.column_cache.filter(|_| in_memory) {
        let results: Vec<Result<Vec<ScannedBatch>>> = row_groups_to_scan
            .par_iter()
            .map(|&group| {
                scan_decoded_row_group(table, group, &all_columns, request, &output_schema, cache)
            })
            .collect();
        return Ok(results
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect());
    }

    // Parallelize across row groups for all scans with >1 RG.
    // Each RG gets its own RowFilter pipeline, evaluated independently.
    let mut all_batches = Vec::new();
    if row_groups_to_scan.len() <= 1 {
        all_batches.extend(scan_row_groups(
            table,
            &row_groups_to_scan,
            &all_columns,
            request,
            &output_schema,
        )?);
    } else {
        let results: Vec<Result<Vec<ScannedBatch>>> = row_groups_to_scan
            .par_iter()
            .map(|&rg_idx| scan_row_groups(table, &[rg_idx], &all_columns, request, &output_schema))
            .collect();
        for result in results {
            all_batches.extend(result?);
        }
    }

    Ok(all_batches)
}

/// Decode block bounds using the column's physical width. Wrapped signed
/// statistics can invert the bounds; such a pair must not prune any rows.
fn block_bounds(table: &ParquetTable, rg: usize, bn_column: &str) -> Option<(u64, u64)> {
    let width = table
        .schema()
        .field(table.column_index(bn_column)?)
        .data_type();
    let stats = table.column_stats(rg, bn_column)?;

    let min = crate::integers::block_number_at(width, stat_scalar(&stats.min?)?)?;
    let max = crate::integers::block_number_at(width, stat_scalar(&stats.max?)?)?;

    (min <= max).then_some((min, max))
}

/// Suggest up to four row-group ranges, merging strict overlaps to avoid
/// repeatedly decoding the same groups. Shared boundary blocks remain separate.
pub(crate) fn next_block_range_end(
    table: &ParquetTable,
    block_column: &str,
    from_block: u64,
) -> Option<u64> {
    let mut bounds = Vec::new();
    for group in 0..table.num_row_groups() {
        let (start, end) = block_bounds(table, group, block_column)?;
        if end >= from_block {
            bounds.push((start, end));
        }
    }
    bounds.sort_unstable();
    let mut ends: Vec<u64> = Vec::new();
    for (start, end) in bounds {
        if let Some(previous) = ends.last_mut() {
            if start < *previous {
                *previous = (*previous).max(end);
                continue;
            }
        }
        ends.push(end);
    }
    ends.get(ends.len().min(4).checked_sub(1)?).copied()
}

/// An upper bound on what a scan's output arrays hold, from the footer alone:
/// every row of every row group the scan would read, at what each column decodes
/// to. A group counts whole however little of its block span the request
/// covers, because its bounds say nothing about how its rows spread between
/// them. `None` when the footer cannot bound a column. ADR-15 lists what the
/// bound deliberately does not trust.
pub(crate) fn estimate_scan_bytes(
    table: &ParquetTable,
    request: &ScanRequest,
) -> Result<Option<u64>> {
    let parquet = table.metadata().file_metadata().schema_descr();
    let mut leaves = Vec::new();
    for &name in &request.output_columns {
        // A column the file lacks decodes to nulls, which hold no buffers.
        let Ok(field) = table.schema().field_with_name(name) else {
            continue;
        };

        let mut costs = Vec::new();
        if leaf_costs(field.data_type(), LeafCost::default(), &mut costs).is_none() {
            return Ok(None);
        }
        let indices: Vec<usize> = (0..parquet.num_columns())
            .filter(|&leaf| parquet.column(leaf).path().parts()[0] == name)
            .collect();
        if indices.len() != costs.len() {
            return Ok(None);
        }

        leaves.extend(indices.into_iter().zip(costs));
    }

    let mut total = 0u64;
    for group in select_row_groups(table, request)? {
        let metadata = table.row_group(group);
        let rows = metadata.num_rows().max(0) as u64;
        let batches = rows.div_ceil(request.batch_size.max(1) as u64);

        for &(leaf, cost) in &leaves {
            let Some(bytes) = decoded_bytes(metadata.column(leaf), cost, batches) else {
                return Ok(None);
            };
            total = total.saturating_add(bytes);
        }
    }

    Ok(Some(total))
}

/// What one level entry of a parquet leaf decodes to, beside the contents of
/// its byte strings.
#[derive(Clone, Copy, Default)]
struct LeafCost {
    /// Fixed-width values and the offsets of the leaf and every list above it.
    bytes: u64,
    /// A validity bit for every level, and a boolean's value.
    bits: u64,
    byte_strings: bool,
}

/// The cost of each parquet leaf under `data_type`, in schema order. `None` for
/// a type whose decoded layout this does not model.
fn leaf_costs(data_type: &DataType, above: LeafCost, out: &mut Vec<LeafCost>) -> Option<()> {
    let level = LeafCost {
        bits: above.bits + 1,
        ..above
    };
    let with_offsets = |width: u64| LeafCost {
        bytes: level.bytes + width,
        ..level
    };

    match data_type {
        DataType::List(item) | DataType::Map(item, _) => {
            leaf_costs(item.data_type(), with_offsets(4), out)
        }
        DataType::LargeList(item) => leaf_costs(item.data_type(), with_offsets(8), out),
        DataType::FixedSizeList(item, _) => leaf_costs(item.data_type(), level, out),
        DataType::Struct(fields) => fields
            .iter()
            .try_for_each(|field| leaf_costs(field.data_type(), level, out)),
        DataType::Utf8 | DataType::Binary => {
            out.push(LeafCost {
                byte_strings: true,
                ..with_offsets(4)
            });
            Some(())
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            out.push(LeafCost {
                byte_strings: true,
                ..with_offsets(8)
            });
            Some(())
        }
        DataType::Boolean => {
            out.push(LeafCost {
                bits: level.bits + 1,
                ..level
            });
            Some(())
        }
        DataType::Null => {
            out.push(level);
            Some(())
        }
        DataType::FixedSizeBinary(width) => {
            out.push(with_offsets((*width).max(0) as u64));
            Some(())
        }
        other => {
            out.push(with_offsets(other.primitive_width()? as u64));
            Some(())
        }
    }
}

/// What one column chunk decodes to: its level entries at `cost`, a bitmap per
/// level rounded up to whole bytes in every batch, and its byte strings.
/// Saturating, because a damaged footer can claim any count and a cost hint must
/// not be what panics on it.
fn decoded_bytes(column: &ColumnChunkMetaData, cost: LeafCost, batches: u64) -> Option<u64> {
    let entries = column.num_values().max(0) as u64;

    let fixed = entries.saturating_mul(cost.bytes);
    let bitmaps = entries
        .saturating_mul(cost.bits)
        .div_ceil(8)
        .saturating_add(cost.bits.saturating_mul(batches));
    let contents = if cost.byte_strings {
        byte_string_bytes(column)?
    } else {
        0
    };

    Some(fixed.saturating_add(bitmaps).saturating_add(contents))
}

/// The bytes a byte-string column chunk's values hold, where the footer bounds
/// them: the writer's count, or the stored pages of an encoding that keeps every
/// value whole. A dictionary keeps a repeated value once and prefix compression
/// a shared prefix once, and no statistic bounds what they expand to: bounds are
/// values rather than lengths, and a writer may truncate or omit them.
fn byte_string_bytes(column: &ColumnChunkMetaData) -> Option<u64> {
    if let Some(bytes) = column.unencoded_byte_array_data_bytes() {
        return Some(bytes.max(0) as u64);
    }

    // A dictionary the footer does not point at still shows in the encodings.
    let encodings = column.encodings();
    let keeps_values_whole = encodings.iter().all(|encoding| {
        matches!(
            encoding,
            Encoding::PLAIN | Encoding::DELTA_LENGTH_BYTE_ARRAY | Encoding::RLE
        )
    });
    let whole =
        !encodings.is_empty() && keeps_values_whole && column.dictionary_page_offset().is_none();

    whole.then(|| column.uncompressed_size().max(0) as u64)
}

fn select_row_groups(table: &ParquetTable, request: &ScanRequest) -> Result<Vec<usize>> {
    let mut row_groups = Vec::new();

    for rg_idx in 0..table.num_row_groups() {
        // Check block range filter
        // Bounds the file does not state, or states in a way no reader can
        // trust, prune nothing: reading the group costs time, skipping it costs
        // the rows, and it costs them silently.
        if let Some(bn_col) = request.block_number_column {
            if let Some((rg_min, rg_max)) = block_bounds(table, rg_idx, bn_col) {
                if let Some(from_block) = request.from_block {
                    if rg_max < from_block {
                        continue; // Entire row group is before our range
                    }
                }
                if let Some(to_block) = request.to_block {
                    if rg_min > to_block {
                        continue; // Entire row group is after our range
                    }
                }
            }
        }

        // Check predicate-based row group skipping
        if !request.predicates.is_empty() {
            let stats = row_group_stats(table, rg_idx, &request.predicates);
            if crate::scan::predicate::can_skip_row_group_or(&request.predicates, &stats) {
                continue;
            }
        }

        // Key filter: skip row groups whose block_number range has no overlap with key set
        if let Some(kf) = &request.key_filter {
            if let Some((rg_min, rg_max)) = block_bounds(table, rg_idx, &kf.block_number_column) {
                // Binary search: any key block number in [rg_min, rg_max]?
                let first = kf.sorted_blocks.partition_point(|&bn| bn < rg_min);
                if first >= kf.sorted_blocks.len() || kf.sorted_blocks[first] > rg_max {
                    continue; // No matching block numbers in this row group
                }
            }
        }

        row_groups.push(rg_idx);
    }

    Ok(row_groups)
}

/// One row group's statistics for the columns `predicates` filter on, each read
/// once, at the type the column is stored at. A hundred items name the same few
/// columns, and reading a statistic allocates.
fn row_group_stats<'p>(
    table: &ParquetTable,
    group: usize,
    predicates: &[&'p RowPredicate],
) -> impl Fn(&str) -> Option<crate::scan::predicate::StatRange> + 'p {
    let mut columns: Vec<(&str, Option<crate::scan::predicate::StatRange>)> = Vec::new();
    for predicate in predicates.iter().flat_map(|p| p.column_predicates()) {
        let column = predicate.column.as_str();
        if columns.iter().all(|(read, _)| *read != column) {
            columns.push((column, column_stats(table, group, column)));
        }
    }

    move |column| columns.iter().find(|(read, _)| *read == column)?.1.clone()
}

fn column_stats(
    table: &ParquetTable,
    group: usize,
    column: &str,
) -> Option<crate::scan::predicate::StatRange> {
    // Parquet stats come at the physical type; the stored type says how to read them.
    let stored = table.schema().field_with_name(column).ok()?.data_type();
    let stats = table.column_stats(group, column)?;
    let (min, max) = (stats.min?, stats.max?);
    crate::scan::predicate::StatRange::new(
        stored,
        stat_value_to_array(&min).as_ref(),
        stat_value_to_array(&max).as_ref(),
    )
}

/// Scan selected row groups using a single reader: read columns, apply predicates, project output.
///
/// Strategy:
/// - For scans with predicates: Use RowFilter with cascading stages (most selective first).
///   This reads predicate columns eagerly during build(), builds a RowSelection, then reads
///   output columns only for matching rows.
/// - For scans with key/hierarchical filters: read all columns including filter columns,
///   apply filters as RowFilter stages to avoid decoding output columns for non-matching rows.
fn scan_row_groups(
    table: &ParquetTable,
    row_groups: &[usize],
    read_columns: &[&str],
    request: &ScanRequest,
    output_schema: &SchemaRef,
) -> Result<Vec<ScannedBatch>> {
    let parquet_schema = table.metadata().file_metadata().schema_descr();
    let indices: Vec<usize> = read_columns
        .iter()
        .filter_map(|name| table.schema().index_of(name).ok())
        .collect();

    let mask = ProjectionMask::roots(parquet_schema, indices);

    // Use RowFilter for predicate pushdown with multi-stage cascading.
    // Each stage reads only its own columns; rows eliminated by early stages
    // avoid column decoding in later stages.
    let has_predicates = !request.predicates.is_empty();

    // An item whose statistics rule this row group out does not run on it.
    let stats: Vec<_> = row_groups
        .iter()
        .map(|&group| row_group_stats(table, group, &request.predicates))
        .collect();
    let active: Vec<usize> = (0..request.predicates.len())
        .filter(|&item| {
            stats
                .iter()
                .any(|stats| !request.predicates[item].can_skip_row_group(stats))
        })
        .collect();
    if has_predicates && active.is_empty() {
        return Ok(Vec::new());
    }
    let predicates: Vec<&RowPredicate> = active
        .iter()
        .map(|&item| request.predicates[item])
        .collect();
    let effective_from = request.from_block.filter(|&b| b > 0);
    // Relation keys already restrict blocks. Materialization keys can include
    // wide strings or lists, so reject blocks before decoding those components.
    let has_block_filter = request.key_filter.is_none_or(|key| key.materialization)
        && request.block_number_column.is_some()
        && (effective_from.is_some() || request.to_block.is_some());
    let has_key_filter = request.key_filter.is_some();
    let has_hierarchical_filter = request.hierarchical_filter.is_some();
    let mut tracked = request
        .positions
        .then(super::positions::TrackedRows::default);
    let item_tags = has_predicates
        .then(|| super::positions::ItemTags::new(request, &active))
        .flatten();
    let mut filter_stages: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> = Vec::new();

    if has_predicates || has_block_filter || has_key_filter || has_hierarchical_filter {
        // Stage 0: block range filter
        if has_block_filter {
            let bn_col = request.block_number_column.unwrap();
            if let Ok(idx) = table.schema().index_of(bn_col) {
                // Checked once here rather than per batch: the row filter's
                // callback may only fail with an `ArrowError`, which carries no
                // kind, and a block number the engine cannot compare has to
                // reach the client as one (INV-E6).
                let stored = table.schema().field(idx).data_type();
                if !crate::integers::is_integer(stored) {
                    return Err(engine_err!(
                        ErrorKind::UnsupportedKeyType,
                        "block number column '{}' is stored as {:?}, which is not an integer",
                        bn_col,
                        stored
                    ));
                }

                let bn_projection = ProjectionMask::roots(parquet_schema, vec![idx]);
                let from_block = effective_from;
                let to_block = request.to_block;
                let bn_col_name = bn_col.to_string();
                filter_stages.push(Box::new(ArrowPredicateFn::new(
                    bn_projection,
                    move |batch: RecordBatch| {
                        let Some(col) = batch.column_by_name(&bn_col_name) else {
                            return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
                        };

                        block_range_mask(col, from_block, to_block)
                            .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))
                    },
                )));
            }
        }

        if let Some(hf) = request.hierarchical_filter {
            // Hierarchical filter and predicates are structurally mutually exclusive:
            // predicates apply to primary scans, hierarchical filters to relation scans.
            assert!(
                request.predicates.is_empty(),
                "hierarchical_filter and predicates must not be set simultaneously"
            );
            // Two-pass approach for hierarchical filters:
            // Pass 1: Read only key columns (cheap integers), find matching row indices
            // Pass 2: Read address + data columns only for matching rows via RowSelection
            // This avoids decoding the expensive List column for 98%+ of rows.
            return scan_hierarchical_two_pass(table, row_groups, request, hf, output_schema);
        } else if let Some(kf) = request.key_filter {
            // KeyFilter only (no hierarchical) — standalone stage
            let key_col_indices: Vec<usize> = kf
                .columns
                .iter()
                .filter_map(|name| table.schema().index_of(name).ok())
                .collect();
            if !key_col_indices.is_empty() {
                let key_proj = ProjectionMask::roots(parquet_schema, key_col_indices);
                let key_columns = Arc::new(kf.columns.clone());
                let key_set = kf.key_set.clone();
                filter_stages.push(Box::new(ArrowPredicateFn::new(
                    key_proj,
                    move |batch: RecordBatch| {
                        composite_key_in_set_mask(&batch, &key_columns, &key_set, None)
                    },
                )));
            }
        }

        // Predicate stages: first column gets its own stage (most selective — sort key leader),
        // remaining columns are merged into a single stage. Tags need each
        // item's own answer, which only the stage evaluating all of them has.
        if predicates.len() == 1 && item_tags.is_none() {
            let pred = predicates[0];
            let (first, rest) = match pred.columns.split_first() {
                Some((first, rest)) => (Some(first), rest),
                None => (None, &[][..]),
            };
            if let Some(first) = first {
                if let Ok(idx) = table.schema().index_of(&first.column) {
                    let col_projection = ProjectionMask::roots(parquet_schema, vec![idx]);
                    let col_name = first.column.clone();
                    let evaluator = first.predicate.clone();
                    filter_stages.push(Box::new(ArrowPredicateFn::new(
                        col_projection,
                        move |batch: RecordBatch| {
                            if let Some(col) = batch.column_by_name(&col_name) {
                                evaluator
                                    .evaluate(col.as_ref())
                                    .map_err(|e| ArrowError::ComputeError(e.to_string()))
                            } else {
                                Ok(BooleanArray::from(vec![true; batch.num_rows()]))
                            }
                        },
                    )));
                }
            }

            let rest = RowPredicate::with_alternatives(rest.to_vec(), pred.alternatives.clone());
            if !rest.matches_every_row() {
                let mut rest_indices: Vec<usize> = rest
                    .required_columns()
                    .into_iter()
                    .filter_map(|column| table.schema().index_of(column).ok())
                    .collect();
                rest_indices.sort_unstable();
                rest_indices.dedup();
                if !rest_indices.is_empty() {
                    let rest_proj = ProjectionMask::roots(parquet_schema, rest_indices);
                    filter_stages.push(Box::new(ArrowPredicateFn::new(
                        rest_proj,
                        move |batch: RecordBatch| {
                            rest.evaluate(&batch)
                                .map_err(|e| ArrowError::ComputeError(e.to_string()))
                        },
                    )));
                }
            }
        } else if has_predicates {
            let mut pred_col_indices: Vec<usize> = Vec::new();
            for pred in &predicates {
                for col in pred.required_columns() {
                    if let Ok(idx) = table.schema().index_of(col) {
                        pred_col_indices.push(idx);
                    }
                }
            }
            pred_col_indices.sort_unstable();
            pred_col_indices.dedup();

            // One list per column admits every row an item could match, so
            // the items themselves run only on the rows it admits.
            let union = (predicates.len() > 1)
                .then(|| crate::scan::predicate::listed_union(&predicates))
                .flatten();
            if let Some(union) = union {
                let indices: Vec<usize> = union
                    .required_columns()
                    .into_iter()
                    .filter_map(|column| table.schema().index_of(column).ok())
                    .collect();
                filter_stages.push(Box::new(ArrowPredicateFn::new(
                    ProjectionMask::roots(parquet_schema, indices),
                    move |batch: RecordBatch| {
                        union
                            .evaluate(&batch)
                            .map_err(|e| ArrowError::ComputeError(e.to_string()))
                    },
                )));
            }

            let pred_projection = ProjectionMask::roots(parquet_schema, pred_col_indices);
            let predicates: Vec<RowPredicate> = predicates.iter().map(|&p| p.clone()).collect();
            let every_item: Vec<usize> = (0..predicates.len()).collect();
            let item_tags = item_tags.clone();

            // Last, so that the rows it selects are the rows read.
            filter_stages.push(Box::new(ArrowPredicateFn::new(
                pred_projection,
                move |batch: RecordBatch| {
                    let masks = predicates
                        .iter()
                        .map(|predicate| predicate.evaluate(&batch))
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| ArrowError::ComputeError(e.to_string()))?;
                    let matched =
                        crate::scan::predicate::or_masks(&masks, &every_item, batch.num_rows());

                    if let Some(tags) = &item_tags {
                        tags.record(&masks, &matched);
                    }
                    Ok(matched)
                },
            )));
        }

        if !filter_stages.is_empty() {
            if let Some(tracked) = &mut tracked {
                filter_stages = tracked.wrap(filter_stages);
            }
        }
    }

    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        table.data(),
        table.arrow_metadata().clone(),
    )
    .with_projection(mask)
    .with_batch_size(request.batch_size)
    .with_row_groups(row_groups.to_vec());
    if !filter_stages.is_empty() {
        builder = builder.with_row_filter(RowFilter::new(filter_stages));
    }
    let reader = builder.build().context("building parquet reader")?;
    let positions = tracked.map(|tracked| tracked.finish(table, row_groups));
    let tags = item_tags.map(|tags| tags.finish());
    let mut rows_read = 0;

    let mut output_batches = Vec::new();

    for batch_result in reader {
        let batch = batch_result.context("reading batch")?;
        let rows = batch.num_rows();
        if rows == 0 {
            continue;
        }

        let batch = project_batch(&batch, output_schema)?;
        let positions = positions
            .as_ref()
            .map(|positions| positions.slice(rows_read, rows));
        output_batches.push(match &tags {
            Some(tags) => ScannedBatch {
                batch,
                positions,
                tags: tags.iter().map(|tag| tag.slice(rows_read, rows)).collect(),
            },
            None => ScannedBatch::untagged(request, batch, positions),
        });
        rows_read += rows;
    }

    Ok(output_batches)
}

/// One row group of a scan whose items match every row: the rows inside the
/// pass's window decoded once for every scan that shares `cache`, and the
/// scan's block range, key and hierarchical filters applied in memory. Rows
/// come out as [`scan_row_groups`] returns them: a relation's rows share its
/// sources' blocks (INV-D5), all inside the window.
fn scan_decoded_row_group(
    table: &ParquetTable,
    group: usize,
    read_columns: &[&str],
    request: &ScanRequest,
    output_schema: &SchemaRef,
    cache: &super::ColumnCache,
) -> Result<Vec<ScannedBatch>> {
    let window = request.window.unwrap_or_default();
    let window_rows = cache.window_rows(table, group, window, || {
        let rows = table.row_group(group).num_rows() as usize;
        let bounded = window.from.is_some_and(|b| b > 0) || window.to.is_some();
        let block_index = request
            .block_number_column
            .and_then(|name| table.schema().index_of(name).ok())
            .filter(|_| bounded);
        let Some(index) = block_index else {
            return Ok(super::columns::WindowRows::All(rows));
        };

        let blocks = super::columns::decode_column(table, group, index)?;
        let inside = block_range_mask(&blocks, window.from.filter(|&b| b > 0), window.to)?;
        let inside = match inside.nulls() {
            Some(nulls) => inside.values() & nulls.inner(),
            None => inside.values().clone(),
        };
        Ok(if inside.count_set_bits() == rows {
            super::columns::WindowRows::All(rows)
        } else {
            super::columns::WindowRows::Some(inside.set_indices().map(|row| row as u32).collect())
        })
    })?;
    let window_rows = &window_rows.value;
    let rows = window_rows.len();

    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for &name in read_columns {
        let Ok(index) = table.schema().index_of(name) else {
            continue;
        };
        fields.push(table.schema().field(index).clone());
        columns.push(cache.column(table, group, index, window, window_rows)?);
    }
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )?;

    let mut keep = BooleanBuffer::new_set(rows);
    let block_column = request
        .block_number_column
        .and_then(|name| batch.column_by_name(name));
    let bounded = request.from_block.is_some_and(|b| b > 0) || request.to_block.is_some();
    // The same rule as the reader's: relation keys already bound the blocks.
    let block_filter = match request.hierarchical_filter {
        Some(_) => bounded,
        None => request.key_filter.is_none_or(|key| key.materialization) && bounded,
    };
    if let Some(column) = block_column.filter(|_| block_filter) {
        let from = request.from_block.filter(|&b| b > 0);
        keep = &keep & block_range_mask(column, from, request.to_block)?.values();
    }

    if let Some(hf) = request.hierarchical_filter {
        let mask = hierarchical_mask(
            &batch,
            &hf.sources,
            &hf.group_key_columns,
            &hf.address_column,
            hf.mode,
            hf.inclusive,
            Some(&keep),
        );
        keep = &keep & mask.values();
    } else if let Some(kf) = request.key_filter {
        let stored = kf
            .columns
            .iter()
            .any(|name| table.schema().index_of(name).is_ok());
        if stored {
            // A row of a block no key names cannot match one; the row group
            // pruning above relies on the same.
            let bounds = kf.sorted_blocks.first().zip(kf.sorted_blocks.last());
            let target_blocks = batch.column_by_name(&kf.block_number_column);
            if let (Some((&first, &last)), Some(column)) = (bounds, target_blocks) {
                keep = &keep & block_range_mask(column, Some(first), Some(last))?.values();
            }
            let mask = composite_key_in_set_mask(&batch, &kf.columns, &kf.key_set, Some(&keep))?;
            keep = &keep & mask.values();
        }
    }

    let kept = keep.count_set_bits();
    if kept == 0 {
        return Ok(Vec::new());
    }
    let selected = BooleanArray::new(keep, None);
    let batch =
        arrow::compute::filter_record_batch(&project_batch(&batch, output_schema)?, &selected)?;

    let positions = request.positions.then(|| {
        let start: u64 = (0..group)
            .map(|g| table.row_group(g).num_rows() as u64)
            .sum();
        selected
            .values()
            .set_indices()
            .map(|row| start + window_rows.offset(row) as u64)
            .collect::<UInt64Array>()
    });
    // Every item matches every row it reads, so a tag holds wherever one of
    // its items runs.
    let tags = request
        .item_tags
        .iter()
        .map(|items| BooleanArray::from(vec![!items.is_empty(); kept]))
        .collect();

    Ok(vec![ScannedBatch {
        batch,
        positions,
        tags,
    }])
}

/// Hierarchical scan with merged key+address RowFilter stage.
/// Reads key + address columns for all rows in a single RowFilter stage (cheap integer + List),
/// which eliminates ~98.8% of rows before the reader decodes heavy output columns
/// (instruction data, accounts, etc.). This is faster than:
/// - No RowFilter: decodes all output columns for all rows (14ms vs 4ms)
/// - Key-only RowFilter + post-filter: RowFilter machinery overhead exceeds savings (6ms vs 4ms)
/// - Two-pass with RowSelection: RowSelection can't skip pages (single page per RG), overhead (7ms)
fn scan_hierarchical_two_pass(
    table: &ParquetTable,
    row_groups: &[usize],
    request: &ScanRequest,
    hf: &HierarchicalFilter,
    output_schema: &SchemaRef,
) -> Result<Vec<ScannedBatch>> {
    let parquet_schema = table.metadata().file_metadata().schema_descr();

    // Collect all columns needed: output + key + address + block range
    let mut all_columns: HashSet<&str> = HashSet::default();
    for col in &request.output_columns {
        all_columns.insert(col);
    }
    for col in &hf.group_key_columns {
        all_columns.insert(col);
    }
    all_columns.insert(&hf.address_column);
    if let Some(bn_col) = request.block_number_column {
        all_columns.insert(bn_col);
    }

    let all_indices: Vec<usize> = all_columns
        .iter()
        .filter_map(|name| table.schema().index_of(name).ok())
        .collect();
    let main_mask = ProjectionMask::roots(parquet_schema, all_indices);

    // Merged KF+HF RowFilter stage: reads key + address columns,
    // applies hierarchical_mask which does first_key_set pre-filter + composite key lookup
    // + address prefix matching in a single pass.
    let mut filter_col_indices: Vec<usize> = Vec::new();
    for col in &hf.group_key_columns {
        if let Ok(idx) = table.schema().index_of(col) {
            filter_col_indices.push(idx);
        }
    }
    if let Ok(idx) = table.schema().index_of(&hf.address_column) {
        filter_col_indices.push(idx);
    }
    filter_col_indices.sort_unstable();
    filter_col_indices.dedup();

    let filter_proj = ProjectionMask::roots(parquet_schema, filter_col_indices);
    let sources = hf.sources.clone();
    let group_key_columns: Vec<String> = hf.group_key_columns.clone();
    let address_column: String = hf.address_column.clone();
    let mode = hf.mode;
    let inclusive = hf.inclusive;

    let filter_stage = Box::new(ArrowPredicateFn::new(
        filter_proj,
        move |batch: RecordBatch| {
            Ok(hierarchical_mask(
                &batch,
                &sources,
                &group_key_columns,
                &address_column,
                mode,
                inclusive,
                None,
            ))
        },
    ));

    let mut tracked = request
        .positions
        .then(super::positions::TrackedRows::default);
    let stages: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> = match &mut tracked {
        Some(tracked) => tracked.wrap(vec![filter_stage]),
        None => vec![filter_stage],
    };
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
        table.data(),
        table.arrow_metadata().clone(),
    )
    .with_projection(main_mask)
    .with_batch_size(request.batch_size)
    .with_row_groups(row_groups.to_vec())
    .with_row_filter(RowFilter::new(stages))
    .build()
    .context("building hierarchical reader")?;
    let positions = tracked.map(|tracked| tracked.finish(table, row_groups));
    let mut position_offset = 0;

    let mut output_batches = Vec::new();

    for batch_result in reader {
        let batch = batch_result.context("reading hierarchical batch")?;
        let count = batch.num_rows();
        if count == 0 {
            continue;
        }
        let batch_positions = positions
            .as_ref()
            .map(|positions| positions.slice(position_offset, count));
        position_offset += count;

        // Apply block range filter if needed
        let bounded = request.from_block.filter(|&b| b > 0).is_some() || request.to_block.is_some();
        let block_column = request
            .block_number_column
            .filter(|_| bounded)
            .and_then(|name| batch.column_by_name(name));
        let (batch, batch_positions) = match block_column {
            Some(column) => {
                let in_range = block_range_mask(column, request.from_block, request.to_block)?;
                let batch = arrow::compute::filter_record_batch(&batch, &in_range)
                    .context("block range filter in hierarchical scan")?;
                let batch_positions = batch_positions
                    .map(|positions| arrow::compute::filter(&positions, &in_range))
                    .transpose()?
                    .map(|positions| positions.as_primitive::<UInt64Type>().clone());
                (batch, batch_positions)
            }
            None => (batch, batch_positions),
        };

        if batch.num_rows() == 0 {
            continue;
        }

        let projected = project_batch(&batch, output_schema)?;
        output_batches.push(ScannedBatch::untagged(request, projected, batch_positions));
    }

    Ok(output_batches)
}

/// Rows whose block number falls in `[from_block, to_block]`.
///
/// A declared `uint64` bounds the values and not the storage, so every integer
/// width a writer may choose has an arm (INV-D7). A bound the stored width
/// cannot hold is not truncated into it: a `from` above the width's ceiling
/// matches nothing, and a `to` above it constrains nothing (INV-P14).
fn block_range_mask(
    column: &Arc<dyn Array>,
    from_block: Option<u64>,
    to_block: Option<u64>,
) -> Result<BooleanArray> {
    macro_rules! mask_over {
        ($($array:ty, $native:ty);+ $(;)?) => {
            $(if let Some(arr) = column.as_any().downcast_ref::<$array>() {
                let ceiling = <$native>::MAX as u64;
                if from_block.is_some_and(|from| from > ceiling) {
                    return Ok(BooleanArray::new(BooleanBuffer::new_unset(arr.len()), None));
                }

                let from = from_block
                    .map(|from| gt_eq(&arr, &<$array>::new_scalar(from as $native)))
                    .transpose()?;
                let to = to_block
                    .filter(|to| *to <= ceiling)
                    .map(|to| lt_eq(&arr, &<$array>::new_scalar(to as $native)))
                    .transpose()?;

                return Ok(match (from, to) {
                    (Some(from), Some(to)) => and(&from, &to)?,
                    (Some(bound), None) | (None, Some(bound)) => bound,
                    (None, None) => BooleanArray::new(BooleanBuffer::new_set(arr.len()), None),
                });
            })+
        };
    }

    mask_over!(
        UInt64Array, u64;
        UInt32Array, u32;
        UInt16Array, u16;
        UInt8Array, u8;
        Int64Array, i64;
        Int32Array, i32;
        Int16Array, i16;
        Int8Array, i8;
    );

    // Returning all-true here would leak every out-of-range row of the batch,
    // and the client cannot tell (INV-B1). A block number column that is not an
    // integer is a chunk disagreeing with its catalog.
    Err(engine_err!(
        ErrorKind::UnsupportedKeyType,
        "block number column is stored as {:?}, which is not an integer",
        column.data_type()
    ))
}

/// Project a RecordBatch to only include the given output columns.
pub(super) fn project_batch(batch: &RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
    let columns: Vec<Arc<dyn Array>> = output_schema
        .fields()
        .iter()
        .map(|field| {
            batch
                .column_by_name(field.name())
                .cloned()
                .unwrap_or_else(|| Arc::new(NullArray::new(batch.num_rows())))
        })
        .collect();

    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

/// Build the output Arrow schema from requested column names.
pub(super) fn build_output_schema(table_schema: &SchemaRef, columns: &[&str]) -> SchemaRef {
    let fields: Vec<_> = columns
        .iter()
        .filter_map(|name| table_schema.field_with_name(name).ok().cloned())
        .collect();
    Arc::new(Schema::new(fields))
}

/// Convert a StatValue to u64 (for block range comparisons).
/// The scalar an integer column's row-group statistic carries.
///
/// Parquet has no physical integer narrower than 32 bits, so this is where a
/// `UInt16` column's statistic arrives too, sign-extended. What the bits mean is
/// the column's width to say, and the caller asks it.
fn stat_scalar(value: &crate::scan::chunk::StatValue) -> Option<i64> {
    use crate::scan::chunk::StatValue;
    match value {
        StatValue::Int32(v) => Some(*v as i64),
        StatValue::Int64(v) => Some(*v),
        _ => None,
    }
}

/// Convert a StatValue to a single-element Arrow array (for predicate can_skip).
fn stat_value_to_array(value: &crate::scan::chunk::StatValue) -> Arc<dyn Array> {
    use crate::scan::chunk::StatValue;
    match value {
        StatValue::Boolean(v) => Arc::new(BooleanArray::from(vec![*v])),
        StatValue::Int32(v) => Arc::new(Int32Array::from(vec![*v])),
        StatValue::Int64(v) => Arc::new(Int64Array::from(vec![*v])),
        StatValue::Float(v) => Arc::new(Float32Array::from(vec![*v])),
        StatValue::Double(v) => Arc::new(Float64Array::from(vec![*v])),
        StatValue::ByteArray(v) => match std::str::from_utf8(v) {
            Ok(text) => Arc::new(StringArray::from(vec![text])),
            // Bytes no string comparison can order. Rendered as `""` they sort
            // below every filter value, and the group is pruned on that alone.
            Err(_) => Arc::new(BinaryArray::from(vec![v.as_slice()])),
        },
        StatValue::FixedLenByteArray(v) => {
            let len = v.len() as i32;
            let mut builder = FixedSizeBinaryBuilder::with_capacity(1, len);
            builder.append_value(v).unwrap();
            Arc::new(builder.finish())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::predicate::InListPredicate;
    use std::path::{Path, PathBuf};

    /// A byte statistic no string can carry prunes nothing. Rendered as `""` it
    /// sorts below every filter value, and the row group goes with it.
    ///
    /// Covers CT-3 · INV-P16
    #[test]
    fn a_statistic_that_is_not_text_prunes_nothing() {
        use crate::scan::chunk::StatValue;
        use crate::scan::predicate::{ArrayPredicate, StatRange};
        use arrow::datatypes::DataType;

        let raw = StatValue::ByteArray(vec![0xff, 0xfe]);
        let stat = stat_value_to_array(&raw);
        assert!(
            StatRange::new(&DataType::Utf8, stat.as_ref(), stat.as_ref()).is_none(),
            "unreadable bounds are no bounds"
        );

        let text = stat_value_to_array(&StatValue::ByteArray(b"0xabc".to_vec()));
        let range = StatRange::new(&DataType::Utf8, text.as_ref(), text.as_ref())
            .expect("valid utf8 still reads");
        assert!(InListPredicate::from_strings(&["0xdef"]).can_skip(&range));
        assert!(!InListPredicate::from_strings(&["0xabc"]).can_skip(&range));
    }

    #[test]
    fn materialization_keys_preserve_nulls_and_list_components() {
        use arrow::datatypes::UInt32Type;
        let identities: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from(vec![Some(0), None, Some(2), Some(3)])),
            Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![
                Some(vec![Some(0)]),
                None,
                Some(vec![None, Some(2)]),
                Some(vec![Some(3)]),
            ])),
        ];
        for identity in identities {
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "number",
                    Arc::new(UInt64Array::from(vec![7; 4])) as ArrayRef,
                ),
                ("identity", identity),
            ])
            .unwrap();
            let columns = vec!["number".to_owned(), "identity".to_owned()];
            let selected = KeyFilter::for_rows(&[batch.slice(1, 2)], &columns, "number").unwrap();
            let mask =
                composite_key_in_set_mask(&batch, &columns, &selected.key_set, None).unwrap();
            assert_eq!(mask, BooleanArray::from(vec![false, true, true, false]));
        }
    }

    /// A list key reads its elements through the list's offsets, so it must
    /// write what the row's own slice holds: at every width, on a sliced list,
    /// for null and empty lists and a null element. Elements that are not
    /// integers give no row a key.
    #[test]
    fn a_list_key_writes_what_its_row_slice_holds() {
        use crate::integers::IntColumn;
        use arrow::datatypes::*;

        fn lists<T: ArrowPrimitiveType>() -> ArrayRef {
            let value = |v: usize| T::Native::from_usize(v);
            Arc::new(ListArray::from_iter_primitive::<T, _, _>(vec![
                Some(vec![value(9)]),
                Some(vec![value(0), value(3)]),
                None,
                Some(vec![]),
                Some(vec![value(1), None, value(2)]),
                Some(vec![value(4)]),
            ]))
        }

        let oracle = |list: &GenericListArray<i32>, row: usize| -> Option<Vec<u8>> {
            if list.is_null(row) {
                return None;
            }
            let slice = list.value(row);
            let elements = IntColumn::resolve(slice.as_ref())?;
            let mut buf = (slice.len() as u32).to_le_bytes().to_vec();
            for i in 0..elements.len() {
                buf.extend_from_slice(&elements.join_key(i).to_le_bytes());
            }
            Some(buf)
        };

        let columns = [
            lists::<UInt8Type>(),
            lists::<UInt16Type>(),
            lists::<UInt32Type>(),
            lists::<UInt64Type>(),
            lists::<Int8Type>(),
            lists::<Int16Type>(),
            lists::<Int32Type>(),
            lists::<Int64Type>(),
            lists::<Float64Type>(),
        ];
        for column in &columns {
            for column in [column.clone(), column.slice(1, 5)] {
                let list = column.as_any().downcast_ref::<ListArray>().unwrap();
                let key = TypedKeyColumn::resolve(column.as_ref()).unwrap();
                for row in 0..list.len() {
                    let mut buf = Vec::new();
                    let written = key.append_to(&mut buf, row).then_some(buf);
                    assert_eq!(
                        written,
                        oracle(list, row),
                        "row {row} of a {} list",
                        column.data_type()
                    );
                }
            }
        }
    }

    /// The address index answers what comparing a target with every source of
    /// its group answers, in both modes, inclusive or not, over random groups,
    /// paths of any depth including the root, repeated sources, null keys and
    /// addresses, element widths that differ between the two sides, and keys
    /// that pack into a pair and keys that do not.
    #[test]
    fn the_address_index_relates_what_a_full_comparison_relates() {
        use arrow::datatypes::{UInt16Type, UInt32Type};

        type Row = (Option<u32>, Option<u32>, Option<Vec<u32>>);

        let mut seed = 0x5EED_0049u64;
        let mut next = move |bound: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % bound
        };

        // Deep paths are looked up by hash, shallow ones by binary search.
        let rows = |count: usize, deep: bool, next: &mut dyn FnMut(u64) -> u64| -> Vec<Row> {
            (0..count)
                .map(|_| {
                    let groups = if deep { 2 } else { 3 };
                    let key = |next: &mut dyn FnMut(u64) -> u64| {
                        (next(10) > 0).then(|| next(groups) as u32)
                    };
                    let block = key(next);
                    let tx = key(next);
                    let address = (next(10) > 0).then(|| {
                        let depth = if deep { 4 + next(7) } else { next(4) } as usize;
                        (0..depth)
                            .map(|_| next(if deep { 2 } else { 3 }) as u32)
                            .collect()
                    });
                    (block, tx, address)
                })
                .collect()
        };

        let batch = |rows: &[Row], wide: bool, text_key: bool| -> RecordBatch {
            let blocks: ArrayRef = Arc::new(Int32Array::from_iter(
                rows.iter().map(|r| r.0.map(|v| v as i32)),
            ));
            let txs: ArrayRef = if text_key {
                Arc::new(StringArray::from_iter(
                    rows.iter().map(|r| r.1.map(|v| v.to_string())),
                ))
            } else {
                Arc::new(UInt32Array::from_iter(rows.iter().map(|r| r.1)))
            };
            let paths = rows.iter().map(|r| {
                r.2.as_ref()
                    .map(|path| path.iter().map(|&v| Some(v)).collect::<Vec<_>>())
            });
            let addresses: ArrayRef = if wide {
                Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(paths))
            } else {
                let paths = paths.map(|p| p.map(|p| p.into_iter().map(|v| v.map(|v| v as u16))));
                Arc::new(ListArray::from_iter_primitive::<UInt16Type, _, _>(paths))
            };
            RecordBatch::try_from_iter(vec![("block", blocks), ("tx", txs), ("address", addresses)])
                .unwrap()
        };

        let related = |target: &[u32], source: &[u32], mode, inclusive: bool| match mode {
            HierarchicalMode::Children => {
                let deep_enough = if inclusive {
                    target.len() >= source.len()
                } else {
                    target.len() > source.len()
                };
                deep_enough && target.starts_with(source)
            }
            HierarchicalMode::Parents => {
                let deep_enough = if inclusive {
                    source.len() >= target.len()
                } else {
                    source.len() > target.len()
                };
                deep_enough && source.starts_with(target)
            }
        };

        let mut seen_both = [false; 2];
        let mut seen_hashed = [false; 2];
        for case in 0..800 {
            let deep = case % 16 >= 8;
            let sources = rows(1 + next(30) as usize, deep, &mut next);
            let mut targets = rows(1 + next(30) as usize, deep, &mut next);
            // Half the targets cut or extend a source's path, which is where a
            // prefix the index left out would show.
            for target in targets.iter_mut() {
                if next(2) == 0 {
                    continue;
                }
                let source = &sources[next(sources.len() as u64) as usize];
                let Some(path) = &source.2 else { continue };
                let mut path = path[..next(path.len() as u64 + 1) as usize].to_vec();
                if next(3) == 0 {
                    path.push(next(2) as u32);
                }
                *target = (source.0, source.1, Some(path));
            }
            let mode = if case % 2 == 0 {
                HierarchicalMode::Children
            } else {
                HierarchicalMode::Parents
            };
            let inclusive = case % 4 < 2;
            let text_key = case % 8 >= 6;

            let source_batch = batch(&sources, true, text_key);
            let target_batch = batch(&targets, case % 3 == 0, text_key);
            let filter = HierarchicalFilter::build(
                &[source_batch.slice(0, sources.len())],
                &["block", "tx"],
                "address",
                "address",
                mode,
                inclusive,
            );
            let mask = hierarchical_mask(
                &target_batch,
                &filter.sources,
                &filter.group_key_columns,
                "address",
                mode,
                inclusive,
                None,
            );
            seen_hashed[usize::from(filter.sources.hashed.is_some())] = true;

            // The same sources as exact keys: a target matches one that has
            // its block, its transaction and its whole path.
            let keys = ["block", "tx", "address"];
            let exact = KeyFilter::build(
                &[source_batch.slice(0, sources.len())],
                &keys,
                &keys,
                "block",
                "block",
            );
            let key_columns: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
            let matched =
                composite_key_in_set_mask(&target_batch, &key_columns, &exact.key_set, None)
                    .unwrap();
            for (row, target) in targets.iter().enumerate() {
                let expected = target.0.is_some()
                    && target.1.is_some()
                    && target.2.is_some()
                    && sources.contains(target);
                assert_eq!(
                    matched.value(row),
                    expected,
                    "case {case}, exact key {target:?}"
                );
            }

            for (row, (block, tx, address)) in targets.iter().enumerate() {
                let expected = match (block, tx, address) {
                    (Some(block), Some(tx), Some(target)) => sources.iter().any(|source| {
                        source.0 == Some(*block)
                            && source.1 == Some(*tx)
                            && source
                                .2
                                .as_ref()
                                .is_some_and(|s| related(target, s, mode, inclusive))
                    }),
                    _ => false,
                };
                seen_both[usize::from(expected)] = true;
                assert_eq!(
                    mask.value(row),
                    expected,
                    "case {case}, target {row} {:?} against {sources:?}",
                    targets[row]
                );
            }
        }
        assert_eq!(seen_both, [true, true], "every answer was the same");
        assert_eq!(
            seen_hashed,
            [true, true],
            "one way of finding paths went untested"
        );
    }

    fn solana_chunk_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk")
    }

    fn evm_chunk_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("data/evm/chunk")
    }

    // --- A3: block_range_mask must filter Int64/UInt16/Int16 columns ---

    #[test]
    fn test_block_range_mask_int64() {
        // A bare INT64 block_number column must be filtered, not pass-through.
        let col: Arc<dyn Array> = Arc::new(Int64Array::from(vec![100, 150, 200, 250]));
        let mask = block_range_mask(&col, Some(150), Some(200)).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, true, true, false]));
    }

    #[test]
    fn test_block_range_mask_uint16() {
        let col: Arc<dyn Array> = Arc::new(UInt16Array::from(vec![10u16, 20, 30, 40]));
        let mask = block_range_mask(&col, Some(20), Some(30)).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, true, true, false]));
    }

    #[test]
    fn test_block_range_mask_int16() {
        let col: Arc<dyn Array> = Arc::new(Int16Array::from(vec![10i16, 20, 30, 40]));
        let mask = block_range_mask(&col, Some(20), None).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, true, true, true]));
    }

    #[test]
    fn test_block_range_mask_int64_from_above_i64_max() {
        let col: Arc<dyn Array> = Arc::new(Int64Array::from(vec![0, i64::MAX]));
        let mask = block_range_mask(&col, Some(u64::MAX), None).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, false]));
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_no_predicate() {
        if !crate::testing::chunks_present() {
            return;
        }

        let table = ParquetTable::open(&solana_chunk_path().join("blocks.parquet")).unwrap();
        let request = ScanRequest::new(vec!["number", "hash"]);
        let batches = scan(&table, &request).unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, table.num_rows() as usize);
        assert_eq!(batches[0].num_columns(), 2);
    }

    /// A table several scans share is decoded once and filtered in memory; each
    /// scan must return the rows, columns, positions and marks the reader
    /// returns, in the same order, inside any window holding its rows.
    #[test]
    #[ignore = "requires external chunk data"]
    fn a_decoded_scan_returns_what_the_reader_returns() {
        if !crate::testing::chunks_present() {
            return;
        }

        let instructions =
            ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();
        let first = 406021645u64;
        let windows = [
            super::super::Window {
                from: Some(first),
                to: Some(first + 3),
            },
            super::super::Window {
                from: Some(first + 10),
                to: None,
            },
            super::super::Window::default(),
        ];
        let concat = |batches: Vec<RecordBatch>| -> Option<RecordBatch> {
            let schema = batches.first()?.schema();
            Some(arrow::compute::concat_batches(&schema, &batches).unwrap())
        };
        let columns = vec![
            "block_number",
            "transaction_index",
            "instruction_address",
            "program_id",
            "data_size",
        ];

        for window in windows {
            let bounded = |request: &mut ScanRequest| {
                request.block_number_column = Some("block_number");
                request.from_block = window.from;
                request.to_block = window.to;
                request.window = Some(window);
            };

            // The sources: some of the window's instructions.
            let mut source = ScanRequest::new(columns.clone());
            bounded(&mut source);
            let sources: Vec<RecordBatch> = scan(&instructions, &source)
                .unwrap()
                .iter()
                .map(|batch| batch.slice(0, batch.num_rows().min(500)))
                .collect();

            let pair_keys = ["block_number", "transaction_index"];
            let pair = KeyFilter::build(
                &sources,
                &pair_keys,
                &pair_keys,
                "block_number",
                "block_number",
            );
            let path_keys = ["block_number", "transaction_index", "instruction_address"];
            let path = KeyFilter::build(
                &sources,
                &path_keys,
                &path_keys,
                "block_number",
                "block_number",
            );
            assert!(matches!(
                path.key_set.as_ref(),
                CompositeKeySet::PairPath(_)
            ));
            let hierarchical = |mode| {
                HierarchicalFilter::build(
                    &sources,
                    &pair_keys,
                    "instruction_address",
                    "instruction_address",
                    mode,
                    false,
                )
            };
            let children = hierarchical(HierarchicalMode::Children);
            let parents = hierarchical(HierarchicalMode::Parents);
            let everything = RowPredicate::new(Vec::new());
            let tags = [&[0][..], &[][..]];
            let flatten = |scanned: Scanned| {
                let positions: Option<Vec<u64>> = scanned.rows.positions().map(|positions| {
                    positions
                        .iter()
                        .flat_map(|batch| batch.values().iter().copied())
                        .collect()
                });
                let tagged: Vec<_> = tags
                    .iter()
                    .map(|items| scanned.matched_by(items).and_then(concat))
                    .collect();
                (concat(scanned.rows.into_batches()), positions, tagged)
            };

            let mut requests = Vec::new();
            for filter in [&pair, &path] {
                let mut request = ScanRequest::new(columns.clone());
                bounded(&mut request);
                request.key_filter = Some(filter);
                requests.push(("key", request));
            }
            for filter in [&children, &parents] {
                let mut request = ScanRequest::new(columns.clone());
                bounded(&mut request);
                request.key_filter = Some(&pair);
                request.hierarchical_filter = Some(filter);
                requests.push(("hierarchical", request));
            }
            let mut unfiltered = ScanRequest::new(columns.clone());
            bounded(&mut unfiltered);
            unfiltered.predicates = vec![&everything];
            unfiltered.item_tags = tags.to_vec();
            requests.push(("unfiltered", unfiltered));

            let cache = super::super::ColumnCache::new(u64::MAX);
            for (what, request) in requests {
                for positions in [false, true] {
                    let mut request = request.clone();
                    request.positions = positions;
                    let expected = flatten(scan_rows(&instructions, &request).unwrap());
                    request.column_cache = Some(&cache);
                    // Twice: the second scan reads what the first decoded.
                    for _ in 0..2 {
                        let decoded = flatten(scan_rows(&instructions, &request).unwrap());
                        assert_eq!(
                            decoded, expected,
                            "{what} scan, positions {positions:?}, window {window:?}"
                        );
                    }
                }
            }
            assert!(cache.used() > 0, "the decoded scans kept nothing");
        }
    }

    /// Covers CT-5 · INV-B1
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_with_block_range() {
        if !crate::testing::chunks_present() {
            return;
        }

        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let total_rows = table.num_rows();

        // Scan with a narrow block range
        let mut request = ScanRequest::new(vec!["block_number", "program_id"]);
        request.block_number_column = Some("block_number");
        // Use a range that's a subset of the data
        request.from_block = Some(406021650);
        request.to_block = Some(406021670);

        let batches = scan(&table, &request).unwrap();
        let filtered_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Should have fewer rows than total
        assert!(
            filtered_rows < total_rows as usize,
            "block range filter should reduce rows: {} vs {}",
            filtered_rows,
            total_rows
        );
        assert!(filtered_rows > 0, "should have some matching rows");
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_with_predicate() {
        if !crate::testing::chunks_present() {
            return;
        }

        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let pred = RowPredicate::new(vec![crate::scan::predicate::ColumnPredicate {
            column: "program_id".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&[
                "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            ])),
        }]);

        let mut request = ScanRequest::new(vec!["block_number", "transaction_index", "program_id"]);
        request.predicates = vec![&pred];

        let batches = scan(&table, &request).unwrap();
        let filtered_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Verify all rows have the correct program_id
        for batch in &batches {
            let col = batch
                .column_by_name("program_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..col.len() {
                assert_eq!(col.value(i), "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
            }
        }

        assert!(
            filtered_rows < table.num_rows() as usize,
            "predicate should filter rows"
        );
    }

    /// Covers CT-5 · INV-B1
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_with_predicate_and_block_range() {
        if !crate::testing::chunks_present() {
            return;
        }

        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let pred = RowPredicate::new(vec![crate::scan::predicate::ColumnPredicate {
            column: "program_id".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&[
                "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            ])),
        }]);

        let mut request = ScanRequest::new(vec!["block_number", "program_id"]);
        request.predicates = vec![&pred];
        request.block_number_column = Some("block_number");
        request.from_block = Some(406021650);
        request.to_block = Some(406021670);

        let batches = scan(&table, &request).unwrap();
        let filtered_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // All rows should have correct program_id and block_number in range
        for batch in &batches {
            let program_id = batch
                .column_by_name("program_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let block_num = batch.column_by_name("block_number").unwrap();

            // Check block range using the appropriate type
            if let Some(arr) = block_num.as_any().downcast_ref::<UInt32Array>() {
                for i in 0..arr.len() {
                    let bn = arr.value(i) as u64;
                    assert!((406021650..=406021670).contains(&bn));
                }
            } else if let Some(arr) = block_num.as_any().downcast_ref::<UInt64Array>() {
                for i in 0..arr.len() {
                    let bn = arr.value(i);
                    assert!((406021650..=406021670).contains(&bn));
                }
            }

            for i in 0..program_id.len() {
                assert_eq!(
                    program_id.value(i),
                    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
                );
            }
        }

        // Without this the per-row checks above are vacuous: zero rows verifies
        // nothing. The chunk and the filter are both fixed, so it either matches
        // or the test is not testing anything.
        assert!(
            filtered_rows > 0,
            "the whirlpool program must match rows in this block range"
        );
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_predicate_columns_not_in_output() {
        if !crate::testing::chunks_present() {
            return;
        }

        // Predicate uses program_id but output only asks for block_number
        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let pred = RowPredicate::new(vec![crate::scan::predicate::ColumnPredicate {
            column: "program_id".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&[
                "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            ])),
        }]);

        let mut request = ScanRequest::new(vec!["block_number", "transaction_index"]);
        request.predicates = vec![&pred];

        let batches = scan(&table, &request).unwrap();

        // Output should NOT contain program_id
        for batch in &batches {
            assert_eq!(batch.num_columns(), 2);
            assert!(batch.schema().field_with_name("program_id").is_err());
        }
    }

    /// hierarchical_filter and predicates must not be set simultaneously.
    ///
    /// Asserted through `catch_unwind` rather than `#[should_panic]` so the test
    /// can skip when the chunk is absent: a `#[should_panic]` test that returns
    /// early fails, and one that panics for another reason passes.
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_hierarchical_filter_with_predicates_panics() {
        if !crate::testing::chunks_present() {
            return;
        }

        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let pred = RowPredicate::new(vec![crate::scan::predicate::ColumnPredicate {
            column: "program_id".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&[
                "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            ])),
        }]);

        // Build a minimal HierarchicalFilter from empty source batches
        let hf = HierarchicalFilter::build(
            &[],
            &["block_number", "transaction_index"],
            "instruction_address",
            "instruction_address",
            HierarchicalMode::Children,
            true,
        );

        let mut request = ScanRequest::new(vec!["block_number"]);
        request.predicates = vec![&pred];
        request.hierarchical_filter = Some(&hf);

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = scan(&table, &request);
        }))
        .expect_err("setting both must trip the debug assertion");

        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or_default()
            .to_string();
        assert!(
            message.contains("hierarchical_filter and predicates must not be set simultaneously"),
            "panicked with: {message}"
        );
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_row_group_pruning() {
        if !crate::testing::chunks_present() {
            return;
        }

        // EVM logs are sorted by topic0, so row group stats on topic0 should be tight.
        // Filtering for a specific topic0 should skip most row groups.
        let table = ParquetTable::open(&evm_chunk_path().join("logs.parquet")).unwrap();

        let pred = RowPredicate::new(vec![crate::scan::predicate::ColumnPredicate {
            column: "topic0".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&[
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            ])),
        }]);

        let mut request = ScanRequest::new(vec!["block_number", "address", "topic0"]);
        request.predicates = vec![&pred];

        let batches = scan(&table, &request).unwrap();
        let filtered_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // ERC-20 Transfer topic should match many rows but not all
        assert!(
            filtered_rows > 0,
            "should match some ERC-20 Transfer events"
        );
        assert!(
            filtered_rows < table.num_rows() as usize,
            "should not match all rows"
        );
    }
}
