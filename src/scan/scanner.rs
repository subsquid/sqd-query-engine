use crate::engine_err;
use crate::error::ErrorKind;
use crate::integers::{IntColumn, OwnedIntColumn};
use crate::scan::chunk::ParquetTable;
use crate::scan::predicate::RowPredicate;
use crate::text::StringColumn;
use anyhow::{Context, Result};
use arrow::array::builder::BooleanBufferBuilder;
use arrow::array::*;
use arrow::compute::kernels::boolean::and;
use arrow::compute::kernels::cmp::{gt_eq, lt_eq};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::row::{RowConverter, SortField};
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;
use rustc_hash::FxHashSet as HashSet;
use std::collections::HashMap;
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
    /// Append absolute physical row positions under this synthetic column name.
    /// Readers that support this must also support `row_indices` on later scans.
    pub row_index_column: Option<&'a str>,
    /// Read these physical rows directly. Positions must be sorted and unique;
    /// predicates and block bounds must already have been applied by the caller.
    pub row_indices: Option<&'a [u64]>,
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
            row_index_column: None,
            row_indices: None,
        }
    }
}

/// Composite-key set with an inline fast path. The common join key is two
/// integer columns (block_number + transaction_index = 16 bytes); packing it
/// into a `u128` avoids a per-key heap allocation on build and a slice hash on
/// probe. Wider or string/list keys fall back to serialized `Vec<u8>`.
enum CompositeKeySet {
    /// Two integer key columns, packed `(c0 << 64) | c1`.
    Fixed16(HashSet<u128>),
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
            Self::Wide(s) => s.is_empty(),
            Self::Rows { values, .. } => values.is_empty(),
        }
    }
}

/// Pack two normalized u64 key components into a single u128 (bijective; build
/// and probe must agree — they both call this).
#[inline(always)]
fn pack16(a: u64, b: u64) -> u128 {
    ((a as u128) << 64) | (b as u128)
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
        assert_eq!(left_keys.len(), right_keys.len());

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

        let key_set = if use_fixed16 {
            let mut set: HashSet<u128> = HashSet::default();
            for batch in primary_batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                if let Some(col) = batch.column_by_name(primary_bn_col) {
                    extract_block_numbers(col.as_ref(), &mut block_numbers);
                }
                let c0 = batch
                    .column_by_name(left_keys[0])
                    .and_then(|c| TypedKeyColumn::resolve(c.as_ref()));
                let c1 = batch
                    .column_by_name(left_keys[1])
                    .and_then(|c| TypedKeyColumn::resolve(c.as_ref()));
                if let (Some(c0), Some(c1)) = (c0, c1) {
                    for row in 0..batch.num_rows() {
                        if c0.is_null(row) || c1.is_null(row) {
                            continue;
                        }
                        set.insert(pack16(c0.get_u64(row), c1.get_u64(row)));
                    }
                }
            }
            CompositeKeySet::Fixed16(set)
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

        KeyFilter {
            columns: right_keys.iter().map(|s| s.to_string()).collect(),
            key_set: Arc::new(key_set),
            sorted_blocks,
            block_number_column: target_bn_col.to_string(),
            materialization: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.key_set.is_empty()
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
    /// Source addresses indexed by group key (serialized block_number + transaction_index).
    source_addresses: Arc<HashMap<Vec<u8>, Vec<Vec<u32>>>>,
    /// Set of first-key values (block_number as u64) for fast pre-filtering.
    /// Most rows don't match any source block, so this cheap check skips ~85-99% of rows
    /// before the expensive composite key build + HashMap lookup.
    first_key_set: Arc<rustc_hash::FxHashSet<u64>>,
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
        let mut source_addresses: HashMap<Vec<u8>, Vec<Vec<u32>>> = HashMap::new();

        for batch in primary_batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let typed_keys: Vec<Option<TypedKeyColumn>> = group_key_columns
                .iter()
                .map(|name| {
                    batch
                        .column_by_name(name)
                        .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
                })
                .collect();

            let addr_col = batch.column_by_name(source_address_column);
            let addr_list =
                addr_col.and_then(|c| c.as_any().downcast_ref::<GenericListArray<i32>>());

            if addr_list.is_none() {
                continue;
            }
            let addr_list = addr_list.unwrap();

            let mut key_buf = Vec::with_capacity(group_key_columns.len() * 8);
            for row in 0..batch.num_rows() {
                if addr_list.is_null(row) {
                    continue;
                }

                key_buf.clear();
                let complete = typed_keys
                    .iter()
                    .all(|tc| matches!(tc, Some(tc) if tc.append_to(&mut key_buf, row)));
                if !complete {
                    continue;
                }
                let addr = extract_address_values(addr_list, row);
                source_addresses
                    .entry(key_buf.clone())
                    .or_default()
                    .push(addr);
            }
        }

        // Extract unique first-key values (block_number) for fast pre-filtering
        let mut first_key_set = rustc_hash::FxHashSet::default();
        for key in source_addresses.keys() {
            if key.len() >= 8 {
                let v = u64::from_le_bytes(key[..8].try_into().unwrap());
                first_key_set.insert(v);
            }
        }

        HierarchicalFilter {
            source_addresses: Arc::new(source_addresses),
            first_key_set: Arc::new(first_key_set),
            group_key_columns: group_key_columns.iter().map(|s| s.to_string()).collect(),
            address_column: target_address_column.to_string(),
            mode,
            inclusive,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.source_addresses.is_empty()
    }
}

/// A hierarchical address as a path of item indices, at whatever width the
/// writer stored the elements.
fn extract_address_values(array: &GenericListArray<i32>, row: usize) -> Vec<u32> {
    if array.is_null(row) {
        return Vec::new();
    }

    let values = array.value(row);
    let Some(elements) = IntColumn::resolve(values.as_ref()) else {
        return Vec::new();
    };

    (0..elements.len())
        .map(|i| elements.block_number(i) as u32)
        .collect()
}

/// Build a boolean mask for hierarchical filtering.
/// Also performs key-in-set check inline, so this can replace a separate KeyFilter stage.
fn hierarchical_mask(
    batch: &RecordBatch,
    source_addresses: &HashMap<Vec<u8>, Vec<Vec<u32>>>,
    first_key_set: &rustc_hash::FxHashSet<u64>,
    group_key_columns: &[String],
    address_column: &str,
    mode: HierarchicalMode,
    inclusive: bool,
) -> BooleanArray {
    let len = batch.num_rows();
    let mut builder = BooleanBufferBuilder::new(len);

    let typed_keys: Vec<Option<TypedKeyColumn>> = group_key_columns
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
        })
        .collect();

    let addr_col = batch.column_by_name(address_column);
    let addr_list = addr_col.and_then(|c| c.as_any().downcast_ref::<GenericListArray<i32>>());

    if addr_list.is_none() {
        builder.append_n(len, false);
        return BooleanArray::new(builder.finish(), None);
    }
    let addr_list = addr_list.unwrap();

    // Resolve the element type once for the whole column
    let values = addr_list.values();
    let elem_type = resolve_list_element_type(values.as_ref());

    // Fast path: 2-column key (block_number + transaction_index) with first-key pre-filter
    if typed_keys.len() == 2 {
        if let (Some(ref c0), Some(ref c1)) = (&typed_keys[0], &typed_keys[1]) {
            let mut key_buf = [0u8; 16];
            for row in 0..len {
                if addr_list.is_null(row) || c0.is_null(row) || c1.is_null(row) {
                    builder.append(false);
                    continue;
                }
                // Pre-filter: check first key (block_number) before building full composite key
                c0.write_to_buf(&mut key_buf[..8], row);
                let first_key = u64::from_le_bytes(key_buf[..8].try_into().unwrap());
                if !first_key_set.contains(&first_key) {
                    builder.append(false);
                    continue;
                }
                c1.write_to_buf(&mut key_buf[8..16], row);
                let matches = source_addresses
                    .get(&key_buf[..16])
                    .map(|addrs| match_address(addr_list, row, &elem_type, addrs, mode, inclusive))
                    .unwrap_or(false);
                builder.append(matches);
            }
            return BooleanArray::new(builder.finish(), None);
        }
    }

    // General path
    let mut key_buf = Vec::with_capacity(group_key_columns.len() * 8);
    for row in 0..len {
        if addr_list.is_null(row) {
            builder.append(false);
            continue;
        }

        key_buf.clear();
        let complete = typed_keys
            .iter()
            .all(|tc| matches!(tc, Some(tc) if tc.append_to(&mut key_buf, row)));
        if !complete {
            builder.append(false);
            continue;
        }

        let matches = source_addresses
            .get(key_buf.as_slice())
            .map(|addrs| match_address(addr_list, row, &elem_type, addrs, mode, inclusive))
            .unwrap_or(false);

        builder.append(matches);
    }

    BooleanArray::new(builder.finish(), None)
}

#[inline]
fn match_address(
    addr_list: &GenericListArray<i32>,
    row: usize,
    elem_type: &ListElementType,
    addrs: &[Vec<u32>],
    mode: HierarchicalMode,
    inclusive: bool,
) -> bool {
    let offsets = addr_list.offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;
    let target_len = end - start;
    match mode {
        HierarchicalMode::Children => addrs.iter().any(|parent| {
            let len_ok = if inclusive {
                target_len >= parent.len()
            } else {
                target_len > parent.len()
            };
            len_ok && compare_address_prefix(elem_type, start, parent)
        }),
        HierarchicalMode::Parents => addrs.iter().any(|child| {
            let len_ok = if inclusive {
                target_len <= child.len()
            } else {
                target_len < child.len()
            };
            len_ok && compare_address_prefix(elem_type, start, &child[..target_len])
        }),
    }
}

/// Resolved list element array for zero-allocation address comparison.
///
/// `None` where the elements are not integers: an address is a path of item
/// indices, so anything else is a chunk that disagrees with its catalog, and
/// nothing matches.
type ListElementType = Option<OwnedIntColumn>;

fn resolve_list_element_type(values: &dyn Array) -> ListElementType {
    OwnedIntColumn::resolve(values)
}

/// Compare address elements starting at `start` against `expected` without allocating.
#[inline]
fn compare_address_prefix(elem_type: &ListElementType, start: usize, expected: &[u32]) -> bool {
    let Some(elements) = elem_type else {
        return false;
    };

    expected
        .iter()
        .enumerate()
        .all(|(i, &v)| elements.value(start + i) == v as i128)
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
    List(&'a GenericListArray<i32>),
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
            return Some(Self::List(a));
        }

        None
    }

    #[inline(always)]
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Int(a) => a.is_null(row),
            Self::Str(a) => a.value(row).is_none(),
            Self::List(a) => a.is_null(row),
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
            Self::List(a) => {
                let arr = a.value(row);
                buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());

                // A list key is a path of item indices, so its elements are
                // integers at whatever width the writer chose, and each is
                // written the fixed eight bytes the scalar arm writes.
                let Some(elements) = IntColumn::resolve(arr.as_ref()) else {
                    return false;
                };
                for i in 0..elements.len() {
                    buf.extend_from_slice(&elements.join_key(i).to_le_bytes());
                }
            }
        }

        true
    }

    /// Get value as u64. Only for integer types.
    #[inline(always)]
    fn get_u64(&self, row: usize) -> u64 {
        match self {
            Self::Int(a) => a.join_key(row),
            _ => 0,
        }
    }

    /// True for integer key columns (eligible for the packed-u128 fast path).
    #[inline(always)]
    fn is_integer(&self) -> bool {
        matches!(self, Self::Int(_))
    }

    /// Write normalized u64 value directly into a fixed-size buffer slice.
    /// Only for integer types (used in optimized paths).
    #[inline(always)]
    fn write_to_buf(&self, buf: &mut [u8], row: usize) {
        buf[..8].copy_from_slice(&self.get_u64(row).to_le_bytes());
    }
}

/// Build a boolean mask: true for rows where composite key is in the set.
/// Resolves column types once per batch, then uses tight typed loops.
fn composite_key_in_set_mask(
    batch: &RecordBatch,
    key_columns: &[String],
    key_set: &CompositeKeySet,
) -> std::result::Result<BooleanArray, ArrowError> {
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
            if let [Some(c0), Some(c1)] = typed_cols.as_slice() {
                for row in 0..len {
                    let present = !c0.is_null(row) && !c1.is_null(row);
                    builder
                        .append(present && set.contains(&pack16(c0.get_u64(row), c1.get_u64(row))));
                }
            } else {
                builder.append_n(len, false);
            }
        }
        // General path: serialize each key column with `append_to` (matches
        // build), reusing one scratch buffer. Correct for string/list keys.
        CompositeKeySet::Wide(set) => {
            let mut key_buf = Vec::with_capacity(key_columns.len() * 8);
            for row in 0..len {
                key_buf.clear();
                let complete = typed_cols
                    .iter()
                    .all(|tc| matches!(tc, Some(tc) if tc.append_to(&mut key_buf, row)));
                builder.append(complete && set.contains(key_buf.as_slice()));
            }
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
        for col_pred in &pred.columns {
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
/// Returns filtered RecordBatches with only the output columns.
pub fn scan(table: &ParquetTable, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
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
        let results: Vec<Result<Vec<RecordBatch>>> = row_groups_to_scan
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
            let stats_fn = |col_name: &str| -> Option<crate::scan::predicate::StatRange> {
                // Parquet stats come at the physical type; the stored type says how to read them.
                let stored = table.schema().field_with_name(col_name).ok()?.data_type();
                let stats = table.column_stats(rg_idx, col_name)?;
                let (min, max) = (stats.min?, stats.max?);
                crate::scan::predicate::StatRange::new(
                    stored,
                    stat_value_to_array(&min).as_ref(),
                    stat_value_to_array(&max).as_ref(),
                )
            };

            let pred_refs: Vec<&RowPredicate> = request.predicates.clone();
            if crate::scan::predicate::can_skip_row_group_or(&pred_refs, &stats_fn) {
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
) -> Result<Vec<RecordBatch>> {
    let parquet_schema = table.metadata().file_metadata().schema_descr();
    let indices: Vec<usize> = read_columns
        .iter()
        .filter_map(|name| table.schema().index_of(name).ok())
        .collect();

    let mask = ProjectionMask::roots(parquet_schema, indices);

    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        table.data(),
        table.arrow_metadata().clone(),
    )
    .with_projection(mask)
    .with_batch_size(request.batch_size)
    .with_row_groups(row_groups.to_vec());

    // Use RowFilter for predicate pushdown with multi-stage cascading.
    // Each stage reads only its own columns; rows eliminated by early stages
    // avoid column decoding in later stages.
    let has_predicates = !request.predicates.is_empty();
    let effective_from = request.from_block.filter(|&b| b > 0);
    // Relation keys already restrict blocks. Materialization keys can include
    // wide strings or lists, so reject blocks before decoding those components.
    let has_block_filter = request.key_filter.is_none_or(|key| key.materialization)
        && request.block_number_column.is_some()
        && (effective_from.is_some() || request.to_block.is_some());
    let has_key_filter = request.key_filter.is_some();
    let has_hierarchical_filter = request.hierarchical_filter.is_some();
    let mut tracked = request
        .row_index_column
        .map(|_| super::positions::TrackedRows::default());

    if has_predicates || has_block_filter || has_key_filter || has_hierarchical_filter {
        let mut filter_stages: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> =
            Vec::new();

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
                        composite_key_in_set_mask(&batch, &key_columns, &key_set)
                    },
                )));
            }
        }

        // Predicate stages: first column gets its own stage (most selective — sort key leader),
        // remaining columns are merged into a single stage.
        if request.predicates.len() == 1 {
            let pred = request.predicates[0].clone();
            let columns = pred.columns;
            if let Some(first) = columns.first() {
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
            if columns.len() > 1 {
                let rest: Vec<_> = columns[1..].to_vec();
                let mut rest_indices: Vec<usize> = Vec::new();
                for cp in &rest {
                    if let Ok(idx) = table.schema().index_of(&cp.column) {
                        rest_indices.push(idx);
                    }
                }
                rest_indices.sort_unstable();
                rest_indices.dedup();
                if !rest_indices.is_empty() {
                    let rest_proj = ProjectionMask::roots(parquet_schema, rest_indices);
                    filter_stages.push(Box::new(ArrowPredicateFn::new(
                        rest_proj,
                        move |batch: RecordBatch| {
                            let mut result: Option<BooleanArray> = None;
                            for cp in &rest {
                                if let Some(col) = batch.column_by_name(&cp.column) {
                                    let mask = cp
                                        .predicate
                                        .evaluate(col.as_ref())
                                        .map_err(|e| ArrowError::ComputeError(e.to_string()))?;
                                    result = Some(match result {
                                        Some(prev) => {
                                            arrow::compute::kernels::boolean::and(&prev, &mask)
                                                .unwrap()
                                        }
                                        None => mask,
                                    });
                                }
                            }
                            Ok(result.unwrap_or_else(|| {
                                BooleanArray::from(vec![true; batch.num_rows()])
                            }))
                        },
                    )));
                }
            }
        } else if has_predicates {
            let mut pred_col_indices: Vec<usize> = Vec::new();
            for pred in &request.predicates {
                for col in pred.required_columns() {
                    if let Ok(idx) = table.schema().index_of(col) {
                        pred_col_indices.push(idx);
                    }
                }
            }
            pred_col_indices.sort_unstable();
            pred_col_indices.dedup();

            let pred_projection = ProjectionMask::roots(parquet_schema, pred_col_indices);
            let predicates: Vec<RowPredicate> =
                request.predicates.iter().map(|&p| p.clone()).collect();

            filter_stages.push(Box::new(ArrowPredicateFn::new(
                pred_projection,
                move |batch: RecordBatch| {
                    let pred_refs: Vec<&RowPredicate> = predicates.iter().collect();
                    crate::scan::predicate::or_row_predicates(&pred_refs, &batch)
                        .map_err(|e| ArrowError::ComputeError(e.to_string()))
                },
            )));
        }

        if !filter_stages.is_empty() {
            if let Some(tracked) = &mut tracked {
                filter_stages = tracked.wrap(filter_stages);
            }
            builder = builder.with_row_filter(RowFilter::new(filter_stages));
        }
    }

    let reader = builder.build().context("building parquet reader")?;
    let positions = tracked.map(|tracked| tracked.finish(table, row_groups));
    let mut position_offset = 0;

    let mut output_batches = Vec::new();

    for batch_result in reader {
        let batch = batch_result.context("reading batch")?;

        if batch.num_rows() == 0 {
            continue;
        }

        // Project to output columns only
        let mut projected = project_batch(&batch, output_schema)?;
        if let (Some(name), Some(positions)) = (request.row_index_column, &positions) {
            projected = super::positions::append_positions(
                &projected,
                name,
                positions.slice(position_offset, batch.num_rows()),
            )?;
            position_offset += batch.num_rows();
        }

        output_batches.push(projected);
    }

    Ok(output_batches)
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
) -> Result<Vec<RecordBatch>> {
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
    let source_addresses = hf.source_addresses.clone();
    let first_key_set = hf.first_key_set.clone();
    let group_key_columns: Vec<String> = hf.group_key_columns.clone();
    let address_column: String = hf.address_column.clone();
    let mode = hf.mode;
    let inclusive = hf.inclusive;

    let filter_stage = Box::new(ArrowPredicateFn::new(
        filter_proj,
        move |batch: RecordBatch| {
            Ok(hierarchical_mask(
                &batch,
                &source_addresses,
                &first_key_set,
                &group_key_columns,
                &address_column,
                mode,
                inclusive,
            ))
        },
    ));

    let mut tracked = request
        .row_index_column
        .map(|_| super::positions::TrackedRows::default());
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
        let mut batch = batch_result.context("reading hierarchical batch")?;
        if batch.num_rows() == 0 {
            continue;
        }
        if let (Some(name), Some(positions)) = (request.row_index_column, &positions) {
            let count = batch.num_rows();
            batch = super::positions::append_positions(
                &batch,
                name,
                positions.slice(position_offset, count),
            )?;
            position_offset += count;
        }

        // Apply block range filter if needed
        let bounded = request.from_block.filter(|&b| b > 0).is_some() || request.to_block.is_some();
        let batch = if let Some(bn_col) = request.block_number_column.filter(|_| bounded) {
            if let Some(col) = batch.column_by_name(bn_col) {
                let br_mask = block_range_mask(col, request.from_block, request.to_block)?;
                arrow::compute::filter_record_batch(&batch, &br_mask)
                    .context("block range filter in hierarchical scan")?
            } else {
                batch
            }
        } else {
            batch
        };

        if batch.num_rows() == 0 {
            continue;
        }

        let mut projected = project_batch(&batch, output_schema)?;
        if let Some(name) = request.row_index_column {
            let positions = batch
                .column_by_name(name)
                .expect("tracked rows have positions");
            let positions = positions
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("physical positions are u64");
            projected = super::positions::append_positions(&projected, name, positions.clone())?;
        }
        output_batches.push(projected);
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
                let mut mask = BooleanArray::from(vec![true; arr.len()]);

                if let Some(from) = from_block {
                    if from > ceiling {
                        return Ok(BooleanArray::from(vec![false; arr.len()]));
                    }
                    let ge = gt_eq(&arr, &<$array>::new_scalar(from as $native))?;
                    mask = and(&mask, &ge)?;
                }

                if let Some(to) = to_block.filter(|to| *to <= ceiling) {
                    let le = lt_eq(&arr, &<$array>::new_scalar(to as $native))?;
                    mask = and(&mask, &le)?;
                }

                return Ok(mask);
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
            let mask = composite_key_in_set_mask(&batch, &columns, &selected.key_set).unwrap();
            assert_eq!(mask, BooleanArray::from(vec![false, true, true, false]));
        }
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
