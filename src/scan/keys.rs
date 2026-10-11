//! The keys a relation scan keeps its target's rows by, built from its source
//! rows and matched against a target batch without the scan knowing how they
//! are stored.

use super::addresses::{address_list, SourceAddresses};
use super::key_columns::{
    complete_keys, keys_readable, pair_keys, typed_key_columns, TypedKeyColumn,
};
use super::pairs::PairSet;
use crate::engine_err;
use crate::error::ErrorKind;
use crate::integers::IntColumn;
use anyhow::Result;
use arrow::array::builder::BooleanBufferBuilder;
use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::error::ArrowError;
use arrow::row::{RowConverter, SortField};
use rustc_hash::FxHashSet as HashSet;
use std::sync::Arc;

/// Composite-key set with an inline fast path. The common join key is two
/// integer columns (block_number + transaction_index = 16 bytes); packing it
/// into a `u128` avoids a per-key heap allocation on build and a slice hash on
/// probe. Wider or string/list keys fall back to serialized `Vec<u8>`.
pub(super) enum CompositeKeySet {
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
            Self::PairPath(s) => s.is_empty(),
            Self::Wide(s) => s.is_empty(),
            Self::Rows { values, .. } => values.is_empty(),
        }
    }
}

/// A set-based filter for join key pushdown during relation scans.
/// Filters rows to only those matching specific composite keys from a primary scan.
/// Cheap to clone into a row filter's stage: the sets are shared.
#[derive(Clone)]
pub struct KeyFilter {
    /// Column names forming the composite key (in the target/relation table).
    pub columns: Vec<String>,
    key_set: Arc<CompositeKeySet>,
    /// Sorted unique block numbers for efficient row group pruning.
    sorted_blocks: Arc<[u64]>,
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
                extract_block_numbers(column.as_ref(), None, &mut blocks);
            }
        }
        Ok(Self {
            columns: columns.to_vec(),
            key_set: Arc::new(CompositeKeySet::Rows { converter, values }),
            sorted_blocks: sorted(blocks),
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

    /// The key columns, the block column, how the keys are held and whether the
    /// block check runs first, as text that is the same in every run, for
    /// diagnostics. The keys themselves are the rows of the scan they came from.
    pub fn describe(&self) -> String {
        let held = match self.key_set.as_ref() {
            CompositeKeySet::Fixed16(_) => "pairs",
            CompositeKeySet::PairPath(_) => "pair paths",
            CompositeKeySet::Wide(_) => "bytes",
            CompositeKeySet::Rows { .. } => "rows",
        };
        let order = if self.materialization {
            ", block first"
        } else {
            ""
        };
        format!(
            "[{}] block={} as {held}{order}",
            self.columns.join(","),
            self.block_number_column
        )
    }

    /// The target table's block-number column.
    pub(super) fn block_column(&self) -> &str {
        &self.block_number_column
    }

    /// Whether a key can fall in a row group whose blocks span `[min, max]`.
    pub(super) fn has_block_within(&self, min: u64, max: u64) -> bool {
        let first = self.sorted_blocks.partition_point(|&block| block < min);
        self.sorted_blocks
            .get(first)
            .is_some_and(|&block| block <= max)
    }

    /// Whether a scan checks block bounds before the keys. Relation keys fix
    /// the blocks of the rows they pick; a row identity can hold wide strings
    /// or lists, which the bounds spare decoding.
    pub(super) fn checks_blocks_first(&self) -> bool {
        self.materialization
    }

    /// Which rows of `batch` hold one of the keys. Only `candidates` are asked.
    pub(super) fn mask(
        &self,
        batch: &RecordBatch,
        candidates: Option<&BooleanBuffer>,
    ) -> std::result::Result<BooleanArray, ArrowError> {
        composite_key_in_set_mask(batch, &self.columns, &self.key_set, candidates)
    }

    #[cfg(test)]
    pub(super) fn key_set(&self) -> &CompositeKeySet {
        &self.key_set
    }
}

/// The keys of some source rows, built once and matched against any target
/// whose key columns line up with them.
pub struct KeySet {
    key_set: Arc<CompositeKeySet>,
    /// The blocks of the source rows with a complete key, sorted and unique,
    /// for row group pruning.
    sorted_blocks: Arc<[u64]>,
    /// How many columns a key has.
    width: usize,
    /// Whether every key column was read, so that a row without a key had a
    /// null in it.
    readable: bool,
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

        let mut readable = keys_readable(primary_batches, left_keys);
        let key_set = if use_pair_path {
            for batch in primary_batches {
                if let Some(col) = batch.column_by_name(primary_bn_col) {
                    let complete = complete_keys(batch, left_keys);
                    extract_block_numbers(col.as_ref(), complete.as_ref(), &mut block_numbers);
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
                let Some((first, second, nulls)) = pair_keys(batch, left_keys) else {
                    readable = false;
                    continue;
                };
                if let Some(col) = batch.column_by_name(primary_bn_col) {
                    extract_block_numbers(col.as_ref(), nulls.as_ref(), &mut block_numbers);
                }
                let present = |row: &usize| nulls.as_ref().is_none_or(|n| n.is_valid(*row));
                pairs.extend(
                    (0..batch.num_rows())
                        .filter(present)
                        .map(|row| (first[row], second[row])),
                );
            }
            CompositeKeySet::Fixed16(PairSet::new(pairs))
        } else {
            let mut set: HashSet<Vec<u8>> = HashSet::default();
            for batch in primary_batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                if let Some(col) = batch.column_by_name(primary_bn_col) {
                    let complete = complete_keys(batch, left_keys);
                    extract_block_numbers(col.as_ref(), complete.as_ref(), &mut block_numbers);
                }
                let typed_cols = typed_key_columns(batch, left_keys);
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

        KeySet {
            key_set: Arc::new(key_set),
            sorted_blocks: sorted(block_numbers),
            width: left_keys.len(),
            readable,
        }
    }

    /// Whether no target row can hold a key: every source row has a null in
    /// its key. Not so when a key column could not be read, which the join
    /// that reads it instead reports.
    pub fn matches_nothing(&self) -> bool {
        self.readable && self.key_set.is_empty()
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

fn sorted(blocks: HashSet<u64>) -> Arc<[u64]> {
    let mut blocks: Vec<u64> = blocks.into_iter().collect();
    blocks.sort_unstable();
    blocks.into()
}

/// Add the block numbers of `col`'s rows to `out`: of every row, or of those
/// valid in `rows`.
///
/// A column that is not an integer contributes nothing, which is what it has:
/// the block-number readers that must not fail silently are the ones the
/// assembly uses, and they raise `UnsupportedKeyType` on the same input.
fn extract_block_numbers(col: &dyn Array, rows: Option<&NullBuffer>, out: &mut HashSet<u64>) {
    let Some(reader) = IntColumn::resolve(col) else {
        return;
    };

    for row in 0..reader.len() {
        if rows.is_none_or(|rows| rows.is_valid(row)) {
            out.insert(reader.block_number(row));
        }
    }
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
    let typed_cols = typed_key_columns(batch, key_columns);

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
            let mask = sources.mask(
                batch,
                &key_columns[..2],
                &key_columns[2],
                candidates,
                |sources, group, path| sources.holds(group, path),
            );
            return Ok(mask);
        }
        CompositeKeySet::Rows { .. } => unreachable!(),
    }

    Ok(BooleanArray::new(builder.finish(), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, ListArray, UInt32Array, UInt64Array};

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
            let mask = selected.mask(&batch, None).unwrap();
            assert_eq!(mask, BooleanArray::from(vec![false, true, true, false]));
        }
    }

    /// A row group can hold a key exactly when one of the keys' blocks falls
    /// inside its bounds, at either edge or between them.
    #[test]
    fn a_row_group_holds_a_key_when_a_key_block_is_inside_its_bounds() {
        let blocks: Vec<u64> = vec![10, 20, 30];
        let source = RecordBatch::try_from_iter([
            (
                "block_number",
                Arc::new(UInt64Array::from(blocks.clone())) as ArrayRef,
            ),
            (
                "index",
                Arc::new(UInt32Array::from(vec![0, 0, 0])) as ArrayRef,
            ),
        ])
        .unwrap();
        let keys = ["block_number", "index"];
        let filter = KeyFilter::build(&[source], &keys, &keys, "block_number", "block_number");

        for min in 0..40u64 {
            for max in min..40 {
                let expected = blocks.iter().any(|&block| (min..=max).contains(&block));
                assert_eq!(filter.has_block_within(min, max), expected, "{min}..={max}");
            }
        }
    }

    /// Source rows in every key layout: two integers, two integers and a path,
    /// and anything else as bytes. Row 0 is in block 10 with a complete key;
    /// row 1, in block 20, has a null in the key column `null_in`.
    fn keyed_rows(null_in: usize, columns: &[ArrayRef]) -> RecordBatch {
        let mut named = vec![(
            "block_number".to_owned(),
            Arc::new(UInt64Array::from(vec![10, 20])) as ArrayRef,
        )];
        for (i, column) in columns.iter().enumerate() {
            let column = match i == null_in {
                true => {
                    arrow::compute::nullif(column, &BooleanArray::from(vec![false, true])).unwrap()
                }
                false => column.clone(),
            };
            named.push((format!("k{i}"), column));
        }
        RecordBatch::try_from_iter(named).unwrap()
    }

    fn layouts() -> Vec<(&'static str, Vec<ArrayRef>)> {
        use arrow::datatypes::UInt32Type;
        let path = || -> ArrayRef {
            Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(vec![
                Some(vec![Some(0)]),
                Some(vec![Some(1)]),
            ]))
        };
        let ints = || -> ArrayRef { Arc::new(UInt32Array::from(vec![1, 2])) };
        let text = || -> ArrayRef { Arc::new(arrow::array::StringArray::from(vec!["a", "b"])) };
        vec![
            ("pairs", vec![ints(), ints()]),
            ("pair paths", vec![ints(), ints(), path()]),
            ("bytes", vec![text()]),
            ("bytes", vec![ints(), text(), ints()]),
        ]
    }

    fn key_names(columns: &[ArrayRef]) -> Vec<String> {
        (0..columns.len()).map(|i| format!("k{i}")).collect()
    }

    /// A source row whose key has a null relates to no target row, so its
    /// block keeps no row group in a scan, in every layout and whichever
    /// column the null is in.
    #[test]
    fn a_row_without_a_key_keeps_no_row_group() {
        for (layout, columns) in layouts() {
            let names = key_names(&columns);
            let keys: Vec<&str> = names.iter().map(String::as_str).collect();
            for null_in in 0..columns.len() {
                let rows = keyed_rows(null_in, &columns);
                let set = KeySet::build(&[rows], &keys, "block_number");
                let filter = set.filter(&keys, "block_number");

                assert!(filter.describe().ends_with(layout), "{}", filter.describe());
                assert!(
                    filter.has_block_within(10, 10),
                    "{layout}: the complete key"
                );
                assert!(
                    !filter.has_block_within(20, 20),
                    "{layout}, null in k{null_in}: a block of null keys"
                );
                assert!(!set.matches_nothing(), "{layout}: one key is complete");
            }
        }
    }

    /// Only nulls: nothing to match. A key column that is missing, or of a
    /// type no key is read from, is not that, however null its values.
    #[test]
    fn only_null_keys_match_nothing_and_unreadable_ones_are_not_null() {
        use arrow::datatypes::Float64Type;
        for (layout, columns) in layouts() {
            let names = key_names(&columns);
            let keys: Vec<&str> = names.iter().map(String::as_str).collect();
            let rows = |columns: &[ArrayRef]| {
                let rows = keyed_rows(0, columns);
                let nulls = BooleanArray::from(vec![true, true]);
                let k0 = arrow::compute::nullif(rows.column(1), &nulls).unwrap();
                let mut all: Vec<ArrayRef> = rows.columns().to_vec();
                all[1] = k0;
                RecordBatch::try_new(rows.schema(), all).unwrap()
            };

            let null = rows(&columns);
            let empty = null.slice(0, 0);
            let set = KeySet::build(&[empty, null.clone()], &keys, "block_number");
            assert!(set.matches_nothing(), "{layout}: every key has a null");

            let floats: ArrayRef = Arc::new(arrow::array::Float64Array::from(vec![None, None]));
            let float_paths: ArrayRef =
                Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                    None::<Vec<Option<f64>>>,
                    None,
                ]));
            for unreadable in [floats, float_paths] {
                let mut retyped = columns.clone();
                *retyped.last_mut().unwrap() = unreadable.clone();
                let set = KeySet::build(&[rows(&retyped)], &keys, "block_number");
                assert!(
                    !set.matches_nothing(),
                    "{layout}: a {} key column",
                    unreadable.data_type()
                );
            }

            let missing = null.project(&(0..null.num_columns() - 1).collect::<Vec<_>>());
            let set = KeySet::build(&[missing.unwrap()], &keys, "block_number");
            assert!(!set.matches_nothing(), "{layout}: a missing key column");
        }

        // Pairs, as the first batch says, and then a batch that holds none.
        let ints: ArrayRef = Arc::new(UInt32Array::from(vec![1, 2]));
        let text: ArrayRef = Arc::new(arrow::array::StringArray::from(vec!["a", "b"]));
        let null_pairs = keyed_rows(0, &[ints.clone(), ints.clone()]).slice(1, 1);
        let text_keys = keyed_rows(9, &[ints, text]);
        let set = KeySet::build(&[null_pairs, text_keys], &["k0", "k1"], "block_number");
        assert!(
            set.key_set.is_empty() && !set.matches_nothing(),
            "keys not read as pairs"
        );
    }

    /// Diagnostics tell apart key filters that differ in their block column, in
    /// how they hold the keys or in whether the block check runs first.
    #[test]
    fn describe_names_the_columns_the_block_column_and_the_key_form() {
        let batch = RecordBatch::try_from_iter([
            (
                "number",
                Arc::new(UInt64Array::from(vec![7, 8])) as ArrayRef,
            ),
            ("index", Arc::new(UInt32Array::from(vec![0, 1])) as ArrayRef),
            (
                "name",
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let pair = ["number", "index"];
        let named = ["number", "name"];
        let build = |keys: &[&str], block: &str| {
            KeyFilter::build(std::slice::from_ref(&batch), keys, keys, "number", block).describe()
        };

        assert_eq!(
            build(&pair, "number"),
            "[number,index] block=number as pairs"
        );
        assert_eq!(build(&pair, "block"), "[number,index] block=block as pairs");
        assert_eq!(
            build(&named, "number"),
            "[number,name] block=number as bytes"
        );

        let columns: Vec<String> = pair.iter().map(|c| c.to_string()).collect();
        let rows = KeyFilter::for_rows(std::slice::from_ref(&batch), &columns, "number").unwrap();
        assert_eq!(
            rows.describe(),
            "[number,index] block=number as pairs, block first"
        );
    }
}
