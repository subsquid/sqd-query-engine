use crate::integers::{BlockNumbers, IntColumn};
use crate::metadata::{TableDescription, WeightSource};
use crate::output::projection::{push_unique, row_key, ResolvedProjection};
use crate::scan::Rows;
use crate::text::StringColumn;
use anyhow::Result;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Maximum response size in bytes (20 MB).
/// `P-WEIGHT-BUDGET` of `spec/09-parameters.md`: the default every caller gets.
/// The value in force is [`crate::output::ExecOptions::weight_budget`] — reading
/// the constant instead of the field is how a stage comes to enforce a budget
/// the rest of the query is not using.
pub(crate) const MAX_RESPONSE_BYTES: u64 = 20 * 1024 * 1024;

/// Default weight per row when no weight column is specified.
const DEFAULT_ROW_WEIGHT: u64 = 32;

/// Table data: the query-driven items and any relation results.
#[derive(Default)]
pub(crate) struct TableOutput {
    /// The primary table's filtered rows.
    pub(crate) rows: Rows,
    /// Relation results, keyed by the relation's position in the table plan.
    /// Two relations of one query item can name the same target table — and then
    /// the same rows — so each keeps its own entry and the overlap is resolved by
    /// deduplication rather than by concatenation.
    pub(crate) relations: HashMap<usize, Rows>,
}

/// What a row weighs (§5.4): a fixed part, and the size columns that add the
/// rest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowWeight<'a> {
    pub(crate) fixed: u64,
    /// One entry per column weighed by its size, so a size column two columns
    /// share is charged for each.
    pub(crate) columns: Vec<&'a str>,
}

impl<'a> RowWeight<'a> {
    /// What a row weighs when it renders `rendered`: its weight key, selected
    /// or not, and the rendered columns.
    pub(crate) fn of(rendered: &[&'a str], desc: &'a TableDescription) -> Self {
        // The item key repeats the address column when it orders by it too.
        let mut charged = Vec::new();
        push_unique(&mut charged, desc.weight_key());
        push_unique(&mut charged, rendered.iter().copied());
        Self::of_columns(&charged, desc)
    }

    /// What `columns`, each once, weigh. The names are columns, never fields:
    /// reading one as a field again charges a key column as whatever field
    /// shares its name.
    fn of_columns(columns: &[&str], desc: &'a TableDescription) -> Self {
        let mut weight = Self {
            fixed: 0,
            columns: Vec::new(),
        };

        for &column in columns {
            let Some(declared) = desc.columns.get(column) else {
                continue;
            };
            if declared.system {
                continue;
            }

            match &declared.weight {
                Some(WeightSource::Column(size)) => weight.columns.push(size),
                Some(WeightSource::Fixed(fixed)) => {
                    weight.fixed = weight.fixed.saturating_add(*fixed);
                }
                None => weight.fixed = weight.fixed.saturating_add(DEFAULT_ROW_WEIGHT),
            }
        }

        weight
    }
}

/// One source of a table's rows: the table's own scan or a relation into it.
pub(crate) struct WeighedRows<'r> {
    /// The fields the source renders, which decide what its rows weigh.
    pub(crate) projection: &'r ResolvedProjection<'r>,
    pub(crate) rows: &'r Rows,
}

/// Rows and what each of them weighs.
struct WeightContribution<'r> {
    rows: &'r Rows,
    weight: &'r RowWeight<'r>,
}

/// Compute exact per-block weights after merging all table contributions.
///
/// Weight is computed per target table with row deduplication, matching legacy behavior:
/// - Direct scan results and relation results targeting the same table are merged
/// - Duplicate rows (same block_number + item_order_keys) are counted only once
///
/// Every block of `candidates` also carries its header, whether or not it holds
/// items: a boundary block is on the page like any other (§5.5).
pub(crate) fn compute_block_weights(
    sources: &[WeighedRows<'_>],
    header: &ResolvedProjection,
    header_batches: &[RecordBatch],
    candidates: &[u64],
) -> Result<FxHashMap<u64, u64>> {
    // 1. Group the sources by the table their rows are of.
    let mut tables: Vec<(&str, Vec<&WeighedRows>)> = Vec::new();
    for source in sources {
        let table = source.projection.table();
        match tables.iter_mut().find(|(name, _)| *name == table) {
            Some((_, group)) => group.push(source),
            None => tables.push((table, vec![source])),
        }
    }

    // 2. For each table, compute deduplicated per-block weights.
    let mut block_weights: FxHashMap<u64, u64> = FxHashMap::default();

    for (_, contribs) in &tables {
        let table_desc = contribs[0].projection.desc();
        let bn_col_name = table_desc.block_number_column.as_str();

        if let [source] = contribs.as_slice() {
            // Single source — no dedup needed, fast path.
            accumulate_block_weights(
                source.rows.batches(),
                bn_col_name,
                source.projection.weight(),
                &mut block_weights,
            );
        } else {
            // The same key the row writer dedups on. `item_order_keys` alone is
            // a prefix of it for any table with an address column: every EVM
            // trace of one transaction shares its `transaction_index`, so all of
            // them would collapse to a single counted row while all of them are
            // emitted, and the response overshoots the cap by the trace count
            // (INV-B6/B10).
            let dedup_keys = &row_key(table_desc)[1..];
            let contributions: Vec<_> = contribs
                .iter()
                .map(|source| WeightContribution {
                    rows: source.rows,
                    weight: source.projection.weight(),
                })
                .collect();
            accumulate_dedup_contributions(
                &contributions,
                bn_col_name,
                dedup_keys,
                &mut block_weights,
            )?;
        }
    }

    // 3. Add block header weight.
    let RowWeight {
        fixed: header_fixed,
        columns: header_weight_cols,
    } = header.weight();
    let header_bn_col = header.desc().block_number_column.as_str();
    let candidates: FxHashSet<u64> = candidates.iter().copied().collect();

    for batch in header_batches {
        let bn_col = match batch.column_by_name(header_bn_col) {
            Some(c) => c,
            None => continue,
        };
        // A header's weight is its fixed part plus whatever its data-dependent
        // columns say. Counting only the fixed part makes a header carrying
        // withdrawals weigh the same as an empty one.
        let weight_columns = weight_columns(batch, header_weight_cols);

        // The scan refuses a chunk whose block-number column cannot place a
        // row, so a batch arriving here without a readable one did not come
        // from one. Weighing nothing for it runs the response large rather
        // than short, which is the direction INV-B9 tolerates.
        let Ok(blocks) = BlockNumbers::resolve(bn_col.as_ref(), header_bn_col) else {
            continue;
        };

        for i in 0..batch.num_rows() {
            let block_num = blocks.at(i);

            if !candidates.contains(&block_num) {
                continue;
            }

            let dynamic: u64 = weight_columns
                .iter()
                .map(|column| weight_at(column, i))
                .fold(0u64, u64::saturating_add);

            let entry = block_weights.entry(block_num).or_default();
            *entry = entry.saturating_add(header_fixed.saturating_add(dynamic));
        }
    }

    Ok(block_weights)
}

/// Select a weighted prefix across disjoint, complete ranges of blocks.
pub(crate) struct BlockSelection {
    budget: u64,
    cumulative: u64,
    blocks: Vec<u64>,
    /// What each of `blocks` weighs.
    weights: Vec<u64>,
}

impl BlockSelection {
    pub(crate) fn new(budget: u64) -> Self {
        Self {
            budget,
            cumulative: 0,
            blocks: Vec::new(),
            weights: Vec::new(),
        }
    }

    /// Returns true once the budget is exceeded, including by the first block.
    pub(crate) fn extend(&mut self, blocks: &[u64], weights: &FxHashMap<u64, u64>) -> bool {
        for &block in blocks {
            let weight = weights.get(&block).copied().unwrap_or(0);
            self.cumulative = self.cumulative.saturating_add(weight);
            if self.blocks.is_empty() || self.cumulative <= self.budget {
                self.blocks.push(block);
                self.weights.push(weight);
            }
            if self.cumulative > self.budget {
                return true;
            }
        }
        false
    }

    /// The selected blocks, and what each weighs.
    pub(crate) fn into_blocks(self) -> (Vec<u64>, Vec<u64>) {
        (self.blocks, self.weights)
    }

    pub(crate) fn blocks(&self) -> &[u64] {
        &self.blocks
    }

    /// What every block offered so far weighs together.
    pub(crate) fn weight(&self) -> u64 {
        self.cumulative
    }
}

/// Suggest the first range through the block that crosses the estimated budget.
/// Including that block lets exact selection observe the overflow in this read.
pub(crate) fn weight_range_end(
    narrow_batches: &[RecordBatch],
    projection: &ResolvedProjection,
    budget: u64,
) -> Option<u64> {
    let mut weights = FxHashMap::default();
    accumulate_block_weights(
        narrow_batches,
        &projection.desc().block_number_column,
        projection.weight(),
        &mut weights,
    );
    let mut blocks: Vec<_> = weights.into_iter().collect();
    blocks.sort_unstable_by_key(|&(block, _)| block);
    let mut cumulative = 0u64;
    for (block, weight) in blocks {
        cumulative = cumulative.saturating_add(weight);
        if cumulative > budget {
            return Some(block);
        }
    }
    None
}

/// The weight columns of `batch`, resolved once for all its rows.
///
/// A width this cannot read weighs nothing, and that is the one to watch:
/// `*_size` companions are narrowed like any other integer, and a weight model
/// blind to `UInt16` charges a whole chunk's rows their fixed part alone.
fn weight_columns<'b>(batch: &'b RecordBatch, names: &[&str]) -> Vec<IntColumn<'b>> {
    names
        .iter()
        .filter_map(|name| IntColumn::resolve(batch.column_by_name(name)?.as_ref()))
        .collect()
}

/// A weight column's value at `row`.
///
/// A size is unsigned by the catalog, so a negative one is a corrupt chunk. Read
/// as a `u64` it becomes ~`u64::MAX`, which drops the block from every response
/// it appears in — or wraps the accumulator and drops the budget instead. It
/// weighs nothing here: a model that under-counts one row emits a slightly large
/// response, and a model that over-counts it by 2⁶⁴ emits nothing (INV-B9).
#[inline]
fn weight_at(column: &IntColumn, row: usize) -> u64 {
    if column.is_null(row) {
        return 0;
    }

    column.value(row).try_into().unwrap_or(0)
}

/// Accumulate per-block weights in a single pass (no dedup, fast path).
pub(crate) fn accumulate_block_weights(
    batches: &[RecordBatch],
    bn_column: &str,
    weight: &RowWeight,
    weights: &mut FxHashMap<u64, u64>,
) {
    for batch in batches {
        let bn_col = match batch.column_by_name(bn_column) {
            Some(c) => c,
            None => continue,
        };

        let resolved = weight_columns(batch, &weight.columns);

        // The scan refuses a chunk whose block-number column cannot place a row,
        // so a batch arriving here without a readable one did not come from one.
        // Weighing nothing for it runs the response large rather than short, which
        // is the direction INV-B9 tolerates.
        let Ok(blocks) = BlockNumbers::resolve(bn_col.as_ref(), bn_column) else {
            continue;
        };

        for i in 0..batch.num_rows() {
            let block_num = blocks.at(i);

            let mut row_weight = weight.fixed;
            for column in &resolved {
                row_weight = row_weight.saturating_add(weight_at(column, i));
            }

            let entry = weights.entry(block_num).or_default();
            *entry = entry.saturating_add(row_weight);
        }
    }
}

/// Accumulate per-block weights with row deduplication.
///
/// A row is its physical position when the scan recorded one, as the reference
/// dedups by row index; otherwise it is its block number and item-key values.
/// Sources that recorded positions read no keys, so they cannot be compared
/// with sources that did not.
fn accumulate_dedup_contributions(
    contributions: &[WeightContribution<'_>],
    bn_column: &str,
    key_columns: &[&str],
    weights: &mut FxHashMap<u64, u64>,
) -> Result<()> {
    let sources: Vec<_> = contributions
        .iter()
        .filter(|source| !source.rows.batches().is_empty())
        .collect();
    let positioned = sources
        .iter()
        .filter(|source| source.rows.positions().is_some())
        .count();

    if positioned == sources.len() {
        let mut seen = FxHashSet::default();
        for source in sources {
            let positions = source.rows.positions().expect("every source has positions");
            for (batch, positions) in source.rows.batches().iter().zip(positions) {
                accumulate_rows(source, batch, bn_column, weights, |row, _| {
                    seen.insert(positions.value(row))
                });
            }
        }
        return Ok(());
    }
    anyhow::ensure!(
        positioned == 0,
        "rows with positions and rows without cannot be deduplicated together"
    );

    let batches: Vec<_> = sources
        .iter()
        .flat_map(|source| {
            source
                .rows
                .batches()
                .iter()
                .map(move |batch| (*source, batch))
        })
        .collect();
    let keys: Vec<Vec<Option<&dyn Array>>> = batches
        .iter()
        .map(|(_, batch)| {
            key_columns
                .iter()
                .map(|name| batch.column_by_name(name).map(|c| c.as_ref()))
                .collect()
        })
        .collect();
    let mut seen = FxHashSet::default();
    for ((source, batch), columns) in batches.iter().zip(&keys) {
        accumulate_rows(source, batch, bn_column, weights, |row, block| {
            seen.insert((block, WeightRow { columns, row }))
        });
    }
    Ok(())
}

/// Add the weight of every row of `batch` that `is_new` admits to its block.
fn accumulate_rows(
    source: &WeightContribution<'_>,
    batch: &RecordBatch,
    bn_column: &str,
    weights: &mut FxHashMap<u64, u64>,
    mut is_new: impl FnMut(usize, u64) -> bool,
) {
    let Some(numbers) = batch.column_by_name(bn_column) else {
        return;
    };
    let Ok(blocks) = BlockNumbers::resolve(numbers.as_ref(), bn_column) else {
        return;
    };
    let weight = source.weight;
    let weight_columns = weight_columns(batch, &weight.columns);

    for row in 0..batch.num_rows() {
        let block = blocks.at(row);
        if !is_new(row, block) {
            continue;
        }

        let weight = weight_columns.iter().fold(weight.fixed, |sum, column| {
            sum.saturating_add(weight_at(column, row))
        });
        let total = weights.entry(block).or_default();
        *total = total.saturating_add(weight);
    }
}

/// Hash collisions must not change a block's weight or its page boundary.
struct WeightRow<'a> {
    columns: &'a [Option<&'a dyn Array>],
    row: usize,
}

impl Hash for WeightRow<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for column in self.columns {
            match column {
                Some(column) => hash_array_value(*column, self.row, state),
                None => 0u8.hash(state),
            }
        }
    }
}

impl PartialEq for WeightRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.columns.len() == other.columns.len()
            && self
                .columns
                .iter()
                .zip(other.columns)
                .all(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => equal_key_value(*a, self.row, *b, other.row),
                    (None, None) => true,
                    _ => false,
                })
    }
}
impl Eq for WeightRow<'_> {}

fn equal_key_value(a: &dyn Array, ai: usize, b: &dyn Array, bi: usize) -> bool {
    if a.is_null(ai) || b.is_null(bi) {
        return a.is_null(ai) && b.is_null(bi);
    }
    if let (Some(a), Some(b)) = (IntColumn::resolve(a), IntColumn::resolve(b)) {
        return a.value(ai) == b.value(bi);
    }
    if let (Some(a), Some(b)) = (StringColumn::resolve(a), StringColumn::resolve(b)) {
        return a.value(ai) == b.value(bi);
    }
    if let (Some(a), Some(b)) = (
        a.as_any().downcast_ref::<GenericListArray<i32>>(),
        b.as_any().downcast_ref::<GenericListArray<i32>>(),
    ) {
        let a = a.value(ai);
        let b = b.value(bi);
        return a.len() == b.len()
            && (0..a.len()).all(|i| equal_key_value(a.as_ref(), i, b.as_ref(), i));
    }
    a.slice(ai, 1).to_data() == b.slice(bi, 1).to_data()
}

/// Hash a single array value for row dedup.
///
/// Integers hash by value rather than by stored width, so a column narrowed in
/// one source and not in another still dedups against itself.
fn hash_array_value(col: &dyn arrow::array::Array, row: usize, hasher: &mut impl Hasher) {
    if col.is_null(row) {
        0xFFu8.hash(hasher);
        return;
    }

    if let Some(ints) = crate::integers::IntColumn::resolve(col) {
        ints.value(row).hash(hasher);
    } else if let Some(a) = StringColumn::resolve(col) {
        a.value(row).hash(hasher);
    } else if let Some(a) = col.as_any().downcast_ref::<GenericListArray<i32>>() {
        let values = a.value(row);
        values.len().hash(hasher);
        for i in 0..values.len() {
            hash_array_value(values.as_ref(), i, hasher);
        }
    } else {
        // Unknown type — use a sentinel to distinguish from null
        0xFEu8.hash(hasher);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{parse_dataset_description, DatasetDescription, VirtualField};

    /// Two rows the key hash cannot tell apart are still two rows.
    ///
    /// `(tx = 0, trace_address = [0])` and `(tx = 1, trace_address = [])` hash
    /// equal: FxHash starts at zero and absorbs a leading zero word, so the first
    /// row's `tx` leaves the state where it found it and the two write the same
    /// sequence from there. Whether a *set* of them merges is a second question —
    /// the key it hashes is `(block, row)`, and the block number ahead of the
    /// columns seeds the state, which is what pulls this pair apart today. That is
    /// masking, not distinguishing: it holds for this pair and says nothing about
    /// the next one. What separates two rows is comparing them, so that is what
    /// is asserted, on a pair whose hashes are equal by construction.
    #[test]
    fn a_row_the_key_hash_cannot_tell_apart_is_still_a_row() {
        use arrow::datatypes::Int32Type;
        use rustc_hash::FxHasher;

        let addresses =
            ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(0)]), Some(vec![])]);
        let indexes = UInt32Array::from(vec![0u32, 1]);
        let columns: Vec<Option<&dyn arrow::array::Array>> = vec![Some(&indexes), Some(&addresses)];
        let row = |row| WeightRow {
            columns: &columns,
            row,
        };

        let hash_of = |r: WeightRow| {
            let mut hasher = FxHasher::default();
            r.hash(&mut hasher);
            hasher.finish()
        };

        assert_eq!(
            hash_of(row(0)),
            hash_of(row(1)),
            "the pair this is about no longer collides, so it no longer asks \
             anything — find one that does before deleting the comparison"
        );
        assert!(
            row(0) != row(1),
            "two distinct rows compared equal, so a set of them counts one weight \
             for two rows and the response overshoots the budget it was cut to"
        );
    }

    #[test]
    fn colliding_keys_are_charged_separately_and_duplicate_rows_once() {
        use arrow::datatypes::{DataType, Field, Int32Type, Schema};
        use std::sync::Arc;

        let addresses =
            ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(0)]), Some(vec![])]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("trace_address", addresses.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![100, 100])),
                Arc::new(UInt32Array::from(vec![0, 1])),
                Arc::new(addresses),
            ],
        )
        .unwrap();
        let primary = Rows::new(vec![batch.clone()]);
        let duplicate = Rows::new(vec![batch]);
        let weight = RowWeight {
            fixed: 100,
            columns: vec![],
        };
        for related in [&Rows::default(), &duplicate] {
            let contributions = [
                WeightContribution {
                    rows: &primary,
                    weight: &weight,
                },
                WeightContribution {
                    rows: related,
                    weight: &weight,
                },
            ];
            let mut weights = FxHashMap::default();
            accumulate_dedup_contributions(
                &contributions,
                "block_number",
                &["transaction_index", "trace_address"],
                &mut weights,
            )
            .unwrap();
            assert_eq!(weights[&100], 200);
        }
    }

    /// A physical row is charged once however many sources return it, and two
    /// rows are two rows even when their keys are equal. Rows without positions
    /// are compared by their keys, and the two kinds never meet.
    #[test]
    fn positions_identify_rows_when_every_source_has_them() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let rows = |positions: Option<Vec<u64>>| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("block_number", DataType::UInt64, false),
                Field::new("transaction_index", DataType::UInt32, false),
            ]));
            let columns: Vec<ArrayRef> = vec![
                Arc::new(UInt64Array::from(vec![100, 100])),
                Arc::new(UInt32Array::from(vec![7, 7])),
            ];
            let batch = RecordBatch::try_new(schema, columns).unwrap();
            match positions {
                Some(positions) => {
                    Rows::with_positions(vec![batch], vec![UInt64Array::from(positions)]).unwrap()
                }
                None => Rows::new(vec![batch]),
            }
        };
        let weight = RowWeight {
            fixed: 10,
            columns: vec![],
        };
        let weigh = |primary: Rows, related: Rows| {
            let contributions = [
                WeightContribution {
                    rows: &primary,
                    weight: &weight,
                },
                WeightContribution {
                    rows: &related,
                    weight: &weight,
                },
            ];
            let mut weights = FxHashMap::default();
            accumulate_dedup_contributions(
                &contributions,
                "block_number",
                &["transaction_index"],
                &mut weights,
            )
            .map(|()| weights[&100])
        };

        let positioned = |a, b| weigh(rows(Some(a)), rows(Some(b))).unwrap();
        assert_eq!(positioned(vec![3, 4], vec![4, 3]), 20);
        assert_eq!(positioned(vec![3, 4], vec![5, 6]), 40);

        assert_eq!(weigh(rows(None), rows(None)).unwrap(), 10);
        assert_eq!(weigh(rows(Some(vec![3, 4])), Rows::default()).unwrap(), 20);
        assert!(weigh(rows(Some(vec![3, 4])), rows(None)).is_err());
    }

    /// Load the solana metadata for tests.
    fn solana_meta() -> DatasetDescription {
        let yaml = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("metadata/solana.yaml"),
        )
        .unwrap();
        parse_dataset_description(&yaml).unwrap()
    }

    /// Load the evm metadata for tests.
    fn evm_meta() -> DatasetDescription {
        let yaml = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("metadata/evm.yaml"),
        )
        .unwrap();
        parse_dataset_description(&yaml).unwrap()
    }

    fn substrate_meta() -> DatasetDescription {
        let yaml = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("metadata/substrate.yaml"),
        )
        .unwrap();
        parse_dataset_description(&yaml).unwrap()
    }

    /// Helper: what `fields` weigh without the weight key.
    fn weight_for(fields: &[&str], table_desc: Option<&TableDescription>) -> (u64, Vec<String>) {
        let desc = table_desc.unwrap();
        let mut columns = Vec::new();
        for field in fields {
            push_unique(&mut columns, desc.field_columns(field));
        }

        owned(RowWeight::of_columns(&columns, desc))
    }

    /// Helper: what a row weighs with `selected` output columns.
    fn weight_of(selected: &[&str], table_desc: Option<&TableDescription>) -> (u64, Vec<String>) {
        let selected: Vec<String> = selected.iter().map(|s| s.to_string()).collect();
        let projection =
            ResolvedProjection::resolve("table", table_desc.unwrap(), &selected).unwrap();

        owned(projection.weight().clone())
    }

    fn owned(weight: RowWeight) -> (u64, Vec<String>) {
        let columns = weight.columns.iter().map(|c| c.to_string()).collect();
        (weight.fixed, columns)
    }

    // -----------------------------------------------------------------------
    // Tests: RowWeight — documents what columns contribute to weight
    // -----------------------------------------------------------------------

    /// Covers CT-1 · INV-D9
    #[test]
    fn test_system_columns_excluded_from_weight() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        for col in ["data_size", "accounts_size", "accounts_bloom", "b9"] {
            let (fixed, dynamic) = weight_for(&[col], Some(instr));
            assert_eq!(
                fixed, 0,
                "system column '{}' must not contribute weight",
                col
            );
            assert!(dynamic.is_empty());
        }
    }

    /// A column read by a filter is not thereby an internal column. The
    /// selectable discriminator prefixes are emitted, so they must be weighed;
    /// counting them as zero under-weighs a block and truncates later than the
    /// cap allows (INV-B10).
    #[test]
    fn test_selectable_discriminator_columns_are_weighed() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        for col in ["d1", "d2", "d4", "d8"] {
            let (fixed, dynamic) = weight_for(&[col], Some(instr));
            assert_eq!(fixed, 32, "'{}' is selectable and must be weighed", col);
            assert!(dynamic.is_empty());
        }
    }

    #[test]
    fn test_fixed_weight_columns() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // program_id: no explicit weight → default 32
        let (fixed, dynamic) = weight_for(&["program_id"], Some(instr));
        assert_eq!(fixed, 32);
        assert!(dynamic.is_empty());

        // is_committed: boolean, no explicit weight → default 32
        let (fixed, dynamic) = weight_for(&["is_committed"], Some(instr));
        assert_eq!(fixed, 32);
        assert!(dynamic.is_empty());

        // a1-a15: explicit weight=0
        let (fixed, dynamic) = weight_for(&["a1"], Some(instr));
        assert_eq!(fixed, 0, "a1 has weight=0");
        assert!(dynamic.is_empty());

        let (fixed, _) = weight_for(&["a5"], Some(instr));
        assert_eq!(fixed, 0, "a5 has weight=0");

        let (fixed, _) = weight_for(&["rest_accounts"], Some(instr));
        assert_eq!(fixed, 0, "rest_accounts has weight=0");
    }

    #[test]
    fn test_dynamic_weight_columns() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // data: weight = data_size (dynamic column)
        let (fixed, dynamic) = weight_for(&["data"], Some(instr));
        assert_eq!(fixed, 0, "data has no fixed weight");
        assert_eq!(dynamic, vec!["data_size"]);

        // a0: weight = accounts_size (dynamic column)
        let (fixed, dynamic) = weight_for(&["a0"], Some(instr));
        assert_eq!(fixed, 0);
        assert_eq!(dynamic, vec!["accounts_size"]);
    }

    #[test]
    fn test_virtual_field_accounts_weight() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // "accounts" is a virtual roll → [a0, a1, ..., a15, rest_accounts]
        // a0 → accounts_size (dynamic), a1-a15 + rest_accounts → weight=0
        let (fixed, dynamic) = weight_for(&["accounts"], Some(instr));
        assert_eq!(
            fixed, 0,
            "all account columns have weight=0 except a0 which is dynamic"
        );
        assert_eq!(dynamic, vec!["accounts_size"]);
    }

    #[test]
    fn test_evm_logs_weight() {
        let meta = evm_meta();
        let logs = meta.table("logs").unwrap();

        // data → data_size (dynamic)
        let (fixed, dynamic) = weight_for(&["data"], Some(logs));
        assert_eq!(fixed, 0);
        assert_eq!(dynamic, vec!["data_size"]);

        // address: no explicit weight → default 32
        let (fixed, dynamic) = weight_for(&["address"], Some(logs));
        assert_eq!(fixed, 32);
        assert!(dynamic.is_empty());

        // topics = roll(topic0, topic1, topic2, topic3), each → default 32
        let (fixed, dynamic) = weight_for(&["topics"], Some(logs));
        assert_eq!(fixed, 4 * 32, "4 topic columns × 32 bytes each");
        assert!(dynamic.is_empty());
    }

    #[test]
    fn test_evm_traces_weight() {
        let meta = evm_meta();
        let traces = meta.table("traces").unwrap();

        // call_input: dynamic weight
        let (fixed, dynamic) = weight_for(&["call_input"], Some(traces));
        assert_eq!(fixed, 0);
        assert_eq!(dynamic, vec!["call_input_size"]);

        // call_result_output: dynamic weight
        let (fixed, dynamic) = weight_for(&["call_result_output"], Some(traces));
        assert_eq!(fixed, 0);
        assert_eq!(dynamic, vec!["call_result_output_size"]);
    }

    #[test]
    fn test_evm_blocks_weight() {
        let meta = evm_meta();
        let blocks = meta.table("blocks").unwrap();

        // logs_bloom: weight=512 (matching legacy set_weight("logs_bloom", 512))
        let (fixed, dynamic) = weight_for(&["logs_bloom"], Some(blocks));
        assert_eq!(fixed, 512, "logs_bloom has fixed weight 512");
        assert!(dynamic.is_empty());

        // extra_data: weight = extra_data_size (dynamic)
        let (fixed, dynamic) = weight_for(&["extra_data"], Some(blocks));
        assert_eq!(fixed, 0);
        assert_eq!(dynamic, vec!["extra_data_size"]);
    }

    #[test]
    fn test_combined_weight_multiple_columns() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // Combining multiple columns: program_id(32) + data(data_size) + is_committed(32)
        let (fixed, dynamic) = weight_for(&["program_id", "data", "is_committed"], Some(instr));
        assert_eq!(fixed, 64, "program_id(32) + is_committed(32)");
        assert_eq!(dynamic, vec!["data_size"]);
    }

    // -----------------------------------------------------------------------
    // Tests: row weight (weight key + selected columns)
    // -----------------------------------------------------------------------

    #[test]
    fn test_weight_projection_solana_instructions_basic() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // Weight key: [block_number, transaction_index, instruction_address]
        // User output: [transaction_index, instruction_address, program_id, accounts, data]
        let (fixed, dynamic) = weight_of(
            &[
                "transaction_index",
                "instruction_address",
                "program_id",
                "accounts",
                "data",
            ],
            Some(instr),
        );

        // block_number: default 32 (not system)
        // transaction_index: default 32
        // instruction_address: default 32
        // program_id: default 32
        // accounts → roll: a0(accounts_size) + a1..a15(0) + rest_accounts(0)
        // data → data_size
        assert_eq!(fixed, 4 * 32, "4 non-system non-dynamic columns × 32");
        assert!(dynamic.contains(&"data_size".to_string()));
        assert!(dynamic.contains(&"accounts_size".to_string()));
        assert_eq!(dynamic.len(), 2);
    }

    #[test]
    fn test_weight_projection_solana_instructions_with_extra_fields() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // Same as above but the user also requests d1, d8 and is_committed —
        // all three are selectable, so all three are weighed.
        let (fixed, dynamic) = weight_of(
            &[
                "transaction_index",
                "instruction_address",
                "program_id",
                "accounts",
                "data",
                "d1",
                "d8",
                "is_committed",
            ],
            Some(instr),
        );

        // block_number, transaction_index, instruction_address, program_id,
        // is_committed, d1, d8 — accounts rolls to 0 and data is dynamic.
        assert_eq!(fixed, 7 * 32, "7 non-system non-dynamic columns × 32");
        assert_eq!(dynamic.len(), 2);
    }

    #[test]
    fn test_weight_projection_evm_logs_basic() {
        let meta = evm_meta();
        let logs = meta.table("logs").unwrap();

        // Weight key: [block_number, log_index]
        // User output: [log_index, transaction_index, address, data, topics]
        let (fixed, dynamic) = weight_of(
            &[
                "log_index",
                "transaction_index",
                "address",
                "data",
                "topics",
            ],
            Some(logs),
        );

        // block_number(32) + log_index(32) + transaction_index(32) + address(32) +
        // topic0(32) + topic1(32) + topic2(32) + topic3(32) + data(data_size)
        assert_eq!(fixed, 8 * 32, "8 fixed-weight columns × 32");
        assert_eq!(dynamic, vec!["data_size"]);
    }

    /// A row is charged for its weight key whether or not a client selects it,
    /// and for nothing else it did not select. The keys are the reference's:
    /// an EVM log is its block and log index although it is ordered by its
    /// transaction too, a Substrate event its block and index although it
    /// carries its call's address, and a trace is read as one of four shapes,
    /// so its `type` is charged with its key.
    #[test]
    fn an_unselected_row_weighs_its_weight_key() {
        let evm = evm_meta();
        let substrate = substrate_meta();
        let cases = [
            (&evm, "blocks", 32),
            (&evm, "transactions", 2 * 32),
            (&evm, "logs", 2 * 32),
            (&evm, "traces", 4 * 32),
            (&evm, "statediffs", 4 * 32),
            (&substrate, "blocks", 32),
            (&substrate, "events", 2 * 32),
            (&substrate, "calls", 3 * 32),
        ];

        for (meta, table, weight) in cases {
            let (fixed, sized) = weight_of(&[], meta.table(table));
            assert_eq!(
                (fixed, sized),
                (weight, Vec::<String>::new()),
                "{}.{table}",
                meta.name
            );
        }
    }

    /// The weight model reads a field name once. Reading its columns as field
    /// names again charged the key column `log_index` as a virtual field of the
    /// same name, here a roll of `data`, weighed only by a size column. The
    /// loader refuses such a catalog (INV-D10); the weight model must not need
    /// it to.
    #[test]
    fn a_weight_key_column_is_charged_as_a_column() {
        let mut meta = evm_meta();
        let logs = meta.tables.get_mut("logs").unwrap();
        logs.output.virtual_fields.insert(
            "log_index".to_string(),
            VirtualField::Roll {
                columns: vec!["data".to_string()],
            },
        );

        assert_eq!(
            weight_of(&[], meta.table("logs")),
            (2 * 32, Vec::<String>::new())
        );
    }

    /// Selecting a key column charges nothing more; selecting a column outside
    /// the key charges it.
    #[test]
    fn a_selected_key_column_is_charged_once() {
        let meta = evm_meta();
        let logs = meta.table("logs");

        assert_eq!(weight_of(&["log_index"], logs).0, 2 * 32);
        assert_eq!(weight_of(&["transaction_index"], logs).0, 3 * 32);

        let traces = meta.table("traces");
        assert_eq!(weight_of(&["type"], traces).0, 4 * 32);
    }

    /// A Substrate digest is a list of logs, and the reference weighs it as four
    /// words.
    #[test]
    fn a_substrate_digest_weighs_four_words() {
        let meta = substrate_meta();
        let blocks = meta.table("blocks");

        assert_eq!(weight_of(&["digest"], blocks).0, 32 + 4 * 32);
    }

    #[test]
    fn test_weight_projection_evm_transactions_with_input() {
        let meta = evm_meta();
        let txs = meta.table("transactions").unwrap();

        // Weight key: [block_number, transaction_index]
        // User output: [hash, from, to, input, value, gas]
        let (fixed, dynamic) =
            weight_of(&["hash", "from", "to", "input", "value", "gas"], Some(txs));

        // block_number(32) + transaction_index(32) + hash(32) + from(32) + to(32) +
        // value(32) + gas(32) + input(input_size)
        assert_eq!(fixed, 7 * 32);
        assert_eq!(dynamic, vec!["input_size"]);
    }

    #[test]
    fn test_weight_projection_excludes_source_predicates() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // The row weight does NOT include source predicate columns (e.g., is_committed)
        // unless the user explicitly requests them in output fields.

        // Scenario: user requests [program_id, data] only.
        // Even if is_committed is used as a source predicate for a relation,
        // it should NOT inflate the weight.
        let (fixed, dynamic) = weight_of(&["program_id", "data"], Some(instr));

        // Weight key: block_number(32) + transaction_index(32) + instruction_address(32)
        // User output: program_id(32) + data(data_size)
        // is_committed NOT included → no extra 32 bytes
        assert_eq!(fixed, 4 * 32, "4 fixed columns (no is_committed)");
        assert_eq!(dynamic.len(), 1, "1 dynamic column (data_size)");

        // Contrast: if user DID request is_committed in output, it would be counted
        let (fixed_with, dynamic_with) =
            weight_of(&["program_id", "data", "is_committed"], Some(instr));
        assert_eq!(fixed_with, 5 * 32, "5 fixed columns (with is_committed)");
        assert_eq!(dynamic_with.len(), 1);
    }

    /// A size column is unsigned by the catalog, so a negative one is a corrupt
    /// chunk. Read as a `u64` it was ~`u64::MAX`: the block outweighs the whole
    /// budget and is dropped from every response it appears in. A null one
    /// weighs nothing either.
    ///
    /// Covers CT-5 · INV-B9
    #[test]
    fn a_negative_size_weighs_nothing() {
        let negative: [std::sync::Arc<dyn Array>; 2] = [
            std::sync::Arc::new(Int64Array::from(vec![-1i64])),
            std::sync::Arc::new(Int32Array::from(vec![-4096i32])),
        ];

        let weight = |col: &dyn Array| weight_at(&IntColumn::resolve(col).unwrap(), 0);
        for col in negative {
            assert_eq!(
                weight(col.as_ref()),
                0,
                "a negative {:?} size",
                col.data_type()
            );
        }

        let sizes: [std::sync::Arc<dyn Array>; 2] = [
            std::sync::Arc::new(Int64Array::from(vec![4096i64])),
            std::sync::Arc::new(UInt32Array::from(vec![4096u32])),
        ];

        for col in sizes {
            assert_eq!(weight(col.as_ref()), 4096);
        }

        // A null size weighs nothing, whatever its slot holds.
        let null = Int64Array::new(vec![4096i64].into(), Some(vec![false].into()));
        assert_eq!(weight(&null), 0);
    }

    /// Weight is a model, and a model that wraps stops bounding anything: the
    /// sum comes back small and the budget lets the whole chunk through
    /// (INV-B9).
    ///
    /// Covers CT-5 · INV-B9
    #[test]
    fn block_weight_saturates_rather_than_wrapping() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("data_size", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(UInt64Array::from(vec![7u64, 7])),
                std::sync::Arc::new(UInt64Array::from(vec![u64::MAX, u64::MAX])),
            ],
        )
        .unwrap();

        let weight = RowWeight {
            fixed: 32,
            columns: vec!["data_size"],
        };
        let mut weights = FxHashMap::default();
        accumulate_block_weights(&[batch], "block_number", &weight, &mut weights);

        assert_eq!(weights[&7], u64::MAX);
    }
}
