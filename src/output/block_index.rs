use crate::engine_err;
use crate::error::ErrorKind;
use crate::integers::IntColumn;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet as HashSet;

/// Visit every block number a set of batches carries, in the order the rows sit
/// in, as `(batch, row, block number)`.
///
/// A batch that does not carry the column at all is skipped: the callers pass
/// relation batches, and a relation whose target contributed nothing has no
/// column to read.
fn for_each_block_number(
    batches: &[RecordBatch],
    bn_column: &str,
    mut visit: impl FnMut(usize, usize, u64),
) -> Result<()> {
    for (batch_idx, batch) in batches.iter().enumerate() {
        let Some(col) = batch.column_by_name(bn_column) else {
            continue;
        };

        // A column that is not an integer at all is a chunk disagreeing with
        // the catalog about what a block number is. Returning "no rows" for one
        // would drop every block of the table silently, so it is an error.
        let reader = IntColumn::resolve(col.as_ref()).ok_or_else(|| {
            engine_err!(
                ErrorKind::UnsupportedKeyType,
                "block number column '{}' is stored as {:?}, which is not an integer",
                bn_column,
                col.data_type()
            )
        })?;

        for row in 0..reader.len() {
            visit(batch_idx, row, reader.block_number(row));
        }
    }

    Ok(())
}

/// Compute the actual min/max block numbers from scan results (for cross-table pruning).
pub(crate) fn compute_block_range(
    batches: &[RecordBatch],
    bn_column: &str,
) -> Result<(Option<u64>, Option<u64>)> {
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;

    for_each_block_number(batches, bn_column, |_, _, bn| {
        min_block = Some(min_block.map_or(bn, |m: u64| m.min(bn)));
        max_block = Some(max_block.map_or(bn, |m: u64| m.max(bn)));
    })?;

    Ok((min_block, max_block))
}

/// Build an index mapping block_number -> list of (batch_index, row_index).
pub(crate) fn build_block_index(
    batches: &[RecordBatch],
    bn_column: &str,
) -> Result<FxHashMap<u64, Vec<(usize, usize)>>> {
    let mut index: FxHashMap<u64, Vec<(usize, usize)>> = FxHashMap::default();

    for_each_block_number(batches, bn_column, |batch_idx, row, bn| {
        index.entry(bn).or_default().push((batch_idx, row));
    })?;

    Ok(index)
}

/// Collect block numbers from batches into a set.
pub(crate) fn collect_block_numbers(
    batches: &[RecordBatch],
    bn_column: &str,
    block_numbers: &mut HashSet<u64>,
) -> Result<()> {
    for_each_block_number(batches, bn_column, |_, _, bn| {
        block_numbers.insert(bn);
    })?;

    Ok(())
}

/// Collect only the first and last block numbers from the blocks table (boundary blocks).
pub(crate) fn collect_boundary_blocks(
    batches: &[RecordBatch],
    bn_column: &str,
    block_numbers: &mut HashSet<u64>,
) -> Result<()> {
    let (min_block, max_block) = compute_block_range(batches, bn_column)?;

    block_numbers.extend(min_block);
    block_numbers.extend(max_block);

    Ok(())
}
