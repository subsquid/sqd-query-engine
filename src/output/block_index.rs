use crate::engine_err;
use crate::error::ErrorKind;
use crate::integers::IntColumn;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
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

/// Where each block of a page sits in it: its position in the page's ascending
/// block list.
pub(crate) struct BlockPositions<'a> {
    blocks: &'a [u64],
    /// `position + 1` of block `blocks[0] + i`, or 0 for a block the page lacks,
    /// when the page's blocks are dense enough to take a slot each.
    dense: Option<Vec<u32>>,
}

impl<'a> BlockPositions<'a> {
    /// `blocks` is ascending.
    pub(crate) fn new(blocks: &'a [u64]) -> Self {
        // A sparse chain's page can span far more block numbers than it holds:
        // from block 0 to the last u64, more than a u64 counts.
        let span = match (blocks.first(), blocks.last()) {
            (Some(&first), Some(&last)) => u128::from(last - first) + 1,
            _ => 0,
        };
        let slots = (blocks.len() as u128 * 4).max(1 << 16);
        let dense = (span <= slots).then(|| {
            let mut dense = vec![0u32; span as usize];
            for (position, &block) in blocks.iter().enumerate() {
                dense[(block - blocks[0]) as usize] = position as u32 + 1;
            }
            dense
        });

        Self { blocks, dense }
    }

    pub(crate) fn len(&self) -> usize {
        self.blocks.len()
    }

    fn position(&self, block: u64) -> Option<usize> {
        match &self.dense {
            Some(dense) => {
                let slot = block.checked_sub(*self.blocks.first()?)?;
                let position = *dense.get(usize::try_from(slot).ok()?)?;
                position.checked_sub(1).map(|position| position as usize)
            }
            None => self.blocks.binary_search(&block).ok(),
        }
    }
}

/// The `(batch, row)` of each row of a page's blocks, by the block's position in
/// the page: one run of rows a block, all in one buffer. Rows of blocks outside
/// the page are left out.
pub(crate) struct BlockIndex {
    /// Block `i`'s rows are `rows[starts[i]..starts[i + 1]]`.
    starts: Vec<usize>,
    rows: Vec<(usize, usize)>,
}

impl BlockIndex {
    /// Each block's rows in file order.
    pub(crate) fn build(
        batches: &[RecordBatch],
        bn_column: &str,
        positions: &BlockPositions,
    ) -> Result<Self> {
        // Counted into the slot after each block's, so that summing them in
        // place leaves where each block's run starts.
        let mut starts = vec![0usize; positions.len() + 1];
        for_each_block_number(batches, bn_column, |_, _, block| {
            if let Some(position) = positions.position(block) {
                starts[position + 1] += 1;
            }
        })?;
        for position in 1..starts.len() {
            starts[position] += starts[position - 1];
        }

        let mut next = starts.clone();
        let mut rows = vec![(0, 0); starts[positions.len()]];
        for_each_block_number(batches, bn_column, |batch, row, block| {
            if let Some(position) = positions.position(block) {
                rows[next[position]] = (batch, row);
                next[position] += 1;
            }
        })?;

        Ok(Self { starts, rows })
    }

    /// The rows of the page's `position`th block.
    pub(crate) fn rows(&self, position: usize) -> &[(usize, usize)] {
        &self.rows[self.starts[position]..self.starts[position + 1]]
    }

    /// Each block's rows, to reorder in place.
    pub(crate) fn runs_mut(&mut self) -> impl Iterator<Item = &mut [(usize, usize)]> {
        let mut rest = self.rows.as_mut_slice();
        self.starts.windows(2).map(move |run| {
            let (head, tail) = std::mem::take(&mut rest).split_at_mut(run[1] - run[0]);
            rest = tail;
            head
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(blocks: &[u64]) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("number", DataType::UInt64, false)]);
        let column: ArrayRef = Arc::new(UInt64Array::from(blocks.to_vec()));
        RecordBatch::try_new(Arc::new(schema), vec![column]).unwrap()
    }

    fn runs(index: &BlockIndex, blocks: usize) -> Vec<Vec<(usize, usize)>> {
        (0..blocks).map(|at| index.rows(at).to_vec()).collect()
    }

    /// Each page block gets its rows in file order, a block without rows gets
    /// none, and a row of a block outside the page is left out — the same on a
    /// dense page and on one sparse enough to be searched.
    #[test]
    fn rows_run_by_page_position() {
        for page in [[10u64, 11, 13], [10, 11, 1 << 40]] {
            let far = page[2];
            let batches = [batch(&[far, 10, 12]), batch(&[10, far, 9, 14])];
            let positions = BlockPositions::new(&page);
            assert_eq!(positions.dense.is_some(), page[2] == 13);

            let index = BlockIndex::build(&batches, "number", &positions).unwrap();

            let expected = vec![vec![(0, 1), (1, 0)], vec![], vec![(0, 0), (1, 1)]];
            assert_eq!(runs(&index, page.len()), expected);
        }
    }

    /// A page from block 0 to the last u64 spans more block numbers than a u64
    /// counts, so it is searched.
    #[test]
    fn a_page_spanning_every_block_number_is_searched() {
        let page = [0, 7, u64::MAX];
        let batches = [batch(&[u64::MAX, 0, 8, 7])];
        let positions = BlockPositions::new(&page);
        assert!(positions.dense.is_none());

        let index = BlockIndex::build(&batches, "number", &positions).unwrap();

        let expected = vec![vec![(0, 1)], vec![(0, 3)], vec![(0, 0)]];
        assert_eq!(runs(&index, page.len()), expected);
    }

    #[test]
    fn each_run_is_reordered_in_place() {
        let page = [1u64, 2];
        let batches = [batch(&[2, 1, 2, 1])];
        let mut index = BlockIndex::build(&batches, "number", &BlockPositions::new(&page)).unwrap();

        for run in index.runs_mut() {
            run.reverse();
        }

        assert_eq!(
            runs(&index, 2),
            vec![vec![(0, 3), (0, 1)], vec![(0, 2), (0, 0)]]
        );
    }
}
