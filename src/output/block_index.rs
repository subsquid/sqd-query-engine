use arrow::array::*;
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap;
use std::collections::HashSet;

/// Typed block number reader — downcasts once per column, reads per row without branching.
/// Parquet stores UInt32/UInt64 as Int32/Int64 physical types, so Int32 is reinterpreted
/// via u32 to avoid sign-extension.
enum BlockNumberReader<'a> {
    UInt64(&'a UInt64Array),
    UInt32(&'a UInt32Array),
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
}

impl BlockNumberReader<'_> {
    fn resolve(col: &dyn Array) -> Option<BlockNumberReader<'_>> {
        if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
            Some(BlockNumberReader::UInt64(a))
        } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
            Some(BlockNumberReader::UInt32(a))
        } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            Some(BlockNumberReader::Int64(a))
        } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            Some(BlockNumberReader::Int32(a))
        } else {
            None
        }
    }

    #[inline]
    fn read(&self, row: usize) -> u64 {
        match self {
            Self::UInt64(a) => a.value(row),
            Self::UInt32(a) => a.value(row) as u64,
            Self::Int64(a) => a.value(row) as u64,
            Self::Int32(a) => (a.value(row) as u32) as u64,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::UInt64(a) => a.len(),
            Self::UInt32(a) => a.len(),
            Self::Int64(a) => a.len(),
            Self::Int32(a) => a.len(),
        }
    }
}

/// Compute the actual min/max block numbers from scan results (for cross-table pruning).
pub(crate) fn compute_block_range(
    batches: &[RecordBatch],
    bn_column: &str,
) -> (Option<u64>, Option<u64>) {
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;

    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            if let Some(reader) = BlockNumberReader::resolve(col.as_ref()) {
                for i in 0..reader.len() {
                    let bn = reader.read(i);
                    min_block = Some(min_block.map_or(bn, |m: u64| m.min(bn)));
                    max_block = Some(max_block.map_or(bn, |m: u64| m.max(bn)));
                }
            }
        }
    }
    (min_block, max_block)
}

/// Build an index mapping block_number -> list of (batch_index, row_index).
pub(crate) fn build_block_index(
    batches: &[RecordBatch],
    bn_column: &str,
) -> FxHashMap<u64, Vec<(usize, usize)>> {
    let mut index: FxHashMap<u64, Vec<(usize, usize)>> = FxHashMap::default();
    for (batch_idx, batch) in batches.iter().enumerate() {
        if let Some(col) = batch.column_by_name(bn_column) {
            if let Some(reader) = BlockNumberReader::resolve(col.as_ref()) {
                for row in 0..reader.len() {
                    index
                        .entry(reader.read(row))
                        .or_default()
                        .push((batch_idx, row));
                }
            }
        }
    }
    index
}

/// Collect block numbers from batches into a set.
pub(crate) fn collect_block_numbers(
    batches: &[RecordBatch],
    bn_column: &str,
    block_numbers: &mut HashSet<u64>,
) {
    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            if let Some(reader) = BlockNumberReader::resolve(col.as_ref()) {
                for i in 0..reader.len() {
                    block_numbers.insert(reader.read(i));
                }
            }
        }
    }
}

/// Collect only the first and last block numbers from the blocks table (boundary blocks).
pub(crate) fn collect_boundary_blocks(
    batches: &[RecordBatch],
    bn_column: &str,
    block_numbers: &mut HashSet<u64>,
) {
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;

    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            if let Some(reader) = BlockNumberReader::resolve(col.as_ref()) {
                for i in 0..reader.len() {
                    let v = reader.read(i);
                    min_block = Some(min_block.map_or(v, |m: u64| m.min(v)));
                    max_block = Some(max_block.map_or(v, |m: u64| m.max(v)));
                }
            }
        }
    }

    if let Some(v) = min_block {
        block_numbers.insert(v);
    }
    if let Some(v) = max_block {
        block_numbers.insert(v);
    }
}
