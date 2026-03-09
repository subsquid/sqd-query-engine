use arrow::array::*;
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap;
use std::collections::HashSet;

/// Compute the actual min/max block numbers from scan results (for cross-table pruning).
pub(crate) fn compute_block_range(
    batches: &[RecordBatch],
    bn_column: &str,
) -> (Option<u64>, Option<u64>) {
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;

    macro_rules! update_range {
        ($arr:expr) => {
            for i in 0..$arr.len() {
                let bn = $arr.value(i) as u64;
                min_block = Some(min_block.map_or(bn, |m: u64| m.min(bn)));
                max_block = Some(max_block.map_or(bn, |m: u64| m.max(bn)));
            }
        };
    }

    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                update_range!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                update_range!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                update_range!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                update_range!(a);
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
            // Type-check once per batch, then iterate with the specific type
            if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                for row in 0..a.len() {
                    index
                        .entry(a.value(row))
                        .or_default()
                        .push((batch_idx, row));
                }
            } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                for row in 0..a.len() {
                    index
                        .entry(a.value(row) as u64)
                        .or_default()
                        .push((batch_idx, row));
                }
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                for row in 0..a.len() {
                    index
                        .entry(a.value(row) as u64)
                        .or_default()
                        .push((batch_idx, row));
                }
            } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                for row in 0..a.len() {
                    index
                        .entry(a.value(row) as u64)
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
            if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                for i in 0..a.len() {
                    block_numbers.insert(a.value(i));
                }
            } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                for i in 0..a.len() {
                    block_numbers.insert(a.value(i) as u64);
                }
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..a.len() {
                    block_numbers.insert(a.value(i) as u64);
                }
            } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..a.len() {
                    block_numbers.insert(a.value(i) as u64);
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

    macro_rules! update {
        ($arr:expr) => {
            for i in 0..$arr.len() {
                let v = $arr.value(i) as u64;
                min_block = Some(min_block.map_or(v, |m: u64| m.min(v)));
                max_block = Some(max_block.map_or(v, |m: u64| m.max(v)));
            }
        };
    }

    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                update!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                update!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                update!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                update!(a);
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
