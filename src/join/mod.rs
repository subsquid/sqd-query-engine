mod hierarchical;
mod semi_join;

pub use hierarchical::*;
pub use semi_join::*;

use anyhow::Result;
use arrow::array::{BooleanArray, RecordBatch};

/// The rows of each batch its mask selects; batches left empty are dropped.
fn keep_matching(batches: &[RecordBatch], masks: Vec<BooleanArray>) -> Result<Vec<RecordBatch>> {
    let mut kept = Vec::new();
    for (batch, mask) in batches.iter().zip(masks) {
        let count = mask.true_count();
        if count == 0 {
            continue;
        }

        if count == batch.num_rows() {
            kept.push(batch.clone());
        } else {
            kept.push(arrow::compute::filter_record_batch(batch, &mask)?);
        }
    }
    Ok(kept)
}

/// A mask selecting no row of each batch.
fn no_rows(batches: &[RecordBatch]) -> Vec<BooleanArray> {
    batches
        .iter()
        .map(|batch| BooleanArray::from(vec![false; batch.num_rows()]))
        .collect()
}
