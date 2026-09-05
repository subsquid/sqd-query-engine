mod chunk;
pub mod predicate;
mod scanner;

pub use chunk::*;
pub use scanner::*;

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// A source of table data for a single chunk (block range).
/// Implementations handle storage-specific details (parquet files, RocksDB, etc.)
/// and return Arrow RecordBatches that the rest of the pipeline operates on.
pub trait ChunkReader: Sync {
    /// Scan a table: apply projection, predicates, block range, and key/hierarchical filters.
    fn scan(&self, table: &str, request: &ScanRequest) -> Result<Vec<RecordBatch>>;

    /// Scan a block-partitioned table in ascending block order, stopping once
    /// the response weight of the blocks the walk has settled (reported by
    /// `settled_weight` after each parallel wave of `wave_size` row groups)
    /// exceeds `budget`. See [`scan_waves_until_budget`] for the contract, and
    /// [`BudgetScan::complete_through`] for what the caller owes the client
    /// afterwards. Default impl falls back to a full `scan` (ignoring the
    /// budget) for readers that can't stream by block.
    fn scan_budget(
        &self,
        table: &str,
        request: &ScanRequest,
        wave_size: usize,
        budget: u64,
        settled_weight: &mut dyn FnMut(&[RecordBatch], Option<u64>) -> u64,
    ) -> Result<BudgetScan> {
        let _ = (wave_size, budget, settled_weight);
        Ok(BudgetScan {
            batches: self.scan(table, request)?,
            complete_through: None,
        })
    }

    /// Check if a table exists in this chunk.
    fn has_table(&self, table: &str) -> bool;

    /// Get the Arrow schema for a table (returns None if table doesn't exist).
    fn table_schema(&self, table: &str) -> Option<SchemaRef>;
}
