mod chunk;
mod positions;
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

    /// Whether scans can return physical row positions and read those positions
    /// back through `ScanRequest::row_indices` on this immutable chunk.
    fn supports_row_positions(&self) -> bool {
        false
    }

    /// Suggest the end of the next block range from storage layout.
    /// This is a cost hint: callers must read every matching row through it.
    /// `None` asks the caller to read the remaining range in one pass.
    fn next_block_range_end(
        &self,
        table: &str,
        block_column: &str,
        from_block: u64,
    ) -> Option<u64> {
        let _ = (table, block_column, from_block);
        None
    }

    /// Check if a table exists in this chunk.
    fn has_table(&self, table: &str) -> bool;

    /// Get the Arrow schema for a table (returns None if table doesn't exist).
    fn table_schema(&self, table: &str) -> Option<SchemaRef>;
}
