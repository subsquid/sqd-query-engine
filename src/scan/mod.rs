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

    /// Check if a table exists in this chunk.
    fn has_table(&self, table: &str) -> bool;

    /// Get the Arrow schema for a table (returns None if table doesn't exist).
    fn table_schema(&self, table: &str) -> Option<SchemaRef>;
}
