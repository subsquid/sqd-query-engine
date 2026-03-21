mod chunk;
pub mod composite_reader;
pub mod crash_log;
pub mod dataset_store;
pub(crate) mod kv_scan;
pub mod memory_backend;
pub mod parquet_writer;
pub mod predicate;
mod scanner;

#[cfg(feature = "lmdb")]
pub mod lmdb_backend;
#[cfg(feature = "rocksdb")]
pub mod rocks_backend;
#[cfg(feature = "legacy-storage")]
pub mod legacy_backend;

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
