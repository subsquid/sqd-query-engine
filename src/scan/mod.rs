mod addresses;
mod chunk;
mod columns;
mod key_columns;
mod keys;
mod pairs;
mod positions;
pub mod predicate;
mod rows;
mod scanner;

pub use addresses::{AddressIndex, HierarchicalFilter, HierarchicalMode};
pub use chunk::*;
pub use columns::{ColumnCache, Window};
pub use keys::{KeyFilter, KeySet};
pub use rows::{Rows, Scanned};
pub use scanner::*;

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// A source of table data for a single chunk (block range).
/// Implementations handle storage-specific details (parquet files, RocksDB, etc.)
/// and return Arrow RecordBatches that the rest of the pipeline operates on.
pub trait ChunkReader: Sync {
    /// Scan a table: apply projection, predicates, block range, and
    /// key/hierarchical filters. Besides the rows, the result holds what the
    /// request asked to learn about them: each row's physical position and
    /// which rows each item tag matched.
    fn scan_rows(&self, table: &str, request: &ScanRequest) -> Result<Scanned>;

    /// The rows of [`ChunkReader::scan_rows`] alone.
    fn scan(&self, table: &str, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
        Ok(self.scan_rows(table, request)?.into_rows().into_batches())
    }

    /// Whether scans can record physical row positions and read those
    /// positions back through `ScanRequest::row_indices` on this immutable chunk.
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

    /// An upper bound on the bytes a scan's output arrays would hold, from
    /// storage metadata alone. A query reads its payloads in one pass on it, so
    /// it may overcount but never undercount; `None` when the reader cannot
    /// bound them, which callers treat as unbounded.
    fn estimate_scan_bytes(&self, table: &str, request: &ScanRequest) -> Option<u64> {
        let _ = (table, request);
        None
    }

    /// Check if a table exists in this chunk.
    fn has_table(&self, table: &str) -> bool;

    /// Get the Arrow schema for a table (returns None if table doesn't exist).
    fn table_schema(&self, table: &str) -> Option<SchemaRef>;
}
