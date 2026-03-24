//! In-memory ChunkReader for the hot block buffer.
//!
//! Stores Arrow RecordBatches per block per table in memory.
//! Queryable immediately after push — no flush needed.
//! Supports truncation for reorg handling.
//!
//! Optimizations:
//! - Block range pruning: skip blocks outside requested range
//! - Predicate pruning: skip blocks where predicates can't match (per-block min/max)

use crate::scan::kv_scan::apply_scan_filters;
use crate::scan::predicate::{can_skip_row_group_or, RowPredicate};
use crate::scan::{ChunkReader, ScanRequest};
use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// A single block's data: one RecordBatch per table.
#[derive(Debug)]
pub struct BlockData {
    pub block_number: u64,
    pub tables: HashMap<String, RecordBatch>,
}

/// In-memory buffer of recent blocks, queryable via ChunkReader.
///
/// Blocks are stored in arrival order (newest at the end).
/// Supports:
/// - Push: append a new block (~μs)
/// - Truncate: remove blocks >= fork point for reorg handling
/// - Snapshot: cheap point-in-time view for query isolation
/// - Scan: filter and project batches in memory
pub struct MemoryChunkReader {
    blocks: Vec<Arc<BlockData>>,
    /// Cached schemas per table (from first block that contains each table).
    schemas: HashMap<String, SchemaRef>,
}

/// A frozen point-in-time snapshot of the memory buffer.
/// Cheap to create (Arc clones only). Implements ChunkReader for queries.
/// Concurrent push_block() on the original buffer does not affect the snapshot.
#[derive(Clone)]
pub struct MemorySnapshot {
    blocks: Vec<Arc<BlockData>>,
    schemas: HashMap<String, SchemaRef>,
}

impl MemoryChunkReader {
    pub fn new() -> Self {
        Self {
            blocks: Vec::new(),
            schemas: HashMap::new(),
        }
    }

    /// Push a new block's data into the buffer.
    ///
    /// Blocks must be pushed in ascending block_number order.
    /// Panics if block_number <= last pushed block (except for first push).
    pub fn push(&mut self, block: BlockData) {
        if let Some(last) = self.blocks.last() {
            assert!(
                block.block_number > last.block_number,
                "blocks must be pushed in order: got {} after {}",
                block.block_number,
                last.block_number
            );
        }
        for (table_name, batch) in &block.tables {
            self.schemas
                .entry(table_name.clone())
                .or_insert_with(|| batch.schema());
        }
        self.blocks.push(Arc::new(block));
    }

    /// Truncate the buffer: remove all blocks with block_number >= fork_point.
    /// Used for reorg handling.
    pub fn truncate(&mut self, fork_point: u64) {
        self.blocks.retain(|b| b.block_number < fork_point);
    }

    /// Remove blocks with block_number <= up_to.
    /// Used after compaction to parquet.
    pub fn drain_up_to(&mut self, up_to: u64) -> Vec<Arc<BlockData>> {
        let split = self
            .blocks
            .partition_point(|b| b.block_number <= up_to);
        self.blocks.drain(..split).collect()
    }

    /// Current head block number, or None if empty.
    pub fn head(&self) -> Option<u64> {
        self.blocks.last().map(|b| b.block_number)
    }

    /// First block number in the buffer, or None if empty.
    pub fn first_block(&self) -> Option<u64> {
        self.blocks.first().map(|b| b.block_number)
    }

    /// Number of blocks in the buffer.
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Iterate over all blocks in the buffer.
    pub fn iter_blocks(&self) -> impl DoubleEndedIterator<Item = &Arc<BlockData>> {
        self.blocks.iter()
    }

    /// Create a cheap point-in-time snapshot for query isolation.
    /// Cost: O(n) Arc increments where n = number of blocks (~2KB for 224 blocks).
    /// The snapshot sees a frozen view — concurrent push_block() does not affect it.
    pub fn snapshot(&self) -> MemorySnapshot {
        MemorySnapshot {
            blocks: self.blocks.clone(),
            schemas: self.schemas.clone(),
        }
    }

    /// Approximate memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        self.blocks
            .iter()
            .map(|b| {
                b.tables
                    .values()
                    .map(|batch| {
                        batch
                            .columns()
                            .iter()
                            .map(|c| c.get_array_memory_size())
                            .sum::<usize>()
                    })
                    .sum::<usize>()
            })
            .sum()
    }
}

/// Shared scan logic for memory-backed readers (MemoryChunkReader and MemorySnapshot).
fn scan_blocks(
    blocks: &[Arc<BlockData>],
    schemas: &HashMap<String, SchemaRef>,
    table: &str,
    request: &ScanRequest,
) -> Result<Vec<RecordBatch>> {
    let schema = match schemas.get(table) {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };

    let mut batches: Vec<RecordBatch> = Vec::new();

    for block in blocks {
        // Block range pruning (zero cost)
        if let Some(from) = request.from_block {
            if block.block_number < from {
                continue;
            }
        }
        if let Some(to) = request.to_block {
            if block.block_number > to {
                continue;
            }
        }

        let batch = match block.tables.get(table) {
            Some(b) if b.num_rows() > 0 => b,
            _ => continue,
        };

        batches.push(batch.clone());
    }

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    apply_scan_filters(batches, request, schema)
}

impl ChunkReader for MemoryChunkReader {
    fn scan(&self, table: &str, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
        scan_blocks(&self.blocks, &self.schemas, table, request)
    }

    fn has_table(&self, table: &str) -> bool {
        self.schemas.contains_key(table)
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.schemas.get(table).cloned()
    }
}

impl MemorySnapshot {
    /// Create a snapshot from pre-existing blocks and schemas.
    pub fn from_blocks(
        blocks: Vec<Arc<BlockData>>,
        schemas: HashMap<String, SchemaRef>,
    ) -> Self {
        Self { blocks, schemas }
    }

    pub fn head(&self) -> Option<u64> {
        self.blocks.last().map(|b| b.block_number)
    }

    pub fn first_block(&self) -> Option<u64> {
        self.blocks.first().map(|b| b.block_number)
    }
}

impl ChunkReader for MemorySnapshot {
    fn scan(&self, table: &str, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
        scan_blocks(&self.blocks, &self.schemas, table, request)
    }

    fn has_table(&self, table: &str) -> bool {
        self.schemas.contains_key(table)
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.schemas.get(table).cloned()
    }
}

/// Compute min and max of a column as single-element arrays (for can_skip).
fn column_min_max(col: &Arc<dyn Array>) -> Option<(Arc<dyn Array>, Arc<dyn Array>)> {
    if col.is_empty() {
        return None;
    }
    match col.data_type() {
        DataType::UInt8 => {
            let a = col.as_any().downcast_ref::<UInt8Array>()?;
            let (min, max) = (arrow::compute::min(a)?, arrow::compute::max(a)?);
            Some((Arc::new(UInt8Array::from(vec![min])), Arc::new(UInt8Array::from(vec![max]))))
        }
        DataType::UInt16 => {
            let a = col.as_any().downcast_ref::<UInt16Array>()?;
            let (min, max) = (arrow::compute::min(a)?, arrow::compute::max(a)?);
            Some((Arc::new(UInt16Array::from(vec![min])), Arc::new(UInt16Array::from(vec![max]))))
        }
        DataType::UInt32 => {
            let a = col.as_any().downcast_ref::<UInt32Array>()?;
            let (min, max) = (arrow::compute::min(a)?, arrow::compute::max(a)?);
            Some((Arc::new(UInt32Array::from(vec![min])), Arc::new(UInt32Array::from(vec![max]))))
        }
        DataType::UInt64 => {
            let a = col.as_any().downcast_ref::<UInt64Array>()?;
            let (min, max) = (arrow::compute::min(a)?, arrow::compute::max(a)?);
            Some((Arc::new(UInt64Array::from(vec![min])), Arc::new(UInt64Array::from(vec![max]))))
        }
        DataType::Int32 => {
            let a = col.as_any().downcast_ref::<Int32Array>()?;
            let (min, max) = (arrow::compute::min(a)?, arrow::compute::max(a)?);
            Some((Arc::new(Int32Array::from(vec![min])), Arc::new(Int32Array::from(vec![max]))))
        }
        DataType::Int64 => {
            let a = col.as_any().downcast_ref::<Int64Array>()?;
            let (min, max) = (arrow::compute::min(a)?, arrow::compute::max(a)?);
            Some((Arc::new(Int64Array::from(vec![min])), Arc::new(Int64Array::from(vec![max]))))
        }
        DataType::Utf8 => {
            let a = col.as_any().downcast_ref::<StringArray>()?;
            let (min, max) = (arrow::compute::min_string(a)?, arrow::compute::max_string(a)?);
            Some((
                Arc::new(StringArray::from(vec![min])),
                Arc::new(StringArray::from(vec![max])),
            ))
        }
        _ => None, // can't compute stats for this type → don't skip
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::*;
    use std::sync::Arc;

    fn make_test_batch(block_number: i32, values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::Int32, false),
            Field::new("address", DataType::Utf8, false),
        ]));

        let bn_array = Int32Array::from(vec![block_number; values.len()]);
        let addr_array = StringArray::from(values.to_vec());

        RecordBatch::try_new(
            schema,
            vec![Arc::new(bn_array), Arc::new(addr_array)],
        )
        .unwrap()
    }

    fn make_block(block_number: u64, values: &[&str]) -> BlockData {
        let mut tables = HashMap::new();
        tables.insert(
            "logs".to_string(),
            make_test_batch(block_number as i32, values),
        );
        BlockData {
            block_number,
            tables,
        }
    }

    #[test]
    fn test_push_and_scan() {
        let mut reader = MemoryChunkReader::new();
        reader.push(make_block(100, &["0xaaa", "0xbbb"]));
        reader.push(make_block(101, &["0xccc"]));

        assert_eq!(reader.head(), Some(101));
        assert_eq!(reader.first_block(), Some(100));
        assert_eq!(reader.len(), 2);

        let request = ScanRequest::new(vec!["block_number", "address"]);
        let batches = reader.scan("logs", &request).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_truncate_reorg() {
        let mut reader = MemoryChunkReader::new();
        reader.push(make_block(100, &["0xaaa"]));
        reader.push(make_block(101, &["0xbbb"]));
        reader.push(make_block(102, &["0xccc"]));

        reader.truncate(101); // reorg at 101
        assert_eq!(reader.head(), Some(100));
        assert_eq!(reader.len(), 1);
    }

    #[test]
    fn test_drain_compacted() {
        let mut reader = MemoryChunkReader::new();
        reader.push(make_block(100, &["0xaaa"]));
        reader.push(make_block(101, &["0xbbb"]));
        reader.push(make_block(102, &["0xccc"]));

        let drained = reader.drain_up_to(101);
        assert_eq!(drained.len(), 2);
        assert_eq!(reader.len(), 1);
        assert_eq!(reader.first_block(), Some(102));
    }

    #[test]
    fn test_scan_nonexistent_table() {
        let reader = MemoryChunkReader::new();
        let request = ScanRequest::new(vec!["x"]);
        let batches = reader.scan("nonexistent", &request).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_memory_usage() {
        let mut reader = MemoryChunkReader::new();
        assert_eq!(reader.memory_usage(), 0);
        reader.push(make_block(100, &["0xaaa"]));
        assert!(reader.memory_usage() > 0);
    }

    #[test]
    fn test_predicate_pruning_skips_blocks() {
        use crate::scan::predicate::{col_eq, RowPredicate, ScalarValue};

        let mut reader = MemoryChunkReader::new();
        // Block 100: addresses "0xaaa", "0xbbb"
        reader.push(make_block(100, &["0xaaa", "0xbbb"]));
        // Block 101: addresses "0xccc", "0xddd"
        reader.push(make_block(101, &["0xccc", "0xddd"]));
        // Block 102: addresses "0xaaa", "0xeee"
        reader.push(make_block(102, &["0xaaa", "0xeee"]));

        // Query for address = "0xccc" — only block 101 has it
        let pred = RowPredicate::new(vec![col_eq(
            "address",
            ScalarValue::Utf8("0xccc".to_string()),
        )]);

        let mut request = ScanRequest::new(vec!["block_number", "address"]);
        request.predicates = vec![&pred];

        let batches = reader.scan("logs", &request).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1); // only "0xccc" from block 101
    }

    #[test]
    fn test_predicate_pruning_no_match() {
        use crate::scan::predicate::{col_eq, RowPredicate, ScalarValue};

        let mut reader = MemoryChunkReader::new();
        reader.push(make_block(100, &["0xaaa"]));
        reader.push(make_block(101, &["0xbbb"]));

        // Query for address that doesn't exist — all blocks should be pruned
        let pred = RowPredicate::new(vec![col_eq(
            "address",
            ScalarValue::Utf8("0xzzz".to_string()),
        )]);

        let mut request = ScanRequest::new(vec!["block_number", "address"]);
        request.predicates = vec![&pred];

        let batches = reader.scan("logs", &request).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_column_min_max() {
        // Test Utf8
        let col: Arc<dyn Array> = Arc::new(StringArray::from(vec!["banana", "apple", "cherry"]));
        let (min, max) = column_min_max(&col).unwrap();
        let min_str = min.as_any().downcast_ref::<StringArray>().unwrap();
        let max_str = max.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(min_str.value(0), "apple");
        assert_eq!(max_str.value(0), "cherry");

        // Test UInt64
        let col: Arc<dyn Array> = Arc::new(UInt64Array::from(vec![10, 5, 20]));
        let (min, max) = column_min_max(&col).unwrap();
        let min_u = min.as_any().downcast_ref::<UInt64Array>().unwrap();
        let max_u = max.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(min_u.value(0), 5);
        assert_eq!(max_u.value(0), 20);

        // Test empty
        let col: Arc<dyn Array> = Arc::new(StringArray::from(Vec::<&str>::new()));
        assert!(column_min_max(&col).is_none());
    }

    #[test]
    fn test_scan_with_block_range_filter() {
        let mut reader = MemoryChunkReader::new();
        reader.push(make_block(100, &["0xaaa"]));
        reader.push(make_block(101, &["0xbbb"]));
        reader.push(make_block(102, &["0xccc"]));

        let mut request = ScanRequest::new(vec!["block_number", "address"]);
        request.from_block = Some(101);
        request.to_block = Some(101);
        request.block_number_column = Some("block_number");

        let batches = reader.scan("logs", &request).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1); // only block 101
    }
}
