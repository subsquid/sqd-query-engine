//! Composite ChunkReader that merges results from multiple ChunkReader tiers.
//!
//! Used by DatasetStore to present memory buffer + spillover parquet + compacted
//! parquet chunks as a single unified ChunkReader to the query pipeline.

use crate::scan::{ChunkReader, ScanRequest};
use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// A ChunkReader that delegates to multiple underlying readers and merges results.
///
/// Block range routing: skips readers whose block range doesn't overlap with the
/// request's from_block/to_block, making "latest" queries hit only memory.
pub struct CompositeChunkReader<'a> {
    readers: Vec<ReaderSlice<'a>>,
}

struct ReaderSlice<'a> {
    reader: &'a dyn ChunkReader,
    /// Inclusive block range this reader covers (for routing optimization).
    first_block: Option<u64>,
    last_block: Option<u64>,
}

impl<'a> CompositeChunkReader<'a> {
    pub fn new() -> Self {
        Self {
            readers: Vec::new(),
        }
    }

    /// Add a reader with known block range (for routing optimization).
    pub fn add(&mut self, reader: &'a dyn ChunkReader, first_block: Option<u64>, last_block: Option<u64>) {
        self.readers.push(ReaderSlice {
            reader,
            first_block,
            last_block,
        });
    }

    /// Add a reader without block range info (always scanned).
    pub fn add_unbounded(&mut self, reader: &'a dyn ChunkReader) {
        self.readers.push(ReaderSlice {
            reader,
            first_block: None,
            last_block: None,
        });
    }
}

impl<'a> ChunkReader for CompositeChunkReader<'a> {
    fn scan(&self, table: &str, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();

        for slice in &self.readers {
            // Block range routing: skip readers that can't contain requested blocks
            if let (Some(reader_first), Some(reader_last)) = (slice.first_block, slice.last_block) {
                if let Some(req_from) = request.from_block {
                    if reader_last < req_from {
                        continue; // reader ends before request starts
                    }
                }
                if let Some(req_to) = request.to_block {
                    if reader_first > req_to {
                        continue; // reader starts after request ends
                    }
                }
            }

            let batches = slice.reader.scan(table, request)?;
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }

    fn has_table(&self, table: &str) -> bool {
        self.readers.iter().any(|s| s.reader.has_table(table))
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.readers
            .iter()
            .find_map(|s| s.reader.table_schema(table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::memory_backend::{BlockData, MemoryChunkReader};
    use arrow::array::*;
    use arrow::datatypes::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_batch(block_number: i32, value: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::Int32, false),
            Field::new("data", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![block_number])),
                Arc::new(StringArray::from(vec![value])),
            ],
        )
        .unwrap()
    }

    fn make_block(block_number: u64, value: &str) -> BlockData {
        let mut tables = HashMap::new();
        tables.insert("events".to_string(), make_batch(block_number as i32, value));
        BlockData {
            block_number,
            tables,
        }
    }

    #[test]
    fn test_composite_merges_tiers() {
        // Tier 1: "old" blocks
        let mut tier1 = MemoryChunkReader::new();
        tier1.push(make_block(100, "old_a"));
        tier1.push(make_block(101, "old_b"));

        // Tier 2: "new" blocks
        let mut tier2 = MemoryChunkReader::new();
        tier2.push(make_block(200, "new_a"));
        tier2.push(make_block(201, "new_b"));

        let mut composite = CompositeChunkReader::new();
        composite.add(&tier1, Some(100), Some(101));
        composite.add(&tier2, Some(200), Some(201));

        let request = ScanRequest::new(vec!["block_number", "data"]);
        let batches = composite.scan("events", &request).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4); // all blocks from both tiers
    }

    #[test]
    fn test_block_range_routing_skips_old() {
        let mut tier1 = MemoryChunkReader::new();
        tier1.push(make_block(100, "old"));

        let mut tier2 = MemoryChunkReader::new();
        tier2.push(make_block(200, "new"));

        let mut composite = CompositeChunkReader::new();
        composite.add(&tier1, Some(100), Some(100));
        composite.add(&tier2, Some(200), Some(200));

        // Query only for block 200+
        let mut request = ScanRequest::new(vec!["block_number", "data"]);
        request.from_block = Some(200);

        let batches = composite.scan("events", &request).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1); // only tier2
    }

    #[test]
    fn test_table_schema_from_first_tier() {
        let mut tier1 = MemoryChunkReader::new();
        tier1.push(make_block(100, "a"));

        let mut tier2 = MemoryChunkReader::new();
        tier2.push(make_block(200, "b"));

        let mut composite = CompositeChunkReader::new();
        composite.add_unbounded(&tier1);
        composite.add_unbounded(&tier2);

        // Schema should be available
        let schema = composite.table_schema("events");
        assert!(schema.is_some());
        let s = schema.unwrap();
        assert!(s.field_with_name("block_number").is_ok());
        assert!(s.field_with_name("data").is_ok());

        // Nonexistent table
        assert!(composite.table_schema("nonexistent").is_none());
    }

    #[test]
    fn test_empty_composite_scan() {
        let composite = CompositeChunkReader::new();
        let request = ScanRequest::new(vec!["x"]);
        let batches = composite.scan("anything", &request).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_has_table_across_tiers() {
        let mut tier1 = MemoryChunkReader::new();
        tier1.push(make_block(100, "a"));

        let composite_empty = CompositeChunkReader::new();
        assert!(!composite_empty.has_table("events"));

        let mut composite = CompositeChunkReader::new();
        composite.add_unbounded(&tier1);
        assert!(composite.has_table("events"));
        assert!(!composite.has_table("nonexistent"));
    }
}
