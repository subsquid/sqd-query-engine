//! Fuzz tests for the storage layer.
//!
//! Tests random sequences of operations (push, truncate, compact, query)
//! and verifies consistency across tiers.

use proptest::prelude::*;
use sqd_query_engine::scan::memory_backend::{BlockData, MemoryChunkReader};
use sqd_query_engine::scan::composite_reader::CompositeChunkReader;
use sqd_query_engine::scan::crash_log::{CrashLogWriter, recover_crash_log};
use sqd_query_engine::scan::parquet_writer::{flush_to_parquet, chunk_dir_name, parse_chunk_dir_name};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest};
use sqd_query_engine::metadata::load_dataset_description;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

// --- Helpers ---

fn make_batch(block_number: i32, addresses: &[&str], values: &[i64]) -> RecordBatch {
    assert_eq!(addresses.len(), values.len());
    let n = addresses.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int32, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![block_number; n])),
            Arc::new(StringArray::from(addresses.to_vec())),
            Arc::new(Int64Array::from(values.to_vec())),
        ],
    )
    .unwrap()
}

fn make_block(block_number: u64, addresses: &[&str], values: &[i64]) -> BlockData {
    let mut tables = HashMap::new();
    tables.insert(
        "logs".to_string(),
        make_batch(block_number as i32, addresses, values),
    );
    BlockData {
        block_number,
        tables,
    }
}

fn query_all_rows(reader: &dyn ChunkReader) -> usize {
    let request = ScanRequest::new(vec!["block_number", "address", "value"]);
    let batches = reader.scan("logs", &request).unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

fn query_block_range(reader: &dyn ChunkReader, from: u64, to: u64) -> usize {
    let mut request = ScanRequest::new(vec!["block_number", "address", "value"]);
    request.from_block = Some(from);
    request.to_block = Some(to);
    request.block_number_column = Some("block_number");
    let batches = reader.scan("logs", &request).unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

// --- Strategy generators ---

fn address_strategy() -> impl Strategy<Value = String> {
    prop::sample::select(vec![
        "0xaaa", "0xbbb", "0xccc", "0xddd", "0xeee",
        "0xfff", "0x111", "0x222", "0x333", "0x444",
    ])
    .prop_map(|s| s.to_string())
}

fn block_strategy(max_block: u64) -> impl Strategy<Value = BlockData> {
    (
        0..max_block,
        prop::collection::vec(address_strategy(), 1..10),
        prop::collection::vec(-1000i64..1000, 1..10),
    )
        .prop_map(|(bn, addrs, vals)| {
            let len = addrs.len().min(vals.len());
            let addr_refs: Vec<&str> = addrs[..len].iter().map(|s| s.as_str()).collect();
            make_block(bn, &addr_refs, &vals[..len])
        })
}

/// Sort blocks by block_number and deduplicate (required by MemoryChunkReader::push)
fn sort_dedup_blocks(blocks: &mut Vec<BlockData>) {
    blocks.sort_by_key(|b| b.block_number);
    blocks.dedup_by_key(|b| b.block_number);
}

// --- Fuzz tests ---

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    /// Push random blocks, verify all rows queryable.
    #[test]
    fn fuzz_memory_push_query(
        mut blocks in prop::collection::vec(block_strategy(1000), 1..50)
    ) {
        sort_dedup_blocks(&mut blocks);
        let mut reader = MemoryChunkReader::new();
        let mut expected_rows = 0;

        for block in &blocks {
            expected_rows += block.tables.get("logs").map(|b| b.num_rows()).unwrap_or(0);
            reader.push(BlockData {
                block_number: block.block_number,
                tables: block.tables.clone(),
            });
        }

        let actual = query_all_rows(&reader);
        prop_assert_eq!(actual, expected_rows);
    }

    /// Push blocks, truncate at random point, verify consistency.
    #[test]
    fn fuzz_memory_truncate(
        mut blocks in prop::collection::vec(block_strategy(100), 5..30),
        truncate_at in 0u64..100,
    ) {
        sort_dedup_blocks(&mut blocks);
        let mut reader = MemoryChunkReader::new();
        for block in &blocks {
            reader.push(BlockData {
                block_number: block.block_number,
                tables: block.tables.clone(),
            });
        }

        reader.truncate(truncate_at);

        // All remaining blocks should have block_number < truncate_at
        for block in reader.iter_blocks() {
            prop_assert!(block.block_number < truncate_at,
                "block {} should have been truncated at {}",
                block.block_number, truncate_at);
        }

        // Query should return only non-truncated rows
        let rows = query_all_rows(&reader);
        let expected: usize = blocks.iter()
            .filter(|b| b.block_number < truncate_at)
            .map(|b| b.tables.get("logs").map(|b| b.num_rows()).unwrap_or(0))
            .sum();
        prop_assert_eq!(rows, expected);
    }

    /// Push sorted blocks, drain some, verify remaining are correct.
    /// (drain_up_to uses partition_point which requires sorted order)
    #[test]
    fn fuzz_memory_drain(
        mut blocks in prop::collection::vec(block_strategy(100), 5..30),
        drain_up_to in 0u64..100,
    ) {
        // Sort blocks — drain_up_to requires sorted buffer
        blocks.sort_by_key(|b| b.block_number);
        blocks.dedup_by_key(|b| b.block_number);

        let mut reader = MemoryChunkReader::new();
        for block in &blocks {
            reader.push(BlockData {
                block_number: block.block_number,
                tables: block.tables.clone(),
            });
        }

        let drained = reader.drain_up_to(drain_up_to);

        // Drained blocks should all have block_number <= drain_up_to
        for block in &drained {
            prop_assert!(block.block_number <= drain_up_to);
        }

        // Remaining blocks should all have block_number > drain_up_to
        for block in reader.iter_blocks() {
            prop_assert!(block.block_number > drain_up_to);
        }
    }

    /// Block range query returns correct subset.
    #[test]
    fn fuzz_block_range_query(
        mut blocks in prop::collection::vec(block_strategy(200), 5..30),
        from in 0u64..200,
        to in 0u64..200,
    ) {
        let (from, to) = if from <= to { (from, to) } else { (to, from) };
        sort_dedup_blocks(&mut blocks);

        let mut reader = MemoryChunkReader::new();
        for block in &blocks {
            reader.push(BlockData {
                block_number: block.block_number,
                tables: block.tables.clone(),
            });
        }

        let actual = query_block_range(&reader, from, to);
        let expected: usize = blocks.iter()
            .filter(|b| b.block_number >= from && b.block_number <= to)
            .map(|b| b.tables.get("logs").map(|b| b.num_rows()).unwrap_or(0))
            .sum();
        prop_assert_eq!(actual, expected);
    }

    /// Crash log: write + recover returns same data.
    #[test]
    fn fuzz_crash_log_recovery(
        mut blocks in prop::collection::vec(block_strategy(500), 1..20)
    ) {
        sort_dedup_blocks(&mut blocks);
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        // Write blocks
        {
            let mut writer = CrashLogWriter::open(&log_dir).unwrap();
            for block in &blocks {
                for (table_name, batch) in &block.tables {
                    writer.append(table_name, block.block_number, batch).unwrap();
                }
            }
        }

        // Recover
        let recovered = recover_crash_log(&log_dir).unwrap();
        prop_assert_eq!(recovered.len(), blocks.len(),
            "recovered {} blocks, expected {}", recovered.len(), blocks.len());

        // Verify row counts match
        for (i, (bn, tables)) in recovered.iter().enumerate() {
            if let Some(batch) = tables.get("logs") {
                let expected_rows = blocks[i].tables.get("logs")
                    .map(|b| b.num_rows()).unwrap_or(0);
                prop_assert_eq!(batch.num_rows(), expected_rows,
                    "block {} row count mismatch", bn);
            }
        }
    }

    /// Crash log: write, truncate at random point, recover.
    #[test]
    fn fuzz_crash_log_truncate(
        block_count in 3u64..15,
        truncate_at in 0u64..15,
    ) {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        let mut writer = CrashLogWriter::open(&log_dir).unwrap();
        for i in 0..block_count {
            let block = make_block(i, &["0xaaa"], &[i as i64]);
            for (table_name, batch) in &block.tables {
                writer.append(table_name, block.block_number, batch).unwrap();
            }
        }

        writer.truncate(truncate_at).unwrap();

        let recovered = recover_crash_log(&log_dir).unwrap();
        let expected_count = (0..block_count).filter(|&i| i < truncate_at).count();
        prop_assert_eq!(recovered.len(), expected_count,
            "truncate_at={}, block_count={}, recovered={}",
            truncate_at, block_count, recovered.len());
    }

    /// Composite reader: memory + spillover returns union of all data.
    #[test]
    fn fuzz_composite_query(
        mut blocks1 in prop::collection::vec(block_strategy(100), 1..10),
        mut blocks2 in prop::collection::vec(block_strategy(200), 1..10),
    ) {
        sort_dedup_blocks(&mut blocks1);
        sort_dedup_blocks(&mut blocks2);
        let mut tier1 = MemoryChunkReader::new();
        let mut tier2 = MemoryChunkReader::new();

        let mut total_rows = 0;
        for block in &blocks1 {
            total_rows += block.tables.get("logs").map(|b| b.num_rows()).unwrap_or(0);
            tier1.push(BlockData {
                block_number: block.block_number,
                tables: block.tables.clone(),
            });
        }
        for block in &blocks2 {
            total_rows += block.tables.get("logs").map(|b| b.num_rows()).unwrap_or(0);
            tier2.push(BlockData {
                block_number: block.block_number + 1000, // offset to avoid overlap
                tables: block.tables.clone(),
            });
        }

        let mut composite = CompositeChunkReader::new();
        composite.add_unbounded(&tier1);
        composite.add_unbounded(&tier2);

        let actual = query_all_rows(&composite);
        prop_assert_eq!(actual, total_rows);
    }

    /// Memory vs Spillover: same data, same query, same result.
    #[test]
    fn fuzz_memory_vs_spillover_consistency(
        mut blocks in prop::collection::vec(block_strategy(100), 3..15),
        from in 0u64..100,
        to in 0u64..100,
    ) {
        sort_dedup_blocks(&mut blocks);
        let (from, to) = if from <= to { (from, to) } else { (to, from) };
        let meta = load_dataset_description(Path::new("metadata/evm.yaml")).unwrap();
        let tmp = TempDir::new().unwrap();

        // Load into memory
        let mut memory = MemoryChunkReader::new();
        for block in &blocks {
            memory.push(BlockData {
                block_number: block.block_number,
                tables: block.tables.clone(),
            });
        }

        // Flush to spillover parquet
        let block_refs: Vec<Arc<BlockData>> = blocks.iter().map(|b| Arc::new(BlockData {
            block_number: b.block_number,
            tables: b.tables.clone(),
        })).collect();
        let spill_dir = tmp.path().join("spill");
        flush_to_parquet(&block_refs, &spill_dir, &meta).unwrap();

        // Query both — should return same row count
        let mem_rows = query_block_range(&memory, from, to);

        if spill_dir.exists() {
            let spillover = ParquetChunkReader::open(&spill_dir).unwrap();
            let spill_rows = query_block_range(&spillover, from, to);
            prop_assert_eq!(mem_rows, spill_rows,
                "memory={} vs spillover={} for blocks {}..{}", mem_rows, spill_rows, from, to);
        }
    }

    /// Chunk dir name round-trip.
    #[test]
    fn fuzz_chunk_dir_name_roundtrip(
        first in 0u64..u64::MAX/2,
        last in 0u64..u64::MAX/2,
    ) {
        let (first, last) = if first <= last { (first, last) } else { (last, first) };
        let name = chunk_dir_name(first, last);
        let parsed = parse_chunk_dir_name(&name);
        prop_assert_eq!(parsed, Some((first, last)));
    }
}
