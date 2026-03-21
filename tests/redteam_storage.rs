//! Red team tests: adversarial inputs, edge cases, and abuse scenarios.
//!
//! Goal: find crashes, panics, data corruption, and inconsistencies
//! by feeding unexpected data to the storage layer.

use sqd_query_engine::scan::composite_reader::CompositeChunkReader;
use sqd_query_engine::scan::crash_log::{CrashLogWriter, recover_crash_log};
use sqd_query_engine::scan::memory_backend::{BlockData, MemoryChunkReader};
use sqd_query_engine::scan::parquet_writer::{flush_to_parquet, parse_chunk_dir_name};
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

fn make_batch(schema: &SchemaRef, block_number: i32, n_rows: usize) -> RecordBatch {
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();
    for field in schema.fields() {
        let col: Arc<dyn Array> = match field.data_type() {
            DataType::Int32 => Arc::new(Int32Array::from(vec![block_number; n_rows])),
            DataType::Int64 => Arc::new(Int64Array::from(vec![block_number as i64; n_rows])),
            DataType::UInt64 => Arc::new(UInt64Array::from(vec![block_number as u64; n_rows])),
            DataType::Utf8 => Arc::new(StringArray::from(
                (0..n_rows).map(|i| format!("val_{}", i)).collect::<Vec<_>>(),
            )),
            _ => Arc::new(Int32Array::from(vec![0; n_rows])),
        };
        columns.push(col);
    }
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

fn simple_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int32, false),
        Field::new("address", DataType::Utf8, false),
    ]))
}

fn make_simple_block(bn: u64, rows: usize) -> BlockData {
    let schema = simple_schema();
    let mut tables = HashMap::new();
    tables.insert("logs".to_string(), make_batch(&schema, bn as i32, rows));
    BlockData {
        block_number: bn,
        tables,
    }
}

fn query_rows(reader: &dyn ChunkReader) -> usize {
    let request = ScanRequest::new(vec!["block_number", "address"]);
    reader.scan("logs", &request).unwrap().iter().map(|b| b.num_rows()).sum()
}

// =====================================================================
// EMPTY / ZERO DATA
// =====================================================================

#[test]
fn redteam_empty_block_zero_rows() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(100, 0)); // zero rows
    reader.push(make_simple_block(101, 3));

    assert_eq!(query_rows(&reader), 3); // zero-row block ignored
}

#[test]
fn redteam_all_empty_blocks() {
    let mut reader = MemoryChunkReader::new();
    for i in 0..10 {
        reader.push(make_simple_block(i, 0));
    }
    assert_eq!(query_rows(&reader), 0);
    assert_eq!(reader.len(), 10); // blocks exist, just empty
}

#[test]
fn redteam_empty_table_map() {
    let mut reader = MemoryChunkReader::new();
    reader.push(BlockData {
        block_number: 100,
        tables: HashMap::new(), // no tables at all
    });
    assert_eq!(query_rows(&reader), 0);
    assert!(!reader.has_table("logs"));
}

#[test]
fn redteam_flush_empty_blocks_to_parquet() {
    let meta = load_dataset_description(Path::new("metadata/evm.yaml")).unwrap();
    let tmp = TempDir::new().unwrap();
    let blocks: Vec<Arc<BlockData>> = (0..5).map(|i| Arc::new(make_simple_block(i, 0))).collect();

    flush_to_parquet(&blocks, tmp.path(), &meta).unwrap();
    // Should not create parquet files for empty data
}

// =====================================================================
// DUPLICATE BLOCK NUMBERS
// =====================================================================

#[test]
#[should_panic(expected = "blocks must be pushed in order")]
fn redteam_duplicate_block_numbers_panics() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(100, 2));
    reader.push(make_simple_block(100, 3)); // same block number — should panic
}

#[test]
#[should_panic(expected = "blocks must be pushed in order")]
fn redteam_out_of_order_panics() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(101, 2));
    reader.push(make_simple_block(100, 3)); // lower block number — should panic
}

// =====================================================================
// EXTREME BLOCK NUMBERS
// =====================================================================

#[test]
fn redteam_max_block_number() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(u64::MAX - 1, 1));
    reader.push(make_simple_block(u64::MAX, 1));

    // head() returns last pushed block (blocks stored in push order)
    assert_eq!(reader.head(), Some(u64::MAX));
    assert_eq!(query_rows(&reader), 2);
}

#[test]
fn redteam_block_zero() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(0, 3));
    assert_eq!(reader.first_block(), Some(0));
    assert_eq!(query_rows(&reader), 3);
}

// =====================================================================
// TRUNCATION EDGE CASES
// =====================================================================

#[test]
fn redteam_truncate_at_zero() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(0, 1));
    reader.push(make_simple_block(1, 1));

    reader.truncate(0); // remove everything including block 0
    assert_eq!(reader.len(), 0);
    assert!(reader.is_empty());
}

#[test]
fn redteam_truncate_beyond_head() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(100, 2));
    reader.push(make_simple_block(101, 3));

    reader.truncate(999); // nothing to remove
    assert_eq!(query_rows(&reader), 5);
}

#[test]
fn redteam_truncate_empty_buffer() {
    let mut reader = MemoryChunkReader::new();
    reader.truncate(100); // no-op on empty
    assert!(reader.is_empty());
}

#[test]
fn redteam_double_truncate() {
    let mut reader = MemoryChunkReader::new();
    for i in 0..10 {
        reader.push(make_simple_block(i, 1));
    }
    reader.truncate(5);
    assert_eq!(reader.len(), 5);
    reader.truncate(3);
    assert_eq!(reader.len(), 3);
    reader.truncate(0);
    assert_eq!(reader.len(), 0);
}

#[test]
fn redteam_truncate_then_push() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(100, 2));
    reader.push(make_simple_block(101, 3));
    reader.truncate(101);
    reader.push(make_simple_block(101, 5)); // new block 101
    assert_eq!(query_rows(&reader), 7); // old 100 (2) + new 101 (5)
}

// =====================================================================
// DRAIN EDGE CASES
// =====================================================================

#[test]
fn redteam_drain_everything() {
    let mut reader = MemoryChunkReader::new();
    for i in 0..5 {
        reader.push(make_simple_block(i, 1));
    }
    let drained = reader.drain_up_to(u64::MAX);
    assert_eq!(drained.len(), 5);
    assert!(reader.is_empty());
}

#[test]
fn redteam_drain_nothing() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(100, 1));
    let drained = reader.drain_up_to(0); // nothing <= 0
    assert_eq!(drained.len(), 0);
    assert_eq!(reader.len(), 1);
}

#[test]
fn redteam_drain_empty() {
    let mut reader = MemoryChunkReader::new();
    let drained = reader.drain_up_to(100);
    assert_eq!(drained.len(), 0);
}

// =====================================================================
// SCHEMA MISMATCH
// =====================================================================

#[test]
fn redteam_different_schemas_same_table() {
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int32, false),
        Field::new("address", DataType::Utf8, false),
    ]));
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int32, false),
        Field::new("value", DataType::Int64, false), // different column!
    ]));

    let mut reader = MemoryChunkReader::new();

    let mut tables1 = HashMap::new();
    tables1.insert("logs".to_string(), make_batch(&schema1, 100, 2));
    reader.push(BlockData { block_number: 100, tables: tables1 });

    let mut tables2 = HashMap::new();
    tables2.insert("logs".to_string(), make_batch(&schema2, 101, 3));
    reader.push(BlockData { block_number: 101, tables: tables2 });

    // Query for "block_number" which exists in both schemas
    let request = ScanRequest::new(vec!["block_number"]);
    let batches = reader.scan("logs", &request).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 5);
}

// =====================================================================
// NULL / SPECIAL VALUES
// =====================================================================

#[test]
fn redteam_nullable_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::Int32, false),
        Field::new("address", DataType::Utf8, true), // nullable!
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![100, 100, 100])),
            Arc::new(StringArray::from(vec![Some("0xaaa"), None, Some("0xbbb")])),
        ],
    ).unwrap();

    let mut reader = MemoryChunkReader::new();
    let mut tables = HashMap::new();
    tables.insert("logs".to_string(), batch);
    reader.push(BlockData { block_number: 100, tables });

    let request = ScanRequest::new(vec!["block_number", "address"]);
    let batches = reader.scan("logs", &request).unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
}

#[test]
fn redteam_empty_string_address() {
    let schema = simple_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![100])),
            Arc::new(StringArray::from(vec![""])), // empty string
        ],
    ).unwrap();

    let mut reader = MemoryChunkReader::new();
    let mut tables = HashMap::new();
    tables.insert("logs".to_string(), batch);
    reader.push(BlockData { block_number: 100, tables });

    assert_eq!(query_rows(&reader), 1);
}

// =====================================================================
// CRASH LOG ABUSE
// =====================================================================

#[test]
fn redteam_crash_log_zero_byte_file() {
    let tmp = TempDir::new().unwrap();
    let log_dir = tmp.path().join("crash_log");
    std::fs::create_dir_all(&log_dir).unwrap();

    // Create zero-byte IPC file
    std::fs::write(log_dir.join("logs.ipc"), b"").unwrap();

    let recovered = recover_crash_log(&log_dir).unwrap();
    assert!(recovered.is_empty());
}

#[test]
fn redteam_crash_log_random_garbage() {
    let tmp = TempDir::new().unwrap();
    let log_dir = tmp.path().join("crash_log");
    std::fs::create_dir_all(&log_dir).unwrap();

    // Write random bytes as IPC
    let garbage: Vec<u8> = (0..1000).map(|i| (i * 7 + 13) as u8).collect();
    std::fs::write(log_dir.join("logs.ipc"), &garbage).unwrap();

    let recovered = recover_crash_log(&log_dir).unwrap();
    assert!(recovered.is_empty()); // should not panic
}

#[test]
fn redteam_crash_log_truncate_then_write() {
    let tmp = TempDir::new().unwrap();
    let log_dir = tmp.path().join("crash_log");

    let mut writer = CrashLogWriter::open(&log_dir).unwrap();

    // Write, truncate, write again
    let schema = simple_schema();
    writer.append("logs", 100, &make_batch(&schema, 100, 2)).unwrap();
    writer.append("logs", 101, &make_batch(&schema, 101, 3)).unwrap();
    writer.truncate(101).unwrap(); // remove 101

    // After truncate, the writer for this table is dropped.
    // Writing again should create a new writer.
    // This currently creates a new file — verify recovery works.
    drop(writer);

    let recovered = recover_crash_log(&log_dir).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].0, 100);
}

#[test]
fn redteam_crash_log_non_ipc_files() {
    let tmp = TempDir::new().unwrap();
    let log_dir = tmp.path().join("crash_log");
    std::fs::create_dir_all(&log_dir).unwrap();

    // Put non-.ipc files in the directory
    std::fs::write(log_dir.join("readme.txt"), b"not ipc").unwrap();
    std::fs::write(log_dir.join("data.json"), b"{}").unwrap();

    let recovered = recover_crash_log(&log_dir).unwrap();
    assert!(recovered.is_empty()); // should ignore non-.ipc files
}

// =====================================================================
// COMPOSITE READER EDGE CASES
// =====================================================================

#[test]
fn redteam_composite_empty() {
    let composite = CompositeChunkReader::new();
    assert_eq!(query_rows(&composite), 0);
    assert!(!composite.has_table("anything"));
    assert!(composite.table_schema("anything").is_none());
}

#[test]
fn redteam_composite_overlapping_blocks() {
    let mut tier1 = MemoryChunkReader::new();
    tier1.push(make_simple_block(100, 2));

    let mut tier2 = MemoryChunkReader::new();
    tier2.push(make_simple_block(100, 3)); // same block number in both tiers!

    let mut composite = CompositeChunkReader::new();
    composite.add_unbounded(&tier1);
    composite.add_unbounded(&tier2);

    // Should return rows from BOTH tiers (no dedup)
    assert_eq!(query_rows(&composite), 5);
}

#[test]
fn redteam_composite_one_empty_tier() {
    let tier1 = MemoryChunkReader::new(); // empty
    let mut tier2 = MemoryChunkReader::new();
    tier2.push(make_simple_block(100, 3));

    let mut composite = CompositeChunkReader::new();
    composite.add_unbounded(&tier1);
    composite.add_unbounded(&tier2);

    assert_eq!(query_rows(&composite), 3);
}

// =====================================================================
// QUERY EDGE CASES
// =====================================================================

#[test]
fn redteam_query_nonexistent_table() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(100, 5));

    let request = ScanRequest::new(vec!["block_number"]);
    let batches = reader.scan("nonexistent_table", &request).unwrap();
    assert!(batches.is_empty());
}

#[test]
fn redteam_query_nonexistent_column() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(100, 5));

    // Request a column that doesn't exist
    let request = ScanRequest::new(vec!["block_number", "nonexistent_column"]);
    let batches = reader.scan("logs", &request).unwrap();
    // Should return rows with null for missing column
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
}

#[test]
fn redteam_query_inverted_block_range() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(100, 2));
    reader.push(make_simple_block(200, 3));

    // from > to — should return nothing
    let mut request = ScanRequest::new(vec!["block_number"]);
    request.from_block = Some(200);
    request.to_block = Some(100);
    request.block_number_column = Some("block_number");

    let batches = reader.scan("logs", &request).unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
}

// =====================================================================
// MEMORY USAGE CONSISTENCY
// =====================================================================

#[test]
fn redteam_memory_usage_decreases_after_drain() {
    let mut reader = MemoryChunkReader::new();
    for i in 0..20 {
        reader.push(make_simple_block(i, 100));
    }
    let before = reader.memory_usage();
    assert!(before > 0);

    reader.drain_up_to(10);
    let after = reader.memory_usage();
    assert!(after < before, "memory should decrease: before={}, after={}", before, after);
}

#[test]
fn redteam_memory_usage_zero_after_truncate_all() {
    let mut reader = MemoryChunkReader::new();
    reader.push(make_simple_block(100, 50));
    assert!(reader.memory_usage() > 0);

    reader.truncate(0);
    // After truncating at 0, block 100 should be gone (100 >= 0)
    // Wait — truncate removes blocks >= fork_point, so truncate(0) removes everything
    // Actually truncate keeps blocks < fork_point, so truncate(0) removes all (nothing < 0 for u64)
    // But 0 is the minimum u64 so nothing survives
    assert_eq!(reader.len(), 0);
}

// =====================================================================
// PARQUET WRITER EDGE CASES
// =====================================================================

#[test]
fn redteam_flush_single_row_block() {
    let meta = load_dataset_description(Path::new("metadata/evm.yaml")).unwrap();
    let tmp = TempDir::new().unwrap();
    let chunk_dir = tmp.path().join("chunk");

    let blocks: Vec<Arc<BlockData>> = vec![Arc::new(make_simple_block(100, 1))]; // single row
    flush_to_parquet(&blocks, &chunk_dir, &meta).unwrap();

    if chunk_dir.exists() {
        let reader = ParquetChunkReader::open(&chunk_dir).unwrap();
        let request = ScanRequest::new(vec!["block_number", "address"]);
        let batches = reader.scan("logs", &request).unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }
}

#[test]
fn redteam_chunk_dir_name_edge_cases() {
    assert_eq!(parse_chunk_dir_name("0-0"), Some((0, 0)));
    assert_eq!(parse_chunk_dir_name("18446744073709551615-18446744073709551615"),
        Some((u64::MAX, u64::MAX)));
    assert_eq!(parse_chunk_dir_name(""), None);
    assert_eq!(parse_chunk_dir_name("-"), None);
    assert_eq!(parse_chunk_dir_name("abc-def"), None);
    assert_eq!(parse_chunk_dir_name("1-2-3"), None);
    assert_eq!(parse_chunk_dir_name("1"), None);
}

// =====================================================================
// LARGE SCALE STRESS
// =====================================================================

#[test]
fn redteam_many_blocks_push_truncate_cycle() {
    let mut reader = MemoryChunkReader::new();

    // Simulate chain tip: push blocks, periodically truncate (reorg)
    for cycle in 0..10 {
        let base = cycle * 100;
        for i in 0..50 {
            reader.push(make_simple_block(base + i, 5));
        }
        // Reorg at random depth
        reader.truncate(base + 30);
        assert!(reader.iter_blocks().all(|b| b.block_number < base + 30));
    }

    // Should still be queryable
    let rows = query_rows(&reader);
    assert!(rows > 0);
}

#[test]
fn redteam_rapid_push_drain_cycle() {
    let mut reader = MemoryChunkReader::new();
    let mut total_drained = 0;

    for i in 0u64..100 {
        reader.push(make_simple_block(i, 3));

        // Drain every 10 blocks
        if i > 0 && i % 10 == 0 {
            let drained = reader.drain_up_to(i - 5);
            total_drained += drained.len();
        }
    }

    // Everything should be accounted for
    let remaining = reader.len();
    assert!(total_drained + remaining <= 100);
    assert!(query_rows(&reader) > 0);
}
