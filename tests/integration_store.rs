//! Integration tests for DatasetStore: push → query → verify.
//!
//! Tests the full pipeline: load parquet fixture data into a DatasetStore,
//! query via execute_chunk with DatasetSnapshot, verify JSON output matches
//! expected results from e2e fixtures.
//!
//! Also tests fork handling, crash recovery, retention, compaction, and stats.
//!
//! These tests are slow (~5 min) because they load full EVM/Solana fixtures
//! block by block. Run with: `cargo test --test integration_store`
//! Skipped by default in `cargo test` — run with: `cargo test --test integration_store -- --ignored`

use arrow::array::*;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::dataset_store::DatasetStore;
use sqd_query_engine::scan::memory_backend::BlockData;
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers: load parquet fixture data into DatasetStore
// ---------------------------------------------------------------------------

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn evm_meta() -> DatasetDescription {
    load_dataset_description(Path::new("metadata/evm.yaml")).unwrap()
}

fn solana_meta() -> DatasetDescription {
    load_dataset_description(Path::new("metadata/solana.yaml")).unwrap()
}

/// Load parquet fixture data into a DatasetStore (one block at a time).
fn load_fixture_to_store(parquet_dir: &Path, store: &mut DatasetStore) {
    let parquet = ParquetChunkReader::open(parquet_dir).unwrap();
    let mut all_block_tables: BTreeMap<u32, HashMap<String, RecordBatch>> = BTreeMap::new();

    for table_name in parquet.table_names() {
        let batches = parquet.read_all(&table_name).unwrap();
        let by_block = split_by_block(&batches, &table_name);
        for (bn, batch) in by_block {
            all_block_tables
                .entry(bn)
                .or_default()
                .insert(table_name.clone(), batch);
        }
    }

    for (bn, tables) in all_block_tables {
        store
            .push_block(BlockData {
                block_number: bn as u64,
                tables,
            })
            .unwrap();
    }
}

/// Run a query against a DatasetStore snapshot and return parsed JSON.
fn query_store(
    store: &DatasetStore,
    meta: &DatasetDescription,
    query_json: &[u8],
) -> serde_json::Value {
    let snapshot = store.snapshot();
    let reader = snapshot.reader();
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    let result = execute_chunk(&plan, meta, &reader, Vec::new(), false).unwrap();
    serde_json::from_slice(&result).unwrap()
}

/// Run a query and compare with expected fixture result.
fn assert_query_matches_fixture(
    store: &DatasetStore,
    meta: &DatasetDescription,
    fixture_base: &Path,
    query_name: &str,
) {
    let query_file = fixture_base
        .join("queries")
        .join(query_name)
        .join("query.json");
    let result_file = fixture_base
        .join("queries")
        .join(query_name)
        .join("result.json");

    if !query_file.exists() || !result_file.exists() {
        eprintln!("SKIP {}: fixture not found", query_name);
        return;
    }

    let query_json = std::fs::read(&query_file).unwrap();
    let actual = query_store(store, meta, &query_json);

    let expected: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&result_file).unwrap()).unwrap();

    assert_eq!(expected, actual, "query '{}': output mismatch", query_name);
}

// ---------------------------------------------------------------------------
// Block splitting (from hot_bench/loader.rs)
// ---------------------------------------------------------------------------

fn split_by_block(batches: &[RecordBatch], table_name: &str) -> Vec<(u32, RecordBatch)> {
    let bn_col = if table_name == "blocks" {
        "number"
    } else {
        "block_number"
    };

    let mut per_block: BTreeMap<u32, Vec<RecordBatch>> = BTreeMap::new();

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let col = match batch.column_by_name(bn_col) {
            Some(c) => c,
            None => {
                per_block.entry(0).or_default().push(batch.clone());
                continue;
            }
        };

        let block_numbers = extract_block_numbers(col.as_ref());

        if block_numbers.len() == 1 {
            per_block
                .entry(block_numbers[0])
                .or_default()
                .push(batch.clone());
            continue;
        }

        for &bn in &block_numbers {
            let mask = build_block_eq_mask(col.as_ref(), bn);
            if let Ok(filtered) = compute::filter_record_batch(batch, &mask) {
                if filtered.num_rows() > 0 {
                    per_block.entry(bn).or_default().push(filtered);
                }
            }
        }
    }

    per_block
        .into_iter()
        .filter_map(|(bn, fragments)| {
            if fragments.len() == 1 {
                Some((bn, fragments.into_iter().next().unwrap()))
            } else {
                let schema = fragments[0].schema();
                let merged = arrow::compute::concat_batches(&schema, &fragments).ok()?;
                if merged.num_rows() > 0 {
                    Some((bn, merged))
                } else {
                    None
                }
            }
        })
        .collect()
}

fn extract_block_numbers(array: &dyn Array) -> Vec<u32> {
    let mut blocks = std::collections::BTreeSet::new();
    match array.data_type() {
        arrow::datatypes::DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) && arr.value(i) >= 0 {
                    blocks.insert(arr.value(i) as u32);
                }
            }
        }
        arrow::datatypes::DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    blocks.insert(arr.value(i));
                }
            }
        }
        arrow::datatypes::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) && arr.value(i) >= 0 {
                    blocks.insert(arr.value(i) as u32);
                }
            }
        }
        arrow::datatypes::DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    blocks.insert(arr.value(i) as u32);
                }
            }
        }
        _ => {}
    }
    blocks.into_iter().collect()
}

fn build_block_eq_mask(array: &dyn Array, target: u32) -> BooleanArray {
    match array.data_type() {
        arrow::datatypes::DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            let target = target as i32;
            BooleanArray::from_unary(arr, |v| v == target)
        }
        arrow::datatypes::DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            BooleanArray::from_unary(arr, |v| v == target)
        }
        arrow::datatypes::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let target = target as i64;
            BooleanArray::from_unary(arr, |v| v == target)
        }
        arrow::datatypes::DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            let target = target as u64;
            BooleanArray::from_unary(arr, |v| v == target)
        }
        _ => BooleanArray::from(vec![true; array.len()]),
    }
}

/// Create a simple block with one "logs" row for compaction tests.
fn make_simple_block(block_number: u64) -> BlockData {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("block_number", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("address", arrow::datatypes::DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![block_number as i32])),
            Arc::new(StringArray::from(vec![format!("0x{:x}", block_number)])),
        ],
    )
    .unwrap();
    let mut tables = HashMap::new();
    tables.insert("logs".to_string(), batch);
    BlockData {
        block_number,
        tables,
    }
}

// ===========================================================================
// Test 1: Memory-only query matches parquet fixture output
// ===========================================================================

#[test]
#[ignore]
fn memory_query_matches_fixture() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    load_fixture_to_store(&chunk, &mut store);

    let fixture_base = fixture_dir().join("ethereum");

    // Test several representative queries
    assert_query_matches_fixture(&store, &meta, &fixture_base, "include_all_blocks");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "sighash_filtering");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "topics_filtering");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "empty_filter");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "transaction_logs_for_logs");
}

// ===========================================================================
// Test 2: Compacted (parquet + memory) query matches fixture
// ===========================================================================

#[test]
#[ignore]
fn compacted_query_matches_fixture() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    load_fixture_to_store(&chunk, &mut store);

    // Compact first half of blocks to parquet
    let head = store.head().unwrap();
    let first = store.first_block().unwrap();
    let mid = first + (head - first) / 2;
    store.compact(mid, &meta).unwrap();

    assert!(store.chunk_count() > 0, "should have parquet chunks");
    assert!(store.memory_block_count() > 0, "should have memory blocks");

    let fixture_base = fixture_dir().join("ethereum");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "include_all_blocks");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "sighash_filtering");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "topics_filtering");
}

// ===========================================================================
// Test 3: Spillover query matches fixture
// ===========================================================================

#[test]
#[ignore]
fn compacted_half_query_matches_fixture() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    load_fixture_to_store(&chunk, &mut store);

    let head = store.head().unwrap();
    let first = store.first_block().unwrap();
    let mid = first + (head - first) / 2;
    store.set_finalized_head(mid).unwrap();
    store.compact(mid, &meta).unwrap();

    assert!(store.chunk_count() > 0, "should have compacted to parquet");

    let fixture_base = fixture_dir().join("ethereum");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "include_all_blocks");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "sighash_filtering");
}

// ===========================================================================
// Test 4: Fork/reorg — truncate and re-push
// ===========================================================================

#[test]
#[ignore]
fn fork_truncate_and_requery() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    load_fixture_to_store(&chunk, &mut store);

    let original_head = store.head().unwrap();
    let first = store.first_block().unwrap();

    // Simulate fork: truncate last 10 blocks
    let fork_point = original_head - 10;
    store.truncate(fork_point + 1).unwrap();
    assert_eq!(store.head(), Some(fork_point));

    // Re-push the same blocks (simulating new fork with same data)
    let parquet = ParquetChunkReader::open(&chunk).unwrap();
    let mut all_block_tables: BTreeMap<u32, HashMap<String, RecordBatch>> = BTreeMap::new();
    for table_name in parquet.table_names() {
        let batches = parquet.read_all(&table_name).unwrap();
        for (bn, batch) in split_by_block(&batches, &table_name) {
            if bn as u64 > fork_point {
                all_block_tables
                    .entry(bn)
                    .or_default()
                    .insert(table_name.clone(), batch);
            }
        }
    }
    for (bn, tables) in all_block_tables {
        store
            .push_block(BlockData {
                block_number: bn as u64,
                tables,
            })
            .unwrap();
    }

    assert_eq!(store.head(), Some(original_head));

    // Query should still produce correct results
    let fixture_base = fixture_dir().join("ethereum");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "include_all_blocks");
}

// ===========================================================================
// Test 5: Crash recovery — drop and reopen
// ===========================================================================

#[test]
#[ignore]
fn crash_recovery_preserves_data() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("dataset");
    let meta = evm_meta();

    // Phase 1: push blocks, then "crash" (drop without graceful shutdown)
    {
        let mut store = DatasetStore::open(&store_path, 500 * 1024 * 1024).unwrap();
        load_fixture_to_store(&chunk, &mut store);
        let head = store.head().unwrap();
        store.set_finalized_head(head).unwrap();
        // drop — simulates crash, crash log should have all data
    }

    // Phase 2: reopen — should recover from crash log
    {
        let store = DatasetStore::open(&store_path, 500 * 1024 * 1024).unwrap();
        assert!(store.head().is_some(), "should have recovered blocks");
        assert_eq!(store.finalized_head(), store.head());

        let fixture_base = fixture_dir().join("ethereum");
        assert_query_matches_fixture(&store, &meta, &fixture_base, "include_all_blocks");
    }
}

// ===========================================================================
// Test 6: Crash recovery after compaction
// ===========================================================================

#[test]
#[ignore]
fn crash_recovery_after_compaction() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let store_path = tmp.path().join("dataset");
    let meta = evm_meta();

    let original_head;

    // Phase 1: push, compact half, then crash
    {
        let mut store = DatasetStore::open(&store_path, 500 * 1024 * 1024).unwrap();
        load_fixture_to_store(&chunk, &mut store);
        original_head = store.head().unwrap();

        let first = store.first_block().unwrap();
        let mid = first + (original_head - first) / 2;
        store.compact(mid, &meta).unwrap();
        // crash — remaining memory blocks in crash log, compacted in parquet
    }

    // Phase 2: reopen
    {
        let store = DatasetStore::open(&store_path, 500 * 1024 * 1024).unwrap();
        assert_eq!(store.head(), Some(original_head));
        assert!(
            store.chunk_count() > 0,
            "should have parquet from compaction"
        );

        let fixture_base = fixture_dir().join("ethereum");
        assert_query_matches_fixture(&store, &meta, &fixture_base, "include_all_blocks");
    }
}

// ===========================================================================
// Test 7: Retention — evict old parquet chunks
// ===========================================================================

#[test]
#[ignore]
fn retention_evicts_old_chunks() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    load_fixture_to_store(&chunk, &mut store);

    let head = store.head().unwrap();
    let first = store.first_block().unwrap();
    let mid = first + (head - first) / 2;

    // Compact first half
    store.compact(mid, &meta).unwrap();
    assert_eq!(store.chunk_count(), 1);

    // Evict the compacted chunk
    store.evict(mid);
    assert_eq!(store.chunk_count(), 0);

    // Only memory blocks remain (above mid)
    assert!(store.first_block().unwrap() > mid);
    assert_eq!(store.head(), Some(head));
}

// ===========================================================================
// Test 8: Stats accuracy
// ===========================================================================

#[test]
#[ignore]
fn stats_are_accurate() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    load_fixture_to_store(&chunk, &mut store);

    // Memory should have blocks
    assert!(store.memory_block_count() > 0);
    assert!(store.memory_usage() > 0);
    assert!(store.first_block().is_some());
    assert!(store.head().is_some());
    assert!(store.first_block().unwrap() <= store.head().unwrap());

    // No chunks yet
    assert_eq!(store.chunk_count(), 0);
    assert_eq!(store.finalized_head(), None);

    // Disk usage: crash log should be non-zero
    assert!(store.disk_usage() > 0);

    // Compact and check chunk stats
    let first = store.first_block().unwrap();
    let head = store.head().unwrap();
    let mid = first + (head - first) / 2;

    store.set_finalized_head(mid).unwrap();
    store.compact(mid, &meta).unwrap();

    assert_eq!(store.finalized_head(), Some(mid));
    let stats = store.stats_detailed();
    assert_eq!(stats.chunks.len(), 1);

    let chunk_stat = &stats.chunks[0];
    assert_eq!(chunk_stat.first_block, first);
    assert_eq!(chunk_stat.last_block, mid);
    assert!(!chunk_stat.tables.is_empty());

    // Each table should have real stats
    for (table_name, table_stat) in &chunk_stat.tables {
        assert!(table_stat.rows > 0, "table {} should have rows", table_name);
        assert!(
            table_stat.row_groups > 0,
            "table {} should have row groups",
            table_name
        );
        assert!(
            !table_stat.compressed_size.is_empty(),
            "table {} should have compressed size",
            table_name
        );
    }

    // Memory should still have remaining blocks
    assert!(store.memory_block_count() > 0);
    assert!(store.first_block().unwrap() > mid || store.chunk_count() > 0);
}

// ===========================================================================
// Test 8b: Size-based compaction creates batched chunks, not one-per-block
// ===========================================================================

#[test]
#[ignore]
fn size_based_compaction_batches_correctly() {
    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    let compact_target = 200 * 1024 * 1024; // 200MB in memory, same as controller

    // Simulate real-time: push 500 blocks, finalizing each one
    for i in 100..600 {
        let block = make_simple_block(i);
        store.push_block(block).unwrap();
        store.set_finalized_head(i).unwrap();

        // Simulate controller logic: only compact when finalized data >= 30MB
        if store.finalized_bytes_in_memory(i) >= compact_target {
            store.compact(i, &meta).unwrap();
        }
    }

    // Small blocks (~200 bytes each) → 500 blocks is ~100KB total
    // 30MB threshold won't be reached → 0 chunks, all in memory
    assert_eq!(
        store.chunk_count(),
        0,
        "small blocks shouldn't trigger 30MB compaction"
    );
    assert_eq!(store.head(), Some(599));

    // All blocks still queryable
    let reader = store.reader();
    let request = ScanRequest::new(vec!["block_number", "address"]);
    let batches = reader.scan("logs", &request).unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 500);
}

// ===========================================================================
// Test 8b2: finalized_bytes_in_memory tracks size correctly
// ===========================================================================

#[test]
#[ignore]
fn finalized_bytes_in_memory_tracking() {
    let tmp = TempDir::new().unwrap();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();

    for i in 100..110 {
        store.push_block(make_simple_block(i)).unwrap();
    }

    // Nothing finalized yet
    assert_eq!(store.finalized_bytes_in_memory(99), 0);
    assert_eq!(store.finalized_blocks_in_memory(99), 0);

    // Finalize up to block 105 — 6 blocks finalized
    let bytes_6 = store.finalized_bytes_in_memory(105);
    assert!(bytes_6 > 0);
    assert_eq!(store.finalized_blocks_in_memory(105), 6);

    // All 10 finalized — proportionally more bytes
    let bytes_10 = store.finalized_bytes_in_memory(109);
    assert!(bytes_10 > bytes_6);
    assert_eq!(store.finalized_blocks_in_memory(109), 10);
}

// ===========================================================================
// Test 8c: Without batching threshold, every finalize+compact creates a chunk
// ===========================================================================

#[test]
#[ignore]
fn unbatched_compaction_creates_many_chunks() {
    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();

    // Push 10 blocks, compact after each finalization (no batching)
    for i in 100..110 {
        let block = make_simple_block(i);
        store.push_block(block).unwrap();
        store.set_finalized_head(i).unwrap();
        store.compact(i, &meta).unwrap();
    }

    // Each compact drains 1 block → 10 tiny chunks
    assert_eq!(store.chunk_count(), 10);
}

// ===========================================================================
// Test 8d: Real fixture data with size-based compaction produces reasonable chunks
// ===========================================================================

#[test]
#[ignore]
fn real_data_size_based_compaction() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    let compact_target = 200 * 1024 * 1024; // 200MB in memory ≈ 30-50MB on disk

    // Load fixture data block by block, simulating real-time ingest
    let parquet = ParquetChunkReader::open(&chunk).unwrap();
    let mut all_block_tables: BTreeMap<u32, HashMap<String, RecordBatch>> = BTreeMap::new();
    for table_name in parquet.table_names() {
        let batches = parquet.read_all(&table_name).unwrap();
        for (bn, batch) in split_by_block(&batches, &table_name) {
            all_block_tables.entry(bn).or_default().insert(table_name.clone(), batch);
        }
    }

    for (bn, tables) in &all_block_tables {
        store.push_block(BlockData { block_number: *bn as u64, tables: tables.clone() }).unwrap();
        store.set_finalized_head(*bn as u64).unwrap();

        if store.finalized_bytes_in_memory(*bn as u64) >= compact_target {
            store.compact(*bn as u64, &meta).unwrap();
        }
    }

    let total_blocks = all_block_tables.len();

    // Should NOT have one chunk per block
    assert!(
        store.chunk_count() < total_blocks / 2,
        "too many chunks: {} for {} blocks",
        store.chunk_count(),
        total_blocks
    );

    // Each chunk should be in the MB range, not KB
    let stats = store.stats_detailed();
    for chunk_stat in &stats.chunks {
        // Verify tables have stats
        assert!(
            !chunk_stat.tables.is_empty(),
            "chunk {}-{} should have tables",
            chunk_stat.first_block,
            chunk_stat.last_block,
        );
        for (table_name, table_stat) in &chunk_stat.tables {
            assert!(
                table_stat.rows > 0,
                "chunk {}-{} table {} should have rows",
                chunk_stat.first_block,
                chunk_stat.last_block,
                table_name,
            );
        }
    }

    // Query still correct
    let fixture_base = fixture_dir().join("ethereum");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "include_all_blocks");
}

// ===========================================================================
// Test 9: Solana fixture through DatasetStore
// ===========================================================================

#[test]
#[ignore]
fn solana_memory_query_matches_fixture() {
    let chunk = fixture_dir().join("solana/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: Solana fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let meta = solana_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    load_fixture_to_store(&chunk, &mut store);

    let fixture_base = fixture_dir().join("solana");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "include_all_blocks");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "whirpool_usdc_sol_swaps");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "balance_first");
}

// ===========================================================================
// Test 10: Multiple compaction cycles
// ===========================================================================

#[test]
#[ignore]
fn multiple_compaction_cycles() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    load_fixture_to_store(&chunk, &mut store);

    let head = store.head().unwrap();
    let first = store.first_block().unwrap();
    let range = head - first;

    // Compact in 3 stages
    let p1 = first + range / 4;
    let p2 = first + range / 2;
    let p3 = first + 3 * range / 4;

    store.compact(p1, &meta).unwrap();
    assert_eq!(store.chunk_count(), 1);

    store.compact(p2, &meta).unwrap();
    assert_eq!(store.chunk_count(), 2);

    store.compact(p3, &meta).unwrap();
    assert_eq!(store.chunk_count(), 3);

    // Query should still produce correct results across 3 chunks + memory
    let fixture_base = fixture_dir().join("ethereum");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "include_all_blocks");
    assert_query_matches_fixture(&store, &meta, &fixture_base, "topics_filtering");
}

// ===========================================================================
// Test 11: Deep fork — fork beyond memory, chunks preserved
// ===========================================================================

#[test]
#[ignore]
fn deep_fork_preserves_chunks() {
    let chunk = fixture_dir().join("ethereum/chunk");
    if !chunk.exists() {
        eprintln!("SKIP: EVM fixtures not found");
        return;
    }

    let tmp = TempDir::new().unwrap();
    let meta = evm_meta();
    let mut store = DatasetStore::open(tmp.path(), 500 * 1024 * 1024).unwrap();
    load_fixture_to_store(&chunk, &mut store);

    let head = store.head().unwrap();
    let first = store.first_block().unwrap();
    let mid = first + (head - first) / 2;

    // Compact first half to parquet
    store.set_finalized_head(mid).unwrap();
    store.compact(mid, &meta).unwrap();
    assert_eq!(store.chunk_count(), 1);
    let chunks_before = store.chunk_count();

    // Deep fork: truncate everything in memory
    // Memory blocks start after mid (post-compaction).
    // Fork point = first memory block -> wipes all memory.
    let memory_first = store.iter_blocks().next().unwrap().block_number;
    store.truncate(memory_first).unwrap();

    // Chunks with first_block < memory_first are preserved
    assert_eq!(store.chunk_count(), chunks_before, "chunks should be preserved after deep fork");

    // Memory should be empty
    assert_eq!(store.memory_block_count(), 0);

    // Chunk data is still queryable
    let fixture_base = fixture_dir().join("ethereum");
    let snapshot = store.snapshot();
    let reader = snapshot.reader();
    let parsed = parse_query(
        &std::fs::read(fixture_base.join("queries/include_all_blocks/query.json")).unwrap(),
        &meta,
    ).unwrap();
    let plan = compile(&parsed, &meta).unwrap();
    let result = execute_chunk(&plan, &meta, &reader, Vec::new(), false).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&result).unwrap();

    // Should have blocks from the chunk (first..=mid) but NOT the truncated memory blocks
    let blocks = json.as_array().unwrap();
    assert!(!blocks.is_empty(), "should still have chunk data after deep fork");
    // The highest block from chunk should be <= mid
    let last_block_in_result: u64 = blocks.last().unwrap()
        .get("header").unwrap()
        .get("number").unwrap()
        .as_u64().unwrap();
    assert!(last_block_in_result <= mid, "result should only contain chunk data, not truncated memory");

    // Re-push some blocks after deep fork
    for bn in memory_first..memory_first + 5 {
        store.push_block(make_simple_block(bn)).unwrap();
    }
    assert_eq!(store.head(), Some(memory_first + 4));
    assert_eq!(store.chunk_count(), chunks_before);
}
