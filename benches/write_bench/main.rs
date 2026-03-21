//! Write path benchmark: measure block push, spillover flush, and concurrent behavior.
//!
//! Tests:
//! 1. Single block push to memory (~μs expected)
//! 2. Spillover: flush N blocks from memory to sorted parquet
//! 3. Concurrent: push blocks while spillover runs in background
//! 4. Query during spillover: does compaction affect read latency?

use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::memory_backend::{BlockData, MemoryChunkReader};
use std::sync::Arc;
use sqd_query_engine::scan::parquet_writer::flush_to_parquet;
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest};
use arrow::array::*;
use arrow::compute::{self, SortColumn};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::LazyLock;
use std::time::Instant;
use tempfile::TempDir;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

static EVM_META: LazyLock<DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap());

static SOLANA_META: LazyLock<DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap());

/// Load parquet into per-block data
fn load_blocks(parquet_dir: &Path) -> Vec<Arc<BlockData>> {
    let parquet = ParquetChunkReader::open(parquet_dir).unwrap();
    let mut all_blocks: BTreeMap<u32, HashMap<String, RecordBatch>> = BTreeMap::new();

    for table_name in parquet.table_names() {
        let batches = parquet.read_all(&table_name).unwrap();
        let by_block = split_by_block(&batches, &table_name);
        for (bn, batch) in by_block {
            all_blocks.entry(bn).or_default().insert(table_name.clone(), batch);
        }
    }

    all_blocks
        .into_iter()
        .map(|(bn, tables)| Arc::new(BlockData {
            block_number: bn as u64,
            tables,
        }))
        .collect()
}

fn split_by_block(batches: &[RecordBatch], table_name: &str) -> Vec<(u32, RecordBatch)> {
    let bn_col = if table_name == "blocks" { "number" } else { "block_number" };
    let mut per_block: BTreeMap<u32, Vec<RecordBatch>> = BTreeMap::new();

    for batch in batches {
        if batch.num_rows() == 0 { continue; }
        let col = match batch.column_by_name(bn_col) {
            Some(c) => c,
            None => { per_block.entry(0).or_default().push(batch.clone()); continue; }
        };
        let block_numbers = extract_block_numbers(col.as_ref());
        if block_numbers.len() == 1 {
            per_block.entry(block_numbers[0]).or_default().push(batch.clone());
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
        .filter_map(|(bn, frags)| {
            if frags.len() == 1 {
                Some((bn, frags.into_iter().next().unwrap()))
            } else {
                let schema = frags[0].schema();
                compute::concat_batches(&schema, &frags).ok().map(|m| (bn, m))
            }
        })
        .collect()
}

fn extract_block_numbers(array: &dyn Array) -> Vec<u32> {
    let mut blocks = std::collections::BTreeSet::new();
    match array.data_type() {
        arrow::datatypes::DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..a.len() { if !a.is_null(i) { blocks.insert(a.value(i) as u32); } }
        }
        arrow::datatypes::DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            for i in 0..a.len() { if !a.is_null(i) { blocks.insert(a.value(i)); } }
        }
        _ => {}
    }
    blocks.into_iter().collect()
}

fn build_block_eq_mask(array: &dyn Array, target: u32) -> BooleanArray {
    match array.data_type() {
        arrow::datatypes::DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            BooleanArray::from_unary(a, |v| v == target as i32)
        }
        arrow::datatypes::DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            BooleanArray::from_unary(a, |v| v == target)
        }
        _ => BooleanArray::from(vec![true; array.len()]),
    }
}

fn block_memory_size(block: &BlockData) -> usize {
    block.tables.values().map(|b| {
        b.columns().iter().map(|c| c.get_array_memory_size()).sum::<usize>()
    }).sum()
}

fn run_query(query_json: &[u8], meta: &DatasetDescription, chunk: &dyn ChunkReader) -> Vec<u8> {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk(&plan, meta, chunk, Vec::new(), false).unwrap()
}

static EVM_USDC_QUERY: &[u8] = br#"{
    "type": "evm", "fromBlock": 0,
    "fields": { "block": { "number": true }, "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true } },
    "logs": [{ "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"], "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"] }]
}"#;

fn main() {
    let evm_dir = Path::new("data/evm/chunk");
    let sol_dir = Path::new("data/solana/chunk");

    if !evm_dir.exists() {
        eprintln!("No EVM data found");
        return;
    }

    eprintln!("Loading blocks...");
    let evm_blocks = load_blocks(evm_dir);
    let sol_blocks = if sol_dir.exists() { load_blocks(sol_dir) } else { Vec::new() };

    let evm_block_size: usize = evm_blocks.iter().map(|b| block_memory_size(b)).sum();
    let sol_block_size: usize = sol_blocks.iter().map(|b| block_memory_size(b)).sum();

    println!();
    println!("=== Block Stats ===");
    println!("EVM:    {} blocks, {:.1} MB total, {:.1} KB/block avg",
        evm_blocks.len(), evm_block_size as f64 / 1024.0 / 1024.0,
        evm_block_size as f64 / 1024.0 / evm_blocks.len() as f64);
    if !sol_blocks.is_empty() {
        println!("Solana: {} blocks, {:.1} MB total, {:.1} KB/block avg",
            sol_blocks.len(), sol_block_size as f64 / 1024.0 / 1024.0,
            sol_block_size as f64 / 1024.0 / sol_blocks.len() as f64);
    }

    // --- Test 1: Single block push latency ---
    println!();
    println!("=== Test 1: Single Block Push Latency ===");
    {
        let n = 1000;
        let mut times = Vec::with_capacity(n);
        for _ in 0..n {
            let mut reader = MemoryChunkReader::new();
            let block = &evm_blocks[0];
            let clone = BlockData {
                block_number: block.block_number,
                tables: block.tables.clone(),
            };
            let t = Instant::now();
            reader.push(clone);
            times.push(t.elapsed());
        }
        times.sort();
        println!("EVM block push (median of {}): {:.2?}", n, times[n / 2]);
        println!("  p99: {:.2?}, max: {:.2?}", times[n * 99 / 100], times[n - 1]);
    }

    // --- Test 2: Spillover flush (memory → sorted parquet) ---
    println!();
    println!("=== Test 2: Spillover Flush (Memory → Parquet) ===");
    for &count in &[10, 50, 100, 224] {
        if count > evm_blocks.len() { continue; }
        let tmp = TempDir::new().unwrap();
        let blocks: Vec<Arc<BlockData>> = evm_blocks[..count].iter().map(|b| Arc::new(BlockData {
            block_number: b.block_number,
            tables: b.tables.clone(),
        })).collect();
        let mem_size: usize = blocks.iter().map(|b| block_memory_size(b)).sum();

        let t = Instant::now();
        flush_to_parquet(&blocks, tmp.path(), &EVM_META).unwrap();
        let elapsed = t.elapsed();

        let disk_size: u64 = std::fs::read_dir(tmp.path()).unwrap()
            .flatten()
            .filter_map(|e| e.metadata().ok())
            .map(|m| m.len())
            .sum();

        println!("{:>3} blocks ({:.1} MB memory → {:.1} MB parquet): {:.2?}  ({:.0} blocks/sec)",
            count,
            mem_size as f64 / 1024.0 / 1024.0,
            disk_size as f64 / 1024.0 / 1024.0,
            elapsed,
            count as f64 / elapsed.as_secs_f64());
    }

    // --- Test 2b: Flush breakdown (sort vs compress) and alternatives ---
    println!();
    println!("=== Test 2b: Flush Variants (100 EVM blocks) ===");
    {
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        let count = 100.min(evm_blocks.len());
        let blocks: Vec<Arc<BlockData>> = evm_blocks[..count].iter().map(|b| Arc::new(BlockData {
            block_number: b.block_number,
            tables: b.tables.clone(),
        })).collect();

        // Collect + merge per table
        let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        for block in &blocks {
            for (name, batch) in &block.tables {
                if batch.num_rows() > 0 {
                    table_batches.entry(name.clone()).or_default().push(batch.clone());
                }
            }
        }
        let mut merged: HashMap<String, RecordBatch> = HashMap::new();
        for (name, batches) in &table_batches {
            let schema = batches[0].schema();
            merged.insert(name.clone(), compute::concat_batches(&schema, batches).unwrap());
        }

        // Measure sort only
        let t = Instant::now();
        let mut sorted: HashMap<String, RecordBatch> = HashMap::new();
        for (name, batch) in &merged {
            if let Some(desc) = EVM_META.table(name) {
                if !desc.sort_key.is_empty() {
                    let sort_cols: Vec<SortColumn> = desc.sort_key.iter()
                        .filter_map(|k| batch.column_by_name(k).map(|c| SortColumn { values: c.clone(), options: None }))
                        .collect();
                    if !sort_cols.is_empty() {
                        let indices = compute::lexsort_to_indices(&sort_cols, None).unwrap();
                        let cols: Vec<_> = batch.columns().iter()
                            .map(|c| compute::take(c.as_ref(), &indices, None).unwrap())
                            .collect();
                        sorted.insert(name.clone(), RecordBatch::try_new(batch.schema(), cols).unwrap());
                        continue;
                    }
                }
            }
            sorted.insert(name.clone(), batch.clone());
        }
        let sort_time = t.elapsed();

        // Measure ZSTD write only (sorted data)
        let tmp1 = TempDir::new().unwrap();
        let t = Instant::now();
        for (name, batch) in &sorted {
            let path = tmp1.path().join(format!("{}.parquet", name));
            let file = std::fs::File::create(&path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .set_max_row_group_size(8192)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                .build();
            let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
            w.write(batch).unwrap();
            w.close().unwrap();
        }
        let zstd_write_time = t.elapsed();
        let zstd_size: u64 = std::fs::read_dir(tmp1.path()).unwrap().flatten()
            .filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum();

        // Measure ZSTD level 1 write (sorted data)
        let tmp1b = TempDir::new().unwrap();
        let t = Instant::now();
        for (name, batch) in &sorted {
            let path = tmp1b.path().join(format!("{}.parquet", name));
            let file = std::fs::File::create(&path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::try_new(1).unwrap()))
                .set_max_row_group_size(8192)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                .build();
            let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
            w.write(batch).unwrap();
            w.close().unwrap();
        }
        let zstd1_write_time = t.elapsed();
        let zstd1_size: u64 = std::fs::read_dir(tmp1b.path()).unwrap().flatten()
            .filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum();

        // Measure LZ4 write (sorted data)
        let tmp2 = TempDir::new().unwrap();
        let t = Instant::now();
        for (name, batch) in &sorted {
            let path = tmp2.path().join(format!("{}.parquet", name));
            let file = std::fs::File::create(&path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::LZ4_RAW)
                .set_max_row_group_size(8192)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                .build();
            let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
            w.write(batch).unwrap();
            w.close().unwrap();
        }
        let lz4_write_time = t.elapsed();
        let lz4_size: u64 = std::fs::read_dir(tmp2.path()).unwrap().flatten()
            .filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum();

        // Measure uncompressed write (sorted data)
        let tmp3 = TempDir::new().unwrap();
        let t = Instant::now();
        for (name, batch) in &sorted {
            let path = tmp3.path().join(format!("{}.parquet", name));
            let file = std::fs::File::create(&path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_max_row_group_size(8192)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                .build();
            let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
            w.write(batch).unwrap();
            w.close().unwrap();
        }
        let uncomp_write_time = t.elapsed();
        let uncomp_size: u64 = std::fs::read_dir(tmp3.path()).unwrap().flatten()
            .filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum();

        // Measure no-sort + LZ4 (fastest possible)
        let tmp4 = TempDir::new().unwrap();
        let t = Instant::now();
        for (name, batch) in &merged {
            let path = tmp4.path().join(format!("{}.parquet", name));
            let file = std::fs::File::create(&path).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::LZ4_RAW)
                .set_max_row_group_size(8192)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
                .build();
            let mut w = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
            w.write(batch).unwrap();
            w.close().unwrap();
        }
        let nosort_lz4_time = t.elapsed();
        let nosort_lz4_size: u64 = std::fs::read_dir(tmp4.path()).unwrap().flatten()
            .filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum();

        println!("Sort only:                    {:.2?}", sort_time);
        println!("ZSTD default write (sorted):  {:.2?}  ({:.1} MB)", zstd_write_time, zstd_size as f64 / 1024.0 / 1024.0);
        println!("ZSTD level 1 write (sorted):  {:.2?}  ({:.1} MB)", zstd1_write_time, zstd1_size as f64 / 1024.0 / 1024.0);
        println!("LZ4 write (sorted):           {:.2?}  ({:.1} MB)", lz4_write_time, lz4_size as f64 / 1024.0 / 1024.0);
        println!("Uncompressed write (sorted):  {:.2?}  ({:.1} MB)", uncomp_write_time, uncomp_size as f64 / 1024.0 / 1024.0);
        println!("No-sort + LZ4 (fastest):      {:.2?}  ({:.1} MB)", nosort_lz4_time, nosort_lz4_size as f64 / 1024.0 / 1024.0);
        println!();
        let total_sorted_zstd = sort_time + zstd_write_time;
        let total_sorted_zstd1 = sort_time + zstd1_write_time;
        println!("Total sorted+ZSTD(default): {:.2?}  ({:.0} blocks/sec)", total_sorted_zstd, count as f64 / total_sorted_zstd.as_secs_f64());
        println!("Total sorted+ZSTD(1):      {:.2?}  ({:.0} blocks/sec)", total_sorted_zstd1, count as f64 / total_sorted_zstd1.as_secs_f64());
        println!("Total sorted+LZ4:          {:.2?}  ({:.0} blocks/sec)", sort_time + lz4_write_time, count as f64 / (sort_time + lz4_write_time).as_secs_f64());
        println!("Total no-sort+LZ4:         {:.2?}  ({:.0} blocks/sec)", nosort_lz4_time, count as f64 / nosort_lz4_time.as_secs_f64());
    }

    // --- Test 3: Concurrent push + flush ---
    println!();
    println!("=== Test 3: Push During Spillover ===");
    {
        let mut reader = MemoryChunkReader::new();
        // Fill with 100 blocks
        for b in &evm_blocks[..100.min(evm_blocks.len())] {
            reader.push(BlockData { block_number: b.block_number, tables: b.tables.clone() });
        }

        // Measure push latency WHILE spillover is running on another thread
        let blocks_for_flush: Vec<Arc<BlockData>> = evm_blocks[..100.min(evm_blocks.len())].iter().map(|b| {
            Arc::new(BlockData { block_number: b.block_number, tables: b.tables.clone() })
        }).collect();

        let n = 100;
        let mut push_times_during = Vec::with_capacity(n);
        let mut push_times_idle = Vec::with_capacity(n);

        // Measure idle push first
        for i in 0..n {
            let block = &evm_blocks[i % evm_blocks.len()];
            let clone = BlockData { block_number: 10000 + i as u64, tables: block.tables.clone() };
            let t = Instant::now();
            reader.push(clone);
            push_times_idle.push(t.elapsed());
        }

        // Now measure push during flush
        let tmp = TempDir::new().unwrap();
        let flush_path = tmp.path().to_path_buf();
        let meta = &*EVM_META;

        std::thread::scope(|s| {
            // Spawn flush thread
            let flush_handle = s.spawn(move || {
                flush_to_parquet(&blocks_for_flush, &flush_path, meta).unwrap();
            });

            // Push blocks while flush is running
            for i in 0..n {
                let block = &evm_blocks[i % evm_blocks.len()];
                let clone = BlockData { block_number: 20000 + i as u64, tables: block.tables.clone() };
                let t = Instant::now();
                reader.push(clone);
                push_times_during.push(t.elapsed());
            }

            flush_handle.join().unwrap();
        });

        push_times_idle.sort();
        push_times_during.sort();
        println!("Push latency idle:          median {:.2?}, p99 {:.2?}",
            push_times_idle[n / 2], push_times_idle[n * 99 / 100]);
        println!("Push latency during flush:  median {:.2?}, p99 {:.2?}",
            push_times_during[n / 2], push_times_during[n * 99 / 100]);
    }

    // --- Test 4: Query latency during spillover ---
    println!();
    println!("=== Test 4: Query During Spillover ===");
    {
        let mut reader = MemoryChunkReader::new();
        for b in &evm_blocks {
            reader.push(BlockData { block_number: b.block_number, tables: b.tables.clone() });
        }

        // Measure query latency idle
        let n = 20;
        let mut query_idle = Vec::with_capacity(n);
        for _ in 0..n {
            let t = Instant::now();
            let _ = run_query(EVM_USDC_QUERY, &EVM_META, &reader);
            query_idle.push(t.elapsed());
        }

        // Measure query latency during flush
        let blocks_for_flush: Vec<Arc<BlockData>> = evm_blocks.iter().map(|b| {
            Arc::new(BlockData { block_number: b.block_number, tables: b.tables.clone() })
        }).collect();
        let tmp = TempDir::new().unwrap();
        let flush_path = tmp.path().to_path_buf();
        let meta = &*EVM_META;

        let mut query_during = Vec::with_capacity(n);
        std::thread::scope(|s| {
            let flush_handle = s.spawn(move || {
                flush_to_parquet(&blocks_for_flush, &flush_path, meta).unwrap();
            });

            for _ in 0..n {
                let t = Instant::now();
                let _ = run_query(EVM_USDC_QUERY, &EVM_META, &reader);
                query_during.push(t.elapsed());
            }

            flush_handle.join().unwrap();
        });

        query_idle.sort();
        query_during.sort();
        println!("Query (evm/usdc) idle:          median {:.2?}", query_idle[n / 2]);
        println!("Query (evm/usdc) during flush:  median {:.2?}", query_during[n / 2]);
    }

    // --- Test 5: Max write throughput ---
    println!();
    println!("=== Test 5: Max Write Throughput ===");
    {
        let duration = std::time::Duration::from_secs(3);
        let mut reader = MemoryChunkReader::new();
        let start = Instant::now();
        let mut count = 0u64;
        while start.elapsed() < duration {
            let block = &evm_blocks[(count as usize) % evm_blocks.len()];
            reader.push(BlockData {
                block_number: count,
                tables: block.tables.clone(),
            });
            count += 1;
        }
        let elapsed = start.elapsed().as_secs_f64();
        let mb_per_sec = (count as f64 * evm_block_size as f64 / evm_blocks.len() as f64) / 1024.0 / 1024.0 / elapsed;
        println!("EVM: {:.0} blocks/sec ({:.0} MB/sec)", count as f64 / elapsed, mb_per_sec);
    }
}
