//! Hot storage benchmark: compare query performance across storage tiers.
//!
//! 1. New cold vs New hot:  Parquet vs Memory vs Spillover(parquet from memory)
//! 2. Old hot vs New hot:   Legacy+RocksDB vs Memory vs Spillover
//!
//! All use the same queries and data.

#[path = "../queries.rs"]
mod queries;

mod loader;

use loader::*;
use queries::*;
use sqd_query::{JsonLinesWriter, ParquetChunk, Query};
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

static SOLANA_META: LazyLock<DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap());

static EVM_META: LazyLock<DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap());

// --- Runners ---

fn run_new_engine(query_json: &[u8], meta: &DatasetDescription, chunk: &dyn ChunkReader) -> Vec<u8> {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk(&plan, meta, chunk, Vec::new(), false).unwrap()
}

fn run_legacy_on_storage(query_json: &[u8], storage: &LegacyStorage) -> Vec<u8> {
    let query = Query::from_json_bytes(query_json).unwrap();
    let plan = query.compile();
    let snapshot = storage.db.snapshot();
    let chunk_reader = snapshot.create_chunk_reader(storage.chunk.clone());
    let mut json_writer = JsonLinesWriter::new(Vec::new());
    if let Some(mut blocks) = plan.execute(&chunk_reader).unwrap() {
        json_writer.write_blocks(&mut blocks).unwrap();
    }
    json_writer.finish().unwrap()
}

fn run_legacy_on_parquet(query_json: &[u8], chunk: &ParquetChunk) -> Vec<u8> {
    let query = Query::from_json_bytes(query_json).unwrap();
    let plan = query.compile();
    let mut json_writer = JsonLinesWriter::new(Vec::new());
    if let Some(mut blocks) = plan.execute(chunk).unwrap() {
        json_writer.write_blocks(&mut blocks).unwrap();
    }
    json_writer.finish().unwrap()
}

// --- Measurement ---

struct BenchCase {
    name: &'static str,
    query_json: &'static [u8],
    meta: &'static DatasetDescription,
}

fn measure_latency_new(
    case: &BenchCase,
    chunk: &dyn ChunkReader,
    n: usize,
) -> Duration {
    let _ = run_new_engine(case.query_json, case.meta, chunk);
    let mut times = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let _ = run_new_engine(case.query_json, case.meta, chunk);
        times.push(t.elapsed());
    }
    times.sort();
    times[n / 2]
}

fn measure_latency_legacy(
    case: &BenchCase,
    storage: &LegacyStorage,
    n: usize,
) -> Duration {
    let _ = run_legacy_on_storage(case.query_json, storage);
    let mut times = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let _ = run_legacy_on_storage(case.query_json, storage);
        times.push(t.elapsed());
    }
    times.sort();
    times[n / 2]
}

fn measure_throughput_new(
    case: &BenchCase,
    chunk: &dyn ChunkReader,
    concurrency: usize,
    duration: Duration,
) -> f64 {
    let stop = AtomicBool::new(false);
    let total = AtomicUsize::new(0);
    let start = Instant::now();

    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    let _ = run_new_engine(case.query_json, case.meta, chunk);
                    total.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
    });

    total.load(Ordering::Relaxed) as f64 / start.elapsed().as_secs_f64()
}

fn measure_throughput_legacy(
    case: &BenchCase,
    storage: &LegacyStorage,
    concurrency: usize,
    duration: Duration,
) -> f64 {
    let stop = AtomicBool::new(false);
    let total = AtomicUsize::new(0);
    let start = Instant::now();

    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    let _ = run_legacy_on_storage(case.query_json, storage);
                    total.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
    });

    total.load(Ordering::Relaxed) as f64 / start.elapsed().as_secs_f64()
}

fn measure_latency_legacy_pq(case: &BenchCase, chunk: &ParquetChunk, n: usize) -> Duration {
    let _ = run_legacy_on_parquet(case.query_json, chunk);
    let mut times = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let _ = run_legacy_on_parquet(case.query_json, chunk);
        times.push(t.elapsed());
    }
    times.sort();
    times[n / 2]
}

fn measure_throughput_legacy_pq(
    case: &BenchCase,
    chunk: &ParquetChunk,
    concurrency: usize,
    duration: Duration,
) -> f64 {
    let stop = AtomicBool::new(false);
    let total = AtomicUsize::new(0);
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    let _ = run_legacy_on_parquet(case.query_json, chunk);
                    total.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
    });
    total.load(Ordering::Relaxed) as f64 / start.elapsed().as_secs_f64()
}

// --- Backend abstraction ---

enum Backend<'a> {
    NewEngine(&'a dyn ChunkReader),
    LegacyEngine(&'a LegacyStorage),
    LegacyParquet(&'a ParquetChunk),
}

impl<'a> Backend<'a> {
    fn measure_latency(&self, case: &BenchCase, n: usize) -> Duration {
        match self {
            Backend::NewEngine(c) => measure_latency_new(case, *c, n),
            Backend::LegacyEngine(s) => measure_latency_legacy(case, s, n),
            Backend::LegacyParquet(c) => measure_latency_legacy_pq(case, c, n),
        }
    }

    fn measure_throughput(&self, case: &BenchCase, concurrency: usize, duration: Duration) -> f64 {
        match self {
            Backend::NewEngine(c) => measure_throughput_new(case, *c, concurrency, duration),
            Backend::LegacyEngine(s) => measure_throughput_legacy(case, s, concurrency, duration),
            Backend::LegacyParquet(c) => measure_throughput_legacy_pq(case, c, concurrency, duration),
        }
    }

    fn run_query(&self, case: &BenchCase) -> Vec<u8> {
        match self {
            Backend::NewEngine(c) => run_new_engine(case.query_json, case.meta, *c),
            Backend::LegacyEngine(s) => run_legacy_on_storage(case.query_json, s),
            Backend::LegacyParquet(c) => run_legacy_on_parquet(case.query_json, c),
        }
    }
}

fn main() {
    let evm_chunk_dir = Path::new("data/evm/chunk");
    let sol_chunk_dir = Path::new("data/solana/chunk");

    let has_evm = evm_chunk_dir.exists();
    let has_sol = sol_chunk_dir.exists();

    if !has_evm && !has_sol {
        eprintln!("No fixture data found in data/{{evm,solana}}/chunk/");
        return;
    }

    let tmp = TempDir::new().unwrap();

    eprintln!("=== Loading data into backends ===");

    // 1. New+Parquet (cold storage baseline)
    let parquet_evm = has_evm.then(|| ParquetChunkReader::open(evm_chunk_dir).unwrap());
    let parquet_sol = has_sol.then(|| ParquetChunkReader::open(sol_chunk_dir).unwrap());

    // 2. New+Memory (hot: all blocks in memory)
    let memory_evm = has_evm.then(|| {
        let t = Instant::now();
        let reader = load_parquet_to_memory(evm_chunk_dir);
        eprintln!("  Memory EVM:      {:.1} MB arrow (load: {:.2?})", reader.memory_usage() as f64 / 1024.0 / 1024.0, t.elapsed());
        reader
    });
    let memory_sol = has_sol.then(|| {
        let t = Instant::now();
        let reader = load_parquet_to_memory(sol_chunk_dir);
        eprintln!("  Memory SOL:      {:.1} MB arrow (load: {:.2?})", reader.memory_usage() as f64 / 1024.0 / 1024.0, t.elapsed());
        reader
    });

    // 3. New+Spillover (hot: blocks flushed from memory to sorted parquet)
    let spillover_evm_dir = tmp.path().join("spill_evm");
    let spillover_sol_dir = tmp.path().join("spill_sol");
    let spillover_evm = has_evm.then(|| {
        let t = Instant::now();
        let reader = load_parquet_to_spillover(evm_chunk_dir, &spillover_evm_dir, &EVM_META);
        eprintln!("  Spillover EVM:   {:.1} MB parquet (load: {:.2?})", dir_size(&spillover_evm_dir) as f64 / 1024.0 / 1024.0, t.elapsed());
        reader
    });
    let spillover_sol = has_sol.then(|| {
        let t = Instant::now();
        let reader = load_parquet_to_spillover(sol_chunk_dir, &spillover_sol_dir, &SOLANA_META);
        eprintln!("  Spillover SOL:   {:.1} MB parquet (load: {:.2?})", dir_size(&spillover_sol_dir) as f64 / 1024.0 / 1024.0, t.elapsed());
        reader
    });

    // 4. Legacy+sqd_storage (old hot baseline)
    let legacy_evm_dir = tmp.path().join("legacy_evm");
    let legacy_sol_dir = tmp.path().join("legacy_sol");
    let mut legacy_evm: Option<LegacyStorage> = None;
    let mut legacy_sol: Option<LegacyStorage> = None;

    if has_evm {
        let t = Instant::now();
        legacy_evm = Some(load_parquet_to_legacy_storage(evm_chunk_dir, &legacy_evm_dir).unwrap());
        eprintln!("  Legacy EVM:      {:.1} MB rocksdb (load: {:.2?})", dir_size(&legacy_evm_dir) as f64 / 1024.0 / 1024.0, t.elapsed());
    }
    if has_sol {
        let t = Instant::now();
        legacy_sol = Some(load_parquet_to_legacy_storage(sol_chunk_dir, &legacy_sol_dir).unwrap());
        eprintln!("  Legacy SOL:      {:.1} MB rocksdb (load: {:.2?})", dir_size(&legacy_sol_dir) as f64 / 1024.0 / 1024.0, t.elapsed());
    }

    // Legacy+Parquet (old cold)
    let legacy_pq_evm: Option<ParquetChunk> =
        has_evm.then(|| ParquetChunk::new(evm_chunk_dir.to_str().unwrap()));
    let legacy_pq_sol: Option<ParquetChunk> =
        has_sol.then(|| ParquetChunk::new(sol_chunk_dir.to_str().unwrap()));

    // Build named backends
    struct NamedBackend<'a> {
        name: &'static str,
        evm: Option<Backend<'a>>,
        sol: Option<Backend<'a>>,
    }

    let backends: Vec<NamedBackend> = vec![
        NamedBackend {
            name: "Leg+Parquet",
            evm: legacy_pq_evm.as_ref().map(|c| Backend::LegacyParquet(c)),
            sol: legacy_pq_sol.as_ref().map(|c| Backend::LegacyParquet(c)),
        },
        NamedBackend {
            name: "Leg+RocksDB",
            evm: legacy_evm.as_ref().map(|s| Backend::LegacyEngine(s)),
            sol: legacy_sol.as_ref().map(|s| Backend::LegacyEngine(s)),
        },
        NamedBackend {
            name: "New+Parquet",
            evm: parquet_evm.as_ref().map(|c| Backend::NewEngine(c as &dyn ChunkReader)),
            sol: parquet_sol.as_ref().map(|c| Backend::NewEngine(c as &dyn ChunkReader)),
        },
        NamedBackend {
            name: "New+Memory",
            evm: memory_evm.as_ref().map(|c| Backend::NewEngine(c as &dyn ChunkReader)),
            sol: memory_sol.as_ref().map(|c| Backend::NewEngine(c as &dyn ChunkReader)),
        },
        NamedBackend {
            name: "New+Spillovr",
            evm: spillover_evm.as_ref().map(|c| Backend::NewEngine(c as &dyn ChunkReader)),
            sol: spillover_sol.as_ref().map(|c| Backend::NewEngine(c as &dyn ChunkReader)),
        },
    ];

    // Build query cases
    let mut evm_cases: Vec<BenchCase> = Vec::new();
    let mut sol_cases: Vec<BenchCase> = Vec::new();
    if has_evm {
        for (name, json) in EVM_QUERIES {
            evm_cases.push(BenchCase { name, query_json: json, meta: &EVM_META });
        }
    }
    if has_sol {
        for (name, json) in SOL_QUERIES {
            sol_cases.push(BenchCase { name, query_json: json, meta: &SOLANA_META });
        }
    }

    let all_cases: Vec<(&BenchCase, bool)> = evm_cases.iter().map(|c| (c, true))
        .chain(sol_cases.iter().map(|c| (c, false)))
        .collect();

    // Verify correctness
    eprintln!("\n=== Verifying correctness ===");
    for (case, is_evm) in &all_cases {
        // Use parquet as reference
        let ref_backend = if *is_evm { &backends[1].evm } else { &backends[1].sol };
        let ref_out = ref_backend.as_ref().map(|b| b.run_query(case)).unwrap_or_default();

        let mut ok = true;
        for backend in &backends {
            if backend.name == "New+Parquet" { continue; }
            let b = if *is_evm { &backend.evm } else { &backend.sol };
            let out = b.as_ref().map(|b| b.run_query(case)).unwrap_or_default();
            if out != ref_out {
                eprintln!("  MISMATCH {}: {} ({} bytes) vs Parquet ({} bytes)", case.name, backend.name, out.len(), ref_out.len());
                ok = false;
            }
        }
        if ok {
            eprintln!("  OK {}: {} bytes", case.name, ref_out.len());
        }
    }

    let backend_names: Vec<&str> = backends.iter().map(|b| b.name).collect();
    let col_w = 14;
    let name_w = 26;

    // Latency
    let n = 20;
    eprintln!("\n=== Latency ({n} runs, median) ===");
    println!();
    println!("=== Latency (median of {n} runs) ===");
    print!("{:<name_w$}", "Query");
    for name in &backend_names {
        print!("{:>col_w$}", name);
    }
    println!();
    println!("{}", "-".repeat(name_w + backend_names.len() * col_w));

    for (case, is_evm) in &all_cases {
        eprint!("\r  running: {:<name_w$}", case.name);
        print!("{:<name_w$}", case.name);
        for backend in &backends {
            let b = if *is_evm { &backend.evm } else { &backend.sol };
            let result = b.as_ref().map(|b| b.measure_latency(case, n));
            print!(
                "{:>col_w$}",
                result.map(|d| format!("{:.2?}", d)).unwrap_or_else(|| "-".to_string())
            );
        }
        println!();
    }
    eprint!("\r{}\r", " ".repeat(80));

    // Throughput
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut cpu_levels = vec![1, 4, 8];
    if cpus >= 16 { cpu_levels.push(16); }
    if cpus >= 24 { cpu_levels.push(24); }
    if cpus >= 30 { cpu_levels.push(30); }
    if !cpu_levels.contains(&cpus) { cpu_levels.push(cpus); }
    cpu_levels.sort();
    cpu_levels.dedup();

    let duration = Duration::from_secs(3);

    for &cpu in &cpu_levels {
        eprintln!("=== Throughput (rps, CPU={cpu}, 3s) ===");
        println!();
        println!("=== Throughput (rps, CPU={cpu}, 3s) ===");
        print!("{:<name_w$}", "Query");
        for name in &backend_names {
            print!("{:>col_w$}", name);
        }
        println!();
        println!("{}", "-".repeat(name_w + backend_names.len() * col_w));

        for (case, is_evm) in &all_cases {
            eprint!("\r  running: {:<name_w$}", case.name);
            print!("{:<name_w$}", case.name);
            for backend in &backends {
                let b = if *is_evm { &backend.evm } else { &backend.sol };
                let rps = b.as_ref().map(|b| b.measure_throughput(case, cpu, duration));
                print!(
                    "{:>col_w$}",
                    rps.map(|r| format!("{r:.1}")).unwrap_or_else(|| "-".to_string())
                );
            }
            println!();
        }
        eprint!("\r{}\r", " ".repeat(80));
    }
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = entry.metadata().unwrap();
            if meta.is_file() {
                total += meta.len();
            } else if meta.is_dir() {
                total += dir_size(&entry.path());
            }
        }
    }
    total
}
