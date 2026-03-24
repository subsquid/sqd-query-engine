//! Storage backend benchmark: Legacy engine (baseline) vs New engine with various backends
//!
//! Baseline: Legacy engine + sqd_storage (RocksDB)
//! Competitors: New engine with Parquet, LMDB, RocksDB, Legacy storage backends

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

// --- New engine ---

fn run_new_engine(query_json: &[u8], meta: &DatasetDescription, chunk: &dyn ChunkReader) -> Vec<u8> {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk(&plan, meta, chunk, Vec::new(), false).unwrap()
}

// --- Legacy engine ---

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

struct BenchCase {
    name: &'static str,
    query_json: &'static [u8],
    meta: &'static DatasetDescription,
}

// --- Measurement ---

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

fn measure_latency_legacy_storage(
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

fn measure_latency_legacy_parquet(
    case: &BenchCase,
    chunk: &ParquetChunk,
    n: usize,
) -> Duration {
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

fn measure_throughput_legacy_storage(
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

fn measure_throughput_legacy_parquet(
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

/// Named backend: either new engine + ChunkReader, or legacy engine + storage
enum BackendKind<'a> {
    NewEngine {
        evm: Option<Box<dyn ChunkReader + 'a>>,
        sol: Option<Box<dyn ChunkReader + 'a>>,
    },
    LegacyStorage {
        evm: Option<LegacyStorage>,
        sol: Option<LegacyStorage>,
    },
    LegacyParquet {
        evm: Option<ParquetChunk>,
        sol: Option<ParquetChunk>,
    },
}

struct Backend<'a> {
    name: &'static str,
    kind: BackendKind<'a>,
}

impl<'a> Backend<'a> {
    fn measure_latency(&self, case: &BenchCase, is_evm: bool, n: usize) -> Option<Duration> {
        match &self.kind {
            BackendKind::NewEngine { evm, sol } => {
                let chunk = if is_evm { evm } else { sol };
                chunk.as_ref().map(|c| measure_latency_new(case, c.as_ref(), n))
            }
            BackendKind::LegacyStorage { evm, sol } => {
                let storage = if is_evm { evm } else { sol };
                storage.as_ref().map(|s| measure_latency_legacy_storage(case, s, n))
            }
            BackendKind::LegacyParquet { evm, sol } => {
                let chunk = if is_evm { evm } else { sol };
                chunk.as_ref().map(|c| measure_latency_legacy_parquet(case, c, n))
            }
        }
    }

    fn measure_throughput(
        &self,
        case: &BenchCase,
        is_evm: bool,
        concurrency: usize,
        duration: Duration,
    ) -> Option<f64> {
        match &self.kind {
            BackendKind::NewEngine { evm, sol } => {
                let chunk = if is_evm { evm } else { sol };
                chunk.as_ref().map(|c| measure_throughput_new(case, c.as_ref(), concurrency, duration))
            }
            BackendKind::LegacyStorage { evm, sol } => {
                let storage = if is_evm { evm } else { sol };
                storage.as_ref().map(|s| {
                    measure_throughput_legacy_storage(case, s, concurrency, duration)
                })
            }
            BackendKind::LegacyParquet { evm, sol } => {
                let chunk = if is_evm { evm } else { sol };
                chunk.as_ref().map(|c| {
                    measure_throughput_legacy_parquet(case, c, concurrency, duration)
                })
            }
        }
    }

    fn run_query(&self, case: &BenchCase, is_evm: bool) -> Option<Vec<u8>> {
        match &self.kind {
            BackendKind::NewEngine { evm, sol } => {
                let chunk = if is_evm { evm } else { sol };
                chunk.as_ref().map(|c| run_new_engine(case.query_json, case.meta, c.as_ref()))
            }
            BackendKind::LegacyStorage { evm, sol } => {
                let storage = if is_evm { evm } else { sol };
                storage.as_ref().map(|s| run_legacy_on_storage(case.query_json, s))
            }
            BackendKind::LegacyParquet { evm, sol } => {
                let chunk = if is_evm { evm } else { sol };
                chunk.as_ref().map(|c| run_legacy_on_parquet(case.query_json, c))
            }
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

    // --- Legacy engine backends (baseline) ---

    // Legacy engine + Parquet
    let legacy_pq_evm: Option<ParquetChunk> =
        has_evm.then(|| ParquetChunk::new(evm_chunk_dir.to_str().unwrap()));
    let legacy_pq_sol: Option<ParquetChunk> =
        has_sol.then(|| ParquetChunk::new(sol_chunk_dir.to_str().unwrap()));

    // Legacy engine + sqd_storage (RocksDB)
    let leg_st_evm_dir = tmp.path().join("leg_st_evm");
    let leg_st_sol_dir = tmp.path().join("leg_st_sol");

    let mut leg_storage_evm: Option<LegacyStorage> = None;
    let mut leg_storage_sol: Option<LegacyStorage> = None;

    if has_evm {
        let t = Instant::now();
        leg_storage_evm = Some(load_parquet_to_legacy_storage(evm_chunk_dir, &leg_st_evm_dir).unwrap());
        eprintln!("  LegacyEng+sqd_storage EVM: {} (load: {:.2?})", format_size(dir_size(&leg_st_evm_dir)), t.elapsed());
    }
    if has_sol {
        let t = Instant::now();
        leg_storage_sol = Some(load_parquet_to_legacy_storage(sol_chunk_dir, &leg_st_sol_dir).unwrap());
        eprintln!("  LegacyEng+sqd_storage SOL: {} (load: {:.2?})", format_size(dir_size(&leg_st_sol_dir)), t.elapsed());
    }

    // --- New engine backends ---

    // New engine + Parquet
    let parquet_evm = has_evm.then(|| ParquetChunkReader::open(evm_chunk_dir).unwrap());
    let parquet_sol = has_sol.then(|| ParquetChunkReader::open(sol_chunk_dir).unwrap());

    // New engine + LMDB per-block
    let lmdb_pb_evm_dir = tmp.path().join("lmdb_pb_evm");
    let lmdb_pb_sol_dir = tmp.path().join("lmdb_pb_sol");
    std::fs::create_dir_all(&lmdb_pb_evm_dir).unwrap();
    std::fs::create_dir_all(&lmdb_pb_sol_dir).unwrap();

    // New engine + LMDB bulk
    let lmdb_bk_evm_dir = tmp.path().join("lmdb_bk_evm");
    let lmdb_bk_sol_dir = tmp.path().join("lmdb_bk_sol");
    std::fs::create_dir_all(&lmdb_bk_evm_dir).unwrap();
    std::fs::create_dir_all(&lmdb_bk_sol_dir).unwrap();

    // New engine + LMDB bulk+zstd
    let lmdb_zs_evm_dir = tmp.path().join("lmdb_zs_evm");
    let lmdb_zs_sol_dir = tmp.path().join("lmdb_zs_sol");
    std::fs::create_dir_all(&lmdb_zs_evm_dir).unwrap();
    std::fs::create_dir_all(&lmdb_zs_sol_dir).unwrap();

    // New engine + RocksDB bulk
    let rocks_bk_evm_dir = tmp.path().join("rocks_bk_evm");
    let rocks_bk_sol_dir = tmp.path().join("rocks_bk_sol");

    // New engine + RocksDB bulk + LZ4
    let rocks_lz4_evm_dir = tmp.path().join("rocks_lz4_evm");
    let rocks_lz4_sol_dir = tmp.path().join("rocks_lz4_sol");

    // New engine + Legacy sqd_storage
    let new_legacy_evm_dir = tmp.path().join("new_legacy_evm");
    let new_legacy_sol_dir = tmp.path().join("new_legacy_sol");

    let mut lmdb_pb_evm = None;
    let mut lmdb_pb_sol = None;
    let mut lmdb_bk_evm = None;
    let mut lmdb_bk_sol = None;
    let mut lmdb_zs_evm = None;
    let mut lmdb_zs_sol = None;
    let mut rocks_bk_evm = None;
    let mut rocks_bk_sol = None;
    let mut rocks_lz4_evm = None;
    let mut rocks_lz4_sol = None;
    let mut new_legacy_evm = None;
    let mut new_legacy_sol = None;

    if has_evm {
        let t = Instant::now();
        lmdb_pb_evm = Some(load_parquet_to_lmdb(evm_chunk_dir, &lmdb_pb_evm_dir).unwrap());
        eprintln!("  LMDB per-block EVM:        {} (load: {:.2?})", format_size(dir_size(&lmdb_pb_evm_dir)), t.elapsed());

        let t = Instant::now();
        lmdb_bk_evm = Some(load_parquet_to_lmdb_bulk(evm_chunk_dir, &lmdb_bk_evm_dir).unwrap());
        eprintln!("  LMDB bulk EVM:             {} (load: {:.2?})", format_size(dir_size(&lmdb_bk_evm_dir)), t.elapsed());

        let t = Instant::now();
        lmdb_zs_evm = Some(load_parquet_to_lmdb_bulk_zstd(evm_chunk_dir, &lmdb_zs_evm_dir).unwrap());
        eprintln!("  LMDB bulk+zstd EVM:        {} (load: {:.2?})", format_size(dir_size(&lmdb_zs_evm_dir)), t.elapsed());

        let t = Instant::now();
        rocks_bk_evm = Some(load_parquet_to_rocksdb_bulk(evm_chunk_dir, &rocks_bk_evm_dir).unwrap());
        eprintln!("  RocksDB bulk EVM:          {} (load: {:.2?})", format_size(dir_size(&rocks_bk_evm_dir)), t.elapsed());

        let t = Instant::now();
        rocks_lz4_evm = Some(load_parquet_to_rocksdb_bulk_lz4(evm_chunk_dir, &rocks_lz4_evm_dir).unwrap());
        eprintln!("  RocksDB lz4 EVM:           {} (load: {:.2?})", format_size(dir_size(&rocks_lz4_evm_dir)), t.elapsed());

        let t = Instant::now();
        new_legacy_evm = Some(load_parquet_to_legacy(evm_chunk_dir, &new_legacy_evm_dir).unwrap());
        eprintln!("  NewEng+sqd_storage EVM:    {} (load: {:.2?})", format_size(dir_size(&new_legacy_evm_dir)), t.elapsed());
    }

    if has_sol {
        let t = Instant::now();
        lmdb_pb_sol = Some(load_parquet_to_lmdb(sol_chunk_dir, &lmdb_pb_sol_dir).unwrap());
        eprintln!("  LMDB per-block SOL:        {} (load: {:.2?})", format_size(dir_size(&lmdb_pb_sol_dir)), t.elapsed());

        let t = Instant::now();
        lmdb_bk_sol = Some(load_parquet_to_lmdb_bulk(sol_chunk_dir, &lmdb_bk_sol_dir).unwrap());
        eprintln!("  LMDB bulk SOL:             {} (load: {:.2?})", format_size(dir_size(&lmdb_bk_sol_dir)), t.elapsed());

        let t = Instant::now();
        lmdb_zs_sol = Some(load_parquet_to_lmdb_bulk_zstd(sol_chunk_dir, &lmdb_zs_sol_dir).unwrap());
        eprintln!("  LMDB bulk+zstd SOL:        {} (load: {:.2?})", format_size(dir_size(&lmdb_zs_sol_dir)), t.elapsed());

        let t = Instant::now();
        rocks_bk_sol = Some(load_parquet_to_rocksdb_bulk(sol_chunk_dir, &rocks_bk_sol_dir).unwrap());
        eprintln!("  RocksDB bulk SOL:          {} (load: {:.2?})", format_size(dir_size(&rocks_bk_sol_dir)), t.elapsed());

        let t = Instant::now();
        rocks_lz4_sol = Some(load_parquet_to_rocksdb_bulk_lz4(sol_chunk_dir, &rocks_lz4_sol_dir).unwrap());
        eprintln!("  RocksDB lz4 SOL:           {} (load: {:.2?})", format_size(dir_size(&rocks_lz4_sol_dir)), t.elapsed());

        let t = Instant::now();
        new_legacy_sol = Some(load_parquet_to_legacy(sol_chunk_dir, &new_legacy_sol_dir).unwrap());
        eprintln!("  NewEng+sqd_storage SOL:    {} (load: {:.2?})", format_size(dir_size(&new_legacy_sol_dir)), t.elapsed());
    }

    // Build backends list: Legacy baselines first, then new engine backends
    let backends: Vec<Backend> = vec![
        // --- Legacy engine baselines ---
        Backend {
            name: "Leg+Parquet",
            kind: BackendKind::LegacyParquet {
                evm: legacy_pq_evm,
                sol: legacy_pq_sol,
            },
        },
        Backend {
            name: "Leg+RocksDB",
            kind: BackendKind::LegacyStorage {
                evm: leg_storage_evm,
                sol: leg_storage_sol,
            },
        },
        // --- New engine ---
        Backend {
            name: "New+Parquet",
            kind: BackendKind::NewEngine {
                evm: parquet_evm.map(|c| Box::new(c) as Box<dyn ChunkReader>),
                sol: parquet_sol.map(|c| Box::new(c) as Box<dyn ChunkReader>),
            },
        },
        Backend {
            name: "New+LMDB/blk",
            kind: BackendKind::NewEngine {
                evm: lmdb_pb_evm.map(|c| Box::new(c) as Box<dyn ChunkReader>),
                sol: lmdb_pb_sol.map(|c| Box::new(c) as Box<dyn ChunkReader>),
            },
        },
        Backend {
            name: "New+LMDB/bk",
            kind: BackendKind::NewEngine {
                evm: lmdb_bk_evm.map(|c| Box::new(c) as Box<dyn ChunkReader>),
                sol: lmdb_bk_sol.map(|c| Box::new(c) as Box<dyn ChunkReader>),
            },
        },
        Backend {
            name: "New+LMDB/zs",
            kind: BackendKind::NewEngine {
                evm: lmdb_zs_evm.map(|c| Box::new(c) as Box<dyn ChunkReader>),
                sol: lmdb_zs_sol.map(|c| Box::new(c) as Box<dyn ChunkReader>),
            },
        },
        Backend {
            name: "New+Rock/bk",
            kind: BackendKind::NewEngine {
                evm: rocks_bk_evm.map(|c| Box::new(c) as Box<dyn ChunkReader>),
                sol: rocks_bk_sol.map(|c| Box::new(c) as Box<dyn ChunkReader>),
            },
        },
        Backend {
            name: "New+Rock/lz4",
            kind: BackendKind::NewEngine {
                evm: rocks_lz4_evm.map(|c| Box::new(c) as Box<dyn ChunkReader>),
                sol: rocks_lz4_sol.map(|c| Box::new(c) as Box<dyn ChunkReader>),
            },
        },
        Backend {
            name: "New+Legacy",
            kind: BackendKind::NewEngine {
                evm: new_legacy_evm.map(|c| Box::new(c) as Box<dyn ChunkReader>),
                sol: new_legacy_sol.map(|c| Box::new(c) as Box<dyn ChunkReader>),
            },
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

    // Verify correctness (use Leg+Parquet as reference since it's the legacy engine)
    eprintln!("\n=== Verifying correctness ===");
    for (case, is_evm) in &all_cases {
        let ref_out = backends[0].run_query(case, *is_evm).unwrap_or_default();
        let mut ok = true;
        for backend in &backends[1..] {
            let out = backend.run_query(case, *is_evm).unwrap_or_default();
            if out != ref_out {
                eprintln!("  MISMATCH {}: {} vs {} differ! ({} vs {} bytes)",
                    case.name, backend.name, backends[0].name, out.len(), ref_out.len());
                ok = false;
            }
        }
        if ok {
            eprintln!("  OK {}: {} bytes", case.name, ref_out.len());
        }
    }

    // Profile New+Legacy scan breakdown
    eprintln!("\n=== Scan profiling (New+Legacy) ===");
    sqd_query_engine::scan::legacy_backend::PROFILE_SCANS.store(true, std::sync::atomic::Ordering::Relaxed);
    for (case, is_evm) in &all_cases {
        eprintln!("--- {} ---", case.name);
        // Find New+Legacy backend
        if let Some(backend) = backends.iter().find(|b| b.name == "New+Legacy") {
            let _ = backend.run_query(case, *is_evm);
        }
    }
    sqd_query_engine::scan::legacy_backend::PROFILE_SCANS.store(false, std::sync::atomic::Ordering::Relaxed);

    // Latency benchmark
    let n = 20;
    let backend_names: Vec<&str> = backends.iter().map(|b| b.name).collect();
    let col_w = 14;
    let name_w = 26;

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
            let result = backend.measure_latency(case, *is_evm, n);
            print!(
                "{:>col_w$}",
                result.map(|d| format!("{:.2?}", d)).unwrap_or_else(|| "-".to_string())
            );
        }
        println!();
    }
    eprint!("\r{}\r", " ".repeat(80));

    // Throughput benchmark at multiple concurrency levels
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut cpu_levels = vec![1, 4];
    if cpus >= 8 { cpu_levels.push(8); }
    if cpus >= 12 { cpu_levels.push(12); }
    if !cpu_levels.contains(&cpus) { cpu_levels.push(cpus); }

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
                let rps = backend.measure_throughput(case, *is_evm, cpu, duration);
                print!(
                    "{:>col_w$}",
                    rps.map(|r| format!("{r:.1}")).unwrap_or_else(|| "-".to_string())
                );
            }
            println!();
        }
        eprint!("\r{}\r", " ".repeat(80));
    }

    // Disk size summary
    println!();
    println!("=== Disk Size ===");
    print!("{:<name_w$}", "Dataset");
    for name in &backend_names {
        print!("{:>col_w$}", name);
    }
    println!();
    println!("{}", "-".repeat(name_w + backend_names.len() * col_w));

    if has_evm {
        print!("{:<name_w$}", "EVM");
        let dirs = [
            evm_chunk_dir.to_path_buf(),    // Leg+Parquet
            leg_st_evm_dir.clone(),          // Leg+RocksDB
            evm_chunk_dir.to_path_buf(),     // New+Parquet
            lmdb_pb_evm_dir.clone(),         // New+LMDB/blk
            lmdb_bk_evm_dir.clone(),         // New+LMDB/bk
            lmdb_zs_evm_dir.clone(),         // New+LMDB/zs
            rocks_bk_evm_dir.clone(),        // New+Rock/bk
            rocks_lz4_evm_dir.clone(),       // New+Rock/lz4
            new_legacy_evm_dir.clone(),      // New+Legacy
        ];
        for dir in &dirs {
            print!("{:>col_w$}", format_size(dir_size(dir)));
        }
        println!();
    }
    if has_sol {
        print!("{:<name_w$}", "Solana");
        let dirs = [
            sol_chunk_dir.to_path_buf(),     // Leg+Parquet
            leg_st_sol_dir.clone(),          // Leg+RocksDB
            sol_chunk_dir.to_path_buf(),     // New+Parquet
            lmdb_pb_sol_dir.clone(),         // New+LMDB/blk
            lmdb_bk_sol_dir.clone(),         // New+LMDB/bk
            lmdb_zs_sol_dir.clone(),         // New+LMDB/zs
            rocks_bk_sol_dir.clone(),        // New+Rock/bk
            rocks_lz4_sol_dir.clone(),       // New+Rock/lz4
            new_legacy_sol_dir.clone(),      // New+Legacy
        ];
        for dir in &dirs {
            print!("{:>col_w$}", format_size(dir_size(dir)));
        }
        println!();
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

fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}
