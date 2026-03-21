//! Legacy engine benchmark: compare Parquet vs sqd_storage (RocksDB) throughput
//! using the legacy sqd-query engine.

#[path = "../queries.rs"]
mod queries;

mod loader;

use loader::*;
use queries::*;
use sqd_query::{Chunk, JsonLinesWriter, ParquetChunk, Query};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn run_query_on_chunk(query_json: &[u8], chunk: &dyn Chunk) -> Vec<u8> {
    let query = Query::from_json_bytes(query_json).unwrap();
    let plan = query.compile();
    let mut json_writer = JsonLinesWriter::new(Vec::new());
    if let Some(mut blocks) = plan.execute(chunk).unwrap() {
        json_writer.write_blocks(&mut blocks).unwrap();
    }
    json_writer.finish().unwrap()
}

fn run_query_on_storage(query_json: &[u8], storage: &LegacyStorage) -> Vec<u8> {
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

struct BenchCase {
    name: &'static str,
    query_json: &'static [u8],
}

fn measure_latency_parquet(
    case: &BenchCase,
    chunk: &ParquetChunk,
    n: usize,
) -> Duration {
    let _ = run_query_on_chunk(case.query_json, chunk);
    let mut times = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let _ = run_query_on_chunk(case.query_json, chunk);
        times.push(t.elapsed());
    }
    times.sort();
    times[n / 2]
}

fn measure_latency_storage(
    case: &BenchCase,
    storage: &LegacyStorage,
    n: usize,
) -> Duration {
    let _ = run_query_on_storage(case.query_json, storage);
    let mut times = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let _ = run_query_on_storage(case.query_json, storage);
        times.push(t.elapsed());
    }
    times.sort();
    times[n / 2]
}

fn measure_throughput_parquet(
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
                    let _ = run_query_on_chunk(case.query_json, chunk);
                    total.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
    });

    total.load(Ordering::Relaxed) as f64 / start.elapsed().as_secs_f64()
}

fn measure_throughput_storage(
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
                    let _ = run_query_on_storage(case.query_json, storage);
                    total.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
    });

    total.load(Ordering::Relaxed) as f64 / start.elapsed().as_secs_f64()
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

    eprintln!("=== Loading data into legacy backends ===");

    // Parquet (legacy engine's ParquetChunk)
    let parquet_evm: Option<ParquetChunk> =
        has_evm.then(|| ParquetChunk::new(evm_chunk_dir.to_str().unwrap()));
    let parquet_sol: Option<ParquetChunk> =
        has_sol.then(|| ParquetChunk::new(sol_chunk_dir.to_str().unwrap()));

    // sqd_storage (legacy RocksDB)
    let legacy_evm_dir = tmp.path().join("legacy_evm");
    let legacy_sol_dir = tmp.path().join("legacy_sol");

    let mut storage_evm: Option<LegacyStorage> = None;
    let mut storage_sol: Option<LegacyStorage> = None;

    if has_evm {
        let t = Instant::now();
        storage_evm = Some(load_parquet_to_legacy_storage(evm_chunk_dir, &legacy_evm_dir).unwrap());
        eprintln!(
            "  sqd_storage EVM:  {} (load: {:.2?})",
            format_size(dir_size(&legacy_evm_dir)),
            t.elapsed()
        );
    }

    if has_sol {
        let t = Instant::now();
        storage_sol = Some(load_parquet_to_legacy_storage(sol_chunk_dir, &legacy_sol_dir).unwrap());
        eprintln!(
            "  sqd_storage SOL:  {} (load: {:.2?})",
            format_size(dir_size(&legacy_sol_dir)),
            t.elapsed()
        );
    }

    // Build query cases
    let mut evm_cases: Vec<BenchCase> = Vec::new();
    let mut sol_cases: Vec<BenchCase> = Vec::new();
    if has_evm {
        for (name, json) in EVM_QUERIES {
            evm_cases.push(BenchCase {
                name,
                query_json: json,
            });
        }
    }
    if has_sol {
        for (name, json) in SOL_QUERIES {
            sol_cases.push(BenchCase {
                name,
                query_json: json,
            });
        }
    }

    let all_cases: Vec<(&BenchCase, bool)> = evm_cases
        .iter()
        .map(|c| (c, true))
        .chain(sol_cases.iter().map(|c| (c, false)))
        .collect();

    // Verify correctness
    eprintln!("\n=== Verifying correctness ===");
    for (case, is_evm) in &all_cases {
        let pq = if *is_evm { &parquet_evm } else { &parquet_sol };
        let st = if *is_evm { &storage_evm } else { &storage_sol };

        let pq_out = pq
            .as_ref()
            .map(|c| run_query_on_chunk(case.query_json, c))
            .unwrap_or_default();
        let st_out = st
            .as_ref()
            .map(|s| run_query_on_storage(case.query_json, s))
            .unwrap_or_default();

        if pq_out == st_out {
            eprintln!("  OK {}: {} bytes", case.name, pq_out.len());
        } else {
            eprintln!(
                "  MISMATCH {}: parquet={} vs storage={} bytes",
                case.name,
                pq_out.len(),
                st_out.len()
            );
        }
    }

    let col_w = 14;
    let name_w = 26;

    // Latency
    let n = 20;
    eprintln!("\n=== Latency ({n} runs, median) ===");
    println!();
    println!("=== Legacy Engine: Latency (median of {n} runs) ===");
    print!("{:<name_w$}", "Query");
    print!("{:>col_w$}", "Parquet");
    print!("{:>col_w$}", "sqd_storage");
    println!();
    println!("{}", "-".repeat(name_w + 2 * col_w));

    for (case, is_evm) in &all_cases {
        eprint!("\r  running: {:<name_w$}", case.name);
        print!("{:<name_w$}", case.name);

        let pq = if *is_evm { &parquet_evm } else { &parquet_sol };
        let st = if *is_evm { &storage_evm } else { &storage_sol };

        let pq_ms = pq
            .as_ref()
            .map(|c| measure_latency_parquet(case, c, n));
        let st_ms = st
            .as_ref()
            .map(|s| measure_latency_storage(case, s, n));

        print!(
            "{:>col_w$}",
            pq_ms
                .map(|d| format!("{:.2?}", d))
                .unwrap_or_else(|| "-".to_string())
        );
        print!(
            "{:>col_w$}",
            st_ms
                .map(|d| format!("{:.2?}", d))
                .unwrap_or_else(|| "-".to_string())
        );
        println!();
    }
    eprint!("\r{}\r", " ".repeat(60));

    // Throughput at multiple concurrency levels
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
        println!("=== Legacy Engine: Throughput (rps, CPU={cpu}, 3s) ===");
        print!("{:<name_w$}", "Query");
        print!("{:>col_w$}", "Parquet");
        print!("{:>col_w$}", "sqd_storage");
        println!();
        println!("{}", "-".repeat(name_w + 2 * col_w));

        for (case, is_evm) in &all_cases {
            eprint!("\r  running: {:<name_w$}", case.name);
            print!("{:<name_w$}", case.name);

            let pq = if *is_evm { &parquet_evm } else { &parquet_sol };
            let st = if *is_evm { &storage_evm } else { &storage_sol };

            let pq_rps = pq
                .as_ref()
                .map(|c| measure_throughput_parquet(case, c, cpu, duration));
            let st_rps = st
                .as_ref()
                .map(|s| measure_throughput_storage(case, s, cpu, duration));

            print!(
                "{:>col_w$}",
                pq_rps
                    .map(|r| format!("{r:.1}"))
                    .unwrap_or_else(|| "-".to_string())
            );
            print!(
                "{:>col_w$}",
                st_rps
                    .map(|r| format!("{r:.1}"))
                    .unwrap_or_else(|| "-".to_string())
            );
            println!();
        }
        eprint!("\r{}\r", " ".repeat(60));
    }

    // Disk size
    println!();
    println!("=== Disk Size ===");
    if has_evm {
        println!(
            "EVM:    Parquet={}, sqd_storage={}",
            format_size(dir_size(evm_chunk_dir)),
            format_size(dir_size(&legacy_evm_dir))
        );
    }
    if has_sol {
        println!(
            "Solana: Parquet={}, sqd_storage={}",
            format_size(dir_size(sol_chunk_dir)),
            format_size(dir_size(&legacy_sol_dir))
        );
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
