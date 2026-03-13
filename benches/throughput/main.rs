#[path = "../queries.rs"]
mod queries;

use queries::*;
use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::execute_plan_cached;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetTable;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

static SOLANA_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap());

static EVM_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap());

fn open_cache(chunk_dir: &Path) -> HashMap<String, ParquetTable> {
    let mut cache = HashMap::new();
    if let Ok(entries) = std::fs::read_dir(chunk_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string();
                if let Ok(table) = ParquetTable::open(&path) {
                    cache.insert(name, table);
                }
            }
        }
    }
    cache
}

/// Full pipeline: parse → compile → execute (no plan caching)
fn run_query(
    query_json: &[u8],
    meta: &sqd_query_engine::metadata::DatasetDescription,
    chunk_dir: &Path,
    cache: &mut HashMap<String, ParquetTable>,
    buf: Vec<u8>,
) -> Vec<u8> {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_plan_cached(&plan, meta, chunk_dir, cache, buf).unwrap()
}

struct BenchCase {
    name: &'static str,
    query_json: &'static [u8],
    meta: &'static sqd_query_engine::metadata::DatasetDescription,
    chunk: &'static Path,
}

fn measure_throughput(case: &BenchCase, concurrency: usize, duration: Duration) -> f64 {
    let stop = AtomicBool::new(false);
    let total = AtomicUsize::new(0);

    let start = Instant::now();

    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| {
                let mut cache = open_cache(case.chunk);
                let mut buf = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    buf = run_query(case.query_json, case.meta, case.chunk, &mut cache, buf);
                    total.fetch_add(1, Ordering::Relaxed);
                    buf.clear();
                }
            });
        }

        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
    });

    let elapsed = start.elapsed().as_secs_f64();
    let count = total.load(Ordering::Relaxed);
    count as f64 / elapsed
}

fn build_cases(
    queries: &'static [(&'static str, &'static [u8])],
    meta: &'static sqd_query_engine::metadata::DatasetDescription,
    chunk: &Path,
) -> Vec<BenchCase> {
    if !chunk.exists() {
        return Vec::new();
    }
    let chunk: &'static Path = Box::leak(chunk.to_path_buf().into_boxed_path());
    queries
        .iter()
        .map(|(name, json)| BenchCase {
            name,
            query_json: json,
            meta,
            chunk,
        })
        .collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Default: only test at CPU=8. Pass "--all" for full sweep (1,2,4,8,...,max).
    let all_levels = args.iter().any(|a| a == "--all");

    let mut cases: Vec<BenchCase> = Vec::new();
    cases.extend(build_cases(
        EVM_QUERIES,
        &EVM_META,
        Path::new("data/evm/chunk"),
    ));
    cases.extend(build_cases(
        SOL_QUERIES,
        &SOLANA_META,
        Path::new("data/solana/chunk"),
    ));

    if cases.is_empty() {
        eprintln!("No fixture data found. Expected tests/fixtures/{{ethereum,solana}}/chunk/");
        return;
    }

    let concurrency_levels: Vec<usize> = if all_levels {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let mut levels = vec![1];
        let mut c = 2;
        while c <= cpus {
            levels.push(c);
            c *= 2;
        }
        if *levels.last().unwrap() != cpus {
            levels.push(cpus);
        }
        levels
    } else if let Some(pos) = args.iter().position(|a| a == "--cpu") {
        args.get(pos + 1)
            .and_then(|v| v.parse::<usize>().ok())
            .map(|c| vec![c])
            .unwrap_or(vec![8])
    } else {
        vec![8]
    };

    let duration = Duration::from_secs(5);

    // Warmup
    eprintln!("Warming up...");
    for case in &cases {
        let mut cache = open_cache(case.chunk);
        run_query(case.query_json, case.meta, case.chunk, &mut cache, Vec::new());
    }

    // Throughput
    println!();
    println!("=== Throughput (rps, 5s per level) ===");
    print!("{:<26}", "Benchmark");
    for cpu in &concurrency_levels {
        print!("{:>10}", format!("CPU={cpu}"));
    }
    println!();
    println!("{}", "-".repeat(26 + concurrency_levels.len() * 10));

    for case in &cases {
        print!("{:<26}", case.name);
        for &cpu in &concurrency_levels {
            eprint!("\r  {:<26} CPU={cpu:<4}", case.name);
            let rps = measure_throughput(case, cpu, duration);
            print!("{:>10}", format!("{rps:.1}"));
        }
        eprintln!();
        println!();
    }
}
