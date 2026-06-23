#[path = "../queries.rs"]
mod queries;
#[cfg(feature = "legacy-query")]
#[path = "../legacy.rs"]
mod legacy;

use queries::*;
use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

static SOLANA_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap());

static EVM_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap());

/// Full pipeline: parse → compile → execute (new engine). Each call allocates
/// a fresh output buffer, mirroring one RPC request → one response buffer (and
/// keeping it symmetric with the legacy engine, which always allocates).
fn run_query(
    query_json: &[u8],
    meta: &sqd_query_engine::metadata::DatasetDescription,
    chunk: &ParquetChunkReader,
) -> Vec<u8> {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk(&plan, meta, chunk, Vec::new(), false).unwrap()
}

struct BenchCase {
    name: String,
    query_json: &'static [u8],
    meta: &'static sqd_query_engine::metadata::DatasetDescription,
    chunk: Arc<ParquetChunkReader>,
    /// Only read by the legacy comparison path (`--features legacy-query`).
    #[cfg_attr(not(feature = "legacy-query"), allow(dead_code))]
    chunk_dir: String,
}

/// Drive `run_once` from `concurrency` threads for `duration`, return req/sec.
fn measure<F: Fn() + Sync>(run_once: F, concurrency: usize, duration: Duration) -> f64 {
    let stop = AtomicBool::new(false);
    let total = AtomicUsize::new(0);

    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    run_once();
                    total.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
    });

    let elapsed = start.elapsed().as_secs_f64();
    total.load(Ordering::Relaxed) as f64 / elapsed
}

fn measure_new(case: &BenchCase, concurrency: usize, duration: Duration) -> f64 {
    measure(
        || {
            std::hint::black_box(run_query(case.query_json, case.meta, &case.chunk));
        },
        concurrency,
        duration,
    )
}

/// Throughput of the legacy engine on the same query/chunk, or `None` when the
/// `legacy-query` feature is disabled.
#[cfg(feature = "legacy-query")]
fn measure_legacy(case: &BenchCase, concurrency: usize, duration: Duration) -> Option<f64> {
    let chunk = legacy::open_chunk(Path::new(&case.chunk_dir));
    // Warm the lazily-populated per-table reader cache before timing.
    std::hint::black_box(legacy::run_query(case.query_json, &chunk));
    Some(measure(
        || {
            std::hint::black_box(legacy::run_query(case.query_json, &chunk));
        },
        concurrency,
        duration,
    ))
}

#[cfg(not(feature = "legacy-query"))]
fn measure_legacy(_case: &BenchCase, _concurrency: usize, _duration: Duration) -> Option<f64> {
    None
}

/// Build one case per query against a single chunk. `label` is prefixed to the
/// case name (e.g. `big/`), empty for single-chunk sets (RPC, Solana).
fn build_cases(
    queries: &'static [(&'static str, &'static [u8])],
    meta: &'static sqd_query_engine::metadata::DatasetDescription,
    label: &str,
    chunk_dir: &str,
) -> Vec<BenchCase> {
    if !Path::new(chunk_dir).exists() {
        return Vec::new();
    }
    let chunk = Arc::new(ParquetChunkReader::open(Path::new(chunk_dir)).unwrap());
    queries
        .iter()
        .map(|(name, json)| BenchCase {
            name: if label.is_empty() {
                name.to_string()
            } else {
                format!("{label}/{name}")
            },
            query_json: json,
            meta,
            chunk: chunk.clone(),
            chunk_dir: chunk_dir.to_string(),
        })
        .collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Default: only test at CPU=8. Pass "--all" for full sweep (1,2,4,8,...,max).
    let all_levels = args.iter().any(|a| a == "--all");
    let legacy_enabled = cfg!(feature = "legacy-query");
    // Optional substring filter: `--filter trace_block` runs only matching cases.
    let filter = args
        .iter()
        .position(|a| a == "--filter")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let mut cases: Vec<BenchCase> = Vec::new();
    // EVM indexer + full-scan queries across the chunk matrix (small + big).
    for (label, path) in evm_chunk_matrix() {
        cases.extend(build_cases(EVM_QUERIES, &EVM_META, label, &path));
        cases.extend(build_cases(EVM_FULLSCAN_QUERIES, &EVM_META, label, &path));
    }
    // RPC queries are block-pinned to the small chunk; Solana has one chunk.
    cases.extend(build_cases(EVM_RPC_QUERIES, &EVM_META, "", &evm_chunk_path("small")));
    cases.extend(build_cases(SOL_QUERIES, &SOLANA_META, "", &sol_chunk_path()));

    if cases.is_empty() {
        eprintln!("No chunk data found. Expected data/{{evm,solana}}/chunk/");
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

    // Warmup (new engine; legacy warms its own cache inside measure_legacy)
    eprintln!("Warming up...");
    for case in &cases {
        std::hint::black_box(run_query(case.query_json, case.meta, &case.chunk));
    }

    println!();
    println!("=== Throughput (rps, 5s per level) ===");
    if legacy_enabled {
        println!("{:<40}{:>6}{:>11}{:>11}{:>9}", "Benchmark", "CPU", "New", "Legacy", "New/Leg");
        println!("{}", "-".repeat(77));
    } else {
        println!("{:<40}{:>6}{:>11}", "Benchmark", "CPU", "New");
        println!("{}", "-".repeat(57));
    }

    for case in &cases {
        if let Some(f) = &filter {
            if !case.name.contains(f.as_str()) {
                continue;
            }
        }
        for &cpu in &concurrency_levels {
            eprint!("\r  {:<40} CPU={cpu:<4}", case.name);
            let new_rps = measure_new(case, cpu, duration);
            if legacy_enabled {
                let leg = measure_legacy(case, cpu, duration).unwrap_or(0.0);
                let ratio = if leg > 0.0 { new_rps / leg } else { f64::NAN };
                println!(
                    "{:<40}{:>6}{:>11.1}{:>11.1}{:>8.2}x",
                    case.name, cpu, new_rps, leg, ratio
                );
            } else {
                println!("{:<40}{:>6}{:>11.1}", case.name, cpu, new_rps);
            }
        }
        eprint!("\r{:<40}\r", "");
        println!();
    }
}
