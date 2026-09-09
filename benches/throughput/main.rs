#[cfg(feature = "legacy-query")]
#[path = "../legacy.rs"]
mod legacy;
#[path = "../queries.rs"]
mod queries;

use queries::*;
use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::path::Path;
use std::sync::{Arc, Barrier, LazyLock, OnceLock};
use std::time::{Duration, Instant};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

static SOLANA_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap());

static EVM_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap());

fn to_json_lines(blocks: Option<sqd_query_engine::output::QueryOutput>) -> Vec<u8> {
    blocks.map(|b| b.into_json_lines()).unwrap_or_default()
}

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
    to_json_lines(execute_chunk(&plan, meta, chunk, false).unwrap())
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

#[derive(serde::Serialize)]
struct Measurement {
    requests: usize,
    elapsed_seconds: f64,
    rps: f64,
    cpu_seconds: Option<f64>,
    cpu_ms_per_query: Option<f64>,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    /// Sorted wall times, including execution, serialization and output drop.
    latency_ms: Vec<f64>,
}

#[cfg(unix)]
fn process_cpu_seconds() -> Option<f64> {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: time points to writable, correctly aligned storage. On success
    // clock_gettime initializes it; on failure we never read the storage.
    let status = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, time.as_mut_ptr()) };
    if status != 0 {
        return None;
    }
    // SAFETY: the successful call above initialized both timespec fields.
    let time = unsafe { time.assume_init() };
    Some(time.tv_sec as f64 + time.tv_nsec as f64 / 1e9)
}

#[cfg(not(unix))]
fn process_cpu_seconds() -> Option<f64> {
    None
}

/// Closed-loop load: each worker starts its next request after the previous
/// one finishes. This excludes any queue before the request enters the engine.
fn measure<F: Fn() + Sync>(run_once: F, concurrency: usize, duration: Duration) -> Measurement {
    assert!(concurrency > 0 && !duration.is_zero());
    let ready = Barrier::new(concurrency + 1);
    let begin = Barrier::new(concurrency + 1);
    let deadline = OnceLock::new();

    let (elapsed_seconds, cpu_seconds, mut latency_ms) = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..concurrency)
            .map(|_| {
                scope.spawn(|| {
                    let mut samples = Vec::with_capacity(4096);
                    ready.wait();
                    begin.wait();
                    let deadline = *deadline.get().expect("deadline set before start barrier");
                    loop {
                        let start = Instant::now();
                        if start >= deadline {
                            break;
                        }
                        run_once();
                        samples.push(start.elapsed().as_secs_f64() * 1000.0);
                    }
                    samples
                })
            })
            .collect();
        ready.wait();
        let cpu_start = process_cpu_seconds();
        let start = Instant::now();
        deadline.set(start + duration).expect("deadline set once");
        begin.wait();
        // Drain all requests started before the deadline. Include that drain
        // in elapsed time and CPU, but exclude sorting and JSON reporting.
        let worker_samples: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().expect("benchmark worker panicked"))
            .collect();
        let elapsed_seconds = start.elapsed().as_secs_f64();
        let cpu_seconds = cpu_start.zip(process_cpu_seconds()).map(|(a, b)| b - a);
        let samples: Vec<f64> = worker_samples.into_iter().flatten().collect();
        (elapsed_seconds, cpu_seconds, samples)
    });
    latency_ms.sort_unstable_by(f64::total_cmp);
    let requests = latency_ms.len();
    assert!(requests > 0, "measurement completed no requests");
    // Nearest-rank quantiles over every completed request, including drain.
    let percentile = |p: f64| latency_ms[(p * requests as f64).ceil() as usize - 1];
    Measurement {
        requests,
        elapsed_seconds,
        rps: requests as f64 / elapsed_seconds,
        cpu_seconds,
        cpu_ms_per_query: cpu_seconds.map(|s| s * 1000.0 / requests as f64),
        p50_ms: percentile(0.50),
        p95_ms: percentile(0.95),
        p99_ms: percentile(0.99),
        latency_ms,
    }
}

fn measure_new(case: &BenchCase, concurrency: usize, duration: Duration) -> Measurement {
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
    Some(
        measure(
            || {
                std::hint::black_box(legacy::run_query(case.query_json, &chunk));
            },
            concurrency,
            duration,
        )
        .rps,
    )
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
    let seconds = |flag: &str, default: f64| {
        args.iter().position(|a| a == flag).map_or(default, |i| {
            let value: f64 = args
                .get(i + 1)
                .expect("missing duration")
                .parse()
                .expect("invalid duration");
            assert!(
                value.is_finite() && value > 0.0,
                "duration must be finite and positive"
            );
            value
        })
    };
    let duration = Duration::from_secs_f64(seconds("--seconds", 5.0));
    let warmup = Duration::from_secs_f64(seconds("--warmup-seconds", 1.0));
    let json = args.iter().any(|a| a == "--json");

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
    cases.extend(build_cases(
        EVM_RPC_QUERIES,
        &EVM_META,
        "",
        &evm_chunk_path("small"),
    ));
    cases.extend(build_cases(
        SOL_QUERIES,
        &SOLANA_META,
        "",
        &sol_chunk_path(),
    ));

    if let Some(filter) = &filter {
        cases.retain(|case| case.name.contains(filter));
    }
    assert!(
        !cases.is_empty(),
        "no benchmark cases matched available chunk data"
    );

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
        let c = args
            .get(pos + 1)
            .expect("missing concurrency")
            .parse::<usize>()
            .expect("invalid concurrency");
        assert!(c > 0, "concurrency must be positive");
        vec![c]
    } else {
        vec![8]
    };

    if !json {
        println!();
        println!(
            "=== Throughput (rps, {}s per level) ===",
            duration.as_secs_f64()
        );
        if legacy_enabled {
            println!(
                "{:<40}{:>6}{:>11}{:>11}{:>9}",
                "Benchmark", "CPU", "New", "Legacy", "New/Leg"
            );
            println!("{}", "-".repeat(77));
        } else {
            println!("{:<40}{:>6}{:>11}", "Benchmark", "CPU", "New");
            println!("{}", "-".repeat(57));
        }
    }

    for case in &cases {
        for &cpu in &concurrency_levels {
            eprint!("\r  {:<40} CPU={cpu:<4}", case.name);
            // Warm the selected case at the same concurrency as measurement.
            std::hint::black_box(measure_new(case, cpu, warmup));
            let measurement = measure_new(case, cpu, duration);
            let new_rps = measurement.rps;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "case": case.name,
                        "concurrency": cpu,
                        "rayon_threads": rayon::current_num_threads(),
                        "warmup_seconds": warmup.as_secs_f64(),
                        "requested_seconds": duration.as_secs_f64(),
                        "measurement": measurement,
                        "legacy_rps": measure_legacy(case, cpu, duration),
                    })
                );
            } else if legacy_enabled {
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
        if !json {
            println!();
        }
    }
}
