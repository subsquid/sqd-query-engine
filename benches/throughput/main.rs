#[cfg(feature = "legacy-query")]
#[path = "../legacy.rs"]
mod legacy;
#[path = "../queries.rs"]
mod queries;
#[path = "../rpc_workload.rs"]
mod rpc_workload;

use queries::*;
use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::{execute_chunk, execute_chunk_arrow};
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

/// Output format for the new-engine path. JSON (NDJSON) is the default and the
/// only format the legacy engine supports, so the new-vs-legacy A/B always runs
/// JSON; the Arrow variants drive the new-engine-only server-throughput numbers
/// (`--format arrow-bin[-zstd]`).
#[derive(Clone, Copy)]
enum OutFmt {
    Json,
    JsonZstd,
    Arrow { compress: bool, binary: bool },
}

impl OutFmt {
    fn parse(s: &str) -> Self {
        match s {
            "json" => OutFmt::Json,
            "json-zstd" => OutFmt::JsonZstd,
            "arrow" => OutFmt::Arrow {
                compress: false,
                binary: false,
            },
            "arrow-zstd" => OutFmt::Arrow {
                compress: true,
                binary: false,
            },
            "arrow-bin" => OutFmt::Arrow {
                compress: false,
                binary: true,
            },
            "arrow-bin-zstd" => OutFmt::Arrow {
                compress: true,
                binary: true,
            },
            other => {
                eprintln!("unknown --format {other}; using json");
                OutFmt::Json
            }
        }
    }
    fn label(self) -> &'static str {
        match self {
            OutFmt::Json => "json",
            OutFmt::JsonZstd => "json-zstd",
            OutFmt::Arrow {
                compress: false,
                binary: false,
            } => "arrow",
            OutFmt::Arrow {
                compress: true,
                binary: false,
            } => "arrow-zstd",
            OutFmt::Arrow {
                compress: false,
                binary: true,
            } => "arrow-bin",
            OutFmt::Arrow {
                compress: true,
                binary: true,
            } => "arrow-bin-zstd",
        }
    }
}

fn to_json_lines(blocks: Option<sqd_query_engine::output::QueryOutput>) -> Vec<u8> {
    blocks.map(|b| b.into_json_lines()).unwrap_or_default()
}

fn to_arrow_bytes(out: Option<sqd_query_engine::output::ArrowOutput>) -> Vec<u8> {
    out.map(|o| o.into_data()).unwrap_or_default()
}

/// Full pipeline: parse → compile → execute (new engine). Each call allocates
/// a fresh output buffer, mirroring one RPC request → one response buffer (and
/// keeping it symmetric with the legacy engine, which always allocates).
fn run_query(
    query_json: &[u8],
    meta: &sqd_query_engine::metadata::DatasetDescription,
    chunk: &ParquetChunkReader,
    fmt: OutFmt,
) -> Vec<u8> {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    match fmt {
        OutFmt::Json => to_json_lines(execute_chunk(&plan, meta, chunk, false).unwrap()),
        OutFmt::JsonZstd => {
            // NDJSON then whole-stream zstd-3 — the json+zstd wire baseline, so
            // arrow-bin-zstd can be compared compressed-vs-compressed.
            let ndjson = to_json_lines(execute_chunk(&plan, meta, chunk, false).unwrap());
            zstd::stream::encode_all(ndjson.as_slice(), 3).unwrap()
        }
        OutFmt::Arrow { compress, binary } => {
            to_arrow_bytes(execute_chunk_arrow(&plan, meta, chunk, compress, binary).unwrap())
        }
    }
}

struct BenchCase {
    name: String,
    /// Generated request variants: `(index into `chunks`, query JSON)`. The timed
    /// loop rotates through these, so successive requests hit different blocks
    /// across several chunks — a large, non-hot working set rather than one
    /// replayed row group.
    variants: Vec<rpc_workload::Variant>,
    meta: &'static sqd_query_engine::metadata::DatasetDescription,
    chunks: Vec<Arc<ParquetChunkReader>>,
    /// Parallel to `chunks`; only read by the legacy path (`--features legacy-query`).
    #[cfg_attr(not(feature = "legacy-query"), allow(dead_code))]
    chunk_dirs: Vec<String>,
}

/// Drive `run_once(i)` from `concurrency` threads for `duration`, return req/sec.
/// `i` is a globally-incrementing request index used to rotate query variants.
fn measure<F: Fn(usize) + Sync>(run_once: F, concurrency: usize, duration: Duration) -> f64 {
    let stop = AtomicBool::new(false);
    let idx = AtomicUsize::new(0);
    let total = AtomicUsize::new(0);

    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    run_once(idx.fetch_add(1, Ordering::Relaxed));
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

fn measure_new(case: &BenchCase, concurrency: usize, duration: Duration, fmt: OutFmt) -> f64 {
    let n = case.variants.len();
    measure(
        |i| {
            let (ci, q) = &case.variants[i % n];
            std::hint::black_box(run_query(q, case.meta, &case.chunks[*ci], fmt));
        },
        concurrency,
        duration,
    )
}

/// Throughput of the legacy engine on the same query/chunk, or `None` when the
/// `legacy-query` feature is disabled.
#[cfg(feature = "legacy-query")]
fn measure_legacy(case: &BenchCase, concurrency: usize, duration: Duration) -> Option<f64> {
    // Open + warm under catch_unwind: a real chunk may have a schema the legacy
    // engine rejects (it uses its own hardcoded dataset description). If warming
    // panics, skip legacy for this case instead of aborting the whole run. A
    // query that warms cleanly won't panic in the timed loop (same code path).
    // Open every chunk (cheap — legacy builds per-table readers lazily), then run
    // one variant (or all, under WARM=full) to mirror the new-engine validation
    // pass and to surface a schema the legacy engine rejects.
    let take = if std::env::var("WARM").as_deref() == Ok("full") {
        usize::MAX
    } else {
        1
    };
    let chunks = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let chunks: Vec<_> = case
            .chunk_dirs
            .iter()
            .map(|d| legacy::open_chunk(Path::new(d)))
            .collect();
        for (ci, q) in case.variants.iter().take(take) {
            std::hint::black_box(legacy::run_query(q, &chunks[*ci]));
        }
        chunks
    }))
    .ok()?;
    let n = case.variants.len();
    Some(measure(
        |i| {
            let (ci, q) = &case.variants[i % n];
            std::hint::black_box(legacy::run_query(q, &chunks[*ci]));
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
            variants: vec![(0, json.to_vec())],
            meta,
            chunks: vec![chunk.clone()],
            chunk_dirs: vec![chunk_dir.to_string()],
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
    // New-engine output format (`--format arrow-bin` etc.). JSON is the default.
    let fmt = args
        .iter()
        .position(|a| a == "--format")
        .and_then(|i| args.get(i + 1))
        .map_or(OutFmt::Json, |s| OutFmt::parse(s));
    // The legacy engine emits JSON only, so a New/Legacy ratio is meaningful just
    // for JSON. Arrow runs are new-engine-only — skip the legacy column entirely.
    let do_legacy = legacy_enabled && matches!(fmt, OutFmt::Json);

    let mut cases: Vec<BenchCase> = Vec::new();

    // Real-data RPC mode. `REAL_CHUNKS` is a comma list of `label=/chunk/dir`
    // (bare paths use the dir basename as label). Entries sharing a label form one
    // dataset's chunk set: every RPC method rotates requests across `m` probe
    // blocks per chunk (env `BENCH_BLOCKS`, default 8), so a case sweeps diverse
    // blocks over several chunks rather than replaying one. Skips the synthetic matrix.
    if let Ok(real) = std::env::var("REAL_CHUNKS") {
        let m = std::env::var("BENCH_BLOCKS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8usize);
        // Group chunk dirs by label, preserving first-seen order.
        let mut order: Vec<String> = Vec::new();
        let mut groups: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for entry in real.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (label, path) = match entry.split_once('=') {
                Some((l, p)) => (l.to_string(), p.to_string()),
                None => (
                    Path::new(entry)
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or(entry)
                        .to_string(),
                    entry.to_string(),
                ),
            };
            if !groups.contains_key(&label) {
                order.push(label.clone());
            }
            groups.entry(label).or_default().push(path);
        }
        for label in &order {
            let mut infos: Vec<rpc_workload::ChunkInfo> = Vec::new();
            let mut chunks: Vec<Arc<ParquetChunkReader>> = Vec::new();
            let mut dirs: Vec<String> = Vec::new();
            for path in &groups[label] {
                if !Path::new(path).exists() {
                    eprintln!("  skip (missing): {label} -> {path}");
                    continue;
                }
                let reader = match ParquetChunkReader::open(Path::new(path)) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("  skip (open failed): {label} -> {path}: {e}");
                        continue;
                    }
                };
                let info = rpc_workload::analyze_chunk(Path::new(path));
                eprintln!(
                    "[{label}] {}..{} ({} blk) traces={} hot_topic0={} hot_addr={}",
                    info.from,
                    info.to,
                    info.to.saturating_sub(info.from) + 1,
                    info.has_traces,
                    info.hot_topic0.as_deref().unwrap_or("-"),
                    info.hot_address.as_deref().unwrap_or("-"),
                );
                infos.push(info);
                chunks.push(Arc::new(reader));
                dirs.push(path.clone());
            }
            if chunks.is_empty() {
                continue;
            }
            eprintln!("[{label}] {} chunks × {m} probe blocks/chunk", chunks.len());
            for (name, variants) in rpc_workload::gen_variants(&infos, m) {
                cases.push(BenchCase {
                    name: format!("{label}/{name}"),
                    variants,
                    meta: &EVM_META,
                    chunks: chunks.clone(),
                    chunk_dirs: dirs.clone(),
                });
            }
        }
    } else {
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
    }

    if cases.is_empty() {
        eprintln!("No chunk data found. Set REAL_CHUNKS=... or provide data/{{evm,solana}}/chunk/");
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
        // Accepts a single level or a comma list, e.g. `--cpu 4,8`.
        args.get(pos + 1)
            .map(|v| {
                v.split(',')
                    .filter_map(|s| s.trim().parse::<usize>().ok())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or(vec![8])
    } else {
        vec![8]
    };

    // Per-(case, cpu, engine) measurement window. Override with `BENCH_SECS` to
    // trade precision for a faster full sweep over many real chunks.
    let duration = Duration::from_secs(
        std::env::var("BENCH_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5),
    );

    // Validation pass — NOT a cache warm. By default it runs just one variant of
    // each case so the new engine can drop a case it can't run (instead of
    // aborting mid-sweep); the timed loop then hits the remaining probe blocks
    // cold (first-touch). Set `WARM=full` to pre-touch every variant instead, for
    // the page-cache-warm number. Working sets here fit in RAM, so over a 5s loop
    // the two converge anyway — warm mainly removes first-touch startup noise.
    let warm_full = std::env::var("WARM").as_deref() == Ok("full");
    eprintln!(
        "{}",
        if warm_full {
            "Warming all variants..."
        } else {
            "Validating cases..."
        }
    );
    cases.retain(|case| {
        let take = if warm_full { usize::MAX } else { 1 };
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            for (ci, q) in case.variants.iter().take(take) {
                std::hint::black_box(run_query(q, case.meta, &case.chunks[*ci], fmt));
            }
        }))
        .map_err(|_| eprintln!("  drop (new-engine panic): {}", case.name))
        .is_ok()
    });

    // Response-size mode (`--sizes`): the JSON output byte size per case (new
    // engine), median/min/max across the case's variants. Deterministic in
    // (query, chunk data) — independent of host — so one run characterises the
    // response for every machine using the same chunks. No timing.
    if args.iter().any(|a| a == "--sizes") {
        println!();
        println!(
            "=== Response size (new engine, {} bytes; median / min / max over variants) ===",
            fmt.label()
        );
        println!(
            "{:<42}{:>12}{:>12}{:>12}",
            "Benchmark", "median", "min", "max"
        );
        println!("{}", "-".repeat(78));
        for case in &cases {
            if let Some(f) = &filter {
                if !case.name.contains(f.as_str()) {
                    continue;
                }
            }
            let mut sizes: Vec<usize> = case
                .variants
                .iter()
                .filter_map(|(ci, q)| {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        run_query(q, case.meta, &case.chunks[*ci], fmt).len()
                    }))
                    .ok()
                })
                .collect();
            if sizes.is_empty() {
                continue;
            }
            sizes.sort_unstable();
            println!(
                "{:<42}{:>12}{:>12}{:>12}",
                case.name,
                sizes[sizes.len() / 2],
                sizes[0],
                sizes[sizes.len() - 1]
            );
        }
        return;
    }

    println!();
    println!(
        "=== Throughput (rps, {}s per level; new-engine format = {}) ===",
        duration.as_secs(),
        fmt.label()
    );
    if do_legacy {
        println!(
            "{:<40}{:>6}{:>11}{:>11}{:>9}",
            "Benchmark", "CPU", "New", "Legacy", "New/Leg"
        );
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
            let new_rps = measure_new(case, cpu, duration, fmt);
            if do_legacy {
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
