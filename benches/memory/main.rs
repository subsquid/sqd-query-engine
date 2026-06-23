//! Memory benchmark: measures heap usage of the new vs legacy engine while
//! serving the same queries on the same chunk.
//!
//! A counting allocator wraps jemalloc and tracks every (de)allocation by
//! requested size, giving two metrics per query/engine:
//!   - alloc/query  : bytes allocated per query (allocator pressure / churn)
//!   - peak heap @C : peak simultaneously-live heap during a run at concurrency
//!                    C (the working set needed to serve C concurrent requests)
//!
//! Notes:
//!   - Sizes are *requested* bytes (pre size-class rounding) — consistent for
//!     both engines, so the comparison is fair; absolute RSS will be higher.
//!   - The mmap'd parquet data (new engine) is NOT counted (it's not heap); the
//!     chunk is loaded during warmup so any one-time heap is excluded from the
//!     per-run peak delta. So this measures per-query *working* memory.
//!   - Build with `--features legacy-query` to get the legacy columns.

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
use std::alloc::{GlobalAlloc, Layout};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::LazyLock;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Counting allocator wrapping the real allocator
// ---------------------------------------------------------------------------

#[cfg(not(target_env = "msvc"))]
const INNER: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
#[cfg(target_env = "msvc")]
const INNER: std::alloc::System = std::alloc::System;

static TOTAL_ALLOC: AtomicU64 = AtomicU64::new(0); // cumulative bytes requested
static LIVE: AtomicI64 = AtomicI64::new(0); // currently-live bytes
static PEAK: AtomicI64 = AtomicI64::new(0); // high-water of LIVE

#[inline]
fn account_alloc(size: i64) {
    TOTAL_ALLOC.fetch_add(size as u64, Ordering::Relaxed);
    let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = INNER.alloc(l);
        if !p.is_null() {
            account_alloc(l.size() as i64);
        }
        p
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = INNER.alloc_zeroed(l);
        if !p.is_null() {
            account_alloc(l.size() as i64);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        INNER.dealloc(p, l);
        LIVE.fetch_sub(l.size() as i64, Ordering::Relaxed);
    }
    unsafe fn realloc(&self, ptr: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let p = INNER.realloc(ptr, l, new);
        if !p.is_null() {
            let delta = new as i64 - l.size() as i64;
            if delta >= 0 {
                account_alloc(delta);
            } else {
                LIVE.fetch_sub(-delta, Ordering::Relaxed);
            }
        }
        p
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

// ---------------------------------------------------------------------------

static SOLANA_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap());
static EVM_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap());

fn run_new(
    json: &[u8],
    meta: &sqd_query_engine::metadata::DatasetDescription,
    chunk: &ParquetChunkReader,
) -> Vec<u8> {
    let parsed = parse_query(json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk(&plan, meta, chunk, Vec::new(), false).unwrap()
}

/// Bytes allocated per query (single-threaded). Allocation is deterministic per
/// query, so a small iteration count gives a stable figure.
fn alloc_per_query<F: Fn()>(run: F, iters: usize) -> u64 {
    let base = TOTAL_ALLOC.load(Ordering::Relaxed);
    for _ in 0..iters {
        run();
    }
    (TOTAL_ALLOC.load(Ordering::Relaxed) - base) / iters as u64
}

/// Peak simultaneously-live heap (bytes) attributable to a run at the given
/// concurrency, over `duration`. Resets the high-water mark to the current idle
/// baseline so engines measured in the same process don't contaminate each other.
fn peak_under_load<F: Fn() + Sync>(run: F, concurrency: usize, duration: Duration) -> i64 {
    let base_live = LIVE.load(Ordering::Relaxed);
    PEAK.store(base_live, Ordering::Relaxed);
    let stop = AtomicBool::new(false);
    std::thread::scope(|s| {
        for _ in 0..concurrency {
            s.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    run();
                }
            });
        }
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
    });
    PEAK.load(Ordering::Relaxed) - base_live
}

fn fmt_bytes(b: i64) -> String {
    let b = b as f64;
    if b >= 1024.0 * 1024.0 {
        format!("{:.1} MB", b / 1024.0 / 1024.0)
    } else if b >= 1024.0 {
        format!("{:.0} KB", b / 1024.0)
    } else {
        format!("{b:.0} B")
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let legacy_enabled = cfg!(feature = "legacy-query");
    let cpu: usize = args
        .iter()
        .position(|a| a == "--cpu")
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let filter = args
        .iter()
        .position(|a| a == "--filter")
        .and_then(|i| args.get(i + 1))
        .cloned();
    let iters = 20;
    let duration = Duration::from_millis(1500);

    type Case<'a> = (
        String,
        &'a [u8],
        &'a sqd_query_engine::metadata::DatasetDescription,
        String,
    );
    let mut cases: Vec<Case> = Vec::new();
    // EVM indexer + full-scan queries across the chunk matrix (small + big).
    for (label, path) in evm_chunk_matrix() {
        for &(n, q) in EVM_QUERIES.iter().chain(EVM_FULLSCAN_QUERIES) {
            cases.push((format!("{label}/{n}"), q, &EVM_META, path.clone()));
        }
    }
    // RPC queries are block-pinned to the small chunk; Solana has one chunk.
    for &(n, q) in EVM_RPC_QUERIES {
        cases.push((n.to_string(), q, &EVM_META, evm_chunk_path("small")));
    }
    for &(n, q) in SOL_QUERIES {
        cases.push((n.to_string(), q, &SOLANA_META, sol_chunk_path()));
    }

    println!();
    println!("=== Memory: alloc/query (single-thread) + peak heap @ CPU={cpu} ===");
    println!("(requested heap bytes; mmap'd parquet not counted)");
    println!();
    if legacy_enabled {
        println!(
            "{:<40}{:>12}{:>12}{:>13}{:>13}",
            "Benchmark", "new a/q", "leg a/q", "new peak", "leg peak"
        );
        println!("{}", "-".repeat(90));
    } else {
        println!("{:<40}{:>12}{:>13}", "Benchmark", "new a/q", "new peak");
        println!("{}", "-".repeat(65));
    }

    for (name, json, meta, dir) in &cases {
        if let Some(f) = &filter {
            if !name.contains(f.as_str()) {
                continue;
            }
        }
        if !Path::new(dir).exists() {
            continue;
        }
        eprint!("\r  measuring {name:<40}");

        // New engine
        let new_chunk = ParquetChunkReader::open(Path::new(dir)).unwrap();
        let new_run = || {
            std::hint::black_box(run_new(json, meta, &new_chunk));
        };
        new_run(); // warm (load chunk / caches)
        let new_aq = alloc_per_query(&new_run, iters);
        let new_peak = peak_under_load(&new_run, cpu, duration);
        drop(new_chunk);

        if legacy_enabled {
            let (leg_aq, leg_peak) = measure_legacy(json, dir, iters, cpu, duration);
            println!(
                "{:<40}{:>12}{:>12}{:>13}{:>13}",
                name,
                fmt_bytes(new_aq as i64),
                fmt_bytes(leg_aq as i64),
                fmt_bytes(new_peak),
                fmt_bytes(leg_peak),
            );
        } else {
            println!(
                "{:<40}{:>12}{:>13}",
                name,
                fmt_bytes(new_aq as i64),
                fmt_bytes(new_peak),
            );
        }
    }
    eprint!("\r{:<60}\r", "");
}

#[cfg(feature = "legacy-query")]
fn measure_legacy(
    json: &[u8],
    dir: &str,
    iters: usize,
    cpu: usize,
    duration: Duration,
) -> (u64, i64) {
    let chunk = legacy::open_chunk(Path::new(dir));
    let run = || {
        std::hint::black_box(legacy::run_query(json, &chunk));
    };
    run(); // warm
    let aq = alloc_per_query(&run, iters);
    let peak = peak_under_load(&run, cpu, duration);
    (aq, peak)
}

#[cfg(not(feature = "legacy-query"))]
fn measure_legacy(
    _json: &[u8],
    _dir: &str,
    _iters: usize,
    _cpu: usize,
    _duration: Duration,
) -> (u64, i64) {
    (0, 0)
}
