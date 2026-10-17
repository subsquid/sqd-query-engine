//! Many queries at once on one rayon pool: no caller waits behind the others.

#[path = "../benches/cases.rs"]
mod cases;
#[path = "../benches/run.rs"]
mod run;

use run::{run_query, Format};
use sqd_query_engine::scan::ParquetChunkReader;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Four callers to a thread. Alone the query takes about 50 ms on four
/// threads, so a fair pool gives each caller ten or more in the window.
const THREADS: usize = 4;
const CALLERS: usize = 16;
const WINDOW: Duration = Duration::from_secs(5);
/// Relations and several tables: forks inside forks.
const CASE: &str = "evm/selective/usdc_traces_and_diffs.evm-small";

/// Callers that hand the query to the pool, as a network worker does, each
/// back to back. A caller that finishes none waited the whole window while the
/// others finished theirs. The threads are not joined: a starved caller can
/// wait for minutes.
#[test]
#[ignore = "requires external chunk data"]
fn every_caller_finishes_a_query() {
    let case = cases::all_cases()
        .into_iter()
        .find(|case| case.name == CASE)
        .unwrap_or_else(|| panic!("no case {CASE}"));
    let query = Arc::new(case.query());
    let meta = Arc::new(case.catalog());
    let chunk = Arc::new(ParquetChunkReader::open(&case.chunk_dir()).unwrap());
    let pool = Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(THREADS)
            .build()
            .unwrap(),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let finished: Arc<Vec<AtomicU64>> = Arc::new((0..CALLERS).map(|_| AtomicU64::new(0)).collect());

    for caller in 0..CALLERS {
        let (query, meta, chunk, pool) = (query.clone(), meta.clone(), chunk.clone(), pool.clone());
        let (stop, finished) = (stop.clone(), finished.clone());

        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                pool.install(|| run_query(&query, &meta, chunk.as_ref(), Format::Json));
                finished[caller].fetch_add(1, Ordering::Relaxed);
            }
        });
    }

    let started = Instant::now();
    std::thread::sleep(WINDOW);
    stop.store(true, Ordering::Relaxed);

    let counts: Vec<u64> = finished
        .iter()
        .map(|count| count.load(Ordering::Relaxed))
        .collect();
    let starved = counts.iter().filter(|&&count| count == 0).count();
    assert_eq!(
        starved,
        0,
        "{starved} of {CALLERS} callers finished no query in {:?}; queries per caller: {counts:?}",
        started.elapsed()
    );
}
