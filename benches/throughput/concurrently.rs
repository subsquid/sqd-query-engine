//! How the throughput bench times queries that run at once.

use std::sync::Barrier;
use std::time::{Duration, Instant};

/// The time `threads` threads take to run `query` `iters` times each, all
/// released at once. The threads start and wait before the timer starts, and
/// the timer starts before they are released, so it holds every query and no
/// thread start-up.
pub fn concurrently(threads: usize, iters: u64, query: impl Fn() + Sync) -> Duration {
    let ready = Barrier::new(threads + 1);
    let release = Barrier::new(threads + 1);

    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    ready.wait();
                    release.wait();
                    for _ in 0..iters {
                        query();
                    }
                })
            })
            .collect();

        ready.wait();
        let began = Instant::now();
        release.wait();

        for worker in workers {
            worker.join().unwrap();
        }
        began.elapsed()
    })
}

/// A thread count from `SQD_BENCH_THREADS`. No threads would run no queries
/// in no time.
pub fn thread_count(value: &str) -> Option<usize> {
    value.parse().ok().filter(|&threads| threads > 0)
}
