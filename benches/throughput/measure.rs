//! How the throughput bench turns finished queries into a rate.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Drive `run_once` from `concurrency` threads for `duration`, return req/sec.
///
/// A thread's rate counts the queries it finished before the stop, up to the
/// last of them, so every counted query ran beside all the others. A query
/// that ends after the stop ran partly alone, which is faster when the threads
/// share cores; timing every thread to the slowest one's finish instead counts
/// the others' idle tails. The stop waits for no thread, since a query can
/// outlast the window, so a thread's unfinished query counts as one query at
/// most, however long the queries it finished took.
pub fn measure<F: Fn() + Sync>(run_once: F, concurrency: usize, duration: Duration) -> f64 {
    let stop = AtomicBool::new(false);

    let (finished, stopped) = std::thread::scope(|s| {
        let threads: Vec<_> = (0..concurrency)
            .map(|_| {
                s.spawn(|| {
                    let start = Instant::now();
                    let mut done = 0u64;
                    let mut last = Duration::ZERO;

                    while !stop.load(Ordering::Relaxed) {
                        run_once();
                        let finished = start.elapsed();
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }

                        done += 1;
                        last = finished;
                    }

                    (done, last, start)
                })
            })
            .collect();

        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);
        let stopped = Instant::now();

        let finished: Vec<(u64, Duration, Instant)> =
            threads.into_iter().map(|t| t.join().unwrap()).collect();
        (finished, stopped)
    });

    let rates: Vec<Option<f64>> = finished
        .into_iter()
        .map(|(done, last, start)| {
            let window = stopped.duration_since(start).as_secs_f64();
            let pace = done as f64 / last.as_secs_f64();
            let at_most = (done + 1) as f64 / window;
            (done > 0).then(|| pace.min(at_most))
        })
        .collect();

    let idle = rates.iter().filter(|rate| rate.is_none()).count();
    if idle > 0 {
        eprintln!(
            "\n{idle} of {concurrency} threads finished no query in {duration:?}; \
             the rate leaves them out"
        );
    }

    rates.into_iter().flatten().sum()
}
