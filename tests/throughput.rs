//! The throughput bench's clock against the queries it counts: every counted
//! query runs inside the timed window, and the window holds no other queries.

#[path = "../benches/throughput/concurrently.rs"]
mod concurrently;

use concurrently::{concurrently, thread_count};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Many threads and short queries give the clock's thread the most chances to
/// fall behind the threads it releases.
const THREADS: usize = 16;
const ITERS: u64 = 20;
const ROUNDS: usize = 20;

/// `thread::sleep` oversleeps; a spin ends on time unless the thread loses its core.
fn spin(time: Duration) {
    let until = Instant::now() + time;
    while Instant::now() < until {
        std::hint::spin_loop();
    }
}

/// A query that starts before the timer, or ends after it, makes the window
/// shorter than the span from the first query's start to the last one's end.
/// Each thread runs exactly its iterations, so the rate counts what ran.
#[test]
fn every_query_runs_inside_the_timed_window() {
    for round in 0..ROUNDS {
        let spans = Mutex::new(Vec::new());

        let elapsed = concurrently(THREADS, ITERS, || {
            let start = Instant::now();
            spin(Duration::from_micros(100));
            spans.lock().unwrap().push((start, Instant::now()));
        });

        let spans = spans.into_inner().unwrap();
        assert_eq!(
            spans.len(),
            THREADS * ITERS as usize,
            "round {round}: the threads ran another number of queries"
        );

        let first = spans.iter().map(|(start, _)| *start).min().unwrap();
        let last = spans.iter().map(|(_, end)| *end).max().unwrap();
        assert!(
            last - first <= elapsed,
            "round {round}: the queries took {:?} from first start to last end, \
             but the window was {elapsed:?}",
            last - first
        );
    }
}

/// No threads run no queries in no time, which reads as a rate of anything.
#[test]
fn a_thread_count_is_at_least_one() {
    assert_eq!(thread_count("0"), None);
    assert_eq!(thread_count("1"), Some(1));
    assert_eq!(thread_count("96"), Some(96));
    assert_eq!(thread_count("eight"), None);
}
