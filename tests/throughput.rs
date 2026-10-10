//! The throughput bench's rate, against threads that share one resource. A
//! slice of a query takes as long as there are threads still in the bench, so
//! the rate while all of them run is known in advance, and a thread left alone
//! runs faster, as under contention for cores.

#[path = "../benches/throughput/measure.rs"]
mod measure;

use std::cell::OnceCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

/// A slice's time for each thread still in the bench.
const SLICE: Duration = Duration::from_millis(10);

/// The tests spin to keep time, so they run one at a time.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

#[derive(Clone, Copy)]
enum Query {
    Slices(u32),
    /// Ends only once every other thread has left the bench.
    HeldBack,
    /// That many one-slice queries, then one held back.
    HeldBackAfter(u32),
}

/// The queries of the bench's threads, in the order the threads start.
struct Model {
    queries: Vec<Query>,
    in_bench: AtomicUsize,
    started: AtomicUsize,
}

impl Model {
    fn new(queries: Vec<Query>) -> Arc<Self> {
        Arc::new(Model {
            in_bench: AtomicUsize::new(queries.len()),
            started: AtomicUsize::new(0),
            queries,
        })
    }

    fn run(model: &Arc<Self>) {
        let query = SEAT.with(|seat| {
            let seat = seat.get_or_init(|| {
                let thread = model.started.fetch_add(1, Ordering::SeqCst);
                Seat {
                    model: model.clone(),
                    query: model.queries[thread],
                    done: std::cell::Cell::new(0),
                }
            });
            let done = seat.done.get();
            seat.done.set(done + 1);

            match seat.query {
                Query::HeldBackAfter(quick) if done < quick => Query::Slices(1),
                Query::HeldBackAfter(_) => Query::HeldBack,
                query => query,
            }
        });

        match query {
            Query::Slices(slices) => {
                for _ in 0..slices {
                    let sharing = model.in_bench.load(Ordering::SeqCst) as u32;
                    wait(SLICE * sharing);
                }
            }
            Query::HeldBack | Query::HeldBackAfter(_) => {
                while model.in_bench.load(Ordering::SeqCst) > 1 {
                    std::hint::spin_loop();
                }
            }
        }
    }
}

/// A bench thread's place, given back when the thread exits.
struct Seat {
    model: Arc<Model>,
    query: Query,
    done: std::cell::Cell<u32>,
}

impl Drop for Seat {
    fn drop(&mut self) {
        self.model.in_bench.fetch_sub(1, Ordering::SeqCst);
    }
}

thread_local! {
    static SEAT: OnceCell<Seat> = const { OnceCell::new() };
}

/// `thread::sleep` oversleeps by a third on some systems; a spin is late only
/// when the thread loses its core.
fn wait(time: Duration) {
    let until = Instant::now() + time;
    while Instant::now() < until {
        std::hint::spin_loop();
    }
}

/// The rate of `queries` over `duration`, or a failure when the bench is still
/// running after `limit`.
fn rate(queries: Vec<Query>, duration: Duration, limit: Duration) -> f64 {
    let model = Model::new(queries);
    let threads = model.queries.len();
    let (sender, receiver) = mpsc::channel();

    std::thread::spawn(move || {
        let rate = measure::measure(|| Model::run(&model), threads, duration);
        sender.send(rate).ok();
    });

    receiver
        .recv_timeout(limit)
        .expect("the bench was still running long after its stop")
}

/// The stop falls between the first query of the thread with 11 slices, at
/// 220 ms, and the second query of the thread with 6, at 240 ms; that query
/// then ends beside the other thread, which finishes its own alone. A late
/// slice only lowers the rate, so the rate may fall short of the expected one
/// but never pass it.
#[test]
fn the_rate_counts_only_queries_run_beside_every_other_thread() {
    let _alone = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let together = |slices: u32| 1.0 / (SLICE * 2 * slices).as_secs_f64();
    let expected = together(6) + together(11);

    let rate = rate(
        vec![Query::Slices(6), Query::Slices(11)],
        Duration::from_millis(235),
        Duration::from_secs(5),
    );

    assert!(
        rate <= expected * 1.03,
        "{rate:.2} rps passes the {expected:.2} rps of the threads running together"
    );
    assert!(
        rate >= expected * 0.85,
        "{rate:.2} rps falls short of the {expected:.2} rps of the threads running together"
    );
}

/// A thread whose query lasts until the others leave finishes nothing before
/// the stop. The bench stops anyway and counts the other thread alone.
#[test]
fn a_query_held_back_until_the_others_leave_does_not_hold_the_stop() {
    let _alone = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let expected = 1.0 / (SLICE * 2).as_secs_f64();

    let rate = rate(
        vec![Query::Slices(1), Query::HeldBack],
        Duration::from_millis(50),
        Duration::from_secs(5),
    );

    assert!(
        rate <= expected * 1.03,
        "{rate:.2} rps passes the {expected:.2} rps of the thread that ran"
    );
    assert!(
        rate >= expected * 0.85,
        "{rate:.2} rps falls short of the {expected:.2} rps of the thread that ran"
    );
}

/// A thread finishes two queries, then waits for the rest of the window. Its unfinished query counts as one query at most: the pace of the
/// two it finished would count the wait as work done.
#[test]
fn a_query_held_back_late_counts_as_at_most_one_query() {
    let _alone = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let window = Duration::from_millis(210);
    let one_slice = SLICE * 2;
    let quick = (window.as_millis() / one_slice.as_millis()) as f64;
    let at_most = (quick + 1.0 + 2.0 + 1.0) / window.as_secs_f64();

    let rate = rate(
        vec![Query::Slices(1), Query::HeldBackAfter(2)],
        window,
        Duration::from_secs(5),
    );

    assert!(
        rate <= at_most * 1.03,
        "{rate:.2} rps passes the {at_most:.2} rps of every query finished or begun in the window"
    );
}
