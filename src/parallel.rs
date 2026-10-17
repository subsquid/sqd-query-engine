//! Fork-join that never waits behind another query.
//!
//! A rayon worker that waits for a stolen half of a `join` runs other jobs in
//! the meantime, the root of another caller's query among them. The waiting
//! query then resumes only once that query finishes, and under steady load
//! some queries wait without bound. Here the thread that forks runs the items
//! itself, pool threads only help, and a thread that waits runs nothing else:
//! a query moves as fast as its own thread when the pool is busy, and faster
//! when the pool has threads to spare.

use std::any::Any;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

/// `f` of every item, in item order.
pub(crate) fn map<T: Sync, R: Send>(items: &[T], f: impl Fn(&T) -> R + Sync) -> Vec<R> {
    let slots: Vec<Mutex<Option<R>>> = items.iter().map(|_| Mutex::new(None)).collect();

    run(items.len(), &|index| {
        let value = f(&items[index]);
        *slots[index].lock().unwrap() = Some(value);
    });

    slots
        .into_iter()
        .map(|slot| slot.into_inner().unwrap().expect("every item ran"))
        .collect()
}

/// Both results, `a` and `b` run side by side.
pub(crate) fn join<A, B, RA, RB>(a: A, b: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    let (a, b) = (Mutex::new(Some(a)), Mutex::new(Some(b)));
    let (ra, rb) = (Mutex::new(None), Mutex::new(None));

    run(2, &|index| {
        if index == 0 {
            let a = a.lock().unwrap().take().expect("a runs once");
            *ra.lock().unwrap() = Some(a());
        } else {
            let b = b.lock().unwrap().take().expect("b runs once");
            *rb.lock().unwrap() = Some(b());
        }
    });

    let ra = ra.into_inner().unwrap().expect("a ran");
    let rb = rb.into_inner().unwrap().expect("b ran");
    (ra, rb)
}

/// Run `task` on every index below `len`, on this thread and on helpers from
/// the current rayon pool, and return once every index has run.
fn run(len: usize, task: &(dyn Fn(usize) + Sync)) {
    let helpers = len.min(rayon::current_num_threads()).saturating_sub(1);
    if helpers == 0 {
        (0..len).for_each(task);
        return;
    }

    // SAFETY: only the lifetime changes, and a raw pointer may outlive its
    // pointee. `Stage::help` calls the task only for an index it claimed below
    // `len`, and this function returns or unwinds only after `wait` has seen
    // every such index finish (see `Finish`), so no call outlives `task`.
    let task = unsafe {
        std::mem::transmute::<*const (dyn Fn(usize) + Sync + '_), *const (dyn Fn(usize) + Sync)>(
            task,
        )
    };
    let stage = Arc::new(Stage {
        task,
        len,
        next: AtomicUsize::new(0),
        unspawned: AtomicIsize::new(helpers as isize),
        unfinished: Mutex::new(len),
        finished: Condvar::new(),
        panic: Mutex::new(None),
    });

    let finish = Finish(&stage);
    stage.spread();
    stage.help();
    drop(finish);

    let panic = lock(&stage.panic).take();
    if let Some(panic) = panic {
        resume_unwind(panic);
    }
}

/// Waits for the helpers on every path out of `run`, unwinding included: a
/// helper may still be inside the task. Before it waits, it runs the items no
/// one claimed, which only an unwind leaves.
struct Finish<'a>(&'a Stage);

impl Drop for Finish<'_> {
    fn drop(&mut self) {
        self.0.help();
        self.0.wait();
    }
}

/// No code that can panic runs while a stage's mutex is held, so a poisoned
/// one holds nothing half-written.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One fork: the items still to claim, and the ones claimed but not finished.
struct Stage {
    /// Valid while any index below `len` is unfinished; see `run`.
    task: *const (dyn Fn(usize) + Sync),
    len: usize,
    next: AtomicUsize,
    /// Helpers still to spawn, one fewer than the items or the pool's threads.
    /// A reservation past zero only takes it further below.
    unspawned: AtomicIsize,
    unfinished: Mutex<usize>,
    finished: Condvar,
    /// The first panic of any item, raised again on the forking thread.
    panic: Mutex<Option<Box<dyn Any + Send>>>,
}

// SAFETY: the task is `Sync`, and `help` reaches it only while it is valid.
unsafe impl Send for Stage {}
unsafe impl Sync for Stage {}

impl Stage {
    /// Spawn up to two helpers while items are unclaimed, each of which spreads
    /// in turn. Waking a pool thread costs the waker a few microseconds, so a
    /// fork that woke every helper itself would start its own items late; and
    /// a helper that a busy pool runs late spawns nothing.
    fn spread(self: &Arc<Self>) {
        for _ in 0..2 {
            if self.next.load(Ordering::Relaxed) >= self.len {
                return;
            }

            let left = self.unspawned.fetch_sub(1, Ordering::Relaxed);
            if left <= 0 {
                return;
            }

            let stage = self.clone();
            rayon::spawn(move || {
                stage.spread();
                stage.help();
            });
        }
    }

    /// Claim and run items until none is left. A helper that starts after the
    /// last claim touches nothing but the counter.
    fn help(&self) {
        loop {
            let index = self.next.fetch_add(1, Ordering::Relaxed);
            if index >= self.len {
                return;
            }

            // SAFETY: `index` is below `len` and unfinished; see `run`.
            let task = unsafe { &*self.task };
            if let Err(panic) = catch_unwind(AssertUnwindSafe(|| task(index))) {
                self.keep_first(panic);
            }

            let mut unfinished = lock(&self.unfinished);
            *unfinished -= 1;
            if *unfinished == 0 {
                self.finished.notify_all();
            }
        }
    }

    /// Keep the first panic to raise again; drop the others. A payload's drop
    /// is the panicking code's own and may panic too, which must not unwind
    /// past the count of finished items: that panic's payload is leaked.
    fn keep_first(&self, panic: Box<dyn Any + Send>) {
        let mut first = lock(&self.panic);
        if first.is_none() {
            *first = Some(panic);
            return;
        }
        drop(first);

        if let Err(again) = catch_unwind(AssertUnwindSafe(|| drop(panic))) {
            std::mem::forget(again);
        }
    }

    /// Block, running nothing else, until every item has finished.
    fn wait(&self) {
        let mut unfinished = lock(&self.unfinished);
        while *unfinished > 0 {
            unfinished = self
                .finished
                .wait(unfinished)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::thread::ThreadId;
    use std::time::{Duration, Instant};

    fn pool(threads: usize) -> rayon::ThreadPool {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap()
    }

    #[test]
    fn results_keep_item_order() {
        let items: Vec<u64> = (0..1000).collect();
        let squares = pool(4).install(|| map(&items, |item| item * item));
        assert_eq!(
            squares,
            items.iter().map(|item| item * item).collect::<Vec<_>>()
        );
    }

    #[test]
    fn an_item_may_fork_again() {
        let rows: Vec<Vec<u64>> = (0..16).map(|row| (0..row).collect()).collect();
        let sums =
            pool(4).install(|| map(&rows, |row| map(row, |value| value + 1).iter().sum::<u64>()));
        let expected: Vec<u64> = rows
            .iter()
            .map(|row| row.iter().map(|value| value + 1).sum())
            .collect();
        assert_eq!(sums, expected);
    }

    #[test]
    fn join_returns_both_sides() {
        let (left, right) = pool(2).install(|| join(|| "left", || 2));
        assert_eq!((left, right), ("left", 2));
    }

    /// The panic reaches the thread that forked, after every other item ran.
    #[test]
    fn a_panic_reaches_the_caller() {
        let ran = AtomicUsize::new(0);
        let items: Vec<usize> = (0..64).collect();

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            pool(4).install(|| {
                map(&items, |&item| {
                    ran.fetch_add(1, Ordering::Relaxed);
                    assert_ne!(item, 7, "item seven");
                })
            })
        }));

        let panic = result.expect_err("the panic is raised again");
        let message = panic.downcast_ref::<String>().unwrap();
        assert!(message.contains("item seven"), "{message}");
        assert_eq!(ran.into_inner(), items.len());
    }

    /// The fork claims every item while the pool's other thread is busy; its
    /// helper starts after the fork returned, and must not reach the task.
    #[test]
    fn a_late_helper_finds_nothing() {
        let pool = pool(2);
        let (busy, blocked) = std::sync::mpsc::channel::<()>();
        let (started, running) = std::sync::mpsc::channel::<()>();
        pool.spawn(move || {
            started.send(()).unwrap();
            blocked.recv().unwrap();
        });
        running.recv().unwrap();

        let items = vec![1u64, 2, 3];
        let doubled = pool.install(|| map(&items, |item| item * 2));
        drop(items);
        // The free thread runs its own queued helper before an injected job.
        pool.install(|| ());
        busy.send(()).unwrap();

        assert_eq!(doubled, [2, 4, 6]);
    }

    /// A panic payload whose drop panics as well.
    struct PanicsOnDrop;

    impl Drop for PanicsOnDrop {
        fn drop(&mut self) {
            panic!("a payload's drop");
        }
    }

    /// Two threads; a helper's job that panics past the fork must not abort
    /// the test binary.
    fn tolerant_pool() -> rayon::ThreadPool {
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .panic_handler(|_| {})
            .build()
            .unwrap()
    }

    /// Spin until `flag` is set, for a few seconds at most.
    fn until(flag: &AtomicBool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !flag.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::hint::spin_loop();
        }
    }

    /// The forking thread panics twice, and the second payload's drop panics.
    /// The fork must still wait for the item a helper is running: that item
    /// borrows from the fork's frame.
    #[test]
    fn a_payload_that_panics_on_drop_still_waits_for_helpers() {
        static RUNNING: AtomicBool = AtomicBool::new(false);
        static DONE: AtomicBool = AtomicBool::new(false);
        static PANICS: AtomicUsize = AtomicUsize::new(0);

        let (sent, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let pool = tolerant_pool();
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                pool.install(|| {
                    let forking = std::thread::current().id();
                    map(&[0, 1, 2], move |_| {
                        if std::thread::current().id() != forking {
                            RUNNING.store(true, Ordering::SeqCst);
                            std::thread::sleep(Duration::from_millis(300));
                            DONE.store(true, Ordering::SeqCst);
                            // A panic, so that a fork that did not wait leaves
                            // no slot for this item to write into.
                            std::panic::panic_any("helper");
                        }

                        until(&RUNNING);
                        if PANICS.fetch_add(1, Ordering::SeqCst) == 0 {
                            std::panic::panic_any("first");
                        }
                        std::panic::panic_any(PanicsOnDrop);
                    })
                })
            }));
            let done = DONE.load(Ordering::SeqCst);
            let first = result.map_err(|panic| panic.downcast_ref::<&str>().copied());
            sent.send((done, first.err().flatten())).unwrap();
        });

        let (done, first) = received
            .recv_timeout(Duration::from_secs(10))
            .expect("the fork waits for an item no one finished");
        assert!(done, "the fork returned while a helper ran");
        assert_eq!(first, Some("first"));
    }

    /// A helper's second panic has a payload whose drop panics. The helper
    /// must still count its item finished, or the fork waits for ever.
    #[test]
    fn a_helper_whose_payload_panics_on_drop_still_finishes() {
        static RUNNING: AtomicBool = AtomicBool::new(false);

        let (sent, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let pool = tolerant_pool();
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                pool.install(|| {
                    let forking: ThreadId = std::thread::current().id();
                    map(&[0, 1], move |_| {
                        if std::thread::current().id() == forking {
                            until(&RUNNING);
                            std::panic::panic_any("first");
                        }

                        RUNNING.store(true, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(100));
                        std::panic::panic_any(PanicsOnDrop);
                    })
                })
            }));
            let first = result.map_err(|panic| panic.downcast_ref::<&str>().copied());
            sent.send(first.err().flatten()).unwrap();
        });

        let first = received
            .recv_timeout(Duration::from_secs(10))
            .expect("the fork waits for an item its helper never finished");
        assert_eq!(first, Some("first"));
    }

    /// One item on each of two threads. The thread whose item ends first waits
    /// for the other one, and a job injected meanwhile must not run on it: the
    /// fork would return only once that job ends.
    #[test]
    fn a_waiting_thread_runs_no_other_job() {
        let pool = pool(2);
        let other = Duration::from_secs(2);

        std::thread::scope(|scope| {
            scope.spawn(|| {
                std::thread::sleep(Duration::from_millis(50));
                pool.spawn(move || std::thread::sleep(other));
            });

            let started = Instant::now();
            pool.install(|| {
                map(&[100, 300], |&ms| {
                    std::thread::sleep(Duration::from_millis(ms))
                })
            });
            let took = started.elapsed();

            assert!(took < other, "the fork took {took:?}, behind the other job");
        });
    }
}
