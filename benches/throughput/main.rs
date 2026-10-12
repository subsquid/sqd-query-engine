//! Queries per second under concurrency, by criterion: every case of
//! `benches/queries/`, with threads that each run the same query back to back.
//! With `--features legacy-query` the legacy engine runs the same query on the
//! same chunk beside JSON, as the `legacy` function of each group.
//!
//!   cargo bench --bench throughput
//!   cargo bench --bench throughput --features legacy-query -- evm/selective
//!   SQD_BENCH_THREADS=1,8,96 SQD_BENCH_FORMATS=json,arrow cargo bench --bench throughput
//!
//! `SQD_BENCH_THREADS` lists the thread counts, 8 when unset. `SQD_BENCH_FORMATS`
//! lists how the response is consumed, from `json`, `stream` and `arrow`, JSON
//! when unset; legacy writes JSON only, so it runs only beside `json`. Criterion's
//! own options apply: a filter, `--save-baseline`, `--baseline`,
//! `--measurement-time`.
//!
//! One iteration is one query on each thread, all started together, and lasts
//! until the last of them finishes. With few iterations to a sample, the
//! slowest query of each round sets the time; a longer `--measurement-time`
//! gives a slow case more of them.

#[path = "../cases.rs"]
mod cases;
mod concurrently;
#[cfg(feature = "legacy-query")]
#[path = "../legacy.rs"]
mod legacy;
#[path = "../run.rs"]
mod run;

use cases::Case;
use concurrently::concurrently;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use run::{run_query, Format};
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::scan::ParquetChunkReader;
use std::cell::OnceCell;
use std::time::Duration;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// A case opened for running, on the first bench that runs it.
struct Prepared {
    query: Vec<u8>,
    meta: DatasetDescription,
    chunk: ParquetChunkReader,
    #[cfg(feature = "legacy-query")]
    legacy_chunk: sqd_query::ParquetChunk,
}

impl Prepared {
    fn open(case: &Case) -> Prepared {
        let dir = case.chunk_dir();
        Prepared {
            query: case.query(),
            meta: case.catalog(),
            chunk: ParquetChunkReader::open(&dir).unwrap(),
            #[cfg(feature = "legacy-query")]
            legacy_chunk: legacy::open_chunk(&dir),
        }
    }
}

fn throughput(c: &mut Criterion) {
    let threads = list("SQD_BENCH_THREADS", "8", concurrently::thread_count);
    let formats = list("SQD_BENCH_FORMATS", "json", |name| {
        Format::ALL.into_iter().find(|format| format.name() == name)
    });

    for case in cases::all_cases() {
        let opened = OnceCell::new();
        let prepared = || opened.get_or_init(|| Prepared::open(&case));

        let mut group = c.benchmark_group(&case.name);
        group.sampling_mode(SamplingMode::Flat);

        for &n in &threads {
            group.throughput(Throughput::Elements(n as u64));

            for &format in &formats {
                group.bench_function(BenchmarkId::new(format.name(), n), |b| {
                    let case = prepared();
                    b.iter_custom(|iters| {
                        concurrently(n, iters, || {
                            run_query(&case.query, &case.meta, &case.chunk, format);
                        })
                    })
                });
            }

            #[cfg(feature = "legacy-query")]
            {
                if formats.contains(&Format::Json) {
                    group.bench_function(BenchmarkId::new("legacy", n), |b| {
                        let case = prepared();
                        b.iter_custom(|iters| {
                            concurrently(n, iters, || {
                                let body = legacy::run_query(&case.query, &case.legacy_chunk);
                                std::hint::black_box(body);
                            })
                        })
                    });
                }
            }
        }

        group.finish();
    }
}

/// The comma-separated values of `variable`, or of `default` when it is unset.
fn list<T>(variable: &str, default: &str, parse: impl Fn(&str) -> Option<T>) -> Vec<T> {
    let text = std::env::var(variable).unwrap_or_else(|_| default.to_owned());
    text.split(',')
        .map(|value| {
            let value = value.trim();
            parse(value).unwrap_or_else(|| panic!("{variable}: cannot use {value:?}"))
        })
        .collect()
}

criterion_group! {
    name = benches;
    // A sample of a slow case lasts seconds, so ten of them.
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(5));
    targets = throughput
}
criterion_main!(benches);
