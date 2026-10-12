//! Instructions and heap of each bench query, counted under Valgrind by
//! Gungraun: Callgrind for instructions, DHAT for bytes allocated and peak heap.
//! One run of each build compares two builds. Bytes allocated and peak heap
//! repeat exactly. Instructions move by up to about 2% between runs of one
//! binary, mostly inside `memcpy`; the limit on instructions sits above that.
//!
//! Linux only. Needs `valgrind` and the runner of the same version as the
//! `gungraun` dev-dependency:
//!
//!   cargo install gungraun-runner --version 0.20.0
//!   git switch BASE && cargo bench --bench instructions -- --save-baseline=base --parallel
//!   git switch NEW  && cargo bench --bench instructions -- --baseline=base --parallel
//!
//! A query runs on one thread. Callgrind runs threads one at a time anyway, and
//! both tools count only the benchmark function's own thread, so work that a
//! pool thread took over would go uncounted. Wall time and contention are for
//! the `throughput` bench.

#[path = "../cases.rs"]
mod cases;
#[path = "../run.rs"]
mod run;

use cases::{cases, Case};
use gungraun::prelude::*;
use gungraun::{Callgrind, Dhat, DhatMetric, EventKind};
use run::{run_query, Format};
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::scan::ParquetChunkReader;
use std::hint::black_box;

/// More than any query's peak heap.
const BALLAST: usize = 4 << 30;

struct Prepared {
    query: Vec<u8>,
    meta: DatasetDescription,
    chunk: ParquetChunkReader,
    _ballast: Vec<u8>,
}

/// The case, opened and run once in each format, outside the counted call: the
/// first run maps the chunk's pages and fills lazy state. Leaked, so that
/// closing the chunk is not counted either.
fn prepare(case: Case) -> &'static Prepared {
    rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .use_current_thread()
        .build_global()
        .unwrap();

    let query = case.query();
    let meta = case.catalog();
    let chunk = ParquetChunkReader::open(&case.chunk_dir()).unwrap();
    for format in [Format::Json, Format::Arrow] {
        run_query(&query, &meta, &chunk, format);
    }

    // DHAT gives the bytes live at the program's peak. Untouched ballast held
    // from here on puts that peak inside the counted run instead of the one above.
    let prepared = Prepared {
        query,
        meta,
        chunk,
        _ballast: Vec::with_capacity(BALLAST),
    };
    Box::leak(Box::new(prepared))
}

/// One benchmark per directory under `benches/queries/`, and in it one case per
/// query and chunk. Gungraun names a case by its directory and its index: case
/// N is the Nth of `cases(...)`, chunks in the order `chunks.yaml` lists them,
/// and for each the queries sorted by name. A new directory is benchmarked once
/// it has a line here.
macro_rules! per_query {
    ($(#[$attr:meta])* fn $name:ident($case:ident: &'static Prepared) -> usize $body:block) => {
        $(#[$attr])*
        #[library_benchmark]
        #[benches::evm_selective(iter = cases("evm/selective"), setup = prepare)]
        #[benches::evm_fullscan(iter = cases("evm/fullscan"), setup = prepare)]
        #[benches::evm_rpc(iter = cases("evm/rpc"), setup = prepare)]
        #[benches::solana(iter = cases("solana"), setup = prepare)]
        fn $name($case: &'static Prepared) -> usize $body
    };
}

per_query! {
    fn json(case: &'static Prepared) -> usize {
        black_box(run_query(&case.query, &case.meta, &case.chunk, Format::Json))
    }
}

per_query! {
    fn arrow(case: &'static Prepared) -> usize {
        black_box(run_query(&case.query, &case.meta, &case.chunk, Format::Arrow))
    }
}

library_benchmark_group!(name = engine, benchmarks = [json, arrow]);

// No cache simulation: it doubles the run time, and the gate reads instructions.
main!(
    config = LibraryBenchmarkConfig::default()
        // Gungraun clears the environment, and the chunk store finds the cache through these.
        .pass_through_envs(["HOME", "SQD_CHUNK_CACHE"])
        .tool(
            Callgrind::with_args(["--cache-sim=no"])
                .format([EventKind::Ir])
                .soft_limits([(EventKind::Ir, 3.0)])
        )
        // Only allocations whose stack reaches the bench function are counted,
        // and Valgrind keeps 12 frames by default: a deeper one would drop out.
        .tool(
            Dhat::with_args(["--num-callers=500"])
                .format([
                    DhatMetric::TotalBytes,
                    DhatMetric::TotalBlocks,
                    DhatMetric::AtTGmaxBytes,
                ])
                .soft_limits([
                    (DhatMetric::TotalBytes, 1.0),
                    (DhatMetric::AtTGmaxBytes, 5.0),
                ])
        ),
    library_benchmark_groups = engine
);
