//! Instructions and simulated branch misses of each membership case (issue #82),
//! counted by Callgrind under Gungraun. The thread benchmark found the bloom's
//! cost in the unpredictable branch, so branches are simulated here, which the
//! `instructions` bench leaves off.
//!
//!   SQD_MEMBERSHIP_DIR=sets cargo bench --features membership-filter \
//!     --bench membership -- --nocapture
//!
//! Case N of a group is the Nth of `cases(...)`; `--nocapture` prints each
//! case's name from its setup.

#[path = "cases.rs"]
mod cases;
#[path = "../run.rs"]
mod run;

use cases::{Case, LOGS, TOKEN_BALANCES, TRANSACTIONS};
use gungraun::prelude::*;
use gungraun::{Callgrind, EventKind};
use run::{run_query, Format};
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::scan::ParquetChunkReader;
use std::hint::black_box;

struct Prepared {
    query: Vec<u8>,
    meta: DatasetDescription,
    chunk: ParquetChunkReader,
}

fn prepare(case: Case) -> &'static Prepared {
    rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .use_current_thread()
        .build_global()
        .unwrap();

    eprintln!("case {}", case.name);
    let meta = case.target.catalog();
    let chunk = ParquetChunkReader::open(&case.target.chunk_dir()).unwrap();
    let query = case.query(&case.target.set());
    run_query(&query, &meta, &chunk, Format::Json);

    Box::leak(Box::new(Prepared { query, meta, chunk }))
}

#[library_benchmark]
#[benches::logs(iter = cases::cases(LOGS), setup = prepare)]
#[benches::transactions(iter = cases::cases(TRANSACTIONS), setup = prepare)]
#[benches::token_balances(iter = cases::cases(TOKEN_BALANCES), setup = prepare)]
fn json(case: &'static Prepared) -> usize {
    black_box(run_query(
        &case.query,
        &case.meta,
        &case.chunk,
        Format::Json,
    ))
}

library_benchmark_group!(name = membership, benchmarks = [json]);

main!(
    config = LibraryBenchmarkConfig::default()
        .pass_through_envs([
            "HOME",
            "SQD_CHUNK_CACHE",
            "SQD_MEMBERSHIP_DIR",
            "SQD_MEMBERSHIP_CHUNK",
            "SQD_MEMBERSHIP_SOLANA_CHUNK",
        ])
        .tool(
            Callgrind::with_args(["--cache-sim=no", "--branch-sim=yes"]).format([
                EventKind::Ir,
                EventKind::Bc,
                EventKind::Bcm
            ])
        ),
    library_benchmark_groups = membership
);
