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
//! pool thread took over would go uncounted. Wall time, contention and memory
//! under concurrency are for the `throughput` and `memory` benches.

#[path = "../queries.rs"]
mod queries;
#[path = "../run.rs"]
mod run;

use gungraun::prelude::*;
use gungraun::{Callgrind, Dhat, DhatMetric, EventKind};
use queries::*;
use run::{run_query, Format};
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::scan::ParquetChunkReader;
use std::hint::black_box;
use std::path::Path;

/// More than any query's peak heap.
const BALLAST: usize = 4 << 30;

struct Case {
    query: &'static [u8],
    meta: DatasetDescription,
    chunk: ParquetChunkReader,
    _ballast: Vec<u8>,
}

/// The query of that name in the bench query lists.
fn find(name: &str) -> &'static [u8] {
    [EVM_QUERIES, EVM_FULLSCAN_QUERIES, EVM_RPC_QUERIES, SOL_QUERIES]
        .into_iter()
        .flatten()
        .find(|(query, _)| *query == name)
        .map(|(_, json)| *json)
        .unwrap_or_else(|| panic!("no bench query is named {name}"))
}

/// The case, opened and run once in each format, outside the counted call: the
/// first run maps the chunk's pages and fills lazy state. Leaked, so that
/// closing the chunk is not counted either.
fn prepare(metadata: &str, chunk: &str, name: &str) -> &'static Case {
    rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .use_current_thread()
        .build_global()
        .unwrap();

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = root.join("data").join(chunk);
    assert!(dir.is_dir(), "{name}: no chunk at {}", dir.display());

    let query = find(name);
    let meta = load_dataset_description(&root.join("metadata").join(metadata)).unwrap();
    let chunk = ParquetChunkReader::open(&dir).unwrap();
    for format in [Format::Json, Format::Arrow] {
        run_query(query, &meta, &chunk, format);
    }

    // DHAT gives the bytes live at the program's peak. Untouched ballast held
    // from here on puts that peak inside the counted run instead of the one above.
    let case = Case {
        query,
        meta,
        chunk,
        _ballast: Vec::with_capacity(BALLAST),
    };
    Box::leak(Box::new(case))
}

fn evm_small(name: &str) -> &'static Case {
    prepare("evm.yaml", "evm/chunk", name)
}

fn evm_big(name: &str) -> &'static Case {
    prepare("evm.yaml", "evm/big", name)
}

fn solana(name: &str) -> &'static Case {
    prepare("solana.yaml", "solana/chunk", name)
}

/// One benchmark per bench query and chunk. Gungraun labels a case with the
/// source text of its argument, so the names are spelled out here, and a query
/// added to `queries.rs` is benchmarked once it is added here too.
macro_rules! per_query {
    ($(#[$attr:meta])* fn $name:ident($case:ident: &'static Case) -> usize $body:block) => {
        $(#[$attr])*
        #[library_benchmark]
        #[benches::small(
            args = [
                "evm/usdc_transfers",
                "evm/contract_calls+logs",
                "evm/usdc_traces+diffs",
                "evm/sparse",
                "evm/all_relations",
                "evm/logs+transaction",
                "evm/all_blocks",
                "evm/all_txs",
                "evm/all_logs",
                "evm/all_traces",
                "evm/all_statediffs",
                "rpc/getBlockByNumber",
                "rpc/getBlockByNumber:txHashes",
                "rpc/getBlockReceipts",
                "rpc/trace_block",
                "rpc/getLogs:1blk",
                "rpc/getLogs:10blk",
                "rpc/getLogs:100blk",
            ],
            setup = evm_small
        )]
        #[benches::big(
            args = [
                "evm/usdc_transfers",
                "evm/contract_calls+logs",
                "evm/usdc_traces+diffs",
                "evm/sparse",
                "evm/all_relations",
                "evm/logs+transaction",
                "evm/all_blocks",
                "evm/all_txs",
                "evm/all_logs",
                "evm/all_traces",
                "evm/all_statediffs",
            ],
            setup = evm_big
        )]
        #[benches::solana(
            args = [
                "sol/whirlpool_swap",
                "sol/hard",
                "sol/instr+logs",
                "sol/instr+balances",
            ],
            setup = solana
        )]
        fn $name($case: &'static Case) -> usize $body
    };
}

per_query! {
    fn json(case: &'static Case) -> usize {
        black_box(run_query(case.query, &case.meta, &case.chunk, Format::Json))
    }
}

per_query! {
    fn arrow(case: &'static Case) -> usize {
        black_box(run_query(case.query, &case.meta, &case.chunk, Format::Arrow))
    }
}

library_benchmark_group!(name = engine, benchmarks = [json, arrow]);

// No cache simulation: it doubles the run time, and the gate reads instructions.
main!(
    config = LibraryBenchmarkConfig::default()
        .tool(
            Callgrind::with_args(["--cache-sim=no"])
                .format([EventKind::Ir])
                .soft_limits([(EventKind::Ir, 3.0)])
        )
        .tool(
            Dhat::default()
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
