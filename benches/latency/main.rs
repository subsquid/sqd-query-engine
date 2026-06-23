#[path = "../queries.rs"]
mod queries;
#[cfg(feature = "legacy-query")]
#[path = "../legacy.rs"]
mod legacy;

use queries::*;
use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::path::Path;
use std::sync::LazyLock;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    divan::main();
}

static SOLANA_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap());

static EVM_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap());

/// Full pipeline: parse → compile → execute (new engine).
fn run_query(
    query_json: &[u8],
    meta: &sqd_query_engine::metadata::DatasetDescription,
    chunk: &ParquetChunkReader,
) -> Vec<u8> {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk(&plan, meta, chunk, Vec::new(), false).unwrap()
}

// EVM benches run across the chunk matrix: divan nests one sub-bench per
// available chunk label ("small", "big") under each query name.
macro_rules! matrix_bench {
    ($name:ident, $query:expr) => {
        #[divan::bench(args = evm_chunk_labels())]
        fn $name(bench: divan::Bencher, chunk_label: &str) {
            let path = evm_chunk_path(chunk_label);
            let chunk = ParquetChunkReader::open(Path::new(&path)).unwrap();
            bench.bench_local(|| run_query($query, &EVM_META, &chunk));
        }
    };
}

/// Single-chunk new-engine bench (RPC on small chunk, Solana on its chunk).
macro_rules! single_bench {
    ($name:ident, $query:expr, $meta:expr, $path:expr) => {
        #[divan::bench]
        fn $name(bench: divan::Bencher) {
            let path = $path;
            let chunk = ParquetChunkReader::open(Path::new(&path)).unwrap();
            bench.bench_local(|| run_query($query, $meta, &chunk));
        }
    };
}

// ---------------------------------------------------------------------------
// New engine
// ---------------------------------------------------------------------------

/// Selective indexer-style queries, across the chunk matrix.
#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod evm {
    use super::*;
    matrix_bench!(usdc_transfers, EVM_USDC_TRANSFERS);
    matrix_bench!(contract_calls_with_logs, EVM_CONTRACT_CALLS_WITH_LOGS);
    matrix_bench!(usdc_traces_and_statediffs, EVM_USDC_TRACES_AND_STATEDIFFS);
    matrix_bench!(sparse, EVM_SPARSE_LOGS);
}

/// Unfiltered per-table full scans (exercise the two-phase budget path), matrix.
#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod evm_fullscan {
    use super::*;
    matrix_bench!(all_blocks, EVM_ALL_BLOCKS);
    matrix_bench!(all_txs, EVM_ALL_TXS);
    matrix_bench!(all_logs, EVM_ALL_LOGS);
    matrix_bench!(all_traces, EVM_ALL_TRACES);
    matrix_bench!(all_statediffs, EVM_ALL_STATEDIFFS);
}

/// RPC-compatible queries (Ethereum JSON-RPC equivalents). Pinned to a block in
/// the small chunk, so they run on the small chunk only.
#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod rpc {
    use super::*;
    single_bench!(get_block_by_number, EVM_GET_BLOCK_BY_NUMBER, &EVM_META, evm_chunk_path("small"));
    single_bench!(get_block_by_number_tx_hashes, EVM_GET_BLOCK_BY_NUMBER_TX_HASHES, &EVM_META, evm_chunk_path("small"));
    single_bench!(get_block_receipts, EVM_GET_BLOCK_RECEIPTS, &EVM_META, evm_chunk_path("small"));
    single_bench!(trace_block, EVM_TRACE_BLOCK, &EVM_META, evm_chunk_path("small"));
    single_bench!(get_logs_1blk, EVM_GET_LOGS_1BLK, &EVM_META, evm_chunk_path("small"));
    single_bench!(get_logs_10blk, EVM_GET_LOGS_10BLK, &EVM_META, evm_chunk_path("small"));
    single_bench!(get_logs_100blk, EVM_GET_LOGS_100BLK, &EVM_META, evm_chunk_path("small"));
}

#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod solana {
    use super::*;
    single_bench!(whirlpool_swap, SOL_WHIRLPOOL_SWAP, &SOLANA_META, sol_chunk_path());
    single_bench!(hard, SOL_HARD, &SOLANA_META, sol_chunk_path());
    single_bench!(instruction_with_logs, SOL_INSTRUCTION_WITH_LOGS, &SOLANA_META, sol_chunk_path());
    single_bench!(balances_from_instruction, SOL_BALANCES_FROM_INSTRUCTION, &SOLANA_META, sol_chunk_path());
}

// ---------------------------------------------------------------------------
// Legacy engine (`--features legacy-query`) — same queries, same matrix.
// Each group mirrors its new-engine counterpart for a direct A/B read.
// ---------------------------------------------------------------------------

#[cfg(feature = "legacy-query")]
macro_rules! matrix_legacy_bench {
    ($name:ident, $query:expr) => {
        #[divan::bench(args = evm_chunk_labels())]
        fn $name(bench: divan::Bencher, chunk_label: &str) {
            let path = evm_chunk_path(chunk_label);
            let chunk = legacy::open_chunk(Path::new(&path));
            bench.bench_local(|| legacy::run_query($query, &chunk));
        }
    };
}

#[cfg(feature = "legacy-query")]
macro_rules! single_legacy_bench {
    ($name:ident, $query:expr, $path:expr) => {
        #[divan::bench]
        fn $name(bench: divan::Bencher) {
            let path = $path;
            let chunk = legacy::open_chunk(Path::new(&path));
            bench.bench_local(|| legacy::run_query($query, &chunk));
        }
    };
}

#[cfg(feature = "legacy-query")]
#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod evm_legacy {
    use super::*;
    matrix_legacy_bench!(usdc_transfers, EVM_USDC_TRANSFERS);
    matrix_legacy_bench!(contract_calls_with_logs, EVM_CONTRACT_CALLS_WITH_LOGS);
    matrix_legacy_bench!(usdc_traces_and_statediffs, EVM_USDC_TRACES_AND_STATEDIFFS);
    matrix_legacy_bench!(sparse, EVM_SPARSE_LOGS);
}

#[cfg(feature = "legacy-query")]
#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod evm_fullscan_legacy {
    use super::*;
    matrix_legacy_bench!(all_blocks, EVM_ALL_BLOCKS);
    matrix_legacy_bench!(all_txs, EVM_ALL_TXS);
    matrix_legacy_bench!(all_logs, EVM_ALL_LOGS);
    matrix_legacy_bench!(all_traces, EVM_ALL_TRACES);
    matrix_legacy_bench!(all_statediffs, EVM_ALL_STATEDIFFS);
}

#[cfg(feature = "legacy-query")]
#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod rpc_legacy {
    use super::*;
    single_legacy_bench!(get_block_by_number, EVM_GET_BLOCK_BY_NUMBER, evm_chunk_path("small"));
    single_legacy_bench!(get_block_by_number_tx_hashes, EVM_GET_BLOCK_BY_NUMBER_TX_HASHES, evm_chunk_path("small"));
    single_legacy_bench!(get_block_receipts, EVM_GET_BLOCK_RECEIPTS, evm_chunk_path("small"));
    single_legacy_bench!(trace_block, EVM_TRACE_BLOCK, evm_chunk_path("small"));
    single_legacy_bench!(get_logs_1blk, EVM_GET_LOGS_1BLK, evm_chunk_path("small"));
    single_legacy_bench!(get_logs_10blk, EVM_GET_LOGS_10BLK, evm_chunk_path("small"));
    single_legacy_bench!(get_logs_100blk, EVM_GET_LOGS_100BLK, evm_chunk_path("small"));
}

#[cfg(feature = "legacy-query")]
#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod solana_legacy {
    use super::*;
    single_legacy_bench!(whirlpool_swap, SOL_WHIRLPOOL_SWAP, sol_chunk_path());
    single_legacy_bench!(hard, SOL_HARD, sol_chunk_path());
    single_legacy_bench!(instruction_with_logs, SOL_INSTRUCTION_WITH_LOGS, sol_chunk_path());
    single_legacy_bench!(balances_from_instruction, SOL_BALANCES_FROM_INSTRUCTION, sol_chunk_path());
}
