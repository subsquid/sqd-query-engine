#[path = "../queries.rs"]
mod queries;

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

/// Full pipeline: parse → compile → execute
fn run_query(
    query_json: &[u8],
    meta: &sqd_query_engine::metadata::DatasetDescription,
    chunk: &ParquetChunkReader,
) -> Vec<u8> {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk(&plan, meta, chunk, Vec::new(), false).unwrap()
}

// ---------------------------------------------------------------------------
// EVM benchmarks
// ---------------------------------------------------------------------------

#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod evm {
    use super::*;

    #[divan::bench]
    fn usdc_transfers(bench: divan::Bencher) {
        let chunk = ParquetChunkReader::open(Path::new("data/evm/chunk")).unwrap();
        bench.bench_local(|| run_query(EVM_USDC_TRANSFERS, &EVM_META, &chunk));
    }

    #[divan::bench]
    fn contract_calls_with_logs(bench: divan::Bencher) {
        let chunk = ParquetChunkReader::open(Path::new("data/evm/chunk")).unwrap();
        bench.bench_local(|| run_query(EVM_CONTRACT_CALLS_WITH_LOGS, &EVM_META, &chunk));
    }

    #[divan::bench]
    fn usdc_traces_and_statediffs(bench: divan::Bencher) {
        let chunk = ParquetChunkReader::open(Path::new("data/evm/chunk")).unwrap();
        bench.bench_local(|| run_query(EVM_USDC_TRACES_AND_STATEDIFFS, &EVM_META, &chunk));
    }

    #[divan::bench]
    fn all_blocks(bench: divan::Bencher) {
        let chunk = ParquetChunkReader::open(Path::new("data/evm/chunk")).unwrap();
        bench.bench_local(|| run_query(EVM_ALL_BLOCKS, &EVM_META, &chunk));
    }
}

// ---------------------------------------------------------------------------
// Solana benchmarks
// ---------------------------------------------------------------------------

#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod solana {
    use super::*;

    #[divan::bench]
    fn whirlpool_swap(bench: divan::Bencher) {
        let chunk = ParquetChunkReader::open(Path::new("data/solana/chunk")).unwrap();
        bench.bench_local(|| run_query(SOL_WHIRLPOOL_SWAP, &SOLANA_META, &chunk));
    }

    #[divan::bench]
    fn hard(bench: divan::Bencher) {
        let chunk = ParquetChunkReader::open(Path::new("data/solana/chunk")).unwrap();
        bench.bench_local(|| run_query(SOL_HARD, &SOLANA_META, &chunk));
    }

    #[divan::bench]
    fn instruction_with_logs(bench: divan::Bencher) {
        let chunk = ParquetChunkReader::open(Path::new("data/solana/chunk")).unwrap();
        bench.bench_local(|| run_query(SOL_INSTRUCTION_WITH_LOGS, &SOLANA_META, &chunk));
    }

    #[divan::bench]
    fn balances_from_instruction(bench: divan::Bencher) {
        let chunk = ParquetChunkReader::open(Path::new("data/solana/chunk")).unwrap();
        bench.bench_local(|| run_query(SOL_BALANCES_FROM_INSTRUCTION, &SOLANA_META, &chunk));
    }

    #[divan::bench]
    fn all_blocks(bench: divan::Bencher) {
        let chunk = ParquetChunkReader::open(Path::new("data/solana/chunk")).unwrap();
        bench.bench_local(|| run_query(SOL_ALL_BLOCKS, &SOLANA_META, &chunk));
    }
}
