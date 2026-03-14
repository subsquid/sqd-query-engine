#[path = "../queries.rs"]
mod queries;

use queries::*;
use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::path::Path;
use std::sync::LazyLock;
use std::time::Instant;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

static SOLANA_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap());

static EVM_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap());

fn run_query(
    query_json: &[u8],
    meta: &sqd_query_engine::metadata::DatasetDescription,
    chunk: &ParquetChunkReader,
    buf: Vec<u8>,
    profile: bool,
) -> Vec<u8> {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk(&plan, meta, chunk, buf, profile).unwrap()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let query_name = args.get(1).map(|s| s.as_str()).unwrap_or("evm/usdc_transfers");
    let iterations: usize = args
        .get(2)
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let profile_mode = args.iter().any(|a| a == "--profile");
    let size_mode = args.iter().any(|a| a == "--size");

    let all_queries: Vec<(&str, &[u8], &sqd_query_engine::metadata::DatasetDescription, &str)> = vec![
        ("evm/usdc_transfers", EVM_USDC_TRANSFERS, &EVM_META, "data/evm/chunk"),
        ("evm/contract_calls+logs", EVM_CONTRACT_CALLS_WITH_LOGS, &EVM_META, "data/evm/chunk"),
        ("evm/usdc_traces+diffs", EVM_USDC_TRACES_AND_STATEDIFFS, &EVM_META, "data/evm/chunk"),
        ("evm/all_blocks", EVM_ALL_BLOCKS, &EVM_META, "data/evm/chunk"),
        ("sol/whirlpool_swap", SOL_WHIRLPOOL_SWAP, &SOLANA_META, "data/solana/chunk"),
        ("sol/hard", SOL_HARD, &SOLANA_META, "data/solana/chunk"),
        ("sol/instr+logs", SOL_INSTRUCTION_WITH_LOGS, &SOLANA_META, "data/solana/chunk"),
        ("sol/instr+balances", SOL_BALANCES_FROM_INSTRUCTION, &SOLANA_META, "data/solana/chunk"),
        ("sol/all_blocks", SOL_ALL_BLOCKS, &SOLANA_META, "data/solana/chunk"),
    ];

    let (name, query_json, meta, chunk_dir) = all_queries
        .iter()
        .find(|(n, _, _, _)| *n == query_name)
        .copied()
        .unwrap_or_else(|| {
            eprintln!("Unknown query: {}", query_name);
            eprintln!("Available: {}", all_queries.iter().map(|(n, _, _, _)| *n).collect::<Vec<_>>().join(", "));
            std::process::exit(1);
        });

    let chunk_path = Path::new(chunk_dir);
    if !chunk_path.exists() {
        eprintln!("Chunk not found: {}", chunk_dir);
        std::process::exit(1);
    }

    let chunk = ParquetChunkReader::open(chunk_path).unwrap();

    // Warmup
    eprintln!("Warming up {} ...", name);
    for _ in 0..10 {
        let _ = run_query(query_json, meta, &chunk, Vec::new(), false);
    }

    if size_mode {
        let result = run_query(query_json, meta, &chunk, Vec::new(), false);
        eprintln!("Output size: {} bytes ({:.2} MB)", result.len(), result.len() as f64 / 1024.0 / 1024.0);
        return;
    }

    if profile_mode {
        eprintln!("\n=== Profiled run: {} ===", name);
        let _ = run_query(query_json, meta, &chunk, Vec::new(), true);
        return;
    }

    eprintln!("Profiling {} x {} iterations ...", name, iterations);
    let start = Instant::now();
    let mut buf = Vec::new();
    for _ in 0..iterations {
        buf = run_query(query_json, meta, &chunk, buf, false);
        buf.clear();
    }
    let elapsed = start.elapsed();
    eprintln!(
        "Done: {} iterations in {:.2?} ({:.2} ms/iter, {:.1} rps)",
        iterations,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64,
        iterations as f64 / elapsed.as_secs_f64()
    );
}
