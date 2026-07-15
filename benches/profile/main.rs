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
use std::time::Instant;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

static SOLANA_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap());

static EVM_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap());

// Overridable via $EVM_CHUNK / $SOL_CHUNK to bench against a larger chunk.
static EVM_CHUNK: LazyLock<String> =
    LazyLock::new(|| std::env::var("EVM_CHUNK").unwrap_or_else(|_| "data/evm/chunk".into()));
static SOL_CHUNK: LazyLock<String> =
    LazyLock::new(|| std::env::var("SOL_CHUNK").unwrap_or_else(|_| "data/solana/chunk".into()));

fn run_query(
    query_json: &[u8],
    meta: &sqd_query_engine::metadata::DatasetDescription,
    chunk: &ParquetChunkReader,
    profile: bool,
) -> Vec<u8> {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk(&plan, meta, chunk, profile)
        .unwrap()
        .map(|blocks| blocks.into_json_lines())
        .unwrap_or_default()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let query_name = args.get(1).map(|s| s.as_str()).unwrap_or("evm/usdc_transfers");
    let iterations: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(1000);
    let profile_mode = args.iter().any(|a| a == "--profile");
    let size_mode = args.iter().any(|a| a == "--size");
    let use_legacy = args.iter().any(|a| a == "--legacy");
    let compare_mode = args.iter().any(|a| a == "--compare");

    let all_queries: Vec<(&str, &[u8], &sqd_query_engine::metadata::DatasetDescription, &str)> = vec![
        ("evm/usdc_transfers", EVM_USDC_TRANSFERS, &EVM_META, EVM_CHUNK.as_str()),
        ("evm/contract_calls+logs", EVM_CONTRACT_CALLS_WITH_LOGS, &EVM_META, EVM_CHUNK.as_str()),
        ("evm/usdc_traces+diffs", EVM_USDC_TRACES_AND_STATEDIFFS, &EVM_META, EVM_CHUNK.as_str()),
        ("evm/sparse", EVM_SPARSE_LOGS, &EVM_META, EVM_CHUNK.as_str()),
        ("evm/all_blocks", EVM_ALL_BLOCKS, &EVM_META, EVM_CHUNK.as_str()),
        ("evm/all_txs", EVM_ALL_TXS, &EVM_META, EVM_CHUNK.as_str()),
        ("evm/all_logs", EVM_ALL_LOGS, &EVM_META, EVM_CHUNK.as_str()),
        ("evm/all_traces", EVM_ALL_TRACES, &EVM_META, EVM_CHUNK.as_str()),
        ("evm/all_statediffs", EVM_ALL_STATEDIFFS, &EVM_META, EVM_CHUNK.as_str()),
        ("rpc/getBlockByNumber", EVM_GET_BLOCK_BY_NUMBER, &EVM_META, EVM_CHUNK.as_str()),
        ("rpc/getBlockByNumber:txHashes", EVM_GET_BLOCK_BY_NUMBER_TX_HASHES, &EVM_META, EVM_CHUNK.as_str()),
        ("rpc/getBlockReceipts", EVM_GET_BLOCK_RECEIPTS, &EVM_META, EVM_CHUNK.as_str()),
        ("rpc/trace_block", EVM_TRACE_BLOCK, &EVM_META, EVM_CHUNK.as_str()),
        ("rpc/getLogs:1blk", EVM_GET_LOGS_1BLK, &EVM_META, EVM_CHUNK.as_str()),
        ("rpc/getLogs:10blk", EVM_GET_LOGS_10BLK, &EVM_META, EVM_CHUNK.as_str()),
        ("rpc/getLogs:100blk", EVM_GET_LOGS_100BLK, &EVM_META, EVM_CHUNK.as_str()),
        ("sol/whirlpool_swap", SOL_WHIRLPOOL_SWAP, &SOLANA_META, SOL_CHUNK.as_str()),
        ("sol/hard", SOL_HARD, &SOLANA_META, SOL_CHUNK.as_str()),
        ("sol/instr+logs", SOL_INSTRUCTION_WITH_LOGS, &SOLANA_META, SOL_CHUNK.as_str()),
        ("sol/instr+balances", SOL_BALANCES_FROM_INSTRUCTION, &SOLANA_META, SOL_CHUNK.as_str()),
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

    if compare_mode {
        compare_engines(name, query_json, meta, chunk_dir);
        return;
    }

    if use_legacy {
        run_legacy(name, query_json, chunk_dir, iterations, size_mode);
        return;
    }

    let chunk = ParquetChunkReader::open(chunk_path).unwrap();

    // Warmup
    eprintln!("Warming up {} ...", name);
    for _ in 0..10 {
        let _ = run_query(query_json, meta, &chunk, false);
    }

    if size_mode {
        let result = run_query(query_json, meta, &chunk, false);
        eprintln!("Output size: {} bytes ({:.2} MB)", result.len(), result.len() as f64 / 1024.0 / 1024.0);
        return;
    }

    if profile_mode {
        eprintln!("\n=== Profiled run: {} ===", name);
        let _ = run_query(query_json, meta, &chunk, true);
        return;
    }

    eprintln!("Profiling {} x {} iterations ...", name, iterations);
    let start = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(run_query(query_json, meta, &chunk, false));
    }
    report(name, iterations, start.elapsed());
}

/// Profile the same query through the legacy engine (`--features legacy-query`).
#[cfg(feature = "legacy-query")]
fn run_legacy(name: &str, query_json: &[u8], chunk_dir: &str, iterations: usize, size_mode: bool) {
    let chunk = legacy::open_chunk(Path::new(chunk_dir));

    eprintln!("Warming up {} (legacy) ...", name);
    for _ in 0..10 {
        let _ = legacy::run_query(query_json, &chunk);
    }

    if size_mode {
        let result = legacy::run_query(query_json, &chunk);
        eprintln!("Output size: {} bytes ({:.2} MB)", result.len(), result.len() as f64 / 1024.0 / 1024.0);
        return;
    }

    eprintln!("Profiling {} (legacy) x {} iterations ...", name, iterations);
    let start = Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(legacy::run_query(query_json, &chunk));
    }
    report(&format!("{name} (legacy)"), iterations, start.elapsed());
}

#[cfg(not(feature = "legacy-query"))]
fn run_legacy(_name: &str, _query_json: &[u8], _chunk_dir: &str, _iterations: usize, _size_mode: bool) {
    eprintln!("--legacy requires building with --features legacy-query");
    std::process::exit(1);
}

/// Run the query through both engines on the same chunk and assert that the
/// decoded JSON is semantically identical (the legacy engine is the reference).
/// Mirrors `generate_fixtures`: legacy emits JSON-lines, new emits a JSON array.
#[cfg(feature = "legacy-query")]
fn compare_engines(
    name: &str,
    query_json: &[u8],
    meta: &sqd_query_engine::metadata::DatasetDescription,
    chunk_dir: &str,
) {
    let new_chunk = ParquetChunkReader::open(Path::new(chunk_dir)).unwrap();
    let new_bytes = run_query(query_json, meta, &new_chunk, false);
    let new_blocks: Vec<serde_json::Value> = String::from_utf8(new_bytes)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let new_val = serde_json::Value::Array(new_blocks);

    let leg_chunk = legacy::open_chunk(Path::new(chunk_dir));
    let leg_bytes = legacy::run_query(query_json, &leg_chunk);
    let leg_blocks: Vec<serde_json::Value> = String::from_utf8(leg_bytes)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let leg_val = serde_json::Value::Array(leg_blocks);

    let new_blocks = new_val.as_array().map(|a| a.len()).unwrap_or(0);
    let leg_n = leg_val.as_array().map(|a| a.len()).unwrap_or(0);
    eprintln!("=== Compare {name}: new={new_blocks} blocks, legacy={leg_n} blocks ===");

    if new_val == leg_val {
        eprintln!("MATCH ✓  (new engine output is identical to legacy)");
        return;
    }

    // Locate the first differing block to make the mismatch actionable.
    if let (Some(a), Some(b)) = (new_val.as_array(), leg_val.as_array()) {
        for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
            if x != y {
                let num = x.get("header").and_then(|h| h.get("number"));
                eprintln!("MISMATCH ✗  first differing block at index {i} (number={num:?})");
                let xs = serde_json::to_string(x).unwrap();
                let ys = serde_json::to_string(y).unwrap();
                eprintln!("  new   : {}", &xs[..xs.len().min(400)]);
                eprintln!("  legacy: {}", &ys[..ys.len().min(400)]);
                std::process::exit(1);
            }
        }
    }
    eprintln!("MISMATCH ✗  block counts differ (new={new_blocks}, legacy={leg_n})");
    std::process::exit(1);
}

#[cfg(not(feature = "legacy-query"))]
fn compare_engines(
    _name: &str,
    _query_json: &[u8],
    _meta: &sqd_query_engine::metadata::DatasetDescription,
    _chunk_dir: &str,
) {
    eprintln!("--compare requires building with --features legacy-query");
    std::process::exit(1);
}

fn report(name: &str, iterations: usize, elapsed: std::time::Duration) {
    eprintln!(
        "Done: {} x {} iterations in {:.2?} ({:.3} ms/iter, {:.1} rps)",
        name,
        iterations,
        elapsed,
        elapsed.as_secs_f64() * 1000.0 / iterations as f64,
        iterations as f64 / elapsed.as_secs_f64()
    );
}
