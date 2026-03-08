use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::{execute_plan_cached, execute_plan_profiled};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetTable;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn open_cache(chunk_dir: &Path) -> HashMap<String, ParquetTable> {
    let mut cache = HashMap::new();
    if let Ok(entries) = std::fs::read_dir(chunk_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                let name = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string();
                if let Ok(table) = ParquetTable::open(&path) {
                    cache.insert(name, table);
                }
            }
        }
    }
    cache
}

fn profile_query(label: &str, json: &[u8], meta_path: &str, chunk_path: &str) {
    let meta = load_dataset_description(Path::new(meta_path)).unwrap();
    let parsed = parse_query(json, &meta).unwrap();
    let plan = compile(&parsed, &meta).unwrap();
    let chunk = Path::new(chunk_path);
    let mut cache = open_cache(chunk);

    // Warmup
    let _ = execute_plan_cached(&plan, &meta, chunk, &mut cache, Vec::new()).unwrap();

    // Timed runs
    let n = 10;
    let mut times = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let _ = execute_plan_cached(&plan, &meta, chunk, &mut cache, Vec::new()).unwrap();
        times.push(t.elapsed());
    }
    times.sort();
    eprintln!(
        "=== {label} ({n} runs): median {:.2?}, min {:.2?}, max {:.2?} ===",
        times[n / 2],
        times[0],
        times[n - 1]
    );

    // Detailed breakdown (profiled uses its own cache internally)
    let _ = execute_plan_profiled(&plan, &meta, chunk, Vec::new()).unwrap();
    eprintln!();
}

fn main() {
    let solana_hard = br#"{
        "type": "solana",
        "fromBlock": 0,
        "fields": {
            "block": {"number": true, "parentNumber": true, "parentHash": true, "height": true, "timestamp": true},
            "transaction": {"signatures": true, "err": true, "feePayer": true},
            "instruction": {"programId": true, "accounts": true, "data": true, "isCommitted": true},
            "log": {"instructionAddress": true, "programId": true, "kind": true, "message": true},
            "balance": {"pre": true, "post": true},
            "tokenBalance": {"preMint": true, "postMint": true, "preDecimals": true, "postDecimals": true, "preOwner": true, "postOwner": true, "preAmount": true, "postAmount": true},
            "reward": {"lamports": true, "postBalance": true, "rewardType": true, "commission": true}
        },
        "instructions": [{"programId": ["LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"], "d8": ["0xf8c69e91e17587c8","0xb59d59438fb63448","0x1c8cee63e7a21595","0x0703967f94283dc8","0x2905eeaf64e106cd","0x5e9b6797465fdca5","0xa1c26754ab47fa9a","0x5055d14818ceb16c","0x0a333d2370691855","0x1a526698f04a691a","0x2d9aedd2dd0fa65c"], "isCommitted": true, "transaction": true, "transactionTokenBalances": true, "innerInstructions": true}]
    }"#;

    let whirlpool_swap = br#"{
        "type": "solana",
        "fromBlock": 0,
        "fields": {
            "block": {"number": true, "hash": true, "parentNumber": true},
            "transaction": {"signatures": true, "err": true},
            "instruction": {"programId": true, "accounts": true, "data": true}
        },
        "instructions": [{"programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"], "d8": ["0xf8c69e91e17587c8"], "transaction": true, "isCommitted": true}]
    }"#;

    profile_query(
        "solana_hard/200",
        solana_hard,
        "metadata/solana.yaml",
        "data/solana/200",
    );
    profile_query(
        "solana_hard/large",
        solana_hard,
        "metadata/solana.yaml",
        "data/solana/large",
    );
    profile_query(
        "whirlpool_swap/200",
        whirlpool_swap,
        "metadata/solana.yaml",
        "data/solana/200",
    );
    profile_query(
        "whirlpool_swap/large",
        whirlpool_swap,
        "metadata/solana.yaml",
        "data/solana/large",
    );
}
