use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::execute_plan_cached;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetTable;
use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    divan::main();
}

// ---------------------------------------------------------------------------
// Query definitions
// ---------------------------------------------------------------------------

struct BenchQuery {
    json: &'static [u8],
}

static WHIRLPOOL_SWAP: BenchQuery = BenchQuery {
    json: br#"{
        "type": "solana",
        "fromBlock": 0,
        "fields": {
            "block": {
                "number": true,
                "hash": true,
                "parentNumber": true
            },
            "transaction": {
                "signatures": true,
                "err": true
            },
            "instruction": {
                "programId": true,
                "accounts": true,
                "data": true
            }
        },
        "instructions": [{
            "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
            "d8": ["0xf8c69e91e17587c8"],
            "transaction": true,
            "isCommitted": true
        }]
    }"#,
};

static SOLANA_HARD: BenchQuery = BenchQuery {
    json: br#"{
        "type": "solana",
        "fromBlock": 0,
        "fields": {
            "block": {
                "number": true,
                "parentNumber": true,
                "parentHash": true,
                "height": true,
                "timestamp": true
            },
            "transaction": {
                "signatures": true,
                "err": true,
                "feePayer": true
            },
            "instruction": {
                "programId": true,
                "accounts": true,
                "data": true,
                "isCommitted": true
            },
            "log": {
                "instructionAddress": true,
                "programId": true,
                "kind": true,
                "message": true
            },
            "balance": {
                "pre": true,
                "post": true
            },
            "tokenBalance": {
                "preMint": true,
                "postMint": true,
                "preDecimals": true,
                "postDecimals": true,
                "preOwner": true,
                "postOwner": true,
                "preAmount": true,
                "postAmount": true
            },
            "reward": {
                "lamports": true,
                "postBalance": true,
                "rewardType": true,
                "commission": true
            }
        },
        "instructions": [{
            "programId": ["LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"],
            "d8": [
                "0xf8c69e91e17587c8",
                "0xb59d59438fb63448",
                "0x1c8cee63e7a21595",
                "0x0703967f94283dc8",
                "0x2905eeaf64e106cd",
                "0x5e9b6797465fdca5",
                "0xa1c26754ab47fa9a",
                "0x5055d14818ceb16c",
                "0x0a333d2370691855",
                "0x1a526698f04a691a",
                "0x2d9aedd2dd0fa65c"
            ],
            "isCommitted": true,
            "transaction": true,
            "transactionTokenBalances": true,
            "innerInstructions": true
        }]
    }"#,
};

static EVM_TRANSFERS: BenchQuery = BenchQuery {
    json: br#"{
        "type": "evm",
        "fromBlock": 0,
        "fields": {
            "block": {
                "number": true,
                "hash": true,
                "timestamp": true
            },
            "log": {
                "address": true,
                "topics": true,
                "data": true,
                "logIndex": true,
                "transactionIndex": true
            },
            "transaction": {
                "hash": true,
                "from": true,
                "to": true,
                "value": true
            }
        },
        "logs": [{
            "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
            "transaction": true
        }]
    }"#,
};

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

static SOLANA_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap());

static EVM_META: LazyLock<sqd_query_engine::metadata::DatasetDescription> =
    LazyLock::new(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap());

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn make_plan(
    query: &BenchQuery,
    meta: &sqd_query_engine::metadata::DatasetDescription,
) -> sqd_query_engine::query::Plan {
    let parsed = parse_query(query.json, meta).unwrap();
    compile(&parsed, meta).unwrap()
}

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

fn run_plan(
    plan: &sqd_query_engine::query::Plan,
    meta: &sqd_query_engine::metadata::DatasetDescription,
    chunk_dir: &Path,
    cache: &mut HashMap<String, ParquetTable>,
) -> Vec<u8> {
    execute_plan_cached(plan, meta, chunk_dir, cache, Vec::new()).unwrap()
}

// ---------------------------------------------------------------------------
// Whirlpool swap benchmarks
// ---------------------------------------------------------------------------

#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod whirlpool_swap {
    use super::*;

    #[divan::bench]
    fn chunk_200(bench: divan::Bencher) {
        let plan = make_plan(&WHIRLPOOL_SWAP, &SOLANA_META);
        let chunk = Path::new("data/solana/200");
        let mut cache = open_cache(chunk);
        bench.bench_local(|| run_plan(&plan, &SOLANA_META, chunk, &mut cache));
    }

    #[divan::bench]
    fn chunk_400(bench: divan::Bencher) {
        let plan = make_plan(&WHIRLPOOL_SWAP, &SOLANA_META);
        let chunk = Path::new("data/solana/400");
        let mut cache = open_cache(chunk);
        bench.bench_local(|| run_plan(&plan, &SOLANA_META, chunk, &mut cache));
    }

    #[divan::bench]
    fn large(bench: divan::Bencher) {
        let plan = make_plan(&WHIRLPOOL_SWAP, &SOLANA_META);
        let chunk = Path::new("data/solana/large");
        let mut cache = open_cache(chunk);
        bench.bench_local(|| run_plan(&plan, &SOLANA_META, chunk, &mut cache));
    }
}

// ---------------------------------------------------------------------------
// Solana hard (complex multi-table query)
// ---------------------------------------------------------------------------

#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod solana_hard {
    use super::*;

    #[divan::bench]
    fn chunk_200(bench: divan::Bencher) {
        let plan = make_plan(&SOLANA_HARD, &SOLANA_META);
        let chunk = Path::new("data/solana/200");
        let mut cache = open_cache(chunk);
        bench.bench_local(|| run_plan(&plan, &SOLANA_META, chunk, &mut cache));
    }

    #[divan::bench]
    fn chunk_400(bench: divan::Bencher) {
        let plan = make_plan(&SOLANA_HARD, &SOLANA_META);
        let chunk = Path::new("data/solana/400");
        let mut cache = open_cache(chunk);
        bench.bench_local(|| run_plan(&plan, &SOLANA_META, chunk, &mut cache));
    }

    #[divan::bench]
    fn large(bench: divan::Bencher) {
        let plan = make_plan(&SOLANA_HARD, &SOLANA_META);
        let chunk = Path::new("data/solana/large");
        let mut cache = open_cache(chunk);
        bench.bench_local(|| run_plan(&plan, &SOLANA_META, chunk, &mut cache));
    }
}

// ---------------------------------------------------------------------------
// EVM Transfer logs benchmark
// ---------------------------------------------------------------------------

#[divan::bench_group(sample_size = 5, sample_count = 20)]
mod evm_transfers {
    use super::*;

    #[divan::bench]
    fn large(bench: divan::Bencher) {
        let plan = make_plan(&EVM_TRANSFERS, &EVM_META);
        let chunk = Path::new("data/evm/large");
        let mut cache = open_cache(chunk);
        bench.bench_local(|| run_plan(&plan, &EVM_META, chunk, &mut cache));
    }
}
