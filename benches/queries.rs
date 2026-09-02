// Shared query definitions for benchmarks.
// Included via `#[path]` from bench binaries; each bench uses a subset.
#![allow(dead_code)]

// EVM queries

/// Selective log filter: USDC Transfer events
pub static EVM_USDC_TRANSFERS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true, "hash": true, "timestamp": true },
        "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true }
    },
    "logs": [{
        "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
        "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
    }]
}"#;

// ---------------------------------------------------------------------------
// Full-scan family: one unfiltered `[{}]` scan per wide table. These exercise
// the response-budget two-phase scan path (narrow weight pre-scan → wide scan
// capped at the 20 MB cutoff). On a small chunk they fit under the budget and
// emit everything; on a large chunk they cap, so the matrix shows the optimizer
// engaging.
// ---------------------------------------------------------------------------

/// Every block header in the range (no item tables).
pub static EVM_ALL_BLOCKS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "includeAllBlocks": true,
    "fields": {
        "block": { "number": true, "hash": true, "parentHash": true, "timestamp": true, "miner": true, "gasUsed": true, "gasLimit": true }
    }
}"#;

/// Every transaction in the chunk.
pub static EVM_ALL_TXS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true },
        "transaction": { "hash": true, "from": true, "to": true, "value": true, "input": true, "nonce": true, "gas": true, "gasPrice": true, "transactionIndex": true }
    },
    "transactions": [{}]
}"#;

/// Every log in the chunk.
pub static EVM_ALL_LOGS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true },
        "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true }
    },
    "logs": [{}]
}"#;

/// Every trace in the chunk.
pub static EVM_ALL_TRACES: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true },
        "trace": { "type": true, "transactionIndex": true, "traceAddress": true, "callFrom": true, "callTo": true, "callInput": true, "callValue": true }
    },
    "traces": [{}]
}"#;

/// Isolated state-diff path: every state diff in the chunk (no relations, no
/// traces). Used to profile the state-diff output assembly — the allocation /
/// latency hot spot in `usdc_traces+diffs`.
pub static EVM_ALL_STATEDIFFS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true },
        "stateDiff": { "kind": true, "next": true, "prev": true, "transactionIndex": true, "address": true, "key": true }
    },
    "stateDiffs": [{}]
}"#;

/// Empty-result floor: all logs of a contract with zero activity in the chunk.
/// Address-only filter (no topic0) means the leading sort key is unconstrained,
/// so row-group pruning by address can't trivially skip everything — this
/// measures how cheaply the engine rejects and returns an empty result.
pub static EVM_SPARSE_LOGS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true, "hash": true, "timestamp": true },
        "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true }
    },
    "logs": [{
        "address": ["0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"]
    }]
}"#;

/// Transaction filter with log relation (all events from USDT calls)
pub static EVM_CONTRACT_CALLS_WITH_LOGS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true, "hash": true, "timestamp": true },
        "transaction": { "from": true, "to": true, "hash": true, "sighash": true, "transactionIndex": true },
        "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true }
    },
    "transactions": [{
        "to": ["0xdac17f958d2ee523a2206206994597c13d831ec7"],
        "logs": true
    }]
}"#;

/// Multi-table: USDC traces + state diffs with transaction relations
pub static EVM_USDC_TRACES_AND_STATEDIFFS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true, "hash": true, "timestamp": true },
        "transaction": { "from": true, "to": true, "hash": true, "transactionIndex": true },
        "trace": { "type": true, "callFrom": true, "callTo": true, "callSighash": true, "transactionIndex": true, "traceAddress": true },
        "stateDiff": { "kind": true, "next": true, "prev": true, "transactionIndex": true, "address": true, "key": true }
    },
    "traces": [{
        "type": ["call"],
        "callTo": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
        "transaction": true
    }],
    "stateDiffs": [{
        "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
        "transaction": true
    }]
}"#;

// ---------------------------------------------------------------------------
// RPC-compatible EVM queries
//
// These reproduce the three workhorse Ethereum JSON-RPC methods as portal
// queries, to measure how the engine serves traffic shaped like a node's RPC
// endpoint rather than an indexer's bulk export.
//
// Note on data layout: EVM tables are sorted by filter columns
// (logs: topic0 → address → block_number; txs: sighash → to → block_number),
// NOT by block_number. So `getLogs` (filter-aligned) prunes row groups well,
// while the single-block `getBlockByNumber` / `getBlockReceipts` must scan
// every row group to collect one block's rows.
//
// Block 24550620 is a representative mid-chunk block (median tx count: 307
// txs / 695 logs) from the EVM bench chunk (range 24550597-24550820).
// ---------------------------------------------------------------------------

/// `eth_getBlockByNumber(0x176fa7c, true)` — full block header + all
/// transaction objects for a single block.
pub static EVM_GET_BLOCK_BY_NUMBER: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 24550620,
    "toBlock": 24550620,
    "includeAllBlocks": true,
    "fields": {
        "block": {
            "number": true, "hash": true, "parentHash": true, "timestamp": true,
            "gasUsed": true, "gasLimit": true, "baseFeePerGas": true, "miner": true,
            "size": true, "stateRoot": true, "transactionsRoot": true, "receiptsRoot": true,
            "logsBloom": true, "difficulty": true, "totalDifficulty": true, "extraData": true,
            "mixHash": true, "nonce": true, "sha3Uncles": true
        },
        "transaction": {
            "transactionIndex": true, "hash": true, "nonce": true, "from": true, "to": true,
            "value": true, "gas": true, "gasPrice": true, "maxFeePerGas": true,
            "maxPriorityFeePerGas": true, "input": true, "type": true,
            "v": true, "r": true, "s": true, "yParity": true, "chainId": true
        }
    },
    "transactions": [{}]
}"#;

/// `eth_getBlockByNumber(0x176fa7c, false)` — same block, header + transaction
/// **hashes** only (the `fullTransactions=false` mode). Still scans the whole
/// tx table to gather the block's rows, but emits only the hash per tx, so it
/// isolates the decode/serialize cost vs the full-object variant above.
pub static EVM_GET_BLOCK_BY_NUMBER_TX_HASHES: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 24550620,
    "toBlock": 24550620,
    "includeAllBlocks": true,
    "fields": {
        "block": {
            "number": true, "hash": true, "parentHash": true, "timestamp": true,
            "gasUsed": true, "gasLimit": true, "baseFeePerGas": true, "miner": true,
            "size": true, "stateRoot": true, "transactionsRoot": true, "receiptsRoot": true,
            "logsBloom": true, "difficulty": true, "totalDifficulty": true, "extraData": true,
            "mixHash": true, "nonce": true, "sha3Uncles": true
        },
        "transaction": { "transactionIndex": true, "hash": true }
    },
    "transactions": [{}]
}"#;

// `eth_getLogs({address, topics, fromBlock, toBlock})` over fixed block windows
// of 1 / 10 / 100 blocks — the canonical USDC Transfer filter. Windows are
// anchored at block 24550620 (same as the getBlockByNumber/Receipts queries);
// they yield 63 / 729 / 7784 matching transfers, a clean range-scaling curve.

/// getLogs over a single block.
pub static EVM_GET_LOGS_1BLK: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 24550620,
    "toBlock": 24550620,
    "fields": {
        "block": { "number": true, "hash": true },
        "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true, "transactionHash": true }
    },
    "logs": [{
        "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
        "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
    }]
}"#;

/// getLogs over a 10-block window.
pub static EVM_GET_LOGS_10BLK: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 24550620,
    "toBlock": 24550629,
    "fields": {
        "block": { "number": true, "hash": true },
        "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true, "transactionHash": true }
    },
    "logs": [{
        "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
        "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
    }]
}"#;

/// getLogs over a 100-block window.
pub static EVM_GET_LOGS_100BLK: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 24550620,
    "toBlock": 24550719,
    "fields": {
        "block": { "number": true, "hash": true },
        "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true, "transactionHash": true }
    },
    "logs": [{
        "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
        "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
    }]
}"#;

/// `eth_getBlockReceipts(0x176fa7c)` — every transaction receipt in a single
/// block: receipt fields (status/gas/effectiveGasPrice/...) + the logs each
/// transaction emitted.
pub static EVM_GET_BLOCK_RECEIPTS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 24550620,
    "toBlock": 24550620,
    "fields": {
        "block": { "number": true, "hash": true },
        "transaction": {
            "transactionIndex": true, "hash": true, "from": true, "to": true, "type": true,
            "status": true, "gasUsed": true, "cumulativeGasUsed": true,
            "effectiveGasPrice": true, "contractAddress": true
        },
        "log": {
            "address": true, "topics": true, "data": true,
            "logIndex": true, "transactionIndex": true, "transactionHash": true
        }
    },
    "transactions": [{ "logs": true }]
}"#;

/// `trace_block(0x176fa7c)` — every trace (call/create/suicide/reward) for all
/// transactions in a single block, with the full action + result field set. The
/// traces table is the widest in the chunk, so this is the heaviest single-block
/// RPC call (scan all row groups for one block × decode every trace column).
pub static EVM_TRACE_BLOCK: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 24550620,
    "toBlock": 24550620,
    "fields": {
        "block": { "number": true, "hash": true },
        "trace": {
            "type": true, "error": true, "revertReason": true,
            "transactionIndex": true, "traceAddress": true, "subtraces": true,
            "callFrom": true, "callTo": true, "callValue": true, "callGas": true,
            "callInput": true, "callSighash": true, "callType": true,
            "callResultGasUsed": true, "callResultOutput": true,
            "createFrom": true, "createValue": true, "createGas": true, "createInit": true,
            "createResultGasUsed": true, "createResultCode": true, "createResultAddress": true,
            "suicideAddress": true, "suicideRefundAddress": true, "suicideBalance": true,
            "rewardAuthor": true, "rewardValue": true, "rewardType": true
        }
    },
    "traces": [{}]
}"#;

// Solana queries

/// Whirlpool swap: instruction filter + inner instructions + tx join
pub static SOL_WHIRLPOOL_SWAP: &[u8] = br#"{
    "type": "solana",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true, "hash": true, "timestamp": true },
        "transaction": { "signatures": true, "err": true },
        "instruction": { "programId": true, "accounts": true, "data": true, "instructionAddress": true }
    },
    "instructions": [{
        "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
        "d8": ["0xf8c69e91e17587c8"],
        "innerInstructions": true,
        "transaction": true,
        "isCommitted": true
    }]
}"#;

/// Instruction filter with logs relation (Jupiter aggregator)
pub static SOL_INSTRUCTION_WITH_LOGS: &[u8] = br#"{
    "type": "solana",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true, "hash": true, "timestamp": true },
        "instruction": { "programId": true, "accounts": true, "data": true, "instructionAddress": true },
        "transaction": { "signatures": true, "err": true, "transactionIndex": true },
        "log": { "instructionAddress": true, "kind": true, "message": true, "transactionIndex": true, "logIndex": true }
    },
    "instructions": [{
        "programId": ["JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"],
        "transaction": true,
        "logs": true
    }]
}"#;

/// Instruction + balance changes relation
pub static SOL_BALANCES_FROM_INSTRUCTION: &[u8] = br#"{
    "type": "solana",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true, "hash": true },
        "instruction": { "programId": true, "accounts": true, "data": true },
        "balance": { "transactionIndex": true, "account": true, "post": true, "pre": true }
    },
    "instructions": [{
        "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
        "d8": ["0xf8c69e91e17587c8"],
        "transactionBalances": true,
        "isCommitted": true
    }]
}"#;

/// Solana hard: Meteora DLMM with all relations (matches legacy solana_hard)
pub static SOL_HARD: &[u8] = br#"{
    "type": "solana",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true, "parentNumber": true, "parentHash": true, "height": true, "timestamp": true },
        "transaction": { "signatures": true, "err": true, "feePayer": true },
        "instruction": { "programId": true, "accounts": true, "data": true, "isCommitted": true },
        "log": { "instructionAddress": true, "programId": true, "kind": true, "message": true },
        "balance": { "pre": true, "post": true },
        "tokenBalance": { "preMint": true, "postMint": true, "preDecimals": true, "postDecimals": true, "preOwner": true, "postOwner": true, "preAmount": true, "postAmount": true },
        "reward": { "lamports": true, "postBalance": true, "rewardType": true, "commission": true }
    },
    "instructions": [{
        "programId": ["LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"],
        "d8": [
            "0xf8c69e91e17587c8", "0xb59d59438fb63448", "0x1c8cee63e7a21595",
            "0x0703967f94283dc8", "0x2905eeaf64e106cd", "0x5e9b6797465fdca5",
            "0xa1c26754ab47fa9a", "0x5055d14818ceb16c", "0x0a333d2370691855",
            "0x1a526698f04a691a", "0x2d9aedd2dd0fa65c"
        ],
        "isCommitted": true,
        "transaction": true,
        "transactionTokenBalances": true,
        "innerInstructions": true
    }]
}"#;

/// Indexer-style EVM queries (selective bulk-export patterns).
pub static EVM_QUERIES: &[(&str, &[u8])] = &[
    ("evm/usdc_transfers", EVM_USDC_TRANSFERS),
    ("evm/contract_calls+logs", EVM_CONTRACT_CALLS_WITH_LOGS),
    ("evm/usdc_traces+diffs", EVM_USDC_TRACES_AND_STATEDIFFS),
    ("evm/sparse", EVM_SPARSE_LOGS),
];

/// Full-scan family: unfiltered per-table scans that exercise the two-phase
/// response-budget path. Run across the chunk matrix (small + big).
pub static EVM_FULLSCAN_QUERIES: &[(&str, &[u8])] = &[
    ("evm/all_blocks", EVM_ALL_BLOCKS),
    ("evm/all_txs", EVM_ALL_TXS),
    ("evm/all_logs", EVM_ALL_LOGS),
    ("evm/all_traces", EVM_ALL_TRACES),
    ("evm/all_statediffs", EVM_ALL_STATEDIFFS),
];

/// RPC-compatible EVM queries (node endpoint patterns), all on `data/evm/chunk`.
pub static EVM_RPC_QUERIES: &[(&str, &[u8])] = &[
    ("rpc/getBlockByNumber + full tx", EVM_GET_BLOCK_BY_NUMBER),
    ("rpc/getBlockByNumber", EVM_GET_BLOCK_BY_NUMBER_TX_HASHES),
    ("rpc/getBlockReceipts", EVM_GET_BLOCK_RECEIPTS),
    ("rpc/trace_block", EVM_TRACE_BLOCK),
    ("rpc/getLogs:1blk", EVM_GET_LOGS_1BLK),
    ("rpc/getLogs:10blk", EVM_GET_LOGS_10BLK),
    ("rpc/getLogs:100blk", EVM_GET_LOGS_100BLK),
];

pub static SOL_QUERIES: &[(&str, &[u8])] = &[
    ("sol/whirlpool_swap", SOL_WHIRLPOOL_SWAP),
    ("sol/hard", SOL_HARD),
    ("sol/instr+logs", SOL_INSTRUCTION_WITH_LOGS),
    ("sol/instr+balances", SOL_BALANCES_FROM_INSTRUCTION),
];

// ---------------------------------------------------------------------------
// Chunk matrix — benches run the EVM query sets across every available chunk.
// ---------------------------------------------------------------------------

/// EVM chunks for the benchmark matrix as `(label, path)`. The big chunk is
/// optional: entries whose directory is absent are skipped, so the suite still
/// runs on a machine that only has the small chunk. Paths are overridable via
/// `$EVM_CHUNK` (small) and `$EVM_CHUNK_BIG` (big).
pub fn evm_chunk_matrix() -> Vec<(&'static str, String)> {
    let small = std::env::var("EVM_CHUNK").unwrap_or_else(|_| "data/evm/chunk".into());
    let big = std::env::var("EVM_CHUNK_BIG").unwrap_or_else(|_| "data/evm/big".into());
    [("small", small), ("big", big)]
        .into_iter()
        .filter(|(_, p)| std::path::Path::new(p).exists())
        .collect()
}

/// Labels of the available EVM chunks (for divan `args`).
pub fn evm_chunk_labels() -> Vec<&'static str> {
    evm_chunk_matrix().into_iter().map(|(l, _)| l).collect()
}

/// Resolve an EVM chunk label to its path (re-reads env; called once per bench).
pub fn evm_chunk_path(label: &str) -> String {
    evm_chunk_matrix()
        .into_iter()
        .find(|(l, _)| *l == label)
        .map(|(_, p)| p)
        .unwrap_or_else(|| "data/evm/chunk".into())
}

/// The Solana chunk path (single chunk, no matrix). Overridable via `$SOL_CHUNK`.
pub fn sol_chunk_path() -> String {
    std::env::var("SOL_CHUNK").unwrap_or_else(|_| "data/solana/chunk".into())
}
