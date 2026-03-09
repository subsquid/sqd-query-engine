// Shared query definitions for benchmarks.
// Included via `#[path]` from bench binaries.

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

/// Transaction filter with log relation (all events from Aave calls)
pub static EVM_CONTRACT_CALLS_WITH_LOGS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "fields": {
        "block": { "number": true, "hash": true, "timestamp": true },
        "transaction": { "from": true, "to": true, "hash": true, "sighash": true, "transactionIndex": true },
        "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true }
    },
    "transactions": [{
        "to": ["0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9"],
        "logs": true
    }]
}"#;

/// Multi-table: BAYC traces + state diffs with transaction relations
pub static EVM_BAYC_TRACES_AND_STATEDIFFS: &[u8] = br#"{
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
        "callTo": ["0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"],
        "transaction": true
    }],
    "stateDiffs": [{
        "address": ["0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d"],
        "transaction": true
    }]
}"#;

/// Full scan: all blocks with header fields
pub static EVM_ALL_BLOCKS: &[u8] = br#"{
    "type": "evm",
    "fromBlock": 0,
    "includeAllBlocks": true,
    "fields": {
        "block": { "number": true, "hash": true, "timestamp": true, "gasUsed": true, "gasLimit": true, "parentHash": true }
    }
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

/// Instruction filter with logs relation
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
        "programId": ["SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ"],
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

/// Full scan: all blocks with header fields
pub static SOL_ALL_BLOCKS: &[u8] = br#"{
    "type": "solana",
    "fromBlock": 0,
    "includeAllBlocks": true,
    "fields": {
        "block": { "number": true, "hash": true, "timestamp": true, "height": true, "parentHash": true, "parentNumber": true }
    }
}"#;

/// All benchmark queries grouped by chain
pub static EVM_QUERIES: &[(&str, &[u8])] = &[
    ("evm/usdc_transfers", EVM_USDC_TRANSFERS),
    ("evm/contract_calls+logs", EVM_CONTRACT_CALLS_WITH_LOGS),
    ("evm/bayc_traces+diffs", EVM_BAYC_TRACES_AND_STATEDIFFS),
    ("evm/all_blocks", EVM_ALL_BLOCKS),
];

pub static SOL_QUERIES: &[(&str, &[u8])] = &[
    ("sol/whirlpool_swap", SOL_WHIRLPOOL_SWAP),
    ("sol/instr+logs", SOL_INSTRUCTION_WITH_LOGS),
    ("sol/instr+balances", SOL_BALANCES_FROM_INSTRUCTION),
    ("sol/all_blocks", SOL_ALL_BLOCKS),
];
