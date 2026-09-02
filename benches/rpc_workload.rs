//! Data-derived EVM JSON-RPC query generation for benchmarking the engines against real
//! worker parquet chunks (random production data, not the synthetic fixture).
//!
//! Production traffic for these engines is shaped like Ethereum JSON-RPC, so the
//! benchmark focus is RPC methods. The static bench queries are pinned to the
//! synthetic chunk's block numbers / addresses, which match nothing on a random
//! real chunk. Here we instead:
//!   1. `analyze_chunk` — read the chunk's block range (from its dir name) and
//!      its hottest log `topic0` / `address` (one projected column scan each).
//!   2. `gen_variants` — emit node-RPC-shaped portal queries rebound to that
//!      chunk: getBlockByNumber / getBlockReceipts / trace_block on a mid-chunk
//!      block, and getLogs over 1/10/100-block windows (hot-topic, hot-address,
//!      unfiltered, and an absent-topic "sparse" empty-result probe).
//!
//! The same JSON drives both the new and legacy engines, keeping the A/B
//! apples-to-apples on identical real data.
#![allow(dead_code)]

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use rustc_hash::FxHashMap as HashMap;
use std::fs::File;
use std::path::Path;

/// Patterns discovered from a single chunk, used to bind RPC queries to it.
pub struct ChunkInfo {
    pub from: u64,
    pub to: u64,
    pub has_traces: bool,
    pub has_statediffs: bool,
    /// Most frequent log `topic0` (canonical `0x…`), if the chunk has logs.
    pub hot_topic0: Option<String>,
    /// Most frequent log `address` (canonical `0x…`), if the chunk has logs.
    pub hot_address: Option<String>,
}

/// Parse `from`/`to` from a chunk dir named `<from>-<to>-<hash>`
/// (e.g. `0012357171-0012420657-34e9f3dc`). Zero-padded decimal block numbers.
fn block_range_from_dirname(dir: &Path) -> Option<(u64, u64)> {
    let name = dir.file_name()?.to_str()?;
    let mut it = name.split('-');
    let from: u64 = it.next()?.parse().ok()?;
    let to: u64 = it.next()?.parse().ok()?;
    Some((from, to))
}

/// Canonicalize a stored hex string to `0x` + lowercase hex.
fn norm_hex_str(s: &str) -> String {
    let body = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let mut out = String::with_capacity(2 + body.len());
    out.push_str("0x");
    out.extend(body.chars().map(|c| c.to_ascii_lowercase()));
    out
}

/// Most frequent non-empty value of one string column in `<table>.parquet`,
/// returned in canonical `0x…` form. `None` if the file/column is absent or
/// empty. One-time (not in the timed loop), so a full single-column scan is fine.
fn top_value(dir: &Path, table: &str, column: &str) -> Option<String> {
    let path = dir.join(format!("{table}.parquet"));
    let file = File::open(&path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
    let descr = builder.parquet_schema();
    let leaf = descr.columns().iter().position(|c| c.name() == column)?;
    let mask = ProjectionMask::leaves(descr, [leaf]);
    let reader = builder
        .with_projection(mask)
        .with_batch_size(16384)
        .build()
        .ok()?;

    let mut counts: HashMap<String, u64> = HashMap::default();
    for batch in reader {
        let batch = batch.ok()?;
        let col = batch.column(0);
        // Cast everything (Dictionary<_,Utf8>, LargeUtf8, Binary-of-utf8) to a
        // plain StringArray so one code path covers all encodings.
        let utf8 = match col.as_any().downcast_ref::<StringArray>() {
            Some(s) => s.clone(),
            None => match arrow::compute::cast(col, &DataType::Utf8) {
                Ok(a) => a.as_any().downcast_ref::<StringArray>()?.clone(),
                Err(_) => continue,
            },
        };
        for i in 0..utf8.len() {
            if utf8.is_valid(i) {
                let v = utf8.value(i);
                if !v.is_empty() {
                    *counts.entry(norm_hex_str(v)).or_default() += 1;
                }
            }
        }
    }
    counts.into_iter().max_by_key(|(_, c)| *c).map(|(v, _)| v)
}

/// Scan a chunk once to learn its block range, present tables, and hot log keys.
pub fn analyze_chunk(dir: &Path) -> ChunkInfo {
    let (from, to) = block_range_from_dirname(dir).unwrap_or((0, u64::MAX));
    ChunkInfo {
        from,
        to,
        has_traces: dir.join("traces.parquet").exists(),
        has_statediffs: dir.join("statediffs.parquet").exists(),
        hot_topic0: top_value(dir, "logs", "topic0"),
        hot_address: top_value(dir, "logs", "address"),
    }
}

// ---------------------------------------------------------------------------
// RPC query templates. Field sets are kept to columns common across EVM chains
// (no baseFeePerGas / EIP-1559 / receipt-only fields) so the same query is
// valid on pre-1559, L2, and traceless datasets alike.
// ---------------------------------------------------------------------------

/// `eth_getBlockByNumber(n, fullTransactions=true)` — header + full transaction objects.
fn q_get_block_by_number(block: u64) -> String {
    format!(
        r#"{{"type":"evm","fromBlock":{block},"toBlock":{block},"includeAllBlocks":true,"fields":{{"block":{{"number":true,"hash":true,"parentHash":true,"timestamp":true,"miner":true,"gasUsed":true,"gasLimit":true}},"transaction":{{"transactionIndex":true,"hash":true,"nonce":true,"from":true,"to":true,"value":true,"gas":true,"gasPrice":true,"input":true,"type":true}}}},"transactions":[{{}}]}}"#
    )
}

/// `eth_getBlockByNumber(n, fullTransactions=false)` — header + tx hashes only,
/// no transaction bodies. The lightweight sibling of `q_get_block_by_number`.
fn q_get_block_tx_hashes(block: u64) -> String {
    format!(
        r#"{{"type":"evm","fromBlock":{block},"toBlock":{block},"includeAllBlocks":true,"fields":{{"block":{{"number":true,"hash":true,"parentHash":true,"timestamp":true,"miner":true,"gasUsed":true,"gasLimit":true}},"transaction":{{"transactionIndex":true,"hash":true}}}},"transactions":[{{}}]}}"#
    )
}

/// `eth_getBlockReceipts(n)` — every tx receipt + its logs in one block.
fn q_get_block_receipts(block: u64) -> String {
    format!(
        r#"{{"type":"evm","fromBlock":{block},"toBlock":{block},"fields":{{"block":{{"number":true,"hash":true}},"transaction":{{"transactionIndex":true,"hash":true,"from":true,"to":true,"type":true,"status":true,"gasUsed":true,"cumulativeGasUsed":true}},"log":{{"address":true,"topics":true,"data":true,"logIndex":true,"transactionIndex":true,"transactionHash":true}}}},"transactions":[{{"logs":true}}]}}"#
    )
}

/// `trace_block(n)` — every trace for all txs in one block.
fn q_trace_block(block: u64) -> String {
    format!(
        r#"{{"type":"evm","fromBlock":{block},"toBlock":{block},"fields":{{"block":{{"number":true,"hash":true}},"trace":{{"type":true,"transactionIndex":true,"traceAddress":true,"subtraces":true,"callFrom":true,"callTo":true,"callValue":true,"callGas":true,"callInput":true,"callSighash":true,"createFrom":true,"createValue":true,"createGas":true}}}},"traces":[{{}}]}}"#
    )
}

/// `eth_getLogs({fromBlock,toBlock,[topic0|address]})`.
fn q_get_logs(from: u64, to: u64, topic0: Option<&str>, address: Option<&str>) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(a) = address {
        parts.push(format!(r#""address":["{a}"]"#));
    }
    if let Some(t) = topic0 {
        parts.push(format!(r#""topic0":["{t}"]"#));
    }
    let filt = parts.join(",");
    format!(
        r#"{{"type":"evm","fromBlock":{from},"toBlock":{to},"fields":{{"block":{{"number":true,"hash":true}},"log":{{"address":true,"topics":true,"data":true,"logIndex":true,"transactionIndex":true,"transactionHash":true}}}},"logs":[{{{filt}}}]}}"#
    )
}

/// A topic0 that no chain emits — drives the empty-result "sparse" getLogs path.
const ABSENT_TOPIC0: &str = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

/// `m` probe blocks spaced evenly inside `[from, to]`, avoiding the exact edges
/// (so windows starting here still have room to extend). Each probe makes a
/// request touch a *different* block, so the timed loop sweeps the whole chunk
/// instead of replaying one hot row group.
fn probe_blocks(from: u64, to: u64, m: usize) -> Vec<u64> {
    if to <= from || m <= 1 {
        return vec![from + to.saturating_sub(from) / 2];
    }
    let span = to - from;
    (1..=m as u64)
        .map(|i| from + span * i / (m as u64 + 1))
        .collect()
}

/// One generated request: which loaded chunk it targets + the query JSON.
pub type Variant = (usize, Vec<u8>);

/// Build the multi-chunk RPC query set for one dataset's `chunks` (index =
/// position in the caller's reader vec). For every RPC method, emit `m` probe
/// blocks **per chunk**, so a single case rotates across `chunks.len() * m`
/// distinct blocks spanning several chunks — no hot-spot, a large working set.
/// Returns `(method_name, variants)` per method.
pub fn gen_variants(chunks: &[ChunkInfo], m: usize) -> Vec<(String, Vec<Variant>)> {
    let mut cases: Vec<(String, Vec<Variant>)> = Vec::new();

    // Single-block methods: one variant per (chunk, probe block).
    for (name, build, need_traces) in [
        (
            "rpc/getBlockByNumber + full tx",
            q_get_block_by_number as fn(u64) -> String,
            false,
        ),
        ("rpc/getBlockByNumber", q_get_block_tx_hashes, false),
        ("rpc/getBlockReceipts", q_get_block_receipts, false),
        ("rpc/trace_block", q_trace_block, true),
    ] {
        let mut v: Vec<Variant> = Vec::new();
        for (ci, info) in chunks.iter().enumerate() {
            if need_traces && !info.has_traces {
                continue;
            }
            for b in probe_blocks(info.from, info.to, m) {
                v.push((ci, build(b).into_bytes()));
            }
        }
        if !v.is_empty() {
            cases.push((name.to_string(), v));
        }
    }

    // getLogs windows: window [b, b+k] anchored at each probe block, filtered per
    // chunk (each chunk carries its own hottest topic0 / address).
    let windows: &[(&str, u64)] = &[
        ("rpc/getLogs:1blk", 0),
        ("rpc/getLogs:10blk", 9),
        ("rpc/getLogs:100blk", 99),
    ];
    for &(name, k) in windows {
        let mut v: Vec<Variant> = Vec::new();
        for (ci, info) in chunks.iter().enumerate() {
            let Some(tp) = info.hot_topic0.as_deref() else {
                continue;
            };
            for b in probe_blocks(info.from, info.to, m) {
                v.push((
                    ci,
                    q_get_logs(b, (b + k).min(info.to), Some(tp), None).into_bytes(),
                ));
            }
        }
        if !v.is_empty() {
            cases.push((name.to_string(), v));
        }
    }

    // Contract-centric / unfiltered / empty-probe getLogs over 100-block windows.
    let mut v_addr: Vec<Variant> = Vec::new();
    let mut v_nofilter: Vec<Variant> = Vec::new();
    let mut v_sparse: Vec<Variant> = Vec::new();
    for (ci, info) in chunks.iter().enumerate() {
        for b in probe_blocks(info.from, info.to, m) {
            let to = (b + 99).min(info.to);
            if let Some(ad) = info.hot_address.as_deref() {
                v_addr.push((ci, q_get_logs(b, to, None, Some(ad)).into_bytes()));
            }
            v_nofilter.push((ci, q_get_logs(b, to, None, None).into_bytes()));
            v_sparse.push((
                ci,
                q_get_logs(b, to, Some(ABSENT_TOPIC0), None).into_bytes(),
            ));
        }
    }
    if !v_addr.is_empty() {
        cases.push(("rpc/getLogs:100blk:addr".to_string(), v_addr));
    }
    cases.push(("rpc/getLogs:100blk:nofilter".to_string(), v_nofilter));
    cases.push(("rpc/getLogs:100blk:sparse".to_string(), v_sparse));

    cases
}
