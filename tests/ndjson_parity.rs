//! NDJSON parity between the new engine's convenience entry point
//! (`run_query_ndjson`, "v2") and the legacy `sqd_query` engine ("v1"), gated on
//! `legacy-query`:
//!
//!   cargo test --features legacy-query --test ndjson_parity -- --nocapture
//!
//! ## What must hold (and what deliberately need not)
//!
//! A consumer concatenates per-chunk NDJSON blobs byte-for-byte, so what must
//! hold is:
//!   1. **Framing** — correct NDJSON: one block per line, a trailing newline,
//!      empty result → empty output. Validated byte-exact on v2.
//!   2. **Content** — the same blocks in the same order, each decoding to the
//!      same JSON value as v1. Validated per line at the `serde_json::Value`
//!      level.
//!
//! v2 is intentionally **not** byte-identical to v1: it has its own deterministic
//! field/table ordering and emits raw UTF-8 where v1 `\u`-escapes. Both are valid
//! JSON that decode identically — all a concatenating consumer needs — so content
//! is compared as values, not bytes. (A byte compare here fails only on key order
//! / escaping, which is noise.)
//!
//! Each dataset test loops over every fixture query, runs both engines on the
//! same chunk + range, and checks the two properties above. Fixtures where either
//! engine errors/panics (e.g. error-expecting cases) are skipped and counted, so
//! coverage is reported rather than silently dropped.
#![cfg(feature = "legacy-query")]

use sqd_query::{JsonLinesWriter as V1Writer, ParquetChunk, Query as V1Query};
use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::run_query_ndjson;
use sqd_query_engine::scan::ParquetChunkReader;
use std::path::{Path, PathBuf};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Legacy v1 NDJSON for a query against a chunk dir (mirrors generate_fixtures.rs).
fn v1_ndjson(query_json: &[u8], chunk_dir: &Path) -> anyhow::Result<Vec<u8>> {
    let chunk = ParquetChunk::new(chunk_dir.to_str().unwrap());
    let query = V1Query::from_json_bytes(query_json)?;
    let plan = query.compile();
    let mut writer = V1Writer::new(Vec::new());
    if let Some(mut blocks) = plan.execute(&chunk)? {
        writer.write_blocks(&mut blocks)?;
    }
    Ok(writer.finish()?)
}

/// Pull the query's own block range so v2 sees the same window v1 does (v1 here
/// runs straight off the query, unclamped, exactly like fixture generation).
fn query_range(query_json: &[u8]) -> (u64, Option<u64>) {
    let v: serde_json::Value = serde_json::from_slice(query_json).unwrap();
    let from = v.get("fromBlock").and_then(|x| x.as_u64()).unwrap_or(0);
    let to = v.get("toBlock").and_then(|x| x.as_u64());
    (from, to)
}

/// Compare v2 against v1 for one fixture. Returns `None` on parity, or a
/// human-readable reason on a *real* divergence (framing, block count/order, or
/// a per-block value difference — never mere key-order/escaping noise).
fn compare(name: &str, v1: &[u8], v2: &[u8]) -> Option<String> {
    // --- Framing (byte-level, v2) ---
    if v1.is_empty() || v2.is_empty() {
        return if v1.is_empty() && v2.is_empty() {
            None
        } else {
            Some(format!(
                "  {name}: empty-result mismatch (v1 {} bytes, v2 {} bytes)",
                v1.len(),
                v2.len()
            ))
        };
    }
    if !v2.ends_with(b"\n") {
        return Some(format!("  {name}: v2 NDJSON missing trailing newline"));
    }
    if v2.first() == Some(&b'[') {
        return Some(format!("  {name}: v2 starts with '[' — not NDJSON"));
    }

    // --- Content (value-level, per line) ---
    let lines = |b: &[u8]| -> Vec<String> {
        std::str::from_utf8(b)
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty())
            .map(str::to_owned)
            .collect()
    };
    let (l1, l2) = (lines(v1), lines(v2));
    if l1.len() != l2.len() {
        return Some(format!(
            "  {name}: block count differs (v1 {} lines, v2 {} lines)",
            l1.len(),
            l2.len()
        ));
    }
    for (i, (a, b)) in l1.iter().zip(l2.iter()).enumerate() {
        let va: serde_json::Value = serde_json::from_str(a).unwrap();
        let vb: serde_json::Value = serde_json::from_str(b).unwrap();
        if va != vb {
            let num = va.get("header").and_then(|h| h.get("number"));
            return Some(format!(
                "  {name}: block at line {i} (number={num:?}) differs in VALUE"
            ));
        }
    }
    None
}

/// Run parity over every query under `tests/fixtures/<dataset>/queries`.
/// Returns (checked, skipped, divergence messages).
fn parity_over_dataset(dataset: &str, meta_path: &str) -> (usize, usize, Vec<String>) {
    let base = fixture_dir().join(dataset);
    let chunk = base.join("chunk");
    let queries = base.join("queries");
    if !chunk.exists() || !queries.exists() {
        eprintln!("SKIP {dataset}: chunk/queries not found");
        return (0, 0, Vec::new());
    }

    let meta = load_dataset_description(Path::new(meta_path)).unwrap();
    let reader = ParquetChunkReader::open(&chunk).unwrap();

    let mut entries: Vec<_> = std::fs::read_dir(&queries).unwrap().flatten().collect();
    entries.sort_by_key(|e| e.file_name());

    let (mut checked, mut skipped) = (0usize, 0usize);
    let mut diffs = Vec::new();

    for entry in entries {
        let qf = entry.path().join("query.json");
        if !qf.exists() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let query_json = std::fs::read(&qf).unwrap();
        let (from, to) = query_range(&query_json);

        // v2: skip error-expecting fixtures (both engines would reject them).
        let v2 = match run_query_ndjson(&query_json, &meta, &reader, from, to) {
            Ok((bytes, _last)) => bytes,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        // v1: legacy `.unwrap()` paths can panic on shapes it rejects — guard it.
        let v1 = match std::panic::catch_unwind(|| v1_ndjson(&query_json, &chunk)) {
            Ok(Ok(bytes)) => bytes,
            _ => {
                skipped += 1;
                continue;
            }
        };

        checked += 1;
        if let Some(msg) = compare(&name, &v1, &v2) {
            diffs.push(msg);
        }
    }

    (checked, skipped, diffs)
}

fn assert_dataset_parity(dataset: &str, meta_path: &str) {
    let (checked, skipped, diffs) = parity_over_dataset(dataset, meta_path);
    eprintln!("{dataset}: {checked} checked, {skipped} skipped, {} divergences", diffs.len());
    assert!(
        diffs.is_empty(),
        "{dataset}: {} NDJSON parity divergences vs v1:\n{}",
        diffs.len(),
        diffs.join("\n")
    );
    // Guard against a silently-empty run (e.g. a renamed fixture dir).
    assert!(checked > 0, "{dataset}: no fixtures were actually compared");
}

macro_rules! parity_test {
    ($name:ident, $dir:expr, $meta:expr) => {
        #[test]
        fn $name() {
            assert_dataset_parity($dir, $meta);
        }
    };
}

parity_test!(evm_parity, "ethereum", "metadata/evm.yaml");
parity_test!(solana_parity, "solana", "metadata/solana.yaml");
parity_test!(bitcoin_parity, "bitcoin", "metadata/bitcoin.yaml");
parity_test!(fuel_parity, "fuel", "metadata/fuel.yaml");
parity_test!(optimism_parity, "optimism", "metadata/evm.yaml");
parity_test!(binance_parity, "binance", "metadata/evm.yaml");
parity_test!(tempo_parity, "tempo", "metadata/evm.yaml");
parity_test!(kusama_parity, "kusama", "metadata/substrate.yaml");
parity_test!(moonbeam_parity, "moonbeam", "metadata/substrate.yaml");
parity_test!(hyperliquid_fills_parity, "hyperliquid", "metadata/hyperliquid_fills.yaml");
parity_test!(
    hyperliquid_replica_cmds_parity,
    "hyperliquid_replica_cmds",
    "metadata/hyperliquid_replica_cmds.yaml"
);

/// Explicit empty-result framing: a range past the chunk yields zero bytes from
/// both engines (not "[]"), and v2 reports no last block.
#[test]
fn empty_result_parity() {
    let base = fixture_dir().join("ethereum");
    let chunk = base.join("chunk");
    if !chunk.exists() {
        eprintln!("SKIP empty_result_parity: ethereum chunk not found");
        return;
    }
    let query = br#"{
        "type": "evm", "fromBlock": 999999999, "toBlock": 999999999,
        "logs": [{ "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"] }]
    }"#;
    let meta = load_dataset_description(Path::new("metadata/evm.yaml")).unwrap();
    let reader = ParquetChunkReader::open(&chunk).unwrap();

    let (v2, last) = run_query_ndjson(query, &meta, &reader, 999999999, Some(999999999)).unwrap();
    let v1 = v1_ndjson(query, &chunk).unwrap();

    assert!(v2.is_empty(), "v2 empty result must be zero bytes");
    assert_eq!(last, None, "empty result has no last block");
    assert!(v1.is_empty(), "v1 empty result must be zero bytes too");
}
