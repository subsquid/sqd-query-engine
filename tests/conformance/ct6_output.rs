//! CT-6 — output and determinism.
//!
//! How a column renders is fixed by the catalog's declared encoding, not by the
//! physical type the chunk happens to store it at; and a response says the same
//! thing however it is read out of the engine — block by block, all at once, or
//! as Arrow.
//!
//! Byte determinism (INV-O12) and the thread-count sweep (INV-O13) are not here:
//! both need HC-3, the chunk writer.

use sqd_query_engine::output::{execute_chunk_arrow, execute_plan};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;

use crate::harness::chunk::chunk_with_column_filled;
use crate::harness::fixtures::{fixture_tree_is_present, meta, run};
use crate::harness::json::parse_response;
use crate::harness::synthetic::{
    catalog, logs_query, run as run_synthetic, uniform, weighted_chunk, BLOCKS,
};

/// Solana discriminator prefixes are selectable columns. Emitted as raw JSON
/// numbers, a `uint64` `d8` above 2^53 is silently re-read as a different value
/// by every JavaScript client — the discriminator a client receives is not the
/// one that was stored. They render as quoted hex, zero-padded to the column's
/// physical width, so that `"0x0640"` and `"0x640"` stay distinguishable.
///
/// Covers CT-6 · INV-O9
#[test]
#[ignore = "requires external fixture data"]
fn discriminator_columns_render_as_padded_hex() {
    if !fixture_tree_is_present() {
        return;
    }

    let solana = meta("solana");
    let body = run(
        "solana",
        &solana,
        br#"{"type":"solana","fromBlock":0,
             "fields":{"instruction":{"d1":true,"d2":true,"d4":true,"d8":true}},
             "instructions":[{"programId":["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"]}]}"#,
    )
    .unwrap();

    let mut seen = 0;
    for block in parse_response(&body) {
        let Some(items) = block.get("instructions").and_then(|v| v.as_array()) else {
            continue;
        };
        for item in items {
            for (key, width) in [("d1", 2), ("d2", 4), ("d4", 8), ("d8", 16)] {
                let value = item.get(key).unwrap();
                let s = value.as_str().unwrap_or_else(|| {
                    panic!("{key} must be a quoted string, got {value} — a JSON number loses precision above 2^53")
                });
                assert!(s.starts_with("0x"), "{key} = {s} must be 0x-prefixed");
                assert_eq!(
                    s.len() - 2,
                    width,
                    "{key} = {s} must be zero-padded to {width} hex digits"
                );
                assert_eq!(s.to_ascii_lowercase(), s, "{key} = {s} must be lowercase");
            }
            seen += 1;
        }
    }
    assert!(seen > 0, "fixture must contain whirlpool instructions");
}

/// `jsonVerbatim` splices stored bytes into a document the engine wrote, so a
/// column declared with it and holding anything else does not corrupt one field
/// — it ends the response, mid-object, for every client at once.
///
/// Tron's `internal_transactions.extra` is the trap: the archive writes it with
/// the same builder as `call_value_info`, so it reads as JSON in the chunk
/// schema, but the model types it `Option<HexBytes>` and appends it raw. The
/// bundled fixture leaves it null in all 9813 rows, which is why the ten Tron
/// fixture tests pass either way.
#[test]
#[ignore = "requires external fixture data"]
fn tron_internal_transaction_extra_renders_as_a_string() {
    if !fixture_tree_is_present() {
        return;
    }

    const EXTRA: &str = "a1b2c3d4";

    let tron = meta("tron");
    let chunk = chunk_with_column_filled("tron", "internal_transactions", "extra", EXTRA);
    let query = br#"{"type":"tron","fromBlock":82644089,"toBlock":82644089,
                     "fields":{"internalTransaction":{"extra":true}},
                     "internalTransactions":[{}]}"#;

    let parsed = parse_query(query, &tron).unwrap();
    let plan = compile(&parsed, &tron).unwrap();
    let body = execute_plan(&plan, &tron, chunk.path())
        .unwrap()
        .map(|out| out.into_json_lines())
        .unwrap_or_default();

    let mut seen = 0;
    for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let block: serde_json::Value = serde_json::from_slice(line).unwrap_or_else(|e| {
            panic!(
                "response line is not JSON ({e}): {}",
                String::from_utf8_lossy(&line[..line.len().min(200)])
            )
        });
        for item in block
            .get("internalTransactions")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            assert_eq!(item["extra"].as_str(), Some(EXTRA));
            seen += 1;
        }
    }

    assert!(seen > 0, "fixture must contain internal transactions");
}

/// A query over a range with no data yields `None`, on both output formats.
///
/// Covers CT-6 · INV-O1
#[test]
fn empty_result_is_none() {
    let chunk = weighted_chunk(BLOCKS, &[(12, 10)], &[]);
    let query = serde_json::json!({
        "type": "test",
        "fromBlock": 100,
        "toBlock": 200,
        "logs": [{}],
        "fields": {"log": {"data": true}}
    });
    assert!(run_synthetic(&catalog(), &chunk, query.clone()).is_none());

    let meta = catalog();
    let parsed = parse_query(query.to_string().as_bytes(), &meta).unwrap();
    let plan = compile(&parsed, &meta).unwrap();
    let reader = ParquetChunkReader::open(chunk.path()).unwrap();
    assert!(execute_chunk_arrow(&plan, &meta, &reader, false, false)
        .unwrap()
        .is_none());
}

/// Block-by-block iteration produces the same bytes as `into_json_lines`
/// (modulo framing), iteration state is tracked correctly, and
/// `into_json_lines` re-encodes everything regardless of prior iteration.
///
/// Covers CT-6 · INV-O1
#[test]
fn iteration_matches_json_lines() {
    let meta = catalog();
    let chunk = weighted_chunk(BLOCKS, &uniform(BLOCKS, 10), &[]);

    let mut blocks = run_synthetic(&meta, &chunk, logs_query()).unwrap();
    let mut iterated = Vec::new();
    let mut count = 0;
    while blocks.has_next_block() {
        blocks.write_next_block(&mut iterated);
        iterated.push(b'\n');
        count += 1;
    }
    assert_eq!(count, blocks.num_blocks());

    // Consumed iterator still re-encodes everything.
    assert_eq!(blocks.into_json_lines(), iterated);

    let mut partial = run_synthetic(&meta, &chunk, logs_query()).unwrap();
    partial.write_next_block(&mut Vec::new());
    assert_eq!(partial.into_json_lines(), iterated);
}
