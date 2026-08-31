//! Query-surface invariants: the failures a client cannot see.
//!
//! Every test here pins one invariant from `spec/07-invariants.md`. They run
//! against the `tests/fixtures` chunks, so they exercise the same path as the
//! parity suite rather than a synthetic schema.

use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::output::execute_plan;
use sqd_query_engine::query::{compile, parse_query};
use std::path::{Path, PathBuf};

fn fixture_chunk(dataset: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(dataset)
        .join("chunk")
}

fn meta(name: &str) -> DatasetDescription {
    load_dataset_description(Path::new(&format!("metadata/{name}.yaml"))).unwrap()
}

/// Run a query to completion and return the NDJSON body.
fn run(dataset: &str, metadata: &DatasetDescription, query_json: &[u8]) -> anyhow::Result<Vec<u8>> {
    let chunk = fixture_chunk(dataset);
    let parsed = parse_query(query_json, metadata)?;
    let plan = compile(&parsed, metadata)?;
    Ok(execute_plan(&plan, metadata, &chunk)?
        .map(|out| out.into_json_lines())
        .unwrap_or_default())
}

/// Count the items a response carries under one table key. A response always
/// carries a header per block in range, so "nothing matched" is an item count of
/// zero, not an empty body.
fn count_items(body: &[u8], table_key: &str) -> usize {
    body.split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let block: serde_json::Value = serde_json::from_slice(line).unwrap();
            block
                .get(table_key)
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0)
        })
        .sum()
}

// ---------------------------------------------------------------------------
// INV-P8 — case folding follows the column, not the filter
// ---------------------------------------------------------------------------

/// Upper-casing a hex filter value must not change the answer, and it must not
/// matter whether the value arrives as a scalar or inside a list.
#[test]
fn hex_filters_fold_case_in_both_shapes() {
    let evm = meta("evm");
    const ADDR_LOWER: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
    const ADDR_UPPER: &str = "0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48";

    let query = |addr: &str, as_list: bool| {
        let filter = if as_list {
            format!(r#""address": ["{addr}"]"#)
        } else {
            format!(r#""address": "{addr}""#)
        };
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                "fields":{{"log":{{"address":true,"logIndex":true}}}},
                "logs":[{{{filter}}}]}}"#
        )
        .into_bytes()
    };

    let list_lower = run("ethereum", &evm, &query(ADDR_LOWER, true)).unwrap();
    let list_upper = run("ethereum", &evm, &query(ADDR_UPPER, true)).unwrap();
    let scalar_lower = run("ethereum", &evm, &query(ADDR_LOWER, false)).unwrap();
    let scalar_upper = run("ethereum", &evm, &query(ADDR_UPPER, false)).unwrap();

    assert!(!list_lower.is_empty(), "fixture must match some logs");
    assert_eq!(list_lower, list_upper, "list filter must fold case");
    assert_eq!(
        list_lower, scalar_lower,
        "scalar and list filters must mean the same thing"
    );
    assert_eq!(scalar_lower, scalar_upper, "scalar filter must fold case");
}

/// The rule is the column's encoding, not "lowercase everything": a non-hex
/// column still compares byte-exactly.
#[test]
fn non_hex_columns_are_not_folded() {
    let evm = meta("evm");
    let query = |ty: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                "fields":{{"trace":{{"type":true}}}},
                "traces":[{{"type":["{ty}"]}}]}}"#
        )
        .into_bytes()
    };

    let lower = count_items(&run("ethereum", &evm, &query("call")).unwrap(), "traces");
    let upper = count_items(&run("ethereum", &evm, &query("CALL")).unwrap(), "traces");

    assert!(lower > 0, "fixture must contain call traces");
    assert_eq!(
        upper, 0,
        "traces.type is not hex-encoded and must compare byte-exactly"
    );
}

// ---------------------------------------------------------------------------
// INV-Q12, INV-P14 — malformed values error, unmatchable ones do not
// ---------------------------------------------------------------------------

/// A malformed hex value is an error, not a silently narrowed filter. The
/// dangerous shape is a list where one of several values is malformed: it used
/// to become a filter on the survivors, which returns a plausible answer to a
/// query the client never wrote.
#[test]
fn malformed_hex_in_list_is_an_error() {
    let solana = meta("solana");
    let malformed = [
        r#"["0xf8c69e91e17587c8", "f8c69e91e17587c8"]"#, // missing 0x on the second
        r#"["0xabc"]"#,                                  // odd digit count
        r#"["0xzz"]"#,                                   // not hex
        r#"[123]"#,                                      // number for a fixed_binary column
        r#"[true]"#,                                     // neither string nor integer
    ];
    for values in malformed {
        let json = format!(
            r#"{{"type":"solana","fromBlock":0,"instructions":[{{"d16":{values}}}]}}"#
        );
        let parsed = parse_query(json.as_bytes(), &solana).unwrap();
        assert!(
            compile(&parsed, &solana).is_err(),
            "expected an error for d16 filter {values}"
        );
    }
}

/// INV-P14: a well-formed value the column cannot hold matches nothing. It is
/// not an error, and it must never be truncated into a *different* value —
/// `instructionAddress` above u32::MAX used to wrap.
#[test]
fn unmatchable_values_are_not_errors() {
    let solana = meta("solana");
    let queries = [
        // 8-byte value against the 1-byte d1 column: cannot match, not malformed.
        r#"{"type":"solana","fromBlock":0,"instructions":[{"d1":["0xf8c69e91e17587c8"]}]}"#,
        // Beyond u32::MAX for a list_uint32 column.
        r#"{"type":"solana","fromBlock":0,"instructions":[{"instructionAddress":[4294967296]}]}"#,
    ];
    for json in queries {
        let out = run("solana", &solana, json.as_bytes())
            .unwrap_or_else(|e| panic!("expected a result, got an error: {e} for {json}"));
        assert_eq!(
            count_items(&out, "instructions"),
            0,
            "expected no matches for {json}"
        );
    }
}

// ---------------------------------------------------------------------------
// INV-O9 — hexNumber renders quoted and zero-padded to the column's width
// ---------------------------------------------------------------------------

/// Solana discriminator prefixes are selectable columns. Emitted as raw JSON
/// numbers, a `uint64` `d8` above 2^53 is silently re-read as a different value
/// by every JavaScript client — the discriminator a client receives is not the
/// one that was stored. They render as quoted hex, zero-padded to the column's
/// physical width, so that `"0x0640"` and `"0x640"` stay distinguishable.
#[test]
fn discriminator_columns_render_as_padded_hex() {
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
    for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let block: serde_json::Value = serde_json::from_slice(line).unwrap();
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

/// The width is the column's, not the value's: a small `d8` still renders
/// sixteen digits, and the prefix bytes agree across widths for the same
/// instruction.
#[test]
fn discriminator_hex_is_a_prefix_chain() {
    let solana = meta("solana");
    let body = run(
        "solana",
        &solana,
        br#"{"type":"solana","fromBlock":0,
             "fields":{"instruction":{"d1":true,"d2":true,"d4":true,"d8":true}},
             "instructions":[{"programId":["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"]}]}"#,
    )
    .unwrap();

    let mut checked = 0;
    for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let block: serde_json::Value = serde_json::from_slice(line).unwrap();
        for item in block
            .get("instructions")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            let d1 = item["d1"].as_str().unwrap();
            let d2 = item["d2"].as_str().unwrap();
            let d4 = item["d4"].as_str().unwrap();
            let d8 = item["d8"].as_str().unwrap();
            assert!(d2.starts_with(d1), "{d2} must extend {d1}");
            assert!(d4.starts_with(d2), "{d4} must extend {d2}");
            assert!(d8.starts_with(d4), "{d8} must extend {d4}");
            checked += 1;
        }
    }
    assert!(checked > 0, "fixture must contain whirlpool instructions");
}
