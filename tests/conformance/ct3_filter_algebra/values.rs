//! What a filter value matches, and what it matches when it cannot match
//! anything: an unmatchable value is an empty result, not an error, and never a
//! widening.

use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::query::{compile, parse_query};
use std::path::Path;

use crate::harness::chunk::write_parquet;
use crate::harness::fixtures::{fixture_tree_is_present, meta, plan_error, run, run_against};
use crate::harness::json::{count_items, items_in, parse_response};

/// INV-P14: a well-formed value the column cannot hold matches nothing. It is
/// not an error, and it must never be truncated into a *different* value —
/// `instructionAddress` above u32::MAX used to wrap.
///
/// Covers CT-3 · INV-P14
#[test]
#[ignore = "requires external fixture data"]
fn unmatchable_values_are_not_errors() {
    if !fixture_tree_is_present() {
        return;
    }

    let solana = meta("solana");
    // An 8-byte value against the 1-byte d1 column: cannot match, not malformed.
    let json = r#"{"type":"solana","fromBlock":0,
                   "instructions":[{"d1":["0xf8c69e91e17587c8"]}]}"#;
    let out = run("solana", &solana, json.as_bytes())
        .unwrap_or_else(|e| panic!("expected a result, got an error: {e}"));
    assert_eq!(count_items(&out, "instructions"), 0, "expected no matches");

    // Beyond u32::MAX for a list_uint32 column: it used to wrap to 0 and match
    // whatever carried asset 0.
    let replica = meta("hyperliquid_replica_cmds");
    let json = r#"{"type":"hyperliquidReplicaCmds","fromBlock":0,
                   "fields":{"action":{"user":true}},
                   "orderActions":[{"containsAsset":[4294967296]}]}"#;
    let out = run("hyperliquid_replica_cmds", &replica, json.as_bytes())
        .unwrap_or_else(|e| panic!("expected a result, got an error: {e}"));
    assert_eq!(count_items(&out, "actions"), 0, "expected no matches");
}

/// The width is the column's, not the value's: a small `d8` still renders
/// sixteen digits, and the prefix bytes agree across widths for the same
/// instruction.
///
/// Covers CT-3 · INV-P13
#[test]
#[ignore = "requires external fixture data"]
fn discriminator_hex_is_a_prefix_chain() {
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

/// A one-element list and the bare value are the same request, so they must
/// compile through the same code. They used to take separate branches: the list
/// branch parsed hex against the column's type, the scalar branch compared a
/// `Utf8` against whatever the column was. On a string column that happened to
/// work; on `d1`, `d2`, `d4`, `d8` it matched nothing and said 200.
#[test]
#[ignore = "requires external fixture data"]
fn a_scalar_filter_means_the_same_as_a_one_element_list() {
    if !fixture_tree_is_present() {
        return;
    }

    let cases: &[(&str, &str, u64, u64, &str, &str, &str)] = &[
        // dataset, chunk, fromBlock, toBlock, request key, filter, value
        (
            "solana",
            "solana",
            217710049,
            217710050,
            "instructions",
            "d1",
            r#""0x02""#,
        ),
        (
            "solana",
            "solana",
            217710049,
            217710050,
            "instructions",
            "d2",
            r#""0x0200""#,
        ),
        (
            "solana",
            "solana",
            217710049,
            217710956,
            "instructions",
            "programId",
            r#""whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc""#,
        ),
        (
            "evm",
            "ethereum",
            17881390,
            17881391,
            "logs",
            "address",
            r#""0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48""#,
        ),
        (
            "evm",
            "ethereum",
            17881390,
            17881391,
            "transactions",
            "sighash",
            r#""0xa9059cbb""#,
        ),
    ];

    let mut exercised = 0;

    for (dataset, chunk, from, to, key, filter, value) in cases {
        let metadata = meta(dataset);
        let query = |wrapped: bool| {
            let value = if wrapped {
                format!("[{value}]")
            } else {
                value.to_string()
            };
            format!(
                r#"{{"type":"{}","fromBlock":{from},"toBlock":{to},
                     "{key}":[{{"{filter}":{value}}}]}}"#,
                metadata.name
            )
            .into_bytes()
        };

        let as_list = run(chunk, &metadata, &query(true))
            .unwrap_or_else(|e| panic!("{dataset}.{filter} as a list: {e}"));
        let as_scalar = run(chunk, &metadata, &query(false))
            .unwrap_or_else(|e| panic!("{dataset}.{filter} as a scalar: {e}"));

        assert_eq!(
            count_items(&as_list, key),
            count_items(&as_scalar, key),
            "{dataset}.{key}.{filter}: a scalar and a one-element list must match the same rows"
        );
        assert!(
            as_list == as_scalar,
            "{dataset}.{key}.{filter}: responses differ"
        );

        if count_items(&as_list, key) > 0 {
            exercised += 1;
        }
    }

    assert!(
        exercised >= 4,
        "at least four of these filters must actually match rows, or the test proves nothing"
    );
}

/// A well-formed value that cannot fit the column is not an error — it matches
/// nothing (INV-P14), which is what the reference does too. Pinned so the
/// rejection above does not get widened into this.
///
/// Covers CT-3 · INV-P14
#[test]
#[ignore = "requires external fixture data"]
fn a_hex_value_too_wide_for_the_column_matches_nothing() {
    if !fixture_tree_is_present() {
        return;
    }

    let solana = meta("solana");
    let query = |filter: &str| {
        format!(
            r#"{{"type":"solana","fromBlock":217710049,"toBlock":217710050,
                 "instructions":[{{"d1":[{filter}]}}],
                 "fields":{{"block":{{"number":true}},"instruction":{{"programId":true}}}}}}"#
        )
        .into_bytes()
    };

    // The same filter with a value that does fit, so "zero" below means the value
    // was dropped rather than the range being empty.
    let fits = run("solana", &solana, &query(r#""0x02""#)).unwrap();
    assert!(
        count_items(&fits, "instructions") > 0,
        "the range must carry instructions"
    );

    let too_wide = run("solana", &solana, &query(r#""0x1234""#))
        .expect("a value that cannot fit is not a malformed value");
    assert_eq!(count_items(&too_wide, "instructions"), 0);

    // The same rule for a number past the column's physical width, in either
    // shape. It used to be an error for the scalar and a no-match for the list.
    for shape in ["256", "[256]"] {
        let body = run(
            "solana",
            &solana,
            format!(
                r#"{{"type":"solana","fromBlock":217710049,"toBlock":217710050,
                     "instructions":[{{"d1":{shape}}}],
                     "fields":{{"instruction":{{"programId":true}}}}}}"#
            )
            .as_bytes(),
        )
        .unwrap_or_else(|e| panic!("d1 = {shape} is out of range, not malformed: {e}"));

        assert_eq!(count_items(&body, "instructions"), 0);
    }

    // Wrong *kind*, on the other hand, is malformed and is refused.
    for shape in [r#""nonsense""#, r#"["nonsense"]"#, "true"] {
        let query = format!(
            r#"{{"type":"solana","fromBlock":217710049,"toBlock":217710050,
                 "instructions":[{{"d1":{shape}}}]}}"#
        );
        let compiled =
            parse_query(query.as_bytes(), &solana).and_then(|parsed| compile(&parsed, &solana));
        assert!(compiled.is_err(), "d1 = {shape} must be refused");
    }
}

/// INV-P14 covers the block bounds in the same breath as filter values, and the
/// bounds are the case where wrapping would be invisible: a chunk stores its
/// block numbers in 32 bits, so a `fromBlock` 2³² above one it holds truncates
/// to a block it does hold — and the client asked about neither.
///
/// Covers CT-3 · INV-P14
#[test]
#[ignore = "requires external fixture data"]
fn a_block_bound_past_the_stored_width_matches_nothing() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let query = |from: u64, to: u64| {
        format!(
            r#"{{"type":"evm","fromBlock":{from},"toBlock":{to},"includeAllBlocks":true,
                "fields":{{"block":{{"number":true}}}}}}"#
        )
        .into_bytes()
    };

    const FIRST: u64 = 17881390;
    const LAST: u64 = 17881391;
    const WIDTH: u64 = 1 << 32;

    let present = run("ethereum", &evm, &query(FIRST, LAST)).unwrap();
    assert!(!present.is_empty(), "the fixture must carry these blocks");

    let wrapped = run("ethereum", &evm, &query(FIRST + WIDTH, LAST + WIDTH))
        .expect("a bound past the stored width is not an error");
    assert!(
        wrapped.is_empty(),
        "a block bound must not wrap into a block the chunk holds"
    );
}

/// A bloom filter was the one filter shape in the engine that failed *open*.
/// Non-string elements were dropped and an empty needle set compiled to no
/// predicate at all, so `{"mentionsAccount": []}` returned every instruction in
/// range at 200 — the widest possible answer to the narrowest possible filter.
/// The reference marks an empty list `is_never`, which matches nothing.
#[test]
#[ignore = "requires external fixture data"]
fn an_empty_bloom_filter_matches_nothing_rather_than_everything() {
    if !fixture_tree_is_present() {
        return;
    }
    let solana = meta("solana");

    let query = |filter: &str| {
        format!(
            r#"{{"type":"solana","fromBlock":217710049,"toBlock":217710060,
                "fields":{{"instruction":{{"programId":true}}}},
                "instructions":[{{{filter}}}]}}"#
        )
        .into_bytes()
    };

    let unfiltered = run("solana", &solana, &query(r#""isCommitted":true"#)).unwrap();
    assert!(
        count_items(&unfiltered, "instructions") > 0,
        "the fixture must carry instructions for this to mean anything"
    );

    let empty_list = run("solana", &solana, &query(r#""mentionsAccount":[]"#)).unwrap();
    assert_eq!(
        count_items(&empty_list, "instructions"),
        0,
        "a filter no value can pass must match nothing, not everything"
    );
}

/// Pinned, not a defect: a *well-formed* value the column cannot hold matches
/// nothing rather than erroring (INV-P14). The reference parses `d8` with a
/// fixed-width hex reader and drops what does not fit, leaving an empty list that
/// its `PredicateBuilder` marks `is_never`. Both engines answer an empty 200.
#[test]
#[ignore = "requires external fixture data"]
fn a_hex_value_of_the_wrong_width_matches_nothing_without_erroring() {
    if !fixture_tree_is_present() {
        return;
    }
    let solana = meta("solana");

    let query = |d8: &str| {
        format!(
            r#"{{"type":"solana","fromBlock":217710049,"toBlock":217710060,
                "fields":{{"instruction":{{"programId":true}}}},
                "instructions":[{{"d8":["{d8}"]}}]}}"#
        )
        .into_bytes()
    };

    // Six bytes on an eight-byte column: well-formed hex, but not a `d8`.
    let narrow = run("solana", &solana, &query("0xf8c69e91e175"))
        .expect("a well-formed value that cannot match is not an error");
    assert_eq!(count_items(&narrow, "instructions"), 0);

    // Not hex at all, which is.
    assert!(run("solana", &solana, &query("zz")).is_err());
}

/// A signed column is filterable, negatives included.
///
/// The scalar path read `as_u64` and nothing else, and `compile_in_list` had no
/// signed arm, so `-1` on Solana's `transactions.version` — the sentinel every
/// legacy transaction carries — came back as "expected array, boolean, number,
/// or string" for a value that was a number. No bundled catalog declares such a
/// filter, so the surface is exercised here on a catalog that does.
///
/// Covers CT-3 · INV-P14
#[test]
fn a_signed_column_takes_negative_filter_values() {
    let (catalog, chunk) = signed_dataset();

    let matched = |filter: &str| -> Vec<i64> {
        let query = format!(
            r#"{{"type":"signed","fromBlock":10,"toBlock":12,
                "fields":{{"item":{{"seq":true}}}},
                "items":[{{{filter}}}]}}"#
        );
        seqs(&catalog, chunk.path(), &query)
    };

    // The sentinel, as a list and as the bare value it abbreviates.
    assert_eq!(matched(r#""version":[-1]"#), vec![0]);
    assert_eq!(matched(r#""version":-1"#), vec![0]);

    // A wider signed column, and a list mixing both signs.
    assert_eq!(matched(r#""lamports":[-4200]"#), vec![1]);
    assert_eq!(matched(r#""lamports":[-4200,7]"#), vec![1, 2]);

    // A value the declared width cannot hold matches nothing, and says so by
    // returning no rows rather than an error (INV-P14).
    assert_eq!(matched(r#""version":[-40000]"#), Vec::<i64>::new());

    // Hex on a signed column is a category error, not a value that fails to
    // match: dropping it would silently widen the list to its other elements.
    let hex = r#"{"type":"signed","fromBlock":10,"toBlock":12,"items":[{"version":["0xffff"]}]}"#;
    let err = plan_error(&catalog, hex);
    assert_eq!(error_kind(&err), Some(ErrorKind::InvalidFilterValue));
}

/// `"c": []` is `r[c] ∈ ∅`, which is false for every row. Reading it as "no
/// constraint" turns "match none of these" into "match every row in the chunk" —
/// the single most destructive misreading available. The discriminator was the
/// one filter that refused it outright instead, so a client asking for nothing
/// got a 400 where the rest of the surface answered emptily.
///
/// Covers CT-3 · INV-P3
#[test]
#[ignore = "requires external fixture data"]
fn an_empty_filter_list_matches_nothing() {
    if !fixture_tree_is_present() {
        return;
    }
    let solana = meta("solana");
    let evm = meta("evm");

    let instructions = |filter: &str| {
        format!(
            r#"{{"type":"solana","fromBlock":0,
                "fields":{{"instruction":{{"programId":true}}}},
                "instructions":[{{{filter}}}]}}"#
        )
        .into_bytes()
    };

    let some = run("solana", &solana, &instructions(r#""isCommitted":true"#)).unwrap();
    assert!(
        count_items(&some, "instructions") > 0,
        "the fixture must carry instructions for the empty-list case to mean anything"
    );

    for filter in [
        r#""discriminator":[]"#,
        r#""discriminator":[],"isCommitted":true"#,
        r#""d8":[]"#,
        r#""programId":[]"#,
    ] {
        let out = run("solana", &solana, &instructions(filter))
            .unwrap_or_else(|e| panic!("{filter} must be answered, not refused: {e}"));
        assert_eq!(
            count_items(&out, "instructions"),
            0,
            "{filter} must match nothing"
        );
    }

    // An empty list next to a filter that does match: the empty one still sinks
    // the item, rather than being conjoined away.
    let logs = |filter: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                "fields":{{"log":{{"address":true}}}},
                "logs":[{{{filter}}}]}}"#
        )
        .into_bytes()
    };

    let topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
    let matching = run("ethereum", &evm, &logs(&format!(r#""topic0":["{topic}"]"#))).unwrap();
    assert!(
        count_items(&matching, "logs") > 0,
        "the fixture must carry transfer logs"
    );

    let sunk = run(
        "ethereum",
        &evm,
        &logs(&format!(r#""address":[],"topic0":["{topic}"]"#)),
    )
    .expect("an empty list is not an error");
    assert_eq!(
        count_items(&sunk, "logs"),
        0,
        "an empty list sinks the item whatever its other filters say"
    );
}

/// A three-row chunk whose two logically signed filterable columns use unsigned
/// physical storage, with a negative value in each.
fn signed_dataset() -> (DatasetDescription, tempfile::TempDir) {
    use arrow::array::{ArrayRef, UInt16Array, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use sqd_query_engine::metadata::parse_dataset_description;
    use std::sync::Arc;

    let catalog = parse_dataset_description(
        r#"
name: signed
tables:
  blocks:
    field_name: block
    block_number_column: number
    sort_key: [number]
    filters: []
    fields: [number]
    columns:
      number: { type: uint64 }
  items:
    query_name: items
    field_name: item
    block_number_column: block_number
    item_order_keys: [seq]
    sort_key: [block_number, seq]
    filters: [version, lamports]
    fields: [seq, version, lamports]
    columns:
      block_number: { type: uint64 }
      seq: { type: uint32 }
      version: { type: int16 }
      lamports: { type: int64 }
"#,
    )
    .unwrap();

    let dir = tempfile::tempdir().unwrap();

    let items_schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("seq", DataType::UInt32, false),
        Field::new("version", DataType::UInt16, false),
        Field::new("lamports", DataType::UInt64, false),
    ]));
    let items = RecordBatch::try_new(
        items_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![10u64, 11, 12])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32, 1, 2])) as ArrayRef,
            Arc::new(UInt16Array::from(vec![u16::MAX, 0, 1])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![0, u64::MAX - 4_199, 7])) as ArrayRef,
        ],
    )
    .unwrap();

    let blocks_schema = Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::UInt64,
        false,
    )]));
    let blocks = RecordBatch::try_new(
        blocks_schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![10u64, 11, 12])) as ArrayRef],
    )
    .unwrap();

    for (name, batch) in [("items", items), ("blocks", blocks)] {
        write_parquet(&dir.path().join(format!("{name}.parquet")), &batch);
    }

    (catalog, dir)
}

/// The `seq` of every item a query returns, in response order.
fn seqs(catalog: &DatasetDescription, chunk: &Path, query: &str) -> Vec<i64> {
    let body = run_against(catalog, chunk, query).expect("query must be answerable");

    parse_response(&body)
        .iter()
        .flat_map(|block| items_in(block, "items"))
        .map(|item| item["seq"].as_i64().unwrap())
        .collect()
}
