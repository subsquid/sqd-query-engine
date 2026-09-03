//! A malformed value is refused, never coerced.

use sqd_query_engine::query::{compile, parse_query};

use crate::harness::fixtures::{fixture_tree_is_present, meta, run};

/// A malformed hex value is an error, not a silently narrowed filter. The
/// dangerous shape is a list where one of several values is malformed: it used
/// to become a filter on the survivors, which returns a plausible answer to a
/// query the client never wrote.
///
/// Covers CT-2 · INV-Q12
#[test]
fn malformed_hex_in_list_is_an_error() {
    let solana = meta("solana");
    let malformed = [
        r#"["0xf8c69e91e17587c8", "f8c69e91e17587c8"]"#, // missing 0x on the second
        r#"["0xabc"]"#,                                  // odd digit count
        r#"["0xzz"]"#,                                   // not hex
        r#"[true]"#,                                     // neither string nor integer
    ];
    for values in malformed {
        let json =
            format!(r#"{{"type":"solana","fromBlock":0,"instructions":[{{"d8":{values}}}]}}"#);
        let parsed = parse_query(json.as_bytes(), &solana).unwrap();
        assert!(
            compile(&parsed, &solana).is_err(),
            "expected an error for d8 filter {values}"
        );
    }

    // The same on a list column, reached through an alias.
    let replica = meta("hyperliquid_replica_cmds");
    let json = r#"{"type":"hyperliquidReplicaCmds","fromBlock":0,
                   "orderActions":[{"containsAsset":["not-a-number"]}]}"#;
    let parsed = parse_query(json.as_bytes(), &replica).unwrap();
    assert!(
        compile(&parsed, &replica).is_err(),
        "a non-integer in a list_uint32 filter must be an error"
    );
}

/// Hex parsing walks the value two characters at a time. A multi-byte character
/// landing across one of those boundaries used to split it and take the whole
/// query thread down with it; the value is simply not hex, and saying so is the
/// entire job.
#[test]
fn a_non_ascii_hex_value_is_rejected_not_a_panic() {
    let solana = meta("solana");

    for value in [
        "0x\u{20ac}1",
        "0x\u{20ac}\u{20ac}",
        "0xff\u{20ac}1",
        "0x\u{4e2d}\u{6587}",
    ] {
        let query = format!(
            r#"{{"type":"solana","fromBlock":406021645,"toBlock":406021646,
                 "instructions":[{{"d8":["{value}"]}}]}}"#
        );
        let err = parse_query(query.as_bytes(), &solana)
            .and_then(|parsed| compile(&parsed, &solana))
            .err()
            .unwrap_or_else(|| panic!("{value:?} is not hex and must be refused"));

        assert!(
            err.to_string().contains("hex"),
            "the error must say the value is not hex, got: {err}"
        );
    }
}

/// A field selector is a boolean. `{"logIndex": 1}` is as much a mistake as
/// `{"logIndx": true}`, and answering it with a 200 that quietly omits the
/// column sends the client looking for the bug everywhere except in its request.
/// The reference rejects it: `invalid type: integer 1, expected a boolean`.
#[test]
fn a_non_boolean_field_selector_is_rejected() {
    let evm = meta("evm");

    for selector in ["1", "\"true\"", "null", "[]", "{}"] {
        let query = format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881390,
                 "logs":[{{}}],"fields":{{"log":{{"logIndex":{selector},"address":true}}}}}}"#
        );
        assert!(
            parse_query(query.as_bytes(), &evm).is_err(),
            "fields.log.logIndex = {selector} must be refused"
        );
    }

    parse_query(
        br#"{"type":"evm","fromBlock":17881390,"toBlock":17881390,
             "logs":[{}],"fields":{"log":{"logIndex":false,"address":true}}}"#,
        &evm,
    )
    .expect("an explicit `false` is a valid selector");
}

/// `includeAllBlocks` decides whether the response carries every block in range
/// or only the ones with matches — the difference between two very different
/// answers. It was read with `.unwrap_or(false)`, so a wrong type picked one of
/// them silently, right next to `fromBlock`/`toBlock` which refuse the same shape.
#[test]
fn a_non_boolean_include_all_blocks_is_rejected() {
    let evm = meta("evm");

    for value in ["\"true\"", "1", "[]"] {
        let query = format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                 "includeAllBlocks":{value},"fields":{{"block":{{"number":true}}}}}}"#
        );
        assert!(
            parse_query(query.as_bytes(), &evm).is_err(),
            "includeAllBlocks = {value} must be refused"
        );
    }

    for value in ["true", "false", "null"] {
        let query = format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                 "includeAllBlocks":{value},"fields":{{"block":{{"number":true}}}}}}"#
        );
        parse_query(query.as_bytes(), &evm)
            .unwrap_or_else(|e| panic!("includeAllBlocks = {value} must be accepted: {e}"));
    }
}

/// The same filter rejects an element that is not a string, rather than dropping
/// it and narrowing to whatever survived. The reference types the field as a list
/// of strings and refuses the request.
#[test]
fn a_bloom_filter_rejects_a_non_string_element() {
    let solana = meta("solana");
    let query = br#"{"type":"solana","fromBlock":217710049,"toBlock":217710060,
        "instructions":[{"mentionsAccount":[123]}]}"#;

    assert!(
        run("solana", &solana, query).is_err(),
        "a numeric account is a client error, not a filter on the rest of the list"
    );
}

/// Most of the engine's hex surface is string-typed — every EVM address, topic,
/// hash and sighash — and a malformed value there compares unequal to every
/// stored value. The answer is 200 with no rows and nothing to say why, which is
/// the failure INV-Q12 exists to prevent.
#[test]
#[ignore = "requires external fixture data"]
fn a_malformed_hex_filter_is_rejected_on_a_string_column() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");

    let query = |address: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                "fields":{{"log":{{"address":true}}}},
                "logs":[{{"address":["{address}"]}}]}}"#
        )
        .into_bytes()
    };

    // Well-formed, and accepted whether or not it matches anything.
    assert!(run(
        "ethereum",
        &evm,
        &query("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
    )
    .is_ok());

    for malformed in [
        "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",   // no 0x
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb4",  // odd length
        "0xZZb86991c6218b36c1d19d4a2e9eb0ce3606eb48", // not hex digits
    ] {
        assert!(
            run("ethereum", &evm, &query(malformed)).is_err(),
            "{malformed} must be refused, not answered with an empty 200"
        );
    }
}

/// `callValueNonZero` and its siblings used to read any non-boolean as "off",
/// answering a strictly wider question than the one asked. The reference types
/// the field `bool` and refuses all four shapes below — as this engine already
/// did for `includeAllBlocks` and the block bounds.
#[test]
#[ignore = "requires external fixture data"]
fn a_non_boolean_flag_filter_is_rejected() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");

    let query = |value: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                "fields":{{"trace":{{"type":true}}}},
                "traces":[{{"type":["call"],"callValueNonZero":{value}}}]}}"#
        )
        .into_bytes()
    };

    assert!(run("ethereum", &evm, &query("true")).is_ok());
    assert!(run("ethereum", &evm, &query("false")).is_ok());

    for malformed in ["\"true\"", "1", "[]", "null", "{}"] {
        assert!(
            run("ethereum", &evm, &query(malformed)).is_err(),
            "callValueNonZero: {malformed} must be refused, not read as 'off'"
        );
    }
}

/// A `fields` of the wrong type read as absent, so `{"fields": []}` answered 200
/// with every projection the client asked for missing — which reads as a dataset
/// that has no such columns. A non-object *inside* `fields` was already refused,
/// and so are the block bounds and `includeAllBlocks` next to it.
#[test]
#[ignore = "requires external fixture data"]
fn a_non_object_fields_is_rejected() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");

    let query = |fields: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                "fields":{fields},"logs":[{{}}]}}"#
        )
        .into_bytes()
    };

    assert!(run("ethereum", &evm, &query(r#"{"log":{"address":true}}"#)).is_ok());
    assert!(run("ethereum", &evm, &query("{}")).is_ok());
    assert!(run("ethereum", &evm, &query("null")).is_ok());

    for malformed in ["[]", "false", "0", r#""log""#] {
        assert!(
            run("ethereum", &evm, &query(malformed)).is_err(),
            "'fields': {malformed} must be refused, not read as no selection"
        );
    }
}
