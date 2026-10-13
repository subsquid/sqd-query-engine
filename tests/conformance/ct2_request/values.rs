//! What a request value must look like. A value of the wrong kind is refused, a
//! malformed hex string is answered as written, and neither is coerced into a
//! value the client did not write.

use serde_json::json;
use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::query::{compile, parse_query};

use crate::harness::fixtures::{bundled_catalogs, fixture_tree_has, meta, plan_error, run};
use crate::harness::hex::{hex_filters, malformed};
use crate::harness::sol_like;

/// A malformed hex value is answered, not refused, on every filter of every
/// bundled catalog that takes hex: beside a well-formed value, alone, and as the
/// bare value a one-element list abbreviates. Clients send them — a topic a digit
/// short, an address a digit over — and the same ones query after query.
///
/// Covers CT-2 · INV-Q12
#[test]
fn a_malformed_hex_value_is_not_refused() {
    let mut tried = 0;

    for catalog in bundled_catalogs() {
        for filter in hex_filters(&catalog) {
            let digits = filter.digits.unwrap_or(40);
            let well_formed = format!("0x{}", "ab".repeat(digits / 2));

            for (how, bad) in malformed(&well_formed) {
                for shape in [json!([well_formed, bad]), json!([bad]), json!(bad)] {
                    let query = json!({
                        "type": catalog.name,
                        "fromBlock": 0,
                        (filter.request.clone()): [{ (filter.key.clone()): shape }],
                    })
                    .to_string();

                    parse_query(query.as_bytes(), &catalog)
                        .and_then(|parsed| compile(&parsed, &catalog))
                        .unwrap_or_else(|e| {
                            panic!(
                                "{}.{} (column {}.{}) with {how} ({shape}) must be \
                                 answered: {e:#}",
                                filter.request, filter.key, filter.table, filter.column
                            )
                        });
                    tried += 1;
                }
            }
        }
    }

    // EVM alone has seventeen hex filters; far fewer means the walk lost a catalog.
    assert!(tried > 500, "only {tried} queries were tried");
}

/// The discriminator is the hex filter that still checks form. Its values are
/// prefixes the client composes, so a malformed one is refused even beside a
/// well-formed one, and so is one longer than any discriminator column.
///
/// Covers CT-2 · INV-Q12
#[test]
fn a_malformed_discriminator_is_refused() {
    let solana = meta("solana");
    let prefix = "0xf8c69e91e17587c8";

    for (how, bad) in malformed(prefix) {
        let query = json!({
            "type": "solana",
            "fromBlock": 0,
            "instructions": [{ "discriminator": [prefix, bad] }],
        })
        .to_string();

        let err = plan_error(&solana, &query);
        assert_eq!(
            error_kind(&err),
            Some(ErrorKind::InvalidHex),
            "a discriminator with {how}: {err:#}"
        );
    }

    let too_long = format!("0x{}", "ab".repeat(17));
    let query = json!({
        "type": "solana",
        "fromBlock": 0,
        "instructions": [{ "discriminator": [prefix, too_long] }],
    })
    .to_string();
    let err = plan_error(&solana, &query);
    assert_eq!(error_kind(&err), Some(ErrorKind::DiscriminatorTooLong));
}

/// A zero-length prefix matches every instruction, but it does not end the check
/// of the list it sits in. Each bad list below is refused in either order: the
/// answer cannot depend on whether `0x` comes first. The synthetic catalog
/// declares columns for three lengths only, so a four-byte prefix has nowhere
/// to go.
///
/// Covers CT-2 · INV-Q12
#[test]
fn a_discriminator_is_checked_in_full_whatever_its_order() {
    let solana = meta("solana");
    let sol_like = sol_like::catalog();
    let too_long = format!("0x{}", "ab".repeat(17));

    let mut refused = vec![
        (&solana, json!(1), ErrorKind::InvalidFilterValue),
        (&solana, json!(null), ErrorKind::InvalidFilterValue),
        (&solana, json!(too_long), ErrorKind::DiscriminatorTooLong),
        (
            &sol_like,
            json!("0x01020304"),
            ErrorKind::InvalidFilterValue,
        ),
    ];
    for (_, bad) in malformed("0xf8c69e91e17587c8") {
        refused.push((&solana, json!(bad), ErrorKind::InvalidHex));
    }

    let mut wrong = Vec::new();
    for (catalog, bad, kind) in refused {
        for list in [json!(["0x", bad]), json!([bad, "0x"])] {
            let query = json!({
                "type": catalog.name,
                "fromBlock": 0,
                "instructions": [{ "discriminator": list }],
            })
            .to_string();

            match parse_query(query.as_bytes(), catalog).and_then(|p| compile(&p, catalog)) {
                Err(err) if error_kind(&err) == Some(kind) => {}
                Err(err) => wrong.push(format!("{list}: {err:#}, not {kind:?}")),
                Ok(_) => wrong.push(format!("{list}: accepted, not {kind:?}")),
            }
        }
    }

    assert!(
        wrong.is_empty(),
        "{} discriminator lists are not refused as they should be:\n  - {}",
        wrong.len(),
        wrong.join("\n  - ")
    );
}

/// A hex filter answers a malformed *string*. A value that is no string at all is
/// a different mistake, and still refused: a boolean or a fraction in a hex list,
/// a number on a column stored as text, a word where a list of integers goes.
///
/// Covers CT-2 · INV-Q12
#[test]
fn a_hex_filter_refuses_a_value_that_is_not_a_string() {
    let refused = [
        ("solana", "instructions", "d8", json!([true])),
        (
            "solana",
            "instructions",
            "d8",
            json!(["0xf8c69e91e17587c8", 1.5]),
        ),
        ("evm", "logs", "address", json!([1])),
        ("evm", "logs", "topic0", json!([null])),
        (
            "hyperliquid_replica_cmds",
            "orderActions",
            "containsAsset",
            json!(["not-a-number"]),
        ),
    ];

    for (catalog, request, key, value) in refused {
        let metadata = meta(catalog);
        let query = json!({
            "type": metadata.name,
            "fromBlock": 0,
            (request): [{ (key): value }],
        })
        .to_string();

        let err = plan_error(&metadata, &query);
        assert_eq!(
            error_kind(&err),
            Some(ErrorKind::InvalidFilterValue),
            "{request}.{key} = {value}: {err:#}"
        );
    }
}

/// Hex parsing used to walk the value two characters at a time, and a multi-byte
/// character landing across one of those boundaries split it and took the whole
/// query thread down with it. On a column filter the value is malformed and
/// matches nothing; in a discriminator it is refused, and the refusal says why.
#[test]
fn a_non_ascii_hex_value_is_not_a_panic() {
    let solana = meta("solana");

    for value in [
        "0x\u{20ac}1",
        "0x\u{20ac}\u{20ac}",
        "0xff\u{20ac}1",
        "0x\u{4e2d}\u{6587}",
    ] {
        let column = json!({
            "type": "solana",
            "fromBlock": 406021645,
            "toBlock": 406021646,
            "instructions": [{ "d8": [value] }],
        })
        .to_string();
        parse_query(column.as_bytes(), &solana)
            .and_then(|parsed| compile(&parsed, &solana))
            .unwrap_or_else(|e| panic!("d8 = {value:?} matches nothing, not an error: {e:#}"));

        let discriminator = json!({
            "type": "solana",
            "fromBlock": 406021645,
            "toBlock": 406021646,
            "instructions": [{ "discriminator": [value] }],
        })
        .to_string();
        let err = plan_error(&solana, &discriminator);
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
    let query = r#"{"type":"solana","fromBlock":217710049,"toBlock":217710060,
        "instructions":[{"mentionsAccount":[123]}]}"#;

    // Refused before any chunk is read, so the test needs none: an error from a
    // missing chunk would pass an `is_err` check just as well.
    let error = format!("{:#}", plan_error(&solana, query));
    assert!(error.contains("expected a string"), "{error}");
}

/// `callValueNonZero` and its siblings used to read any non-boolean as "off",
/// answering a strictly wider question than the one asked. The reference types
/// the field `bool` and refuses all four shapes below — as this engine already
/// did for `includeAllBlocks` and the block bounds.
#[test]
#[ignore = "requires external fixture data"]
fn a_non_boolean_flag_filter_is_rejected() {
    if !fixture_tree_has("ethereum") {
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
    if !fixture_tree_has("ethereum") {
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
