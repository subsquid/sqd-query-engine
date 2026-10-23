//! The bounds cost, not correctness, motivates: a request the engine will answer
//! must be one it can afford to answer.

use serde_json::{json, Map, Value};
use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::sets::MAX_SET_REFERENCES;

use crate::harness::fixtures::meta;
use crate::harness::sets::BuiltSet;

/// Each bloom value is a separate hash-and-probe over every row, so the length
/// of the list is a cost multiplier the client picks. `P-MAX-BLOOM-VALUES` caps
/// it at ten.
///
/// Covers CT-2 · INV-Q10
#[test]
fn a_bloom_filter_takes_at_most_ten_values() {
    let solana = meta("solana");

    let query = |count: usize| {
        let values: Vec<String> = (0..count).map(|i| format!("\"account{i}\"")).collect();
        format!(
            r#"{{"type":"solana","fromBlock":0,
                "transactions":[{{"mentionsAccount":[{}]}}]}}"#,
            values.join(",")
        )
        .into_bytes()
    };

    let compiles = |json: &[u8]| {
        let parsed = parse_query(json, &solana)?;
        compile(&parsed, &solana).map(|_| ())
    };

    compiles(&query(10)).expect("ten values must be accepted");
    let err = compiles(&query(11))
        .expect_err("eleven values must be refused")
        .to_string();
    assert!(err.contains("mentions_account"), "got: {err}");
}

/// `discriminator`, `d1` and `d8` narrow one column family. Two of them in one
/// item request ask two different questions of it, and which one holds was
/// decided by the order the filters happened to be read in.
///
/// Covers CT-2 · INV-Q11
#[test]
fn one_discriminator_filter_per_item_request() {
    let solana = meta("solana");

    let query = |filters: &str| {
        format!(r#"{{"type":"solana","fromBlock":0,"instructions":[{{{filters}}}]}}"#).into_bytes()
    };

    let compiles = |json: &[u8]| {
        let parsed = parse_query(json, &solana)?;
        compile(&parsed, &solana).map(|_| ())
    };

    for accepted in [
        r#""d8":["0xf8c69e91e17587c8"]"#,
        r#""discriminator":["0xf8c69e91e17587c8"]"#,
        r#""d1":["0xf8"],"programId":["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"]"#,
    ] {
        compiles(&query(accepted))
            .unwrap_or_else(|e| panic!("{accepted} is one discriminator filter: {e}"));
    }

    for refused in [
        r#""d1":["0xf8"],"d8":["0xf8c69e91e17587c8"]"#,
        r#""discriminator":["0xf8"],"d8":["0xf8c69e91e17587c8"]"#,
    ] {
        assert!(
            compiles(&query(refused)).is_err(),
            "{refused} narrows one column family twice and must be refused"
        );
    }
}

/// The item-request cap bounds how many scans a request asks for, not how much
/// memory it builds first. Every list filter becomes a hash set before a row is
/// read, so `P-MAX-IN-LIST` separately caps the collection built for one filter
/// (ADR-13).
///
/// Covers CT-2 · INV-Q13
#[test]
fn an_in_list_is_bounded_in_length() {
    let evm = meta("evm");

    // Short values, so the request stays under the byte cap and the length is
    // the only bound the test can be measuring.
    let query = |count: usize| {
        let values = std::iter::repeat_n(r#""0x01""#, count)
            .collect::<Vec<_>>()
            .join(",");
        format!(r#"{{"type":"evm","fromBlock":0,"logs":[{{"address":[{values}]}}]}}"#).into_bytes()
    };

    let at_the_cap = query(100_000);
    assert!(at_the_cap.len() < 2 * 1024 * 1024);
    parse_query(&at_the_cap, &evm).expect("a list at the cap must be accepted");

    let err = parse_query(&query(100_001), &evm)
        .expect_err("a list over the cap must be refused")
        .to_string();
    assert!(err.contains("address"), "got: {err}");
}

/// The other half of the same bound, and the one that has to be checked against
/// the raw body: parsing a request is the first thing its size costs.
///
/// Covers CT-2 · INV-Q13
#[test]
fn a_request_is_bounded_in_bytes() {
    let evm = meta("evm");

    let query = |padding: usize| {
        format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{"address":["0x{}"]}}]}}"#,
            "ab".repeat(padding)
        )
        .into_bytes()
    };

    let under = query(700_000);
    assert!(under.len() < 2 * 1024 * 1024);
    parse_query(&under, &evm).expect("a request under the cap must be parsed");

    let over = query(1_200_000);
    assert!(over.len() > 2 * 1024 * 1024);
    let err = parse_query(&over, &evm)
        .expect_err("a request over the cap must be refused")
        .to_string();
    assert!(err.contains("bytes"), "got: {err}");
}

/// Each set reference is a lookup for every row its table scans, and a set
/// declared under many names is many lookups. `P-MAX-SET-REFERENCES` caps the
/// references a request makes, summed across every list of every item request,
/// a name counted once per list.
///
/// Covers CT-2 · INV-Q13
#[test]
fn set_references_are_bounded_in_number() {
    let evm = meta("evm");

    // Every set is the same structure under its own name, which is what makes
    // this cheap to send.
    let value = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc";
    let set = BuiltSet::of(&[value]).declared(&[value]);
    let reference = |i: usize| json!({ "set": format!("s{i}") });
    let outcome = |lists: Vec<Vec<Value>>| {
        let declared: Map<String, Value> = lists
            .iter()
            .flatten()
            .filter_map(|r| r["set"].as_str())
            .map(|name| (name.to_owned(), set.clone()))
            .collect();
        let items: Vec<Value> = lists
            .into_iter()
            .map(|list| json!({ "address": list }))
            .collect();
        let query = json!({
            "type": "evm",
            "sets": declared,
            "fields": { "log": { "address": true } },
            "logs": items,
        })
        .to_string();

        let err = parse_query(query.as_bytes(), &evm)
            .and_then(|parsed| compile(&parsed, &evm))
            .err()?;
        Some(error_kind(&err).unwrap_or_else(|| panic!("refused without a kind: {err:#}")))
    };

    let one_list = |count: usize| vec![(0..count).map(reference).collect()];
    assert_eq!(outcome(one_list(MAX_SET_REFERENCES)), None);
    assert_eq!(
        outcome(one_list(MAX_SET_REFERENCES + 1)),
        Some(ErrorKind::RequestTooLarge)
    );

    // Two references per item, so the count runs across items.
    let spread = |count: usize| {
        (0..count)
            .step_by(2)
            .map(|i| (i..count.min(i + 2)).map(reference).collect())
            .collect()
    };
    assert_eq!(outcome(spread(MAX_SET_REFERENCES)), None);
    assert_eq!(
        outcome(spread(MAX_SET_REFERENCES + 1)),
        Some(ErrorKind::RequestTooLarge)
    );

    // One set, referred to from more items than the cap: each is a lookup.
    let one_set_everywhere = vec![vec![reference(0)]; MAX_SET_REFERENCES + 1];
    assert_eq!(
        outcome(one_set_everywhere),
        Some(ErrorKind::RequestTooLarge)
    );

    // A name written again in one list is one lookup.
    let repeated = vec![vec![reference(0); 10 * MAX_SET_REFERENCES]];
    assert_eq!(outcome(repeated), None);
}
