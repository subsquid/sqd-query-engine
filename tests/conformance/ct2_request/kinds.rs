//! An error carries a kind, and the kind is what a client reads.

use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::query::{compile, parse_query};

use crate::harness::fixtures::meta;

/// The first error a request produces, with its kind.
fn refusal(metadata: &DatasetDescription, json: &str) -> (ErrorKind, String) {
    let err = parse_query(json.as_bytes(), metadata)
        .and_then(|parsed| compile(&parsed, metadata).map(|_| ()))
        .expect_err("this request must be refused");

    let kind = error_kind(&err)
        .unwrap_or_else(|| panic!("refused without a kind: {err}\n  request: {json}"));

    (kind, err.to_string())
}

/// One case per row of §6.2, asserting the *kind* rather than a substring of
/// the message. A client library that switches on message text breaks the day
/// someone improves the wording, and improving the wording is the one thing §6
/// says is always allowed.
///
/// Covers CT-2 · INV-E6
#[test]
fn every_validation_error_carries_its_kind() {
    let cases: &[(&str, &str, ErrorKind)] = &[
        ("evm", r#"{"fromBlock":0}"#, ErrorKind::UnknownDataset),
        (
            "evm",
            r#"{"type":"eth","fromBlock":0}"#,
            ErrorKind::UnknownDataset,
        ),
        ("evm", r#"[]"#, ErrorKind::MalformedRequest),
        (
            "evm",
            r#"{"type":"evm","logs":{}}"#,
            ErrorKind::MalformedRequest,
        ),
        (
            "evm",
            r#"{"type":"evm","fields":{"log":{"logIndex":1}}}"#,
            ErrorKind::MalformedRequest,
        ),
        (
            "evm",
            r#"{"type":"evm","logz":[]}"#,
            ErrorKind::UnknownTable,
        ),
        (
            "evm",
            r#"{"type":"evm","logs":[{"dataSize":[1]}]}"#,
            ErrorKind::UnknownFilter,
        ),
        (
            "evm",
            r#"{"type":"evm","fields":{"lgo":{}}}"#,
            ErrorKind::UnknownFieldGroup,
        ),
        (
            "evm",
            r#"{"type":"evm","fields":{"log":{"logIndx":true}}}"#,
            ErrorKind::UnknownField,
        ),
        (
            "evm",
            r#"{"type":"evm","fromBlock":10,"toBlock":5}"#,
            ErrorKind::InvalidBlockRange,
        ),
        (
            "evm",
            r#"{"type":"evm","fromBlock":"abc"}"#,
            ErrorKind::InvalidBlockNumber,
        ),
        (
            "evm",
            r#"{"type":"evm","fromBlock":-1}"#,
            ErrorKind::InvalidBlockNumber,
        ),
        (
            "evm",
            r#"{"type":"evm","fromBlock":1.5}"#,
            ErrorKind::InvalidBlockNumber,
        ),
        (
            "solana",
            r#"{"type":"solana","instructions":[{"d1":["0x01"],"d8":["0x0102030405060708"]}]}"#,
            ErrorKind::ConflictingFilters,
        ),
        (
            "solana",
            r#"{"type":"solana","instructions":[{"discriminator":["0xabc"]}]}"#,
            ErrorKind::InvalidHex,
        ),
        (
            "solana",
            r#"{"type":"solana","instructions":[
                {"discriminator":["0x0102030405060708090a0b0c0d0e0f1011"]}]}"#,
            ErrorKind::DiscriminatorTooLong,
        ),
        (
            "evm",
            r#"{"type":"evm","logs":[{"address":1.5}]}"#,
            ErrorKind::InvalidFilterValue,
        ),
        (
            "evm",
            r#"{"type":"evm","logs":[{"transaction":1}]}"#,
            ErrorKind::InvalidFilterValue,
        ),
    ];

    for (dataset, json, expected) in cases {
        let (kind, message) = refusal(&meta(dataset), json);
        assert_eq!(kind, *expected, "{json} was refused as {kind}: {message}");
    }
}

/// The three bounds, which need a request too big to write inline.
///
/// Covers CT-2 · INV-E6
#[test]
fn every_request_bound_carries_its_kind() {
    let evm = meta("evm");
    let solana = meta("solana");

    let items = std::iter::repeat_n(r#"{"address":["0x00"]}"#, 101)
        .collect::<Vec<_>>()
        .join(",");
    let (kind, _) = refusal(
        &evm,
        &format!(r#"{{"type":"evm","fromBlock":0,"logs":[{items}]}}"#),
    );
    assert_eq!(kind, ErrorKind::TooManyItemRequests);

    let accounts = std::iter::repeat_n(r#""account""#, 11)
        .collect::<Vec<_>>()
        .join(",");
    let (kind, _) = refusal(
        &solana,
        &format!(r#"{{"type":"solana","transactions":[{{"mentionsAccount":[{accounts}]}}]}}"#),
    );
    assert_eq!(kind, ErrorKind::TooManyBloomValues);

    let addresses = std::iter::repeat_n(r#""0x01""#, 100_001)
        .collect::<Vec<_>>()
        .join(",");
    let (kind, _) = refusal(
        &evm,
        &format!(r#"{{"type":"evm","logs":[{{"address":[{addresses}]}}]}}"#),
    );
    assert_eq!(kind, ErrorKind::RequestTooLarge);
}

/// The one reserved key a dataset can be unable to honour, which needs a catalog
/// whose block table declares no parent-hash column.
///
/// Covers CT-2 · INV-E6
#[test]
fn an_unanswerable_reserved_key_carries_its_kind() {
    use sqd_query_engine::metadata::parse_dataset_description;

    let silent = parse_dataset_description(
        r#"
name: test

tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
      hash: { type: string }
"#,
    )
    .unwrap();

    let (kind, _) = refusal(
        &silent,
        r#"{"type":"test","fromBlock":10,"parentBlockHash":"0xabcd"}"#,
    );
    assert_eq!(kind, ErrorKind::UnsupportedRequestField);
}
