//! Request-surface properties: whatever arrives, the engine answers or refuses.
//!
//! Every value here reaches the engine straight off the network. The property is
//! not that any particular value is accepted — most are nonsense — but that a
//! value is never able to take the thread down instead of producing an error.

use proptest::prelude::*;
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::query::{compile, parse_query};
use std::path::Path;
use std::sync::OnceLock;

fn evm() -> &'static DatasetDescription {
    static META: OnceLock<DatasetDescription> = OnceLock::new();
    META.get_or_init(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap())
}

fn solana() -> &'static DatasetDescription {
    static META: OnceLock<DatasetDescription> = OnceLock::new();
    META.get_or_init(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap())
}

/// Parse and compile, discarding the outcome: the test is that we get one.
fn settle(query: &str, metadata: &DatasetDescription) {
    if let Ok(parsed) = parse_query(query.as_bytes(), metadata) {
        let _ = compile(&parsed, metadata);
    }
}

/// JSON-escape a generated string so the query stays well-formed and the value
/// under test reaches the filter rather than the JSON parser.
fn json(value: &str) -> String {
    serde_json::to_string(value).unwrap()
}

/// Values shaped like hex, because that is the only shape that gets past the
/// `0x` prefix check and into the parsing itself. An unbiased string generator
/// produces essentially none of them and exercises nothing.
fn hexish() -> impl Strategy<Value = String> {
    prop_oneof![
        "0[xX][0-9a-fA-F]*",
        "0[xX].*",
        "0[xX][0-9a-fA-F]*.[0-9a-fA-F]*",
        ".*",
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    /// Hex-typed filters: discriminators, fixed-width binary, and the byte-pair
    /// walk inside them.
    #[test]
    fn no_hex_filter_value_panics(value in hexish()) {
        let v = json(&value);
        settle(&format!(
            r#"{{"type":"solana","fromBlock":0,"instructions":[{{"d8":[{v}]}}]}}"#), solana());
        settle(&format!(
            r#"{{"type":"solana","fromBlock":0,"instructions":[{{"d1":[{v}]}}]}}"#), solana());
        settle(&format!(
            r#"{{"type":"solana","fromBlock":0,"instructions":[{{"discriminator":[{v}]}}]}}"#),
            solana());
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{"topic0":[{v}]}}]}}"#), evm());
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"transactions":[{{"sighash":[{v}]}}]}}"#), evm());
    }

    /// String-typed filters take the value as written, but still walk it.
    #[test]
    fn no_string_filter_value_panics(value in hexish()) {
        let v = json(&value);
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{"address":[{v}]}}]}}"#), evm());
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"traces":[{{"type":[{v}]}}]}}"#), evm());
        settle(&format!(
            r#"{{"type":"solana","fromBlock":0,"instructions":[{{"programId":[{v}]}}]}}"#),
            solana());
    }

    /// Keys are turned into column names before anything looks them up.
    #[test]
    fn no_filter_or_field_name_panics(name in ".*") {
        let n = json(&name);
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{{n}:["0x00"]}}]}}"#), evm());
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{}}],"fields":{{"log":{{{n}:true}}}}}}"#),
            evm());
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{}}],"fields":{{{n}:{{"address":true}}}}}}"#),
            evm());
    }
}

/// The scalars a client can put anywhere: wrong types are refused, but the
/// refusal is an error, not a panic.
#[test]
fn no_scalar_shape_panics_in_a_top_level_slot() {
    const SHAPES: &[&str] = &[
        "null",
        "true",
        "false",
        "0",
        "-1",
        "1.5",
        "1e400",
        "18446744073709551616",
        r#""""#,
        r#""0x""#,
        r#""0x0""#,
        r#""nonsense""#,
        "[]",
        "{}",
        r#"[null]"#,
        r#"[[]]"#,
    ];

    for shape in SHAPES {
        for slot in [
            "fromBlock",
            "toBlock",
            "includeAllBlocks",
            "parentBlockHash",
            "fields",
        ] {
            settle(
                &format!(r#"{{"type":"evm","{slot}":{shape},"logs":[{{}}]}}"#),
                evm(),
            );
        }
        settle(
            &format!(r#"{{"type":"evm","fromBlock":0,"logs":{shape}}}"#),
            evm(),
        );
        settle(
            &format!(r#"{{"type":"evm","fromBlock":0,"logs":[{shape}]}}"#),
            evm(),
        );
        settle(
            &format!(r#"{{"type":"evm","fromBlock":0,"logs":[{{"address":{shape}}}]}}"#),
            evm(),
        );
    }
}
