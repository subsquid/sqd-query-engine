//! Sets (§2.7): declared, referred to, and vouched for by their canaries, all
//! before any data is read.

use serde_json::{json, Value};
use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::sets::MAX_SET_CANARIES;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

use crate::harness::fixtures::{bundled_catalogs, meta};
use crate::harness::sets::{every_filter, BuiltSet, Rendering};

/// What validating `query` comes to: `None` when it compiles, the error's kind
/// otherwise.
fn outcome(catalog: &DatasetDescription, query: &Value) -> Option<ErrorKind> {
    let query = query.to_string();
    let err = parse_query(query.as_bytes(), catalog)
        .and_then(|parsed| compile(&parsed, catalog))
        .err()?;

    Some(error_kind(&err).unwrap_or_else(|| panic!("refused without a kind: {err:#}")))
}

/// A log request filtered on a set of addresses, the address selected.
fn logs_on(set: Value) -> Value {
    json!({
        "type": "evm",
        "sets": { "s": set },
        "fields": { "log": { "address": true } },
        "logs": [{ "address": { "set": "s" } }],
    })
}

const ADDRESSES: [&str; 4] = [
    "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc",
    "0x0d4a11d5EEaaC28EC3F61d100daF4d40471f1852",
    "0xA478c2975Ab1Ea89e8196811F51A7B7Ade33eB11",
    "0xBb2b8038a1640196FbE3e38816F3e67Cba72D940",
];

fn lowercase() -> Vec<String> {
    ADDRESSES.iter().map(|a| a.to_lowercase()).collect()
}

/// A reference names a declared set, a declared set has a reference, and each
/// set holds exactly one structure, `fuse8`, in canonical base 64 and sound.
///
/// Covers CT-2 · INV-Q15
#[test]
fn a_set_is_declared_used_and_well_formed() {
    let evm = meta("evm");
    let set = BuiltSet::of(&lowercase());
    let fuse8 = set.wire();
    let canaries = ["0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"];

    assert_eq!(outcome(&evm, &logs_on(set.declared(&canaries))), None);

    let undeclared = json!({
        "type": "evm",
        "fields": { "log": { "address": true } },
        "logs": [{ "address": { "set": "s" } }],
    });
    assert_eq!(outcome(&evm, &undeclared), Some(ErrorKind::UnknownSet));

    let unused = json!({ "type": "evm", "sets": { "unused": set.declared(&canaries) } });
    assert_eq!(outcome(&evm, &unused), Some(ErrorKind::InvalidSet));

    let one_of_two_unused = json!({
        "type": "evm",
        "sets": { "s": set.declared(&canaries), "t": set.declared(&canaries) },
        "fields": { "log": { "address": true } },
        "logs": [{ "address": { "set": "s" } }],
    });
    assert_eq!(
        outcome(&evm, &one_of_two_unused),
        Some(ErrorKind::InvalidSet)
    );

    // A reference in a list, beside values and other references, is a
    // reference, and so is one named twice.
    let in_a_list = |second: &str| {
        json!({
            "type": "evm",
            "sets": { "s": set.declared(&canaries), "t": set.declared(&canaries) },
            "fields": { "log": { "address": true } },
            "logs": [{
                "address": [ADDRESSES[1], { "set": "s" }, { "set": second }, { "set": "s" }],
            }],
        })
    };
    assert_eq!(outcome(&evm, &in_a_list("t")), None);
    assert_eq!(outcome(&evm, &in_a_list("u")), Some(ErrorKind::UnknownSet));

    // A reference in an item that matches nothing is still a reference, and
    // still checked.
    let beside_an_empty_list = json!({
        "type": "evm",
        "sets": { "s": set.declared(&canaries) },
        "fields": { "log": { "address": true } },
        "logs": [{ "address": { "set": "s" }, "topic0": [] }],
    });
    assert_eq!(outcome(&evm, &beside_an_empty_list), None);

    let mut short = base64_bytes(&fuse8);
    short.pop();
    let unpadded = fuse8.trim_end_matches('=').to_string();
    let malformed = [
        ("no structure", json!({ "canaries": canaries })),
        (
            "two structures",
            json!({ "fuse8": fuse8, "fuse16": fuse8, "canaries": canaries }),
        ),
        (
            "an unknown structure",
            json!({ "fuse16": fuse8, "canaries": canaries }),
        ),
        (
            "an unknown key",
            json!({ "fuse8": fuse8, "canaries": canaries, "x": 1 }),
        ),
        (
            "a number for bytes",
            json!({ "fuse8": 1, "canaries": canaries }),
        ),
        (
            "base 64 with a line break",
            json!({ "fuse8": format!("{fuse8}\n"), "canaries": canaries }),
        ),
        (
            "fingerprints short of N + 2L",
            json!({ "fuse8": base64_encode(&short), "canaries": canaries }),
        ),
        ("not an object", json!(fuse8)),
    ];
    for (what, declared) in malformed {
        assert_eq!(
            outcome(&evm, &logs_on(declared)),
            Some(ErrorKind::InvalidSet),
            "a set with {what}"
        );
    }

    // Padding is only missing where the length is not a multiple of three.
    if unpadded != fuse8 {
        let declared = json!({ "fuse8": unpadded, "canaries": canaries });
        assert_eq!(
            outcome(&evm, &logs_on(declared)),
            Some(ErrorKind::InvalidSet),
            "unpadded base 64"
        );
    }

    let not_an_object = json!({ "type": "evm", "sets": [] });
    assert_eq!(
        outcome(&evm, &not_an_object),
        Some(ErrorKind::MalformedRequest)
    );
}

fn base64_bytes(text: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .unwrap()
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// A set reference is accepted on exactly the filters that take a list of
/// `hexBytes`, `base58` or `utf8` text, on every filter of every bundled
/// catalog, tables and aliases alike, written alone or in a list beside a
/// value. Elsewhere it is `InvalidFilterValue`, and so is an object that is not
/// a reference.
///
/// Covers CT-2 · INV-Q16, INV-Q18
#[test]
fn a_set_stands_only_in_a_list_of_text() {
    let mut accepted = 0;
    let mut refused = 0;

    for catalog in bundled_catalogs() {
        for filter in every_filter(&catalog) {
            let rendering = filter.rendering.unwrap_or(Rendering::Utf8);
            let sample = rendering.sample();
            let selected: serde_json::Map<String, Value> = filter
                .fields
                .iter()
                .map(|field| (field.clone(), json!(true)))
                .collect();

            let expected = match (filter.rendering, filter.fields.is_empty()) {
                (None, _) => Some(ErrorKind::InvalidFilterValue),
                (Some(_), true) => Some(ErrorKind::SetFieldNotSelected),
                (Some(_), false) => None,
            };

            for value in [json!({ "set": "s" }), json!([sample, { "set": "s" }])] {
                let query = json!({
                    "type": catalog.name,
                    "sets": { "s": BuiltSet::of(&[sample]).declared(&[sample]) },
                    "fields": { (filter.output.clone()): selected },
                    (filter.request.clone()): [{ (filter.key.clone()): value }],
                });
                assert_eq!(
                    outcome(&catalog, &query),
                    expected,
                    "{}: {} on {}.{}",
                    catalog.name,
                    value,
                    filter.request,
                    filter.key
                );
            }

            if expected.is_none() {
                accepted += 1;
            } else {
                refused += 1;
            }
        }
    }

    // EVM alone has seventeen hex filters, and every catalog some that take no
    // set; far fewer means the walk lost a catalog.
    assert!(accepted > 50, "only {accepted} filters took a set");
    assert!(refused > 20, "only {refused} filters refused one");

    // The cases the invariant names, so the walk's own reading of the catalog
    // is not the only witness.
    let evm = meta("evm");
    let solana = meta("solana");
    let hex = Rendering::Hex.sample();
    let set = BuiltSet::of(&[hex]).declared(&[hex]);

    let from = json!({
        "type": "evm",
        "sets": { "s": set },
        "fields": { "transaction": { "from": true } },
        "transactions": [{ "from": { "set": "s" } }],
    });
    assert_eq!(outcome(&evm, &from), None);

    let nonce = json!({
        "type": "evm",
        "sets": { "s": set },
        "fields": { "transaction": { "nonce": true } },
        "transactions": [{ "firstNonce": { "set": "s" } }],
    });
    assert_eq!(outcome(&evm, &nonce), Some(ErrorKind::InvalidFilterValue));

    let discriminator = json!({
        "type": "solana",
        "sets": { "s": set },
        "fields": { "instruction": { "data": true } },
        "instructions": [{ "discriminator": { "set": "s" } }],
    });
    assert_eq!(
        outcome(&solana, &discriminator),
        Some(ErrorKind::InvalidFilterValue)
    );

    for reference in [
        json!({}),
        json!({ "set": 1 }),
        json!({ "set": "s", "canaries": [hex] }),
        json!({ "name": "s" }),
    ] {
        for value in [reference.clone(), json!([hex, { "set": "s" }, reference])] {
            let query = json!({
                "type": "evm",
                "sets": { "s": set },
                "fields": { "log": { "address": true } },
                "logs": [{ "address": { "set": "s" } }, { "address": value }],
            });
            assert_eq!(
                outcome(&evm, &query),
                Some(ErrorKind::InvalidFilterValue),
                "{value} as a filter value"
            );
        }
    }
}

/// Every way to build a set wrong is refused by its canaries, and the right set
/// passes them however the client cases them.
///
/// Covers CT-2 · INV-Q17
#[test]
fn a_sets_canaries_are_values_it_holds() {
    let evm = meta("evm");
    let solana = meta("solana");
    let lowercase = lowercase();

    // The canaries are the values as the client holds them, checksummed.
    let wrong: [(&str, BuiltSet); 5] = [
        ("checksummed text", BuiltSet::of(&ADDRESSES)),
        (
            "text without 0x",
            BuiltSet::of(&lowercase.iter().map(|a| &a[2..]).collect::<Vec<_>>()),
        ),
        (
            "the raw bytes",
            BuiltSet::of_keys(lowercase.iter().map(|a| {
                let bytes: Vec<u8> = (2..a.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&a[i..i + 2], 16).unwrap())
                    .collect();
                xxh3_64(&bytes)
            })),
        ),
        (
            "another seed",
            BuiltSet::of_keys(lowercase.iter().map(|a| xxh3_64_with_seed(a.as_bytes(), 1))),
        ),
        (
            "another hash",
            BuiltSet::of_keys(lowercase.iter().map(|a| {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::hash::DefaultHasher::new();
                a.hash(&mut hasher);
                hasher.finish()
            })),
        ),
    ];
    for (how, set) in wrong {
        assert_eq!(
            outcome(&evm, &logs_on(set.declared(&ADDRESSES))),
            Some(ErrorKind::SetCanaryMismatch),
            "a set built from {how}"
        );
    }

    let right = BuiltSet::of(&lowercase);
    let upper_cased: Vec<String> = ADDRESSES.iter().map(|a| a.to_uppercase()).collect();
    assert_eq!(outcome(&evm, &logs_on(right.declared(&ADDRESSES))), None);
    assert_eq!(outcome(&evm, &logs_on(right.declared(&upper_cased))), None);

    // One bad canary among good ones is enough, and a canary must be a value the
    // column can hold even where the structure happens to hold it.
    // The non-member is one the builder's own lookup rejects: a false positive
    // passes any canary check, about once in 256.
    let not_a_member = (1..=9)
        .map(|digit| format!("0x{}", digit.to_string().repeat(40)))
        .find(|value| !right.passes(value))
        .unwrap();
    let not_a_member = [ADDRESSES[0], not_a_member.as_str()];
    assert_eq!(
        outcome(&evm, &logs_on(right.declared(&not_a_member))),
        Some(ErrorKind::SetCanaryMismatch)
    );
    let odd = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9d";
    assert_eq!(
        outcome(&evm, &logs_on(BuiltSet::of(&[odd]).declared(&[odd]))),
        Some(ErrorKind::SetCanaryMismatch),
        "a hex canary with an odd number of digits"
    );

    let hex = Rendering::Hex.sample();
    let hex_on_base58 = json!({
        "type": "solana",
        "sets": { "s": BuiltSet::of(&[hex]).declared(&[hex]) },
        "fields": { "instruction": { "programId": true } },
        "instructions": [{ "programId": { "set": "s" } }],
    });
    assert_eq!(
        outcome(&solana, &hex_on_base58),
        Some(ErrorKind::SetCanaryMismatch)
    );

    // The canaries are checked for every column that refers to the set, each
    // folding them its own way: an upper-cased address passes on a hex column
    // and is not the text a `utf8` column holds. Four of them, so that a false
    // positive does not decide it.
    let two_columns = json!({
        "type": "evm",
        "sets": { "s": right.declared(&upper_cased) },
        "fields": { "log": { "address": true }, "trace": { "type": true } },
        "logs": [{ "address": { "set": "s" } }],
        "traces": [{ "type": { "set": "s" } }],
    });
    assert_eq!(
        outcome(&evm, &two_columns),
        Some(ErrorKind::SetCanaryMismatch)
    );

    // Every set a list refers to is checked.
    let one_wrong_of_two = json!({
        "type": "evm",
        "sets": {
            "right": right.declared(&ADDRESSES),
            "wrong": BuiltSet::of(&ADDRESSES).declared(&ADDRESSES),
        },
        "fields": { "log": { "address": true } },
        "logs": [{ "address": [{ "set": "right" }, { "set": "wrong" }] }],
    });
    assert_eq!(
        outcome(&evm, &one_wrong_of_two),
        Some(ErrorKind::SetCanaryMismatch)
    );

    let too_many = vec![ADDRESSES[0]; MAX_SET_CANARIES + 1];
    for canaries in [Vec::new(), too_many] {
        assert_eq!(
            outcome(&evm, &logs_on(right.declared(&canaries))),
            Some(ErrorKind::InvalidSet),
            "{} canaries",
            canaries.len()
        );
    }
    let most = vec![ADDRESSES[0]; MAX_SET_CANARIES];
    assert_eq!(outcome(&evm, &logs_on(right.declared(&most))), None);
}

/// A set-filtered column is selected, directly or through the field that rolls
/// it, since the client has to see the rows the set passes that are not in it.
///
/// Covers CT-2 · INV-Q18
#[test]
fn a_set_filtered_column_reaches_the_response() {
    let evm = meta("evm");
    let hex = Rendering::Hex.sample();
    let set = BuiltSet::of(&[hex]).declared(&[hex]);

    let query = |key: &str, fields: Value| {
        json!({
            "type": "evm",
            "sets": { "s": set },
            "fields": fields,
            "logs": [{ (key): { "set": "s" } }],
        })
    };

    let cases = [
        ("address", json!({ "log": { "address": true } }), None),
        ("address", json!({}), Some(ErrorKind::SetFieldNotSelected)),
        (
            "address",
            json!({ "log": { "address": false, "data": true } }),
            Some(ErrorKind::SetFieldNotSelected),
        ),
        (
            "address",
            json!({ "transaction": { "to": true } }),
            Some(ErrorKind::SetFieldNotSelected),
        ),
        ("topic0", json!({ "log": { "topics": true } }), None),
        (
            "topic0",
            json!({ "log": { "address": true } }),
            Some(ErrorKind::SetFieldNotSelected),
        ),
    ];
    for (key, fields, expected) in cases {
        assert_eq!(
            outcome(&evm, &query(key, fields.clone())),
            expected,
            "a set on logs.{key} with fields {fields}"
        );
    }

    // A value listed beside the reference does not make the rows visible.
    let beside_a_value = json!({
        "type": "evm",
        "sets": { "s": set },
        "fields": {},
        "logs": [{ "address": [hex, { "set": "s" }] }],
    });
    assert_eq!(
        outcome(&evm, &beside_a_value),
        Some(ErrorKind::SetFieldNotSelected)
    );
}
