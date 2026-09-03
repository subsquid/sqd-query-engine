//! What the reference implementation serves, the request surface must serve.
//!
//! Closing a surface must not close it on anything real. Every alias and every
//! filter the reference accepts is named here, so a catalog that drifts away
//! from it fails rather than silently rejecting a query that used to work.

use sqd_query_engine::query::{compile, parse_query};

use crate::harness::fixtures::meta;

/// Every alias the reference serves: the implicit predicate that makes it a
/// narrower view, and the filters and relations it exposes. Eight of these were
/// missing from the catalog outright — five Substrate aliases and all three
/// Tron ones — which left their extraction columns in the chunks and
/// unreachable from any query.
#[allow(clippy::type_complexity)]
const REFERENCE_ALIASES: &[(&str, &str, (&str, &str), &[&str], &[&str])] = &[
    (
        "substrate",
        "evmLogs",
        ("name", "EVM.Log"),
        &["address", "topic0", "topic1", "topic2", "topic3"],
        &["extrinsic", "call", "stack"],
    ),
    (
        "substrate",
        "ethereumTransactions",
        ("name", "Ethereum.transact"),
        &["to", "sighash"],
        &["extrinsic", "stack", "events"],
    ),
    (
        "substrate",
        "contractsEvents",
        ("name", "Contracts.ContractEmitted"),
        &["contractAddress"],
        &["extrinsic", "call", "stack"],
    ),
    (
        "substrate",
        "gearMessagesEnqueued",
        ("name", "Gear.UserMessageEnqueued"),
        &["programId"],
        &["extrinsic", "call", "stack"],
    ),
    (
        "substrate",
        "gearUserMessagesSent",
        ("name", "Gear.UserMessageSent"),
        &["programId"],
        &["extrinsic", "call", "stack"],
    ),
    (
        "substrate",
        "reviveContractEmitted",
        ("name", "Revive.ContractEmitted"),
        &["contract", "topic0", "topic1", "topic2", "topic3"],
        &["extrinsic", "call", "stack"],
    ),
    (
        "tron",
        "transferTransactions",
        ("type", "TransferContract"),
        &["owner", "to"],
        &["logs", "internalTransactions"],
    ),
    (
        "tron",
        "transferAssetTransactions",
        ("type", "TransferAssetContract"),
        &["owner", "to", "asset"],
        &["logs", "internalTransactions"],
    ),
    (
        "tron",
        "triggerSmartContractTransactions",
        ("type", "TriggerSmartContract"),
        &["owner", "contract", "sighash"],
        &["logs", "internalTransactions"],
    ),
];

/// Covers CT-2 · INV-Q6
#[test]
fn reference_aliases_are_all_served() {
    for (dataset, alias, _, filters, relations) in REFERENCE_ALIASES {
        let metadata = meta(dataset);
        let name = &metadata.name;

        for filter in *filters {
            let json =
                format!(r#"{{"type":"{name}","fromBlock":0,"{alias}":[{{"{filter}":[]}}]}}"#);
            parse_query(json.as_bytes(), &metadata)
                .unwrap_or_else(|e| panic!("{alias}.{filter} must be filterable: {e}"));
        }

        for relation in *relations {
            let json =
                format!(r#"{{"type":"{name}","fromBlock":0,"{alias}":[{{"{relation}":true}}]}}"#);
            let parsed = parse_query(json.as_bytes(), &metadata)
                .unwrap_or_else(|e| panic!("{alias}.{relation} must be requestable: {e}"));
            compile(&parsed, &metadata)
                .unwrap_or_else(|e| panic!("{alias}.{relation} must resolve: {e}"));
        }
    }
}

/// An alias narrows its table by a predicate the client cannot see or override.
/// A catalog that pins the wrong one answers a different question at 200.
#[test]
fn each_alias_pins_the_predicate_the_reference_pins() {
    for (dataset, alias, (column, value), _, _) in REFERENCE_ALIASES {
        let metadata = meta(dataset);
        let def = metadata
            .query_aliases
            .get(*alias)
            .unwrap_or_else(|| panic!("{dataset} must serve '{alias}'"));

        assert_eq!(
            def.implicit_predicates.get(*column).map(Vec::as_slice),
            Some([value.to_string()].as_slice()),
            "{dataset}.{alias} must pin {column} = {value}"
        );
    }
}

/// Closing the surface must not close it on anything real. Every filter the
/// reference implementation accepts is exercised here, so a catalog that drifts
/// away from it fails rather than silently rejecting a working query.
///
/// Covers CT-2 · INV-Q6
#[test]
fn reference_filters_are_all_accepted() {
    // (dataset, request key, filter keys) — mirrors the reference's requests.
    let cases: &[(&str, &str, &[&str])] = &[
        (
            "evm",
            "transactions",
            &["from", "to", "sighash", "firstNonce", "lastNonce"],
        ),
        (
            "evm",
            "logs",
            &["address", "topic0", "topic1", "topic2", "topic3"],
        ),
        (
            "evm",
            "traces",
            &[
                "type",
                "createFrom",
                "createResultAddress",
                "callFrom",
                "callTo",
                "callSighash",
                "callCallType",
                "suicideAddress",
                "suicideRefundAddress",
                "rewardAuthor",
            ],
        ),
        ("evm", "stateDiffs", &["address", "key", "kind"]),
        (
            "solana",
            "instructions",
            &[
                "programId",
                "discriminator",
                "d1",
                "d2",
                "d4",
                "d8",
                "mentionsAccount",
                "a0",
                "a1",
                "a2",
                "a3",
                "a4",
                "a5",
                "a6",
                "a7",
                "a8",
                "a9",
                "a10",
                "a11",
                "a12",
                "a13",
                "a14",
                "a15",
                "isCommitted",
            ],
        ),
        ("solana", "transactions", &["feePayer", "mentionsAccount"]),
        ("solana", "logs", &["programId", "kind"]),
        ("solana", "balances", &["account"]),
        (
            "solana",
            "tokenBalances",
            &[
                "account",
                "preMint",
                "postMint",
                "preProgramId",
                "postProgramId",
                "preOwner",
                "postOwner",
            ],
        ),
        ("solana", "rewards", &["pubkey"]),
        ("substrate", "events", &["name"]),
        ("substrate", "calls", &["name"]),
        (
            "substrate",
            "evmLogs",
            &["address", "topic0", "topic1", "topic2", "topic3"],
        ),
        ("substrate", "ethereumTransactions", &["to", "sighash"]),
        ("substrate", "contractsEvents", &["contractAddress"]),
        ("substrate", "gearMessagesEnqueued", &["programId"]),
        ("substrate", "gearUserMessagesSent", &["programId"]),
        (
            "substrate",
            "reviveContractEmitted",
            &["contract", "topic0", "topic1", "topic2", "topic3"],
        ),
        (
            "bitcoin",
            "outputs",
            &["scriptPubKeyAddress", "scriptPubKeyType"],
        ),
        (
            "bitcoin",
            "inputs",
            &[
                "type",
                "prevoutScriptPubKeyAddress",
                "prevoutScriptPubKeyType",
                "prevoutGenerated",
            ],
        ),
        ("tron", "transactions", &["type"]),
        (
            "tron",
            "logs",
            &["address", "topic0", "topic1", "topic2", "topic3"],
        ),
        ("tron", "internalTransactions", &["caller", "transferTo"]),
    ];

    for (dataset, request_key, filters) in cases {
        let metadata = meta(dataset);
        for filter in *filters {
            // A permissive value: every filter accepts a list of strings or an
            // empty list, and this test is about the surface, not the values.
            let json = format!(
                r#"{{"type":"{}","fromBlock":0,"{request_key}":[{{"{filter}":[]}}]}}"#,
                metadata.name
            );
            parse_query(json.as_bytes(), &metadata).unwrap_or_else(|e| {
                panic!("{dataset}.{request_key}.{filter} must stay filterable: {e}")
            });
        }
    }
}
