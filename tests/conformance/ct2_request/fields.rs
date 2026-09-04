//! The field surface is closed, and it is exactly the one the catalog declares.

use sqd_query_engine::output::snake_to_camel;
use sqd_query_engine::query::parse_query;
use std::collections::BTreeSet;

use crate::harness::fixtures::meta;

/// A misspelled field name used to come back as a 200 with the field missing,
/// which sends the client looking for the bug everywhere except in its own
/// request.
///
/// Covers CT-2 · INV-Q7
#[test]
fn unknown_field_names_are_rejected() {
    let evm = meta("evm");
    let rejected = [
        r#"{"type":"evm","fromBlock":0,"fields":{"log":{"logIndx":true}}}"#,
        // A typo is a typo whether or not it was switched on.
        r#"{"type":"evm","fromBlock":0,"fields":{"log":{"logIndx":false}}}"#,
        // System columns back blooms and size counters; they are not selectable.
        r#"{"type":"evm","fromBlock":0,"fields":{"log":{"dataSize":true}}}"#,
        // A real column, but of a different table.
        r#"{"type":"evm","fromBlock":0,"fields":{"log":{"sighash":true}}}"#,
    ];
    for json in rejected {
        assert!(
            parse_query(json.as_bytes(), &evm).is_err(),
            "expected an error for {json}"
        );
    }
}

/// The check must not overreach: ordinary columns, virtual fields and
/// field-group request keys all stay selectable.
#[test]
fn selectable_field_shapes_are_accepted() {
    let evm = meta("evm");
    let accepted = [
        // Ordinary column.
        r#"{"type":"evm","fromBlock":0,"fields":{"log":{"logIndex":true}}}"#,
        // Virtual field rolled from topic0..topic3.
        r#"{"type":"evm","fromBlock":0,"fields":{"log":{"topics":true}}}"#,
        // Field-group request key on the polymorphic trace table.
        r#"{"type":"evm","fromBlock":0,"fields":{"trace":{"callCallType":true}}}"#,
    ];
    for json in accepted {
        parse_query(json.as_bytes(), &evm)
            .unwrap_or_else(|e| panic!("expected {json} to parse, got {e}"));
    }
}

/// Every field the reference implementation lets a client select must still be
/// selectable here, or closing the surface would reject working queries. The
/// reference's own lists are the oracle; this pins the datasets that are in
/// sync so they cannot drift back.
#[test]
fn reference_selectable_fields_are_all_accepted() {
    // (dataset, field group, fields) — mirrors the reference's field selections.
    let cases: &[(&str, &str, &[&str])] = &[
        (
            "evm",
            "block",
            &[
                "number",
                "hash",
                "parentHash",
                "timestamp",
                "transactionsRoot",
                "receiptsRoot",
                "stateRoot",
                "logsBloom",
                "sha3Uncles",
                "extraData",
                "miner",
                "nonce",
                "mixHash",
                "size",
                "gasLimit",
                "gasUsed",
                "difficulty",
                "totalDifficulty",
                "baseFeePerGas",
                "uncles",
                "withdrawals",
                "withdrawalsRoot",
                "blobGasUsed",
                "excessBlobGas",
                "parentBeaconBlockRoot",
                "requestsHash",
                "l1BlockNumber",
            ],
        ),
        (
            "evm",
            "transaction",
            &[
                "transactionIndex",
                "hash",
                "nonce",
                "from",
                "to",
                "input",
                "value",
                "gas",
                "gasPrice",
                "maxFeePerGas",
                "maxPriorityFeePerGas",
                "v",
                "r",
                "s",
                "yParity",
                "chainId",
                "sighash",
                "contractAddress",
                "gasUsed",
                "cumulativeGasUsed",
                "effectiveGasPrice",
                "type",
                "status",
                "accessList",
                "logsBloom",
                "blobGasUsed",
                "blobGasPrice",
            ],
        ),
        (
            "solana",
            "instruction",
            &[
                "transactionIndex",
                "instructionAddress",
                "programId",
                "accounts",
                "data",
                "d1",
                "d2",
                "d4",
                "d8",
                "error",
                "computeUnitsConsumed",
                "isCommitted",
                "hasDroppedLogMessages",
            ],
        ),
    ];

    for (dataset, group, fields) in cases {
        let metadata = meta(dataset);
        for field in *fields {
            let json = format!(
                r#"{{"type":"{dataset}","fromBlock":0,"fields":{{"{group}":{{"{field}":true}}}}}}"#
            );
            parse_query(json.as_bytes(), &metadata)
                .unwrap_or_else(|e| panic!("{dataset}.{group}.{field} must stay selectable: {e}"));
        }
    }
}

/// The field surface of every table, transcribed from the reference
/// implementation's `item_field_selection!` macros. It is the wire contract, and
/// the catalog is measured against it rather than the other way round.
///
/// `(dataset file, table, fieldName, fields)`.
#[allow(clippy::type_complexity)]
const REFERENCE_FIELD_SURFACE: &[(&str, &str, &str, &[&str])] = &[
    (
        "evm",
        "blocks",
        "block",
        &[
            "number",
            "hash",
            "parentHash",
            "timestamp",
            "transactionsRoot",
            "receiptsRoot",
            "stateRoot",
            "logsBloom",
            "sha3Uncles",
            "extraData",
            "miner",
            "nonce",
            "mixHash",
            "size",
            "gasLimit",
            "gasUsed",
            "difficulty",
            "totalDifficulty",
            "baseFeePerGas",
            "uncles",
            "withdrawals",
            "withdrawalsRoot",
            "blobGasUsed",
            "excessBlobGas",
            "parentBeaconBlockRoot",
            "requestsHash",
            "l1BlockNumber",
            "mainBlockGeneralGasLimit",
            "sharedGasLimit",
            "timestampMillisPart",
        ],
    ),
    (
        "evm",
        "transactions",
        "transaction",
        &[
            "transactionIndex",
            "hash",
            "nonce",
            "from",
            "to",
            "input",
            "value",
            "gas",
            "gasPrice",
            "maxFeePerGas",
            "maxPriorityFeePerGas",
            "v",
            "r",
            "s",
            "yParity",
            "accessList",
            "chainId",
            "sighash",
            "contractAddress",
            "gasUsed",
            "logsBloom",
            "cumulativeGasUsed",
            "effectiveGasPrice",
            "type",
            "status",
            "blobGasUsed",
            "blobGasPrice",
            "maxFeePerBlobGas",
            "blobVersionedHashes",
            "authorizationList",
            "calls",
            "nonceKey",
            "signature",
            "feeToken",
            "feePayerV",
            "feePayerR",
            "feePayerS",
            "validBefore",
            "validAfter",
            "aaAuthorizationList",
            "keyAuthorization",
            "l1Fee",
            "l1FeeScalar",
            "l1GasPrice",
            "l1GasUsed",
            "l1BlobBaseFee",
            "l1BlobBaseFeeScalar",
            "l1BaseFeeScalar",
        ],
    ),
    (
        "evm",
        "logs",
        "log",
        &[
            "logIndex",
            "transactionIndex",
            "transactionHash",
            "address",
            "data",
            "topics",
        ],
    ),
    (
        "evm",
        "traces",
        "trace",
        &[
            "transactionIndex",
            "traceAddress",
            "subtraces",
            "type",
            "error",
            "revertReason",
            "createFrom",
            "createValue",
            "createGas",
            "createInit",
            "createResultGasUsed",
            "createResultCode",
            "createResultAddress",
            "callFrom",
            "callTo",
            "callValue",
            "callGas",
            "callInput",
            "callSighash",
            "callType",
            "callCallType",
            "callResultGasUsed",
            "callResultOutput",
            "suicideAddress",
            "suicideRefundAddress",
            "suicideBalance",
            "rewardAuthor",
            "rewardValue",
            "rewardType",
        ],
    ),
    (
        "evm",
        "statediffs",
        "stateDiff",
        &["transactionIndex", "address", "key", "kind", "prev", "next"],
    ),
    (
        "solana",
        "blocks",
        "block",
        &[
            "number",
            "hash",
            "parentNumber",
            "parentHash",
            "height",
            "timestamp",
        ],
    ),
    (
        "solana",
        "transactions",
        "transaction",
        &[
            "transactionIndex",
            "version",
            "accountKeys",
            "addressTableLookups",
            "numReadonlySignedAccounts",
            "numReadonlyUnsignedAccounts",
            "numRequiredSignatures",
            "recentBlockhash",
            "signatures",
            "err",
            "fee",
            "computeUnitsConsumed",
            "loadedAddresses",
            "feePayer",
            "hasDroppedLogMessages",
        ],
    ),
    (
        "solana",
        "instructions",
        "instruction",
        &[
            "transactionIndex",
            "instructionAddress",
            "programId",
            "accounts",
            "data",
            "d1",
            "d2",
            "d4",
            "d8",
            "error",
            "computeUnitsConsumed",
            "isCommitted",
            "hasDroppedLogMessages",
        ],
    ),
    (
        "solana",
        "logs",
        "log",
        &[
            "transactionIndex",
            "logIndex",
            "instructionAddress",
            "programId",
            "kind",
            "message",
        ],
    ),
    (
        "solana",
        "balances",
        "balance",
        &["transactionIndex", "account", "pre", "post"],
    ),
    (
        "solana",
        "token_balances",
        "tokenBalance",
        &[
            "transactionIndex",
            "account",
            "preMint",
            "postMint",
            "preDecimals",
            "postDecimals",
            "preProgramId",
            "postProgramId",
            "preOwner",
            "postOwner",
            "preAmount",
            "postAmount",
        ],
    ),
    (
        "solana",
        "rewards",
        "reward",
        &[
            "pubkey",
            "lamports",
            "postBalance",
            "rewardType",
            "commission",
        ],
    ),
    (
        "substrate",
        "blocks",
        "block",
        &[
            "number",
            "hash",
            "parentHash",
            "stateRoot",
            "extrinsicsRoot",
            "digest",
            "specName",
            "specVersion",
            "implName",
            "implVersion",
            "validator",
            "timestamp",
        ],
    ),
    (
        "substrate",
        "extrinsics",
        "extrinsic",
        &[
            "index",
            "version",
            "success",
            "hash",
            "fee",
            "tip",
            "signature",
            "error",
        ],
    ),
    (
        "substrate",
        "calls",
        "call",
        &[
            "extrinsicIndex",
            "address",
            "name",
            "success",
            "args",
            "origin",
            "error",
        ],
    ),
    (
        "substrate",
        "events",
        "event",
        &[
            "index",
            "extrinsicIndex",
            "name",
            "phase",
            "callAddress",
            "topics",
            "args",
        ],
    ),
    (
        "bitcoin",
        "blocks",
        "block",
        &[
            "number",
            "hash",
            "parentHash",
            "timestamp",
            "medianTime",
            "version",
            "merkleRoot",
            "nonce",
            "target",
            "bits",
            "difficulty",
            "chainWork",
            "strippedSize",
            "size",
            "weight",
        ],
    ),
    (
        "bitcoin",
        "transactions",
        "transaction",
        &[
            "transactionIndex",
            "hex",
            "txid",
            "hash",
            "size",
            "vsize",
            "weight",
            "version",
            "locktime",
        ],
    ),
    (
        "bitcoin",
        "inputs",
        "input",
        &[
            "transactionIndex",
            "inputIndex",
            "type",
            "txid",
            "vout",
            "scriptSigHex",
            "scriptSigAsm",
            "sequence",
            "coinbase",
            "txInWitness",
            "prevoutGenerated",
            "prevoutHeight",
            "prevoutValue",
            "prevoutScriptPubKeyHex",
            "prevoutScriptPubKeyAsm",
            "prevoutScriptPubKeyDesc",
            "prevoutScriptPubKeyType",
            "prevoutScriptPubKeyAddress",
        ],
    ),
    (
        "bitcoin",
        "outputs",
        "output",
        &[
            "transactionIndex",
            "outputIndex",
            "value",
            "scriptPubKeyHex",
            "scriptPubKeyAsm",
            "scriptPubKeyDesc",
            "scriptPubKeyType",
            "scriptPubKeyAddress",
        ],
    ),
    (
        "hyperliquid_fills",
        "blocks",
        "block",
        &["number", "hash", "parentHash", "timestamp"],
    ),
    (
        "hyperliquid_fills",
        "fills",
        "fill",
        &[
            "fillIndex",
            "user",
            "coin",
            "px",
            "sz",
            "side",
            "time",
            "startPosition",
            "dir",
            "closedPnl",
            "hash",
            "oid",
            "crossed",
            "fee",
            "builderFee",
            "tid",
            "cloid",
            "feeToken",
            "builder",
            "twapId",
        ],
    ),
    (
        "hyperliquid_replica_cmds",
        "blocks",
        "block",
        &[
            "number",
            "hash",
            "parentHash",
            "round",
            "parentRound",
            "proposer",
            "timestamp",
            "hardfork",
        ],
    ),
    (
        "hyperliquid_replica_cmds",
        "actions",
        "action",
        &[
            "actionIndex",
            "user",
            "action",
            "signature",
            "nonce",
            "vaultAddress",
            "status",
            "response",
        ],
    ),
    (
        "tron",
        "blocks",
        "block",
        &[
            "number",
            "hash",
            "parentHash",
            "txTrieRoot",
            "version",
            "timestamp",
            "witnessAddress",
            "witnessSignature",
        ],
    ),
    (
        "tron",
        "transactions",
        "transaction",
        &[
            "transactionIndex",
            "hash",
            "ret",
            "signature",
            "type",
            "parameter",
            "permissionId",
            "refBlockBytes",
            "refBlockHash",
            "feeLimit",
            "expiration",
            "timestamp",
            "rawDataHex",
            "fee",
            "contractResult",
            "contractAddress",
            "resMessage",
            "withdrawAmount",
            "unfreezeAmount",
            "withdrawExpireAmount",
            "cancelUnfreezeV2Amount",
            "result",
            "energyFee",
            "energyUsage",
            "energyUsageTotal",
            "netUsage",
            "netFee",
            "originEnergyUsage",
            "energyPenaltyTotal",
        ],
    ),
    (
        "tron",
        "logs",
        "log",
        &["transactionIndex", "logIndex", "address", "data", "topics"],
    ),
    (
        "tron",
        "internal_transactions",
        "internalTransaction",
        &[
            "transactionIndex",
            "internalTransactionIndex",
            "hash",
            "callerAddress",
            "transferToAddress",
            "callValueInfo",
            "note",
            "rejected",
            "extra",
        ],
    ),
];

/// A table's selectable fields are the ones the catalog declares — not "every
/// non-`system` column", which is the derivation §3 offers as a convenience.
///
/// Read as a definition it publishes every column the catalog carries for
/// filtering, grouping, joining or rolling: `blockNumber` on every item table,
/// `topic0…3` on `evm.logs`, `a0…a15` on `solana.instructions`. Nothing returns a
/// wrong answer that way — the surface is a superset — but the superset is
/// "whatever columns the archive happens to have", and a client that pins
/// `topic0` today breaks the day the archiver stops writing it, on a field the
/// catalog never promised.
///
/// Covers CT-2 · INV-Q14, INV-Q7
#[test]
fn the_field_surface_is_exactly_the_declared_one() {
    for (dataset, table, field_name, reference) in REFERENCE_FIELD_SURFACE {
        let metadata = meta(dataset);
        let desc = metadata
            .table(table)
            .unwrap_or_else(|| panic!("{dataset} has no table '{table}'"));

        let declared: BTreeSet<String> = desc
            .output
            .fields
            .iter()
            .map(|f| snake_to_camel(f))
            .collect();
        let expected: BTreeSet<String> = reference.iter().map(|f| f.to_string()).collect();

        assert_eq!(
            declared, expected,
            "{dataset}.{table} declares a field surface the reference does not"
        );

        // The catalog and the request path must agree on the same list: a name
        // the catalog declares and the parser refuses is a field promised and
        // not served.
        for field in *reference {
            let json = format!(
                r#"{{"type":"{}","fromBlock":0,"fields":{{"{field_name}":{{"{field}":true}}}}}}"#,
                metadata.name
            );
            parse_query(json.as_bytes(), &metadata).unwrap_or_else(|e| {
                panic!("{dataset}.{field_name}.{field} must be selectable: {e}")
            });
        }
    }
}

/// The two the specification names, through the request path a client uses.
/// Both are real columns of `evm.logs`; neither is a field.
///
/// Covers CT-2 · INV-Q14, INV-Q7
#[test]
fn a_column_the_catalog_carries_is_not_a_field() {
    let evm = meta("evm");

    for refused in [
        // Already in `header.number`; the column exists to group, join and order.
        r#"{"type":"evm","fromBlock":0,"fields":{"log":{"blockNumber":true}}}"#,
        // A filter column, rolled into `topics` on the way out.
        r#"{"type":"evm","fromBlock":0,"fields":{"log":{"topic0":true}}}"#,
    ] {
        let err = parse_query(refused.as_bytes(), &evm)
            .expect_err("a column the catalog does not offer as a field is UnknownField")
            .to_string();
        assert!(err.contains("unknown field"), "got: {err}");
    }

    // Not overreach: `d1` is a filter column *and* a declared field, which is
    // why `system` cannot be the discriminator and the list has to be declared.
    let solana = meta("solana");
    parse_query(
        br#"{"type":"solana","fromBlock":0,"fields":{"instruction":{"d1":true}}}"#,
        &solana,
    )
    .expect("a filter column the catalog declares as a field stays selectable");
}
