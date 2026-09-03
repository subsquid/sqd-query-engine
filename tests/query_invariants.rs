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

/// Whether the fixture tree is checked out at all. It is not in the repository,
/// so in a plain checkout the tests that read it have nothing to run against.
///
/// A skip that looks like a pass is what this branch set out to remove, so it is
/// only allowed where nobody was promised coverage. Setting
/// `SQD_REQUIRE_FIXTURES=1` — which CI should — turns an absent tree into a
/// failure rather than a silent green run.
fn fixture_tree_is_present() -> bool {
    if fixture_chunk("ethereum").is_dir() {
        return true;
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_FIXTURES").is_none(),
        "SQD_REQUIRE_FIXTURES is set but tests/fixtures is not checked out, so these \
         tests would report green having compared nothing"
    );

    false
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

/// Case folding must follow the column an alias *resolves to*, not the name the
/// client wrote. `evmLogs.address` reaches a system column on the substrate
/// events table, and a client sending a checksummed address gets a 200 with no
/// events — the shape of answer that means "this address emitted nothing".
#[test]
fn an_alias_folds_case_on_the_column_it_resolves_to() {
    let substrate = meta("substrate");
    const ADDR_LOWER: &str = "0x00261a16442bc063573d2cbb0b5f398f9e1e14b9";
    const ADDR_UPPER: &str = "0x00261A16442BC063573D2CBB0B5F398F9E1E14B9";

    let query = |addr: &str| {
        format!(
            r#"{{"type":"substrate","fromBlock":0,
                 "fields":{{"block":{{"number":true}},"event":{{"name":true,"args":true}}}},
                 "evmLogs":[{{"address":["{addr}"]}}]}}"#
        )
        .into_bytes()
    };

    let lower = run("moonbeam", &substrate, &query(ADDR_LOWER)).unwrap();
    let upper = run("moonbeam", &substrate, &query(ADDR_UPPER)).unwrap();

    let found = count_items(&lower, "events");
    assert!(
        found > 0,
        "the fixture must carry EVM logs for this contract"
    );
    assert_eq!(
        found,
        count_items(&upper, "events"),
        "an alias filter must fold case like any other"
    );
    assert!(lower == upper, "the two responses must be identical");
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

/// INV-P14: a well-formed value the column cannot hold matches nothing. It is
/// not an error, and it must never be truncated into a *different* value —
/// `instructionAddress` above u32::MAX used to wrap.
#[test]
fn unmatchable_values_are_not_errors() {
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

// ---------------------------------------------------------------------------
// INV-Q7 — a `fields` key that names nothing selectable is an error
// ---------------------------------------------------------------------------

/// A misspelled field name used to come back as a 200 with the field missing,
/// which sends the client looking for the bug everywhere except in its own
/// request.
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

// ---------------------------------------------------------------------------
// INV-X3 — a filtered column absent from the chunk is an error
// ---------------------------------------------------------------------------

/// Copy a fixture chunk into a temp dir, dropping one column from one table —
/// the shape of a chunk written before that column existed.
fn chunk_without_column(dataset: &str, table: &str, drop_column: &str) -> tempfile::TempDir {
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let src = fixture_chunk(dataset);
    let dir = tempfile::TempDir::new().unwrap();

    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let dst = dir.path().join(&name);

        if name != format!("{table}.parquet") {
            std::fs::copy(&path, &dst).unwrap();
            continue;
        }

        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let full = batches[0].schema();
        let keep: Vec<usize> = (0..full.fields().len())
            .filter(|&i| full.field(i).name() != drop_column)
            .collect();
        assert_eq!(
            keep.len() + 1,
            full.fields().len(),
            "'{drop_column}' must be present in the source table to drop it"
        );

        let trimmed = Arc::new(full.project(&keep).unwrap());
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&dst).unwrap(), trimmed.clone(), None)
                .unwrap();
        for batch in &batches {
            writer.write(&batch.project(&keep).unwrap()).unwrap();
        }
        writer.close().unwrap();
    }

    dir
}

/// The single most dangerous silent failure available: a filter the engine
/// cannot evaluate stops narrowing the scan and starts matching everything, so
/// a query asking for four rows is answered with the whole chunk — and the
/// response gives the client no way to tell.
#[test]
fn filtering_an_absent_column_is_an_error() {
    let evm = meta("evm");
    let chunk = chunk_without_column("ethereum", "transactions", "sighash");
    let query = br#"{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                     "fields":{"transaction":{"transactionIndex":true}},
                     "transactions":[{"sighash":["0xa9059cbb"]}]}"#;

    let parsed = parse_query(query, &evm).unwrap();
    let plan = compile(&parsed, &evm).unwrap();
    let result = execute_plan(&plan, &evm, chunk.path());

    let err = match result {
        Err(e) => e,
        Ok(out) => {
            let items = count_items(
                &out.map(|o| o.into_json_lines()).unwrap_or_default(),
                "transactions",
            );
            panic!("filtering on an absent column must error; got {items} transactions instead");
        }
    };
    let message = err.root_cause().to_string();
    assert!(
        message.contains("sighash"),
        "the error must name the missing column, got: {message}"
    );
}

/// The check is about the chunk, not the catalog: with the column present the
/// same query is answered normally.
#[test]
fn filtering_a_present_column_still_works() {
    let evm = meta("evm");
    let body = run(
        "ethereum",
        &evm,
        br#"{"type":"evm","fromBlock":17881390,"toBlock":17881391,
             "fields":{"transaction":{"transactionIndex":true}},
             "transactions":[{"sighash":["0xa9059cbb"]}]}"#,
    )
    .unwrap();
    assert!(
        count_items(&body, "transactions") > 0,
        "fixture must contain ERC-20 transfers"
    );
}

/// Which of the two scan entry points a query lands on is decided by the table's
/// declared sort key, which no client can see. `transactions` leads with
/// `sighash` and takes the plain scan; `statediffs` leads with the block number
/// and takes the budget walk. The guarantee has to hold on both.
#[test]
fn filtering_an_absent_column_is_an_error_on_a_block_sorted_table() {
    let evm = meta("evm");
    let chunk = chunk_without_column("ethereum", "statediffs", "address");
    let query = br#"{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                     "fields":{"stateDiff":{"key":true}},
                     "stateDiffs":[{"address":["0xdac17f958d2ee523a2206206994597c13d831ec7"]}]}"#;

    let parsed = parse_query(query, &evm).unwrap();
    let plan = compile(&parsed, &evm).unwrap();

    let err = match execute_plan(&plan, &evm, chunk.path()) {
        Err(e) => e,
        Ok(out) => {
            let items = count_items(
                &out.map(|o| o.into_json_lines()).unwrap_or_default(),
                "stateDiffs",
            );
            panic!("filtering on an absent column must error; got {items} state diffs instead");
        }
    };
    assert!(
        err.root_cause().to_string().contains("address"),
        "the error must name the missing column, got: {}",
        err.root_cause()
    );
}

/// Query items are alternatives, but the reference implementation still refuses
/// the whole request when any one of them names a column the chunk lacks —
/// verified against it directly. Pinned because "make the unanswerable item match
/// nothing instead" reads like the kinder behaviour and would silently diverge.
#[test]
fn one_unanswerable_item_rejects_the_whole_request() {
    let evm = meta("evm");
    let chunk = chunk_without_column("ethereum", "traces", "reward_author");
    let query = br#"{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                     "fields":{"trace":{"transactionIndex":true,"type":true}},
                     "traces":[{"type":["call"]},
                               {"rewardAuthor":["0xdead000000000000000000000000000000000000"]}]}"#;

    let parsed = parse_query(query, &evm).unwrap();
    let plan = compile(&parsed, &evm).unwrap();
    let err = match execute_plan(&plan, &evm, chunk.path()) {
        Err(e) => e,
        Ok(_) => panic!("an item naming an absent column must reject the request"),
    };

    assert!(
        err.root_cause().to_string().contains("reward_author"),
        "the error must name the missing column, got: {}",
        err.root_cause()
    );
}

// ---------------------------------------------------------------------------
// A filter value means the same thing scalar or in a list
// ---------------------------------------------------------------------------

/// A one-element list and the bare value are the same request, so they must
/// compile through the same code. They used to take separate branches: the list
/// branch parsed hex against the column's type, the scalar branch compared a
/// `Utf8` against whatever the column was. On a string column that happened to
/// work; on `d1`, `d2`, `d4`, `d8` it matched nothing and said 200.
#[test]
fn a_scalar_filter_means_the_same_as_a_one_element_list() {
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

// ---------------------------------------------------------------------------
// INV-Q12 / request surface — a malformed request is refused, never coerced
// ---------------------------------------------------------------------------

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

/// A well-formed value that cannot fit the column is not an error — it matches
/// nothing (INV-P14), which is what the reference does too. Pinned so the
/// rejection above does not get widened into this.
#[test]
fn a_hex_value_too_wide_for_the_column_matches_nothing() {
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

// ---------------------------------------------------------------------------
// Relations — a row is emitted once, however many relations reach it
// ---------------------------------------------------------------------------

/// Every item a response carries under one table key, as `(block, item)` pairs.
fn items_of(body: &[u8], table_key: &str) -> Vec<(u64, serde_json::Value)> {
    body.split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .flat_map(|line| {
            let block: serde_json::Value = serde_json::from_slice(line).unwrap();
            let number = block["header"]["number"].as_u64().unwrap();
            block
                .get(table_key)
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(move |item| (number, item))
        })
        .collect()
}

/// A relation result carries the rows the source rows point at, and no others.
/// A null key is not a key: an event that belongs to no call used to serialize
/// byte-for-byte like an event whose call is the root one (address `[]`), so
/// asking for `call` returned every inherent's root call on top of the real
/// answer — extra rows, no filter that could exclude them, and nothing in the
/// response to say so.
#[test]
fn a_null_join_key_matches_nothing() {
    let substrate = meta("substrate");
    let body = run(
        "moonbeam",
        &substrate,
        br#"{"type":"substrate","fromBlock":4668500,"toBlock":4668502,
             "events":[{"call":true}],
             "fields":{"block":{"number":true},
                       "event":{"name":true,"callAddress":true,"extrinsicIndex":true},
                       "call":{"name":true,"address":true,"extrinsicIndex":true}}}"#,
    )
    .unwrap();

    let events = items_of(&body, "events");
    let calls = items_of(&body, "calls");
    assert!(
        !events.is_empty() && !calls.is_empty(),
        "the fixture must carry both"
    );

    // What the events actually point at.
    let pointed_at: std::collections::HashSet<_> = events
        .iter()
        .filter(|(_, event)| !event["callAddress"].is_null())
        .map(|(block, event)| {
            (
                *block,
                event["extrinsicIndex"].clone(),
                event["callAddress"].clone(),
            )
        })
        .collect();

    let orphans: Vec<_> = calls
        .iter()
        .filter(|(block, call)| {
            !pointed_at.contains(&(
                *block,
                call["extrinsicIndex"].clone(),
                call["address"].clone(),
            ))
        })
        .collect();

    assert!(
        orphans.is_empty(),
        "{} of {} calls are in the response with no event pointing at them, first: {:?}",
        orphans.len(),
        calls.len(),
        orphans.first()
    );
}

/// The same rule on the hierarchical path: `stack` walks from an event up to the
/// root call, and an event with no call has no stack. Indexing its null address
/// as the empty one made every inherent's root call an ancestor of it.
#[test]
fn a_null_address_has_no_ancestors() {
    let substrate = meta("substrate");
    let body = run(
        "moonbeam",
        &substrate,
        br#"{"type":"substrate","fromBlock":4668500,"toBlock":4668502,
             "events":[{"stack":true}],
             "fields":{"block":{"number":true},
                       "event":{"name":true,"callAddress":true,"extrinsicIndex":true},
                       "call":{"name":true,"address":true,"extrinsicIndex":true}}}"#,
    )
    .unwrap();

    let events = items_of(&body, "events");
    let calls = items_of(&body, "calls");
    assert!(
        !events.is_empty() && !calls.is_empty(),
        "the fixture must carry both"
    );

    let address_of = |item: &serde_json::Value| -> Vec<serde_json::Value> {
        item["address"]
            .as_array()
            .or_else(|| item["callAddress"].as_array())
            .cloned()
            .unwrap_or_default()
    };

    let orphans: Vec<_> = calls
        .iter()
        .filter(|(block, call)| {
            let ancestor = address_of(call);
            !events.iter().any(|(event_block, event)| {
                event_block == block
                    && event["extrinsicIndex"] == call["extrinsicIndex"]
                    && !event["callAddress"].is_null()
                    && address_of(event).starts_with(&ancestor)
            })
        })
        .collect();

    assert!(
        orphans.is_empty(),
        "{} of {} calls are ancestors of no event in the response, first: {:?}",
        orphans.len(),
        calls.len(),
        orphans.first()
    );
}

/// Two relations of the same item can name the same rows — `transactionTraces`
/// returns every trace of the transaction and so contains `subtraces` whole. The
/// overlap is the normal case, not a malformed query, and the row belongs in the
/// response once.
#[test]
fn stacked_relations_do_not_duplicate_rows() {
    let evm = meta("evm");
    let body = run(
        "ethereum",
        &evm,
        br#"{"type":"evm","fromBlock":17881391,"toBlock":17881391,
             "traces":[{"callSighash":["0xe21fd0e9"],
                        "transactionTraces":true,"subtraces":true}],
             "fields":{"block":{"number":true},
                       "trace":{"transactionIndex":true,"traceAddress":true,"type":true}}}"#,
    )
    .unwrap();

    let items = items_of(&body, "traces");
    assert!(!items.is_empty(), "the fixture must match traces");

    let mut seen = std::collections::HashSet::new();
    let duplicates: Vec<_> = items
        .iter()
        .filter(|(block, item)| {
            !seen.insert((
                *block,
                item["transactionIndex"].clone(),
                item["traceAddress"].clone(),
            ))
        })
        .collect();

    assert!(
        duplicates.is_empty(),
        "{} of {} traces came back twice, first: {:?}",
        duplicates.len(),
        items.len(),
        duplicates.first()
    );
}

/// The same row reached through one relation or through two must produce the same
/// response: adding a relation that names rows already present widens nothing.
#[test]
fn adding_an_overlapping_relation_changes_nothing() {
    let evm = meta("evm");
    let query = |relations: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881391,"toBlock":17881391,
                 "traces":[{{"callSighash":["0xe21fd0e9"],{relations}}}],
                 "fields":{{"block":{{"number":true}},
                            "trace":{{"transactionIndex":true,"traceAddress":true,"type":true}}}}}}"#
        )
        .into_bytes()
    };

    let alone = run("ethereum", &evm, &query(r#""transactionTraces":true"#)).unwrap();
    let with_subtraces = run(
        "ethereum",
        &evm,
        &query(r#""transactionTraces":true,"subtraces":true"#),
    )
    .unwrap();

    assert!(!alone.is_empty(), "the fixture must match traces");
    assert_eq!(
        alone, with_subtraces,
        "`subtraces` names a subset of `transactionTraces`, so it adds nothing"
    );
}

// ---------------------------------------------------------------------------
// INV-E5 — fork detection
// ---------------------------------------------------------------------------

/// The parent hash of the first block a chunk can speak about, read from the
/// fixture itself so the test is not pinned to a hard-coded chain.
fn parent_hash_of(dataset: &str, metadata: &DatasetDescription, block: u64) -> String {
    let body = run(
        dataset,
        metadata,
        format!(
            r#"{{"type":"{}","fromBlock":{block},"toBlock":{block},"includeAllBlocks":true,
                 "fields":{{"block":{{"number":true,"parentHash":true}}}}}}"#,
            metadata.name
        )
        .as_bytes(),
    )
    .unwrap();
    let line = body.split(|b| *b == b'\n').find(|l| !l.is_empty()).unwrap();
    let block: serde_json::Value = serde_json::from_slice(line).unwrap();
    block["header"]["parentHash"].as_str().unwrap().to_string()
}

/// A client paging through a chain sends back the hash it believes the previous
/// block has. Accepting the field and ignoring it — which is what happened
/// before — serves data from a branch the client did not ask about, with nothing
/// in the response to say so.
#[test]
fn a_mismatched_parent_block_hash_is_reported() {
    let evm = meta("evm");
    const FROM: u64 = 17881391;

    let query = |parent: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":{FROM},"toBlock":{FROM},"includeAllBlocks":true,
                 "parentBlockHash":"{parent}",
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .into_bytes()
    };

    // The chunk agrees: the query is answered.
    let actual_parent = parent_hash_of("ethereum", &evm, FROM);
    let body = run("ethereum", &evm, &query(&actual_parent)).unwrap();
    assert!(!body.is_empty(), "a matching parent hash must be served");

    // The chunk disagrees: the client is told, and told what the chunk has.
    let err = run("ethereum", &evm, &query("0xdeadbeef"))
        .expect_err("a mismatched parent hash must be reported");
    let reported = err
        .downcast_ref::<sqd_query_engine::output::UnexpectedBaseBlock>()
        .expect("the error must be an UnexpectedBaseBlock a client can act on");
    assert_eq!(reported.expected_hash, "0xdeadbeef");
    let parent = reported
        .prev_blocks
        .last()
        .expect("prev_blocks must not be empty");
    assert_eq!(parent.number, FROM - 1);
    assert_eq!(parent.hash, actual_parent);
}

/// The chunk's own first block still settles the question — its row carries its
/// parent's hash — so the check fires there too.
#[test]
fn the_first_block_of_a_chunk_still_settles_its_parent() {
    let evm = meta("evm");
    let actual_parent = parent_hash_of("ethereum", &meta("evm"), 17881390);

    run(
        "ethereum",
        &evm,
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881390,"includeAllBlocks":true,
                 "parentBlockHash":"{actual_parent}",
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .as_bytes(),
    )
    .expect("a matching parent hash must be served");

    run(
        "ethereum",
        &evm,
        br#"{"type":"evm","fromBlock":17881390,"toBlock":17881390,"includeAllBlocks":true,
             "parentBlockHash":"0xnot-the-parent-of-anything",
             "fields":{"block":{"number":true}}}"#,
    )
    .expect_err("a mismatched parent hash must be reported");
}

/// A chunk holding no evidence about the parent must stay quiet rather than
/// reject the request: the chunk not knowing is not the chain having forked.
#[test]
fn an_invisible_parent_block_is_not_a_fork() {
    let evm = meta("evm");
    // The chunk starts at 17881390, so nothing in it speaks about this window.
    run(
        "ethereum",
        &evm,
        br#"{"type":"evm","fromBlock":17000000,"toBlock":17000001,"includeAllBlocks":true,
             "parentBlockHash":"0xnot-the-parent-of-anything",
             "fields":{"block":{"number":true}}}"#,
    )
    .expect("a parent the chunk cannot see must not be reported as a fork");
}

/// The field is a hash, and a non-string is rejected at parse time rather than
/// quietly ignored.
#[test]
fn a_malformed_parent_block_hash_is_rejected() {
    let evm = meta("evm");
    for json in [
        r#"{"type":"evm","fromBlock":0,"parentBlockHash":123}"#,
        r#"{"type":"evm","fromBlock":0,"parentBlockHash":["0xabc"]}"#,
    ] {
        assert!(
            parse_query(json.as_bytes(), &evm).is_err(),
            "expected an error for {json}"
        );
    }
}

/// A chain that skips numbers has no block at `fromBlock - 1`, so the
/// predecessor has to be read from the parent-number column rather than
/// computed. Solana slot 217710449 follows 217710447.
/// Fork detection reads two columns out of the block table. When the chunk does
/// not carry them the check cannot run — and it used to answer the query anyway,
/// which is the one outcome `parentBlockHash` exists to prevent. The reference
/// refuses: `column 'parent_hash' is not found in 'blocks'`.
#[test]
fn a_chunk_that_cannot_answer_the_fork_check_is_an_error() {
    let evm = meta("evm");
    let chunk = chunk_without_column("ethereum", "blocks", "parent_hash");
    let parent = parent_hash_of("ethereum", &evm, 17881390);
    let query = format!(
        r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
             "parentBlockHash":"{parent}",
             "fields":{{"block":{{"number":true}}}},"logs":[{{}}]}}"#
    );

    let parsed = parse_query(query.as_bytes(), &evm).unwrap();
    let plan = compile(&parsed, &evm).unwrap();
    let err = match execute_plan(&plan, &evm, chunk.path()) {
        Err(e) => e,
        Ok(_) => panic!("a chunk without 'parent_hash' cannot serve a fork-checked query"),
    };

    assert!(
        err.root_cause().to_string().contains("parent_hash"),
        "the error must name the missing column, got: {}",
        err.root_cause()
    );
}

/// A chunk with no block table at all cannot answer the check either, and the
/// scan of a table a chunk does not have returns no rows rather than an error —
/// which reads here as "no evidence of a fork" and serves the query.
#[test]
fn a_chunk_with_no_block_table_cannot_clear_the_fork_check() {
    use arrow::array::{ArrayRef, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use sqd_query_engine::metadata::parse_dataset_description;
    use sqd_query_engine::output::execute_chunk;
    use sqd_query_engine::scan::ParquetChunkReader;
    use std::sync::Arc;

    let catalog = parse_dataset_description(
        r#"
name: test
tables:
  blocks:
    block_number_column: number
    parent_hash_column: parent_hash
    sort_key: [number]
    filters: []
    columns:
      number: { type: uint64 }
      parent_hash: { type: string }
  logs:
    query_name: logs
    field_name: log
    block_number_column: block_number
    item_order_keys: [log_index]
    sort_key: [block_number, log_index]
    filters: []
    columns:
      block_number: { type: uint64 }
      log_index: { type: uint32 }
"#,
    )
    .unwrap();

    // Only the item table is on disk.
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![10u64, 11])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32, 0])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = std::fs::File::create(dir.path().join("logs.parquet")).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let answer = |query: &str| {
        let parsed = parse_query(query.as_bytes(), &catalog).unwrap();
        let plan = compile(&parsed, &catalog).unwrap();
        let reader = ParquetChunkReader::open(dir.path()).unwrap();
        execute_chunk(&plan, &catalog, &reader, false)
    };

    assert!(
        answer(r#"{"type":"test","fromBlock":10,"toBlock":11,"logs":[{}]}"#).is_ok(),
        "without the field the chunk answers as before"
    );
    assert!(
        answer(
            r#"{"type":"test","fromBlock":10,"toBlock":11,
                "parentBlockHash":"0xabcd","logs":[{}]}"#
        )
        .is_err(),
        "a chunk with no block table holds no answer, and must say so"
    );
}

/// The column is required only of a chunk that reaches into the lookback window.
/// `required_columns` is checked before row groups are pruned, so requiring it of
/// every chunk fails a whole multi-chunk request over one that was never going to
/// carry the predecessor — a chunk far below `fromBlock` says nothing about it
/// either way.
#[test]
fn a_chunk_below_the_lookback_window_is_not_asked_for_the_parent_hash() {
    use arrow::array::{ArrayRef, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use sqd_query_engine::metadata::parse_dataset_description;
    use sqd_query_engine::output::execute_chunk;
    use sqd_query_engine::scan::ParquetChunkReader;
    use std::sync::Arc;

    let catalog = parse_dataset_description(
        r#"
name: test
tables:
  blocks:
    block_number_column: number
    parent_hash_column: parent_hash
    sort_key: [number]
    filters: []
    columns:
      number: { type: uint64, stats: true }
      parent_hash: { type: string }
"#,
    )
    .unwrap();

    // Blocks 1000..1009, written before `parent_hash` existed.
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from((1000u64..1010).collect::<Vec<_>>())) as ArrayRef],
    )
    .unwrap();
    let file = std::fs::File::create(dir.path().join("blocks.parquet")).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let answer = |from: u64, to: u64| {
        let query = format!(
            r#"{{"type":"test","fromBlock":{from},"toBlock":{to},
                "parentBlockHash":"0xabcd","includeAllBlocks":true}}"#
        );
        let parsed = parse_query(query.as_bytes(), &catalog).unwrap();
        let plan = compile(&parsed, &catalog).unwrap();
        let reader = ParquetChunkReader::open(dir.path()).unwrap();
        execute_chunk(&plan, &catalog, &reader, false)
    };

    assert!(
        answer(5000, 5010).is_ok(),
        "the window is blocks 4900..5000 and this chunk ends at 1009, so it is not \
         being asked anything"
    );
    assert!(
        answer(1005, 1009).is_err(),
        "this chunk does hold the predecessor and cannot produce its hash"
    );
}

/// Two behaviours below are pinned because they read like defects and are not:
/// both were compared against the reference implementation on this chunk and
/// produce its message verbatim. Changing either is a divergence, not a fix.
///
/// A hash is compared byte for byte. Every *filter* value in this engine folds
/// case, so an upper-cased hash looks like it should be accepted — the reference
/// rejects it, and a client that upper-cases hashes is broken against both
/// engines identically rather than against one of them.
#[test]
fn a_parent_block_hash_is_compared_byte_for_byte() {
    let evm = meta("evm");
    let parent = parent_hash_of("ethereum", &evm, 17881390);
    let query = |hash: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                 "parentBlockHash":"{hash}",
                 "fields":{{"block":{{"number":true}}}},"logs":[{{}}]}}"#
        )
        .into_bytes()
    };

    run("ethereum", &evm, &query(&parent)).expect("the true parent hash is accepted");

    let upper = parent.to_uppercase().replace("0X", "0x");
    assert!(
        run("ethereum", &evm, &query(&upper)).is_err(),
        "an upper-cased hash is a different hash, as it is for the reference"
    );
}

/// Only the row *at* `fromBlock` states what precedes it (INV-E5). With
/// `fromBlock` past the end of the chunk there is no such row, and the highest
/// row the chunk does hold describes a different block — comparing against it
/// reports a fork on a chain that never reorganised.
///
/// The reference compares against that row anyway, so this is a deliberate
/// divergence, in the same direction as the one already taken for an empty
/// window: a chunk that cannot see the block is not evidence about it.
#[test]
fn a_from_block_past_the_chunk_is_not_evidence_of_a_fork() {
    let evm = meta("evm");
    let query = |parent: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17882796,"toBlock":17882800,
                 "parentBlockHash":"{parent}",
                 "fields":{{"block":{{"number":true}}}},"logs":[{{}}]}}"#
        )
        .into_bytes()
    };

    // The hash of a real block's parent, and a hash of nothing at all. Neither
    // describes the parent of 17882796, and the chunk cannot say what does.
    for hash in [
        parent_hash_of("ethereum", &evm, 17881390),
        "0xdeadbeef".to_string(),
    ] {
        assert!(
            run("ethereum", &evm, &query(&hash)).is_ok(),
            "a chunk that does not reach fromBlock must not report a fork"
        );
    }
}

/// The counterpart: where the chunk *does* hold the row at `fromBlock`, that row
/// is the one compared, and a wrong hash is still a fork.
#[test]
fn the_row_at_from_block_is_the_one_compared() {
    let evm = meta("evm");
    const FROM: u64 = 17881500;

    let query = |parent: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":{FROM},"toBlock":{FROM},
                 "parentBlockHash":"{parent}","includeAllBlocks":true,
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .into_bytes()
    };

    let its_own_parent = parent_hash_of("ethereum", &evm, FROM);
    assert!(run("ethereum", &evm, &query(&its_own_parent)).is_ok());

    // An earlier block's parent hash is inside the lookback window, so it would
    // pass if any row of the window could answer. Only one can.
    let earlier = parent_hash_of("ethereum", &evm, FROM - 10);
    assert!(
        run("ethereum", &evm, &query(&earlier)).is_err(),
        "a hash from elsewhere in the window is not the parent of {FROM}"
    );
}

#[test]
fn fork_detection_follows_a_chain_that_skips_numbers() {
    let solana = meta("solana");
    const FROM: u64 = 217_710_449;
    const PARENT: u64 = 217_710_447;

    let actual_parent = parent_hash_of("solana", &solana, FROM);

    let err = run(
        "solana",
        &solana,
        format!(
            r#"{{"type":"solana","fromBlock":{FROM},"toBlock":{FROM},"includeAllBlocks":true,
                 "parentBlockHash":"not-the-parent",
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .as_bytes(),
    )
    .expect_err("a mismatched parent hash must be reported");

    let reported = err
        .downcast_ref::<sqd_query_engine::output::UnexpectedBaseBlock>()
        .expect("the error must be an UnexpectedBaseBlock");
    let parent = reported.prev_blocks.last().unwrap();
    assert_eq!(
        parent.number, PARENT,
        "the predecessor is the declared parent slot, not fromBlock - 1"
    );
    assert_eq!(parent.hash, actual_parent);

    run(
        "solana",
        &solana,
        format!(
            r#"{{"type":"solana","fromBlock":{FROM},"toBlock":{FROM},"includeAllBlocks":true,
                 "parentBlockHash":"{actual_parent}",
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .as_bytes(),
    )
    .expect("a matching parent hash must be served");
}

// ---------------------------------------------------------------------------
// INV-P15 — the filter surface is closed
// ---------------------------------------------------------------------------

/// Tables carry blooms, size counters and denormalised extractions. Resolving a
/// filter key against "any column of the table" exposed all of them, and made
/// the column list the public API: adding a column added a filter.
#[test]
fn undeclared_columns_are_not_filterable() {
    let evm = meta("evm");
    let rejected = [
        // System columns backing the weight model.
        (
            r#"{"type":"evm","fromBlock":0,"logs":[{"dataSize":[100]}]}"#,
            "data_size",
        ),
        // A real, emitted column that the reference does not let a client filter.
        (
            r#"{"type":"evm","fromBlock":0,"logs":[{"logIndex":[3]}]}"#,
            "log_index",
        ),
        (
            r#"{"type":"evm","fromBlock":0,"transactions":[{"gasUsed":["0x1"]}]}"#,
            "gas_used",
        ),
    ];
    for (json, column) in rejected {
        let table = evm.table("logs").unwrap();
        assert!(
            table.columns.contains_key(column)
                || evm
                    .table("transactions")
                    .unwrap()
                    .columns
                    .contains_key(column),
            "the test is only meaningful while '{column}' is a real column"
        );
        assert!(
            parse_query(json.as_bytes(), &evm).is_err(),
            "expected {json} to be rejected"
        );
    }
}

/// An alias is a narrower view of its table, and exposes its own filters rather
/// than inheriting everything the table accepts.
#[test]
fn an_alias_has_its_own_filter_surface() {
    let substrate = meta("substrate");

    // `evmLogs` accepts the log filters it aliases...
    parse_query(
        br#"{"type":"substrate","fromBlock":0,"evmLogs":[{"address":["0xabcd"]}]}"#,
        &substrate,
    )
    .expect("an alias must accept the filters it declares");

    // ...but not `name`, which belongs to the underlying events table and is
    // already pinned by the alias's implicit predicate.
    assert!(
        parse_query(
            br#"{"type":"substrate","fromBlock":0,"evmLogs":[{"name":["Balances.Transfer"]}]}"#,
            &substrate,
        )
        .is_err(),
        "an alias must not inherit the table's whole filter surface"
    );

    // The table itself still accepts it.
    parse_query(
        br#"{"type":"substrate","fromBlock":0,"events":[{"name":["Balances.Transfer"]}]}"#,
        &substrate,
    )
    .expect("the underlying table keeps its own filters");
}

/// Closing the surface must not close it on anything real. Every filter the
/// reference implementation accepts is exercised here, so a catalog that drifts
/// away from it fails rather than silently rejecting a working query.
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

// ---------------------------------------------------------------------------
// INV-P14 / INV-Q12 — a filter that cannot be evaluated must not widen the query
// ---------------------------------------------------------------------------

/// A bloom filter was the one filter shape in the engine that failed *open*.
/// Non-string elements were dropped and an empty needle set compiled to no
/// predicate at all, so `{"mentionsAccount": []}` returned every instruction in
/// range at 200 — the widest possible answer to the narrowest possible filter.
/// The reference marks an empty list `is_never`, which matches nothing.
#[test]
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
fn a_malformed_hex_filter_is_rejected_on_a_string_column() {
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

/// Pinned, not a defect: a *well-formed* value the column cannot hold matches
/// nothing rather than erroring (INV-P14). The reference parses `d8` with a
/// fixed-width hex reader and drops what does not fit, leaving an empty list that
/// its `PredicateBuilder` marks `is_never`. Both engines answer an empty 200.
#[test]
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

// ---------------------------------------------------------------------------
// INV-Q — a flag filter is a boolean
// ---------------------------------------------------------------------------

/// `callValueNonZero` and its siblings used to read any non-boolean as "off",
/// answering a strictly wider question than the one asked. The reference types
/// the field `bool` and refuses all four shapes below — as this engine already
/// did for `includeAllBlocks` and the block bounds.
#[test]
fn a_non_boolean_flag_filter_is_rejected() {
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
fn a_non_object_fields_is_rejected() {
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

// ---------------------------------------------------------------------------
// INV-E5 — parentBlockHash is answered or refused, never ignored
// ---------------------------------------------------------------------------

/// A dataset whose block table declares no parent-hash column cannot answer the
/// question at all. Accepting the field and returning data is the reorg being
/// served silently, so the request is refused before a chunk is opened.
#[test]
fn parent_block_hash_is_refused_where_the_catalog_cannot_answer_it() {
    use sqd_query_engine::metadata::parse_dataset_description;
    use sqd_query_engine::query::{compile, parse_query};

    let without_parent_hash = r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    filters: []
    columns:
      number: { type: uint64 }
      hash: { type: string }
"#;
    let with_parent_hash = format!("{without_parent_hash}      parent_hash: {{ type: string }}\n")
        .replace(
            "    sort_key: [number]",
            "    sort_key: [number]\n    parent_hash_column: parent_hash",
        );

    let query = br#"{"type":"test","fromBlock":10,"parentBlockHash":"0xabcd"}"#;

    let silent = parse_dataset_description(without_parent_hash).unwrap();
    let parsed = parse_query(query, &silent).unwrap();
    let err = compile(&parsed, &silent).unwrap_err().to_string();
    assert!(
        err.contains("parentBlockHash"),
        "the refusal must name the field the client sent, got: {err}"
    );

    let answerable = parse_dataset_description(&with_parent_hash).unwrap();
    let parsed = parse_query(query, &answerable).unwrap();
    assert!(
        compile(&parsed, &answerable).is_ok(),
        "a catalog that declares the column must still accept the field"
    );
}

// ---------------------------------------------------------------------------
// INV-B10 — every emitted row is counted against the response budget
// ---------------------------------------------------------------------------

/// A field-group request key names its column indirectly: `callCallType` reads
/// `call_type`. Projection resolved that; the weight model did not, so the column
/// was emitted at a weight of zero and the response ran past the cap. Selecting
/// it must cost what selecting the same column under its own name costs.
#[test]
fn a_field_group_request_key_weighs_what_its_column_weighs() {
    if !fixture_tree_is_present() {
        return;
    }
    let evm = meta("evm");

    let query = |field: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17882786,
                "fields":{{"trace":{{"{field}":true}}}},
                "traces":[{{}}]}}"#
        )
        .into_bytes()
    };

    let by_column = run("ethereum", &evm, &query("callType")).unwrap();
    let by_request_key = run("ethereum", &evm, &query("callCallType")).unwrap();

    let blocks = |body: &[u8]| {
        body.split(|b| *b == b'\n')
            .filter(|l| !l.is_empty())
            .count()
    };

    assert_eq!(
        blocks(&by_request_key),
        blocks(&by_column),
        "the two names read the same column, so they must be trimmed at the same block"
    );
    assert!(
        by_request_key.len() as u64 <= 20 * 1024 * 1024,
        "a response the budget model did not count ran to {} bytes",
        by_request_key.len()
    );
}

// ---------------------------------------------------------------------------
// INV-P3 — an empty list matches nothing
// ---------------------------------------------------------------------------

/// `"c": []` is `r[c] ∈ ∅`, which is false for every row. Reading it as "no
/// constraint" turns "match none of these" into "match every row in the chunk" —
/// the single most destructive misreading available. The discriminator was the
/// one filter that refused it outright instead, so a client asking for nothing
/// got a 400 where the rest of the surface answered emptily.
#[test]
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

// ---------------------------------------------------------------------------
// INV-Q10 / INV-Q11 — the request bounds that cost, not correctness, motivates
// ---------------------------------------------------------------------------

/// Each bloom value is a separate hash-and-probe over every row, so the length
/// of the list is a cost multiplier the client picks. `P-MAX-BLOOM-VALUES` caps
/// it at ten.
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

// ---------------------------------------------------------------------------
// INV-E1 — no input causes a crash
// ---------------------------------------------------------------------------

/// A chunk whose physical types disagree with the catalog's declared ones must
/// be answered or refused, never crash the process.
///
/// An encoding names the type the catalog *believes* a column has, and archives
/// outlive the catalogs that described them. The encoders reached that way used
/// to downcast on the catalog's word, so one column written at a different width
/// took down a worker serving every other query with it.
#[test]
fn a_chunk_that_disagrees_with_the_catalog_does_not_panic() {
    use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use sqd_query_engine::metadata::parse_dataset_description;
    use sqd_query_engine::output::execute_chunk;
    use sqd_query_engine::scan::ParquetChunkReader;
    use std::sync::Arc;

    let catalog = parse_dataset_description(
        r#"
name: test
tables:
  blocks:
    field_name: block
    block_number_column: number
    sort_key: [number]
    filters: []
    columns:
      number: { type: uint64 }
  items:
    query_name: items
    field_name: item
    block_number_column: block_number
    item_order_keys: [seq]
    sort_key: [block_number, seq]
    filters: []
    columns:
      block_number: { type: uint64 }
      seq: { type: uint32 }
      stamp:
        type: timestamp_millisecond
        json_encoding: timestamp_millisecond
      version:
        type: int16
        json_encoding: solana_tx_version
      doc:
        type: string
        json_encoding: json
      big:
        type: uint64
        json_encoding: string
      d8:
        type: uint64
        json_encoding: hex_number
"#,
    )
    .unwrap();

    // Every encoded column is stored as something the catalog does not describe.
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("seq", DataType::UInt32, false),
        Field::new("stamp", DataType::UInt64, false),
        Field::new("version", DataType::Utf8, false),
        Field::new("doc", DataType::UInt32, false),
        Field::new("big", DataType::Utf8, false),
        Field::new("d8", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![10u64, 11])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32, 0])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![1_700_000_000_000u64; 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["legacy", "0"])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![1u32, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["1", "2"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["0xff", "0x00"])) as ArrayRef,
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
        vec![Arc::new(UInt64Array::from(vec![10u64, 11])) as ArrayRef],
    )
    .unwrap();

    for (name, schema, batch) in [("items", schema, batch), ("blocks", blocks_schema, blocks)] {
        let file = std::fs::File::create(dir.path().join(format!("{name}.parquet"))).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let query = br#"{"type":"test","fromBlock":10,"toBlock":11,
        "fields":{"block":{"number":true},
                  "item":{"stamp":true,"version":true,"doc":true,"big":true,"d8":true}},
        "items":[{}]}"#;

    let parsed = parse_query(query, &catalog).unwrap();
    let plan = compile(&parsed, &catalog).unwrap();
    let reader = ParquetChunkReader::open(dir.path()).unwrap();

    // Either outcome is conforming; the test is that we get one.
    let Ok(Some(out)) = execute_chunk(&plan, &catalog, &reader, false) else {
        return;
    };

    let body = out.into_json_lines();
    for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        serde_json::from_slice::<serde_json::Value>(line)
            .expect("a response the encoders wrote must still be JSON");
    }
}
