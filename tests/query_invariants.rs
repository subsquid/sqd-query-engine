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
                "number", "hash", "parentHash", "timestamp", "transactionsRoot",
                "receiptsRoot", "stateRoot", "logsBloom", "sha3Uncles", "extraData",
                "miner", "nonce", "mixHash", "size", "gasLimit", "gasUsed",
                "difficulty", "totalDifficulty", "baseFeePerGas", "uncles",
                "withdrawals", "withdrawalsRoot", "blobGasUsed", "excessBlobGas",
                "parentBeaconBlockRoot", "requestsHash", "l1BlockNumber",
            ],
        ),
        (
            "evm",
            "transaction",
            &[
                "transactionIndex", "hash", "nonce", "from", "to", "input", "value",
                "gas", "gasPrice", "maxFeePerGas", "maxPriorityFeePerGas", "v", "r",
                "s", "yParity", "chainId", "sighash", "contractAddress",
                "gasUsed", "cumulativeGasUsed", "effectiveGasPrice", "type",
                "status", "accessList", "logsBloom", "blobGasUsed", "blobGasPrice",
            ],
        ),
        (
            "solana",
            "instruction",
            &[
                "transactionIndex", "instructionAddress", "programId", "accounts",
                "data", "d1", "d2", "d4", "d8", "error", "computeUnitsConsumed",
                "isCommitted", "hasDroppedLogMessages",
            ],
        ),
    ];

    for (dataset, group, fields) in cases {
        let metadata = meta(dataset);
        for field in *fields {
            let json = format!(
                r#"{{"type":"{dataset}","fromBlock":0,"fields":{{"{group}":{{"{field}":true}}}}}}"#
            );
            parse_query(json.as_bytes(), &metadata).unwrap_or_else(|e| {
                panic!("{dataset}.{group}.{field} must stay selectable: {e}")
            });
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
    let parent = reported.prev_blocks.last().expect("prev_blocks must not be empty");
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
        (r#"{"type":"evm","fromBlock":0,"logs":[{"dataSize":[100]}]}"#, "data_size"),
        // A real, emitted column that the reference does not let a client filter.
        (r#"{"type":"evm","fromBlock":0,"logs":[{"logIndex":[3]}]}"#, "log_index"),
        (r#"{"type":"evm","fromBlock":0,"transactions":[{"gasUsed":["0x1"]}]}"#, "gas_used"),
    ];
    for (json, column) in rejected {
        let table = evm.table("logs").unwrap();
        assert!(
            table.columns.contains_key(column) || evm.table("transactions").unwrap().columns.contains_key(column),
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
        ("evm", "transactions", &["from", "to", "sighash", "firstNonce", "lastNonce"]),
        ("evm", "logs", &["address", "topic0", "topic1", "topic2", "topic3"]),
        (
            "evm",
            "traces",
            &[
                "type", "createFrom", "createResultAddress", "callFrom", "callTo",
                "callSighash", "callCallType", "suicideAddress", "suicideRefundAddress",
                "rewardAuthor",
            ],
        ),
        ("evm", "stateDiffs", &["address", "key", "kind"]),
        (
            "solana",
            "instructions",
            &[
                "programId", "discriminator", "d1", "d2", "d4", "d8", "mentionsAccount",
                "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11",
                "a12", "a13", "a14", "a15", "isCommitted",
            ],
        ),
        ("solana", "transactions", &["feePayer", "mentionsAccount"]),
        ("solana", "logs", &["programId", "kind"]),
        ("solana", "balances", &["account"]),
        (
            "solana",
            "tokenBalances",
            &[
                "account", "preMint", "postMint", "preProgramId", "postProgramId",
                "preOwner", "postOwner",
            ],
        ),
        ("solana", "rewards", &["pubkey"]),
        ("substrate", "events", &["name"]),
        ("substrate", "calls", &["name"]),
        ("bitcoin", "outputs", &["scriptPubKeyAddress", "scriptPubKeyType"]),
        (
            "bitcoin",
            "inputs",
            &["type", "prevoutScriptPubKeyAddress", "prevoutScriptPubKeyType", "prevoutGenerated"],
        ),
        ("fuel", "receipts", &["type", "contract"]),
        (
            "fuel",
            "inputs",
            &["type", "coinOwner", "coinAssetId", "contractContract", "messageSender", "messageRecipient"],
        ),
        ("fuel", "outputs", &["type"]),
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
            parse_query(json.as_bytes(), &metadata)
                .unwrap_or_else(|e| panic!("{dataset}.{request_key}.{filter} must stay filterable: {e}"));
        }
    }
}
