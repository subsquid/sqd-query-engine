//! CT-8 — adversarial chunks.
//!
//! A chunk written by an older archiver disagrees with today's catalog. The
//! failure that matters is the quiet one: a filter on a column the chunk does
//! not carry must be an error, because the alternative is a filter that matches
//! every row and a client that cannot tell.
//!
//! Dropping and adding columns or tables are portable synthetic cases. Retyping
//! and relaying a chunk are HC-3's other axes, and they live in CT-6, where what
//! they are being asked is whether the answer moved rather than whether the
//! engine noticed.

use arrow::datatypes::{DataType, Field, TimeUnit};
use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::output::{execute_chunk_arrow, execute_plan};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::sync::Arc;

use crate::harness::chunk::{
    chunk_with_column_retyped, chunk_with_nullable_column, chunk_without_column,
    chunk_without_column_at, chunk_without_table,
};
use crate::harness::fixtures::{answers_the_same, fixture_tree_is_present, meta, run, run_against};
use crate::harness::json::count_items;
use crate::harness::sol_like;
use crate::harness::synthetic::{catalog, logs_query, uniform, weighted_chunk, BLOCKS};

/// Covers CT-8 · INV-E3
#[test]
fn selecting_an_absent_column_is_an_error() {
    let metadata = catalog();
    let source = weighted_chunk(BLOCKS, &uniform(BLOCKS, 0), &[]);
    let chunk = chunk_without_column_at(source.path(), "logs", "data");
    let query = logs_query().to_string();

    let err = run_against(&metadata, chunk.path(), &query)
        .expect_err("a selected column absent from the chunk must error");

    assert_eq!(error_kind(&err), Some(ErrorKind::ColumnNotFound));
    assert!(
        err.root_cause().to_string().contains("data"),
        "the error must name the missing column, got: {}",
        err.root_cause()
    );
}

/// Covers CT-8 · INV-E4
#[test]
fn a_missing_table_is_an_error() {
    let metadata = catalog();
    let source = weighted_chunk(BLOCKS, &uniform(BLOCKS, 0), &[]);
    let chunk = chunk_without_table(source.path(), "logs");
    let query = logs_query().to_string();

    let err = run_against(&metadata, chunk.path(), &query)
        .expect_err("a table absent from the chunk must error");

    assert_eq!(error_kind(&err), Some(ErrorKind::TableNotFound));
    assert!(
        err.root_cause().to_string().contains("logs"),
        "the error must name the missing table, got: {}",
        err.root_cause()
    );
}

/// A relation target is part of the query even though it has no item request of
/// its own. Its absence must not turn a requested relation into an empty result.
///
/// Covers CT-8 · INV-E4
#[test]
fn a_missing_relation_table_is_an_error() {
    let metadata = catalog();
    let source = weighted_chunk(BLOCKS, &uniform(BLOCKS, 0), &uniform(BLOCKS, 0));
    let chunk = chunk_without_table(source.path(), "transactions");
    let query = serde_json::json!({
        "type": "test",
        "fromBlock": 10,
        "toBlock": 14,
        "logs": [{"transaction": true}],
        "fields": {"transaction": {"input": true}}
    })
    .to_string();

    let err = run_against(&metadata, chunk.path(), &query)
        .expect_err("a relation table absent from the chunk must error");

    assert_eq!(error_kind(&err), Some(ErrorKind::TableNotFound));
    assert!(
        err.root_cause().to_string().contains("transactions"),
        "the error must name the missing relation table, got: {}",
        err.root_cause()
    );
}

/// The block table supplies response framing and is required by every query.
///
/// Covers CT-8 · INV-E4
#[test]
fn a_missing_block_table_is_an_error() {
    let metadata = catalog();
    let source = weighted_chunk(BLOCKS, &uniform(BLOCKS, 0), &[]);
    let chunk = chunk_without_table(source.path(), "blocks");
    let query = logs_query().to_string();

    let err = run_against(&metadata, chunk.path(), &query)
        .expect_err("a block table absent from the chunk must error");

    assert_eq!(error_kind(&err), Some(ErrorKind::TableNotFound));
    assert!(
        err.root_cause().to_string().contains("blocks"),
        "the error must name the missing block table, got: {}",
        err.root_cause()
    );
}

/// Covers CT-8 · INV-X2
#[test]
fn an_ignored_nullable_column_does_not_change_output() {
    let metadata = catalog();
    let source = weighted_chunk(BLOCKS, &uniform(BLOCKS, 0), &[]);
    let extended =
        chunk_with_nullable_column(source.path(), "logs", "archiver_note", DataType::Utf8);
    let query = logs_query().to_string();

    let expected = run_against(&metadata, source.path(), &query).unwrap();
    let actual = run_against(&metadata, extended.path(), &query).unwrap();

    assert_eq!(actual, expected);
}

/// The single most dangerous silent failure available: a filter the engine
/// cannot evaluate stops narrowing the scan and starts matching everything, so
/// a query asking for four rows is answered with the whole chunk — and the
/// response gives the client no way to tell.
///
/// Covers CT-8 · INV-X3
#[test]
#[ignore = "requires external fixture data"]
fn filtering_an_absent_column_is_an_error() {
    if !fixture_tree_is_present() {
        return;
    }

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
    assert_eq!(error_kind(&err), Some(ErrorKind::ColumnNotFound));
}

/// The check is about the chunk, not the catalog: with the column present the
/// same query is answered normally.
///
/// Covers CT-8 · INV-X3
#[test]
#[ignore = "requires external fixture data"]
fn filtering_a_present_column_still_works() {
    if !fixture_tree_is_present() {
        return;
    }

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
/// and can use range reads. The guarantee has to hold on both.
#[test]
#[ignore = "requires external fixture data"]
fn filtering_an_absent_column_is_an_error_on_a_block_sorted_table() {
    if !fixture_tree_is_present() {
        return;
    }

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

/// The rule reaches a filter that resolves through an alias, which is where a
/// chunk older than the catalog actually shows up: `reviveContractEmitted` reads
/// extraction columns no chunk in the fixture tree carries. Answering that with
/// every `Revive.ContractEmitted` event in range would be the widest possible
/// answer to the narrowest possible filter, and no client could tell.
///
/// Covers CT-8 · INV-X3
#[test]
#[ignore = "requires external fixture data"]
fn an_alias_filter_on_a_column_the_chunk_lacks_is_an_error() {
    if !fixture_tree_is_present() {
        return;
    }

    let substrate = meta("substrate");

    // Without the filter the alias reads only `name`, which every chunk has.
    run(
        "moonbeam",
        &substrate,
        br#"{"type":"substrate","fromBlock":0,
             "fields":{"event":{"name":true}},
             "reviveContractEmitted":[{}]}"#,
    )
    .expect("an alias whose implicit predicate the chunk can answer is answerable");

    let err = run(
        "moonbeam",
        &substrate,
        br#"{"type":"substrate","fromBlock":0,
             "fields":{"event":{"name":true}},
             "reviveContractEmitted":[{"contract":["0xdead"]}]}"#,
    )
    .expect_err("filtering on a column the chunk lacks must error");

    assert!(
        err.root_cause().to_string().contains("_revive_contract"),
        "the error must name the missing column, got: {}",
        err.root_cause()
    );
}

/// Query items are alternatives, but the reference implementation still refuses
/// the whole request when any one of them names a column the chunk lacks —
/// verified against it directly. Pinned because "make the unanswerable item match
/// nothing instead" reads like the kinder behaviour and would silently diverge.
///
/// Covers CT-8 · INV-X3
#[test]
#[ignore = "requires external fixture data"]
fn one_unanswerable_item_rejects_the_whole_request() {
    if !fixture_tree_is_present() {
        return;
    }

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
// A chunk of another vintage: columns stored at a type the catalog does not name
// ---------------------------------------------------------------------------

/// Every integer width, signed and unsigned.
fn every_integer_width() -> Vec<DataType> {
    use DataType::{Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8};
    vec![UInt8, Int8, UInt16, Int16, UInt32, Int32, UInt64, Int64]
}

/// The error a query fails with on a rewritten chunk, with the kind checked and
/// the column named.
fn refusal(chunk: &std::path::Path, query: &str, kind: ErrorKind, column: &str, what: &str) {
    let err = match run_against(&sol_like::catalog(), chunk, query) {
        Err(e) => e,
        Ok(body) => panic!("{what} must be refused; got {} bytes instead", body.len()),
    };
    assert_eq!(error_kind(&err), Some(kind), "{what}: {err:#}");
    assert!(
        err.root_cause().to_string().contains(column),
        "{what}: the error must name '{column}', got: {}",
        err.root_cause()
    );
}

/// A filter on an integer column reads the column at whatever width the writer
/// stored it — Solana's `d1` — through both the plain list and the discriminator
/// that resolves onto it, and a filter on a flag reads a flag stored as a number.
///
/// Covers CT-8 · INV-D7
#[test]
fn an_integer_filter_reads_every_physical_width() {
    let meta = sol_like::catalog();
    let source = sol_like::chunk();

    let filters = [
        ("a discriminator list", r#"{"d1":["0x2a"]}"#.to_string()),
        (
            "the discriminator filter",
            r#"{"discriminator":["0x2a"]}"#.to_string(),
        ),
        ("an index list", r#"{"transactionIndex":[1,3]}"#.to_string()),
    ];
    for (column, widths) in [
        ("d1", every_integer_width()),
        ("transaction_index", every_integer_width()),
    ] {
        for width in widths {
            let retyped =
                chunk_with_column_retyped(source.path(), "instructions", column, width.clone());
            for (name, filter) in &filters {
                answers_the_same(
                    &meta,
                    &sol_like::query_with(filter),
                    source.path(),
                    retyped.path(),
                    &format!("{name} with instructions.{column} stored as {width:?}"),
                );
            }
        }
    }
}

/// Covers CT-8 · INV-D7
#[test]
fn a_boolean_filter_reads_a_flag_stored_as_a_number() {
    let meta = sol_like::catalog();
    let source = sol_like::chunk();
    let query = sol_like::query_selecting(
        r#"{"isCommitted":true}"#,
        r#"{"transactionIndex":true,"programId":true}"#,
    );

    for width in [DataType::Int8, DataType::UInt8, DataType::Int32] {
        let retyped =
            chunk_with_column_retyped(source.path(), "instructions", "is_committed", width.clone());
        answers_the_same(
            &meta,
            &query,
            source.path(),
            retyped.path(),
            &format!("is_committed stored as {width:?}"),
        );
    }
}

/// A filter whose values cannot be compared against the column as stored is an
/// error, never "matches nothing" — through every filter kind that reaches a
/// column: a list, a scalar flag, the discriminator. (A bloom over text is
/// refused the same way; no cast writes a chunk that shape, so the predicate's
/// own tests cover it.)
///
/// Covers CT-8 · INV-E7
#[test]
fn a_filter_on_an_uncomparable_column_is_an_error() {
    let source = sol_like::chunk();
    let index_only = r#"{"transactionIndex":true}"#;

    let cases: Vec<(&str, DataType, String)> = vec![
        (
            "d1",
            DataType::Utf8,
            sol_like::query_selecting(r#"{"d1":["0x2a"]}"#, index_only),
        ),
        (
            "d1",
            DataType::Utf8,
            sol_like::query_selecting(r#"{"discriminator":["0x2a"]}"#, index_only),
        ),
        (
            "is_committed",
            DataType::Utf8,
            sol_like::query_selecting(r#"{"isCommitted":true}"#, index_only),
        ),
        (
            "transaction_index",
            DataType::Utf8,
            sol_like::query_selecting(r#"{"transactionIndex":[1]}"#, index_only),
        ),
        (
            "program_id",
            DataType::UInt64,
            sol_like::query_selecting(r#"{"programId":["101"]}"#, index_only),
        ),
        // Bytes are not text: read as UTF-8 they never equal the literal, and
        // the chunk would answer "no rows" instead of refusing.
        (
            "program_id",
            DataType::Binary,
            sol_like::query_selecting(r#"{"programId":["101"]}"#, index_only),
        ),
        (
            "d1",
            DataType::Boolean,
            sol_like::query_selecting(r#"{"d1":["0x2a"]}"#, index_only),
        ),
    ];

    for (column, stored, query) in cases {
        let retyped =
            chunk_with_column_retyped(source.path(), "instructions", column, stored.clone());
        refusal(
            retyped.path(),
            &query,
            ErrorKind::UnsupportedKeyType,
            column,
            &format!("filtering on {column} stored as {stored:?}"),
        );
    }
}

/// Two shapes that used to panic: a bloom stored as plain bytes, and a
/// dictionary keyed by a width other than `Int32`. Neither is an error — the
/// values compare — so the answer must not move.
///
/// Covers CT-8 · INV-E7
#[test]
fn a_bloom_and_a_dictionary_of_any_key_width_are_read() {
    let meta = sol_like::catalog();
    let source = sol_like::chunk();
    let index_only = r#"{"transactionIndex":true}"#;

    for stored in [DataType::Binary, DataType::LargeBinary] {
        let retyped = chunk_with_column_retyped(
            source.path(),
            "instructions",
            "accounts_bloom",
            stored.clone(),
        );
        answers_the_same(
            &meta,
            &sol_like::query_selecting(r#"{"mentionsAccount":["acc-3"]}"#, index_only),
            source.path(),
            retyped.path(),
            &format!("accounts_bloom stored as {stored:?}"),
        );
    }

    for key in [DataType::Int8, DataType::UInt16, DataType::Int64] {
        let stored = DataType::Dictionary(Box::new(key), Box::new(DataType::Utf8));
        let retyped =
            chunk_with_column_retyped(source.path(), "instructions", "program_id", stored.clone());
        answers_the_same(
            &meta,
            &sol_like::query_selecting(r#"{"programId":["101"]}"#, index_only),
            source.path(),
            retyped.path(),
            &format!("program_id stored as {stored:?}"),
        );
    }
}

/// A selected column stored at a type nothing renders is an error, not a
/// `null` in every row. The chunk is refused off its schema, so a
/// query the column's rows never reach refuses too.
///
/// Covers CT-8 · INV-E3
#[test]
fn a_selected_column_nothing_renders_is_an_error() {
    let source = sol_like::chunk();
    let all = sol_like::query_with("{}");

    let cases = [
        (
            "instructions",
            "program_id",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        ),
        ("instructions", "a0", DataType::LargeUtf8),
        ("instructions", "a1", DataType::Utf8View),
        (
            "instructions",
            "rest_accounts",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
        ),
        (
            "blocks",
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
    ];

    for (table, column, stored) in cases {
        let retyped = chunk_with_column_retyped(source.path(), table, column, stored.clone());
        let what = format!("selecting {table}.{column} stored as {stored:?}");
        refusal(
            retyped.path(),
            &all,
            ErrorKind::MalformedChunkData,
            column,
            &what,
        );

        // Nothing in range: still refused.
        let empty = all.replace(
            &format!("\"toBlock\":{}", sol_like::BLOCKS.end()),
            &format!("\"toBlock\":{}", sol_like::BLOCKS.start() - 1),
        );
        let empty = empty.replace(
            &format!("\"fromBlock\":{}", sol_like::BLOCKS.start()),
            &format!("\"fromBlock\":{}", sol_like::BLOCKS.start() - 1),
        );
        refusal(
            retyped.path(),
            &empty,
            ErrorKind::MalformedChunkData,
            column,
            &format!("{what}, out of range"),
        );
    }
}

/// A roll field's sources are positional: a chunk written before one of them
/// existed puts the next account in its place, and a client reading `accounts[2]`
/// gets `accounts[3]`. The reference errors on the first missing
/// source, and so does this engine.
///
/// Covers CT-8 · INV-E3
#[test]
fn a_roll_field_with_a_missing_source_is_an_error() {
    let source = sol_like::chunk();

    for dropped in ["a2", "rest_accounts"] {
        let chunk = chunk_without_column_at(source.path(), "instructions", dropped);
        refusal(
            chunk.path(),
            &sol_like::query_with("{}"),
            ErrorKind::ColumnNotFound,
            dropped,
            &format!("selecting accounts without {dropped}"),
        );

        // The roll is the only thing that needs the column: without it selected
        // the same chunk answers.
        run_against(
            &sol_like::catalog(),
            chunk.path(),
            &sol_like::query_selecting("{}", r#"{"transactionIndex":true,"programId":true}"#),
        )
        .unwrap_or_else(|e| panic!("without accounts selected the chunk must answer: {e:#}"));
    }
}

/// A field that weighs through a `*_size` column cannot be weighed without it:
/// weighed at zero, the page is bounded by nothing but the transport.
///
/// Covers CT-8 · INV-B9
#[test]
fn a_missing_weight_column_is_an_error() {
    let metadata = catalog();
    let source = weighted_chunk(BLOCKS, &uniform(BLOCKS, 0), &[]);
    let chunk = chunk_without_column_at(source.path(), "logs", "data_size");

    let err = run_against(&metadata, chunk.path(), &logs_query().to_string())
        .expect_err("a weight column absent from the chunk must error when its field is selected");
    assert_eq!(error_kind(&err), Some(ErrorKind::ColumnNotFound));
    assert!(
        err.root_cause().to_string().contains("data_size"),
        "the error must name the missing weight column, got: {}",
        err.root_cause()
    );

    // The size column is the field's, not the table's: with the field
    // unselected the chunk answers.
    let mut without_data = logs_query();
    without_data["fields"]["log"] = serde_json::json!({"logIndex": true});
    run_against(&metadata, chunk.path(), &without_data.to_string())
        .expect("without the weighed field selected the chunk answers");
}

/// A `*_size` companion the weight model cannot read weighs its field at zero,
/// which disables the bound as surely as the column being absent does. The
/// encoder check let a companion stored as text through — text renders — so
/// the weight is checked as what it is: an integer.
///
/// Covers CT-8 · INV-B9
#[test]
fn a_weight_column_nothing_weighs_is_an_error() {
    let metadata = catalog();
    let source = weighted_chunk(BLOCKS, &uniform(BLOCKS, 0), &[]);

    for stored in [DataType::Utf8, DataType::Float64, DataType::Boolean] {
        let chunk = chunk_with_column_retyped(source.path(), "logs", "data_size", stored.clone());

        let err = match run_against(&metadata, chunk.path(), &logs_query().to_string()) {
            Err(e) => e,
            Ok(_) => panic!("data_size stored as {stored:?} must be refused"),
        };
        assert_eq!(
            error_kind(&err),
            Some(ErrorKind::MalformedChunkData),
            "{stored:?}: {err:#}"
        );
        assert!(
            err.root_cause().to_string().contains("data_size"),
            "the error must name the weight column, got: {}",
            err.root_cause()
        );

        let mut without_data = logs_query();
        without_data["fields"]["log"] = serde_json::json!({"logIndex": true});
        run_against(&metadata, chunk.path(), &without_data.to_string())
            .expect("without the weighed field selected the chunk answers");
    }
}

/// A sort key stored at a type nothing orders used to resolve to no sort
/// column at all, and the items came out in file order. The key is not
/// selected here, so the encoder check never sees it; the order check does.
///
/// Covers CT-8 · INV-E3
#[test]
fn a_sort_key_nothing_orders_is_an_error() {
    let source = sol_like::chunk();
    let retyped = chunk_with_column_retyped(
        source.path(),
        "instructions",
        "transaction_index",
        DataType::Boolean,
    );

    refusal(
        retyped.path(),
        &sol_like::query_selecting("{}", r#"{"programId":true}"#),
        ErrorKind::MalformedChunkData,
        "transaction_index",
        "ordering by transaction_index stored as Boolean",
    );
}

/// The Arrow output ships the column as stored and orders by an Arrow row
/// key, so a type the JSON encoder cannot render is not a type Arrow cannot
/// ship. The JSON pre-check refuses these chunks; the Arrow one must not.
///
/// Covers CT-8 · INV-E3
#[test]
fn the_arrow_output_ships_what_json_cannot_render() {
    let meta = sol_like::catalog();
    let source = sol_like::chunk();
    let query = sol_like::query_selecting("{}", r#"{"transactionIndex":true,"programId":true}"#);

    for stored in [
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ] {
        let retyped =
            chunk_with_column_retyped(source.path(), "instructions", "program_id", stored.clone());
        let what = format!("program_id stored as {stored:?}");

        refusal(
            retyped.path(),
            &query,
            ErrorKind::MalformedChunkData,
            "program_id",
            &format!("JSON, {what}"),
        );

        let parsed = parse_query(query.as_bytes(), &meta).unwrap();
        let plan = compile(&parsed, &meta).unwrap();
        let reader = ParquetChunkReader::open(retyped.path()).unwrap();
        let output = execute_chunk_arrow(&plan, &meta, &reader, false, false)
            .unwrap_or_else(|e| panic!("Arrow, {what}: must answer, got {e:#}"))
            .unwrap_or_else(|| panic!("Arrow, {what}: must carry the rows"));
        assert_eq!(
            output.num_blocks(),
            sol_like::BLOCKS.count(),
            "Arrow, {what}"
        );
    }
}

/// The variant column picks a row's groups; stored as a dictionary it used to
/// pick none, and every trace rendered without its `action` or `result`.
///
/// Covers CT-8 · INV-E3
#[test]
#[ignore = "requires external fixture data"]
fn a_variant_column_nothing_reads_is_an_error() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let chunk = chunk_with_column_retyped(
        &crate::harness::fixtures::fixture_chunk("ethereum"),
        "traces",
        "type",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    );
    let query = r#"{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                     "fields":{"trace":{"transactionIndex":true,"type":true,"callTo":true}},
                     "traces":[{"type":["call"]}]}"#;

    let err = run_against(&evm, chunk.path(), query)
        .expect_err("a variant column stored as a dictionary must error");
    assert_eq!(
        error_kind(&err),
        Some(ErrorKind::MalformedChunkData),
        "{err:#}"
    );
    assert!(
        err.root_cause().to_string().contains("type"),
        "the error must name the column, got: {}",
        err.root_cause()
    );
}
