//! Joins written by hand, against chunks written to disagree with their catalog.
//!
//! The synthetic chunks here pin what the fixture tree cannot express — a chunk
//! that disagrees with its catalog about the join key, a relation target naming
//! its own block column. The fixture-backed cases pin what a client sees: a row
//! is emitted once however many relations reach it, and a null key is not a key.
//!
//! A relation is answered by scanning the target table with the source's join
//! keys pushed into it. Both halves of that — the pushdown and the weight the
//! result is charged — go wrong quietly when the chunk does not match the
//! catalog, so both are pinned here against chunks written to disagree on
//! purpose.

use arrow::array::{ArrayRef, BinaryArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use sqd_query_engine::metadata::parse_dataset_description;
use std::sync::Arc;
use tempfile::TempDir;

use crate::harness::chunk::{blocks_parquet, write_parquet};
use crate::harness::fixtures::{fixture_tree_is_present, meta, run};
use crate::harness::json::items_of;
use crate::harness::synthetic::run_json as run_chunk;

// ---------------------------------------------------------------------------
// INV-B6 / INV-B10 — a row that is emitted is a row that is weighed
// ---------------------------------------------------------------------------

/// A table whose rows are identified by more than `item_order_keys`: a trace is a
/// transaction index *and* an address within it.
const TRACES: &str = r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    filters: []
    columns:
      number: { type: uint64 }
  traces:
    query_name: traces
    field_name: trace
    block_number_column: block_number
    item_order_keys: [transaction_index]
    address_column: trace_address
    sort_key: [block_number, transaction_index]
    filters: [ kind ]
    fields: [transaction_index, trace_address, kind, payload]
    relations:
      transaction_traces:
        table: traces
        key: [block_number, transaction_index]
    columns:
      block_number: { type: uint64 }
      transaction_index: { type: uint32 }
      trace_address: { type: uint32 }
      kind: { type: string }
      payload:
        type: string
        json_encoding: hex
        weight: payload_size
      payload_size: { type: uint64, system: true }
"#;

/// One transaction per block, `traces_per_tx` traces inside it, each claiming
/// `weight` bytes.
fn traces_chunk(blocks: &[u64], traces_per_tx: u32, weight: u64) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    blocks_parquet(dir.path(), blocks);

    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("trace_address", DataType::UInt32, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("payload_size", DataType::UInt64, false),
    ]));

    let rows: Vec<(u64, u32)> = blocks
        .iter()
        .flat_map(|&b| (0..traces_per_tx).map(move |a| (b, a)))
        .collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|(b, _)| *b).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32; rows.len()])) as ArrayRef,
            Arc::new(UInt32Array::from(
                rows.iter().map(|(_, a)| *a).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(vec!["call"; rows.len()])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![b"a".as_slice(); rows.len()])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![weight; rows.len()])) as ArrayRef,
        ],
    )
    .unwrap();
    write_parquet(&dir.path().join("traces.parquet"), &batch);

    dir
}

/// Weight dedups rows across the sources contributing to one table, and a trace
/// self-relation makes that the normal case for traces. Keying the dedup on
/// `item_order_keys` alone collapses every trace of a transaction into one, so a
/// block weighs a fraction of what it emits and the response runs past the cap
/// with nothing in it to say so.
#[test]
fn traces_of_one_transaction_are_weighed_separately() {
    let meta = parse_dataset_description(TRACES).unwrap();

    // Four traces per block at 3 MB each: one block fits the 20 MB cap, two do
    // not. Counting one trace per block would put both well inside it.
    const TRACES_PER_TX: u32 = 4;
    const TRACE_WEIGHT: u64 = 3 * 1024 * 1024;

    let chunk = traces_chunk(&[10, 11], TRACES_PER_TX, TRACE_WEIGHT);
    let query = r#"{"type":"test","fromBlock":10,"toBlock":11,
        "fields":{"trace":{"kind":true,"traceAddress":true,"payload":true}},
        "traces":[{"kind":["call"],"transactionTraces":true}]}"#;

    let blocks = run_chunk(&meta, &chunk, query).unwrap().unwrap();

    let emitted: usize = blocks
        .iter()
        .map(|b| b["traces"].as_array().map(|a| a.len()).unwrap_or(0))
        .sum();
    assert_eq!(
        emitted, TRACES_PER_TX as usize,
        "each trace is emitted once, and only the first block fits"
    );
    assert_eq!(
        blocks.len(),
        1,
        "two blocks of {TRACES_PER_TX} traces at {TRACE_WEIGHT} bytes exceed the cap, \
         so the second must be trimmed"
    );
}

// ---------------------------------------------------------------------------
// INV-X3 — a filter that cannot be evaluated must not widen the scan
// ---------------------------------------------------------------------------

const LOGS_AND_TXS: &str = r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    filters: []
    columns:
      number: { type: uint64 }
  logs:
    query_name: logs
    field_name: log
    block_number_column: block_number
    item_order_keys: [log_index]
    sort_key: [block_number, log_index]
    filters: [ address ]
    fields: [log_index, transaction_index, address]
    relations:
      transaction:
        table: transactions
        key: [block_number, transaction_index]
    columns:
      block_number: { type: uint64 }
      log_index: { type: uint32 }
      transaction_index: { type: uint32 }
      address: { type: string }
  transactions:
    query_name: transactions
    field_name: transaction
    block_number_column: block_number
    item_order_keys: [transaction_index]
    sort_key: [block_number, transaction_index]
    filters: []
    fields: [transaction_index, hash]
    columns:
      block_number: { type: uint64 }
      transaction_index: { type: uint32 }
      hash: { type: string }
"#;

/// A chunk whose `transactions` table is missing `transaction_index` — the shape
/// of an archive written before the column existed.
fn chunk_without_the_join_key() -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    blocks_parquet(dir.path(), &[10, 11]);

    let logs_schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("address", DataType::Utf8, false),
    ]));
    let logs = RecordBatch::try_new(
        logs_schema,
        vec![
            Arc::new(UInt64Array::from(vec![10u64, 11])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32, 0])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32, 0])) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(vec!["0xaa", "0xbb"])) as ArrayRef,
        ],
    )
    .unwrap();
    write_parquet(&dir.path().join("logs.parquet"), &logs);

    // No `transaction_index`, and several transactions per block, so a scan that
    // fails to filter returns rows the join would never have matched.
    let txs_schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("hash", DataType::Utf8, false),
    ]));
    let txs = RecordBatch::try_new(
        txs_schema,
        vec![
            Arc::new(UInt64Array::from(vec![10u64, 10, 11, 11])) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(vec![
                "0x1", "0x2", "0x3", "0x4",
            ])) as ArrayRef,
        ],
    )
    .unwrap();
    write_parquet(&dir.path().join("transactions.parquet"), &txs);

    dir
}

/// A relation's join key is as load-bearing as a predicate. When the target table
/// does not carry it the pushdown quietly drops itself, and assembly then skips
/// the join on the grounds that the pushdown already did the work — so every
/// transaction in the block range comes back attached to every log.
#[test]
fn a_relation_whose_join_key_is_missing_is_an_error() {
    let meta = parse_dataset_description(LOGS_AND_TXS).unwrap();
    let chunk = chunk_without_the_join_key();

    // The same query without the relation is answerable, so the failure below is
    // about the join key and not about the chunk in general.
    let without = run_chunk(
        &meta,
        &chunk,
        r#"{"type":"test","fromBlock":10,"toBlock":11,
            "fields":{"log":{"address":true}},"logs":[{}]}"#,
    );
    assert!(
        without.is_ok(),
        "the chunk answers a query that avoids the key"
    );

    let err = run_chunk(
        &meta,
        &chunk,
        r#"{"type":"test","fromBlock":10,"toBlock":11,
            "fields":{"log":{"address":true},"transaction":{"hash":true}},
            "logs":[{"transaction":true}]}"#,
    )
    .unwrap_err()
    .to_string();

    assert!(
        err.contains("transaction_index"),
        "the error must name the column the chunk lacks, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// A row is emitted once, however many relations reach it
// ---------------------------------------------------------------------------

/// A relation result carries the rows the source rows point at, and no others.
/// A null key is not a key: an event that belongs to no call used to serialize
/// byte-for-byte like an event whose call is the root one (address `[]`), so
/// asking for `call` returned every inherent's root call on top of the real
/// answer — extra rows, no filter that could exclude them, and nothing in the
/// response to say so.
#[test]
#[ignore = "requires external fixture data"]
fn a_null_join_key_matches_nothing() {
    if !fixture_tree_is_present() {
        return;
    }

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
#[ignore = "requires external fixture data"]
fn a_null_address_has_no_ancestors() {
    if !fixture_tree_is_present() {
        return;
    }

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
#[ignore = "requires external fixture data"]
fn stacked_relations_do_not_duplicate_rows() {
    if !fixture_tree_is_present() {
        return;
    }

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
#[ignore = "requires external fixture data"]
fn adding_an_overlapping_relation_changes_nothing() {
    if !fixture_tree_is_present() {
        return;
    }

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
