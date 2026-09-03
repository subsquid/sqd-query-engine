//! Relation invariants on synthetic chunks.
//!
//! A relation is answered by scanning the target table with the source's join
//! keys pushed into it. Both halves of that — the pushdown and the weight the
//! result is charged — go wrong quietly when the chunk does not match the
//! catalog, so both are pinned here against chunks written to disagree on
//! purpose. The fixture tree cannot express either shape.

use arrow::array::{ArrayRef, BinaryArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn blocks_parquet(dir: &Path, numbers: &[u64]) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(numbers.to_vec())) as ArrayRef],
    )
    .unwrap();
    write_parquet(&dir.join("blocks.parquet"), &batch);
}

fn run(
    meta: &DatasetDescription,
    chunk: &TempDir,
    query: &str,
) -> anyhow::Result<Option<Vec<serde_json::Value>>> {
    let parsed = parse_query(query.as_bytes(), meta)?;
    let plan = compile(&parsed, meta)?;
    let reader = ParquetChunkReader::open(chunk.path())?;
    let out = execute_chunk(&plan, meta, &reader, false)?;

    Ok(out.map(|o| {
        String::from_utf8(o.into_json_lines())
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }))
}

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

    let blocks = run(&meta, &chunk, query).unwrap().unwrap();

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
    let without = run(
        &meta,
        &chunk,
        r#"{"type":"test","fromBlock":10,"toBlock":11,
            "fields":{"log":{"address":true}},"logs":[{}]}"#,
    );
    assert!(
        without.is_ok(),
        "the chunk answers a query that avoids the key"
    );

    let err = run(
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
// INV-X1 — a column name the engine knows is a column name the catalog lost
// ---------------------------------------------------------------------------

/// A chain that calls its block number `height`, with a relation onto a table
/// that also carries an unrelated column named `block_number` — the archive's
/// note of which block a receipt *refers to*, not the block it is in.
const HEIGHT_CHAIN: &str = r#"
name: test
tables:
  blocks:
    field_name: block
    block_number_column: height
    sort_key: [height]
    filters: []
    fields: [height]
    columns:
      height: { type: uint64 }
  events:
    query_name: events
    field_name: event
    block_number_column: height
    item_order_keys: [seq]
    sort_key: [height, seq]
    filters: [ kind ]
    fields: [seq, kind]
    relations:
      receipt:
        table: receipts
        key: [height, seq]
    columns:
      height: { type: uint64 }
      seq: { type: uint32 }
      kind: { type: string }
  receipts:
    query_name: receipts
    field_name: receipt
    block_number_column: height
    item_order_keys: [seq]
    sort_key: [height, seq]
    filters: []
    # `block_number` here is the block a receipt *cites*, an ordinary data
    # column of this chain and a selectable field like any other.
    fields: [seq, block_number, status]
    columns:
      height: { type: uint64 }
      seq: { type: uint32 }
      block_number: { type: uint64 }
      status: { type: string }
"#;

/// One event and one receipt per block. Every receipt cites block 1, which is
/// not in the chunk at all.
fn height_chain_chunk(blocks: &[u64]) -> TempDir {
    let dir = tempfile::tempdir().unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "height",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(blocks.to_vec())) as ArrayRef],
    )
    .unwrap();
    write_parquet(&dir.path().join("blocks.parquet"), &batch);

    let events_schema = Arc::new(Schema::new(vec![
        Field::new("height", DataType::UInt64, false),
        Field::new("seq", DataType::UInt32, false),
        Field::new("kind", DataType::Utf8, false),
    ]));
    let events = RecordBatch::try_new(
        events_schema,
        vec![
            Arc::new(UInt64Array::from(blocks.to_vec())) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32; blocks.len()])) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(vec!["call"; blocks.len()])) as ArrayRef,
        ],
    )
    .unwrap();
    write_parquet(&dir.path().join("events.parquet"), &events);

    let receipts_schema = Arc::new(Schema::new(vec![
        Field::new("height", DataType::UInt64, false),
        Field::new("seq", DataType::UInt32, false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let receipts = RecordBatch::try_new(
        receipts_schema,
        vec![
            Arc::new(UInt64Array::from(blocks.to_vec())) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32; blocks.len()])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![1u64; blocks.len()])) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(vec!["ok"; blocks.len()])) as ArrayRef,
        ],
    )
    .unwrap();
    write_parquet(&dir.path().join("receipts.parquet"), &receipts);

    dir
}

/// Block selection read the block number of a relation's rows under the literal
/// name `block_number`. Every table in every shipped catalog happens to use it,
/// so the engine worked — until a chain that does not, or a table that uses the
/// name for something else. Both are catalog edits, and INV-X1 says a catalog
/// edit is all it takes to serve a new chain.
#[test]
fn a_relation_target_names_its_own_block_column() {
    let meta = parse_dataset_description(HEIGHT_CHAIN).unwrap();
    let chunk = height_chain_chunk(&[10, 11, 12]);

    let blocks = run(
        &meta,
        &chunk,
        r#"{"type":"test","fromBlock":10,"toBlock":12,
            "fields":{"block":{"height":true},"event":{"kind":true},
                      "receipt":{"blockNumber":true,"status":true}},
            "events":[{"kind":["call"],"receipt":true}]}"#,
    )
    .unwrap()
    .unwrap();

    let heights: Vec<u64> = blocks
        .iter()
        .map(|b| b["header"]["height"].as_u64().unwrap())
        .collect();
    assert_eq!(
        heights,
        vec![10, 11, 12],
        "the response carries the blocks the chunk has, not the ones a receipt cites"
    );

    let receipts: usize = blocks
        .iter()
        .map(|b| b["receipts"].as_array().map(|a| a.len()).unwrap_or(0))
        .sum();
    assert_eq!(receipts, 3, "one receipt joins each event");
}
