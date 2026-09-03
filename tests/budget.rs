//! Response-budget and block-selection tests on synthetic chunks, covering the
//! spots the fixture suite can't reach: budget trimming (via oversized weight
//! columns), boundary-block handling under the phase-1 scan cutoff, empty
//! results, and JSON/Arrow parity of the reported block range.

use arrow::array::{ArrayRef, BinaryArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use sqd_query_engine::output::{execute_chunk, execute_chunk_arrow, QueryOutput};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

const MB: u64 = 1024 * 1024;

const SCHEMA: &str = r#"
name: test
tables:
  blocks:
    block_number_column: number
    field_name: block
    sort_key: [number]
    filters: []
    fields: [number]
    columns:
      number:
        type: uint64
  logs:
    query_name: logs
    field_name: log
    block_number_column: block_number
    item_order_keys: [log_index]
    sort_key: [block_number, log_index]
    filters: []
    fields: [log_index, data]
    columns:
      block_number:
        type: uint64
      log_index:
        type: uint32
      data:
        type: string
        json_encoding: hex
        weight: data_size
      data_size:
        type: uint64
        system: true
  transactions:
    query_name: transactions
    field_name: transaction
    block_number_column: block_number
    item_order_keys: [transaction_index]
    sort_key: [block_number, transaction_index]
    filters: []
    fields: [transaction_index, input]
    columns:
      block_number:
        type: uint64
      transaction_index:
        type: uint32
      input:
        type: string
        json_encoding: hex
        weight: input_size
      input_size:
        type: uint64
        system: true
"#;

fn schema() -> DatasetDescription {
    parse_dataset_description(SCHEMA).unwrap()
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

/// A chunk with blocks 10..=14 and one item per (block, weight) entry in each
/// item table. `logs`/`txs` map block number → claimed item weight in bytes.
fn make_chunk(blocks: &[u64], logs: &[(u64, u64)], txs: &[(u64, u64)]) -> TempDir {
    let dir = tempfile::tempdir().unwrap();

    let blocks_schema = Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        blocks_schema,
        vec![Arc::new(UInt64Array::from(blocks.to_vec())) as ArrayRef],
    )
    .unwrap();
    write_parquet(&dir.path().join("blocks.parquet"), &batch);

    for (table, data_col, size_col, rows) in [
        ("logs", "data", "data_size", logs),
        ("transactions", "input", "input_size", txs),
    ] {
        let index_col = if table == "logs" {
            "log_index"
        } else {
            "transaction_index"
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new(index_col, DataType::UInt32, false),
            Field::new(data_col, DataType::Binary, false),
            Field::new(size_col, DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(b, _)| *b).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0u32; rows.len()])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![b"a".as_slice(); rows.len()])) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, w)| *w).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap();
        write_parquet(&dir.path().join(format!("{table}.parquet")), &batch);
    }

    dir
}

fn run(
    meta: &DatasetDescription,
    chunk: &TempDir,
    query: serde_json::Value,
) -> Option<QueryOutput> {
    let parsed = parse_query(query.to_string().as_bytes(), meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    let reader = ParquetChunkReader::open(chunk.path()).unwrap();
    execute_chunk(&plan, meta, &reader, false).unwrap()
}

fn parse_lines(bytes: &[u8]) -> Vec<serde_json::Value> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn block_numbers(blocks: &[serde_json::Value]) -> Vec<u64> {
    blocks
        .iter()
        .map(|b| b["header"]["number"].as_u64().unwrap())
        .collect()
}

const BLOCKS: &[u64] = &[10, 11, 12, 13, 14];

fn logs_query() -> serde_json::Value {
    serde_json::json!({
        "type": "test",
        "fromBlock": 10,
        "toBlock": 14,
        "logs": [{}],
        "fields": {"block": {"number": true}, "log": {"data": true}}
    })
}

/// Regression test for the phase-1 cutoff bug: a single-table full scan whose
/// budget cutoff falls below `toBlock` must not include the range-end boundary
/// block (its item rows were never scanned) — neither in the output nor in the
/// reported last block.
#[test]
fn budget_trim_excludes_range_end_boundary_block() {
    let chunk = make_chunk(
        BLOCKS,
        &BLOCKS.iter().map(|&b| (b, 15 * MB)).collect::<Vec<_>>(),
        &[],
    );
    let meta = schema();
    let blocks = run(&meta, &chunk, logs_query()).unwrap();

    assert_eq!(blocks.num_blocks(), 1);
    assert_eq!(blocks.first_block(), 10);
    assert_eq!(blocks.last_block(), 10);

    let lines = parse_lines(&blocks.into_json_lines());
    assert_eq!(block_numbers(&lines), vec![10]);
    assert_eq!(lines[0]["logs"].as_array().unwrap().len(), 1);
}

/// Without trimming, every item block is included and the reported range spans
/// the whole data.
#[test]
fn untrimmed_scan_includes_all_blocks() {
    let chunk = make_chunk(
        BLOCKS,
        &BLOCKS.iter().map(|&b| (b, 10)).collect::<Vec<_>>(),
        &[],
    );
    let meta = schema();
    let blocks = run(&meta, &chunk, logs_query()).unwrap();

    assert_eq!((blocks.first_block(), blocks.last_block()), (10, 14));
    let lines = parse_lines(&blocks.into_json_lines());
    assert_eq!(block_numbers(&lines), BLOCKS);
}

/// The first/last blocks of the scanned range are emitted as header-only
/// entries even when they contain no matching items.
#[test]
fn boundary_blocks_emitted_without_items() {
    let chunk = make_chunk(BLOCKS, &[(12, 10)], &[]);
    let meta = schema();
    let blocks = run(&meta, &chunk, logs_query()).unwrap();

    let lines = parse_lines(&blocks.into_json_lines());
    assert_eq!(block_numbers(&lines), vec![10, 12, 14]);
    assert!(lines[0].get("logs").is_none());
    assert_eq!(lines[1]["logs"].as_array().unwrap().len(), 1);
    assert!(lines[2].get("logs").is_none());
}

/// A query over a range with no data yields `None`, on both output formats.
#[test]
fn empty_result_is_none() {
    let chunk = make_chunk(BLOCKS, &[(12, 10)], &[]);
    let query = serde_json::json!({
        "type": "test",
        "fromBlock": 100,
        "toBlock": 200,
        "logs": [{}],
        "fields": {"log": {"data": true}}
    });
    assert!(run(&schema(), &chunk, query.clone()).is_none());

    let meta = schema();
    let parsed = parse_query(query.to_string().as_bytes(), &meta).unwrap();
    let plan = compile(&parsed, &meta).unwrap();
    let reader = ParquetChunkReader::open(chunk.path()).unwrap();
    assert!(execute_chunk_arrow(&plan, &meta, &reader, false, false)
        .unwrap()
        .is_none());
}

/// Budget trimming with two item tables (the wave-budget path, not phase-1):
/// per-block weight is summed across tables, and the reported range reflects
/// the trim.
#[test]
fn multi_table_trim_reports_true_last_block() {
    let weights: Vec<(u64, u64)> = BLOCKS.iter().map(|&b| (b, 15 * MB)).collect();
    let chunk = make_chunk(BLOCKS, &weights, &weights);
    let query = serde_json::json!({
        "type": "test",
        "fromBlock": 10,
        "toBlock": 14,
        "logs": [{}],
        "transactions": [{}],
        "fields": {
            "block": {"number": true},
            "log": {"data": true},
            "transaction": {"input": true}
        }
    });
    let meta = schema();
    let blocks = run(&meta, &chunk, query).unwrap();

    // 15MB (logs) + 15MB (transactions) per block: only the first block fits.
    assert_eq!(blocks.last_block(), 10);
    let lines = parse_lines(&blocks.into_json_lines());
    assert_eq!(block_numbers(&lines), vec![10]);
    assert_eq!(lines[0]["logs"].as_array().unwrap().len(), 1);
    assert_eq!(lines[0]["transactions"].as_array().unwrap().len(), 1);
}

/// Block-by-block iteration produces the same bytes as `into_json_lines`
/// (modulo framing), iteration state is tracked correctly, and
/// `into_json_lines` re-encodes everything regardless of prior iteration.
#[test]
fn iteration_matches_json_lines() {
    let meta = schema();
    let chunk = make_chunk(
        BLOCKS,
        &BLOCKS.iter().map(|&b| (b, 10)).collect::<Vec<_>>(),
        &[],
    );

    let mut blocks = run(&meta, &chunk, logs_query()).unwrap();
    let mut iterated = Vec::new();
    let mut count = 0;
    while blocks.has_next_block() {
        blocks.write_next_block(&mut iterated);
        iterated.push(b'\n');
        count += 1;
    }
    assert_eq!(count, blocks.num_blocks());

    // Consumed iterator still re-encodes everything.
    assert_eq!(blocks.into_json_lines(), iterated);

    let mut partial = run(&meta, &chunk, logs_query()).unwrap();
    partial.write_next_block(&mut Vec::new());
    assert_eq!(partial.into_json_lines(), iterated);
}

#[test]
#[should_panic]
fn write_next_block_panics_when_exhausted() {
    let meta = schema();
    let chunk = make_chunk(&[10], &[(10, 10)], &[]);
    let mut blocks = run(&meta, &chunk, logs_query()).unwrap();
    let mut out = Vec::new();
    blocks.write_next_block(&mut out);
    blocks.write_next_block(&mut out);
}

/// The Arrow output reports the same trimmed block range as the JSON output.
#[test]
fn arrow_json_parity_on_trim() {
    let chunk = make_chunk(
        BLOCKS,
        &BLOCKS.iter().map(|&b| (b, 15 * MB)).collect::<Vec<_>>(),
        &[],
    );
    let meta = schema();
    let parsed = parse_query(logs_query().to_string().as_bytes(), &meta).unwrap();
    let plan = compile(&parsed, &meta).unwrap();
    let reader = ParquetChunkReader::open(chunk.path()).unwrap();

    let json = execute_chunk(&plan, &meta, &reader, false)
        .unwrap()
        .unwrap();
    let arrow = execute_chunk_arrow(&plan, &meta, &reader, false, false)
        .unwrap()
        .unwrap();

    assert_eq!(arrow.num_blocks(), json.num_blocks());
    assert_eq!(arrow.first_block(), json.first_block());
    assert_eq!(arrow.last_block(), json.last_block());
    assert!(!arrow.data().is_empty());
}
