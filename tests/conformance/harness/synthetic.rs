//! A minimal catalog and the chunks written against it.
//!
//! Two classes drive this one: CT-5 makes budget trimming observable by claiming
//! an item weight the rows do not have, and CT-6 reads the block framing back off
//! the same chunks. It lives here rather than with either because a second copy
//! would answer a slightly different question under the same name.

use arrow::array::{ArrayRef, BinaryArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use sqd_query_engine::output::{execute_chunk, QueryOutput};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::sync::Arc;
use tempfile::TempDir;

use crate::harness::chunk::{blocks_parquet, write_table};

/// `data`/`input` declare their weight through a system column, so a test can
/// state what a row costs without writing a row that large.
const CATALOG: &str = r#"
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
    relations:
      transaction:
        table: transactions
        left_key: [block_number, log_index]
        right_key: [block_number, transaction_index]
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

pub const MB: u64 = 1024 * 1024;

pub const BLOCKS: &[u64] = &[10, 11, 12, 13, 14];

pub fn catalog() -> DatasetDescription {
    parse_dataset_description(CATALOG).unwrap()
}

/// A chunk with the given blocks and one item per `(block, weight)` entry in each
/// item table. `logs`/`txs` map a block number to the item weight it claims.
pub fn weighted_chunk(blocks: &[u64], logs: &[(u64, u64)], txs: &[(u64, u64)]) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    blocks_parquet(dir.path(), blocks);

    for (table, index_col, data_col, size_col, rows) in [
        ("logs", "log_index", "data", "data_size", logs),
        (
            "transactions",
            "transaction_index",
            "input",
            "input_size",
            txs,
        ),
    ] {
        write_table(
            dir.path(),
            table,
            vec![
                Field::new("block_number", DataType::UInt64, false),
                Field::new(index_col, DataType::UInt32, false),
                Field::new(data_col, DataType::Binary, false),
                Field::new(size_col, DataType::UInt64, false),
            ],
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
        );
    }

    dir
}

/// Every block carrying one item of the given weight.
pub fn uniform(blocks: &[u64], weight: u64) -> Vec<(u64, u64)> {
    blocks.iter().map(|&b| (b, weight)).collect()
}

/// A request for every log in `BLOCKS`, with the block number selected so the
/// framing is readable.
pub fn logs_query() -> serde_json::Value {
    serde_json::json!({
        "type": "test",
        "fromBlock": 10,
        "toBlock": 14,
        "logs": [{}],
        "fields": {"block": {"number": true}, "log": {"data": true}}
    })
}

/// Run a query against a synthetic chunk, keeping the output object: the block
/// range it reports is what most of these tests assert on.
pub fn run(
    meta: &DatasetDescription,
    chunk: &TempDir,
    query: serde_json::Value,
) -> Option<QueryOutput> {
    let parsed = parse_query(query.to_string().as_bytes(), meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    let reader = ParquetChunkReader::open(chunk.path()).unwrap();

    execute_chunk(&plan, meta, &reader, false).unwrap()
}

/// Run a query against any synthetic chunk and parse the response, propagating
/// the error a deliberately unanswerable request produces.
pub fn run_json(
    meta: &DatasetDescription,
    chunk: &TempDir,
    query: &str,
) -> anyhow::Result<Option<Vec<serde_json::Value>>> {
    let parsed = parse_query(query.as_bytes(), meta)?;
    let plan = compile(&parsed, meta)?;
    let reader = ParquetChunkReader::open(chunk.path())?;

    Ok(execute_chunk(&plan, meta, &reader, false)?
        .map(|out| crate::harness::json::parse_response(&out.into_json_lines())))
}
