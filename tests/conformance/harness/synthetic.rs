//! A minimal catalog and the chunks written against it.
//!
//! Two classes drive this one: CT-5 makes budget trimming observable by claiming
//! an item weight the rows do not have, and CT-6 reads the block framing back off
//! the same chunks. It lives here rather than with either because a second copy
//! would answer a slightly different question under the same name.

use arrow::array::{ArrayRef, BinaryArray, UInt16Array, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use sqd_query_engine::output::{execute_chunk, QueryOutput};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::sync::Arc;
use tempfile::TempDir;

use crate::harness::chunk::{blocks_parquet, write_table, write_table_row_groups};
use crate::harness::json::items_of;

/// `data`/`input` declare their weight through a system column, so a test can
/// state what a row costs without writing a row that large.
const CATALOG: &str = r#"
name: test

tables:
  blocks:
    output:
      name: block
      fields: [number]
    block_number_column: number
    sort_key: [number]
    columns:
      number:
        type: uint64

  logs:
    request:
      name: logs
      filters: []
      relations:
        transaction:
          table: transactions
          left_key: [block_number, log_index]
          right_key: [block_number, transaction_index]
    output:
      name: log
      fields: [log_index, data]
    block_number_column: block_number
    item_order_keys: [log_index]
    sort_key: [block_number, log_index]
    columns:
      block_number:
        type: uint64
      log_index:
        type: uint32
      data:
        type: string
        encoding: hex_bytes
        weight: data_size
      data_size:
        type: uint64
        system: true

  transactions:
    request:
      name: transactions
      filters: []
    output:
      name: transaction
      fields: [transaction_index, input]
    block_number_column: block_number
    item_order_keys: [transaction_index]
    sort_key: [block_number, transaction_index]
    columns:
      block_number:
        type: uint64
      transaction_index:
        type: uint32
      input:
        type: string
        encoding: hex_bytes
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

/// The same chunk with every integer narrowed to sixteen bits — the block
/// number, the item index and the `*_size` companion the weight model reads.
///
/// A writer narrows an integer to the width the chunk's values need, so a `uint64`
/// block number and a `uint64` size arrive as `UInt16` in a chunk whose blocks and
/// rows are small enough. Nothing about the response may follow from that
/// (INV-D7), and the weight model is where it is easiest for it to: a size the
/// model cannot read weighs zero, so the rows are emitted and never counted, and
/// the budget it was supposed to enforce is enforced on nothing.
///
/// `logs` only, and one log per row of `weights`, so the arithmetic is legible:
/// the response weight is the sum of the sizes plus each row's fixed part.
pub fn narrow_weighted_chunk(blocks: &[u64], logs: &[(u64, u64)]) -> TempDir {
    let dir = tempfile::tempdir().unwrap();

    write_table(
        dir.path(),
        "blocks",
        vec![Field::new("number", DataType::UInt16, false)],
        vec![Arc::new(UInt16Array::from(
            blocks.iter().map(|&b| b as u16).collect::<Vec<_>>(),
        )) as ArrayRef],
    );

    write_table(
        dir.path(),
        "logs",
        vec![
            Field::new("block_number", DataType::UInt16, false),
            Field::new("log_index", DataType::UInt16, false),
            Field::new("data", DataType::Binary, false),
            Field::new("data_size", DataType::UInt16, false),
        ],
        vec![
            Arc::new(UInt16Array::from(
                logs.iter().map(|(b, _)| *b as u16).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt16Array::from(vec![0u16; logs.len()])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![b"a".as_slice(); logs.len()])) as ArrayRef,
            Arc::new(UInt16Array::from(
                logs.iter().map(|(_, w)| *w as u16).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    );

    dir
}

/// Every block carrying one item of the given weight.
pub fn uniform(blocks: &[u64], weight: u64) -> Vec<(u64, u64)> {
    blocks.iter().map(|&b| (b, weight)).collect()
}

// ---------------------------------------------------------------------------
// A block-partitioned chunk: the shape the budget walk exists for
// ---------------------------------------------------------------------------
//
// Row group `g` of `logs` owns five blocks and *shares its boundary block* with
// row group `g + 1` — half that block's logs are written on each side of the
// line, which is how the archiver writes a chunk today. The fixture chunks have
// no such layout: every row group there spans the whole range.
//
// The shared block is deliberately far heavier than the blocks below it, which
// is what separates the two weights a walk could stop on. Everything it has read
// crosses the budget on the half-block it may not emit; the blocks it may emit
// are still well under. A walk that stops on the first has read almost nothing
// it can serve, and a response assembled from it ends far past where the data
// does.
//
// `transactions` is one row group over the whole range, so a query naming both
// has a table the walk cuts and a table it reads whole — which is what lets a
// block above the cut come back looking complete.

const PART_BLOCKS_PER_GROUP: u64 = 5;
const PART_GROUPS: u64 = 12;
/// Logs in a block two row groups share, split evenly between them.
const PART_SHARED_LOGS: u64 = 40;
/// Logs in every other block.
const PART_PLAIN_LOGS: u64 = 1;

/// Every block the partitioned chunk holds.
pub fn part_blocks() -> Vec<u64> {
    (0..PART_GROUPS * PART_BLOCKS_PER_GROUP).collect()
}

/// A block two row groups share carries the heavy load; every other block one
/// log. Block 0 is nobody's boundary.
fn part_logs_in(block: u64) -> u64 {
    if block > 0 && block.is_multiple_of(PART_BLOCKS_PER_GROUP) {
        PART_SHARED_LOGS
    } else {
        PART_PLAIN_LOGS
    }
}

/// Every `(block, log_index)` the chunk holds, which is what paging it has to
/// add back up to.
pub fn part_log_rows() -> Vec<(u64, u64)> {
    part_blocks()
        .into_iter()
        .flat_map(|block| (0..part_logs_in(block)).map(move |index| (block, index)))
        .collect()
}

/// The `(block, log_index)` rows row group `g` carries: the blocks it owns
/// whole, plus half of the block it shares with each neighbour.
fn part_group_rows(group: u64) -> Vec<(u64, u32)> {
    let first = group * PART_BLOCKS_PER_GROUP;
    let last = first + PART_BLOCKS_PER_GROUP;

    let mut rows = Vec::new();
    for block in first..=last {
        let logs = part_logs_in(block);
        let (from, to) = match block {
            b if b == first && group > 0 => (logs / 2, logs),
            b if b == last && group + 1 < PART_GROUPS => (0, logs / 2),
            b if b == last => continue, // the file ends before the next group's half
            _ => (0, logs),
        };
        for index in from..to {
            rows.push((block, index as u32));
        }
    }
    rows
}

/// Write that chunk, with every log claiming `log_weight` bytes.
pub fn partitioned_chunk(log_weight: u64) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let blocks = part_blocks();
    blocks_parquet(dir.path(), &blocks);

    let log_fields = vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt32, false),
        Field::new("data", DataType::Binary, false),
        Field::new("data_size", DataType::UInt64, false),
    ];
    let groups: Vec<Vec<ArrayRef>> = (0..PART_GROUPS)
        .map(|group| {
            let rows = part_group_rows(group);
            vec![
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(b, _)| *b).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt32Array::from(
                    rows.iter().map(|(_, i)| *i).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(BinaryArray::from(vec![b"a".as_slice(); rows.len()])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![log_weight; rows.len()])) as ArrayRef,
            ]
        })
        .collect();
    write_table_row_groups(dir.path(), "logs", log_fields, groups);

    // One transaction per block, weighing next to nothing, in a single row
    // group: a table the walk reads whole while the other one stops.
    write_table(
        dir.path(),
        "transactions",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("input", DataType::Binary, false),
            Field::new("input_size", DataType::UInt64, false),
        ],
        vec![
            Arc::new(UInt64Array::from(blocks.clone())) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32; blocks.len()])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![b"a".as_slice(); blocks.len()])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![1u64; blocks.len()])) as ArrayRef,
        ],
    );

    dir
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

/// A block's logs, as `(block, log_index)`.
fn log_rows(body: &[u8]) -> Vec<(u64, u64)> {
    items_of(body, "logs")
        .into_iter()
        .map(|(block, item)| (block, item["logIndex"].as_u64().unwrap()))
        .collect()
}

fn paging_query(from: u64, to: u64) -> String {
    serde_json::json!({
        "type": "test",
        "fromBlock": from,
        "toBlock": to,
        "logs": [{}],
        "transactions": [{}],
        "fields": {
            "block": {"number": true},
            "log": {"logIndex": true, "data": true},
            "transaction": {"transactionIndex": true}
        }
    })
    .to_string()
}

/// Page a chunk end to end the way a client does: ask from `lastBlock + 1` until
/// the range runs out. Returns every log seen, in order, and where each page
/// ended.
pub fn page_through(
    meta: &DatasetDescription,
    chunk: &TempDir,
    to: u64,
) -> (Vec<(u64, u64)>, Vec<u64>) {
    let reader = ParquetChunkReader::open(chunk.path()).unwrap();

    let mut logs = Vec::new();
    let mut last_blocks = Vec::new();
    let mut from = 0u64;

    while from <= to {
        let query = paging_query(from, to);
        let parsed = parse_query(query.as_bytes(), meta).unwrap();
        let plan = compile(&parsed, meta).unwrap();

        let Some(output) = execute_chunk(&plan, meta, &reader, false).unwrap() else {
            break;
        };
        let last = output.last_block();
        logs.extend(log_rows(&output.into_json_lines()));
        last_blocks.push(last);

        assert!(
            last >= from,
            "a page that ends below where it started leaves the client with nowhere to go"
        );
        from = last + 1;

        assert!(
            last_blocks.len() < 100,
            "paging did not terminate: {last_blocks:?}"
        );
    }

    (logs, last_blocks)
}

/// The same, in a pool of the given size: the wave is as wide as the pool, so
/// the pool decides how far each wave over-reads.
pub fn paged_at(
    meta: &DatasetDescription,
    chunk: &TempDir,
    to: u64,
    threads: usize,
) -> (Vec<(u64, u64)>, Vec<u64>) {
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap()
        .install(|| page_through(meta, chunk, to))
}
