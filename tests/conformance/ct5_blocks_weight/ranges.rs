use crate::harness::chunk::{chunk_relaid, repartition, write_table_row_groups, Layout};
use crate::harness::evm_like;
use crate::harness::json::{assert_same_response, block_numbers, parse_response};
use crate::harness::synthetic::{catalog, weighted_chunk, MB};
use arrow::array::{ArrayRef, BinaryArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::output::{execute_chunk_with, ExecOptions};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest};
use std::path::Path;
use std::sync::{Arc, Mutex};

struct Read {
    table: String,
    from: Option<u64>,
    to: Option<u64>,
    rows: usize,
    columns: Vec<String>,
}

struct ObservedReader {
    inner: ParquetChunkReader,
    span: Option<u64>,
    reads: Mutex<Vec<Read>>,
}

impl ObservedReader {
    fn new(path: &Path, span: Option<u64>) -> Self {
        Self {
            inner: ParquetChunkReader::open(path).unwrap(),
            span,
            reads: Mutex::new(Vec::new()),
        }
    }
}

impl ChunkReader for ObservedReader {
    fn scan(&self, table: &str, request: &ScanRequest) -> anyhow::Result<Vec<RecordBatch>> {
        let batches = self.inner.scan(table, request)?;
        self.reads.lock().unwrap().push(Read {
            table: table.into(),
            from: request.from_block,
            to: request.to_block,
            rows: batches.iter().map(RecordBatch::num_rows).sum(),
            columns: request
                .output_columns
                .iter()
                .map(|c| c.to_string())
                .collect(),
        });
        Ok(batches)
    }
    fn next_block_range_end(&self, table: &str, column: &str, from: u64) -> Option<u64> {
        self.span
            .map(|span| from.saturating_add(span - 1))
            .or_else(|| self.inner.next_block_range_end(table, column, from))
    }
    fn has_table(&self, table: &str) -> bool {
        self.inner.has_table(table)
    }
    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.inner.table_schema(table)
    }
}

fn run(
    meta: &DatasetDescription,
    reader: &dyn ChunkReader,
    query: &str,
    budget: u64,
    ranges: bool,
) -> Vec<u8> {
    let plan = compile(&parse_query(query.as_bytes(), meta).unwrap(), meta).unwrap();
    execute_chunk_with(
        &plan,
        meta,
        reader,
        ExecOptions {
            weight_budget: budget,
            range_reads: ranges,
            ..ExecOptions::default()
        },
    )
    .unwrap()
    .map(|out| out.into_json_lines())
    .unwrap_or_default()
}

fn query(from: u64, to: u64) -> String {
    serde_json::json!({"type":"test", "fromBlock":from, "toBlock":to,
        "logs":[{}], "transactions":[{}],
        "fields":{"block":{"number":true},"log":{"logIndex":true,"data":true},
        "transaction":{"transactionIndex":true,"input":true}}})
    .to_string()
}

/// Covers CT-5 · INV-B4
#[test]
fn an_oversized_first_block_does_not_restart_the_query() {
    let blocks: Vec<_> = (0..60).collect();
    let logs: Vec<_> = blocks.iter().map(|&b| (b, 30 * MB)).collect();
    let txs: Vec<_> = blocks.iter().map(|&b| (b, 1)).collect();
    let chunk = weighted_chunk(&blocks, &logs, &txs);
    repartition(chunk.path(), "logs", 1);
    let meta = catalog();
    let query = query(0, 59);
    for threads in [1, 2, 17] {
        let reader = ObservedReader::new(chunk.path(), Some(1));
        let response = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap()
            .install(|| run(&meta, &reader, &query, 20 * MB, true));
        assert_eq!(block_numbers(&parse_response(&response)), vec![0]);
        let reads = reader.reads.lock().unwrap();
        for table in ["logs", "transactions"] {
            let reads: Vec<_> = reads.iter().filter(|r| r.table == table).collect();
            assert_eq!(reads.len(), 1, "{table} was read again");
            assert_eq!(
                (reads[0].from, reads[0].to, reads[0].rows),
                (Some(0), Some(0), 1)
            );
        }
    }
}

/// Covers CT-5 · INV-B6
#[test]
fn the_narrow_prescan_limits_wide_column_decoding() {
    let blocks: Vec<_> = (0..60).collect();
    let rows: Vec<_> = blocks.iter().map(|&b| (b, 30 * MB)).collect();
    let chunk = weighted_chunk(&blocks, &rows, &[]);
    let mut query: serde_json::Value = serde_json::from_str(&query(0, 59)).unwrap();
    query.as_object_mut().unwrap().remove("transactions");
    let reader = ObservedReader::new(chunk.path(), None);
    let response = run(&catalog(), &reader, &query.to_string(), 20 * MB, true);
    assert_eq!(block_numbers(&parse_response(&response)), vec![0]);
    let reads = reader.reads.lock().unwrap();
    let wide: Vec<_> = reads
        .iter()
        .filter(|r| r.table == "logs" && r.columns.iter().any(|c| c == "data"))
        .collect();
    assert_eq!(wide.len(), 1);
    assert_eq!(wide[0].rows, 1);
}

/// Covers CT-5 · INV-B3
#[test]
fn internal_range_boundaries_do_not_emit_headers() {
    let chunk = weighted_chunk(&(0..10).collect::<Vec<_>>(), &[], &[]);
    let reader = ObservedReader::new(chunk.path(), Some(2));
    let response = run(&catalog(), &reader, &query(0, 9), 1000, true);
    assert_eq!(block_numbers(&parse_response(&response)), vec![0, 9]);
    assert_eq!(
        reader
            .reads
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.table == "logs")
            .count(),
        5
    );
    let mut all: serde_json::Value = serde_json::from_str(&query(0, 9)).unwrap();
    all["includeAllBlocks"] = true.into();
    for budget in [0, 64, 1000] {
        assert_same_response(
            &run(&catalog(), &reader, &all.to_string(), budget, false),
            &run(&catalog(), &reader, &all.to_string(), budget, true),
            "header-only ranges with includeAllBlocks",
        );
    }
}

/// Covers CT-5 · INV-B6
#[test]
fn cumulative_weight_survives_range_boundaries() {
    let blocks: Vec<_> = (0..10).collect();
    let rows: Vec<_> = blocks.iter().map(|&b| (b, 100)).collect();
    let chunk = weighted_chunk(&blocks, &rows, &rows);
    let meta = catalog();
    for span in [1, 2, 3, 17] {
        let reader = ObservedReader::new(chunk.path(), Some(span));
        for budget in [0, 1, 400, 1000, 2000, u64::MAX] {
            assert_same_response(
                &run(&meta, &reader, &query(0, 9), budget, false),
                &run(&meta, &reader, &query(0, 9), budget, true),
                "cumulative range budget",
            );
        }
    }
}

/// Covers CT-5 · INV-B7
#[test]
fn overlapping_groups_and_relations_use_the_same_complete_range() {
    let chunk = evm_like::chunk();
    let shuffled = chunk_relaid(chunk.path(), &Layout::shuffled());
    repartition(shuffled.path(), "logs", 7);
    repartition(shuffled.path(), "transactions", 3);
    let meta = evm_like::catalog();
    let query = evm_like::query(100, 115);
    for span in [1, 3, 7] {
        let reader = ObservedReader::new(shuffled.path(), Some(span));
        for budget in [1, 256, 1024, 8192, u64::MAX] {
            assert_same_response(
                &run(&meta, &reader, &query, budget, false),
                &run(&meta, &reader, &query, budget, true),
                "overlapping groups and relation rows",
            );
        }
    }
}

/// Covers CT-5 · INV-B7
#[test]
fn a_range_ending_at_the_largest_block_terminates() {
    let blocks = [u64::MAX - 1, u64::MAX];
    let rows = [(blocks[0], 1), (blocks[1], 1)];
    let chunk = weighted_chunk(&blocks, &rows, &rows);
    let reader = ObservedReader::new(chunk.path(), Some(1));
    let response = run(
        &catalog(),
        &reader,
        &query(blocks[0], blocks[1]),
        u64::MAX,
        true,
    );
    assert_eq!(block_numbers(&parse_response(&response)), blocks);
}

#[test]
fn overlapping_groups_and_missing_statistics_do_not_cause_repeated_reads() {
    let blocks: Vec<_> = (0..60).collect();
    let chunk = weighted_chunk(&blocks, &[], &[]);
    for (table, index, data, size) in [
        ("logs", "log_index", "data", "data_size"),
        ("transactions", "transaction_index", "input", "input_size"),
    ] {
        let groups = (52..60)
            .map(|end| {
                let numbers: Vec<u64> = (0..=end).collect();
                let n = numbers.len();
                vec![
                    Arc::new(UInt64Array::from(numbers)) as ArrayRef,
                    Arc::new(UInt32Array::from(vec![end as u32; n])) as ArrayRef,
                    Arc::new(BinaryArray::from(vec![b"a".as_slice(); n])) as ArrayRef,
                    Arc::new(UInt64Array::from(vec![1; n])) as ArrayRef,
                ]
            })
            .collect();
        write_table_row_groups(
            chunk.path(),
            table,
            vec![
                Field::new("block_number", DataType::UInt64, false),
                Field::new(index, DataType::UInt32, false),
                Field::new(data, DataType::Binary, false),
                Field::new(size, DataType::UInt64, false),
            ],
            groups,
        );
    }
    let without_stats = chunk_relaid(chunk.path(), &Layout::without_statistics());
    let mut unbounded: serde_json::Value = serde_json::from_str(&query(0, 59)).unwrap();
    unbounded.as_object_mut().unwrap().remove("toBlock");
    for path in [chunk.path(), without_stats.path()] {
        let reader = ObservedReader::new(path, None);
        run(&catalog(), &reader, &unbounded.to_string(), u64::MAX, true);
        let reads = reader.reads.lock().unwrap();
        for table in ["logs", "transactions"] {
            let table_reads: Vec<_> = reads.iter().filter(|r| r.table == table).collect();
            assert_eq!(table_reads.len(), 1);
            assert_eq!(table_reads[0].to, None, "a full read added a block filter");
        }
    }
}
