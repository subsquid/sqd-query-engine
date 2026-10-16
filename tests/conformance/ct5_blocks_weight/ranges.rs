use crate::harness::chunk::{
    blocks_parquet, chunk_relaid, dictionary_unannounced, edit_footer, repartition,
    without_byte_counts, write_table, write_table_row_groups, write_table_with, Layout,
};
use crate::harness::evm_like;
use crate::harness::json::{assert_same_response, block_numbers, parse_response};
use crate::harness::synthetic::{catalog, catalog_with_heavy_headers, weighted_chunk, MB};
use arrow::array::{ArrayRef, BinaryArray, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, SchemaRef};
use parquet::basic::Encoding;
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::output::{execute_chunk_with, ExecOptions};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest, Scanned};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

struct Read {
    table: String,
    from: Option<u64>,
    to: Option<u64>,
    rows: usize,
    columns: Vec<String>,
    physical_rows: bool,
    /// The read reports its rows' positions, as a selection does.
    positions: bool,
    filters: bool,
}

struct ObservedReader {
    inner: ParquetChunkReader,
    span: Option<u64>,
    /// Whether the reader bounds a scan from the footer.
    estimates: bool,
    reads: Mutex<Vec<Read>>,
}

impl ObservedReader {
    fn new(path: &Path, span: Option<u64>) -> Self {
        Self {
            inner: ParquetChunkReader::open(path).unwrap(),
            span,
            estimates: true,
            reads: Mutex::new(Vec::new()),
        }
    }

    fn without_estimates(self) -> Self {
        Self {
            estimates: false,
            ..self
        }
    }
}

impl ChunkReader for ObservedReader {
    fn supports_row_positions(&self) -> bool {
        self.inner.supports_row_positions()
    }

    fn scan_rows(&self, table: &str, request: &ScanRequest) -> anyhow::Result<Scanned> {
        let scanned = self.inner.scan_rows(table, request)?;
        self.reads.lock().unwrap().push(Read {
            table: table.into(),
            from: request.from_block,
            to: request.to_block,
            rows: scanned.rows().num_rows(),
            physical_rows: request.row_indices.is_some(),
            positions: request.positions,
            filters: !request.predicates.is_empty()
                || request.key_filter.is_some()
                || request.hierarchical_filter.is_some(),
            columns: request
                .output_columns
                .iter()
                .map(|c| c.to_string())
                .collect(),
        });
        Ok(scanned)
    }
    fn next_block_range_end(&self, table: &str, column: &str, from: u64) -> Option<u64> {
        self.span
            .map(|span| from.saturating_add(span - 1))
            .or_else(|| self.inner.next_block_range_end(table, column, from))
    }
    fn estimate_scan_bytes(&self, table: &str, request: &ScanRequest) -> Option<u64> {
        if !self.estimates {
            return None;
        }
        self.inner.estimate_scan_bytes(table, request)
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

/// How many rows each read of `column` from `table` decoded.
fn payload_rows(reader: &ObservedReader, table: &str, column: &str) -> Vec<usize> {
    let reads = reader.reads.lock().unwrap();
    reads
        .iter()
        .filter(|read| read.table == table && read.columns.iter().any(|c| c == column))
        .map(|read| read.rows)
        .collect()
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

/// Logs sorted by their filter columns, as EVM chunks store them: one row group
/// holds two addresses, each in every block, so the group's block bounds span the
/// whole chunk and no block range can skip it. Each log carries `payload` bytes,
/// and each block `transactions` transactions.
fn unprunable_chunk(blocks: u64, payload: usize, transactions: u32) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let numbers: Vec<u64> = (0..blocks).collect();
    write_table(
        dir.path(),
        "blocks",
        vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("hash", DataType::Utf8, false),
        ],
        vec![
            Arc::new(UInt64Array::from(numbers.clone())) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                numbers.iter().map(|&n| evm_like::word(n as u8)),
            )) as ArrayRef,
        ],
    );

    let rows: Vec<(u8, u64)> = [0, 2]
        .into_iter()
        .flat_map(|address| numbers.iter().map(move |&block| (address, block)))
        .collect();
    let data = format!("0x{}", "ab".repeat(payload / 2));
    write_table(
        dir.path(),
        "logs",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("topic0", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, false),
        ],
        vec![
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|r| r.0 as u32),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0; rows.len()])) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| evm_like::address(r.0)),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|_| evm_like::word(0)),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|_| data.as_str()),
            )) as ArrayRef,
        ],
    );

    let txs: Vec<(u64, u32)> = numbers
        .iter()
        .flat_map(|&block| (0..transactions).map(move |index| (block, index)))
        .collect();
    write_table(
        dir.path(),
        "transactions",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("gas_used", DataType::UInt64, false),
        ],
        vec![
            Arc::new(UInt64Array::from_iter_values(txs.iter().map(|t| t.0))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(txs.iter().map(|t| t.1))) as ArrayRef,
            Arc::new(UInt64Array::from(vec![21_000; txs.len()])) as ArrayRef,
        ],
    );

    dir
}

/// Each selection read of `table`, as the blocks it spanned.
fn narrow_reads(
    reader: &ObservedReader,
    table: &str,
    payload: &str,
) -> Vec<(Option<u64>, Option<u64>)> {
    let reads = reader.reads.lock().unwrap();
    reads
        .iter()
        .filter(|read| read.table == table && !read.physical_rows)
        .filter(|read| !read.columns.iter().any(|c| c == payload))
        .map(|read| (read.from, read.to))
        .collect()
}

const UNPRUNABLE_BLOCKS: u64 = 2_500;

fn address_query(address: u8, transaction: bool) -> String {
    serde_json::json!({"type": "test", "fromBlock": 0,
        "logs": [{"address": [evm_like::address(address)], "transaction": transaction}],
        "fields": {"block": {"number": true}, "log": {"logIndex": true, "data": true},
                   "transaction": {"gasUsed": true}}})
    .to_string()
}

/// A block range cannot skip a row group whose blocks span the request, so a
/// selection split into ranges reads it again for each. Where the footer bounds
/// the selection's columns within the budget, one read covers the request.
///
/// Covers CT-5 · INV-B7
#[test]
fn a_selection_reads_groups_no_range_can_skip_once() {
    let chunk = unprunable_chunk(UNPRUNABLE_BLOCKS, 1024, 1);
    let meta = evm_like::catalog();

    let mut failures = Vec::new();
    for (address, what) in [
        (1, "an address no row has"),
        (0, "an address half the rows have"),
    ] {
        let query = address_query(address, false);
        let full = run(
            &meta,
            &ParquetChunkReader::open(chunk.path()).unwrap(),
            &query,
            MB,
            false,
        );
        let reader = ObservedReader::new(chunk.path(), None);
        let paged = run(&meta, &reader, &query, MB, true);
        assert_same_response(&full, &paged, what);

        let reads = narrow_reads(&reader, "logs", "data");
        if reads.len() != 1 {
            failures.push(format!(
                "{what}: logs were selected in {} reads: {reads:?}",
                reads.len()
            ));
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Where the footer cannot bound the selection within the budget, the selection
/// keeps its block ranges, so that what it holds stays bounded. A relation's
/// target counts toward it whole, since the selection reads it too.
///
/// Covers CT-5 · INV-B7
#[test]
fn a_selection_over_the_budget_keeps_its_ranges() {
    // Twenty transactions a block weigh more than the logs' own selection.
    let chunk = unprunable_chunk(UNPRUNABLE_BLOCKS, 1024, 20);
    let meta = evm_like::catalog();
    let cases = [
        (address_query(1, false), 16 * 1024, "the logs alone"),
        (
            address_query(1, true),
            256 * 1024,
            "the logs and their transactions",
        ),
    ];

    let mut failures = Vec::new();
    for (query, budget, what) in cases {
        let full = run(
            &meta,
            &ParquetChunkReader::open(chunk.path()).unwrap(),
            &query,
            budget,
            false,
        );
        let reader = ObservedReader::new(chunk.path(), None);
        let paged = run(&meta, &reader, &query, budget, true);
        assert_same_response(&full, &paged, what);

        let reads = narrow_reads(&reader, "logs", "data");
        if reads.len() < 2 {
            failures.push(format!(
                "{what}: one read held the whole selection: {reads:?}"
            ));
        }
        for (from, to) in reads {
            let last = to.unwrap_or(UNPRUNABLE_BLOCKS - 1);
            let span = last - from.unwrap_or(0) + 1;
            if span > 1024 {
                failures.push(format!("{what}: a read spanned {from:?}..{to:?}"));
            }
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Rows far sparser than headers, as on a quiet chain: one log and one
/// transaction in every `every`-th of `headers` blocks, each holding `payload`
/// bytes.
fn sparse_chunk(headers: u64, every: usize, payload: usize) -> TempDir {
    let rows: Vec<u64> = (0..headers).step_by(every).collect();
    let chunk = heavy_chunk(&rows, payload);
    blocks_parquet(chunk.path(), &(0..headers).collect::<Vec<_>>());
    chunk
}

/// An item that matches every row fills a page within a few blocks only where
/// its table has rows in most of them, and its first range finds out. Past it,
/// where the rest of the page spans more blocks than a selective range and the
/// footer bounds the selection within the budget, one read covers the rest of
/// the request however sparse the rows are: in ranges of a few blocks, a quiet
/// chain took a read of each table per range.
///
/// Covers CT-5 · INV-B7
#[test]
fn an_unfiltered_selection_that_fits_reads_the_rest_at_once() {
    let sparse = sparse_chunk(2_500, 8, 4 * 1024);
    let unprunable = unprunable_chunk(UNPRUNABLE_BLOCKS, 1024, 1);
    let fields = |transaction: &str| {
        serde_json::json!({"block": {"number": true},
            "log": {"logIndex": true, "data": true},
            "transaction": {"transactionIndex": true, transaction: true}})
    };
    let cases = [
        (
            sparse.path(),
            catalog(),
            serde_json::json!({"type": "test", "fromBlock": 0, "includeAllBlocks": true,
                "logs": [{}], "fields": fields("input")}),
            MB,
            vec![("logs", "data")],
            "an item beside every header",
        ),
        (
            sparse.path(),
            catalog(),
            serde_json::json!({"type": "test", "fromBlock": 0, "includeAllBlocks": true,
                "logs": [{"transaction": true}], "fields": fields("input")}),
            2 * MB,
            vec![("logs", "data"), ("transactions", "input")],
            "an item with a relation",
        ),
        (
            unprunable.path(),
            evm_like::catalog(),
            serde_json::json!({"type": "test", "fromBlock": 0,
                "logs": [{"address": [evm_like::address(0)]}], "transactions": [{}],
                "fields": fields("gasUsed")}),
            4 * MB,
            vec![("logs", "data"), ("transactions", "gas_used")],
            "an item beside a filtered one",
        ),
    ];

    let mut failures = Vec::new();
    for (path, meta, query, budget, tables, what) in cases {
        let query = query.to_string();
        let full = run(
            &meta,
            &ParquetChunkReader::open(path).unwrap(),
            &query,
            budget,
            false,
        );
        let reader = ObservedReader::new(path, None);
        let paged = run(&meta, &reader, &query, budget, true);
        assert_same_response(&full, &paged, what);

        for (table, payload) in tables {
            let reads = narrow_reads(&reader, table, payload);
            if reads.len() != 2 {
                failures.push(format!(
                    "{what}: {table} was selected in {} reads",
                    reads.len()
                ));
            }
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Where its table has rows in every block, an item that matches every row
/// fills the page within a few blocks, inside its first range or soon after
/// it. A read of the rest of the request would decode every other block's rows
/// for nothing, even where the footer bounds that read within the budget.
///
/// Covers CT-5 · INV-B7
#[test]
fn a_dense_unfiltered_selection_reads_little_past_its_page() {
    let meta = catalog();
    let query = serde_json::json!({"type": "test", "fromBlock": 0, "includeAllBlocks": true,
        "logs": [{}],
        "fields": {"block": {"number": true}, "log": {"logIndex": true, "data": true}}})
    .to_string();
    let budget = 64 * 1024;

    let mut failures = Vec::new();
    for (payload, what) in [
        (16 * 1024, "a page inside the first range"),
        (2 * 1024, "a page past the first range"),
    ] {
        let chunk = heavy_chunk(&(0..200).collect::<Vec<_>>(), payload);
        let full = run(
            &meta,
            &ParquetChunkReader::open(chunk.path()).unwrap(),
            &query,
            budget,
            false,
        );
        let reader = ObservedReader::new(chunk.path(), None);
        let paged = run(&meta, &reader, &query, budget, true);
        assert_same_response(&full, &paged, what);

        let page = block_numbers(&parse_response(&paged)).len();
        assert!(page < 100, "{what}: the budget must cut the page");
        let reads = reader.reads.lock().unwrap();
        let selected: usize = reads
            .iter()
            .filter(|read| read.table == "logs" && read.positions && !read.physical_rows)
            .map(|read| read.rows)
            .sum();
        if selected > page + 16 {
            failures.push(format!(
                "{what}: selection read {selected} rows for a page of {page}"
            ));
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Where the footer cannot bound the selection, an item that matches every row
/// is selected in ranges sized by what the blocks read so far weigh: few of them
/// where rows are sparse, and little past the page where they are dense. Ranges
/// of a fixed few blocks took a read per range on a quiet chain, and ranges of
/// the most blocks a selection may span decoded far past a dense page.
///
/// Covers CT-5 · INV-B7
#[test]
fn an_unfiltered_selection_over_the_budget_sizes_its_ranges_by_weight() {
    const HEADERS: u64 = 20_000;
    let headers: Vec<u64> = (0..HEADERS).collect();
    let logs: Vec<(u64, u64)> = headers.iter().step_by(4).map(|&b| (b, 100)).collect();
    let sparse = weighted_chunk(&headers, &logs, &[]);
    let dense = unprunable_chunk(UNPRUNABLE_BLOCKS, 16, 20);
    let budget = 64 * 1024;

    let logs_only = serde_json::json!({"type": "test", "fromBlock": 0, "logs": [{}],
        "fields": {"block": {"number": true}, "log": {"logIndex": true, "data": true}}});
    let mut logs_beside_headers = logs_only.clone();
    logs_beside_headers["includeAllBlocks"] = true.into();
    let transactions = serde_json::json!({"type": "test", "fromBlock": 0,
        "includeAllBlocks": true, "transactions": [{}],
        "fields": {"block": {"number": true},
                   "transaction": {"transactionIndex": true, "gasUsed": true}}});

    let mut failures = Vec::new();
    for (path, meta, query, what) in [
        (sparse.path(), catalog(), &logs_only, "sparse rows"),
        (
            sparse.path(),
            catalog(),
            &logs_beside_headers,
            "sparse rows beside every header",
        ),
        (
            dense.path(),
            evm_like::catalog(),
            &transactions,
            "dense rows",
        ),
    ] {
        let query = query.to_string();
        let full = run(
            &meta,
            &ParquetChunkReader::open(path).unwrap(),
            &query,
            budget,
            false,
        );
        let reader = ObservedReader::new(path, None);
        let paged = run(&meta, &reader, &query, budget, true);
        assert_same_response(&full, &paged, what);

        let (table, payload) = if query.contains("transactions") {
            ("transactions", "gas_used")
        } else {
            ("logs", "data")
        };
        let page_rows: usize = parse_response(&paged)
            .iter()
            .filter_map(|block| block[table].as_array())
            .map(Vec::len)
            .sum();
        let reads = reader.reads.lock().unwrap();
        // A narrow size scan can choose the first range; it reports no positions.
        let narrow: Vec<_> = reads
            .iter()
            .filter(|read| read.table == table && read.positions && !read.physical_rows)
            .filter(|read| !read.columns.iter().any(|c| c == payload))
            .collect();
        let selected_rows: usize = narrow.iter().map(|read| read.rows).sum();

        if narrow.len() < 2 {
            failures.push(format!("{what}: one read held the whole selection"));
        }
        if narrow.len() > 4 {
            failures.push(format!(
                "{what}: {} reads selected a page of {page_rows} rows",
                narrow.len()
            ));
        }
        if selected_rows > 2 * page_rows {
            failures.push(format!(
                "{what}: selection read {selected_rows} rows for a page of {page_rows}"
            ));
        }
        for read in &narrow {
            let span = read.to.unwrap_or(HEADERS - 1) - read.from.unwrap_or(0) + 1;
            if span > 1024 {
                failures.push(format!("{what}: a read spanned {span} blocks"));
            }
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

#[test]
fn relations_select_the_page_before_reading_payloads() {
    // Sparse numbers exercise range progress without relying on numeric distance.
    let blocks: Vec<_> = (0..600).map(|i| 10 + i * 1_000_000).collect();
    let logs: Vec<_> = blocks.iter().map(|&b| (b, 1)).collect();
    let transactions: Vec<_> = blocks.iter().map(|&b| (b, MB)).collect();
    let chunk = weighted_chunk(&blocks, &logs, &transactions);
    let without_stats = chunk_relaid(chunk.path(), &Layout::without_statistics());
    let meta = catalog();
    let mut query: serde_json::Value = serde_json::from_str(&query(10, blocks[599])).unwrap();
    query["logs"][0]["transaction"] = true.into();
    query["includeAllBlocks"] = true.into();
    query.as_object_mut().unwrap().remove("toBlock");

    for path in [chunk.path(), without_stats.path()] {
        for budget in [0, 2 * MB + 512, 300 * MB + 100_000] {
            let full = run(
                &meta,
                &ParquetChunkReader::open(path).unwrap(),
                &query.to_string(),
                budget,
                false,
            );
            // Where the footer bounds the selection, one read may cover the
            // request (ADR-15); where it cannot, a read holds about a page.
            let readers = [
                ObservedReader::new(path, None),
                ObservedReader::new(path, None).without_estimates(),
            ];
            for reader in readers {
                let selected = run(&meta, &reader, &query.to_string(), budget, true);
                assert_same_response(&full, &selected, "relations must keep the weighted prefix");
                let count = block_numbers(&parse_response(&selected)).len();
                assert!(count < blocks.len());
                let reads = reader.reads.lock().unwrap();
                for (table, payload) in [("logs", "data"), ("transactions", "input")] {
                    let wide: Vec<_> = reads
                        .iter()
                        .filter(|read| {
                            read.table == table && read.columns.iter().any(|c| c == payload)
                        })
                        .collect();
                    assert_eq!(
                        wide.len(),
                        1,
                        "{table}: duplicate payload read through a relation"
                    );
                    assert_eq!(
                        wide[0].rows, count,
                        "{table}: decoded rows outside the page"
                    );
                    if reader.estimates {
                        continue;
                    }
                    for narrow in reads.iter().filter(|read| {
                        read.table == table && !read.columns.iter().any(|c| c == payload)
                    }) {
                        assert!(
                            narrow.rows <= count + 16,
                            "{table}: unbounded selection scan"
                        );
                    }
                }
            }
        }
    }
}

/// A chunk whose rows weigh what they carry: one log and one transaction in
/// every block, each holding `payload(block)`, in one row group per table.
fn heavy_chunk_with(
    blocks: &[u64],
    payload: impl Fn(u64) -> Vec<u8>,
    props: impl Fn() -> WriterProperties,
) -> TempDir {
    let chunk = weighted_chunk(blocks, &[], &[]);
    let payloads: Vec<Vec<u8>> = blocks.iter().map(|&block| payload(block)).collect();
    let sizes: Vec<u64> = payloads.iter().map(|bytes| bytes.len() as u64).collect();
    let n = blocks.len();

    for (table, index, data, size) in [
        ("logs", "log_index", "data", "data_size"),
        ("transactions", "transaction_index", "input", "input_size"),
    ] {
        write_table_with(
            chunk.path(),
            table,
            vec![
                Field::new("block_number", DataType::UInt64, false),
                Field::new(index, DataType::UInt32, false),
                Field::new(data, DataType::Binary, false),
                Field::new(size, DataType::UInt64, false),
            ],
            vec![
                Arc::new(UInt64Array::from(blocks.to_vec())) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0; n])) as ArrayRef,
                Arc::new(BinaryArray::from_iter_values(&payloads)) as ArrayRef,
                Arc::new(UInt64Array::from(sizes.clone())) as ArrayRef,
            ],
            props(),
        );
    }

    chunk
}

fn heavy_chunk(blocks: &[u64], payload: usize) -> TempDir {
    heavy_chunk_with(blocks, |_| vec![b'x'; payload], WriterProperties::default)
}

/// The same rows under each way found for a footer to understate what its
/// payloads decode to (see `estimate.rs`). The payloads repeat, so a writer
/// keeps each table's once, in a dictionary, unless told otherwise.
fn disguised_chunks(blocks: &[u64]) -> Vec<(&'static str, TempDir)> {
    const LONG: usize = 64 * 1024;
    let repeated = |_| vec![b'x'; LONG];
    let no_statistics = || {
        WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .build()
    };
    let edited = |chunk: TempDir, edit: fn(ColumnChunkMetaData) -> ColumnChunkMetaData| {
        for table in ["logs", "transactions"] {
            edit_footer(chunk.path(), table, edit);
        }
        chunk
    };

    let bounded = heavy_chunk_with(
        blocks,
        |block| match block {
            0 => b"a".to_vec(),
            99 => b"z".to_vec(),
            _ => vec![b'm'; LONG],
        },
        WriterProperties::default,
    );
    let truncated = heavy_chunk_with(blocks, repeated, || {
        WriterProperties::builder()
            .set_statistics_truncate_length(Some(2))
            .build()
    });
    let prefixed = heavy_chunk_with(blocks, repeated, || {
        WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::None)
            .set_dictionary_enabled(false)
            .set_column_encoding(ColumnPath::from("data"), Encoding::DELTA_BYTE_ARRAY)
            .set_column_encoding(ColumnPath::from("input"), Encoding::DELTA_BYTE_ARRAY)
            .build()
    });
    let unannounced = heavy_chunk_with(blocks, repeated, no_statistics);
    let mut bunched = blocks.to_vec();
    bunched.push(1_000_000);

    vec![
        (
            "sizes recorded",
            heavy_chunk_with(blocks, repeated, WriterProperties::default),
        ),
        (
            "no statistics",
            heavy_chunk_with(blocks, repeated, no_statistics),
        ),
        (
            "bounds shorter than the values",
            edited(bounded, without_byte_counts),
        ),
        (
            "truncated statistics",
            edited(truncated, without_byte_counts),
        ),
        ("prefix compression", prefixed),
        (
            "an unannounced dictionary",
            edited(unannounced, dictionary_unannounced),
        ),
        (
            "rows bunched at one end of the block span",
            heavy_chunk_with(&bunched, repeated, WriterProperties::default),
        ),
    ]
}

/// Without relations, payloads are read for the page only once one pass could
/// decode more than a page: two tables, or one beside header-only blocks. Read
/// for every matching row first, they held the whole range in memory before the
/// page cut. A footer that cannot bound what its payloads decode to has to count
/// as too much rather than as what it stores.
#[test]
fn a_query_without_relations_reads_payloads_only_for_the_page() {
    let blocks: Vec<u64> = (0..100).collect();
    let meta = catalog();

    let mut with_headers: serde_json::Value = serde_json::from_str(&query(0, 99)).unwrap();
    with_headers.as_object_mut().unwrap().remove("transactions");
    with_headers["includeAllBlocks"] = true.into();
    let cases = [
        (
            query(0, 99),
            vec![("logs", "data"), ("transactions", "input")],
        ),
        (with_headers.to_string(), vec![("logs", "data")]),
    ];

    let mut failures = Vec::new();
    for (chunk_name, chunk) in disguised_chunks(&blocks) {
        let path = chunk.path();
        for (query, payloads) in &cases {
            let full = run(
                &meta,
                &ParquetChunkReader::open(path).unwrap(),
                query,
                MB,
                false,
            );
            let reader = ObservedReader::new(path, None);
            let paged = run(&meta, &reader, query, MB, true);
            assert_same_response(&full, &paged, "deferred payloads keep the weighted prefix");

            let count = block_numbers(&parse_response(&paged)).len();
            assert!(
                count < blocks.len(),
                "{chunk_name}: the budget must cut the page: {query}"
            );

            for &(table, payload) in payloads {
                let rows = payload_rows(&reader, table, payload);
                if rows != [count] {
                    failures.push(format!(
                        "{chunk_name}: {table} payloads decoded for {rows:?} rows, the page has {count}: {query}"
                    ));
                }
            }
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Header rows are read for the whole range before a page is cut, so a heavy
/// header defers like an item's payload: beside two tables, and beside the one
/// unfiltered table whose first range a narrow scan chooses.
#[test]
fn header_payloads_are_read_only_for_the_page() {
    let blocks: Vec<u64> = (0..100).collect();
    let items: Vec<(u64, u64)> = blocks.iter().map(|&block| (block, 1)).collect();
    let chunk = weighted_chunk(&blocks, &items, &items);
    let extra = vec![b'x'; 64 * 1024];
    write_table(
        chunk.path(),
        "blocks",
        vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("extra", DataType::Binary, false),
            Field::new("extra_size", DataType::UInt64, false),
        ],
        vec![
            Arc::new(UInt64Array::from(blocks.clone())) as ArrayRef,
            Arc::new(BinaryArray::from(vec![extra.as_slice(); blocks.len()])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![extra.len() as u64; blocks.len()])) as ArrayRef,
        ],
    );
    let without_stats = chunk_relaid(chunk.path(), &Layout::without_statistics());
    let meta = catalog_with_heavy_headers();

    let fields = serde_json::json!({"block": {"number": true, "extra": true},
        "log": {"logIndex": true}, "transaction": {"transactionIndex": true}});
    let two_tables = serde_json::json!({"type": "test", "fromBlock": 0, "toBlock": 99,
        "logs": [{}], "transactions": [{}], "fields": fields});
    let one_table = serde_json::json!({"type": "test", "fromBlock": 0, "toBlock": 99,
        "logs": [{}], "fields": fields});

    for path in [chunk.path(), without_stats.path()] {
        for query in [two_tables.to_string(), one_table.to_string()] {
            let full = run(
                &meta,
                &ParquetChunkReader::open(path).unwrap(),
                &query,
                MB,
                false,
            );
            let reader = ObservedReader::new(path, None);
            let paged = run(&meta, &reader, &query, MB, true);
            assert_same_response(&full, &paged, "deferred headers keep the weighted prefix");

            let count = block_numbers(&parse_response(&paged)).len();
            assert!(
                count < blocks.len(),
                "the budget must cut the page: {query}"
            );

            assert_eq!(
                payload_rows(&reader, "blocks", "extra"),
                [count],
                "headers decoded outside the page in {}: {query}",
                path.display()
            );
        }
    }
}

/// A page always holds its first block whole, so a one-block request decodes
/// the same rows in one pass as in two, and reads in one even where the footer
/// cannot size its payloads (ADR-15). A second block is what deferral can save.
#[test]
fn a_one_block_request_reads_in_one_pass() {
    let blocks: Vec<u64> = (0..100).collect();
    let chunk = heavy_chunk_with(
        &blocks,
        |_| vec![b'x'; 64 * 1024],
        || {
            WriterProperties::builder()
                .set_statistics_enabled(EnabledStatistics::None)
                .build()
        },
    );
    let meta = catalog();
    let budget = 100 * 1024;

    for (query, reads_per_table) in [(query(50, 50), 1), (query(50, 51), 2)] {
        let full = run(
            &meta,
            &ParquetChunkReader::open(chunk.path()).unwrap(),
            &query,
            budget,
            false,
        );
        let reader = ObservedReader::new(chunk.path(), None);
        let paged = run(&meta, &reader, &query, budget, true);
        assert_same_response(&full, &paged, "a one-block page is the same either way");
        assert_eq!(block_numbers(&parse_response(&paged)), vec![50]);

        for (table, payload) in [("logs", "data"), ("transactions", "input")] {
            let count = reader
                .reads
                .lock()
                .unwrap()
                .iter()
                .filter(|read| read.table == table)
                .count();
            assert_eq!(count, reads_per_table, "{table} reads: {query}");

            assert_eq!(
                payload_rows(&reader, table, payload),
                [1],
                "{table}: payloads decoded outside the page: {query}"
            );
        }
    }
}

/// A query's tables are read in the same pass, so it is their sum that has to
/// fit: two tables that each fit a page alone can overflow it together.
#[test]
fn tables_that_fit_a_page_alone_can_overflow_it_together() {
    let blocks: Vec<u64> = (0..100).collect();
    let chunk = heavy_chunk(&blocks, 6 * 1024);
    let meta = catalog();
    let query = query(0, 99);

    let full = run(
        &meta,
        &ParquetChunkReader::open(chunk.path()).unwrap(),
        &query,
        MB,
        false,
    );
    let reader = ObservedReader::new(chunk.path(), None);
    let paged = run(&meta, &reader, &query, MB, true);
    assert_same_response(&full, &paged, "deferred payloads keep the weighted prefix");

    let count = block_numbers(&parse_response(&paged)).len();
    assert!(count < blocks.len(), "the budget must cut the page");

    for (table, payload) in [("logs", "data"), ("transactions", "input")] {
        assert_eq!(
            payload_rows(&reader, table, payload),
            [count],
            "{table}: payloads decoded outside the page"
        );
    }
}

/// A query whose rows fit a page reads each table once: a second pass by
/// position costs more than the payloads it could skip. A request counts every
/// row of each group it touches, because a group's block bounds say nothing about
/// how its rows spread between them; a short request fits when its groups do.
#[test]
fn a_query_that_fits_a_page_reads_each_table_once() {
    let blocks: Vec<u64> = (0..100).collect();
    let chunk = heavy_chunk(&blocks, 64 * 1024);
    let split = heavy_chunk(&blocks, 64 * 1024);
    for table in ["logs", "transactions"] {
        repartition(split.path(), table, 5);
    }
    let meta = catalog();

    for (path, query, budget) in [
        (chunk.path(), query(0, 99), 64 * MB),
        (split.path(), query(50, 51), MB),
    ] {
        let reader = ObservedReader::new(path, None);
        let response = run(&meta, &reader, &query, budget, true);
        assert!(!response.is_empty());

        let reads = reader.reads.lock().unwrap();
        for table in ["logs", "transactions"] {
            let count = reads.iter().filter(|read| read.table == table).count();
            assert_eq!(count, 1, "{table} was read {count} times: {query}");
        }
    }
}

#[test]
fn materialization_does_not_fetch_other_rows_of_selected_blocks() {
    let chunk = evm_like::chunk();
    let meta = evm_like::catalog();
    let query = serde_json::json!({
        "type": "test", "fromBlock": 100, "toBlock": 115,
        "transactions": [{"transactionIndex": [0], "transactionLogs": true}],
        "fields": {"log": {"logIndex": true, "data": true},
                   "transaction": {"transactionIndex": true, "gasUsed": true}}
    })
    .to_string();
    let reader = ObservedReader::new(chunk.path(), None);
    let selected = run(&meta, &reader, &query, 512, true);
    let full = run(
        &meta,
        &ParquetChunkReader::open(chunk.path()).unwrap(),
        &query,
        512,
        false,
    );
    assert_same_response(&full, &selected, "materialize exact item keys");
    let expected: usize = parse_response(&selected)
        .iter()
        .filter_map(|block| block["logs"].as_array())
        .map(Vec::len)
        .sum();
    assert!(expected > 0);
    let reads = reader.reads.lock().unwrap();
    let wide: Vec<_> = reads
        .iter()
        .filter(|read| read.table == "logs" && read.columns.iter().any(|column| column == "data"))
        .collect();
    assert_eq!(wide.len(), 1);
    assert_eq!(wide[0].rows, expected);
    assert!(wide[0].physical_rows);
    assert!(!wide[0].filters, "output pass repeated selection filters");
    let narrow: Vec<_> = reads
        .iter()
        .filter(|read| read.table == "logs" && !read.columns.iter().any(|c| c == "data"))
        .collect();
    assert!(!narrow.is_empty());
    assert!(
        narrow
            .iter()
            .all(|read| !read.columns.iter().any(|c| c == "log_index")),
        "a single source needs no item key for weight deduplication"
    );
}
