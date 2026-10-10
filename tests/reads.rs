//! Every scan each bench query asks the chunk reader for, as one snapshot per
//! query, for the default page, a small page and Arrow output.
//!
//! A read line holds every field of the scan request, so a change to what a
//! query reads or filters on changes the snapshot even when the response does
//! not. It records requests, not the bytes the reader decodes: a change inside
//! the reader shows only in rows and in the instruction counts of
//! `benches/instructions`. The response itself is the business of the fixture
//! and conformance suites; only its block range is kept here, because it says
//! where the page stopped reading.
//!
//! Reads run in parallel, so a case lists its reads sorted; projections are
//! sorted too, since some are collected through hash sets. Predicates keep
//! their order, which decides the scanner's stages. Each case runs twice and
//! fails if the two runs differ.
//!
//!   SQD_REQUIRE_CHUNKS=1 cargo test --test reads -- --ignored
//!   cargo insta review

#[path = "../benches/queries.rs"]
#[rustfmt::skip]
mod queries;

use queries::*;
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::output::{execute_chunk_arrow, execute_chunk_with, ExecOptions};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::predicate::{col_eq, ColumnPredicate, RowPredicate, ScalarValue};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest, Scanned};
use std::path::Path;
use std::sync::Mutex;
use xxhash_rust::xxh3::xxh3_64;

/// Small enough that full scans stop after a few blocks and one EVM block
/// alone can exceed it.
const SMALL_PAGE: u64 = 256 << 10;

/// Longer predicate texts become a digest, which keeps a line readable and
/// still changes with any operand.
const SHORT: usize = 80;

fn metadata(name: &str) -> DatasetDescription {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("metadata")
        .join(name);
    load_dataset_description(&path).unwrap()
}

/// The chunk under `data/`, or `None` when it is not checked out. The
/// snapshots belong to these exact chunks, so no variable overrides the path.
fn chunk(relative: &str) -> Option<ParquetChunkReader> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join(relative);

    if path.is_dir() {
        return Some(ParquetChunkReader::open(&path).unwrap());
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_CHUNKS").is_none(),
        "SQD_REQUIRE_CHUNKS is set but data/{relative} is not checked out, so this \
         test would report green having read nothing"
    );

    None
}

struct Recorder<'a> {
    inner: &'a ParquetChunkReader,
    reads: Mutex<Vec<String>>,
}

fn sorted(columns: &[&str]) -> String {
    let mut columns = columns.to_vec();
    columns.sort_unstable();
    columns.join(",")
}

fn bound(block: Option<u64>) -> String {
    block.map_or_else(|| "-".to_owned(), |b| b.to_string())
}

/// The column a predicate reads, its operation and its operands.
fn describe_column(column: &ColumnPredicate) -> String {
    let text = column.predicate.describe();
    if text.len() <= SHORT {
        return format!("{} {text}", column.column);
    }

    let operation = text.split('(').next().unwrap_or(&text);
    format!(
        "{} {operation}(#{:016x})",
        column.column,
        xxh3_64(text.as_bytes())
    )
}

fn describe_columns(columns: &[ColumnPredicate]) -> String {
    let parts: Vec<String> = columns.iter().map(describe_column).collect();
    parts.join(" ")
}

/// An item's columns, then the groups of which one must also match.
fn describe_item(item: &RowPredicate) -> String {
    let mut text = describe_columns(&item.columns);
    if !item.alternatives.is_empty() {
        let groups: Vec<String> = item
            .alternatives
            .iter()
            .map(|g| describe_columns(g))
            .collect();
        text += &format!(" any({})", groups.join(" | "));
    }
    text
}

fn describe_items(items: &[&RowPredicate]) -> String {
    let items: Vec<String> = items.iter().map(|item| describe_item(item)).collect();
    items.join("; ")
}

fn describe(table: &str, request: &ScanRequest, scanned: &Scanned) -> String {
    let mut line = format!(
        "read {table} blocks={}..{} cols=[{}]",
        bound(request.from_block),
        bound(request.to_block),
        sorted(&request.output_columns),
    );

    if let Some(column) = request.block_number_column {
        line += &format!(" block_column={column}");
    }
    if !request.required_columns.is_empty() {
        line += &format!(" required=[{}]", sorted(&request.required_columns));
    }
    if !request.predicates.is_empty() {
        line += &format!(" items=[{}]", describe_items(&request.predicates));
    }
    if !request.item_tags.is_empty() {
        let tags: Vec<String> = request.item_tags.iter().map(|t| format!("{t:?}")).collect();
        line += &format!(" tags={}", tags.join(""));
    }
    if let Some(keys) = request.key_filter {
        line += &format!(" keys={}", keys.describe());
    }
    if let Some(hierarchy) = request.hierarchical_filter {
        line += &format!(" hierarchy={}", hierarchy.describe());
    }
    if request.positions {
        line += " positions";
    }
    if let Some(rows) = request.row_indices {
        let bytes: Vec<u8> = rows.iter().flat_map(|row| row.to_le_bytes()).collect();
        line += &format!(" at={}:{:016x}", rows.len(), xxh3_64(&bytes));
    }
    if request.column_cache.is_some() {
        line += " cached";
    }
    if let Some(window) = &request.window {
        line += &format!(" window={}..{}", bound(window.from), bound(window.to));
    }
    if request.batch_size != usize::MAX {
        line += &format!(" batch={}", request.batch_size);
    }

    line + &format!(" -> rows={}", scanned.rows().num_rows())
}

impl ChunkReader for Recorder<'_> {
    fn scan_rows(&self, table: &str, request: &ScanRequest) -> anyhow::Result<Scanned> {
        let scanned = self.inner.scan_rows(table, request)?;
        let line = describe(table, request, &scanned);
        self.reads.lock().unwrap().push(line);
        Ok(scanned)
    }

    fn supports_row_positions(&self) -> bool {
        self.inner.supports_row_positions()
    }

    fn next_block_range_end(&self, table: &str, column: &str, from: u64) -> Option<u64> {
        self.inner.next_block_range_end(table, column, from)
    }

    fn estimate_scan_bytes(&self, table: &str, request: &ScanRequest) -> Option<u64> {
        self.inner.estimate_scan_bytes(table, request)
    }

    fn has_table(&self, table: &str) -> bool {
        self.inner.has_table(table)
    }

    fn table_schema(&self, table: &str) -> Option<arrow::datatypes::SchemaRef> {
        self.inner.table_schema(table)
    }
}

#[derive(Clone, Copy)]
enum Variant {
    Json,
    SmallPage,
    Arrow,
}

impl Variant {
    fn name(self) -> &'static str {
        match self {
            Variant::Json => "json",
            Variant::SmallPage => "json, page 256 KiB",
            Variant::Arrow => "arrow",
        }
    }
}

/// Where the response starts and stops, then the sorted reads of one run.
fn report(
    query: &[u8],
    meta: &DatasetDescription,
    chunk: &ParquetChunkReader,
    variant: Variant,
) -> String {
    let recorder = Recorder {
        inner: chunk,
        reads: Mutex::new(Vec::new()),
    };
    let plan = compile(&parse_query(query, meta).unwrap(), meta).unwrap();

    let response = match variant {
        Variant::Json | Variant::SmallPage => {
            let weight_budget = match variant {
                Variant::SmallPage => SMALL_PAGE,
                _ => ExecOptions::default().weight_budget,
            };
            let options = ExecOptions {
                weight_budget,
                ..ExecOptions::default()
            };
            execute_chunk_with(&plan, meta, &recorder, options)
                .unwrap()
                .map(|output| {
                    let blocks = format!(
                        "{}..{} n={} read_through={}",
                        output.first_block(),
                        output.last_block(),
                        output.num_blocks(),
                        bound(output.read_through()),
                    );
                    std::hint::black_box(output.into_json_lines());
                    blocks
                })
        }
        Variant::Arrow => execute_chunk_arrow(&plan, meta, &recorder, false, false)
            .unwrap()
            .map(|output| {
                format!(
                    "{}..{} n={}",
                    output.first_block(),
                    output.last_block(),
                    output.num_blocks(),
                )
            }),
    };

    let mut reads = recorder.reads.into_inner().unwrap();
    reads.sort_unstable();

    let mut text = format!("response {}", response.unwrap_or_else(|| "none".to_owned()));
    for read in reads {
        text.push('\n');
        text += &read;
    }
    text
}

/// One snapshot per query: each variant's response range and reads.
fn check(
    prefix: &str,
    queries: &[(&str, &[u8])],
    meta: &DatasetDescription,
    chunk: &ParquetChunkReader,
) {
    for &(name, query) in queries {
        let mut snapshot = String::new();

        for variant in [Variant::Json, Variant::SmallPage, Variant::Arrow] {
            let first = report(query, meta, chunk, variant);
            let second = report(query, meta, chunk, variant);
            assert_eq!(
                first,
                second,
                "{name} {}: a second run read or returned something else",
                variant.name()
            );

            snapshot += &format!("## {}\n{first}\n\n", variant.name());
        }

        let id: String = format!("{prefix}{name}")
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        insta::assert_snapshot!(id, snapshot.trim_end());
    }
}

/// The scanner gives a lone item's first column a stage of its own, and the
/// union of several items picks between equally demanded columns by their
/// order, so a reordering at any level changes the line.
#[test]
fn a_line_keeps_the_order_of_the_predicates() {
    let a = col_eq("a", ScalarValue::UInt64(1));
    let b = col_eq("b", ScalarValue::UInt64(2));

    let columns = |first: &ColumnPredicate, second: &ColumnPredicate| {
        describe_item(&RowPredicate::new(vec![first.clone(), second.clone()]))
    };
    let group = |first: &ColumnPredicate, second: &ColumnPredicate| {
        describe_item(&RowPredicate::with_alternatives(
            Vec::new(),
            vec![vec![first.clone(), second.clone()]],
        ))
    };
    let groups = |first: &ColumnPredicate, second: &ColumnPredicate| {
        describe_item(&RowPredicate::with_alternatives(
            Vec::new(),
            vec![vec![first.clone()], vec![second.clone()]],
        ))
    };
    let items = |first: &ColumnPredicate, second: &ColumnPredicate| {
        let first = RowPredicate::new(vec![first.clone()]);
        let second = RowPredicate::new(vec![second.clone()]);
        describe_items(&[&first, &second])
    };

    let mut same = Vec::new();
    for (level, line) in [
        ("an item's columns", &columns as &dyn Fn(_, _) -> String),
        ("a group's columns", &group),
        ("an item's groups", &groups),
        ("a request's items", &items),
    ] {
        if line(&a, &b) == line(&b, &a) {
            same.push(level);
        }
    }

    assert!(same.is_empty(), "a reordering of {same:?} reads the same");
}

#[test]
#[ignore = "requires external chunk data"]
fn evm_small_chunk() {
    let Some(chunk) = chunk("evm/chunk") else {
        return;
    };
    let meta = metadata("evm.yaml");

    check("small/", EVM_QUERIES, &meta, &chunk);
    check("small/", EVM_FULLSCAN_QUERIES, &meta, &chunk);
    check("", EVM_RPC_QUERIES, &meta, &chunk);
}

#[test]
#[ignore = "requires external chunk data"]
fn evm_big_chunk() {
    let Some(chunk) = chunk("evm/big") else {
        return;
    };
    let meta = metadata("evm.yaml");

    check("big/", EVM_QUERIES, &meta, &chunk);
    check("big/", EVM_FULLSCAN_QUERIES, &meta, &chunk);
}

#[test]
#[ignore = "requires external chunk data"]
fn solana_chunk() {
    let Some(chunk) = chunk("solana/chunk") else {
        return;
    };

    check("", SOL_QUERIES, &metadata("solana.yaml"), &chunk);
}
