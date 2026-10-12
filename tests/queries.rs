//! Queries on real chunks: the tests under `tests/queries/` and the bench
//! workload under `benches/queries/`, in one format.
//!
//! Each directory's `chunks.yaml` names chunks from `chunk-registry.yaml` at
//! the repository root, and every `*.json` beside it is a query that runs on
//! each of them. Git keeps the query and, for each chunk, two insta
//! snapshots: the answer, and what the query read. The answer snapshot holds the
//! counts, then the answer whole, or a digest when it is over 64 KB. A snapshot
//! takes the query's name, and the chunk's too when the directory names several.
//! `make fetch` downloads chunks into a local cache.
//!
//! With `--features legacy-query` both engines answer, and they must agree before
//! the snapshot is checked. Without it the snapshot alone guards the answer, which
//! is what keeps a query useful once the reference engine is gone.
//!
//! The reads snapshot holds every scan the query asks the chunk reader for, for
//! the default page, a small page and Arrow output. A read line holds every field
//! of the scan request, so a change to what a query reads or filters on changes
//! it even when the answer does not. It records requests, not the bytes the
//! reader decodes: a change inside the reader shows only in rows and in the
//! instruction counts of `benches/instructions`. Reads run in parallel, so they
//! are listed sorted; projections are sorted too, since some are collected
//! through hash sets. Predicates keep their order, which decides the scanner's
//! stages. Each output runs twice and fails if the two runs differ.
//!
//! Every query is ignored by the portable suite. An explicitly selected query fails
//! if its chunk is missing from the cache or unchecked against its hashes.
//! `--list-chunks` prints the chunk paths for `make fetch`, and `--verify-chunks`
//! hashes what it fetched, using the same YAML parser. See `tests/queries/README.md`.

use libtest_mimic::{Arguments, Failed, Trial};
use serde_json::Value;
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::output::{execute_chunk_arrow, execute_chunk_with, ExecOptions};
use sqd_query_engine::query::{compile, parse_query, Plan};
use sqd_query_engine::scan::predicate::{col_eq, ColumnPredicate, RowPredicate, ScalarValue};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest, Scanned};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use xxhash_rust::xxh3::xxh3_64;

// CT-7 alone puts a query to this engine through `run_new`.
#[allow(dead_code)]
#[path = "conformance/harness/engines.rs"]
mod engines;
#[path = "conformance/harness/store.rs"]
mod store;

use store::QueryDir;

type Catalogs = HashMap<String, DatasetDescription>;

type Answer = Result<Vec<Value>, String>;

/// One query on one chunk.
struct Case {
    // Only the comparison with the reference reads it.
    #[cfg_attr(not(feature = "legacy-query"), allow(dead_code))]
    setup: Arc<QueryDir>,
    dir: PathBuf,
    /// The query's file stem, which `legacy_differs` names.
    #[cfg_attr(not(feature = "legacy-query"), allow(dead_code))]
    stem: String,
    /// The name of both snapshots.
    snapshot: String,
    chunk: String,
    query: PathBuf,
}

fn main() {
    if std::env::args().skip(1).eq(["--list-chunks"]) {
        for chunk in store::registry().values() {
            if let Some(path) = chunk.reference.strip_prefix("s3://") {
                println!("{path}");
            }
        }
        return;
    }
    if std::env::args().skip(1).eq(["--verify-chunks"]) {
        let all_match = verify_chunks();
        std::process::exit(if all_match { 0 } else { 1 });
    }

    let args = Arguments::from_args();
    let catalogs = Arc::new(load_catalogs());
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));

    let mut trials = vec![Trial::test(
        "a_read_line_keeps_the_order_of_the_predicates",
        a_read_line_keeps_the_order_of_the_predicates,
    )];

    for dir in store::query_dirs() {
        let setup = Arc::new(load_query_dir(&dir));
        let several = setup.chunks.len() > 1;

        for chunk in &setup.chunks {
            for query in store::query_files(&dir) {
                let stem = query.file_stem().unwrap().to_string_lossy().into_owned();
                let snapshot = if several {
                    format!("{stem}.{chunk}")
                } else {
                    stem.clone()
                };
                let name = dir.strip_prefix(root).unwrap().join(&snapshot);

                let case = Case {
                    setup: setup.clone(),
                    dir: dir.clone(),
                    stem,
                    snapshot,
                    chunk: chunk.clone(),
                    query,
                };
                let catalogs = catalogs.clone();
                let trial = Trial::test(name.to_string_lossy(), move || run(&case, &catalogs));
                trials.push(trial.with_ignored_flag(true));
            }
        }
    }

    libtest_mimic::run(&args, trials).exit();
}

/// Every bundled catalog by the query `type` it serves.
fn load_catalogs() -> Catalogs {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("metadata");
    let mut catalogs = Catalogs::new();

    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "yaml") {
            let catalog = load_dataset_description(&path).unwrap();
            catalogs.insert(catalog.name.clone(), catalog);
        }
    }

    catalogs
}

/// A malformed `chunks.yaml` fails the whole run, the portable suite included.
fn load_query_dir(dir: &Path) -> QueryDir {
    let file = dir.join("chunks.yaml");
    let setup = store::load_query_dir(&file);

    for stem in setup.legacy_differs.keys() {
        assert!(
            dir.join(format!("{stem}.json")).is_file(),
            "{}: legacy_differs names {stem}, which has no query file",
            file.display()
        );
    }

    setup
}

fn run(case: &Case, catalogs: &Catalogs) -> Result<(), Failed> {
    let chunk = cached_chunk(&case.chunk)?;

    let query = std::fs::read(&case.query)?;
    let kind = query_type(&query)?;
    let catalog = catalogs
        .get(&kind)
        .ok_or_else(|| format!("no catalog serves query type {kind}"))?;
    let reader = ParquetChunkReader::open(&chunk).map_err(|e| format!("{e:#}"))?;

    let (new, reads) = answer(&query, catalog, &reader)?;

    #[cfg(feature = "legacy-query")]
    check_reference(case, &query, &chunk, &new)?;

    let summary = summarize(&new);
    insta::with_settings!({
        snapshot_path => &case.dir,
        prepend_module_to_snapshot => false,
        omit_expression => true,
    }, {
        insta::assert_snapshot!(case.snapshot.as_str(), summary);
        if let Some(reads) = reads {
            insta::assert_snapshot!(format!("{}.reads", case.snapshot), reads);
        }
    });

    Ok(())
}

/// The chunk's directory; a selected query requires its input, and the right one.
fn cached_chunk(name: &str) -> Result<PathBuf, Failed> {
    let chunk = store::named(name);
    let dir = store::chunk_path(&chunk.reference);

    if !dir.is_dir() {
        let remedy = if chunk.is_stored() {
            "run `make fetch`"
        } else {
            "the fixture tree is not checked out"
        };
        let message = format!(
            "{name} ({}) is not at {}; {remedy}",
            chunk.reference,
            dir.display()
        );
        return Err(message.into());
    }

    store::check_verified(chunk, &dir)?;
    Ok(dir)
}

fn query_type(query: &[u8]) -> Result<String, Failed> {
    let parsed: Value = serde_json::from_slice(query)?;
    let kind = parsed["type"].as_str().ok_or("the query has no \"type\"")?;
    Ok(kind.to_owned())
}

/// This engine's answer, and its reads snapshot unless it rejects the query
/// before reading.
fn answer(
    query: &[u8],
    catalog: &DatasetDescription,
    reader: &ParquetChunkReader,
) -> Result<(Answer, Option<String>), Failed> {
    let plan = match parse_query(query, catalog).and_then(|parsed| compile(&parsed, catalog)) {
        Ok(plan) => plan,
        Err(e) => return Ok((Err(format!("{e:#}")), None)),
    };

    let mut answer = None;
    let mut snapshot = String::new();

    for variant in [Variant::Json, Variant::SmallPage, Variant::Arrow] {
        let (first, body) = recorded_run(&plan, catalog, reader, variant);
        let (second, _) = recorded_run(&plan, catalog, reader, variant);
        if first != second {
            let message = format!(
                "{}: a second run read or returned something else",
                variant.name()
            );
            return Err(message.into());
        }

        if variant == Variant::Json {
            answer = body.map(|body| body.map(|bytes| engines::as_blocks(&bytes)));
        }
        snapshot += &format!("## {}\n{first}\n\n", variant.name());
    }

    let answer = answer.expect("the JSON output writes a body");
    Ok((answer, Some(snapshot.trim_end().to_owned())))
}

/// The largest answer a snapshot keeps whole; a bigger one is too long to
/// review, so it keeps a digest instead.
const FULL_ANSWER_BYTES: usize = 64 * 1024;

/// What the snapshot keeps: counts that show which table changed, then the
/// answer itself, or a digest of it that catches a change the counts miss.
fn summarize(answer: &Answer) -> String {
    let blocks = match answer {
        Ok(blocks) => blocks,
        Err(message) => return format!("rejected: {message}\n"),
    };

    let mut out = match (blocks.first(), blocks.last()) {
        (Some(first), Some(last)) => format!(
            "blocks: {} ({} to {})\n",
            blocks.len(),
            block_number(first),
            block_number(last)
        ),
        _ => "blocks: 0\n".to_owned(),
    };
    for (table, count) in item_counts(blocks) {
        out += &format!("{table}: {count}\n");
    }

    let blocks: Vec<Value> = blocks.iter().map(sorted_keys).collect();
    let full = serde_json::to_string_pretty(&blocks).unwrap();
    if full.len() <= FULL_ANSWER_BYTES {
        out += &format!("\n{full}\n");
        return out;
    }

    let mut text = String::new();
    for block in &blocks {
        text += &serde_json::to_string(block).unwrap();
        text.push('\n');
    }
    out += &format!(
        "digest: {:032x}\n",
        xxhash_rust::xxh3::xxh3_128(text.as_bytes())
    );

    out
}

fn block_number(block: &Value) -> String {
    block["header"]["number"].to_string()
}

/// Items per table, summed over blocks. A table is any array beside the header.
fn item_counts(blocks: &[Value]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();

    for block in blocks {
        let Some(fields) = block.as_object() else {
            continue;
        };
        for (key, value) in fields {
            if let Some(items) = value.as_array() {
                *counts.entry(key.clone()).or_default() += items.len();
            }
        }
    }

    counts
}

/// `value` with object keys sorted at every level, so the snapshot does not
/// depend on the order the engine writes keys in.
fn sorted_keys(value: &Value) -> Value {
    match value {
        Value::Object(fields) => {
            let mut keys: Vec<&String> = fields.keys().collect();
            keys.sort();

            let sorted = keys
                .into_iter()
                .map(|key| (key.clone(), sorted_keys(&fields[key])))
                .collect();
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.iter().map(sorted_keys).collect()),
        scalar => scalar.clone(),
    }
}

/// Small enough that full scans stop after a few blocks and one EVM block
/// alone can exceed it.
const SMALL_PAGE: u64 = 256 << 10;

/// Longer predicate texts become a digest, which keeps a line readable and
/// still changes with any operand.
const SHORT: usize = 80;

#[derive(Clone, Copy, PartialEq, Eq)]
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

/// One run through the recorder: where the response starts and stops, then
/// the sorted reads. The JSON outputs also give their body.
fn recorded_run(
    plan: &Plan,
    catalog: &DatasetDescription,
    reader: &ParquetChunkReader,
    variant: Variant,
) -> (String, Option<Result<Vec<u8>, String>>) {
    let recorder = Recorder {
        inner: reader,
        reads: Mutex::new(Vec::new()),
    };

    let (response, body) = match variant {
        Variant::Json | Variant::SmallPage => {
            let weight_budget = match variant {
                Variant::SmallPage => SMALL_PAGE,
                _ => ExecOptions::default().weight_budget,
            };
            let options = ExecOptions {
                weight_budget,
                ..ExecOptions::default()
            };

            match execute_chunk_with(plan, catalog, &recorder, options) {
                Ok(Some(output)) => {
                    let range = format!(
                        "{}..{} n={} read_through={}",
                        output.first_block(),
                        output.last_block(),
                        output.num_blocks(),
                        bound(output.read_through()),
                    );
                    (range, Some(Ok(output.into_json_lines())))
                }
                Ok(None) => ("none".to_owned(), Some(Ok(Vec::new()))),
                Err(e) => (format!("error: {e:#}"), Some(Err(format!("{e:#}")))),
            }
        }
        Variant::Arrow => {
            let range = match execute_chunk_arrow(plan, catalog, &recorder, false, false) {
                Ok(Some(output)) => format!(
                    "{}..{} n={}",
                    output.first_block(),
                    output.last_block(),
                    output.num_blocks(),
                ),
                Ok(None) => "none".to_owned(),
                Err(e) => format!("error: {e:#}"),
            };
            (range, None)
        }
    };

    let mut reads = recorder.reads.into_inner().unwrap();
    reads.sort_unstable();

    let mut text = format!("response {response}");
    for read in reads {
        text.push('\n');
        text += &read;
    }
    (text, body)
}

struct Recorder<'a> {
    inner: &'a ParquetChunkReader,
    reads: Mutex<Vec<String>>,
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

/// The scanner gives a lone item's first column a stage of its own, and the
/// union of several items picks between equally demanded columns by their
/// order, so a reordering at any level changes the line.
fn a_read_line_keeps_the_order_of_the_predicates() -> Result<(), Failed> {
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

    if same.is_empty() {
        Ok(())
    } else {
        Err(format!("a reordering of {same:?} reads the same").into())
    }
}

/// Hashes every cached `s3://` chunk of the registry and marks the ones whose
/// files match, so the tests read only those. Prints what does not match, and
/// the hashes of a chunk that has none yet.
fn verify_chunks() -> bool {
    let mut all_match = true;

    for (name, chunk) in store::registry() {
        if !chunk.is_stored() {
            continue;
        }

        let dir = store::chunk_path(&chunk.reference);
        // Gone until this check passes, so a failed one leaves nothing to trust.
        let marker = store::marker_path(&dir);
        let _ = std::fs::remove_file(&marker);

        let found = match hash_files(&dir) {
            Ok(found) => found,
            Err(e) => {
                eprintln!("{name}: {e}");
                all_match = false;
                continue;
            }
        };

        if chunk.xxh3.is_empty() {
            eprintln!(
                "{name}: chunk-registry.yaml gives no hashes. If the copy at {} is the right one, add:\n{}",
                dir.display(),
                as_yaml(&found)
            );
            all_match = false;
            continue;
        }

        let differences = hash_differences(&chunk.xxh3, &found);
        if !differences.is_empty() {
            eprintln!(
                "{name}: the copy at {} is not the one chunk-registry.yaml describes:\n{differences}",
                dir.display()
            );
            all_match = false;
            continue;
        }

        std::fs::write(&marker, store::marker_text(&chunk.xxh3)).unwrap();
        println!("{name}: verified");
    }

    all_match
}

/// The XXH3-128 of every file in `dir`, by name. A check against damage and
/// rewrites, not tampering, so it needs no cryptographic hash, and this one
/// runs at the speed the disk reads.
fn hash_files(dir: &Path) -> Result<BTreeMap<String, String>, String> {
    let entries = std::fs::read_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let mut hashes = BTreeMap::new();

    for entry in entries {
        let path = entry.map_err(|e| e.to_string())?.path();
        if !path.is_file() {
            return Err(format!("{} is not a file", path.display()));
        }

        let hash = hash_file(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        hashes.insert(name, hash);
    }

    Ok(hashes)
}

fn hash_file(path: &Path) -> std::io::Result<String> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    let mut buffer = vec![0u8; 1 << 20];

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(format!("{:032x}", hasher.digest128()))
}

fn hash_differences(
    expected: &BTreeMap<String, String>,
    found: &BTreeMap<String, String>,
) -> String {
    let names: BTreeSet<&String> = expected.keys().chain(found.keys()).collect();
    let mut out = String::new();

    for name in names {
        match (expected.get(name), found.get(name)) {
            (Some(a), Some(b)) if a == b => {}
            (Some(a), Some(b)) => out += &format!("  {name}: expected {a}, found {b}\n"),
            (Some(_), None) => out += &format!("  {name}: missing\n"),
            (None, _) => out += &format!("  {name}: not in chunk-registry.yaml\n"),
        }
    }

    out
}

/// The lines a registry entry takes for `hashes`.
fn as_yaml(hashes: &BTreeMap<String, String>) -> String {
    let mut out = "  xxh3:\n".to_owned();
    for (name, hash) in hashes {
        out += &format!("    {name}: {hash}\n");
    }
    out
}

/// Both engines must give the same answer, or both must reject the query,
/// unless `chunks.yaml` says this query differs on purpose.
#[cfg(feature = "legacy-query")]
fn check_reference(case: &Case, query: &[u8], chunk: &Path, new: &Answer) -> Result<(), Failed> {
    let reference = engines::run_legacy(query, chunk).map(|body| engines::as_blocks(&body));
    let agree = match (&reference, new) {
        (Ok(theirs), Ok(ours)) => theirs == ours,
        (Err(_), Err(_)) => true,
        _ => false,
    };

    match (case.setup.legacy_differs.get(&case.stem), agree) {
        (None, true) | (Some(_), false) => Ok(()),
        (Some(reason), true) => {
            let message = format!(
                "the answer now equals the reference; remove {} from legacy_differs \
                 (it said: {reason})",
                case.stem
            );
            Err(message.into())
        }
        (None, false) => Err(describe_mismatch(case, &reference, new).into()),
    }
}

#[cfg(feature = "legacy-query")]
fn describe_mismatch(case: &Case, reference: &Answer, new: &Answer) -> String {
    let (theirs, ours) = match (reference, new) {
        (Ok(theirs), Ok(ours)) => (theirs, ours),
        _ => {
            return format!(
                "the answer differs from the reference\n  reference: {}\n  new: {}",
                outcome(reference),
                outcome(new)
            );
        }
    };

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let saved = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("queries")
        .join(case.dir.strip_prefix(root).unwrap())
        .join(&case.snapshot);
    std::fs::create_dir_all(&saved).unwrap();
    for (name, blocks) in [("reference.json", theirs), ("new.json", ours)] {
        let file = std::fs::File::create(saved.join(name)).unwrap();
        serde_json::to_writer_pretty(file, blocks).unwrap();
    }

    format!(
        "the answer differs from the reference\n  reference: {}\n  new: {}\n  \
         first difference: {}\n  full answers: {}",
        outcome(reference),
        outcome(new),
        first_difference(theirs, ours),
        saved.display()
    )
}

#[cfg(feature = "legacy-query")]
fn outcome(answer: &Answer) -> String {
    let blocks = match answer {
        Ok(blocks) => blocks,
        Err(message) => return format!("rejected: {message}"),
    };

    let mut out = format!("blocks {}", blocks.len());
    for (table, count) in item_counts(blocks) {
        out += &format!(", {table} {count}");
    }
    out
}

#[cfg(feature = "legacy-query")]
fn first_difference(theirs: &[Value], ours: &[Value]) -> String {
    for (a, b) in theirs.iter().zip(ours) {
        if let Some(difference) = difference(a, b, "") {
            return format!("block {}{difference}", block_number(a));
        }
    }

    let common = theirs.len().min(ours.len());
    match (theirs.get(common), ours.get(common)) {
        (Some(block), _) => format!("block {}: only the reference has it", block_number(block)),
        (_, Some(block)) => format!("block {}: only the new engine has it", block_number(block)),
        _ => "none".to_owned(),
    }
}

/// The path to the first value that differs, with both sides.
#[cfg(feature = "legacy-query")]
fn difference(a: &Value, b: &Value, path: &str) -> Option<String> {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let keys: BTreeSet<&String> = x.keys().chain(y.keys()).collect();
            for key in keys {
                let path = format!("{path}.{key}");
                match (x.get(key), y.get(key)) {
                    (Some(p), Some(q)) => {
                        if let Some(found) = difference(p, q, &path) {
                            return Some(found);
                        }
                    }
                    (p, q) => {
                        return Some(format!("{path}: reference {}, new {}", show(p), show(q)))
                    }
                }
            }
            None
        }
        (Value::Array(x), Value::Array(y)) => {
            for (i, (p, q)) in x.iter().zip(y).enumerate() {
                if let Some(found) = difference(p, q, &format!("{path}[{i}]")) {
                    return Some(found);
                }
            }
            let lengths_differ = x.len() != y.len();
            lengths_differ.then(|| {
                format!(
                    "{path}: reference has {} items, new has {}",
                    x.len(),
                    y.len()
                )
            })
        }
        _ => {
            (a != b).then(|| format!("{path}: reference {}, new {}", show(Some(a)), show(Some(b))))
        }
    }
}

#[cfg(feature = "legacy-query")]
fn show(value: Option<&Value>) -> String {
    let Some(value) = value else {
        return "absent".to_owned();
    };

    let text = value.to_string();
    if text.len() <= 120 {
        return text;
    }
    let cut = text.char_indices().nth(120).map_or(text.len(), |(i, _)| i);
    format!("{}…", &text[..cut])
}
