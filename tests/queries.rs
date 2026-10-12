//! Queries on real chunks, each directory with a reference to the chunk it runs on.
//!
//! Each `chunks.yaml` under `tests/queries/` names one chunk, by its storage path or,
//! until the store holds it, by its place in the fixture tree. Every `*.json`
//! beside it is a query for that chunk. Git keeps neither the chunk
//! nor the answer: `make fetch` downloads chunks into a local cache, and an
//! insta snapshot keeps each answer as counts and a digest.
//!
//! With `--features legacy-query` both engines answer, and they must agree before
//! the snapshot is checked. Without it the snapshot alone guards the answer, which
//! is what keeps a query useful once the reference engine is gone.
//!
//! Every query is ignored by the portable suite. An explicitly selected query fails
//! if its chunk is missing from the cache or unchecked against its hashes.
//! `--list-chunks` prints the chunk paths for `make fetch`, and `--verify-chunks`
//! hashes what it fetched, using the same YAML parser. See `tests/queries/README.md`.

use libtest_mimic::{Arguments, Failed, Trial};
use serde_json::Value;
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[path = "conformance/harness/engines.rs"]
mod engines;
// Reads chunks by directory, so the by-dataset lookup goes unused here.
#[allow(dead_code)]
#[path = "conformance/harness/store.rs"]
mod store;

use store::QueryDir;

type Catalogs = HashMap<String, DatasetDescription>;

type Answer = Result<Vec<Value>, String>;

fn main() {
    let root = store::queries_dir();
    if std::env::args().skip(1).eq(["--list-chunks"]) {
        let chunks: BTreeSet<_> = chunk_files(&root)
            .into_iter()
            .map(|file| load_query_dir(&file, file.parent().unwrap()).chunk)
            .filter_map(|chunk| chunk.strip_prefix("s3://").map(str::to_owned))
            .collect();
        for chunk in chunks {
            println!("{chunk}");
        }
        return;
    }
    if std::env::args().skip(1).eq(["--verify-chunks"]) {
        let all_match = verify_chunks(&root);
        std::process::exit(if all_match { 0 } else { 1 });
    }

    let args = Arguments::from_args();
    let catalogs = Arc::new(load_catalogs());

    let mut trials = Vec::new();
    for chunk_file in chunk_files(&root) {
        let dir = chunk_file.parent().unwrap().to_path_buf();
        let setup = Arc::new(load_query_dir(&chunk_file, &dir));

        for query in query_files(&dir) {
            let stem = query.file_stem().unwrap().to_string_lossy().into_owned();
            let name = dir.strip_prefix(&root).unwrap().join(&stem);
            let (setup, dir, catalogs) = (setup.clone(), dir.clone(), catalogs.clone());

            let trial = Trial::test(name.to_string_lossy(), move || {
                run(&setup, &dir, &stem, &query, &catalogs)
            });
            trials.push(trial.with_ignored_flag(true));
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

/// Every `chunks.yaml` under `dir`, at any depth, in a stable order.
fn chunk_files(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };

    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found.extend(chunk_files(&path));
        } else if path.file_name().is_some_and(|n| n == "chunks.yaml") {
            found.push(path);
        }
    }

    found.sort();
    found
}

fn query_files(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "json"))
        .collect();

    found.sort();
    found
}

/// A malformed `chunks.yaml` fails the whole run, the portable suite included.
fn load_query_dir(file: &Path, dir: &Path) -> QueryDir {
    let setup = store::load_query_dir(file);

    for stem in setup.legacy_differs.keys() {
        assert!(
            dir.join(format!("{stem}.json")).is_file(),
            "{}: legacy_differs names {stem}, which has no query file",
            file.display()
        );
    }

    setup
}

fn run(
    setup: &QueryDir,
    dir: &Path,
    stem: &str,
    query_file: &Path,
    catalogs: &Catalogs,
) -> Result<(), Failed> {
    let chunk = cached_chunk(setup)?;

    let query = std::fs::read(query_file)?;
    let kind = query_type(&query)?;
    let catalog = catalogs
        .get(&kind)
        .ok_or_else(|| format!("no catalog serves query type {kind}"))?;

    let new = engines::run_new(&query, catalog, &chunk).map(|body| engines::as_blocks(&body));

    #[cfg(feature = "legacy-query")]
    check_reference(setup, dir, stem, &query, &chunk, &new)?;

    let summary = summarize(&new);
    insta::with_settings!({
        snapshot_path => dir,
        prepend_module_to_snapshot => false,
        omit_expression => true,
    }, {
        insta::assert_snapshot!(stem, summary);
    });

    Ok(())
}

/// The chunk's directory; a selected query requires its input, and the right one.
fn cached_chunk(setup: &QueryDir) -> Result<PathBuf, Failed> {
    let reference = &setup.chunk;
    let dir = store::chunk_path(reference);

    if !dir.is_dir() {
        let remedy = if reference.starts_with("s3://") {
            "run `make fetch`"
        } else {
            "the fixture tree is not checked out"
        };
        return Err(format!("{reference} is not at {}; {remedy}", dir.display()).into());
    }

    store::check_verified(setup, &dir)?;
    Ok(dir)
}

/// Hashes every cached `s3://` chunk and marks the ones whose files match their
/// `chunks.yaml`, so the tests read only those. Prints what does not match, and
/// the hashes of a chunk that has none yet.
fn verify_chunks(root: &Path) -> bool {
    let mut chunks: BTreeMap<String, (PathBuf, BTreeMap<String, String>)> = BTreeMap::new();
    for file in chunk_files(root) {
        let setup = store::load_query_dir(&file);
        if !setup.chunk.starts_with("s3://") {
            continue;
        }

        if let Some((other, hashes)) = chunks.get(&setup.chunk) {
            assert!(
                *hashes == setup.xxh3,
                "{} and {} give {} different hashes",
                other.display(),
                file.display(),
                setup.chunk
            );
            continue;
        }
        chunks.insert(setup.chunk, (file, setup.xxh3));
    }

    let mut all_match = true;
    for (reference, (file, expected)) in chunks {
        let dir = store::chunk_path(&reference);
        // Gone until this check passes, so a failed one leaves nothing to trust.
        let marker = store::marker_path(&dir);
        let _ = std::fs::remove_file(&marker);

        let found = match hash_files(&dir) {
            Ok(found) => found,
            Err(e) => {
                eprintln!("{reference}: {e}");
                all_match = false;
                continue;
            }
        };

        if expected.is_empty() {
            eprintln!(
                "{reference}: {} has no hashes. If the copy at {} is the right one, add:\n{}",
                shown(&file),
                dir.display(),
                as_yaml(&found)
            );
            all_match = false;
            continue;
        }

        let differences = hash_differences(&expected, &found);
        if !differences.is_empty() {
            eprintln!(
                "{reference}: the copy at {} is not the one {} names:\n{differences}",
                dir.display(),
                shown(&file)
            );
            all_match = false;
            continue;
        }

        std::fs::write(&marker, store::marker_text(&expected)).unwrap();
        println!("{reference}: verified");
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
            (None, _) => out += &format!("  {name}: not in chunks.yaml\n"),
        }
    }

    out
}

/// `file` from the repository root.
fn shown(file: &Path) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    file.strip_prefix(root)
        .unwrap_or(file)
        .display()
        .to_string()
}

fn as_yaml(hashes: &BTreeMap<String, String>) -> String {
    let mut out = "xxh3:\n".to_owned();
    for (name, hash) in hashes {
        out += &format!("  {name}: {hash}\n");
    }
    out
}

fn query_type(query: &[u8]) -> Result<String, Failed> {
    let parsed: Value = serde_json::from_slice(query)?;
    let kind = parsed["type"].as_str().ok_or("the query has no \"type\"")?;
    Ok(kind.to_owned())
}

/// What the snapshot keeps: enough to see which table changed, plus a digest
/// that catches a change the counts miss.
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

    let mut text = String::new();
    for block in blocks {
        canonical(block, &mut text);
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

/// JSON with object keys sorted at every level, so the digest does not depend
/// on the order the engine writes keys in.
fn canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(fields) => {
            let mut keys: Vec<&String> = fields.keys().collect();
            keys.sort();

            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).unwrap());
                out.push(':');
                canonical(&fields[key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// Both engines must give the same answer, or both must reject the query,
/// unless `chunks.yaml` says this query differs on purpose.
#[cfg(feature = "legacy-query")]
fn check_reference(
    setup: &QueryDir,
    dir: &Path,
    stem: &str,
    query: &[u8],
    chunk: &Path,
    new: &Answer,
) -> Result<(), Failed> {
    let reference = engines::run_legacy(query, chunk).map(|body| engines::as_blocks(&body));
    let agree = match (&reference, new) {
        (Ok(theirs), Ok(ours)) => theirs == ours,
        (Err(_), Err(_)) => true,
        _ => false,
    };

    match (setup.legacy_differs.get(stem), agree) {
        (None, true) | (Some(_), false) => Ok(()),
        (Some(reason), true) => {
            let message = format!(
                "the answer now equals the reference; remove {stem} from legacy_differs \
                 (it said: {reason})"
            );
            Err(message.into())
        }
        (None, false) => Err(describe_mismatch(dir, stem, &reference, new).into()),
    }
}

#[cfg(feature = "legacy-query")]
fn describe_mismatch(dir: &Path, stem: &str, reference: &Answer, new: &Answer) -> String {
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

    let saved = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("queries")
        .join(dir.strip_prefix(store::queries_dir()).unwrap())
        .join(stem);
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
            let keys: std::collections::BTreeSet<&String> = x.keys().chain(y.keys()).collect();
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
