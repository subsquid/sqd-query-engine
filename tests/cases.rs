//! Real-chunk cases: queries plus a reference to the network chunk they run on.
//!
//! Each `case.yaml` under `tests/cases/` names one chunk by its storage path, and
//! every `*.json` beside it is a query for that chunk. Git keeps neither the chunk
//! nor the answer: `make fetch-cases` downloads chunks into a local cache, and an
//! insta snapshot keeps each answer as counts and a digest.
//!
//! With `--features legacy-query` both engines answer, and they must agree before
//! the snapshot is checked. Without it the snapshot alone guards the answer, which
//! is what keeps a case useful once the reference engine is gone.
//!
//! Every case is ignored by the portable suite. `make test-cases` selects them and
//! sets `SQD_REQUIRE_CASES=1`, which turns a chunk missing from the cache into a
//! failure. See `tests/cases/README.md`.

use libtest_mimic::{Arguments, Failed, Trial};
use serde::Deserialize;
use serde_json::Value;
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[path = "conformance/harness/engines.rs"]
mod engines;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    /// `s3://<dataset id>/<chunk path>`, the dataset id as the network lists it.
    chunk: String,
    /// Queries whose answers differ from the reference on purpose: file stem to reason.
    #[serde(default)]
    legacy_differs: BTreeMap<String, String>,
}

type Catalogs = HashMap<String, DatasetDescription>;

type Answer = Result<Vec<Value>, String>;

fn main() {
    let args = Arguments::from_args();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/cases");
    let catalogs = Arc::new(load_catalogs());

    let mut trials = Vec::new();
    for case_file in case_files(&root) {
        let dir = case_file.parent().unwrap().to_path_buf();
        let case = Arc::new(load_case(&case_file, &dir));

        for query in query_files(&dir) {
            let stem = query.file_stem().unwrap().to_string_lossy().into_owned();
            let name = dir.strip_prefix(&root).unwrap().join(&stem);
            let (case, dir, catalogs) = (case.clone(), dir.clone(), catalogs.clone());

            let trial = Trial::test(name.to_string_lossy(), move || {
                run(&case, &dir, &stem, &query, &catalogs)
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

/// Every `case.yaml` under `dir`, at any depth, in a stable order.
fn case_files(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };

    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found.extend(case_files(&path));
        } else if path.file_name().is_some_and(|n| n == "case.yaml") {
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

/// A malformed case fails the whole run, the portable suite included, so it
/// cannot sit unnoticed until someone fetches its chunk.
fn load_case(file: &Path, dir: &Path) -> Case {
    let text = std::fs::read(file).unwrap();
    let case: Case =
        serde_yaml::from_slice(&text).unwrap_or_else(|e| panic!("{}: {e}", file.display()));

    assert!(
        case.chunk.starts_with("s3://"),
        "{}: chunk must be an s3:// path, got {}",
        file.display(),
        case.chunk
    );
    for stem in case.legacy_differs.keys() {
        assert!(
            dir.join(format!("{stem}.json")).is_file(),
            "{}: legacy_differs names {stem}, which has no query file",
            file.display()
        );
    }

    case
}

fn run(
    case: &Case,
    dir: &Path,
    stem: &str,
    query_file: &Path,
    catalogs: &Catalogs,
) -> Result<(), Failed> {
    let Some(chunk) = cached_chunk(&case.chunk)? else {
        return Ok(());
    };

    let query = std::fs::read(query_file)?;
    let kind = query_type(&query)?;
    let catalog = catalogs
        .get(&kind)
        .ok_or_else(|| format!("no catalog serves query type {kind}"))?;

    let new = engines::run_new(&query, catalog, &chunk).map(|body| engines::as_blocks(&body));

    #[cfg(feature = "legacy-query")]
    check_reference(case, dir, stem, &query, &chunk, &new)?;

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

fn cache_root() -> PathBuf {
    if let Some(dir) = std::env::var_os("SQD_CHUNK_CACHE") {
        return PathBuf::from(dir);
    }

    let home = std::env::var_os("HOME").expect("HOME or SQD_CHUNK_CACHE must be set");
    Path::new(&home).join(".cache/sqd-chunks")
}

/// The chunk's directory in the cache, or `None` when it has not been fetched.
fn cached_chunk(reference: &str) -> Result<Option<PathBuf>, Failed> {
    let dir = cache_root().join(reference.trim_start_matches("s3://"));
    if dir.is_dir() {
        return Ok(Some(dir));
    }

    if std::env::var_os("SQD_REQUIRE_CASES").is_some() {
        let message = format!(
            "{reference} is not in the cache at {}; run `make fetch-cases`",
            dir.display()
        );
        return Err(message.into());
    }

    Ok(None)
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
/// unless the case says this query differs on purpose.
#[cfg(feature = "legacy-query")]
fn check_reference(
    case: &Case,
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

    match (case.legacy_differs.get(stem), agree) {
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

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/cases");
    let saved = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("cases")
        .join(dir.strip_prefix(root).unwrap())
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
