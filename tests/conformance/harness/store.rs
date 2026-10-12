//! Where a real chunk is: the reference a `chunks.yaml` under `tests/queries/`
//! gives it, resolved to a directory on disk.
//!
//! Shared through `#[path]` by every target that reads a real chunk, so a change
//! of reference in one `chunks.yaml` moves all of them at once.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// What a `chunks.yaml` says about the queries beside it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryDir {
    /// `s3://<bucket>/<chunk path>`, or `fixture:<dataset>` for a chunk only the
    /// fixture tree holds.
    pub chunk: String,
    /// The XXH3-128 of every file of an `s3://` chunk, by file name.
    #[serde(default)]
    pub xxh3: BTreeMap<String, String>,
    /// Queries whose answers differ from the reference on purpose: file stem to reason.
    // Only `tests/queries.rs` reads it; the other targets parse the same file.
    #[allow(dead_code)]
    #[serde(default)]
    pub legacy_differs: BTreeMap<String, String>,
}

pub fn queries_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/queries")
}

pub fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// A malformed file fails whoever reads it, the portable suite included, so it
/// cannot sit unnoticed until someone fetches its chunk.
pub fn load_query_dir(file: &Path) -> QueryDir {
    let text = std::fs::read(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
    let dir: QueryDir =
        serde_yaml::from_slice(&text).unwrap_or_else(|e| panic!("{}: {e}", file.display()));

    let stored = dir.chunk.starts_with("s3://");
    let fixture = dir.chunk.starts_with("fixture:");
    assert!(
        stored || fixture,
        "{}: chunk must be s3://… or fixture:…, got {}",
        file.display(),
        dir.chunk
    );
    assert!(
        stored || dir.xxh3.is_empty(),
        "{}: only an s3:// chunk takes hashes; the legacy repo's LFS pins a fixture chunk",
        file.display()
    );

    dir
}

/// The chunk `tests/queries/<dataset>/chunks.yaml` names, whether or not it is
/// there. One that is there but not checked against its hashes fails here
/// rather than being read as the right one.
pub fn dataset_chunk(dataset: &str) -> PathBuf {
    let dir = load_query_dir(&queries_dir().join(dataset).join("chunks.yaml"));
    let chunk = chunk_path(&dir.chunk);

    if chunk.is_dir() {
        if let Err(message) = check_verified(&dir, &chunk) {
            panic!("{message}");
        }
    }

    chunk
}

/// Where the chunk `reference` names sits on disk, whether or not it is there.
pub fn chunk_path(reference: &str) -> PathBuf {
    if let Some(path) = reference.strip_prefix("s3://") {
        return cache_root().join(path);
    }

    let dataset = reference
        .strip_prefix("fixture:")
        .unwrap_or_else(|| panic!("unknown chunk reference {reference}"));
    fixture_dir().join(dataset).join("chunk")
}

/// Where `make fetch` puts `s3://` chunks; the Makefile reads the same variable.
pub fn cache_root() -> PathBuf {
    if let Some(dir) = std::env::var_os("SQD_CHUNK_CACHE") {
        return PathBuf::from(dir);
    }

    let home = std::env::var_os("HOME").expect("HOME or SQD_CHUNK_CACHE must be set");
    Path::new(&home).join(".cache/sqd-chunks")
}

/// The file beside a cached chunk that `make fetch` writes once the chunk's
/// files match their hashes. Beside, not inside, so the chunk directory stays
/// exactly what the store holds.
pub fn marker_path(chunk: &Path) -> PathBuf {
    let mut name = chunk.file_name().unwrap().to_owned();
    name.push(".verified");
    chunk.with_file_name(name)
}

/// What the marker holds for `hashes`: a `<hash>  <file>` line per file.
pub fn marker_text(hashes: &BTreeMap<String, String>) -> String {
    hashes
        .iter()
        .map(|(file, hash)| format!("{hash}  {file}\n"))
        .collect()
}

/// Whether the chunk at `path` is the one `dir` names: for a cached chunk, a
/// marker that matches the hashes `chunks.yaml` gives now. A fixture chunk is
/// the fixture tree's, so it passes.
pub fn check_verified(dir: &QueryDir, path: &Path) -> Result<(), String> {
    if !dir.chunk.starts_with("s3://") {
        return Ok(());
    }

    if dir.xxh3.is_empty() {
        let message = format!(
            "{} has no hashes in its chunks.yaml; `make fetch` prints them",
            dir.chunk
        );
        return Err(message);
    }

    let marker = std::fs::read_to_string(marker_path(path)).unwrap_or_default();
    if marker == marker_text(&dir.xxh3) {
        return Ok(());
    }

    let message = format!(
        "{} at {} is not checked against the hashes in its chunks.yaml; run `make fetch`",
        dir.chunk,
        path.display()
    );
    Err(message)
}
