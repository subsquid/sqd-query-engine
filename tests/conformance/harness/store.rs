//! Where a real chunk is. `chunk-registry.yaml` at the repository root
//! describes each chunk once, by name: the reference that says where it is, and
//! the hashes of its files. A query directory's `chunks.yaml` names the chunks
//! its queries run on.
//!
//! Shared through `#[path]` by every target that reads a real chunk, so a change
//! of reference in the registry moves all of them at once. Each uses a part.
#![allow(dead_code)]

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// One chunk of the registry.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chunk {
    /// `s3://<bucket>/<chunk path>`, or `fixture:<dataset>` for a chunk only the
    /// fixture tree holds.
    #[serde(rename = "ref")]
    pub reference: String,
    /// The XXH3-128 of every file of an `s3://` chunk, by file name.
    #[serde(default)]
    pub xxh3: BTreeMap<String, String>,
}

impl Chunk {
    pub fn is_stored(&self) -> bool {
        self.reference.starts_with("s3://")
    }
}

/// What a query directory's `chunks.yaml` says about the queries beside it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryDir {
    /// The registry names of the chunks each query runs on, one at a time.
    pub chunks: Vec<String>,
    /// Queries whose answers differ from the reference on purpose: file stem to reason.
    #[serde(default)]
    pub legacy_differs: BTreeMap<String, String>,
}

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

pub fn registry_file() -> PathBuf {
    root().join("chunk-registry.yaml")
}

/// The queries on real chunks that are tests.
pub fn queries_dir() -> PathBuf {
    root().join("tests/queries")
}

/// The queries the benches run. The same format as `tests/queries/`.
pub fn bench_queries_dir() -> PathBuf {
    root().join("benches/queries")
}

pub fn fixture_dir() -> PathBuf {
    root().join("tests/fixtures")
}

/// Every chunk by name. A malformed registry fails whoever reads it, the
/// portable suite included, so it cannot sit unnoticed until someone fetches
/// a chunk.
pub fn registry() -> &'static BTreeMap<String, Chunk> {
    static REGISTRY: OnceLock<BTreeMap<String, Chunk>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let file = registry_file();
        let text = std::fs::read(&file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        let chunks: BTreeMap<String, Chunk> =
            serde_yaml::from_slice(&text).unwrap_or_else(|e| panic!("{}: {e}", file.display()));

        for (name, chunk) in &chunks {
            let fixture = chunk.reference.starts_with("fixture:");
            assert!(
                chunk.is_stored() || fixture,
                "{}: {name} must be s3://… or fixture:…, got {}",
                file.display(),
                chunk.reference
            );
            assert!(
                chunk.is_stored() || chunk.xxh3.is_empty(),
                "{}: {name}: only an s3:// chunk takes hashes; the legacy repo's LFS pins \
                 a fixture chunk",
                file.display()
            );
        }

        chunks
    })
}

/// The registry's chunk of that name.
pub fn named(name: &str) -> &'static Chunk {
    registry()
        .get(name)
        .unwrap_or_else(|| panic!("{} has no chunk named {name}", registry_file().display()))
}

/// A malformed file fails whoever reads it, like the registry.
pub fn load_query_dir(file: &Path) -> QueryDir {
    let text = std::fs::read(file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
    let dir: QueryDir =
        serde_yaml::from_slice(&text).unwrap_or_else(|e| panic!("{}: {e}", file.display()));

    assert!(!dir.chunks.is_empty(), "{}: names no chunk", file.display());
    for (i, name) in dir.chunks.iter().enumerate() {
        assert!(
            registry().contains_key(name),
            "{}: names {name}, which {} does not describe",
            file.display(),
            registry_file().display()
        );
        assert!(
            !dir.chunks[..i].contains(name),
            "{}: names {name} twice",
            file.display()
        );
    }

    dir
}

/// Every directory with a `chunks.yaml` under `tests/queries/` and
/// `benches/queries/`, at any depth, in a stable order.
pub fn query_dirs() -> Vec<PathBuf> {
    let mut found = Vec::new();
    for root in [queries_dir(), bench_queries_dir()] {
        collect_query_dirs(&root, &mut found);
    }
    found
}

fn collect_query_dirs(dir: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries.map(|entry| entry.unwrap().path()).collect();
    paths.sort();

    if dir.join("chunks.yaml").is_file() {
        found.push(dir.to_path_buf());
    }
    for path in paths {
        if path.is_dir() {
            collect_query_dirs(&path, found);
        }
    }
}

/// The queries of a directory, sorted by name.
pub fn query_files(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "json"))
        .collect();

    found.sort();
    found
}

/// The registry's chunk of that name, whether or not it is there. One that is
/// there but not checked against its hashes fails here rather than being read
/// as the right one.
pub fn chunk(name: &str) -> PathBuf {
    let described = named(name);
    let path = chunk_path(&described.reference);

    if path.is_dir() {
        if let Err(message) = check_verified(described, &path) {
            panic!("{message}");
        }
    }

    path
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

/// Whether the chunk at `path` is the one `chunk` describes: for a cached
/// chunk, a marker that matches the hashes the registry gives now. A fixture
/// chunk is the fixture tree's, so it passes.
pub fn check_verified(chunk: &Chunk, path: &Path) -> Result<(), String> {
    if !chunk.is_stored() {
        return Ok(());
    }

    if chunk.xxh3.is_empty() {
        let message = format!(
            "{} has no hashes in chunk-registry.yaml; `make fetch` prints them",
            chunk.reference
        );
        return Err(message);
    }

    let marker = std::fs::read_to_string(marker_path(path)).unwrap_or_default();
    if marker == marker_text(&chunk.xxh3) {
        return Ok(());
    }

    let message = format!(
        "{} at {} is not checked against the hashes in chunk-registry.yaml; run `make fetch`",
        chunk.reference,
        path.display()
    );
    Err(message)
}
