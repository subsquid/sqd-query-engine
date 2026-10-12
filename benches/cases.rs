// The bench workload: every query under `benches/queries/`, on each chunk its
// directory's `chunks.yaml` names. Included via `#[path]` from bench binaries.
#![allow(dead_code)]

#[path = "../tests/conformance/harness/store.rs"]
mod store;

use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use std::path::{Path, PathBuf};

/// One query on one chunk. Opens nothing, so listing the cases costs no reads.
#[derive(Clone, Debug)]
pub struct Case {
    /// The query's path under `benches/queries/`, without `.json`, and the
    /// chunk's name after a dot when the directory names several; the name of
    /// its snapshots too.
    pub name: String,
    pub query: PathBuf,
    pub chunk: String,
}

impl Case {
    pub fn query(&self) -> Vec<u8> {
        std::fs::read(&self.query).unwrap_or_else(|e| panic!("{}: {e}", self.query.display()))
    }

    /// The bundled catalog that serves the query's `type`.
    pub fn catalog(&self) -> DatasetDescription {
        let query: serde_json::Value = serde_json::from_slice(&self.query()).unwrap();
        let kind = query["type"].as_str().expect("the query has a \"type\"");
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("metadata");

        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "yaml") {
                let catalog = load_dataset_description(&path).unwrap();
                if catalog.name == kind {
                    return catalog;
                }
            }
        }

        panic!("{}: no catalog serves query type {kind}", self.name)
    }

    /// The chunk's directory; a bench that runs a case needs it there.
    pub fn chunk_dir(&self) -> PathBuf {
        let dir = store::chunk(&self.chunk);
        assert!(
            dir.is_dir(),
            "{}: chunk {} is not at {}; run `make fetch`",
            self.name,
            self.chunk,
            dir.display()
        );
        dir
    }
}

/// The cases of one directory under `benches/queries/`: its chunks in the
/// order `chunks.yaml` lists them, and for each the queries sorted by name.
pub fn cases(dir: &str) -> Vec<Case> {
    let root = store::bench_queries_dir();
    let path = root.join(dir);
    let setup = store::load_query_dir(&path.join("chunks.yaml"));
    let several = setup.chunks.len() > 1;

    let mut cases = Vec::new();
    for chunk in &setup.chunks {
        for query in store::query_files(&path) {
            let stem = query.file_stem().unwrap().to_string_lossy();
            let name = if several {
                format!("{dir}/{stem}.{chunk}")
            } else {
                format!("{dir}/{stem}")
            };

            cases.push(Case {
                name,
                query,
                chunk: chunk.clone(),
            });
        }
    }

    cases
}

/// Every case of every directory under `benches/queries/`.
pub fn all_cases() -> Vec<Case> {
    let root = store::bench_queries_dir();
    store::query_dirs()
        .into_iter()
        .filter_map(|dir| {
            let relative = dir.strip_prefix(&root).ok()?;
            Some(cases(&relative.to_string_lossy()))
        })
        .flatten()
        .collect()
}
