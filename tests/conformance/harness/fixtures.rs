//! HC-1 — the fixture chunk loader and query runner.
//!
//! The externally supplied tree under `tests/fixtures/` is not in the
//! repository. `make test-data` sets `SQD_REQUIRE_FIXTURES=1`, which turns its
//! absence from a skip into a failure; the portable suite leaves it unset and
//! does not select the tests that read it.

pub use crate::harness::guard::{fixture_chunk, fixture_tree_is_present};

use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::output::execute_plan;
use sqd_query_engine::query::{compile, parse_query};
use std::path::Path;

pub fn meta(name: &str) -> DatasetDescription {
    load_dataset_description(Path::new(&format!("metadata/{name}.yaml"))).unwrap()
}

/// Run a query against a bundled dataset's fixture chunk, to completion.
pub fn run(
    dataset: &str,
    metadata: &DatasetDescription,
    query_json: &[u8],
) -> anyhow::Result<Vec<u8>> {
    run_against_bytes(metadata, &fixture_chunk(dataset), query_json)
}

/// Run a query against a chunk named directly — a synthetic one, or a fixture
/// chunk rewritten for the test.
pub fn run_against(
    catalog: &DatasetDescription,
    chunk: &Path,
    query: &str,
) -> anyhow::Result<Vec<u8>> {
    run_against_bytes(catalog, chunk, query.as_bytes())
}

fn run_against_bytes(
    catalog: &DatasetDescription,
    chunk: &Path,
    query: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let parsed = parse_query(query, catalog)?;
    let plan = compile(&parsed, catalog)?;

    Ok(execute_plan(&plan, catalog, chunk)?
        .map(|out| out.into_json_lines())
        .unwrap_or_default())
}

/// The error a query that parses but cannot be planned fails with.
pub fn plan_error(catalog: &DatasetDescription, query: &str) -> anyhow::Error {
    let parsed = parse_query(query.as_bytes(), catalog).expect("the filter surface accepts it");
    compile(&parsed, catalog).expect_err("the value does not")
}
