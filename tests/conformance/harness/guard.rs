//! Whether the real chunk a test needs is there, and what its absence means.
//!
//! Shared verbatim by both test targets through `#[path]`, because a skip guard
//! in two copies is a skip guard that will disagree with itself — and the copy
//! that says "absent" when the tree is present reports green having compared
//! nothing, which §8.1 names as the most common way a conformance suite lies.
//!
//! A guard names the dataset its test reads. A guard on some other dataset
//! skips a test whose chunk is there, or lets one through whose chunk is not.

use std::path::PathBuf;

pub use super::store::{dataset_chunk as fixture_chunk, fixture_dir};

/// Whether one dataset's chunk is available. `make test-data` sets
/// `SQD_REQUIRE_CHUNKS=1`, which turns its absence from a skip into a failure.
pub fn fixture_tree_has(dataset: &str) -> bool {
    let chunk = fixture_chunk(dataset);
    if chunk.is_dir() {
        return true;
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_CHUNKS").is_none(),
        "SQD_REQUIRE_CHUNKS is set but {dataset}'s chunk is not at {}, \
         so these tests would report green having compared nothing",
        chunk.display()
    );

    false
}

/// [`fixture_tree_has`] for a test that reads several datasets.
pub fn fixture_tree_has_all<'a>(datasets: impl IntoIterator<Item = &'a str>) -> bool {
    datasets.into_iter().all(fixture_tree_has)
}

/// Whether the fixture tree's queries are checked out, for a test that reads
/// them rather than a chunk. Same contract as [`fixture_tree_has`].
pub fn fixture_queries_present() -> bool {
    let dir = fixture_dir();
    if dir.is_dir() {
        return true;
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_CHUNKS").is_none(),
        "SQD_REQUIRE_CHUNKS is set but the fixture tree is not at {}, \
         so these tests would report green having compared nothing",
        dir.display()
    );

    false
}

/// Where the reference implementation's query sources are, if they are.
///
/// The fixture tree is a checkout of the reference's `crates/query/fixtures`,
/// so the sources sit two directories up from it; `SQD_REFERENCE_SRC` names
/// them directly for a tree that was copied without them. Same contract as
/// [`fixture_tree_has`]: absent is a skip, unless `SQD_REQUIRE_CHUNKS` says
/// it is a failure.
pub fn reference_query_sources() -> Option<PathBuf> {
    let sources = match std::env::var_os("SQD_REFERENCE_SRC") {
        Some(path) => PathBuf::from(path),
        None => std::fs::canonicalize(fixture_dir())
            .ok()
            .and_then(|fixtures| fixtures.parent().map(|query| query.join("src/query")))
            .unwrap_or_default(),
    };

    if sources.join("eth.rs").is_file() {
        return Some(sources);
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_CHUNKS").is_none(),
        "SQD_REQUIRE_CHUNKS is set but the reference sources are not at {}, \
         so this test would report green having compared nothing",
        sources.display()
    );

    None
}
