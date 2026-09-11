//! Where the external fixture tree is, and whether it is there.
//!
//! Shared verbatim by both test targets through `#[path]`, because a skip guard
//! in two copies is a skip guard that will disagree with itself — and the copy
//! that says "absent" when the tree is present reports green having compared
//! nothing, which §8.1 names as the most common way a conformance suite lies.

use std::path::{Path, PathBuf};

pub fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

pub fn fixture_chunk(dataset: &str) -> PathBuf {
    fixture_dir().join(dataset).join("chunk")
}

/// Whether one dataset's chunk is available. `make test-data` sets
/// `SQD_REQUIRE_FIXTURES=1`, which turns its absence from a skip into a failure.
pub fn fixture_tree_has(dataset: &str) -> bool {
    if fixture_chunk(dataset).is_dir() {
        return true;
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_FIXTURES").is_none(),
        "SQD_REQUIRE_FIXTURES is set but tests/fixtures/{dataset} is not checked out, \
         so these tests would report green having compared nothing"
    );

    false
}

/// Whether the fixture tree is available, answered by its ethereum chunk.
///
/// A test reading some other dataset must name that one instead. A checkout
/// carrying only solana takes the false branch here and skips with the chunk it
/// needed sitting on disk unread, and one carrying only ethereum takes the true
/// branch and then dies inside the reader — a missing input reported as a
/// malformed chunk.
pub fn fixture_tree_is_present() -> bool {
    fixture_tree_has("ethereum")
}

/// Where the reference implementation's query sources are, if they are.
///
/// The fixture tree is a checkout of the reference's `crates/query/fixtures`,
/// so the sources sit two directories up from it; `SQD_REFERENCE_SRC` names
/// them directly for a tree that was copied without them. Same contract as
/// [`fixture_tree_has`]: absent is a skip, unless `SQD_REQUIRE_FIXTURES` says
/// it is a failure.
// Shared with `e2e_fixtures`, which has no use for it.
#[allow(dead_code)]
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
        std::env::var_os("SQD_REQUIRE_FIXTURES").is_none(),
        "SQD_REQUIRE_FIXTURES is set but the reference sources are not at {}, \
         so this test would report green having compared nothing",
        sources.display()
    );

    None
}
