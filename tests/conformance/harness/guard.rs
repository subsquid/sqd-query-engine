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

/// Whether the external fixture tree is available. `make test-data` sets
/// `SQD_REQUIRE_FIXTURES=1`, which turns its absence from a skip into a failure.
pub fn fixture_tree_is_present() -> bool {
    if fixture_chunk("ethereum").is_dir() {
        return true;
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_FIXTURES").is_none(),
        "SQD_REQUIRE_FIXTURES is set but tests/fixtures is not checked out, so these \
         tests would report green having compared nothing"
    );

    false
}
