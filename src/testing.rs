//! Test-only access to external chunks under `data/`.
//!
//! Data-backed tests are ignored by the portable suite. `make test-data` selects
//! them and sets `SQD_REQUIRE_CHUNKS=1`, making a missing input a failure.

use std::path::{Path, PathBuf};

/// The chunk directory for a dataset, or `None` when it is not checked out.
pub(crate) fn chunk_dir(dataset: &str) -> Option<PathBuf> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("data")
        .join(dataset)
        .join("chunk");

    if path.is_dir() {
        return Some(path);
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_CHUNKS").is_none(),
        "SQD_REQUIRE_CHUNKS is set but {} is not checked out, so this test would \
         report green having read nothing",
        path.display()
    );

    None
}

/// Whether both bundled chunks are checked out. They arrive together, and the
/// tests that read one usually read the other.
pub(crate) fn chunks_present() -> bool {
    chunk_dir("evm").is_some() && chunk_dir("solana").is_some()
}
