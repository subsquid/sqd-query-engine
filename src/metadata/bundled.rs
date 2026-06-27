//! Metadata bundled into the binary at compile time.
//!
//! The 7 dataset descriptions are `include_str!`'d and parsed once on first use.
//! This makes the engine self-contained: callers can select metadata by the
//! query's `type` field with no runtime file dependency and no reliance on the
//! sibling `../data` checkout (the build gotcha that bites the YAML-on-disk
//! path). Parsed lazily via `OnceLock`; a parse failure is a build-time-shaped
//! programmer error, so it panics with a clear message.

use crate::metadata::{parse_dataset_description, DatasetDescription};
use std::sync::OnceLock;

/// The bundled YAML sources. The dataset *name* (the query `type`) is taken from
/// each parsed description, so this list is the single source of truth — adding a
/// dataset is one line here.
const SOURCES: &[&str] = &[
    include_str!("../../metadata/evm.yaml"),
    include_str!("../../metadata/solana.yaml"),
    include_str!("../../metadata/substrate.yaml"),
    include_str!("../../metadata/bitcoin.yaml"),
    include_str!("../../metadata/fuel.yaml"),
    include_str!("../../metadata/hyperliquid_fills.yaml"),
    include_str!("../../metadata/hyperliquid_replica_cmds.yaml"),
];

/// Parse all bundled descriptions once. The returned slice is `'static`, so the
/// borrows handed out by [`bundled_metadata`] / [`supported_kinds`] live forever.
fn registry() -> &'static [DatasetDescription] {
    static REGISTRY: OnceLock<Vec<DatasetDescription>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        SOURCES
            .iter()
            .map(|yaml| {
                parse_dataset_description(yaml).expect("bundled dataset metadata must parse")
            })
            .collect()
    })
}

/// Look up a bundled dataset description by its name (the query `type`), e.g.
/// `"evm"`, `"solana"`, `"hyperliquidFills"`. Returns `None` for an unknown type
/// (the caller treats that as "this engine can't serve it").
pub fn bundled_metadata(name: &str) -> Option<&'static DatasetDescription> {
    registry().iter().find(|d| d.name == name)
}

/// The dataset kinds (names / query `type`s) this engine can serve. Seeds a
/// downstream capability allowlist. Ordered to match [`SOURCES`].
pub fn supported_kinds() -> &'static [&'static str] {
    static KINDS: OnceLock<Vec<&'static str>> = OnceLock::new();
    KINDS.get_or_init(|| registry().iter().map(|d| d.name.as_str()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_seven_datasets_bundled() {
        let kinds = supported_kinds();
        assert_eq!(kinds.len(), 7, "expected 7 bundled datasets, got {kinds:?}");
        for expected in [
            "evm",
            "solana",
            "substrate",
            "bitcoin",
            "fuel",
            "hyperliquidFills",
            "hyperliquidReplicaCmds",
        ] {
            assert!(
                kinds.contains(&expected),
                "missing bundled dataset '{expected}' in {kinds:?}"
            );
            // Each advertised kind must resolve to a description whose name matches.
            let desc = bundled_metadata(expected)
                .unwrap_or_else(|| panic!("bundled_metadata('{expected}') is None"));
            assert_eq!(desc.name, expected);
        }
    }

    #[test]
    fn unknown_kind_is_none() {
        assert!(bundled_metadata("dogecoin").is_none());
        assert!(bundled_metadata("").is_none());
    }

    #[test]
    fn bundled_matches_on_disk_evm() {
        // The bundled copy must equal the YAML the e2e/loader tests read from disk.
        let on_disk = crate::metadata::load_dataset_description(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/metadata/evm.yaml"
        )))
        .unwrap();
        let bundled = bundled_metadata("evm").unwrap();
        assert_eq!(bundled.tables.len(), on_disk.tables.len());
        assert_eq!(bundled.name, on_disk.name);
    }
}
