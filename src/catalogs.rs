//! The catalogs under `metadata/`, compiled in for an embedder that has no
//! checkout to load them from.

/// The YAML of a bundled catalog, by its file name without the extension.
pub fn bundled(name: &str) -> Option<&'static str> {
    let yaml = match name {
        "bitcoin" => include_str!("../metadata/bitcoin.yaml"),
        "evm" => include_str!("../metadata/evm.yaml"),
        "hyperliquid_fills" => include_str!("../metadata/hyperliquid_fills.yaml"),
        "hyperliquid_replica_cmds" => include_str!("../metadata/hyperliquid_replica_cmds.yaml"),
        "solana" => include_str!("../metadata/solana.yaml"),
        "substrate" => include_str!("../metadata/substrate.yaml"),
        "tron" => include_str!("../metadata/tron.yaml"),
        _ => return None,
    };
    Some(yaml)
}

#[cfg(test)]
mod tests {
    use super::bundled;

    #[test]
    fn every_bundled_catalog_parses() {
        for entry in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/metadata")).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|ext| ext != "yaml") {
                continue;
            }
            let name = path.file_stem().unwrap().to_str().unwrap();
            let yaml = bundled(name).unwrap_or_else(|| panic!("{name} is not bundled"));
            crate::metadata::parse_dataset_description(yaml).unwrap();
        }
    }
}
