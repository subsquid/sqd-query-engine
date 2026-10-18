//! The catalogs in `metadata/`, built into the crate, and the rendering a
//! description takes from them.

use crate::DatasetDescription;
use std::sync::OnceLock;

const SHIPPED: [&str; 7] = [
    include_str!("../../../metadata/bitcoin.yaml"),
    include_str!("../../../metadata/evm.yaml"),
    include_str!("../../../metadata/hyperliquid_fills.yaml"),
    include_str!("../../../metadata/hyperliquid_replica_cmds.yaml"),
    include_str!("../../../metadata/solana.yaml"),
    include_str!("../../../metadata/substrate.yaml"),
    include_str!("../../../metadata/tron.yaml"),
];

/// The catalog this release ships under `name`.
pub(crate) fn shipped(name: &str) -> Option<&'static DatasetDescription> {
    static CATALOGS: OnceLock<Vec<DatasetDescription>> = OnceLock::new();
    CATALOGS
        .get_or_init(|| {
            SHIPPED
                .iter()
                .map(|yaml| crate::loader::parse_as_written(yaml).expect("a shipped catalog loads"))
                .collect()
        })
        .iter()
        .find(|catalog| catalog.name == name)
}

/// Render each column the way the catalog this release ships under the same
/// name does, where the two store it as the same type.
///
/// A description is often a copy kept beside the chunks it was written with,
/// and it does not change once they are stored. How a value renders belongs to
/// the engine, not to the chunk: without this, an encoding added to a shipped
/// catalog never reaches a copy made before it.
pub(crate) fn render_as_shipped(desc: &mut DatasetDescription) {
    let Some(shipped) = shipped(&desc.name) else {
        return;
    };

    for (table_name, table) in &mut desc.tables {
        let Some(shipped_table) = shipped.tables.get(table_name) else {
            continue;
        };

        for (column_name, column) in &mut table.columns {
            let Some(shipped_column) = shipped_table.columns.get(column_name) else {
                continue;
            };
            if shipped_column.data_type != column.data_type {
                continue;
            }

            column.encoding = shipped_column.encoding.clone();
            column.members = shipped_column.members.clone();
        }
    }
}
