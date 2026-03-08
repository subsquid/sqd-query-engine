use crate::metadata::DatasetDescription;
use anyhow::{Context, Result};
use std::path::Path;

/// Load a dataset description from a YAML file.
pub fn load_dataset_description(path: &Path) -> Result<DatasetDescription> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let desc: DatasetDescription =
        serde_yaml::from_str(&content).with_context(|| format!("parsing {}", path.display()))?;
    validate(&desc)?;
    Ok(desc)
}

/// Load a dataset description from a YAML string.
pub fn parse_dataset_description(yaml: &str) -> Result<DatasetDescription> {
    let desc: DatasetDescription =
        serde_yaml::from_str(yaml).context("parsing dataset description")?;
    validate(&desc)?;
    Ok(desc)
}

fn validate(desc: &DatasetDescription) -> Result<()> {
    for (table_name, table) in &desc.tables {
        // Validate block_number_column exists in columns
        if let Some(bn_col) = &table.block_number_column {
            anyhow::ensure!(
                table.columns.contains_key(bn_col),
                "table '{}': block_number_column '{}' not found in columns",
                table_name,
                bn_col
            );
        }

        // Validate item_order_keys exist in columns
        for key in &table.item_order_keys {
            anyhow::ensure!(
                table.columns.contains_key(key),
                "table '{}': item_order_key '{}' not found in columns",
                table_name,
                key
            );
        }

        // Validate sort_key columns exist
        for key in &table.sort_key {
            anyhow::ensure!(
                table.columns.contains_key(key),
                "table '{}': sort_key column '{}' not found in columns",
                table_name,
                key
            );
        }

        // Validate weight column references
        for (col, weight) in &table.weight_columns {
            anyhow::ensure!(
                table.columns.contains_key(col),
                "table '{}': weight_columns references unknown column '{}'",
                table_name,
                col
            );
            if let crate::metadata::WeightSource::Column(weight_col) = weight {
                anyhow::ensure!(
                    table.columns.contains_key(weight_col),
                    "table '{}': weight column '{}' for '{}' not found in columns",
                    table_name,
                    weight_col,
                    col
                );
            }
        }

        // Validate children references
        for child in &table.children {
            anyhow::ensure!(
                desc.tables.contains_key(child),
                "table '{}': child table '{}' not found in dataset",
                table_name,
                child
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number:
        type: uint64
        stats: true
      hash:
        type: string
"#;
        let desc = parse_dataset_description(yaml).unwrap();
        assert_eq!(desc.name, "test");
        assert_eq!(desc.tables.len(), 1);
        let blocks = desc.table("blocks").unwrap();
        assert_eq!(blocks.block_number_column.as_deref(), Some("number"));
        assert_eq!(blocks.sort_key, vec!["number"]);
        assert_eq!(blocks.stats_columns(), vec!["number"]);
    }

    #[test]
    fn test_validation_bad_block_number_column() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: nonexistent
    columns:
      number: { type: uint64 }
"#;
        let err = parse_dataset_description(yaml).unwrap_err();
        assert!(
            err.to_string().contains("nonexistent"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_load_solana_metadata() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("metadata/solana.yaml");
        let desc = load_dataset_description(&path).unwrap();
        assert_eq!(desc.name, "solana");
        assert_eq!(desc.tables.len(), 7);

        let instructions = desc.table("instructions").unwrap();
        assert_eq!(
            instructions.block_number_column.as_deref(),
            Some("block_number")
        );
        assert_eq!(
            instructions.sort_key,
            vec![
                "program_id",
                "d1",
                "b9",
                "block_number",
                "transaction_index"
            ]
        );
        assert_eq!(
            instructions.item_order_keys,
            vec!["transaction_index", "instruction_address"]
        );
        assert!(instructions.column("program_id").unwrap().stats);
        assert!(instructions.column("program_id").unwrap().dictionary);
        assert_eq!(
            instructions.column("d8").unwrap().data_type,
            crate::metadata::ColumnType::UInt64
        );
        assert_eq!(
            instructions.column("accounts_bloom").unwrap().data_type,
            crate::metadata::ColumnType::FixedBinary(64)
        );
    }

    #[test]
    fn test_load_evm_metadata() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("metadata/evm.yaml");
        let desc = load_dataset_description(&path).unwrap();
        assert_eq!(desc.name, "evm");
        assert_eq!(desc.tables.len(), 5);

        let txs = desc.table("transactions").unwrap();
        assert_eq!(
            txs.sort_key,
            vec!["sighash", "to", "block_number", "transaction_index"]
        );
        assert!(txs.column("sighash").unwrap().stats);
        assert!(txs.column("sighash").unwrap().dictionary);

        let logs = desc.table("logs").unwrap();
        assert_eq!(
            logs.sort_key,
            vec!["topic0", "address", "block_number", "log_index"]
        );
    }

    #[test]
    fn test_validation_bad_child_reference() {
        let yaml = r#"
name: test
tables:
  blocks:
    children: [missing_table]
    columns:
      number: { type: uint64 }
"#;
        let err = parse_dataset_description(yaml).unwrap_err();
        assert!(
            err.to_string().contains("missing_table"),
            "unexpected error: {}",
            err
        );
    }
}
