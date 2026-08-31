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
        anyhow::ensure!(
            table.columns.contains_key(&table.block_number_column),
            "table '{}': block_number_column '{}' not found in columns",
            table_name,
            table.block_number_column
        );

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
        for (col_name, col) in &table.columns {
            if let Some(crate::metadata::WeightSource::Column(weight_col)) = &col.weight {
                anyhow::ensure!(
                    table.columns.contains_key(weight_col.as_str()),
                    "table '{}': weight column '{}' for '{}' not found in columns",
                    table_name,
                    weight_col,
                    col_name
                );
            }
        }

        // A `hex_number` column renders as a zero-padded hex string of its
        // physical width, which only means anything for an unsigned integer. The
        // encoder assumes this check exists.
        for (col_name, col) in &table.columns {
            if col.json_encoding == Some(crate::metadata::JsonEncoding::HexNumber) {
                anyhow::ensure!(
                    matches!(
                        col.data_type,
                        crate::metadata::ColumnType::UInt8
                            | crate::metadata::ColumnType::UInt16
                            | crate::metadata::ColumnType::UInt32
                            | crate::metadata::ColumnType::UInt64
                    ),
                    "table '{}': column '{}' declares json_encoding hex_number, \
                     which needs an unsigned integer column, not {:?}",
                    table_name,
                    col_name,
                    col.data_type
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

        // Validate the declared filter surface. A typo here does not fail a
        // query — it removes a filter, and the query it was meant to narrow
        // comes back wrong instead.
        for filter in &table.filters {
            anyhow::ensure!(
                table.columns.contains_key(filter),
                "table '{}': filter '{}' not found in columns",
                table_name,
                filter
            );
        }

        // Fork detection is off when nothing is declared, so a typo here would
        // turn it off silently.
        for (label, column) in [
            ("parent_hash_column", table.parent_hash_column.as_ref()),
            ("parent_number_column", table.parent_number_column.as_ref()),
        ] {
            if let Some(column) = column {
                anyhow::ensure!(
                    table.columns.contains_key(column),
                    "table '{}': {} '{}' not found in columns",
                    table_name,
                    label,
                    column
                );
            }
        }
    }

    for (alias_name, alias) in &desc.query_aliases {
        let table = desc.tables.get(&alias.table).ok_or_else(|| {
            anyhow::anyhow!(
                "alias '{}': table '{}' not found in dataset",
                alias_name,
                alias.table
            )
        })?;

        for filter in &alias.filters {
            anyhow::ensure!(
                table.columns.contains_key(filter),
                "alias '{}': filter '{}' not found in columns of '{}'",
                alias_name,
                filter,
                alias.table
            );
        }

        for (key, column) in &alias.filter_aliases {
            anyhow::ensure!(
                table.columns.contains_key(column),
                "alias '{}': filter '{}' targets column '{}', which '{}' does not have",
                alias_name,
                key,
                column,
                alias.table
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
        assert_eq!(blocks.block_number_column, "number");
        assert_eq!(blocks.sort_key, vec!["number"]);
        assert_eq!(blocks.stats_columns(), vec!["number"]);
    }

    #[test]
    fn test_default_block_number_column() {
        let yaml = r#"
name: test
tables:
  transactions:
    columns:
      block_number: { type: uint64 }
"#;
        let desc = parse_dataset_description(yaml).unwrap();
        let txs = desc.table("transactions").unwrap();
        assert_eq!(txs.block_number_column, "block_number");
    }

    #[test]
    fn test_column_json_encoding() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
      hash:
        type: string
        json_encoding: hex
      fee:
        type: uint64
        json_encoding: string
"#;
        let desc = parse_dataset_description(yaml).unwrap();
        let blocks = desc.table("blocks").unwrap();
        let hash = blocks.column("hash").unwrap();
        assert_eq!(hash.data_type, crate::metadata::ColumnType::String);
        assert_eq!(hash.json_encoding, Some(crate::metadata::JsonEncoding::Hex));
        let fee = blocks.column("fee").unwrap();
        assert_eq!(fee.data_type, crate::metadata::ColumnType::UInt64);
        assert_eq!(
            fee.json_encoding,
            Some(crate::metadata::JsonEncoding::String)
        );
        let number = blocks.column("number").unwrap();
        assert_eq!(number.json_encoding, None);
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
        assert_eq!(instructions.block_number_column, "block_number");
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
    block_number_column: number
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

    /// A typo in the filter surface does not fail a query — it removes a filter,
    /// and the query it was meant to narrow comes back wrong instead. It has to
    /// fail at load.
    #[test]
    fn test_validate_rejects_unknown_filter_column() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
  items:
    filters: [ no_such_column ]
    columns:
      block_number: { type: uint64 }
"#;
        let err = parse_dataset_description(yaml).unwrap_err().to_string();
        assert!(err.contains("no_such_column"), "got: {err}");
    }

    /// Fork detection is off when nothing is declared, so a typo would turn it
    /// off silently rather than loudly.
    #[test]
    fn test_validate_rejects_unknown_parent_columns() {
        for column in ["parent_hash_column", "parent_number_column"] {
            let yaml = format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    {column}: no_such_column
    columns:
      number: {{ type: uint64 }}
"#
            );
            let err = parse_dataset_description(&yaml).unwrap_err().to_string();
            assert!(err.contains("no_such_column"), "{column}: got: {err}");
        }
    }

    #[test]
    fn test_validate_rejects_broken_alias_references() {
        let bad_table = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
query_aliases:
  view:
    table: no_such_table
"#;
        assert!(parse_dataset_description(bad_table).is_err());

        let bad_filter = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
  items:
    columns:
      block_number: { type: uint64 }
query_aliases:
  view:
    table: items
    filters: [ no_such_column ]
"#;
        let err = parse_dataset_description(bad_filter).unwrap_err().to_string();
        assert!(err.contains("no_such_column"), "got: {err}");

        let bad_target = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
  items:
    columns:
      block_number: { type: uint64 }
query_aliases:
  view:
    table: items
    filters: []
    filter_aliases:
      topic0: no_such_column
"#;
        let err = parse_dataset_description(bad_target)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no_such_column"), "got: {err}");
    }

    /// A catalog is written by hand and read by nothing else. Each check below
    /// covers a mistake that would otherwise load clean and change an answer.
    #[test]
    fn test_validate_rejects_catalog_mistakes() {
        // `{defect}` is spliced into an otherwise valid two-table catalog.
        let catalog = |defect: &str| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: {{ type: uint64 }}
  items:
    columns:
      block_number: {{ type: uint64 }}
      user: {{ type: string }}
      user_bloom: {{ type: string, system: true }}
    filters: [ user ]
{defect}
"#
            )
        };

        let rejected: &[(&str, &str)] = &[
            (
                "an alias that omits its filter surface",
                "query_aliases:\n  view:\n    table: items",
            ),
            (
                "a misspelled alias key",
                "query_aliases:\n  view:\n    table: items\n    filters: []\n    filter: [ user ]",
            ),
            (
                "an implicit predicate on a column that is not there",
                "query_aliases:\n  view:\n    table: items\n    filters: []\n\
                 \x20   implicit_predicates:\n      no_such_column: [ x ]",
            ),
            (
                "an alias relation to a table that is not there",
                "query_aliases:\n  view:\n    table: items\n    filters: []\n    relations:\n\
                 \x20     thing:\n        table: no_such_table\n        left_key: [ block_number ]\n\
                 \x20       right_key: [ block_number ]",
            ),
        ];

        for (what, defect) in rejected {
            assert!(
                parse_dataset_description(&catalog(defect)).is_err(),
                "{what} must be refused"
            );
        }
    }

    /// A filter naming a system column would publish an internal column as API.
    #[test]
    fn test_validate_rejects_a_filter_on_a_system_column() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
  items:
    columns:
      block_number: { type: uint64 }
      user_bloom: { type: string, system: true }
    filters: [ user_bloom ]
"#;
        let err = parse_dataset_description(yaml).unwrap_err().to_string();
        assert!(err.contains("system column"), "got: {err}");
    }

    /// A special filter reaches a column the same way a declared filter does.
    #[test]
    fn test_validate_rejects_a_special_filter_on_a_missing_column() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
  items:
    columns:
      block_number: { type: uint64 }
    special_filters:
      callValueNonZero:
        type: gte_const
        column: no_such_column
        value: "0x1"
"#;
        let err = parse_dataset_description(yaml).unwrap_err().to_string();
        assert!(err.contains("no_such_column"), "got: {err}");
    }

    /// `hex_number` renders the column's physical width as hex digits, which only
    /// means anything for an unsigned integer. The encoder relies on this check.
    #[test]
    fn test_validate_rejects_hex_number_on_a_non_integer_column() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
  items:
    columns:
      block_number: { type: uint64 }
      label: { type: string, json_encoding: hex_number }
"#;
        let err = parse_dataset_description(yaml).unwrap_err().to_string();
        assert!(err.contains("hex_number"), "got: {err}");
    }

    /// Every catalog shipped with the engine must load.
    #[test]
    fn test_bundled_catalogs_validate() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("metadata");
        let mut loaded = 0;
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
                continue;
            }
            load_dataset_description(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            loaded += 1;
        }
        assert!(loaded >= 7, "expected the bundled catalogs, found {loaded}");
    }
}
