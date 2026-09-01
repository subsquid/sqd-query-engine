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
        check_filter_surface(
            &format!("table '{}'", table_name),
            &table.filters,
            table_name,
            table,
        )?;

        // A special filter reaches its column the same way a declared filter
        // does, and a typo in one is just as invisible.
        for (filter_name, special) in &table.special_filters {
            let columns: Vec<&String> = match special {
                crate::metadata::SpecialFilter::Discriminator { columns } => {
                    columns.values().collect()
                }
                crate::metadata::SpecialFilter::BloomFilter { column, .. }
                | crate::metadata::SpecialFilter::RangeGte { column }
                | crate::metadata::SpecialFilter::RangeLte { column }
                | crate::metadata::SpecialFilter::ColumnAlias { column }
                | crate::metadata::SpecialFilter::GteConst { column, .. } => vec![column],
            };
            for column in columns {
                anyhow::ensure!(
                    table.columns.contains_key(column),
                    "table '{}': special filter '{}' targets column '{}', which is not there",
                    table_name,
                    filter_name,
                    column
                );
            }
        }

        // A relation naming a table that is not there does not fail: the scan
        // returns nothing for an unknown table and assembly skips the source, so
        // the relation comes back empty at 200. A mistyped key column is worse —
        // an unresolvable key makes the key set guaranteed-empty, so the relation
        // is empty rather than absent.
        for (relation_name, relation) in &table.relations {
            check_relation(
                &format!("table '{}'", table_name),
                relation_name,
                relation,
                table_name,
                table,
                desc,
            )?;
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

        // An alias is the one place the closed filter surface can be reopened, so
        // it is held to the table's rules, system columns included.
        check_filter_surface(
            &format!("alias '{}'", alias_name),
            &alias.filters,
            &alias.table,
            table,
        )?;

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

        // An implicit predicate is what makes an alias a *narrower* view of its
        // table. Naming a column that is not there widens it back to the whole
        // table without saying so.
        for column in alias.implicit_predicates.keys() {
            anyhow::ensure!(
                table.columns.contains_key(column),
                "alias '{}': implicit predicate on '{}', which '{}' does not have",
                alias_name,
                column,
                alias.table
            );
        }

        for (relation_name, relation) in &alias.relations {
            check_relation(
                &format!("alias '{}'", alias_name),
                relation_name,
                relation,
                &alias.table,
                table,
                desc,
            )?;
        }
    }

    Ok(())
}

/// Every name in a declared filter list must be a non-system column of `table`.
///
/// System columns — blooms, size counters, denormalised extractions — are the
/// engine's own, and publishing one as a filter makes an internal detail part of
/// the request API.
fn check_filter_surface(
    owner: &str,
    filters: &[String],
    table_name: &str,
    table: &crate::metadata::TableDescription,
) -> Result<()> {
    for filter in filters {
        let column = table.columns.get(filter).ok_or_else(|| {
            anyhow::anyhow!(
                "{}: filter '{}' not found in columns of '{}'",
                owner,
                filter,
                table_name
            )
        })?;

        anyhow::ensure!(
            !column.system,
            "{}: filter '{}' names a system column, which is not part of the public surface",
            owner,
            filter
        );
    }

    Ok(())
}

/// A relation must name a table the dataset has, and key columns both sides
/// actually carry: left keys in `source`, right keys in the target.
fn check_relation(
    owner: &str,
    relation_name: &str,
    relation: &crate::metadata::RelationDef,
    source_name: &str,
    source: &crate::metadata::TableDescription,
    desc: &DatasetDescription,
) -> Result<()> {
    let target = desc.tables.get(&relation.table).ok_or_else(|| {
        anyhow::anyhow!(
            "{}: relation '{}' targets table '{}', which the dataset does not have",
            owner,
            relation_name,
            relation.table
        )
    })?;

    for (side, keys, table_name, table) in [
        ("left", relation.effective_left_key(), source_name, source),
        (
            "right",
            relation.effective_right_key(),
            relation.table.as_str(),
            target,
        ),
    ] {
        for key in keys {
            anyhow::ensure!(
                table.columns.contains_key(key),
                "{}: relation '{}' joins on {} key '{}', which '{}' does not have",
                owner,
                relation_name,
                side,
                key,
                table_name
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
    filters: []
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
    filters: []
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
    filters: []
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
    filters: []
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
    filters: []
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
    filters: []
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
    filters: []
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
    filters: []
    columns:
      number: { type: uint64 }
query_aliases:
  view:
    table: no_such_table
    filters: []
"#;
        assert!(parse_dataset_description(bad_table).is_err());

        let bad_filter = r#"
name: test
tables:
  blocks:
    block_number_column: number
    filters: []
    columns:
      number: { type: uint64 }
  items:
    filters: []
    columns:
      block_number: { type: uint64 }
query_aliases:
  view:
    table: items
    filters: [ no_such_column ]
"#;
        let err = parse_dataset_description(bad_filter)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no_such_column"), "got: {err}");

        let bad_target = r#"
name: test
tables:
  blocks:
    block_number_column: number
    filters: []
    columns:
      number: { type: uint64 }
  items:
    filters: []
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
    filters: []
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
            (
                "an alias filter on a system column, which the table itself may not declare",
                "query_aliases:\n  view:\n    table: items\n    filters: [ user_bloom ]",
            ),
            (
                "an alias relation joining on a key the target does not have",
                "query_aliases:\n  view:\n    table: items\n    filters: []\n    relations:\n\
                 \x20     thing:\n        table: blocks\n        left_key: [ block_number ]\n\
                 \x20       right_key: [ no_such_column ]",
            ),
        ];

        for (what, defect) in rejected {
            assert!(
                parse_dataset_description(&catalog(defect)).is_err(),
                "{what} must be refused"
            );
        }
    }

    /// A table relation is validated like an alias one. A mistyped target table
    /// makes the relation come back empty at 200 — the scan returns nothing for a
    /// table it does not know and assembly skips the source. A mistyped key column
    /// is worse: the key set is then guaranteed-empty, so the relation is empty
    /// rather than absent.
    #[test]
    fn test_validate_rejects_broken_table_relations() {
        let catalog = |table: &str, right_key: &str| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    filters: []
    columns:
      number: {{ type: uint64 }}
  items:
    filters: []
    relations:
      kids:
        table: {table}
        left_key: [ block_number, index ]
        right_key: [ block_number, {right_key} ]
    columns:
      block_number: {{ type: uint64 }}
      index: {{ type: uint32 }}
  children:
    filters: []
    columns:
      block_number: {{ type: uint64 }}
      parent_index: {{ type: uint32 }}
"#
            )
        };

        parse_dataset_description(&catalog("children", "parent_index"))
            .expect("a relation naming real tables and columns must load");

        for (what, yaml) in [
            (
                "a relation target that is not a table",
                catalog("no_such_table", "parent_index"),
            ),
            (
                "a right key the target does not have",
                catalog("children", "no_such_column"),
            ),
        ] {
            assert!(
                parse_dataset_description(&yaml).is_err(),
                "{what} must be refused"
            );
        }
    }

    /// A table that omits `filters` accepts no filters at all and 400s every one
    /// a client sends, which `deny_unknown_fields` cannot catch — it sees an
    /// absent key, not a misspelled one.
    #[test]
    fn test_validate_rejects_a_table_without_a_filter_surface() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
"#;
        // The missing key is a serde error, so it arrives as the cause rather than
        // the context line.
        let err = format!("{:#}", parse_dataset_description(yaml).unwrap_err());
        assert!(err.contains("filters"), "got: {err}");
    }

    /// A filter naming a system column would publish an internal column as API.
    #[test]
    fn test_validate_rejects_a_filter_on_a_system_column() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    filters: []
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
    filters: []
    columns:
      number: { type: uint64 }
  items:
    filters: []
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
    filters: []
    columns:
      number: { type: uint64 }
  items:
    filters: []
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
        let mut loaded: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
                continue;
            }
            load_dataset_description(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            loaded.push(path.file_stem().unwrap().to_string_lossy().to_string());
        }
        loaded.sort();

        // Named rather than counted: a dataset appearing or disappearing is a
        // decision, and it should read as one here.
        assert_eq!(
            loaded,
            [
                "bitcoin",
                "evm",
                "hyperliquid_fills",
                "hyperliquid_replica_cmds",
                "solana",
                "substrate",
            ]
        );
    }
}
