use crate::metadata::{DatasetDescription, MAX_DISCRIMINATOR_BYTES};
use anyhow::{Context, Result};
use std::collections::HashMap;
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

        // The output surface, closed the same way and for the same reason. A
        // name that resolves to nothing here is a field the catalog promises and
        // the engine then refuses.
        check_field_surface(table_name, table)?;

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

            // A discriminator dispatches on the byte length of the value it is
            // given, looking the column up by that length's decimal form. A key
            // that is not such a form — out of range, or `"01"`, which parses and
            // then never matches the `"1"` the lookup asks for — leaves its column
            // unreachable, and every request carrying a value of that length is
            // refused as having no column.
            if let crate::metadata::SpecialFilter::Discriminator { columns } = special {
                for length in columns.keys() {
                    let byte_count = length
                        .parse::<usize>()
                        .ok()
                        .filter(|n| (1..=MAX_DISCRIMINATOR_BYTES).contains(n));

                    anyhow::ensure!(
                        byte_count.is_some_and(|n| n.to_string() == *length),
                        "table '{}': special filter '{}' maps length '{}', which is not a \
                         byte count between 1 and {} written as the lookup asks for it",
                        table_name,
                        filter_name,
                        length,
                        MAX_DISCRIMINATOR_BYTES
                    );
                }
            }
        }

        // A hierarchical address is what `children` and `parents` walk, and the
        // scan reads it by this name.
        if let Some(column) = &table.address_column {
            anyhow::ensure!(
                table.columns.contains_key(column),
                "table '{}': address_column '{}' not found in columns",
                table_name,
                column
            );
        }

        for key in &table.parent_key {
            anyhow::ensure!(
                table.columns.contains_key(key),
                "table '{}': parent_key column '{}' not found in columns",
                table_name,
                key
            );
        }

        // A roll gathers several columns into one array and stops at the first
        // null. A name that resolves to nothing is not an error at query time —
        // it shortens the array, on every row, quietly.
        for (field_name, virtual_field) in &table.virtual_fields {
            let crate::metadata::VirtualField::Roll { columns } = virtual_field;
            for column in columns {
                anyhow::ensure!(
                    table.columns.contains_key(column),
                    "table '{}': virtual field '{}' rolls column '{}', which is not there",
                    table_name,
                    field_name,
                    column
                );
            }

            // Only a trailing list is spread into the array; anywhere earlier it
            // nests instead, and the field comes back a different shape than the
            // one it exists to present.
            let leading = columns.split_last().map_or(&[][..], |(_, rest)| rest);
            for column in leading {
                let is_list = table
                    .columns
                    .get(column)
                    .is_some_and(|c| c.data_type.is_list());

                anyhow::ensure!(
                    !is_list,
                    "table '{}': virtual field '{}' rolls list column '{}' before the last \
                     position; only a trailing list is spread",
                    table_name,
                    field_name,
                    column
                );
            }
        }

        // A field group is dispatched on its tag column's value. A typo in the
        // tag drops every variant field from every row; a typo in a mapping
        // drops one field from one variant.
        if let Some(groups) = &table.field_groups {
            anyhow::ensure!(
                table.columns.contains_key(&groups.tag_column),
                "table '{}': field group tag column '{}' not found in columns",
                table_name,
                groups.tag_column
            );

            for column in &groups.base_fields {
                anyhow::ensure!(
                    table.columns.contains_key(column),
                    "table '{}': field group base field '{}' not found in columns",
                    table_name,
                    column
                );
            }

            for (variant, variant_groups) in &groups.variants {
                for (group, mappings) in variant_groups {
                    for mapping in mappings {
                        anyhow::ensure!(
                            table.columns.contains_key(&mapping.column),
                            "table '{}': field group '{}.{}' maps column '{}', which is \
                             not there",
                            table_name,
                            variant,
                            group,
                            mapping.column
                        );
                    }
                }
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

    check_block_table(desc)?;
    check_names_are_unique(desc)?;

    Ok(())
}

/// A response is a sequence of blocks, so there has to be exactly one thing a
/// block is (INV-D3).
///
/// The engine finds it by its item key: a block is the row a block number alone
/// identifies. A second table of that shape would make which one it is depend on
/// catalog order; none at all leaves the engine looking for a table called
/// `blocks` that is not there, and every header in the response empty.
///
/// Identity is not read off the sort key. That is storage layout, which no
/// answer may depend on (INV-D8) — a block table rewritten under a different
/// sort key is the same block table.
fn check_block_table(desc: &DatasetDescription) -> Result<()> {
    let block_tables: Vec<&str> = desc
        .tables
        .iter()
        .filter(|(_, table)| table.is_block_table())
        .map(|(name, _)| name.as_str())
        .collect();

    anyhow::ensure!(
        block_tables.len() == 1,
        "dataset '{}': {} tables are identified by a block number alone ({:?}); \
         exactly one is the block table",
        desc.name,
        block_tables.len(),
        block_tables
    );

    let first = desc.tables.keys().next().map(String::as_str);
    anyhow::ensure!(
        first == Some(block_tables[0]),
        "dataset '{}': the block table is '{}' but '{}' is declared first; the block \
         table leads the catalog",
        desc.name,
        block_tables[0],
        first.unwrap_or("")
    );

    Ok(())
}

/// `query_name` is unique across tables and aliases, `field_name` across tables
/// (INV-D10). A duplicate makes a client's request ambiguous, and iteration
/// order — not the catalog — decides which table answers it.
///
/// A table with no `query_name` still holds one: a request may address a table
/// by its own name, so an undeclared name is claimed as surely as a declared
/// one, and another table declaring it shadows the first out of the request
/// surface entirely. `field_name` has no such default — a table that declares
/// none is simply not addressable in `fields` — so only declared ones are
/// claimed there.
fn check_names_are_unique(desc: &DatasetDescription) -> Result<()> {
    let mut query_names: HashMap<&str, &str> = HashMap::new();
    let mut field_names: HashMap<&str, &str> = HashMap::new();

    for (table_name, table) in &desc.tables {
        let query_name = table.query_name.as_deref().unwrap_or(table_name);
        claim(
            &desc.name,
            "queryName",
            query_name,
            table_name,
            &mut query_names,
        )?;

        if let Some(name) = &table.field_name {
            claim(&desc.name, "fieldName", name, table_name, &mut field_names)?;
        }
    }

    for alias_name in desc.query_aliases.keys() {
        claim(
            &desc.name,
            "queryName",
            alias_name,
            alias_name,
            &mut query_names,
        )?;
    }

    Ok(())
}

/// Record `owner` as the holder of `name`, refusing a name already held.
fn claim<'a>(
    dataset: &str,
    kind: &str,
    name: &'a str,
    owner: &'a str,
    seen: &mut HashMap<&'a str, &'a str>,
) -> Result<()> {
    if let Some(held_by) = seen.insert(name, owner) {
        anyhow::bail!(
            "dataset '{}': {} '{}' is claimed by both '{}' and '{}'",
            dataset,
            kind,
            name,
            held_by,
            owner
        );
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

/// Every name in a declared field list must resolve to something the table can
/// emit: a non-system column, a virtual field, or a field-group request key.
///
/// A table addressable in `fields` — one that declares a `field_name` — must
/// declare a list. An absent one reads as "nothing is selectable", which answers
/// every field a client asks of it with `UnknownField` and looks, from outside,
/// exactly like a dataset that carries no such columns.
fn check_field_surface(table_name: &str, table: &crate::metadata::TableDescription) -> Result<()> {
    anyhow::ensure!(
        table.field_name.is_none() || !table.fields.is_empty(),
        "table '{}': declares field_name '{}' but no output fields, so every \
         selection against it would be refused",
        table_name,
        table.field_name.as_deref().unwrap_or_default()
    );

    for field in &table.fields {
        if let Some(column) = table.columns.get(field) {
            anyhow::ensure!(
                !column.system,
                "table '{}': field '{}' names a system column, which is not part of the \
                 public surface",
                table_name,
                field
            );
            continue;
        }

        if let Some(crate::metadata::VirtualField::Roll { columns }) =
            table.virtual_fields.get(field)
        {
            for physical in columns {
                check_public_field_source(table_name, field, physical, table)?;
            }
            continue;
        }

        if let Some(physical) = table
            .field_groups
            .as_ref()
            .and_then(|fg| fg.physical_column_for_request(field))
        {
            check_public_field_source(table_name, field, physical, table)?;
            continue;
        }

        anyhow::bail!(
            "table '{}': field '{}' names no column, virtual field or field-group key",
            table_name,
            field
        );
    }

    Ok(())
}

/// A public field may rename or combine physical columns, but it must not expose
/// a column that the catalog marks as internal.
fn check_public_field_source(
    table_name: &str,
    field: &str,
    physical: &str,
    table: &crate::metadata::TableDescription,
) -> Result<()> {
    let column = table.columns.get(physical).ok_or_else(|| {
        anyhow::anyhow!(
            "table '{}': field '{}' resolves to missing column '{}'",
            table_name,
            field,
            physical
        )
    })?;

    anyhow::ensure!(
        !column.system,
        "table '{}': field '{}' resolves to system column '{}', which is not part of the \
         public surface",
        table_name,
        field,
        physical
    );

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

    let (left, right) = (
        relation.effective_left_key(),
        relation.effective_right_key(),
    );

    // An empty key is not "join on nothing" — every composite key is then the
    // same empty key, so every row of the target matches every source row.
    anyhow::ensure!(
        !left.is_empty() && !right.is_empty(),
        "{}: relation '{}' declares no join key, which matches every row of '{}'",
        owner,
        relation_name,
        relation.table
    );

    // The two sides are zipped column by column; a length mismatch panics the
    // query thread rather than failing the request.
    anyhow::ensure!(
        left.len() == right.len(),
        "{}: relation '{}' joins {} left keys against {} right keys",
        owner,
        relation_name,
        left.len(),
        right.len()
    );

    for (side, keys, table_name, table) in [
        ("left", left, source_name, source),
        ("right", right, relation.table.as_str(), target),
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

        // A relation is answered within one block: the scan is bounded by the
        // request's block range, and a key that does not start with the block
        // number matches rows in other blocks of the same chunk — which the
        // response presents as belonging to this one.
        let block_column = table.block_number_column.as_str();
        anyhow::ensure!(
            keys.first().map(String::as_str) == Some(block_column),
            "{}: relation '{}' starts its {} key with '{}' rather than '{}', so it can \
             join across blocks",
            owner,
            relation_name,
            side,
            keys.first().map(String::as_str).unwrap_or(""),
            block_column
        );
    }

    // `children` and `parents` walk a hierarchical address on both sides: the
    // source row's address is the prefix the target's is matched against.
    // Without one there is no hierarchy to walk, and the relation resolves to
    // nothing.
    if matches!(
        relation.kind,
        crate::metadata::RelationKind::Children | crate::metadata::RelationKind::Parents
    ) {
        for (side, table_name, table) in [
            ("source", source_name, source),
            ("target", relation.table.as_str(), target),
        ] {
            anyhow::ensure!(
                table.address_column.is_some(),
                "{}: relation '{}' is {:?}, but its {} '{}' declares no address column \
                 to walk",
                owner,
                relation_name,
                relation.kind,
                side,
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
    sort_key: [block_number]
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
    sort_key: [number]
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
    sort_key: [number]
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
    sort_key: [number]
    filters: []
    columns:
      number: {{ type: uint64 }}
  items:
    item_order_keys: [ index ]
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
    item_order_keys: [ parent_index ]
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

    /// Existence is not enough: the *shape* of a relation key decides whether the
    /// join means anything, and each shape below fails somewhere the response
    /// cannot show.
    #[test]
    fn test_validate_rejects_a_relation_that_cannot_join() {
        let catalog = |relation: &str| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    filters: []
    columns:
      number: {{ type: uint64 }}
  items:
    item_order_keys: [ index ]
    filters: []
    relations:
      kids:
{relation}
    columns:
      block_number: {{ type: uint64 }}
      index: {{ type: uint32 }}
  children:
    item_order_keys: [ parent_index ]
    filters: []
    columns:
      block_number: {{ type: uint64 }}
      parent_index: {{ type: uint32 }}
      address: {{ type: list_uint32 }}
"#
            )
        };

        let good = "        table: children\n\
                    \x20       left_key: [ block_number, index ]\n\
                    \x20       right_key: [ block_number, parent_index ]";
        parse_dataset_description(&catalog(good)).expect("a well-formed relation must load");

        let rejected: &[(&str, &str)] = &[
            (
                // Every composite key is then the same empty key.
                "a relation with no join key at all",
                "        table: children",
            ),
            (
                // The two sides are zipped, and the mismatch panics the scan.
                "a relation whose two sides are different lengths",
                "        table: children\n\
                 \x20       left_key: [ block_number, index ]\n\
                 \x20       right_key: [ block_number ]",
            ),
            (
                // Joins rows of one block onto another block's items.
                "a relation whose key does not start with the block number",
                "        table: children\n\
                 \x20       left_key: [ index, block_number ]\n\
                 \x20       right_key: [ parent_index, block_number ]",
            ),
            (
                // There is no hierarchy to walk, so it resolves to nothing.
                "a children relation onto a table with no address column",
                "        table: items\n        kind: children\n\
                 \x20       left_key: [ block_number, index ]\n\
                 \x20       right_key: [ block_number, index ]",
            ),
        ];

        for (what, relation) in rejected {
            assert!(
                parse_dataset_description(&catalog(relation)).is_err(),
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

    /// A catalog reference that resolves to nothing does not fail a query. It
    /// shortens an array, drops a variant's fields, or hides a discriminator
    /// column — on every row, quietly (INV-D1).
    #[test]
    fn test_validate_rejects_unresolvable_references() {
        const HEAD: &str = r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    filters: []
    columns:
      number: { type: uint64 }
  items:
    filters: []
"#;
        const TAIL: &str = r#"
    columns:
      block_number: { type: uint64 }
      seq: { type: uint32 }
      a0: { type: string }
      rest: { type: list_string }
      d1: { type: uint8 }
      kind: { type: string }
      payload: { type: string }
"#;
        let catalog = |stanza: &str| format!("{HEAD}{stanza}{TAIL}");

        const GOOD: &str = r#"
    address_column: seq
    parent_key: [ block_number ]
    virtual_fields:
      accounts: { type: roll, columns: [ a0, rest ] }
    special_filters:
      discriminator: { type: discriminator, columns: { "1": d1 } }
    field_groups:
      tag_column: kind
      base_fields: [ seq ]
      variants:
        call:
          action: [ { column: payload, field: payload } ]
"#;
        parse_dataset_description(&catalog(GOOD))
            .expect("a catalog whose every reference resolves must load");

        let rejected: &[(&str, &str)] = &[
            (
                "an address column that is not there",
                "    address_column: nope\n",
            ),
            (
                "a parent key column that is not there",
                "    parent_key: [ nope ]\n",
            ),
            (
                "a sort key column that is not there",
                "    sort_key: [ block_number, nope ]\n",
            ),
            (
                "an item order key that is not there",
                "    item_order_keys: [ nope ]\n",
            ),
            (
                "a roll over a column that is not there",
                "    virtual_fields:\n      accounts: { type: roll, columns: [ a0, nope ] }\n",
            ),
            (
                "a roll whose spread list is not its last column",
                "    virtual_fields:\n      accounts: { type: roll, columns: [ rest, a0 ] }\n",
            ),
            (
                "a discriminator length that is not a byte count",
                "    special_filters:\n      \
                 discriminator: { type: discriminator, columns: { d1: d1 } }\n",
            ),
            (
                "a discriminator length the lookup will never ask for",
                "    special_filters:\n      \
                 discriminator: { type: discriminator, columns: { \"01\": d1 } }\n",
            ),
            (
                "a discriminator length beyond the value cap",
                "    special_filters:\n      \
                 discriminator: { type: discriminator, columns: { \"17\": d1 } }\n",
            ),
            (
                "a field group tag column that is not there",
                "    field_groups:\n      tag_column: nope\n      variants: {}\n",
            ),
            (
                "a field group base field that is not there",
                "    field_groups:\n      tag_column: kind\n      \
                 base_fields: [ nope ]\n      variants: {}\n",
            ),
            (
                "a field group mapping a column that is not there",
                "    field_groups:\n      tag_column: kind\n      variants:\n        \
                 call:\n          action: [ { column: nope, field: nope } ]\n",
            ),
        ];

        for (what, stanza) in rejected {
            assert!(
                parse_dataset_description(&catalog(stanza)).is_err(),
                "{what} must be refused"
            );
        }

        // A weight source is declared on the column it charges, so it cannot be
        // spliced in above `columns:` like the rest.
        let weighed = catalog("").replace(
            "      payload: { type: string }",
            "      payload: { type: string, weight: nope }",
        );
        assert!(
            parse_dataset_description(&weighed).is_err(),
            "a weight column that is not there must be refused"
        );
    }

    /// Renaming or rolling a column does not make a system value public. The
    /// declared field surface must validate the physical source as well as the
    /// request key.
    #[test]
    fn test_validate_rejects_fields_backed_by_system_columns() {
        let catalog = |fields: &str| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    filters: []
    columns:
      number: {{ type: uint64 }}
  items:
    query_name: items
    field_name: item
    sort_key: [block_number, seq]
    item_order_keys: [seq]
    filters: []
    fields: [{fields}]
    virtual_fields:
      rolled: {{ type: roll, columns: [hidden] }}
    field_groups:
      tag_column: kind
      variants:
        call:
          action: [ {{ column: hidden, field: hidden, request: grouped }} ]
    columns:
      block_number: {{ type: uint64 }}
      seq: {{ type: uint32 }}
      kind: {{ type: string }}
      hidden: {{ type: string, system: true }}
"#
            )
        };

        for field in ["hidden", "rolled", "grouped"] {
            let err = parse_dataset_description(&catalog(field))
                .expect_err("a system-backed public field must be refused")
                .to_string();
            assert!(err.contains("system column"), "{field}: {err}");
        }
    }

    /// A response is a sequence of blocks, so there has to be exactly one thing a
    /// block is. The engine picks the first table of that shape; a second makes
    /// the choice depend on catalog order, and none leaves every header empty
    /// (INV-D3).
    ///
    /// The shape is the item key, not the sort key: what a block is cannot depend
    /// on how the chunk was written (INV-D8).
    #[test]
    fn test_validate_requires_exactly_one_block_table() {
        let catalog = |tables: &str| format!("name: test\ntables:\n{tables}");

        let blocks = "  blocks:\n                           block_number_column: number\n                           sort_key: [number]\n                           filters: []\n                           columns:\n                             number: { type: uint64 }\n";
        let items = "  items:\n                          block_number_column: block_number\n                          sort_key: [block_number, seq]\n                          item_order_keys: [seq]\n                          filters: []\n                          columns:\n                            block_number: { type: uint64 }\n                            seq: { type: uint32 }\n";

        parse_dataset_description(&catalog(&format!("{blocks}{items}")))
            .expect("one block table followed by an item table must load");

        assert!(
            parse_dataset_description(&catalog(items)).is_err(),
            "a dataset with no block table must be refused"
        );

        let second_block_table = "  epochs:\n                                       block_number_column: number\n                                       sort_key: [number]\n                                       filters: []\n                                       columns:\n                                         number: { type: uint64 }\n";
        assert!(
            parse_dataset_description(&catalog(&format!("{blocks}{second_block_table}"))).is_err(),
            "two tables of block shape must be refused"
        );

        assert!(
            parse_dataset_description(&catalog(&format!("{items}{blocks}"))).is_err(),
            "a block table that does not lead the catalog must be refused"
        );

        let unsorted_blocks = "  blocks:\n                                    block_number_column: number\n                                    sort_key: [hash, number]\n                                    filters: []\n                                    columns:\n                                      number: { type: uint64 }\n                                      hash: { type: string }\n";
        parse_dataset_description(&catalog(&format!("{unsorted_blocks}{items}")))
            .expect("storage order does not decide what a block is");

        // Its item key is `number ++ address`, so a block number alone does not
        // identify one of its rows and it is not a second block table.
        let addressed = "  traces:\n                             block_number_column: number\n                             address_column: address\n                             sort_key: [number]\n                             filters: []\n                             columns:\n                               number: { type: uint64 }\n                               address: { type: list_uint32 }\n";
        parse_dataset_description(&catalog(&format!("{blocks}{addressed}")))
            .expect("an addressed table with no order keys is not a block table");
    }

    /// A duplicate name makes a client's request ambiguous, and iteration order
    /// — not the catalog — decides which table answers it (INV-D10).
    #[test]
    fn test_validate_rejects_duplicate_names() {
        let catalog = |second: &str| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    filters: []
    columns:
      number: {{ type: uint64 }}
  items:
    query_name: items
    field_name: item
    sort_key: [block_number, seq]
    item_order_keys: [seq]
    filters: []
    fields: [seq]
    columns:
      block_number: {{ type: uint64 }}
      seq: {{ type: uint32 }}
  others:
{second}
    sort_key: [block_number, seq]
    item_order_keys: [seq]
    filters: []
    fields: [seq]
    columns:
      block_number: {{ type: uint64 }}
      seq: {{ type: uint32 }}
query_aliases:
  aliased:
    table: items
    filters: []
"#
            )
        };

        parse_dataset_description(&catalog("    query_name: others\n    field_name: other"))
            .expect("distinct names must load");

        for (what, second) in [
            (
                "two tables claiming one queryName",
                "    query_name: items\n    field_name: other",
            ),
            (
                "two tables claiming one fieldName",
                "    query_name: others\n    field_name: item",
            ),
            (
                "an alias claiming a table's queryName",
                "    query_name: aliased\n    field_name: other",
            ),
            (
                // `blocks` declares no `query_name`, so it holds its own name —
                // and a table declaring that name takes it, leaving the block
                // table unaddressable.
                "a table claiming another's undeclared queryName",
                "    query_name: blocks\n    field_name: other",
            ),
        ] {
            assert!(
                parse_dataset_description(&catalog(second)).is_err(),
                "{what} must be refused"
            );
        }
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
