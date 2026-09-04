use crate::metadata::{DatasetDescription, SpecialFilter, MAX_DISCRIMINATOR_BYTES};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;

/// Load a dataset description from a YAML file.
pub fn load_dataset_description(path: &Path) -> Result<DatasetDescription> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    parse_dataset_description(&content).with_context(|| format!("parsing {}", path.display()))
}

/// Load a dataset description from a YAML string.
pub fn parse_dataset_description(yaml: &str) -> Result<DatasetDescription> {
    let desc: DatasetDescription =
        serde_yaml::from_str(yaml).context("parsing dataset description")?;
    check_stale_keys(yaml)?;
    validate(&desc)?;
    Ok(desc)
}

/// Refuse a key serde would drop in silence.
///
/// `special_filters` and `virtual_fields` hold internally tagged enums, the one
/// shape `deny_unknown_fields` cannot be applied to: serde buffers the entry,
/// takes the keys the variant declares and discards the rest. Every other part
/// of a catalog fails loudly on a stray key, and a catalog is written by hand,
/// so a filter left half-renamed would otherwise load and do nothing the author
/// asked for.
///
/// Reads the raw document rather than the parsed description, which by then has
/// forgotten the keys it dropped. Runs after the parse, so an unknown `kind` is
/// already refused and every entry here has a key list to check against.
fn check_stale_keys(yaml: &str) -> Result<()> {
    let doc: serde_yaml::Value = serde_yaml::from_str(yaml).context("re-reading the catalog")?;
    let mut trail = Vec::new();
    walk_tagged_entries(&doc, &mut trail)
}

/// Walk every mapping, checking the entries of the two blocks that hold a tagged
/// enum. `trail` is the path taken, for an error a reader can find in the file.
fn walk_tagged_entries<'a>(node: &'a serde_yaml::Value, trail: &mut Vec<&'a str>) -> Result<()> {
    let serde_yaml::Value::Mapping(map) = node else {
        return Ok(());
    };

    for (key, value) in map {
        let Some(key) = key.as_str() else { continue };

        let allowed = match key {
            "special_filters" => {
                Some(crate::metadata::SpecialFilter::allowed_keys as fn(&str) -> _)
            }
            "virtual_fields" => Some(crate::metadata::VirtualField::allowed_keys as fn(&str) -> _),
            _ => None,
        };

        trail.push(key);

        if let Some(allowed) = allowed {
            if let serde_yaml::Value::Mapping(entries) = value {
                for (name, entry) in entries {
                    let name = name.as_str().unwrap_or_default();
                    check_entry_keys(&trail.join("."), name, entry, allowed)?;
                }
            }
        }

        walk_tagged_entries(value, trail)?;
        trail.pop();
    }

    Ok(())
}

/// Every key of one tagged entry must be one its `kind` declares.
fn check_entry_keys(
    where_: &str,
    name: &str,
    entry: &serde_yaml::Value,
    allowed: fn(&str) -> Option<&'static [&'static str]>,
) -> Result<()> {
    let serde_yaml::Value::Mapping(fields) = entry else {
        return Ok(());
    };

    let kind = fields
        .get(serde_yaml::Value::from("kind"))
        .and_then(serde_yaml::Value::as_str)
        .unwrap_or_default();

    let Some(allowed) = allowed(kind) else {
        return Ok(());
    };

    for key in fields.keys().filter_map(serde_yaml::Value::as_str) {
        anyhow::ensure!(
            allowed.contains(&key),
            "{}: '{}' is a {} and carries no '{}'; it takes {:?}",
            where_,
            name,
            kind,
            key,
            allowed
        );
    }

    Ok(())
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
            if col.encoding == Some(crate::metadata::JsonEncoding::HexNumber) {
                anyhow::ensure!(
                    matches!(
                        col.data_type,
                        crate::metadata::ColumnType::UInt8
                            | crate::metadata::ColumnType::UInt16
                            | crate::metadata::ColumnType::UInt32
                            | crate::metadata::ColumnType::UInt64
                    ),
                    "table '{}': column '{}' declares encoding hex_number, \
                     which needs an unsigned integer column, not {:?}",
                    table_name,
                    col_name,
                    col.data_type
                );
            }
        }

        // A table with no `request` block accepts no filters and no relations,
        // and answers every one a client sends with a 400 that reads, from
        // outside, like a dataset missing those columns. `filters` is required
        // for that reason, and an absent block would step around the
        // requirement one level up: `deny_unknown_fields` no more sees a missing
        // `request:` than it saw a missing `filters:`. Only the block table has
        // nothing to say here.
        anyhow::ensure!(
            table.is_block_table() || table.request_surface.is_some(),
            "table '{}': no request block, so it would take no filters and no \
             relations; declare one, with 'filters: []' if that is the intent",
            table_name
        );

        // Validate the declared filter surface. A typo here does not fail a
        // query — it removes a filter, and the query it was meant to narrow
        // comes back wrong instead.
        let request = table.request();
        let special: Vec<&str> = request.special_filters.keys().map(String::as_str).collect();
        check_filter_surface(
            &format!("table '{}'", table_name),
            &request.filters,
            &special,
            table_name,
            table,
        )?;

        // The output surface, closed the same way and for the same reason. A
        // name that resolves to nothing here is a field the catalog promises and
        // the engine then refuses.
        check_field_surface(table_name, table)?;

        // A special filter reaches its column the same way a declared filter
        // does, and a typo in one is just as invisible.
        for (filter_name, special) in &request.special_filters {
            anyhow::ensure!(
                request.filters.contains(filter_name),
                "table '{}': special filter '{}' is not in its filter list, so no request \
                 can reach it",
                table_name,
                filter_name
            );

            let columns: Vec<&String> = match special {
                SpecialFilter::Discriminator { by_length } => by_length.values().collect(),
                SpecialFilter::Bloom { column, .. }
                | SpecialFilter::RangeGte { column }
                | SpecialFilter::RangeLte { column }
                | SpecialFilter::ColumnAlias { column }
                | SpecialFilter::GteConst { column, .. } => vec![column],
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

            // A bloom is probed as fixed-width bytes. Pointed at anything else
            // the probe has no array to read and takes the scan threads down
            // with it, so the type is checked here rather than discovered at
            // query time. The declared width is the archive writer's, and the
            // column's width is what the writer produced: a catalog where they
            // disagree describes a bloom nobody wrote.
            if let SpecialFilter::Bloom { column, bytes, .. } = special {
                let data_type = &table.columns[column].data_type;
                let crate::metadata::ColumnType::FixedBinary(width) = data_type else {
                    anyhow::bail!(
                        "table '{}': special filter '{}' probes column '{}' as a bloom, \
                         but it is {:?}, not fixed-size binary",
                        table_name,
                        filter_name,
                        column,
                        data_type
                    );
                };

                anyhow::ensure!(
                    width == bytes,
                    "table '{}': special filter '{}' declares a {}-byte bloom over a \
                     {}-byte column '{}'",
                    table_name,
                    filter_name,
                    bytes,
                    width,
                    column
                );
            }

            // A discriminator dispatches on the byte length of the value it is
            // given, looking the column up by that length's decimal form. A key
            // that is not such a form — out of range, or `"01"`, which parses and
            // then never matches the `"1"` the lookup asks for — leaves its column
            // unreachable, and every request carrying a value of that length is
            // refused as having no column.
            if let SpecialFilter::Discriminator { by_length } = special {
                for length in by_length.keys() {
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

        // A roll gathers several columns into one array and stops at the first
        // null. A name that resolves to nothing is not an error at query time —
        // it shortens the array, on every row, quietly.
        for (field_name, virtual_field) in &table.output.virtual_fields {
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

        // Variants are dispatched on the variant column's value. A typo in the
        // column drops every variant field from every row; a typo in a mapping
        // drops one field from one variant. Variants with no column to dispatch
        // on are never consulted, and a column with no variants dispatches
        // nothing — either is a catalog that says less than it looks like.
        let output = &table.output;
        let dispatches = output.variant_column.is_some();
        let has_variants = !output.variants.is_empty();
        anyhow::ensure!(
            dispatches == has_variants,
            "table '{}': variants and variant_column come together; one without the \
             other does nothing",
            table_name
        );

        if let Some(column) = &output.variant_column {
            anyhow::ensure!(
                table.columns.contains_key(column),
                "table '{}': variant_column '{}' not found in columns",
                table_name,
                column
            );
        }

        check_variant_mappings(table_name, table)?;

        // A relation naming a table that is not there does not fail: the scan
        // returns nothing for an unknown table and assembly skips the source, so
        // the relation comes back empty at 200. A mistyped key column is worse —
        // an unresolvable key makes the key set guaranteed-empty, so the relation
        // is empty rather than absent.
        for (relation_name, relation) in &request.relations {
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

    for (alias_name, alias) in &desc.aliases {
        let table = desc.tables.get(&alias.table).ok_or_else(|| {
            anyhow::anyhow!(
                "alias '{}': table '{}' not found in dataset",
                alias_name,
                alias.table
            )
        })?;

        // An alias is the one place the closed filter surface can be reopened, so
        // it is held to the table's rules, system columns included.
        let special: Vec<&str> = alias
            .special_filters
            .keys()
            .chain(table.request().special_filters.keys())
            .map(String::as_str)
            .collect();
        check_filter_surface(
            &format!("alias '{}'", alias_name),
            &alias.filters,
            &special,
            &alias.table,
            table,
        )?;

        for (key, special) in &alias.special_filters {
            anyhow::ensure!(
                alias.filters.contains(key),
                "alias '{}': special filter '{}' is not in its filter list, so no request \
                 can reach it",
                alias_name,
                key
            );

            // An item request carries the column an alias filter resolves to,
            // and the plan looks any other kind of special filter up on the
            // table — where an alias's own would not be found. This constrains
            // what an alias may *define*, not what it may reach: naming one of
            // the table's own special filters in `filters` reaches it, whatever
            // its kind.
            let SpecialFilter::ColumnAlias { column } = special else {
                anyhow::bail!(
                    "alias '{}': special filter '{}' is not a column_alias; an alias \
                     defines only renames of its own, and reaches every other kind \
                     through '{}'",
                    alias_name,
                    key,
                    alias.table
                );
            };
            anyhow::ensure!(
                table.columns.contains_key(column),
                "alias '{}': filter '{}' targets column '{}', which '{}' does not have",
                alias_name,
                key,
                column,
                alias.table
            );
        }

        // An implicit filter is what makes an alias a *narrower* view of its
        // table. Naming a column that is not there widens it back to the whole
        // table without saying so.
        for column in alias.implicit_filters.keys() {
            anyhow::ensure!(
                table.columns.contains_key(column),
                "alias '{}': implicit filter on '{}', which '{}' does not have",
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

/// A request name is unique across tables and aliases, an output name across
/// tables (INV-D10). A duplicate makes a client's request ambiguous, and
/// iteration order — not the catalog — decides which table answers it.
///
/// A table with no `request.name` still holds one: a request may address a
/// table by its own name, so an undeclared name is claimed as surely as a
/// declared one, and another table declaring it shadows the first out of the
/// request surface entirely. `output.name` has no such default — a table that
/// declares none is simply not addressable in `fields` — so only declared ones
/// are claimed there.
fn check_names_are_unique(desc: &DatasetDescription) -> Result<()> {
    let mut request_names: HashMap<&str, &str> = HashMap::new();
    let mut output_names: HashMap<&str, &str> = HashMap::new();

    for (table_name, table) in &desc.tables {
        claim(
            &desc.name,
            "request name",
            table.request_name(table_name),
            table_name,
            &mut request_names,
        )?;

        if let Some(name) = &table.output.name {
            claim(
                &desc.name,
                "output name",
                name,
                table_name,
                &mut output_names,
            )?;
        }
    }

    for alias_name in desc.aliases.keys() {
        claim(
            &desc.name,
            "request name",
            alias_name,
            alias_name,
            &mut request_names,
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

/// Every name in a declared filter list must be one of the owner's `special`
/// filters or a non-system column of `table`.
///
/// System columns — blooms, size counters, denormalised extractions — are the
/// engine's own, and publishing one as a filter makes an internal detail part of
/// the request API. A special filter is how such a column is reached on
/// purpose, under a name of the catalog's choosing.
fn check_filter_surface(
    owner: &str,
    filters: &[String],
    special: &[&str],
    table_name: &str,
    table: &crate::metadata::TableDescription,
) -> Result<()> {
    for filter in filters {
        if special.contains(&filter.as_str()) {
            continue;
        }

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
/// emit: a non-system column, a virtual field, or a variant mapping's field.
///
/// A table addressable in `fields` — one that declares an output name — must
/// declare a list. An absent one reads as "nothing is selectable", which answers
/// every field a client asks of it with `UnknownField` and looks, from outside,
/// exactly like a dataset that carries no such columns.
fn check_field_surface(table_name: &str, table: &crate::metadata::TableDescription) -> Result<()> {
    let output = &table.output;

    anyhow::ensure!(
        output.name.is_none() || !output.fields.is_empty(),
        "table '{}': declares output name '{}' but no output fields, so every \
         selection against it would be refused",
        table_name,
        output.name.as_deref().unwrap_or_default()
    );

    for field in &output.fields {
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
            output.virtual_fields.get(field)
        {
            for physical in columns {
                check_public_field_source(table_name, field, physical, table)?;
            }
            continue;
        }

        if let Some(physical) = table.variant_source(field) {
            check_public_field_source(table_name, field, physical, table)?;
            continue;
        }

        anyhow::bail!(
            "table '{}': field '{}' names no column, virtual field or variant field",
            table_name,
            field
        );
    }

    Ok(())
}

/// The mappings that nest a variant's fields, checked for the four ways a
/// plausible one changes an answer without failing anything.
///
/// A mapping says three things: the column it reads, the `output.fields` key
/// that selects it, and the name it renders under. Each of the three can be
/// written so that the catalog means one thing and the engine does another.
fn check_variant_mappings(
    table_name: &str,
    table: &crate::metadata::TableDescription,
) -> Result<()> {
    // Columns that say which row this is. A mapping over one of them moves it
    // out of the top level for every row and off the rows of every variant that
    // does not repeat it — the field vanishes from the shapes that need it most.
    let mut identity: Vec<&str> = vec![table.block_number_column.as_str()];
    identity.extend(table.item_order_keys.iter().map(String::as_str));
    identity.extend(table.address_column.as_deref());
    identity.extend(table.output.variant_column.as_deref());

    // field key → the column it reads, to catch two mappings answering to one
    // name with two different answers.
    let mut source_of: HashMap<&str, &str> = HashMap::new();

    for (variant, groups) in &table.output.variants {
        for (group, mappings) in groups {
            let mut rendered: HashMap<&str, &str> = HashMap::new();

            for mapping in mappings {
                anyhow::ensure!(
                    table.columns.contains_key(&mapping.column),
                    "table '{}': variant '{}.{}' maps column '{}', which is not there",
                    table_name,
                    variant,
                    group,
                    mapping.column
                );

                let field = mapping.field();

                anyhow::ensure!(
                    !identity.contains(&field),
                    "table '{}': variant '{}.{}' maps '{}', which identifies a row and so \
                     is written flat for every one of them",
                    table_name,
                    variant,
                    group,
                    field
                );

                // A field key that renames its column must not be a column of
                // its own. `physical_output_column` answers from the column list
                // before it consults the mappings, and the row writer resolves
                // the mappings first: a key that is both projects one column and
                // writes another, and the field ships empty.
                anyhow::ensure!(
                    field == mapping.column || !table.columns.contains_key(field),
                    "table '{}': variant '{}.{}' selects column '{}' under the name of \
                     column '{}'; a field key either is its own column or is not a \
                     column at all",
                    table_name,
                    variant,
                    group,
                    mapping.column,
                    field
                );

                if let Some(other) = source_of.insert(field, mapping.column.as_str()) {
                    anyhow::ensure!(
                        other == mapping.column,
                        "table '{}': field '{}' is mapped to both '{}' and '{}'; one field \
                         key reads one column",
                        table_name,
                        field,
                        other,
                        mapping.column
                    );
                }

                if let Some(other) = rendered.insert(mapping.json_name.as_str(), field) {
                    anyhow::bail!(
                        "table '{}': variant '{}.{}' renders both '{}' and '{}' as '{}', \
                         which would repeat the key in one object",
                        table_name,
                        variant,
                        group,
                        other,
                        field,
                        mapping.json_name
                    );
                }
            }
        }
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
    columns:
      number:
        type: uint64
      hash:
        type: string
"#;
        let desc = parse_dataset_description(yaml).unwrap();
        assert_eq!(desc.name, "test");
        assert_eq!(desc.tables.len(), 1);
        let blocks = desc.table("blocks").unwrap();
        assert_eq!(blocks.block_number_column, "number");
        assert_eq!(blocks.sort_key, vec!["number"]);
        assert_eq!(blocks.request_name("blocks"), "blocks");
        assert!(blocks.output.name.is_none());
    }

    #[test]
    fn test_default_block_number_column() {
        let yaml = r#"
name: test
tables:
  transactions:
    sort_key: [block_number]
    columns:
      block_number: { type: uint64 }
"#;
        let desc = parse_dataset_description(yaml).unwrap();
        let txs = desc.table("transactions").unwrap();
        assert_eq!(txs.block_number_column, "block_number");
    }

    #[test]
    fn test_column_encoding() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
      hash:
        type: string
        encoding: hex_bytes
      fee:
        type: uint64
        encoding: decimal_string
"#;
        let desc = parse_dataset_description(yaml).unwrap();
        let blocks = desc.table("blocks").unwrap();
        let hash = blocks.column("hash").unwrap();
        assert_eq!(hash.data_type, crate::metadata::ColumnType::String);
        assert_eq!(hash.encoding, Some(crate::metadata::JsonEncoding::HexBytes));
        let fee = blocks.column("fee").unwrap();
        assert_eq!(fee.data_type, crate::metadata::ColumnType::UInt64);
        assert_eq!(
            fee.encoding,
            Some(crate::metadata::JsonEncoding::DecimalString)
        );
        let number = blocks.column("number").unwrap();
        assert_eq!(number.encoding, None);
    }

    /// Covers CT-1 · INV-D1
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
        assert_eq!(instructions.request_name("instructions"), "instructions");
        assert_eq!(instructions.output.name.as_deref(), Some("instruction"));
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

        let logs = desc.table("logs").unwrap();
        assert_eq!(
            logs.sort_key,
            vec!["topic0", "address", "block_number", "log_index"]
        );
    }

    /// A typo in the filter surface does not fail a query — it removes a filter,
    /// and the query it was meant to narrow comes back wrong instead. It has to
    /// fail at load.
    ///
    /// Covers CT-1 · INV-D1
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
    request:
      filters: [ no_such_column ]
    columns:
      block_number: { type: uint64 }
"#;
        let err = parse_dataset_description(yaml).unwrap_err().to_string();
        assert!(err.contains("no_such_column"), "got: {err}");
    }

    /// Fork detection is off when nothing is declared, so a typo would turn it
    /// off silently rather than loudly.
    ///
    /// Covers CT-1 · INV-D1
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

    /// Covers CT-1 · INV-D1, INV-D2
    #[test]
    fn test_validate_rejects_broken_alias_references() {
        let bad_table = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
aliases:
  view:
    table: no_such_table
    filters: []
"#;
        assert!(parse_dataset_description(bad_table).is_err());

        // `{alias}` is spliced in as the body of one alias over `items`.
        let catalog = |alias: &str| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: {{ type: uint64 }}
  items:
    request:
      filters: []
    columns:
      block_number: {{ type: uint64 }}
      topic: {{ type: string }}
aliases:
  view:
    table: items
{alias}
"#
            )
        };

        let bad_filter = catalog("    filters: [ no_such_column ]");
        let err = parse_dataset_description(&bad_filter)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no_such_column"), "got: {err}");

        let bad_target = catalog(
            "    filters: [ topic0 ]\n    special_filters:\n      \
             topic0: { kind: column_alias, column: no_such_column }",
        );
        let err = parse_dataset_description(&bad_target)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no_such_column"), "got: {err}");

        let unlisted = catalog(
            "    filters: []\n    special_filters:\n      \
             topic0: { kind: column_alias, column: topic }",
        );
        let err = parse_dataset_description(&unlisted)
            .unwrap_err()
            .to_string();
        assert!(err.contains("topic0"), "got: {err}");

        // A request carries the column an alias filter resolves to, and the plan
        // looks any other kind up on the table — where the alias's own is not.
        let not_a_rename = catalog(
            "    filters: [ topic0 ]\n    special_filters:\n      \
             topic0: { kind: range_gte, column: topic }",
        );
        let err = parse_dataset_description(&not_a_rename)
            .unwrap_err()
            .to_string();
        assert!(err.contains("column_alias"), "got: {err}");
    }

    /// A table that declares no request surface takes no filters and no
    /// relations, and refuses every one a client sends — which reads from
    /// outside like a dataset without those columns. Only the block table, whose
    /// rows come with every response, has nothing to say here (INV-D1).
    ///
    /// Covers CT-1 · INV-D1
    #[test]
    fn test_validate_requires_a_request_surface() {
        let catalog = |request: &str| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: {{ type: uint64 }}
  items:
{request}    item_order_keys: [ seq ]
    columns:
      block_number: {{ type: uint64 }}
      seq: {{ type: uint32 }}
"#
            )
        };

        parse_dataset_description(&catalog("    request:\n      filters: []\n"))
            .expect("an item table that declares an empty surface on purpose must load");

        let err = format!(
            "{:#}",
            parse_dataset_description(&catalog(""))
                .expect_err("a surface left out must be refused")
        );
        assert!(err.contains("no request block"), "got: {err}");
    }

    /// The three names in a field mapping — the column, the key that selects it
    /// and the key it renders under — can each be written so that the catalog
    /// means one thing and the engine does another, without failing anything
    /// (INV-D1).
    ///
    /// Covers CT-1 · INV-D1
    #[test]
    fn test_validate_rejects_variant_mapping_mistakes() {
        let catalog = |variants: &str| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: {{ type: uint64 }}
  items:
    request:
      filters: []
    output:
      name: item
      fields: [ seq, kind, payload ]
      variant_column: kind
      variants:
{variants}
    item_order_keys: [ seq ]
    columns:
      block_number: {{ type: uint64 }}
      seq: {{ type: uint32 }}
      kind: {{ type: string }}
      payload: {{ type: string }}
      other: {{ type: string }}
"#
            )
        };

        parse_dataset_description(&catalog(
            "        call:\n          action: [ { column: payload, as: payload } ]",
        ))
        .expect("a mapping that renames nothing must load");

        let rejected: &[(&str, &str, &str)] = &[
            (
                "a mapping over the column that says which variant a row is",
                "        call:\n          action: [ { column: kind, as: kindOfRow } ]",
                "identifies a row",
            ),
            (
                "a mapping over an item order key",
                "        call:\n          action: [ { column: seq, as: seq } ]",
                "identifies a row",
            ),
            (
                "a mapping over the block number",
                "        call:\n          action: [ { column: block_number, as: blockNumber } ]",
                "identifies a row",
            ),
            (
                "a field key that is the name of another column",
                "        call:\n          action: [ { column: payload, field_key: other, as: p } ]",
                "is not a column at all",
            ),
            (
                "one field key reading two columns",
                "        call:\n          action: [ { column: payload, field_key: v, as: p } ]\n\
                 \x20       create:\n          action: [ { column: other, field_key: v, as: p } ]",
                "one field key reads one column",
            ),
            (
                "two mappings rendering under one key",
                "        call:\n          action: [ { column: payload, as: p }, \
                 { column: other, as: p } ]",
                "repeat the key in one object",
            ),
        ];

        for (what, variants, reason) in rejected {
            let err = format!(
                "{:#}",
                parse_dataset_description(&catalog(variants)).expect_err(what)
            );
            assert!(
                err.contains(reason),
                "{what}: wanted {reason:?}, got: {err}"
            );
        }
    }

    /// A bloom is probed as fixed-width bytes. Pointed at anything else the probe
    /// has no array to read and takes the scan threads down with it, and a width
    /// that is not the column's describes a bloom nobody wrote (INV-D1).
    ///
    /// Covers CT-1 · INV-D1
    #[test]
    fn test_validate_rejects_a_bloom_that_does_not_match_its_column() {
        let catalog = |column: &str, bytes: usize| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: {{ type: uint64 }}
  items:
    request:
      filters: [ mentions ]
      special_filters:
        mentions: {{ kind: bloom, column: {column}, bytes: {bytes}, hashes: 7 }}
    item_order_keys: [ seq ]
    columns:
      block_number: {{ type: uint64 }}
      seq: {{ type: uint32 }}
      label: {{ type: string, system: true }}
      accounts_bloom: {{ type: fixed_binary_64, system: true }}
"#
            )
        };

        parse_dataset_description(&catalog("accounts_bloom", 64))
            .expect("a bloom over its own column at its own width must load");

        let err = format!(
            "{:#}",
            parse_dataset_description(&catalog("label", 64)).expect_err("a bloom over a string")
        );
        assert!(err.contains("not fixed-size binary"), "got: {err}");

        let err = format!(
            "{:#}",
            parse_dataset_description(&catalog("accounts_bloom", 8))
                .expect_err("a bloom at a width the column does not have")
        );
        assert!(err.contains("8-byte bloom over a 64-byte"), "got: {err}");
    }

    /// `special_filters` and `virtual_fields` hold internally tagged enums, the
    /// one shape serde cannot apply `deny_unknown_fields` to: a key it does not
    /// know is buffered and dropped. A catalog left half-renamed would otherwise
    /// load and do nothing the author asked for (INV-D1).
    ///
    /// Covers CT-1 · INV-D1
    #[test]
    fn test_validate_rejects_stale_keys_in_tagged_blocks() {
        let catalog = |bloom: &str, roll: &str| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: {{ type: uint64 }}
  items:
    request:
      filters: [ mentions ]
      special_filters:
        mentions: {{ kind: bloom, column: accounts_bloom, bytes: 64, hashes: 7{bloom} }}
    output:
      name: item
      fields: [ topics ]
      virtual_fields:
        topics: {{ kind: roll, columns: [ topic0 ]{roll} }}
    item_order_keys: [ seq ]
    columns:
      block_number: {{ type: uint64 }}
      seq: {{ type: uint32 }}
      topic0: {{ type: string }}
      accounts_bloom: {{ type: fixed_binary_64, system: true }}
"#
            )
        };

        parse_dataset_description(&catalog("", "")).expect("the catalog without stale keys loads");

        // The spelling `hashes` replaced. Serde takes `hashes`, drops this, and
        // the author believes the edit took effect.
        let err = format!(
            "{:#}",
            parse_dataset_description(&catalog(", num_hashes: 3", ""))
                .expect_err("a stale special-filter key")
        );
        assert!(err.contains("num_hashes"), "got: {err}");

        let err = format!(
            "{:#}",
            parse_dataset_description(&catalog("", ", type: roll"))
                .expect_err("a stale virtual-field key")
        );
        assert!(err.contains("'topics'"), "got: {err}");
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
    columns:
      number: {{ type: uint64 }}
  items:
    request:
      filters: [ user ]
    item_order_keys: [ seq ]
    columns:
      block_number: {{ type: uint64 }}
      seq: {{ type: uint32 }}
      user: {{ type: string }}
      user_bloom: {{ type: string, system: true }}
{defect}
"#
            )
        };

        // Without this the whole table below proves nothing: a base catalog that
        // is itself refused makes every case pass on the base's own defect,
        // whatever the spliced stanza says.
        parse_dataset_description(&catalog(""))
            .expect("the catalog the defects are spliced into must itself be valid");

        let rejected: &[(&str, &str, &str)] = &[
            (
                "an alias that omits its filter surface",
                "aliases:\n  view:\n    table: items",
                "missing field `filters`",
            ),
            (
                "a misspelled alias key",
                "aliases:\n  view:\n    table: items\n    filters: []\n    filter: [ user ]",
                "unknown field `filter`",
            ),
            (
                "an implicit filter on a column that is not there",
                "aliases:\n  view:\n    table: items\n    filters: []\n\
                 \x20   implicit_filters:\n      no_such_column: [ x ]",
                "implicit filter on 'no_such_column'",
            ),
            (
                "an alias relation to a table that is not there",
                "aliases:\n  view:\n    table: items\n    filters: []\n    relations:\n\
                 \x20     thing:\n        table: no_such_table\n        left_key: [ block_number ]\n\
                 \x20       right_key: [ block_number ]",
                "targets table 'no_such_table'",
            ),
            (
                "an alias filter on a system column, which the table itself may not declare",
                "aliases:\n  view:\n    table: items\n    filters: [ user_bloom ]",
                "system column",
            ),
            (
                "an alias relation joining on a key the target does not have",
                "aliases:\n  view:\n    table: items\n    filters: []\n    relations:\n\
                 \x20     thing:\n        table: blocks\n        left_key: [ block_number ]\n\
                 \x20       right_key: [ no_such_column ]",
                "'no_such_column'",
            ),
        ];

        for (what, defect, reason) in rejected {
            // `{:#}` so a serde error is read through the context anyhow wraps it in.
            let err = format!(
                "{:#}",
                parse_dataset_description(&catalog(defect)).expect_err(what)
            );
            assert!(
                err.contains(reason),
                "{what}: wanted {reason:?}, got: {err}"
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
    columns:
      number: {{ type: uint64 }}
  items:
    request:
      filters: []
      relations:
        kids:
          table: {table}
          left_key: [ block_number, index ]
          right_key: [ block_number, {right_key} ]
    item_order_keys: [ index ]
    columns:
      block_number: {{ type: uint64 }}
      index: {{ type: uint32 }}
  children:
    request:
      filters: []
    item_order_keys: [ parent_index ]
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
    ///
    /// Covers CT-1 · INV-D5, INV-D6
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
    columns:
      number: {{ type: uint64 }}
  items:
    request:
      filters: []
      relations:
        kids:
{relation}
    item_order_keys: [ index ]
    columns:
      block_number: {{ type: uint64 }}
      index: {{ type: uint32 }}
  children:
    request:
      filters: []
    item_order_keys: [ parent_index ]
    columns:
      block_number: {{ type: uint64 }}
      parent_index: {{ type: uint32 }}
      address: {{ type: list_uint32 }}
"#
            )
        };

        let good = "          table: children\n\
                    \x20         left_key: [ block_number, index ]\n\
                    \x20         right_key: [ block_number, parent_index ]";
        parse_dataset_description(&catalog(good)).expect("a well-formed relation must load");

        let rejected: &[(&str, &str)] = &[
            (
                // Every composite key is then the same empty key.
                "a relation with no join key at all",
                "          table: children",
            ),
            (
                // The two sides are zipped, and the mismatch panics the scan.
                "a relation whose two sides are different lengths",
                "          table: children\n\
                 \x20         left_key: [ block_number, index ]\n\
                 \x20         right_key: [ block_number ]",
            ),
            (
                // Joins rows of one block onto another block's items.
                "a relation whose key does not start with the block number",
                "          table: children\n\
                 \x20         left_key: [ index, block_number ]\n\
                 \x20         right_key: [ parent_index, block_number ]",
            ),
            (
                // There is no hierarchy to walk, so it resolves to nothing.
                "a children relation onto a table with no address column",
                "          table: items\n          kind: children\n\
                 \x20         left_key: [ block_number, index ]\n\
                 \x20         right_key: [ block_number, index ]",
            ),
        ];

        for (what, relation) in rejected {
            assert!(
                parse_dataset_description(&catalog(relation)).is_err(),
                "{what} must be refused"
            );
        }
    }

    /// A request block that omits `filters` accepts no filters at all and 400s
    /// every one a client sends, which `deny_unknown_fields` cannot catch — it
    /// sees an absent key, not a misspelled one.
    #[test]
    fn test_validate_rejects_a_request_without_a_filter_surface() {
        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    columns:
      number: { type: uint64 }
  items:
    request:
      name: items
    columns:
      block_number: { type: uint64 }
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
    columns:
      number: { type: uint64 }
  items:
    request:
      filters: [ user_bloom ]
    columns:
      block_number: { type: uint64 }
      user_bloom: { type: string, system: true }
"#;
        let err = parse_dataset_description(yaml).unwrap_err().to_string();
        assert!(err.contains("system column"), "got: {err}");
    }

    /// A special filter reaches a column the same way a declared filter does.
    ///
    /// Covers CT-1 · INV-D1
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
    request:
      filters: [ call_value_non_zero ]
      special_filters:
        call_value_non_zero:
          kind: gte_const
          column: no_such_column
          value: "0x1"
    columns:
      block_number: { type: uint64 }
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
    request:
      filters: []
    columns:
      block_number: { type: uint64 }
      label: { type: string, encoding: hex_number }
"#;
        let err = parse_dataset_description(yaml).unwrap_err().to_string();
        assert!(err.contains("hex_number"), "got: {err}");
    }

    /// A catalog reference that resolves to nothing does not fail a query. It
    /// shortens an array, drops a variant's fields, or hides a discriminator
    /// column — on every row, quietly (INV-D1).
    ///
    /// Covers CT-1 · INV-D1
    #[test]
    fn test_validate_rejects_unresolvable_references() {
        const HEAD: &str = r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
  items:
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
        // A stanza that declares a special filter carries the filter list that
        // reaches it, so the list is never what a stanza fails on.
        let catalog = |stanza: &str| format!("{HEAD}{stanza}{TAIL}");

        const GOOD: &str = r#"
    request:
      filters: [ discriminator ]
      special_filters:
        discriminator: { kind: discriminator, by_length: { "1": d1 } }
    output:
      virtual_fields:
        accounts: { kind: roll, columns: [ a0, rest ] }
      variant_column: kind
      variants:
        call:
          action: [ { column: payload, as: payload } ]
    address_column: seq
"#;
        parse_dataset_description(&catalog(GOOD))
            .expect("a catalog whose every reference resolves must load");

        // Each case carries the reason it must be refused *for*. Asserting only
        // that the load failed would let a case pass on some unrelated defect of
        // the scaffolding — and the scaffolding grows a check under it every
        // time the validator does.
        let rejected: &[(&str, &str, &str)] = &[
            (
                "an address column that is not there",
                "    request:\n      filters: []\n    address_column: nope\n",
                "address_column 'nope'",
            ),
            (
                "a sort key column that is not there",
                "    sort_key: [ block_number, nope ]\n",
                "sort_key column 'nope'",
            ),
            (
                "an item order key that is not there",
                "    item_order_keys: [ nope ]\n",
                "item_order_key 'nope'",
            ),
            (
                "a roll over a column that is not there",
                "    output:\n      virtual_fields:\n        \
                 accounts: { kind: roll, columns: [ a0, nope ] }\n",
                "rolls column 'nope'",
            ),
            (
                "a roll whose spread list is not its last column",
                "    output:\n      virtual_fields:\n        \
                 accounts: { kind: roll, columns: [ rest, a0 ] }\n",
                "rolls list column 'rest'",
            ),
            (
                "a discriminator length that is not a byte count",
                "    request:\n      filters: [ discriminator ]\n      special_filters:\n        \
                 discriminator: { kind: discriminator, by_length: { d1: d1 } }\n",
                "maps length 'd1'",
            ),
            (
                "a discriminator length the lookup will never ask for",
                "    request:\n      filters: [ discriminator ]\n      special_filters:\n        \
                 discriminator: { kind: discriminator, by_length: { \"01\": d1 } }\n",
                "maps length '01'",
            ),
            (
                "a discriminator length beyond the value cap",
                "    request:\n      filters: [ discriminator ]\n      special_filters:\n        \
                 discriminator: { kind: discriminator, by_length: { \"17\": d1 } }\n",
                "maps length '17'",
            ),
            (
                "a special filter the filter list does not reach",
                "    request:\n      filters: []\n      special_filters:\n        \
                 discriminator: { kind: discriminator, by_length: { \"1\": d1 } }\n",
                "is not in its filter list",
            ),
            (
                "a variant column that is not there",
                "    output:\n      variant_column: nope\n      variants:\n        \
                 call:\n          action: [ { column: payload, as: payload } ]\n",
                "variant_column 'nope'",
            ),
            (
                "a variant mapping a column that is not there",
                "    output:\n      variant_column: kind\n      variants:\n        \
                 call:\n          action: [ { column: nope, as: nope } ]\n",
                "maps column 'nope'",
            ),
            (
                "variants with no column to dispatch on",
                "    output:\n      variants:\n        \
                 call:\n          action: [ { column: payload, as: payload } ]\n",
                "come together",
            ),
            (
                "a variant column with nothing to dispatch",
                "    output:\n      variant_column: kind\n",
                "come together",
            ),
        ];

        for (what, stanza, reason) in rejected {
            // `{:#}` so a serde error is read through the context anyhow wraps it in.
            let err = format!(
                "{:#}",
                parse_dataset_description(&catalog(stanza)).expect_err(what)
            );
            assert!(
                err.contains(reason),
                "{what}: wanted {reason:?}, got: {err}"
            );
        }

        // A weight source is declared on the column it charges, so it cannot be
        // spliced in above `columns:` like the rest. It goes onto the stanza that
        // loads, so the only thing wrong with the catalog is the weight.
        let weighed = catalog(GOOD).replace(
            "      payload: { type: string }",
            "      payload: { type: string, weight: nope }",
        );
        let err = format!(
            "{:#}",
            parse_dataset_description(&weighed).expect_err("a weight column that is not there")
        );
        assert!(err.contains("weight column 'nope'"), "got: {err}");
    }

    /// Renaming or rolling a column does not make a system value public. The
    /// declared field surface must validate the physical source as well as the
    /// name a selection uses.
    ///
    /// Covers CT-1 · INV-D9
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
    columns:
      number: {{ type: uint64 }}
  items:
    request:
      name: items
      filters: []
    output:
      name: item
      fields: [{fields}]
      virtual_fields:
        rolled: {{ kind: roll, columns: [hidden] }}
      variant_column: kind
      variants:
        call:
          action: [ {{ column: hidden, field_key: grouped, as: hidden }} ]
    sort_key: [block_number, seq]
    item_order_keys: [seq]
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
    ///
    /// Covers CT-1 · INV-D3
    #[test]
    fn test_validate_requires_exactly_one_block_table() {
        let catalog = |tables: &str| format!("name: test\ntables:\n{tables}");

        let blocks = "  blocks:\n\
                      \x20   block_number_column: number\n\
                      \x20   sort_key: [number]\n\
                      \x20   columns:\n\
                      \x20     number: { type: uint64 }\n";
        let items = "  items:\n\
                     \x20   request:\n\
                     \x20     filters: []\n\
                     \x20   block_number_column: block_number\n\
                     \x20   sort_key: [block_number, seq]\n\
                     \x20   item_order_keys: [seq]\n\
                     \x20   columns:\n\
                     \x20     block_number: { type: uint64 }\n\
                     \x20     seq: { type: uint32 }\n";

        parse_dataset_description(&catalog(&format!("{blocks}{items}")))
            .expect("one block table followed by an item table must load");

        assert!(
            parse_dataset_description(&catalog(items)).is_err(),
            "a dataset with no block table must be refused"
        );

        let second_block_table = "  epochs:\n\
                                  \x20   block_number_column: number\n\
                                  \x20   sort_key: [number]\n\
                                  \x20   columns:\n\
                                  \x20     number: { type: uint64 }\n";
        assert!(
            parse_dataset_description(&catalog(&format!("{blocks}{second_block_table}"))).is_err(),
            "two tables of block shape must be refused"
        );

        assert!(
            parse_dataset_description(&catalog(&format!("{items}{blocks}"))).is_err(),
            "a block table that does not lead the catalog must be refused"
        );

        let unsorted_blocks = "  blocks:\n\
                               \x20   block_number_column: number\n\
                               \x20   sort_key: [hash, number]\n\
                               \x20   columns:\n\
                               \x20     number: { type: uint64 }\n\
                               \x20     hash: { type: string }\n";
        parse_dataset_description(&catalog(&format!("{unsorted_blocks}{items}")))
            .expect("storage order does not decide what a block is");

        // Its item key is `number ++ address`, so a block number alone does not
        // identify one of its rows and it is not a second block table.
        let addressed = "  traces:\n\
                         \x20   request:\n\
                         \x20     filters: []\n\
                         \x20   block_number_column: number\n\
                         \x20   address_column: address\n\
                         \x20   sort_key: [number]\n\
                         \x20   columns:\n\
                         \x20     number: { type: uint64 }\n\
                         \x20     address: { type: list_uint32 }\n";
        parse_dataset_description(&catalog(&format!("{blocks}{addressed}")))
            .expect("an addressed table with no order keys is not a block table");
    }

    /// A duplicate name makes a client's request ambiguous, and iteration order
    /// — not the catalog — decides which table answers it (INV-D10).
    ///
    /// Covers CT-1 · INV-D10
    #[test]
    fn test_validate_rejects_duplicate_names() {
        let catalog = |request_name: &str, output_name: &str| {
            format!(
                r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: {{ type: uint64 }}
  items:
    request:
      name: items
      filters: []
    output:
      name: item
      fields: [seq]
    sort_key: [block_number, seq]
    item_order_keys: [seq]
    columns:
      block_number: {{ type: uint64 }}
      seq: {{ type: uint32 }}
  others:
    request:
      name: {request_name}
      filters: []
    output:
      name: {output_name}
      fields: [seq]
    sort_key: [block_number, seq]
    item_order_keys: [seq]
    columns:
      block_number: {{ type: uint64 }}
      seq: {{ type: uint32 }}
aliases:
  aliased:
    table: items
    filters: []
"#
            )
        };

        parse_dataset_description(&catalog("others", "other")).expect("distinct names must load");

        for (what, request_name, output_name) in [
            ("two tables claiming one request name", "items", "other"),
            ("two tables claiming one output name", "others", "item"),
            (
                "an alias claiming a table's request name",
                "aliased",
                "other",
            ),
            // `blocks` declares no request name, so it holds its own — and a
            // table declaring that name takes it, leaving the block table
            // unaddressable.
            (
                "a table claiming another's undeclared request name",
                "blocks",
                "other",
            ),
        ] {
            assert!(
                parse_dataset_description(&catalog(request_name, output_name)).is_err(),
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
                "tron",
            ]
        );
    }
}
