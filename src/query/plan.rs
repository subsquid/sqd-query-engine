use crate::error::ErrorKind;
use crate::metadata::{
    ColumnDescription, ColumnType, DatasetDescription, JsonEncoding, RelationDef,
    RelationKind as MetaRelationKind, SpecialFilter, TableDescription, MAX_DISCRIMINATOR_BYTES,
};
use crate::query::parse::{parse_hex, Query, QueryItem};
use crate::scan::predicate::{
    col_bloom, col_eq, col_in_list, col_list_contains_any_string, col_list_contains_any_u32,
    ColumnPredicate, InListPredicate, RowPredicate, ScalarValue,
};
use crate::{engine_bail, engine_ensure, engine_err};
use anyhow::Result;
use arrow::array::*;
use std::collections::HashSet;
use std::sync::Arc;

/// `P-MAX-BLOOM-VALUES` (spec/09-parameters.md §9.1). Each value is a separate
/// hash-and-probe over every row, so the list is a cost multiplier the client
/// picks.
const MAX_BLOOM_VALUES: usize = 10;

/// `P-MAX-DISCRIMINATOR-FILTERS` (spec/09-parameters.md §9.1).
const MAX_DISCRIMINATOR_FILTERS: usize = 1;

/// An execution plan compiled from a query.
#[derive(Debug)]
pub struct Plan {
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
    /// Hash the client believes the block before `from_block` has, if supplied.
    pub parent_block_hash: Option<String>,
    /// The blocks table name (e.g., "blocks").
    pub block_table: String,
    /// Output columns for the blocks table.
    pub block_output_columns: Vec<String>,
    /// Plans for each table that has query items.
    pub table_plans: Vec<TablePlan>,
}

/// Plan for scanning and filtering a single table.
#[derive(Debug)]
pub struct TablePlan {
    /// Table name.
    pub table: String,
    /// Columns to include in the output.
    pub output_columns: Vec<String>,
    /// Predicates from all query items, OR'd together.
    pub predicates: Vec<RowPredicate>,
    /// All relations requested (union across all items).
    pub relations: Vec<RelationPlan>,
}

/// A relation to evaluate after scanning.
#[derive(Debug, Clone)]
pub struct RelationPlan {
    pub target_table: String,
    pub kind: RelationKind,
    pub left_key: Vec<String>,
    pub right_key: Vec<String>,
    pub output_columns: Vec<String>,
    /// Predicates from items that requested this relation (OR'd).
    /// `None` means all rows qualify (an item with no filters requested it).
    /// `Some(preds)` means only rows matching these predicates should feed the relation.
    pub source_predicates: Option<Vec<RowPredicate>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationKind {
    Join,
    Children,
    Parents,
}

/// Compile a parsed query into an execution plan.
pub fn compile(query: &Query, metadata: &DatasetDescription) -> Result<Plan> {
    // The block table is the one a block number alone identifies. Validation has
    // already established there is exactly one (INV-D3); the fallback is for a
    // catalog that never went through it.
    let block_table = metadata
        .tables
        .iter()
        .find(|(_, desc)| desc.is_block_table())
        .map(|(name, _)| name.clone())
        .unwrap_or_else(|| "blocks".to_string());

    let block_table_desc = metadata.table(&block_table);

    // Fork detection needs a column holding the predecessor's hash. Where the
    // catalog declares none the request cannot be answered, and answering it
    // anyway serves data from a branch the client did not ask about — the one
    // outcome `parentBlockHash` exists to prevent (INV-E5). It is refused here
    // rather than at execution because it is a decision about the request and
    // the catalog, made before a chunk is opened: from inside the chunk, "this
    // dataset has no parent hash" and "this chunk cannot see that far back" both
    // have to return quietly.
    if query.parent_block_hash.is_some() {
        let declared = block_table_desc.is_some_and(|d| d.parent_hash_column.is_some());
        engine_ensure!(
            declared,
            ErrorKind::UnsupportedRequestField,
            "'parentBlockHash' is not supported for dataset '{}': its '{}' table declares \
             no parent-hash column",
            metadata.name,
            block_table
        );
    }

    let block_output_columns: Vec<String> = query
        .fields
        .get(&block_table)
        .map(|cols| order_columns_by_metadata(cols, block_table_desc))
        .unwrap_or_default();

    let mut table_plans = Vec::new();

    for (table_name, items) in &query.items {
        let table_desc = metadata.table(table_name).ok_or_else(|| {
            engine_err!(
                ErrorKind::UnknownTable,
                "table '{}' not found in metadata",
                table_name
            )
        })?;

        // Determine output columns, ordered by metadata column definition order
        let output_columns: Vec<String> = query
            .fields
            .get(table_name)
            .map(|cols| order_columns_by_metadata(cols, Some(table_desc)))
            .unwrap_or_default();

        // Compile each item into predicates
        let mut all_predicates = Vec::new();
        let mut all_relations: Vec<RelationPlan> = Vec::new();
        // Keyed by the alias the item came through as well as the relation's
        // name: two aliases over one table declare their relations separately,
        // and the same name can mean a different join on each.
        let mut seen_relations: HashSet<(Option<String>, String)> = HashSet::new();
        // Track source predicates per relation name, and how many items request each
        let mut rel_source_preds: std::collections::HashMap<String, Option<Vec<RowPredicate>>> =
            std::collections::HashMap::new();
        let mut rel_item_count: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        // Unsatisfiable items are not counted: they contribute no rows, so a
        // relation every *remaining* item asks for still applies to every row
        // the scan produces.
        let mut total_items = 0usize;

        for item in items {
            let item_predicates = match compile_item_predicates(item, table_desc)? {
                CompiledItem::Unsatisfiable => continue,
                CompiledItem::Predicates(preds) => preds,
            };
            total_items += 1;
            let item_has_predicates = !item_predicates.is_empty();
            all_predicates.extend(item_predicates.clone());

            // Collect relations (dedup across items)
            for rel_name in &item.relations {
                *rel_item_count.entry(rel_name.clone()).or_default() += 1;

                // Update source predicates for this relation
                let entry = rel_source_preds
                    .entry(rel_name.clone())
                    .or_insert_with(|| Some(Vec::new()));
                if !item_has_predicates {
                    // Item with no filters matches all rows → relation applies to all
                    *entry = None;
                } else if let Some(preds) = entry {
                    preds.extend(item_predicates.clone());
                }

                let rel_key = (item.alias.clone(), rel_name.clone());
                if seen_relations.contains(&rel_key) {
                    continue;
                }
                seen_relations.insert(rel_key);

                let rel_def =
                    relation_def(metadata, table_desc, item, rel_name).ok_or_else(|| {
                        engine_err!(
                            ErrorKind::UnknownFilter,
                            "unknown relation '{}' for table '{}'",
                            rel_name,
                            table_name
                        )
                    })?;

                let rel_table_desc = metadata.table(&rel_def.table);
                let target_output: Vec<String> = query
                    .fields
                    .get(&rel_def.table)
                    .map(|cols| order_columns_by_metadata(cols, rel_table_desc))
                    .unwrap_or_default();

                let kind = match rel_def.kind {
                    MetaRelationKind::Join => RelationKind::Join,
                    MetaRelationKind::Children => RelationKind::Children,
                    MetaRelationKind::Parents => RelationKind::Parents,
                };

                let plan = RelationPlan {
                    target_table: rel_def.table.clone(),
                    kind,
                    left_key: rel_def.effective_left_key().to_vec(),
                    right_key: rel_def.effective_right_key().to_vec(),
                    output_columns: target_output,
                    source_predicates: None, // filled in below
                };

                // Two aliases naming the same join is one scan, not two.
                let already = all_relations.iter().any(|r| {
                    r.target_table == plan.target_table
                        && r.kind == plan.kind
                        && r.left_key == plan.left_key
                        && r.right_key == plan.right_key
                        && r.output_columns == plan.output_columns
                });
                if !already {
                    all_relations.push(plan);
                }
            }
        }

        // Set source_predicates on each relation.
        // If ALL items request a relation, source_predicates = None (all primary rows qualify,
        // since the union of all items' predicates IS the primary scan predicate).
        for rel in &mut all_relations {
            for (rel_name, preds) in &rel_source_preds {
                // Resolve the way the relation was created: through the alias of
                // any item that asked for it, and otherwise on the table.
                let rel_def = items
                    .iter()
                    .filter(|item| item.relations.contains(rel_name))
                    .find_map(|item| relation_def(metadata, table_desc, item, rel_name));
                if let Some(rel_def) = rel_def {
                    if rel_def.table == rel.target_table {
                        let kind = match rel_def.kind {
                            MetaRelationKind::Join => RelationKind::Join,
                            MetaRelationKind::Children => RelationKind::Children,
                            MetaRelationKind::Parents => RelationKind::Parents,
                        };
                        if kind == rel.kind
                            && rel_def.effective_left_key() == rel.left_key.as_slice()
                        {
                            // If all items request this relation, no filtering needed
                            let count = rel_item_count.get(rel_name).copied().unwrap_or(0);
                            rel.source_predicates = if count >= total_items {
                                None
                            } else {
                                preds.clone()
                            };
                            break;
                        }
                    }
                }
            }
        }

        // Every item matched nothing, so the table has nothing to contribute.
        // Planning it anyway would scan it to produce an empty result.
        if total_items == 0 {
            continue;
        }

        table_plans.push(TablePlan {
            table: table_name.clone(),
            output_columns,
            predicates: all_predicates,
            relations: all_relations,
        });
    }

    Ok(Plan {
        from_block: query.from_block,
        to_block: query.to_block,
        include_all_blocks: query.include_all_blocks,
        parent_block_hash: query.parent_block_hash.clone(),
        block_table,
        block_output_columns,
        table_plans,
    })
}

/// The relation `name` denotes for one item.
///
/// An item addressed through an alias is admitted against the alias's own
/// relation list (INV-Q6), so that is where its definition comes from: the
/// table's list would be a different join under the same name, and another
/// alias's is not this request's at all.
fn relation_def<'a>(
    metadata: &'a DatasetDescription,
    table_desc: &'a TableDescription,
    item: &QueryItem,
    name: &str,
) -> Option<&'a RelationDef> {
    match &item.alias {
        Some(alias) => metadata.aliases.get(alias)?.relations.get(name),
        None => table_desc.request().relations.get(name),
    }
}

/// Order output columns according to metadata column definition order (YAML key order).
/// Virtual fields are placed after the last column they reference, or at the end.
fn order_columns_by_metadata(
    cols: &[String],
    table_desc: Option<&TableDescription>,
) -> Vec<String> {
    let Some(desc) = table_desc else {
        return cols.to_vec();
    };

    let mut result: Vec<String> = Vec::with_capacity(cols.len());
    let col_set: HashSet<&str> = cols.iter().map(|s| s.as_str()).collect();

    // First pass: add columns in metadata definition order
    for col_name in desc.columns.keys() {
        if col_set.contains(col_name.as_str()) {
            result.push(col_name.clone());
        }
    }

    // Second pass: add virtual fields in metadata definition order
    for vf_name in desc.output.virtual_fields.keys() {
        if col_set.contains(vf_name.as_str()) {
            result.push(vf_name.clone());
        }
    }

    // Third pass: add any remaining columns not found in metadata (shouldn't happen, but safe)
    for col in cols {
        if !result.contains(col) {
            result.push(col.clone());
        }
    }

    result
}

/// What one item request compiles to.
enum CompiledItem {
    /// No row can match, whatever the chunk holds (INV-P3).
    Unsatisfiable,
    /// One predicate per discriminator length group, OR'd together. A single
    /// predicate with no columns matches every row.
    Predicates(Vec<RowPredicate>),
}

/// Filter keys that address one discriminator: the special filter itself and
/// every column it dispatches to (`d1`, `d2`, …).
///
/// Read from the catalog rather than named here, so a dataset with a different
/// discriminator is bounded by the same rule without code (INV-X1).
fn discriminator_family(table: &TableDescription) -> HashSet<&str> {
    let mut family: HashSet<&str> = HashSet::new();

    for (name, special) in &table.request().special_filters {
        if let SpecialFilter::Discriminator { by_length } = special {
            family.insert(name.as_str());
            family.extend(by_length.values().map(String::as_str));
        }
    }

    family
}

/// The request-shape bounds that cost, rather than correctness, motivates
/// (INV-Q10, INV-Q11). Checked before anything compiles, so which filter the
/// engine happens to read first cannot decide the outcome.
fn check_item_limits(item: &QueryItem, table: &TableDescription) -> Result<()> {
    let family = discriminator_family(table);
    let discriminators = item
        .filters
        .iter()
        .filter(|(key, _)| family.contains(key.as_str()))
        .count();

    engine_ensure!(
        discriminators <= MAX_DISCRIMINATOR_FILTERS,
        ErrorKind::ConflictingFilters,
        "item request carries {} discriminator filters, at most {} allowed: they \
         narrow the same column family and only one of them can hold",
        discriminators,
        MAX_DISCRIMINATOR_FILTERS
    );

    for (key, value) in &item.filters {
        let is_bloom = matches!(
            table.request().special_filters.get(key),
            Some(SpecialFilter::Bloom { .. })
        );
        if !is_bloom {
            continue;
        }

        let len = value.as_array().map(Vec::len).unwrap_or(0);
        engine_ensure!(
            len <= MAX_BLOOM_VALUES,
            ErrorKind::TooManyBloomValues,
            "filter '{}' carries {} values, at most {} allowed",
            key,
            len,
            MAX_BLOOM_VALUES
        );
    }

    Ok(())
}

/// Compile a single query item's filters into one or more RowPredicates.
/// Returns multiple predicates when discriminator dispatches to multiple column lengths
/// (each length group becomes its own predicate, OR'd with others).
fn compile_item_predicates(item: &QueryItem, table: &TableDescription) -> Result<CompiledItem> {
    check_item_limits(item, table)?;

    let mut col_predicates: Vec<ColumnPredicate> = Vec::new();
    let mut discriminator_groups: Option<Vec<Vec<ColumnPredicate>>> = None;

    for (key, value) in &item.filters {
        // `"c": []` is a filter no row passes, not an absent filter, and it
        // sinks the whole item whatever its other filters say (INV-P3). Reading
        // it as "unconstrained" would turn "none of these addresses" into
        // "every row in the chunk".
        //
        // Only where a list is a value at all: a flag or a range bound takes a
        // scalar, so `[]` there is the wrong type and stays an error.
        let takes_value_list = !matches!(
            table.request().special_filters.get(key),
            Some(
                SpecialFilter::RangeGte { .. }
                    | SpecialFilter::RangeLte { .. }
                    | SpecialFilter::GteConst { .. }
            )
        );

        if takes_value_list && value.as_array().is_some_and(|values| values.is_empty()) {
            return Ok(CompiledItem::Unsatisfiable);
        }

        // Special filters
        if let Some(special) = table.request().special_filters.get(key) {
            match special {
                SpecialFilter::Discriminator { by_length } => {
                    let groups = compile_discriminator(value, by_length)?;
                    discriminator_groups = groups;
                }
                SpecialFilter::Bloom {
                    column,
                    bytes,
                    hashes,
                } => {
                    col_predicates.push(compile_bloom_filter(value, column, *bytes, *hashes)?);
                }
                SpecialFilter::RangeGte { column } => {
                    let pred = compile_range_gte(value, column, table)?;
                    if let Some(p) = pred {
                        col_predicates.push(p);
                    }
                }
                SpecialFilter::RangeLte { column } => {
                    let pred = compile_range_lte(value, column, table)?;
                    if let Some(p) = pred {
                        col_predicates.push(p);
                    }
                }
                SpecialFilter::ColumnAlias { column } => {
                    let col_desc = table.column(column).ok_or_else(|| {
                        engine_err!(
                            ErrorKind::UnknownFilter,
                            "alias target column '{}' not found",
                            column
                        )
                    })?;
                    let boolean_filter = value
                        .as_bool()
                        .filter(|_| matches!(col_desc.data_type, ColumnType::Boolean));

                    if let Some(bool_val) = boolean_filter {
                        col_predicates.push(col_eq(column, ScalarValue::Boolean(bool_val)));
                    } else if is_filter_scalar(value) || value.is_array() {
                        let values = match value.as_array() {
                            Some(arr) => arr,
                            None => std::slice::from_ref(value),
                        };
                        col_predicates.push(compile_in_list(column, values, col_desc)?);
                    } else {
                        engine_bail!(
                            ErrorKind::InvalidFilterValue,
                            "invalid filter value for '{}': expected array, boolean, number, or string",
                            key
                        );
                    }
                }
                SpecialFilter::GteConst {
                    column,
                    value: konst,
                } => {
                    // A non-boolean here used to mean "filter off", which answers
                    // a strictly wider question than the one asked and says
                    // nothing about it. The reference types the field `bool`.
                    let enabled = value.as_bool().ok_or_else(|| {
                        engine_err!(
                            ErrorKind::InvalidFilterValue,
                            "filter '{}' must be a boolean, got {}",
                            key,
                            value
                        )
                    })?;

                    if enabled {
                        table.column(column).ok_or_else(|| {
                            engine_err!(
                                ErrorKind::UnknownFilter,
                                "gte_const column '{}' not found",
                                column
                            )
                        })?;
                        col_predicates.push(ColumnPredicate {
                            column: column.to_string(),
                            predicate: Arc::new(crate::scan::predicate::RangeGtePredicate::new(
                                ScalarValue::Utf8(konst.clone()),
                            )),
                        });
                    }
                }
            }
            continue;
        }

        // Column filter
        let col_desc = table.column(key).ok_or_else(|| {
            engine_err!(
                ErrorKind::UnknownFilter,
                "column '{}' not found in table",
                key
            )
        })?;

        // A boolean only means anything on a boolean column. Anywhere else it
        // compiles to a comparison that cannot match, and the query comes back
        // empty with nothing to say why.
        let boolean_filter = value
            .as_bool()
            .filter(|_| matches!(col_desc.data_type, ColumnType::Boolean));

        if let Some(bool_val) = boolean_filter {
            col_predicates.push(col_eq(key, ScalarValue::Boolean(bool_val)));
        } else if is_filter_scalar(value) || value.is_array() {
            // A bare value is a one-element list and compiles as one, so the two
            // forms cannot drift: the scalar branch used to compare a `Utf8`
            // against whatever the column was, which worked on a string column
            // and silently matched nothing on a binary one.
            let values = match value.as_array() {
                Some(arr) => arr,
                None => std::slice::from_ref(value),
            };
            col_predicates.push(compile_in_list(key, values, col_desc)?);
        } else {
            engine_bail!(
                ErrorKind::InvalidFilterValue,
                "invalid filter value for '{}': expected array, boolean, number, or string",
                key
            );
        }
    }

    // If there are discriminator groups, distribute other predicates across them
    if let Some(groups) = discriminator_groups {
        let mut result = Vec::new();
        for group in groups {
            let mut preds = col_predicates.clone();
            preds.extend(group);
            result.push(RowPredicate::new(preds));
        }
        Ok(CompiledItem::Predicates(result))
    } else {
        Ok(CompiledItem::Predicates(vec![RowPredicate::new(
            col_predicates,
        )]))
    }
}

/// Reject a malformed value on a column declared `encoding: hex_bytes`
/// (INV-Q12). Columns with any other encoding take their values verbatim.
fn ensure_hex_for_column(s: &str, column: &str, col_desc: &ColumnDescription) -> Result<()> {
    if col_desc.encoding != Some(JsonEncoding::HexBytes) {
        return Ok(());
    }

    engine_ensure!(
        crate::query::parse::is_hex_literal(s),
        ErrorKind::InvalidHex,
        "invalid hex value '{}' in filter on '{}': expected a 0x-prefixed, \
         even-length hex string",
        s,
        column
    );

    Ok(())
}

/// Whether a bare value is one a filter can carry: a string, or an integer of
/// either sign. `is_u64` alone leaves out every negative, which is how a
/// `{"version": -1}` on a signed column used to come back as "expected array,
/// boolean, number, or string" for a value that was all four.
fn is_filter_scalar(value: &serde_json::Value) -> bool {
    value.is_string() || value.is_u64() || value.is_i64()
}

/// Case folding follows the column, not the filter: a column the catalog marks
/// case-insensitive compares that way whether the value arrives as a scalar or
/// inside an IN-list (INV-P8). Clients send checksummed addresses.
fn fold_for_column(s: &str, col_desc: &ColumnDescription) -> String {
    if col_desc.folds_case() {
        s.to_ascii_lowercase()
    } else {
        s.to_string()
    }
}

/// Split an IN-list over a binary column into hex byte strings and plain
/// integers.
///
/// A string element must be well-formed hex — `0x`/`0X` prefix, even digit
/// count (INV-Q12); anything that is neither a string nor an integer is an
/// error. Numbers are widened to `i128`, which is the one type that holds both a
/// `uint64` above `i64::MAX` and a negative value, so the caller narrows once
/// rather than each branch guessing a signedness. Whether a parsed value *fits*
/// the column is left to the caller: one that does not fit matches nothing
/// (INV-P14), and a never-matching disjunct is a no-op inside an IN-list.
fn split_binary_in_list(
    column: &str,
    values: &[serde_json::Value],
) -> Result<(Vec<Vec<u8>>, Vec<i128>)> {
    let mut hex = Vec::new();
    let mut numbers = Vec::new();

    for v in values {
        if let Some(s) = v.as_str() {
            let bytes = parse_hex(s).ok_or_else(|| {
                engine_err!(
                    ErrorKind::InvalidHex,
                    "invalid hex value '{}' in filter on '{}': expected a 0x-prefixed, \
                     even-length hex string",
                    s,
                    column
                )
            })?;
            hex.push(bytes);
        } else if let Some(n) = v.as_u64() {
            numbers.push(i128::from(n));
        } else if let Some(n) = v.as_i64() {
            numbers.push(i128::from(n));
        } else {
            engine_bail!(
                ErrorKind::InvalidFilterValue,
                "invalid value {} in filter on '{}': expected a hex string or an integer",
                v,
                column
            );
        }
    }

    Ok((hex, numbers))
}

/// Compile an IN-list over a signed integer column.
///
/// Signed columns hold counts and sentinels — Solana's `-1` for a legacy
/// transaction version, a negative reward — never addresses, so a hex value on
/// one is a category error rather than a value that happens not to match. A
/// number outside the declared range is dropped instead: it matches nothing
/// (INV-P14).
///
/// The list is built at `int64` whatever the declared width, because the width
/// the chunk was written at is its own choice (INV-D7); the range is what the
/// declared type decides.
fn compile_signed_in_list(
    column: &str,
    values: &[serde_json::Value],
    range: std::ops::RangeInclusive<i128>,
    declared: &str,
) -> Result<ColumnPredicate> {
    let (hex, numbers) = split_binary_in_list(column, values)?;

    engine_ensure!(
        hex.is_empty(),
        ErrorKind::InvalidFilterValue,
        "invalid value in filter on '{}': a signed {} column takes numbers, not hex strings",
        column,
        declared
    );

    let vals: Vec<i64> = numbers
        .into_iter()
        .filter(|n| range.contains(n))
        .map(|n| n as i64)
        .collect();

    Ok(col_in_list(
        column,
        Arc::new(Int64Array::from(vals)) as Arc<dyn Array>,
    ))
}

/// Compile an IN-list filter for a column based on its type.
fn compile_in_list(
    column: &str,
    values: &[serde_json::Value],
    col_desc: &ColumnDescription,
) -> Result<ColumnPredicate> {
    match &col_desc.data_type {
        ColumnType::String => {
            // Stored as text (base58 keys, `0x…` hex, enum names), so the value
            // is compared as text rather than parsed. A column declared
            // `hex` still has its values checked for well-formedness (INV-Q12):
            // most of the engine's hex surface is string-typed, and an address
            // missing its `0x` compares unequal to every stored value, so
            // without the check it answers 200 with no rows and no reason.
            let mut vals: Vec<String> = Vec::with_capacity(values.len());
            for v in values {
                let s = v.as_str().ok_or_else(|| {
                    engine_err!(
                        ErrorKind::InvalidFilterValue,
                        "invalid value {} in filter on '{}': expected a string",
                        v,
                        column
                    )
                })?;
                ensure_hex_for_column(s, column, col_desc)?;
                vals.push(fold_for_column(s, col_desc));
            }
            let str_refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
            Ok(col_in_list(
                column,
                Arc::new(StringArray::from(str_refs)) as Arc<dyn Array>,
            ))
        }
        ColumnType::UInt8 => {
            let (hex, numbers) = split_binary_in_list(column, values)?;
            let mut vals: Vec<u8> = hex.iter().filter(|b| b.len() == 1).map(|b| b[0]).collect();
            vals.extend(numbers.iter().filter_map(|&n| u8::try_from(n).ok()));
            Ok(col_in_list(
                column,
                Arc::new(UInt8Array::from(vals)) as Arc<dyn Array>,
            ))
        }
        ColumnType::UInt16 => {
            let (hex, numbers) = split_binary_in_list(column, values)?;
            let mut vals: Vec<u16> = hex
                .iter()
                .filter(|b| b.len() == 2)
                .map(|b| u16::from_be_bytes([b[0], b[1]]))
                .collect();
            vals.extend(numbers.iter().filter_map(|&n| u16::try_from(n).ok()));
            Ok(col_in_list(
                column,
                Arc::new(UInt16Array::from(vals)) as Arc<dyn Array>,
            ))
        }
        ColumnType::UInt32 => {
            let (hex, numbers) = split_binary_in_list(column, values)?;
            let mut vals: Vec<u32> = hex
                .iter()
                .filter(|b| b.len() == 4)
                .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
                .collect();
            vals.extend(numbers.iter().filter_map(|&n| u32::try_from(n).ok()));
            Ok(col_in_list(
                column,
                Arc::new(UInt32Array::from(vals)) as Arc<dyn Array>,
            ))
        }
        ColumnType::UInt64 => {
            let (hex, numbers) = split_binary_in_list(column, values)?;
            let mut vals: Vec<u64> = hex
                .iter()
                .filter(|b| b.len() == 8)
                .map(|b| u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
                .collect();
            vals.extend(numbers.iter().filter_map(|&n| u64::try_from(n).ok()));
            Ok(col_in_list(
                column,
                Arc::new(UInt64Array::from(vals)) as Arc<dyn Array>,
            ))
        }
        ColumnType::Int16 => compile_signed_in_list(
            column,
            values,
            i128::from(i16::MIN)..=i128::from(i16::MAX),
            "int16",
        ),
        ColumnType::Int32 => compile_signed_in_list(
            column,
            values,
            i128::from(i32::MIN)..=i128::from(i32::MAX),
            "int32",
        ),
        ColumnType::Int64 => compile_signed_in_list(
            column,
            values,
            i128::from(i64::MIN)..=i128::from(i64::MAX),
            "int64",
        ),
        ColumnType::FixedBinary(size) => {
            let (hex, numbers) = split_binary_in_list(column, values)?;
            engine_ensure!(
                numbers.is_empty(),
                ErrorKind::InvalidFilterValue,
                "invalid value in filter on '{}': a fixed_binary_{} column takes hex strings, \
                 not numbers",
                column,
                size
            );
            let vals: Vec<&Vec<u8>> = hex.iter().filter(|b| b.len() == *size).collect();
            let mut builder = FixedSizeBinaryBuilder::with_capacity(vals.len(), *size as i32);
            for v in vals {
                builder.append_value(v)?;
            }
            Ok(col_in_list(
                column,
                Arc::new(builder.finish()) as Arc<dyn Array>,
            ))
        }
        ColumnType::ListUInt32 => {
            // List-contains-any over integers. An out-of-range value matches
            // nothing; it must never be truncated into a *different* id.
            let mut vals: Vec<u32> = Vec::with_capacity(values.len());
            for v in values {
                let n = v.as_u64().ok_or_else(|| {
                    engine_err!(
                        ErrorKind::InvalidFilterValue,
                        "invalid value {} in filter on '{}': expected an unsigned integer",
                        v,
                        column
                    )
                })?;
                if let Ok(n32) = u32::try_from(n) {
                    vals.push(n32);
                }
            }
            Ok(col_list_contains_any_u32(column, vals))
        }
        ColumnType::ListString => {
            let mut vals: Vec<String> = Vec::with_capacity(values.len());
            for v in values {
                let s = v.as_str().ok_or_else(|| {
                    engine_err!(
                        ErrorKind::InvalidFilterValue,
                        "invalid value {} in filter on '{}': expected a string",
                        v,
                        column
                    )
                })?;
                ensure_hex_for_column(s, column, col_desc)?;
                vals.push(fold_for_column(s, col_desc));
            }
            Ok(col_list_contains_any_string(column, vals))
        }
        _ => engine_bail!(
            ErrorKind::InvalidFilterValue,
            "unsupported column type {:?} for IN-list filter on '{}'",
            col_desc.data_type,
            column
        ),
    }
}

/// Compile a discriminator filter: dispatch hex prefixes to d1-d16 by length.
/// Returns None if an empty prefix is found (matches everything).
/// Returns groups of ColumnPredicates, one group per byte length.
///
/// An empty *list* never reaches here: the caller has already read it as an
/// item that matches nothing (INV-P3).
fn compile_discriminator(
    value: &serde_json::Value,
    columns: &std::collections::BTreeMap<String, String>,
) -> Result<Option<Vec<Vec<ColumnPredicate>>>> {
    let arr = value.as_array().ok_or_else(|| {
        engine_err!(
            ErrorKind::InvalidFilterValue,
            "discriminator values must be an array"
        )
    })?;

    // Parse hex strings and group by byte length
    let mut by_length: std::collections::BTreeMap<usize, Vec<Vec<u8>>> =
        std::collections::BTreeMap::new();

    for v in arr {
        let s = v.as_str().ok_or_else(|| {
            engine_err!(
                ErrorKind::InvalidFilterValue,
                "discriminator value must be a string"
            )
        })?;
        let bytes = parse_hex(s).ok_or_else(|| {
            engine_err!(ErrorKind::InvalidHex, "invalid hex in discriminator: {}", s)
        })?;
        engine_ensure!(
            bytes.len() <= MAX_DISCRIMINATOR_BYTES,
            ErrorKind::DiscriminatorTooLong,
            "discriminator max {} bytes, got {}",
            MAX_DISCRIMINATOR_BYTES,
            bytes.len()
        );
        if bytes.is_empty() {
            return Ok(None); // empty prefix matches everything
        }
        by_length.entry(bytes.len()).or_default().push(bytes);
    }

    let mut groups = Vec::new();

    for (len, values) in by_length {
        let col_name = columns.get(&len.to_string()).ok_or_else(|| {
            engine_err!(
                ErrorKind::InvalidFilterValue,
                "no discriminator column for length {}",
                len
            )
        })?;

        let array: Arc<dyn Array> = match len {
            1 => Arc::new(UInt8Array::from_iter_values(
                values
                    .iter()
                    .map(|d| u8::from_be_bytes(d.as_slice().try_into().unwrap())),
            )),
            2 => Arc::new(UInt16Array::from_iter_values(
                values
                    .iter()
                    .map(|d| u16::from_be_bytes(d.as_slice().try_into().unwrap())),
            )),
            4 => Arc::new(UInt32Array::from_iter_values(
                values
                    .iter()
                    .map(|d| u32::from_be_bytes(d.as_slice().try_into().unwrap())),
            )),
            8 => Arc::new(UInt64Array::from_iter_values(
                values
                    .iter()
                    .map(|d| u64::from_be_bytes(d.as_slice().try_into().unwrap())),
            )),
            _ => {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), len as i32);
                for d in &values {
                    builder.append_value(d)?;
                }
                Arc::new(builder.finish())
            }
        };

        let pred = ColumnPredicate {
            column: col_name.clone(),
            predicate: Arc::new(InListPredicate::new(array)),
        };
        groups.push(vec![pred]);
    }

    Ok(Some(groups))
}

/// Compile a bloom filter predicate.
///
/// A non-string element is an error and an empty list matches nothing, matching
/// the reference and every other filter shape here. Dropping the bad elements
/// and then compiling *no predicate* — which is what this used to do — is the
/// only way a filter in this engine could fail open: the query widens to the
/// whole table and answers 200.
fn compile_bloom_filter(
    value: &serde_json::Value,
    column: &str,
    num_bytes: usize,
    num_hashes: usize,
) -> Result<ColumnPredicate> {
    let arr = value.as_array().ok_or_else(|| {
        engine_err!(
            ErrorKind::InvalidFilterValue,
            "bloom filter values must be an array"
        )
    })?;

    let mut needles: Vec<Vec<u8>> = Vec::with_capacity(arr.len());
    for v in arr {
        let s = v.as_str().ok_or_else(|| {
            engine_err!(
                ErrorKind::InvalidFilterValue,
                "invalid value {} in filter on '{}': expected a string",
                v,
                column
            )
        })?;
        needles.push(s.as_bytes().to_vec());
    }

    if needles.is_empty() {
        return Ok(crate::scan::predicate::col_never(column));
    }

    Ok(col_bloom(column, needles, num_bytes, num_hashes))
}

/// Compile a range >= filter.
fn compile_range_gte(
    value: &serde_json::Value,
    column: &str,
    table: &TableDescription,
) -> Result<Option<ColumnPredicate>> {
    let col_desc = table.column(column).ok_or_else(|| {
        engine_err!(
            ErrorKind::UnknownFilter,
            "range column '{}' not found",
            column
        )
    })?;

    let n = value.as_u64().ok_or_else(|| {
        engine_err!(
            ErrorKind::InvalidFilterValue,
            "range filter value must be a number"
        )
    })?;

    match col_desc.data_type {
        ColumnType::UInt64 => Ok(Some(ColumnPredicate {
            column: column.to_string(),
            predicate: Arc::new(crate::scan::predicate::RangeGtePredicate::new(
                ScalarValue::UInt64(n),
            )),
        })),
        _ => engine_bail!(
            ErrorKind::InvalidFilterValue,
            "range filter only supports UInt64 columns"
        ),
    }
}

/// Compile a range <= filter.
fn compile_range_lte(
    value: &serde_json::Value,
    column: &str,
    table: &TableDescription,
) -> Result<Option<ColumnPredicate>> {
    let col_desc = table.column(column).ok_or_else(|| {
        engine_err!(
            ErrorKind::UnknownFilter,
            "range column '{}' not found",
            column
        )
    })?;

    let n = value.as_u64().ok_or_else(|| {
        engine_err!(
            ErrorKind::InvalidFilterValue,
            "range filter value must be a number"
        )
    })?;

    match col_desc.data_type {
        ColumnType::UInt64 => Ok(Some(ColumnPredicate {
            column: column.to_string(),
            predicate: Arc::new(crate::scan::predicate::RangeLtePredicate::new(
                ScalarValue::UInt64(n),
            )),
        })),
        _ => engine_bail!(
            ErrorKind::InvalidFilterValue,
            "range filter only supports UInt64 columns"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::load_dataset_description;
    use crate::query::parse::parse_query;
    use std::path::Path;

    fn solana_metadata() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/solana.yaml")).unwrap()
    }

    fn evm_metadata() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/evm.yaml")).unwrap()
    }

    /// A `d4` value that occurs in the local Solana chunk, so numeric-filter
    /// tests assert against data rather than a guessed constant.
    fn first_d4_value() -> u32 {
        use arrow::array::UInt32Array;
        let table_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk/instructions.parquet");
        let table = crate::scan::ParquetTable::open(&table_path).unwrap();
        let request = crate::scan::ScanRequest::new(vec!["d4"]);
        let batches = crate::scan::scan(&table, &request).unwrap();
        for batch in &batches {
            let col = batch.column_by_name("d4").unwrap();
            let values = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            for i in 0..values.len() {
                if !values.is_null(i) {
                    return values.value(i);
                }
            }
        }
        panic!("chunk has no non-null d4 value");
    }

    #[test]
    fn test_compile_evm_logs_query() {
        let meta = evm_metadata();
        let json = br#"{
            "type": "evm",
            "fromBlock": 17881390,
            "toBlock": 17882786,
            "fields": {
                "block": { "number": true, "hash": true },
                "log": { "address": true, "data": true, "logIndex": true }
            },
            "logs": [{
                "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
                "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                "transaction": true
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        assert_eq!(plan.from_block, 17881390);
        assert_eq!(plan.to_block, Some(17882786));
        assert_eq!(plan.block_table, "blocks");
        // Verify metadata-defined ordering: number comes before hash in evm.yaml
        assert_eq!(plan.block_output_columns, vec!["number", "hash"]);

        assert_eq!(plan.table_plans.len(), 1);
        let logs_plan = &plan.table_plans[0];
        assert_eq!(logs_plan.table, "logs");
        assert!(logs_plan.output_columns.contains(&"address".to_string()));
        assert!(logs_plan.output_columns.contains(&"data".to_string()));
        assert_eq!(logs_plan.predicates.len(), 1);
        // 2 column predicates: address + topic0
        assert_eq!(logs_plan.predicates[0].columns.len(), 2);

        // 1 relation: transaction
        assert_eq!(logs_plan.relations.len(), 1);
        assert_eq!(logs_plan.relations[0].target_table, "transactions");
        assert_eq!(logs_plan.relations[0].kind, RelationKind::Join);
    }

    #[test]
    fn test_compile_solana_instructions_query() {
        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 0,
            "fields": {
                "instruction": { "programId": true, "data": true }
            },
            "instructions": [{
                "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
                "d8": ["0xf8c69e91e17587c8"],
                "transaction": true,
                "innerInstructions": true
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let instr_plan = &plan.table_plans[0];
        assert_eq!(instr_plan.table, "instructions");
        // 1 predicate: programId IN [...] AND d8 IN [...]
        assert_eq!(instr_plan.predicates.len(), 1);
        assert_eq!(instr_plan.predicates[0].columns.len(), 2);

        // 2 relations: transaction + inner_instructions
        assert_eq!(instr_plan.relations.len(), 2);
        let rel_names: Vec<&str> = instr_plan
            .relations
            .iter()
            .map(|r| r.target_table.as_str())
            .collect();
        assert!(rel_names.contains(&"transactions"));
        assert!(rel_names.contains(&"instructions"));
    }

    /// Covers CT-3 · INV-P13
    #[test]
    fn test_compile_discriminator_mixed_lengths() {
        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 0,
            "instructions": [{
                "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
                "discriminator": ["0xab", "0xf8c69e91e17587c8"]
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let instr_plan = &plan.table_plans[0];
        // Mixed discriminator: 1 byte (d1) + 8 bytes (d8)
        // This creates 2 predicates (one per length group):
        // 1: programId IN [...] AND d1 IN [0xab]
        // 2: programId IN [...] AND d8 IN [0xf8c6...]
        assert_eq!(instr_plan.predicates.len(), 2);

        // Each predicate has 2 columns: programId + dN
        assert_eq!(instr_plan.predicates[0].columns.len(), 2);
        assert_eq!(instr_plan.predicates[1].columns.len(), 2);
    }

    /// Covers CT-3 · INV-P1
    #[test]
    fn test_compile_empty_item_no_filters() {
        let meta = evm_metadata();
        let json = br#"{
            "type": "evm",
            "fromBlock": 0,
            "fields": { "transaction": { "hash": true } },
            "transactions": [{}]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let tx_plan = &plan.table_plans[0];
        assert_eq!(tx_plan.predicates.len(), 1);
        // Empty predicate (matches all rows)
        assert_eq!(tx_plan.predicates[0].columns.len(), 0);
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_end_to_end_evm_logs_scan() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = evm_metadata();
        let json = br#"{
            "type": "evm",
            "fromBlock": 0,
            "fields": {
                "log": { "address": true, "topics": true }
            },
            "logs": [{
                "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        // Execute the plan against real data
        let evm_chunk_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/evm/chunk");
        let table_path = evm_chunk_path.join("logs.parquet");
        let parquet_table = crate::scan::ParquetTable::open(&table_path).unwrap();

        let logs_plan = &plan.table_plans[0];
        let table_desc = meta.table("logs").unwrap();

        // Build a ScanRequest from the plan. `output_columns` are logical names,
        // and `topics` is a roll rather than a column of its own.
        let physical =
            crate::output::columns::physical_output_columns(&logs_plan.output_columns, table_desc);
        let output_cols: Vec<&str> = physical.iter().map(|s| s.as_str()).collect();
        let pred_refs: Vec<&crate::scan::predicate::RowPredicate> =
            logs_plan.predicates.iter().collect();

        let mut request = crate::scan::ScanRequest::new(output_cols);
        request.predicates = pred_refs;
        request.from_block = Some(plan.from_block);
        request.to_block = plan.to_block;
        request.block_number_column = Some(table_desc.block_number_column.as_str());

        let batches = crate::scan::scan(&parquet_table, &request).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // ERC-20 Transfer topic should match many rows but not all
        assert!(total_rows > 0, "should match ERC-20 Transfer events");
        assert!(
            total_rows < parquet_table.num_rows() as usize,
            "should not match all rows"
        );

        // Verify output only has the requested columns. `topics` is a roll, so
        // it projects the four columns it gathers rather than one of its own.
        for batch in &batches {
            assert_eq!(batch.num_columns(), 5);
            for column in ["address", "topic0", "topic1", "topic2", "topic3"] {
                assert!(batch.schema().field_with_name(column).is_ok());
            }
        }
    }

    /// Regression: numeric scalar filters were always compiled as UInt64,
    /// causing type mismatch on UInt32 columns → zero results.
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_numeric_filter_on_uint32_column() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = solana_metadata();
        // d4 is a UInt32 column, and one the catalog declares filterable. The
        // value comes from the chunk so the test does not turn on a guess.
        let d4 = first_d4_value();
        let json = format!(
            r#"{{
            "type": "solana",
            "fromBlock": 0,
            "fields": {{
                "instruction": {{ "programId": true }}
            }},
            "instructions": [{{
                "d4": {d4}
            }}]
        }}"#
        );

        let query = parse_query(json.as_bytes(), &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk");
        let table_path = chunk_path.join("instructions.parquet");
        let parquet_table = crate::scan::ParquetTable::open(&table_path).unwrap();

        let instr_plan = &plan.table_plans[0];
        let table_desc = meta.table("instructions").unwrap();

        let output_cols: Vec<&str> = instr_plan
            .output_columns
            .iter()
            .map(|s| s.as_str())
            .collect();
        let pred_refs: Vec<&crate::scan::predicate::RowPredicate> =
            instr_plan.predicates.iter().collect();

        let mut request = crate::scan::ScanRequest::new(output_cols);
        request.predicates = pred_refs;
        request.from_block = Some(plan.from_block);
        request.block_number_column = Some(table_desc.block_number_column.as_str());

        let batches = crate::scan::scan(&parquet_table, &request).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Must match the instructions carrying that d4 (not empty!)
        assert!(
            total_rows > 0,
            "numeric filter on UInt32 column must match rows (was broken when always UInt64)"
        );
    }

    /// Regression: JSON numeric arrays like [0, 1] were silently compiled to
    /// empty IN-lists because compile_in_list only parsed string values.
    ///
    /// Covers CT-3 · INV-P2
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_numeric_in_list_filter() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = solana_metadata();
        let d4 = first_d4_value();
        let json = format!(
            r#"{{
            "type": "solana",
            "fromBlock": 0,
            "fields": {{
                "instruction": {{ "programId": true }}
            }},
            "instructions": [{{
                "d4": [{d4}]
            }}]
        }}"#
        );

        let query = parse_query(json.as_bytes(), &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk");
        let table_path = chunk_path.join("instructions.parquet");
        let parquet_table = crate::scan::ParquetTable::open(&table_path).unwrap();

        let instr_plan = &plan.table_plans[0];
        let table_desc = meta.table("instructions").unwrap();

        let output_cols: Vec<&str> = instr_plan
            .output_columns
            .iter()
            .map(|s| s.as_str())
            .collect();
        let pred_refs: Vec<&crate::scan::predicate::RowPredicate> =
            instr_plan.predicates.iter().collect();

        let mut request = crate::scan::ScanRequest::new(output_cols);
        request.predicates = pred_refs;
        request.from_block = Some(plan.from_block);
        request.block_number_column = Some(table_desc.block_number_column.as_str());

        let batches = crate::scan::scan(&parquet_table, &request).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert!(
            total_rows > 0,
            "numeric IN-list [0, 1, 2] on UInt32 column must match rows"
        );
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_end_to_end_solana_instructions_scan() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 0,
            "fields": {
                "instruction": { "programId": true, "transactionIndex": true }
            },
            "instructions": [{
                "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"]
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk");
        let table_path = chunk_path.join("instructions.parquet");
        let parquet_table = crate::scan::ParquetTable::open(&table_path).unwrap();

        let instr_plan = &plan.table_plans[0];
        let table_desc = meta.table("instructions").unwrap();

        let output_cols: Vec<&str> = instr_plan
            .output_columns
            .iter()
            .map(|s| s.as_str())
            .collect();
        let pred_refs: Vec<&crate::scan::predicate::RowPredicate> =
            instr_plan.predicates.iter().collect();

        let mut request = crate::scan::ScanRequest::new(output_cols);
        request.predicates = pred_refs;
        request.from_block = Some(plan.from_block);
        request.block_number_column = Some(table_desc.block_number_column.as_str());

        let batches = crate::scan::scan(&parquet_table, &request).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert!(total_rows > 0, "should match whirlpool instructions");
        assert!(
            total_rows < parquet_table.num_rows() as usize,
            "should not match all instructions"
        );

        // Verify all matching rows have the correct program_id
        for batch in &batches {
            let col = batch
                .column_by_name("program_id")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for i in 0..col.len() {
                assert_eq!(col.value(i), "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
            }
        }
    }

    /// Alias-relation must propagate source_predicates when not all items request it.
    ///
    /// Covers CT-4 · INV-R1
    #[test]
    fn test_alias_relation_source_predicates() {
        use crate::metadata::parse_dataset_description;

        let yaml = r#"
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
  items:
    request:
      filters: []
    columns:
      block_number: { type: uint64 }
      transaction_index: { type: uint32 }
      kind: { type: string }
    item_order_keys: [transaction_index]
  related:
    request:
      filters: []
    columns:
      block_number: { type: uint64 }
      transaction_index: { type: uint32 }
      data: { type: string }
    item_order_keys: [transaction_index]
aliases:
  filteredItems:
    table: items
    filters: [kind]
    implicit_filters:
      kind: ["special"]
    relations:
      related:
        table: related
        kind: join
        key: [block_number, transaction_index]
"#;
        let meta = parse_dataset_description(yaml).unwrap();

        // Two items: one with filter + relation, one without relation
        let json = br#"{
            "type": "test",
            "fromBlock": 0,
            "filteredItems": [
                { "kind": ["special"], "related": true },
                { "kind": ["other"] }
            ]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let items_plan = &plan.table_plans[0];
        assert_eq!(items_plan.table, "items");

        let rel = items_plan
            .relations
            .iter()
            .find(|r| r.target_table == "related")
            .expect("should have 'related' relation");

        // source_predicates should be Some because only 1 of 2 items requests it
        assert!(
            rel.source_predicates.is_some(),
            "alias relation must have source_predicates when not all items request it"
        );
    }
}
