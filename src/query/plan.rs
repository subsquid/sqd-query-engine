use crate::metadata::{
    ColumnDescription, ColumnType, DatasetDescription, RelationKind as MetaRelationKind,
    JsonEncoding, SpecialFilter, TableDescription,
};
use crate::query::parse::{parse_hex, Query, QueryItem};
use crate::scan::predicate::{
    col_bloom, col_eq, col_in_list, col_list_contains_any_string, col_list_contains_any_u32,
    ColumnPredicate, InListPredicate, RowPredicate, ScalarValue,
};
use anyhow::{anyhow, bail, ensure, Result};
use arrow::array::*;
use std::collections::HashSet;
use std::sync::Arc;

/// Convert a JSON numeric value to the appropriate ScalarValue based on column type.
fn numeric_scalar(n: u64, col_type: &ColumnType) -> Result<ScalarValue> {
    match col_type {
        ColumnType::UInt8 => Ok(ScalarValue::UInt8(
            u8::try_from(n).map_err(|_| anyhow!("value {} out of range for UInt8", n))?,
        )),
        ColumnType::UInt16 => Ok(ScalarValue::UInt16(
            u16::try_from(n).map_err(|_| anyhow!("value {} out of range for UInt16", n))?,
        )),
        ColumnType::UInt32 => Ok(ScalarValue::UInt32(
            u32::try_from(n).map_err(|_| anyhow!("value {} out of range for UInt32", n))?,
        )),
        ColumnType::UInt64 => Ok(ScalarValue::UInt64(n)),
        ColumnType::Int16 => Ok(ScalarValue::Int16(
            i16::try_from(n).map_err(|_| anyhow!("value {} out of range for Int16", n))?,
        )),
        ColumnType::Int64 => Ok(ScalarValue::Int64(n as i64)),
        _ => Ok(ScalarValue::UInt64(n)),
    }
}

/// An execution plan compiled from a query.
#[derive(Debug)]
pub struct Plan {
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
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
    // Find the blocks table
    let block_table = metadata
        .tables
        .iter()
        .find(|(_, desc)| {
            desc.sort_key
                .first()
                .map(|s| s == &desc.block_number_column)
                .unwrap_or(false)
                && desc.item_order_keys.is_empty()
        })
        .map(|(name, _)| name.clone())
        .unwrap_or_else(|| "blocks".to_string());

    let block_table_desc = metadata.table(&block_table);
    let block_output_columns: Vec<String> = query
        .fields
        .get(&block_table)
        .map(|cols| order_columns_by_metadata(cols, block_table_desc))
        .unwrap_or_default();

    let mut table_plans = Vec::new();

    for (table_name, items) in &query.items {
        let table_desc = metadata
            .table(table_name)
            .ok_or_else(|| anyhow!("table '{}' not found in metadata", table_name))?;

        // Determine output columns, ordered by metadata column definition order
        let output_columns: Vec<String> = query
            .fields
            .get(table_name)
            .map(|cols| order_columns_by_metadata(cols, Some(table_desc)))
            .unwrap_or_default();

        // Compile each item into predicates
        let mut all_predicates = Vec::new();
        let mut all_relations: Vec<RelationPlan> = Vec::new();
        let mut seen_relations: HashSet<String> = HashSet::new();
        // Track source predicates per relation name, and how many items request each
        let mut rel_source_preds: std::collections::HashMap<String, Option<Vec<RowPredicate>>> =
            std::collections::HashMap::new();
        let mut rel_item_count: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let total_items = items.len();

        for item in items {
            let item_predicates = compile_item_predicates(item, table_desc)?;
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

                if seen_relations.contains(rel_name) {
                    continue;
                }
                seen_relations.insert(rel_name.clone());

                // Look up relation in table first, then in any alias
                let rel_def = table_desc
                    .relations
                    .get(rel_name)
                    .or_else(|| {
                        metadata
                            .query_aliases
                            .values()
                            .find(|a| a.table == *table_name)
                            .and_then(|a| a.relations.get(rel_name))
                    })
                    .ok_or_else(|| {
                        anyhow!("unknown relation '{}' for table '{}'", rel_name, table_name)
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

                all_relations.push(RelationPlan {
                    target_table: rel_def.table.clone(),
                    kind,
                    left_key: rel_def.effective_left_key().to_vec(),
                    right_key: rel_def.effective_right_key().to_vec(),
                    output_columns: target_output,
                    source_predicates: None, // filled in below
                });
            }
        }

        // Set source_predicates on each relation.
        // If ALL items request a relation, source_predicates = None (all primary rows qualify,
        // since the union of all items' predicates IS the primary scan predicate).
        for rel in &mut all_relations {
            for (rel_name, preds) in &rel_source_preds {
                if let Some(rel_def) = table_desc.relations.get(rel_name) {
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
        block_table,
        block_output_columns,
        table_plans,
    })
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
    for vf_name in desc.virtual_fields.keys() {
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

/// Compile a single query item's filters into one or more RowPredicates.
/// Returns multiple predicates when discriminator dispatches to multiple column lengths
/// (each length group becomes its own predicate, OR'd with others).
fn compile_item_predicates(
    item: &QueryItem,
    table: &TableDescription,
) -> Result<Vec<RowPredicate>> {
    let mut col_predicates: Vec<ColumnPredicate> = Vec::new();
    let mut discriminator_groups: Option<Vec<Vec<ColumnPredicate>>> = None;

    for (key, value) in &item.filters {
        // Special filters
        if let Some(special) = table.special_filters.get(key) {
            match special {
                SpecialFilter::Discriminator { columns } => {
                    let groups = compile_discriminator(value, columns)?;
                    discriminator_groups = groups;
                }
                SpecialFilter::BloomFilter {
                    column,
                    num_bytes,
                    num_hashes,
                } => {
                    let pred = compile_bloom_filter(value, column, *num_bytes, *num_hashes)?;
                    if let Some(p) = pred {
                        col_predicates.push(p);
                    }
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
                        anyhow!("alias target column '{}' not found", column)
                    })?;
                    if let Some(bool_val) = value.as_bool() {
                        col_predicates.push(col_eq(column, ScalarValue::Boolean(bool_val)));
                    } else if let Some(arr) = value.as_array() {
                        let pred = compile_in_list(column, arr, col_desc)?;
                        col_predicates.push(pred);
                    } else if let Some(n) = value.as_u64() {
                        col_predicates.push(col_eq(column, numeric_scalar(n, &col_desc.data_type)?));
                    }
                }
            }
            continue;
        }

        // Column filter
        let col_desc = table
            .column(key)
            .ok_or_else(|| anyhow!("column '{}' not found in table", key))?;

        if let Some(bool_val) = value.as_bool() {
            col_predicates.push(col_eq(key, ScalarValue::Boolean(bool_val)));
        } else if let Some(arr) = value.as_array() {
            let pred = compile_in_list(key, arr, col_desc)?;
            col_predicates.push(pred);
        } else if let Some(n) = value.as_u64() {
            col_predicates.push(col_eq(key, numeric_scalar(n, &col_desc.data_type)?));
        } else if let Some(s) = value.as_str() {
            col_predicates.push(col_eq(key, ScalarValue::Utf8(s.to_string())));
        } else {
            bail!(
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
        Ok(result)
    } else {
        Ok(vec![RowPredicate::new(col_predicates)])
    }
}

/// Compile an IN-list filter for a column based on its type.
fn compile_in_list(
    column: &str,
    values: &[serde_json::Value],
    col_desc: &ColumnDescription,
) -> Result<ColumnPredicate> {
    let strings: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();

    match &col_desc.data_type {
        ColumnType::String => {
            let vals: Vec<String> = if col_desc.json_encoding == Some(JsonEncoding::Hex) {
                strings.iter().map(|s| s.to_ascii_lowercase()).collect()
            } else {
                strings.iter().map(|s| s.to_string()).collect()
            };
            let str_refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
            Ok(col_in_list(
                column,
                Arc::new(StringArray::from(str_refs)) as Arc<dyn Array>,
            ))
        }
        ColumnType::UInt8 => {
            let vals: Vec<u8> = strings
                .iter()
                .filter_map(|s| parse_hex(s).filter(|b| b.len() == 1).map(|b| b[0]))
                .collect();
            Ok(col_in_list(
                column,
                Arc::new(UInt8Array::from(vals)) as Arc<dyn Array>,
            ))
        }
        ColumnType::UInt16 => {
            let vals: Vec<u16> = strings
                .iter()
                .filter_map(|s| {
                    parse_hex(s)
                        .filter(|b| b.len() == 2)
                        .map(|b| u16::from_be_bytes([b[0], b[1]]))
                })
                .collect();
            Ok(col_in_list(
                column,
                Arc::new(UInt16Array::from(vals)) as Arc<dyn Array>,
            ))
        }
        ColumnType::UInt32 => {
            let vals: Vec<u32> = strings
                .iter()
                .filter_map(|s| {
                    parse_hex(s)
                        .filter(|b| b.len() == 4)
                        .map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
                })
                .collect();
            Ok(col_in_list(
                column,
                Arc::new(UInt32Array::from(vals)) as Arc<dyn Array>,
            ))
        }
        ColumnType::UInt64 => {
            let vals: Vec<u64> = strings
                .iter()
                .filter_map(|s| {
                    parse_hex(s).filter(|b| b.len() == 8).map(|b| {
                        u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
                    })
                })
                .collect();
            Ok(col_in_list(
                column,
                Arc::new(UInt64Array::from(vals)) as Arc<dyn Array>,
            ))
        }
        ColumnType::FixedBinary(size) => {
            let vals: Vec<Vec<u8>> = strings
                .iter()
                .filter_map(|s| parse_hex(s).filter(|b| b.len() == *size))
                .collect();
            let mut builder = FixedSizeBinaryBuilder::with_capacity(vals.len(), *size as i32);
            for v in &vals {
                builder.append_value(v)?;
            }
            Ok(col_in_list(
                column,
                Arc::new(builder.finish()) as Arc<dyn Array>,
            ))
        }
        ColumnType::ListUInt32 => {
            // List-contains-any: values are integers (possibly JSON numbers)
            let vals: Vec<u32> = values
                .iter()
                .filter_map(|v| v.as_u64().map(|n| n as u32))
                .collect();
            Ok(col_list_contains_any_u32(column, vals))
        }
        ColumnType::ListString => {
            // List-contains-any: values are strings
            let vals: Vec<String> = values
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
            Ok(col_list_contains_any_string(column, vals))
        }
        _ => bail!(
            "unsupported column type {:?} for IN-list filter on '{}'",
            col_desc.data_type,
            column
        ),
    }
}

/// Compile a discriminator filter: dispatch hex prefixes to d1-d16 by length.
/// Returns None if an empty prefix is found (matches everything).
/// Returns groups of ColumnPredicates, one group per byte length.
fn compile_discriminator(
    value: &serde_json::Value,
    columns: &std::collections::BTreeMap<String, String>,
) -> Result<Option<Vec<Vec<ColumnPredicate>>>> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("discriminator values must be an array"))?;

    // Parse hex strings and group by byte length
    let mut by_length: std::collections::BTreeMap<usize, Vec<Vec<u8>>> =
        std::collections::BTreeMap::new();

    for v in arr {
        let s = v
            .as_str()
            .ok_or_else(|| anyhow!("discriminator value must be a string"))?;
        let bytes = parse_hex(s).ok_or_else(|| anyhow!("invalid hex in discriminator: {}", s))?;
        ensure!(
            bytes.len() <= 16,
            "discriminator max 16 bytes, got {}",
            bytes.len()
        );
        if bytes.is_empty() {
            return Ok(None); // empty prefix matches everything
        }
        by_length.entry(bytes.len()).or_default().push(bytes);
    }

    ensure!(!by_length.is_empty(), "discriminator list is empty");

    let mut groups = Vec::new();

    for (len, values) in by_length {
        let col_name = columns
            .get(&len.to_string())
            .ok_or_else(|| anyhow!("no discriminator column for length {}", len))?;

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
fn compile_bloom_filter(
    value: &serde_json::Value,
    column: &str,
    num_bytes: usize,
    num_hashes: usize,
) -> Result<Option<ColumnPredicate>> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow!("bloom filter values must be an array"))?;

    let needles: Vec<Vec<u8>> = arr
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.as_bytes().to_vec()))
        .collect();

    if needles.is_empty() {
        return Ok(None);
    }

    Ok(Some(col_bloom(column, needles, num_bytes, num_hashes)))
}

/// Compile a range >= filter.
fn compile_range_gte(
    value: &serde_json::Value,
    column: &str,
    table: &TableDescription,
) -> Result<Option<ColumnPredicate>> {
    let col_desc = table
        .column(column)
        .ok_or_else(|| anyhow!("range column '{}' not found", column))?;

    let n = value
        .as_u64()
        .ok_or_else(|| anyhow!("range filter value must be a number"))?;

    match col_desc.data_type {
        ColumnType::UInt64 => Ok(Some(ColumnPredicate {
            column: column.to_string(),
            predicate: Arc::new(crate::scan::predicate::RangeGtePredicate::new(
                ScalarValue::UInt64(n),
            )),
        })),
        _ => bail!("range filter only supports UInt64 columns"),
    }
}

/// Compile a range <= filter.
fn compile_range_lte(
    value: &serde_json::Value,
    column: &str,
    table: &TableDescription,
) -> Result<Option<ColumnPredicate>> {
    let col_desc = table
        .column(column)
        .ok_or_else(|| anyhow!("range column '{}' not found", column))?;

    let n = value
        .as_u64()
        .ok_or_else(|| anyhow!("range filter value must be a number"))?;

    match col_desc.data_type {
        ColumnType::UInt64 => Ok(Some(ColumnPredicate {
            column: column.to_string(),
            predicate: Arc::new(crate::scan::predicate::RangeLtePredicate::new(
                ScalarValue::UInt64(n),
            )),
        })),
        _ => bail!("range filter only supports UInt64 columns"),
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
    fn test_end_to_end_evm_logs_scan() {
        let meta = evm_metadata();
        let json = br#"{
            "type": "evm",
            "fromBlock": 0,
            "fields": {
                "log": { "address": true, "topic0": true }
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

        // Build a ScanRequest from the plan
        let output_cols: Vec<&str> = logs_plan
            .output_columns
            .iter()
            .map(|s| s.as_str())
            .collect();
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

        // Verify output only has the requested columns
        for batch in &batches {
            assert_eq!(batch.num_columns(), 2);
            assert!(batch.schema().field_with_name("address").is_ok());
            assert!(batch.schema().field_with_name("topic0").is_ok());
        }
    }

    #[test]
    fn test_numeric_scalar_type_dispatch() {
        assert!(matches!(
            numeric_scalar(42, &ColumnType::UInt8).unwrap(),
            ScalarValue::UInt8(42)
        ));
        assert!(matches!(
            numeric_scalar(1000, &ColumnType::UInt16).unwrap(),
            ScalarValue::UInt16(1000)
        ));
        assert!(matches!(
            numeric_scalar(100000, &ColumnType::UInt32).unwrap(),
            ScalarValue::UInt32(100000)
        ));
        assert!(matches!(
            numeric_scalar(u64::MAX, &ColumnType::UInt64).unwrap(),
            ScalarValue::UInt64(u64::MAX)
        ));
        // Out of range
        assert!(numeric_scalar(256, &ColumnType::UInt8).is_err());
        assert!(numeric_scalar(70000, &ColumnType::UInt16).is_err());
        assert!(numeric_scalar(u64::MAX, &ColumnType::UInt32).is_err());
    }

    /// Regression: numeric scalar filters were always compiled as UInt64,
    /// causing type mismatch on UInt32 columns → zero results.
    #[test]
    fn test_numeric_filter_on_uint32_column() {
        let meta = solana_metadata();
        // transaction_index is UInt32 in Solana metadata
        let json = br#"{
            "type": "solana",
            "fromBlock": 0,
            "fields": {
                "instruction": { "programId": true, "transactionIndex": true }
            },
            "instructions": [{
                "transactionIndex": 0
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

        // Must match instructions with transaction_index=0 (not empty!)
        assert!(
            total_rows > 0,
            "numeric filter on UInt32 column must match rows (was broken when always UInt64)"
        );
    }

    #[test]
    fn test_end_to_end_solana_instructions_scan() {
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
}
