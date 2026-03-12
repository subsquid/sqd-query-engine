use crate::metadata::{DatasetDescription, QueryAlias, TableDescription};
use anyhow::{anyhow, bail, ensure, Result};
use std::collections::HashMap;

/// A parsed query ready for compilation into an execution plan.
#[derive(Debug)]
pub struct Query {
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
    /// Field selections: table_name → ordered list of column names (snake_case).
    /// Order matches the query JSON for deterministic output key ordering.
    pub fields: HashMap<String, Vec<String>>,
    /// Filter items per table: table_name → items.
    pub items: HashMap<String, Vec<QueryItem>>,
}

/// A single filter item within a table request.
#[derive(Debug)]
pub struct QueryItem {
    /// Column or special filter entries: snake_case key → JSON value.
    pub filters: Vec<(String, serde_json::Value)>,
    /// Relation names requested (snake_case).
    pub relations: Vec<String>,
}

/// Convert camelCase to snake_case.
pub fn camel_to_snake(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(c.to_ascii_lowercase());
    }
    result
}

/// Parse a hex string like "0xaabb" to bytes.
pub fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let hex = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"))?;
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

const KNOWN_TOP_KEYS: &[&str] = &[
    "type",
    "fromBlock",
    "toBlock",
    "includeAllBlocks",
    "fields",
    "parentBlockHash",
];

/// Parse a JSON query against a dataset description.
pub fn parse_query(json_bytes: &[u8], metadata: &DatasetDescription) -> Result<Query> {
    let raw: serde_json::Value = serde_json::from_slice(json_bytes)?;
    let obj = raw
        .as_object()
        .ok_or_else(|| anyhow!("query must be a JSON object"))?;

    // Validate type
    let dataset_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing 'type' field"))?;
    ensure!(
        dataset_type == metadata.name,
        "query type '{}' doesn't match metadata '{}'",
        dataset_type,
        metadata.name
    );

    // Block range
    let from_block = obj.get("fromBlock").and_then(|v| v.as_u64()).unwrap_or(0);
    let to_block = obj.get("toBlock").and_then(|v| v.as_u64());
    if let Some(to) = to_block {
        ensure!(from_block <= to, "'toBlock' must be >= 'fromBlock'");
    }

    let include_all_blocks = obj
        .get("includeAllBlocks")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Build lookup maps: query_name → table_name, field_name → table_name
    let query_name_to_table: HashMap<&str, &str> = metadata
        .tables
        .iter()
        .filter_map(|(name, desc)| desc.query_name.as_deref().map(|qn| (qn, name.as_str())))
        .collect();

    let field_name_to_table: HashMap<&str, &str> = metadata
        .tables
        .iter()
        .filter_map(|(name, desc)| desc.field_name.as_deref().map(|fn_| (fn_, name.as_str())))
        .collect();

    // Parse fields
    let fields = parse_fields(obj.get("fields"), metadata, &field_name_to_table)?;

    // Parse table filter arrays
    let mut items: HashMap<String, Vec<QueryItem>> = HashMap::new();

    for (key, value) in obj {
        if KNOWN_TOP_KEYS.contains(&key.as_str()) {
            continue;
        }

        // Resolve table name (try query_name, snake_case, then query_aliases)
        let table_name = query_name_to_table.get(key.as_str()).copied().or_else(|| {
            let snake = camel_to_snake(key);
            if metadata.tables.contains_key(&snake) {
                None
            } else {
                None
            }
        });
        let snake_key = camel_to_snake(key);
        let alias_name: Option<&str> = None;
        let (table_name, alias_name) = if let Some(tn) = table_name {
            (tn, alias_name)
        } else if metadata.tables.contains_key(&snake_key) {
            (snake_key.as_str(), alias_name)
        } else if let Some(alias) = metadata.query_aliases.get(key.as_str()).or_else(|| {
            metadata.query_aliases.get(&snake_key)
        }) {
            // Query alias → resolve to the real table
            (alias.table.as_str(), Some(key.as_str()))
        } else {
            return Err(anyhow!("unknown table filter '{}' in query", key));
        };

        let table_desc = metadata.table(table_name).unwrap();
        let alias_def = alias_name.and_then(|an| {
            metadata
                .query_aliases
                .get(an)
                .or_else(|| metadata.query_aliases.get(&camel_to_snake(an)))
        });

        let arr = value
            .as_array()
            .ok_or_else(|| anyhow!("'{}' must be an array of filter items", key))?;

        let mut table_items = Vec::new();
        for item_value in arr {
            let item = parse_query_item(item_value, table_desc, table_name, alias_def)?;
            table_items.push(item);
        }

        items
            .entry(table_name.to_string())
            .or_default()
            .extend(table_items);
    }

    // Validate total item count
    let total_items: usize = items.values().map(|v| v.len()).sum();
    ensure!(
        total_items <= 100,
        "query contains {} item requests, max 100 allowed",
        total_items
    );

    Ok(Query {
        from_block,
        to_block,
        include_all_blocks,
        fields,
        items,
    })
}

fn parse_fields(
    fields_value: Option<&serde_json::Value>,
    _metadata: &DatasetDescription,
    field_name_to_table: &HashMap<&str, &str>,
) -> Result<HashMap<String, Vec<String>>> {
    let mut result = HashMap::new();

    let Some(fields_obj) = fields_value.and_then(|v| v.as_object()) else {
        return Ok(result);
    };

    for (key, value) in fields_obj {
        // Resolve field_name to table_name
        let table_name = field_name_to_table
            .get(key.as_str())
            .copied()
            .ok_or_else(|| anyhow!("unknown field group '{}' in query", key))?;

        let field_obj = value
            .as_object()
            .ok_or_else(|| anyhow!("fields.{} must be an object", key))?;

        // Preserve insertion order from JSON for deterministic output key ordering
        let columns: Vec<String> = field_obj
            .iter()
            .filter(|(_, v)| v.as_bool() == Some(true))
            .map(|(k, _)| camel_to_snake(k))
            .collect();

        result.insert(table_name.to_string(), columns);
    }

    Ok(result)
}

fn parse_query_item(
    value: &serde_json::Value,
    table: &TableDescription,
    table_name: &str,
    alias: Option<&QueryAlias>,
) -> Result<QueryItem> {
    let obj = value
        .as_object()
        .ok_or_else(|| anyhow!("filter item must be a JSON object"))?;

    let mut filters = Vec::new();
    let mut relations = Vec::new();

    // Alias-defined relations
    let alias_relations = alias.map(|a| &a.relations);

    for (key, val) in obj {
        let snake_key = camel_to_snake(key);

        // Check if it's a relation flag (boolean true + known relation)
        let is_relation = table.relations.contains_key(&snake_key)
            || alias_relations
                .map(|r| r.contains_key(&snake_key))
                .unwrap_or(false);
        if val.as_bool() == Some(true) && is_relation {
            relations.push(snake_key);
            continue;
        }
        if val.as_bool() == Some(false) && is_relation {
            continue;
        }

        // Check alias filter aliases (e.g., topic0 → _evm_log_topic0)
        if let Some(alias_def) = alias {
            if let Some(real_col) = alias_def.filter_aliases.get(&snake_key) {
                filters.push((real_col.clone(), val.clone()));
                continue;
            }
        }

        // Check if it's a special filter
        if table.special_filters.contains_key(&snake_key) {
            filters.push((snake_key, val.clone()));
            continue;
        }

        // Must be a column filter
        if table.columns.contains_key(&snake_key) {
            filters.push((snake_key, val.clone()));
            continue;
        }

        bail!(
            "unknown filter '{}' (resolved: '{}') for table '{}'",
            key,
            snake_key,
            table_name
        );
    }

    // Add implicit predicates from alias (e.g., name: ["EVM.Log"])
    if let Some(alias_def) = alias {
        for (col_name, values) in &alias_def.implicit_predicates {
            let json_values: Vec<serde_json::Value> =
                values.iter().map(|v| serde_json::Value::String(v.clone())).collect();
            filters.push((col_name.clone(), serde_json::Value::Array(json_values)));
        }
    }

    Ok(QueryItem { filters, relations })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::load_dataset_description;
    use std::path::Path;

    fn solana_metadata() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/solana.yaml")).unwrap()
    }

    fn evm_metadata() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/evm.yaml")).unwrap()
    }

    #[test]
    fn test_camel_to_snake() {
        assert_eq!(camel_to_snake("programId"), "program_id");
        assert_eq!(camel_to_snake("transactionIndex"), "transaction_index");
        assert_eq!(camel_to_snake("innerInstructions"), "inner_instructions");
        assert_eq!(camel_to_snake("isCommitted"), "is_committed");
        assert_eq!(camel_to_snake("l1BlockNumber"), "l1_block_number");
        assert_eq!(camel_to_snake("d8"), "d8");
        assert_eq!(camel_to_snake("a0"), "a0");
        assert_eq!(camel_to_snake("feePayer"), "fee_payer");
    }

    #[test]
    fn test_parse_hex() {
        assert_eq!(parse_hex("0xab"), Some(vec![0xab]));
        assert_eq!(
            parse_hex("0xf8c69e91e17587c8"),
            Some(vec![0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8])
        );
        assert_eq!(parse_hex("invalid"), None);
        assert_eq!(parse_hex("0xabc"), None); // odd length
    }

    #[test]
    fn test_parse_evm_logs_query() {
        let meta = evm_metadata();
        let json = br#"{
            "type": "evm",
            "fromBlock": 17881390,
            "toBlock": 17882786,
            "fields": {
                "log": { "address": true, "data": true, "logIndex": true }
            },
            "logs": [{
                "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
                "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                "transaction": true
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        assert_eq!(query.from_block, 17881390);
        assert_eq!(query.to_block, Some(17882786));
        assert!(!query.include_all_blocks);

        // Check fields
        let log_fields = query.fields.get("logs").unwrap();
        assert!(log_fields.contains(&"address".to_string()));
        assert!(log_fields.contains(&"data".to_string()));
        assert!(log_fields.contains(&"log_index".to_string()));

        // Check items
        let log_items = query.items.get("logs").unwrap();
        assert_eq!(log_items.len(), 1);
        assert_eq!(log_items[0].relations, vec!["transaction"]);
        assert_eq!(log_items[0].filters.len(), 2);
    }

    #[test]
    fn test_parse_solana_instructions_query() {
        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 0,
            "fields": {
                "instruction": { "programId": true, "data": true, "accounts": true }
            },
            "instructions": [{
                "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
                "d8": ["0xf8c69e91e17587c8"],
                "transaction": true,
                "innerInstructions": true
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        assert_eq!(query.from_block, 0);
        assert_eq!(query.to_block, None);

        let instr_items = query.items.get("instructions").unwrap();
        assert_eq!(instr_items.len(), 1);

        let item = &instr_items[0];
        // programId and d8 are column filters
        assert_eq!(item.filters.len(), 2);
        // transaction and innerInstructions are relations
        assert!(item.relations.contains(&"transaction".to_string()));
        assert!(item.relations.contains(&"inner_instructions".to_string()));
    }

    #[test]
    fn test_parse_solana_discriminator_query() {
        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 0,
            "instructions": [{
                "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
                "discriminator": ["0xf8c69e91e17587c8"]
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let item = &query.items.get("instructions").unwrap()[0];
        // discriminator is a special filter
        let disc_filter = item
            .filters
            .iter()
            .find(|(k, _)| k == "discriminator")
            .unwrap();
        assert!(disc_filter.1.is_array());
    }

    #[test]
    fn test_parse_unknown_table_error() {
        let meta = evm_metadata();
        let json = br#"{
            "type": "evm",
            "fromBlock": 0,
            "unicorns": [{ "color": ["rainbow"] }]
        }"#;
        assert!(parse_query(json, &meta).is_err());
    }

    #[test]
    fn test_parse_unknown_filter_error() {
        let meta = evm_metadata();
        let json = br#"{
            "type": "evm",
            "fromBlock": 0,
            "logs": [{ "nonexistentField": ["value"] }]
        }"#;
        assert!(parse_query(json, &meta).is_err());
    }

    #[test]
    fn test_parse_item_count_limit() {
        let meta = evm_metadata();
        // Build a query with 101 items
        let mut items = Vec::new();
        for _ in 0..101 {
            items.push(serde_json::json!({}));
        }
        let json = serde_json::json!({
            "type": "evm",
            "fromBlock": 0,
            "logs": items
        });
        let result = parse_query(json.to_string().as_bytes(), &meta);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("100"));
    }

    #[test]
    fn test_parse_block_range_validation() {
        let meta = evm_metadata();
        let json = br#"{
            "type": "evm",
            "fromBlock": 100,
            "toBlock": 50
        }"#;
        assert!(parse_query(json, &meta).is_err());
    }
}
