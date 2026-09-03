use crate::error::ErrorKind;
use crate::metadata::{DatasetDescription, QueryAlias, TableDescription};
use crate::{engine_bail, engine_ensure, engine_err};
use anyhow::Result;
use std::collections::HashMap;

/// A parsed query ready for compilation into an execution plan.
#[derive(Debug)]
pub struct Query {
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub include_all_blocks: bool,
    /// Hash the client believes the block before `from_block` has. When set, a
    /// mismatch against the chunk means the chain reorganised between pages.
    pub parent_block_hash: Option<String>,
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

/// Whether `s` is a well-formed hex literal: `0x`/`0X` prefix, an even number of
/// digits, and nothing but ASCII hex digits (INV-Q12).
///
/// Checks bytes rather than characters: a filter value arrives from the network
/// and may hold any UTF-8, and a multi-byte character is never a hex digit.
pub fn is_hex_literal(s: &str) -> bool {
    let Some(digits) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) else {
        return false;
    };

    digits.len() % 2 == 0 && digits.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Parse a hex string like "0xaabb" to bytes, or `None` when it is not one.
///
/// The digit check is separate from the conversion because `from_str_radix`
/// accepts a sign: it reads `"+1"` as 1, so `"0x+1+2"` would otherwise parse to
/// the bytes of `"0x0102"` — a value the client never typed. Callers treat a
/// `Some` as proof of well-formedness.
pub fn parse_hex(s: &str) -> Option<Vec<u8>> {
    if !is_hex_literal(s) {
        return None;
    }

    let digits = &s.as_bytes()[2..];
    let mut bytes = vec![0u8; digits.len() / 2];
    faster_hex::hex_decode(digits, &mut bytes).ok()?;

    Some(bytes)
}

/// `P-MAX-ITEM-REQUESTS` (spec/09-parameters.md §9.1). Each item request is an
/// independent scan of its table.
const MAX_ITEM_REQUESTS: usize = 100;

/// `P-MAX-REQUEST-BYTES` (spec/09-parameters.md §9.1). Checked against the raw
/// body, before it is parsed: the parse itself is the first thing a large
/// request makes the engine pay for.
const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

/// `P-MAX-IN-LIST` (spec/09-parameters.md §9.1). A filter list becomes a hash set
/// built before a single row is read, and the request cap alone does not bound
/// it: a hundred item requests each carrying a million addresses is well-formed
/// under every other rule (INV-Q13).
const MAX_IN_LIST: usize = 100_000;

const KNOWN_TOP_KEYS: &[&str] = &[
    "type",
    "fromBlock",
    "toBlock",
    "includeAllBlocks",
    "fields",
    "parentBlockHash",
];

/// A block bound is an unsigned 64-bit integer or absent. A present-but-malformed
/// bound is an error rather than a default: coercing `{"fromBlock": "18000000"}`
/// to genesis answers a different question than the one asked, and says nothing
/// about it.
fn parse_block_bound(value: Option<&serde_json::Value>, key: &str) -> Result<Option<u64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value.as_u64().map(Some).ok_or_else(|| {
        engine_err!(
            ErrorKind::InvalidBlockNumber,
            "'{}' must be an unsigned integer, got {}",
            key,
            value
        )
    })
}

/// Parse a JSON query against a dataset description.
pub fn parse_query(json_bytes: &[u8], metadata: &DatasetDescription) -> Result<Query> {
    engine_ensure!(
        json_bytes.len() <= MAX_REQUEST_BYTES,
        ErrorKind::RequestTooLarge,
        "request is {} bytes, at most {} allowed",
        json_bytes.len(),
        MAX_REQUEST_BYTES
    );

    let raw: serde_json::Value = serde_json::from_slice(json_bytes)
        .map_err(|e| engine_err!(ErrorKind::MalformedRequest, "request is not JSON: {}", e))?;
    let obj = raw
        .as_object()
        .ok_or_else(|| engine_err!(ErrorKind::MalformedRequest, "query must be a JSON object"))?;

    // Validate type
    let dataset_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| engine_err!(ErrorKind::UnknownDataset, "missing 'type' field"))?;
    engine_ensure!(
        dataset_type == metadata.name,
        ErrorKind::UnknownDataset,
        "query type '{}' doesn't match metadata '{}'",
        dataset_type,
        metadata.name
    );

    // Block range
    let from_block = parse_block_bound(obj.get("fromBlock"), "fromBlock")?.unwrap_or(0);
    let to_block = parse_block_bound(obj.get("toBlock"), "toBlock")?;
    if let Some(to) = to_block {
        engine_ensure!(
            from_block <= to,
            ErrorKind::InvalidBlockRange,
            "'toBlock' must be >= 'fromBlock'"
        );
    }

    // A wrong type here silently picks one of two very different answers — every
    // block in range, or only the blocks with matches — so it is refused, like
    // the block bounds next to it.
    let include_all_blocks = match obj.get("includeAllBlocks") {
        None | Some(serde_json::Value::Null) => false,
        Some(v) => v.as_bool().ok_or_else(|| {
            engine_err!(
                ErrorKind::MalformedRequest,
                "'includeAllBlocks' must be a boolean, got {}",
                v
            )
        })?,
    };

    let parent_block_hash = match obj.get("parentBlockHash") {
        None | Some(serde_json::Value::Null) => None,
        Some(v) => Some(
            v.as_str()
                .ok_or_else(|| {
                    engine_err!(
                        ErrorKind::MalformedRequest,
                        "'parentBlockHash' must be a string, got {}",
                        v
                    )
                })?
                .to_string(),
        ),
    };

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
        let table_name = query_name_to_table.get(key.as_str()).copied();
        let snake_key = camel_to_snake(key);
        let alias_name: Option<&str> = None;
        let (table_name, alias_name) = if let Some(tn) = table_name {
            (tn, alias_name)
        } else if metadata.tables.contains_key(&snake_key) {
            (snake_key.as_str(), alias_name)
        } else if let Some(alias) = metadata
            .query_aliases
            .get(key.as_str())
            .or_else(|| metadata.query_aliases.get(&snake_key))
        {
            // Query alias → resolve to the real table
            (alias.table.as_str(), Some(key.as_str()))
        } else {
            engine_bail!(
                ErrorKind::UnknownTable,
                "unknown table filter '{}' in query",
                key
            );
        };

        let table_desc = metadata.table(table_name).unwrap();
        let alias_def = alias_name.and_then(|an| {
            metadata
                .query_aliases
                .get(an)
                .or_else(|| metadata.query_aliases.get(&camel_to_snake(an)))
        });

        let arr = value.as_array().ok_or_else(|| {
            engine_err!(
                ErrorKind::MalformedRequest,
                "'{}' must be an array of filter items",
                key
            )
        })?;

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
    engine_ensure!(
        total_items <= MAX_ITEM_REQUESTS,
        ErrorKind::TooManyItemRequests,
        "query contains {} item requests, max {} allowed",
        total_items,
        MAX_ITEM_REQUESTS
    );

    Ok(Query {
        from_block,
        to_block,
        include_all_blocks,
        parent_block_hash,
        fields,
        items,
    })
}

fn parse_fields(
    fields_value: Option<&serde_json::Value>,
    metadata: &DatasetDescription,
    field_name_to_table: &HashMap<&str, &str>,
) -> Result<HashMap<String, Vec<String>>> {
    let mut result = HashMap::new();

    // A present-but-wrong `fields` is refused rather than read as absent:
    // `"fields": []` would otherwise answer 200 with every projection the client
    // asked for missing, which reads as "this dataset has no such columns".
    if let Some(value) = fields_value {
        engine_ensure!(
            value.is_null() || value.is_object(),
            ErrorKind::MalformedRequest,
            "'fields' must be an object, got {}",
            value
        );
    }

    let Some(fields_obj) = fields_value.and_then(|v| v.as_object()) else {
        return Ok(result);
    };

    for (key, value) in fields_obj {
        // Resolve field_name to table_name
        let table_name = field_name_to_table
            .get(key.as_str())
            .copied()
            .ok_or_else(|| {
                engine_err!(
                    ErrorKind::UnknownFieldGroup,
                    "unknown field group '{}' in query",
                    key
                )
            })?;

        let field_obj = value.as_object().ok_or_else(|| {
            engine_err!(
                ErrorKind::MalformedRequest,
                "fields.{} must be an object",
                key
            )
        })?;

        let table_desc = metadata.table(table_name).ok_or_else(|| {
            engine_err!(
                ErrorKind::UnknownFieldGroup,
                "field group '{}' targets unknown table",
                key
            )
        })?;

        // Preserve insertion order from JSON for deterministic output key ordering
        let mut columns = Vec::new();
        for (field_key, selected) in field_obj {
            let column = camel_to_snake(field_key);

            // A misspelled name is rejected whether or not it was switched on:
            // `{"logIndx": false}` is as much a typo as `{"logIndx": true}`, and
            // answering it with a 200 sends the client looking for the bug
            // everywhere except in its own request.
            engine_ensure!(
                table_desc.is_selectable_field(&column),
                ErrorKind::UnknownField,
                "unknown field '{}' in fields.{}",
                field_key,
                key
            );

            // A selector is a boolean. `{"logIndex": 1}` is as much a mistake as
            // `{"logIndx": true}`, and treating it as "not selected" answers with
            // a 200 that is missing a column the client asked for.
            let selected = selected.as_bool().ok_or_else(|| {
                engine_err!(
                    ErrorKind::MalformedRequest,
                    "field '{}' in fields.{} must be a boolean, got {}",
                    field_key,
                    key,
                    selected
                )
            })?;

            if selected {
                columns.push(column);
            }
        }

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
    let obj = value.as_object().ok_or_else(|| {
        engine_err!(
            ErrorKind::MalformedRequest,
            "filter item must be a JSON object"
        )
    })?;

    let mut filters = Vec::new();
    let mut relations = Vec::new();

    // Alias-defined relations
    let alias_relations = alias.map(|a| &a.relations);

    for (key, val) in obj {
        let snake_key = camel_to_snake(key);

        // Every filter kind that takes a list turns it into a set before any
        // data is read, so the bound is on the list itself rather than on the
        // kind the key resolves to.
        if let Some(values) = val.as_array() {
            engine_ensure!(
                values.len() <= MAX_IN_LIST,
                ErrorKind::RequestTooLarge,
                "filter '{}' carries {} values, at most {} allowed",
                key,
                values.len(),
                MAX_IN_LIST
            );
        }

        // A known relation is a boolean flag. Diagnose a malformed value as
        // such instead of letting it fall through to the unknown-filter path.
        let is_relation = table.relations.contains_key(&snake_key)
            || alias_relations
                .map(|r| r.contains_key(&snake_key))
                .unwrap_or(false);
        if is_relation {
            let enabled = val.as_bool().ok_or_else(|| {
                engine_err!(
                    ErrorKind::InvalidFilterValue,
                    "relation '{}' of table '{}' must be a boolean",
                    key,
                    table_name
                )
            })?;
            if enabled {
                relations.push(snake_key);
            }
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

        // A declared column filter. The list is what the table (or, when the
        // request came through an alias, the alias) says is filterable — not
        // whatever columns happen to exist. Tables carry blooms, size counters
        // and denormalised extractions that are no client's business, and
        // filtering on any column would make the column list the public API.
        let declared = match alias {
            Some(alias_def) => &alias_def.filters,
            None => &table.filters,
        };
        if declared.contains(&snake_key) {
            engine_ensure!(
                table.columns.contains_key(&snake_key),
                ErrorKind::UnknownFilter,
                "filter '{}' of table '{}' names no column",
                snake_key,
                table_name
            );
            filters.push((snake_key, val.clone()));
            continue;
        }

        engine_bail!(
            ErrorKind::UnknownFilter,
            "unknown filter '{}' (resolved: '{}') for table '{}'",
            key,
            snake_key,
            table_name
        );
    }

    // Add implicit predicates from alias (e.g., name: ["EVM.Log"])
    if let Some(alias_def) = alias {
        for (col_name, values) in &alias_def.implicit_predicates {
            let json_values: Vec<serde_json::Value> = values
                .iter()
                .map(|v| serde_json::Value::String(v.clone()))
                .collect();
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

    /// `from_str_radix` reads a sign, so a digit pair of `+1` used to parse as 1
    /// and `"0x+1+2"` as the bytes of `"0x0102"`. Callers take a `Some` here as
    /// proof of well-formedness, so that hands them a value the client never
    /// typed — a discriminator filter matching a different discriminator.
    #[test]
    fn test_parse_hex_rejects_a_sign_in_a_digit_pair() {
        assert_eq!(u8::from_str_radix("+1", 16), Ok(1), "the trap this guards");

        for malformed in ["0x+1", "0x+1+2", "0x-1", "0x 1", "0x+f"] {
            assert_eq!(parse_hex(malformed), None, "{malformed} is not hex");
            assert!(!is_hex_literal(malformed), "{malformed} is not hex");
        }
    }

    /// A filter value arrives from the network and may hold any UTF-8. Walking it
    /// two bytes at a time must not slice a multi-byte character in half.
    #[test]
    fn test_parse_hex_survives_multi_byte_characters() {
        for malformed in ["0xé", "0xaé", "0x\u{1F600}", "0xабвг"] {
            assert_eq!(parse_hex(malformed), None, "{malformed:?} is not hex");
        }
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

    #[test]
    fn test_table_name_fallback_resolution() {
        // "blocks" has no query_name in EVM metadata, so it resolves via the
        // snake_case table name fallback (line 128), not via query_name lookup.
        let meta = evm_metadata();
        assert!(meta.tables.get("blocks").unwrap().query_name.is_none());
        let json = br#"{
            "type": "evm",
            "fromBlock": 0,
            "blocks": [{}]
        }"#;
        let query = parse_query(json, &meta).unwrap();
        assert!(query.items.contains_key("blocks"));
    }

    /// INV-Q4: a present-but-malformed block bound is an error, never
    /// coerced. `{"fromBlock": "18000000"}` is a common client bug; silently
    /// scanning from genesis hides it.
    #[test]
    fn test_malformed_block_bounds_error() {
        let meta = evm_metadata();
        let bad = [
            br#"{"type":"evm","fromBlock":"18000000"}"#.to_vec(),
            br#"{"type":"evm","fromBlock":"abc"}"#.to_vec(),
            br#"{"type":"evm","fromBlock":-1}"#.to_vec(),
            br#"{"type":"evm","fromBlock":1.5}"#.to_vec(),
            br#"{"type":"evm","fromBlock":1e30}"#.to_vec(),
            br#"{"type":"evm","toBlock":-1}"#.to_vec(),
            br#"{"type":"evm","toBlock":"100"}"#.to_vec(),
            br#"{"type":"evm","toBlock":[]}"#.to_vec(),
        ];
        for json in &bad {
            assert!(
                parse_query(json, &meta).is_err(),
                "expected error for {}",
                std::str::from_utf8(json).unwrap()
            );
        }
    }

    /// The defaults survive: absent means genesis / unbounded (INV-Q9), and an
    /// explicit `null` is treated as absent rather than as a malformed value.
    #[test]
    fn test_block_bounds_defaults_and_null() {
        let meta = evm_metadata();
        let q = parse_query(br#"{"type":"evm"}"#, &meta).unwrap();
        assert_eq!(q.from_block, 0);
        assert_eq!(q.to_block, None);

        let q = parse_query(br#"{"type":"evm","fromBlock":null,"toBlock":null}"#, &meta).unwrap();
        assert_eq!(q.from_block, 0);
        assert_eq!(q.to_block, None);
    }

    /// INV-P14: a well-formed bound beyond the chunk's physical range matches
    /// nothing — it is not a parse error.
    #[test]
    fn test_out_of_chunk_range_block_bound_is_not_an_error() {
        let meta = evm_metadata();
        let q = parse_query(br#"{"type":"evm","fromBlock":1099511627776}"#, &meta).unwrap();
        assert_eq!(q.from_block, 1_099_511_627_776);
    }
}
