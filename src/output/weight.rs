use crate::metadata::{DatasetDescription, TableDescription, VirtualField, WeightSource};
use crate::query::Plan;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// Maximum response size in bytes (20 MB).
const MAX_RESPONSE_BYTES: u64 = 20 * 1024 * 1024;

/// Default weight per row when no weight column is specified.
const DEFAULT_ROW_WEIGHT: u64 = 32;

/// Table data: the query-driven items and any relation results.
pub(crate) struct TableOutput {
    /// The primary table's filtered rows.
    pub(crate) batches: Vec<RecordBatch>,
    /// Relation results keyed by target table name.
    pub(crate) relation_batches: HashMap<String, Vec<RecordBatch>>,
}

/// A batch source with its weight parameters.
struct WeightContribution<'a> {
    batches: &'a [RecordBatch],
    fixed_weight: u64,
    weight_cols: Vec<String>,
}

/// Apply weight-based limit: select blocks until cumulative weight exceeds MAX_RESPONSE_BYTES.
///
/// Weight is computed per target table with row deduplication, matching legacy behavior:
/// - Direct scan results and relation results targeting the same table are merged
/// - Duplicate rows (same block_number + item_order_keys) are counted only once
pub(crate) fn apply_weight_limit(
    sorted_blocks: &[u64],
    table_outputs: &HashMap<String, TableOutput>,
    block_batches: &[RecordBatch],
    metadata: &DatasetDescription,
    plan: &Plan,
) -> Vec<u64> {
    if sorted_blocks.is_empty() {
        return Vec::new();
    }

    // 1. Group all batch contributions by TARGET table name.
    // Direct batches → target is the table_plan's own table.
    // Relation batches → target is the relation's target_table.
    let mut target_contribs: HashMap<&str, Vec<WeightContribution>> = HashMap::new();

    for table_plan in &plan.table_plans {
        let output = match table_outputs.get(&table_plan.table) {
            Some(o) => o,
            None => continue,
        };
        let table_desc = metadata.table(&table_plan.table);

        let weight_cols = weight_projection(&table_plan.output_columns, table_desc);
        let (fixed_weight, weight_col_names) = compute_weight_params(&weight_cols, table_desc);

        target_contribs
            .entry(&table_plan.table)
            .or_default()
            .push(WeightContribution {
                batches: &output.batches,
                fixed_weight,
                weight_cols: weight_col_names,
            });

        for rel in &table_plan.relations {
            if let Some(rel_batches) = output.relation_batches.get(&rel.target_table) {
                let rel_desc = metadata.table(&rel.target_table);
                let rel_weight_cols = weight_projection(&rel.output_columns, rel_desc);
                let (rel_fixed, rel_weight_col_names) =
                    compute_weight_params(&rel_weight_cols, rel_desc);

                target_contribs
                    .entry(&rel.target_table)
                    .or_default()
                    .push(WeightContribution {
                        batches: rel_batches,
                        fixed_weight: rel_fixed,
                        weight_cols: rel_weight_col_names,
                    });
            }
        }
    }

    // 2. For each target table, compute deduplicated per-block weights.
    let mut block_weights: FxHashMap<u64, u64> = FxHashMap::default();

    for (table_name, contribs) in &target_contribs {
        let table_desc = metadata.table(table_name);
        let bn_col_name = table_desc
            .map(|d| d.block_number_column.as_str())
            .unwrap_or("block_number");
        let dedup_keys: Vec<&str> = table_desc
            .map(|d| d.item_order_keys.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();

        if contribs.len() == 1 {
            // Single source — no dedup needed, fast path.
            accumulate_block_weights(
                contribs[0].batches,
                bn_col_name,
                contribs[0].fixed_weight,
                &contribs[0].weight_cols,
                &mut block_weights,
            );
        } else {
            // Multiple sources — deduplicate by (block_number, item_order_key_hash).
            let mut seen: FxHashSet<(u64, u64)> = FxHashSet::default();

            for contrib in contribs {
                accumulate_block_weights_dedup(
                    contrib.batches,
                    bn_col_name,
                    &dedup_keys,
                    contrib.fixed_weight,
                    &contrib.weight_cols,
                    &mut block_weights,
                    &mut seen,
                );
            }
        }
    }

    // 3. Add block header weight.
    let block_desc = metadata.table(&plan.block_table);
    let (header_fixed, header_weight_cols) =
        compute_weight_params(&plan.block_output_columns, block_desc);
    let header_bn_col = block_desc
        .map(|d| d.block_number_column.as_str())
        .unwrap_or("number");

    if plan.include_all_blocks {
        accumulate_block_weights(
            block_batches,
            header_bn_col,
            header_fixed,
            &header_weight_cols,
            &mut block_weights,
        );
    } else {
        let blocks_with_items: HashSet<u64> = block_weights.keys().copied().collect();
        for batch in block_batches {
            let bn_col = match batch.column_by_name(header_bn_col) {
                Some(c) => c,
                None => continue,
            };
            for i in 0..batch.num_rows() {
                if let Some(block_num) = get_block_number(bn_col.as_ref(), i) {
                    if blocks_with_items.contains(&block_num) {
                        *block_weights.entry(block_num).or_default() += header_fixed;
                    }
                }
            }
        }
    }

    // 4. Select blocks by cumulative weight.
    let mut cumulative_weight: u64 = 0;
    let mut selected = Vec::new();

    for &block_num in sorted_blocks {
        let block_weight = block_weights.get(&block_num).copied().unwrap_or(0);
        cumulative_weight += block_weight;

        if selected.is_empty() || cumulative_weight <= MAX_RESPONSE_BYTES {
            selected.push(block_num);
        } else {
            break;
        }
    }

    selected
}

/// Compute fixed weight per row and list of weight columns for weight limiting.
fn compute_weight_params(
    output_columns: &[String],
    table_desc: Option<&TableDescription>,
) -> (u64, Vec<String>) {
    let desc = match table_desc {
        Some(d) => d,
        None => return (DEFAULT_ROW_WEIGHT, Vec::new()),
    };

    let mut projected: HashSet<&str> = HashSet::new();
    for col_name in output_columns {
        if let Some(vf) = desc.virtual_fields.get(col_name.as_str()) {
            match vf {
                VirtualField::Roll { columns } => {
                    for c in columns {
                        projected.insert(c.as_str());
                    }
                }
            }
        } else if desc.columns.contains_key(col_name.as_str()) {
            projected.insert(col_name.as_str());
        }
    }

    let mut fixed_weight: u64 = 0;
    let mut weight_cols = Vec::new();

    for &col_name in &projected {
        let col_desc = match desc.columns.get(col_name) {
            Some(c) => c,
            None => continue,
        };
        if col_desc.system {
            continue;
        }
        match &col_desc.weight {
            Some(WeightSource::Column(wc)) => {
                weight_cols.push(wc.clone());
            }
            Some(WeightSource::Fixed(w)) => {
                fixed_weight += w;
            }
            None => {
                fixed_weight += DEFAULT_ROW_WEIGHT;
            }
        }
    }

    (fixed_weight, weight_cols)
}

/// Get block number from an array at row index.
fn get_block_number(col: &dyn arrow::array::Array, i: usize) -> Option<u64> {
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        Some(a.value(i))
    } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        Some(a.value(i) as u64)
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        Some(a.value(i) as u64)
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        Some(a.value(i) as u64)
    } else {
        None
    }
}

/// Get uint64 value from a weight column at row index.
fn get_weight_value(col: &dyn arrow::array::Array, i: usize) -> u64 {
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        a.value(i)
    } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        a.value(i) as u64
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        a.value(i) as u64
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        a.value(i) as u64
    } else {
        0
    }
}

/// Accumulate per-block weights in a single pass (no dedup, fast path).
fn accumulate_block_weights(
    batches: &[RecordBatch],
    bn_column: &str,
    fixed_weight_per_row: u64,
    weight_columns: &[String],
    weights: &mut FxHashMap<u64, u64>,
) {
    for batch in batches {
        let bn_col = match batch.column_by_name(bn_column) {
            Some(c) => c,
            None => continue,
        };

        let wc_arrays: Vec<Option<&dyn arrow::array::Array>> = weight_columns
            .iter()
            .map(|name| batch.column_by_name(name).map(|c| c.as_ref()))
            .collect();

        for i in 0..batch.num_rows() {
            if let Some(block_num) = get_block_number(bn_col.as_ref(), i) {
                let mut row_weight = fixed_weight_per_row;
                for wc in &wc_arrays {
                    if let Some(arr) = wc {
                        row_weight += get_weight_value(*arr, i);
                    }
                }
                *weights.entry(block_num).or_default() += row_weight;
            }
        }
    }
}

/// Accumulate per-block weights with row deduplication.
/// Rows are identified by (block_number, hash of item_order_key columns).
/// Duplicate rows (already in `seen`) are skipped.
fn accumulate_block_weights_dedup(
    batches: &[RecordBatch],
    bn_column: &str,
    dedup_key_columns: &[&str],
    fixed_weight_per_row: u64,
    weight_columns: &[String],
    weights: &mut FxHashMap<u64, u64>,
    seen: &mut FxHashSet<(u64, u64)>,
) {
    for batch in batches {
        let bn_col = match batch.column_by_name(bn_column) {
            Some(c) => c,
            None => continue,
        };

        let wc_arrays: Vec<Option<&dyn arrow::array::Array>> = weight_columns
            .iter()
            .map(|name| batch.column_by_name(name).map(|c| c.as_ref()))
            .collect();

        let key_arrays: Vec<Option<&dyn arrow::array::Array>> = dedup_key_columns
            .iter()
            .map(|name| batch.column_by_name(name).map(|c| c.as_ref()))
            .collect();

        for i in 0..batch.num_rows() {
            if let Some(block_num) = get_block_number(bn_col.as_ref(), i) {
                let row_hash = compute_row_key_hash(&key_arrays, i);

                if !seen.insert((block_num, row_hash)) {
                    continue; // Duplicate row, skip.
                }

                let mut row_weight = fixed_weight_per_row;
                for wc in &wc_arrays {
                    if let Some(arr) = wc {
                        row_weight += get_weight_value(*arr, i);
                    }
                }
                *weights.entry(block_num).or_default() += row_weight;
            }
        }
    }
}

/// Compute a hash of the dedup key columns for a given row.
fn compute_row_key_hash(key_arrays: &[Option<&dyn arrow::array::Array>], row: usize) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    for col in key_arrays {
        match col {
            Some(arr) => hash_array_value(*arr, row, &mut hasher),
            None => 0u8.hash(&mut hasher),
        }
    }
    hasher.finish()
}

/// Compute the column set used for weight calculation.
/// Matches legacy behavior: weight projection = primary_key + user_output_columns.
///
/// Primary key = block_number_column + item_order_keys (+ address_column if present).
///
/// Unlike `resolve_output_columns`, this does NOT include:
/// - Join key columns (for relations)
/// - Source predicate columns (e.g., is_committed)
/// - Tag columns (for field groups)
fn weight_projection(
    user_output_columns: &[String],
    table_desc: Option<&TableDescription>,
) -> Vec<String> {
    let desc = match table_desc {
        Some(d) => d,
        None => return user_output_columns.to_vec(),
    };

    let mut cols: HashSet<String> = HashSet::new();

    // 1. Primary key: block_number_column + item_order_keys + address_column
    cols.insert(desc.block_number_column.clone());
    for key in &desc.item_order_keys {
        cols.insert(key.clone());
    }
    if let Some(ac) = &desc.address_column {
        cols.insert(ac.clone());
    }

    // 2. User-requested output columns (with virtual field expansion)
    for col_name in user_output_columns {
        if let Some(vf) = desc.virtual_fields.get(col_name.as_str()) {
            match vf {
                VirtualField::Roll { columns } => {
                    for c in columns {
                        cols.insert(c.clone());
                    }
                }
            }
        } else if desc.columns.contains_key(col_name.as_str()) {
            cols.insert(col_name.clone());
        }
    }

    // 3. Weight/size columns for any projected column that uses dynamic weight
    for (col_name, col_desc) in &desc.columns {
        if cols.contains(col_name) {
            if let Some(WeightSource::Column(wc)) = &col_desc.weight {
                cols.insert(wc.clone());
            }
        }
    }

    cols.into_iter().collect()
}

/// Hash a single array value for row dedup.
fn hash_array_value(col: &dyn arrow::array::Array, row: usize, hasher: &mut impl Hasher) {
    if col.is_null(row) {
        0xFFu8.hash(hasher);
        return;
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = col.as_any().downcast_ref::<UInt16Array>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        a.value(row).hash(hasher);
    } else if let Some(a) = col.as_any().downcast_ref::<GenericListArray<i32>>() {
        let values = a.value(row);
        values.len().hash(hasher);
        for i in 0..values.len() {
            hash_array_value(values.as_ref(), i, hasher);
        }
    } else {
        // Unknown type — use a sentinel to distinguish from null
        0xFEu8.hash(hasher);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::parse_dataset_description;
    use crate::output::columns::resolve_output_columns;

    /// Load the solana metadata for tests.
    fn solana_meta() -> DatasetDescription {
        let yaml = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("metadata/solana.yaml"),
        )
        .unwrap();
        parse_dataset_description(&yaml).unwrap()
    }

    /// Load the evm metadata for tests.
    fn evm_meta() -> DatasetDescription {
        let yaml = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("metadata/evm.yaml"),
        )
        .unwrap();
        parse_dataset_description(&yaml).unwrap()
    }

    /// Helper: compute weight from a set of column names using compute_weight_params.
    fn weight_for(cols: &[&str], table_desc: Option<&TableDescription>) -> (u64, Vec<String>) {
        let col_strings: Vec<String> = cols.iter().map(|s| s.to_string()).collect();
        compute_weight_params(&col_strings, table_desc)
    }

    /// Helper: compute weight using weight_projection (primary_key + user output).
    fn legacy_weight_for(
        user_output: &[&str],
        table_desc: Option<&TableDescription>,
    ) -> (u64, Vec<String>) {
        let user_strings: Vec<String> = user_output.iter().map(|s| s.to_string()).collect();
        let projected = weight_projection(&user_strings, table_desc);
        compute_weight_params(&projected, table_desc)
    }

    // -----------------------------------------------------------------------
    // Tests: compute_weight_params — documents what columns contribute to weight
    // -----------------------------------------------------------------------

    #[test]
    fn test_system_columns_excluded_from_weight() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // d1 is system=true, should contribute 0 weight
        let (fixed, dynamic) = weight_for(&["d1"], Some(instr));
        assert_eq!(fixed, 0, "system columns should not contribute weight");
        assert!(dynamic.is_empty());

        // d8 is also system
        let (fixed, dynamic) = weight_for(&["d8"], Some(instr));
        assert_eq!(fixed, 0);
        assert!(dynamic.is_empty());

        // data_size is system
        let (fixed, dynamic) = weight_for(&["data_size"], Some(instr));
        assert_eq!(fixed, 0);
        assert!(dynamic.is_empty());
    }

    #[test]
    fn test_fixed_weight_columns() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // program_id: no explicit weight → default 32
        let (fixed, dynamic) = weight_for(&["program_id"], Some(instr));
        assert_eq!(fixed, 32);
        assert!(dynamic.is_empty());

        // is_committed: boolean, no explicit weight → default 32
        let (fixed, dynamic) = weight_for(&["is_committed"], Some(instr));
        assert_eq!(fixed, 32);
        assert!(dynamic.is_empty());

        // a1-a15: explicit weight=0
        let (fixed, dynamic) = weight_for(&["a1"], Some(instr));
        assert_eq!(fixed, 0, "a1 has weight=0");
        assert!(dynamic.is_empty());

        let (fixed, _) = weight_for(&["a5"], Some(instr));
        assert_eq!(fixed, 0, "a5 has weight=0");

        let (fixed, _) = weight_for(&["rest_accounts"], Some(instr));
        assert_eq!(fixed, 0, "rest_accounts has weight=0");
    }

    #[test]
    fn test_dynamic_weight_columns() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // data: weight = data_size (dynamic column)
        let (fixed, dynamic) = weight_for(&["data"], Some(instr));
        assert_eq!(fixed, 0, "data has no fixed weight");
        assert_eq!(dynamic, vec!["data_size"]);

        // a0: weight = accounts_size (dynamic column)
        let (fixed, dynamic) = weight_for(&["a0"], Some(instr));
        assert_eq!(fixed, 0);
        assert_eq!(dynamic, vec!["accounts_size"]);
    }

    #[test]
    fn test_virtual_field_accounts_weight() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // "accounts" is a virtual roll → [a0, a1, ..., a15, rest_accounts]
        // a0 → accounts_size (dynamic), a1-a15 + rest_accounts → weight=0
        let (fixed, dynamic) = weight_for(&["accounts"], Some(instr));
        assert_eq!(fixed, 0, "all account columns have weight=0 except a0 which is dynamic");
        assert_eq!(dynamic, vec!["accounts_size"]);
    }

    #[test]
    fn test_evm_logs_weight() {
        let meta = evm_meta();
        let logs = meta.table("logs").unwrap();

        // data → data_size (dynamic)
        let (fixed, dynamic) = weight_for(&["data"], Some(logs));
        assert_eq!(fixed, 0);
        assert_eq!(dynamic, vec!["data_size"]);

        // address: no explicit weight → default 32
        let (fixed, dynamic) = weight_for(&["address"], Some(logs));
        assert_eq!(fixed, 32);
        assert!(dynamic.is_empty());

        // topics = roll(topic0, topic1, topic2, topic3), each → default 32
        let (fixed, dynamic) = weight_for(&["topics"], Some(logs));
        assert_eq!(fixed, 4 * 32, "4 topic columns × 32 bytes each");
        assert!(dynamic.is_empty());
    }

    #[test]
    fn test_evm_traces_weight() {
        let meta = evm_meta();
        let traces = meta.table("traces").unwrap();

        // call_input: dynamic weight
        let (fixed, dynamic) = weight_for(&["call_input"], Some(traces));
        assert_eq!(fixed, 0);
        assert_eq!(dynamic, vec!["call_input_size"]);

        // call_result_output: dynamic weight
        let (fixed, dynamic) = weight_for(&["call_result_output"], Some(traces));
        assert_eq!(fixed, 0);
        assert_eq!(dynamic, vec!["call_result_output_size"]);
    }

    #[test]
    fn test_evm_blocks_weight() {
        let meta = evm_meta();
        let blocks = meta.table("blocks").unwrap();

        // logs_bloom: weight=512 (matching legacy set_weight("logs_bloom", 512))
        let (fixed, dynamic) = weight_for(&["logs_bloom"], Some(blocks));
        assert_eq!(fixed, 512, "logs_bloom has fixed weight 512");
        assert!(dynamic.is_empty());

        // extra_data: weight = extra_data_size (dynamic)
        let (fixed, dynamic) = weight_for(&["extra_data"], Some(blocks));
        assert_eq!(fixed, 0);
        assert_eq!(dynamic, vec!["extra_data_size"]);
    }

    #[test]
    fn test_combined_weight_multiple_columns() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // Combining multiple columns: program_id(32) + data(data_size) + is_committed(32)
        let (fixed, dynamic) = weight_for(
            &["program_id", "data", "is_committed"],
            Some(instr),
        );
        assert_eq!(fixed, 64, "program_id(32) + is_committed(32)");
        assert_eq!(dynamic, vec!["data_size"]);
    }

    // -----------------------------------------------------------------------
    // Tests: legacy-compatible weight columns (primary_key + user output)
    // -----------------------------------------------------------------------

    #[test]
    fn test_weight_projection_solana_instructions_basic() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // weight_projection adds primary key: [block_number, transaction_index, instruction_address]
        // User output: [transaction_index, instruction_address, program_id, accounts, data]
        let (fixed, dynamic) = legacy_weight_for(
            &["transaction_index", "instruction_address", "program_id", "accounts", "data"],
            Some(instr),
        );

        // block_number: default 32 (not system)
        // transaction_index: default 32
        // instruction_address: default 32
        // program_id: default 32
        // accounts → roll: a0(accounts_size) + a1..a15(0) + rest_accounts(0)
        // data → data_size
        assert_eq!(fixed, 4 * 32, "4 non-system non-dynamic columns × 32");
        assert!(dynamic.contains(&"data_size".to_string()));
        assert!(dynamic.contains(&"accounts_size".to_string()));
        assert_eq!(dynamic.len(), 2);
    }

    #[test]
    fn test_weight_projection_solana_instructions_with_extra_fields() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // Same as above but user also requests d1, d8, is_committed
        // d1 and d8 are system → excluded from weight
        // is_committed is NOT system → 32 bytes
        let (fixed, dynamic) = legacy_weight_for(
            &[
                "transaction_index",
                "instruction_address",
                "program_id",
                "accounts",
                "data",
                "d1",
                "d8",
                "is_committed",
            ],
            Some(instr),
        );

        // block_number(32) + transaction_index(32) + instruction_address(32) +
        // program_id(32) + is_committed(32) + accounts_roll(0) + data(dynamic) +
        // d1(system=0) + d8(system=0)
        assert_eq!(fixed, 5 * 32, "5 non-system non-dynamic columns × 32");
        assert_eq!(dynamic.len(), 2);
    }

    #[test]
    fn test_weight_projection_evm_logs_basic() {
        let meta = evm_meta();
        let logs = meta.table("logs").unwrap();

        // weight_projection adds primary key: [block_number, transaction_index, log_index]
        // User output: [log_index, transaction_index, address, data, topics]
        let (fixed, dynamic) = legacy_weight_for(
            &["log_index", "transaction_index", "address", "data", "topics"],
            Some(logs),
        );

        // block_number(32) + log_index(32) + transaction_index(32) + address(32) +
        // topic0(32) + topic1(32) + topic2(32) + topic3(32) + data(data_size)
        assert_eq!(fixed, 8 * 32, "8 fixed-weight columns × 32");
        assert_eq!(dynamic, vec!["data_size"]);
    }

    #[test]
    fn test_weight_projection_evm_transactions_with_input() {
        let meta = evm_meta();
        let txs = meta.table("transactions").unwrap();

        // weight_projection adds primary key: [block_number, transaction_index]
        // User output: [hash, from, to, input, value, gas]
        let (fixed, dynamic) = legacy_weight_for(
            &["hash", "from", "to", "input", "value", "gas"],
            Some(txs),
        );

        // block_number(32) + transaction_index(32) + hash(32) + from(32) + to(32) +
        // value(32) + gas(32) + input(input_size)
        assert_eq!(fixed, 7 * 32);
        assert_eq!(dynamic, vec!["input_size"]);
    }

    // -----------------------------------------------------------------------
    // Tests: resolve_output_columns includes extra columns NOT in legacy weight
    // -----------------------------------------------------------------------

    #[test]
    fn test_resolve_includes_join_keys_for_relations() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // When instructions have a "transaction" relation, resolve_output_columns
        // adds the join key columns (block_number, transaction_index) even if
        // the user doesn't request them.
        let table_plan = crate::query::TablePlan {
            table: "instructions".to_string(),
            output_columns: vec!["program_id".to_string()],
            predicates: vec![],
            relations: vec![crate::query::RelationPlan {
                target_table: "transactions".to_string(),
                kind: crate::query::RelationKind::Join,
                left_key: vec![
                    "block_number".to_string(),
                    "transaction_index".to_string(),
                ],
                right_key: vec![
                    "block_number".to_string(),
                    "transaction_index".to_string(),
                ],
                output_columns: vec![],
                source_predicates: None,
            }],
        };

        let resolved = resolve_output_columns(&table_plan, instr);

        // resolve_output_columns always includes:
        // - block_number (block number column)
        // - transaction_index, instruction_address (item_order_keys)
        // - program_id (user output)
        // - block_number, transaction_index (relation join keys — already in set)
        assert!(resolved.contains(&"block_number".to_string()));
        assert!(resolved.contains(&"transaction_index".to_string()));
        assert!(resolved.contains(&"instruction_address".to_string()));
        assert!(resolved.contains(&"program_id".to_string()));
    }

    #[test]
    fn test_resolve_includes_source_predicate_columns() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // When a relation has source_predicates (e.g., is_committed filter),
        // resolve_output_columns adds those predicate columns.
        let table_plan = crate::query::TablePlan {
            table: "instructions".to_string(),
            output_columns: vec!["program_id".to_string()],
            predicates: vec![],
            relations: vec![crate::query::RelationPlan {
                target_table: "transactions".to_string(),
                kind: crate::query::RelationKind::Join,
                left_key: vec![
                    "block_number".to_string(),
                    "transaction_index".to_string(),
                ],
                right_key: vec![
                    "block_number".to_string(),
                    "transaction_index".to_string(),
                ],
                output_columns: vec![],
                source_predicates: Some(vec![crate::scan::predicate::RowPredicate::new(vec![
                    crate::scan::predicate::col_eq(
                        "is_committed",
                        crate::scan::predicate::ScalarValue::Boolean(true),
                    ),
                ])]),
            }],
        };

        let resolved = resolve_output_columns(&table_plan, instr);

        // is_committed is added because of source_predicates
        assert!(
            resolved.contains(&"is_committed".to_string()),
            "source predicate column should be in resolved columns"
        );
    }

    #[test]
    fn test_weight_projection_excludes_source_predicates() {
        let meta = solana_meta();
        let instr = meta.table("instructions").unwrap();

        // weight_projection does NOT include source predicate columns (e.g., is_committed)
        // unless the user explicitly requests them in output fields.

        // Scenario: user requests [program_id, data] only.
        // Even if is_committed is used as a source predicate for a relation,
        // it should NOT inflate the weight.
        let (fixed, dynamic) = legacy_weight_for(
            &["program_id", "data"],
            Some(instr),
        );

        // Primary key: block_number(32) + transaction_index(32) + instruction_address(32)
        // User output: program_id(32) + data(data_size)
        // is_committed NOT included → no extra 32 bytes
        assert_eq!(fixed, 4 * 32, "4 fixed columns (no is_committed)");
        assert_eq!(dynamic.len(), 1, "1 dynamic column (data_size)");

        // Contrast: if user DID request is_committed in output, it would be counted
        let (fixed_with, dynamic_with) = legacy_weight_for(
            &["program_id", "data", "is_committed"],
            Some(instr),
        );
        assert_eq!(fixed_with, 5 * 32, "5 fixed columns (with is_committed)");
        assert_eq!(dynamic_with.len(), 1);
    }
}
