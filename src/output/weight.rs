use crate::metadata::{DatasetDescription, TableDescription, VirtualField, WeightSource};
use crate::query::Plan;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap;
use std::collections::{HashMap, HashSet};

use super::columns::{resolve_output_columns, resolve_relation_output_columns};

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

/// Apply weight-based limit: select blocks until cumulative weight exceeds MAX_RESPONSE_BYTES.
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

    // Pre-build per-block weight in a single pass over all rows
    let mut block_weights: FxHashMap<u64, u64> = FxHashMap::default();

    for table_plan in &plan.table_plans {
        let output = match table_outputs.get(&table_plan.table) {
            Some(o) => o,
            None => continue,
        };
        let table_desc = metadata.table(&table_plan.table);
        let bn_col = table_desc
            .map(|d| d.block_number_column.as_str())
            .unwrap_or("block_number");

        // Compute weight from the full resolved projection
        let resolved_cols = resolve_output_columns(table_plan, table_desc.unwrap());
        let (fixed_weight, weight_cols) = compute_weight_params(&resolved_cols, table_desc);

        accumulate_block_weights(
            &output.batches,
            bn_col,
            fixed_weight,
            &weight_cols,
            &mut block_weights,
        );

        for rel in &table_plan.relations {
            if let Some(rel_batches) = output.relation_batches.get(&rel.target_table) {
                let rel_desc = metadata.table(&rel.target_table);
                let rel_bn = rel_desc
                    .map(|d| d.block_number_column.as_str())
                    .unwrap_or("block_number");

                let rel_resolved = resolve_relation_output_columns(&rel.output_columns, rel_desc);
                let (rel_fixed, rel_weight_cols) = compute_weight_params(&rel_resolved, rel_desc);

                accumulate_block_weights(
                    rel_batches,
                    rel_bn,
                    rel_fixed,
                    &rel_weight_cols,
                    &mut block_weights,
                );
            }
        }
    }

    // Add block header weight for blocks that have items.
    // In legacy: when include_all_blocks == false, first/last boundary blocks get weight 0,
    // other blocks get header weight added.
    let block_desc = metadata.table(&plan.block_table);
    let (header_fixed, header_weight_cols) =
        compute_weight_params(&plan.block_output_columns, block_desc);
    let header_bn_col = block_desc
        .map(|d| d.block_number_column.as_str())
        .unwrap_or("number");

    if plan.include_all_blocks {
        // All blocks get header weight
        accumulate_block_weights(
            block_batches,
            header_bn_col,
            header_fixed,
            &header_weight_cols,
            &mut block_weights,
        );
    } else {
        // Only blocks that have item data get header weight added
        // (boundary blocks without items get 0 weight)
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
/// This matches legacy behavior: every projected column contributes either a fixed weight
/// (default 32) or a dynamic weight column. System columns do NOT add weight.
fn compute_weight_params(
    output_columns: &[String],
    table_desc: Option<&TableDescription>,
) -> (u64, Vec<String>) {
    let desc = match table_desc {
        Some(d) => d,
        None => return (DEFAULT_ROW_WEIGHT, Vec::new()),
    };

    // Collect all physical columns in the projection
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
        // Skip system columns — they don't contribute to output weight
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

/// Accumulate per-block weights in a single pass over all rows.
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

        // Resolve weight columns for this batch
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
