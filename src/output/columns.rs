use crate::metadata::{TableDescription, VirtualField, WeightSource};
use crate::query::TablePlan;
use std::collections::HashSet;

/// Map a table's *requested output fields* (logical names, possibly virtual or
/// field-group requests) to the physical parquet columns that back them, in
/// request order. Unlike [`resolve_output_columns`], this does NOT add scan
/// helpers (join keys, weight columns, sort keys, tag column) — it is exactly
/// the columns a user asked to see, expanded to physical form. Used to project
/// flat Arrow output.
pub(crate) fn physical_output_columns(
    out_cols: &[String],
    table_desc: &TableDescription,
) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    let push = |c: String, cols: &mut Vec<String>| {
        if !cols.contains(&c) {
            cols.push(c);
        }
    };
    for col in out_cols {
        if let Some(VirtualField::Roll { columns }) = table_desc.virtual_fields.get(col.as_str()) {
            for c in columns {
                push(c.clone(), &mut cols);
            }
        } else if table_desc.columns.contains_key(col.as_str()) {
            push(col.clone(), &mut cols);
        } else if let Some(phys) = table_desc
            .field_groups
            .as_ref()
            .and_then(|fg| fg.physical_column_for_request(col.as_str()))
        {
            push(phys.to_string(), &mut cols);
        }
    }
    cols
}

/// Resolve all physical columns needed for a table's output (including virtual field sources).
pub(crate) fn resolve_output_columns(
    table_plan: &TablePlan,
    table_desc: &TableDescription,
) -> Vec<String> {
    let mut cols: HashSet<String> = HashSet::new();

    // Block number column always needed for grouping
    cols.insert(table_desc.block_number_column.clone());

    // Sort columns: item_order_keys + address_column
    for key in &table_desc.item_order_keys {
        cols.insert(key.clone());
    }
    if let Some(ac) = &table_desc.address_column {
        cols.insert(ac.clone());
    }

    // Tag column for field groups (needed for variant dispatch)
    if let Some(fg) = &table_desc.field_groups {
        cols.insert(fg.tag_column.clone());
    }

    // Requested output columns
    for col in &table_plan.output_columns {
        // Check if this is a virtual field
        if let Some(vf) = table_desc.virtual_fields.get(col.as_str()) {
            match vf {
                VirtualField::Roll { columns } => {
                    for c in columns {
                        cols.insert(c.clone());
                    }
                }
            }
        } else if table_desc.columns.contains_key(col.as_str()) {
            cols.insert(col.clone());
        } else if let Some(phys) = table_desc
            .field_groups
            .as_ref()
            .and_then(|fg| fg.physical_column_for_request(col.as_str()))
        {
            // Request key that maps to a differently-named physical column
            // (e.g. trace `call_call_type` → `call_type`).
            cols.insert(phys.to_string());
        }
    }

    // Join key columns (needed for relations)
    for rel in &table_plan.relations {
        for key in &rel.left_key {
            cols.insert(key.clone());
        }
        // Source predicate columns (needed for post-hoc filtering of relation sources)
        if let Some(preds) = &rel.source_predicates {
            for pred in preds {
                for col_pred in &pred.columns {
                    cols.insert(col_pred.column.clone());
                }
            }
        }
    }

    // Weight columns (needed for response size limiting)
    for (col_name, col_desc) in &table_desc.columns {
        if cols.contains(col_name) {
            if let Some(WeightSource::Column(wc)) = &col_desc.weight {
                cols.insert(wc.clone());
            }
        }
    }

    cols.into_iter().collect()
}

/// Resolve output columns for a relation target table.
pub(crate) fn resolve_relation_output_columns(
    output_columns: &[String],
    table_desc: Option<&TableDescription>,
) -> Vec<String> {
    let mut cols: HashSet<String> = HashSet::new();

    if let Some(desc) = table_desc {
        // Block number column
        cols.insert(desc.block_number_column.clone());
        // Sort columns: item_order_keys + address_column
        for key in &desc.item_order_keys {
            cols.insert(key.clone());
        }
        if let Some(ac) = &desc.address_column {
            cols.insert(ac.clone());
        }
        // Tag column for field groups
        if let Some(fg) = &desc.field_groups {
            cols.insert(fg.tag_column.clone());
        }
    }

    for col in output_columns {
        if let Some(desc) = table_desc {
            if let Some(vf) = desc.virtual_fields.get(col.as_str()) {
                match vf {
                    VirtualField::Roll { columns } => {
                        for c in columns {
                            cols.insert(c.clone());
                        }
                    }
                }
            } else if let Some(phys) = desc
                .field_groups
                .as_ref()
                .filter(|_| !desc.columns.contains_key(col.as_str()))
                .and_then(|fg| fg.physical_column_for_request(col.as_str()))
            {
                cols.insert(phys.to_string());
            } else {
                cols.insert(col.clone());
            }
        } else {
            cols.insert(col.clone());
        }
    }

    // Weight columns (needed for response size limiting)
    if let Some(desc) = table_desc {
        for (col_name, col_desc) in &desc.columns {
            if cols.contains(col_name) {
                if let Some(WeightSource::Column(wc)) = &col_desc.weight {
                    cols.insert(wc.clone());
                }
            }
        }
    }

    cols.into_iter().collect()
}

/// Physical columns that the user explicitly requested as output fields and
/// which must exist in the parquet (a missing one is a hard error, matching
/// legacy). Engine-internal columns (block number, sort keys, weights) are
/// excluded, as are `system` columns and virtual-field sources (the latter are
/// tolerated when absent).
pub(crate) fn required_output_columns(
    output_columns: &[String],
    table_desc: &TableDescription,
) -> Vec<String> {
    let mut cols = Vec::new();
    for col in output_columns {
        // Virtual fields roll several optional sources — don't hard-require them.
        if table_desc.virtual_fields.contains_key(col.as_str()) {
            continue;
        }
        // Resolve field-group request keys (e.g. `call_call_type` → `call_type`).
        let phys = table_desc
            .field_groups
            .as_ref()
            .filter(|_| !table_desc.columns.contains_key(col.as_str()))
            .and_then(|fg| fg.physical_column_for_request(col.as_str()))
            .unwrap_or(col.as_str());
        if let Some(desc) = table_desc.columns.get(phys) {
            if !desc.system {
                cols.push(phys.to_string());
            }
        }
    }
    cols
}

/// Get the hierarchical address column from table metadata.
pub(crate) fn find_address_column(desc: &TableDescription) -> Option<&str> {
    desc.address_column.as_deref()
}

/// Extract group keys from a relation's key list by removing the address column.
pub(crate) fn group_keys_for_relation<'a>(
    keys: &'a [String],
    addr_col: Option<&str>,
) -> Vec<&'a str> {
    keys.iter()
        .map(String::as_str)
        .filter(|k| Some(*k) != addr_col)
        .collect()
}
