use crate::metadata::{TableDescription, VirtualField, WeightSource};
use crate::query::TablePlan;
use std::collections::HashSet;

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
