//! The tables a response carries and the rows that fill each one: the table's
//! own scan, and every relation into it.
//!
//! Both output formats take their tables from here, so they agree on which
//! sources a table has and on the order the tables come in.

use crate::metadata::{DatasetDescription, TableDescription};
use crate::output::weight::TableOutput;
use crate::query::Plan;
use crate::scan::Rows;
use std::collections::HashMap;

/// One source of a response table's rows.
pub(crate) struct OutputSource<'a> {
    /// The fields this source renders.
    pub(crate) fields: &'a [String],
    pub(crate) rows: Rows,
}

/// A response table and its sources, in plan order.
pub(crate) struct OutputTable<'a> {
    /// The name the response gives the table's items.
    pub(crate) name: &'a str,
    pub(crate) desc: &'a TableDescription,
    pub(crate) sources: Vec<OutputSource<'a>>,
}

/// Every table with a source, in catalog order: the order its items take in a
/// block (INV-O4). A source without rows is kept; each format decides what one
/// means to it.
pub(crate) fn output_tables<'a>(
    plan: &'a Plan,
    metadata: &'a DatasetDescription,
    mut outputs: HashMap<String, TableOutput>,
) -> Vec<OutputTable<'a>> {
    // Request names are unique per catalog table, so a catalog position
    // identifies a response table.
    let mut tables: Vec<(usize, OutputTable<'a>)> = Vec::new();
    let mut add = |table: &str, fields: &'a [String], rows: Rows| {
        let Some((position, key, desc)) = metadata.tables.get_full(table) else {
            return;
        };
        let source = OutputSource { fields, rows };

        match tables.iter_mut().find(|(at, _)| *at == position) {
            Some((_, output)) => output.sources.push(source),
            None => tables.push((
                position,
                OutputTable {
                    name: desc.request_name(key),
                    desc,
                    sources: vec![source],
                },
            )),
        }
    };

    for table_plan in &plan.table_plans {
        let Some(TableOutput {
            rows,
            mut relations,
        }) = outputs.remove(&table_plan.table)
        else {
            continue;
        };
        add(&table_plan.table, &table_plan.output_columns, rows);

        for (index, relation) in table_plan.relations.iter().enumerate() {
            if let Some(rows) = relations.remove(&index) {
                add(&relation.target_table, &relation.output_columns, rows);
            }
        }
    }

    tables.sort_by_key(|(position, _)| *position);
    tables.into_iter().map(|(_, table)| table).collect()
}
