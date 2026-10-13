//! The tables a response carries and the rows that fill each one: the table's
//! own scan, and every relation into it; and the block headers beside them.
//!
//! Both output formats take their tables from here, so they agree on which
//! sources a table has and on the order the tables come in.

use crate::metadata::{DatasetDescription, TableDescription};
use crate::output::projection::ResolvedProjection;
use crate::scan::Rows;
use arrow::record_batch::RecordBatch;

/// The block headers of a response.
pub(crate) struct OutputHeader<'a> {
    /// The header fields the request selects.
    pub(crate) projection: &'a ResolvedProjection<'a>,
    /// Header rows of the page's blocks, possibly beside other blocks' rows.
    pub(crate) batches: Vec<RecordBatch>,
}

/// One source of a response table's rows.
pub(crate) struct OutputSource<'a> {
    /// The fields this source renders.
    pub(crate) projection: &'a ResolvedProjection<'a>,
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
/// block (INV-O4). `sources` come in plan order: each table's own rows, then
/// the rows of each of its relations. A source without rows is kept; each
/// format decides what one means to it.
pub(crate) fn output_tables<'a>(
    metadata: &'a DatasetDescription,
    sources: Vec<OutputSource<'a>>,
) -> Vec<OutputTable<'a>> {
    // Request names are unique per catalog table, so a catalog position
    // identifies a response table.
    let mut tables: Vec<(usize, OutputTable<'a>)> = Vec::new();

    for source in sources {
        let Some((position, key, desc)) = metadata.tables.get_full(source.projection.table())
        else {
            continue;
        };

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
    }

    tables.sort_by_key(|(position, _)| *position);
    tables.into_iter().map(|(_, table)| table).collect()
}
