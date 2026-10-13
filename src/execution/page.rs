//! Page selection: read complete block ranges from the start of the request
//! and weigh their blocks until the response budget is spent or the request
//! ends.

use super::materialize::retain_selected_keys;
use super::prepare::Execution;
use super::tables::scan_tables;
use crate::metadata::TableDescription;
use crate::output::block_index::{collect_block_numbers, collect_boundary_blocks};
use crate::output::filter_to_blocks;
use crate::output::weight::{
    compute_block_weights, weight_range_end, BlockSelection, TableOutput, WeighedRows,
};
use crate::scan::predicate::RowPredicate;
use crate::scan::{Rows, ScanRequest};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use rustc_hash::{FxHashMap, FxHashSet as HashSet};
use std::collections::HashMap;

/// The whole blocks a response carries, and the rows that fill them.
pub(super) struct Page {
    /// Ascending, never empty.
    pub(super) blocks: Vec<u64>,
    /// Every header row of the requested range; only the page's blocks once a
    /// deferred page is materialized.
    pub(super) headers: Rows,
    /// Each table plan's rows and its relations' rows, by table name: only
    /// identities until a deferred page is materialized.
    pub(super) tables: HashMap<String, TableOutput>,
    /// The last block read when the page stopped before the requested end.
    pub(super) read_through: Option<u64>,
}

/// Read and weigh complete block ranges until the page is full or the request
/// ends. `None` when the request reaches no block of the chunk.
pub(super) fn select_page(execution: &Execution) -> Result<Option<Page>> {
    let plan = execution.plan;
    let bn_column = execution.blocks.block_number_column.as_str();
    let headers = read_headers(execution)?;

    let mut boundary_blocks = HashSet::default();
    collect_boundary_blocks(headers.batches(), bn_column, &mut boundary_blocks)?;

    let mut header_numbers = HashSet::default();
    if execution.selection.is_some() {
        collect_block_numbers(headers.batches(), bn_column, &mut header_numbers)?;
    }
    let mut header_numbers: Vec<_> = header_numbers.into_iter().collect();
    header_numbers.sort_unstable();

    let mut tables: HashMap<String, TableOutput> = HashMap::new();
    let mut selection = BlockSelection::new(execution.options.weight_budget);
    let no_rows = Rows::default();
    let mut from_block = plan.from_block;
    let mut read_through = None;
    let mut first_range = true;
    let request_end = boundary_blocks
        .iter()
        .copied()
        .max()
        .map(|last| plan.to_block.map_or(last, |end| end.min(last)))
        .or(plan.to_block);

    loop {
        let (to_block, last_range) = range_end(
            execution,
            from_block,
            first_range,
            &header_numbers,
            request_end,
        )?;
        let mut range_outputs = scan_tables(execution, from_block, to_block)?;

        let in_range = |block: u64| block >= from_block && to_block.is_none_or(|end| block <= end);
        let range_headers = if first_range && last_range {
            headers.batches().to_vec()
        } else {
            headers
                .batches()
                .iter()
                .map(|batch| filter_to_blocks(batch, bn_column, in_range))
                .collect::<Result<Vec<_>>>()?
        };
        let blocks = range_blocks(
            execution,
            &range_outputs,
            &range_headers,
            &boundary_blocks,
            in_range,
        )?;
        let exhausted = if blocks.is_empty() {
            false
        } else {
            let sources = weighed_sources(execution, &range_outputs, &no_rows);
            let weights =
                compute_block_weights(&sources, &execution.header, &range_headers, &blocks)?;
            selection.extend(&blocks, &weights)
        };

        if execution.selection.is_some() {
            retain_selected_keys(&mut range_outputs, &execution.tables, selection.blocks())?;
        }

        for (table, output) in range_outputs {
            let accumulated = tables.entry(table).or_default();
            accumulated.rows.append(output.rows)?;
            for (relation, rows) in output.relations {
                accumulated
                    .relations
                    .entry(relation)
                    .or_default()
                    .append(rows)?;
            }
        }

        if exhausted || last_range {
            if exhausted && !last_range {
                read_through = to_block;
            }
            break;
        }
        let Some(next) = to_block.and_then(|end| end.checked_add(1)) else {
            break;
        };
        from_block = next;
        first_range = false;
    }

    let blocks = selection.into_blocks();
    if blocks.is_empty() {
        return Ok(None);
    }

    Ok(Some(Page {
        blocks,
        headers,
        tables,
        read_through,
    }))
}

/// Every header row of the requested range, read once so internal ranges never
/// add boundary blocks: the block number, the selected fields and the weight
/// companions those fields declare. A declared `weight: <column>` that is never
/// read is a weight of zero, so the header under-weighs and truncation lands
/// past the cap (INV-B10).
fn read_headers(execution: &Execution) -> Result<Rows> {
    let (plan, desc, header) = (execution.plan, execution.blocks, &execution.header);

    let mut request = ScanRequest::new(header.row_columns().to_vec());
    request.from_block = Some(plan.from_block);
    request.to_block = plan.to_block;
    request.block_number_column = Some(desc.block_number_column.as_str());
    request.required_columns = header.required().to_vec();

    let headers = execution
        .scan_reader()
        .scan_rows(&plan.block_table, &request)?;
    Ok(headers.into_rows())
}

/// Where the range from `from_block` ends, and whether it reaches the end of
/// the request.
fn range_end(
    execution: &Execution,
    from_block: u64,
    first_range: bool,
    header_numbers: &[u64],
    request_end: Option<u64>,
) -> Result<(Option<u64>, bool)> {
    let plan = execution.plan;
    let mut hint = if execution.options.range_reads {
        if first_range {
            initial_range_end(execution)?
        } else {
            next_range_end(execution, from_block)
        }
    } else {
        None
    };
    // Count actual headers so sparse block numbers do not turn into empty
    // scans across numeric gaps.
    let first = header_numbers.partition_point(|&block| block < from_block);
    let headers_left = header_numbers.len() - first;
    if let Some(blocks_per_selection) = selection_span(execution, headers_left) {
        if let Some(&end) = header_numbers.get(first + blocks_per_selection - 1) {
            hint = Some(hint.map_or(end, |hint| hint.min(end)));
        }
    }

    // A hint covering the whole request needs no extra block filter. Keep
    // the original bounds so a selective predicate can run first.
    let hint = hint.filter(|&end| request_end.is_none_or(|last| end < last));
    let to_block = match (hint, request_end) {
        (Some(hint), Some(end)) => Some(hint.max(from_block).min(end)),
        (Some(hint), None) => Some(hint.max(from_block)),
        (None, _) => plan.to_block,
    };
    let last_range = hint.is_none() || to_block == request_end;

    Ok((to_block, last_range))
}

/// How many header blocks one selection pass may span, with `headers_left` of
/// the request still to read; `None` when it may span the request, or when
/// payloads are not deferred.
///
/// Each pass is bounded even when row groups overlap or have no statistics.
/// Unfiltered scans fill a page within a few blocks. A selective scan spans the
/// request when the footer bounds its selection within the budget: a range reads
/// again every row group whose blocks it shares with another, and a table sorted
/// by its filter columns shares them all. Otherwise it uses larger ranges to
/// amortize predicate decoding over overlapping row groups.
fn selection_span(execution: &Execution, headers_left: usize) -> Option<usize> {
    const UNFILTERED: usize = 16;
    const SELECTIVE: usize = 1024;

    execution.selection.as_ref()?;

    let unfiltered = execution
        .plan
        .table_plans
        .iter()
        .any(|table| table.predicates.iter().any(RowPredicate::matches_every_row));
    if unfiltered {
        return Some(UNFILTERED);
    }

    // Only a request that a range would split asks the footer.
    let splits = headers_left > SELECTIVE;
    if splits && execution.selection_fits() {
        return None;
    }

    Some(SELECTIVE)
}

/// A narrow size scan avoids decoding wide columns for a large unfiltered query.
/// Its estimate only chooses the first read range; exact selection can read on.
fn initial_range_end(execution: &Execution) -> Result<Option<u64>> {
    let plan = execution.plan;
    let Some(table) = execution.prescanned_table() else {
        return Ok(next_range_end(execution, plan.from_block));
    };
    let (table_plan, desc) = (table.plan, table.desc);

    let mut request = ScanRequest::new(table.projection.size_columns().to_vec());
    request.predicates = table_plan.predicates.iter().collect();
    request.from_block = Some(plan.from_block);
    request.to_block = plan.to_block;
    request.block_number_column = Some(&desc.block_number_column);
    let batches = execution.chunk.scan(&table_plan.table, &request)?;

    Ok(weight_range_end(
        &batches,
        &table.projection,
        execution.options.weight_budget,
    ))
}

/// Suggest a range large enough to cover a batch of row groups in every table.
/// Missing statistics select a full read; overlapping groups produce larger ranges.
fn next_range_end(execution: &Execution, from_block: u64) -> Option<u64> {
    let mut tables = FxHashMap::<&str, &TableDescription>::default();
    for table in &execution.tables {
        tables.insert(&table.plan.table, table.desc);
        tables.extend(
            table
                .relations()
                .map(|(relation, target)| (relation.target_table.as_str(), target.desc())),
        );
    }

    let mut end = None;
    for (table, desc) in tables {
        let hint =
            execution
                .chunk
                .next_block_range_end(table, &desc.block_number_column, from_block)?;
        end = Some(end.map_or(hint, |end: u64| end.max(hint)));
    }
    end
}

/// The blocks of one range a page can hold, ascending: every block with a row
/// of a table or of a relation, and the boundary blocks — or, under
/// `includeAllBlocks`, every header.
fn range_blocks(
    execution: &Execution,
    outputs: &HashMap<String, TableOutput>,
    headers: &[RecordBatch],
    boundary_blocks: &HashSet<u64>,
    in_range: impl Fn(u64) -> bool,
) -> Result<Vec<u64>> {
    let mut block_numbers: HashSet<u64> = HashSet::default();
    for table in &execution.tables {
        let Some(output) = outputs.get(&table.plan.table) else {
            continue;
        };
        collect_block_numbers(
            output.rows.batches(),
            &table.desc.block_number_column,
            &mut block_numbers,
        )?;

        // A relation's target is a different table, and it names its own block
        // number column. Reading one literal name here would drop every row of
        // a table that calls it something else — out of block selection and out
        // of the weight model both (INV-X1).
        for (&relation, rows) in &output.relations {
            let target = table.targets[relation].desc();
            collect_block_numbers(
                rows.batches(),
                &target.block_number_column,
                &mut block_numbers,
            )?;
        }
    }

    if execution.plan.include_all_blocks {
        let bn_column = &execution.blocks.block_number_column;
        collect_block_numbers(headers, bn_column, &mut block_numbers)?;
    } else {
        block_numbers.extend(boundary_blocks.iter().copied().filter(|&b| in_range(b)));
    }

    let mut blocks: Vec<_> = block_numbers.into_iter().collect();
    blocks.sort_unstable();
    Ok(blocks)
}

/// Every source of a range's rows with the fields it renders: each table's own
/// rows, and each of its relations', read or not.
fn weighed_sources<'r>(
    execution: &'r Execution,
    outputs: &'r HashMap<String, TableOutput>,
    no_rows: &'r Rows,
) -> Vec<WeighedRows<'r>> {
    let mut sources = Vec::new();
    for table in &execution.tables {
        let Some(output) = outputs.get(&table.plan.table) else {
            continue;
        };
        sources.push(WeighedRows {
            projection: &table.projection,
            rows: &output.rows,
        });

        for (index, target) in table.targets.iter().enumerate() {
            sources.push(WeighedRows {
                projection: target,
                rows: output.relations.get(&index).unwrap_or(no_rows),
            });
        }
    }

    sources
}
