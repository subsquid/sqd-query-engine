use super::arrow_out::{blocks_mask, project_columns};
use super::block_index::compute_block_range;
use super::columns::resolve_relation_output_columns;
use super::row_order::build_full_sort_columns;
use super::weight::{weight_projection, weight_scan_columns, TableOutput};
use crate::metadata::{DatasetDescription, TableDescription};
use crate::query::{Plan, RelationKind};
use crate::scan::predicate::RowPredicate;
use crate::scan::{ChunkReader, ColumnCache, KeyFilter, Rows, ScanRequest, Scanned};
use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};

/// Read only row identities, relation inputs and size columns while selecting a
/// page, with each row's physical position when the chunk can read positions
/// back. Required output columns are still checked by the underlying scanner.
pub(super) struct SelectionReader<'a> {
    inner: &'a dyn ChunkReader,
    columns: HashMap<String, Vec<String>>,
    track_positions: bool,
    /// Tables more than one scan reads: their columns are decoded once, kept
    /// here, and filtered in memory by each.
    shared: HashSet<String>,
    decoded: ColumnCache,
}

/// Decoded key and size columns one query may keep while it selects a page.
const SELECTION_COLUMN_BUDGET: u64 = 64 << 20;

fn row_key(desc: &TableDescription) -> Vec<String> {
    let mut columns = vec![desc.block_number_column.clone()];
    columns.extend(build_full_sort_columns(desc));
    columns
}

fn extend_unique(columns: &mut Vec<String>, extra: impl IntoIterator<Item = String>) {
    for column in extra {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
}

impl<'a> SelectionReader<'a> {
    pub(super) fn new(
        inner: &'a dyn ChunkReader,
        plan: &Plan,
        metadata: &DatasetDescription,
    ) -> Option<Self> {
        let track_positions = inner.supports_row_positions();
        let mut columns = HashMap::<String, Vec<String>>::new();
        // Only scans that read most of a table share it: a relation from a
        // selective item reads a few row groups, and keeping a table decoded
        // for it costs memory and saves little.
        let mut scans = HashMap::<&str, usize>::new();
        for table in &plan.table_plans {
            let unfiltered = table.predicates.iter().all(RowPredicate::matches_every_row);
            *scans.entry(&table.table).or_default() += usize::from(unfiltered);
            for relation in &table.relations {
                let broad = match &relation.source_items {
                    Some(items) => items
                        .iter()
                        .all(|&item| table.predicates[item].matches_every_row()),
                    None => unfiltered,
                };
                *scans.entry(&relation.target_table).or_default() += usize::from(broad);
            }
        }
        let shared = scans
            .into_iter()
            .filter(|&(_, count)| count > 1)
            .map(|(table, _)| table.to_owned())
            .collect();
        let block_desc = metadata.table(&plan.block_table)?;
        columns.insert(
            plan.block_table.clone(),
            weight_scan_columns(&plan.block_output_columns, block_desc),
        );
        for table in &plan.table_plans {
            let desc = metadata.table(&table.table)?;
            let primary = columns.entry(table.table.clone()).or_default();
            // Physical positions identify a row both to the weight dedup and to
            // the later read, so wide item keys can stay unread here.
            if !track_positions {
                extend_unique(primary, row_key(desc));
            }
            extend_unique(
                primary,
                weight_scan_columns(&weight_projection(&table.output_columns, Some(desc)), desc),
            );
            for relation in &table.relations {
                extend_unique(primary, relation.left_key.iter().cloned());
                if relation.kind != RelationKind::Join {
                    extend_unique(primary, desc.address_column.iter().cloned());
                }
            }
            for relation in &table.relations {
                let desc = metadata.table(&relation.target_table)?;
                let target = columns.entry(relation.target_table.clone()).or_default();
                if !track_positions {
                    extend_unique(target, row_key(desc));
                }
                extend_unique(target, relation.right_key.iter().cloned());
                if relation.kind != RelationKind::Join {
                    extend_unique(target, desc.address_column.iter().cloned());
                }
                extend_unique(
                    target,
                    weight_scan_columns(
                        &weight_projection(&relation.output_columns, Some(desc)),
                        desc,
                    ),
                );
            }
        }
        // A row without its complete identity cannot be fetched by key. Keep
        // the existing scan path for such schemas, including its error
        // behavior. Only the tables this reader scans are asked (INV-E4).
        for table in columns.keys() {
            let schema = inner.table_schema(table)?;
            let incomplete_key = row_key(metadata.table(table)?)
                .iter()
                .any(|key| schema.index_of(key).is_err());
            if incomplete_key {
                return None;
            }
        }
        Some(Self {
            inner,
            columns,
            track_positions,
            shared,
            decoded: ColumnCache::new(SELECTION_COLUMN_BUDGET),
        })
    }
}

impl SelectionReader<'_> {
    /// Release the columns kept for selection; payload reads do not use them.
    pub(super) fn release_columns(&self) {
        self.decoded.clear();
    }
}

impl ChunkReader for SelectionReader<'_> {
    fn scan_rows(&self, table: &str, request: &ScanRequest) -> Result<Scanned> {
        let Some(columns) = self.columns.get(table) else {
            return self.inner.scan_rows(table, request);
        };
        self.inner.scan_rows(
            table,
            &ScanRequest {
                output_columns: columns.iter().map(String::as_str).collect(),
                positions: self.track_positions,
                column_cache: self.shared.contains(table).then_some(&self.decoded),
                ..request.clone()
            },
        )
    }

    fn has_table(&self, table: &str) -> bool {
        self.inner.has_table(table)
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.inner.table_schema(table)
    }
}

/// Drop rows beyond the selected prefix before retaining another range.
pub(super) fn retain_blocks(rows: &Rows, block_column: &str, selected: &[u64]) -> Result<Rows> {
    rows.filter(|_, batch| {
        blocks_mask(batch, block_column, |block| {
            selected.binary_search(&block).is_ok()
        })
    })
}

/// Read back the rows of `rows` with `output_columns`: by their positions when
/// the scan recorded them, and by their keys otherwise.
pub(super) fn read_rows(
    chunk: &dyn ChunkReader,
    table: &str,
    desc: &TableDescription,
    rows: &Rows,
    output_columns: &[String],
) -> Result<Vec<RecordBatch>> {
    let batches = rows.batches();
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(positions) = rows.sorted_positions() {
        let mut request = ScanRequest::new(output_columns.iter().map(String::as_str).collect());
        request.block_number_column = Some(&desc.block_number_column);
        request.row_indices = Some(&positions);
        return chunk.scan(table, &request);
    }
    let key = row_key(desc);
    let filter = KeyFilter::for_rows(batches, &key, &desc.block_number_column)?;
    let mut request = ScanRequest::new(output_columns.iter().map(String::as_str).collect());
    request.block_number_column = Some(&desc.block_number_column);
    request.key_filter = Some(&filter);
    (request.from_block, request.to_block) =
        compute_block_range(batches, &desc.block_number_column)?;
    chunk.scan(table, &request)
}

/// The selected rows of a range, kept until the page is read back: their
/// identity, and the weight columns no longer needed.
fn retain_identity(rows: &Rows, desc: &TableDescription, selected: &[u64]) -> Result<Rows> {
    let key = row_key(desc);
    retain_blocks(rows, &desc.block_number_column, selected)?
        .map_batches(|batch| project_columns(batch, &key))
}

pub(super) fn retain_selected_keys(
    outputs: &mut HashMap<String, TableOutput>,
    plan: &Plan,
    metadata: &DatasetDescription,
    selected: &[u64],
) -> Result<()> {
    let desc = |table: &str| metadata.table(table).expect("planned table has a catalog");
    for table in &plan.table_plans {
        if let Some(output) = outputs.get_mut(&table.table) {
            output.rows = retain_identity(&output.rows, desc(&table.table), selected)?;
            for (&index, rows) in &mut output.relations {
                let target = desc(&table.relations[index].target_table);
                *rows = retain_identity(rows, target, selected)?;
            }
        }
    }
    Ok(())
}

/// Merge sources with the same projection before reading payloads. A table
/// reached through several relations is decoded once and stored under its first
/// source; output assembly already unions these sources by table name.
pub(super) fn materialize_tables(
    outputs: &mut HashMap<String, TableOutput>,
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
) -> Result<()> {
    struct Source<'a> {
        owner: String,
        relation: Option<usize>,
        table: String,
        projection: Vec<String>,
        rows: Rows,
        primary_predicates: Option<&'a [RowPredicate]>,
    }
    let mut sources = Vec::<Source>::new();
    let mut groups = HashMap::new();
    for table in &plan.table_plans {
        let Some(output) = outputs.get_mut(&table.table) else {
            continue;
        };
        let primary = (None, table.table.as_str(), &table.output_columns);
        let relations = table.relations.iter().enumerate().map(|(index, relation)| {
            (
                Some(index),
                relation.target_table.as_str(),
                &relation.output_columns,
            )
        });
        for (relation, target, projection) in std::iter::once(primary).chain(relations) {
            let rows = match relation {
                None => std::mem::take(&mut output.rows),
                Some(index) => output.relations.remove(&index).unwrap_or_default(),
            };
            if rows.batches().is_empty() {
                continue;
            }
            let desc = metadata.table(target).expect("planned table has a catalog");
            let key = row_key(desc);
            let rows = rows.map_batches(|batch| project_columns(batch, &key))?;
            let is_unfiltered = |predicates: &[RowPredicate]| {
                predicates.iter().any(RowPredicate::matches_every_row)
            };
            let primary_predicates = relation.is_none().then_some(table.predicates.as_slice());
            let group = *groups.entry((target, projection)).or_insert_with(|| {
                let index = sources.len();
                sources.push(Source {
                    owner: table.table.clone(),
                    relation,
                    table: target.to_owned(),
                    projection: projection.clone(),
                    rows: Rows::default(),
                    primary_predicates,
                });
                index
            });
            let source = &mut sources[group];
            if !source.rows.batches().is_empty() {
                // An unfiltered primary already contains every relation row.
                // Otherwise, merging sources needs their union of exact keys.
                source.primary_predicates = source
                    .primary_predicates
                    .filter(|predicates| is_unfiltered(predicates))
                    .or(primary_predicates.filter(|predicates| is_unfiltered(predicates)));
            }
            source.rows.append(rows)?;
        }
    }
    // Each source is its own read of a few row groups; run them side by side.
    let read: Vec<Result<Vec<RecordBatch>>> = sources
        .par_iter()
        .map(|source| {
            let desc = metadata
                .table(&source.table)
                .expect("planned table has a catalog");
            let columns = resolve_relation_output_columns(&source.projection, Some(desc));
            if source.rows.positions().is_some() {
                read_rows(chunk, &source.table, desc, &source.rows, &columns)
            } else if let Some(predicates) = source.primary_predicates {
                // A primary source can reproduce its selected rows with the original
                // predicates and the page bounds. This retains predicate-statistics
                // pruning and avoids decoding a wide item key just to select it again.
                let mut request = ScanRequest::new(columns.iter().map(String::as_str).collect());
                request.predicates = predicates.iter().collect();
                request.block_number_column = Some(&desc.block_number_column);
                (request.from_block, request.to_block) =
                    compute_block_range(source.rows.batches(), &desc.block_number_column)?;
                chunk.scan(&source.table, &request)
            } else {
                read_rows(chunk, &source.table, desc, &source.rows, &columns)
            }
        })
        .collect();

    // The first failure in source order, the one a serial read would report.
    for (source, batches) in sources.iter().zip(read) {
        let batches = batches?;
        let output = outputs
            .get_mut(&source.owner)
            .expect("source has an output slot");
        let rows = Rows::new(batches);
        match source.relation {
            None => output.rows = rows,
            Some(index) => {
                output.relations.insert(index, rows);
            }
        }
    }
    Ok(())
}
