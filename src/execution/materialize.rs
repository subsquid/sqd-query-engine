use super::page::Page;
use super::prepare::{Execution, PlannedTable};
use super::Stopwatch;
use crate::metadata::TableDescription;
use crate::output::block_index::compute_block_range;
use crate::output::blocks_mask;
use crate::output::projection::{push_unique, row_key, ResolvedProjection};
use crate::output::weight::TableOutput;
use crate::query::RelationKind;
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
    columns: HashMap<&'a str, Vec<&'a str>>,
    track_positions: bool,
    /// Tables more than one scan reads: their columns are decoded once, kept
    /// here, and filtered in memory by each.
    shared: HashSet<&'a str>,
    decoded: ColumnCache,
}

/// Decoded key and size columns one query may keep while it selects a page.
const SELECTION_COLUMN_BUDGET: u64 = 64 << 20;

/// Each table a selection reads, with its catalog entry and the columns read
/// of it.
type TableColumns<'a> = HashMap<&'a str, (&'a TableDescription, Vec<&'a str>)>;

/// The columns read of `projection`'s table.
fn entry<'c, 'a>(
    columns: &'c mut TableColumns<'a>,
    projection: &ResolvedProjection<'a>,
) -> &'c mut Vec<&'a str> {
    let (_, columns) = columns
        .entry(projection.table())
        .or_insert_with(|| (projection.desc(), Vec::new()));
    columns
}

impl<'a> SelectionReader<'a> {
    pub(super) fn new(
        inner: &'a dyn ChunkReader,
        header: &ResolvedProjection<'a>,
        tables: &[PlannedTable<'a>],
    ) -> Option<Self> {
        let track_positions = inner.supports_row_positions();
        // Only scans that read most of a table share it: a relation from a
        // selective item reads a few row groups, and keeping a table decoded
        // for it costs memory and saves little.
        let mut scans = HashMap::<&str, usize>::new();
        for table in tables {
            let predicates = &table.plan.predicates;
            let unfiltered = predicates.iter().all(RowPredicate::matches_every_row);
            *scans.entry(&table.plan.table).or_default() += usize::from(unfiltered);
            for relation in &table.plan.relations {
                let broad = match &relation.source_items {
                    Some(items) => items
                        .iter()
                        .all(|&item| predicates[item].matches_every_row()),
                    None => unfiltered,
                };
                *scans.entry(&relation.target_table).or_default() += usize::from(broad);
            }
        }
        let shared = scans
            .into_iter()
            .filter(|&(_, count)| count > 1)
            .map(|(table, _)| table)
            .collect();

        let mut columns = TableColumns::new();
        entry(&mut columns, header).extend_from_slice(header.size_columns());
        for table in tables {
            let desc = table.desc;
            let primary = entry(&mut columns, &table.projection);
            // Physical positions identify a row both to the weight dedup and to
            // the later read, so wide item keys can stay unread here.
            if !track_positions {
                push_unique(primary, row_key(desc));
            }
            push_unique(primary, table.projection.size_columns().iter().copied());
            for relation in &table.plan.relations {
                push_unique(primary, relation.left_key.iter().map(String::as_str));
                if relation.kind != RelationKind::Join {
                    push_unique(primary, desc.address_column.as_deref());
                }
            }
            for (relation, projection) in table.relations() {
                let desc = projection.desc();
                let target = entry(&mut columns, projection);
                if !track_positions {
                    push_unique(target, row_key(desc));
                }
                push_unique(target, relation.right_key.iter().map(String::as_str));
                if relation.kind != RelationKind::Join {
                    push_unique(target, desc.address_column.as_deref());
                }
                push_unique(target, projection.size_columns().iter().copied());
            }
        }
        // A row without its complete identity cannot be fetched by key. Keep
        // the existing scan path for such schemas, including its error
        // behavior. Only the tables this reader scans are asked (INV-E4).
        for (&table, (desc, _)) in &columns {
            let schema = inner.table_schema(table)?;
            let incomplete_key = row_key(desc)
                .iter()
                .any(|key| schema.index_of(key).is_err());
            if incomplete_key {
                return None;
            }
        }
        let columns = columns
            .into_iter()
            .map(|(table, (_, columns))| (table, columns))
            .collect();
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
    fn release_columns(&self) {
        self.decoded.clear();
    }

    /// `request` as this reader runs it on `table`: its selection columns, with
    /// positions. `None` for a table it reads as asked.
    fn narrowed<'r>(&'r self, table: &str, request: &ScanRequest<'r>) -> Option<ScanRequest<'r>> {
        let columns = self.columns.get(table)?;

        Some(ScanRequest {
            output_columns: columns.clone(),
            positions: self.track_positions,
            column_cache: self.shared.contains(table).then_some(&self.decoded),
            ..request.clone()
        })
    }
}

impl ChunkReader for SelectionReader<'_> {
    fn scan_rows(&self, table: &str, request: &ScanRequest) -> Result<Scanned> {
        match self.narrowed(table, request) {
            Some(narrowed) => self.inner.scan_rows(table, &narrowed),
            None => self.inner.scan_rows(table, request),
        }
    }

    fn estimate_scan_bytes(&self, table: &str, request: &ScanRequest) -> Option<u64> {
        match self.narrowed(table, request) {
            Some(narrowed) => self.inner.estimate_scan_bytes(table, &narrowed),
            None => self.inner.estimate_scan_bytes(table, request),
        }
    }

    fn has_table(&self, table: &str) -> bool {
        self.inner.has_table(table)
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.inner.table_schema(table)
    }
}

/// Drop rows beyond the selected prefix before retaining another range.
fn retain_blocks(rows: &Rows, block_column: &str, selected: &[u64]) -> Result<Rows> {
    rows.filter(|_, batch| {
        blocks_mask(batch, block_column, |block| {
            selected.binary_search(&block).is_ok()
        })
    })
}

/// Read back the rows of `rows` with `output_columns`: by their positions when
/// the scan recorded them, and by their keys otherwise.
fn read_rows(
    chunk: &dyn ChunkReader,
    table: &str,
    desc: &TableDescription,
    rows: &Rows,
    output_columns: &[&str],
) -> Result<Vec<RecordBatch>> {
    let batches = rows.batches();
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(positions) = rows.sorted_positions() {
        let mut request = ScanRequest::new(output_columns.to_vec());
        request.block_number_column = Some(&desc.block_number_column);
        request.row_indices = Some(&positions);
        return chunk.scan(table, &request);
    }
    let key: Vec<String> = row_key(desc).into_iter().map(str::to_owned).collect();
    let filter = KeyFilter::for_rows(batches, &key, &desc.block_number_column)?;
    let mut request = ScanRequest::new(output_columns.to_vec());
    request.block_number_column = Some(&desc.block_number_column);
    request.key_filter = Some(&filter);
    (request.from_block, request.to_block) =
        compute_block_range(batches, &desc.block_number_column)?;
    chunk.scan(table, &request)
}

/// The selected rows of a range, kept until the page is read back: their
/// identity, and the weight columns no longer needed.
fn retain_identity(rows: &Rows, desc: &TableDescription, selected: &[u64]) -> Result<Rows> {
    retain_blocks(rows, &desc.block_number_column, selected)?.project(&row_key(desc))
}

pub(super) fn retain_selected_keys(
    outputs: &mut HashMap<String, TableOutput>,
    tables: &[PlannedTable],
    selected: &[u64],
) -> Result<()> {
    for table in tables {
        if let Some(output) = outputs.get_mut(&table.plan.table) {
            output.rows = retain_identity(&output.rows, table.desc, selected)?;
            for (&index, rows) in &mut output.relations {
                let target = table.targets[index].desc();
                *rows = retain_identity(rows, target, selected)?;
            }
        }
    }
    Ok(())
}

/// Read back what a deferred selection left out: the output columns of every
/// selected row, and the headers of the selected blocks. A page selected with
/// its payloads comes back as it is.
pub(super) fn materialize_page(execution: &Execution, page: Page) -> Result<Page> {
    let Some(reader) = &execution.selection else {
        return Ok(page);
    };
    let Page {
        blocks,
        headers,
        mut tables,
        read_through,
    } = page;
    let (plan, chunk, desc) = (execution.plan, execution.chunk, execution.blocks);

    reader.release_columns();
    let started = Stopwatch::start(execution.options.profile);
    let (read, headers) = rayon::join(
        || materialize_tables(&mut tables, &execution.tables, chunk),
        || -> Result<Vec<RecordBatch>> {
            let headers = retain_blocks(&headers, &desc.block_number_column, &blocks)?;
            let columns = execution.header.row_columns();
            read_rows(chunk, &plan.block_table, desc, &headers, columns)
        },
    );
    read?;
    started.report("materialize selected rows");

    Ok(Page {
        blocks,
        headers: Rows::new(headers?),
        tables,
        read_through,
    })
}

/// Merge sources with the same projection before reading payloads. A table
/// reached through several relations is decoded once and stored under its first
/// source; `output_tables` already unions these sources by table name.
fn materialize_tables(
    outputs: &mut HashMap<String, TableOutput>,
    tables: &[PlannedTable],
    chunk: &dyn ChunkReader,
) -> Result<()> {
    struct Source<'a> {
        owner: &'a str,
        relation: Option<usize>,
        projection: &'a ResolvedProjection<'a>,
        rows: Rows,
        primary_predicates: Option<&'a [RowPredicate]>,
    }
    let mut sources = Vec::<Source>::new();
    let mut groups = HashMap::new();
    for table in tables {
        let Some(output) = outputs.get_mut(&table.plan.table) else {
            continue;
        };
        let primary = (None, &table.projection);
        let relations = table.targets.iter().enumerate();
        let relations = relations.map(|(index, target)| (Some(index), target));
        for (relation, projection) in std::iter::once(primary).chain(relations) {
            let rows = match relation {
                None => std::mem::take(&mut output.rows),
                Some(index) => output.relations.remove(&index).unwrap_or_default(),
            };
            if rows.batches().is_empty() {
                continue;
            }
            let rows = rows.project(&row_key(projection.desc()))?;
            let is_unfiltered = |predicates: &[RowPredicate]| {
                predicates.iter().any(RowPredicate::matches_every_row)
            };
            let predicates = table.plan.predicates.as_slice();
            let primary_predicates = relation.is_none().then_some(predicates);
            let read = (projection.table(), projection.names());
            let group = *groups.entry(read).or_insert_with(|| {
                let index = sources.len();
                sources.push(Source {
                    owner: &table.plan.table,
                    relation,
                    projection,
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
            let (table, desc) = (source.projection.table(), source.projection.desc());
            let columns = source.projection.row_columns();
            if source.rows.positions().is_some() {
                read_rows(chunk, table, desc, &source.rows, columns)
            } else if let Some(predicates) = source.primary_predicates {
                // A primary source can reproduce its selected rows with the original
                // predicates and the page bounds. This retains predicate-statistics
                // pruning and avoids decoding a wide item key just to select it again.
                let mut request = ScanRequest::new(columns.to_vec());
                request.predicates = predicates.iter().collect();
                request.block_number_column = Some(&desc.block_number_column);
                (request.from_block, request.to_block) =
                    compute_block_range(source.rows.batches(), &desc.block_number_column)?;
                chunk.scan(table, &request)
            } else {
                read_rows(chunk, table, desc, &source.rows, columns)
            }
        })
        .collect();

    // The first failure in source order, the one a serial read would report.
    for (source, batches) in sources.iter().zip(read) {
        let batches = batches?;
        let output = outputs
            .get_mut(source.owner)
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
