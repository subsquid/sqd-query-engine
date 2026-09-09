use super::arrow_out::{filter_to_blocks, project_columns};
use super::block_index::compute_block_range;
use super::columns::resolve_relation_output_columns;
use super::row_writer::build_full_sort_columns;
use super::weight::{weight_projection, weight_scan_columns, TableOutput};
use crate::metadata::{DatasetDescription, TableDescription};
use crate::query::{Plan, RelationKind};
use crate::scan::predicate::RowPredicate;
use crate::scan::{ChunkReader, KeyFilter, ScanRequest};
use anyhow::Result;
use arrow::array::UInt64Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// Read only row identities, relation inputs and size columns while selecting a
/// page. Required output columns are still checked by the underlying scanner.
pub(super) struct SelectionReader<'a> {
    inner: &'a dyn ChunkReader,
    columns: HashMap<String, Vec<String>>,
    track_positions: bool,
}

const ROW_INDEX: &str = "__sqd_selected_row";

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
        if metadata.tables.keys().any(|table| {
            inner
                .table_schema(table)
                .is_some_and(|schema| schema.index_of(ROW_INDEX).is_ok())
        }) {
            return None;
        }
        let track_positions = inner.supports_row_positions();
        let mut source_counts = HashMap::<&str, usize>::new();
        for table in &plan.table_plans {
            *source_counts.entry(&table.table).or_default() += 1;
            for relation in &table.relations {
                *source_counts.entry(&relation.target_table).or_default() += 1;
            }
        }
        let mut columns = HashMap::<String, Vec<String>>::new();
        let block_desc = metadata.table(&plan.block_table)?;
        columns.insert(
            plan.block_table.clone(),
            weight_scan_columns(&plan.block_output_columns, block_desc),
        );
        for table in &plan.table_plans {
            let desc = metadata.table(&table.table)?;
            let primary = columns.entry(table.table.clone()).or_default();
            // A single source needs no weight deduplication. Physical positions
            // identify its rows later, so wide item keys can stay unread here.
            if !track_positions || source_counts[table.table.as_str()] > 1 {
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
                for predicate in relation.source_predicates.iter().flatten() {
                    extend_unique(primary, predicate.columns.iter().map(|p| p.column.clone()));
                }
            }
            for relation in &table.relations {
                let desc = metadata.table(&relation.target_table)?;
                let target = columns.entry(relation.target_table.clone()).or_default();
                if !track_positions || source_counts[relation.target_table.as_str()] > 1 {
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
        // the existing scan path for such schemas, including its error behavior.
        for table in columns.keys() {
            let schema = inner.table_schema(table)?;
            if row_key(metadata.table(table)?)
                .iter()
                .any(|key| schema.index_of(key).is_err())
            {
                return None;
            }
        }
        Some(Self {
            inner,
            columns,
            track_positions,
        })
    }
}

impl ChunkReader for SelectionReader<'_> {
    fn scan(&self, table: &str, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
        let Some(columns) = self.columns.get(table) else {
            return self.inner.scan(table, request);
        };
        self.inner.scan(
            table,
            &ScanRequest {
                output_columns: columns.iter().map(String::as_str).collect(),
                row_index_column: self.track_positions.then_some(ROW_INDEX),
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

/// Drop rows beyond the selected prefix before retaining another range. Arrow's
/// filter copies a partial batch so excluded rows do not keep its buffers alive.
pub(super) fn retain_blocks(
    batches: Vec<RecordBatch>,
    block_column: &str,
    selected: &[u64],
) -> Result<Vec<RecordBatch>> {
    let mut kept = Vec::new();
    for batch in batches {
        let batch = filter_to_blocks(&batch, block_column, |b| selected.binary_search(&b).is_ok())?;
        if batch.num_rows() > 0 {
            kept.push(batch);
        }
    }
    Ok(kept)
}

pub(super) fn read_rows(
    chunk: &dyn ChunkReader,
    table: &str,
    desc: &TableDescription,
    batches: &[RecordBatch],
    output_columns: &[String],
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(rows) = physical_rows(batches) {
        let mut request = ScanRequest::new(output_columns.iter().map(String::as_str).collect());
        request.block_number_column = Some(&desc.block_number_column);
        request.row_indices = Some(&rows);
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

fn physical_rows(batches: &[RecordBatch]) -> Option<Vec<u64>> {
    let mut rows = Vec::new();
    for batch in batches {
        let positions = batch
            .column_by_name(ROW_INDEX)?
            .as_any()
            .downcast_ref::<UInt64Array>()?;
        rows.extend_from_slice(positions.values());
    }
    rows.sort_unstable();
    rows.dedup();
    Some(rows)
}

fn retained_columns(desc: &TableDescription) -> Vec<String> {
    let mut columns = row_key(desc);
    columns.push(ROW_INDEX.to_owned());
    columns
}

pub(super) fn retain_selected_keys(
    outputs: &mut HashMap<String, TableOutput>,
    plan: &Plan,
    metadata: &DatasetDescription,
    selected: &[u64],
) -> Result<()> {
    let retain = |batches: &mut Vec<RecordBatch>, table: &str| -> Result<()> {
        let desc = metadata.table(table).expect("planned table has a catalog");
        let key = retained_columns(desc);
        *batches = retain_blocks(std::mem::take(batches), &desc.block_number_column, selected)?
            .iter()
            .map(|batch| project_columns(batch, &key))
            .collect::<Result<_>>()?;
        Ok(())
    };
    for table in &plan.table_plans {
        if let Some(output) = outputs.get_mut(&table.table) {
            retain(&mut output.batches, &table.table)?;
            for (&index, batches) in &mut output.relation_batches {
                retain(batches, &table.relations[index].target_table)?;
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
        batches: Vec<RecordBatch>,
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
            let batches = match relation {
                None => std::mem::take(&mut output.batches),
                Some(index) => output.relation_batches.remove(&index).unwrap_or_default(),
            };
            if batches.is_empty() {
                continue;
            }
            let desc = metadata.table(target).expect("planned table has a catalog");
            let key = retained_columns(desc);
            let batches = batches
                .iter()
                .map(|batch| project_columns(batch, &key))
                .collect::<Result<Vec<_>>>()?;
            let is_unfiltered = |predicates: &[RowPredicate]| {
                predicates
                    .iter()
                    .any(|predicate| predicate.columns.is_empty())
            };
            let primary_predicates = relation.is_none().then_some(table.predicates.as_slice());
            let group = *groups.entry((target, projection)).or_insert_with(|| {
                let index = sources.len();
                sources.push(Source {
                    owner: table.table.clone(),
                    relation,
                    table: target.to_owned(),
                    projection: projection.clone(),
                    batches: Vec::new(),
                    primary_predicates,
                });
                index
            });
            let source = &mut sources[group];
            if !source.batches.is_empty() {
                // An unfiltered primary already contains every relation row.
                // Otherwise, merging sources needs their union of exact keys.
                source.primary_predicates = source
                    .primary_predicates
                    .filter(|predicates| is_unfiltered(predicates))
                    .or(primary_predicates.filter(|predicates| is_unfiltered(predicates)));
            }
            source.batches.extend(batches);
        }
    }
    for source in sources {
        let desc = metadata
            .table(&source.table)
            .expect("planned table has a catalog");
        let columns = resolve_relation_output_columns(&source.projection, Some(desc));
        let batches = if let Some(rows) = physical_rows(&source.batches) {
            let mut request = ScanRequest::new(columns.iter().map(String::as_str).collect());
            request.block_number_column = Some(&desc.block_number_column);
            request.row_indices = Some(&rows);
            chunk.scan(&source.table, &request)?
        } else if let Some(predicates) = source.primary_predicates {
            // A primary source can reproduce its selected rows with the original
            // predicates and the page bounds. This retains predicate-statistics
            // pruning and avoids decoding a wide item key just to select it again.
            let mut request = ScanRequest::new(columns.iter().map(String::as_str).collect());
            request.predicates = predicates.iter().collect();
            request.block_number_column = Some(&desc.block_number_column);
            (request.from_block, request.to_block) =
                compute_block_range(&source.batches, &desc.block_number_column)?;
            chunk.scan(&source.table, &request)?
        } else {
            read_rows(chunk, &source.table, desc, &source.batches, &columns)?
        };
        let output = outputs
            .get_mut(&source.owner)
            .expect("source has an output slot");
        match source.relation {
            None => output.batches = batches,
            Some(index) => {
                output.relation_batches.insert(index, batches);
            }
        }
    }
    Ok(())
}
