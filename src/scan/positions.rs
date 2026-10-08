use super::predicate::or_masks;
use super::rows::ScannedBatch;
use super::scanner::{build_output_schema, project_batch};
use super::{ParquetTable, ScanRequest};
use anyhow::{Context, Result};
use arrow::array::{Array, AsArray, BooleanArray, UInt64Array};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;
use std::sync::{Arc, Mutex};

/// Observe the existing filter pipeline without evaluating its predicates again.
/// Parquet evaluates every stage eagerly during reader construction. Each later
/// mask addresses the rows surviving earlier stages, so the masks compose with
/// `and_then`, not an intersection of physical positions.
#[derive(Default)]
pub(super) struct TrackedRows {
    stages: Vec<Arc<Mutex<Vec<BooleanArray>>>>,
}

impl TrackedRows {
    pub(super) fn wrap(
        &mut self,
        predicates: Vec<Box<dyn ArrowPredicate>>,
    ) -> Vec<Box<dyn ArrowPredicate>> {
        predicates
            .into_iter()
            .map(|mut predicate| {
                let masks = Arc::new(Mutex::new(Vec::new()));
                self.stages.push(masks.clone());
                Box::new(ArrowPredicateFn::new(
                    predicate.projection().clone(),
                    move |batch| {
                        let mask = predicate.evaluate(batch)?;
                        let recorded = if mask.null_count() == 0 {
                            mask.clone()
                        } else {
                            BooleanArray::from_iter(
                                mask.iter().map(|value| Some(value.unwrap_or(false))),
                            )
                        };
                        masks
                            .lock()
                            .expect("position recorder poisoned")
                            .push(recorded);
                        Ok(mask)
                    },
                )) as Box<dyn ArrowPredicate>
            })
            .collect()
    }

    pub(super) fn finish(self, table: &ParquetTable, groups: &[usize]) -> UInt64Array {
        let total = groups
            .iter()
            .map(|&group| table.row_group(group).num_rows() as usize)
            .sum();
        let mut selection = RowSelection::from(vec![RowSelector::select(total)]);
        for stage in self.stages {
            if !selection.selects_any() {
                break;
            }
            let masks = stage.lock().expect("position recorder poisoned");
            selection = selection.and_then(&RowSelection::from_filters(&masks));
        }
        let mut starts = Vec::with_capacity(table.num_row_groups());
        let mut offset = 0u64;
        for group in table.metadata().row_groups() {
            starts.push(offset);
            offset += group.num_rows() as u64;
        }
        let mut physical = groups.iter().flat_map(|&group| {
            let start = starts[group];
            start..start + table.row_group(group).num_rows() as u64
        });
        let mut positions = Vec::with_capacity(selection.row_count());
        for selector in selection.iter() {
            if selector.skip {
                if selector.row_count > 0 {
                    physical.nth(selector.row_count - 1);
                }
            } else {
                positions.extend(physical.by_ref().take(selector.row_count));
            }
        }
        UInt64Array::from(positions)
    }
}

/// Whether each tag's items matched a row, kept by the stage that evaluates
/// the items. That stage is the last one, so the rows it selects are the rows
/// the reader returns, in the same order.
pub(super) struct ItemTags {
    tags: Vec<Vec<usize>>,
    recorded: Mutex<Vec<Vec<BooleanArray>>>,
}

impl ItemTags {
    /// Tags over the `active` items only, the ones the scan evaluates; an
    /// item left out matches no row.
    pub(super) fn new(request: &ScanRequest, active: &[usize]) -> Option<Arc<Self>> {
        let tags = request
            .item_tags
            .iter()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| active.iter().position(|a| a == item))
                    .collect()
            })
            .collect::<Vec<_>>();

        (!tags.is_empty()).then(|| {
            Arc::new(Self {
                tags,
                recorded: Mutex::new(Vec::new()),
            })
        })
    }

    /// Keep each tag's answer for the rows `matched` selects. A row whose
    /// items say neither yes nor no is not one they matched.
    pub(super) fn record(&self, masks: &[BooleanArray], matched: &BooleanArray) {
        let tagged = self
            .tags
            .iter()
            .map(|items| {
                let mask = or_masks(masks, items, matched.len());
                let kept = arrow::compute::filter(&mask, matched).expect("mask sized to the batch");
                let kept = kept.as_boolean();
                let values = match kept.nulls() {
                    Some(nulls) => kept.values() & nulls.inner(),
                    None => kept.values().clone(),
                };
                BooleanArray::new(values, None)
            })
            .collect();

        self.recorded
            .lock()
            .expect("item tag recorder poisoned")
            .push(tagged);
    }

    /// Each tag over every row the reader returned.
    pub(super) fn finish(&self) -> Vec<BooleanArray> {
        let recorded = self.recorded.lock().expect("item tag recorder poisoned");

        (0..self.tags.len())
            .map(|tag| {
                let parts: Vec<&dyn Array> = recorded
                    .iter()
                    .map(|batch| &batch[tag] as &dyn Array)
                    .collect();
                if parts.is_empty() {
                    return BooleanArray::from(Vec::<bool>::new());
                }
                let joined = arrow::compute::concat(&parts).expect("tags are boolean");
                joined.as_boolean().clone()
            })
            .collect()
    }
}

/// Decode only the requested physical rows, without re-reading predicate or
/// relation keys. Row selections are relative to each selected row group.
pub(super) fn read_rows(
    table: &ParquetTable,
    request: &ScanRequest,
    rows: &[u64],
) -> Result<Vec<ScannedBatch>> {
    crate::engine_ensure!(
        rows.windows(2).all(|pair| pair[0] < pair[1])
            && rows
                .last()
                .is_none_or(|&row| row < table.metadata().file_metadata().num_rows() as u64),
        crate::error::ErrorKind::MalformedChunkData,
        "physical row selection is not sorted, unique and in bounds"
    );
    let schema = build_output_schema(table.schema(), &request.output_columns);
    let indices = request
        .output_columns
        .iter()
        .filter_map(|column| table.schema().index_of(column).ok())
        .collect::<Vec<_>>();
    let projection =
        ProjectionMask::roots(table.metadata().file_metadata().schema_descr(), indices);
    let mut offset = 0u64;
    let mut pending = rows;
    let mut groups = Vec::new();
    for group in 0..table.num_row_groups() {
        let count = table.row_group(group).num_rows() as usize;
        let end = offset + count as u64;
        let split = pending.partition_point(|&row| row < end);
        let (selected, rest) = pending.split_at(split);
        if !selected.is_empty() {
            groups.push((group, offset, count, selected));
        }
        pending = rest;
        offset = end;
    }
    let batches = groups
        .par_iter()
        .map(|&(group, offset, count, selected)| {
            let selection = RowSelection::from_consecutive_ranges(
                selected.iter().map(|&row| {
                    let position = (row - offset) as usize;
                    position..position + 1
                }),
                count,
            );
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                table.data(),
                table.arrow_metadata().clone(),
            )
            .with_row_groups(vec![group])
            .with_row_selection(selection)
            .with_projection(projection.clone())
            .with_batch_size(request.batch_size)
            .build()
            .context("building selected-row reader")?;
            let mut position_offset = 0;
            reader
                .map(|batch| {
                    let batch = project_batch(&batch.context("reading selected rows")?, &schema)?;
                    let end = position_offset + batch.num_rows();
                    let positions = request
                        .positions
                        .then(|| UInt64Array::from(selected[position_offset..end].to_vec()));
                    position_offset = end;
                    Ok(ScannedBatch::untagged(request, batch, positions))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(batches.into_iter().flatten().collect())
}
