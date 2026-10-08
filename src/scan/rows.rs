use super::ScanRequest;
use anyhow::Result;
use arrow::array::{AsArray, BooleanArray, UInt64Array};
use arrow::compute::{filter, filter_record_batch};
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;

/// Rows read from one table, in batches. When the scan recorded positions,
/// each batch comes with the physical position of every one of its rows, and
/// every operation here keeps the two aligned.
#[derive(Clone, Debug, Default)]
pub struct Rows {
    batches: Vec<RecordBatch>,
    /// One array per batch.
    positions: Option<Vec<UInt64Array>>,
}

impl Rows {
    /// Rows whose positions are not known.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            positions: None,
        }
    }

    /// Rows with the position of each: `positions[i]` belongs to `batches[i]`.
    pub fn with_positions(batches: Vec<RecordBatch>, positions: Vec<UInt64Array>) -> Self {
        let aligned = batches.len() == positions.len()
            && batches
                .iter()
                .zip(&positions)
                .all(|(batch, positions)| batch.num_rows() == positions.len());
        assert!(aligned, "every row has exactly one position");

        Self {
            batches,
            positions: Some(positions),
        }
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }

    /// Each batch's row positions, when the scan recorded them.
    pub fn positions(&self) -> Option<&[UInt64Array]> {
        self.positions.as_deref()
    }

    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Keep the rows `mask` selects in each batch, dropping batches it empties.
    /// Arrow's filter copies a partial batch, so dropped rows do not keep the
    /// batch's buffers alive.
    pub fn filter(
        &self,
        mut mask: impl FnMut(usize, &RecordBatch) -> Result<BooleanArray>,
    ) -> Result<Self> {
        let mut batches = Vec::new();
        let mut positions = self.positions.as_ref().map(|_| Vec::new());
        for (index, batch) in self.batches.iter().enumerate() {
            let mask = mask(index, batch)?;
            if mask.true_count() == 0 {
                continue;
            }

            batches.push(filter_record_batch(batch, &mask)?);
            if let (Some(kept), Some(all)) = (&mut positions, &self.positions) {
                kept.push(
                    filter(&all[index], &mask)?
                        .as_primitive::<UInt64Type>()
                        .clone(),
                );
            }
        }

        Ok(Self { batches, positions })
    }

    /// Replace each batch with what `f` makes of it, which has the same rows.
    pub fn map_batches(self, f: impl FnMut(&RecordBatch) -> Result<RecordBatch>) -> Result<Self> {
        let batches = self.batches.iter().map(f).collect::<Result<Vec<_>>>()?;
        let same_rows = batches
            .iter()
            .zip(&self.batches)
            .all(|(new, old)| new.num_rows() == old.num_rows());
        anyhow::ensure!(
            same_rows,
            "a batch changed its rows where only its columns may change"
        );

        Ok(Self {
            batches,
            positions: self.positions,
        })
    }

    /// Append `other`'s rows. Rows with positions and rows without do not mix,
    /// unless one side has no batches at all.
    pub fn append(&mut self, other: Rows) -> Result<()> {
        if other.batches.is_empty() {
            return Ok(());
        }
        if self.batches.is_empty() {
            *self = other;
            return Ok(());
        }

        match (&mut self.positions, other.positions) {
            (Some(mine), Some(theirs)) => mine.extend(theirs),
            (None, None) => {}
            _ => anyhow::bail!("rows with positions and rows without cannot be merged"),
        }
        self.batches.extend(other.batches);
        Ok(())
    }

    /// Every row's position, sorted and without repeats, when recorded.
    pub fn sorted_positions(&self) -> Option<Vec<u64>> {
        let positions = self.positions.as_ref()?;
        let mut sorted: Vec<u64> = positions
            .iter()
            .flat_map(|batch| batch.values().iter().copied())
            .collect();
        sorted.sort_unstable();
        sorted.dedup();
        Some(sorted)
    }
}

/// What a scan returns: its rows, and for each of the request's item tags,
/// which of those rows one of the tag's items matched.
#[derive(Debug, Default)]
pub struct Scanned {
    pub rows: Rows,
    /// Per tag, in request order: its items and one mask per batch of `rows`.
    tags: Vec<(Vec<usize>, Vec<BooleanArray>)>,
}

impl Scanned {
    /// The rows one of `items` matched, when the request named them as a tag.
    pub fn matched_by(&self, items: &[usize]) -> Option<Vec<RecordBatch>> {
        let (_, masks) = self.tags.iter().find(|(tag, _)| tag == items)?;

        let matched = self
            .rows
            .batches
            .iter()
            .zip(masks)
            .filter(|(_, mask)| mask.true_count() > 0)
            .map(|(batch, mask)| filter_record_batch(batch, mask).expect("one mask entry per row"))
            .collect();
        Some(matched)
    }

    pub(super) fn collect(request: &ScanRequest, parts: Vec<ScannedBatch>) -> Self {
        let mut batches = Vec::with_capacity(parts.len());
        let mut positions = request.positions.then(Vec::new);
        let mut tags: Vec<(Vec<usize>, Vec<BooleanArray>)> = request
            .item_tags
            .iter()
            .map(|items| (items.to_vec(), Vec::new()))
            .collect();

        for part in parts {
            let rows = part.batch.num_rows();
            let tagged =
                part.tags.len() == tags.len() && part.tags.iter().all(|mask| mask.len() == rows);
            assert!(tagged, "every tag has one mask entry per row");

            if let Some(positions) = &mut positions {
                positions.push(
                    part.positions
                        .expect("the scan recorded every row's position"),
                );
            }
            for ((_, masks), mask) in tags.iter_mut().zip(part.tags) {
                masks.push(mask);
            }
            batches.push(part.batch);
        }

        let rows = match positions {
            Some(positions) => Rows::with_positions(batches, positions),
            None => Rows::new(batches),
        };
        Self { rows, tags }
    }
}

/// One batch of a scan, with what the scan learned about its rows.
pub(super) struct ScannedBatch {
    pub(super) batch: RecordBatch,
    pub(super) positions: Option<UInt64Array>,
    /// One mask per tag of the request.
    pub(super) tags: Vec<BooleanArray>,
}

impl ScannedBatch {
    /// A batch no item ran on, so no tag holds for any of its rows.
    pub(super) fn untagged(
        request: &ScanRequest,
        batch: RecordBatch,
        positions: Option<UInt64Array>,
    ) -> Self {
        let none = BooleanArray::from(vec![false; batch.num_rows()]);
        let tags = request.item_tags.iter().map(|_| none.clone()).collect();

        Self {
            batch,
            positions,
            tags,
        }
    }
}
