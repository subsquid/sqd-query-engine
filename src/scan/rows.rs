use super::ScanRequest;
use anyhow::{Context, Result};
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
    /// An error unless every row has exactly one.
    pub fn with_positions(batches: Vec<RecordBatch>, positions: Vec<UInt64Array>) -> Result<Self> {
        let aligned = batches.len() == positions.len()
            && batches
                .iter()
                .zip(&positions)
                .all(|(batch, positions)| batch.num_rows() == positions.len());
        anyhow::ensure!(aligned, "every row must have exactly one position");

        Ok(Self {
            batches,
            positions: Some(positions),
        })
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

    /// Keep only `columns` of each batch, in their order, leaving out those a
    /// batch lacks. Every row keeps its position.
    pub fn project(self, columns: &[impl AsRef<str>]) -> Result<Self> {
        let batches = self
            .batches
            .iter()
            .map(|batch| {
                let schema = batch.schema();
                let indices: Vec<usize> = columns
                    .iter()
                    .filter_map(|name| schema.index_of(name.as_ref()).ok())
                    .collect();
                batch.project(&indices)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

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
#[derive(Debug)]
pub struct Scanned {
    rows: Rows,
    /// Per tag, in request order: its items and one mask per batch of `rows`.
    tags: Vec<(Vec<usize>, Vec<BooleanArray>)>,
}

impl Scanned {
    /// What a reader read for `request`: the rows, and for each of the
    /// request's item tags in order, one mask per batch of `rows` marking the
    /// rows one of the tag's items matched. An error when the masks do not
    /// cover the rows one entry per row, or the rows lack the positions the
    /// request asked for.
    pub fn new(request: &ScanRequest, rows: Rows, tags: Vec<Vec<BooleanArray>>) -> Result<Self> {
        anyhow::ensure!(
            tags.len() == request.item_tags.len(),
            "{} item tags reported where the request asked for {}",
            tags.len(),
            request.item_tags.len()
        );
        for masks in &tags {
            let aligned = masks.len() == rows.batches.len()
                && masks
                    .iter()
                    .zip(&rows.batches)
                    .all(|(mask, batch)| mask.len() == batch.num_rows());
            anyhow::ensure!(aligned, "an item tag must mark every row once");
        }
        anyhow::ensure!(
            !request.positions || rows.positions.is_some(),
            "the request asked for each row's position"
        );

        let items = request.item_tags.iter().map(|items| items.to_vec());
        Ok(Self {
            rows,
            tags: items.zip(tags).collect(),
        })
    }

    pub fn rows(&self) -> &Rows {
        &self.rows
    }

    pub fn into_rows(self) -> Rows {
        self.rows
    }

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

    pub(super) fn collect(request: &ScanRequest, parts: Vec<ScannedBatch>) -> Result<Self> {
        let mut batches = Vec::with_capacity(parts.len());
        let mut positions = request.positions.then(Vec::new);
        let mut tags = vec![Vec::new(); request.item_tags.len()];

        for part in parts {
            anyhow::ensure!(
                part.tags.len() == tags.len(),
                "a batch must carry every tag"
            );
            if let Some(positions) = &mut positions {
                let recorded = part.positions;
                positions.push(recorded.context("the scan must record every row's position")?);
            }
            for (masks, mask) in tags.iter_mut().zip(part.tags) {
                masks.push(mask);
            }
            batches.push(part.batch);
        }

        let rows = match positions {
            Some(positions) => Rows::with_positions(batches, positions)?,
            None => Rows::new(batches),
        };
        Self::new(request, rows, tags)
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
    /// This batch in slices of at most `size` rows, as a reader returns them,
    /// each with its rows' positions and tags.
    pub(super) fn split(self, size: usize) -> Vec<Self> {
        let size = size.max(1);
        let rows = self.batch.num_rows();
        if rows <= size {
            return vec![self];
        }

        (0..rows)
            .step_by(size)
            .map(|start| {
                let length = size.min(rows - start);
                Self {
                    batch: self.batch.slice(start, length),
                    positions: self.positions.as_ref().map(|p| p.slice(start, length)),
                    tags: self
                        .tags
                        .iter()
                        .map(|tag| tag.slice(start, length))
                        .collect(),
                }
            })
            .collect()
    }

    /// The rows `mask` selects, with their positions and tags.
    pub(super) fn filter(self, mask: &BooleanArray) -> Result<Self> {
        let positions = self
            .positions
            .map(|positions| filter(&positions, mask))
            .transpose()?
            .map(|positions| positions.as_primitive::<UInt64Type>().clone());
        let tags = self
            .tags
            .iter()
            .map(|tag| Ok(filter(tag, mask)?.as_boolean().clone()))
            .collect::<Result<_>>()?;

        Ok(Self {
            batch: filter_record_batch(&self.batch, mask)?,
            positions,
            tags,
        })
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::ArrayRef;
    use std::sync::Arc;

    fn batch(rows: u64) -> RecordBatch {
        let values: ArrayRef = Arc::new(UInt64Array::from_iter_values(0..rows));
        RecordBatch::try_from_iter([("n", values)]).unwrap()
    }

    fn marks(lengths: &[usize]) -> Vec<BooleanArray> {
        let mark = |length| BooleanArray::from(vec![true; length]);
        lengths.iter().map(|&length| mark(length)).collect()
    }

    /// A reader's rows and item marks make a scan result only when each tag
    /// marks every row once, and every row has a position when the request
    /// asked for them.
    #[test]
    fn a_scan_result_takes_marks_that_cover_its_rows() {
        let items = [0];
        let mut request = ScanRequest::new(vec!["n"]);
        request.item_tags = vec![&items];
        let rows = || Rows::new(vec![batch(2), batch(3)]);

        let marked = vec![
            BooleanArray::from(vec![true, false]),
            BooleanArray::from(vec![false, false, true]),
        ];
        let scanned = Scanned::new(&request, rows(), vec![marked]).unwrap();
        let matched = scanned.matched_by(&items).unwrap();
        assert_eq!(matched.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(scanned.matched_by(&[1]).is_none());

        let refused = [
            ("a tag left out", vec![]),
            ("a tag too many", vec![marks(&[2, 3]), marks(&[2, 3])]),
            ("a batch without marks", vec![marks(&[2])]),
            ("a mark past the rows", vec![marks(&[2, 4])]),
            ("a row without a mark", vec![marks(&[2, 2])]),
        ];
        for (case, tags) in refused {
            assert!(Scanned::new(&request, rows(), tags).is_err(), "{case}");
        }

        request.positions = true;
        let unplaced = Scanned::new(&request, rows(), vec![marks(&[2, 3])]);
        assert!(unplaced.is_err(), "rows without the positions asked for");
        let positions = vec![
            UInt64Array::from(vec![0, 1]),
            UInt64Array::from(vec![2, 3, 4]),
        ];
        let placed = Rows::with_positions(vec![batch(2), batch(3)], positions).unwrap();
        assert!(Scanned::new(&request, placed, vec![marks(&[2, 3])]).is_ok());

        let short = Rows::with_positions(vec![batch(2)], vec![UInt64Array::from(vec![0])]);
        assert!(short.is_err(), "a row without a position");
    }

    /// A projection keeps every row and its position: in the order asked,
    /// without the columns a batch lacks, and with none of them at all.
    #[test]
    fn a_projection_keeps_every_row_and_its_position() {
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("a", Arc::new(UInt64Array::from(vec![1, 2, 3]))),
            ("b", Arc::new(UInt64Array::from(vec![4, 5, 6]))),
        ];
        let three = RecordBatch::try_from_iter(columns).unwrap();
        let positions = vec![UInt64Array::from(vec![7, 8, 9]), UInt64Array::from(vec![3])];
        let rows = Rows::with_positions(vec![three, batch(1)], positions.clone()).unwrap();

        for asked in [vec!["b", "a"], vec!["b", "n", "missing"], vec![]] {
            let projected = rows.clone().project(&asked).unwrap();

            assert_eq!(projected.positions(), Some(&positions[..]), "{asked:?}");
            for (batch, before) in projected.batches().iter().zip(rows.batches()) {
                assert_eq!(batch.num_rows(), before.num_rows(), "{asked:?}");
                let names: Vec<&str> = asked
                    .iter()
                    .copied()
                    .filter(|name| before.column_by_name(name).is_some())
                    .collect();
                let kept: Vec<String> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect();
                assert_eq!(kept, names, "{asked:?}");
            }
        }
    }

    /// Filtering a scanned batch keeps each kept row's position and tags.
    #[test]
    fn a_filtered_batch_keeps_its_rows_positions_and_tags() {
        let scanned = ScannedBatch {
            batch: batch(4),
            positions: Some(UInt64Array::from(vec![10, 11, 12, 13])),
            tags: vec![
                BooleanArray::from(vec![true, false, true, false]),
                BooleanArray::from(vec![false, false, true, true]),
            ],
        };
        let mask = BooleanArray::from(vec![Some(false), Some(true), None, Some(true)]);

        let kept = scanned.filter(&mask).unwrap();

        let values: ArrayRef = Arc::new(UInt64Array::from(vec![1, 3]));
        assert_eq!(
            kept.batch,
            RecordBatch::try_from_iter([("n", values)]).unwrap()
        );
        assert_eq!(kept.positions, Some(UInt64Array::from(vec![11, 13])));
        assert_eq!(
            kept.tags,
            vec![
                BooleanArray::from(vec![false, false]),
                BooleanArray::from(vec![false, true]),
            ]
        );
    }
}
