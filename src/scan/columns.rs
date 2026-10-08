//! Whole columns of row groups, decoded once for every scan of a query pass
//! that filters the same table.
//!
//! A query that reads most of a table reaches it through many scans: its own
//! items and every relation that targets it. Each scan of the table in one pass
//! reads the same columns, so [`ColumnCache`] decodes a row group's rows inside
//! the pass's block range once, and each scan filters them in memory.
//!
//! The budget bounds what the cache decodes, not only what it keeps: a column
//! is decoded at the window's rows alone, after the most it can decode to is
//! counted against the budget. A row group the budget cannot take is read by
//! each scan as any other scan reads it.

use super::ParquetTable;
use arrow::array::{Array, ArrayRef, UInt32Array};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Decoded columns of row groups that several scans filter, kept until the
/// cache is cleared, up to a byte budget.
#[derive(Clone)]
pub struct ColumnCache {
    inner: Arc<Inner>,
}

struct Inner {
    budget: u64,
    used: AtomicU64,
    windows: Mutex<FxHashMap<WindowKey, Arc<Slot<Held<WindowRows>>>>>,
    columns: Mutex<FxHashMap<ColumnKey, Arc<Slot<Held<ArrayRef>>>>>,
}

/// A row group of a file, by the metadata the slot holds alive so that the
/// address cannot name another file while cached, and a block range.
type WindowKey = (usize, usize, u64, u64);

/// A row group of a file, an Arrow field of it, and a block range.
type ColumnKey = (usize, usize, usize, u64, u64);

/// The block range one pass of a query reads. Every scan of the pass reads
/// rows inside it, so the rows outside are never decoded into the cache.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Window {
    pub from: Option<u64>,
    pub to: Option<u64>,
}

impl Window {
    fn bounds(&self) -> (u64, u64) {
        (self.from.unwrap_or(0), self.to.unwrap_or(u64::MAX))
    }
}

/// The rows of one row group inside a window: all of them, or these, by
/// their offsets into the group.
pub(super) enum WindowRows {
    All(usize),
    Some(UInt32Array),
}

impl WindowRows {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::All(rows) => *rows,
            Self::Some(offsets) => offsets.len(),
        }
    }

    /// The offset into the group of the window's `row`th row.
    pub(super) fn offset(&self, row: usize) -> usize {
        match self {
            Self::All(_) => row,
            Self::Some(offsets) => offsets.value(row) as usize,
        }
    }
}

struct Slot<T> {
    held: Mutex<Option<Arc<T>>>,
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Self {
            held: Mutex::new(None),
        }
    }
}

/// A value derived from a file, which it holds alive.
pub(super) struct Held<T> {
    _file: Arc<ParquetMetaData>,
    pub(super) value: T,
}

impl ColumnCache {
    pub fn new(budget: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                budget,
                used: AtomicU64::new(0),
                windows: Mutex::new(FxHashMap::default()),
                columns: Mutex::new(FxHashMap::default()),
            }),
        }
    }

    /// Bytes held.
    pub fn used(&self) -> u64 {
        self.inner.used.load(Ordering::Relaxed)
    }

    /// Drop everything held. Readers already open keep what they hold.
    pub fn clear(&self) {
        self.inner
            .windows
            .lock()
            .expect("column cache poisoned")
            .clear();
        self.inner
            .columns
            .lock()
            .expect("column cache poisoned")
            .clear();
        self.inner.used.store(0, Ordering::Relaxed);
    }

    /// The rows of row group `group` inside `window`, found by `rows` the
    /// first time. `None` when the budget cannot take an offset per row.
    pub(super) fn window_rows(
        &self,
        table: &ParquetTable,
        group: usize,
        window: Window,
        rows: impl FnOnce() -> anyhow::Result<WindowRows>,
    ) -> anyhow::Result<Option<Arc<Held<WindowRows>>>> {
        let file = table.metadata_arc();
        let (from, to) = window.bounds();
        let key = (Arc::as_ptr(&file) as usize, group, from, to);
        let most = table.row_group(group).num_rows().max(0) as u64 * 4;
        let size = |held: &Held<WindowRows>| match &held.value {
            WindowRows::All(_) => 0,
            WindowRows::Some(offsets) => offsets.get_array_memory_size() as u64,
        };

        hold(
            &self.inner.windows,
            &self.inner,
            key,
            Some(most),
            size,
            || {
                Ok(Held {
                    _file: file.clone(),
                    value: rows()?,
                })
            },
        )
    }

    /// Column `index` of row group `group`, at the window's `rows`. `None`
    /// when the budget cannot take the most the column may decode to, or the
    /// footer does not bound it.
    pub(super) fn column(
        &self,
        table: &ParquetTable,
        group: usize,
        index: usize,
        window: Window,
        rows: &WindowRows,
    ) -> anyhow::Result<Option<ArrayRef>> {
        let file = table.metadata_arc();
        let (from, to) = window.bounds();
        let key = (Arc::as_ptr(&file) as usize, group, index, from, to);
        let most = super::scanner::column_bytes_bound(table, group, index);
        let size = |held: &Held<ArrayRef>| held.value.get_array_memory_size() as u64;

        let held = hold(&self.inner.columns, &self.inner, key, most, size, || {
            Ok(Held {
                _file: file.clone(),
                value: decode_rows(table, group, index, rows)?,
            })
        })?;
        Ok(held.map(|held| held.value.clone()))
    }
}

/// Column `index` of row group `group` at `rows` alone: the reader skips the
/// other rows instead of decoding them.
fn decode_rows(
    table: &ParquetTable,
    group: usize,
    index: usize,
    rows: &WindowRows,
) -> anyhow::Result<ArrayRef> {
    let projection =
        ProjectionMask::roots(table.metadata().file_metadata().schema_descr(), [index]);
    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        table.data(),
        table.arrow_metadata().clone(),
    )
    .with_projection(projection)
    .with_row_groups(vec![group])
    .with_batch_size(rows.len().max(1));
    if let WindowRows::Some(offsets) = rows {
        let total = table.row_group(group).num_rows().max(0) as usize;
        builder =
            builder.with_row_selection(RowSelection::from_consecutive_ranges(runs(offsets), total));
    }

    let batches = builder.build()?.collect::<Result<Vec<_>, _>>()?;
    Ok(match batches.as_slice() {
        [] => arrow::array::new_empty_array(table.schema().field(index).data_type()),
        [batch] => batch.column(0).clone(),
        _ => {
            let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
            arrow::compute::concat(&parts)?
        }
    })
}

/// Sorted offsets as runs of consecutive rows.
fn runs(offsets: &UInt32Array) -> impl Iterator<Item = std::ops::Range<usize>> + '_ {
    let mut offsets = offsets
        .values()
        .iter()
        .map(|&offset| offset as usize)
        .peekable();

    std::iter::from_fn(move || {
        let start = offsets.next()?;
        let mut end = start + 1;
        while offsets.next_if_eq(&end).is_some() {
            end += 1;
        }
        Some(start..end)
    })
}

/// The value `map` holds under `key`, loaded by the first caller while the
/// others wait. The `most` it may take is counted against the budget before it
/// is loaded, and what it takes once it is; `None` when that does not fit.
fn hold<K: std::hash::Hash + Eq, T>(
    map: &Mutex<FxHashMap<K, Arc<Slot<T>>>>,
    inner: &Inner,
    key: K,
    most: Option<u64>,
    size: impl FnOnce(&T) -> u64,
    load: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<Option<Arc<T>>> {
    let slot = map
        .lock()
        .expect("column cache poisoned")
        .entry(key)
        .or_default()
        .clone();

    let mut held = slot.held.lock().expect("column cache slot poisoned");
    if let Some(value) = held.as_ref() {
        return Ok(Some(value.clone()));
    }

    let Some(most) = most.filter(|&most| inner.reserve(most)) else {
        return Ok(None);
    };
    let value = match load() {
        Ok(value) => Arc::new(value),
        Err(error) => {
            inner.settle(most, 0);
            return Err(error);
        }
    };

    inner.settle(most, size(&value));
    *held = Some(value.clone());
    Ok(Some(value))
}

impl Inner {
    /// Count `bytes` against the budget, if they fit in what is left of it.
    fn reserve(&self, bytes: u64) -> bool {
        self.update(|used| {
            used.checked_add(bytes)
                .filter(|&total| total <= self.budget)
        })
    }

    /// Count `actual` bytes in place of the `reserved` ones.
    fn settle(&self, reserved: u64, actual: u64) {
        self.update(|used| Some(used.saturating_sub(reserved).saturating_add(actual)));
    }

    /// Replace the bytes counted with what `next` makes of them, unless it
    /// declines.
    fn update(&self, next: impl Fn(u64) -> Option<u64>) -> bool {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let Some(updated) = next(used) else {
                return false;
            };
            match self.used.compare_exchange_weak(
                used,
                updated,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(now) => used = now,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, UInt16Type};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    /// Nullable integers, strings and paths, in row groups of 64 rows and
    /// pages of 3.
    fn write(dir: &std::path::Path) -> ParquetTable {
        let rows = 250;
        let number = Int32Array::from_iter((0..rows).map(|i| (i % 5 != 4).then_some(i / 3)));
        let text =
            StringArray::from_iter((0..rows).map(|i| (i % 7 != 6).then(|| format!("v{}", i % 11))));
        let path = ListArray::from_iter_primitive::<UInt16Type, _, _>((0..rows).map(|i| {
            (i % 9 != 8).then(|| {
                (0..i % 4)
                    .map(|d| Some((i + d) as u16 % 6))
                    .collect::<Vec<_>>()
            })
        }));
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::Int32, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("path", path.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(number), Arc::new(text), Arc::new(path)],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_size(64)
            .set_data_page_row_count_limit(3)
            .set_write_batch_size(3)
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let path = dir.join("table.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        ParquetTable::open(&path).unwrap()
    }

    /// A window decodes its own rows of a column, not the whole row group
    /// around them.
    #[test]
    fn a_column_decodes_only_the_window_rows() {
        let dir = tempfile::tempdir().unwrap();
        let table = crate::testing::wide_table(dir.path());
        let window = Window {
            from: Some(1000),
            to: Some(1000),
        };
        let one = WindowRows::Some(arrow::array::UInt32Array::from(vec![7]));
        let cache = ColumnCache::new(u64::MAX);

        let (column, peak) =
            crate::testing::peak_bytes(|| cache.column(&table, 0, 2, window, &one).unwrap());

        assert_eq!(column.map(|column| column.len()), Some(1));
        let whole = crate::testing::WIDE_PAYLOAD;
        assert!(
            peak < whole / 4,
            "{peak} bytes to decode one row of {whole}"
        );
    }

    /// A column holds the rows parquet reads at the window's offsets, the
    /// same on every ask, and none when the budget cannot take it. What the
    /// cache counts never passes its budget.
    #[test]
    fn a_decoded_column_holds_the_window_rows_parquet_reads() {
        let dir = tempfile::tempdir().unwrap();
        let table = write(dir.path());
        let whole = Window::default();
        let part = Window {
            from: Some(10),
            to: Some(20),
        };
        let none = Window {
            from: Some(30),
            to: Some(30),
        };

        for budget in [0, 600, u64::MAX] {
            let cache = ColumnCache::new(budget);
            for group in 0..table.num_row_groups() {
                let rows = table.row_group(group).num_rows() as usize;
                let some: UInt32Array = (0..rows as u32).filter(|row| row % 3 != 1).collect();
                for column in 0..3 {
                    let mask = ProjectionMask::roots(
                        table.metadata().file_metadata().schema_descr(),
                        [column],
                    );
                    let expected = ParquetRecordBatchReaderBuilder::new_with_metadata(
                        table.data(),
                        table.arrow_metadata().clone(),
                    )
                    .with_projection(mask)
                    .with_row_groups(vec![group])
                    .build()
                    .unwrap()
                    .map(|batch| batch.unwrap().column(0).clone())
                    .next()
                    .unwrap();
                    let expected_some =
                        arrow::compute::take(expected.as_ref(), &some, None).unwrap();
                    let asks = [
                        (whole, WindowRows::All(rows), expected.clone()),
                        (part, WindowRows::Some(some.clone()), expected_some),
                        (
                            none,
                            WindowRows::Some(UInt32Array::from(Vec::<u32>::new())),
                            expected.slice(0, 0),
                        ),
                    ];

                    for _ in 0..2 {
                        for (window, rows, expected) in &asks {
                            let held = cache.column(&table, group, column, *window, rows).unwrap();
                            match held {
                                Some(held) => assert_eq!(held.as_ref(), expected.as_ref()),
                                None => assert!(budget < u64::MAX),
                            }
                            assert!(cache.used() <= budget);
                        }
                    }
                }
            }
            assert_eq!(cache.used() > 0, budget > 0);
            cache.clear();
            assert_eq!(cache.used(), 0);
        }
    }
}
