//! Whole columns of row groups, decoded once for every scan of a query pass
//! that filters the same table.
//!
//! A query that reads most of a table reaches it through many scans: its own
//! items and every relation that targets it. Each scan of the table in one pass
//! reads the same columns, so [`ColumnCache`] decodes a row group's rows inside
//! the pass's block range once, and each scan filters them in memory.

use super::ParquetTable;
use arrow::array::{Array, ArrayRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Decoded columns of row groups that several scans filter, kept until the
/// cache is cleared, up to a byte budget. What does not fit is decoded again
/// by each scan that asks.
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
    Some(arrow::array::UInt32Array),
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
    /// first time.
    pub(super) fn window_rows(
        &self,
        table: &ParquetTable,
        group: usize,
        window: Window,
        rows: impl FnOnce() -> anyhow::Result<WindowRows>,
    ) -> anyhow::Result<Arc<Held<WindowRows>>> {
        let file = table.metadata_arc();
        let (from, to) = window.bounds();
        let key = (Arc::as_ptr(&file) as usize, group, from, to);
        let size = |held: &Held<WindowRows>| match &held.value {
            WindowRows::All(_) => 0,
            WindowRows::Some(offsets) => offsets.get_array_memory_size() as u64,
        };

        hold(&self.inner.windows, &self.inner, key, size, || {
            Ok::<_, anyhow::Error>(Held {
                _file: file.clone(),
                value: rows()?,
            })
        })
    }

    /// Column `index` of row group `group`, at the window's `rows`.
    pub(super) fn column(
        &self,
        table: &ParquetTable,
        group: usize,
        index: usize,
        window: Window,
        rows: &WindowRows,
    ) -> anyhow::Result<ArrayRef> {
        let file = table.metadata_arc();
        let (from, to) = window.bounds();
        let key = (Arc::as_ptr(&file) as usize, group, index, from, to);
        let size = |held: &Held<ArrayRef>| held.value.get_array_memory_size() as u64;
        let held = hold(&self.inner.columns, &self.inner, key, size, || {
            let whole = decode_column(table, group, index)?;
            let value = match rows {
                WindowRows::All(_) => whole,
                WindowRows::Some(offsets) => arrow::compute::take(whole.as_ref(), offsets, None)?,
            };
            Ok::<_, anyhow::Error>(Held {
                _file: file.clone(),
                value,
            })
        })?;

        Ok(held.value.clone())
    }
}

/// Column `index` of row group `group`, every row of it.
pub(super) fn decode_column(
    table: &ParquetTable,
    group: usize,
    index: usize,
) -> anyhow::Result<ArrayRef> {
    let projection =
        ProjectionMask::roots(table.metadata().file_metadata().schema_descr(), [index]);
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
        table.data(),
        table.arrow_metadata().clone(),
    )
    .with_projection(projection)
    .with_row_groups(vec![group])
    .build()?;
    let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(match batches.as_slice() {
        [batch] => batch.column(0).clone(),
        _ => {
            let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
            arrow::compute::concat(&parts)?
        }
    })
}

/// The value `map` holds under `key`, loaded by the first caller while the
/// others wait, and kept while the budget allows.
fn hold<K: std::hash::Hash + Eq, T, E>(
    map: &Mutex<FxHashMap<K, Arc<Slot<T>>>>,
    inner: &Inner,
    key: K,
    size: impl FnOnce(&T) -> u64,
    load: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<Arc<T>, E> {
    let slot = map
        .lock()
        .expect("column cache poisoned")
        .entry(key)
        .or_default()
        .clone();

    let mut held = slot.held.lock().expect("column cache slot poisoned");
    if let Some(value) = held.as_ref() {
        return Ok(value.clone());
    }

    let value = Arc::new(load()?);
    let bytes = size(&value);
    let used = inner.used.fetch_add(bytes, Ordering::Relaxed) + bytes;
    if used <= inner.budget {
        *held = Some(value.clone());
    } else {
        inner.used.fetch_sub(bytes, Ordering::Relaxed);
    }

    Ok(value)
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

    #[test]
    fn a_decoded_column_holds_the_window_rows_parquet_reads() {
        let dir = tempfile::tempdir().unwrap();
        let table = write(dir.path());
        let whole = Window::default();
        let part = Window {
            from: Some(10),
            to: Some(20),
        };

        for budget in [0, u64::MAX] {
            let cache = ColumnCache::new(budget);
            for group in 0..table.num_row_groups() {
                let rows = table.row_group(group).num_rows() as usize;
                let some: arrow::array::UInt32Array =
                    (0..rows as u32).filter(|row| row % 3 != 1).collect();
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

                    for _ in 0..2 {
                        let all = cache
                            .column(&table, group, column, whole, &WindowRows::All(rows))
                            .unwrap();
                        let some = cache
                            .column(&table, group, column, part, &WindowRows::Some(some.clone()))
                            .unwrap();
                        assert_eq!(all.as_ref(), expected.as_ref());
                        assert_eq!(some.as_ref(), expected_some.as_ref());
                    }
                }
            }
            assert_eq!(cache.used() > 0, budget > 0);
            cache.clear();
            assert_eq!(cache.used(), 0);
        }
    }
}
