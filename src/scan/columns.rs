//! Whole columns of row groups, decoded once for every scan of a query pass
//! that filters the same table.
//!
//! A query that reads most of a table reaches it through many scans: its own
//! items and every relation that targets it. Each scan of the table in one pass
//! reads the same columns, so [`ColumnCache`] decodes a row group's rows inside
//! the pass's block range once, and each scan filters them in memory.
//!
//! The budget bounds what the cache holds, and it is checked before anything is
//! decoded: a column is decoded at the window's rows alone, after the most it
//! can hold is counted against the budget. A row group the budget cannot take
//! is read by each scan as any other scan reads it. While a column decodes, the
//! reader's buffers grow by doubling, so for a moment it takes up to twice what
//! it then holds.

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
        let most = || Some(window_reservation(table, group));
        let size = |held: &Held<WindowRows>| match &held.value {
            WindowRows::All(_) => 0,
            WindowRows::Some(offsets) => offsets.get_array_memory_size() as u64,
        };

        hold(&self.inner.windows, &self.inner, key, most, size, || {
            Ok(Held {
                _file: file.clone(),
                value: rows()?,
            })
        })
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
        let most = || column_reservation(table, group, index);
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

/// The most the rows of row group `group` inside a window hold: an offset for
/// each, and the array around them.
pub(super) fn window_reservation(table: &ParquetTable, group: usize) -> u64 {
    let offsets = table.row_group(group).num_rows().max(0) as u64 * 4;
    offsets.saturating_add(empty_size(&arrow::datatypes::DataType::UInt32))
}

/// The most column `index` of row group `group` holds once decoded, at any of
/// its rows: its values, and the arrays around them. `None` where the footer
/// does not bound it.
fn column_reservation(table: &ParquetTable, group: usize, index: usize) -> Option<u64> {
    let values = super::scanner::column_bytes_bound(table, group, index)?;
    let data_type = table.schema().field(index).data_type();

    Some(values.saturating_add(empty_size(data_type)))
}

/// What an empty array of `data_type` holds: the arrays a column of it is made
/// of, with an offset of each list in it.
fn empty_size(data_type: &arrow::datatypes::DataType) -> u64 {
    arrow::array::new_empty_array(data_type).get_array_memory_size() as u64
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

    // Each batch is dropped as its column is taken, so the column is the only
    // owner of its buffers and can trim them.
    let mut parts = builder
        .build()?
        .map(|batch| Ok(batch?.column(0).clone()))
        .collect::<anyhow::Result<Vec<ArrayRef>>>()?;
    let mut column = match parts.len() {
        0 => arrow::array::new_empty_array(table.schema().field(index).data_type()),
        1 => parts.pop().expect("one part"),
        _ => {
            let parts: Vec<&dyn Array> = parts.iter().map(|part| part.as_ref()).collect();
            arrow::compute::concat(&parts)?
        }
    };

    // The reader leaves spare capacity, up to as much again as the values.
    column.shrink_to_fit();
    Ok(column)
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
/// others wait. The `most` it may take, asked only when it is to be loaded, is
/// counted against the budget before it is loaded, and what it takes once it
/// is; `None` when that does not fit. A
/// value that takes more than `most` is kept only if the budget takes the
/// rest; this caller gets it either way.
fn hold<K: std::hash::Hash + Eq, T>(
    map: &Mutex<FxHashMap<K, Arc<Slot<T>>>>,
    inner: &Inner,
    key: K,
    most: impl FnOnce() -> Option<u64>,
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

    let Some(most) = most().filter(|&most| inner.reserve(most)) else {
        return Ok(None);
    };
    let value = match load() {
        Ok(value) => Arc::new(value),
        Err(error) => {
            inner.settle(most, 0);
            return Err(error);
        }
    };

    if inner.settle(most, size(&value)) {
        *held = Some(value.clone());
    }
    Ok(Some(value))
}

impl Inner {
    /// Count `bytes` against the budget, if they fit in what is left of it.
    fn reserve(&self, bytes: u64) -> bool {
        self.used
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes)
                    .filter(|&total| total <= self.budget)
            })
            .is_ok()
    }

    /// Count `actual` bytes in place of the `reserved` ones, if the budget
    /// takes them; else release the reservation and return `false`.
    fn settle(&self, reserved: u64, actual: u64) -> bool {
        let mut taken = false;
        self.used
            .update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let rest = used.saturating_sub(reserved);
                let total = rest.saturating_add(actual);
                taken = total <= self.budget;

                if taken {
                    total
                } else {
                    rest
                }
            });
        taken
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, FixedSizeBinaryArray, Int32Array, Int64Array, LargeStringArray,
        ListArray, RecordBatch, StringArray, StructArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field, Fields, Schema, UInt16Type};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::sync::atomic::AtomicUsize;

    /// Nullable columns of every layout the footer bound models, in row
    /// groups of 64 rows and pages of 3.
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
        let flag = BooleanArray::from_iter((0..rows).map(|i| (i % 6 != 5).then_some(i % 2 == 0)));
        let hash = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            (0..rows).map(|i| (i % 8 != 7).then_some([i as u8; 5])),
            5,
        )
        .unwrap();
        let long = LargeStringArray::from_iter(
            (0..rows).map(|i| (i % 4 != 3).then(|| "w".repeat(i as usize % 13))),
        );
        let pair_fields = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("data", DataType::Binary, true),
        ]);
        let pair = StructArray::new(
            pair_fields.clone(),
            vec![
                Arc::new(Int64Array::from_iter(
                    (0..rows).map(|i| (i % 3 != 2).then_some(i as i64 * 7)),
                )),
                Arc::new(BinaryArray::from_iter(
                    (0..rows).map(|i| (i % 5 != 1).then(|| vec![i as u8; i as usize % 9])),
                )),
            ],
            Some(NullBuffer::from_iter((0..rows).map(|i| i % 10 != 9))),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("number", DataType::Int32, true),
            Field::new("text", DataType::Utf8, true),
            Field::new("path", path.data_type().clone(), true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("hash", DataType::FixedSizeBinary(5), true),
            Field::new("long", DataType::LargeUtf8, true),
            Field::new("pair", DataType::Struct(pair_fields), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(number),
                Arc::new(text),
                Arc::new(path),
                Arc::new(flag),
                Arc::new(hash),
                Arc::new(long),
                Arc::new(pair),
            ],
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
                for column in 0..table.schema().fields().len() {
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

    /// Every column of the first `groups` row groups of `table`, at any of its
    /// rows, holds no more than the cache reserves for it before decoding it.
    /// Returns how many columns the footer bounds, which are the ones checked.
    fn columns_hold_what_was_reserved(table: &ParquetTable, groups: usize) -> usize {
        let mut checked = 0;
        for group in 0..groups.min(table.num_row_groups()) {
            let rows = table.row_group(group).num_rows() as usize;
            let windows = [
                WindowRows::All(rows),
                WindowRows::Some((0..rows as u32).step_by(2).collect()),
                WindowRows::Some(UInt32Array::from(vec![rows as u32 - 1])),
                WindowRows::Some(UInt32Array::from(Vec::<u32>::new())),
            ];
            for index in 0..table.schema().fields().len() {
                let Some(reserved) = column_reservation(table, group, index) else {
                    continue;
                };
                checked += 1;
                for rows in &windows {
                    let cache = ColumnCache::new(u64::MAX);
                    let column = cache.column(table, group, index, Window::default(), rows);

                    assert!(column.unwrap().is_some());
                    let name = table.schema().field(index).name();
                    assert!(
                        cache.used() <= reserved,
                        "{name} of group {group} holds {} rows in {} bytes, {reserved} reserved",
                        rows.len(),
                        cache.used()
                    );
                }
            }
        }
        checked
    }

    #[test]
    fn a_column_holds_no_more_than_the_cache_reserves() {
        let dir = tempfile::tempdir().unwrap();
        let table = write(dir.path());

        let checked = columns_hold_what_was_reserved(&table, usize::MAX);

        let columns = table.num_row_groups() * table.schema().fields().len();
        assert_eq!(checked, columns, "the footer left a column unbounded");
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn a_chunk_column_holds_no_more_than_the_cache_reserves() {
        if !crate::testing::chunks_present() {
            return;
        }

        let mut checked = 0;
        for dataset in ["evm", "solana"] {
            let dir = crate::testing::chunk_dir(dataset).unwrap();
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().is_some_and(|ext| ext == "parquet") {
                    let table = ParquetTable::open(&path).unwrap();
                    checked += columns_hold_what_was_reserved(&table, 2);
                }
            }
        }
        assert!(checked > 100, "{checked} columns checked");
    }

    /// A value that takes more than was reserved for it is kept only if the
    /// budget takes the rest. The asker gets it either way, and a value not
    /// kept is loaded again.
    #[test]
    fn a_value_past_its_reservation_is_kept_only_inside_the_budget() {
        for (budget, kept) in [(15, false), (20, true)] {
            let cache = ColumnCache::new(budget);
            let slots = Mutex::new(FxHashMap::default());
            let loads = AtomicUsize::new(0);
            let ask = || {
                hold(
                    &slots,
                    &cache.inner,
                    (),
                    || Some(10),
                    |value| *value,
                    || {
                        loads.fetch_add(1, Ordering::Relaxed);
                        Ok(20u64)
                    },
                )
            };

            for _ in 0..2 {
                assert_eq!(ask().unwrap().as_deref(), Some(&20));
                assert_eq!(cache.used(), if kept { 20 } else { 0 });
            }
            assert_eq!(loads.load(Ordering::Relaxed), if kept { 1 } else { 2 });
        }
    }
}
