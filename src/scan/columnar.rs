//! A [`ChunkReader`] over column storage that is not parquet: a store that can
//! read any column for any set of row ranges, and may keep min/max statistics
//! over row windows. The scan reads the candidate rows of every column it needs
//! in one pass and filters them in memory, with the masks the parquet scanner
//! applies as row-filter stages, so the two readers answer alike (INV-D8).

use super::predicate::{or_masks, StatRange};
use super::rows::{Scanned, ScannedBatch};
use super::scanner::{
    block_range_mask, build_output_schema, ensure_columns_present, ensure_predicates_comparable,
    nullable_block_numbers, project_batch, Relation, RowFilters,
};
use super::{ChunkReader, ScanRequest};
use crate::error::ErrorKind;
use crate::integers::IntColumn;
use anyhow::Result;
use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt64Array};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::SchemaRef;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, Mutex, RwLock};

/// One column's min and max over consecutive row windows: window `i` holds
/// rows `offsets[i]..offsets[i + 1]`, and `min` and `max` hold one entry per
/// window, null where the window states nothing. The offsets run from 0 to the
/// table's row count and never step back; statistics that break this prune
/// nothing.
pub struct WindowStats {
    pub offsets: Vec<usize>,
    pub min: ArrayRef,
    pub max: ArrayRef,
}

/// One table of a chunk, read by column and row range.
pub trait ColumnarTable: Send + Sync {
    fn schema(&self) -> SchemaRef;

    fn num_rows(&self) -> usize;

    /// The columns at `columns`, ascending indices into the schema, at the
    /// schema's types, for the rows `ranges` names: sorted, disjoint and
    /// non-empty, every row when `None`. With no columns the batch carries only
    /// its row count. A batch with other rows or columns fails the scan.
    fn read(&self, columns: &[usize], ranges: Option<&[Range<usize>]>) -> Result<RecordBatch>;

    /// The column's window statistics, `None` when it has none.
    fn stats(&self, column: usize) -> Result<Option<WindowStats>>;
}

/// The tables of one chunk.
pub trait ColumnarChunk: Sync {
    type Table: ColumnarTable;

    fn has_table(&self, name: &str) -> bool;

    /// `None` when the chunk has no table by that name.
    fn open_table(&self, name: &str) -> Result<Option<Self::Table>>;
}

/// A [`ChunkReader`] over a [`ColumnarChunk`]. Tables open on first use and
/// stay open for the reader's lifetime.
pub struct ColumnarChunkReader<C: ColumnarChunk> {
    chunk: C,
    tables: RwLock<HashMap<String, Arc<OpenTable<C::Table>>>>,
}

struct OpenTable<T> {
    table: T,
    /// Block-number columns already read whole and found to hold no null.
    complete_blocks: Mutex<HashSet<String>>,
}

impl<C: ColumnarChunk> ColumnarChunkReader<C> {
    pub fn new(chunk: C) -> Self {
        Self {
            chunk,
            tables: RwLock::new(HashMap::new()),
        }
    }

    pub fn chunk(&self) -> &C {
        &self.chunk
    }

    fn table(&self, name: &str) -> Result<Option<Arc<OpenTable<C::Table>>>> {
        if let Some(open) = self.tables.read().unwrap().get(name) {
            return Ok(Some(open.clone()));
        }

        let Some(table) = self.chunk.open_table(name)? else {
            return Ok(None);
        };
        let open = Arc::new(OpenTable {
            table,
            complete_blocks: Mutex::new(HashSet::new()),
        });

        // Two scans racing on the same table both open it; either copy serves.
        let mut tables = self.tables.write().unwrap();
        let entry = tables.entry(name.to_string()).or_insert(open);
        Ok(Some(entry.clone()))
    }
}

impl<C: ColumnarChunk> ChunkReader for ColumnarChunkReader<C> {
    fn supports_row_positions(&self) -> bool {
        true
    }

    fn scan_rows(&self, table: &str, request: &ScanRequest) -> Result<Scanned> {
        let Some(open) = self.table(table)? else {
            crate::engine_bail!(
                ErrorKind::TableNotFound,
                "table '{}' is not found in the chunk",
                table
            );
        };
        let batches = scan(table, &open, request)?;
        Scanned::collect(request, batches)
    }

    fn has_table(&self, table: &str) -> bool {
        self.chunk.has_table(table)
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        Some(self.table(table).ok()??.table.schema())
    }
}

fn scan<T: ColumnarTable>(
    name: &str,
    open: &OpenTable<T>,
    request: &ScanRequest,
) -> Result<Vec<ScannedBatch>> {
    let table = &open.table;
    let schema = table.schema();
    ensure_columns_present(name, &schema, request)?;
    ensure_predicates_comparable(name, &schema, request)?;
    ensure_block_numbers_readable(name, open, request)?;

    let output_schema = build_output_schema(&schema, &request.output_columns);
    if let Some(rows) = request.row_indices {
        return read_positions(name, table, request, rows, &output_schema);
    }

    let ranges = candidate_rows(table, request)?;
    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    let columns = read_columns(&schema, request);
    let batch = checked_read(name, table, &columns, Some(&ranges))?;
    let (keep, tags) = row_masks(&batch, request)?;
    if keep.true_count() == 0 {
        return Ok(Vec::new());
    }

    let scanned = ScannedBatch {
        batch: project_batch(&batch, &output_schema)?,
        positions: request.positions.then(|| positions_of(&ranges)),
        tags,
    };
    Ok(scanned.filter(&keep)?.split(request.batch_size))
}

/// Refuse a block-number column that cannot place every row of the table,
/// whatever rows this scan selects (see the parquet reader's check). Reading it
/// whole is paid once per column.
fn ensure_block_numbers_readable<T: ColumnarTable>(
    name: &str,
    open: &OpenTable<T>,
    request: &ScanRequest,
) -> Result<()> {
    let schema = open.table.schema();
    let Some(bn_column) = nullable_block_numbers(name, &schema, request)? else {
        return Ok(());
    };
    if open.complete_blocks.lock().unwrap().contains(bn_column) {
        return Ok(());
    }

    let index = schema.index_of(bn_column)?;
    let batch = checked_read(name, &open.table, &[index], None)?;
    let column = batch.column(0);
    crate::engine_ensure!(
        column.null_count() == 0,
        ErrorKind::MalformedChunkData,
        "block-number column '{}' of '{}' leaves {} of {} rows without a block",
        bn_column,
        name,
        column.null_count(),
        column.len()
    );

    open.complete_blocks
        .lock()
        .unwrap()
        .insert(bn_column.to_string());
    Ok(())
}

/// [`ColumnarTable::read`], refused unless it returned the rows and columns it
/// was asked for, at the schema's types, and cut down to those columns in that
/// order. Rows pair with positions by order, so a short answer loses rows
/// silently; a filtered column left out filters nothing (INV-X3).
fn checked_read<T: ColumnarTable>(
    name: &str,
    table: &T,
    columns: &[usize],
    ranges: Option<&[Range<usize>]>,
) -> Result<RecordBatch> {
    let batch = table.read(columns, ranges)?;

    let asked = match ranges {
        Some(ranges) => ranges.iter().map(Range::len).sum(),
        None => table.num_rows(),
    };
    crate::engine_ensure!(
        batch.num_rows() == asked,
        ErrorKind::MalformedChunkData,
        "'{}' returned {} rows where {} were asked for",
        name,
        batch.num_rows(),
        asked
    );

    let schema = table.schema();
    let mut returned = Vec::with_capacity(columns.len());
    for &index in columns {
        let field = schema.field(index);
        let found = batch.schema().index_of(field.name()).ok();
        let stated = found.filter(|&at| batch.column(at).data_type() == field.data_type());
        let Some(at) = stated else {
            crate::engine_bail!(
                ErrorKind::MalformedChunkData,
                "'{}' did not return column '{}' as the {} its schema states",
                name,
                field.name(),
                field.data_type()
            );
        };
        returned.push(at);
    }

    Ok(batch.project(&returned)?)
}

/// Every column the scan reads: its output and whatever its filters look at.
fn read_columns(schema: &SchemaRef, request: &ScanRequest) -> Vec<usize> {
    let mut names: Vec<&str> = request.output_columns.clone();
    for predicate in &request.predicates {
        names.extend(predicate.required_columns());
    }
    names.extend(request.block_number_column);
    if let Some(kf) = request.key_filter {
        names.push(kf.block_column());
        names.extend(kf.columns.iter().map(String::as_str));
    }
    if let Some(hf) = request.hierarchical_filter {
        names.extend(hf.group_key_columns.iter().map(String::as_str));
        names.push(&hf.address_column);
    }

    let mut columns: Vec<usize> = names
        .into_iter()
        .filter_map(|name| schema.index_of(name).ok())
        .collect();
    columns.sort_unstable();
    columns.dedup();
    columns
}

/// The keep mask of a decoded batch, and each item tag's mask over it. The
/// filters are the parquet scanner's, decided by the same [`RowFilters`]: block
/// range, key or hierarchical filter, then the items, ORed.
fn row_masks(
    batch: &RecordBatch,
    request: &ScanRequest,
) -> Result<(BooleanArray, Vec<BooleanArray>)> {
    let rows = batch.num_rows();
    let filters = RowFilters::of(request);
    let mut keep = BooleanBuffer::new_set(rows);

    let block_column = request
        .block_number_column
        .and_then(|name| batch.column_by_name(name));
    if let Some(((from, to), column)) = filters.blocks.zip(block_column) {
        keep = &keep & &selected(&block_range_mask(column, from, to)?);
    }

    match filters.relation {
        Relation::Addresses(hf) => {
            assert!(
                request.predicates.is_empty(),
                "hierarchical_filter and predicates must not be set simultaneously"
            );
            keep = &keep & &selected(&hf.mask(batch, Some(&keep)));
        }
        Relation::Keys(kf) => {
            keep = &keep & &selected(&kf.mask(batch, Some(&keep))?);
        }
        Relation::None => {}
    }

    if request.predicates.is_empty() {
        let none = BooleanArray::from(vec![false; rows]);
        let tags = request.item_tags.iter().map(|_| none.clone()).collect();
        return Ok((BooleanArray::new(keep, None), tags));
    }

    let masks = request
        .predicates
        .iter()
        .map(|predicate| predicate.evaluate(batch))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let every_item: Vec<usize> = (0..masks.len()).collect();
    keep = &keep & &selected(&or_masks(&masks, &every_item, rows));

    let tags = request
        .item_tags
        .iter()
        .map(|items| BooleanArray::new(selected(&or_masks(&masks, items, rows)), None))
        .collect();
    Ok((BooleanArray::new(keep, None), tags))
}

/// The rows a mask selects. A row it says neither yes nor no about is not one
/// it selects.
fn selected(mask: &BooleanArray) -> BooleanBuffer {
    match mask.nulls() {
        Some(nulls) => mask.values() & nulls.inner(),
        None => mask.values().clone(),
    }
}

/// The row windows the scan cannot rule out by statistics, merged into ranges.
/// Statistics that are absent, that do not cover the table, or that cut it into
/// windows other than the first column's, prune nothing.
fn candidate_rows<T: ColumnarTable>(table: &T, request: &ScanRequest) -> Result<Vec<Range<usize>>> {
    let rows = table.num_rows();
    if rows == 0 {
        return Ok(Vec::new());
    }
    let schema = table.schema();

    let mut wanted: Vec<&str> = request.block_number_column.into_iter().collect();
    wanted.extend(request.key_filter.map(|kf| kf.block_column()));
    for predicate in &request.predicates {
        wanted.extend(predicate.required_columns());
    }
    wanted.sort_unstable();
    wanted.dedup();

    let mut stats: HashMap<&str, WindowStats> = HashMap::new();
    let mut offsets: Option<Vec<usize>> = None;
    for name in wanted {
        let Ok(index) = schema.index_of(name) else {
            continue;
        };
        let Some(column_stats) = table.stats(index)? else {
            continue;
        };
        if !covers(&column_stats, rows) {
            continue;
        }
        match &offsets {
            None => offsets = Some(column_stats.offsets.clone()),
            Some(first) if *first != column_stats.offsets => continue,
            Some(_) => {}
        }
        stats.insert(name, column_stats);
    }
    let Some(offsets) = offsets else {
        return Ok(vec![Range {
            start: 0,
            end: rows,
        }]);
    };

    let range_of = |name: &str, window: usize| -> Option<StatRange> {
        let column_stats = stats.get(name)?;
        let (min, max) = (&column_stats.min, &column_stats.max);
        if min.is_null(window) || max.is_null(window) {
            return None;
        }
        let stored = schema.field_with_name(name).ok()?.data_type();
        StatRange::new(
            stored,
            min.slice(window, 1).as_ref(),
            max.slice(window, 1).as_ref(),
        )
    };
    // Read by the rule every reader of the column applies; bounds that come
    // out inverted prune nothing.
    let block_bounds = |name: &str, window: usize| -> Option<(u64, u64)> {
        let column_stats = stats.get(name)?;
        let stored = schema.field_with_name(name).ok()?.data_type();
        let bound = |values: &ArrayRef| {
            let values = IntColumn::resolve(values.as_ref())?;
            if values.is_null(window) {
                return None;
            }
            crate::integers::block_number_at(stored, values.value(window) as i64)
        };
        let (min, max) = (bound(&column_stats.min)?, bound(&column_stats.max)?);
        (min <= max).then_some((min, max))
    };

    let mut ranges: Vec<Range<usize>> = Vec::new();
    for window in 0..offsets.len() - 1 {
        if let Some((min, max)) = request
            .block_number_column
            .and_then(|bn| block_bounds(bn, window))
        {
            let before = request.from_block.is_some_and(|from| max < from);
            let after = request.to_block.is_some_and(|to| min > to);
            if before || after {
                continue;
            }
        }

        // A window goes only when every item is ruled out of it.
        let stats_fn = |column: &str| range_of(column, window);
        let no_item_matches = !request.predicates.is_empty()
            && request
                .predicates
                .iter()
                .all(|item| item.can_skip_row_group(&stats_fn));
        if no_item_matches {
            continue;
        }

        if let Some(kf) = request.key_filter {
            if let Some((min, max)) = block_bounds(kf.block_column(), window) {
                if !kf.has_block_within(min, max) {
                    continue;
                }
            }
        }

        let rows = offsets[window]..offsets[window + 1];
        match ranges.last_mut() {
            Some(last) if last.end == rows.start => last.end = rows.end,
            _ if rows.is_empty() => {}
            _ => ranges.push(rows),
        }
    }

    Ok(ranges)
}

/// Whether `stats` covers a table of `rows` rows once and in order, with one
/// min and one max per window. Offsets that start late leave the rows before
/// them unread; offsets that step back or overshoot ask `read` for ranges its
/// contract rules out.
fn covers(stats: &WindowStats, rows: usize) -> bool {
    let offsets = &stats.offsets;
    let windows = offsets.len().saturating_sub(1);

    let from_first_row = offsets.first() == Some(&0);
    let to_last_row = offsets.last() == Some(&rows);
    let in_order = offsets.windows(2).all(|pair| pair[0] <= pair[1]);
    let one_bound_each = stats.min.len() == windows && stats.max.len() == windows;

    from_first_row && to_last_row && in_order && one_bound_each
}

fn positions_of(ranges: &[Range<usize>]) -> UInt64Array {
    ranges
        .iter()
        .flat_map(|range| range.start as u64..range.end as u64)
        .collect()
}

/// Read the physical rows `rows` names, without filtering them again.
fn read_positions<T: ColumnarTable>(
    name: &str,
    table: &T,
    request: &ScanRequest,
    rows: &[u64],
    output_schema: &SchemaRef,
) -> Result<Vec<ScannedBatch>> {
    crate::engine_ensure!(
        rows.windows(2).all(|pair| pair[0] < pair[1])
            && rows.last().is_none_or(|&row| row < table.num_rows() as u64),
        ErrorKind::MalformedChunkData,
        "physical row selection is not sorted, unique and in bounds"
    );
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut ranges: Vec<Range<usize>> = Vec::new();
    for &row in rows {
        let row = row as usize;
        match ranges.last_mut() {
            Some(last) if last.end == row => last.end += 1,
            _ => ranges.push(row..row + 1),
        }
    }

    let schema = table.schema();
    let mut columns: Vec<usize> = request
        .output_columns
        .iter()
        .filter_map(|column| schema.index_of(column).ok())
        .collect();
    columns.sort_unstable();
    columns.dedup();

    let batch = checked_read(name, table, &columns, Some(&ranges))?;
    let batch = project_batch(&batch, output_schema)?;
    let positions = request.positions.then(|| UInt64Array::from(rows.to_vec()));

    Ok(ScannedBatch::untagged(request, batch, positions).split(request.batch_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::error_kind;
    use crate::scan::predicate::{col_eq, RowPredicate, ScalarValue};
    use arrow::array::{AsArray, StringArray, UInt32Array};
    use arrow::compute::kernels::aggregate::{max, max_string, min, min_string};
    use arrow::compute::{cast, concat_batches};
    use arrow::datatypes::{DataType, Field, Schema, UInt32Type};

    /// Blocks 1 to 3; the middle block's program differs from the other two.
    fn rows() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("block_number", DataType::UInt32, true),
            Field::new("program", DataType::Utf8, false),
        ]);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "a"])),
        ];
        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }

    /// How a store's read breaks its word.
    #[derive(Clone, Copy, Default)]
    enum Read {
        #[default]
        Honest,
        /// One row fewer than a ranged read asked for.
        ShortRanged,
        /// One row fewer than a whole-column read asked for.
        ShortWhole,
        /// Without this column.
        Without(&'static str),
        /// This column as large strings.
        Retyped(&'static str),
    }

    /// One table, `t`, holding [`rows`]. A read refuses ranges the contract
    /// rules out, so a reader that sends them fails rather than passes.
    #[derive(Clone, Copy, Default)]
    struct Store {
        /// The window offsets its statistics state; none when `None`. A window
        /// states the rows between its two offsets, in either order.
        offsets: Option<&'static [usize]>,
        /// The statistics state one window fewer than the offsets make.
        short_stats: bool,
        read: Read,
    }

    impl ColumnarChunk for Store {
        type Table = Store;

        fn has_table(&self, name: &str) -> bool {
            name == "t"
        }

        fn open_table(&self, name: &str) -> Result<Option<Store>> {
            Ok(self.has_table(name).then_some(*self))
        }
    }

    impl ColumnarTable for Store {
        fn schema(&self) -> SchemaRef {
            rows().schema()
        }

        fn num_rows(&self) -> usize {
            rows().num_rows()
        }

        fn read(&self, columns: &[usize], ranges: Option<&[Range<usize>]>) -> Result<RecordBatch> {
            let all = rows().project(columns)?;
            let whole = 0..all.num_rows();
            let asked = ranges.unwrap_or(std::slice::from_ref(&whole));

            let in_order = asked.windows(2).all(|pair| pair[0].end <= pair[1].start);
            let in_table = asked
                .iter()
                .all(|range| range.start < range.end && range.end <= all.num_rows());
            anyhow::ensure!(in_order && in_table, "{asked:?} break the read contract");

            let slices: Vec<RecordBatch> = asked
                .iter()
                .map(|range| all.slice(range.start, range.len()))
                .collect();
            let batch = concat_batches(&all.schema(), &slices)?;
            let short = batch.slice(0, batch.num_rows() - 1);

            match self.read {
                Read::Honest => Ok(batch),
                Read::ShortRanged if ranges.is_some() => Ok(short),
                Read::ShortWhole if ranges.is_none() => Ok(short),
                Read::ShortRanged | Read::ShortWhole => Ok(batch),
                Read::Without(name) => {
                    let mut batch = batch;
                    if let Ok(index) = batch.schema().index_of(name) {
                        batch.remove_column(index);
                    }
                    Ok(batch)
                }
                Read::Retyped(name) => {
                    let Ok(index) = batch.schema().index_of(name) else {
                        return Ok(batch);
                    };
                    let mut fields: Vec<Field> = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.as_ref().clone())
                        .collect();
                    let mut columns = batch.columns().to_vec();
                    fields[index] = Field::new(name, DataType::LargeUtf8, false);
                    columns[index] = cast(&columns[index], &DataType::LargeUtf8)?;
                    Ok(RecordBatch::try_new(
                        Arc::new(Schema::new(fields)),
                        columns,
                    )?)
                }
            }
        }

        fn stats(&self, column: usize) -> Result<Option<WindowStats>> {
            let Some(offsets) = self.offsets else {
                return Ok(None);
            };
            let values = rows().column(column).clone();
            let rows = values.len();
            let mut windows: Vec<ArrayRef> = offsets
                .windows(2)
                .map(|w| {
                    let (start, end) = (w[0].min(w[1]).min(rows), w[0].max(w[1]).min(rows));
                    values.slice(start, end - start)
                })
                .collect();
            if self.short_stats {
                windows.pop();
            }

            let (min, max): (ArrayRef, ArrayRef) = match values.data_type() {
                DataType::UInt32 => {
                    let ints = |w: &ArrayRef| w.as_primitive::<UInt32Type>().clone();
                    let min: UInt32Array = windows.iter().map(|w| min(&ints(w))).collect();
                    let max: UInt32Array = windows.iter().map(|w| max(&ints(w))).collect();
                    (Arc::new(min), Arc::new(max))
                }
                _ => {
                    let min: StringArray = windows
                        .iter()
                        .map(|w| min_string(w.as_string::<i32>()))
                        .collect();
                    let max: StringArray = windows
                        .iter()
                        .map(|w| max_string(w.as_string::<i32>()))
                        .collect();
                    (Arc::new(min), Arc::new(max))
                }
            };
            Ok(Some(WindowStats {
                offsets: offsets.to_vec(),
                min,
                max,
            }))
        }
    }

    /// The blocks of the rows a scan of `t` returns, keeping those of `program`
    /// when it names one.
    fn blocks(store: Store, program: Option<&str>) -> Result<Vec<u32>> {
        blocks_of(store, program, None)
    }

    /// [`blocks`], reading the physical `rows` when it names them.
    fn blocks_of(store: Store, program: Option<&str>, rows: Option<&[u64]>) -> Result<Vec<u32>> {
        let item = program
            .map(|p| RowPredicate::new(vec![col_eq("program", ScalarValue::Utf8(p.into()))]));
        let mut request = ScanRequest::new(vec!["block_number"]);
        request.block_number_column = Some("block_number");
        request.predicates.extend(item.as_ref());
        request.row_indices = rows;

        let reader = ColumnarChunkReader::new(store);
        let batches = reader.scan("t", &request)?;
        let blocks = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<UInt32Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        Ok(blocks)
    }

    #[test]
    fn window_offsets_that_do_not_cover_the_table_prune_nothing() {
        const EVERY_ROW: &[u32] = &[1, 2, 3];
        const A_ROWS: &[u32] = &[1, 3];
        // What, the offsets the store states, the program kept, the blocks
        // returned.
        type Case = (
            &'static str,
            &'static [usize],
            Option<&'static str>,
            &'static [u32],
        );
        let cases: [Case; 9] = [
            ("windows from row 1", &[1, 3], None, EVERY_ROW),
            ("an end and no window", &[3], None, EVERY_ROW),
            ("a window that steps back", &[0, 2, 1, 3], Some("a"), A_ROWS),
            (
                "a window past the table and back",
                &[0, 5, 2, 3],
                Some("b"),
                &[2],
            ),
            ("a window past the table", &[0, 5, 3], None, EVERY_ROW),
            ("windows that end short", &[0, 2], None, EVERY_ROW),
            ("windows that end past the table", &[0, 4], None, EVERY_ROW),
            ("empty windows", &[0, 0, 1, 1, 3], Some("a"), A_ROWS),
            ("a window per row", &[0, 1, 2, 3], Some("b"), &[2]),
        ];

        let mut wrong = Vec::new();
        for (what, offsets, program, expected) in cases {
            let store = Store {
                offsets: Some(offsets),
                ..Store::default()
            };
            let actual = blocks(store, program).map_err(|e| format!("{e:#}"));
            if actual.as_deref() != Ok(expected) {
                wrong.push(format!("{what}: {actual:?}"));
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[test]
    fn statistics_of_fewer_windows_than_the_offsets_make_prune_nothing() {
        let store = Store {
            offsets: Some(&[0, 1, 3]),
            short_stats: true,
            ..Store::default()
        };

        assert_eq!(blocks(store, Some("b")).unwrap(), vec![2]);
    }

    #[test]
    fn a_read_that_does_not_return_what_was_asked_is_refused() {
        let by_position: Option<&[u64]> = Some(&[0, 1, 2]);
        let cases = [
            ("a ranged read one row short", Read::ShortRanged, None, None),
            (
                "a whole-column read one row short",
                Read::ShortWhole,
                None,
                None,
            ),
            (
                "a read by position one row short",
                Read::ShortRanged,
                None,
                by_position,
            ),
            (
                "a read without the filtered column",
                Read::Without("program"),
                Some("a"),
                None,
            ),
            (
                "a read by position without its column",
                Read::Without("block_number"),
                None,
                by_position,
            ),
            (
                "a read of the filtered column at another type",
                Read::Retyped("program"),
                Some("a"),
                None,
            ),
        ];

        let mut wrong = Vec::new();
        for (what, read, program, rows) in cases {
            let store = Store {
                read,
                ..Store::default()
            };
            match blocks_of(store, program, rows) {
                Ok(blocks) => wrong.push(format!("{what} answered {blocks:?}")),
                Err(e) if error_kind(&e) != Some(ErrorKind::MalformedChunkData) => {
                    wrong.push(format!("{what} failed without its kind: {e:#}"))
                }
                Err(_) => {}
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }
}
