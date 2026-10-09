//! A chunk held in memory and read through [`ColumnarChunkReader`], the reader
//! for storage that is not parquet. Window statistics are computed here, so a
//! test chooses whether the reader may prune and how finely.

use anyhow::Result;
use arrow::array::{ArrayRef, AsArray, PrimitiveArray, RecordBatch, StringArray};
use arrow::compute::concat_batches;
use arrow::compute::kernels::aggregate::{max, max_string, min, min_string};
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::output::{execute_chunk_with, ExecOptions};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::{ColumnarChunk, ColumnarChunkReader, ColumnarTable, WindowStats};
use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;

pub struct MemoryChunk {
    tables: HashMap<String, RecordBatch>,
    windows: Windows,
}

/// How a table's statistics cut it into windows.
#[derive(Clone, Copy)]
enum Windows {
    /// No statistics.
    None,
    /// Windows of this many rows.
    Every(usize),
    /// The offsets a table of this many rows reports, whether or not they
    /// describe it. A window states the rows between its two offsets, in
    /// either order, that the table holds.
    Offsets(fn(usize) -> Vec<usize>),
}

impl MemoryChunk {
    /// Every table of a parquet chunk directory, decoded whole, with windows
    /// of `window` rows, or no statistics for `None`.
    pub fn load(chunk: &Path, window: Option<usize>) -> Self {
        let windows = window.map_or(Windows::None, Windows::Every);
        Self::load_with(chunk, windows)
    }

    /// The same, with window statistics at the offsets `offsets` gives for a
    /// table's row count.
    pub fn load_with_offsets(chunk: &Path, offsets: fn(usize) -> Vec<usize>) -> Self {
        Self::load_with(chunk, Windows::Offsets(offsets))
    }

    fn load_with(chunk: &Path, windows: Windows) -> Self {
        let mut tables = HashMap::new();
        for entry in std::fs::read_dir(chunk).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|ext| ext != "parquet") {
                continue;
            }
            let name = path.file_stem().unwrap().to_str().unwrap().to_string();
            let file = std::fs::File::open(&path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let schema = builder.schema().clone();
            let reader = builder.build().unwrap();
            let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
            tables.insert(name, concat_batches(&schema, &batches).unwrap());
        }
        Self { tables, windows }
    }
}

impl ColumnarChunk for MemoryChunk {
    type Table = MemoryTable;

    fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    fn open_table(&self, name: &str) -> Result<Option<MemoryTable>> {
        Ok(self.tables.get(name).map(|batch| MemoryTable {
            batch: batch.clone(),
            windows: self.windows,
        }))
    }
}

pub struct MemoryTable {
    batch: RecordBatch,
    windows: Windows,
}

impl ColumnarTable for MemoryTable {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    fn read(&self, columns: &[usize], ranges: Option<&[Range<usize>]>) -> Result<RecordBatch> {
        let projected = self.batch.project(columns)?;
        let Some(ranges) = ranges else {
            return Ok(projected);
        };
        let in_order = ranges.windows(2).all(|pair| pair[0].end <= pair[1].start);
        let in_table = ranges
            .iter()
            .all(|range| range.start < range.end && range.end <= self.batch.num_rows());
        anyhow::ensure!(
            in_order && in_table,
            "{ranges:?} are not sorted, disjoint, non-empty ranges of {} rows",
            self.batch.num_rows()
        );
        let slices: Vec<RecordBatch> = ranges
            .iter()
            .map(|range| projected.slice(range.start, range.len()))
            .collect();
        Ok(concat_batches(&projected.schema(), &slices)?)
    }

    fn stats(&self, column: usize) -> Result<Option<WindowStats>> {
        let rows = self.batch.num_rows();
        let offsets: Vec<usize> = match self.windows {
            Windows::None => return Ok(None),
            Windows::Every(window) => (0..rows).step_by(window).chain([rows]).collect(),
            Windows::Offsets(offsets) => offsets(rows),
        };
        let values = self.batch.column(column);
        let windows: Vec<ArrayRef> = offsets
            .windows(2)
            .map(|w| {
                let (start, end) = (w[0].min(w[1]).min(rows), w[0].max(w[1]).min(rows));
                values.slice(start, end - start)
            })
            .collect();

        Ok(min_max(&windows).map(|(min, max)| WindowStats { offsets, min, max }))
    }
}

/// Each window's min and max, null where it holds no value; `None` for a type
/// that keeps no statistics.
fn min_max(windows: &[ArrayRef]) -> Option<(ArrayRef, ArrayRef)> {
    macro_rules! ints {
        ($($t:ty),+) => {
            $(if windows.iter().all(|w| w.as_primitive_opt::<$t>().is_some()) {
                let min: PrimitiveArray<$t> = windows.iter().map(|w| min(w.as_primitive::<$t>())).collect();
                let max: PrimitiveArray<$t> = windows.iter().map(|w| max(w.as_primitive::<$t>())).collect();
                return Some((std::sync::Arc::new(min), std::sync::Arc::new(max)));
            })+
        };
    }
    ints!(Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type);

    if windows.iter().all(|w| w.as_string_opt::<i32>().is_some()) {
        let min: StringArray = windows
            .iter()
            .map(|w| min_string(w.as_string::<i32>()))
            .collect();
        let max: StringArray = windows
            .iter()
            .map(|w| max_string(w.as_string::<i32>()))
            .collect();
        return Some((std::sync::Arc::new(min), std::sync::Arc::new(max)));
    }

    None
}

/// Run a query to completion through the columnar reader.
pub fn run_columnar(
    catalog: &DatasetDescription,
    chunk: &MemoryChunk,
    query: &[u8],
) -> Result<Vec<u8>> {
    let parsed = parse_query(query, catalog)?;
    let plan = compile(&parsed, catalog)?;
    let reader = ColumnarChunkReader::new(MemoryChunk {
        tables: chunk.tables.clone(),
        windows: chunk.windows,
    });

    Ok(
        execute_chunk_with(&plan, catalog, &reader, ExecOptions::default())?
            .map(|out| out.into_json_lines())
            .unwrap_or_default(),
    )
}
