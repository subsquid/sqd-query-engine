//! Synthetic chunk construction.
//!
//! A fixture chunk is one shape written by one archiver version. Several
//! invariants are about the shapes it is *not* — a narrower physical type, a
//! column dropped, a sort key the catalog does not expect — so those tests write
//! their own parquet.
//!
//! This is HC-3: it copies a chunk with a table or column dropped, a nullable
//! column added, a column overwritten, a column stored at another physical
//! width, or the whole thing written under a different storage layout — row
//! group size, compression, dictionary and statistics encoding, physical row
//! order.

use arrow::array::{
    new_null_array, Array, ArrayRef, ListArray, StringArray, UInt32Array, UInt64Array,
};
use arrow::compute::{cast, concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::harness::fixtures::fixture_chunk;

pub fn write_parquet(path: &Path, batch: &RecordBatch) {
    write_parquet_file(path, batch.schema(), std::slice::from_ref(batch), None);
}

fn write_parquet_file(
    path: &Path,
    schema: SchemaRef,
    batches: &[RecordBatch],
    props: Option<WriterProperties>,
) {
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, props).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap();
}

/// Write one table of a synthetic chunk, named the way a chunk names it.
pub fn write_table(dir: &Path, table: &str, fields: Vec<Field>, columns: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    write_parquet(&dir.join(format!("{table}.parquet")), &batch);
}

/// A blocks table carrying nothing but the numbers, under the column name the
/// minimal catalogs in these tests declare.
pub fn blocks_parquet(dir: &Path, numbers: &[u64]) {
    blocks_parquet_named(dir, "number", numbers);
}

/// The same, for a catalog that calls its block number something else.
pub fn blocks_parquet_named(dir: &Path, column: &str, numbers: &[u64]) {
    write_table(
        dir,
        "blocks",
        vec![Field::new(column, DataType::UInt64, false)],
        vec![Arc::new(UInt64Array::from(numbers.to_vec())) as ArrayRef],
    );
}

/// Copy a chunk into a temp dir, letting `visit` replace or omit an entry.
///
/// Returning `true` means the visitor handled the entry. Everything else is
/// copied byte for byte.
fn copy_chunk(src: &Path, mut visit: impl FnMut(&str, &Path, &Path) -> bool) -> tempfile::TempDir {
    let dir = tempfile::TempDir::new().unwrap();

    for entry in std::fs::read_dir(src).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let dst = dir.path().join(&name);

        if !visit(&name, &path, &dst) {
            std::fs::copy(&path, &dst).unwrap();
        }
    }

    dir
}

/// Copy a chunk into a temp dir, rewriting one table through `rewrite`.
///
/// Every table but one is copied byte for byte; the named one is read, passed
/// through, and written back.
fn rewrite_table(
    src: &Path,
    table: &str,
    rewrite: impl Fn(SchemaRef, &[RecordBatch]) -> (SchemaRef, Vec<RecordBatch>),
) -> tempfile::TempDir {
    let table_file = format!("{table}.parquet");
    let mut rewritten = false;
    let dir = copy_chunk(src, |name, path, dst| {
        if name != table_file {
            return false;
        }

        rewritten = true;
        let (schema, batches) = read_parquet(path);
        let (schema, batches) = rewrite(schema, &batches);
        write_parquet_file(dst, schema, &batches, None);
        true
    });

    assert!(
        rewritten,
        "'{table_file}' must be present in the source chunk"
    );
    dir
}

/// Copy a fixture chunk into a temp dir, dropping one column from one table —
/// the shape of a chunk written before that column existed.
pub fn chunk_without_column(dataset: &str, table: &str, drop_column: &str) -> tempfile::TempDir {
    chunk_without_column_at(&fixture_chunk(dataset), table, drop_column)
}

/// Copy any chunk while dropping one column from one table.
pub fn chunk_without_column_at(src: &Path, table: &str, drop_column: &str) -> tempfile::TempDir {
    rewrite_table(src, table, |full, batches| {
        let keep: Vec<usize> = (0..full.fields().len())
            .filter(|&i| full.field(i).name() != drop_column)
            .collect();
        assert_eq!(
            keep.len() + 1,
            full.fields().len(),
            "'{drop_column}' must be present in the source table to drop it"
        );

        let trimmed = Arc::new(full.project(&keep).unwrap());
        let projected = batches.iter().map(|b| b.project(&keep).unwrap()).collect();

        (trimmed, projected)
    })
}

/// Copy a chunk while omitting one table entirely.
pub fn chunk_without_table(src: &Path, table: &str) -> tempfile::TempDir {
    let table_file = format!("{table}.parquet");
    let mut removed = false;
    let dir = copy_chunk(src, |name, _, _| {
        if name == table_file {
            removed = true;
            true
        } else {
            false
        }
    });

    assert!(
        removed,
        "'{table_file}' must be present in the source chunk"
    );
    dir
}

/// Copy a chunk while adding an all-null column to one table.
pub fn chunk_with_nullable_column(
    src: &Path,
    table: &str,
    column: &str,
    data_type: DataType,
) -> tempfile::TempDir {
    rewrite_table(src, table, |schema, batches| {
        assert!(
            schema.index_of(column).is_err(),
            "'{column}' must be absent from {table} before it is added"
        );

        let mut fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields.push(Field::new(column, data_type.clone(), true));
        let extended = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let rewritten = batches
            .iter()
            .map(|batch| {
                let mut columns = batch.columns().to_vec();
                columns.push(new_null_array(&data_type, batch.num_rows()));
                RecordBatch::try_new(extended.clone(), columns).unwrap()
            })
            .collect();

        (extended, rewritten)
    })
}

/// Copy a fixture chunk into a temp dir, filling one `utf8` column of one table
/// with the same value in every row — the shape of a chunk whose column is
/// populated where the bundled fixture happens to leave it null.
pub fn chunk_with_column_filled(
    dataset: &str,
    table: &str,
    column: &str,
    value: &str,
) -> tempfile::TempDir {
    rewrite_table(&fixture_chunk(dataset), table, |schema, batches| {
        let index = schema
            .index_of(column)
            .unwrap_or_else(|_| panic!("'{column}' must be present in {table} to fill it"));

        let filled = batches
            .iter()
            .map(|batch| {
                let mut columns = batch.columns().to_vec();
                columns[index] =
                    Arc::new(StringArray::from(vec![value; batch.num_rows()])) as ArrayRef;
                RecordBatch::try_new(schema.clone(), columns).unwrap()
            })
            .collect();

        (schema, filled)
    })
}

/// Read one parquet file back. The schema comes from the file rather than from
/// the first batch, so a table with no rows still round-trips.
fn read_parquet(path: &Path) -> (SchemaRef, Vec<RecordBatch>) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let batches = builder.build().unwrap().map(|b| b.unwrap()).collect();

    (schema, batches)
}

// ---------------------------------------------------------------------------
// Physical type — INV-D7
// ---------------------------------------------------------------------------

/// Copy a chunk while storing one column at a different physical type.
///
/// A declared integer type bounds the values, not the storage, so a chunk whose
/// `uint64` block number is written in 32 bits is a chunk the engine must read.
/// The cast is checked: a width too narrow for the values it holds would rewrite
/// the data rather than restore it, and a test comparing against nulls compares
/// nothing.
pub fn chunk_with_column_retyped(
    src: &Path,
    table: &str,
    column: &str,
    to: DataType,
) -> tempfile::TempDir {
    rewrite_table(src, table, |schema, batches| {
        let index = schema
            .index_of(column)
            .unwrap_or_else(|_| panic!("'{column}' must be present in {table} to retype it"));

        let mut fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields[index] = fields[index].clone().with_data_type(to.clone());
        let retyped = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));

        let rewritten = batches
            .iter()
            .map(|batch| {
                let source = batch.column(index);
                let narrowed = cast(source, &to).unwrap();
                assert_eq!(
                    narrowed.null_count(),
                    source.null_count(),
                    "{table}.{column} does not fit {to:?}: the cast introduced nulls, so the \
                     rewritten chunk holds different values rather than the same ones"
                );

                let mut columns = batch.columns().to_vec();
                columns[index] = narrowed;
                RecordBatch::try_new(retyped.clone(), columns).unwrap()
            })
            .collect();

        (retyped, rewritten)
    })
}

// ---------------------------------------------------------------------------
// Storage layout — INV-D8
// ---------------------------------------------------------------------------

/// How a chunk is written: the tuning knobs [INV-D8] says no answer may depend
/// on. `Layout::default()` writes what the parquet defaults produce; each
/// constructor turns one knob and leaves the rest alone, so a test that fails
/// names the knob that broke it.
#[derive(Clone, Default)]
pub struct Layout {
    row_group_size: Option<usize>,
    page_size: Option<usize>,
    compression: Option<Compression>,
    dictionary: Option<bool>,
    statistics: Option<EnabledStatistics>,
    row_order: RowOrder,
}

/// What order the rows sit in on disk.
#[derive(Clone, Copy, Default, PartialEq)]
enum RowOrder {
    /// However the source chunk had them.
    #[default]
    Stored,
    /// Back to front — a descending sort on whatever the writer sorted by.
    Reversed,
    /// No order at all, which is the strongest statement of "the sort key is the
    /// writer's choice": a permutation no sort key produces.
    Shuffled,
}

impl Layout {
    /// Row groups of a chosen size — the boundary a scan prunes against.
    pub fn row_groups(rows: usize) -> Self {
        Self {
            row_group_size: Some(rows),
            ..Self::default()
        }
    }

    /// Data pages of a chosen size, the boundary inside a row group.
    pub fn pages(rows: usize) -> Self {
        Self {
            page_size: Some(rows),
            ..Self::default()
        }
    }

    pub fn compressed(codec: Compression) -> Self {
        Self {
            compression: Some(codec),
            ..Self::default()
        }
    }

    pub fn without_dictionary() -> Self {
        Self {
            dictionary: Some(false),
            ..Self::default()
        }
    }

    /// No column statistics, which is the shape that leaves a scan nothing to
    /// prune with: it must return the same rows the hard way.
    pub fn without_statistics() -> Self {
        Self {
            statistics: Some(EnabledStatistics::None),
            ..Self::default()
        }
    }

    /// Every table stored back to front — a descending sort on whatever the
    /// writer sorted by.
    pub fn reversed() -> Self {
        Self {
            row_order: RowOrder::Reversed,
            ..Self::default()
        }
    }

    /// Every table shuffled. Reversal is still an order; this is the case where
    /// the stored order carries no information at all, which is what a catalog
    /// declaring a sort key must not be allowed to assume.
    pub fn shuffled() -> Self {
        Self {
            row_order: RowOrder::Shuffled,
            ..Self::default()
        }
    }

    fn properties(&self) -> WriterProperties {
        let mut props = WriterProperties::builder();

        if let Some(rows) = self.row_group_size {
            props = props.set_max_row_group_size(rows);
        }
        if let Some(rows) = self.page_size {
            props = props
                .set_data_page_row_count_limit(rows)
                .set_write_batch_size(rows);
        }
        if let Some(codec) = self.compression {
            props = props.set_compression(codec);
        }
        if let Some(enabled) = self.dictionary {
            props = props.set_dictionary_enabled(enabled);
        }
        if let Some(statistics) = self.statistics {
            props = props.set_statistics_enabled(statistics);
        }

        props.build()
    }
}

/// Copy a chunk into a temp dir, rewriting every table under another storage
/// layout. The rows and their values are untouched; only how they are laid out
/// on disk changes.
pub fn chunk_relaid(src: &Path, layout: &Layout) -> tempfile::TempDir {
    let mut tables = 0;
    let dir = copy_chunk(src, |name, path, dst| {
        if !name.ends_with(".parquet") {
            return false;
        }

        tables += 1;
        let (schema, batches) = read_parquet(path);
        let batches = match layout.row_order {
            RowOrder::Stored => batches,
            order => vec![permute(&schema, &batches, order)],
        };
        write_parquet_file(dst, schema, &batches, Some(layout.properties()));
        true
    });

    assert!(tables > 0, "the source chunk must hold at least one table");
    dir
}

/// One batch holding every row of a table, in the given order.
///
/// The shuffle is a fixed-seed Fisher-Yates rather than a random one: a layout
/// test that fails on one run in twenty and passes on the rest is a layout test
/// nobody trusts.
fn permute(schema: &SchemaRef, batches: &[RecordBatch], order: RowOrder) -> RecordBatch {
    let whole = concat_batches(schema, batches).unwrap();
    let rows = whole.num_rows();

    let mut indices: Vec<u32> = (0..rows as u32).rev().collect();
    if order == RowOrder::Shuffled {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        for i in (1..rows).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            indices.swap(i, (state % (i as u64 + 1)) as usize);
        }
    }

    let indices = UInt32Array::from(indices);
    let columns = whole
        .columns()
        .iter()
        .map(|column| take(column, &indices, None).unwrap())
        .collect();

    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

/// Read named columns of one table of a chunk, leaving the rest on disk.
///
/// Returns `None` when the chunk has no such table: a catalog names every table
/// the dataset can have, and a chunk carries the ones its range needed.
pub fn read_columns(chunk: &Path, table: &str, columns: &[&str]) -> Option<Vec<RecordBatch>> {
    let path = chunk.join(format!("{table}.parquet"));
    if !path.is_file() {
        return None;
    }

    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let projection: Vec<usize> = columns
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .unwrap_or_else(|_| panic!("{table} must carry '{name}'"))
        })
        .collect();
    let mask = parquet::arrow::ProjectionMask::roots(builder.parquet_schema(), projection);

    let reader = builder.with_projection(mask).build().unwrap();

    Some(reader.map(|b| b.unwrap()).collect())
}

/// Copy a chunk while storing the *elements* of one list column at a different
/// physical type.
///
/// A hierarchical address is a list of item indices, and INV-D7's tolerance
/// reaches inside the list: Solana stores the elements in sixteen bits and EVM
/// in thirty-two, and both are the same declared column.
pub fn chunk_with_list_elements_retyped(
    src: &Path,
    table: &str,
    column: &str,
    to: DataType,
) -> tempfile::TempDir {
    rewrite_table(src, table, |schema, batches| {
        let index = schema
            .index_of(column)
            .unwrap_or_else(|_| panic!("'{column}' must be present in {table} to retype it"));

        let DataType::List(item) = schema.field(index).data_type() else {
            panic!("{table}.{column} is not a list");
        };
        let item = Arc::new(Field::new(item.name(), to.clone(), item.is_nullable()));

        let mut fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields[index] = fields[index]
            .clone()
            .with_data_type(DataType::List(item.clone()));
        let retyped = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));

        let rewritten = batches
            .iter()
            .map(|batch| {
                let list = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                let values = cast(list.values(), &to).unwrap();
                assert_eq!(
                    values.null_count(),
                    list.values().null_count(),
                    "{table}.{column}'s elements do not fit {to:?}: the cast introduced nulls, \
                     so the rewritten chunk holds different values rather than the same ones"
                );

                let narrowed = ListArray::new(
                    item.clone(),
                    list.offsets().clone(),
                    values,
                    list.nulls().cloned(),
                );

                let mut columns = batch.columns().to_vec();
                columns[index] = Arc::new(narrowed) as ArrayRef;
                RecordBatch::try_new(retyped.clone(), columns).unwrap()
            })
            .collect();

        (retyped, rewritten)
    })
}
