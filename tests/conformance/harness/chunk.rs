//! Synthetic chunk construction.
//!
//! A fixture chunk is one shape written by one archiver version. Several
//! invariants are about the shapes it is *not* — a narrower physical type, a
//! column dropped, a sort key the catalog does not expect — so those tests write
//! their own parquet.
//!
//! This is the portable core of HC-3: it copies a chunk with a table or column
//! dropped, a nullable column added, or a column overwritten. Physical type,
//! sort key and row-group size are the remaining axes.

use arrow::array::{new_null_array, ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::harness::fixtures::fixture_chunk;

pub fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
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
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let (schema, rewritten) = rewrite(batches[0].schema(), &batches);

        let mut writer = ArrowWriter::try_new(File::create(dst).unwrap(), schema, None).unwrap();
        for batch in &rewritten {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
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
