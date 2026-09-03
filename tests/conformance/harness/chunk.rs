//! Synthetic chunk construction.
//!
//! A fixture chunk is one shape written by one archiver version. Several
//! invariants are about the shapes it is *not* — a narrower physical type, a
//! column dropped, a sort key the catalog does not expect — so those tests write
//! their own parquet.
//!
//! This is not HC-3, but the two fixture rewriters below are where HC-3 starts:
//! they copy a chunk with one column dropped or one column overwritten. HC-3
//! adds the other axes — physical type, sort key, row-group size — and with them
//! the invariants CT-6 and CT-8 cannot reach today.

use arrow::array::{ArrayRef, StringArray, UInt64Array};
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

/// Copy a fixture chunk into a temp dir, rewriting one table through `rewrite`.
///
/// Every table but one is copied byte for byte; the named one is read, passed
/// through, and written back. Both rewriters below are this walk plus four
/// lines, and they were two copies of it.
fn rewrite_chunk(
    dataset: &str,
    table: &str,
    rewrite: impl Fn(SchemaRef, &[RecordBatch]) -> (SchemaRef, Vec<RecordBatch>),
) -> tempfile::TempDir {
    let src = fixture_chunk(dataset);
    let dir = tempfile::TempDir::new().unwrap();

    for entry in std::fs::read_dir(&src).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let dst = dir.path().join(&name);

        if name != format!("{table}.parquet") {
            std::fs::copy(&path, &dst).unwrap();
            continue;
        }

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let (schema, rewritten) = rewrite(batches[0].schema(), &batches);

        let mut writer = ArrowWriter::try_new(File::create(&dst).unwrap(), schema, None).unwrap();
        for batch in &rewritten {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
    }

    dir
}

/// Copy a fixture chunk into a temp dir, dropping one column from one table —
/// the shape of a chunk written before that column existed.
pub fn chunk_without_column(dataset: &str, table: &str, drop_column: &str) -> tempfile::TempDir {
    rewrite_chunk(dataset, table, |full, batches| {
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

/// Copy a fixture chunk into a temp dir, filling one `utf8` column of one table
/// with the same value in every row — the shape of a chunk whose column is
/// populated where the bundled fixture happens to leave it null.
pub fn chunk_with_column_filled(
    dataset: &str,
    table: &str,
    column: &str,
    value: &str,
) -> tempfile::TempDir {
    rewrite_chunk(dataset, table, |schema, batches| {
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
