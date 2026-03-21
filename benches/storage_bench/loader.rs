//! Load parquet fixture data into LMDB and RocksDB for benchmarking.
//!
//! Two loading strategies:
//! - per-block: one KV entry per block (fine-grained, but IPC overhead per entry)
//! - bulk: one KV entry per table (fewer entries, amortized IPC overhead)

use anyhow::Result;
use arrow::array::*;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use sqd_query_engine::scan::legacy_backend::LegacyStorageChunkReader;
use sqd_query_engine::scan::lmdb_backend::LmdbChunkReader;
use sqd_query_engine::scan::rocks_backend::RocksDbChunkReader;
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader};
use sqd_storage::db::{Chunk, DatasetId, DatasetKind};
use std::collections::BTreeMap;
use std::path::Path;

/// Load all parquet tables into LMDB — one entry per block per table.
pub fn load_parquet_to_lmdb(parquet_dir: &Path, lmdb_dir: &Path) -> Result<LmdbChunkReader> {
    let parquet = ParquetChunkReader::open(parquet_dir)?;
    let mut reader = LmdbChunkReader::open(lmdb_dir)?;

    for table_name in parquet.table_names() {
        let schema = parquet.table_schema(&table_name).unwrap();
        reader.create_table(&table_name, schema)?;

        let batches = parquet.read_all(&table_name)?;
        let by_block = split_by_block(&batches, &table_name);
        let entries: Vec<(u32, &RecordBatch)> = by_block.iter().map(|(bn, b)| (*bn, b)).collect();
        reader.put_batches(&table_name, &entries)?;
    }

    Ok(reader)
}

/// Load all parquet tables into LMDB — ONE entry per table (bulk).
pub fn load_parquet_to_lmdb_bulk(parquet_dir: &Path, lmdb_dir: &Path) -> Result<LmdbChunkReader> {
    let parquet = ParquetChunkReader::open(parquet_dir)?;
    let mut reader = LmdbChunkReader::open(lmdb_dir)?;

    for table_name in parquet.table_names() {
        let schema = parquet.table_schema(&table_name).unwrap();
        reader.create_table(&table_name, schema)?;

        let batches = parquet.read_all(&table_name)?;
        if !batches.is_empty() {
            let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
            reader.put_batches(&table_name, &[(0, &merged)])?;
        }
    }

    Ok(reader)
}

/// Load all parquet tables into LMDB with zstd compression — ONE entry per table.
pub fn load_parquet_to_lmdb_bulk_zstd(
    parquet_dir: &Path,
    lmdb_dir: &Path,
) -> Result<LmdbChunkReader> {
    let parquet = ParquetChunkReader::open(parquet_dir)?;
    let mut reader = LmdbChunkReader::open_compressed(lmdb_dir)?;

    for table_name in parquet.table_names() {
        let schema = parquet.table_schema(&table_name).unwrap();
        reader.create_table(&table_name, schema)?;

        let batches = parquet.read_all(&table_name)?;
        if !batches.is_empty() {
            let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
            reader.put_batches(&table_name, &[(0, &merged)])?;
        }
    }

    Ok(reader)
}

/// Load all parquet tables into RocksDB — one entry per block per table.
pub fn load_parquet_to_rocksdb(parquet_dir: &Path, rocks_dir: &Path) -> Result<RocksDbChunkReader> {
    let parquet = ParquetChunkReader::open(parquet_dir)?;
    let table_names = parquet.table_names();
    let table_name_refs: Vec<&str> = table_names.iter().map(String::as_str).collect();

    let mut reader = RocksDbChunkReader::create(rocks_dir, &table_name_refs)?;

    for table_name in &table_names {
        let schema = parquet.table_schema(table_name).unwrap();
        reader.set_schema(table_name, schema);

        let batches = parquet.read_all(table_name)?;
        let by_block = split_by_block(&batches, table_name);
        let entries: Vec<(u32, &RecordBatch)> = by_block.iter().map(|(bn, b)| (*bn, b)).collect();
        reader.put_batches(table_name, &entries)?;
    }

    Ok(reader)
}

/// Load all parquet tables into RocksDB — ONE entry per table (bulk).
pub fn load_parquet_to_rocksdb_bulk(
    parquet_dir: &Path,
    rocks_dir: &Path,
) -> Result<RocksDbChunkReader> {
    let parquet = ParquetChunkReader::open(parquet_dir)?;
    let table_names = parquet.table_names();
    let table_name_refs: Vec<&str> = table_names.iter().map(String::as_str).collect();

    let mut reader = RocksDbChunkReader::create(rocks_dir, &table_name_refs)?;

    for table_name in &table_names {
        let schema = parquet.table_schema(table_name).unwrap();
        reader.set_schema(table_name, schema);

        let batches = parquet.read_all(table_name)?;
        if !batches.is_empty() {
            let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
            reader.put_batches(table_name, &[(0, &merged)])?;
        }
    }

    Ok(reader)
}

/// Load all parquet tables into RocksDB with LZ4 compression — ONE entry per table (bulk).
pub fn load_parquet_to_rocksdb_bulk_lz4(
    parquet_dir: &Path,
    rocks_dir: &Path,
) -> Result<RocksDbChunkReader> {
    let parquet = ParquetChunkReader::open(parquet_dir)?;
    let table_names = parquet.table_names();
    let table_name_refs: Vec<&str> = table_names.iter().map(String::as_str).collect();

    let mut reader = RocksDbChunkReader::create_compressed(rocks_dir, &table_name_refs)?;

    for table_name in &table_names {
        let schema = parquet.table_schema(table_name).unwrap();
        reader.set_schema(table_name, schema);

        let batches = parquet.read_all(table_name)?;
        if !batches.is_empty() {
            let merged = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
            reader.put_batches(table_name, &[(0, &merged)])?;
        }
    }

    Ok(reader)
}

/// Split RecordBatches by block_number column, returning one merged batch per unique block.
fn split_by_block(batches: &[RecordBatch], table_name: &str) -> Vec<(u32, RecordBatch)> {
    let bn_col = if table_name == "blocks" {
        "number"
    } else {
        "block_number"
    };

    let mut per_block: BTreeMap<u32, Vec<RecordBatch>> = BTreeMap::new();

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let col = match batch.column_by_name(bn_col) {
            Some(c) => c,
            None => {
                per_block.entry(0).or_default().push(batch.clone());
                continue;
            }
        };

        let block_numbers = extract_block_numbers_sorted(col.as_ref());

        if block_numbers.len() == 1 {
            per_block
                .entry(block_numbers[0])
                .or_default()
                .push(batch.clone());
            continue;
        }

        for &bn in &block_numbers {
            let mask = build_block_eq_mask(col.as_ref(), bn);
            if let Ok(filtered) = compute::filter_record_batch(batch, &mask) {
                if filtered.num_rows() > 0 {
                    per_block.entry(bn).or_default().push(filtered);
                }
            }
        }
    }

    per_block
        .into_iter()
        .filter_map(|(bn, fragments)| {
            if fragments.len() == 1 {
                Some((bn, fragments.into_iter().next().unwrap()))
            } else {
                let schema = fragments[0].schema();
                let merged = arrow::compute::concat_batches(&schema, &fragments).ok()?;
                if merged.num_rows() > 0 {
                    Some((bn, merged))
                } else {
                    None
                }
            }
        })
        .collect()
}

fn extract_block_numbers_sorted(array: &dyn Array) -> Vec<u32> {
    let mut blocks = std::collections::BTreeSet::new();
    match array.data_type() {
        arrow::datatypes::DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) && arr.value(i) >= 0 {
                    blocks.insert(arr.value(i) as u32);
                }
            }
        }
        arrow::datatypes::DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    blocks.insert(arr.value(i));
                }
            }
        }
        arrow::datatypes::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) && arr.value(i) >= 0 {
                    blocks.insert(arr.value(i) as u32);
                }
            }
        }
        arrow::datatypes::DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    blocks.insert(arr.value(i) as u32);
                }
            }
        }
        _ => {}
    }
    blocks.into_iter().collect()
}

fn build_block_eq_mask(array: &dyn Array, target: u32) -> BooleanArray {
    let len = array.len();
    match array.data_type() {
        arrow::datatypes::DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            let target = target as i32;
            BooleanArray::from_unary(arr, |v| v == target)
        }
        arrow::datatypes::DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            BooleanArray::from_unary(arr, |v| v == target)
        }
        arrow::datatypes::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let target = target as i64;
            BooleanArray::from_unary(arr, |v| v == target)
        }
        arrow::datatypes::DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            let target = target as u64;
            BooleanArray::from_unary(arr, |v| v == target)
        }
        _ => BooleanArray::from(vec![true; len]),
    }
}

/// Result of loading parquet data into legacy sqd_storage (for legacy engine).
pub struct LegacyStorage {
    pub db: sqd_storage::db::Database,
    pub chunk: Chunk,
}

fn build_legacy_storage(
    parquet_dir: &Path,
    storage_dir: &Path,
) -> Result<(sqd_storage::db::Database, Chunk)> {
    use sqd_storage::db::DatabaseSettings;

    let parquet = ParquetChunkReader::open(parquet_dir)?;
    let db = DatabaseSettings::default().open(storage_dir)?;

    let dataset_id = DatasetId::try_from("bench").unwrap();
    let dataset_kind = DatasetKind::try_from("bench").unwrap();
    db.create_dataset_if_not_exists(dataset_id, dataset_kind)?;

    let mut table_ids = BTreeMap::new();
    let mut first_block: u64 = u64::MAX;
    let mut last_block: u64 = 0;

    for table_name in parquet.table_names() {
        let schema = parquet.table_schema(&table_name).unwrap();
        let batches = parquet.read_all(&table_name)?;

        let mut builder = db.new_table_builder(schema);
        for batch in &batches {
            if batch.num_rows() > 0 {
                builder.write_record_batch(batch)?;

                let bn_col_name = if table_name == "blocks" {
                    "number"
                } else {
                    "block_number"
                };
                if let Some(col) = batch.column_by_name(bn_col_name) {
                    let block_numbers = extract_block_numbers_sorted(col.as_ref());
                    if let Some(&min) = block_numbers.first() {
                        first_block = first_block.min(min as u64);
                    }
                    if let Some(&max) = block_numbers.last() {
                        last_block = last_block.max(max as u64);
                    }
                }
            }
        }
        let table_id = builder.finish()?;
        table_ids.insert(table_name, table_id);
    }

    if first_block == u64::MAX {
        first_block = 0;
    }

    let chunk = Chunk::V1 {
        first_block,
        last_block,
        last_block_hash: "0x0000".to_string(),
        parent_block_hash: "0x0000".to_string(),
        first_block_time: None,
        last_block_time: None,
        tables: table_ids,
    };

    db.insert_chunk(dataset_id, &chunk)?;

    Ok((db, chunk))
}

/// Load parquet into legacy sqd_storage for use with legacy engine.
pub fn load_parquet_to_legacy_storage(
    parquet_dir: &Path,
    storage_dir: &Path,
) -> Result<LegacyStorage> {
    let (db, chunk) = build_legacy_storage(parquet_dir, storage_dir)?;
    Ok(LegacyStorage { db, chunk })
}

/// Load parquet into legacy sqd_storage for use with new engine.
pub fn load_parquet_to_legacy(
    parquet_dir: &Path,
    storage_dir: &Path,
) -> Result<LegacyStorageChunkReader> {
    let (db, chunk) = build_legacy_storage(parquet_dir, storage_dir)?;
    LegacyStorageChunkReader::from_database(db, chunk)
}
