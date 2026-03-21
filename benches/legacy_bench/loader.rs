//! Load parquet data into legacy sqd_storage for benchmarking.

use anyhow::Result;
use arrow::array::*;
use sqd_query_engine::scan::{ChunkReader as NewChunkReader, ParquetChunkReader};
use sqd_storage::db::{
    Chunk as StorageChunk, Database, DatabaseSettings, DatasetId, DatasetKind,
};
use std::collections::BTreeMap;
use std::path::Path;

/// Result of loading parquet data into legacy sqd_storage.
pub struct LegacyStorage {
    pub db: Database,
    pub chunk: StorageChunk,
}

/// Load parquet data into legacy sqd_storage format.
pub fn load_parquet_to_legacy_storage(
    parquet_dir: &Path,
    storage_dir: &Path,
) -> Result<LegacyStorage> {
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

                let bn_col = if table_name == "blocks" {
                    "number"
                } else {
                    "block_number"
                };
                if let Some(col) = batch.column_by_name(bn_col) {
                    let (lo, hi) = block_range(col.as_ref());
                    first_block = first_block.min(lo as u64);
                    last_block = last_block.max(hi as u64);
                }
            }
        }
        let table_id = builder.finish()?;
        table_ids.insert(table_name, table_id);
    }

    if first_block == u64::MAX {
        first_block = 0;
    }

    let chunk = StorageChunk::V1 {
        first_block,
        last_block,
        last_block_hash: "0x0000".to_string(),
        parent_block_hash: "0x0000".to_string(),
        first_block_time: None,
        last_block_time: None,
        tables: table_ids,
    };

    db.insert_chunk(dataset_id, &chunk)?;

    Ok(LegacyStorage { db, chunk })
}

fn block_range(array: &dyn Array) -> (u32, u32) {
    let mut lo = u32::MAX;
    let mut hi = 0u32;
    match array.data_type() {
        arrow::datatypes::DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let v = arr.value(i) as u32;
                    lo = lo.min(v);
                    hi = hi.max(v);
                }
            }
        }
        arrow::datatypes::DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    lo = lo.min(arr.value(i));
                    hi = hi.max(arr.value(i));
                }
            }
        }
        arrow::datatypes::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let v = arr.value(i) as u32;
                    lo = lo.min(v);
                    hi = hi.max(v);
                }
            }
        }
        arrow::datatypes::DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let v = arr.value(i) as u32;
                    lo = lo.min(v);
                    hi = hi.max(v);
                }
            }
        }
        _ => {}
    }
    (lo, hi)
}
