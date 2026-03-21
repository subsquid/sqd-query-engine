//! Load parquet data into memory and spillover backends for benchmarking.

use anyhow::Result;
use arrow::array::*;
use arrow::compute;
use arrow::record_batch::RecordBatch;
use sqd_query_engine::scan::memory_backend::{BlockData, MemoryChunkReader};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader};
use sqd_storage::db::{Chunk, Database, DatabaseSettings, DatasetId, DatasetKind};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

/// Load all parquet tables into MemoryChunkReader (one block at a time).
pub fn load_parquet_to_memory(parquet_dir: &Path) -> MemoryChunkReader {
    let parquet = ParquetChunkReader::open(parquet_dir).unwrap();
    let mut reader = MemoryChunkReader::new();

    // Collect all blocks across all tables
    let mut all_block_tables: BTreeMap<u32, HashMap<String, RecordBatch>> = BTreeMap::new();

    for table_name in parquet.table_names() {
        let batches = parquet.read_all(&table_name).unwrap();
        let by_block = split_by_block(&batches, &table_name);
        for (bn, batch) in by_block {
            all_block_tables
                .entry(bn)
                .or_default()
                .insert(table_name.clone(), batch);
        }
    }

    // Push blocks in order
    for (bn, tables) in all_block_tables {
        reader.push(BlockData {
            block_number: bn as u64,
            tables,
        });
    }

    reader
}

/// Load parquet data into a spillover-style parquet directory.
/// Simulates spillover: blocks from memory flushed to parquet, sorted by sort_key.
pub fn load_parquet_to_spillover(
    parquet_dir: &Path,
    spillover_dir: &Path,
    metadata: &sqd_query_engine::metadata::DatasetDescription,
) -> ParquetChunkReader {
    use sqd_query_engine::scan::memory_backend::BlockData;
    use sqd_query_engine::scan::parquet_writer::flush_to_parquet;

    let memory = load_parquet_to_memory(parquet_dir);
    let blocks: Vec<std::sync::Arc<BlockData>> = memory
        .iter_blocks()
        .cloned()
        .collect();

    flush_to_parquet(&blocks, spillover_dir, metadata).unwrap();

    ParquetChunkReader::open(spillover_dir).unwrap()
}

/// Result of loading parquet data into legacy sqd_storage (for legacy engine).
pub struct LegacyStorage {
    pub db: Database,
    pub chunk: Chunk,
}

/// Load parquet into legacy sqd_storage for use with legacy engine.
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

    Ok(LegacyStorage { db, chunk })
}

/// Split RecordBatches by block_number column.
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
        _ => BooleanArray::from(vec![true; array.len()]),
    }
}
