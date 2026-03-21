//! Shared scan logic for KV-backed ChunkReaders (LMDB, RocksDB).
//!
//! These backends store data as Arrow IPC mini-batches (one per block per table).
//! On scan, they read all relevant batches and apply filters in-memory using the
//! same predicate/key-filter/hierarchical-filter logic as the parquet scanner.

use crate::scan::predicate::{evaluate_predicates_on_batch, RowPredicate};
use crate::scan::scanner::{
    block_range_mask, build_output_schema, composite_key_in_set_mask, hierarchical_mask,
};
use crate::scan::ScanRequest;
use anyhow::Result;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
/// Serialize a RecordBatch to Arrow IPC stream format.
pub fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

/// Deserialize Arrow IPC stream bytes into RecordBatches.
pub fn decode_batches(data: &[u8]) -> Result<Vec<RecordBatch>> {
    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Apply all ScanRequest filters to raw RecordBatches read from a KV store.
/// This is the shared post-read filter pipeline for LMDB and RocksDB backends.
pub fn apply_scan_filters(
    batches: Vec<RecordBatch>,
    request: &ScanRequest,
    table_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let output_schema = build_output_schema(table_schema, &request.output_columns);

    // Early projection: collect all columns needed for filtering + output,
    // so filter_record_batch copies fewer columns through masks.
    let mut needed_columns: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for col in &request.output_columns {
        needed_columns.insert(col);
    }
    for pred in &request.predicates {
        for col in pred.required_columns() {
            needed_columns.insert(col);
        }
    }
    if let Some(bn) = request.block_number_column {
        needed_columns.insert(bn);
    }
    if let Some(kf) = &request.key_filter {
        for col in &kf.columns {
            needed_columns.insert(col.as_str());
        }
    }
    if let Some(hf) = &request.hierarchical_filter {
        for col in &hf.group_key_columns {
            needed_columns.insert(col.as_str());
        }
        needed_columns.insert(&hf.address_column);
    }

    // Pre-compute column indices for early projection (fewer columns through filter masks)
    let needed_indices: Vec<usize> = (0..table_schema.fields().len())
        .filter(|&i| needed_columns.contains(table_schema.field(i).name().as_str()))
        .collect();
    let do_early_project = needed_indices.len() < table_schema.fields().len();

    let mut result = Vec::new();

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        // Project to needed columns before filtering (fewer columns to copy through masks)
        let mut current = if do_early_project {
            batch.project(&needed_indices)?
        } else {
            batch
        };

        // 1. Block range filter
        if let Some(bn_col) = request.block_number_column {
            if request.from_block.is_some() || request.to_block.is_some() {
                if let Some(col) = current.column_by_name(bn_col) {
                    let mask = block_range_mask(col, request.from_block, request.to_block);
                    current = arrow::compute::filter_record_batch(&current, &mask)?;
                    if current.num_rows() == 0 {
                        continue;
                    }
                }
            }
        }

        // 2. Predicates (OR across items)
        if !request.predicates.is_empty() {
            let preds: Vec<RowPredicate> = request
                .predicates
                .iter()
                .map(|p| (*p).clone())
                .collect();
            match evaluate_predicates_on_batch(&current, &preds) {
                Some(filtered) => current = filtered,
                None => continue,
            }
        }

        // 3. Key filter
        if let Some(kf) = &request.key_filter {
            let mask = composite_key_in_set_mask(&current, &kf.columns, &kf.key_set);
            current = arrow::compute::filter_record_batch(&current, &mask)?;
            if current.num_rows() == 0 {
                continue;
            }
        }

        // 4. Hierarchical filter
        if let Some(hf) = &request.hierarchical_filter {
            let mask = hierarchical_mask(
                &current,
                &hf.source_addresses,
                &hf.first_key_set,
                &hf.group_key_columns,
                &hf.address_column,
                hf.mode,
                hf.inclusive,
            );
            current = arrow::compute::filter_record_batch(&current, &mask)?;
            if current.num_rows() == 0 {
                continue;
            }
        }

        // 5. Project to output columns
        let projected = project_batch(&current, &output_schema)?;
        if projected.num_rows() > 0 {
            result.push(projected);
        }
    }

    Ok(result)
}

/// Project a RecordBatch to only the columns in the output schema.
fn project_batch(batch: &RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
    let columns: Vec<_> = output_schema
        .fields()
        .iter()
        .map(|field| {
            batch
                .column_by_name(field.name())
                .cloned()
                .unwrap_or_else(|| {
                    arrow::array::new_null_array(field.data_type(), batch.num_rows())
                })
        })
        .collect();
    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

/// Serialize a RecordBatch to Arrow IPC + zstd compression.
#[cfg(feature = "lmdb")]
pub fn encode_batch_zstd(batch: &RecordBatch) -> Result<Vec<u8>> {
    let ipc = encode_batch(batch)?;
    let compressed = zstd::encode_all(ipc.as_slice(), 3)?;
    Ok(compressed)
}

/// Deserialize zstd-compressed Arrow IPC bytes into RecordBatches.
#[cfg(feature = "lmdb")]
pub fn decode_batches_zstd(data: &[u8]) -> Result<Vec<RecordBatch>> {
    let decompressed = zstd::decode_all(data)?;
    decode_batches(&decompressed)
}

/// Build a KV key for a table + block: `[block_number as u32 BE]`.
/// This gives us natural sort order by block number in LMDB/RocksDB.
pub fn block_key(block_number: u32) -> [u8; 4] {
    block_number.to_be_bytes()
}

/// Extract block_number from a KV key.
pub fn key_block_number(key: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&key[..4]);
    u32::from_be_bytes(buf)
}
