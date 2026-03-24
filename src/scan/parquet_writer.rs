//! Flush in-memory blocks to sorted parquet chunk files.
//!
//! Takes blocks from the memory buffer, sorts each table by its sort_key
//! (from metadata YAML), and writes one parquet file per table into a chunk directory.

use crate::metadata::DatasetDescription;
use crate::scan::memory_backend::BlockData;
use anyhow::{Context, Result};
use std::sync::Arc;
use arrow::array::*;
use arrow::compute::{self, SortColumn};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::Path;

/// Flush blocks to a parquet chunk directory.
///
/// - `blocks`: blocks to write (will be consumed)
/// - `chunk_dir`: output directory (e.g., `chunks/24550597-24550820/`)
/// - `metadata`: dataset description for sort keys
///
/// Creates one parquet file per table, sorted by the table's `sort_key`.
pub fn flush_to_parquet(
    blocks: &[Arc<BlockData>],
    chunk_dir: &Path,
    metadata: &DatasetDescription,
) -> Result<()> {
    if blocks.is_empty() {
        return Ok(());
    }

    // Write to a temp directory, then atomically rename.
    // If killed mid-write, only the temp dir is left (cleaned up on next startup).
    let parent = chunk_dir.parent().unwrap_or(Path::new("."));
    let chunk_name = chunk_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("chunk");
    let tmp_dir = parent.join(format!(".tmp-{}", chunk_name));

    // Clean up any leftover temp dir from a previous crash
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir)
            .with_context(|| format!("removing stale temp dir {}", tmp_dir.display()))?;
    }

    fs::create_dir_all(&tmp_dir)
        .with_context(|| format!("creating temp chunk dir {}", tmp_dir.display()))?;

    let result = write_chunk_tables(blocks, &tmp_dir, metadata);

    if let Err(e) = result {
        let _ = fs::remove_dir_all(&tmp_dir);
        return Err(e);
    }

    // Atomic rename: temp dir → final dir
    fs::rename(&tmp_dir, chunk_dir)
        .with_context(|| format!("renaming {} → {}", tmp_dir.display(), chunk_dir.display()))?;

    Ok(())
}

fn write_chunk_tables(
    blocks: &[Arc<BlockData>],
    chunk_dir: &Path,
    metadata: &DatasetDescription,
) -> Result<()> {
    // Collect all batches per table across all blocks
    let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    for block in blocks {
        for (table_name, batch) in &block.tables {
            if batch.num_rows() > 0 {
                table_batches
                    .entry(table_name.clone())
                    .or_default()
                    .push(batch.clone());
            }
        }
    }

    // Write each table as a parquet file
    for (table_name, batches) in &table_batches {
        if batches.is_empty() {
            continue;
        }

        // Merge all batches into one
        let schema = batches[0].schema();
        let merged = compute::concat_batches(&schema, batches)
            .with_context(|| format!("merging batches for table {}", table_name))?;

        if merged.num_rows() == 0 {
            continue;
        }

        // Sort by sort_key if available
        let sorted = if let Some(table_desc) = metadata.table(table_name) {
            if !table_desc.sort_key.is_empty() {
                sort_batch(&merged, &table_desc.sort_key)?
            } else {
                merged
            }
        } else {
            merged
        };

        // Write parquet file
        let file_path = chunk_dir.join(format!("{}.parquet", table_name));
        write_parquet(&sorted, &file_path)?;
    }

    Ok(())
}

/// Sort a RecordBatch by the given column names.
fn sort_batch(batch: &RecordBatch, sort_columns: &[String]) -> Result<RecordBatch> {
    if batch.num_rows() <= 1 {
        return Ok(batch.clone());
    }

    // Build sort columns
    let sort_cols: Vec<SortColumn> = sort_columns
        .iter()
        .filter_map(|name| {
            batch.column_by_name(name).map(|col| SortColumn {
                values: col.clone(),
                options: None, // ascending, nulls last (default)
            })
        })
        .collect();

    if sort_cols.is_empty() {
        return Ok(batch.clone());
    }

    let indices = compute::lexsort_to_indices(&sort_cols, None)
        .context("computing sort indices")?;

    let sorted_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &indices, None).map(|a| a))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("applying sort indices")?;

    RecordBatch::try_new(batch.schema(), sorted_columns).context("building sorted batch")
}

/// Write a RecordBatch to a parquet file with compression and row group stats.
fn write_parquet(batch: &RecordBatch, path: &Path) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("creating parquet file {}", path.display()))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::LZ4)
        .set_max_row_group_size(8_192) // small RGs for selective row group pruning
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .with_context(|| format!("creating parquet writer {}", path.display()))?;

    writer.write(batch).context("writing parquet data")?;
    writer.close().context("closing parquet file")?;

    Ok(())
}

/// Determine the chunk directory name from block range.
pub fn chunk_dir_name(first_block: u64, last_block: u64) -> String {
    format!("{}-{}", first_block, last_block)
}

/// Parse a chunk directory name into (first_block, last_block).
pub fn parse_chunk_dir_name(name: &str) -> Option<(u64, u64)> {
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() == 2 {
        let first = parts[0].parse::<u64>().ok()?;
        let last = parts[1].parse::<u64>().ok()?;
        Some((first, last))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::load_dataset_description;
    use crate::scan::memory_backend::BlockData;
    use crate::scan::{ChunkReader, ParquetChunkReader};
    use arrow::datatypes::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_batch(block_number: i32, values: &[(&str, i32)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::Int32, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let bn: Vec<i32> = values.iter().map(|_| block_number).collect();
        let addrs: Vec<&str> = values.iter().map(|(a, _)| *a).collect();
        let vals: Vec<i32> = values.iter().map(|(_, v)| *v).collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(bn)),
                Arc::new(StringArray::from(addrs)),
                Arc::new(Int32Array::from(vals)),
            ],
        )
        .unwrap()
    }

    fn make_block(block_number: u64, values: &[(&str, i32)]) -> Arc<BlockData> {
        let mut tables = HashMap::new();
        tables.insert("logs".to_string(), make_batch(block_number as i32, values));
        Arc::new(BlockData {
            block_number,
            tables,
        })
    }

    #[test]
    fn test_flush_creates_parquet() {
        let tmp = TempDir::new().unwrap();
        let chunk_dir = tmp.path().join("100-101");
        let meta = load_dataset_description(Path::new("metadata/evm.yaml")).unwrap();

        let blocks: Vec<Arc<BlockData>> = vec![
            make_block(100, &[("0xaaa", 1), ("0xbbb", 2)]),
            make_block(101, &[("0xccc", 3)]),
        ];

        flush_to_parquet(&blocks, &chunk_dir, &meta).unwrap();

        // Verify parquet file exists and is readable
        let parquet_path = chunk_dir.join("logs.parquet");
        assert!(parquet_path.exists());

        let reader = ParquetChunkReader::open(&chunk_dir).unwrap();
        assert!(reader.has_table("logs"));

        let batches = reader.read_all("logs").unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_flush_empty() {
        let tmp = TempDir::new().unwrap();
        let chunk_dir = tmp.path().join("empty");
        let meta = load_dataset_description(Path::new("metadata/evm.yaml")).unwrap();

        let empty: Vec<Arc<BlockData>> = vec![];
        flush_to_parquet(&empty, &chunk_dir, &meta).unwrap();
        assert!(!chunk_dir.exists()); // no dir created for empty
    }

    #[test]
    fn test_sort_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![3, 1, 2])),
                Arc::new(StringArray::from(vec!["c", "a", "b"])),
            ],
        )
        .unwrap();

        let sorted = sort_batch(&batch, &["a".to_string()]).unwrap();
        let col = sorted
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_sort_multi_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["b", "a", "b", "a"])),
                Arc::new(Int32Array::from(vec![2, 1, 1, 2])),
            ],
        )
        .unwrap();

        // Sort by group, then value
        let sorted = sort_batch(&batch, &["group".to_string(), "value".to_string()]).unwrap();
        let groups = sorted.column_by_name("group").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let values = sorted.column_by_name("value").unwrap()
            .as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(groups.value(0), "a");
        assert_eq!(groups.value(1), "a");
        assert_eq!(groups.value(2), "b");
        assert_eq!(groups.value(3), "b");
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        assert_eq!(values.value(2), 1);
        assert_eq!(values.value(3), 2);
    }

    #[test]
    fn test_flush_with_real_evm_data() {
        let evm_dir = Path::new("data/evm/chunk");
        if !evm_dir.exists() {
            return; // skip if no fixture data
        }

        let meta = load_dataset_description(Path::new("metadata/evm.yaml")).unwrap();
        let parquet = ParquetChunkReader::open(evm_dir).unwrap();

        // Load a few blocks from real data
        let mut tables = HashMap::new();
        for table_name in parquet.table_names() {
            let batches = parquet.read_all(&table_name).unwrap();
            for batch in &batches {
                if batch.num_rows() == 0 { continue; }
                tables.insert(table_name.clone(), batch.clone());
                break;
            }
        }
        let blocks = vec![Arc::new(BlockData { block_number: 100, tables })];

        let tmp = TempDir::new().unwrap();
        let chunk_dir = tmp.path().join("100-100");
        flush_to_parquet(&blocks, &chunk_dir, &meta).unwrap();

        // Verify all tables written
        let reader = ParquetChunkReader::open(&chunk_dir).unwrap();
        for table_name in parquet.table_names() {
            assert!(reader.has_table(&table_name),
                "missing table {} in flushed parquet", table_name);
        }
    }

    #[test]
    fn test_chunk_dir_name() {
        assert_eq!(chunk_dir_name(100, 200), "100-200");
        assert_eq!(parse_chunk_dir_name("100-200"), Some((100, 200)));
        assert_eq!(parse_chunk_dir_name("invalid"), None);
    }
}
