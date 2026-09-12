use super::scanner;
use super::ChunkReader;
use super::ScanRequest;
use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use memmap2::Mmap;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// A single parquet table file with cached metadata and memory-mapped data.
pub struct ParquetTable {
    path: PathBuf,
    data: Bytes,
    metadata: Arc<ParquetMetaData>,
    schema: SchemaRef,
    arrow_metadata: ArrowReaderMetadata,
    statistics_leaves: HashMap<String, usize>,
}

/// Statistics for a single column within a row group.
#[derive(Debug)]
pub struct ColumnStats {
    pub min: Option<StatValue>,
    pub max: Option<StatValue>,
    pub null_count: Option<i64>,
    pub distinct_count: Option<u64>,
}

/// A typed statistic value extracted from parquet metadata.
#[derive(Debug, Clone)]
pub enum StatValue {
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    ByteArray(Vec<u8>),
    Boolean(bool),
    FixedLenByteArray(Vec<u8>),
}

/// A `ChunkReader` backed by a directory of parquet files, one per table.
///
/// A table is opened the first time a query touches it and kept for the
/// reader's lifetime. Nothing else in the directory is read, so a file the
/// query does not name — a stray temporary, a truncated table of another
/// kind — cannot fail it, and a header-only query pays for one footer.
pub struct ParquetChunkReader {
    chunk_dir: PathBuf,
    tables: RwLock<HashMap<String, Arc<ParquetTable>>>,
}

impl ParquetChunkReader {
    /// Create a reader for a chunk directory. No table is opened yet.
    pub fn open(chunk_dir: &Path) -> Result<Self> {
        anyhow::ensure!(
            chunk_dir.is_dir(),
            "chunk directory {} does not exist",
            chunk_dir.display()
        );
        Ok(Self {
            chunk_dir: chunk_dir.to_path_buf(),
            tables: RwLock::new(HashMap::new()),
        })
    }

    pub fn path(&self) -> &Path {
        &self.chunk_dir
    }

    fn table_path(&self, table: &str) -> PathBuf {
        self.chunk_dir.join(format!("{table}.parquet"))
    }

    /// The table, opened on first use. `None` when the chunk has no file for
    /// it; an error when the file is there but is not a parquet file the
    /// engine can read.
    pub fn table(&self, table: &str) -> Result<Option<Arc<ParquetTable>>> {
        if let Some(opened) = self.tables.read().unwrap().get(table) {
            return Ok(Some(opened.clone()));
        }

        let path = self.table_path(table);
        if !path.is_file() {
            return Ok(None);
        }

        let opened = match ParquetTable::open(&path) {
            Ok(opened) => Arc::new(opened),
            Err(e) => crate::engine_bail!(
                crate::error::ErrorKind::MalformedChunkData,
                "table '{}' cannot be opened: {:#}",
                table,
                e
            ),
        };

        // Two scans racing on the same table both open it; either copy serves.
        let mut tables = self.tables.write().unwrap();
        let entry = tables.entry(table.to_string()).or_insert(opened);
        Ok(Some(entry.clone()))
    }
}

impl ChunkReader for ParquetChunkReader {
    fn supports_row_positions(&self) -> bool {
        true
    }

    fn scan(&self, table: &str, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
        let Some(parquet_table) = self.table(table)? else {
            crate::engine_bail!(
                crate::error::ErrorKind::TableNotFound,
                "table '{}' is not found in the chunk",
                table
            );
        };
        scanner::scan(&parquet_table, request)
    }

    fn next_block_range_end(
        &self,
        table: &str,
        block_column: &str,
        from_block: u64,
    ) -> Option<u64> {
        let table = self.table(table).ok()??;
        scanner::next_block_range_end(&table, block_column, from_block)
    }

    fn has_table(&self, table: &str) -> bool {
        self.table_path(table).is_file()
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        let table = self.table(table).ok()??;
        Some(table.schema().clone())
    }
}

impl ParquetTable {
    /// Open a parquet file: memory-map it and cache metadata.
    pub fn open(path: &Path) -> Result<Self> {
        let file =
            File::open(path).with_context(|| format!("opening parquet file {}", path.display()))?;

        // Memory-map the file — pages are faulted in lazily by the OS on demand.
        // SAFETY: The file is read-only and we hold no mutable references.
        let mmap = unsafe { Mmap::map(&file) }
            .with_context(|| format!("memory-mapping parquet file {}", path.display()))?;
        let data = Bytes::from_owner(mmap);

        let arrow_metadata = ArrowReaderMetadata::load(&data, Default::default())
            .with_context(|| format!("reading parquet metadata {}", path.display()))?;
        let metadata = arrow_metadata.metadata().clone();
        let schema = arrow_metadata.schema().clone();

        let statistics_leaves = statistics_leaves(&metadata);

        Ok(Self {
            path: path.to_path_buf(),
            data,
            metadata,
            schema,
            arrow_metadata,
            statistics_leaves,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Logical table name (parquet file stem, e.g. `blocks`).
    pub fn name(&self) -> &str {
        self.path.file_stem().and_then(|s| s.to_str()).unwrap_or("")
    }

    /// Arrow schema for this table.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Parquet metadata.
    pub fn metadata(&self) -> &ParquetMetaData {
        &self.metadata
    }

    /// Pre-built Arrow reader metadata (avoids re-reading parquet footer).
    pub fn arrow_metadata(&self) -> &ArrowReaderMetadata {
        &self.arrow_metadata
    }

    /// Memory-mapped file data (O(1) clone via Bytes refcount).
    pub fn data(&self) -> Bytes {
        self.data.clone()
    }

    /// Number of row groups in this file.
    pub fn num_row_groups(&self) -> usize {
        self.metadata.num_row_groups()
    }

    /// Total number of rows across all row groups.
    pub fn num_rows(&self) -> i64 {
        self.metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .sum()
    }

    /// Get row group metadata by index.
    pub fn row_group(&self, index: usize) -> &RowGroupMetaData {
        self.metadata.row_group(index)
    }

    /// The column's position in the *Arrow* schema, which is what a projection
    /// is built from. It is not where the column's statistics live — see
    /// [`statistics_leaves`].
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.schema.index_of(name).ok()
    }

    /// What one row group's statistics say about one column, when the file
    /// states something this can act on.
    pub fn column_stats(&self, row_group: usize, column_name: &str) -> Option<ColumnStats> {
        let leaf = *self.statistics_leaves.get(column_name)?;
        let stats = self.row_group(row_group).column(leaf).statistics()?;
        Some(convert_stats(stats))
    }

    /// Read selected columns from specified row groups, returning RecordBatches.
    ///
    /// - `columns`: column names to read (projection)
    /// - `row_groups`: which row groups to read (None = all)
    /// - `batch_size`: max rows per RecordBatch
    pub fn read(
        &self,
        columns: &[&str],
        row_groups: Option<&[usize]>,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>> {
        let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            self.data.clone(),
            self.arrow_metadata.clone(),
        )
        .with_batch_size(batch_size);

        // Column projection
        if !columns.is_empty() {
            let parquet_schema = self.metadata.file_metadata().schema_descr();
            let indices: Vec<usize> = columns
                .iter()
                .filter_map(|name| {
                    // Find the column index in the parquet schema (leaf columns)
                    self.schema.index_of(name).ok()
                })
                .collect();

            let mask = ProjectionMask::roots(parquet_schema, indices);
            builder = builder.with_projection(mask);
        }

        // Row group selection
        if let Some(rg_indices) = row_groups {
            let mut selectors = Vec::new();
            for (i, rg) in self.metadata.row_groups().iter().enumerate() {
                let num_rows = rg.num_rows() as usize;
                if rg_indices.contains(&i) {
                    selectors.push(RowSelector::select(num_rows));
                } else {
                    selectors.push(RowSelector::skip(num_rows));
                }
            }
            builder = builder.with_row_selection(RowSelection::from(selectors));
        }

        let reader = builder.build()?;

        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result.context("reading record batch")?);
        }

        Ok(batches)
    }
}

/// Column name → the parquet column chunk its statistics live in.
///
/// Arrow counts a file's columns in top-level fields and parquet counts them in
/// leaves, and the two agree only while every field is a primitive. One
/// `List<Struct<…>>` — `access_list` on an EVM transaction, `address_table_lookups`
/// on a Solana one — and every field after it sits further along in the file than
/// in the schema, so an index taken from the schema reads another column's
/// minimum and maximum. On a real Solana chunk that put `fee_payer`, the table's
/// leading sort key, on the bounds of `loaded_addresses.readonly`: a filter then
/// skips the row group holding its own rows, and a dropped match looks exactly
/// like a row that was never there.
///
/// A nested field has no one statistic to be read as a bound, so it is left out
/// and never pruned on — a reader that cannot interpret a statistic declines
/// rather than guesses (INV-P16). Nothing is lost by it: the predicates that
/// apply to a list column decline to prune anyway.
fn statistics_leaves(metadata: &ParquetMetaData) -> HashMap<String, usize> {
    let columns = metadata.file_metadata().schema_descr();

    (0..columns.num_columns())
        .filter_map(|leaf| {
            let column = columns.column(leaf);

            match column.path().parts() {
                [name] => Some((name.clone(), leaf)),
                _ => None,
            }
        })
        .collect()
}

fn convert_stats(stats: &Statistics) -> ColumnStats {
    let (min, max) = match stats {
        Statistics::Boolean(s) => (
            s.min_opt().map(|v| StatValue::Boolean(*v)),
            s.max_opt().map(|v| StatValue::Boolean(*v)),
        ),
        Statistics::Int32(s) => (
            s.min_opt().map(|v| StatValue::Int32(*v)),
            s.max_opt().map(|v| StatValue::Int32(*v)),
        ),
        Statistics::Int64(s) => (
            s.min_opt().map(|v| StatValue::Int64(*v)),
            s.max_opt().map(|v| StatValue::Int64(*v)),
        ),
        Statistics::Float(s) => (
            s.min_opt().map(|v| StatValue::Float(*v)),
            s.max_opt().map(|v| StatValue::Float(*v)),
        ),
        Statistics::Double(s) => (
            s.min_opt().map(|v| StatValue::Double(*v)),
            s.max_opt().map(|v| StatValue::Double(*v)),
        ),
        Statistics::ByteArray(s) => (
            s.min_opt().map(|v| StatValue::ByteArray(v.data().to_vec())),
            s.max_opt().map(|v| StatValue::ByteArray(v.data().to_vec())),
        ),
        Statistics::FixedLenByteArray(s) => (
            s.min_opt()
                .map(|v| StatValue::FixedLenByteArray(v.data().to_vec())),
            s.max_opt()
                .map(|v| StatValue::FixedLenByteArray(v.data().to_vec())),
        ),
        // Int96 is deprecated, treat as no stats
        _ => (None, None),
    };

    ColumnStats {
        min,
        max,
        null_count: stats.null_count_opt().map(|v| v as i64),
        distinct_count: stats.distinct_count_opt(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn solana_chunk_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk")
    }

    fn evm_chunk_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("data/evm/chunk")
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_open_solana_chunk() {
        if !crate::testing::chunks_present() {
            return;
        }

        let chunk = ParquetChunkReader::open(&solana_chunk_path()).unwrap();
        for name in [
            "balances",
            "blocks",
            "instructions",
            "logs",
            "rewards",
            "token_balances",
            "transactions",
        ] {
            assert!(chunk.has_table(name), "{name} is missing");
            assert!(chunk.table(name).unwrap().is_some(), "{name} does not open");
        }
        assert!(!chunk.has_table("receipts"));
        assert!(chunk.table("receipts").unwrap().is_none());
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_table_metadata() {
        if !crate::testing::chunks_present() {
            return;
        }

        let chunk = ParquetChunkReader::open(&solana_chunk_path()).unwrap();
        let instructions = chunk.table("instructions").unwrap().unwrap();

        assert!(instructions.num_rows() > 0);
        assert!(instructions.num_row_groups() > 0);
        assert!(instructions.column_index("block_number").is_some());
        assert!(instructions.column_index("program_id").is_some());
        assert!(instructions.column_index("nonexistent").is_none());
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_column_stats() {
        if !crate::testing::chunks_present() {
            return;
        }

        let chunk = ParquetChunkReader::open(&solana_chunk_path()).unwrap();
        let blocks = chunk.table("blocks").unwrap().unwrap();

        // blocks.number should have stats
        let stats = blocks.column_stats(0, "number");
        assert!(stats.is_some(), "blocks.number should have statistics");
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_read_projected_columns() {
        if !crate::testing::chunks_present() {
            return;
        }

        let chunk = ParquetChunkReader::open(&solana_chunk_path()).unwrap();
        let blocks = chunk.table("blocks").unwrap().unwrap();

        let batches = blocks.read(&["number", "hash"], None, 1000).unwrap();
        assert!(!batches.is_empty());

        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "number");
        assert_eq!(batch.schema().field(1).name(), "hash");
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_read_specific_row_groups() {
        if !crate::testing::chunks_present() {
            return;
        }

        let chunk = ParquetChunkReader::open(&solana_chunk_path()).unwrap();
        let instructions = chunk.table("instructions").unwrap().unwrap();

        // Read only the first row group
        let batches = instructions
            .read(&["block_number"], Some(&[0]), 50000)
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let rg0_rows = instructions.row_group(0).num_rows() as usize;
        assert_eq!(total_rows, rg0_rows);
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_read_evm_logs() {
        if !crate::testing::chunks_present() {
            return;
        }

        let chunk = ParquetChunkReader::open(&evm_chunk_path()).unwrap();
        let logs = chunk.table("logs").unwrap().unwrap();

        let batches = logs
            .read(&["block_number", "address", "topic0"], None, 10000)
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, logs.num_rows() as usize);
    }
}
