//! RocksDB-backed ChunkReader implementation.
//!
//! Data model: one column family per table.
//! Key: block_number as u32 BE (4 bytes)
//! Value: Arrow IPC stream bytes (all rows for that block in that table)
//!
//! On scan: iterate over relevant key range, decode IPC batches, apply filters.

use crate::scan::kv_scan::{apply_scan_filters, block_key, decode_batches, encode_batch};
use crate::scan::{ChunkReader, ScanRequest};
use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options};
use std::collections::HashMap;
use std::path::Path;

pub struct RocksDbChunkReader {
    db: DBWithThreadMode<MultiThreaded>,
    table_names: Vec<String>,
    schemas: HashMap<String, SchemaRef>,
}

impl RocksDbChunkReader {
    /// Open an existing RocksDB with known column families (no compression).
    pub fn open(path: &Path, table_names: &[&str]) -> Result<Self> {
        Self::open_with_compression(path, table_names, false)
    }

    /// Open with LZ4 compression enabled on all column families.
    pub fn open_compressed(path: &Path, table_names: &[&str]) -> Result<Self> {
        Self::open_with_compression(path, table_names, true)
    }

    fn open_with_compression(
        path: &Path,
        table_names: &[&str],
        compressed: bool,
    ) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs: Vec<ColumnFamilyDescriptor> = table_names
            .iter()
            .map(|name| {
                let mut cf_opts = Options::default();
                if compressed {
                    cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                }
                ColumnFamilyDescriptor::new(*name, cf_opts)
            })
            .collect();

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&opts, path, cfs)
            .context("opening RocksDB")?;

        Ok(Self {
            db,
            table_names: table_names.iter().map(|s| s.to_string()).collect(),
            schemas: HashMap::new(),
        })
    }

    /// Create a new RocksDB with column families for each table.
    pub fn create(path: &Path, table_names: &[&str]) -> Result<Self> {
        Self::open(path, table_names)
    }

    /// Create a new RocksDB with LZ4 compression.
    pub fn create_compressed(path: &Path, table_names: &[&str]) -> Result<Self> {
        Self::open_compressed(path, table_names)
    }

    /// Register schema for a table.
    pub fn set_schema(&mut self, table: &str, schema: SchemaRef) {
        self.schemas.insert(table.to_string(), schema);
    }

    /// Write a RecordBatch for a specific block in a table.
    pub fn put_batch(
        &self,
        table: &str,
        block_number: u32,
        batch: &RecordBatch,
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(table)
            .context("column family not found")?;
        let key = block_key(block_number);
        let value = encode_batch(batch)?;
        self.db.put_cf(&cf, &key, &value)?;
        Ok(())
    }

    /// Write multiple blocks in a single WriteBatch (much faster).
    pub fn put_batches(
        &self,
        table: &str,
        blocks: &[(u32, &RecordBatch)],
    ) -> Result<()> {
        let cf = self
            .db
            .cf_handle(table)
            .context("column family not found")?;
        let mut wb = rocksdb::WriteBatch::default();
        for &(block_number, batch) in blocks {
            let key = block_key(block_number);
            let value = encode_batch(batch)?;
            wb.put_cf(&cf, &key, &value);
        }
        self.db.write(wb)?;
        Ok(())
    }

    /// Delete all entries for blocks in [from_block, to_block].
    pub fn delete_range(&self, table: &str, from_block: u32, to_block: u32) -> Result<()> {
        let cf = self
            .db
            .cf_handle(table)
            .context("column family not found")?;
        let start = block_key(from_block);
        let end = block_key(to_block.saturating_add(1));
        self.db.delete_range_cf(&cf, &start, &end)?;
        self.db.compact_range_cf(&cf, Some(&start), Some(&end));
        Ok(())
    }
}

impl ChunkReader for RocksDbChunkReader {
    fn scan(&self, table: &str, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
        let cf = match self.db.cf_handle(table) {
            Some(cf) => cf,
            None => return Ok(Vec::new()),
        };
        let schema = match self.schemas.get(table) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let mut all_batches = Vec::new();

        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for result in iter {
            let (_key, value) = result?;
            let batches = decode_batches(&value)?;
            all_batches.extend(batches);
        }

        apply_scan_filters(all_batches, request, schema)
    }

    fn has_table(&self, table: &str) -> bool {
        self.db.cf_handle(table).is_some()
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.schemas.get(table).cloned()
    }
}
