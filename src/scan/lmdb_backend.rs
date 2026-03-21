//! LMDB-backed ChunkReader implementation.
//!
//! Data model: one LMDB sub-database per table.
//! Key: block_number as u32 BE (4 bytes)
//! Value: Arrow IPC stream bytes, optionally zstd-compressed
//!
//! On scan: iterate over relevant key range, decode IPC batches, apply filters.

use crate::scan::kv_scan::{
    apply_scan_filters, block_key, decode_batches, decode_batches_zstd, encode_batch,
    encode_batch_zstd,
};
use crate::scan::{ChunkReader, ScanRequest};
use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions};
use std::collections::HashMap;
use std::path::Path;

type LmdbDb = Database<Bytes, Bytes>;

pub struct LmdbChunkReader {
    env: Env,
    tables: HashMap<String, LmdbDb>,
    schemas: HashMap<String, SchemaRef>,
    compressed: bool,
}

impl LmdbChunkReader {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_options(path, false)
    }

    pub fn open_compressed(path: &Path) -> Result<Self> {
        Self::open_with_options(path, true)
    }

    fn open_with_options(path: &Path, compressed: bool) -> Result<Self> {
        let env = unsafe {
            EnvOpenOptions::new()
                .max_dbs(32)
                .map_size(10 * 1024 * 1024 * 1024) // 10 GB virtual
                .max_readers(128)
                .open(path)
                .context("opening LMDB environment")?
        };

        Ok(Self {
            env,
            tables: HashMap::new(),
            schemas: HashMap::new(),
            compressed,
        })
    }

    /// Register a table (sub-database) with its schema.
    pub fn create_table(&mut self, name: &str, schema: SchemaRef) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        let db: LmdbDb = self
            .env
            .create_database(&mut wtxn, Some(name))
            .context("creating LMDB sub-database")?;
        wtxn.commit()?;
        self.tables.insert(name.to_string(), db);
        self.schemas.insert(name.to_string(), schema);
        Ok(())
    }

    /// Open an existing table by name.
    pub fn open_table(&mut self, name: &str) -> Result<bool> {
        let rtxn = self.env.read_txn()?;
        match self.env.open_database::<Bytes, Bytes>(&rtxn, Some(name))? {
            Some(db) => {
                self.tables.insert(name.to_string(), db);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Write a RecordBatch for a specific block in a table.
    pub fn put_batch(&self, table: &str, block_number: u32, batch: &RecordBatch) -> Result<()> {
        let db = self
            .tables
            .get(table)
            .context("table not found in LMDB")?;
        let key = block_key(block_number);
        let value = if self.compressed {
            encode_batch_zstd(batch)?
        } else {
            encode_batch(batch)?
        };

        let mut wtxn = self.env.write_txn()?;
        db.put(&mut wtxn, &key, &value)?;
        wtxn.commit()?;
        Ok(())
    }

    /// Write multiple blocks in a single transaction (much faster).
    pub fn put_batches(&self, table: &str, blocks: &[(u32, &RecordBatch)]) -> Result<()> {
        let db = self
            .tables
            .get(table)
            .context("table not found in LMDB")?;

        let mut wtxn = self.env.write_txn()?;
        for &(block_number, batch) in blocks {
            let key = block_key(block_number);
            let value = if self.compressed {
                encode_batch_zstd(batch)?
            } else {
                encode_batch(batch)?
            };
            db.put(&mut wtxn, &key, &value)?;
        }
        wtxn.commit()?;
        Ok(())
    }

    /// Delete all entries for blocks in [from_block, to_block].
    pub fn delete_range(&self, table: &str, from_block: u32, to_block: u32) -> Result<usize> {
        let db = self
            .tables
            .get(table)
            .context("table not found in LMDB")?;
        let mut wtxn = self.env.write_txn()?;
        let mut count = 0;
        for bn in from_block..=to_block {
            let key = block_key(bn);
            if db.delete(&mut wtxn, &key)? {
                count += 1;
            }
        }
        wtxn.commit()?;
        Ok(count)
    }

    pub fn env(&self) -> &Env {
        &self.env
    }
}

impl ChunkReader for LmdbChunkReader {
    fn scan(&self, table: &str, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
        let db = match self.tables.get(table) {
            Some(db) => db,
            None => return Ok(Vec::new()),
        };
        let schema = match self.schemas.get(table) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let rtxn = self.env.read_txn()?;
        let mut all_batches = Vec::new();

        let iter = db.iter(&rtxn)?;
        for result in iter {
            let (_key, value) = result?;
            let batches = if self.compressed {
                decode_batches_zstd(value)?
            } else {
                decode_batches(value)?
            };
            all_batches.extend(batches);
        }

        apply_scan_filters(all_batches, request, schema)
    }

    fn has_table(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.schemas.get(table).cloned()
    }
}
