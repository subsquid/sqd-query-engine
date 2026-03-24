//! DatasetStore: orchestrates memory buffer + crash log + parquet chunks per dataset.
//!
//! This is the main entry point for the hot storage layer.
//! Implements ChunkReader by composing MemoryChunkReader + ParquetChunkReaders
//! via CompositeChunkReader with block range routing.

use crate::metadata::DatasetDescription;
use crate::scan::composite_reader::CompositeChunkReader;
use crate::scan::wal::{self, WalWriter};
use crate::scan::memory_backend::{BlockData, MemoryChunkReader, MemorySnapshot};
use crate::scan::parquet_writer::{self, chunk_dir_name, parse_chunk_dir_name};
use crate::scan::{ChunkReader, ParquetChunkReader, ScanRequest};
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// A parquet chunk on disk with its block range.
struct ChunkEntry {
    first_block: u64,
    last_block: u64,
    reader: Arc<ParquetChunkReader>,
}

/// An owned point-in-time snapshot of the dataset store.
/// Can be used independently without holding any lock on the store.
/// Contains Arc-cloned parquet readers and a frozen memory snapshot.
pub struct DatasetSnapshot {
    memory: MemorySnapshot,
    compacting: Option<MemorySnapshot>,
    chunks: Vec<(u64, u64, Arc<ParquetChunkReader>)>,
}

impl DatasetSnapshot {
    /// Build a composite ChunkReader for querying across all tiers.
    pub fn reader(&self) -> CompositeChunkReader<'_> {
        let mut composite = CompositeChunkReader::new();
        for (first, last, reader) in &self.chunks {
            composite.add(reader.as_ref(), Some(*first), Some(*last));
        }
        if let Some(ref compacting) = self.compacting {
            composite.add(compacting, compacting.first_block(), compacting.head());
        }
        composite.add(&self.memory, self.memory.first_block(), self.memory.head());
        composite
    }
}

/// Summary stats for a dataset store (default API response).
#[derive(Debug, Serialize)]
pub struct DatasetStats {
    pub first_block: Option<String>,
    pub last_block: Option<String>,
    pub finalized_block: Option<String>,
    pub disk_size: String,
    pub memory: MemorySummary,
    pub parquet_files: ParquetSummary,
}

/// Memory buffer summary.
#[derive(Debug, Serialize)]
pub struct MemorySummary {
    pub first_block: Option<String>,
    pub last_block: Option<String>,
    pub blocks: String,
    pub size: String,
}

/// Parquet chunks summary.
#[derive(Debug, Serialize)]
pub struct ParquetSummary {
    pub first_block: Option<String>,
    pub last_block: Option<String>,
    pub blocks: String,
    pub chunks: String,
    pub avg_compressed_size: String,
}

/// Detailed stats for a dataset store (per-chunk breakdown).
#[derive(Debug, Serialize)]
pub struct DatasetStatsDetailed {
    pub first_block: Option<String>,
    pub last_block: Option<String>,
    pub finalized_block: Option<String>,
    pub disk_size: String,
    pub memory: MemorySummary,
    pub chunks: Vec<ChunkStats>,
}

/// Stats for a single parquet chunk on disk.
#[derive(Debug, Serialize)]
pub struct ChunkStats {
    pub first_block: u64,
    pub last_block: u64,
    pub tables: HashMap<String, TableStats>,
}

/// Stats for a single table within a parquet chunk.
#[derive(Debug, Serialize)]
pub struct TableStats {
    pub rows: u64,
    pub row_groups: usize,
    pub compressed_size: String,
}

fn fmt_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn fmt_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Hot storage for a single dataset.
///
/// Three tiers:
/// - In-memory buffer (latest blocks, queryable immediately)
/// - Parquet chunks on disk (finalized blocks, query-optimized)
/// - IPC crash log (durability for in-memory buffer, never queried)
pub struct DatasetStore {
    dir: PathBuf,
    buffer: MemoryChunkReader,
    wal: WalWriter,
    chunks: Vec<ChunkEntry>,
    /// Memory budget in bytes. When exceeded, oldest blocks spill to parquet.
    memory_warning: usize,
    /// Block number up to which data has been compacted to parquet.
    compacted_up_to: Option<u64>,
    /// Finalized block reference (persisted to disk for crash recovery).
    finalized_head: Option<u64>,
    /// Blocks currently being compacted in background.
    /// Included in reader/snapshot so queries still see this data.
    compacting: Option<MemorySnapshot>,
}

impl DatasetStore {
    /// Default crash log flush interval.
    const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(10);

    /// Open or create a dataset store at the given directory.
    pub fn open(dir: &Path, memory_warning: usize) -> Result<Self> {
        Self::open_with_flush_interval(dir, memory_warning, Self::DEFAULT_FLUSH_INTERVAL)
    }

    /// Open with a custom crash log flush interval.
    pub fn open_with_flush_interval(
        dir: &Path,
        memory_warning: usize,
        flush_interval: Duration,
    ) -> Result<Self> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating dataset dir {}", dir.display()))?;

        let chunks_dir = dir.join("chunks");
        std::fs::create_dir_all(&chunks_dir).context("creating chunks dir")?;

        let wal = WalWriter::open(&dir.join("wal.bin"), flush_interval)?;

        let mut store = Self {
            dir: dir.to_path_buf(),
            buffer: MemoryChunkReader::new(),
            wal,
            chunks: Vec::new(),
            memory_warning,
            compacted_up_to: None,
            finalized_head: None,
            compacting: None,
        };

        // Load existing parquet chunks
        store.load_chunks()?;

        // finalized_head recovered from WAL during recover()

        // Recover from crash log
        store.recover()?;

        Ok(store)
    }

    /// Push a new block. Queryable immediately.
    /// Crash log writes are buffered and flushed periodically (default: every 10s).
    pub fn push_block(&mut self, block: BlockData) -> Result<()> {
        self.wal.append(block.block_number, &block.tables)?;
        self.buffer.push(block);
        Ok(())
    }

    /// Handle a reorg: remove blocks >= fork_point.
    pub fn truncate(&mut self, fork_point: u64) -> Result<()> {
        self.buffer.truncate(fork_point);
        self.wal.truncate(fork_point)?;
        // Parquet chunks: delete any that start >= fork_point
        self.chunks.retain(|c| {
            if c.first_block >= fork_point {
                let chunk_dir = self
                    .dir
                    .join("chunks")
                    .join(chunk_dir_name(c.first_block, c.last_block));
                let _ = std::fs::remove_dir_all(&chunk_dir);
                false
            } else {
                true
            }
        });
        Ok(())
    }

    /// Compact finalized blocks to parquet (blocking, single-phase).
    /// Use `begin_compact` + `finish_compact` for non-blocking compaction.
    pub fn compact(&mut self, finalized_block: u64, metadata: &DatasetDescription) -> Result<()> {
        let drained = match self.begin_compact(finalized_block) {
            Some(d) => d,
            None => return Ok(()),
        };

        let first = drained.first().unwrap().block_number;
        let last = drained.last().unwrap().block_number;
        let chunk_dir = self.dir.join("chunks").join(chunk_dir_name(first, last));

        parquet_writer::flush_to_parquet(&drained, &chunk_dir, metadata)?;

        let reader = Arc::new(
            ParquetChunkReader::open(&chunk_dir)
                .with_context(|| format!("opening new chunk {}", chunk_dir.display()))?,
        );

        self.finish_compact(first, last, reader)?;
        Ok(())
    }

    /// Phase 1: drain finalized blocks from buffer into a compacting snapshot.
    /// Returns the drained blocks, or None if nothing to compact.
    /// The drained blocks remain visible to queries via the `compacting` snapshot.
    /// Call `finish_compact` after writing parquet to register the chunk.
    pub fn begin_compact(&mut self, finalized_block: u64) -> Option<Vec<Arc<BlockData>>> {
        if self.compacting.is_some() {
            return None; // compaction already in progress
        }

        let drained = self.buffer.drain_up_to(finalized_block);
        if drained.is_empty() {
            return None;
        }

        // Build schemas from drained blocks
        let mut schemas = HashMap::new();
        for block in &drained {
            for (table_name, batch) in &block.tables {
                schemas
                    .entry(table_name.clone())
                    .or_insert_with(|| batch.schema());
            }
        }

        self.compacting = Some(MemorySnapshot::from_blocks(drained.clone(), schemas));
        Some(drained)
    }

    /// Phase 3: register the new parquet chunk and clean up.
    /// Call after writing parquet in background.
    pub fn finish_compact(
        &mut self,
        first_block: u64,
        last_block: u64,
        reader: Arc<ParquetChunkReader>,
    ) -> Result<()> {
        self.chunks.push(ChunkEntry {
            first_block,
            last_block,
            reader,
        });
        self.chunks.sort_by_key(|c| c.first_block);

        self.compacted_up_to = Some(last_block);
        self.compacting = None;

        // Rebuild WAL with remaining buffer blocks
        self.wal.clear()?;
        for block in self.buffer.iter_blocks() {
            self.wal.append(block.block_number, &block.tables)?;
        }
        // Re-persist finalized_head
        if let Some(fh) = self.finalized_head {
            self.wal.set_finalized_head(fh)?;
        }

        Ok(())
    }

    /// Spillover: if memory exceeds cap, flush oldest blocks to parquet.
    /// Unlike compact(), this writes unfinalized blocks (deletable on reorg).
    /// Check if memory usage exceeds the cap. Returns true if over budget.
    pub fn is_over_memory_warning(&self) -> bool {
        self.buffer.memory_usage() > self.memory_warning
    }

    /// Evict chunks with last_block <= up_to (cold storage confirmed).
    pub fn evict(&mut self, up_to: u64) {
        self.chunks.retain(|c| {
            if c.last_block <= up_to {
                let chunk_dir = self
                    .dir
                    .join("chunks")
                    .join(chunk_dir_name(c.first_block, c.last_block));
                let _ = std::fs::remove_dir_all(&chunk_dir);
                false
            } else {
                true
            }
        });
    }

    /// Set finalized head. In-memory immediate, persisted on next crash log flush.
    /// Monotonic: value can only increase.
    pub fn set_finalized_head(&mut self, block_number: u64) -> Result<()> {
        self.wal.set_finalized_head(block_number)?;
        self.finalized_head = self.wal.finalized_head();
        Ok(())
    }

    /// Get finalized head block number.
    pub fn finalized_head(&self) -> Option<u64> {
        self.finalized_head
    }

    /// Current head block number.
    pub fn head(&self) -> Option<u64> {
        self.buffer
            .head()
            .or_else(|| self.chunks.last().map(|c| c.last_block))
    }

    /// Total disk usage (parquet chunks + crash log).
    pub fn disk_usage(&self) -> u64 {
        let wal_size = self.dir.join("wal.bin")
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);
        dir_size(&self.dir.join("chunks")) + wal_size
    }

    /// In-memory buffer usage.
    pub fn memory_usage(&self) -> usize {
        self.buffer.memory_usage()
    }

    /// Count how many blocks in memory are at or below the finalized block.
    pub fn finalized_blocks_in_memory(&self, finalized_block: u64) -> usize {
        self.buffer
            .iter_blocks()
            .filter(|b| b.block_number <= finalized_block)
            .count()
    }

    /// Estimate byte size of finalized blocks in memory.
    pub fn finalized_bytes_in_memory(&self, finalized_block: u64) -> usize {
        self.buffer
            .iter_blocks()
            .filter(|b| b.block_number <= finalized_block)
            .map(|b| {
                b.tables
                    .values()
                    .map(|batch| batch.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum()
    }

    /// Path to the chunks directory.
    pub fn chunks_dir(&self) -> PathBuf {
        self.dir.join("chunks")
    }

    /// Iterate over blocks in memory buffer.
    pub fn iter_blocks(&self) -> impl DoubleEndedIterator<Item = &std::sync::Arc<BlockData>> {
        self.buffer.iter_blocks()
    }

    /// First block number across all tiers (memory + chunks).
    pub fn first_block(&self) -> Option<u64> {
        self.chunks
            .first()
            .map(|c| c.first_block)
            .or_else(|| self.buffer.first_block())
    }

    /// Number of memory blocks.
    pub fn memory_block_count(&self) -> usize {
        self.buffer.len()
    }

    /// Number of parquet chunks.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Build a composite ChunkReader for querying across all tiers.
    pub fn reader(&self) -> CompositeChunkReader<'_> {
        let mut composite = CompositeChunkReader::new();

        // Add parquet chunks (oldest first)
        for chunk in &self.chunks {
            composite.add(
                chunk.reader.as_ref(),
                Some(chunk.first_block),
                Some(chunk.last_block),
            );
        }

        // Add blocks being compacted (visible until parquet chunk is registered)
        if let Some(ref compacting) = self.compacting {
            composite.add(compacting, compacting.first_block(), compacting.head());
        }

        // Add memory buffer (newest blocks)
        composite.add(&self.buffer, self.buffer.first_block(), self.buffer.head());

        composite
    }

    /// Create an owned snapshot of the current state.
    /// The snapshot can be used independently (e.g., while the store is being mutated).
    /// Cheap: Arc clones for parquet readers, Vec<Arc<BlockData>> clone for memory.
    pub fn snapshot(&self) -> DatasetSnapshot {
        DatasetSnapshot {
            memory: self.buffer.snapshot(),
            compacting: self.compacting.clone(),
            chunks: self
                .chunks
                .iter()
                .map(|c| (c.first_block, c.last_block, c.reader.clone()))
                .collect(),
        }
    }

    /// Summary stats for the dataset.
    pub fn stats(&self) -> DatasetStats {
        let memory = self.memory_summary();
        let (pq_summary, _) = self.parquet_summary();
        DatasetStats {
            first_block: self.first_block().map(fmt_number),
            last_block: self.head().map(fmt_number),
            finalized_block: self.finalized_head.map(fmt_number),
            disk_size: fmt_bytes(self.disk_usage()),
            memory,
            parquet_files: pq_summary,
        }
    }

    /// Detailed stats with per-chunk breakdown.
    pub fn stats_detailed(&self) -> DatasetStatsDetailed {
        let memory = self.memory_summary();
        let (_, chunks) = self.parquet_summary();
        DatasetStatsDetailed {
            first_block: self.first_block().map(fmt_number),
            last_block: self.head().map(fmt_number),
            finalized_block: self.finalized_head.map(fmt_number),
            disk_size: fmt_bytes(self.disk_usage()),
            memory,
            chunks,
        }
    }

    fn memory_summary(&self) -> MemorySummary {
        MemorySummary {
            first_block: self.buffer.first_block().map(fmt_number),
            last_block: self.buffer.head().map(fmt_number),
            blocks: fmt_number(self.buffer.len() as u64),
            size: fmt_bytes(self.buffer.memory_usage() as u64),
        }
    }

    fn parquet_summary(&self) -> (ParquetSummary, Vec<ChunkStats>) {
        let mut total_compressed: u64 = 0;
        let mut total_blocks: u64 = 0;

        let chunks: Vec<ChunkStats> = self.chunks.iter().map(|c| {
            total_blocks += c.last_block - c.first_block + 1;

            let mut tables = HashMap::new();
            for table_name in c.reader.table_names() {
                if let Some(table) = c.reader.table(&table_name) {
                    let compressed: u64 = table.metadata()
                        .row_groups()
                        .iter()
                        .map(|rg| rg.compressed_size() as u64)
                        .sum();
                    total_compressed += compressed;
                    tables.insert(table_name.to_string(), TableStats {
                        rows: table.num_rows() as u64,
                        row_groups: table.num_row_groups(),
                        compressed_size: fmt_bytes(compressed),
                    });
                }
            }
            ChunkStats {
                first_block: c.first_block,
                last_block: c.last_block,
                tables,
            }
        }).collect();

        let n = self.chunks.len();
        let avg = if n > 0 { total_compressed / n as u64 } else { 0 };

        let summary = ParquetSummary {
            first_block: self.chunks.first().map(|c| fmt_number(c.first_block)),
            last_block: self.chunks.last().map(|c| fmt_number(c.last_block)),
            blocks: fmt_number(total_blocks),
            chunks: fmt_number(n as u64),
            avg_compressed_size: fmt_bytes(avg),
        };

        (summary, chunks)
    }

    // --- Private ---

    fn load_chunks(&mut self) -> Result<()> {
        let chunks_dir = self.dir.join("chunks");
        if !chunks_dir.exists() {
            return Ok(());
        }

        let mut entries: Vec<(u64, u64, PathBuf)> = Vec::new();

        for entry in std::fs::read_dir(&chunks_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            // Clean up incomplete temp dirs from interrupted compaction
            if name.starts_with(".tmp-") {
                eprintln!("WARN: removing incomplete temp chunk dir: {}", path.display());
                let _ = std::fs::remove_dir_all(&path);
                continue;
            }

            if let Some((first, last)) = parse_chunk_dir_name(name) {
                entries.push((first, last, path));
            }
        }

        entries.sort_by_key(|(first, _, _)| *first);

        for (first, last, path) in entries {
            let reader = Arc::new(
                ParquetChunkReader::open(&path)
                    .with_context(|| format!("opening chunk {}", path.display()))?,
            );
            self.chunks.push(ChunkEntry {
                first_block: first,
                last_block: last,
                reader,
            });
            self.compacted_up_to = Some(last);
        }

        Ok(())
    }

    fn recover(&mut self) -> Result<()> {
        let wal_path = self.dir.join("wal.bin");
        let recovery = wal::recover_wal(&wal_path)?;

        // Restore finalized_head from WAL
        if let Some(fh) = recovery.finalized_head {
            if self.finalized_head.map_or(true, |c| fh > c) {
                self.finalized_head = Some(fh);
            }
        }

        for (block_number, tables) in recovery.blocks {
            // Skip blocks already in parquet chunks
            if let Some(compacted) = self.compacted_up_to {
                if block_number <= compacted {
                    continue;
                }
            }
            self.buffer.push(BlockData {
                block_number,
                tables,
            });
        }

        Ok(())
    }
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.is_file() {
                total += meta.len();
            } else if meta.is_dir() {
                total += dir_size(&entry.path());
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::load_dataset_description;
    use arrow::array::*;
    use arrow::datatypes::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_batch(block_number: i32, values: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::Int32, false),
            Field::new("address", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![block_number; values.len()])),
                Arc::new(StringArray::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_block(block_number: u64, values: &[&str]) -> BlockData {
        let mut tables = HashMap::new();
        tables.insert("logs".to_string(), make_batch(block_number as i32, values));
        BlockData {
            block_number,
            tables,
        }
    }

    fn evm_meta() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/evm.yaml")).unwrap()
    }

    #[test]
    fn test_push_and_query() {
        let tmp = TempDir::new().unwrap();
        let mut store = DatasetStore::open(tmp.path(), 100 * 1024 * 1024).unwrap();

        store
            .push_block(make_block(100, &["0xaaa", "0xbbb"]))
            .unwrap();
        store.push_block(make_block(101, &["0xccc"])).unwrap();

        assert_eq!(store.head(), Some(101));

        let reader = store.reader();
        let request = ScanRequest::new(vec!["block_number", "address"]);
        let batches = reader.scan("logs", &request).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn test_compact_and_query() {
        let tmp = TempDir::new().unwrap();
        let meta = evm_meta();
        let mut store = DatasetStore::open(tmp.path(), 100 * 1024 * 1024).unwrap();

        store.push_block(make_block(100, &["0xaaa"])).unwrap();
        store.push_block(make_block(101, &["0xbbb"])).unwrap();
        store.push_block(make_block(102, &["0xccc"])).unwrap();

        // Compact blocks 100-101
        store.compact(101, &meta).unwrap();

        assert_eq!(store.chunk_count(), 1);
        assert_eq!(store.buffer.len(), 1); // only block 102 in buffer

        // Query spans both parquet chunk and memory
        let reader = store.reader();
        let request = ScanRequest::new(vec!["block_number", "address"]);
        let batches = reader.scan("logs", &request).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3); // all 3 blocks
    }

    #[test]
    fn test_reorg() {
        let tmp = TempDir::new().unwrap();
        let mut store = DatasetStore::open(tmp.path(), 100 * 1024 * 1024).unwrap();

        store.push_block(make_block(100, &["0xaaa"])).unwrap();
        store.push_block(make_block(101, &["0xbbb"])).unwrap();
        store.push_block(make_block(102, &["0xccc"])).unwrap();

        // Reorg at block 101
        store.truncate(101).unwrap();
        assert_eq!(store.head(), Some(100));

        // Push new chain
        store.push_block(make_block(101, &["0xnew"])).unwrap();
        assert_eq!(store.head(), Some(101));

        let reader = store.reader();
        let request = ScanRequest::new(vec!["block_number", "address"]);
        let batches = reader.scan("logs", &request).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2); // block 100 + new block 101
    }

    #[test]
    fn test_evict() {
        let tmp = TempDir::new().unwrap();
        let meta = evm_meta();
        let mut store = DatasetStore::open(tmp.path(), 100 * 1024 * 1024).unwrap();

        store.push_block(make_block(100, &["0xaaa"])).unwrap();
        store.push_block(make_block(101, &["0xbbb"])).unwrap();
        store.push_block(make_block(200, &["0xccc"])).unwrap();

        store.compact(101, &meta).unwrap();
        assert_eq!(store.chunk_count(), 1);

        // Cold storage says: I have up to block 101
        store.evict(101);
        assert_eq!(store.chunk_count(), 0);

        // Only block 200 remains (in memory)
        let reader = store.reader();
        let request = ScanRequest::new(vec!["block_number", "address"]);
        let batches = reader.scan("logs", &request).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
    }

    #[test]
    fn test_crash_recovery() {
        let tmp = TempDir::new().unwrap();
        let store_path = tmp.path().join("dataset");

        // Write blocks and drop store (simulate crash)
        {
            let mut store = DatasetStore::open(&store_path, 100 * 1024 * 1024).unwrap();
            store.push_block(make_block(100, &["0xaaa"])).unwrap();
            store.push_block(make_block(101, &["0xbbb"])).unwrap();
            // drop without compact — simulates crash
        }

        // Reopen — should recover from crash log
        let store = DatasetStore::open(&store_path, 100 * 1024 * 1024).unwrap();
        assert_eq!(store.head(), Some(101));

        let reader = store.reader();
        let request = ScanRequest::new(vec!["block_number", "address"]);
        let batches = reader.scan("logs", &request).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_deep_reorg_across_parquet() {
        let tmp = TempDir::new().unwrap();
        let meta = evm_meta();
        let mut store = DatasetStore::open(tmp.path(), 100 * 1024 * 1024).unwrap();

        // Write and compact blocks 100-104
        for i in 100..=104 {
            store
                .push_block(make_block(i, &[&format!("0x{:x}", i)]))
                .unwrap();
        }
        store.compact(102, &meta).unwrap(); // chunk 100-102
        assert_eq!(store.chunk_count(), 1);
        assert_eq!(store.buffer.len(), 2); // 103, 104

        // Deep reorg at block 101 — crosses into parquet chunk
        store.truncate(101).unwrap();

        // Buffer: 103, 104 truncated (>= 101), so buffer is empty
        assert_eq!(store.buffer.len(), 0);
        // Chunk 100-102: starts at 100 < fork_point, so it survives in current impl.
        // It still contains stale blocks 101, 102 — known simplification.
        // Deep reorgs across compacted parquet are extremely rare (<1/year).
        // Cold storage will eventually replace this data.
        assert_eq!(store.chunk_count(), 1);

        // Push new chain from fork point
        store.push_block(make_block(101, &["0xnew101"])).unwrap();
        store.push_block(make_block(102, &["0xnew102"])).unwrap();

        // Query works — returns data from both chunk (stale 100-102) and memory (new 101-102)
        let reader = store.reader();
        let request = ScanRequest::new(vec!["block_number", "address"]);
        let batches = reader.scan("logs", &request).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 3 from chunk (100, stale 101, stale 102) + 2 from memory (new 101, 102) = 5
        // Duplicates at block 101, 102 — acceptable for this edge case
        assert_eq!(total, 5);
    }

    #[test]
    fn test_multiple_compactions() {
        let tmp = TempDir::new().unwrap();
        let meta = evm_meta();
        let mut store = DatasetStore::open(tmp.path(), 100 * 1024 * 1024).unwrap();

        for i in 100..=109 {
            store
                .push_block(make_block(i, &[&format!("0x{:x}", i)]))
                .unwrap();
        }

        // Compact in stages
        store.compact(102, &meta).unwrap(); // chunk 100-102
        store.compact(105, &meta).unwrap(); // chunk 103-105
        store.compact(108, &meta).unwrap(); // chunk 106-108

        assert_eq!(store.chunk_count(), 3);
        assert_eq!(store.buffer.len(), 1); // only block 109

        // Query across 3 chunks + memory
        let reader = store.reader();
        let request = ScanRequest::new(vec!["block_number", "address"]);
        let batches = reader.scan("logs", &request).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_crash_recovery_with_compacted_chunks() {
        let tmp = TempDir::new().unwrap();
        let meta = evm_meta();
        let store_path = tmp.path().join("dataset");

        {
            let mut store = DatasetStore::open(&store_path, 100 * 1024 * 1024).unwrap();
            for i in 100..=105 {
                store
                    .push_block(make_block(i, &[&format!("0x{:x}", i)]))
                    .unwrap();
            }
            store.compact(103, &meta).unwrap(); // chunk 100-103, buffer 104-105
                                                // crash here — buffer 104-105 in crash log
        }

        // Reopen
        let store = DatasetStore::open(&store_path, 100 * 1024 * 1024).unwrap();
        assert_eq!(store.chunk_count(), 1); // chunk 100-103 from disk
        assert_eq!(store.head(), Some(105)); // 104-105 recovered from crash log

        let reader = store.reader();
        let request = ScanRequest::new(vec!["block_number", "address"]);
        let batches = reader.scan("logs", &request).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 6);
    }

    #[test]
    fn test_disk_usage() {
        let tmp = TempDir::new().unwrap();
        let meta = evm_meta();
        let mut store = DatasetStore::open_with_flush_interval(
            tmp.path(),
            100 * 1024 * 1024,
            Duration::ZERO,
        )
        .unwrap();

        assert_eq!(store.disk_usage(), 0);

        store.push_block(make_block(100, &["0xaaa"])).unwrap();
        assert!(store.disk_usage() > 0); // WAL on disk

        store.compact(100, &meta).unwrap();
        assert!(store.disk_usage() > 0); // parquet chunk on disk
    }

    /// Regression: crash recovery with blocks that have different tables.
    /// Block 100 has "logs" + "traces", block 101 has only "logs".
    /// Before the fix, index-based zip in recover_crash_log would misalign
    /// tables across blocks, causing push to fail with "blocks out of order".
    #[test]
    fn test_crash_recovery_sparse_tables() {
        let tmp = TempDir::new().unwrap();
        let store_path = tmp.path().join("dataset");

        // Phase 1: push blocks with different table sets
        {
            let mut store = DatasetStore::open(&store_path, 100 * 1024 * 1024).unwrap();

            // Block 100: logs + traces
            let mut tables100 = HashMap::new();
            tables100.insert("logs".to_string(), make_batch(100, &["0xaaa"]));
            tables100.insert("traces".to_string(), make_batch(100, &["0x111"]));
            store
                .push_block(BlockData {
                    block_number: 100,
                    tables: tables100,
                })
                .unwrap();

            // Block 101: only logs (no traces for this block)
            let mut tables101 = HashMap::new();
            tables101.insert("logs".to_string(), make_batch(101, &["0xbbb"]));
            store
                .push_block(BlockData {
                    block_number: 101,
                    tables: tables101,
                })
                .unwrap();

            // Block 102: logs + traces again
            let mut tables102 = HashMap::new();
            tables102.insert("logs".to_string(), make_batch(102, &["0xccc"]));
            tables102.insert("traces".to_string(), make_batch(102, &["0x333"]));
            store
                .push_block(BlockData {
                    block_number: 102,
                    tables: tables102,
                })
                .unwrap();

            // crash — all 3 blocks in crash log with sparse "traces" table
        }

        // Phase 2: reopen — must not panic on "blocks out of order"
        let store = DatasetStore::open(&store_path, 100 * 1024 * 1024).unwrap();
        assert_eq!(store.head(), Some(102));

        let reader = store.reader();
        let request = ScanRequest::new(vec!["block_number", "address"]);

        // logs: all 3 blocks
        let logs = reader.scan("logs", &request).unwrap();
        let log_rows: usize = logs.iter().map(|b| b.num_rows()).sum();
        assert_eq!(log_rows, 3);

        // traces: only blocks 100 and 102
        let traces = reader.scan("traces", &request).unwrap();
        let trace_rows: usize = traces.iter().map(|b| b.num_rows()).sum();
        assert_eq!(trace_rows, 2);
    }

    /// Regression: per-block compaction creates one chunk per block.
    /// This test proves the bug exists at the DatasetStore level —
    /// calling compact() after each finalization drains only 1 block each time.
    #[test]
    fn test_per_block_compact_creates_many_chunks() {
        let tmp = TempDir::new().unwrap();
        let meta = evm_meta();
        let mut store = DatasetStore::open(tmp.path(), 100 * 1024 * 1024).unwrap();

        for i in 100..110 {
            store
                .push_block(make_block(i, &[&format!("0x{:x}", i)]))
                .unwrap();
            store.set_finalized_head(i).unwrap();
            // Bug pattern: compact after every block
            store.compact(i, &meta).unwrap();
        }

        // Each compact drained 1 block → 10 tiny chunks
        assert_eq!(store.chunk_count(), 10);

        // Correct pattern: batch compaction
        let tmp2 = TempDir::new().unwrap();
        let mut store2 = DatasetStore::open(tmp2.path(), 100 * 1024 * 1024).unwrap();
        for i in 100..110 {
            store2
                .push_block(make_block(i, &[&format!("0x{:x}", i)]))
                .unwrap();
        }
        // Single compact at the end
        store2.compact(109, &meta).unwrap();
        assert_eq!(store2.chunk_count(), 1);
    }

    #[test]
    fn test_finalized_bytes_in_memory() {
        let tmp = TempDir::new().unwrap();
        let mut store = DatasetStore::open(tmp.path(), 100 * 1024 * 1024).unwrap();

        store.push_block(make_block(100, &["0xaaa"])).unwrap();
        store.push_block(make_block(101, &["0xbbb"])).unwrap();
        store.push_block(make_block(102, &["0xccc"])).unwrap();

        // Nothing finalized
        assert_eq!(store.finalized_bytes_in_memory(99), 0);
        assert_eq!(store.finalized_blocks_in_memory(99), 0);

        // 2 blocks finalized
        assert!(store.finalized_bytes_in_memory(101) > 0);
        assert_eq!(store.finalized_blocks_in_memory(101), 2);

        // All 3 finalized
        let bytes_3 = store.finalized_bytes_in_memory(102);
        let bytes_2 = store.finalized_bytes_in_memory(101);
        assert!(bytes_3 > bytes_2);
        assert_eq!(store.finalized_blocks_in_memory(102), 3);
    }

    #[test]
    fn test_block_range_routing_latest() {
        let tmp = TempDir::new().unwrap();
        let meta = evm_meta();
        let mut store = DatasetStore::open(tmp.path(), 100 * 1024 * 1024).unwrap();

        store.push_block(make_block(100, &["0xaaa"])).unwrap();
        store.push_block(make_block(101, &["0xbbb"])).unwrap();
        store.push_block(make_block(200, &["0xccc"])).unwrap();

        store.compact(101, &meta).unwrap();

        // Query only latest block — should skip parquet chunk
        let reader = store.reader();
        let mut request = ScanRequest::new(vec!["block_number", "address"]);
        request.from_block = Some(200);

        let batches = reader.scan("logs", &request).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1); // only block 200 from memory
    }

    #[test]
    fn test_finalized_head_survives_crash() {
        let tmp = TempDir::new().unwrap();
        let store_path = tmp.path().join("dataset");

        // Phase 1: set finalized head and crash
        {
            let mut store = DatasetStore::open(&store_path, 100 * 1024 * 1024).unwrap();
            store.push_block(make_block(100, &["0xaaa"])).unwrap();
            store.push_block(make_block(101, &["0xbbb"])).unwrap();
            store.set_finalized_head(101).unwrap();
            assert_eq!(store.finalized_head(), Some(101));
            // drop — crash, crash log flush should persist finalized_head
        }

        // Phase 2: reopen — finalized_head must be recovered
        {
            let store = DatasetStore::open(&store_path, 100 * 1024 * 1024).unwrap();
            assert_eq!(store.finalized_head(), Some(101));
        }
    }

    #[test]
    fn test_finalized_head_monotonic_in_store() {
        let tmp = TempDir::new().unwrap();
        let mut store = DatasetStore::open(tmp.path(), 100 * 1024 * 1024).unwrap();

        store.set_finalized_head(100).unwrap();
        assert_eq!(store.finalized_head(), Some(100));

        store.set_finalized_head(200).unwrap();
        assert_eq!(store.finalized_head(), Some(200));

        // Cannot go backward
        store.set_finalized_head(150).unwrap();
        assert_eq!(store.finalized_head(), Some(200));
    }

    #[test]
    fn test_finalized_head_survives_compaction() {
        let tmp = TempDir::new().unwrap();
        let meta = evm_meta();
        let store_path = tmp.path().join("dataset");

        {
            let mut store = DatasetStore::open(&store_path, 100 * 1024 * 1024).unwrap();
            store.push_block(make_block(100, &["0xaaa"])).unwrap();
            store.push_block(make_block(101, &["0xbbb"])).unwrap();
            store.push_block(make_block(102, &["0xccc"])).unwrap();
            store.set_finalized_head(101).unwrap();
            store.compact(101, &meta).unwrap();
            // finalized_head = 101, now crash
        }

        {
            let store = DatasetStore::open(&store_path, 100 * 1024 * 1024).unwrap();
            assert_eq!(store.finalized_head(), Some(101));
            assert_eq!(store.chunk_count(), 1);
            // block 102 recovered from crash log
            assert_eq!(store.head(), Some(102));
        }
    }
}
