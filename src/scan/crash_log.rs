//! IPC crash log for durability of the in-memory buffer.
//!
//! Append-only Arrow IPC stream file. Never queried during normal operation —
//! only read on startup to rebuild the in-memory buffer after a crash.
//!
//! Format: one IPC file per table per dataset.
//! Each file: Arrow IPC Stream (schema header + length-prefixed RecordBatch messages).
//! One RecordBatch per block.
//!
//! Crash recovery: read from start, skip incomplete trailing message.
//! Reorg: ftruncate to offset of fork point.

use anyhow::{Context, Result};
use arrow::ipc::writer::StreamWriter;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Tracks the file offset after each block write, for truncation on reorg.
#[derive(Clone, Debug)]
struct BlockOffset {
    block_number: u64,
    /// File offset AFTER this block's data was written (truncate point).
    end_offset: u64,
}

/// Writer for a single table's IPC crash log file.
struct TableLog {
    path: PathBuf,
    writer: StreamWriter<BufWriter<File>>,
    /// Offsets after each block, for reorg truncation.
    block_offsets: Vec<BlockOffset>,
}

/// Crash log writer: one IPC file per table.
/// Writes are buffered and flushed periodically via `maybe_flush()`.
/// Also persists finalized_head alongside flush — no separate IO.
pub struct CrashLogWriter {
    dir: PathBuf,
    tables: HashMap<String, TableLog>,
    dirty: bool,
    last_flush: std::time::Instant,
    flush_interval: Duration,
    finalized_head: Option<u64>,
}

impl CrashLogWriter {
    /// Create a new crash log in the given directory.
    /// `flush_interval` controls how often buffered writes are flushed to disk.
    pub fn open(dir: &Path, flush_interval: Duration) -> Result<Self> {
        fs::create_dir_all(dir).context("creating crash log directory")?;
        Ok(Self {
            dir: dir.to_path_buf(),
            tables: HashMap::new(),
            dirty: false,
            last_flush: std::time::Instant::now(),
            flush_interval,
            finalized_head: None,
        })
    }

    /// Append a block's RecordBatch for a table.
    pub fn append(
        &mut self,
        table: &str,
        block_number: u64,
        batch: &RecordBatch,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let log = if let Some(log) = self.tables.get_mut(table) {
            log
        } else {
            let path = self.dir.join(format!("{}.ipc", table));
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .with_context(|| format!("creating crash log file {}", path.display()))?;
            let buf_writer = BufWriter::with_capacity(1024 * 1024, file); // 1 MB buffer
            let writer = StreamWriter::try_new(buf_writer, &batch.schema())
                .context("creating IPC stream writer")?;

            self.tables.entry(table.to_string()).or_insert(TableLog {
                path,
                writer,
                block_offsets: Vec::new(),
            })
        };

        log.writer.write(batch).context("writing IPC batch")?;

        // Track offset (BufWriter may not have flushed yet, so use writer position)
        log.block_offsets.push(BlockOffset {
            block_number,
            end_offset: 0, // updated on flush
        });

        self.dirty = true;
        self.maybe_flush()?;
        Ok(())
    }

    /// Update finalized_head. Monotonic: ignores values <= current.
    /// Always persisted immediately (one small file write, no IPC flush).
    pub fn set_finalized_head(&mut self, block_number: u64) -> Result<()> {
        if self.finalized_head.map_or(true, |c| block_number > c) {
            self.finalized_head = Some(block_number);
            let path = self.dir.join("finalized_head");
            let tmp = self.dir.join("finalized_head.tmp");
            std::fs::write(&tmp, block_number.to_string().as_bytes())
                .context("writing finalized_head")?;
            std::fs::rename(&tmp, &path).context("renaming finalized_head")?;
        }
        Ok(())
    }

    pub fn finalized_head(&self) -> Option<u64> {
        self.finalized_head
    }

    /// Flush buffered writes to disk if enough time has elapsed.
    pub fn maybe_flush(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        if self.last_flush.elapsed() < self.flush_interval {
            return Ok(());
        }
        self.flush()
    }

    /// Force flush all buffered writes to disk.
    pub fn flush(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        for log in self.tables.values_mut() {
            log.writer.get_mut().flush().context("flushing IPC")?;

            // Update block offsets with actual file position
            let offset = log
                .writer
                .get_ref()
                .get_ref()
                .metadata()
                .map(|m| m.len())
                .unwrap_or(0);

            // Set all pending offsets to current position
            // (conservative: truncation will remove all blocks after the last flush)
            for bo in log.block_offsets.iter_mut().rev() {
                if bo.end_offset == 0 {
                    bo.end_offset = offset;
                } else {
                    break;
                }
            }
        }
        self.dirty = false;
        self.last_flush = std::time::Instant::now();
        Ok(())
    }

    /// Truncate all table logs to remove blocks >= fork_point.
    pub fn truncate(&mut self, fork_point: u64) -> Result<()> {
        // Flush pending writes so offsets are valid for truncation
        self.flush()?;
        let table_names: Vec<String> = self.tables.keys().cloned().collect();

        for table in table_names {
            let log = self.tables.get_mut(&table).unwrap();

            // Find the truncation point
            let keep_count = log
                .block_offsets
                .partition_point(|o| o.block_number < fork_point);

            if keep_count == log.block_offsets.len() {
                continue; // nothing to truncate
            }

            let truncate_offset = if keep_count == 0 {
                // Remove everything — delete file and re-create on next write
                log.block_offsets.clear();
                drop(self.tables.remove(&table));
                let path = self.dir.join(format!("{}.ipc", table));
                let _ = fs::remove_file(&path);
                continue;
            } else {
                log.block_offsets[keep_count - 1].end_offset
            };

            log.block_offsets.truncate(keep_count);

            // Close current writer, truncate file, reopen
            // StreamWriter doesn't support truncation, so we need to recreate
            drop(self.tables.remove(&table));

            let path = self.dir.join(format!("{}.ipc", table));
            let file = OpenOptions::new()
                .write(true)
                .open(&path)
                .with_context(|| format!("opening crash log for truncation {}", path.display()))?;
            file.set_len(truncate_offset)
                .context("truncating crash log")?;

            // Note: the file is now truncated but no longer has a valid writer.
            // Next append() for this table will create a fresh writer.
            // This means we lose the IPC stream footer, but since we only
            // read crash logs via recovery (which handles incomplete streams),
            // this is fine.
        }

        Ok(())
    }

    /// Remove all crash log files (after successful compaction).
    pub fn clear(&mut self) -> Result<()> {
        self.tables.clear();
        // Remove all .ipc files in directory
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("ipc") {
                    let _ = fs::remove_file(&path);
                }
            }
        }
        Ok(())
    }
}

/// Recover persisted finalized_head from crash log directory.
pub fn recover_finalized_head(dir: &Path) -> Option<u64> {
    let path = dir.join("finalized_head");
    std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
}

/// Read crash log files and recover blocks.
/// Returns blocks in order, grouped by block_number.
pub fn recover_crash_log(
    dir: &Path,
) -> Result<Vec<(u64, HashMap<String, RecordBatch>)>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    // Read each table's IPC file
    let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();

    for entry in fs::read_dir(dir)
        .with_context(|| format!("reading crash log dir {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("ipc") {
            continue;
        }

        let table_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();

        let batches = read_ipc_file_safe(&path)?;
        if !batches.is_empty() {
            table_batches.insert(table_name, batches);
        }
    }

    if table_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Group by block_number across all tables.
    // Each batch has a block_number column — use it to correlate,
    // since not all tables have data for every block.
    use std::collections::BTreeMap;
    let mut by_block: BTreeMap<u64, HashMap<String, RecordBatch>> = BTreeMap::new();

    for (table_name, batches) in &table_batches {
        for batch in batches {
            let bn = extract_block_number(batch, table_name)
                .unwrap_or(0);
            by_block
                .entry(bn)
                .or_default()
                .insert(table_name.clone(), batch.clone());
        }
    }

    Ok(by_block.into_iter().collect())
}

/// Read an IPC stream file, handling incomplete trailing messages gracefully.
fn read_ipc_file_safe(path: &Path) -> Result<Vec<RecordBatch>> {
    let mut data = Vec::new();
    File::open(path)
        .with_context(|| format!("opening crash log {}", path.display()))?
        .read_to_end(&mut data)?;

    if data.is_empty() {
        return Ok(Vec::new());
    }

    let cursor = Cursor::new(&data);
    let reader = match StreamReader::try_new(cursor, None) {
        Ok(r) => r,
        Err(_) => return Ok(Vec::new()), // corrupted header
    };

    let mut batches = Vec::new();
    for batch_result in reader {
        match batch_result {
            Ok(batch) => batches.push(batch),
            Err(_) => break, // incomplete trailing message — stop here
        }
    }

    Ok(batches)
}

/// Try to extract block_number from a RecordBatch.
fn extract_block_number(batch: &RecordBatch, table_name: &str) -> Option<u64> {
    let col_name = if table_name == "blocks" {
        "number"
    } else {
        "block_number"
    };

    let col = batch.column_by_name(col_name)?;
    use arrow::array::*;

    if col.is_empty() {
        return None;
    }

    match col.data_type() {
        arrow::datatypes::DataType::Int32 => {
            Some(col.as_any().downcast_ref::<Int32Array>()?.value(0) as u64)
        }
        arrow::datatypes::DataType::UInt32 => {
            Some(col.as_any().downcast_ref::<UInt32Array>()?.value(0) as u64)
        }
        arrow::datatypes::DataType::Int64 => {
            Some(col.as_any().downcast_ref::<Int64Array>()?.value(0) as u64)
        }
        arrow::datatypes::DataType::UInt64 => {
            Some(col.as_any().downcast_ref::<UInt64Array>()?.value(0))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::*;
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

    #[test]
    fn test_write_and_recover() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        // Write blocks
        {
            let mut writer = CrashLogWriter::open(&log_dir, Duration::ZERO).unwrap();
            writer
                .append("logs", 100, &make_batch(100, &["0xaaa", "0xbbb"]))
                .unwrap();
            writer
                .append("logs", 101, &make_batch(101, &["0xccc"]))
                .unwrap();
            writer
                .append("txs", 100, &make_batch(100, &["0x111"]))
                .unwrap();
            writer
                .append("txs", 101, &make_batch(101, &["0x222"]))
                .unwrap();
        }

        // Recover
        let recovered = recover_crash_log(&log_dir).unwrap();
        assert_eq!(recovered.len(), 2); // 2 blocks

        let (bn0, tables0) = &recovered[0];
        assert_eq!(*bn0, 100);
        assert!(tables0.contains_key("logs"));
        assert!(tables0.contains_key("txs"));
        assert_eq!(tables0["logs"].num_rows(), 2);
        assert_eq!(tables0["txs"].num_rows(), 1);

        let (bn1, tables1) = &recovered[1];
        assert_eq!(*bn1, 101);
        assert_eq!(tables1["logs"].num_rows(), 1);
    }

    #[test]
    fn test_truncate_reorg() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        let mut writer = CrashLogWriter::open(&log_dir, Duration::ZERO).unwrap();
        writer
            .append("logs", 100, &make_batch(100, &["0xaaa"]))
            .unwrap();
        writer
            .append("logs", 101, &make_batch(101, &["0xbbb"]))
            .unwrap();
        writer
            .append("logs", 102, &make_batch(102, &["0xccc"]))
            .unwrap();

        // Reorg at block 101
        writer.truncate(101).unwrap();

        // Recover — should only have block 100
        let recovered = recover_crash_log(&log_dir).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].0, 100);
    }

    #[test]
    fn test_clear() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        let mut writer = CrashLogWriter::open(&log_dir, Duration::ZERO).unwrap();
        writer
            .append("logs", 100, &make_batch(100, &["0xaaa"]))
            .unwrap();

        writer.clear().unwrap();

        let recovered = recover_crash_log(&log_dir).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_recover_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let recovered = recover_crash_log(tmp.path()).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_recover_nonexistent_dir() {
        let recovered = recover_crash_log(Path::new("/nonexistent/path")).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_recover_corrupt_trailing_data() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        // Write valid blocks
        {
            let mut writer = CrashLogWriter::open(&log_dir, Duration::ZERO).unwrap();
            writer
                .append("logs", 100, &make_batch(100, &["0xaaa"]))
                .unwrap();
            writer
                .append("logs", 101, &make_batch(101, &["0xbbb"]))
                .unwrap();
        }

        // Corrupt the file: append garbage bytes (simulates crash mid-write)
        let ipc_path = log_dir.join("logs.ipc");
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&ipc_path)
                .unwrap();
            file.write_all(b"GARBAGE_CORRUPT_DATA_12345").unwrap();
        }

        // Recovery should return the 2 valid blocks, skip corrupt tail
        let recovered = recover_crash_log(&log_dir).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].0, 100);
        assert_eq!(recovered[1].0, 101);
    }

    #[test]
    fn test_recover_completely_corrupt_file() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");
        std::fs::create_dir_all(&log_dir).unwrap();

        // Write pure garbage as an IPC file
        std::fs::write(log_dir.join("logs.ipc"), b"NOT_VALID_IPC_AT_ALL").unwrap();

        // Recovery should return empty (corrupt header)
        let recovered = recover_crash_log(&log_dir).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_truncate_all_blocks() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        let mut writer = CrashLogWriter::open(&log_dir, Duration::ZERO).unwrap();
        writer
            .append("logs", 100, &make_batch(100, &["0xaaa"]))
            .unwrap();
        writer
            .append("logs", 101, &make_batch(101, &["0xbbb"]))
            .unwrap();

        // Truncate everything (fork at block 100 = remove 100 and above)
        writer.truncate(100).unwrap();

        let recovered = recover_crash_log(&log_dir).unwrap();
        assert!(recovered.is_empty());
    }

    /// Regression: tables with different block counts must recover correctly.
    /// Before the fix, recovery zipped by index — if "logs" had blocks [100,101,102]
    /// but "traces" only had [100,102] (block 101 had no traces), the index-based
    /// zip would misalign: traces batch[1] (block 102) would be paired with
    /// logs batch[1] (block 101), producing wrong block_numbers.
    #[test]
    fn test_recover_sparse_tables() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        {
            let mut writer = CrashLogWriter::open(&log_dir, Duration::ZERO).unwrap();
            // "logs" has data for blocks 100, 101, 102
            writer.append("logs", 100, &make_batch(100, &["0xaaa"])).unwrap();
            writer.append("logs", 101, &make_batch(101, &["0xbbb"])).unwrap();
            writer.append("logs", 102, &make_batch(102, &["0xccc"])).unwrap();

            // "traces" only has data for blocks 100 and 102 (block 101 had no traces)
            writer.append("traces", 100, &make_batch(100, &["0x111"])).unwrap();
            writer.append("traces", 102, &make_batch(102, &["0x333"])).unwrap();
        }

        let recovered = recover_crash_log(&log_dir).unwrap();
        assert_eq!(recovered.len(), 3, "should have 3 blocks");

        // Block 100: both tables
        assert_eq!(recovered[0].0, 100);
        assert!(recovered[0].1.contains_key("logs"));
        assert!(recovered[0].1.contains_key("traces"));

        // Block 101: only logs (no traces)
        assert_eq!(recovered[1].0, 101);
        assert!(recovered[1].1.contains_key("logs"));
        assert!(!recovered[1].1.contains_key("traces"));

        // Block 102: both tables
        assert_eq!(recovered[2].0, 102);
        assert!(recovered[2].1.contains_key("logs"));
        assert!(recovered[2].1.contains_key("traces"));
    }

    #[test]
    fn test_finalized_head_persisted_immediately() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        let mut writer = CrashLogWriter::open(&log_dir, Duration::from_secs(600)).unwrap();

        // Written immediately on set (no flush needed)
        writer.set_finalized_head(100).unwrap();
        assert_eq!(writer.finalized_head(), Some(100));
        assert_eq!(recover_finalized_head(&log_dir), Some(100));

        writer.set_finalized_head(200).unwrap();
        assert_eq!(recover_finalized_head(&log_dir), Some(200));
    }

    #[test]
    fn test_finalized_head_monotonic() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        let mut writer = CrashLogWriter::open(&log_dir, Duration::ZERO).unwrap();

        writer.set_finalized_head(100);
        assert_eq!(writer.finalized_head(), Some(100));

        // Can go forward
        writer.set_finalized_head(200);
        assert_eq!(writer.finalized_head(), Some(200));

        // Cannot go backward
        writer.set_finalized_head(150);
        assert_eq!(writer.finalized_head(), Some(200));

        // Cannot go to same value (no-op)
        writer.set_finalized_head(200);
        assert_eq!(writer.finalized_head(), Some(200));
    }

    #[test]
    fn test_finalized_head_survives_crash() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        // Phase 1: write finalized_head and crash
        {
            let mut writer = CrashLogWriter::open(&log_dir, Duration::ZERO).unwrap();
            writer.append("logs", 100, &make_batch(100, &["0xaaa"])).unwrap();
            writer.set_finalized_head(500);
            // flush triggered by Duration::ZERO in append's maybe_flush
            // drop — simulates crash
        }

        // Phase 2: recover
        let recovered_fh = recover_finalized_head(&log_dir);
        assert_eq!(recovered_fh, Some(500));

        // Reopen writer — finalized_head should be loadable
        let mut writer = CrashLogWriter::open(&log_dir, Duration::ZERO).unwrap();
        // Writer starts fresh (doesn't auto-load), but file is on disk
        // DatasetStore loads it via recover_finalized_head()
        assert_eq!(recover_finalized_head(&log_dir), Some(500));

        // New writes should not regress it
        writer.set_finalized_head(600);
        writer.append("logs", 101, &make_batch(101, &["0xbbb"])).unwrap();
        assert_eq!(recover_finalized_head(&log_dir), Some(600));
    }

    #[test]
    fn test_finalized_head_not_lost_on_clear() {
        let tmp = TempDir::new().unwrap();
        let log_dir = tmp.path().join("crash_log");

        let mut writer = CrashLogWriter::open(&log_dir, Duration::ZERO).unwrap();
        writer.append("logs", 100, &make_batch(100, &["0xaaa"])).unwrap();
        writer.set_finalized_head(100);

        assert_eq!(recover_finalized_head(&log_dir), Some(100));

        // clear() removes IPC files but should preserve finalized_head
        writer.clear().unwrap();

        assert_eq!(recover_finalized_head(&log_dir), Some(100));
    }
}
