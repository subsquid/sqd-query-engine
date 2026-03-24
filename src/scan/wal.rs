//! Single-file WAL (Write-Ahead Log) for durability of the in-memory buffer.
//!
//! One file per dataset. Each record = one block with all its tables.
//! Arrow IPC encoding for RecordBatch data inside a simple binary container.
//!
//! Format:
//! ```text
//! Record = [block_number: u64]
//!          [finalized_head: u64]  (0 = no update)
//!          [num_tables: u32]
//!          For each table:
//!            [table_name_len: u16][table_name: bytes]
//!            [ipc_len: u32][ipc_data: bytes]  ← Arrow IPC encoded RecordBatch
//! ```
//!
//! Writes are buffered (1 MB BufWriter) and flushed periodically.
//! Recovery: sequential read, each record is self-contained (no sparse table bugs).
//! Reorg: ftruncate by block offset.

use anyhow::{Context, Result};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Tracks the file offset after each block, for truncation on reorg.
#[derive(Clone, Debug)]
struct BlockOffset {
    block_number: u64,
    end_offset: u64,
}

/// Single-file WAL writer for one dataset.
pub struct WalWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    block_offsets: Vec<BlockOffset>,
    dirty: bool,
    last_flush: std::time::Instant,
    flush_interval: Duration,
    finalized_head: Option<u64>,
}

const BUF_CAPACITY: usize = 1024 * 1024; // 1 MB

impl WalWriter {
    /// Open or create a WAL file.
    pub fn open(path: &Path, flush_interval: Duration) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).context("creating WAL directory")?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("opening WAL {}", path.display()))?;

        let offset = file.metadata().map(|m| m.len()).unwrap_or(0);

        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::with_capacity(BUF_CAPACITY, file),
            block_offsets: Vec::new(),
            dirty: false,
            last_flush: std::time::Instant::now(),
            flush_interval,
            finalized_head: None,
        })
    }

    /// Append a block with all its tables.
    pub fn append(
        &mut self,
        block_number: u64,
        tables: &HashMap<String, RecordBatch>,
    ) -> Result<()> {
        let finalized = self.finalized_head.unwrap_or(0);

        // Write block header
        self.writer.write_all(&block_number.to_le_bytes())?;
        self.writer.write_all(&finalized.to_le_bytes())?;
        self.writer
            .write_all(&(tables.len() as u32).to_le_bytes())?;

        // Write each table
        for (name, batch) in tables {
            if batch.num_rows() == 0 {
                continue;
            }
            let name_bytes = name.as_bytes();
            self.writer
                .write_all(&(name_bytes.len() as u16).to_le_bytes())?;
            self.writer.write_all(name_bytes)?;

            // Encode RecordBatch as Arrow IPC
            let ipc_data = encode_batch_to_ipc(batch)?;
            self.writer
                .write_all(&(ipc_data.len() as u32).to_le_bytes())?;
            self.writer.write_all(&ipc_data)?;
        }

        self.block_offsets.push(BlockOffset {
            block_number,
            end_offset: 0, // updated on flush
        });

        self.dirty = true;
        self.maybe_flush()?;
        Ok(())
    }

    /// Update finalized_head. Monotonic: ignores values <= current.
    /// Persisted with the next block append or flush.
    pub fn set_finalized_head(&mut self, block_number: u64) -> Result<()> {
        if self.finalized_head.map_or(true, |c| block_number > c) {
            self.finalized_head = Some(block_number);
            // Write a standalone finalized_head marker (block with 0 tables)
            self.writer.write_all(&0u64.to_le_bytes())?; // block_number = 0 (marker)
            self.writer
                .write_all(&block_number.to_le_bytes())?; // finalized_head
            self.writer.write_all(&0u32.to_le_bytes())?; // num_tables = 0
            self.dirty = true;
            self.maybe_flush()?;
        }
        Ok(())
    }

    pub fn finalized_head(&self) -> Option<u64> {
        self.finalized_head
    }

    /// Flush if enough time has elapsed.
    pub fn maybe_flush(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        if self.last_flush.elapsed() < self.flush_interval {
            return Ok(());
        }
        self.flush()
    }

    /// Force flush all buffered writes.
    pub fn flush(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        self.writer.flush().context("flushing WAL")?;

        // Update block offsets with actual file position
        let offset = self
            .writer
            .get_ref()
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0);
        for bo in self.block_offsets.iter_mut().rev() {
            if bo.end_offset == 0 {
                bo.end_offset = offset;
            } else {
                break;
            }
        }

        self.dirty = false;
        self.last_flush = std::time::Instant::now();
        Ok(())
    }

    /// Truncate: remove blocks >= fork_point.
    pub fn truncate(&mut self, fork_point: u64) -> Result<()> {
        self.flush()?;

        let keep_count = self
            .block_offsets
            .partition_point(|o| o.block_number < fork_point);

        if keep_count == self.block_offsets.len() {
            return Ok(()); // nothing to truncate
        }

        let truncate_offset = if keep_count == 0 {
            0
        } else {
            self.block_offsets[keep_count - 1].end_offset
        };

        self.block_offsets.truncate(keep_count);

        // Reopen file for truncation (BufWriter doesn't support set_len)
        drop(std::mem::replace(
            &mut self.writer,
            BufWriter::with_capacity(BUF_CAPACITY, File::open("/dev/null").unwrap()),
        ));

        let file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .context("opening WAL for truncation")?;
        file.set_len(truncate_offset)
            .context("truncating WAL")?;

        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .context("reopening WAL after truncation")?;
        self.writer = BufWriter::with_capacity(BUF_CAPACITY, file);

        Ok(())
    }

    /// Clear all data (after compaction).
    pub fn clear(&mut self) -> Result<()> {
        self.block_offsets.clear();

        drop(std::mem::replace(
            &mut self.writer,
            BufWriter::with_capacity(BUF_CAPACITY, File::open("/dev/null").unwrap()),
        ));

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)
            .context("clearing WAL")?;
        self.writer = BufWriter::with_capacity(BUF_CAPACITY, file);
        self.dirty = false;

        Ok(())
    }
}

/// Encode a RecordBatch as Arrow IPC bytes.
fn encode_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .context("creating IPC writer")?;
        writer.write(batch).context("writing IPC batch")?;
        writer.finish().context("finishing IPC writer")?;
    }
    Ok(buf)
}

/// Decode a RecordBatch from Arrow IPC bytes.
fn decode_batch_from_ipc(data: &[u8]) -> Result<RecordBatch> {
    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None).context("reading IPC header")?;
    for batch_result in reader {
        return batch_result.context("reading IPC batch");
    }
    anyhow::bail!("empty IPC stream")
}

/// Recovered WAL data.
pub struct WalRecovery {
    pub blocks: Vec<(u64, HashMap<String, RecordBatch>)>,
    pub finalized_head: Option<u64>,
}

/// Read WAL file and recover blocks + finalized_head.
pub fn recover_wal(path: &Path) -> Result<WalRecovery> {
    if !path.exists() {
        return Ok(WalRecovery {
            blocks: Vec::new(),
            finalized_head: None,
        });
    }

    let mut data = Vec::new();
    File::open(path)
        .with_context(|| format!("opening WAL {}", path.display()))?
        .read_to_end(&mut data)?;

    if data.is_empty() {
        return Ok(WalRecovery {
            blocks: Vec::new(),
            finalized_head: None,
        });
    }

    let mut cursor = Cursor::new(&data);
    let mut blocks: Vec<(u64, HashMap<String, RecordBatch>)> = Vec::new();
    let mut finalized_head: Option<u64> = None;

    loop {
        // Try to read block header
        let mut header = [0u8; 8 + 8 + 4]; // block_number + finalized_head + num_tables
        match cursor.read_exact(&mut header) {
            Ok(()) => {}
            Err(_) => break, // EOF or incomplete record
        }

        let block_number = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let fh = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let num_tables = u32::from_le_bytes(header[16..20].try_into().unwrap());

        if fh > 0 {
            finalized_head = Some(fh);
        }

        // Read tables
        let mut tables = HashMap::new();
        let mut ok = true;

        for _ in 0..num_tables {
            // table name
            let mut name_len_buf = [0u8; 2];
            if cursor.read_exact(&mut name_len_buf).is_err() {
                ok = false;
                break;
            }
            let name_len = u16::from_le_bytes(name_len_buf) as usize;

            let mut name_buf = vec![0u8; name_len];
            if cursor.read_exact(&mut name_buf).is_err() {
                ok = false;
                break;
            }
            let name = String::from_utf8_lossy(&name_buf).to_string();

            // IPC data
            let mut ipc_len_buf = [0u8; 4];
            if cursor.read_exact(&mut ipc_len_buf).is_err() {
                ok = false;
                break;
            }
            let ipc_len = u32::from_le_bytes(ipc_len_buf) as usize;

            let mut ipc_data = vec![0u8; ipc_len];
            if cursor.read_exact(&mut ipc_data).is_err() {
                ok = false;
                break;
            }

            match decode_batch_from_ipc(&ipc_data) {
                Ok(batch) => {
                    tables.insert(name, batch);
                }
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }

        if !ok {
            break; // incomplete record at end of file — stop here
        }

        // Skip finalized_head-only markers (block_number=0, num_tables=0)
        if block_number > 0 || !tables.is_empty() {
            blocks.push((block_number, tables));
        }
    }

    Ok(WalRecovery {
        blocks,
        finalized_head,
    })
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

    fn make_tables(block_number: u64) -> HashMap<String, RecordBatch> {
        let mut tables = HashMap::new();
        tables.insert(
            "logs".to_string(),
            make_batch(block_number as i32, &["0xaaa"]),
        );
        tables
    }

    #[test]
    fn test_write_and_recover() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal.bin");

        {
            let mut wal = WalWriter::open(&wal_path, Duration::ZERO).unwrap();
            wal.append(100, &make_tables(100)).unwrap();
            wal.append(101, &make_tables(101)).unwrap();
        }

        let recovery = recover_wal(&wal_path).unwrap();
        assert_eq!(recovery.blocks.len(), 2);
        assert_eq!(recovery.blocks[0].0, 100);
        assert_eq!(recovery.blocks[1].0, 101);
        assert!(recovery.blocks[0].1.contains_key("logs"));
    }

    #[test]
    fn test_finalized_head_monotonic() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal.bin");

        let mut wal = WalWriter::open(&wal_path, Duration::ZERO).unwrap();
        wal.set_finalized_head(100).unwrap();
        assert_eq!(wal.finalized_head(), Some(100));

        wal.set_finalized_head(200).unwrap();
        assert_eq!(wal.finalized_head(), Some(200));

        // Cannot go backward
        wal.set_finalized_head(150).unwrap();
        assert_eq!(wal.finalized_head(), Some(200));
    }

    #[test]
    fn test_finalized_head_survives_crash() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal.bin");

        {
            let mut wal = WalWriter::open(&wal_path, Duration::ZERO).unwrap();
            wal.append(100, &make_tables(100)).unwrap();
            wal.set_finalized_head(500).unwrap();
            // drop = crash
        }

        let recovery = recover_wal(&wal_path).unwrap();
        assert_eq!(recovery.finalized_head, Some(500));
        assert_eq!(recovery.blocks.len(), 1);
        assert_eq!(recovery.blocks[0].0, 100);
    }

    #[test]
    fn test_truncate_reorg() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal.bin");

        let mut wal = WalWriter::open(&wal_path, Duration::ZERO).unwrap();
        wal.append(100, &make_tables(100)).unwrap();
        wal.append(101, &make_tables(101)).unwrap();
        wal.append(102, &make_tables(102)).unwrap();

        wal.truncate(101).unwrap();

        let recovery = recover_wal(&wal_path).unwrap();
        assert_eq!(recovery.blocks.len(), 1);
        assert_eq!(recovery.blocks[0].0, 100);
    }

    #[test]
    fn test_truncate_all() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal.bin");

        let mut wal = WalWriter::open(&wal_path, Duration::ZERO).unwrap();
        wal.append(100, &make_tables(100)).unwrap();
        wal.truncate(100).unwrap();

        let recovery = recover_wal(&wal_path).unwrap();
        assert!(recovery.blocks.is_empty());
    }

    #[test]
    fn test_clear() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal.bin");

        let mut wal = WalWriter::open(&wal_path, Duration::ZERO).unwrap();
        wal.append(100, &make_tables(100)).unwrap();
        wal.clear().unwrap();

        let recovery = recover_wal(&wal_path).unwrap();
        assert!(recovery.blocks.is_empty());
    }

    #[test]
    fn test_sparse_tables_no_bug() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal.bin");

        {
            let mut wal = WalWriter::open(&wal_path, Duration::ZERO).unwrap();

            // Block 100: logs + traces
            let mut tables100 = HashMap::new();
            tables100.insert("logs".to_string(), make_batch(100, &["0xaaa"]));
            tables100.insert("traces".to_string(), make_batch(100, &["0x111"]));
            wal.append(100, &tables100).unwrap();

            // Block 101: only logs (no traces)
            let mut tables101 = HashMap::new();
            tables101.insert("logs".to_string(), make_batch(101, &["0xbbb"]));
            wal.append(101, &tables101).unwrap();

            // Block 102: logs + traces
            let mut tables102 = HashMap::new();
            tables102.insert("logs".to_string(), make_batch(102, &["0xccc"]));
            tables102.insert("traces".to_string(), make_batch(102, &["0x333"]));
            wal.append(102, &tables102).unwrap();
        }

        let recovery = recover_wal(&wal_path).unwrap();
        assert_eq!(recovery.blocks.len(), 3);

        // Block 100: both tables
        assert!(recovery.blocks[0].1.contains_key("logs"));
        assert!(recovery.blocks[0].1.contains_key("traces"));

        // Block 101: only logs — no misalignment
        assert!(recovery.blocks[1].1.contains_key("logs"));
        assert!(!recovery.blocks[1].1.contains_key("traces"));

        // Block 102: both tables
        assert!(recovery.blocks[2].1.contains_key("logs"));
        assert!(recovery.blocks[2].1.contains_key("traces"));
    }

    #[test]
    fn test_corrupt_trailing_data() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("wal.bin");

        {
            let mut wal = WalWriter::open(&wal_path, Duration::ZERO).unwrap();
            wal.append(100, &make_tables(100)).unwrap();
            wal.append(101, &make_tables(101)).unwrap();
        }

        // Append garbage
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .unwrap();
            file.write_all(b"GARBAGE").unwrap();
        }

        let recovery = recover_wal(&wal_path).unwrap();
        assert_eq!(recovery.blocks.len(), 2);
    }

    #[test]
    fn test_nonexistent_file() {
        let recovery = recover_wal(Path::new("/nonexistent/wal.bin")).unwrap();
        assert!(recovery.blocks.is_empty());
        assert_eq!(recovery.finalized_head, None);
    }
}
