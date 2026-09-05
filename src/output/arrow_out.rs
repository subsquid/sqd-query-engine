//! Flat per-table Arrow IPC output.
//!
//! Emits the post-scan `RecordBatch`es as flat, per-table Arrow IPC streams.
//! The Arrow schema is derived automatically from each batch — no hand-written
//! schema — and a consumer reads it back with full types via any Arrow binding,
//! zero schema config.
//!
//! ## Contract (analytical-native)
//!
//! - **Flat, per-table streams**, not nested-by-block. Every row carries its
//!   `block_number` (always a leading sort/filter column), so the JSON block
//!   nesting is reconstructable client-side with one integer group-by.
//! - **Columns are projected to the requested output fields** (+ `block_number`
//!   as the join key). Internal scan/weight/join columns are dropped.
//! - **snake_case, raw physical columns** — `topic0..3` stay separate (no
//!   `topics` array reconstruction), names are the parquet names. This is a
//!   deliberate, documented divergence from the JSON field shape; the trade is
//!   maximum producer speed and columnar-native ergonomics.
//! - **Multi-source tables are merged + deduped** to match JSON: a table fed by
//!   several relations (e.g. `transactions` pulled by both `traces` and
//!   `stateDiffs`) is unioned and deduped by `block_number + item_order_keys +
//!   address` (the same key the JSON path uses).
//! - **Optional hex→bytes**: with `binary`, columns declared `encoding: hex_bytes`
//!   in the metadata are decoded from `0x…` `Utf8` to raw `Binary`. The hex set
//!   is taken from the schema, not sniffed from the values, so a column's emitted
//!   type is stable across responses (an all-null hex column is still `Binary`;
//!   base58/other `Utf8` columns are left untouched). ~2× smaller raw, ~20-30%
//!   smaller after zstd, ~100× faster client decode, at the cost of a decode pass.
//!
//! ## Framing
//!
//! Tables are concatenated into one byte stream with a self-describing envelope:
//!
//! ```text
//! [u32 LE name_len][name utf8][u32 LE payload_len][arrow ipc stream bytes] ...
//! ```

use crate::integers::BlockNumbers;
use crate::metadata::{JsonEncoding, TableDescription};
use anyhow::Result;
use arrow::array::{Array, ArrayRef, BinaryBuilder, BooleanArray, StringArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::CompressionType;
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};
use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;

/// Result of an Arrow query execution: flat per-table IPC streams plus the
/// included block range (which may end below the queried range end if the
/// response was trimmed to the size budget).
pub struct ArrowOutput {
    data: Vec<u8>,
    first_block: u64,
    last_block: u64,
    num_blocks: usize,
}

impl ArrowOutput {
    pub(crate) fn new(data: Vec<u8>, selected_blocks: &[u64]) -> Self {
        Self {
            data,
            first_block: selected_blocks[0],
            last_block: selected_blocks[selected_blocks.len() - 1],
            num_blocks: selected_blocks.len(),
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    pub fn data_size(&self) -> usize {
        self.data.len()
    }

    pub fn num_blocks(&self) -> usize {
        self.num_blocks
    }

    pub fn first_block(&self) -> u64 {
        self.first_block
    }

    pub fn last_block(&self) -> u64 {
        self.last_block
    }
}

/// Output format selector threaded into the execution core.
#[derive(Clone, Copy, Debug)]
pub enum OutputFormat {
    /// Nested JSON (the production format): `[{header, logs:[...], ...}, ...]`.
    Json,
    /// Flat per-table Arrow IPC streams. `compress` toggles Arrow's built-in
    /// Zstd; `binary` decodes hex `Utf8` columns to raw bytes.
    Arrow { compress: bool, binary: bool },
}

/// Project a batch to `names` (by name, in the given order). Names absent from
/// the batch are skipped — every batch of a table shares a schema, so the result
/// schema is stable across a stream.
pub fn project_columns(batch: &RecordBatch, names: &[String]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut fields: Vec<Field> = Vec::with_capacity(names.len());
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(names.len());
    for n in names {
        if let Ok(i) = schema.index_of(n) {
            fields.push(schema.field(i).clone());
            cols.push(batch.column(i).clone());
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
}

/// Keep only rows whose `bn_col` block number satisfies `keep` (the weight-limit
/// row trim, matching the JSON path's `selected_blocks`).
pub fn filter_to_blocks(
    batch: &RecordBatch,
    bn_col: &str,
    keep: impl Fn(u64) -> bool,
) -> Result<RecordBatch> {
    let Some(col) = batch.column_by_name(bn_col) else {
        return Ok(batch.clone());
    };

    let blocks = BlockNumbers::resolve(col.as_ref(), bn_col)?;
    let mask: BooleanArray = (0..blocks.len())
        .map(|i| Some(keep(blocks.at(i))))
        .collect();
    Ok(filter_record_batch(batch, &mask)?)
}

/// Keep the first row for each distinct `key_cols` tuple (drops cross-source
/// duplicates after a multi-source union). Key columns absent from the batch are
/// ignored. Uses Arrow's row format so it is type-general.
///
/// A key column Arrow cannot put in row format is an error rather than a skipped
/// dedup: skipping it emits the duplicates the caller unioned two sources to
/// remove, and nothing in the response says so.
pub fn dedup_first(batch: &RecordBatch, key_cols: &[String]) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = key_cols
        .iter()
        .filter_map(|n| batch.column_by_name(n).cloned())
        .collect();
    if arrays.is_empty() {
        return Ok(batch.clone());
    }
    let fields: Vec<SortField> = arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields)?;
    let rows = converter.convert_columns(&arrays)?;
    let mut seen: HashSet<OwnedRow> = HashSet::with_capacity(batch.num_rows());
    let mask: BooleanArray = (0..batch.num_rows())
        .map(|i| Some(seen.insert(rows.row(i).owned())))
        .collect();
    Ok(filter_record_batch(batch, &mask)?)
}

/// Decode the table's hex `Utf8` columns from `0x…` text to raw `Binary`.
///
/// Which columns are hex is taken from the metadata (`encoding: hex_bytes`), not
/// sniffed from the values — so a column's emitted type is **stable across
/// responses** regardless of which rows are present: an all-null hex column is
/// still `Binary`, and base58/other `Utf8` columns are left as `Utf8`. Always
/// variable `Binary` (never `FixedSizeBinary`): the type then never depends on
/// the values seen, and the post-zstd size is equivalent.
pub fn hexify_group(
    batches: Vec<RecordBatch>,
    table_desc: &TableDescription,
) -> Result<Vec<RecordBatch>> {
    let Some(first) = batches.first() else {
        return Ok(batches);
    };
    let hex_idxs: HashSet<usize> = first
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            *f.data_type() == DataType::Utf8
                && matches!(
                    table_desc
                        .columns
                        .get(f.name())
                        .and_then(|c| c.encoding.as_ref()),
                    Some(JsonEncoding::HexBytes)
                )
        })
        .map(|(i, _)| i)
        .collect();
    if hex_idxs.is_empty() {
        return Ok(batches);
    }
    batches.iter().map(|b| hexify_batch(b, &hex_idxs)).collect()
}

/// Decode the `hex_idxs` columns of one batch from `0x…` hex `Utf8` to `Binary`.
/// A value that fails to decode (malformed/odd-length hex — a corrupt source)
/// becomes null rather than silently-zeroed bytes.
fn hexify_batch(batch: &RecordBatch, hex_idxs: &HashSet<usize>) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        // `hex_idxs` is read off the first batch of the group. A later batch
        // holding something else at that index is passed through rather than
        // downcast blindly; the write then fails on the schema mismatch, which
        // is a message rather than a dead worker thread.
        let decodable = hex_idxs
            .contains(&i)
            .then(|| col.as_any().downcast_ref::<StringArray>())
            .flatten();

        let Some(sa) = decodable else {
            fields.push(field.as_ref().clone());
            cols.push(col.clone());
            continue;
        };
        let mut b = BinaryBuilder::new();
        for r in 0..sa.len() {
            if sa.is_null(r) {
                b.append_null();
                continue;
            }
            let v = sa.value(r);
            let h = v.strip_prefix("0x").unwrap_or(v);
            let mut buf = vec![0u8; h.len() / 2];
            match faster_hex::hex_decode(h.as_bytes(), &mut buf) {
                Ok(()) => b.append_value(&buf),
                Err(_) => b.append_null(),
            }
        }
        fields.push(Field::new(
            field.name(),
            DataType::Binary,
            field.is_nullable(),
        ));
        cols.push(Arc::new(b.finish()));
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
}

/// Serialize `(table_name, batches)` groups as framed Arrow IPC streams. Empty
/// groups (no rows) are skipped, so a result with no blocks in range is zero
/// frames (zero bytes) — the framed equivalent of the JSON path's `[]`. `compress`
/// enables Arrow's built-in Zstd.
pub fn write_arrow_frames<W: Write>(
    mut writer: W,
    groups: &[(String, Vec<RecordBatch>)],
    compress: bool,
) -> Result<W> {
    for (name, batches) in groups {
        let Some(first) = batches.first() else {
            continue;
        };
        if batches.iter().all(|b| b.num_rows() == 0) {
            continue;
        }
        let schema = first.schema();

        let mut payload: Vec<u8> = Vec::new();
        {
            let mut options = IpcWriteOptions::default();
            if compress {
                options = options.try_with_compression(Some(CompressionType::ZSTD))?;
            }
            let mut sw = StreamWriter::try_new_with_options(&mut payload, &schema, options)?;
            for batch in batches {
                if batch.num_rows() > 0 {
                    sw.write(batch)?;
                }
            }
            sw.finish()?;
        }

        let name_bytes = name.as_bytes();
        writer.write_all(&(name_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(name_bytes)?;
        writer.write_all(&(payload.len() as u32).to_le_bytes())?;
        writer.write_all(&payload)?;
    }
    Ok(writer)
}
