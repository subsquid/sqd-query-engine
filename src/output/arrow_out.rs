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
//!   base58/other `Utf8` columns are left untouched). An odd digit count — an EVM
//!   quantity such as `0x0` — decodes as if it had a leading zero. ~2× smaller
//!   raw, ~20-30% smaller after zstd, ~100× faster client decode, at the cost of
//!   a decode pass.
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
    if batch.column_by_name(bn_col).is_none() {
        return Ok(batch.clone());
    }

    let mask = blocks_mask(batch, bn_col, keep)?;
    Ok(filter_record_batch(batch, &mask)?)
}

/// The rows whose `bn_col` block number satisfies `keep`; every row of a batch
/// without that column.
pub fn blocks_mask(
    batch: &RecordBatch,
    bn_col: &str,
    keep: impl Fn(u64) -> bool,
) -> Result<BooleanArray> {
    let Some(col) = batch.column_by_name(bn_col) else {
        return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
    };

    let blocks = BlockNumbers::resolve(col.as_ref(), bn_col)?;
    Ok((0..blocks.len())
        .map(|i| Some(keep(blocks.at(i))))
        .collect())
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
/// A value that is not hex is an error: a null there would be a different
/// answer from the JSON one, which renders the same text verbatim (INV-O14).
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
        let mut buf = Vec::new();
        for r in 0..sa.len() {
            if sa.is_null(r) {
                b.append_null();
                continue;
            }

            let v = sa.value(r);
            if decode_hex(v, &mut buf).is_err() {
                // Hex columns include calldata, so the value can be megabytes.
                let head: String = v.chars().take(40).collect();
                crate::engine_bail!(
                    crate::error::ErrorKind::MalformedChunkData,
                    "column '{}' is declared hex but holds {head:?}",
                    field.name()
                );
            }
            b.append_value(&buf);
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

/// Decode `0x…` text into `out`, replacing its contents.
///
/// An odd digit count is a quantity in minimal form (`0x0`, `0x3938700`), so it
/// reads as if it had a leading zero: the bytes are the same number, big-endian.
fn decode_hex(text: &str, out: &mut Vec<u8>) -> Result<(), faster_hex::Error> {
    let digits = text.strip_prefix("0x").unwrap_or(text).as_bytes();
    let odd = digits.len() % 2 == 1;

    out.clear();
    out.resize(digits.len().div_ceil(2), 0);

    if !odd {
        return faster_hex::hex_decode(digits, out);
    }

    faster_hex::hex_decode(&[b'0', digits[0]], &mut out[..1])?;
    faster_hex::hex_decode(&digits[1..], &mut out[1..])
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::BinaryArray;

    fn decoded(text: &str) -> Vec<u8> {
        let mut out = vec![0xff; 7];
        decode_hex(text, &mut out).unwrap();
        out
    }

    fn hexified(values: Vec<Option<&str>>) -> Result<Vec<Option<Vec<u8>>>> {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))])?;
        let out = hexify_batch(&batch, &HashSet::from([0]))?;

        let column = out
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        Ok(column.iter().map(|v| v.map(<[u8]>::to_vec)).collect())
    }

    /// Covers CT-6 · INV-O14
    #[test]
    fn an_odd_digit_count_decodes_with_a_leading_zero() {
        assert_eq!(decoded("0x0"), [0x00]);
        assert_eq!(decoded("0x1"), [0x01]);
        assert_eq!(decoded("0xf"), [0x0f]);
        assert_eq!(decoded("0x3938700"), [0x03, 0x93, 0x87, 0x00]);
        assert_eq!(decoded("0x123"), [0x01, 0x23]);
        assert_eq!(decoded("0xABC"), [0x0a, 0xbc]);
    }

    #[test]
    fn an_even_digit_count_decodes_unchanged() {
        assert_eq!(decoded("0x"), Vec::<u8>::new());
        assert_eq!(decoded(""), Vec::<u8>::new());
        assert_eq!(decoded("0x00"), [0x00]);
        assert_eq!(decoded("0xdeadbeef"), [0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(decoded("deadbeef"), [0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn a_value_that_is_not_hex_does_not_decode() {
        let mut out = Vec::new();
        for text in ["0xg", "0x0g", "0xzz", "0X12", "0x 1", "0x1-"] {
            assert!(
                decode_hex(text, &mut out).is_err(),
                "{text:?} must not decode"
            );
        }
    }

    /// The number a quantity names survives the trip: big-endian bytes read back
    /// as the value the JSON rendering states.
    ///
    /// Covers CT-6 · INV-O14
    #[test]
    fn a_decoded_quantity_is_the_same_number() {
        for value in [0u64, 1, 15, 16, 255, 256, 60_000_000, 30_000_000, u64::MAX] {
            let text = format!("{value:#x}");
            let bytes = decoded(&text);

            let mut word = [0u8; 8];
            word[8 - bytes.len()..].copy_from_slice(&bytes);
            assert_eq!(u64::from_be_bytes(word), value, "{text}");
        }
    }

    /// A row the decoder cannot read used to become null, which a client cannot
    /// tell from a value the chain left unset.
    ///
    /// Covers CT-6 · INV-O14
    #[test]
    fn hexify_keeps_every_row_and_only_the_nulls_null() {
        let rows = hexified(vec![
            Some("0x0"),
            None,
            Some("0x3938700"),
            Some("0x"),
            Some("0xdeadbeef"),
        ])
        .unwrap();

        assert_eq!(
            rows,
            [
                Some(vec![0x00]),
                None,
                Some(vec![0x03, 0x93, 0x87, 0x00]),
                Some(vec![]),
                Some(vec![0xde, 0xad, 0xbe, 0xef]),
            ]
        );
    }

    /// Covers CT-6 · INV-O14
    #[test]
    fn hexify_refuses_a_value_that_is_not_hex() {
        let err = hexified(vec![Some("0x00"), Some("0xnot-hex")]).unwrap_err();

        assert_eq!(
            crate::error::error_kind(&err),
            Some(crate::error::ErrorKind::MalformedChunkData)
        );
        assert!(err.to_string().contains("value"), "names the column: {err}");
    }
}
