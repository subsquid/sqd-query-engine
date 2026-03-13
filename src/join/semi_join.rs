use anyhow::{anyhow, Result};
use arrow::array::*;
use arrow::compute;
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::sync::Arc;

/// A composite key for joining on multiple columns.
/// Stores a hash + row data for equality comparison.
#[derive(Clone, Eq, PartialEq, Hash)]
struct CompositeKey(Vec<u8>);

/// Append a single value from an array to the key buffer (fallback for unknown types).
fn append_value_to_key(buf: &mut Vec<u8>, array: &Arc<dyn Array>, row: usize) {
    if array.is_null(row) {
        buf.push(0);
        return;
    }
    buf.push(1);
    buf.extend_from_slice(&(row as u64).to_le_bytes());
}

/// Resolve column names to column indices in a batch schema.
fn resolve_key_indices(schema: &SchemaRef, key_columns: &[&str]) -> Result<Vec<usize>> {
    key_columns
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|_| anyhow!("key column '{}' not found in schema", name))
        })
        .collect()
}

/// A typed key extractor that avoids per-row downcast checks.
enum TypedExtractor {
    UInt32(usize),
    UInt64(usize),
    Int32(usize),
    Int64(usize),
    Utf8(usize),
    UInt16(usize),
    UInt8(usize),
    Boolean(usize),
    FixedBinary(usize),
    ListInt32(usize),
    Fallback(usize),
}

impl TypedExtractor {
    fn new(batch: &RecordBatch, col_idx: usize) -> Self {
        let col = batch.column(col_idx);
        let dt = col.data_type();
        match dt {
            arrow::datatypes::DataType::UInt32 => Self::UInt32(col_idx),
            arrow::datatypes::DataType::UInt64 => Self::UInt64(col_idx),
            arrow::datatypes::DataType::Int32 => Self::Int32(col_idx),
            arrow::datatypes::DataType::Int64 => Self::Int64(col_idx),
            arrow::datatypes::DataType::Utf8 => Self::Utf8(col_idx),
            arrow::datatypes::DataType::UInt16 => Self::UInt16(col_idx),
            arrow::datatypes::DataType::UInt8 => Self::UInt8(col_idx),
            arrow::datatypes::DataType::Boolean => Self::Boolean(col_idx),
            arrow::datatypes::DataType::FixedSizeBinary(_) => Self::FixedBinary(col_idx),
            arrow::datatypes::DataType::List(_) => Self::ListInt32(col_idx),
            _ => Self::Fallback(col_idx),
        }
    }

    #[inline]
    fn append(&self, buf: &mut Vec<u8>, batch: &RecordBatch, row: usize) {
        match self {
            Self::UInt32(i) => {
                let a = batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap();
                buf.extend_from_slice(&a.value(row).to_le_bytes());
            }
            Self::UInt64(i) => {
                let a = batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                buf.extend_from_slice(&a.value(row).to_le_bytes());
            }
            Self::Int32(i) => {
                let a = batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                buf.extend_from_slice(&a.value(row).to_le_bytes());
            }
            Self::Int64(i) => {
                let a = batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                buf.extend_from_slice(&a.value(row).to_le_bytes());
            }
            Self::Utf8(i) => {
                let a = batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let s = a.value(row);
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Self::UInt16(i) => {
                let a = batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .unwrap();
                buf.extend_from_slice(&a.value(row).to_le_bytes());
            }
            Self::UInt8(i) => {
                let a = batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap();
                buf.push(a.value(row));
            }
            Self::Boolean(i) => {
                let a = batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                buf.push(a.value(row) as u8);
            }
            Self::FixedBinary(i) => {
                let a = batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                buf.extend_from_slice(a.value(row));
            }
            Self::ListInt32(i) => {
                let a = batch
                    .column(*i)
                    .as_any()
                    .downcast_ref::<GenericListArray<i32>>()
                    .unwrap();
                let values = a.value(row);
                let len = values.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                if let Some(arr) = values.as_any().downcast_ref::<UInt32Array>() {
                    for j in 0..arr.len() {
                        buf.extend_from_slice(&arr.value(j).to_le_bytes());
                    }
                } else if let Some(arr) = values.as_any().downcast_ref::<Int32Array>() {
                    for j in 0..arr.len() {
                        buf.extend_from_slice(&arr.value(j).to_le_bytes());
                    }
                }
            }
            Self::Fallback(i) => {
                append_value_to_key(buf, batch.column(*i), row);
            }
        }
    }
}

/// Extract a composite key using typed extractors (no per-row downcast).
fn extract_key_typed(
    batch: &RecordBatch,
    row: usize,
    extractors: &[TypedExtractor],
) -> CompositeKey {
    let mut buf = Vec::with_capacity(extractors.len() * 8);
    for ext in extractors {
        ext.append(&mut buf, batch, row);
    }
    CompositeKey(buf)
}

/// Build a hash set of composite keys from batches.
fn build_key_set(batches: &[RecordBatch], key_columns: &[&str]) -> Result<HashSet<CompositeKey>> {
    let mut set = HashSet::new();
    for batch in batches {
        let indices = resolve_key_indices(batch.schema_ref(), key_columns)?;
        let extractors: Vec<TypedExtractor> = indices
            .iter()
            .map(|&i| TypedExtractor::new(batch, i))
            .collect();
        for row in 0..batch.num_rows() {
            set.insert(extract_key_typed(batch, row, &extractors));
        }
    }
    Ok(set)
}

/// Semi-join: filter `probe_batches` to only rows whose key columns match
/// keys present in `build_batches`.
///
/// - `build_batches`: the "right side" — we build a hash set from these
/// - `build_key`: column names on the build side
/// - `probe_batches`: the "left side" — we filter these
/// - `probe_key`: column names on the probe side
///
/// Returns filtered probe batches with only matching rows.
pub fn semi_join(
    build_batches: &[RecordBatch],
    build_key: &[&str],
    probe_batches: &[RecordBatch],
    probe_key: &[&str],
) -> Result<Vec<RecordBatch>> {
    if build_batches.is_empty() || probe_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Build phase: hash set of keys from the build side
    let key_set = build_key_set(build_batches, build_key)?;
    if key_set.is_empty() {
        return Ok(Vec::new());
    }

    // Probe phase: filter probe batches using typed extractors
    let mut result = Vec::new();
    for batch in probe_batches {
        let indices = resolve_key_indices(batch.schema_ref(), probe_key)?;
        let extractors: Vec<TypedExtractor> = indices
            .iter()
            .map(|&i| TypedExtractor::new(batch, i))
            .collect();
        let mut matches = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = extract_key_typed(batch, row, &extractors);
            matches.push(key_set.contains(&key));
        }
        let mask = BooleanArray::from(matches);
        if mask.true_count() == 0 {
            continue;
        }
        if mask.true_count() == batch.num_rows() {
            result.push(batch.clone());
        } else {
            result.push(compute::filter_record_batch(batch, &mask)?);
        }
    }

    Ok(result)
}

/// Lookup join: for each row in `input_batches`, find matching rows in `lookup_batches`
/// and return them. This is the "other direction" of semi-join — returns rows from
/// the lookup side that have matching keys in the input side.
///
/// - `input_batches`: rows that drive the lookup
/// - `input_key`: key columns on the input side
/// - `lookup_batches`: rows to search through
/// - `lookup_key`: key columns on the lookup side
///
/// Returns rows from `lookup_batches` that match keys in `input_batches`.
pub fn lookup_join(
    input_batches: &[RecordBatch],
    input_key: &[&str],
    lookup_batches: &[RecordBatch],
    lookup_key: &[&str],
) -> Result<Vec<RecordBatch>> {
    // Same as semi_join but reversed: build from input, probe on lookup
    semi_join(input_batches, input_key, lookup_batches, lookup_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(block_numbers: Vec<u64>, tx_indices: Vec<u32>, extra: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(block_numbers)),
                Arc::new(UInt32Array::from(tx_indices)),
                Arc::new(StringArray::from(extra)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_semi_join_basic() {
        // Build side: transactions (block_number=1, tx_index=0) and (1, 1)
        let build = vec![make_batch(vec![1, 1], vec![0, 1], vec!["tx0", "tx1"])];
        // Probe side: logs for (1, 0), (1, 1), (1, 2), (2, 0)
        let probe = vec![make_batch(
            vec![1, 1, 1, 2],
            vec![0, 1, 2, 0],
            vec!["log0", "log1", "log2", "log3"],
        )];

        let result = semi_join(
            &build,
            &["block_number", "transaction_index"],
            &probe,
            &["block_number", "transaction_index"],
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2); // Only (1,0) and (1,1) match

        let values = result[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "log0");
        assert_eq!(values.value(1), "log1");
    }

    #[test]
    fn test_semi_join_no_matches() {
        let build = vec![make_batch(vec![1], vec![0], vec!["tx0"])];
        let probe = vec![make_batch(vec![2], vec![0], vec!["log0"])];

        let result = semi_join(
            &build,
            &["block_number", "transaction_index"],
            &probe,
            &["block_number", "transaction_index"],
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_semi_join_all_match() {
        let build = vec![make_batch(vec![1, 1], vec![0, 1], vec!["tx0", "tx1"])];
        let probe = vec![make_batch(vec![1, 1], vec![0, 1], vec!["log0", "log1"])];

        let result = semi_join(
            &build,
            &["block_number", "transaction_index"],
            &probe,
            &["block_number", "transaction_index"],
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn test_lookup_join() {
        // Input: filtered instructions with (block_number=1, tx_index=0)
        let input = vec![make_batch(vec![1], vec![0], vec!["instr0"])];
        // Lookup: transactions table
        let lookup = vec![make_batch(
            vec![1, 1, 2],
            vec![0, 1, 0],
            vec!["tx0", "tx1", "tx2"],
        )];

        let result = lookup_join(
            &input,
            &["block_number", "transaction_index"],
            &lookup,
            &["block_number", "transaction_index"],
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1); // Only tx0 matches

        let values = result[0]
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "tx0");
    }

    #[test]
    fn test_semi_join_empty_inputs() {
        let empty: Vec<RecordBatch> = vec![];
        let build = vec![make_batch(vec![1], vec![0], vec!["tx0"])];

        assert_eq!(
            semi_join(&empty, &["block_number"], &build, &["block_number"])
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            semi_join(&build, &["block_number"], &empty, &["block_number"])
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn test_semi_join_string_keys() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("data", DataType::UInt32, false),
        ]));

        let build = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(UInt32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let probe = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["alice", "charlie", "bob", "dave"])),
                Arc::new(UInt32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let result = semi_join(&[build], &["id"], &[probe], &["id"]).unwrap();
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2); // alice and bob
    }

    #[test]
    fn test_semi_join_with_real_data() {
        // Test against real Solana data
        use crate::scan::ParquetTable;
        use std::path::Path;

        let chunk = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk");
        let instructions = ParquetTable::open(&chunk.join("instructions.parquet")).unwrap();
        let transactions = ParquetTable::open(&chunk.join("transactions.parquet")).unwrap();

        // Read some instructions (whirlpool program)
        let instr_batches = instructions
            .read(
                &["block_number", "transaction_index", "program_id"],
                None,
                50000,
            )
            .unwrap();

        // Filter to whirlpool
        let mut filtered_instr = Vec::new();
        for batch in &instr_batches {
            let program_id = batch
                .column_by_name("program_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let mask: BooleanArray = (0..program_id.len())
                .map(|i| {
                    Some(
                        !program_id.is_null(i)
                            && program_id.value(i) == "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
                    )
                })
                .collect();
            if mask.true_count() > 0 {
                filtered_instr.push(compute::filter_record_batch(batch, &mask).unwrap());
            }
        }

        if filtered_instr.is_empty() {
            return; // No whirlpool data in this chunk
        }

        // Read all transactions
        let tx_batches = transactions
            .read(
                &["block_number", "transaction_index", "fee_payer"],
                None,
                50000,
            )
            .unwrap();

        // Lookup join: find transactions for matching instructions
        let result = lookup_join(
            &filtered_instr,
            &["block_number", "transaction_index"],
            &tx_batches,
            &["block_number", "transaction_index"],
        )
        .unwrap();

        let matched_txs: usize = result.iter().map(|b| b.num_rows()).sum();
        let total_txs: usize = tx_batches.iter().map(|b| b.num_rows()).sum();

        assert!(matched_txs > 0, "should match some transactions");
        assert!(matched_txs < total_txs, "should not match all transactions");
    }
}
