use crate::engine_err;
use crate::error::ErrorKind;
use crate::integers::{is_integer, IntColumn};
use anyhow::Result;
use arrow::array::*;
use arrow::compute;
use arrow::datatypes::SchemaRef;
use rustc_hash::FxHashSet as HashSet;
use std::borrow::Borrow;

/// A composite key for joining on multiple columns.
#[derive(Clone, Eq, PartialEq, Hash)]
struct CompositeKey(Vec<u8>);

impl Borrow<[u8]> for CompositeKey {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

/// Resolve column names to column indices in a batch schema.
fn resolve_key_indices(schema: &SchemaRef, key_columns: &[&str]) -> Result<Vec<usize>> {
    key_columns
        .iter()
        .map(|name| {
            schema.index_of(name).map_err(|_| {
                engine_err!(
                    ErrorKind::ColumnNotFound,
                    "key column '{}' not found in schema",
                    name
                )
            })
        })
        .collect()
}

/// A typed key extractor that avoids per-row downcast checks.
enum TypedExtractor {
    /// Any integer width, written eight bytes wide.
    Int(usize),
    Utf8(usize),
    Boolean(usize),
    FixedBinary(usize),
    /// A list of integers, each element written eight bytes wide.
    IntList(usize),
}

impl TypedExtractor {
    fn new(batch: &RecordBatch, col_idx: usize) -> Result<Self> {
        let dt = batch.column(col_idx).data_type();
        Ok(match dt {
            _ if is_integer(dt) => Self::Int(col_idx),
            arrow::datatypes::DataType::Utf8 => Self::Utf8(col_idx),
            arrow::datatypes::DataType::Boolean => Self::Boolean(col_idx),
            arrow::datatypes::DataType::FixedSizeBinary(_) => Self::FixedBinary(col_idx),
            arrow::datatypes::DataType::List(field) if is_integer(field.data_type()) => {
                Self::IntList(col_idx)
            }
            arrow::datatypes::DataType::List(field) => {
                return Err(engine_err!(
                    ErrorKind::UnsupportedKeyType,
                    "unsupported list element type for join key: {:?}",
                    field.data_type()
                ))
            }
            dt => {
                return Err(engine_err!(
                    ErrorKind::UnsupportedKeyType,
                    "unsupported join key column type: {:?}",
                    dt
                ))
            }
        })
    }

    #[inline]
    fn col_idx(&self) -> usize {
        match self {
            Self::Int(i)
            | Self::Utf8(i)
            | Self::Boolean(i)
            | Self::FixedBinary(i)
            | Self::IntList(i) => *i,
        }
    }

    /// Append this column's value at `row` to `buf`. Returns false if null.
    #[inline]
    fn append(&self, buf: &mut Vec<u8>, batch: &RecordBatch, row: usize) -> bool {
        let col = batch.column(self.col_idx());
        if col.is_null(row) {
            return false;
        }

        match self {
            Self::Int(_) => {
                let ints = IntColumn::resolve(col.as_ref()).expect("`is_integer` admitted it");
                buf.extend_from_slice(&ints.join_key(row).to_le_bytes());
            }
            Self::Utf8(_) => {
                let a = col.as_any().downcast_ref::<StringArray>().unwrap();
                let s = a.value(row);
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Self::Boolean(_) => {
                let a = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                buf.push(a.value(row) as u8);
            }
            Self::FixedBinary(_) => {
                let a = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                buf.extend_from_slice(a.value(row));
            }
            Self::IntList(_) => {
                let a = col
                    .as_any()
                    .downcast_ref::<GenericListArray<i32>>()
                    .unwrap();
                let values = a.value(row);
                buf.extend_from_slice(&(values.len() as u32).to_le_bytes());

                let elements =
                    IntColumn::resolve(values.as_ref()).expect("`is_integer` admitted it");
                for j in 0..elements.len() {
                    buf.extend_from_slice(&elements.join_key(j).to_le_bytes());
                }
            }
        }
        true
    }
}

/// Write a composite key into `buf`. Returns false if any column is null (row skipped).
#[inline]
fn write_key(
    buf: &mut Vec<u8>,
    batch: &RecordBatch,
    row: usize,
    extractors: &[TypedExtractor],
) -> bool {
    buf.clear();
    for ext in extractors {
        if !ext.append(buf, batch, row) {
            return false;
        }
    }
    true
}

/// Build a hash set of composite keys from batches. Rows with any null key are skipped.
fn build_key_set(batches: &[RecordBatch], key_columns: &[&str]) -> Result<HashSet<CompositeKey>> {
    let mut set = HashSet::default();
    let mut buf = Vec::with_capacity(key_columns.len() * 8);
    for batch in batches {
        let indices = resolve_key_indices(batch.schema_ref(), key_columns)?;
        let extractors: Vec<TypedExtractor> = indices
            .iter()
            .map(|&i| TypedExtractor::new(batch, i))
            .collect::<Result<_>>()?;
        for row in 0..batch.num_rows() {
            if write_key(&mut buf, batch, row, &extractors) {
                set.insert(CompositeKey(buf.clone()));
            }
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

    // Probe phase: reuse scratch buffer, null keys → no match
    let mut result = Vec::new();
    let mut buf = Vec::with_capacity(probe_key.len() * 8);
    for batch in probe_batches {
        let indices = resolve_key_indices(batch.schema_ref(), probe_key)?;
        let extractors: Vec<TypedExtractor> = indices
            .iter()
            .map(|&i| TypedExtractor::new(batch, i))
            .collect::<Result<_>>()?;
        let mut matches = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if write_key(&mut buf, batch, row, &extractors) {
                matches.push(key_set.contains(buf.as_slice()));
            } else {
                matches.push(false); // null key → no match
            }
        }
        let mask = BooleanArray::from(matches);
        let tc = mask.true_count();
        if tc == 0 {
            continue;
        }
        if tc == batch.num_rows() {
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
    use std::sync::Arc;

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

    /// Covers CT-4 · INV-E7
    #[test]
    fn test_semi_join_unsupported_key_type() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![1.0, 2.0]))])
                .unwrap();

        let rows = [batch];
        let err = semi_join(&rows, &["x"], &rows, &["x"]);
        assert!(err.is_err(), "Float64 key should be rejected");
        assert!(
            err.unwrap_err().to_string().contains("unsupported"),
            "error should mention unsupported type"
        );
    }

    /// NULL join keys must not collide with real zero values.
    ///
    /// Covers CT-4 · INV-R5
    #[test]
    fn test_semi_join_null_key_no_false_match() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt32, true), // nullable
            Field::new("data", DataType::Utf8, false),
        ]));

        // Build side: key=0 (real zero)
        let build = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![Some(0)])),
                Arc::new(StringArray::from(vec!["zero"])),
            ],
        )
        .unwrap();

        // Probe side: key=NULL, key=0
        let probe = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(vec![None, Some(0)])),
                Arc::new(StringArray::from(vec!["null_row", "zero_row"])),
            ],
        )
        .unwrap();

        let result = semi_join(&[build], &["key"], &[probe], &["key"]).unwrap();
        let total: usize = result.iter().map(|b| b.num_rows()).sum();

        // Only key=0 should match, not key=NULL
        assert_eq!(total, 1, "NULL key must not match real 0");
        let data = result[0]
            .column_by_name("data")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(data.value(0), "zero_row");
    }

    /// NULL=NULL must NOT match (SQL semantics: NULL is unknown, not equal to anything).
    ///
    /// Covers CT-4 · INV-R5
    #[test]
    fn test_semi_join_null_null_no_match() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt32, true),
            Field::new("data", DataType::Utf8, false),
        ]));

        // Build side: key=NULL
        let build = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![None::<u32>])),
                Arc::new(StringArray::from(vec!["build_null"])),
            ],
        )
        .unwrap();

        // Probe side: key=NULL
        let probe = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(vec![None::<u32>])),
                Arc::new(StringArray::from(vec!["probe_null"])),
            ],
        )
        .unwrap();

        let result = semi_join(&[build], &["key"], &[probe], &["key"]).unwrap();
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0, "NULL=NULL must not match");
    }

    #[test]
    fn test_semi_join_list_uint16_keys() {
        // Regression: List<UInt16> was accepted by TypedExtractor but append()
        // only handled UInt32/Int32 elements, writing only the list length.
        // Two rows with different UInt16 values but equal list length would
        // produce identical keys → false join matches.
        let list_field = Arc::new(Field::new("item", DataType::UInt16, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("addr", DataType::List(list_field.clone()), false),
            Field::new("data", DataType::Utf8, false),
        ]));

        let mut build_list =
            ListBuilder::new(UInt16Builder::new()).with_field((*list_field).clone());
        // Build side: one row with addr=[10, 20]
        build_list.values().append_value(10u16);
        build_list.values().append_value(20u16);
        build_list.append(true);

        let build = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(build_list.finish()),
                Arc::new(StringArray::from(vec!["build"])),
            ],
        )
        .unwrap();

        let mut probe_list =
            ListBuilder::new(UInt16Builder::new()).with_field((*list_field).clone());
        // Probe row 0: [10, 20] — should match
        probe_list.values().append_value(10u16);
        probe_list.values().append_value(20u16);
        probe_list.append(true);
        // Probe row 1: [99, 88] — same length but different values, must NOT match
        probe_list.values().append_value(99u16);
        probe_list.values().append_value(88u16);
        probe_list.append(true);

        let probe = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(probe_list.finish()),
                Arc::new(StringArray::from(vec!["match", "no_match"])),
            ],
        )
        .unwrap();

        let result = semi_join(&[build], &["addr"], &[probe], &["addr"]).unwrap();
        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "only [10,20] should match, not [99,88]");

        let data = result[0]
            .column_by_name("data")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(data.value(0), "match");
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_semi_join_with_real_data() {
        if !crate::testing::chunks_present() {
            return;
        }

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
