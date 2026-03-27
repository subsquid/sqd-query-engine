use anyhow::{anyhow, Result};
use arrow::array::*;
use arrow::compute;
use arrow::datatypes::SchemaRef;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};

/// Extract a List value as a Vec<u32> from a row.
/// Supports List<UInt16>, List<UInt32>, and List<Int32>.
fn extract_address(array: &GenericListArray<i32>, row: usize) -> Vec<u32> {
    if array.is_null(row) {
        return Vec::new();
    }
    let values = array.value(row);
    if let Some(arr) = values.as_any().downcast_ref::<UInt16Array>() {
        (0..arr.len()).map(|i| arr.value(i) as u32).collect()
    } else if let Some(arr) = values.as_any().downcast_ref::<UInt32Array>() {
        (0..arr.len()).map(|i| arr.value(i)).collect()
    } else if let Some(arr) = values.as_any().downcast_ref::<Int32Array>() {
        (0..arr.len())
            .filter_map(|i| u32::try_from(arr.value(i)).ok())
            .collect()
    } else {
        Vec::new()
    }
}

/// Composite key for grouping by (block_number, transaction_index).
#[derive(Clone, Eq, PartialEq, Hash)]
struct GroupKey(Vec<u8>);

impl Borrow<[u8]> for GroupKey {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

/// Typed group key extractor — downcasts once per batch.
enum GroupKeyCol<'a> {
    UInt64(&'a UInt64Array),
    UInt32(&'a UInt32Array),
    UInt16(&'a UInt16Array),
    Int32(&'a Int32Array),
}

impl<'a> GroupKeyCol<'a> {
    fn resolve(batch: &'a RecordBatch, idx: usize) -> Result<Self> {
        let col = batch.column(idx);
        if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
            Ok(Self::UInt64(a))
        } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
            Ok(Self::UInt32(a))
        } else if let Some(a) = col.as_any().downcast_ref::<UInt16Array>() {
            Ok(Self::UInt16(a))
        } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            Ok(Self::Int32(a))
        } else {
            Err(anyhow!(
                "unsupported group key column type: {:?}",
                col.data_type()
            ))
        }
    }

    /// Append this column's value at `row` to `buf`. Returns false if null.
    #[inline]
    fn append(&self, row: usize, buf: &mut Vec<u8>) -> bool {
        match self {
            Self::UInt64(a) => {
                if a.is_null(row) { return false; }
                buf.extend_from_slice(&a.value(row).to_le_bytes());
            }
            Self::UInt32(a) => {
                if a.is_null(row) { return false; }
                buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes());
            }
            Self::UInt16(a) => {
                if a.is_null(row) { return false; }
                buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes());
            }
            Self::Int32(a) => {
                if a.is_null(row) { return false; }
                buf.extend_from_slice(&((a.value(row) as u32) as u64).to_le_bytes());
            }
        }
        true
    }
}

/// Write group key into `buf`, returns false if any column is null.
#[inline]
fn write_group_key(cols: &[GroupKeyCol], row: usize, buf: &mut Vec<u8>) -> bool {
    buf.clear();
    for col in cols {
        if !col.append(row, buf) {
            return false;
        }
    }
    true
}

fn resolve_indices(schema: &SchemaRef, columns: &[&str]) -> Result<Vec<usize>> {
    columns
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|_| anyhow!("column '{}' not found in schema", name))
        })
        .collect()
}

/// Find all children of the given rows. A child is a row in `target_batches` whose
/// address column is a prefix extension of an address in `source_batches`.
///
/// For example, if a source row has `instruction_address = [0]`, then target rows
/// with `[0, 0]`, `[0, 1]`, `[0, 0, 3]` etc. are children.
///
/// - `group_key_columns`: columns that group rows (e.g., ["block_number", "transaction_index"])
/// - `source_address_column`: address column in source batches (e.g., "address")
/// - `target_address_column`: address column in target batches (e.g., "call_address")
/// - `inclusive`: controls whether equal-depth addresses count as a match.
///   When `false` (self-join, e.g. calls→calls), only strictly deeper addresses match:
///   a call at `[0]` does NOT match itself, only `[0, *]`.
///   When `true` (cross-table, e.g. calls→events), same-depth addresses also match:
///   an event with `call_address=[0]` IS considered a child of a call with `address=[0]`,
///   because the event was emitted by that call.
pub fn find_children(
    source_batches: &[RecordBatch],
    target_batches: &[RecordBatch],
    group_key_columns: &[&str],
    source_address_column: &str,
    target_address_column: &str,
    inclusive: bool,
) -> Result<Vec<RecordBatch>> {
    if source_batches.is_empty() || target_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Build an index: group_key -> set of source addresses
    let mut source_addresses: HashMap<GroupKey, HashSet<Vec<u32>>> = HashMap::new();

    let mut buf = Vec::with_capacity(group_key_columns.len() * 8);
    for batch in source_batches {
        let key_indices = resolve_indices(batch.schema_ref(), group_key_columns)?;
        let key_cols: Vec<GroupKeyCol> = key_indices.iter().map(|&i| GroupKeyCol::resolve(batch, i)).collect::<Result<_>>()?;
        let addr_idx = batch
            .schema()
            .index_of(source_address_column)
            .map_err(|_| anyhow!("column '{}' not found", source_address_column))?;
        let addr_array = batch
            .column(addr_idx)
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .ok_or_else(|| anyhow!("'{}' must be a List<UInt32> column", source_address_column))?;

        for row in 0..batch.num_rows() {
            if addr_array.is_null(row) {
                continue;
            }
            if !write_group_key(&key_cols, row, &mut buf) {
                continue;
            }
            let addr = extract_address(addr_array, row);
            source_addresses.entry(GroupKey(buf.clone())).or_default().insert(addr);
        }
    }

    // Probe target batches: keep rows whose address is a child of any source address
    let mut result = Vec::new();
    for batch in target_batches {
        let key_indices = resolve_indices(batch.schema_ref(), group_key_columns)?;
        let key_cols: Vec<GroupKeyCol> = key_indices.iter().map(|&i| GroupKeyCol::resolve(batch, i)).collect::<Result<_>>()?;
        let addr_idx = batch
            .schema()
            .index_of(target_address_column)
            .map_err(|_| anyhow!("column '{}' not found", target_address_column))?;
        let addr_array = batch
            .column(addr_idx)
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .ok_or_else(|| anyhow!("'{}' must be a List<UInt32> column", target_address_column))?;

        let mut matches = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if addr_array.is_null(row) {
                matches.push(false);
                continue;
            }
            if !write_group_key(&key_cols, row, &mut buf) {
                matches.push(false);
                continue;
            }
            let target_addr = extract_address(addr_array, row);

            let is_child = source_addresses
                .get(buf.as_slice())
                .map(|addrs| {
                    addrs.iter().any(|parent| {
                        if inclusive {
                            target_addr.len() >= parent.len()
                                && target_addr[..parent.len()] == parent[..]
                        } else {
                            target_addr.len() > parent.len()
                                && target_addr[..parent.len()] == parent[..]
                        }
                    })
                })
                .unwrap_or(false);
            matches.push(is_child);
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

/// Find all parents (ancestors) of the given rows. A parent is a row in `target_batches`
/// whose address is a prefix of an address in `source_batches`.
///
/// For example, if source has `instruction_address = [0, 1, 2]`, then target rows with
/// `[0]` and `[0, 1]` are parents.
///
/// - `source_address_column`: address column in source batches
/// - `target_address_column`: address column in target batches
/// - `inclusive`: same semantics as in `find_children` — when `true`, a target row
///   at the same depth as a source row counts as a parent (cross-table case).
pub fn find_parents(
    source_batches: &[RecordBatch],
    target_batches: &[RecordBatch],
    group_key_columns: &[&str],
    source_address_column: &str,
    target_address_column: &str,
    inclusive: bool,
) -> Result<Vec<RecordBatch>> {
    if source_batches.is_empty() || target_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Build index: group_key -> set of source addresses
    let mut source_addresses: HashMap<GroupKey, HashSet<Vec<u32>>> = HashMap::new();
    let mut buf = Vec::with_capacity(group_key_columns.len() * 8);

    for batch in source_batches {
        let key_indices = resolve_indices(batch.schema_ref(), group_key_columns)?;
        let key_cols: Vec<GroupKeyCol> = key_indices.iter().map(|&i| GroupKeyCol::resolve(batch, i)).collect::<Result<_>>()?;
        let addr_idx = batch
            .schema()
            .index_of(source_address_column)
            .map_err(|_| anyhow!("column '{}' not found", source_address_column))?;
        let addr_array = batch
            .column(addr_idx)
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .ok_or_else(|| anyhow!("'{}' must be a List<UInt32> column", source_address_column))?;

        for row in 0..batch.num_rows() {
            if addr_array.is_null(row) {
                continue;
            }
            if !write_group_key(&key_cols, row, &mut buf) {
                continue;
            }
            let addr = extract_address(addr_array, row);
            source_addresses.entry(GroupKey(buf.clone())).or_default().insert(addr);
        }
    }

    // Probe: keep rows whose address is a prefix of any source address
    let mut result = Vec::new();
    for batch in target_batches {
        let key_indices = resolve_indices(batch.schema_ref(), group_key_columns)?;
        let key_cols: Vec<GroupKeyCol> = key_indices.iter().map(|&i| GroupKeyCol::resolve(batch, i)).collect::<Result<_>>()?;
        let addr_idx = batch
            .schema()
            .index_of(target_address_column)
            .map_err(|_| anyhow!("column '{}' not found", target_address_column))?;
        let addr_array = batch
            .column(addr_idx)
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .ok_or_else(|| anyhow!("'{}' must be a List<UInt32> column", target_address_column))?;

        let mut matches = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if addr_array.is_null(row) {
                matches.push(false);
                continue;
            }
            if !write_group_key(&key_cols, row, &mut buf) {
                matches.push(false);
                continue;
            }
            let target_addr = extract_address(addr_array, row);

            let is_parent = source_addresses
                .get(buf.as_slice())
                .map(|addrs| {
                    addrs.iter().any(|child| {
                        if inclusive {
                            target_addr.len() <= child.len()
                                && child[..target_addr.len()] == target_addr[..]
                        } else {
                            target_addr.len() < child.len()
                                && child[..target_addr.len()] == target_addr[..]
                        }
                    })
                })
                .unwrap_or(false);
            matches.push(is_parent);
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_instruction_batch(
        block_numbers: Vec<u64>,
        tx_indices: Vec<u32>,
        addresses: Vec<Vec<u32>>,
        data: Vec<&str>,
    ) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new(
                "instruction_address",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                false,
            ),
            Field::new("data", DataType::Utf8, false),
        ]));

        let mut list_builder = ListBuilder::new(UInt32Builder::new()).with_field(Field::new(
            "item",
            DataType::UInt32,
            true,
        ));
        for addr in &addresses {
            for &v in addr {
                list_builder.values().append_value(v);
            }
            list_builder.append(true);
        }

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(block_numbers)),
                Arc::new(UInt32Array::from(tx_indices)),
                Arc::new(list_builder.finish()),
                Arc::new(StringArray::from(data)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_find_children_basic() {
        // Source: top-level instruction at [0]
        let source = vec![make_instruction_batch(
            vec![1],
            vec![0],
            vec![vec![0]],
            vec!["parent"],
        )];
        // Target: various instructions in same tx
        let target = vec![make_instruction_batch(
            vec![1, 1, 1, 1, 1],
            vec![0, 0, 0, 0, 0],
            vec![vec![0], vec![0, 0], vec![0, 1], vec![0, 0, 0], vec![1]],
            vec!["self", "child1", "child2", "grandchild", "sibling"],
        )];

        let result = find_children(
            &source,
            &target,
            &["block_number", "transaction_index"],
            "instruction_address",
            "instruction_address",
            false,
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3); // [0,0], [0,1], [0,0,0]

        let data = result[0]
            .column_by_name("data")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values: Vec<&str> = (0..data.len()).map(|i| data.value(i)).collect();
        assert_eq!(values, vec!["child1", "child2", "grandchild"]);
    }

    #[test]
    fn test_find_parents_basic() {
        // Source: deep instruction at [0, 1, 2]
        let source = vec![make_instruction_batch(
            vec![1],
            vec![0],
            vec![vec![0, 1, 2]],
            vec!["deep"],
        )];
        // Target: all instructions in same tx
        let target = vec![make_instruction_batch(
            vec![1, 1, 1, 1, 1],
            vec![0, 0, 0, 0, 0],
            vec![vec![], vec![0], vec![0, 1], vec![0, 1, 2], vec![0, 1, 2, 0]],
            vec!["root", "level1", "level2", "self", "child"],
        )];

        let result = find_parents(
            &source,
            &target,
            &["block_number", "transaction_index"],
            "instruction_address",
            "instruction_address",
            false,
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        // Parents of [0,1,2]: [] is prefix, [0] is prefix, [0,1] is prefix
        // [0,1,2] is NOT a parent (same length), [0,1,2,0] is NOT (longer)
        assert_eq!(total, 3);

        let data = result[0]
            .column_by_name("data")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values: Vec<&str> = (0..data.len()).map(|i| data.value(i)).collect();
        assert_eq!(values, vec!["root", "level1", "level2"]);
    }

    #[test]
    fn test_find_children_different_transactions() {
        // Source: instructions from two different transactions
        let source = vec![make_instruction_batch(
            vec![1, 1],
            vec![0, 1],
            vec![vec![0], vec![0]],
            vec!["tx0_parent", "tx1_parent"],
        )];
        // Target: children in both txs + unrelated
        let target = vec![make_instruction_batch(
            vec![1, 1, 1, 2],
            vec![0, 1, 1, 0],
            vec![vec![0, 0], vec![0, 0], vec![1], vec![0, 0]],
            vec!["tx0_child", "tx1_child", "tx1_sibling", "other_block"],
        )];

        let result = find_children(
            &source,
            &target,
            &["block_number", "transaction_index"],
            "instruction_address",
            "instruction_address",
            false,
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2); // tx0_child and tx1_child
    }

    #[test]
    fn test_find_children_empty_inputs() {
        let empty: Vec<RecordBatch> = vec![];
        let batch = vec![make_instruction_batch(
            vec![1],
            vec![0],
            vec![vec![0]],
            vec!["instr"],
        )];

        assert_eq!(
            find_children(
                &empty,
                &batch,
                &["block_number", "transaction_index"],
                "instruction_address",
                "instruction_address",
                false,
            )
            .unwrap()
            .len(),
            0
        );
        assert_eq!(
            find_children(
                &batch,
                &empty,
                &["block_number", "transaction_index"],
                "instruction_address",
                "instruction_address",
                false,
            )
            .unwrap()
            .len(),
            0
        );
    }

    #[test]
    fn test_find_children_no_matches() {
        let source = vec![make_instruction_batch(
            vec![1],
            vec![0],
            vec![vec![0]],
            vec!["parent"],
        )];
        let target = vec![make_instruction_batch(
            vec![1, 1],
            vec![0, 0],
            vec![vec![1, 0], vec![2]],
            vec!["not_child1", "not_child2"],
        )];

        let result = find_children(
            &source,
            &target,
            &["block_number", "transaction_index"],
            "instruction_address",
            "instruction_address",
            false,
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    /// Null source address must be skipped, not indexed as [] (which is a prefix
    /// of every address and would cause the entire transaction group to match).
    #[test]
    fn test_find_children_null_source_address_skipped() {
        // Source: one row with NULL instruction_address
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new(
                "instruction_address",
                DataType::List(Arc::new(Field::new("item", DataType::UInt32, true))),
                true, // nullable
            ),
            Field::new("data", DataType::Utf8, false),
        ]));

        let mut list_builder = ListBuilder::new(UInt32Builder::new())
            .with_field(Field::new("item", DataType::UInt32, true));
        list_builder.append_null(); // NULL address

        let source = vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(list_builder.finish()),
                Arc::new(StringArray::from(vec!["null_source"])),
            ],
        )
        .unwrap()];

        // Target: a real child at [0, 0]
        let target = vec![make_instruction_batch(
            vec![1],
            vec![0],
            vec![vec![0, 0]],
            vec!["child"],
        )];

        let result = find_children(
            &source,
            &target,
            &["block_number", "transaction_index"],
            "instruction_address",
            "instruction_address",
            false,
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0, "null source address must not match any target");
    }

    /// Negative Int32 address elements must be dropped, not wrap to huge u32.
    #[test]
    fn test_extract_address_negative_int32_dropped() {
        let list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("addr", DataType::List(list_field.clone()), false),
            Field::new("data", DataType::Utf8, false),
        ]));

        // Source: address [0, -1] — the -1 should be filtered out
        let mut src_list =
            ListBuilder::new(Int32Builder::new()).with_field((*list_field).clone());
        src_list.values().append_value(0i32);
        src_list.values().append_value(-1i32);
        src_list.append(true);

        let source = vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(src_list.finish()),
                Arc::new(StringArray::from(vec!["src"])),
            ],
        )
        .unwrap()];

        // Target: address [0] — would match [0, 4294967295] as child if -1 wrapped
        let mut tgt_list =
            ListBuilder::new(Int32Builder::new()).with_field((*list_field).clone());
        tgt_list.values().append_value(0i32);
        tgt_list.values().append_value(0i32);
        tgt_list.append(true);

        let target = vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(UInt32Array::from(vec![0])),
                Arc::new(tgt_list.finish()),
                Arc::new(StringArray::from(vec!["tgt"])),
            ],
        )
        .unwrap()];

        // Source addr is [0] (after dropping -1), target is [0, 0] → target IS a child of [0]
        let result = find_children(
            &source,
            &target,
            &["block_number", "transaction_index"],
            "addr",
            "addr",
            false,
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        // [0, 0] is a child of [0] — this should match
        assert_eq!(total, 1);
    }

    #[test]
    fn test_hierarchical_with_real_data() {
        use crate::scan::ParquetTable;
        use std::path::Path;

        let chunk = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk");
        let instructions = ParquetTable::open(&chunk.join("instructions.parquet")).unwrap();

        // Read all instructions with address info
        let all_batches = instructions
            .read(
                &[
                    "block_number",
                    "transaction_index",
                    "instruction_address",
                    "program_id",
                ],
                None,
                50000,
            )
            .unwrap();

        // Filter to whirlpool as source
        let mut source = Vec::new();
        for batch in &all_batches {
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
                source.push(compute::filter_record_batch(batch, &mask).unwrap());
            }
        }

        if source.is_empty() {
            return;
        }

        let source_count: usize = source.iter().map(|b| b.num_rows()).sum();

        // Find children
        let children = find_children(
            &source,
            &all_batches,
            &["block_number", "transaction_index"],
            "instruction_address",
            "instruction_address",
            false,
        )
        .unwrap();

        let children_count: usize = children.iter().map(|b| b.num_rows()).sum();
        // Whirlpool instructions may or may not have inner CPI calls
        let _ = children_count;

        // Find parents
        let parents = find_parents(
            &source,
            &all_batches,
            &["block_number", "transaction_index"],
            "instruction_address",
            "instruction_address",
            false,
        )
        .unwrap();

        let parents_count: usize = parents.iter().map(|b| b.num_rows()).sum();
        // Whirlpool top-level instructions ([0]) have no parents, but nested ones do
        println!(
            "Hierarchical join: source={}, children={}, parents={}",
            source_count, children_count, parents_count
        );
    }
}
