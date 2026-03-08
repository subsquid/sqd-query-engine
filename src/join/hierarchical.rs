use anyhow::{anyhow, Result};
use arrow::array::*;
use arrow::compute;
use arrow::datatypes::SchemaRef;
use std::collections::HashMap;

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
        (0..arr.len()).map(|i| arr.value(i) as u32).collect()
    } else {
        Vec::new()
    }
}

/// Composite key for grouping by (block_number, transaction_index).
#[derive(Clone, Eq, PartialEq, Hash)]
struct GroupKey(Vec<u8>);

fn make_group_key(batch: &RecordBatch, row: usize, key_indices: &[usize]) -> GroupKey {
    let mut buf = Vec::with_capacity(key_indices.len() * 8);
    for &idx in key_indices {
        let col = batch.column(idx);
        if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
            buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
            buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<UInt16Array>() {
            buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes());
        } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes());
        }
    }
    GroupKey(buf)
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
/// address column is a strict extension of an address in `source_batches`.
///
/// For example, if a source row has `instruction_address = [0]`, then target rows
/// with `[0, 0]`, `[0, 1]`, `[0, 0, 3]` etc. are children.
///
/// Both source and target are from the same table (self-join).
///
/// - `group_key_columns`: columns that group rows (e.g., ["block_number", "transaction_index"])
/// - `address_column`: the List<UInt32> column (e.g., "instruction_address", "trace_address")
pub fn find_children(
    source_batches: &[RecordBatch],
    target_batches: &[RecordBatch],
    group_key_columns: &[&str],
    address_column: &str,
) -> Result<Vec<RecordBatch>> {
    if source_batches.is_empty() || target_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Build an index: group_key -> set of source addresses
    let mut source_addresses: HashMap<GroupKey, Vec<Vec<u32>>> = HashMap::new();

    for batch in source_batches {
        let key_indices = resolve_indices(batch.schema_ref(), group_key_columns)?;
        let addr_idx = batch
            .schema()
            .index_of(address_column)
            .map_err(|_| anyhow!("column '{}' not found", address_column))?;
        let addr_array = batch
            .column(addr_idx)
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .ok_or_else(|| anyhow!("'{}' must be a List<UInt32> column", address_column))?;

        for row in 0..batch.num_rows() {
            let gk = make_group_key(batch, row, &key_indices);
            let addr = extract_address(addr_array, row);
            source_addresses.entry(gk).or_default().push(addr);
        }
    }

    // Probe target batches: keep rows whose address is a strict child of any source address
    let mut result = Vec::new();
    for batch in target_batches {
        let key_indices = resolve_indices(batch.schema_ref(), group_key_columns)?;
        let addr_idx = batch
            .schema()
            .index_of(address_column)
            .map_err(|_| anyhow!("column '{}' not found", address_column))?;
        let addr_array = batch
            .column(addr_idx)
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .ok_or_else(|| anyhow!("'{}' must be a List<UInt32> column", address_column))?;

        let mut matches = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let gk = make_group_key(batch, row, &key_indices);
            let target_addr = extract_address(addr_array, row);

            let is_child = source_addresses
                .get(&gk)
                .map(|addrs| {
                    addrs.iter().any(|parent| {
                        // Child: target is strictly longer and starts with parent
                        target_addr.len() > parent.len()
                            && target_addr[..parent.len()] == parent[..]
                    })
                })
                .unwrap_or(false);
            matches.push(is_child);
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

/// Find all parents (ancestors) of the given rows. A parent is a row in `target_batches`
/// whose address is a strict prefix of an address in `source_batches`.
///
/// For example, if source has `instruction_address = [0, 1, 2]`, then target rows with
/// `[0]` and `[0, 1]` are parents.
pub fn find_parents(
    source_batches: &[RecordBatch],
    target_batches: &[RecordBatch],
    group_key_columns: &[&str],
    address_column: &str,
) -> Result<Vec<RecordBatch>> {
    if source_batches.is_empty() || target_batches.is_empty() {
        return Ok(Vec::new());
    }

    // Build index: group_key -> set of source addresses
    let mut source_addresses: HashMap<GroupKey, Vec<Vec<u32>>> = HashMap::new();

    for batch in source_batches {
        let key_indices = resolve_indices(batch.schema_ref(), group_key_columns)?;
        let addr_idx = batch
            .schema()
            .index_of(address_column)
            .map_err(|_| anyhow!("column '{}' not found", address_column))?;
        let addr_array = batch
            .column(addr_idx)
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .ok_or_else(|| anyhow!("'{}' must be a List<UInt32> column", address_column))?;

        for row in 0..batch.num_rows() {
            let gk = make_group_key(batch, row, &key_indices);
            let addr = extract_address(addr_array, row);
            source_addresses.entry(gk).or_default().push(addr);
        }
    }

    // Probe: keep rows whose address is a strict prefix of any source address
    let mut result = Vec::new();
    for batch in target_batches {
        let key_indices = resolve_indices(batch.schema_ref(), group_key_columns)?;
        let addr_idx = batch
            .schema()
            .index_of(address_column)
            .map_err(|_| anyhow!("column '{}' not found", address_column))?;
        let addr_array = batch
            .column(addr_idx)
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .ok_or_else(|| anyhow!("'{}' must be a List<UInt32> column", address_column))?;

        let mut matches = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let gk = make_group_key(batch, row, &key_indices);
            let target_addr = extract_address(addr_array, row);

            let is_parent = source_addresses
                .get(&gk)
                .map(|addrs| {
                    addrs.iter().any(|child| {
                        // Parent: target is strictly shorter and child starts with target
                        target_addr.len() < child.len()
                            && child[..target_addr.len()] == target_addr[..]
                    })
                })
                .unwrap_or(false);
            matches.push(is_parent);
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
                "instruction_address"
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
                "instruction_address"
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
        )
        .unwrap();

        let total: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_hierarchical_with_real_data() {
        use crate::scan::ParquetTable;
        use std::path::Path;

        let chunk = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/200");
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
