//! Whether a hierarchy includes the row at the source's own address. A call's
//! events carry the call's address, so a relation between two tables includes
//! it (INV-R8); a relation within one table does not, since the row there is
//! the source itself (INV-R6, INV-R7). That is a property of the tables, so two
//! tables that name their address columns alike must still include it.

use arrow::array::{ArrayRef, ListBuilder, StringArray, UInt32Array, UInt32Builder, UInt64Array};
use arrow::datatypes::{DataType, Field};
use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use std::sync::Arc;
use tempfile::TempDir;

use crate::harness::chunk::{blocks_parquet, write_table};
use crate::harness::json::items_in;
use crate::harness::synthetic::run_json;

/// `nodes` form a tree and `marks` hang off it, both grouped by `tx`. The
/// marks' address column is named `{mark_address}`.
const CATALOG: &str = r#"
version: v2
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
  nodes:
    request:
      name: nodes
      filters: [ name ]
      relations:
        subnodes:
          table: nodes
          kind: children
          key: [ block_number, tx ]
        stack:
          table: nodes
          kind: parents
          key: [ block_number, tx ]
        marks:
          table: marks
          kind: children
          key: [ block_number, tx ]
    output:
      name: node
      fields: [ name ]
    address_column: address
    item_order_keys: [ tx, address, name ]
    sort_key: [ name, block_number, tx ]
    columns:
      block_number: { type: uint64 }
      tx: { type: uint32 }
      address: { type: list_uint32 }
      name: { type: string }
  marks:
    request:
      name: marks
      filters: [ name ]
      relations:
        nodes:
          table: nodes
          kind: parents
          key: [ block_number, tx ]
    output:
      name: mark
      fields: [ name ]
    address_column: {mark_address}
    item_order_keys: [ index ]
    sort_key: [ name, block_number, index ]
    columns:
      block_number: { type: uint64 }
      index: { type: uint32 }
      tx: { type: uint32 }
      {mark_address}: { type: list_uint32 }
      name: { type: string }
"#;

fn catalog(mark_address: &str) -> DatasetDescription {
    parse_dataset_description(&CATALOG.replace("{mark_address}", mark_address))
        .expect("the catalog is valid")
}

/// One block, one group. `a2` shares `a`'s address, so a relation that includes
/// equal addresses within `nodes` shows it.
const NODES: [(&str, &[u32]); 5] = [
    ("root", &[]),
    ("a", &[0]),
    ("a2", &[0]),
    ("aa", &[0, 0]),
    ("b", &[1]),
];

/// A mark at each node's address, named after it.
const MARKS: [(&str, &[u32]); 4] = [
    ("m_root", &[]),
    ("m_a", &[0]),
    ("m_aa", &[0, 0]),
    ("m_b", &[1]),
];

fn chunk(mark_address: &str) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    blocks_parquet(dir.path(), &[1]);

    let address_type = DataType::List(Arc::new(Field::new("item", DataType::UInt32, true)));
    let addresses = |rows: &[(&str, &[u32])]| -> ArrayRef {
        let mut list = ListBuilder::new(UInt32Builder::new());
        for (_, address) in rows {
            list.append_value(address.iter().map(|step| Some(*step)));
        }
        Arc::new(list.finish())
    };
    let names = |rows: &[(&str, &[u32])]| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|(name, _)| *name),
        ))
    };

    write_table(
        dir.path(),
        "nodes",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("tx", DataType::UInt32, false),
            Field::new("address", address_type.clone(), false),
            Field::new("name", DataType::Utf8, false),
        ],
        vec![
            Arc::new(UInt64Array::from(vec![1; NODES.len()])),
            Arc::new(UInt32Array::from(vec![0; NODES.len()])),
            addresses(&NODES),
            names(&NODES),
        ],
    );
    write_table(
        dir.path(),
        "marks",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("index", DataType::UInt32, false),
            Field::new("tx", DataType::UInt32, false),
            Field::new(mark_address, address_type, false),
            Field::new("name", DataType::Utf8, false),
        ],
        vec![
            Arc::new(UInt64Array::from(vec![1; MARKS.len()])),
            Arc::new(UInt32Array::from_iter_values(0..MARKS.len() as u32)),
            Arc::new(UInt32Array::from(vec![0; MARKS.len()])),
            addresses(&MARKS),
            names(&MARKS),
        ],
    );

    dir
}

/// The names under `table_key` that following `relation` from the items named
/// `from` returns, sorted.
fn related(
    mark_address: &str,
    items: &str,
    from: &str,
    relation: &str,
    table_key: &str,
) -> Vec<String> {
    let query = serde_json::json!({
        "type": "test",
        "fromBlock": 0,
        "fields": {"node": {"name": true}, "mark": {"name": true}},
        items: [{"name": [from], relation: true}],
    });
    let chunk = chunk(mark_address);
    let blocks = run_json(&catalog(mark_address), &chunk, &query.to_string())
        .unwrap()
        .expect("the range holds a block");

    let mut names: Vec<String> = blocks
        .iter()
        .flat_map(|block| items_in(block, table_key))
        .map(|item| item["name"].as_str().unwrap().to_owned())
        .collect();
    names.sort();
    names
}

/// Between two tables the row at the source's address is related, in both
/// directions, whether the address columns share a name or not.
///
/// Covers CT-4 · INV-R8
#[test]
fn a_hierarchy_across_tables_includes_the_equal_address() {
    for mark_address in ["address", "node_address"] {
        assert_eq!(
            related(mark_address, "nodes", "a", "marks", "marks"),
            ["m_a", "m_aa"],
            "marks of a, the marks' address column named {mark_address}"
        );
        assert_eq!(
            related(mark_address, "marks", "m_a", "nodes", "nodes"),
            ["a", "a2", "root"],
            "nodes above m_a, the marks' address column named {mark_address}"
        );
    }
}

/// Within one table the row at the source's address is not related: `a2` comes
/// back neither as a child nor as a parent of `a`. It is a parent of `aa`, and
/// so is every row above it.
///
/// Covers CT-4 · INV-R6, INV-R7
#[test]
fn a_hierarchy_within_a_table_excludes_the_equal_address() {
    for mark_address in ["address", "node_address"] {
        assert_eq!(
            related(mark_address, "nodes", "a", "subnodes", "nodes"),
            ["a", "aa"],
            "subnodes of a, the marks' address column named {mark_address}"
        );
        assert_eq!(
            related(mark_address, "nodes", "a", "stack", "nodes"),
            ["a", "root"],
            "the stack of a, the marks' address column named {mark_address}"
        );
        assert_eq!(
            related(mark_address, "nodes", "aa", "stack", "nodes"),
            ["a", "a2", "aa", "root"],
            "the stack of aa, the marks' address column named {mark_address}"
        );
    }
}
