//! A relation the plan evaluates once for several item requests follows the rows
//! of every item request that asked for it, and of no other.
//!
//! The plan keeps one relation per join, so a join two names declare is planned
//! once. What it follows has to be gathered per join too: the items that asked
//! for it under either name, each once, and none that asked for another join
//! under the same name.

use crate::harness::chunk::write_table;
use crate::harness::fixtures::run_against;
use crate::harness::json::parse_response;
use arrow::array::{ArrayRef, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::TempDir;

const BLOCK: u64 = 10;

/// `items` joins `targets` three ways. `first` and `second` are one join under
/// two names; `by_other` keys the same columns on the source side and another on
/// the target side, and so does `first` when it is asked through `special`.
/// Through `elsewhere`, `first` joins `items` to itself.
fn catalog() -> DatasetDescription {
    parse_dataset_description(
        r#"
version: v2
name: test
tables:
  blocks:
    output:
      name: block
      fields: [number]
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
  items:
    request:
      name: items
      filters: [seq]
      relations:
        first: { table: targets, key: [block_number, seq] }
        second: { table: targets, key: [block_number, seq] }
        by_other:
          table: targets
          left_key: [block_number, seq]
          right_key: [block_number, other]
    output:
      name: item
      fields: [seq]
    item_order_keys: [seq]
    sort_key: [block_number, seq]
    columns:
      block_number: { type: uint64 }
      seq: { type: uint32 }
  targets:
    request:
      name: targets
      filters: []
    output:
      name: target
      fields: [seq, other]
    item_order_keys: [seq]
    sort_key: [block_number, seq]
    columns:
      block_number: { type: uint64 }
      seq: { type: uint32 }
      other: { type: uint32 }
aliases:
  special:
    table: items
    filters: [seq]
    relations:
      first:
        table: targets
        left_key: [block_number, seq]
        right_key: [block_number, other]
  elsewhere:
    table: items
    filters: [seq]
    relations:
      first: { table: items, key: [block_number, seq] }
"#,
    )
    .unwrap()
}

/// Items 1–4, and targets 1–4 whose `other` runs the other way, so a join on
/// `seq` and a join on `other` reach different targets from one item.
fn chunk() -> TempDir {
    let dir = TempDir::new().unwrap();
    let seq = || Arc::new(UInt32Array::from(vec![1u32, 2, 3, 4])) as ArrayRef;
    let block = || Arc::new(UInt64Array::from(vec![BLOCK; 4])) as ArrayRef;

    write_table(
        dir.path(),
        "blocks",
        vec![Field::new("number", DataType::UInt64, false)],
        vec![Arc::new(UInt64Array::from(vec![BLOCK]))],
    );
    write_table(
        dir.path(),
        "items",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("seq", DataType::UInt32, false),
        ],
        vec![block(), seq()],
    );
    write_table(
        dir.path(),
        "targets",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("seq", DataType::UInt32, false),
            Field::new("other", DataType::UInt32, false),
        ],
        vec![
            block(),
            seq(),
            Arc::new(UInt32Array::from(vec![4u32, 3, 2, 1])),
        ],
    );
    dir
}

/// The targets `items` reaches, by `seq`.
fn targets(chunk: &TempDir, items: &str) -> BTreeSet<u64> {
    let query = format!(
        r#"{{"type":"test","fromBlock":{BLOCK},"toBlock":{BLOCK},{items},
             "fields":{{"target":{{"seq":true}}}}}}"#
    );
    let body = run_against(&catalog(), chunk.path(), &query).unwrap();

    parse_response(&body)
        .iter()
        .flat_map(|block| block["targets"].as_array().cloned().unwrap_or_default())
        .map(|target| target["seq"].as_u64().unwrap())
        .collect()
}

/// Covers CT-4 · INV-R1
#[test]
fn a_relation_planned_once_follows_every_item_that_asked_for_it() {
    let chunk = chunk();

    let cases = [
        (
            "one join under two names, from two items",
            r#""items":[{"seq":[1],"first":true},{"seq":[2],"second":true},{"seq":[3]}]"#,
            vec![1, 2],
        ),
        (
            "one join under two names, from one item",
            r#""items":[{"seq":[1],"first":true,"second":true},{"seq":[2]}]"#,
            vec![1],
        ),
        (
            "one join under two names, from every item",
            r#""items":[{"seq":[1],"first":true},{"seq":[2],"second":true}]"#,
            vec![1, 2],
        ),
        (
            "two joins apart only in the target's key",
            r#""items":[{"seq":[1],"first":true},{"seq":[2],"byOther":true},{"seq":[3]}]"#,
            // `seq` 1 by `seq`; `seq` 2 by `other`, which target 3 holds.
            vec![1, 3],
        ),
        (
            "one name, one join on the table and another through an alias",
            r#""items":[{"seq":[1],"first":true},{"seq":[3]}],
               "special":[{"seq":[2],"first":true}]"#,
            vec![1, 3],
        ),
        (
            "one name, joins to two tables through two aliases",
            r#""special":[{"seq":[2],"first":true}],
               "elsewhere":[{"seq":[1],"first":true}],"items":[{"seq":[4]}]"#,
            // `elsewhere` joins `items`, so only `special` reaches a target.
            vec![3],
        ),
        (
            "one relation flagged twice in one item",
            r#""items":[{"seq":[1],"first":true,"First":true},{"seq":[2]}]"#,
            vec![1],
        ),
    ];

    let mut wrong = Vec::new();
    for (what, items, expected) in cases {
        let expected: BTreeSet<u64> = expected.into_iter().collect();
        let found = targets(&chunk, items);
        if found != expected {
            wrong.push(format!("{what}: {found:?}, not {expected:?}"));
        }
    }

    assert!(wrong.is_empty(), "{wrong:#?}");
}
