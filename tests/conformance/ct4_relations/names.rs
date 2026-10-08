//! A stored column's name means nothing to the engine.
//!
//! The engine adds columns of its own to what a scan returns: a mark per
//! relation for the rows its items matched, and the physical position of each
//! row. A chunk may store a column under the same name. Each case below runs a
//! query against a chunk where a column takes such a name, and against the same
//! chunk where it has a plain one; the two answers must be the same, field name
//! aside.

use crate::harness::chunk::write_table;
use crate::harness::fixtures::run_against_with_cut;
use arrow::array::{ArrayRef, BooleanArray, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use sqd_query_engine::output::ExecOptions;
use std::sync::Arc;
use tempfile::TempDir;

const BLOCKS: std::ops::RangeInclusive<u64> = 100..=115;
const PLAIN: Names = Names {
    key: "transaction_index",
    flag: "flag",
    other: "other",
    value: "row_value",
};

/// The columns a case renames.
#[derive(Clone, Copy)]
struct Names {
    /// The join key of both relations.
    key: &'static str,
    /// A selected boolean log field.
    flag: &'static str,
    /// A selected integer log field.
    other: &'static str,
    /// A selected transaction field, the same in every row of a block.
    value: &'static str,
}

fn catalog(names: Names) -> DatasetDescription {
    let Names {
        key,
        flag,
        other,
        value,
    } = names;
    parse_dataset_description(&format!(
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
      number: {{ type: uint64 }}
  logs:
    request:
      name: logs
      filters: [address]
      relations:
        transaction:
          table: transactions
          key: [block_number, {key}]
        transaction_logs:
          table: logs
          key: [block_number, {key}]
    output:
      name: log
      fields: [log_index, {key}, address, {flag}, {other}]
    block_number_column: block_number
    item_order_keys: [{key}, log_index]
    sort_key: [address, block_number, log_index]
    columns:
      block_number: {{ type: uint64 }}
      log_index: {{ type: uint32 }}
      {key}: {{ type: uint32 }}
      address: {{ type: string }}
      {flag}: {{ type: boolean }}
      {other}: {{ type: uint32 }}
  transactions:
    request:
      name: transactions
      filters: [{key}]
    output:
      name: transaction
      fields: [{key}, {value}, gas_used]
    block_number_column: block_number
    item_order_keys: [{key}]
    sort_key: [block_number, {key}]
    columns:
      block_number: {{ type: uint64 }}
      {key}: {{ type: uint32 }}
      {value}: {{ type: uint64 }}
      gas_used: {{ type: uint64 }}
"#
    ))
    .unwrap()
}

/// Four logs and two transactions a block. `value` is written at the width
/// `value_type` names, the same in every row: a column a reader took for row
/// positions would make every row the same row.
fn chunk(names: Names, value_type: DataType) -> TempDir {
    let dir = TempDir::new().unwrap();
    let blocks: Vec<u64> = BLOCKS.collect();
    write_table(
        dir.path(),
        "blocks",
        vec![Field::new("number", DataType::UInt64, false)],
        vec![Arc::new(UInt64Array::from(blocks.clone()))],
    );

    let logs: Vec<(String, u64, u32, u32)> = ["a", "b"]
        .iter()
        .flat_map(|address| {
            blocks.iter().flat_map(move |&block| {
                (0..4u32)
                    .filter(move |i| i % 2 == u32::from(*address == "b"))
                    .map(move |i| (address.to_string(), block, i, i / 2))
            })
        })
        .collect();
    write_table(
        dir.path(),
        "logs",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new(names.key, DataType::UInt32, false),
            Field::new("address", DataType::Utf8, false),
            Field::new(names.flag, DataType::Boolean, false),
            Field::new(names.other, DataType::UInt32, false),
        ],
        vec![
            Arc::new(UInt64Array::from_iter_values(logs.iter().map(|l| l.1))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(logs.iter().map(|l| l.2))),
            Arc::new(UInt32Array::from_iter_values(logs.iter().map(|l| l.3))),
            Arc::new(StringArray::from_iter_values(
                logs.iter().map(|l| l.0.clone()),
            )),
            // True where the relation would not follow, so a reader taking
            // it for the relation's mark follows the wrong rows.
            Arc::new(BooleanArray::from_iter(
                logs.iter().map(|l| Some(l.0 == "b")),
            )),
            Arc::new(UInt32Array::from_iter_values(logs.iter().map(|l| l.2 + 7))),
        ],
    );

    let transactions: Vec<(u64, u32)> = blocks
        .iter()
        .flat_map(|&block| (0..2u32).map(move |tx| (block, tx)))
        .collect();
    let values: ArrayRef = match value_type {
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![5u64; transactions.len()])),
        _ => Arc::new(UInt32Array::from(vec![5u32; transactions.len()])),
    };
    write_table(
        dir.path(),
        "transactions",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new(names.key, DataType::UInt32, false),
            Field::new(names.value, value_type, false),
            Field::new("gas_used", DataType::UInt64, false),
        ],
        vec![
            Arc::new(UInt64Array::from_iter_values(
                transactions.iter().map(|t| t.0),
            )),
            Arc::new(UInt32Array::from_iter_values(
                transactions.iter().map(|t| t.1),
            )),
            values,
            Arc::new(UInt64Array::from_iter_values(
                transactions.iter().map(|t| 21_000 + u64::from(t.1)),
            )),
        ],
    );
    dir
}

fn camel(name: &str) -> String {
    sqd_query_engine::output::snake_to_camel(name)
}

/// The query's response under `names`, with each renamed field put back under
/// its plain name.
fn answer(
    names: Names,
    query: &str,
    value_type: DataType,
    budget: Option<u64>,
    range_reads: bool,
) -> String {
    let chunk = chunk(names, value_type);
    let catalog = catalog(names);
    let query = rename(query, PLAIN, names, |to| to.to_owned());
    let options = ExecOptions {
        weight_budget: budget.unwrap_or(ExecOptions::default().weight_budget),
        range_reads,
        ..ExecOptions::default()
    };
    let body = run_against_with_cut(&catalog, chunk.path(), &query, options)
        .unwrap()
        .0;
    rename(&String::from_utf8(body).unwrap(), names, PLAIN, camel)
}

/// Replace each `from` field's response key with `to`'s, spelled by `spell`. A
/// request names a field in camel case or as stored, and a name with no capital
/// in it is the stored one.
fn rename(text: &str, from: Names, to: Names, spell: impl Fn(&str) -> String) -> String {
    [
        (from.key, to.key),
        (from.flag, to.flag),
        (from.other, to.other),
        (from.value, to.value),
    ]
    .iter()
    .fold(text.to_owned(), |text, (from, to)| {
        text.replace(
            &format!("\"{}\"", camel(from)),
            &format!("\"{}\"", spell(to)),
        )
    })
}

const RELATION_QUERY: &str = r#"{"type":"test","fromBlock":100,"toBlock":115,
    "fields":{"block":{"number":true},
              "log":{"logIndex":true,"transactionIndex":true,"address":true,
                     "flag":true,"other":true},
              "transaction":{"transactionIndex":true,"gasUsed":true}},
    "logs":[{"address":["a"],"transaction":true,"transactionLogs":true},
            {"address":["b"]}]}"#;

/// Items that match every row, so that both tables are read by more than one
/// scan and their columns are decoded once and filtered in memory.
const SHARED_QUERY: &str = r#"{"type":"test","fromBlock":100,"toBlock":115,
    "fields":{"block":{"number":true},
              "log":{"logIndex":true,"transactionIndex":true,"address":true,
                     "flag":true,"other":true},
              "transaction":{"transactionIndex":true,"gasUsed":true}},
    "transactions":[{}],
    "logs":[{"transaction":true,"transactionLogs":true},{}]}"#;

/// The rows a relation follows are never read from a stored column, whatever
/// its name: not from the relation's own join key, not from a selected
/// boolean, and not from a selected field.
///
/// Covers CT-4 · INV-R1
#[test]
fn a_stored_column_named_like_a_relation_mark_is_a_column() {
    let cases = [
        (
            "the join key",
            Names {
                key: "__sqd_relation_0",
                ..PLAIN
            },
        ),
        (
            "a selected boolean",
            Names {
                flag: "__sqd_relation_0",
                ..PLAIN
            },
        ),
        (
            "a field named like the second relation's mark",
            Names {
                other: "__sqd_relation_1",
                ..PLAIN
            },
        ),
    ];
    let mut failed = Vec::new();
    for (query_name, query) in [("filtered", RELATION_QUERY), ("shared", SHARED_QUERY)] {
        for range_reads in [true, false] {
            let expected = answer(PLAIN, query, DataType::UInt64, None, range_reads);
            assert!(
                expected.contains("gasUsed"),
                "the relation followed nothing"
            );

            for (what, names) in cases {
                let answered = std::panic::catch_unwind(|| {
                    answer(names, query, DataType::UInt64, None, range_reads)
                });
                if answered.ok().as_ref() != Some(&expected) {
                    failed.push(format!(
                        "{what}, {query_name} items, range reads {range_reads}"
                    ));
                }
            }
        }
    }
    assert!(
        failed.is_empty(),
        "answered differently with these named like a relation's mark: {failed:?}"
    );
}

/// Rows are told apart by positions the scan records, never by a stored column,
/// whatever its name. Every transaction stores the same value
/// there, so a weight model that took it for positions would count one row for
/// the whole table and let the response run far past its budget.
///
/// Covers CT-4 · INV-R3
/// Covers CT-5 · INV-B6
#[test]
fn a_stored_column_named_like_row_positions_is_a_column() {
    let query = r#"{"type":"test","fromBlock":100,"toBlock":115,
        "fields":{"block":{"number":true},
                  "log":{"logIndex":true},
                  "transaction":{"transactionIndex":true,"rowValue":true,"gasUsed":true}},
        "transactions":[{}],
        "logs":[{"address":["a"],"transaction":true}]}"#;

    let shared = r#"{"type":"test","fromBlock":100,"toBlock":115,
        "fields":{"block":{"number":true},
                  "log":{"logIndex":true},
                  "transaction":{"transactionIndex":true,"rowValue":true,"gasUsed":true}},
        "transactions":[{}],
        "logs":[{"transaction":true}]}"#;

    // A payload field is read only for the rows a page holds; a join key is
    // read while the page is chosen, beside the positions.
    let cases = [
        (
            "a payload field",
            Names {
                value: "__sqd_selected_row",
                ..PLAIN
            },
        ),
        (
            "the join key",
            Names {
                key: "__sqd_selected_row",
                ..PLAIN
            },
        ),
    ];
    let mut failed = Vec::new();
    for (what, positions) in cases {
        for (query_name, query) in [("filtered", query), ("shared", shared)] {
            for range_reads in [true, false] {
                for value_type in [DataType::UInt64, DataType::UInt32] {
                    for budget in [None, Some(512), Some(2048)] {
                        let run = |names| {
                            std::panic::catch_unwind(|| {
                                answer(names, query, value_type.clone(), budget, range_reads)
                            })
                            .ok()
                        };
                        if run(positions) != run(PLAIN) {
                            failed.push(format!(
                                "{what}, {value_type} at a budget of {budget:?}, \
                                 {query_name} items, range reads {range_reads}"
                            ));
                        }
                    }
                }
            }
        }
    }
    assert!(
        failed.is_empty(),
        "answered differently with a column named like row positions: {failed:?}"
    );
}
