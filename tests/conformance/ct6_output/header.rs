//! A header field is read from the columns it renders, on every path that reads
//! the header.
//!
//! A roll field is not a column: it renders the columns it rolls. The header
//! scan read the field's own name, which no chunk stores, so the header came
//! back without it while validation, which expands the roll, found nothing
//! missing.

use arrow::array::{ArrayRef, AsArray, UInt64Array};
use arrow::datatypes::{DataType, Field, UInt64Type};
use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use sqd_query_engine::output::{execute_chunk_arrow, ExecOptions};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::sync::Arc;
use tempfile::TempDir;

use crate::harness::arrow::read_frames;
use crate::harness::chunk::write_table;
use crate::harness::fixtures::run_against_with_cut;
use crate::harness::json::parse_response;

/// A block table whose `parts` rolls `a` and `b`, and an item table with a
/// relation, which reads the header on the deferred path.
fn catalog() -> DatasetDescription {
    parse_dataset_description(
        r#"
version: v2
name: test
tables:
  blocks:
    output:
      name: block
      fields: [number, parts]
      virtual_fields:
        parts: { kind: roll, columns: [a, b] }
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
      a: { type: uint64 }
      b: { type: uint64 }
  items:
    request:
      name: items
      filters: []
      relations:
        same: { table: items, key: [block_number, seq] }
    output:
      name: item
      fields: [seq]
    item_order_keys: [seq]
    sort_key: [block_number, seq]
    columns:
      block_number: { type: uint64 }
      seq: { type: uint64 }
"#,
    )
    .unwrap()
}

/// Blocks 1 and 2, `a` and `b` 7 and 9, then 8 and 10, one item in each.
fn chunk() -> TempDir {
    let dir = TempDir::new().unwrap();
    let column = |values: [u64; 2]| Arc::new(UInt64Array::from(values.to_vec())) as ArrayRef;

    write_table(
        dir.path(),
        "blocks",
        vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::UInt64, false),
        ],
        vec![column([1, 2]), column([7, 8]), column([9, 10])],
    );
    write_table(
        dir.path(),
        "items",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("seq", DataType::UInt64, false),
        ],
        vec![column([1, 2]), column([0, 0])],
    );
    dir
}

/// Covers CT-6 · INV-O7
#[test]
fn a_rolled_header_field_is_rendered() {
    let chunk = chunk();
    let expected = [serde_json::json!([7, 9]), serde_json::json!([8, 10])];

    let shapes = [
        ("headers alone", r#""includeAllBlocks":true"#),
        ("headers beside a relation", r#""items":[{"same":true}]"#),
    ];

    let mut wrong = Vec::new();
    for (what, items) in shapes {
        let query = format!(
            r#"{{"type":"test","fromBlock":1,"toBlock":2,{items},
                 "fields":{{"block":{{"parts":true}}}}}}"#
        );
        for range_reads in [true, false] {
            let options = ExecOptions {
                range_reads,
                ..ExecOptions::default()
            };
            let (body, _) =
                run_against_with_cut(&catalog(), chunk.path(), &query, options).unwrap();
            let parts: Vec<_> = parse_response(&body)
                .iter()
                .map(|block| block["header"]["parts"].clone())
                .collect();

            if parts != expected {
                wrong.push(format!("{what}, range reads {range_reads}: {parts:?}"));
            }
        }
    }

    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The Arrow rendering ships the rolled columns themselves.
///
/// Covers CT-6 · INV-O14
#[test]
fn a_rolled_header_field_is_shipped_in_arrow() {
    let chunk = chunk();
    let meta = catalog();
    let query = r#"{"type":"test","fromBlock":1,"toBlock":2,"includeAllBlocks":true,
                    "fields":{"block":{"parts":true}}}"#;
    let plan = compile(&parse_query(query.as_bytes(), &meta).unwrap(), &meta).unwrap();
    let reader = ParquetChunkReader::open(chunk.path()).unwrap();

    let arrow = execute_chunk_arrow(&plan, &meta, &reader, false, false)
        .unwrap()
        .map(|out| out.into_data())
        .unwrap_or_default();
    let frames = read_frames(&arrow);
    let headers = &frames["blocks"];

    for (column, values) in [("a", [7u64, 8]), ("b", [9, 10])] {
        let stored: Vec<u64> = headers
            .iter()
            .filter_map(|batch| batch.column_by_name(column))
            .flat_map(|array| array.as_primitive::<UInt64Type>().values().to_vec())
            .collect();
        assert_eq!(stored, values, "header column `{column}`");
    }
}
