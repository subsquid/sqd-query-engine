//! `gteConst` compares lexicographically on the stored string.
//!
//! The columns it guards hold minimal-form hex quantities, and the catalog's
//! only constants today are `"0x1"` — which is where the temptation lies, since
//! over minimal-form hex "≥ 0x1" and "is not zero" pick the same rows however
//! the comparison is done. So the cases below are written against a second
//! constant as well, `"0x9"`, where the two readings disagree: `"0x10"` is
//! sixteen and is lexicographically below `"0x9"`. An engine parsing the column
//! into an integer passes every `"0x1"` case and fails that one.

use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use std::path::Path;

use crate::harness::chunk::{blocks_parquet, write_table};
use crate::harness::fixtures::run_against;
use crate::harness::json::{items_in, parse_response};

/// Two constants over one column. The second is invented — no chain writes a
/// `≥ 0x9` filter — which is the point: the invariant is about the comparison,
/// not about the one constant the bundled catalogs happen to carry.
const CATALOG: &str = r#"
name: gte

tables:
  blocks:
    output:
      name: block
      fields: [number]
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }

  traces:
    request:
      name: traces
      filters: [call_value_non_zero, call_value_at_least_nine]
      special_filters:
        call_value_non_zero:
          kind: gte_const
          column: call_value
          value: "0x1"
        call_value_at_least_nine:
          kind: gte_const
          column: call_value
          value: "0x9"
    output:
      name: trace
      fields: [trace_index, call_value]
    block_number_column: block_number
    item_order_keys: [trace_index]
    sort_key: [block_number, trace_index]
    columns:
      block_number: { type: uint64 }
      trace_index: { type: uint32 }
      call_value: { type: string, encoding: hex_bytes }
"#;

/// A 256-bit quantity: wider than any integer the engine has, and a value an
/// engine that parsed the column would have to reject or truncate.
const HUGE: &str = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

/// One trace per value, in `trace_index` order, with a null last. Zero has two
/// minimal forms in the wild and both are here.
const VALUES: [Option<&str>; 8] = [
    Some("0x"),
    Some("0x0"),
    Some("0x1"),
    Some("0x9"),
    Some("0x10"),
    Some("0xff"),
    Some(HUGE),
    None,
];

const BLOCK: u64 = 10;

fn dataset() -> (DatasetDescription, tempfile::TempDir) {
    use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    let catalog = parse_dataset_description(CATALOG).unwrap();
    let dir = tempfile::tempdir().unwrap();

    blocks_parquet(dir.path(), &[BLOCK]);
    write_table(
        dir.path(),
        "traces",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("trace_index", DataType::UInt32, false),
            Field::new("call_value", DataType::Utf8, true),
        ],
        vec![
            Arc::new(UInt64Array::from(vec![BLOCK; VALUES.len()])) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(0..VALUES.len() as u32)) as ArrayRef,
            Arc::new(StringArray::from(VALUES.to_vec())) as ArrayRef,
        ],
    );

    (catalog, dir)
}

/// The `call_value` of every trace a query returns, in response order.
fn values(catalog: &DatasetDescription, chunk: &Path, filter: &str) -> Vec<String> {
    let query = format!(
        r#"{{"type":"gte","fromBlock":{BLOCK},"toBlock":{BLOCK},
            "fields":{{"trace":{{"callValue":true}}}},
            "traces":[{{{filter}}}]}}"#
    );
    let body = run_against(catalog, chunk, &query).expect("the query must be answerable");

    parse_response(&body)
        .iter()
        .flat_map(|block| items_in(block, "traces"))
        .map(|item| match item.get("callValue") {
            Some(value) if !value.is_null() => value.as_str().unwrap().to_string(),
            _ => "null".to_string(),
        })
        .collect()
}

/// Covers CT-3 · INV-P11
#[test]
fn gte_const_compares_lexicographically() {
    let (catalog, chunk) = dataset();

    // The flag off is the whole table, and the baseline the two filters narrow.
    assert_eq!(
        values(&catalog, chunk.path(), r#""callValueNonZero": false"#),
        ["0x", "0x0", "0x1", "0x9", "0x10", "0xff", HUGE, "null"],
        "a flag set to false must not filter"
    );

    // Against "0x1": every non-zero minimal-form quantity, and neither spelling
    // of zero. The 256-bit value is here because an engine parsing the column
    // into an integer has nowhere to put it.
    assert_eq!(
        values(&catalog, chunk.path(), r#""callValueNonZero": true"#),
        ["0x1", "0x9", "0x10", "0xff", HUGE],
        "≥ 0x1 must keep every non-zero value and drop both spellings of zero"
    );

    // Against "0x9", where the two readings part: "0x10" is sixteen, and it is
    // lexicographically below "0x9" because '1' is.
    assert_eq!(
        values(&catalog, chunk.path(), r#""callValueAtLeastNine": true"#),
        ["0x9", "0xff", HUGE],
        "the comparison is on the string, not on the quantity it denotes"
    );
}

/// A null is not a value below the constant; it is no value at all, and the
/// comparison against it is unknown rather than false ([INV-P7]).
///
/// It is the case a real chunk reaches on the first `create` trace, which
/// carries no `call_value` — and the one where a comparison kernel hands the
/// scan a mask with nulls in it.
///
/// [INV-P7]: ../../../spec/07-invariants.md#inv-p7
///
/// Covers CT-3 · INV-P11
/// Covers CT-3 · INV-P7
#[test]
fn a_null_is_not_greater_than_the_constant() {
    let (catalog, chunk) = dataset();

    for filter in [
        r#""callValueNonZero": true"#,
        r#""callValueAtLeastNine": true"#,
    ] {
        let returned = values(&catalog, chunk.path(), filter);

        // Without this the test is one an empty response passes: a filter
        // compiling to "match nothing" returns no null because it returns no
        // row, and reports green having compared nothing.
        assert!(
            !returned.is_empty(),
            "{filter} returned no rows at all, so the null's absence asserts nothing"
        );
        assert!(
            !returned.contains(&"null".to_string()),
            "{filter} returned the trace whose call value is null"
        );
    }
}
