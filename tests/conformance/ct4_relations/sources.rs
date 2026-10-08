//! A relation some item requests asked for follows only the rows those matched.
//! The scan marks them as it evaluates the items, so the mark has to land on the
//! row it was computed for, through every stage that drops rows before it.

use crate::harness::chunk::write_table_row_groups;
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use sqd_query_engine::scan::predicate::{col_eq, col_in_list, or_masks, RowPredicate, ScalarValue};
use sqd_query_engine::scan::{ChunkReader, ItemTag, ParquetChunkReader, ScanRequest};
use std::sync::Arc;
use tempfile::TempDir;

const BN: &str = "block_number";

fn chunk() -> TempDir {
    let dir = TempDir::new().unwrap();
    let kinds = ["a", "b", "c"];
    let groups = [100u64, 200, 300]
        .into_iter()
        .map(|start| {
            let rows = 0..8u64;
            vec![
                Arc::new(UInt64Array::from_iter_values(
                    rows.clone().map(|r| start + r),
                )) as ArrayRef,
                Arc::new(UInt32Array::from_iter_values(
                    rows.clone().map(|r| r as u32),
                )),
                Arc::new(StringArray::from_iter(rows.clone().map(|r| {
                    (r % 4 != 3).then(|| kinds[((r + start / 100) % 3) as usize])
                }))),
                Arc::new(BooleanArray::from_iter(
                    rows.map(|r| (r % 5 != 4).then_some(r % 2 == 0)),
                )),
            ]
        })
        .collect();
    write_table_row_groups(
        dir.path(),
        "items",
        vec![
            Field::new(BN, DataType::UInt64, false),
            Field::new("index", DataType::UInt32, false),
            Field::new("kind", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
        ],
        groups,
    );
    dir
}

/// Covers CT-4 · INV-R1
#[test]
fn a_relation_follows_the_rows_its_own_items_matched() {
    let dir = chunk();
    let reader = ParquetChunkReader::open(dir.path()).unwrap();

    let items = [
        RowPredicate::new(vec![col_in_list(
            "kind",
            Arc::new(StringArray::from(vec!["a"])),
        )]),
        RowPredicate::new(vec![col_in_list(
            "index",
            Arc::new(UInt32Array::from(vec![1, 3, 5])),
        )]),
        RowPredicate::new(vec![
            col_eq("flag", ScalarValue::Boolean(true)),
            col_in_list("kind", Arc::new(StringArray::from(vec!["b", "c"]))),
        ]),
        RowPredicate::new(vec![]),
        // Unknown on a null flag, and the kernel leaves the value bit set there.
        RowPredicate::new(vec![col_eq("flag", ScalarValue::Boolean(false))]),
    ];
    let item_sets: [&[usize]; 4] = [&[0, 1, 2], &[0, 1, 2, 3], &[1], &[0, 4]];
    let tag_sets: [&[usize]; 5] = [&[0], &[1, 2], &[2], &[0, 1, 2], &[4]];

    let mut tagged_rows = 0;
    for chosen in item_sets {
        let predicates: Vec<&RowPredicate> = chosen.iter().map(|&i| &items[i]).collect();
        let tags: Vec<(String, Vec<usize>)> = tag_sets
            .iter()
            .enumerate()
            .map(|(n, set)| {
                let positions = set
                    .iter()
                    .filter_map(|item| chosen.iter().position(|c| c == item))
                    .collect();
                (format!("tag{n}"), positions)
            })
            .collect();

        for batch_size in [1, 3, usize::MAX] {
            for range in [None, Some((102, 305))] {
                for positions in [None, Some("position")] {
                    let mut request = ScanRequest::new(vec![BN, "index", "kind", "flag"]);
                    request.predicates = predicates.clone();
                    request.block_number_column = Some(BN);
                    (request.from_block, request.to_block) =
                        range.map_or((None, None), |(from, to)| (Some(from), Some(to)));
                    request.batch_size = batch_size;
                    request.row_index_column = positions;
                    request.item_tags = tags
                        .iter()
                        .map(|(column, items)| ItemTag { column, items })
                        .collect();

                    let batches = reader.scan("items", &request).unwrap();
                    let context = format!(
                        "items {chosen:?}, batches of {batch_size}, range {range:?}, \
                         positions {positions:?}"
                    );

                    let mut returned = 0;
                    for batch in &batches {
                        let masks: Vec<BooleanArray> = predicates
                            .iter()
                            .map(|p| p.evaluate(batch).unwrap())
                            .collect();
                        let every: Vec<usize> = (0..masks.len()).collect();
                        let any = or_masks(&masks, &every, batch.num_rows());
                        assert!(
                            any.iter().all(|v| v == Some(true)),
                            "a row no item matched came back: {context}"
                        );

                        for (column, items) in &tags {
                            let expected = or_masks(&masks, items, batch.num_rows());
                            let tag = batch
                                .column_by_name(column)
                                .unwrap_or_else(|| panic!("{column} is missing: {context}"))
                                .as_boolean();
                            for row in 0..batch.num_rows() {
                                let expected = expected.is_valid(row) && expected.value(row);
                                assert_eq!(
                                    tag.value(row),
                                    expected,
                                    "{column}, row {row}: {context}"
                                );
                                tagged_rows += usize::from(expected);
                            }
                        }
                        returned += batch.num_rows();
                    }
                    assert!(returned > 0, "nothing came back: {context}");
                }
            }
        }
    }
    assert!(tagged_rows > 0, "no row was ever tagged");
}
