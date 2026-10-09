//! A relation some item requests asked for follows only the rows those matched.
//! The scan marks them as it evaluates the items, so the mark has to land on the
//! row it was computed for, through every stage that drops rows before it.

use crate::harness::chunk::write_table_row_groups;
use arrow::array::{ArrayRef, AsArray, BooleanArray, StringArray, UInt32Array, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, UInt32Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use sqd_query_engine::scan::predicate::{col_eq, col_in_list, or_masks, RowPredicate, ScalarValue};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest};
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

/// Each row's block number and index, which together name it.
fn identities(batches: &[RecordBatch]) -> Vec<(u64, u32)> {
    batches
        .iter()
        .flat_map(|batch| {
            let blocks = batch
                .column_by_name(BN)
                .unwrap()
                .as_primitive::<UInt64Type>();
            let indexes = batch
                .column_by_name("index")
                .unwrap()
                .as_primitive::<UInt32Type>();
            (0..batch.num_rows()).map(|row| (blocks.value(row), indexes.value(row)))
        })
        .collect()
}

/// The rows one of `predicates` matches; a row an item can say neither yes nor
/// no about is not one it matched.
fn any_item(predicates: &[&RowPredicate], batch: &RecordBatch) -> BooleanArray {
    let masks: Vec<BooleanArray> = predicates
        .iter()
        .map(|p| p.evaluate(batch).unwrap())
        .collect();
    let every: Vec<usize> = (0..masks.len()).collect();

    or_masks(&masks, &every, batch.num_rows())
        .iter()
        .map(|v| Some(v == Some(true)))
        .collect()
}

/// Covers CT-4 · INV-R1
#[test]
fn a_relation_follows_the_rows_its_own_items_matched() {
    let dir = chunk();
    let reader = ParquetChunkReader::open(dir.path()).unwrap();
    let columns = vec![BN, "index", "kind", "flag"];
    let whole = reader
        .scan("items", &ScanRequest::new(columns.clone()))
        .unwrap();

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
        // Statistics rule it out of every row group but the second.
        RowPredicate::new(vec![col_in_list(
            BN,
            Arc::new(UInt64Array::from(vec![204, 206])),
        )]),
    ];
    let item_sets: [&[usize]; 6] = [&[0, 1, 2], &[0, 1, 2, 3], &[1], &[0, 4], &[5, 1], &[0, 5]];
    let tag_sets: [&[usize]; 7] = [&[0], &[1, 2], &[2], &[0, 1, 2], &[4], &[5], &[1, 5]];

    let mut tagged_rows = 0;
    for chosen in item_sets {
        let predicates: Vec<&RowPredicate> = chosen.iter().map(|&i| &items[i]).collect();
        let tags: Vec<Vec<usize>> = tag_sets
            .iter()
            .map(|set| {
                set.iter()
                    .filter_map(|item| chosen.iter().position(|c| c == item))
                    .collect()
            })
            .collect();

        for batch_size in [1, 3, usize::MAX] {
            for range in [None, Some((102, 305))] {
                for positions in [false, true] {
                    let mut request = ScanRequest::new(columns.clone());
                    request.predicates = predicates.clone();
                    request.block_number_column = Some(BN);
                    (request.from_block, request.to_block) =
                        range.map_or((None, None), |(from, to)| (Some(from), Some(to)));
                    request.batch_size = batch_size;
                    request.positions = positions;
                    request.item_tags = tags.iter().map(Vec::as_slice).collect();

                    let scanned = reader.scan_rows("items", &request).unwrap();
                    let batches = scanned.rows().batches();
                    let context = format!(
                        "items {chosen:?}, batches of {batch_size}, range {range:?}, \
                         positions {positions}"
                    );

                    // Every row one of the items matches comes back, whichever
                    // row groups the others' statistics rule out.
                    let matched: Vec<RecordBatch> = whole
                        .iter()
                        .map(|batch| filter_record_batch(batch, &any_item(&predicates, batch)))
                        .collect::<Result<_, _>>()
                        .unwrap();
                    let in_range = |&(block, _): &(u64, u32)| {
                        range.is_none_or(|(from, to)| (from..=to).contains(&block))
                    };
                    let wanted: Vec<_> =
                        identities(&matched).into_iter().filter(in_range).collect();
                    assert_eq!(identities(batches), wanted, "rows: {context}");

                    let mut expected: Vec<Vec<RecordBatch>> = vec![Vec::new(); tags.len()];
                    for batch in batches {
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

                        for (items, expected) in tags.iter().zip(&mut expected) {
                            let matched = or_masks(&masks, items, batch.num_rows());
                            let matched: BooleanArray =
                                matched.iter().map(|v| Some(v == Some(true))).collect();
                            expected.push(filter_record_batch(batch, &matched).unwrap());
                        }
                    }

                    for (items, expected) in tags.iter().zip(&expected) {
                        let tagged = scanned
                            .matched_by(items)
                            .unwrap_or_else(|| panic!("tag {items:?} is missing: {context}"));
                        assert_eq!(
                            identities(&tagged),
                            identities(expected),
                            "tag {items:?}: {context}"
                        );
                        tagged_rows += tagged.iter().map(RecordBatch::num_rows).sum::<usize>();
                    }

                    let returned = scanned.rows().num_rows();
                    assert!(returned > 0, "nothing came back: {context}");
                }
            }
        }
    }
    assert!(tagged_rows > 0, "no row was ever tagged");
}

/// Two relations on the same key, asked for by different items, follow only
/// their own items' rows: a key set built for one is not the other's.
///
/// Covers CT-4 · INV-R1
#[test]
fn relations_on_one_key_follow_only_their_own_items() {
    use crate::harness::evm_like;
    use crate::harness::generator::Generator;
    use crate::harness::json::items_of;

    let chunk = evm_like::chunk();
    let generator = Generator::new(evm_like::catalog(), chunk.path());
    let (first, last) = generator.blocks();
    let query = format!(
        r#"{{"type":"test","fromBlock":{first},"toBlock":{last},
            "fields":{{"block":{{"number":true}},
                       "log":{{"transactionIndex":true,"logIndex":true}},
                       "trace":{{"transactionIndex":true,"traceIndex":true}}}},
            "transactions":[{{"transactionIndex":[0],"transactionLogs":true}},
                            {{"transactionIndex":[1],"transactionTraces":true}}]}}"#
    );
    let body = generator.run(&query);

    for (table, index) in [("logs", 0), ("traces", 1)] {
        let items = items_of(&body, table);
        assert!(!items.is_empty(), "no {table} came back");
        for (block, item) in items {
            assert_eq!(
                item["transactionIndex"], index,
                "{table} of block {block} came from another item's transaction: {item}"
            );
        }
    }
}
