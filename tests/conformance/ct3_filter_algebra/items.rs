//! INV-P16 per item: a row group whose statistics rule an item out holds no row
//! that item matches.
//!
//! Items are ORed into one scan, and each runs only on the row groups its own
//! statistics leave in. What can go wrong is a bound read at the wrong width, an
//! alternative judged as if it were required, or an item dropped from a row
//! group another item kept it in. Each loses rows silently, so the answer is
//! compared with every row evaluated by hand.
//!
//! Statistics also prove filters: a row group that holds one value, which a
//! filter lists, needs no evaluation of that filter. A proof read at the wrong
//! width, or on a column that holds nulls, keeps rows the filter drops.

use crate::harness::chunk::write_table_with;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int32Array, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, UInt32Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use sqd_query_engine::scan::predicate::{
    col_eq, col_in_list, or_row_predicates, ColumnPredicate, RangeGtePredicate, RangeLtePredicate,
    RowPredicate, ScalarValue,
};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, Rows, ScanRequest};
use std::sync::Arc;
use tempfile::TempDir;

const ROWS: usize = 900;
const BN: &str = "block_number";

struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

/// Rows sorted the way an archive sorts them: by the filtered columns first,
/// then by block. `code` crosses 2³¹, where its statistics turn negative, and
/// `name` shares a long prefix so a truncated bound is a prefix of a value.
/// `group` and `label` hold one value per `kind`, so many row groups hold one
/// value of each; `group` crosses 2³¹ between kinds, and `label` is null in
/// runs of rows, so some of those row groups hold nulls beside the value.
fn columns(rng: &mut Rng, wrapped_blocks: bool) -> Vec<(Field, ArrayRef)> {
    let mut rows: Vec<(u32, u32, u64)> = (0..ROWS)
        .map(|_| {
            let kind = rng.below(4) as u32;
            let code = match rng.below(3) {
                0 => rng.below(8) as u32,
                1 => u32::MAX - rng.below(8) as u32,
                _ => (1 << 31) + rng.below(8) as u32,
            };
            (kind, code, 100 + rng.below(40))
        })
        .collect();
    rows.sort_unstable();

    let ids = 0..rows.len() as u32;
    let names = rows
        .iter()
        .map(|&(kind, _, _)| format!("a-shared-prefix-for-every-name-{kind}"));
    let groups = rows.iter().map(|&(kind, _, _)| group_of(kind as u64));
    let labels = rows
        .iter()
        .map(|&(kind, code, _)| (code % 5 != 0).then(|| format!("label-{kind}")));
    let flags = rows
        .iter()
        .map(|&(_, code, block)| (block % 7 != 0).then_some(code % 2 == 0));
    let blocks: ArrayRef = if wrapped_blocks {
        // Above 2³¹ an Int32 block number wraps negative, so its bounds invert.
        Arc::new(Int32Array::from_iter_values(
            rows.iter()
                .map(|&(_, _, block)| (block as u32 + (1 << 31) - 120) as i32),
        ))
    } else {
        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.2)))
    };

    vec![
        (
            Field::new("name", DataType::Utf8, false),
            Arc::new(StringArray::from_iter_values(names)) as ArrayRef,
        ),
        (
            Field::new("code", DataType::UInt32, false),
            Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.1))),
        ),
        (
            Field::new("flag", DataType::Boolean, true),
            Arc::new(BooleanArray::from_iter(flags)),
        ),
        (
            Field::new("group", DataType::UInt32, false),
            Arc::new(UInt32Array::from_iter_values(groups)),
        ),
        (
            Field::new("label", DataType::Utf8, true),
            Arc::new(StringArray::from_iter(labels)),
        ),
        (Field::new(BN, blocks.data_type().clone(), false), blocks),
        // The row's position, so that tagged rows can be told apart.
        (
            Field::new("id", DataType::UInt32, false),
            Arc::new(UInt32Array::from_iter_values(ids)),
        ),
    ]
}

/// Kinds 0 and 1 below 2³¹, 2 and 3 above it.
fn group_of(kind: u64) -> u32 {
    (1u32 << 31) - 2 + kind as u32
}

fn writers(rng: &mut Rng) -> Vec<WriterProperties> {
    let rows_per_group = [16, 50, 128][rng.below(3) as usize];
    let base = || WriterProperties::builder().set_max_row_group_size(rows_per_group);
    vec![
        base().build(),
        // Bounds cut to a prefix: still bounds, but every name's look alike.
        base().set_statistics_truncate_length(Some(8)).build(),
        // A column with no statistics rules nothing out, and must not stop the
        // others from doing so.
        base()
            .set_column_statistics_enabled(ColumnPath::from("code"), EnabledStatistics::None)
            .build(),
    ]
}

fn filter(rng: &mut Rng) -> ColumnPredicate {
    let code = |rng: &mut Rng| match rng.below(3) {
        0 => rng.below(10) as u32,
        1 => u32::MAX - rng.below(10) as u32,
        _ => (1 << 31) + rng.below(10) as u32,
    };
    match rng.below(9) {
        0 => col_in_list(
            "name",
            Arc::new(StringArray::from_iter_values((0..1 + rng.below(2)).map(
                |_| format!("a-shared-prefix-for-every-name-{}", rng.below(5)),
            ))),
        ),
        1 => col_in_list(
            "code",
            Arc::new(UInt32Array::from_iter_values(
                (0..1 + rng.below(3)).map(|_| code(rng)),
            )),
        ),
        2 => col_eq("flag", ScalarValue::Boolean(rng.below(2) == 0)),
        3 => ColumnPredicate {
            column: "code".into(),
            predicate: Arc::new(RangeGtePredicate::new(ScalarValue::UInt32(code(rng)))),
        },
        4 => ColumnPredicate {
            column: "code".into(),
            predicate: Arc::new(RangeLtePredicate::new(ScalarValue::UInt32(code(rng)))),
        },
        5 => col_in_list(
            "group",
            Arc::new(UInt32Array::from_iter_values(
                (0..1 + rng.below(2)).map(|_| group_of(rng.below(5))),
            )),
        ),
        6 => col_eq("group", ScalarValue::UInt32(group_of(rng.below(5)))),
        7 => col_in_list(
            "label",
            Arc::new(StringArray::from_iter_values(
                (0..1 + rng.below(2)).map(|_| format!("label-{}", rng.below(5))),
            )),
        ),
        _ => match rng.below(2) {
            0 => col_eq(
                "label",
                ScalarValue::Utf8(format!("label-{}", rng.below(5))),
            ),
            _ => col_eq(
                "name",
                ScalarValue::Utf8(format!("a-shared-prefix-for-every-name-{}", rng.below(5))),
            ),
        },
    }
}

fn item(rng: &mut Rng) -> RowPredicate {
    let columns = (0..1 + rng.below(2)).map(|_| filter(rng)).collect();
    if rng.below(3) == 0 {
        // Two filters in one alternative let a proof cover part of it.
        let alternatives = (0..2)
            .map(|_| (0..1 + rng.below(2)).map(|_| filter(rng)).collect())
            .collect();
        RowPredicate::with_alternatives(columns, alternatives)
    } else {
        RowPredicate::new(columns)
    }
}

/// Whether `mask` selects `row`: a row it says neither yes nor no about is not
/// selected.
fn matches(mask: &BooleanArray, row: usize) -> bool {
    mask.is_valid(row) && mask.value(row)
}

fn ids(batches: &[RecordBatch]) -> Vec<u64> {
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<UInt32Type>();
            ids.values().iter().map(|&id| id as u64).collect::<Vec<_>>()
        })
        .collect()
}

fn positions(rows: &Rows) -> Vec<u64> {
    rows.positions()
        .expect("the scan recorded positions")
        .iter()
        .flat_map(|batch| batch.values().iter().copied())
        .collect()
}

/// Covers CT-3 · INV-P16
#[test]
fn a_row_group_an_items_statistics_rule_out_holds_none_of_its_rows() {
    let mut rng = Rng(0x5EED_0049);
    let mut matched = 0;
    let mut empty = 0;

    for chunk in 0..6 {
        let wrapped_blocks = chunk % 3 == 2;
        let columns = columns(&mut rng, wrapped_blocks);
        for props in writers(&mut rng) {
            let dir = TempDir::new().unwrap();
            let (fields, arrays) = columns.iter().cloned().unzip();
            write_table_with(dir.path(), "items", fields, arrays, props);
            let reader = ParquetChunkReader::open(dir.path()).unwrap();

            let mut everything =
                ScanRequest::new(vec!["name", "code", "flag", "group", "label", BN]);
            everything.positions = true;
            let all = reader.scan_rows("items", &everything).unwrap().into_rows();
            let all_positions = all.positions().unwrap();

            for _ in 0..40 {
                let items: Vec<RowPredicate> =
                    (0..1 + rng.below(4)).map(|_| item(&mut rng)).collect();
                let refs: Vec<&RowPredicate> = items.iter().collect();
                // Below 2³¹ the row filter and a row group's bounds read a
                // wrapped block number alike; above it they do not (gap 31).
                let range = (rng.below(3) == 0).then(|| {
                    if wrapped_blocks {
                        let from = (1 << 31) - 20 + rng.below(20);
                        (from, (from + rng.below(10)).min(i32::MAX as u64))
                    } else {
                        let from = 100 + rng.below(40);
                        (from, from + rng.below(20))
                    }
                });

                let mut expected = Vec::new();
                let mut expected_tags = vec![Vec::new(); items.len()];
                for (batch, rows) in all.batches().iter().zip(all_positions) {
                    let any = or_row_predicates(&refs, batch).unwrap();
                    let each: Vec<BooleanArray> = refs
                        .iter()
                        .map(|item| item.evaluate(batch).unwrap())
                        .collect();
                    let blocks = batch.column_by_name(BN).unwrap();
                    let block = |row: usize| match blocks.as_any().downcast_ref::<UInt64Array>() {
                        Some(blocks) => blocks.value(row),
                        None => blocks
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(row) as u32 as u64,
                    };
                    for row in 0..batch.num_rows() {
                        let in_range =
                            range.is_none_or(|(from, to)| (from..=to).contains(&block(row)));
                        if !in_range || !matches(&any, row) {
                            continue;
                        }
                        expected.push(rows.value(row));
                        for (tag, mask) in expected_tags.iter_mut().zip(&each) {
                            if matches(mask, row) {
                                tag.push(rows.value(row));
                            }
                        }
                    }
                }

                // Tags take the path that evaluates every item, even a lone one.
                let tags: Vec<Vec<usize>> = (0..items.len()).map(|item| vec![item]).collect();
                for (batch_size, tagged) in [(3, false), (usize::MAX, false), (3, true)] {
                    let mut request = ScanRequest::new(vec![BN, "id"]);
                    request.predicates = refs.clone();
                    request.block_number_column = Some(BN);
                    (request.from_block, request.to_block) =
                        range.map_or((None, None), |(from, to)| (Some(from), Some(to)));
                    request.positions = true;
                    request.batch_size = batch_size;
                    if tagged {
                        request.item_tags = tags.iter().map(Vec::as_slice).collect();
                    }

                    let scanned = reader.scan_rows("items", &request).unwrap();
                    let context = format!(
                        "chunk {chunk}, range {range:?}, batches of {batch_size}, \
                         tagged {tagged}: {items:?}"
                    );
                    if tagged {
                        for (tag, expected) in tags.iter().zip(&expected_tags) {
                            let ids = ids(&scanned.matched_by(tag).unwrap());
                            assert_eq!(&ids, expected, "tag {tag:?}, {context}");
                        }
                    }
                    let returned = positions(&scanned.into_rows());
                    assert_eq!(returned, expected, "{context}");
                }
                matched += usize::from(!expected.is_empty());
                empty += usize::from(expected.is_empty());
            }
        }
    }
    assert!(
        matched > 0 && empty > 0,
        "{matched} queries matched rows, {empty} matched none"
    );
}
