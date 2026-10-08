//! INV-P16 per item: a row group whose statistics rule an item out holds no row
//! that item matches.
//!
//! Items are ORed into one scan, and each runs only on the row groups its own
//! statistics leave in. What can go wrong is a bound read at the wrong width, an
//! alternative judged as if it were required, or an item dropped from a row
//! group another item kept it in. Each loses rows silently, so the answer is
//! compared with every row evaluated by hand.

use crate::harness::chunk::write_table_with;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use sqd_query_engine::scan::predicate::{
    col_eq, col_in_list, or_row_predicates, ColumnPredicate, RangeGtePredicate, RangeLtePredicate,
    RowPredicate, ScalarValue,
};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest};
use std::sync::Arc;
use tempfile::TempDir;

const ROWS: usize = 900;
const BN: &str = "block_number";
const POSITION: &str = "position";

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

    let names = rows
        .iter()
        .map(|&(kind, _, _)| format!("a-shared-prefix-for-every-name-{kind}"));
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
        (Field::new(BN, blocks.data_type().clone(), false), blocks),
    ]
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
    match rng.below(5) {
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
        _ => ColumnPredicate {
            column: "code".into(),
            predicate: Arc::new(RangeLtePredicate::new(ScalarValue::UInt32(code(rng)))),
        },
    }
}

fn item(rng: &mut Rng) -> RowPredicate {
    let columns = (0..1 + rng.below(2)).map(|_| filter(rng)).collect();
    if rng.below(3) == 0 {
        let alternatives = (0..2).map(|_| vec![filter(rng)]).collect();
        RowPredicate::with_alternatives(columns, alternatives)
    } else {
        RowPredicate::new(columns)
    }
}

fn positions(batches: &[arrow::record_batch::RecordBatch]) -> Vec<u64> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name(POSITION).unwrap();
            let column = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            column.values().to_vec()
        })
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

            let mut everything = ScanRequest::new(vec!["name", "code", "flag", BN]);
            everything.row_index_column = Some(POSITION);
            let all = reader.scan("items", &everything).unwrap();

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
                for batch in &all {
                    let any = or_row_predicates(&refs, batch).unwrap();
                    let blocks = batch.column_by_name(BN).unwrap();
                    let block = |row: usize| match blocks.as_any().downcast_ref::<UInt64Array>() {
                        Some(blocks) => blocks.value(row),
                        None => blocks
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(row) as u32 as u64,
                    };
                    let rows = batch.column_by_name(POSITION).unwrap();
                    let rows = rows.as_any().downcast_ref::<UInt64Array>().unwrap();
                    for row in 0..batch.num_rows() {
                        let in_range =
                            range.is_none_or(|(from, to)| (from..=to).contains(&block(row)));
                        if in_range && any.is_valid(row) && any.value(row) {
                            expected.push(rows.value(row));
                        }
                    }
                }

                for batch_size in [3, usize::MAX] {
                    let mut request = ScanRequest::new(vec![BN]);
                    request.predicates = refs.clone();
                    request.block_number_column = Some(BN);
                    (request.from_block, request.to_block) =
                        range.map_or((None, None), |(from, to)| (Some(from), Some(to)));
                    request.row_index_column = Some(POSITION);
                    request.batch_size = batch_size;

                    let returned = positions(&reader.scan("items", &request).unwrap());
                    assert_eq!(
                        returned, expected,
                        "chunk {chunk}, range {range:?}, batches of {batch_size}: {items:?}"
                    );
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
