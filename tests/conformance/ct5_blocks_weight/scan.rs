//! Range scans preserve rows when block statistics cannot safely prune them.
use arrow::array::{Array, ArrayRef, Int32Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use sqd_query_engine::integers::BlockNumbers;
use sqd_query_engine::scan::{ChunkReader, KeyFilter, ParquetChunkReader, ScanRequest};
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

const BN: &str = "block_number";
fn block_number_at(array: &dyn Array, row: usize) -> u64 {
    BlockNumbers::resolve(array, BN).unwrap().at(row)
}

fn rows_per_block(batches: &[RecordBatch], bn_col: &str) -> BTreeMap<u64, usize> {
    let mut counts = BTreeMap::new();
    for batch in batches {
        let idx = batch.schema().index_of(bn_col).unwrap();
        let column = batch.column(idx);
        for row in 0..batch.num_rows() {
            *counts
                .entry(block_number_at(column.as_ref(), row))
                .or_insert(0) += 1;
        }
    }
    counts
}

#[test]
fn physical_positions_survive_pruning_cascaded_filters_and_batches() {
    use crate::harness::chunk::write_table_row_groups;
    use arrow::array::{BooleanArray, StringArray, UInt64Array};
    use sqd_query_engine::scan::predicate::{col_eq, col_in_list, RowPredicate, ScalarValue};

    let dir = TempDir::new().unwrap();
    let groups = [100, 200, 300]
        .into_iter()
        .map(|start| {
            vec![
                Arc::new(UInt64Array::from((start..start + 6).collect::<Vec<_>>())) as ArrayRef,
                Arc::new(UInt32Array::from((0..6).collect::<Vec<_>>())) as ArrayRef,
                Arc::new(BooleanArray::from(vec![
                    Some(false),
                    Some(true),
                    Some(false),
                    None,
                    Some(false),
                    Some(true),
                ])) as ArrayRef,
                Arc::new(StringArray::from(
                    (start..start + 6)
                        .map(|block| format!("value-{block}"))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ]
        })
        .collect();
    write_table_row_groups(
        dir.path(),
        "items",
        vec![
            Field::new(BN, DataType::UInt64, false),
            Field::new("index", DataType::UInt32, false),
            Field::new("flag", DataType::Boolean, true),
            Field::new("payload", DataType::Utf8, false),
        ],
        groups,
    );
    let reader = ParquetChunkReader::open(dir.path()).unwrap();
    let predicate = RowPredicate::new(vec![
        col_eq("flag", ScalarValue::Boolean(true)),
        col_in_list("index", Arc::new(UInt32Array::from(vec![1, 3, 5]))),
    ]);
    for batch_size in [1, 2, usize::MAX] {
        let mut request = ScanRequest::new(vec![BN, "index"]);
        request.from_block = Some(201);
        request.to_block = Some(304);
        request.block_number_column = Some(BN);
        request.predicates = vec![&predicate];
        request.batch_size = batch_size;
        request.row_index_column = Some("position");
        let selected = reader.scan("items", &request).unwrap();
        let positions: Vec<_> = selected
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("position")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(positions, [7, 11, 13]);

        let mut fetch = ScanRequest::new(vec!["payload"]);
        fetch.row_indices = Some(&[7, 13]);
        fetch.row_index_column = Some("position");
        fetch.batch_size = batch_size;
        let batches = reader.scan("items", &fetch).unwrap();
        let payloads: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("payload")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_owned())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(payloads, ["value-201", "value-301"]);
        let fetched_positions: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("position")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(fetched_positions, [7, 13]);
        for invalid in [&[13, 7][..], &[7, 7], &[18]] {
            fetch.row_indices = Some(invalid);
            assert!(reader.scan("items", &fetch).is_err());
        }
    }
}

fn synthetic_request<'a>() -> ScanRequest<'a> {
    let mut request = ScanRequest::new(vec![BN, "row_index"]);
    request.block_number_column = Some(BN);
    request
}

/// The same inverted statistic, read one layer earlier.
///
/// Row-group pruning runs before anything else looks at these bounds, and it
/// runs before rows are read. An inverted pair reads as a
/// range starting above where the group ends, so a query whose `toBlock` falls
/// below that start skips the group — and the rows it holds inside the range
/// leave with it, with no error and nothing in the response to say so.
///
/// The range below is chosen so that only the pruning is wrong. The row filter
/// compares at the stored type, so a lower bound of one excludes the wrapped
/// values — they are negative there (gap 31) — and the two blocks that remain are
/// exactly the two the query asks for. A lower bound of zero would not: zero is
/// no bound at all, and the wrapped rows would come back as blocks far outside
/// the range.
#[test]
fn an_inverted_statistic_does_not_prune_a_row_group_away() {
    const WRAP: u64 = 1 << 31;

    let dir = TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(BN, DataType::Int32, false),
        Field::new("row_index", DataType::UInt32, false),
    ]));

    // One row group, straddling the wrap: two blocks below it, two above.
    let blocks = [WRAP - 2, WRAP - 1, WRAP, WRAP + 1];
    let stored: Vec<i32> = blocks.iter().map(|&b| b as u32 as i32).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(stored)) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32; blocks.len()])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = File::create(dir.path().join("items.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = ParquetChunkReader::open(dir.path()).unwrap();
    let mut request = synthetic_request();
    request.from_block = Some(1);
    request.to_block = Some(WRAP - 1);

    let rows = rows_per_block(&reader.scan("items", &request).unwrap(), BN);

    assert_eq!(
        rows.keys().copied().collect::<Vec<_>>(),
        vec![WRAP - 2, WRAP - 1],
        "the range holds two of the group's four blocks, and pruning on a bound \
         that reads back above the group's own end drops all four"
    );
}

/// The relation pruner reads the same statistic as the range pruner, and has to
/// refuse the same ones.
///
/// A row group whose statistic reads back inverted reports a range starting above
/// where it ends, and the overlap test below cannot pass on one: every key at or
/// above the reported minimum is above the reported maximum by construction. So
/// the group is not *sometimes* skipped, it is always skipped — and a relation
/// pull loses every row it holds, silently, while the same chunk answers a query
/// that takes the range path instead.
///
/// Covers CT-5 · INV-B7
#[test]
fn an_inverted_statistic_does_not_prune_a_relation_row_group_away() {
    let dir = TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(BN, DataType::Int32, false),
        Field::new("row_index", DataType::UInt32, false),
    ]));

    // The second row group straddles the wrap, so a writer comparing the stored
    // values records its bounds inverted.
    const WRAP: u64 = 1 << 31;
    let groups: Vec<Vec<u64>> = vec![vec![100, 101], vec![WRAP - 1, WRAP, WRAP + 1]];

    let file = File::create(dir.path().join("items.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    for blocks in &groups {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(
                    blocks.iter().map(|&b| b as u32 as i32).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0u32; blocks.len()])) as ArrayRef,
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();

    // A primary scan that pulled the three blocks of the straddling group.
    let wanted: Vec<u64> = groups[1].clone();
    let primary = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(
                wanted.iter().map(|&b| b as u32 as i32).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32; wanted.len()])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut request = synthetic_request();
    let key_filter = KeyFilter::build(&[primary], &[BN, "row_index"], &[BN, "row_index"], BN, BN);
    request.key_filter = Some(&key_filter);

    let reader = ParquetChunkReader::open(dir.path()).unwrap();
    let pulled = reader.scan("items", &request).unwrap();

    assert_eq!(
        rows_per_block(&pulled, BN),
        wanted.iter().map(|&b| (b, 1)).collect::<BTreeMap<_, _>>(),
        "the relation pruner dropped a row group on a statistic it cannot read"
    );
}
