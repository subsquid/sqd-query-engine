//! Range scans preserve rows when block statistics cannot safely prune them.
use arrow::array::{Array, ArrayRef, Int32Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use sqd_query_engine::integers::BlockNumbers;
use sqd_query_engine::scan::{ChunkReader, KeyFilter, ParquetChunkReader, Rows, ScanRequest};
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
        request.positions = true;
        let selected = reader.scan_rows("items", &request).unwrap().rows;
        assert_eq!(positions(&selected), [7, 11, 13]);

        let mut fetch = ScanRequest::new(vec!["payload"]);
        fetch.row_indices = Some(&[7, 13]);
        fetch.positions = true;
        fetch.batch_size = batch_size;
        let fetched = reader.scan_rows("items", &fetch).unwrap().rows;
        let payloads: Vec<_> = fetched
            .batches()
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
        assert_eq!(positions(&fetched), [7, 13]);
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

/// A hierarchical scan drops the rows outside its block range after it finds
/// them, and each position it reports still names the row beside it.
#[test]
fn hierarchical_positions_name_their_rows_after_the_block_range() {
    use crate::harness::chunk::write_table_row_groups;
    use arrow::array::{AsArray, ListArray, UInt64Array};
    use arrow::datatypes::{UInt32Type, UInt64Type};
    use sqd_query_engine::scan::{HierarchicalFilter, HierarchicalMode};

    // Each block holds one transaction: a root call, its child and grandchild.
    let addresses = |paths: Vec<Vec<u32>>| {
        ListArray::from_iter_primitive::<UInt32Type, _, _>(
            paths
                .into_iter()
                .map(|path| Some(path.into_iter().map(Some))),
        )
    };
    let blocks: Vec<u64> = (100..104).flat_map(|block| [block; 3]).collect();
    let calls = addresses(
        (100..104)
            .flat_map(|_| [vec![], vec![0], vec![0, 0]])
            .collect(),
    );
    let fields = vec![
        Field::new(BN, DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("address", calls.data_type().clone(), true),
    ];
    let dir = TempDir::new().unwrap();
    write_table_row_groups(
        dir.path(),
        "calls",
        fields.clone(),
        vec![vec![
            Arc::new(UInt64Array::from(blocks.clone())) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0; blocks.len()])),
            Arc::new(calls),
        ]],
    );
    let reader = ParquetChunkReader::open(dir.path()).unwrap();

    // Every root is a source, and the range keeps two of the four blocks.
    let roots = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(UInt64Array::from_iter_values(100..104)),
            Arc::new(UInt32Array::from(vec![0; 4])),
            Arc::new(addresses(vec![vec![]; 4])),
        ],
    )
    .unwrap();
    let children = HierarchicalFilter::build(
        &[roots],
        &[BN, "transaction_index"],
        "address",
        "address",
        HierarchicalMode::Children,
        false,
    );

    for batch_size in [1, 2, usize::MAX] {
        let mut request = ScanRequest::new(vec![BN]);
        request.block_number_column = Some(BN);
        request.from_block = Some(101);
        request.to_block = Some(102);
        request.hierarchical_filter = Some(&children);
        request.positions = true;
        request.batch_size = batch_size;
        let rows = reader.scan_rows("calls", &request).unwrap().rows;

        let blocks: Vec<u64> = rows
            .batches()
            .iter()
            .flat_map(|batch| {
                let blocks = batch.column_by_name(BN).unwrap();
                blocks.as_primitive::<UInt64Type>().values().to_vec()
            })
            .collect();
        assert_eq!(blocks, [101, 101, 102, 102], "batches of {batch_size}");
        assert_eq!(positions(&rows), [4, 5, 7, 8], "batches of {batch_size}");
    }
}

fn positions(rows: &Rows) -> Vec<u64> {
    rows.positions()
        .expect("the scan recorded positions")
        .iter()
        .flat_map(|batch| batch.values().iter().copied())
        .collect()
}
