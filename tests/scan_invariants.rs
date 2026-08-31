//! Scanner invariants: the two scan entry points must agree.
//!
//! `scan` reads every matching row group; `scan_waves_until_budget` walks them in
//! block order and stops early once a response-weight budget is crossed. The
//! second is an optimisation of the first, so the only thing that may differ
//! between them is *how many blocks* come back — never the contents of a block
//! that does come back.

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use proptest::prelude::*;
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

fn fixture_chunk(dataset: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(dataset)
        .join("chunk")
}

/// Block numbers are stored with whatever integer width the writer chose, so
/// widen every one of them to `u64` before comparing.
fn block_number_at(array: &dyn Array, row: usize) -> u64 {
    use arrow::array::{Int16Array, Int32Array, Int64Array, UInt16Array, UInt32Array, UInt64Array};

    macro_rules! try_as {
        ($($ty:ty),+) => {
            $(if let Some(a) = array.as_any().downcast_ref::<$ty>() {
                return a.value(row) as u64;
            })+
        };
    }
    try_as!(
        Int16Array,
        Int32Array,
        Int64Array,
        UInt16Array,
        UInt32Array,
        UInt64Array
    );
    panic!("unsupported block number type: {:?}", array.data_type());
}

/// How many rows each block contributed, which is what "the block came back
/// whole" is measured against.
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

/// The EVM state-diff table: 144 row groups, every one of them spanning the whole
/// chunk's block range. Its declared sort key leads with the block number, so the
/// engine routes it to the budget walk — and the physical layout gives the walk
/// nothing to cut on.
const TABLE: &str = "statediffs";
const BN: &str = "block_number";
const FIRST_BLOCK: u64 = 17881390;
const LAST_BLOCK: u64 = 17882786;

fn request<'a>() -> ScanRequest<'a> {
    let mut request = ScanRequest::new(vec![BN, "transaction_index", "address", "key"]);
    request.block_number_column = Some(BN);
    request.from_block = Some(FIRST_BLOCK);
    request.to_block = Some(LAST_BLOCK);
    request
}

/// Opening the chunk memory-maps 144 row groups' metadata; the property below
/// runs dozens of cases against it, so open it once.
fn reader() -> &'static ParquetChunkReader {
    static READER: OnceLock<ParquetChunkReader> = OnceLock::new();
    READER.get_or_init(|| ParquetChunkReader::open(&fixture_chunk("ethereum")).unwrap())
}

/// Rows per block for an unbudgeted scan — what every budgeted scan is compared
/// against.
fn whole_table() -> &'static BTreeMap<u64, usize> {
    static FULL: OnceLock<BTreeMap<u64, usize>> = OnceLock::new();
    FULL.get_or_init(|| rows_per_block(&reader().scan(TABLE, &request()).unwrap(), BN))
}

/// Run the budget walk with a per-row weight, so the stop point is a function of
/// the budget alone.
fn scan_with_budget(budget: u64, wave_size: usize) -> Vec<RecordBatch> {
    const ROW_WEIGHT: u64 = 64;

    let mut cumulative = 0u64;
    let mut weight_of = |wave: &[RecordBatch]| {
        cumulative += wave.iter().map(|b| b.num_rows() as u64).sum::<u64>() * ROW_WEIGHT;
        cumulative
    };
    reader()
        .scan_budget(TABLE, &request(), wave_size, budget, &mut weight_of)
        .unwrap()
}

/// The invariant a paginating client depends on: a block in the response is a
/// complete block. Stopping the scan early may drop blocks off the end; it may
/// never drop rows out of a block that is still emitted, because the response
/// gives the client no way to tell that happened — it reads as "this block had
/// fewer state diffs".
#[test]
fn a_budget_stop_never_emits_a_partial_block() {
    let full = whole_table();
    assert!(!full.is_empty(), "the fixture must carry state diffs");

    for budget in [0, 1, 1_000, 100_000, 1_000_000, 10_000_000] {
        let stopped = rows_per_block(&scan_with_budget(budget, 4), BN);

        for (block, rows) in &stopped {
            assert_eq!(
                full.get(block),
                Some(rows),
                "budget {budget}: block {block} came back with {rows} rows, the whole block has {:?}",
                full.get(block),
            );
        }
    }
}

/// A budget stop must also make progress: an engine that answers "no blocks" for
/// a range it holds data for leaves a paginating client with nowhere to go.
#[test]
fn a_budget_stop_still_returns_at_least_one_block() {
    for budget in [0, 1, 1_000] {
        let stopped = rows_per_block(&scan_with_budget(budget, 4), BN);
        assert!(
            !stopped.is_empty(),
            "budget {budget} returned nothing at all"
        );
    }
}

proptest! {
    // Row-group layout is fixed by the fixture, so the interesting axes are where
    // the budget lands and how wide a wave is. Both are engine-chosen numbers a
    // client never sees, and neither may change an answer.
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    #[test]
    fn no_budget_or_wave_size_makes_a_block_partial(
        budget in 0u64..40_000_000,
        wave_size in 1usize..17,
    ) {
        let full = whole_table();
        let stopped = rows_per_block(&scan_with_budget(budget, wave_size), BN);

        for (block, rows) in &stopped {
            prop_assert_eq!(
                full.get(block),
                Some(rows),
                "budget {} wave {}: block {} is partial",
                budget, wave_size, block
            );
        }
        prop_assert!(!stopped.is_empty(), "budget {} returned no blocks", budget);
    }
}
