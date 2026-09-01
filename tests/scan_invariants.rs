//! Scanner invariants: the two scan entry points must agree.
//!
//! `scan` reads every matching row group; `scan_waves_until_budget` walks them in
//! block order and stops early once a response-weight budget is crossed. The
//! second is an optimisation of the first, so the only thing that may differ
//! between them is *how many blocks* come back — never the contents of a block
//! that does come back.

use arrow::array::{Array, ArrayRef, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use proptest::prelude::*;
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tempfile::TempDir;

fn fixture_chunk(dataset: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(dataset)
        .join("chunk")
}

/// Whether the fixture tree is checked out at all. It is not in the repository,
/// so in a plain checkout the tests that read it have nothing to run against.
///
/// A skip that looks like a pass is what this branch set out to remove, so it is
/// only allowed where nobody was promised coverage. Setting
/// `SQD_REQUIRE_FIXTURES=1` — which CI should — turns an absent tree into a
/// failure rather than a silent green run.
fn fixture_tree_is_present() -> bool {
    if fixture_chunk("ethereum").is_dir() {
        return true;
    }

    assert!(
        std::env::var_os("SQD_REQUIRE_FIXTURES").is_none(),
        "SQD_REQUIRE_FIXTURES is set but tests/fixtures is not checked out, so these \
         tests would report green having compared nothing"
    );

    false
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
    if !fixture_tree_is_present() {
        return;
    }

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
    if !fixture_tree_is_present() {
        return;
    }

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
        prop_assume!(fixture_tree_is_present());

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

// ---------------------------------------------------------------------------
// A block-partitioned layout, which the fixture chunk is not
// ---------------------------------------------------------------------------
//
// Every row group of the fixture's `statediffs` spans the whole chunk, so no
// early stop is ever sound there and the tests above pass on a walk that reads
// everything. Chunks written today are partitioned: row group `g` covers a slice
// of the block range and *shares its boundary block* with row group `g + 1`.
//
// That shared block is the whole difficulty. A stop rule phrased as "is there a
// gap between what I have read and what I have not" never fires on this layout,
// because the gap is zero. The rule that does work is "which blocks can no
// unread row group still add to", and the boundary block is not one of them.

const SYNTH_ROWS_PER_BLOCK: usize = 4;
const SYNTH_BLOCKS_PER_GROUP: u64 = 5;
const SYNTH_GROUPS: u64 = 10;

/// Rows of one row group: the blocks it owns whole, plus half of the block it
/// shares with each neighbour.
fn synthetic_group_rows(group: u64) -> Vec<(u64, u32)> {
    let first = group * SYNTH_BLOCKS_PER_GROUP;
    let last = first + SYNTH_BLOCKS_PER_GROUP;

    let mut rows = Vec::new();
    for block in first..=last {
        // The block shared with the previous group contributes its tail here, the
        // one shared with the next group its head. Interior blocks are whole.
        let (from, to) = match block {
            b if b == first && group > 0 => (SYNTH_ROWS_PER_BLOCK / 2, SYNTH_ROWS_PER_BLOCK),
            b if b == last && group + 1 < SYNTH_GROUPS => (0, SYNTH_ROWS_PER_BLOCK / 2),
            b if b == last => continue, // the file ends before the next group's half
            _ => (0, SYNTH_ROWS_PER_BLOCK),
        };
        for index in from..to {
            rows.push((block, index as u32));
        }
    }
    rows
}

/// Write a chunk whose row groups are block-partitioned with shared boundaries,
/// and return the directory holding it.
fn synthetic_chunk() -> TempDir {
    let dir = TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(BN, DataType::UInt64, false),
        Field::new("row_index", DataType::UInt32, false),
    ]));

    let file = File::create(dir.path().join("items.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    for group in 0..SYNTH_GROUPS {
        let rows = synthetic_group_rows(group);
        let blocks: Vec<u64> = rows.iter().map(|(b, _)| *b).collect();
        let indexes: Vec<u32> = rows.iter().map(|(_, i)| *i).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(blocks)) as ArrayRef,
                Arc::new(UInt32Array::from(indexes)) as ArrayRef,
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        // One row group per batch, which is what puts the shared block in two of
        // them.
        writer.flush().unwrap();
    }
    writer.close().unwrap();

    dir
}

fn synthetic_request<'a>() -> ScanRequest<'a> {
    let mut request = ScanRequest::new(vec![BN, "row_index"]);
    request.block_number_column = Some(BN);
    request
}

/// The walk must stop somewhere short of the end once the budget is crossed.
///
/// Reading on to the last row group is not a wrong *answer* — the exact trim
/// runs afterwards — but it is the whole point of the walk: on a real chunk this
/// is the difference between decoding four row groups and decoding forty-nine,
/// and the memory that goes with them.
#[test]
fn a_budget_stop_cuts_the_scan_on_a_partitioned_chunk() {
    let dir = synthetic_chunk();
    let reader = ParquetChunkReader::open(dir.path()).unwrap();

    let full = rows_per_block(&reader.scan("items", &synthetic_request()).unwrap(), BN);
    assert_eq!(
        full.len() as u64,
        SYNTH_GROUPS * SYNTH_BLOCKS_PER_GROUP,
        "the synthetic chunk should hold every block once"
    );

    // One row group per wave, and a budget one row group's worth of rows cannot
    // cover, so the walk crosses it on the first wave.
    let mut cumulative = 0u64;
    let mut weight_of = |wave: &[RecordBatch]| {
        cumulative += wave.iter().map(|b| b.num_rows() as u64).sum::<u64>();
        cumulative
    };
    let stopped = rows_per_block(
        &reader
            .scan_budget("items", &synthetic_request(), 1, 1, &mut weight_of)
            .unwrap(),
        BN,
    );

    assert!(
        !stopped.is_empty(),
        "a budget stop must still return at least one block"
    );
    assert!(
        stopped.len() < full.len(),
        "the walk read all {} blocks instead of stopping: adjacent row groups share \
         their boundary block, so a stop rule that needs a gap between them never fires",
        full.len()
    );

    for (block, rows) in &stopped {
        assert_eq!(
            full.get(block),
            Some(rows),
            "block {block} came back with {rows} of its {:?} rows",
            full.get(block)
        );
    }
}

/// The block on the boundary is the one a partial answer would leak, so name it:
/// it belongs to the row group the walk stopped before, and must not be emitted
/// with only the half this side of the line.
#[test]
fn a_budget_stop_drops_the_block_it_shares_with_the_next_row_group() {
    let dir = synthetic_chunk();
    let reader = ParquetChunkReader::open(dir.path()).unwrap();

    let mut cumulative = 0u64;
    let mut weight_of = |wave: &[RecordBatch]| {
        cumulative += wave.iter().map(|b| b.num_rows() as u64).sum::<u64>();
        cumulative
    };
    let stopped = rows_per_block(
        &reader
            .scan_budget("items", &synthetic_request(), 1, 1, &mut weight_of)
            .unwrap(),
        BN,
    );

    let shared = SYNTH_BLOCKS_PER_GROUP; // the last block of row group 0
    assert_eq!(
        stopped.keys().copied().max(),
        Some(shared - 1),
        "the walk read row group 0 and stopped; block {shared} is the half of it that \
         row group 1 completes, so the response ends one block below"
    );
}
