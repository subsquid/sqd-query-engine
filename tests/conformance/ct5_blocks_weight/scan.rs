//! Scanner invariants: the two scan entry points must agree.
//!
//! `scan` reads every matching row group; `scan_waves_until_budget` walks them in
//! block order and stops early once a response-weight budget is crossed. The
//! second is an optimisation of the first, so the only thing that may differ
//! between them is *how many blocks* come back — never the contents of a block
//! that does come back.

use arrow::array::{Array, ArrayRef, Int32Array, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::integers::BlockNumbers;
use sqd_query_engine::scan::{BudgetScan, ChunkReader, KeyFilter, ParquetChunkReader, ScanRequest};
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::{Arc, OnceLock};
use tempfile::TempDir;

use crate::harness::fixtures::{fixture_chunk, fixture_tree_is_present};

/// Block numbers widened by the engine's own rule.
///
/// A chain of its own here is how a test comes to disagree with the code it
/// checks: above 2³¹ a signed width sign-extends one way and reinterprets the
/// other, and the test would assert the disagreement rather than catch it.
fn block_number_at(array: &dyn Array, row: usize) -> u64 {
    BlockNumbers::resolve(array, "block_number")
        .expect("block numbers must be readable")
        .at(row)
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

/// The walk's stop rule, as the engine states it: a row weighs `ROW_WEIGHT`, a
/// block's weight counts only once no unread row group can add to it, and the
/// walk stops when the settled blocks outweigh the budget.
///
/// A callback that weighed everything read instead would stop on rows belonging
/// to a block the next row group still owns — the stop point would then move
/// with the wave width, which is the thing the walk may not let a client see.
struct SettledWeight {
    open: BTreeMap<u64, u64>,
    settled: u64,
}

impl SettledWeight {
    const ROW_WEIGHT: u64 = 64;

    fn new() -> Self {
        Self {
            open: BTreeMap::new(),
            settled: 0,
        }
    }

    fn fold(&mut self, wave: &[RecordBatch], boundary: Option<u64>) -> u64 {
        for (block, rows) in rows_per_block(wave, BN) {
            *self.open.entry(block).or_insert(0) += rows as u64 * Self::ROW_WEIGHT;
        }

        let closed = match boundary {
            Some(b) => {
                let still_open = self.open.split_off(&b);
                std::mem::replace(&mut self.open, still_open)
            }
            None => std::mem::take(&mut self.open),
        };
        self.settled += closed.values().sum::<u64>();

        self.settled
    }
}

/// Run the budget walk with a per-row weight, so the stop point is a function of
/// the budget alone.
fn scan_with_budget(budget: u64, wave_size: usize) -> Vec<RecordBatch> {
    let mut weight = SettledWeight::new();
    let mut settled_weight =
        |wave: &[RecordBatch], boundary: Option<u64>| weight.fold(wave, boundary);

    reader()
        .scan_budget(TABLE, &request(), wave_size, budget, &mut settled_weight)
        .unwrap()
        .batches
}

/// The invariant a paginating client depends on: a block in the response is a
/// complete block. Stopping the scan early may drop blocks off the end; it may
/// never drop rows out of a block that is still emitted, because the response
/// gives the client no way to tell that happened — it reads as "this block had
/// fewer state diffs".
///
/// Covers CT-5 · INV-B4
#[test]
#[ignore = "requires external fixture data"]
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
#[ignore = "requires external fixture data"]
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
    // Where the recorded counterexamples live. The default, `SourceParallel`,
    // resolves against the nearest `lib.rs` or `main.rs` — so merging the suite
    // into one target silently moved the file out from under the checked-in one,
    // and proptest treats a missing persistence file as "no regressions".
    #![proptest_config(ProptestConfig {
        cases: 48,
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    })]

    /// Covers CT-5 · INV-B4
    #[test]
    #[ignore = "requires external fixture data"]
    fn no_budget_or_wave_size_makes_a_block_partial(
        budget in 0u64..40_000_000,
        wave_size in 1usize..17,
    ) {
        // Not `prop_assume!`: an assumption that never holds is a proptest
        // failure, so a checkout without fixtures would report a red suite
        // rather than a skipped case.
        if !fixture_tree_is_present() {
            return Ok(());
        }

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

/// The same stop rule as the fixture walk, at a weight of one per row so the
/// budget can be read as a row count.
fn scan_synthetic_with_budget(
    reader: &ParquetChunkReader,
    budget: u64,
    wave_size: usize,
) -> BudgetScan {
    let mut open: BTreeMap<u64, u64> = BTreeMap::new();
    let mut settled = 0u64;
    let mut settled_weight = |wave: &[RecordBatch], boundary: Option<u64>| {
        for (block, rows) in rows_per_block(wave, BN) {
            *open.entry(block).or_insert(0) += rows as u64;
        }
        let closed = match boundary {
            Some(b) => {
                let still_open = open.split_off(&b);
                std::mem::replace(&mut open, still_open)
            }
            None => std::mem::take(&mut open),
        };
        settled += closed.values().sum::<u64>();
        settled
    };

    reader
        .scan_budget(
            "items",
            &synthetic_request(),
            wave_size,
            budget,
            &mut settled_weight,
        )
        .unwrap()
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
    let stopped = rows_per_block(&scan_synthetic_with_budget(&reader, 1, 1).batches, BN);

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

    let stopped = rows_per_block(&scan_synthetic_with_budget(&reader, 1, 1).batches, BN);

    let shared = SYNTH_BLOCKS_PER_GROUP; // the last block of row group 0
    assert_eq!(
        stopped.keys().copied().max(),
        Some(shared - 1),
        "the walk read row group 0 and stopped; block {shared} is the half of it that \
         row group 1 completes, so the response ends one block below"
    );
}

/// The other layout the catalog claims is block-sorted and is not.
///
/// `statediffs` is declared with a block-leading sort key, and one of the two
/// archivers wrote it sorted by address instead: every row group then spans most
/// of the chunk, and no prefix of them settles any block. A walk that took the
/// declared key at its word would compute a boundary from row groups that
/// overlap, settle blocks a later group still owns, and cut the answer at a
/// block picked out of a layout that isn't there.
///
/// The statistics say which layout the file in hand has, so the walk reads them
/// and falls back to reading everything — which is what the reference does on
/// every layout.
#[test]
fn an_overlapping_layout_is_read_whole_rather_than_cut() {
    let dir = TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(BN, DataType::UInt64, false),
        Field::new("row_index", DataType::UInt32, false),
    ]));

    // Four row groups, each holding a different set of addresses and so a
    // different scatter of blocks: they start at 0, 2, 5 and 7 and all run to
    // the end. The starts differ, which is what a walk reading them in order
    // mistakes for a partition; the ranges overlap, which is what makes any cut
    // it draws from them wrong.
    const LAST: u64 = 19;
    let file = File::create(dir.path().join("items.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    for (group, first) in [0u64, 2, 5, 7].into_iter().enumerate() {
        let blocks: Vec<u64> = (first..=LAST).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(blocks.clone())) as ArrayRef,
                Arc::new(UInt32Array::from(vec![group as u32; blocks.len()])) as ArrayRef,
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();

    let reader = ParquetChunkReader::open(dir.path()).unwrap();
    let stopped = scan_synthetic_with_budget(&reader, 1, 1);

    assert_eq!(
        stopped.complete_through, None,
        "the walk reported a cut on a layout that settles nothing below it"
    );
    assert_eq!(
        rows_per_block(&stopped.batches, BN),
        rows_per_block(&reader.scan("items", &synthetic_request()).unwrap(), BN),
        "a budget of one stopped a walk that had no sound place to stop"
    );
}

/// The contract `complete_through` states, asserted directly: a block at or
/// below the cut is a block the walk read in full.
///
/// Everything else the walk does is an optimisation the response can absorb —
/// reading too much costs memory, stopping too early costs a round trip. This is
/// the one claim the response is assembled on top of, and the one that turns a
/// wrong layout guess into rows that vanish with a 200.
fn assert_cut_covers_what_it_claims(dir: &std::path::Path, budget: u64, wave_size: usize) {
    let reader = ParquetChunkReader::open(dir).unwrap();
    let stopped = scan_synthetic_with_budget(&reader, budget, wave_size);

    let Some(cut) = stopped.complete_through else {
        return; // no cut claimed, nothing to check
    };

    let full = rows_per_block(&reader.scan("items", &synthetic_request()).unwrap(), BN);
    let walked = rows_per_block(&stopped.batches, BN);

    for (block, rows) in full.range(..=cut) {
        assert_eq!(
            walked.get(block),
            Some(rows),
            "the walk claims block {block} is complete at or below its cut of {cut}, \
             but returned {:?} of its {rows} rows",
            walked.get(block).copied().unwrap_or(0)
        );
    }
}

/// A signed `Int32` block number above 2³¹ wraps, and a writer comparing the
/// wrapped values signed records the row group's statistics inverted: the
/// minimum is the block above the wrap, the maximum the highest block below it.
///
/// Widening those bits back to `u64` — which is what every block-number reader
/// here does — yields `min > max`, a range that excludes most of the group. The
/// group then sorts after row groups it precedes, and its understated maximum
/// clears the overlap check, so the walk would call blocks settled that this
/// group has not been read for. Whether the layout underneath happens to be
/// partitioned is beside the point: the statistics no longer say.
#[test]
fn a_wrapped_int32_statistic_draws_no_cut() {
    let dir = TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(BN, DataType::Int32, false),
        Field::new("row_index", DataType::UInt32, false),
    ]));

    // Blocks below the wrap in one row group, blocks straddling it in the next.
    const WRAP: u64 = 1 << 31;
    let groups: Vec<Vec<u64>> = vec![
        vec![100, 101, 102],
        vec![WRAP - 3, WRAP - 2, WRAP - 1, WRAP, WRAP + 1],
    ];

    let file = File::create(dir.path().join("items.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    for blocks in &groups {
        let stored: Vec<i32> = blocks
            .iter()
            .flat_map(|&b| [b as u32 as i32, b as u32 as i32])
            .collect();
        let indexes: Vec<u32> = blocks.iter().flat_map(|_| [0u32, 1]).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(stored)) as ArrayRef,
                Arc::new(UInt32Array::from(indexes)) as ArrayRef,
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();

    let reader = ParquetChunkReader::open(dir.path()).unwrap();
    let stopped = scan_synthetic_with_budget(&reader, 1, 1);

    assert_eq!(
        stopped.complete_through, None,
        "the walk drew a cut from a statistic that reads back inverted"
    );
    assert_eq!(
        rows_per_block(&stopped.batches, BN),
        rows_per_block(&reader.scan("items", &synthetic_request()).unwrap(), BN),
        "the walk stopped on an order the statistics do not establish"
    );

    for wave_size in [1, 2, 4] {
        for budget in [0, 1, 4, 12] {
            assert_cut_covers_what_it_claims(dir.path(), budget, wave_size);
        }
    }
}

/// The same claim over the layout the walk is built for, at every budget and
/// wave width: a cut is only ever drawn over blocks that came back whole.
#[test]
fn a_cut_never_outruns_the_rows_behind_it() {
    let partitioned = synthetic_chunk();

    for wave_size in [1, 2, 3, 17] {
        for budget in [0, 1, 5, 12, 40, 1_000] {
            assert_cut_covers_what_it_claims(partitioned.path(), budget, wave_size);
        }
    }
}

/// The same inverted statistic, read one layer earlier.
///
/// Row-group pruning runs before anything else looks at these bounds, and it
/// runs on every scan, not just the budget walk. An inverted pair reads as a
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

/// A block number the scan cannot place refuses the chunk, on every path.
///
/// The value is what every layer puts a row somewhere by: which row group can
/// still own it, what it weighs, which block it is emitted under. Read through
/// the width-tolerant reader a null returns the slot's placeholder, so the row
/// silently becomes block 0's — a wrong answer, and a different wrong answer in
/// each reader. INV-E1 asks for the error instead, and the scan is where it is
/// raised, so that no query shape can be the one that answers.
///
/// Covers CT-5 · INV-E1
#[test]
fn a_block_number_the_scan_cannot_place_is_refused() {
    let dir = TempDir::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(BN, DataType::UInt64, true),
        Field::new("row_index", DataType::UInt32, false),
    ]));

    // Two partitioned row groups, the first carrying one row whose block number
    // is missing.
    let groups: Vec<Vec<Option<u64>>> = vec![
        vec![Some(0), Some(1), None, Some(2)],
        vec![Some(3), Some(4), Some(5)],
    ];

    let file = File::create(dir.path().join("items.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    for blocks in &groups {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(blocks.clone())) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0u32; blocks.len()])) as ArrayRef,
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }
    writer.close().unwrap();

    let reader = ParquetChunkReader::open(dir.path()).unwrap();

    let plain = reader.scan("items", &synthetic_request()).unwrap_err();
    assert_eq!(
        error_kind(&plain),
        Some(ErrorKind::MalformedChunkData),
        "a plain scan answered from a column it cannot place every row against"
    );

    let mut settled = |_: &[RecordBatch], _: Option<u64>| 0u64;
    let walked = reader
        .scan_budget("items", &synthetic_request(), 1, 1, &mut settled)
        .unwrap_err();
    assert_eq!(
        error_kind(&walked),
        Some(ErrorKind::MalformedChunkData),
        "the budget walk answered where the plain scan refused"
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
