//! Response-budget and block-selection tests on synthetic chunks, covering the
//! spots the fixture suite can't reach: budget trimming (via oversized weight
//! columns), boundary-block handling under the phase-1 scan cutoff, empty
//! results, and JSON/Arrow parity of the reported block range.

use sqd_query_engine::output::{execute_chunk, execute_chunk_arrow};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;

use crate::harness::json::{block_numbers, parse_response};
use crate::harness::synthetic::{
    catalog, logs_query, narrow_weighted_chunk, paged_at, part_blocks, part_log_rows,
    partitioned_chunk, run, uniform, weighted_chunk, BLOCKS, MB,
};

/// Regression test for the phase-1 cutoff bug: a single-table full scan whose
/// budget cutoff falls below `toBlock` must not include the range-end boundary
/// block (its item rows were never scanned) — neither in the output nor in the
/// reported last block.
///
/// Covers CT-5 · INV-B3
#[test]
fn budget_trim_excludes_range_end_boundary_block() {
    let chunk = weighted_chunk(BLOCKS, &uniform(BLOCKS, 15 * MB), &[]);
    let meta = catalog();
    let blocks = run(&meta, &chunk, logs_query()).unwrap();

    assert_eq!(blocks.num_blocks(), 1);
    assert_eq!(blocks.first_block(), 10);
    assert_eq!(blocks.last_block(), 10);

    let lines = parse_response(&blocks.into_json_lines());
    assert_eq!(block_numbers(&lines), vec![10]);
    assert_eq!(lines[0]["logs"].as_array().unwrap().len(), 1);
}

/// Without trimming, every item block is included and the reported range spans
/// the whole data.
///
/// Covers CT-5 · INV-B2
#[test]
fn untrimmed_scan_includes_all_blocks() {
    let chunk = weighted_chunk(BLOCKS, &uniform(BLOCKS, 10), &[]);
    let meta = catalog();
    let blocks = run(&meta, &chunk, logs_query()).unwrap();

    assert_eq!((blocks.first_block(), blocks.last_block()), (10, 14));
    let lines = parse_response(&blocks.into_json_lines());
    assert_eq!(block_numbers(&lines), BLOCKS);
}

/// The first/last blocks of the scanned range are emitted as header-only
/// entries even when they contain no matching items.
///
/// Covers CT-5 · INV-B3
#[test]
fn boundary_blocks_emitted_without_items() {
    let chunk = weighted_chunk(BLOCKS, &[(12, 10)], &[]);
    let meta = catalog();
    let blocks = run(&meta, &chunk, logs_query()).unwrap();

    let lines = parse_response(&blocks.into_json_lines());
    assert_eq!(block_numbers(&lines), vec![10, 12, 14]);
    assert!(lines[0].get("logs").is_none());
    assert_eq!(lines[1]["logs"].as_array().unwrap().len(), 1);
    assert!(lines[2].get("logs").is_none());
}

/// Budget trimming with two item tables (the wave-budget path, not phase-1):
/// per-block weight is summed across tables, and the reported range reflects
/// the trim.
///
/// Covers CT-5 · INV-B6
#[test]
fn multi_table_trim_reports_true_last_block() {
    let weights: Vec<(u64, u64)> = BLOCKS.iter().map(|&b| (b, 15 * MB)).collect();
    let chunk = weighted_chunk(BLOCKS, &weights, &weights);
    let query = serde_json::json!({
        "type": "test",
        "fromBlock": 10,
        "toBlock": 14,
        "logs": [{}],
        "transactions": [{}],
        "fields": {
            "block": {"number": true},
            "log": {"data": true},
            "transaction": {"input": true}
        }
    });
    let meta = catalog();
    let blocks = run(&meta, &chunk, query).unwrap();

    // 15MB (logs) + 15MB (transactions) per block: only the first block fits.
    assert_eq!(blocks.last_block(), 10);
    let lines = parse_response(&blocks.into_json_lines());
    assert_eq!(block_numbers(&lines), vec![10]);
    assert_eq!(lines[0]["logs"].as_array().unwrap().len(), 1);
    assert_eq!(lines[0]["transactions"].as_array().unwrap().len(), 1);
}

#[test]
#[should_panic]
fn write_next_block_panics_when_exhausted() {
    let meta = catalog();
    let chunk = weighted_chunk(&[10], &[(10, 10)], &[]);
    let mut blocks = run(&meta, &chunk, logs_query()).unwrap();
    let mut out = Vec::new();
    blocks.write_next_block(&mut out);
    blocks.write_next_block(&mut out);
}

/// The Arrow output reports the same trimmed block range as the JSON output.
#[test]
fn arrow_json_parity_on_trim() {
    let chunk = weighted_chunk(BLOCKS, &uniform(BLOCKS, 15 * MB), &[]);
    let meta = catalog();
    let parsed = parse_query(logs_query().to_string().as_bytes(), &meta).unwrap();
    let plan = compile(&parsed, &meta).unwrap();
    let reader = ParquetChunkReader::open(chunk.path()).unwrap();

    let json = execute_chunk(&plan, &meta, &reader, false)
        .unwrap()
        .unwrap();
    let arrow = execute_chunk_arrow(&plan, &meta, &reader, false, false)
        .unwrap()
        .unwrap();

    assert_eq!(arrow.num_blocks(), json.num_blocks());
    assert_eq!(arrow.first_block(), json.first_block());
    assert_eq!(arrow.last_block(), json.last_block());
    assert!(!arrow.data().is_empty());
}

// ---------------------------------------------------------------------------
// Paging over a block-partitioned chunk
// ---------------------------------------------------------------------------
//
// The budget walk reads one table in block order and stops where its weight
// crosses the budget. Every other table was read for the whole range, so a
// response assembled without carrying that stop point out to block selection
// ends past it: the blocks above the cut come back with the other tables' rows
// and none of the walked table's, and `lastBlock` names the top of them. The
// client resumes above that and the rows below are gone, with a 200 and nothing
// to notice.
//
// This runs the walk through the whole response, which is what the scanner tests
// in `scan.rs` cannot see: they assert on the rows the walk returns, and the rows
// the walk returns were never the wrong part. The pool-size half of the property
// sits with the other determinism tests, in `ct6_output::determinism`.

/// INV-B7's own test, run on a table that takes the budget walk: the pages
/// concatenate back into the whole chunk, with no block skipped and none served
/// twice.
///
/// Run at three pool sizes because the pool sizes the wave: at one row group a
/// wave the walk stops on the first, at seventeen it reads the chunk in one go
/// and never stops at all. Only the narrow pools reach the cut, and the answer
/// has to be the same either way.
///
/// Covers CT-5 · INV-B4, INV-B7
#[test]
fn paging_a_partitioned_chunk_loses_no_block() {
    let meta = catalog();
    // A megabyte a log: a plain block costs that, the block two row groups share
    // costs forty, and the budget holds twenty. The walk crosses the budget on
    // the half of the shared block it reads first, while the blocks it can serve
    // still add up to five.
    let chunk = partitioned_chunk(MB);
    let to = *part_blocks().last().unwrap();

    for threads in [1, 2, 17] {
        let (paged, last_blocks) = paged_at(&meta, &chunk, to, threads);

        assert!(
            last_blocks.len() > 3,
            "at {threads} threads the budget did not split this chunk, so the test proves \
             nothing: {last_blocks:?}"
        );
        assert_eq!(
            paged,
            part_log_rows(),
            "at {threads} threads the pages ended at {last_blocks:?} and did not add back up \
             to the chunk"
        );
    }
}

/// A chunk whose integers are narrowed to sixteen bits still weighs what it
/// weighs.
///
/// The weight model reads two columns by hand — the block number, to know whose
/// weight a row is, and the `*_size` companion, to know how much. A width either
/// read cannot resolve does not raise anything: the row weighs its fixed part, or
/// belongs to no block at all, and the response the budget was meant to cap goes
/// out whole. Both readers now resolve through the one list of widths, so this
/// asserts the trim happens at sixteen bits exactly as it does at sixty-four.
///
/// Covers CT-5 · INV-B9
#[test]
fn a_narrow_size_column_still_reaches_the_budget() {
    // 65 535 is all a `UInt16` size can claim, so the budget is crossed by row
    // count: 480 logs over 8 blocks is about 31 MB against a 20 MB cap.
    const SIZE: u64 = u16::MAX as u64;
    const LOGS_PER_BLOCK: usize = 60;

    let blocks: Vec<u64> = (10..18).collect();
    let logs: Vec<(u64, u64)> = blocks
        .iter()
        .flat_map(|&b| std::iter::repeat_n((b, SIZE), LOGS_PER_BLOCK))
        .collect();

    let chunk = narrow_weighted_chunk(&blocks, &logs);
    let meta = catalog();
    let query = serde_json::json!({
        "type": "test",
        "fromBlock": 10,
        "toBlock": 17,
        "logs": [{}],
        "fields": {"block": {"number": true}, "log": {"data": true}}
    });
    let out = run(&meta, &chunk, query).unwrap();

    assert!(
        out.last_block() < 17,
        "the whole chunk weighs about {} MB against a 20 MB budget, yet every block \
         came back: a size stored at a width the weight model cannot read weighs nothing",
        logs.len() as u64 * SIZE / MB
    );

    let lines = parse_response(&out.into_json_lines());
    let emitted: usize = lines
        .iter()
        .map(|b| b.get("logs").and_then(|l| l.as_array()).map_or(0, Vec::len))
        .sum();
    assert_eq!(
        emitted,
        block_numbers(&lines).len() * LOGS_PER_BLOCK,
        "a block that is emitted is emitted whole"
    );
}
