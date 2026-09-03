//! Response-budget and block-selection tests on synthetic chunks, covering the
//! spots the fixture suite can't reach: budget trimming (via oversized weight
//! columns), boundary-block handling under the phase-1 scan cutoff, empty
//! results, and JSON/Arrow parity of the reported block range.

use sqd_query_engine::output::{execute_chunk, execute_chunk_arrow};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;

use crate::harness::json::{block_numbers, parse_response};
use crate::harness::synthetic::{catalog, logs_query, run, uniform, weighted_chunk, BLOCKS, MB};

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
