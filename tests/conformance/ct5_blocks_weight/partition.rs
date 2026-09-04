//! INV-B8 — a range split in two answers what the whole range answers.
//!
//! This is the property the distributed architecture rests on: a chunk can live
//! on its own machine only because merging two adjacent ranges is concatenation
//! and nothing more. It follows from relation locality (INV-D5) and from nothing
//! else, so it is also the cheapest way to notice that a join has started
//! reaching across a block boundary.
//!
//! Headers may differ — each sub-query contributes its own boundary blocks
//! (INV-B3) — so the comparison is over items, not over the whole response.

use serde_json::Value;
use sqd_query_engine::metadata::DatasetDescription;
use std::path::Path;

use crate::harness::evm_like;
use crate::harness::fixtures::{fixture_chunk, fixture_tree_is_present, meta, run_against};
use crate::harness::json::{block_numbers, items_in, items_of, parse_response};

/// The tables a response may carry items under, in the order the assertions
/// read them.
const ITEM_TABLES: [&str; 2] = ["logs", "transactions"];

/// Every item of a response, per table, in the order it was written.
///
/// A response that stopped short of `to` was trimmed by the weight budget, and
/// a trimmed half is not the half this invariant is about — so it is a failure
/// here rather than a difference to explain away.
fn items(
    catalog: &DatasetDescription,
    chunk: &Path,
    query: &str,
    to: u64,
) -> Vec<(&'static str, Vec<(u64, Value)>)> {
    let body = run_against(catalog, chunk, query).unwrap();
    let blocks = parse_response(&body);

    if let Some(&last) = block_numbers(&blocks).last() {
        assert!(
            last >= to,
            "the response stops at block {last} of {to}, so the weight budget trimmed it \
             and the two sides are no longer comparable"
        );
    }

    ITEM_TABLES
        .iter()
        .map(|&table| (table, items_of(&body, table)))
        .collect()
}

/// Split at every block boundary in the range, which for a sixteen-block chunk
/// is every split there is, for every filter shape the chain admits.
///
/// One query would not do: composability is a claim about how a *filter* and a
/// *relation* interact with a range boundary, and a query carrying neither has
/// nothing to say about either.
///
/// Covers CT-5 · INV-B8
#[test]
fn splitting_the_range_returns_the_same_items() {
    let catalog = evm_like::catalog();
    let chunk = evm_like::chunk();
    let (from, to) = (*evm_like::BLOCKS.start() + 2, *evm_like::BLOCKS.end() - 1);

    let mut with_items = 0;

    for (what, item_request) in evm_like::item_requests() {
        let query = |a: u64, b: u64| evm_like::query_with(a, b, &item_request);
        let whole = items(&catalog, chunk.path(), &query(from, to), to);

        if whole.iter().any(|(_, rows)| !rows.is_empty()) {
            with_items += 1;
        }

        for split in from..to {
            let left = items(&catalog, chunk.path(), &query(from, split), split);
            let right = items(&catalog, chunk.path(), &query(split + 1, to), to);

            for (i, (table, expected)) in whole.iter().enumerate() {
                let joined: Vec<_> = left[i].1.iter().chain(&right[i].1).cloned().collect();
                assert_eq!(
                    &joined, expected,
                    "with {what}, splitting after block {split} changed the {table} items"
                );
            }
        }
    }

    assert!(
        with_items >= 4,
        "only {with_items} of the query shapes returned items, so the rest matched \
         trivially however the split behaved"
    );
}

/// The only difference a split may make is to the headers, and only by the
/// boundary blocks each side contributes.
///
/// Covers CT-5 · INV-B3
#[test]
fn a_split_adds_only_boundary_headers() {
    let catalog = evm_like::catalog();
    let chunk = evm_like::chunk();
    let (from, to) = (*evm_like::BLOCKS.start() + 2, *evm_like::BLOCKS.end() - 1);

    let headers = |query: &str| -> Vec<u64> {
        let body = run_against(&catalog, chunk.path(), query).unwrap();
        block_numbers(&parse_response(&body))
    };

    let whole = headers(&evm_like::query(from, to));

    for split in from..to {
        let mut halves = headers(&evm_like::query(from, split));
        halves.extend(headers(&evm_like::query(split + 1, to)));
        halves.dedup();

        let extra: Vec<u64> = halves
            .iter()
            .copied()
            .filter(|b| !whole.contains(b))
            .collect();
        assert!(
            extra.len() <= 2,
            "splitting after block {split} added {} headers, and only the two boundary \
             blocks of the new range may appear: {extra:?}",
            extra.len()
        );
    }
}

/// The same law over a chunk the archiver wrote, where a block carries hundreds
/// of items rather than four and the relation joins real transactions.
///
/// Covers CT-5 · INV-B8
#[test]
#[ignore = "requires external fixture data"]
fn splitting_a_fixture_range_returns_the_same_items() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let chunk = fixture_chunk("optimism");
    let (from, to) = (125_800_020, 125_800_060);

    let whole = fixture_items(&evm, &chunk, from, to);
    assert!(!whole.is_empty(), "the query must return items");

    for split in from..to {
        let mut joined = fixture_items(&evm, &chunk, from, split);
        joined.extend(fixture_items(&evm, &chunk, split + 1, to));

        assert_eq!(
            joined, whole,
            "splitting after block {split} changed the items"
        );
    }
}

/// A hierarchical relation across a split.
///
/// INV-B8 follows from relation locality (INV-D5) and nothing else, and a
/// hierarchical relation is where locality is least obvious: `innerInstructions`
/// matches on an address *prefix* rather than on an equal key, so a walk that
/// reached past the block would be invisible until a range was split under it.
///
/// Covers CT-5 · INV-B8
#[test]
#[ignore = "requires external fixture data"]
fn splitting_a_hierarchical_range_returns_the_same_items() {
    if !fixture_tree_is_present() {
        return;
    }

    let solana = meta("solana");
    let chunk = fixture_chunk("solana");
    let (from, to) = (217_710_050, 217_710_090);

    let instructions = |from: u64, to: u64| -> Vec<(u64, Value)> {
        let query = format!(
            r#"{{"type":"solana","fromBlock":{from},"toBlock":{to},
                 "fields":{{"block":{{"number":true}},
                           "instruction":{{"programId":true,"transactionIndex":true,
                                          "instructionAddress":true}}}},
                 "instructions":[{{"programId":["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
                                  "innerInstructions":true}}]}}"#
        );

        items_of(
            &run_against(&solana, &chunk, &query).unwrap(),
            "instructions",
        )
    };

    let whole = instructions(from, to);
    assert!(!whole.is_empty(), "the query must return instructions");

    for split in from..to {
        let mut joined = instructions(from, split);
        joined.extend(instructions(split + 1, to));

        assert_eq!(
            joined, whole,
            "splitting after block {split} changed the instructions"
        );
    }
}

/// Every log and every joined transaction of a transfer query, as
/// `(block, log index or transaction index)`.
fn fixture_items(
    catalog: &DatasetDescription,
    chunk: &Path,
    from: u64,
    to: u64,
) -> Vec<(u64, u64, u64)> {
    let query = format!(
        r#"{{"type":"evm","fromBlock":{from},"toBlock":{to},
             "fields":{{"block":{{"number":true}},
                       "log":{{"logIndex":true,"address":true}},
                       "transaction":{{"transactionIndex":true,"hash":true}}}},
             "logs":[{{"topic0":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                      "transaction":true}}]}}"#
    );

    let body = run_against(catalog, chunk, &query).unwrap();

    parse_response(&body)
        .iter()
        .flat_map(|block| {
            let number = block["header"]["number"].as_u64().unwrap();
            let logs = items_in(block, "logs")
                .into_iter()
                .map(move |log| (number, 0, log["logIndex"].as_u64().unwrap()));
            let txs = items_in(block, "transactions")
                .into_iter()
                .map(move |tx| (number, 1, tx["transactionIndex"].as_u64().unwrap()));

            logs.chain(txs).collect::<Vec<_>>()
        })
        .collect()
}
