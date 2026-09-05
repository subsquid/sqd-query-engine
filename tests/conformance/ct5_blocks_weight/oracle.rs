//! The budget walk against the scan it optimises, over generated queries.
//!
//! The walk exists to stop a block-sorted scan before it decodes row groups the
//! response cannot carry. That is a claim about *cost*, and it comes with a
//! claim about the answer: whatever the walk does, the response has to be the
//! one a full read would have produced. Reading everything is the reference
//! implementation of the walk, and it is in this binary, so the law can be
//! stated directly rather than modelled.
//!
//! Stating it that way is the point. A test that models the cut has to know how
//! the cut is drawn, and then it agrees with the cut about the things the cut is
//! wrong about — the walk's weight estimate is one model of a block's weight and
//! `apply_weight_limit` is another, and a test written from either finds nothing.
//! This one knows neither.

use crate::harness::evm_like;
use crate::harness::generator::{Generator, ItemRequest, Rng, TableCorpus};
use crate::harness::json::{assert_same_response, block_numbers, parse_response};
use sqd_query_engine::output::ExecOptions;
use tempfile::TempDir;

/// Recorded, so a failure replays. Changing it is changing the test.
const SEED: u64 = 0x5EED_0005;

const CASES: usize = 48;

/// Budgets straddling what this chunk's queries actually weigh — a response here
/// runs from about 200 bytes to about 14 KiB — so the trim lands at every depth
/// from the first block to the last.
///
/// Picking them by eye is how a law goes quiet: at the production 20 MiB nothing
/// on a chunk this size is ever trimmed, and every comparison below would be
/// between two full reads.
const BUDGETS: [u64; 6] = [1, 256, 1024, 2048, 4096, 8192];

/// One run, at a given budget, with the walk on or off, in a pool of `threads`.
fn run(generator: &Generator, query: &str, budget: u64, walk: bool, threads: usize) -> Vec<u8> {
    let options = ExecOptions {
        weight_budget: budget,
        budget_walk: walk,
        ..ExecOptions::default()
    };

    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap()
        .install(|| generator.run_with(query, options))
}

fn corpus() -> (TempDir, Generator) {
    let chunk = evm_like::partitioned_chunk();
    let generator = Generator::new(evm_like::catalog(), chunk.path());

    (chunk, generator)
}

/// A query over some of the chunk's tables, each with its own filters and
/// relations, and the range to ask it over.
fn case(generator: &Generator, rng: &mut Rng) -> (String, (u64, u64)) {
    let tables = generator.tables();
    let mut chosen: Vec<&TableCorpus> = rng.subset(tables);

    // An empty subset asks for no items at all, which the response answers with
    // headers and tells the law nothing.
    if chosen.is_empty() {
        chosen.push(rng.pick(tables));
    }

    let range = generator.range(rng);
    let requests: Vec<(&TableCorpus, Vec<ItemRequest>)> = chosen
        .into_iter()
        .map(|table| (table, vec![generator.item_request(table, rng)]))
        .collect();

    (generator.query(range, &requests), range)
}

/// The response does not depend on whether the walk ran.
///
/// Every difference between the two paths is a bug in the walk, because the walk
/// is the only thing that differs: the same plan, the same chunk, the same
/// budget, the same trim. A cut drawn one block short shows up as a missing
/// block; one drawn on rows the next row group still owns shows up as a block
/// short of its rows; an estimate that runs *over* the exact model — which is
/// what makes the cut land below where the trim would have — shows up as a
/// response that ends earlier than the full read's.
///
/// What the law cannot see is worth stating, because it is not a gap. A cut
/// drawn *above* where the trim stops changes nothing — the trim ends the
/// response first — so a walk that claims one block too many, or carries rows it
/// should have dropped, is invisible here and invisible in production for the
/// same reason. The class that is visible is the one that matters: a cut landing
/// below the trim, which is what an estimate running over the exact model does,
/// and what ends a response short of the budget the client was promised.
///
/// Covers CT-5 · INV-B7 · INV-O13
#[test]
fn the_budget_walk_returns_what_a_full_read_returns() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);

    let mut trimmed = 0usize;

    for _ in 0..CASES {
        let (query, range) = case(&generator, &mut rng);

        for budget in BUDGETS {
            let whole = run(&generator, &query, budget, false, 1);

            // The wave is as wide as the rayon pool, so the pool size decides how
            // far the walk over-reads before it settles. Narrow pools make it
            // stop soonest, which is where a cut drawn wrong is visible; a pool
            // wider than the row groups leaves one wave, no unread group, and no
            // cut at all.
            for threads in [1, 2, 5] {
                let walked = run(&generator, &query, budget, true, threads);

                assert_same_response(
                    &whole,
                    &walked,
                    &format!(
                        "the walk answered differently at a budget of {budget} \
                         on {threads} threads: {query}"
                    ),
                );
            }

            let blocks = block_numbers(&parse_response(&whole));
            if blocks.last().is_some_and(|&last| last < range.1) {
                trimmed += 1;
            }
        }
    }

    assert!(
        trimmed > 0,
        "the law compared {CASES} queries at {} budgets and the budget never bit, \
         so every comparison was between two untrimmed responses",
        BUDGETS.len()
    );
}
