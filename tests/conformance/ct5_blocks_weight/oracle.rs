//! Range reads and a full read must select the same response.

use crate::harness::evm_like;
use crate::harness::generator::{Generator, ItemRequest, Rng, TableCorpus};
use crate::harness::json::{assert_same_response, block_numbers, parse_response};
use sqd_query_engine::output::ExecOptions;
use tempfile::TempDir;

const SEED: u64 = 0x5EED_0005;

const CASES: usize = 48;

const BUDGETS: [u64; 6] = [1, 256, 1024, 2048, 4096, 8192];

fn run(
    generator: &Generator,
    query: &str,
    budget: u64,
    ranges: bool,
    threads: usize,
) -> (Vec<u8>, Option<u64>) {
    let options = ExecOptions {
        weight_budget: budget,
        range_reads: ranges,
        ..ExecOptions::default()
    };

    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap()
        .install(|| generator.run_with_cut(query, options))
}

fn corpus() -> (TempDir, Generator) {
    let chunk = evm_like::partitioned_chunk();
    let generator = Generator::new(evm_like::catalog(), chunk.path());

    (chunk, generator)
}

fn case(generator: &Generator, rng: &mut Rng) -> (String, (u64, u64)) {
    let tables = generator.tables();
    let mut chosen: Vec<&TableCorpus> = rng.subset(tables);

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

/// Covers CT-5 · INV-B7
#[test]
fn range_reads_return_what_a_full_read_returns() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);

    let mut trimmed = 0usize;
    let mut cut = 0usize;

    for _ in 0..CASES {
        let (query, range) = case(&generator, &mut rng);

        for budget in BUDGETS {
            let (whole, _) = run(&generator, &query, budget, false, 1);

            for threads in [1, 2, 5] {
                let (ranged, stopped_at) = run(&generator, &query, budget, true, threads);

                cut += usize::from(stopped_at.is_some());

                assert_same_response(
                    &whole,
                    &ranged,
                    &format!(
                        "range reads answered differently at a budget of {budget} \
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

    assert!(
        cut > 0,
        "the law ran {CASES} queries at {} budgets and three pool sizes and the \
         range reads never stopped early, so every comparison was between two whole reads",
        BUDGETS.len()
    );
}

/// Covers CT-5 · INV-B7
#[test]
fn a_table_a_relation_also_fills_pages_the_same() {
    let chunk = evm_like::partitioned_chunk();
    let generator = Generator::new(evm_like::catalog(), chunk.path());

    let (first, last) = generator.blocks();
    let query = format!(
        r#"{{"type":"test","fromBlock":{first},"toBlock":{last},
            "fields":{{"log":{{"logIndex":true,"transactionIndex":true}},
                       "transaction":{{"transactionIndex":true,"gasUsed":true}}}},
            "transactions":[{{}}],
            "logs":[{{"transaction":true}}]}}"#
    );

    for budget in BUDGETS {
        let (single, _) = run(&generator, &query, budget, true, 1);

        for threads in [2, 5, 17] {
            assert_same_response(
                &single,
                &run(&generator, &query, budget, true, threads).0,
                &format!("{threads} threads ended the response elsewhere at a budget of {budget}"),
            );
        }

        assert_same_response(
            &single,
            &run(&generator, &query, budget, false, 1).0,
            &format!("range reads changed the answer at a budget of {budget}"),
        );
    }
}

/// Covers CT-5 · INV-B7
#[test]
fn each_requested_table_can_stop_on_complete_blocks() {
    let (_chunk, generator) = corpus();
    let catalog = evm_like::catalog();
    let range = generator.blocks();

    let mut asked = 0;

    for (name, table) in &catalog.tables {
        if table.is_block_table() {
            continue;
        }

        asked += 1;
        let query = generator.query(
            range,
            &[(generator.table(name), vec![ItemRequest::default()])],
        );

        let (_, cut) = run(&generator, &query, 512, true, 1);

        assert!(
            cut.is_some(),
            "`{name}` is requested and a query reached it, and range reads drew no \
             cut: {query}"
        );
    }

    assert!(
        asked > 1,
        "the chain has {asked} requested tables, so this law is about one table \
         or none of them"
    );
}
