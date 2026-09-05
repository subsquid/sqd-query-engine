//! The answer does not move when the storage does.
//!
//! An archive writer picks a physical width per chunk, a row-group size, a
//! compression codec, whether to write a dictionary or column statistics, and
//! what to sort by. None of those is part of the question, so none of them may
//! reach the answer. Each test here writes one logical chunk two ways and
//! compares the responses byte for byte.
//!
//! The chain is local rather than a fixture, because the portable gate has to
//! run these: a determinism test that only runs where the fixture tree is
//! checked out is a determinism test that does not run in CI.

use arrow::datatypes::DataType;
use parquet::basic::Compression;

use crate::harness::chunk::{
    chunk_relaid, chunk_with_column_retyped, chunk_with_list_elements_retyped, Layout,
};
use crate::harness::evm_like;
use crate::harness::fixtures::{answers_the_same, fixture_chunk, fixture_tree_is_present, meta};
use crate::harness::generator::{Generator, ItemRequest, Rng, TableCorpus};
use crate::harness::json::{assert_same_response, block_numbers, parse_response};
use crate::harness::synthetic::{catalog, paged_at, part_blocks, partitioned_chunk, MB};
use sqd_query_engine::output::ExecOptions;

// ---------------------------------------------------------------------------
// INV-D7 — physical width
// ---------------------------------------------------------------------------

/// Every integer physical width, for every integer column, that can hold the
/// column's values. A declared integer type bounds the values and not the
/// storage, and the invariant says *any* width and signedness — so the sweep is
/// the whole set rather than a sample of it, and the widths a column cannot hold
/// are left out by arithmetic rather than by choice.
fn widths_per_column() -> Vec<(&'static str, &'static str, Vec<DataType>)> {
    use DataType::{Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64, UInt8};

    // Block numbers are 100..=115 and indices are 0..=3, so every width holds
    // them. `gas_used` runs to five figures and needs sixteen bits.
    let all = vec![UInt8, Int8, UInt16, Int16, UInt32, Int32, UInt64, Int64];
    let wide = vec![UInt16, Int16, UInt32, Int32, UInt64, Int64];

    vec![
        ("blocks", "number", all.clone()),
        ("logs", "block_number", all.clone()),
        ("logs", "log_index", all.clone()),
        ("logs", "transaction_index", all.clone()),
        ("transactions", "block_number", all.clone()),
        ("transactions", "transaction_index", all),
        ("transactions", "gas_used", wide),
    ]
}

/// The same chunk written with one column at one other width, byte-compared,
/// once per (column, width) pair the chain admits.
///
/// The pairs that matter most are the ones a key travels through: a block number
/// reaches the scan's range filter, the assembly's block index and the weight
/// model; a transaction index reaches the relation's key filter and the output
/// sort. A width missing from any one of those returns fewer rows and says
/// nothing about it.
///
/// Covers CT-6 · INV-D7
#[test]
fn physical_width_does_not_reach_the_answer() {
    let meta = evm_like::catalog();
    let source = evm_like::chunk();
    let query = evm_like::query(103, 113);

    for (table, column, widths) in widths_per_column() {
        for width in widths {
            let narrowed = chunk_with_column_retyped(source.path(), table, column, width.clone());
            answers_the_same(
                &meta,
                &query,
                source.path(),
                narrowed.path(),
                &format!("storing {table}.{column} as {width:?}"),
            );
        }
    }
}

/// Every integer column narrowed to the same width at once, which is what an
/// archiver generation actually does. One column at a time can pass while the
/// combination does not: two columns joined on each other are compared by a code
/// path neither reaches alone.
///
/// Covers CT-6 · INV-D7
#[test]
fn narrowing_every_column_at_once_does_not_reach_the_answer() {
    let meta = evm_like::catalog();
    let source = evm_like::chunk();
    let query = evm_like::query(103, 113);

    for width in [
        DataType::UInt8,
        DataType::Int8,
        DataType::UInt16,
        DataType::Int16,
    ] {
        let mut narrowed =
            chunk_with_column_retyped(source.path(), "blocks", "number", width.clone());

        for (table, column, widths) in widths_per_column() {
            if table == "blocks" || !widths.contains(&width) {
                continue;
            }
            narrowed = chunk_with_column_retyped(narrowed.path(), table, column, width.clone());
        }

        answers_the_same(
            &meta,
            &query,
            source.path(),
            narrowed.path(),
            &format!("storing every integer column as {width:?}"),
        );
    }
}

/// A narrow column *and* a storage order that is not the item-key order.
///
/// Either alone can pass while the pair does not: the sort comparator only has
/// to compare anything when the file order differs from the order the response
/// must be in, and a width missing from *it* then leaves the items in file
/// order — which, in a chunk that happens to be written in key order, is the
/// right answer for the wrong reason.
///
/// Covers CT-6 · INV-D7
#[test]
fn a_narrow_column_in_a_shuffled_chunk_does_not_reach_the_answer() {
    let meta = evm_like::catalog();
    let source = evm_like::chunk();
    let query = evm_like::query(103, 113);
    let backwards = chunk_relaid(source.path(), &Layout::shuffled());

    for (table, column, widths) in widths_per_column() {
        for width in widths {
            let narrowed =
                chunk_with_column_retyped(backwards.path(), table, column, width.clone());
            answers_the_same(
                &meta,
                &query,
                source.path(),
                narrowed.path(),
                &format!("storing {table}.{column} as {width:?} in a shuffled chunk"),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// INV-D8 — storage layout
// ---------------------------------------------------------------------------

/// Row-group boundaries, compression, dictionary encoding, the presence of
/// statistics and the physical row order are tuning knobs. Each is turned on its
/// own, so a failure names the knob.
///
/// The reversed case is the sharpest: it is the chunk stored under the opposite
/// of its declared sort key, which is what a scan that trusts the order rather
/// than the key would answer differently.
///
/// Covers CT-6 · INV-D8
#[test]
fn storage_layout_does_not_reach_the_answer() {
    let meta = evm_like::catalog();
    let source = evm_like::chunk();
    let query = evm_like::query(103, 113);

    for (what, layout) in [
        ("one row per row group", Layout::row_groups(1)),
        ("row groups of 7", Layout::row_groups(7)),
        (
            "row groups larger than the table",
            Layout::row_groups(1 << 20),
        ),
        (
            "uncompressed",
            Layout::compressed(Compression::UNCOMPRESSED),
        ),
        ("snappy", Layout::compressed(Compression::SNAPPY)),
        ("one row per data page", Layout::pages(1)),
        ("data pages of 5", Layout::pages(5)),
        ("no dictionary", Layout::without_dictionary()),
        ("no column statistics", Layout::without_statistics()),
        ("the rows stored back to front", Layout::reversed()),
        ("the rows stored in no order", Layout::shuffled()),
    ] {
        let relaid = chunk_relaid(source.path(), &layout);
        answers_the_same(&meta, &query, source.path(), relaid.path(), what);
    }
}

// ---------------------------------------------------------------------------
// INV-O12, INV-O13 — the same chunk, read again
// ---------------------------------------------------------------------------

/// Covers CT-6 · INV-O12
#[test]
fn the_same_chunk_and_query_give_the_same_bytes() {
    let meta = evm_like::catalog();
    let chunk = evm_like::chunk();
    let query = evm_like::query(103, 113);

    answers_the_same(
        &meta,
        &query,
        chunk.path(),
        chunk.path(),
        "running the same query twice",
    );
}

/// Rows are read and encoded in parallel, so the pool size decides how the work
/// is split — and a response assembled in completion order rather than item-key
/// order would differ between a one-thread run and a sixteen-thread one.
///
/// The rest of what INV-O13 names — row-group and page boundaries, compression,
/// physical row order, physical widths, statistics and dictionaries — is
/// INV-D7's and INV-D8's equality runs above, which are the same assertion made
/// of the chunk instead of the pool.
///
/// Covers CT-6 · INV-O13
#[test]
fn thread_count_does_not_reach_the_answer() {
    let meta = evm_like::catalog();
    let chunk = evm_like::chunk();

    for (what, item_request) in evm_like::item_requests() {
        let query = evm_like::query_with(103, 113, &item_request);
        let answer = |threads: usize| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap()
                .install(|| {
                    crate::harness::fixtures::run_against(&meta, chunk.path(), &query).unwrap()
                })
        };

        let single = answer(1);
        assert!(!single.is_empty(), "{what} must return a response");

        for threads in [2, 4, 16] {
            assert_same_response(
                &single,
                &answer(threads),
                &format!("{what} at {threads} threads"),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The same three properties over a real chunk
// ---------------------------------------------------------------------------

/// A chunk written by the archiver has column kinds the local chain does not —
/// lists, structs, nullable strings — and ten thousand rows rather than
/// sixty-four. Optimism rather than Ethereum because both are the same catalog
/// and one is six megabytes: these tests rewrite the whole chunk once per case.
///
/// Covers CT-6 · INV-D8
#[test]
#[ignore = "requires external fixture data"]
fn a_fixture_chunk_answers_the_same_under_any_layout() {
    if !fixture_tree_is_present() {
        return;
    }

    let ethereum = meta("evm");
    let source = fixture_chunk("optimism");

    for (what, layout) in [
        ("row groups of 1000", Layout::row_groups(1000)),
        (
            "uncompressed",
            Layout::compressed(Compression::UNCOMPRESSED),
        ),
        ("data pages of 100", Layout::pages(100)),
        ("no column statistics", Layout::without_statistics()),
        ("the rows stored back to front", Layout::reversed()),
        ("the rows stored in no order", Layout::shuffled()),
    ] {
        let relaid = chunk_relaid(&source, &layout);
        answers_the_same(&ethereum, EVM_QUERY, &source, relaid.path(), what);
    }
}

/// Covers CT-6 · INV-D7
#[test]
#[ignore = "requires external fixture data"]
fn a_fixture_chunk_answers_the_same_at_any_width() {
    if !fixture_tree_is_present() {
        return;
    }

    let ethereum = meta("evm");
    let source = fixture_chunk("optimism");

    for (table, column, width) in [
        ("logs", "block_number", DataType::Int64),
        ("logs", "log_index", DataType::UInt16),
        ("logs", "transaction_index", DataType::UInt16),
        ("transactions", "transaction_index", DataType::UInt16),
    ] {
        let narrowed = chunk_with_column_retyped(&source, table, column, width.clone());
        answers_the_same(
            &ethereum,
            EVM_QUERY,
            &source,
            narrowed.path(),
            &format!("storing {table}.{column} as {width:?}"),
        );
    }
}

/// The width tolerance reaches inside a list.
///
/// A hierarchical address is a path of item indices, stored as a list of
/// integers — sixteen bits on Solana, thirty-two on EVM, and the same declared
/// column in both catalogs. It is read in four places (the scan's address
/// prefix comparison, the hierarchical group key, the join key and the output
/// sort), so an element width one of them has forgotten drops the inner
/// instructions and says nothing.
///
/// Covers CT-6 · INV-D7
#[test]
#[ignore = "requires external fixture data"]
fn a_list_key_answers_the_same_at_any_element_width() {
    if !fixture_tree_is_present() {
        return;
    }

    let solana = meta("solana");
    let source = fixture_chunk("solana");

    for width in [
        DataType::UInt8,
        DataType::Int8,
        DataType::Int16,
        DataType::UInt32,
        DataType::Int32,
        DataType::UInt64,
        DataType::Int64,
    ] {
        let mut retyped = chunk_with_list_elements_retyped(
            &source,
            "instructions",
            "instruction_address",
            width.clone(),
        );
        retyped = chunk_with_list_elements_retyped(
            retyped.path(),
            "logs",
            "instruction_address",
            width.clone(),
        );

        answers_the_same(
            &solana,
            SOLANA_QUERY,
            &source,
            retyped.path(),
            &format!("storing instruction_address as a list of {width:?}"),
        );
    }
}

/// A whirlpool swap with its inner instructions and its transaction — the
/// hierarchical relation is what makes the address column load-bearing.
const SOLANA_QUERY: &str = r#"{"type":"solana","fromBlock":0,
    "fields":{"block":{"number":true},
              "instruction":{"programId":true,"accounts":true,"data":true,
                             "transactionIndex":true,"instructionAddress":true},
              "transaction":{"signatures":true,"feePayer":true}},
    "instructions":[{"programId":["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
                     "innerInstructions":true,"transaction":true}]}"#;

const EVM_QUERY: &str = r#"{"type":"evm","fromBlock":125800020,"toBlock":125800080,
    "fields":{"block":{"number":true,"timestamp":true},
              "log":{"logIndex":true,"transactionIndex":true,"address":true,
                     "topics":true,"data":true},
              "transaction":{"from":true,"to":true,"value":true}},
    "logs":[{"topic0":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
             "transaction":true}]}"#;

/// The wave is as wide as the rayon pool, so a walk that let the wave boundary
/// decide where to stop would page a chunk differently on a four-core worker and
/// a sixteen-core one — the same query, the same chunk, two answers.
///
/// This one is a single hand-written shape; `every_generated_query_pages_the_same`
/// below asserts the same property over the query surface.
///
/// Covers CT-6 · INV-O13
#[test]
fn the_pool_size_does_not_move_a_page_boundary() {
    let meta = catalog();
    let chunk = partitioned_chunk(MB);
    let to = *part_blocks().last().unwrap();

    let (single_logs, single_pages) = paged_at(&meta, &chunk, to, 1);

    for threads in [2, 17] {
        let (logs, pages) = paged_at(&meta, &chunk, to, threads);
        assert_eq!(
            pages, single_pages,
            "{threads} threads paged the chunk at {pages:?}, one thread at {single_pages:?}"
        );
        assert_eq!(logs, single_logs, "{threads} threads returned other logs");
    }
}

// ---------------------------------------------------------------------------
// INV-O13 — the pool size, over generated queries
// ---------------------------------------------------------------------------

/// Recorded, so a failure replays. Changing it is changing the test.
const DETERMINISM_SEED: u64 = 0x5EED_0006;

const DETERMINISM_CASES: usize = 32;

/// Budgets straddling what this chunk's queries actually weigh — a response here
/// runs from about 200 bytes to about 14 KiB — so the trim lands at every depth
/// from the first block to the last. At the production 20 MiB nothing on a chunk
/// this size is trimmed, and every comparison below would be between two full
/// reads.
const DETERMINISM_BUDGETS: [u64; 6] = [1, 256, 1024, 2048, 4096, 8192];

fn paged_in_pool(generator: &Generator, query: &str, budget: u64, threads: usize) -> Vec<u8> {
    let options = ExecOptions {
        weight_budget: budget,
        ..ExecOptions::default()
    };

    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap()
        .install(|| generator.run_with(query, options))
}

/// The machine the query ran on is not part of the answer.
///
/// A worker fleet is not uniform, and the wave is as wide as the pool, so a
/// response that depends on the pool is a response that depends on which worker
/// served it: two clients paging the same range get different `lastBlock`s and
/// neither can tell. One hand-written shape cannot establish that — it
/// establishes it for one shape — so the property is asserted over the query
/// surface the generator covers, at budgets where the walk actually stops.
///
/// Covers CT-6 · INV-O13
#[test]
fn every_generated_query_pages_the_same() {
    let chunk = evm_like::partitioned_chunk();
    let generator = Generator::new(evm_like::catalog(), chunk.path());
    let mut rng = Rng::new(DETERMINISM_SEED);

    let mut trimmed = 0usize;

    for _ in 0..DETERMINISM_CASES {
        let tables = generator.tables();
        let mut chosen: Vec<&TableCorpus> = rng.subset(tables);
        if chosen.is_empty() {
            chosen.push(rng.pick(tables));
        }

        let range = generator.range(&mut rng);
        let requests: Vec<(&TableCorpus, Vec<ItemRequest>)> = chosen
            .into_iter()
            .map(|table| (table, vec![generator.item_request(table, &mut rng)]))
            .collect();
        let query = generator.query(range, &requests);

        for budget in DETERMINISM_BUDGETS {
            let single = paged_in_pool(&generator, &query, budget, 1);

            for threads in [2, 5, 17] {
                assert_same_response(
                    &single,
                    &paged_in_pool(&generator, &query, budget, threads),
                    &format!(
                        "{threads} threads answered differently from one at a \
                         budget of {budget}: {query}"
                    ),
                );
            }

            let blocks = block_numbers(&parse_response(&single));
            if blocks.last().is_some_and(|&last| last < range.1) {
                trimmed += 1;
            }
        }
    }

    assert!(
        trimmed > 0,
        "the law ran {DETERMINISM_CASES} queries at {} budgets and none was trimmed, \
         so no comparison reached the walk at all",
        DETERMINISM_BUDGETS.len()
    );
}
