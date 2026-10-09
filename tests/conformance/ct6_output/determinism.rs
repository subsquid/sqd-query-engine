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
use crate::harness::columnar::{run_columnar, MemoryChunk};
use crate::harness::evm_like;
use crate::harness::fixtures::{
    answers_the_same, fixture_chunk, fixture_tree_has, fixture_tree_is_present, meta, run,
    run_against, FIXTURE_DATASETS,
};
use crate::harness::generator::{Generator, ItemRequest, Rng, TableCorpus};
use crate::harness::guard::fixture_dir;
use crate::harness::json::{assert_same_response, block_numbers, parse_response};
use crate::harness::sol_like;
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

/// Parquet is one storage among several: the hot store keeps chunks in a
/// key-value database and hands the engine columns by row range. Its reader
/// filters in memory what the parquet reader filters in a row filter, and
/// prunes by statistics windows of its own, so every case runs with no
/// statistics, with one window larger than the table, with windows of a few
/// rows and with a window per row, where every bound prunes exactly.
///
/// Covers CT-6 · INV-D8
#[test]
fn a_columnar_reader_answers_what_the_parquet_reader_does() {
    for (catalog, chunk, cases) in columnar_cases() {
        let readers = [None, Some(1 << 20), Some(5), Some(1)]
            .map(|w| (w, MemoryChunk::load(chunk.path(), w)));
        for (what, query) in cases {
            let expected = run_against(&catalog, chunk.path(), &query).unwrap();
            assert!(!expected.is_empty(), "{what} must return a response");

            for (window, reader) in &readers {
                let what = format!("{what}, statistics windows {window:?}");
                let actual = run_columnar(&catalog, reader, query.as_bytes())
                    .unwrap_or_else(|e| panic!("{what} made the query fail: {e:#}"));
                assert_same_response(&expected, &actual, &what);
            }
        }
    }
}

/// A store's window offsets are its word on which rows each window holds. A
/// word that does not cover the table once and in order prunes nothing:
/// trusted, it drops the rows before its first offset, or reads rows twice.
/// Empty windows still cover it.
///
/// Covers CT-6 · INV-D8
#[test]
fn window_offsets_that_do_not_cover_the_table_prune_nothing() {
    let offsets: [(&str, Offsets); 8] = [
        ("windows from row 1", |rows| vec![1, rows]),
        ("an end and no window", |rows| vec![rows]),
        ("a window that steps back", |rows| vec![0, 2, 1, rows]),
        ("a window past the table", |rows| vec![0, rows + 2, rows]),
        ("a window past the table and back", |rows| {
            vec![0, rows + 2, rows - 1, rows]
        }),
        ("windows that end short", |rows| vec![0, rows - 1]),
        ("windows that end past the table", |rows| vec![0, rows + 1]),
        ("empty windows", |rows| vec![0, 0, 1, 1, rows]),
    ];

    for (catalog, chunk, cases) in columnar_cases() {
        let readers =
            offsets.map(|(how, at)| (how, MemoryChunk::load_with_offsets(chunk.path(), at)));
        for (what, query) in cases {
            let expected = run_against(&catalog, chunk.path(), &query).unwrap();

            for (how, reader) in &readers {
                let what = format!("{what}, {how}");
                let actual = run_columnar(&catalog, reader, query.as_bytes())
                    .unwrap_or_else(|e| panic!("{what} made the query fail: {e:#}"));
                assert_same_response(&expected, &actual, &what);
            }
        }
    }
}

/// The window offsets a store reports for a table of so many rows.
type Offsets = fn(usize) -> Vec<usize>;

/// A chain's catalog, a chunk of it, and the queries the chunk is read with.
type ColumnarCase = (
    sqd_query_engine::metadata::DatasetDescription,
    tempfile::TempDir,
    Vec<(String, String)>,
);

/// The evm-like and solana-like chunks, each with the queries it is read with.
fn columnar_cases() -> Vec<ColumnarCase> {
    let evm_cases = evm_like::item_requests()
        .into_iter()
        .map(|(what, items)| (what.to_string(), evm_like::query_with(103, 113, &items)))
        .collect();

    let solana_cases = [
        ("no instruction filter", "{}".to_string()),
        ("a program filter", r#"{"programId":["100"]}"#.to_string()),
        (
            "a discriminator of mixed lengths",
            r#"{"discriminator":["0x07","0x0700","0x2a0001"]}"#.to_string(),
        ),
        (
            "an account mention",
            format!(r#"{{"mentionsAccount":["{}"]}}"#, sol_like::account(4)),
        ),
        (
            "two items",
            r#"{"programId":["100"]},{"d1":["0x07"],"isCommitted":true}"#.to_string(),
        ),
    ]
    .into_iter()
    .map(|(what, items)| (what.to_string(), sol_like::query_with(&items)))
    .collect();

    vec![
        (evm_like::catalog(), evm_like::chunk(), evm_cases),
        (sol_like::catalog(), sol_like::chunk(), solana_cases),
    ]
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

/// Every fixture query of every dataset, through the parquet reader and the
/// columnar one. A query the parquet reader refuses must be refused with the
/// same message.
///
/// Covers CT-6 · INV-D8
#[test]
#[ignore = "requires external fixture data"]
fn every_fixture_query_answers_the_same_through_the_columnar_reader() {
    if !fixture_tree_is_present() {
        return;
    }

    let mut compared = 0;
    for (dataset, catalog) in FIXTURE_DATASETS {
        if !fixture_tree_has(dataset) {
            continue;
        }
        let catalog = meta(catalog);
        let chunk = fixture_chunk(dataset);
        let readers = [None, Some(4096), Some(100)].map(|w| (w, MemoryChunk::load(&chunk, w)));

        let mut queries: Vec<_> = std::fs::read_dir(fixture_dir().join(dataset).join("queries"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|dir| dir.join("query.json").is_file())
            .collect();
        queries.sort();

        for dir in queries {
            let query = std::fs::read(dir.join("query.json")).unwrap();
            let expected = run(dataset, &catalog, &query);
            for (window, reader) in &readers {
                let what = format!("{}, statistics windows {window:?}", dir.display());
                match (&expected, run_columnar(&catalog, reader, &query)) {
                    (Ok(expected), Ok(actual)) => assert_same_response(expected, &actual, &what),
                    (Err(expected), Err(actual)) => assert_eq!(
                        expected.root_cause().to_string(),
                        actual.root_cause().to_string(),
                        "{what}"
                    ),
                    (expected, actual) => panic!(
                        "{what}: parquet answered {:?}, columnar {:?}",
                        expected
                            .as_ref()
                            .map(Vec::len)
                            .map_err(|e| format!("{e:#}")),
                        actual.as_ref().map(Vec::len).map_err(|e| format!("{e:#}"))
                    ),
                }
                compared += 1;
            }
        }
    }

    assert!(compared > 300, "only {compared} fixture runs were compared");
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

/// Parallelism must not move the response boundary. A reader that lets scheduling
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
/// Query results must be independent of the pool size. A
/// response that depends on the pool is a response that depends on which worker
/// served it: two clients paging the same range get different `lastBlock`s and
/// neither can tell. One hand-written shape cannot establish that — it
/// establishes it for one shape — so the property is asserted over the query
/// surface the generator covers, at budgets where reads stop early.
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
         so no comparison reached an early stop",
        DETERMINISM_BUDGETS.len()
    );
}

/// Tables are scanned side by side, so two that fail fail at once. The error a
/// client sees must still be one error, not whichever thread lost the race:
/// the kind is what a client switches on (INV-E6).
///
/// Covers CT-6 · INV-O13
#[test]
fn the_error_of_two_failing_tables_does_not_depend_on_the_pool() {
    use crate::harness::chunk::{chunk_with_column_retyped, chunk_without_column_at};
    use arrow::datatypes::DataType;

    let meta = evm_like::catalog();
    let source = evm_like::chunk();
    let no_address = chunk_without_column_at(source.path(), "logs", "address");
    let broken = chunk_with_column_retyped(
        no_address.path(),
        "transactions",
        "transaction_index",
        DataType::Utf8,
    );
    let query = format!(
        r#"{{"type":"test","fromBlock":100,"toBlock":115,
            "fields":{{"log":{{"logIndex":true}},"transaction":{{"gasUsed":true}}}},
            "logs":[{{"address":["{}"]}}],
            "transactions":[{{"transactionIndex":[0]}}]}}"#,
        evm_like::address(evm_like::PRESENT_ADDRESS)
    );
    let error = |threads: usize| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap()
            .install(|| {
                crate::harness::fixtures::run_against(&meta, broken.path(), &query)
                    .expect_err("both tables are broken")
                    .to_string()
            })
    };

    let single = error(1);
    for threads in [2, 4, 16] {
        for _ in 0..20 {
            assert_eq!(error(threads), single, "at {threads} threads");
        }
    }
}
