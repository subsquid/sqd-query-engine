//! Every emitted row is counted against the response budget, at what the
//! reference charges for it.

use arrow::array::{ArrayRef, BinaryArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use sqd_query_engine::metadata::{DatasetDescription, WeightSource};
use sqd_query_engine::output::{execute_chunk_with, ExecOptions};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use crate::harness::chunk::{blocks_parquet, write_table};
use crate::harness::fixtures::{fixture_tree_is_present, meta, run};
use crate::harness::guard::reference_query_sources;
use crate::harness::json::parse_response;
use crate::harness::synthetic::{catalog, catalog_with_log_weight_key, weighted_chunk, BLOCKS};

/// What a column weighs when the catalog gives it no weight
/// (`P-DEFAULT-COLUMN-WEIGHT`).
const WORD: u64 = 32;

/// The last block of the page `query` gets under `budget`.
fn page_end(meta: &DatasetDescription, chunk: &Path, query: &str, budget: u64) -> Option<u64> {
    let parsed = parse_query(query.as_bytes(), meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    let reader = ParquetChunkReader::open(chunk).unwrap();
    let options = ExecOptions {
        weight_budget: budget,
        ..ExecOptions::default()
    };

    execute_chunk_with(&plan, meta, &reader, options)
        .unwrap()
        .map(|page| page.last_block())
}

/// A header is charged for its block number whether or not a client selects
/// it, as an item is charged for its key. Charging an unselected `number`
/// nothing lets a page of bare headers run past any budget.
///
/// Covers CT-5 · INV-B5, INV-B10
#[test]
fn a_header_weighs_its_key_when_no_header_field_is_selected() {
    let chunk = weighted_chunk(BLOCKS, &[], &[]);
    let query =
        r#"{"type":"test","fromBlock":10,"toBlock":14,"includeAllBlocks":true,"fields":{}}"#;

    assert_eq!(
        page_end(&catalog(), chunk.path(), query, 2 * WORD),
        Some(11)
    );
    assert_eq!(
        page_end(&catalog(), chunk.path(), query, 2 * WORD - 1),
        Some(10)
    );
}

/// The first and last block of the covered range are on the page whether or
/// not they hold items, so each is charged its header like any other block
/// (§5.5). Charging them nothing lets the last one past the budget.
///
/// Covers CT-5 · INV-B3, INV-B5
#[test]
fn a_boundary_block_weighs_its_header() {
    const DATA: u64 = 1000;

    let chunk = weighted_chunk(BLOCKS, &[(12, DATA)], &[]);
    let query = r#"{"type":"test","fromBlock":10,"toBlock":14,"logs":[{}],
                    "fields":{"log":{"data":true}}}"#;

    // 10 and 14 are the boundary blocks; 12 holds a log charged its key and data.
    let log = 2 * WORD + DATA;
    let through_12 = WORD + (WORD + log);
    let through_14 = through_12 + WORD;

    for (budget, end) in [
        (through_14, 14),
        (through_14 - 1, 12),
        (through_12, 12),
        (through_12 - 1, 10),
    ] {
        assert_eq!(
            page_end(&catalog(), chunk.path(), query, budget),
            Some(end),
            "a budget of {budget}"
        );
    }
}

/// A table that declares a `weight_key` is charged for those columns in place
/// of its item key.
///
/// Covers CT-5 · INV-B10
#[test]
fn a_declared_weight_key_replaces_the_item_key() {
    let chunk = tempfile::tempdir().unwrap();
    blocks_parquet(chunk.path(), &[10, 11]);

    let blocks = [10u64, 10, 10, 11, 11, 11];
    write_table(
        chunk.path(),
        "logs",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("data", DataType::Binary, false),
            Field::new("data_size", DataType::UInt64, false),
        ],
        vec![
            Arc::new(UInt64Array::from(blocks.to_vec())) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32, 1, 2, 0, 1, 2])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![b"a".as_slice(); 6])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![0u64; 6])) as ArrayRef,
        ],
    );
    let query = r#"{"type":"test","fromBlock":10,"toBlock":11,"logs":[{}],"fields":{}}"#;

    // A block is its header and three logs, each charged its key alone.
    let by_block_number = catalog_with_log_weight_key("[block_number]");
    let block = WORD + 3 * WORD;
    assert_eq!(
        page_end(&by_block_number, chunk.path(), query, 2 * block),
        Some(11)
    );
    assert_eq!(
        page_end(&by_block_number, chunk.path(), query, 2 * block - 1),
        Some(10)
    );

    // Undeclared, a log is charged its item key: block number and log index.
    let block = WORD + 3 * 2 * WORD;
    assert_eq!(
        page_end(&catalog(), chunk.path(), query, 2 * block),
        Some(11)
    );
    assert_eq!(
        page_end(&catalog(), chunk.path(), query, 2 * block - 1),
        Some(10)
    );
}

/// A field-group request key names its column indirectly: `callCallType` reads
/// `call_type`. Projection resolved that; the weight model did not, so the column
/// was emitted at a weight of zero and the response ran past the cap. Selecting
/// it must cost what selecting the same column under its own name costs.
#[test]
#[ignore = "requires external fixture data"]
fn a_field_group_request_key_weighs_what_its_column_weighs() {
    if !fixture_tree_is_present() {
        return;
    }
    let evm = meta("evm");

    let query = |field: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17882786,
                "fields":{{"trace":{{"{field}":true}}}},
                "traces":[{{}}]}}"#
        )
        .into_bytes()
    };

    let by_column = run("ethereum", &evm, &query("callType")).unwrap();
    let by_request_key = run("ethereum", &evm, &query("callCallType")).unwrap();

    assert_eq!(
        parse_response(&by_request_key).len(),
        parse_response(&by_column).len(),
        "the two names read the same column, so they must be trimmed at the same block"
    );
    assert!(
        by_request_key.len() as u64 <= 20 * 1024 * 1024,
        "a response the budget model did not count ran to {} bytes",
        by_request_key.len()
    );
}

/// A table as the reference's query sources declare it: its primary key, and
/// the weight it sets for a column, fixed or read from a size column.
struct ReferenceTable {
    name: String,
    key: Vec<String>,
    weights: BTreeMap<String, WeightSource>,
}

/// The string literals of `text`, in order.
fn literals(text: &str) -> Vec<String> {
    text.split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

/// The text inside the parentheses `text` opens with, and what follows them.
fn parenthesized(text: &str) -> (&str, &str) {
    let mut depth = 0;
    for (at, c) in text.char_indices() {
        match c {
            '(' => depth += 1,
            ')' if depth == 1 => return (&text[1..at], &text[at + 1..]),
            ')' => depth -= 1,
            _ => {}
        }
    }
    panic!("unbalanced parentheses in the reference's table declarations")
}

/// Every `add_table` of a reference source and the calls chained onto it.
fn reference_tables(source: &str) -> Vec<ReferenceTable> {
    let mut tables = Vec::new();
    let mut rest = source;

    while let Some(at) = rest.find(".add_table(") {
        let (arguments, mut chain) = parenthesized(&rest[at + ".add_table".len()..]);
        let mut names = literals(arguments).into_iter();
        let mut table = ReferenceTable {
            name: names.next().expect("a table is declared by name"),
            key: names.collect(),
            weights: BTreeMap::new(),
        };

        // `.set_weight("col", 32 * 4)`, `.set_weight_column("col", "col_size")`,
        // and calls that say nothing about weight, up to the end of the statement.
        while let Some(call) = chain.trim_start().strip_prefix('.') {
            let open = call.find('(').expect("a chained call has arguments");
            let (arguments, after) = parenthesized(&call[open..]);
            let column = literals(arguments).into_iter().next();

            match (&call[..open], column) {
                ("set_weight", Some(column)) => {
                    let value = arguments.rsplit(',').next().unwrap();
                    let weight = value
                        .split('*')
                        .map(|factor| factor.trim().parse::<u64>().unwrap())
                        .product();
                    table.weights.insert(column, WeightSource::Fixed(weight));
                }
                ("set_weight_column", Some(column)) => {
                    let size = literals(arguments).pop().unwrap();
                    table.weights.insert(column, WeightSource::Column(size));
                }
                _ => {}
            }
            chain = after;
        }

        tables.push(table);
        rest = chain;
    }

    tables
}

/// The reference declares what it charges in its query sources: each table's
/// primary key, weighed in every row, and `set_weight` or `set_weight_column`
/// for a column that does not weigh one word. Read back, they are what the
/// catalogs must charge, so a weight the reference changes is a failing test
/// here rather than a page that ends a few blocks off.
///
/// A trace is the one row the reference reads as a variant, by its `type`
/// column, so the weight key adds the variant column to the primary key; the
/// source must name that column as the tag it reads.
///
/// Covers CT-5 · INV-B10
#[test]
#[ignore = "requires external fixture data"]
fn every_table_is_charged_what_the_reference_charges() {
    let Some(sources) = reference_query_sources() else {
        return;
    };

    let datasets = [
        ("eth.rs", "evm"),
        ("solana.rs", "solana"),
        ("substrate.rs", "substrate"),
        ("bitcoin.rs", "bitcoin"),
        ("tron.rs", "tron"),
        ("hyperliquid_fills.rs", "hyperliquid_fills"),
        ("hyperliquid_replica_cmds.rs", "hyperliquid_replica_cmds"),
    ];

    let mut problems = Vec::new();
    let mut tables = 0;

    for (file, dataset) in datasets {
        let source = std::fs::read_to_string(sources.join(file))
            .unwrap_or_else(|e| panic!("reading the reference's {file}: {e}"));
        let metadata = meta(dataset);

        for table in reference_tables(&source) {
            tables += 1;
            let Some(desc) = metadata.table(&table.name) else {
                problems.push(format!("{dataset}: no table `{}` here", table.name));
                continue;
            };

            let mut key: BTreeSet<&str> = table.key.iter().map(String::as_str).collect();
            if let Some(variant) = desc.output.variant_column.as_deref() {
                if !source.contains(&format!("tag_column: \"{variant}\"")) {
                    problems.push(format!(
                        "{dataset}.{}: the reference does not read `{variant}` as a tag",
                        table.name
                    ));
                }
                key.insert(variant);
            }
            let ours: BTreeSet<&str> = desc.weight_key().into_iter().collect();
            if ours != key {
                problems.push(format!(
                    "{dataset}.{}: weighs {ours:?} in every row, the reference {key:?}",
                    table.name
                ));
            }

            for (name, column) in &desc.columns {
                if column.system {
                    continue;
                }
                if column.weight.as_ref() != table.weights.get(name) {
                    problems.push(format!(
                        "{dataset}.{}.{name}: weighs {:?} here, {:?} in the reference",
                        table.name,
                        column.weight,
                        table.weights.get(name)
                    ));
                }
            }
            for name in table.weights.keys() {
                if desc.column(name).is_none() {
                    problems.push(format!(
                        "{dataset}.{}: the reference weighs `{name}`, which is not a column here",
                        table.name
                    ));
                }
            }
        }
    }

    assert!(tables >= 28, "only {tables} reference tables were read");
    assert!(
        problems.is_empty(),
        "the catalogs charge otherwise than the reference:\n  - {}",
        problems.join("\n  - ")
    );
}
