//! CT-1 — catalog validation.
//!
//! Static, no chunk, milliseconds. Most of the class lives with the validator in
//! `src/metadata/loader.rs`, because those cases construct catalogs that violate
//! one rule each and assert the loader rejects them — which needs the loader's
//! private surface, not the engine's public one. They carry the same `CT-1` tag
//! as everything here, so the coverage report finds them wherever they live.
//!
//! What is here is the other half of INV-X1: not that a bad catalog is rejected,
//! but that a good one is *enough* — a chain nothing in the engine has heard of,
//! served from a synthetic chunk with no code change.
//!
//! What is missing is HC-2: a builder that derives one deliberately invalid
//! catalog per check, rather than the hand-written negative cases that exist
//! today. A validator nobody has seen reject anything is a validator that
//! returns `Ok`, and a validator seen to reject nine things out of forty is
//! thirty-one unproven checks.

use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use sqd_query_engine::metadata::parse_dataset_description;
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;

use crate::harness::chunk::{blocks_parquet_named, read_columns, write_table};
use crate::harness::fixtures::{fixture_chunk, fixture_tree_is_present, meta};
use crate::harness::synthetic::run_json;

// ---------------------------------------------------------------------------
// INV-X1 — a column name the engine knows is a column name the catalog lost
// ---------------------------------------------------------------------------

/// A chain that calls its block number `height`, with a relation onto a table
/// that also carries an unrelated column named `block_number` — the archive's
/// note of which block a receipt *refers to*, not the block it is in.
const HEIGHT_CHAIN: &str = r#"
name: test
tables:
  blocks:
    field_name: block
    block_number_column: height
    sort_key: [height]
    filters: []
    fields: [height]
    columns:
      height: { type: uint64 }
  events:
    query_name: events
    field_name: event
    block_number_column: height
    item_order_keys: [seq]
    sort_key: [height, seq]
    filters: [ kind ]
    fields: [seq, kind]
    relations:
      receipt:
        table: receipts
        key: [height, seq]
    columns:
      height: { type: uint64 }
      seq: { type: uint32 }
      kind: { type: string }
  receipts:
    query_name: receipts
    field_name: receipt
    block_number_column: height
    item_order_keys: [seq]
    sort_key: [height, seq]
    filters: []
    # `block_number` here is the block a receipt *cites*, an ordinary data
    # column of this chain and a selectable field like any other.
    fields: [seq, block_number, status]
    columns:
      height: { type: uint64 }
      seq: { type: uint32 }
      block_number: { type: uint64 }
      status: { type: string }
"#;

/// One event and one receipt per block. Every receipt cites block 1, which is
/// not in the chunk at all.
fn height_chain_chunk(blocks: &[u64]) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    blocks_parquet_named(dir.path(), "height", blocks);

    let height = || Arc::new(UInt64Array::from(blocks.to_vec())) as ArrayRef;
    let seq = || Arc::new(UInt32Array::from(vec![0u32; blocks.len()])) as ArrayRef;

    write_table(
        dir.path(),
        "events",
        vec![
            Field::new("height", DataType::UInt64, false),
            Field::new("seq", DataType::UInt32, false),
            Field::new("kind", DataType::Utf8, false),
        ],
        vec![
            height(),
            seq(),
            Arc::new(StringArray::from(vec!["call"; blocks.len()])) as ArrayRef,
        ],
    );

    write_table(
        dir.path(),
        "receipts",
        vec![
            Field::new("height", DataType::UInt64, false),
            Field::new("seq", DataType::UInt32, false),
            Field::new("block_number", DataType::UInt64, false),
            Field::new("status", DataType::Utf8, false),
        ],
        vec![
            height(),
            seq(),
            Arc::new(UInt64Array::from(vec![1u64; blocks.len()])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ok"; blocks.len()])) as ArrayRef,
        ],
    );

    dir
}

/// Block selection read the block number of a relation's rows under the literal
/// name `block_number`. Every table in every shipped catalog happens to use it,
/// so the engine worked — until a chain that does not, or a table that uses the
/// name for something else. Both are catalog edits, and INV-X1 says a catalog
/// edit is all it takes to serve a new chain.
///
/// Covers CT-1 · INV-X1
#[test]
fn a_relation_target_names_its_own_block_column() {
    let meta = parse_dataset_description(HEIGHT_CHAIN).unwrap();
    let chunk = height_chain_chunk(&[10, 11, 12]);

    let blocks = run_json(
        &meta,
        &chunk,
        r#"{"type":"test","fromBlock":10,"toBlock":12,
            "fields":{"block":{"height":true},"event":{"kind":true},
                      "receipt":{"blockNumber":true,"status":true}},
            "events":[{"kind":["call"],"receipt":true}]}"#,
    )
    .unwrap()
    .unwrap();

    let heights: Vec<u64> = blocks
        .iter()
        .map(|b| b["header"]["height"].as_u64().unwrap())
        .collect();
    assert_eq!(
        heights,
        vec![10, 11, 12],
        "the response carries the blocks the chunk has, not the ones a receipt cites"
    );

    let receipts: usize = blocks
        .iter()
        .map(|b| b["receipts"].as_array().map(|a| a.len()).unwrap_or(0))
        .sum();
    assert_eq!(receipts, 3, "one receipt joins each event");
}

// ---------------------------------------------------------------------------
// INV-D4 — the item key identifies a row
// ---------------------------------------------------------------------------

/// Every fixture dataset and the catalog it is served by.
const FIXTURE_DATASETS: [(&str, &str); 11] = [
    ("ethereum", "evm"),
    ("optimism", "evm"),
    ("binance", "evm"),
    ("tempo", "evm"),
    ("tron", "tron"),
    ("bitcoin", "bitcoin"),
    ("solana", "solana"),
    ("kusama", "substrate"),
    ("moonbeam", "substrate"),
    ("hyperliquid", "hyperliquid_fills"),
    ("hyperliquid_replica_cmds", "hyperliquid_replica_cmds"),
];

/// `[blockNumberColumn] ++ itemOrderKeys ++ [addressColumn]?` must identify a
/// row. Deduplication and output ordering both use it: where two rows share a
/// key, which of them the response carries depends on scan order, and nothing in
/// the response says so.
///
/// This is the one CT-1 check that needs a chunk rather than a catalog — the
/// catalog cannot say whether the data holds.
///
/// Covers CT-1 · INV-D4
#[test]
#[ignore = "requires external fixture data"]
fn item_keys_are_unique_within_a_chunk() {
    if !fixture_tree_is_present() {
        return;
    }

    let mut checked = 0;

    for (dataset, catalog) in FIXTURE_DATASETS {
        let chunk = fixture_chunk(dataset);
        if !chunk.is_dir() {
            continue;
        }

        let metadata = meta(catalog);
        for (table, desc) in &metadata.tables {
            let mut key = vec![desc.block_number_column.as_str()];
            key.extend(desc.item_order_keys.iter().map(String::as_str));
            key.extend(desc.address_column.as_deref());

            let Some(batches) = read_columns(&chunk, table, &key) else {
                continue;
            };

            assert_eq!(
                duplicate_key(&batches, &key),
                None,
                "{dataset}.{table}: two rows share the item key {key:?}, so which one the \
                 response carries depends on scan order"
            );
            checked += 1;
        }
    }

    assert!(
        checked > 20,
        "only {checked} tables were checked, so the fixture tree is not the one this \
         test was written against"
    );
}

/// The first key value two rows share, if any.
///
/// The comparison goes through Arrow's row format rather than a hand-written
/// extractor per physical type, because the key columns include lists
/// (`instruction_address`) and integers at four widths, and a comparison that
/// silently declines to compare one of them reports uniqueness it never checked.
fn duplicate_key(batches: &[RecordBatch], key: &[&str]) -> Option<String> {
    let first = batches.first()?;

    let fields: Vec<SortField> = key
        .iter()
        .map(|name| SortField::new(first.column_by_name(name).unwrap().data_type().clone()))
        .collect();
    let converter =
        RowConverter::new(fields).expect("the key columns must have a row encoding to compare");

    let mut seen: HashSet<Vec<u8>> = HashSet::new();

    for batch in batches {
        let columns: Vec<ArrayRef> = key
            .iter()
            .map(|name| batch.column_by_name(name).unwrap().clone())
            .collect();
        let rows = converter.convert_columns(&columns).unwrap();

        for (row_idx, row) in rows.iter().enumerate() {
            if !seen.insert(row.as_ref().to_vec()) {
                let values: Vec<String> = columns
                    .iter()
                    .map(|c| {
                        arrow::util::display::array_value_to_string(c.as_ref(), row_idx)
                            .unwrap_or_default()
                    })
                    .collect();
                return Some(values.join(", "));
            }
        }
    }

    None
}

/// The sweep above passes on every chunk on disk, which is also what a check
/// that compares nothing does. This is the other direction: a chunk written with
/// one key twice, which the sweep must call out.
///
/// Covers CT-1 · INV-D4
#[test]
fn a_duplicated_item_key_is_caught() {
    let unique = height_chain_chunk(&[10, 11, 12]);
    let key = ["height", "seq"];

    let batches = read_columns(unique.path(), "events", &key).unwrap();
    assert_eq!(duplicate_key(&batches, &key), None);

    // The same block twice, which is what an archiver writing a fork boundary
    // twice produces.
    let repeated = height_chain_chunk(&[10, 11, 11]);
    let batches = read_columns(repeated.path(), "events", &key).unwrap();
    assert_eq!(duplicate_key(&batches, &key), Some("11, 0".to_string()));
}
