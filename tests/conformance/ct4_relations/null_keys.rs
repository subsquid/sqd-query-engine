//! A source row with a null in its key relates to nothing (INV-R5), so a
//! relation whose followed rows all have one has no target row to find. The
//! bundled Substrate catalog reaches this through system events, which belong
//! to no extrinsic and no call, so that each of `events.extrinsic`,
//! `events.call` and `events.stack` follows only null keys: as a pair, as a
//! pair and a path, and as a hierarchy. An event of an extrinsic that no call
//! emitted has a group but no address.

use arrow::array::{Array, UInt64Array};
use arrow::array::{ArrayRef, Float64Array, ListBuilder, StringArray, UInt32Array, UInt32Builder};
use arrow::datatypes::{DataType, Field, SchemaRef};
use serde_json::Value;
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest, Scanned};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

use crate::harness::chunk::{blocks_parquet, write_table};
use crate::harness::fixtures::meta;
use crate::harness::json::items_of;

const BLOCKS: u64 = 20;

/// Each block: two extrinsics with a call each, two events inside the first
/// extrinsic's call, one event of the second extrinsic outside its call, and
/// two system events, whose extrinsic and call are null.
fn chunk(extrinsic_index: DataType) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let blocks: Vec<u64> = (1..=BLOCKS).collect();
    blocks_parquet(dir.path(), &blocks);

    let per_block = |n: usize| -> Vec<u64> { blocks.iter().flat_map(|&b| vec![b; n]).collect() };
    let indices = |n: u32| -> Vec<u32> { blocks.iter().flat_map(|_| 0..n).collect() };
    let addresses = |rows: usize, present: &dyn Fn(usize) -> bool| -> ArrayRef {
        let mut list = ListBuilder::new(UInt32Builder::new());
        for row in 0..rows {
            match present(row) {
                true => list.append_value([Some(0u32)]),
                false => list.append_null(),
            }
        }
        Arc::new(list.finish())
    };

    write_table(
        dir.path(),
        "extrinsics",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("index", DataType::UInt32, false),
        ],
        vec![
            Arc::new(UInt64Array::from(per_block(2))),
            Arc::new(UInt32Array::from(indices(2))),
        ],
    );

    let calls = 2 * BLOCKS as usize;
    write_table(
        dir.path(),
        "calls",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("extrinsic_index", DataType::UInt32, false),
            Field::new("address", address_type(), true),
            Field::new("name", DataType::Utf8, false),
        ],
        vec![
            Arc::new(UInt64Array::from(per_block(2))),
            Arc::new(UInt32Array::from(indices(2))),
            addresses(calls, &|_| true),
            Arc::new(StringArray::from(vec!["Balances.transfer"; calls])),
        ],
    );

    let events = 5 * BLOCKS as usize;
    let in_call = |row: usize| row % 5 < 2;
    let names: Vec<&str> = (0..events)
        .map(|row| match row % 5 {
            0 | 1 => "Balances.Transfer",
            2 => "System.ExtrinsicSuccess",
            _ => "Session.NewSession",
        })
        .collect();
    let extrinsics: ArrayRef = match extrinsic_index {
        DataType::Float64 => Arc::new(Float64Array::from(vec![None; events])),
        _ => Arc::new(UInt32Array::from_iter((0..events).map(|row| {
            match row % 5 {
                0 | 1 => Some(0),
                2 => Some(1),
                _ => None,
            }
        }))),
    };
    write_table(
        dir.path(),
        "events",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("index", DataType::UInt32, false),
            Field::new("extrinsic_index", extrinsics.data_type().clone(), true),
            Field::new("call_address", address_type(), true),
            Field::new("name", DataType::Utf8, false),
        ],
        vec![
            Arc::new(UInt64Array::from(per_block(5))),
            Arc::new(UInt32Array::from(indices(5))),
            extrinsics,
            addresses(events, &in_call),
            Arc::new(StringArray::from(names)),
        ],
    );

    dir
}

fn address_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::UInt32, true)))
}

/// A chunk reader that counts the rows each table's scans return.
struct Reads {
    inner: ParquetChunkReader,
    scans: Mutex<Vec<String>>,
}

impl Reads {
    fn of(&self, table: &str) -> usize {
        let scans = self.scans.lock().unwrap();
        scans.iter().filter(|scanned| *scanned == table).count()
    }
}

impl ChunkReader for Reads {
    fn scan_rows(&self, table: &str, request: &ScanRequest) -> anyhow::Result<Scanned> {
        self.scans.lock().unwrap().push(table.to_owned());
        self.inner.scan_rows(table, request)
    }

    fn supports_row_positions(&self) -> bool {
        self.inner.supports_row_positions()
    }

    fn next_block_range_end(&self, table: &str, block_column: &str, from: u64) -> Option<u64> {
        self.inner.next_block_range_end(table, block_column, from)
    }

    fn estimate_scan_bytes(&self, table: &str, request: &ScanRequest) -> Option<u64> {
        self.inner.estimate_scan_bytes(table, request)
    }

    fn has_table(&self, table: &str) -> bool {
        self.inner.has_table(table)
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.inner.table_schema(table)
    }
}

fn query(names: &[&str], relation: &str) -> String {
    serde_json::json!({
        "type": "substrate",
        "fromBlock": 0,
        "fields": {
            "block": {"number": true},
            "event": {"name": true},
            "extrinsic": {"index": true},
            "call": {"name": true},
        },
        "events": [{"name": names, relation: true}],
    })
    .to_string()
}

/// The response to `query` over `chunk`, and the reader that served it.
fn run(
    catalog: &DatasetDescription,
    chunk: &Path,
    query: &str,
) -> (anyhow::Result<Vec<u8>>, Reads) {
    let plan = compile(&parse_query(query.as_bytes(), catalog).unwrap(), catalog).unwrap();
    let reads = Reads {
        inner: ParquetChunkReader::open(chunk).unwrap(),
        scans: Mutex::default(),
    };
    let answer = execute_chunk(&plan, catalog, &reads, false)
        .map(|output| output.expect("the range holds blocks").into_json_lines());

    (answer, reads)
}

/// What each relation flag pulls in, and from which table.
const RELATIONS: [(&str, &str, &str); 3] = [
    ("extrinsic", "extrinsics", "extrinsics"),
    ("call", "calls", "calls"),
    ("stack", "calls", "calls"),
];

/// Events with a null where a relation looks: system events for all three,
/// and an event outside any call for the two that follow the call address.
fn keyless(relation: &str) -> &'static [&'static str] {
    match relation {
        "extrinsic" => &["Session.NewSession"],
        _ => &["Session.NewSession", "System.ExtrinsicSuccess"],
    }
}

/// Following only rows with a null in the key reads no target, by any of the
/// three relations; following them beside rows that have a key finds exactly
/// what those rows find.
///
/// Covers CT-4 · INV-R5
#[test]
fn a_relation_whose_followed_keys_are_all_null_reads_no_target() {
    let catalog = meta("substrate");
    let chunk = chunk(DataType::UInt32);

    let mut read_for_nothing = Vec::new();
    for (relation, table, key) in RELATIONS {
        for &name in keyless(relation) {
            let (answer, reads) = run(&catalog, chunk.path(), &query(&[name], relation));
            let answer = answer.unwrap();
            assert!(
                !items_of(&answer, "events").is_empty(),
                "{relation}, {name}"
            );
            assert!(
                items_of(&answer, key).is_empty(),
                "{relation}, {name}: relates to nothing"
            );
            if reads.of(table) > 0 {
                read_for_nothing.push(format!("{relation} of {name}"));
            }
        }

        let (keyed, _) = run(
            &catalog,
            chunk.path(),
            &query(&["Balances.Transfer"], relation),
        );
        let every_name = [
            "Balances.Transfer",
            "System.ExtrinsicSuccess",
            "Session.NewSession",
        ];
        let (all, reads) = run(&catalog, chunk.path(), &query(&every_name, relation));
        let related: Vec<(u64, Value)> = items_of(&keyed.unwrap(), key);
        assert_eq!(related.len(), BLOCKS as usize, "{relation}: one per block");
        let expected = match relation {
            "extrinsic" => 2 * BLOCKS as usize,
            _ => BLOCKS as usize,
        };
        let all = items_of(&all.unwrap(), key);
        assert_eq!(all.len(), expected, "{relation}");
        assert!(related.iter().all(|row| all.contains(row)), "{relation}");
        assert!(reads.of(table) > 0, "{relation}: keys to follow are read");
    }

    assert!(
        read_for_nothing.is_empty(),
        "nothing to follow, yet read: {read_for_nothing:?}"
    );
}

/// A key column the engine cannot read a key from is not a column of nulls:
/// both joins still report it, however null its values. The hierarchy skips a
/// row with a null address before it reads the group key, so it answers here.
///
/// Covers CT-4 · INV-R5
#[test]
fn an_unreadable_key_column_is_still_refused() {
    let catalog = meta("substrate");
    let readable = chunk(DataType::UInt32);
    let unreadable = chunk(DataType::Float64);

    for relation in ["extrinsic", "call"] {
        let query = query(&["Session.NewSession"], relation);

        let (answer, _) = run(&catalog, readable.path(), &query);
        answer.unwrap_or_else(|error| panic!("{relation}: null keys match nothing: {error:#}"));
        let (answer, _) = run(&catalog, unreadable.path(), &query);
        let Err(error) = answer else {
            panic!("{relation}: a float is no key")
        };
        assert!(
            format!("{error:#}").contains("Float64"),
            "{relation}: the refusal names the type: {error:#}"
        );
    }
}
