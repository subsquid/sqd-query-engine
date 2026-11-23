//! The two Gear aliases over a chunk shaped like Vara's.
//!
//! `gearMessagesEnqueued` pinned `Gear.UserMessageEnqueued`, a name no Gear
//! runtime ever emitted, so it answered every query with nothing at 200. The
//! event it is for is `Gear.MessageQueued`, and `gearMessagesQueued` is the name
//! the SDK sends for it. `gearUserMessagesSent` is in use and must not move.

use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use std::sync::Arc;

use crate::harness::chunk::{blocks_parquet, write_table};
use crate::harness::fixtures::{meta, run_against};
use crate::harness::json::{items_in, parse_response};

const PROGRAM: &str = "0xa749ccd80d71637b450789e12e3d94524e9ae17877d1b59f5ddda784f89a2cba";
const OTHER: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
const BLOCK: u64 = 37_000_000;

/// One event per row, in `sort_key` order (`name`, `block_number`, `index`).
/// The extraction column is filled for the two events that carry a program id,
/// as the ingest fills it; `Gear.UserMessageEnqueued` is planted to show the old
/// predicate is gone.
const EVENTS: [(&str, u32, Option<&str>); 5] = [
    ("Balances.Transfer", 0, None),
    ("Gear.MessageQueued", 1, Some(PROGRAM)),
    ("Gear.MessageQueued", 2, Some(OTHER)),
    ("Gear.UserMessageEnqueued", 3, Some(PROGRAM)),
    ("Gear.UserMessageSent", 4, Some(PROGRAM)),
];

fn chunk() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    blocks_parquet(dir.path(), &[BLOCK]);
    write_table(
        dir.path(),
        "events",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("index", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("_gear_program_id", DataType::Utf8, true),
        ],
        vec![
            Arc::new(UInt64Array::from(vec![BLOCK; EVENTS.len()])) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(EVENTS.iter().map(|e| e.1))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(EVENTS.iter().map(|e| e.0))) as ArrayRef,
            Arc::new(StringArray::from_iter(EVENTS.iter().map(|e| e.2))) as ArrayRef,
        ],
    );
    dir
}

/// The `index` of every event the request returns.
fn indexes(chunk: &std::path::Path, request: &str, item: &str) -> Vec<u64> {
    let query = format!(
        r#"{{"type":"substrate","fromBlock":{BLOCK},"toBlock":{BLOCK},
            "fields":{{"event":{{"index":true}}}},
            "{request}":[{item}]}}"#
    );
    let body = run_against(&meta("substrate"), chunk, &query)
        .unwrap_or_else(|e| panic!("{request} {item} must be answerable: {e:#}"));
    parse_response(&body)
        .iter()
        .flat_map(|block| items_in(block, "events"))
        .map(|event| event["index"].as_u64().unwrap())
        .collect()
}

/// Covers CT-3 · INV-P2
#[test]
fn gear_messages_enqueued_selects_message_queued_events() {
    let chunk = chunk();
    for request in ["gearMessagesEnqueued", "gearMessagesQueued"] {
        assert_eq!(
            indexes(chunk.path(), request, "{}"),
            [1, 2],
            "{request} must select every Gear.MessageQueued event"
        );
        assert_eq!(
            indexes(
                chunk.path(),
                request,
                &format!(r#"{{"programId":["{PROGRAM}"]}}"#)
            ),
            [1],
            "{request} must narrow by the destination program"
        );
    }
}

/// Covers CT-3 · INV-P2
#[test]
fn gear_user_messages_sent_is_unchanged() {
    let chunk = chunk();
    assert_eq!(indexes(chunk.path(), "gearUserMessagesSent", "{}"), [4]);
    assert_eq!(
        indexes(
            chunk.path(),
            "gearUserMessagesSent",
            &format!(r#"{{"programId":["{PROGRAM}"]}}"#)
        ),
        [4]
    );
    assert_eq!(
        indexes(
            chunk.path(),
            "gearUserMessagesSent",
            &format!(r#"{{"programId":["{OTHER}"]}}"#)
        ),
        Vec::<u64>::new()
    );
}
