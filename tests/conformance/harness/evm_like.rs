//! A chain shaped like a real one, and the chunk written against it.
//!
//! `synthetic` answers what a block looks like when a table declares its weight;
//! this answers what a query looks like when it filters, joins, trims a range
//! and renders every column kind — which is what CT-5's partition split and
//! CT-6's rewrite comparisons both need to be measuring something.
//!
//! It is deliberately not a fixture: a law that only holds where the external
//! tree is checked out is a law the per-PR gate never checks.

use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field};
use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use std::sync::Arc;
use tempfile::TempDir;

use crate::harness::chunk::write_table;

/// A cut-down EVM: the log table's four-part sort key, a relation onto
/// transactions — which is where a narrowed join key would bite — and, back from
/// transactions, relations onto logs and traces. That cycle is deliberate: it is
/// what makes "relations resolve one hop" ([INV-R2]) a question this chain can
/// ask, since a second hop would come back with logs the filter excluded, and a
/// third-table one with traces nothing asked for.
///
/// Two of the three relations are named in two words, as the real catalog names
/// them. A request spells the key camelCased and the catalog keys its map in
/// snake case, so a chain whose relations are all one word is a chain where
/// those two names are the same string and nothing notices a missing
/// conversion.
///
/// [INV-R2]: ../../../spec/07-invariants.md#inv-r2
const CHAIN: &str = r#"
name: test

tables:
  blocks:
    output:
      name: block
      fields: [number, hash]
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
      hash: { type: string, encoding: hex_bytes }

  logs:
    request:
      name: logs
      filters: [address, topic0]
      relations:
        transaction:
          table: transactions
          key: [block_number, transaction_index]
    output:
      name: log
      fields: [log_index, transaction_index, address, topic0, data]
    block_number_column: block_number
    item_order_keys: [transaction_index, log_index]
    sort_key: [topic0, address, block_number, log_index]
    columns:
      block_number: { type: uint64 }
      log_index: { type: uint32 }
      transaction_index: { type: uint32 }
      address: { type: string, encoding: hex_bytes }
      topic0: { type: string, encoding: hex_bytes }
      data: { type: string, encoding: hex_bytes }

  transactions:
    request:
      name: transactions
      filters: [transaction_index]
      relations:
        transaction_logs:
          table: logs
          key: [block_number, transaction_index]
        transaction_traces:
          table: traces
          key: [block_number, transaction_index]
    output:
      name: transaction
      fields: [transaction_index, gas_used]
    block_number_column: block_number
    item_order_keys: [transaction_index]
    sort_key: [block_number, transaction_index]
    columns:
      block_number: { type: uint64 }
      transaction_index: { type: uint32 }
      gas_used: { type: uint64 }

  traces:
    request:
      name: traces
      filters: [kind]
    output:
      name: trace
      fields: [transaction_index, trace_index, kind]
    block_number_column: block_number
    item_order_keys: [transaction_index, trace_index]
    sort_key: [block_number, transaction_index, trace_index]
    columns:
      block_number: { type: uint64 }
      transaction_index: { type: uint32 }
      trace_index: { type: uint32 }
      kind: { type: string }
"#;

pub const BLOCKS: std::ops::RangeInclusive<u64> = 100..=115;
const LOGS_PER_BLOCK: u32 = 4;

/// A 32-byte hex word, the way a chunk stores one: a `0x`-prefixed string, not
/// the raw bytes.
pub fn word(byte: u8) -> String {
    format!("0x{byte:064x}")
}

/// A 20-byte hex address.
pub fn address(byte: u8) -> String {
    format!("0x{byte:040x}")
}

pub fn catalog() -> DatasetDescription {
    parse_dataset_description(CHAIN).unwrap()
}

/// Sixteen blocks; four logs, two transactions and four traces each, over two
/// addresses and two topics — enough that a filter prunes something and a row-group boundary
/// can fall inside a block.
pub fn chunk() -> TempDir {
    let dir = tempfile::tempdir().unwrap();

    let numbers: Vec<u64> = BLOCKS.collect();
    write_table(
        dir.path(),
        "blocks",
        vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("hash", DataType::Utf8, false),
        ],
        vec![
            Arc::new(UInt64Array::from(numbers.clone())) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                numbers.iter().map(|n| word(*n as u8)),
            )) as ArrayRef,
        ],
    );

    // Written in the declared sort order: topic0, address, block_number,
    // log_index. Anything a test does to that order is then a change from it.
    let mut rows: Vec<(u64, u32, u32, u8, u8)> = Vec::new();
    for block in BLOCKS {
        for index in 0..LOGS_PER_BLOCK {
            let address = (index % 2) as u8;
            let topic = (index / 2) as u8;
            rows.push((block, index, index / 2, address, topic));
        }
    }
    rows.sort_by_key(|(block, index, _, address, topic)| (*topic, *address, *block, *index));

    write_table(
        dir.path(),
        "logs",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("topic0", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, false),
        ],
        vec![
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.2))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| address(r.3)),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| word(r.4)),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| format!("0x{:016x}", r.1)),
            )) as ArrayRef,
        ],
    );

    let txs: Vec<(u64, u32)> = BLOCKS
        .flat_map(|b| (0..2u32).map(move |i| (b, i)))
        .collect();
    write_table(
        dir.path(),
        "transactions",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("gas_used", DataType::UInt64, false),
        ],
        vec![
            Arc::new(UInt64Array::from_iter_values(txs.iter().map(|t| t.0))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(txs.iter().map(|t| t.1))) as ArrayRef,
            Arc::new(UInt64Array::from_iter_values(
                txs.iter().map(|t| t.0 * 100 + t.1 as u64),
            )) as ArrayRef,
        ],
    );

    // Two traces per transaction, of two kinds. Nothing reaches this table but
    // `transactions.trace`, which is what lets a test say "no trace was asked
    // for, so no trace came back".
    let traces: Vec<(u64, u32, u32)> = txs
        .iter()
        .flat_map(|(block, index)| (0..2u32).map(move |trace| (*block, *index, trace)))
        .collect();
    write_table(
        dir.path(),
        "traces",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("trace_index", DataType::UInt32, false),
            Field::new("kind", DataType::Utf8, false),
        ],
        vec![
            Arc::new(UInt64Array::from_iter_values(traces.iter().map(|t| t.0))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(traces.iter().map(|t| t.1))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(traces.iter().map(|t| t.2))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                traces.iter().map(|t| TRACE_KINDS[t.2 as usize % 2]),
            )) as ArrayRef,
        ],
    );

    dir
}

/// The two values `traces.kind` takes.
pub const TRACE_KINDS: [&str; 2] = ["call", "create"];

/// A filter (so pruning runs), a relation (so a join runs), a block range
/// narrower than the chunk (so trimming runs), and a projection of every column
/// kind the chain has.
pub fn query(from: u64, to: u64) -> String {
    query_with(
        from,
        to,
        &format!(r#"{{"address":["{}"],"transaction":true}}"#, address(0)),
    )
}

/// The same query with a chosen item request, so a test can vary the filter and
/// hold everything else still.
pub fn query_with(from: u64, to: u64, item_request: &str) -> String {
    format!(
        r#"{{"type":"test","fromBlock":{from},"toBlock":{to},
            "fields":{{"block":{{"number":true,"hash":true}},
                      "log":{{"logIndex":true,"transactionIndex":true,"address":true,
                             "topic0":true,"data":true}},
                      "transaction":{{"transactionIndex":true,"gasUsed":true}}}},
            "logs":[{item_request}]}}"#
    )
}

/// The item requests worth splitting a range over, or reading twice, or
/// rewriting a chunk under. One filter kind is one mechanism, and a law asserted
/// over one query is a law asserted about that query.
pub fn item_requests() -> Vec<(&'static str, String)> {
    vec![
        ("no filter", "{}".to_string()),
        (
            "an address filter",
            format!(r#"{{"address":["{}"]}}"#, address(PRESENT_ADDRESS)),
        ),
        (
            "an address filter and a relation",
            format!(
                r#"{{"address":["{}"],"transaction":true}}"#,
                address(PRESENT_ADDRESS)
            ),
        ),
        (
            "two filters at once",
            format!(
                r#"{{"address":["{}"],"topic0":["{}"]}}"#,
                address(PRESENT_ADDRESS),
                word(PRESENT_TOPIC)
            ),
        ),
        (
            "a filter nothing matches",
            format!(r#"{{"address":["{}"]}}"#, address(ABSENT_ADDRESS)),
        ),
        ("an empty filter list", r#"{"address":[]}"#.to_string()),
        (
            "a relation with no filter",
            r#"{"transaction":true}"#.to_string(),
        ),
    ]
}

/// The two addresses and two topics the chunk holds, and one address it does
/// not.
pub const PRESENT_ADDRESS: u8 = 0;
pub const ABSENT_ADDRESS: u8 = 9;
pub const PRESENT_TOPIC: u8 = 1;
