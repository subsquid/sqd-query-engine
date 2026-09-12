//! A chain shaped like Solana, and the chunk written against it.
//!
//! `evm_like` filters on text and joins on integers. This one has the column
//! kinds the other does not: a discriminator narrowed to one byte, a boolean
//! flag, a bloom over accounts, a roll field spread across positional columns,
//! and a timestamp — each of which a writer of another vintage has stored at a
//! type the catalog does not name. CT-8 rewrites the chunk that way and asks
//! whether the engine notices.

use arrow::array::{
    ArrayRef, BooleanArray, FixedSizeBinaryArray, ListArray, StringArray, TimestampSecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, TimeUnit};
use sqd_query_engine::metadata::{parse_dataset_description, DatasetDescription};
use sqd_query_engine::scan::predicate::bloom_bit;
use std::sync::Arc;
use tempfile::TempDir;

use crate::harness::chunk::write_table;

const CHAIN: &str = r#"
version: v2
name: test

tables:
  blocks:
    output:
      name: block
      fields: [number, timestamp]
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
      timestamp: { type: timestamp_second }

  instructions:
    request:
      name: instructions
      filters: [program_id, discriminator, d1, is_committed, mentions_account, transaction_index]
      special_filters:
        discriminator:
          kind: discriminator
          by_length:
            "1": d1
            "2": d2
            "3": d3
        mentions_account:
          kind: bloom
          column: accounts_bloom
          bytes: 64
          hashes: 7
    output:
      name: instruction
      fields: [transaction_index, program_id, accounts, d1, is_committed]
      virtual_fields:
        accounts:
          kind: roll
          columns: [a0, a1, a2, rest_accounts]
    block_number_column: block_number
    item_order_keys: [transaction_index]
    sort_key: [program_id, d1, block_number, transaction_index]
    columns:
      block_number: { type: uint64 }
      transaction_index: { type: uint32 }
      program_id: { type: string }
      d1: { type: uint8, encoding: hex_number }
      d2: { type: uint16, system: true }
      d3: { type: fixed_binary_3, system: true }
      a0: { type: string }
      a1: { type: string }
      a2: { type: string }
      rest_accounts: { type: list_string }
      accounts_bloom: { type: fixed_binary_64, system: true }
      is_committed: { type: boolean }
"#;

pub const BLOCKS: std::ops::RangeInclusive<u64> = 200..=207;
const INSTRUCTIONS_PER_BLOCK: u32 = 4;
const BLOOM_BYTES: usize = 64;
const BLOOM_HASHES: usize = 7;

/// The two programs, and the discriminator each one's instructions carry. Both
/// discriminators fit a signed byte, so the column can be stored at any width;
/// both program ids spell a number, so the column can be stored as one.
pub const PROGRAMS: [&str; 2] = ["100", "101"];
pub const DISCRIMINATORS: [u8; 2] = [0x2a, 0x07];

pub fn catalog() -> DatasetDescription {
    parse_dataset_description(CHAIN).unwrap()
}

/// The `n`-th account name.
pub fn account(n: u32) -> String {
    format!("acc-{n}")
}

fn bloom_of<'a>(accounts: impl IntoIterator<Item = &'a str>) -> Vec<u8> {
    let mut bytes = vec![0u8; BLOOM_BYTES];
    for value in accounts {
        for n in 0..BLOOM_HASHES {
            let bit = bloom_bit(value.as_bytes(), n, BLOOM_BYTES * 8);
            bytes[bit / 8] |= 1 << (bit % 8);
        }
    }
    bytes
}

/// Eight blocks of four instructions: two programs, two discriminators, half
/// committed, and accounts that spread across the roll's positional columns
/// into its trailing list.
pub fn chunk() -> TempDir {
    let dir = tempfile::tempdir().unwrap();

    let numbers: Vec<u64> = BLOCKS.collect();
    write_table(
        dir.path(),
        "blocks",
        vec![
            Field::new("number", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
        ],
        vec![
            Arc::new(UInt64Array::from(numbers.clone())) as ArrayRef,
            Arc::new(TimestampSecondArray::from_iter_values(
                numbers.iter().map(|n| 1_700_000_000 + *n as i64),
            )) as ArrayRef,
        ],
    );

    struct Row {
        block: u64,
        index: u32,
        program: usize,
        discriminator: u8,
        committed: bool,
        accounts: Vec<String>,
    }

    let mut rows: Vec<Row> = Vec::new();
    for block in BLOCKS {
        for index in 0..INSTRUCTIONS_PER_BLOCK {
            let program = (index % 2) as usize;
            // Between one and five accounts, so the roll uses every source.
            let count = 1 + ((block + index as u64) % 5) as u32;
            rows.push(Row {
                block,
                index,
                program,
                discriminator: DISCRIMINATORS[program],
                committed: index < 2,
                accounts: (0..count).map(|n| account(index + n)).collect(),
            });
        }
    }
    rows.sort_by_key(|r| (r.program, r.discriminator, r.block, r.index));

    let positional = |slot: usize| -> ArrayRef {
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.accounts.get(slot).map(String::as_str))
                .collect::<Vec<_>>(),
        ))
    };
    let rest: ListArray = {
        let mut builder =
            arrow::array::builder::ListBuilder::new(arrow::array::builder::StringBuilder::new());
        for row in &rows {
            for account in row.accounts.iter().skip(3) {
                builder.values().append_value(account);
            }
            builder.append(true);
        }
        builder.finish()
    };
    let blooms = FixedSizeBinaryArray::try_from_iter(
        rows.iter()
            .map(|r| bloom_of(r.accounts.iter().map(String::as_str))),
    )
    .unwrap();

    write_table(
        dir.path(),
        "instructions",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("program_id", DataType::Utf8, false),
            Field::new("d1", DataType::UInt8, false),
            Field::new("d2", DataType::UInt16, false),
            Field::new("d3", DataType::FixedSizeBinary(3), false),
            Field::new("a0", DataType::Utf8, true),
            Field::new("a1", DataType::Utf8, true),
            Field::new("a2", DataType::Utf8, true),
            Field::new(
                "rest_accounts",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("accounts_bloom", DataType::FixedSizeBinary(64), false),
            Field::new("is_committed", DataType::Boolean, false),
        ],
        vec![
            Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.block))) as ArrayRef,
            Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.index))) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| PROGRAMS[r.program]),
            )) as ArrayRef,
            Arc::new(UInt8Array::from_iter_values(
                rows.iter().map(|r| r.discriminator),
            )) as ArrayRef,
            Arc::new(UInt16Array::from_iter_values(
                rows.iter().map(|r| (r.discriminator as u16) << 8),
            )) as ArrayRef,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    rows.iter().map(|r| vec![r.discriminator, 0, 0]),
                )
                .unwrap(),
            ) as ArrayRef,
            positional(0),
            positional(1),
            positional(2),
            Arc::new(rest) as ArrayRef,
            Arc::new(blooms) as ArrayRef,
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.committed).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    );

    dir
}

/// A query over the whole chunk with a chosen item request, selecting every
/// field the chain has.
pub fn query_with(item_request: &str) -> String {
    query_selecting(
        item_request,
        r#"{"transactionIndex":true,"programId":true,"accounts":true,"d1":true,"isCommitted":true}"#,
    )
}

/// The same, with a chosen instruction projection, for a test whose rewritten
/// column must be filtered on but not rendered.
pub fn query_selecting(item_request: &str, instruction_fields: &str) -> String {
    format!(
        r#"{{"type":"test","fromBlock":{from},"toBlock":{to},
            "fields":{{"block":{{"number":true,"timestamp":true}},
                      "instruction":{instruction_fields}}},
            "instructions":[{item_request}]}}"#,
        from = BLOCKS.start(),
        to = BLOCKS.end(),
    )
}
