//! The footer estimate decides whether a query without relations reads its
//! payloads in one pass. It may count more than that pass decodes, which costs
//! a second pass; it must never count less, which costs memory past the page.
//! A footer that cannot bound what its pages decode to has no estimate.
//!
//! These are the ways found for a footer to understate its pages, each a column
//! or a writer below:
//!
//! - a dictionary keeps a repeated value once, and each row decodes to a copy;
//! - statistics bound values, not lengths: "a" and "z" surround longer values,
//!   and a writer may truncate the bounds or leave them out;
//! - a dictionary the footer does not point at is still a dictionary;
//! - prefix compression stores a shared prefix once;
//! - a constant boolean column compresses to one run;
//! - a decoded type can be wider than the stored one: a 32-bit decimal decodes
//!   to 128 bits, a large string has 64-bit offsets, a list adds its own;
//! - a row group's block bounds say nothing about how its rows spread between
//!   them.
//!
//! ADR-15 records why each is out of bounds, and what that costs.

use crate::harness::chunk::{
    dictionary_unannounced, edit_footer, without_byte_counts, write_table_with,
};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Int64Array,
    LargeBinaryArray, ListArray, ListBuilder, StringArray, StringBuilder, StructArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, UInt16Type};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowSchemaConverter;
use parquet::basic::{Encoding, Type as PhysicalType};
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use sqd_query_engine::scan::{ChunkReader, ParquetChunkReader, ScanRequest};
use std::sync::Arc;

const TABLE: &str = "items";
const ROWS: usize = 1000;
const LATE_BLOCK: u64 = 1_000_000;

/// Ten rows in each of the first hundred blocks, and the last row a million
/// blocks later.
fn block_of(row: usize) -> u64 {
    if row == ROWS - 1 {
        LATE_BLOCK
    } else {
        (row / 10) as u64
    }
}

fn columns() -> Vec<(&'static str, ArrayRef, bool)> {
    let long = vec![b'm'; 1024];
    let payload: Vec<&[u8]> = (0..ROWS)
        .map(|row| match row {
            0 => b"a".as_slice(),
            _ if row == ROWS - 1 => b"z".as_slice(),
            _ => long.as_slice(),
        })
        .collect();

    let mut names = ListBuilder::new(StringBuilder::new());
    for _ in 0..ROWS {
        names.values().append_value("n");
        names.values().append_value("nn");
        names.append(true);
    }

    let pair = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["x".repeat(16); ROWS])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::UInt32, false)),
            Arc::new(UInt32Array::from_iter_values(0..ROWS as u32)) as ArrayRef,
        ),
    ]);

    let amount = Decimal128Array::from_iter_values((0..ROWS).map(|row| row as i128))
        .with_precision_and_scale(9, 2)
        .unwrap();
    let path = ListArray::from_iter_primitive::<UInt16Type, _, _>(
        (0..ROWS).map(|row| Some(vec![Some(row as u16)])),
    );
    let hash = FixedSizeBinaryArray::try_from_iter((0..ROWS).map(|row| [row as u8; 32])).unwrap();

    vec![
        (
            "block_number",
            Arc::new(UInt64Array::from_iter_values((0..ROWS).map(block_of))) as ArrayRef,
            false,
        ),
        ("payload", Arc::new(BinaryArray::from(payload)), false),
        (
            "text",
            Arc::new(StringArray::from_iter_values(
                (0..ROWS).map(|row| format!("{row:08}")),
            )),
            false,
        ),
        (
            "big",
            Arc::new(LargeBinaryArray::from(vec![b"".as_slice(); ROWS])),
            false,
        ),
        (
            "flag",
            Arc::new(BooleanArray::from(vec![true; ROWS])),
            false,
        ),
        ("amount", Arc::new(amount), false),
        (
            "maybe",
            Arc::new(Int64Array::from_iter(
                (0..ROWS).map(|row| (row % 2 == 0).then_some(row as i64)),
            )),
            true,
        ),
        ("path", Arc::new(path), false),
        ("names", Arc::new(names.finish()), false),
        ("hash", Arc::new(hash), false),
        ("pair", Arc::new(pair), false),
    ]
}

struct Writer {
    name: &'static str,
    props: WriterProperties,
    footer: fn(ColumnChunkMetaData) -> ColumnChunkMetaData,
    /// The footer states what every column decodes to, so the estimate has no
    /// reason to give up.
    sized: bool,
}

fn as_written(column: ColumnChunkMetaData) -> ColumnChunkMetaData {
    column
}

/// The same encoding for every byte-string leaf, which is all it applies to.
fn byte_strings_encoded(schema: &Schema, encoding: Encoding) -> WriterProperties {
    let parquet = ArrowSchemaConverter::new().convert(schema).unwrap();
    let mut props = WriterProperties::builder().set_dictionary_enabled(false);
    for leaf in parquet.columns() {
        if leaf.physical_type() == PhysicalType::BYTE_ARRAY {
            props = props.set_column_encoding(leaf.path().clone(), encoding);
        }
    }
    props.build()
}

fn writers(schema: &Schema) -> Vec<Writer> {
    let no_statistics =
        || WriterProperties::builder().set_statistics_enabled(EnabledStatistics::None);
    let version_2 = || WriterProperties::builder().set_writer_version(WriterVersion::PARQUET_2_0);

    vec![
        Writer {
            name: "parquet defaults",
            props: WriterProperties::default(),
            footer: as_written,
            sized: true,
        },
        Writer {
            name: "no dictionary",
            props: WriterProperties::builder()
                .set_dictionary_enabled(false)
                .build(),
            footer: as_written,
            sized: true,
        },
        Writer {
            name: "small row groups and pages",
            props: WriterProperties::builder()
                .set_max_row_group_size(128)
                .set_data_page_row_count_limit(64)
                .set_write_batch_size(64)
                .build(),
            footer: as_written,
            sized: true,
        },
        Writer {
            name: "no statistics",
            props: no_statistics().build(),
            footer: as_written,
            sized: false,
        },
        Writer {
            name: "no statistics and no dictionary",
            props: no_statistics().set_dictionary_enabled(false).build(),
            footer: as_written,
            sized: true,
        },
        Writer {
            name: "statistics without byte counts",
            props: WriterProperties::default(),
            footer: without_byte_counts,
            sized: false,
        },
        Writer {
            name: "truncated statistics without byte counts",
            props: WriterProperties::builder()
                .set_statistics_truncate_length(Some(2))
                .build(),
            footer: without_byte_counts,
            sized: false,
        },
        Writer {
            name: "prefix compression without byte counts",
            props: byte_strings_encoded(schema, Encoding::DELTA_BYTE_ARRAY),
            footer: without_byte_counts,
            sized: false,
        },
        Writer {
            name: "length-prefixed byte strings without byte counts",
            props: byte_strings_encoded(schema, Encoding::DELTA_LENGTH_BYTE_ARRAY),
            footer: without_byte_counts,
            sized: true,
        },
        Writer {
            name: "an unannounced dictionary",
            props: no_statistics().build(),
            footer: dictionary_unannounced,
            sized: false,
        },
        Writer {
            name: "version 2 pages without dictionary",
            props: version_2().set_dictionary_enabled(false).build(),
            footer: as_written,
            sized: true,
        },
        Writer {
            name: "version 2 pages without dictionary or byte counts",
            props: version_2().set_dictionary_enabled(false).build(),
            footer: without_byte_counts,
            sized: false,
        },
    ]
}

/// What the scan's arrays hold, not what was allocated for them.
fn decoded_bytes(batches: &[RecordBatch]) -> u64 {
    batches
        .iter()
        .flat_map(RecordBatch::columns)
        .map(|column| column.to_data().get_slice_memory_size().unwrap() as u64)
        .sum()
}

#[test]
fn the_estimate_never_undercounts_what_a_scan_decodes() {
    let columns = columns();
    let schema = Schema::new(
        columns
            .iter()
            .map(|(name, array, nullable)| Field::new(*name, array.data_type().clone(), *nullable))
            .collect::<Vec<_>>(),
    );
    let ranges = [
        (None, None),
        (Some(0), Some(99)),
        (Some(5), Some(5)),
        (Some(LATE_BLOCK), None),
        (Some(200), Some(300)),
    ];
    let mut failures = Vec::new();

    for writer in writers(&schema) {
        let dir = tempfile::tempdir().unwrap();
        write_table_with(
            dir.path(),
            TABLE,
            schema.fields().iter().map(|f| f.as_ref().clone()).collect(),
            columns.iter().map(|(_, array, _)| array.clone()).collect(),
            writer.props,
        );
        edit_footer(dir.path(), TABLE, writer.footer);
        let reader = ParquetChunkReader::open(dir.path()).unwrap();

        for (column, _, _) in &columns {
            for (from, to) in ranges {
                let mut request = ScanRequest::new(vec![column]);
                request.block_number_column = Some("block_number");
                request.from_block = from;
                request.to_block = to;

                let decoded = decoded_bytes(&reader.scan(TABLE, &request).unwrap());
                let case = format!("{} / {column} / {from:?}..{to:?}", writer.name);

                match reader.estimate_scan_bytes(TABLE, &request) {
                    Some(estimate) if estimate < decoded => failures.push(format!(
                        "{case}: estimated {estimate} bytes, the scan decoded {decoded}"
                    )),
                    None if writer.sized => failures.push(format!(
                        "{case}: the footer states every size, yet there is no estimate"
                    )),
                    _ => {}
                }
            }
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
