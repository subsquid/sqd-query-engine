//! How a column renders, and that reading a response out one way says what
//! reading it out another way says.
//!
//! The encoding is fixed by the catalog, not by the physical type the chunk
//! happens to store the column at. Whether the *chunk* can move under the
//! answer without changing it is the other half of the class, in `determinism`.

use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::output::{execute_chunk_arrow, execute_plan};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;

use crate::harness::arrow::read_frames;
use crate::harness::chunk::{chunk_with_column_filled, column_names, write_table};
use crate::harness::fixtures::{
    fixture_chunk, fixture_tree_has, fixture_tree_is_present, meta, run, FIXTURE_DATASETS,
};
use crate::harness::json::parse_response;
use crate::harness::synthetic::{
    catalog, logs_query, run as run_synthetic, uniform, weighted_chunk, BLOCKS,
};

/// Solana discriminator prefixes are selectable columns. Emitted as raw JSON
/// numbers, a `uint64` `d8` above 2^53 is silently re-read as a different value
/// by every JavaScript client — the discriminator a client receives is not the
/// one that was stored. They render as quoted hex, zero-padded to the column's
/// physical width, so that `"0x0640"` and `"0x640"` stay distinguishable.
///
/// Covers CT-6 · INV-O9
#[test]
#[ignore = "requires external fixture data"]
fn discriminator_columns_render_as_padded_hex() {
    if !fixture_tree_is_present() {
        return;
    }

    let solana = meta("solana");
    let body = run(
        "solana",
        &solana,
        br#"{"type":"solana","fromBlock":0,
             "fields":{"instruction":{"d1":true,"d2":true,"d4":true,"d8":true}},
             "instructions":[{"programId":["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"]}]}"#,
    )
    .unwrap();

    let mut seen = 0;
    for block in parse_response(&body) {
        let Some(items) = block.get("instructions").and_then(|v| v.as_array()) else {
            continue;
        };
        for item in items {
            for (key, width) in [("d1", 2), ("d2", 4), ("d4", 8), ("d8", 16)] {
                let value = item.get(key).unwrap();
                let s = value.as_str().unwrap_or_else(|| {
                    panic!("{key} must be a quoted string, got {value} — a JSON number loses precision above 2^53")
                });
                assert!(s.starts_with("0x"), "{key} = {s} must be 0x-prefixed");
                assert_eq!(
                    s.len() - 2,
                    width,
                    "{key} = {s} must be zero-padded to {width} hex digits"
                );
                assert_eq!(s.to_ascii_lowercase(), s, "{key} = {s} must be lowercase");
            }
            seen += 1;
        }
    }
    assert!(seen > 0, "fixture must contain whirlpool instructions");
}

/// `jsonVerbatim` splices stored bytes into a document the engine wrote, so a
/// column declared with it and holding anything else does not corrupt one field
/// — it ends the response, mid-object, for every client at once.
///
/// Tron's `internal_transactions.extra` is the trap: the archive writes it with
/// the same builder as `call_value_info`, so it reads as JSON in the chunk
/// schema, but the model types it `Option<HexBytes>` and appends it raw. The
/// bundled fixture leaves it null in all 9813 rows, which is why the ten Tron
/// fixture tests pass either way.
#[test]
#[ignore = "requires external fixture data"]
fn tron_internal_transaction_extra_renders_as_a_string() {
    if !fixture_tree_is_present() {
        return;
    }

    const EXTRA: &str = "a1b2c3d4";

    let tron = meta("tron");
    let chunk = chunk_with_column_filled("tron", "internal_transactions", "extra", EXTRA);
    let query = br#"{"type":"tron","fromBlock":82644089,"toBlock":82644089,
                     "fields":{"internalTransaction":{"extra":true}},
                     "internalTransactions":[{}]}"#;

    let parsed = parse_query(query, &tron).unwrap();
    let plan = compile(&parsed, &tron).unwrap();
    let body = execute_plan(&plan, &tron, chunk.path())
        .unwrap()
        .map(|out| out.into_json_lines())
        .unwrap_or_default();

    let mut seen = 0;
    for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let block: serde_json::Value = serde_json::from_slice(line).unwrap_or_else(|e| {
            panic!(
                "response line is not JSON ({e}): {}",
                String::from_utf8_lossy(&line[..line.len().min(200)])
            )
        });
        for item in block
            .get("internalTransactions")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            assert_eq!(item["extra"].as_str(), Some(EXTRA));
            seen += 1;
        }
    }

    assert!(seen > 0, "fixture must contain internal transactions");
}

/// A query over a range with no data yields `None`, on both output formats.
///
/// Covers CT-6 · INV-O1
#[test]
fn empty_result_is_none() {
    let chunk = weighted_chunk(BLOCKS, &[(12, 10)], &[]);
    let query = serde_json::json!({
        "type": "test",
        "fromBlock": 100,
        "toBlock": 200,
        "logs": [{}],
        "fields": {"log": {"data": true}}
    });
    assert!(run_synthetic(&catalog(), &chunk, query.clone()).is_none());

    let meta = catalog();
    let parsed = parse_query(query.to_string().as_bytes(), &meta).unwrap();
    let plan = compile(&parsed, &meta).unwrap();
    let reader = ParquetChunkReader::open(chunk.path()).unwrap();
    assert!(execute_chunk_arrow(&plan, &meta, &reader, false, false)
        .unwrap()
        .is_none());
}

/// Block-by-block iteration produces the same bytes as `into_json_lines`
/// (modulo framing), iteration state is tracked correctly, and
/// `into_json_lines` re-encodes everything regardless of prior iteration.
///
/// Covers CT-6 · INV-O1
#[test]
fn iteration_matches_json_lines() {
    let meta = catalog();
    let chunk = weighted_chunk(BLOCKS, &uniform(BLOCKS, 10), &[]);

    let mut blocks = run_synthetic(&meta, &chunk, logs_query()).unwrap();
    let mut iterated = Vec::new();
    let mut count = 0;
    while blocks.has_next_block() {
        blocks.write_next_block(&mut iterated);
        iterated.push(b'\n');
        count += 1;
    }
    assert_eq!(count, blocks.num_blocks());

    // Consumed iterator still re-encodes everything.
    assert_eq!(blocks.into_json_lines(), iterated);

    let mut partial = run_synthetic(&meta, &chunk, logs_query()).unwrap();
    partial.write_next_block(&mut Vec::new());
    assert_eq!(partial.into_json_lines(), iterated);
}

// ---------------------------------------------------------------------------
// INV-O14 — the binary Arrow rendering carries the values JSON does
// ---------------------------------------------------------------------------

/// A hex value's digits, lower-cased, with the leading zero an odd count leaves
/// out. Two renderings of one value are equal after this and only then.
fn hex_digits(text: &str) -> String {
    let digits = text.strip_prefix("0x").unwrap_or(text).to_ascii_lowercase();
    if digits.len() % 2 == 1 {
        format!("0{digits}")
    } else {
        digits
    }
}

/// One column of an Arrow table stream as `(block number, hex digits)`, in
/// stream order. Bytes print as hex; text, where a column stayed text, is
/// normalised the way JSON text is.
fn arrow_hex_column(
    batches: &[arrow::record_batch::RecordBatch],
    block_column: &str,
    column: &str,
) -> Vec<(u64, Option<String>)> {
    use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray, StringArray, UInt64Array};
    use arrow::compute::cast;
    use arrow::datatypes::DataType;

    let mut values = Vec::new();
    for batch in batches {
        let blocks = cast(
            batch.column_by_name(block_column).unwrap(),
            &DataType::UInt64,
        )
        .unwrap();
        let blocks = blocks.as_any().downcast_ref::<UInt64Array>().unwrap();
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("Arrow stream has no column '{column}'"));

        for row in 0..batch.num_rows() {
            let value = if col.is_null(row) {
                None
            } else if let Some(bytes) = col.as_any().downcast_ref::<BinaryArray>() {
                Some(faster_hex::hex_string(bytes.value(row)))
            } else if let Some(bytes) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                Some(faster_hex::hex_string(bytes.value(row)))
            } else if let Some(text) = col.as_any().downcast_ref::<StringArray>() {
                Some(hex_digits(text.value(row)))
            } else {
                panic!("'{column}' is {:?} in the Arrow stream", col.data_type());
            };
            values.push((blocks.value(row), value));
        }
    }
    values
}

/// One field of a JSON response as `(block number, text)`, in response order:
/// the header's field when `items` is `None`, every item's otherwise. `None`
/// when no object carries the key at all — the field is nested under a variant
/// there, and this comparison does not reach it.
fn json_hex_field(
    blocks: &[serde_json::Value],
    items: Option<&str>,
    key: &str,
) -> Option<Vec<(u64, Option<String>)>> {
    let mut values = Vec::new();
    let mut seen_key = false;

    for block in blocks {
        let number = block["header"]["number"].as_u64().unwrap();
        let objects: Vec<&serde_json::Value> = match items {
            None => vec![&block["header"]],
            Some(items) => block
                .get(items)
                .and_then(|v| v.as_array())
                .map(|a| a.iter().collect())
                .unwrap_or_default(),
        };

        for object in objects {
            let value = object.get(key);
            seen_key |= value.is_some();
            let text = value.and_then(|v| v.as_str()).map(String::from);
            values.push((number, text));
        }
    }

    seen_key.then_some(values)
}

/// [`json_hex_field`]'s text as [`hex_digits`], to compare with the Arrow side.
fn as_digits(values: &[(u64, Option<String>)]) -> Vec<(u64, Option<String>)> {
    values
        .iter()
        .map(|(block, text)| (*block, text.as_deref().map(hex_digits)))
        .collect()
}

/// Run one query both ways — JSON, and Arrow with hex decoded to bytes.
fn json_and_binary_arrow(
    catalog: &sqd_query_engine::metadata::DatasetDescription,
    chunk: &std::path::Path,
    query: &str,
) -> (
    Vec<serde_json::Value>,
    std::collections::HashMap<String, Vec<arrow::record_batch::RecordBatch>>,
) {
    let plan = compile(&parse_query(query.as_bytes(), catalog).unwrap(), catalog).unwrap();
    let reader = ParquetChunkReader::open(chunk).unwrap();

    let json = execute_plan(&plan, catalog, chunk)
        .unwrap()
        .map(|out| out.into_json_lines())
        .unwrap_or_default();
    let arrow = execute_chunk_arrow(&plan, catalog, &reader, false, true)
        .unwrap()
        .map(|out| out.into_data())
        .unwrap_or_default();

    (parse_response(&json), read_frames(&arrow))
}

/// A block table whose `gas_limit` is declared hex, holding `values`.
fn hex_column_chunk(
    values: Vec<Option<&str>>,
) -> (
    sqd_query_engine::metadata::DatasetDescription,
    tempfile::TempDir,
) {
    use arrow::array::{ArrayRef, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field};
    use sqd_query_engine::metadata::parse_dataset_description;
    use std::sync::Arc;

    let catalog = parse_dataset_description(
        r#"
version: v2
name: test

tables:
  blocks:
    output:
      name: block
      fields: [number, gas_limit]
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
      gas_limit: { type: string, encoding: hex_bytes }
"#,
    )
    .unwrap();

    let numbers: Vec<u64> = (1..=values.len() as u64).collect();
    let dir = tempfile::tempdir().unwrap();
    write_table(
        dir.path(),
        "blocks",
        vec![
            Field::new("number", DataType::UInt64, false),
            Field::new("gas_limit", DataType::Utf8, true),
        ],
        vec![
            Arc::new(UInt64Array::from(numbers)) as ArrayRef,
            Arc::new(StringArray::from(values)) as ArrayRef,
        ],
    );
    (catalog, dir)
}

const HEX_BLOCKS_QUERY: &str = r#"{"type":"test","fromBlock":0,"includeAllBlocks":true,
    "fields":{"block":{"number":true,"gasLimit":true}}}"#;

/// EVM chunks store quantities in minimal form, so an odd digit count is common:
/// `0x0`, `0x3938700`. The binary Arrow rendering decoded each value into a
/// buffer of half its digit count, failed on every odd one, and emitted `null`
/// — block `gasLimit` was null in every block of the real EVM chunk. An odd
/// count decodes as if it had a leading zero, which is the same number.
///
/// Covers CT-6 · INV-O14
#[test]
fn binary_arrow_keeps_an_odd_length_hex_value() {
    let (catalog, chunk) = hex_column_chunk(vec![
        Some("0x0"),
        Some("0x3938700"),
        Some("0x"),
        None,
        Some("0xdeadbeef"),
        Some("0xABC"),
        Some("0x1"),
    ]);
    let (json, arrow) = json_and_binary_arrow(&catalog, chunk.path(), HEX_BLOCKS_QUERY);

    let from_arrow = arrow_hex_column(&arrow["blocks"], "number", "gas_limit");
    let expected: Vec<(u64, Option<String>)> = [
        Some("00"),
        Some("03938700"),
        Some(""),
        None,
        Some("deadbeef"),
        Some("0abc"),
        Some("01"),
    ]
    .into_iter()
    .zip(1..)
    .map(|(digits, block)| (block, digits.map(String::from)))
    .collect();
    assert_eq!(from_arrow, expected);

    let from_json = as_digits(&json_hex_field(&json, None, "gasLimit").unwrap());
    assert_eq!(
        from_arrow, from_json,
        "the two renderings must carry one value"
    );
}

/// A column declared hex that holds something else has no byte rendering. A
/// `null` in its place is a different answer from the JSON one, which renders
/// the text verbatim, so the binary rendering refuses the chunk instead. The
/// text Arrow rendering has nothing to decode and serves it.
///
/// Covers CT-6 · INV-O14
#[test]
fn binary_arrow_refuses_a_hex_column_holding_something_else() {
    let (catalog, chunk) = hex_column_chunk(vec![Some("0x10"), Some("0xnot-hex")]);
    let plan = compile(
        &parse_query(HEX_BLOCKS_QUERY.as_bytes(), &catalog).unwrap(),
        &catalog,
    )
    .unwrap();
    let reader = ParquetChunkReader::open(chunk.path()).unwrap();

    let json = execute_plan(&plan, &catalog, chunk.path())
        .unwrap()
        .unwrap()
        .into_json_lines();
    assert_eq!(
        parse_response(&json)[1]["header"]["gasLimit"],
        "0xnot-hex",
        "JSON renders the stored text"
    );

    let Err(err) = execute_chunk_arrow(&plan, &catalog, &reader, false, true) else {
        panic!("the binary rendering cannot carry a value that is not hex");
    };
    assert_eq!(error_kind(&err), Some(ErrorKind::MalformedChunkData));
    assert!(
        err.to_string().contains("gas_limit"),
        "names the column: {err}"
    );

    assert!(
        execute_chunk_arrow(&plan, &catalog, &reader, false, false).is_ok(),
        "the text rendering does not decode"
    );
}

/// Every field a catalog declares hex, on every fixture chunk, rendered both
/// ways over the same blocks: the binary Arrow rendering must carry the value
/// the JSON one does, row for row, nulls included.
///
/// Covers CT-6 · INV-O14
#[test]
#[ignore = "requires external fixture data"]
fn binary_arrow_carries_every_hex_value_json_does() {
    use sqd_query_engine::metadata::JsonEncoding;
    use sqd_query_engine::output::snake_to_camel;

    /// Blocks per dataset: enough for every field to hold values, few enough
    /// that no response reaches the size budget.
    const SPAN: u64 = 20;

    let mut compared = 0usize;
    let mut odd_values = 0usize;

    for (dataset, catalog_name) in FIXTURE_DATASETS {
        if !fixture_tree_has(dataset) {
            continue;
        }
        let catalog = meta(catalog_name);
        let chunk = fixture_chunk(dataset);

        let first_block = format!(
            r#"{{"type":"{}","fromBlock":0,"includeAllBlocks":true,
                 "fields":{{"block":{{"number":true}}}}}}"#,
            catalog.name
        );
        let first = parse_response(&run(dataset, &catalog, first_block.as_bytes()).unwrap())[0]
            ["header"]["number"]
            .as_u64()
            .unwrap();

        for (table_name, table) in &catalog.tables {
            let Some(output_name) = table.output.name.as_deref() else {
                continue;
            };
            // A fixture chunk can be older than its catalog, and selecting a
            // column it lacks is an error on both renderings (INV-E3).
            let Some(stored) = column_names(&chunk, table_name) else {
                continue;
            };
            let hex_fields: Vec<&String> = table
                .output
                .fields
                .iter()
                .filter(|field| {
                    let encoding = table.columns.get(*field).and_then(|c| c.encoding.as_ref());
                    matches!(encoding, Some(JsonEncoding::HexBytes)) && stored.contains(field)
                })
                .collect();
            if hex_fields.is_empty() {
                continue;
            }

            let block_table = table.is_block_table();
            let request_name = table.request().name.as_deref();
            if !block_table && request_name.is_none() {
                continue;
            }

            let selection: Vec<String> = hex_fields
                .iter()
                .map(|f| format!(r#""{}":true"#, snake_to_camel(f)))
                .collect();
            let selection = selection.join(",");
            let (fields, items) = if block_table {
                (
                    format!(r#""block":{{"number":true,{selection}}}"#),
                    String::new(),
                )
            } else {
                (
                    format!(r#""block":{{"number":true}},"{output_name}":{{{selection}}}"#),
                    format!(r#","{}":[{{}}]"#, request_name.unwrap()),
                )
            };
            let query = format!(
                r#"{{"type":"{}","fromBlock":{first},"toBlock":{},"includeAllBlocks":true,
                     "fields":{{{fields}}}{items}}}"#,
                catalog.name,
                first + SPAN
            );

            let (json, arrow) = json_and_binary_arrow(&catalog, &chunk, &query);
            let frame = table.request_name(table_name);
            let Some(batches) = arrow.get(frame) else {
                // Nothing matched in the span: JSON must agree.
                let items = json_hex_field(&json, request_name, "number");
                assert!(
                    items.is_none_or(|v| v.is_empty()),
                    "{dataset}/{table_name}: JSON has rows the Arrow rendering lacks"
                );
                continue;
            };

            for field in hex_fields {
                let json_key = snake_to_camel(field);
                let items = if block_table { None } else { request_name };
                let Some(from_json) = json_hex_field(&json, items, &json_key) else {
                    continue;
                };
                let from_arrow = arrow_hex_column(batches, &table.block_number_column, field);

                let mut sorted_json = as_digits(&from_json);
                let mut sorted_arrow = from_arrow;
                sorted_json.sort();
                sorted_arrow.sort();
                assert_eq!(
                    sorted_arrow, sorted_json,
                    "{dataset}/{table_name}.{field}: the binary Arrow rendering differs from JSON"
                );

                compared += from_json.len();
                odd_values += from_json
                    .iter()
                    .filter_map(|(_, text)| text.as_deref())
                    .filter(|text| text.strip_prefix("0x").unwrap_or(text).len() % 2 == 1)
                    .count();
            }
        }
    }

    if compared == 0 {
        return;
    }
    assert!(
        odd_values > 0,
        "no odd-length value was compared, so the case this test is for never ran"
    );
    eprintln!("{compared} hex values compared, {odd_values} of them odd-length");
}
