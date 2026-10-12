//! Query execution: a plan, its catalog and a chunk in, a response out.
//!
//! [`execute`] runs four steps in order. [`Execution::prepare`] checks the plan
//! against the catalog and the chunk and chooses how payloads are read;
//! [`select_page`] reads and weighs complete block ranges until the response
//! budget is spent; [`materialize_page`] reads the payloads a deferred
//! selection left out; and `output` encodes the page as JSON or Arrow.

mod materialize;
mod page;
mod prepare;
mod tables;

use crate::metadata::DatasetDescription;
use crate::output::sources::{output_tables, OutputHeader};
use crate::output::{arrow_output, ArrowOutput, OutputFormat, QueryOutput};
use crate::query::Plan;
use crate::scan::{ChunkReader, ParquetChunkReader};
use anyhow::Result;
use materialize::materialize_page;
use page::select_page;
use prepare::Execution;
use std::fmt::Display;
use std::path::Path;
use std::time::Instant;

/// Execution controls. Disabling range reads provides a full-read reference.
#[derive(Debug, Clone, Copy)]
pub struct ExecOptions {
    /// Print stage timings to stderr.
    pub profile: bool,
    /// The cumulative block weight a response may carry (`P-WEIGHT-BUDGET`).
    pub weight_budget: u64,
    /// Read complete block ranges until the response budget is exhausted.
    /// When disabled, read the whole requested range before selecting blocks.
    pub range_reads: bool,
}

impl Default for ExecOptions {
    fn default() -> Self {
        Self {
            profile: false,
            weight_budget: crate::output::weight::MAX_RESPONSE_BYTES,
            range_reads: true,
        }
    }
}

impl ExecOptions {
    pub fn profiled(profile: bool) -> Self {
        Self {
            profile,
            ..Self::default()
        }
    }
}

/// Execute a plan against a chunk directory. Returns `None` if the output
/// contains no blocks, which only happens when the queried block range doesn't
/// intersect the chunk's data — a query whose filters match nothing still
/// yields the boundary blocks of the range as header-only entries. See
/// [`QueryOutput`] for the block range metadata and lazy block encoding.
pub fn execute_plan(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk_dir: &Path,
) -> Result<Option<QueryOutput>> {
    let chunk = ParquetChunkReader::open(chunk_dir)?;
    execute_chunk(plan, metadata, &chunk, false)
}

/// Execute a plan against any ChunkReader, with the engine's knobs given
/// explicitly. See [`ExecOptions`].
pub fn execute_chunk_with(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    options: ExecOptions,
) -> Result<Option<QueryOutput>> {
    match execute(plan, metadata, chunk, options, OutputFormat::Json)? {
        FmtOutput::Json(blocks) => Ok(blocks.map(|b| *b)),
        FmtOutput::Arrow(_) => unreachable!(),
    }
}

/// Execute a plan with timing instrumentation printed to stderr.
pub fn execute_plan_profiled(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk_dir: &Path,
) -> Result<Option<QueryOutput>> {
    let chunk = ParquetChunkReader::open(chunk_dir)?;
    execute_chunk(plan, metadata, &chunk, true)
}

/// Execute a plan against a chunk directory, producing flat per-table Arrow IPC
/// streams instead of nested JSON (prototype). `compress` toggles Arrow's
/// built-in Zstd. See [`crate::output::arrow_out`].
pub fn execute_plan_arrow(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk_dir: &Path,
    compress: bool,
    binary: bool,
) -> Result<Option<ArrowOutput>> {
    let chunk = ParquetChunkReader::open(chunk_dir)?;
    execute_chunk_arrow(plan, metadata, &chunk, compress, binary)
}

/// Execute a plan against any ChunkReader implementation.
pub fn execute_chunk(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    profile: bool,
) -> Result<Option<QueryOutput>> {
    match execute(
        plan,
        metadata,
        chunk,
        ExecOptions::profiled(profile),
        OutputFormat::Json,
    )? {
        FmtOutput::Json(blocks) => Ok(blocks.map(|b| *b)),
        FmtOutput::Arrow(_) => unreachable!(),
    }
}

/// Execute a plan against any ChunkReader implementation, producing flat
/// per-table Arrow IPC streams (prototype). See [`crate::output::arrow_out`].
pub fn execute_chunk_arrow(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    compress: bool,
    binary: bool,
) -> Result<Option<ArrowOutput>> {
    match execute(
        plan,
        metadata,
        chunk,
        ExecOptions::default(),
        OutputFormat::Arrow { compress, binary },
    )? {
        FmtOutput::Arrow(output) => Ok(output),
        FmtOutput::Json(_) => unreachable!(),
    }
}

enum FmtOutput {
    Json(Option<Box<QueryOutput>>),
    Arrow(Option<ArrowOutput>),
}

/// The one path from a plan to a response. `format` chooses the encoding;
/// every step before it is shared.
fn execute(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    options: ExecOptions,
    format: OutputFormat,
) -> Result<FmtOutput> {
    let total = Stopwatch::start(options.profile);
    let execution = Execution::prepare(plan, metadata, chunk, options, format)?;

    let blocks = Stopwatch::start(options.profile);
    let Some(page) = select_page(&execution)? else {
        return Ok(match format {
            OutputFormat::Json => FmtOutput::Json(None),
            OutputFormat::Arrow { .. } => FmtOutput::Arrow(None),
        });
    };
    let page = materialize_page(&execution, page)?;

    let header = OutputHeader {
        table: &plan.block_table,
        desc: execution.blocks,
        fields: &plan.block_output_columns,
        batches: page.headers.into_batches(),
    };
    let tables = output_tables(plan, metadata, page.tables);

    match format {
        OutputFormat::Arrow { compress, binary } => {
            let output = arrow_output(&page.blocks, header, tables, compress, binary)?;
            total.report("TOTAL (arrow)");

            Ok(FmtOutput::Arrow(Some(output)))
        }
        OutputFormat::Json => {
            let output = QueryOutput::prepare(page.blocks, header, tables, page.read_through)?;
            blocks.report_with("blocks + indexing", || {
                format!("{} blocks", output.num_blocks())
            });
            total.report("TOTAL (front half; blocks encode lazily)");

            // Blocks are encoded lazily, one per QueryOutput::write_next_block call.
            // See decisions/002: sequential encoding wins at production concurrency.
            Ok(FmtOutput::Json(Some(Box::new(output))))
        }
    }
}

/// Stage timings on stderr under [`ExecOptions::profile`]; nothing otherwise.
#[derive(Clone, Copy)]
struct Stopwatch(Option<Instant>);

impl Stopwatch {
    fn start(profile: bool) -> Self {
        Self(profile.then(Instant::now))
    }

    fn report(self, label: impl Display) {
        if let Some(started) = self.0 {
            eprintln!("  {label}: {:.2?}", started.elapsed());
        }
    }

    /// `detail` is built only when profiling.
    fn report_with(self, label: impl Display, detail: impl FnOnce() -> String) {
        if let Some(started) = self.0 {
            eprintln!("  {label}: {:.2?} ({})", started.elapsed(), detail());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::load_dataset_description;
    use crate::query::compile;
    use crate::query::parse_query;
    use std::collections::HashMap;

    fn to_blocks(blocks: Option<QueryOutput>) -> Vec<serde_json::Value> {
        let mut result = Vec::new();
        if let Some(mut blocks) = blocks {
            let mut buf = Vec::new();
            while blocks.has_next_block() {
                buf.clear();
                blocks.write_next_block(&mut buf);
                result.push(serde_json::from_slice(&buf).unwrap());
            }
        }
        result
    }

    fn solana_metadata() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/solana.yaml")).unwrap()
    }

    fn evm_metadata() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/evm.yaml")).unwrap()
    }

    fn evm_chunk() -> ParquetChunkReader {
        let dir = crate::testing::evm_chunk_path();
        ParquetChunkReader::open(&dir).unwrap()
    }

    /// Parse framed Arrow streams → map of table name → (column names, row count).
    fn read_arrow_frames(framed: &[u8]) -> HashMap<String, (Vec<String>, usize)> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;
        let mut out: HashMap<String, (Vec<String>, usize)> = HashMap::new();
        let mut pos = 0usize;
        while pos + 4 <= framed.len() {
            let nl = u32::from_le_bytes(framed[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let name = String::from_utf8(framed[pos..pos + nl].to_vec()).unwrap();
            pos += nl;
            let pl = u32::from_le_bytes(framed[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let payload = &framed[pos..pos + pl];
            pos += pl;
            let reader = StreamReader::try_new(Cursor::new(payload), None).unwrap();
            let cols: Vec<String> = reader
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect();
            let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
            let e = out.entry(name).or_insert_with(|| (cols.clone(), 0));
            e.1 += rows;
        }
        out
    }

    /// Sum of array lengths per output table across all blocks in the JSON.
    fn json_item_counts(blocks: &[serde_json::Value]) -> HashMap<String, usize> {
        let mut map: HashMap<String, usize> = HashMap::new();
        for b in blocks {
            for (k, val) in b.as_object().unwrap() {
                if k == "header" {
                    continue;
                }
                if let Some(arr) = val.as_array() {
                    *map.entry(k.clone()).or_default() += arr.len();
                }
            }
        }
        map
    }

    /// Covers CT-6 · INV-O7
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_parity_and_projection() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = evm_metadata();
        let q = br#"{
            "type": "evm", "fromBlock": 0,
            "fields": {
                "block": { "number": true, "hash": true },
                "log": { "address": true, "topics": true, "data": true, "logIndex": true }
            },
            "logs": [{ "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"] }]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let chunk = evm_chunk();

        let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, false)
            .unwrap()
            .unwrap()
            .into_data();

        let jcounts = json_item_counts(&json);
        let frames = read_arrow_frames(&arrow);

        // Row-count parity on the item table (weight-trim correctness).
        assert_eq!(
            frames["logs"].1, jcounts["logs"],
            "arrow log rows must equal json log items"
        );
        assert!(frames["logs"].1 > 0, "should have logs");

        // `topics` (virtual Roll) is expanded to physical topic0..3, not dropped.
        let cols = &frames["logs"].0;
        assert!(
            cols.contains(&"topic0".to_string()),
            "topic0 present: {cols:?}"
        );
        assert!(cols.contains(&"topic1".to_string()), "topic1 present");
        assert!(cols.contains(&"address".to_string()));
        assert!(
            cols.contains(&"block_number".to_string()),
            "join key present"
        );
        // Internal scan/weight columns must NOT leak into output.
        assert!(
            !cols.contains(&"data_size".to_string()),
            "no internal data_size: {cols:?}"
        );
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_weight_trim_parity() {
        if !crate::testing::chunks_present() {
            return;
        }

        // Full-scan logs exceed the response budget on this chunk, so the JSON
        // path trims to weight-limited blocks. Flat Arrow must trim identically.
        let meta = evm_metadata();
        let q = br#"{
            "type": "evm", "fromBlock": 0,
            "fields": {
                "block": { "number": true },
                "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true }
            },
            "logs": [{}]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let chunk = evm_chunk();

        let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, false)
            .unwrap()
            .unwrap()
            .into_data();

        let jcounts = json_item_counts(&json);
        let frames = read_arrow_frames(&arrow);
        assert_eq!(
            frames["logs"].1, jcounts["logs"],
            "arrow must apply the same weight-limit row trim as json"
        );
    }

    /// Covers CT-4 · INV-R3
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_multisource_dedup() {
        if !crate::testing::chunks_present() {
            return;
        }

        // Only a table asked directly and pulled by another table's relation has
        // two sources, which Arrow merges on its own path. The relation leaves out
        // the rows the scan matched, so the merge finds nothing to dedup; the
        // counts catch a source it drops. The other shapes come out as one source.
        // A source without rows still sends Arrow down the merge path, while JSON
        // drops it before merging.
        let usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
        let no_topic = format!("0x{}", "00".repeat(32));
        let shapes = [
            (
                "a table and its own relation",
                format!(r#""logs": [{{ "address": ["{usdc}"], "transactionLogs": true }}]"#),
            ),
            (
                "a table asked directly and pulled by another",
                format!(
                    r#""transactions": [{{ "to": ["{usdc}"], "logs": true }}],
                       "logs": [{{ "address": ["{usdc}"] }}]"#
                ),
            ),
            (
                "a table asked directly that matches nothing, pulled by another",
                format!(
                    r#""transactions": [{{ "to": ["{usdc}"], "logs": true }}],
                       "logs": [{{ "topic0": ["{no_topic}"] }}]"#
                ),
            ),
            (
                "two relations into one table",
                format!(
                    r#""traces": [{{ "type": ["call"], "callTo": ["{usdc}"], "transaction": true }}],
                       "stateDiffs": [{{ "address": ["{usdc}"], "transaction": true }}]"#
                ),
            ),
        ];

        let meta = evm_metadata();
        let chunk = evm_chunk();
        let mut wrong = Vec::new();

        for (what, items) in shapes {
            let query = format!(
                r#"{{"type": "evm", "fromBlock": 0, {items},
                    "fields": {{
                        "block": {{ "number": true }},
                        "transaction": {{ "transactionIndex": true, "hash": true }},
                        "log": {{ "logIndex": true, "transactionIndex": true }},
                        "trace": {{ "transactionIndex": true, "traceAddress": true }},
                        "stateDiff": {{ "transactionIndex": true, "address": true, "key": true }}
                    }}}}"#
            );
            let plan = compile(&parse_query(query.as_bytes(), &meta).unwrap(), &meta).unwrap();

            let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
            let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, false)
                .unwrap()
                .unwrap()
                .into_data();

            let json_rows = json_item_counts(&json);
            let arrow_rows: HashMap<String, usize> = read_arrow_frames(&arrow)
                .into_iter()
                .filter(|(name, _)| name != "blocks")
                .map(|(name, (_, rows))| (name, rows))
                .collect();

            if arrow_rows != json_rows {
                wrong.push(format!("{what}: arrow {arrow_rows:?}, json {json_rows:?}"));
            }
        }

        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_solana_base58_and_list() {
        if !crate::testing::chunks_present() {
            return;
        }

        // Solana: base58 columns (not 0x-hex) must stay Utf8 under `binary`, and
        // List<UInt16> instructionAddress must round-trip. Parity with JSON.
        let meta = solana_metadata();
        let q = br#"{
            "type": "solana", "fromBlock": 0,
            "fields": {
                "instruction": { "programId": true, "transactionIndex": true, "instructionAddress": true }
            },
            "instructions": [{ "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"] }]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let dir = crate::testing::solana_chunk_path();
        let chunk = ParquetChunkReader::open(&dir).unwrap();

        let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
        // binary=true must not corrupt base58 (non-0x) columns.
        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, true)
            .unwrap()
            .unwrap()
            .into_data();

        let jcounts = json_item_counts(&json);
        let frames = read_arrow_frames(&arrow);
        assert_eq!(frames["instructions"].1, jcounts["instructions"]);
        assert!(frames["instructions"]
            .0
            .contains(&"instruction_address".to_string()));
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_binary_columns() {
        if !crate::testing::chunks_present() {
            return;
        }

        use arrow::datatypes::DataType;
        let meta = evm_metadata();
        let q = br#"{
            "type": "evm", "fromBlock": 0,
            "fields": {
                "block": { "number": true },
                "log": { "address": true, "topics": true, "data": true, "logIndex": true }
            },
            "logs": [{ "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"] }]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let chunk = evm_chunk();

        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, true)
            .unwrap()
            .unwrap()
            .into_data();

        // Inspect the logs stream schema: hex columns (encoding: hex_bytes) decode
        // to variable Binary, driven by metadata so the type is stable across
        // responses — including all-null columns like topic3 on 3-topic logs.
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;
        let mut pos = 0usize;
        let mut checked = false;
        while pos + 4 <= arrow.len() {
            let nl = u32::from_le_bytes(arrow[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let name = String::from_utf8(arrow[pos..pos + nl].to_vec()).unwrap();
            pos += nl;
            let pl = u32::from_le_bytes(arrow[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let payload = &arrow[pos..pos + pl];
            pos += pl;
            if name == "logs" {
                let reader = StreamReader::try_new(Cursor::new(payload), None).unwrap();
                let schema = reader.schema();
                let addr = schema
                    .field_with_name("address")
                    .unwrap()
                    .data_type()
                    .clone();
                let topic0 = schema
                    .field_with_name("topic0")
                    .unwrap()
                    .data_type()
                    .clone();
                let topic3 = schema
                    .field_with_name("topic3")
                    .unwrap()
                    .data_type()
                    .clone();
                let data = schema.field_with_name("data").unwrap().data_type().clone();
                assert_eq!(addr, DataType::Binary, "address hex → Binary");
                assert_eq!(topic0, DataType::Binary, "topic0 hex → Binary");
                // topic3 is all-null for 3-topic Transfer logs but is still Binary:
                // the type comes from metadata, not from the values present.
                assert_eq!(
                    topic3,
                    DataType::Binary,
                    "all-null topic3 → Binary (stable)"
                );
                assert_eq!(data, DataType::Binary, "data hex → Binary");
                checked = true;
            }
        }
        assert!(checked, "logs stream must be present");
    }

    /// Block `gasLimit` and transaction `value` are minimal-form quantities, so
    /// many have an odd digit count. The binary rendering used to emit `null` for
    /// every one: `gasLimit` in all 224 blocks of this chunk.
    ///
    /// Covers CT-6 · INV-O14
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_binary_quantities_match_json() {
        if !crate::testing::chunks_present() {
            return;
        }

        use arrow::array::{Array, BinaryArray, UInt64Array};
        use arrow::datatypes::DataType;
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;

        let meta = evm_metadata();
        let q = br#"{
            "type": "evm", "fromBlock": 0, "includeAllBlocks": true,
            "fields": {
                "block": { "number": true, "gasLimit": true },
                "transaction": { "transactionIndex": true, "value": true }
            },
            "transactions": [{}]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let chunk = evm_chunk();

        let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, true)
            .unwrap()
            .unwrap()
            .into_data();

        let digits = |text: &str| {
            let d = text.trim_start_matches("0x");
            if d.len() % 2 == 1 {
                format!("0{d}")
            } else {
                d.to_string()
            }
        };
        let mut json_values: HashMap<&str, Vec<(u64, Option<String>)>> = HashMap::new();
        for block in &json {
            let number = block["header"]["number"].as_u64().unwrap();
            let gas_limit = block["header"]["gasLimit"].as_str().map(digits);
            json_values
                .entry("blocks")
                .or_default()
                .push((number, gas_limit));
            for tx in block["transactions"].as_array().into_iter().flatten() {
                let value = tx["value"].as_str().map(digits);
                json_values
                    .entry("transactions")
                    .or_default()
                    .push((number, value));
            }
        }

        let mut arrow_values: HashMap<&str, Vec<(u64, Option<String>)>> = HashMap::new();
        let mut rest = arrow.as_slice();
        while !rest.is_empty() {
            let name_len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            let name = std::str::from_utf8(&rest[4..4 + name_len])
                .unwrap()
                .to_string();
            rest = &rest[4 + name_len..];
            let payload_len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            let payload = &rest[4..4 + payload_len];
            rest = &rest[4 + payload_len..];

            let (table, block_column, column) = match name.as_str() {
                "blocks" => ("blocks", "number", "gas_limit"),
                "transactions" => ("transactions", "block_number", "value"),
                _ => continue,
            };
            for batch in StreamReader::try_new(Cursor::new(payload), None).unwrap() {
                let batch = batch.unwrap();
                let blocks = arrow::compute::cast(
                    batch.column_by_name(block_column).unwrap(),
                    &DataType::UInt64,
                )
                .unwrap();
                let blocks = blocks.as_any().downcast_ref::<UInt64Array>().unwrap();
                let values = batch.column_by_name(column).unwrap();
                let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
                for row in 0..batch.num_rows() {
                    let value = values
                        .is_valid(row)
                        .then(|| faster_hex::hex_string(values.value(row)));
                    arrow_values
                        .entry(table)
                        .or_default()
                        .push((blocks.value(row), value));
                }
            }
        }

        for table in ["blocks", "transactions"] {
            let (mut ours, mut json) = (arrow_values[table].clone(), json_values[table].clone());
            ours.sort();
            json.sort();
            assert!(
                json.iter().all(|(_, v)| v.is_some()),
                "{table}: every value is set"
            );
            assert_eq!(
                ours, json,
                "{table}: the binary rendering must carry the JSON values"
            );
        }
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_execute_solana_instructions() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 0,
            "fields": {
                "block": { "number": true, "hash": true },
                "instruction": { "programId": true, "transactionIndex": true, "instructionAddress": true }
            },
            "instructions": [{
                "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"]
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_dir = crate::testing::solana_chunk_path();
        let blocks = to_blocks(execute_plan(&plan, &meta, &chunk_dir).unwrap());

        assert!(!blocks.is_empty(), "should have at least one block");

        // Each block should have a header
        for block in &blocks {
            assert!(block.get("header").is_some(), "block should have header");
        }

        // At least one block should have instructions
        let has_instructions = blocks.iter().any(|b| b.get("instructions").is_some());
        assert!(has_instructions, "should have instructions in output");

        // Verify instruction fields are camelCase
        for block in &blocks {
            if let Some(instrs) = block.get("instructions") {
                for instr in instrs.as_array().unwrap() {
                    assert!(instr.get("programId").is_some());
                    assert!(instr.get("transactionIndex").is_some());
                    assert!(instr.get("instructionAddress").is_some());
                }
            }
        }
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_execute_evm_logs() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = evm_metadata();
        let json = br#"{
            "type": "evm",
            "fromBlock": 0,
            "fields": {
                "block": { "number": true, "hash": true },
                "log": { "address": true, "topics": true, "data": true, "logIndex": true }
            },
            "logs": [{
                "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_dir = crate::testing::evm_chunk_path();
        let blocks = to_blocks(execute_plan(&plan, &meta, &chunk_dir).unwrap());

        assert!(!blocks.is_empty());

        // Check topics is an array (virtual field via roll)
        for block in &blocks {
            if let Some(logs) = block.get("logs") {
                for log in logs.as_array().unwrap() {
                    if let Some(topics) = log.get("topics") {
                        assert!(topics.is_array(), "topics should be an array");
                        let topics_arr = topics.as_array().unwrap();
                        assert!(!topics_arr.is_empty(), "topics should not be empty");
                        // First topic should be the Transfer event signature
                        let t0 = topics_arr[0].as_str().unwrap();
                        assert!(t0.starts_with("0x"), "topic should be hex");
                    }
                }
            }
        }
    }

    /// Covers CT-4 · INV-R10
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_execute_with_relations() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 0,
            "fields": {
                "instruction": { "programId": true, "transactionIndex": true },
                "transaction": { "transactionIndex": true, "feePayer": true }
            },
            "instructions": [{
                "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
                "transaction": true
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_dir = crate::testing::solana_chunk_path();
        let blocks = to_blocks(execute_plan(&plan, &meta, &chunk_dir).unwrap());

        // Should have both instructions and transactions
        let has_txs = blocks.iter().any(|b| b.get("transactions").is_some());
        assert!(has_txs, "should have related transactions");
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_execute_empty_result() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 999999999,
            "toBlock": 999999999,
            "instructions": [{
                "programId": ["nonexistent_program"]
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_dir = crate::testing::solana_chunk_path();
        let blocks = to_blocks(execute_plan(&plan, &meta, &chunk_dir).unwrap());

        assert!(blocks.is_empty());
    }
}
