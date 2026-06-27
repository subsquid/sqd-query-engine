//! Prototype measurement: nested JSON vs flat Arrow IPC output — full timing.
//!
//! For each query, on the real EVM chunk, reports the end-to-end latency split
//! into producer (build + optional whole-stream zstd) and consumer
//! (decompress + decode) for the shippable variants:
//!   - JSON               (current production format)
//!   - JSON + zstd        (whole-stream, level 3)
//!   - Arrow flat         (per-table IPC streams, Utf8 hex columns)
//!   - Arrow flat + zstd  (whole-stream, level 3)
//!   - Arrow Binary       (hex columns decoded to bytes, engine `binary` flag)
//!   - Arrow Binary + zstd
//!
//! "Decode" is what a consumer pays to get usable data: `serde_json` parse for
//! JSON, vs reading the Arrow batches back (no schema supplied — self-describing).
//!
//! Run: `cargo run --release --example arrow_proto`

#[path = "../benches/queries.rs"]
mod queries;
use queries::*;

use arrow::array::Array;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use serde_json::{json, Value};
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::output::{execute_chunk, execute_chunk_arrow};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::io::Cursor;
use std::path::Path;
use std::time::Instant;

const ITERS: usize = 20;

fn run_json(query: &[u8], meta: &DatasetDescription, chunk: &ParquetChunkReader) -> Vec<u8> {
    let parsed = parse_query(query, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk(&plan, meta, chunk, Vec::new(), false).unwrap()
}

fn run_arrow(
    query: &[u8],
    meta: &DatasetDescription,
    chunk: &ParquetChunkReader,
    compress: bool,
    binary: bool,
) -> Vec<u8> {
    let parsed = parse_query(query, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();
    execute_chunk_arrow(&plan, meta, chunk, Vec::new(), compress, binary).unwrap()
}

/// Minimum wall-clock over ITERS runs, in milliseconds.
fn min_ms<T, F: FnMut() -> T>(mut f: F) -> f64 {
    let mut best = u128::MAX;
    for _ in 0..ITERS {
        let t = Instant::now();
        let out = f();
        std::hint::black_box(&out);
        best = best.min(t.elapsed().as_nanos());
    }
    best as f64 / 1_000_000.0
}

fn zstd3(data: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(data, 3).unwrap()
}

fn unzstd(data: &[u8]) -> Vec<u8> {
    zstd::stream::decode_all(data).unwrap()
}

/// NDJSON block count (one block object per line).
fn json_parse(json: &[u8]) -> usize {
    std::str::from_utf8(json)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .count()
}

/// Parse NDJSON output into a single `Value::Array` of block objects.
fn ndjson_to_value(json: &[u8]) -> serde_json::Value {
    let blocks: Vec<serde_json::Value> = std::str::from_utf8(json)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    serde_json::Value::Array(blocks)
}

/// Read every frame's batches back (no schema input) and total the rows.
fn arrow_read(framed: &[u8]) -> usize {
    let mut pos = 0usize;
    let mut rows = 0usize;
    while pos + 4 <= framed.len() {
        let name_len = u32::from_le_bytes(framed[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + name_len;
        let plen = u32::from_le_bytes(framed[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let payload = &framed[pos..pos + plen];
        pos += plen;
        let reader = StreamReader::try_new(Cursor::new(payload), None).unwrap();
        rows += reader.map(|b| b.unwrap().num_rows()).sum::<usize>();
    }
    rows
}

// --- Consumer-side reconstruction of the nested JSON response from flat Arrow.
// This is the work an existing JSON client would do to get a byte-comparable
// nested shape: decode columns, group by block_number, re-encode. (Column 0 of
// every stream is block_number, since the engine emits it first.)

/// Convert one Arrow cell to a serde_json::Value (mirrors the engine's encoder).
fn av(a: &dyn Array, i: usize) -> Value {
    use arrow::array::*;
    if a.is_null(i) {
        return Value::Null;
    }
    macro_rules! n {
        ($t:ty) => {
            json!(a.as_any().downcast_ref::<$t>().unwrap().value(i))
        };
    }
    match a.data_type() {
        DataType::Boolean => n!(BooleanArray),
        DataType::Int8 => n!(Int8Array),
        DataType::Int16 => n!(Int16Array),
        DataType::Int32 => n!(Int32Array),
        DataType::Int64 => n!(Int64Array),
        DataType::UInt8 => n!(UInt8Array),
        DataType::UInt16 => n!(UInt16Array),
        DataType::UInt32 => n!(UInt32Array),
        DataType::UInt64 => n!(UInt64Array),
        DataType::Float64 => n!(Float64Array),
        DataType::Utf8 => Value::String(a.as_any().downcast_ref::<StringArray>().unwrap().value(i).to_string()),
        DataType::Timestamp(TimeUnit::Millisecond, _) => n!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Second, _) => n!(TimestampSecondArray),
        DataType::Binary => Value::String(format!(
            "0x{}",
            faster_hex::hex_string(a.as_any().downcast_ref::<BinaryArray>().unwrap().value(i))
        )),
        DataType::FixedSizeBinary(_) => Value::String(format!(
            "0x{}",
            faster_hex::hex_string(a.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap().value(i))
        )),
        DataType::List(_) => {
            let la = a.as_any().downcast_ref::<ListArray>().unwrap();
            let sub = la.value(i);
            Value::Array((0..sub.len()).map(|j| av(sub.as_ref(), j)).collect())
        }
        _ => Value::Null,
    }
}

fn block_num_at(a: &dyn Array, i: usize) -> i64 {
    use arrow::array::*;
    match a.data_type() {
        DataType::Int16 => a.as_any().downcast_ref::<Int16Array>().unwrap().value(i) as i64,
        DataType::Int32 => a.as_any().downcast_ref::<Int32Array>().unwrap().value(i) as i64,
        DataType::Int64 => a.as_any().downcast_ref::<Int64Array>().unwrap().value(i),
        DataType::UInt16 => a.as_any().downcast_ref::<UInt16Array>().unwrap().value(i) as i64,
        DataType::UInt32 => a.as_any().downcast_ref::<UInt32Array>().unwrap().value(i) as i64,
        DataType::UInt64 => a.as_any().downcast_ref::<UInt64Array>().unwrap().value(i) as i64,
        _ => 0,
    }
}

/// Read flat frames, group by block, and rebuild the nested `[{header,table:[…]}]`
/// Value — the consumer cost to *decode + reshape* (before final serialization).
fn arrow_to_nested_value(framed: &[u8]) -> Value {
    use std::collections::BTreeMap;
    // Parse frames → (name, batches).
    let mut streams: Vec<(String, Vec<RecordBatch>)> = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= framed.len() {
        let nl = u32::from_le_bytes(framed[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let name = String::from_utf8(framed[pos..pos + nl].to_vec()).unwrap();
        pos += nl;
        let pl = u32::from_le_bytes(framed[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let reader = StreamReader::try_new(Cursor::new(&framed[pos..pos + pl]), None).unwrap();
        pos += pl;
        streams.push((name, reader.map(|b| b.unwrap()).collect()));
    }

    // Build a row object {col: value} for a given batch row.
    let row_obj = |b: &RecordBatch, r: usize| -> Value {
        let mut m = serde_json::Map::new();
        for (c, f) in b.schema().fields().iter().enumerate() {
            m.insert(f.name().clone(), av(b.column(c).as_ref(), r));
        }
        Value::Object(m)
    };

    // Header rows by block number (the stream named "blocks").
    let mut headers: BTreeMap<i64, Value> = BTreeMap::new();
    for (name, batches) in &streams {
        if name == "blocks" {
            for b in batches {
                let bn = b.column(0);
                for r in 0..b.num_rows() {
                    headers.insert(block_num_at(bn.as_ref(), r), row_obj(b, r));
                }
            }
        }
    }

    // Item rows grouped by block number, per table.
    let mut items: BTreeMap<i64, serde_json::Map<String, Value>> = BTreeMap::new();
    for (name, batches) in &streams {
        if name == "blocks" {
            continue;
        }
        for b in batches {
            let bn = b.column(0);
            for r in 0..b.num_rows() {
                let block = block_num_at(bn.as_ref(), r);
                let entry = items.entry(block).or_default();
                entry
                    .entry(name.clone())
                    .or_insert_with(|| Value::Array(Vec::new()))
                    .as_array_mut()
                    .unwrap()
                    .push(row_obj(b, r));
            }
        }
    }

    // Assemble blocks in ascending order.
    let mut blocks: Vec<Value> = Vec::new();
    let all: std::collections::BTreeSet<i64> =
        headers.keys().chain(items.keys()).copied().collect();
    for bn in all {
        let mut obj = serde_json::Map::new();
        if let Some(h) = headers.get(&bn) {
            obj.insert("header".to_string(), h.clone());
        }
        if let Some(tabs) = items.get(&bn) {
            for (k, v) in tabs {
                obj.insert(k.clone(), v.clone());
            }
        }
        blocks.push(Value::Object(obj));
    }
    Value::Array(blocks)
}

// --- Fast direct Arrow→nested-JSON encoder: writes bytes straight from the
// columns (no serde_json::Value tree), the way the engine's own JSON encoder
// does. Assumes hex/base58/number values need no JSON escaping (true here).

fn append_hex(buf: &mut Vec<u8>, v: &[u8]) {
    let start = buf.len();
    buf.resize(start + v.len() * 2, 0);
    faster_hex::hex_encode(v, &mut buf[start..]).unwrap();
}

fn write_json_val(buf: &mut Vec<u8>, a: &dyn Array, row: usize, ib: &mut itoa::Buffer) {
    use arrow::array::*;
    if a.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    macro_rules! int {
        ($t:ty) => {{
            let s = ib.format(a.as_any().downcast_ref::<$t>().unwrap().value(row));
            buf.extend_from_slice(s.as_bytes());
        }};
    }
    match a.data_type() {
        DataType::Boolean => buf.extend_from_slice(
            if a.as_any().downcast_ref::<BooleanArray>().unwrap().value(row) {
                b"true"
            } else {
                b"false"
            },
        ),
        DataType::Int8 => int!(Int8Array),
        DataType::Int16 => int!(Int16Array),
        DataType::Int32 => int!(Int32Array),
        DataType::Int64 => int!(Int64Array),
        DataType::UInt8 => int!(UInt8Array),
        DataType::UInt16 => int!(UInt16Array),
        DataType::UInt32 => int!(UInt32Array),
        DataType::UInt64 => int!(UInt64Array),
        DataType::Timestamp(TimeUnit::Millisecond, _) => int!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Second, _) => int!(TimestampSecondArray),
        DataType::Float64 => {
            let mut rb = ryu::Buffer::new();
            let s = rb.format(a.as_any().downcast_ref::<Float64Array>().unwrap().value(row));
            buf.extend_from_slice(s.as_bytes());
        }
        DataType::Utf8 => {
            let s = a.as_any().downcast_ref::<StringArray>().unwrap().value(row);
            buf.push(b'"');
            buf.extend_from_slice(s.as_bytes());
            buf.push(b'"');
        }
        DataType::Binary => {
            buf.extend_from_slice(b"\"0x");
            append_hex(buf, a.as_any().downcast_ref::<BinaryArray>().unwrap().value(row));
            buf.push(b'"');
        }
        DataType::FixedSizeBinary(_) => {
            buf.extend_from_slice(b"\"0x");
            append_hex(buf, a.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap().value(row));
            buf.push(b'"');
        }
        DataType::List(_) => {
            let la = a.as_any().downcast_ref::<ListArray>().unwrap();
            let sub = la.value(row);
            buf.push(b'[');
            for j in 0..sub.len() {
                if j > 0 {
                    buf.push(b',');
                }
                write_json_val(buf, sub.as_ref(), j, ib);
            }
            buf.push(b']');
        }
        _ => buf.extend_from_slice(b"null"),
    }
}

fn write_json_row(buf: &mut Vec<u8>, b: &RecordBatch, row: usize, ib: &mut itoa::Buffer) {
    buf.push(b'{');
    for (c, f) in b.schema().fields().iter().enumerate() {
        if c > 0 {
            buf.push(b',');
        }
        buf.push(b'"');
        buf.extend_from_slice(f.name().as_bytes());
        buf.extend_from_slice(b"\":");
        write_json_val(buf, b.column(c).as_ref(), row, ib);
    }
    buf.push(b'}');
}

/// Direct Arrow→nested-JSON: group flat frames by block (column 0 = block_number)
/// and write bytes straight from columns. No serde_json::Value.
fn arrow_to_nested_json_fast(framed: &[u8]) -> Vec<u8> {
    use std::collections::{BTreeSet, HashMap};
    // Parse frames.
    let mut streams: Vec<(String, Vec<RecordBatch>)> = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= framed.len() {
        let nl = u32::from_le_bytes(framed[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let name = String::from_utf8(framed[pos..pos + nl].to_vec()).unwrap();
        pos += nl;
        let pl = u32::from_le_bytes(framed[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let reader = StreamReader::try_new(Cursor::new(&framed[pos..pos + pl]), None).unwrap();
        pos += pl;
        streams.push((name, reader.map(|b| b.unwrap()).collect()));
    }

    // Per-stream block index (block_number = column 0).
    let index = |batches: &[RecordBatch]| -> HashMap<i64, Vec<(usize, usize)>> {
        let mut m: HashMap<i64, Vec<(usize, usize)>> = HashMap::new();
        for (bi, b) in batches.iter().enumerate() {
            if b.num_columns() == 0 {
                continue;
            }
            let bn = b.column(0);
            for r in 0..b.num_rows() {
                m.entry(block_num_at(bn.as_ref(), r)).or_default().push((bi, r));
            }
        }
        m
    };

    let mut headers: Option<(&Vec<RecordBatch>, HashMap<i64, Vec<(usize, usize)>>)> = None;
    let mut items: Vec<(&String, &Vec<RecordBatch>, HashMap<i64, Vec<(usize, usize)>>)> = Vec::new();
    for (name, batches) in &streams {
        if name == "blocks" {
            headers = Some((batches, index(batches)));
        } else {
            items.push((name, batches, index(batches)));
        }
    }

    let mut block_order: BTreeSet<i64> = BTreeSet::new();
    if let Some((_, idx)) = &headers {
        block_order.extend(idx.keys());
    }
    for (_, _, idx) in &items {
        block_order.extend(idx.keys());
    }

    let mut buf = Vec::with_capacity(1 << 20);
    let mut ib = itoa::Buffer::new();
    buf.push(b'[');
    for (i, &bn) in block_order.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        buf.push(b'{');
        buf.extend_from_slice(b"\"header\":");
        match headers.as_ref().and_then(|(b, idx)| idx.get(&bn).map(|r| (b, r[0]))) {
            Some((b, (hbi, hri))) => write_json_row(&mut buf, &b[hbi], hri, &mut ib),
            None => buf.extend_from_slice(b"null"),
        }
        for (name, batches, idx) in &items {
            if let Some(rows) = idx.get(&bn) {
                buf.push(b',');
                buf.push(b'"');
                buf.extend_from_slice(name.as_bytes());
                buf.extend_from_slice(b"\":[");
                for (k, &(b, r)) in rows.iter().enumerate() {
                    if k > 0 {
                        buf.push(b',');
                    }
                    write_json_row(&mut buf, &batches[b], r, &mut ib);
                }
                buf.push(b']');
            }
        }
        buf.push(b'}');
    }
    buf.push(b']');
    buf
}

fn mb(bytes: usize) -> String {
    if bytes >= 1_000_000 {
        format!("{:.2} MB", bytes as f64 / 1e6)
    } else {
        format!("{:.0} KB", bytes as f64 / 1e3)
    }
}

fn main() {
    let meta = load_dataset_description(Path::new("metadata/evm.yaml")).unwrap();
    let chunk = ParquetChunkReader::open(Path::new("data/evm/chunk")).unwrap();

    let cases: &[(&str, &[u8])] = &[
        ("usdc_transfers (flat logs)", EVM_USDC_TRANSFERS),
        ("contract_calls+logs (nested)", EVM_CONTRACT_CALLS_WITH_LOGS),
        ("all_logs (full scan)", EVM_ALL_LOGS),
        ("usdc_traces+diffs (heavy)", EVM_USDC_TRACES_AND_STATEDIFFS),
    ];

    for (name, query) in cases {
        let json = run_json(query, &meta, &chunk);
        let arrow = run_arrow(query, &meta, &chunk, false, false);
        let bin = run_arrow(query, &meta, &chunk, false, true);
        let json_z = zstd3(&json);
        let arrow_z = zstd3(&arrow);
        let bin_z = zstd3(&bin);

        // Producer-side timings.
        let t_json_prod = min_ms(|| run_json(query, &meta, &chunk));
        let t_arrow_prod = min_ms(|| run_arrow(query, &meta, &chunk, false, false));
        let t_bin_prod = min_ms(|| run_arrow(query, &meta, &chunk, false, true));
        let t_json_zstd = min_ms(|| zstd3(&json));
        let t_arrow_zstd = min_ms(|| zstd3(&arrow));
        let t_bin_zstd = min_ms(|| zstd3(&bin));

        // Consumer-side timings.
        let t_json_parse = min_ms(|| json_parse(&json));
        let t_arrow_read = min_ms(|| arrow_read(&arrow));
        let t_bin_read = min_ms(|| arrow_read(&bin));
        let t_unz_json = min_ms(|| unzstd(&json_z));
        let t_unz_arrow = min_ms(|| unzstd(&arrow_z));
        let t_unz_bin = min_ms(|| unzstd(&bin_z));

        println!("\n=== {name} ===");
        println!(
            "  {:<22} {:>9}  {:>22}  {:>22}",
            "variant", "size", "producer ms", "consumer ms"
        );
        println!("  {}", "-".repeat(80));
        let row = |label: &str, size: usize, prod: String, cons: String| {
            println!("  {:<22} {:>9}  {:>22}  {:>22}", label, mb(size), prod, cons);
        };
        row(
            "JSON",
            json.len(),
            format!("{:.2}  (build)", t_json_prod),
            format!("{:.2}  (parse)", t_json_parse),
        );
        row(
            "JSON + zstd",
            json_z.len(),
            format!("{:.2}  ({:.2}+{:.2})", t_json_prod + t_json_zstd, t_json_prod, t_json_zstd),
            format!("{:.2}  ({:.2}+{:.2})", t_unz_json + t_json_parse, t_unz_json, t_json_parse),
        );
        row(
            "Arrow flat",
            arrow.len(),
            format!("{:.2}  (build)", t_arrow_prod),
            format!("{:.2}  (read)", t_arrow_read),
        );
        row(
            "Arrow flat + zstd",
            arrow_z.len(),
            format!("{:.2}  ({:.2}+{:.2})", t_arrow_prod + t_arrow_zstd, t_arrow_prod, t_arrow_zstd),
            format!("{:.2}  ({:.2}+{:.2})", t_unz_arrow + t_arrow_read, t_unz_arrow, t_arrow_read),
        );
        row(
            "Arrow Binary",
            bin.len(),
            format!("{:.2}  (build+dec)", t_bin_prod),
            format!("{:.2}  (read)", t_bin_read),
        );
        row(
            "Arrow Binary + zstd",
            bin_z.len(),
            format!("{:.2}  ({:.2}+{:.2})", t_bin_prod + t_bin_zstd, t_bin_prod, t_bin_zstd),
            format!("{:.2}  ({:.2}+{:.2})", t_unz_bin + t_bin_read, t_unz_bin, t_bin_read),
        );

        // Full e2e: producer + transfer @1Gbps + consumer. The RPC-JSON target
        // forces the consumer to decode + re-serialize to JSON in every variant;
        // the last row is Arrow's best case where the consumer keeps columnar
        // data and never re-serializes.
        let json_val: serde_json::Value = ndjson_to_value(&json);
        let arrow_val = arrow_to_nested_value(&arrow);
        let bin_val = arrow_to_nested_value(&bin);

        let t_dec_json = min_ms(|| ndjson_to_value(&json));
        let t_ser_json = min_ms(|| serde_json::to_vec(&json_val).unwrap());
        // Slow path (serde_json::Value tree) vs fast direct byte encoder.
        let t_slow_bin = min_ms(|| serde_json::to_vec(&bin_val).unwrap())
            + min_ms(|| arrow_to_nested_value(&bin));
        let fast = arrow_to_nested_json_fast(&bin);
        assert!(serde_json::from_slice::<serde_json::Value>(&fast).is_ok(), "valid json");
        let t_fast_bin = min_ms(|| arrow_to_nested_json_fast(&bin));
        let _ = (arrow_val, t_slow_bin);

        let tx = |b: usize| b as f64 / 125_000.0; // ms to send `b` bytes at 1 Gbit/s

        println!("\n  Arrow→JSON encode: slow (Value tree) {:.1} ms  vs  fast (direct bytes) {:.1} ms",
            t_slow_bin, t_fast_bin);
        println!("  e2e to RPC-JSON @ 1 Gbit/s  (producer + transfer + consumer):");
        println!(
            "    {:<30} {:>7} {:>7} {:>9} {:>8}",
            "variant", "prod", "xfer", "consumer", "TOTAL"
        );
        let e2e = |label: &str, prod: f64, size: usize, cons: f64| {
            println!(
                "    {:<30} {:>7.1} {:>7.1} {:>9.1} {:>8.1}",
                label,
                prod,
                tx(size),
                cons,
                prod + tx(size) + cons
            );
        };
        e2e(
            "JSON+zstd (gateway reshapes)",
            t_json_prod + t_json_zstd,
            json_z.len(),
            t_unz_json + t_dec_json + t_ser_json,
        );
        e2e(
            "JSON+zstd (forward, no reshape)",
            t_json_prod + t_json_zstd,
            json_z.len(),
            t_unz_json,
        );
        e2e(
            "Arrow Bin+zstd (fast enc→JSON)",
            t_bin_prod + t_bin_zstd,
            bin_z.len(),
            t_unz_bin + t_fast_bin,
        );
        e2e(
            "Arrow Bin+zstd (columnar)",
            t_bin_prod + t_bin_zstd,
            bin_z.len(),
            t_unz_bin + t_bin_read,
        );
    }
    println!();
}
