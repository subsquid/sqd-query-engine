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
#[path = "../benches/rpc_workload.rs"]
mod rpc_workload;
use queries::*;

use arrow::ipc::reader::StreamReader;
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
    execute_chunk(&plan, meta, chunk, false)
        .unwrap()
        .map(|blocks| blocks.into_json_lines())
        .unwrap_or_default()
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
    execute_chunk_arrow(&plan, meta, chunk, compress, binary)
        .unwrap()
        .map(|output| output.into_data())
        .unwrap_or_default()
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

fn json_parse(json: &[u8]) -> usize {
    let blocks: Vec<serde_json::Value> = std::str::from_utf8(json)
        .unwrap()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    blocks.len()
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

fn mb(bytes: usize) -> String {
    if bytes >= 1_000_000 {
        format!("{:.2} MB", bytes as f64 / 1e6)
    } else {
        format!("{:.0} KB", bytes as f64 / 1e3)
    }
}

fn main() {
    let meta = load_dataset_description(Path::new("metadata/evm.yaml")).unwrap();
    // Chunk is overridable so this can run on the bundled fixture (default,
    // deterministic — the RPC queries below are block-pinned to it) or any other
    // EVM chunk via `ARROW_PROTO_CHUNK=/path`.
    let chunk_dir = std::env::var("ARROW_PROTO_CHUNK").unwrap_or_else(|_| "data/evm/chunk".into());
    let chunk = ParquetChunkReader::open(Path::new(&chunk_dir)).unwrap();

    // On a REAL chunk (`ARROW_PROTO_CHUNK` set): RPC queries bound to that chunk's
    // own blocks / hot keys via `rpc_workload` (one mid-chunk probe block per method), so
    // the e2e numbers land on real response sizes — the same data the throughput
    // bench uses. On the bundled fixture: the static indexer + block-pinned RPC set.
    let real = std::env::var("ARROW_PROTO_CHUNK").is_ok();
    let mut cases: Vec<(String, Vec<u8>)> = Vec::new();
    if real {
        let info = rpc_workload::analyze_chunk(Path::new(&chunk_dir));
        for (name, variants) in rpc_workload::gen_variants(std::slice::from_ref(&info), 1) {
            if let Some((_, q)) = variants.into_iter().next() {
                cases.push((name, q));
            }
        }
    } else {
        for (n, q) in [
            ("usdc_transfers (flat logs)", EVM_USDC_TRANSFERS),
            ("contract_calls+logs (nested)", EVM_CONTRACT_CALLS_WITH_LOGS),
            ("all_logs (full scan)", EVM_ALL_LOGS),
            ("usdc_traces+diffs (heavy)", EVM_USDC_TRACES_AND_STATEDIFFS),
        ] {
            cases.push((n.to_string(), q.to_vec()));
        }
        for (n, q) in EVM_RPC_QUERIES {
            cases.push((n.to_string(), q.to_vec()));
        }
    }

    for (name, query) in &cases {
        let query: &[u8] = query;
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
            println!(
                "  {:<22} {:>9}  {:>22}  {:>22}",
                label,
                mb(size),
                prod,
                cons
            );
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
            format!(
                "{:.2}  ({:.2}+{:.2})",
                t_json_prod + t_json_zstd,
                t_json_prod,
                t_json_zstd
            ),
            format!(
                "{:.2}  ({:.2}+{:.2})",
                t_unz_json + t_json_parse,
                t_unz_json,
                t_json_parse
            ),
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
            format!(
                "{:.2}  ({:.2}+{:.2})",
                t_arrow_prod + t_arrow_zstd,
                t_arrow_prod,
                t_arrow_zstd
            ),
            format!(
                "{:.2}  ({:.2}+{:.2})",
                t_unz_arrow + t_arrow_read,
                t_unz_arrow,
                t_arrow_read
            ),
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
            format!(
                "{:.2}  ({:.2}+{:.2})",
                t_bin_prod + t_bin_zstd,
                t_bin_prod,
                t_bin_zstd
            ),
            format!(
                "{:.2}  ({:.2}+{:.2})",
                t_unz_bin + t_bin_read,
                t_unz_bin,
                t_bin_read
            ),
        );
    }
    println!();
}
