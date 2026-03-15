//! Generate result.json fixtures using the legacy sqd-query engine.
//!
//! Usage:
//!   cargo run --bin generate_fixtures --features legacy-query
//!
//! Scans tests/fixtures/{ethereum,solana}/queries/prod_pattern_*/query.json
//! and generates result.json using the legacy engine's ParquetChunk.

use sqd_query::{JsonLinesWriter, ParquetChunk, Query};
use std::path::{Path, PathBuf};

fn run_legacy_query(query_json: &[u8], chunk: &ParquetChunk) -> anyhow::Result<Vec<u8>> {
    let query = Query::from_json_bytes(query_json)?;
    let plan = query.compile();
    let mut writer = JsonLinesWriter::new(Vec::new());
    if let Some(mut blocks) = plan.execute(chunk)? {
        writer.write_blocks(&mut blocks)?;
    }
    Ok(writer.finish()?)
}

fn process_dataset(dataset: &str, chunk_dir: &Path, queries_dir: &Path) {
    if !chunk_dir.exists() {
        eprintln!("  SKIP {}: chunk dir not found at {:?}", dataset, chunk_dir);
        return;
    }

    let chunk = ParquetChunk::new(chunk_dir.to_str().unwrap());

    let mut entries: Vec<_> = std::fs::read_dir(queries_dir)
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.starts_with("prod_pattern_"))
                .unwrap_or(false)
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let query_name = entry.file_name();
        let query_name = query_name.to_str().unwrap();
        let query_file = entry.path().join("query.json");
        let result_file = entry.path().join("result.json");

        if !query_file.exists() {
            continue;
        }

        if result_file.exists() {
            eprintln!(
                "  SKIP {}/{}: result.json already exists",
                dataset, query_name
            );
            continue;
        }

        let query_json = std::fs::read(&query_file).unwrap();

        match run_legacy_query(&query_json, &chunk) {
            Ok(result) => {
                // Legacy outputs JSON lines. Convert to JSON array to match
                // our engine's output format (which e2e tests expect).
                let result_str = String::from_utf8(result).unwrap();
                let blocks: Vec<serde_json::Value> = result_str
                    .lines()
                    .filter(|l| !l.is_empty())
                    .map(|l| serde_json::from_str(l).unwrap())
                    .collect();

                let json_array = serde_json::to_string(&blocks).unwrap();
                std::fs::write(&result_file, json_array.as_bytes()).unwrap();
                eprintln!(
                    "  OK {}/{}: {} blocks, {} bytes",
                    dataset, query_name, blocks.len(), json_array.len()
                );
            }
            Err(e) => {
                eprintln!("  FAIL {}/{}: {}", dataset, query_name, e);
            }
        }
    }
}

fn main() {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");

    eprintln!("=== Generating legacy engine results ===");
    eprintln!();

    eprintln!("Ethereum:");
    process_dataset(
        "ethereum",
        &base.join("ethereum/chunk"),
        &base.join("ethereum/queries"),
    );

    eprintln!();
    eprintln!("Solana:");
    process_dataset(
        "solana",
        &base.join("solana/chunk"),
        &base.join("solana/queries"),
    );

    eprintln!();
    eprintln!("Done!");
}
