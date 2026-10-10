//! Both engines behind one call shape, so a test can put the same query to each.
//! CT-7 and the real-chunk cases (`tests/cases.rs`) share this file.

use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::output::execute_plan;
use sqd_query_engine::query::{compile, parse_query};
use std::path::Path;

/// This engine's NDJSON answer, or its error.
pub fn run_new(
    query: &[u8],
    metadata: &DatasetDescription,
    chunk: &Path,
) -> Result<Vec<u8>, String> {
    let parsed = parse_query(query, metadata).map_err(|e| format!("{e:#}"))?;
    let plan = compile(&parsed, metadata).map_err(|e| format!("{e:#}"))?;
    Ok(execute_plan(&plan, metadata, chunk)
        .map_err(|e| format!("{e:#}"))?
        .map(|out| out.into_json_lines())
        .unwrap_or_default())
}

/// The reference engine's NDJSON answer, or its error.
#[cfg(feature = "legacy-query")]
pub fn run_legacy(query: &[u8], chunk: &Path) -> Result<Vec<u8>, String> {
    let chunk = sqd_query::ParquetChunk::new(chunk.to_string_lossy().into_owned());
    let query = sqd_query::Query::from_json_bytes(query).map_err(|e| format!("{e:#}"))?;
    let mut writer = sqd_query::JsonLinesWriter::new(Vec::new());
    match query.compile().execute(&chunk) {
        Ok(Some(mut blocks)) => writer
            .write_blocks(&mut blocks)
            .map_err(|e| format!("{e:#}"))?,
        Ok(None) => {}
        Err(e) => return Err(format!("{e:#}")),
    }
    writer.finish().map_err(|e| format!("{e:#}"))
}

/// NDJSON to a comparable value. The two engines order object keys differently
/// and escape differently, so the comparison is over parsed values, never bytes.
pub fn as_blocks(body: &[u8]) -> Vec<serde_json::Value> {
    body.split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}
