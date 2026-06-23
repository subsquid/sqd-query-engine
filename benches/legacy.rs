//! Legacy engine (`sqd-query`) adapter for A/B benchmarking.
//!
//! Only compiled under `--features legacy-query`, which pulls in the legacy
//! `sqd-query` crate from `../data/crates/query`. This lets the latency /
//! throughput / profile harnesses run the exact same portal queries through
//! the old engine, on the same parquet chunk, for a direct comparison.
//!
//! The pipeline mirrors `src/bin/generate_fixtures.rs`: parse → compile →
//! execute → JSON-lines encode. Polars uses its global rayon pool internally,
//! so no explicit `POOL.install` is required (execution dispatches to it from
//! any caller thread, exactly as in production).

use sqd_query::{JsonLinesWriter, ParquetChunk, Query};
use std::path::Path;

/// Open a legacy parquet chunk reader for the given chunk directory.
/// `ParquetChunk` lazily memoises per-table readers in a `DashMap`, so it is
/// safe to share `&ParquetChunk` across concurrent worker threads.
pub fn open_chunk(dir: &Path) -> ParquetChunk {
    ParquetChunk::new(dir.to_str().expect("chunk dir must be valid UTF-8"))
}

/// Full legacy pipeline for one query. Returns the encoded JSON-lines output.
pub fn run_query(query_json: &[u8], chunk: &ParquetChunk) -> Vec<u8> {
    let query = Query::from_json_bytes(query_json).expect("legacy query parse");
    let plan = query.compile();
    let mut writer = JsonLinesWriter::new(Vec::new());
    if let Some(mut blocks) = plan.execute(chunk).expect("legacy execute") {
        writer.write_blocks(&mut blocks).expect("legacy write_blocks");
    }
    writer.finish().expect("legacy finish")
}
