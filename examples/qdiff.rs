//! Run the same JSON query through the legacy (sqd-query) and new
//! (sqd-query-engine) engines against the same parquet chunk and diff outputs.
//!
//! Usage: qdiff <metadata.yaml> <chunk_dir> <query.json>

#[cfg(not(feature = "legacy-query"))]
fn main() {
    eprintln!("rebuild with --features legacy-query");
}

#[cfg(feature = "legacy-query")]
use std::path::Path;

#[cfg(feature = "legacy-query")]
fn run_legacy(query_json: &[u8], chunk_dir: &str) -> Result<serde_json::Value, String> {
    let chunk = sqd_query::ParquetChunk::new(chunk_dir);
    let query = sqd_query::Query::from_json_bytes(query_json).map_err(|e| format!("{e:#}"))?;
    let plan = query.compile();
    let mut writer = sqd_query::JsonLinesWriter::new(Vec::new());
    match plan.execute(&chunk) {
        Ok(Some(mut blocks)) => writer
            .write_blocks(&mut blocks)
            .map_err(|e| format!("{e:#}"))?,
        Ok(None) => {}
        Err(e) => return Err(format!("{e:#}")),
    }
    let bytes = writer.finish().map_err(|e| format!("{e:#}"))?;
    let text = String::from_utf8(bytes).map_err(|e| format!("{e:#}"))?;
    let blocks: Result<Vec<serde_json::Value>, _> = text
        .lines()
        .filter(|l| !l.is_empty())
        .map(serde_json::from_str)
        .collect();
    Ok(serde_json::Value::Array(blocks.map_err(|e| format!("{e:#}"))?))
}

#[cfg(feature = "legacy-query")]
fn run_new(query_json: &[u8], meta_path: &Path, chunk_dir: &Path) -> Result<serde_json::Value, String> {
    let meta = sqd_query_engine::metadata::load_dataset_description(meta_path)
        .map_err(|e| format!("{e:#}"))?;
    let parsed =
        sqd_query_engine::query::parse_query(query_json, &meta).map_err(|e| format!("{e:#}"))?;
    let plan = sqd_query_engine::query::compile(&parsed, &meta).map_err(|e| format!("{e:#}"))?;
    let mut result = Vec::new();
    if let Some(mut blocks) = sqd_query_engine::output::execute_plan(&plan, &meta, chunk_dir)
        .map_err(|e| format!("{e:#}"))?
    {
        let mut buf = Vec::new();
        while blocks.has_next_block() {
            buf.clear();
            blocks.write_next_block(&mut buf);
            result.push(serde_json::from_slice(&buf).map_err(|e| format!("{e:#}"))?);
        }
    }
    Ok(serde_json::Value::Array(result))
}

#[cfg(feature = "legacy-query")]
fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: qdiff <metadata.yaml> <chunk_dir> <query.json>");
        std::process::exit(2);
    }
    let meta_path = Path::new(&args[1]);
    let chunk_dir = Path::new(&args[2]);
    let query_json = std::fs::read(&args[3]).expect("read query file");

    let legacy = run_legacy(&query_json, chunk_dir.to_str().unwrap());
    let new = std::panic::catch_unwind(|| run_new(&query_json, meta_path, chunk_dir))
        .unwrap_or_else(|e| {
            let msg = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "unknown panic".into());
            Err(format!("PANIC: {msg}"))
        });

    match (&legacy, &new) {
        (Ok(a), Ok(b)) if a == b => {
            println!("MATCH ({} blocks)", a.as_array().map(|v| v.len()).unwrap_or(0));
        }
        _ => {
            println!("=== DIFFER ===");
            match &legacy {
                Ok(v) => println!("--- legacy ---\n{}", serde_json::to_string_pretty(v).unwrap()),
                Err(e) => println!("--- legacy ERROR ---\n{e}"),
            }
            match &new {
                Ok(v) => println!("--- new ---\n{}", serde_json::to_string_pretty(v).unwrap()),
                Err(e) => println!("--- new ERROR ---\n{e}"),
            }
            std::process::exit(1);
        }
    }
}
