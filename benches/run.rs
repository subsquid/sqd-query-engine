// One query through the full pipeline, consumed the way a format's client
// consumes it. Included via `#[path]` from bench binaries.
#![allow(dead_code)]

use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::output::{execute_chunk, execute_chunk_arrow};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ChunkReader;
use std::cell::RefCell;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    /// The whole response as JSON lines in one fresh buffer.
    Json,
    /// One block at a time into a buffer the thread keeps across queries.
    Stream,
    /// Flat per-table Arrow IPC streams, uncompressed, hex columns as text.
    Arrow,
}

impl Format {
    /// `--format json|stream|arrow`; JSON when absent.
    pub fn from_args(args: &[String]) -> Format {
        let value = args
            .iter()
            .position(|a| a == "--format")
            .and_then(|i| args.get(i + 1));
        match value.map(String::as_str) {
            None | Some("json") => Format::Json,
            Some("stream") => Format::Stream,
            Some("arrow") => Format::Arrow,
            Some(other) => {
                eprintln!("unknown --format {other}; expected json, stream or arrow");
                std::process::exit(2);
            }
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Stream => "stream",
            Format::Arrow => "arrow",
        }
    }
}

/// Whether to run the legacy engine beside this one. Legacy writes JSON lines
/// into one fresh buffer and has no other mode, so beside any other format the
/// two columns would measure different work.
pub fn compare_with_legacy(format: Format) -> bool {
    if !cfg!(feature = "legacy-query") {
        return false;
    }

    if format != Format::Json {
        eprintln!("legacy answers only --format json; its columns are left out");
        return false;
    }

    true
}

thread_local! {
    static STREAM_BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Parse, compile, execute and consume one query; returns the response bytes.
pub fn run_query(
    query_json: &[u8],
    meta: &DatasetDescription,
    chunk: &dyn ChunkReader,
    format: Format,
) -> usize {
    let parsed = parse_query(query_json, meta).unwrap();
    let plan = compile(&parsed, meta).unwrap();

    match format {
        Format::Json => {
            let output = execute_chunk(&plan, meta, chunk, false).unwrap();
            let bytes = output.map(|o| o.into_json_lines()).unwrap_or_default();
            std::hint::black_box(&bytes);
            bytes.len()
        }
        Format::Stream => {
            let Some(mut output) = execute_chunk(&plan, meta, chunk, false).unwrap() else {
                return 0;
            };
            STREAM_BUFFER.with_borrow_mut(|buffer| {
                let mut total = 0;
                while output.has_next_block() {
                    buffer.clear();
                    output.write_next_block(buffer);
                    std::hint::black_box(&buffer);
                    total += buffer.len();
                }
                total
            })
        }
        Format::Arrow => {
            let output = execute_chunk_arrow(&plan, meta, chunk, false, false).unwrap();
            let bytes = output.map(|o| o.into_data()).unwrap_or_default();
            std::hint::black_box(&bytes);
            bytes.len()
        }
    }
}
