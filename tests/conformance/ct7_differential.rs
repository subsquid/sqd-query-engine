//! CT-7 — differential testing against the reference implementation.
//!
//! The fixture suite compares against recorded answers, so it only ever asks the
//! questions someone thought to record. This one builds its questions out of the
//! catalog instead: every table, every filter it declares, every relation, with
//! values sampled from the chunk itself. Both engines answer, and the answers
//! must agree.
//!
//! Requires the reference implementation:
//!
//! ```text
//! cargo test --test conformance --features legacy-query ct7_ -- --nocapture
//! ```

use sqd_query_engine::metadata::{
    load_dataset_description, DatasetDescription, TableDescription, VirtualField, WeightSource,
};
use sqd_query_engine::output::{execute_chunk_with, execute_plan, ExecOptions};
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::ParquetChunkReader;
use std::collections::{BTreeSet, HashSet};
use std::path::Path;

use crate::harness::fixtures::fixture_chunk;

/// (catalog, fixture directory). Only datasets both engines serve.
const DATASETS: &[(&str, &str)] = &[
    ("evm", "ethereum"),
    ("evm", "optimism"),
    ("solana", "solana"),
    ("substrate", "kusama"),
    ("substrate", "moonbeam"),
    ("bitcoin", "bitcoin"),
    ("tron", "tron"),
];

/// How many blocks each generated query covers. Wide enough that filters match
/// something, narrow enough that a few hundred queries finish.
const BLOCK_SPAN: u64 = 40;

/// How many sampled values a generated IN-list carries.
const VALUES_PER_FILTER: usize = 3;

fn snake_to_camel(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut upper = false;
    for c in s.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

fn run_new(query: &[u8], metadata: &DatasetDescription, chunk: &Path) -> Result<Vec<u8>, String> {
    let parsed = parse_query(query, metadata).map_err(|e| format!("{e:#}"))?;
    let plan = compile(&parsed, metadata).map_err(|e| format!("{e:#}"))?;
    Ok(execute_plan(&plan, metadata, chunk)
        .map_err(|e| format!("{e:#}"))?
        .map(|out| out.into_json_lines())
        .unwrap_or_default())
}

fn run_legacy(query: &[u8], chunk: &Path) -> Result<Vec<u8>, String> {
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
fn as_blocks(body: &[u8]) -> Vec<serde_json::Value> {
    body.split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

/// The first block the chunk holds, so generated ranges land on real data.
fn first_block(metadata: &DatasetDescription, chunk: &Path) -> Option<u64> {
    let query = format!(
        r#"{{"type":"{}","fromBlock":0,"includeAllBlocks":true,
             "fields":{{"block":{{"number":true}}}}}}"#,
        metadata.name
    );
    let body = run_new(query.as_bytes(), metadata, chunk).ok()?;
    as_blocks(&body)
        .first()
        .and_then(|b| b["header"]["number"].as_u64())
}

/// Sample real values of one filter column by asking for the column itself.
/// A filter built from invented values matches nothing and compares nothing.
fn sample_values(
    metadata: &DatasetDescription,
    chunk: &Path,
    query_name: &str,
    field_name: &str,
    column: &str,
    from: u64,
    to: u64,
) -> Vec<serde_json::Value> {
    let field = snake_to_camel(column);
    let query = format!(
        r#"{{"type":"{}","fromBlock":{from},"toBlock":{to},
             "{query_name}":[{{}}],
             "fields":{{"{field_name}":{{"{field}":true}}}}}}"#,
        metadata.name
    );

    let Ok(body) = run_new(query.as_bytes(), metadata, chunk) else {
        return Vec::new();
    };

    let mut seen = BTreeSet::new();
    let mut values = Vec::new();
    for block in as_blocks(&body) {
        for item in block
            .get(query_name)
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
        {
            let Some(value) = item.get(&field) else {
                continue;
            };
            if value.is_null() || value.is_object() || value.is_array() {
                continue;
            }
            if seen.insert(value.to_string()) {
                values.push(value.clone());
            }
            if values.len() == VALUES_PER_FILTER {
                return values;
            }
        }
    }
    values
}

/// One generated question: a label for the failure message and the query itself.
struct Probe {
    what: String,
    query: String,
}

fn probes_for(metadata: &DatasetDescription, chunk: &Path, from: u64, to: u64) -> Vec<Probe> {
    let mut probes = Vec::new();

    for (table_name, table) in &metadata.tables {
        let Some(query_name) = table.request().name.as_deref() else {
            continue;
        };
        let Some(field_name) = table.output.name.as_deref() else {
            continue;
        };

        // The bare item request, and every relation the table declares.
        let mut item_shapes = vec![("plain".to_string(), String::new())];
        for relation in table.request().relations.keys() {
            item_shapes.push((
                format!("relation {relation}"),
                format!(r#""{}":true"#, snake_to_camel(relation)),
            ));
        }
        if table.request().relations.len() > 1 {
            let all: Vec<String> = table
                .request()
                .relations
                .keys()
                .map(|r| format!(r#""{}":true"#, snake_to_camel(r)))
                .collect();
            item_shapes.push(("every relation at once".to_string(), all.join(",")));
        }

        for (shape_label, relations) in &item_shapes {
            let comma = if relations.is_empty() { "" } else { "," };
            probes.push(Probe {
                what: format!("{table_name}: no filter, {shape_label}"),
                query: format!(
                    r#"{{"type":"{}","fromBlock":{from},"toBlock":{to},
                         "{query_name}":[{{{relations}}}]}}"#,
                    metadata.name
                ),
            });

            // A special filter takes a value shape of its own; the plain columns
            // are the ones a sampled value can drive.
            for column in &table.request().filters {
                if table.request().special_filters.contains_key(column) {
                    continue;
                }
                let values =
                    sample_values(metadata, chunk, query_name, field_name, column, from, to);
                if values.is_empty() {
                    continue;
                }
                let list = serde_json::Value::Array(values.clone()).to_string();
                let key = snake_to_camel(column);

                probes.push(Probe {
                    what: format!(
                        "{table_name}.{column}: {} values, {shape_label}",
                        values.len()
                    ),
                    query: format!(
                        r#"{{"type":"{}","fromBlock":{from},"toBlock":{to},
                             "{query_name}":[{{"{key}":{list}{comma}{relations}}}]}}"#,
                        metadata.name
                    ),
                });

                // Two items over the same table are alternatives, and the union
                // is where duplicate rows show up.
                let single = serde_json::Value::Array(values[..1].to_vec()).to_string();
                probes.push(Probe {
                    what: format!("{table_name}.{column}: two alternative items, {shape_label}"),
                    query: format!(
                        r#"{{"type":"{}","fromBlock":{from},"toBlock":{to},
                             "{query_name}":[{{"{key}":{single}{comma}{relations}}},
                                             {{"{key}":{list}}}]}}"#,
                        metadata.name
                    ),
                });
            }
        }
    }

    probes
}

#[test]
fn every_catalog_filter_answers_the_same_as_the_reference() {
    let mut checked = 0usize;
    let mut mismatches: Vec<String> = Vec::new();

    for (catalog, dataset) in DATASETS {
        let chunk = fixture_chunk(dataset);
        if !chunk.is_dir() {
            continue;
        }
        let metadata =
            load_dataset_description(Path::new(&format!("metadata/{catalog}.yaml"))).unwrap();
        let Some(first) = first_block(&metadata, &chunk) else {
            continue;
        };
        let (from, to) = (first, first + BLOCK_SPAN);

        for probe in probes_for(&metadata, &chunk, from, to) {
            let ours = run_new(probe.query.as_bytes(), &metadata, &chunk);
            let theirs = run_legacy(probe.query.as_bytes(), &chunk);
            checked += 1;

            match (ours, theirs) {
                (Ok(ours), Ok(theirs)) => {
                    let (ours, theirs) = (as_blocks(&ours), as_blocks(&theirs));
                    if ours != theirs {
                        mismatches.push(format!(
                            "{dataset}/{}: {} blocks vs {} from the reference\n      {}",
                            probe.what,
                            ours.len(),
                            theirs.len(),
                            probe.query.split_whitespace().collect::<Vec<_>>().join(" "),
                        ));
                    }
                }
                // We accept a superset of the reference's request surface on
                // purpose, so answering where it refuses is not a mismatch.
                (Ok(_), Err(_)) => {}
                (Err(ours), Ok(_)) => mismatches.push(format!(
                    "{dataset}/{}: we refuse a request the reference answers: {ours}",
                    probe.what
                )),
                (Err(_), Err(_)) => {}
            }
        }
    }

    assert!(checked > 200, "only {checked} probes were generated");
    assert!(
        mismatches.is_empty(),
        "{} of {checked} generated queries disagree with the reference:\n  - {}",
        mismatches.len(),
        mismatches.join("\n  - ")
    );
    eprintln!("{checked} generated queries agree with the reference");
}

/// How many `fromBlock` values the fork probe samples from each chunk, besides
/// the ones next to a skipped number. Every number of every fixture chunk agreed
/// when this was written — 43 936 probes — but that run takes eight minutes.
const FORK_PROBES_PER_CHUNK: u64 = 100;

/// Block numbers a chunk covers, the skipped ones included, as a `fromBlock`
/// with a `parentBlockHash`: the hash the chunk holds for the parent, and one it
/// does not. Both engines must serve the first and refuse the second with the
/// same message, which names the block they compared against.
///
/// The range stops at the chunk's ends. Past them the two disagree on purpose
/// (the divergence table in GAPS.md).
///
/// Covers CT-7 · INV-E5
#[test]
fn the_fork_check_answers_the_same_as_the_reference() {
    let mut checked = 0usize;
    let mut skipped_numbers = 0usize;
    let mut mismatches: Vec<String> = Vec::new();

    for (catalog, dataset) in DATASETS {
        let chunk = fixture_chunk(dataset);
        if !chunk.is_dir() {
            continue;
        }
        let metadata =
            load_dataset_description(Path::new(&format!("metadata/{catalog}.yaml"))).unwrap();

        let all_blocks = format!(
            r#"{{"type":"{}","fromBlock":0,"includeAllBlocks":true,
                 "fields":{{"block":{{"number":true,"parentHash":true}}}}}}"#,
            metadata.name
        );
        let blocks: Vec<(u64, String)> =
            as_blocks(&run_new(all_blocks.as_bytes(), &metadata, &chunk).unwrap())
                .iter()
                .map(|b| {
                    let header = &b["header"];
                    let number = header["number"].as_u64().unwrap();
                    let parent_hash = header["parentHash"].as_str().unwrap().to_string();
                    (number, parent_hash)
                })
                .collect();
        let (first, last) = (blocks[0].0, blocks.last().unwrap().0);
        skipped_numbers += (last - first + 1) as usize - blocks.len();

        // A stride through the chunk, and every number within two of a gap: the
        // numbers where the block after `fromBlock` is not the one at it.
        let stride = ((last - first) / FORK_PROBES_PER_CHUNK).max(1);
        let near_a_gap = |from: u64| {
            blocks.windows(2).any(|pair| {
                let gap = pair[0].0 + 1..pair[1].0;
                !gap.is_empty() && from + 2 >= gap.start && from <= gap.end + 2
            })
        };
        let froms = (first..=last).filter(|from| (from - first) % stride == 0 || near_a_gap(*from));

        let mut next = 0;
        for from in froms {
            // The first block at or after `fromBlock` states the parent's hash.
            while blocks[next].0 < from {
                next += 1;
            }
            let true_parent = &blocks[next].1;

            for parent in [true_parent.as_str(), "0xnot-the-parent"] {
                let query = format!(
                    r#"{{"type":"{}","fromBlock":{from},"toBlock":{from},"includeAllBlocks":true,
                         "parentBlockHash":"{parent}",
                         "fields":{{"block":{{"number":true}}}}}}"#,
                    metadata.name
                );
                let ours = run_new(query.as_bytes(), &metadata, &chunk);
                let theirs = run_legacy(query.as_bytes(), &chunk);
                checked += 1;

                let agree = match (&ours, &theirs) {
                    (Ok(ours), Ok(theirs)) => as_blocks(ours) == as_blocks(theirs),
                    (Err(ours), Err(theirs)) => ours == theirs,
                    _ => false,
                };
                if !agree {
                    mismatches.push(format!(
                        "{dataset} fromBlock {from}, parentBlockHash {parent}:\n      ours: {ours:?}\n    theirs: {theirs:?}"
                    ));
                }
            }
        }
    }

    assert!(checked > 500, "only {checked} fork probes ran");
    assert!(skipped_numbers > 0, "no chunk skipped a block number");
    assert!(
        mismatches.is_empty(),
        "{} of {checked} fork probes disagree with the reference:\n  - {}",
        mismatches.len(),
        mismatches
            .iter()
            .take(20)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n  - ")
    );
    eprintln!("{checked} fork probes agree with the reference ({skipped_numbers} skipped numbers)");
}

/// How many `fromBlock` values each page shape starts from, spread over the
/// chunk. A page checks one running sum of block weights, the one at its cut, so
/// one start per shape would leave most blocks of the chunk unweighed.
const PAGE_STARTS: u64 = 6;

/// The same for the shapes selecting every field, which read the most.
const WIDE_PAGE_STARTS: u64 = 2;

/// The last block of the page each engine returns, at its default budget.
fn page_end_new(
    query: &str,
    metadata: &DatasetDescription,
    chunk: &Path,
) -> Result<Option<u64>, String> {
    let parsed = parse_query(query.as_bytes(), metadata).map_err(|e| format!("{e:#}"))?;
    let plan = compile(&parsed, metadata).map_err(|e| format!("{e:#}"))?;
    let reader = ParquetChunkReader::open(chunk).map_err(|e| format!("{e:#}"))?;
    let page = execute_chunk_with(&plan, metadata, &reader, ExecOptions::default())
        .map_err(|e| format!("{e:#}"))?;

    Ok(page.map(|page| page.last_block()))
}

fn page_end_legacy(query: &str, chunk: &Path) -> Result<Option<u64>, String> {
    let chunk = sqd_query::ParquetChunk::new(chunk.to_string_lossy().into_owned());
    let query =
        sqd_query::Query::from_json_bytes(query.as_bytes()).map_err(|e| format!("{e:#}"))?;
    let page = query
        .compile()
        .execute(&chunk)
        .map_err(|e| format!("{e:#}"))?;

    Ok(page.map(|page| page.last_block()))
}

/// The fields of `table` whose columns the chunk stores, with the size columns
/// that weigh them. A fixture chunk predates some of the catalog's fields, and a
/// request for one is refused by both engines, which compares nothing.
fn stored_fields(table: &TableDescription, name: &str, chunk: &Path) -> Vec<String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let Ok(file) = std::fs::File::open(chunk.join(format!("{name}.parquet"))) else {
        return Vec::new();
    };
    let reader = SerializedFileReader::new(file).unwrap();
    let stored: HashSet<String> = reader
        .metadata()
        .file_metadata()
        .schema()
        .get_fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();

    let is_stored = |column: &String| {
        let size = match table.column(column).and_then(|c| c.weight.as_ref()) {
            Some(WeightSource::Column(size)) => Some(size),
            _ => None,
        };
        stored.contains(column) && size.is_none_or(|size| stored.contains(size))
    };

    table
        .output
        .fields
        .iter()
        .filter(|field| {
            let columns = match table.output.virtual_fields.get(field.as_str()) {
                Some(VirtualField::Roll { columns }) => columns.clone(),
                None => table
                    .physical_output_column(field)
                    .map(|column| vec![column.to_string()])
                    .unwrap_or_default(),
            };
            !columns.is_empty() && columns.iter().all(is_stored)
        })
        .cloned()
        .collect()
}

fn selection(fields: &[String]) -> String {
    let fields: Vec<String> = fields
        .iter()
        .map(|field| format!(r#""{}":true"#, snake_to_camel(field)))
        .collect();
    format!("{{{}}}", fields.join(","))
}

/// Page shapes over one chunk, each with how many starts it gets. `FROM` stands
/// for the start.
fn page_shapes(metadata: &DatasetDescription, chunk: &Path) -> Vec<(String, String, u64)> {
    let ty = &metadata.name;
    let (block_key, block) = metadata
        .tables
        .iter()
        .find(|(_, table)| table.is_block_table())
        .unwrap();
    let headers = selection(&stored_fields(block, block_key, chunk));
    let block_output = block.output.name.as_deref().unwrap_or("block");

    let mut shapes = vec![(
        "bare headers".to_string(),
        format!(r#"{{"type":"{ty}","fromBlock":FROM,"includeAllBlocks":true,"fields":{{}}}}"#),
        PAGE_STARTS,
    )];

    for (name, table) in &metadata.tables {
        let (Some(request), Some(output)) = (table.request().name.as_deref(), &table.output.name)
        else {
            continue;
        };
        if !chunk.join(format!("{name}.parquet")).is_file() {
            continue;
        }
        let relations: Vec<String> = table
            .request()
            .relations
            .keys()
            .map(|r| format!(r#""{}":true"#, snake_to_camel(r)))
            .collect();
        let fields = selection(&stored_fields(table, name, chunk));

        shapes.push((
            format!("{name}: no field"),
            format!(r#"{{"type":"{ty}","fromBlock":FROM,"{request}":[{{}}],"fields":{{}}}}"#),
            PAGE_STARTS,
        ));
        shapes.push((
            format!("{name}: no field, every block"),
            format!(
                r#"{{"type":"{ty}","fromBlock":FROM,"includeAllBlocks":true,
                     "{request}":[{{}}],"fields":{{}}}}"#
            ),
            PAGE_STARTS,
        ));
        if !relations.is_empty() {
            shapes.push((
                format!("{name}: no field, every relation"),
                format!(
                    r#"{{"type":"{ty}","fromBlock":FROM,"{request}":[{{{}}}],"fields":{{}}}}"#,
                    relations.join(",")
                ),
                PAGE_STARTS,
            ));
        }
        shapes.push((
            format!("{name}: every field"),
            format!(
                r#"{{"type":"{ty}","fromBlock":FROM,"{request}":[{{}}],
                     "fields":{{"{output}":{fields},"{block_output}":{headers}}}}}"#
            ),
            WIDE_PAGE_STARTS,
        ));
    }

    shapes
}

/// Each engine ends a page where its weight model says the budget runs out, so
/// a page that ends at another block than the reference's is a weight model
/// that charges something else for some block before the cut.
///
/// The shapes select no field, where the weight key is all a row is charged
/// for, and every field the chunk stores, where the projection is; each starts
/// from several blocks of every fixture chunk, so the boundary blocks are not
/// always the chunk's own.
///
/// Covers CT-7 · INV-B5
#[test]
fn every_page_ends_where_the_reference_ends() {
    let mut checked = 0usize;
    let mut cut = 0usize;
    let mut mismatches: Vec<String> = Vec::new();

    for (catalog, dataset) in DATASETS {
        let chunk = fixture_chunk(dataset);
        if !chunk.is_dir() {
            continue;
        }
        let metadata =
            load_dataset_description(Path::new(&format!("metadata/{catalog}.yaml"))).unwrap();
        let Some(first) = first_block(&metadata, &chunk) else {
            continue;
        };
        let all_blocks = format!(
            r#"{{"type":"{}","fromBlock":0,"includeAllBlocks":true,"fields":{{}}}}"#,
            metadata.name
        );
        let last = page_end_new(&all_blocks, &metadata, &chunk)
            .unwrap()
            .unwrap();

        for (shape, query, starts) in page_shapes(&metadata, &chunk) {
            for start in 0..starts {
                let from = first + start * (last - first) / starts;
                let query = query.replace("FROM", &from.to_string());

                let ours = page_end_new(&query, &metadata, &chunk);
                let theirs = page_end_legacy(&query, &chunk);

                match (ours, theirs) {
                    (Ok(ours), Ok(theirs)) => {
                        checked += 1;
                        cut += usize::from(theirs.is_some_and(|end| end < last));
                        if ours != theirs {
                            mismatches.push(format!(
                                "{dataset}/{shape} from {from}: the page ends at {ours:?}, \
                                 the reference's at {theirs:?}"
                            ));
                        }
                    }
                    // A request the reference does not serve compares nothing.
                    (Ok(_), Err(_)) => {}
                    (Err(ours), _) => {
                        mismatches.push(format!("{dataset}/{shape} from {from}: {ours}"))
                    }
                }
            }
        }
    }

    assert!(checked > 400, "only {checked} pages were compared");
    assert!(
        cut > 50,
        "only {cut} of {checked} pages ended before their chunk did, so the rest \
         compared two whole chunks and said nothing about where a budget runs out"
    );
    assert!(
        mismatches.is_empty(),
        "{} of {checked} pages end elsewhere than the reference's:\n  - {}",
        mismatches.len(),
        mismatches.join("\n  - ")
    );
    eprintln!("{checked} pages end where the reference's do ({cut} cut short)");
}
