//! Differential test against the reference implementation.
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
//! cargo test --test oracle_diff --features legacy-query -- --nocapture
//! ```
#![cfg(feature = "legacy-query")]

use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::output::execute_plan;
use sqd_query_engine::query::{compile, parse_query};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// (catalog, fixture directory). Only datasets both engines serve.
const DATASETS: &[(&str, &str)] = &[
    ("evm", "ethereum"),
    ("evm", "optimism"),
    ("solana", "solana"),
    ("substrate", "kusama"),
    ("substrate", "moonbeam"),
    ("bitcoin", "bitcoin"),
];

/// How many blocks each generated query covers. Wide enough that filters match
/// something, narrow enough that a few hundred queries finish.
const BLOCK_SPAN: u64 = 40;

/// How many sampled values a generated IN-list carries.
const VALUES_PER_FILTER: usize = 3;

fn fixture_chunk(dataset: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(dataset)
        .join("chunk")
}

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
        let Some(query_name) = table.query_name.as_deref() else {
            continue;
        };
        let Some(field_name) = table.field_name.as_deref() else {
            continue;
        };

        // The bare item request, and every relation the table declares.
        let mut item_shapes = vec![("plain".to_string(), String::new())];
        for relation in table.relations.keys() {
            item_shapes.push((
                format!("relation {relation}"),
                format!(r#""{}":true"#, snake_to_camel(relation)),
            ));
        }
        if table.relations.len() > 1 {
            let all: Vec<String> = table
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

            for column in &table.filters {
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
