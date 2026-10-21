//! Wall time of each membership case (issue #82) on one thread, after a report
//! of what each answers: rows, members, junk rows and bytes, checked against
//! the unfiltered scan. A case that drops a member panics before any timing.
//!
//! Three groups per table: the whole request; the predicate alone over the
//! filtered column, one array per row group as the scan hands it over; and
//! parsing and compiling the request, which the portal makes a worker pay once
//! per chunk because it sends the whole query with every chunk.
//!
//!   SQD_MEMBERSHIP_DIR=sets taskset -c 2 cargo bench --features membership-filter \
//!     --bench membership_wall

#[path = "../membership/cases.rs"]
mod cases;
#[path = "../run.rs"]
mod run;

use arrow::array::{Array, ArrayRef, StringArray};
use cases::{Filter, Target, LOGS, TOKEN_BALANCES, TRANSACTIONS};
use criterion::{criterion_group, criterion_main, Criterion};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use run::{run_query, Format};
use serde_json::Value;
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::membership::MembershipPredicate;
use sqd_query_engine::scan::predicate::{ArrayPredicate, InListPredicate};
use sqd_query_engine::scan::ParquetChunkReader;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// A row of an answer: its block and its key within the block, and the
/// filtered value.
type Row = ((u64, String), String);

/// Every row of the answer, paged the way a client pages, and the bytes of
/// every page.
fn rows(
    query: &[u8],
    meta: &DatasetDescription,
    chunk: &ParquetChunkReader,
    target: Target,
) -> (Vec<Row>, usize) {
    let mut query: Value = serde_json::from_slice(query).unwrap();
    let mut rows = Vec::new();
    let mut bytes = 0;

    loop {
        let parsed = parse_query(&serde_json::to_vec(&query).unwrap(), meta).unwrap();
        let plan = compile(&parsed, meta).unwrap();
        let Some(output) = execute_chunk(&plan, meta, chunk, false).unwrap() else {
            break;
        };
        // Not `read_through()`: a page can read blocks its budget left out, and
        // continuing after them loses their rows.
        let last = output.last_block();
        let json = output.into_json_lines();
        bytes += json.len();

        for line in json.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            let block: Value = serde_json::from_slice(line).unwrap();
            let number = block["header"]["number"].as_u64().unwrap();
            for row in block[target.request].as_array().into_iter().flatten() {
                let key: Vec<String> = target.keys.iter().map(|k| row[*k].to_string()).collect();
                let value = row[target.filter].as_str().unwrap_or_default().to_owned();
                rows.push(((number, key.join("/")), value));
            }
        }

        if last >= target.chunk_last() {
            break;
        }
        query["fromBlock"] = (last + 1).into();
    }

    (rows, bytes)
}

fn report(target: Target, set: &[String], meta: &DatasetDescription, chunk: &ParquetChunkReader) {
    let cases = cases::cases(target);
    let (all, all_bytes) = rows(&cases[0].query(set), meta, chunk, target);

    let mut per_value: HashMap<&str, usize> = HashMap::new();
    for (_, value) in &all {
        *per_value.entry(value).or_default() += 1;
    }
    let hottest = per_value.values().max().copied().unwrap_or(0);
    eprintln!(
        "\n{}: {} rows, {} distinct values, hottest value {} rows ({:.1}%), {} bytes unfiltered",
        target.request,
        all.len(),
        per_value.len(),
        hottest,
        100.0 * hottest as f64 / all.len().max(1) as f64,
        all_bytes
    );
    eprintln!(
        "{:<34} {:>9} {:>8} {:>8} {:>9} {:>11} {:>10}",
        "case", "wire B", "rows", "members", "junk", "junk/memb", "resp B"
    );

    for case in &cases[1..] {
        let members: HashSet<&str> = set[..case.set_size].iter().map(String::as_str).collect();
        let exact: HashSet<&(u64, String)> = all
            .iter()
            .filter(|(_, value)| members.contains(value.as_str()))
            .map(|(key, _)| key)
            .collect();

        let query = case.query(set);
        let (got, bytes) = rows(&query, meta, chunk, target);
        let got_keys: HashSet<&(u64, String)> = got.iter().map(|(key, _)| key).collect();
        let lost = exact.difference(&got_keys).count();
        assert_eq!(lost, 0, "{} dropped {lost} member rows", case.name);

        let wire = match case.filter {
            Filter::List => query.len(),
            _ => wire_bytes(&query),
        };
        let junk = got.len() - exact.len();
        if matches!(case.filter, Filter::List) {
            assert_eq!(junk, 0, "{} is exact, so the report is wrong", case.name);
        }
        eprintln!(
            "{:<34} {:>9} {:>8} {:>8} {:>9} {:>11.3} {:>10}",
            case.name,
            wire,
            got.len(),
            exact.len(),
            junk,
            junk as f64 / exact.len().max(1) as f64,
            bytes
        );
    }
}

/// The base64 text of the structure, as it travels.
fn wire_bytes(query: &[u8]) -> usize {
    let query: Value = serde_json::from_slice(query).unwrap();
    let mut longest = 0;
    let mut stack = vec![&query];
    while let Some(value) = stack.pop() {
        match value {
            Value::String(s) => longest = longest.max(s.len()),
            Value::Array(a) => stack.extend(a),
            Value::Object(o) => stack.extend(o.values()),
            _ => {}
        }
    }
    longest
}

/// The filtered column, one array per row group.
fn column(target: Target) -> Vec<ArrayRef> {
    let path = target.chunk_dir().join(target.file);
    let open = || {
        let file = std::fs::File::open(&path).unwrap();
        ParquetRecordBatchReaderBuilder::try_new(file).unwrap()
    };
    let groups = open().metadata().num_row_groups();

    let mut arrays = Vec::new();
    for group in 0..groups {
        let builder = open();
        let mask = ProjectionMask::columns(builder.parquet_schema(), [target.column]);
        let rows = builder.metadata().row_group(group).num_rows() as usize;
        let reader = builder
            .with_projection(mask)
            .with_row_groups(vec![group])
            .with_batch_size(rows)
            .build()
            .unwrap();
        for batch in reader {
            arrays.push(batch.unwrap().column(0).clone());
        }
    }
    arrays
}

fn predicate(value: &Value) -> Arc<dyn ArrayPredicate> {
    match value.as_array() {
        Some(list) => {
            let values: Vec<&str> = list.iter().map(|v| v.as_str().unwrap()).collect();
            Arc::new(InListPredicate::new(Arc::new(StringArray::from(values))))
        }
        None => Arc::new(MembershipPredicate::from_json(value).unwrap()),
    }
}

/// The targets `SQD_MEMBERSHIP_TARGETS` names (`logs,tokenBalances`), or all.
fn targets() -> Vec<Target> {
    let only = std::env::var("SQD_MEMBERSHIP_TARGETS").ok();
    [LOGS, TRANSACTIONS, TOKEN_BALANCES]
        .into_iter()
        .filter(|t| {
            only.as_deref()
                .is_none_or(|only| only.split(',').any(|o| o == t.request))
        })
        .collect()
}

fn membership(c: &mut Criterion) {
    rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build_global()
        .unwrap();

    let only = std::env::var("SQD_MEMBERSHIP_CASES").ok();
    let selected = |case: &cases::Case| only.as_deref().is_none_or(|only| case.name.contains(only));

    for target in targets() {
        let meta = target.catalog();
        let chunk = ParquetChunkReader::open(&target.chunk_dir()).unwrap();
        let set = target.set();
        eprintln!(
            "\n{}: chunk {}, last block {}; set of {} values, inList and the small structures take {}",
            target.request,
            target.chunk_name(),
            target.chunk_last(),
            set.len(),
            cases::LIST_SIZE.min(set.len())
        );
        report(target, &set, &meta, &chunk);

        let mut group = c.benchmark_group(target.request);
        for case in cases::cases(target).into_iter().filter(&selected) {
            let query = case.query(&set);
            let name = case.name.split_once('/').map(|(_, name)| name.to_owned());
            group.bench_function(name.unwrap_or(case.name), |b| {
                b.iter(|| run_query(&query, &meta, &chunk, Format::Json));
            });
        }
        group.finish();

        let mut group = c.benchmark_group(format!("{}/prepare", target.request));
        for case in cases::cases(target).into_iter().filter(&selected) {
            if matches!(case.filter, Filter::All) {
                continue;
            }
            let query = case.query(&set);
            let name = case.name.split_once('/').map(|(_, name)| name.to_owned());
            group.bench_function(name.unwrap_or(case.name), |b| {
                b.iter(|| {
                    let parsed = parse_query(&query, &meta).unwrap();
                    std::hint::black_box(compile(&parsed, &meta).unwrap());
                });
            });
        }
        group.finish();

        let arrays = column(target);
        let rows: usize = arrays.iter().map(|a| a.len()).sum();
        eprintln!(
            "{}.{}: {} rows in {} row groups, stored as {}",
            target.request,
            target.column,
            rows,
            arrays.len(),
            arrays[0].data_type()
        );
        let mut group = c.benchmark_group(format!("{}.{}/evaluate", target.request, target.column));
        group.throughput(criterion::Throughput::Elements(rows as u64));
        for case in cases::cases(target).into_iter().filter(&selected) {
            let Some(value) = case.filter_value(&set) else {
                continue;
            };
            let predicate = predicate(&value);
            let name = case.name.split_once('/').map(|(_, name)| name.to_owned());
            group.bench_function(name.unwrap_or(case.name), |b| {
                b.iter(|| {
                    for array in &arrays {
                        std::hint::black_box(predicate.evaluate(array.as_ref()).unwrap());
                    }
                });
            });
        }
        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = membership
}
criterion_main!(benches);
