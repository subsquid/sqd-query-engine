//! CT-9 — fuzz. Whatever arrives, the engine answers or refuses.
//!
//! The assertion is panic-only. Both surfaces are inputs the engine does not
//! control: the request comes off the network, and the chunk comes from an
//! archiver version that may predate the catalog.
//!
//! HC-7, a deterministic fuzzer with a recorded seed, does not exist; what is
//! here is proptest over the request surface and one hand-written chunk.
//!
//! Every value here reaches the engine straight off the network. The property is
//! not that any particular value is accepted — most are nonsense — but that a
//! value is never able to take the thread down instead of producing an error.

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::query::{compile, parse_query};
use std::path::Path;
use std::sync::OnceLock;

use crate::harness::chunk::write_parquet;

fn evm() -> &'static DatasetDescription {
    static META: OnceLock<DatasetDescription> = OnceLock::new();
    META.get_or_init(|| load_dataset_description(Path::new("metadata/evm.yaml")).unwrap())
}

fn solana() -> &'static DatasetDescription {
    static META: OnceLock<DatasetDescription> = OnceLock::new();
    META.get_or_init(|| load_dataset_description(Path::new("metadata/solana.yaml")).unwrap())
}

/// Parse and compile, discarding the outcome: the test is that we get one.
fn settle(query: &str, metadata: &DatasetDescription) {
    if let Ok(parsed) = parse_query(query.as_bytes(), metadata) {
        let _ = compile(&parsed, metadata);
    }
}

/// JSON-escape a generated string so the query stays well-formed and the value
/// under test reaches the filter rather than the JSON parser.
fn json(value: &str) -> String {
    serde_json::to_string(value).unwrap()
}

/// Values shaped like hex, because that is the only shape that gets past the
/// `0x` prefix check and into the parsing itself. An unbiased string generator
/// produces essentially none of them and exercises nothing.
fn hexish() -> impl Strategy<Value = String> {
    prop_oneof![
        "0[xX][0-9a-fA-F]*",
        "0[xX].*",
        "0[xX][0-9a-fA-F]*.[0-9a-fA-F]*",
        ".*",
    ]
}

proptest! {
    // Where the recorded counterexamples live. The default, `SourceParallel`,
    // resolves against the nearest `lib.rs` or `main.rs` — so merging the suite
    // into one target silently moved the file out from under the checked-in one,
    // and proptest treats a missing persistence file as "no regressions".
    #![proptest_config(ProptestConfig {
        cases: 512,
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    })]

    /// Hex-typed filters: discriminators, fixed-width binary, and the byte-pair
    /// walk inside them.
    #[test]
    fn no_hex_filter_value_panics(value in hexish()) {
        let v = json(&value);
        settle(&format!(
            r#"{{"type":"solana","fromBlock":0,"instructions":[{{"d8":[{v}]}}]}}"#), solana());
        settle(&format!(
            r#"{{"type":"solana","fromBlock":0,"instructions":[{{"d1":[{v}]}}]}}"#), solana());
        settle(&format!(
            r#"{{"type":"solana","fromBlock":0,"instructions":[{{"discriminator":[{v}]}}]}}"#),
            solana());
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{"topic0":[{v}]}}]}}"#), evm());
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"transactions":[{{"sighash":[{v}]}}]}}"#), evm());
    }

    /// String-typed filters take the value as written, but still walk it.
    #[test]
    fn no_string_filter_value_panics(value in hexish()) {
        let v = json(&value);
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{"address":[{v}]}}]}}"#), evm());
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"traces":[{{"type":[{v}]}}]}}"#), evm());
        settle(&format!(
            r#"{{"type":"solana","fromBlock":0,"instructions":[{{"programId":[{v}]}}]}}"#),
            solana());
    }

    /// Keys are turned into column names before anything looks them up.
    #[test]
    fn no_filter_or_field_name_panics(name in ".*") {
        let n = json(&name);
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{{n}:["0x00"]}}]}}"#), evm());
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{}}],"fields":{{"log":{{{n}:true}}}}}}"#),
            evm());
        settle(&format!(
            r#"{{"type":"evm","fromBlock":0,"logs":[{{}}],"fields":{{{n}:{{"address":true}}}}}}"#),
            evm());
    }
}

/// The scalars a client can put anywhere: wrong types are refused, but the
/// refusal is an error, not a panic.
#[test]
fn no_scalar_shape_panics_in_a_top_level_slot() {
    const SHAPES: &[&str] = &[
        "null",
        "true",
        "false",
        "0",
        "-1",
        "1.5",
        "1e400",
        "18446744073709551616",
        r#""""#,
        r#""0x""#,
        r#""0x0""#,
        r#""nonsense""#,
        "[]",
        "{}",
        r#"[null]"#,
        r#"[[]]"#,
    ];

    for shape in SHAPES {
        for slot in [
            "fromBlock",
            "toBlock",
            "includeAllBlocks",
            "parentBlockHash",
            "fields",
        ] {
            settle(
                &format!(r#"{{"type":"evm","{slot}":{shape},"logs":[{{}}]}}"#),
                evm(),
            );
        }
        settle(
            &format!(r#"{{"type":"evm","fromBlock":0,"logs":{shape}}}"#),
            evm(),
        );
        settle(
            &format!(r#"{{"type":"evm","fromBlock":0,"logs":[{shape}]}}"#),
            evm(),
        );
        settle(
            &format!(r#"{{"type":"evm","fromBlock":0,"logs":[{{"address":{shape}}}]}}"#),
            evm(),
        );
    }
}

// ---------------------------------------------------------------------------
// The chunk surface — a chunk that disagrees with the catalog
// ---------------------------------------------------------------------------

/// A chunk whose physical types disagree with the catalog's declared ones must
/// be answered or refused, never crash the process.
///
/// An encoding names the type the catalog *believes* a column has, and archives
/// outlive the catalogs that described them. The encoders reached that way used
/// to downcast on the catalog's word, so one column written at a different width
/// took down a worker serving every other query with it.
///
/// Covers CT-9 · INV-E1
#[test]
fn a_chunk_that_disagrees_with_the_catalog_does_not_panic() {
    use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use sqd_query_engine::metadata::parse_dataset_description;
    use sqd_query_engine::output::execute_chunk;
    use sqd_query_engine::scan::ParquetChunkReader;
    use std::sync::Arc;

    let catalog = parse_dataset_description(
        r#"
version: v2
name: test

tables:
  blocks:
    output:
      name: block
      fields: [number]
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }

  items:
    request:
      name: items
      filters: []
    output:
      name: item
      fields: [seq, stamp, version, doc, big, d8]
    block_number_column: block_number
    item_order_keys: [seq]
    sort_key: [block_number, seq]
    columns:
      block_number: { type: uint64 }
      seq: { type: uint32 }
      stamp:
        type: timestamp_millisecond
        encoding: timestamp_millisecond
      version:
        type: int16
        encoding: solana_tx_version
      doc:
        type: string
        encoding: json_verbatim
      big:
        type: uint64
        encoding: decimal_string
      d8:
        type: uint64
        encoding: hex_number
"#,
    )
    .unwrap();

    // Every encoded column is stored as something the catalog does not describe.
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("seq", DataType::UInt32, false),
        Field::new("stamp", DataType::UInt64, false),
        Field::new("version", DataType::Utf8, false),
        Field::new("doc", DataType::UInt32, false),
        Field::new("big", DataType::Utf8, false),
        Field::new("d8", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![10u64, 11])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32, 0])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![1_700_000_000_000u64; 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["legacy", "0"])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![1u32, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["1", "2"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["0xff", "0x00"])) as ArrayRef,
        ],
    )
    .unwrap();

    let blocks_schema = Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::UInt64,
        false,
    )]));
    let blocks = RecordBatch::try_new(
        blocks_schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![10u64, 11])) as ArrayRef],
    )
    .unwrap();

    for (name, batch) in [("items", batch), ("blocks", blocks)] {
        write_parquet(&dir.path().join(format!("{name}.parquet")), &batch);
    }

    let query = br#"{"type":"test","fromBlock":10,"toBlock":11,
        "fields":{"block":{"number":true},
                  "item":{"stamp":true,"version":true,"doc":true,"big":true,"d8":true}},
        "items":[{}]}"#;

    let parsed = parse_query(query, &catalog).unwrap();
    let plan = compile(&parsed, &catalog).unwrap();
    let reader = ParquetChunkReader::open(dir.path()).unwrap();

    // Either outcome is conforming; the test is that we get one.
    let Ok(Some(out)) = execute_chunk(&plan, &catalog, &reader, false) else {
        return;
    };

    let body = out.into_json_lines();
    for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        serde_json::from_slice::<serde_json::Value>(line)
            .expect("a response the encoders wrote must still be JSON");
    }
}

// ---------------------------------------------------------------------------
// A block number nothing can place, reached from every scan path
// ---------------------------------------------------------------------------

/// A chunk whose block-number column cannot place a row is refused, whatever
/// query reaches it.
///
/// The value is what every layer puts a row somewhere by: which row group can
/// still own it, what it weighs, which block it is emitted under. Read through
/// the width-tolerant reader a null returns the slot's placeholder, so the row
/// quietly becomes block 0's — a wrong answer, and a different wrong answer in
/// each reader.
///
/// The shapes are the point. A check placed where the rows come out sees only
/// the rows a predicate left, and the hierarchical scan returns before reaching
/// it at all, so the same chunk would error on a direct scan and answer, short,
/// on a relation pull. That is worse than either — a client cannot tell a chunk
/// the engine refuses from one it silently under-answers — so the check runs
/// once, on entry, over every row group rather than the selected ones.
///
/// The same chunk is written twice, because what the check reads is the file's
/// null count and a file need not state one. Stated, `Some(0)` and "not stated"
/// are the same value to a reader that defaults the absent one to zero, and the
/// chunk goes on to answer with the null read as block zero. So where the
/// metadata says nothing the column is read instead, and the row group that says
/// nothing is the one this second chunk is made of.
///
/// Covers CT-9 · INV-E1
#[test]
fn a_block_number_nothing_can_place_is_refused_on_every_path() {
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    for (stated, props) in [
        ("stating a null count", None),
        (
            "stating none",
            Some(
                WriterProperties::builder()
                    .set_statistics_enabled(EnabledStatistics::None)
                    .build(),
            ),
        ),
    ] {
        refuses_every_path(stated, props);
    }
}

/// One writing of that chunk, read back along every path.
fn refuses_every_path(stated: &str, props: Option<parquet::file::properties::WriterProperties>) {
    use arrow::array::{ArrayRef, ListArray, UInt32Array, UInt64Array};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use sqd_query_engine::error::{error_kind, ErrorKind};
    use sqd_query_engine::scan::{
        ChunkReader, HierarchicalFilter, HierarchicalMode, KeyFilter, ParquetChunkReader,
        ScanRequest,
    };
    use std::fs::File;
    use std::sync::Arc;

    const BN: &str = "block_number";

    let dir = tempfile::tempdir().unwrap();
    let address_field = Arc::new(Field::new("item", DataType::UInt32, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new(BN, DataType::UInt64, true),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new(
            "instruction_address",
            DataType::List(address_field.clone()),
            false,
        ),
    ]));

    // Four rows over three blocks, the third of them carrying no block at all.
    let blocks = vec![Some(100u64), Some(101), None, Some(102)];
    let addresses = ListArray::new(
        address_field,
        OffsetBuffer::from_lengths([1usize, 1, 1, 1]),
        Arc::new(UInt32Array::from(vec![0u32, 1, 2, 3])) as ArrayRef,
        None,
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(blocks)) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32, 0, 0, 0])) as ArrayRef,
            Arc::new(addresses) as ArrayRef,
        ],
    )
    .unwrap();

    let file = File::create(dir.path().join("items.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), props).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = ParquetChunkReader::open(dir.path()).unwrap();

    let request = || {
        let mut request = ScanRequest::new(vec![BN, "transaction_index"]);
        request.block_number_column = Some(BN);
        request
    };

    // A relation pull, and a hierarchical one: both built from no source rows,
    // since what is under test is the path taken, not what the filter selects.
    let key_filter = KeyFilter::build(&[], &[BN], &[BN], BN, BN);
    let hierarchical = HierarchicalFilter::build(
        &[],
        &[BN, "transaction_index"],
        "instruction_address",
        "instruction_address",
        HierarchicalMode::Children,
        true,
    );

    let mut bounded = request();
    bounded.from_block = Some(100);
    bounded.to_block = Some(101);

    let mut pulled = request();
    pulled.key_filter = Some(&key_filter);

    let mut descended = request();
    descended.hierarchical_filter = Some(&hierarchical);

    // A range that excludes the offending row: the chunk is still the chunk.
    let mut past_it = request();
    past_it.from_block = Some(102);

    for (shape, request) in [
        ("an unbounded scan", request()),
        ("a bounded range", bounded),
        ("a relation pull", pulled),
        ("a hierarchical pull", descended),
        ("a range past the unplaceable row", past_it),
    ] {
        let err = reader.scan("items", &request).expect_err(&format!(
            "{shape} answered from a chunk it cannot place every row against, \
             written {stated}"
        ));

        assert_eq!(
            error_kind(&err),
            Some(ErrorKind::MalformedChunkData),
            "{shape} failed with something other than a malformed chunk \
             on a chunk written {stated}: {err:#}"
        );
    }
}
