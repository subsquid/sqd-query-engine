//! Fork detection: `parentBlockHash` is answered or refused, never ignored.

use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::metadata::DatasetDescription;
use sqd_query_engine::output::execute_plan;
use sqd_query_engine::query::{compile, parse_query};

use crate::harness::chunk::chunk_without_column;
use crate::harness::chunk::{write_parquet, write_table_row_groups};
use crate::harness::fixtures::{fixture_tree_is_present, meta, run, run_against};
use crate::harness::json::parse_response;

/// The parent hash of the first block a chunk can speak about, read from the
/// fixture itself so the test is not pinned to a hard-coded chain.
fn parent_hash_of(dataset: &str, metadata: &DatasetDescription, block: u64) -> String {
    let body = run(
        dataset,
        metadata,
        format!(
            r#"{{"type":"{}","fromBlock":{block},"toBlock":{block},"includeAllBlocks":true,
                 "fields":{{"block":{{"number":true,"parentHash":true}}}}}}"#,
            metadata.name
        )
        .as_bytes(),
    )
    .unwrap();
    let block = &parse_response(&body)[0];
    block["header"]["parentHash"].as_str().unwrap().to_string()
}

/// A client paging through a chain sends back the hash it believes the previous
/// block has. Accepting the field and ignoring it — which is what happened
/// before — serves data from a branch the client did not ask about, with nothing
/// in the response to say so.
///
/// Covers CT-2 · INV-E5
#[test]
#[ignore = "requires external fixture data"]
fn a_mismatched_parent_block_hash_is_reported() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    const FROM: u64 = 17881391;

    let query = |parent: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":{FROM},"toBlock":{FROM},"includeAllBlocks":true,
                 "parentBlockHash":"{parent}",
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .into_bytes()
    };

    // The chunk agrees: the query is answered.
    let actual_parent = parent_hash_of("ethereum", &evm, FROM);
    let body = run("ethereum", &evm, &query(&actual_parent)).unwrap();
    assert!(!body.is_empty(), "a matching parent hash must be served");

    // The chunk disagrees: the client is told, and told what the chunk has.
    let err = run("ethereum", &evm, &query("0xdeadbeef"))
        .expect_err("a mismatched parent hash must be reported");
    assert_eq!(error_kind(&err), Some(ErrorKind::UnexpectedBaseBlock));
    let reported = err
        .downcast_ref::<sqd_query_engine::output::UnexpectedBaseBlock>()
        .expect("the error must be an UnexpectedBaseBlock a client can act on");
    assert_eq!(reported.expected_hash, "0xdeadbeef");
    let parent = reported
        .prev_blocks
        .last()
        .expect("prev_blocks must not be empty");
    assert_eq!(parent.number, FROM - 1);
    assert_eq!(parent.hash, actual_parent);
}

/// The chunk's own first block still settles the question — its row carries its
/// parent's hash — so the check fires there too.
///
/// Covers CT-2 · INV-E5
#[test]
#[ignore = "requires external fixture data"]
fn the_first_block_of_a_chunk_still_settles_its_parent() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let actual_parent = parent_hash_of("ethereum", &meta("evm"), 17881390);

    run(
        "ethereum",
        &evm,
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881390,"includeAllBlocks":true,
                 "parentBlockHash":"{actual_parent}",
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .as_bytes(),
    )
    .expect("a matching parent hash must be served");

    run(
        "ethereum",
        &evm,
        br#"{"type":"evm","fromBlock":17881390,"toBlock":17881390,"includeAllBlocks":true,
             "parentBlockHash":"0xnot-the-parent-of-anything",
             "fields":{"block":{"number":true}}}"#,
    )
    .expect_err("a mismatched parent hash must be reported");
}

/// A chunk holding no evidence about the parent must stay quiet rather than
/// reject the request: the chunk not knowing is not the chain having forked.
///
/// Covers CT-2 · INV-E5
#[test]
#[ignore = "requires external fixture data"]
fn an_invisible_parent_block_is_not_a_fork() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    // The chunk starts at 17881390, so nothing in it speaks about this window.
    run(
        "ethereum",
        &evm,
        br#"{"type":"evm","fromBlock":17000000,"toBlock":17000001,"includeAllBlocks":true,
             "parentBlockHash":"0xnot-the-parent-of-anything",
             "fields":{"block":{"number":true}}}"#,
    )
    .expect("a parent the chunk cannot see must not be reported as a fork");
}

/// The field is a hash, and a non-string is rejected at parse time rather than
/// quietly ignored.
///
/// Covers CT-2 · INV-E5
#[test]
fn a_malformed_parent_block_hash_is_rejected() {
    let evm = meta("evm");
    for json in [
        r#"{"type":"evm","fromBlock":0,"parentBlockHash":123}"#,
        r#"{"type":"evm","fromBlock":0,"parentBlockHash":["0xabc"]}"#,
    ] {
        assert!(
            parse_query(json.as_bytes(), &evm).is_err(),
            "expected an error for {json}"
        );
    }
}

/// A chain that skips numbers has no block at `fromBlock - 1`, so the
/// predecessor has to be read from the parent-number column rather than
/// computed. Solana slot 217710449 follows 217710447.
/// Fork detection reads two columns out of the block table. When the chunk does
/// not carry them the check cannot run — and it used to answer the query anyway,
/// which is the one outcome `parentBlockHash` exists to prevent. The reference
/// refuses: `column 'parent_hash' is not found in 'blocks'`.
///
/// Covers CT-2 · INV-E5
#[test]
#[ignore = "requires external fixture data"]
fn a_chunk_that_cannot_answer_the_fork_check_is_an_error() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let chunk = chunk_without_column("ethereum", "blocks", "parent_hash");
    let parent = parent_hash_of("ethereum", &evm, 17881390);
    let query = format!(
        r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
             "parentBlockHash":"{parent}",
             "fields":{{"block":{{"number":true}}}},"logs":[{{}}]}}"#
    );

    let parsed = parse_query(query.as_bytes(), &evm).unwrap();
    let plan = compile(&parsed, &evm).unwrap();
    let err = match execute_plan(&plan, &evm, chunk.path()) {
        Err(e) => e,
        Ok(_) => panic!("a chunk without 'parent_hash' cannot serve a fork-checked query"),
    };

    assert!(
        err.root_cause().to_string().contains("parent_hash"),
        "the error must name the missing column, got: {}",
        err.root_cause()
    );
}

/// A chunk with no block table at all cannot answer the check either. The query
/// must fail rather than read the missing table as "no evidence of a fork".
///
/// Covers CT-2 · INV-E5
#[test]
fn a_chunk_with_no_block_table_cannot_clear_the_fork_check() {
    use arrow::array::{ArrayRef, UInt32Array, UInt64Array};
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
    block_number_column: number
    parent_hash_column: parent_hash
    sort_key: [number]
    columns:
      number: { type: uint64 }
      parent_hash: { type: string }

  logs:
    request:
      name: logs
      filters: []
    output:
      name: log
      fields: [log_index]
    block_number_column: block_number
    item_order_keys: [log_index]
    sort_key: [block_number, log_index]
    columns:
      block_number: { type: uint64 }
      log_index: { type: uint32 }
"#,
    )
    .unwrap();

    // Only the item table is on disk.
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![10u64, 11])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0u32, 0])) as ArrayRef,
        ],
    )
    .unwrap();
    write_parquet(&dir.path().join("logs.parquet"), &batch);

    let answer = |query: &str| {
        let parsed = parse_query(query.as_bytes(), &catalog).unwrap();
        let plan = compile(&parsed, &catalog).unwrap();
        let reader = ParquetChunkReader::open(dir.path()).unwrap();
        execute_chunk(&plan, &catalog, &reader, false)
    };

    assert!(
        answer(
            r#"{"type":"test","fromBlock":10,"toBlock":11,
                "parentBlockHash":"0xabcd","logs":[{}]}"#
        )
        .is_err(),
        "a chunk with no block table holds no answer, and must say so"
    );
}

/// The column is required only of a chunk that reaches into the lookback window.
/// `required_columns` is checked before row groups are pruned, so requiring it of
/// every chunk fails a whole multi-chunk request over one that was never going to
/// carry the predecessor — a chunk far below `fromBlock` says nothing about it
/// either way.
///
/// Covers CT-2 · INV-E5
#[test]
fn a_chunk_below_the_lookback_window_is_not_asked_for_the_parent_hash() {
    use arrow::array::{ArrayRef, UInt64Array};
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
    block_number_column: number
    parent_hash_column: parent_hash
    sort_key: [number]
    columns:
      number: { type: uint64 }
      parent_hash: { type: string }
"#,
    )
    .unwrap();

    // Blocks 1000..1009, written before `parent_hash` existed.
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from((1000u64..1010).collect::<Vec<_>>())) as ArrayRef],
    )
    .unwrap();
    let file = std::fs::File::create(dir.path().join("blocks.parquet")).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let answer = |from: u64, to: u64| {
        let query = format!(
            r#"{{"type":"test","fromBlock":{from},"toBlock":{to},
                "parentBlockHash":"0xabcd","includeAllBlocks":true}}"#
        );
        let parsed = parse_query(query.as_bytes(), &catalog).unwrap();
        let plan = compile(&parsed, &catalog).unwrap();
        let reader = ParquetChunkReader::open(dir.path()).unwrap();
        execute_chunk(&plan, &catalog, &reader, false)
    };

    assert!(
        answer(5000, 5010).is_ok(),
        "the window is blocks 4900..5000 and this chunk ends at 1009, so it is not \
         being asked anything"
    );
    assert!(
        answer(1005, 1009).is_err(),
        "this chunk does hold the predecessor and cannot produce its hash"
    );
}

/// Two behaviours below are pinned because they read like defects and are not:
/// both were compared against the reference implementation on this chunk and
/// produce its message verbatim. Changing either is a divergence, not a fix.
///
/// A hash is compared byte for byte. Every *filter* value in this engine folds
/// case, so an upper-cased hash looks like it should be accepted — the reference
/// rejects it, and a client that upper-cases hashes is broken against both
/// engines identically rather than against one of them.
///
/// Covers CT-2 · INV-E5
#[test]
#[ignore = "requires external fixture data"]
fn a_parent_block_hash_is_compared_byte_for_byte() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let parent = parent_hash_of("ethereum", &evm, 17881390);
    let query = |hash: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                 "parentBlockHash":"{hash}",
                 "fields":{{"block":{{"number":true}}}},"logs":[{{}}]}}"#
        )
        .into_bytes()
    };

    run("ethereum", &evm, &query(&parent)).expect("the true parent hash is accepted");

    let upper = parent.to_uppercase().replace("0X", "0x");
    assert!(
        run("ethereum", &evm, &query(&upper)).is_err(),
        "an upper-cased hash is a different hash, as it is for the reference"
    );
}

/// Only the row *at* `fromBlock` states what precedes it (INV-E5). With
/// `fromBlock` past the end of the chunk there is no such row, and the highest
/// row the chunk does hold describes a different block — comparing against it
/// reports a fork on a chain that never reorganised.
///
/// The reference compares against that row anyway, so this is a deliberate
/// divergence, in the same direction as the one already taken for an empty
/// window: a chunk that cannot see the block is not evidence about it.
///
/// Covers CT-2 · INV-E5
#[test]
#[ignore = "requires external fixture data"]
fn a_from_block_past_the_chunk_is_not_evidence_of_a_fork() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let query = |parent: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17882796,"toBlock":17882800,
                 "parentBlockHash":"{parent}",
                 "fields":{{"block":{{"number":true}}}},"logs":[{{}}]}}"#
        )
        .into_bytes()
    };

    // The hash of a real block's parent, and a hash of nothing at all. Neither
    // describes the parent of 17882796, and the chunk cannot say what does.
    for hash in [
        parent_hash_of("ethereum", &evm, 17881390),
        "0xdeadbeef".to_string(),
    ] {
        assert!(
            run("ethereum", &evm, &query(&hash)).is_ok(),
            "a chunk that does not reach fromBlock must not report a fork"
        );
    }
}

/// The counterpart: where the chunk *does* hold the row at `fromBlock`, that row
/// is the one compared, and a wrong hash is still a fork.
///
/// Covers CT-2 · INV-E5
#[test]
#[ignore = "requires external fixture data"]
fn the_row_at_from_block_is_the_one_compared() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    const FROM: u64 = 17881500;

    let query = |parent: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":{FROM},"toBlock":{FROM},
                 "parentBlockHash":"{parent}","includeAllBlocks":true,
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .into_bytes()
    };

    let its_own_parent = parent_hash_of("ethereum", &evm, FROM);
    assert!(run("ethereum", &evm, &query(&its_own_parent)).is_ok());

    // An earlier block's parent hash is inside the lookback window, so it would
    // pass if any row of the window could answer. Only one can.
    let earlier = parent_hash_of("ethereum", &evm, FROM - 10);
    assert!(
        run("ethereum", &evm, &query(&earlier)).is_err(),
        "a hash from elsewhere in the window is not the parent of {FROM}"
    );
}

/// Covers CT-2 · INV-E5
#[test]
#[ignore = "requires external fixture data"]
fn fork_detection_follows_a_chain_that_skips_numbers() {
    if !fixture_tree_is_present() {
        return;
    }

    let solana = meta("solana");
    const FROM: u64 = 217_710_449;
    const PARENT: u64 = 217_710_447;

    let actual_parent = parent_hash_of("solana", &solana, FROM);

    let err = run(
        "solana",
        &solana,
        format!(
            r#"{{"type":"solana","fromBlock":{FROM},"toBlock":{FROM},"includeAllBlocks":true,
                 "parentBlockHash":"not-the-parent",
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .as_bytes(),
    )
    .expect_err("a mismatched parent hash must be reported");

    let reported = err
        .downcast_ref::<sqd_query_engine::output::UnexpectedBaseBlock>()
        .expect("the error must be an UnexpectedBaseBlock");
    let parent = reported.prev_blocks.last().unwrap();
    assert_eq!(
        parent.number, PARENT,
        "the predecessor is the declared parent slot, not fromBlock - 1"
    );
    assert_eq!(parent.hash, actual_parent);

    run(
        "solana",
        &solana,
        format!(
            r#"{{"type":"solana","fromBlock":{FROM},"toBlock":{FROM},"includeAllBlocks":true,
                 "parentBlockHash":"{actual_parent}",
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .as_bytes(),
    )
    .expect("a matching parent hash must be served");
}

/// A chain that skips more numbers than `P-FORK-WINDOW` used to lose fork
/// detection at exactly the point it was needed: the window was searched over
/// *parent* numbers, so the row at `fromBlock` — the only row that states what
/// precedes it — fell outside it and the check was skipped in silence.
///
/// The window sizes the evidence an `UnexpectedBaseBlock` carries. It must not
/// decide whether the parent is found.
///
/// Covers CT-2 · INV-E5
#[test]
fn a_numbering_gap_wider_than_the_window_still_settles_the_parent() {
    use arrow::array::{ArrayRef, StringArray, UInt64Array};
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
    parent_hash_column: parent_hash
    parent_number_column: parent_number
    sort_key: [number]
    columns:
      number: { type: uint64 }
      parent_number: { type: uint64 }
      parent_hash: { type: string }
"#,
    )
    .unwrap();

    // Two slots five hundred numbers apart — a gap five times the window.
    const PARENT: u64 = 1_000;
    const FROM: u64 = 1_500;

    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("parent_number", DataType::UInt64, false),
        Field::new("parent_hash", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![PARENT, FROM])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![PARENT - 1, PARENT])) as ArrayRef,
            Arc::new(StringArray::from(vec!["0x0999", "0x1000"])) as ArrayRef,
        ],
    )
    .unwrap();
    let file = std::fs::File::create(dir.path().join("blocks.parquet")).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let answer = |parent: &str| {
        let query = format!(
            r#"{{"type":"test","fromBlock":{FROM},"toBlock":{FROM},"includeAllBlocks":true,
                 "parentBlockHash":"{parent}","fields":{{"block":{{"number":true}}}}}}"#
        );
        let parsed = parse_query(query.as_bytes(), &catalog).unwrap();
        let plan = compile(&parsed, &catalog).unwrap();
        let reader = ParquetChunkReader::open(dir.path()).unwrap();
        execute_chunk(&plan, &catalog, &reader, false)
    };

    let Err(err) = answer("0xdead") else {
        panic!("the parent is five hundred numbers back, and known");
    };
    assert_eq!(error_kind(&err), Some(ErrorKind::UnexpectedBaseBlock));

    let reported = err
        .downcast_ref::<sqd_query_engine::output::UnexpectedBaseBlock>()
        .expect("the error must carry the refs a client rewinds with");
    let parent = reported
        .prev_blocks
        .last()
        .expect("prev_blocks must not be empty");
    assert_eq!(parent.number, PARENT, "the row states its own parent slot");
    assert_eq!(parent.hash, "0x1000");

    assert!(
        answer("0x1000").is_ok(),
        "the hash the row states must be accepted"
    );
}

/// A dataset whose block table declares no parent-hash column cannot answer the
/// question at all. Accepting the field and returning data is the reorg being
/// served silently, so the request is refused before a chunk is opened.
///
/// Covers CT-2 · INV-E5
#[test]
fn parent_block_hash_is_refused_where_the_catalog_cannot_answer_it() {
    use sqd_query_engine::metadata::parse_dataset_description;
    use sqd_query_engine::query::{compile, parse_query};

    let without_parent_hash = r#"
version: v2
name: test

tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
      hash: { type: string }
"#;
    let with_parent_hash = format!("{without_parent_hash}      parent_hash: {{ type: string }}\n")
        .replace(
            "    sort_key: [number]",
            "    sort_key: [number]\n    parent_hash_column: parent_hash",
        );

    let query = br#"{"type":"test","fromBlock":10,"parentBlockHash":"0xabcd"}"#;

    let silent = parse_dataset_description(without_parent_hash).unwrap();
    let parsed = parse_query(query, &silent).unwrap();
    let err = compile(&parsed, &silent).unwrap_err().to_string();
    assert!(
        err.contains("parentBlockHash"),
        "the refusal must name the field the client sent, got: {err}"
    );

    let answerable = parse_dataset_description(&with_parent_hash).unwrap();
    let parsed = parse_query(query, &answerable).unwrap();
    assert!(
        compile(&parsed, &answerable).is_ok(),
        "a catalog that declares the column must still accept the field"
    );
}

// ---------------------------------------------------------------------------
// `fromBlock` on a number the chain skipped
// ---------------------------------------------------------------------------

/// Solana skips slots, so a client that resumes at `lastBlock + 1` often names
/// a slot with no block. The first block after the gap states the parent the
/// client means — the block before the gap — and that row is the one compared.
/// The check used to require a row *at* `fromBlock`, find none, and serve the
/// data with no error. The reference answers `UnexpectedBaseBlock` here, with
/// this message.
///
/// Covers CT-2 · INV-E5
#[test]
#[ignore = "requires external fixture data"]
fn a_skipped_from_block_is_settled_by_the_block_after_the_gap() {
    if !fixture_tree_is_present() {
        return;
    }

    let solana = meta("solana");
    const PARENT: u64 = 217_710_447;
    const SKIPPED: u64 = 217_710_448;
    const NEXT: u64 = 217_710_449;

    let query = |parent: &str| {
        format!(
            r#"{{"type":"solana","fromBlock":{SKIPPED},"toBlock":{NEXT},"includeAllBlocks":true,
                 "parentBlockHash":"{parent}",
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .into_bytes()
    };

    let actual_parent = parent_hash_of("solana", &solana, NEXT);

    let err = run("solana", &solana, &query("not-the-parent"))
        .expect_err("a skipped fromBlock must not skip the check");
    assert_eq!(error_kind(&err), Some(ErrorKind::UnexpectedBaseBlock));

    let reported = err
        .downcast_ref::<sqd_query_engine::output::UnexpectedBaseBlock>()
        .expect("the error must carry the refs a client rewinds with");
    let parent = reported.prev_blocks.last().unwrap();
    assert_eq!(parent.number, PARENT, "the block before the gap");
    assert_eq!(parent.hash, actual_parent);
    assert_eq!(
        reported.to_string(),
        format!("unexpected base block: expected not-the-parent, but got {PARENT}#{actual_parent}"),
        "the reference's message, verbatim"
    );

    let body = run("solana", &solana, &query(&actual_parent))
        .expect("the hash of the block before the gap must be accepted");
    let blocks = parse_response(&body);
    assert_eq!(blocks[0]["header"]["number"], NEXT);
}

/// A client paging through the chunk resumes at `lastBlock + 1` after every
/// page, wherever the page ended. Each slot of the fixture chunk is such a
/// resume point, the skipped ones included: the hash of the last block before
/// it must be accepted, and any other hash reported with that block as the
/// chunk's answer.
///
/// Covers CT-2 · INV-E5
#[test]
#[ignore = "requires external fixture data"]
fn a_client_resuming_at_any_slot_is_checked() {
    if !fixture_tree_is_present() {
        return;
    }

    let solana = meta("solana");
    let body = run(
        "solana",
        &solana,
        br#"{"type":"solana","fromBlock":0,"includeAllBlocks":true,
             "fields":{"block":{"number":true,"hash":true}}}"#,
    )
    .unwrap();
    let blocks: Vec<(u64, String)> = parse_response(&body)
        .iter()
        .map(|b| {
            let header = &b["header"];
            let number = header["number"].as_u64().unwrap();
            let hash = header["hash"].as_str().unwrap().to_string();
            (number, hash)
        })
        .collect();

    let first = blocks[0].0;
    let last = blocks.last().unwrap().0;
    let skipped = (last - first + 1) as usize - blocks.len();
    assert!(skipped > 0, "the fixture chunk must skip slots");

    let query = |from: u64, parent: &str| {
        format!(
            r#"{{"type":"solana","fromBlock":{from},"toBlock":{from},"includeAllBlocks":true,
                 "parentBlockHash":"{parent}",
                 "fields":{{"block":{{"number":true}}}}}}"#
        )
        .into_bytes()
    };

    let mut parent = 0;
    for from in first + 1..=last {
        while blocks[parent + 1].0 < from {
            parent += 1;
        }
        let (parent_number, parent_hash) = &blocks[parent];

        run("solana", &solana, &query(from, parent_hash)).unwrap_or_else(|e| {
            panic!("fromBlock {from}: the hash of block {parent_number} must be accepted: {e:#}")
        });

        let err = run("solana", &solana, &query(from, "not-the-parent"))
            .expect_err("a wrong hash must be reported at every slot");
        let reported = err
            .downcast_ref::<sqd_query_engine::output::UnexpectedBaseBlock>()
            .unwrap_or_else(|| panic!("fromBlock {from}: not a fork error: {err:#}"));
        let answer = reported.prev_blocks.last().unwrap();
        assert_eq!(
            (answer.number, answer.hash.as_str()),
            (*parent_number, parent_hash.as_str()),
            "fromBlock {from}"
        );
    }
}

/// A Solana-shaped catalog: the block table states its parent's number, so the
/// chain may skip numbers.
const SKIPPING_CATALOG: &str = r#"
version: v2
name: test

tables:
  blocks:
    output:
      name: block
      fields: [number]
    block_number_column: number
    parent_hash_column: parent_hash
    parent_number_column: parent_number
    sort_key: [number]
    columns:
      number: { type: uint64 }
      parent_number: { type: uint64 }
      parent_hash: { type: string }
"#;

/// The hash the synthetic chain gives each block.
fn hash_of(block: u64) -> String {
    format!("0x{block:04x}")
}

/// How a synthetic chain is written: the parent number's physical width, how
/// many rows go in each row group, and whether the parent hash is there at all.
#[derive(Clone)]
struct Writing {
    parent_number: arrow::datatypes::DataType,
    rows_per_group: usize,
    parent_hash: bool,
}

impl Default for Writing {
    fn default() -> Self {
        Self {
            parent_number: arrow::datatypes::DataType::UInt64,
            rows_per_group: usize::MAX,
            parent_hash: true,
        }
    }
}

/// A block table of `(number, parent number)` rows, each stating its parent's
/// hash as [`hash_of`] the parent.
fn skipping_chain(blocks: &[(u64, u64)], writing: &Writing) -> tempfile::TempDir {
    use arrow::array::{ArrayRef, StringArray, UInt64Array};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    let mut fields = vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("parent_number", writing.parent_number.clone(), false),
    ];
    if writing.parent_hash {
        fields.push(Field::new("parent_hash", DataType::Utf8, false));
    }

    let groups = blocks
        .chunks(writing.rows_per_group.min(blocks.len()))
        .map(|rows| {
            let numbers: Vec<u64> = rows.iter().map(|(n, _)| *n).collect();
            let parents: Vec<u64> = rows.iter().map(|(_, p)| *p).collect();
            let parent_numbers =
                cast(&UInt64Array::from(parents.clone()), &writing.parent_number).unwrap();

            let mut columns = vec![
                Arc::new(UInt64Array::from(numbers)) as ArrayRef,
                parent_numbers,
            ];
            if writing.parent_hash {
                let hashes: Vec<String> = parents.iter().map(|p| hash_of(*p)).collect();
                columns.push(Arc::new(StringArray::from(hashes)) as ArrayRef);
            }
            columns
        })
        .collect();

    let dir = tempfile::tempdir().unwrap();
    write_table_row_groups(dir.path(), "blocks", fields, groups);
    dir
}

/// What the fork check made of one `(fromBlock, parentBlockHash)` pair.
#[derive(Debug, PartialEq)]
enum Verdict {
    Served,
    /// The block the chunk reported as the parent, and the hash it holds for it.
    Fork(u64, String),
}

fn verdict(chunk: &std::path::Path, from: u64, parent_hash: &str) -> Verdict {
    use sqd_query_engine::metadata::parse_dataset_description;

    let catalog = parse_dataset_description(SKIPPING_CATALOG).unwrap();
    let query = format!(
        r#"{{"type":"test","fromBlock":{from},"includeAllBlocks":true,
             "parentBlockHash":"{parent_hash}","fields":{{"block":{{"number":true}}}}}}"#
    );

    let err = match run_against(&catalog, chunk, &query) {
        Ok(_) => return Verdict::Served,
        Err(err) => err,
    };
    let Some(fork) = err.downcast_ref::<sqd_query_engine::output::UnexpectedBaseBlock>() else {
        panic!("fromBlock {from}: expected a fork or an answer, got: {err:#}");
    };
    let answer = fork
        .prev_blocks
        .last()
        .expect("prev_blocks must not be empty");
    Verdict::Fork(answer.number, answer.hash.clone())
}

/// Every `fromBlock` inside a gap names the same parent: the block before the
/// gap. That holds where the gap is wider than `P-FORK-WINDOW` and the window
/// behind `fromBlock` holds no block at all, at every width the parent number
/// is stored at, and whether the block after the gap shares a row group with
/// the one before it or not.
///
/// Covers CT-2 · INV-E5
#[test]
fn every_from_block_inside_a_gap_is_settled_by_the_block_after_it() {
    use arrow::datatypes::DataType;

    // 1001..=1499 are skipped, five times the window.
    let blocks = [
        (999, 998),
        (1000, 999),
        (1500, 1000),
        (1501, 1500),
        (1502, 1501),
    ];

    for parent_number in [
        DataType::UInt64,
        DataType::UInt32,
        DataType::Int64,
        DataType::Int32,
    ] {
        for rows_per_group in [1, 2, usize::MAX] {
            let writing = Writing {
                parent_number: parent_number.clone(),
                rows_per_group,
                ..Writing::default()
            };
            let chain = skipping_chain(&blocks, &writing);
            let at = |from: u64, hash: &str| verdict(chain.path(), from, hash);
            let case = format!("{parent_number} parent number, {rows_per_group} rows per group");

            for from in [1001, 1002, 1100, 1101, 1399, 1400, 1401, 1498, 1499, 1500] {
                assert_eq!(
                    at(from, &hash_of(1000)),
                    Verdict::Served,
                    "{case}, fromBlock {from}"
                );
                assert_eq!(
                    at(from, "0xdead"),
                    Verdict::Fork(1000, hash_of(1000)),
                    "{case}, fromBlock {from}"
                );
            }

            // Either side of the gap the row at `fromBlock` answers, as before.
            assert_eq!(
                at(1000, "0xdead"),
                Verdict::Fork(999, hash_of(999)),
                "{case}"
            );
            assert_eq!(
                at(1501, "0xdead"),
                Verdict::Fork(1500, hash_of(1500)),
                "{case}"
            );
            assert_eq!(at(1502, &hash_of(1501)), Verdict::Served, "{case}");
        }
    }
}

/// A gap that straddles the start of the chunk: the chunk's first block names a
/// parent that lives in the chunk before it. For a `fromBlock` inside the gap
/// that row is still the block after the parent, so this chunk answers. For a
/// `fromBlock` at or below the parent it is not, and the chunk stays quiet.
///
/// Covers CT-2 · INV-E5
#[test]
fn a_gap_across_the_start_of_the_chunk_is_settled_by_its_first_block() {
    let chain = skipping_chain(&[(1500, 1000), (1501, 1500)], &Writing::default());
    let at = |from: u64, hash: &str| verdict(chain.path(), from, hash);

    for from in [1001, 1200, 1499, 1500] {
        assert_eq!(
            at(from, &hash_of(1000)),
            Verdict::Served,
            "fromBlock {from}"
        );
        assert_eq!(
            at(from, "0xdead"),
            Verdict::Fork(1000, hash_of(1000)),
            "fromBlock {from}"
        );
    }

    for from in [0, 1, 999, 1000] {
        assert_eq!(
            at(from, "0xdead"),
            Verdict::Served,
            "fromBlock {from}: the chunk does not hold the block after its parent"
        );
    }
}

/// A gap that straddles the end of the chunk: the block after it is in the next
/// chunk, which answers. This one holds only blocks below `fromBlock`, and none
/// of them states what precedes it.
///
/// Covers CT-2 · INV-E5
#[test]
fn a_gap_across_the_end_of_the_chunk_is_left_to_the_next_chunk() {
    let chain = skipping_chain(&[(999, 998), (1000, 999)], &Writing::default());

    for from in [1001, 1050, 1100, 1101, 5000] {
        assert_eq!(
            verdict(chain.path(), from, "0xdead"),
            Verdict::Served,
            "fromBlock {from}"
        );
    }
    assert_eq!(
        verdict(chain.path(), 1000, "0xdead"),
        Verdict::Fork(999, hash_of(999))
    );
}

/// The genesis slot has no parent below it. The row at `fromBlock` answers there
/// as it always has, rather than the bound on the parent number wrapping.
///
/// Covers CT-2 · INV-E5
#[test]
fn a_from_block_of_zero_is_settled_by_the_row_at_zero() {
    let chain = skipping_chain(&[(0, 0), (2, 0)], &Writing::default());

    assert_eq!(verdict(chain.path(), 0, &hash_of(0)), Verdict::Served);
    assert_eq!(
        verdict(chain.path(), 0, "0xdead"),
        Verdict::Fork(0, hash_of(0))
    );
    assert_eq!(
        verdict(chain.path(), 1, "0xdead"),
        Verdict::Fork(0, hash_of(0))
    );
}

/// A chunk that holds the block after the gap owes the answer, and one that
/// cannot produce the parent hash must fail rather than serve the query. A
/// chunk that holds nothing at or after `fromBlock` is not asked.
///
/// Covers CT-2 · INV-E5
#[test]
fn a_chunk_holding_the_block_after_the_gap_must_carry_its_parent_hash() {
    use sqd_query_engine::metadata::parse_dataset_description;

    let writing = Writing {
        parent_hash: false,
        ..Writing::default()
    };
    let chain = skipping_chain(&[(1000, 999), (1500, 1000)], &writing);
    let catalog = parse_dataset_description(SKIPPING_CATALOG).unwrap();
    let answer = |from: u64| {
        let query = format!(
            r#"{{"type":"test","fromBlock":{from},"includeAllBlocks":true,
                 "parentBlockHash":"0xdead","fields":{{"block":{{"number":true}}}}}}"#
        );
        run_against(&catalog, chain.path(), &query)
    };

    let err = answer(1200).expect_err("the chunk holds the block that answers");
    assert_eq!(error_kind(&err), Some(ErrorKind::ColumnNotFound));

    assert!(
        answer(5000).is_ok(),
        "nothing at or after fromBlock, and nothing in the window behind it"
    );
}
