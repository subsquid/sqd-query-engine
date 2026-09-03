//! CT-8 — adversarial chunks.
//!
//! A chunk written by an older archiver disagrees with today's catalog. The
//! failure that matters is the quiet one: a filter on a column the chunk does
//! not carry must be an error, because the alternative is a filter that matches
//! every row and a client that cannot tell.
//!
//! Only the dropped-column shape is reachable today; retyping, reordering and
//! adding a column need HC-3.

use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::output::execute_plan;
use sqd_query_engine::query::{compile, parse_query};

use crate::harness::chunk::chunk_without_column;
use crate::harness::fixtures::{fixture_tree_is_present, meta, run};
use crate::harness::json::count_items;

/// The single most dangerous silent failure available: a filter the engine
/// cannot evaluate stops narrowing the scan and starts matching everything, so
/// a query asking for four rows is answered with the whole chunk — and the
/// response gives the client no way to tell.
///
/// Covers CT-8 · INV-X3
#[test]
#[ignore = "requires external fixture data"]
fn filtering_an_absent_column_is_an_error() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let chunk = chunk_without_column("ethereum", "transactions", "sighash");
    let query = br#"{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                     "fields":{"transaction":{"transactionIndex":true}},
                     "transactions":[{"sighash":["0xa9059cbb"]}]}"#;

    let parsed = parse_query(query, &evm).unwrap();
    let plan = compile(&parsed, &evm).unwrap();
    let result = execute_plan(&plan, &evm, chunk.path());

    let err = match result {
        Err(e) => e,
        Ok(out) => {
            let items = count_items(
                &out.map(|o| o.into_json_lines()).unwrap_or_default(),
                "transactions",
            );
            panic!("filtering on an absent column must error; got {items} transactions instead");
        }
    };
    let message = err.root_cause().to_string();
    assert!(
        message.contains("sighash"),
        "the error must name the missing column, got: {message}"
    );
    assert_eq!(error_kind(&err), Some(ErrorKind::ColumnNotFound));
}

/// The check is about the chunk, not the catalog: with the column present the
/// same query is answered normally.
///
/// Covers CT-8 · INV-X3
#[test]
#[ignore = "requires external fixture data"]
fn filtering_a_present_column_still_works() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let body = run(
        "ethereum",
        &evm,
        br#"{"type":"evm","fromBlock":17881390,"toBlock":17881391,
             "fields":{"transaction":{"transactionIndex":true}},
             "transactions":[{"sighash":["0xa9059cbb"]}]}"#,
    )
    .unwrap();
    assert!(
        count_items(&body, "transactions") > 0,
        "fixture must contain ERC-20 transfers"
    );
}

/// Which of the two scan entry points a query lands on is decided by the table's
/// declared sort key, which no client can see. `transactions` leads with
/// `sighash` and takes the plain scan; `statediffs` leads with the block number
/// and takes the budget walk. The guarantee has to hold on both.
#[test]
#[ignore = "requires external fixture data"]
fn filtering_an_absent_column_is_an_error_on_a_block_sorted_table() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let chunk = chunk_without_column("ethereum", "statediffs", "address");
    let query = br#"{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                     "fields":{"stateDiff":{"key":true}},
                     "stateDiffs":[{"address":["0xdac17f958d2ee523a2206206994597c13d831ec7"]}]}"#;

    let parsed = parse_query(query, &evm).unwrap();
    let plan = compile(&parsed, &evm).unwrap();

    let err = match execute_plan(&plan, &evm, chunk.path()) {
        Err(e) => e,
        Ok(out) => {
            let items = count_items(
                &out.map(|o| o.into_json_lines()).unwrap_or_default(),
                "stateDiffs",
            );
            panic!("filtering on an absent column must error; got {items} state diffs instead");
        }
    };
    assert!(
        err.root_cause().to_string().contains("address"),
        "the error must name the missing column, got: {}",
        err.root_cause()
    );
}

/// The rule reaches a filter that resolves through an alias, which is where a
/// chunk older than the catalog actually shows up: `reviveContractEmitted` reads
/// extraction columns no chunk in the fixture tree carries. Answering that with
/// every `Revive.ContractEmitted` event in range would be the widest possible
/// answer to the narrowest possible filter, and no client could tell.
///
/// Covers CT-8 · INV-X3
#[test]
#[ignore = "requires external fixture data"]
fn an_alias_filter_on_a_column_the_chunk_lacks_is_an_error() {
    if !fixture_tree_is_present() {
        return;
    }

    let substrate = meta("substrate");

    // Without the filter the alias reads only `name`, which every chunk has.
    run(
        "moonbeam",
        &substrate,
        br#"{"type":"substrate","fromBlock":0,
             "fields":{"event":{"name":true}},
             "reviveContractEmitted":[{}]}"#,
    )
    .expect("an alias whose implicit predicate the chunk can answer is answerable");

    let err = run(
        "moonbeam",
        &substrate,
        br#"{"type":"substrate","fromBlock":0,
             "fields":{"event":{"name":true}},
             "reviveContractEmitted":[{"contract":["0xdead"]}]}"#,
    )
    .expect_err("filtering on a column the chunk lacks must error");

    assert!(
        err.root_cause().to_string().contains("_revive_contract"),
        "the error must name the missing column, got: {}",
        err.root_cause()
    );
}

/// Query items are alternatives, but the reference implementation still refuses
/// the whole request when any one of them names a column the chunk lacks —
/// verified against it directly. Pinned because "make the unanswerable item match
/// nothing instead" reads like the kinder behaviour and would silently diverge.
///
/// Covers CT-8 · INV-X3
#[test]
#[ignore = "requires external fixture data"]
fn one_unanswerable_item_rejects_the_whole_request() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let chunk = chunk_without_column("ethereum", "traces", "reward_author");
    let query = br#"{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                     "fields":{"trace":{"transactionIndex":true,"type":true}},
                     "traces":[{"type":["call"]},
                               {"rewardAuthor":["0xdead000000000000000000000000000000000000"]}]}"#;

    let parsed = parse_query(query, &evm).unwrap();
    let plan = compile(&parsed, &evm).unwrap();
    let err = match execute_plan(&plan, &evm, chunk.path()) {
        Err(e) => e,
        Ok(_) => panic!("an item naming an absent column must reject the request"),
    };

    assert!(
        err.root_cause().to_string().contains("reward_author"),
        "the error must name the missing column, got: {}",
        err.root_cause()
    );
}
