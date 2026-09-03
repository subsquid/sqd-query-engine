//! The filter surface is closed: a column the catalog does not declare filterable
//! is not filterable, and an alias has its own surface rather than its table's.

use sqd_query_engine::error::{error_kind, ErrorKind};
use sqd_query_engine::query::parse_query;

use crate::harness::fixtures::meta;

/// Tables carry blooms, size counters and denormalised extractions. Resolving a
/// filter key against "any column of the table" exposed all of them, and made
/// the column list the public API: adding a column added a filter.
///
/// Covers CT-1 · INV-D9
/// Covers CT-2 · INV-Q6
/// Covers CT-3 · INV-P15
#[test]
fn undeclared_columns_are_not_filterable() {
    let evm = meta("evm");
    let rejected = [
        // System columns backing the weight model.
        (
            r#"{"type":"evm","fromBlock":0,"logs":[{"dataSize":[100]}]}"#,
            "data_size",
        ),
        // A real, emitted column that the reference does not let a client filter.
        (
            r#"{"type":"evm","fromBlock":0,"logs":[{"logIndex":[3]}]}"#,
            "log_index",
        ),
        (
            r#"{"type":"evm","fromBlock":0,"transactions":[{"gasUsed":["0x1"]}]}"#,
            "gas_used",
        ),
    ];
    for (json, column) in rejected {
        let table = evm.table("logs").unwrap();
        assert!(
            table.columns.contains_key(column)
                || evm
                    .table("transactions")
                    .unwrap()
                    .columns
                    .contains_key(column),
            "the test is only meaningful while '{column}' is a real column"
        );
        assert!(
            parse_query(json.as_bytes(), &evm).is_err(),
            "expected {json} to be rejected"
        );
    }
}

/// An alias is a narrower view of its table, and exposes its own filters rather
/// than inheriting everything the table accepts.
///
/// Covers CT-2 · INV-Q6
/// Covers CT-3 · INV-P15
#[test]
fn an_alias_has_its_own_filter_surface() {
    let substrate = meta("substrate");

    // `evmLogs` accepts the log filters it aliases...
    parse_query(
        br#"{"type":"substrate","fromBlock":0,"evmLogs":[{"address":["0xabcd"]}]}"#,
        &substrate,
    )
    .expect("an alias must accept the filters it declares");

    // ...but not `name`, which belongs to the underlying events table and is
    // already pinned by the alias's implicit predicate.
    assert!(
        parse_query(
            br#"{"type":"substrate","fromBlock":0,"evmLogs":[{"name":["Balances.Transfer"]}]}"#,
            &substrate,
        )
        .is_err(),
        "an alias must not inherit the table's whole filter surface"
    );

    // The table itself still accepts it.
    parse_query(
        br#"{"type":"substrate","fromBlock":0,"events":[{"name":["Balances.Transfer"]}]}"#,
        &substrate,
    )
    .expect("the underlying table keeps its own filters");
}

/// The same holds for relations, which used to be read as the union of the
/// alias's and its table's. An `Ethereum.transact` call has no `subcalls` in the
/// reference, and an alias that inherits one is not the narrower view it claims
/// to be.
///
/// Covers CT-2 · INV-Q6
/// Covers CT-3 · INV-P15
#[test]
fn an_alias_has_its_own_relation_surface() {
    let substrate = meta("substrate");

    for declared in [
        r#"{"type":"substrate","fromBlock":0,"ethereumTransactions":[{"stack":true}]}"#,
        r#"{"type":"substrate","fromBlock":0,"ethereumTransactions":[{"events":true}]}"#,
        r#"{"type":"substrate","fromBlock":0,"ethereumTransactions":[{"extrinsic":true}]}"#,
    ] {
        parse_query(declared.as_bytes(), &substrate)
            .expect("an alias must accept the relations it declares");
    }

    let err = parse_query(
        br#"{"type":"substrate","fromBlock":0,"ethereumTransactions":[{"subcalls":true}]}"#,
        &substrate,
    )
    .expect_err("an alias must not inherit its table's whole relation surface");
    assert_eq!(error_kind(&err), Some(ErrorKind::UnknownFilter));

    // The table itself still has it.
    parse_query(
        br#"{"type":"substrate","fromBlock":0,"calls":[{"subcalls":true}]}"#,
        &substrate,
    )
    .expect("the underlying table keeps its own relations");
}
