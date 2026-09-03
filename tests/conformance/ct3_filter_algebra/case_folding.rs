//! Case folding follows the column, not the filter.

use crate::harness::fixtures::{fixture_tree_is_present, meta, run};
use crate::harness::json::count_items;

/// Upper-casing a hex filter value must not change the answer, and it must not
/// matter whether the value arrives as a scalar or inside a list.
///
/// Covers CT-3 · INV-P8
#[test]
#[ignore = "requires external fixture data"]
fn hex_filters_fold_case_in_both_shapes() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    const ADDR_LOWER: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
    const ADDR_UPPER: &str = "0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48";

    let query = |addr: &str, as_list: bool| {
        let filter = if as_list {
            format!(r#""address": ["{addr}"]"#)
        } else {
            format!(r#""address": "{addr}""#)
        };
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                "fields":{{"log":{{"address":true,"logIndex":true}}}},
                "logs":[{{{filter}}}]}}"#
        )
        .into_bytes()
    };

    let list_lower = run("ethereum", &evm, &query(ADDR_LOWER, true)).unwrap();
    let list_upper = run("ethereum", &evm, &query(ADDR_UPPER, true)).unwrap();
    let scalar_lower = run("ethereum", &evm, &query(ADDR_LOWER, false)).unwrap();
    let scalar_upper = run("ethereum", &evm, &query(ADDR_UPPER, false)).unwrap();

    assert!(!list_lower.is_empty(), "fixture must match some logs");
    assert_eq!(list_lower, list_upper, "list filter must fold case");
    assert_eq!(
        list_lower, scalar_lower,
        "scalar and list filters must mean the same thing"
    );
    assert_eq!(scalar_lower, scalar_upper, "scalar filter must fold case");
}

/// Case folding must follow the column an alias *resolves to*, not the name the
/// client wrote. `evmLogs.address` reaches a system column on the substrate
/// events table, and a client sending a checksummed address gets a 200 with no
/// events — the shape of answer that means "this address emitted nothing".
///
/// Covers CT-3 · INV-P8
#[test]
#[ignore = "requires external fixture data"]
fn an_alias_folds_case_on_the_column_it_resolves_to() {
    if !fixture_tree_is_present() {
        return;
    }

    let substrate = meta("substrate");
    const ADDR_LOWER: &str = "0x00261a16442bc063573d2cbb0b5f398f9e1e14b9";
    const ADDR_UPPER: &str = "0x00261A16442BC063573D2CBB0B5F398F9E1E14B9";

    let query = |addr: &str| {
        format!(
            r#"{{"type":"substrate","fromBlock":0,
                 "fields":{{"block":{{"number":true}},"event":{{"name":true,"args":true}}}},
                 "evmLogs":[{{"address":["{addr}"]}}]}}"#
        )
        .into_bytes()
    };

    let lower = run("moonbeam", &substrate, &query(ADDR_LOWER)).unwrap();
    let upper = run("moonbeam", &substrate, &query(ADDR_UPPER)).unwrap();

    let found = count_items(&lower, "events");
    assert!(
        found > 0,
        "the fixture must carry EVM logs for this contract"
    );
    assert_eq!(
        found,
        count_items(&upper, "events"),
        "an alias filter must fold case like any other"
    );
    assert!(lower == upper, "the two responses must be identical");
}

/// Tron writes hex without the `0x` prefix, so its addresses are `utf8` on the
/// way out and the encoding cannot carry the folding rule. The catalog says it
/// separately, on the columns and through the alias extractions — otherwise a
/// checksummed address gets a 200 with no rows.
///
/// Covers CT-3 · INV-P8
#[test]
#[ignore = "requires external fixture data"]
fn bare_hex_columns_fold_case_too() {
    if !fixture_tree_is_present() {
        return;
    }

    let tron = meta("tron");
    const ADDR: &str = "a614f803b6fd780986a42c78ec9c7f77e6ded13c";
    const CONTRACT: &str = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c";

    let logs = |addr: &str| {
        format!(
            r#"{{"type":"tron","fromBlock":82644089,"toBlock":82644090,
                "fields":{{"log":{{"address":true,"logIndex":true}}}},
                "logs":[{{"address":["{addr}"]}}]}}"#
        )
        .into_bytes()
    };

    let lower = run("tron", &tron, &logs(ADDR)).unwrap();
    let upper = run("tron", &tron, &logs(&ADDR.to_ascii_uppercase())).unwrap();

    let found = count_items(&lower, "logs");
    assert!(found > 0, "the fixture must carry logs for this contract");
    assert_eq!(
        found,
        count_items(&upper, "logs"),
        "a bare-hex column must fold case"
    );
    assert!(lower == upper, "the two responses must be identical");

    // The same through an alias, which reaches a system extraction column.
    let calls = |contract: &str| {
        format!(
            r#"{{"type":"tron","fromBlock":82644089,"toBlock":82644089,
                "fields":{{"transaction":{{"hash":true}}}},
                "triggerSmartContractTransactions":[{{"contract":["{contract}"]}}]}}"#
        )
        .into_bytes()
    };

    let lower = run("tron", &tron, &calls(CONTRACT)).unwrap();
    let upper = run("tron", &tron, &calls(&CONTRACT.to_ascii_uppercase())).unwrap();

    let found = count_items(&lower, "transactions");
    assert!(found > 0, "the fixture must carry calls into this contract");
    assert_eq!(
        found,
        count_items(&upper, "transactions"),
        "an alias over a bare-hex column folds too"
    );
    assert!(lower == upper, "the two responses must be identical");
}

/// The rule is the column's encoding, not "lowercase everything": a non-hex
/// column still compares byte-exactly.
///
/// Covers CT-3 · INV-P8
#[test]
#[ignore = "requires external fixture data"]
fn non_hex_columns_are_not_folded() {
    if !fixture_tree_is_present() {
        return;
    }

    let evm = meta("evm");
    let query = |ty: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17881391,
                "fields":{{"trace":{{"type":true}}}},
                "traces":[{{"type":["{ty}"]}}]}}"#
        )
        .into_bytes()
    };

    let lower = count_items(&run("ethereum", &evm, &query("call")).unwrap(), "traces");
    let upper = count_items(&run("ethereum", &evm, &query("CALL")).unwrap(), "traces");

    assert!(lower > 0, "fixture must contain call traces");
    assert_eq!(
        upper, 0,
        "traces.type is not hex-encoded and must compare byte-exactly"
    );
}
