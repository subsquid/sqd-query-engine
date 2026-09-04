//! INV-P16 — a filter's row set does not depend on what the scan could skip.
//!
//! Row-group statistics are an optimisation: they let a scan decline to read a
//! row group whose range cannot hold a match. A chunk written without them
//! leaves the scan nothing to decline on, so it must reach the same rows the
//! long way. Where the two disagree, the fast path is dropping matches — and a
//! dropped match looks exactly like a row that was never there.
//!
//! The same argument covers the other things a writer can turn off: page
//! boundaries, the dictionary, and the row-group size that decides how much any
//! of them can skip.
//!
//! The filters vary rather than the query, because this is a claim about
//! filters: one that matches, one that matches nothing, and one whose empty list
//! matches nothing by construction.

use crate::harness::chunk::{chunk_relaid, Layout};
use crate::harness::evm_like;
use crate::harness::fixtures::answers_the_same;

/// Covers CT-3 · INV-P16
#[test]
fn a_filter_returns_the_same_rows_with_nothing_to_prune_on() {
    let catalog = evm_like::catalog();
    let source = evm_like::chunk();

    let present = evm_like::address(evm_like::PRESENT_ADDRESS);
    let absent = evm_like::address(evm_like::ABSENT_ADDRESS);
    let topic = evm_like::word(evm_like::PRESENT_TOPIC);

    let filters = [
        (
            "a matching address",
            format!(r#"{{"address":["{present}"]}}"#),
        ),
        (
            "an address the chunk lacks",
            format!(r#"{{"address":["{absent}"]}}"#),
        ),
        (
            "an address and a topic together",
            format!(r#"{{"address":["{present}"],"topic0":["{topic}"]}}"#),
        ),
        ("an empty list", r#"{"address":[]}"#.to_string()),
        ("no filter at all", "{}".to_string()),
    ];

    // Statistics removed, so nothing can be skipped; and one row per row group,
    // so nearly everything can be. A filter that survives both reads the same
    // rows however much of the chunk the scan chose to look at.
    let layouts = [
        (
            "with no statistics to prune on",
            Layout::without_statistics(),
        ),
        ("with one row per row group", Layout::row_groups(1)),
        ("with one row per data page", Layout::pages(1)),
        (
            "with no dictionary to evaluate against",
            Layout::without_dictionary(),
        ),
    ];

    for (what_filter, item_request) in &filters {
        let query = evm_like::query_with(103, 113, item_request);

        for (what_layout, layout) in &layouts {
            let rewritten = chunk_relaid(source.path(), layout);
            answers_the_same(
                &catalog,
                &query,
                source.path(),
                rewritten.path(),
                &format!("{what_filter} {what_layout}"),
            );
        }
    }
}
