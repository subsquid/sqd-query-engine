//! Every emitted row is counted against the response budget.

use crate::harness::fixtures::{fixture_tree_is_present, meta, run};
use crate::harness::json::parse_response;

/// A field-group request key names its column indirectly: `callCallType` reads
/// `call_type`. Projection resolved that; the weight model did not, so the column
/// was emitted at a weight of zero and the response ran past the cap. Selecting
/// it must cost what selecting the same column under its own name costs.
#[test]
#[ignore = "requires external fixture data"]
fn a_field_group_request_key_weighs_what_its_column_weighs() {
    if !fixture_tree_is_present() {
        return;
    }
    let evm = meta("evm");

    let query = |field: &str| {
        format!(
            r#"{{"type":"evm","fromBlock":17881390,"toBlock":17882786,
                "fields":{{"trace":{{"{field}":true}}}},
                "traces":[{{}}]}}"#
        )
        .into_bytes()
    };

    let by_column = run("ethereum", &evm, &query("callType")).unwrap();
    let by_request_key = run("ethereum", &evm, &query("callCallType")).unwrap();

    assert_eq!(
        parse_response(&by_request_key).len(),
        parse_response(&by_column).len(),
        "the two names read the same column, so they must be trimmed at the same block"
    );
    assert!(
        by_request_key.len() as u64 <= 20 * 1024 * 1024,
        "a response the budget model did not count ran to {} bytes",
        by_request_key.len()
    );
}
