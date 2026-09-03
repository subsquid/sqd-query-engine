//! The conformance suite: [`spec/08-conformance.md`] turned into tests.
//!
//! One module per CT class, in the order the specification lists them. A test
//! lives in the module of a class it claims: a test pinning invariants from two
//! classes carries a tag line for each and may live in either, but not in a
//! third. `make spec-check` reads the tags back against the directory and fails
//! on a test filed under a class it does not claim, because class written in two
//! places and reconciled in neither is class written nowhere.
//!
//! Two axes cut across this and must not be confused with it:
//!
//! - **What a test needs.** Tests requiring the external fixture tree are
//!   `#[ignore]`d out of the portable suite and selected by `make test-data`.
//!   Tests requiring the reference implementation are behind `legacy-query`.
//! - **Which gate selects it.** MG-3 owns CT-1 – CT-6 and MG-4 owns CT-7 – CT-9,
//!   and each set is selected by its module prefix: `make test-pr` skips the
//!   nightly classes, `make test-nightly` names them. The split is by capability,
//!   not by cost — CT-9's proptests run in a tenth of a second and `make test`
//!   runs them per-PR, while CT-7 and CT-8 wait on inputs no job supplies yet.
//!
//! Fixture-comparison tests are deliberately *not* here. They stop regressions
//! rather than find gaps (§8.1), so they keep their own target,
//! `tests/e2e_fixtures.rs`.
//!
//! [`spec/08-conformance.md`]: ../../spec/08-conformance.md

mod harness;

mod ct1_catalog;
mod ct2_request;
mod ct3_filter_algebra;
mod ct4_relations;
mod ct5_blocks_weight;
mod ct6_output;
#[cfg(feature = "legacy-query")]
mod ct7_differential;
mod ct8_adversarial;
mod ct9_fuzz;
