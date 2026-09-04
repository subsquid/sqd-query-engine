//! CT-5 — blocks, weight, pagination.
//!
//! Four harness setups, so four modules: `budget` drives `P-WEIGHT-BUDGET`
//! (HC-6) over synthetic chunks written to make trimming observable, `scan` pins
//! the two scan entry points against each other on both a fixture chunk and a
//! partitioned synthetic one, `weight` checks that a row the response carries is
//! a row the budget counted, and `partition` splits a range in two and asserts
//! the halves concatenate back.

mod budget;
mod partition;
mod scan;
mod weight;
