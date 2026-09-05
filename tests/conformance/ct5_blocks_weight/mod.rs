//! CT-5 — blocks, weight, pagination.
//!
//! Five harness setups, so five modules: `budget` drives `P-WEIGHT-BUDGET`
//! (HC-6) over synthetic chunks written to make trimming observable, `scan` pins
//! the two scan entry points against each other on both a fixture chunk and a
//! partitioned synthetic one, `weight` checks that a row the response carries is
//! a row the budget counted, `partition` splits a range in two and asserts the
//! halves concatenate back, and `oracle` runs generated queries with the budget
//! walk switched on and off and requires the same answer from both.

mod budget;
mod oracle;
mod partition;
mod scan;
mod weight;
