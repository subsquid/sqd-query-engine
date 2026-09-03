//! CT-5 — blocks, weight, pagination.
//!
//! Three harness setups, so three modules: `budget` drives `P-WEIGHT-BUDGET`
//! (HC-6) over synthetic chunks written to make trimming observable, `scan` pins
//! the two scan entry points against each other on both a fixture chunk and a
//! partitioned synthetic one, and `weight` checks that a row the response
//! carries is a row the budget counted.

mod budget;
mod scan;
mod weight;
