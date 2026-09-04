//! Machinery the CT classes share — the harness capabilities of §8.13.
//!
//! Anything used by more than one class belongs here, and the guards especially:
//! a skip guard that exists in several copies is a skip guard that will disagree
//! with itself, and §8.1 names silent skips as the most common way a conformance
//! suite lies.

pub mod chunk;
pub mod evm_like;
pub mod fixtures;
pub mod generator;
pub mod guard;
pub mod json;
pub mod synthetic;
