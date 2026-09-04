//! CT-3 — filter algebra.
//!
//! The laws of §4.2 over the filter surface. Two of them are worth writing by
//! hand however much of the rest gets generated, because they encode the two
//! catastrophic misreadings: an empty list that matches everything, and a filter
//! that silently no-ops on a column the chunk does not carry.
//!
//! The generated half is `laws`: §8.4's table over queries HC-4 composed out of
//! the catalog and the chunk's own contents.

mod case_folding;
mod laws;
mod pruning;
mod surface;
mod values;
