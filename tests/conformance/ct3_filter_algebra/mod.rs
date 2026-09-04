//! CT-3 — filter algebra.
//!
//! The laws of §4.2 over the filter surface. Two of them are worth writing by
//! hand however much of the rest gets generated, because they encode the two
//! catastrophic misreadings: an empty list that matches everything, and a filter
//! that silently no-ops on a column the chunk does not carry.
//!
//! The generated half needs HC-4, the catalog-walking query generator, which
//! does not exist yet.

mod case_folding;
mod pruning;
mod surface;
mod values;
