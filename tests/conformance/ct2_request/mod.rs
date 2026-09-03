//! CT-2 — request validation.
//!
//! Table-driven, one case per row of §6.2, asserting the error *kind* rather
//! than its message. The cases that matter are the ones an engine gets wrong by
//! being permissive: a value it coerces instead of refusing is a client shipping
//! against behaviour the contract does not promise.

mod bounds;
mod fields;
mod fork;
mod kinds;
mod surface;
mod values;
