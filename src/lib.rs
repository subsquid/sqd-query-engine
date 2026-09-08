pub mod error;
pub(crate) mod integers;
pub mod join;
/// The catalog types and loader, a crate of their own in `crates/metadata`
/// so other projects can fetch them; re-exported under the module's old path.
pub use sqd_metadata as metadata;
pub mod output;
pub mod query;
pub mod scan;

#[cfg(test)]
mod testing;
