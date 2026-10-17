pub mod error;
mod execution;
pub mod integers;
pub mod join;
/// The catalog types and loader, a crate of their own in `crates/metadata`
/// so other projects can fetch them; re-exported under the module's old path.
pub use sqd_metadata as metadata;
pub mod output;
mod parallel;
pub mod query;
pub mod scan;
pub mod text;

#[cfg(test)]
mod testing;

#[cfg(test)]
#[global_allocator]
static ALLOCATOR: testing::Counting = testing::Counting;
