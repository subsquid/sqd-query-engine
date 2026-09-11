//! Dataset catalogs: the types a catalog loads into ([`types`]) and the checks
//! it must pass before the engine will use it ([`loader`]).
//!
//! The catalog format itself — the request, output and storage blocks of a
//! table, special filters, relations, variants, aliases — is documented next to
//! the catalogs the engine ships, in `metadata/README.md` at the repository root.
//!
//! [`types`]: crate::DatasetDescription
//! [`loader`]: crate::load_dataset_description

mod loader;
mod types;

pub use loader::*;
pub use types::*;
