# sqd-metadata

The types a catalog loads into ([`src/types.rs`](src/types.rs)) and the checks
it must pass before the engine will use it ([`src/loader.rs`](src/loader.rs)).
A separate crate so that other projects can read a catalog without pulling in
the engine:

```toml
[dependencies]
sqd-metadata = { git = "https://github.com/subsquid/sqd-query-engine", package = "sqd-metadata" }
```

## Versioning

A catalog opens with `version`, a string, and this crate reads exactly one:
`v2`, the `SCHEMA_VERSION` constant. The loader refuses any other, so a
catalog written for a later schema fails at load rather than being misread.

Before introducing a new version, make sure it stays backwards compatible
with `v2`. A catalog is published once by its provider (the network
scheduler, for one) and read by several consumers, each on its own release
cycle. A version a `v2` reader cannot accept obliges the provider to publish
one catalog per version still in use, so prefer additive changes under `v2`:
a new optional key, a new `kind`, a new encoding. Reserve a bump for a change
that alters the meaning of what a `v2` reader already accepts.

A reader skips a key it does not know, which is what lets a catalog gain an
optional key without breaking readers that predate it. A misspelled optional
key is skipped the same way and changes query results, so a provider checks a
catalog with `parse_dataset_description_strict` (or
`load_dataset_description_strict`) before publishing it: it refuses every key
the release does not know and names each by its path.

The catalog format itself — the request, output and storage blocks of a table,
special filters, relations, variants, aliases — is documented next to the
catalogs, in [`metadata/README.md`](../../metadata/README.md). The catalogs stay
at the repository root; this crate's tests read them from there.
