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

A catalog is published once by its provider (the network scheduler, for one)
and read by consumers on their own release cycles. Prefer additions under
`v2` that older readers can safely ignore, such as optional keys whose absence
preserves existing behavior. Adding a new `kind`, encoding, or column type
is not forward compatible: older readers reject unknown enum values even
when the catalog still declares `v2`. Using those values requires upgrading
all readers or publishing a separate catalog for older consumers. Changes
that alter existing meanings or require a new schema version likewise need
catalogs for each version still in use; a `v2` reader rejects other versions.

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
