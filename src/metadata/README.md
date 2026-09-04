# Catalog loading

The types a catalog loads into ([`types.rs`](types.rs)) and the checks it must
pass before the engine will use it ([`loader.rs`](loader.rs)).

The catalog format itself — the request, output and storage blocks of a table,
special filters, relations, variants, aliases — is documented next to the
catalogs, in [`metadata/README.md`](../../metadata/README.md).
