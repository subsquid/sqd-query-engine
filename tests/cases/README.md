# Real-chunk cases

A case is a set of queries plus the network chunk they run on. Git keeps the queries and a reference to the chunk, never the chunk and never the answers. So a case costs a few kilobytes however big its chunk is.

```text
tests/cases/bench/evm/
  case.yaml             # chunk: s3://<dataset id>/<chunk path>
  usdc_transfers.json   # a query
  usdc_transfers.snap   # what its answer looked like
```

The dataset id is the `id` the network lists for the dataset, and the chunk path is the chunk's directory under it. The `bench/` cases are the benchmark queries on the chunks the benchmarks read.

## Running them

```sh
make fetch-cases   # download the chunks the cases name
make test-cases    # run every case against both engines
```

`make fetch-cases` uses the AWS CLI, so configure it for the chunk store first: credentials, and `AWS_ENDPOINT_URL_S3` if the store isn't AWS. Chunks go to `~/.cache/sqd-chunks`, and ones already there are skipped. Set `SQD_CHUNK_CACHE` to keep them somewhere else; the test reads the same variable.

Each query is a test named by its path, so `cargo test --features legacy-query --test cases -- --ignored bench/evm` runs one case. The harness takes a single filter, not several.

## What a case checks

With `--features legacy-query`, each query runs through this engine and the legacy one, and the answers must match. When they don't, the test prints the first difference, with the block, the path and both values, and writes both full answers under `target/tmp/cases/`.

Then the answer is checked against the snapshot beside the query: the block count, the items in each table, and a digest of the whole answer. The snapshot is what keeps a case useful without the legacy engine, because without the feature it's the only check. A new or changed answer fails until you accept it with `cargo insta review` or `INSTA_UPDATE=always`, so read the diff first.

When the engines differ on purpose, say so in `case.yaml`, and that query checks the snapshot only:

```yaml
legacy_differs:
  some_query: why the legacy answer is wrong
```

The test fails once the answers match again, so the note can't go stale.

## Comparing any query

Git ignores `tests/cases/local/`. Put a `case.yaml` and the queries there, in as many directories as you like, then:

```sh
make fetch-cases
cargo test --features legacy-query --test cases -- --ignored local/
```

To turn a bug report into a regression test, make a case from the reported query and chunk, fix the engine, and commit the case. If the cause is small, add a conformance test on a synthetic chunk as well. That one runs in CI, and a case doesn't.

## Limits

- The cases need access to the chunk store, so CI doesn't run them.
- A reference works as long as the store keeps that chunk. A dataset rewritten under a new id needs new references.
- A snapshot says that an answer changed, and in which table, but not where inside it. While the legacy engine exists, the live comparison shows that.
