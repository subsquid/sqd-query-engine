# Queries on real chunks

Each directory here holds queries, a snapshot of each answer, and a reference to the network chunk they run on. Git keeps the queries, the snapshots and the reference, never the chunk. So a directory stays small however big its chunk is.

```text
tests/queries/bench/evm/
  chunks.yaml           # chunk: s3://<dataset id>/<chunk path>, and its file hashes
  usdc_transfers.json   # a query
  usdc_transfers.snap   # what its answer looked like
```

The dataset id is the `id` the network lists for the dataset, and the chunk path is the chunk's directory under it. The `bench/` queries are the benchmark queries on the chunks the benchmarks read.

A directory named after a dataset, such as `ethereum/`, holds `chunk: fixture:ethereum` instead. That is the chunk of the fixture tree in the legacy repo, and the conformance tests that need a real chunk read it through this file. Its queries are the legacy repo's fixture queries, and each first snapshot was accepted only where the answer equalled the answer recorded there. When the store holds that chunk, the line becomes an `s3://` reference, and every test that reads the chunk moves with it.

## Running them

```sh
make fetch   # download the chunks the directories name
cargo test --features legacy-query --test queries -- --ignored
```

`make fetch` uses the AWS CLI, so configure it for the chunk store first: credentials, and `AWS_ENDPOINT_URL_S3` if the store isn't AWS. Chunks go to `~/.cache/sqd-chunks`, and ones already there are skipped. Set `SQD_CHUNK_CACHE` to keep them somewhere else; the test reads the same variable.

Each query is a test named by its path, so `cargo test --features legacy-query --test queries -- --ignored bench/evm` runs one directory. The harness takes a single filter, not several.

Selected queries fail if their chunks are missing from the cache. The portable suite leaves them ignored.

## Hashes

A reference says where a chunk is, not what it holds. A dataset can be rewritten under the same name, and a cached copy can be damaged. So `chunks.yaml` also gives the XXH3-128 hash of every file in the chunk:

```yaml
chunk: s3://<dataset id>/<chunk path>
xxh3:
  blocks.parquet: 24ea70dc…
  logs.parquet: 39278146…
```

XXH3 isn't a cryptographic hash. It catches a damaged or rewritten file, not a forged one, and it runs faster than a disk reads, so hashing the whole cache on every fetch takes no longer than reading it.

`make fetch` hashes every cached chunk against these lines, old ones too. A chunk whose files all match gets a `<chunk>.verified` file beside it, and the tests read a cached chunk only when that file matches the hashes `chunks.yaml` gives now. A changed, missing or extra file fails `make fetch` with the file named, and the tests refuse the chunk until it matches again.

For a new chunk, leave `xxh3` out. `make fetch` downloads the chunk, fails, and prints the lines to add. Check that the copy is the right one before you add them, because from then on the hashes are what the tests trust.

A `fixture:` chunk takes no hashes, since the legacy repo's LFS already pins it.

## What a query checks

With `--features legacy-query`, each query runs through this engine and the legacy one, and the answers must match. When they don't, the test prints the first difference, with the block, the path and both values, and writes both full answers under `target/tmp/queries/`.

Then the answer is checked against the snapshot beside the query: the block count, the items in each table, and the answer itself, with object keys sorted. An answer over 64 KB is too long to review, so its snapshot keeps a digest of it instead. The snapshot is what keeps a query useful without the legacy engine, because without the feature it's the only check. A new or changed answer fails until you accept it with `cargo insta review` or `INSTA_UPDATE=always`, so read the diff first.

When the engines differ on purpose, say so in `chunks.yaml`, and that query checks the snapshot only:

```yaml
legacy_differs:
  some_query: why the legacy answer is wrong
```

The test fails once the answers match again, so the note can't go stale.

## Comparing any query

Git ignores `tests/queries/local/`. Put a `chunks.yaml` and the queries there, in as many directories as you like, then:

```sh
make fetch   # the first time, prints the hashes to add to chunks.yaml
cargo test --features legacy-query --test queries -- --ignored local/
```

To turn a bug report into a regression test, put the reported query and chunk in a directory here, fix the engine, and commit the directory. If the cause is small, add a conformance test on a synthetic chunk as well. That one runs in CI, and a query here doesn't.

## Limits

- The queries need access to the chunk store, so CI doesn't run them.
- A reference works as long as the store keeps that chunk. A dataset rewritten under a new id needs new references.
- A digest says that an answer changed, and the counts say in which table, but not where inside it. While the legacy engine exists, the live comparison shows that.
