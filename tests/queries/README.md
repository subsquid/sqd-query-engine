# Queries on real chunks

Each directory here holds queries, two snapshots of each one, and the names of the chunks they run on. Git keeps the queries, the snapshots and the names, never the chunks. So a directory stays small however big its chunks are.

```text
tests/queries/ethereum/
  chunks.yaml           # chunks: [ethereum]
  some_query.json       # a query
  some_query.snap       # what its answer looked like
  some_query.reads.snap # what it read from the chunk
```

The bench workload lives in `benches/queries/` in the same format, and the same runner checks it. A directory there can name several chunks, and then each query runs on each of them, with snapshots named after the chunk:

```text
benches/queries/evm/selective/
  chunks.yaml                          # chunks: [evm-small, evm-big]
  usdc_transfers.json
  usdc_transfers.evm-small.snap
  usdc_transfers.evm-small.reads.snap
  usdc_transfers.evm-big.snap
  usdc_transfers.evm-big.reads.snap
```

## Chunks

The names come from `chunk-registry.yaml` at the repository root, which describes every real chunk once:

```yaml
evm-small:
  ref: s3://<dataset id>/<chunk path>
  xxh3:
    blocks.parquet: 24ea70dc…
    logs.parquet: 39278146…
ethereum:
  ref: fixture:ethereum
```

An `s3://` reference is a chunk in the chunk store. The dataset id is the `id` the network lists for the dataset, and the chunk path is the chunk's directory under it.

A `fixture:` reference is the chunk of the fixture tree in the legacy repo. The directories named after a dataset, such as `ethereum/`, run on those, and the conformance tests that need a real chunk find them by the same name. Their queries are the legacy repo's fixture queries, and each first snapshot was accepted only where the answer equalled the answer recorded there. When the store holds that chunk, its reference becomes an `s3://` one, and every test that reads the chunk moves with it.

The unit tests and the benches find their chunks here too, so there is no other copy of a chunk to keep in step.

## Running them

```sh
make fetch   # download the chunks chunk-registry.yaml names
cargo test --features legacy-query --test queries -- --ignored
```

`make fetch` uses the AWS CLI, so configure it for the chunk store first: credentials, and `AWS_ENDPOINT_URL_S3` if the store isn't AWS. Chunks go to `~/.cache/sqd-chunks`, and ones already there are skipped. Set `SQD_CHUNK_CACHE` to keep them somewhere else; the tests read the same variable.

Each query is a test named by its path from the repository root, with the chunk's name after a dot when its directory names several. So `cargo test --features legacy-query --test queries -- --ignored benches/queries/evm` runs the EVM bench queries. The harness takes a single filter, not several.

Selected queries fail if their chunks are missing from the cache. The portable suite leaves them ignored.

## Hashes

A reference says where a chunk is, not what it holds. A dataset can be rewritten under the same name, and a cached copy can be damaged. So the registry also gives the XXH3-128 hash of every file of a stored chunk.

XXH3 isn't a cryptographic hash. It catches a damaged or rewritten file, not a forged one, and it runs faster than a disk reads, so hashing the whole cache on every fetch takes no longer than reading it.

`make fetch` hashes every cached chunk against these lines, old ones too. A chunk whose files all match gets a `<chunk>.verified` file beside it, and the tests read a cached chunk only when that file matches the hashes the registry gives now. A changed, missing or extra file fails `make fetch` with the file named, and the tests refuse the chunk until it matches again.

For a new chunk, leave `xxh3` out. `make fetch` downloads the chunk, fails, and prints the lines to add. Check that the copy is the right one before you add them, because from then on the hashes are what the tests trust.

A `fixture:` chunk takes no hashes, since the legacy repo's LFS already pins it.

## What a query checks

With `--features legacy-query`, each query runs through this engine and the legacy one, and the answers must match. When they don't, the test prints the first difference, with the block, the path and both values, and writes both full answers under `target/tmp/queries/`.

Then the answer is checked against its snapshot: the block count, the items in each table, and the answer itself, with object keys sorted. An answer over 64 KB is too long to review, so its snapshot keeps a digest of it instead. The snapshot is what keeps a query useful without the legacy engine, because without the feature it's the only check.

The reads snapshot lists every scan the query asks the chunk reader for: the table, the block range, the columns, the filters and the rows that came back. It does so three times, for the default page, a page of 256 KiB and Arrow output. So a change to what a query reads shows in review even when the answer stays the same. A query the engine rejects before reading has none.

A new or changed snapshot fails until you accept it with `cargo insta review` or `INSTA_UPDATE=always`, so read the diff first.

When the engines differ on purpose, say so in the directory's `chunks.yaml`, and that query checks the snapshots only:

```yaml
legacy_differs:
  some_query: why the legacy answer is wrong
```

The test fails once the answers match again, so the note can't go stale.

## Comparing any query

Git ignores `tests/queries/local/`. Put a `chunks.yaml` and the queries there, in as many directories as you like. If the chunk isn't in the registry yet, add it there, and leave that entry out of your commits unless a committed query uses it. Then:

```sh
make fetch   # the first time, prints the hashes to add to chunk-registry.yaml
cargo test --features legacy-query --test queries -- --ignored local/
```

To turn a bug report into a regression test, put the reported query in a directory here, add its chunk to the registry, fix the engine, and commit both. If the cause is small, add a conformance test on a synthetic chunk as well. That one runs in CI, and a query here doesn't.

## Limits

- The queries need access to the chunk store, so CI doesn't run them.
- A reference works as long as the store keeps that chunk. A dataset rewritten under a new id needs new references.
- A digest says that an answer changed, and the counts say in which table, but not where inside it. While the legacy engine exists, the live comparison shows that.
