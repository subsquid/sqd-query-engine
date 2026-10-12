# sqd-query-engine

sqd-query-engine is a query engine for blockchain data in parquet files. The
engine does not contain code for a specific chain. A YAML catalog describes each
dataset: the tables, the relations, the sort keys and the output encoding. To add
a dataset, write a catalog. A change to the engine code is not necessary.

## Architecture

```text
JSON bytes + DatasetDescription -> parse_query -> Query -> compile -> Plan
Plan + ChunkReader -> validate + choose read strategy
                   -> scan complete block ranges + expand relations
                   -> select whole blocks by weight
                   -> read selected payloads when deferred
                   -> QueryOutput -> encode one JSON block on demand
                   -> ArrowOutput (alternative, buffered IPC)
```

The [architecture guide](docs/architecture.md) gives the inputs, the results and
the source files of each stage. It also shows where the executor decides when to
read the payloads: with the scan, or after the page selection. The
[specification](spec/README.md) gives the observable behavior of the engine.

### Design decisions

- **No chain-specific code.** The YAML catalogs in `metadata/` give the table
  schemas, the relations, the sort keys and the output encoding. The
  [catalog guide](metadata/README.md) gives the format.
- **The sort key starts with the filter columns.** For example, the sort key of
  the Solana `instructions` table is
  `program_id -> d1 -> b9 -> block_number -> transaction_index`. Because of this
  order, each row group holds a small range of filter values. The engine uses the row group
  statistics to skip the row groups that cannot match.
- **The parquet type of an integer column can be different from the catalog
  type.** The catalog gives the logical type, and a chunk can keep the column at
  a smaller width. For example, the EVM `block_number` is `uint64` in the
  catalog and `Int32` in parquet. The Solana `instruction_address` is
  `list_uint32` in the catalog and `List<UInt16>` in parquet. The engine reads
  all the widths that `src/integers.rs` lists.
- **mmap I/O.** The reader maps each parquet file into memory with
  `memmap2::Mmap` and wraps the map in `Bytes::from_owner()`. The reader does
  not copy the file, and the operating system loads the pages when the engine
  reads them. All threads use one map for each file.

## Project structure

```text
crates/metadata/ # sqd-metadata: catalog types, YAML loader, validation
src/
  lib.rs        # Public modules; re-exports sqd_metadata as metadata
  query/        # JSON parser and plan compiler
  scan/         # ChunkReader, Parquet and columnar readers, predicates, row identities
  join/         # Equality and hierarchical joins, with scan fallbacks
  execution/    # Plan checks, page selection, deferred payload reads (private)
  output/       # Response tables, block weight, fork check, JSON and Arrow encoders
  error.rs      # Error kinds and their classes
  integers.rs   # Integer column access at all widths
  text.rs       # Shared text functions
  testing.rs    # Test helpers: bench chunks, allocation counter, proptest seeds
metadata/       # Dataset YAML catalogs and the catalog format guide
spec/           # Behavior contract, invariants, conformance matrix, decisions
docs/           # Architecture guide, weight model, design history
chunk-registry.yaml # The location and the file hashes of each real chunk
benches/
  queries/      # The bench workload, in the format of tests/queries/
  instructions/ # Instruction and heap counts of each bench query, under Valgrind
  throughput/   # Queries per second at a set number of threads
tests/
  conformance/
    main.rs     # CT-1 to CT-9 modules (some classes have subdirectories)
    harness/    # Fixture loaders, runners, synthetic chunk writers, the chunk store
  queries.rs    # Runs each query of tests/queries/ and benches/queries/
  queries/      # Queries, snapshots of their answers and reads, chunk names; see its README
  fixtures/     # The fixture chunks per dataset; a link into the legacy repo, not in git
  throughput.rs # Checks that the throughput bench times each query that it counts
```

[Tests](#tests) tells you where each type of test goes.

## Usage

```rust
use std::path::Path;

use anyhow::Result;
use sqd_query_engine::metadata::load_dataset_description;
use sqd_query_engine::output::execute_plan;
use sqd_query_engine::query::{compile, parse_query};

pub fn query_json_lines(query_json: &[u8], chunk_dir: &Path) -> Result<Vec<u8>> {
    let metadata = load_dataset_description(Path::new("metadata/evm.yaml"))?;
    let query = parse_query(query_json, &metadata)?;
    let plan = compile(&query, &metadata)?;
    let output = execute_plan(&plan, &metadata, chunk_dir)?;
    Ok(output.map(|blocks| blocks.into_json_lines()).unwrap_or_default())
}
```

`execute_plan` returns `None` when the block range of the query is outside the
chunk. A query without matches also returns two blocks as headers only: the
first and the last block of the query range inside the chunk.

`into_json_lines` keeps the full answer in memory. To encode one block at a
time, use `QueryOutput` directly:

1. Make one buffer.
2. Call `has_next_block`. If it returns `false`, stop.
3. Call `write_next_block`. It adds the next block to the buffer.
4. Add a newline to the buffer.
5. Send the buffer to its destination.
6. Clear the buffer, and go back to step 2.

The Arrow batches of the selected page are in memory when the execution
function returns. Only the JSON encoding occurs later.

To use a reader that is already open, call `output::execute_chunk` or
`output::execute_chunk_with` with a `scan::ChunkReader`. The modules
`metadata`, `query` and `output` give the imports above as public re-exports.
Their implementation modules, for example `loader`, `parse` and `execution`, are
private.

## Query format

A query is a JSON object. It gives the block range, the table filters, the
relations and the output fields:

```json
{
  "type": "evm",
  "fromBlock": 17881390,
  "toBlock": 17881400,
  "logs": [
    {
      "topic0": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
      ],
      "transaction": true
    }
  ],
  "fields": {
    "log": {
      "address": true,
      "topics": true,
      "data": true
    },
    "transaction": {
      "from": true,
      "sighash": true
    }
  }
}
```

- A table can have many filter items. A row is in the answer if it matches one
  item or more.
- A relation, for example `"transaction": true`, applies only to the item that
  declares it. Only the rows of that item bring their related rows.
- The field selection of a table applies to all the items of that table.

The [request chapter](spec/02-request.md) of the specification gives the full
format.

## Benchmarks

The bench workload is in `benches/queries/`. Download its chunks before the
first run.

```bash
make fetch                         # the chunks, one time
cargo bench --bench throughput     # queries per second at 8 threads
cargo bench --bench instructions   # instruction and heap counts; Linux and Valgrind only
```

[BENCHMARKS.md](BENCHMARKS.md#how-to-run) gives the options. It also tells you
how to compare two builds.

## Supported datasets

- **EVM** (for example Ethereum, Optimism, Binance, Monad, Tempo): `metadata/evm.yaml`
- **Solana**: `metadata/solana.yaml`
- **Substrate** (for example Kusama, Moonbeam): `metadata/substrate.yaml`
- **Tron**: `metadata/tron.yaml`
- **Bitcoin**: `metadata/bitcoin.yaml`
- **Hyperliquid fills**: `metadata/hyperliquid_fills.yaml`
- **Hyperliquid replica commands**: `metadata/hyperliquid_replica_cmds.yaml`

## Tests

Select the location of a test from what it checks and the data that it uses.

| What the test checks | Location | Data |
|---|---|---|
| One function or module | `#[cfg(test)] mod tests` in `src/` | None, or the bench chunks from `make fetch` |
| A rule of the specification | `tests/conformance/ctN_*`, in its class | A small chunk that the harness writes |
| Generated queries give the same answers as the legacy engine | `tests/conformance/ct7_differential.rs` | The fixture tree and `--features legacy-query` |
| A query on a real chunk keeps its answer and its reads | `tests/queries/` | The chunk; see [the query guide](tests/queries/README.md) |
| The same, for a query of the bench workload | `benches/queries/` | The same |
| The CPU and heap cost of each bench query | `benches/instructions` | Linux and `valgrind`; see [BENCHMARKS.md](BENCHMARKS.md) |
| How `throughput` times the queries that run at the same time | `tests/throughput.rs` | None |

### The location of a new end-to-end test

Usually, put the test in the conformance suite. Its class is the section of
`spec/08-conformance.md` that owns the rule that you check. For example, CT-3 is
for filters, CT-4 for relations, CT-5 for blocks and pages, and CT-6 for output.
A conformance test writes its own small chunk with the harness (`synthetic`,
`evm_like`, `sol_like`). Because of this, the test runs in each PR, on all
machines. The
test also gives the rule that it checks:

```rust
/// Covers CT-6 · INV-O7
#[test]
fn a_selected_null_renders_as_null_at_every_integer_width() { ... }
```

`make spec-check` compares these tags with the matrix in
`spec/08-conformance.md`. The build fails in these conditions:

- A test is in the wrong class.
- A tag gives an invariant that does not exist.
- A matrix row gives a test that does not exist.

A query on a real chunk is a weaker test. It shows if an answer changes. When
it fails, it does not show which rule broke. Write one only when a real chunk is
necessary. CT-7 makes its queries from the catalog, but you select the query of
this test yourself.

To add a query on a real chunk:

1. Put the query in `tests/queries/<dataset>/<name>.json`, next to the
   `chunks.yaml` that names the chunk.
2. Run `cargo test --test queries -- --ignored tests/queries/<dataset>/<name>`.
   The new snapshot makes the test fail.
3. Examine the snapshot.
4. Accept it with `cargo insta review`.

[The query guide](tests/queries/README.md) gives the full procedure.

### Property tests

proptest keeps the cases that it finds in a `.proptest-regressions` file next to
the test. It runs these cases again before it makes new cases.
`seeds_beside_source()` in `src/testing.rs` puts the file there. Without it,
proptest puts the file in a directory at the crate root.

A seed gives the same case again only while the strategy that made it does not
change. Because of this, when proptest finds a bug, also pin the case that it
printed as a named test. This test calls the same check with that input.

### Run the tests

Git does not keep the real chunks. `make fetch` downloads the stored chunks into
a cache. The fixture tree is a link into the legacy repo. The tests that use
these chunks have the `#[ignore]` attribute. CI runs the other tests.

```bash
make fetch          # download the chunks that chunk-registry.yaml names, and check their hashes
make test           # the CI suite; the tests that use external data show as ignored
make test-data      # the tests that use external data, but not CT-7
make test-nightly   # CT-7 to CT-9, with the legacy engine
make spec-check     # check that the spec, the conformance matrix and the test tags agree

# All the tests
SQD_REQUIRE_CHUNKS=1 \
  cargo test --workspace --features legacy-query -- --include-ignored
```

With `SQD_REQUIRE_CHUNKS=1`, a test fails if its chunk is not in the cache.
Without it, a test without its data stops early and passes. This is satisfactory
on a laptop, but it does not prove anything.

A query of `tests/queries/` or `benches/queries/` that you run always fails if
its chunk is not in the cache. The test compares the answer with the legacy engine
only in a build with `--features legacy-query`, as in the command above.
