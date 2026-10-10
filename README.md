# sqd-query-engine

Schema-agnostic, high-performance query engine for blockchain parquet data. New dataset types (EVM, Solana, Fuel, etc.)
are added via YAML metadata, not code.

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

Start with the [architecture reading guide](docs/architecture.md) for the inputs,
results, and source files of each stage. It also identifies where the executor
chooses between reading payloads immediately and deferring them until page
selection. The [specification](spec/README.md) defines the observable behavior.

### Key Design Decisions

- **Schema-agnostic**: Table schemas, relations, sort keys, and output encoding are all defined in YAML metadata (
  `metadata/evm.yaml`, `metadata/solana.yaml`). No chain-specific code.
- **Sort keys are filter-first, not block-first**: e.g. `program_id -> d1 -> b9 -> block_number -> tx_index`. Row group
  stats on filter columns are highly selective.
- **Column types may differ from metadata**: block_number is Int32 in EVM parquet (metadata says UInt64),
  instruction_address is List\<UInt16\> (metadata says List\<UInt32\>). All extractors handle multiple integer types.
- **mmap I/O**: `memmap2::Mmap` + `Bytes::from_owner()` — zero-copy, OS handles paging. Single mmap per file shared
  across all threads.

## Project Structure

```text
crates/metadata/ # sqd-metadata: catalog types, YAML loader, validation
src/
  lib.rs        # Public modules; re-exports sqd_metadata as metadata
  query/        # JSON parser and plan compiler
  scan/         # ChunkReader, Parquet and columnar readers, predicates, row identities
  join/         # Equality and hierarchical joins, including scan fallbacks
  output/       # Execution, projection, weight selection, materialization, JSON/Arrow
  error.rs      # Error kinds and classification
  integers.rs   # Integer-width-independent column access
  text.rs       # Shared text handling
metadata/       # Dataset YAML catalogs and catalog format documentation
spec/           # Behavioral contract, invariants, conformance matrix, decisions
docs/           # Architecture reading guide, weight model, design history
benches/        # Latency, throughput, memory, profiling and instruction-count benchmarks
tests/
  conformance/
    main.rs     # CT-1 through CT-9 modules (some classes have subdirectories)
    harness/    # Fixture loaders, runners, synthetic chunk writers
  e2e_fixtures.rs
  fixtures/     # Query/result JSON pairs per dataset; a link into the legacy repo
  reads.rs      # What each bench query reads, as snapshots
  snapshots/    # Those snapshots
  throughput.rs # How the throughput bench counts its rate
```

[Tests](#tests) says which kind of test goes where.

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

`None` means the requested range does not intersect the chunk. A query with no
matching items still returns the covered range's boundary blocks as headers.
`into_json_lines` buffers the complete answer. For incremental encoding, consume
`QueryOutput` with `has_next_block` and `write_next_block`, reusing a buffer and
adding a newline after each block. The selected page's Arrow batches are already
in memory when execution returns.

To reuse an open reader, call `output::execute_chunk` or
`output::execute_chunk_with` with a `scan::ChunkReader`. `metadata`, `query`, and
`output` expose the imports above through public re-exports; their implementation
modules such as `loader`, `parse`, and `assembly` are private.

## Query Format

Queries are JSON objects specifying block ranges, table filters, relations, and output fields:

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

- Multiple filter items per table are OR'd together
- Relations (e.g. `"transaction": true`) are scoped per request item — only items that declare a relation contribute to
  its join
- Field selection is global across all items of the same table type

## Benchmarks

Historical throughput measurements vs the legacy engine (median across query types).
See [BENCHMARKS.md](BENCHMARKS.md) for recorded results and the
[historical comparison](docs/history/engine-comparison.md) for context; these are
not a baseline for the current revision.

### x86_64: Intel Xeon E-2136 (6C/12T @ 3.3GHz), 64GB DDR4, Linux — prior run (pre-RPC query set)

| Median           | CPU=1           | CPU=4           | CPU=8           | CPU=12          |
|------------------|-----------------|-----------------|-----------------|-----------------|
| General queries  | **17% faster**  | **56% faster**  | **67% faster**  | **73% faster**  |
| Only full blocks | **485% faster** | **599% faster** | **522% faster** | **455% faster** |

### Apple M2 Pro (12-core), 32GB, macOS — current (RPC-inclusive query set)

Median throughput speedup vs legacy. RPC queries reproduce `eth_getBlockByNumber`
(full + tx-hashes), `eth_getBlockReceipts`, and `eth_getLogs` (1/10/100-block).

| Median          | CPU=1            | CPU=4            | CPU=8           | CPU=12          |
|-----------------|------------------|------------------|-----------------|-----------------|
| General queries | **17% faster**   | **27% faster**   | **40% faster**  | **45% faster**  |
| RPC queries     | **~2.9× faster** | **~2.0× faster** | **75% faster**  | **77% faster**  |

New engine is faster on **every** RPC call at every concurrency level
(1.1×–4.7×); single-thread RPC latency is 1.4×–4.6× faster.

```bash
cargo bench --bench latency               # latency (divan)
cargo bench --bench throughput -- --all    # throughput (all CPU levels)

# A/B vs the legacy engine on the same chunk (requires sibling ../data repo):
cargo bench --bench latency    --features legacy-query
cargo bench --bench throughput --features legacy-query -- --all
```

## Supported Datasets

- **EVM** (Ethereum, Optimism, Binance) — `metadata/evm.yaml`
- **Solana** — `metadata/solana.yaml`
- **Substrate** (Kusama, Moonbeam) — `metadata/substrate.yaml`
- **Tron** — `metadata/tron.yaml`
- **Bitcoin** — `metadata/bitcoin.yaml`
- **Hyperliquid Fills** — `metadata/hyperliquid_fills.yaml`
- **Hyperliquid Replica Commands** — `metadata/hyperliquid_replica_cmds.yaml`

## Tests

Pick the place for a test by what it checks and what data it needs.

| What the test checks | Where it goes | What it needs |
|---|---|---|
| One function or module | `#[cfg(test)] mod tests` in `src/` | Nothing, or the chunks in `data/` |
| A rule of the spec | `tests/conformance/ctN_*`, in its class | A small chunk the harness writes |
| Generated queries answer as the legacy engine does | `tests/conformance/ct7_differential.rs` | The fixture tree and `--features legacy-query` |
| A recorded query keeps its recorded answer | `tests/e2e_fixtures.rs` | The fixture tree |
| What each bench query reads | `tests/reads.rs` | The chunks in `data/` |
| CPU and heap cost of each bench query | `benches/instructions` | Linux and `valgrind`; see [BENCHMARKS.md](BENCHMARKS.md) |
| How `throughput` turns finished queries into a rate | `tests/throughput.rs` | Nothing |

### Where a new end-to-end test goes

Usually into the conformance suite. The class is the section of
`spec/08-conformance.md` that owns the rule you check: CT-3 for filters, CT-4
for relations, CT-5 for blocks and pages, CT-6 for output, and so on. A
conformance test writes its own small chunk with the harness (`synthetic`,
`evm_like`, `sol_like`), so it runs in every PR on any machine. And it names the
rule it pins:

```rust
/// Covers CT-6 · INV-O7
#[test]
fn a_selected_null_renders_as_null_at_every_integer_width() { ... }
```

`make spec-check` reads those tags back against the matrix in
`spec/08-conformance.md`. A test under the wrong class, a tag for an invariant
that doesn't exist, and a matrix row naming a test that's gone all fail the
build.

A fixture test is weaker. It says an answer didn't change, but when it fails it
doesn't say which rule broke. Write one when you need a real chunk and an answer
recorded from the legacy engine. The files don't go into this repo:
`tests/fixtures` is a link to `crates/query/fixtures` in the legacy repo, so a
new fixture is a commit there.

1. Put `query.json` in `tests/fixtures/<dataset>/queries/<name>/`.
2. Get `result.json` from the legacy engine with
   `cargo run --bin generate_fixtures --features legacy-query`. It fills only
   `ethereum` and `solana` directories named `prod_pattern_*` that have no
   `result.json` yet; for another name or dataset, extend it first.
3. Add a line such as `evm_fixture!(<name>);` to `tests/e2e_fixtures.rs`.

Don't rename `actual.temp.json` to `result.json`. The test writes that file
from this engine's output, so a fixture made from it compares the engine with
itself.

CT-7 builds its queries from the catalog, and there's no list there for a
hand-written query. A specific query compared live with the legacy engine has
no place yet.

### Running them

The chunks in `data/` and the fixture tree are not in git, so tests that need
them are `#[ignore]`d. CI runs the rest.

```bash
make test           # what CI runs; tests that need data show as ignored
make test-data      # only the tests that need data/ or tests/fixtures, CT-7 aside
make test-nightly   # CT-7 to CT-9, with the legacy engine
make spec-check     # the spec, the conformance matrix and the test tags agree

# Everything at once
SQD_REQUIRE_CHUNKS=1 SQD_REQUIRE_FIXTURES=1 \
  cargo test --workspace --features legacy-query -- --include-ignored
```

`SQD_REQUIRE_CHUNKS=1` and `SQD_REQUIRE_FIXTURES=1` turn a missing chunk into a
failure. Without them, a test whose data is missing returns early and passes,
which is fine on a laptop and useless as evidence.
