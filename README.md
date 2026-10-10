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
benches/        # Latency, throughput, memory, and profiling benchmarks
tests/
  conformance/
    main.rs     # CT-1 through CT-9 modules (some classes have subdirectories)
    harness/    # Fixture loaders, runners, synthetic chunk writers
  cases.rs
  cases/        # Queries plus a reference to the chunk they run on; see its README
  e2e_fixtures.rs
  fixtures/     # Query/result JSON pairs per dataset
```

Tests are laid out by conformance class, and each one that pins an invariant
carries a tag naming it:

```rust
/// Covers CT-2 · INV-Q10
#[test]
fn a_bloom_filter_takes_at_most_ten_values() { ... }
```

`make spec-check` reads those tags back against the traceability matrix in
`spec/08-conformance.md`, so a test filed under the wrong class, a tag naming an
invariant that does not exist, a matrix row claiming a test that carries no tag,
and a row naming a test that no longer exists are all build failures rather than
review comments.

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
cargo bench --bench profile    --features legacy-query -- rpc/getLogs --compare
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

```bash
# Unit tests, including the metadata crate
cargo test --workspace --lib

# E2E fixture tests (external fixtures required)
SQD_REQUIRE_FIXTURES=1 cargo test --test e2e_fixtures -- --ignored

# Portable workspace suite; external-data tests are explicitly ignored
cargo test --workspace

# Real-chunk cases against the legacy engine (chunk store access required)
make fetch-cases && make test-cases

# Specification and test-tag consistency
make spec-check
```
