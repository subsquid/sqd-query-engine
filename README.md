# sqd-query-engine

Schema-agnostic, high-performance query engine for blockchain parquet data. New dataset types (EVM, Solana, Fuel, etc.)
are added via YAML metadata, not code.

## Architecture

```
JSON query -> parse (metadata-driven) -> compile -> Plan
Plan -> parallel parquet scan (mmap, RowFilter pushdown)
     -> KeyFilter + HierarchicalFilter relation scans
     -> join (semi_join / lookup_join / find_children / find_parents)
     -> block grouping + sequential JSON output
```

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

```
src/
  metadata/     # YAML metadata loader, dataset description types
  query/        # JSON query parser, plan compiler
  scan/         # Parquet scanner, predicates, chunk reader
  join/         # Semi-join, lookup-join, hierarchical join
  output/       # JSON encoders, block grouping, streaming writer
metadata/
  evm.yaml      # Ethereum dataset description
  solana.yaml   # Solana dataset description
benches/        # divan benchmarks
tests/
  e2e_fixtures.rs  # 46 fixture-based tests (legacy output comparison)
  fixtures/        # Query/result JSON pairs per dataset
```

## Usage

```rust
use sqd_query_engine::metadata::loader::load_dataset_description;
use sqd_query_engine::query::{parse::parse_query, plan::compile};
use sqd_query_engine::output::assembly::execute_plan;

let meta = load_dataset_description(Path::new("metadata/evm.yaml")) ?;
let parsed = parse_query(query_json, & meta) ?;
let plan = compile( & parsed, & meta) ?;
let result = execute_plan( & plan, & meta, chunk_dir, Vec::new()) ?;
```

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

Throughput improvement vs legacy engine (median across query types). See [BENCHMARKS.md](BENCHMARKS.md) for full results.

### x86_64: Intel Xeon E-2136 (6C/12T @ 3.3GHz), 64GB DDR4, Linux

| Median           | CPU=1           | CPU=4           | CPU=8           | CPU=12          |
|------------------|-----------------|-----------------|-----------------|-----------------|
| General queries  | **17% faster**  | **56% faster**  | **67% faster**  | **73% faster**  |
| Only full blocks | **485% faster** | **599% faster** | **522% faster** | **455% faster** |

### Apple M2 Pro (12-core), 32GB, macOS

| Median           | CPU=1           | CPU=4           | CPU=8           | CPU=12          |
|------------------|-----------------|-----------------|-----------------|-----------------|
| General queries  | **7% faster**   | **40% faster**  | **53% faster**  | **49% faster**  |
| Only full blocks | **337% faster** | **139% faster** | **99% faster**  | **64% faster**  |

```bash
cargo bench --bench latency               # latency (divan)
cargo bench --bench throughput -- --all    # throughput (all CPU levels)
```

## Supported Datasets

- **EVM** (Ethereum, Optimism, Binance) — `metadata/evm.yaml`
- **Solana** — `metadata/solana.yaml`
- **Substrate** (Kusama, Moonbeam) — `metadata/substrate.yaml`
- **Bitcoin** — `metadata/bitcoin.yaml`
- **Fuel** — `metadata/fuel.yaml`
- **Hyperliquid Fills** — `metadata/hyperliquid_fills.yaml`
- **Hyperliquid Replica Commands** — `metadata/hyperliquid_replica_cmds.yaml`

## Tests

```bash
# Unit tests (82 tests)
cargo test --lib

# E2E fixture tests (46 tests, compare output against legacy engine)
cargo test --test e2e_fixtures

# All tests
cargo test
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md).
