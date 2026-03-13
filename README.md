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
- **Pre-built ParquetTable cache**: `execute_plan_cached()` accepts `&mut HashMap<String, ParquetTable>` to reuse across
  calls.

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

Use `execute_plan_cached()` to reuse `ParquetTable` instances across calls:

```rust
use sqd_query_engine::output::assembly::execute_plan_cached;

let mut cache: HashMap<String, ParquetTable> = HashMap::new();
let result = execute_plan_cached( & plan, & meta, chunk_dir, Vec::new(), & mut cache) ?;
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

Data: R2 production chunks (EVM: 224 blocks, ~70 MB; Solana: 48 blocks, ~27 MB).
Jemalloc allocator, pre-cached ParquetTable, Apple M2 Pro 12-core.

### Throughput (requests/sec, 5s per concurrency level)

| Benchmark                  | CPU | New       | Legacy  | Diff            |
|----------------------------|-----|-----------|---------|-----------------|
| evm/usdc_transfers         | 4   | 324       | **357** | 9% slower       |
|                            | 8   | **538**   | 503     | **7% faster**   |
|                            | 12  | **602**   | 536     | **12% faster**  |
| evm/contract_calls+logs    | 4   | **189**   | 159     | **19% faster**  |
|                            | 8   | **272**   | 184     | **48% faster**  |
|                            | 12  | **281**   | 198     | **42% faster**  |
| evm/usdc_traces+statediffs | 4   | **39**    | 31      | **26% faster**  |
|                            | 8   | **48**    | 30      | **60% faster**  |
|                            | 12  | **48**    | 34      | **41% faster**  |
| evm/all_blocks             | 4   | **22445** | 9035    | **148% faster** |
|                            | 8   | **36310** | 14075   | **158% faster** |
|                            | 12  | **35891** | 17555   | **104% faster** |
| sol/whirlpool_swap         | 4   | **327**   | 250     | **31% faster**  |
|                            | 8   | **387**   | 272     | **42% faster**  |
|                            | 12  | **395**   | 281     | **41% faster**  |
| sol/hard (Meteora DLMM)    | 4   | **186**   | 143     | **30% faster**  |
|                            | 8   | **227**   | 146     | **55% faster**  |
|                            | 12  | **236**   | 147     | **61% faster**  |
| sol/instr+logs             | 4   | **144**   | 127     | **13% faster**  |
|                            | 8   | **203**   | 145     | **40% faster**  |
|                            | 12  | **210**   | 142     | **48% faster**  |
| sol/instr+balances         | 4   | **1085**  | 670     | **62% faster**  |
|                            | 8   | **1234**  | 719     | **72% faster**  |
|                            | 12  | **1217**  | 729     | **67% faster**  |
| sol/all_blocks             | 4   | **47701** | 15817   | **202% faster** |
|                            | 8   | **48458** | 26872   | **80% faster**  |
|                            | 12  | **42726** | 32443   | **32% faster**  |

| Median           | CPU=4           | CPU=8           | CPU=12         |
|------------------|-----------------|-----------------|----------------|
| General queries  | **26% faster**  | **48% faster**  | **42% faster** |
| Only full blocks | **175% faster** | **119% faster** | **68% faster** |

```bash
# Run latency benchmarks
cargo bench --bench latency

# Run throughput benchmarks (CPU=4,8,12)
cargo bench --bench throughput
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
