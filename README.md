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

All benchmarks run on the same fixture data (EVM: 1397 blocks, 391 MB parquet; Solana: 200 blocks, 51 MB parquet).
Jemalloc allocator, pre-cached ParquetTable, Apple M2 Pro 12-core.

### Throughput (rps, 5s per concurrency level)

| Benchmark               | CPU | New        | Legacy  | Diff            |
|-------------------------|-----|------------|---------|-----------------|
| evm/usdc_transfers      | 4   | 588        | **636** | 8% slower       |
|                         | 8   | **986**    | 815     | **21% faster**  |
|                         | 12  | **1120**   | 889     | **26% faster**  |
| evm/contract_calls+logs | 4   | **215**    | 158     | **36% faster**  |
|                         | 8   | **217**    | 166     | **31% faster**  |
|                         | 12  | **222**    | 174     | **28% faster**  |
| evm/bayc_traces+diffs   | 4   | **209**    | 179     | **17% faster**  |
|                         | 8   | **209**    | 181     | **15% faster**  |
|                         | 12  | **217**    | 180     | **21% faster**  |
| evm/all_blocks          | 4   | **5298**   | 3593    | **47% faster**  |
|                         | 8   | **10102**  | 5079    | **99% faster**  |
|                         | 12  | **11991**  | 5468    | **119% faster** |
| sol/whirlpool_swap      | 4   | **127**    | 95      | **34% faster**  |
|                         | 8   | **138**    | 99      | **39% faster**  |
|                         | 12  | **139**    | 99      | **40% faster**  |
| sol/instr+logs          | 4   | **116**    | 108     | **7% faster**   |
|                         | 8   | **121**    | 112     | **8% faster**   |
|                         | 12  | **118**    | 113     | **4% faster**   |
| sol/instr+balances      | 4   | **277**    | 236     | **17% faster**  |
|                         | 8   | **331**    | 246     | **35% faster**  |
|                         | 12  | **338**    | 256     | **32% faster**  |
| sol/all_blocks          | 4   | **9278**   | 5212    | **78% faster**  |
|                         | 8   | **17896**  | 7575    | **136% faster** |
|                         | 12  | **20516**  | 8768    | **134% faster** |

| Median          | CPU=4          | CPU=8           | CPU=12          |
|-----------------|----------------|-----------------|-----------------|
| Queries         | **17% faster** | **26% faster**  | **27% faster**  |
| All blocks      | **63% faster** | **118% faster** | **127% faster** |

```bash
# Run benchmarks (latency + throughput)
cargo bench --bench throughput
```

Legacy engine benchmark (same queries, same data):

```bash
cd /path/to/legacy/data
git checkout benchmark-comparison
cargo bench --bench throughput -p sqd-query --features parquet
```

## Tests

```bash
# Unit tests (72 tests)
cargo test --lib

# E2E fixture tests (46 tests, compare output against legacy engine)
cargo test --test e2e_fixtures
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md).
