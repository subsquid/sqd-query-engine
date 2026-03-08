# Query Engine

## Project Goal

Schema-agnostic, high-performance query engine for blockchain parquet data. New dataset types (EVM, Solana, Fuel, etc.) added via YAML metadata, not code. No code reuse from legacy engine (`/Users/mo4islona/Projects/subsquid/data`).

**Dependencies**: `arrow`, `parquet`, `rayon`, `serde`, `serde_json`, `anyhow`, `divan`, `memmap2`, `bytes`, `faster-hex`, `tikv-jemallocator`

---

## Architecture

```
JSON query -> parse (metadata-driven) -> compile -> Plan
Plan -> parallel parquet scan (mmap, RowFilter pushdown)
     -> KeyFilter + HierarchicalFilter relation scans
     -> join (semi_join / lookup_join / find_children / find_parents)
     -> block grouping + parallel JSON output
```

### Key Design Decisions

- **Sort keys are filter-first, not block-first**: e.g. `program_id -> d1 -> b9 -> block_number -> tx_index`. Row group stats on filter columns are highly selective.
- **Column types may differ from metadata**: block_number is Int32 in EVM parquet (metadata says UInt64), instruction_address is List<UInt16> (metadata says List<UInt32>). All extractors handle multiple integer types.
- **mmap I/O**: `memmap2::Mmap` + `Bytes::from_owner()` — zero-copy, OS handles paging. Single mmap per file shared across all threads.
- **Pre-built ParquetTable cache**: `execute_plan_cached()` accepts `&mut HashMap<String, ParquetTable>` to reuse across calls.

---

## Changelog

### Phase 1: Foundation
- Project setup, YAML metadata loader, parquet chunk reader with mmap

### Phase 2: Scanning & Filtering
- Predicate system (Eq/InList/BloomFilter/Range/And/Or), parallel row-group scanner, block range filter
- Multi-stage RowFilter cascading (most selective column first)

### Phase 3: Query Language
- JSON query parser, schema-driven validation, plan compiler
- camelCase <-> snake_case, discriminator dispatch (d1/d2/d4/d8/d3-d16)

### Phase 4: Join Engine
- Semi-join (hash-based), lookup-join, hierarchical join (instruction_address prefix matching)
- Multi-column composite keys, bidirectional (children + parents)

### Phase 5: Output Assembly
- JSON encoders (Value, BigNum, Json, SolanaTransactionVersion, TimestampSecond)
- Block grouping, roll() for a0-a15 + rest_accounts, weight-based size limits
- Streaming JSON writer with 16KB flush threshold

### Phase 6: Optimization Round 1
- RowFilter predicate pushdown, parallel relation scans, pre-computed JSON field writers
- Cross-table row group pruning, O(1) block indexing, cached ArrowReaderMetadata
- jemalloc, batch size 65536, early exit on 0 primary rows

### Phase 7: Low-Level Performance
- mmap I/O (memmap2 + Bytes::from_owner)
- Pre-built typed HashSet in InListPredicate + BooleanBufferBuilder
- Batch size usize::MAX (one batch per row group)
- faster-hex SIMD encoding, resolved field writers, typed join extractors

### Phase 8: Relation Scan Pushdown
- KeyFilter: push join keys from primary scan into relation scans as RowFilter stage
- Composite key serialization with cross-type normalization (UInt8/16/32/64, Int16/32/64 -> u64)
- Row group pruning by block number set (binary search on sorted blocks)
- solana_hard/large: 288.9ms -> 123.8ms (2.33x speedup)

### Phase 9: Final Optimizations
- Skip redundant join for Join-type relations when KeyFilter already applied
- HierarchicalFilter as RowFilter stage (children/parents filtering during scan, not post-scan)
- Bug fix: UInt16 instruction_address handling (was silently returning 0 children)
- Parallel JSON generation via rayon par_iter (47ms -> 13ms)
- TypedKeyColumn: resolve column types once per batch, fixed-size stack buffer for 2-column keys

### E2E Tests & Sort Optimization
- 46 fixture-based e2e tests comparing output against legacy engine (all passing)
- Relation scoping via `source_predicates` (per-item relation filtering, not global)
- Fixed `item_order_keys` for statediffs, balances, token_balances, rewards
- Field group emission: emit group when writers exist, not based on non-null values
- **Sort precompute**: `build_full_sort_columns()` + column index resolution moved outside per-block `par_iter` loop into `IndexedBatches` struct. Eliminates per-block `Vec<String>` allocation, `HashSet` construction, and `schema().index_of()` lookups.

---

## Final Benchmarks (2026-03-08)

### Head-to-Head: New Engine vs Legacy

All numbers: median, 20 samples x 100 iterations, jemalloc, pre-cached ParquetTable.
Legacy numbers from fresh run on same machine (2026-03-08).

| Benchmark | New Engine | Legacy | Ratio | Status |
|---|---|---|---|---|
| whirlpool_swap/200 | **6.0 ms** | 4.0 ms | 1.50x slower | fixed-cost floor |
| whirlpool_swap/400 | **5.4 ms** | — | — | |
| whirlpool_swap/large | **11.5 ms** | 20.9 ms | **0.55x (1.82x faster)** | |
| solana_hard/200 | **14.0 ms** | 13.6 ms | 1.03x slower | ~parity |
| solana_hard/400 | **17.7 ms** | 21.4 ms | **0.83x (17% faster)** | |
| solana_hard/large | **61.1 ms** | 84.3 ms | **0.72x (28% faster)** | |
| evm_transfers/large | **70.7 ms** | — | — | no legacy benchmark |

**Large chunks: we beat legacy by 17-45%. Small chunks (/200): ~parity to 1.5x slower due to arrow-rs RowFilter build overhead.**

### Optimization Journey (solana_hard/large)

| Phase | Time | vs Previous | vs Legacy |
|---|---|---|---|
| Unoptimized (Phase 5) | 583.8 ms | — | 6.7x slower |
| Phase 6 (RowFilter, parallel) | 300.5 ms | 1.9x | 3.5x slower |
| Phase 7 (mmap, typed extractors) | 288.9 ms | 1.04x | 3.3x slower |
| Phase 8 (KeyFilter pushdown) | 123.8 ms | 2.3x | 1.4x slower |
| Phase 9 (HierarchicalFilter, parallel JSON) | 67.1 ms | 1.8x | 0.77x |
| Phase 10 (RowFilter tuning) | 64.7 ms | 1.04x | 0.74x |
| E2E + sort precompute | **61.1 ms** | 1.06x | **0.72x (28% faster!)** |
| **Total speedup** | **9.6x** | | |

---

## Stage Profile Results (per benchmark)

Stage profiles below are from Phase 9/10 profiled runs. Absolute times may differ slightly from current benchmarks but relative proportions are representative.

### solana_hard/large (~61ms median, 523 blocks, 4205 primary rows)

| Stage | ~Time | ~% | Details |
|---|---|---|---|
| Primary scan | 8.6 ms | 14% | instructions table, 11 RGs scanned (of ~107) |
| KeyFilter build | 1.5 ms | 2% | extract keys from 4205 rows |
| Relation scans (parallel) | 41 ms | 67% | 3 tables in parallel, wall time = max of 3 |
| Blocks + indexing | 3.1 ms | 5% | 523 blocks, HashMap build, sort precompute |
| JSON output (parallel) | 7 ms | 11% | parallel per-block, ~46K total rows |

### whirlpool_swap/large (~11.5ms median, 488 blocks, 2339 primary rows)

| Stage | ~Time | ~% | Details |
|---|---|---|---|
| Primary scan | 4.9 ms | 42% | instructions table |
| KeyFilter build | 0.2 ms | 2% | 2339 rows |
| Relation scans | 4.6 ms | 40% | transactions only |
| Blocks + indexing | 0.5 ms | 4% | 488 blocks |
| JSON output (parallel) | 1.3 ms | 11% | |

### evm_transfers/large (~71ms median, EVM log Transfer events)

| Stage | ~Time | ~% | Details |
|---|---|---|---|
| Primary scan | ~15 ms | ~21% | logs table, RowFilter on topic0 |
| Relation scans | ~25 ms | ~35% | transactions table |
| JSON output (parallel) | ~20 ms | ~28% | large output (~20MB) |
| Other | ~11 ms | ~16% | blocks, indexing, overhead |

---

## Remaining Performance Gap Analysis

### Why /200 chunks are slightly slower than legacy (~1.0-1.5x)

The primary scan has a **~4.7ms floor** for the whirlpool query (120K rows in 6 RGs, 27 output columns). The arrow-rs RowFilter pipeline creates a complete `ArrayReader` tree per filter stage per `build()` call. This fixed overhead dominates on small chunks but is amortized on large chunks.

| | Legacy | New Engine |
|---|---|---|
| RowFilter stages | **0** (no RowFilter) | 2 stages |
| `build()` cost per RG | ~3µs | ~800µs (2 stages) |
| Filtering | Post-read `filter_record_batch()` | During read (RowFilter pushdown) |

For small chunks (few RGs, low total rows) this is a net loss; for large chunks (many RGs, high selectivity) it's a major win.

### Where time goes for large benchmarks

The dominant cost for solana_hard/large is **relation scans at ~41ms** (~67% of total). Each relation table must:
1. Prune row groups by block number set (KeyFilter sorted_blocks)
2. Read key columns through KeyFilter RowFilter stage
3. Decode data columns for matching rows only

Even with KeyFilter pushdown, the 3 relation tables (~254K tx + ~988K token_bal + ~2.1M instructions) require significant I/O.

---

## Output Sorting & Deduplication

### Why sorting is needed

Parquet files are sorted by **filter columns** (e.g. `topic0 → address → block_number → log_index`), not by logical output order. Within a single block, rows from different row groups may arrive interleaved. The output must match the legacy engine's ordering, which sorts by the table's **primary key** (minus `block_number`, since all rows in a block share the same block number).

### Sort keys per table (defined in YAML `item_order_keys`)

The `item_order_keys` field in metadata YAML defines the output sort order within a block. These match the legacy engine's primary keys:

| Table | `item_order_keys` | Notes |
|---|---|---|
| transactions | `[transaction_index]` | |
| logs (EVM) | `[transaction_index, log_index]` | |
| traces | `[transaction_index]` + `address_column: trace_address` | address_column appended automatically |
| statediffs | `[transaction_index, address, key]` | |
| instructions | `[transaction_index]` + `address_column: instruction_address` | |
| logs (Solana) | `[transaction_index, log_index]` | |
| balances | `[transaction_index, account]` | |
| token_balances | `[transaction_index, account]` | |
| rewards | `[pubkey, reward_type]` | no transaction_index |

The full sort column list is built by `build_full_sort_columns()`: **item_order_keys + address_column** (if present). No sort_key tiebreakers — the parquet file order (batch_idx, row_idx) serves as final tiebreaker, which preserves the physical sort order for rows with identical primary keys.

### How sorting works

**Single-source path** (`write_table_items_indexed`): When a table has one data source (the common case — primary scan only, or a single relation):

1. Collect `(batch_idx, row_idx)` tuples for the current block from the pre-built block index
2. Pre-resolve sort column indices per batch (once per schema, not per row)
3. Sort by comparing column values using `compare_array_values()` — handles UInt8/16/32/64, Int16/32/64, String, and List<Int32> types
4. Tiebreaker: `(batch_idx, row_idx)` preserves parquet file order

**Multi-source path** (`write_merged_table_items`): When the same output table has rows from multiple sources (primary scan + relation scan, or multiple relation scans targeting the same table):

1. Collect `(source_idx, batch_idx, row_idx)` triples from all sources
2. Sort by `item_order_keys + address_column`, then tiebreak by `(source_idx, batch_idx, row_idx)`
3. **Deduplicate**: after sorting, adjacent rows from *different* sources with identical primary keys (item_order_keys + address_column) are deduped — keeps the first occurrence (lower source index). This prevents the same row from appearing twice when it matches both a primary scan and a relation scan.

### Dedup details

Dedup uses `Vec::dedup_by()` on the sorted rows:
- Only considers rows from **different sources** (`a.source_idx != b.source_idx`)
- Compares only the first `dedup_key_count` sort columns (item_order_keys + address_column), not tiebreakers
- The first occurrence wins (from the lower-indexed source)

This is necessary because a table like `logs` can appear as both a primary scan result and a relation result (e.g., `transaction_logs` relation). Without dedup, the same log row would appear twice in the output.

### Relation scoping (`source_predicates`)

Relations are scoped per query item, not globally. When a query has multiple filter items for the same table, only items that explicitly request a relation (e.g., `"transaction": true`) contribute rows to that relation's join.

```json
{
  "logs": [
    {},                                    // matches all logs, NO transaction relation
    {"topic0": ["0x..."], "transaction": true}  // matches filtered logs, WITH transaction relation
  ]
}
```

Implementation:
- `RelationPlan.source_predicates`: `Option<Vec<RowPredicate>>` — predicates from items that requested this relation
  - `None` = all rows qualify (an item with no filters requested it)
  - `Some(preds)` = only rows matching these predicates (OR'd) feed the relation
- During execution, `evaluate_predicates_on_batch()` filters primary scan results before building KeyFilters and join inputs for that relation
- The predicate columns are included in the primary scan output via `resolve_output_columns()`

