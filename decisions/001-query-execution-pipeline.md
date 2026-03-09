# Query Execution Pipeline

## Pipeline Stages

```
JSON query
  → 1. Parse (validate against YAML metadata)
  → 2. Compile (build predicates, plan relations)
  → 3. Scan (parallel parquet read with RowFilter pushdown)
  → 4. Join (semi_join / lookup_join / find_children / find_parents)
  → 5. Output (block grouping, sequential JSON)
```

### 1. Parse

`src/query/parse.rs` — JSON → `Query` struct.

Metadata-driven: validates field names, table names, column types against YAML.
Produces block range, table items with filters, field selections.

### 2. Compile

`src/query/plan.rs` — `Query` → `Plan`.

- Converts filters into `RowPredicate` (column predicates combined with AND/OR)
- Groups discriminators by byte length (d1/d2/d4/d8)
- Collects unique relations, implements **relation scoping** via `source_predicates`:
  items without filters → applies to ALL primary rows; items with filters → narrows scope

### 3. Scan

`src/scan/scanner.rs` — the most expensive stage.

Three levels:

**3a. Row group pruning** (`select_row_groups`):

- Block range stats (min/max)
- Predicate column stats (can row group be excluded?)
- KeyFilter block set (binary search on `sorted_blocks`)

**3b. RowFilter pipeline** (arrow-rs):
Multiple stages applied in order during parquet read, each reading only its needed columns:

1. Block range filter
2. KeyFilter (composite key IN set) — for Join relations
3. HierarchicalFilter (address prefix) — for Children/Parents relations
4. Predicate filters (most selective column first)

**3c. Parallel row group scan**:

- 1 row group → sequential
- Multiple row groups → `par_iter()` across RGs

### 4. Join

`src/join/` — parallel across relations via `into_par_iter()`.

- **Join** (`lookup_join`): hash map from source keys → probe target. Skipped if KeyFilter was pushed down.
- **Children** (`find_children`): target address is strict extension of source. Skipped if HierarchicalFilter pushed
  down.
- **Parents** (`find_parents`): target address is strict prefix of source. Skipped if HierarchicalFilter pushed down.

### 5. Output

`src/output/assembly.rs` — sequential JSON generation.

- Pre-build block indices, sort columns, field writers (all outside per-block loop)
- Select blocks up to 20MB weight limit
- Sequential per-block JSON (see [001-json-serialization-strategy.md](001-json-serialization-strategy.md))
- Sort + dedup rows from multiple sources within each block

## RowFilter Overhead

The arrow-rs RowFilter pipeline creates a complete `ArrayReader` tree per filter stage
per `build()` call. Each `build()` costs **~800us per stage per row group**.

This means:

- 2 filter stages x 6 row groups = 12 build() calls = **~9.6ms overhead**
- 2 filter stages x 50 row groups = 100 build() calls = **~80ms overhead**

But more row groups means more data per group, so the overhead is amortized by actual
I/O savings from filtering.

### Why latency is worse on small chunks

For the whirlpool_swap/200 benchmark (120K rows, 6 RGs):

|                          | New Engine | Legacy               |
|--------------------------|------------|----------------------|
| RowFilter build overhead | ~9.6ms     | 0 (post-read filter) |
| Actual scan work         | ~4ms       | ~4ms                 |
| **Total**                | ~14ms      | ~4ms                 |

The RowFilter overhead dominates when there are few rows to filter. On small chunks,
the legacy approach (read all, filter after) is faster because there's no per-RG setup cost.

On large chunks (50+ RGs, millions of rows), RowFilter saves significant I/O by
skipping entire row groups and filtering rows during decode. The setup cost becomes
negligible relative to the data volume.

### Where time goes (large benchmark example)

For `solana_hard/large` (61ms total):

| Stage                     | Time  | %   |
|---------------------------|-------|-----|
| Primary scan              | ~15ms | 25% |
| Relation scans (3 tables) | ~41ms | 67% |
| Joins                     | ~2ms  | 3%  |
| Block grouping + indexing | ~2ms  | 3%  |
| JSON generation           | ~1ms  | 2%  |

Relation scans dominate because they touch large tables:

- transactions: ~254K rows
- token_balances: ~988K rows
- instructions: ~2.1M rows

Even with KeyFilter pushdown (prune RGs by block set, filter rows by composite key
during read), scanning these tables is significant I/O.

## Optimization History

| Phase                 | solana_hard/large | vs Legacy       | Key change                        |
|-----------------------|-------------------|-----------------|-----------------------------------|
| Phase 5 (unoptimized) | 584ms             | 6.7x slower     | Baseline                          |
| Phase 6               | 301ms             | 3.5x slower     | RowFilter, parallel RG scan       |
| Phase 7               | 289ms             | 3.3x slower     | mmap, typed extractors            |
| Phase 8               | 124ms             | 1.4x slower     | KeyFilter pushdown                |
| Phase 9               | 67ms              | 1.3x faster     | HierarchicalFilter, parallel JSON |
| Final                 | **61ms**          | **1.4x faster** | Sort precompute, sequential JSON  |

Total speedup from Phase 5 to final: **9.6x**.

## Final Latency (single-threaded, median of 100 iterations)

Full pipeline per iteration: parse JSON → compile plan → execute query.

> **Note:** Latency measures a single isolated query. In production, the worker always handles
> multiple concurrent queries, so single-query latency is not a realistic scenario. Throughput
> at CPU=4-12 is the metric that matters.

| Benchmark               | New         | Legacy       | Diff           |
|-------------------------|-------------|--------------|----------------|
| evm/usdc_transfers      | 6.73 ms     | **4.63 ms**  | 45% slower     |
| evm/contract_calls+logs | **5.92 ms** | 8.21 ms      | **28% faster** |
| evm/bayc_traces+diffs   | 7.26 ms     | **6.80 ms**  | 7% slower      |
| evm/all_blocks          | **0.87 ms** | 0.99 ms      | **12% faster** |
| sol/whirlpool_swap      | 15.21 ms    | **14.16 ms** | 7% slower      |
| sol/instr+logs          | 12.62 ms    | **11.82 ms** | 7% slower      |
| sol/instr+balances      | 8.01 ms     | **7.31 ms**  | 10% slower     |
| sol/all_blocks          | **0.53 ms** | 0.73 ms      | **27% faster** |

On average, single-query latency is roughly the same as legacy.
