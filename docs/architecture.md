# Query Engine Architecture: Scanning & Execution

Detailed comparison of the new (`sqd-query-engine`) and legacy (`sqd-query`) query engines.

Both engines solve the same problem: given a JSON query describing which blockchain data to extract (blocks, transactions, logs, traces, instructions, etc.), scan parquet chunks and return matching data as JSON.

---

## Table of Contents

1. [Data Layout](#data-layout)
2. [New Engine: RowFilter Pipeline](#new-engine-rowfilter-pipeline)
3. [Legacy Engine: 3-Phase Execution](#legacy-engine-3-phase-execution)
4. [Key Architectural Differences](#key-architectural-differences)
5. [Concurrency & Scaling](#concurrency--scaling)
6. [Performance Characteristics](#performance-characteristics)
   - [Latency (CPU=1)](#latency-cpu1)
   - [Throughput](#throughput-x86_64-rps)
   - [Platform Differences: x86_64 vs Apple M2](#platform-differences-x86_64-vs-apple-m2)
   - [Hierarchical Filter Approach Comparison](#hierarchical-filter-approach-comparison)

---

## Data Layout

Both engines read the same R2 parquet chunks. Each chunk is a directory containing one `.parquet` file per table:

```
chunk/
  blocks.parquet
  transactions.parquet
  logs.parquet
  traces.parquet
  state_diffs.parquet       # EVM
  instructions.parquet      # Solana
  transaction_balances.parquet
  ...
```

### Sort Keys

Tables are sorted by **filter columns first**, not by `block_number`:

| Table | Sort Key |
|-------|----------|
| EVM logs | `topic0 -> address -> block_number -> log_index` |
| EVM txs | `sighash -> to -> block_number -> tx_index` |
| EVM traces | `type -> call_to -> call_sighash -> block_number -> ...` |
| Solana instructions | `program_id -> d1 -> d8 -> block_number -> tx_index -> ...` |
| Solana txs | `fee_payer -> block_number -> tx_index` |

This makes row group min/max statistics on filter columns (topic0, address, program_id, etc.) **highly selective** -- most row groups can be pruned by stats alone.

### File Access

Both engines use `memmap2::Mmap` for I/O. The OS manages page faults, prefetching, and caching. Files are never read entirely into memory.

---

## New Engine: RowFilter Pipeline

### Overview

The new engine uses **arrow-rs RowFilter** with multi-stage cascading. Each stage has its own `ProjectionMask`, meaning it decodes only the columns it needs. Rows eliminated by early stages **avoid column decoding in all subsequent stages**, including output columns.

This is a single-pass approach: one `ParquetRecordBatchReader` per row group handles filtering and output column reading in one scan.

### Execution Flow

```
JSON Query
    |
    v
parse_query() --> ParsedQuery
    |
    v
compile()     --> Plan { table_plans, block_table, block_output_columns, ... }
    |
    v
execute_chunk()
    |
    +---> 1. Primary table scans (parallel per table)
    |         scan() --> Vec<RecordBatch>
    |
    +---> 2. Relation scans (parallel per relation)
    |         KeyFilter/HierarchicalFilter build
    |         scan() with pushdown filters
    |         lookup_join / find_children / find_parents
    |
    +---> 3. Block header scan
    |
    +---> 4. Collect block numbers (HashSet)
    +---> 5. Sort blocks, apply weight limit (20MB cap)
    +---> 6. Build block indexes (block_number -> batch positions)
    +---> 7. Write JSON output (sequential iteration over selected blocks)
```

### scan() Internals

**Entry point:** `src/scan/scanner.rs:523`

```
scan(table, request)
  |
  +-- 1. Collect all needed columns:
  |      output_columns + predicate_columns + block_number + key_filter_columns + hierarchical_filter_columns
  |
  +-- 2. select_row_groups(): prune row groups via statistics
  |      - Block range: skip RG where block_number max < from_block or min > to_block
  |      - Predicates: can_skip_row_group_or() using min/max stats
  |      - KeyFilter: binary search on sorted_blocks vs RG block range
  |
  +-- 3. scan_row_groups(): build RowFilter pipeline, read data
  |      - Parallelized via rayon (one task per row group)
  |
  +-- 4. project_batch(): drop non-output columns from result
```

### RowFilter Multi-Stage Pipeline

The core innovation. Each stage is an `ArrowPredicateFn` with its own `ProjectionMask`:

```
Stage 0: Block Range Filter
    ProjectionMask: [block_number]
    Evaluates: from_block <= block_number <= to_block
    Result: BooleanArray mask
        |
        v  (only surviving rows proceed)

Stage 1: KeyFilter (composite key IN set)
    ProjectionMask: [block_number, tx_index, ...]  (join key columns)
    Evaluates: (block_number, tx_index) IN key_set
    Result: BooleanArray mask
        |
        v

Stage 2: HierarchicalFilter (children/parents)
    ProjectionMask: [group_keys, address_column]
    Evaluates: address prefix matching against source addresses
    Result: BooleanArray mask
        |
        v

Stage 3: First Predicate Column (most selective -- sort key leader)
    ProjectionMask: [topic0]  (or program_id, sighash, etc.)
    Evaluates: topic0 IN [0xddf252ad...]
    Result: BooleanArray mask
        |
        v

Stage 4: Remaining Predicate Columns (merged)
    ProjectionMask: [address, ...]
    Evaluates: AND of all remaining column predicates
    Result: BooleanArray mask
        |
        v

Output Column Decode
    ProjectionMask: [all remaining columns in the reader's projection]
    Only rows surviving ALL stages get their output columns decoded
```

**How arrow-rs RowFilter works internally:**

1. Reader starts with a full `RowSelection` (all rows in the row group).
2. For each filter stage:
   a. Decode only the stage's `ProjectionMask` columns for the current `RowSelection`.
   b. Evaluate the predicate function -> `BooleanArray`.
   c. Intersect with `RowSelection` -> new, smaller `RowSelection`.
3. After all filter stages, decode the remaining output columns for surviving rows only.

This means output columns (data, topics, accounts, etc.) are **never decoded for filtered-out rows**. The deferred-read effect is achieved within a single parquet reader pass.

### Predicate System

Each query item (e.g., a log filter `{topic0: [...], address: [...]}`) compiles to a `RowPredicate`:

```rust
pub struct RowPredicate {
    pub columns: Vec<ColumnPredicate>,  // AND of column conditions
}

pub struct ColumnPredicate {
    pub column: String,
    pub predicate: Arc<dyn ColumnEvaluator>,
}
```

Multiple query items for the same table are ORed together.

Column evaluators:
- `EqEvaluator`: single value equality
- `InListEvaluator`: value IN set (HashSet-based)
- `BloomFilterEvaluator`: probabilistic set membership
- `ListContainsAnyEvaluator`: list column contains any of values

### Relation Pushdown (KeyFilter)

When a query requests related data (e.g., "transactions" for matched logs), the engine:

1. Scans the primary table (logs) with predicates.
2. Extracts join keys (block_number, transaction_index) from matched rows.
3. Builds a `KeyFilter`:
   ```rust
   pub struct KeyFilter {
       pub columns: Vec<String>,           // key column names in target table
       pub block_number_column: String,
       pub sorted_blocks: Vec<u64>,         // for RG-level pruning
       pub key_set: Arc<FxHashSet<u64>>,    // composite key hashes
   }
   ```
4. Scans the target table (transactions) with the `KeyFilter` as a RowFilter stage.
   - RG pruning: skip row groups with no matching block_numbers.
   - Row filtering: only decode rows where composite key hash is in `key_set`.
5. Result: only matching transaction rows, no post-scan join needed.

### Output Assembly

After all scans complete:

1. **Collect block numbers** from all table outputs into a `HashSet<u64>`.
2. **Sort** block numbers.
3. **Apply weight limit**: iterate sorted blocks, accumulate per-block weight (computed from output column weights), stop at 20MB.
4. **Build block indexes**: `FxHashMap<u64, Vec<(batch_idx, row_idx)>>` for each batch set.
5. **Write JSON**: iterate selected blocks, write header + table items. Multiple sources for the same output table are merged and sorted by item_order_keys.

---

## Legacy Engine: 3-Phase Execution

### Overview

The legacy engine uses an **explicit 3-phase approach**:

1. **Phase 1 (Key Scan)**: Read ZERO data columns, only predicates + row_index.
2. **Phase 2 (Relations)**: Polars semi-join on key columns.
3. **Phase 3 (Data Read)**: Read output columns for matched rows only via RowSelection.

### Execution Flow

```
JSON Query
    |
    v
Query::from_json_bytes() --> Query
    |
    v
query.compile()          --> Plan { scans, relations, outputs }
    |
    v
plan.execute(chunk)
    |
    +---> Phase 1: execute_scans() [parallel via rayon]
    |       For each scan:
    |         scan_table(table)
    |           .with_row_index(true)
    |           .with_columns([])         <-- ZERO data columns
    |           .with_predicate(pred)
    |           .execute()
    |         --> Vec<RecordBatch> with only [row_index] column
    |         Store row_index values in RowList (BTreeSet<u32>)
    |
    +---> Phase 2: execute_relations() [parallel via rayon]
    |       For each relation:
    |         Read input table key columns (for matched rows only)
    |         Read output table key columns (all rows + row_index)
    |         Polars semi-join --> matching output row indices
    |         Store in RowList
    |
    +---> Phase 3: execute_output() [parallel via rayon]
    |       Sub-phase A: Weight calculation
    |         For each output table:
    |           scan_table(table)
    |             .with_row_selection(matched_rows)
    |             .with_column(block_number)
    |             .with_columns(weight_columns)
    |             .execute()
    |           --> Polars: group_by(block_number).agg(sum(weight))
    |           --> cum_sum, filter <= 20MB --> selected blocks
    |
    |       Sub-phase B: Final data read
    |         For each output table:
    |           scan_table(table)
    |             .with_row_selection(final_rows)
    |             .with_projection(output_columns)
    |             .execute()
    |           --> DataItem for JSON assembly
    |
    +---> BlockWriter::write_blocks() --> JSON-Lines output
```

### Phase 1: Scan with Zero Columns

```rust
// plan.rs:180-186
let rows = self.data_chunk
    .scan_table(scan.table)?
    .with_row_index(true)
    .with_columns([])            // Empty projection!
    .with_predicate(scan.predicate.clone())
    .execute()?;
```

The scan builder handles this by:
1. Creating a `ProjectionMask` from predicate columns only (since `with_columns` is empty).
2. Adding a synthetic `row_index` column (UInt32) mapping each output row to its file position.
3. After predicate evaluation, dropping predicate columns, keeping only `row_index`.

Result: `Vec<RecordBatch>` where each batch has a single `row_index: UInt32` column.

### Phase 2: Polars Semi-Join

```rust
// rel.rs:103-137
let input_rows = chunk.scan_table(input_table)?
    .with_row_selection(RowRangeList::from_sorted_indexes(input.iter().copied()))
    .with_columns(input_key.iter().copied())
    .to_lazy_df()?;

let output_rows = chunk.scan_table(output_table)?
    .with_row_index(true)
    .with_columns(output_key.iter().copied())
    .to_lazy_df()?;

let result = output_rows.join(
    input_rows,
    output_key.map(col),
    input_key.map(col),
    JoinArgs::new(JoinType::Semi)
).select([col("row_index")]).collect()?;
```

Key points:
- Input table: re-reads key columns for matched rows only (via `RowRangeList`).
- Output table: reads ALL rows but only key columns + row_index.
- Semi-join returns output `row_index` values where keys match.
- No data columns touched yet.

### Phase 3: Data Read with RowSelection

```rust
// plan.rs:382-389
let row_selection = RowRangeList::from_sorted_indexes(
    row_index.column("row_index").unwrap().u32()?.into_no_null_iter()
);

let records = self.data_chunk
    .scan_table(output.table)?
    .with_row_selection(row_selection)
    .with_projection(output.projection.clone())
    .execute()?;
```

`RowRangeList` is converted to parquet's `RowSelection` (skip/select pairs), allowing arrow-rs to skip entire pages without decompression.

### Stats Pruning (Two Levels)

The legacy engine applies statistics pruning at two levels:

1. **Row group level**: min/max stats per column per row group.
2. **Page level**: min/max stats per column per page (column page index).

Both return `RowRangeList` which gets intersected with any existing `RowSelection`.

### Global Rayon Thread Pool

All parallel operations use a single global Rayon pool:

```rust
// Benchmark entry point
sqd_polars::POOL.install(|| {
    plan.execute(chunk)?;
    // ...
})
```

This means:
- All concurrent queries share the same thread pool.
- `par_iter()` calls within scans/joins/output all compete for pool threads.
- At high concurrency, this becomes a bottleneck (see [Concurrency & Scaling](#concurrency--scaling)).

---

## Key Architectural Differences

### 1. Column Read Strategy

| Aspect | New Engine | Legacy |
|--------|-----------|--------|
| **Mechanism** | RowFilter stages with per-stage ProjectionMask | Explicit 3-phase: scan keys, join, read data |
| **Output column deferral** | Implicit (arrow-rs RowFilter pipeline) | Explicit (RowSelection from Phase 1-2 indices) |
| **Data column reads** | Once (final decode within RowFilter reader) | Twice minimum (weight calc + final projection) |
| **Parquet reader instances** | 1 per row group per table | 3-5 per table (Phase 1 + Phase 2 joins + Phase 3 reads) |

Both engines achieve the same effect: output columns are only decoded for rows that survive all filters. The difference is in mechanism:

- **New engine**: Single parquet reader per row group. Arrow-rs internally tracks a `RowSelection` that shrinks after each filter stage. Output columns are decoded as the final step within the same reader.

- **Legacy**: Multiple separate parquet readers per table. Phase 1 returns row indices. Phase 3 opens a new reader with `RowSelection` pointing to matched rows.

### 2. Join Strategy

| Aspect | New Engine | Legacy |
|--------|-----------|--------|
| **Join mechanism** | KeyFilter pushdown into scan | Polars semi-join on materialized DataFrames |
| **When applied** | During parquet read (RowFilter stage) | After Phase 1 scan completes |
| **I/O operations** | 1 scan of target table | 2 scans: key columns (Phase 2) + data columns (Phase 3) |
| **Memory** | HashSet of composite key hashes | Polars DataFrames for both sides |

### 3. Weight Calculation

| Aspect | New Engine | Legacy |
|--------|-----------|--------|
| **Implementation** | Direct arrow array iteration | Polars group_by + cum_sum |
| **When** | After all scans complete | Phase 3, sub-phase A |
| **Extra I/O** | None (uses already-scanned batches) | Separate scan with weight columns |

### 4. Statistics Pruning

| Aspect | New Engine | Legacy |
|--------|-----------|--------|
| **Row group stats** | Yes | Yes |
| **Page-level stats** | No | Yes (column page index) |
| **KeyFilter RG pruning** | Yes (binary search on sorted_blocks) | No |

The legacy engine has an advantage with page-level statistics pruning, which can eliminate individual pages within a row group. The new engine compensates with KeyFilter-based row group pruning for relation scans.

### 5. Parquet Reader Count Per Query

For a query like "USDC transfers with transactions" (logs table + transaction relation):

**New engine (2 scan() calls):**
1. `scan(logs, predicates + output_columns)` -- 1 reader per RG
2. `scan(transactions, key_filter + output_columns)` -- 1 reader per RG

**Legacy engine (5+ scan_table() calls):**
1. Phase 1: `scan_table(logs).with_columns([]).with_predicate()` -- row indices
2. Phase 2: `scan_table(logs).with_row_selection().with_columns(keys)` -- input keys for join
3. Phase 2: `scan_table(transactions).with_row_index().with_columns(keys)` -- output keys + indices
4. Phase 3: `scan_table(logs).with_row_selection().with_column(block_number).with_columns(weights)` -- weight calc
5. Phase 3: `scan_table(transactions).with_row_selection().with_column(block_number).with_columns(weights)` -- weight calc
6. Phase 3: `scan_table(logs).with_row_selection().with_projection(output)` -- final data
7. Phase 3: `scan_table(transactions).with_row_selection().with_projection(output)` -- final data

The new engine does fundamentally fewer I/O operations per query, which matters at high concurrency.

---

## Concurrency & Scaling

### New Engine: Independent Threads

Each query runs on a single thread (no internal parallelism except for multi-RG scans via rayon). Queries are fully independent: no shared state, no global thread pool contention.

```
Thread 1: query A scan(logs) -> scan(txs) -> weight -> json
Thread 2: query B scan(logs) -> scan(txs) -> weight -> json
Thread 3: query C scan(logs) -> scan(txs) -> weight -> json
...no contention...
```

Multi-RG parallelism within a single scan:
```rust
// scanner.rs:580-583
let results: Vec<Result<Vec<RecordBatch>>> = row_groups_to_scan
    .par_iter()
    .map(|&rg_idx| scan_row_groups(table, &[rg_idx], ...))
    .collect();
```

This helps at low concurrency (CPU=1-2) but is neutral at high concurrency where each thread already has enough work.

### Legacy Engine: Shared Rayon Pool

All queries funnel through a single Rayon thread pool:

```
Thread 1: query A -> sqd_polars::POOL.install(|| ...)
Thread 2: query B -> sqd_polars::POOL.install(|| ...)
...all par_iter() calls compete for same pool...
```

At CPU=1-4, the pool efficiently distributes work across threads. At CPU=8+, contention on the pool scheduler adds overhead. Both engines plateau after CPU=8 (matching the 6 physical cores on Xeon E-2136), but the new engine maintains a wider throughput gap at every concurrency level due to fewer I/O operations and no shared pool contention.

---

## Performance Characteristics

All data: R2 production chunks (EVM: 224 blocks, ~70 MB; Solana: 48 blocks, ~27 MB). Jemalloc allocator, pre-cached ParquetTable. Full results in [BENCHMARKS.md](../BENCHMARKS.md).

### Latency (CPU=1)

#### x86_64: Intel Xeon E-2136 (6C/12T @ 3.3GHz), 12MB L3, DDR4

| Benchmark                  | New           | Legacy    | Diff             |
|----------------------------|---------------|-----------|------------------|
| evm/usdc_transfers         | **10.95 ms**  | 12.61 ms  | **1.15x faster** |
| evm/contract_calls+logs    | **18.53 ms**  | 19.51 ms  | **1.05x faster** |
| evm/usdc_traces+statediffs | 72.15 ms      | 57.85 ms  | 1.25x slower     |
| evm/all_blocks             | **0.18 ms**   | 0.62 ms   | **3.44x faster** |
| sol/whirlpool_swap         | **6.56 ms**   | 8.15 ms   | **1.24x faster** |
| sol/hard (Meteora DLMM)    | **10.37 ms**  | 12.03 ms  | **1.16x faster** |
| sol/instr+logs             | 24.01 ms      | 23.94 ms  | ~same            |
| sol/instr+balances         | **2.07 ms**   | 3.26 ms   | **1.57x faster** |
| sol/all_blocks             | **0.06 ms**   | 0.50 ms   | **7.69x faster** |

New engine wins **7 of 9** benchmarks on x86_64. Legacy wins only evm/usdc_traces+statediffs (heavy output, many columns).

#### Apple M2 Pro (12-core), 32MB L2, unified memory

| Benchmark                  | New          | Legacy   | Diff             |
|----------------------------|--------------|----------|------------------|
| evm/usdc_transfers         | **7.48 ms**  | 8.04 ms  | **1.07x faster** |
| evm/contract_calls+logs    | **13.23 ms** | 13.28 ms | **~same**        |
| evm/usdc_traces+statediffs | 52.11 ms     | 42.11 ms | 1.24x slower     |
| evm/all_blocks             | **0.14 ms**  | 0.42 ms  | **3.00x faster** |
| sol/whirlpool_swap         | 5.39 ms      | 2.28 ms  | 2.36x slower     |
| sol/hard (Meteora DLMM)    | **9.35 ms**  | 9.38 ms  | **~same**        |
| sol/instr+balances         | **1.91 ms**  | 2.90 ms  | **1.52x faster** |
| sol/all_blocks             | **0.05 ms**  | 0.27 ms  | **5.40x faster** |

New engine wins **5 of 8** on M2. Legacy wins evm/usdc_traces+statediffs and sol/whirlpool_swap.

### Throughput (x86_64, rps)

| Benchmark                  | CPU | New        | Legacy   | Diff             |
|----------------------------|-----|------------|----------|------------------|
| evm/usdc_transfers         | 4   | **295**    | 202      | **46% faster**   |
|                            | 8   | **441**    | 268      | **64% faster**   |
|                            | 12  | **500**    | 289      | **73% faster**   |
| evm/contract_calls+logs    | 4   | **152**    | 98       | **56% faster**   |
|                            | 8   | **190**    | 111      | **72% faster**   |
|                            | 12  | **204**    | 111      | **83% faster**   |
| evm/all_blocks             | 4   | **23251**  | 4671     | **398% faster**  |
|                            | 8   | **35040**  | 7506     | **367% faster**  |
|                            | 12  | **37468**  | 8528     | **339% faster**  |
| sol/whirlpool_swap         | 4   | **285**    | 172      | **66% faster**   |
|                            | 8   | **316**    | 185      | **71% faster**   |
|                            | 12  | **325**    | 181      | **80% faster**   |
| sol/all_blocks             | 4   | **62810**  | 6979     | **800% faster**  |
|                            | 8   | **95900**  | 12330    | **677% faster**  |
|                            | 12  | **102320** | 15270    | **570% faster**  |

New engine wins **every** throughput benchmark at **every** concurrency level on x86_64.

### Why New Engine Wins at High Concurrency

1. **Fewer I/O operations**: 2 scans vs 5-7 scans per query means less mmap page fault pressure.
2. **No global thread pool**: No scheduler contention between concurrent queries.
3. **Smaller working set**: Single-pass RowFilter pipeline keeps less intermediate data in memory.
4. **No Polars overhead**: Direct arrow array operations instead of DataFrame construction/joins.

### Memory Bandwidth Saturation

At extremely high rps (e.g., all_blocks at ~102K rps on x86_64), the bottleneck shifts to memory bandwidth. Each query creates new `Vec<RecordBatch>` for scan results, `HashSet<u64>` for block numbers, `FxHashMap` for block indexes. This causes the slight throughput drop for all_blocks at CPU=12 vs CPU=8 on some machines.

### Platform Differences: x86_64 vs Apple M2

The key anomaly: legacy's sol/whirlpool_swap goes from 8.15ms (x86) to 2.28ms (M2) — a **3.6x** platform speedup, while our engine scales normally (6.56ms → 5.39ms, 1.2x).

**Root cause: CPU cache hierarchy.**

| | M2 Pro | Xeon E-2136 |
|---|---|---|
| L2 cache | **32 MB** | 256 KB per core |
| L3 cache | — | **12 MB** shared |
| Memory bandwidth | ~200 GB/s | ~25-50 GB/s (DDR4) |
| Cache access latency | 3-10 ns | 10-30 ns (L3), 50-100 ns (RAM miss) |

Legacy's 3-phase approach makes **multiple passes** over the same table data:
1. Phase 1: scan key columns (touches all pages)
2. Phase 2: re-read key columns for join (same pages)
3. Phase 3: read data columns for matched rows

On M2 Pro, the Solana instruction table (~27 MB) fits almost entirely in the 32 MB L2 cache. Phases 2-3 hit **warm cache** (3-10 ns access). On Xeon with only 12 MB L3, the data is evicted between phases, causing **RAM cache misses** (50-100 ns access) — a 10-30x penalty per access.

Our single-pass RowFilter approach reads everything in one scan, which is inherently cache-friendly regardless of cache size:
- On **x86** (small cache): we **beat** legacy because we avoid multi-pass cache miss storms
- On **M2** (large cache): legacy's multi-pass approach becomes essentially free, and its Polars-vectorized joins + NEON SIMD optimizations give it an edge on some queries

At high concurrency, our architectural advantages (fewer I/O ops, no global thread pool) dominate on both platforms.

### Hierarchical Filter Approach Comparison

For relation scans with hierarchical filters (e.g., sol/whirlpool_swap instruction children), we evaluated several approaches (M2 Pro, instruction scan time only):

| Approach | Latency | Why |
|---|---|---|
| No RowFilter (read all + post-filter) | 14 ms | Decodes all output columns for all 201K rows |
| Two-pass with RowSelection | 7 ms | Second reader construction + RowSelection can't skip pages (1 page/RG) |
| Key-only RowFilter + post-filter | 6 ms | RowFilter machinery overhead exceeds savings from deferring address column |
| **Merged KF+HF RowFilter** (current) | **4.4 ms** | Single stage reads key+address (cheap), defers heavy output columns |

The merged approach is optimal: key + address columns (integers + List offsets) are cheap to decode for all rows, while the heavy output columns (instruction data, account keys) are only decoded for the ~1.2% of matching rows.
