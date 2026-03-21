# Storage Tier Design

## Context

The query engine needs a storage layer for blockchain data that supports:
- 200+ datasets writing in parallel, blocks every 50-400ms (up to 100 blocks/sec for MegaETH)
- Blocks queryable immediately after push (per-block head updates)
- Fork/reorg rollback
- Per-dataset isolation (size tracking, quotas, deletion)
- Cold storage on separate machines; local data evicted on confirmation

The previous system uses `sqd_storage` — a custom columnar page format on RocksDB.

## Decision

Three-tier file-based storage: **Memory → Spillover Parquet → Cold Parquet**.
No RocksDB. No shared state between datasets.

## Why Not RocksDB

Validated against legacy codebase (`sqd_storage`):

| Problem | Evidence |
|---------|----------|
| Can't measure dataset size | All data in shared `CF_TABLES`. Must iterate all pages. |
| Can't delete dataset fast | Individual `batch.delete_cf()` per key + wait for compaction |
| Write stalls | `OptimisticTransactionDB` with infinite retry, no backoff |
| Layered write amplification | sqd_storage merge 1.9x × RocksDB LSM 5-10x = ~10-20x total |
| Shared resources | 5 global CFs, one 256MB block cache for all 200 datasets |

Blockchain data is append-only, immutable, range-queried, deleted in bulk — the opposite
of what RocksDB is optimized for (random updates, point lookups, transactions).

## Three Tiers

```
Block arrives (every 50-400ms per dataset)
     ↓
[Memory buffer]          ← queryable instantly, ~255ns push
     ↓  background flush
[Spillover parquet]      ← sorted, 8K row groups, on disk
     ↓  finalization
[Cold parquet chunks]    ← sorted, compressed, query-optimized
     ↓  eviction
[deleted]
```

### Tier 1: Memory Buffer

Arrow RecordBatches in memory, one per block per table.

| Metric | Value |
|--------|-------|
| Push latency | **255 ns** (Arc clone, zero-copy) |
| Push during flush | **280 ns** (no degradation) |
| Max throughput | **642K blocks/sec** |
| Query: block-level pruning | Skips blocks by range + predicate min/max |

### Tier 2: Spillover Parquet

When memory exceeds cap (default 500MB), oldest blocks flush to sorted parquet files.
Background thread, does not block writes or queries.

| Metric | Value |
|--------|-------|
| Compression | 9.2x (627 MB memory → 68 MB parquet) |
| Query impact during flush | **+3.6%** latency (noise level) |
| Write impact during flush | **0%** (zero degradation) |

Flush variants (100 EVM blocks, 269 MB memory):

| Variant | Sort | Write | Total | Rate | Parquet size |
|---------|------|-------|-------|------|-------------|
| Sorted + ZSTD(default) | 314ms | 609ms | 923ms | 108 b/s | **30.7 MB** |
| Sorted + ZSTD(1) | 314ms | 603ms | 916ms | 109 b/s | **30.7 MB** |
| Sorted + LZ4 | 314ms | 499ms | 812ms | 123 b/s | 55.2 MB |
| Sorted + uncompressed | 314ms | 416ms | 730ms | 137 b/s | 146.0 MB |
| **No-sort + LZ4** | — | 520ms | **520ms** | **192 b/s** | 61.7 MB |

Key findings:
- **ZSTD(1) ≈ ZSTD(default)** — no speed gain from lower level, same file size
- **Parquet encoding is the bottleneck**, not compression (416ms even uncompressed)
- **Sort costs 35%** of total time — skip for ultra-fast chains
- **LZ4 is 18% faster** than ZSTD with 1.8x larger files

### MegaETH: real data from R2

Measured from actual MegaETH R2 chunk (`s3://mega-mainnet-1`, 5938 blocks):

| Table | Parquet size | Per block |
|-------|--:|--:|
| blocks | 1.2 MB | 208 bytes |
| transactions | 18.5 MB | 3.3 KB |
| logs | 7.9 MB | 1.4 KB |
| **Total** | **28.6 MB** | **5 KB** |

Only 3 tables (no traces, no statediffs — L2). **63x smaller** per block than ETH mainnet.

| | ETH mainnet | MegaETH |
|---|---|---|
| Per block (parquet) | 311 KB | **5 KB** |
| Per block (Arrow, ~10x) | 2.8 MB | **~50 KB** |
| Tables | 5 | 3 |

MegaETH on portal: **1 EVM block/sec** (mini blocks aggregated). Finalization gap: 2346 blocks.

| Metric | Value |
|--------|-------|
| Unfinalized gap in memory | 2346 blocks × 50 KB = **117 MB** |
| Archival gap on disk | 7956 blocks × 5 KB = **40 MB parquet** |
| Spillover flush 100 blocks | ~5 MB → ~0.5 MB parquet, **<10ms** |
| Estimated flush rate | **10,000+ blocks/sec** |
| Headroom vs 1 block/sec | **10,000x** |

Even at hypothetical mini-block indexing (100/sec), flush rate would be
~1000+ blocks/sec — **10x headroom** without any optimization.

For general ultra-fast chains: use **no-sort + LZ4** (192 blocks/sec for full EVM blocks).
Sort deferred to cold compaction.

### Tier 3: Cold Parquet

Finalized blocks sorted by table `sort_key` and compressed with ZSTD.
Same format as R2 production chunks. Queried via existing `ParquetChunkReader`.

## Query Performance (GCP c2-standard-30, 30 vCPU)

All tiers implement `ChunkReader` trait — transparent to the query pipeline.
`CompositeChunkReader` merges tiers with block range routing.

### Throughput (rps, CPU=30) vs Legacy+RocksDB

| Query | Leg+Rdb | New+Parquet | diff | New+Memory | diff | New+Spillover | diff |
|-------|--:|--:|---|--:|---|--:|---|
| evm/usdc_transfers | 196 | 767 | +292% | 541 | +176% | 768 | +292% |
| evm/calls+logs | 234 | 336 | +44% | 395 | +69% | 364 | +56% |
| evm/traces+diffs | 55 | 59 | +7% | 104 | +89% | 118 | +115% |
| evm/all_blocks | 10557 | 69303 | +557% | 6382 | -40% | 67031 | +535% |
| sol/whirlpool | 206 | 604 | +193% | 716 | +248% | 615 | +199% |
| sol/hard | 148 | 310 | +109% | 515 | +248% | 343 | +132% |
| sol/instr+logs | 104 | 236 | +127% | 253 | +143% | 260 | +150% |
| sol/instr+bal | 362 | 2038 | +463% | 1298 | +259% | 2232 | +517% |
| sol/all_blocks | 17433 | 200392 | +1050% | 27754 | +59% | 137800 | +690% |

**New engine wins all 9/9 queries** vs legacy, from +44% to +1050% faster.

### Scaling behavior

- **Legacy**: plateaus at CPU=8-16 (RocksDB contention)
- **New+Parquet**: scales to CPU=16-24 (disk I/O bound)
- **New+Memory**: scales linearly to CPU=30 on join-heavy queries (no I/O)
- **New+Spillover**: scales like parquet with better RG pruning

### When each tier wins

| Tier | Best for | Why |
|------|----------|-----|
| **Cold parquet** | Full scans, archival queries | Best compression, native arrow-rs stats |
| **Memory** | Chain tip, join-heavy queries (CPU≥8) | Zero I/O, linear CPU scaling |
| **Spillover** | Selective queries, overflow | Sorted parquet with 8K RGs = tight pruning |

## Key Design Choices

### Spillover must be sorted

Without sorting by `sort_key`, row group stats are useless — filter column values
spread across all row groups. Tested: unsorted spillover **lost to legacy** on
`evm/calls+logs` (-25%). After adding sort + 8K row groups: **won all 9 queries**.

### Cost of skipping sort (unsorted spillover)

Measured on 12-core x86_64, CPU=12. Unsorted = blocks in arrival order (no sort by filter keys).

| Query | Leg+Rdb | Sorted spill | Unsorted spill | Sorted vs Leg | Unsorted vs Leg |
|-------|--:|--:|--:|---|---|
| evm/usdc_transfers | 143 | 351 | 208 | +145% | +45% |
| evm/calls+logs | 155 | 187 | **129** | +21% | **-17%** |
| evm/traces+diffs | 39 | 57 | **28** | +46% | **-28%** |
| evm/all_blocks | 8003 | 31624 | 31057 | +295% | +288% |
| sol/whirlpool | 181 | 340 | 339 | +88% | +87% |
| sol/hard | 135 | 186 | **159** | +38% | **+18%** |
| sol/instr+logs | 74 | 126 | 117 | +70% | +58% |
| sol/instr+bal | 355 | 1095 | 1246 | +208% | +251% |
| sol/all_blocks | 10446 | 68876 | 69867 | +559% | +569% |

**Unsorted loses to legacy on 2 queries** (`evm/calls+logs` -17%, `evm/traces+diffs` -28%).
These are join-heavy queries where sort by filter columns (`to`, `callTo`) enables
row group pruning — without sort, all row groups must be scanned.

**Recommendation**: sort spillover by default. Skip sort only for ultra-fast chains
(MegaETH) where flush rate is the bottleneck. Even unsorted, 7/9 queries still beat legacy.

### 8K rows per row group

Smaller row groups = tighter min/max stats = better pruning for selective filters.
Metadata overhead is negligible (115 KB footer for 75 RGs on a 16 MB file = 0.7%).
Tested: 50K RGs → 8K RGs fixed the last remaining loss vs legacy.

### IPC crash log, not parquet WAL

Parquet requires a footer → can't append. Arrow IPC Stream is append-only with
length-prefixed messages. Crash = skip incomplete tail. But IPC is **never queried**
during normal operation — only read on crash recovery to rebuild memory buffer.

### Block range routing in CompositeChunkReader

"Latest" queries (`from_block=head-10`) skip all parquet tiers and hit only memory.
Zero disk I/O for the most common query pattern.

## Memory Estimation

Measured from live portal (2026-03-21, 231 datasets):

| Category | Datasets | Memory |
|----------|---------|--------|
| High-gap (Arbitrum, zkSync, Celo, Base, OP, ETH) | ~6 | ~11.4 GB |
| Medium-gap (Gnosis, Solana) | ~10 | ~0.5 GB |
| Low/zero-gap (everything else) | ~215 | ~0.2 GB |
| **Total** | **231** | **~12 GB** |

With 500MB per-dataset cap: spillover kicks in for Arbitrum (4.2 GB unfinalized → 500 MB
in memory, rest on disk as spillover parquet). MegaETH fits entirely in memory (117 MB).
Disk penalty for spillover: 1-2ms vs μs — still 10-100x faster than legacy RocksDB.

## Reorg Handling

- **Memory**: truncate buffer to fork point (instant)
- **Crash log**: ftruncate IPC file (μs)
- **Spillover parquet**: delete affected files (rare, most reorgs ≤2 blocks)
- **Cold parquet**: immutable (only finalized blocks)

## Write Path Performance

| Metric | Value |
|--------|-------|
| Block push | 255 ns |
| Push p99 | 320 ns |
| Push during background flush | 280 ns (zero impact) |
| Query during background flush | +3.6% (noise) |
| Spillover flush (sorted+ZSTD) | 108 blocks/sec |
| Spillover flush (sorted+LZ4) | 123 blocks/sec |
| Spillover flush (no-sort+LZ4) | 192 blocks/sec |
| Max write throughput | 642K blocks/sec |

## Files

| File | Purpose |
|------|---------|
| `src/scan/memory_backend.rs` | Memory buffer with block-level pruning |
| `src/scan/composite_reader.rs` | Merges tiers with block range routing |
| `src/scan/crash_log.rs` | IPC crash log writer + recovery |
| `src/scan/parquet_writer.rs` | Sorted parquet flush (8K RGs, ZSTD) |
| `src/scan/dataset_store.rs` | Orchestrates all tiers per dataset |
