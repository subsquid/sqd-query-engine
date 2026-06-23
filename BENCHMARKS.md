# Benchmarks

Data: R2 production chunks. The EVM suite runs a **chunk matrix**:

- **small** — `data/evm/chunk`, 224 blocks (24550597–24550820), ~70 MB
- **big** — `data/evm/big`, 1592 blocks (22562400–22563991), ~287 MB (dense,
  USDC-active; downloaded, gitignored — the matrix auto-skips it if absent)

Solana: 48 blocks, ~27 MB. Jemalloc allocator, pre-cached ParquetTable. Chunk
paths override via `$EVM_CHUNK` (small), `$EVM_CHUNK_BIG` (big), `$SOL_CHUNK`.

The legacy engine (`sqd-query`) runs the **identical** query JSON on the
**identical** chunk via `--features legacy-query`; the RPC outputs are verified
byte-identical between engines (`cargo bench --bench profile --features
legacy-query -- rpc/getLogs --compare`).

## Query catalog

All RPC queries reproduce the real Ethereum JSON-RPC method semantics and run
on `data/evm/chunk` (block 24550620 for the single-block calls; getLogs windows
anchored there).

| Name | RPC / indexer equivalent | Shape |
|------|--------------------------|-------|
| `evm/usdc_transfers`            | indexer log scan                  | USDC `Transfer` logs, whole chunk |
| `evm/contract_calls+logs`       | indexer tx + join                 | USDT txs + their logs |
| `evm/usdc_traces+diffs`         | indexer multi-table               | USDC traces + state diffs + tx |
| `evm/sparse`                    | empty-result floor                | all logs of a non-existent contract (0 rows) |
| `evm/all_blocks`                | full scan                         | every block header (`includeAllBlocks`) |
| `evm/all_txs`                   | full scan                         | every transaction (`transactions:[{}]`) |
| `evm/all_logs`                  | full scan                         | every log (`logs:[{}]`) |
| `evm/all_traces`                | full scan                         | every trace (`traces:[{}]`) |
| `evm/all_statediffs`            | full scan                         | every state diff (`stateDiffs:[{}]`) |
| `rpc/getBlockByNumber`          | `eth_getBlockByNumber(N, true)`   | one block, header + full tx objects (~17 cols) |
| `rpc/getBlockByNumber:txHashes` | `eth_getBlockByNumber(N, false)`  | one block, header + tx **hashes** only |
| `rpc/getBlockReceipts`          | `eth_getBlockReceipts(N)`         | one block, all tx receipts + their logs |
| `rpc/trace_block`               | `trace_block(N)`                  | one block, **all** traces (call/create/suicide/reward), full action+result fields |
| `rpc/getLogs:1blk`              | `eth_getLogs` (1-block range)     | USDC `Transfer` logs, 1 block (63 logs) |
| `rpc/getLogs:10blk`             | `eth_getLogs` (10-block range)    | USDC `Transfer` logs, 10 blocks (729 logs) |
| `rpc/getLogs:100blk`            | `eth_getLogs` (100-block range)   | USDC `Transfer` logs, 100 blocks (7784 logs) |
| `sol/whirlpool_swap`            | indexer                           | Whirlpool swaps + inner ix + tx |
| `sol/hard`                      | indexer                           | Meteora DLMM, all relations |
| `sol/instr+logs`                | indexer                           | Jupiter ix + logs |
| `sol/instr+balances`            | indexer                           | Whirlpool ix + balance changes |

### What the RPC numbers reveal

EVM tables are sorted by **filter columns** (logs: `topic0 → address →
block_number`; txs: `sighash → to → block_number`), *not* by `block_number`.
Two consequences fall straight out of the measurements:

- **`getLogs` is filter-aligned and scales with range.** Row-group statistics on
  `topic0`/`address` prune almost everything, so a 1-block fetch is ~0.5 ms and
  the cost grows with the window (1 → 10 → 100 blocks).
- **Single-block calls can't prune by block.** `getBlockByNumber` /
  `getBlockReceipts` select one block with no filter-column predicate, so within
  every row group `block_number` spans the whole chunk and *no* row group is
  pruned — every row group is scanned to gather one block's rows.
- **The single-block cost is dominated by column decode, not the scan itself.**
  `getBlockByNumber:txHashes` (header + hashes, 2 tx columns) is **~4× faster**
  than the full variant (~17 tx columns) on the *same* scan — column projection
  is what pays off. So full `getBlockByNumber`/`getBlockReceipts` are the
  expensive RPC calls because they decode every column of every tx in the block —
  and `trace_block` is the heaviest of all (14.9 ms), because the traces table is
  the widest in the chunk and it decodes the full call/create/result field set.

Both engines pay the layout cost; the new engine pays it 1.4×–4.6× more
efficiently across every RPC call. Note a `block_number` *index can't* fix this:
because `block_number` is the 3rd sort key (not a prefix) it's smeared uniformly
across every row group and page, so row-group stats, the page index, and even a
row-position posting list all fail to prune (a block's rows are physically
scattered, and parquet decodes at page granularity). The only real lever is a
separate **block-clustered storage tier** at ingestion (writer-side, ~2× storage,
conflicts with the filter-optimized layout) — justified only if single-block RPC
becomes a first-class workload. Otherwise the win is making the unavoidable scan
cheap (column projection + RowFilter deferral, already done; P1 JSON; LZ4 codec).

## Prior run — x86_64: Intel Xeon E-2136 (6C/12T @ 3.3GHz), 64GB DDR4, Linux

> Historical numbers from an **earlier query set** (includes the since-removed
> `all_blocks` full-scan bench, no RPC queries). See the Apple M2 Pro section
> below for the current RPC-inclusive comparison.

### Latency (single-threaded, median)

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

### Throughput (requests/sec, 5s per concurrency level)

| Benchmark                  | CPU | New        | Legacy   | Diff            |
|----------------------------|-----|------------|----------|-----------------|
| evm/usdc_transfers         | 1   | **104**    | 80       | **30% faster**  |
|                            | 4   | **295**    | 202      | **46% faster**  |
|                            | 8   | **441**    | 268      | **64% faster**  |
|                            | 12  | **500**    | 289      | **73% faster**  |
| evm/contract_calls+logs    | 1   | **59**     | 51       | **17% faster**  |
|                            | 4   | **152**    | 98       | **56% faster**  |
|                            | 8   | **190**    | 111      | **72% faster**  |
|                            | 12  | **204**    | 111      | **83% faster**  |
| evm/usdc_traces+statediffs | 1   | 15         | **17**   | 13% slower      |
|                            | 4   | **30**     | 24       | **25% faster**  |
|                            | 8   | **34**     | 25       | **39% faster**  |
|                            | 12  | **35**     | 25       | **42% faster**  |
| evm/all_blocks             | 1   | **5969**   | 1514     | **294% faster** |
|                            | 4   | **23251**  | 4671     | **398% faster** |
|                            | 8   | **35040**  | 7506     | **367% faster** |
|                            | 12  | **37468**  | 8528     | **339% faster** |
| sol/whirlpool_swap         | 1   | **151**    | 124      | **22% faster**  |
|                            | 4   | **285**    | 172      | **66% faster**  |
|                            | 8   | **316**    | 185      | **71% faster**  |
|                            | 12  | **325**    | 181      | **80% faster**  |
| sol/hard (Meteora DLMM)    | 1   | **94**     | 80       | **17% faster**  |
|                            | 4   | **160**    | 100      | **59% faster**  |
|                            | 8   | **174**    | 106      | **65% faster**  |
|                            | 12  | **168**    | 104      | **62% faster**  |
| sol/instr+logs             | 1   | **43**     | 41       | **5% faster**   |
|                            | 4   | **103**    | 75       | **38% faster**  |
|                            | 8   | **131**    | 78       | **67% faster**  |
|                            | 12  | **134**    | 86       | **57% faster**  |
| sol/instr+balances         | 1   | **485**    | 302      | **61% faster**  |
|                            | 4   | **866**    | 501      | **73% faster**  |
|                            | 8   | **996**    | 521      | **91% faster**  |
|                            | 12  | **1060**   | 538      | **97% faster**  |
| sol/all_blocks             | 1   | **15846**  | 2039     | **677% faster** |
|                            | 4   | **62810**  | 6979     | **800% faster** |
|                            | 8   | **95900**  | 12330    | **677% faster** |
|                            | 12  | **102320** | 15270    | **570% faster** |

### Summary

| Median           | CPU=1           | CPU=4           | CPU=8           | CPU=12          |
|------------------|-----------------|-----------------|-----------------|-----------------|
| General queries  | **17% faster**  | **56% faster**  | **67% faster**  | **73% faster**  |
| Only full blocks | **485% faster** | **599% faster** | **522% faster** | **455% faster** |

---

## Apple M2 Pro (12-core), 32GB, macOS — current

Current working-tree build, RPC-inclusive query set. New = `sqd-query-engine`,
Legacy = `sqd-query`; identical query JSON and chunk for both. "New vs legacy"
is `legacy / new` (>1 means the new engine is faster).

### Latency (single-threaded, median of 20 samples × 5 iters)

| Benchmark                         | New          | Legacy    | New vs legacy    |
|-----------------------------------|--------------|-----------|------------------|
| evm/usdc_transfers                | **6.64 ms**  | 7.90 ms   | **1.19× faster** |
| evm/contract_calls+logs           | **11.13 ms** | 12.95 ms  | **1.16× faster** |
| evm/usdc_traces+diffs             | 44.01 ms     | 40.68 ms  | 1.08× slower     |
| **rpc/getBlockByNumber**          | **5.67 ms**  | 11.13 ms  | **1.96× faster** |
| **rpc/getBlockByNumber:txHashes** | **1.28 ms**  | 5.94 ms   | **4.63× faster** |
| **rpc/getBlockReceipts**          | **6.19 ms**  | 17.10 ms  | **2.76× faster** |
| **rpc/trace_block**               | **14.88 ms** | 47.53 ms  | **3.19× faster** |
| **rpc/getLogs:1blk**              | **0.51 ms**  | 2.36 ms   | **4.65× faster** |
| **rpc/getLogs:10blk**             | **0.81 ms**  | 2.66 ms   | **3.26× faster** |
| **rpc/getLogs:100blk**            | **4.07 ms**  | 5.67 ms   | **1.39× faster** |
| sol/whirlpool_swap                | **4.87 ms**  | 7.00 ms   | **1.44× faster** |
| sol/hard (Meteora DLMM)           | **8.36 ms**  | 10.91 ms  | **1.30× faster** |
| sol/instr+logs                    | **15.69 ms** | 17.30 ms  | **1.10× faster** |
| sol/instr+balances                | **1.62 ms**  | 3.52 ms   | **2.17× faster** |

The new engine is faster on every RPC call (1.4×–4.6×). Two things stand out:
`getBlockByNumber` with tx hashes (1.28 ms) is **4.4× cheaper** than with full
tx objects (5.67 ms) — column projection, not block lookup, dominates the cost;
and `getLogs` scales cleanly with range (0.51 → 0.81 → 4.07 ms for 1/10/100
blocks). The only regression is the heavy multi-table `usdc_traces+diffs`
(1.08× slower).

### Throughput (requests/sec, 5s per concurrency level)

| Benchmark                       | CPU | New      | Legacy | New/Leg   |
|---------------------------------|-----|----------|--------|-----------|
| evm/usdc_transfers              | 1   | **156**  | 135    | **1.15×** |
|                                 | 4   | **479**  | 425    | **1.13×** |
|                                 | 8   | **726**  | 625    | **1.16×** |
|                                 | 12  | **866**  | 722    | **1.20×** |
| evm/contract_calls+logs         | 1   | **89**   | 76     | **1.17×** |
|                                 | 4   | **263**  | 215    | **1.22×** |
|                                 | 8   | **366**  | 269    | **1.36×** |
|                                 | 12  | **386**  | 296    | **1.31×** |
| evm/usdc_traces+diffs           | 1   | 22       | 24     | 0.94×     |
|                                 | 4   | **47**   | 43     | **1.10×** |
|                                 | 8   | **56**   | 44     | **1.26×** |
|                                 | 12  | **59**   | 44     | **1.34×** |
| **rpc/getBlockByNumber**        | 1   | **177**  | 96     | **1.84×** |
|                                 | 4   | **247**  | 193    | **1.28×** |
|                                 | 8   | **263**  | 224    | **1.17×** |
|                                 | 12  | **265**  | 234    | **1.13×** |
| **rpc/getBlockByNumber:txHashes** | 1 | **770**  | 163    | **4.72×** |
|                                 | 4   | **1245** | 533    | **2.34×** |
|                                 | 8   | **1324** | 707    | **1.87×** |
|                                 | 12  | **1355** | 773    | **1.75×** |
| **rpc/getBlockReceipts**        | 1   | **160**  | 59     | **2.73×** |
|                                 | 4   | **207**  | 119    | **1.74×** |
|                                 | 8   | **205**  | 126    | **1.63×** |
|                                 | 12  | **197**  | 111    | **1.78×** |
| **rpc/trace_block**             | 1   | **66**   | 21     | **3.18×** |
|                                 | 4   | **78**   | 47     | **1.64×** |
|                                 | 8   | **80**   | 56     | **1.43×** |
|                                 | 12  | **81**   | 57     | **1.41×** |
| **rpc/getLogs:1blk**            | 1   | **2014** | 445    | **4.53×** |
|                                 | 4   | **5756** | 1381   | **4.17×** |
|                                 | 8   | **6427** | 1880   | **3.42×** |
|                                 | 12  | **6923** | 2101   | **3.30×** |
| **rpc/getLogs:10blk**           | 1   | **1182** | 396    | **2.99×** |
|                                 | 4   | **3985** | 1215   | **3.28×** |
|                                 | 8   | **4633** | 1777   | **2.61×** |
|                                 | 12  | **5118** | 2005   | **2.55×** |
| **rpc/getLogs:100blk**          | 1   | **248**  | 184    | **1.35×** |
|                                 | 4   | **877**  | 631    | **1.39×** |
|                                 | 8   | **1348** | 868    | **1.55×** |
|                                 | 12  | **1323** | 933    | **1.42×** |
| sol/whirlpool_swap              | 1   | **207**  | 151    | **1.37×** |
|                                 | 4   | **429**  | 268    | **1.60×** |
|                                 | 8   | **481**  | 306    | **1.57×** |
|                                 | 12  | **456**  | 313    | **1.45×** |
| sol/hard (Meteora DLMM)         | 1   | **121**  | 95     | **1.28×** |
|                                 | 4   | **228**  | 144    | **1.59×** |
|                                 | 8   | **260**  | 148    | **1.75×** |
|                                 | 12  | **263**  | 152    | **1.73×** |
| sol/instr+logs                  | 1   | **64**   | 59     | **1.09×** |
|                                 | 4   | **194**  | 153    | **1.27×** |
|                                 | 8   | **260**  | 186    | **1.40×** |
|                                 | 12  | **257**  | 171    | **1.51×** |
| sol/instr+balances              | 1   | **614**  | 289    | **2.13×** |
|                                 | 4   | **1274** | 717    | **1.78×** |
|                                 | 8   | **1505** | 863    | **1.74×** |
|                                 | 12  | **1317** | 879    | **1.50×** |

### Summary

The new engine wins **every** RPC benchmark at every concurrency level. The
cheapest RPC calls — single-block `getLogs` (up to 6.9k rps) and
`getBlockByNumber:txHashes` (up to 1.4k rps) — are also where it leads by the
widest margin (3–5×), because legacy's per-query fixed overhead dominates there.
Full-block `getBlockByNumber`/`getBlockReceipts` saturate early (each request
already fans out across cores internally), so their rps is flat across
concurrency but still 1.1–2.7× ahead of legacy.

---

## Memory (heap usage)

Measured with a counting allocator wrapping jemalloc (`cargo bench --bench
memory`). Two metrics per query: **alloc/query** = bytes allocated per request
(allocator churn / pressure), and **peak heap @ CPU=8** = peak simultaneously-live
heap while serving 8 concurrent requests (working set). Requested bytes, mmap'd
parquet excluded; the chunk is loaded during warmup so the peak is per-query
working memory.

| Benchmark                       | new a/q | leg a/q | new peak@8 | leg peak@8 |
|---------------------------------|---------|---------|------------|------------|
| evm/usdc_transfers              | 50 MB   | 55 MB   | 157 MB     | 159 MB     |
| evm/contract_calls+logs         | 87 MB   | 107 MB  | 160 MB ⚠️  | 110 MB     |
| evm/usdc_traces+diffs           | 642 MB  | 678 MB  | 343 MB ⚠️  | 209 MB     |
| rpc/getBlockByNumber            | 160 MB  | 163 MB  | 19.9 MB    | 19.5 MB    |
| rpc/getBlockByNumber:txHashes   | 34 MB   | 36 MB   | **6.4 MB** | 10.8 MB    |
| rpc/getBlockReceipts            | 196 MB  | 196 MB  | **13 MB**  | 22 MB      |
| rpc/trace_block                 | 580 MB  | 614 MB  | **30 MB**  | 57 MB      |
| rpc/getLogs:1blk                | 6.3 MB  | 14 MB   | **4.6 MB** | 18 MB      |
| rpc/getLogs:10blk               | 8.0 MB  | 16 MB   | **8.1 MB** | 18 MB      |
| rpc/getLogs:100blk              | 30 MB   | 36 MB   | 70 MB ⚠️   | 58 MB      |
| sol/whirlpool_swap              | 65 MB   | 74 MB   | **15 MB**  | 59 MB      |
| sol/hard (Meteora DLMM)         | 143 MB  | 164 MB  | **19 MB**  | 59 MB      |
| sol/instr+logs                  | 116 MB  | 120 MB  | 172 MB ⚠️  | 104 MB     |
| sol/instr+balances              | 23 MB   | 30 MB   | **7.9 MB** | 22 MB      |

Two findings:

- **Allocator churn (alloc/query): the new engine allocates less on every
  query.** Note the huge read-amplification on single-block calls — 160 MB
  allocated to produce a 0.3 MB `getBlockByNumber` (decode every row group to
  find one block). Column projection cuts it ~5×: `:txHashes` is 34 MB vs 160 MB.
- **Peak working set (peak@8): a speed/memory trade-off.** On RPC and light
  queries the new engine holds **2–4× less** simultaneous heap (e.g. trace_block
  30 vs 57 MB, getLogs:1blk 4.6 vs 18 MB, sol/hard 19 vs 59 MB). But on the heavy
  multi-table indexer queries (⚠️ `usdc_traces+diffs`, `contract_calls+logs`,
  `instr+logs`, `getLogs:100blk`) it holds **1.4–1.65× more** — because it fans
  each query across cores, so many row-group decode buffers are live at once.
  That intra-query parallelism is exactly what buys the throughput; the cost is a
  larger transient working set on the heaviest queries.

---

## Chunk matrix + full-scan family (Apple M2 Pro, 2026-06-23)

The EVM latency/throughput/memory suites now run every query across the chunk
matrix (small + big), and add a **full-scan family** (`all_blocks/all_txs/
all_logs/all_traces/all_statediffs`) — one unfiltered `[{}]` scan per wide
table. These exercise two response-budget optimizations, both byte-identical to
legacy (the exact `apply_weight_limit` always does the final trim):

- **Single-table two-phase scan** (full-scan family): a single-table full scan
  with no real filter first does a cheap *narrow* pre-scan (block number + the
  `*_size` weight columns) to find the 20 MB block cutoff, then scans the wide
  data columns only up to that cutoff.
- **Multi-table budget early-stop** (`usdc_traces+diffs`): for a *block-sorted*
  table (one whose `sort_key` leads with `block_number`, e.g. `statediffs`),
  the scanner reads matching row groups in block order in parallel waves and
  stops once the cumulative response weight — this table plus the already-scanned
  tables plus headers — crosses 20 MB. Because the table is block-sorted, the
  cutoff prunes whole row groups, so wide columns decode only for blocks that
  can actually be emitted. (A purely sequential early-stop was measured 3–4×
  slower, so the waves preserve intra-wave parallelism.)

### Latency (new vs legacy; profile harness, mean over 60–120 iters)

| Benchmark | small new | small leg | big new | big leg | big ratio |
|-----------|-----------|-----------|---------|---------|-----------|
| evm/usdc_transfers      | 6.1 ms  | 8.3 ms  | 14.1 ms  | 16.8 ms  | **1.19×** |
| evm/contract_calls+logs | 10.4 ms | 13.3 ms | 19.7 ms  | 25.3 ms  | **1.29×** |
| evm/usdc_traces+diffs   | 45.1 ms | 41.3 ms | 87.4 ms  | 86.4 ms  | **0.99×** |
| evm/sparse              | 0.48 ms | 1.36 ms | 0.93 ms  | 1.97 ms  | **2.12×** |
| evm/all_blocks          | 0.14 ms | 0.79 ms | 0.69 ms  | 1.41 ms  | **2.04×** |
| evm/all_txs             | 27.4 ms | 32.2 ms | 38.7 ms  | 52.5 ms  | **1.36×** |
| evm/all_logs            | 29.9 ms | 35.8 ms | 50.5 ms  | 86.7 ms  | **1.72×** |
| evm/all_traces          | 57.4 ms | 66.9 ms | 109.7 ms | 206.6 ms | **1.88×** |
| evm/all_statediffs      | 35.2 ms | 56.9 ms | 72.0 ms  | 160.1 ms | **2.22×** |

The new engine wins the entire full-scan class on both chunks. The former weak
spot `usdc_traces+diffs` on the big chunk improved from **0.81× to 0.99×** (at
parity with legacy) thanks to the multi-table budget early-stop.

### Memory on the big chunk (alloc/query, peak heap @ CPU=8)

| Benchmark | new a/q | leg a/q | new peak | leg peak |
|-----------|---------|---------|----------|----------|
| evm/all_blocks        | 4 MB       | 5 MB     | 10 MB      | 9 MB   |
| evm/all_txs           | 667 MB     | 661 MB   | 486 MB     | 376 MB |
| evm/all_logs          | 721 MB     | 717 MB   | 455 MB     | 303 MB |
| evm/all_traces        | 1988 MB    | 1909 MB  | 435 MB     | 441 MB |
| evm/all_statediffs    | **465 MB** | 587 MB   | **518 MB** | 626 MB |
| evm/usdc_traces+diffs | **1534 MB** | 1782 MB | 504 MB    | 215 MB |

**State-diff two-phase win.** Before the optimization `all_statediffs` on the
big chunk allocated **2438 MB** / peaked at **5405 MB** (8.5× worse than legacy)
— it scanned, decoded and indexed all 2.06 M rows, then `apply_weight_limit`
discarded ~97 %. The two-phase scan cut that to 465 MB / 518 MB (**5.2× / 10.4×**
less), and latency from 129 ms to 77 ms — now beating legacy on every metric.

**Former weak spot, now fixed.** `usdc_traces+diffs` is multi-table (traces +
state diffs + joins) and falls outside the single-table two-phase gate. The
multi-table budget early-stop brought its big-chunk allocation from **2488 MB to
1534 MB** (−38 %, now below legacy's 1782 MB) and peak heap from **672 MB to
504 MB** (−25 %), with latency at parity (above). Root cause: `statediffs` is
physically **block-sorted**, not address-sorted (the metadata `sort_key` was
corrected to match), so an `address` filter can't prune row groups — but a
`block_number` cap can, which is exactly what the early-stop exploits.

---

## How to run

All commands take an optional `--features legacy-query` to additionally run the
legacy `sqd-query` engine on the same chunk for a side-by-side comparison
(requires the sibling `../data` repo to be present). The EVM suites run the
chunk matrix automatically: `data/evm/chunk` (small) plus `data/evm/big` (big)
if present. To point the big slot elsewhere, set `EVM_CHUNK_BIG=/path`.

```bash
# Latency (divan). EVM benches nest one sub-bench per chunk ("small"/"big").
# With the feature, prints parallel `*_legacy` groups.
cargo bench --bench latency
cargo bench --bench latency --features legacy-query
# Scope to one group/query, e.g. the full-scan family on both chunks:
cargo bench --bench latency --features legacy-query -- evm_fullscan

# Throughput (default CPU=8, --all for full sweep). With the feature, prints
# New / Legacy / ratio columns.
cargo bench --bench throughput -- --all
cargo bench --bench throughput --features legacy-query -- --all

# Single-query profiling with stage timing breakdown
cargo bench --bench profile -- "rpc/getLogs" 1 --profile

# Profile the same query through the legacy engine
cargo bench --bench profile --features legacy-query -- "rpc/getBlockReceipts" 1000 --legacy

# Verify the new engine matches legacy byte-for-byte on a query
cargo bench --bench profile --features legacy-query -- "rpc/getBlockByNumber" --compare

# Memory: alloc/query + peak heap under concurrency (add --features legacy-query
# for the legacy columns; --cpu N sets the concurrency, --filter <substr> scopes)
cargo bench --bench memory --features legacy-query -- --cpu 8
```
