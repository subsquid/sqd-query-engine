# JSON Serialization Strategy

## Context

After query execution, the engine serializes result blocks into JSON. Each block is a JSON object
containing a header and table items (logs, transactions, etc.). A typical chunk produces 200-1400
result blocks.

The question: should blocks be serialized in parallel (using rayon) or sequentially?

## Three Approaches Tested

### 1. Parallel (`par_iter` per block)

```rust
let block_jsons: Vec<Vec<u8> > = selected_blocks
.par_iter()
.map( | & block_num| generate_block_json(block_num))
.collect();
```

Each block becomes an individual rayon task. For 1348 blocks, rayon creates ~1348 tasks
distributed across its thread pool (12 workers on M2 Pro).

**How it works:** rayon splits the slice recursively using work-stealing. The calling thread
participates as a worker too. `collect()` blocks until all tasks complete.

### 2. Chunked parallel (`par_chunks(128)`)

```rust
let chunk_results: Vec<Vec<Vec<u8> > > = selected_blocks
.par_chunks(128)
.map( | chunk| chunk.iter().map( | & bn| generate_block_json(bn)).collect())
.collect();
```

Blocks are grouped into batches of 128. Each batch becomes one rayon task that processes its
blocks sequentially. For 1348 blocks: `ceil(1348/128) = 11` rayon tasks instead of 1348.

**How it works:** same rayon mechanics, but each task does more work. Fewer tasks = less
scheduling overhead and less contention for the thread pool.

### 3. Sequential (current)

```rust
for & block_num in & selected_blocks {
json_writer.begin_item() ?;
write_block_json(json_writer.buf_mut(), block_num);
}
```

Blocks are serialized one by one on the calling thread. No rayon involvement. No intermediate
`Vec<Vec<u8>>` allocation — writes directly into the output buffer.

**How it works:** simple loop, zero overhead. The calling thread does all the work itself.

## Benchmark Results

All three approaches benchmarked on the same fixture data, same session.

### Latency (single-threaded, ms, lower is better)

| Benchmark               | Parallel  | Chunked(128) | Sequential | Legacy |
|-------------------------|-----------|--------------|------------|--------|
| evm/usdc_transfers      | **3.55**  | 3.75         | 6.73       | 4.63   |
| evm/contract_calls+logs | **5.99**  | 6.12         | 5.92       | 8.21   |
| evm/bayc_traces+diffs   | 7.25      | **7.33**     | 7.26       | 6.80   |
| evm/all_blocks          | 0.82      | **0.80**     | 0.87       | 0.99   |
| sol/whirlpool_swap      | 16.33     | **13.76**    | 15.21      | 14.16  |
| sol/instr+logs          | **11.26** | 11.71        | 12.62      | 11.82  |
| sol/instr+balances      | **6.78**  | 7.11         | 8.01       | 7.31   |
| sol/all_blocks          | 0.57      | **0.51**     | 0.53       | 0.73   |

Parallel is best for latency when there are many blocks with heavy per-block JSON (e.g.,
USDC with 1348 blocks of log data: 3.55 vs 6.73ms). For simple header-only queries
(all_blocks), the difference is small.

> **Note:** Latency numbers here measure a single isolated query. In production, the worker
> always handles multiple concurrent queries, so single-query latency is not a realistic
> scenario. Throughput at CPU=4-12 is the metric that matters.

### Throughput at CPU=4 (rps, higher is better)

| Benchmark               | Parallel | Chunked(128) | Sequential | Legacy |
|-------------------------|----------|--------------|------------|--------|
| evm/usdc_transfers      | **769**  | 733          | 508        | 636    |
| evm/contract_calls+logs | **211**  | 198          | 188        | 158    |
| evm/bayc_traces+diffs   | 203      | 200          | **201**    | 179    |
| evm/all_blocks          | 3930     | 4072         | **4389**   | 3593   |
| sol/whirlpool_swap      | 111      | **117**      | 115        | 95     |
| sol/instr+logs          | **109**  | 103          | 105        | 108    |
| sol/instr+balances      | 285      | **285**      | 275        | 236    |
| sol/all_blocks          | 5767     | 6211         | **7447**   | 5212   |

At CPU=4 it's mixed. Parallel still wins for some queries (USDC, contract_calls+logs),
but sequential already pulls ahead for `all_blocks` queries.

### Throughput at CPU=8 (rps, higher is better)

| Benchmark               | Parallel | Chunked(128) | Sequential | Legacy |
|-------------------------|----------|--------------|------------|--------|
| evm/usdc_transfers      | 804      | 862          | **848**    | 815    |
| evm/contract_calls+logs | 215      | **211**      | 214        | 166    |
| evm/bayc_traces+diffs   | 209      | 206          | **209**    | 181    |
| evm/all_blocks          | 5336     | 5883         | **8161**   | 5079   |
| sol/whirlpool_swap      | 123      | **130**      | 133        | 99     |
| sol/instr+logs          | 114      | 103          | **114**    | 112    |
| sol/instr+balances      | 311      | 315          | **323**    | 246    |
| sol/all_blocks          | 9097     | 9804         | **13989**  | 7575   |

At CPU=8 sequential dominates across the board. The `all_blocks` gap is already huge:
sequential 8161 vs parallel 5336 (+53%).

### Throughput at CPU=12 (rps, higher is better)

| Benchmark               | Parallel | Chunked(128) | Sequential | Legacy |
|-------------------------|----------|--------------|------------|--------|
| evm/usdc_transfers      | 734      | 900          | **979**    | 889    |
| evm/contract_calls+logs | 195      | **221**      | 209        | 174    |
| evm/bayc_traces+diffs   | 204      | 208          | **202**    | 180    |
| evm/all_blocks          | 5410     | 6108         | **9674**   | 5468   |
| sol/whirlpool_swap      | 122      | **130**      | 129        | 99     |
| sol/instr+logs          | 112      | 113          | **115**    | 113    |
| sol/instr+balances      | 306      | 326          | **338**    | 256    |
| sol/all_blocks          | 9234     | 10612        | **16973**  | 8768   |

Sequential wins decisively at high concurrency. The `all_blocks` queries show the biggest
gap: sequential is 77-94% faster than parallel at CPU=12.

### Summary: where each approach wins

| Concurrency | Best approach | Why                                          |
|-------------|---------------|----------------------------------------------|
| CPU=1       | Parallel      | All 12 rayon workers available for one query |
| CPU=4       | Mixed         | Parallel wins some, sequential wins others   |
| CPU=8+      | Sequential    | Rayon pool oversubscription dominates        |

## Why Parallel Hurts Throughput

At CPU=12, twelve OS threads run queries concurrently. Each query that uses `par_iter` for
JSON submits tasks to the **shared global rayon pool** (12 workers).

```
CPU=12 with parallel JSON:
  12 query threads (each blocked on par_iter().collect())
+ 12 rayon workers (processing JSON tasks from all 12 queries)
= 24 active threads on 12 cores = 2x oversubscription
```

The `collect()` call is a **barrier** — the query thread blocks until all its parallel tasks
complete. With 12 queries blocking simultaneously, the rayon pool must interleave tasks from
all queries, causing cache thrashing and scheduling overhead.

```
CPU=12 with sequential JSON:
  12 query threads (each doing its own JSON work)
+ 0 rayon workers for JSON
= 12 active threads on 12 cores = perfect fit
```

For USDC transfers (1348 blocks): parallel creates 1348 tasks x 12 queries = 16,176 rayon
tasks. Chunked(128) reduces this to 11 x 12 = 132 tasks. Sequential: 0 rayon tasks.

## Decision: Sequential

**Chosen approach: Sequential JSON serialization.**

Rationale:

- Production workload is concurrent (CPU=4-12), where sequential wins by 10-94%
- The latency tradeoff (USDC: 6.73ms vs 3.55ms) is acceptable — still under 10ms
- Zero rayon contention means the thread pool is fully available for parquet scanning,
  which is where parallelism matters most (I/O bound, large row groups)
- Simpler code: no intermediate allocations, writes directly to output buffer
- Legacy engine also uses sequential JSON and achieves good throughput

If single-query latency becomes critical in the future, the chunked approach (par_chunks(128))
is a good middle ground — only 6% slower than parallel for latency, while cutting rayon tasks
by 100x.
