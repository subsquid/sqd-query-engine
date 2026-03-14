# Benchmarks

Data: R2 production chunks (EVM: 224 blocks, ~70 MB; Solana: 48 blocks, ~27 MB).
Jemalloc allocator, pre-cached ParquetTable.

## x86_64: Intel Xeon E-2136 (6C/12T @ 3.3GHz), 64GB DDR4, Linux

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

## Apple M2 Pro (12-core), 32GB, macOS

### Latency (single-threaded, median, divan 20x100)

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

### Throughput (requests/sec, 5s per concurrency level)

| Benchmark                  | CPU | New       | Legacy  | Diff            |
|----------------------------|-----|-----------|---------|-----------------|
| evm/usdc_transfers         | 1   | **147**   | 124     | **18% faster**  |
|                            | 4   | **487**   | 357     | **36% faster**  |
|                            | 8   | **735**   | 503     | **46% faster**  |
|                            | 12  | **775**   | 551     | **41% faster**  |
| evm/contract_calls+logs    | 1   | **80**    | 75      | **7% faster**   |
|                            | 4   | **231**   | 159     | **45% faster**  |
|                            | 8   | **292**   | 184     | **59% faster**  |
|                            | 12  | **290**   | 189     | **53% faster**  |
| evm/usdc_traces+statediffs | 1   | 20        | **24**  | 17% slower      |
|                            | 4   | **40**    | 31      | **27% faster**  |
|                            | 8   | **47**    | 30      | **57% faster**  |
|                            | 12  | **48**    | 33      | **45% faster**  |
| evm/all_blocks             | 1   | **7515**  | 2381    | **216% faster** |
|                            | 4   | **26312** | 9035    | **191% faster** |
|                            | 8   | **31128** | 14075   | **121% faster** |
|                            | 12  | **34676** | 17904   | **94% faster**  |
| sol/whirlpool_swap         | 1   | 180       | **439** | 59% slower      |
|                            | 4   | **356**   | 250     | **42% faster**  |
|                            | 8   | **389**   | 272     | **43% faster**  |
|                            | 12  | **409**   | 284     | **44% faster**  |
| sol/hard (Meteora DLMM)    | 1   | **114**   | 107     | **7% faster**   |
|                            | 4   | **200**   | 143     | **40% faster**  |
|                            | 8   | **221**   | 146     | **51% faster**  |
|                            | 12  | **228**   | 150     | **52% faster**  |
| sol/instr+logs             | 1   | **60**    | 56      | **6% faster**   |
|                            | 4   | **179**   | 145     | **23% faster**  |
|                            | 8   | **228**   | 145     | **57% faster**  |
|                            | 12  | **242**   | 146     | **66% faster**  |
| sol/instr+balances         | 1   | **517**   | 345     | **50% faster**  |
|                            | 4   | **1090**  | 719     | **52% faster**  |
|                            | 8   | **1226**  | 719     | **71% faster**  |
|                            | 12  | **1210**  | 738     | **64% faster**  |
| sol/all_blocks             | 1   | **20710** | 3704    | **459% faster** |
|                            | 4   | **49927** | 26872   | **86% faster**  |
|                            | 8   | **47267** | 26872   | **76% faster**  |
|                            | 12  | **42507** | 31831   | **34% faster**  |

### Summary

| Median           | CPU=1           | CPU=4           | CPU=8           | CPU=12          |
|------------------|-----------------|-----------------|-----------------|-----------------|
| General queries  | **7% faster**   | **40% faster**  | **53% faster**  | **49% faster**  |
| Only full blocks | **337% faster** | **139% faster** | **99% faster**  | **64% faster**  |

---

## How to run

```bash
# Latency benchmarks (divan, 20x100)
cargo bench --bench latency

# Throughput benchmarks (default CPU=8, --all for full sweep)
cargo bench --bench throughput -- --all

# Single query profiling with timing breakdown
cargo bench --bench profile -- "evm/usdc_transfers" 1 --profile
```
