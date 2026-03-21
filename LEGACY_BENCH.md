# Legacy Engine Benchmark: Parquet vs sqd_storage

Compares the legacy `sqd-query` engine performance using two storage backends:
- **Parquet** — mmap'd parquet files (same as production R2 chunks)
- **sqd_storage** — legacy custom columnar page format on RocksDB

## Test Environment

- **Server**: Debian 12, x86_64, 12 cores
- **Data**: EVM chunk (224 blocks, ~70 MB parquet), Solana chunk (48 blocks, ~27 MB parquet)
- **Duration**: 3s per concurrency level

## Disk Size

| Dataset | Parquet | sqd_storage |
|---------|---------|-------------|
| EVM     | 69.7 MB | 257.6 MB    |
| Solana  | 26.6 MB | 82.7 MB     |

## Latency (median of 20 runs, single-threaded)
    
| Query | Parquet | sqd_storage | Diff |
|-------|---------|-------------|------|
| evm/usdc_transfers | 15.33ms | 19.79ms | +29% slower |
| evm/contract_calls+logs | 23.03ms | 21.54ms | -6% faster |
| evm/usdc_traces+diffs | 62.64ms | 57.08ms | -9% faster |
| evm/all_blocks | 0.99ms | 0.90ms | -9% faster |
| sol/whirlpool_swap | 9.93ms | 11.39ms | +15% slower |
| sol/hard | 15.38ms | 13.18ms | -14% faster |
| sol/instr+logs | 28.42ms | 28.42ms | 0% |
| sol/instr+balances | 4.17ms | 7.97ms | +91% slower |
| sol/all_blocks | 0.66ms | 0.59ms | -11% faster |

## Throughput (rps)

### CPU=1

| Query | Parquet | sqd_storage | Diff |
|-------|---------|-------------|------|
| evm/usdc_transfers | 68.0 | 52.0 | -24% slower |
| evm/contract_calls+logs | 45.9 | 47.1 | +3% faster |
| evm/usdc_traces+diffs | 15.9 | 17.2 | +8% faster |
| evm/all_blocks | 1169.2 | 1257.2 | +8% faster |
| sol/whirlpool_swap | 104.6 | 86.8 | -17% slower |
| sol/hard | 67.7 | 73.5 | +9% faster |
| sol/instr+logs | 35.4 | 35.0 | -1% slower |
| sol/instr+balances | 249.0 | 125.0 | -50% slower |
| sol/all_blocks | 1515.9 | 1606.2 | +6% faster |

### CPU=4

| Query | Parquet | sqd_storage | Diff |
|-------|---------|-------------|------|
| evm/usdc_transfers | 199.9 | 111.3 | -44% slower |
| evm/contract_calls+logs | 103.6 | 114.6 | +11% faster |
| evm/usdc_traces+diffs | 27.5 | 35.2 | +28% faster |
| evm/all_blocks | 3836.2 | 4298.1 | +12% faster |
| sol/whirlpool_swap | 153.2 | 151.4 | -1% slower |
| sol/hard | 92.4 | 119.9 | +30% faster |
| sol/instr+logs | 79.9 | 65.1 | -19% slower |
| sol/instr+balances | 440.1 | 237.0 | -46% slower |
| sol/all_blocks | 5390.4 | 5706.1 | +6% faster |

### CPU=8

| Query | Parquet | sqd_storage | Diff |
|-------|---------|-------------|------|
| evm/usdc_transfers | 255.7 | 135.5 | -47% slower |
| evm/contract_calls+logs | 124.0 | 142.8 | +15% faster |
| evm/usdc_traces+diffs | 27.8 | 38.1 | +37% faster |
| evm/all_blocks | 7057.2 | 6792.7 | -4% slower |
| sol/whirlpool_swap | 160.1 | 165.7 | +4% faster |
| sol/hard | 92.3 | 129.0 | +40% faster |
| sol/instr+logs | 91.3 | 72.5 | -21% slower |
| sol/instr+balances | 500.0 | 297.7 | -40% slower |
| sol/all_blocks | 10456.4 | 9782.8 | -6% slower |

### CPU=12

| Query | Parquet | sqd_storage | Diff |
|-------|---------|-------------|------|
| evm/usdc_transfers | **278.4** | 141.5 | -49% slower |
| evm/contract_calls+logs | 130.6 | **153.8** | +18% faster |
| evm/usdc_traces+diffs | 27.9 | **39.2** | +40% faster |
| evm/all_blocks | **8545.3** | 8345.7 | -2% slower |
| sol/whirlpool_swap | 162.9 | **172.5** | +6% faster |
| sol/hard | 93.6 | **132.3** | +41% faster |
| sol/instr+logs | **93.2** | 72.5 | -22% slower |
| sol/instr+balances | **522.6** | 313.5 | -40% slower |
| sol/all_blocks | **13051.4** | 12360.1 | -5% slower |

## Analysis

### Where sqd_storage wins (at CPU=12)
- **evm/usdc_traces+diffs**: 39.2 vs 27.9 rps (**+40%**) — heavy multi-table join (traces + state diffs + transactions)
- **sol/hard**: 132.3 vs 93.6 rps (**+41%**) — Meteora DLMM with all relations
- **evm/contract_calls+logs**: 153.8 vs 130.6 rps (**+18%**) — transaction filter with log join
- **sol/whirlpool_swap**: 172.5 vs 162.9 rps (**+6%**) — instruction with inner instructions + tx join

### Where Parquet wins (at CPU=12)
- **evm/usdc_transfers**: 278.4 vs 141.5 rps (**-49%**) — selective single-table log filter
- **sol/instr+balances**: 522.6 vs 313.5 rps (**-40%**) — selective instruction + balance join
- **sol/instr+logs**: 93.2 vs 72.5 rps (**-22%**) — instruction with logs
- **sol/all_blocks**: 13051.4 vs 12360.1 rps (**-5%**) — full block scan

### Scaling behavior
- **Parquet** scales well on selective queries but plateaus on heavy queries (traces+diffs: ~28 rps at CPU=4/8/12)
- **sqd_storage** scales more linearly on heavy queries (traces+diffs: 17 -> 35 -> 38 -> 39 rps)
- Both backends plateau on the heaviest queries, suggesting CPU-bound bottleneck in the query engine itself

### Why sqd_storage wins on heavy queries
The legacy engine reads **only key columns** for relation joins, then reads remaining columns only for matched rows. sqd_storage's columnar page format supports efficient partial-column reads (each column stored in separate pages). Parquet also supports this but has higher per-column-read overhead (page decompression, dictionary decoding).

### Why Parquet wins on selective queries
Row group statistics and predicate pushdown allow Parquet to skip entire row groups. For selective queries (e.g., USDC transfers filtering by topic0+address), most row groups are skipped entirely. sqd_storage reads all data and filters in-memory.
