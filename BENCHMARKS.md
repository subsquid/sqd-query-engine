# Benchmarks

Data: R2 production chunks (EVM: 224 blocks, ~70 MB; Solana: 48 blocks, ~27 MB).
Jemalloc allocator.

---

# Storage Tiers

Legacy engine (sqd_storage/RocksDB) vs new engine across all storage tiers:
cold (parquet), hot memory (Arrow RecordBatches), hot spillover (sorted parquet, 8K row groups).

> **Note on Memory tier benchmark**: All backends are tested with the same 224-block EVM chunk
> (or 48-block Solana chunk) loaded entirely into memory. In production, the memory buffer holds
> only the last few blocks (chain tip), while older blocks live in spillover parquet.
> This makes the Memory results **pessimistic** — real chain-tip queries scan 5-10 blocks
> (microseconds), not 224 blocks. The benchmark tests worst-case memory performance to ensure
> it stays competitive even when the buffer is large.

### Backends

| Backend | Engine | Storage | Description |
|---------|--------|---------|-------------|
| **Leg+Parquet** | Legacy | Parquet | Legacy engine on mmap'd parquet (old cold) |
| **Leg+RocksDB** | Legacy | sqd_storage | Legacy engine on RocksDB columnar pages (old hot) |
| **New+Parquet** | New | Parquet | New engine on mmap'd parquet (new cold) |
| **New+Memory** | New | In-memory | Arrow RecordBatches in memory (new hot, chain tip) |
| **New+Spillover** | New | Parquet | Sorted parquet from memory flush (new hot, overflow) |

## OVH Dedicated: Intel Xeon E-2136 (6C/12T @ 3.3GHz), 64GB DDR4, NVMe SSD

### Summary (throughput rps, CPU=12)

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 270 | 377 | +40% | | 141 | 266 | +89% | 378 | +168% |
| evm/calls+logs | 129 | 186 | +44% | | 155 | 271 | +75% | 231 | +49% |
| evm/traces+diffs | 27 | 29 | +8% | | 39 | 64 | +64% | 66 | +69% |
| evm/all_blocks | 8526 | 32180 | +277% | | 9193 | 11478 | +25% | 42847 | +366% |
| sol/whirlpool | 170 | 312 | +84% | | 169 | 397 | +135% | 458 | +171% |
| sol/hard | 95 | 177 | +86% | | 128 | 320 | +150% | 287 | +124% |
| sol/instr+logs | 95 | 119 | +25% | | 73 | 142 | +95% | 141 | +93% |
| sol/instr+bal | 521 | 1138 | +118% | | 306 | 687 | +125% | 2262 | +639% |
| sol/all_blocks | 13190 | 92685 | +603% | | 10972 | 51070 | +365% | 114278 | +941% |

### Latency (single-threaded, median of 20 runs)

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 14.57ms | 11.35ms | +22% | | 19.35ms | 21.99ms | -14% | 9.83ms | +49% |
| evm/calls+logs | 22.26ms | 18.95ms | +15% | | 21.03ms | 21.55ms | -2% | 14.73ms | +30% |
| evm/traces+diffs | 65.16ms | 77.05ms | -18% | | 56.49ms | 95.83ms | -70% | 57.37ms | -2% |
| evm/all_blocks | 1.11ms | 347µs | +69% | | 844µs | 1.05ms | -24% | 144µs | +83% |
| sol/whirlpool | 10.25ms | 6.63ms | +35% | | 11.23ms | 16.12ms | -44% | 3.80ms | +66% |
| sol/hard | 15.00ms | 11.37ms | +24% | | 13.83ms | 17.76ms | -28% | 6.08ms | +56% |
| sol/instr+logs | 28.15ms | 24.42ms | +13% | | 28.37ms | 37.91ms | -34% | 21.52ms | +24% |
| sol/instr+bal | 4.48ms | 2.02ms | +55% | | 8.24ms | 9.35ms | -13% | 1.54ms | +81% |
| sol/all_blocks | 794µs | 72µs | +91% | | 615µs | 70µs | +89% | 55µs | +91% |

### Throughput scaling (rps)

<details>
<summary>CPU=1</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 67 | 91 | +36% | | 51 | 44 | -14% | 100 | +96% |
| evm/calls+logs | 45 | 50 | +11% | | 48 | 46 | -4% | 65 | +35% |
| evm/traces+diffs | 15 | 13 | -17% | | 17 | 10 | -41% | 17 | -1% |
| evm/all_blocks | 1143 | 5215 | +356% | | 1273 | 2205 | +73% | 7241 | +469% |
| sol/whirlpool | 104 | 147 | +41% | | 87 | 62 | -29% | 264 | +204% |
| sol/hard | 68 | 87 | +28% | | 70 | 58 | -17% | 163 | +133% |
| sol/instr+logs | 36 | 40 | +11% | | 34 | 26 | -24% | 45 | +32% |
| sol/instr+bal | 241 | 494 | +105% | | 122 | 108 | -11% | 640 | +425% |
| sol/all_blocks | 1496 | 14869 | +893% | | 1558 | 14782 | +849% | 18863 | +1111% |

</details>

<details>
<summary>CPU=4</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 189 | 242 | +28% | | 112 | 164 | +47% | 271 | +143% |
| evm/calls+logs | 104 | 142 | +37% | | 123 | 172 | +40% | 181 | +47% |
| evm/traces+diffs | 27 | 27 | +2% | | 35 | 40 | +14% | 50 | +43% |
| evm/all_blocks | 3697 | 20892 | +465% | | 4394 | 6887 | +57% | 28477 | +548% |
| sol/whirlpool | 151 | 282 | +87% | | 152 | 222 | +46% | 433 | +185% |
| sol/hard | 92 | 160 | +74% | | 118 | 179 | +52% | 273 | +131% |
| sol/instr+logs | 79 | 96 | +23% | | 64 | 83 | +30% | 115 | +80% |
| sol/instr+bal | 444 | 944 | +113% | | 233 | 416 | +79% | 1652 | +609% |
| sol/all_blocks | 5701 | 58424 | +924% | | 5595 | 36997 | +561% | 74203 | +1226% |

</details>

<details>
<summary>CPU=8</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 251 | 349 | +39% | | 136 | 247 | +81% | 365 | +168% |
| evm/calls+logs | 125 | 183 | +46% | | 147 | 250 | +70% | 225 | +53% |
| evm/traces+diffs | 28 | 30 | +8% | | 40 | 59 | +48% | 63 | +60% |
| evm/all_blocks | 6580 | 28908 | +339% | | 7549 | 9550 | +26% | 40077 | +431% |
| sol/whirlpool | 164 | 302 | +84% | | 161 | 345 | +114% | 442 | +174% |
| sol/hard | 95 | 171 | +80% | | 122 | 276 | +126% | 280 | +130% |
| sol/instr+logs | 91 | 119 | +31% | | 72 | 118 | +64% | 135 | +88% |
| sol/instr+bal | 499 | 1071 | +115% | | 284 | 612 | +116% | 2078 | +632% |
| sol/all_blocks | 10380 | 85751 | +726% | | 9123 | 46062 | +405% | 107946 | +1083% |

</details>

<details>
<summary>CPU=12</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 270 | 377 | +40% | | 141 | 266 | +89% | 378 | +168% |
| evm/calls+logs | 129 | 186 | +44% | | 155 | 271 | +75% | 231 | +49% |
| evm/traces+diffs | 27 | 29 | +8% | | 39 | 64 | +64% | 66 | +69% |
| evm/all_blocks | 8526 | 32180 | +277% | | 9193 | 11478 | +25% | 42847 | +366% |
| sol/whirlpool | 170 | 312 | +84% | | 169 | 397 | +135% | 458 | +171% |
| sol/hard | 95 | 177 | +86% | | 128 | 320 | +150% | 287 | +124% |
| sol/instr+logs | 95 | 119 | +25% | | 73 | 142 | +95% | 141 | +93% |
| sol/instr+bal | 521 | 1138 | +118% | | 306 | 687 | +125% | 2262 | +639% |
| sol/all_blocks | 13190 | 92685 | +603% | | 10972 | 51070 | +365% | 114278 | +941% |

</details>

---

## Servarica VPS: AMD EPYC 7551 (4 vCPU), 11GB RAM, virtual disk

### Summary (throughput rps, CPU=4)

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 69 | 83 | +20% | | 46 | 74 | +61% | 95 | +107% |
| evm/calls+logs | 33 | 40 | +21% | | 42 | 63 | +50% | 45 | +7% |
| evm/traces+diffs | 7 | 7 | +0% | | 12 | 17 | +42% | 15 | +25% |
| evm/all_blocks | 1145 | 6277 | +448% | | 1112 | 1316 | +18% | 5856 | +427% |
| sol/whirlpool | 40 | 75 | +88% | | 48 | 78 | +63% | 76 | +58% |
| sol/hard | 23 | 36 | +57% | | 37 | 55 | +49% | 39 | +5% |
| sol/instr+logs | 24 | 28 | +17% | | 29 | 33 | +14% | 30 | +3% |
| sol/instr+bal | 99 | 212 | +114% | | 95 | 175 | +84% | 218 | +129% |
| sol/all_blocks | 1496 | 13503 | +803% | | 1284 | 10580 | +724% | 8972 | +599% |

**Memory wins 9/9 queries vs Legacy** on this slow 4-vCPU VPS.

<details>
<summary>Latency + CPU=1 + CPU=8</summary>

### Latency (single-threaded, median of 20 runs)

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 49.38ms | 37.79ms | +24% | | 53.02ms | 52.70ms | +1% | 31.68ms | +40% |
| evm/calls+logs | 80.79ms | 68.92ms | +15% | | 67.34ms | 58.30ms | +13% | 50.67ms | +25% |
| evm/traces+diffs | 200.88ms | 235.11ms | -17% | | 155.66ms | 217.83ms | -40% | 160.16ms | -3% |
| evm/all_blocks | 3.84ms | 584µs | +85% | | 3.14ms | 1.10ms | +65% | 588µs | +81% |
| sol/whirlpool | 35.29ms | 18.82ms | +47% | | 36.86ms | 55.18ms | -50% | 20.81ms | +44% |
| sol/hard | 59.88ms | 43.80ms | +27% | | 45.76ms | 59.19ms | -29% | 32.05ms | +30% |
| sol/instr+logs | 88.75ms | 84.73ms | +5% | | 76.30ms | 102.50ms | -34% | 80.04ms | -5% |
| sol/instr+bal | 20.18ms | 11.52ms | +43% | | 25.68ms | 23.18ms | +10% | 11.81ms | +54% |
| sol/all_blocks | 3.97ms | 547µs | +86% | | 4.96ms | 384µs | +92% | 399µs | +92% |

### CPU=1

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 22 | 27 | +23% | | 19 | 19 | +0% | 31 | +63% |
| evm/calls+logs | 12 | 15 | +25% | | 16 | 17 | +6% | 19 | +19% |
| evm/traces+diffs | 5 | 5 | +0% | | 7 | 5 | -29% | 6 | -14% |
| evm/all_blocks | 188 | 1580 | +740% | | 221 | 839 | +280% | 1501 | +579% |
| sol/whirlpool | 27 | 43 | +59% | | 25 | 21 | -16% | 51 | +104% |
| sol/hard | 20 | 23 | +15% | | 22 | 18 | -18% | 28 | +27% |
| sol/instr+logs | 12 | 13 | +8% | | 14 | 10 | -29% | 14 | +0% |
| sol/instr+bal | 52 | 90 | +73% | | 36 | 44 | +22% | 80 | +122% |
| sol/all_blocks | 302 | 3674 | +1117% | | 266 | 4714 | +1672% | 2355 | +786% |

### CPU=8

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 81 | 99 | +22% | | 50 | 71 | +42% | 100 | +100% |
| evm/calls+logs | 33 | 40 | +21% | | 47 | 64 | +36% | 48 | +2% |
| evm/traces+diffs | 7 | 7 | +0% | | 14 | 17 | +21% | 15 | +7% |
| evm/all_blocks | 1379 | 6078 | +341% | | 1358 | 1416 | +4% | 5952 | +338% |
| sol/whirlpool | 40 | 80 | +100% | | 48 | 94 | +96% | 79 | +65% |
| sol/hard | 23 | 36 | +57% | | 35 | 70 | +100% | 37 | +6% |
| sol/instr+logs | 24 | 30 | +25% | | 30 | 42 | +40% | 32 | +7% |
| sol/instr+bal | 104 | 212 | +104% | | 97 | 171 | +76% | 233 | +140% |
| sol/all_blocks | 2080 | 13535 | +551% | | 1723 | 11076 | +543% | 9178 | +433% |

</details>

---

## GCP Cloud VM: c2-standard-30, 30 vCPU, 120 GB RAM, pd-ssd

### Summary (throughput rps, CPU=30)

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|--:|--:|---|---|--:|---|
| evm/usdc_transfers | 527 | 738 | +40% | | 200 | 528 | +164% | 738 | +269% |
| evm/calls+logs | 207 | 327 | +58% | | 230 | 483 | +110% | 354 | +54% |
| evm/traces+diffs | 45 | 58 | +29% | | 54 | 129 | +139% | 115 | +113% |
| evm/all_blocks | 10450 | 66643 | +538% | | 10212 | 9106 | -11% | 64684 | +533% |
| sol/whirlpool | 265 | 603 | +127% | | 211 | 770 | +265% | 608 | +188% |
| sol/hard | 142 | 302 | +113% | | 145 | 550 | +279% | 340 | +134% |
| sol/instr+logs | 141 | 232 | +65% | | 104 | 269 | +159% | 254 | +144% |
| sol/instr+bal | 756 | 2000 | +165% | | 373 | 1310 | +251% | 2191 | +487% |
| sol/all_blocks | 21487 | 195608 | +810% | | 17079 | 40679 | +138% | 135656 | +694% |

### Latency (single-threaded, median of 20 runs)

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|--:|--:|---|---|--:|---|
| evm/usdc_transfers | 15.08ms | 13.62ms | +10% | | 21.64ms | 28.57ms | -32% | 12.70ms | +41% |
| evm/calls+logs | 24.06ms | 23.20ms | +4% | | 21.00ms | 40.01ms | -91% | 19.27ms | +8% |
| evm/traces+diffs | 64.50ms | 81.77ms | -27% | | 65.28ms | 160.81ms | -146% | 70.48ms | -8% |
| evm/all_blocks | 939µs | 232µs | +75% | | 795µs | 1.02ms | -28% | 238µs | +70% |
| sol/whirlpool | 11.21ms | 7.79ms | +31% | | 12.37ms | 21.12ms | -71% | 6.07ms | +51% |
| sol/hard | 14.80ms | 11.79ms | +20% | | 14.61ms | 22.62ms | -55% | 9.90ms | +32% |
| sol/instr+logs | 31.18ms | 28.96ms | +7% | | 30.00ms | 47.97ms | -60% | 27.70ms | +8% |
| sol/instr+bal | 4.81ms | 2.40ms | +50% | | 9.64ms | 12.85ms | -33% | 2.71ms | +72% |
| sol/all_blocks | 701µs | 87µs | +88% | | 679µs | 106µs | +84% | 125µs | +82% |

### Throughput scaling (rps)

<details>
<summary>CPU=1</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|--:|--:|---|---|--:|---|
| evm/usdc_transfers | 65 | 76 | +17% | | 47 | 34 | -28% | 77 | +64% |
| evm/calls+logs | 42 | 43 | +2% | | 45 | 25 | -44% | 50 | +11% |
| evm/traces+diffs | 15 | 12 | -20% | | 16 | 6 | -62% | 14 | -12% |
| evm/all_blocks | 1144 | 4276 | +274% | | 1219 | 985 | -19% | 4188 | +244% |
| sol/whirlpool | 95 | 137 | +44% | | 80 | 48 | -40% | 175 | +119% |
| sol/hard | 69 | 85 | +23% | | 68 | 45 | -34% | 104 | +53% |
| sol/instr+logs | 33 | 34 | +3% | | 33 | 21 | -36% | 35 | +6% |
| sol/instr+bal | 214 | 469 | +119% | | 103 | 79 | -23% | 373 | +262% |
| sol/all_blocks | 1494 | 12014 | +704% | | 1459 | 9629 | +560% | 8314 | +470% |

</details>

<details>
<summary>CPU=4</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|--:|--:|---|---|--:|---|
| evm/usdc_transfers | 200 | 236 | +18% | | 118 | 130 | +10% | 246 | +108% |
| evm/calls+logs | 113 | 138 | +22% | | 126 | 94 | -25% | 160 | +27% |
| evm/traces+diffs | 37 | 35 | -5% | | 38 | 25 | -34% | 49 | +29% |
| evm/all_blocks | 2877 | 17107 | +495% | | 2751 | 3068 | +12% | 16747 | +509% |
| sol/whirlpool | 181 | 377 | +108% | | 153 | 186 | +22% | 446 | +192% |
| sol/hard | 118 | 216 | +83% | | 122 | 160 | +31% | 251 | +106% |
| sol/instr+logs | 86 | 107 | +24% | | 71 | 69 | -3% | 119 | +68% |
| sol/instr+bal | 431 | 1230 | +185% | | 219 | 309 | +41% | 1052 | +380% |
| sol/all_blocks | 3388 | 47165 | +1292% | | 3524 | 22371 | +535% | 32525 | +823% |

</details>

<details>
<summary>CPU=8</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|--:|--:|---|---|--:|---|
| evm/usdc_transfers | 317 | 424 | +34% | | 153 | 251 | +64% | 430 | +181% |
| evm/calls+logs | 160 | 211 | +32% | | 178 | 183 | +3% | 249 | +40% |
| evm/traces+diffs | 43 | 49 | +14% | | 48 | 48 | +0% | 80 | +67% |
| evm/all_blocks | 4625 | 34396 | +644% | | 5045 | 4732 | -6% | 33368 | +561% |
| sol/whirlpool | 216 | 513 | +138% | | 192 | 366 | +91% | 552 | +188% |
| sol/hard | 133 | 274 | +106% | | 144 | 275 | +91% | 316 | +119% |
| sol/instr+logs | 111 | 166 | +50% | | 92 | 124 | +35% | 190 | +107% |
| sol/instr+bal | 615 | 1656 | +169% | | 305 | 614 | +101% | 1599 | +424% |
| sol/all_blocks | 7137 | 94091 | +1218% | | 7256 | 25287 | +248% | 65334 | +800% |

</details>

<details>
<summary>CPU=16</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|--:|--:|---|---|--:|---|
| evm/usdc_transfers | 457 | 615 | +35% | | 181 | 464 | +156% | 652 | +260% |
| evm/calls+logs | 199 | 303 | +52% | | 221 | 336 | +52% | 338 | +53% |
| evm/traces+diffs | 46 | 59 | +28% | | 52 | 88 | +69% | 108 | +108% |
| evm/all_blocks | 8814 | 64689 | +634% | | 8685 | 5292 | -39% | 63093 | +626% |
| sol/whirlpool | 259 | 585 | +126% | | 207 | 571 | +176% | 606 | +193% |
| sol/hard | 139 | 304 | +119% | | 147 | 446 | +203% | 342 | +133% |
| sol/instr+logs | 132 | 225 | +70% | | 100 | 195 | +95% | 246 | +146% |
| sol/instr+bal | 738 | 1939 | +163% | | 355 | 1143 | +222% | 2061 | +481% |
| sol/all_blocks | 14518 | 176032 | +1113% | | 13626 | 25638 | +88% | 122283 | +797% |

</details>

<details>
<summary>CPU=24</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|--:|--:|---|---|--:|---|
| evm/usdc_transfers | 518 | 755 | +46% | | 191 | 509 | +166% | 753 | +294% |
| evm/calls+logs | 209 | 329 | +57% | | 230 | 375 | +63% | 359 | +56% |
| evm/traces+diffs | 45 | 59 | +31% | | 51 | 95 | +86% | 116 | +127% |
| evm/all_blocks | 10806 | 67409 | +524% | | 10648 | 5965 | -44% | 65647 | +517% |
| sol/whirlpool | 267 | 603 | +126% | | 210 | 700 | +233% | 618 | +194% |
| sol/hard | 137 | 310 | +126% | | 152 | 505 | +232% | 343 | +126% |
| sol/instr+logs | 133 | 237 | +78% | | 102 | 236 | +131% | 261 | +156% |
| sol/instr+bal | 761 | 2024 | +166% | | 363 | 1260 | +247% | 2192 | +504% |
| sol/all_blocks | 19919 | 191834 | +863% | | 16757 | 26824 | +60% | 132360 | +690% |

</details>

<details>
<summary>CPU=30</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|--:|--:|---|---|--:|---|
| evm/usdc_transfers | 527 | 738 | +40% | | 200 | 528 | +164% | 738 | +269% |
| evm/calls+logs | 207 | 327 | +58% | | 230 | 483 | +110% | 354 | +54% |
| evm/traces+diffs | 45 | 58 | +29% | | 54 | 129 | +139% | 115 | +113% |
| evm/all_blocks | 10450 | 66643 | +538% | | 10212 | 9106 | -11% | 64684 | +533% |
| sol/whirlpool | 265 | 603 | +127% | | 211 | 770 | +265% | 608 | +188% |
| sol/hard | 142 | 302 | +113% | | 145 | 550 | +279% | 340 | +134% |
| sol/instr+logs | 141 | 232 | +65% | | 104 | 269 | +159% | 254 | +144% |
| sol/instr+bal | 756 | 2000 | +165% | | 373 | 1310 | +251% | 2191 | +487% |
| sol/all_blocks | 21487 | 195608 | +810% | | 17079 | 40679 | +138% | 135656 | +694% |

</details>

### Analysis

- **Legacy** plateaus at CPU=8-16 (RocksDB contention)
- **New+Memory** scales linearly with CPU on join-heavy queries (no I/O, lock-free)
- **New+Spillover** scales like parquet with better row group pruning (8K RGs)

---

# Query Engine: Parquet-on-Parquet

New query engine vs legacy query engine. Both on the same mmap'd parquet data (no storage layer difference).

## x86_64: Intel Xeon E-2136 (6C/12T @ 3.3GHz), 64GB DDR4, Linux

### Latency (single-threaded, median)

| Benchmark                  | New           | Legacy    | Diff             |
|----------------------------|---------------|-----------|------------------|
| evm/usdc_transfers         | **10.47 ms**  | 14.57 ms  | **1.39x faster** |
| evm/contract_calls+logs    | **18.09 ms**  | 22.26 ms  | **1.23x faster** |
| evm/usdc_traces+statediffs | 71.64 ms      | 65.16 ms  | 1.10x slower     |
| evm/all_blocks             | **0.18 ms**   | 1.11 ms   | **6.24x faster** |
| sol/whirlpool_swap         | **6.51 ms**   | 10.25 ms  | **1.57x faster** |
| sol/hard (Meteora DLMM)    | **10.34 ms**  | 15.00 ms  | **1.45x faster** |
| sol/instr+logs             | **24.18 ms**  | 28.15 ms  | **1.16x faster** |
| sol/instr+balances         | **1.92 ms**   | 4.48 ms   | **2.33x faster** |
| sol/all_blocks             | **0.07 ms**   | 0.79 ms   | **12.2x faster** |

### Summary

| Median           | CPU=1           | CPU=4           | CPU=8           | CPU=12          |
|------------------|-----------------|-----------------|-----------------|-----------------|
| General queries  | **28% faster**  | **37% faster**  | **46% faster**  | **44% faster**  |
| Only full blocks | **625% faster** | **695% faster** | **533% faster** | **440% faster** |

<details>
<summary>Full throughput table (requests/sec, 5s per level)</summary>

| Benchmark                  | CPU | New        | Legacy   | Diff            |
|----------------------------|-----|------------|----------|-----------------|
| evm/usdc_transfers         | 1   | **91**     | 67       | **36% faster**  |
|                            | 4   | **242**    | 189      | **28% faster**  |
|                            | 8   | **349**    | 251      | **39% faster**  |
|                            | 12  | **377**    | 270      | **40% faster**  |
| evm/contract_calls+logs    | 1   | **50**     | 45       | **11% faster**  |
|                            | 4   | **142**    | 104      | **37% faster**  |
|                            | 8   | **183**    | 125      | **46% faster**  |
|                            | 12  | **186**    | 129      | **44% faster**  |
| evm/usdc_traces+statediffs | 1   | 13         | **15**   | 17% slower      |
|                            | 4   | **27**     | 27       | **2% faster**   |
|                            | 8   | **30**     | 28       | **8% faster**   |
|                            | 12  | **29**     | 27       | **8% faster**   |
| evm/all_blocks             | 1   | **5215**   | 1143     | **356% faster** |
|                            | 4   | **20892**  | 3697     | **465% faster** |
|                            | 8   | **28908**  | 6580     | **339% faster** |
|                            | 12  | **32180**  | 8526     | **277% faster** |
| sol/whirlpool_swap         | 1   | **147**    | 104      | **41% faster**  |
|                            | 4   | **282**    | 151      | **87% faster**  |
|                            | 8   | **302**    | 164      | **84% faster**  |
|                            | 12  | **312**    | 170      | **84% faster**  |
| sol/hard (Meteora DLMM)    | 1   | **87**     | 68       | **28% faster**  |
|                            | 4   | **160**    | 92       | **74% faster**  |
|                            | 8   | **171**    | 95       | **80% faster**  |
|                            | 12  | **177**    | 95       | **86% faster**  |
| sol/instr+logs             | 1   | **40**     | 36       | **11% faster**  |
|                            | 4   | **96**     | 79       | **23% faster**  |
|                            | 8   | **119**    | 91       | **31% faster**  |
|                            | 12  | **119**    | 95       | **25% faster**  |
| sol/instr+balances         | 1   | **494**    | 241      | **105% faster** |
|                            | 4   | **944**    | 444      | **113% faster** |
|                            | 8   | **1071**   | 499      | **115% faster** |
|                            | 12  | **1138**   | 521      | **118% faster** |
| sol/all_blocks             | 1   | **14869**  | 1496     | **893% faster** |
|                            | 4   | **58424**  | 5701     | **924% faster** |
|                            | 8   | **85751**  | 10380    | **726% faster** |
|                            | 12  | **92685**  | 13190    | **603% faster** |

</details>

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

### Summary

| Median           | CPU=1           | CPU=4           | CPU=8           | CPU=12          |
|------------------|-----------------|-----------------|-----------------|-----------------|
| General queries  | **7% faster**   | **40% faster**  | **53% faster**  | **49% faster**  |
| Only full blocks | **337% faster** | **139% faster** | **99% faster**  | **64% faster**  |

<details>
<summary>Full throughput table (requests/sec, 5s per level)</summary>

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

</details>

---

## How to run

```bash
# Parquet latency benchmarks (divan, 20x100)
cargo bench --bench latency

# Parquet throughput benchmarks (default CPU=8, --all for full sweep)
cargo bench --bench throughput -- --all

# Storage tier benchmark (memory, spillover, legacy comparison)
cargo bench --bench hot_bench --features "legacy-query"

# Single query profiling with timing breakdown
cargo bench --bench profile -- "evm/usdc_transfers" 1 --profile
```
