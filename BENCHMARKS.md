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
| evm/usdc_transfers | 270 | 377 | +40% | | 138 | 262 | +90% | 381 | +176% |
| evm/calls+logs | 130 | 187 | +44% | | 157 | 271 | +73% | 231 | +47% |
| evm/traces+diffs | 27 | 30 | +11% | | 40 | 65 | +63% | 67 | +68% |
| evm/all_blocks | 8444 | 32422 | +284% | | 8380 | 11369 | +36% | 42289 | +404% |
| sol/whirlpool | 165 | 313 | +90% | | 173 | 399 | +131% | 456 | +164% |
| sol/hard | 95 | 176 | +85% | | 128 | 322 | +152% | 286 | +123% |
| sol/instr+logs | 93 | 124 | +33% | | 73 | 145 | +99% | 141 | +93% |
| sol/instr+bal | 513 | 1120 | +118% | | 298 | 687 | +130% | 2162 | +626% |
| sol/all_blocks | 13271 | 91567 | +590% | | 13944 | 51842 | +272% | 113383 | +713% |

### Latency (single-threaded, median of 20 runs)

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 14.80ms | 11.11ms | +25% | | 20.08ms | 21.76ms | -8% | 9.68ms | +52% |
| evm/calls+logs | 22.43ms | 18.45ms | +18% | | 18.86ms | 21.04ms | -12% | 15.00ms | +20% |
| evm/traces+diffs | 67.61ms | 79.78ms | -18% | | 58.26ms | 94.47ms | -62% | 56.14ms | +4% |
| evm/all_blocks | 1.04ms | 225µs | +78% | | 896µs | 466µs | +48% | 143µs | +84% |
| sol/whirlpool | 10.67ms | 6.67ms | +37% | | 11.54ms | 15.61ms | -35% | 3.73ms | +68% |
| sol/hard | 15.41ms | 11.15ms | +28% | | 13.49ms | 16.73ms | -24% | 6.17ms | +54% |
| sol/instr+logs | 28.28ms | 24.05ms | +15% | | 29.14ms | 36.98ms | -27% | 21.72ms | +25% |
| sol/instr+bal | 4.75ms | 1.98ms | +58% | | 8.59ms | 9.22ms | -7% | 1.58ms | +82% |
| sol/all_blocks | 790µs | 75µs | +91% | | 658µs | 70µs | +89% | 56µs | +91% |

### Throughput scaling (rps)

<details>
<summary>CPU=1</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 66 | 92 | +39% | | 50 | 44 | -12% | 103 | +106% |
| evm/calls+logs | 45 | 54 | +20% | | 49 | 47 | -4% | 64 | +31% |
| evm/traces+diffs | 14 | 12 | -14% | | 16 | 11 | -31% | 18 | +13% |
| evm/all_blocks | 1131 | 5358 | +374% | | 1248 | 2219 | +78% | 7353 | +489% |
| sol/whirlpool | 105 | 148 | +41% | | 87 | 64 | -26% | 265 | +205% |
| sol/hard | 68 | 87 | +28% | | 70 | 60 | -14% | 164 | +134% |
| sol/instr+logs | 35 | 41 | +17% | | 34 | 26 | -24% | 46 | +35% |
| sol/instr+bal | 232 | 480 | +107% | | 121 | 107 | -12% | 628 | +419% |
| sol/all_blocks | 1473 | 14488 | +884% | | 1611 | 13961 | +767% | 18068 | +1022% |

</details>

<details>
<summary>CPU=4</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 189 | 245 | +30% | | 112 | 166 | +48% | 283 | +153% |
| evm/calls+logs | 104 | 140 | +35% | | 118 | 172 | +46% | 179 | +52% |
| evm/traces+diffs | 26 | 27 | +4% | | 34 | 39 | +15% | 50 | +47% |
| evm/all_blocks | 3697 | 20661 | +459% | | 4137 | 6681 | +62% | 28038 | +578% |
| sol/whirlpool | 151 | 279 | +85% | | 147 | 224 | +52% | 432 | +194% |
| sol/hard | 92 | 160 | +74% | | 117 | 175 | +50% | 272 | +132% |
| sol/instr+logs | 78 | 96 | +23% | | 64 | 81 | +27% | 116 | +81% |
| sol/instr+bal | 440 | 942 | +114% | | 230 | 415 | +80% | 1653 | +619% |
| sol/all_blocks | 5598 | 56879 | +916% | | 6444 | 35673 | +453% | 72218 | +1020% |

</details>

<details>
<summary>CPU=8</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 252 | 349 | +39% | | 135 | 250 | +85% | 368 | +173% |
| evm/calls+logs | 121 | 185 | +53% | | 149 | 252 | +69% | 229 | +54% |
| evm/traces+diffs | 28 | 30 | +7% | | 40 | 59 | +48% | 65 | +63% |
| evm/all_blocks | 6806 | 30460 | +348% | | 6892 | 9843 | +43% | 41462 | +502% |
| sol/whirlpool | 165 | 310 | +88% | | 163 | 350 | +115% | 452 | +177% |
| sol/hard | 95 | 174 | +83% | | 128 | 280 | +119% | 284 | +122% |
| sol/instr+logs | 92 | 119 | +29% | | 73 | 120 | +64% | 138 | +89% |
| sol/instr+bal | 504 | 1085 | +115% | | 283 | 631 | +123% | 2129 | +652% |
| sol/all_blocks | 10352 | 81506 | +687% | | 11260 | 46337 | +312% | 100874 | +796% |

</details>

<details>
<summary>CPU=12</summary>

| Query | Leg+Pq | New+Pq | diff | | Leg+Rdb | New+Mem | diff | New+Spill | diff |
|-------|--:|--:|---|---|--:|--:|---|--:|---|
| evm/usdc_transfers | 270 | 377 | +40% | | 138 | 262 | +90% | 381 | +176% |
| evm/calls+logs | 130 | 187 | +44% | | 157 | 271 | +73% | 231 | +47% |
| evm/traces+diffs | 27 | 30 | +11% | | 40 | 65 | +63% | 67 | +68% |
| evm/all_blocks | 8444 | 32422 | +284% | | 8380 | 11369 | +36% | 42289 | +404% |
| sol/whirlpool | 165 | 313 | +90% | | 173 | 399 | +131% | 456 | +164% |
| sol/hard | 95 | 176 | +85% | | 128 | 322 | +152% | 286 | +123% |
| sol/instr+logs | 93 | 124 | +33% | | 73 | 145 | +99% | 141 | +93% |
| sol/instr+bal | 513 | 1120 | +118% | | 298 | 687 | +130% | 2162 | +626% |
| sol/all_blocks | 13271 | 91567 | +590% | | 13944 | 51842 | +272% | 113383 | +713% |

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
| evm/usdc_transfers         | **11.18 ms**  | 14.57 ms  | **1.30x faster** |
| evm/contract_calls+logs    | **18.37 ms**  | 22.26 ms  | **1.21x faster** |
| evm/usdc_traces+statediffs | 71.76 ms      | 65.16 ms  | 1.10x slower     |
| evm/all_blocks             | **0.18 ms**   | 1.11 ms   | **6.10x faster** |
| sol/whirlpool_swap         | **6.61 ms**   | 10.25 ms  | **1.55x faster** |
| sol/hard (Meteora DLMM)    | **10.34 ms**  | 15.00 ms  | **1.45x faster** |
| sol/instr+logs             | **24.65 ms**  | 28.15 ms  | **1.14x faster** |
| sol/instr+balances         | **1.97 ms**   | 4.48 ms   | **2.27x faster** |
| sol/all_blocks             | **0.07 ms**   | 0.79 ms   | **12.2x faster** |

### Summary

| Median           | CPU=1           | CPU=4           | CPU=8           | CPU=12          |
|------------------|-----------------|-----------------|-----------------|-----------------|
| General queries  | **41% faster**  | **51% faster**  | **69% faster**  | **79% faster**  |
| Only full blocks | **681% faster** | **751% faster** | **617% faster** | **505% faster** |

<details>
<summary>Full throughput table (requests/sec, 5s per level)</summary>

| Benchmark                  | CPU | New        | Legacy   | Diff            |
|----------------------------|-----|------------|----------|-----------------|
| evm/usdc_transfers         | 1   | **103**    | 67       | **54% faster**  |
|                            | 4   | **285**    | 189      | **51% faster**  |
|                            | 8   | **425**    | 251      | **69% faster**  |
|                            | 12  | **482**    | 270      | **79% faster**  |
| evm/contract_calls+logs    | 1   | **58**     | 45       | **29% faster**  |
|                            | 4   | **153**    | 104      | **47% faster**  |
|                            | 8   | **205**    | 125      | **64% faster**  |
|                            | 12  | **214**    | 129      | **66% faster**  |
| evm/usdc_traces+statediffs | 1   | 14         | **15**   | 7% slower       |
|                            | 4   | **30**     | 27       | **11% faster**  |
|                            | 8   | **34**     | 28       | **21% faster**  |
|                            | 12  | **35**     | 27       | **30% faster**  |
| evm/all_blocks             | 1   | **5616**   | 1143     | **391% faster** |
|                            | 4   | **22329**  | 3697     | **504% faster** |
|                            | 8   | **33851**  | 6580     | **415% faster** |
|                            | 12  | **36311**  | 8526     | **326% faster** |
| sol/whirlpool_swap         | 1   | **151**    | 104      | **45% faster**  |
|                            | 4   | **281**    | 151      | **86% faster**  |
|                            | 8   | **316**    | 164      | **93% faster**  |
|                            | 12  | **322**    | 170      | **89% faster**  |
| sol/hard (Meteora DLMM)    | 1   | **96**     | 68       | **41% faster**  |
|                            | 4   | **166**    | 92       | **80% faster**  |
|                            | 8   | **183**    | 95       | **93% faster**  |
|                            | 12  | **180**    | 95       | **89% faster**  |
| sol/instr+logs             | 1   | **42**     | 36       | **17% faster**  |
|                            | 4   | **103**    | 79       | **30% faster**  |
|                            | 8   | **130**    | 91       | **43% faster**  |
|                            | 12  | **134**    | 95       | **41% faster**  |
| sol/instr+balances         | 1   | **519**    | 241      | **115% faster** |
|                            | 4   | **970**    | 444      | **118% faster** |
|                            | 8   | **1145**   | 499      | **129% faster** |
|                            | 12  | **1196**   | 521      | **130% faster** |
| sol/all_blocks             | 1   | **16013**  | 1496     | **970% faster** |
|                            | 4   | **62532**  | 5701     | **997% faster** |
|                            | 8   | **95423**  | 10380    | **819% faster** |
|                            | 12  | **103252** | 13190    | **683% faster** |

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
