# Changelog

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

### Substrate & Naming
- Fixed moonbeam_example_giant_squid_stats weight mismatch: added `weight: 128` to extrinsics.signature in substrate.yaml (matches legacy `set_weight("signature", 4*32)`)
- Renamed `include_equal` → `inclusive` with detailed English comments explaining cross-table vs self-join semantics

### Hyperliquid Support
- Added `metadata/hyperliquid_fills.yaml` (blocks + fills, sorted by user/coin)
- Added `metadata/hyperliquid_replica_cmds.yaml` (blocks + actions with weight columns, query aliases for order/cancel/cancelByCloid/batchModify actions)
- `ListContainsAnyPredicate`: new predicate type for List\<UInt32\>/List\<String\> columns
- Scalar string equality filter support (`status: "ok"` → `col_eq(key, Utf8)`)
- `QueryAlias` mechanism: implicit predicates + filter aliases map query names to underlying tables
- 7 new e2e fixture tests (3 fills + 4 replica_cmds)

### Benchmark Data Update
- Switched benchmark data to fresh R2 production chunks (ETH: 224 blocks, SOL: 48 blocks)
- Updated benchmark queries: USDT for tx+logs, USDC for traces+statediffs, Jupiter for sol/instr+logs
- Added sol/hard benchmark (Meteora DLMM, matches legacy solana_hard query)
- New engine wins all throughput benchmarks at CPU>=4 (1.07x–2.58x faster than legacy)

### RPC-Compatible Benchmarks
- Added RPC-method-equivalent queries to `benches/queries.rs`: `getBlockByNumber` (full tx objects + `:txHashes` for `fullTransactions=false`), `getBlockReceipts`, `trace_block` (all traces in a block, full action+result fields), and `getLogs` over 1/10/100-block windows
- Removed the synthetic `all_blocks` full-scan benchmarks (EVM + Solana)
- Inline legacy A/B: all benches accept `--features legacy-query` to run the same query JSON on the same chunk through the legacy `sqd-query` engine (latency `*_legacy` groups; throughput New/Legacy/ratio columns; `profile --legacy`)
- `profile --compare` verifies the new engine's output is byte-identical to legacy — confirmed for all RPC + indexer + Solana queries
- `memory` bench (counting allocator over jemalloc): measures alloc/query (churn) and peak heap under concurrency, new vs legacy. New allocates less per query everywhere; peak working set is 2–4× lower on RPC/light queries but 1.4–1.65× higher on heavy multi-table queries (intra-query parallelism trade-off)
- `autobenches = false`: `benches/{queries,legacy}.rs` are `#[path]`-included shared modules, not standalone bench targets
- Finding: tables are sorted by filter columns, not `block_number`, so single-block RPC calls scan every row group; the new engine still beats legacy on all three (getBlockReceipts 2.8×, getBlockByNumber 1.9×, getLogs 1.1× faster on single-thread latency)

### Response-Budget Scan Optimizations
- Single-table two-phase scan: full-table scans capped by the 20 MB response budget now do a cheap narrow pre-scan (block number + `*_size` weight columns) to find the block cutoff, then decode wide columns only up to it (all_statediffs big chunk: alloc 2438→465 MB, peak 5405→518 MB)
- Multi-table budget early-stop: block-sorted tables (`sort_key` leads with `block_number`, e.g. `statediffs`) are scanned in block order in parallel waves that stop once cumulative response weight crosses the budget; wide columns decode only for emittable blocks. Fixes the `usdc_traces+diffs` weak spot (big chunk: latency 0.81×→0.99× vs legacy, alloc 2488→1534 MB, peak 672→504 MB), byte-identical output
- Fixed EVM `statediffs` metadata `sort_key` to reflect the actual block-sorted physical layout (was incorrectly listed as address-sorted; `sort_key` is documentation/validation only, not used at query time)
