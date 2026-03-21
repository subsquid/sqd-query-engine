# Integration: Replace sqd_storage with DatasetStore in hotblocks server

## Context

Replace RocksDB-based `sqd_storage::Database` with our file-based `DatasetStore` (memory + parquet)
inside the existing `sqd-hotblocks` server. Remove RocksDB dependency entirely.

The storage layer (DatasetStore, MemoryChunkReader, CompositeChunkReader, CrashLogWriter,
ParquetWriter) is complete and benchmarked. This plan covers only the integration wiring
and the two missing safety guarantees.

## What's ready

| Component | Status | Location |
|-----------|--------|----------|
| Memory buffer + push/truncate/drain | Done | `sqd-query-engine/src/scan/memory_backend.rs` |
| Composite reader (multi-tier query) | Done | `sqd-query-engine/src/scan/composite_reader.rs` |
| IPC crash log (durability) | Done | `sqd-query-engine/src/scan/crash_log.rs` |
| Parquet flush (sorted, 8K RGs) | Done | `sqd-query-engine/src/scan/parquet_writer.rs` |
| DatasetStore (orchestration) | Done | `sqd-query-engine/src/scan/dataset_store.rs` |
| Query engine (execute_chunk) | Done | `sqd-query-engine/src/output/assembly.rs` |
| 257 tests (unit + e2e + fuzz + redteam) | Done | |

## What needs to be added (2 safety items)

### 1. Snapshot isolation for queries

**Problem**: `CompositeChunkReader` reads current memory state. Concurrent `push_block()` during
query could cause inconsistent reads.

**Solution**: Snapshot the block list at query start.

```rust
// In MemoryChunkReader:
pub fn snapshot(&self) -> MemorySnapshot {
    MemorySnapshot {
        blocks: self.blocks.clone(), // Vec<Arc<BlockData>> — cheap Arc clones
        schemas: self.schemas.clone(),
    }
}

// MemorySnapshot implements ChunkReader (read-only, frozen)
// DatasetStore.reader() returns CompositeChunkReader using snapshot
```

Change `BlockData` storage from `Vec<BlockData>` to `Vec<Arc<BlockData>>` so cloning
is O(n) Arc increments, not O(n×data) copies.

**Files**: `memory_backend.rs`, `dataset_store.rs`

### 2. Persist finalized_head

**Problem**: finalized_head lost on crash. Data source re-sends it, but clients
querying `/finalized-head` get stale data until re-received.

**Solution**: Write `finalized_head.json` with fsync on each finalization update.

```rust
// In DatasetStore:
pub fn set_finalized_head(&mut self, head: BlockRef) -> Result<()> {
    self.finalized_head = Some(head.clone());
    let path = self.dir.join("finalized_head.json");
    let tmp = self.dir.join("finalized_head.json.tmp");
    std::fs::write(&tmp, serde_json::to_vec(&head)?)?;
    std::fs::rename(&tmp, &path)?; // atomic on same filesystem
    Ok(())
}
```

Atomic via write-to-tmp + rename. Recoverable on startup.

**Files**: `dataset_store.rs`

## Integration plan (modify legacy hotblocks server)

### Phase 1: Add sqd-query-engine as dependency

**File**: `/Users/mo4islona/Projects/subsquid/data/crates/hotblocks/Cargo.toml`

```toml
[dependencies]
sqd-query-engine = { path = "../../sqd-query-engine" }
```

Remove `sqd-storage` dependency.

### Phase 2: Replace DBRef type

**File**: `/Users/mo4islona/Projects/subsquid/data/crates/hotblocks/src/types.rs`

```rust
// Before:
pub type DBRef = Arc<sqd_storage::db::Database>;

// After:
pub type DatasetStoreRef = Arc<RwLock<DatasetStore>>;
// One per dataset, managed by DataService
```

### Phase 3: Replace WriteController

**File**: `/Users/mo4islona/Projects/subsquid/data/crates/hotblocks/src/dataset_controller/write_controller.rs`

Current WriteController methods → DatasetStore equivalents:

| WriteController method | DatasetStore equivalent |
|----------------------|------------------------|
| `new_chunk(chunk)` | `push_block(block)` (called per block, not per chunk) |
| `retain(from_block)` | `evict(up_to)` |
| `compute_rollback()` | `truncate(fork_point)` |
| `finalize(head)` | `set_finalized_head(head)` + `compact(head)` |
| `head()` | `head()` |
| `next_block()` | `head().map(|h| h + 1)` |

**Key change**: legacy buffers blocks into chunks (200K rows), then writes chunk atomically.
Our system pushes blocks individually (instant, to memory). No buffering needed.

This means `IngestGeneric`'s `maybe_flush()` / `MaybeOnHead` flush logic changes:
- No more `DataBuilder` buffering
- Each `DataEvent::Block` → immediately `push_block()`
- `MaybeOnHead` → no-op (blocks already queryable in memory)
- `FinalizedHead` → `compact()` in background

### Phase 4: Replace query pipeline

**File**: `/Users/mo4islona/Projects/subsquid/data/crates/hotblocks/src/query/running.rs`

Current flow:
```
db.snapshot() → StaticSnapshot → ChunkIterator → plan.execute(chunk_reader)
```

New flow:
```
dataset_store.reader() → CompositeChunkReader → execute_chunk(plan, meta, &reader)
```

No more `StaticSnapshot`, `StaticChunkIterator`, `StaticChunkReader` (ouroboros self-referential).
Replace with simple `dataset_store.reader()` which returns a snapshot.

**File**: `/Users/mo4islona/Projects/subsquid/data/crates/hotblocks/src/query/static_snapshot.rs`
→ Delete entirely.

### Phase 5: Replace DataService

**File**: `/Users/mo4islona/Projects/subsquid/data/crates/hotblocks/src/data_service.rs`

Change: instead of `Arc<Database>` shared across all datasets, each dataset gets
its own `DatasetStore`:

```rust
struct DataService {
    datasets: HashMap<DatasetId, DatasetController>,
    // No shared DB anymore
}
```

`DatasetController` creates its own `DatasetStore` at `{data_dir}/{dataset_id}/`.

### Phase 6: Replace CLI / main.rs

**File**: `/Users/mo4islona/Projects/subsquid/data/crates/hotblocks/src/cli.rs`

Remove:
- `--database-dir` (no more RocksDB)
- `--data-cache-size` (no more block cache)
- `--rocksdb-stats` / `--rocksdb-disable-direct-io`

Add:
- `--data-dir` (base directory for all datasets, each gets a subdirectory)
- `--memory-cap` (per-dataset memory cap, default 500MB)

**File**: `/Users/mo4islona/Projects/subsquid/data/crates/hotblocks/src/main.rs`

Remove:
- `DatabaseSettings::default().open()` → no DB opening
- `db_cleanup_task` → no RocksDB cleanup needed
- `rocksdb_stats` / `rocksdb_prop` API endpoints → remove

### Phase 7: Update API endpoints

**File**: `/Users/mo4islona/Projects/subsquid/data/crates/hotblocks/src/api.rs`

Remove:
- `GET /rocksdb/stats`
- `GET /rocksdb/prop/{cf}/{name}`

Keep unchanged:
- `POST /datasets/{id}/stream` (uses new query pipeline)
- `POST /datasets/{id}/finalized-stream`
- `GET /datasets/{id}/head`
- `GET /datasets/{id}/finalized-head`
- `GET /datasets/{id}/status`
- `GET/POST /datasets/{id}/retention`

### Phase 8: Update Dockerfile

**File**: `/Users/mo4islona/Projects/subsquid/data/Dockerfile`

Remove RocksDB build dependencies (librocksdb, clang for jemalloc).
Binary becomes smaller, builds faster.

### Phase 9: Block conversion

**Key question**: how blocks arrive from data source → how they become `BlockData`.

Current: `DataEvent::Block { block }` → `ChunkBuilder` accumulates → `PreparedChunk` →
`WriteController.new_chunk()` writes Arrow tables to RocksDB.

New: `DataEvent::Block { block }` → `ChunkBuilder` converts single block → `BlockData` →
`DatasetStore.push_block()`.

Need to check if `ChunkBuilder` can produce per-block RecordBatches (currently it
accumulates). May need a thin adapter or use `sqd_data_core` directly.

## Atomicity guarantees (comparison)

| Guarantee | RocksDB (before) | DatasetStore (after) | Safe? |
|-----------|-------------------|---------------------|-------|
| Chunk write atomicity | Transaction commit | Memory push (instant) + IPC crash log | ✅ Block re-fetchable |
| Snapshot isolation | `db.snapshot()` | `memory.snapshot()` (Arc clone) | ✅ After Phase 1 fix |
| Fork handling | `insert_fork()` atomic delete+write | `truncate()` + `push_block()` separate | ✅ Data re-fetchable |
| Finalized head durability | Transaction + WAL | `finalized_head.json` atomic rename | ✅ After Phase 2 fix |
| Cleanup safety | Write batch atomicity | `rm -rf chunk_dir/` + mmap lazy unlink | ✅ Linux mmap safe |
| Concurrent write protection | OptimisticTransaction retry | Single writer per dataset | ✅ By design |
| Crash recovery | RocksDB WAL replay | IPC crash log + parquet scan | ✅ At most 1 block lost |

## What we lose (acceptable)

1. **At most 1 block on crash** — IPC may have incomplete last message. Re-fetched from data source.
2. **No page-level stats pruning** — legacy had column page index. We have row group stats (sufficient).
3. **No RocksDB diagnostics** — `/rocksdb/stats` endpoint removed. Replace with dataset size/memory metrics.

## Verification

1. **Unit tests**: run existing 257 tests + new snapshot isolation tests
2. **Integration test**: start server, push blocks via data source mock, query via HTTP, verify results
3. **Reorg test**: push blocks, simulate fork (409 response), verify truncation + re-ingest
4. **Crash test**: push blocks, kill -9, restart, verify recovery from crash log
5. **Retention test**: set Head(100) retention, push 200 blocks, verify old blocks evicted
6. **Benchmark**: run hot_bench comparing legacy hotblocks vs new on same data
7. **Docker**: build image, deploy alongside legacy, compare outputs

## Files to modify in legacy repo

| File | Change |
|------|--------|
| `Cargo.toml` | Add sqd-query-engine, remove sqd-storage |
| `src/types.rs` | Replace DBRef with DatasetStoreRef |
| `src/cli.rs` | Remove RocksDB args, add --data-dir |
| `src/main.rs` | Remove DB init, cleanup task, RocksDB endpoints |
| `src/api.rs` | Remove /rocksdb/*, update query handlers |
| `src/data_service.rs` | One DatasetStore per dataset instead of shared DB |
| `src/dataset_controller/write_controller.rs` | Replace with DatasetStore calls |
| `src/dataset_controller/dataset_controller.rs` | Simplify (no chunk buffering) |
| `src/dataset_controller/ingest_generic.rs` | Push per-block instead of chunk flush |
| `src/query/running.rs` | Use CompositeChunkReader instead of StaticSnapshot |
| `src/query/static_snapshot.rs` | Delete |
| `src/query/service.rs` | Use DatasetStore.reader() |
| `Dockerfile` | Remove RocksDB build deps |

## Files to modify in sqd-query-engine

| File | Change |
|------|--------|
| `src/scan/memory_backend.rs` | Add snapshot() method, use Arc<BlockData> |
| `src/scan/dataset_store.rs` | Add set_finalized_head() with fsync |
