# Integration: Replace sqd_storage with DatasetStore in hotblocks server

## Status: COMPLETE

All phases implemented. The hotblocks server compiles with no `sqd_storage` code used.
`sqd-query` kept in Cargo.toml only to fix a transitive `hashbrown` version conflict with `polars-core`.

## What was done

### sqd-query-engine changes

| File | Change |
|------|--------|
| `src/scan/memory_backend.rs` | `MemorySnapshot` + `snapshot()` (Arc clones) |
| `src/scan/dataset_store.rs` | `DatasetSnapshot` + `snapshot()`, `Arc<ParquetChunkReader>` |
| `src/scan/mod.rs` | Export `DatasetSnapshot` |
| `src/query/parse.rs` | `RawQuery` + `parse_raw_query()` for single-deserialization pipeline |

### hotblocks changes

| File | Change |
|------|--------|
| `Cargo.toml` | Removed `sqd-storage`, `ouroboros`. Added `sqd-query-engine`. |
| `src/types.rs` | Removed `DBRef`. Added `DatasetId`, `DatasetKind::from_query_type()`, `metadata_name()`. |
| `src/dataset_controller/dataset_controller.rs` | Full rewrite. `DatasetStore` behind `Arc<RwLock>`, per-block ingest, fork detection via `block_refs`. |
| `src/dataset_controller/ingest_generic.rs` | Full rewrite. No `DataBuilder` buffering. Per-block `block_to_block_data()` conversion. `MaybeOnHead` is no-op. |
| `src/dataset_controller/ingest.rs` | Updated for new `IngestGeneric` API (type param, no builder instance). |
| `src/dataset_controller/mod.rs` | Removed `write_controller`. Exports `DatasetStoreRef`. |
| `src/dataset_controller/write_controller.rs` | **Deleted.** |
| `src/block_converter.rs` | Per-block `ChunkBuilder` → `BlockData` conversion. |
| `src/data_service.rs` | One `DatasetStore` per dataset. Loads metadata YAML per kind. |
| `src/cli.rs` | `--data-dir` + `--memory-cap` replace `--db` + `--data-cache-size` + `--rocksdb-*`. |
| `src/main.rs` | Removed DB init, `db_cleanup_task`. |
| `src/api.rs` | Removed `/rocksdb/*`. Uses `RawQuery` + `parse_raw_query()`. Single JSON deserialization. |
| `src/query/running.rs` | Uses `parse_query_raw()` + `compile()` + `execute_chunk()`. |
| `src/query/service.rs` | Uses `RawQuery` for validation. `DatasetSnapshot` for query isolation. |
| `src/query/response.rs` | Single-shot execution via `execute_chunk()`. No chunk iteration. |
| `src/query/static_snapshot.rs` | **Deleted** (ouroboros no longer needed). |
| `src/metrics.rs` | Removed `DatasetMetricsCollector` (was RocksDB-dependent). |
| `src/errors.rs` | `UnexpectedBaseBlock` defined locally. `QueryKindMismatch` uses `DatasetKind` enum. |
| `src/dataset_config.rs` | Uses `crate::types::DatasetId` instead of `sqd_storage::db::DatasetId`. |

## Query pipeline (single deserialization)

```
HTTP body (bytes)
  → parse_raw_query() → RawQuery { dataset_type, from_block, to_block, parent_block_hash, raw: Value }
  → service validates: dataset type, head check, chain continuity
  → parse_query_raw(raw, metadata) → Query (reuses already-parsed Value, no second deserialization)
  → compile(&query, metadata) → Plan
  → execute_chunk(&plan, metadata, &reader, writer)
```

## Storage architecture

```
DataEvent::Block { block }
  → block_to_block_data::<_, CB>(&block)  -- per-block ChunkBuilder, ~μs
  → IngestMessage::Block { block_data, block_ref, ... }
  → DatasetStore.push_block(block_data)   -- memory push ~255ns + crash log append
  → queryable immediately via DatasetSnapshot
```

Three tiers per dataset:
- **Memory buffer**: `Vec<Arc<BlockData>>`, instant push, snapshot isolation
- **Spillover parquet**: sorted by table sort_key, 8K row groups, triggered when memory > cap
- **Compacted parquet**: finalized blocks, same format as spillover

## Atomicity guarantees

| Guarantee | RocksDB (before) | DatasetStore (after) |
|-----------|-------------------|---------------------|
| Chunk write atomicity | Transaction commit | Memory push (instant) + IPC crash log |
| Snapshot isolation | `db.snapshot()` | `MemorySnapshot` (Arc clones) + `DatasetSnapshot` |
| Fork handling | `insert_fork()` atomic | `truncate()` + re-push (data re-fetchable) |
| Finalized head | Transaction + WAL | `finalized_head` file + atomic rename |
| Crash recovery | RocksDB WAL replay | IPC crash log + parquet scan |

## Dockerfile

No changes needed. The shared builder stage still installs `clang` for the archive binary
(which still uses RocksDB). Hotblocks binary is smaller since it no longer links `librocksdb`.

For Docker builds, `sqd-query-engine` path (`../../../sqd-query-engine`) needs to be
inside the build context or changed to a git dependency.

## Dataset stats API

`GET /datasets/{id}/stats` returns:

```json
{
  "memory": {
    "blocks": 42,
    "first_block": 24550780,
    "last_block": 24550821,
    "bytes": 1234567
  },
  "chunks": [
    {
      "first_block": 24550597,
      "last_block": 24550779,
      "tables": {
        "transactions": { "rows": 8500, "row_groups": 2, "compressed_bytes": 450000 },
        "logs": { "rows": 12000, "row_groups": 2, "compressed_bytes": 680000 }
      }
    }
  ],
  "finalized_head": 24550800,
  "disk_bytes": 2345678
}
```

Implemented via `DatasetStore::stats()` → `DatasetStats` (serializable).

## Remaining work

### Deployment
- [ ] Docker build context: make `sqd-query-engine` accessible (git dep or monorepo restructure)
- [ ] Remove `sqd-query` dependency once `polars-core` hashbrown conflict is resolved upstream

### Observability
- [ ] Prometheus metrics collector: expose per-dataset memory/disk/block stats via `/metrics`

### Testing
- [ ] Integration tests: start server, push blocks via mock data source, query via HTTP
- [ ] Fork/reorg test: simulate fork (409 response), verify truncation + re-ingest
- [ ] Crash recovery test: push blocks, kill -9, restart, verify data from crash log
- [ ] Retention test: set Head(100) retention, push 200 blocks, verify eviction
