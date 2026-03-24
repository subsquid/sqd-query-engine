//! Legacy sqd_storage-backed ChunkReader implementation.
//!
//! Wraps the legacy `sqd_storage::Database` to implement our `ChunkReader` trait.
//! The legacy storage uses a custom columnar page format on RocksDB — NOT Arrow IPC.
//!
//! Optimizations (all local to this file, no impact on parquet scanner):
//! - Two-phase read: filter columns first → narrow rows → read data columns for matches only
//! - Stats-based row filtering: predicate + block range + key filter block pruning
//! - Column projection: reads only the columns needed at each phase

use crate::scan::kv_scan::apply_scan_filters;
use crate::scan::predicate::RowPredicate;
use crate::scan::scanner::{
    block_range_mask, build_output_schema, composite_key_in_set_mask_fast, hierarchical_mask,
};
use crate::scan::{ChunkReader, ScanRequest};
use anyhow::{Context, Result};
use arrow::array::Array;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use sqd_primitives::range::RangeList;
use sqd_storage::db::{Chunk, ChunkReader as SqdChunkReader, Database, DatabaseSettings, ReadSnapshot};
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

/// Enable detailed scan profiling to stderr.
pub static PROFILE_SCANS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Per-thread snapshot + chunk_reader with warm table reader cache.
///
/// SAFETY: `chunk_reader` borrows from `snapshot`, which borrows from the `Database`
/// in `LegacyStorageChunkReader::db` (heap-pinned via Box).
/// Field drop order (top to bottom): chunk_reader → snapshot. Both valid since db outlives them.
struct PooledReader {
    chunk_reader: SqdChunkReader<'static>,
    snapshot: Box<ReadSnapshot<'static>>,
}

pub struct LegacyStorageChunkReader {
    // IMPORTANT: field order = drop order. `pool` must drop before `db`.
    pool: std::sync::Mutex<Vec<PooledReader>>,
    db: Box<Database>,
    chunk: Chunk,
    schemas: HashMap<String, SchemaRef>,
}

unsafe impl Sync for LegacyStorageChunkReader {}
unsafe impl Send for LegacyStorageChunkReader {}

impl LegacyStorageChunkReader {
    /// Open a legacy sqd_storage database and prepare for reading a specific chunk.
    pub fn open(path: &Path, chunk: Chunk) -> Result<Self> {
        let db = DatabaseSettings::default()
            .open(path)
            .context("opening legacy sqd_storage database")?;

        Self::from_database(db, chunk)
    }

    /// Create from an already-opened database (avoids lock conflicts).
    pub fn from_database(db: Database, chunk: Chunk) -> Result<Self> {
        let db = Box::new(db);

        // Load schemas using a temporary snapshot
        let mut schemas = HashMap::new();
        {
            let snapshot = db.snapshot();
            let chunk_reader = snapshot.create_chunk_reader(chunk.clone());
            for table_name in chunk.tables().keys() {
                if let Ok(table_reader) = chunk_reader.get_table_reader(table_name) {
                    schemas.insert(table_name.clone(), table_reader.schema());
                }
            }
        }

        Ok(Self {
            pool: std::sync::Mutex::new(Vec::new()),
            db,
            chunk,
            schemas,
        })
    }

    /// Borrow a reader from the pool, or create a new one with warm cache.
    fn acquire_reader(&self) -> PooledReader {
        // Fast path: reuse from pool
        if let Some(reader) = self.pool.lock().unwrap().pop() {
            return reader;
        }

        // Slow path: create new snapshot + chunk_reader, pre-warm cache
        // SAFETY: db is Box<Database> (heap-pinned, stable address).
        // snapshot borrows from db, chunk_reader borrows from snapshot (also heap-pinned).
        // PooledReader drop order: chunk_reader first, snapshot second.
        let snapshot = Box::new(self.db.snapshot());
        let snapshot: Box<ReadSnapshot<'static>> = unsafe { std::mem::transmute(snapshot) };
        let chunk_reader = snapshot.create_chunk_reader(self.chunk.clone());
        let chunk_reader: SqdChunkReader<'static> =
            unsafe { std::mem::transmute(chunk_reader) };

        // Pre-warm table reader cache
        for table_name in self.chunk.tables().keys() {
            let _ = chunk_reader.get_table_reader(table_name);
        }

        PooledReader {
            chunk_reader,
            snapshot,
        }
    }

    /// Return a reader to the pool for reuse.
    fn release_reader(&self, reader: PooledReader) {
        self.pool.lock().unwrap().push(reader);
    }
}

impl LegacyStorageChunkReader {
    fn scan_inner(
        &self,
        pooled: &PooledReader,
        table: &str,
        schema: &SchemaRef,
        request: &ScanRequest,
    ) -> Result<Vec<RecordBatch>> {
        let t0 = std::time::Instant::now();
        let table_reader = pooled.chunk_reader
            .get_table_reader(table)
            .context("getting legacy table reader")?;
        let t_get_reader = t0.elapsed();

        let has_filters = !request.predicates.is_empty()
            || request.key_filter.is_some()
            || request.hierarchical_filter.is_some()
            || request.from_block.is_some()
            || request.to_block.is_some();

        // Determine if two-phase read is worthwhile:
        // only when we have filters AND there are data-only columns to defer
        let filter_columns = collect_filter_columns(request);
        let all_columns = collect_all_columns(request);
        let has_deferred_columns = has_filters && all_columns.len() > filter_columns.len();

        if !has_deferred_columns {
            // Single-phase: no benefit from deferring, read everything at once
            let t1 = std::time::Instant::now();
            let stats_ranges = compute_all_row_ranges(&table_reader, request, schema);
            let t_stats = t1.elapsed();

            if stats_ranges.as_ref().map_or(false, |r| r.is_empty()) {
                return Ok(Vec::new());
            }

            let t2 = std::time::Instant::now();
            let batch = table_reader
                .read_table(Some(&all_columns), stats_ranges.as_ref())
                .context("reading legacy table")?;
            let t_read = t2.elapsed();

            if batch.num_rows() == 0 {
                return Ok(Vec::new());
            }

            let t3 = std::time::Instant::now();
            let result = apply_scan_filters(vec![batch], request, schema);
            let t_filter = t3.elapsed();

            if PROFILE_SCANS.load(std::sync::atomic::Ordering::Relaxed) {
                eprintln!(
                    "  [legacy 1-phase] {}: get_reader={:.2?} stats={:.2?} read={:.2?}({} cols) filter={:.2?} total={:.2?}",
                    table, t_get_reader, t_stats, t_read, all_columns.len(), t_filter, t0.elapsed()
                );
            }

            return result;
        }

        // === Two-phase read ===

        // Phase 1: read only filter/key columns with stats-based row ranges
        let t1 = std::time::Instant::now();
        let stats_ranges = compute_all_row_ranges(&table_reader, request, schema);
        let t_stats = t1.elapsed();

        if stats_ranges.as_ref().map_or(false, |r| r.is_empty()) {
            return Ok(Vec::new());
        }

        let t2 = std::time::Instant::now();
        let phase1_batch = table_reader
            .read_table(Some(&filter_columns), stats_ranges.as_ref())
            .context("reading legacy table (phase 1: filter columns)")?;
        let t_phase1 = t2.elapsed();

        if phase1_batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        // Apply all filters to get a boolean mask
        let t3 = std::time::Instant::now();
        let mask = compute_filter_mask(&phase1_batch, request);
        let t_mask = t3.elapsed();

        let match_count = mask.true_count();
        if match_count == 0 {
            return Ok(Vec::new());
        }

        // Compute deferred columns (output columns not yet read in phase 1)
        let deferred_columns: HashSet<&str> = all_columns
            .difference(&filter_columns)
            .copied()
            .collect();
        let output_schema = build_output_schema(schema, &request.output_columns);

        // If all rows match, read only deferred columns and merge with phase 1
        if match_count == phase1_batch.num_rows() {
            let t4 = std::time::Instant::now();
            if deferred_columns.is_empty() {
                let r = Ok(vec![project_batch(&phase1_batch, &output_schema)?]);
                if PROFILE_SCANS.load(std::sync::atomic::Ordering::Relaxed) {
                    eprintln!(
                        "  [legacy 2-phase all-match] {}: get_reader={:.2?} stats={:.2?} p1_read={:.2?}({} cols, {} rows) mask={:.2?} total={:.2?}",
                        table, t_get_reader, t_stats, t_phase1, filter_columns.len(), phase1_batch.num_rows(), t_mask, t0.elapsed()
                    );
                }
                return r;
            }

            let deferred_batch = table_reader
                .read_table(Some(&deferred_columns), stats_ranges.as_ref())
                .context("reading legacy table (deferred columns, all matched)")?;
            let t_phase2 = t4.elapsed();

            if PROFILE_SCANS.load(std::sync::atomic::Ordering::Relaxed) {
                eprintln!(
                    "  [legacy 2-phase all-match] {}: get_reader={:.2?} stats={:.2?} p1_read={:.2?}({} cols, {} rows) mask={:.2?} p2_read={:.2?}({} cols) total={:.2?}",
                    table, t_get_reader, t_stats, t_phase1, filter_columns.len(), phase1_batch.num_rows(), t_mask, t_phase2, deferred_columns.len(), t0.elapsed()
                );
            }

            return Ok(vec![merge_and_project(
                &phase1_batch,
                &deferred_batch,
                &output_schema,
            )?]);
        }

        // Phase 2: map filtered rows back to absolute indices
        let t4 = std::time::Instant::now();
        let final_ranges = mask_to_absolute_ranges(&mask, stats_ranges.as_ref());

        if final_ranges.is_empty() {
            return Ok(Vec::new());
        }

        // Filter phase 1 batch, read only deferred columns for matched rows
        let filtered_phase1 =
            arrow::compute::filter_record_batch(&phase1_batch, &mask)?;

        if deferred_columns.is_empty() {
            return Ok(vec![project_batch(&filtered_phase1, &output_schema)?]);
        }

        let deferred_batch = table_reader
            .read_table(Some(&deferred_columns), Some(&final_ranges))
            .context("reading legacy table (phase 2: deferred columns)")?;
        let t_phase2 = t4.elapsed();

        if PROFILE_SCANS.load(std::sync::atomic::Ordering::Relaxed) {
            eprintln!(
                "  [legacy 2-phase partial] {}: get_reader={:.2?} stats={:.2?} p1_read={:.2?}({} cols, {} rows) mask={:.2?}({}/{}) p2_read={:.2?}({} cols, {} rows) total={:.2?}",
                table, t_get_reader, t_stats, t_phase1, filter_columns.len(), phase1_batch.num_rows(),
                t_mask, match_count, phase1_batch.num_rows(),
                t_phase2, deferred_columns.len(), deferred_batch.num_rows(), t0.elapsed()
            );
        }

        Ok(vec![merge_and_project(
            &filtered_phase1,
            &deferred_batch,
            &output_schema,
        )?])
    }
}

impl ChunkReader for LegacyStorageChunkReader {
    fn scan(&self, table: &str, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
        let schema = match self.schemas.get(table) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };
        if !self.chunk.tables().contains_key(table) {
            return Ok(Vec::new());
        }

        let pooled = self.acquire_reader();
        let result = self.scan_inner(&pooled, table, schema, request);
        self.release_reader(pooled);
        result
    }

    fn has_table(&self, table: &str) -> bool {
        self.chunk.tables().contains_key(table)
    }

    fn table_schema(&self, table: &str) -> Option<SchemaRef> {
        self.schemas.get(table).cloned()
    }
}

// ---------------------------------------------------------------------------
// Column collection
// ---------------------------------------------------------------------------

/// Columns needed for filtering only (phase 1).
fn collect_filter_columns<'a>(request: &'a ScanRequest) -> HashSet<&'a str> {
    let mut cols: HashSet<&str> = HashSet::new();

    // Block number (for block range filter)
    if let Some(bn) = request.block_number_column {
        cols.insert(bn);
    }

    // Predicate columns
    for pred in &request.predicates {
        for col in pred.required_columns() {
            cols.insert(col);
        }
    }

    // Key filter columns
    if let Some(kf) = &request.key_filter {
        for col in &kf.columns {
            cols.insert(col.as_str());
        }
    }

    // Hierarchical filter columns
    if let Some(hf) = &request.hierarchical_filter {
        for col in &hf.group_key_columns {
            cols.insert(col.as_str());
        }
        cols.insert(&hf.address_column);
    }

    cols
}

/// All columns needed for the final output (phase 2 = filter + output).
fn collect_all_columns<'a>(request: &'a ScanRequest) -> HashSet<&'a str> {
    let mut cols = collect_filter_columns(request);
    for col in &request.output_columns {
        cols.insert(col);
    }
    cols
}

// ---------------------------------------------------------------------------
// Stats-based row range computation
// ---------------------------------------------------------------------------

/// Compute row ranges from all available stats: predicates + block range + key filter blocks.
fn compute_all_row_ranges(
    table_reader: &Arc<sqd_storage::db::SnapshotTableReader>,
    request: &ScanRequest,
    schema: &SchemaRef,
) -> Option<RangeList<u32>> {
    let num_rows = table_reader.num_rows();
    if num_rows == 0 {
        return Some(RangeList::new(vec![]));
    }

    let mut combined: Option<RangeList<u32>> = None;

    // 1. Predicate-based stats filtering
    if !request.predicates.is_empty() {
        if let Some(pred_ranges) = compute_predicate_row_ranges(table_reader, request, schema) {
            combined = Some(pred_ranges);
        }
    }

    // 2. Block range stats filtering
    if request.from_block.is_some() || request.to_block.is_some() {
        if let Some(bn_col) = request.block_number_column {
            if let Some(bn_ranges) =
                compute_block_range_rows(table_reader, schema, bn_col, request.from_block, request.to_block)
            {
                combined = Some(match combined {
                    None => bn_ranges,
                    Some(existing) => existing.intersection(&bn_ranges),
                });
            }
        }
    }

    // 3. Key filter block number pruning
    if let Some(kf) = &request.key_filter {
        if !kf.sorted_blocks.is_empty() {
            if let Some(kf_ranges) =
                compute_key_filter_block_ranges(table_reader, schema, &kf.block_number_column, &kf.sorted_blocks)
            {
                combined = Some(match combined {
                    None => kf_ranges,
                    Some(existing) => existing.intersection(&kf_ranges),
                });
            }
        }
    }

    combined
}

/// Use block_number column stats to filter rows by from_block/to_block range.
fn compute_block_range_rows(
    table_reader: &Arc<sqd_storage::db::SnapshotTableReader>,
    schema: &SchemaRef,
    bn_column: &str,
    from_block: Option<u64>,
    to_block: Option<u64>,
) -> Option<RangeList<u32>> {
    let col_index = schema.index_of(bn_column).ok()?;
    let stats = table_reader.get_column_stats(col_index).ok()??;

    let offsets = stats.offsets.as_ref();
    let num_windows = offsets.len().saturating_sub(1);
    if num_windows == 0 {
        return None;
    }

    let mut matching_ranges: Vec<Range<u32>> = Vec::new();

    for w in 0..num_windows {
        let win_max = stats_value_as_u64(&stats.max, w);
        let win_min = stats_value_as_u64(&stats.min, w);

        // Skip window if entirely outside block range
        if let Some(from) = from_block {
            if let Some(wmax) = win_max {
                if wmax < from {
                    continue;
                }
            }
        }
        if let Some(to) = to_block {
            if let Some(wmin) = win_min {
                if wmin > to {
                    continue;
                }
            }
        }

        matching_ranges.push(offsets[w]..offsets[w + 1]);
    }

    Some(RangeList::seal(matching_ranges.into_iter()))
}

/// Use block_number stats to filter by key filter's sorted_blocks.
fn compute_key_filter_block_ranges(
    table_reader: &Arc<sqd_storage::db::SnapshotTableReader>,
    schema: &SchemaRef,
    bn_column: &str,
    sorted_blocks: &[u64],
) -> Option<RangeList<u32>> {
    let col_index = schema.index_of(bn_column).ok()?;
    let stats = table_reader.get_column_stats(col_index).ok()??;

    let offsets = stats.offsets.as_ref();
    let num_windows = offsets.len().saturating_sub(1);
    if num_windows == 0 {
        return None;
    }

    let mut matching_ranges: Vec<Range<u32>> = Vec::new();

    for w in 0..num_windows {
        let win_min = stats_value_as_u64(&stats.min, w);
        let win_max = stats_value_as_u64(&stats.max, w);

        match (win_min, win_max) {
            (Some(wmin), Some(wmax)) => {
                // Binary search: any block number from key filter in [wmin, wmax]?
                let first = sorted_blocks.partition_point(|&bn| bn < wmin);
                if first < sorted_blocks.len() && sorted_blocks[first] <= wmax {
                    matching_ranges.push(offsets[w]..offsets[w + 1]);
                }
            }
            _ => {
                // Can't determine, include window
                matching_ranges.push(offsets[w]..offsets[w + 1]);
            }
        }
    }

    Some(RangeList::seal(matching_ranges.into_iter()))
}

/// Extract a u64 value from a stats min/max array at the given window index.
fn stats_value_as_u64(array: &dyn Array, index: usize) -> Option<u64> {
    use arrow::array::*;
    if array.is_null(index) {
        return None;
    }
    match array.data_type() {
        arrow::datatypes::DataType::Int32 => {
            Some(array.as_any().downcast_ref::<Int32Array>()?.value(index) as u64)
        }
        arrow::datatypes::DataType::UInt32 => {
            Some(array.as_any().downcast_ref::<UInt32Array>()?.value(index) as u64)
        }
        arrow::datatypes::DataType::Int64 => {
            Some(array.as_any().downcast_ref::<Int64Array>()?.value(index) as u64)
        }
        arrow::datatypes::DataType::UInt64 => {
            Some(array.as_any().downcast_ref::<UInt64Array>()?.value(index))
        }
        _ => None,
    }
}

/// Evaluate predicates against per-column window statistics.
fn compute_predicate_row_ranges(
    table_reader: &Arc<sqd_storage::db::SnapshotTableReader>,
    request: &ScanRequest,
    schema: &SchemaRef,
) -> Option<RangeList<u32>> {
    let mut any_stats_used = false;
    let mut combined_ranges: Option<RangeList<u32>> = None;

    for pred in &request.predicates {
        let window_ranges = compute_single_predicate_ranges(table_reader, pred, schema);
        match window_ranges {
            Some(ranges) => {
                any_stats_used = true;
                combined_ranges = Some(match combined_ranges {
                    None => ranges,
                    Some(existing) => existing.union(&ranges), // OR across predicates
                });
            }
            None => {
                // This predicate can't use stats → must include all rows
                return None;
            }
        }
    }

    if any_stats_used {
        combined_ranges
    } else {
        None
    }
}

/// For a single RowPredicate (AND of column predicates), compute matching row ranges.
fn compute_single_predicate_ranges(
    table_reader: &Arc<sqd_storage::db::SnapshotTableReader>,
    pred: &RowPredicate,
    schema: &SchemaRef,
) -> Option<RangeList<u32>> {
    let mut result: Option<RangeList<u32>> = None;

    for col_pred in &pred.columns {
        let col_index = match schema.index_of(&col_pred.column) {
            Ok(idx) => idx,
            Err(_) => {
                if PROFILE_SCANS.load(std::sync::atomic::Ordering::Relaxed) {
                    eprintln!("    stats: column '{}' not in schema", col_pred.column);
                }
                continue;
            }
        };

        let stats = match table_reader.get_column_stats(col_index) {
            Ok(Some(s)) => s,
            Ok(None) => {
                if PROFILE_SCANS.load(std::sync::atomic::Ordering::Relaxed) {
                    eprintln!("    stats: column '{}' (idx={}) has no stats", col_pred.column, col_index);
                }
                continue;
            }
            Err(e) => {
                if PROFILE_SCANS.load(std::sync::atomic::Ordering::Relaxed) {
                    eprintln!("    stats: column '{}' (idx={}) error: {}", col_pred.column, col_index, e);
                }
                continue;
            }
        };

        let offsets = stats.offsets.as_ref();
        let num_windows = offsets.len().saturating_sub(1);
        if num_windows == 0 {
            continue;
        }

        let mut matching_ranges: Vec<Range<u32>> = Vec::new();

        for w in 0..num_windows {
            let win_min = stats.min.slice(w, 1);
            let win_max = stats.max.slice(w, 1);

            if !col_pred.predicate.can_skip(win_min.as_ref(), win_max.as_ref()) {
                matching_ranges.push(offsets[w]..offsets[w + 1]);
            }
        }

        if PROFILE_SCANS.load(std::sync::atomic::Ordering::Relaxed) {
            let total_rows: u32 = matching_ranges.iter().map(|r| r.end - r.start).sum();
            eprintln!("    stats: column '{}' {} windows, {}/{} matched ({} rows)",
                col_pred.column, num_windows, matching_ranges.len(), num_windows, total_rows);
        }

        let col_ranges = RangeList::seal(matching_ranges.into_iter());

        // AND semantics: intersect
        result = Some(match result {
            None => col_ranges,
            Some(existing) => existing.intersection(&col_ranges),
        });

        if result.as_ref().map_or(false, |r| r.is_empty()) {
            return result;
        }
    }

    if result.is_none() {
        return None;
    }

    result
}

// ---------------------------------------------------------------------------
// Filter mask computation (phase 1 → BooleanArray)
// ---------------------------------------------------------------------------

/// Apply all filters to a phase-1 batch and return a combined BooleanArray mask.
/// Uses arrow `and()` to fuse masks without per-row loops.
fn compute_filter_mask(batch: &RecordBatch, request: &ScanRequest) -> arrow::array::BooleanArray {
    use arrow::array::BooleanArray;
    use arrow::compute::kernels::boolean::and;

    let mut mask: Option<BooleanArray> = None;

    // 1. Block range
    if let Some(bn_col) = request.block_number_column {
        if request.from_block.is_some() || request.to_block.is_some() {
            if let Some(col) = batch.column_by_name(bn_col) {
                let br_mask = block_range_mask(col, request.from_block, request.to_block);
                mask = Some(br_mask);
            }
        }
    }

    // 2. Predicates (OR across items) — no clone, use refs directly
    if !request.predicates.is_empty() {
        let pred_mask = crate::scan::predicate::or_row_predicates(&request.predicates, batch);
        mask = Some(match mask {
            None => pred_mask,
            Some(m) => and(&m, &pred_mask).unwrap(),
        });
    }

    // 3. Key filter
    if let Some(kf) = &request.key_filter {
        let kf_mask = composite_key_in_set_mask_fast(
            batch, &kf.columns, &kf.key_set,
            kf.key_set_fast.as_deref(),
        );
        mask = Some(match mask {
            None => kf_mask,
            Some(m) => and(&m, &kf_mask).unwrap(),
        });
    }

    // 4. Hierarchical filter
    if let Some(hf) = &request.hierarchical_filter {
        let hf_mask = hierarchical_mask(
            batch,
            &hf.source_addresses,
            &hf.first_key_set,
            &hf.group_key_columns,
            &hf.address_column,
            hf.mode,
            hf.inclusive,
        );
        mask = Some(match mask {
            None => hf_mask,
            Some(m) => and(&m, &hf_mask).unwrap(),
        });
    }

    mask.unwrap_or_else(|| BooleanArray::from(vec![true; batch.num_rows()]))
}

// ---------------------------------------------------------------------------
// Row index mapping
// ---------------------------------------------------------------------------

/// Convert a BooleanArray mask (relative to stats_ranges) back to absolute
/// row ranges for the final read. Builds ranges directly without expanding
/// to individual indices.
fn mask_to_absolute_ranges(
    mask: &arrow::array::BooleanArray,
    stats_ranges: Option<&RangeList<u32>>,
) -> RangeList<u32> {
    let mut result_ranges: Vec<Range<u32>> = Vec::new();

    match stats_ranges {
        Some(ranges) => {
            let mut relative = 0u32;
            for range in ranges.iter() {
                // Scan this range for consecutive runs of true values
                let base = range.start;
                let mut run_start: Option<u32> = None;

                for offset in 0..(range.end - range.start) {
                    let rel = relative + offset;
                    if rel < mask.len() as u32 && mask.value(rel as usize) {
                        if run_start.is_none() {
                            run_start = Some(base + offset);
                        }
                    } else if let Some(start) = run_start.take() {
                        result_ranges.push(start..(base + offset));
                    }
                }
                if let Some(start) = run_start {
                    result_ranges.push(start..range.end);
                }
                relative += range.end - range.start;
            }
        }
        None => {
            let mut run_start: Option<u32> = None;
            for i in 0..mask.len() {
                if mask.value(i) {
                    if run_start.is_none() {
                        run_start = Some(i as u32);
                    }
                } else if let Some(start) = run_start.take() {
                    result_ranges.push(start..i as u32);
                }
            }
            if let Some(start) = run_start {
                result_ranges.push(start..mask.len() as u32);
            }
        }
    }

    if result_ranges.is_empty() {
        RangeList::new(vec![])
    } else {
        RangeList::seal(result_ranges.into_iter())
    }
}

/// Merge two batches (phase1 filter columns + deferred data columns) and project to output.
/// Picks each output column from whichever batch has it; fills nulls for missing columns.
fn merge_and_project(
    phase1: &RecordBatch,
    deferred: &RecordBatch,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let columns: Vec<_> = output_schema
        .fields()
        .iter()
        .map(|field| {
            let name = field.name();
            phase1
                .column_by_name(name)
                .or_else(|| deferred.column_by_name(name))
                .cloned()
                .unwrap_or_else(|| {
                    arrow::array::new_null_array(field.data_type(), phase1.num_rows())
                })
        })
        .collect();
    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

/// Project a RecordBatch to only the columns in the output schema.
fn project_batch(batch: &RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
    let columns: Vec<_> = output_schema
        .fields()
        .iter()
        .map(|field| {
            batch
                .column_by_name(field.name())
                .cloned()
                .unwrap_or_else(|| {
                    arrow::array::new_null_array(field.data_type(), batch.num_rows())
                })
        })
        .collect();
    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}
