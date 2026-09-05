use crate::integers::BlockNumbers;
use crate::metadata::DatasetDescription;
use crate::output::arrow_out::{
    dedup_first, filter_to_blocks, hexify_group, project_columns, write_arrow_frames, ArrowOutput,
    OutputFormat,
};
use crate::output::block_index::{
    build_block_index, collect_block_numbers, collect_boundary_blocks, compute_block_range,
};
use crate::output::columns::{
    find_address_column, group_keys_for_relation, physical_output_columns, required_output_columns,
    resolve_output_columns, resolve_relation_output_columns,
};
use crate::output::encoder::{encode_json_string, snake_to_camel};
use crate::output::row_writer::{
    build_field_writers, build_full_sort_columns, build_grouped_writers, resolve_grouped_writers,
    resolve_sort_columns, resolve_writers, IndexedBatches,
};
use crate::output::weight::{
    accumulate_block_weights, apply_weight_limit, block_scan_columns, get_weight_value,
    primary_weight_params, weight_cutoff_block, weight_scan_columns, TableOutput,
};
use crate::output::writer::QueryOutput;
use crate::query::{Plan, RelationKind};
use crate::scan::predicate::{evaluate_predicates_on_batch, RowPredicate};
use crate::scan::{
    ChunkReader, HierarchicalFilter, HierarchicalMode, KeyFilter, ParquetChunkReader, ScanRequest,
};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet as HashSet};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

/// A table is block-sorted when its physical sort key leads with the block
/// number column. Only then does a `to_block` cap prune whole row groups, which
/// is what makes the budget early-stop scan worthwhile (see `scan_budget`).
fn is_block_sorted(table_desc: &crate::metadata::TableDescription) -> bool {
    table_desc
        .sort_key
        .first()
        .map(|s| s == &table_desc.block_number_column)
        .unwrap_or(false)
}

/// The running weight of a budget walk, split at the walk's settle boundary.
///
/// Only the settled side may stop the walk. Rows above the boundary belong to a
/// block the next row group still owns, so their weight is not the block's; a
/// walk that stopped on them would leave the response short of the budget by
/// however much the last wave happened to hold, and by a different amount at
/// every wave width (INV-O13).
///
/// Every part of the estimate is a lower bound on what `apply_weight_limit`
/// charges the same block — this table's rows without its relations, the other
/// tables' rows without theirs, the fixed part of the header without its
/// data-dependent columns. That is what makes the cut safe to act on: once the
/// settled blocks weigh more than the budget, the exact trim lands at or below
/// the cut, so the blocks it selects are the ones a full scan would have
/// selected.
struct WaveWeight {
    /// Per-block weight of the blocks the walk has read but not yet settled.
    open: BTreeMap<u64, u64>,
    /// Blocks no unread row group can add to, summed.
    settled: u64,
    /// Blocks already charged their header and other-table weight.
    seen: HashSet<u64>,
}

impl WaveWeight {
    fn new() -> Self {
        Self {
            open: BTreeMap::new(),
            settled: 0,
            seen: HashSet::default(),
        }
    }

    /// Fold one wave's batches in, then settle every block below `boundary`
    /// (`None` settles all of them: the walk has read the last row group).
    /// Returns the settled weight so far.
    fn fold(
        &mut self,
        batches: &[RecordBatch],
        boundary: Option<u64>,
        params: &WaveWeightParams,
    ) -> u64 {
        for batch in batches {
            let bn = match batch.column_by_name(params.bn_col) {
                Some(c) => c,
                None => continue,
            };
            let wc_arrays: Vec<Option<&dyn arrow::array::Array>> = params
                .weight_cols
                .iter()
                .map(|name| batch.column_by_name(name).map(|c| c.as_ref()))
                .collect();

            // The scan resolves the block-number column on every batch it hands
            // out, so a batch arriving here without a readable one did not come
            // from one. Weighing nothing for it keeps the walk from cutting on
            // rows it cannot place: it reads on instead.
            let Ok(blocks) = BlockNumbers::resolve(bn.as_ref(), params.bn_col) else {
                continue;
            };

            for i in 0..batch.num_rows() {
                let block = blocks.at(i);

                let mut row_weight = params.fixed;
                for arr in wc_arrays.iter().flatten() {
                    row_weight = row_weight.saturating_add(get_weight_value(*arr, i));
                }
                if self.seen.insert(block) {
                    row_weight = row_weight.saturating_add(
                        params
                            .external
                            .get(&block)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(params.header_fixed),
                    );
                }

                let entry = self.open.entry(block).or_insert(0);
                *entry = entry.saturating_add(row_weight);
            }
        }

        let closed = match boundary {
            Some(b) => {
                let still_open = self.open.split_off(&b);
                std::mem::replace(&mut self.open, still_open)
            }
            None => std::mem::take(&mut self.open),
        };
        for weight in closed.into_values() {
            self.settled = self.settled.saturating_add(weight);
        }

        self.settled
    }
}

/// What a row of the walked table costs, and what its block costs the first time
/// the walk sees it.
struct WaveWeightParams<'a> {
    bn_col: &'a str,
    fixed: u64,
    weight_cols: &'a [String],
    external: &'a FxHashMap<u64, u64>,
    header_fixed: u64,
}

/// Execute a plan against a chunk directory. Returns `None` if the output
/// contains no blocks, which only happens when the queried block range doesn't
/// intersect the chunk's data — a query whose filters match nothing still
/// yields the boundary blocks of the range as header-only entries. See
/// [`QueryOutput`] for the block range metadata and lazy block encoding.
/// Request name → the position its table holds in the catalog, which is the
/// order item arrays appear in a response block.
fn request_name_positions(metadata: &DatasetDescription) -> HashMap<&str, usize> {
    metadata
        .tables
        .iter()
        .enumerate()
        .map(|(pos, (name, desc))| (desc.request_name(name), pos))
        .collect()
}

/// The knobs production leaves alone and a test has to move.
///
/// Both fields hold one value in the field — `P-WEIGHT-BUDGET`, and a budget
/// walk that is always allowed — and exist because a test that cannot move them
/// has to reach past the pipeline to say anything about them. A budget test that
/// cannot lower the budget runs against the scanner alone, where the estimate
/// and the exact trim never meet; a differential test that cannot switch the walk
/// off has to model what the walk does in order to check it, and a model of the
/// thing under test agrees with its bugs.
#[derive(Debug, Clone, Copy)]
pub struct ExecOptions {
    /// Print stage timings to stderr.
    pub profile: bool,
    /// The cumulative block weight a response may carry (`P-WEIGHT-BUDGET`).
    pub weight_budget: u64,
    /// Whether a block-sorted scan may stop early once the budget walk has read
    /// enough. Off reads every table whole, which is the answer the walk exists
    /// to reach sooner and must not change.
    pub budget_walk: bool,
}

impl Default for ExecOptions {
    fn default() -> Self {
        Self {
            profile: false,
            weight_budget: crate::output::weight::MAX_RESPONSE_BYTES,
            budget_walk: true,
        }
    }
}

impl ExecOptions {
    pub fn profiled(profile: bool) -> Self {
        Self {
            profile,
            ..Self::default()
        }
    }
}

pub fn execute_plan(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk_dir: &Path,
) -> Result<Option<QueryOutput>> {
    let chunk = ParquetChunkReader::open(chunk_dir)?;
    execute_chunk(plan, metadata, &chunk, false)
}

/// Execute a plan against any ChunkReader, with the engine's knobs given
/// explicitly. See [`ExecOptions`].
pub fn execute_chunk_with(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    options: ExecOptions,
) -> Result<Option<QueryOutput>> {
    match execute_chunk_fmt(plan, metadata, chunk, options, OutputFormat::Json)? {
        FmtOutput::Json(blocks) => Ok(blocks.map(|b| *b)),
        FmtOutput::Arrow(_) => unreachable!(),
    }
}

/// Execute a plan with timing instrumentation printed to stderr.
pub fn execute_plan_profiled(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk_dir: &Path,
) -> Result<Option<QueryOutput>> {
    let chunk = ParquetChunkReader::open(chunk_dir)?;
    execute_chunk(plan, metadata, &chunk, true)
}

/// Execute a plan against a chunk directory, producing flat per-table Arrow IPC
/// streams instead of nested JSON (prototype). `compress` toggles Arrow's
/// built-in Zstd. See [`crate::output::arrow_out`].
pub fn execute_plan_arrow(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk_dir: &Path,
    compress: bool,
    binary: bool,
) -> Result<Option<ArrowOutput>> {
    let chunk = ParquetChunkReader::open(chunk_dir)?;
    execute_chunk_arrow(plan, metadata, &chunk, compress, binary)
}

/// Execute a plan against any ChunkReader implementation.
pub fn execute_chunk(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    profile: bool,
) -> Result<Option<QueryOutput>> {
    match execute_chunk_fmt(
        plan,
        metadata,
        chunk,
        ExecOptions::profiled(profile),
        OutputFormat::Json,
    )? {
        FmtOutput::Json(blocks) => Ok(blocks.map(|b| *b)),
        FmtOutput::Arrow(_) => unreachable!(),
    }
}

/// Execute a plan against any ChunkReader implementation, producing flat
/// per-table Arrow IPC streams (prototype). See [`crate::output::arrow_out`].
pub fn execute_chunk_arrow(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    compress: bool,
    binary: bool,
) -> Result<Option<ArrowOutput>> {
    match execute_chunk_fmt(
        plan,
        metadata,
        chunk,
        ExecOptions::default(),
        OutputFormat::Arrow { compress, binary },
    )? {
        FmtOutput::Arrow(output) => Ok(output),
        FmtOutput::Json(_) => unreachable!(),
    }
}

enum FmtOutput {
    Json(Option<Box<QueryOutput>>),
    Arrow(Option<ArrowOutput>),
}

fn ensure_required_tables_present(plan: &Plan, chunk: &dyn ChunkReader) -> Result<()> {
    let ensure_present = |table: &str| {
        crate::engine_ensure!(
            chunk.has_table(table),
            crate::error::ErrorKind::TableNotFound,
            "table '{}' is not found in the chunk",
            table
        );
        Ok(())
    };

    ensure_present(&plan.block_table)?;
    for table_plan in &plan.table_plans {
        ensure_present(&table_plan.table)?;
        for relation in &table_plan.relations {
            ensure_present(&relation.target_table)?;
        }
    }

    Ok(())
}

/// Core execution: scan → block selection → output assembly. The `format`
/// selects the back half: nested JSON block encoding or flat Arrow IPC streams.
/// The expensive front half (scan, joins, weight limit) is shared.
fn execute_chunk_fmt(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    options: ExecOptions,
    format: OutputFormat,
) -> Result<FmtOutput> {
    use std::time::Instant;

    let profile = options.profile;

    macro_rules! timer {
        () => {
            if profile {
                Some(Instant::now())
            } else {
                None
            }
        };
    }
    macro_rules! elapsed {
        ($t:expr, $label:expr) => {
            if let Some(t) = $t { eprintln!("  {}: {:.2?}", $label, t.elapsed()); }
        };
        ($t:expr, $label:expr, $($arg:tt)*) => {
            if let Some(t) = $t { eprintln!("  {}: {:.2?} ({})", $label, t.elapsed(), format!($($arg)*)); }
        };
    }

    let t_total = timer!();

    // 0. A missing table is an incompatible chunk, not an empty table. Check
    //    every table the plan names before a zero-row primary scan can hide a
    //    missing relation target (INV-E4).
    ensure_required_tables_present(plan, chunk)?;

    // 1. A reorg between two pages must be reported, not paved over with data
    //    from the branch the client did not ask about.
    crate::output::fork::check_parent_block(plan, metadata, chunk)?;

    // 2. Scan all tables specified in the plan
    let mut table_outputs: HashMap<String, TableOutput> = HashMap::new();

    // Process block-sorted tables LAST so the budget early-stop scan can weigh
    // them against the (already known) per-block weight of the other tables.
    // sort is stable, so non-block-sorted tables keep their original order.
    let mut proc_order: Vec<usize> = (0..plan.table_plans.len()).collect();
    proc_order.sort_by_key(|&i| {
        metadata
            .table(&plan.table_plans[i].table)
            .map(is_block_sorted)
            .unwrap_or(false)
    });

    // Per-block weight contributed by already-scanned non-block-sorted tables,
    // seeding the early-stop budget walk. Deliberately under-counts (primary rows
    // only, no relations) so the cutoff can only over-include → byte-identical.
    // Only worth seeding when a block-sorted table coexists with another table;
    // otherwise the early-stop walk has no external weight to add, so skip the
    // extra accumulation pass entirely (keeps single-table / logs-only queries
    // on their original cost).
    let mut external_block_weight: FxHashMap<u64, u64> = FxHashMap::default();
    let seed_external = plan.table_plans.len() > 1
        && !plan.include_all_blocks
        && proc_order.iter().any(|&i| {
            metadata
                .table(&plan.table_plans[i].table)
                .map(is_block_sorted)
                .unwrap_or(false)
        });
    let header_fixed = crate::output::weight::header_weight_params(
        &plan.block_output_columns,
        metadata.table(&plan.block_table),
    )
    .0;

    // The block header scan must be capped to the phase-1 cutoff (when engaged):
    // otherwise the range-end boundary block enters block selection with only its
    // header weight (its item rows were never scanned) and wrongly survives the
    // budget trim, unlike in the exact path.
    let mut header_to_block = plan.to_block;

    // The highest block every early-stopped budget walk covers completely. The
    // walks read one table each and stop where its weight crosses the budget;
    // the response may not reach past the lowest of those cuts.
    let mut complete_through: Option<u64> = None;

    for &tp_idx in &proc_order {
        let table_plan = &plan.table_plans[tp_idx];
        let table_desc = metadata.table(&table_plan.table).ok_or_else(|| {
            crate::engine_err!(
                crate::error::ErrorKind::TableNotFound,
                "table '{}' not found",
                table_plan.table
            )
        })?;

        // Determine all columns needed for output (including virtual field sources)
        let output_cols = resolve_output_columns(table_plan, table_desc);
        let output_col_refs: Vec<&str> = output_cols.iter().map(|s| s.as_str()).collect();
        let req_cols = required_output_columns(&table_plan.output_columns, table_desc);
        let req_col_refs: Vec<&str> = req_cols.iter().map(|s| s.as_str()).collect();
        let pred_refs: Vec<&RowPredicate> = table_plan.predicates.iter().collect();

        // Two-phase scan for large single-table full scans: a cheap narrow scan
        // (block number + weight columns) finds the response-budget block cutoff,
        // so the real scan below decodes wide data columns only for rows that will
        // actually be emitted. Restricted to the single-source, unfiltered,
        // non-include-all-blocks case where the cutoff depends solely on this
        // table; the exact, header-aware `apply_weight_limit` still runs later and
        // performs the precise trim, so the output is byte-for-byte unchanged.
        let mut effective_to_block = plan.to_block;
        // Engage the two-phase scan only for a genuine single-table full scan:
        // one table, no relations, not include-all-blocks, and no *real* row
        // filter (an empty selector `{}` compiles to a trivial predicate with no
        // columns). Selective queries are left on the original path — their
        // result rarely hits the budget, so a phase-1 pre-scan would be pure
        // overhead.
        let single_full_scan = plan.table_plans.len() == 1
            && table_plan.relations.is_empty()
            && !plan.include_all_blocks
            && table_plan.predicates.iter().all(|p| p.columns.is_empty());
        if single_full_scan {
            // Narrow projection = block number + data-dependent weight columns
            // (the `*_size` companions) — never the wide data columns.
            let wcols = weight_scan_columns(&table_plan.output_columns, table_desc);
            let wcol_refs: Vec<&str> = wcols.iter().map(|s| s.as_str()).collect();
            let mut narrow_req = ScanRequest::new(wcol_refs);
            narrow_req.predicates = pred_refs.clone();
            narrow_req.from_block = Some(plan.from_block);
            narrow_req.to_block = plan.to_block;
            narrow_req.block_number_column = Some(table_desc.block_number_column.as_str());

            let t_phase1 = timer!();
            let narrow_batches = chunk.scan(&table_plan.table, &narrow_req)?;
            if let Some(cutoff) = weight_cutoff_block(
                &narrow_batches,
                &table_plan.output_columns,
                table_desc,
                options.weight_budget,
            ) {
                effective_to_block = Some(match plan.to_block {
                    Some(tb) => tb.min(cutoff),
                    None => cutoff,
                });
                header_to_block = effective_to_block;
            }
            elapsed!(
                t_phase1,
                "weight pre-scan",
                "cutoff -> {:?}",
                effective_to_block
            );
        }

        // Budget early-stop applies to block-sorted, non-include-all-blocks tables
        // that aren't already on the cheap single-table narrow pre-scan path. For
        // these a `to_block` cap prunes whole row groups, so reading row groups in
        // block order and stopping at the budget avoids decoding wide columns for
        // blocks that can't be emitted.
        let wave_eligible = options.budget_walk
            && is_block_sorted(table_desc)
            && !plan.include_all_blocks
            && !single_full_scan;

        let mut request = ScanRequest::new(output_col_refs);
        request.predicates = pred_refs;
        request.from_block = Some(plan.from_block);
        request.to_block = effective_to_block;
        request.block_number_column = Some(table_desc.block_number_column.as_str());
        request.required_columns = req_col_refs;

        let t_primary = timer!();
        let batches = if wave_eligible {
            // Stop once cumulative weight (this table + already-scanned tables +
            // header) crosses the budget. Over-reads ≤ one wave; the exact
            // `apply_weight_limit` trims afterwards, so output is byte-identical.
            let (fixed, weight_cols) =
                primary_weight_params(&table_plan.output_columns, Some(table_desc));
            let bn_col = table_desc.block_number_column.to_string();
            let params = WaveWeightParams {
                bn_col: &bn_col,
                fixed,
                weight_cols: &weight_cols,
                external: &external_block_weight,
                header_fixed,
            };
            let mut weight = WaveWeight::new();
            // Wave width = the rayon pool size: each wave saturates all cores in
            // one parallel shot. It decides how far the walk over-reads, never
            // which blocks come back — the walk stops on settled weight alone.
            let wave_size = rayon::current_num_threads().max(1);
            let mut settled_weight =
                |wave: &[RecordBatch], boundary: Option<u64>| weight.fold(wave, boundary, &params);

            let scan = chunk.scan_budget(
                &table_plan.table,
                &request,
                wave_size,
                options.weight_budget,
                &mut settled_weight,
            )?;

            // A walk that stopped early read this table only up to its cut. Every
            // other table was read for the whole range, so nothing further down
            // can tell that this one is short — the cut has to reach block
            // selection itself (INV-B7).
            if let Some(cut) = scan.complete_through {
                complete_through = Some(complete_through.map_or(cut, |c: u64| c.min(cut)));
            }

            scan.batches
        } else {
            chunk.scan(&table_plan.table, &request)?
        };
        let primary_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        elapsed!(t_primary, "primary scan", "{} rows", primary_rows);

        // Seed external weight for any block-sorted table processed later (only
        // non-block-sorted tables contribute; their full primary scan is done).
        if seed_external && !wave_eligible {
            let (fixed, weight_cols) =
                primary_weight_params(&table_plan.output_columns, Some(table_desc));
            accumulate_block_weights(
                &batches,
                table_desc.block_number_column.as_str(),
                fixed,
                &weight_cols,
                &mut external_block_weight,
            );
        }

        // Compute actual block range from primary scan for cross-table pruning
        let bn_col_name = table_desc.block_number_column.as_str();
        let (actual_min_block, actual_max_block) = compute_block_range(&batches, bn_col_name)?;

        // Execute relations (skip if primary scan returned no rows)
        let mut relation_batches: HashMap<usize, Vec<RecordBatch>> = HashMap::new();

        let has_primary_rows = batches.iter().any(|b| b.num_rows() > 0);
        if has_primary_rows && !table_plan.relations.is_empty() {
            // (relation tables are accessed via chunk.scan() — no pre-opening needed)

            // Build key filters for each relation (before parallel scan)
            let t_kf = timer!();
            let primary_bn_col = table_desc.block_number_column.as_str();

            // Pre-filter primary batches per relation when source_predicates are set
            let rel_filtered_batches: Vec<Option<Vec<RecordBatch>>> = table_plan
                .relations
                .iter()
                .map(|rel| {
                    rel.source_predicates.as_ref().map(|preds| {
                        batches
                            .iter()
                            .filter_map(|b| evaluate_predicates_on_batch(b, preds))
                            .collect()
                    })
                })
                .collect();

            let key_filters: Vec<Option<KeyFilter>> = table_plan
                .relations
                .iter()
                .enumerate()
                .map(|(rel_idx, rel)| {
                    let rel_table_desc = metadata.table(&rel.target_table);
                    let target_bn_col = rel_table_desc
                        .map(|d| d.block_number_column.as_str())
                        .unwrap_or("block_number");

                    // Use filtered batches if source_predicates are set
                    let source_batches =
                        rel_filtered_batches[rel_idx].as_deref().unwrap_or(&batches);

                    // Determine key columns for pushdown
                    let (left_keys, right_keys): (Vec<&str>, Vec<&str>) = match rel.kind {
                        RelationKind::Join => {
                            let lk: Vec<&str> = rel.left_key.iter().map(String::as_str).collect();
                            let rk: Vec<&str> = rel.right_key.iter().map(String::as_str).collect();
                            (lk, rk)
                        }
                        RelationKind::Children | RelationKind::Parents => {
                            // Use group keys (non-address columns) for pushdown
                            let addr_col = rel_table_desc.and_then(find_address_column);
                            let lk = group_keys_for_relation(&rel.left_key, addr_col);
                            let rk = group_keys_for_relation(&rel.right_key, addr_col);
                            if lk.is_empty() || rk.is_empty() {
                                return None;
                            }
                            (lk, rk)
                        }
                    };

                    if left_keys.is_empty() {
                        return None;
                    }

                    let kf = KeyFilter::build(
                        source_batches,
                        &left_keys,
                        &right_keys,
                        primary_bn_col,
                        target_bn_col,
                    );
                    if kf.is_empty() {
                        None
                    } else {
                        Some(kf)
                    }
                })
                .collect();

            // Build hierarchical filters for Children/Parents relations
            let hierarchical_filters: Vec<Option<HierarchicalFilter>> = table_plan
                .relations
                .iter()
                .enumerate()
                .map(|(rel_idx, rel)| match rel.kind {
                    RelationKind::Children | RelationKind::Parents => {
                        let rel_table_desc = metadata.table(&rel.target_table)?;
                        let target_addr_col = find_address_column(rel_table_desc)?;
                        let source_addr_col =
                            find_address_column(table_desc).unwrap_or(target_addr_col);
                        // Cross-table relations use inclusive prefix matching because the
                        // source and target address columns are different (e.g., calls.address
                        // vs events.call_address). Same-table uses strict matching.
                        let inclusive = source_addr_col != target_addr_col;
                        let gk = group_keys_for_relation(&rel.left_key, Some(target_addr_col));
                        if gk.is_empty() {
                            return None;
                        }
                        let mode = match rel.kind {
                            RelationKind::Children => HierarchicalMode::Children,
                            _ => HierarchicalMode::Parents,
                        };
                        let source_batches =
                            rel_filtered_batches[rel_idx].as_deref().unwrap_or(&batches);
                        let hf = HierarchicalFilter::build(
                            source_batches,
                            &gk,
                            source_addr_col,
                            target_addr_col,
                            mode,
                            inclusive,
                        );
                        if hf.is_empty() {
                            None
                        } else {
                            Some(hf)
                        }
                    }
                    _ => None,
                })
                .collect();

            elapsed!(t_kf, "key filter build");

            // Scan + join relations in parallel
            let t_rel = timer!();
            let rel_results: Vec<(usize, Result<Vec<RecordBatch>>)> = (0..table_plan
                .relations
                .len())
                .into_par_iter()
                .filter_map(|rel_idx| {
                    let rel = &table_plan.relations[rel_idx];
                    let kf_opt = &key_filters[rel_idx];
                    let hf_opt = &hierarchical_filters[rel_idx];

                    let rel_table_desc = metadata.table(&rel.target_table);

                    let rel_output_cols =
                        resolve_relation_output_columns(&rel.output_columns, rel_table_desc);
                    let rel_col_refs: Vec<&str> =
                        rel_output_cols.iter().map(|s| s.as_str()).collect();
                    let rel_req_cols = rel_table_desc
                        .map(|d| required_output_columns(&rel.output_columns, d))
                        .unwrap_or_default();
                    let rel_req_refs: Vec<&str> = rel_req_cols.iter().map(|s| s.as_str()).collect();

                    let mut rel_request = ScanRequest::new(rel_col_refs);
                    rel_request.from_block = actual_min_block;
                    rel_request.to_block = actual_max_block;
                    rel_request.required_columns = rel_req_refs;
                    if let Some(desc) = rel_table_desc {
                        rel_request.block_number_column = Some(desc.block_number_column.as_str());
                    }
                    rel_request.key_filter = kf_opt.as_ref();
                    rel_request.hierarchical_filter = hf_opt.as_ref();

                    let t_scan = if profile {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    let rel_all_batches = match chunk.scan(&rel.target_table, &rel_request) {
                        Ok(b) => b,
                        Err(e) => return Some((rel_idx, Err(e))),
                    };
                    let scan_rows: usize = rel_all_batches.iter().map(|b| b.num_rows()).sum();
                    if let Some(t) = t_scan {
                        eprintln!(
                            "    {} scan: {:.2?} ({} rows)",
                            rel.target_table,
                            t.elapsed(),
                            scan_rows
                        );
                    }

                    let left_key: Vec<&str> = rel.left_key.iter().map(String::as_str).collect();
                    let right_key: Vec<&str> = rel.right_key.iter().map(String::as_str).collect();

                    // Use filtered batches for join source when source_predicates are set
                    let source_batches =
                        rel_filtered_batches[rel_idx].as_deref().unwrap_or(&batches);

                    let t_join = if profile {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    let joined = match rel.kind {
                        RelationKind::Join if kf_opt.is_some() => {
                            // KeyFilter already ensured only matching rows were returned
                            // by the scan — skip redundant lookup_join.
                            Ok(rel_all_batches)
                        }
                        RelationKind::Join => crate::join::lookup_join(
                            source_batches,
                            &left_key,
                            &rel_all_batches,
                            &right_key,
                        ),
                        RelationKind::Children if hf_opt.is_some() => {
                            // HierarchicalFilter already applied during scan.
                            Ok(rel_all_batches)
                        }
                        RelationKind::Children => {
                            if let Some(desc) = rel_table_desc {
                                if let Some(target_addr) = find_address_column(desc) {
                                    let source_addr =
                                        find_address_column(table_desc).unwrap_or(target_addr);
                                    // Cross-table → inclusive prefix, same-table → strict prefix
                                    let inclusive = source_addr != target_addr;
                                    let gk =
                                        group_keys_for_relation(&rel.left_key, Some(target_addr));
                                    crate::join::find_children(
                                        source_batches,
                                        &rel_all_batches,
                                        &gk,
                                        source_addr,
                                        target_addr,
                                        inclusive,
                                    )
                                } else {
                                    Ok(Vec::new())
                                }
                            } else {
                                Ok(Vec::new())
                            }
                        }
                        RelationKind::Parents if hf_opt.is_some() => {
                            // HierarchicalFilter already applied during scan.
                            Ok(rel_all_batches)
                        }
                        RelationKind::Parents => {
                            if let Some(desc) = rel_table_desc {
                                if let Some(target_addr) = find_address_column(desc) {
                                    let source_addr =
                                        find_address_column(table_desc).unwrap_or(target_addr);
                                    // Cross-table → inclusive prefix, same-table → strict prefix
                                    let inclusive = source_addr != target_addr;
                                    let gk =
                                        group_keys_for_relation(&rel.left_key, Some(target_addr));
                                    crate::join::find_parents(
                                        source_batches,
                                        &rel_all_batches,
                                        &gk,
                                        source_addr,
                                        target_addr,
                                        inclusive,
                                    )
                                } else {
                                    Ok(Vec::new())
                                }
                            } else {
                                Ok(Vec::new())
                            }
                        }
                    };

                    if let Some(t) = t_join {
                        eprintln!("    {} join: {:.2?}", rel.target_table, t.elapsed());
                    }

                    Some((rel_idx, joined))
                })
                .collect();

            elapsed!(t_rel, "relation scans+joins");

            for (rel_idx, result) in rel_results {
                let rows: Vec<RecordBatch> = result?;
                if profile {
                    let n: usize = rows.iter().map(|b| b.num_rows()).sum();
                    eprintln!(
                        "    {}: {} rows",
                        table_plan.relations[rel_idx].target_table, n
                    );
                }
                relation_batches.entry(rel_idx).or_default().extend(rows);
            }
        }

        table_outputs.insert(
            table_plan.table.clone(),
            TableOutput {
                batches,
                relation_batches,
            },
        );
    }

    // A budget walk stopped short of `to_block`, so the covered range ends at its
    // cut: the header scan must not reach past it either, or the range-end
    // boundary block of INV-B3 enters selection weighing nothing and survives the
    // trim as a header with no items below it.
    if let Some(cut) = complete_through {
        header_to_block = Some(header_to_block.map_or(cut, |t: u64| t.min(cut)));
    }

    // 3. Read blocks table header
    let t_blocks = timer!();
    let block_table_desc = metadata.table(&plan.block_table);
    let readable_block_table = block_table_desc.filter(|_| chunk.has_table(&plan.block_table));
    let block_batches = if let Some(block_desc) = readable_block_table {
        // Block number + requested output columns + the weight companions those
        // columns declare (see `block_scan_columns`).
        let bn_col = block_desc.block_number_column.as_str();
        let block_cols = block_scan_columns(&plan.block_output_columns, block_desc);
        let block_col_vec: Vec<&str> = block_cols.iter().map(|s| s.as_str()).collect();
        let block_req_cols = required_output_columns(&plan.block_output_columns, block_desc);
        let block_req_refs: Vec<&str> = block_req_cols.iter().map(|s| s.as_str()).collect();

        let mut request = ScanRequest::new(block_col_vec);
        request.from_block = Some(plan.from_block);
        request.to_block = header_to_block;
        request.block_number_column = Some(bn_col);
        request.required_columns = block_req_refs;

        chunk.scan(&plan.block_table, &request)?
    } else {
        Vec::new()
    };

    // 4. Collect all block numbers that have data
    let mut block_numbers: HashSet<u64> = HashSet::default();

    // From table outputs
    for (table_name, output) in &table_outputs {
        let table_desc = metadata.table(table_name).unwrap();
        let bn_col = table_desc.block_number_column.as_str();
        collect_block_numbers(&output.batches, bn_col, &mut block_numbers)?;

        // A relation's target is a different table, and it names its own block
        // number column. Reading one literal name here would drop every row of
        // a table that calls it something else — out of block selection and out
        // of the weight model both (INV-X1).
        let plan_relations = plan
            .table_plans
            .iter()
            .find(|p| &p.table == table_name)
            .map(|p| p.relations.as_slice())
            .unwrap_or_default();

        for (rel_idx, rel_batches) in &output.relation_batches {
            let Some(rel) = plan_relations.get(*rel_idx) else {
                continue;
            };
            let rel_bn_col = metadata
                .table(&rel.target_table)
                .map(|d| d.block_number_column.as_str())
                .unwrap_or(bn_col);
            collect_block_numbers(rel_batches, rel_bn_col, &mut block_numbers)?;
        }
    }

    // Always include boundary blocks (first/last in range) from the block table
    {
        let bn_col = block_table_desc
            .map(|d| d.block_number_column.as_str())
            .unwrap_or("number");
        if plan.include_all_blocks {
            collect_block_numbers(&block_batches, bn_col, &mut block_numbers)?;
        } else {
            collect_boundary_blocks(&block_batches, bn_col, &mut block_numbers)?;
        }
    }

    // 5. Sort block numbers and apply weight-based limit
    let mut sorted_blocks: Vec<u64> = block_numbers.into_iter().collect();
    sorted_blocks.sort_unstable();

    // A block above a walk's cut carries rows from the tables that were read
    // whole and none from the one that stopped, and `lastBlock` naming it sends
    // the client past rows it will never ask for again (INV-B7).
    //
    // The walk only cuts once the blocks below the cut outweigh the budget, and
    // it weighs them at or below what the trim charges — so the trim ends at or
    // under the cut on its own, and this line normally drops nothing. It is the
    // scan's contract rather than the trim's backstop: the two agree only while
    // the walk's estimate stays under the exact model, and a block whose header
    // the chunk is missing is already outside that.
    #[cfg(debug_assertions)]
    let before_cut = sorted_blocks.clone();

    if let Some(cut) = complete_through {
        sorted_blocks.retain(|&block| block <= cut);
    }

    let selected_blocks = apply_weight_limit(
        options.weight_budget,
        &sorted_blocks,
        &table_outputs,
        &block_batches,
        metadata,
        plan,
    );

    // The paragraph above is a claim, so it is checked rather than believed.
    //
    // A cut above where the trim stops cannot change an answer, which is exactly
    // why a test that compares responses cannot see one drawn wrong until it
    // drops below — by then the response is already short of the budget the
    // client was promised, and the walk's estimate has already stopped being a
    // lower bound. Comparing the two selections says so at the moment it happens,
    // in whichever test happens to run the walk, rather than in the one somebody
    // wrote for it.
    #[cfg(debug_assertions)]
    if complete_through.is_some() {
        let uncut = apply_weight_limit(
            options.weight_budget,
            &before_cut,
            &table_outputs,
            &block_batches,
            metadata,
            plan,
        );

        debug_assert_eq!(
            selected_blocks, uncut,
            "the budget walk cut below the trim: it stopped at {complete_through:?}"
        );
    }

    if selected_blocks.is_empty() {
        return Ok(match format {
            OutputFormat::Json => FmtOutput::Json(None),
            OutputFormat::Arrow { .. } => FmtOutput::Arrow(None),
        });
    }

    // Arrow branch: emit flat per-table IPC streams straight from the post-scan
    // batches and return, skipping the entire JSON assembly below. Columns are
    // projected to the requested output fields (+ block_number key), rows are
    // trimmed to the weight-limited `selected_blocks`, and tables fed by several
    // sources are merged + deduped to match JSON. See `crate::output::arrow_out`.
    if let OutputFormat::Arrow { compress, binary } = format {
        let selected: HashSet<u64> = selected_blocks.iter().copied().collect();
        let keep = |b: u64| selected.contains(&b);

        // Collect every output source (primary scans + relation pulls).
        struct Src<'a> {
            qn: String,
            td: &'a crate::metadata::TableDescription,
            out_cols: &'a [String],
            batches: &'a [RecordBatch],
        }
        let mut srcs: Vec<Src> = Vec::new();
        for table_plan in &plan.table_plans {
            if let Some(output) = table_outputs.get(&table_plan.table) {
                let td = metadata.table(&table_plan.table).unwrap();
                srcs.push(Src {
                    qn: td.request_name(&table_plan.table).to_string(),
                    td,
                    out_cols: &table_plan.output_columns,
                    batches: &output.batches,
                });
                for (rel_idx, rel) in table_plan.relations.iter().enumerate() {
                    if let Some(rb) = output.relation_batches.get(&rel_idx) {
                        if let Some(rd) = metadata.table(&rel.target_table) {
                            srcs.push(Src {
                                qn: rd.request_name(&rel.target_table).to_string(),
                                td: rd,
                                out_cols: &rel.output_columns,
                                batches: rb,
                            });
                        }
                    }
                }
            }
        }

        // Group sources by output table name, ordered by metadata table order.
        let qn_pos = request_name_positions(metadata);
        let mut order: Vec<String> = Vec::new();
        let mut by_name: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, s) in srcs.iter().enumerate() {
            by_name
                .entry(s.qn.clone())
                .or_insert_with(|| {
                    order.push(s.qn.clone());
                    Vec::new()
                })
                .push(i);
        }
        order.sort_by_key(|qn| qn_pos.get(qn.as_str()).copied().unwrap_or(usize::MAX));

        let mut groups: Vec<(String, Vec<RecordBatch>)> = Vec::new();

        // Block header stream: project + weight-trim.
        {
            let bn = block_table_desc
                .map(|d| d.block_number_column.clone())
                .unwrap_or_else(|| "number".to_string());
            let mut wanted = vec![bn.clone()];
            let block_phys = match block_table_desc {
                Some(bd) => physical_output_columns(&plan.block_output_columns, bd),
                None => plan.block_output_columns.clone(),
            };
            for c in block_phys {
                if !wanted.contains(&c) {
                    wanted.push(c);
                }
            }
            let name = block_table_desc
                .map(|d| d.request_name(&plan.block_table))
                .unwrap_or(plan.block_table.as_str())
                .to_string();
            let mut batches: Vec<RecordBatch> = Vec::with_capacity(block_batches.len());
            for b in &block_batches {
                let trimmed = filter_to_blocks(&project_columns(b, &wanted)?, &bn, keep)?;
                if trimmed.num_rows() > 0 {
                    batches.push(trimmed);
                }
            }
            let batches = match (binary, block_table_desc) {
                (true, Some(bd)) => hexify_group(batches, bd)?,
                _ => batches,
            };
            groups.push((name, batches));
        }

        // Item tables.
        for qn in &order {
            let idxs = &by_name[qn];
            let td = srcs[idxs[0]].td;
            let bn = td.block_number_column.clone();
            let mut emit_cols = vec![bn.clone()];
            for c in physical_output_columns(srcs[idxs[0]].out_cols, td) {
                if !emit_cols.contains(&c) {
                    emit_cols.push(c);
                }
            }
            let multi = idxs.len() > 1;
            // For a multi-source table, carry the dedup key columns through the
            // projection so they exist at dedup time, then drop them on emit.
            let sort_cols = build_full_sort_columns(td);
            let mut proc_cols = emit_cols.clone();
            if multi {
                for c in &sort_cols {
                    if !proc_cols.contains(c) {
                        proc_cols.push(c.clone());
                    }
                }
            }

            let mut projected: Vec<RecordBatch> = Vec::new();
            for &si in idxs {
                for b in srcs[si].batches {
                    let f = filter_to_blocks(&project_columns(b, &proc_cols)?, &bn, keep)?;
                    if f.num_rows() > 0 {
                        projected.push(f);
                    }
                }
            }
            if projected.is_empty() {
                continue;
            }

            let batches: Vec<RecordBatch> = if multi {
                let schema = projected[0].schema();
                let merged = arrow::compute::concat_batches(&schema, &projected)?;
                let mut key = vec![bn.clone()];
                for c in &sort_cols {
                    if !key.contains(c) {
                        key.push(c.clone());
                    }
                }
                vec![project_columns(&dedup_first(&merged, &key)?, &emit_cols)?]
            } else {
                projected
            };

            groups.push((
                qn.clone(),
                if binary {
                    hexify_group(batches, td)?
                } else {
                    batches
                },
            ));
        }

        let data = write_arrow_frames(Vec::new(), &groups, compress)?;
        elapsed!(t_total, "TOTAL (arrow)");
        return Ok(FmtOutput::Arrow(Some(ArrowOutput::new(
            data,
            &selected_blocks,
        ))));
    }

    // 6. Pre-build block→rows indexes for each batch set
    let block_index = build_block_index(
        &block_batches,
        block_table_desc
            .map(|d| d.block_number_column.as_str())
            .unwrap_or("number"),
    )?;

    // Collect all indexed batch sources (both primary and relation), keyed by output table name.
    // Multiple sources for the same table get merged into a single output array.
    let mut all_indexes: Vec<IndexedBatches> = Vec::new();

    for table_plan in &plan.table_plans {
        if let Some(output) = table_outputs.remove(&table_plan.table) {
            let TableOutput {
                batches,
                mut relation_batches,
            } = output;
            let table_desc = metadata.table(&table_plan.table).unwrap();
            let bn_col = table_desc.block_number_column.as_str();
            let query_name = table_desc.request_name(&table_plan.table);

            let grouped = build_grouped_writers(&table_plan.output_columns, table_desc);
            let sort_columns = build_full_sort_columns(table_desc);
            let sort_col_resolved = resolve_sort_columns(&batches, &sort_columns);
            all_indexes.push(IndexedBatches {
                index: build_block_index(&batches, bn_col)?,
                batches,
                writers: build_field_writers(&table_plan.output_columns, Some(table_desc)),
                grouped,
                table_name: query_name.to_string(),
                sort_columns,
                sort_col_resolved,
            });

            for (rel_idx, rel) in table_plan.relations.iter().enumerate() {
                if let Some(rel_batches) = relation_batches.remove(&rel_idx) {
                    if let Some(rd) = metadata.table(&rel.target_table) {
                        let rel_bn = rd.block_number_column.as_str();
                        let rel_qn = rd.request_name(&rel.target_table);

                        let rel_grouped = build_grouped_writers(&rel.output_columns, rd);
                        let rel_sort_columns = build_full_sort_columns(rd);
                        let rel_sort_resolved =
                            resolve_sort_columns(&rel_batches, &rel_sort_columns);
                        all_indexes.push(IndexedBatches {
                            index: build_block_index(&rel_batches, rel_bn)?,
                            batches: rel_batches,
                            writers: build_field_writers(&rel.output_columns, Some(rd)),
                            grouped: rel_grouped,
                            table_name: rel_qn.to_string(),
                            sort_columns: rel_sort_columns,
                            sort_col_resolved: rel_sort_resolved,
                        });
                    }
                }
            }
        }
    }

    // Group indexes by output table name, ordered by metadata table definition order.
    let mut table_group_order: Vec<String> = Vec::new();
    let mut table_groups: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, idx) in all_indexes.iter().enumerate() {
        let entry = table_groups
            .entry(idx.table_name.clone())
            .or_insert_with(|| {
                table_group_order.push(idx.table_name.clone());
                Vec::new()
            });
        entry.push(i);
    }

    // Sort table_group_order by metadata table definition order (YAML key order).
    let query_name_order = request_name_positions(metadata);
    table_group_order.sort_by_key(|name| {
        query_name_order
            .get(name.as_str())
            .copied()
            .unwrap_or(usize::MAX)
    });

    // Build JSON array prefixes per output table
    let table_json_prefixes: HashMap<String, Vec<u8>> = table_group_order
        .iter()
        .map(|name| {
            let mut prefix = Vec::new();
            encode_json_string(name, &mut prefix);
            prefix.extend_from_slice(b":[");
            (name.clone(), prefix)
        })
        .collect();

    // Pre-compute block header writers
    let header_writers = build_field_writers(&plan.block_output_columns, block_table_desc);
    let bn_col = block_table_desc
        .map(|d| d.block_number_column.as_str())
        .unwrap_or("number");
    let mut bn_key_prefix = Vec::new();
    encode_json_string(&snake_to_camel(bn_col), &mut bn_key_prefix);
    bn_key_prefix.push(b':');

    // Pre-resolve column indices for header writers (once per batch schema)
    let header_resolved = block_batches
        .iter()
        .map(|b| resolve_writers(&header_writers, b))
        .collect::<Vec<_>>();

    // Pre-resolve column indices for each source (once per batch schema)
    let all_resolved = all_indexes
        .iter()
        .map(|idx| {
            idx.batches
                .iter()
                .map(|b| resolve_writers(&idx.writers, b))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    // Pre-resolve grouped writers
    let all_grouped_resolved = all_indexes
        .iter()
        .map(|idx| {
            idx.grouped.as_ref().map(|gw| {
                idx.batches
                    .iter()
                    .map(|b| resolve_grouped_writers(gw, b))
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();

    elapsed!(
        t_blocks,
        "blocks + indexing",
        "{} blocks",
        selected_blocks.len()
    );

    elapsed!(t_total, "TOTAL (front half; blocks encode lazily)");

    // Blocks are encoded lazily, one per QueryOutput::write_next_block call.
    // See decisions/002: sequential encoding wins at production concurrency.
    Ok(FmtOutput::Json(Some(Box::new(QueryOutput {
        selected_blocks,
        next: 0,
        block_batches,
        block_index,
        header_resolved,
        bn_key_prefix,
        all_indexes,
        all_resolved,
        all_grouped_resolved,
        table_group_order,
        table_groups,
        table_json_prefixes,
        sort_scratch: Vec::new(),
        merge_scratch: Vec::new(),
    }))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::load_dataset_description;
    use crate::output::row_writer::json_close;
    use crate::query::compile;
    use crate::query::parse_query;

    fn to_blocks(blocks: Option<QueryOutput>) -> Vec<serde_json::Value> {
        let mut result = Vec::new();
        if let Some(mut blocks) = blocks {
            let mut buf = Vec::new();
            while blocks.has_next_block() {
                buf.clear();
                blocks.write_next_block(&mut buf);
                result.push(serde_json::from_slice(&buf).unwrap());
            }
        }
        result
    }

    fn solana_metadata() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/solana.yaml")).unwrap()
    }

    fn evm_metadata() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/evm.yaml")).unwrap()
    }

    fn evm_chunk() -> ParquetChunkReader {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/evm/chunk");
        ParquetChunkReader::open(&dir).unwrap()
    }

    /// Parse framed Arrow streams → map of table name → (column names, row count).
    fn read_arrow_frames(framed: &[u8]) -> HashMap<String, (Vec<String>, usize)> {
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;
        let mut out: HashMap<String, (Vec<String>, usize)> = HashMap::new();
        let mut pos = 0usize;
        while pos + 4 <= framed.len() {
            let nl = u32::from_le_bytes(framed[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let name = String::from_utf8(framed[pos..pos + nl].to_vec()).unwrap();
            pos += nl;
            let pl = u32::from_le_bytes(framed[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let payload = &framed[pos..pos + pl];
            pos += pl;
            let reader = StreamReader::try_new(Cursor::new(payload), None).unwrap();
            let cols: Vec<String> = reader
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect();
            let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
            let e = out.entry(name).or_insert_with(|| (cols.clone(), 0));
            e.1 += rows;
        }
        out
    }

    /// Sum of array lengths per output table across all blocks in the JSON.
    fn json_item_counts(blocks: &[serde_json::Value]) -> HashMap<String, usize> {
        let mut map: HashMap<String, usize> = HashMap::new();
        for b in blocks {
            for (k, val) in b.as_object().unwrap() {
                if k == "header" {
                    continue;
                }
                if let Some(arr) = val.as_array() {
                    *map.entry(k.clone()).or_default() += arr.len();
                }
            }
        }
        map
    }

    /// Covers CT-6 · INV-O7
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_parity_and_projection() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = evm_metadata();
        let q = br#"{
            "type": "evm", "fromBlock": 0,
            "fields": {
                "block": { "number": true, "hash": true },
                "log": { "address": true, "topics": true, "data": true, "logIndex": true }
            },
            "logs": [{ "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"] }]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let chunk = evm_chunk();

        let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, false)
            .unwrap()
            .unwrap()
            .into_data();

        let jcounts = json_item_counts(&json);
        let frames = read_arrow_frames(&arrow);

        // Row-count parity on the item table (weight-trim correctness).
        assert_eq!(
            frames["logs"].1, jcounts["logs"],
            "arrow log rows must equal json log items"
        );
        assert!(frames["logs"].1 > 0, "should have logs");

        // `topics` (virtual Roll) is expanded to physical topic0..3, not dropped.
        let cols = &frames["logs"].0;
        assert!(
            cols.contains(&"topic0".to_string()),
            "topic0 present: {cols:?}"
        );
        assert!(cols.contains(&"topic1".to_string()), "topic1 present");
        assert!(cols.contains(&"address".to_string()));
        assert!(
            cols.contains(&"block_number".to_string()),
            "join key present"
        );
        // Internal scan/weight columns must NOT leak into output.
        assert!(
            !cols.contains(&"data_size".to_string()),
            "no internal data_size: {cols:?}"
        );
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_weight_trim_parity() {
        if !crate::testing::chunks_present() {
            return;
        }

        // Full-scan logs exceed the response budget on this chunk, so the JSON
        // path trims to weight-limited blocks. Flat Arrow must trim identically.
        let meta = evm_metadata();
        let q = br#"{
            "type": "evm", "fromBlock": 0,
            "fields": {
                "block": { "number": true },
                "log": { "address": true, "topics": true, "data": true, "logIndex": true, "transactionIndex": true }
            },
            "logs": [{}]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let chunk = evm_chunk();

        let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, false)
            .unwrap()
            .unwrap()
            .into_data();

        let jcounts = json_item_counts(&json);
        let frames = read_arrow_frames(&arrow);
        assert_eq!(
            frames["logs"].1, jcounts["logs"],
            "arrow must apply the same weight-limit row trim as json"
        );
    }

    /// Covers CT-4 · INV-R3
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_multisource_dedup() {
        if !crate::testing::chunks_present() {
            return;
        }

        // `transactions` is pulled by BOTH the traces and stateDiffs relations.
        // JSON merges+dedups into one array; flat Arrow must do the same.
        let meta = evm_metadata();
        let q = br#"{
            "type": "evm", "fromBlock": 0,
            "fields": {
                "block": { "number": true },
                "transaction": { "from": true, "to": true, "hash": true, "transactionIndex": true },
                "trace": { "type": true, "transactionIndex": true },
                "stateDiff": { "kind": true, "transactionIndex": true, "address": true }
            },
            "traces": [{ "type": ["call"], "callTo": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"], "transaction": true }],
            "stateDiffs": [{ "address": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"], "transaction": true }]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let chunk = evm_chunk();

        let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, false)
            .unwrap()
            .unwrap()
            .into_data();

        let jcounts = json_item_counts(&json);
        let frames = read_arrow_frames(&arrow);

        assert_eq!(
            frames["transactions"].1, jcounts["transactions"],
            "multi-source transactions must be deduped to match json"
        );
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_solana_base58_and_list() {
        if !crate::testing::chunks_present() {
            return;
        }

        // Solana: base58 columns (not 0x-hex) must stay Utf8 under `binary`, and
        // List<UInt16> instructionAddress must round-trip. Parity with JSON.
        let meta = solana_metadata();
        let q = br#"{
            "type": "solana", "fromBlock": 0,
            "fields": {
                "instruction": { "programId": true, "transactionIndex": true, "instructionAddress": true }
            },
            "instructions": [{ "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"] }]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk");
        let chunk = ParquetChunkReader::open(&dir).unwrap();

        let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
        // binary=true must not corrupt base58 (non-0x) columns.
        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, true)
            .unwrap()
            .unwrap()
            .into_data();

        let jcounts = json_item_counts(&json);
        let frames = read_arrow_frames(&arrow);
        assert_eq!(frames["instructions"].1, jcounts["instructions"]);
        assert!(frames["instructions"]
            .0
            .contains(&"instruction_address".to_string()));
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_binary_columns() {
        if !crate::testing::chunks_present() {
            return;
        }

        use arrow::datatypes::DataType;
        let meta = evm_metadata();
        let q = br#"{
            "type": "evm", "fromBlock": 0,
            "fields": {
                "block": { "number": true },
                "log": { "address": true, "topics": true, "data": true, "logIndex": true }
            },
            "logs": [{ "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"] }]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let chunk = evm_chunk();

        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, true)
            .unwrap()
            .unwrap()
            .into_data();

        // Inspect the logs stream schema: hex columns (encoding: hex_bytes) decode
        // to variable Binary, driven by metadata so the type is stable across
        // responses — including all-null columns like topic3 on 3-topic logs.
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;
        let mut pos = 0usize;
        let mut checked = false;
        while pos + 4 <= arrow.len() {
            let nl = u32::from_le_bytes(arrow[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let name = String::from_utf8(arrow[pos..pos + nl].to_vec()).unwrap();
            pos += nl;
            let pl = u32::from_le_bytes(arrow[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let payload = &arrow[pos..pos + pl];
            pos += pl;
            if name == "logs" {
                let reader = StreamReader::try_new(Cursor::new(payload), None).unwrap();
                let schema = reader.schema();
                let addr = schema
                    .field_with_name("address")
                    .unwrap()
                    .data_type()
                    .clone();
                let topic0 = schema
                    .field_with_name("topic0")
                    .unwrap()
                    .data_type()
                    .clone();
                let topic3 = schema
                    .field_with_name("topic3")
                    .unwrap()
                    .data_type()
                    .clone();
                let data = schema.field_with_name("data").unwrap().data_type().clone();
                assert_eq!(addr, DataType::Binary, "address hex → Binary");
                assert_eq!(topic0, DataType::Binary, "topic0 hex → Binary");
                // topic3 is all-null for 3-topic Transfer logs but is still Binary:
                // the type comes from metadata, not from the values present.
                assert_eq!(
                    topic3,
                    DataType::Binary,
                    "all-null topic3 → Binary (stable)"
                );
                assert_eq!(data, DataType::Binary, "data hex → Binary");
                checked = true;
            }
        }
        assert!(checked, "logs stream must be present");
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_execute_solana_instructions() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 0,
            "fields": {
                "block": { "number": true, "hash": true },
                "instruction": { "programId": true, "transactionIndex": true, "instructionAddress": true }
            },
            "instructions": [{
                "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"]
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk");
        let blocks = to_blocks(execute_plan(&plan, &meta, &chunk_dir).unwrap());

        assert!(!blocks.is_empty(), "should have at least one block");

        // Each block should have a header
        for block in &blocks {
            assert!(block.get("header").is_some(), "block should have header");
        }

        // At least one block should have instructions
        let has_instructions = blocks.iter().any(|b| b.get("instructions").is_some());
        assert!(has_instructions, "should have instructions in output");

        // Verify instruction fields are camelCase
        for block in &blocks {
            if let Some(instrs) = block.get("instructions") {
                for instr in instrs.as_array().unwrap() {
                    assert!(instr.get("programId").is_some());
                    assert!(instr.get("transactionIndex").is_some());
                    assert!(instr.get("instructionAddress").is_some());
                }
            }
        }
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_execute_evm_logs() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = evm_metadata();
        let json = br#"{
            "type": "evm",
            "fromBlock": 0,
            "fields": {
                "block": { "number": true, "hash": true },
                "log": { "address": true, "topics": true, "data": true, "logIndex": true }
            },
            "logs": [{
                "topic0": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/evm/chunk");
        let blocks = to_blocks(execute_plan(&plan, &meta, &chunk_dir).unwrap());

        assert!(!blocks.is_empty());

        // Check topics is an array (virtual field via roll)
        for block in &blocks {
            if let Some(logs) = block.get("logs") {
                for log in logs.as_array().unwrap() {
                    if let Some(topics) = log.get("topics") {
                        assert!(topics.is_array(), "topics should be an array");
                        let topics_arr = topics.as_array().unwrap();
                        assert!(!topics_arr.is_empty(), "topics should not be empty");
                        // First topic should be the Transfer event signature
                        let t0 = topics_arr[0].as_str().unwrap();
                        assert!(t0.starts_with("0x"), "topic should be hex");
                    }
                }
            }
        }
    }

    /// Covers CT-4 · INV-R10
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_execute_with_relations() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 0,
            "fields": {
                "instruction": { "programId": true, "transactionIndex": true },
                "transaction": { "transactionIndex": true, "feePayer": true }
            },
            "instructions": [{
                "programId": ["whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"],
                "transaction": true
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk");
        let blocks = to_blocks(execute_plan(&plan, &meta, &chunk_dir).unwrap());

        // Should have both instructions and transactions
        let has_txs = blocks.iter().any(|b| b.get("transactions").is_some());
        assert!(has_txs, "should have related transactions");
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_execute_empty_result() {
        if !crate::testing::chunks_present() {
            return;
        }

        let meta = solana_metadata();
        let json = br#"{
            "type": "solana",
            "fromBlock": 999999999,
            "toBlock": 999999999,
            "instructions": [{
                "programId": ["nonexistent_program"]
            }]
        }"#;

        let query = parse_query(json, &meta).unwrap();
        let plan = compile(&query, &meta).unwrap();

        let chunk_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/chunk");
        let blocks = to_blocks(execute_plan(&plan, &meta, &chunk_dir).unwrap());

        assert!(blocks.is_empty());
    }

    /// Covers CT-6 · INV-O1
    #[test]
    fn test_json_close() {
        let mut buf = vec![b'{', b'"', b'a', b'"', b':', b'1', b','];
        json_close(b'}', &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "{\"a\":1}");

        let mut buf = vec![b'{'];
        json_close(b'}', &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "{}");
    }

    /// Covers CT-6 · INV-O8
    #[test]
    fn test_snake_to_camel_in_output() {
        assert_eq!(snake_to_camel("log_index"), "logIndex");
        assert_eq!(snake_to_camel("transaction_hash"), "transactionHash");
        assert_eq!(snake_to_camel("number"), "number");
    }
}
