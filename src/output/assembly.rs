use crate::metadata::{
    DatasetDescription, FieldGrouping, OutputEncoding, TableDescription, VirtualField,
};
use crate::output::encoder::{
    encode_bignum, encode_json_passthrough, encode_json_string, encode_roll,
    encode_solana_tx_version, encode_value, snake_to_camel,
};
use crate::output::writer::JsonArrayWriter;
use crate::query::{Plan, RelationKind, TablePlan};
use crate::scan::predicate::{evaluate_predicates_on_batch, RowPredicate};
use crate::scan::{
    scan, HierarchicalFilter, HierarchicalMode, KeyFilter, ParquetTable, ScanRequest,
};
use anyhow::{anyhow, Result};
use arrow::array::*;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;

/// Maximum response size in bytes (20 MB).
const MAX_RESPONSE_BYTES: u64 = 20 * 1024 * 1024;

/// Default weight per row when no weight column is specified.
const DEFAULT_ROW_WEIGHT: u64 = 32;

/// Table data: the query-driven items and any relation results.
struct TableOutput {
    /// The primary table's filtered rows.
    batches: Vec<RecordBatch>,
    /// Relation results keyed by target table name.
    relation_batches: HashMap<String, Vec<RecordBatch>>,
}

/// Pre-indexed batch data for a single table source (primary or relation).
struct IndexedBatches<'a> {
    batches: &'a [RecordBatch],
    index: HashMap<u64, Vec<(usize, usize)>>,
    table_desc: &'a TableDescription,
    writers: Vec<FieldWriter>,
    grouped: Option<GroupedWriters>,
    /// Actual table name (for merging same-table sources)
    table_name: String,
    /// Pre-computed sort columns (item_order_keys + address_column, excluding block_number).
    sort_columns: Vec<String>,
    /// Pre-resolved sort column indices per batch.
    sort_col_resolved: Vec<Vec<Option<usize>>>,
}

/// Execute a plan with timing instrumentation printed to stderr.
pub fn execute_plan_profiled<W: Write>(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk_dir: &Path,
    writer: W,
) -> Result<W> {
    let mut cache = HashMap::new();
    execute_plan_inner(plan, metadata, chunk_dir, &mut cache, writer, true)
}

/// Execute a plan against a chunk directory and write JSON output.
pub fn execute_plan<W: Write>(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk_dir: &Path,
    writer: W,
) -> Result<W> {
    let mut cache = HashMap::new();
    execute_plan_inner(plan, metadata, chunk_dir, &mut cache, writer, false)
}

/// Execute a plan with a pre-populated parquet table cache.
/// Tables in the cache are reused; missing tables are opened and added to the cache.
pub fn execute_plan_cached<W: Write>(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk_dir: &Path,
    cache: &mut HashMap<String, ParquetTable>,
    writer: W,
) -> Result<W> {
    execute_plan_inner(plan, metadata, chunk_dir, cache, writer, false)
}

fn execute_plan_inner<W: Write>(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk_dir: &Path,
    parquet_cache: &mut HashMap<String, ParquetTable>,
    writer: W,
    profile: bool,
) -> Result<W> {
    use std::time::Instant;

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
    let mut json_writer = JsonArrayWriter::new(writer);

    // 1. Scan all tables specified in the plan
    let mut table_outputs: HashMap<String, TableOutput> = HashMap::new();

    for table_plan in &plan.table_plans {
        let table_desc = metadata
            .table(&table_plan.table)
            .ok_or_else(|| anyhow!("table '{}' not found", table_plan.table))?;

        let table_path = chunk_dir.join(format!("{}.parquet", table_plan.table));
        if !table_path.exists() {
            continue;
        }
        if !parquet_cache.contains_key(&table_plan.table) {
            parquet_cache.insert(table_plan.table.clone(), ParquetTable::open(&table_path)?);
        }
        let parquet_table = parquet_cache.get(&table_plan.table).unwrap();

        // Determine all columns needed for output (including virtual field sources)
        let output_cols = resolve_output_columns(table_plan, table_desc);
        let output_col_refs: Vec<&str> = output_cols.iter().map(|s| s.as_str()).collect();
        let pred_refs: Vec<&RowPredicate> = table_plan.predicates.iter().collect();

        let mut request = ScanRequest::new(output_col_refs);
        request.predicates = pred_refs;
        request.from_block = Some(plan.from_block);
        request.to_block = plan.to_block;
        request.block_number_column = table_desc.block_number_column.as_deref();

        let t_primary = timer!();
        let batches = scan(&parquet_table, &request)?;
        let primary_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        elapsed!(t_primary, "primary scan", "{} rows", primary_rows);

        // Compute actual block range from primary scan for cross-table pruning
        let bn_col_name = table_desc
            .block_number_column
            .as_deref()
            .unwrap_or("block_number");
        let (actual_min_block, actual_max_block) = compute_block_range(&batches, bn_col_name);

        // Execute relations (skip if primary scan returned no rows)
        let mut relation_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();

        let has_primary_rows = batches.iter().any(|b| b.num_rows() > 0);
        if has_primary_rows && !table_plan.relations.is_empty() {
            // Pre-open all relation parquet files
            for rel in &table_plan.relations {
                if !parquet_cache.contains_key(&rel.target_table) {
                    let rel_table_path = chunk_dir.join(format!("{}.parquet", rel.target_table));
                    if rel_table_path.exists() {
                        parquet_cache.insert(
                            rel.target_table.clone(),
                            ParquetTable::open(&rel_table_path)?,
                        );
                    }
                }
            }

            // Build key filters for each relation (before parallel scan)
            let t_kf = timer!();
            let primary_bn_col = table_desc
                .block_number_column
                .as_deref()
                .unwrap_or("block_number");

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
                        .and_then(|d| d.block_number_column.as_deref())
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
                        let addr_col = find_address_column(rel_table_desc)?;
                        let gk = group_keys_for_relation(&rel.left_key, Some(addr_col));
                        if gk.is_empty() {
                            return None;
                        }
                        let mode = match rel.kind {
                            RelationKind::Children => HierarchicalMode::Children,
                            _ => HierarchicalMode::Parents,
                        };
                        let source_batches =
                            rel_filtered_batches[rel_idx].as_deref().unwrap_or(&batches);
                        let hf = HierarchicalFilter::build(source_batches, &gk, addr_col, mode);
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
            let rel_results: Vec<(String, Result<Vec<RecordBatch>>)> = (0..table_plan
                .relations
                .len())
                .into_par_iter()
                .filter_map(|rel_idx| {
                    let rel = &table_plan.relations[rel_idx];
                    let kf_opt = &key_filters[rel_idx];
                    let hf_opt = &hierarchical_filters[rel_idx];

                    let rel_parquet = parquet_cache.get(&rel.target_table)?;
                    let rel_table_desc = metadata.table(&rel.target_table);

                    let rel_output_cols =
                        resolve_relation_output_columns(&rel.output_columns, rel_table_desc);
                    let rel_col_refs: Vec<&str> =
                        rel_output_cols.iter().map(|s| s.as_str()).collect();

                    let mut rel_request = ScanRequest::new(rel_col_refs);
                    rel_request.from_block = actual_min_block;
                    rel_request.to_block = actual_max_block;
                    if let Some(desc) = rel_table_desc {
                        rel_request.block_number_column = desc.block_number_column.as_deref();
                    }
                    rel_request.key_filter = kf_opt.as_ref();
                    rel_request.hierarchical_filter = hf_opt.as_ref();

                    let t_scan = if profile {
                        Some(std::time::Instant::now())
                    } else {
                        None
                    };
                    let rel_all_batches = match scan(rel_parquet, &rel_request) {
                        Ok(b) => b,
                        Err(e) => return Some((rel.target_table.clone(), Err(e))),
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
                                if let Some(addr) = find_address_column(desc) {
                                    let gk = group_keys_for_relation(&rel.left_key, Some(addr));
                                    crate::join::find_children(
                                        source_batches,
                                        &rel_all_batches,
                                        &gk,
                                        addr,
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
                                if let Some(addr) = find_address_column(desc) {
                                    let gk = group_keys_for_relation(&rel.left_key, Some(addr));
                                    crate::join::find_parents(
                                        source_batches,
                                        &rel_all_batches,
                                        &gk,
                                        addr,
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

                    Some((rel.target_table.clone(), joined))
                })
                .collect();

            elapsed!(t_rel, "relation scans+joins");

            for (target_table, result) in rel_results {
                let rows: Vec<RecordBatch> = result?;
                if profile {
                    let n: usize = rows.iter().map(|b| b.num_rows()).sum();
                    eprintln!("    {}: {} rows", target_table, n);
                }
                relation_batches
                    .entry(target_table)
                    .or_default()
                    .extend(rows);
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

    // 2. Read blocks table header
    let t_blocks = timer!();
    let block_table_desc = metadata.table(&plan.block_table);
    let block_path = chunk_dir.join(format!("{}.parquet", plan.block_table));
    let block_batches = if block_path.exists() && block_table_desc.is_some() {
        let block_desc = block_table_desc.unwrap();
        if !parquet_cache.contains_key(&plan.block_table) {
            parquet_cache.insert(plan.block_table.clone(), ParquetTable::open(&block_path)?);
        }
        let block_parquet = parquet_cache.get(&plan.block_table).unwrap();

        // Always read block_number column + requested output columns
        let bn_col = block_desc
            .block_number_column
            .as_deref()
            .unwrap_or("number");
        let mut block_cols: HashSet<&str> = HashSet::new();
        block_cols.insert(bn_col);
        for col in &plan.block_output_columns {
            block_cols.insert(col);
        }
        let block_col_vec: Vec<&str> = block_cols.into_iter().collect();

        let mut request = ScanRequest::new(block_col_vec);
        request.from_block = Some(plan.from_block);
        request.to_block = plan.to_block;
        request.block_number_column = Some(bn_col);

        scan(&block_parquet, &request)?
    } else {
        Vec::new()
    };

    // 3. Collect all block numbers that have data
    let mut block_numbers: HashSet<u64> = HashSet::new();

    // From table outputs
    for (table_name, output) in &table_outputs {
        let table_desc = metadata.table(table_name).unwrap();
        let bn_col = table_desc
            .block_number_column
            .as_deref()
            .unwrap_or("block_number");
        collect_block_numbers(&output.batches, bn_col, &mut block_numbers);
        for rel_batches in output.relation_batches.values() {
            // Relations may be from different tables with different bn columns
            collect_block_numbers(rel_batches, "block_number", &mut block_numbers);
        }
    }

    // Always include boundary blocks (first/last in range) from the block table
    {
        let bn_col = block_table_desc
            .and_then(|d| d.block_number_column.as_deref())
            .unwrap_or("number");
        if plan.include_all_blocks {
            collect_block_numbers(&block_batches, bn_col, &mut block_numbers);
        } else {
            collect_boundary_blocks(&block_batches, bn_col, &mut block_numbers);
        }
    }

    // 4. Sort block numbers and apply weight-based limit
    let mut sorted_blocks: Vec<u64> = block_numbers.into_iter().collect();
    sorted_blocks.sort_unstable();

    let selected_blocks = apply_weight_limit(
        &sorted_blocks,
        &table_outputs,
        &block_batches,
        metadata,
        plan,
    );

    // 5. Pre-build block→rows indexes for each batch set
    let block_index = build_block_index(
        &block_batches,
        block_table_desc
            .and_then(|d| d.block_number_column.as_deref())
            .unwrap_or("number"),
    );

    // Collect all indexed batch sources (both primary and relation), keyed by output table name.
    // Multiple sources for the same table get merged into a single output array.
    let mut all_indexes: Vec<IndexedBatches> = Vec::new();

    for table_plan in &plan.table_plans {
        if let Some(output) = table_outputs.get(&table_plan.table) {
            let table_desc = metadata.table(&table_plan.table).unwrap();
            let bn_col = table_desc
                .block_number_column
                .as_deref()
                .unwrap_or("block_number");
            let query_name = table_desc
                .query_name
                .as_deref()
                .unwrap_or(&table_plan.table);

            let grouped = table_desc
                .field_groups
                .as_ref()
                .map(|fg| build_grouped_writers(&table_plan.output_columns, table_desc, fg));
            let sort_columns = build_full_sort_columns(table_desc);
            let sort_col_resolved = resolve_sort_columns(&output.batches, &sort_columns);
            all_indexes.push(IndexedBatches {
                batches: &output.batches,
                index: build_block_index(&output.batches, bn_col),
                table_desc,
                writers: build_field_writers(&table_plan.output_columns, Some(table_desc)),
                grouped,
                table_name: query_name.to_string(),
                sort_columns,
                sort_col_resolved,
            });

            for rel in &table_plan.relations {
                if let Some(rel_batches) = output.relation_batches.get(&rel.target_table) {
                    if let Some(rd) = metadata.table(&rel.target_table) {
                        let rel_bn = rd.block_number_column.as_deref().unwrap_or("block_number");
                        let rel_qn = rd.query_name.as_deref().unwrap_or(&rel.target_table);

                        let rel_grouped = rd
                            .field_groups
                            .as_ref()
                            .map(|fg| build_grouped_writers(&rel.output_columns, rd, fg));
                        let rel_sort_columns = build_full_sort_columns(rd);
                        let rel_sort_resolved =
                            resolve_sort_columns(rel_batches, &rel_sort_columns);
                        all_indexes.push(IndexedBatches {
                            batches: rel_batches,
                            index: build_block_index(rel_batches, rel_bn),
                            table_desc: rd,
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
    // Build a map from query_name → position in metadata.tables.
    let query_name_order: HashMap<&str, usize> = metadata
        .tables
        .iter()
        .enumerate()
        .map(|(pos, (name, desc))| {
            let qn = desc.query_name.as_deref().unwrap_or(name.as_str());
            (qn, pos)
        })
        .collect();
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
        .and_then(|d| d.block_number_column.as_deref())
        .unwrap_or("number");
    let mut bn_key_prefix = Vec::new();
    encode_json_string(&snake_to_camel(bn_col), &mut bn_key_prefix);
    bn_key_prefix.push(b':');

    // Pre-resolve column indices for header writers (once per batch schema)
    let header_resolved: Vec<Vec<ResolvedFieldWriter>> = block_batches
        .iter()
        .map(|b| resolve_writers(&header_writers, b))
        .collect();

    // Pre-resolve column indices for each source (once per batch schema)
    let all_resolved: Vec<Vec<Vec<ResolvedFieldWriter>>> = all_indexes
        .iter()
        .map(|idx| {
            idx.batches
                .iter()
                .map(|b| resolve_writers(&idx.writers, b))
                .collect()
        })
        .collect();

    // Pre-resolve grouped writers
    let all_grouped_resolved: Vec<Option<Vec<ResolvedGroupedWriters>>> = all_indexes
        .iter()
        .map(|idx| {
            idx.grouped.as_ref().map(|gw| {
                idx.batches
                    .iter()
                    .map(|b| resolve_grouped_writers(gw, b))
                    .collect()
            })
        })
        .collect();

    elapsed!(
        t_blocks,
        "blocks + indexing",
        "{} blocks",
        selected_blocks.len()
    );

    // 6. Write each block as JSON (parallel generation, sequential output)
    let t_json = timer!();
    let block_jsons: Vec<Vec<u8>> = selected_blocks
        .par_iter()
        .map(|&block_num| {
            let mut block_buf = Vec::with_capacity(4 * 1024);
            block_buf.push(b'{');

            // Write header
            write_header(
                &mut block_buf,
                block_num,
                &block_batches,
                &block_index,
                block_table_desc,
                &header_writers,
                &bn_key_prefix,
                &header_resolved,
            );

            // Write table items, merging multiple sources for the same output table
            for table_name in &table_group_order {
                let source_indices = &table_groups[table_name];
                let json_prefix = &table_json_prefixes[table_name];

                write_merged_table_items(
                    &mut block_buf,
                    block_num,
                    &all_indexes,
                    source_indices,
                    &all_resolved,
                    &all_grouped_resolved,
                    json_prefix,
                );
            }

            json_close(b'}', &mut block_buf);
            block_buf
        })
        .collect();

    for block_buf in &block_jsons {
        json_writer.begin_item()?;
        json_writer.write_bytes(block_buf);
    }

    elapsed!(t_json, "json output");
    elapsed!(t_total, "TOTAL");

    Ok(json_writer.finish()?)
}

/// Resolve all physical columns needed for a table's output (including virtual field sources).
fn resolve_output_columns(table_plan: &TablePlan, table_desc: &TableDescription) -> Vec<String> {
    let mut cols: HashSet<String> = HashSet::new();

    // Block number column always needed for grouping
    if let Some(bn) = &table_desc.block_number_column {
        cols.insert(bn.clone());
    }

    // Sort columns: item_order_keys + address_column
    for key in &table_desc.item_order_keys {
        cols.insert(key.clone());
    }
    if let Some(ac) = &table_desc.address_column {
        cols.insert(ac.clone());
    }

    // Tag column for field groups (needed for variant dispatch)
    if let Some(fg) = &table_desc.field_groups {
        cols.insert(fg.tag_column.clone());
    }

    // Requested output columns
    for col in &table_plan.output_columns {
        // Check if this is a virtual field
        if let Some(vf) = table_desc.virtual_fields.get(col.as_str()) {
            match vf {
                VirtualField::Roll { columns } => {
                    for c in columns {
                        cols.insert(c.clone());
                    }
                }
            }
        } else if table_desc.columns.contains_key(col.as_str()) {
            cols.insert(col.clone());
        }
    }

    // Join key columns (needed for relations)
    for rel in &table_plan.relations {
        for key in &rel.left_key {
            cols.insert(key.clone());
        }
        // Source predicate columns (needed for post-hoc filtering of relation sources)
        if let Some(preds) = &rel.source_predicates {
            for pred in preds {
                for col_pred in &pred.columns {
                    cols.insert(col_pred.column.clone());
                }
            }
        }
    }

    // Weight columns (needed for response size limiting)
    for (data_col, weight_src) in &table_desc.weight_columns {
        if cols.contains(data_col) {
            if let crate::metadata::WeightSource::Column(wc) = weight_src {
                cols.insert(wc.clone());
            }
        }
    }

    cols.into_iter().collect()
}

/// Resolve output columns for a relation target table.
fn resolve_relation_output_columns(
    output_columns: &[String],
    table_desc: Option<&TableDescription>,
) -> Vec<String> {
    let mut cols: HashSet<String> = HashSet::new();

    if let Some(desc) = table_desc {
        // Block number column
        if let Some(bn) = &desc.block_number_column {
            cols.insert(bn.clone());
        }
        // Sort columns: item_order_keys + address_column
        for key in &desc.item_order_keys {
            cols.insert(key.clone());
        }
        if let Some(ac) = &desc.address_column {
            cols.insert(ac.clone());
        }
        // Tag column for field groups
        if let Some(fg) = &desc.field_groups {
            cols.insert(fg.tag_column.clone());
        }
    }

    for col in output_columns {
        if let Some(desc) = table_desc {
            if let Some(vf) = desc.virtual_fields.get(col.as_str()) {
                match vf {
                    VirtualField::Roll { columns } => {
                        for c in columns {
                            cols.insert(c.clone());
                        }
                    }
                }
            } else {
                cols.insert(col.clone());
            }
        } else {
            cols.insert(col.clone());
        }
    }

    // Weight columns (needed for response size limiting)
    if let Some(desc) = table_desc {
        for (data_col, weight_src) in &desc.weight_columns {
            if cols.contains(data_col) {
                if let crate::metadata::WeightSource::Column(wc) = weight_src {
                    cols.insert(wc.clone());
                }
            }
        }
    }

    cols.into_iter().collect()
}

/// Get the hierarchical address column from table metadata.
fn find_address_column(desc: &TableDescription) -> Option<&str> {
    desc.address_column.as_deref()
}

/// Extract group keys from a relation's key list by removing the address column.
fn group_keys_for_relation<'a>(keys: &'a [String], addr_col: Option<&str>) -> Vec<&'a str> {
    keys.iter()
        .map(String::as_str)
        .filter(|k| Some(*k) != addr_col)
        .collect()
}

/// Compute the actual min/max block numbers from scan results (for cross-table pruning).
fn compute_block_range(batches: &[RecordBatch], bn_column: &str) -> (Option<u64>, Option<u64>) {
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;

    macro_rules! update_range {
        ($arr:expr) => {
            for i in 0..$arr.len() {
                let bn = $arr.value(i) as u64;
                min_block = Some(min_block.map_or(bn, |m: u64| m.min(bn)));
                max_block = Some(max_block.map_or(bn, |m: u64| m.max(bn)));
            }
        };
    }

    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                update_range!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                update_range!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                update_range!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                update_range!(a);
            }
        }
    }
    (min_block, max_block)
}

/// Build an index mapping block_number → list of (batch_index, row_index).
fn build_block_index(
    batches: &[RecordBatch],
    bn_column: &str,
) -> HashMap<u64, Vec<(usize, usize)>> {
    let mut index: HashMap<u64, Vec<(usize, usize)>> = HashMap::new();
    for (batch_idx, batch) in batches.iter().enumerate() {
        if let Some(col) = batch.column_by_name(bn_column) {
            // Type-check once per batch, then iterate with the specific type
            if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                for row in 0..a.len() {
                    index
                        .entry(a.value(row))
                        .or_default()
                        .push((batch_idx, row));
                }
            } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                for row in 0..a.len() {
                    index
                        .entry(a.value(row) as u64)
                        .or_default()
                        .push((batch_idx, row));
                }
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                for row in 0..a.len() {
                    index
                        .entry(a.value(row) as u64)
                        .or_default()
                        .push((batch_idx, row));
                }
            } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                for row in 0..a.len() {
                    index
                        .entry(a.value(row) as u64)
                        .or_default()
                        .push((batch_idx, row));
                }
            }
        }
    }
    index
}

/// Collect block numbers from batches into a set.
fn collect_block_numbers(
    batches: &[RecordBatch],
    bn_column: &str,
    block_numbers: &mut HashSet<u64>,
) {
    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                for i in 0..a.len() {
                    block_numbers.insert(a.value(i));
                }
            } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                for i in 0..a.len() {
                    block_numbers.insert(a.value(i) as u64);
                }
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..a.len() {
                    block_numbers.insert(a.value(i) as u64);
                }
            } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..a.len() {
                    block_numbers.insert(a.value(i) as u64);
                }
            }
        }
    }
}

/// Collect only the first and last block numbers from the blocks table (boundary blocks).
fn collect_boundary_blocks(
    batches: &[RecordBatch],
    bn_column: &str,
    block_numbers: &mut HashSet<u64>,
) {
    let mut min_block: Option<u64> = None;
    let mut max_block: Option<u64> = None;

    macro_rules! update {
        ($arr:expr) => {
            for i in 0..$arr.len() {
                let v = $arr.value(i) as u64;
                min_block = Some(min_block.map_or(v, |m: u64| m.min(v)));
                max_block = Some(max_block.map_or(v, |m: u64| m.max(v)));
            }
        };
    }

    for batch in batches {
        if let Some(col) = batch.column_by_name(bn_column) {
            if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                update!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                update!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                update!(a);
            } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                update!(a);
            }
        }
    }

    if let Some(v) = min_block {
        block_numbers.insert(v);
    }
    if let Some(v) = max_block {
        block_numbers.insert(v);
    }
}

/// Apply weight-based limit: select blocks until cumulative weight exceeds MAX_RESPONSE_BYTES.
fn apply_weight_limit(
    sorted_blocks: &[u64],
    table_outputs: &HashMap<String, TableOutput>,
    block_batches: &[RecordBatch],
    metadata: &DatasetDescription,
    plan: &Plan,
) -> Vec<u64> {
    if sorted_blocks.is_empty() {
        return Vec::new();
    }

    // Pre-build per-block weight in a single pass over all rows
    let mut block_weights: HashMap<u64, u64> = HashMap::new();

    for table_plan in &plan.table_plans {
        let output = match table_outputs.get(&table_plan.table) {
            Some(o) => o,
            None => continue,
        };
        let table_desc = metadata.table(&table_plan.table);
        let bn_col = table_desc
            .and_then(|d| d.block_number_column.as_deref())
            .unwrap_or("block_number");

        // Compute weight from the full resolved projection
        let resolved_cols = resolve_output_columns(table_plan, table_desc.unwrap());
        let (fixed_weight, weight_cols) = compute_weight_params(&resolved_cols, table_desc);

        accumulate_block_weights(
            &output.batches,
            bn_col,
            fixed_weight,
            &weight_cols,
            &mut block_weights,
        );

        for rel in &table_plan.relations {
            if let Some(rel_batches) = output.relation_batches.get(&rel.target_table) {
                let rel_desc = metadata.table(&rel.target_table);
                let rel_bn = rel_desc
                    .and_then(|d| d.block_number_column.as_deref())
                    .unwrap_or("block_number");

                let rel_resolved = resolve_relation_output_columns(&rel.output_columns, rel_desc);
                let (rel_fixed, rel_weight_cols) = compute_weight_params(&rel_resolved, rel_desc);

                accumulate_block_weights(
                    rel_batches,
                    rel_bn,
                    rel_fixed,
                    &rel_weight_cols,
                    &mut block_weights,
                );
            }
        }
    }

    // Add block header weight for blocks that have items.
    // In legacy: when include_all_blocks == false, first/last boundary blocks get weight 0,
    // other blocks get header weight added.
    let block_desc = metadata.table(&plan.block_table);
    let (header_fixed, header_weight_cols) =
        compute_weight_params(&plan.block_output_columns, block_desc);
    let header_bn_col = block_desc
        .and_then(|d| d.block_number_column.as_deref())
        .unwrap_or("number");

    if plan.include_all_blocks {
        // All blocks get header weight
        accumulate_block_weights(
            block_batches,
            header_bn_col,
            header_fixed,
            &header_weight_cols,
            &mut block_weights,
        );
    } else {
        // Only blocks that have item data get header weight added
        // (boundary blocks without items get 0 weight)
        let blocks_with_items: HashSet<u64> = block_weights.keys().copied().collect();
        for batch in block_batches {
            let bn_col = match batch.column_by_name(header_bn_col) {
                Some(c) => c,
                None => continue,
            };
            for i in 0..batch.num_rows() {
                if let Some(block_num) = get_block_number(bn_col.as_ref(), i) {
                    if blocks_with_items.contains(&block_num) {
                        *block_weights.entry(block_num).or_default() += header_fixed;
                    }
                }
            }
        }
    }

    let mut cumulative_weight: u64 = 0;
    let mut selected = Vec::new();

    for &block_num in sorted_blocks {
        let block_weight = block_weights.get(&block_num).copied().unwrap_or(0);
        cumulative_weight += block_weight;

        if selected.is_empty() || cumulative_weight <= MAX_RESPONSE_BYTES {
            selected.push(block_num);
        } else {
            break;
        }
    }

    selected
}

/// Compute fixed weight per row and list of weight columns for weight limiting.
/// This matches legacy behavior: every projected column contributes either a fixed weight
/// (default 32) or a dynamic weight column. Weight/size columns themselves do NOT add weight.
fn compute_weight_params(
    output_columns: &[String],
    table_desc: Option<&TableDescription>,
) -> (u64, Vec<String>) {
    let desc = match table_desc {
        Some(d) => d,
        None => return (DEFAULT_ROW_WEIGHT, Vec::new()),
    };

    // Collect the set of weight/size column names so we can exclude them
    let weight_col_names: HashSet<&str> = desc
        .weight_columns
        .values()
        .filter_map(|ws| match ws {
            crate::metadata::WeightSource::Column(wc) => Some(wc.as_str()),
            _ => None,
        })
        .collect();

    // Collect all physical columns in the projection
    let mut projected: HashSet<&str> = HashSet::new();
    for col_name in output_columns {
        if let Some(vf) = desc.virtual_fields.get(col_name.as_str()) {
            match vf {
                VirtualField::Roll { columns } => {
                    for c in columns {
                        projected.insert(c.as_str());
                    }
                }
            }
        } else if desc.columns.contains_key(col_name.as_str()) {
            projected.insert(col_name.as_str());
        }
    }

    let mut fixed_weight: u64 = 0;
    let mut weight_cols = Vec::new();

    for &col_name in &projected {
        // Skip weight/size columns themselves — they don't contribute to output weight
        if weight_col_names.contains(col_name) {
            continue;
        }
        if let Some(weight_src) = desc.weight_columns.get(col_name) {
            match weight_src {
                crate::metadata::WeightSource::Column(wc) => {
                    weight_cols.push(wc.clone());
                }
                crate::metadata::WeightSource::Fixed(w) => {
                    fixed_weight += w;
                }
            }
        } else {
            fixed_weight += DEFAULT_ROW_WEIGHT;
        }
    }

    (fixed_weight, weight_cols)
}

/// Get block number from an array at row index.
fn get_block_number(col: &dyn arrow::array::Array, i: usize) -> Option<u64> {
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        Some(a.value(i))
    } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        Some(a.value(i) as u64)
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        Some(a.value(i) as u64)
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        Some(a.value(i) as u64)
    } else {
        None
    }
}

/// Get uint64 value from a weight column at row index.
fn get_weight_value(col: &dyn arrow::array::Array, i: usize) -> u64 {
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        a.value(i)
    } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        a.value(i) as u64
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        a.value(i) as u64
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        a.value(i) as u64
    } else {
        0
    }
}

/// Accumulate per-block weights in a single pass over all rows.
fn accumulate_block_weights(
    batches: &[RecordBatch],
    bn_column: &str,
    fixed_weight_per_row: u64,
    weight_columns: &[String],
    weights: &mut HashMap<u64, u64>,
) {
    for batch in batches {
        let bn_col = match batch.column_by_name(bn_column) {
            Some(c) => c,
            None => continue,
        };

        // Resolve weight columns for this batch
        let wc_arrays: Vec<Option<&dyn arrow::array::Array>> = weight_columns
            .iter()
            .map(|name| batch.column_by_name(name).map(|c| c.as_ref()))
            .collect();

        for i in 0..batch.num_rows() {
            if let Some(block_num) = get_block_number(bn_col.as_ref(), i) {
                let mut row_weight = fixed_weight_per_row;
                for wc in &wc_arrays {
                    if let Some(arr) = wc {
                        row_weight += get_weight_value(*arr, i);
                    }
                }
                *weights.entry(block_num).or_default() += row_weight;
            }
        }
    }
}

/// Write a block header JSON.
fn write_header(
    buf: &mut Vec<u8>,
    block_num: u64,
    block_batches: &[RecordBatch],
    block_index: &HashMap<u64, Vec<(usize, usize)>>,
    _block_desc: Option<&TableDescription>,
    _writers: &[FieldWriter],
    bn_key_prefix: &[u8],
    resolved_by_batch: &[Vec<ResolvedFieldWriter>],
) {
    buf.extend_from_slice(b"\"header\":{");

    let mut found = false;
    if let Some(rows) = block_index.get(&block_num) {
        if let Some(&(batch_idx, row)) = rows.first() {
            write_row_fields_resolved(
                buf,
                &block_batches[batch_idx],
                row,
                &resolved_by_batch[batch_idx],
            );
            found = true;
        }
    }

    if !found {
        buf.extend_from_slice(bn_key_prefix);
        let mut tmp = itoa::Buffer::new();
        buf.extend_from_slice(tmp.format(block_num).as_bytes());
    }

    json_close(b'}', buf);
    buf.push(b',');
}

/// Write items from a table for a specific block, using pre-built index.
fn write_table_items_indexed(
    buf: &mut Vec<u8>,
    block_num: u64,
    batches: &[RecordBatch],
    block_index: &HashMap<u64, Vec<(usize, usize)>>,
    sort_columns: &[String],
    sort_col_resolved: &[Vec<Option<usize>>],
    json_array_prefix: &[u8],
    resolved_by_batch: &[Vec<ResolvedFieldWriter>],
    grouped_resolved: Option<&[ResolvedGroupedWriters]>,
) {
    let row_refs = match block_index.get(&block_num) {
        Some(refs) if !refs.is_empty() => refs,
        _ => return,
    };

    // Build sortable rows (with batch_idx for resolved writer lookup)
    let mut rows: Vec<(usize, usize)> = row_refs.to_vec();

    // Sort by full sort columns (item_order_keys + address)
    sort_rows_by_order_keys_indexed(&mut rows, batches, sort_columns, sort_col_resolved);

    // Write array
    buf.extend_from_slice(json_array_prefix);

    for (i, &(batch_idx, row)) in rows.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        buf.push(b'{');
        if let Some(gr) = grouped_resolved {
            write_row_grouped(buf, &batches[batch_idx], row, &gr[batch_idx]);
        } else {
            write_row_fields_resolved(buf, &batches[batch_idx], row, &resolved_by_batch[batch_idx]);
        }
        json_close(b'}', buf);
    }

    buf.push(b']');
    buf.push(b',');
}

/// Write items from multiple sources for the same output table, merging into a single JSON array.
/// Rows are collected from all sources, deduplicated by (item_order_keys), sorted, and written.
fn write_merged_table_items(
    buf: &mut Vec<u8>,
    block_num: u64,
    all_indexes: &[IndexedBatches],
    source_indices: &[usize],
    all_resolved: &[Vec<Vec<ResolvedFieldWriter>>],
    all_grouped_resolved: &[Option<Vec<ResolvedGroupedWriters>>],
    json_array_prefix: &[u8],
) {
    // If single source, use optimized path
    if source_indices.len() == 1 {
        let si = source_indices[0];
        let idx = &all_indexes[si];
        write_table_items_indexed(
            buf,
            block_num,
            idx.batches,
            &idx.index,
            &idx.sort_columns,
            &idx.sort_col_resolved,
            json_array_prefix,
            &all_resolved[si],
            all_grouped_resolved[si].as_deref(),
        );
        return;
    }

    // Collect rows from all sources: (source_idx, batch_idx, row_idx)
    let mut rows: Vec<(usize, usize, usize)> = Vec::new();
    for &si in source_indices {
        let idx = &all_indexes[si];
        if let Some(refs) = idx.index.get(&block_num) {
            for &(bi, ri) in refs {
                rows.push((si, bi, ri));
            }
        }
    }

    if rows.is_empty() {
        return;
    }

    // Sort by item_order_keys (use first source's sort_columns, pre-resolved)
    let first = &all_indexes[source_indices[0]];
    let sort_columns = &first.sort_columns;

    if !sort_columns.is_empty() {
        let dedup_key_count = sort_columns.len();

        rows.sort_by(|a, b| {
            let res_a = &all_indexes[a.0].sort_col_resolved[a.1];
            let res_b = &all_indexes[b.0].sort_col_resolved[b.1];
            let batch_a = &all_indexes[a.0].batches[a.1];
            let batch_b = &all_indexes[b.0].batches[b.1];
            for (idx_a, idx_b) in res_a.iter().zip(res_b.iter()) {
                if let (Some(ia), Some(ib)) = (idx_a, idx_b) {
                    let ord = compare_array_values(
                        batch_a.column(*ia).as_ref(),
                        a.2,
                        batch_b.column(*ib).as_ref(),
                        b.2,
                    );
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
            }
            // Final tiebreaker: source index, then batch index, then row index
            (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2))
        });

        // Deduplicate rows from different sources with identical order keys
        rows.dedup_by(|b, a| {
            if a.0 == b.0 {
                return false;
            }
            let res_a = &all_indexes[a.0].sort_col_resolved[a.1];
            let res_b = &all_indexes[b.0].sort_col_resolved[b.1];
            let batch_a = &all_indexes[a.0].batches[a.1];
            let batch_b = &all_indexes[b.0].batches[b.1];
            for (idx_a, idx_b) in res_a.iter().zip(res_b.iter()).take(dedup_key_count) {
                if let (Some(ia), Some(ib)) = (idx_a, idx_b) {
                    if compare_array_values(
                        batch_a.column(*ia).as_ref(),
                        a.2,
                        batch_b.column(*ib).as_ref(),
                        b.2,
                    ) != std::cmp::Ordering::Equal
                    {
                        return false;
                    }
                }
            }
            true
        });
    }

    // Write array
    buf.extend_from_slice(json_array_prefix);

    for (i, &(si, batch_idx, row)) in rows.iter().enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        buf.push(b'{');
        if let Some(Some(gr)) = all_grouped_resolved.get(si) {
            write_row_grouped(
                buf,
                &all_indexes[si].batches[batch_idx],
                row,
                &gr[batch_idx],
            );
        } else {
            write_row_fields_resolved(
                buf,
                &all_indexes[si].batches[batch_idx],
                row,
                &all_resolved[si][batch_idx],
            );
        }
        json_close(b'}', buf);
    }

    buf.push(b']');
    buf.push(b',');
}

/// Pre-computed information for writing a single output column.
enum FieldWriter {
    /// Virtual field: roll columns together.
    Roll {
        json_key_prefix: Vec<u8>,
        source_column_names: Vec<String>,
    },
    /// Regular column with optional encoding override.
    Regular {
        json_key_prefix: Vec<u8>,
        column_name: String,
        encoding: Option<OutputEncoding>,
    },
}

/// FieldWriter with column indices resolved for a specific batch schema.
struct ResolvedFieldWriter<'a> {
    writer: &'a FieldWriter,
    /// Resolved column index for Regular, or resolved indices for Roll.
    indices: ResolvedIndices,
}

enum ResolvedIndices {
    /// Single column index for Regular fields.
    Single(Option<usize>),
    /// Multiple column indices for Roll fields.
    Multi(Vec<usize>),
}

/// Resolve field writers against a specific batch schema (done once per batch).
fn resolve_writers<'a>(
    writers: &'a [FieldWriter],
    batch: &RecordBatch,
) -> Vec<ResolvedFieldWriter<'a>> {
    writers
        .iter()
        .map(|w| {
            let indices = match w {
                FieldWriter::Roll {
                    source_column_names,
                    ..
                } => {
                    let idxs: Vec<usize> = source_column_names
                        .iter()
                        .filter_map(|c| batch.schema().index_of(c).ok())
                        .collect();
                    ResolvedIndices::Multi(idxs)
                }
                FieldWriter::Regular { column_name, .. } => {
                    ResolvedIndices::Single(batch.schema().index_of(column_name).ok())
                }
            };
            ResolvedFieldWriter { writer: w, indices }
        })
        .collect()
}

/// Pre-compute the JSON key prefixes and column resolution for a table's output columns.
fn build_field_writers(
    output_columns: &[String],
    table_desc: Option<&TableDescription>,
) -> Vec<FieldWriter> {
    output_columns
        .iter()
        .map(|col_name| {
            // Check virtual fields first
            if let Some(desc) = table_desc {
                if let Some(vf) = desc.virtual_fields.get(col_name) {
                    match vf {
                        VirtualField::Roll { columns } => {
                            let mut prefix = Vec::with_capacity(col_name.len() + 4);
                            encode_json_string(&snake_to_camel(col_name), &mut prefix);
                            prefix.push(b':');
                            return FieldWriter::Roll {
                                json_key_prefix: prefix,
                                source_column_names: columns.clone(),
                            };
                        }
                    }
                }
            }

            let mut prefix = Vec::with_capacity(col_name.len() + 4);
            encode_json_string(&snake_to_camel(col_name), &mut prefix);
            prefix.push(b':');

            let encoding = table_desc
                .and_then(|d| d.output_encodings.get(col_name))
                .cloned();

            FieldWriter::Regular {
                json_key_prefix: prefix,
                column_name: col_name.clone(),
                encoding,
            }
        })
        .collect()
}

/// Write all fields for a single row as JSON key-value pairs using resolved writers.
fn write_row_fields_resolved(
    buf: &mut Vec<u8>,
    batch: &RecordBatch,
    row: usize,
    resolved: &[ResolvedFieldWriter],
) {
    for rw in resolved {
        match (&rw.writer, &rw.indices) {
            (
                FieldWriter::Roll {
                    json_key_prefix, ..
                },
                ResolvedIndices::Multi(indices),
            ) => {
                if !indices.is_empty() {
                    buf.extend_from_slice(json_key_prefix);
                    encode_roll(batch, row, indices, buf);
                    buf.push(b',');
                }
            }
            (
                FieldWriter::Regular {
                    json_key_prefix,
                    encoding,
                    ..
                },
                ResolvedIndices::Single(Some(idx)),
            ) => {
                let col = batch.column(*idx);
                buf.extend_from_slice(json_key_prefix);
                match encoding {
                    Some(OutputEncoding::Bignum) => encode_bignum(col.as_ref(), row, buf),
                    Some(OutputEncoding::Json) => encode_json_passthrough(col.as_ref(), row, buf),
                    Some(OutputEncoding::SolanaTxVersion) => {
                        encode_solana_tx_version(col.as_ref(), row, buf)
                    }
                    _ => encode_value(col.as_ref(), row, buf),
                }
                buf.push(b',');
            }
            _ => {}
        }
    }
}

/// Pre-computed structure for polymorphic field grouping (e.g., EVM traces).
struct GroupedWriters {
    /// Writers for base fields (shared across all variants).
    base_writers: Vec<FieldWriter>,
    /// Tag column name for variant dispatch.
    tag_column: String,
    /// Per-variant grouped writers: tag_value → [(group_json_key, writers)].
    variant_writers: HashMap<String, Vec<(Vec<u8>, Vec<FieldWriter>)>>,
}

/// Resolved grouped writers for a specific batch schema.
struct ResolvedGroupedWriters<'a> {
    base_resolved: Vec<ResolvedFieldWriter<'a>>,
    tag_col_idx: Option<usize>,
    variant_resolved: HashMap<&'a str, Vec<(&'a [u8], Vec<ResolvedFieldWriter<'a>>)>>,
}

fn build_grouped_writers(
    output_columns: &[String],
    table_desc: &TableDescription,
    grouping: &FieldGrouping,
) -> GroupedWriters {
    let base_set: HashSet<&str> = grouping.base_fields.iter().map(|s| s.as_str()).collect();

    // Build a reverse map: physical_column → (variant, group, field_name)
    let mut col_to_group: HashMap<&str, (&str, &str, &str)> = HashMap::new();
    for (variant_name, groups) in &grouping.variants {
        for (group_name, mappings) in groups {
            for mapping in mappings {
                col_to_group.insert(
                    mapping.column.as_str(),
                    (
                        variant_name.as_str(),
                        group_name.as_str(),
                        mapping.field.as_str(),
                    ),
                );
            }
        }
    }

    // Separate output columns into base vs grouped
    let mut base_writers = Vec::new();
    // variant → group → Vec<FieldWriter>
    let mut variant_groups: HashMap<String, HashMap<String, Vec<FieldWriter>>> = HashMap::new();

    for col_name in output_columns {
        if base_set.contains(col_name.as_str()) {
            // Base field — write flat
            if let Some(vf) = table_desc.virtual_fields.get(col_name) {
                match vf {
                    VirtualField::Roll { columns } => {
                        let mut prefix = Vec::with_capacity(col_name.len() + 4);
                        encode_json_string(&snake_to_camel(col_name), &mut prefix);
                        prefix.push(b':');
                        base_writers.push(FieldWriter::Roll {
                            json_key_prefix: prefix,
                            source_column_names: columns.clone(),
                        });
                        continue;
                    }
                }
            }
            let mut prefix = Vec::with_capacity(col_name.len() + 4);
            encode_json_string(&snake_to_camel(col_name), &mut prefix);
            prefix.push(b':');
            let encoding = table_desc.output_encodings.get(col_name).cloned();
            base_writers.push(FieldWriter::Regular {
                json_key_prefix: prefix,
                column_name: col_name.clone(),
                encoding,
            });
        } else if let Some(&(variant, group, field_name)) = col_to_group.get(col_name.as_str()) {
            // Grouped field
            let mut prefix = Vec::with_capacity(field_name.len() + 4);
            encode_json_string(field_name, &mut prefix);
            prefix.push(b':');
            let encoding = table_desc.output_encodings.get(col_name).cloned();
            variant_groups
                .entry(variant.to_string())
                .or_default()
                .entry(group.to_string())
                .or_default()
                .push(FieldWriter::Regular {
                    json_key_prefix: prefix,
                    column_name: col_name.clone(),
                    encoding,
                });
        }
        // else: column not in base or any group — skip (e.g., weight columns)
    }

    // Convert variant_groups into the final structure
    let mut variant_writers: HashMap<String, Vec<(Vec<u8>, Vec<FieldWriter>)>> = HashMap::new();
    for (variant, groups) in variant_groups {
        let mut group_list = Vec::new();
        for (group_name, writers) in groups {
            let mut key = Vec::with_capacity(group_name.len() + 4);
            encode_json_string(&group_name, &mut key);
            key.push(b':');
            group_list.push((key, writers));
        }
        variant_writers.insert(variant, group_list);
    }

    GroupedWriters {
        base_writers,
        tag_column: grouping.tag_column.clone(),
        variant_writers,
    }
}

fn resolve_grouped_writers<'a>(
    gw: &'a GroupedWriters,
    batch: &RecordBatch,
) -> ResolvedGroupedWriters<'a> {
    let base_resolved = resolve_writers(&gw.base_writers, batch);
    let tag_col_idx = batch.schema().index_of(&gw.tag_column).ok();
    let mut variant_resolved: HashMap<&'a str, Vec<(&'a [u8], Vec<ResolvedFieldWriter<'a>>)>> =
        HashMap::new();
    for (variant, groups) in &gw.variant_writers {
        let resolved_groups: Vec<_> = groups
            .iter()
            .map(|(key, writers)| {
                let resolved = resolve_writers(writers, batch);
                (key.as_slice(), resolved)
            })
            .collect();
        variant_resolved.insert(variant.as_str(), resolved_groups);
    }
    ResolvedGroupedWriters {
        base_resolved,
        tag_col_idx,
        variant_resolved,
    }
}

fn write_row_grouped(
    buf: &mut Vec<u8>,
    batch: &RecordBatch,
    row: usize,
    resolved: &ResolvedGroupedWriters,
) {
    // Write base fields
    write_row_fields_resolved(buf, batch, row, &resolved.base_resolved);

    // Read tag column to determine variant
    let tag_value = resolved.tag_col_idx.and_then(|idx| {
        let col = batch.column(idx);
        col.as_any().downcast_ref::<StringArray>().and_then(|arr| {
            if arr.is_null(row) {
                None
            } else {
                Some(arr.value(row))
            }
        })
    });

    if let Some(tag) = tag_value {
        if let Some(groups) = resolved.variant_resolved.get(tag) {
            for (group_key, writers) in groups {
                // Emit group if it has any selected fields (matching legacy behavior:
                // group is emitted when user selected at least one field, even if all null)
                if !writers.is_empty() {
                    buf.extend_from_slice(group_key);
                    buf.push(b'{');
                    write_row_fields_resolved(buf, batch, row, writers);
                    json_close(b'}', buf);
                    buf.push(b',');
                }
            }
        }
    }
}

/// Build the full sort column list for a table: item_order_keys + address_column.
/// These match the legacy engine's primary key (minus block_number) for output ordering.
fn build_full_sort_columns(table_desc: &TableDescription) -> Vec<String> {
    let bn_col = table_desc
        .block_number_column
        .as_deref()
        .unwrap_or("block_number");

    let mut used: HashSet<&str> = HashSet::new();
    used.insert(bn_col);

    let mut cols: Vec<String> = Vec::new();

    // Primary sort: item_order_keys
    for key in &table_desc.item_order_keys {
        if used.insert(key.as_str()) {
            cols.push(key.clone());
        }
    }

    // Secondary: address column
    if let Some(ac) = table_desc.address_column.as_deref() {
        if used.insert(ac) {
            cols.push(ac.to_string());
        }
    }

    cols
}

/// Pre-resolve sort column indices for each batch (done once, reused per block).
fn resolve_sort_columns(
    batches: &[RecordBatch],
    sort_columns: &[String],
) -> Vec<Vec<Option<usize>>> {
    batches
        .iter()
        .map(|b| {
            sort_columns
                .iter()
                .map(|k| b.schema().index_of(k).ok())
                .collect()
        })
        .collect()
}

/// Sort (batch_idx, row) references by the full sort columns.
/// Uses (batch_idx, row_idx) as final tiebreaker to preserve parquet file order.
fn sort_rows_by_order_keys_indexed(
    rows: &mut Vec<(usize, usize)>,
    batches: &[RecordBatch],
    sort_columns: &[String],
    pre_resolved: &[Vec<Option<usize>>],
) {
    if rows.is_empty() {
        return;
    }
    if sort_columns.is_empty() {
        // Even with no sort columns, sort by (batch, row) for deterministic order
        rows.sort_by_key(|&(bi, ri)| (bi, ri));
        return;
    }

    rows.sort_by(|&(bi_a, row_a), &(bi_b, row_b)| {
        let batch_a = &batches[bi_a];
        let batch_b = &batches[bi_b];
        let res_a = &pre_resolved[bi_a];
        let res_b = &pre_resolved[bi_b];
        for (idx_a, idx_b) in res_a.iter().zip(res_b.iter()) {
            if let (Some(ia), Some(ib)) = (idx_a, idx_b) {
                let ord = compare_array_values(
                    batch_a.column(*ia).as_ref(),
                    row_a,
                    batch_b.column(*ib).as_ref(),
                    row_b,
                );
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
        }
        // Final tiebreaker: parquet file order (batch index, then row index)
        (bi_a, row_a).cmp(&(bi_b, row_b))
    });
}

/// Compare two values from arrays for sorting.
fn compare_array_values(
    a: &dyn Array,
    row_a: usize,
    b: &dyn Array,
    row_b: usize,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<UInt64Array>(),
        b.as_any().downcast_ref::<UInt64Array>(),
    ) {
        return aa.value(row_a).cmp(&bb.value(row_b));
    }
    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<UInt32Array>(),
        b.as_any().downcast_ref::<UInt32Array>(),
    ) {
        return aa.value(row_a).cmp(&bb.value(row_b));
    }
    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<UInt16Array>(),
        b.as_any().downcast_ref::<UInt16Array>(),
    ) {
        return aa.value(row_a).cmp(&bb.value(row_b));
    }
    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<Int64Array>(),
        b.as_any().downcast_ref::<Int64Array>(),
    ) {
        return aa.value(row_a).cmp(&bb.value(row_b));
    }
    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<Int32Array>(),
        b.as_any().downcast_ref::<Int32Array>(),
    ) {
        return aa.value(row_a).cmp(&bb.value(row_b));
    }
    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<Int16Array>(),
        b.as_any().downcast_ref::<Int16Array>(),
    ) {
        return aa.value(row_a).cmp(&bb.value(row_b));
    }
    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<StringArray>(),
        b.as_any().downcast_ref::<StringArray>(),
    ) {
        return aa.value(row_a).cmp(bb.value(row_b));
    }
    // List<UInt32> comparison (instruction_address, trace_address)
    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<GenericListArray<i32>>(),
        b.as_any().downcast_ref::<GenericListArray<i32>>(),
    ) {
        return compare_list_values(aa, row_a, bb, row_b);
    }
    Ordering::Equal
}

/// Compare two List<UInt32/Int32> arrays element by element.
fn compare_list_values(
    a: &GenericListArray<i32>,
    row_a: usize,
    b: &GenericListArray<i32>,
    row_b: usize,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    let va = a.value(row_a);
    let vb = b.value(row_b);
    let len = va.len().min(vb.len());

    for i in 0..len {
        let val_a = if let Some(arr) = va.as_any().downcast_ref::<UInt16Array>() {
            arr.value(i) as i64
        } else if let Some(arr) = va.as_any().downcast_ref::<UInt32Array>() {
            arr.value(i) as i64
        } else if let Some(arr) = va.as_any().downcast_ref::<Int32Array>() {
            arr.value(i) as i64
        } else {
            0
        };
        let val_b = if let Some(arr) = vb.as_any().downcast_ref::<UInt16Array>() {
            arr.value(i) as i64
        } else if let Some(arr) = vb.as_any().downcast_ref::<UInt32Array>() {
            arr.value(i) as i64
        } else if let Some(arr) = vb.as_any().downcast_ref::<Int32Array>() {
            arr.value(i) as i64
        } else {
            0
        };
        let ord = val_a.cmp(&val_b);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    va.len().cmp(&vb.len())
}

/// Replace trailing comma with closing bracket, or just add closing bracket.
#[inline]
fn json_close(end: u8, buf: &mut Vec<u8>) {
    if let Some(last) = buf.last() {
        if *last == b',' {
            let len = buf.len();
            buf[len - 1] = end;
            return;
        }
    }
    buf.push(end);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::load_dataset_description;
    use crate::query::compile;
    use crate::query::parse_query;

    fn solana_metadata() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/solana.yaml")).unwrap()
    }

    fn evm_metadata() -> DatasetDescription {
        load_dataset_description(Path::new("metadata/evm.yaml")).unwrap()
    }

    #[test]
    fn test_execute_solana_instructions() {
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

        let chunk_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/200");
        let output = Vec::new();
        let result = execute_plan(&plan, &meta, &chunk_dir, output).unwrap();

        let json_str = String::from_utf8(result).unwrap();
        assert!(json_str.starts_with('['));
        assert!(json_str.ends_with(']'));

        // Parse to validate JSON
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let blocks = parsed.as_array().unwrap();
        assert!(!blocks.is_empty(), "should have at least one block");

        // Each block should have a header
        for block in blocks {
            assert!(block.get("header").is_some(), "block should have header");
        }

        // At least one block should have instructions
        let has_instructions = blocks.iter().any(|b| b.get("instructions").is_some());
        assert!(has_instructions, "should have instructions in output");

        // Verify instruction fields are camelCase
        for block in blocks {
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
    fn test_execute_evm_logs() {
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

        let chunk_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/evm/large");
        let output = Vec::new();
        let result = execute_plan(&plan, &meta, &chunk_dir, output).unwrap();

        let json_str = String::from_utf8(result).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let blocks = parsed.as_array().unwrap();
        assert!(!blocks.is_empty());

        // Check topics is an array (virtual field via roll)
        for block in blocks {
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

    #[test]
    fn test_execute_with_relations() {
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

        let chunk_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/200");
        let output = Vec::new();
        let result = execute_plan(&plan, &meta, &chunk_dir, output).unwrap();

        let json_str = String::from_utf8(result).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let blocks = parsed.as_array().unwrap();

        // Should have both instructions and transactions
        let has_txs = blocks.iter().any(|b| b.get("transactions").is_some());
        assert!(has_txs, "should have related transactions");
    }

    #[test]
    fn test_execute_empty_result() {
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

        let chunk_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/200");
        let output = Vec::new();
        let result = execute_plan(&plan, &meta, &chunk_dir, output).unwrap();

        assert_eq!(String::from_utf8(result).unwrap(), "[]");
    }

    #[test]
    fn test_json_close() {
        let mut buf = vec![b'{', b'"', b'a', b'"', b':', b'1', b','];
        json_close(b'}', &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "{\"a\":1}");

        let mut buf = vec![b'{'];
        json_close(b'}', &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "{}");
    }

    #[test]
    fn test_snake_to_camel_in_output() {
        assert_eq!(snake_to_camel("log_index"), "logIndex");
        assert_eq!(snake_to_camel("transaction_hash"), "transactionHash");
        assert_eq!(snake_to_camel("number"), "number");
    }
}
