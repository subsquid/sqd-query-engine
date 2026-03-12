use crate::metadata::DatasetDescription;
use crate::output::block_index::{
    build_block_index, collect_block_numbers, collect_boundary_blocks, compute_block_range,
};
use crate::output::columns::{find_address_column, group_keys_for_relation, resolve_output_columns, resolve_relation_output_columns};
use crate::output::encoder::{encode_json_string, snake_to_camel};
use crate::output::row_writer::{
    build_field_writers, build_full_sort_columns, build_grouped_writers, json_close,
    resolve_grouped_writers, resolve_sort_columns, resolve_writers, write_header,
    write_merged_table_items, IndexedBatches,
};
use crate::output::weight::{apply_weight_limit, TableOutput};
use crate::output::writer::JsonArrayWriter;
use crate::query::{Plan, RelationKind};
use crate::scan::predicate::{evaluate_predicates_on_batch, RowPredicate};
use crate::scan::{
    scan, HierarchicalFilter, HierarchicalMode, KeyFilter, ParquetTable, ScanRequest,
};
use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;

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
        request.block_number_column = Some(table_desc.block_number_column.as_str());

        let t_primary = timer!();
        let batches = scan(&parquet_table, &request)?;
        let primary_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        elapsed!(t_primary, "primary scan", "{} rows", primary_rows);

        // Compute actual block range from primary scan for cross-table pruning
        let bn_col_name = table_desc.block_number_column.as_str();
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
                        rel_request.block_number_column =
                            Some(desc.block_number_column.as_str());
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
                                if let Some(target_addr) = find_address_column(desc) {
                                    let source_addr = find_address_column(table_desc)
                                        .unwrap_or(target_addr);
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
                                    let source_addr = find_address_column(table_desc)
                                        .unwrap_or(target_addr);
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
        let bn_col = block_desc.block_number_column.as_str();
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
        let bn_col = table_desc.block_number_column.as_str();
        collect_block_numbers(&output.batches, bn_col, &mut block_numbers);
        for rel_batches in output.relation_batches.values() {
            // Relations may be from different tables with different bn columns
            collect_block_numbers(rel_batches, "block_number", &mut block_numbers);
        }
    }

    // Always include boundary blocks (first/last in range) from the block table
    {
        let bn_col = block_table_desc
            .map(|d| d.block_number_column.as_str())
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
            .map(|d| d.block_number_column.as_str())
            .unwrap_or("number"),
    );

    // Collect all indexed batch sources (both primary and relation), keyed by output table name.
    // Multiple sources for the same table get merged into a single output array.
    let mut all_indexes: Vec<IndexedBatches> = Vec::new();

    for table_plan in &plan.table_plans {
        if let Some(output) = table_outputs.get(&table_plan.table) {
            let table_desc = metadata.table(&table_plan.table).unwrap();
            let bn_col = table_desc.block_number_column.as_str();
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
                        let rel_bn = rd.block_number_column.as_str();
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

    // 6. Write each block as JSON (sequential, directly into output buffer)
    // See decisions/002: sequential wins at production concurrency (CPU=8+).
    let t_json = timer!();
    for &block_num in &selected_blocks {
        json_writer.begin_item()?;
        let buf = json_writer.buf_mut();
        buf.push(b'{');

        // Write header
        write_header(
            buf,
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
                buf,
                block_num,
                &all_indexes,
                source_indices,
                &all_resolved,
                &all_grouped_resolved,
                json_prefix,
            );
        }

        json_close(b'}', buf);
    }

    elapsed!(t_json, "json output");
    elapsed!(t_total, "TOTAL");

    Ok(json_writer.finish()?)
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
