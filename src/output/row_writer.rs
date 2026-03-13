use crate::metadata::{FieldGrouping, JsonEncoding, TableDescription, VirtualField};
use crate::output::encoder::{
    encode_json_string, encode_roll, resolve_encoder, snake_to_camel, EncoderFn,
};
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap;
use std::collections::HashMap;
use std::collections::HashSet;

/// Pre-indexed batch data for a single table source (primary or relation).
pub(crate) struct IndexedBatches<'a> {
    pub(crate) batches: &'a [RecordBatch],
    pub(crate) index: FxHashMap<u64, Vec<(usize, usize)>>,
    pub(crate) table_desc: &'a TableDescription,
    pub(crate) writers: Vec<FieldWriter>,
    pub(crate) grouped: Option<GroupedWriters>,
    /// Actual table name (for merging same-table sources)
    pub(crate) table_name: String,
    /// Pre-computed sort columns (item_order_keys + address_column, excluding block_number).
    pub(crate) sort_columns: Vec<String>,
    /// Pre-resolved typed sort columns per batch (eliminates per-comparison downcast chain).
    pub(crate) sort_col_resolved: Vec<Vec<Option<TypedSortColumn<'a>>>>,
}

/// Pre-resolved typed array reference for sort comparisons.
/// Resolved once per column per batch; eliminates 8-way downcast chain per comparison.
pub(crate) enum TypedSortColumn<'a> {
    UInt64(&'a UInt64Array),
    UInt32(&'a UInt32Array),
    UInt16(&'a UInt16Array),
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Int16(&'a Int16Array),
    Utf8(&'a StringArray),
    List(&'a GenericListArray<i32>),
}

impl<'a> TypedSortColumn<'a> {
    fn resolve(col: &'a dyn Array) -> Option<Self> {
        if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
            Some(Self::UInt64(a))
        } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
            Some(Self::UInt32(a))
        } else if let Some(a) = col.as_any().downcast_ref::<UInt16Array>() {
            Some(Self::UInt16(a))
        } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            Some(Self::Int64(a))
        } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            Some(Self::Int32(a))
        } else if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
            Some(Self::Int16(a))
        } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
            Some(Self::Utf8(a))
        } else if let Some(a) = col.as_any().downcast_ref::<GenericListArray<i32>>() {
            Some(Self::List(a))
        } else {
            None
        }
    }

    fn cmp_rows(&self, row_a: usize, other: &TypedSortColumn<'a>, row_b: usize) -> std::cmp::Ordering {
        match (self, other) {
            (Self::UInt64(a), Self::UInt64(b)) => a.value(row_a).cmp(&b.value(row_b)),
            (Self::UInt32(a), Self::UInt32(b)) => a.value(row_a).cmp(&b.value(row_b)),
            (Self::UInt16(a), Self::UInt16(b)) => a.value(row_a).cmp(&b.value(row_b)),
            (Self::Int64(a), Self::Int64(b)) => a.value(row_a).cmp(&b.value(row_b)),
            (Self::Int32(a), Self::Int32(b)) => a.value(row_a).cmp(&b.value(row_b)),
            (Self::Int16(a), Self::Int16(b)) => a.value(row_a).cmp(&b.value(row_b)),
            (Self::Utf8(a), Self::Utf8(b)) => a.value(row_a).cmp(b.value(row_b)),
            (Self::List(a), Self::List(b)) => compare_list_values(a, row_a, b, row_b),
            _ => {
                debug_assert!(false, "TypedSortColumn type mismatch in cmp_rows");
                std::cmp::Ordering::Equal
            }
        }
    }
}

/// Pre-computed information for writing a single output column.
pub(crate) enum FieldWriter {
    /// Virtual field: roll columns together.
    Roll {
        json_key_prefix: Vec<u8>,
        source_column_names: Vec<String>,
    },
    /// Regular column with optional encoding override.
    Regular {
        json_key_prefix: Vec<u8>,
        column_name: String,
        encoding: Option<JsonEncoding>,
    },
}

/// FieldWriter with column indices resolved for a specific batch schema.
pub(crate) struct ResolvedFieldWriter<'a> {
    writer: &'a FieldWriter,
    /// Resolved column index for Regular, or resolved indices for Roll.
    indices: ResolvedIndices,
    /// Pre-resolved encoder function for Regular fields (eliminates per-row DataType dispatch).
    encoder: Option<EncoderFn>,
}

pub(crate) enum ResolvedIndices {
    /// Single column index for Regular fields.
    Single(Option<usize>),
    /// Multiple column indices for Roll fields.
    Multi(Vec<usize>),
}

/// Pre-computed structure for polymorphic field grouping (e.g., EVM traces).
pub(crate) struct GroupedWriters {
    /// Writers for base fields (shared across all variants).
    base_writers: Vec<FieldWriter>,
    /// Tag column name for variant dispatch.
    tag_column: String,
    /// Per-variant grouped writers: tag_value -> [(group_json_key, writers)].
    variant_writers: HashMap<String, Vec<(Vec<u8>, Vec<FieldWriter>)>>,
}

/// Resolved grouped writers for a specific batch schema.
pub(crate) struct ResolvedGroupedWriters<'a> {
    base_resolved: Vec<ResolvedFieldWriter<'a>>,
    tag_col_idx: Option<usize>,
    variant_resolved: HashMap<&'a str, Vec<(&'a [u8], Vec<ResolvedFieldWriter<'a>>)>>,
}

/// Resolve field writers against a specific batch schema (done once per batch).
pub(crate) fn resolve_writers<'a>(
    writers: &'a [FieldWriter],
    batch: &RecordBatch,
) -> Vec<ResolvedFieldWriter<'a>> {
    writers
        .iter()
        .map(|w| match w {
            FieldWriter::Roll {
                source_column_names,
                ..
            } => {
                let idxs: Vec<usize> = source_column_names
                    .iter()
                    .filter_map(|c| batch.schema().index_of(c).ok())
                    .collect();
                ResolvedFieldWriter {
                    writer: w,
                    indices: ResolvedIndices::Multi(idxs),
                    encoder: None,
                }
            }
            FieldWriter::Regular {
                column_name,
                encoding,
                ..
            } => {
                let idx = batch.schema().index_of(column_name).ok();
                let encoder = idx.map(|i| {
                    resolve_encoder(batch.column(i).data_type(), encoding.as_ref())
                });
                ResolvedFieldWriter {
                    writer: w,
                    indices: ResolvedIndices::Single(idx),
                    encoder,
                }
            }
        })
        .collect()
}

/// Pre-compute the JSON key prefixes and column resolution for a table's output columns.
pub(crate) fn build_field_writers(
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
                .and_then(|d| d.columns.get(col_name))
                .and_then(|c| c.json_encoding.clone());

            FieldWriter::Regular {
                json_key_prefix: prefix,
                column_name: col_name.clone(),
                encoding,
            }
        })
        .collect()
}

pub(crate) fn build_grouped_writers(
    output_columns: &[String],
    table_desc: &TableDescription,
    grouping: &FieldGrouping,
) -> GroupedWriters {
    let base_set: HashSet<&str> = grouping.base_fields.iter().map(|s| s.as_str()).collect();

    // Build a reverse map: physical_column -> (variant, group, field_name)
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
    // variant -> group -> Vec<FieldWriter>
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
            let encoding = table_desc
                .columns
                .get(col_name)
                .and_then(|c| c.json_encoding.clone());
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
            let encoding = table_desc
                .columns
                .get(col_name)
                .and_then(|c| c.json_encoding.clone());
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
            // Special group name "_" means flat output (no wrapping sub-object)
            let key = if group_name == "_" {
                Vec::new()
            } else {
                let mut key = Vec::with_capacity(group_name.len() + 4);
                encode_json_string(&group_name, &mut key);
                key.push(b':');
                key
            };
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

pub(crate) fn resolve_grouped_writers<'a>(
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
                    json_key_prefix, ..
                },
                ResolvedIndices::Single(Some(idx)),
            ) => {
                let col = batch.column(*idx);
                buf.extend_from_slice(json_key_prefix);
                // Use pre-resolved encoder (no per-row DataType match)
                (rw.encoder.unwrap())(col.as_ref(), row, buf);
                buf.push(b',');
            }
            _ => {}
        }
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
                    if group_key.is_empty() {
                        // Flat group ("_"): write fields directly at current level
                        write_row_fields_resolved(buf, batch, row, writers);
                    } else {
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
}

/// Write a block header JSON.
pub(crate) fn write_header(
    buf: &mut Vec<u8>,
    block_num: u64,
    block_batches: &[RecordBatch],
    block_index: &FxHashMap<u64, Vec<(usize, usize)>>,
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
    block_index: &FxHashMap<u64, Vec<(usize, usize)>>,
    sort_col_resolved: &[Vec<Option<TypedSortColumn>>],
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

    // Sort by pre-resolved typed sort columns (item_order_keys + address)
    sort_rows_by_order_keys_indexed(&mut rows, sort_col_resolved);

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
pub(crate) fn write_merged_table_items(
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
            for (col_a, col_b) in res_a.iter().zip(res_b.iter()) {
                if let (Some(ca), Some(cb)) = (col_a, col_b) {
                    let ord = ca.cmp_rows(a.2, cb, b.2);
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
            for (col_a, col_b) in res_a.iter().zip(res_b.iter()).take(dedup_key_count) {
                if let (Some(ca), Some(cb)) = (col_a, col_b) {
                    if ca.cmp_rows(a.2, cb, b.2) != std::cmp::Ordering::Equal {
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

/// Build the full sort column list for a table: item_order_keys + address_column.
/// These match the legacy engine's primary key (minus block_number) for output ordering.
pub(crate) fn build_full_sort_columns(table_desc: &TableDescription) -> Vec<String> {
    let bn_col = table_desc.block_number_column.as_str();

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

/// Pre-resolve typed sort columns for each batch (done once, reused per block).
pub(crate) fn resolve_sort_columns<'a>(
    batches: &'a [RecordBatch],
    sort_columns: &[String],
) -> Vec<Vec<Option<TypedSortColumn<'a>>>> {
    batches
        .iter()
        .map(|b| {
            sort_columns
                .iter()
                .map(|k| {
                    b.schema()
                        .index_of(k)
                        .ok()
                        .and_then(|i| TypedSortColumn::resolve(b.column(i).as_ref()))
                })
                .collect()
        })
        .collect()
}

/// Sort (batch_idx, row) references by pre-resolved typed sort columns.
/// Uses (batch_idx, row_idx) as final tiebreaker to preserve parquet file order.
fn sort_rows_by_order_keys_indexed(
    rows: &mut Vec<(usize, usize)>,
    sort_col_resolved: &[Vec<Option<TypedSortColumn>>],
) {
    if rows.is_empty() {
        return;
    }
    let has_sort_cols = sort_col_resolved.first().is_some_and(|v| !v.is_empty());
    if !has_sort_cols {
        // Even with no sort columns, sort by (batch, row) for deterministic order
        rows.sort_by_key(|&(bi, ri)| (bi, ri));
        return;
    }

    rows.sort_by(|&(bi_a, row_a), &(bi_b, row_b)| {
        let res_a = &sort_col_resolved[bi_a];
        let res_b = &sort_col_resolved[bi_b];
        for (col_a, col_b) in res_a.iter().zip(res_b.iter()) {
            if let (Some(ca), Some(cb)) = (col_a, col_b) {
                let ord = ca.cmp_rows(row_a, cb, row_b);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
        }
        // Final tiebreaker: parquet file order (batch index, then row index)
        (bi_a, row_a).cmp(&(bi_b, row_b))
    });
}

/// Compare two List<UInt16/UInt32/Int32> arrays element by element.
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
pub(crate) fn json_close(end: u8, buf: &mut Vec<u8>) {
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

    #[test]
    fn test_typed_sort_column_same_type_comparison() {
        let a = UInt32Array::from(vec![10, 20, 30]);
        let b = UInt32Array::from(vec![15, 25, 5]);
        let col_a = TypedSortColumn::UInt32(&a);
        let col_b = TypedSortColumn::UInt32(&b);
        assert_eq!(col_a.cmp_rows(0, &col_b, 0), std::cmp::Ordering::Less); // 10 < 15
        assert_eq!(col_a.cmp_rows(1, &col_b, 2), std::cmp::Ordering::Greater); // 20 > 5
        assert_eq!(col_a.cmp_rows(0, &col_b, 2), std::cmp::Ordering::Greater); // 10 > 5
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "TypedSortColumn type mismatch")]
    fn test_typed_sort_column_type_mismatch_panics_in_debug() {
        let a = UInt32Array::from(vec![10]);
        let b = Int32Array::from(vec![10]);
        let col_a = TypedSortColumn::UInt32(&a);
        let col_b = TypedSortColumn::Int32(&b);
        col_a.cmp_rows(0, &col_b, 0);
    }
}
