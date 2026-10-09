use crate::metadata::{
    ColumnType, JsonEncoding, MemberDescription, TableDescription, VirtualField,
};
use crate::output::encoder::{
    encode_json_string, encode_roll, resolve_encoder, snake_to_camel, Encoder, ResolvedRollEncoder,
    RollSource, Unrenderable,
};
use crate::output::row_order::RowOrder;
use crate::text::StringColumn;
use anyhow::Result;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap;
use std::collections::BTreeMap;
use std::collections::HashMap;

/// Pre-indexed batch data for a single table source (primary or relation).
pub(crate) struct IndexedBatches {
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) index: FxHashMap<u64, Vec<(usize, usize)>>,
    pub(crate) writers: Vec<FieldWriter>,
    pub(crate) grouped: Option<GroupedWriters>,
    /// Actual table name (for merging same-table sources)
    pub(crate) table_name: String,
    pub(crate) order: RowOrder,
}

/// Pre-computed information for writing a single output column.
pub(crate) enum FieldWriter {
    /// Virtual field: roll columns together.
    Roll {
        json_key_prefix: Vec<u8>,
        sources: Vec<RollSourceColumn>,
    },
    /// Regular column with optional encoding override.
    Regular {
        json_key_prefix: Vec<u8>,
        column_name: String,
        encoding: Option<JsonEncoding>,
        /// The catalog's type for `column_name`, which the parquet file does not
        /// always match. `hexNumber` pads to the declared width.
        declared_type: Option<ColumnType>,
        /// How a struct column's members render, where the catalog says.
        members: Option<BTreeMap<String, MemberDescription>>,
    },
}

/// One column of a roll, with what the catalog declares for it.
pub(crate) struct RollSourceColumn {
    name: String,
    encoding: Option<JsonEncoding>,
    declared_type: Option<ColumnType>,
}

/// FieldWriter with column indices resolved for a specific batch schema.
pub(crate) struct ResolvedFieldWriter {
    json_key_prefix: Vec<u8>,
    /// Resolved column index for Regular, or resolved indices for Roll.
    indices: ResolvedIndices,
    /// Pre-resolved roll encoder (eliminates per-row DataType dispatch for Roll fields).
    roll_encoder: Option<ResolvedRollEncoder>,
}

pub(crate) enum ResolvedIndices {
    /// A Regular field's column index and its encoder, resolved together: the
    /// encoder is chosen from the array at that index, so neither exists without
    /// the other.
    Single(Option<(usize, Encoder)>),
    /// Multiple column indices for Roll fields.
    Multi(Vec<usize>),
}

/// Pre-computed structure for polymorphic field grouping (e.g., EVM traces).
/// A variant's groups, each a JSON key and the writers that fill it. `"_"` is
/// the flat group, which is written without a wrapping object.
type VariantGroups<W> = HashMap<String, Vec<(Vec<u8>, Vec<W>)>>;

pub(crate) struct GroupedWriters {
    /// Writers for the fields no variant claims, written flat on every row.
    base_writers: Vec<FieldWriter>,
    /// The column whose value picks the variant.
    variant_column: String,
    /// Per-variant grouped writers: variant -> [(group_json_key, writers)].
    variant_writers: VariantGroups<FieldWriter>,
}

/// Resolved grouped writers for a specific batch schema.
pub(crate) struct ResolvedGroupedWriters {
    base_resolved: Vec<ResolvedFieldWriter>,
    variant_col_idx: Option<usize>,
    variant_resolved: VariantGroups<ResolvedFieldWriter>,
}

/// Resolve field writers against a specific batch schema (done once per batch).
///
/// A column stored at a type nothing renders is refused here as well as at the
/// pre-scan check in assembly: that check is what makes the refusal independent
/// of which rows a query reaches, and this is what makes it impossible to
/// render `null` in a value's place if a batch ever carries a type the schema
/// did not announce.
pub(crate) fn resolve_writers(
    writers: &[FieldWriter],
    batch: &RecordBatch,
) -> Result<Vec<ResolvedFieldWriter>> {
    let mut resolved_writers = Vec::with_capacity(writers.len());

    for writer in writers {
        let resolved = match writer {
            FieldWriter::Roll {
                json_key_prefix,
                sources,
            } => {
                let present: Vec<(usize, &RollSourceColumn)> = sources
                    .iter()
                    .filter_map(|s| Some((batch.schema().index_of(&s.name).ok()?, s)))
                    .collect();
                let roll_sources: Vec<RollSource<'_>> = present
                    .iter()
                    .map(|(index, s)| RollSource {
                        column_index: *index,
                        encoding: s.encoding.as_ref(),
                        declared_type: s.declared_type.as_ref(),
                    })
                    .collect();
                let roll_encoder = ResolvedRollEncoder::resolve(batch, &roll_sources)
                    .map_err(|e| unrenderable(roll_source_names(sources), e))?;
                let idxs = present.iter().map(|(index, _)| *index).collect();
                ResolvedFieldWriter {
                    json_key_prefix: json_key_prefix.clone(),
                    indices: ResolvedIndices::Multi(idxs),
                    roll_encoder: Some(roll_encoder),
                }
            }
            FieldWriter::Regular {
                json_key_prefix,
                column_name,
                encoding,
                declared_type,
                members,
            } => {
                let mut resolved = None;
                if let Ok(i) = batch.schema().index_of(column_name) {
                    let encoder = resolve_encoder(
                        batch.column(i).data_type(),
                        encoding.as_ref(),
                        declared_type.as_ref(),
                        members.as_ref(),
                    )
                    .map_err(|e| unrenderable(column_name.clone(), e))?;
                    resolved = Some((i, encoder));
                }
                ResolvedFieldWriter {
                    json_key_prefix: json_key_prefix.clone(),
                    indices: ResolvedIndices::Single(resolved),
                    roll_encoder: None,
                }
            }
        };
        resolved_writers.push(resolved);
    }

    Ok(resolved_writers)
}

fn roll_source_names(sources: &[RollSourceColumn]) -> String {
    sources
        .iter()
        .map(|s| s.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn unrenderable(column: String, cause: Unrenderable) -> anyhow::Error {
    crate::engine_err!(
        crate::error::ErrorKind::MalformedChunkData,
        "column '{}': {}",
        column,
        cause
    )
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
                if let Some(vf) = desc.output.virtual_fields.get(col_name) {
                    match vf {
                        VirtualField::Roll { columns } => {
                            let mut prefix = Vec::with_capacity(col_name.len() + 4);
                            encode_json_string(&snake_to_camel(col_name), &mut prefix);
                            prefix.push(b':');
                            let sources = columns
                                .iter()
                                .map(|name| {
                                    let declared = desc.columns.get(name);
                                    RollSourceColumn {
                                        name: name.clone(),
                                        encoding: declared.and_then(|c| c.encoding.clone()),
                                        declared_type: declared.map(|c| c.data_type.clone()),
                                    }
                                })
                                .collect();
                            return FieldWriter::Roll {
                                json_key_prefix: prefix,
                                sources,
                            };
                        }
                    }
                }
            }

            let mut prefix = Vec::with_capacity(col_name.len() + 4);
            encode_json_string(&snake_to_camel(col_name), &mut prefix);
            prefix.push(b':');

            let declared = table_desc.and_then(|d| d.columns.get(col_name));

            FieldWriter::Regular {
                json_key_prefix: prefix,
                column_name: col_name.clone(),
                encoding: declared.and_then(|c| c.encoding.clone()),
                declared_type: declared.map(|c| c.data_type.clone()),
                members: declared.and_then(|c| c.members.clone()),
            }
        })
        .collect()
}

/// Writers for a table whose rows come in variants. A field some variant maps
/// is written inside that variant's group; every other field is written flat,
/// for every row, so it is the mappings that decide which is which.
///
/// `None` when the table dispatches on nothing, which is the same table for
/// which the mappings are empty — the loader ties the two together.
pub(crate) fn build_grouped_writers(
    output_columns: &[String],
    table_desc: &TableDescription,
) -> Option<GroupedWriters> {
    let variant_column = table_desc.output.variant_column.as_deref()?;

    // Build a reverse map: field -> (variant, group, json_name, physical_column).
    // The field usually equals the physical column, but a column may back
    // several output fields (e.g. trace `call_type` → `type` and `callType`).
    let mut field_to_group: HashMap<&str, (&str, &str, &str, &str)> = HashMap::new();
    for (variant_name, groups) in &table_desc.output.variants {
        for (group_name, mappings) in groups {
            for mapping in mappings {
                field_to_group.insert(
                    mapping.field(),
                    (
                        variant_name.as_str(),
                        group_name.as_str(),
                        mapping.json_name.as_str(),
                        mapping.column.as_str(),
                    ),
                );
            }
        }
    }

    // Separate output columns into base vs grouped.
    let mut base_writers = Vec::new();
    // variant -> group -> Vec<FieldWriter>. Ordered: the groups of a variant are
    // written in this order, and a HashMap would give it the process's hash
    // seed, so `action` and `result` would swap places between restarts.
    let mut variant_groups: BTreeMap<String, BTreeMap<String, Vec<FieldWriter>>> = BTreeMap::new();

    for col_name in output_columns {
        let Some(&(variant, group, json_name, phys_col)) = field_to_group.get(col_name.as_str())
        else {
            base_writers.extend(build_field_writers(
                std::slice::from_ref(col_name),
                Some(table_desc),
            ));
            continue;
        };

        // Grouped field. `phys_col` is the parquet column to read, which may
        // differ from the field `col_name` (one column, many fields).
        let mut prefix = Vec::with_capacity(json_name.len() + 4);
        encode_json_string(json_name, &mut prefix);
        prefix.push(b':');
        let declared = table_desc.columns.get(phys_col);
        variant_groups
            .entry(variant.to_string())
            .or_default()
            .entry(group.to_string())
            .or_default()
            .push(FieldWriter::Regular {
                json_key_prefix: prefix,
                column_name: phys_col.to_string(),
                encoding: declared.and_then(|c| c.encoding.clone()),
                declared_type: declared.map(|c| c.data_type.clone()),
                members: declared.and_then(|c| c.members.clone()),
            });
    }

    // Convert variant_groups into the final structure
    let mut variant_writers: VariantGroups<FieldWriter> = HashMap::new();
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

    Some(GroupedWriters {
        base_writers,
        variant_column: variant_column.to_string(),
        variant_writers,
    })
}

pub(crate) fn resolve_grouped_writers(
    gw: &GroupedWriters,
    batch: &RecordBatch,
) -> Result<ResolvedGroupedWriters> {
    let base_resolved = resolve_writers(&gw.base_writers, batch)?;

    let variant_col_idx = batch.schema().index_of(&gw.variant_column).ok();
    if let Some(idx) = variant_col_idx {
        // The tag picks which groups a row gets. A tag no reader resolves
        // would pick none, and every row would render as the bare variant.
        let column = batch.column(idx);
        crate::engine_ensure!(
            StringColumn::resolve(column.as_ref()).is_some(),
            crate::error::ErrorKind::MalformedChunkData,
            "variant column '{}' is stored as {}, which is not text",
            gw.variant_column,
            column.data_type()
        );
    }

    let mut variant_resolved: VariantGroups<ResolvedFieldWriter> = HashMap::new();
    for (variant, groups) in &gw.variant_writers {
        let mut resolved_groups = Vec::with_capacity(groups.len());
        for (key, writers) in groups {
            resolved_groups.push((key.clone(), resolve_writers(writers, batch)?));
        }
        variant_resolved.insert(variant.clone(), resolved_groups);
    }

    Ok(ResolvedGroupedWriters {
        base_resolved,
        variant_col_idx,
        variant_resolved,
    })
}

/// Write all fields for a single row as JSON key-value pairs using resolved writers.
fn write_row_fields_resolved(
    buf: &mut Vec<u8>,
    batch: &RecordBatch,
    row: usize,
    resolved: &[ResolvedFieldWriter],
) {
    for rw in resolved {
        match &rw.indices {
            ResolvedIndices::Multi(indices) => {
                if !indices.is_empty() {
                    buf.extend_from_slice(&rw.json_key_prefix);
                    if let Some(ref roll_enc) = rw.roll_encoder {
                        roll_enc.encode(batch, row, buf);
                    } else {
                        encode_roll(batch, row, indices, buf);
                    }
                    buf.push(b',');
                }
            }
            ResolvedIndices::Single(Some((idx, encoder))) => {
                let col = batch.column(*idx);
                buf.extend_from_slice(&rw.json_key_prefix);
                encoder.encode(col.as_ref(), row, buf);
                buf.push(b',');
            }
            ResolvedIndices::Single(None) => {}
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

    // Read the variant column to determine the variant
    let tag_value = resolved.variant_col_idx.and_then(|idx| {
        let col = batch.column(idx);
        StringColumn::resolve(col.as_ref())
            .expect("checked when the writers were resolved")
            .value(row)
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
#[allow(clippy::too_many_arguments)]
fn write_table_items_indexed(
    buf: &mut Vec<u8>,
    block_num: u64,
    batches: &[RecordBatch],
    block_index: &FxHashMap<u64, Vec<(usize, usize)>>,
    order: &RowOrder,
    json_array_prefix: &[u8],
    resolved_by_batch: &[Vec<ResolvedFieldWriter>],
    grouped_resolved: Option<&[ResolvedGroupedWriters]>,
    scratch: &mut Vec<(usize, usize)>,
) {
    let row_refs = match block_index.get(&block_num) {
        Some(refs) if !refs.is_empty() => refs,
        _ => return,
    };

    // Build sortable rows (with batch_idx for resolved writer lookup). Reuse the
    // caller-owned scratch buffer to avoid a fresh per-block allocation.
    let rows = scratch;
    rows.clear();
    rows.extend_from_slice(row_refs);

    order.sort(rows);

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
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_merged_table_items(
    buf: &mut Vec<u8>,
    block_num: u64,
    all_indexes: &[IndexedBatches],
    source_indices: &[usize],
    all_resolved: &[Vec<Vec<ResolvedFieldWriter>>],
    all_grouped_resolved: &[Option<Vec<ResolvedGroupedWriters>>],
    json_array_prefix: &[u8],
    sort_scratch: &mut Vec<(usize, usize)>,
    merge_scratch: &mut Vec<(usize, usize, usize)>,
) {
    // If single source, use optimized path
    if source_indices.len() == 1 {
        let si = source_indices[0];
        let idx = &all_indexes[si];
        write_table_items_indexed(
            buf,
            block_num,
            &idx.batches,
            &idx.index,
            &idx.order,
            json_array_prefix,
            &all_resolved[si],
            all_grouped_resolved[si].as_deref(),
            sort_scratch,
        );
        return;
    }

    // Collect rows from all sources: (source_idx, batch_idx, row_idx). Reuse the
    // caller-owned scratch buffer to avoid a fresh per-block allocation.
    let rows = merge_scratch;
    rows.clear();
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

    // Sources of one table order and deduplicate by its keys.
    let first = &all_indexes[source_indices[0]].order;
    if first.is_keyed() {
        let order = |a: &(usize, usize, usize), b: &(usize, usize, usize)| {
            let (a_order, b_order) = (&all_indexes[a.0].order, &all_indexes[b.0].order);
            a_order.compare((a.1, a.2), b_order, (b.1, b.2))
        };
        rows.sort_unstable_by(|a, b| order(a, b).then((a.0, a.1, a.2).cmp(&(b.0, b.1, b.2))));
        rows.dedup_by(|b, a| a.0 != b.0 && order(a, b).is_eq());
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

    /// A trace catalog in miniature: two variants, two groups each, one column
    /// that is only ever flat.
    fn variant_catalog() -> crate::metadata::DatasetDescription {
        crate::metadata::parse_dataset_description(
            r#"
version: v2
name: test
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number: { type: uint64 }
  items:
    request:
      filters: []
    output:
      name: item
      fields: [ seq, kind, note, call_from, call_gas, create_init ]
      variant_column: kind
      variants:
        call:
          action: [ { column: call_from, as: from } ]
          result: [ { column: call_gas, as: gas } ]
        create:
          action: [ { column: create_init, as: init } ]
    item_order_keys: [ seq ]
    columns:
      block_number: { type: uint64 }
      seq: { type: uint32 }
      kind: { type: string }
      note: { type: string }
      call_from: { type: string }
      call_gas: { type: uint64 }
      create_init: { type: string }
"#,
        )
        .expect("the catalog is valid")
    }

    /// A field no mapping claims is flat; a field some mapping claims is not.
    ///
    /// The catalog says which is which by omission — the explicit list of flat
    /// fields is gone — so nothing but this test states the direction, and the
    /// direction is the difference between `type` at the top level of every
    /// trace and `type` inside one variant's group.
    ///
    /// Covers CT-6 · INV-O6
    #[test]
    fn a_field_is_flat_exactly_when_no_variant_claims_it() {
        let meta = variant_catalog();
        let table = meta.table("items").expect("the table is there");
        let selected: Vec<String> = [
            "seq",
            "kind",
            "note",
            "call_from",
            "call_gas",
            "create_init",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        let grouped = build_grouped_writers(&selected, table).expect("the table dispatches");

        let flat: Vec<&str> = grouped
            .base_writers
            .iter()
            .map(|w| match w {
                FieldWriter::Regular { column_name, .. } => column_name.as_str(),
                FieldWriter::Roll { .. } => "roll",
            })
            .collect();
        assert_eq!(flat, ["seq", "kind", "note"], "only the unclaimed fields");
    }

    /// The groups of a variant are written in catalog order. Held in a `HashMap`
    /// they came out in the process's hash order instead, so `action` and
    /// `result` swapped places between restarts — the one part of field order
    /// the engine, rather than the catalog, decides.
    ///
    /// Covers CT-6 · INV-O6
    #[test]
    fn variant_groups_keep_their_catalog_order() {
        let meta = variant_catalog();
        let table = meta.table("items").expect("the table is there");
        let selected: Vec<String> = ["call_from", "call_gas"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        for _ in 0..16 {
            let grouped = build_grouped_writers(&selected, table).expect("the table dispatches");
            let keys: Vec<String> = grouped.variant_writers["call"]
                .iter()
                .map(|(key, _)| String::from_utf8_lossy(key).into_owned())
                .collect();
            assert_eq!(keys, ["\"action\":", "\"result\":"]);
        }
    }
}
