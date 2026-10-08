use crate::integers::OwnedIntColumn;
use crate::metadata::{
    ColumnType, JsonEncoding, MemberDescription, TableDescription, VirtualField,
};
use crate::output::encoder::{
    encode_json_string, encode_roll, resolve_encoder, snake_to_camel, Encoder, ResolvedRollEncoder,
    RollSource, Unrenderable,
};
use crate::text::{OwnedStringColumn, StringColumn};
use anyhow::Result;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

/// Pre-indexed batch data for a single table source (primary or relation).
pub(crate) struct IndexedBatches {
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) index: FxHashMap<u64, Vec<(usize, usize)>>,
    pub(crate) writers: Vec<FieldWriter>,
    pub(crate) grouped: Option<GroupedWriters>,
    /// Actual table name (for merging same-table sources)
    pub(crate) table_name: String,
    /// Pre-computed sort columns (item_order_keys + address_column, excluding block_number).
    pub(crate) sort_columns: Vec<String>,
    /// Pre-resolved typed sort columns per batch (eliminates per-comparison downcast chain).
    pub(crate) sort_col_resolved: Vec<Vec<Option<TypedSortColumn>>>,
    /// The same keys as byte strings, where they order rows exactly as the
    /// typed columns do: two rows then compare as two byte strings.
    pub(crate) sort_rows: Option<SortKeys>,
}

/// Every batch's sort keys, in a form two rows compare by without resolving
/// their columns' types.
pub(crate) enum SortKeys {
    /// Integer columns and at most a trailing path of integers, read at their
    /// stored types, null slots as stored, as the typed columns read them.
    Typed(Vec<TypedKeys>),
    /// The same, written out as bytes for deep paths: each value at its
    /// stored width, big-endian, sign bit flipped, so a long shared prefix
    /// compares as one run of bytes rather than element by element.
    Packed(Vec<PackedKeys>),
    /// Arrow's row format, for any other columns without nulls.
    Rows(Vec<arrow::row::Rows>),
}

pub(crate) struct PackedKeys {
    ends: Vec<usize>,
    bytes: Vec<u8>,
}

impl PackedKeys {
    #[inline]
    fn key(&self, row: usize) -> &[u8] {
        let start = if row == 0 { 0 } else { self.ends[row - 1] };
        &self.bytes[start..self.ends[row]]
    }
}

pub(crate) struct TypedKeys {
    fixed: Vec<Vec<i128>>,
    path: Option<(arrow::buffer::OffsetBuffer<i32>, PathValues)>,
}

/// A path's elements at their stored type.
pub(crate) enum PathValues {
    U8(arrow::buffer::ScalarBuffer<u8>),
    U16(arrow::buffer::ScalarBuffer<u16>),
    U32(arrow::buffer::ScalarBuffer<u32>),
    U64(arrow::buffer::ScalarBuffer<u64>),
    I8(arrow::buffer::ScalarBuffer<i8>),
    I16(arrow::buffer::ScalarBuffer<i16>),
    I32(arrow::buffer::ScalarBuffer<i32>),
    I64(arrow::buffer::ScalarBuffer<i64>),
}

impl PathValues {
    fn resolve(values: &dyn Array) -> Option<Self> {
        macro_rules! resolve {
            ($($variant:ident($array:ty)),+) => {
                $(if let Some(a) = values.as_any().downcast_ref::<$array>() {
                    return Some(Self::$variant(a.values().clone()));
                })+
            };
        }
        resolve!(
            U8(UInt8Array),
            U16(UInt16Array),
            U32(UInt32Array),
            U64(UInt64Array),
            I8(Int8Array),
            I16(Int16Array),
            I32(Int32Array),
            I64(Int64Array)
        );
        None
    }

    /// The stored width in bytes.
    fn width(&self) -> usize {
        match self {
            Self::U8(_) | Self::I8(_) => 1,
            Self::U16(_) | Self::I16(_) => 2,
            Self::U32(_) | Self::I32(_) => 4,
            Self::U64(_) | Self::I64(_) => 8,
        }
    }

    /// Write elements `range` as order-preserving bytes into `out`.
    fn pack(&self, range: std::ops::Range<usize>, out: &mut [u8]) {
        macro_rules! pack {
            ($values:expr, $flip:expr) => {
                for (chunk, &value) in out
                    .chunks_exact_mut(std::mem::size_of_val(&$values[0]))
                    .zip(&$values[range])
                {
                    chunk.copy_from_slice(&$flip(value).to_be_bytes());
                }
            };
        }
        match self {
            Self::U8(v) if !v.is_empty() => pack!(v, |x: u8| x),
            Self::U16(v) if !v.is_empty() => pack!(v, |x: u16| x),
            Self::U32(v) if !v.is_empty() => pack!(v, |x: u32| x),
            Self::U64(v) if !v.is_empty() => pack!(v, |x: u64| x),
            Self::I8(v) if !v.is_empty() => pack!(v, |x: i8| (x as u8) ^ 0x80),
            Self::I16(v) if !v.is_empty() => pack!(v, |x: i16| (x as u16) ^ 0x8000),
            Self::I32(v) if !v.is_empty() => pack!(v, |x: i32| (x as u32) ^ 0x8000_0000),
            Self::I64(v) if !v.is_empty() => pack!(v, |x: i64| (x as u64) ^ (1 << 63)),
            _ => {}
        }
    }

    fn value(&self, i: usize) -> i128 {
        match self {
            Self::U8(v) => v[i] as i128,
            Self::U16(v) => v[i] as i128,
            Self::U32(v) => v[i] as i128,
            Self::U64(v) => v[i] as i128,
            Self::I8(v) => v[i] as i128,
            Self::I16(v) => v[i] as i128,
            Self::I32(v) => v[i] as i128,
            Self::I64(v) => v[i] as i128,
        }
    }

    /// Two paths compared element by element, then by length.
    fn compare(
        &self,
        a: std::ops::Range<usize>,
        other: &Self,
        b: std::ops::Range<usize>,
    ) -> std::cmp::Ordering {
        match (self, other) {
            (Self::U8(x), Self::U8(y)) => x[a].cmp(&y[b]),
            (Self::U16(x), Self::U16(y)) => x[a].cmp(&y[b]),
            (Self::U32(x), Self::U32(y)) => x[a].cmp(&y[b]),
            (Self::U64(x), Self::U64(y)) => x[a].cmp(&y[b]),
            (Self::I8(x), Self::I8(y)) => x[a].cmp(&y[b]),
            (Self::I16(x), Self::I16(y)) => x[a].cmp(&y[b]),
            (Self::I32(x), Self::I32(y)) => x[a].cmp(&y[b]),
            (Self::I64(x), Self::I64(y)) => x[a].cmp(&y[b]),
            _ => {
                let pairs = a.clone().zip(b.clone());
                pairs
                    .map(|(i, j)| self.value(i).cmp(&other.value(j)))
                    .find(|order| order.is_ne())
                    .unwrap_or(a.len().cmp(&b.len()))
            }
        }
    }
}

impl SortKeys {
    #[inline]
    fn compare(&self, (ba, ra): (usize, usize), (bb, rb): (usize, usize)) -> std::cmp::Ordering {
        match self {
            Self::Typed(keys) => {
                let (a, b) = (&keys[ba], &keys[bb]);
                for (x, y) in a.fixed.iter().zip(&b.fixed) {
                    let order = x[ra].cmp(&y[rb]);
                    if order.is_ne() {
                        return order;
                    }
                }
                match (&a.path, &b.path) {
                    (Some((oa, va)), Some((ob, vb))) => {
                        let path = |o: &arrow::buffer::OffsetBuffer<i32>, r: usize| {
                            o[r] as usize..o[r + 1] as usize
                        };
                        va.compare(path(oa, ra), vb, path(ob, rb))
                    }
                    _ => std::cmp::Ordering::Equal,
                }
            }
            Self::Packed(keys) => keys[ba].key(ra).cmp(keys[bb].key(rb)),
            Self::Rows(rows) => rows[ba].row(ra).cmp(&rows[bb].row(rb)),
        }
    }
}

/// Pre-resolved typed array (Arc-backed, cheap to clone) for sort comparisons.
/// Resolved once per column per batch; eliminates the downcast chain per
/// comparison.
pub(crate) enum TypedSortColumn {
    Int(OwnedIntColumn),
    Text(OwnedStringColumn),
    /// A list and its elements, when they are integers.
    List(GenericListArray<i32>, Option<OwnedIntColumn>),
}

impl TypedSortColumn {
    pub(crate) fn resolve(col: &dyn Array) -> Option<Self> {
        if let Some(ints) = OwnedIntColumn::resolve(col) {
            return Some(Self::Int(ints));
        }
        if let Some(text) = OwnedStringColumn::resolve(col) {
            return Some(Self::Text(text));
        }
        if let Some(a) = col.as_any().downcast_ref::<GenericListArray<i32>>() {
            let elements = OwnedIntColumn::resolve(a.values().as_ref());
            return Some(Self::List(a.clone(), elements));
        }

        None
    }

    /// Order two rows by this column.
    ///
    /// Integers compare as `i128`, so two sides stored at different widths — or
    /// one signed and one not — order by value rather than by bit pattern.
    /// Text compares as text whichever type carries it.
    fn cmp_rows(&self, row_a: usize, other: &TypedSortColumn, row_b: usize) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a.value(row_a).cmp(&b.value(row_b)),
            (Self::Text(a), Self::Text(b)) => a.value(row_a).cmp(&b.value(row_b)),
            (Self::List(a, ea), Self::List(b, eb)) => {
                compare_list_values((a, ea.as_ref()), row_a, (b, eb.as_ref()), row_b)
            }
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
    sort_col_resolved: &[Vec<Option<TypedSortColumn>>],
    sort_rows: Option<&SortKeys>,
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

    // Sort by pre-resolved typed sort columns (item_order_keys + address)
    sort_rows_by_order_keys_indexed(rows, sort_col_resolved, sort_rows);

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
            &idx.sort_col_resolved,
            idx.sort_rows.as_ref(),
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

    // Sort by item_order_keys (use first source's sort_columns, pre-resolved)
    let first = &all_indexes[source_indices[0]];
    let sort_columns = &first.sort_columns;

    if !sort_columns.is_empty() {
        let dedup_key_count = sort_columns.len();

        rows.sort_unstable_by(|a, b| {
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
pub(crate) fn resolve_sort_columns(
    batches: &[RecordBatch],
    sort_columns: &[String],
) -> Vec<Vec<Option<TypedSortColumn>>> {
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

/// The sort keys of every batch in Arrow's row format, when that orders rows
/// as the typed columns do: every key column resolved, of one type across the
/// batches, and without nulls, whose placement the two orders disagree on.
pub(crate) fn row_sort_keys(
    batches: &[RecordBatch],
    sort_columns: &[String],
    resolved: &[Vec<Option<TypedSortColumn>>],
) -> Option<SortKeys> {
    use arrow::row::{RowConverter, SortField};

    let first = batches.first()?;
    if sort_columns.is_empty() || resolved.iter().flatten().any(Option::is_none) {
        return None;
    }
    let types: Vec<arrow::datatypes::DataType> = sort_columns
        .iter()
        .map(|name| Some(first.column_by_name(name)?.data_type().clone()))
        .collect::<Option<_>>()?;
    let columns: Vec<Vec<arrow::array::ArrayRef>> = batches
        .iter()
        .map(|batch| {
            sort_columns
                .iter()
                .map(|name| batch.column_by_name(name).cloned())
                .collect::<Option<_>>()
        })
        .collect::<Option<_>>()?;
    let same_types = columns
        .iter()
        .all(|columns| columns.iter().zip(&types).all(|(c, t)| c.data_type() == t));
    if !same_types {
        return None;
    }

    if let Some(typed) = columns
        .iter()
        .map(|columns| typed_keys(columns))
        .collect::<Option<Vec<_>>>()
    {
        let deep = typed.iter().any(|keys| {
            keys.path.as_ref().is_some_and(|(offsets, _)| {
                let rows = offsets.len() - 1;
                let elements = (offsets[rows] - offsets[0]) as usize;
                elements >= PACKED_DEPTH * rows.max(1)
            })
        });
        if deep {
            return Some(SortKeys::Packed(typed.iter().map(packed_keys).collect()));
        }
        return Some(SortKeys::Typed(typed));
    }

    // The row format places nulls where the typed columns do not, and orders
    // list elements the typed columns hold equal when they are not integers.
    let representable = columns.iter().flatten().all(|column| {
        let list = matches!(column.data_type(), arrow::datatypes::DataType::List(_));
        column.null_count() == 0 && !list
    });
    if !representable {
        return None;
    }
    let converter = RowConverter::new(types.into_iter().map(SortField::new).collect()).ok()?;
    columns
        .iter()
        .map(|columns| converter.convert_columns(columns).ok())
        .collect::<Option<_>>()
        .map(SortKeys::Rows)
}

/// Paths at least this deep on average are compared as bytes.
const PACKED_DEPTH: usize = 8;

/// Typed keys written out as bytes.
fn packed_keys(keys: &TypedKeys) -> PackedKeys {
    let rows = keys.fixed.first().map_or_else(
        || {
            keys.path
                .as_ref()
                .map_or(0, |(offsets, _)| offsets.len() - 1)
        },
        Vec::len,
    );
    // Fixed columns are compared at i128, sign included.
    let fixed_bytes = keys.fixed.len() * 16;
    let mut ends = Vec::with_capacity(rows);
    let mut total = 0;
    for row in 0..rows {
        total += fixed_bytes;
        if let Some((offsets, values)) = &keys.path {
            total += (offsets[row + 1] - offsets[row]) as usize * values.width();
        }
        ends.push(total);
    }

    let mut bytes = vec![0u8; total];
    let mut start = 0;
    for row in 0..rows {
        let mut at = start;
        for column in &keys.fixed {
            let ordered = (column[row] as u128) ^ (1 << 127);
            bytes[at..at + 16].copy_from_slice(&ordered.to_be_bytes());
            at += 16;
        }
        if let Some((offsets, values)) = &keys.path {
            let path = offsets[row] as usize..offsets[row + 1] as usize;
            values.pack(path, &mut bytes[at..ends[row]]);
        }
        start = ends[row];
    }

    PackedKeys { ends, bytes }
}

/// One batch's keys when its columns are integers with at most a trailing
/// list of integers.
fn typed_keys(columns: &[arrow::array::ArrayRef]) -> Option<TypedKeys> {
    let (last, leading) = columns.split_last()?;
    let list = last.as_any().downcast_ref::<GenericListArray<i32>>();
    let fixed = if list.is_some() { leading } else { columns };

    let fixed = fixed
        .iter()
        .map(|column| {
            let values = OwnedIntColumn::resolve(column.as_ref())?;
            Some((0..column.len()).map(|row| values.value(row)).collect())
        })
        .collect::<Option<_>>()?;
    let path = match list {
        Some(list) => Some((
            list.offsets().clone(),
            PathValues::resolve(list.values().as_ref())?,
        )),
        None => None,
    };

    Some(TypedKeys { fixed, path })
}

/// Sort (batch_idx, row) references by pre-resolved typed sort columns.
/// Uses (batch_idx, row_idx) as final tiebreaker to preserve parquet file order.
fn sort_rows_by_order_keys_indexed(
    rows: &mut [(usize, usize)],
    sort_col_resolved: &[Vec<Option<TypedSortColumn>>],
    sort_rows: Option<&SortKeys>,
) {
    if rows.is_empty() {
        return;
    }
    if let Some(keys) = sort_rows {
        rows.sort_unstable_by(|&a, &b| keys.compare(a, b).then(a.cmp(&b)));
        return;
    }
    let has_sort_cols = sort_col_resolved.first().is_some_and(|v| !v.is_empty());
    if !has_sort_cols {
        // Even with no sort columns, sort by (batch, row) for deterministic order
        rows.sort_by_key(|&(bi, ri)| (bi, ri));
        return;
    }

    // The file-order tiebreaker makes the order total, so stability buys nothing.
    rows.sort_unstable_by(|&(bi_a, row_a), &(bi_b, row_b)| {
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

/// Compare two lists of integers element by element.
///
/// A list key is a path of item indices — a trace or instruction address — so
/// its elements are integers at whatever width the writer chose. Elements that
/// are not integers order as equal, which leaves the file-order tiebreaker to
/// decide; there is no order to invent for them.
fn compare_list_values(
    (a, elements_a): (&GenericListArray<i32>, Option<&OwnedIntColumn>),
    row_a: usize,
    (b, elements_b): (&GenericListArray<i32>, Option<&OwnedIntColumn>),
    row_b: usize,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    let (Some(ea), Some(eb)) = (elements_a, elements_b) else {
        return Ordering::Equal;
    };
    let path = |list: &GenericListArray<i32>, row: usize| {
        let offsets = list.value_offsets();
        offsets[row] as usize..offsets[row + 1] as usize
    };
    let (pa, pb) = (path(a, row_a), path(b, row_b));

    for (i, j) in pa.clone().zip(pb.clone()) {
        let ord = ea.value(i).cmp(&eb.value(j));
        if ord != Ordering::Equal {
            return ord;
        }
    }

    pa.len().cmp(&pb.len())
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

    fn sort_column(array: impl Array + 'static) -> TypedSortColumn {
        TypedSortColumn::resolve(&array).expect("the array is a sortable type")
    }

    /// Covers CT-6 · INV-O5
    #[test]
    fn test_typed_sort_column_same_type_comparison() {
        let col_a = sort_column(UInt32Array::from(vec![10, 20, 30]));
        let col_b = sort_column(UInt32Array::from(vec![15, 25, 5]));
        assert_eq!(col_a.cmp_rows(0, &col_b, 0), std::cmp::Ordering::Less); // 10 < 15
        assert_eq!(col_a.cmp_rows(1, &col_b, 2), std::cmp::Ordering::Greater); // 20 > 5
        assert_eq!(col_a.cmp_rows(0, &col_b, 2), std::cmp::Ordering::Greater); // 10 > 5
    }

    /// Two sources of one table can be stored at different widths, and the
    /// items still have to come back in item-key order. This used to assert the
    /// opposite — that a width mismatch was a programming error worth a
    /// `debug_assert` — which is what let a chunk written in eight bits emit its
    /// items in file order.
    ///
    /// Covers CT-6 · INV-D7
    /// Covers CT-6 · INV-O5
    #[test]
    fn a_narrower_column_orders_against_a_wider_one_by_value() {
        let wide = sort_column(UInt64Array::from(vec![10, 300]));

        for narrow in [
            sort_column(UInt8Array::from(vec![9, 11])),
            sort_column(Int8Array::from(vec![9, 11])),
            sort_column(UInt16Array::from(vec![9, 11])),
            sort_column(Int32Array::from(vec![9, 11])),
        ] {
            assert_eq!(narrow.cmp_rows(0, &wide, 0), std::cmp::Ordering::Less); // 9 < 10
            assert_eq!(narrow.cmp_rows(1, &wide, 0), std::cmp::Ordering::Greater); // 11 > 10
            assert_eq!(narrow.cmp_rows(1, &wide, 1), std::cmp::Ordering::Less); // 11 < 300
        }
    }

    /// A text sort key stored wide still orders. It used to resolve to nothing,
    /// and a `None` sort column compares every pair as equal, so the items came
    /// out in file order without a word.
    ///
    /// Covers CT-6 · INV-D7
    /// Covers CT-6 · INV-O5
    #[test]
    fn a_text_sort_key_orders_at_every_text_type() {
        let rows = vec!["b", "a", "c"];
        let plain = sort_column(StringArray::from(rows.clone()));

        for wide in [
            sort_column(LargeStringArray::from(rows.clone())),
            sort_column(StringViewArray::from(rows.clone())),
        ] {
            assert_eq!(wide.cmp_rows(1, &plain, 0), std::cmp::Ordering::Less); // a < b
            assert_eq!(wide.cmp_rows(2, &plain, 0), std::cmp::Ordering::Greater); // c > b
            assert_eq!(wide.cmp_rows(0, &wide, 0), std::cmp::Ordering::Equal);
        }
    }

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

    /// A width mismatch is no longer a mismatch; a *kind* mismatch still is, and
    /// the `debug_assert` is what says so rather than an invented ordering.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "TypedSortColumn type mismatch")]
    fn test_typed_sort_column_type_mismatch_panics_in_debug() {
        let number = sort_column(UInt32Array::from(vec![10]));
        let text = sort_column(StringArray::from(vec!["10"]));
        number.cmp_rows(0, &text, 0);
    }

    mod sort_keys {
        use super::super::*;
        use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray, UInt64Array};
        use arrow::datatypes::{Field, Int32Type, Schema, UInt16Type, UInt32Type};
        use proptest::prelude::*;
        use std::sync::Arc;

        /// With `nulls`, a value of 1 is stored as a null, in a slot that
        /// holds zero.
        /// With `deep`, every path starts with the same eight elements, deep
        /// enough to be compared as bytes.
        fn batch(
            rows: &[(i32, u64, String, Vec<u16>)],
            width: u8,
            nulls: bool,
            deep: bool,
        ) -> RecordBatch {
            let kept = |v: i64| (!nulls || v != 1).then_some(v);
            let rows: Vec<_> = rows
                .iter()
                .map(|r| {
                    let mut path = if deep { vec![258u16; 8] } else { Vec::new() };
                    path.extend(&r.3);
                    (r.0, r.1, r.2.clone(), path)
                })
                .collect();
            let signed: ArrayRef = Arc::new(Int32Array::from_iter(
                rows.iter().map(|r| kept(r.0 as i64).map(|v| v as i32)),
            ));
            let unsigned: ArrayRef =
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.1)));
            let text: ArrayRef = Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.2.clone()),
            ));
            let path: ArrayRef = if width == 2 {
                // Signed elements, some below zero.
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                    rows.iter().map(|r| {
                        Some(
                            r.3.iter()
                                .map(|&e| kept(e as i64).map(|v| v as i32 - 257))
                                .collect::<Vec<_>>(),
                        )
                    }),
                ))
            } else if width == 1 {
                Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(
                    rows.iter().map(|r| {
                        Some(
                            r.3.iter()
                                .map(|&e| kept(e as i64).map(|v| v as u32))
                                .collect::<Vec<_>>(),
                        )
                    }),
                ))
            } else {
                Arc::new(ListArray::from_iter_primitive::<UInt16Type, _, _>(
                    rows.iter().map(|r| {
                        Some(
                            r.3.iter()
                                .map(|&e| kept(e as i64).map(|v| v as u16))
                                .collect::<Vec<_>>(),
                        )
                    }),
                ))
            };
            let schema = Schema::new(vec![
                Field::new("signed", signed.data_type().clone(), true),
                Field::new("unsigned", unsigned.data_type().clone(), false),
                Field::new("text", text.data_type().clone(), false),
                Field::new("path", path.data_type().clone(), true),
            ]);
            RecordBatch::try_new(Arc::new(schema), vec![signed, unsigned, text, path]).unwrap()
        }

        fn row() -> impl Strategy<Value = (i32, u64, String, Vec<u16>)> {
            (
                prop_oneof![
                    Just(i32::MIN),
                    Just(-1),
                    Just(0),
                    Just(1),
                    Just(i32::MAX),
                    -3i32..3
                ],
                prop_oneof![Just(0u64), Just(u64::MAX), Just(1 << 63), 0u64..3],
                "[a-c]{0,3}",
                // Both bytes of an element vary, so their order shows.
                prop::collection::vec(
                    prop_oneof![Just(0u16), Just(u16::MAX), 0u16..3, 254u16..259],
                    0..4,
                ),
            )
        }

        proptest! {
            #[test]
            fn row_keys_order_rows_as_the_typed_columns_do(
                parts in prop::collection::vec(prop::collection::vec(row(), 0..12), 1..4),
                columns in prop::sample::subsequence(vec!["signed", "unsigned", "text", "path"], 1..=4),
                order in Just(()).prop_perturb(|_, mut rng| {
                    let mut all = vec!["signed", "unsigned", "text", "path"];
                    for i in (1..all.len()).rev() { all.swap(i, rng.random_range(0..=i)); }
                    all
                }),
                width in 0u8..3,
                nulls in any::<bool>(),
                deep in any::<bool>(),
            ) {
                let batches: Vec<RecordBatch> =
                    parts.iter().map(|rows| batch(rows, width, nulls, deep)).collect();
                let columns: Vec<String> = order
                    .iter()
                    .filter(|name| columns.contains(name))
                    .map(|name| name.to_string())
                    .collect();
                let resolved = resolve_sort_columns(&batches, &columns);
                let keys = row_sort_keys(&batches, &columns, &resolved);

                let all: Vec<(usize, usize)> = batches
                    .iter()
                    .enumerate()
                    .flat_map(|(b, batch)| (0..batch.num_rows()).map(move |r| (b, r)))
                    .collect();
                let mut typed = all.clone();
                sort_rows_by_order_keys_indexed(&mut typed, &resolved, None);
                let mut by_rows = all;
                sort_rows_by_order_keys_indexed(&mut by_rows, &resolved, keys.as_ref());
                let has = |name: &str| columns.iter().any(|c| c == name);
                let path_last = columns
                    .iter()
                    .position(|c| c == "path")
                    .is_none_or(|at| at + 1 == columns.len());
                // Integers with a trailing path are compared at their types;
                // other columns in the row format, which has no lists and no
                // nulls; the rest by the typed columns themselves.
                let expected = if !has("text") && path_last {
                    "typed"
                } else if !has("path") && !columns.iter().any(|c| {
                    batches.iter().any(|b| b.column_by_name(c).unwrap().null_count() > 0)
                }) {
                    "rows"
                } else {
                    "none"
                };
                let found = match &keys {
                    Some(SortKeys::Typed(_)) | Some(SortKeys::Packed(_)) => "typed",
                    Some(SortKeys::Rows(_)) => "rows",
                    None => "none",
                };
                prop_assert_eq!(found, expected);
                let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                let packed = matches!(keys, Some(SortKeys::Packed(_)));
                prop_assert_eq!(packed, expected == "typed" && deep && has("path") && rows > 0);
                prop_assert_eq!(by_rows, typed);
            }
        }

        #[test]
        fn a_null_path_sorts_as_the_empty_path_its_slot_holds() {
            let path: ArrayRef =
                Arc::new(ListArray::from_iter_primitive::<UInt16Type, _, _>(vec![
                    Some(vec![Some(1)]),
                    None,
                    Some(vec![]),
                    Some(vec![Some(0)]),
                ]));
            let schema = Schema::new(vec![Field::new("path", path.data_type().clone(), true)]);
            let batch = RecordBatch::try_new(Arc::new(schema), vec![path]).unwrap();
            let columns = vec!["path".to_string()];
            let batches = [batch];
            let resolved = resolve_sort_columns(&batches, &columns);
            let keys = row_sort_keys(&batches, &columns, &resolved);
            assert!(matches!(keys, Some(SortKeys::Typed(_))));

            let mut typed: Vec<(usize, usize)> = (0..4).map(|row| (0, row)).collect();
            let mut by_keys = typed.clone();
            sort_rows_by_order_keys_indexed(&mut typed, &resolved, None);
            sort_rows_by_order_keys_indexed(&mut by_keys, &resolved, keys.as_ref());
            assert_eq!(by_keys, typed);
            assert_eq!(typed, vec![(0, 1), (0, 2), (0, 3), (0, 0)]);
        }
    }
}
