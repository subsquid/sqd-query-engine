use crate::metadata::{ColumnType, FieldSource, JsonEncoding, MemberDescription, TableDescription};
use crate::output::block_index::{build_block_index, BlockIndex};
use crate::output::encoder::{
    encode_json_string, encode_roll, resolve_encoder, snake_to_camel, Encoder, ResolvedRollEncoder,
    RollSource, Unrenderable,
};
use crate::output::projection::{ResolvedProjection, SelectedField};
use crate::output::row_order::RowOrder;
use crate::output::sources::{OutputHeader, OutputTable};
use crate::text::StringColumn;
use anyhow::Result;
use arrow::array::*;
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::collections::HashMap;

/// The block header table, ready to encode block by block.
pub(crate) struct PreparedHeader {
    batches: Vec<RecordBatch>,
    index: BlockIndex,
    resolved: Vec<Vec<ResolvedFieldWriter>>,
    /// The block number's key, for a block the table has no row for.
    number_prefix: Vec<u8>,
}

impl PreparedHeader {
    pub(crate) fn new(header: OutputHeader) -> Result<Self> {
        let batches = header.batches;
        let number_column = header.projection.desc().block_number_column.as_str();
        let index = build_block_index(&batches, number_column)?;

        let writers = build_field_writers(header.projection);
        let resolved = batches
            .iter()
            .map(|batch| resolve_writers(&writers, batch))
            .collect::<Result<_>>()?;

        let mut number_prefix = Vec::new();
        encode_json_string(&snake_to_camel(number_column), &mut number_prefix);
        number_prefix.push(b':');

        Ok(Self {
            batches,
            index,
            resolved,
            number_prefix,
        })
    }

    /// Write the `"header"` entry of block `block_num`.
    pub(crate) fn write(&self, buf: &mut Vec<u8>, block_num: u64) {
        buf.extend_from_slice(b"\"header\":{");

        let first = self.index.get(&block_num).and_then(|rows| rows.first());
        match first {
            Some(&(batch, row)) => {
                write_row_fields_resolved(buf, &self.batches[batch], row, &self.resolved[batch])
            }
            None => {
                buf.extend_from_slice(&self.number_prefix);
                let mut tmp = itoa::Buffer::new();
                buf.extend_from_slice(tmp.format(block_num).as_bytes());
            }
        }

        json_close(b'}', buf);
        buf.push(b',');
    }
}

/// A response table, ready to encode block by block.
pub(crate) struct PreparedTable {
    /// `"name":[`, which opens the table's items in a block.
    prefix: Vec<u8>,
    /// Never empty: a table without rows is not prepared.
    sources: Vec<PreparedSource>,
}

/// One source of a response table: its rows indexed by block, their order, and
/// its writers resolved against each batch.
struct PreparedSource {
    batches: Vec<RecordBatch>,
    index: BlockIndex,
    order: RowOrder,
    writers: SourceWriters,
}

/// A source's writers, resolved once per batch.
enum SourceWriters {
    Flat(Vec<Vec<ResolvedFieldWriter>>),
    Grouped(Vec<ResolvedGroupedWriters>),
}

/// Row references reused from block to block.
#[derive(Default)]
pub(crate) struct RowScratch {
    /// `(batch, row)` of one source.
    sort: Vec<(usize, usize)>,
    /// `(source, batch, row)` of several sources of one table.
    merge: Vec<(usize, usize, usize)>,
}

impl PreparedTable {
    /// Sources without rows are left out, and `None` is a table with none left:
    /// a source whose rows were read under another adds nothing to any block,
    /// and as a second source it would only slow the merge.
    pub(crate) fn new(table: OutputTable) -> Result<Option<Self>> {
        let sources = table
            .sources
            .into_iter()
            .filter(|source| source.rows.num_rows() > 0)
            .map(|source| PreparedSource::new(source.rows.into_batches(), source.projection))
            .collect::<Result<Vec<_>>>()?;
        if sources.is_empty() {
            return Ok(None);
        }

        let mut prefix = Vec::new();
        encode_json_string(table.name, &mut prefix);
        prefix.extend_from_slice(b":[");

        Ok(Some(Self { prefix, sources }))
    }

    /// Write the table's items of block `block_num`, if it has any.
    pub(crate) fn write_items(&self, buf: &mut Vec<u8>, block_num: u64, scratch: &mut RowScratch) {
        match self.sources.as_slice() {
            [source] => source.write_items(buf, block_num, &self.prefix, &mut scratch.sort),
            _ => self.write_merged_items(buf, block_num, &mut scratch.merge),
        }
    }

    /// Items of several sources merged into one array, ordered and deduplicated
    /// by the table's keys.
    fn write_merged_items(
        &self,
        buf: &mut Vec<u8>,
        block_num: u64,
        rows: &mut Vec<(usize, usize, usize)>,
    ) {
        rows.clear();
        for (si, source) in self.sources.iter().enumerate() {
            if let Some(refs) = source.index.get(&block_num) {
                rows.extend(refs.iter().map(|&(bi, ri)| (si, bi, ri)));
            }
        }

        if rows.is_empty() {
            return;
        }

        let sources = &self.sources;
        if sources[0].order.is_keyed() {
            let order = |a: &(usize, usize, usize), b: &(usize, usize, usize)| {
                let (a_order, b_order) = (&sources[a.0].order, &sources[b.0].order);
                a_order.compare((a.1, a.2), b_order, (b.1, b.2))
            };
            rows.sort_unstable_by(|a, b| order(a, b).then((a.0, a.1, a.2).cmp(&(b.0, b.1, b.2))));
            rows.dedup_by(|b, a| a.0 != b.0 && order(a, b).is_eq());
        }

        buf.extend_from_slice(&self.prefix);

        for (i, &(si, batch, row)) in rows.iter().enumerate() {
            if i > 0 {
                buf.push(b',');
            }
            buf.push(b'{');
            sources[si].write_row(buf, batch, row);
            json_close(b'}', buf);
        }

        buf.push(b']');
        buf.push(b',');
    }
}

impl PreparedSource {
    fn new(batches: Vec<RecordBatch>, projection: &ResolvedProjection) -> Result<Self> {
        let desc = projection.desc();
        let index = build_block_index(&batches, &desc.block_number_column)?;
        let order = RowOrder::new(&batches, desc);

        let writers = match build_grouped_writers(projection) {
            Some(grouped) => SourceWriters::Grouped(
                batches
                    .iter()
                    .map(|batch| resolve_grouped_writers(&grouped, batch))
                    .collect::<Result<_>>()?,
            ),
            None => {
                let writers = build_field_writers(projection);
                SourceWriters::Flat(
                    batches
                        .iter()
                        .map(|batch| resolve_writers(&writers, batch))
                        .collect::<Result<_>>()?,
                )
            }
        };

        Ok(Self {
            batches,
            index,
            order,
            writers,
        })
    }

    /// Write this source's items of block `block_num` as the table's array.
    fn write_items(
        &self,
        buf: &mut Vec<u8>,
        block_num: u64,
        prefix: &[u8],
        rows: &mut Vec<(usize, usize)>,
    ) {
        let row_refs = match self.index.get(&block_num) {
            Some(refs) if !refs.is_empty() => refs,
            _ => return,
        };

        rows.clear();
        rows.extend_from_slice(row_refs);
        self.order.sort(rows);

        buf.extend_from_slice(prefix);

        for (i, &(batch, row)) in rows.iter().enumerate() {
            if i > 0 {
                buf.push(b',');
            }
            buf.push(b'{');
            self.write_row(buf, batch, row);
            json_close(b'}', buf);
        }

        buf.push(b']');
        buf.push(b',');
    }

    #[inline]
    fn write_row(&self, buf: &mut Vec<u8>, batch: usize, row: usize) {
        match &self.writers {
            SourceWriters::Flat(resolved) => {
                write_row_fields_resolved(buf, &self.batches[batch], row, &resolved[batch])
            }
            SourceWriters::Grouped(resolved) => {
                write_row_grouped(buf, &self.batches[batch], row, &resolved[batch])
            }
        }
    }
}

/// Pre-computed information for writing a single output column.
enum FieldWriter {
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
struct RollSourceColumn {
    name: String,
    encoding: Option<JsonEncoding>,
    declared_type: Option<ColumnType>,
}

/// FieldWriter with column indices resolved for a specific batch schema.
struct ResolvedFieldWriter {
    json_key_prefix: Vec<u8>,
    /// Resolved column index for Regular, or resolved indices for Roll.
    indices: ResolvedIndices,
    /// Pre-resolved roll encoder (eliminates per-row DataType dispatch for Roll fields).
    roll_encoder: Option<ResolvedRollEncoder>,
}

enum ResolvedIndices {
    /// A Regular field's column index and its encoder, resolved together: the
    /// encoder is chosen from the array at that index, so neither exists without
    /// the other.
    Single(Option<(usize, Encoder)>),
    /// Multiple column indices for Roll fields.
    Multi(Vec<usize>),
}

/// Pre-computed structure for polymorphic field grouping (e.g., EVM traces).
/// Each variant's groups, each a JSON key and the writers that fill it. `"_"`
/// is the flat group, which is written without a wrapping object. A list, since
/// every row looks its variant up among a handful, which comparing tags does
/// faster than hashing them.
type VariantGroups<W> = Vec<(String, Vec<(Vec<u8>, Vec<W>)>)>;

struct GroupedWriters {
    /// Writers for the fields no variant claims, written flat on every row.
    base_writers: Vec<FieldWriter>,
    /// The column whose value picks the variant.
    variant_column: String,
    /// Per-variant grouped writers: variant -> [(group_json_key, writers)].
    variant_writers: VariantGroups<FieldWriter>,
}

/// Resolved grouped writers for a specific batch schema.
struct ResolvedGroupedWriters {
    base_resolved: Vec<ResolvedFieldWriter>,
    variant_col_idx: Option<usize>,
    variant_resolved: VariantGroups<ResolvedFieldWriter>,
}

/// Resolve field writers against a specific batch schema (done once per batch).
///
/// A column stored at a type nothing renders is refused here as well as at the
/// pre-scan check in execution: that check is what makes the refusal independent
/// of which rows a query reaches, and this is what makes it impossible to
/// render `null` in a value's place if a batch ever carries a type the schema
/// did not announce.
fn resolve_writers(
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

/// The writers of the projection's fields, each with its JSON key and the
/// columns it reads.
fn build_field_writers(projection: &ResolvedProjection) -> Vec<FieldWriter> {
    let desc = projection.desc();

    projection
        .fields()
        .iter()
        .map(|field| build_field_writer(field, desc))
        .collect()
}

fn build_field_writer(field: &SelectedField, desc: &TableDescription) -> FieldWriter {
    let mut prefix = Vec::with_capacity(field.name.len() + 4);
    encode_json_string(&snake_to_camel(field.name), &mut prefix);
    prefix.push(b':');

    match field.source {
        FieldSource::Roll(columns) => {
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

            FieldWriter::Roll {
                json_key_prefix: prefix,
                sources,
            }
        }
        FieldSource::Column(column) => {
            let declared = desc.columns.get(column);

            FieldWriter::Regular {
                json_key_prefix: prefix,
                column_name: column.to_string(),
                encoding: declared.and_then(|c| c.encoding.clone()),
                declared_type: declared.map(|c| c.data_type.clone()),
                members: declared.and_then(|c| c.members.clone()),
            }
        }
    }
}

/// Writers for a table whose rows come in variants. A field some variant maps
/// is written inside that variant's group; every other field is written flat,
/// for every row, so it is the mappings that decide which is which.
///
/// `None` when the table dispatches on nothing, which is the same table for
/// which the mappings are empty — the loader ties the two together.
fn build_grouped_writers(projection: &ResolvedProjection) -> Option<GroupedWriters> {
    let table_desc = projection.desc();
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
    let mut variant_groups: HashMap<(&str, &str), Vec<FieldWriter>> = HashMap::new();

    for field in projection.fields() {
        let Some(&(variant, group, json_name, phys_col)) = field_to_group.get(field.name) else {
            base_writers.push(build_field_writer(field, table_desc));
            continue;
        };

        // Grouped field. `phys_col` is the parquet column to read, which may
        // differ from the field's name (one column, many fields).
        let mut prefix = Vec::with_capacity(json_name.len() + 4);
        encode_json_string(json_name, &mut prefix);
        prefix.push(b':');
        let declared = table_desc.columns.get(phys_col);
        variant_groups
            .entry((variant, group))
            .or_default()
            .push(FieldWriter::Regular {
                json_key_prefix: prefix,
                column_name: phys_col.to_string(),
                encoding: declared.and_then(|c| c.encoding.clone()),
                declared_type: declared.map(|c| c.data_type.clone()),
                members: declared.and_then(|c| c.members.clone()),
            });
    }

    // A variant's groups are written in the order the catalog declares them.
    let mut variant_writers: VariantGroups<FieldWriter> = Vec::new();
    for (variant, groups) in &table_desc.output.variants {
        let mut group_list = Vec::new();
        for group_name in groups.keys() {
            let Some(writers) = variant_groups.remove(&(variant.as_str(), group_name.as_str()))
            else {
                continue;
            };
            // Special group name "_" means flat output (no wrapping sub-object)
            let key = if group_name == "_" {
                Vec::new()
            } else {
                let mut key = Vec::with_capacity(group_name.len() + 4);
                encode_json_string(group_name, &mut key);
                key.push(b':');
                key
            };
            group_list.push((key, writers));
        }
        if !group_list.is_empty() {
            variant_writers.push((variant.clone(), group_list));
        }
    }

    Some(GroupedWriters {
        base_writers,
        variant_column: variant_column.to_string(),
        variant_writers,
    })
}

fn resolve_grouped_writers(
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

    let mut variant_resolved: VariantGroups<ResolvedFieldWriter> = Vec::new();
    for (variant, groups) in &gw.variant_writers {
        let mut resolved_groups = Vec::with_capacity(groups.len());
        for (key, writers) in groups {
            resolved_groups.push((key.clone(), resolve_writers(writers, batch)?));
        }
        variant_resolved.push((variant.clone(), resolved_groups));
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
        let variant = resolved
            .variant_resolved
            .iter()
            .find(|(name, _)| name == tag);
        if let Some((_, groups)) = variant {
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

        let projection = ResolvedProjection::resolve("items", table, &selected).unwrap();
        let grouped = build_grouped_writers(&projection).expect("the table dispatches");

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

    /// Each row writes the groups of its own variant; a tag no variant names,
    /// and a null one, write the flat fields alone.
    ///
    /// Covers CT-6 · INV-O6
    #[test]
    fn a_row_writes_the_groups_of_its_own_variant() {
        use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
        use std::sync::Arc;

        let meta = variant_catalog();
        let table = meta.table("items").expect("the table is there");
        let selected: Vec<String> = ["seq", "kind", "call_from", "call_gas", "create_init"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let text = |values: [Option<&str>; 4]| -> ArrayRef {
            Arc::new(StringArray::from(values.to_vec()))
        };
        let batch = RecordBatch::try_from_iter([
            (
                "seq",
                Arc::new(UInt32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            (
                "kind",
                text([Some("call"), Some("create"), Some("reward"), None]),
            ),
            (
                "call_from",
                text([Some("a"), Some("b"), Some("c"), Some("d")]),
            ),
            (
                "call_gas",
                Arc::new(UInt64Array::from(vec![5, 6, 7, 8])) as ArrayRef,
            ),
            (
                "create_init",
                text([Some("i"), Some("j"), Some("k"), Some("l")]),
            ),
        ])
        .unwrap();

        let projection = ResolvedProjection::resolve("items", table, &selected).unwrap();
        let grouped = build_grouped_writers(&projection).expect("the table dispatches");
        let resolved = resolve_grouped_writers(&grouped, &batch).unwrap();
        let rows: Vec<String> = (0..batch.num_rows())
            .map(|row| {
                let mut buf = vec![b'{'];
                write_row_grouped(&mut buf, &batch, row, &resolved);
                json_close(b'}', &mut buf);
                String::from_utf8(buf).unwrap()
            })
            .collect();

        assert_eq!(
            rows,
            [
                r#"{"seq":0,"kind":"call","action":{"from":"a"},"result":{"gas":5}}"#,
                r#"{"seq":1,"kind":"create","action":{"init":"j"}}"#,
                r#"{"seq":2,"kind":"reward"}"#,
                r#"{"seq":3,"kind":null}"#,
            ]
        );
    }

    /// The groups of a variant are written in the order the catalog declares
    /// them. Neither the alphabet nor the column order decides it: here the
    /// declared order is neither, and `_`, which sorts first, is declared last
    /// for one variant and first for the other.
    ///
    /// Covers CT-6 · INV-O6
    #[test]
    fn variant_groups_keep_their_catalog_order() {
        use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt64Array};
        use std::sync::Arc;

        let meta = crate::metadata::parse_dataset_description(
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
      fields: [ seq, kind, call_from, call_note, call_gas, create_init, create_salt ]
      variant_column: kind
      variants:
        call:
          result: [ { column: call_gas, as: gas } ]
          action: [ { column: call_from, as: from } ]
          _: [ { column: call_note, as: note } ]
        create:
          _: [ { column: create_salt, as: salt } ]
          action: [ { column: create_init, as: init } ]
    item_order_keys: [ seq ]
    columns:
      block_number: { type: uint64 }
      seq: { type: uint32 }
      kind: { type: string }
      call_from: { type: string }
      call_note: { type: string }
      call_gas: { type: uint64 }
      create_init: { type: string }
      create_salt: { type: string }
"#,
        )
        .expect("the catalog is valid");
        let table = meta.table("items").expect("the table is there");
        let text = |values: [&str; 2]| -> ArrayRef { Arc::new(StringArray::from(values.to_vec())) };
        let batch = RecordBatch::try_from_iter([
            ("seq", Arc::new(UInt32Array::from(vec![0, 1])) as ArrayRef),
            ("kind", text(["call", "create"])),
            ("call_from", text(["a", "b"])),
            ("call_note", text(["n", "m"])),
            (
                "call_gas",
                Arc::new(UInt64Array::from(vec![5, 6])) as ArrayRef,
            ),
            ("create_init", text(["i", "j"])),
            ("create_salt", text(["s", "t"])),
        ])
        .unwrap();
        let render = |selected: &[&str]| -> Vec<String> {
            let selected: Vec<String> = selected.iter().map(|s| s.to_string()).collect();
            let projection = ResolvedProjection::resolve("items", table, &selected).unwrap();
            let grouped = build_grouped_writers(&projection).expect("the table dispatches");
            let resolved = resolve_grouped_writers(&grouped, &batch).unwrap();
            (0..batch.num_rows())
                .map(|row| {
                    let mut buf = vec![b'{'];
                    write_row_grouped(&mut buf, &batch, row, &resolved);
                    json_close(b'}', &mut buf);
                    String::from_utf8(buf).unwrap()
                })
                .collect()
        };

        assert_eq!(
            render(&[
                "seq",
                "call_from",
                "call_note",
                "call_gas",
                "create_init",
                "create_salt"
            ]),
            [
                r#"{"seq":0,"result":{"gas":5},"action":{"from":"a"},"note":"n"}"#,
                r#"{"seq":1,"salt":"t","action":{"init":"j"}}"#,
            ]
        );
        assert_eq!(
            render(&["seq", "call_from", "call_gas"]),
            [
                r#"{"seq":0,"result":{"gas":5},"action":{"from":"a"}}"#,
                r#"{"seq":1}"#,
            ],
            "a group with nothing selected is left out and the rest keep their order"
        );
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
}
