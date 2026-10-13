//! A table's selected fields, resolved once per query against the table's
//! catalog entry, and the columns each step of the query reads for them.
//!
//! A selection pass reads what weighs a row; a pass with payloads and a
//! read-back read what renders it; Arrow output ships the rendered columns
//! alone. Every one of those lists comes from one [`ResolvedProjection`], so no
//! two steps can read a field two ways.

use crate::error::ErrorKind;
use crate::metadata::{FieldSource, TableDescription, WeightSource};
use crate::output::weight::RowWeight;
use crate::query::RelationPlan;
use anyhow::Result;

/// One selected field and the columns it reads.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SelectedField<'a> {
    pub(crate) name: &'a str,
    pub(crate) source: FieldSource<'a>,
}

/// A table's selected fields, resolved against its catalog entry, and the
/// columns each step reads for them.
///
/// Each list is in a fixed order: the row key first where the list holds it,
/// then the fields in plan order, then the size columns. Only the
/// [`required`](Self::required) columns must be in the chunk; the row key and
/// the variant tag are the engine's own, and their absence is a different
/// error, raised where they are read.
pub(crate) struct ResolvedProjection<'a> {
    /// The table's catalog name.
    table: &'a str,
    desc: &'a TableDescription,
    /// The fields as the plan names them.
    names: &'a [String],
    fields: Vec<SelectedField<'a>>,
    rendered: Vec<&'a str>,
    required: Vec<&'a str>,
    weight: RowWeight<'a>,
    row_columns: Vec<&'a str>,
    size_columns: Vec<&'a str>,
}

impl<'a> ResolvedProjection<'a> {
    /// `UnknownField` for a field the catalog does not resolve, which a plan
    /// compiled against another catalog can name.
    pub(crate) fn resolve(
        table: &'a str,
        desc: &'a TableDescription,
        names: &'a [String],
    ) -> Result<Self> {
        let fields = names
            .iter()
            .map(|name| {
                let source = desc.field_source(name).ok_or_else(|| {
                    crate::engine_err!(
                        ErrorKind::UnknownField,
                        "field '{}' of '{}' is not in the catalog",
                        name,
                        table
                    )
                })?;

                Ok(SelectedField { name, source })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut rendered = Vec::new();
        for field in &fields {
            push_unique(&mut rendered, field.source.columns());
        }

        let required = required_columns(&rendered, desc);
        let weight = RowWeight::of(&rendered, desc);

        let mut row_columns = row_key(desc);
        push_unique(&mut row_columns, desc.output.variant_column.as_deref());
        push_unique(&mut row_columns, rendered.iter().copied());
        push_unique(&mut row_columns, weight.columns.iter().copied());

        let mut size_columns = vec![desc.block_number_column.as_str()];
        push_unique(&mut size_columns, weight.columns.iter().copied());

        Ok(Self {
            table,
            desc,
            names,
            fields,
            rendered,
            required,
            weight,
            row_columns,
            size_columns,
        })
    }

    /// The table's catalog name.
    pub(crate) fn table(&self) -> &'a str {
        self.table
    }

    pub(crate) fn desc(&self) -> &'a TableDescription {
        self.desc
    }

    /// The fields as the plan names them.
    pub(crate) fn names(&self) -> &'a [String] {
        self.names
    }

    /// Each field with the columns it reads, in the order a row writes them.
    pub(crate) fn fields(&self) -> &[SelectedField<'a>] {
        &self.fields
    }

    /// The columns the fields render, each once, in field order.
    pub(crate) fn rendered(&self) -> &[&'a str] {
        &self.rendered
    }

    /// The columns a chunk must hold for the fields to render; a missing one
    /// is `ColumnNotFound` (INV-E3). See `required_columns`.
    pub(crate) fn required(&self) -> &[&'a str] {
        &self.required
    }

    /// What a row weighs with these fields.
    pub(crate) fn weight(&self) -> &RowWeight<'a> {
        &self.weight
    }

    /// What one pass reads to render, order and weigh a row: its key, its
    /// variant tag, the rendered columns and the size columns that weigh them.
    pub(crate) fn row_columns(&self) -> &[&'a str] {
        &self.row_columns
    }

    /// What a narrow pass reads to weigh a row: its block number and the size
    /// columns.
    pub(crate) fn size_columns(&self) -> &[&'a str] {
        &self.size_columns
    }

    /// [`Self::row_columns`] and the source key of each relation: what one pass
    /// reads of a table whose rows those relations follow.
    pub(crate) fn with_relation_keys(&self, relations: &'a [RelationPlan]) -> Vec<&'a str> {
        let mut columns = self.row_columns.clone();
        for relation in relations {
            push_unique(&mut columns, relation.left_key.iter().map(String::as_str));
        }

        columns
    }
}

/// The columns that identify a row: the block number, the item order keys and
/// the address column (INV-D3).
pub(crate) fn row_key(desc: &TableDescription) -> Vec<&str> {
    let mut key = vec![desc.block_number_column.as_str()];
    push_unique(&mut key, desc.item_order_keys.iter().map(String::as_str));
    push_unique(&mut key, desc.address_column.as_deref());
    key
}

/// Append each of `extra` that `columns` does not hold yet.
pub(crate) fn push_unique<'a>(
    columns: &mut Vec<&'a str>,
    extra: impl IntoIterator<Item = &'a str>,
) {
    for column in extra {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
}

/// Physical columns a selected field cannot be rendered without, which must
/// exist in the parquet (a missing one is `ColumnNotFound`, INV-E3).
///
/// That is the column behind a plain or variant field, every source of a roll
/// field, and the `*_size` companion a field declares its weight through.
/// Engine-internal columns (block number, sort keys) are excluded: their
/// absence is a different error, raised where they are read.
///
/// A roll's sources are required even though a roll stops at its first null:
/// the sources are positional, and a chunk written before `a12` existed puts
/// `a13` in its place. The reference errors on the first missing source. A
/// weight column is required because a value weighed at zero is a page bounded
/// only by the transport (INV-B9).
fn required_columns<'a>(rendered: &[&'a str], desc: &'a TableDescription) -> Vec<&'a str> {
    let mut required = Vec::new();

    for &column in rendered {
        let Some((name, declared)) = desc.columns.get_key_value(column) else {
            continue;
        };
        if declared.system {
            continue;
        }

        push_unique(&mut required, [name.as_str()]);
        if let Some(WeightSource::Column(size)) = &declared.weight {
            push_unique(&mut required, [size.as_str()]);
        }
    }

    required
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{parse_dataset_description, DatasetDescription};
    use crate::query::RelationKind;

    fn catalog(name: &str) -> DatasetDescription {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("metadata")
            .join(format!("{name}.yaml"));
        parse_dataset_description(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn names(fields: &[&str]) -> Vec<String> {
        fields.iter().map(|field| field.to_string()).collect()
    }

    /// `withdrawals` and `extra_data` declare a `*_size` companion. The header
    /// scan used to project the block number and the requested columns only, so
    /// the companion was absent from the batch, the weight found nothing,
    /// and a block carrying two kilobytes of withdrawals weighed the 32 bytes of
    /// an empty one.
    #[test]
    fn a_header_reads_the_weight_columns_it_declares() {
        let meta = catalog("evm");
        let fields = names(&["number", "withdrawals", "extra_data"]);
        let header =
            ResolvedProjection::resolve("blocks", meta.table("blocks").unwrap(), &fields).unwrap();

        assert_eq!(
            header.row_columns(),
            [
                "number",
                "withdrawals",
                "extra_data",
                "withdrawals_size",
                "extra_data_size"
            ]
        );
    }

    /// A fixed-weight column brings no companion, and nothing is read twice.
    #[test]
    fn a_fixed_weight_column_adds_no_column() {
        let meta = catalog("evm");
        let fields = names(&["number", "logs_bloom"]);
        let header =
            ResolvedProjection::resolve("blocks", meta.table("blocks").unwrap(), &fields).unwrap();

        assert_eq!(header.row_columns(), ["number", "logs_bloom"]);
        assert_eq!(header.size_columns(), ["number"]);
    }

    /// A roll renders the columns it rolls, a variant field the column its
    /// mapping renames, and each column is read once.
    #[test]
    fn a_field_renders_the_columns_its_source_names() {
        let meta = catalog("evm");
        let fields = names(&["address", "topics", "data"]);
        let logs =
            ResolvedProjection::resolve("logs", meta.table("logs").unwrap(), &fields).unwrap();

        assert_eq!(
            logs.rendered(),
            ["address", "topic0", "topic1", "topic2", "topic3", "data"]
        );
        assert_eq!(
            logs.required(),
            [
                "address",
                "topic0",
                "topic1",
                "topic2",
                "topic3",
                "data",
                "data_size"
            ]
        );

        let fields = names(&["call_call_type", "call_type"]);
        let traces =
            ResolvedProjection::resolve("traces", meta.table("traces").unwrap(), &fields).unwrap();
        assert_eq!(traces.rendered(), ["call_type"]);
    }

    /// One pass reads a row's key and variant tag whether or not they are
    /// selected: the output orders, deduplicates and groups rows by them.
    #[test]
    fn a_row_reads_its_key_and_variant_tag() {
        let meta = catalog("evm");
        let fields = names(&["call_input"]);
        let traces =
            ResolvedProjection::resolve("traces", meta.table("traces").unwrap(), &fields).unwrap();

        assert_eq!(
            traces.row_columns(),
            [
                "block_number",
                "transaction_index",
                "trace_address",
                "type",
                "call_input",
                "call_input_size"
            ]
        );
        assert_eq!(traces.size_columns(), ["block_number", "call_input_size"]);
        assert_eq!(traces.required(), ["call_input", "call_input_size"]);
    }

    /// A pass whose rows feed a relation reads the relation's source key, even
    /// when no field selects it.
    #[test]
    fn a_relation_source_reads_its_key() {
        let meta = catalog("solana");
        let fields = names(&["program_id"]);
        let instructions = ResolvedProjection::resolve(
            "instructions",
            meta.table("instructions").unwrap(),
            &fields,
        )
        .unwrap();
        let relations = [RelationPlan {
            target_table: "transactions".to_string(),
            kind: RelationKind::Join,
            left_key: names(&["block_number", "transaction_index"]),
            right_key: names(&["block_number", "transaction_index"]),
            output_columns: vec![],
            source_items: None,
        }];

        assert_eq!(
            instructions.with_relation_keys(&relations),
            [
                "block_number",
                "transaction_index",
                "instruction_address",
                "program_id"
            ]
        );
    }

    /// A relation some items asked for follows the rows the scan marked as
    /// theirs, so the columns those items filter on are not read for it.
    #[test]
    fn a_relation_source_reads_no_filter_column() {
        let meta = catalog("solana");
        let fields = names(&["program_id"]);
        let instructions = ResolvedProjection::resolve(
            "instructions",
            meta.table("instructions").unwrap(),
            &fields,
        )
        .unwrap();
        let relations = [RelationPlan {
            target_table: "transactions".to_string(),
            kind: RelationKind::Join,
            left_key: names(&["block_number", "transaction_index"]),
            right_key: names(&["block_number", "transaction_index"]),
            output_columns: vec![],
            source_items: Some(vec![0]),
        }];

        let columns = instructions.with_relation_keys(&relations);
        assert!(!columns.contains(&"is_committed"));
    }

    #[test]
    fn a_field_the_catalog_lacks_is_unknown() {
        let meta = catalog("evm");
        let fields = names(&["address", "payload"]);
        let err = ResolvedProjection::resolve("logs", meta.table("logs").unwrap(), &fields)
            .err()
            .unwrap();

        assert_eq!(
            crate::error::error_kind(&err),
            Some(ErrorKind::UnknownField)
        );
    }
}
