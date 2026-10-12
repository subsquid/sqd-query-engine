//! Everything settled before a row is read: the catalog entry of every planned
//! table, the chunk's fitness for the query, the client's view of the chain,
//! and whether payloads are read during selection or after it.

use super::materialize::SelectionReader;
use super::ExecOptions;
use crate::integers::is_integer;
use crate::metadata::{DatasetDescription, TableDescription, WeightSource};
use crate::output::columns::{required_output_columns, resolve_output_columns};
use crate::output::row_order::{build_full_sort_columns, orders_rows};
use crate::output::weight::block_scan_columns;
use crate::output::{resolve_encoder, OutputFormat};
use crate::query::{Plan, RelationPlan, TablePlan};
use crate::scan::predicate::RowPredicate;
use crate::scan::{ChunkReader, ScanRequest};
use crate::text::StringColumn;
use anyhow::Result;
use std::sync::OnceLock;

/// One query's execution: the request, the catalog entries it reads, the
/// chunk, and how payloads are read. Every step after preparation reads it.
pub(super) struct Execution<'a> {
    pub(super) plan: &'a Plan,
    pub(super) metadata: &'a DatasetDescription,
    pub(super) chunk: &'a dyn ChunkReader,
    pub(super) options: ExecOptions,
    /// The block table's catalog entry.
    pub(super) blocks: &'a TableDescription,
    /// Each table plan with its catalog entries, in plan order.
    pub(super) tables: Vec<PlannedTable<'a>>,
    /// Present when payloads are deferred: a page is selected through it from
    /// row identities, keys and sizes, and its rows are read back afterwards.
    pub(super) selection: Option<SelectionReader<'a>>,
    /// See [`Self::selection_fits`].
    selection_fits: OnceLock<bool>,
}

/// A table plan with the catalog entries of its table and of each relation's
/// target.
pub(super) struct PlannedTable<'a> {
    pub(super) plan: &'a TablePlan,
    pub(super) desc: &'a TableDescription,
    /// One per relation of `plan`, in the same order.
    pub(super) targets: Vec<&'a TableDescription>,
}

impl<'a> Execution<'a> {
    /// Refuse a plan the catalog does not describe, then a chunk that cannot
    /// answer it, then a client whose chain has moved; then choose the read
    /// strategy.
    pub(super) fn prepare(
        plan: &'a Plan,
        metadata: &'a DatasetDescription,
        chunk: &'a dyn ChunkReader,
        options: ExecOptions,
        format: OutputFormat,
    ) -> Result<Self> {
        let blocks = catalog_entry(metadata, &plan.block_table)?;
        let tables = plan
            .table_plans
            .iter()
            .map(|table| PlannedTable::resolve(table, metadata))
            .collect::<Result<_>>()?;

        let mut execution = Self {
            plan,
            metadata,
            chunk,
            options,
            blocks,
            tables,
            selection: None,
            selection_fits: OnceLock::new(),
        };

        // A missing table is an incompatible chunk, not an empty table (INV-E4).
        execution.ensure_required_tables_present()?;
        execution.ensure_columns_renderable(format)?;

        // A reorg between two pages must be reported, not paved over with data
        // from the branch the client did not ask about.
        crate::output::fork::check_parent_block(plan, metadata, chunk)?;

        // A selective query reads its rows in one pass: a second pass by position
        // costs more than the payloads it would skip.
        let defers = options.range_reads && execution.defers_payloads();
        execution.selection = defers
            .then(|| SelectionReader::new(chunk, plan, metadata))
            .flatten();

        Ok(execution)
    }

    /// The reader a page is selected through.
    pub(super) fn scan_reader(&self) -> &dyn ChunkReader {
        match &self.selection {
            Some(reader) => reader,
            None => self.chunk,
        }
    }

    /// The tables every query scans: the block table and each table with an item
    /// request. A relation's target is not among them — it is opened only when the
    /// relation has rows to follow, and its absence is reported there (INV-E4).
    fn ensure_required_tables_present(&self) -> Result<()> {
        let ensure_present = |table: &str| {
            crate::engine_ensure!(
                self.chunk.has_table(table),
                crate::error::ErrorKind::TableNotFound,
                "table '{}' is not found in the chunk",
                table
            );
            Ok(())
        };

        ensure_present(&self.plan.block_table)?;
        for table in &self.tables {
            ensure_present(&table.plan.table)?;
        }

        Ok(())
    }

    /// The table whose first read range a narrow size scan can choose: the only
    /// one in the plan, unfiltered, with no relations, and with no header-only
    /// blocks to weigh beside it.
    pub(super) fn prescanned_table(&self) -> Option<&PlannedTable<'a>> {
        let [table] = self.tables.as_slice() else {
            return None;
        };
        let filtered = !table
            .plan
            .predicates
            .iter()
            .all(RowPredicate::matches_every_row);
        let sized = !self.plan.include_all_blocks && table.plan.relations.is_empty() && !filtered;

        sized.then_some(table)
    }

    /// Refuse a chunk that stores a selected column at a type the output cannot
    /// use, before any row is read (INV-E3).
    ///
    /// Off the schema rather than off the batches, so that the same chunk refuses
    /// whatever the query: the writers resolve against batches, and a range that
    /// reaches no row of a table resolves nothing. A column the chunk lacks is not
    /// this check's business — the scan raises `ColumnNotFound` for it.
    ///
    /// What "use" means depends on the format. JSON renders through an encoder,
    /// orders rows by the sort keys and groups them by the variant tag; Arrow ships
    /// the physical column and orders by an Arrow row key, so only the weight
    /// companion — an integer both formats read to bound the page (INV-B9) — is
    /// checked for it.
    fn ensure_columns_renderable(&self, format: OutputFormat) -> Result<()> {
        let json = matches!(format, OutputFormat::Json);
        let plan = self.plan;

        self.ensure_renderable(
            &plan.block_table,
            self.blocks,
            &plan.block_output_columns,
            json,
        )?;
        for table in &self.tables {
            let fields = &table.plan.output_columns;
            self.ensure_renderable(&table.plan.table, table.desc, fields, json)?;
            for (relation, target) in table.relations() {
                let fields = &relation.output_columns;
                self.ensure_renderable(&relation.target_table, target, fields, json)?;
            }
        }

        Ok(())
    }

    /// [`Self::ensure_columns_renderable`] for one table.
    fn ensure_renderable(
        &self,
        table: &str,
        desc: &TableDescription,
        output_columns: &[String],
        json: bool,
    ) -> Result<()> {
        let Some(schema) = self.chunk.table_schema(table) else {
            return Ok(());
        };

        for column in required_output_columns(output_columns, desc) {
            let Ok(field) = schema.field_with_name(&column) else {
                continue;
            };
            let declared = desc.columns.get(&column);

            if let Some(WeightSource::Column(companion)) = declared.and_then(|c| c.weight.as_ref())
            {
                if let Ok(field) = schema.field_with_name(companion) {
                    crate::engine_ensure!(
                        is_integer(field.data_type()),
                        crate::error::ErrorKind::MalformedChunkData,
                        "weight column '{}' of '{}' is stored as {}, which is not an integer",
                        companion,
                        table,
                        field.data_type()
                    );
                }
            }

            if !json {
                continue;
            }

            if let Err(e) = resolve_encoder(
                field.data_type(),
                declared.and_then(|c| c.encoding.as_ref()),
                declared.map(|c| &c.data_type),
                declared.and_then(|c| c.members.as_ref()),
            ) {
                crate::engine_bail!(
                    crate::error::ErrorKind::MalformedChunkData,
                    "column '{}' of '{}': {}",
                    column,
                    table,
                    e
                );
            }
        }

        if !json {
            return Ok(());
        }

        for key in build_full_sort_columns(desc) {
            if let Ok(field) = schema.field_with_name(&key) {
                crate::engine_ensure!(
                    orders_rows(field.data_type()),
                    crate::error::ErrorKind::MalformedChunkData,
                    "sort key '{}' of '{}' is stored as {}, which cannot order rows",
                    key,
                    table,
                    field.data_type()
                );
            }
        }

        if let Some(variant) = &desc.output.variant_column {
            if let Ok(field) = schema.field_with_name(variant) {
                let probe = arrow::array::new_empty_array(field.data_type());
                crate::engine_ensure!(
                    StringColumn::resolve(probe.as_ref()).is_some(),
                    crate::error::ErrorKind::MalformedChunkData,
                    "variant column '{}' of '{}' is stored as {}, which is not text",
                    variant,
                    table,
                    field.data_type()
                );
            }
        }

        Ok(())
    }

    /// Whether reading every output column of every matching row in the request
    /// could decode more than a page (ADR-15). Relations always defer their
    /// payloads; a prescanned table bounds its own first range. Header rows are read
    /// for the whole range either way.
    fn defers_payloads(&self) -> bool {
        let plan = self.plan;
        let has_relations = plan.table_plans.iter().any(|t| !t.relations.is_empty());
        if has_relations {
            return true;
        }

        // A page always holds its first block whole, so one pass over one block
        // decodes exactly the rows a deferred read would.
        let one_block = plan.to_block == Some(plan.from_block);
        if one_block {
            return false;
        }

        let columns = block_scan_columns(&plan.block_output_columns, self.blocks);
        let mut estimate =
            self.estimate_range_bytes(self.chunk, &plan.block_table, self.blocks, &columns, &[]);

        if self.prescanned_table().is_none() {
            for table in &self.tables {
                let columns = resolve_output_columns(table.plan, table.desc);
                let bytes = self.estimate_range_bytes(
                    self.chunk,
                    &table.plan.table,
                    table.desc,
                    &columns,
                    &table.plan.predicates,
                );
                estimate = estimate.saturating_add(bytes);
            }
        }

        estimate > self.options.weight_budget
    }

    /// Whether one selection pass over the whole request holds no more than the
    /// budget, by the same footer bound as the payloads (ADR-15). Taken when
    /// first asked, since only a request that ranges would split needs it.
    pub(super) fn selection_fits(&self) -> bool {
        *self.selection_fits.get_or_init(|| {
            self.selection.as_ref().is_some_and(|selection| {
                self.selection_bound(selection) <= self.options.weight_budget
            })
        })
    }

    /// What one selection pass over the whole request holds, by the footer: each
    /// table's matching row groups and every row group of each relation's target.
    /// Header rows are read for the whole range either way.
    fn selection_bound(&self, selection: &SelectionReader) -> u64 {
        // The selection reader reads its own columns of a table.
        let columns = [];

        let mut bound = 0u64;
        for table in &self.tables {
            let predicates = &table.plan.predicates;
            let bytes = self.estimate_range_bytes(
                selection,
                &table.plan.table,
                table.desc,
                &columns,
                predicates,
            );
            bound = bound.saturating_add(bytes);

            for (relation, target) in table.relations() {
                let bytes = self.estimate_range_bytes(
                    selection,
                    &relation.target_table,
                    target,
                    &columns,
                    &[],
                );
                bound = bound.saturating_add(bytes);
            }
        }

        bound
    }

    /// What one pass of `reader` over the request's range decodes for one table.
    /// A reader that cannot tell counts as unbounded, which only an unbounded
    /// budget holds.
    fn estimate_range_bytes(
        &self,
        reader: &dyn ChunkReader,
        table: &str,
        desc: &TableDescription,
        columns: &[String],
        predicates: &[RowPredicate],
    ) -> u64 {
        let mut request = ScanRequest::new(columns.iter().map(String::as_str).collect());
        request.predicates = predicates.iter().collect();
        request.from_block = Some(self.plan.from_block);
        request.to_block = self.plan.to_block;
        request.block_number_column = Some(&desc.block_number_column);

        reader
            .estimate_scan_bytes(table, &request)
            .unwrap_or(u64::MAX)
    }
}

impl<'a> PlannedTable<'a> {
    fn resolve(plan: &'a TablePlan, metadata: &'a DatasetDescription) -> Result<Self> {
        let desc = catalog_entry(metadata, &plan.table)?;
        let targets = plan
            .relations
            .iter()
            .map(|relation| catalog_entry(metadata, &relation.target_table))
            .collect::<Result<_>>()?;

        Ok(Self {
            plan,
            desc,
            targets,
        })
    }

    /// Each relation with its target's catalog entry.
    pub(super) fn relations(
        &self,
    ) -> impl Iterator<Item = (&'a RelationPlan, &'a TableDescription)> + '_ {
        self.plan.relations.iter().zip(self.targets.iter().copied())
    }
}

/// A plan compiled against another catalog can name a table this one lacks,
/// and such a table can be neither read nor rendered.
fn catalog_entry<'a>(
    metadata: &'a DatasetDescription,
    table: &str,
) -> Result<&'a TableDescription> {
    metadata.table(table).ok_or_else(|| {
        crate::engine_err!(
            crate::error::ErrorKind::TableNotFound,
            "table '{}' is not in the catalog",
            table
        )
    })
}
