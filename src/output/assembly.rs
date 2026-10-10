use crate::integers::is_integer;
use crate::metadata::{DatasetDescription, TableDescription, WeightSource};
use crate::output::arrow_out::{
    dedup_first, filter_to_blocks, hexify_group, project_columns, write_arrow_frames, ArrowOutput,
    OutputFormat,
};
use crate::output::block_index::{
    collect_block_numbers, collect_boundary_blocks, compute_block_range,
};
use crate::output::columns::{
    find_address_column, group_keys_for_relation, physical_output_columns, required_output_columns,
    resolve_output_columns, resolve_relation_output_columns,
};
use crate::output::encoder::resolve_encoder;
use crate::output::materialize::{
    materialize_tables, read_rows, retain_blocks, retain_selected_keys, SelectionReader,
};
use crate::output::row_order::{build_full_sort_columns, orders_rows};
use crate::output::row_writer::{PreparedHeader, PreparedTable};
use crate::output::sources::output_tables;
use crate::output::weight::{
    block_scan_columns, compute_block_weights, weight_range_end, weight_scan_columns,
    BlockSelection, TableOutput,
};
use crate::output::writer::QueryOutput;
use crate::query::{Plan, RelationKind, RelationPlan, TablePlan};
use crate::scan::predicate::RowPredicate;
use crate::scan::{
    AddressIndex, ChunkReader, HierarchicalFilter, HierarchicalMode, KeyFilter, KeySet,
    ParquetChunkReader, Rows, ScanRequest, Window,
};
use crate::text::StringColumn;
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use rustc_hash::FxHashSet as HashSet;
use std::collections::HashMap;
use std::path::Path;

/// Execution controls. Disabling range reads provides a full-read reference.
#[derive(Debug, Clone, Copy)]
pub struct ExecOptions {
    /// Print stage timings to stderr.
    pub profile: bool,
    /// The cumulative block weight a response may carry (`P-WEIGHT-BUDGET`).
    pub weight_budget: u64,
    /// Read complete block ranges until the response budget is exhausted.
    /// When disabled, read the whole requested range before selecting blocks.
    pub range_reads: bool,
}

impl Default for ExecOptions {
    fn default() -> Self {
        Self {
            profile: false,
            weight_budget: crate::output::weight::MAX_RESPONSE_BYTES,
            range_reads: true,
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

/// Execute a plan against a chunk directory. Returns `None` if the output
/// contains no blocks, which only happens when the queried block range doesn't
/// intersect the chunk's data — a query whose filters match nothing still
/// yields the boundary blocks of the range as header-only entries. See
/// [`QueryOutput`] for the block range metadata and lazy block encoding.
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

/// The tables every query scans: the block table and each table with an item
/// request. A relation's target is not among them — it is opened only when the
/// relation has rows to follow, and its absence is reported there (INV-E4).
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
    }

    Ok(())
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
fn ensure_columns_renderable(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    format: OutputFormat,
) -> Result<()> {
    let json = matches!(format, OutputFormat::Json);

    let check = |table: &str, output_columns: &[String]| -> Result<()> {
        let (Some(desc), Some(schema)) = (metadata.table(table), chunk.table_schema(table)) else {
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
    };

    check(&plan.block_table, &plan.block_output_columns)?;
    for table_plan in &plan.table_plans {
        check(&table_plan.table, &table_plan.output_columns)?;
        for relation in &table_plan.relations {
            check(&relation.target_table, &relation.output_columns)?;
        }
    }

    Ok(())
}

/// Suggest a range large enough to cover a batch of row groups in every table.
/// Missing statistics select a full read; overlapping groups produce larger ranges.
fn next_range_end(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    from_block: u64,
) -> Option<u64> {
    let mut tables = HashSet::default();
    for table in &plan.table_plans {
        tables.insert(table.table.as_str());
        tables.extend(table.relations.iter().map(|r| r.target_table.as_str()));
    }
    let mut end = None;
    for table in tables {
        let desc = metadata.table(table)?;
        let hint = chunk.next_block_range_end(table, &desc.block_number_column, from_block)?;
        end = Some(end.map_or(hint, |end: u64| end.max(hint)));
    }
    end
}

/// The table whose first read range a narrow size scan can choose: the only one
/// in the plan, unfiltered, with no relations, and with no header-only blocks
/// to weigh beside it.
fn prescanned_table(plan: &Plan) -> Option<&TablePlan> {
    let [table] = plan.table_plans.as_slice() else {
        return None;
    };
    let filtered = !table.predicates.iter().all(RowPredicate::matches_every_row);
    let sized = !plan.include_all_blocks && table.relations.is_empty() && !filtered;

    sized.then_some(table)
}

/// Whether reading every output column of every matching row in the request
/// could decode more than a page (ADR-15). Relations always defer their
/// payloads; a prescanned table bounds its own first range. Header rows are read
/// for the whole range either way.
fn defers_payloads(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    budget: u64,
) -> bool {
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

    let block_desc = metadata
        .table(&plan.block_table)
        .filter(|_| chunk.has_table(&plan.block_table));
    let mut estimate = block_desc.map_or(0, |desc| {
        let columns = block_scan_columns(&plan.block_output_columns, desc);
        estimate_range_bytes(plan, chunk, &plan.block_table, desc, &columns, &[])
    });

    if prescanned_table(plan).is_none() {
        for table_plan in &plan.table_plans {
            let Some(desc) = metadata.table(&table_plan.table) else {
                return true;
            };
            let columns = resolve_output_columns(table_plan, desc);
            let bytes = estimate_range_bytes(
                plan,
                chunk,
                &table_plan.table,
                desc,
                &columns,
                &table_plan.predicates,
            );
            estimate = estimate.saturating_add(bytes);
        }
    }

    estimate > budget
}

/// What one pass over the request's range decodes for one table. A reader that
/// cannot tell counts as unbounded, which only an unbounded budget holds.
fn estimate_range_bytes(
    plan: &Plan,
    chunk: &dyn ChunkReader,
    table: &str,
    desc: &TableDescription,
    columns: &[String],
    predicates: &[RowPredicate],
) -> u64 {
    let mut request = ScanRequest::new(columns.iter().map(String::as_str).collect());
    request.predicates = predicates.iter().collect();
    request.from_block = Some(plan.from_block);
    request.to_block = plan.to_block;
    request.block_number_column = Some(&desc.block_number_column);

    chunk
        .estimate_scan_bytes(table, &request)
        .unwrap_or(u64::MAX)
}

/// A narrow size scan avoids decoding wide columns for a large unfiltered query.
/// Its estimate only chooses the first read range; exact selection can read on.
fn initial_range_end(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    budget: u64,
) -> Result<Option<u64>> {
    let Some(table) = prescanned_table(plan) else {
        return Ok(next_range_end(plan, metadata, chunk, plan.from_block));
    };
    let desc = metadata.table(&table.table).ok_or_else(|| {
        crate::engine_err!(
            crate::error::ErrorKind::TableNotFound,
            "table '{}' not found",
            table.table
        )
    })?;
    let columns = weight_scan_columns(&table.output_columns, desc);
    let mut request = ScanRequest::new(columns.iter().map(String::as_str).collect());
    request.predicates = table.predicates.iter().collect();
    request.from_block = Some(plan.from_block);
    request.to_block = plan.to_block;
    request.block_number_column = Some(&desc.block_number_column);
    let batches = chunk.scan(&table.table, &request)?;
    Ok(weight_range_end(
        &batches,
        &table.output_columns,
        desc,
        budget,
    ))
}

/// What a Children or Parents relation follows: which source addresses, and how
/// a target's address relates to them.
struct HierarchicalSpec<'a> {
    group_keys: Vec<&'a str>,
    source_address: &'a str,
    target_address: &'a str,
    mode: HierarchicalMode,
    inclusive: bool,
}

impl<'a> HierarchicalSpec<'a> {
    /// `None` for a join, and for a target without an address column.
    fn of(
        relation: &'a RelationPlan,
        source: &'a TableDescription,
        metadata: &'a DatasetDescription,
    ) -> Option<Self> {
        let mode = match relation.kind {
            RelationKind::Children => HierarchicalMode::Children,
            RelationKind::Parents => HierarchicalMode::Parents,
            RelationKind::Join => return None,
        };
        let target_address = find_address_column(metadata.table(&relation.target_table)?)?;
        let source_address = find_address_column(source).unwrap_or(target_address);
        // Cross-table relations use inclusive prefix matching because the
        // source and target address columns are different (e.g., calls.address
        // vs events.call_address). Same-table uses strict matching.
        let inclusive = source_address != target_address;

        Some(Self {
            group_keys: group_keys_for_relation(&relation.left_key, Some(target_address)),
            source_address,
            target_address,
            mode,
            inclusive,
        })
    }
}

/// The key a relation pushes down to its target scan, source columns first:
/// the join key, or a hierarchical relation's group key (its non-address
/// columns).
fn pushdown_keys<'a>(
    relation: &'a RelationPlan,
    metadata: &DatasetDescription,
) -> Option<(Vec<&'a str>, Vec<&'a str>)> {
    let (left, right) = match relation.kind {
        RelationKind::Join => (
            relation.left_key.iter().map(String::as_str).collect(),
            relation.right_key.iter().map(String::as_str).collect(),
        ),
        RelationKind::Children | RelationKind::Parents => {
            let address = metadata
                .table(&relation.target_table)
                .and_then(find_address_column);
            (
                group_keys_for_relation(&relation.left_key, address),
                group_keys_for_relation(&relation.right_key, address),
            )
        }
    };

    let usable = !left.is_empty() && !right.is_empty();
    usable.then_some((left, right))
}

/// Values built once per distinct key, side by side.
struct BuiltOnce<K, V>(Vec<(K, V)>);

impl<K: PartialEq + Send, V: Send> BuiltOnce<K, V> {
    fn build(keys: impl IntoIterator<Item = K>, build: impl Fn(&K) -> V + Sync + Send) -> Self {
        let mut distinct: Vec<K> = Vec::new();
        for key in keys {
            if !distinct.contains(&key) {
                distinct.push(key);
            }
        }

        let built = distinct
            .into_par_iter()
            .map(|key| {
                let value = build(&key);
                (key, value)
            })
            .collect();
        Self(built)
    }

    /// [`Self::build`] where a value may fail to build: the first failure in
    /// key order.
    fn try_build(
        keys: impl IntoIterator<Item = K>,
        build: impl Fn(&K) -> Result<V> + Sync + Send,
    ) -> Result<Self> {
        let built = BuiltOnce::build(keys, build).0;
        let values = built
            .into_iter()
            .map(|(key, value)| Ok((key, value?)))
            .collect::<Result<_>>()?;

        Ok(Self(values))
    }

    /// The value built for `key`, which must be one of the keys built.
    fn get(&self, key: &K) -> &V {
        let (_, value) = self
            .0
            .iter()
            .find(|(built, _)| built == key)
            .expect("a value is built for every key");
        value
    }
}

/// What one relation's target scan needs from its table's rows.
struct RelationInput<'a> {
    relation: &'a RelationPlan,
    /// The primary rows it follows: all of them, or those its own items matched.
    source: &'a [RecordBatch],
    key_filter: Option<KeyFilter>,
    hierarchical_filter: Option<HierarchicalFilter>,
}

/// Each relation's input. Relations that follow the same rows share the key
/// set and the address index built from them.
fn relation_inputs<'a>(
    table_plan: &'a TablePlan,
    table_desc: &'a TableDescription,
    metadata: &'a DatasetDescription,
    sources: &'a BuiltOnce<Option<&'a [usize]>, Vec<RecordBatch>>,
) -> Vec<RelationInput<'a>> {
    let primary_bn = table_desc.block_number_column.as_str();
    let followed = |relation: &'a RelationPlan| relation.source_items.as_deref();
    let address_spec = |relation: &'a RelationPlan| {
        HierarchicalSpec::of(relation, table_desc, metadata)
            .filter(|spec| !spec.group_keys.is_empty())
    };

    let key_sets = BuiltOnce::build(
        table_plan.relations.iter().filter_map(|relation| {
            let (left, _) = pushdown_keys(relation, metadata)?;
            Some((followed(relation), left))
        }),
        |(items, left)| KeySet::build(sources.get(items), left, primary_bn),
    );
    let address_indexes = BuiltOnce::build(
        table_plan.relations.iter().filter_map(|relation| {
            let spec = address_spec(relation)?;
            Some((followed(relation), spec.group_keys, spec.source_address))
        }),
        |(items, group_keys, address)| AddressIndex::build(sources.get(items), group_keys, address),
    );

    table_plan
        .relations
        .iter()
        .map(|relation| {
            let items = followed(relation);
            let target_bn = metadata
                .table(&relation.target_table)
                .map_or("block_number", |desc| desc.block_number_column.as_str());

            let key_filter = pushdown_keys(relation, metadata)
                .map(|(left, right)| key_sets.get(&(items, left)).filter(&right, target_bn))
                .filter(|filter| !filter.is_empty());
            let hierarchical_filter = address_spec(relation)
                .map(|spec| {
                    let index = address_indexes.get(&(items, spec.group_keys, spec.source_address));
                    index.filter(spec.target_address, spec.mode, spec.inclusive)
                })
                .filter(|filter| !filter.is_empty());

            RelationInput {
                relation,
                source: sources.get(&items),
                key_filter,
                hierarchical_filter,
            }
        })
        .collect()
}

/// Read the target rows one relation relates to its source rows.
fn scan_relation(
    input: &RelationInput,
    table_desc: &TableDescription,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    window: Window,
    (from_block, to_block): (Option<u64>, Option<u64>),
    profile: bool,
) -> Result<Rows> {
    let relation = input.relation;
    let target_desc = metadata.table(&relation.target_table);

    let output_columns = resolve_relation_output_columns(&relation.output_columns, target_desc);
    let required = target_desc
        .map(|desc| required_output_columns(&relation.output_columns, desc))
        .unwrap_or_default();
    let mut request = ScanRequest::new(output_columns.iter().map(String::as_str).collect());
    request.from_block = from_block;
    request.to_block = to_block;
    request.window = Some(window);
    request.required_columns = required.iter().map(String::as_str).collect();
    request.block_number_column = target_desc.map(|desc| desc.block_number_column.as_str());
    request.key_filter = input.key_filter.as_ref();
    request.hierarchical_filter = input.hierarchical_filter.as_ref();

    let started = profile.then(std::time::Instant::now);
    let target = chunk
        .scan_rows(&relation.target_table, &request)?
        .into_rows();
    if let Some(started) = started {
        eprintln!(
            "    {} scan: {:.2?} ({} rows)",
            relation.target_table,
            started.elapsed(),
            target.num_rows()
        );
    }

    // A filter the scan applied already kept only the related rows.
    let started = profile.then(std::time::Instant::now);
    let filtered_in_scan = match relation.kind {
        RelationKind::Join => input.key_filter.is_some(),
        RelationKind::Children | RelationKind::Parents => input.hierarchical_filter.is_some(),
    };
    let masks = if filtered_in_scan {
        None
    } else if relation.kind == RelationKind::Join {
        let left: Vec<&str> = relation.left_key.iter().map(String::as_str).collect();
        let right: Vec<&str> = relation.right_key.iter().map(String::as_str).collect();
        Some(crate::join::lookup_join_masks(
            input.source,
            &left,
            target.batches(),
            &right,
        )?)
    } else {
        let Some(spec) = HierarchicalSpec::of(relation, table_desc, metadata) else {
            return Ok(Rows::default());
        };
        let relatives = match spec.mode {
            HierarchicalMode::Children => crate::join::children_masks,
            HierarchicalMode::Parents => crate::join::parents_masks,
        };
        Some(relatives(
            input.source,
            target.batches(),
            &spec.group_keys,
            spec.source_address,
            spec.target_address,
            spec.inclusive,
        )?)
    };
    let related = match masks {
        None => target,
        Some(masks) => target.filter(|index, _| Ok(masks[index].clone()))?,
    };

    if let Some(started) = started {
        eprintln!(
            "    {} join: {:.2?}",
            relation.target_table,
            started.elapsed()
        );
    }
    Ok(related)
}

/// Read all primary and relation rows for one complete block range.
fn scan_tables(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
    from_block: u64,
    to_block: Option<u64>,
    profile: bool,
) -> Result<HashMap<String, TableOutput>> {
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

    let window = Window {
        from: Some(from_block),
        to: to_block,
    };

    // Tables are independent, and one table's scan rarely has row groups
    // enough to keep every thread busy.
    let scan_table = |table_plan: &TablePlan| -> Result<(String, TableOutput)> {
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

        let mut request = ScanRequest::new(output_col_refs);
        request.predicates = pred_refs;
        request.from_block = window.from;
        request.to_block = window.to;
        request.window = Some(window);
        request.block_number_column = Some(table_desc.block_number_column.as_str());
        request.required_columns = req_col_refs;
        // A relation some items asked for follows only the rows they matched,
        // which the scan reports as it evaluates them, once per list of items.
        for items in table_plan
            .relations
            .iter()
            .filter_map(|relation| relation.source_items.as_deref())
        {
            if !request.item_tags.contains(&items) {
                request.item_tags.push(items);
            }
        }

        let t_primary = timer!();
        let scanned = chunk.scan_rows(&table_plan.table, &request)?;
        let primary = scanned.rows().batches();
        elapsed!(
            t_primary,
            "primary scan",
            "{} rows",
            scanned.rows().num_rows()
        );

        // Compute actual block range from primary scan for cross-table pruning
        let bn_col_name = table_desc.block_number_column.as_str();
        let block_range = compute_block_range(primary, bn_col_name)?;

        let mut relations = HashMap::new();
        let has_primary_rows = primary.iter().any(|b| b.num_rows() > 0);
        if has_primary_rows && !table_plan.relations.is_empty() {
            let t_kf = timer!();
            let sources = BuiltOnce::try_build(
                table_plan
                    .relations
                    .iter()
                    .map(|relation| relation.source_items.as_deref()),
                |items| {
                    match items {
                    None => Ok(primary.to_vec()),
                    Some(items) => scanned.matched_by(items).with_context(|| {
                        format!(
                            "the chunk reader did not report which rows of '{}' items {items:?} matched",
                            table_plan.table
                        )
                    }),
                }
                },
            )?;
            let inputs = relation_inputs(table_plan, table_desc, metadata, &sources);
            elapsed!(t_kf, "key filter build");

            let t_rel = timer!();
            let related: Vec<(usize, Result<Rows>)> = inputs
                .par_iter()
                .enumerate()
                .filter_map(|(index, input)| {
                    // A relation with no rows to follow never opens its target.
                    if input.source.iter().all(|b| b.num_rows() == 0) {
                        return None;
                    }
                    let rows = scan_relation(
                        input,
                        table_desc,
                        metadata,
                        chunk,
                        window,
                        block_range,
                        profile,
                    );
                    Some((index, rows))
                })
                .collect();
            elapsed!(t_rel, "relation scans+joins");

            for (index, rows) in related {
                let rows = rows?;
                if profile {
                    let target = &table_plan.relations[index].target_table;
                    eprintln!("    {}: {} rows", target, rows.num_rows());
                }
                relations.insert(index, rows);
            }
        }

        let output = TableOutput {
            rows: scanned.into_rows(),
            relations,
        };
        Ok((table_plan.table.clone(), output))
    };

    // The first failure in plan order, the one a serial scan would report.
    let scanned: Vec<Result<(String, TableOutput)>> =
        plan.table_plans.par_iter().map(scan_table).collect();
    scanned.into_iter().collect()
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

    // 0. A missing table is an incompatible chunk, not an empty table (INV-E4).
    ensure_required_tables_present(plan, chunk)?;
    ensure_columns_renderable(plan, metadata, chunk, format)?;

    // 1. A reorg between two pages must be reported, not paved over with data
    //    from the branch the client did not ask about.
    crate::output::fork::check_parent_block(plan, metadata, chunk)?;

    // A selective query reads its rows in one pass: a second pass by position
    // costs more than the payloads it would skip.
    let selection_reader = (options.range_reads
        && defers_payloads(plan, metadata, chunk, options.weight_budget))
    .then(|| SelectionReader::new(chunk, plan, metadata))
    .flatten();
    let scan_reader: &dyn ChunkReader = selection_reader
        .as_ref()
        .map_or(chunk, |reader| reader as &dyn ChunkReader);

    // Read header identities once so internal ranges never add boundary blocks.
    let t_blocks = timer!();
    let block_table_desc = metadata.table(&plan.block_table);
    let readable_block_table = block_table_desc.filter(|_| chunk.has_table(&plan.block_table));
    let header_rows = if let Some(block_desc) = readable_block_table {
        // Block number + requested output columns + the weight companions those
        // columns declare (see `block_scan_columns`).
        let bn_col = block_desc.block_number_column.as_str();
        let block_cols = block_scan_columns(&plan.block_output_columns, block_desc);
        let block_col_vec: Vec<&str> = block_cols.iter().map(|s| s.as_str()).collect();
        let block_req_cols = required_output_columns(&plan.block_output_columns, block_desc);
        let block_req_refs: Vec<&str> = block_req_cols.iter().map(|s| s.as_str()).collect();

        let mut request = ScanRequest::new(block_col_vec);
        request.from_block = Some(plan.from_block);
        request.to_block = plan.to_block;
        request.block_number_column = Some(bn_col);
        request.required_columns = block_req_refs;

        scan_reader
            .scan_rows(&plan.block_table, &request)?
            .into_rows()
    } else {
        Rows::default()
    };

    let bn_column = block_table_desc
        .map(|d| d.block_number_column.as_str())
        .unwrap_or("number");
    let mut boundary_blocks = HashSet::default();
    collect_boundary_blocks(header_rows.batches(), bn_column, &mut boundary_blocks)?;

    let mut header_numbers = HashSet::default();
    if selection_reader.is_some() {
        collect_block_numbers(header_rows.batches(), bn_column, &mut header_numbers)?;
    }
    let mut header_numbers: Vec<_> = header_numbers.into_iter().collect();
    header_numbers.sort_unstable();

    let mut table_outputs: HashMap<String, TableOutput> = HashMap::new();
    let mut selection = BlockSelection::new(options.weight_budget);
    let mut from_block = plan.from_block;
    let mut read_through = None;
    let mut first_range = true;
    let request_end = boundary_blocks
        .iter()
        .copied()
        .max()
        .map(|last| plan.to_block.map_or(last, |end| end.min(last)))
        .or(plan.to_block);

    loop {
        let mut hint = if options.range_reads {
            if first_range {
                initial_range_end(plan, metadata, chunk, options.weight_budget)?
            } else {
                next_range_end(plan, metadata, chunk, from_block)
            }
        } else {
            None
        };
        if selection_reader.is_some() {
            // Bound each key/size pass even when row groups overlap or have no
            // statistics. Count actual headers so sparse block numbers do not
            // turn into empty scans across numeric gaps.
            // Unfiltered scans fill a page quickly; selective scans use larger
            // ranges to amortize predicate decoding over overlapping row groups.
            let blocks_per_selection = if plan
                .table_plans
                .iter()
                .any(|table| table.predicates.iter().any(RowPredicate::matches_every_row))
            {
                16
            } else {
                1024
            };
            let first = header_numbers.partition_point(|&block| block < from_block);
            if let Some(&end) = header_numbers.get(first + blocks_per_selection - 1) {
                hint = Some(hint.map_or(end, |hint| hint.min(end)));
            }
        }
        // A hint covering the whole request needs no extra block filter. Keep
        // the original bounds so a selective predicate can run first.
        let hint = hint.filter(|&end| request_end.is_none_or(|last| end < last));
        let to_block = match (hint, request_end) {
            (Some(hint), Some(end)) => Some(hint.max(from_block).min(end)),
            (Some(hint), None) => Some(hint.max(from_block)),
            (None, _) => plan.to_block,
        };
        let mut range_outputs =
            scan_tables(plan, metadata, scan_reader, from_block, to_block, profile)?;
        let in_range = |block: u64| block >= from_block && to_block.is_none_or(|end| block <= end);
        let range_headers = if first_range && (hint.is_none() || to_block == request_end) {
            header_rows.batches().to_vec()
        } else {
            header_rows
                .batches()
                .iter()
                .map(|batch| filter_to_blocks(batch, bn_column, in_range))
                .collect::<Result<Vec<_>>>()?
        };
        let mut block_numbers: HashSet<u64> = HashSet::default();
        // From table outputs
        for (table_name, output) in &range_outputs {
            let table_desc = metadata.table(table_name).unwrap();
            let bn_col = table_desc.block_number_column.as_str();
            collect_block_numbers(output.rows.batches(), bn_col, &mut block_numbers)?;

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

            for (rel_idx, rel_rows) in &output.relations {
                let Some(rel) = plan_relations.get(*rel_idx) else {
                    continue;
                };
                let rel_bn_col = metadata
                    .table(&rel.target_table)
                    .map(|d| d.block_number_column.as_str())
                    .unwrap_or(bn_col);
                collect_block_numbers(rel_rows.batches(), rel_bn_col, &mut block_numbers)?;
            }
        }

        if plan.include_all_blocks {
            collect_block_numbers(&range_headers, bn_column, &mut block_numbers)?;
        } else {
            block_numbers.extend(boundary_blocks.iter().copied().filter(|&b| in_range(b)));
        }
        let mut sorted_blocks: Vec<_> = block_numbers.into_iter().collect();
        sorted_blocks.sort_unstable();
        let exhausted = if sorted_blocks.is_empty() {
            false
        } else {
            let weights = compute_block_weights(
                &range_outputs,
                &range_headers,
                metadata,
                plan,
                &sorted_blocks,
            )?;
            selection.extend(&sorted_blocks, &weights)
        };

        if selection_reader.is_some() {
            retain_selected_keys(&mut range_outputs, plan, metadata, selection.blocks())?;
        }

        for (table, output) in range_outputs {
            let accumulated = table_outputs.entry(table).or_default();
            accumulated.rows.append(output.rows)?;
            for (relation, rows) in output.relations {
                accumulated
                    .relations
                    .entry(relation)
                    .or_default()
                    .append(rows)?;
            }
        }

        let finished = hint.is_none() || to_block == request_end;
        if exhausted || finished {
            if exhausted && !finished {
                read_through = to_block;
            }
            break;
        }
        let Some(next) = to_block.and_then(|end| end.checked_add(1)) else {
            break;
        };
        from_block = next;
        first_range = false;
    }
    let selected_blocks = selection.into_blocks();

    if selected_blocks.is_empty() {
        return Ok(match format {
            OutputFormat::Json => FmtOutput::Json(None),
            OutputFormat::Arrow { .. } => FmtOutput::Arrow(None),
        });
    }

    let block_batches = match &selection_reader {
        None => header_rows.into_batches(),
        Some(reader) => {
            reader.release_columns();
            let t_materialize = timer!();
            let (tables, headers) = rayon::join(
                || materialize_tables(&mut table_outputs, plan, metadata, chunk),
                || -> Result<Vec<RecordBatch>> {
                    let Some(desc) = block_table_desc else {
                        return Ok(header_rows.batches().to_vec());
                    };
                    let headers = retain_blocks(&header_rows, bn_column, &selected_blocks)?;
                    read_rows(
                        chunk,
                        &plan.block_table,
                        desc,
                        &headers,
                        &block_scan_columns(&plan.block_output_columns, desc),
                    )
                },
            );
            tables?;
            elapsed!(t_materialize, "materialize selected rows");
            headers?
        }
    };

    // Arrow branch: emit flat per-table IPC streams straight from the post-scan
    // batches and return, skipping the entire JSON assembly below. Columns are
    // projected to the requested output fields (+ block_number key), rows are
    // trimmed to the weight-limited `selected_blocks`, and tables fed by several
    // sources are merged + deduped to match JSON. See `crate::output::arrow_out`.
    if let OutputFormat::Arrow { compress, binary } = format {
        let selected: HashSet<u64> = selected_blocks.iter().copied().collect();
        let keep = |b: u64| selected.contains(&b);

        let tables = output_tables(plan, metadata, table_outputs);

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
        for table in &tables {
            let td = table.desc;
            let bn = td.block_number_column.clone();
            let mut emit_cols = vec![bn.clone()];
            for c in physical_output_columns(table.sources[0].fields, td) {
                if !emit_cols.contains(&c) {
                    emit_cols.push(c);
                }
            }
            let multi = table.sources.len() > 1;
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
            for source in &table.sources {
                for b in source.rows.batches() {
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
                table.name.to_string(),
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

    let header = PreparedHeader::new(block_batches, block_table_desc, &plan.block_output_columns)?;
    let tables = output_tables(plan, metadata, table_outputs)
        .into_iter()
        .filter_map(|table| PreparedTable::new(table).transpose())
        .collect::<Result<Vec<_>>>()?;

    elapsed!(
        t_blocks,
        "blocks + indexing",
        "{} blocks",
        selected_blocks.len()
    );

    elapsed!(t_total, "TOTAL (front half; blocks encode lazily)");

    // Blocks are encoded lazily, one per QueryOutput::write_next_block call.
    // See decisions/002: sequential encoding wins at production concurrency.
    Ok(FmtOutput::Json(Some(Box::new(QueryOutput::new(
        selected_blocks,
        header,
        tables,
        read_through,
    )))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::load_dataset_description;
    use crate::output::row_writer::json_close;
    use crate::output::snake_to_camel;
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

        // Only a table asked directly and pulled by another table's relation has
        // two sources, which Arrow merges on its own path. The relation leaves out
        // the rows the scan matched, so the merge finds nothing to dedup; the
        // counts catch a source it drops. The other shapes come out as one source.
        // A source without rows still sends Arrow down the merge path, while JSON
        // drops it before merging.
        let usdc = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
        let no_topic = format!("0x{}", "00".repeat(32));
        let shapes = [
            (
                "a table and its own relation",
                format!(r#""logs": [{{ "address": ["{usdc}"], "transactionLogs": true }}]"#),
            ),
            (
                "a table asked directly and pulled by another",
                format!(
                    r#""transactions": [{{ "to": ["{usdc}"], "logs": true }}],
                       "logs": [{{ "address": ["{usdc}"] }}]"#
                ),
            ),
            (
                "a table asked directly that matches nothing, pulled by another",
                format!(
                    r#""transactions": [{{ "to": ["{usdc}"], "logs": true }}],
                       "logs": [{{ "topic0": ["{no_topic}"] }}]"#
                ),
            ),
            (
                "two relations into one table",
                format!(
                    r#""traces": [{{ "type": ["call"], "callTo": ["{usdc}"], "transaction": true }}],
                       "stateDiffs": [{{ "address": ["{usdc}"], "transaction": true }}]"#
                ),
            ),
        ];

        let meta = evm_metadata();
        let chunk = evm_chunk();
        let mut wrong = Vec::new();

        for (what, items) in shapes {
            let query = format!(
                r#"{{"type": "evm", "fromBlock": 0, {items},
                    "fields": {{
                        "block": {{ "number": true }},
                        "transaction": {{ "transactionIndex": true, "hash": true }},
                        "log": {{ "logIndex": true, "transactionIndex": true }},
                        "trace": {{ "transactionIndex": true, "traceAddress": true }},
                        "stateDiff": {{ "transactionIndex": true, "address": true, "key": true }}
                    }}}}"#
            );
            let plan = compile(&parse_query(query.as_bytes(), &meta).unwrap(), &meta).unwrap();

            let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
            let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, false)
                .unwrap()
                .unwrap()
                .into_data();

            let json_rows = json_item_counts(&json);
            let arrow_rows: HashMap<String, usize> = read_arrow_frames(&arrow)
                .into_iter()
                .filter(|(name, _)| name != "blocks")
                .map(|(name, (_, rows))| (name, rows))
                .collect();

            if arrow_rows != json_rows {
                wrong.push(format!("{what}: arrow {arrow_rows:?}, json {json_rows:?}"));
            }
        }

        assert!(wrong.is_empty(), "{wrong:#?}");
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

    /// Block `gasLimit` and transaction `value` are minimal-form quantities, so
    /// many have an odd digit count. The binary rendering used to emit `null` for
    /// every one: `gasLimit` in all 224 blocks of this chunk.
    ///
    /// Covers CT-6 · INV-O14
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_arrow_binary_quantities_match_json() {
        if !crate::testing::chunks_present() {
            return;
        }

        use arrow::array::{Array, BinaryArray, UInt64Array};
        use arrow::datatypes::DataType;
        use arrow::ipc::reader::StreamReader;
        use std::io::Cursor;

        let meta = evm_metadata();
        let q = br#"{
            "type": "evm", "fromBlock": 0, "includeAllBlocks": true,
            "fields": {
                "block": { "number": true, "gasLimit": true },
                "transaction": { "transactionIndex": true, "value": true }
            },
            "transactions": [{}]
        }"#;
        let plan = compile(&parse_query(q, &meta).unwrap(), &meta).unwrap();
        let chunk = evm_chunk();

        let json = to_blocks(execute_chunk(&plan, &meta, &chunk, false).unwrap());
        let arrow = execute_chunk_arrow(&plan, &meta, &chunk, false, true)
            .unwrap()
            .unwrap()
            .into_data();

        let digits = |text: &str| {
            let d = text.trim_start_matches("0x");
            if d.len() % 2 == 1 {
                format!("0{d}")
            } else {
                d.to_string()
            }
        };
        let mut json_values: HashMap<&str, Vec<(u64, Option<String>)>> = HashMap::new();
        for block in &json {
            let number = block["header"]["number"].as_u64().unwrap();
            let gas_limit = block["header"]["gasLimit"].as_str().map(digits);
            json_values
                .entry("blocks")
                .or_default()
                .push((number, gas_limit));
            for tx in block["transactions"].as_array().into_iter().flatten() {
                let value = tx["value"].as_str().map(digits);
                json_values
                    .entry("transactions")
                    .or_default()
                    .push((number, value));
            }
        }

        let mut arrow_values: HashMap<&str, Vec<(u64, Option<String>)>> = HashMap::new();
        let mut rest = arrow.as_slice();
        while !rest.is_empty() {
            let name_len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            let name = std::str::from_utf8(&rest[4..4 + name_len])
                .unwrap()
                .to_string();
            rest = &rest[4 + name_len..];
            let payload_len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            let payload = &rest[4..4 + payload_len];
            rest = &rest[4 + payload_len..];

            let (table, block_column, column) = match name.as_str() {
                "blocks" => ("blocks", "number", "gas_limit"),
                "transactions" => ("transactions", "block_number", "value"),
                _ => continue,
            };
            for batch in StreamReader::try_new(Cursor::new(payload), None).unwrap() {
                let batch = batch.unwrap();
                let blocks = arrow::compute::cast(
                    batch.column_by_name(block_column).unwrap(),
                    &DataType::UInt64,
                )
                .unwrap();
                let blocks = blocks.as_any().downcast_ref::<UInt64Array>().unwrap();
                let values = batch.column_by_name(column).unwrap();
                let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
                for row in 0..batch.num_rows() {
                    let value = values
                        .is_valid(row)
                        .then(|| faster_hex::hex_string(values.value(row)));
                    arrow_values
                        .entry(table)
                        .or_default()
                        .push((blocks.value(row), value));
                }
            }
        }

        for table in ["blocks", "transactions"] {
            let (mut ours, mut json) = (arrow_values[table].clone(), json_values[table].clone());
            ours.sort();
            json.sort();
            assert!(
                json.iter().all(|(_, v)| v.is_some()),
                "{table}: every value is set"
            );
            assert_eq!(
                ours, json,
                "{table}: the binary rendering must carry the JSON values"
            );
        }
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
