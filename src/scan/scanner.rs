use super::addresses::HierarchicalFilter;
use super::keys::KeyFilter;
use super::positions::{ItemTags, TrackedRows};
use crate::engine_err;
use crate::error::ErrorKind;
use crate::integers::IntColumn;
use crate::scan::chunk::ParquetTable;
use crate::scan::predicate::{ColumnPredicate, RowPredicate, StatRange};
use crate::scan::rows::{Scanned, ScannedBatch};
use anyhow::{Context, Result};
use arrow::array::*;
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::error::ArrowError;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter,
};
use parquet::arrow::ProjectionMask;
use parquet::basic::Encoding;
use parquet::file::metadata::ColumnChunkMetaData;
use rayon::prelude::*;
use rustc_hash::FxHashSet as HashSet;
use std::sync::Arc;

/// A scan request: which columns to read, what predicates to apply.
#[derive(Clone)]
pub struct ScanRequest<'a> {
    /// Columns to include in the output.
    pub output_columns: Vec<&'a str>,
    /// Row predicates (multiple items ORed together).
    pub predicates: Vec<&'a RowPredicate>,
    /// Block range filter: only include rows where block_number >= from_block.
    pub from_block: Option<u64>,
    /// Block range filter: only include rows where block_number <= to_block.
    pub to_block: Option<u64>,
    /// The column name that holds the block number (for block range filtering).
    pub block_number_column: Option<&'a str>,
    /// Max rows per batch during reading.
    pub batch_size: usize,
    /// Optional key filter for join pushdown (relation scans only).
    pub key_filter: Option<&'a KeyFilter>,
    /// Optional hierarchical filter for Children/Parents relations.
    pub hierarchical_filter: Option<&'a HierarchicalFilter>,
    /// Columns that the user explicitly requested and which MUST exist in the
    /// parquet file. A missing one is a hard error (matches legacy
    /// `ColumnDoesNotExist`), as opposed to engine-internal columns that are
    /// tolerated when absent.
    pub required_columns: Vec<&'a str>,
    /// Record each returned row's absolute physical position. Only readers
    /// that support row positions can.
    pub positions: bool,
    /// Read these physical rows directly. Positions must be sorted and unique;
    /// predicates and block bounds must already have been applied by the caller.
    pub row_indices: Option<&'a [u64]>,
    /// Lists of items, by index into `predicates`: for each list the scan
    /// reports which rows one of its items matched, so that a relation follows
    /// the rows its own items matched without evaluating them again.
    pub item_tags: Vec<&'a [usize]>,
    /// Where to keep this scan's columns decoded whole, for a table several
    /// scans of the query filter: each then filters them in memory instead of
    /// decoding them again. Used when the scan's items match every row.
    pub column_cache: Option<&'a super::ColumnCache>,
    /// The block range of the query pass this scan belongs to, which holds
    /// every row the scan can return. Decoded columns keep only its rows.
    pub window: Option<super::Window>,
}

impl<'a> ScanRequest<'a> {
    pub fn new(output_columns: Vec<&'a str>) -> Self {
        Self {
            output_columns,
            predicates: Vec::new(),
            from_block: None,
            to_block: None,
            block_number_column: None,
            batch_size: usize::MAX,
            key_filter: None,
            hierarchical_filter: None,
            required_columns: Vec::new(),
            positions: false,
            row_indices: None,
            item_tags: Vec::new(),
            column_cache: None,
            window: None,
        }
    }
}

/// Determine all columns a scan must read: requested output, predicate columns,
/// the block-number column and any key or hierarchical filter columns —
/// restricted to those that actually exist in the table.
fn collect_read_columns<'a, 'b>(table: &ParquetTable, request: &'b ScanRequest<'a>) -> Vec<&'b str>
where
    'a: 'b,
{
    let mut all_columns: HashSet<&str> = HashSet::default();
    for col in &request.output_columns {
        all_columns.insert(col);
    }
    for pred in &request.predicates {
        for col in pred.required_columns() {
            all_columns.insert(col);
        }
    }
    if let Some(bn_col) = request.block_number_column {
        all_columns.insert(bn_col);
    }
    // Key filter columns must be available for RowFilter
    if let Some(kf) = &request.key_filter {
        all_columns.insert(kf.block_column());
        for col in &kf.columns {
            all_columns.insert(col);
        }
    }
    // Hierarchical filter columns
    if let Some(hf) = &request.hierarchical_filter {
        for col in &hf.group_key_columns {
            all_columns.insert(col);
        }
        all_columns.insert(&hf.address_column);
    }

    all_columns
        .into_iter()
        .filter(|c| table.column_index(c).is_some())
        .collect()
}

/// A user-requested column declared in metadata but absent from this parquet
/// file is a hard error (matches legacy `ColumnDoesNotExist`).
///
/// The same applies to a *filtered* column. Reading it is what makes the filter
/// mean anything, and a filter that cannot be evaluated does not narrow the scan
/// — it widens it to everything, which no client can detect in the response
/// (INV-X3). Every scan entry point runs this, over every filter kind: a
/// relation's join key is as load-bearing as a predicate, and an unresolvable
/// one makes the pushdown drop itself while assembly still skips the join that
/// would have corrected it.
/// Refuse a chunk whose block-number column cannot place a row, before anything
/// reads it.
///
/// This runs once per scan, at the entry both scan paths share, off metadata
/// where the metadata answers and off the column where it does not. Later is not
/// good enough, and the reason is the shape of the bug rather than an ordering
/// detail: a check that sits where the rows are produced misses the rows a
/// predicate excluded, misses the hierarchical scan that returns before reaching
/// it, and finds an absent column only in the batches that happened to project
/// it. What a per-reader check gives is one chunk erroring on a direct scan and
/// answering, short, on a relation pull — the divergence
/// [gap 31](../../spec/GAPS.md) calls terminal, arrived at from the other
/// direction.
///
/// Every row group is checked, not the ones this query selects, for the same
/// reason: a narrow range must not answer where a wide one fails.
fn ensure_block_numbers_readable(table: &ParquetTable, request: &ScanRequest) -> Result<()> {
    let Some(bn_column) = nullable_block_numbers(table.name(), table.schema(), request)? else {
        return Ok(());
    };

    let mut unstated: Vec<usize> = Vec::new();

    for rg in 0..table.num_row_groups() {
        match table.column_stats(rg, bn_column).and_then(|s| s.null_count) {
            Some(nulls) => crate::engine_ensure!(
                nulls <= 0,
                crate::error::ErrorKind::MalformedChunkData,
                "block-number column '{}' of '{}' leaves {} row(s) of row group {} without a block",
                bn_column,
                table.name(),
                nulls,
                rg
            ),
            None => unstated.push(rg),
        }
    }

    if unstated.is_empty() {
        return Ok(());
    }

    // A file that states no null count has not said there are none, and this is
    // the only place that can tell the two apart before a row is acted on.
    // Reading the column costs one narrow column of the groups that were silent,
    // and buys the promise the paragraphs above make: the same chunk answers, or
    // refuses, whatever the query. Left to the readers behind here it would
    // depend on the query — on the rows a predicate leaves, and on whether the
    // plan takes the hierarchical path, which returns before reaching any of
    // them.
    for batch in table.read(&[bn_column], Some(&unstated), 8192)? {
        let column = batch.column(0);

        crate::engine_ensure!(
            column.null_count() == 0,
            crate::error::ErrorKind::MalformedChunkData,
            "block-number column '{}' of '{}' leaves {} of {} rows without a block, \
             and the file states no null count",
            bn_column,
            table.name(),
            column.null_count(),
            column.len()
        );
    }

    Ok(())
}

/// The schema half of [`ensure_block_numbers_readable`], which every reader
/// shares: the block-number column must exist and be an integer. Returns the
/// column when its schema lets it hold a null, which only the reader can rule
/// out.
pub(super) fn nullable_block_numbers<'r>(
    table: &str,
    schema: &Schema,
    request: &ScanRequest<'r>,
) -> Result<Option<&'r str>> {
    let Some(bn_column) = request.block_number_column else {
        return Ok(None);
    };

    let Ok(index) = schema.index_of(bn_column) else {
        crate::engine_bail!(
            crate::error::ErrorKind::ColumnNotFound,
            "block-number column '{}' is not found in '{}'",
            bn_column,
            table
        );
    };

    let field = schema.field(index);
    crate::engine_ensure!(
        crate::integers::is_integer(field.data_type()),
        crate::error::ErrorKind::MalformedChunkData,
        "block-number column '{}' of '{}' is stored as {}, which is not an integer",
        bn_column,
        table,
        field.data_type()
    );

    // A column the schema marks non-nullable (parquet's REQUIRED) cannot hold a
    // null, whatever its statistics say or fail to say.
    Ok(field.is_nullable().then_some(bn_column))
}

pub(super) fn ensure_columns_present(
    table: &str,
    schema: &Schema,
    request: &ScanRequest,
) -> Result<()> {
    let mut required: Vec<&str> = request.required_columns.clone();

    for pred in &request.predicates {
        required.extend(pred.required_columns());
    }

    if let Some(kf) = &request.key_filter {
        required.push(kf.block_column());
        required.extend(kf.columns.iter().map(String::as_str));
    }

    if let Some(hf) = &request.hierarchical_filter {
        required.extend(hf.group_key_columns.iter().map(String::as_str));
        required.push(&hf.address_column);
    }

    for col in required {
        if schema.index_of(col).is_err() {
            crate::engine_bail!(
                crate::error::ErrorKind::ColumnNotFound,
                "column '{}' is not found in '{}'",
                col,
                table
            );
        }
    }

    Ok(())
}

/// Refuse a filter whose values cannot be compared against the column as this
/// chunk stores it, before any row is read (INV-E7).
///
/// Here rather than in the row filter for the same reason the block-number
/// check is: a row filter's callback can only fail with an `ArrowError`, which
/// carries no kind, and a chunk that answers or refuses depending on how many
/// rows a predicate happened to reach is the same bug from the other side.
pub(super) fn ensure_predicates_comparable(
    table: &str,
    schema: &Schema,
    request: &ScanRequest,
) -> Result<()> {
    for pred in &request.predicates {
        for col_pred in pred.column_predicates() {
            let Ok(index) = schema.index_of(&col_pred.column) else {
                continue;
            };
            let stored = schema.field(index).data_type();

            if let Err(e) =
                crate::scan::predicate::check_stored_type(col_pred.predicate.as_ref(), stored)
            {
                crate::engine_bail!(
                    ErrorKind::UnsupportedKeyType,
                    "filter on column '{}' of '{}': {}",
                    col_pred.column,
                    table,
                    e
                );
            }
        }
    }

    Ok(())
}

/// Execute a scan against a parquet table: read, filter, project.
/// Returns the filtered rows with only the output columns.
pub fn scan(table: &ParquetTable, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
    Ok(scan_rows(table, request)?.into_rows().into_batches())
}

/// [`scan`], with what the request asked to learn about the rows.
pub fn scan_rows(table: &ParquetTable, request: &ScanRequest) -> Result<Scanned> {
    let batches = scan_batches(table, request)?;
    Scanned::collect(request, batches)
}

fn scan_batches(table: &ParquetTable, request: &ScanRequest) -> Result<Vec<ScannedBatch>> {
    ensure_columns_present(table.name(), table.schema(), request)?;
    ensure_predicates_comparable(table.name(), table.schema(), request)?;
    ensure_block_numbers_readable(table, request)?;

    if let Some(rows) = request.row_indices {
        return super::positions::read_rows(table, request, rows);
    }

    // 1. Determine all columns we need to read (output + predicate + block range).
    // A cached row group is filtered in memory, so the cache needs all of them.
    let all_columns = collect_read_columns(table, request);

    // 2. Determine which row groups to scan (skip via statistics)
    let row_groups = select_row_groups(table, request);

    if row_groups.is_empty() {
        return Ok(Vec::new());
    }

    // 3. Read and filter each row group on its own, in parallel
    let output_schema = build_output_schema(table.schema(), &request.output_columns);
    // A scan that evaluates no predicate keeps every row of the window, which
    // the cache decodes once for all such scans of a pass.
    let cache = request
        .column_cache
        .filter(|_| request.predicates.iter().all(|p| p.matches_every_row()));
    let unions = ItemUnions::build(request, &row_groups);
    let scan_group = |group: &PreparedRowGroup| {
        if let Some(cache) = cache {
            let decoded = scan_decoded_row_group(
                table,
                group.index,
                &all_columns,
                request,
                &output_schema,
                cache,
            )?;
            if let Some(batches) = decoded {
                return Ok(batches);
            }
            // The cache cannot take this row group: read it as any scan does.
        }
        let union = unions.get(&group.active_items);
        scan_row_group(table, group, request, &output_schema, union)
    };

    let results: Vec<Result<Vec<ScannedBatch>>> = row_groups.par_iter().map(scan_group).collect();
    // The first failure in row group order, whichever finished first.
    let mut batches = Vec::new();
    for result in results {
        batches.extend(result?);
    }

    Ok(batches)
}

/// What a scan filters rows by beside its items, decided once, so that the
/// reader and the cache apply the same.
pub(super) struct RowFilters<'r> {
    /// The `[from, to]` blocks a row must fall in, when a filter checks them.
    pub(super) blocks: Option<(Option<u64>, Option<u64>)>,
    pub(super) relation: Relation<'r>,
}

/// How a relation scan picks its target's rows.
pub(super) enum Relation<'r> {
    /// Not a relation scan.
    None,
    /// The rows whose key the source rows hold.
    Keys(&'r KeyFilter),
    /// The rows whose address relates to a source address of their group.
    Addresses(&'r HierarchicalFilter),
}

impl<'r> RowFilters<'r> {
    pub(super) fn of(request: &ScanRequest<'r>) -> Self {
        let relation = match (request.hierarchical_filter, request.key_filter) {
            (Some(addresses), _) => Relation::Addresses(addresses),
            (None, Some(keys)) => Relation::Keys(keys),
            (None, None) => Relation::None,
        };

        // Relation keys fix the blocks of the rows they pick.
        let keyed = matches!(relation, Relation::Keys(keys) if !keys.checks_blocks_first());
        let from = request.from_block.filter(|&block| block > 0);
        let bounded = from.is_some() || request.to_block.is_some();
        let checked = bounded && !keyed && request.block_number_column.is_some();

        Self {
            blocks: checked.then_some((from, request.to_block)),
            relation,
        }
    }
}

/// Decode block bounds using the column's physical width. Wrapped signed
/// statistics can invert the bounds; such a pair must not prune any rows.
fn block_bounds(table: &ParquetTable, rg: usize, bn_column: &str) -> Option<(u64, u64)> {
    let width = table
        .schema()
        .field(table.column_index(bn_column)?)
        .data_type();
    let stats = table.column_stats(rg, bn_column)?;

    let min = crate::integers::block_number_at(width, stat_scalar(&stats.min?)?)?;
    let max = crate::integers::block_number_at(width, stat_scalar(&stats.max?)?)?;

    (min <= max).then_some((min, max))
}

/// Suggest up to four row-group ranges, merging strict overlaps to avoid
/// repeatedly decoding the same groups. Shared boundary blocks remain separate.
pub(crate) fn next_block_range_end(
    table: &ParquetTable,
    block_column: &str,
    from_block: u64,
) -> Option<u64> {
    let mut bounds = Vec::new();
    for group in 0..table.num_row_groups() {
        let (start, end) = block_bounds(table, group, block_column)?;
        if end >= from_block {
            bounds.push((start, end));
        }
    }
    bounds.sort_unstable();
    let mut ends: Vec<u64> = Vec::new();
    for (start, end) in bounds {
        if let Some(previous) = ends.last_mut() {
            if start < *previous {
                *previous = (*previous).max(end);
                continue;
            }
        }
        ends.push(end);
    }
    ends.get(ends.len().min(4).checked_sub(1)?).copied()
}

/// An upper bound on what a scan's output arrays hold, from the footer alone:
/// every row of every row group the scan would read, at what each column decodes
/// to. A group counts whole however little of its block span the request
/// covers, because its bounds say nothing about how its rows spread between
/// them. `None` when the footer cannot bound a column. ADR-15 lists what the
/// bound deliberately does not trust.
pub(crate) fn estimate_scan_bytes(
    table: &ParquetTable,
    request: &ScanRequest,
) -> Result<Option<u64>> {
    let mut leaves = Vec::new();
    for &name in &request.output_columns {
        // A column the file lacks decodes to nulls, which hold no buffers.
        let Ok(field) = table.schema().field_with_name(name) else {
            continue;
        };
        let Some(field_leaves) = field_leaves(table, field) else {
            return Ok(None);
        };
        leaves.extend(field_leaves);
    }

    let mut total = 0u64;
    for group in select_row_groups(table, request) {
        let metadata = table.row_group(group.index);
        let rows = metadata.num_rows().max(0) as u64;
        let batches = rows.div_ceil(request.batch_size.max(1) as u64);

        for &(leaf, cost) in &leaves {
            let Some(bytes) = decoded_bytes(metadata.column(leaf), cost, batches) else {
                return Ok(None);
            };
            total = total.saturating_add(bytes);
        }
    }

    Ok(Some(total))
}

/// The most column `index` of row group `group` decodes to, every row of it;
/// `None` where the footer does not bound it.
pub(super) fn column_bytes_bound(table: &ParquetTable, group: usize, index: usize) -> Option<u64> {
    let metadata = table.row_group(group);

    field_leaves(table, table.schema().field(index))?
        .into_iter()
        .try_fold(0u64, |total, (leaf, cost)| {
            Some(total.saturating_add(decoded_bytes(metadata.column(leaf), cost, 1)?))
        })
}

/// The parquet leaves of a root column, each with what one of its level
/// entries decodes to; `None` for a type whose decoded layout is not modelled.
fn field_leaves(
    table: &ParquetTable,
    field: &arrow::datatypes::Field,
) -> Option<Vec<(usize, LeafCost)>> {
    let parquet = table.metadata().file_metadata().schema_descr();
    let mut costs = Vec::new();
    leaf_costs(field.data_type(), LeafCost::default(), &mut costs)?;

    let indices: Vec<usize> = (0..parquet.num_columns())
        .filter(|&leaf| &parquet.column(leaf).path().parts()[0] == field.name())
        .collect();
    (indices.len() == costs.len()).then(|| indices.into_iter().zip(costs).collect())
}

/// What one level entry of a parquet leaf decodes to, beside the contents of
/// its byte strings.
#[derive(Clone, Copy, Default)]
struct LeafCost {
    /// Fixed-width values and the offsets of the leaf and every list above it.
    bytes: u64,
    /// A validity bit for every level, and a boolean's value.
    bits: u64,
    byte_strings: bool,
}

/// The cost of each parquet leaf under `data_type`, in schema order. `None` for
/// a type whose decoded layout this does not model.
fn leaf_costs(data_type: &DataType, above: LeafCost, out: &mut Vec<LeafCost>) -> Option<()> {
    let level = LeafCost {
        bits: above.bits + 1,
        ..above
    };
    let with_offsets = |width: u64| LeafCost {
        bytes: level.bytes + width,
        ..level
    };

    match data_type {
        DataType::List(item) | DataType::Map(item, _) => {
            leaf_costs(item.data_type(), with_offsets(4), out)
        }
        DataType::LargeList(item) => leaf_costs(item.data_type(), with_offsets(8), out),
        DataType::FixedSizeList(item, _) => leaf_costs(item.data_type(), level, out),
        DataType::Struct(fields) => fields
            .iter()
            .try_for_each(|field| leaf_costs(field.data_type(), level, out)),
        DataType::Utf8 | DataType::Binary => {
            out.push(LeafCost {
                byte_strings: true,
                ..with_offsets(4)
            });
            Some(())
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            out.push(LeafCost {
                byte_strings: true,
                ..with_offsets(8)
            });
            Some(())
        }
        DataType::Boolean => {
            out.push(LeafCost {
                bits: level.bits + 1,
                ..level
            });
            Some(())
        }
        DataType::Null => {
            out.push(level);
            Some(())
        }
        DataType::FixedSizeBinary(width) => {
            out.push(with_offsets((*width).max(0) as u64));
            Some(())
        }
        other => {
            out.push(with_offsets(other.primitive_width()? as u64));
            Some(())
        }
    }
}

/// What one column chunk decodes to: its level entries at `cost`, a bitmap per
/// level rounded up to whole bytes in every batch, and its byte strings.
/// Saturating, because a damaged footer can claim any count and a cost hint must
/// not be what panics on it.
fn decoded_bytes(column: &ColumnChunkMetaData, cost: LeafCost, batches: u64) -> Option<u64> {
    let entries = column.num_values().max(0) as u64;

    let fixed = entries.saturating_mul(cost.bytes);
    let bitmaps = entries
        .saturating_mul(cost.bits)
        .div_ceil(8)
        .saturating_add(cost.bits.saturating_mul(batches));
    let contents = if cost.byte_strings {
        byte_string_bytes(column)?
    } else {
        0
    };

    Some(fixed.saturating_add(bitmaps).saturating_add(contents))
}

/// The bytes a byte-string column chunk's values hold, where the footer bounds
/// them: the writer's count, or the stored pages of an encoding that keeps every
/// value whole. A dictionary keeps a repeated value once and prefix compression
/// a shared prefix once, and no statistic bounds what they expand to: bounds are
/// values rather than lengths, and a writer may truncate or omit them.
fn byte_string_bytes(column: &ColumnChunkMetaData) -> Option<u64> {
    if let Some(bytes) = column.unencoded_byte_array_data_bytes() {
        return Some(bytes.max(0) as u64);
    }

    // A dictionary the footer does not point at still shows in the encodings.
    let encodings = column.encodings();
    let keeps_values_whole = encodings.iter().all(|encoding| {
        matches!(
            encoding,
            Encoding::PLAIN | Encoding::DELTA_LENGTH_BYTE_ARRAY | Encoding::RLE
        )
    });
    let whole =
        !encodings.is_empty() && keeps_values_whole && column.dictionary_page_offset().is_none();

    whole.then(|| column.uncompressed_size().max(0) as u64)
}

/// A row group a scan reads, with the items its statistics leave to run on it.
struct PreparedRowGroup {
    index: usize,
    /// Indices into the request's predicates, ascending; empty when the scan
    /// has none.
    active_items: Vec<usize>,
    /// Each active item some of whose filters the statistics prove, with the
    /// filters left to check; ascending, as `active_items` is.
    narrowed: Vec<(usize, RowPredicate)>,
}

impl PreparedRowGroup {
    /// The active items as this row group checks them.
    fn items<'r>(&'r self, request: &ScanRequest<'r>) -> Vec<&'r RowPredicate> {
        let mut narrowed = self.narrowed.iter().peekable();

        self.active_items
            .iter()
            .map(|&item| match narrowed.next_if(|(n, _)| *n == item) {
                Some((_, left)) => left,
                None => request.predicates[item],
            })
            .collect()
    }
}

fn select_row_groups(table: &ParquetTable, request: &ScanRequest) -> Vec<PreparedRowGroup> {
    (0..table.num_row_groups())
        .filter_map(|index| prepare_row_group(table, request, index))
        .collect()
}

/// Row group `index` as `request` reads it, or `None` when its statistics rule
/// out every row the scan could return.
///
/// Bounds the file does not state, or states in a way no reader can trust,
/// prune nothing: reading the group costs time, skipping it costs the rows, and
/// it costs them silently.
fn prepare_row_group(
    table: &ParquetTable,
    request: &ScanRequest,
    index: usize,
) -> Option<PreparedRowGroup> {
    let block_bounds_of = |column| block_bounds(table, index, column);

    if let Some((min, max)) = request.block_number_column.and_then(block_bounds_of) {
        let before = request.from_block.is_some_and(|from| max < from);
        let after = request.to_block.is_some_and(|to| min > to);
        if before || after {
            return None;
        }
    }

    if let Some(keys) = request.key_filter {
        if let Some((min, max)) = block_bounds_of(keys.block_column()) {
            if !keys.has_block_within(min, max) {
                return None;
            }
        }
    }

    // An item whose statistics rule this row group out does not run on it.
    let stats = GroupStats::read(table, index, &request.predicates);
    let range = |column: &str| stats.range(column).cloned();
    let active_items: Vec<usize> = (0..request.predicates.len())
        .filter(|&item| !request.predicates[item].can_skip_row_group(&range))
        .collect();
    if !request.predicates.is_empty() && active_items.is_empty() {
        return None;
    }

    // A filter the statistics prove for every row is not evaluated on it.
    let narrowed = active_items
        .iter()
        .filter_map(|&item| {
            let left = request.predicates[item].without_proven(|p| stats.proves(p))?;
            Some((item, left))
        })
        .collect();

    Some(PreparedRowGroup {
        index,
        active_items,
        narrowed,
    })
}

/// One row group's statistics for the columns a scan's items filter on, each
/// read once, at the type the column is stored at. A hundred items name the
/// same few columns, and reading a statistic allocates.
struct GroupStats<'p>(Vec<ColumnSummary<'p>>);

struct ColumnSummary<'p> {
    column: &'p str,
    range: Option<StatRange>,
    /// The writer counted the column's nulls in the group, and found none.
    null_free: bool,
}

impl<'p> GroupStats<'p> {
    fn read(table: &ParquetTable, group: usize, predicates: &[&'p RowPredicate]) -> Self {
        let mut columns: Vec<ColumnSummary<'p>> = Vec::new();
        for predicate in predicates.iter().flat_map(|p| p.column_predicates()) {
            let column = predicate.column.as_str();
            if columns.iter().all(|read| read.column != column) {
                columns.push(column_summary(table, group, column));
            }
        }

        Self(columns)
    }

    fn range(&self, column: &str) -> Option<&StatRange> {
        self.0.iter().find(|c| c.column == column)?.range.as_ref()
    }

    /// Whether every row of the group holds a value `predicate` matches. A
    /// null satisfies no filter (INV-P7), so a column that may hold one
    /// proves nothing.
    fn proves(&self, predicate: &ColumnPredicate) -> bool {
        let Some(summary) = self.0.iter().find(|c| c.column == predicate.column) else {
            return false;
        };
        let range = summary.range.as_ref().filter(|_| summary.null_free);

        range.is_some_and(|range| predicate.predicate.matches_every_value(range))
    }
}

fn column_summary<'p>(table: &ParquetTable, group: usize, column: &'p str) -> ColumnSummary<'p> {
    let stats = table.column_stats(group, column);
    let null_free = stats.as_ref().is_some_and(|s| s.null_count == Some(0));
    let range = stats.and_then(|stats| {
        // Parquet stats come at the physical type; the stored type says how to read them.
        let stored = table.schema().field_with_name(column).ok()?.data_type();
        let (min, max) = (stats.min?, stats.max?);
        StatRange::new(
            stored,
            stat_value_to_array(&min).as_ref(),
            stat_value_to_array(&max).as_ref(),
        )
    });

    ColumnSummary {
        column,
        range,
        null_free,
    }
}

/// For each set of items that runs on some row group, the list of every value
/// they list, which admits the rows they could match to the stage that runs
/// them. Built before the row groups are read: it depends on the items alone,
/// and a list of twenty thousand addresses is costly to copy.
struct ItemUnions(rustc_hash::FxHashMap<Vec<usize>, Option<Arc<RowPredicate>>>);

impl ItemUnions {
    fn build(request: &ScanRequest, row_groups: &[PreparedRowGroup]) -> Self {
        let mut sets: HashSet<&[usize]> = HashSet::default();
        for group in row_groups
            .iter()
            .filter(|group| group.active_items.len() > 1)
        {
            sets.insert(&group.active_items);
        }

        if sets.is_empty() {
            return Self(Default::default());
        }

        let sets: Vec<&[usize]> = sets.into_iter().collect();
        let unions = sets
            .into_par_iter()
            .map(|active| {
                let items: Vec<&RowPredicate> = active
                    .iter()
                    .map(|&item| request.predicates[item])
                    .collect();
                let union = crate::scan::predicate::listed_union(&items).map(Arc::new);
                (active.to_vec(), union)
            })
            .collect();

        Self(unions)
    }

    /// The union for the `active` items, when they are several and list values.
    fn get(&self, active: &[usize]) -> Option<Arc<RowPredicate>> {
        self.0.get(active).cloned().flatten()
    }
}

/// Read one row group through a parquet reader whose row filter applies the
/// scan's filters, and project what it returns to the output columns.
fn scan_row_group(
    table: &ParquetTable,
    group: &PreparedRowGroup,
    request: &ScanRequest,
    output_schema: &SchemaRef,
    union: Option<Arc<RowPredicate>>,
) -> Result<Vec<ScannedBatch>> {
    let filter = reader_filter(table, request, group, union);

    read_row_group(table, group.index, request, output_schema, filter)
}

/// How a reader picks a row group's rows.
struct ReaderFilter {
    /// The row filter's stages, in the order the reader runs them. Each reads
    /// only its own columns, and only for the rows the stages before it kept.
    stages: Vec<Box<dyn ArrowPredicate>>,
    /// Block bounds checked on the rows the reader returns, not by a stage.
    blocks_after: Option<(Option<u64>, Option<u64>)>,
    /// Which rows each tag's items matched, recorded by the last stage.
    tags: Option<Arc<ItemTags>>,
}

/// The reader filter that applies `request` to row group `group`, admitting
/// rows by the `union` of its active items when they have one.
fn reader_filter(
    table: &ParquetTable,
    request: &ScanRequest,
    group: &PreparedRowGroup,
    union: Option<Arc<RowPredicate>>,
) -> ReaderFilter {
    let filters = RowFilters::of(request);

    if let Relation::Addresses(addresses) = filters.relation {
        // Hierarchical filter and predicates are structurally mutually exclusive:
        // predicates apply to primary scans, hierarchical filters to relation scans.
        assert!(
            request.predicates.is_empty(),
            "hierarchical_filter and predicates must not be set simultaneously"
        );

        return ReaderFilter {
            stages: vec![address_stage(table, addresses)],
            blocks_after: filters.blocks,
            tags: None,
        };
    }

    let mut stages = Vec::new();
    if let Some(((from, to), column)) = filters.blocks.zip(request.block_number_column) {
        stages.push(block_stage(table, column, from, to));
    }
    if let Relation::Keys(keys) = filters.relation {
        stages.push(key_stage(table, keys));
    }

    let items = group.items(request);
    let tags = (!items.is_empty())
        .then(|| ItemTags::new(request, &group.active_items))
        .flatten();
    stages.extend(item_stages(table, &items, tags.clone(), union));

    ReaderFilter {
        stages,
        blocks_after: None,
        tags,
    }
}

/// The projection reading `columns`, those the table has.
fn roots<'c>(table: &ParquetTable, columns: impl IntoIterator<Item = &'c str>) -> ProjectionMask {
    let indices = columns
        .into_iter()
        .filter_map(|name| table.schema().index_of(name).ok());

    ProjectionMask::roots(table.metadata().file_metadata().schema_descr(), indices)
}

fn block_stage(
    table: &ParquetTable,
    column: &str,
    from: Option<u64>,
    to: Option<u64>,
) -> Box<dyn ArrowPredicate> {
    let name = column.to_string();

    Box::new(ArrowPredicateFn::new(
        roots(table, [column]),
        move |batch: RecordBatch| {
            let Some(column) = batch.column_by_name(&name) else {
                return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
            };

            block_range_mask(column, from, to)
                .map_err(|e| ArrowError::InvalidArgumentError(e.to_string()))
        },
    ))
}

fn key_stage(table: &ParquetTable, keys: &KeyFilter) -> Box<dyn ArrowPredicate> {
    let projection = roots(table, keys.columns.iter().map(String::as_str));
    let keys = keys.clone();

    Box::new(ArrowPredicateFn::new(
        projection,
        move |batch: RecordBatch| keys.mask(&batch, None),
    ))
}

/// One stage reads the group keys and the addresses together, and drops most
/// rows before the reader decodes an output column. The alternatives measured
/// slower: see "Hierarchical Filter Approach Comparison" in
/// docs/architecture.md.
fn address_stage(table: &ParquetTable, addresses: &HierarchicalFilter) -> Box<dyn ArrowPredicate> {
    let columns = addresses
        .group_key_columns
        .iter()
        .chain([&addresses.address_column]);
    let projection = roots(table, columns.map(String::as_str));
    let addresses = addresses.clone();

    Box::new(ArrowPredicateFn::new(
        projection,
        move |batch: RecordBatch| Ok(addresses.mask(&batch, None)),
    ))
}

/// The stages that evaluate `items`. Tags need each item's own answer, which
/// only a stage that evaluates all of them has.
fn item_stages(
    table: &ParquetTable,
    items: &[&RowPredicate],
    tags: Option<Arc<ItemTags>>,
    union: Option<Arc<RowPredicate>>,
) -> Vec<Box<dyn ArrowPredicate>> {
    match items {
        [] => Vec::new(),
        [item] if tags.is_none() => lone_item_stages(table, item),
        _ => every_item_stages(table, items, tags, union),
    }
}

/// A lone item's first column gets a stage of its own, and its other columns
/// share a second, which decodes only the rows the first kept. The first is the
/// client's first filter, which need not be the most selective; a filter the
/// row group's statistics prove is already left out.
fn lone_item_stages(table: &ParquetTable, item: &RowPredicate) -> Vec<Box<dyn ArrowPredicate>> {
    let mut stages: Vec<Box<dyn ArrowPredicate>> = Vec::new();
    let (first, rest) = match item.columns.split_first() {
        Some((first, rest)) => (Some(first), rest),
        None => (None, &[][..]),
    };

    if let Some(first) = first {
        let name = first.column.clone();
        let evaluator = first.predicate.clone();
        stages.push(Box::new(ArrowPredicateFn::new(
            roots(table, [first.column.as_str()]),
            move |batch: RecordBatch| {
                let Some(column) = batch.column_by_name(&name) else {
                    return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
                };

                evaluator
                    .evaluate(column.as_ref())
                    .map_err(|e| ArrowError::ComputeError(e.to_string()))
            },
        )));
    }

    let rest = RowPredicate::with_alternatives(rest.to_vec(), item.alternatives.clone());
    if !rest.matches_every_row() {
        stages.push(Box::new(ArrowPredicateFn::new(
            roots(table, rest.required_columns()),
            move |batch: RecordBatch| {
                rest.evaluate(&batch)
                    .map_err(|e| ArrowError::ComputeError(e.to_string()))
            },
        )));
    }

    stages
}

/// Several items, or items whose tags are asked for: one stage evaluates them
/// all, after a stage that admits only the rows `union` admits, every row one
/// of them could match.
fn every_item_stages(
    table: &ParquetTable,
    items: &[&RowPredicate],
    tags: Option<Arc<ItemTags>>,
    union: Option<Arc<RowPredicate>>,
) -> Vec<Box<dyn ArrowPredicate>> {
    let mut stages: Vec<Box<dyn ArrowPredicate>> = Vec::new();

    // An item that matches every row admits every row, to the union as well.
    // Only tags still need each item's answer.
    let every_row = items.iter().any(|item| item.matches_every_row());
    if every_row && tags.is_none() {
        return stages;
    }

    if let Some(union) = union.filter(|_| !every_row) {
        stages.push(Box::new(ArrowPredicateFn::new(
            roots(table, union.required_columns()),
            move |batch: RecordBatch| {
                union
                    .evaluate(&batch)
                    .map_err(|e| ArrowError::ComputeError(e.to_string()))
            },
        )));
    }

    let projection = roots(table, items.iter().flat_map(|item| item.required_columns()));
    let items: Vec<RowPredicate> = items.iter().map(|&item| item.clone()).collect();
    let every_item: Vec<usize> = (0..items.len()).collect();

    // Last, so that the rows it selects are the rows read.
    stages.push(Box::new(ArrowPredicateFn::new(
        projection,
        move |batch: RecordBatch| {
            let masks = items
                .iter()
                .map(|item| item.evaluate(&batch))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| ArrowError::ComputeError(e.to_string()))?;
            let matched = crate::scan::predicate::or_masks(&masks, &every_item, batch.num_rows());

            if let Some(tags) = &tags {
                tags.record(&masks, &matched);
            }
            Ok(matched)
        },
    )));

    stages
}

/// The columns a reader decodes for the rows its row filter keeps: the output,
/// and the block number when `filter` checks block bounds on those rows. The
/// stages decode their own columns, and one decoded again for the rows kept
/// would only be dropped from the output.
fn reader_columns<'a, 'r>(
    request: &'a ScanRequest<'r>,
    filter: &ReaderFilter,
) -> impl Iterator<Item = &'r str> + 'a {
    let block = request
        .block_number_column
        .filter(|_| filter.blocks_after.is_some());

    request.output_columns.iter().copied().chain(block)
}

/// Read row group `group` through `filter`. Each row comes with its position
/// and tags when the request asks for them.
fn read_row_group(
    table: &ParquetTable,
    group: usize,
    request: &ScanRequest,
    output_schema: &SchemaRef,
    filter: ReaderFilter,
) -> Result<Vec<ScannedBatch>> {
    let projection = roots(table, reader_columns(request, &filter));
    let mut tracked = request.positions.then(TrackedRows::default);
    let stages = match &mut tracked {
        Some(tracked) => tracked.wrap(filter.stages),
        None => filter.stages,
    };

    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        table.data(),
        table.arrow_metadata().clone(),
    )
    .with_projection(projection)
    .with_batch_size(request.batch_size)
    .with_row_groups(vec![group]);
    if !stages.is_empty() {
        builder = builder.with_row_filter(RowFilter::new(stages));
    }
    let reader = builder.build().context("building parquet reader")?;
    let positions = tracked.map(|tracked| tracked.finish(table, group));
    let tags = filter.tags.map(|tags| tags.finish());

    let mut batches = Vec::new();
    let mut rows_read = 0;
    for batch in reader {
        let batch = batch.context("reading batch")?;
        let rows = batch.num_rows();
        if rows == 0 {
            continue;
        }

        let projected = project_batch(&batch, output_schema)?;
        let positions = positions
            .as_ref()
            .map(|positions| positions.slice(rows_read, rows));
        let mut scanned = match &tags {
            Some(tags) => ScannedBatch {
                batch: projected,
                positions,
                tags: tags.iter().map(|tag| tag.slice(rows_read, rows)).collect(),
            },
            None => ScannedBatch::untagged(request, projected, positions),
        };
        rows_read += rows;

        let block_column = request
            .block_number_column
            .and_then(|name| batch.column_by_name(name));
        if let Some(((from, to), column)) = filter.blocks_after.zip(block_column) {
            scanned = scanned.filter(&block_range_mask(column, from, to)?)?;
        }

        if scanned.batch.num_rows() > 0 {
            batches.push(scanned);
        }
    }

    Ok(batches)
}

/// One row group of a scan whose items match every row: the rows inside the
/// pass's window decoded once for every scan that shares `cache`, and the
/// scan's block range, key and hierarchical filters applied in memory. Rows
/// come out as [`read_row_group`] returns them: a relation's rows share its
/// sources' blocks (INV-D5), all inside the window. `None` when the cache
/// cannot take the row group.
fn scan_decoded_row_group(
    table: &ParquetTable,
    group: usize,
    read_columns: &[&str],
    request: &ScanRequest,
    output_schema: &SchemaRef,
    cache: &super::ColumnCache,
) -> Result<Option<Vec<ScannedBatch>>> {
    let window = request.window.unwrap_or_default();
    let found = cache.window_rows(table, group, window, || {
        window_rows(table, group, window, request.block_number_column)
    })?;
    let Some(window_rows) = found else {
        return Ok(None);
    };
    let window_rows = &window_rows.value;
    let rows = window_rows.len();

    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for &name in read_columns {
        let Ok(index) = table.schema().index_of(name) else {
            continue;
        };
        let Some(column) = cache.column(table, group, index, window, window_rows)? else {
            return Ok(None);
        };
        fields.push(table.schema().field(index).clone());
        columns.push(column);
    }
    let batch = RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )?;

    let filters = RowFilters::of(request);
    let mut keep = BooleanBuffer::new_set(rows);
    let block_column = request
        .block_number_column
        .and_then(|name| batch.column_by_name(name));
    if let Some(((from, to), column)) = filters.blocks.zip(block_column) {
        keep = &keep & block_range_mask(column, from, to)?.values();
    }

    match filters.relation {
        Relation::Addresses(hf) => {
            keep = &keep & hf.mask(&batch, Some(&keep)).values();
        }
        Relation::Keys(kf) => {
            keep = &keep & kf.mask(&batch, Some(&keep))?.values();
        }
        Relation::None => {}
    }

    let kept = keep.count_set_bits();
    if kept == 0 {
        return Ok(Some(Vec::new()));
    }
    let selected = BooleanArray::new(keep, None);
    let batch =
        arrow::compute::filter_record_batch(&project_batch(&batch, output_schema)?, &selected)?;

    let positions = request.positions.then(|| {
        let start = table.row_group_start(group);
        selected
            .values()
            .set_indices()
            .map(|row| start + window_rows.offset(row) as u64)
            .collect::<UInt64Array>()
    });
    // Every item matches every row it reads, so a tag holds wherever one of
    // its items runs.
    let tags = request
        .item_tags
        .iter()
        .map(|items| BooleanArray::from(vec![!items.is_empty(); kept]))
        .collect();

    let scanned = ScannedBatch {
        batch,
        positions,
        tags,
    };
    Ok(Some(scanned.split(request.batch_size)))
}

/// Block numbers read at a time while a window's rows are found.
const WINDOW_BATCH: usize = 1 << 16;

/// The rows of row group `group` inside `window`, found a batch of block
/// numbers at a time.
fn window_rows(
    table: &ParquetTable,
    group: usize,
    window: super::Window,
    block_column: Option<&str>,
) -> Result<super::columns::WindowRows> {
    let rows = table.row_group(group).num_rows() as usize;
    let bounded = window.from.is_some_and(|b| b > 0) || window.to.is_some();
    let block_index = block_column
        .and_then(|name| table.schema().index_of(name).ok())
        .filter(|_| bounded);
    let Some(index) = block_index else {
        return Ok(super::columns::WindowRows::All(rows));
    };

    let projection =
        ProjectionMask::roots(table.metadata().file_metadata().schema_descr(), [index]);
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
        table.data(),
        table.arrow_metadata().clone(),
    )
    .with_projection(projection)
    .with_row_groups(vec![group])
    .with_batch_size(WINDOW_BATCH)
    .build()?;

    let mut masks = Vec::new();
    for batch in reader {
        let blocks = batch?.column(0).clone();
        let inside = block_range_mask(&blocks, window.from.filter(|&b| b > 0), window.to)?;
        masks.push(match inside.nulls() {
            Some(nulls) => inside.values() & nulls.inner(),
            None => inside.values().clone(),
        });
    }

    // Counted first, so the offsets take no spare capacity.
    let inside: usize = masks.iter().map(BooleanBuffer::count_set_bits).sum();
    if inside == rows {
        return Ok(super::columns::WindowRows::All(rows));
    }
    let mut offsets = Vec::with_capacity(inside);
    let mut start = 0u32;
    for mask in &masks {
        offsets.extend(mask.set_indices().map(|row| start + row as u32));
        start += mask.len() as u32;
    }

    Ok(super::columns::WindowRows::Some(offsets.into()))
}

/// Rows whose block number falls in `[from_block, to_block]`.
///
/// A block number is read as every reader of the column reads it
/// ([`IntColumn::block_number`]): a signed width as unsigned, so a negative
/// value is a block past the signed ceiling rather than one before block 0. A
/// declared `uint64` bounds the values and not the storage, so every integer
/// width a writer may choose is read (INV-D7), and a bound the width cannot
/// hold is not truncated into it: a `from` above the width's ceiling matches
/// nothing, and a `to` above it constrains nothing (INV-P14).
pub(super) fn block_range_mask(
    column: &Arc<dyn Array>,
    from_block: Option<u64>,
    to_block: Option<u64>,
) -> Result<BooleanArray> {
    // Returning all-true here would leak every out-of-range row of the batch,
    // and the client cannot tell (INV-B1). A block number column that is not an
    // integer is a chunk disagreeing with its catalog.
    let Some(blocks) = IntColumn::resolve(column.as_ref()) else {
        return Err(engine_err!(
            ErrorKind::UnsupportedKeyType,
            "block number column is stored as {:?}, which is not an integer",
            column.data_type()
        ));
    };

    Ok(blocks.block_numbers_within(from_block, to_block)?)
}

/// Project a RecordBatch to only include the given output columns.
pub(super) fn project_batch(batch: &RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
    let columns: Vec<Arc<dyn Array>> = output_schema
        .fields()
        .iter()
        .map(|field| {
            batch
                .column_by_name(field.name())
                .cloned()
                .unwrap_or_else(|| Arc::new(NullArray::new(batch.num_rows())))
        })
        .collect();

    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

/// Build the output Arrow schema from requested column names.
pub(super) fn build_output_schema(table_schema: &SchemaRef, columns: &[&str]) -> SchemaRef {
    let fields: Vec<_> = columns
        .iter()
        .filter_map(|name| table_schema.field_with_name(name).ok().cloned())
        .collect();
    Arc::new(Schema::new(fields))
}

/// Convert a StatValue to u64 (for block range comparisons).
/// The scalar an integer column's row-group statistic carries.
///
/// Parquet has no physical integer narrower than 32 bits, so this is where a
/// `UInt16` column's statistic arrives too, sign-extended. What the bits mean is
/// the column's width to say, and the caller asks it.
fn stat_scalar(value: &crate::scan::chunk::StatValue) -> Option<i64> {
    use crate::scan::chunk::StatValue;
    match value {
        StatValue::Int32(v) => Some(*v as i64),
        StatValue::Int64(v) => Some(*v),
        _ => None,
    }
}

/// Convert a StatValue to a single-element Arrow array (for predicate can_skip).
fn stat_value_to_array(value: &crate::scan::chunk::StatValue) -> Arc<dyn Array> {
    use crate::scan::chunk::StatValue;
    match value {
        StatValue::Boolean(v) => Arc::new(BooleanArray::from(vec![*v])),
        StatValue::Int32(v) => Arc::new(Int32Array::from(vec![*v])),
        StatValue::Int64(v) => Arc::new(Int64Array::from(vec![*v])),
        StatValue::Float(v) => Arc::new(Float32Array::from(vec![*v])),
        StatValue::Double(v) => Arc::new(Float64Array::from(vec![*v])),
        StatValue::ByteArray(v) => match std::str::from_utf8(v) {
            Ok(text) => Arc::new(StringArray::from(vec![text])),
            // Bytes no string comparison can order. Rendered as `""` they sort
            // below every filter value, and the group is pruned on that alone.
            Err(_) => Arc::new(BinaryArray::from(vec![v.as_slice()])),
        },
        StatValue::FixedLenByteArray(v) => {
            let len = v.len() as i32;
            let mut builder = FixedSizeBinaryBuilder::with_capacity(1, len);
            builder.append_value(v).unwrap();
            Arc::new(builder.finish())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scan::addresses::HierarchicalMode;
    use crate::scan::predicate::InListPredicate;
    use std::path::Path;

    /// A byte statistic no string can carry prunes nothing. Rendered as `""` it
    /// sorts below every filter value, and the row group goes with it.
    ///
    /// Covers CT-3 · INV-P16
    #[test]
    fn a_statistic_that_is_not_text_prunes_nothing() {
        use crate::scan::chunk::StatValue;
        use crate::scan::predicate::{ArrayPredicate, StatRange};
        use arrow::datatypes::DataType;

        let raw = StatValue::ByteArray(vec![0xff, 0xfe]);
        let stat = stat_value_to_array(&raw);
        assert!(
            StatRange::new(&DataType::Utf8, stat.as_ref(), stat.as_ref()).is_none(),
            "unreadable bounds are no bounds"
        );

        let text = stat_value_to_array(&StatValue::ByteArray(b"0xabc".to_vec()));
        let range = StatRange::new(&DataType::Utf8, text.as_ref(), text.as_ref())
            .expect("valid utf8 still reads");
        assert!(InListPredicate::from_strings(&["0xdef"]).can_skip(&range));
        assert!(!InListPredicate::from_strings(&["0xabc"]).can_skip(&range));
    }

    use crate::testing::{evm_chunk_path, solana_chunk_path};

    // --- A3: block_range_mask must filter Int64/UInt16/Int16 columns ---

    /// A block range keeps the rows whose block number, as every other reader
    /// of the column takes it, falls inside it: a signed width read as
    /// unsigned, so a negative value is a block past the signed ceiling.
    #[test]
    fn a_block_range_reads_block_numbers_as_every_reader_does() {
        use crate::integers::IntColumn;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![0, 1, 5, i8::MAX, i8::MIN, -1])),
            Arc::new(Int16Array::from(vec![0, 7, i16::MAX, i16::MIN, -1])),
            Arc::new(Int32Array::from(vec![0, 5, 7, i32::MAX, i32::MIN, -2, -1])),
            Arc::new(Int64Array::from(vec![0, 5, i64::MAX, i64::MIN, -1])),
            Arc::new(UInt8Array::from(vec![0, 5, u8::MAX])),
            Arc::new(UInt16Array::from(vec![0, 5, u16::MAX])),
            Arc::new(UInt32Array::from(vec![0, 5, u32::MAX])),
            Arc::new(UInt64Array::from(vec![0, 5, u64::MAX])),
        ];
        let edges = [
            0,
            1,
            5,
            6,
            1 << 7,
            1 << 8,
            1 << 15,
            1 << 16,
            (1 << 31) - 1,
            1 << 31,
            u32::MAX as u64,
            1 << 32,
            1 << 63,
            u64::MAX,
        ];
        let bounds = || std::iter::once(None).chain(edges.into_iter().map(Some));

        for column in &columns {
            let blocks = IntColumn::resolve(column.as_ref()).unwrap();
            for from in bounds() {
                for to in bounds() {
                    let mask = block_range_mask(column, from, to).unwrap();

                    for row in 0..column.len() {
                        let block = blocks.block_number(row);
                        let inside = from.is_none_or(|from| block >= from)
                            && to.is_none_or(|to| block <= to);
                        assert_eq!(
                            mask.value(row),
                            inside,
                            "{} row {row} in {from:?}..={to:?}",
                            column.data_type()
                        );
                    }
                }
            }
        }
    }

    /// Calls in blocks 1 to 6, one transaction each, a root call and its child.
    fn calls_table(dir: &Path) -> (ParquetTable, RecordBatch) {
        use arrow::datatypes::UInt32Type;
        let blocks: Vec<u64> = (1..=6).flat_map(|b| [b, b]).collect();
        let addresses = || {
            ListArray::from_iter_primitive::<UInt32Type, _, _>(
                (1..=6).flat_map(|_| [Some(vec![Some(0)]), Some(vec![Some(0), Some(0)])]),
            )
        };
        let table = crate::testing::write_table(
            dir,
            vec![
                arrow::datatypes::Field::new("block_number", DataType::UInt64, false),
                arrow::datatypes::Field::new("transaction_index", DataType::UInt32, false),
                arrow::datatypes::Field::new("address", addresses().data_type().clone(), true),
            ],
            vec![
                Arc::new(UInt64Array::from(blocks.clone())),
                Arc::new(UInt32Array::from(vec![0; blocks.len()])),
                Arc::new(addresses()),
            ],
            4,
        );
        let roots = RecordBatch::try_from_iter([
            (
                "block_number",
                Arc::new(UInt64Array::from_iter_values(1..=6)) as ArrayRef,
            ),
            (
                "transaction_index",
                Arc::new(UInt32Array::from(vec![0; 6])) as ArrayRef,
            ),
            (
                "address",
                Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(
                    (1..=6).map(|_| Some(vec![Some(0)])),
                )) as ArrayRef,
            ),
        ])
        .unwrap();

        (table, roots)
    }

    /// The reader decodes the output for the rows its filter keeps, and the
    /// block number only where it checks block bounds on those rows: the
    /// filter's stages decode the columns they filter on themselves.
    #[test]
    fn the_reader_decodes_only_what_it_returns() {
        let dir = tempfile::tempdir().unwrap();
        let (table, roots) = calls_table(dir.path());
        let pair = ["block_number", "transaction_index"];
        let keys = KeyFilter::build(
            std::slice::from_ref(&roots),
            &pair,
            &pair,
            "block_number",
            "block_number",
        );
        let children = HierarchicalFilter::build(
            std::slice::from_ref(&roots),
            &pair,
            "address",
            "address",
            HierarchicalMode::Children,
            false,
        );
        let item = RowPredicate::new(vec![crate::scan::predicate::col_in_list(
            "transaction_index",
            Arc::new(UInt32Array::from(vec![0])),
        )]);

        let output = vec!["address"];
        let bounded = |request: &mut ScanRequest| {
            request.block_number_column = Some("block_number");
            request.from_block = Some(3);
            request.to_block = Some(4);
        };
        let mut items = ScanRequest::new(output.clone());
        bounded(&mut items);
        items.predicates = vec![&item];
        let mut keyed = ScanRequest::new(output.clone());
        bounded(&mut keyed);
        keyed.key_filter = Some(&keys);
        let mut related = ScanRequest::new(output.clone());
        bounded(&mut related);
        related.key_filter = Some(&keys);
        related.hierarchical_filter = Some(&children);
        let mut unbounded = related.clone();
        unbounded.from_block = None;
        unbounded.to_block = None;

        for (what, request, expected) in [
            ("items", &items, &["address"][..]),
            ("keys", &keyed, &["address"]),
            ("addresses, bounded", &related, &["address", "block_number"]),
            ("addresses", &unbounded, &["address"]),
        ] {
            let group = PreparedRowGroup {
                index: 0,
                active_items: (0..request.predicates.len()).collect(),
                narrowed: Vec::new(),
            };
            let filter = reader_filter(&table, request, &group, None);
            let columns: Vec<&str> = reader_columns(request, &filter).collect();
            assert_eq!(columns, expected, "{what}");
        }

        // Bounds checked on the rows read keep only the rows inside them.
        let mut request = ScanRequest::new(vec!["block_number", "address"]);
        bounded(&mut request);
        request.hierarchical_filter = Some(&children);
        let read = scan(&table, &request).unwrap();
        let blocks: Vec<u64> = read
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_primitive::<arrow::datatypes::UInt64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(blocks, [3, 4]);
    }

    /// Items over row groups whose statistics leave different items to run:
    /// `["a", "b"]` in one row group runs the items listing `a` and `b`.
    fn items_over_row_groups(dir: &Path) -> (ParquetTable, Vec<RowPredicate>) {
        let addresses = ["a", "b", "a", "c", "b", "c", "a", "b", "c", "c", "b", "b"];
        let table = crate::testing::write_table(
            dir,
            vec![
                arrow::datatypes::Field::new("block_number", DataType::UInt64, false),
                arrow::datatypes::Field::new("address", DataType::Utf8, false),
            ],
            vec![
                Arc::new(UInt64Array::from_iter_values(0..addresses.len() as u64)),
                Arc::new(StringArray::from(addresses.to_vec())),
            ],
            2,
        );
        let item = |value: &str| {
            let values = Arc::new(StringArray::from(vec![value]));
            RowPredicate::new(vec![crate::scan::predicate::col_in_list("address", values)])
        };

        (table, vec![item("a"), item("b"), item("c")])
    }

    /// A scan builds one union for each set of items that runs on some row
    /// group, however many row groups that set runs on, and none for an item
    /// that runs alone. Each admits the values its items list.
    #[test]
    fn a_scan_builds_one_union_per_set_of_items() {
        let dir = tempfile::tempdir().unwrap();
        let (table, items) = items_over_row_groups(dir.path());
        let mut request = ScanRequest::new(vec!["address"]);
        request.predicates = items.iter().collect();

        let row_groups = select_row_groups(&table, &request);
        let active: Vec<&[usize]> = row_groups
            .iter()
            .map(|g| g.active_items.as_slice())
            .collect();
        assert_eq!(
            active,
            [&[0, 1][..], &[0, 1, 2], &[1, 2], &[0, 1], &[2], &[1]],
            "the row groups this test is about"
        );

        let unions = ItemUnions::build(&request, &row_groups);
        assert_eq!(unions.0.len(), 3, "one per distinct set of several items");

        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let batch = RecordBatch::try_from_iter([("address", values)]).unwrap();
        for (set, admitted) in [
            (&[0, 1][..], [true, true, false]),
            (&[0, 1, 2], [true, true, true]),
            (&[1, 2], [false, true, true]),
        ] {
            let union = unions.get(set).expect("several items");
            assert_eq!(
                union.evaluate(&batch).unwrap(),
                BooleanArray::from(admitted.to_vec())
            );
        }
        assert!(unions.get(&[2]).is_none() && unions.get(&[1]).is_none());
    }

    /// Several items return the rows any of them matches, whichever set of
    /// them each row group runs.
    #[test]
    fn several_items_return_the_rows_any_of_them_matches() {
        let dir = tempfile::tempdir().unwrap();
        let (table, items) = items_over_row_groups(dir.path());
        for picked in [vec![0, 1], vec![0, 2], vec![1, 2], vec![0, 1, 2]] {
            let mut request = ScanRequest::new(vec!["block_number", "address"]);
            request.predicates = picked.iter().map(|&i| &items[i]).collect();

            let read = scan(&table, &request).unwrap();
            let blocks: Vec<u64> = read
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_primitive::<arrow::datatypes::UInt64Type>()
                        .values()
                        .to_vec()
                })
                .collect();
            let listed: Vec<&str> = picked.iter().map(|&i| ["a", "b", "c"][i]).collect();
            let addresses = ["a", "b", "a", "c", "b", "c", "a", "b", "c", "c", "b", "b"];
            let expected: Vec<u64> = (0..addresses.len() as u64)
                .filter(|&row| listed.contains(&addresses[row as usize]))
                .collect();
            assert_eq!(blocks, expected, "items {picked:?}");
        }
    }

    /// A filter a row group's statistics prove for every row runs no stage
    /// there, and an item left with nothing to check admits every row without
    /// one. Tags still need each item's answer, so one stage stays for them;
    /// the union in front of it goes, since it would admit every row.
    #[test]
    fn a_filter_the_statistics_prove_runs_no_stage() {
        let dir = tempfile::tempdir().unwrap();
        let (table, items) = items_over_row_groups(dir.path());
        let b_or_c = RowPredicate::new(vec![crate::scan::predicate::col_in_list(
            "address",
            Arc::new(StringArray::from(vec!["b", "c"])),
        )]);
        let stages = |request: &ScanRequest| -> Vec<(usize, usize)> {
            let row_groups = select_row_groups(&table, request);
            let unions = ItemUnions::build(request, &row_groups);
            row_groups
                .iter()
                .map(|group| {
                    let union = unions.get(&group.active_items);
                    let filter = reader_filter(&table, request, group, union);
                    (group.index, filter.stages.len())
                })
                .collect()
        };

        // Row groups: [a, b] [a, c] [b, c] [a, b] [c, c] [b, b].
        let mut request = ScanRequest::new(vec!["address"]);
        request.predicates = vec![&items[2]];
        assert_eq!(stages(&request), [(1, 1), (2, 1), (4, 0)], "c");

        request.predicates = vec![&items[2], &b_or_c];
        assert_eq!(
            stages(&request),
            [(0, 1), (1, 2), (2, 2), (3, 1), (4, 0), (5, 0)],
            "c, and b or c"
        );

        request.item_tags = vec![&[0][..], &[1][..]];
        assert_eq!(
            stages(&request),
            [(0, 1), (1, 2), (2, 2), (3, 1), (4, 1), (5, 1)],
            "the same, tagged"
        );
    }

    /// A relation key keeps the same rows whether the scan reads them or the
    /// cache holds them, including a block number stored signed past 2^31.
    #[test]
    fn a_cached_key_scan_keeps_what_the_reader_keeps() {
        let dir = tempfile::tempdir().unwrap();
        let table = crate::testing::write_table(
            dir.path(),
            vec![
                arrow::datatypes::Field::new("block_number", DataType::Int32, false),
                arrow::datatypes::Field::new("index", DataType::UInt32, false),
            ],
            vec![
                Arc::new(Int32Array::from(vec![5, 7, -2, -1, -1])),
                Arc::new(UInt32Array::from(vec![0, 0, 0, 0, 1])),
            ],
            8,
        );
        let source = RecordBatch::try_from_iter([
            (
                "block_number",
                Arc::new(Int32Array::from(vec![7, -1])) as ArrayRef,
            ),
            ("index", Arc::new(UInt32Array::from(vec![0, 1])) as ArrayRef),
        ])
        .unwrap();
        let keys = ["block_number", "index"];
        let filter = KeyFilter::build(&[source], &keys, &keys, "block_number", "block_number");

        let mut request = ScanRequest::new(keys.to_vec());
        request.block_number_column = Some("block_number");
        request.key_filter = Some(&filter);
        let read = scan(&table, &request).unwrap();
        let cache = super::super::ColumnCache::new(u64::MAX);
        request.column_cache = Some(&cache);
        let decoded = scan(&table, &request).unwrap();

        assert_eq!(read.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(decoded, read);
    }

    /// A scan the cache serves returns the batches the reader returns: no
    /// longer than the request's batch size, with each row's position and
    /// tags beside it.
    #[test]
    fn a_cached_scan_returns_the_batches_the_reader_returns() {
        let dir = tempfile::tempdir().unwrap();
        let rows = 10u64;
        let table = crate::testing::write_table(
            dir.path(),
            vec![arrow::datatypes::Field::new(
                "block_number",
                DataType::UInt64,
                false,
            )],
            vec![Arc::new(UInt64Array::from_iter_values(0..rows))],
            rows as usize,
        );
        let every = RowPredicate::new(Vec::new());
        let items = [0];
        let window = super::super::Window {
            from: Some(2),
            to: Some(8),
        };

        for batch_size in [1, 2, 3, 7, 1000] {
            let mut request = ScanRequest::new(vec!["block_number"]);
            request.block_number_column = Some("block_number");
            (request.from_block, request.to_block) = (window.from, window.to);
            request.window = Some(window);
            request.predicates = vec![&every];
            request.item_tags = vec![&items];
            request.positions = true;
            request.batch_size = batch_size;
            let read = scan_rows(&table, &request).unwrap();
            let cache = super::super::ColumnCache::new(u64::MAX);
            request.column_cache = Some(&cache);
            let decoded = scan_rows(&table, &request).unwrap();

            assert!(cache.used() > 0, "the cache served the scan");
            assert_eq!(
                decoded.rows().batches(),
                read.rows().batches(),
                "{batch_size}"
            );
            assert_eq!(decoded.rows().positions(), read.rows().positions());
            assert_eq!(decoded.matched_by(&items), read.matched_by(&items));
        }
    }

    #[test]
    fn test_block_range_mask_int64() {
        // A bare INT64 block_number column must be filtered, not pass-through.
        let col: Arc<dyn Array> = Arc::new(Int64Array::from(vec![100, 150, 200, 250]));
        let mask = block_range_mask(&col, Some(150), Some(200)).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, true, true, false]));
    }

    #[test]
    fn test_block_range_mask_uint16() {
        let col: Arc<dyn Array> = Arc::new(UInt16Array::from(vec![10u16, 20, 30, 40]));
        let mask = block_range_mask(&col, Some(20), Some(30)).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, true, true, false]));
    }

    #[test]
    fn test_block_range_mask_int16() {
        let col: Arc<dyn Array> = Arc::new(Int16Array::from(vec![10i16, 20, 30, 40]));
        let mask = block_range_mask(&col, Some(20), None).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, true, true, true]));
    }

    #[test]
    fn test_block_range_mask_int64_from_above_i64_max() {
        let col: Arc<dyn Array> = Arc::new(Int64Array::from(vec![0, i64::MAX]));
        let mask = block_range_mask(&col, Some(u64::MAX), None).unwrap();
        assert_eq!(mask, BooleanArray::from(vec![false, false]));
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_no_predicate() {
        if !crate::testing::chunks_present() {
            return;
        }

        let table = ParquetTable::open(&solana_chunk_path().join("blocks.parquet")).unwrap();
        let request = ScanRequest::new(vec!["number", "hash"]);
        let batches = scan(&table, &request).unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, table.num_rows() as usize);
        assert_eq!(batches[0].num_columns(), 2);
    }

    /// The rows and peak bytes of `request`, its parallel work held to the
    /// calling thread so that every allocation is counted.
    fn scan_counted(table: &ParquetTable, request: &ScanRequest) -> (Vec<RecordBatch>, usize) {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        pool.install(|| crate::testing::peak_bytes(|| scan(table, request).unwrap()))
    }

    /// A row group whose window the cache cannot hold is read as the reader
    /// reads it: the scan decodes the rows its key selects, not the window.
    #[test]
    fn a_window_the_cache_cannot_hold_is_read_by_the_reader() {
        let dir = tempfile::tempdir().unwrap();
        let table = crate::testing::wide_table(dir.path());
        let source = RecordBatch::try_from_iter([
            (
                "block_number",
                Arc::new(UInt64Array::from(vec![1003])) as ArrayRef,
            ),
            ("index", Arc::new(UInt32Array::from(vec![7])) as ArrayRef),
        ])
        .unwrap();
        let keys = ["block_number", "index"];
        let filter = KeyFilter::build(&[source], &keys, &keys, "block_number", "block_number");
        let window = super::super::Window {
            from: Some(1000),
            to: None,
        };

        let mut request = ScanRequest::new(vec!["block_number", "index", "payload"]);
        request.block_number_column = Some("block_number");
        request.from_block = window.from;
        request.window = Some(window);
        request.key_filter = Some(&filter);
        let (expected, _) = scan_counted(&table, &request);

        let cache = super::super::ColumnCache::new(16 << 10);
        request.column_cache = Some(&cache);
        let (rows, peak) = scan_counted(&table, &request);

        assert_eq!(rows, expected);
        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let whole = crate::testing::WIDE_PAYLOAD;
        assert!(peak < whole / 4, "{peak} bytes to read one row of {whole}");
    }

    /// Finding a window's rows reads the row group's block numbers a batch at
    /// a time, not all of them at once.
    #[test]
    fn a_window_finds_its_rows_without_holding_every_block_number() {
        let dir = tempfile::tempdir().unwrap();
        let rows = 2u64 << 20;
        let table = crate::testing::write_table(
            dir.path(),
            vec![arrow::datatypes::Field::new(
                "block_number",
                DataType::UInt64,
                false,
            )],
            vec![Arc::new(UInt64Array::from_iter_values(0..rows))],
            rows as usize,
        );
        let window = super::super::Window {
            from: Some(1000),
            to: Some(1063),
        };

        let mut request = ScanRequest::new(vec!["block_number"]);
        request.block_number_column = Some("block_number");
        (request.from_block, request.to_block) = (window.from, window.to);
        request.window = Some(window);
        let cache = super::super::ColumnCache::new(u64::MAX);
        request.column_cache = Some(&cache);
        let (found, peak) = scan_counted(&table, &request);

        assert_eq!(found.iter().map(RecordBatch::num_rows).sum::<usize>(), 64);
        let whole = rows as usize * 8;
        assert!(peak < whole / 4, "{peak} bytes to find 64 rows of {whole}");
    }

    /// The offsets of a window's rows stay inside the cache's budget however
    /// they grew while the rows were found. A budget that cannot take them
    /// leaves the row group to the reader.
    #[test]
    fn a_window_the_cache_holds_stays_inside_its_budget() {
        let dir = tempfile::tempdir().unwrap();
        let rows = 100_000u64;
        let table = crate::testing::write_table(
            dir.path(),
            vec![arrow::datatypes::Field::new(
                "block_number",
                DataType::UInt64,
                false,
            )],
            vec![Arc::new(UInt64Array::from_iter_values(0..rows))],
            rows as usize,
        );
        let window = super::super::Window {
            from: Some(1),
            to: Some(rows - 2),
        };

        for budget in [rows * 4, 500_000, u64::MAX] {
            let cache = super::super::ColumnCache::new(budget);
            let mut request = ScanRequest::new(vec!["block_number"]);
            request.block_number_column = Some("block_number");
            (request.from_block, request.to_block) = (window.from, window.to);
            request.window = Some(window);
            request.column_cache = Some(&cache);
            let found = scan(&table, &request).unwrap();

            assert_eq!(
                found.iter().map(RecordBatch::num_rows).sum::<usize>(),
                rows as usize - 2
            );
            assert!(cache.used() <= budget, "{} bytes held", cache.used());
            // The offsets fit in all but the smallest budget.
            assert_eq!(cache.used() > 0, budget > rows * 4, "budget {budget}");
        }
    }

    /// A read-back by row identity checks blocks before it decodes the wide
    /// parts of the identity, so it decodes those of its own block alone.
    #[test]
    fn a_read_back_decodes_the_wide_keys_of_its_block_alone() {
        let dir = tempfile::tempdir().unwrap();
        let table = crate::testing::wide_table(dir.path());
        let block = 1003u64;
        let index = 7u32;
        let row = (block - 1000) * 64 + index as u64;
        let identity = RecordBatch::try_from_iter([
            (
                "block_number",
                Arc::new(UInt64Array::from(vec![block])) as ArrayRef,
            ),
            (
                "index",
                Arc::new(UInt32Array::from(vec![index])) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(StringArray::from(vec![format!("{row:0>512}")])) as ArrayRef,
            ),
        ])
        .unwrap();
        let columns = ["block_number", "index", "payload"];
        let names: Vec<String> = columns.iter().map(|name| name.to_string()).collect();
        let filter = KeyFilter::for_rows(&[identity], &names, "block_number").unwrap();

        let mut request = ScanRequest::new(columns.to_vec());
        request.block_number_column = Some("block_number");
        (request.from_block, request.to_block) = (Some(block), Some(block));
        request.key_filter = Some(&filter);
        let (rows, peak) = scan_counted(&table, &request);

        assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let whole = crate::testing::WIDE_PAYLOAD;
        assert!(
            peak < whole / 4,
            "{peak} bytes to read back one row of {whole}"
        );
    }

    /// The offsets of a window's rows hold no more than the cache reserves
    /// for them at their most, every row of the group but one.
    #[test]
    fn a_window_holds_no_more_than_the_cache_reserves() {
        let dir = tempfile::tempdir().unwrap();
        let rows = 1000u64;
        let table = crate::testing::write_table(
            dir.path(),
            vec![arrow::datatypes::Field::new(
                "block_number",
                DataType::UInt64,
                false,
            )],
            vec![Arc::new(UInt64Array::from_iter_values(0..rows))],
            rows as usize,
        );
        let window = super::super::Window {
            from: Some(1),
            to: None,
        };
        let cache = super::super::ColumnCache::new(u64::MAX);

        let held = cache
            .window_rows(&table, 0, window, || {
                window_rows(&table, 0, window, Some("block_number"))
            })
            .unwrap()
            .unwrap();

        assert_eq!(held.value.len(), rows as usize - 1);
        let reserved = super::super::columns::window_reservation(&table, 0);
        assert!(cache.used() <= reserved, "{} of {reserved}", cache.used());
    }

    /// A table several scans share is decoded once and filtered in memory; each
    /// scan must return the rows, columns, positions and marks the reader
    /// returns, in the same order, inside any window holding its rows.
    #[test]
    #[ignore = "requires external chunk data"]
    fn a_decoded_scan_returns_what_the_reader_returns() {
        if !crate::testing::chunks_present() {
            return;
        }

        let instructions =
            ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();
        let first = 406021645u64;
        let windows = [
            super::super::Window {
                from: Some(first),
                to: Some(first + 3),
            },
            super::super::Window {
                from: Some(first + 10),
                to: None,
            },
            super::super::Window::default(),
        ];
        let concat = |batches: Vec<RecordBatch>| -> Option<RecordBatch> {
            let schema = batches.first()?.schema();
            Some(arrow::compute::concat_batches(&schema, &batches).unwrap())
        };
        let columns = vec![
            "block_number",
            "transaction_index",
            "instruction_address",
            "program_id",
            "data_size",
        ];

        for window in windows {
            let bounded = |request: &mut ScanRequest| {
                request.block_number_column = Some("block_number");
                request.from_block = window.from;
                request.to_block = window.to;
                request.window = Some(window);
            };

            // The sources: some of the window's instructions.
            let mut source = ScanRequest::new(columns.clone());
            bounded(&mut source);
            let sources: Vec<RecordBatch> = scan(&instructions, &source)
                .unwrap()
                .iter()
                .map(|batch| batch.slice(0, batch.num_rows().min(500)))
                .collect();

            let pair_keys = ["block_number", "transaction_index"];
            let pair = KeyFilter::build(
                &sources,
                &pair_keys,
                &pair_keys,
                "block_number",
                "block_number",
            );
            let path_keys = ["block_number", "transaction_index", "instruction_address"];
            let path = KeyFilter::build(
                &sources,
                &path_keys,
                &path_keys,
                "block_number",
                "block_number",
            );
            assert!(matches!(
                path.key_set(),
                crate::scan::keys::CompositeKeySet::PairPath(_)
            ));
            let hierarchical = |mode| {
                HierarchicalFilter::build(
                    &sources,
                    &pair_keys,
                    "instruction_address",
                    "instruction_address",
                    mode,
                    false,
                )
            };
            let children = hierarchical(HierarchicalMode::Children);
            let parents = hierarchical(HierarchicalMode::Parents);
            let everything = RowPredicate::new(Vec::new());
            let tags = [&[0][..], &[][..]];
            let flatten = |scanned: Scanned| {
                let positions: Option<Vec<u64>> = scanned.rows().positions().map(|positions| {
                    positions
                        .iter()
                        .flat_map(|batch| batch.values().iter().copied())
                        .collect()
                });
                let tagged: Vec<_> = tags
                    .iter()
                    .map(|items| scanned.matched_by(items).and_then(concat))
                    .collect();
                (
                    concat(scanned.into_rows().into_batches()),
                    positions,
                    tagged,
                )
            };

            let mut requests = Vec::new();
            for filter in [&pair, &path] {
                let mut request = ScanRequest::new(columns.clone());
                bounded(&mut request);
                request.key_filter = Some(filter);
                requests.push(("key", request));
            }
            for filter in [&children, &parents] {
                let mut request = ScanRequest::new(columns.clone());
                bounded(&mut request);
                request.key_filter = Some(&pair);
                request.hierarchical_filter = Some(filter);
                requests.push(("hierarchical", request));
            }
            let mut unfiltered = ScanRequest::new(columns.clone());
            bounded(&mut unfiltered);
            unfiltered.predicates = vec![&everything];
            unfiltered.item_tags = tags.to_vec();
            requests.push(("unfiltered", unfiltered));

            let cache = super::super::ColumnCache::new(u64::MAX);
            for (what, request) in requests {
                for positions in [false, true] {
                    let mut request = request.clone();
                    request.positions = positions;
                    let expected = flatten(scan_rows(&instructions, &request).unwrap());
                    request.column_cache = Some(&cache);
                    // Twice: the second scan reads what the first decoded.
                    for _ in 0..2 {
                        let decoded = flatten(scan_rows(&instructions, &request).unwrap());
                        assert_eq!(
                            decoded, expected,
                            "{what} scan, positions {positions:?}, window {window:?}"
                        );
                    }
                }
            }
            assert!(cache.used() > 0, "the decoded scans kept nothing");
        }
    }

    /// Covers CT-5 · INV-B1
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_with_block_range() {
        if !crate::testing::chunks_present() {
            return;
        }

        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let total_rows = table.num_rows();

        // Scan with a narrow block range
        let mut request = ScanRequest::new(vec!["block_number", "program_id"]);
        request.block_number_column = Some("block_number");
        // Use a range that's a subset of the data
        request.from_block = Some(406021650);
        request.to_block = Some(406021670);

        let batches = scan(&table, &request).unwrap();
        let filtered_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Should have fewer rows than total
        assert!(
            filtered_rows < total_rows as usize,
            "block range filter should reduce rows: {} vs {}",
            filtered_rows,
            total_rows
        );
        assert!(filtered_rows > 0, "should have some matching rows");
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_with_predicate() {
        if !crate::testing::chunks_present() {
            return;
        }

        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let pred = RowPredicate::new(vec![crate::scan::predicate::ColumnPredicate {
            column: "program_id".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&[
                "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            ])),
        }]);

        let mut request = ScanRequest::new(vec!["block_number", "transaction_index", "program_id"]);
        request.predicates = vec![&pred];

        let batches = scan(&table, &request).unwrap();
        let filtered_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Verify all rows have the correct program_id
        for batch in &batches {
            let col = batch
                .column_by_name("program_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..col.len() {
                assert_eq!(col.value(i), "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
            }
        }

        assert!(
            filtered_rows < table.num_rows() as usize,
            "predicate should filter rows"
        );
    }

    /// Covers CT-5 · INV-B1
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_with_predicate_and_block_range() {
        if !crate::testing::chunks_present() {
            return;
        }

        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let pred = RowPredicate::new(vec![crate::scan::predicate::ColumnPredicate {
            column: "program_id".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&[
                "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            ])),
        }]);

        let mut request = ScanRequest::new(vec!["block_number", "program_id"]);
        request.predicates = vec![&pred];
        request.block_number_column = Some("block_number");
        request.from_block = Some(406021650);
        request.to_block = Some(406021670);

        let batches = scan(&table, &request).unwrap();
        let filtered_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // All rows should have correct program_id and block_number in range
        for batch in &batches {
            let program_id = batch
                .column_by_name("program_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let block_num = batch.column_by_name("block_number").unwrap();

            // Check block range using the appropriate type
            if let Some(arr) = block_num.as_any().downcast_ref::<UInt32Array>() {
                for i in 0..arr.len() {
                    let bn = arr.value(i) as u64;
                    assert!((406021650..=406021670).contains(&bn));
                }
            } else if let Some(arr) = block_num.as_any().downcast_ref::<UInt64Array>() {
                for i in 0..arr.len() {
                    let bn = arr.value(i);
                    assert!((406021650..=406021670).contains(&bn));
                }
            }

            for i in 0..program_id.len() {
                assert_eq!(
                    program_id.value(i),
                    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
                );
            }
        }

        // Without this the per-row checks above are vacuous: zero rows verifies
        // nothing. The chunk and the filter are both fixed, so it either matches
        // or the test is not testing anything.
        assert!(
            filtered_rows > 0,
            "the whirlpool program must match rows in this block range"
        );
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_predicate_columns_not_in_output() {
        if !crate::testing::chunks_present() {
            return;
        }

        // Predicate uses program_id but output only asks for block_number
        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let pred = RowPredicate::new(vec![crate::scan::predicate::ColumnPredicate {
            column: "program_id".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&[
                "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            ])),
        }]);

        let mut request = ScanRequest::new(vec!["block_number", "transaction_index"]);
        request.predicates = vec![&pred];

        let batches = scan(&table, &request).unwrap();

        // Output should NOT contain program_id
        for batch in &batches {
            assert_eq!(batch.num_columns(), 2);
            assert!(batch.schema().field_with_name("program_id").is_err());
        }
    }

    /// hierarchical_filter and predicates must not be set simultaneously.
    ///
    /// Asserted through `catch_unwind` rather than `#[should_panic]` so the test
    /// can skip when the chunk is absent: a `#[should_panic]` test that returns
    /// early fails, and one that panics for another reason passes.
    #[test]
    #[ignore = "requires external chunk data"]
    fn test_hierarchical_filter_with_predicates_panics() {
        if !crate::testing::chunks_present() {
            return;
        }

        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let pred = RowPredicate::new(vec![crate::scan::predicate::ColumnPredicate {
            column: "program_id".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&[
                "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            ])),
        }]);

        // Build a minimal HierarchicalFilter from empty source batches
        let hf = HierarchicalFilter::build(
            &[],
            &["block_number", "transaction_index"],
            "instruction_address",
            "instruction_address",
            HierarchicalMode::Children,
            true,
        );

        let mut request = ScanRequest::new(vec!["block_number"]);
        request.predicates = vec![&pred];
        request.hierarchical_filter = Some(&hf);

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = scan(&table, &request);
        }))
        .expect_err("setting both must trip the debug assertion");

        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or_default()
            .to_string();
        assert!(
            message.contains("hierarchical_filter and predicates must not be set simultaneously"),
            "panicked with: {message}"
        );
    }

    #[test]
    #[ignore = "requires external chunk data"]
    fn test_scan_row_group_pruning() {
        if !crate::testing::chunks_present() {
            return;
        }

        // EVM logs are sorted by topic0, so row group stats on topic0 should be tight.
        // Filtering for a specific topic0 should skip most row groups.
        let table = ParquetTable::open(&evm_chunk_path().join("logs.parquet")).unwrap();

        let pred = RowPredicate::new(vec![crate::scan::predicate::ColumnPredicate {
            column: "topic0".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&[
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            ])),
        }]);

        let mut request = ScanRequest::new(vec!["block_number", "address", "topic0"]);
        request.predicates = vec![&pred];

        let batches = scan(&table, &request).unwrap();
        let filtered_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // ERC-20 Transfer topic should match many rows but not all
        assert!(
            filtered_rows > 0,
            "should match some ERC-20 Transfer events"
        );
        assert!(
            filtered_rows < table.num_rows() as usize,
            "should not match all rows"
        );
    }
}
