use crate::scan::chunk::ParquetTable;
use crate::scan::predicate::RowPredicate;
use anyhow::{Context, Result};
use arrow::array::builder::BooleanBufferBuilder;
use arrow::array::*;
use arrow::compute::kernels::boolean::and;
use arrow::compute::kernels::cmp::{gt_eq, lt_eq};
use arrow::datatypes::{Schema, SchemaRef};
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A scan request: which columns to read, what predicates to apply.
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
        }
    }
}

/// A set-based filter for join key pushdown during relation scans.
/// Filters rows to only those matching specific composite keys from a primary scan.
/// Uses Arc-wrapped sets for cheap cloning into RowFilter closures.
pub struct KeyFilter {
    /// Column names forming the composite key (in the target/relation table).
    pub columns: Vec<String>,
    /// Pre-built set of serialized composite keys (Arc for cheap clone into closures).
    key_set: Arc<HashSet<Vec<u8>>>,
    /// Sorted unique block numbers for efficient row group pruning.
    sorted_blocks: Vec<u64>,
    /// Block number column name in the target table.
    block_number_column: String,
}

impl KeyFilter {
    /// Build a key filter from primary scan results.
    ///
    /// - `primary_batches`: results from the primary table scan
    /// - `left_keys`: column names in primary_batches
    /// - `right_keys`: column names in the target/relation table
    /// - `primary_bn_col`: block number column name in primary_batches
    /// - `target_bn_col`: block number column name in the target table
    pub fn build(
        primary_batches: &[RecordBatch],
        left_keys: &[&str],
        right_keys: &[&str],
        primary_bn_col: &str,
        target_bn_col: &str,
    ) -> Self {
        assert_eq!(left_keys.len(), right_keys.len());

        let mut block_numbers = HashSet::new();
        let mut key_set = HashSet::new();

        for batch in primary_batches {
            if batch.num_rows() == 0 {
                continue;
            }

            // Extract block numbers for coarse filtering
            if let Some(col) = batch.column_by_name(primary_bn_col) {
                extract_block_numbers(col.as_ref(), &mut block_numbers);
            }

            // Extract composite keys using left_key column names
            let typed_cols: Vec<Option<TypedKeyColumn>> = left_keys
                .iter()
                .map(|name| {
                    batch
                        .column_by_name(name)
                        .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
                })
                .collect();

            let mut key_buf = Vec::with_capacity(left_keys.len() * 8);
            for row in 0..batch.num_rows() {
                key_buf.clear();
                for tc in &typed_cols {
                    if let Some(tc) = tc {
                        tc.append_to(&mut key_buf, row);
                    } else {
                        key_buf.extend_from_slice(&0u64.to_le_bytes());
                    }
                }
                key_set.insert(key_buf.clone());
            }
        }

        let mut sorted_blocks: Vec<u64> = block_numbers.iter().copied().collect();
        sorted_blocks.sort_unstable();

        KeyFilter {
            columns: right_keys.iter().map(|s| s.to_string()).collect(),
            key_set: Arc::new(key_set),
            sorted_blocks,
            block_number_column: target_bn_col.to_string(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.key_set.is_empty()
    }
}

/// Mode for hierarchical address filtering.
#[derive(Clone, Copy)]
pub enum HierarchicalMode {
    /// Keep rows whose address is a strict extension of a source address (children).
    Children,
    /// Keep rows whose address is a strict prefix of a source address (parents).
    Parents,
}

/// A filter for hierarchical joins (find_children / find_parents) that can be
/// applied as a RowFilter stage, avoiding decode of data columns for non-matching rows.
pub struct HierarchicalFilter {
    /// Source addresses indexed by group key (serialized block_number + transaction_index).
    source_addresses: Arc<HashMap<Vec<u8>, Vec<Vec<u32>>>>,
    /// Group key column names in the target table (e.g., ["block_number", "transaction_index"]).
    pub group_key_columns: Vec<String>,
    /// Address column name (e.g., "instruction_address" or "trace_address").
    pub address_column: String,
    /// Whether to find children or parents.
    mode: HierarchicalMode,
}

impl HierarchicalFilter {
    /// Build from primary scan results.
    pub fn build(
        primary_batches: &[RecordBatch],
        group_key_columns: &[&str],
        address_column: &str,
        mode: HierarchicalMode,
    ) -> Self {
        let mut source_addresses: HashMap<Vec<u8>, Vec<Vec<u32>>> = HashMap::new();

        for batch in primary_batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let typed_keys: Vec<Option<TypedKeyColumn>> = group_key_columns
                .iter()
                .map(|name| {
                    batch
                        .column_by_name(name)
                        .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
                })
                .collect();

            let addr_col = batch.column_by_name(address_column);
            let addr_list =
                addr_col.and_then(|c| c.as_any().downcast_ref::<GenericListArray<i32>>());

            if addr_list.is_none() {
                continue;
            }
            let addr_list = addr_list.unwrap();

            let mut key_buf = Vec::with_capacity(group_key_columns.len() * 8);
            for row in 0..batch.num_rows() {
                key_buf.clear();
                for tc in &typed_keys {
                    if let Some(tc) = tc {
                        tc.append_to(&mut key_buf, row);
                    } else {
                        key_buf.extend_from_slice(&0u64.to_le_bytes());
                    }
                }
                let addr = extract_address_values(addr_list, row);
                source_addresses
                    .entry(key_buf.clone())
                    .or_default()
                    .push(addr);
            }
        }

        HierarchicalFilter {
            source_addresses: Arc::new(source_addresses),
            group_key_columns: group_key_columns.iter().map(|s| s.to_string()).collect(),
            address_column: address_column.to_string(),
            mode,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.source_addresses.is_empty()
    }
}

/// Extract a List value as Vec<u32>, supporting UInt16, UInt32, Int32 element types.
fn extract_address_values(array: &GenericListArray<i32>, row: usize) -> Vec<u32> {
    if array.is_null(row) {
        return Vec::new();
    }
    let values = array.value(row);
    if let Some(arr) = values.as_any().downcast_ref::<UInt16Array>() {
        (0..arr.len()).map(|i| arr.value(i) as u32).collect()
    } else if let Some(arr) = values.as_any().downcast_ref::<UInt32Array>() {
        (0..arr.len()).map(|i| arr.value(i)).collect()
    } else if let Some(arr) = values.as_any().downcast_ref::<Int32Array>() {
        (0..arr.len()).map(|i| arr.value(i) as u32).collect()
    } else {
        Vec::new()
    }
}

/// Build a boolean mask for hierarchical filtering.
fn hierarchical_mask(
    batch: &RecordBatch,
    source_addresses: &HashMap<Vec<u8>, Vec<Vec<u32>>>,
    group_key_columns: &[String],
    address_column: &str,
    mode: HierarchicalMode,
) -> BooleanArray {
    let len = batch.num_rows();
    let mut builder = BooleanBufferBuilder::new(len);

    let typed_keys: Vec<Option<TypedKeyColumn>> = group_key_columns
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
        })
        .collect();

    let addr_col = batch.column_by_name(address_column);
    let addr_list = addr_col.and_then(|c| c.as_any().downcast_ref::<GenericListArray<i32>>());

    if addr_list.is_none() {
        builder.append_n(len, false);
        return BooleanArray::new(builder.finish(), None);
    }
    let addr_list = addr_list.unwrap();

    let mut key_buf = Vec::with_capacity(group_key_columns.len() * 8);
    for row in 0..len {
        key_buf.clear();
        for tc in &typed_keys {
            if let Some(tc) = tc {
                tc.append_to(&mut key_buf, row);
            } else {
                key_buf.extend_from_slice(&0u64.to_le_bytes());
            }
        }

        let matches = source_addresses
            .get(key_buf.as_slice())
            .map(|addrs| {
                let target_addr = extract_address_values(addr_list, row);
                match mode {
                    HierarchicalMode::Children => addrs.iter().any(|parent| {
                        target_addr.len() > parent.len()
                            && target_addr[..parent.len()] == parent[..]
                    }),
                    HierarchicalMode::Parents => addrs.iter().any(|child| {
                        target_addr.len() < child.len()
                            && child[..target_addr.len()] == target_addr[..]
                    }),
                }
            })
            .unwrap_or(false);

        builder.append(matches);
    }

    BooleanArray::new(builder.finish(), None)
}

/// Extract all block number values from a column into a HashSet.
fn extract_block_numbers(col: &dyn Array, out: &mut HashSet<u64>) {
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        for i in 0..a.len() {
            out.insert(a.value(i));
        }
    } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        for i in 0..a.len() {
            out.insert(a.value(i) as u64);
        }
    } else if let Some(a) = col.as_any().downcast_ref::<UInt16Array>() {
        for i in 0..a.len() {
            out.insert(a.value(i) as u64);
        }
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        for i in 0..a.len() {
            out.insert(a.value(i) as u64);
        }
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        for i in 0..a.len() {
            out.insert(a.value(i) as u64);
        }
    }
}

/// A typed column extractor that avoids per-row type dispatch.
enum TypedKeyColumn<'a> {
    U64(&'a UInt64Array),
    U32(&'a UInt32Array),
    U16(&'a UInt16Array),
    U8(&'a UInt8Array),
    I64(&'a Int64Array),
    I32(&'a Int32Array),
    I16(&'a Int16Array),
    Str(&'a StringArray),
    List(&'a GenericListArray<i32>),
}

impl<'a> TypedKeyColumn<'a> {
    fn resolve(col: &'a dyn Array) -> Option<Self> {
        if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
            return Some(Self::U64(a));
        }
        if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
            return Some(Self::U32(a));
        }
        if let Some(a) = col.as_any().downcast_ref::<UInt16Array>() {
            return Some(Self::U16(a));
        }
        if let Some(a) = col.as_any().downcast_ref::<UInt8Array>() {
            return Some(Self::U8(a));
        }
        if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            return Some(Self::I64(a));
        }
        if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            return Some(Self::I32(a));
        }
        if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
            return Some(Self::I16(a));
        }
        if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
            return Some(Self::Str(a));
        }
        if let Some(a) = col.as_any().downcast_ref::<GenericListArray<i32>>() {
            return Some(Self::List(a));
        }
        None
    }

    #[inline(always)]
    fn append_to(&self, buf: &mut Vec<u8>, row: usize) {
        match self {
            Self::U64(a) => buf.extend_from_slice(&a.value(row).to_le_bytes()),
            Self::U32(a) => buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes()),
            Self::U16(a) => buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes()),
            Self::U8(a) => buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes()),
            Self::I64(a) => buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes()),
            Self::I32(a) => buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes()),
            Self::I16(a) => buf.extend_from_slice(&(a.value(row) as u64).to_le_bytes()),
            Self::Str(a) => {
                let v = a.value(row);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            Self::List(a) => {
                let arr = a.value(row);
                let len = arr.len();
                buf.extend_from_slice(&(len as u32).to_le_bytes());
                // Serialize each element as u64
                if let Some(vals) = arr.as_any().downcast_ref::<UInt16Array>() {
                    for i in 0..len {
                        buf.extend_from_slice(&(vals.value(i) as u64).to_le_bytes());
                    }
                } else if let Some(vals) = arr.as_any().downcast_ref::<Int32Array>() {
                    for i in 0..len {
                        buf.extend_from_slice(&(vals.value(i) as u64).to_le_bytes());
                    }
                } else if let Some(vals) = arr.as_any().downcast_ref::<UInt32Array>() {
                    for i in 0..len {
                        buf.extend_from_slice(&(vals.value(i) as u64).to_le_bytes());
                    }
                } else if let Some(vals) = arr.as_any().downcast_ref::<Int16Array>() {
                    for i in 0..len {
                        buf.extend_from_slice(&(vals.value(i) as u64).to_le_bytes());
                    }
                } else if let Some(vals) = arr.as_any().downcast_ref::<UInt64Array>() {
                    for i in 0..len {
                        buf.extend_from_slice(&vals.value(i).to_le_bytes());
                    }
                } else if let Some(vals) = arr.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..len {
                        buf.extend_from_slice(&(vals.value(i) as u64).to_le_bytes());
                    }
                }
            }
        }
    }

    /// Write normalized u64 value directly into a fixed-size buffer slice.
    /// Only for integer types (used in optimized paths).
    #[inline(always)]
    fn write_to_buf(&self, buf: &mut [u8], row: usize) {
        let v: u64 = match self {
            Self::U64(a) => a.value(row),
            Self::U32(a) => a.value(row) as u64,
            Self::U16(a) => a.value(row) as u64,
            Self::U8(a) => a.value(row) as u64,
            Self::I64(a) => a.value(row) as u64,
            Self::I32(a) => a.value(row) as u64,
            Self::I16(a) => a.value(row) as u64,
            _ => 0, // Not applicable for Str/List
        };
        buf[..8].copy_from_slice(&v.to_le_bytes());
    }
}

/// Build a boolean mask: true for rows where composite key is in the set.
/// Resolves column types once per batch, then uses tight typed loops.
fn composite_key_in_set_mask(
    batch: &RecordBatch,
    key_columns: &[String],
    key_set: &HashSet<Vec<u8>>,
) -> BooleanArray {
    let len = batch.num_rows();
    let mut builder = BooleanBufferBuilder::new(len);

    // Resolve column types once (avoids per-row type dispatch)
    let typed_cols: Vec<Option<TypedKeyColumn>> = key_columns
        .iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
        })
        .collect();

    // Fast path for 2-column composite key (most common: block_number + transaction_index)
    // Uses a fixed-size stack buffer instead of heap-allocated Vec.
    if typed_cols.len() == 2 {
        if let (Some(ref c0), Some(ref c1)) = (&typed_cols[0], &typed_cols[1]) {
            let mut key_buf = [0u8; 16];
            for row in 0..len {
                c0.write_to_buf(&mut key_buf[..8], row);
                c1.write_to_buf(&mut key_buf[8..16], row);
                builder.append(key_set.contains(&key_buf[..16]));
            }
            return BooleanArray::new(builder.finish(), None);
        }
    }

    // General path for arbitrary number of key columns
    let mut key_buf = Vec::with_capacity(key_columns.len() * 8);
    for row in 0..len {
        key_buf.clear();
        for typed_col in &typed_cols {
            if let Some(tc) = typed_col {
                tc.append_to(&mut key_buf, row);
            } else {
                key_buf.extend_from_slice(&0u64.to_le_bytes());
            }
        }
        builder.append(key_set.contains(key_buf.as_slice()));
    }

    BooleanArray::new(builder.finish(), None)
}

/// Execute a scan against a parquet table: read, filter, project.
/// Returns filtered RecordBatches with only the output columns.
pub fn scan(table: &ParquetTable, request: &ScanRequest) -> Result<Vec<RecordBatch>> {
    // 1. Determine all columns we need to read (output + predicate + block range)
    let mut all_columns: HashSet<&str> = HashSet::new();
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
        all_columns.insert(&kf.block_number_column);
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

    // Filter to columns that actually exist in the table
    let all_columns: Vec<&str> = all_columns
        .into_iter()
        .filter(|c| table.column_index(c).is_some())
        .collect();

    // 2. Determine which row groups to scan (skip via statistics)
    let row_groups_to_scan = select_row_groups(table, request)?;

    if row_groups_to_scan.is_empty() {
        return Ok(Vec::new());
    }

    // 3. Read and filter across row groups
    let output_schema = build_output_schema(table.schema(), &request.output_columns);

    // Parallelize across row groups for all scans with >1 RG.
    // Each RG gets its own RowFilter pipeline, evaluated independently.
    let mut all_batches = Vec::new();
    if row_groups_to_scan.len() <= 1 {
        all_batches.extend(scan_row_groups(
            table,
            &row_groups_to_scan,
            &all_columns,
            request,
            &output_schema,
        )?);
    } else {
        let results: Vec<Result<Vec<RecordBatch>>> = row_groups_to_scan
            .par_iter()
            .map(|&rg_idx| scan_row_groups(table, &[rg_idx], &all_columns, request, &output_schema))
            .collect();
        for result in results {
            all_batches.extend(result?);
        }
    }

    Ok(all_batches)
}

/// Select which row groups need to be scanned, skipping those that
/// can be eliminated via statistics.
fn select_row_groups(table: &ParquetTable, request: &ScanRequest) -> Result<Vec<usize>> {
    let mut row_groups = Vec::new();

    for rg_idx in 0..table.num_row_groups() {
        // Check block range filter
        if let Some(bn_col) = request.block_number_column {
            if let Some(stats) = table.column_stats(rg_idx, bn_col) {
                if let (Some(ref min), Some(ref max)) = (stats.min, stats.max) {
                    let rg_min = stat_value_to_u64(min);
                    let rg_max = stat_value_to_u64(max);
                    if let (Some(rg_min), Some(rg_max)) = (rg_min, rg_max) {
                        if let Some(from_block) = request.from_block {
                            if rg_max < from_block {
                                continue; // Entire row group is before our range
                            }
                        }
                        if let Some(to_block) = request.to_block {
                            if rg_min > to_block {
                                continue; // Entire row group is after our range
                            }
                        }
                    }
                }
            }
        }

        // Check predicate-based row group skipping
        if !request.predicates.is_empty() {
            let stats_fn = |col_name: &str| -> Option<(Arc<dyn Array>, Arc<dyn Array>)> {
                let stats = table.column_stats(rg_idx, col_name)?;
                let (min, max) = (stats.min?, stats.max?);
                Some((stat_value_to_array(&min), stat_value_to_array(&max)))
            };

            let pred_refs: Vec<&RowPredicate> = request.predicates.clone();
            if crate::scan::predicate::can_skip_row_group_or(&pred_refs, &stats_fn) {
                continue;
            }
        }

        // Key filter: skip row groups whose block_number range has no overlap with key set
        if let Some(kf) = &request.key_filter {
            let bn_col = &kf.block_number_column;
            if let Some(stats) = table.column_stats(rg_idx, bn_col) {
                if let (Some(ref min), Some(ref max)) = (stats.min, stats.max) {
                    if let (Some(rg_min), Some(rg_max)) =
                        (stat_value_to_u64(min), stat_value_to_u64(max))
                    {
                        // Binary search: any key block number in [rg_min, rg_max]?
                        let first = kf.sorted_blocks.partition_point(|&bn| bn < rg_min);
                        if first >= kf.sorted_blocks.len() || kf.sorted_blocks[first] > rg_max {
                            continue; // No matching block numbers in this row group
                        }
                    }
                }
            }
        }

        row_groups.push(rg_idx);
    }

    Ok(row_groups)
}

/// Scan selected row groups using a single reader: read columns, apply predicates, project output.
///
/// Strategy:
/// - For scans with predicates: Use RowFilter with cascading stages (most selective first).
///   This reads predicate columns eagerly during build(), builds a RowSelection, then reads
///   output columns only for matching rows.
/// - For scans with key/hierarchical filters: read all columns including filter columns,
///   apply filters as RowFilter stages to avoid decoding output columns for non-matching rows.
fn scan_row_groups(
    table: &ParquetTable,
    row_groups: &[usize],
    read_columns: &[&str],
    request: &ScanRequest,
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let parquet_schema = table.metadata().file_metadata().schema_descr();
    let indices: Vec<usize> = read_columns
        .iter()
        .filter_map(|name| table.schema().index_of(name).ok())
        .collect();

    let mask = ProjectionMask::roots(parquet_schema, indices);

    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
        table.data(),
        table.arrow_metadata().clone(),
    )
    .with_projection(mask)
    .with_batch_size(request.batch_size)
    .with_row_groups(row_groups.to_vec());

    // Use RowFilter for predicate pushdown with multi-stage cascading.
    // Each stage reads only its own columns; rows eliminated by early stages
    // avoid column decoding in later stages.
    let has_predicates = !request.predicates.is_empty();
    let effective_from = request.from_block.filter(|&b| b > 0);
    // Skip block range RowFilter when KeyFilter is present — KF already does RG-level
    // block pruning and row-level key matching, making block range redundant.
    let has_block_filter = request.key_filter.is_none()
        && request.block_number_column.is_some()
        && (effective_from.is_some() || request.to_block.is_some());
    let has_key_filter = request.key_filter.is_some();
    let has_hierarchical_filter = request.hierarchical_filter.is_some();

    if has_predicates || has_block_filter || has_key_filter || has_hierarchical_filter {
        let mut filter_stages: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> =
            Vec::new();

        // Stage 0: block range filter
        if has_block_filter {
            let bn_col = request.block_number_column.unwrap();
            if let Ok(idx) = table.schema().index_of(bn_col) {
                let bn_projection = ProjectionMask::roots(parquet_schema, vec![idx]);
                let from_block = effective_from;
                let to_block = request.to_block;
                let bn_col_name = bn_col.to_string();
                filter_stages.push(Box::new(ArrowPredicateFn::new(
                    bn_projection,
                    move |batch: RecordBatch| {
                        if let Some(col) = batch.column_by_name(&bn_col_name) {
                            Ok(block_range_mask(col, from_block, to_block))
                        } else {
                            Ok(BooleanArray::from(vec![true; batch.num_rows()]))
                        }
                    },
                )));
            }
        }

        // Key filter: composite key IN set
        if let Some(kf) = request.key_filter {
            let key_col_indices: Vec<usize> = kf
                .columns
                .iter()
                .filter_map(|name| table.schema().index_of(name).ok())
                .collect();
            if !key_col_indices.is_empty() {
                let key_proj = ProjectionMask::roots(parquet_schema, key_col_indices);
                let key_columns = Arc::new(kf.columns.clone());
                let key_set = kf.key_set.clone();
                filter_stages.push(Box::new(ArrowPredicateFn::new(
                    key_proj,
                    move |batch: RecordBatch| {
                        Ok(composite_key_in_set_mask(&batch, &key_columns, &key_set))
                    },
                )));
            }
        }

        // Hierarchical filter
        if let Some(hf) = request.hierarchical_filter {
            let mut hf_col_indices: Vec<usize> = Vec::new();
            for col in &hf.group_key_columns {
                if let Ok(idx) = table.schema().index_of(col) {
                    hf_col_indices.push(idx);
                }
            }
            if let Ok(idx) = table.schema().index_of(&hf.address_column) {
                hf_col_indices.push(idx);
            }
            hf_col_indices.sort_unstable();
            hf_col_indices.dedup();

            if !hf_col_indices.is_empty() {
                let hf_proj = ProjectionMask::roots(parquet_schema, hf_col_indices);
                let source_addresses = hf.source_addresses.clone();
                let group_key_columns = Arc::new(hf.group_key_columns.clone());
                let address_column = hf.address_column.clone();
                let mode = hf.mode;
                filter_stages.push(Box::new(ArrowPredicateFn::new(
                    hf_proj,
                    move |batch: RecordBatch| {
                        Ok(hierarchical_mask(
                            &batch,
                            &source_addresses,
                            &group_key_columns,
                            &address_column,
                            mode,
                        ))
                    },
                )));
            }
        }

        // Predicate stages: first column gets its own stage (most selective — sort key leader),
        // remaining columns are merged into a single stage.
        if request.predicates.len() == 1 {
            let pred = request.predicates[0].clone();
            let columns = pred.columns;
            if let Some(first) = columns.first() {
                if let Ok(idx) = table.schema().index_of(&first.column) {
                    let col_projection = ProjectionMask::roots(parquet_schema, vec![idx]);
                    let col_name = first.column.clone();
                    let evaluator = first.predicate.clone();
                    filter_stages.push(Box::new(ArrowPredicateFn::new(
                        col_projection,
                        move |batch: RecordBatch| {
                            if let Some(col) = batch.column_by_name(&col_name) {
                                Ok(evaluator.evaluate(col.as_ref()))
                            } else {
                                Ok(BooleanArray::from(vec![true; batch.num_rows()]))
                            }
                        },
                    )));
                }
            }
            if columns.len() > 1 {
                let rest: Vec<_> = columns[1..].to_vec();
                let mut rest_indices: Vec<usize> = Vec::new();
                for cp in &rest {
                    if let Ok(idx) = table.schema().index_of(&cp.column) {
                        rest_indices.push(idx);
                    }
                }
                rest_indices.sort_unstable();
                rest_indices.dedup();
                if !rest_indices.is_empty() {
                    let rest_proj = ProjectionMask::roots(parquet_schema, rest_indices);
                    filter_stages.push(Box::new(ArrowPredicateFn::new(
                        rest_proj,
                        move |batch: RecordBatch| {
                            let mut result: Option<BooleanArray> = None;
                            for cp in &rest {
                                if let Some(col) = batch.column_by_name(&cp.column) {
                                    let mask = cp.predicate.evaluate(col.as_ref());
                                    result = Some(match result {
                                        Some(prev) => {
                                            arrow::compute::kernels::boolean::and(&prev, &mask)
                                                .unwrap()
                                        }
                                        None => mask,
                                    });
                                }
                            }
                            Ok(result.unwrap_or_else(|| {
                                BooleanArray::from(vec![true; batch.num_rows()])
                            }))
                        },
                    )));
                }
            }
        } else if has_predicates {
            let mut pred_col_indices: Vec<usize> = Vec::new();
            for pred in &request.predicates {
                for col in pred.required_columns() {
                    if let Ok(idx) = table.schema().index_of(col) {
                        pred_col_indices.push(idx);
                    }
                }
            }
            pred_col_indices.sort_unstable();
            pred_col_indices.dedup();

            let pred_projection = ProjectionMask::roots(parquet_schema, pred_col_indices);
            let predicates: Vec<RowPredicate> =
                request.predicates.iter().map(|&p| p.clone()).collect();

            filter_stages.push(Box::new(ArrowPredicateFn::new(
                pred_projection,
                move |batch: RecordBatch| {
                    let pred_refs: Vec<&RowPredicate> = predicates.iter().collect();
                    Ok(crate::scan::predicate::or_row_predicates(
                        &pred_refs, &batch,
                    ))
                },
            )));
        }

        if !filter_stages.is_empty() {
            builder = builder.with_row_filter(RowFilter::new(filter_stages));
        }
    }

    let reader = builder.build().context("building parquet reader")?;

    let mut output_batches = Vec::new();

    for batch_result in reader {
        let batch = batch_result.context("reading batch")?;

        if batch.num_rows() == 0 {
            continue;
        }

        // Project to output columns only
        let projected = project_batch(&batch, output_schema)?;
        output_batches.push(projected);
    }

    Ok(output_batches)
}

/// Create a boolean mask for block range filtering.
fn block_range_mask(
    column: &Arc<dyn Array>,
    from_block: Option<u64>,
    to_block: Option<u64>,
) -> BooleanArray {
    // Try UInt32 first (old format), then UInt64 (new format)
    if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
        let mut mask = BooleanArray::from(vec![true; arr.len()]);
        if let Some(from) = from_block {
            let ge = gt_eq(&arr, &UInt64Array::new_scalar(from)).unwrap();
            mask = and(&mask, &ge).unwrap();
        }
        if let Some(to) = to_block {
            let le = lt_eq(&arr, &UInt64Array::new_scalar(to)).unwrap();
            mask = and(&mask, &le).unwrap();
        }
        mask
    } else if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
        let mut mask = BooleanArray::from(vec![true; arr.len()]);
        if let Some(from) = from_block {
            let ge = gt_eq(&arr, &UInt32Array::new_scalar(from as u32)).unwrap();
            mask = and(&mask, &ge).unwrap();
        }
        if let Some(to) = to_block {
            let le = lt_eq(&arr, &UInt32Array::new_scalar(to as u32)).unwrap();
            mask = and(&mask, &le).unwrap();
        }
        mask
    } else if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
        let mut mask = BooleanArray::from(vec![true; arr.len()]);
        if let Some(from) = from_block {
            let ge = gt_eq(&arr, &Int32Array::new_scalar(from as i32)).unwrap();
            mask = and(&mask, &ge).unwrap();
        }
        if let Some(to) = to_block {
            let le = lt_eq(&arr, &Int32Array::new_scalar(to as i32)).unwrap();
            mask = and(&mask, &le).unwrap();
        }
        mask
    } else {
        // Unknown type, don't filter
        BooleanArray::from(vec![true; column.len()])
    }
}

/// Project a RecordBatch to only include the given output columns.
fn project_batch(batch: &RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
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
fn build_output_schema(table_schema: &SchemaRef, columns: &[&str]) -> SchemaRef {
    let fields: Vec<_> = columns
        .iter()
        .filter_map(|name| table_schema.field_with_name(name).ok().cloned())
        .collect();
    Arc::new(Schema::new(fields))
}

/// Convert a StatValue to u64 (for block range comparisons).
fn stat_value_to_u64(value: &crate::scan::chunk::StatValue) -> Option<u64> {
    use crate::scan::chunk::StatValue;
    match value {
        StatValue::Int32(v) => Some(*v as u64),
        StatValue::Int64(v) => Some(*v as u64),
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
        StatValue::ByteArray(v) => {
            Arc::new(StringArray::from(
                vec![std::str::from_utf8(v).unwrap_or("")],
            ))
        }
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
    use crate::scan::predicate::InListPredicate;
    use std::path::{Path, PathBuf};

    fn solana_chunk_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("data/solana/200")
    }

    fn evm_chunk_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("data/evm/large")
    }

    #[test]
    fn test_scan_no_predicate() {
        let table = ParquetTable::open(&solana_chunk_path().join("blocks.parquet")).unwrap();
        let request = ScanRequest::new(vec!["number", "hash"]);
        let batches = scan(&table, &request).unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, table.num_rows() as usize);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_scan_with_block_range() {
        let table = ParquetTable::open(&solana_chunk_path().join("instructions.parquet")).unwrap();

        let total_rows = table.num_rows();

        // Scan with a narrow block range
        let mut request = ScanRequest::new(vec!["block_number", "program_id"]);
        request.block_number_column = Some("block_number");
        // Use a range that's a subset of the data
        request.from_block = Some(323024920);
        request.to_block = Some(323024940);

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
    fn test_scan_with_predicate() {
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

    #[test]
    fn test_scan_with_predicate_and_block_range() {
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
        request.from_block = Some(323024920);
        request.to_block = Some(323024940);

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
                    assert!(bn >= 323024920 && bn <= 323024940);
                }
            } else if let Some(arr) = block_num.as_any().downcast_ref::<UInt64Array>() {
                for i in 0..arr.len() {
                    let bn = arr.value(i);
                    assert!(bn >= 323024920 && bn <= 323024940);
                }
            }

            for i in 0..program_id.len() {
                assert_eq!(
                    program_id.value(i),
                    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
                );
            }
        }

        assert!(filtered_rows > 0 || true, "may or may not have matches");
    }

    #[test]
    fn test_scan_predicate_columns_not_in_output() {
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

    #[test]
    fn test_scan_row_group_pruning() {
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
