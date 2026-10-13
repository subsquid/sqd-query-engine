//! Reading one block range: every table plan's rows, and the rows each of its
//! relations relates to them.

use super::prepare::{Execution, PlannedTable};
use super::Stopwatch;
use crate::metadata::TableDescription;
use crate::output::block_index::compute_block_range;
use crate::output::projection::ResolvedProjection;
use crate::output::weight::TableOutput;
use crate::query::{RelationKind, RelationPlan};
use crate::scan::{
    AddressIndex, ChunkReader, HierarchicalFilter, HierarchicalMode, KeyFilter, KeySet, Rows,
    ScanRequest, Window,
};
use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::collections::HashMap;

/// Read all primary and relation rows for one complete block range.
pub(super) fn scan_tables(
    execution: &Execution,
    from_block: u64,
    to_block: Option<u64>,
) -> Result<HashMap<String, TableOutput>> {
    let window = Window {
        from: Some(from_block),
        to: to_block,
    };

    // Tables are independent, and one table's scan rarely has row groups
    // enough to keep every thread busy.
    let scanned: Vec<Result<(String, TableOutput)>> = execution
        .tables
        .par_iter()
        .map(|table| scan_table(execution, table, window))
        .collect();

    // The first failure in plan order, the one a serial scan would report.
    scanned.into_iter().collect()
}

/// One table's rows in `window`, and each of its relations' rows.
fn scan_table(
    execution: &Execution,
    table: &PlannedTable,
    window: Window,
) -> Result<(String, TableOutput)> {
    let chunk = execution.scan_reader();
    let profile = execution.options.profile;
    let (table_plan, table_desc) = (table.plan, table.desc);

    let mut request = ScanRequest::new(table.scan_columns.clone());
    request.predicates = table_plan.predicates.iter().collect();
    request.from_block = window.from;
    request.to_block = window.to;
    request.window = Some(window);
    request.block_number_column = Some(table_desc.block_number_column.as_str());
    request.required_columns = table.projection.required().to_vec();
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

    let started = Stopwatch::start(profile);
    let scanned = chunk.scan_rows(&table_plan.table, &request)?;
    let primary = scanned.rows().batches();
    started.report_with("primary scan", || {
        format!("{} rows", scanned.rows().num_rows())
    });

    // Compute actual block range from primary scan for cross-table pruning
    let bn_col_name = table_desc.block_number_column.as_str();
    let block_range = compute_block_range(primary, bn_col_name)?;

    let mut relations = HashMap::new();
    let has_primary_rows = primary.iter().any(|b| b.num_rows() > 0);
    if has_primary_rows && !table_plan.relations.is_empty() {
        let started = Stopwatch::start(profile);
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
        let inputs = relation_inputs(table, &sources);
        started.report("key filter build");

        let started = Stopwatch::start(profile);
        let related: Vec<(usize, Result<Rows>)> = inputs
            .par_iter()
            .enumerate()
            .filter_map(|(index, input)| {
                if input.follows_nothing() {
                    return None;
                }
                let rows = scan_relation(input, chunk, window, block_range, profile);
                Some((index, rows))
            })
            .collect();
        started.report("relation scans+joins");

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
        target: &'a TableDescription,
        (source_table, source): (&str, &'a TableDescription),
    ) -> Option<Self> {
        let mode = match relation.kind {
            RelationKind::Children => HierarchicalMode::Children,
            RelationKind::Parents => HierarchicalMode::Parents,
            RelationKind::Join => return None,
        };
        let target_address = target.address_column.as_deref()?;
        let source_address = source.address_column.as_deref().unwrap_or(target_address);
        // Another table's row at the source's address belongs to it (INV-R8),
        // whatever the two address columns are called.
        let inclusive = relation.target_table != source_table;

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
    target: &TableDescription,
) -> Option<(Vec<&'a str>, Vec<&'a str>)> {
    let (left, right) = match relation.kind {
        RelationKind::Join => (
            relation.left_key.iter().map(String::as_str).collect(),
            relation.right_key.iter().map(String::as_str).collect(),
        ),
        RelationKind::Children | RelationKind::Parents => {
            let address = target.address_column.as_deref();
            (
                group_keys_for_relation(&relation.left_key, address),
                group_keys_for_relation(&relation.right_key, address),
            )
        }
    };

    let usable = !left.is_empty() && !right.is_empty();
    usable.then_some((left, right))
}

/// A relation's key without the address column: the columns a hierarchical
/// relation groups rows by.
fn group_keys_for_relation<'a>(keys: &'a [String], address: Option<&str>) -> Vec<&'a str> {
    keys.iter()
        .map(String::as_str)
        .filter(|key| Some(*key) != address)
        .collect()
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
    /// The fields of the target.
    target: &'a ResolvedProjection<'a>,
    /// How a `children` or `parents` relation walks the address; `None` for a
    /// join.
    hierarchy: Option<HierarchicalSpec<'a>>,
    /// The primary rows it follows: all of them, or those its own items matched.
    source: &'a [RecordBatch],
    key_filter: Option<KeyFilter>,
    hierarchical_filter: Option<HierarchicalFilter>,
    /// Every row it follows has a null in its key or address (INV-R5).
    keyless: bool,
}

impl RelationInput<'_> {
    /// Whether no target row can relate to the rows it follows, so that the
    /// target is never opened.
    fn follows_nothing(&self) -> bool {
        self.keyless || self.source.iter().all(|batch| batch.num_rows() == 0)
    }
}

/// Each relation's input. Relations that follow the same rows share the key
/// set and the address index built from them.
fn relation_inputs<'a>(
    table: &'a PlannedTable<'a>,
    sources: &'a BuiltOnce<Option<&'a [usize]>, Vec<RecordBatch>>,
) -> Vec<RelationInput<'a>> {
    let source = (table.plan.table.as_str(), table.desc);
    let primary_bn = table.desc.block_number_column.as_str();
    let followed = |relation: &'a RelationPlan| relation.source_items.as_deref();
    let hierarchy = |relation: &'a RelationPlan, target: &'a TableDescription| {
        HierarchicalSpec::of(relation, target, source)
    };
    let address_spec = |relation: &'a RelationPlan, target: &'a TableDescription| {
        hierarchy(relation, target).filter(|spec| !spec.group_keys.is_empty())
    };

    let key_sets = BuiltOnce::build(
        table.relations().filter_map(|(relation, target)| {
            let (left, _) = pushdown_keys(relation, target.desc())?;
            Some((followed(relation), left))
        }),
        |(items, left)| KeySet::build(sources.get(items), left, primary_bn),
    );
    let address_indexes = BuiltOnce::build(
        table.relations().filter_map(|(relation, target)| {
            let spec = address_spec(relation, target.desc())?;
            Some((followed(relation), spec.group_keys, spec.source_address))
        }),
        |(items, group_keys, address)| AddressIndex::build(sources.get(items), group_keys, address),
    );

    table
        .relations()
        .map(|(relation, target)| {
            let items = followed(relation);
            let target_desc = target.desc();
            let target_bn = target_desc.block_number_column.as_str();

            let keys = pushdown_keys(relation, target_desc)
                .map(|(left, right)| (key_sets.get(&(items, left)), right));
            let hierarchy = hierarchy(relation, target_desc);
            let addresses = hierarchy
                .as_ref()
                .filter(|spec| !spec.group_keys.is_empty())
                .map(|spec| {
                    let group_keys = spec.group_keys.clone();
                    let index = address_indexes.get(&(items, group_keys, spec.source_address));
                    (index, spec.target_address, spec.mode, spec.inclusive)
                });
            let keyless = keys.as_ref().is_some_and(|(set, _)| set.matches_nothing())
                || addresses
                    .as_ref()
                    .is_some_and(|(index, ..)| index.matches_nothing());

            let key_filter = keys
                .map(|(set, right)| set.filter(&right, target_bn))
                .filter(|filter| !filter.is_empty());
            let hierarchical_filter = addresses
                .map(|(index, target, mode, inclusive)| index.filter(target, mode, inclusive))
                .filter(|filter| !filter.is_empty());

            RelationInput {
                relation,
                target,
                hierarchy,
                source: sources.get(&items),
                key_filter,
                hierarchical_filter,
                keyless,
            }
        })
        .collect()
}

/// Read the target rows one relation relates to its source rows.
fn scan_relation(
    input: &RelationInput,
    chunk: &dyn ChunkReader,
    window: Window,
    (from_block, to_block): (Option<u64>, Option<u64>),
    profile: bool,
) -> Result<Rows> {
    let relation = input.relation;
    let target_desc = input.target.desc();

    let mut request = ScanRequest::new(input.target.row_columns().to_vec());
    request.from_block = from_block;
    request.to_block = to_block;
    request.window = Some(window);
    request.required_columns = input.target.required().to_vec();
    request.block_number_column = Some(target_desc.block_number_column.as_str());
    request.key_filter = input.key_filter.as_ref();
    request.hierarchical_filter = input.hierarchical_filter.as_ref();

    let started = Stopwatch::start(profile);
    let target = chunk
        .scan_rows(&relation.target_table, &request)?
        .into_rows();
    started.report_with(format_args!("  {} scan", relation.target_table), || {
        format!("{} rows", target.num_rows())
    });

    // A filter the scan applied already kept only the related rows.
    let started = Stopwatch::start(profile);
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
        let Some(spec) = &input.hierarchy else {
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

    started.report(format_args!("  {} join", relation.target_table));
    Ok(related)
}
