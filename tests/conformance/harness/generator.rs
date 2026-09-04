//! HC-4 — the query generator.
//!
//! §8.4 and §8.5 are tables of laws over *pairs* of queries: a union, a
//! widening, an idempotence. A hand-written case can only assert a law at one
//! pair, and the pair a person picks is the pair they already believe works. So
//! this walks a catalog and a chunk and produces the pairs.
//!
//! Three rules keep it from reporting green having compared nothing.
//!
//! Values come from the chunk's actual contents. A filter that matches nothing
//! satisfies every law in both tables — ∅ ∪ ∅ = ∅ — so a generator that invents
//! its values proves the engine returns no rows, over and over.
//!
//! The seed is fixed. A law that fails on one run in twenty is a law nobody
//! trusts, and a counterexample that cannot be replayed is not a bug report.
//!
//! And what it cannot generate, it counts. The filter surface is wider than the
//! in-lists here: a discriminator, a bloom, a range bound and a `gteConst` flag
//! all take value shapes of their own. Those are skipped, and [`Generator::skipped`]
//! says how many, so a catalog whose whole surface fell through the cracks does
//! not look like a catalog that passed.

use arrow::array::{
    Array, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use serde_json::{Map, Value};
use sqd_query_engine::metadata::{DatasetDescription, SpecialFilter, TableDescription};
use sqd_query_engine::output::snake_to_camel;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::harness::chunk::{column_names, read_columns};
use crate::harness::fixtures::run_against;

/// How many distinct values of one column a filter may be offered. Enough that a
/// generated list is a choice rather than the whole column, few enough that the
/// laws stay fast.
const VALUES_PER_FILTER: usize = 12;

/// Past this cardinality a column stops yielding an absent value: knowing a
/// value is *not* in a column means having seen all of them, and the laws that
/// need one are worth less than a scan of a chunk's widest column.
const DISTINCT_CAP: usize = 4096;

// ---------------------------------------------------------------------------
// The seeded generator
// ---------------------------------------------------------------------------

/// SplitMix64: small, and identical on every platform — which `DefaultHasher` is
/// not. A counterexample that only reproduces on the machine that found it is not
/// a bug report.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);

        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);

        z ^ (z >> 31)
    }

    /// A number below `n`, or 0 when there is nothing to choose from.
    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }

        (self.next() % n as u64) as usize
    }

    pub fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    /// A subset, in the original order — possibly empty, possibly all of it.
    pub fn subset<'a, T>(&mut self, items: &'a [T]) -> Vec<&'a T> {
        items.iter().filter(|_| self.chance(50)).collect()
    }

    /// One of `items`, which must not be empty.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

// ---------------------------------------------------------------------------
// What the chunk holds
// ---------------------------------------------------------------------------

/// One filter of one table: what a request may give it.
pub struct FilterCorpus {
    /// The request key, camelCased the way a client writes it.
    pub key: String,
    /// Distinct values the chunk holds, in the order it holds them.
    pub present: Vec<Value>,
    /// One value of the same shape the chunk does not hold, where the column's
    /// cardinality was small enough to be sure of that.
    pub absent: Option<Value>,
    /// Whether the catalog marks the column case-insensitive. Carried here
    /// because [INV-P8] makes folding a property of the column and a test that
    /// decides for itself which columns fold is a test asserting its own guess.
    ///
    /// [INV-P8]: ../../../spec/07-invariants.md#inv-p8
    pub folds_case: bool,
}

impl FilterCorpus {
    /// One to three present values, and now and then the absent one alongside
    /// them — a list that matches something and carries a miss is the shape that
    /// finds a filter which drops the whole list on one bad value.
    pub fn values(&self, rng: &mut Rng) -> Vec<Value> {
        let wanted = 1 + rng.below(3.min(self.present.len()));
        let mut values: Vec<Value> = (0..wanted)
            .map(|_| rng.pick(&self.present).clone())
            .collect();
        values.dedup();

        if let Some(absent) = &self.absent {
            if rng.chance(25) {
                values.push(absent.clone());
            }
        }

        values
    }

    /// The present values cut in two, so a law can compare the union of two
    /// item requests against one carrying both halves. Empty halves are allowed:
    /// `Q([]) ∪ Q(all)` is as much an instance of the law as an even split.
    pub fn split(&self, rng: &mut Rng) -> (Vec<Value>, Vec<Value>) {
        let at = rng.below(self.present.len() + 1);

        (self.present[..at].to_vec(), self.present[at..].to_vec())
    }
}

/// One relation, under both the names it has.
///
/// A request writes the key camelCased and the catalog keys its map in snake
/// case, so a relation named in two words — `state_diffs`, `transaction_traces`,
/// `inner_instructions` — is spelled differently on each side. Carrying the
/// target here rather than converting the key back means no law has to know
/// that, and none of them panics on a catalog whose relations are not all one
/// word.
pub struct Relation {
    /// The key an item request flags it under.
    pub key: String,
    /// The catalog's name for the table it reaches.
    pub target: String,
}

/// One queryable table: its request key, its filters and its relations.
pub struct TableCorpus {
    /// The catalog's name for it.
    pub name: String,
    /// The column its block numbers sit in.
    block_number_column: String,
    /// The key an item request sits under.
    pub query_name: String,
    pub filters: Vec<FilterCorpus>,
    /// The relations whose target table the chunk carries.
    pub relations: Vec<Relation>,
}

impl TableCorpus {
    /// The filters with at least two distinct values — what a law needs to split
    /// a list in two and still be comparing anything.
    ///
    /// All of them rather than the first: which filter a law splits decides what
    /// its two halves can tell apart, and the one declared first is nobody's
    /// considered choice.
    pub fn splittable_filters(&self) -> Vec<&FilterCorpus> {
        self.filters
            .iter()
            .filter(|f| f.present.len() >= 2)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// One item request
// ---------------------------------------------------------------------------

/// Filters and relation flags kept apart, so a law can drop a flag without
/// re-deriving the filters it was paired with.
#[derive(Clone, Debug, Default)]
pub struct ItemRequest {
    pub filters: BTreeMap<String, Value>,
    pub relations: BTreeSet<String>,
}

impl ItemRequest {
    pub fn to_json(&self) -> Value {
        let mut obj = Map::new();
        for (key, value) in &self.filters {
            obj.insert(key.clone(), value.clone());
        }
        for relation in &self.relations {
            obj.insert(relation.clone(), Value::Bool(true));
        }

        Value::Object(obj)
    }

    pub fn with(mut self, relation: &str) -> Self {
        self.relations.insert(relation.to_string());
        self
    }

    pub fn without(mut self, relation: &str) -> Self {
        self.relations.remove(relation);
        self
    }

    pub fn without_relations(mut self) -> Self {
        self.relations.clear();
        self
    }

    pub fn filtering(key: &str, values: Vec<Value>) -> Self {
        Self {
            filters: BTreeMap::from([(key.to_string(), Value::Array(values))]),
            relations: BTreeSet::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// The generator
// ---------------------------------------------------------------------------

pub struct Generator {
    catalog: DatasetDescription,
    chunk: PathBuf,
    tables: Vec<TableCorpus>,
    blocks: (u64, u64),
    /// Every field of every table the chunk carries. Held constant across a
    /// law's queries: two responses compared under different projections
    /// compare renderings, not rows.
    fields: Value,
    skipped: usize,
}

impl Generator {
    pub fn new(catalog: DatasetDescription, chunk: &Path) -> Self {
        let blocks = block_range(&catalog, chunk);

        let mut tables = Vec::new();
        let mut fields = Map::new();
        let mut skipped = 0;

        for (name, table) in &catalog.tables {
            let Some(columns) = column_names(chunk, name) else {
                continue;
            };

            if let Some(output_name) = &table.output.name {
                let selected = projection(table, &columns);
                if !selected.is_empty() {
                    fields.insert(output_name.clone(), Value::Object(selected));
                }
            }

            if table.is_block_table() {
                continue;
            }

            let Some(query_name) = &table.request.name else {
                continue;
            };

            let (filters, unsupported) = filters_of(chunk, name, &catalog, &columns);
            skipped += unsupported;

            tables.push(TableCorpus {
                name: name.clone(),
                block_number_column: table.block_number_column.clone(),
                query_name: query_name.clone(),
                filters,
                relations: relations_of(chunk, table),
            });
        }

        Self {
            catalog,
            chunk: chunk.to_path_buf(),
            tables,
            blocks,
            fields: Value::Object(fields),
            skipped,
        }
    }

    pub fn tables(&self) -> &[TableCorpus] {
        &self.tables
    }

    /// The table the catalog calls `name`, or a panic — a law naming a table the
    /// corpus does not have is a law that would otherwise pass vacuously.
    pub fn table(&self, name: &str) -> &TableCorpus {
        self.tables
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("the chunk carries no queryable table '{name}'"))
    }

    /// Tables with something to filter on. A table with no filter corpus can
    /// still be a relation target, but it cannot carry a law about filters.
    pub fn filterable(&self) -> Vec<&TableCorpus> {
        self.tables
            .iter()
            .filter(|t| !t.filters.is_empty())
            .collect()
    }

    /// The chunk's block range.
    pub fn blocks(&self) -> (u64, u64) {
        self.blocks
    }

    /// How many filters the generator could not supply values for.
    pub fn skipped(&self) -> usize {
        self.skipped
    }

    /// A random item request: a filter subset with values from the chunk, and a
    /// relation subset.
    pub fn item_request(&self, table: &TableCorpus, rng: &mut Rng) -> ItemRequest {
        let filters = rng
            .subset(&table.filters)
            .into_iter()
            .map(|filter| (filter.key.clone(), Value::Array(filter.values(rng))))
            .collect::<Vec<_>>();

        let relations = rng
            .subset(&table.relations)
            .into_iter()
            .map(|relation| relation.key.clone())
            .collect();

        ItemRequest {
            filters: filters.into_iter().collect(),
            relations,
        }
    }

    /// A sub-range of the chunk, sometimes the whole of it.
    pub fn range(&self, rng: &mut Rng) -> (u64, u64) {
        let (first, last) = self.blocks;
        let span = last - first;
        let from = first + rng.below(span as usize + 1) as u64;
        let to = from + rng.below((last - from) as usize + 1) as u64;

        (from, to)
    }

    /// A whole query over the given item requests, projecting every field of
    /// every table.
    pub fn query(
        &self,
        range: (u64, u64),
        requests: &[(&TableCorpus, Vec<ItemRequest>)],
    ) -> String {
        let mut root = Map::new();
        root.insert("type".to_string(), Value::from(self.catalog.name.clone()));
        root.insert("fromBlock".to_string(), Value::from(range.0));
        root.insert("toBlock".to_string(), Value::from(range.1));
        root.insert("fields".to_string(), self.fields.clone());

        for (table, items) in requests {
            root.insert(
                table.query_name.clone(),
                Value::Array(items.iter().map(ItemRequest::to_json).collect()),
            );
        }

        Value::Object(root).to_string()
    }

    /// How many rows of a table the chunk holds in a block range.
    ///
    /// Read off the chunk rather than from another query, because it is what
    /// [INV-P1] and [INV-P6] compare a query *against*: an unfiltered request
    /// answered from the engine's own idea of the whole table would agree with
    /// itself however wrong it was.
    ///
    /// [INV-P1]: ../../../spec/07-invariants.md#inv-p1
    /// [INV-P6]: ../../../spec/07-invariants.md#inv-p6
    pub fn rows_in_range(&self, table: &TableCorpus, range: (u64, u64)) -> usize {
        let numbers = column_values(&self.chunk, &table.name, &table.block_number_column)
            .expect("a queryable table carries its block number column");

        numbers
            .iter()
            .filter_map(|v| v.as_u64())
            .filter(|n| (range.0..=range.1).contains(n))
            .count()
    }

    /// Run a generated query. A generated query that fails to parse or plan is a
    /// bug in the generator or in the engine, never something a law may skip.
    pub fn run(&self, query: &str) -> Vec<u8> {
        run_against(&self.catalog, &self.chunk, query)
            .unwrap_or_else(|e| panic!("a generated query failed: {e:#}\n  query: {query}"))
    }
}

// ---------------------------------------------------------------------------
// Reading the chunk
// ---------------------------------------------------------------------------

fn block_range(catalog: &DatasetDescription, chunk: &Path) -> (u64, u64) {
    let (name, table) = catalog
        .tables
        .iter()
        .find(|(_, t)| t.is_block_table())
        .expect("a catalog has exactly one block table (INV-D3)");

    let numbers = column_values(chunk, name, &table.block_number_column)
        .expect("the chunk carries its block table");
    let numbers: Vec<u64> = numbers.iter().filter_map(|v| v.as_u64()).collect();

    assert!(!numbers.is_empty(), "the chunk's block table is empty");

    (
        *numbers.iter().min().unwrap(),
        *numbers.iter().max().unwrap(),
    )
}

/// Which fields a request may select.
///
/// A catalog names every field the dataset can have; a chunk carries the ones its
/// archiver version wrote, and selecting one it does not is an error (INV-E3). So
/// the projection is the intersection — which is also what keeps it the same on
/// both sides of a law.
fn projection(table: &TableDescription, columns: &[String]) -> Map<String, Value> {
    table
        .output
        .fields
        .iter()
        .filter(|field| {
            let backing = table.physical_output_column(field).unwrap_or(field);
            columns.iter().any(|c| c == backing)
        })
        .map(|field| (snake_to_camel(field), Value::Bool(true)))
        .collect()
}

/// Every filter of one table the generator can supply values for, and a count of
/// the ones it cannot.
///
/// The count is the point of returning it: a discriminator, a bloom, a range
/// bound and a `gteConst` flag each take a value shape of their own, and a
/// catalog whose whole surface fell through here must not look like one that
/// passed.
fn filters_of(
    chunk: &Path,
    name: &str,
    catalog: &DatasetDescription,
    columns: &[String],
) -> (Vec<FilterCorpus>, usize) {
    let table = catalog
        .table(name)
        .expect("the table came from the catalog");

    let mut filters = Vec::new();
    let mut skipped = 0;

    for key in &table.request.filters {
        let column = match table.request.special_filters.get(key) {
            None => key.as_str(),
            Some(SpecialFilter::ColumnAlias { column }) => column.as_str(),
            Some(_) => {
                skipped += 1;
                continue;
            }
        };

        match filter_corpus(chunk, name, key, column, catalog, columns) {
            Some(corpus) => filters.push(corpus),
            None => skipped += 1,
        }
    }

    (filters, skipped)
}

/// The relations whose target table this chunk carries.
fn relations_of(chunk: &Path, table: &TableDescription) -> Vec<Relation> {
    table
        .request
        .relations
        .iter()
        .filter(|(_, def)| column_names(chunk, &def.table).is_some())
        .map(|(key, def)| Relation {
            key: snake_to_camel(key),
            target: def.table.clone(),
        })
        .collect()
}

/// The corpus for one filter, or `None` when the column is absent from the chunk
/// or holds values no request can carry.
fn filter_corpus(
    chunk: &Path,
    table: &str,
    key: &str,
    column: &str,
    catalog: &DatasetDescription,
    columns: &[String],
) -> Option<FilterCorpus> {
    if !columns.iter().any(|c| c == column) {
        return None;
    }

    let values = column_values(chunk, table, column)?;
    let folds = catalog
        .table(table)
        .and_then(|t| t.column(column))
        .is_some_and(|c| c.folds_case());

    let mut distinct: Vec<Value> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut overflowed = false;

    for value in values {
        if seen.len() >= DISTINCT_CAP {
            overflowed = true;
            break;
        }
        if seen.insert(fingerprint(&value, folds)) {
            distinct.push(value);
        }
    }

    if distinct.is_empty() {
        return None;
    }

    let absent = (!overflowed)
        .then(|| absent_value(&distinct[0], &seen, folds))
        .flatten();
    distinct.truncate(VALUES_PER_FILTER);

    Some(FilterCorpus {
        key: snake_to_camel(key),
        present: distinct,
        absent,
        folds_case: folds,
    })
}

/// How a value is compared for distinctness: a case-folding column holds `0xAB`
/// and `0xab` once, and an "absent" value differing only in case is present.
fn fingerprint(value: &Value, folds: bool) -> String {
    match (value, folds) {
        (Value::String(s), true) => s.to_lowercase(),
        _ => value.to_string(),
    }
}

/// A value of the same shape as `like` that `seen` does not hold.
///
/// Counting rather than randomising: the search has to terminate, and over a
/// small set it does so on the first or second try.
fn absent_value(like: &Value, seen: &BTreeSet<String>, folds: bool) -> Option<Value> {
    let candidates: Vec<Value> = match like {
        Value::String(s) if s.starts_with("0x") => {
            let width = s.len() - 2;
            (0..64u64)
                .map(|n| Value::from(format!("0x{n:0width$x}")))
                .collect()
        }
        Value::String(s) => (0..64u64).map(|n| Value::from(format!("{s}{n}"))).collect(),
        Value::Number(n) => {
            let base = n.as_u64()?;
            (1..64u64)
                .map(|d| Value::from(base.saturating_add(d)))
                .collect()
        }
        _ => return None,
    };

    candidates
        .into_iter()
        .find(|c| !seen.contains(&fingerprint(c, folds)))
}

/// Every non-null value of one column, in the order the chunk stores them.
///
/// `None` when the chunk has no such table or column, or when the column's
/// physical type is not one a JSON filter value can express.
fn column_values(chunk: &Path, table: &str, column: &str) -> Option<Vec<Value>> {
    let batches = read_columns(chunk, table, &[column])?;
    let mut values = Vec::new();

    for batch in &batches {
        let array = batch.column(0);

        // A dictionary-encoded column arrives as its indices; the filter wants
        // what they point at.
        let decoded = match array.data_type() {
            DataType::Dictionary(_, value_type) => cast(array, value_type).ok()?,
            _ => array.clone(),
        };

        for row in 0..decoded.len() {
            if let Some(value) = scalar_at(decoded.as_ref(), row) {
                values.push(value);
            }
        }
    }

    (!values.is_empty()).then_some(values)
}

/// One cell as a filter would write it, or `None` for a null and for the types a
/// list filter cannot carry — lists, binary, floats, timestamps.
fn scalar_at(array: &dyn Array, row: usize) -> Option<Value> {
    if array.is_null(row) {
        return None;
    }

    macro_rules! read {
        ($ty:ty) => {
            Some(Value::from(
                array.as_any().downcast_ref::<$ty>()?.value(row),
            ))
        };
    }

    match array.data_type() {
        DataType::Utf8 => read!(StringArray),
        DataType::LargeUtf8 => read!(LargeStringArray),
        DataType::Boolean => read!(BooleanArray),
        DataType::Int8 => read!(Int8Array),
        DataType::Int16 => read!(Int16Array),
        DataType::Int32 => read!(Int32Array),
        DataType::Int64 => read!(Int64Array),
        DataType::UInt8 => read!(UInt8Array),
        DataType::UInt16 => read!(UInt16Array),
        DataType::UInt32 => read!(UInt32Array),
        DataType::UInt64 => read!(UInt64Array),
        _ => None,
    }
}
