//! A set passes exactly the rows its lookup passes (§4.2.11), on every filter
//! that takes one, against the rows a fixture chunk holds.

use serde_json::{json, Map, Value};
use sqd_query_engine::metadata::DatasetDescription;
use std::collections::{BTreeMap, BTreeSet};

use crate::harness::fixtures::{fixture_tree_has, meta, run_page, FIXTURE_DATASETS};
use crate::harness::json::{items_in, items_of, parse_response};
use crate::harness::sets::{every_filter, BuiltSet, CatalogFilter};

/// The rows of one request key on one fixture chunk, with every field that
/// renders a column a set may filter on, cut to about twenty thousand.
struct Rows<'a> {
    dataset: &'a str,
    catalog: &'a DatasetDescription,
    request: &'a str,
    response_key: String,
    fields: Value,
    /// The block every query is cut at.
    last: u64,
    every_row: Vec<(u64, String)>,
    /// Every string the rows carry. A filtered column's text is among them,
    /// wherever the field that renders it nests it.
    seen: BTreeSet<String>,
}

impl<'a> Rows<'a> {
    fn read(
        dataset: &'a str,
        catalog: &'a DatasetDescription,
        request: &'a str,
        filters: &[&CatalogFilter],
    ) -> Rows<'a> {
        let first = filters[0];
        let table = catalog.table(&first.table).unwrap();
        let selected: Map<String, Value> = filters
            .iter()
            .flat_map(|filter| &filter.fields)
            .map(|field| (field.clone(), json!(true)))
            .collect();

        let mut rows = Rows {
            dataset,
            catalog,
            request,
            response_key: table.request_name(&first.table).to_string(),
            fields: json!({ "block": { "number": true } }),
            last: 0,
            every_row: Vec::new(),
            seen: BTreeSet::new(),
        };

        // Counted first with no field selected, which costs little. A query
        // cut at the same block holds fewer rows, so one page of it reaches
        // that block too.
        let (counts, page_end) = rows.page(json!({}), json!({}), None);
        let mut counted = 0;
        rows.last = parse_response(&counts)
            .iter()
            .find_map(|block| {
                counted += items_in(block, &rows.response_key).len();
                (counted >= 20_000).then(|| block["header"]["number"].as_u64().unwrap())
            })
            .unwrap_or(page_end);

        rows.fields = json!({ "block": { "number": true }, (first.output.clone()): selected });
        let every_row = rows.answer(json!({}), json!({}));
        for (_, item) in items_of(&every_row, &rows.response_key) {
            strings(&item, &mut rows.seen);
        }
        rows.every_row = sorted(&every_row, &rows.response_key);

        rows
    }

    /// One page of the request with a single item, and the block it ends at.
    fn page(&self, item: Value, sets: Value, to: Option<u64>) -> (Vec<u8>, u64) {
        let query = json!({
            "type": self.catalog.name,
            "fromBlock": 0,
            "toBlock": to,
            "sets": sets,
            "fields": self.fields,
            (self.request): [item],
        });

        run_page(self.dataset, self.catalog, query.to_string().as_bytes())
            .unwrap_or_else(|e| panic!("{}: {}: {e:#}", self.dataset, self.request))
            .unwrap_or_else(|| panic!("{}: the range misses the chunk", self.dataset))
    }

    /// The rows up to the cut, in one page.
    fn answer(&self, item: Value, sets: Value) -> Vec<u8> {
        let (body, ends_at) = self.page(item, sets, Some(self.last));
        assert_eq!(
            ends_at, self.last,
            "{}: {}: a narrower query stopped short",
            self.dataset, self.request
        );
        body
    }
}

/// Every string anywhere in `value`.
fn strings(value: &Value, found: &mut BTreeSet<String>) {
    match value {
        Value::String(text) => {
            found.insert(text.clone());
        }
        Value::Array(values) => values.iter().for_each(|v| strings(v, found)),
        Value::Object(members) => members.values().for_each(|v| strings(v, found)),
        _ => {}
    }
}

/// The items under `key`, sorted, with their blocks. A multiset rather than a
/// set: the query selects little more than the filtered fields, so two rows of
/// one block often render alike, and a set would count them once.
fn sorted(body: &[u8], key: &str) -> Vec<(u64, String)> {
    let mut rows: Vec<(u64, String)> = items_of(body, key)
        .into_iter()
        .map(|(block, item)| (block, item.to_string()))
        .collect();
    rows.sort();
    rows
}

/// Whether every row of `part` is in `whole` as many times, both sorted.
fn contains_all(whole: &[(u64, String)], part: &[(u64, String)]) -> bool {
    let mut whole = whole.iter().peekable();

    part.iter().all(|row| {
        while whole.next_if(|candidate| *candidate < row).is_some() {}
        whole.next_if(|candidate| *candidate == row).is_some()
    })
}

/// Fails at the first row where the answers by a set and by its lookup differ.
fn same_rows(what: &str, by_set: &[(u64, String)], by_lookup: &[(u64, String)]) {
    let differs =
        (0..by_set.len().max(by_lookup.len())).find(|&i| by_set.get(i) != by_lookup.get(i));

    if let Some(at) = differs {
        panic!(
            "{what}: {} rows by the set, {} by the lookup; first difference at row {at}: \
             {:?} by the set, {:?} by the lookup",
            by_set.len(),
            by_lookup.len(),
            by_set.get(at),
            by_lookup.get(at)
        );
    }
}

/// What one comparison saw: whether the set left any row out, and how many rows
/// it passed beyond its members'.
struct Compared {
    narrowed: bool,
    extra_rows: usize,
}

/// A set on `filter` against a list of every value the builder's own lookup
/// passes; `None` where the rows hold no value to build one from.
fn compare(rows: &Rows, filter: &CatalogFilter) -> Option<Compared> {
    let table = rows.catalog.table(&filter.table).unwrap();
    let column = table.column(filter.column.as_ref().unwrap()).unwrap();
    let rendering = filter.rendering.unwrap();
    let what = format!(
        "{}: a set on {}.{}",
        rows.dataset, filter.request, filter.key
    );

    let seen: BTreeSet<String> = rows
        .seen
        .iter()
        .map(|text| match column.folds_case() {
            true => text.to_ascii_lowercase(),
            false => text.clone(),
        })
        .collect();

    // Half the values, or fewer where so many would not fit in a request as the
    // list the set is compared against.
    let candidates: Vec<&String> = seen.iter().filter(|text| rendering.holds(text)).collect();
    let step = candidates.len().div_ceil(10_000).max(2);
    let members: Vec<&String> = candidates.iter().step_by(step).copied().collect();
    if members.is_empty() {
        return None;
    }

    let set = BuiltSet::of(&members);
    let canaries: Vec<&String> = members
        .iter()
        .step_by(members.len().div_ceil(4))
        .copied()
        .collect();
    let passed: Vec<&String> = seen.iter().filter(|text| set.passes(text)).collect();

    let key = filter.key.clone();
    let by_set = sorted(
        &rows.answer(
            json!({ (key.clone()): { "set": "s" } }),
            json!({ "s": set.declared(&canaries) }),
        ),
        &rows.response_key,
    );
    let by_lookup = sorted(
        &rows.answer(json!({ (key.clone()): passed }), json!({})),
        &rows.response_key,
    );
    let by_members = sorted(
        &rows.answer(json!({ (key): members }), json!({})),
        &rows.response_key,
    );

    same_rows(&what, &by_set, &by_lookup);
    assert!(
        contains_all(&by_set, &by_members),
        "{what} lost rows of its members"
    );

    // The same members as a list of values and two sets beside them, which
    // passes the values and what either set's lookup passes.
    if members.len() >= 3 {
        let third = members.len() / 3;
        let (listed, rest) = members.split_at(third);
        let (in_a, in_b) = rest.split_at(third);
        let (a, b) = (BuiltSet::of(in_a), BuiltSet::of(in_b));

        let mut list: Vec<Value> = listed.iter().map(|value| json!(value)).collect();
        list.extend([json!({ "set": "a" }), json!({ "set": "b" })]);
        let sets = json!({ "a": a.declared(&in_a[..1]), "b": b.declared(&in_b[..1]) });
        let by_list = sorted(
            &rows.answer(json!({ (filter.key.clone()): list }), sets),
            &rows.response_key,
        );

        let listed: BTreeSet<&String> = listed.iter().copied().collect();
        let passed: Vec<&String> = seen
            .iter()
            .filter(|text| listed.contains(text) || a.passes(text) || b.passes(text))
            .collect();
        let by_lookup = sorted(
            &rows.answer(json!({ (filter.key.clone()): passed }), json!({})),
            &rows.response_key,
        );

        same_rows(
            &format!("{what}, listed beside values"),
            &by_list,
            &by_lookup,
        );
        assert!(
            contains_all(&by_list, &by_members),
            "{what}, listed beside values, lost rows of its members"
        );
    }

    Some(Compared {
        narrowed: by_set.len() < rows.every_row.len(),
        extra_rows: by_set.len() - by_members.len(),
    })
}

/// For every filter of every fixture catalog that takes a set, a set is built
/// from half the values its column renders, or ten thousand of them. The answer
/// must equal the answer to a list of every value the builder's own lookup
/// passes, which is the rows the lookup passes: a list holds each row it names,
/// and no other. Since that list holds the members, the answer also holds the
/// answer to the members as a list.
///
/// The members are sent again as a list of a third of them and two sets of the
/// rest, and the answer must equal the answer to the listed values and every
/// value either set's lookup passes.
///
/// The list is drawn from every string the table's rows carry, a superset of the
/// column's values: a string the column does not hold matches nothing.
///
/// Covers CT-3 · INV-P17
#[test]
#[ignore = "requires external fixture data"]
fn a_set_passes_exactly_what_its_lookup_passes() {
    let mut compared = 0;
    let mut narrowed = 0;
    let mut extra_rows = 0;

    for (dataset, name) in FIXTURE_DATASETS {
        if !fixture_tree_has(dataset) {
            continue;
        }
        let catalog = meta(name);

        // By the request key they are sent under, so that its rows are read once.
        let filters = every_filter(&catalog);
        let mut by_request: BTreeMap<&str, Vec<&CatalogFilter>> = BTreeMap::new();
        for filter in &filters {
            if filter.rendering.is_some() && !filter.fields.is_empty() {
                by_request.entry(&filter.request).or_default().push(filter);
            }
        }

        for (request, filters) in &by_request {
            let rows = Rows::read(dataset, &catalog, request, filters);
            for filter in filters {
                let Some(seen) = compare(&rows, filter) else {
                    continue;
                };
                compared += 1;
                narrowed += usize::from(seen.narrowed);
                extra_rows += seen.extra_rows;
            }
        }
    }

    if fixture_tree_has("ethereum") {
        assert!(compared > 20, "only {compared} filters were compared");
        // A set that passes every row cannot tell an engine that drops rows from
        // one that does not; a column of one value makes such a set.
        assert!(
            narrowed * 2 > compared,
            "only {narrowed} of {compared} sets left any row out"
        );
        // Nor can the equality tell an engine that drops false positives from
        // one that keeps them, unless some set passed one.
        assert!(extra_rows > 0, "no set passed a row outside its members");
    }
}
