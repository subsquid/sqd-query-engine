//! §8.4's table, asserted over generated queries.
//!
//! Each row of that table is an equality or an inclusion between a *pair* of
//! queries. A hand-written case asserts one such law at one pair — and the pair
//! a person picks is the pair they already believe works. Here the pairs come
//! from HC-4: filter subsets, value subsets and block ranges the generator
//! composed, with values read out of the chunk.
//!
//! Two of the laws stay hand-written as well, in `surface` and `values`. §8.4
//! names them as the two catastrophic misreadings, and a law that exists only
//! inside a generator stops existing the day somebody quarantines the generator.

use std::collections::BTreeSet;
use tempfile::TempDir;

use crate::harness::evm_like;
use crate::harness::fixtures::{fixture_chunk, fixture_tree_is_present, meta};
use crate::harness::generator::{Generator, ItemRequest, Rng, TableCorpus};
use crate::harness::json::{assert_same_response, block_numbers, parse_response, row_set};

/// Recorded, so a failure replays. Changing it is changing the test.
const SEED: u64 = 0x5EED_0003;

/// Enough cases that each law meets several filter combinations and both ends of
/// the chunk; few enough that CT-3 stays inside a per-PR gate.
const CASES: usize = 48;

type Rows = BTreeSet<(u64, String)>;

/// The chunk has to outlive the generator, which holds a path into it.
fn corpus() -> (TempDir, Generator) {
    let chunk = evm_like::chunk();
    let generator = Generator::new(evm_like::catalog(), chunk.path());

    (chunk, generator)
}

/// Run a generated query, having first checked it answered the whole range it
/// was given.
///
/// The laws below compare two responses. A weight budget that trimmed one of
/// them would turn every inclusion into a triviality and every equality into a
/// mystery, so a short answer is a failure here rather than a difference to
/// explain away.
fn answer(generator: &Generator, query: &str, range: (u64, u64)) -> Vec<u8> {
    let body = generator.run(query);
    let blocks = block_numbers(&parse_response(&body));

    assert_eq!(
        (blocks.first().copied(), blocks.last().copied()),
        (Some(range.0), Some(range.1)),
        "the response does not cover blocks {}..={}: {query}",
        range.0,
        range.1
    );

    body
}

/// The rows one table returns for one set of item requests on it.
fn rows_of(
    generator: &Generator,
    table: &TableCorpus,
    range: (u64, u64),
    items: Vec<ItemRequest>,
) -> Rows {
    let query = generator.query(range, &[(table, items)]);

    row_set(&answer(generator, &query, range), &table.query_name)
}

/// A table with something to filter on, and a range to filter it over.
fn case<'a>(generator: &'a Generator, rng: &mut Rng) -> (&'a TableCorpus, (u64, u64)) {
    let tables = generator.filterable();
    assert!(
        !tables.is_empty(),
        "no table of this chunk has a filter the generator can supply values for, \
         so every law below would pass having compared nothing"
    );

    (*rng.pick(&tables), generator.range(rng))
}

/// A sweep that compared no rows proves nothing, and the way a filter surface
/// breaks is that everything stops matching. Every law counts what it saw.
fn assert_saw_rows(seen: usize, law: &str) {
    assert!(
        seen > 0,
        "{law} was asserted over {CASES} generated queries and not one returned a row"
    );
}

// ---------------------------------------------------------------------------
// The generator itself
// ---------------------------------------------------------------------------

/// What HC-4 built out of this chunk. A generator that quietly found no values
/// would make every law in this file and in CT-4 pass over empty responses, so
/// what it holds is asserted before anything is asserted with it.
///
/// Covers CT-3 · INV-P1
#[test]
fn the_generator_walks_the_whole_filter_surface() {
    let (_chunk, generator) = corpus();

    let filters: Vec<&str> = generator
        .tables()
        .iter()
        .flat_map(|t| t.filters.iter().map(|f| f.key.as_str()))
        .collect();

    assert_eq!(
        filters,
        ["address", "topic0", "transactionIndex", "kind"],
        "the generator's filter surface is not the catalog's"
    );
    assert_eq!(
        generator.skipped(),
        0,
        "this chain declares no filter kind the generator cannot supply"
    );
    assert_eq!(generator.blocks(), (100, 115));

    // A request spells a relation camelCased; the catalog keys its map in snake
    // case. Two of this chain's three are named in two words precisely so the
    // two spellings differ, and the surface is pinned as the request writes it.
    let relations: Vec<(&str, &str)> = generator
        .tables()
        .iter()
        .flat_map(|t| {
            t.relations
                .iter()
                .map(|r| (r.key.as_str(), r.target.as_str()))
        })
        .collect();

    assert_eq!(
        relations,
        [
            ("transaction", "transactions"),
            ("transactionLogs", "logs"),
            ("transactionTraces", "traces"),
        ],
        "the generator's relation surface is not the catalog's"
    );

    for table in generator.tables() {
        for filter in &table.filters {
            assert!(
                filter.present.len() >= 2,
                "{}.{} offers {} value(s); a law that splits a list needs two",
                table.name,
                filter.key,
                filter.present.len()
            );
            assert!(
                filter.absent.is_some(),
                "{}.{} offers no value the chunk lacks",
                table.name,
                filter.key
            );
        }
    }
}

// ---------------------------------------------------------------------------
// `Q([s₁]) ∪ Q([s₂])` = `Q([s₁, s₂])` — INV-P5
// ---------------------------------------------------------------------------

/// The law the U row named: item requests on one table disjoin, as a *set*. A
/// row matched by both is returned once, which is why the comparison is against
/// a union and not a concatenation.
///
/// Covers CT-3 · INV-P5
#[test]
fn item_requests_on_one_table_disjoin() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let mut seen = 0;

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);

        // Relations are INV-R11's half of this law; here it is the direct rows.
        let first = generator.item_request(table, &mut rng).without_relations();
        let second = generator.item_request(table, &mut rng).without_relations();

        let left = rows_of(&generator, table, range, vec![first.clone()]);
        let right = rows_of(&generator, table, range, vec![second.clone()]);
        let both = rows_of(
            &generator,
            table,
            range,
            vec![first.clone(), second.clone()],
        );

        let union: Rows = left.union(&right).cloned().collect();
        assert_eq!(
            union,
            both,
            "{}: {:?} then {:?} is not their union over blocks {}..={}",
            table.name,
            first.to_json(),
            second.to_json(),
            range.0,
            range.1
        );

        seen += both.len();
    }

    assert_saw_rows(seen, "the union of two item requests");
}

/// The same law read the other way: the order they are written in, and how many
/// times, do not reach the answer. Byte-identical rather than set-equal — two
/// item requests are a way of asking, not a way of ordering.
///
/// Covers CT-3 · INV-P5
#[test]
fn item_request_order_and_repetition_do_not_reach_the_answer() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let mut seen = 0;

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);

        let first = generator.item_request(table, &mut rng).without_relations();
        let second = generator.item_request(table, &mut rng).without_relations();

        let forwards = generator.query(range, &[(table, vec![first.clone(), second.clone()])]);
        let backwards = generator.query(range, &[(table, vec![second.clone(), first.clone()])]);
        let doubled = generator.query(
            range,
            &[(
                table,
                vec![first.clone(), second.clone(), first.clone(), second.clone()],
            )],
        );

        let expected = answer(&generator, &forwards, range);
        assert_same_response(
            &expected,
            &answer(&generator, &backwards, range),
            "reversing the item requests",
        );
        assert_same_response(
            &expected,
            &answer(&generator, &doubled, range),
            "repeating the item requests",
        );

        seen += row_set(&expected, &table.query_name).len();
    }

    assert_saw_rows(seen, "the order of item requests");
}

// ---------------------------------------------------------------------------
// A filter only ever narrows — INV-P1, INV-P4, INV-P6
// ---------------------------------------------------------------------------

/// An item request with no filters returns the table, and every filtered one
/// returns a subset of it. Counted against the chunk rather than against another
/// query: an engine that answered "the whole table" from its own idea of the
/// table would agree with itself however wrong it was.
///
/// The inclusion has to be *strict* somewhere, or an engine whose filters
/// silently no-op satisfies it everywhere. §8.4 says so in as many words, so the
/// sweep asserts it saw a proper subset.
///
/// Covers CT-3 · INV-P1, INV-P6
#[test]
fn an_unfiltered_item_request_is_the_whole_table_and_a_filter_only_narrows() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let (mut seen, mut narrowed) = (0, 0);

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);

        let whole = rows_of(&generator, table, range, vec![ItemRequest::default()]);
        assert_eq!(
            whole.len(),
            generator.rows_in_range(table, range),
            "an unfiltered {} request over blocks {}..={} is not the whole table",
            table.name,
            range.0,
            range.1
        );

        let filtered_request = generator.item_request(table, &mut rng).without_relations();
        let filtered = rows_of(&generator, table, range, vec![filtered_request.clone()]);

        assert!(
            filtered.is_subset(&whole),
            "{} under {:?} returns rows the unfiltered request does not",
            table.name,
            filtered_request.to_json()
        );

        seen += whole.len();
        narrowed += usize::from(filtered.len() < whole.len());
    }

    assert_saw_rows(seen, "an unfiltered item request");
    assert!(
        narrowed > 0,
        "no generated filter removed a single row, so a filter that never fires \
         would pass this test"
    );
}

/// Filters within one item request conjoin, so adding one can only take rows
/// away. The pair is built by *adding* to a request rather than by generating
/// two, so the two sides differ in exactly one filter.
///
/// Covers CT-3 · INV-P4
#[test]
fn conjoining_a_filter_only_narrows() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let (mut seen, mut narrowed) = (0, 0);

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);

        let mut wider = generator.item_request(table, &mut rng).without_relations();
        let extra = rng.pick(&table.filters);
        wider.filters.remove(&extra.key);

        let mut narrower = wider.clone();
        narrower.filters.insert(
            extra.key.clone(),
            serde_json::Value::Array(extra.values(&mut rng)),
        );

        let wide = rows_of(&generator, table, range, vec![wider]);
        let narrow = rows_of(&generator, table, range, vec![narrower.clone()]);

        assert!(
            narrow.is_subset(&wide),
            "{} under {:?} returns rows the same request without '{}' does not",
            table.name,
            narrower.to_json(),
            extra.key
        );

        seen += wide.len();
        narrowed += usize::from(narrow.len() < wide.len());
    }

    assert_saw_rows(seen, "conjoining a filter");
    assert!(narrowed > 0, "no conjoined filter removed a single row");
}

// ---------------------------------------------------------------------------
// A value list is the union of its values — INV-P2, INV-P3
// ---------------------------------------------------------------------------

/// `Q(c: [a]) ∪ Q(c: [b])` = `Q(c: [a, b])`, over lists the generator cut in
/// two — including the cut that leaves one side empty, which is the same law
/// with [INV-P3] on one arm.
///
/// [INV-P3]: ../../../spec/07-invariants.md#inv-p3
///
/// Covers CT-3 · INV-P2, INV-P3
#[test]
fn a_value_list_is_the_union_of_its_values() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let (mut seen, mut split_empty) = (0, 0);

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);
        let splittable = table.splittable_filters();
        if splittable.is_empty() {
            continue;
        }
        let filter = *rng.pick(&splittable);

        let (left_values, right_values) = filter.split(&mut rng);
        split_empty += usize::from(left_values.is_empty() || right_values.is_empty());

        let whole: Vec<_> = left_values.iter().chain(&right_values).cloned().collect();
        let left = rows_of(
            &generator,
            table,
            range,
            vec![ItemRequest::filtering(&filter.key, left_values)],
        );
        let right = rows_of(
            &generator,
            table,
            range,
            vec![ItemRequest::filtering(&filter.key, right_values)],
        );
        let both = rows_of(
            &generator,
            table,
            range,
            vec![ItemRequest::filtering(&filter.key, whole)],
        );

        let union: Rows = left.union(&right).cloned().collect();
        assert_eq!(
            union, both,
            "{}.{} does not split: the halves and the whole list disagree",
            table.name, filter.key
        );

        seen += both.len();
    }

    assert_saw_rows(seen, "splitting a value list");
    assert!(
        split_empty > 0,
        "no generated split left a side empty, so the empty-list arm of this law \
         was never taken"
    );
}

/// The values are a set: reordering the list, repeating a value, or adding one
/// the chunk does not hold leaves the response byte-identical.
///
/// Covers CT-3 · INV-P2, INV-P14
#[test]
fn value_order_repetition_and_misses_do_not_reach_the_answer() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let mut seen = 0;

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);
        let filter = rng.pick(&table.filters);

        let values = filter.values(&mut rng);
        let baseline = generator.query(
            range,
            &[(
                table,
                vec![ItemRequest::filtering(&filter.key, values.clone())],
            )],
        );
        let expected = answer(&generator, &baseline, range);

        let mut reordered = values.clone();
        reordered.reverse();
        reordered.extend(values.iter().cloned());
        if let Some(absent) = &filter.absent {
            reordered.push(absent.clone());
        }

        let rewritten = generator.query(
            range,
            &[(table, vec![ItemRequest::filtering(&filter.key, reordered)])],
        );
        assert_same_response(
            &expected,
            &answer(&generator, &rewritten, range),
            "reversing the value list, repeating it and adding a value the chunk lacks",
        );

        seen += row_set(&expected, &table.query_name).len();
    }

    assert_saw_rows(seen, "the order of filter values");
}

/// An empty list makes the whole item request unsatisfiable, whatever else it
/// carries — the generated sweep behind the hand-written case in `surface`.
///
/// Covers CT-3 · INV-P3, INV-P4
#[test]
fn an_empty_list_empties_the_item_request_it_sits_in() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let mut matched_without_it = 0;

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);
        let filter = rng.pick(&table.filters);

        let mut request = generator.item_request(table, &mut rng).without_relations();
        request.filters.remove(&filter.key);

        // Only worth asserting where the request matched something first: an
        // empty answer that was already empty says nothing about the empty list.
        matched_without_it +=
            usize::from(!rows_of(&generator, table, range, vec![request.clone()]).is_empty());

        request
            .filters
            .insert(filter.key.clone(), serde_json::Value::Array(vec![]));

        let rows = rows_of(&generator, table, range, vec![request.clone()]);
        assert!(
            rows.is_empty(),
            "{} under {:?} returned {} row(s); an empty list matches nothing",
            table.name,
            request.to_json(),
            rows.len()
        );
    }

    assert!(
        matched_without_it > 0,
        "every request this law emptied was already empty without the empty list"
    );
}

// ---------------------------------------------------------------------------
// Case folding follows the column — INV-P8
// ---------------------------------------------------------------------------

/// `Q(c: [upper(a)])` = `Q(c: [a])` exactly where the catalog marks `c`
/// case-insensitive, and differs everywhere else. Which columns fold comes from
/// the corpus, which read it off the catalog: a test that decides for itself is
/// a test asserting its own guess.
///
/// Covers CT-3 · INV-P8
#[test]
fn case_folding_follows_the_column() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let (mut folded, mut exact) = (0, 0);

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);
        let filter = rng.pick(&table.filters);

        let values = filter.values(&mut rng);
        let upper: Vec<_> = values
            .iter()
            .map(|v| match v {
                serde_json::Value::String(s) => serde_json::Value::from(s.to_uppercase()),
                other => other.clone(),
            })
            .collect();
        if upper == values {
            continue;
        }

        let lower_rows = rows_of(
            &generator,
            table,
            range,
            vec![ItemRequest::filtering(&filter.key, values)],
        );
        let upper_rows = rows_of(
            &generator,
            table,
            range,
            vec![ItemRequest::filtering(&filter.key, upper)],
        );

        if filter.folds_case {
            assert_eq!(
                lower_rows, upper_rows,
                "{}.{} is marked case-insensitive and did not fold",
                table.name, filter.key
            );
            folded += lower_rows.len();
        } else {
            assert!(
                upper_rows.is_empty() || upper_rows != lower_rows,
                "{}.{} is not marked case-insensitive and folded anyway",
                table.name,
                filter.key
            );
            exact += lower_rows.len();
        }
    }

    assert_saw_rows(folded, "case folding on a column that folds");
    assert_saw_rows(exact, "byte-exact comparison on a column that does not");
}

// ---------------------------------------------------------------------------
// The same laws over a chunk an archiver wrote
// ---------------------------------------------------------------------------

/// The synthetic chain above is one shape, chosen by the person who wrote it.
/// This runs the same generator over a real catalog and a chunk an archiver
/// produced — where columns are nullable, cardinalities are not two, and the
/// catalog names fields the chunk does not carry.
///
/// `optimism` rather than `ethereum` on purpose: a hundred blocks of real output
/// is six megabytes and a few seconds, where ethereum's chunk is four hundred
/// megabytes and turns a sweep into a coffee break.
///
/// The one thing that changes here is the weight budget, which a real chunk
/// actually reaches. A trimmed response is a *prefix* of the answer ([INV-B7]),
/// so the laws are asserted over the prefix every response in the pair covers,
/// and the prefix is required to be non-empty rather than assumed to be.
///
/// [INV-B7]: ../../../spec/07-invariants.md#inv-b7
///
/// Covers CT-3 · INV-P2, INV-P5, INV-P6
#[test]
#[ignore = "requires external fixture data"]
fn the_laws_hold_over_a_chunk_an_archiver_wrote() {
    if !fixture_tree_is_present() {
        return;
    }

    let generator = Generator::new(meta("evm"), &fixture_chunk("optimism"));
    let mut rng = Rng::new(SEED);
    let (mut seen, mut narrowed) = (0, 0);

    assert!(
        !generator.filterable().is_empty(),
        "the generator found nothing to filter on in a real chunk"
    );

    // A real chunk is slower per query than a synthetic one by three orders of
    // magnitude, and this sweep runs four queries a case.
    for _ in 0..CASES / 4 {
        let tables = generator.filterable();
        let table = *rng.pick(&tables);
        let range = generator.range(&mut rng);

        let first = generator.item_request(table, &mut rng).without_relations();
        let second = generator.item_request(table, &mut rng).without_relations();

        let bodies: Vec<Vec<u8>> = [
            vec![ItemRequest::default()],
            vec![first.clone()],
            vec![second.clone()],
            vec![first, second],
        ]
        .into_iter()
        .map(|items| generator.run(&generator.query(range, &[(table, items)])))
        .collect();

        let upto = common_prefix(&bodies);
        assert!(
            upto >= range.0,
            "every response over blocks {}..={} was trimmed away entirely",
            range.0,
            range.1
        );

        let rows: Vec<Rows> = bodies
            .iter()
            .map(|body| upto_block(&row_set(body, &table.query_name), upto))
            .collect();
        let (whole, left, right, both) = (&rows[0], &rows[1], &rows[2], &rows[3]);

        let union: Rows = left.union(right).cloned().collect();
        assert_eq!(
            &union, both,
            "{} does not disjoin its item requests over blocks {}..={upto}",
            table.name, range.0
        );
        assert!(
            both.is_subset(whole),
            "{} returns filtered rows the unfiltered request does not",
            table.name
        );

        seen += whole.len();
        narrowed += usize::from(both.len() < whole.len());
    }

    assert_saw_rows(seen, "the laws over a real chunk");
    assert!(
        narrowed > 0,
        "no generated filter removed a row from a real chunk"
    );
}

/// The last block every one of these responses reached. Under a weight budget
/// two queries over one range can stop at different blocks, and only the prefix
/// they share is an answer both of them finished.
fn common_prefix(bodies: &[Vec<u8>]) -> u64 {
    bodies
        .iter()
        .map(|body| {
            *block_numbers(&parse_response(body))
                .last()
                .expect("a response covers at least its first block")
        })
        .min()
        .expect("there is at least one response")
}

fn upto_block(rows: &Rows, last: u64) -> Rows {
    rows.iter()
        .filter(|(block, _)| *block <= last)
        .cloned()
        .collect()
}
