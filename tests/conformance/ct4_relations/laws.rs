//! §8.5's table, asserted over generated queries.
//!
//! A relation is the part of the engine where a bug is invisible from one query.
//! "Adding a flag never removes a row", "a row appears once however many paths
//! reach it", "a pulled row does not pull its own" — each names two queries and
//! the difference between them, and a suite of hand-written cases can only ever
//! witness the pair somebody thought of.
//!
//! The chain these run against is a cycle on purpose: `logs.transaction` reaches
//! transactions, and `transactions.log` and `transactions.trace` reach back. A
//! second hop is therefore *observable* — it comes back carrying logs the filter
//! excluded, and traces nothing asked for — rather than merely not terminating.

use std::collections::BTreeSet;
use tempfile::TempDir;

use crate::harness::evm_like;
use crate::harness::generator::{Generator, ItemRequest, Rng, TableCorpus};
use crate::harness::json::{
    assert_same_response, block_numbers, items_of, parse_response, row_set,
};

/// Recorded, so a failure replays. Changing it is changing the test.
const SEED: u64 = 0x5EED_0004;

const CASES: usize = 48;

type Rows = BTreeSet<(u64, String)>;

fn corpus() -> (TempDir, Generator) {
    let chunk = evm_like::chunk();
    let generator = Generator::new(evm_like::catalog(), chunk.path());

    (chunk, generator)
}

/// Run a generated query, having first checked it answered the whole range.
/// A trimmed response makes every inclusion below trivially true.
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

/// Every table's rows, keyed by the response key each sits under. The laws are
/// about *every* table, not about the one the relation names: [INV-R4] says
/// adding a flag may not remove a row from the table the relation starts at
/// either.
///
/// [INV-R4]: ../../../spec/07-invariants.md#inv-r4
fn all_rows(generator: &Generator, body: &[u8]) -> Vec<(String, Rows)> {
    generator
        .tables()
        .iter()
        .map(|t| (t.query_name.clone(), row_set(body, &t.query_name)))
        .collect()
}

/// A table that has a relation to flag, and a range to ask it over.
fn case<'a>(generator: &'a Generator, rng: &mut Rng) -> (&'a TableCorpus, (u64, u64)) {
    let tables: Vec<&TableCorpus> = generator
        .tables()
        .iter()
        .filter(|t| !t.relations.is_empty())
        .collect();

    assert!(
        !tables.is_empty(),
        "no table of this chunk declares a relation, so every law below would \
         pass having compared two identical queries"
    );

    (*rng.pick(&tables), generator.range(rng))
}

fn assert_saw_rows(seen: usize, law: &str) {
    assert!(
        seen > 0,
        "{law} was asserted over {CASES} generated queries and not one returned a row"
    );
}

// ---------------------------------------------------------------------------
// Relations widen — INV-R4
// ---------------------------------------------------------------------------

/// The most useful metamorphic property in the suite, and the reason it is:
/// nothing else notices a relation that *removes* rows. The pair differs in one
/// flag on one item request, and every table is compared — including the table
/// the flag hangs off, and the tables the flag does not name.
///
/// Every table is requested directly as well, so the flag's target is reachable
/// two ways at once. That is where a relation removes rows: not by pulling too
/// few, but by a merge of the two sources that keeps one of them.
///
/// Covers CT-4 · INV-R4
#[test]
fn adding_a_relation_flag_never_removes_a_row() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let (mut seen, mut widened) = (0, 0);

    for _ in 0..CASES {
        let range = generator.range(&mut rng);
        let mut requests = every_table(&generator, &mut rng);

        let Some((table, item, relation)) = a_flag_that_is_set(&requests, &mut rng) else {
            continue;
        };

        let wide = answer(&generator, &generator.query(range, &requests), range);
        requests[table].1[item] = requests[table].1[item].clone().without(&relation);
        let narrow = answer(&generator, &generator.query(range, &requests), range);

        for ((key, before), (_, after)) in all_rows(&generator, &narrow)
            .into_iter()
            .zip(all_rows(&generator, &wide))
        {
            assert!(
                before.is_subset(&after),
                "adding '{relation}' to a {} request dropped {} row(s) from '{key}'",
                requests[table].0.name,
                before.difference(&after).count()
            );

            seen += before.len();
            widened += usize::from(after.len() > before.len());
        }
    }

    assert_saw_rows(seen, "the widening of a relation flag");
    assert!(
        widened > 0,
        "no generated relation flag added a single row, so a relation that never \
         fires would pass this test"
    );
}

/// One or two item requests for every table, so each table is reachable
/// directly as well as through whatever relations point at it.
fn every_table<'a>(
    generator: &'a Generator,
    rng: &mut Rng,
) -> Vec<(&'a TableCorpus, Vec<ItemRequest>)> {
    generator
        .tables()
        .iter()
        .map(|table| {
            let items = (0..1 + rng.below(2))
                .map(|_| generator.item_request(table, rng))
                .collect();

            (table, items)
        })
        .collect()
}

/// A relation flag one of the generated item requests actually carries, as the
/// indices needed to take it back out again.
fn a_flag_that_is_set(
    requests: &[(&TableCorpus, Vec<ItemRequest>)],
    rng: &mut Rng,
) -> Option<(usize, usize, String)> {
    let set: Vec<(usize, usize, String)> = requests
        .iter()
        .enumerate()
        .flat_map(|(t, (_, items))| {
            items
                .iter()
                .enumerate()
                .flat_map(move |(i, item)| item.relations.iter().map(move |r| (t, i, r.clone())))
        })
        .collect();

    (!set.is_empty()).then(|| rng.pick(&set).clone())
}

// ---------------------------------------------------------------------------
// Relations resolve one hop — INV-R2
// ---------------------------------------------------------------------------

/// The chain's relations form a cycle, so a second hop leaves marks: from a
/// filtered log request it comes back through `transactions.log` carrying the
/// logs the filter excluded, and through `transactions.trace` carrying traces
/// nothing asked for. Both are asserted — the table the relation started at is
/// unchanged, and the table two hops out is empty.
///
/// Covers CT-4 · INV-R2
#[test]
fn a_pulled_row_does_not_pull_its_own() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let (mut seen, mut pulled) = (0, 0);

    let logs = generator.table("logs");

    for _ in 0..CASES {
        let range = generator.range(&mut rng);

        let direct = generator.item_request(logs, &mut rng).without_relations();
        let hop = direct.clone().with("transaction");

        let before = answer(
            &generator,
            &generator.query(range, &[(logs, vec![direct])]),
            range,
        );
        let after = answer(
            &generator,
            &generator.query(range, &[(logs, vec![hop])]),
            range,
        );

        assert_eq!(
            row_set(&before, "logs"),
            row_set(&after, "logs"),
            "asking a log request for its transactions changed which logs came \
             back, so `transactions.log` was followed a second hop"
        );

        let traces = row_set(&after, "traces");
        assert!(
            traces.is_empty(),
            "a log request that asked for transactions returned {} trace(s), \
             though nothing asked for a trace and only `transactions.trace` \
             reaches that table",
            traces.len()
        );

        seen += row_set(&after, "logs").len();
        pulled += row_set(&after, "transactions").len();
    }

    assert_saw_rows(seen, "one hop from a log request");
    assert_saw_rows(pulled, "the transactions one hop pulled");
}

// ---------------------------------------------------------------------------
// A relation's sources are its own item request's matches — INV-R1
// ---------------------------------------------------------------------------

/// §8.5's construction, generated: two item requests over disjoint halves of a
/// filter's values, only the first carrying the flag. An engine that scopes
/// relations to the table returns the second's targets too, and every query with
/// one item request passes regardless.
///
/// Covers CT-4 · INV-R1
#[test]
fn a_relation_pulls_only_its_own_item_requests_matches() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let (mut seen, mut distinguishing) = (0, 0);

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);
        let splittable = table.splittable_filters();
        if splittable.is_empty() {
            continue;
        }
        let filter = *rng.pick(&splittable);
        let relation = rng.pick(&table.relations);
        let target = generator.table(&relation.target);

        let (mine, theirs) = filter.split(&mut rng);
        if mine.is_empty() || theirs.is_empty() {
            continue;
        }

        let flagged = ItemRequest::filtering(&filter.key, mine).with(&relation.key);
        let bare = ItemRequest::filtering(&filter.key, theirs);

        let alone = answer(
            &generator,
            &generator.query(range, &[(table, vec![flagged.clone()])]),
            range,
        );
        let beside = answer(
            &generator,
            &generator.query(range, &[(table, vec![flagged, bare.clone()])]),
            range,
        );

        // The unflagged request adds its own rows to its own table; what it must
        // not do is add targets to the relation's.
        let mine_targets = row_set(&alone, &target.query_name);
        assert_eq!(
            mine_targets,
            row_set(&beside, &target.query_name),
            "an item request with no '{}' flag changed which {} came back",
            relation.key,
            target.query_name
        );

        // Whether this case could have caught the wrong behaviour at all. An
        // engine scoping the relation to its table returns the targets of *both*
        // halves, so the two answers differ only where the second half reaches a
        // target the first does not. A filter whose every value shares the same
        // targets — an address column over a chunk where each transaction holds
        // one log of each address — distinguishes nothing, however many cases
        // run over it.
        let theirs_alone = answer(
            &generator,
            &generator.query(range, &[(table, vec![bare.with(&relation.key)])]),
            range,
        );

        seen += mine_targets.len();
        distinguishing +=
            usize::from(!row_set(&theirs_alone, &target.query_name).is_subset(&mine_targets));
    }

    assert_saw_rows(seen, "a relation scoped to one item request");
    assert!(
        distinguishing > 0,
        "no generated split had a second half reaching a target the first does \
         not, so an engine pulling both halves' targets would have answered \
         identically every time"
    );
}

// ---------------------------------------------------------------------------
// Relations are idempotent across item requests — INV-R11
// ---------------------------------------------------------------------------

/// What licenses the "all item requests want it, so evaluate it once against the
/// whole table" optimisation: the same relation asked from two item requests
/// yields what asking it once against the union of their matches yields.
///
/// The two halves come from splitting one filter's values, so their union *is*
/// an item request — which is what makes the right-hand side of the law
/// writable at all.
///
/// Covers CT-4 · INV-R11
#[test]
fn a_relation_asked_from_two_item_requests_is_asked_once_of_their_union() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let mut seen = 0;

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);
        let splittable = table.splittable_filters();
        if splittable.is_empty() {
            continue;
        }
        let filter = *rng.pick(&splittable);
        let relation = rng.pick(&table.relations).key.clone();

        let (left, right) = filter.split(&mut rng);
        let whole: Vec<_> = left.iter().chain(&right).cloned().collect();

        let separately = generator.query(
            range,
            &[(
                table,
                vec![
                    ItemRequest::filtering(&filter.key, left).with(&relation),
                    ItemRequest::filtering(&filter.key, right).with(&relation),
                ],
            )],
        );
        let together = generator.query(
            range,
            &[(
                table,
                vec![ItemRequest::filtering(&filter.key, whole).with(&relation)],
            )],
        );

        let apart = answer(&generator, &separately, range);
        let once = answer(&generator, &together, range);

        assert_same_response(
            &once,
            &apart,
            &format!("asking '{relation}' from two item requests instead of one"),
        );

        seen += row_set(&once, &table.query_name).len();
    }

    assert_saw_rows(seen, "a relation asked twice over a split filter");
}

// ---------------------------------------------------------------------------
// Every row appears once — INV-R3
// ---------------------------------------------------------------------------

/// `Items(T) = Direct(T) ∪ Related(T)` is a set. Asserted over every table of
/// every generated query, since the paths that reach one row twice are exactly
/// the ones nobody enumerates: a row matched by two item requests, a row pulled
/// by two relations, a row both matched and pulled.
///
/// Covers CT-4 · INV-R3
#[test]
fn a_row_reachable_several_ways_is_returned_once() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let mut seen = 0;

    for _ in 0..CASES {
        let range = generator.range(&mut rng);

        // Several item requests per table, and every table asked at once, so the
        // overlapping paths are actually built.
        let requests: Vec<(&TableCorpus, Vec<ItemRequest>)> = generator
            .tables()
            .iter()
            .map(|table| {
                let items = (0..1 + rng.below(2))
                    .map(|_| generator.item_request(table, &mut rng))
                    .collect();

                (table, items)
            })
            .collect();

        let body = answer(&generator, &generator.query(range, &requests), range);

        for table in generator.tables() {
            let emitted = items_of(&body, &table.query_name).len();
            let distinct = row_set(&body, &table.query_name).len();

            assert_eq!(
                emitted, distinct,
                "'{}' returned {} row(s) of which {distinct} are distinct",
                table.query_name, emitted
            );

            seen += distinct;
        }
    }

    assert_saw_rows(seen, "deduplication across paths");
}

// ---------------------------------------------------------------------------
// A matched pair shares a block — INV-D5
// ---------------------------------------------------------------------------

/// The chunk-level half of [INV-D5], which the static checks in CT-1 cannot
/// reach: a join key led by the block number means no matched pair spans two
/// blocks, so a row that only a relation could have pulled sits in a block where
/// a source row sits.
///
/// [INV-D5]: ../../../spec/07-invariants.md#inv-d5
///
/// Covers CT-4 · INV-D5
#[test]
fn a_relation_pulls_nothing_out_of_its_sources_blocks() {
    let (_chunk, generator) = corpus();
    let mut rng = Rng::new(SEED);
    let mut seen = 0;

    for _ in 0..CASES {
        let (table, range) = case(&generator, &mut rng);
        let relation = rng.pick(&table.relations);
        let target = generator.table(&relation.target);

        // Only the source table is asked for, so every row of the target got
        // there through the relation.
        let request = generator
            .item_request(table, &mut rng)
            .without_relations()
            .with(&relation.key);
        let body = answer(
            &generator,
            &generator.query(range, &[(table, vec![request])]),
            range,
        );

        let sources: BTreeSet<u64> = row_set(&body, &table.query_name)
            .iter()
            .map(|(block, _)| *block)
            .collect();
        let pulled: BTreeSet<u64> = row_set(&body, &target.query_name)
            .iter()
            .map(|(block, _)| *block)
            .collect();

        assert!(
            pulled.is_subset(&sources),
            "'{}' pulled {} into block(s) {:?}, where {} has no matched row",
            relation.key,
            target.query_name,
            pulled.difference(&sources).collect::<Vec<_>>(),
            table.name
        );

        seen += pulled.len();
    }

    assert_saw_rows(seen, "a relation's blocks");
}
