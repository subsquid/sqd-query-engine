# 8. Conformance

How to turn [07-invariants.md](07-invariants.md) into a test suite that finds
gaps rather than confirming what you already believe.

The invariants are the contract; this is how it gets checked.

**This document is mutable.** Along with [09-parameters.md](09-parameters.md) it
is one of the two files that may change without intended behaviour changing:
§8.11 tracks what is actually covered today, and the gates in §8.12 tighten as
coverage grows. Sections 8.1–8.10 are advisory — one way to build the suite.
Sections 8.11–8.14 are the current state and the bar for changes.

## 8.1 The problem with fixture tests

A fixture suite compares an engine's output against recorded output for a
recorded query. It is necessary and it is not sufficient, for three reasons.

**Fixtures only test what someone thought to record.** A fixture set derived from
production traffic tests the queries clients happen to send. The gaps are, by
construction, in the queries nobody sent.

**Fixtures pass when the fixture is missing.** A suite that skips a test whose
chunk is absent reports green for a test that never ran. This is the most common
way a conformance suite lies. Every skip MUST be counted and the count MUST be
asserted against an expected total.

**Fixtures are agnostic about why.** When output matches, you learn nothing about
which invariant held. When it differs, you learn nothing about which broke.

So: build fixtures *and* the nine test classes below. The classes find the gaps;
the fixtures stop regressions.

Each class has an ID. The traceability matrix (§8.11) maps every invariant to the
class that checks it and to an honest status; the merge gates (§8.12) say which
classes a change must pass; the capability register (§8.13) says which machinery
each class needs and whether it exists.

| Class | What it exercises | Needs a chunk? | Fits a PR budget? |
|---|---|---|---|
| **CT-1** — [catalog validation](#82-ct-1--catalog-validation) | Catalog well-formedness, statically | no | yes |
| **CT-2** — [request validation](#83-ct-2--request-validation) | One case per error kind | no | yes |
| **CT-3** — [filter algebra](#84-ct-3--filter-algebra) | The laws of §4.2, generated | yes | yes |
| **CT-4** — [relations](#85-ct-4--relations) | Scoping, hops, hierarchies | yes | yes |
| **CT-5** — [blocks and weight](#86-ct-5--blocks-weight-pagination) | Partition invariance, budget, paging | yes | yes |
| **CT-6** — [output](#87-ct-6--output-and-determinism) | Encodings, ordering, byte determinism | yes | yes |
| **CT-7** — [differential](#88-ct-7--differential-testing) | Generated queries against the reference | yes | nightly |
| **CT-8** — [adversarial chunks](#89-ct-8--adversarial-chunks) | Dropped columns, retyped, reordered | yes, written | nightly |
| **CT-9** — [fuzz](#810-ct-9--fuzz) | Both surfaces, panic-only assertion | both | nightly |

## 8.2 CT-1 — Catalog validation

Static, no chunk, runs in milliseconds. Checks [INV-D1](07-invariants.md#inv-d1)
– [INV-D3](07-invariants.md#inv-d3), [INV-D5](07-invariants.md#inv-d5),
[INV-D6](07-invariants.md#inv-d6), [INV-D10](07-invariants.md#inv-d10).

For each bundled dataset assert every catalog reference resolves; every relation
key begins with the block number column on both sides; every hierarchical
relation has address columns; `queryName` and `fieldName` are unique across
tables and aliases.

Then invert it: for each check, construct a catalog that violates it and assert
validation rejects it. A validator nobody has seen reject anything is a validator
that returns `Ok`.

## 8.3 CT-2 — Request validation

Table-driven. One case per row of §6.2, asserting the error *kind*, not the
message. Covers [INV-Q1](07-invariants.md#inv-q1) –
[INV-Q13](07-invariants.md#inv-q13), [INV-E2](07-invariants.md#inv-e2),
[INV-E6](07-invariants.md#inv-e6).

The cases that matter most are the ones an engine is likeliest to get wrong by
being permissive:

```jsonc
{"type":"evm","fromBlock":"abc"}                       → InvalidBlockNumber
{"type":"evm","fromBlock":-1}                          → InvalidBlockNumber
{"type":"evm","fromBlock":1.5}                         → InvalidBlockNumber
{"type":"evm","toBlock":5,"fromBlock":10}              → InvalidBlockRange
{"type":"evm","fields":{"log":{"logIndx":true}}}       → UnknownField
{"type":"evm","fields":{"lgo":{}}}                     → UnknownFieldGroup
{"type":"evm","logs":[{"dataSize":[1]}]}               → UnknownFilter
{"type":"evm","logs":{}}                               → MalformedRequest
{"type":"evm","logz":[]}                               → UnknownTable
{"type":"solana","instructions":[{"d1":["0x01"],"d8":["0x0102030405060708"]}]}
                                                       → ConflictingFilters
{"type":"solana","instructions":[{"discriminator":["0xabc"]}]}
                                                       → InvalidHex
```

Then fuzz — [CT-9](#810-ct-9--fuzz).

## 8.4 CT-3 — Filter algebra

This is where the interesting bugs are, because these tests can be generated.

Pick a chunk. Enumerate a few dozen filter values that actually occur in it, plus
a few that do not. Then assert the laws, over randomly composed queries:

| Law | Invariant |
|---|---|
| `Q(c: [])` = ∅ | [INV-P3](07-invariants.md#inv-p3) |
| `Q(c: [])` = `Q(c: [], d: anything)` = ∅ | [INV-P3](07-invariants.md#inv-p3) + [INV-P4](07-invariants.md#inv-p4) |
| `Q(c: [a]) ∪ Q(c: [b])` = `Q(c: [a, b])` | [INV-P2](07-invariants.md#inv-p2) |
| `Q(c: [a,b])` = `Q(c: [b,a])` | value order irrelevant |
| `Q(c: [a]) ⊆ Q({})` | [INV-P6](07-invariants.md#inv-p6) |
| `Q(c: [a], d: [b]) ⊆ Q(c: [a])` | [INV-P4](07-invariants.md#inv-p4) |
| `Q([s₁]) ∪ Q([s₂])` = `Q([s₁, s₂])` | [INV-P5](07-invariants.md#inv-p5) |
| `Q([s, s])` = `Q([s])` | idempotence |
| `Q([s₁, s₂])` = `Q([s₂, s₁])` | request order irrelevant |
| `Q(c: [upper(a)])` = `Q(c: [a])` iff `c` is `hexBytes` | [INV-P8](07-invariants.md#inv-p8) |
| `Q(c: [huge])` = ∅, no error | [INV-P14](07-invariants.md#inv-p14) |
| `Q` with pruning disabled = `Q` | [INV-P16](07-invariants.md#inv-p16) |

Two of these are worth writing by hand even if you generate the rest, because
they encode the two catastrophic misreadings:

- **`Q(c: []) = ∅`.** An engine that returns the whole chunk here will pass every
  fixture test ever written, because no fixture sends an empty list.
- **`Q(c: [a]) ⊆ Q({})`.** An engine whose filter silently no-ops — because the
  column is missing from the chunk ([INV-X3](07-invariants.md#inv-x3)) — makes
  this an *equality*. Assert strict subset for a value you know is selective.

## 8.5 CT-4 — Relations

| Property | Invariant |
|---|---|
| Adding a relation flag never shrinks any table's row set | [INV-R4](07-invariants.md#inv-r4) |
| Relation rows all lie in the same block as some source row | [INV-D5](07-invariants.md#inv-d5) |
| Every returned row appears exactly once | [INV-R3](07-invariants.md#inv-r3) |
| Requesting a relation twice changes nothing | [INV-R11](07-invariants.md#inv-r11) |
| A relation on a filtered item request pulls only that filter's matches | [INV-R1](07-invariants.md#inv-r1) |
| Pulled rows do not expand their own relations | [INV-R2](07-invariants.md#inv-r2) |

[INV-R1](07-invariants.md#inv-r1) needs a deliberate construction, because a
single-item-request query cannot distinguish it from the wrong behaviour:

```jsonc
"logs": [
  { "address": ["A"], "transaction": true },
  { "address": ["B"] }
]
```

Assert the returned transactions are exactly those of A-logs. An engine that
scopes relations to the whole table returns B's too, and every fixture with one
item request passes regardless.

For hierarchies, build a synthetic chunk with a known tree — a trace at
`[0]` with children `[0,0]`, `[0,1]`, `[0,0,0]` — and assert `children([0])` is
all three, `parents([0,0,0])` is `[0,0]` and `[0]`, and neither includes the
source. Then the cross-table case ([INV-R8](07-invariants.md#inv-r8)): an event
at exactly the call's address must be returned.

## 8.6 CT-5 — Blocks, weight, pagination

The single most valuable test in the suite:

> **Partition invariance.** For each fixture query, split its block range at every
> internal boundary. Assert that concatenating the two responses' *items* equals
> the whole-range response's items. Ignore header-only blocks.

That is [INV-B8](07-invariants.md#inv-b8), and it transitively exercises
[INV-D5](07-invariants.md#inv-d5), [INV-B1](07-invariants.md#inv-b1),
[INV-B4](07-invariants.md#inv-b4) and [INV-R10](07-invariants.md#inv-r10). If
partition invariance holds for a query, that query is safe to serve from any
chunking of the archive.

Then:

- Force the budget small (an injectable `P-WEIGHT-BUDGET`) and page a chunk end
  to end. Assert no gap, no duplicate, and equality with the single-shot result
  ([INV-B7](07-invariants.md#inv-b7)).
- A block heavier than the whole budget is still returned, whole and alone
  ([INV-B4](07-invariants.md#inv-b4), [INV-B6](07-invariants.md#inv-b6)).
- A narrower projection returns at least as many blocks
  ([INV-B10](07-invariants.md#inv-b10)).
- A query matching nothing returns exactly the boundary headers
  ([INV-B3](07-invariants.md#inv-b3)).
- `includeAllBlocks: true` returns every block in range
  ([INV-B2](07-invariants.md#inv-b2)).

## 8.7 CT-6 — Output and determinism

- Run every fixture query at 1, 2 and 16 threads; assert byte equality
  ([INV-O13](07-invariants.md#inv-o13)).
- Rewrite a fixture chunk with a different storage sort key, row-group size and
  compression; assert byte equality ([INV-D8](07-invariants.md#inv-d8)).
- Rewrite it with every integer column at its widest and narrowest legal physical
  type, signed and unsigned; assert byte equality
  ([INV-D7](07-invariants.md#inv-d7)).
- Add a nullable column; assert byte equality for queries that ignore it
  ([INV-X2](07-invariants.md#inv-x2)).
- Encoding table tests: one row per encoding in §1.5, including the edge cases —
  a `hexNumber` that needs padding, a `decimalString` above 2⁵³, a NaN, an empty
  `jsonVerbatim`, a roll with a null in the middle, a field-group tag the catalog
  does not know ([INV-O9](07-invariants.md#inv-o9) –
  [INV-O11](07-invariants.md#inv-o11)).
- Framing: empty result is zero bytes; every block ends in `\n`; two responses
  concatenate ([INV-O1](07-invariants.md#inv-o1)).

## 8.8 CT-7 — Differential testing

Where a reference implementation exists, run both over the same chunk and the
same query and compare.

Compare **values, not bytes**, unless the two agree on field order and string
escaping. A value-level comparison catches real divergence; a byte-level one
drowns it in `A` versus `A`.

Generate the queries rather than curating them. A generator that walks the
catalog — pick a table, pick a filter, pick values from the chunk's actual
column contents, pick a random subset of relations, pick a random projection —
explores the surface that fixtures cannot.

Three rules keep this honest:

1. **Count the skips.** A case where either engine errors or panics is *skipped*,
   and the skip count is reported and asserted. A suite that quietly skips is a
   suite that quietly passes.
2. **Assert a floor.** `assert!(compared > N)` per dataset. Otherwise a broken
   fixture path turns the whole suite into a no-op that reports green.
3. **Divergence is a finding, not a failure to explain away.** When the engines
   differ, decide which is right and write it into
   [07-invariants.md](07-invariants.md) before fixing either.

## 8.9 CT-8 — Adversarial chunks

Every one of these should produce a typed error or a correct answer, and never a
panic ([INV-E1](07-invariants.md#inv-e1)):

| Chunk mutation | Expected |
|---|---|
| Drop a column that a query filters on | `ColumnNotFound` ([INV-X3](07-invariants.md#inv-x3)) |
| Drop a column that a query selects | `ColumnNotFound` ([INV-E3](07-invariants.md#inv-e3)) |
| Drop a column nothing mentions | Unchanged output ([INV-X2](07-invariants.md#inv-x2)) |
| Drop a whole table a query needs | `TableNotFound` ([INV-E4](07-invariants.md#inv-e4)) |
| Widen `block_number` from 32 to 64 bits | Unchanged output ([INV-D7](07-invariants.md#inv-d7)) |
| Store an item index as signed | Unchanged output ([INV-D7](07-invariants.md#inv-d7)) |
| Store an address list's elements at each legal width | Unchanged output |
| Make a join key column all-null | Those rows join to nothing ([INV-R5](07-invariants.md#inv-r5)) |
| Put non-JSON in a `jsonVerbatim` column | Error, or a valid response ([INV-E1](07-invariants.md#inv-e1)) — never corrupt framing |
| Put an unknown value in a field-group tag column | Base fields only ([INV-O11](07-invariants.md#inv-o11)) |
| Reverse the physical row order | Unchanged output ([INV-D8](07-invariants.md#inv-d8)) |
| Split into one row group per row | Unchanged output ([INV-D8](07-invariants.md#inv-d8)) |
| Strip all column statistics | Unchanged output ([INV-P16](07-invariants.md#inv-p16)) |

Most of these require a chunk *writer* in the test harness. Building one is the
highest-leverage investment a conformance suite can make: without it, half the
invariants in [07-invariants.md](07-invariants.md) cannot be tested at all, and
those are precisely the half that governs how the engine behaves on data written
by a version of the archiver that no longer exists.

## 8.10 CT-9 — Fuzz

Two surfaces, one assertion.

**Requests.** Random JSON, random mutations of valid queries, and every
type-substitution at every position, against every dataset. Assert only
[INV-E1](07-invariants.md#inv-e1): no panic, ever. This finds more than the
§8.3 table does, because the table only contains the mistakes someone thought of.

**Chunks.** Random physical types, random nulls, random list nesting, truncated
files. Same assertion.

Seed deterministically and print the seed on failure. A fuzz finding nobody can
reproduce is a rumour.

## 8.11 Traceability matrix

Dated **2026-09-02**. Every invariant, the class that checks it, and an honest
status:

- **C** — covered. A test would fail if the invariant broke.
- **P** — partial. Something exercises it, but a plausible break would still pass:
  the test covers one mechanism of several, or one direction of an equality, or
  reaches the invariant only through a fixture that would also change for a dozen
  other reasons.
- **U** — unchecked. Nothing would notice.

A row marked *known-violated* names the gap: the invariant is false today and
[GAPS.md](GAPS.md) says why. *known-suspect* means nobody has looked.

Statuses come from the test inventory, not from intent. A test that asserts a
value is *accepted* does not cover an invariant that says which values are
*rejected* — that is the mistake that let GAP 27 stand.

| Invariant | Class | Status | Note |
|---|---|---|---|
| [INV-D1](07-invariants.md#inv-d1) | CT-1 | **C** | every reference the invariant lists has a negative case: `test_validate_rejects_unresolvable_references` covers the address, parent-key, sort-key, item-order, weight, roll, discriminator-length and field-group ones, and with them the two shape rules §1.10 carries alongside — a roll's spread list is last, and a discriminator length is written the way the lookup asks for it; `test_validate_rejects_broken_alias_references`, `test_validate_rejects_unknown_filter_column`, `test_validate_rejects_a_special_filter_on_a_missing_column` and `test_validation_bad_block_number_column` the rest |
| [INV-D2](07-invariants.md#inv-d2) | CT-1 | **C** | `test_validate_rejects_broken_alias_references` |
| [INV-D3](07-invariants.md#inv-d3) | CT-1 | **C** | `test_validate_requires_exactly_one_block_table` — none, two, and one that does not lead the catalog; identity is the item key, so a block table stored under another sort key is still one and an addressed table with no order keys is not |
| [INV-D4](07-invariants.md#inv-d4) | CT-1 | **U** | no chunk-level key-uniqueness check on any fixture |
| [INV-D5](07-invariants.md#inv-d5) | CT-1 | **P** | the static half is checked on both sides — key non-empty, equal length, block number first — one case per shape in `test_validate_rejects_a_relation_that_cannot_join`. The chunk-level half its *Test:* line asks for, that no matched pair spans two blocks, is not run |
| [INV-D6](07-invariants.md#inv-d6) | CT-1 | **C** | both sides are checked for `children` and `parents` alike, with a case in `test_validate_rejects_a_relation_that_cannot_join` |
| [INV-D7](07-invariants.md#inv-d7) | CT-6 | **P** | width tolerance is covered per mechanism — block-range masks, in-lists, semi-joins, range predicates, and on the output side `test_solana_tx_version_reads_the_sentinel_at_every_physical_width` and `test_bignum_reads_a_narrowed_column`. No same-chunk-at-several-widths equality run |
| [INV-D8](07-invariants.md#inv-d8) | CT-6 | **U** | needs the chunk writer, HC-3 |
| [INV-D9](07-invariants.md#inv-d9) | CT-1 | **C** | `test_system_columns_excluded_from_weight`, `undeclared_columns_are_not_filterable` |
| [INV-D10](07-invariants.md#inv-d10) | CT-1 | **C** | `test_validate_rejects_duplicate_names` — two tables on one `queryName`, two on one `fieldName`, an alias on a table's `queryName`, and a table on the name another holds without declaring it |
| [INV-Q1](07-invariants.md#inv-q1) | CT-2 | **P** | reached through every fixture; no negative case for an unknown `type` |
| [INV-Q2](07-invariants.md#inv-q2) | CT-2 | **C** | `test_parse_unknown_table_error` |
| [INV-Q3](07-invariants.md#inv-q3) | CT-2 | **C** | `test_parse_block_range_validation` |
| [INV-Q4](07-invariants.md#inv-q4) | CT-2 | **C** | `test_malformed_block_bounds_error`, `test_block_bounds_defaults_and_null` |
| [INV-Q5](07-invariants.md#inv-q5) | CT-2 | **C** | `test_parse_item_count_limit` |
| [INV-Q6](07-invariants.md#inv-q6) | CT-2 | **C** | `test_parse_unknown_filter_error`, `undeclared_columns_are_not_filterable`, `an_alias_has_its_own_filter_surface`, `reference_filters_are_all_accepted` |
| [INV-Q7](07-invariants.md#inv-q7) | CT-2 | **P** | `unknown_field_names_are_rejected` covers misspellings; the reference-surface test only asserts acceptance — see INV-Q14 |
| [INV-Q8](07-invariants.md#inv-q8) | CT-2 | **P** | an unknown `fields` key errors; `fields.X` not being an object is untested |
| [INV-Q9](07-invariants.md#inv-q9) | CT-2 | **P** | block-bound defaults only |
| [INV-Q10](07-invariants.md#inv-q10) | CT-2 | **C** | `a_bloom_filter_takes_at_most_ten_values`, either side of the cap |
| [INV-Q11](07-invariants.md#inv-q11) | CT-2 | **C** | `one_discriminator_filter_per_item_request`; the family is read from the catalog, so the check is not Solana-specific |
| [INV-Q12](07-invariants.md#inv-q12) | CT-2 | **C** | `malformed_hex_in_list_is_an_error`, `test_parse_hex`; the byte cap is enforced where discriminators compile |
| [INV-Q13](07-invariants.md#inv-q13) | CT-2 | **U** | known-violated — GAP 23 |
| [INV-Q14](07-invariants.md#inv-q14) | CT-2 | **U** | known-violated — GAP 27 |
| [INV-P1](07-invariants.md#inv-p1) | CT-3 | **P** | `test_compile_empty_item_no_filters`; not asserted as a law over generated queries |
| [INV-P2](07-invariants.md#inv-p2) | CT-3 | **C** | `test_in_list_predicate_strings`, `test_in_list_predicate_u64`, `test_numeric_in_list_filter` |
| [INV-P3](07-invariants.md#inv-p3) | CT-3 | **C** | `an_empty_filter_list_matches_nothing` — the discriminator, a discriminator column, an ordinary in-list, and an empty list beside a filter that does match |
| [INV-P4](07-invariants.md#inv-p4) | CT-3 | **C** | `test_row_predicate_and` |
| [INV-P5](07-invariants.md#inv-p5) | CT-3 | **U** | nothing asserts `Q([s₁]) ∪ Q([s₂]) = Q([s₁, s₂])` |
| [INV-P6](07-invariants.md#inv-p6) | CT-3 | **P** | compile-side only |
| [INV-P7](07-invariants.md#inv-p7) | CT-3 | **P** | the disjunction cases cover null propagation; no per-filter-kind null sweep |
| [INV-P8](07-invariants.md#inv-p8) | CT-3 | **C** | `hex_filters_fold_case_in_both_shapes`, `non_hex_columns_are_not_folded` |
| [INV-P9](07-invariants.md#inv-p9) | CT-3 | **U** | **nothing checks the engine's bloom bits against the writer's.** A construction mismatch produces false *negatives*, which no client can detect |
| [INV-P10](07-invariants.md#inv-p10) | CT-3 | **C** | four range cases across physical widths |
| [INV-P11](07-invariants.md#inv-p11) | CT-3 | **U** | `gteConst` lexicographic comparison has no test |
| [INV-P12](07-invariants.md#inv-p12) | CT-3 | **C** | three `list_contains_any` cases including the unknown-type one |
| [INV-P13](07-invariants.md#inv-p13) | CT-3 | **C** | `test_compile_discriminator_mixed_lengths`, `discriminator_hex_is_a_prefix_chain`, and the Kleene cases in the predicate unit tests |
| [INV-P14](07-invariants.md#inv-p14) | CT-3 | **P** | `unmatchable_values_are_not_errors` plus the overflow cases; known-violated for negative values on signed columns — GAP 14 |
| [INV-P15](07-invariants.md#inv-p15) | CT-3 | **C** | `undeclared_columns_are_not_filterable`, `an_alias_has_its_own_filter_surface` |
| [INV-P16](07-invariants.md#inv-p16) | CT-3 | **P** | row-group pruning and the `can_skip` cases; no pruning-disabled equality run |
| [INV-R1](07-invariants.md#inv-r1) | CT-4 | **P** | `test_alias_relation_source_predicates`, `test_resolve_includes_source_predicate_columns`; the two-item-request construction of §8.5 is not written |
| [INV-R2](07-invariants.md#inv-r2) | CT-4 | **U** | nothing asserts relations stop at one hop |
| [INV-R3](07-invariants.md#inv-r3) | CT-4 | **P** | `test_arrow_multisource_dedup` |
| [INV-R4](07-invariants.md#inv-r4) | CT-4 | **U** | the widening sweep does not exist — the most useful metamorphic property in the suite |
| [INV-R5](07-invariants.md#inv-r5) | CT-4 | **C** | `test_semi_join_null_key_no_false_match`, `test_semi_join_null_null_no_match`, and six more |
| [INV-R6](07-invariants.md#inv-r6) | CT-4 | **C** | `test_find_children_basic` and three siblings |
| [INV-R7](07-invariants.md#inv-r7) | CT-4 | **P** | `test_find_parents_basic` only; the full ancestor chain is not asserted |
| [INV-R8](07-invariants.md#inv-r8) | CT-4 | **P** | reached through the Kusama and Moonbeam fixtures; no synthetic tree |
| [INV-R9](07-invariants.md#inv-r9) | CT-4 | **P** | fixtures only |
| [INV-R10](07-invariants.md#inv-r10) | CT-4 | **P** | `test_execute_with_relations` plus the budget suite |
| [INV-R11](07-invariants.md#inv-r11) | CT-4 | **U** | no idempotence test |
| [INV-B1](07-invariants.md#inv-b1) | CT-5 | **C** | `test_scan_with_block_range`, `test_scan_with_predicate_and_block_range` |
| [INV-B2](07-invariants.md#inv-b2) | CT-5 | **C** | `untrimmed_scan_includes_all_blocks` |
| [INV-B3](07-invariants.md#inv-b3) | CT-5 | **C** | `boundary_blocks_emitted_without_items`, `budget_trim_excludes_range_end_boundary_block` |
| [INV-B4](07-invariants.md#inv-b4) | CT-5 | **P** | implied by the budget suite; no block-larger-than-budget case |
| [INV-B5](07-invariants.md#inv-b5) | CT-5 | **P** | the weight unit tests cover the components, not the block sum |
| [INV-B6](07-invariants.md#inv-b6) | CT-5 | **P** | `multi_table_trim_reports_true_last_block`; the keep-at-least-one rule is untested |
| [INV-B7](07-invariants.md#inv-b7) | CT-5 | **P** | same test; no end-to-end paging run |
| [INV-B8](07-invariants.md#inv-b8) | CT-5 | **U** | **partition invariance is not tested.** §8.6 calls it the single most valuable test in the suite, and it transitively exercises five other invariants |
| [INV-B9](07-invariants.md#inv-b9) | CT-5 | **P** | known-suspect — unchecked weight arithmetic, GAP 22 |
| [INV-B10](07-invariants.md#inv-b10) | CT-5 | **C** | four weight-projection cases |
| [INV-O1](07-invariants.md#inv-o1) | CT-6 | **C** | `empty_result_is_none`, `iteration_matches_json_lines`, `test_json_close` |
| [INV-O2](07-invariants.md#inv-o2) | CT-6 | **P** | fixtures only |
| [INV-O3](07-invariants.md#inv-o3) | CT-6 | **P** | fixtures only |
| [INV-O4](07-invariants.md#inv-o4) | CT-6 | **P** | fixtures only |
| [INV-O5](07-invariants.md#inv-o5) | CT-6 | **P** | fixtures plus the typed sort-column case |
| [INV-O6](07-invariants.md#inv-o6) | CT-6 | **P** | fixtures compare values, not bytes — see the divergence table in GAPS.md |
| [INV-O7](07-invariants.md#inv-o7) | CT-6 | **P** | `test_arrow_parity_and_projection`; no empty-`fields` case |
| [INV-O8](07-invariants.md#inv-o8) | CT-6 | **C** | `test_snake_to_camel`, `test_snake_to_camel_in_output` |
| [INV-O9](07-invariants.md#inv-o9) | CT-6 | **P** | sixteen encoder cases plus `discriminator_columns_render_as_padded_hex`; known-violated for undeclared millisecond timestamps and for base58 — GAPS 20, 21 |
| [INV-O10](07-invariants.md#inv-o10) | CT-6 | **C** | `test_encode_roll`, `test_encode_roll_with_list_spread` |
| [INV-O11](07-invariants.md#inv-o11) | CT-6 | **P** | field groups are exercised; an unknown tag value is not, and that is the case archives outliving catalogs produce |
| [INV-O12](07-invariants.md#inv-o12) | CT-6 | **U** | byte determinism is never asserted |
| [INV-O13](07-invariants.md#inv-o13) | CT-6 | **U** | no thread-count sweep |
| [INV-O14](07-invariants.md#inv-o14) | CT-6 | **C** | six Arrow-parity cases |
| [INV-E1](07-invariants.md#inv-e1) | CT-9 | **P** | the request half is fuzzed (`request_props`), and `a_chunk_that_disagrees_with_the_catalog_does_not_panic` pins the encoders against a chunk written to disagree. The chunk-type *sweep* the invariant asks for needs HC-3, and two existing tests still assert a panic rather than forbid one |
| [INV-E2](07-invariants.md#inv-e2) | CT-2 | **P** | validation precedes scanning by construction; nothing asserts that no output precedes an error |
| [INV-E3](07-invariants.md#inv-e3) | CT-8 | **U** | the absent-column test covers filtering, not selection |
| [INV-E4](07-invariants.md#inv-e4) | CT-8 | **U** | no missing-table case |
| [INV-E5](07-invariants.md#inv-e5) | CT-2 | **C** | five cases, including a chain that skips block numbers |
| [INV-E6](07-invariants.md#inv-e6) | CT-2 | **U** | known-violated — GAP 28. Errors carry prose, so CT-2 cannot assert a kind |
| [INV-E7](07-invariants.md#inv-e7) | CT-4 | **C** | `test_semi_join_unsupported_key_type` |
| [INV-X1](07-invariants.md#inv-x1) | CT-1 | **P** | `a_relation_target_names_its_own_block_column` serves an invented chain from a synthetic chunk with no code change. One chain and one relation shape: a hardcoded name elsewhere would still pass |
| [INV-X2](07-invariants.md#inv-x2) | CT-8 | **U** | adding a nullable column is never tested |
| [INV-X3](07-invariants.md#inv-x3) | CT-8 | **C** | `filtering_an_absent_column_is_an_error`, `filtering_a_present_column_still_works` |

**Totals: 35 C, 33 P, 17 U** of 85. Property coverage is therefore 0.41
(`P-COV-PROPERTY` in [09-parameters.md](09-parameters.md)).

The shape of the U column is worth reading on its own. The unchecked invariants
cluster in three places, and none of them is an accident:

- **Everything needing a chunk writer** — D8, O12, O13, X2, E3, E4, and half of
  D7. These are the invariants about data written by a version of the archiver
  that no longer exists, and they cannot be tested by reading fixtures. HC-3 is
  the single highest-leverage thing missing.
- **Everything needing generated queries** — P5, R2, R4, R11, B8. Each is a law
  over pairs of queries, and a suite of hand-written cases cannot express one.
- **The catalog validator's remaining blind spot** — D4, and the chunk-level half
  of D5. Both need a chunk to look at rather than a catalog, so both wait on
  HC-3 with the first group.

INV-P9 belongs to none of those groups and is the most dangerous single row in
the table: a bloom construction that disagrees with the archive writer's returns
false negatives, and no client — and no fixture test — can see them.

## 8.12 Merge gates

The specification defines the system's quality bar, so it defines the bar for
changes to it. Without this the matrix above is a status report nobody is obliged
to improve.

Thresholds are `P-*` symbols resolved in [09-parameters.md](09-parameters.md).
A gate is *advisory* while the capability it needs is unbuilt; it is not dropped,
and its promotion is tracked in [GAPS.md](GAPS.md).

| Gate | Threshold | When | Enforced by | Blocking? |
|---|---|---|---|---|
| **MG-1** Property coverage never regresses | `P-COV-PROPERTY`, ratchet only | per-PR | HC-8 | advisory until HC-8 exists |
| **MG-2** Line coverage on changed lines, and a repository floor | `P-COV-DIFF`, `P-COV-TOTAL` | per-PR | HC-9 | advisory until HC-9 exists |
| **MG-3** The PR-budget classes pass | CT-1, CT-2, CT-3, CT-4, CT-5, CT-6 green | per-PR | HC-1, HC-2, HC-4, HC-6 | advisory until HC-4 exists and a job runs the classes |
| **MG-4** The slow classes pass | CT-7, CT-8, CT-9 green | nightly | HC-3, HC-5, HC-7 | advisory until HC-3 exists |
| **MG-5** No performance regression outside the noise band | `P-PERF-NOISE-BAND` | nightly | HC-10 | advisory |
| **MG-6** Spec integrity | the suite's own checker reports no error | per-PR | HC-11 | **blocking** |
| **MG-7** Flake policy | `P-FLAKE-RETRIES`, then quarantine with an owner and an expiry | per-PR | HC-8 | advisory until HC-8 exists |
| **MG-8** Static gates | formatter, linter, dependency audit clean | per-PR | HC-12 | advisory until a job runs them |

Two rules that no tool enforces, so they are review checklist items:

- **A PR that adds an invariant adds its matrix row and its CT case in the same
  change.** A row at **U** on the day it is written is a promise nobody made.
- **A gap closes with the test that fails without the fix**, named in the gap's
  *first test* line. [GAPS.md](GAPS.md) carries that column for exactly this.

One gate blocks today: MG-6, whose capability is built and whose job runs. The
rest name capabilities that do not exist yet, or exist with nothing running them,
so they are advisory — which is the rule above applied rather than an exception
to it. A gate that says **blocking** while nothing can enforce it teaches people
to read the column as decoration.

MG-1's ratchet is the gate that matters. Absolute coverage is a number to argue
about; *the matrix may not get worse* is a rule. Its two inputs — the totals line
under §8.11 and `P-COV-PROPERTY`'s observed cell — are recomputed from the matrix
rows by the checker, so the baseline cannot be loosened by editing a number.

## 8.13 Harness capability register

The gates and the CT classes both assume machinery. Listed with build status, or
"we should test X" stays aspirational forever.

| Capability | Needed by | Status | Note |
|---|---|---|---|
| **HC-1** Fixture chunk loader and query runner | CT-2 – CT-6 | **C** | exists |
| **HC-2** Catalog builder for deliberately invalid catalogs | CT-1 | **P** | a few negative cases exist; no systematic "one violation per check" sweep |
| **HC-3** Chunk *writer* — rewrite a fixture at a chosen physical type, sort key, row-group size, with a column dropped or added | CT-6, CT-8 | **U** | the single highest-leverage missing piece; without it, D7, D8, O12, O13, X2, E3, E4 cannot be tested at all |
| **HC-4** Query generator walking the catalog — pick a table, a filter, values from the chunk's actual contents, a relation subset, a projection | CT-3, CT-4, CT-7 | **U** | what turns the algebraic laws from prose into tests |
| **HC-5** Reference-implementation runner and value-level comparator, with skip counting and a per-dataset floor | CT-7 | **P** | fixtures compare against recorded reference output; nothing runs the reference live |
| **HC-6** Injectable `P-WEIGHT-BUDGET` | CT-5 | **C** | the budget suite already drives it |
| **HC-7** Deterministic fuzzer with a recorded seed | CT-9 | **U** | |
| **HC-8** Matrix parser and coverage reporter | MG-1, MG-7 | **U** | reads §8.11, counts C/P/U, compares against the previous commit |
| **HC-9** Line-coverage instrumentation | MG-2 | **U** | |
| **HC-10** Benchmark runner with committed baselines and a noise band | MG-5 | **P** | benchmarks exist and are recorded; nothing gates on them |
| **HC-11** Spec checker | MG-6 | **C** | reference integrity, dead weight, matrix coverage, normative shape; carries its own mutation tests |
| **HC-12** Formatter, linter and dependency audit | MG-8 | **P** | the commands exist as `make` targets; no job runs them, so nothing gates on them — GAP 29 |

A CT class whose capabilities are all **U** is not "unchecked" — it is
*unbuildable today*, and belongs in the build order rather than the backlog.
CT-8 and CT-9 are in that state.

## 8.14 Build order

Each phase ends by updating §8.11 and [GAPS.md](GAPS.md).

1. **The chunk writer** (HC-3). Seven invariants become testable and the whole
   CT-8 class turns on. Nothing else unblocks as much.
2. **The CI gate** (GAP 29). The suite that pins the closed gaps is worth what
   CI does with it, and today CI runs the spec checker alone. A workflow file.
3. **The query generator** (HC-4). Turns §8.4 and §8.5 from tables of prose into
   generated laws, and closes P5, R2, R4, R11.
4. **INV-B8, partition invariance.** One test, six invariants, and the property
   the distributed architecture rests on.
5. **INV-P9, the bloom oracle.** Compare the engine's bloom bits against the
   writer's for known values. The only invariant here whose failure is invisible
   to every other test in the suite.
6. **Coverage reporting** (HC-8) and line instrumentation (HC-9), which promote
   MG-1 and MG-2 from advisory to blocking.
