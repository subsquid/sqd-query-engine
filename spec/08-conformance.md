# 8. Conformance

How to turn [07-invariants.md](07-invariants.md) into a test suite that finds
gaps rather than confirming what you already believe.

This chapter is advisory. The invariants are the contract; this is one way to
check it.

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

So: build fixtures *and* the five layers below. The layers find the gaps;
the fixtures stop regressions.

## 8.2 Layer 0 — Catalog validation

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

## 8.3 Layer 1 — Request validation

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

Then fuzz. Generate random JSON, random mutations of valid queries, and every
type-substitution at every position. Assert only one thing:
[INV-E1](07-invariants.md#inv-e1) — no panic. This finds more than the table
does.

## 8.4 Layer 2 — Algebraic laws

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

## 8.5 Layer 3 — Relations

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

## 8.6 Layer 4 — Blocks, weight, pagination

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

- Force the budget small (an injectable bound, not 20 MiB) and page a chunk end
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

## 8.7 Layer 5 — Output and determinism

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

## 8.8 Layer 6 — Differential testing

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

## 8.9 Layer 7 — Adversarial chunks

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

## 8.10 Coverage ledger

Keep a machine-checked map from invariant ID to test name, and fail the build on
an unmapped invariant. Something as simple as:

```
INV-P3   → filters::empty_list_matches_nothing
INV-P8   → filters::hex_columns_fold_case
INV-X3   → chunks::filter_on_absent_column_errors
INV-B8   → paging::partition_invariance
…
```

An invariant with no test is an invariant the engine does not have. The ledger is
how you find out which ones those are, and it is the answer to the question this
whole document exists to serve: *what haven't we checked?*
