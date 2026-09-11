# Gap analysis — implementation vs. specification

**Non-normative.** This document records where the engine in this repository
currently diverges from [the specification](README.md). It is kept out of the
normative chapters on purpose: a spec that describes today's bugs cannot be used
to find them.

Delete an entry when the gap closes. If the spec turns out to be wrong and the
implementation right, fix the spec and delete the entry. The document should tend
toward empty.

Compared against the reference implementation, as of 2026-09-05.

---

## Severity

| | Meaning |
|---|---|
| **S1** | Wrong results that look right. A client cannot detect the failure. |
| **S2** | Missing capability. A query that should work is rejected, or a dataset is unserved. |
| **S3** | Crash, or an error where a result was due. Loud; a client can retry or route around it. |
| **S4** | Robustness, hygiene, latent. |

## Summary

| # | Gap | Invariant | Sev |
|---|---|---|---|
| 37 | A relation onto a table the chunk lacks is an error even when the primary scan is empty | [INV-E4](07-invariants.md#inv-e4) | **S3** |
| 38 | Every parquet file in the chunk directory is opened for every query | [INV-E4](07-invariants.md#inv-e4) | **S3** |
| 44 | The weight model differs from the reference in four places | [INV-B5](07-invariants.md#inv-b5) | **S4** |
| 31 | A block number above 2³¹ stored in `Int32` is read as negative by the range filter | [INV-D7](07-invariants.md#inv-d7) | **S4** |
| 32 | The bloom's hash function is not pinned by the manifest, and the version it resolves to today ignores the seed above 240 bytes | [INV-P9](07-invariants.md#inv-p9) | **S4** |

Every dataset [chapter 3](03-catalog.md) names is served except `fuel`, which is
out of scope ([ADR-10](decisions/ADR-10-fuel-is-out-of-scope.md)). On a
well-formed chunk carrying every table the query names, the only requests the
reference answers and this engine refuses are the ones the divergence table
below says are deliberate. Outside that case gaps 37 and 38 refuse as well: a
missing target table, and an unreadable file beside the ones the query reads.

Gaps 33 to 44 came from one review, done before the engine goes to a fleet of
workers that nobody can patch quickly. The number 34 was never assigned, and 33,
35, 36 and 39 to 43 are closed, so three of those entries are left. The review ran the reference
and this engine side by side: every filter, relation, alias and field of all
seven datasets diffed against the reference's request macros; about 330
request probes and about 700 response runs on the real chunks and every
fixture chunk; about 70 synthetic chunk variants (retyped columns, dropped
columns, every codec and encoding, shuffled row groups, empty tables); and the
peak memory of the worst query shapes on both engines. Everything the review
could reach that is not listed here matched the reference exactly: the whole
filter algebra, every `lastBlock` case off the wave path, ordering, dedup,
every encoding in use, and every integer width, codec and row-group layout
either writer has produced.

Several of the review's entries were about what the engine does with a chunk
that is older than its catalog. Two archiver generations wrote the chunks in the field.
The Python one wrote every Substrate dataset and most EVM datasets, including
ethereum-mainnet in 2026: `Int32` block numbers, `Int64` sizes, and a column set
that stops at the 2024 additions. The Rust one wrote Solana, bitcoin,
hyperliquid, tempo and some EVM datasets, with `UInt16` indices and state diffs
sorted by address rather than block. The catalog declares the Rust layout for
widths and columns, but its `statediffs` `sort_key` is block-led, which is the
Python one; most chunks have the Python layout throughout. Range planning uses
row-group statistics to suggest read sizes. Overlapping
groups produce larger ranges, while missing or inverted bounds select a full
read. Where the engine
meets a column that is absent, it errors as [INV-E3](07-invariants.md#inv-e3)
and [INV-X3](07-invariants.md#inv-x3) require; where it meets one stored at
another type, it reads every integer width and every text type, and errors as
[INV-E7](07-invariants.md#inv-e7) requires on a type it cannot compare or
render. What is left of that family is gap 44's weight model.

The reference moved while this was written. Twelve Avalanche block-header fields
and Solana's `transactionConfig` landed there in the first days of September,
and the test that pins the field surface did not notice because it is a hand
transcription of the older revision. The lag is closed, and
`the_catalogs_serve_every_field_the_reference_serves` now reads the reference's
field macros off the checkout the fixture tree comes from, so the next such
move fails a test rather than waiting for a review.

---

## S3 — Loud

### 37. A relation onto a table the chunk lacks is an error even when the primary scan is empty

`ensure_required_tables_present` runs before any scan. A chunk with no
`statediffs` file, queried with `logs: [{transactionStateDiffs: true}]` over an
empty logs table, gets `TableNotFound`; the reference opens the target only when
the relation has inputs and answers with header-only blocks.
[INV-E4](07-invariants.md#inv-e4) says a missing table is an error and does not
say when; it should.

*First test:* an EVM fixture chunk with `statediffs.parquet` removed — every
chunk in the tree carries one — queried as above, asserting the reference's
answer.

### 38. Every parquet file in the chunk directory is opened for every query

`ParquetChunk::open` maps every `*.parquet` in the directory and parses each
footer before the plan runs. A truncated `traces.parquet` or a stray temporary
file fails a blocks-only query with an untyped error; the reference opens tables
lazily and answers. It is also a cost: a header-only query pays every table's
footer parse.

*First test:* a chunk with one zero-byte extra parquet file and a query that does
not touch it.

---

## S4 — Latent

### 44. The weight model differs from the reference in four places

Weight dedup now compares key values after hashing, so colliding hashes cannot
merge distinct rows. The remaining differences concern the weight model.

Four smaller differences move the cut by one block in either direction: boundary
blocks weigh zero here and their header weight there; the key columns weighed
when not selected differ; the header's `number` is weighed only when selected;
Substrate `digest` weighs 32 here and 128 there. Items are equal when each engine
pages independently, so none of this is wrong, but until it is closed the parity
suite cannot assert `lastBlock` equality against the reference. The engine's
answer is a prefix of the reference's on every shape the review ran, so a
paging client loses nothing; only the page boundary moves.

Range reads compute the final weight after collecting every table and relation
for a complete block range. The narrow size pre-scan only suggests the first
range; it does not decide the response boundary. There is no table-local budget
estimate or recursive query restart.

*First test:* pin the reference's per-block weights for the real chunk and diff.

### 31. A block number above 2³¹ stored in `Int32` reads as negative

Two places widen a stored block number and they disagree about sign.
`IntColumn::block_number` reinterprets — `(v as u32) as u64` — so a wrapped value
reads back as the block it is, and every reader that resolves a block number
through it agrees. `block_range_mask` in `src/scan/scanner.rs` does not: it
compares through Arrow's kernels at the stored type, so the same value sorts
below every bound and the row group is filtered away.

The visible effect is a response of zero bytes for a range the chunk covers. Not
"the wrong rows" — no rows, and no error, which is indistinguishable from a chunk
that ends before the range starts.

The same widening reaches the row-group statistics. A writer comparing wrapped
values signed records the block above the wrap as the group's minimum and the
highest block below it as the maximum, so widened back the pair reads `min > max`:
a range starting above where the group ends. Three readers act on it — the range
planner sizes reads with it, the block-range pruner skips on it, and the relation
pruner tests it for overlap with the key set — and the pruners are the ones that
lose rows. The relation pruner loses them unconditionally: every key at or above
an inverted minimum is above the inverted maximum by construction, so the overlap
test cannot pass and the group is dropped whatever the query asked for.

All three now read bounds through one function, `block_bounds`, which returns
nothing for a pair that reads back inverted; a statistic no reader can trust is
refused for all of them or for none. That is not a fix for this entry, only a
refusal to make it worse — and, having been split across three sites once
already, one place it cannot be forgotten in again.

The widening is not `Int32`'s alone, and `block_bounds` no longer writes it out:
parquet carries a narrowed integer's statistics in an `Int32` whatever the
column's own width, so the same bits mean one block at the column's width and
another four thousand million away at the statistic's. Read at 32 bits, a signed
`Int16` block column places its row groups where its rows are not, and the
pruners drop them from every query that asks for the blocks they hold — this
entry at a width a chain could actually reach, since sixteen bits runs out at
block 32 768. The bounds now widen through `block_number_at`, the rule the rows
themselves are read by. Nothing observes the difference yet: `block_range_mask`
refuses every bound above a signed column's own maximum, so such a chunk answers
no rows either way. The refusal argued for below is what would make it
observable, and it covers these widths as it covers `Int32`.

[INV-D7](07-invariants.md#inv-d7) says any integer width and *signedness*, so the
engine is wrong and the spec is right. It is **S4** rather than **S1** because
reaching it needs a writer that keeps `Int32` past 2³¹ instead of widening — the
`UInt32` arm, which any sane writer would pick, is already correct — and because
the chains served are between one and two orders of magnitude below that number.

The fix is not a one-line reinterpretation. A `[from, to]` range that straddles
2³¹ is two disjoint intervals in the signed domain, so `block_range_mask` needs
the straddle case rather than a different scalar, and writing it as a scalar loop
the way `block_below_mask` does would give up the SIMD kernel on a path that runs
over every row of every block-filtered scan. Below it sits a second rule already
in the way: `from_block` of zero is treated as no lower bound, which is true for
an unsigned column and false for this one — the wrapped rows are negative there,
and nothing excludes them.

So serving such a chunk correctly means three readers that know about the wrap,
plus a fourth reader whose statistics cannot be repaired at all: a straddling row
group's bounds do not say where it starts, because the writer recorded its largest
block as the minimum. That much is not a bug to fix but information the file does
not carry.

Refuse the chunk instead. `MalformedChunkData` says what is true — the catalog
declares `uint64` and the column stores a number its physical type cannot hold —
and a signed block number below zero is exactly that, detectable from one
statistic per row group. Refusing turns this entry from silent into loud, **S4**
into **S3**, and replaces three straddle-aware readers with one check.

It costs something, and the cost should be stated: the rows themselves widen
correctly, so a fully straddle-aware engine would answer this chunk and this one
refuses it. That is a divergence from the reference, which answers, and belongs in
the table above when it lands. It is a trade worth making while the layouts that
can reach it — those keeping block numbers in a signed 32-bit column — are eight
centuries from 2³¹ at twelve seconds a block. Solana is the closest chain to the
number, five times below it, and cannot reach this entry at all: it is written
`UInt32`, so its statistics are sorted unsigned and every reader here widens them
correctly.

The check belongs where the block-number column is resolved, once per scan, not
where each reader happens to notice. The same chunk failing on one query and
answering on another — which is what a per-reader check gives, since a plan that
takes a different path never looks — is the *terminal* case the divergence table
describes.

That resolution point now exists: `BlockNumbers` in `src/integers.rs`, run by the
scan on every batch it hands out. It refuses a block-number column stored at no
integer width, and one carrying a null — a row the width-tolerant reader would
have placed under block 0, differently in each of the five readers that used to
widen the column themselves. What it does not yet refuse is the value this entry
is about, because a wrapped block is a legal `Int32` and only the catalog's
declared `uint64` says otherwise. Adding that check is the remaining work, and it
is now one line in one place rather than a straddle case in three readers. The
statistic refusals stay either way: a corrupt statistic needs no wrapped block to
arrive.

*First test:* a chunk whose block numbers start at 2 200 000 000 stored as
`Int32` must be refused with `MalformedChunkData`, on every query shape — a
bounded range, an unbounded one, a filtered scan, a relation pull — rather than
answered by one and dropped by another.
`physical_width_does_not_reach_the_answer` sweeps every width already; what it
cannot reach is a value that does not fit the width it is stored at, which needs a
writer that wraps rather than widens.

### 32. The bloom's hash function is not pinned, and today's version drops the seed above 240 bytes

`bloom_bit` tells one of a value's seven hashes from another by exactly one
thing: it passes `n` as the seed. XXH3 mixes a seed differently in its two
regimes — directly on inputs up to 240 bytes, and through a secret derived from
it above that — and on the version `Cargo.lock` currently resolves, 0.8.15, the
derivation does not happen. Above the threshold the seed is ignored, so all seven
hashes return one bit and the filter carries one bit per value instead of seven.

Measured against 0.8.18, which does not have it; the fix first appears in
0.8.17. Below the threshold the two are
byte-identical, including for a real 44-byte account key; at 240 bytes 0.8.15
returns the same bit seven times and 0.8.18 returns seven different ones.

Nothing the engine hashes reaches the threshold. `mentionsAccount` needles are
base58 account keys, 44 bytes at most, and no other filter uses a bloom — which
is why this is **S4** and not **S1**, and why `cargo update` is safe today rather
than the hash change it looks like.

The direction that would hurt is a version skew between the archive writer and
the engine, on a value long enough to reach the long path. A writer on the broken
version sets one bit where a reader on the fixed one tests seven, so every such
value is a false negative — what [INV-P9](07-invariants.md#inv-p9) forbids and no
client can detect. The reverse skew only floods false positives, which the
invariant permits.

Half of this is already guarded, and it is worth being precise about which half.
`the_engine_builds_the_bloom_the_archiver_wrote` compares the engine's bits
against rows an archiver wrote, and the portable gate runs it, so a hash that
moved under a resolution would fail the build for any value of ordinary length.
What no test reaches is a value of 240 bytes or more, and a vector for one cannot
be written: no chunk carries such a value, so there is nothing to oracle against
and the test would only pin the engine to itself.

The manifest is the real gap. `xxhash-rust = { version = "0.8.15" }` is a caret
requirement, so the hash function is fixed by `Cargo.lock` alone — and a lock does
not apply to a consumer that takes this crate as a library. The worker that does
resolves 0.8.17 today, and 0.8.17 is where the seed fix landed — so the skew is
already deployed, this tree's 0.8.15 against the worker's 0.8.17, rather than
hypothetical. INV-P9 says the hash function must match the archive writer's
exactly; the manifest does not say which one that is. An exact `=` requirement,
on 0.8.17 or later rather than 0.8.15, states it and costs nothing measurable.

`bloom_bit`'s own doc says the value goes "through XXH3 seeded with `n`", which is
true only below the threshold on the pinned version.

*First test:* not an oracle — there is nothing to compare a long value against.
The check that fits is a unit test pinning `bloom_bit`'s seven bits for a
240-byte value once a version is chosen, which fails if the resolution moves
across the bug in either direction.

---

## Deliberate divergences from the reference

Cases where the spec sides with the engine, or with neither, and the reference is
the one that is wrong. Listed so nobody "fixes" them back.

One thing to hold in mind while reading the table. The portal validates every
query with the reference's parser before it sends it, so a query that reaches a
worker is one the reference accepts. Where this engine rejects such a query, the
worker reports a request error, the portal does not retry, and the client sees
the failure. During a mixed fleet — some workers on each engine — every row
marked *terminal* below is a request that works or fails depending on which
worker answers.

| Behaviour | Reference | Spec | Reason |
|---|---|---|---|
| Request bounds | No byte or list-length bound in the engine; the transport admits a little over 4 MiB | `P-MAX-REQUEST-BYTES` and `P-MAX-IN-LIST`, refused as `RequestTooLarge` ([ADR-13](decisions/ADR-13-request-resource-bounds.md)) | The engine states and enforces its own resource contract. *Terminal* for a body between the two bounds; the largest request on record is 1.13 MiB with 21 821 addresses, so the margin is under 2× on bytes. |
| Malformed hex in a filter list — missing `0x`, odd length, a non-hex digit, an empty string | Lower-cases and compares: the bad value matches nothing and the well-formed values in the same list still match | `InvalidHex` for the whole request ([INV-Q12](07-invariants.md#inv-q12)) | A value that cannot match is a mistake the client should hear about. *Terminal*, and the reference's leniency is what the SDK relies on: it lower-cases list values and validates nothing, so a typo in one address of twenty reaches the worker. The change that introduced it (the last day of August) conceded the reference is more lenient; whether to hold the line during the migration is undecided. |
| `null` as a filter value | Absent: `"address": null` is no constraint | `InvalidFilterValue` | Every filter value has a form the kind accepts and `null` is not one. *Terminal*; also present in earlier revisions of this engine. |
| `hyperliquidReplicaCmds` aliases | `orderActions`, `cancelActions`, `cancelByCloidActions`, `batchModifyActions` add no `actionType` predicate — `orderActions: [{user: [U]}]` returns every action type for `U` | Each alias carries an implicit `actionType` predicate ([§3.8](03-catalog.md#38-hyperliquidreplicacmds)) | The alias name promises orders; the reference does not keep the promise. **Silent:** 7 092 rows there, 3 856 here for the same request. The reference also reads `asset`/`cloid` columns the parquet does not have for `cancelByCloidActions.containsAsset`/`containsCloid`, so those filters error there and answer here. |
| Item-request cap counting | `hyperliquidReplicaCmds` counts only the `actions` array; its four aliases are uncounted | Count every item request, uniformly ([INV-Q5](07-invariants.md#inv-q5)) | An uncounted alias is an unbounded scan. *Terminal* above a hundred items through aliases. |
| Shapes the reference rejects and this engine accepts | Duplicate JSON keys, a bare string where a list is expected, `null` for `fromBlock` / `includeAllBlocks` / `fields`, `status` as a list or in upper case, `0X` on a Solana discriminator, an integer in a `d1` list | Accepted; last key wins, a scalar is a one-element list, `null` is the default, bogus values match nothing | Harmless while the portal filters with the reference's parser; a client relying on one of these gets a request error from a reference worker and data from this one. |
| Empty result | `[]` (array writer) or empty (lines writer) | Zero bytes ([INV-O1](07-invariants.md#inv-o1)) | NDJSON is the only format; zero bytes concatenates. Moot on the wire: the worker uses the lines writer for the reference too. |
| Response field order | Declaration order in the query DSL | Catalog column order ([INV-O6](07-invariants.md#inv-o6)) | Both are stable; the catalog is the single source of truth. Values are equal, bytes are not — the parity suite must compare values, not bytes. |
| String escaping | `\u`-escapes non-ASCII | Raw UTF-8 | Both valid JSON. Same reason as above. |
| Float formatting | `1.634916549089593e11` | `163491654908.9593` | Same `f64`; any JSON parser agrees. Bitcoin `difficulty` and values, hyperliquid prices. |
| Signed filter values | No signed arm either; `transactions.version` and `rewards.lamports` are unfilterable | Filterable where a catalog declares it ([INV-P14](07-invariants.md#inv-p14)) | No bundled catalog declares such a filter, so nothing changes on the wire. What changed is that refusing one is now a catalog decision rather than a hole in the compiler. |
| `parentBlockHash` when `fromBlock` is outside the chunk | Errors: the window below `fromBlock` is empty and the lookup fails with a server error | Skip the check ([INV-E5](07-invariants.md#inv-e5)) | A chunk that cannot see the block is not evidence of a fork. Unreachable through the portal, which intersects the range with the chunk before sending. |
| Fork search window | Searches back over *parent* numbers, with a standing FIXME that a longer gap in block numbering misses the parent | Anchor the check at `fromBlock`, whose row states its own parent's hash; the window only sizes the evidence ([§2.1](02-request.md#parentblockhash)) | A window is a guess about how far back the parent lies. The row that answers is at a known number, so nothing has to be guessed and a numbering gap of any width is answered. |

Two rows that used to be here are gone: the reference emitted Substrate
`event.callAddress` twice and now emits it once, and the reference's bloom was
suspected of post-filtering and does not — both engines return the same false
positives, as [ADR-8](decisions/ADR-8-bloom-false-positives-are-contract.md)
says they may.

---

## Extensions beyond the reference

Permitted; must not change the meaning of anything above.

- A `substrate.extrinsics` item request array. The reference reaches extrinsics
  only through relations. The reference rejects the key, so it must not be
  advertised until every worker serves it.
- A columnar (Arrow IPC) response encoding, subject to
  [INV-O14](07-invariants.md#inv-o14).
- `evm.traces.rewardValueNonZero` — present in both, listed here because it is
  easy to miss among the other three `*NonZero` flags.

---

## Outside the engine

The spec ends at the engine's boundary, and the review did not. Nobody reading
this document should think closing the gaps above makes the switch safe.

The review also covered the seams between the engine and the systems that
deploy, distribute and route around it: how catalogs reach a worker, what the
released engine revision does, how an execution error is classified once it
leaves the engine, and the behaviour of a mixed deployment during a switch.
Those findings concern operational surfaces outside this repository and are not
recorded here. They are tracked privately and are a precondition of the switch.
What is in scope for this document is what the suite itself proves.

### What the suite proves about parity

The continuous build runs no comparison against the reference at all. The
fixture tree is not in the repository, and `fixture_tree_has` skips a test whose
chunk is absent, so CI reports green having compared nothing. `SQD_REQUIRE_FIXTURES`
is what turns that skip into a failure, and CI does not set it. On a checkout
with the tree in place no variable is needed: all 112 fixture queries and the 48
fixture-backed conformance tests run and pass. The one live differential (CT-7) runs
600 probes with no disagreement, but against a reference checkout a month old,
over the first forty blocks of seven fixture chunks, with plain
in-list filters and the default projection only, and never in CI. It covers
ethereum, optimism, solana, kusama, moonbeam, bitcoin and tron; binance,
hyperliquid, hyperliquid_replica_cmds and tempo have fixture comparison against
reference-generated results but no live differential. Datasets with a real
archive chunk on disk: one Ethereum, one Solana.

The three rows that overstated their evidence — [INV-D6](07-invariants.md#inv-d6),
[INV-P13](07-invariants.md#inv-p13) and [INV-Q14](07-invariants.md#inv-q14) — were
read against the tests they name and lowered to **P**, which took
`P-COV-PROPERTY` from 0.73 to 0.69. The number a ratchet starts from has to be one
the tests earn, so it was moved down once rather than locked in. What each row is
missing is now written in the row; INV-Q14 has since earned **C** back with a test
that recomputes the field surface from the reference's own source.
[ADR-4](decisions/ADR-4-closed-field-surface.md)
and [ADR-9](decisions/ADR-9-reject-undecidable-fork-checks.md) are still `Proposed`
while their MUST text is implemented.
