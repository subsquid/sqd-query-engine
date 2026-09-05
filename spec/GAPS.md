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
| 33 | The budget early-stop on a block-sorted table drops whole blocks, and `lastBlock` jumps past them | [INV-B7](07-invariants.md#inv-b7) | **S1** |
| 40 | A filter on a column stored at a physical type outside the predicate's downcast matrix matches nothing | [INV-D7](07-invariants.md#inv-d7) | **S1** |
| 41 | A roll field tolerates absent source columns and emits a misaligned list | [INV-E3](07-invariants.md#inv-e3) | **S1** |
| 35 | The catalogs lag the reference by thirteen fields, and one of them cannot be expressed | [INV-X1](07-invariants.md#inv-x1) | **S2** |
| 36 | Relation expansion materialises every target table before the weight budget is applied | [INV-B6](07-invariants.md#inv-b6) | **S3** |
| 37 | A relation onto a table the chunk lacks is an error even when the primary scan is empty | [INV-E4](07-invariants.md#inv-e4) | **S3** |
| 38 | Every parquet file in the chunk directory is opened for every query | [INV-E4](07-invariants.md#inv-e4) | **S3** |
| 39 | Two predicate downcasts panic instead of returning `UnsupportedKeyType` | [INV-E7](07-invariants.md#inv-e7) | **S3** |
| 42 | A missing `*_size` column weighs zero | [INV-B9](07-invariants.md#inv-b9) | **S4** |
| 43 | An unsupported physical type renders as `null` instead of erroring | [INV-E3](07-invariants.md#inv-e3) | **S4** |
| 44 | The weight-dedup hash collides structurally, and the weight model differs from the reference in four places | [INV-B5](07-invariants.md#inv-b5) | **S4** |
| 31 | A block number above 2³¹ stored in `Int32` is read as negative by the range filter | [INV-D7](07-invariants.md#inv-d7) | **S4** |
| 32 | The bloom's hash function is not pinned by the manifest, and the version it resolves to today ignores the seed above 240 bytes | [INV-P9](07-invariants.md#inv-p9) | **S4** |

Gaps 40 and 41 need a chunk of an older archiver vintage to reach. Whether any
such chunk is still served is unknown here, and the rating turns on it: **S1** if
one is, **S4** if none is. Both are listed at the higher rating until an
inventory settles it.

Every dataset [chapter 3](03-catalog.md) names is served except `fuel`, which is
out of scope ([ADR-10](decisions/ADR-10-fuel-is-out-of-scope.md)). On a
well-formed chunk carrying every table the query names, the only requests the
reference answers and this engine refuses are the ones the divergence table
below says are deliberate, plus gap 35, which is not. Outside that case gaps 37
to 39 refuse as well: a missing target table, an unreadable file beside the ones
the query reads, and two physical types that panic.

Gaps 33 to 44 come from one review, done before the engine goes to a fleet of
workers that nobody can patch quickly. The number 34 was never assigned; the
register carries eleven new entries, not twelve. The review ran the reference
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

Three of the new entries are about what the engine does with a chunk that is
older than its catalog. Two archiver generations wrote the chunks in the field.
The Python one wrote every Substrate dataset and most EVM datasets, including
ethereum-mainnet in 2026: `Int32` block numbers, `Int64` sizes, and a column set
that stops at the 2024 additions. The Rust one wrote Solana, bitcoin,
hyperliquid, tempo and some EVM datasets, with `UInt16` indices and — the fact
gap 33 turns on — state diffs sorted by address rather than block. The catalog
declares the Rust layout for widths and columns, but its `statediffs`
`sort_key` is block-led, which is the Python one; most chunks have the Python
layout throughout. Where the engine
meets a column that is absent or retyped, it now mostly errors as
[INV-E3](07-invariants.md#inv-e3) and [INV-X3](07-invariants.md#inv-x3) require.
Gaps 40 to 43 are the places it still does not.

The reference moved while this was written. Twelve Avalanche block-header fields
and Solana's `transactionConfig` landed there in the first days of September; gap 35
is that lag, and the test that pins the field surface did not notice because it
is a hand transcription of the older revision. A check that diffs the reference's
field macros against the catalogs on every build would have.

---

## S1 — Wrong results that look right

### 33. The budget early-stop on a block-sorted table drops whole blocks, and `lastBlock` jumps past them

A table whose sort key starts with the block column — `evm.statediffs`,
`bitcoin.transactions`, `substrate.extrinsics` — is scanned in waves that stop
once the cumulative weight crosses the budget, keeping only the rows of blocks
below the first unread row group (`scan_waves_until_budget` and
`retain_blocks_below` in `src/scan/scanner.rs`). That cut never reaches block
selection. The other item tables were already scanned for the whole range, the
range-end boundary block of [INV-B3](07-invariants.md#inv-b3) stays a candidate,
and `apply_weight_limit` sees only the rows the walk kept. When the retained
weight sits under the budget — which on block-partitioned layouts happens
whenever the wave's overshoot is smaller than the shared boundary block, and on
address-sorted layouts happens nearly always — selection runs past the cut. The
response then either ends with a header-only block at `toBlock`, or carries
blocks with every table's rows but this one's. The client resumes from
`lastBlock + 1` and never asks for the missing blocks again, which is exactly
what [INV-B7](07-invariants.md#inv-b7) forbids.

The wave is `rayon::current_num_threads()` row groups wide, so the same query on
the same chunk answers differently on workers with different core counts,
against [INV-O13](07-invariants.md#inv-o13). The reference reads every selected
row group before weighing and has no such cut.

Measured on the real EVM chunk with `stateDiffs: [{kind: […]}]`, fields
`{transactionIndex, key}`, range 24550605–24550820: the reference returns
`lastBlock = 24550676` and 72 blocks; this engine at one thread and at seventeen
returns the same 72 blocks, then `{"header":{"number":24550820}}`, and
`lastBlock = 24550820` — 143 blocks of state diffs gone. At two to sixteen
threads it matches the reference. On the Rust-written `ethereum` fixture chunk,
`stateDiffs: [{}]` with `logs: [{topic0: […]}]` returns 326 blocks with 868 state
diffs in total, 325 of the blocks with no `stateDiffs` array at all; the
reference returns 73 blocks and 71 472 diffs.

The address-sorted layout is a second fault under the first: the catalog's
`sort_key` for `statediffs` describes the Python archiver's layout, and the wave
path trusts it. Row-group statistics on the block column say which layout a file
has; the path should read them before assuming order.

Introduced on the first of September. Revisions before it have the previous
fault on the same path instead: on the address-sorted layout
it emits a block with 197 of its 868 diffs, against
[INV-B4](07-invariants.md#inv-b4).

*Why the suite missed it:* the five tests of the cut assert on the scanner's
rows, not on the response, and no fixture query truncates on a block-sorted
table. Of the three `stateDiffs` fixtures only
`state_diffs_no_predicate_with_transaction` is a single block; the other two
reach the wave path and do not truncate on it.

*First test:* an R2-layout synthetic chunk whose overshoot lands inside the block
two row groups share; page it end to end at one, two and seventeen threads and
assert the concatenation equals the reference's single-shot answer
([INV-B7](07-invariants.md#inv-b7)'s own test, run on a table that takes this
path). Then the fix: return the cut from the walk and cap block selection and
`header_to_block` at `unread_min - 1`, as the two-phase path already does.

### 40. A filter on a column stored at a physical type outside the predicate's downcast matrix matches nothing

`InListPredicate::evaluate` matches a `uint8` list only against `UInt8Array`, a
`uint16` list only against `UInt16Array`, and a string list only against
`StringArray`; `EqPredicate` wants the exact array type for booleans and text.
Anything else evaluates to all-false, silently. The reference casts the scalar to
the column's type and errors when it cannot.

A Solana chunk with `d1` stored as `UInt16` or as a hex string, or `is_committed`
as `Int8`, loses 1 968 of 3 742 instructions and eight blocks from the answer,
with a 200. The Rust archiver stored `d1…d8` as hex strings until late February 2025, and
the Python one always did; every Solana chunk of either vintage takes this path
for every discriminator filter. Whether any is still served could not be checked
from here — the current dataset is a later generation — so this is **S1** if one
is and **S4** if none is.

[INV-D7](07-invariants.md#inv-d7) says any width and signedness for an integer
column; [INV-E7](07-invariants.md#inv-e7) says an uncomparable type is an error
and never "matches nothing". The width sweep the suite already has filters on a
string column only.

*First test:* extend `physical_width_does_not_reach_the_answer` to filter on an
integer and a boolean column at every width the writer could narrow to, and on
the string spelling of a discriminator.

### 41. A roll field tolerates absent source columns and emits a misaligned list

`required_output_columns` skips the sources of a virtual field, and the writer
rolls whatever columns exist. A Solana chunk without `a12…a15` — the Python
layout before late March 2024 — renders `accounts` with the tail spliced into position
twelve and no error. The reference errors on the first missing source. Positional
data that is silently shifted is the case
[INV-E3](07-invariants.md#inv-e3)'s *Why* describes, one column further in.

Same reach and same caveat as gap 40.

*First test:* drop one source column of a roll field from a fixture chunk and
assert `ColumnNotFound`.

---

## S2 — Missing capability

### 35. The catalogs lag the reference by thirteen fields, and one of them cannot be expressed

The reference at its current revision serves twelve Avalanche
block-header fields — `blockExtraData`, `blockGasCost`, `extDataGasUsed`,
`extDataHash`, `minDelayExcess`, `timestampMilliseconds`, `targetExponent`,
`minPriceExponent`, `settledHeight`, `settledGasUnix`, `settledGasNumerator`,
`settledExcess`, with `blockExtraData` weighed by `block_extra_data_size` — and
Solana `transactionConfig`. Neither the catalogs nor [chapter 3](03-catalog.md)
has them, so a client selecting any of them gets `UnknownField`, which the worker
reports as a request error and the portal does not retry.

Twelve are plain hex-bytes columns and a catalog edit. `transactionConfig` is a
struct whose `priorityFee` the reference renders as a decimal string; the catalog
has a `struct` type but no per-member encoding, so the closest catalog expresses
it with a JSON number. By [INV-X1](07-invariants.md#inv-x1) that is a spec bug:
the catalog format needs member encodings.

The test that pins the field surface, `the_field_surface_is_exactly_the_declared_one`,
is green because its reference list is a transcription of the older revision.

*First test:* a check that reads the reference's field macros and fails when a
catalog lacks a field the reference serves — the transcription, recomputed.

---

## S3 — Loud

### 36. Relation expansion materialises every target table before the weight budget is applied

The budget is applied before rows are materialised on two paths only: the
single-table pre-scan, and the wave walk of gap 33. Every other plan scans each
item table for the whole range, then scans each relation target with a key
filter built from *all* primary rows, and only then selects the weighted prefix
of [INV-B6](07-invariants.md#inv-b6). The wire answer honours the invariant; the
process does not. The reference reads key columns first and data columns after
the join.

Measured on the real EVM chunk (73 MB), release build, JSON output, identical
responses on both engines:

| Query shape | Reference | This engine |
|---|---|---|
| `includeAllBlocks`, `{}` on all four tables, every field, every relation | 203 MB, 196 ms | 3 134 MB, 2 369 ms |
| same, no relations | 199 MB, 158 ms | 771 MB, 234 ms |
| `traces {transaction, transactionLogs}` + `stateDiffs {transaction}` | 267 MB, 243 ms | 704 MB, 271 ms |
| `logs {transaction}` | 121 MB, 86 ms | 238 MB, 106 ms |
| `logs: [{}]` (two-phase path) | 99 MB, 66 ms | 85 MB, 56 ms |

A worker runs many such queries at once, on chunks larger than this one, and an
out-of-memory kill is not a panic anyone catches: it ends every query on the
box. No invariant bounds working memory, which is a gap in the spec as much as in
the engine; the perf gate of [§8.12](08-conformance.md#812-merge-gates) measures
time, not space.

*First test:* a memory bench with a counting allocator, gated on a ratio to the
chunk size, run over the shapes above.

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

### 39. Two predicate downcasts panic instead of returning `UnsupportedKeyType`

`BloomFilterPredicate::evaluate` expects a `FixedSizeBinary` array and
`eval_dict_typed` unwraps a `DictionaryArray<Int32>`. A bloom column stored as
`Binary`, or a dictionary keyed by anything but `Int32`, panics. The worker
catches the panic and reports a server error, so this is loud rather than fatal;
[INV-E7](07-invariants.md#inv-e7) wants the typed error. No current writer
produces either shape.

*First test:* the two retyped columns on a synthetic chunk, asserting the kind.

---

## S4 — Latent

### 42. A missing `*_size` column weighs zero

System columns are not required for output, so a chunk without `logs.message_size`
or `data_size` answers with the variable-size field selected and weighed at zero.
The page is then bounded only by the transport's limit. The reference requires
the column. [INV-B9](07-invariants.md#inv-b9) says weight is a pure function of
projection and values; a value that contributes nothing because its size column
is missing is not that. Rust-written Solana chunks from mid-December 2024 to
mid-February 2025 and Python-written EVM chunks before late October 2023 lack these columns.

*First test:* drop `message_size` from a fixture chunk and assert
`ColumnNotFound` when `message` is selected.

### 43. An unsupported physical type renders as `null` instead of erroring

`resolve_value_encoder` falls back to the null encoder for `LargeUtf8`,
`Utf8View`, dictionary-encoded columns, `LargeList`, and microsecond or
nanosecond timestamps, and the variant writer reads the tag column as
`StringArray` only, so a dictionary-encoded `traces.type` drops every `action`
and `result` group. The reference errors on each. No writer today produces these
types; the next writer change would turn this into gap 40's shape.

*First test:* each retyping on a fixture chunk, asserting `MalformedChunkData`.

### 44. The weight-dedup hash collides structurally, and the weight model differs from the reference in four places

Weight dedup identifies a row by an FxHash of `(item_order_keys, address_column)`,
and FxHash absorbs leading zero words, so `(tx = 0, trace_address = [0])` hashes
equal to `(tx = 1, trace_address = [])`. Six of seventy blocks on the real EVM
chunk are under-weighed by up to 13 130, and the cut lands one block later than
the reference's. The emitted rows are right because the writer dedups by value;
only [INV-B5](07-invariants.md#inv-b5)'s arithmetic is off.

Four smaller differences move the cut by one block in either direction: boundary
blocks weigh zero here and their header weight there; the key columns weighed
when not selected differ; the header's `number` is weighed only when selected;
Substrate `digest` weighs 32 here and 128 there. Items are equal when each engine
pages independently, so none of this is wrong, but until it is closed the parity
suite cannot assert `lastBlock` equality, which is the assertion gap 33 needed.

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

[INV-D7](07-invariants.md#inv-d7) says any integer width and *signedness*, so the
engine is wrong and the spec is right. It is **S4** rather than **S1** because
reaching it needs a writer that keeps `Int32` past 2³¹ instead of widening — the
`UInt32` arm, which any sane writer would pick, is already correct — and because
the chains served are between one and two orders of magnitude below that number.

The fix is not a one-line reinterpretation: a `[from, to]` range that straddles
2³¹ is two disjoint intervals in the signed domain, so the arm needs the straddle
case rather than a different scalar. Writing it as a scalar loop the way
`block_below_mask` does would work and would give up the SIMD kernel on a path
that runs over every row of every block-filtered scan.

*First test:* a chunk whose block numbers start at 2 200 000 000 stored as
`Int32`, queried over its own range, must return the same response as the same
chunk stored as `UInt64`. `physical_width_does_not_reach_the_answer` sweeps every
width already; what it cannot reach is a value that does not fit the width it is
stored at, which needs a writer that wraps rather than widens.

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

Three matrix rows overstate their evidence — [INV-D6](07-invariants.md#inv-d6)
rests on one `is_err()` case, [INV-P13](07-invariants.md#inv-p13) has no
behavioural test of prefix dispatch, [INV-Q14](07-invariants.md#inv-q14) is
green against the stale transcription of gap 35 — and
[ADR-4](decisions/ADR-4-closed-field-surface.md) and
[ADR-9](decisions/ADR-9-reject-undecidable-fork-checks.md) are still `Proposed`
while their MUST text is implemented. Whether to lower those three rows to **P**
and take the coverage ratchet of
[§8.12](08-conformance.md#812-merge-gates) below its floor is a decision, not a
finding, and is left to one.
