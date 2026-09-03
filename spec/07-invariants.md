# 7. Invariants

Every normative rule in this specification, numbered. Each entry states the rule,
why it exists, and how to falsify it. If an invariant cannot be falsified by a
test, it is not an invariant — it is an opinion, and it does not belong here.

Families:

| Prefix | Domain | Checked against |
|---|---|---|
| `INV-D` | Data model & catalog | the catalog alone |
| `INV-Q` | Request validation | request + catalog |
| `INV-P` | Filter algebra | a chunk |
| `INV-R` | Relations | a chunk |
| `INV-B` | Block selection, weight, pagination | a chunk |
| `INV-O` | Response format | a chunk |
| `INV-E` | Errors | request or chunk |
| `INV-X` | Cross-cutting | the whole system |

---

## D — Data model and catalog

These are static. A conforming engine validates them before serving any query,
and a conformance suite checks them without touching a chunk.

### INV-D1
**Every catalog reference resolves.** Every column named by a
`blockNumberColumn`, `addressColumn`, `itemOrderKeys` entry, `sortKey` entry,
`weight` source, filter target, `gteConst` target, discriminator length mapping,
virtual-field roll member, field-group tag or field mapping, alias implicit
predicate, or alias filter alias MUST exist in the table it is declared on.

*Why:* an unresolved reference fails at query time, on a query nobody ran during testing, in production.
*Test:* walk the catalog; assert every name resolves. No chunk needed.

### INV-D2
**Every alias is well-formed.** An alias's target table exists; its implicit
predicate columns, filter-alias targets, and relations are all valid against that
table.

*Why:* an alias with a bad target is a latent crash on the first query that uses it.
*Test:* static walk.

### INV-D3
**Exactly one block table.** Each dataset has precisely one table whose item key
is `[blockNumberColumn]` and whose `itemOrderKeys` is empty. It is the first
table in catalog order.

*Why:* the response is a sequence of blocks; there must be exactly one thing a block is.
*Test:* static.

### INV-D4
**Item keys are unique.** Within a chunk, `[blockNumberColumn] ++ itemOrderKeys
++ [addressColumn]?` uniquely identifies a row of an item table.

*Why:* deduplication and output ordering both use it. If it is not unique, neither is well-defined, and the response order depends on scan order.
*Test:* for each fixture chunk and each table, assert the projected key columns have no duplicate tuple.

### INV-D5
**Relation locality.** For every relation, `leftKey[0]` is the source table's
block number column and `rightKey[0]` is the target's. `leftKey` and `rightKey`
have equal length.

*Why:* this is the sole reason a chunk can be evaluated alone. A relation that could cross a block boundary would make [INV-B8](#inv-b8) false and the whole distributed architecture unsound.
*Test:* static. Additionally, a chunk-level test: for every relation, assert no matched `(source, target)` pair has differing block numbers.

### INV-D6
**Hierarchies need addresses.** Every `children` or `parents` relation has an
`addressColumn` declared on both source and target tables.

*Test:* static.

### INV-D7
**Physical width tolerance.** A declared integer type bounds the values, not the
storage. The engine MUST accept any integer physical width and signedness for any
declared integer column — including element types inside lists — and produce
identical results.

*Why:* archive writers narrow integers per chunk. A `uint64` block number is normally stored in 32 bits, and a `uint32` item index in 16. Chunks written by different generations of writer differ.
*Test:* generate the same logical chunk at several physical widths; assert byte-identical responses.

### INV-D8
**Storage layout independence.** No query result may depend on the storage sort
key, row-group boundaries, page boundaries, compression, dictionary encoding, or
the presence of column statistics.

*Why:* these are tuning knobs. Changing one must never change an answer.
*Test:* rewrite a fixture chunk with a different sort key and row-group size; assert byte-identical responses.

### INV-D9
**System columns are invisible.** A `system` column is never emitted, is never
selectable, is never filterable except through the filter that declares it, and
contributes zero weight.

*Test:* for every system column, assert it cannot be named in `fields` directly,
through a virtual field or through a field-group request key; assert response
weight is unchanged by its presence.

### INV-D10
**Names are unique.** Within a dataset, `queryName` is unique across tables and
aliases; `fieldName` is unique across tables.

*Why:* a duplicate makes a client's request ambiguous, resolved by iteration order — arbitrarily.
*Test:* static.

---

## Q — Request validation

All checked before any data is read ([INV-E2](#inv-e2)).

### INV-Q1
**`type` selects the dataset.** `type` is required and MUST equal a dataset's
name. Otherwise `UnknownDataset`.

### INV-Q2
**No unknown top-level keys.** A top-level key is one of the six reserved keys
(§2.1) or a `queryName`. Anything else is `UnknownTable`.

### INV-Q3
**`fromBlock ≤ toBlock`.** Otherwise `InvalidBlockRange`.

### INV-Q4
**Block bounds are well-formed.** `fromBlock` and `toBlock`, when present, are
unsigned 64-bit integers. A string, float, negative, or out-of-range value is
`InvalidBlockNumber`. It is never coerced.

*Why:* coercion answers a different question than the one asked, and says nothing about it.
*Test:* `{"fromBlock": "abc"}`, `{"toBlock": -1}`, `{"fromBlock": 1.5}`, `{"fromBlock": 1e30}` each error.

### INV-Q5
**At most `P-MAX-ITEM-REQUESTS` item requests**, summed across every table and
alias. Otherwise `TooManyItemRequests`.

*Why:* each item request is an independent scan. The bound must be global, or an alias becomes a way to buy another hundred.
*Test:* 100 requests split across tables passes; 101 fails; 100 aimed at an alias of an already-requested table fails when the total exceeds 100.

### INV-Q6
**Declared filters and relations only.** Every key of an item request names a
declared filter or a declared relation of that table (or alias). Anything else is
`UnknownFilter`. The value of a `queryName` key MUST be an array.

### INV-Q7
**Unknown field names are errors.** A key inside `fields.X` that names no
selectable field of `X` is `UnknownField`. It MUST NOT be silently dropped.

*Why:* a client that misspells `logIndx` gets a 200 and a response missing the field, and will look for the bug everywhere except in its own request.

### INV-Q8
**Unknown field groups are errors.** A key of `fields` naming no table's
`fieldName` is `UnknownFieldGroup`. `fields.X` MUST be an object.

### INV-Q9
**Defaults.** `fromBlock` = 0. `toBlock` = unbounded. `includeAllBlocks` = false.
`fields` = `{}`. `parentBlockHash` = absent. Every item request array = `[]`.
Inside an item request, every filter absent and every relation flag `false`.

A default applies to an *absent* key only. A key that is present with the wrong
type is an error, never its default: reading `{"fields": []}` as "no selection"
answers 200 with every projection the client asked for missing, which is
indistinguishable from a dataset that has none of those columns. This is the same
rule INV-Q4 states for the block bounds, and it holds for every optional key.

*Test:* `{"type": "evm"}` is valid and returns headers for the chunk's boundary blocks with empty header objects. `{"fields": []}`, `{"fields": false}` and `{"includeAllBlocks": 1}` each error.

### INV-Q10
**Bloom filters take at most `P-MAX-BLOOM-VALUES` values.** Otherwise
`TooManyBloomValues`.

### INV-Q11
**At most `P-MAX-DISCRIMINATOR-FILTERS` discriminator-family filter per item
request.** For Solana
instructions, at most one of `discriminator`, `d1`, `d2`, `d4`, `d8`. Otherwise
`ConflictingFilters`.

### INV-Q12
**Hex values are well-formed.** `0x` or `0X` prefix, even number of hex digits,
and nothing but hex digits between them. A discriminator value is at most
`P-MAX-DISCRIMINATOR-BYTES`. Otherwise `InvalidHex` or `DiscriminatorTooLong`.

Which values these are follows the *column*, exactly as case folding does
([INV-P8](#inv-p8)): every filter value on a `hexBytes` column, whether the
column is stored as text or as bytes. A `hexBytes` column stores lowercase `0x…`
values by §1.5, so a filter value that is not one of those can never equal a
stored value — and answering that with an empty `200` tells the client nothing.

*Note:* a well-formed value that cannot match anything is not an error. See [INV-P14](#inv-p14).

### INV-Q13
**Requests are bounded in size.** An engine SHOULD reject a request exceeding
`P-MAX-REQUEST-BYTES`, or containing an `inList` longer than `P-MAX-IN-LIST`,
with `RequestTooLarge`.

*Why:* 100 item requests, each with a million-address filter list, is well-formed under every rule above and a memory-amplification attack in practice.

### INV-Q14
**The field surface is closed.** A table's selectable fields are the ones the
catalog declares, enumerated per dataset in
[03-catalog.md](03-catalog.md). A key naming an undeclared field is
`UnknownField`, even when a column of that name exists. Deriving the surface from
the column list instead — "every non-`system` column is selectable" — is not
conforming, because it exposes columns the catalog carries for filtering,
grouping, joining and rolling.

*Why:* the same reason [INV-P15](#inv-p15) closes the filter surface. An open output surface makes the physical column layout part of the wire contract, and every column an archive writer adds becomes a field clients may pin on.
*Test:* for every dataset and table, assert `fields` accepts exactly the §3 list. In particular `{"fields":{"log":{"blockNumber":true}}}` and `{"fields":{"log":{"topic0":true}}}` error, though `block_number` and `topic0` are real columns.

---

## P — Filter algebra

### INV-P1
**An absent filter constrains nothing.** Every row passes.

### INV-P2
**A non-empty list is OR-membership.** `r[c] ∈ values`.

### INV-P3
**An empty list matches nothing.** `"c": []` makes the entire item request
unsatisfiable, whatever its other filters. It is not an error, and it is not the
same as omitting the key.

*Why:* `r[c] ∈ ∅` is false. An engine that reads `[]` as "unconstrained" turns "match none of these addresses" into "match every row in the chunk" — the single most destructive misreading available.
*Test:* `{"logs":[{"address":[]}]}` returns no logs. `{"logs":[{"address":[], "topic0":["0x…"]}]}` returns no logs.

### INV-P4
**Filters within one item request conjoin.** All must hold.

### INV-P5
**Item requests on one table disjoin.** `Direct(T) = ⋃ Match(sᵢ)`, a set union:
a row matched by two item requests appears once.

*Test:* `Q(a) ∪ Q(b) = Q(a, b)` for any two item requests on the same table.

### INV-P6
**An empty item request selects the whole table**, within the covered range.

### INV-P7
**Null satisfies no filter.** For every filter kind and every value list
including the empty one.

### INV-P8
**Case folding follows the column, not the filter.** Values of `hexBytes`
columns compare case-insensitively, for both `inList` and scalar `equals`
filters. All other columns compare byte-exactly.

Folding is one-sided: stored `hexBytes` values are lowercase by §1.5, so only
the filter value is folded. A chunk storing upper-case hex is malformed, and no
engine is required to match it.

*Why:* clients send checksummed addresses. Folding lists but not scalars makes `{"to": ["0xAB…"]}` and `{"to": "0xAB…"}` mean different things.
*Test:* upper-casing every hex filter value in a query leaves the response byte-identical. Upper-casing a value of a non-hex column (`statediffs.key`, `traces.type`) changes it.

### INV-P9
**A bloom filter over-approximates.** No false negatives; false positives
permitted. Rows not mentioning the requested account MAY appear.

An engine MUST NOT post-filter them away, and its bloom construction (width, hash
count, hash function, value serialisation) MUST match the archive writer's
exactly.

*Why:* a mismatched construction produces false *negatives*, which no client can detect.
*Test:* for a known chunk, assert every row truly mentioning the account is present. Assert the engine's bloom of a known value equals the writer's stored bloom bits.

### INV-P10
**Ranges are inclusive.** `firstNonce: 5, lastNonce: 5` selects nonce 5.

### INV-P11
**`gteConst` compares lexicographically** on the stored string, against the
catalog's constant.

*Why:* the columns hold minimal-form hex quantities. Parsing them as arbitrary-precision integers to answer "is it zero" buys nothing.
*Test:* `"0x0"` fails `≥ "0x1"`; `"0x1"`, `"0x10"`, `"0xff"` pass.

### INV-P12
**`listContainsAny` is set intersection.** The row matches iff its list shares at
least one element with the filter's. A null list, an empty stored list, and an
empty filter list each match nothing.

### INV-P13
**Discriminators dispatch by prefix length.** Group filter values by byte length.
A row matches iff for some length `L`, `r[dL] ∈ values_L`. The disjunction over
lengths is Kleene: `true ∨ null = true`, `false ∨ null = false`.

A zero-length value (`"0x"`) matches every row. An empty list matches none
([INV-P3](#inv-p3)). Neither is an error.

*Why:* a row whose data is one byte long has a null `d2`. Without Kleene disjunction, mixing `d1` and `d2` prefixes in one filter would drop it.
*Test:* `{"discriminator": ["0xab", "0xabcd"]}` returns rows matching either prefix. `{"discriminator": ["0x"]}` returns every instruction.

### INV-P14
**Values outside the physical range match nothing.** They do not error, panic, or
wrap. This holds for filter values and for `fromBlock`/`toBlock` alike.

*Test:* on a chunk whose block numbers are stored in 32 bits, `{"fromBlock": 1099511627776}` returns an empty response, not an error. A `uint16` `d2` filter given a value above 65535 matches nothing.

### INV-P15
**The filter surface is closed.** A table declares which filters exist. A key
naming an undeclared column is `UnknownFilter`, even when a column of that name
exists.

*Why:* tables carry system columns holding blooms, size counters and denormalised extractions. Filtering on them exposes internals and makes the column list part of the public API.
*Test:* `{"logs":[{"dataSize":[100]}]}` errors, though `data_size` is a real column.

### INV-P16
**Pushdown is invisible.** Row-group pruning, page pruning, dictionary
evaluation, filter reordering, and early termination MUST NOT change the row set.
An engine that cannot interpret a statistic MUST decline to prune rather than
guess.

*Test:* run every fixture query with pruning disabled; assert byte-identical responses.

---

## R — Relations

### INV-R1
**A relation's sources are its own item request's matches.** Not the table, not
the union of all item requests on the table.

*Test:* `{"logs":[{"address":["A"],"transaction":true},{"address":["B"]}]}` returns the transactions of A-logs only.

### INV-R2
**Relations resolve one hop.** Rows pulled in by a relation do not have their own
relations applied.

*Why:* the relation graph has cycles. Transitive expansion would not terminate, and an occurs-check would make results unpredictable.
*Test:* `{"logs":[{"transaction":true}]}` returns no traces, though `transactions.traces` exists.

### INV-R3
**Rows are deduplicated.** `Items(T) = Direct(T) ∪ Related(T)` is a set under the
item key. A row reachable by several paths appears once.

*Test:* `{"logs":[{"transaction":true}],"traces":[{"transaction":true}]}` yields each transaction once.

### INV-R4
**Relations widen.** For any query `Q` and relation flag `f`, and every table `U`:
`Items(U) under Q ⊇ Items(U) under Q∖f`.

*Why:* the most useful metamorphic property in the suite. Adding a relation must never remove a row from any table, including the table the relation originates from.
*Test:* enumerate relation flags; for each, assert the row sets grow monotonically.

### INV-R5
**`join` is a semi-join, and null keys never match.** It returns rows of the
target, never a product. Comparison is component-wise on the key tuple. A null in
any key column, on either side, makes the row match nothing.

### INV-R6
**`children` returns all strict descendants** within the key group: rows whose
address strictly extends a source address. Not just immediate children. The
source row is not included by this relation.

### INV-R7
**`parents` returns the full ancestor chain** within the key group: every row
whose address is a strict prefix of a source address. Not just the immediate
parent.

### INV-R8
**Cross-table hierarchies include equal depth.** When the source and target
address columns differ, the prefix relation relaxes from `≺` to `⪯`.

*Why:* an event whose `callAddress` equals a call's `address` is attached to that call. Requiring a strict extension drops exactly the rows the client wanted.
*Test:* Substrate `calls.events` returns the events directly on the matched call.

### INV-R9
**Relation rows share the table's field selection.** A row's rendering does not
depend on the path it took into the response.

### INV-R10
**Relation rows obey the range and the budget.** A relation cannot pull a row
from outside the covered range, nor into a block the weight budget excluded.

### INV-R11
**Relations are idempotent across item requests.** Requesting the same relation
from several item requests on a table yields the same target row set as
requesting it once against the union of their matches.

*Why:* this is what licenses the "all item requests want it, so evaluate it once against the whole table" optimisation.

---

## B — Blocks, weight, pagination

### INV-B1
**Rows are confined to the covered range**, `[fromBlock, toBlock] ∩
[chunk.first, chunk.last]`. Direct and relation-supplied rows alike.

### INV-B2
**`includeAllBlocks: true` emits a header for every block in the covered range**,
subject only to the weight budget.

### INV-B3
**`includeAllBlocks: false` emits a header for every block with at least one
item, plus the first and last block of the covered range.**

*Why:* without the last one, a client cannot distinguish "the chunk ended here" from "no matching items beyond here", and cannot advance its cursor across a long empty stretch without re-asking.
*Test:* a query matching nothing over a non-empty chunk returns exactly two header-only blocks (or one, if the range is a single block).

### INV-B4
**Blocks are atomic.** A block is emitted whole or not at all. A block is never
split across responses.

### INV-B5
**Block weight** = the header row's weight + the weight of every item in the
block, after deduplication.

### INV-B6
**Selection is a weighted prefix.** Sort candidate blocks ascending, accumulate
weight, keep the longest prefix whose cumulative weight does not exceed
`P-WEIGHT-BUDGET`.
Always keep at least one block, however heavy.

*Test:* a chunk whose first block alone exceeds the budget still returns that block, in full.

### INV-B7
**`lastBlock` is a sound resume cursor.** It is the highest block number emitted.
Nothing at or below it was omitted. Resuming with `fromBlock = lastBlock + 1`
yields no gap and no duplicate.

*Test:* page a chunk end to end with a budget forced small; assert the concatenated item sequence equals the single-shot result.

### INV-B8
**Partition composability.** For any split point `m` inside the covered range,
the items of `Q[from…m]` concatenated with those of `Q[m+1…to]` equal the items
of `Q[from…to]`.

Headers may differ: each sub-query contributes its own boundary blocks
([INV-B3](#inv-b3)), so up to two extra header-only blocks may appear. That is
the only permitted difference.

*Why:* this is the property that lets chunks live on different machines and be merged by concatenation. It follows from [INV-D5](#inv-d5) and nothing else.
*Test:* for every fixture query, split the range at every block boundary; assert item-level equality.

### INV-B9
**Weight is a deterministic model, not a measurement.** It need not equal the
response's byte length. It MUST be a pure function of the selected projection and
the chunk's values.

### INV-B10
**Weight is computed over the emitted projection.** Selecting fewer fields makes
blocks lighter and lets more of them fit. System columns contribute zero.

*Test:* a narrow projection returns at least as many blocks as a wide one over the same range.

---

## O — Response format

### INV-O1
**NDJSON framing.** One JSON object per block, each followed by `\n`, including
the last. An empty result is zero bytes — not `[]`, not a newline. Responses for
adjacent ranges concatenate into a valid document.

### INV-O2
**Block object shape.** `header` is always present, `{}` when no header field was
selected. Item arrays are keyed by `queryName` and are omitted entirely when
empty, never rendered as `[]`.

### INV-O3
**Blocks ascend** by block number.

### INV-O4
**Item arrays follow catalog table order.**

### INV-O5
**Items ascend by item key** (`itemOrderKeys`, then `addressColumn`).

### INV-O6
**Fields follow catalog column order**, with virtual fields after real columns.

### INV-O7
**A field appears iff selected.** No implicit fields — not `blockNumber`, not
`transactionIndex`. A selected null renders as `null`, never omitted. Each key
appears at most once in an object.

*Test:* `{"fields":{}}` yields items serialised as `{}`.

### INV-O8
**Keys are camelCase**, including keys nested inside structs.

### INV-O9
**Encodings.** `decimalString` → quoted decimal. `hexNumber` → quoted, zero-padded
to the column's declared width. `hexBytes` → `0x` + lowercase hex, variable
length. `jsonVerbatim` → spliced as-is; empty renders `null`. Timestamps →
integers in the *declared* unit. NaN and ±∞ → `null`.

*Why:* a `uint64` value above 2⁵³ emitted as a JSON number rounds in every JavaScript client, silently. `hexNumber` padding is load-bearing: `"0x0640"` and `"0x640"` are different discriminators.
*Test:* round-trip each encoding against fixtures; assert a `uint16` `hexNumber` of 1600 renders `"0x0640"`.

### INV-O10
**Rolls truncate at the first null** and spread a trailing list column rather than
nesting it.

*Test:* a log with two topics set renders `topics` of length 2, not 4.

### INV-O11
**Field groups dispatch on the tag.** Base fields flat; groups named `_` flattened,
others nested. A group with at least one selected field is emitted even if all its
values are null. **A tag value absent from the catalog emits base fields only, and
is never an error or a crash.**

*Why:* archives outlive catalogs. A new trace type must not take the engine down.

### INV-O12
**Byte determinism.** The same chunk and query produce byte-identical output.

### INV-O13
**Execution independence.** Output does not depend on thread count, completion
order, row-group or page boundaries, compression, physical row order, physical
integer widths, or which columns carry statistics or dictionaries.

*Test:* run each fixture query at 1, 2 and 16 threads; assert byte equality.

### INV-O14
**Alternate encodings carry the same response.** A columnar (Arrow) rendering has
the same rows, the same blocks and the same values as the JSON one. It MAY differ
in nesting and field naming. It is never a different query result.

---

## E — Errors

### INV-E1
**No input causes a crash.** Not a malformed request, not an absurd one, not a
chunk with unexpected physical types, a missing column, or a corrupt value. The
engine returns an error.

*Test:* fuzz the request against every dataset; fuzz chunk column types; assert no panic, ever.

### INV-E2
**Validation precedes execution.** Every error in §6.2 is raised before any chunk
data is read. No output precedes any error.

### INV-E3
**A selected column absent from the chunk is an error** (`ColumnNotFound`), not a
`null`.

*Why:* `null` tells the client "this transaction had no `sighash`". The truth is "this chunk predates `sighash`". Those call for different actions.

### INV-E4
**A missing table is an error** (`TableNotFound`).

### INV-E5
**Fork detection.** When `parentBlockHash` is supplied and the chunk knows the
block preceding `fromBlock`, its hash MUST be compared. A mismatch is
`UnexpectedBaseBlock`, carrying the expected hash and recent
`(blockNumber, hash)` pairs. Accepting the field and ignoring it silently serves
data from a chain the client did not ask about.

Each block row carries its own parent's hash, so the row at `fromBlock` answers
the question, and only that row does; the window around it exists to carry the
recent pairs a client needs to find the fork point, not to answer in its place.
The check is skipped without error when the chunk does not hold that row —
whether because the window caught nothing at all, or because the chunk ends
below `fromBlock`. A chunk that cannot see the block is not evidence about it.

A dataset whose block table declares no parent-hash column cannot answer the
question at all. Such a dataset MUST reject `parentBlockHash`
(`UnsupportedRequestField`) rather than accept it and skip the check — a skipped
check is indistinguishable, at the client, from a chain that did not reorganise.

The same holds one level down. A *chunk* that holds a block of the window but
carries neither the block table nor the parent-hash column cannot answer either,
and MUST fail rather than return data. Only a chunk the window does not reach is
the skipped case: it is silent because it was never asked.

*Test:* supply a wrong `parentBlockHash` for the chunk's first block and assert `UnexpectedBaseBlock`; supply any `parentBlockHash` to a dataset with no parent-hash column and assert `UnsupportedRequestField`; supply one to a chunk inside the window whose block table lacks the column and assert an error; supply one with `fromBlock` below the chunk, and one with `fromBlock` above it, and assert the query succeeds in both.

### INV-E6
**Error kinds are stable and machine-readable.** Clients switch on the kind; only
humans read the message.

### INV-E7
**An uncomparable key type is an error** (`UnsupportedKeyType`), never "matches
nothing".

*Why:* "matches nothing" silently returns a response missing every related row.

---

## X — Cross-cutting

### INV-X1
**Schema-agnosticism.** No behaviour is specific to a chain. Adding a dataset is
a catalog edit. If an engine needs new code to serve a new chain, the catalog
format is missing something, and that is a spec bug.

*Test:* a synthetic catalog describing an invented chain, with a synthetic chunk, is served correctly with no code change.

### INV-X2
**Additive schema evolution.** Adding a nullable column to a chunk MUST NOT change
the answer to any query that neither selects nor filters on it.

*Why:* archives are extended routinely. If extension changed old answers, no archive could ever be extended.

### INV-X3
**A filtered column absent from the chunk is an error**, never "unconstrained".

*Why:* the single most dangerous silent failure available. Treating an absent column as matching everything turns a selective query into a full table scan and returns the entire chunk to a client that asked for four rows. It is indistinguishable, at the client, from a correct answer.
*Test:* delete `sighash` from a fixture chunk; assert `{"transactions":[{"sighash":["0xa9059cbb"]}]}` errors rather than returning every transaction.

This covers a relation's join key too: an unevaluable key does not narrow the
relation scan, so every row of the target table in range is attached to every
source row. The catalog is checked at load for the shapes that cannot narrow —
an empty key, sides of unequal length, and a key not led by the block number,
which joins across blocks within a chunk.
