# 4. Evaluation

This chapter defines *which rows* a query selects. [05-response.md](05-response.md)
defines how they are rendered.

Evaluation is specified as set construction. It says nothing about the order
operations happen in, whether they happen in parallel, or what may be skipped.
An engine may do anything it likes so long as the resulting sets are these sets
([INV-P16](07-invariants.md#inv-p16)).

## 4.1 Notation

For a chunk `C` and a query `Q`:

- `Rows(T)` — the rows of table `T` in `C` whose block number lies in
  `[Q.fromBlock, Q.toBlock] ∩ [C.firstBlock, C.lastBlock]`. Call that
  intersection the **covered range**. Every row set below is implicitly confined
  to it ([INV-B1](07-invariants.md#inv-b1)).
- `Requests(T)` — the item requests aimed at `T`, including those arriving
  through aliases of `T`.
- `Match(s)` for an item request `s` on table `T` — the rows of `Rows(T)`
  satisfying `s`'s filters, per §4.2.
- `Direct(T) = ⋃ { Match(s) : s ∈ Requests(T) }`
- `Related(T)` — rows of `T` pulled in by relations, per §4.3.
- `Items(T) = Direct(T) ∪ Related(T)`

Union is set union: a row selected by two paths appears once
([INV-R3](07-invariants.md#inv-r3)). Row identity is the item key (§1.3).

## 4.2 Filters

### 4.2.1 Composition

An item request's filters combine with **AND**
([INV-P4](07-invariants.md#inv-p4)):

```
Match(s) = { r ∈ Rows(T) : ∀ f ∈ s.filters . satisfies(r, f) }
```

Item requests on the same table combine with **OR**
([INV-P5](07-invariants.md#inv-p5)). There is no way to express OR *within* one
item request except through a list, and no way to express NOT at all.

An item request with no filters matches every row of the table in the covered
range ([INV-P6](07-invariants.md#inv-p6)).

An item request whose filter set is unsatisfiable — see `[]` below — matches
nothing. It contributes nothing to `Direct(T)`, and its relations contribute
nothing to any `Related(·)`, because they have no source rows
([INV-R1](07-invariants.md#inv-r1)).

### 4.2.2 The three states of a filter

This is the part clients get wrong, so it is worth stating plainly. For a filter
on column `c`:

| Written as | Meaning | |
|---|---|---|
| *key absent* | No constraint. Every row passes. | [INV-P1](07-invariants.md#inv-p1) |
| `"c": ["a", "b"]` | `r[c] ∈ {a, b}`. | [INV-P2](07-invariants.md#inv-p2) |
| `"c": []` | **Nothing passes.** The item request matches no rows. | [INV-P3](07-invariants.md#inv-p3) |

`[]` is not "no constraint" ([INV-P3](07-invariants.md#inv-p3)). The natural
reading of `r[c] ∈ ∅` is *false*, and that is what it means. An engine that
treats an empty list as an absent filter turns a client's "match none of these
addresses" into "match every transaction in the chunk".

Because filters conjoin, one empty list makes the whole item request
unsatisfiable, whatever else it contains.

### 4.2.3 Null

A null column value satisfies no value filter
([INV-P7](07-invariants.md#inv-p7)). `r[c] ∈ L` is false when `r[c]` is null, for
every `L` including the empty list. There is no filter syntax that selects nulls.

### 4.2.4 Case folding

Values of columns declared `hexBytes` or `hexUnprefixed` compare
**case-insensitively**: the engine
folds both the filter values and the stored values to lowercase before comparing
([INV-P8](07-invariants.md#inv-p8)). Everything else compares byte-exactly.

This is a property of the *column*, not of the filter. `evm.statediffs.key` holds
hex-looking strings but is not declared `hexBytes`, so `key` filters are
exact. `evm.traces.type` holds identifiers, and so is exact.

Folding applies to scalar `equals` filters as well as to `inList`. A rule that
folds lists but not scalars produces a query language where `{"to": ["0xAB…"]}`
and `{"to": "0xAB…"}` mean different things.

### 4.2.5 Bloom filters

A bloom filter tests membership of any of up to ten values against a fixed-size
bloom column. It is an **over-approximation**: it has no false negatives and MAY
have false positives ([INV-P9](07-invariants.md#inv-p9)).

Rows that do not mention the requested account therefore MAY appear in the
response. This is part of the contract, not a defect. Clients that need exactness
must re-check the returned rows.

An engine MUST NOT "fix" this by post-filtering, because the exact account set of
a row is not always available in the projected columns, and a client that has
learned to re-check would then silently depend on engine-version behaviour.

The bloom construction — width, hash count, hash function, how a value is
serialised into it — is part of the catalog and MUST match the archive writer's
exactly. A mismatch produces false *negatives*, which no client can detect.

### 4.2.6 Discriminators

A discriminator filter carries a list of hex byte-strings of possibly different
lengths. The table declares a column per length: `d1` for 1-byte prefixes, `d2`
for 2-byte, up to `d16`.

Group the values by byte length. A row matches if **for some length `L`**, the
first `L` bytes of the row's instruction data equal one of the `L`-byte values —
which is exactly `r[dL] ∈ values_L` ([INV-P13](07-invariants.md#inv-p13)).

So the filter is an OR across length groups, and within a group an `inList` on
that group's column.

Two consequences:

- A **zero-length** value (`"0x"`) is a prefix of everything. Its presence makes
  the discriminator filter match all rows. It is not an error.
- An **empty list** (`[]`) matches nothing, per [INV-P3](07-invariants.md#inv-p3).
  It is not an error either.

Because a discriminator filter expands to a disjunction, and disjunction does not
distribute into an item request's conjunction for free, an engine that compiles
item requests into flat conjunctions must split the item request into one per
length group, replicating the other filters into each. That is an implementation
concern; the semantics are just the ones above.

Null propagation matters here. If a row's `d2` is null because its data is one
byte long, `r[d2] ∈ {…}` is false, and the disjunct for `d1` must still be able
to make the row match. The OR must therefore treat `false OR null` as `false` and
`true OR null` as `true` — Kleene disjunction, not an AND-of-negations.

### 4.2.7 Ranges and `gteConst`

`rangeGte` and `rangeLte` are inclusive ([INV-P10](07-invariants.md#inv-p10)).
`firstNonce: 5, lastNonce: 5` selects nonce 5.

`gteConst` is a boolean flag that, when `true`, constrains a column to be `≥` a
constant fixed by the catalog. Every current use has the constant `"0x1"` over a
column of minimal-form hex quantities. On minimal-form hex, lexicographic order
agrees with numeric order for the purpose of "is it zero", because the only
value below `"0x1"` is `"0x0"`. The comparison MUST be lexicographic on the
stored string ([INV-P11](07-invariants.md#inv-p11)); reinterpreting it as a
numeric comparison would require parsing arbitrary-precision integers to no
purpose.

`false` disables the filter, exactly like omitting it.

### 4.2.8 `listContainsAny`

The column holds a list per row. The row matches if that list and the filter's
value list share at least one element ([INV-P12](07-invariants.md#inv-p12)). A
null list matches nothing. An empty stored list matches nothing. An empty filter
list matches nothing.

### 4.2.9 Type divergence

A filter value is compared against the column's *physical* representation, which
may be narrower or of different signedness than the declared type (§1.4).

- A value that does not fit the physical type matches nothing, for that
  comparison. It MUST NOT error, panic, or wrap
  ([INV-P14](07-invariants.md#inv-p14)).
- A value that does fit compares numerically, not bitwise. A stored `-1` in a
  signed column is not equal to `0xFFFFFFFF`.

The same holds for the block range: `fromBlock: 2^40` against a chunk whose block
numbers are stored as 32-bit integers selects nothing and is not an error.

### 4.2.10 Pushdown is invisible

Engines prune: they skip row groups whose statistics exclude a filter, use
dictionaries, evaluate cheap filters before expensive ones, and stop reading
early once the weight budget is spent.

None of this may change the answer ([INV-P16](07-invariants.md#inv-p16)). In
particular, statistics are a *hint*. A column may lack statistics, have stale
ones, or have them under a different physical type. An engine that prunes on a
stat it cannot interpret MUST decline to prune, not guess.

The one visible consequence of pruning is where truncation lands (§5.5), and
that is governed by weight, not by scan order.

## 4.3 Relations

### 4.3.1 Source scoping

A relation is requested *inside an item request*. Its sources are the rows that
item request matched — not the whole table, and not the union of every item
request on the table ([INV-R1](07-invariants.md#inv-r1)):

```
Related(U) = ⋃ { resolve(rel, Match(s)) : s ∈ Requests(T),
                                          rel ∈ s.relations,
                                          rel.target = U }
```

This is the difference between

```jsonc
"logs": [
  { "address": ["0xUSDC"], "transaction": true },
  { "topic0": ["0xTransfer"] }
]
```

pulling in the transactions of USDC logs, and pulling in the transactions of
every Transfer log on the chain.

An engine MAY notice that every item request on a table asks for the same
relation, and if so evaluate it once against `Direct(T)`. That is an
optimisation, and it is sound precisely because `⋃ᵢ resolve(rel, Match(sᵢ)) =
resolve(rel, ⋃ᵢ Match(sᵢ))` — relations distribute over union.

### 4.3.2 One hop

Relations resolve exactly once ([INV-R2](07-invariants.md#inv-r2)). Rows in
`Related(U)` do **not** have `U`'s relations applied to them, even when the same
query requests those relations elsewhere.

`{"logs": [{"transaction": true}]}` returns logs and their transactions. It does
not return those transactions' traces, even though `transactions.traces` exists.
To get traces the client asks for them.

Without this rule, `transaction → logs → transaction → …` would not terminate,
and every relation graph with a cycle would need an occurs-check whose result no
client could predict.

### 4.3.3 Widening

For any query `Q` and any relation flag `f` in it,

```
Items(U)  under  Q          ⊇   Items(U)  under  Q with f removed
```

for every table `U` ([INV-R4](07-invariants.md#inv-r4)). Relations add rows.
They never remove them, never filter them, and never constrain the table they
originate from.

### 4.3.4 `join`

```
resolve(join(U, leftKey, rightKey), S)
  = { u ∈ Rows(U) : ∃ s ∈ S . s[leftKey] = u[rightKey] }
```

A semi-join: it returns rows of `U`, never a product. If two source rows share a
key, the target rows appear once.

Comparison is component-wise on the key tuple. **A null in any key column makes
the row match nothing**, on either side ([INV-R5](07-invariants.md#inv-r5)).
Null is not equal to null.

Because `leftKey[0]` and `rightKey[0]` are the block number columns
([INV-D5](07-invariants.md#inv-d5)), a join can only ever relate rows of the same
block. This is what lets an engine restrict the target scan to the source rows'
block range, and what lets a chunk be evaluated alone.

### 4.3.5 `children` and `parents`

Both are defined on tables with an address column, whose values are lists of
integers naming a path in a tree — a trace's position in the call tree, an
instruction's position in the CPI tree, a call's position in an extrinsic.

Rows are compared only within a **key group**, given by the relation key
(typically `[blockNumber, transactionIndex]`).

Let `a ≺ b` mean "`a` is a strict prefix of `b`": `len(a) < len(b)` and
`a = b[0 … len(a))`. Let `a ⪯ b` mean `a ≺ b` or `a = b`.

```
children(S) = { u : ∃ s ∈ S . group(u) = group(s) ∧ addr(s) ≺ addr(u) }
parents(S)  = { u : ∃ s ∈ S . group(u) = group(s) ∧ addr(u) ≺ addr(s) }
```

`parents` returns the entire ancestor chain, not just the immediate parent
([INV-R7](07-invariants.md#inv-r7)). `children` returns all descendants, not just
immediate ones ([INV-R6](07-invariants.md#inv-r6)).

Neither includes the source row itself. When the source row is also in
`Direct(T)` — which it is, for a self-relation — it appears in the output anyway,
by union.

### 4.3.6 Cross-table hierarchies

When the source and target address columns are *different* — Substrate's
`calls.address` and `events.callAddress` — the strict prefix relaxes to `⪯`
([INV-R8](07-invariants.md#inv-r8)):

```
foreignChildren(S) = { u : group(u) = group(s) ∧ addr_src(s) ⪯ addr_tgt(u) }
foreignParents(S)  = { u : group(u) = group(s) ∧ addr_tgt(u) ⪯ addr_src(s) }
```

An event whose `callAddress` equals a call's `address` is *attached to* that
call, not a sibling of it. Requiring a strict extension would drop exactly the
events the client wanted.

The distinction is structural — same column or different column — not a flag a
catalog author sets by hand.

### 4.3.7 Relations and the covered range

Relation targets are subject to the covered range like every other row
([INV-B1](07-invariants.md#inv-b1)), and to the weight budget like every other
row ([INV-R10](07-invariants.md#inv-r10)). A relation cannot pull a row from
outside `[fromBlock, toBlock]`, and cannot pull a row into a block that the
budget excluded.

Relation-supplied rows are rendered with the same field selection as directly
selected rows of that table ([INV-R9](07-invariants.md#inv-r9)). `fields` is
per-table; the path a row took to reach the response is not part of its identity.

## 4.4 Chunk independence

Everything above depends on the chunk only through `Rows(T)`, and `Rows(T)` is
determined by the covered range. Relations never leave a block. Therefore:

> Partition `[fromBlock, toBlock]` into contiguous sub-ranges. Evaluate the query
> once per sub-range. The union of the item sets equals the item set of the whole
> range.

This is [INV-B8](07-invariants.md#inv-b8), and it is the property that makes the
whole architecture work — chunks can be stored separately, served by different
machines, and merged by concatenation.

It is worth naming what would break it. A relation whose key omitted the block
number could match a source row in one chunk to a target row in another. Every
key in [03-catalog.md](03-catalog.md) starts with the block number. A catalog
validator MUST enforce it ([INV-D5](07-invariants.md#inv-d5)) rather than trust
the author.
