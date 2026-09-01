# 5. The response

## 5.1 Framing

The response is **newline-delimited JSON**: one JSON object per block, each
followed by a newline, including the last
([INV-O1](07-invariants.md#inv-o1)).

```
{"header":{"number":18000000},"logs":[…]}\n
{"header":{"number":18000001},"logs":[…]}\n
```

An empty result is a **zero-byte** response. Not `[]`, not a newline
([ADR-1](decisions/ADR-1-ndjson-is-the-only-json-framing.md)).

Two properties follow, and both are relied on:

- Responses for adjacent block ranges **concatenate**. A client fetching a range
  in pages can append the bytes and hold a valid document.
- A block object never spans a line, so a reader can find block boundaries
  without parsing.

An engine MAY additionally offer a columnar encoding (Arrow IPC or similar). Such
an encoding MUST carry the same rows, the same blocks, and the same values as the
JSON form; it MAY differ in nesting and in field naming
([INV-O14](07-invariants.md#inv-o14)). It is an alternative rendering of the same
response, never a different query result
([ADR-11](decisions/ADR-11-columnar-encoding-is-a-rendering.md)).

## 5.2 Block objects

```jsonc
{
  "header": { … },              // always present
  "transactions": [ … ],        // omitted when empty
  "logs":         [ … ]         // omitted when empty
}
```

- `header` is present in every block object, even when no header field was
  selected — in which case it is `{}` ([INV-O2](07-invariants.md#inv-o2)).
- An item array is emitted only if it has at least one item. Empty arrays are
  omitted, not rendered as `[]`.
- Item arrays are keyed by the table's `queryName`.

Order:

| Level | Order | Invariant |
|---|---|---|
| Blocks | ascending block number | [INV-O3](07-invariants.md#inv-o3) |
| Item arrays within a block | catalog table order | [INV-O4](07-invariants.md#inv-o4) |
| Items within an array | item key, ascending | [INV-O5](07-invariants.md#inv-o5) |
| Fields within an item | catalog column order, virtual fields after real ones ([ADR-5](decisions/ADR-5-catalog-order-for-output-fields.md)) | [INV-O6](07-invariants.md#inv-o6) |

None of these orders is the order the data sits in storage, and none may depend
on it ([INV-D8](07-invariants.md#inv-d8)). Item keys are unique within a block
([INV-D4](07-invariants.md#inv-d4)), so item order is total, and the whole
response is determined by the chunk and the query alone
([INV-O12](07-invariants.md#inv-o12)) — down to the byte.

## 5.3 Item objects

A field appears if and only if it was selected
([INV-O7](07-invariants.md#inv-o7)). A selected field whose value is null is
emitted as `null`; it is not omitted. There are no implicit fields: not
`blockNumber`, not `transactionIndex`, not `logIndex`. The engine reads those
columns to group, order, join and weigh rows, but reading is not emitting.

Keys are camelCase, including keys inside structs
([INV-O8](07-invariants.md#inv-o8)). Each key appears at most once in an object.

### Values

Rendered per the column's encoding (§1.5). Restating the ones that surprise
people ([INV-O9](07-invariants.md#inv-o9)):

- `decimalString` columns are **quoted**: `"1000000000000000000"`. Emitting them
  as JSON numbers would silently round in every JavaScript client above 2⁵³.
- `hexNumber` columns are quoted and **zero-padded to the column's width**:
  a `uint16` holding 1600 is `"0x0640"`, not `"0x640"`.
- `hexBytes` columns are `0x` + lowercase hex, variable length, exactly the bytes
  stored.
- `jsonVerbatim` columns are spliced in as-is. An empty stored value renders as
  `null`.
- A float that is NaN or ±∞ renders as `null`.
- Timestamps render as integers in their declared unit. A column declared
  `timestampMillisecond` renders in milliseconds. Rescaling by unit of the
  *physical* type rather than the *declared* encoding is a bug that only shows up
  on chains with sub-second blocks.

### Virtual fields

A roll renders as a JSON array, truncated at the first null, with a trailing list
column spread rather than nested ([INV-O10](07-invariants.md#inv-o10)).

A log with `topic0` and `topic1` set and `topic2`, `topic3` null renders
`"topics": ["0x…", "0x…"]` — length two, not four with trailing nulls.

### Field groups

The engine reads the tag column, emits the base fields flat, then emits the
groups belonging to that tag. A group named `_` is flattened into the enclosing
object; any other group name nests under that key.

A group with at least one selected field is emitted even if every one of its
values is null. A tag value not present in the catalog emits the base fields and
no groups, and MUST NOT be an error or a crash
([INV-O11](07-invariants.md#inv-o11)) — archives outlive catalogs.

## 5.4 Weight

Every response is bounded by a **weight budget** rather than a row count, because
rows differ in size by four orders of magnitude
([ADR-2](decisions/ADR-2-weight-budget-not-row-count.md)).

The weight of a row is the sum of the weights of the columns *actually emitted*
for it ([INV-B10](07-invariants.md#inv-b10)):

| Column's catalog `weight` | Contribution per row |
|---|---|
| absent | `P-DEFAULT-COLUMN-WEIGHT` |
| an integer `n` | `n` |
| the name of a size column | the value of that size column in this row |
| — and the column is `system` | 0 ([INV-D9](07-invariants.md#inv-d9)) |

Weight is a *model* of response size, not a measurement of it. It exists to be
cheap: an engine must be able to compute a block's weight from narrow columns
before decoding the wide ones. Its absolute accuracy does not matter. Its
determinism does ([INV-B9](07-invariants.md#inv-b9)).

Two properties of the definition are load-bearing:

- Weight depends on the **projection**. Selecting fewer fields makes blocks
  lighter and lets more of them fit. A weight computed over all columns would
  truncate a narrow query as harshly as a wide one.
- Weight is computed **after deduplication**. A transaction pulled in by both a
  `logs.transaction` and a `traces.transaction` relation is one row and is
  counted once.

The weight of a block is the header row's weight plus the weight of every item
in it ([INV-B5](07-invariants.md#inv-b5)).

## 5.5 Which blocks are returned

Start from the covered range: `[fromBlock, toBlock] ∩ [chunk.first, chunk.last]`.

**Candidate blocks:**

- If `includeAllBlocks` is true — every block in the covered range
  ([INV-B2](07-invariants.md#inv-b2)).
- Otherwise — every block holding at least one selected item, **plus the first
  and last block of the covered range** ([INV-B3](07-invariants.md#inv-b3)).

The two boundary blocks are not decoration. Without the last one, a client cannot
distinguish "the chunk ended here" from "no matching items after this point", and
cannot advance its cursor past a long stretch of empty blocks without re-asking.
They are emitted with header weight only, and are subject to truncation like any
other block.

**Selection.** Sort the candidates ascending. Accumulate weight. Keep the longest
prefix whose cumulative weight does not exceed **`P-WEIGHT-BUDGET`**. Always keep
at least one block, however heavy
([INV-B6](07-invariants.md#inv-b6)).

Blocks are atomic. A block is emitted whole or not at all
([INV-B4](07-invariants.md#inv-b4)). A single block that exceeds the budget is
still emitted, in full, alone.

**`lastBlock`.** Alongside the response the engine reports the highest block
number it emitted. Nothing was omitted at or below it, within the query's
constraints. A client resumes with `fromBlock = lastBlock + 1`
([INV-B7](07-invariants.md#inv-b7)).

If the covered range is empty the response is empty and `lastBlock` is absent.

### Paging is exact

For any query `Q` and any split point `m` inside the covered range, let `Q₁` be
`Q` with `toBlock = m` and `Q₂` be `Q` with `fromBlock = m + 1`. Then the items
of `Q₁` concatenated with the items of `Q₂` equal the items of `Q`
([INV-B8](07-invariants.md#inv-b8)).

The *headers* may differ: `Q₁` and `Q₂` each contribute their own boundary blocks
(§5.5), so the concatenation may carry up to two header-only blocks that `Q` did
not. This is the only permitted difference, and clients tolerate it because a
header-only block carries no items.

Item-level exactness is what matters and is not negotiable. A client that pages
through a range MUST see each item exactly once.

## 5.6 Determinism

Given the same chunk and the same query, an engine MUST produce byte-identical
output ([INV-O12](07-invariants.md#inv-o12)).

In particular the output MUST NOT depend on
([INV-O13](07-invariants.md#inv-o13)):

- the number of threads, or the order work completed in;
- row-group boundaries, page boundaries, or compression settings;
- the physical order of rows within the chunk;
- the physical integer width chosen for any column;
- which columns happen to carry statistics or dictionaries;
- the presence of columns the query neither selects nor filters on
  ([INV-X2](07-invariants.md#inv-x2)).

The last one deserves emphasis. Adding a nullable column to an archive is a
routine act. If it changed the answer to a query that never mentioned it, no
archive could ever be extended.
