# 1. Data model

## 1.1 Chunks

A **chunk** holds all archived data for a contiguous, closed range of block
numbers `[firstBlock, lastBlock]`. Within a chunk, a **dataset** is materialised
as a set of **tables**; each table is a bag of **rows** with a fixed set of
**columns**.

A chunk is self-contained. Every fact needed to answer a query about the blocks
in `[firstBlock, lastBlock]` is present in that chunk. Nothing a query can ask
for requires reading a neighbouring chunk.

The engine is given one chunk at a time. It does not know what other chunks
exist, and it MUST NOT behave differently depending on which chunk it is handed
beyond the block numbers that chunk contains.

> Chunk *layout* — how many files, what compression, where statistics live — is
> outside this specification. The engine reads rows and column statistics; how
> those are stored is a storage concern.

## 1.2 Datasets, tables, columns

A **dataset** has a name (`evm`, `solana`, …) and an ordered list of tables. The
name is the value clients send as the query's `type` field.

Exactly one table in each dataset is the **block table**. It carries one row per
block and is the anchor for everything else. Every other table is an **item
table**: its rows are items belonging to a block.

A **table** is described by:

| Property | Meaning |
|---|---|
| `name` | The table's identity within the dataset. |
| `queryName` | The JSON key clients use to request items from it, and the key under which its items appear in the response. Defaults to `name`. |
| `fieldName` | The JSON key under `fields` used to select this table's output columns. Defaults to `name`. |
| `blockNumberColumn` | The column holding the block number. |
| `addressColumn` | For hierarchical tables, the column holding the tree path. Absent otherwise. |
| `itemOrderKeys` | Ordered columns that, together with the block number, totally order the table's rows within a block. |
| `columns` | An ordered map of column name → column description. Order is significant; it fixes output field order. |
| `filters` | The closed set of filters clients may apply. See §1.6. |
| `relations` | Named links to other tables (or to itself). See §1.7. |
| `virtualFields` | Output-only fields synthesised from several columns. See §1.8. |
| `fieldGroups` | Polymorphic output shape, for tables whose rows have variants. See §1.9. |

A **column** is described by:

| Property | Meaning |
|---|---|
| `type` | The *logical* type. See §1.4. |
| `encoding` | How values are rendered in the response. See §1.5. Defaults to the type's natural rendering. |
| `stats` | Whether the storage layer keeps min/max statistics for this column. Advisory: an optimisation hint only. |
| `dictionary` | Whether the column is dictionary-encoded in storage. Advisory. |
| `weight` | How much this column contributes to the response weight budget. See §5.4. |
| `system` | If true, the column exists to serve filters, joins, ordering or weights, and is never emitted. |

Ordering of `columns` is normative: it determines the order fields appear in
output objects ([INV-O6](07-invariants.md#inv-o6)). Ordering of `tables` is
normative: it determines the order item arrays appear in a block object
([INV-O4](07-invariants.md#inv-o4)).

## 1.3 Keys

Three distinct notions of "key" appear in this system, and conflating them
causes real bugs.

**Item key.** For an item table, `[blockNumberColumn] ++ itemOrderKeys` (with
`addressColumn` appended when present) uniquely identifies a row within a chunk
([INV-D4](07-invariants.md#inv-d4)). This is the row's identity: it is what
deduplication compares, and what output ordering sorts by. For the block table,
the item key is `[blockNumberColumn]` alone.

**Relation key.** The pair of column lists a relation joins on. Relation keys
are unrelated to item keys except for one hard constraint: the first column on
each side MUST be that side's block number column
([INV-D5](07-invariants.md#inv-d5)). This is what confines relations to a single
block, and hence what makes chunks independently evaluable.

**Storage sort key.** The order rows physically sit in. It is chosen to make
filtering fast, so it usually leads with high-selectivity filter columns rather
than the block number — an EVM `logs` table might be sorted by
`(topic0, address, blockNumber, logIndex)`.

The storage sort key is **not** part of this specification's semantics. An engine
MAY use it to prune work. It MUST NOT let it affect results
([INV-D8](07-invariants.md#inv-d8)).

## 1.4 Logical column types

```
uint8 | uint16 | uint32 | uint64
int16 | int32  | int64
float64
boolean
string
timestampSecond | timestampMillisecond
decimal128
fixedBinary(N)
list<T>            where T is any of the above
struct{ field: T, ... }
list<struct{...}>
```

### Physical width is not a promise

A declared type is an upper bound on the *values* a column holds, not an
assertion about how they are stored. An archive writer is free to narrow integer
columns to the smallest width that fits the chunk — a `uint64` block number
column will normally be stored as a 32-bit integer, and a `uint32` item index as
a 16-bit one. Different chunks of the same dataset may differ. Signed and
unsigned physical types may both appear for the same logical column.

An engine therefore MUST accept any integer physical width for any declared
integer column, and MUST produce identical results regardless
([INV-D7](07-invariants.md#inv-d7)). This applies to element types inside lists
as well.

A filter value that does not fit the physical width MUST evaluate to *false*, not
error, not panic, and not wrap ([INV-P14](07-invariants.md#inv-p14)). Asking for
block number 2⁴⁰ in a chunk whose block numbers are stored as 32-bit integers is
a well-formed query that matches nothing.

### Nullability

Every column is nullable unless the catalog says otherwise. A null value is
distinct from every non-null value, including for filters
([INV-P7](07-invariants.md#inv-p7)) and for join keys
([INV-R5](07-invariants.md#inv-r5)).

### Absent columns

A chunk written before a column existed will not contain it. Three cases, and
they must be distinguished:

1. The column is **not selected and not filtered on** — the query is unaffected.
2. The column is **selected for output** — the engine MUST fail with a missing-column error ([INV-E3](07-invariants.md#inv-e3)). Silently emitting `null` would tell the client the data is absent when in fact the chunk predates the field.
3. The column is **filtered on** — the engine MUST fail with the same error ([INV-X3](07-invariants.md#inv-x3)). Treating an absent column as unconstrained turns a selective query into a full table scan and returns rows the client did not ask for. This is the single most dangerous silent failure in the system.

## 1.5 Encodings

An encoding maps a stored value to its JSON rendering. The catalog assigns one
per column.

| Encoding | Rendering | Notes |
|---|---|---|
| `hexBytes` | `"0x"` followed by lowercase hex | Filters on such columns compare case-insensitively ([INV-P8](07-invariants.md#inv-p8)). |
| `base58` | base58 string | Filters compare exactly. |
| `utf8` | JSON string | Filters compare exactly. |
| `decimalString` | quoted decimal, e.g. `"1000000000000000000"` | For values that exceed IEEE-754 exact integer range. |
| `hexNumber` | quoted, zero-padded to the declared type's width, e.g. `"0x0640"` for a `uint16` | Distinct from `hexBytes`, which is variable-length. |
| `jsonVerbatim` | the stored bytes, spliced into the response uncopied | The stored value is already valid JSON. Empty string renders as `null`. |
| `timestampSecond` | integer seconds | |
| `timestampMillisecond` | integer milliseconds | |
| `number` | JSON number | The default for integers, floats, booleans. |
| *(chain-specific)* | e.g. Solana's transaction version: `-1` → `"legacy"`, else the number | Declared in the catalog. |

A float that is NaN or infinite renders as `null`
([INV-O9](07-invariants.md#inv-o9)).

`jsonVerbatim` splices bytes that the engine did not produce into a document it
did. An engine MUST either validate those bytes or guarantee by construction
that the archive writer only ever stored valid JSON there. A malformed value
must not be able to corrupt the response framing.

## 1.6 Filters

`filters` is a **closed allowlist**. A table declares which of its columns —
and which of its *special filters* — clients may filter on. A filter key that is
not declared is an error ([INV-Q6](07-invariants.md#inv-q6)), even if a column of
that name exists ([INV-P15](07-invariants.md#inv-p15)).

This matters. A table's columns include `system` columns holding blooms,
discriminators, size counters and denormalised extraction results. Letting a
client filter on those exposes internals, produces nonsense results, and makes
the catalog's column list part of the public API.

Filter kinds:

| Kind | Client supplies | Semantics |
|---|---|---|
| `inList` | array of values | Row matches if the column value is in the array. §4.2 |
| `equals` | a scalar | Row matches if the column value equals it. |
| `rangeGte` / `rangeLte` | a number | Inclusive bound on a named column. |
| `gteConst` | a boolean | When `true`, constrains a named column to be ≥ a catalog-fixed constant. Used for "is non-zero" flags over minimal-form hex. |
| `bloom` | array of values | Probabilistic membership against a bloom column. §4.2.5 |
| `listContainsAny` | array of values | The column is itself a list; row matches if the lists intersect. |
| `discriminator` | array of hex prefixes | Dispatches to per-length prefix columns. §4.2.6 |
| `columnAlias` | anything the target accepts | A filter key that reads a differently-named column. |

A filter may name a column different from the filter key (`columnAlias`), and
two filter keys may target the same column.

## 1.7 Relations

A **relation** is a named, directed link from a table to a table (possibly
itself). Requesting it in an item request pulls the related rows into the
response.

| Kind | Definition |
|---|---|
| `join` | Rows of the target whose `rightKey` equals some source row's `leftKey`. |
| `children` | Rows of the target, in the same key group, whose address strictly extends a source row's address. |
| `parents` | Rows of the target, in the same key group, whose address is a strict prefix of a source row's address. |

`children` and `parents` require an `addressColumn` on both sides
([INV-D6](07-invariants.md#inv-d6)). The **key group** is the relation key: rows
are compared only within the same group. When source and target are the same
column of the same table, the prefix relation is strict; when they are different
columns (a call tree and the events attached to it), equal-depth addresses also
match ([INV-R8](07-invariants.md#inv-r8)).

Relations widen the result. They never filter it
([INV-R4](07-invariants.md#inv-r4)). They resolve exactly one hop
([INV-R2](07-invariants.md#inv-r2)).

## 1.8 Virtual fields

A **virtual field** is an output-only field assembled from several columns. Only
one kind exists:

**`roll`** — given an ordered list of columns, produce a JSON array. Scalar
columns contribute one element each. The array **stops at the first null**. If
the last column is a list, its elements are spread into the array rather than
nested ([INV-O10](07-invariants.md#inv-o10)).

This exists so that a physically-flattened structure — `topic0, topic1, topic2,
topic3` or `a0 … a15, restAccounts` — can be presented as the array it logically
is. Virtual fields are selectable in `fields` exactly like columns. They are not
filterable.

## 1.9 Field groups

Some tables hold rows of several shapes. An EVM trace is a *create*, a *call*, a
*suicide* or a *reward*, and each carries different columns. Bitcoin-style
inputs and outputs are similar.

A table with `fieldGroups` declares:

- a **tag column** whose value names the variant,
- **base fields** emitted flat for every variant,
- for each tag value, a set of **groups**, each a named object of field mappings.

A field mapping is `(column, outputField, requestKey?)`. It says: read `column`,
emit it as `outputField`, and let the client select it under `requestKey`
(defaulting to `outputField`). Distinct request keys may map the same column to
different output positions.

The group name `_` means "flatten into the enclosing object" rather than nesting.

For a row, the engine emits the base fields, then the groups belonging to the
row's tag. A tag value the catalog does not know MUST NOT crash the engine; the
row emits its base fields and no variant groups
([INV-O11](07-invariants.md#inv-o11)).

## 1.10 Catalog well-formedness

The catalog is data, and data can be wrong. An engine MUST validate it before
serving any query — at build time if the catalog is compiled in, at load time
otherwise. A catalog that fails validation MUST NOT be used to answer queries.

Every check below is a static property of the catalog alone, requiring no chunk.
They are collected as [INV-D1](07-invariants.md#inv-d1) … [INV-D10](07-invariants.md#inv-d10).

- Exactly one block table; its `itemOrderKeys` is empty.
- `blockNumberColumn`, `addressColumn`, every `itemOrderKeys` entry, every
  `sortKey` entry: exists in `columns`.
- Every `weight` that names a size column: that column exists.
- Every relation: its target table exists; `leftKey` and `rightKey` have equal
  length; both begin with the respective block number column; `children` and
  `parents` relations have `addressColumn` on both sides.
- Every filter: its target column exists and is not `system`-only in a way that
  contradicts the filter kind. Every `discriminator` length→column mapping names
  a real column. Every `gteConst` names a real column.
- Every virtual field: all rolled columns exist; a spread list column, if
  present, is last.
- Every field group: the tag column exists; every mapped column exists.
- Every alias: its target table exists; its implicit-predicate columns exist;
  its filter aliases target real columns; its relations are valid.
- `queryName` and `fieldName` are unique across tables *and* aliases.

The last check is easy to overlook and easy to violate. A duplicate `queryName`
makes a client's request ambiguous, and the ambiguity is resolved by iteration
order — which is to say, arbitrarily.
