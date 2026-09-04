# 2. The request

A query is a single JSON object.

```jsonc
{
  "type": "evm",                    // required: dataset name
  "fromBlock": 18000000,            // default 0
  "toBlock": 18000100,              // default: unbounded
  "parentBlockHash": "0x…",         // optional: chain-continuity check
  "includeAllBlocks": false,        // default false

  "fields": {                       // default {}
    "block": { "number": true, "timestamp": true },
    "log":   { "address": true, "topics": true, "data": true }
  },

  "logs": [                         // an item request array
    { "address": ["0xa0b8…"], "topic0": ["0xddf2…"], "transaction": true }
  ],
  "transactions": []
}
```

Three kinds of key appear at the top level: the six **reserved keys** above, and
**item request arrays** named after a table's request name (§1.2). There is no
third kind. A key that is neither reserved nor a known request name is an error
([INV-Q2](07-invariants.md#inv-q2)).

## 2.1 Reserved keys

| Key | Type | Default | Meaning |
|---|---|---|---|
| `type` | string | — *(required)* | Must equal the dataset's name. |
| `fromBlock` | unsigned integer | `0` | Inclusive lower bound. |
| `toBlock` | unsigned integer | unbounded | Inclusive upper bound. |
| `parentBlockHash` | string | absent | Hash the client believes precedes `fromBlock`. |
| `includeAllBlocks` | boolean | `false` | Emit headers for blocks with no items. |
| `fields` | object | `{}` | Output field selection, per table. |

Every key but `type` is optional; the defaults above are normative
([INV-Q9](07-invariants.md#inv-q9)).

`fromBlock` and `toBlock` MUST be non-negative integers within the unsigned
64-bit range. A value that is a string, a float, negative, or out of range is an
error ([INV-Q4](07-invariants.md#inv-q4)). Coercing a malformed bound to `0` or
to "unbounded" silently answers a different question than the one asked.

`toBlock` MUST be greater than or equal to `fromBlock`
([INV-Q3](07-invariants.md#inv-q3)).

### parentBlockHash

When present, it asserts: *"the block immediately before `fromBlock` has this
hash."* The engine checks the assertion against the chunk before returning any
data. If the chunk knows the parent block and its hash differs, the engine MUST
fail with a fork error carrying enough recent block hashes for the client to
find the common ancestor ([INV-E5](07-invariants.md#inv-e5)).

This is not decoration. Clients use it to detect that the chain reorganised
under them between one page and the next. An engine that accepts the field and
ignores it silently serves data from a chain the client did not ask about.

Every row of the block table carries its own parent's hash, so the row *at*
`fromBlock` is the one that answers the question. The check therefore works even
when `fromBlock` is the chunk's first block. Where a dataset numbers blocks with
gaps, the block table also declares a parent-*number* column, and the engine
matches on that instead of assuming `fromBlock - 1`.

The search is anchored at `fromBlock` rather than aimed at where the parent might
lie, so no window width can make the parent unfindable: a dataset that skips a
thousand numbers is answered by the same row as one that skips none. An engine
also returns the `P-FORK-WINDOW` blocks ending at `fromBlock`, which is the
evidence a client needs to find the fork point — a dataset that skips numbers
returns fewer pairs, and still answers.

The check is skipped, without error, in exactly one case: the chunk holds no
block in that range, because `fromBlock` lies outside the chunk. A chunk that
cannot see the block is not evidence of a fork.

If the dataset's block table declares **no parent-hash column at all**, the
engine MUST reject the request (`UnsupportedRequestField`;
[ADR-9](decisions/ADR-9-reject-undecidable-fork-checks.md)). It MUST NOT accept
the field and skip the check. A client that asks for fork detection and is
silently given none cannot tell the difference from a chain that did not
reorganise ([INV-E5](07-invariants.md#inv-e5)).

## 2.2 Item request arrays

Each key naming a table's request name carries an array of **item requests**. The
value MUST be an array ([INV-Q6](07-invariants.md#inv-q6)).

An item request is an object whose keys are either **filters** or **relation
flags** for that table:

```jsonc
{
  "address": ["0xa0b8…"],     // filter:  inList
  "topic0":  ["0xddf2…"],     // filter:  inList
  "transaction": true,        // relation flag
  "transactionLogs": false    // relation flag, disabled — same as omitting it
}
```

- A key that names a declared filter carries a filter value.
- A key that names a declared relation carries a boolean. `true` requests it;
  `false` is identical to omitting the key.
- Any other key is an error ([INV-Q6](07-invariants.md#inv-q6)).

An **empty item request** `{}` imposes no constraint and selects the whole table
within the block range ([INV-P6](07-invariants.md#inv-p6)).

An **empty array** `[]` for a table selects nothing from it, and is identical to
omitting the key.

Multiple item requests on the same table are independent and their results are
unioned ([INV-P5](07-invariants.md#inv-p5)).

### Aliases

A request name may name an **alias** rather than a table. An alias targets a
real table and carries a request surface of its own:

- **implicit filters** — always applied, which the client cannot see or override;
- **filters** — its own closed list, typically `columnAlias` keys onto the
  columns the implicit filter makes meaningful;
- its own **relations**.

An alias is otherwise an ordinary item request array. Substrate's `evmLogs` is
an alias over `events` with the implicit filter `name = "EVM.Log"` and
`columnAlias` filters mapping `address` and `topic0…3` onto extraction columns.

Items requested through an alias and items requested through its underlying table
in the same query land in the same output array, deduplicated
([INV-R3](07-invariants.md#inv-r3)).

## 2.3 Field selection

```jsonc
"fields": {
  "log": { "address": true, "topics": true, "logIndex": false }
}
```

The keys of `fields` are tables' output names (§1.2). An unknown one is an error
([INV-Q8](07-invariants.md#inv-q8)). The value MUST be an object; each of its
keys names a selectable field of that table, and each value is a boolean. Only
`true` selects.

The selectable fields of a table are the ones the catalog declares for it,
enumerated per dataset in [03-catalog.md](03-catalog.md). Names are camelCase. A
key that is not a selectable field is an error
([INV-Q7](07-invariants.md#inv-q7)).

The output surface is **closed**, for the same reason the filter surface is
(§1.6) — [ADR-4](decisions/ADR-4-closed-field-surface.md). A catalog may *derive* it — non-`system` columns, plus virtual fields,
plus variant fields — but the derivation is a convenience, not the
definition. Where it would admit a name §3 does not list, §3 wins
([INV-Q7](07-invariants.md#inv-q7), [INV-Q14](07-invariants.md#inv-q14)).

Two classes of column are declared but not selectable, and both are load-bearing:
the block number column of an item table, which is read for grouping, joining and
ordering and is already carried by the block header; and a filter column that the
catalog rolls into a virtual field — `logs.topic0…3` behind `topics`,
`instructions.a0…a15` behind `accounts`. Exposing either makes the physical
column layout part of the wire contract.

Silently ignoring an unrecognised field name is the wrong choice. A client that
misspells `logIndx` gets a `200` and a response missing the field, and will look
for the bug everywhere except in its own request.

Field selection is **per table**, not per item request. All item requests on a
table share one projection.

Field selection is **purely additive**. There are no implicit output fields. A
field appears in an item's output object if and only if it was selected
([INV-O7](07-invariants.md#inv-o7)). If no field of a table is selected, its
items serialise as empty objects. The engine reads whatever columns it needs for
filtering, joining, ordering and weighing, but reading is not emitting.

## 2.4 Limits

An engine MUST enforce, before reading any data:

| Limit | Value | Invariant |
|---|---|---|
| Total item requests, summed across every table | ≤ `P-MAX-ITEM-REQUESTS` | [INV-Q5](07-invariants.md#inv-q5) |
| Values in one bloom filter | ≤ `P-MAX-BLOOM-VALUES` | [INV-Q10](07-invariants.md#inv-q10) |
| Discriminator-family filters per item request | ≤ `P-MAX-DISCRIMINATOR-FILTERS` | [INV-Q11](07-invariants.md#inv-q11) |
| Bytes in one discriminator value | ≤ `P-MAX-DISCRIMINATOR-BYTES` | [INV-Q12](07-invariants.md#inv-q12) |

The item-request cap is counted across *all* tables and aliases uniformly
([ADR-6](decisions/ADR-6-aliases-count-toward-the-request-cap.md)). A per-table
exemption would let a client aim a hundred requests at one table and a hundred
more at an alias of the same table.

An engine SHOULD additionally bound the whole request to `P-MAX-REQUEST-BYTES`
and a single `inList` to `P-MAX-IN-LIST` values
([INV-Q13](07-invariants.md#inv-q13)). One hundred item requests, each with a
filter list of a million addresses, is a well-formed query by the rules above and
a memory-amplification attack in practice.

Every value here is resolved in [09-parameters.md](09-parameters.md).

## 2.5 Filter value forms

| Filter kind | Accepted value | Rejected |
|---|---|---|
| `inList` | array of strings, or of numbers where the column is numeric | anything else |
| `equals` | string, number, or boolean matching the column's type | arrays |
| `rangeGte` / `rangeLte` | unsigned integer | strings, floats, negatives |
| `gteConst` | boolean | anything else |
| `bloom` | array of strings, ≤ `P-MAX-BLOOM-VALUES` entries | anything else |
| `listContainsAny` | array matching the list's element type | anything else |
| `discriminator` | array of `0x`-prefixed even-length hex strings, each ≤ `P-MAX-DISCRIMINATOR-BYTES` | anything else |

Hex strings MUST start with `0x` or `0X` and have an even number of hex digits.
An odd-length or non-hex value is an error, not a value that matches nothing
([INV-Q12](07-invariants.md#inv-q12)).

A value that is well-formed but cannot possibly match — an address longer than
the column ever holds, an integer beyond the column's physical width — is **not**
an error. It matches nothing ([INV-P14](07-invariants.md#inv-p14)). The
distinction is between *"you wrote nonsense"* and *"nothing here is what you
asked for"*.

## 2.6 Validation order

All of §2.1–§2.5 MUST be checked before any chunk data is read, and an engine
MUST NOT emit partial output before a validation error
([INV-E2](07-invariants.md#inv-e2)). Validation is a pure function of the request
and the catalog.

Errors detectable only against a chunk — a missing column, a fork — are
execution errors and are covered in [06-errors.md](06-errors.md).
