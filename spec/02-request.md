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
**item request arrays** named after a table's `queryName`. There is no third
kind. A key that is neither reserved nor a known `queryName` is an error
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

If the chunk cannot see the parent block — because `fromBlock` is the chunk's
first block, and the parent lives elsewhere — the check is skipped and no error
is raised.

## 2.2 Item request arrays

Each key naming a table's `queryName` carries an array of **item requests**. The
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

A `queryName` may name an **alias** rather than a table. An alias targets a real
table and adds:

- **implicit predicates** — filters always applied, which the client cannot see or override;
- **filter aliases** — filter keys that read differently-named columns;
- its own **relations**.

An alias is otherwise an ordinary item request array. Substrate's `evmLogs` is
an alias over `events` with the implicit predicate `name = "EVM.Log"` and filter
aliases mapping `address` and `topic0…3` onto extraction columns.

Items requested through an alias and items requested through its underlying table
in the same query land in the same output array, deduplicated
([INV-R3](07-invariants.md#inv-r3)).

## 2.3 Field selection

```jsonc
"fields": {
  "log": { "address": true, "topics": true, "logIndex": false }
}
```

The keys of `fields` are table `fieldName`s. An unknown one is an error
([INV-Q8](07-invariants.md#inv-q8)). The value MUST be an object; each of its
keys names a selectable field of that table, and each value is a boolean. Only
`true` selects.

The selectable fields of a table are its non-`system` columns, its virtual
fields, and — for tables with field groups — its field mappings' request keys.
Names are camelCase. A key that is not a selectable field is an error
([INV-Q7](07-invariants.md#inv-q7)).

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
| Total item requests, summed across every table | ≤ 100 | [INV-Q5](07-invariants.md#inv-q5) |
| Values in one bloom filter | ≤ 10 | [INV-Q10](07-invariants.md#inv-q10) |
| Discriminator-family filters per item request | ≤ 1 | [INV-Q11](07-invariants.md#inv-q11) |
| Bytes in one discriminator value | ≤ 16 | [INV-Q12](07-invariants.md#inv-q12) |

The item-request cap is counted across *all* tables and aliases uniformly. A
per-table exemption would let a client aim a hundred requests at one table and a
hundred more at an alias of the same table.

An engine SHOULD additionally bound the total request size in bytes and the
number of values in a single `inList` ([INV-Q13](07-invariants.md#inv-q13)). One
hundred item requests, each with a filter list of a million addresses, is a
well-formed query by the rules above and a memory-amplification attack in
practice.

## 2.5 Filter value forms

| Filter kind | Accepted value | Rejected |
|---|---|---|
| `inList` | array of strings, or of numbers where the column is numeric | anything else |
| `equals` | string, number, or boolean matching the column's type | arrays |
| `rangeGte` / `rangeLte` | unsigned integer | strings, floats, negatives |
| `gteConst` | boolean | anything else |
| `bloom` | array of strings, ≤ 10 entries | anything else |
| `listContainsAny` | array matching the list's element type | anything else |
| `discriminator` | array of `0x`-prefixed even-length hex strings, each ≤ 16 bytes | anything else |

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
