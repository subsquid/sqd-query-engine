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

Two kinds of key appear at the top level: the **reserved keys** of §2.1, and
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
| `sets` | object | `{}` | Named value sets that filters refer to (§2.7). |

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

Every row of the block table carries its own parent's hash, so the first block
at or after `fromBlock` is the one that answers the question. The check therefore
works even when `fromBlock` is the chunk's first block. Where a dataset numbers
blocks with gaps, the block table also declares a parent-*number* column, and the
engine reads that instead of assuming `fromBlock - 1`.

On such a chain `fromBlock` can be a number the chain skipped. A client that
resumes at `lastBlock + 1` lands on one whenever the next slot was empty, so this
is the common case, not an edge. There is no row at `fromBlock` then, and the
block that answers is the next one: its parent is the block before the gap, which
is the block the client means.

The search starts at `fromBlock` and moves forward to that block, rather than
backward to where the parent might lie, so no window width can make the parent
unfindable: a dataset that skips a thousand numbers is answered by the same rule
as one that skips none. An engine also returns the blocks of the `P-FORK-WINDOW`
behind `fromBlock`, which is the evidence a client needs to find the fork point —
a dataset that skips numbers returns fewer pairs, and still answers.

The check is skipped, without error, in exactly one case: the chunk does not
hold the block that answers, because `fromBlock` lies outside the chunk. A chunk
that cannot see the block is not evidence of a fork.

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
| Canaries in one set | at least one, ≤ `P-MAX-SET-CANARIES` | [INV-Q17](07-invariants.md#inv-q17) |

The item-request cap is counted across *all* tables and aliases uniformly
([ADR-6](decisions/ADR-6-aliases-count-toward-the-request-cap.md)). A per-table
exemption would let a client aim a hundred requests at one table and a hundred
more at an alias of the same table.

An engine SHOULD additionally bound the whole request to `P-MAX-REQUEST-BYTES`,
a single `inList` to `P-MAX-IN-LIST` values, and the set references a request
makes to `P-MAX-SET-REFERENCES` ([INV-Q13](07-invariants.md#inv-q13)). One hundred item requests, each with a
filter list of a million addresses, is a well-formed query by the rules above and
a memory-amplification attack in practice.

A set (§2.7) counts toward `P-MAX-REQUEST-BYTES` like the rest of the request.
`P-MAX-IN-LIST` does not apply to it: a set is not a list, and the bytes that
carry it are its bound. What bounds the references is their number, since each
is a lookup for every row its table scans.

Every value here is resolved in [09-parameters.md](09-parameters.md).

## 2.5 Filter value forms

| Filter kind | Accepted value | Rejected |
|---|---|---|
| `inList` | array of strings, or of numbers where the column is numeric; on a `hexBytes`, `base58` or `utf8` column the array may also hold set references (§2.7) | anything else |
| `equals` | string, number, or boolean matching the column's type | arrays |
| `rangeGte` / `rangeLte` | unsigned integer | strings, floats, negatives |
| `gteConst` | boolean | anything else |
| `bloom` | array of strings, ≤ `P-MAX-BLOOM-VALUES` entries | anything else |
| `listContainsAny` | array matching the list's element type | anything else |
| `discriminator` | array of `0x`-prefixed even-length hex strings, each ≤ `P-MAX-DISCRIMINATOR-BYTES` | anything else |

A hex string is well-formed when it starts with `0x` or `0X` and has an even
number of hex digits. Only a `discriminator` value has to be: a malformed one is
an error. In every other filter a malformed value is answered as written. It
compares as text on a column stored as text, and matches nothing on a column
stored as bytes or integers ([INV-Q12](07-invariants.md#inv-q12)).

A value that is well-formed but cannot possibly match — an address longer than
the column ever holds, an integer beyond the column's physical width — is not an
error either. It matches nothing ([INV-P14](07-invariants.md#inv-p14)).

## 2.6 Validation order

All of §2.1–§2.5 and §2.7 MUST be checked before any chunk data is read, and an engine
MUST NOT emit partial output before a validation error
([INV-E2](07-invariants.md#inv-e2)). Validation is a pure function of the request
and the catalog.

Errors detectable only against a chunk — a missing column, a fork — are
execution errors and are covered in [06-errors.md](06-errors.md).

## 2.7 Sets

A **set** stands in for a list too long to send. The client builds a compact
structure over its values and sends that instead, and the engine keeps every row
whose value the structure may hold. The structure has false positives and no
false negatives, so the answer contains the one the same values would give as a
list, and the client removes the extra rows itself
([ADR-17](decisions/ADR-17-large-value-sets-travel-as-fuse8-with-canaries.md)).

Sets are declared once, under `sets`, and filters refer to them by name:

```jsonc
{
  "type": "evm",
  "sets": {
    "pairs": {
      "fuse8": "<base 64>",
      "canaries": ["0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc", "0x0d4a11d5…"]
    }
  },
  "fields": {
    "log": { "address": true },
    "transaction": { "from": true, "to": true }
  },
  "logs": [{ "address": [{ "set": "pairs" }], "topic0": ["0x1c41…"] }],
  "transactions": [{ "from": [{ "set": "pairs" }] }, { "to": [{ "set": "pairs" }] }]
}
```

A name is any string. It means nothing beyond tying references to their set, so
a client may make names up however it likes.

### The set object

| Key | Value |
|---|---|
| `fuse8` | The structure, as canonical base 64 with padding (RFC 4648). §4.2.11 says what its bytes are. |
| `canaries` | An array of strings: a few of the set's values, as the client holds them. |

A set carries exactly one structure key, and `fuse8` is the only one. The key
names the format, so a later format can arrive under a key of its own without
changing what `fuse8` means. A set with no structure key, with two, or with an
unknown one is `InvalidSet`. So is a structure that is not canonical base 64 or
whose header does not fit its body (§4.2.11), and so is a set that no filter
refers to, which is nearly always a reference the client meant to write and did
not ([INV-Q15](07-invariants.md#inv-q15)).

### References

A filter refers to a set with an object that holds one key, `{"set": "<name>"}`,
written as an element of the filter's list. A list may hold values and references
to several sets, and a row passes it when it equals one of the values or one of
the sets passes it (§4.2.11). So a client adds a few values to a set, or joins two
sets, without building one again:

```jsonc
"logs": [{ "address": ["0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f", { "set": "pairs" }, { "set": "pools" }] }]
```

A reference written alone is a list of one, as a value written alone is. An
object in a list that is not a reference is `InvalidFilterValue`.

- The name MUST be declared under `sets`. Otherwise `UnknownSet`
  ([INV-Q15](07-invariants.md#inv-q15)).
- The filter MUST be an `inList` filter on a column whose encoding is
  `hexBytes`, `base58` or `utf8` (§1.5). A reference in any other list is
  `InvalidFilterValue` ([INV-Q16](07-invariants.md#inv-q16)). The structure is
  looked up with the text the response renders, and only these encodings render a
  value as the text a client holds.
- The table's selected fields MUST render the filtered column, directly or
  through the virtual field that rolls it (§2.3): `address` for a set on
  `logs.address`, `topics` for one on `logs.topic0`. Otherwise
  `SetFieldNotSelected` ([INV-Q18](07-invariants.md#inv-q18)). A client can
  remove only the extra rows it can see.
- A request SHOULD make at most `P-MAX-SET-REFERENCES` references, summed across
  every list of every item request, a name counted once per list. Otherwise
  `RequestTooLarge` ([INV-Q13](07-invariants.md#inv-q13)).

Any number of filters may refer to one set, and its structure still travels once.
That is why sets are declared apart from the filters that use them:
`from ∈ S ∨ to ∈ S` is two item requests ([INV-P5](07-invariants.md#inv-p5)), and
so are the two owners of a Solana token balance.

A set has at least one value, because each canary is one of its values. A client
whose set is empty sends `[]` instead, which matches nothing
([INV-P3](07-invariants.md#inv-p3)).

### Canaries

A structure is opaque. The engine cannot tell one built from the right bytes from
one built from checksummed addresses, from addresses without their `0x`, from raw
bytes, or with another hash. Each of those matches almost nothing, and an answer
that is almost empty looks like a quiet stretch of chain: a wrong answer that
looks right ([§6.1](06-errors.md#61-principles)). Canaries make the mistake an
error.

A set carries at least one canary and at most `P-MAX-SET-CANARIES`, otherwise it
is `InvalidSet`. For every column that refers to the set, the engine folds each
canary as that column folds a list value ([INV-P8](07-invariants.md#inv-p8)),
checks that the result is a value the column can render — well-formed hex (§2.5)
on a `hexBytes` column, a non-empty run of base58 digits on a `base58` one — and
looks it up in the structure. A canary that fails either check is
`SetCanaryMismatch` ([INV-Q17](07-invariants.md#inv-q17)).

A wrong structure passes a canary only by a false positive, so each canary a
client adds multiplies the chance that a wrong structure gets through by the
format's false-positive rate. A client SHOULD draw its canaries at random from
the set each time it builds one: a mistake that spoils only some values is caught
only by a canary among them. No canary catches a set on the wrong field, such as
a set of addresses on a topic filter, because the canaries are well-formed values
there too.
