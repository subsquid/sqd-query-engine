# 6. Errors

## 6.1 Principles

**No input causes a crash.** Not a malformed request, not a request that is
well-formed but absurd, not a chunk whose columns have unexpected physical types,
not a chunk missing a column, not a corrupt value. An engine MUST return an error
([INV-E1](07-invariants.md#inv-e1)).

The engine sits behind an untrusted network boundary. A panic is a denial of
service, and a panic in a worker serving many queries is a denial of service for
all of them.

**Validation precedes execution.** Every error in §6.2 is detectable from the
request and the catalog alone, and MUST be raised before any chunk data is read.
No partial output may precede any error
([INV-E2](07-invariants.md#inv-e2)).

**Silence is the worst failure.** Ranked from best to worst, an engine's
responses to a bad situation are: a typed error; a crash; a wrong answer that
looks wrong; a wrong answer that looks right. Every rule below exists to keep the
system out of the last category.

Specifically, none of the following may be answered with a `200` and a plausible
body:

| Situation | Wrong behaviour | Required behaviour |
|---|---|---|
| Filter on a column the chunk lacks | Match everything | Error ([INV-X3](07-invariants.md#inv-x3)) |
| Select a field the chunk lacks | Emit `null` | Error ([INV-E3](07-invariants.md#inv-e3)) |
| Select a field the catalog lacks | Drop it | Error ([INV-Q7](07-invariants.md#inv-q7)) |
| `fromBlock: "abc"` | Coerce to 0 | Error ([INV-Q4](07-invariants.md#inv-q4)) |
| `parentBlockHash` mismatch | Serve the data | Fork error ([INV-E5](07-invariants.md#inv-e5)) |
| `parentBlockHash` on a dataset with no parent-hash column | Accept and skip the check | Error ([INV-E5](07-invariants.md#inv-e5)) |
| Join key of an unsupported type | Match nothing | Error ([INV-E7](07-invariants.md#inv-e7)) |

**Errors are stable.** Each error has a machine-readable kind. Clients switch on
the kind; only humans read the message ([INV-E6](07-invariants.md#inv-e6)).

## 6.2 Validation errors

Raised from the request and catalog, before any data is read.

| Kind | Trigger |
|---|---|
| `MalformedRequest` | The body is not a JSON object, or a value has the wrong JSON type. |
| `UnknownDataset` | `type` is absent, or names no dataset. |
| `UnknownTable` | A top-level key is neither reserved nor a `queryName`. |
| `UnknownFilter` | An item request key names neither a declared filter nor a declared relation of its table. |
| `UnknownFieldGroup` | A key of `fields` names no table's `fieldName`. |
| `UnknownField` | A key inside `fields.X` names no selectable field of `X`. |
| `InvalidBlockRange` | `toBlock < fromBlock`. |
| `InvalidBlockNumber` | `fromBlock` or `toBlock` is not an unsigned 64-bit integer. |
| `TooManyItemRequests` | More than `P-MAX-ITEM-REQUESTS` item requests across all tables. |
| `TooManyBloomValues` | More than `P-MAX-BLOOM-VALUES` values in one bloom filter. |
| `ConflictingFilters` | More than one discriminator-family filter in one item request. |
| `InvalidHex` | A hex value lacks `0x`, has odd length, or contains a non-hex digit. |
| `DiscriminatorTooLong` | A discriminator value exceeds `P-MAX-DISCRIMINATOR-BYTES`. |
| `InvalidFilterValue` | A filter value has a form the filter kind does not accept. |
| `RequestTooLarge` | The request exceeds an engine-configured byte or list-length bound. |
| `UnsupportedRequestField` | A reserved key the dataset cannot honour — `parentBlockHash` where the block table declares no parent-hash column. |

An engine SHOULD report the offending path (`transactions[2].sighash`) and, for
unknown names, the resolved internal spelling. A client that misspells a
camelCase key should not have to guess how the engine snake-cased it.

## 6.3 Execution errors

Raised against a chunk. Detectable only once the data is in hand.

| Kind | Trigger |
|---|---|
| `TableNotFound` | The chunk has no data for a table the query needs. |
| `ColumnNotFound` | A column the query selects or filters on is absent from the chunk. |
| `UnexpectedBaseBlock` | `parentBlockHash` does not match the hash of the block preceding `fromBlock`. |
| `UnsupportedKeyType` | A relation key, group key, or ordering key has a physical type the engine cannot compare. |
| `MalformedChunkData` | A stored value violates the catalog: a list where a scalar was declared, a `jsonVerbatim` column holding non-JSON, a fixed-width column of the wrong width. |

### `ColumnNotFound`

Fires for a column the query **selects** or **filters on**, never for one the
engine merely reads for its own purposes. The distinction is what makes it usable:
a chunk written before `sighash` existed must reject `{"sighash": [...]}` rather
than answer it with every transaction in the block.

An engine MAY treat a column declared in the catalog as *optional* — nullable and
tolerated when absent — but only if the catalog says so explicitly, and even then
selecting it yields `null` rather than an error. Optionality is a catalog
decision, never an inference from the column being missing.

### `UnexpectedBaseBlock`

Carries the expected hash and enough recent `(blockNumber, hash)` pairs for the
client to locate the common ancestor and rewind. This is how a client learns a
reorg happened between two pages.

The check is skipped, without error, only when no block of the search window is
in the chunk — `fromBlock` lies outside it. It is never skipped merely because
`fromBlock` is the chunk's first block: that block's row carries its parent's
hash. A dataset with no parent-hash column rejects the field instead
(`UnsupportedRequestField`).

### `UnsupportedKeyType`

An engine will not support comparing every physical type as a join key —
floating-point keys are meaningless, and dictionary-encoded keys may not be worth
the trouble. That is fine. What is not fine is treating an uncomparable key as
"never matches", which silently returns a response missing every related row.

If a key type cannot be compared, the query fails.

## 6.4 Error rendering

An error terminates the response. An engine MUST NOT emit a partial NDJSON stream
followed by an error, because a client that already parsed and committed the
first blocks cannot un-commit them.

Where the transport requires a body, the error is rendered as a JSON object
carrying at least `kind` and `message`, and for `UnexpectedBaseBlock` the
additional fields above.

## 6.5 What is *not* an error

For symmetry, because over-erroring is its own failure mode:

| Situation | Result |
|---|---|
| `[]` as a filter value | Matches nothing. |
| `"0x"` in a discriminator list | Matches everything. |
| A filter value too large for the column's physical type | Matches nothing. |
| `fromBlock` beyond the chunk's last block | Empty response, no `lastBlock`. |
| `toBlock` beyond the chunk's last block | Response covers the chunk. |
| A relation whose target has no matching rows | The relation contributes nothing. |
| `includeAllBlocks: true` over a range with no items | Headers for every block. |
| A tag value not in the catalog's field groups | Base fields only. |
| A bloom filter admitting a row that does not mention the account | A legal false positive. |
| The response exceeding `P-WEIGHT-BUDGET` because one block does | The block is emitted whole. |
