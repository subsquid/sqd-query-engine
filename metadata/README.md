# Catalogs

A catalog describes one dataset: which tables it has, what a client may ask of
each, and what the parquet files hold. The engine has no chain-specific code;
everything it knows about EVM or Solana it reads from a catalog at load time.
Adding a chain means writing a YAML file, not a module.

The catalogs live in this directory, one per dataset. They load into the types
in [`src/metadata/types.rs`](../src/metadata/types.rs) and must pass the checks
in [`src/metadata/loader.rs`](../src/metadata/loader.rs). A key the types do
not know is an error, not a warning: a misspelled key would otherwise silently
do nothing.

## The shape of a table

A table answers three questions, and each has a block of its own: what a client
may *send* for it, what a client may *see* of it, and what is actually *stored*.

```yaml
name: evm

tables:
  logs:
    request:                      # what a client writes in `logs: [ { ... } ]`
      name: logs                  # also the key of this table's array in each response block
      filters: [ address, topic0, topic1, topic2, topic3 ]
      relations:
        transaction: { table: transactions, key: [ block_number, transaction_index ] }
    output:                       # what a client picks in `fields: { log: { ... } }`
      name: log
      fields: [ log_index, transaction_index, transaction_hash, address, data, topics ]
      virtual_fields:
        topics: { kind: roll, columns: [ topic0, topic1, topic2, topic3 ] }
    item_order_keys: [ transaction_index, log_index ]
    sort_key: [ topic0, address, block_number, log_index ]
    columns:
      block_number:      { type: uint64 }
      log_index:         { type: uint32 }
      transaction_index: { type: uint32 }
      transaction_hash:  { type: string, encoding: hex_bytes }
      address:           { type: string, encoding: hex_bytes }
      data:              { type: string, encoding: hex_bytes, weight: data_size }
      topic0:            { type: string, encoding: hex_bytes }
      topic1:            { type: string, encoding: hex_bytes }
      topic2:            { type: string, encoding: hex_bytes }
      topic3:            { type: string, encoding: hex_bytes }
      data_size:         { type: uint64, system: true }
```

The two surfaces are closed. A column is filterable because `request.filters`
names it and visible because `output.fields` names it; a column in neither list
— `data_size`, `topic1` on its own — is the engine's business alone. This is
what keeps the physical layout out of the wire contract: a column can be added,
split or renamed without any client noticing, so long as the two lists still
resolve.

`tables` is ordered. Its order is the order item arrays appear in a response
block, and a table's `columns` order is the order fields appear in an item. The
block table comes first.

### Names

Everything inside a catalog is `snake_case`: columns, fields, filters, relation
names. Clients see `camelCase`, and the engine converts at the boundary —
`log_index` is `logIndex` on the wire, `call_call_type` is `callCallType`.

Four keys are written exactly as they will be read, with no conversion in
between, so they are spelled the way the client spells them:

- `request.name` and `output.name` — `tokenBalances` and `tokenBalance`, not
  `token_balances`;
- a variant mapping's `as`, and the name of the group holding it — `gasUsed`,
  `callType`, `refundAddress`, inside `action` and `result`.

Writing `as: gas_used` is therefore not a style slip the engine corrects; it is
a response field named `gas_used` while everything around it is camelCase.

## `request`

| Key | Required | Meaning |
|---|---|---|
| `name` | no | The key of this table's item requests in a query, and of its array in every response block. Defaults to the table's own name. |
| `filters` | **yes** | The filters an item request may carry. Each entry is a non-system column of the same name, or a key of `special_filters`. |
| `special_filters` | no | Filters that are not a plain column comparison. Each must also appear in `filters`, or no request can reach it. |
| `relations` | no | The relation flags an item request may switch on. |

The block table has no `request` block: its rows are the headers, which come
with every response, so it keeps its default name, takes no filters and no
relations, and accepts `{}` as its only item request.

Every other table declares one. Leaving it out is not an error the loader could
otherwise see — it would be a table that rejects every filter and every relation
with a 400, which from outside looks like a catalog missing those columns rather
than one with a hole in it. `filters` is required inside the block for the same
reason, and a table that really has no filters says `filters: []`.

A plain filter compares a column against a list of values. A list-typed column
(`list_uint32`, `list_string`) matches when the lists intersect.

### Special filters

Each has a `kind`:

| `kind` | Keys | Meaning |
|---|---|---|
| `column_alias` | `column` | The filter reads a column of another name. `call_call_type` → `call_type`. |
| `bloom` | `column`, `bytes`, `hashes` | Probabilistic membership in a bloom column, which must be `fixed_binary_N`. `bytes` is the bloom's size and has to be that `N` — the probe reads the width off the stored array, so the key is a statement about the archive writer that the loader holds to the column. `hashes` is how many hash functions the writer used, and nothing but the writer can tell you. |
| `discriminator` | `by_length` | Dispatches a hex prefix to the column holding prefixes of its byte length: `{ "1": d1, "2": d2, … }`. |
| `range_gte` / `range_lte` | `column` | An inclusive bound on the column. |
| `gte_const` | `column`, `value` | A boolean flag. `true` keeps rows where the column is at least a catalog-fixed constant — `call_value >= "0x1"` is the `callValueNonZero` filter. |

```yaml
request:
  name: instructions
  filters: [ program_id, discriminator, mentions_account, a0 ]
  special_filters:
    discriminator:
      kind: discriminator
      by_length: { "1": d1, "2": d2, "4": d4, "8": d8 }
    mentions_account:
      kind: bloom
      column: accounts_bloom
      bytes: 64
      hashes: 7
```

### Relations

A relation pulls related rows into the response when an item request sets its
flag to `true`. It never narrows the result.

| Key | Required | Meaning |
|---|---|---|
| `table` | **yes** | The target table. |
| `kind` | no | `join` (default), `children` or `parents`. |
| `key` | no | Join columns, the same on both sides. |
| `left_key` / `right_key` | no | Join columns per side, when they differ. Override `key`. |

The first key column on each side must be that side's block number column;
that is what keeps a relation inside one block, and a chunk answerable on its
own.

`join` matches rows whose keys are equal. `children` and `parents` walk the
tree an `address_column` describes: a trace at `[0]` has children `[0, 0]` and
`[0, 1]`, and an instruction at `[1, 2, 3]` has parents `[1]` and `[1, 2]`.
Both need an `address_column` on both tables.

```yaml
relations:
  transaction:  { table: transactions, key: [ block_number, transaction_index ] }
  subtraces:    { table: traces, kind: children, key: [ block_number, transaction_index, trace_address ] }
  parents:      { table: traces, kind: parents,  key: [ block_number, transaction_index, trace_address ] }
```

## `output`

| Key | Required | Meaning |
|---|---|---|
| `name` | no | The key of this table's selection under a query's `fields`. No default: a table without one cannot be selected. |
| `fields` | with `name` | The fields a client may select. Each names a non-system column, a virtual field, or a variant mapping's field key. |
| `virtual_fields` | no | Fields assembled from several columns. |
| `variant_column` | with `variants` | The column whose value says which shape a row has. |
| `variants` | with `variant_column` | Per-variant nesting of fields. |

### Virtual fields

One kind exists, `roll`: an ordered list of columns rendered as one JSON array.
The array stops at the first null, and if the last column is a list its
elements are spread into the array rather than nested. It presents a structure
the writer flattened — `topic0 … topic3`, `a0 … a15, rest_accounts` — as the
array it logically is.

```yaml
virtual_fields:
  topics:   { kind: roll, columns: [ topic0, topic1, topic2, topic3 ] }
  accounts: { kind: roll, columns: [ a0, a1, a2, a3, rest_accounts ] }
```

A virtual field is selectable like a column once `fields` lists it. The columns
it rolls usually are not; a client sees `topics`, not `topic2`.

### Variants

Some tables hold rows of several shapes. An EVM trace is a `create`, a `call`,
a `suicide` or a `reward`, and each carries its own columns and its own nested
objects in the response.

```yaml
output:
  name: trace
  fields: [ transaction_index, trace_address, type, call_from, call_to, call_type, call_call_type ]
  variant_column: type
  variants:
    call:
      action:
        - { column: call_from, as: from }
        - { column: call_to,   as: to }
        - { column: call_type, as: type }
        - { column: call_type, field_key: call_call_type, as: callType }
```

Each variant maps groups to fields. A group is a nested object in the response
(`action`, `result`); the group `_` is written flat. A mapping reads `column`
and renders it under the key `as`; a client selects it as `field_key`, which
defaults to the column's name. Two mappings may read one column under
different field keys — `call_type` above renders as both `action.type` and
`action.callType`.

`field_key` is not spelled `field`, which is what the key was called when it
meant the *rendered* name. A mapping still carrying the old spelling is refused
rather than read the other way round.

A field no variant claims is a plain column, and renders flat for every row —
which is to say the mappings, and nothing else, decide the shape. Claiming a
field is all-or-nothing: it leaves the top level for every row, and rows of the
variants that do not also claim it lose it entirely. That is what
`create_init` wants and what `type` does not, so a mapping over the columns
that identify a row — the variant column, `item_order_keys`, `address_column`,
the block number — is refused.

A row whose `variant_column` holds a value the catalog does not know renders its
plain fields and nothing else; it is not an error, because archives outlive
catalogs.

## Storage

| Key | Required | Meaning |
|---|---|---|
| `block_number_column` | no | The block number column. Defaults to `block_number`; the block table usually says `number`. |
| `parent_hash_column` | no | Block table only. The parent block's hash; declaring it is what enables fork detection for the dataset. |
| `parent_number_column` | no | Block table only. The parent block's number, for chains that skip numbers (Solana slots). Absent means `number - 1`. |
| `address_column` | no | The tree path of a hierarchical table: `trace_address`, `instruction_address`. Used by `children`/`parents` relations and appended to the item order. |
| `item_order_keys` | no | The columns that, with the block number, order items within a block. The block table has none — a block number alone identifies its rows, and that is how the engine knows which table is the block table. |
| `sort_key` | no | The order rows physically sit in. Filter columns first, then block number. The engine may use it to prune work; it never affects a result. |
| `columns` | **yes** | The columns, in output order. |

### Columns

| Key | Required | Meaning |
|---|---|---|
| `type` | **yes** | The logical type, below. |
| `encoding` | no | How the value renders in a response, when not as the type's natural JSON. |
| `weight` | no | What the column adds to a row's weight: a size column's name, or a fixed integer. Absent means 32. |
| `system` | no | The column exists for filters, joins, ordering or weights, and is never emitted. It cannot be a field or a plain filter; a special filter is how it is reached on purpose. Weighs nothing. |
| `fold_case` | no | Compare filter values case-insensitively. A `hex_bytes` column folds already; this is for hex stored without the `0x` prefix, as Tron writes it, which renders verbatim and so cannot say it through the encoding. |

Types:

| `type` | Arrow |
|---|---|
| `uint8`, `uint16`, `uint32`, `uint64` | unsigned integers |
| `int16`, `int32`, `int64` | signed integers |
| `float64` | Float64 |
| `boolean` | Boolean |
| `string` | Utf8 |
| `timestamp_second`, `timestamp_millisecond` | timestamps, rendered as integers in the declared unit |
| `decimal128` | Decimal128 |
| `list_uint8`, `list_uint32`, `list_string` | lists |
| `struct`, `list_struct` | passed through as JSON |
| `fixed_binary_N` | FixedSizeBinary(N) — `fixed_binary_64` for a bloom |

A declared type bounds the values, not the storage. An archive writer narrows
integers to the smallest width that fits the chunk, so a `uint64` block number
is usually stored as 32 bits and a `uint32` index as 16, and different chunks of
one dataset differ. The engine reads any integer width for any declared integer
type and answers the same.

Encodings, named as the specification names them:

| `encoding` | Renders as |
|---|---|
| `hex_bytes` | `0x` + lowercase hex, variable length. Filters on such a column fold case. |
| `base58` | base58 string. |
| `decimal_string` | the integer quoted, `"1000000000000000000"`, for values a JavaScript number cannot hold exactly. |
| `hex_number` | the integer as hex, zero-padded to the column's width: a `uint16` 1600 is `"0x0640"`. Unsigned integers only. |
| `json_verbatim` | the stored bytes spliced in as they are — the column already holds JSON. Empty renders as `null`. |
| `timestamp_millisecond` | the raw millisecond integer. |
| `solana_tx_version` | `-1` renders as `"legacy"`, anything else as the number. |

Weight is how a response is bounded: blocks are added until their rows' weight
crosses the budget, and a row's weight is the sum over the columns actually
emitted for it. A variable-size column (`data`, `input`) names a size column the
writer filled; `a0` carries `accounts_size` for all sixteen account columns and
`a1 … a15` say `weight: 0`.

## Aliases

An alias is another request surface over an existing table — a narrower view
under a name of its own. It has the shape of a `request` block, plus the filters
that make it a view:

```yaml
aliases:
  evmLogs:
    table: events
    implicit_filters:
      name: [ "EVM.Log" ]
    filters: [ address, topic0, topic1, topic2, topic3 ]
    special_filters:
      address: { kind: column_alias, column: _evm_log_address }
      topic0:  { kind: column_alias, column: _evm_log_topic0 }
      topic1:  { kind: column_alias, column: _evm_log_topic1 }
      topic2:  { kind: column_alias, column: _evm_log_topic2 }
      topic3:  { kind: column_alias, column: _evm_log_topic3 }
    relations:
      extrinsic:
        table: extrinsics
        left_key:  [ block_number, extrinsic_index ]
        right_key: [ block_number, index ]
```

Every name in `filters` resolves, here as on a table: each is a column of the
target or a key of the alias's own `special_filters`. The five above are all
renames, so all five are declared — listing `topic1` without defining it is a
catalog the loader refuses, not a filter that quietly falls through to the
table.

`implicit_filters` are always applied and a client can neither see nor override
them. `filters`, `special_filters` and `relations` replace the table's own.

An alias *defines* only `column_alias` filters, because an alias's job is to
reach the extraction columns its implicit filter makes meaningful, and because
an item request carries the column a rename resolves to while the plan looks
every other kind up on the table. It *reaches* more than that: naming one of the
table's own special filters in `filters` — a bloom, a bound, a discriminator —
admits it, whatever its kind. What an alias cannot do is invent one.

Items requested through an alias and through its table in one query land in the
same array, deduplicated.

## What the loader rejects

A catalog is validated when it is loaded, and one that fails is not used. The
checks need no chunk:

- exactly one block table — the one whose `item_order_keys` is empty and which
  has no `address_column` — and it is declared first, and every other table
  declares a `request` block;
- every column a `block_number_column`, `address_column`, `item_order_keys`,
  `sort_key`, `weight`, special filter, virtual field, variant or alias names
  exists;
- every entry of `filters` is a special filter or a non-system column, and every
  special filter is in `filters`;
- every entry of `fields` is a non-system column, a virtual field or a variant
  field, and a table that declares an output `name` declares `fields`;
- a roll's spread list column, if any, is last; a discriminator length is
  written the way the lookup reads it (`"8"`, not `8`) and is at most 16;
- `variant_column` and `variants` come together; `hex_number` is declared on an
  unsigned integer; a `bloom` names a `fixed_binary_N` column and declares that
  `N`;
- every field mapping resolves once: a `field_key` is its own column's name or
  no column's, two mappings answering to one field key read one column, no two
  mappings in a group share an `as`, and none claims a column that identifies a
  row;
- no `special_filters` or `virtual_fields` entry carries a key its `kind` does
  not take — the one place serde would drop it in silence rather than complain;
- every relation targets a real table, both keys have equal length and begin
  with the block number column, and `children`/`parents` relations have an
  `address_column` on both sides;
- request names are unique across tables and aliases, and output names across
  tables. A duplicate would be resolved by iteration order, which is to say
  arbitrarily.

## A minimal catalog

```yaml
name: my_chain

tables:
  blocks:
    output:
      name: block
      fields: [ number, hash, parent_hash, timestamp ]
    block_number_column: number
    parent_hash_column: parent_hash
    sort_key: [ number ]
    columns:
      number:      { type: uint64 }
      hash:        { type: string, encoding: hex_bytes }
      parent_hash: { type: string, encoding: hex_bytes }
      timestamp:   { type: timestamp_second }

  transactions:
    request:
      name: transactions
      filters: [ from, to ]
    output:
      name: transaction
      fields: [ transaction_index, hash, from, to, value ]
    item_order_keys: [ transaction_index ]
    sort_key: [ to, block_number, transaction_index ]
    columns:
      block_number:      { type: uint64 }
      transaction_index: { type: uint32 }
      hash:              { type: string, encoding: hex_bytes }
      from:              { type: string, encoding: hex_bytes }
      to:                { type: string, encoding: hex_bytes }
      value:             { type: uint64, encoding: decimal_string }
```
