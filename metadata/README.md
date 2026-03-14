# Metadata Module

This module defines and loads dataset metadata from YAML files. Metadata drives the entire query engine — new chain types (EVM, Solana, Fuel, etc.) are added by writing a YAML file, not code.

YAML files live in [`metadata/`](../../metadata/).

## Table of Contents

1. [Top-Level Structure](#top-level-structure)
2. [Table Properties](#table-properties)
3. [Columns](#columns)
   - [Column Properties](#column-properties)
   - [Column Types](#column-types)
   - [JSON Encodings](#json-encodings)
   - [Weight](#weight)
   - [System Columns](#system-columns)
4. [Relations](#relations)
5. [Special Filters](#special-filters)
6. [Virtual Fields](#virtual-fields)
7. [Field Groups (Polymorphic Output)](#field-groups-polymorphic-output)
8. [Query Aliases](#query-aliases)
9. [Complete Minimal Example](#complete-minimal-example)

---

## Top-Level Structure

```yaml
name: solana                     # Dataset identifier

tables:
  blocks:                        # Table name (matches parquet filename: blocks.parquet)
    ...
  transactions:
    ...

query_aliases:                   # Optional: virtual query names mapped to existing tables
  ...
```

`tables` is an **ordered map** — insertion order determines the output ordering of table arrays within each block JSON object.

---

## Table Properties

### Identity & Naming

| Property | Type | Required | Description |
|---|---|---|---|
| `query_name` | string | no | Name used in the JSON query filter (e.g., `"logs"`, `"stateDiffs"`, `"tokenBalances"`). Defaults to the table key. Used as the JSON array key in output. |
| `field_name` | string | no | Name used in the JSON query `fields` object (e.g., `"log"`, `"stateDiff"`, `"tokenBalance"`). Defaults to the table key. |

The distinction matters because queries use plural names for filters (`"logs": [...]`) and singular for field selection (`"fields": { "log": { "address": true } }`).

### Block Number

| Property | Type | Required | Description |
|---|---|---|---|
| `block_number_column` | string | no | Column holding the block number. Defaults to `"block_number"`. The blocks table typically uses `"number"`. Used for block range filtering, block grouping, and cross-table joins. |

### Sorting & Ordering

| Property | Type | Required | Description |
|---|---|---|---|
| `sort_key` | list[string] | no | Physical sort order of the parquet file. Filter columns come first (e.g., `[topic0, address, block_number, log_index]`). This makes row group min/max statistics highly selective for filter pushdown. Not used at query time — only documents the physical layout. |
| `item_order_keys` | list[string] | no | Output sort order of items within a block (e.g., `[transaction_index, log_index]`). These match the legacy engine's primary keys minus `block_number`. |
| `address_column` | string | no | Hierarchical address column (e.g., `"trace_address"`, `"instruction_address"`). Appended to `item_order_keys` for output sorting. Also used for children/parents relation joins. |

### Hierarchy (Parent-Child)

| Property | Type | Required | Description |
|---|---|---|---|
| `children` | list[string] | no | Tables that are children of this table (e.g., transactions has children `[logs, balances, token_balances]`). Currently informational. |
| `parent_key` | list[string] | no | Key columns linking this table to its parent (e.g., `[block_number, transaction_index]`). Currently informational. |

---

## Columns

```yaml
columns:
  block_number:
    type: uint64
    stats: true
  address:
    type: string
    json_encoding: hex
    stats: true
    dictionary: true
  data:
    type: string
    json_encoding: hex
    weight: data_size
  fee:
    type: uint64
    json_encoding: string

  # system
  data_size:
    type: uint64
    system: true
```

### Column Properties

| Property | Type | Required | Description |
|---|---|---|---|
| `type` | string | **yes** | Data type. See [Column Types](#column-types) below. |
| `stats` | bool | no | Whether parquet row group statistics (min/max) are available. Enables predicate pushdown for row group pruning. |
| `dictionary` | bool | no | Whether dictionary encoding is used. Informational — may enable dictionary-level filtering in the future. |
| `json_encoding` | string | no | Controls JSON output encoding. See [JSON Encodings](#json-encodings) below. |
| `weight` | string or int | no | Weight for response size limiting. A string references a size column (e.g., `"data_size"`); an integer sets a fixed weight per row (e.g., `0`). Columns without `weight` get a default of 32 bytes per row. See [Weight](#weight). |
| `system` | bool | no | Marks internal columns not included in user output (size columns, bloom filters, discriminator columns). System columns are excluded from weight calculation. |

### Column Types

| Type | Arrow Type | Description |
|---|---|---|
| `uint8` | UInt8 | |
| `uint16` | UInt16 | |
| `uint32` | UInt32 | |
| `uint64` | UInt64 | |
| `int16` | Int16 | |
| `int64` | Int64 | |
| `float64` | Float64 | |
| `decimal128` | Decimal128 | High-precision decimal (used for large numeric values) |
| `boolean` | Boolean | |
| `string` | Utf8 | |
| `timestamp_second` | TimestampSecond | Unix timestamp in seconds, output as integer |
| `timestamp_millisecond` | TimestampMillisecond | Unix timestamp in milliseconds |
| `list_uint8` | List\<UInt8\> | |
| `list_uint32` | List\<UInt32\> | |
| `list_string` | List\<Utf8\> | |
| `list_struct` | List\<Struct\> | Passed through as JSON |
| `struct` | Struct | Passed through as JSON |
| `fixed_binary_N` | FixedSizeBinary(N) | Fixed-size byte array (e.g., `fixed_binary_64` for bloom filters) |

**Note:** Actual parquet types may differ from metadata declarations (e.g., `block_number` is declared `uint64` but stored as `Int32` in EVM parquet). The engine handles type coercion automatically.

### JSON Encodings

The `json_encoding` field controls how a column's value is serialized to JSON output.

| Encoding | Description | Example |
|---|---|---|
| `hex` | Bytes displayed as `0x`-prefixed hex string | `0xa0b1...` |
| `base58` | Bytes displayed as base58 string | `So11111111111111111111111111111112` |
| `string` | Integer as quoted decimal string (avoids JS precision loss for >2^53) | `5000` → `"5000"` |
| `json` | String containing JSON — parsed and embedded as raw JSON in output | `"{\"a\":1}"` → `{"a":1}` |
| `solana_tx_version` | Solana transaction version: `-1` → `"legacy"`, otherwise the number | `-1` → `"legacy"`, `0` → `0` |
| `timestamp_millisecond` | Millisecond timestamp, output as integer | `1710000000000` |

### Weight

The engine caps responses at 20 MB. Each projected column contributes to the estimated row weight:

- Columns with `weight: <column_name>` use the value of the referenced size column as their dynamic weight per row.
- Columns with `weight: <integer>` use a fixed weight per row (use `0` for columns already accounted for by another column's weight).
- Columns without `weight` get a default of 32 bytes per row.
- System columns (`system: true`) are excluded from weight calculation entirely.

```yaml
columns:
  data:
    type: string
    json_encoding: hex
    weight: data_size              # Dynamic: uses data_size column value
  a0:
    type: string
    json_encoding: base58
    weight: accounts_size          # First account carries weight for all
  a1:
    type: string
    json_encoding: base58
    weight: 0                      # Zero: already counted by a0
  block_number:
    type: uint64                   # No weight specified: defaults to 32 bytes

  # system
  data_size:
    type: uint64
    system: true                   # Not in output, not in weight calc
  accounts_size:
    type: uint64
    system: true
```

### System Columns

Columns with `system: true` are internal to the storage layer and are never exposed in query output. They exist to support engine features like weight estimation, bloom filter checks, and discriminator matching.

Common system columns:
- **Size columns**: `data_size`, `input_size`, `accounts_size`, `signatures_size`, etc. — used as weight sources.
- **Bloom filters**: `accounts_bloom` — used for `mentions_account` special filter.
- **Discriminator columns**: `d1` through `d16`, `b9` — used for Solana instruction discriminator matching.

In YAML files, system columns are conventionally separated from data columns with a `# system` comment.

---

## Relations

Relations define how tables can be joined when a query item requests related data.

```yaml
relations:
  transaction:                   # Relation name (used in query JSON)
    table: transactions          # Target table to join
    key: [block_number, transaction_index]  # Join key (same columns on both sides)

  subtraces:
    table: traces
    kind: children               # Hierarchical: find child rows
    key: [block_number, transaction_index, trace_address]

  parents:
    table: traces
    kind: parents                # Hierarchical: find parent rows
    key: [block_number, transaction_index, trace_address]

  instruction:
    table: instructions
    left_key: [block_number, transaction_index, instruction_address]  # Key on this table
    right_key: [block_number, transaction_index, instruction_address] # Key on target table
```

### Relation Properties

| Property | Type | Required | Description |
|---|---|---|---|
| `table` | string | **yes** | Target table name. |
| `kind` | string | no | Relation type: `join` (default), `children`, or `parents`. |
| `key` | list[string] | no | Join key columns (used for both sides when `left_key`/`right_key` are not set). |
| `left_key` | list[string] | no | Key columns on the source (current) table. Overrides `key`. |
| `right_key` | list[string] | no | Key columns on the target table. Overrides `key`. |

### Relation Kinds

- **`join`** (default): Hash-based equi-join. Returns rows from the target table whose key matches the source. With KeyFilter pushdown, matching is done during the scan itself.
- **`children`**: Hierarchical join using `address_column`. Finds rows in the target whose address is a prefix-child of the source row's address (e.g., trace `[0]` has children `[0,0]`, `[0,1]`, etc.).
- **`parents`**: Inverse of children. Finds rows whose address is a prefix-parent of the source (e.g., instruction `[1,2,3]` has parents `[1]` and `[1,2]`).

---

## Special Filters

Special filters handle query predicates that don't map to a single column comparison.

```yaml
special_filters:
  discriminator:                 # Solana instruction discriminator
    type: discriminator
    columns:
      "1": d1                    # 1-byte discriminator → column d1
      "2": d2                    # 2-byte → d2
      "8": d8                    # 8-byte → d8 (most common: Anchor uses 8-byte)
      ...

  mentions_account:              # Bloom filter for account mentions
    type: bloom_filter
    column: accounts_bloom
    num_bytes: 64
    num_hashes: 7

  first_nonce:                   # Range filter: nonce >= value
    type: range_gte
    column: nonce

  last_nonce:                    # Range filter: nonce <= value
    type: range_lte
    column: nonce

  type:                          # Column alias: query key maps to different column
    type: column_alias
    column: receipt_type
```

### Special Filter Types

| Type | Description |
|---|---|
| `discriminator` | Dispatches a hex byte string to the appropriate `dN` column based on byte length. Used for Solana instruction discriminators. |
| `bloom_filter` | Tests membership in a pre-computed bloom filter column. Used for "mentions account" queries without scanning all account columns. |
| `range_gte` | Maps to a `column >= value` predicate. |
| `range_lte` | Maps to a `column <= value` predicate. |
| `column_alias` | Maps a query filter key to a different physical column name (e.g., `type` → `receipt_type`). |

---

## Virtual Fields

Virtual fields combine multiple physical columns into a single output value.

```yaml
virtual_fields:
  accounts:                      # Output field name
    type: roll
    columns: [a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15, rest_accounts]

  topics:
    type: roll
    columns: [topic0, topic1, topic2, topic3]
```

### `roll`

Combines multiple columns into a JSON array. Non-null values are collected in order; the roll stops at the first null. If the last column is a list type, its elements are spread into the array.

Example: if `a0="X"`, `a1="Y"`, `a2=null`, then `accounts` outputs `["X","Y"]`.

---

## Field Groups (Polymorphic Output)

Field groups handle tables where the output JSON structure depends on a tag column value. Used for EVM traces and Fuel inputs/outputs where `type` determines which nested objects appear.

```yaml
field_groups:
  tag_column: type               # Column that determines the variant
  base_fields:                   # Fields at top level for ALL variants
    - transaction_index
    - trace_address
    - subtraces
    - type
    - error
    - revert_reason
  variants:
    create:                      # When type = "create"
      action:                    # Nested object "action"
        - { column: create_from, field: from }
        - { column: create_value, field: value }
        - { column: create_gas, field: gas }
        - { column: create_init, field: init }
      result:                    # Nested object "result"
        - { column: create_result_gas_used, field: gasUsed }
        - { column: create_result_code, field: code }
        - { column: create_result_address, field: address }
    call:
      action:
        - { column: call_from, field: from }
        - { column: call_to, field: to }
        ...
      result:
        - { column: call_result_gas_used, field: gasUsed }
        - { column: call_result_output, field: output }
```

Each variant maps its physical `column` to a display `field` name within a group. The group name becomes a nested JSON key. A group is emitted in the output if the user selected at least one field from it, even if all values are null.

Use `_` as the group name to flatten fields directly into the top-level object instead of nesting:

```yaml
variants:
  InputCoin:
    _:                           # Underscore = no nesting, fields go at top level
      - { column: coin_utxo_id, field: utxoId }
      - { column: coin_owner, field: owner }
```

---

## Query Aliases

Query aliases create virtual query names that map to existing tables with implicit predicates and column remapping. This allows a single physical table to be queried as if it were multiple specialized tables.

```yaml
query_aliases:
  evmLogs:
    table: events                          # Physical table to query
    implicit_predicates:                   # Always-applied filters
      name: [ "EVM.Log" ]
    filter_aliases:                        # Remap query filter keys to physical columns
      topic0: _evm_log_topic0
      address: _evm_log_address
    relations:                             # Relations available for this alias
      extrinsic:
        table: extrinsics
        left_key: [ block_number, extrinsic_index ]
        right_key: [ block_number, index ]
```

### Query Alias Properties

| Property | Type | Required | Description |
|---|---|---|---|
| `table` | string | **yes** | Physical table this alias queries. |
| `implicit_predicates` | map | no | Column → values filters automatically applied to every query through this alias. |
| `filter_aliases` | map | no | Maps query filter keys to physical column names (e.g., `topic0` → `_evm_log_topic0`). |
| `relations` | map | no | Relations available when querying via this alias. Same structure as table-level relations. |

### Use Cases

- **Substrate `evmLogs`**: Queries the `events` table with `name: ["EVM.Log"]` implicit filter, remapping EVM-style filter keys (`topic0`, `address`) to the physical columns (`_evm_log_topic0`, `_evm_log_address`).
- **Hyperliquid `orderActions`/`cancelActions`**: Queries the `actions` table with `action_type` implicit filter, remapping `contains_asset` to type-specific columns (`order_asset`, `cancel_asset`).

---

## Complete Minimal Example

```yaml
name: my_chain

tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number:
        type: uint64
        stats: true
      hash:
        type: string
        json_encoding: hex
      timestamp:
        type: timestamp_second

  transactions:
    query_name: transactions
    field_name: transaction
    item_order_keys: [transaction_index]
    sort_key: [to, block_number, transaction_index]
    columns:
      block_number:
        type: uint64
        stats: true
      transaction_index:
        type: uint32
      to:
        type: string
        json_encoding: hex
        stats: true
        dictionary: true
      value:
        type: string
        json_encoding: hex
      input:
        type: string
        json_encoding: hex
        weight: input_size

      # system
      input_size:
        type: uint64
        system: true
```
