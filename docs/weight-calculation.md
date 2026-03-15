# Weight Calculation

Weight-based limiting controls how many blocks fit into a single response.
The engine accumulates per-block weight and stops when the total exceeds
20 MB (`MAX_RESPONSE_BYTES`). At least one block is always included.

## Overview

```
Query → Plan → Scan & Join → Weight Calculation → Block Selection → JSON Output
                                    ↑
                           This document covers this step
```

Each row in the result contributes weight based on its projected columns.
Weight is summed per block, then blocks are selected in order until the
cumulative weight exceeds the limit.

## Weight Formula

For each row in a table:

```
row_weight = fixed_weight + sum(weight_column_values)
```

Where:
- **fixed_weight**: sum of fixed per-column weights for all projected non-system columns
- **weight_column_values**: actual values read from parquet "size" columns (for variable-length data)

## Column Weight Types

Defined in metadata YAML per column:

| YAML `weight` value | Type | Example | Meaning |
|---|---|---|---|
| _(not set)_ | Default fixed | `program_id` | 32 bytes per row |
| `0` | Explicit fixed | `a1` (Solana account) | 0 bytes (free) |
| `512` | Explicit fixed | `logs_bloom` (EVM) | 512 bytes per row |
| `data_size` | Dynamic (column ref) | `data` → `data_size` | Actual byte count read from `data_size` column |

System columns (`system: true`) are always excluded from weight, even if projected.

## Which Columns Contribute to Weight

### Legacy Engine

The legacy engine computes weight from:

```
projection = table.primary_key + output_expression_columns
```

Where:
- **primary_key**: e.g., `[block_number, transaction_index, instruction_address]`
- **output_expression_columns**: columns walked from the user's field selection expression
  (`Exp::Prop` → column name, `Exp::Roll` → all source columns, `Exp::Enum` → tag + variant columns)

Notably, the legacy engine does NOT include in weight:
- Join key columns (read separately during relation lookup)
- Predicate/filter columns (unless also in user output)
- Sort key columns (unless they are primary key or in user output)

### New Engine

The new engine uses `weight_projection()` to compute weight columns, matching
legacy behavior:

```
weight_projection = primary_key + user_output_columns
```

Where primary key = `block_number_column` + `item_order_keys` + `address_column`.

This is separate from `resolve_output_columns()`, which determines the full set
of columns to read from parquet (including join keys, source predicates, tag
columns). Those extra columns are needed for scanning and joining but do NOT
contribute to weight.

| Column Category | In weight_projection? | In resolve_output_columns? |
|---|---|---|
| Block number column | Yes | Yes |
| Item order keys | Yes | Yes |
| Address column | Yes (if exists) | Yes |
| User-requested fields | Yes | Yes |
| Virtual field sources | Yes | Yes |
| Join key columns | No | Yes |
| Source predicate columns | No | Yes |
| Tag column (field groups) | No | Yes |
| Weight/size columns | Yes (if data col projected) | Yes |

## Virtual Fields

Virtual fields (e.g., `accounts`, `topics`) expand to their source columns:

```yaml
accounts:
  type: roll
  columns: [a0, a1, a2, ..., a15, rest_accounts]
```

Each source column contributes weight independently:
- `a0`: `weight: accounts_size` (dynamic — actual byte count)
- `a1`–`a15`, `rest_accounts`: `weight: 0` (free)

```yaml
topics:
  type: roll
  columns: [topic0, topic1, topic2, topic3]
```

Each topic column: no explicit weight → default 32 bytes. Total: 128 bytes/row.

## Weight Configuration by Chain

### EVM

| Table | Column | Weight |
|---|---|---|
| blocks | logs_bloom | 512 fixed |
| blocks | extra_data | extra_data_size (dynamic) |
| transactions | input | input_size (dynamic) |
| logs | data | data_size (dynamic) |
| traces | create_init | create_init_size (dynamic) |
| traces | create_result_code | create_result_code_size (dynamic) |
| traces | call_input | call_input_size (dynamic) |
| traces | call_result_output | call_result_output_size (dynamic) |
| statediffs | prev | prev_size (dynamic) |
| statediffs | next | next_size (dynamic) |

All other non-system columns: 32 bytes default.

### Solana

| Table | Column | Weight |
|---|---|---|
| transactions | account_keys | account_keys_size (dynamic) |
| transactions | address_table_lookups | address_table_lookups_size (dynamic) |
| transactions | signatures | signatures_size (dynamic) |
| transactions | loaded_addresses | loaded_addresses_size (dynamic) |
| instructions | data | data_size (dynamic) |
| instructions | a0 | accounts_size (dynamic) |
| instructions | a1–a15, rest_accounts | 0 fixed (free) |
| logs | message | message_size (dynamic) |

All other non-system columns: 32 bytes default.
System columns (d1–d16, b9, bloom filters, size columns): excluded from weight.

## Deduplication

When multiple query items scan the same table (direct + relation batches targeting
the same table), rows may be duplicated. The weight system deduplicates by
`(block_number, hash(item_order_keys))`:

- **Single source**: Fast path, no dedup needed
- **Multiple sources**: Tracks seen `(block_number, key_hash)` pairs; duplicates are skipped

## Block Selection

After per-block weights are computed:

1. Blocks are processed in sorted order (ascending block number)
2. Each block's weight is added to a cumulative total
3. The first block is always included
4. Subsequent blocks are included while `cumulative_weight <= MAX_RESPONSE_BYTES`
5. Once exceeded, no more blocks are added

Block header weight is always included for blocks that have items (unless
`include_all_blocks` is set, in which case all blocks contribute header weight).

## Code References

- `src/output/weight.rs`: `apply_weight_limit()`, `compute_weight_params()`
- `src/output/columns.rs`: `resolve_output_columns()`, `resolve_relation_output_columns()`
- `metadata/*.yaml`: Column weight annotations
