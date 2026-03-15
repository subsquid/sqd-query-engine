# Worker Query Patterns Analysis

Analysis of the most common query patterns from `worker_query_logs_local` ClickHouse table.
Data covers Ethereum (`ethereum-mainnet-2`) and Solana (`solana-mainnet-3`) datasets.

## Summary

- **26** unique structural query patterns (from top 50 query_hashes per chain)
- **15** Ethereum patterns (6,658,801 total requests)
- **11** Solana patterns (10,405,854 total requests)

## Ethereum Query Patterns

| #  | Requests  | Variants | Pattern |
|----|-----------|----------|---------|
| 1  | 2,764,165 | 3        | `logs[address, topic0](5 values) -> transaction` + `logs[topic0](11 values) -> transaction` + `logs[address, topic0](4 values) -> transaction` + `logs[address, topic0](5 values) -> transaction` + `logs[address, topic0](3 values) -> transaction` + `logs[address, topic0](3 values) -> transaction` + `logs[address, topic0](7 values) -> transaction` |
| 2  | 2,172,078 | 27       | `logs[topic0](1 values)` |
| 3  | 1,030,206 | 2        | (fields only, no table filters) |
| 4  | 127,774   | 1        | `transactions[from] -> logs` + `transactions[to] -> logs` + includeAllBlocks |
| 5  | 107,128   | 1        | `logs[address, topic0](2 values)` |
| 6  | 105,564   | 1        | (fields only, no table filters) |
| 7  | 97,165    | 1        | `logs[topic0](1 values) -> transaction` |
| 8  | 67,472    | 6        | `logs[address, topic0](2 values) -> transaction` |
| 9  | 43,480    | 1        | `logs[address, topic0](2 values)` + `logs[address, topic0](2 values)` |
| 10 | 41,335    | 1        | `logs[topic0](1 values)` |
| 11 | 32,528    | 1        | `logs[no filter](0 values)` + includeAllBlocks |
| 12 | 26,206    | 2        | `logs[topic0](4 values) -> transaction` |
| 13 | 19,607    | 1        | `logs[no filter](0 values) -> transaction` + `logs[topic0](16 values) -> transaction` + `transactions[no filter]` + `traces[type] -> transaction` |
| 14 | 13,169    | 1        | includeAllBlocks |
| 15 | 10,924    | 1        | `logs[address, topic0](2 values)` + `logs[topic0](1 values) -> transaction` |

## Solana Query Patterns

| #  | Requests  | Variants | Pattern |
|----|-----------|----------|---------|
| 1  | 5,699,723 | 9        | `instructions[programId, d4] -> isCommitted, transaction` + `instructions[programId, d1] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + `instructions[programId, d1] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + 8 more filters |
| 2  | 1,996,972 | 12       | `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions, logs` + `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions, logs` + `instructions[programId, d1] -> isCommitted, transaction, transactionTokenBalances, innerInstructions, logs` |
| 3  | 747,526   | 2        | `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + `instructions[programId, d1] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` |
| 4  | 590,909   | 13       | `instructions[programId, d8] -> isCommitted, transaction, innerInstructions` |
| 5  | 485,533   | 1        | `instructions[programId, d8] -> isCommitted, transaction, innerInstructions` + `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + 11 more filters |
| 6  | 456,389   | 6        | `instructions[programId, d1, a1] -> isCommitted, transaction, transactionBalances, transactionTokenBalances, transactionInstructions, innerInstructions, logs` + `instructions[programId, d1, a1] -> isCommitted, transaction, transactionBalances, transactionTokenBalances, transactionInstructions, innerInstructions, logs` + `instructions[programId, d1, a1] -> isCommitted, transaction, transactionBalances, transactionTokenBalances, transactionInstructions, innerInstructions, logs` + 1 more filters |
| 7  | 111,453   | 2        | `instructions[programId] -> isCommitted, transaction, transactionTokenBalances, innerInstructions, logs` |
| 8  | 100,959   | 2        | `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions, logs` + `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions, logs` |
| 9  | 87,767    | 1        | `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` |
| 10 | 78,761    | 1        | `instructions[programId, d1] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + `instructions[programId, d1] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + `instructions[programId, d1] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + 5 more filters |
| 11 | 49,862    | 1        | `instructions[programId, d8] -> isCommitted, transaction, innerInstructions` + `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + `instructions[programId, d8] -> isCommitted, transaction, transactionTokenBalances, innerInstructions` + 10 more filters |

## Pattern Categories

### Ethereum

- **Log filter only (no relations)**: 5 patterns, 2,396,549 requests (36%)
- **Log filter with transaction relation**: 5 patterns, 2,965,932 requests (45%)
- **Transaction-first query**: 1 patterns, 127,774 requests (2%)
- **Fields only (no filters)**: 3 patterns, 1,148,939 requests (17%)

### Solana

- **Instructions + tx + balances + logs + inner**: 6 patterns, 8,444,257 requests (81%)
- **Instructions + tx + inner instructions**: 1 patterns, 590,909 requests (6%)
- **Simple instruction filter**: 2 patterns, 835,293 requests (8%)
- **Multi-filter instructions**: 2 patterns, 535,395 requests (5%)

## Key Observations

1. **EVM is dominated by log queries** — ~80% of requests filter by `topic0` (event signature), often with `address`
   filter
2. **Log → transaction relation** is the most common join pattern in EVM
3. **Solana is dominated by instruction queries** with `programId` + discriminator (`d8`, `d4`, `d1`) filters
4. **Solana queries are heavier** — most request `transaction`, `tokenBalances`, `innerInstructions`, and `logs`
   relations simultaneously
5. **Multi-filter queries are common in Solana** — single query often contains 3-14 instruction filter groups (different
   programs/discriminators)
6. **Fields-only queries exist** in EVM — just requesting block headers with no table filters

## Recommended Test Queries (~25)

### EVM (12 queries)

| #  | Category     | Description                                 |
|----|--------------|---------------------------------------------|
| 1  | logs_only    | Single topic0 filter (Transfer events)      |
| 2  | logs_only    | topic0 + address filter                     |
| 3  | logs_only    | Two log filter groups (different events)    |
| 4  | logs_with_tx | topic0 → transaction                        |
| 5  | logs_with_tx | address+topic0 → transaction                |
| 6  | logs_with_tx | Multi-log (7 groups) → all with transaction |
| 7  | multi_table  | logs + txs + traces + stateDiffs (heavy)    |
| 8  | multi_table  | logs + allBlocks (full scan with events)    |
| 9  | multi_table  | logs + traces → transaction                 |
| 10 | tx_first     | transactions[from,to] → logs + allBlocks    |
| 11 | all_blocks   | includeAllBlocks with all field types       |
| 12 | fields_only  | Block headers only                          |

### Solana (13 queries)

| #  | Category | Description                                                 |
|----|----------|-------------------------------------------------------------|
| 13 | simple   | instr[programId,d8] → tx,inner                              |
| 14 | simple   | instr[programId,d8] → tx,tokenBal,inner                     |
| 15 | simple   | instr[programId] → tx,tokenBal,inner,logs                   |
| 16 | with_all | instr[programId,d1,a1] → tx,bal,tokenBal,txInstr,inner,logs |
| 17 | with_all | instr[programId,d8] → tx,tokenBal,inner,logs (2 filters)    |
| 18 | multi    | 3 instr filters (d8,d8,d1) → tx,tokenBal,inner,logs         |
| 19 | multi    | 3 instr filters (d8,d1,d8) → tx,tokenBal,inner              |
| 20 | multi    | 8 instr filters (mixed d1,d8) → tx,tokenBal,inner,logs      |
| 21 | heavy    | 11 instr filters (d4,d1,d8) → tx,tokenBal,inner,logs        |
| 22 | heavy    | 13 instr filters → tx,bal,tokenBal,inner,logs,reward        |
| 23 | heavy    | 14 instr filters → tx,inner,tokenBal,txInstr                |
| 24 | simple   | instr[programId,d8] → tx,inner (single)                     |
| 25 | simple   | instr[programId,d4] → tx (single, minimal)                  |
