# Gap analysis — implementation vs. specification

**Non-normative.** This document records where the engine in this repository
currently diverges from [the specification](README.md). It is kept out of the
normative chapters on purpose: a spec that describes today's bugs cannot be used
to find them.

Delete an entry when the gap closes. If the spec turns out to be wrong and the
implementation right, fix the spec and delete the entry. The document should tend
toward empty.

Compared against the reference implementation at
`/Users/mo4islona/Projects/subsquid/data/crates/query`, as of 2026-07-09.

---

## Severity

| | Meaning |
|---|---|
| **S1** | Wrong results that look right. A client cannot detect the failure. |
| **S2** | Missing capability. A query that should work is rejected, or a dataset is unserved. |
| **S3** | Crash, or an error where a result was due. Loud; a client can retry or route around it. |
| **S4** | Robustness, hygiene, latent. |

## Summary

| # | Gap | Invariant | Sev |
|---|---|---|---|
| 1 | Filtering on a column absent from the chunk matches **every row** | [INV-X3](07-invariants.md#inv-x3) | **S1** |
| 2 | `parentBlockHash` is accepted and ignored — no fork detection | [INV-E5](07-invariants.md#inv-e5) | **S1** |
| 3 | Unknown field names in `fields` are silently dropped | [INV-Q7](07-invariants.md#inv-q7) | **S1** |
| 4 | Malformed `fromBlock` / `toBlock` coerced to `0` / unbounded | [INV-Q4](07-invariants.md#inv-q4) | **S1** |
| 5 | The filter surface is open: any column is filterable, including `system` ones | [INV-P15](07-invariants.md#inv-p15) | **S1** |
| 6 | Scalar string filters are not case-folded; `inList` filters are | [INV-P8](07-invariants.md#inv-p8) | **S1** |
| 7 | `inList` silently drops values it cannot parse | [INV-Q12](07-invariants.md#inv-q12) | **S1** |
| 8 | 26 `prod_pattern_*` e2e tests have no fixtures on disk and report green | §8.1 | **S1** |
| 9 | `tron` dataset is entirely absent | [03-catalog.md §3.4](03-catalog.md) | **S2** |
| 10 | Five of six Substrate aliases are missing | [03-catalog.md §3.3](03-catalog.md) | **S2** |
| 11 | EVM `blocks` is missing 5 selectable fields | [03-catalog.md §3.1](03-catalog.md) | **S2** |
| 12 | EVM `transactions` is missing 4 selectable fields | [03-catalog.md §3.1](03-catalog.md) | **S2** |
| 13 | Solana `d1/d2/d4/d8` are not selectable; no `hexNumber` encoding | [INV-O9](07-invariants.md#inv-o9) | **S2** |
| 14 | Negative values cannot filter signed columns | [INV-P14](07-invariants.md#inv-p14) | **S2** |
| 15 | `discriminator: []` errors instead of matching nothing | [INV-P3](07-invariants.md#inv-p3) | **S3** |
| 16 | Bloom ≤ 10 and discriminator-exclusivity validations are absent | [INV-Q10](07-invariants.md#inv-q10), [INV-Q11](07-invariants.md#inv-q11) | **S3** |
| 17 | Relation block-number collection hardcodes the name `block_number` | [INV-X1](07-invariants.md#inv-x1) | **S3** |
| 18 | Arrow output and typed encoders panic on unexpected input | [INV-E1](07-invariants.md#inv-e1) | **S3** |
| 19 | Catalog validation covers a subset of the required checks | [INV-D1](07-invariants.md#inv-d1)–[INV-D10](07-invariants.md#inv-d10) | **S3** |
| 20 | A `timestampMillisecond` column with no declared encoding is divided by 1000 | [INV-O9](07-invariants.md#inv-o9) | **S4** |
| 21 | `base58` encoding is a no-op; a physically-`Binary` column would emit hex | [INV-O9](07-invariants.md#inv-o9) | **S4** |
| 22 | Weight accumulation is unchecked; a negative size column yields ~`u64::MAX` | [INV-B9](07-invariants.md#inv-b9) | **S4** |
| 23 | No request byte cap and no `inList` length cap | [INV-Q13](07-invariants.md#inv-q13) | **S4** |

---

## S1 — Silent wrong results

### 1. Filtering on an absent column matches every row

`src/scan/predicate.rs:819` — `RowPredicate::evaluate` skips a column predicate
whose column is not in the batch, and a predicate with no evaluable columns
returns an all-true mask.

A chunk written before `sighash` existed, queried with
`{"transactions":[{"sighash":["0xa9059cbb"]}]}`, returns **every transaction in
the chunk**. The client sees a `200`, a plausible body, and no way to tell.

The reference implementation raises `missing column: sighash`. The engine already
hard-errors for a *selected* column absent from the parquet; the same check is
simply not applied to *filtered* columns.

This is the highest-severity item in the document. Fix: resolve every predicate
column against the chunk schema before scanning, and fail with `ColumnNotFound`.

### 2. `parentBlockHash` is accepted and ignored

`src/query/parse.rs:57` lists `parentBlockHash` among the known top-level keys.
Nothing ever reads it.

The reference implementation walks back up to 100 blocks from `fromBlock`, finds
the parent, compares hashes, and fails with `UnexpectedBaseBlock` — carrying the
recent block hashes — on mismatch. That is how a client detects that the chain
reorganised between two pages.

Today, a client that supplies `parentBlockHash` after a reorg is served data from
a chain it did not ask about, with no indication.

### 3. Unknown field names are silently dropped

`src/query/parse.rs:198` keeps only the keys whose value is exactly `true` and
never checks that the key names a real field. `{"fields":{"log":{"logIndx":true}}}`
returns a `200` and logs without `logIndex`.

The reference implementation uses `deny_unknown_fields` on every selection struct
and rejects it.

### 4. Malformed block bounds are coerced

`src/query/parse.rs:80-81` — `fromBlock` falls back to `0` and `toBlock` to
unbounded whenever `as_u64()` fails. `{"fromBlock": "18000000"}` (a string, a
common client bug) silently scans from block zero.

### 5. The filter surface is open

`src/query/parse.rs` resolves an item-request key against, in order, the table's
relations, the alias's filter aliases, the table's special filters, and then
**any column of the table**. There is no declared filter list.

Consequences:

- `system` columns are filterable: `{"logs":[{"dataSize":[100]}]}`,
  `{"transactions":[{"accountsBloom":["0x…"]}]}`,
  `{"events":[{"_evmLogAddress":["0x…"]}]}` are all accepted.
- The catalog's column list becomes the public API. Adding a column adds a filter.
- The reference implementation's closed allowlist means these queries are
  rejected. Any client relying on the open surface is relying on a bug.

Fix: add a `filters:` declaration per table to the catalog and resolve against it.
This is also the natural home for the `hexBytes` case-folding flag, the
`columnAlias` mappings that already exist as `special_filters`, and the closed set
[03-catalog.md](03-catalog.md) specifies.

### 6. Case-folding is inconsistent

`src/query/plan.rs:418` lowercases `inList` values when the column's
`json_encoding` is `hex`. The scalar-string path (`as_str` → equality) does not.

`{"to": ["0xA0B8…"]}` matches; `{"to": "0xA0B8…"}` does not. The two should mean
the same thing ([INV-P8](07-invariants.md#inv-p8)).

Separately, the current rule folds *every* hex-encoded column, where the
reference folds a specific list. The two agree on all catalog columns today
(`statediffs.key` and `traces.type` are not hex-encoded, and so are not folded in
either), but the rule should be stated and tested rather than coincidental.

### 7. `inList` silently drops unparseable values

`compile_in_list` (`src/query/plan.rs:409`) filters values through
`parse_hex(...).filter(|b| b.len() == N)`. A list of two discriminators, one with
the wrong byte length, silently becomes a filter on one value. A list where *every*
value is unparseable becomes an empty `inList`, which matches nothing — the right
answer for the wrong reason, indistinguishable from a correctly-empty filter.

`ColumnType::ListUInt32` additionally truncates with an unchecked `n as u32`.

Per [INV-Q12](07-invariants.md#inv-q12), a malformed value is an error. Per
[INV-P14](07-invariants.md#inv-p14), a well-formed but out-of-range value matches
nothing. The current code cannot distinguish the two.

### 8. 26 e2e tests report green with no fixtures

`tests/e2e_fixtures.rs` prints `SKIP` and returns when a fixture's chunk or
`query.json` is absent. The test passes.

Verified: `ls tests/fixtures/*/queries/ | grep -c prod_pattern` → **0**. All 26
macro-generated `prod_pattern_*` tests (11 Solana, 15 EVM) — the ones derived from
real production traffic — have never run.

Fix: assert an expected fixture count per dataset, and fail on a shortfall. §8.8
rule 2.

---

## S2 — Missing capability

### 9. `tron` is absent

No metadata, no fixtures, no mention anywhere in `src/` or `metadata/`. The
reference implementation serves it: 4 tables, 6 item request arrays including 3
aliases (`transferTransactions`, `transferAssetTransactions`,
`triggerSmartContractTransactions`), and the `internal_transactions` table whose
`queryName` is `internalTransactions`.

See [03-catalog.md §3.4](03-catalog.md).

### 10. Five of six Substrate aliases are missing

`metadata/substrate.yaml` declares only `evmLogs`.

| Alias | Status |
|---|---|
| `evmLogs` | present |
| `ethereumTransactions` | **missing** — the `_ethereum_transact_to` / `_ethereum_transact_sighash` columns exist and are unreachable |
| `contractsEvents` | **missing** — `_contract_address` exists and is unreachable |
| `gearMessagesEnqueued` | **missing** — `_gear_program_id` exists and is unreachable |
| `gearUserMessagesSent` | **missing** — same column, different implicit predicate |
| `reviveContractEmitted` | **missing** — `_revive_contract`, `_revive_topic0…3` do not exist at all |

The first four are catalog edits. `reviveContractEmitted` needs columns added.

### 11–12. Missing EVM columns

`metadata/evm.yaml` omits, versus the reference's selectable field lists:

- `blocks`: `uncles`, `withdrawals`, `withdrawals_root`, `parent_beacon_block_root`, `requests_hash`
- `transactions`: `access_list`, `logs_bloom`, `blob_gas_used`, `blob_gas_price`

`withdrawals` and `access_list` also carry weight columns (`withdrawals_size`,
`access_list_size`) in the reference's weight model, so adding them changes block
weights and hence where truncation lands.

A query selecting any of these fields today gets a `200` with the field silently
absent, by gap 3. After gap 3 is fixed it will get `UnknownField` — which is at
least loud, but still wrong.

### 13. Solana discriminator columns are not selectable

`metadata/solana.yaml` marks `d1`…`d16` as `system: true`, so they are excluded
from output and from weight.

The reference implementation exposes `d1`, `d2`, `d4`, `d8` as selectable fields
of `instructions`, encoded as `hexNumber`: zero-padded to the column's width, so a
`uint16` `d2` of 1600 renders `"0x0640"`.

There is no `hexNumber` encoding in the engine at all. A column being read by a
filter does not make it a `system` column — the two concerns are independent, and
the catalog conflates them.

### 14. Negative values cannot filter signed columns

The scalar filter path (`src/query/plan.rs`) has `as_bool`, `as_u64` and `as_str`
branches and no `as_i64`. `compile_in_list` has no `Int16` or `Int64` arm. A
negative filter value falls through to
`"invalid filter value for '<c>': expected array, boolean, number, or string"`.

Solana `transactions.version` is `int16` and holds `-1` for legacy transactions.
`rewards.lamports` is `int64` and is routinely negative. Neither is filterable in
the reference implementation either, so no client depends on it — but the
catalog permits it (gap 5), and once the filter surface is closed this becomes a
deliberate choice rather than an accident.

---

## S3 — Loud failures

### 15. `discriminator: []` errors

`compile_discriminator` raises `"discriminator list is empty"`. Per
[INV-P3](07-invariants.md#inv-p3) an empty list matches nothing, uniformly, for
every filter kind. The reference implementation treats it as unsatisfiable and
drops the item request.

### 16. Missing request validations

- Bloom filters accept any number of values. The reference caps at 10
  ([INV-Q10](07-invariants.md#inv-q10)); each value is a separate hash-and-probe
  over every row.
- An item request may carry `d1` and `d8` simultaneously. The reference rejects
  it ([INV-Q11](07-invariants.md#inv-q11)); the engine silently lets the last one
  win, or ANDs them, depending on iteration order.

### 17. Relation block numbers are looked up by hardcoded name

`src/output/weight.rs` collects block numbers from relation batches under the
literal column name `"block_number"`. Every current item table happens to use
that name, and the blocks table (`number`) is handled separately — so it works
today by coincidence.

A catalog with a differently-named block column on an item table drops those rows
from the weight model and from block selection. This is a direct violation of
[INV-X1](07-invariants.md#inv-x1): the engine knows a column name it should have
read from the catalog.

### 18. Panics on unexpected input

- `src/output/arrow_out.rs` — 6 `.expect()` calls on the prototype path.
- The `solana_tx_version` and timestamp encoders `.unwrap()` their downcast: a
  physical type drift turns into a panic in a worker serving many queries.
- `src/metadata/bundled.rs` panics on a catalog parse failure. Acceptable at
  startup, but only if the catalog is validated at build time (gap 19).

[INV-E1](07-invariants.md#inv-e1) admits no exceptions.

### 19. Catalog validation is partial

`loader::validate` checks `block_number_column`, `item_order_keys`, `sort_key`,
`weight` sources and `children`. It does not check: relation targets and keys,
special-filter columns, field-group tag and mapped columns, virtual-field roll
columns, `address_column`, `parent_key`, aliases, or `query_name` / `field_name`
uniqueness.

Each unchecked item is a deploy-time landmine. An alias pointing at a missing
table panics on the first query that uses it. A typo in a `tag_column` silently
drops every variant field. A typo in a roll column silently shortens every array.

[INV-D1](07-invariants.md#inv-d1), [INV-D2](07-invariants.md#inv-d2),
[INV-D5](07-invariants.md#inv-d5), [INV-D6](07-invariants.md#inv-d6),
[INV-D10](07-invariants.md#inv-d10) enumerate the full check list. §8.2 describes
how to test the validator itself.

Related: the block table is found by the heuristic *"first table whose
`sort_key[0]` equals its `block_number_column` and whose `item_order_keys` is
empty, else the table named `blocks`"*. [INV-D3](07-invariants.md#inv-d3) makes it
a declared property.

---

## S4 — Latent

### 20. Timestamp scaling depends on a declared encoding

A column of type `timestamp_millisecond` with no `json_encoding` is divided by
1000 and emitted as seconds. Every current catalog entry sets
`json_encoding: timestamp_millisecond`, so the bug is invisible — until someone
adds a column and forgets.

[INV-O9](07-invariants.md#inv-o9): the *declared type* determines the unit.

### 21. `base58` and `hex` encodings are no-ops

Both fall through to the physical-type encoder. They work because every such
column is physically `Utf8` holding the display string already. A base58 column
stored as `Binary` would emit `0x…` hex. The encoding declarations do carry
meaning elsewhere (case-folding on input, Arrow binary decoding), so they cannot
simply be removed — they need to actually drive encoding.

### 22. Unchecked weight arithmetic

`src/output/weight.rs` uses `+=` and reads `Int64` size columns as `value as u64`.
A negative size (a corrupt chunk) becomes ~`u64::MAX`, and the block is dropped or
the accumulator wraps. Saturating arithmetic and a rejected-negative check both
belong here.

### 23. No request size bound

The only guard is the 100-item-request cap. One hundred item requests, each with
an `inList` of a million addresses, is well-formed and builds a hundred
million-element hash sets before any data is read.
[INV-Q13](07-invariants.md#inv-q13).

---

## Deliberate divergences from the reference

Cases where the spec sides with the engine, or with neither, and the reference is
the one that is wrong. Listed so nobody "fixes" them back.

| Behaviour | Reference | Spec | Reason |
|---|---|---|---|
| Item-request cap counting | `hyperliquidReplicaCmds` counts only the `actions` array; its four aliases are uncounted | Count every item request, uniformly ([INV-Q5](07-invariants.md#inv-q5)) | An uncounted alias is an unbounded scan. |
| Substrate `event.callAddress` | Emitted **twice** in the item object — the projection lists the column twice | Emitted once ([INV-O7](07-invariants.md#inv-o7)) | A duplicate JSON key is undefined behaviour for most parsers. |
| Empty result | `[]` (array writer) or empty (lines writer) | Zero bytes ([INV-O1](07-invariants.md#inv-o1)) | NDJSON is the only format; zero bytes concatenates. |
| Response field order | Declaration order in the query DSL | Catalog column order ([INV-O6](07-invariants.md#inv-o6)) | Both are stable; the catalog is the single source of truth. Values are equal, bytes are not — the parity suite must compare values, not bytes. |
| String escaping | `\u`-escapes non-ASCII | Raw UTF-8 | Both valid JSON. Same reason as above. |
| Fuel `ContractOutput.stateRoot` | Reads the column `state_root`, not `contract_state_root` | *Unresolved* | Preserved as a catalog quirk pending a check against real Fuel chunks. Flagged so it is not silently "corrected" into a regression. |

## Extensions beyond the reference

Permitted; must not change the meaning of anything above.

- A `substrate.extrinsics` item request array. The reference reaches extrinsics
  only through relations.
- A columnar (Arrow IPC) response encoding, subject to
  [INV-O14](07-invariants.md#inv-o14).
- `evm.traces.rewardValueNonZero` — present in both, listed here because it is
  easy to miss among the other three `*NonZero` flags.

---

## Suggested order of work

1. **Gap 1** (absent filtered column → match everything). One check, and it is the
   only entry that can hand a client the wrong answer with no way to notice.
2. **Gaps 3, 4, 7** (silent drops and coercions). Cheap, and each converts a
   silent wrong answer into an error.
3. **Gap 8** (skipped tests). Until this is fixed the suite cannot tell you
   whether anything else is fixed.
4. **Gap 5** (closed filter surface) + **gap 19** (catalog validation). These are
   one change: introduce `filters:` in the catalog, and validate the whole catalog
   properly. Everything in [03-catalog.md](03-catalog.md) becomes checkable.
5. **Gap 2** (fork detection). Needed before any client pages across a reorg.
6. **Gaps 9–13** (missing surface). Mechanical catalog work, gated on 4.
7. The rest.
