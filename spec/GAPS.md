# Gap analysis — implementation vs. specification

**Non-normative.** This document records where the engine in this repository
currently diverges from [the specification](README.md). It is kept out of the
normative chapters on purpose: a spec that describes today's bugs cannot be used
to find them.

Delete an entry when the gap closes. If the spec turns out to be wrong and the
implementation right, fix the spec and delete the entry. The document should tend
toward empty.

Compared against the reference implementation, as of 2026-09-02.

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
| 9 | `tron` dataset is entirely absent | [03-catalog.md §3.4](03-catalog.md) | **S2** |
| 10 | Five of six Substrate aliases are missing | [03-catalog.md §3.3](03-catalog.md) | **S2** |
| 14 | Negative values cannot filter signed columns | [INV-P14](07-invariants.md#inv-p14) | **S2** |
| 20 | A `timestampMillisecond` column with no declared encoding is divided by 1000 | [INV-O9](07-invariants.md#inv-o9) | **S4** |
| 21 | `base58` encoding is a no-op; a physically-`Binary` column would emit hex | [INV-O9](07-invariants.md#inv-o9) | **S4** |
| 22 | Weight accumulation is unchecked; a negative size column yields ~`u64::MAX` | [INV-B9](07-invariants.md#inv-b9) | **S4** |
| 23 | No request byte cap and no `inList` length cap | [INV-Q13](07-invariants.md#inv-q13) | **S4** |
| 27 | The output field surface is open: every non-`system` column is selectable | [INV-Q14](07-invariants.md#inv-q14) | **S4** |
| 28 | Errors carry prose, not machine-readable kinds | [INV-E6](07-invariants.md#inv-e6) | **S4** |
| 29 | No job runs the test, lint or format gates | [08-conformance.md §8.12](08-conformance.md#812-merge-gates) | **S4** |
| 30 | The fork window is inherited, not derived | [INV-E5](07-invariants.md#inv-e5) | **S4** |

There are no open S1 or S3 entries. Every silent-wrong-answer gap and every
crash-where-a-result-was-due recorded here has been closed, with a test pinning
the invariant behind it.

Closing the S3 set moved four invariants to **C** in the traceability matrix
([INV-D3](07-invariants.md#inv-d3), [INV-D10](07-invariants.md#inv-d10),
[INV-Q10](07-invariants.md#inv-q10), [INV-Q11](07-invariants.md#inv-q11)) and
raised [INV-D1](07-invariants.md#inv-d1), [INV-D6](07-invariants.md#inv-d6) and
[INV-P3](07-invariants.md#inv-p3) to **C** from partial. It did not close
[INV-E1](07-invariants.md#inv-e1): the panics gap 18 named are gone and a chunk
that disagrees with its catalog is now a test, but the fuzz sweep the invariant
asks for still needs a chunk writer
([HC-3](08-conformance.md#813-harness-capability-register)).

`fuel` is out of scope ([03-catalog.md §3.6](03-catalog.md)); the one gap this
document carried against it is gone with it, and its catalog file with it, so the
engine no longer serves the dataset. The Fuel fixtures are still in the fixture
tree and no test reads them. [ADR-10](decisions/ADR-10-fuel-is-out-of-scope.md)
records why.

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

### 27. The output field surface is open

`is_selectable_field` answers "is this a non-`system` column, a virtual field, or
a field-group request key". That is the *derivation* [03-catalog.md](03-catalog.md)
describes as a convenience, used as the definition — so every column the catalog
carries for filtering, grouping, joining or rolling is also a selectable output
field. [INV-Q14](07-invariants.md#inv-q14) requires the declared list instead.

Measured against the reference implementation, the engine accepts these extra
field names:

| Where | Extra names | Why the catalog carries the column |
|---|---|---|
| every item table of every dataset (16 tables) | `blockNumber` | grouping, joining, ordering; already in `header.number` |
| `evm.logs` | `topic0`, `topic1`, `topic2`, `topic3` | filter columns, rolled into `topics` |
| `solana.instructions` | `a0`…`a15`, `restAccounts` | filter columns, rolled into `accounts` |
| `solana.transactions` | `costUnits` | no reference field |
| `hyperliquidReplicaCmds.actions` | `actionType` | filter column |

Note that `solana.instructions.d1/d2/d4/d8` are *not* in this list. They are
filter columns and selectable fields both, deliberately
([03-catalog.md §3.2](03-catalog.md)) — which is why `system` cannot be the
discriminator and the surface has to be declared.

Nothing returns a wrong answer today, which is why this is S4 and not S2: the
engine accepts a superset of what it should. It matters because the superset is
"whatever columns the archive happens to have". A client that pins `topic0`
today breaks the day the archiver stops writing it, on a field the catalog never
promised.

Marking the columns `system` is not the fix — a `system` column contributes zero
weight ([INV-D9](07-invariants.md#inv-d9)), so hiding `topic0…3` that way would
silently drop the `topics` roll out of the weight model.

*First test:* `{"fields":{"log":{"blockNumber":true}}}` and
`{"fields":{"log":{"topic0":true}}}` must both raise `UnknownField`.

### 28. Errors carry prose, not kinds

[06-errors.md](06-errors.md) names sixteen error kinds and
[INV-E6](07-invariants.md#inv-e6) requires clients to switch on them. The engine
has one typed error — `UnexpectedBaseBlock` — and returns everything else as a
formatted message: `"unknown table filter '{}' in query"`, `"'{}' must be an
unsigned integer, got {}"`, and so on.

The reference implementation is the same, so this is not a regression; the
taxonomy is a contract the specification proposes and neither engine implements.
It is recorded here rather than removed from §6 because the alternative — a
client library string-matching on messages — is worse, and because the taxonomy
is already the structure the request-validation tests are written against
([CT-2](08-conformance.md#83-ct-2--request-validation) asserts the *kind*, which today it cannot).

Mechanically small: one enum over the §6.2 and §6.3 rows, carried alongside the
message. The work is in the call sites, not the design.

*First test:* the [CT-2](08-conformance.md#83-ct-2--request-validation) table asserts a kind per
row rather than a substring.

### 29. No job runs the test, lint or format gates

The repository's only CI job runs the spec checker. `make test`, `make lint` and
`make fmt` exist and pass nothing on: a change that breaks the suite, trips the
linter or is unformatted merges green.

So [MG-3](08-conformance.md#812-merge-gates) and
[MG-8](08-conformance.md#812-merge-gates) are advisory, and
[HC-12](08-conformance.md#813-harness-capability-register) is **P** rather than
**C** — the commands are built, the wiring is not. Closing this is a workflow
file and a decision about what the format gate does with the code that does not
satisfy it today.

*First test:* a job that runs `make test` and fails on a deliberately broken test.

---

### 30. The fork window is inherited, not derived

`P-FORK-WINDOW` is a fixed 100 blocks. [§2.2](02-request.md) asks for a window at
least as wide as the largest gap the dataset's block numbering permits, and
nothing derives it, records it per dataset, or checks it. A dataset that skips
more than 100 numbers in a row loses fork detection at that point and says
nothing — the failure [INV-E5](07-invariants.md#inv-e5) exists to prevent.

The reference implementation carries the same constant and a standing note about
it; the divergence table below records that the spec names the defect rather than
inheriting it. This entry is the engine's own side of it.

*First test:* a chunk whose block numbers skip more than the window, asserting
the parent is still found or the request is refused.

---

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
| `parentBlockHash` when `fromBlock` is outside the chunk | Errors: *"block N is not present in the chunk"* | Skip the check ([INV-E5](07-invariants.md#inv-e5)) | A chunk that cannot see the block is not evidence of a fork. The reference turns a routing artefact into a client-visible failure. |
| Fork search window | Fixed 100 blocks, with a standing FIXME that a longer gap in block numbering misses the parent | A window wide enough for the dataset's largest numbering gap ([§2.2](02-request.md)) | Same defect, named rather than inherited. Both implementations currently use 100. |

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

1. **Gaps 9, 10** (missing surface). Mechanical catalog work, and the validator
   now checks everything a new catalog entry can get wrong.
2. **Gap 27** (open field surface). Mechanical once the per-dataset lists in
   [03-catalog.md](03-catalog.md) are in the catalog: 16 tables, one declared
   list each, and [INV-Q14](07-invariants.md#inv-q14) becomes checkable in a loop.
3. **Gap 29** (no test, lint or format gate). The suite that pins the closed gaps
   is only worth what CI does with it, and today CI runs the spec checker alone.
4. **Gap 14** (negative filter values), then **gaps 20–23, 28, 30** (latent).
   None is reachable from a well-formed request against a well-formed chunk
   today.
