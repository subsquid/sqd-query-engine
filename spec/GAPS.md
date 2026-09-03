# Gap analysis — implementation vs. specification

**Non-normative.** This document records where the engine in this repository
currently diverges from [the specification](README.md). It is kept out of the
normative chapters on purpose: a spec that describes today's bugs cannot be used
to find them.

Delete an entry when the gap closes. If the spec turns out to be wrong and the
implementation right, fix the spec and delete the entry. The document should tend
toward empty.

Compared against the reference implementation, as of 2026-09-03.

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

What is left is missing surface: two datasets' worth of catalog and one filter
value type. There are no open S1, S3 or S4 entries — no silent wrong answer, no
crash where a result was due, and nothing latent that a test does not now hold
in place.

Closing the S4 set moved [INV-Q13](07-invariants.md#inv-q13),
[INV-Q14](07-invariants.md#inv-q14) and [INV-E6](07-invariants.md#inv-e6) from
**U** to **C** in the traceability matrix and
[INV-Q7](07-invariants.md#inv-q7) from partial, and turned
the portable portion of [MG-3](08-conformance.md#812-merge-gates) and
[MG-8](08-conformance.md#812-merge-gates) into blocking gates with a job behind
them ([HC-12](08-conformance.md#813-harness-capability-register)).

It did not close [INV-O9](07-invariants.md#inv-o9) or
[INV-B9](07-invariants.md#inv-b9). Both keep their partial status for what was
never the gap: no test asserts that NaN and ±∞ render as `null`, and nothing
asserts weight is the same function twice over the same chunk.

Closing the S3 set before it moved four invariants to **C**
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

## Deliberate divergences from the reference

Cases where the spec sides with the engine, or with neither, and the reference is
the one that is wrong. Listed so nobody "fixes" them back.

| Behaviour | Reference | Spec | Reason |
|---|---|---|---|
| Request bounds | No byte or list-length bound in the engine | `P-MAX-REQUEST-BYTES` and `P-MAX-IN-LIST`, refused as `RequestTooLarge` ([ADR-13](decisions/ADR-13-request-resource-bounds.md)) | The engine states and enforces its own resource contract. |
| Item-request cap counting | `hyperliquidReplicaCmds` counts only the `actions` array; its four aliases are uncounted | Count every item request, uniformly ([INV-Q5](07-invariants.md#inv-q5)) | An uncounted alias is an unbounded scan. |
| Substrate `event.callAddress` | Emitted **twice** in the item object — the projection lists the column twice | Emitted once ([INV-O7](07-invariants.md#inv-o7)) | A duplicate JSON key is undefined behaviour for most parsers. |
| Empty result | `[]` (array writer) or empty (lines writer) | Zero bytes ([INV-O1](07-invariants.md#inv-o1)) | NDJSON is the only format; zero bytes concatenates. |
| Response field order | Declaration order in the query DSL | Catalog column order ([INV-O6](07-invariants.md#inv-o6)) | Both are stable; the catalog is the single source of truth. Values are equal, bytes are not — the parity suite must compare values, not bytes. |
| String escaping | `\u`-escapes non-ASCII | Raw UTF-8 | Both valid JSON. Same reason as above. |
| `parentBlockHash` when `fromBlock` is outside the chunk | Errors: *"block N is not present in the chunk"* | Skip the check ([INV-E5](07-invariants.md#inv-e5)) | A chunk that cannot see the block is not evidence of a fork. The reference turns a routing artefact into a client-visible failure. |
| Fork search window | Searches back over *parent* numbers, with a standing FIXME that a longer gap in block numbering misses the parent | Anchor the check at `fromBlock`, whose row states its own parent's hash; the window only sizes the evidence ([§2.2](02-request.md)) | A window is a guess about how far back the parent lies. The row that answers is at a known number, so nothing has to be guessed and a numbering gap of any width is answered. |

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
   now checks everything a new catalog entry can get wrong — including the
   declared field list, which a new dataset can no longer omit.
2. **Gap 14** (negative filter values). The one remaining case where a
   well-formed request against a well-formed chunk is refused.

After that the register is empty, and the next work is not in it: the capability
gaps in [§8.13](08-conformance.md#813-harness-capability-register) — a chunk
writer and a query generator — are what stand between the suite and the
invariants it still cannot check.
