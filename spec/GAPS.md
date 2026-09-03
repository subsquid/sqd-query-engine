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

**The register is empty.** Every dataset [chapter 3](03-catalog.md) names is
served except `fuel`, which is out of scope
([ADR-10](decisions/ADR-10-fuel-is-out-of-scope.md)). Nothing the reference
answers is refused here except by the bounds the table below says are
deliberate.

The last three entries were missing surface rather than wrong behaviour, and each
closed differently:

- **`tron`** was absent outright — no catalog, no tests, ten fixture query
  directories sitting on disk that nothing read. It is now a catalog file and
  ten fixture tests, and its parquet needed no engine change beyond the two
  below.
- **Five of six Substrate aliases** were missing, which left their extraction
  columns in every chunk and unreachable from any query.
  `reviveContractEmitted` needed five columns the catalog never declared. Adding
  the four aliases over `calls` and `events` exposed a second thing: the relation
  surface was read as the *union* of an alias's relations and its table's, so
  `ethereumTransactions` would have inherited `subcalls` from `calls`. An alias
  is a narrower view or it is nothing ([INV-Q6](07-invariants.md#inv-q6)), so it
  now declares its own.
- **Negative filter values** could not reach a signed column: the scalar path
  read `as_u64` and nothing else, and `compile_in_list` had no signed arm. Both
  now go through one `i128` widening, which is the only representation that holds
  a `uint64` above `i64::MAX` and a negative in the same list.

Closing them moved [INV-P14](07-invariants.md#inv-p14) from partial to **C** —
the signed floor and the block-bound half of it both have tests now — and left
[INV-P8](07-invariants.md#inv-p8), [INV-Q6](07-invariants.md#inv-q6) and
[INV-P15](07-invariants.md#inv-p15) covered from one more direction each.

The next work is not in this document. It is the capability gaps in
[§8.13](08-conformance.md#813-harness-capability-register) — a chunk writer and a
query generator — which are what stand between the suite and the fourteen
invariants nothing would notice breaking, [INV-P9](07-invariants.md#inv-p9)
first among them.

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
| Signed filter values | No signed arm either; `transactions.version` and `rewards.lamports` are unfilterable | Filterable where a catalog declares it ([INV-P14](07-invariants.md#inv-p14)) | No bundled catalog declares such a filter, so nothing changes on the wire. What changed is that refusing one is now a catalog decision rather than a hole in the compiler. |
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
