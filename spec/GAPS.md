# Gap analysis — implementation vs. specification

**Non-normative.** This document records where the engine in this repository
currently diverges from [the specification](README.md). It is kept out of the
normative chapters on purpose: a spec that describes today's bugs cannot be used
to find them.

Delete an entry when the gap closes. If the spec turns out to be wrong and the
implementation right, fix the spec and delete the entry. The document should tend
toward empty.

Compared against the reference implementation, as of 2026-09-04.

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
| 31 | Twelve item types order their fields differently from the reference | [INV-O6](07-invariants.md#inv-o6) | **S4** |
| 32 | No test can see field order, so nothing falsifies INV-O6 or INV-O12 | [INV-O12](07-invariants.md#inv-o12) | **S4** |

There are no open S1, S2 or S3 entries. Every dataset
[chapter 3](03-catalog.md) names is served except `fuel`, which is out of scope
([ADR-10](decisions/ADR-10-fuel-is-out-of-scope.md)). Nothing the reference
answers is refused here except by the bounds the table below says are
deliberate.

The two entries above are one discovery read twice. Field order was recorded as
a deliberate divergence on the strength of a claim about the reference that was
not true ([ADR-5](decisions/ADR-5-catalog-order-for-output-fields.md)), and the
reason nobody noticed is gap 32: no suite on either side compares field order,
so the claim was never going to be contradicted by a failing test.

The three entries before them were missing surface rather than wrong behaviour,
and each closed differently:

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

Past the two entries above, the next work is not in this document; the section
after them says where it is instead.

---

## S4 — Robustness, hygiene, latent

### 31. Twelve item types order their fields differently from the reference

[INV-O6](07-invariants.md#inv-o6) takes field order from the catalog. The
reference takes it from a list written by hand in each item type's projection.
Both are fixed and neither reads the request, so the two can be made to agree
field for field.

Of the 28 item types both engines serve, 16 already agree. Twelve do not, and
each differs by a local transposition:

| Item type | Reference | Catalog |
|---|---|---|
| `evm.blocks` | …`baseFeePerGas, uncles, withdrawals, withdrawalsRoot, blobGasUsed, excessBlobGas` | …`baseFeePerGas, blobGasUsed, excessBlobGas, uncles, withdrawals, withdrawalsRoot` |
| `evm.transactions` | …`s, yParity, accessList, chainId`… | …`s, yParity, chainId`…`accessList`… |
| `solana.transactions` | …`err, fee, computeUnitsConsumed`… | …`err, computeUnitsConsumed, fee`… |
| `solana.instructions` | …`programId, accounts, data`… | …`programId, data`…`accounts`… |
| `substrate.blocks` | …`implVersion, validator, timestamp` | …`implVersion, timestamp, validator` |
| `substrate.extrinsics` | `index, version, success, hash, fee, tip, signature` | `index, version, signature, fee, tip, error, success` |
| `substrate.calls` | `address, name, success, args, origin, error` | `address, name, args, origin, error, success` |
| `substrate.events` | `index, extrinsicIndex, callAddress, name, phase, topics` | `index, name, args, phase, extrinsicIndex, callAddress` |
| `tron.blocks` | …`version, witnessAddress, witnessSignature, timestamp` | …`version, timestamp, witnessAddress, witnessSignature` |
| `tron.transactions` | `transactionIndex, hash, signature, permissionId`… | `transactionIndex, hash, ret, signature, parameter, permissionId`… |
| `tron.logs` | `transactionIndex, logIndex, address, data, topics` | `logIndex, transactionIndex, address, data, topics` |
| `tron.internalTransactions` | …`transferToAddress, note, rejected, extra, callValueInfo` | …`transferToAddress, callValueInfo, note, rejected, extra` |

The list was read off the reference's projection macros mechanically and has been
spot-checked, not verified field by field. Confirming it needs the byte-level
oracle gap 32 describes; until that exists the table is a starting point rather
than a diff.

Nothing here is a wrong answer: JSON object order carries no meaning, and every
value and every key is the same on both sides. What it costs is
interchangeability. Two engines that agree on every value still return different
bytes, so anything that identifies a response by hashing it — attribution,
caching, any check that two servers answering the same question agree — must
treat them as different answers. Closing the gap is twelve catalog edits; the
decision to make them is [ADR-5](decisions/ADR-5-catalog-order-for-output-fields.md)'s
to revisit, not this document's.

### 32. No test can see field order, so nothing falsifies INV-O6 or INV-O12

The fixture suite parses each response into a JSON value before comparing, and
value equality ignores key order. Transposing two columns in any catalog changes
the bytes of every response that selects them, and the suite stays green. The
reference's own fixture test does the same thing, which is why the goldens under
`tests/fixtures/` disagree with each other about order: they were written at
different times by different versions, and nothing has ever compared them by
order. They are evidence of values, not of bytes.

So [INV-O6](07-invariants.md#inv-o6) rests on reading the code, and
[INV-O12](07-invariants.md#inv-o12) on nothing at all — as
[§8.11](08-conformance.md#811-traceability-matrix) already records for O12 and
[INV-O13](07-invariants.md#inv-o13).

Three things close it, cheapest first. An order-preserving reader and a
recursive key-sequence assertion give the suite eyes. Running one query twice and
at several thread counts and asserting byte equality discharges O12 and O13,
which hold today — measured, not assumed — but are guarded by nothing. And a
catalog with two columns transposed, asserted to change the output, is the
anti-vacuity guard without which the first two can pass while comparing nothing.

---

## Open work outside this register

This document holds behavioural divergences only — places where the engine and
the spec disagree about an answer. Most of what is open is not that. It is work
the other registers already track, and it is deliberately not copied here: a copy
rots, and the checker recomputes the originals.

This section is a reading order, current as of the date at the top.

**Invariants nothing would notice breaking** —
[§8.11](08-conformance.md#811-traceability-matrix). Twelve of the eighty-five sit
at **U**, and another thirty at **P**. Two are worth naming outside the table:

- [INV-P9](07-invariants.md#inv-p9). The engine's bloom construction is never
  checked against the archive writer's. A disagreement drops matching rows, and
  the client receives a short answer indistinguishable from a correct one. It is
  the only unchecked invariant that can be wrong silently, which makes it the
  most dangerous row in the matrix.
- [INV-B8](07-invariants.md#inv-b8). Partition invariance — the law that lets a
  chunk be answered alone, and therefore the one the distributed architecture
  rests on. §8.6 calls testing it the single most valuable thing the suite could
  do, partly because it exercises five other invariants on the way.

**Capabilities the harness lacks** —
[§8.13](08-conformance.md#813-harness-capability-register). A query generator
(HC-4) and a seeded fuzzer (HC-7) do not exist; the chunk writer (HC-3) is half
built. HC-4 is what turns the algebraic laws — P5, R2, R4, R11, B8 — from prose
into tests, since no hand-written case can state a law over pairs of queries.
HC-3 unblocks the storage-shape invariants. Between them they account for most of
the **U** column.

**Gates that do not yet gate** —
[§8.12](08-conformance.md#812-merge-gates). Three of the eight block a merge:
the portable test classes, spec integrity, and the formatter and linter. The
other five are advisory, each waiting on a capability above rather than on a
decision.

**Thresholds nobody has ratified** —
[§9.4](09-parameters.md#94-merge-gate-thresholds), five values carrying ⚠ under
[ADR-12](decisions/ADR-12-unratified-thresholds-ship-as-proposals.md). All five
are gate thresholds. None of them is engine behaviour, so nothing on the wire
depends on how they are settled.

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
| Where response field order comes from | A second fixed order, listed by hand in the projection macro beside each schema | The catalog's own column order ([INV-O6](07-invariants.md#inv-o6), [ADR-5](decisions/ADR-5-catalog-order-for-output-fields.md)) | Neither engine consults the request; both emit a fixed order. Only the *source* of that order is a decision here. The two orders are meant to agree field for field, and mostly do — the twelve item types where they do not are gap 31, drift rather than intent. |
| String escaping | `\u`-escapes non-ASCII | Raw UTF-8 | Both are valid JSON carrying the same string. A comparison of parsed values sees no difference; a byte comparison must decode one side first. |
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
