# 9. Parameters

Every constant the specification depends on, in one place. Normative text names
the symbol; this file resolves it.

**This document is mutable.** Along with [08-conformance.md](08-conformance.md)
it is one of the two files that may change without intended behaviour changing.
Everything else changes only when the contract does. Observed as of **2026-09-03**.

Columns:

- **Observed** — what the engine in this repository does today. Where it
  violates the target, say so and name the gap.
- **Target** — what a conforming engine must do. A ⚠ marks a value nobody has
  ratified yet; it is a proposal until a decision in
  [decisions/](decisions/) accepts it.

---

## 9.1 Request limits

Checked before any chunk data is read ([INV-E2](07-invariants.md#inv-e2)).

| Parameter | Role | Observed | Target |
|---|---|---|---|
| `P-MAX-ITEM-REQUESTS` | Item requests summed across every table and alias ([INV-Q5](07-invariants.md#inv-q5)) | 100 | 100 |
| `P-MAX-BLOOM-VALUES` | Values in one bloom filter ([INV-Q10](07-invariants.md#inv-q10)) | 10 | 10 |
| `P-MAX-DISCRIMINATOR-FILTERS` | Discriminator-family filters in one item request ([INV-Q11](07-invariants.md#inv-q11)) | 1 | 1 |
| `P-MAX-DISCRIMINATOR-BYTES` | Bytes in one discriminator value ([INV-Q12](07-invariants.md#inv-q12)) | 16 | 16 |
| `P-MAX-REQUEST-BYTES` | Whole-request size bound ([INV-Q13](07-invariants.md#inv-q13)) | 2 MiB | 2 MiB |
| `P-MAX-IN-LIST` | Values in one `inList` ([INV-Q13](07-invariants.md#inv-q13)) | 100 000 | 100 000 |

The discriminator family is read from the catalog, not named in code: it is the
`discriminator` special filter plus every column it dispatches to. A dataset with
a differently-named discriminator is bounded by the same rule with no code change
([INV-X1](07-invariants.md#inv-x1)).

`P-MAX-REQUEST-BYTES` bounds parsing work and total request representation.
`P-MAX-IN-LIST` independently bounds the set built for one filter, including the
case where short values allow a large list to fit below the byte cap
([ADR-13](decisions/ADR-13-request-resource-bounds.md)).

## 9.2 Response bounds

| Parameter | Role | Observed | Target |
|---|---|---|---|
| `P-WEIGHT-BUDGET` | Cumulative block weight a response may carry ([INV-B6](07-invariants.md#inv-b6)) | 20 MiB | 20 MiB |
| `P-DEFAULT-COLUMN-WEIGHT` | Weight of a column the catalog gives no weight (§5.4) | 32 | 32 |

Both match the reference implementation exactly. `P-WEIGHT-BUDGET` is a *model*
bound, not a byte count ([INV-B9](07-invariants.md#inv-b9)); changing it changes
where responses truncate, not whether they are correct. A conformance harness
needs to drive it down to page a fixture chunk end to end, so it must be
injectable rather than compiled in.

## 9.3 Fork detection

| Parameter | Role | Observed | Target |
|---|---|---|---|
| `P-FORK-WINDOW` | Recent `(blockNumber, hash)` pairs an `UnexpectedBaseBlock` carries, as a span of block numbers behind `fromBlock` (§2.2, [INV-E5](07-invariants.md#inv-e5)) | 100 | 100 |

This parameter no longer decides whether the parent is found, so there is nothing
to derive it from. The row *at* `fromBlock` states its own parent's hash, and the
search is anchored there; the window behind it only sizes the evidence a client
gets for locating the fork point. A dataset whose numbering skips further than the
window returns fewer pairs, and still answers.

The reference implementation searches over parent numbers instead, and carries a
standing note that a longer gap misses the parent. That divergence is recorded in
[GAPS.md](GAPS.md).

## 9.4 Merge-gate thresholds

Used by the gates in [08-conformance.md](08-conformance.md). All of these ratchet
upward only: a change may raise them, never lower them.

| Parameter | Role | Observed | Target |
|---|---|---|---|
| `P-COV-PROPERTY` | Fraction of invariants at status **C** in the traceability matrix ([MG-1](08-conformance.md#812-merge-gates)) | 0.71 (60 of 85) | ⚠ 0.71, then ratchet |
| `P-COV-DIFF` | Line coverage of lines a change touches ([MG-2](08-conformance.md#812-merge-gates)) | unmeasured | ⚠ 0.80 |
| `P-COV-TOTAL` | Whole-repository line coverage floor ([MG-2](08-conformance.md#812-merge-gates)) | unmeasured | ⚠ 0.70 |
| `P-FLAKE-RETRIES` | Retries a test gets before it is quarantined ([MG-7](08-conformance.md#812-merge-gates)) | none | ⚠ 1 |
| `P-PERF-NOISE-BAND` | Benchmark movement treated as noise rather than regression ([MG-5](08-conformance.md#812-merge-gates)) | unmeasured | ⚠ 3 % |

`P-COV-PROPERTY` is the one that means something. Line coverage says which
statements ran; property coverage says which promises are checked. The observed
value is counted from the matrix in
[08-conformance.md §8.11](08-conformance.md#811-traceability-matrix), not
estimated.

`P-COV-DIFF` and `P-COV-TOTAL` are unmeasured because no coverage instrumentation
is wired up yet — [HC-9](08-conformance.md#813-harness-capability-register). Their gate is advisory until it
is.

Every ⚠ in this section is a guess, and [ADR-12](decisions/ADR-12-unratified-thresholds-ship-as-proposals.md)
is the decision to publish guesses rather than blanks: a marked proposal says what
is missing, an empty cell says nothing. Each becomes the contract when a decision
names it and the measurement behind it.
