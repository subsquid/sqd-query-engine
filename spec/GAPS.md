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
| 31 | A block number above 2³¹ stored in `Int32` is read as negative by the range filter | [INV-D7](07-invariants.md#inv-d7) | **S4** |
| 32 | The bloom's hash function is not pinned by the manifest, and the version it resolves to today ignores the seed above 240 bytes | [INV-P9](07-invariants.md#inv-p9) | **S4** |

Every dataset [chapter 3](03-catalog.md) names is served except `fuel`, which is
out of scope ([ADR-10](decisions/ADR-10-fuel-is-out-of-scope.md)). Nothing the
reference answers is refused here except by the bounds the table below says are
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

Gap 31 arrived the way the rest of this document did not: nobody was looking for
it. The chunk writer that [§8.13](08-conformance.md#813-harness-capability-register)
asks for got finished, and the equality runs it made possible failed four times
over, each on a different reader that knew a different subset of the eight
integer widths — one that dropped a block's header fields, one that dropped a
whole relation, one that emitted items in file order, one that matched no address
at all. Those are fixed, and the readers now share one list of widths. Gap 31 is
what was left over.

Gap 32 arrived the same way one step later: the bloom oracle had to state what
the engine's hash actually is, and stating it exposed that the manifest does not
pin it. Neither gap is reachable by a query anyone writes today, which is what
**S4** means and why the register stays as short as it looks.

The next work is still not in this document. The capability gaps this paragraph
used to point at are closed — the query generator, and after it the bloom oracle
that [INV-P9](07-invariants.md#inv-p9) needed, which turned out to need no
capability of its own. What is left is coverage reporting, and the rows of
[§8.11](08-conformance.md#811-traceability-matrix) whose status rests on prose
that nothing recomputes.

---

## S4 — Latent

### 31. A block number above 2³¹ stored in `Int32` reads as negative

Two places widen a stored block number and they disagree about sign.
`IntColumn::block_number` reinterprets — `(v as u32) as u64` — so a wrapped value
reads back as the block it is, and every reader that resolves a block number
through it agrees. `block_range_mask` in `src/scan/scanner.rs` does not: it
compares through Arrow's kernels at the stored type, so the same value sorts
below every bound and the row group is filtered away.

The visible effect is a response of zero bytes for a range the chunk covers. Not
"the wrong rows" — no rows, and no error, which is indistinguishable from a chunk
that ends before the range starts.

[INV-D7](07-invariants.md#inv-d7) says any integer width and *signedness*, so the
engine is wrong and the spec is right. It is **S4** rather than **S1** because
reaching it needs a writer that keeps `Int32` past 2³¹ instead of widening — the
`UInt32` arm, which any sane writer would pick, is already correct — and because
the chains served are between one and two orders of magnitude below that number.

The fix is not a one-line reinterpretation: a `[from, to]` range that straddles
2³¹ is two disjoint intervals in the signed domain, so the arm needs the straddle
case rather than a different scalar. Writing it as a scalar loop the way
`block_below_mask` does would work and would give up the SIMD kernel on a path
that runs over every row of every block-filtered scan.

*First test:* a chunk whose block numbers start at 2 200 000 000 stored as
`Int32`, queried over its own range, must return the same response as the same
chunk stored as `UInt64`. `physical_width_does_not_reach_the_answer` sweeps every
width already; what it cannot reach is a value that does not fit the width it is
stored at, which needs a writer that wraps rather than widens.


### 32. The bloom's hash function is not pinned, and today's version drops the seed above 240 bytes

`bloom_bit` tells one of a value's seven hashes from another by exactly one
thing: it passes `n` as the seed. XXH3 mixes a seed differently in its two
regimes — directly on inputs up to 240 bytes, and through a secret derived from
it above that — and on the version `Cargo.lock` currently resolves, 0.8.15, the
derivation does not happen. Above the threshold the seed is ignored, so all seven
hashes return one bit and the filter carries one bit per value instead of seven.

Measured against 0.8.18, which does not have it. Below the threshold the two are
byte-identical, including for a real 44-byte account key; at 240 bytes 0.8.15
returns the same bit seven times and 0.8.18 returns seven different ones.

Nothing the engine hashes reaches the threshold. `mentionsAccount` needles are
base58 account keys, 44 bytes at most, and no other filter uses a bloom — which
is why this is **S4** and not **S1**, and why `cargo update` is safe today rather
than the hash change it looks like.

The direction that would hurt is a version skew between the archive writer and
the engine, on a value long enough to reach the long path. A writer on the broken
version sets one bit where a reader on the fixed one tests seven, so every such
value is a false negative — what [INV-P9](07-invariants.md#inv-p9) forbids and no
client can detect. The reverse skew only floods false positives, which the
invariant permits.

Half of this is already guarded, and it is worth being precise about which half.
`the_engine_builds_the_bloom_the_archiver_wrote` compares the engine's bits
against rows an archiver wrote, and the portable gate runs it, so a hash that
moved under a resolution would fail the build for any value of ordinary length.
What no test reaches is a value of 240 bytes or more, and a vector for one cannot
be written: no chunk carries such a value, so there is nothing to oracle against
and the test would only pin the engine to itself.

The manifest is the real gap. `xxhash-rust = { version = "0.8.15" }` is a caret
requirement, so the hash function is fixed by `Cargo.lock` alone — and a lock does
not apply to a consumer that takes this crate as a library. INV-P9 says the hash
function must match the archive writer's exactly; the manifest does not say which
one that is. An exact `=` requirement, on 0.8.18 rather than 0.8.15, states it and
costs nothing measurable.

`bloom_bit`'s own doc says the value goes "through XXH3 seeded with `n`", which is
true only below the threshold on the pinned version.

*First test:* not an oracle — there is nothing to compare a long value against.
The check that fits is a unit test pinning `bloom_bit`'s seven bits for a
240-byte value once a version is chosen, which fails if the resolution moves
across the bug in either direction.

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
