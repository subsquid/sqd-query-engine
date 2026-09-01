# ADR-8 — Bloom false positives are contract, not defect

Status: Accepted (historical)

## Context

`mentionsAccount` is answered from a fixed-size bloom column. Blooms
over-approximate: rows that do not mention the account can match. The tempting
fix is to post-filter them away before returning.

Two problems. The exact account set of a row is not always in the projected
columns, so post-filtering is not always possible — which means it would happen
sometimes and not others. And a client that learned to re-check would then be
silently depending on which engine version it was talking to.

## Decision

The filter is an over-approximation and says so. No false negatives; false
positives permitted and never removed. Clients needing exactness re-check.

## Consequences

[INV-P9](../07-invariants.md#inv-p9). The load-bearing half of the invariant is
the *no false negatives* half: the engine's bloom construction — width, hash
count, hash function, value serialisation — must match the archive writer's
exactly, because a mismatch produces false negatives and no client can detect
those.

That is the one property in the suite whose failure is invisible to every other
test, which is why it has its own line in the build order.
