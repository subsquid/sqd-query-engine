# ADR-6 — Alias item requests count toward the global cap

Status: Accepted (historical)

## Context

The item-request cap exists because each request is an independent scan. The
reference implementation counts the arrays it lists per dataset, and for
`hyperliquidReplicaCmds` it lists only `actions` — the four aliases over that
same table are uncounted.

An uncounted array is an unbounded scan. A client can send
`P-MAX-ITEM-REQUESTS` requests at a table and as many again through each of its
aliases.

## Decision

Count every item request, uniformly, across every table and every alias.

## Consequences

[INV-Q5](../07-invariants.md#inv-q5). A query the reference accepts may be
rejected here. That is the intended direction: the cap is a resource bound, and a
resource bound with an exemption is not one.

Recorded in the divergence table in [GAPS.md](../GAPS.md).
