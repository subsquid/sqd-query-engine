# ADR-5 — Output field order follows catalog column order

Status: Accepted (historical)

## Context

The response is byte-deterministic ([INV-O12](../07-invariants.md#inv-o12)), so
field order has to come from somewhere fixed. Two candidates: the order fields
are declared in the query DSL, which is what the reference implementation uses,
and the order columns appear in the catalog.

## Decision

Catalog column order, virtual fields after real ones.

## Consequences

Both orders are stable, so this changes no meaning — but it does mean the two
implementations produce equal *values* in different *bytes*.

Every parity suite comparing this engine against the reference must therefore
compare values, not bytes. That is a real cost and it is paid deliberately: the
catalog is the single source of truth for everything else about a table, and
having output order come from a second place is how the two drift apart.

Recorded in the divergence table in [GAPS.md](../GAPS.md) so nobody "fixes" it.
