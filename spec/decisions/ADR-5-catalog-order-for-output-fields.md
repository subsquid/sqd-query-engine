# ADR-5 — Output field order follows catalog column order

Status: Accepted (historical; context corrected 2026-09-04)

## Context

The response is byte-deterministic ([INV-O12](../07-invariants.md#inv-o12)), so
field order has to come from somewhere fixed.

The catalog already fixes one order. The reference fixes a second: each item
type's projection lists its fields in source order, in a macro maintained by
hand beside the schema it projects.

An earlier version of this record said the reference used the order fields are
declared in the query. It does not, and did not before the field-selection
rewrite either. Neither engine consults the request. Both emit a fixed order;
they read it from different places.

## Decision

Catalog column order, virtual fields after real ones.

## Consequences

Two hand-maintained orders drift apart. One cannot. The decision stands, and on
the corrected reading it is the easier call, not the harder one.

What changes is the price. Both orders are fixed and neither depends on the
request, so they can be made to agree field for field. For most item types they
already do; the rest differ by a transposition or two, and each is a catalog
edit ([gap 31](../GAPS.md)). Byte parity is reachable without moving where the
order comes from.

So the cost this record used to describe — parity suites comparing values rather
than bytes — is real, but it is not the price of the decision. It is the price
of not having finished. A value comparison cannot see field order at all, which
means it cannot see the divergence it was adopted to tolerate, nor any later one
([gap 32](../GAPS.md)).
