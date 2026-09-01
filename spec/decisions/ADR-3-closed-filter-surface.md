# ADR-3 — The filter surface is a closed allowlist

Status: Accepted (historical)

## Context

Resolving a filter key against "any column of the table" is the cheap
implementation, and it was the implementation. Item tables carry bloom columns,
size counters, discriminator prefixes and denormalised extraction results, all of
which then became filterable.

Two costs. Clients can filter on internals and get answers that mean nothing —
`{"dataSize": [100]}` is not a question about logs. And the column list becomes
the public API: adding a column to an archive adds a filter, and removing one
removes it.

## Decision

Each table declares the filters clients may use. A key naming an undeclared
column is an error even when a column of that name exists.

## Consequences

[INV-P15](../07-invariants.md#inv-p15). The catalog now carries the filter
surface explicitly, which makes it checkable at load
([INV-D1](../07-invariants.md#inv-d1)) and makes drift from the reference a test
failure rather than a discovery.

Adding a filter is now a deliberate catalog edit. That is the point.
