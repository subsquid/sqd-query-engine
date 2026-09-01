# ADR-4 — The output field surface is closed too

Status: Proposed

## Context

[ADR-3](ADR-3-closed-filter-surface.md) closed the filter surface but left the
*output* surface derived: a field is selectable if it names a non-`system`
column, a virtual field, or a field-group request key. That derivation is wider
than the surface the reference implementation offers, by 16 tables' worth of
`blockNumber` plus the filter columns behind every roll — `logs.topic0…3` behind
`topics`, `instructions.a0…a15` behind `accounts`.

The obvious fix — mark those columns `system` — does not work.
`instructions.d1…d8` are filter columns *and* selectable fields, deliberately, so
`system` cannot be the discriminator. And a `system` column contributes zero
weight ([INV-D9](../07-invariants.md#inv-d9)), so hiding `topic0…3` that way
would drop the `topics` roll out of the weight model.

## Decision

The selectable fields of a table are the ones the catalog declares, enumerated
per dataset in [03-catalog.md](../03-catalog.md). The derivation stays as a
convenience for authoring a catalog; where it and the list disagree, the list
wins.

## Consequences

[INV-Q14](../07-invariants.md#inv-q14), and GAP 27 for the engine's current
behaviour. Sixteen tables need a declared list.

Symmetry with ADR-3 is the argument: both surfaces are wire contract, and neither
should be a side effect of what columns an archive happens to carry.

This ADR is Proposed. Accepting it commits to the catalog carrying a field list,
which is a format addition.
