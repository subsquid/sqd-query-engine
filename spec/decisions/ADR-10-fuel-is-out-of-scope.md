# ADR-10 — `fuel` is out of scope

Status: Accepted (2026-08-31)

## Context

`fuel` was being carried as a dataset to port. Its catalog trailed the reference
implementation's field list by nineteen transaction fields, kept the
input-contract, output-contract and policy values as struct columns where the
reference exposes them flat, renamed one field-group request key, and could not
support fork detection at all because the Fuel block table carries no parent
hash — `prev_root` is not one.

None of that was cheaply fixable: a declared column absent from a chunk is a hard
error ([INV-E3](../07-invariants.md#inv-e3)), so guessing at the current archive
layout turns working queries into failing ones, and the fixture chunk on hand has
the old layout.

## Decision

Do not port `fuel`. It is not a dataset a conforming engine must serve.
[§3.6](../03-catalog.md) keeps its section number and says so.

## Consequences

The one gap this document's register carried against it disappears with it.

`metadata/fuel.yaml` goes with the decision: a catalog file for a dataset the
specification does not cover is an invitation to serve it. The Fuel fixtures stay
in the fixture tree, now describing a dataset nothing constrains — no test reads
them, and nothing may depend on them.

The section number is kept rather than reclaimed so §3.7 and §3.8 do not move.
