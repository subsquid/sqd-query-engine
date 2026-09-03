# ADR-13 — Bound request resource use

Status: Accepted (2026-09-03)

## Context

[INV-Q13](../07-invariants.md#inv-q13) requires a whole-request byte cap and a
cap on the length of one `inList`. The item-request count alone does not bound
either parsing work or the collections built from filter values before data is
read.

[§9.1](../09-parameters.md) originally listed 1 MiB and 10 000 as unratified
proposals under
[ADR-12](ADR-12-unratified-thresholds-ship-as-proposals.md). The accepted values
need to accommodate supported request shapes while placing explicit bounds on
resource use.

## Decision

`P-MAX-REQUEST-BYTES` is **2 MiB**. `P-MAX-IN-LIST` is **100 000**. Neither
carries a ⚠.

The byte cap bounds parsing and the total representation of a request. The list
cap independently bounds the set built for a single filter, including requests
whose individual values are short.

Both violations are reported by the engine as `RequestTooLarge`.

## Consequences

[§9.1](../09-parameters.md)'s two ⚠ markers are removed and the observed values
match the target values.

These limits are compatibility parameters rather than protocol constants. A
future change must update the parameter table and record the rationale in this
ADR.
