# ADR-2 — Bound responses by weight, not by row count

Status: Accepted (historical)

## Context

A response has to be bounded or a single query can ask for a gigabyte. The
obvious bound is a row count, and it is wrong here: rows in these archives differ
in size by four orders of magnitude. A thousand block headers and a thousand
traces carrying contract init code are not the same response.

A byte count is the honest bound but cannot be evaluated cheaply — knowing the
size of a response requires building it.

## Decision

Bound the response by a *weight budget*: a per-row model summing declared column
weights over the columns actually emitted. Weight is computed from narrow columns
before the wide ones are decoded, and blocks are admitted as a weighted prefix.

## Consequences

Weight is a model, not a measurement, and its absolute accuracy does not matter —
[INV-B9](../07-invariants.md#inv-b9) requires only that it be deterministic.

Because it is computed over the *emitted projection*
([INV-B10](../07-invariants.md#inv-b10)), a narrow query gets more blocks than a
wide one over the same range, which is what a client asking for two fields
expects.

Blocks stay atomic ([INV-B4](../07-invariants.md#inv-b4)): a block over budget is
emitted whole and alone, because a split block would break resumption.

`P-WEIGHT-BUDGET` and `P-DEFAULT-COLUMN-WEIGHT` are the parameters.
