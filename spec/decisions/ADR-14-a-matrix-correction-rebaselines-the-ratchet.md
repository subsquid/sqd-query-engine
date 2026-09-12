# ADR-14 — A matrix correction re-baselines the ratchet

Status: Accepted (2026-09-12)

## Context

[§9.4](../09-parameters.md#94-merge-gate-thresholds) says the gate thresholds
ratchet upward only. The rule exists so that a change cannot buy its way past
[MG-1](../08-conformance.md#812-merge-gates) by deleting a test and lowering
the floor to match.

The observed `P-COV-PROPERTY` is counted from the traceability matrix, and the
matrix can be wrong. Three rows — INV-D6, INV-P13, INV-Q14 — stood at **C** on
tests that do not reach the invariant they were cited for. Correcting them
moved the count from 0.73 (62 of 85) to 0.69 (59 of 85) without a test being
removed. Read literally, the rule forbids writing the true number down; a
ratchet started from the inflated one would have locked the inflation in as the
floor.

## Decision

Lowering a row's grade because its evidence never reached the invariant is a
*correction*, not a coverage loss, and a correction may move the observed value
and the ⚠ target down together. The ratchet then resumes from the corrected
number.

A correction must say which rows moved and why, and must not remove or weaken a
test. Removing a test and lowering the floor is still what the rule forbids.

This ADR records the one correction made so far: commit `0be7c67`, 0.73 → 0.69.

## Consequences

The floor tracks what the tests check, not what the matrix once claimed. A
reader auditing a lowered threshold looks for the ADR or commit that names the
corrected rows; a lowering with neither is a rule violation.

`check.py` does not compare thresholds across revisions, so the rule is still
enforced by review. That is unchanged by this decision, and is where the next
capability in [§8.14](../08-conformance.md#814-build-order) would go.
