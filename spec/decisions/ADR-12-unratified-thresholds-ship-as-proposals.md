# ADR-12 — Merge-gate thresholds ship as proposals

Status: Accepted (2026-09-02)

## Context

Five of the merge gates rest on numbers nobody has measured: a property-coverage
floor, two line-coverage floors, a flake retry count, a benchmark noise band. The
capabilities that would measure them do not exist yet, so the numbers are
guesses — sized by eye, in the way a first draft of any threshold is.

There were two ways to publish them. Leave them out until something can measure
them, which means the gates have no thresholds and the register cannot say what
is missing. Or write the guesses down and mark them.

## Decision

Write them down, mark each with ⚠, and let the gates that use them stay advisory.
A ⚠ target is a proposal; it becomes the contract when a decision accepts it, and
until then nothing blocks on it.

This ADR is what the ⚠ markers in
[§9.4](../09-parameters.md) route to. It ratifies the *arrangement*, not the
values.

## Consequences

A reader can tell a measured threshold from a guessed one by looking, which is
the whole point of the column.

Ratifying a value later means a new ADR naming it and the measurement behind it,
and dropping the ⚠. Until then no PR can be blocked by a number nobody defended,
and no number can quietly become the contract by sitting in the table long enough.

The arrangement is not a licence to leave them. Each stays ⚠ only while the
capability that would measure it is unbuilt; §8.14 puts those in order.
