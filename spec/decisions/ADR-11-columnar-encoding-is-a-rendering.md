# ADR-11 — A columnar encoding is a rendering, never a second result

Status: Accepted (historical)

## Context

The JSON response is expensive to produce and to parse. A columnar encoding is
substantially cheaper on both ends for large responses, and the engine offers
one.

The risk is obvious: two output paths drift, and a client gets different answers
depending on which it asked for. That is worse than having no columnar encoding
at all, because the difference is invisible until someone compares.

## Decision

An alternate encoding carries the same rows, the same blocks and the same values
as the JSON form. It MAY differ in nesting and in field naming. It is never a
different query result.

## Consequences

[INV-O14](../07-invariants.md#inv-o14), and a parity test per encoding — the
Arrow path currently has six.

Everything upstream of encoding — filtering, relations, dedup, block selection,
weight — is shared by construction, so parity is a property of the encoder alone.
Any optimisation that changes which *rows* the columnar path produces is out of
bounds, however fast.
