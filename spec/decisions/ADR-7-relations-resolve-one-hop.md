# ADR-7 — Relations resolve exactly one hop

Status: Accepted (historical)

## Context

The relation graph has cycles: a log's transaction has logs, whose transaction
has logs. Transitive expansion does not terminate. An occurs-check terminates but
produces a result no client can predict, because what it returns depends on which
path the engine happened to take first.

## Decision

Rows pulled in by a relation do not have their own relations applied, even when
the same query requests those relations elsewhere.

## Consequences

[INV-R2](../07-invariants.md#inv-r2). `{"logs":[{"transaction":true}]}` returns
logs and their transactions, and no traces. A client wanting traces asks for
them.

The rule is what makes the cost of a query predictable from the query text: the
number of scans is bounded by the number of item requests and their flags, not by
the shape of the data.
