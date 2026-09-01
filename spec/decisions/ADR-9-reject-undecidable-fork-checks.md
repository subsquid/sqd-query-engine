# ADR-9 — Reject `parentBlockHash` where the dataset cannot answer it

Status: Proposed

## Context

`parentBlockHash` asks the engine to confirm the client is still on the branch it
thinks it is. The check needs the block table to carry its parent's hash. Not
every dataset's archive does.

The engine's current behaviour is to accept the field and skip the check when the
column is absent. The client gets data back and nothing in the response says the
question went unanswered — which is indistinguishable, at the client, from a
chain that did not reorganise.

There is a legitimate skip: the chunk holds no block in the search window because
`fromBlock` lies outside it. A chunk that cannot see the block is not evidence of
a fork, and the client's next chunk will carry it.

## Decision

Skip only for that reason. Where the dataset's block table declares no parent-hash
column, reject the request with `UnsupportedRequestField`.

## Consequences

[INV-E5](../07-invariants.md#inv-e5), and a new error kind in
[06-errors.md](../06-errors.md).

A client asking for fork detection on such a dataset now gets a loud failure it
can route around instead of a silent one it cannot see. The cost is that a query
that "worked" stops working, which is the correct trade when what it was doing
was nothing.

This ADR is Proposed. It is what §6.1's ranking — *a typed error beats a wrong
answer that looks right* — implies, but it does make the engine stricter than the
reference implementation.
