# ADR-1 — NDJSON is the only JSON framing

Status: Accepted (historical)

## Context

The reference implementation can emit two JSON shapes: a single array, and one
object per line. Both were in use. An array is what a naive client expects; lines
are what a client paging a large range actually wants, because it can parse and
commit block by block without holding the whole response.

Two responses for adjacent block ranges must be joinable. With an array that
means parsing both and splicing; with lines it means concatenating bytes.

## Decision

One JSON object per block, each followed by a newline, including the last. An
empty result is zero bytes — not `[]`, not a bare newline.

## Consequences

Responses concatenate. A reader finds block boundaries without a JSON parser.
[INV-O1](../07-invariants.md#inv-o1) and the partition property
[INV-B8](../07-invariants.md#inv-b8) both rest on this.

The cost is that a client cannot `JSON.parse` a whole response, and that the
array writer's behaviour — `[]` for an empty result — is now a divergence from
the reference rather than an alternative.
