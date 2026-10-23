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

## Revision: 16 MiB

`P-MAX-REQUEST-BYTES` is now **16 MiB**, a backstop rather than the bound a
deployment works under. It was raised with sets
([ADR-17](ADR-17-large-value-sets-travel-as-fuse8-with-canaries.md)).

The 2 MiB came from the reference's transport, not from anything the engine
needs: it was the default body limit of the reference's HTTP server. Two bounds
now sit in front of the engine. The portal admits what it decides to, and the
network carries a query of up to 4 MiB and a kibibyte of envelope to a worker,
refusing a larger one before it is sent. An engine bound below them refuses
requests the rest of the path accepts, and one equal to them makes every change
to them wait for an engine release. So the engine's bound sits well above them,
and stops only a body no deployment sends.

Sets are why that matters. A set is bounded by nothing but the bytes that carry
it, about 1.5 bytes of base 64 per value: the network's 4 MiB carries about 2.8
million values.

What a bound this size admits was measured on one core, parsing and compiling
the worst request, one of short list values. At 4 MiB it took up to 113 MiB of
memory and 70 ms, at 16 MiB up to 452 MiB and 0.3 s. A set the same size took 9
and 36 MiB. The cost of lists is the parsed JSON tree, about 72 bytes a value,
so an engine embedded where no transport bounds the request should have one
that does.

`P-MAX-IN-LIST` is unchanged. It bounds the hash set a list builds, and a set
builds none.
