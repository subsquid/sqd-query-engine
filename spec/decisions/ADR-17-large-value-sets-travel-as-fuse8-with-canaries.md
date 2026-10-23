# ADR-17 — Large value sets travel as fuse8 structures with canaries

Status: Proposed

## Context

Some clients filter on more values than a list can carry. A DEX factory deploys a
contract for every pair it lists, and a client that indexes the pairs filters logs
on hundreds of thousands of addresses; the largest factories have millions. On
Solana the same shape is every token account a program owns. An `inList` stops at
`P-MAX-IN-LIST` values and `P-MAX-REQUEST-BYTES`, and an address costs about 45
bytes of JSON, so one request carries a few tens of thousands at most. The
workarounds are worse than the problem: scan without the filter and drop rows on
the client, or split the list over many queries that each scan the same range
([#82](https://github.com/subsquid/sqd-query-engine/issues/82)).

The portal sends the whole query with every chunk request, and recent chunks are
small. So whatever stands in for the list is paid for once per chunk, and its
size matters more than anything else about it. On a 224-block Ethereum chunk, a
structure over 400 000 values is four to eight times the size of the answer it
selects. The measurements behind this record are in
[#84](https://github.com/subsquid/sqd-query-engine/pull/84).

## Decision

A filter on a column that renders as text may list a **set** among its values
(§2.7). Sets are declared once per request, under a top-level `sets` key, and
filters refer to them by name.

- **One format, `fuse8`**: a binary fuse filter with 8-bit fingerprints over the
  XXH3-64 of each value's rendered text. The specification fixes the lookup, not
  the construction (§4.2.11). The engine never builds a set, so any bytes the
  lookup accepts for every value are a correct one.
- **Canaries are required.** A set carries a few of its values as the client
  holds them, and the engine refuses the request when one of them is not a value
  the column can render or is not in the structure.
- **The filtered field must be selected**, so that the client can see what it
  has to remove.
- **False positives are the client's to remove**, as they are for the stored
  bloom filters of [ADR-8](ADR-8-bloom-false-positives-are-contract.md). The
  engine cannot remove them: it never sees the values.

## Why these and not the alternatives

**Fuse8, not a Bloom filter.** At the same rate of false positives, about one in
256, a fuse8 structure takes about 9 bits per value and a Bloom filter about 11.5.
The bound for any structure is log₂ 256 = 8 bits, so nothing else does much
better. Over 400 000 values the prototype measured 612 KB of base64 against
826 KB, and the Bloom filter's seeded hashes took two to three times as long per
row. A
Bloom filter can also be filled until it matches everything, which would need a
density rule. A fuse8 lookup cannot be made to pass every value, because part of
each fingerprint comes from hash bits that no index reads.

**Fuse8, not Fuse16.** Fuse16 returned almost no false positives and made the
fastest requests, because a false positive costs more than its bytes: the worker
reads back the output columns of the row group it sits in. But it is twice the
size, and the size is paid on every chunk.

**No cache of sets on the worker.** Sending a set to each worker once, and only
its digest after that, would make size cheap. It does not pay off. A factory's set
grows while the client discovers children, so the set and its digest change with
every rebuild. Each change misses on every worker, and each miss costs one more
round trip.

**Named sets, not sets written inside the filter.** A set written inside a filter
travels once per filter that uses it. `from ∈ S ∨ to ∈ S` takes two item requests,
and both owners of a Solana token balance take two more. Inline, each would carry
its own copy; named, the bytes travel once.

**References in the list, not in place of it.** A reference is one more element
of a filter's list, so a filter's value stays a list, and a client adds a value
to a set or joins two sets by listing them instead of building another. The
list passes what any of its parts passes. Its values can rule out no row group
that a set in the same list may still pass, so a list with a set costs what the
set costs. Each reference is a lookup for every row its table scans, and a set
costs a few dozen bytes to declare again under another name, so a request makes
at most `P-MAX-SET-REFERENCES` references.

**Canaries, not trust.** Every way to build a set wrong — hashing a checksummed
address, dropping the `0x`, hashing the raw bytes instead of the text, another
hash or another seed — produces a structure that is accepted and matches almost
nothing. An almost empty answer looks like a quiet stretch of chain. Canaries turn
each of these mistakes into an error before any data is read. A wrong structure
passes a canary only by a false positive, so four of them let one through about
once in 4·10⁹ requests.

## Consequences

An engine that predates sets refuses a request carrying them, with `UnknownTable`
on the top-level key ([INV-Q2](../07-invariants.md#inv-q2)). It cannot answer as if
the filter were absent. The portal can route on the same key: a request with
`sets` goes only to an engine that supports them, and fails where none holds the
range. A real-time source that cannot evaluate a set has to refuse it in the same
way.

Canaries miss two mistakes. A set on the wrong field, such as addresses on a topic
filter, passes them, because the canaries are well-formed values there too. And a
mistake that spoils only some values, say only the long ones, is caught only by a
canary among them, which is why a client should pick its canaries at random each
time it builds a set.

The client builds the set, and rebuilds it whenever the set grows. A stream that
meets a new member at block B restarts at B with a set holding everything its last
batch found, so it rebuilds at most once a batch. The prototype's builder,
compiled to WASM, builds 400 000 values in about 26 ms under Node. A TypeScript
port takes about 87 ms, and is a second implementation to keep in step with the
lookup.

`P-MAX-REQUEST-BYTES` still bounds a request, and fuse8 costs about 1.5 bytes of
base 64 per value. With this record the bound becomes a 16 MiB backstop
([ADR-13](ADR-13-request-resource-bounds.md)). What bounds a set in practice is
the network's 4 MiB message, about 2.8 million values, and the portal decides how
much of it a client may use.
