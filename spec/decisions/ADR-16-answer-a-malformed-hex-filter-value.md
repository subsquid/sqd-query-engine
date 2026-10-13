# ADR-16 — Answer a malformed hex filter value

Status: Accepted (2026-10-13)

## Context

[INV-Q12](../07-invariants.md#inv-q12) used to refuse any request that carried a
malformed hex value in a filter: a missing `0x`, an odd number of digits, a
character that is not a hex digit. The reasoning was §6.1's ranking. An address
without its prefix matches nothing, an empty answer tells the client nothing, and
a typed error is better than silence.

The reference implementation never refused one. On a column stored as text it
compares the value as written, so a malformed address matches nothing. On a
hex-number column it drops the values that do not parse and filters on the rest,
and when none parse the item request matches nothing. Only the discriminator
filter checks its values, and there both engines refuse.

Clients send malformed values, and send the same ones query after query: 32-byte
topics with 63 or 65 digits, 20-byte values padded to 39 or 41, a `d8` with 17.
The reference answers each of them. Under the strict rule this engine would
answer each with an error instead, for a request whose rows the bad value does
not change.

## Decision

A malformed hex value in a column filter is answered, not refused. On a column
stored as text it compares as text. On a column stored as bytes or integers it
has no stored form and matches nothing: inside a list it is a no-op, and a list
with no value left is an empty list. The item then matches nothing and reads
nothing, so a chunk that lacks the column answers it as the reference does.

The discriminator filter keeps its check. A malformed value is `InvalidHex`, and
one longer than any discriminator column is `DiscriminatorTooLong`.

## Consequences

The answer is the one the request asks for, row for row. A malformed value adds
the rows equal to it, which on a column of addresses or hashes is none. What is
lost is the signal: a client with a typo in one address of twenty gets the rows
for the other nineteen and no word about the twentieth. The strict rule existed
to avoid that cost. It is accepted here because the alternative breaks clients
that work today, over values that change nothing they receive.

Comparing as text, rather than dropping the value, matters on text columns. A
column of quantities stores minimal-form values such as `0x0`, whose digit count
is odd, and a client filtering on zero writes exactly that.

The alternative was to keep the strict rule and announce it as an API change.
That is still open, as a deliberate change with its own notice, but not as a
side effect of which engine answers the request.

`malformed_hex_values_answer_the_same_as_the_reference` holds the decision to the
reference on every hex filter of every fixture chunk.
