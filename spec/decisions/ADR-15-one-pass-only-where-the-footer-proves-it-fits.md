# ADR-15 — Read payloads in one pass only where the footer proves it fits

Status: Accepted (2026-10-08)

## Context

An engine chooses a page by weight ([§5.4](../05-response.md), [§5.5](../05-response.md)).
It can decode every requested column of every matching row in the covered range
and then cut the page, or it can read narrow columns first, choose the page, and
decode the wide ones for the page's rows only. The first holds the whole range in
memory. On a table sorted by a filter column, the range is the whole chunk: one
query over all tables of a 1 592-block EVM chunk peaked at 3.4 GB that way, and
at 79 MB with the second.

The second costs a pass. On a query whose rows fit a page anyway it buys nothing,
and on small queries the pass is most of the time. So the engine reads in one pass
when it can tell that one pass fits the page, and it tells from the parquet footer.

Each of the first estimates undercounted for some file, and an undercount is the
whole range in memory again:

- A dictionary keeps a repeated value once, and every row decodes to its own copy.
  Counting the stored dictionary counted a 64 KiB payload in 100 rows as one.
- Statistics bound values, not lengths. The bounds `"a"` and `"z"` surround values
  of any length, a writer may truncate them, and a writer may leave them out.
  Sizing a dictionary's rows by its bounds decoded 100 rows of 64 KiB for an
  8-block page.
- A writer may leave out the dictionary page offset. Only the list of encodings
  then shows that the column has a dictionary.
- Prefix compression stores a shared prefix once.
- A decoded type can be wider than the stored one: a 32-bit decimal decodes to
  128 bits, a large string's offsets are 64 bits, and a list adds its own offsets.
- A row group's block bounds say nothing about how its rows spread between them.
  Counting the share of the span a request covers saw a 100-block request as
  tiny when the group held 100 rows in those blocks and one row a million blocks
  later.
- Header rows are read for the whole range, and the tables of one query are read
  in the same pass.

## Decision

An engine reads payloads in one pass only when one of these holds:

1. **The request covers one block.** A page always holds its first block whole
   ([INV-B4](../07-invariants.md#inv-b4), [INV-B6](../07-invariants.md#inv-b6)),
   so both reads decode the same rows.
2. **An upper bound taken from the footer fits the budget.** The bound counts the
   header rows and every table of the query; every row of each row group the scan
   reads; each column at the width of the type it decodes to, with its offsets and
   validity bitmaps; and a byte string's contents only from the writer's byte
   count, or from the stored pages when the encoding keeps every value whole
   (plain or length-prefixed).

Otherwise, and always for a query with relations, it reads narrow columns first.

The bound deliberately does not use:

- min/max statistics for a length;
- the stored size of a dictionary or of prefix-compressed pages;
- the dictionary page offset as the only sign of a dictionary;
- the share of a row group's block span that the request covers;
- the stored width of a column instead of its decoded width.

A size the footer cannot bound counts as unbounded.

## Consequences

The chunks written today record no byte counts for EVM dictionary columns
(`address`, `topic0`, `from`, `to`, `sighash` and every header string), so most
EVM queries without relations over more than one block read in two passes.
Measured on the bench chunks against an estimate that trusted statistics, with
identical answers: +8–13% on selective queries over many blocks (`getLogs` over
10 and 100 blocks, a USDC transfer scan), +7–11% on a full transaction scan
that asks for header fields, and no change on one-block RPC queries
(`getBlockByNumber`, `getBlockReceipts`, `trace_block`, `getLogs` over one block)
or on queries that defer anyway. This cost is accepted: the memory bound must not
depend on how a file was written.

CT-5 pins the decision. `the_estimate_never_undercounts_what_a_scan_decodes`
checks the bound against what the scan decodes for eleven column types under
twelve writer layouts. `a_query_without_relations_reads_payloads_only_for_the_page`
runs the query on a chunk for each case in the Context, and
`header_payloads_are_read_only_for_the_page`,
`tables_that_fit_a_page_alone_can_overflow_it_together` and
`a_one_block_request_reads_in_one_pass` cover the rest. Putting back any rejected
rule fails at least one of them.

Rejected alternatives:

- **Size from statistics.** It is fast on every shape, and on today's EVM chunks
  it is exact, because their dictionary columns hold hex strings of one length.
  It fails on every file in the Context.
- **Always read narrow columns first.** It is safe, and it cost
  `getBlockByNumber` with transaction hashes 22–33% for no memory gain.
- **Read the dictionary pages for the longest value.** It is exact for
  fixed-length columns, but it decompresses dictionaries on every query, and it
  still counts whole row groups. It was not measured.
- **Cap the one pass while it runs and restart on overflow.** The pass decodes at
  least one batch before it can stop, and with long values one batch can exceed
  the page many times.
- **Prune by the page index.** A bound per page needs a byte count per page, which
  the chunks written today do not record.

Revisit when one of these changes:

- Chunk writers record byte counts. Most EVM queries then read in one pass with
  no change to this decision.
- The second pass gets cheaper for small results. The remaining cost then goes.

Neither is a reason to size from statistics, from a stored dictionary or from a
block-span share again.
