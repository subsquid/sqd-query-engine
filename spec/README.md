# SQD Block Query Engine — Specification

This directory specifies the behaviour of a *block query engine*: a system that
answers structured queries over columnar archives of blockchain data.

The specification is **abstract**. It describes what a conforming engine must do,
not how any particular engine does it. It names no functions, files, types, or
libraries. Two engines written in different languages, with different storage
layers and different execution strategies, are both conforming if they satisfy
these documents.

The point of writing it this way is that the spec becomes a *test oracle*. Every
normative statement here should be mechanically checkable against an
implementation. Where a statement cannot be turned into a test, it is either
misplaced or too vague, and should be fixed.

## Documents

| File | Contents | Normative |
|---|---|---|
| [01-data-model.md](01-data-model.md) | Chunks, datasets, tables, columns, keys, catalog | yes |
| [02-request.md](02-request.md) | Request grammar, defaults, validation | yes |
| [03-catalog.md](03-catalog.md) | The dataset catalog: per-dataset tables, filters, relations, fields | yes |
| [04-evaluation.md](04-evaluation.md) | Filter algebra, relation resolution, row-set semantics | yes |
| [05-response.md](05-response.md) | Response shape, ordering, encoding, weight, pagination | yes |
| [06-errors.md](06-errors.md) | Error taxonomy and when each error fires | yes |
| [07-invariants.md](07-invariants.md) | The invariant list — every rule, numbered and testable | yes |
| [08-conformance.md](08-conformance.md) | How to build a TDD suite from this spec | advisory |
| [GAPS.md](GAPS.md) | Where the current implementation diverges | **non-normative** |

Read them in order the first time. After that, [07-invariants.md](07-invariants.md)
is the working document; everything else exists to explain it.

## Requirement levels

The words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are used as in RFC 2119.

A statement with no requirement word is descriptive: it explains the model but
imposes no obligation.

## What the engine does

An engine is handed two things: a **chunk** of archived block data, and a
**query**. It returns a **response**: the blocks in the requested range, each
carrying exactly the items the query asked for.

```
        query (JSON)
             │
             ▼
   ┌──────────────────┐
   │ validate         │ ── rejects malformed queries before any data is read
   ├──────────────────┤
   │ select rows      │ ── per-table filters produce row sets
   ├──────────────────┤
   │ expand relations │ ── one hop: pull in related rows
   ├──────────────────┤
   │ choose blocks    │ ── weight budget decides where the response ends
   ├──────────────────┤
   │ encode           │ ── one JSON object per block
   └──────────────────┘
             │
             ▼
   response + lastBlock
```

Three properties make this useful, and each is load-bearing:

**Chunk independence.** A query against a range of blocks can be split across
chunks, evaluated independently and in parallel, and the results concatenated.
This works only because every relation is confined to a single block
([INV-D5](07-invariants.md#inv-d5)). If a relation could reach across blocks, no
chunk could be evaluated alone.

**Bounded responses.** A response is capped by a *weight budget*, not by a row
count. The engine emits a prefix of the requested block range and reports the
last block it emitted. The caller resumes from the next one. Blocks are never
split ([INV-B4](07-invariants.md#inv-b4)), so a client that concatenates
responses sees exactly what it would have seen from one enormous response.

**Layout independence.** The archive stores rows in whatever order makes
filtering fast — usually sorted by the columns people filter on, not by block
number. The response is always in a canonical order regardless
([INV-O12](07-invariants.md#inv-o12), [INV-O13](07-invariants.md#inv-o13)). No
query result may depend on how the data happens to sit on disk.

## Schema-agnosticism

The engine contains no knowledge of any particular blockchain. Everything it
knows about EVM, Solana, Bitcoin and the rest lives in the **catalog**
([03-catalog.md](03-catalog.md)) — a declarative description of tables, columns,
filters and relations.

This is a hard rule, not a design preference. Adding a chain MUST be a catalog
edit ([INV-X1](07-invariants.md#inv-x1)). If an engine needs new code to serve a
new chain, the catalog format is missing something, and the missing thing is a
spec bug.

## Reading the gap document

[GAPS.md](GAPS.md) records where the implementation in this repository currently
diverges from this spec. It is deliberately kept out of the normative documents,
because a spec that describes today's bugs cannot be used to find them.

When a gap is closed, delete its entry. When the spec is wrong and the
implementation is right, fix the spec and delete the entry. The document should
tend toward empty.
