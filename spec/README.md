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
| [08-conformance.md](08-conformance.md) | Test classes, traceability matrix, merge gates, harness register | §8.1–8.10 advisory; §8.11–8.14 current state |
| [09-parameters.md](09-parameters.md) | Every constant the spec depends on | yes |
| [decisions/](decisions/) | The ADR log — why the load-bearing choices are what they are | rationale, not rules |
| [GAPS.md](GAPS.md) | Where the current implementation diverges | **non-normative** |

Read 01 through 07 in order the first time. After that,
[07-invariants.md](07-invariants.md) is the working document; everything else
exists to explain it.

## Conventions

### Requirement levels

The words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are used as in RFC 2119.

A statement with no requirement word is descriptive: it explains the model but
imposes no obligation.

### Identifiers

Every rule has a stable ID. IDs are never reused and never renumbered; a deleted
rule leaves a hole.

| Prefix | What it names | Home |
|---|---|---|
| `INV-D` `INV-Q` `INV-P` `INV-R` `INV-B` `INV-O` `INV-E` `INV-X` | Invariants, banded by domain | [07-invariants.md](07-invariants.md) |
| `CT-n` | Test class | [08-conformance.md](08-conformance.md) |
| `MG-n` | Merge gate | [08-conformance.md](08-conformance.md) |
| `HC-n` | Harness capability | [08-conformance.md](08-conformance.md) |
| `P-*` | Parameter | [09-parameters.md](09-parameters.md) |
| `ADR-n` | Decision | [decisions/](decisions/) |
| gap numbers | Implementation divergence | [GAPS.md](GAPS.md) |

Invariants are banded by letter rather than numbered straight through, so a new
rule about relations lands next to the other relation rules without moving
anything. `INV-D` is the data model and catalog, `INV-Q` request validation,
`INV-P` the filter algebra, `INV-R` relations, `INV-B` blocks and weight, `INV-O`
the response format, `INV-E` errors, `INV-X` cross-cutting.

### Constants

No normative sentence contains a number. Every constant is a `P-*` symbol
resolved in [09-parameters.md](09-parameters.md), with an *observed* column
saying what the engine does today and a *target* column saying what it should do.
A ⚠ marks a target nobody has ratified.

### What changes, and when

Two files are **mutable**: [09-parameters.md](09-parameters.md) and §8.11–8.14 of
[08-conformance.md](08-conformance.md). They track the current state, and change
as the engine does.

[decisions/](decisions/) is **append-only**. An accepted ADR is never rewritten.
Changing course means a new ADR whose Context cites the old one, and the old
one's Status becoming `Superseded by ADR-n` — the only edit ever permitted.

Everything else changes only when intended behaviour changes.

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

The distinction between a gap and a decision matters. A gap is a divergence
somebody intends to close. A divergence somebody intends to *keep* is a decision,
and it lives in [decisions/](decisions/) with the reasoning attached — otherwise
the next reader closes it as a bug.

## How to use this

Checking an engine: read 01–07, then work down the traceability matrix in
[§8.11](08-conformance.md) and write the tests for the rows marked **U**.

Changing the engine: the gates in [§8.12](08-conformance.md) say what a change
has to pass. `make spec-check` is one of them and runs in milliseconds.
`--list-checks` prints the rules; `--severity` decides what is printed and
`--gate` decides what fails, which are deliberately separate — CI prints
everything and fails on errors alone.

The checker has its own tests — `make spec-test` — one mutation per rule. A check
that silently does nothing prints what a satisfied check prints, so the only way
to know a rule works is to break something and watch it complain.

A finding can be suppressed by a line in `spec/.speccheck-ignore`, written
`check | file-glob | message-regex`. All three are required, the check must be
one real name — there is no wildcard — and a malformed line is a usage error
rather than a rule silently dropped. Every run reports how many findings it
suppressed, and a run that suppressed anything never prints `clean`. A gate with
a quiet off switch is not a gate.

Changing the spec: a new rule gets an ID, a matrix row and a test class in the
same change. A new constant gets a `P-*` symbol and a registry row. A choice a
future maintainer could relitigate gets an ADR.
