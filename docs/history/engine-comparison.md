# Historical engine comparison

Earlier architecture notes compared an initial RowFilter-based implementation
of `sqd-query-engine` with the legacy `sqd-query` engine. This page records the
scope of that comparison. Use the [current reading guide](../architecture.md)
for today's execution path.

## Earlier designs

The initial comparison described `sqd-query-engine` scanning predicates and
output columns together, pushing relation key filters into target scans, then
selecting blocks by weight from the resulting batches. Arrow's `RowFilter`
limited decoding to rows surviving its predicate stages.

The legacy path was described as explicit passes: collect matching row indexes,
expand relations through key reads and joins, calculate output weights, then
read the selected payloads. This is historical context rather than a maintained
guide to the legacy source tree.

The comparison's reader counts and one-pass-versus-three-phase tables do not
characterize the current engine. It can defer payloads until page selection,
read selected physical positions, and share decoded selection columns. It also
parallelizes primary tables and relations, not only Parquet row groups.

## Measurements and decisions

Recorded latency and throughput results remain in
[BENCHMARKS.md](../../BENCHMARKS.md). They describe their particular query sets,
input data, hardware, and revisions; they do not establish performance of the
current revision or prove a cause for a timing difference.

The measured JSON serialization alternatives and their rationale are in
[the serialization decision](../decisions/001-json-serialization-strategy.md).
The current payload-read decision is specified by
[ADR-15](../../spec/decisions/ADR-15-one-pass-only-where-the-footer-proves-it-fits.md).
