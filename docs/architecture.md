# Query execution: a code reading guide

This guide follows the current implementation from JSON bytes to a selected page
and its output. The [specification](../spec/README.md) defines the contract;
[historical engine comparisons](history/engine-comparison.md) describe earlier
implementations and measurements.

## Route through the code

File links point to implementations. Public imports use the re-exports in
`metadata`, `query`, `scan`, and `output`, as shown in the [README](../README.md#usage).
Most implementation modules are private.

| Stage | Input → result | Start reading here |
|---|---|---|
| Load catalog | YAML → validated `DatasetDescription` | [`load_dataset_description`, `parse_dataset_description`](../crates/metadata/src/loader.rs) |
| Parse | JSON bytes + catalog → `Query` with `QueryItem`s | [`parse_query`](../src/query/parse.rs) |
| Compile | `Query` + catalog → `Plan`, `TablePlan`, `RelationPlan` | [`compile`](../src/query/plan.rs) |
| Prepare execution | Plan + catalog + `ChunkReader` + `ExecOptions` → validation, fork check, optional `SelectionReader`, header rows | [`execute_chunk_fmt`](../src/output/assembly.rs), [`check_parent_block`](../src/output/fork.rs) |
| Scan a range | Table plans + inclusive block bounds → `TableOutput`s containing primary and related `Rows` | [`scan_tables`, `scan_relation`](../src/output/assembly.rs) |
| Select a page | Range outputs + headers → per-block weights and an ordered prefix of whole blocks | [`compute_block_weights`, `BlockSelection::extend`](../src/output/weight.rs) |
| Materialize when deferred | Selected row identities + physical projections → payload batches for tables and headers | [`retain_selected_keys`, `materialize_tables`, `read_rows`](../src/output/materialize.rs) |
| Prepare JSON | Selected blocks + payload batches → indexes, resolved writers, `QueryOutput` | [`execute_chunk_fmt`](../src/output/assembly.rs), [`build_block_index`](../src/output/block_index.rs), [`resolve_writers`](../src/output/row_writer.rs) |
| Consume JSON | `QueryOutput` + caller's byte buffer → one encoded block per call | [`QueryOutput::write_next_block`](../src/output/writer.rs) |
| Produce Arrow | Selected blocks + payload batches → buffered `ArrowOutput` | Arrow branch of [`execute_chunk_fmt`](../src/output/assembly.rs), [`write_arrow_frames`](../src/output/arrow_out.rs) |

Scanning and selection repeat for complete block ranges until the page is full
or the request is exhausted. Materialization runs after selection when payloads
were deferred. JSON and Arrow share that front half, then take separate output
branches.

## Logical fields and physical columns

The catalog lives in the `sqd-metadata` workspace crate and is re-exported as
`sqd_query_engine::metadata`. Its [types](../crates/metadata/src/types.rs) describe
request names, physical columns, output fields, weights, relations, and ordering.
See [the catalog format](../metadata/README.md) for YAML examples.

`Query::fields`, `Plan::block_output_columns`, `TablePlan::output_columns`, and
`RelationPlan::output_columns` contain **logical output names**, despite the
`columns` suffix. Parsing normalizes request names; compilation orders selected
fields according to the catalog. These names cannot be passed directly to a
storage reader in general.

`TableDescription::field_source` resolves a logical field to `FieldSource::Column`
or `FieldSource::Roll`; `field_columns` enumerates its physical sources. For
example, EVM log `topics` reads `topic0` through `topic3`, while a trace variant's
field can refer to a differently named physical column. JSON writers then apply
the catalog's naming, roll, variant, and encoding rules.

[`output/columns.rs`](../src/output/columns.rs) turns logical selections into
physical projections:

- `physical_output_columns` expands only the selected fields, for flat Arrow output.
- `resolve_output_columns` and `resolve_relation_output_columns` also include
  grouping, sorting, variant, relation, and weight helpers as needed.
- `required_output_columns` identifies physical field sources and size companions
  whose absence is an error.

[`weight.rs`](../src/output/weight.rs) builds the header projection with
`block_scan_columns` and the narrow selection projection with
`weight_scan_columns`. Header fields use their physical sources too.
`ScanRequest::output_columns` and `ScanRequest::required_columns` are **physical
column names**. Predicate columns are physical as well. Scan-only helpers need
not appear in the answer. Missing required sources are covered by
[INV-E3](../spec/07-invariants.md#inv-e3).

## Parsing and compilation

[`parse_query`](../src/query/parse.rs) validates the request surface against the
catalog, resolves aliases to tables, normalizes names, and retains each item's
alias identity. It returns `Query`, not a separate `ParsedQuery` type.
[`compile`](../src/query/plan.rs) builds typed `RowPredicate`s, projections, and
relation plans; compilation also performs filter-specific validation.

Items of one table are ORed; an item's conditions are ANDed, with alternative
groups for filters such as mixed-length discriminators. `RelationPlan::source_items`
records which items requested that relation. Equivalent joins combine their
source item sets through `RelationPlan::same_join`; different keys or kinds stay
separate. Relations expand one hop, as specified by
[INV-R1](../spec/07-invariants.md#inv-r1) and
[ADR-7](../spec/decisions/ADR-7-relations-resolve-one-hop.md).

## Execution and read strategy

`output::execute_plan` opens a `ParquetChunkReader` for a chunk directory and
calls `execute_chunk`. Callers with an existing reader use `execute_chunk` or
`execute_chunk_with`; the latter takes explicit `ExecOptions`. Arrow entry points
are `execute_plan_arrow` and `execute_chunk_arrow`.

All reach [`execute_chunk_fmt`](../src/output/assembly.rs). Before scanning for
output, it checks required tables and renderable columns and performs the
optional `parentBlockHash` check. Relation targets are required only when there
are source rows to follow; an empty source does not open its target.

### Where payload deferral is chosen

Read `defers_payloads`, `prescanned_table`, and `initial_range_end` in
[`assembly.rs`](../src/output/assembly.rs), then `SelectionReader::new` in
[`materialize.rs`](../src/output/materialize.rs).

With `ExecOptions::range_reads` enabled:

1. A plan with relations requests deferred payload reads.
2. A request for exactly one block without relations reads payloads immediately:
   the first block must be returned whole either way.
3. Otherwise, `defers_payloads` compares the estimated decoded bytes for the
   header and table projections against the weight budget. An unavailable
   estimate counts as unbounded. A sole unfiltered table without relations or
   `includeAllBlocks` uses a narrow weight prescan to choose its first range;
   its payload estimate is omitted from this comparison, but headers still count.
4. When deferral is requested, `SelectionReader::new` checks the table schemas
   and complete item keys. If it cannot construct the reader, execution keeps
   the direct scan path.

With `range_reads = false`, the executor scans the whole requested range before
selecting blocks and does not install `SelectionReader`. This is the full-read
comparison path. The strategy's rationale is recorded in
[ADR-15](../spec/decisions/ADR-15-one-pass-only-where-the-footer-proves-it-fits.md);
[`ct5_blocks_weight/estimate.rs`](../tests/conformance/ct5_blocks_weight/estimate.rs)
checks the decoded-byte upper bound.

### Ranges, relations, and page selection

Headers establish the covered range's boundary blocks once. The deferred path
initially reads their identities and weight columns; the direct path also reads
requested header payloads. `initial_range_end` and `next_range_end` use a weight
prescan or storage hints to choose complete block ranges. Deferred selection
additionally bounds ranges by actual header numbers, so sparse numbering does
not create scans across empty numeric gaps.

For each range, `scan_tables` builds physical `ScanRequest`s and scans primary
tables. `Scanned` contains `Rows` plus matches for `item_tags`; `Rows` keeps Arrow
batches aligned with optional physical positions. Item tags let relations use
only their own source items without evaluating those predicates again.

`relation_inputs` reuses source subsets and prepared keys or address indexes.
`scan_relation` passes a `KeyFilter` or `HierarchicalFilter` to the target scan
when possible. Otherwise it applies the equality or hierarchical masks from
[`join/`](../src/join/mod.rs) after scanning. A pushed-down relation needs no
second join over its result.

[`compute_block_weights`](../src/output/weight.rs) accounts for selected fields
and deduplicates contributions from overlapping sources. `BlockSelection`
accumulates an ascending prefix across ranges. Blocks remain whole, including
an oversized first block. `includeAllBlocks` includes every covered header;
otherwise candidate blocks come from matched/related rows plus the original
range's boundary headers. Internal read ranges do not add response boundaries.
See [response selection and paging](../spec/05-response.md#55-which-blocks-are-returned)
and the [weight model](weight-calculation.md).

The deferred path retains selected identities after each range and reads payloads
only after selection. `read_rows` uses physical positions when available;
otherwise it builds a filter from complete item keys. `materialize_tables`
combines compatible sources before reading. Without positions, an eligible
primary source can instead repeat its original predicates within the selected
page bounds. The selection column cache is released before payload materialization.

## Storage and scan paths

[`ChunkReader`](../src/scan/mod.rs) is the storage boundary. Its required
`scan_rows` method returns `Scanned`; `scan` returns just the batches. Optional
capabilities provide physical positions, block-range hints, and byte estimates.
The executor relies on these capabilities rather than a concrete reader type.

[`ParquetChunkReader`](../src/scan/chunk.rs) opens tables lazily and caches their
`ParquetTable`s. Each table shares its mmap-backed bytes and Arrow/Parquet metadata.
A chunk contains one Parquet file per table. Catalog sort keys may put selective
filter columns before block numbers, so block ranges need not be contiguous in
physical row order.

The Parquet entry is [`scan_rows` → `scan_batches`](../src/scan/scanner.rs):

1. Validate required columns and compatible predicate/block types.
2. If `row_indices` is supplied, use [`positions::read_rows`](../src/scan/positions.rs)
   directly. These sorted, unique positions already identify the selected rows.
3. Otherwise collect physical columns and prune row groups using block bounds,
   relation keys, and per-item statistics. Keep only the active items for each group.
4. Choose a decoded-column cache read where eligible, or build a Parquet
   `RowFilter` for the row group. If the cache cannot admit the group, use the
   ordinary reader path.
5. Project output columns and collect batches, positions, and item tags.

`reader_filter` chooses the actual stages. Ordinary scans can apply block bounds,
key membership, and item predicates with separate projections. A lone untagged
item gets a stage for its first predicate column and another for the rest;
multiple items or item tags use the combined item evaluator. Hierarchical scans
use their address stage and apply any remaining block bounds afterward. There
is no single fixed stage sequence for all requests.

`SelectionReader` can supply a bounded `ColumnCache` for tables read by several
broad scans. It keeps decoded columns for the current `Window` and shares them
between those scans; it is not an unbounded cache of payloads.

[`ColumnarChunkReader`](../src/scan/columnar.rs) adapts `ColumnarChunk` and
`ColumnarTable` to the same contract. It uses column reads, row ranges, optional
window statistics, and in-memory masks instead of Parquet `RowFilter` stages.
Reader equivalence is exercised in
[`ct6_output/determinism.rs`](../tests/conformance/ct6_output/determinism.rs).

## JSON and Arrow output

For JSON, execution builds block indexes and `IndexedBatches` sources, groups
sources by output table, and resolves field writers against each batch schema.
`QueryOutput` owns the selected block numbers, batches, indexes, and writers.
No reader is retained for fetching payloads during encoding.

[`write_next_block`](../src/output/writer.rs) appends one JSON object to the caller's
buffer. It writes the header and merges, sorts, and deduplicates the table's rows
for that block through [`row_writer.rs`](../src/output/row_writer.rs) and
[`row_order.rs`](../src/output/row_order.rs). Check `has_next_block` before calling
it; append a newline for NDJSON. `into_json_lines` encodes every selected block
into one buffer, including blocks already consumed through incremental calls.

Laziness applies to **JSON encoding**, not to loading the page's payloads.
`first_block`, `last_block`, and `num_blocks` are available before encoding.
`read_through` reports how far a range scan read when it stopped early; it may
exceed the last emitted block. `None` from execution means no covered blocks,
not simply no filter matches.

The Arrow branch runs before JSON writer preparation. It trims batches to selected
blocks, projects physical field sources plus the block-number key, and merges
and deduplicates tables with multiple sources. `write_arrow_frames` creates
per-table IPC streams in a single buffered `ArrowOutput`, with optional Zstd
compression and hex-to-binary conversion. This is flat physical-column output,
not nested JSON fields. See
[ADR-11](../spec/decisions/ADR-11-columnar-encoding-is-a-rendering.md).

## Parallelism and invariants

Primary tables, relation work, and Parquet row groups use Rayon; deferred table
and header materialization also overlap via `rayon::join`. They use the active
Rayon pool. Query concurrency and pool size are separate controls. Collected
results are checked in stable plan or row-group order to preserve deterministic
errors. JSON blocks encode sequentially.

When following or changing this route, start with the relevant conformance class:

| Contract | Tests |
|---|---|
| Required catalogs and request fields | [`ct1_catalog.rs`](../tests/conformance/ct1_catalog.rs), [`ct2_request/`](../tests/conformance/ct2_request/mod.rs) |
| Filtering and pruning | [`ct3_filter_algebra/`](../tests/conformance/ct3_filter_algebra/mod.rs) |
| Relation scope and identities | [`ct4_relations/`](../tests/conformance/ct4_relations/mod.rs) |
| Whole-block budget, ranges, positions, deferred reads | [`ct5_blocks_weight/`](../tests/conformance/ct5_blocks_weight/mod.rs) |
| Encoding, source deduplication, reader and thread determinism | [`ct6_output/`](../tests/conformance/ct6_output/mod.rs) |

The [invariant index](../spec/07-invariants.md) and
[traceability matrix](../spec/08-conformance.md) link the rules to their tests.
