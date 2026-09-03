//! Fork detection.
//!
//! A client paging through a chain sends back the hash it believes the block
//! before `fromBlock` has. If the chunk disagrees, the chain reorganised between
//! the two requests and the data about to be returned belongs to a branch the
//! client did not ask about. Accepting the field and ignoring it serves that
//! data silently (INV-E5).

use crate::metadata::{DatasetDescription, TableDescription};
use crate::query::Plan;
use crate::scan::{ChunkReader, ScanRequest};
use anyhow::Result;
use arrow::array::Array;
use std::fmt;

/// One `(block number, hash)` pair from the window preceding `fromBlock`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: String,
}

/// The chunk's predecessor of `fromBlock` is not the block the client expected.
///
/// Carries the recent refs so the client can find the fork point and resume from
/// a block both sides agree on, rather than starting over.
#[derive(Clone, Debug)]
pub struct UnexpectedBaseBlock {
    pub expected_hash: String,
    pub prev_blocks: Vec<BlockRef>,
}

impl fmt::Display for UnexpectedBaseBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.prev_blocks.last() {
            Some(last) => write!(
                f,
                "unexpected base block: expected {}, but got {}#{}",
                self.expected_hash, last.number, last.hash
            ),
            None => write!(
                f,
                "unexpected base block: expected {}, but got empty prev_blocks",
                self.expected_hash
            ),
        }
    }
}

impl std::error::Error for UnexpectedBaseBlock {}

/// `P-FORK-WINDOW` (spec/09-parameters.md §9.3): how many recent
/// `(block number, hash)` pairs an `UnexpectedBaseBlock` carries, expressed as a
/// span of block numbers behind `from_block`.
///
/// It does not decide whether the parent is found. The row *at* `from_block`
/// states its own parent's hash, and the scan is anchored there, so a dataset
/// whose numbering skips further than this window returns fewer pairs rather
/// than losing fork detection (INV-E5).
const FORK_WINDOW: u64 = 100;

/// Compare the client's `parentBlockHash` against the chunk, if the client
/// supplied one and the chunk can see the preceding block.
///
/// Returns `Ok(())` when they agree, when no hash was supplied, or when the
/// predecessor is outside this chunk — a chunk that cannot see the block is not
/// evidence of a fork. A chunk that *should* be able to see it but cannot
/// produce the hash is an error: a quiet "no fork" there is the answer this
/// check exists to prevent.
///
/// A dataset that declares no parent-hash column at all is refused earlier, in
/// [`crate::query::compile`]; by the time execution runs, the column is declared.
pub(crate) fn check_parent_block(
    plan: &Plan,
    metadata: &DatasetDescription,
    chunk: &dyn ChunkReader,
) -> Result<()> {
    let Some(expected_hash) = plan.parent_block_hash.as_deref() else {
        return Ok(());
    };
    let Some(table) = metadata.table(&plan.block_table) else {
        return Ok(());
    };
    let Some(parent_hash_column) = table.parent_hash_column.as_deref() else {
        return Ok(());
    };

    // A scan of a table the chunk does not have returns no rows rather than an
    // error, which would read here as "no evidence of a fork".
    crate::engine_ensure!(
        chunk.has_table(&plan.block_table),
        crate::error::ErrorKind::TableNotFound,
        "chunk has no '{}' table, so 'parentBlockHash' cannot be checked",
        plan.block_table
    );

    let (refs, answer) = read_prev_blocks(plan, table, parent_hash_column, chunk)?;

    // Only the row *at* `from_block` states what precedes it; every other row in
    // the window is there so the client can find the fork point. Comparing
    // against the highest row the chunk happens to hold reports a fork whenever
    // the chunk ends below `from_block` — the chunk not reaching the block is
    // not the chain having moved under the client.
    let Some(parent) = answer else {
        return Ok(());
    };

    if parent.hash == expected_hash {
        return Ok(());
    }

    Err(UnexpectedBaseBlock {
        expected_hash: expected_hash.to_string(),
        prev_blocks: refs,
    }
    .into())
}

/// Read `(preceding block number, its hash)` for the blocks in the window,
/// ascending, along with the one that answers the client's question — the ref
/// contributed by the row at `from_block`, if the chunk holds it.
///
/// Each row of the block table carries its own parent's hash, so a row is read
/// as a statement about the block before it. Where the dataset declares a parent
/// *number* column that number is used verbatim, because a chain that skips
/// numbers has no `n - 1`.
fn read_prev_blocks(
    plan: &Plan,
    table: &TableDescription,
    parent_hash_column: &str,
    chunk: &dyn ChunkReader,
) -> Result<(Vec<BlockRef>, Option<BlockRef>)> {
    // Whether the *chunk* carries a parent-number column, not whether the catalog
    // declares one: a chunk written before the column existed still answers the
    // check by falling back to `n - 1`, which is what the reference does.
    let parent_number_column = table.parent_number_column.as_deref().filter(|col| {
        chunk
            .table_schema(&plan.block_table)
            .is_some_and(|schema| schema.column_with_name(col).is_some())
    });
    // The scan is anchored on the block number and reaches `from_block` itself,
    // because that row is the one that answers. Filtering on the parent number
    // instead made the answer depend on how far back the parent lies, so a chain
    // that skipped more numbers than the window silently lost fork detection —
    // the failure INV-E5 exists to prevent. The window behind it carries the
    // recent pairs and nothing more.
    let block_column = table.block_number_column.as_str();
    let mut columns = vec![block_column, parent_hash_column];
    if let Some(parent_number) = parent_number_column {
        columns.push(parent_number);
    }

    let mut request = ScanRequest::new(columns);
    request.from_block = Some(plan.from_block.saturating_sub(FORK_WINDOW));
    request.to_block = Some(plan.from_block);
    request.block_number_column = Some(block_column);

    // A chunk that cannot produce the hash cannot clear the client's
    // `parentBlockHash`, and serving the query regardless is the reorg being
    // served silently. But only a chunk that reaches into the lookback window
    // owes an answer: `required_columns` is checked before row groups are pruned,
    // so requiring it unconditionally fails a whole multi-chunk request over a
    // chunk that was never going to contribute a predecessor.
    let carries_parent_hash = chunk
        .table_schema(&plan.block_table)
        .is_some_and(|schema| schema.column_with_name(parent_hash_column).is_some());

    let batches = chunk.scan(&plan.block_table, &request)?;

    if !carries_parent_hash {
        let reaches_window = batches.iter().any(|b| b.num_rows() > 0);
        crate::engine_ensure!(
            !reaches_window,
            crate::error::ErrorKind::ColumnNotFound,
            "column '{}' is not found in '{}', so 'parentBlockHash' cannot be checked",
            parent_hash_column,
            plan.block_table
        );
        return Ok((Vec::new(), None));
    }

    let mut refs = Vec::new();
    let mut answer = None;
    for batch in &batches {
        let (Some(blocks), Some(hashes)) = (
            batch.column_by_name(block_column),
            batch.column_by_name(parent_hash_column),
        ) else {
            continue;
        };
        let parent_numbers = parent_number_column.and_then(|c| batch.column_by_name(c));
        let hashes = hashes
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| {
                crate::engine_err!(
                    crate::error::ErrorKind::MalformedChunkData,
                    "'{parent_hash_column}' must be a string column"
                )
            })?;

        for row in 0..batch.num_rows() {
            let Some(row_block) = crate::output::weight::get_block_number(blocks.as_ref(), row)
            else {
                continue;
            };

            // Which block this row is *about*: the number it states as its
            // parent's where the chunk carries one, and `n - 1` otherwise.
            //
            // At block 0 the saturating subtraction labels genesis its own
            // predecessor. The *hash* it reports is still the one block 0 stores,
            // so the comparison is right and only the label is odd; the reference
            // does the same, and a client paging from genesis is not a real case.
            let number = match parent_numbers {
                // A null reads as zero through the width-tolerant reader, which
                // would report block 0 as this block's parent.
                Some(col) if col.is_null(row) => continue,
                Some(col) => match crate::output::weight::get_block_number(col.as_ref(), row) {
                    Some(n) => n,
                    None => continue,
                },
                None => row_block.saturating_sub(1),
            };
            let hash = if hashes.is_null(row) {
                String::new()
            } else {
                hashes.value(row).to_string()
            };

            let block_ref = BlockRef { number, hash };
            if row_block == plan.from_block {
                answer = Some(block_ref.clone());
            }
            refs.push(block_ref);
        }
    }

    refs.sort_by_key(|r| r.number);
    Ok((refs, answer))
}
