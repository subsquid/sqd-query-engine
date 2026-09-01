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

/// How far back to look for the predecessor. The window exists because a chain
/// that skips numbers has no predecessor at `from_block - 1`.
const LOOKBACK: u64 = 100;

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
    anyhow::ensure!(
        chunk.has_table(&plan.block_table),
        "chunk has no '{}' table, so 'parentBlockHash' cannot be checked",
        plan.block_table
    );

    let refs = read_prev_blocks(plan, table, parent_hash_column, chunk)?;

    // The chunk starts at or after `from_block`, so it holds no evidence about
    // the parent. Skipping is right: the caller's next chunk will carry it.
    let Some(parent) = refs.last() else {
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

/// Read `(preceding block number, its hash)` for the blocks in the lookback
/// window, ascending.
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
) -> Result<Vec<BlockRef>> {
    // Whether the *chunk* carries a parent-number column, not whether the catalog
    // declares one: a chunk written before the column existed still answers the
    // check by falling back to `n - 1`, which is what the reference does.
    let parent_number_column = table.parent_number_column.as_deref().filter(|col| {
        chunk
            .table_schema(&plan.block_table)
            .is_some_and(|schema| schema.column_with_name(col).is_some())
    });
    let has_parent_number = parent_number_column.is_some();
    let number_column = parent_number_column.unwrap_or(table.block_number_column.as_str());

    // With a parent-number column the window is over parent numbers, so it stops
    // one short of `from_block`; without one it is over block numbers, and the
    // row *at* `from_block` is the one describing its parent.
    let upper = if has_parent_number {
        plan.from_block.saturating_sub(1)
    } else {
        plan.from_block
    };

    let mut request = ScanRequest::new(vec![number_column, parent_hash_column]);
    request.from_block = Some(plan.from_block.saturating_sub(LOOKBACK));
    request.to_block = Some(upper);
    request.block_number_column = Some(number_column);

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
        anyhow::ensure!(
            !reaches_window,
            "column '{}' is not found in '{}', so 'parentBlockHash' cannot be checked",
            parent_hash_column,
            plan.block_table
        );
        return Ok(Vec::new());
    }

    let mut refs = Vec::new();
    for batch in &batches {
        let (Some(numbers), Some(hashes)) = (
            batch.column_by_name(number_column),
            batch.column_by_name(parent_hash_column),
        ) else {
            continue;
        };
        let hashes = hashes
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| anyhow::anyhow!("'{parent_hash_column}' must be a string column"))?;

        for row in 0..batch.num_rows() {
            let Some(number) = crate::output::weight::get_block_number(numbers.as_ref(), row)
            else {
                continue;
            };
            // At block 0 the saturating subtraction labels genesis its own
            // predecessor. The *hash* it reports is still the one block 0 stores,
            // so the comparison is right and only the label is odd; the reference
            // does the same, and a client paging from genesis is not a real case.
            let number = if has_parent_number {
                number
            } else {
                number.saturating_sub(1)
            };
            let hash = if hashes.is_null(row) {
                String::new()
            } else {
                hashes.value(row).to_string()
            };
            refs.push(BlockRef { number, hash });
        }
    }

    refs.sort_by_key(|r| r.number);
    Ok(refs)
}
