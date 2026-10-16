use crate::output::block_index::BlockPositions;
use crate::output::row_writer::{json_close, PreparedHeader, PreparedTable, RowScratch};
use crate::output::sources::{OutputHeader, OutputTable};
use anyhow::Result;

/// Initial buffer capacity, tuned for typical response sizes.
const INITIAL_CAPACITY: usize = 256 * 1024;

/// Result of a query execution: the selected blocks plus everything needed to
/// encode them on demand.
///
/// The block range metadata is available immediately — block selection happens
/// before any encoding — and may end below the queried range end if the
/// response was trimmed to the size budget. Blocks are encoded lazily, one per
/// [`write_next_block`](Self::write_next_block) call, so a streaming consumer
/// never holds more than one encoded block; buffering consumers use
/// [`into_json_lines`](Self::into_json_lines).
///
pub struct QueryOutput {
    selected_blocks: Vec<u64>,
    next: usize,
    header: PreparedHeader,
    /// In the order their items take in a block.
    tables: Vec<PreparedTable>,
    scratch: RowScratch,
    read_through: Option<u64>,
}

impl QueryOutput {
    /// Prepare the headers and every table to encode. `selected_blocks` is
    /// sorted and not empty.
    pub(crate) fn prepare(
        selected_blocks: Vec<u64>,
        header: OutputHeader,
        tables: Vec<OutputTable>,
        read_through: Option<u64>,
    ) -> Result<Self> {
        let positions = BlockPositions::new(&selected_blocks);
        let header = PreparedHeader::new(header, &positions)?;
        let tables = tables
            .into_iter()
            .filter_map(|table| PreparedTable::new(table, &positions).transpose())
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            selected_blocks,
            next: 0,
            header,
            tables,
            scratch: RowScratch::default(),
            read_through,
        })
    }

    pub fn num_blocks(&self) -> usize {
        self.selected_blocks.len()
    }

    /// Last fully read block when range reads stopped before the requested end.
    pub fn read_through(&self) -> Option<u64> {
        self.read_through
    }

    pub fn first_block(&self) -> u64 {
        self.selected_blocks[0]
    }

    pub fn last_block(&self) -> u64 {
        *self.selected_blocks.last().expect("never empty")
    }

    pub fn has_next_block(&self) -> bool {
        self.next < self.selected_blocks.len()
    }

    /// Encodes the next block as a JSON object appended to `out`.
    /// Panics if there is no next block — check [`has_next_block`](Self::has_next_block) first.
    pub fn write_next_block(&mut self, out: &mut Vec<u8>) {
        let position = self.next;
        let block_num = self.selected_blocks[position];
        out.push(b'{');

        self.header.write(out, block_num, position);
        for table in &self.tables {
            table.write_items(out, position, &mut self.scratch);
        }

        json_close(b'}', out);
        self.next += 1;
    }

    /// Encodes all blocks (regardless of prior iteration) as JSON Lines.
    pub fn into_json_lines(mut self) -> Vec<u8> {
        self.next = 0;
        let mut out = Vec::with_capacity(INITIAL_CAPACITY);
        while self.has_next_block() {
            self.write_next_block(&mut out);
            out.push(b'\n');
        }
        out
    }
}
