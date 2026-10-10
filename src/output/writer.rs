use crate::output::row_writer::{json_close, PreparedHeader, PreparedTable, RowScratch};

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
    /// `selected_blocks` is sorted and not empty.
    pub(crate) fn new(
        selected_blocks: Vec<u64>,
        header: PreparedHeader,
        tables: Vec<PreparedTable>,
        read_through: Option<u64>,
    ) -> Self {
        Self {
            selected_blocks,
            next: 0,
            header,
            tables,
            scratch: RowScratch::default(),
            read_through,
        }
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
        let block_num = self.selected_blocks[self.next];
        out.push(b'{');

        self.header.write(out, block_num);
        for table in &self.tables {
            table.write_items(out, block_num, &mut self.scratch);
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
