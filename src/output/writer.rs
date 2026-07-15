use crate::output::row_writer::{
    json_close, write_header, write_merged_table_items, IndexedBatches, ResolvedFieldWriter,
    ResolvedGroupedWriters,
};
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap;
use std::collections::HashMap;

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
    pub(crate) selected_blocks: Vec<u64>,
    pub(crate) next: usize,
    pub(crate) block_batches: Vec<RecordBatch>,
    pub(crate) block_index: FxHashMap<u64, Vec<(usize, usize)>>,
    pub(crate) header_resolved: Vec<Vec<ResolvedFieldWriter>>,
    pub(crate) bn_key_prefix: Vec<u8>,
    pub(crate) all_indexes: Vec<IndexedBatches>,
    pub(crate) all_resolved: Vec<Vec<Vec<ResolvedFieldWriter>>>,
    pub(crate) all_grouped_resolved: Vec<Option<Vec<ResolvedGroupedWriters>>>,
    pub(crate) table_group_order: Vec<String>,
    pub(crate) table_groups: HashMap<String, Vec<usize>>,
    pub(crate) table_json_prefixes: HashMap<String, Vec<u8>>,
    // Reusable per-block row-ref scratch buffers (sort + multi-source merge).
    pub(crate) sort_scratch: Vec<(usize, usize)>,
    pub(crate) merge_scratch: Vec<(usize, usize, usize)>,
}

impl QueryOutput {
    pub fn num_blocks(&self) -> usize {
        self.selected_blocks.len()
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

        write_header(
            out,
            block_num,
            &self.block_batches,
            &self.block_index,
            &self.bn_key_prefix,
            &self.header_resolved,
        );

        // Table items, merging multiple sources for the same output table
        for table_name in &self.table_group_order {
            write_merged_table_items(
                out,
                block_num,
                &self.all_indexes,
                &self.table_groups[table_name],
                &self.all_resolved,
                &self.all_grouped_resolved,
                &self.table_json_prefixes[table_name],
                &mut self.sort_scratch,
                &mut self.merge_scratch,
            );
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
