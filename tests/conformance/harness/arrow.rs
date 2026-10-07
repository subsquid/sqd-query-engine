//! Reading an Arrow response back.
//!
//! The Arrow rendering is one IPC stream per table, each framed as
//! `[u32 LE name_len][name][u32 LE payload_len][payload]`.

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::io::Cursor;

/// Every table stream of a response, by the name it was framed under.
pub fn read_frames(framed: &[u8]) -> HashMap<String, Vec<RecordBatch>> {
    let mut tables = HashMap::new();
    let mut rest = framed;

    while !rest.is_empty() {
        let name = take_prefixed(&mut rest);
        let name = String::from_utf8(name.to_vec()).unwrap();
        let payload = take_prefixed(&mut rest);

        let reader = StreamReader::try_new(Cursor::new(payload), None).unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        tables.insert(name, batches);
    }

    tables
}

/// Split one `[u32 LE len][bytes]` field off the front of `rest`.
fn take_prefixed<'a>(rest: &mut &'a [u8]) -> &'a [u8] {
    let (len, tail) = rest.split_at(4);
    let len = u32::from_le_bytes(len.try_into().unwrap()) as usize;
    let (field, tail) = tail.split_at(len);
    *rest = tail;
    field
}
