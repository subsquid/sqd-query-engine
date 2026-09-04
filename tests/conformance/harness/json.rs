//! Reading a response back.
//!
//! A response is NDJSON: one object per block in range, whether or not anything
//! matched. Splitting it was written out by hand in ten places, and a hand-rolled
//! split that forgets the trailing newline reports one block too many.

use serde_json::Value;

/// Every block object in a response, in the order it was written.
pub fn parse_response(body: &[u8]) -> Vec<Value> {
    body.split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect()
}

/// The block numbers a response covers.
pub fn block_numbers(blocks: &[Value]) -> Vec<u64> {
    blocks
        .iter()
        .map(|b| b["header"]["number"].as_u64().unwrap())
        .collect()
}

/// Count the items a response carries under one table key. A response always
/// carries a header per block in range, so "nothing matched" is an item count of
/// zero, not an empty body.
pub fn count_items(body: &[u8], table_key: &str) -> usize {
    parse_response(body)
        .iter()
        .map(|block| items_in(block, table_key).len())
        .sum()
}

/// Every item a response carries under one table key, as `(block, item)` pairs.
///
/// The query has to have selected `block.number`, since that is where the block
/// half of each pair comes from; `items_in` is the same walk without it.
pub fn items_of(body: &[u8], table_key: &str) -> Vec<(u64, Value)> {
    parse_response(body)
        .iter()
        .flat_map(|block| {
            let number = block["header"]["number"].as_u64().unwrap();
            items_in(block, table_key)
                .into_iter()
                .map(move |item| (number, item))
        })
        .collect()
}

/// The items one block object carries under a table key, or none.
pub fn items_in(block: &Value, table_key: &str) -> Vec<Value> {
    block
        .get(table_key)
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
}

/// Assert two responses are byte-identical, reporting the first line that
/// differs — and, within it, the first byte — rather than two megabytes of
/// NDJSON or three hundred identical characters of prefix.
pub fn assert_same_response(expected: &[u8], actual: &[u8], what: &str) {
    if expected == actual {
        return;
    }

    let (want, got) = (lines(expected), lines(actual));
    let at = (0..want.len().max(got.len()))
        .find(|&i| want.get(i) != got.get(i))
        .expect("the responses differ, so some line does");

    let (Some(want), Some(got)) = (want.get(at), got.get(at)) else {
        panic!(
            "{what} changed the response length: {} lines expected, {} actual",
            want.len(),
            got.len()
        );
    };

    let byte = (0..want.len().max(got.len()))
        .find(|&i| want.get(i) != got.get(i))
        .unwrap_or(0);
    let from = byte.saturating_sub(60);

    panic!(
        "{what} changed the response at line {at}, byte {byte}\n  expected: …{}\n  actual:   …{}",
        window(want, from),
        window(got, from),
    );
}

fn lines(body: &[u8]) -> Vec<&[u8]> {
    body.split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .collect()
}

fn window(line: &[u8], from: usize) -> String {
    let slice = &line[from.min(line.len())..(from + 240).min(line.len())];

    String::from_utf8_lossy(slice).into_owned()
}
