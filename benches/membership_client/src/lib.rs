//! Builds the `fuse8` filter value the prototype engine reads: a binary fuse
//! filter over `xxh3_64` of each value's bytes, sent as its 20-byte descriptor
//! (seed u64, segment length, mask and count length u32, all little-endian)
//! followed by the fingerprints.

use xorf::BinaryFuse8;
use xxhash_rust::xxh3::xxh3_64;

/// `values` holds every value's bytes back to back; value `i` ends at `ends[i]`.
pub fn build_fuse8(values: &[u8], ends: &[u32]) -> Result<Vec<u8>, &'static str> {
    let mut start = 0;
    let keys: Vec<u64> = ends
        .iter()
        .map(|&end| {
            let key = xxh3_64(&values[start..end as usize]);
            start = end as usize;
            key
        })
        .collect();

    let filter = BinaryFuse8::try_from(&keys)?;
    Ok(serialize(&filter))
}

pub fn serialize(filter: &BinaryFuse8) -> Vec<u8> {
    let descriptor = &filter.descriptor;
    let mut out = Vec::with_capacity(20 + filter.fingerprints.len());
    out.extend_from_slice(&descriptor.seed.to_le_bytes());
    out.extend_from_slice(&descriptor.segment_length.to_le_bytes());
    out.extend_from_slice(&descriptor.segment_length_mask.to_le_bytes());
    out.extend_from_slice(&descriptor.segment_count_length.to_le_bytes());
    out.extend_from_slice(&filter.fingerprints);
    out
}

/// Packs values the way the WASM entry point takes them.
pub fn pack<'a>(values: impl IntoIterator<Item = &'a str>) -> (Vec<u8>, Vec<u32>) {
    let mut bytes = Vec::new();
    let mut ends = Vec::new();
    for value in values {
        bytes.extend_from_slice(value.as_bytes());
        ends.push(bytes.len() as u32);
    }
    (bytes, ends)
}

/// A WASM memory grows and never shrinks, so this is the most any build in
/// the process has needed, and what the process keeps.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(js_name = memoryBytes)]
pub fn memory_bytes() -> usize {
    core::arch::wasm32::memory_size(0) * 65536
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(js_name = buildFuse8)]
pub fn build_fuse8_wasm(values: &[u8], ends: &[u32]) -> Result<Vec<u8>, wasm_bindgen::JsError> {
    build_fuse8(values, ends).map_err(wasm_bindgen::JsError::new)
}
