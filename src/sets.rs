//! Sets: filters on value sets too long to send as a list (spec §2.7, §4.2.11,
//! ADR-17).
//!
//! A client builds a `fuse8` structure over its values and declares it once,
//! under the request's `sets`; filters refer to it by name. The engine never
//! builds one. It only looks values up, so [`Fuse8::contains`] is the whole
//! contract between a client's builder and the engine: any bytes it passes
//! every value for are a correct set, however they were made.

use crate::engine_err;
use crate::error::ErrorKind;
use anyhow::Result;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use indexmap::IndexMap;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64;

/// `P-MAX-SET-CANARIES` (spec/09-parameters.md §9.1). Each canary is a lookup
/// for every column that refers to its set, paid before any data is read.
pub const MAX_SET_CANARIES: usize = 64;

/// `P-MAX-SET-REFERENCES` (spec/09-parameters.md §9.1). Each reference is a
/// lookup for every row its table scans, and a set costs a client little to
/// declare again under another name, so the references are counted, not the
/// sets.
pub const MAX_SET_REFERENCES: usize = 64;

/// The request's declared sets, by name, in the order the request gives them.
pub type Sets = IndexMap<String, Arc<Set>>;

/// One declared set.
#[derive(Debug)]
pub struct Set {
    pub fuse8: Fuse8,
    /// A few of the set's values as the client holds them, before the column
    /// folds them (INV-Q17).
    pub canaries: Vec<String>,
}

/// A binary fuse filter with one-byte fingerprints, checked sound: no value's
/// lookup reads past its fingerprints.
pub struct Fuse8 {
    seed: u64,
    /// `L`, `M` and `N` of §4.2.11, widened once so the lookup needs no casts.
    segment_length: u64,
    segment_mask: u64,
    segment_count_length: u64,
    fingerprints: Box<[u8]>,
    /// XXH3 of the wire bytes, so diagnostics can tell two sets apart.
    digest: u64,
}

/// The header: a 64-bit seed, then `L`, `M` and `N` as 32-bit words, all
/// little-endian.
const HEADER_BYTES: usize = 20;

/// The bound on `L`. Past it, `i1` would read the hash bits 36–39 the
/// fingerprint takes, and a crafted structure could pass every value.
const MAX_SEGMENT_LENGTH: u64 = 1 << 18;

impl Fuse8 {
    /// The structure the bytes hold, or why its lookup could read past them.
    pub fn from_bytes(bytes: &[u8]) -> Result<Fuse8, String> {
        let Some((header, fingerprints)) = bytes.split_first_chunk::<HEADER_BYTES>() else {
            return Err(format!(
                "{} bytes, shorter than its {HEADER_BYTES}-byte header",
                bytes.len()
            ));
        };

        let word = |at: usize| {
            let word: [u8; 4] = header[at..at + 4].try_into().unwrap();
            u64::from(u32::from_le_bytes(word))
        };
        let seed = u64::from_le_bytes(header[..8].try_into().unwrap());
        let (length, mask, count_length) = (word(8), word(12), word(16));

        let sound = length.is_power_of_two()
            && length <= MAX_SEGMENT_LENGTH
            && mask == length - 1
            && count_length > 0
            && count_length % length == 0
            && fingerprints.len() as u64 >= count_length + 2 * length;
        if !sound {
            return Err(format!(
                "the header says L = {length}, M = {mask}, N = {count_length} over {} \
                 fingerprints; a sound one has L a power of two up to 2^18, M = L - 1, N a \
                 positive multiple of L, and at least N + 2L fingerprints",
                fingerprints.len()
            ));
        }

        Ok(Fuse8 {
            seed,
            segment_length: length,
            segment_mask: mask,
            segment_count_length: count_length,
            fingerprints: fingerprints.into(),
            digest: xxh3_64(bytes),
        })
    }

    /// Whether `value`, the text the response renders, passes the lookup of
    /// §4.2.11. Every value the set was built from does; others pass at the
    /// format's false-positive rate.
    #[inline]
    pub fn contains(&self, value: &[u8]) -> bool {
        let hash = mix(xxh3_64(value).wrapping_add(self.seed));
        let fingerprint = (hash ^ (hash >> 32)) as u8;

        // Soundness bounds every index below N + 2L, and so below the length.
        let i0 = ((u128::from(hash) * u128::from(self.segment_count_length)) >> 64) as u64;
        let i1 = (i0 + self.segment_length) ^ ((hash >> 18) & self.segment_mask);
        let i2 = (i0 + 2 * self.segment_length) ^ (hash & self.segment_mask);

        let f = &self.fingerprints;
        fingerprint ^ f[i0 as usize] ^ f[i1 as usize] ^ f[i2 as usize] == 0
    }

    pub fn fingerprints(&self) -> usize {
        self.fingerprints.len()
    }

    pub fn digest(&self) -> u64 {
        self.digest
    }
}

/// The fingerprints are hundreds of kilobytes, and no reader wants them.
impl std::fmt::Debug for Fuse8 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fuse8")
            .field("fingerprints", &self.fingerprints.len())
            .field("digest", &format_args!("{:016x}", self.digest))
            .finish()
    }
}

/// MurmurHash3's 64-bit finaliser.
#[inline]
fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^ (x >> 33)
}

/// The request's `sets`, each checked well-formed (INV-Q15, INV-Q17). Whether
/// filters refer to them is the plan's to check, since only it resolves the
/// references.
pub fn parse_sets(value: Option<&serde_json::Value>) -> Result<Sets> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(Sets::new());
    };
    let declared = value.as_object().ok_or_else(|| {
        engine_err!(
            ErrorKind::MalformedRequest,
            "'sets' must be an object, got {}",
            value
        )
    })?;

    declared
        .iter()
        .map(|(name, set)| {
            let set = parse_set(set)
                .map_err(|why| engine_err!(ErrorKind::InvalidSet, "set '{}': {}", name, why))?;
            Ok((name.clone(), Arc::new(set)))
        })
        .collect()
}

fn parse_set(value: &serde_json::Value) -> Result<Set, String> {
    let object = value
        .as_object()
        .ok_or_else(|| format!("expected an object holding 'fuse8' and 'canaries', got {value}"))?;

    // A key the set does not know may be a structure this engine predates, and
    // guessing what it holds would answer without the filter.
    if let Some(key) = object
        .keys()
        .find(|key| !matches!(key.as_str(), "fuse8" | "canaries"))
    {
        return Err(format!(
            "unknown key '{key}': 'fuse8' is the only structure, beside 'canaries'"
        ));
    }

    let encoded = object
        .get("fuse8")
        .ok_or("no structure: expected 'fuse8'")?
        .as_str()
        .ok_or("'fuse8' must be a base 64 string")?;
    let bytes = BASE64
        .decode(encoded)
        .map_err(|e| format!("'fuse8' is not canonical base 64: {e}"))?;
    let fuse8 = Fuse8::from_bytes(&bytes).map_err(|why| format!("'fuse8': {why}"))?;

    let canaries: Vec<String> = object
        .get("canaries")
        .and_then(|canaries| canaries.as_array())
        .and_then(|canaries| {
            canaries
                .iter()
                .map(|canary| canary.as_str().map(str::to_owned))
                .collect()
        })
        .ok_or("'canaries' must be an array of strings: a few of the set's values")?;
    if !(1..=MAX_SET_CANARIES).contains(&canaries.len()) {
        return Err(format!(
            "{} canaries; a set carries at least one and at most {MAX_SET_CANARIES}",
            canaries.len()
        ));
    }

    Ok(Set { fuse8, canaries })
}

/// The wire form of a set over `values`, built by `xorf`, the builder the
/// lookup was written against.
#[cfg(test)]
pub(crate) fn wire<S: AsRef<str>>(values: &[S]) -> Vec<u8> {
    let keys: Vec<u64> = values
        .iter()
        .map(|v| xxh3_64(v.as_ref().as_bytes()))
        .collect();
    let filter = xorf::BinaryFuse8::try_from(&keys).unwrap();

    let mut bytes = filter.descriptor.seed.to_le_bytes().to_vec();
    bytes.extend(filter.descriptor.segment_length.to_le_bytes());
    bytes.extend(filter.descriptor.segment_length_mask.to_le_bytes());
    bytes.extend(filter.descriptor.segment_count_length.to_le_bytes());
    bytes.extend(filter.fingerprints.iter());
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use xorf::{BinaryFuse8, Filter};

    fn header(length: u32, mask: u32, count_length: u32, fingerprints: usize) -> Vec<u8> {
        let mut bytes = 7u64.to_le_bytes().to_vec();
        bytes.extend(length.to_le_bytes());
        bytes.extend(mask.to_le_bytes());
        bytes.extend(count_length.to_le_bytes());
        bytes.resize(HEADER_BYTES + fingerprints, 0);
        bytes
    }

    fn address(i: u64) -> String {
        format!("0x{:040x}", i.wrapping_mul(0x9e3779b97f4a7c15))
    }

    /// The lookup agrees with `xorf`'s on members and non-members alike, over
    /// sizes that give `xorf` different segment lengths.
    ///
    /// Covers CT-3 · INV-P17
    #[test]
    fn the_lookup_agrees_with_the_builders_own() {
        for n in [1u64, 2, 3, 10, 100, 1_000, 10_000, 100_000] {
            let values: Vec<String> = (0..n).map(address).collect();
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            let keys: Vec<u64> = values.iter().map(|v| xxh3_64(v.as_bytes())).collect();
            let reference = BinaryFuse8::try_from(&keys).unwrap();
            let set = Fuse8::from_bytes(&wire(&refs)).unwrap();

            for value in &values {
                assert!(
                    set.contains(value.as_bytes()),
                    "{n} values: {value} missing"
                );
            }

            let mut passed = 0;
            for i in n..n + 20_000 {
                let other = address(i);
                let ours = set.contains(other.as_bytes());
                assert_eq!(ours, reference.contains(&xxh3_64(other.as_bytes())));
                passed += usize::from(ours);
            }
            // About one in 256; far more means the lookup reads the wrong bytes.
            assert!(passed < 300, "{n} values: {passed} of 20 000 others passed");
        }
    }

    /// Each condition of §4.2.11's soundness, broken alone.
    ///
    /// Covers CT-2 · INV-Q15
    #[test]
    fn an_unsound_header_is_refused() {
        let sound = header(4, 3, 8, 16);
        assert!(Fuse8::from_bytes(&sound).is_ok());

        let cases = [
            ("short", sound[..HEADER_BYTES - 1].to_vec()),
            ("L not a power of two", header(3, 2, 9, 15)),
            ("L zero", header(0, u32::MAX, 8, 16)),
            (
                "L over 2^18",
                header(1 << 19, (1 << 19) - 1, 1 << 19, 3 << 19),
            ),
            ("M not L - 1", header(4, 7, 8, 16)),
            ("N zero", header(4, 3, 0, 8)),
            ("N not a multiple of L", header(4, 3, 6, 14)),
            ("too few fingerprints", header(4, 3, 8, 15)),
        ];
        for (what, bytes) in cases {
            assert!(Fuse8::from_bytes(&bytes).is_err(), "{what} was accepted");
        }

        let largest = header(1 << 18, (1 << 18) - 1, 1 << 18, 3 << 18);
        assert!(Fuse8::from_bytes(&largest).is_ok());
    }

    /// The largest index a sound header allows is the last fingerprint, so a
    /// lookup never reads past it whatever the hash.
    ///
    /// Covers CT-2 · INV-Q15
    #[test]
    fn a_sound_header_keeps_every_lookup_inside() {
        for (length, count_length) in [(1, 1), (1, 5), (4, 4), (4, 64), (1 << 18, 1 << 18)] {
            let set = Fuse8::from_bytes(&header(
                length,
                length - 1,
                count_length,
                (count_length + 2 * length) as usize,
            ))
            .unwrap();
            for i in 0..20_000u64 {
                set.contains(&i.to_le_bytes());
            }
        }
    }

    fn parse(sets: serde_json::Value) -> Result<Sets> {
        parse_sets(Some(&sets))
    }

    fn kind(result: Result<Sets>) -> Option<ErrorKind> {
        crate::error::error_kind(&result.expect_err("the sets must be refused"))
    }

    #[test]
    fn a_set_is_well_formed() {
        let fuse8 = BASE64.encode(wire(&["0xab", "0xcd"]));
        let set = |set: serde_json::Value| serde_json::json!({ "s": set });

        let parsed = parse(set(
            serde_json::json!({"fuse8": fuse8, "canaries": ["0xab"]}),
        ))
        .unwrap();
        assert_eq!(parsed["s"].canaries, ["0xab"]);
        assert!(parse_sets(None).unwrap().is_empty());
        assert!(parse_sets(Some(&serde_json::Value::Null))
            .unwrap()
            .is_empty());

        let unpadded = fuse8.trim_end_matches('=');
        let invalid = [
            serde_json::json!("not an object"),
            serde_json::json!({"canaries": ["0xab"]}),
            serde_json::json!({"fuse16": fuse8, "canaries": ["0xab"]}),
            serde_json::json!({"fuse8": fuse8, "fuse16": fuse8, "canaries": ["0xab"]}),
            serde_json::json!({"fuse8": 1, "canaries": ["0xab"]}),
            serde_json::json!({"fuse8": format!("{fuse8}\n"), "canaries": ["0xab"]}),
            serde_json::json!({"fuse8": BASE64.encode(header(4, 3, 8, 15)), "canaries": ["0xab"]}),
            serde_json::json!({"fuse8": fuse8}),
            serde_json::json!({"fuse8": fuse8, "canaries": []}),
            serde_json::json!({"fuse8": fuse8, "canaries": "0xab"}),
            serde_json::json!({"fuse8": fuse8, "canaries": [1]}),
            serde_json::json!({"fuse8": fuse8, "canaries": vec!["0xab"; MAX_SET_CANARIES + 1]}),
        ];
        for value in invalid {
            assert_eq!(
                kind(parse(set(value.clone()))),
                Some(ErrorKind::InvalidSet),
                "{value}"
            );
        }

        // Unpadded only differs from canonical where the length is not a
        // multiple of three.
        if unpadded != fuse8 {
            let value = serde_json::json!({"fuse8": unpadded, "canaries": ["0xab"]});
            assert_eq!(kind(parse(set(value))), Some(ErrorKind::InvalidSet));
        }

        assert_eq!(
            kind(parse(serde_json::json!([]))),
            Some(ErrorKind::MalformedRequest)
        );
    }
}
