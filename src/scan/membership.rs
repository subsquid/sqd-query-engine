//! A filter value that is an approximate set. The client builds a compact
//! structure over its exact set; the engine keeps every row whose value the
//! structure may hold. False positives are the client's to remove (ADR-8). A
//! false negative is a construction mismatch, and no client can see one.
//!
//! Research prototype for issue #82, behind the `membership-filter` feature.
//! Every structure hashes the bytes the chunk stores, which for a hex column is
//! the lowercase `0x…` text the response renders.

use super::predicate::{through_dictionary, unsupported, ArrayPredicate, Mask, StatRange};
use crate::text::StringColumn;
use arrow::array::{Array, BooleanArray, BooleanBufferBuilder};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use xorf::{BinaryFuse16, BinaryFuse8, Descriptor, Filter};
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};
use xxhash_rust::xxh64::xxh64;

/// The salts of Parquet's split-block Bloom filter, from the format spec.
const SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
];

pub enum Structure {
    /// The RFC draft: bit `xxh3_64_with_seed(value, i) mod m` for `i in 0..k`,
    /// LSB-first within a byte. A probe stops at the first unset bit.
    Bloom {
        bits: Vec<u8>,
        m: u64,
        k: u32,
    },
    /// Parquet's split-block Bloom filter: one XXH64, eight bits in one
    /// 32-byte block, tested without a branch.
    SplitBlock {
        blocks: Vec<[u32; 8]>,
    },
    /// Binary fuse filters keyed by `xxh3_64(value)`.
    Fuse8(BinaryFuse8),
    Fuse16(BinaryFuse16),
}

impl Structure {
    #[inline]
    pub fn contains(&self, value: &[u8]) -> bool {
        match self {
            Structure::Bloom { bits, m, k } => (0..*k).all(|seed| {
                let bit = xxh3_64_with_seed(value, seed as u64) % m;
                bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0
            }),
            Structure::SplitBlock { blocks } => {
                let hash = xxh64(value, 0);
                let block = &blocks[block_index(hash, blocks.len())];
                let key = hash as u32;

                let mut missing = 0;
                for (word, salt) in block.iter().zip(SALT) {
                    missing |= (1u32 << (key.wrapping_mul(salt) >> 27)) & !word;
                }
                missing == 0
            }
            Structure::Fuse8(filter) => filter.contains(&xxh3_64(value)),
            Structure::Fuse16(filter) => filter.contains(&xxh3_64(value)),
        }
    }

    /// The structure's size on the wire before base64.
    pub fn wire_bytes(&self) -> usize {
        match self {
            Structure::Bloom { bits, .. } => bits.len(),
            Structure::SplitBlock { blocks } => blocks.len() * 32,
            Structure::Fuse8(f) => Descriptor::DMA_LEN + f.fingerprints.len(),
            Structure::Fuse16(f) => Descriptor::DMA_LEN + f.fingerprints.len() * 2,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Structure::Bloom { .. } => "bloom",
            Structure::SplitBlock { .. } => "splitBlock",
            Structure::Fuse8(_) => "fuse8",
            Structure::Fuse16(_) => "fuse16",
        }
    }
}

/// Each canary costs a structure lookup when the request compiles.
const MAX_CANARIES: usize = 64;

fn block_index(hash: u64, blocks: usize) -> usize {
    (((hash >> 32) * blocks as u64) >> 32) as usize
}

pub struct MembershipPredicate {
    structure: Structure,
    /// Reuse the previous row's verdict when its value repeats. Pays only on a
    /// column the table is sorted by.
    skip_repeats: bool,
    /// For `describe`: the same structure reads the same in every run.
    digest: u64,
    /// Members as the client had them, before it normalised and hashed them.
    canaries: Vec<String>,
}

impl MembershipPredicate {
    pub fn new(structure: Structure, skip_repeats: bool, digest: u64) -> Self {
        Self {
            structure,
            skip_repeats,
            digest,
            canaries: Vec::new(),
        }
    }

    pub fn structure(&self) -> &Structure {
        &self.structure
    }

    pub fn canaries(&self) -> &[String] {
        &self.canaries
    }

    /// Refuse a structure built from other bytes than the column stores.
    ///
    /// A canary is a member as the client had it; `stored` turns it into the
    /// value the column would hold, or says why the column holds no such value.
    /// A structure that misses a canary misses rows the same way, and nothing in
    /// the answer would show it. A wrong structure passes all `c` canaries only
    /// by false positives, with probability `fpr^c`.
    pub fn check_canaries(
        &self,
        stored: impl Fn(&str) -> Result<String, String>,
    ) -> Result<(), String> {
        if self.canaries.is_empty() {
            return Err("canaries are required: a few members, as the client had them".into());
        }
        if self.canaries.len() > MAX_CANARIES {
            return Err(format!(
                "{} canaries, at most {MAX_CANARIES}",
                self.canaries.len()
            ));
        }

        for canary in &self.canaries {
            let value = stored(canary).map_err(|why| format!("canary {canary:?}: {why}"))?;
            if !self.structure.contains(value.as_bytes()) {
                return Err(format!(
                    "canary {canary:?} is not in the {}: it was built from other bytes \
                     than the column stores, which for this canary is {value:?} \
                     (check case, the 0x prefix, the hash and its seeds)",
                    self.structure.name()
                ));
            }
        }

        Ok(())
    }

    /// `{"bloom": {"m", "k", "bits"}}`, `{"splitBlock": "<base64>"}`,
    /// `{"fuse8": "<base64>"}` or `{"fuse16": "<base64>"}`, with `"canaries"`,
    /// a few members as the client had them, and an optional
    /// `"skipRepeats": bool`. A fuse filter is its 20-byte descriptor (seed u64,
    /// segment length, mask and count length u32, all little-endian) followed
    /// by its fingerprints.
    pub fn from_json(value: &serde_json::Value) -> Result<Self, String> {
        let object = value.as_object().ok_or("expected an object")?;
        let skip_repeats = match object.get("skipRepeats") {
            None => false,
            Some(flag) => flag.as_bool().ok_or("skipRepeats must be a boolean")?,
        };
        let canaries = match object.get("canaries") {
            None => Vec::new(),
            Some(list) => list
                .as_array()
                .ok_or("canaries must be a list of strings")?
                .iter()
                .map(|c| c.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
                .ok_or("canaries must be a list of strings")?,
        };

        let mut structures = object
            .iter()
            .filter(|(key, _)| !matches!(key.as_str(), "skipRepeats" | "canaries"));
        let (kind, body) = structures.next().ok_or("no structure given")?;
        if structures.next().is_some() {
            return Err("more than one structure given".into());
        }

        let encoded = match kind.as_str() {
            "bloom" => body.get("bits"),
            _ => Some(body),
        };
        let bytes = encoded
            .and_then(|e| e.as_str())
            .ok_or_else(|| format!("{kind}: expected base64 text"))
            .and_then(|e| BASE64.decode(e).map_err(|e| format!("{kind}: {e}")))?;
        let digest = xxh3_64(&bytes);

        let structure = match kind.as_str() {
            "bloom" => parse_bloom(body, bytes)?,
            "splitBlock" => parse_split_block(&bytes)?,
            "fuse8" => {
                let (descriptor, fingerprints) = parse_fuse(&bytes, 1)?;
                Structure::Fuse8(BinaryFuse8 {
                    descriptor,
                    fingerprints: fingerprints.into(),
                })
            }
            "fuse16" => {
                let (descriptor, fingerprints) = parse_fuse(&bytes, 2)?;
                let fingerprints: Vec<u16> = fingerprints
                    .chunks_exact(2)
                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                    .collect();
                Structure::Fuse16(BinaryFuse16 {
                    descriptor,
                    fingerprints: fingerprints.into(),
                })
            }
            other => return Err(format!("unknown structure '{other}'")),
        };

        Ok(Self {
            canaries,
            ..Self::new(structure, skip_repeats, digest)
        })
    }
}

fn parse_bloom(body: &serde_json::Value, bits: Vec<u8>) -> Result<Structure, String> {
    let m = body["m"].as_u64().ok_or("bloom: m must be an integer")?;
    let k = body["k"].as_u64().ok_or("bloom: k must be an integer")?;

    if m == 0 || !(1..=16).contains(&k) {
        return Err(format!(
            "bloom: m = {m}, k = {k}; need m > 0 and 1 <= k <= 16"
        ));
    }
    if bits.len() as u64 != m.div_ceil(8) {
        return Err(format!("bloom: {} bytes of bits for m = {m}", bits.len()));
    }

    Ok(Structure::Bloom {
        bits,
        m,
        k: k as u32,
    })
}

fn parse_split_block(bytes: &[u8]) -> Result<Structure, String> {
    if bytes.is_empty() || bytes.len() % 32 != 0 {
        return Err(format!(
            "splitBlock: {} bytes, need a multiple of 32",
            bytes.len()
        ));
    }

    let blocks = bytes
        .chunks_exact(32)
        .map(|block| {
            std::array::from_fn(|i| u32::from_le_bytes(block[i * 4..i * 4 + 4].try_into().unwrap()))
        })
        .collect();
    Ok(Structure::SplitBlock { blocks })
}

/// The descriptor, checked so that no key can index past the fingerprints:
/// `h0 < count_length`, and `h1`, `h2` stay inside the next two segments.
fn parse_fuse(bytes: &[u8], width: usize) -> Result<(Descriptor, &[u8]), String> {
    if bytes.len() < Descriptor::DMA_LEN {
        return Err("fuse: shorter than its descriptor".into());
    }
    let (head, fingerprints) = bytes.split_at(Descriptor::DMA_LEN);
    let word = |at: usize| u32::from_le_bytes(head[at..at + 4].try_into().unwrap());
    let descriptor = Descriptor {
        seed: u64::from_le_bytes(head[0..8].try_into().unwrap()),
        segment_length: word(8),
        segment_length_mask: word(12),
        segment_count_length: word(16),
    };

    let length = descriptor.segment_length as usize;
    let count_length = descriptor.segment_count_length as usize;
    let sound = length.is_power_of_two()
        && descriptor.segment_length_mask as usize == length - 1
        && count_length > 0
        && count_length % length == 0
        && fingerprints.len() % width == 0
        && fingerprints.len() / width >= count_length + 2 * length;
    if !sound {
        return Err("fuse: descriptor does not fit its fingerprints".into());
    }

    Ok((descriptor, fingerprints))
}

impl ArrayPredicate for MembershipPredicate {
    fn evaluate(&self, array: &dyn Array) -> Mask {
        if let Some(mask) = through_dictionary(self, array) {
            return mask;
        }

        let column = StringColumn::resolve(array)
            .ok_or_else(|| unsupported("a membership filter", array))?;
        let len = column.len();
        let mut builder = BooleanBufferBuilder::new(len);

        if self.skip_repeats {
            let mut previous: Option<(&str, bool)> = None;
            for i in 0..len {
                let verdict = column.value(i).is_some_and(|value| match previous {
                    Some((last, verdict)) if last == value => verdict,
                    _ => {
                        let verdict = self.structure.contains(value.as_bytes());
                        previous = Some((value, verdict));
                        verdict
                    }
                });
                builder.append(verdict);
            }
        } else {
            for i in 0..len {
                let verdict = column
                    .value(i)
                    .is_some_and(|value| self.structure.contains(value.as_bytes()));
                builder.append(verdict);
            }
        }

        Ok(BooleanArray::new(builder.finish(), column.nulls()))
    }

    /// Statistics say nothing about which values an opaque structure holds
    /// (INV-P16).
    fn can_skip(&self, _stats: &StatRange) -> bool {
        false
    }

    fn describe(&self) -> String {
        format!(
            "membership({}, {} bytes, xxh3 {:016x}, skipRepeats {})",
            self.structure.name(),
            self.structure.wire_bytes(),
            self.digest,
            self.skip_repeats
        )
    }
}

/// The reference builders: what a client sends for an exact set. They hash the
/// values as given; normalising them first is the caller's job.
pub mod build {
    use super::*;
    use serde_json::{json, Value};

    /// The RFC's sizing for a target false-positive rate.
    pub fn bloom<V: AsRef<[u8]>>(values: &[V], fpr: f64) -> Value {
        let n = values.len().max(1) as f64;
        let m = (-n * fpr.ln() / std::f64::consts::LN_2.powi(2)).ceil() as u64;
        let k = ((m as f64 / n) * std::f64::consts::LN_2)
            .round()
            .clamp(1.0, 16.0) as u64;

        let mut bits = vec![0u8; m.div_ceil(8) as usize];
        for value in values {
            for seed in 0..k {
                let bit = xxh3_64_with_seed(value.as_ref(), seed) % m;
                bits[(bit / 8) as usize] |= 1 << (bit % 8);
            }
        }
        json!({"bloom": {"m": m, "k": k, "bits": BASE64.encode(bits)}})
    }

    /// Sized by Parquet's formula, `-8 n / ln(1 - fpr^(1/8))` bits.
    pub fn split_block<V: AsRef<[u8]>>(values: &[V], fpr: f64) -> Value {
        let n = values.len().max(1) as f64;
        let bits = -8.0 * n / (1.0 - fpr.powf(1.0 / 8.0)).ln();
        let count = ((bits / 256.0).ceil() as usize).max(1);

        let mut blocks = vec![[0u32; 8]; count];
        for value in values {
            let hash = xxh64(value.as_ref(), 0);
            let block = &mut blocks[block_index(hash, count)];
            let key = hash as u32;
            for (word, salt) in block.iter_mut().zip(SALT) {
                *word |= 1 << (key.wrapping_mul(salt) >> 27);
            }
        }

        let bytes: Vec<u8> = blocks
            .iter()
            .flatten()
            .flat_map(|w| w.to_le_bytes())
            .collect();
        json!({"splitBlock": BASE64.encode(bytes)})
    }

    pub fn fuse8<V: AsRef<[u8]>>(values: &[V]) -> Value {
        fuse8_of_keys(keys(values))
    }

    pub fn fuse16<V: AsRef<[u8]>>(values: &[V]) -> Value {
        fuse16_of_keys(keys(values), u16::to_le_bytes)
    }

    /// The structure with `canaries` attached.
    pub fn with_canaries<S: AsRef<str>>(mut structure: Value, canaries: &[S]) -> Value {
        let canaries: Vec<&str> = canaries.iter().map(AsRef::as_ref).collect();
        structure["canaries"] = json!(canaries);
        structure
    }

    pub(crate) fn fuse8_of_keys(keys: Vec<u64>) -> Value {
        let filter = BinaryFuse8::try_from(keys).expect("distinct keys build a fuse filter");
        json!({"fuse8": BASE64.encode(fuse_bytes(&filter.descriptor, &filter.fingerprints))})
    }

    /// `encode` writes one fingerprint; the format is little-endian, and a test
    /// passes another order to see it refused.
    pub(crate) fn fuse16_of_keys(keys: Vec<u64>, encode: fn(u16) -> [u8; 2]) -> Value {
        let filter = BinaryFuse16::try_from(keys).expect("distinct keys build a fuse filter");
        let fingerprints: Vec<u8> = filter
            .fingerprints
            .iter()
            .flat_map(|f| encode(*f))
            .collect();
        json!({"fuse16": BASE64.encode(fuse_bytes(&filter.descriptor, &fingerprints))})
    }

    fn keys<V: AsRef<[u8]>>(values: &[V]) -> Vec<u64> {
        let mut keys: Vec<u64> = values.iter().map(|v| xxh3_64(v.as_ref())).collect();
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    fn fuse_bytes(descriptor: &Descriptor, fingerprints: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Descriptor::DMA_LEN + fingerprints.len());
        bytes.extend(descriptor.seed.to_le_bytes());
        bytes.extend(descriptor.segment_length.to_le_bytes());
        bytes.extend(descriptor.segment_length_mask.to_le_bytes());
        bytes.extend(descriptor.segment_count_length.to_le_bytes());
        bytes.extend(fingerprints);
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{DictionaryArray, StringArray};
    use arrow::datatypes::Int32Type;
    use std::sync::Arc;

    fn address(i: u64) -> String {
        format!("0x{:040x}", xxh3_64(&i.to_le_bytes()) as u128 * 0x9e37_79b9)
    }

    fn every_builder(values: &[&str]) -> Vec<serde_json::Value> {
        vec![
            build::bloom(values, 0.0026),
            build::split_block(values, 0.0026),
            build::fuse8(values),
            build::fuse16(values),
        ]
    }

    /// The half of INV-P9 no client can check: every member is kept, by every
    /// structure, with and without the repeat skip, plain and dictionary-encoded.
    #[test]
    fn no_member_is_dropped() {
        let members: Vec<String> = (0..20_000).map(address).collect();
        let others: Vec<String> = (20_000..60_000).map(address).collect();
        let values: Vec<&str> = members.iter().map(String::as_str).collect();

        let mut rows: Vec<Option<&str>> = Vec::new();
        for (i, member) in members.iter().enumerate() {
            rows.push(Some(member));
            rows.push(Some(&others[i]));
            rows.push(None);
            rows.push(Some(member));
        }
        let plain = StringArray::from(rows.clone());
        let dictionary: DictionaryArray<Int32Type> = rows.into_iter().collect();

        for json in every_builder(&values) {
            for skip in [false, true] {
                let mut json = json.clone();
                json["skipRepeats"] = skip.into();
                let predicate = MembershipPredicate::from_json(&json).unwrap();

                for array in [&plain as &dyn Array, &dictionary] {
                    let mask = predicate.evaluate(array).unwrap();
                    for row in (0..mask.len()).step_by(4) {
                        assert!(
                            mask.value(row),
                            "{} dropped row {row}",
                            predicate.describe()
                        );
                        assert!(
                            mask.value(row + 3),
                            "{} dropped row {}",
                            predicate.describe(),
                            row + 3
                        );
                        assert!(
                            !mask.is_valid(row + 2) || !mask.value(row + 2),
                            "null matched"
                        );
                    }
                }
            }
        }
    }

    /// The measured rate lands near the sized one; a structure that kept
    /// everything would pass the test above.
    #[test]
    fn false_positives_stay_near_the_sizing() {
        let members: Vec<String> = (0..50_000).map(address).collect();
        let others: Vec<String> = (50_000..250_000).map(address).collect();
        let values: Vec<&str> = members.iter().map(String::as_str).collect();
        let array = StringArray::from(others.iter().map(String::as_str).collect::<Vec<_>>());

        for (json, ceiling) in every_builder(&values)
            .into_iter()
            .zip([0.004, 0.006, 0.005, 0.0001])
        {
            let predicate = MembershipPredicate::from_json(&json).unwrap();
            let mask = predicate.evaluate(&array).unwrap();
            let rate = mask.true_count() as f64 / others.len() as f64;
            assert!(rate <= ceiling, "{}: {rate}", predicate.describe());
        }
    }

    /// The RFC draft's published vector for the zero address.
    #[test]
    fn the_rfc_vector() {
        let expected = "AAAAAAAAAAAAAAAAAAAAAAgABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAQAAAAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIgAAAAAAAAAAAAAAA=";
        let value = "0x0000000000000000000000000000000000000000";
        let (m, k) = (1024u64, 9u64);

        let mut bits = vec![0u8; 128];
        for seed in 0..k {
            let bit = xxh3_64_with_seed(value.as_bytes(), seed) % m;
            bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
        assert_eq!(BASE64.encode(bits), expected);
    }

    #[test]
    fn malformed_structures_are_refused() {
        let fuse = build::fuse8(&["0xab"]);
        let mut bytes = BASE64.decode(fuse["fuse8"].as_str().unwrap()).unwrap();
        bytes[16] = 0xff;
        let cases = [
            serde_json::json!({}),
            serde_json::json!({"bloom": {"m": 0, "k": 9, "bits": ""}}),
            serde_json::json!({"bloom": {"m": 64, "k": 17, "bits": "AAAAAAAAAAA="}}),
            serde_json::json!({"bloom": {"m": 64, "k": 9, "bits": "AAAA"}}),
            serde_json::json!({"splitBlock": "AAAA"}),
            serde_json::json!({"fuse8": BASE64.encode(bytes)}),
            serde_json::json!({"fuse8": "AAAA", "fuse16": "AAAA"}),
            serde_json::json!({"cuckoo": "AAAA"}),
        ];
        for case in cases {
            assert!(MembershipPredicate::from_json(&case).is_err(), "{case}");
        }
    }

    #[test]
    fn a_column_that_holds_no_text_is_refused() {
        let predicate = MembershipPredicate::from_json(&build::fuse8(&["0xab"])).unwrap();
        let ints: Arc<dyn Array> = Arc::new(arrow::array::Int32Array::from(vec![1]));
        assert!(predicate.evaluate(ints.as_ref()).is_err());
    }
}

/// What a wrongly built filter does to a query, through the bundled catalogs.
/// Each build here is a mistake a client can make, and each drops members
/// without a word if answered, so each must be refused before any row is read.
#[cfg(test)]
mod canary_tests {
    use super::*;
    use crate::error::{error_kind, ErrorKind};
    use crate::metadata::{load_dataset_description, DatasetDescription};
    use crate::query::{compile, parse_query};
    use serde_json::{json, Value};

    const BASE58: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    fn catalog(name: &str) -> DatasetDescription {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("metadata")
            .join(format!("{name}.yaml"));
        load_dataset_description(&path).unwrap()
    }

    fn compile_query(query: Value) -> anyhow::Result<()> {
        let meta = catalog(query["type"].as_str().unwrap());
        let parsed = parse_query(&serde_json::to_vec(&query).unwrap(), &meta)?;
        compile(&parsed, &meta).map(|_| ())
    }

    fn on_logs(filter: Value) -> anyhow::Result<()> {
        compile_query(json!({
            "type": "evm",
            "fromBlock": 0,
            "fields": {"log": {"address": true}},
            "logs": [{"address": filter}],
        }))
    }

    fn on_owners(filter: Value) -> anyhow::Result<()> {
        compile_query(json!({
            "type": "solana",
            "fromBlock": 0,
            "fields": {"tokenBalance": {"postOwner": true}},
            "tokenBalances": [{"postOwner": filter}],
        }))
    }

    fn assert_refused(result: anyhow::Result<()>, mistake: &str) {
        let err = result.expect_err(mistake);
        let text = format!("{err:#}");
        assert_eq!(
            error_kind(&err),
            Some(ErrorKind::InvalidFilterValue),
            "{mistake}: {text}"
        );
        assert!(
            text.contains("canar"),
            "{mistake}: refused for another reason: {text}"
        );
    }

    /// Lowercase hex addresses, as an EVM column stores them.
    fn members() -> Vec<String> {
        (0..1_000u64)
            .map(|i| {
                let hash = |seed| xxh3_64_with_seed(&i.to_le_bytes(), seed);
                format!("0x{:016x}{:016x}{:08x}", hash(1), hash(2), hash(3) as u32)
            })
            .collect()
    }

    /// What a client holds when it copied addresses from an explorer.
    fn mixed_case(address: &str) -> String {
        let digits: String = address[2..]
            .chars()
            .enumerate()
            .map(|(i, c)| {
                if i % 2 == 0 {
                    c.to_ascii_uppercase()
                } else {
                    c
                }
            })
            .collect();
        format!("0x{digits}")
    }

    fn every_structure<V: AsRef<[u8]>>(values: &[V]) -> Vec<Value> {
        vec![
            build::bloom(values, 0.0026),
            build::split_block(values, 0.0026),
            build::fuse8(values),
            build::fuse16(values),
        ]
    }

    #[test]
    fn a_correct_build_passes_its_canaries() {
        let members = members();
        let as_given: Vec<String> = members.iter().map(|m| mixed_case(m)).collect();

        // Normalised before the build; canaries as the client got them.
        for structure in every_structure(&members) {
            on_logs(build::with_canaries(structure, &as_given[..4])).unwrap();
        }
    }

    #[test]
    fn a_build_from_values_in_another_case_is_refused() {
        let as_given: Vec<String> = members().iter().map(|m| mixed_case(m)).collect();

        for structure in every_structure(&as_given) {
            let filter = build::with_canaries(structure, &as_given[..4]);
            assert_refused(on_logs(filter), "hashed mixed-case addresses");
        }
    }

    #[test]
    fn a_build_without_the_prefix_is_refused() {
        let bare: Vec<String> = members().iter().map(|m| m[2..].to_owned()).collect();
        let filter = build::with_canaries(build::fuse8(&bare), &bare[..4]);
        assert_refused(on_logs(filter), "hashed hex without 0x");
    }

    #[test]
    fn a_build_from_raw_bytes_is_refused() {
        let members = members();
        let raw: Vec<Vec<u8>> = members
            .iter()
            .map(|m| {
                (0..20)
                    .map(|i| u8::from_str_radix(&m[2 + 2 * i..4 + 2 * i], 16).unwrap())
                    .collect()
            })
            .collect();

        let filter = build::with_canaries(build::fuse8(&raw), &members[..4]);
        assert_refused(on_logs(filter), "hashed the 20 bytes instead of the text");
    }

    #[test]
    fn a_fuse_keyed_by_another_hash_is_refused() {
        let members = members();
        let mut keys: Vec<u64> = members.iter().map(|m| xxh64(m.as_bytes(), 0)).collect();
        keys.sort_unstable();

        let filter = build::with_canaries(build::fuse8_of_keys(keys), &members[..4]);
        assert_refused(on_logs(filter), "keyed by xxh64");
    }

    #[test]
    fn fuse16_fingerprints_in_the_other_byte_order_are_refused() {
        let members = members();
        let mut keys: Vec<u64> = members.iter().map(|m| xxh3_64(m.as_bytes())).collect();
        keys.sort_unstable();

        let structure = build::fuse16_of_keys(keys, u16::to_be_bytes);
        let filter = build::with_canaries(structure, &members[..4]);
        assert_refused(on_logs(filter), "big-endian fingerprints");
    }

    #[test]
    fn a_bloom_with_other_seeds_is_refused() {
        let members = members();
        // Sparse, so that no canary passes on bits other members set.
        let (m, k) = (1u64 << 20, 9u64);

        let mut bits = vec![0u8; (m / 8) as usize];
        for member in &members {
            for seed in 1..=k {
                let bit = xxh3_64_with_seed(member.as_bytes(), seed) % m;
                bits[(bit / 8) as usize] |= 1 << (bit % 8);
            }
        }
        let structure = json!({"bloom": {"m": m, "k": k, "bits": BASE64.encode(bits)}});

        let filter = build::with_canaries(structure, &members[..4]);
        assert_refused(on_logs(filter), "seeds 1..=k instead of 0..k");
    }

    #[test]
    fn a_filter_without_canaries_is_refused() {
        assert_refused(on_logs(build::fuse8(&members())), "no canaries");
        let empty: [&str; 0] = [];
        let filter = build::with_canaries(build::fuse8(&members()), &empty);
        assert_refused(on_logs(filter), "an empty canary list");
    }

    #[test]
    fn a_canary_the_column_cannot_hold_is_refused() {
        let owners: Vec<String> = (0..1_000u64)
            .map(|i| {
                (0..44u64)
                    .map(|j| BASE58[(xxh3_64_with_seed(&i.to_le_bytes(), j) % 58) as usize] as char)
                    .collect()
            })
            .collect();
        let filter = build::with_canaries(build::fuse8(&owners), &owners[..4]);
        on_owners(filter).unwrap();

        // Hex where the column holds base58: the build and the canaries agree,
        // and no stored owner can match either.
        let members = members();
        let filter = build::with_canaries(build::fuse8(&members), &members[..4]);
        assert_refused(on_owners(filter), "hex canaries on a base58 column");
    }
}
