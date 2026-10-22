//! Reference outputs for the TypeScript builders.
//!
//! fuse8 random <n>          n distinct random addresses, one per line
//! fuse8 vectors             `<text>\t<xxh3_64 hex>` for texts of every length 0..=128
//! fuse8 sweep <file> <max>  `<n>\t<descriptor hex>\t<FNV-1a of the filter>` for the first n lines, n = 0..=max
//! fuse8 build <file> <out>  the fuse8 bytes for the file's lines, in order

use fuse_build::{build_fuse8, pack};
use std::io::Write;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

fn random_address(i: u64) -> String {
    let words = [0, 1, 2].map(|seed| xxh3_64_with_seed(&i.to_le_bytes(), seed).to_le_bytes());
    format!("0x{}", hex(&words.concat()[..20]))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut out = std::io::BufWriter::new(std::io::stdout().lock());

    match args.get(1).map(String::as_str) {
        Some("random") => {
            let n: u64 = args[2].parse().unwrap();
            for i in 0..n {
                writeln!(out, "{}", random_address(i)).unwrap();
            }
        }
        Some("vectors") => {
            const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz0x";
            for len in 0..=128u64 {
                for variant in 0..4u64 {
                    let text: String = (0..len)
                        .map(|j| {
                            let h = xxh3_64_with_seed(&(len * 1000 + j).to_le_bytes(), variant);
                            ALPHABET[(h % ALPHABET.len() as u64) as usize] as char
                        })
                        .collect();
                    writeln!(out, "{text}\t{:016x}", xxh3_64(text.as_bytes())).unwrap();
                }
            }
        }
        Some("sweep") => {
            // Small sets need retries and the halved segment layout far more
            // often than large ones, so every size up to <max> is checked.
            let text = std::fs::read_to_string(&args[2]).unwrap();
            let values: Vec<&str> = text.lines().collect();
            let max: usize = args[3].parse().unwrap();
            for n in 0..=max {
                let (bytes, ends) = pack(values[..n].iter().copied());
                let filter = build_fuse8(&bytes, &ends).unwrap();
                let fnv = filter.iter().fold(0x811c9dc5u32, |h, &b| {
                    (h ^ b as u32).wrapping_mul(0x01000193)
                });
                writeln!(out, "{n}\t{}\t{fnv:08x}", hex(&filter[..20])).unwrap();
            }
        }
        Some("build") => {
            let text = std::fs::read_to_string(&args[2]).unwrap();
            let (bytes, ends) = pack(text.lines());
            let filter = build_fuse8(&bytes, &ends).unwrap();
            std::fs::write(&args[3], filter).unwrap();
        }
        _ => panic!("usage: fuse8 random <n> | vectors | sweep <file> <max> | build <file> <out>"),
    }
}
