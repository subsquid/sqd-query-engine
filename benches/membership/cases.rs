// Membership filters (issue #82): the same set as an `inList` and as each
// approximate structure, on EVM logs and transactions and on Solana token
// balances. Included via `#[path]` from bench binaries.
//
// `inList` takes the first `LIST_SIZE` values of the set, the most a request
// under 2 MiB can carry, so every structure runs on that subset too.
#![allow(dead_code)]

#[path = "../../tests/conformance/harness/store.rs"]
mod store;

use serde_json::{json, Value};
use sqd_query_engine::metadata::{load_dataset_description, DatasetDescription};
use sqd_query_engine::output::execute_chunk;
use sqd_query_engine::query::{compile, parse_query};
use sqd_query_engine::scan::membership::build;
use sqd_query_engine::scan::ParquetChunkReader;
use std::collections::BTreeSet;
use std::path::PathBuf;
use xxhash_rust::xxh3::{xxh3_64, xxh3_64_with_seed};

pub const LIST_SIZE: usize = 40_000;
pub const SET_SIZE: usize = 400_000;
/// Members each structure carries as they are, for the engine to look up.
pub const CANARIES: usize = 4;
/// The RFC draft's sizing: k = 9, 12.4 bits per element.
pub const FPR: f64 = 0.0026;

const SYNC: &str = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1";
const BASE58: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

#[derive(Clone, Copy, Debug)]
pub struct Target {
    /// The query `type`, and the bundled catalog of that name.
    pub kind: &'static str,
    /// The variable that names the chunk in `chunk-registry.yaml`, and the
    /// chunk without it.
    pub chunk_variable: &'static str,
    pub default_chunk: &'static str,
    pub request: &'static str,
    /// The table's file in the chunk, and the filtered column in it.
    pub file: &'static str,
    pub column: &'static str,
    pub filter: &'static str,
    /// The field selection: the row's key and the filtered value.
    pub fields: &'static str,
    pub keys: &'static [&'static str],
    /// The set's file under `SQD_MEMBERSHIP_DIR`, made by `derive` when missing.
    pub set: &'static str,
    pub derive: fn() -> Vec<String>,
}

pub const LOGS: Target = Target {
    kind: "evm",
    chunk_variable: "SQD_MEMBERSHIP_CHUNK",
    default_chunk: "evm-big",
    request: "logs",
    file: "logs.parquet",
    column: "address",
    filter: "address",
    fields: "log",
    keys: &["logIndex"],
    set: "evm-pairs",
    derive: derive_pairs,
};

pub const TRANSACTIONS: Target = Target {
    request: "transactions",
    file: "transactions.parquet",
    column: "from",
    filter: "from",
    fields: "transaction",
    keys: &["transactionIndex"],
    ..LOGS
};

pub const TOKEN_BALANCES: Target = Target {
    kind: "solana",
    chunk_variable: "SQD_MEMBERSHIP_SOLANA_CHUNK",
    default_chunk: "solana-small",
    request: "tokenBalances",
    file: "token_balances.parquet",
    column: "post_owner",
    filter: "postOwner",
    fields: "tokenBalance",
    keys: &["transactionIndex", "account"],
    set: "solana-owners",
    derive: derive_owners,
};

impl Target {
    pub fn chunk_name(&self) -> String {
        std::env::var(self.chunk_variable).unwrap_or_else(|_| self.default_chunk.into())
    }

    pub fn chunk_dir(&self) -> PathBuf {
        store::chunk(&self.chunk_name())
    }

    /// The chunk's last block, from the range in its name.
    pub fn chunk_last(&self) -> u64 {
        let reference = &store::named(&self.chunk_name()).reference;
        let range = reference.rsplit('/').next().unwrap();
        range.split('-').nth(1).unwrap().parse().unwrap()
    }

    pub fn catalog(&self) -> DatasetDescription {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("metadata")
            .join(format!("{}.yaml", self.kind));
        load_dataset_description(&path).unwrap()
    }

    /// The set, as the column stores it. Derived once into its file, and read
    /// from it after.
    pub fn set(&self) -> Vec<String> {
        let dir = std::env::var_os("SQD_MEMBERSHIP_DIR")
            .expect("SQD_MEMBERSHIP_DIR names the set directory");
        let path = PathBuf::from(dir).join(format!("{}.txt", self.set));
        if !path.exists() {
            let text = (self.derive)().join("\n");
            std::fs::write(&path, text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        }

        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        text.lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_owned)
            .collect()
    }

    fn query(&self, filter: Option<Value>) -> Vec<u8> {
        let item = match filter {
            Some(filter) => json!({ self.filter: filter }),
            None => json!({}),
        };
        let mut fields = json!({ self.filter: true });
        for key in self.keys {
            fields[*key] = true.into();
        }
        let query = json!({
            "type": self.kind,
            "fromBlock": 0,
            "fields": {"block": {"number": true}, self.fields: fields},
            self.request: [item],
        });
        serde_json::to_vec(&query).unwrap()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Filter {
    /// The unfiltered scan the others are checked against.
    All,
    List,
    Structure {
        kind: &'static str,
        skip_repeats: bool,
    },
}

/// Opens and builds nothing: Gungraun lists the cases again in every process.
#[derive(Clone, Debug)]
pub struct Case {
    pub name: String,
    pub target: Target,
    pub filter: Filter,
    /// How many of the set's values the filter carries.
    pub set_size: usize,
}

impl Case {
    pub fn query(&self, set: &[String]) -> Vec<u8> {
        self.target.query(self.filter_value(set))
    }

    /// What the item request carries under the filtered field.
    pub fn filter_value(&self, set: &[String]) -> Option<Value> {
        let values: Vec<&str> = set[..self.set_size].iter().map(String::as_str).collect();
        match self.filter {
            Filter::All => None,
            Filter::List => Some(json!(values)),
            Filter::Structure { kind, skip_repeats } => {
                let mut structure = match kind {
                    "bloom" => build::bloom(&values, FPR),
                    "splitBlock" => build::split_block(&values, FPR),
                    "fuse8" => build::fuse8(&values),
                    "fuse16" => build::fuse16(&values),
                    other => panic!("no structure {other}"),
                };
                structure["skipRepeats"] = skip_repeats.into();
                Some(build::with_canaries(structure, &values[..CANARIES]))
            }
        }
    }
}

/// In a fixed order: Gungraun names a case by its index.
pub fn cases(target: Target) -> Vec<Case> {
    let full = target.set().len();
    let listed = LIST_SIZE.min(full);
    let case = |name: String, filter, set_size| Case {
        name: format!("{}/{name}", target.request),
        target,
        filter,
        set_size,
    };

    let mut cases = vec![
        case("all".into(), Filter::All, 0),
        case(format!("inList.{listed}"), Filter::List, listed),
    ];
    for size in [listed, full] {
        for kind in ["bloom", "splitBlock", "fuse8", "fuse16"] {
            for skip_repeats in [false, true] {
                let suffix = if skip_repeats { "+skip" } else { "" };
                let filter = Filter::Structure { kind, skip_repeats };
                cases.push(case(format!("{kind}{suffix}.{size}"), filter, size));
            }
        }
    }

    cases
}

/// Every value of the filtered field in the rows `item` selects, read from the
/// target's default chunk page by page.
fn values_of(target: Target, item: Value) -> BTreeSet<String> {
    let meta = target.catalog();
    let chunk = ParquetChunkReader::open(&store::chunk(target.default_chunk)).unwrap();
    let last_block = {
        let reference = &store::named(target.default_chunk).reference;
        let range = reference.rsplit('/').next().unwrap();
        range.split('-').nth(1).unwrap().parse::<u64>().unwrap()
    };
    let mut query = json!({
        "type": target.kind,
        "fromBlock": 0,
        "fields": {target.fields: {target.filter: true}},
        target.request: [item],
    });
    let mut values = BTreeSet::new();

    loop {
        let parsed = parse_query(&serde_json::to_vec(&query).unwrap(), &meta).unwrap();
        let plan = compile(&parsed, &meta).unwrap();
        let Some(output) = execute_chunk(&plan, &meta, &chunk, false).unwrap() else {
            break;
        };
        let last = output.last_block();
        let json = output.into_json_lines();
        for line in json.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            let block: Value = serde_json::from_slice(line).unwrap();
            for row in block[target.request].as_array().into_iter().flatten() {
                if let Some(value) = row[target.filter].as_str() {
                    values.insert(value.to_owned());
                }
            }
        }

        if last >= last_block {
            break;
        }
        query["fromBlock"] = (last + 1).into();
    }

    values
}

/// Padded with random values to `SET_SIZE`. A member the chunk never mentions
/// changes a structure's size and nothing else, so the padding stands for
/// members idle in these blocks. Sorted by hash, so that a prefix of the set is
/// a uniform sample.
fn padded(mut set: BTreeSet<String>, random: impl Fn(u64) -> String) -> Vec<String> {
    let mut i = 0u64;
    while set.len() < SET_SIZE {
        set.insert(random(i));
        i += 1;
    }

    let mut set: Vec<String> = set.into_iter().collect();
    set.sort_by_key(|value| xxh3_64(value.as_bytes()));
    set
}

/// Every address that emits a V2-style `Sync` in `evm-big`.
fn derive_pairs() -> Vec<String> {
    let pairs = values_of(LOGS, json!({"topic0": [SYNC]}));
    assert!(!pairs.is_empty(), "no address emits Sync in evm-big");
    eprintln!("set: {} addresses emit Sync in evm-big", pairs.len());

    padded(pairs, |i| {
        let hash = |seed| xxh3_64_with_seed(&i.to_le_bytes(), seed);
        format!("0x{:016x}{:016x}{:08x}", hash(1), hash(2), hash(3) as u32)
    })
}

/// One in twenty of the owners in `solana-small`, the share a program's
/// derived addresses might hold, and base58 keys for the rest.
fn derive_owners() -> Vec<String> {
    let owners = values_of(TOKEN_BALANCES, json!({}));
    let total = owners.len();
    let owners: BTreeSet<String> = owners
        .into_iter()
        .filter(|owner| xxh3_64(owner.as_bytes()) % 20 == 0)
        .collect();
    assert!(
        !owners.is_empty(),
        "no token balance owners in solana-small"
    );
    eprintln!("set: {} of {total} owners in solana-small", owners.len());

    padded(owners, |i| {
        (0..44)
            .map(|j| BASE58[(xxh3_64_with_seed(&i.to_le_bytes(), j) % 58) as usize] as char)
            .collect()
    })
}
