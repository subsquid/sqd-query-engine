//! The filters a set may stand in for, and sets built the way a client builds
//! them.
//!
//! [INV-Q16] lets a set stand only in a list of text, so a test of sets
//! reaches only as far as the filters it tries. Reading them off the catalog
//! keeps a filter added later inside every such test.
//!
//! [INV-Q16]: ../../../spec/07-invariants.md#inv-q16

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use sqd_query_engine::metadata::{ColumnType, DatasetDescription, JsonEncoding, SpecialFilter};
use sqd_query_engine::output::snake_to_camel;
use xorf::{BinaryFuse8, Filter};
use xxhash_rust::xxh3::xxh3_64;

/// Every filter of a catalog, under the request key a client sends it in.
pub struct CatalogFilter {
    /// A table's request name, or an alias over it.
    pub request: String,
    /// The filter key, camelCased the way a client writes it.
    pub key: String,
    /// The catalog's name for the table it reads.
    pub table: String,
    /// The text a set on it is looked up by, or `None` when it takes no set.
    pub rendering: Option<Rendering>,
    /// The column it reads, for a filter that takes a list of values.
    pub column: Option<String>,
    /// The output name of its table, the key of a selection under `fields`.
    pub output: String,
    /// The fields that render its column, camelCased.
    pub fields: Vec<String>,
}

/// The text a column renders, which is what a set is looked up by (§4.2.11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rendering {
    Hex,
    Base58,
    Utf8,
}

impl Rendering {
    /// A value of this rendering, as a client would hold it.
    pub fn sample(self) -> &'static str {
        match self {
            Rendering::Hex => "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
            Rendering::Base58 => "So11111111111111111111111111111111111111112",
            Rendering::Utf8 => "transfer",
        }
    }

    /// Whether `value` is one a column of this rendering can hold.
    pub fn holds(self, value: &str) -> bool {
        const BASE58: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

        match self {
            Rendering::Hex => value.strip_prefix("0x").is_some_and(|digits| {
                digits.len() % 2 == 0 && digits.bytes().all(|b| b.is_ascii_hexdigit())
            }),
            Rendering::Base58 => !value.is_empty() && value.bytes().all(|b| BASE58.contains(&b)),
            Rendering::Utf8 => true,
        }
    }
}

/// Every filter of `catalog`, on its tables and on the aliases over them.
pub fn every_filter(catalog: &DatasetDescription) -> Vec<CatalogFilter> {
    let mut found = Vec::new();

    for (name, table) in &catalog.tables {
        let surface = table.request();
        for key in &surface.filters {
            let column = match surface.special_filters.get(key) {
                None => Some(key),
                Some(SpecialFilter::ColumnAlias { column }) => Some(column),
                Some(_) => None,
            };
            found.push(filter(catalog, table.request_name(name), key, name, column));
        }
    }

    for (request, alias) in &catalog.aliases {
        for key in &alias.filters {
            let column = match alias.special_filters.get(key) {
                None => Some(key),
                Some(SpecialFilter::ColumnAlias { column }) => Some(column),
                Some(_) => None,
            };
            found.push(filter(catalog, request, key, &alias.table, column));
        }
    }

    found
}

fn filter(
    catalog: &DatasetDescription,
    request: &str,
    key: &str,
    table: &str,
    column: Option<&String>,
) -> CatalogFilter {
    let description = catalog.table(table).unwrap();
    let declared = column.and_then(|column| description.column(column));

    let rendering = declared.and_then(|c| match (&c.encoding, &c.data_type) {
        (None, ColumnType::String) => Some(Rendering::Utf8),
        (Some(JsonEncoding::HexBytes), ColumnType::String | ColumnType::FixedBinary(_)) => {
            Some(Rendering::Hex)
        }
        (Some(JsonEncoding::Base58), ColumnType::String | ColumnType::FixedBinary(_)) => {
            Some(Rendering::Base58)
        }
        _ => None,
    });

    let fields = match column {
        Some(column) => description
            .output
            .fields
            .iter()
            .filter(|field| description.field_columns(field).any(|c| c == column))
            .map(|field| snake_to_camel(field))
            .collect(),
        None => Vec::new(),
    };

    CatalogFilter {
        request: request.to_string(),
        key: snake_to_camel(key),
        table: table.to_string(),
        rendering,
        column: column.cloned(),
        output: description.output.name.clone().unwrap_or_default(),
        fields,
    }
}

/// A set as a client builds it: a binary fuse filter over the XXH3 of each
/// value's text, by the builder the lookup was written against.
pub struct BuiltSet {
    filter: BinaryFuse8,
}

impl BuiltSet {
    pub fn of<S: AsRef<str>>(values: &[S]) -> BuiltSet {
        BuiltSet::of_keys(values.iter().map(|v| xxh3_64(v.as_ref().as_bytes())))
    }

    /// Over keys made some other way, as a client that builds a set wrong does.
    pub fn of_keys(keys: impl IntoIterator<Item = u64>) -> BuiltSet {
        let mut keys: Vec<u64> = keys.into_iter().collect();
        keys.sort_unstable();
        keys.dedup();

        BuiltSet {
            filter: BinaryFuse8::try_from(&keys).unwrap(),
        }
    }

    /// The `fuse8` value a request carries.
    pub fn wire(&self) -> String {
        let descriptor = &self.filter.descriptor;
        let mut bytes = descriptor.seed.to_le_bytes().to_vec();
        bytes.extend(descriptor.segment_length.to_le_bytes());
        bytes.extend(descriptor.segment_length_mask.to_le_bytes());
        bytes.extend(descriptor.segment_count_length.to_le_bytes());
        bytes.extend(self.filter.fingerprints.iter());

        BASE64.encode(bytes)
    }

    /// The set as it is declared under `sets`.
    pub fn declared<S: AsRef<str>>(&self, canaries: &[S]) -> serde_json::Value {
        let canaries: Vec<&str> = canaries.iter().map(AsRef::as_ref).collect();
        serde_json::json!({ "fuse8": self.wire(), "canaries": canaries })
    }

    /// The builder's own lookup of `value`'s text, independent of the engine's.
    pub fn passes(&self, value: &str) -> bool {
        self.filter.contains(&xxh3_64(value.as_bytes()))
    }
}
