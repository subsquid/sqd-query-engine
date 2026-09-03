use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level dataset description. One per chain type (evm, solana, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetDescription {
    /// Dataset name (e.g., "solana", "evm")
    pub name: String,
    /// Table definitions keyed by table name.
    /// Uses IndexMap to preserve YAML insertion order (determines output table ordering in blocks).
    pub tables: indexmap::IndexMap<String, TableDescription>,
    /// Query aliases: maps alternative query names to existing tables with implicit predicates.
    /// E.g., "evmLogs" → events table with implicit name="EVM.Log" filter.
    #[serde(default)]
    pub query_aliases: BTreeMap<String, QueryAlias>,
}

/// A query alias that maps to an existing table with implicit predicates and filter aliases.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryAlias {
    /// The actual table name this alias refers to.
    pub table: String,
    /// Implicit predicates: always-applied filters (column → list of allowed values).
    #[serde(default)]
    pub implicit_predicates: BTreeMap<String, Vec<String>>,
    /// Filter column aliases: maps query filter keys to actual column names.
    #[serde(default)]
    pub filter_aliases: BTreeMap<String, String>,
    /// Relations available (same format as table relations).
    #[serde(default)]
    pub relations: BTreeMap<String, RelationDef>,
    /// Column filters this alias accepts, in place of the table's own list. An
    /// alias is a narrower view of its table and usually exposes fewer filters.
    ///
    /// Required, with no default: an alias that omits the key would silently
    /// accept no filters at all, and every client filter on it would 400.
    /// Declaring `filters: []` says that on purpose.
    pub filters: Vec<String>,
}

/// Description of a single parquet table within a dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableDescription {
    /// Column that holds the block number for block range filtering.
    /// Defaults to "block_number". The blocks table typically overrides this to "number".
    #[serde(default = "default_block_number_column")]
    pub block_number_column: String,

    /// Column holding the hash of the preceding block. Only the block table sets
    /// it; declaring it is what enables fork detection for the dataset.
    #[serde(default)]
    pub parent_hash_column: Option<String>,

    /// Column holding the *number* of the preceding block. Chains that skip
    /// numbers (Solana slots) need it; where it is absent the predecessor is
    /// taken to be `number - 1`.
    #[serde(default)]
    pub parent_number_column: Option<String>,

    /// Hierarchical address column for children/parents relations.
    /// E.g., "instruction_address" for Solana instructions, "trace_address" for EVM traces.
    #[serde(default)]
    pub address_column: Option<String>,

    /// Columns that define the sort order of items within a block.
    /// Used for output ordering. E.g., ["transaction_index", "instruction_address"]
    #[serde(default)]
    pub item_order_keys: Vec<String>,

    /// The sort key used when writing parquet files.
    /// Data is physically sorted by these columns.
    /// E.g., ["program_id", "d1", "b9", "block_number", "transaction_index"]
    #[serde(default)]
    pub sort_key: Vec<String>,

    /// Column definitions keyed by column name.
    /// Uses IndexMap to preserve YAML definition order (determines output field ordering).
    pub columns: IndexMap<String, ColumnDescription>,

    /// Tables that are children of this table (joined via same key prefix).
    /// E.g., transactions has children [logs, balances, token_balances]
    #[serde(default)]
    pub children: Vec<String>,

    /// The parent table key columns for child relationship.
    /// E.g., for logs as child of transactions: ["block_number", "transaction_index"]
    #[serde(default)]
    pub parent_key: Vec<String>,

    /// Name used in query JSON filter arrays (e.g., "logs", "stateDiffs").
    #[serde(default)]
    pub query_name: Option<String>,

    /// Name used in query fields object (e.g., "log", "stateDiff").
    #[serde(default)]
    pub field_name: Option<String>,

    /// Relations available for query items on this table.
    #[serde(default)]
    pub relations: BTreeMap<String, RelationDef>,

    /// Special filters not directly mapped to a single column.
    #[serde(default)]
    pub special_filters: BTreeMap<String, SpecialFilter>,

    /// Column filters this table accepts, each naming a column of the same name.
    /// Declaring them is what keeps the filter surface closed: without a list,
    /// every column — blooms, size counters, denormalised extractions — would be
    /// filterable, and the column list would become the public API.
    /// `special_filters` and `relations` are accepted in addition.
    ///
    /// Required, with no default, for the same reason [`QueryAlias::filters`] is:
    /// omitting the key would accept no filters at all and 400 every client
    /// filter on the table, and `deny_unknown_fields` does not catch an absent
    /// key the way it catches a misspelled one. `filters: []` says it on purpose.
    pub filters: Vec<String>,

    /// Output fields this table can emit, each naming a non-system column, a
    /// virtual field, or a field-group request key.
    ///
    /// Declaring them is what keeps the *output* surface closed, for the reason
    /// [`TableDescription::filters`] closes the input one: derived from the
    /// column list instead, every column the catalog carries for filtering,
    /// grouping, joining or rolling becomes a field a client may pin on, and the
    /// physical layout becomes the wire contract (INV-Q14).
    ///
    /// Empty is "nothing is selectable", which is only meaningful for a table no
    /// `fields` key can name. The validator requires a list from every table that
    /// declares a `field_name`.
    #[serde(default)]
    pub fields: Vec<String>,

    /// Virtual fields that combine multiple columns into one output field.
    /// E.g., "accounts" → roll(a0..a15, rest_accounts), "topics" → roll(topic0..topic3)
    #[serde(default)]
    pub virtual_fields: BTreeMap<String, VirtualField>,

    /// Polymorphic field grouping: columns with certain prefixes are grouped into
    /// nested JSON objects based on a tag column value.
    /// E.g., EVM traces: `create_from` → `action.from` when type=create.
    #[serde(default)]
    pub field_groups: Option<FieldGrouping>,
}

impl TableDescription {
    /// The physical parquet column an output key reads, when the catalog
    /// declares one: an ordinary column, or a field-group request key that
    /// renames it (`call_call_type` → `call_type`).
    ///
    /// A virtual field resolves to nothing — it rolls several columns, and the
    /// caller expands it. Everything that projects, weighs or requires an output
    /// column goes through here, so the three cannot disagree on what a key
    /// means.
    pub fn physical_output_column(&self, key: &str) -> Option<&str> {
        if let Some((name, _)) = self.columns.get_key_value(key) {
            return Some(name.as_str());
        }

        self.field_groups
            .as_ref()
            .and_then(|fg| fg.physical_column_for_request(key))
    }

    /// Whether a `fields` key names something this table can emit. The declared
    /// list is the answer, not the column list it is drawn from: a column exists
    /// for whatever the engine needs it for, and only the catalog says which of
    /// them a client may ask for (INV-Q14).
    pub fn is_selectable_field(&self, name: &str) -> bool {
        self.fields.iter().any(|f| f == name)
    }
}

fn default_block_number_column() -> String {
    "block_number".to_string()
}

/// Weight source for a column — either a reference to a size column or a fixed value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WeightSource {
    Column(String),
    Fixed(u64),
}

/// Description of a single column in a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnDescription {
    /// Arrow/parquet data type
    #[serde(rename = "type")]
    pub data_type: ColumnType,

    /// JSON output encoding override. When set, controls how this column is serialized in JSON.
    /// E.g., `hex` for "0x..."-prefixed hex strings, `string` for number-as-quoted-string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_encoding: Option<JsonEncoding>,

    /// Whether parquet statistics (min/max) are written for this column
    #[serde(default)]
    pub stats: bool,

    /// Whether dictionary encoding is used
    #[serde(default)]
    pub dictionary: bool,

    /// Weight source for response size limiting.
    /// References a size column (e.g., "input_size") or a fixed weight (e.g., 0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<WeightSource>,

    /// System column — not included in user output (e.g., size columns, bloom filters).
    #[serde(default)]
    pub system: bool,

    /// Compare filter values case-insensitively (INV-P8).
    ///
    /// A `hex` column folds already — its values are `0x…` lowercase by §1.5.
    /// This is for a column that holds hex *without* the prefix, which Tron's
    /// addresses and topics do: they render verbatim, so the encoding cannot
    /// say it, and a client sending an upper-case address would otherwise get an
    /// empty response rather than its rows.
    #[serde(default)]
    pub fold_case: bool,
}

impl ColumnDescription {
    /// Whether filter values on this column compare case-insensitively.
    pub fn folds_case(&self) -> bool {
        self.fold_case || self.json_encoding == Some(JsonEncoding::Hex)
    }
}

/// Supported column data types (maps to Arrow types).
///
/// Serialized as simple strings: "uint64", "string", "fixed_binary_64", etc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int16,
    Int32,
    Int64,
    Float64,
    Boolean,
    String,
    TimestampSecond,
    TimestampMillisecond,
    ListUInt8,
    ListUInt32,
    ListString,
    ListStruct,
    Struct,
    Decimal128,
    /// Fixed-size binary with given byte length.
    FixedBinary(usize),
}

impl ColumnType {
    /// A list column contributes its elements to a roll rather than one value.
    pub fn is_list(&self) -> bool {
        matches!(
            self,
            ColumnType::ListUInt8
                | ColumnType::ListUInt32
                | ColumnType::ListString
                | ColumnType::ListStruct
        )
    }
}

impl Serialize for ColumnType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let s = match self {
            ColumnType::UInt8 => "uint8".to_string(),
            ColumnType::UInt16 => "uint16".to_string(),
            ColumnType::UInt32 => "uint32".to_string(),
            ColumnType::UInt64 => "uint64".to_string(),
            ColumnType::Int16 => "int16".to_string(),
            ColumnType::Int32 => "int32".to_string(),
            ColumnType::Int64 => "int64".to_string(),
            ColumnType::Float64 => "float64".to_string(),
            ColumnType::Boolean => "boolean".to_string(),
            ColumnType::String => "string".to_string(),
            ColumnType::TimestampSecond => "timestamp_second".to_string(),
            ColumnType::TimestampMillisecond => "timestamp_millisecond".to_string(),
            ColumnType::ListUInt8 => "list_uint8".to_string(),
            ColumnType::ListUInt32 => "list_uint32".to_string(),
            ColumnType::ListString => "list_string".to_string(),
            ColumnType::ListStruct => "list_struct".to_string(),
            ColumnType::Struct => "struct".to_string(),
            ColumnType::Decimal128 => "decimal128".to_string(),
            ColumnType::FixedBinary(size) => format!("fixed_binary_{}", size),
        };
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for ColumnType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = std::string::String::deserialize(deserializer)?;
        match s.as_str() {
            "uint8" => Ok(ColumnType::UInt8),
            "uint16" => Ok(ColumnType::UInt16),
            "uint32" => Ok(ColumnType::UInt32),
            "uint64" => Ok(ColumnType::UInt64),
            "int16" => Ok(ColumnType::Int16),
            "int32" => Ok(ColumnType::Int32),
            "int64" => Ok(ColumnType::Int64),
            "float64" => Ok(ColumnType::Float64),
            "boolean" => Ok(ColumnType::Boolean),
            "string" => Ok(ColumnType::String),
            "timestamp_second" => Ok(ColumnType::TimestampSecond),
            "timestamp_millisecond" => Ok(ColumnType::TimestampMillisecond),
            "list_uint8" => Ok(ColumnType::ListUInt8),
            "list_uint32" => Ok(ColumnType::ListUInt32),
            "list_string" => Ok(ColumnType::ListString),
            "list_struct" => Ok(ColumnType::ListStruct),
            "struct" => Ok(ColumnType::Struct),
            "decimal128" => Ok(ColumnType::Decimal128),
            _ if s.starts_with("fixed_binary_") => {
                let size: usize = s["fixed_binary_".len()..].parse().map_err(|_| {
                    serde::de::Error::custom(format!("invalid fixed_binary size in '{}'", s))
                })?;
                Ok(ColumnType::FixedBinary(size))
            }
            _ => Err(serde::de::Error::custom(format!(
                "unknown column type '{}'",
                s
            ))),
        }
    }
}

/// JSON output encoding for a column.
/// Controls how the column value is serialized in JSON output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JsonEncoding {
    /// Hex-encoded string with "0x" prefix (e.g., addresses, hashes)
    Hex,
    /// Base58-encoded string (Solana addresses)
    Base58,
    /// Number as quoted string (for large integers that exceed JS safe range)
    String,
    /// Raw JSON pass-through — string column containing JSON, embedded without quoting
    Json,
    /// Unsigned integer as a quoted hex string, zero-padded to the column's
    /// physical width (`uint16` 1600 → `"0x0640"`). The padding is load-bearing:
    /// `"0x0640"` and `"0x640"` are different discriminators. A `uint64` above
    /// 2^53 emitted as a JSON number would round in every JavaScript client.
    HexNumber,
    /// Solana transaction version: -1 → "legacy", else number
    SolanaTxVersion,
    /// Timestamp as raw millisecond integer (no conversion to seconds)
    TimestampMillisecond,
}

/// Description of a relation available in query items.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationDef {
    /// Target table name.
    pub table: String,
    /// Relation kind (default: join).
    #[serde(default)]
    pub kind: RelationKind,
    /// Join key columns (same for both sides).
    #[serde(default)]
    pub key: Vec<String>,
    /// Left side key columns (overrides `key`).
    #[serde(default)]
    pub left_key: Vec<String>,
    /// Right side key columns (overrides `key`).
    #[serde(default)]
    pub right_key: Vec<String>,
}

impl RelationDef {
    pub fn effective_left_key(&self) -> &[String] {
        if !self.left_key.is_empty() {
            &self.left_key
        } else {
            &self.key
        }
    }
    pub fn effective_right_key(&self) -> &[String] {
        if !self.right_key.is_empty() {
            &self.right_key
        } else {
            &self.key
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RelationKind {
    #[default]
    Join,
    Children,
    Parents,
}

/// `P-MAX-DISCRIMINATOR-BYTES` (spec/09-parameters.md §9.1): the longest prefix
/// a discriminator can dispatch on, and so the largest length a catalog may map
/// to a column.
pub const MAX_DISCRIMINATOR_BYTES: usize = 16;

/// Special filter that doesn't map directly to a single column.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SpecialFilter {
    /// Dispatch hex prefix to d1-d16 columns by byte length.
    #[serde(rename = "discriminator")]
    Discriminator {
        /// Maps stringified byte length → column name (e.g., "1" → "d1").
        columns: BTreeMap<String, String>,
    },
    /// Bloom filter membership test.
    #[serde(rename = "bloom_filter")]
    BloomFilter {
        column: String,
        num_bytes: usize,
        num_hashes: usize,
    },
    /// Range filter: column >= value.
    #[serde(rename = "range_gte")]
    RangeGte { column: String },
    /// Range filter: column <= value.
    #[serde(rename = "range_lte")]
    RangeLte { column: String },
    /// Alias: query key maps to a different physical column name.
    #[serde(rename = "column_alias")]
    ColumnAlias { column: String },
    /// Boolean flag filter: when `true`, emits `column >= value` against a fixed
    /// metadata-defined constant. Used for EVM trace `*NonZero` filters, e.g.
    /// `callValueNonZero: true` → `call_value >= "0x1"` (minimal-form hex, so this
    /// keeps every non-zero value and drops the zero representation "0x").
    #[serde(rename = "gte_const")]
    GteConst { column: String, value: String },
}

/// Polymorphic field grouping for tables whose output structure depends on a tag column.
///
/// Physical columns with certain prefixes are grouped into nested JSON sub-objects.
/// The tag column determines which variant is active for a given row, controlling
/// which prefix group's columns appear in the output.
///
/// Example: EVM traces have `type` as tag column with variants:
/// - `create` → `action.{from,value,gas,init}`, `result.{gasUsed,code,address}`
/// - `call` → `action.{from,to,value,gas,input,sighash,type}`, `result.{gasUsed,output}`
/// - `suicide` → `action.{address,refundAddress,balance}`
/// - `reward` → `action.{author,value,type}`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldGrouping {
    /// Column that determines the variant (e.g., "type").
    pub tag_column: String,
    /// Columns that appear at the top level for all variants (e.g., transaction_index, trace_address).
    #[serde(default)]
    pub base_fields: Vec<String>,
    /// Per-variant definitions mapping tag value → groups.
    pub variants: BTreeMap<String, BTreeMap<String, Vec<FieldMapping>>>,
}

impl FieldGrouping {
    /// Resolve a requested output field key to the physical parquet column it
    /// reads, if any mapping declares it. Handles request keys that differ from
    /// the column name (e.g. `call_call_type` → `call_type`).
    pub fn physical_column_for_request(&self, request_key: &str) -> Option<&str> {
        for groups in self.variants.values() {
            for mappings in groups.values() {
                for m in mappings {
                    if m.request_key() == request_key {
                        return Some(m.column.as_str());
                    }
                }
            }
        }
        None
    }
}

/// Maps a physical column to a JSON field name within a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldMapping {
    /// Physical column name in parquet.
    pub column: String,
    /// JSON field name in output (camelCase).
    pub field: String,
    /// Optional request key (snake_case) that selects this mapping, when it
    /// differs from `column`. Lets one physical column back several output
    /// fields — e.g. EVM `call_type` powers both `action.type` (request `type`)
    /// and `action.callType` (request `call_call_type`).
    #[serde(default)]
    pub request: Option<String>,
}

impl FieldMapping {
    /// The query-field key that selects this mapping (defaults to `column`).
    pub fn request_key(&self) -> &str {
        self.request.as_deref().unwrap_or(&self.column)
    }
}

/// A virtual field that combines multiple physical columns into one output value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum VirtualField {
    /// Roll multiple columns into a single JSON array.
    /// Non-nullable columns come first, then nullable (stops at first null),
    /// then an optional trailing list column (spread into array).
    #[serde(rename = "roll")]
    Roll { columns: Vec<String> },
}

impl DatasetDescription {
    /// Get a table description by name.
    pub fn table(&self, name: &str) -> Option<&TableDescription> {
        self.tables.get(name)
    }
}

impl TableDescription {
    /// Whether a block number alone identifies a row of this table, which is what
    /// makes it the dataset's block table (INV-D3). The item key is
    /// `block_number_column ++ item_order_keys ++ address_column?`, so an empty
    /// tail is the whole test.
    ///
    /// The sort key says nothing about it: that is storage layout, and no answer
    /// may depend on it (INV-D8).
    pub fn is_block_table(&self) -> bool {
        self.item_order_keys.is_empty() && self.address_column.is_none()
    }

    /// Get a column description by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDescription> {
        self.columns.get(name)
    }

    /// Returns the names of all columns that have statistics enabled.
    pub fn stats_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|(_, col)| col.stats)
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Returns the names of all columns that use dictionary encoding.
    pub fn dictionary_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|(_, col)| col.dictionary)
            .map(|(name, _)| name.as_str())
            .collect()
    }
}
