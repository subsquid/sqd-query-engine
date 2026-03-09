use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level dataset description. One per chain type (evm, solana, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetDescription {
    /// Dataset name (e.g., "solana", "evm")
    pub name: String,
    /// Table definitions keyed by table name.
    /// Uses IndexMap to preserve YAML insertion order (determines output table ordering in blocks).
    pub tables: indexmap::IndexMap<String, TableDescription>,
}

/// Description of a single parquet table within a dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDescription {
    /// Column that holds the block number for block range filtering.
    /// Defaults to "block_number". The blocks table typically overrides this to "number".
    #[serde(default = "default_block_number_column")]
    pub block_number_column: String,

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
    Int64,
    Float64,
    Boolean,
    String,
    TimestampSecond,
    ListUInt8,
    ListUInt32,
    ListString,
    ListStruct,
    Struct,
    /// Fixed-size binary with given byte length.
    FixedBinary(usize),
}

impl Serialize for ColumnType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let s = match self {
            ColumnType::UInt8 => "uint8".to_string(),
            ColumnType::UInt16 => "uint16".to_string(),
            ColumnType::UInt32 => "uint32".to_string(),
            ColumnType::UInt64 => "uint64".to_string(),
            ColumnType::Int16 => "int16".to_string(),
            ColumnType::Int64 => "int64".to_string(),
            ColumnType::Float64 => "float64".to_string(),
            ColumnType::Boolean => "boolean".to_string(),
            ColumnType::String => "string".to_string(),
            ColumnType::TimestampSecond => "timestamp_second".to_string(),
            ColumnType::ListUInt8 => "list_uint8".to_string(),
            ColumnType::ListUInt32 => "list_uint32".to_string(),
            ColumnType::ListString => "list_string".to_string(),
            ColumnType::ListStruct => "list_struct".to_string(),
            ColumnType::Struct => "struct".to_string(),
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
            "int64" => Ok(ColumnType::Int64),
            "float64" => Ok(ColumnType::Float64),
            "boolean" => Ok(ColumnType::Boolean),
            "string" => Ok(ColumnType::String),
            "timestamp_second" => Ok(ColumnType::TimestampSecond),
            "list_uint8" => Ok(ColumnType::ListUInt8),
            "list_uint32" => Ok(ColumnType::ListUInt32),
            "list_string" => Ok(ColumnType::ListString),
            "list_struct" => Ok(ColumnType::ListStruct),
            "struct" => Ok(ColumnType::Struct),
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
    /// Solana transaction version: -1 → "legacy", else number
    SolanaTxVersion,
}

/// Description of a relation available in query items.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct FieldGrouping {
    /// Column that determines the variant (e.g., "type").
    pub tag_column: String,
    /// Columns that appear at the top level for all variants (e.g., transaction_index, trace_address).
    #[serde(default)]
    pub base_fields: Vec<String>,
    /// Per-variant definitions mapping tag value → groups.
    pub variants: BTreeMap<String, BTreeMap<String, Vec<FieldMapping>>>,
}

/// Maps a physical column to a JSON field name within a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    /// Physical column name in parquet.
    pub column: String,
    /// JSON field name in output (camelCase).
    pub field: String,
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
