use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::OnceLock;

/// Top-level dataset description. One per chain type (evm, solana, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetDescription {
    /// Dataset name (e.g., "solana", "evm")
    pub name: String,
    /// Table definitions keyed by table name.
    /// Uses IndexMap to preserve YAML insertion order (determines output table ordering in blocks).
    pub tables: indexmap::IndexMap<String, TableDescription>,
    /// Further request surfaces over the tables above, each a narrower view of
    /// one table under a name of its own (`evmLogs` over substrate `events`).
    #[serde(default)]
    pub aliases: BTreeMap<String, Alias>,
}

/// A request surface over an existing table: the same shape as a table's own
/// [`RequestSurface`], plus the filters that make it a narrower view.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Alias {
    /// The table the alias reads.
    pub table: String,
    /// Filters always applied, which the client cannot see or override
    /// (column → allowed values). `name: ["EVM.Log"]` is what makes `evmLogs`
    /// a view of `events` rather than a second name for it.
    #[serde(default)]
    pub implicit_filters: BTreeMap<String, Vec<String>>,
    /// Filters this alias accepts, in place of the table's own list. An alias
    /// is a narrower view of its table and usually exposes fewer filters.
    ///
    /// Required, with no default: an alias that omits the key would silently
    /// accept no filters at all, and every client filter on it would 400.
    /// Declaring `filters: []` says that on purpose.
    pub filters: Vec<String>,
    /// Filters of the alias's own, under names the table does not have. Only
    /// `column_alias` is admitted here (the loader checks): an item request
    /// carries the resolved column, and the plan looks every other kind up on
    /// the table.
    #[serde(default)]
    pub special_filters: BTreeMap<String, SpecialFilter>,
    /// Relations available through this alias, in place of the table's own.
    #[serde(default)]
    pub relations: BTreeMap<String, RelationDef>,
}

/// Description of a single parquet table within a dataset.
///
/// Three things are described, and each has a block of its own: what a client
/// may send for the table (`request`), what it may ask to see (`output`), and
/// what the parquet actually holds (the rest).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableDescription {
    /// What an item request on this table may say. Absent only for the block
    /// table, which has no filters and no relations and is still addressed by
    /// its own name; the loader requires it of every other table, for the reason
    /// [`RequestSurface::filters`] is itself required.
    ///
    /// Read it through [`TableDescription::request`], which supplies the empty
    /// surface. The field is `Option` so that absence stays visible to the
    /// loader: a defaulted `RequestSurface` is indistinguishable from one
    /// written out with empty lists, and that is the difference the check turns
    /// on.
    #[serde(default, rename = "request")]
    pub request_surface: Option<RequestSurface>,

    /// What a client may ask to see of this table's rows.
    #[serde(default)]
    pub output: OutputSurface,

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
}

/// The request side of a table: how a client addresses it and what an item
/// request on it may contain.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestSurface {
    /// The key of this table's item requests in a query (`logs: [ ... ]`), and
    /// of its array in every response block. Defaults to the table's own name;
    /// [`TableDescription::request_name`] applies the default.
    #[serde(default)]
    pub name: Option<String>,

    /// Filters this table accepts: columns of the same name, and the
    /// `special_filters` declared below. Declaring them is what keeps the filter
    /// surface closed: without a list, every column — blooms, size counters,
    /// denormalised extractions — would be filterable, and the column list would
    /// become the public API.
    ///
    /// Required, with no default, once a `request` block is present, for the
    /// reason [`Alias::filters`] is: an omitted key would accept no filters at
    /// all and 400 every client filter on the table, and `deny_unknown_fields`
    /// does not catch an absent key the way it catches a misspelled one.
    /// `filters: []` says it on purpose.
    pub filters: Vec<String>,

    /// Filters that are not a column of the same name: a dispatch, a bloom
    /// probe, a bound, a rename. Reachable only when listed in `filters`.
    #[serde(default)]
    pub special_filters: BTreeMap<String, SpecialFilter>,

    /// Relations an item request may switch on.
    #[serde(default)]
    pub relations: BTreeMap<String, RelationDef>,
}

/// The output side of a table: how a client selects its fields and what each
/// field renders as.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputSurface {
    /// The key of this table's selection under a query's `fields`
    /// (`fields: { log: { ... } }`). No default: a table that declares none is
    /// not selectable, and a selection naming it is refused.
    #[serde(default)]
    pub name: Option<String>,

    /// Fields a client may select, each naming a non-system column, a virtual
    /// field, or a variant mapping's field key.
    ///
    /// Declaring them is what keeps the output surface closed, for the reason
    /// [`RequestSurface::filters`] closes the input one: derived from the column
    /// list instead, every column the catalog carries for filtering, grouping,
    /// joining or rolling becomes a field a client may pin on, and the physical
    /// layout becomes the wire contract (INV-Q14).
    ///
    /// Empty is "nothing is selectable", which is only meaningful for a table no
    /// selection can name. The loader requires a list from every table that
    /// declares a `name`.
    #[serde(default)]
    pub fields: Vec<String>,

    /// Fields assembled from several columns.
    /// E.g., "accounts" → roll(a0..a15, rest_accounts), "topics" → roll(topic0..topic3)
    #[serde(default)]
    pub virtual_fields: BTreeMap<String, VirtualField>,

    /// For a table whose rows come in several shapes, the column whose value
    /// says which shape a row has (EVM traces: `type`). Each `variants` entry
    /// is keyed by one of its values.
    #[serde(default)]
    pub variant_column: Option<String>,

    /// Per-variant nesting of output fields: variant → JSON group → the fields
    /// the group holds. A group named `_` is written flat. Fields no variant
    /// claims are written flat for every row.
    ///
    /// Claiming is therefore all-or-nothing per field: a field one variant maps
    /// leaves the top level for *every* row, and rows of the variants that do
    /// not map it lose the field entirely. That is what a per-variant field
    /// wants (`create_init` has no meaning on a `call` row) and what a shared
    /// one does not, so the loader refuses a mapping over the columns that
    /// identify a row — the variant column, the item order keys, the address
    /// column and the block number.
    ///
    /// EVM traces: `create` → `action.{from,value,gas,init}`, `result.{...}`;
    /// `call` → `action.{from,to,...}`, `result.{gasUsed,output}`; and so on.
    #[serde(default)]
    pub variants: BTreeMap<String, BTreeMap<String, Vec<FieldMapping>>>,
}

impl TableDescription {
    /// What an item request on this table may say. A table without the block —
    /// only the block table — takes the empty surface: no filters, no relations,
    /// and its own name.
    pub fn request(&self) -> &RequestSurface {
        static EMPTY: OnceLock<RequestSurface> = OnceLock::new();

        match &self.request_surface {
            Some(request) => request,
            None => EMPTY.get_or_init(RequestSurface::default),
        }
    }

    /// The name a request addresses this table by, `key` being the table's
    /// own name in the catalog.
    pub fn request_name<'a>(&'a self, key: &'a str) -> &'a str {
        self.request().name.as_deref().unwrap_or(key)
    }

    /// The physical parquet column an output key reads, when the catalog
    /// declares one: an ordinary column, or a variant mapping's `field_key` that
    /// renames it (`call_call_type` → `call_type`).
    ///
    /// A virtual field resolves to nothing — it rolls several columns, and the
    /// caller expands it. Everything that projects, weighs or requires an output
    /// column goes through here, so the three cannot disagree on what a key
    /// means.
    ///
    /// The column list is consulted first and the mappings second, which is the
    /// opposite of the order the row writer resolves them in. The loader keeps
    /// the two from ever disagreeing: a `field_key` that differs from its own
    /// column may not name a column at all.
    pub fn physical_output_column(&self, key: &str) -> Option<&str> {
        if let Some((name, _)) = self.columns.get_key_value(key) {
            return Some(name.as_str());
        }

        self.variant_source(key)
    }

    /// The physical column a variant mapping reads for `field`, if any mapping
    /// is selected by that name.
    ///
    /// Takes the first of several mappings that answer to the name; the loader
    /// requires them all to read the same column, so which one it is does not
    /// matter.
    pub fn variant_source(&self, field: &str) -> Option<&str> {
        self.output
            .variants
            .values()
            .flat_map(|groups| groups.values())
            .flatten()
            .find(|mapping| mapping.field() == field)
            .map(|mapping| mapping.column.as_str())
    }

    /// Whether a `fields` key names something this table can emit. The declared
    /// list is the answer, not the column list it is drawn from: a column exists
    /// for whatever the engine needs it for, and only the catalog says which of
    /// them a client may ask for (INV-Q14).
    pub fn is_selectable_field(&self, name: &str) -> bool {
        self.output.fields.iter().any(|f| f == name)
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

    /// How the value renders in a response, when not as the type's natural
    /// JSON. E.g. `hex_bytes` for `0x…` strings, `decimal_string` for an
    /// integer quoted so it survives a JavaScript client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<JsonEncoding>,

    /// Weight source for response size limiting.
    /// References a size column (e.g., "input_size") or a fixed weight (e.g., 0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<WeightSource>,

    /// System column — not included in user output (e.g., size columns, bloom filters).
    #[serde(default)]
    pub system: bool,

    /// Compare filter values case-insensitively (INV-P8).
    ///
    /// A `hex_bytes` column folds already — its values are `0x…` lowercase by
    /// §1.5. This is for a column that holds hex *without* the prefix, which
    /// Tron's addresses and topics do: they render verbatim, so the encoding
    /// cannot say it, and a client sending an upper-case address would
    /// otherwise get an empty response rather than its rows.
    #[serde(default)]
    pub fold_case: bool,
}

impl ColumnDescription {
    /// Whether filter values on this column compare case-insensitively.
    pub fn folds_case(&self) -> bool {
        self.fold_case || self.encoding == Some(JsonEncoding::HexBytes)
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

/// How a column's value renders in a JSON response (spec §1.5). The names are
/// the spec's, in snake case.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JsonEncoding {
    /// Hex-encoded string with "0x" prefix (e.g., addresses, hashes)
    HexBytes,
    /// Base58-encoded string (Solana addresses)
    Base58,
    /// Integer as a quoted decimal string, for values beyond the range a
    /// JavaScript number holds exactly.
    DecimalString,
    /// The stored bytes spliced in as they are: the column already holds JSON.
    JsonVerbatim,
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

/// A filter that is not a column of the same name.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpecialFilter {
    /// Dispatch a hex prefix to a column by its byte length.
    Discriminator {
        /// Byte length, in decimal → column holding prefixes of that length
        /// (e.g., "1" → "d1").
        by_length: BTreeMap<String, String>,
    },
    /// Probabilistic membership test against a bloom column.
    Bloom {
        column: String,
        /// The bloom's size in bytes. The probe reads the width off the stored
        /// array, so this is a statement about the archive writer rather than an
        /// input: the loader requires it to equal the column's own width, which
        /// is what makes it worth writing down.
        bytes: usize,
        /// How many hash functions the archive writer set per value.
        hashes: usize,
    },
    /// Range filter: column >= value.
    RangeGte { column: String },
    /// Range filter: column <= value.
    RangeLte { column: String },
    /// The filter key reads a column of another name.
    ColumnAlias { column: String },
    /// Boolean flag filter: when `true`, emits `column >= value` against a fixed
    /// metadata-defined constant. Used for EVM trace `*NonZero` filters, e.g.
    /// `callValueNonZero: true` → `call_value >= "0x1"` (minimal-form hex, so this
    /// keeps every non-zero value and drops the zero representation "0x").
    GteConst { column: String, value: String },
}

/// One field of a variant group: the column it reads, the name a selection
/// picks it by, and the name it renders under.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldMapping {
    /// Physical column name in parquet.
    pub column: String,
    /// The `output.fields` key that selects this mapping, when it differs from
    /// `column`. Lets one physical column back several output fields — EVM
    /// `call_type` renders as both `action.type` (field key `call_type`) and
    /// `action.callType` (field key `call_call_type`).
    ///
    /// Not spelled `field`: that key once meant the *rendered* name, which is
    /// now `as`, and a catalog carrying the old spelling would otherwise load
    /// clean and move a field to another place in the response.
    #[serde(default)]
    pub field_key: Option<String>,
    /// JSON key inside the group, written to the wire exactly as it stands.
    #[serde(rename = "as")]
    pub json_name: String,
}

impl FieldMapping {
    /// The `output.fields` key that selects this mapping (defaults to `column`).
    pub fn field(&self) -> &str {
        self.field_key.as_deref().unwrap_or(&self.column)
    }
}

impl SpecialFilter {
    /// The keys a catalog may write under each `kind`, the tag itself included.
    ///
    /// Serde cannot apply `deny_unknown_fields` to an internally tagged enum: a
    /// key it does not know is buffered and dropped, so a filter still carrying
    /// a spelling from an older catalog would load and quietly do nothing. Every
    /// other shape in a catalog refuses a stray key, and [`check_stale_keys`]
    /// gives these two the same answer by reading the list below.
    ///
    /// [`check_stale_keys`]: crate::metadata::loader
    pub fn allowed_keys(kind: &str) -> Option<&'static [&'static str]> {
        Some(match kind {
            "discriminator" => &["kind", "by_length"],
            "bloom" => &["kind", "column", "bytes", "hashes"],
            "range_gte" | "range_lte" | "column_alias" => &["kind", "column"],
            "gte_const" => &["kind", "column", "value"],
            _ => return None,
        })
    }
}

/// A virtual field that combines multiple physical columns into one output value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VirtualField {
    /// Roll multiple columns into a single JSON array.
    /// Non-nullable columns come first, then nullable (stops at first null),
    /// then an optional trailing list column (spread into array).
    Roll { columns: Vec<String> },
}

impl VirtualField {
    /// The keys a catalog may write under each `kind`, for the reason
    /// [`SpecialFilter::allowed_keys`] exists.
    pub fn allowed_keys(kind: &str) -> Option<&'static [&'static str]> {
        Some(match kind {
            "roll" => &["kind", "columns"],
            _ => return None,
        })
    }
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
}
