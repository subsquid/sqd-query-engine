//! The filters that take hex, and the ways a client writes a hex value wrong.
//!
//! [INV-Q12] answers a malformed value rather than refusing it, so a test of the
//! rule reaches only as far as the filters it tries. Reading them off the catalog
//! keeps a hex filter added later inside every such test.
//!
//! [INV-Q12]: ../../../spec/07-invariants.md#inv-q12

use sqd_query_engine::metadata::{ColumnType, DatasetDescription, JsonEncoding, SpecialFilter};
use sqd_query_engine::output::snake_to_camel;

/// One filter whose values are hex, under the request key a client sends it in.
pub struct HexFilter {
    /// A table's request name, or an alias over it.
    pub request: String,
    /// The filter key, camelCased the way a client writes it.
    pub key: String,
    /// The catalog's name for the table it reads.
    pub table: String,
    pub column: String,
    /// The digit count of a `hexNumber` column's values, which are zero-padded
    /// to the declared width. `None` for `hexBytes`, which has no fixed width.
    pub digits: Option<usize>,
}

/// Every filter of `catalog` that reads its values as hex: a column declared
/// `hexBytes` or `hexNumber`, by its own name or through a column alias, on a
/// table or on an alias over one. The discriminator dispatch is not a column
/// filter and is left out.
pub fn hex_filters(catalog: &DatasetDescription) -> Vec<HexFilter> {
    let mut found = Vec::new();

    for (name, table) in &catalog.tables {
        let surface = table.request();
        let request = table.request_name(name);
        for key in &surface.filters {
            let column = match surface.special_filters.get(key) {
                None => key,
                Some(SpecialFilter::ColumnAlias { column }) => column,
                Some(_) => continue,
            };
            found.extend(hex_filter(catalog, request, key, name, column));
        }
    }

    for (request, alias) in &catalog.aliases {
        for key in &alias.filters {
            let column = match alias.special_filters.get(key) {
                None => key,
                Some(SpecialFilter::ColumnAlias { column }) => column,
                Some(_) => continue,
            };
            found.extend(hex_filter(catalog, request, key, &alias.table, column));
        }
    }

    found
}

fn hex_filter(
    catalog: &DatasetDescription,
    request: &str,
    key: &str,
    table: &str,
    column: &str,
) -> Option<HexFilter> {
    let description = catalog.table(table)?.column(column)?;

    let digits = match (&description.encoding, &description.data_type) {
        (Some(JsonEncoding::HexBytes), _) => None,
        (Some(JsonEncoding::HexNumber), ColumnType::UInt8) => Some(2),
        (Some(JsonEncoding::HexNumber), ColumnType::UInt16) => Some(4),
        (Some(JsonEncoding::HexNumber), ColumnType::UInt32) => Some(8),
        (Some(JsonEncoding::HexNumber), ColumnType::UInt64) => Some(16),
        _ => return None,
    };

    Some(HexFilter {
        request: request.to_string(),
        key: snake_to_camel(key),
        table: table.to_string(),
        column: column.to_string(),
        digits,
    })
}

/// Each way a hex value is malformed, every one a single edit away from the
/// well-formed `value`, so the value it was meant to be is one the column holds.
///
/// The non-ASCII digit and the sign keep the byte count even: those two get past
/// a length check, and only a digit check stops them.
pub fn malformed(value: &str) -> Vec<(&'static str, String)> {
    let digits = value
        .strip_prefix("0x")
        .expect("a malformed value is made from a well-formed one");
    assert!(
        digits.len() >= 2 && digits.len().is_multiple_of(2),
        "{value} is not well-formed"
    );

    let short = &value[..value.len() - 1];
    let head = &value[..value.len() - 2];

    vec![
        ("no 0x prefix", digits.to_string()),
        ("a digit short", short.to_string()),
        ("a digit over", format!("{value}0")),
        ("a letter past f", format!("{short}g")),
        ("a non-ASCII digit", format!("{head}é")),
        ("a sign", format!("0x+{}", &digits[1..])),
        ("a trailing space", format!("{value} ")),
        ("an empty string", String::new()),
    ]
}
