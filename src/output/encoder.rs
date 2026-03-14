use crate::metadata::JsonEncoding;
use arrow::array::*;
use arrow::datatypes::DataType;
use serde::Serializer;

/// Function pointer type for pre-resolved encoders.
pub type EncoderFn = fn(&dyn Array, usize, &mut Vec<u8>);

/// Resolve an encoder function once per column based on DataType and encoding.
/// Eliminates per-row DataType match + downcast dispatch in the hot loop.
pub fn resolve_encoder(data_type: &DataType, encoding: Option<&JsonEncoding>) -> EncoderFn {
    match encoding {
        Some(JsonEncoding::String) => encode_bignum,
        Some(JsonEncoding::Json) => encode_json_passthrough,
        Some(JsonEncoding::SolanaTxVersion) => encode_solana_tx_version,
        Some(JsonEncoding::TimestampMillisecond) => encode_timestamp_millisecond_raw,
        // Disabled: ~11% faster but not 100% safe — malformed data could produce invalid JSON
        // Some(JsonEncoding::Hex) | Some(JsonEncoding::Base58) => match data_type {
        //     DataType::Utf8 => encode_utf8_safe,
        //     _ => resolve_value_encoder(data_type),
        // },
        Some(JsonEncoding::Hex) | Some(JsonEncoding::Base58) | None => {
            resolve_value_encoder(data_type)
        }
    }
}

fn resolve_value_encoder(data_type: &DataType) -> EncoderFn {
    match data_type {
        DataType::Boolean => encode_boolean,
        DataType::Int8 => encode_int8,
        DataType::UInt8 => encode_uint8,
        DataType::UInt16 => encode_uint16,
        DataType::UInt32 => encode_uint32,
        DataType::UInt64 => encode_uint64,
        DataType::Int16 => encode_int16,
        DataType::Int32 => encode_int32,
        DataType::Int64 => encode_int64,
        DataType::Float64 => encode_float64,
        DataType::Utf8 => encode_utf8_value,
        DataType::Binary => encode_binary_value,
        DataType::FixedSizeBinary(_) => encode_fixed_binary_value,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => encode_timestamp_second,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
            encode_timestamp_millisecond
        }
        DataType::List(_) => encode_list_value,
        DataType::Struct(_) => encode_struct_value,
        _ => encode_null_value,
    }
}

fn encode_null_value(_array: &dyn Array, _row: usize, buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"null");
}

fn encode_boolean(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    if a.value(row) {
        buf.extend_from_slice(b"true");
    } else {
        buf.extend_from_slice(b"false");
    }
}

fn encode_int8(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
    write_i64(buf, a.value(row) as i64);
}

fn encode_uint8(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
    write_u64(buf, a.value(row) as u64);
}

fn encode_uint16(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<UInt16Array>().unwrap();
    write_u64(buf, a.value(row) as u64);
}

fn encode_uint32(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    write_u64(buf, a.value(row) as u64);
}

fn encode_uint64(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    write_u64(buf, a.value(row));
}

fn encode_int16(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
    write_i64(buf, a.value(row) as i64);
}

fn encode_int32(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
    write_i64(buf, a.value(row) as i64);
}

fn encode_int64(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
    write_i64(buf, a.value(row));
}

fn encode_float64(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
    let v = a.value(row);
    if v.is_nan() || v.is_infinite() {
        buf.extend_from_slice(b"null");
    } else {
        let mut tmp = ryu::Buffer::new();
        buf.extend_from_slice(tmp.format(v).as_bytes());
    }
}

fn encode_utf8_value(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
    encode_json_string(a.value(row), buf);
}

/// Encode a Utf8 string without JSON escaping.
/// SAFETY: assumes hex/base58 values contain only safe ASCII chars.
/// This is not 100% safe — malformed data could produce invalid JSON.
fn encode_utf8_safe(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
    let s = a.value(row);
    buf.push(b'"');
    buf.extend_from_slice(s.as_bytes());
    buf.push(b'"');
}

fn encode_binary_value(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
    encode_hex_bytes(a.value(row), buf);
}

fn encode_fixed_binary_value(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    encode_hex_bytes(a.value(row), buf);
}

fn encode_timestamp_second(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap();
    write_i64(buf, a.value(row));
}

fn encode_timestamp_millisecond(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    // Convert milliseconds to seconds to match expected output
    write_i64(buf, a.value(row) / 1000);
}

fn encode_timestamp_millisecond_raw(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    write_i64(buf, a.value(row));
}

fn encode_list_value(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array
        .as_any()
        .downcast_ref::<GenericListArray<i32>>()
        .unwrap();
    encode_list(a, row, buf);
}

fn encode_struct_value(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<StructArray>().unwrap();
    encode_struct(a, row, buf);
}

/// Encode a single value from an Arrow array to JSON bytes (generic fallback).
/// Prefer `resolve_encoder` + direct call in hot loops.
pub fn encode_value(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    resolve_value_encoder(array.data_type())(array, row, buf);
}

/// Encode a value as a bignum (quoted string number).
pub fn encode_bignum(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    buf.push(b'"');
    match array.data_type() {
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            write_u64(buf, a.value(row));
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            write_i64(buf, a.value(row));
        }
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            write_u64(buf, a.value(row) as u64);
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            write_i64(buf, a.value(row) as i64);
        }
        DataType::Decimal128(_, scale) => {
            let a = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            let v = a.value(row);
            if *scale == 0 {
                write_i128(buf, v);
            } else {
                write_decimal128(buf, v, *scale);
            }
        }
        _ => {
            buf.pop(); // remove opening quote
            buf.extend_from_slice(b"null");
            return;
        }
    }
    buf.push(b'"');
}

/// Encode a string column that contains raw JSON — pass through without quoting.
pub fn encode_json_passthrough(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
    let s = a.value(row);
    if s.is_empty() {
        buf.extend_from_slice(b"null");
    } else {
        buf.extend_from_slice(s.as_bytes());
    }
}

/// Encode Solana transaction version: -1 → "legacy", else number.
pub fn encode_solana_tx_version(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
    let v = a.value(row);
    if v == -1 {
        buf.extend_from_slice(b"\"legacy\"");
    } else {
        write_i64(buf, v as i64);
    }
}

/// Encode a list of columns as a "rolled" JSON array.
/// Non-null values are added sequentially; stops at first null.
/// If the last column is a list, its elements are spread into the array.
pub fn encode_roll(batch: &RecordBatch, row: usize, column_indices: &[usize], buf: &mut Vec<u8>) {
    buf.push(b'[');
    let mut has_items = false;

    for (i, &col_idx) in column_indices.iter().enumerate() {
        let col = batch.column(col_idx);
        let is_last = i == column_indices.len() - 1;

        if col.is_null(row) {
            break;
        }

        if is_last && matches!(col.data_type(), DataType::List(_)) {
            let list = col
                .as_any()
                .downcast_ref::<GenericListArray<i32>>()
                .unwrap();
            let values = list.value(row);
            for j in 0..values.len() {
                if has_items {
                    buf.push(b',');
                }
                encode_value(values.as_ref(), j, buf);
                has_items = true;
            }
        } else {
            if has_items {
                buf.push(b',');
            }
            encode_value(col.as_ref(), row, buf);
            has_items = true;
        }
    }

    buf.push(b']');
}

/// Pre-resolved encoder for a Roll column. Resolves once per batch, reused per row.
pub struct ResolvedRollEncoder {
    /// Per-column: (column_index, encoder_fn, is_last_and_list)
    columns: Vec<(usize, EncoderFn, bool)>,
}

impl ResolvedRollEncoder {
    /// Resolve encoders for a Roll's column indices against a specific batch.
    pub fn resolve(batch: &RecordBatch, column_indices: &[usize]) -> Self {
        let columns = column_indices
            .iter()
            .enumerate()
            .map(|(i, &col_idx)| {
                let col = batch.column(col_idx);
                let is_last = i == column_indices.len() - 1;
                let is_list = matches!(col.data_type(), DataType::List(_));
                let encoder = resolve_value_encoder(col.data_type());
                (col_idx, encoder, is_last && is_list)
            })
            .collect();
        Self { columns }
    }

    /// Encode the roll for a given row using pre-resolved encoders.
    #[inline]
    pub fn encode(&self, batch: &RecordBatch, row: usize, buf: &mut Vec<u8>) {
        buf.push(b'[');
        let mut has_items = false;

        for &(col_idx, encoder, is_last_list) in &self.columns {
            let col = batch.column(col_idx);

            if col.is_null(row) {
                break;
            }

            if is_last_list {
                let list = col
                    .as_any()
                    .downcast_ref::<GenericListArray<i32>>()
                    .unwrap();
                let values = list.value(row);
                // List elements need per-element dispatch (heterogeneous types possible)
                let elem_encoder = resolve_value_encoder(values.data_type());
                for j in 0..values.len() {
                    if has_items {
                        buf.push(b',');
                    }
                    elem_encoder(values.as_ref(), j, buf);
                    has_items = true;
                }
            } else {
                if has_items {
                    buf.push(b',');
                }
                encoder(col.as_ref(), row, buf);
                has_items = true;
            }
        }

        buf.push(b']');
    }
}

/// Encode a JSON-escaped string with quotes.
/// Uses serde_json's Serializer directly (same as legacy engine).
#[inline]
pub fn encode_json_string(s: &str, buf: &mut Vec<u8>) {
    serde_json::Serializer::new(buf).serialize_str(s).unwrap();
}

fn encode_hex_bytes(bytes: &[u8], buf: &mut Vec<u8>) {
    buf.push(b'"');
    buf.extend_from_slice(b"0x");
    let hex_len = bytes.len() * 2;
    let start = buf.len();
    buf.resize(start + hex_len, 0);
    faster_hex::hex_encode(bytes, &mut buf[start..]).unwrap();
    buf.push(b'"');
}

fn encode_list(array: &GenericListArray<i32>, row: usize, buf: &mut Vec<u8>) {
    let values = array.value(row);
    buf.push(b'[');
    for i in 0..values.len() {
        if i > 0 {
            buf.push(b',');
        }
        encode_value(values.as_ref(), i, buf);
    }
    buf.push(b']');
}

fn encode_struct(array: &StructArray, row: usize, buf: &mut Vec<u8>) {
    buf.push(b'{');
    let fields = array.fields();
    for (i, (field, col)) in fields.iter().zip(array.columns().iter()).enumerate() {
        if i > 0 {
            buf.push(b',');
        }
        encode_json_string(&snake_to_camel(field.name()), buf);
        buf.push(b':');
        encode_value(col.as_ref(), row, buf);
    }
    buf.push(b'}');
}

fn write_u64(buf: &mut Vec<u8>, v: u64) {
    let mut tmp = itoa::Buffer::new();
    buf.extend_from_slice(tmp.format(v).as_bytes());
}

fn write_i64(buf: &mut Vec<u8>, v: i64) {
    let mut tmp = itoa::Buffer::new();
    buf.extend_from_slice(tmp.format(v).as_bytes());
}

fn write_i128(buf: &mut Vec<u8>, v: i128) {
    let mut tmp = itoa::Buffer::new();
    buf.extend_from_slice(tmp.format(v).as_bytes());
}

fn write_decimal128(buf: &mut Vec<u8>, v: i128, scale: i8) {
    if scale <= 0 {
        write_i128(buf, v);
        return;
    }
    let s = scale as u32;
    let divisor = 10i128.pow(s);
    let int_part = v / divisor;
    let frac_abs = (v % divisor).unsigned_abs();

    if v < 0 && int_part == 0 {
        buf.push(b'-');
    }

    let mut tmp = itoa::Buffer::new();
    buf.extend_from_slice(tmp.format(int_part).as_bytes());
    buf.push(b'.');

    let mut tmp2 = itoa::Buffer::new();
    let frac_str = tmp2.format(frac_abs);
    for _ in 0..(s as usize).saturating_sub(frac_str.len()) {
        buf.push(b'0');
    }
    buf.extend_from_slice(frac_str.as_bytes());
}

/// Convert snake_case to camelCase.
pub fn snake_to_camel(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut capitalize_next = false;
    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }
    result
}

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_encode_value_uint64() {
        let arr = UInt64Array::from(vec![42]);
        let mut buf = Vec::new();
        encode_value(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "42");
    }

    #[test]
    fn test_encode_value_string() {
        let arr = StringArray::from(vec!["hello \"world\""]);
        let mut buf = Vec::new();
        encode_value(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), r#""hello \"world\"""#);
    }

    #[test]
    fn test_encode_value_null() {
        let arr = UInt64Array::from(vec![None as Option<u64>]);
        let mut buf = Vec::new();
        encode_value(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "null");
    }

    #[test]
    fn test_encode_value_boolean() {
        let arr = BooleanArray::from(vec![true, false]);
        let mut buf = Vec::new();
        encode_value(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "true");
        let mut buf = Vec::new();
        encode_value(&arr, 1, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "false");
    }

    #[test]
    fn test_encode_bignum() {
        let arr = UInt64Array::from(vec![12345678901234567890u64]);
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"12345678901234567890\"");
    }

    #[test]
    fn test_encode_json_passthrough() {
        let arr = StringArray::from(vec![Some(r#"{"key":"value"}"#), None]);
        let mut buf = Vec::new();
        encode_json_passthrough(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), r#"{"key":"value"}"#);
        buf = Vec::new();
        encode_json_passthrough(&arr, 1, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "null");
    }

    #[test]
    fn test_encode_solana_tx_version() {
        let arr = Int16Array::from(vec![Some(-1), Some(0), Some(1), None]);
        let mut buf = Vec::new();
        encode_solana_tx_version(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"legacy\"");
        buf = Vec::new();
        encode_solana_tx_version(&arr, 1, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "0");
        buf = Vec::new();
        encode_solana_tx_version(&arr, 3, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "null");
    }

    #[test]
    fn test_encode_roll() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a0", DataType::Utf8, true),
            Field::new("a1", DataType::Utf8, true),
            Field::new("a2", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
                Arc::new(StringArray::from(vec![Some("charlie"), None])),
                Arc::new(StringArray::from(vec![Some("dave"), None])),
            ],
        )
        .unwrap();

        // Row 0: all present
        let mut buf = Vec::new();
        encode_roll(&batch, 0, &[0, 1, 2], &mut buf);
        assert_eq!(
            String::from_utf8(buf).unwrap(),
            r#"["alice","charlie","dave"]"#
        );

        // Row 1: stops at first null (a1)
        let mut buf = Vec::new();
        encode_roll(&batch, 1, &[0, 1, 2], &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), r#"["bob"]"#);
    }

    #[test]
    fn test_encode_roll_with_list_spread() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a0", DataType::Utf8, false),
            Field::new(
                "rest",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
        ]));

        let mut list_builder = ListBuilder::new(StringBuilder::new()).with_field(Field::new(
            "item",
            DataType::Utf8,
            true,
        ));
        list_builder.values().append_value("b1");
        list_builder.values().append_value("b2");
        list_builder.append(true);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a0_val"])),
                Arc::new(list_builder.finish()),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        encode_roll(&batch, 0, &[0, 1], &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), r#"["a0_val","b1","b2"]"#);
    }

    #[test]
    fn test_snake_to_camel() {
        assert_eq!(snake_to_camel("transaction_index"), "transactionIndex");
        assert_eq!(snake_to_camel("block_number"), "blockNumber");
        assert_eq!(snake_to_camel("number"), "number");
        assert_eq!(snake_to_camel("log_index"), "logIndex");
        assert_eq!(snake_to_camel("instruction_address"), "instructionAddress");
        assert_eq!(snake_to_camel("fee_payer"), "feePayer");
        assert_eq!(snake_to_camel("a0"), "a0");
    }

    #[test]
    fn test_encode_list_uint32() {
        let mut builder = ListBuilder::new(UInt32Builder::new()).with_field(Field::new(
            "item",
            DataType::UInt32,
            true,
        ));
        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        let arr = builder.finish();

        let mut buf = Vec::new();
        encode_value(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "[0,1,2]");
    }

    #[test]
    fn test_decimal128_scale_zero() {
        let arr = Decimal128Array::from(vec![12345i128]).with_precision_and_scale(38, 0).unwrap();
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"12345\"");
    }

    #[test]
    fn test_decimal128_scale_nonzero() {
        let arr = Decimal128Array::from(vec![12345i128]).with_precision_and_scale(38, 2).unwrap();
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"123.45\"");
    }

    #[test]
    fn test_decimal128_scale_negative_value() {
        let arr = Decimal128Array::from(vec![-12345i128]).with_precision_and_scale(38, 3).unwrap();
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"-12.345\"");
    }

    #[test]
    fn test_decimal128_scale_small_value() {
        // 5 with scale 3 → "0.005"
        let arr = Decimal128Array::from(vec![5i128]).with_precision_and_scale(38, 3).unwrap();
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"0.005\"");
    }

    #[test]
    fn test_decimal128_negative_small_value() {
        // -5 with scale 3 → "-0.005"
        let arr = Decimal128Array::from(vec![-5i128]).with_precision_and_scale(38, 3).unwrap();
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"-0.005\"");
    }
}
