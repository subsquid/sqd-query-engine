use crate::metadata::{ColumnType, JsonEncoding};
use arrow::array::*;
use arrow::datatypes::DataType;
use serde::Serializer;

/// Function pointer type for pre-resolved encoders.
pub type EncoderFn = fn(&dyn Array, usize, &mut Vec<u8>);

/// Resolve an encoder function once per column based on DataType and encoding.
/// Eliminates per-row DataType match + downcast dispatch in the hot loop.
///
/// An encoding names the *declared* type; the chunk decides the physical one,
/// and the two drift — an archive outlives the catalog that described it. Where
/// an encoder's array type is not the one in front of it, the column falls back
/// to its physical encoder here, once, rather than downcasting per row and
/// taking the thread down mid-response (INV-E1).
pub fn resolve_encoder(
    data_type: &DataType,
    encoding: Option<&JsonEncoding>,
    declared_type: Option<&ColumnType>,
) -> EncoderFn {
    let declared: Option<EncoderFn> = match encoding {
        // Reads its own type and emits null for anything else.
        Some(JsonEncoding::DecimalString) => Some(encode_bignum),
        Some(JsonEncoding::HexNumber) => Some(resolve_hex_number_encoder(data_type, declared_type)),
        Some(JsonEncoding::JsonVerbatim) => {
            matches!(data_type, DataType::Utf8).then_some(encode_json_passthrough as EncoderFn)
        }
        Some(JsonEncoding::SolanaTxVersion) => {
            is_integer(data_type).then_some(encode_solana_tx_version as EncoderFn)
        }
        Some(JsonEncoding::TimestampMillisecond) => matches!(
            data_type,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _)
        )
        .then_some(encode_timestamp_millisecond_raw as EncoderFn),
        Some(JsonEncoding::HexBytes) => resolve_hex_bytes_encoder(data_type),
        Some(JsonEncoding::Base58) => resolve_base58_encoder(data_type),
        None => None,
    };

    declared.unwrap_or_else(|| resolve_declared_encoder(data_type, declared_type))
}

/// `hexBytes` — `0x` and lowercase hex, whatever the column is stored as. A text
/// column already holds the display string; bytes are rendered into one.
fn resolve_hex_bytes_encoder(data_type: &DataType) -> Option<EncoderFn> {
    match data_type {
        DataType::Utf8 => Some(encode_utf8_value),
        DataType::Binary => Some(encode_binary_value),
        DataType::FixedSizeBinary(_) => Some(encode_fixed_binary_value),
        _ => None,
    }
}

/// `base58`, same idea: the encoding names what the value *is*, so a column
/// stored as bytes is rendered rather than handed to the physical encoder, which
/// would emit `0x…` hex for an address every client reads as base58.
fn resolve_base58_encoder(data_type: &DataType) -> Option<EncoderFn> {
    match data_type {
        DataType::Utf8 => Some(encode_utf8_value),
        DataType::Binary => Some(encode_base58_binary),
        DataType::FixedSizeBinary(_) => Some(encode_base58_fixed_binary),
        _ => None,
    }
}

/// The physical encoder, except that a timestamp's unit is the *declared* one
/// (INV-O9). Storage picks a resolution per chunk; the catalog says what the
/// number means, and only the pair of them decides what to emit.
fn resolve_declared_encoder(data_type: &DataType, declared_type: Option<&ColumnType>) -> EncoderFn {
    use arrow::datatypes::TimeUnit;

    match (data_type, declared_type) {
        (DataType::Timestamp(TimeUnit::Millisecond, _), Some(ColumnType::TimestampMillisecond)) => {
            encode_timestamp_millisecond_raw
        }
        (DataType::Timestamp(TimeUnit::Second, _), Some(ColumnType::TimestampMillisecond)) => {
            encode_timestamp_second_as_millisecond
        }
        (DataType::Timestamp(TimeUnit::Millisecond, _), Some(ColumnType::TimestampSecond)) => {
            encode_timestamp_millisecond
        }
        (DataType::Timestamp(TimeUnit::Second, _), Some(ColumnType::TimestampSecond)) => {
            encode_timestamp_second
        }
        _ => resolve_value_encoder(data_type),
    }
}

/// A declared integer type bounds the values, not the storage: the writer picks
/// a width per chunk, so the same logical column arrives as `int16` in one and
/// `int32` in the next. An encoding that reads an integer has to accept every
/// width, or the same row renders two ways (INV-D7).
fn is_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
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

fn encode_base58_binary(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
    encode_base58_bytes(a.value(row), buf);
}

fn encode_base58_fixed_binary(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    encode_base58_bytes(a.value(row), buf);
}

/// The Bitcoin alphabet, which is the one Solana uses. `0`, `O`, `I` and `l` are
/// absent so no two characters look alike.
const BASE58_ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// Base58 of a byte string, quoted.
///
/// Leading zero bytes carry no value through the base conversion, so they are
/// emitted as `1`s first — that is what makes the encoding injective, and what
/// keeps a 32-byte key that starts with a zero byte 32 bytes long on the way
/// back.
fn encode_base58_bytes(bytes: &[u8], buf: &mut Vec<u8>) {
    buf.push(b'"');

    let zeros = bytes.iter().take_while(|b| **b == 0).count();
    buf.resize(buf.len() + zeros, b'1');

    // log(256)/log(58) ≈ 1.366, rounded up: the digit count can never exceed it.
    let mut digits: Vec<u8> = Vec::with_capacity(bytes.len() * 137 / 100 + 1);

    for &byte in &bytes[zeros..] {
        let mut carry = byte as u32;
        for digit in digits.iter_mut() {
            carry += (*digit as u32) << 8;
            *digit = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }

    buf.extend(digits.iter().rev().map(|d| BASE58_ALPHABET[*d as usize]));
    buf.push(b'"');
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

/// A column declared in milliseconds that a chunk stores in seconds. The unit is
/// the catalog's, so the second is scaled up rather than emitted as if it were a
/// millisecond — a value a client would read as 1970.
fn encode_timestamp_second_as_millisecond(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }
    let a = array
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap();
    write_i64(buf, a.value(row).saturating_mul(1000));
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
///
/// Every integer width is read, not just the wide ones: a `uint64` fee whose
/// values all fit in sixteen bits is stored in sixteen bits, and falling through
/// to the null arm would drop it from the response (INV-D7).
pub fn encode_bignum(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }

    // Signedness is the array's own: an unsigned column above `i64::MAX` read as
    // signed would come back negative.
    macro_rules! widened {
        ($array:ty, $write:ident, $wide:ty) => {{
            let a = array.as_any().downcast_ref::<$array>().unwrap();
            $write(buf, a.value(row) as $wide);
        }};
    }

    buf.push(b'"');
    match array.data_type() {
        DataType::UInt8 => widened!(UInt8Array, write_u64, u64),
        DataType::UInt16 => widened!(UInt16Array, write_u64, u64),
        DataType::UInt32 => widened!(UInt32Array, write_u64, u64),
        DataType::UInt64 => widened!(UInt64Array, write_u64, u64),
        DataType::Int8 => widened!(Int8Array, write_i64, i64),
        DataType::Int16 => widened!(Int16Array, write_i64, i64),
        DataType::Int32 => widened!(Int32Array, write_i64, i64),
        DataType::Int64 => widened!(Int64Array, write_i64, i64),
        DataType::Decimal128(_, scale) => {
            let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
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
///
/// The sentinel is a value of the *declared* type, so it is read at the declared
/// signedness whatever width the chunk used. Reading the array's own type instead
/// turns a legacy transaction stored in an `int32` into a bare `-1`, which is a
/// version number no client has ever seen.
pub fn encode_solana_tx_version(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
    if array.is_null(row) {
        buf.extend_from_slice(b"null");
        return;
    }

    match signed_at(array, row) {
        Some(-1) => buf.extend_from_slice(b"\"legacy\""),
        Some(v) => write_i64(buf, v),
        None => buf.extend_from_slice(b"null"),
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
    buf.reserve(hex_len);
    let start = buf.len();
    // Encode directly into the reserved spare capacity, skipping the redundant
    // zero-fill that `resize(.., 0)` would do before `hex_encode` overwrites every
    // byte. SAFETY: `reserve(hex_len)` guarantees `[start, start+hex_len)` is
    // allocated; `faster_hex::hex_encode` writes exactly `bytes.len()*2 == hex_len`
    // bytes (it errors otherwise, before `set_len` runs), initializing the whole
    // range before it is exposed via `set_len`.
    unsafe {
        let dst = std::slice::from_raw_parts_mut(buf.as_mut_ptr().add(start), hex_len);
        faster_hex::hex_encode(bytes, dst).unwrap();
        buf.set_len(start + hex_len);
    }
    buf.push(b'"');
}

/// `hexNumber` renders an unsigned integer as a quoted, zero-padded hex string
/// of the column's physical width. The width comes from the array rather than
/// the catalog so that a `uint16` always renders four digits, whatever value it
/// holds.
/// The padding width is the *declared* one (§1.5): `"0x0640"` and `"0x640"` are
/// different discriminators, so the digits may not depend on how the writer
/// happened to type the column, and physical and declared types disagree
/// routinely in these chunks. The physical width is the fallback for a column the
/// catalog says nothing about.
fn resolve_hex_number_encoder(
    data_type: &DataType,
    declared_type: Option<&ColumnType>,
) -> EncoderFn {
    let physical_bytes = match data_type {
        DataType::UInt8 | DataType::Int8 => 1,
        DataType::UInt16 | DataType::Int16 => 2,
        DataType::UInt32 | DataType::Int32 => 4,
        DataType::UInt64 | DataType::Int64 => 8,
        // The declaration is checked at load (`metadata::loader::validate`), so
        // reaching here means the chunk stores something other than an integer
        // under a column the catalog calls one.
        other => return resolve_value_encoder(other),
    };

    match declared_type {
        Some(ColumnType::UInt8) => encode_hex_number_u8,
        Some(ColumnType::UInt16) => encode_hex_number_u16,
        Some(ColumnType::UInt32) => encode_hex_number_u32,
        Some(ColumnType::UInt64) => encode_hex_number_u64,
        _ => match physical_bytes {
            1 => encode_hex_number_u8,
            2 => encode_hex_number_u16,
            4 => encode_hex_number_u32,
            _ => encode_hex_number_u64,
        },
    }
}

/// The stored value of an integer column, whatever width and signedness the
/// writer chose. A signed array is read through its unsigned twin: physical and
/// declared types disagree routinely, and `d8` rendered as a signed number would
/// be a different discriminator.
fn unsigned_at(array: &dyn Array, row: usize) -> Option<u64> {
    macro_rules! read {
        ($array:ty, $unsigned:ty) => {
            if let Some(a) = array.as_any().downcast_ref::<$array>() {
                return Some((a.value(row) as $unsigned) as u64);
            }
        };
    }

    read!(UInt8Array, u8);
    read!(UInt16Array, u16);
    read!(UInt32Array, u32);
    read!(UInt64Array, u64);
    read!(Int8Array, u8);
    read!(Int16Array, u16);
    read!(Int32Array, u32);
    read!(Int64Array, u64);

    None
}

/// The stored value of an integer column read at the *declared* signedness,
/// whatever width the writer chose. A signed value and its unsigned twin carry
/// the same bits, so a `-1` written into a `uint16` still reads back as `-1`.
fn signed_at(array: &dyn Array, row: usize) -> Option<i64> {
    macro_rules! read {
        ($array:ty, $signed:ty) => {
            if let Some(a) = array.as_any().downcast_ref::<$array>() {
                return Some((a.value(row) as $signed) as i64);
            }
        };
    }

    read!(Int8Array, i8);
    read!(Int16Array, i16);
    read!(Int32Array, i32);
    read!(Int64Array, i64);
    read!(UInt8Array, i8);
    read!(UInt16Array, i16);
    read!(UInt32Array, i32);
    read!(UInt64Array, i64);

    None
}

/// Renders one integer column as a zero-padded hex string of the given width.
macro_rules! hex_number_encoder {
    ($name:ident, $unsigned:ty) => {
        fn $name(array: &dyn Array, row: usize, buf: &mut Vec<u8>) {
            if array.is_null(row) {
                buf.extend_from_slice(b"null");
                return;
            }

            match unsigned_at(array, row) {
                // A stored value too wide for the declared type means the catalog
                // is wrong about the column. Rendering it at the declared width
                // would truncate it into a different discriminator, so it is
                // rendered at the width it actually needs.
                Some(value) => match <$unsigned>::try_from(value) {
                    Ok(narrowed) => encode_hex_bytes(&narrowed.to_be_bytes(), buf),
                    Err(_) => encode_hex_bytes(&value.to_be_bytes(), buf),
                },
                None => buf.extend_from_slice(b"null"),
            }
        }
    };
}

hex_number_encoder!(encode_hex_number_u8, u8);
hex_number_encoder!(encode_hex_number_u16, u16);
hex_number_encoder!(encode_hex_number_u32, u32);
hex_number_encoder!(encode_hex_number_u64, u64);

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

    /// Covers CT-6 · INV-O10
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

    /// Covers CT-6 · INV-O10
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

    /// Covers CT-6 · INV-O8
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
        let arr = Decimal128Array::from(vec![12345i128])
            .with_precision_and_scale(38, 0)
            .unwrap();
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"12345\"");
    }

    #[test]
    fn test_decimal128_scale_nonzero() {
        let arr = Decimal128Array::from(vec![12345i128])
            .with_precision_and_scale(38, 2)
            .unwrap();
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"123.45\"");
    }

    #[test]
    fn test_decimal128_scale_negative_value() {
        let arr = Decimal128Array::from(vec![-12345i128])
            .with_precision_and_scale(38, 3)
            .unwrap();
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"-12.345\"");
    }

    #[test]
    fn test_decimal128_scale_small_value() {
        // 5 with scale 3 → "0.005"
        let arr = Decimal128Array::from(vec![5i128])
            .with_precision_and_scale(38, 3)
            .unwrap();
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"0.005\"");
    }

    #[test]
    fn test_decimal128_negative_small_value() {
        // -5 with scale 3 → "-0.005"
        let arr = Decimal128Array::from(vec![-5i128])
            .with_precision_and_scale(38, 3)
            .unwrap();
        let mut buf = Vec::new();
        encode_bignum(&arr, 0, &mut buf);
        assert_eq!(String::from_utf8(buf).unwrap(), "\"-0.005\"");
    }

    fn rendered_as_hex_number(array: std::sync::Arc<dyn Array>) -> String {
        rendered_as_declared(array, None)
    }

    /// Render with an explicit catalog type, which is what sets the width.
    fn rendered_as_declared(
        array: std::sync::Arc<dyn Array>,
        declared: Option<ColumnType>,
    ) -> String {
        let encoder = resolve_encoder(
            array.data_type(),
            Some(&JsonEncoding::HexNumber),
            declared.as_ref(),
        );
        let mut buf = Vec::new();
        encoder(array.as_ref(), 0, &mut buf);
        String::from_utf8(buf).unwrap()
    }

    /// The width comes from the column, so `d2` is four digits whatever it holds
    /// — that is the shape a client parses.
    #[test]
    fn test_hex_number_pads_to_the_column_width() {
        assert_eq!(
            rendered_as_hex_number(std::sync::Arc::new(UInt8Array::from(vec![1u8]))),
            "\"0x01\""
        );
        assert_eq!(
            rendered_as_hex_number(std::sync::Arc::new(UInt16Array::from(vec![1600u16]))),
            "\"0x0640\""
        );
        assert_eq!(
            rendered_as_hex_number(std::sync::Arc::new(UInt32Array::from(vec![1600u32]))),
            "\"0x00000640\""
        );
    }

    /// §1.5 pins the width to the *declared* type, not to whatever the writer
    /// chose. `"0x0640"` and `"0x640"` are different discriminators, so a `uint16`
    /// `d2` stored in a `uint32` column must still render four digits.
    #[test]
    fn test_hex_number_pads_to_the_declared_width_not_the_physical_one() {
        assert_eq!(
            rendered_as_declared(
                std::sync::Arc::new(UInt32Array::from(vec![1600u32])),
                Some(ColumnType::UInt16)
            ),
            "\"0x0640\"",
            "the catalog says uint16, so the rendering is four digits"
        );
        assert_eq!(
            rendered_as_declared(
                std::sync::Arc::new(Int32Array::from(vec![1600i32])),
                Some(ColumnType::UInt64)
            ),
            "\"0x0000000000000640\"",
            "a `d8` stored narrow and signed still renders at its declared width"
        );
    }

    /// Padding is not truncation: a value that does not fit the declared width
    /// says the catalog is wrong about the column, and rendering it short would
    /// emit a different discriminator than the one stored.
    #[test]
    fn test_hex_number_never_truncates_a_value_too_wide_to_declare() {
        assert_eq!(
            rendered_as_declared(
                std::sync::Arc::new(UInt32Array::from(vec![0x12345u32])),
                Some(ColumnType::UInt16)
            ),
            "\"0x0000000000012345\""
        );
    }

    /// A declared width bounds the values, not the storage (INV-D7). A legacy
    /// transaction is the sentinel `-1` of the *declared* `int16`, so it renders
    /// `"legacy"` out of whatever width the writer chose — and out of an unsigned
    /// column, which carries the same bits under another name.
    ///
    /// Covers CT-6 · INV-D7
    #[test]
    fn test_solana_tx_version_reads_the_sentinel_at_every_physical_width() {
        let legacy: [std::sync::Arc<dyn Array>; 6] = [
            std::sync::Arc::new(Int16Array::from(vec![-1i16])),
            std::sync::Arc::new(Int32Array::from(vec![-1i32])),
            std::sync::Arc::new(Int64Array::from(vec![-1i64])),
            std::sync::Arc::new(Int8Array::from(vec![-1i8])),
            std::sync::Arc::new(UInt16Array::from(vec![u16::MAX])),
            std::sync::Arc::new(UInt32Array::from(vec![u32::MAX])),
        ];

        for array in legacy {
            assert_eq!(
                rendered_as_version(&array),
                "\"legacy\"",
                "-1 in a {:?} column is a legacy transaction",
                array.data_type()
            );
        }

        let versioned: [std::sync::Arc<dyn Array>; 3] = [
            std::sync::Arc::new(Int16Array::from(vec![0i16])),
            std::sync::Arc::new(UInt8Array::from(vec![0u8])),
            std::sync::Arc::new(Int64Array::from(vec![0i64])),
        ];

        for array in versioned {
            assert_eq!(rendered_as_version(&array), "0");
        }
    }

    fn rendered_as_version(array: &std::sync::Arc<dyn Array>) -> String {
        let encoder = resolve_encoder(
            array.data_type(),
            Some(&JsonEncoding::SolanaTxVersion),
            Some(&ColumnType::Int16),
        );
        let mut buf = Vec::new();
        encoder(array.as_ref(), 0, &mut buf);
        String::from_utf8(buf).unwrap()
    }

    /// Same rule for a bignum: a `uint64` fee whose values all fit in sixteen
    /// bits is *stored* in sixteen bits. Reading only the wide arrays rendered it
    /// `null`, which reads as "this transaction had no fee".
    ///
    /// Covers CT-6 · INV-D7
    #[test]
    fn test_bignum_reads_a_narrowed_column() {
        let narrow: [(std::sync::Arc<dyn Array>, &str); 5] = [
            (std::sync::Arc::new(UInt8Array::from(vec![7u8])), "\"7\""),
            (
                std::sync::Arc::new(UInt16Array::from(vec![5000u16])),
                "\"5000\"",
            ),
            (std::sync::Arc::new(Int8Array::from(vec![-7i8])), "\"-7\""),
            (
                std::sync::Arc::new(Int16Array::from(vec![-5000i16])),
                "\"-5000\"",
            ),
            (
                std::sync::Arc::new(UInt64Array::from(vec![u64::MAX])),
                "\"18446744073709551615\"",
            ),
        ];

        for (array, expected) in narrow {
            let mut buf = Vec::new();
            encode_bignum(array.as_ref(), 0, &mut buf);
            assert_eq!(
                String::from_utf8(buf).unwrap(),
                expected,
                "a {:?} column",
                array.data_type()
            );
        }
    }

    /// An encoding names the type the *catalog* believes a column has. The chunk
    /// decides the real one, and an archive outlives the catalog that described
    /// it — so every encoding must survive being pointed at the wrong array.
    /// Downcasting on the catalog's word took the worker thread down mid-response
    /// (INV-E1).
    #[test]
    fn test_an_encoding_survives_the_wrong_physical_type() {
        let wrong: std::sync::Arc<dyn Array> = std::sync::Arc::new(UInt32Array::from(vec![7u32]));

        for encoding in [
            JsonEncoding::JsonVerbatim,
            JsonEncoding::SolanaTxVersion,
            JsonEncoding::TimestampMillisecond,
            JsonEncoding::DecimalString,
            JsonEncoding::HexBytes,
            JsonEncoding::Base58,
        ] {
            let encoder = resolve_encoder(wrong.data_type(), Some(&encoding), None);
            let mut buf = Vec::new();
            encoder(wrong.as_ref(), 0, &mut buf);

            let rendered = String::from_utf8(buf).unwrap();
            assert!(
                serde_json::from_str::<serde_json::Value>(&rendered).is_ok(),
                "{encoding:?} on a UInt32 column rendered {rendered}, which is not JSON"
            );
        }
    }

    /// Physical and declared types disagree routinely in these chunks. A signed
    /// array of the same width must still render hex: falling through to the
    /// numeric encoder emits a `uint64` discriminator as a bare JSON number,
    /// which every parser with 53-bit floats silently rounds.
    #[test]
    fn test_hex_number_survives_a_signed_physical_column() {
        assert_eq!(
            rendered_as_hex_number(std::sync::Arc::new(Int16Array::from(vec![1600i16]))),
            "\"0x0640\""
        );
        assert_eq!(
            rendered_as_hex_number(std::sync::Arc::new(Int64Array::from(vec![-1i64]))),
            "\"0xffffffffffffffff\"",
            "the bytes are the column's, not a re-reading of its sign"
        );
    }

    fn rendered_with(
        array: std::sync::Arc<dyn Array>,
        encoding: Option<&JsonEncoding>,
        declared: Option<&ColumnType>,
    ) -> String {
        let encoder = resolve_encoder(array.data_type(), encoding, declared);
        let mut buf = Vec::new();
        encoder(array.as_ref(), 0, &mut buf);
        String::from_utf8(buf).unwrap()
    }

    /// The *declared* type carries the unit (INV-O9). Every catalog entry today
    /// also sets `encoding: timestamp_millisecond`, which hid this: a
    /// millisecond column that leaves the encoding off was divided by 1000 and
    /// served as seconds.
    ///
    /// Covers CT-6 · INV-O9
    #[test]
    fn test_a_timestamp_takes_its_unit_from_the_declared_type() {
        let millis: std::sync::Arc<dyn Array> =
            std::sync::Arc::new(TimestampMillisecondArray::from(vec![1_700_000_001_500i64]));
        let seconds: std::sync::Arc<dyn Array> =
            std::sync::Arc::new(TimestampSecondArray::from(vec![1_700_000_001i64]));

        assert_eq!(
            rendered_with(
                millis.clone(),
                None,
                Some(&ColumnType::TimestampMillisecond)
            ),
            "1700000001500",
            "declared in milliseconds, stored in milliseconds"
        );
        assert_eq!(
            rendered_with(millis, None, Some(&ColumnType::TimestampSecond)),
            "1700000001",
            "declared in seconds, stored in milliseconds"
        );
        assert_eq!(
            rendered_with(
                seconds.clone(),
                None,
                Some(&ColumnType::TimestampMillisecond)
            ),
            "1700000001000",
            "declared in milliseconds, stored in seconds"
        );
        assert_eq!(
            rendered_with(seconds, None, Some(&ColumnType::TimestampSecond)),
            "1700000001"
        );
    }

    /// An encoding says what the value *is*, so it has to drive the rendering.
    /// Both of these fell through to the physical encoder, which works only
    /// because every such column is stored as the display string already — a
    /// base58 address stored as bytes came back as `0x…` hex.
    ///
    /// Covers CT-6 · INV-O9
    #[test]
    fn test_hex_and_base58_render_a_column_stored_as_bytes() {
        let key = [
            0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
            24, 25, 26, 27, 28, 29, 30, 31,
        ];

        let binary: std::sync::Arc<dyn Array> =
            std::sync::Arc::new(BinaryArray::from(vec![key.as_slice()]));
        let fixed: std::sync::Arc<dyn Array> = std::sync::Arc::new(
            FixedSizeBinaryArray::try_from_iter([key].iter().map(|k| k.as_slice())).unwrap(),
        );

        // Leading zero byte → a leading '1', which is what keeps the encoding
        // injective over 32-byte keys.
        let expected = "\"1thX6LZfHDZZKUs92febYZhYRcXddmzfzF2NvTkPNE\"";
        assert_eq!(
            rendered_with(binary.clone(), Some(&JsonEncoding::Base58), None),
            expected
        );
        assert_eq!(
            rendered_with(fixed.clone(), Some(&JsonEncoding::Base58), None),
            expected
        );

        assert_eq!(
            rendered_with(binary, Some(&JsonEncoding::HexBytes), None),
            "\"0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\""
        );
        assert_eq!(
            rendered_with(fixed, Some(&JsonEncoding::HexBytes), None),
            "\"0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\""
        );
    }

    /// The column every chunk today actually carries: the display string itself.
    /// Rendering it a second time would double-encode it.
    #[test]
    fn test_hex_and_base58_pass_a_text_column_through() {
        let text: std::sync::Arc<dyn Array> =
            std::sync::Arc::new(StringArray::from(vec!["0xdeadbeef"]));
        assert_eq!(
            rendered_with(text.clone(), Some(&JsonEncoding::HexBytes), None),
            "\"0xdeadbeef\""
        );

        let base58: std::sync::Arc<dyn Array> = std::sync::Arc::new(StringArray::from(vec![
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        ]));
        assert_eq!(
            rendered_with(base58, Some(&JsonEncoding::Base58), None),
            "\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\""
        );
    }
}
