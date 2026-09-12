use crate::integers::{
    declared_value, slot_of, stored_key, width_of, IntColumn, IntValues, IntVisitor, INT_TYPES,
};
use crate::text::StringColumn;
use arrow::array::*;
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::boolean::{and, or_kleene};
use arrow::compute::kernels::cmp::{eq, gt_eq, lt_eq};
use arrow::datatypes::*;
use rustc_hash::FxHashSet as HashSet;
use std::sync::{Arc, OnceLock};

/// A predicate met a column stored at a type its values cannot be compared
/// against.
///
/// This is the one outcome a predicate may not answer with a mask. "Matches
/// nothing" is what an all-false mask says, and on a column the writer typed
/// differently from the catalog it is a lie the response cannot be seen to tell
/// (INV-E7). The scan turns this into `UnsupportedKeyType` before any row is
/// read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedType {
    /// What the filter carries, for the message.
    pub expected: &'static str,
    pub stored: DataType,
}

impl std::fmt::Display for UnsupportedType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot compare {} against a column stored as {}",
            self.expected, self.stored
        )
    }
}

impl std::error::Error for UnsupportedType {}

/// A mask, or the reason the column could not be masked.
pub type Mask = Result<BooleanArray, UnsupportedType>;

fn unsupported(expected: &'static str, array: &dyn Array) -> UnsupportedType {
    UnsupportedType {
        expected,
        stored: array.data_type().clone(),
    }
}

fn all(len: usize, value: bool) -> BooleanArray {
    let values = if value {
        BooleanBuffer::new_set(len)
    } else {
        BooleanBuffer::new_unset(len)
    };
    BooleanArray::new(values, None)
}

/// A mask that answers `value` for every row that has one. Nulls stay null,
/// as the arrow kernels leave them, so a shortcut that never compares a row
/// keeps the same rows a comparison would (INV-P7).
fn constant(array: &dyn Array, value: bool) -> BooleanArray {
    let mask = all(array.len(), value);
    BooleanArray::new(mask.values().clone(), array.nulls().cloned())
}

/// A row group's `(min, max)` for one column, in the value space of the type
/// the column is *stored* at.
///
/// Parquet records statistics at the physical type, and that type is `INT32`
/// for every arrow integer up to 32 bits: an `Int8` column and a `UInt32` one
/// arrive alike, and the raw statistic cannot say which reinterpretation is the
/// right one. Reading a `UInt32` above `i32::MAX` needs the bits taken as
/// unsigned; reading an `Int8` of `-1` needs them taken as the value they are.
/// The stored type decides, once, here, so that no predicate has to guess and
/// none of them can guess differently.
pub enum StatRange {
    Ints {
        stored: DataType,
        min: i128,
        max: i128,
    },
    Text {
        min: String,
        max: String,
    },
}

impl StatRange {
    /// Normalize a row group's statistics against the type the column is stored
    /// at. `None` where the pair says nothing a predicate can use.
    pub fn new(stored: &DataType, min: &dyn Array, max: &dyn Array) -> Option<Self> {
        if crate::integers::is_integer(stored) {
            return Some(StatRange::Ints {
                stored: stored.clone(),
                min: stat_int(min, stored)?,
                max: stat_int(max, stored)?,
            });
        }

        Some(StatRange::Text {
            min: stat_text(min)?,
            max: stat_text(max)?,
        })
    }
}

/// One statistic in the stored type's value space.
///
/// Parquet carries it at the physical width — `INT32` or `INT64` — so the
/// stored type's bits are read out of it the way [`declared_value`] reads a
/// column's bits at the catalog's type.
fn stat_int(stat: &dyn Array, stored: &DataType) -> Option<i128> {
    let (bits, signed) = width_of(stored)?;

    let raw = IntColumn::resolve(stat)?;
    if raw.len() == 0 {
        return None;
    }

    Some(declared_value(raw.value(0), bits, signed))
}

fn stat_text(stat: &dyn Array) -> Option<String> {
    let a = stat.as_any().downcast_ref::<StringArray>()?;
    (!a.is_empty()).then(|| a.value(0).to_string())
}

/// Whether every one of a filter's integer values falls outside `[min, max]`,
/// narrowed to the stored width the same way evaluation narrows it.
///
/// Derived from [`IntValues::stored_keys`] rather than from the declared
/// values, so the pruning and the mask cannot disagree: a value the width drops
/// matches nothing and prunes, and a value the width keeps is compared where
/// the stored column actually puts it. Written separately, the two answered
/// differently — a `uint8` 255 matched the `-1` of an `Int8` column and pruned
/// the row group holding it.
fn ints_outside(ints: &IntValues, stored: &DataType, min: i128, max: i128) -> Option<bool> {
    let (_, signed) = width_of(stored)?;

    Some(ints.stored_keys(stored)?.all(|key| {
        let value = if signed {
            key as i64 as i128
        } else {
            key as i128
        };
        value < min || value > max
    }))
}

/// Whether a `>=` (`gte`) or `<=` threshold prunes the group, decided exactly
/// as [`compare_int`] decides the mask: a threshold the stored width cannot
/// hold sits above or below every value the column could have stored.
///
/// A column stored at the other signedness is read at the declared one, and
/// under that reading the group's `[min, max]` is not an interval — the values
/// above the sign bit come out at the other end. Such a group is read.
fn range_outside(threshold: &ScalarValue, stats: &StatRange, gte: bool) -> bool {
    match (threshold, stats) {
        (ScalarValue::Utf8(v), StatRange::Text { min, max }) => {
            if gte {
                max < v
            } else {
                min > v
            }
        }
        (ScalarValue::Boolean(_), _) => false,
        (other, StatRange::Ints { stored, min, max }) => {
            let Some(threshold) = other
                .as_ints()
                .and_then(|ints| Threshold::new(&ints, stored))
            else {
                return false;
            };

            match threshold {
                // Every value the width holds is at or above the threshold.
                Threshold::Below => !gte,
                // Every value the width holds is at or below it.
                Threshold::Above => gte,
                Threshold::Within(value) => {
                    if gte {
                        *max < value
                    } else {
                        *min > value
                    }
                }
                Threshold::Reinterpreted { .. } => false,
            }
        }
        _ => false,
    }
}

/// A predicate that evaluates against an Arrow array, producing a boolean mask.
pub trait ArrayPredicate: Send + Sync {
    /// Evaluate this predicate against the given array, returning a boolean mask.
    ///
    /// Every implementation answers for every physical type the catalog's
    /// declared type may be stored at, and refuses the rest: there is no third
    /// outcome. Callers probe a predicate against an empty array of the stored
    /// type to learn which it is before reading anything.
    fn evaluate(&self, array: &dyn Array) -> Mask;

    /// Whether a row group whose statistics are `stats` can hold no matching
    /// row, and so may be skipped without reading it.
    ///
    /// The answer must agree with [`evaluate`](Self::evaluate): a `true` here
    /// claims every row of the group would have been masked out, and a group
    /// wrongly skipped loses its rows silently. Implementations derive the
    /// claim from the same narrowing evaluation uses rather than repeating it.
    fn can_skip(&self, stats: &StatRange) -> bool;
}

/// Evaluate a predicate on a dictionary-encoded column of any key width: once
/// over the dictionary, then mapped through the keys. `None` when the column is
/// not dictionary-encoded.
///
/// A null key has no value to compare, so its row is false rather than null —
/// the same as a null value in a plain column under the hand-rolled loops
/// below.
fn through_dictionary(predicate: &dyn ArrayPredicate, array: &dyn Array) -> Option<Mask> {
    let dictionary = array.as_any_dictionary_opt()?;

    let mask = match predicate.evaluate(dictionary.values().as_ref()) {
        Ok(mask) => mask,
        Err(e) => return Some(Err(e)),
    };
    let taken = arrow::compute::take(&mask, dictionary.keys(), None)
        .expect("a dictionary's keys index its values");
    let taken = taken.as_boolean();

    let mask = match taken.nulls() {
        Some(nulls) => taken.values() & nulls.inner(),
        None => taken.values().clone(),
    };
    Some(Ok(BooleanArray::new(mask, None)))
}

/// A predicate applied to a specific column in a row.
#[derive(Clone)]
pub struct ColumnPredicate {
    pub column: String,
    pub predicate: Arc<dyn ArrayPredicate>,
}

impl std::fmt::Debug for ColumnPredicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnPredicate")
            .field("column", &self.column)
            .finish()
    }
}

/// A multi-column row predicate. Combines column predicates with AND.
/// Multiple RowPredicates are combined with OR (for multiple request items).
#[derive(Debug, Clone)]
pub struct RowPredicate {
    /// All column predicates must match (AND).
    pub columns: Vec<ColumnPredicate>,
}

// ---------------------------------------------------------------------------
// Concrete predicate implementations
// ---------------------------------------------------------------------------

/// Equality predicate: value == constant.
pub struct EqPredicate {
    value: ScalarValue,
}

/// IN-list predicate: value IN (v1, v2, ...).
///
/// The list arrives typed by the catalog's declared type, and is kept that way:
/// text as text, integers at the declared signedness, fixed-width bytes as
/// bytes. The stored type is met at evaluation, where the integer values are
/// narrowed to it once and remembered.
pub struct InListPredicate {
    values: Arc<dyn Array>,
    string_set: Option<HashSet<String>>,
    ints: Option<IntValues>,
    /// The integer keys at each stored width, built on first use. Indexed by
    /// [`slot_of`].
    stored_keys: [OnceLock<HashSet<u64>>; INT_TYPES],
    fixed_binary_set: Option<HashSet<Vec<u8>>>,
}

/// Bloom filter predicate: check if any of the given values might be in the bloom filter.
pub struct BloomFilterPredicate {
    needles: Vec<Vec<u8>>,
    #[allow(dead_code)]
    num_bytes: usize,
    num_hashes: usize,
}

/// Range >= predicate: value >= threshold.
pub struct RangeGtePredicate {
    value: ScalarValue,
}

/// Range <= predicate: value <= threshold.
pub struct RangeLtePredicate {
    value: ScalarValue,
}

/// List-contains-any predicate: checks if a List column contains any of the target values.
/// Used for filtering on pre-extracted nested list columns (e.g., order_asset: List<UInt32>).
pub struct ListContainsAnyPredicate {
    u32_set: Option<HashSet<u32>>,
    string_set: Option<HashSet<String>>,
}

/// A scalar value for equality comparison.
#[derive(Debug, Clone)]
pub enum ScalarValue {
    Boolean(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int16(i16),
    Int64(i64),
    Utf8(String),
}

impl ScalarValue {
    /// The value as a one-element integer list at its declared signedness, or
    /// `None` for the kinds that are not integers.
    fn as_ints(&self) -> Option<IntValues> {
        match self {
            ScalarValue::UInt8(v) => Some(IntValues::Unsigned(vec![*v as u64])),
            ScalarValue::UInt16(v) => Some(IntValues::Unsigned(vec![*v as u64])),
            ScalarValue::UInt32(v) => Some(IntValues::Unsigned(vec![*v as u64])),
            ScalarValue::UInt64(v) => Some(IntValues::Unsigned(vec![*v])),
            ScalarValue::Int16(v) => Some(IntValues::Signed(vec![*v as i64])),
            ScalarValue::Int64(v) => Some(IntValues::Signed(vec![*v])),
            ScalarValue::Boolean(_) | ScalarValue::Utf8(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Shared kernels
// ---------------------------------------------------------------------------

/// Membership of an integer column's values in a set of [`stored_key`]s, at
/// whatever width the column is stored. `None` when the column is not an
/// integer.
fn in_list_int(array: &dyn Array, keys: &HashSet<u64>) -> Option<BooleanArray> {
    Some(IntColumn::resolve(array)?.visit(InKeys(keys)))
}

struct InKeys<'k>(&'k HashSet<u64>);

impl IntVisitor for InKeys<'_> {
    type Out = BooleanArray;

    fn visit<T>(self, array: &PrimitiveArray<T>) -> BooleanArray
    where
        T: ArrowPrimitiveType,
        T::Native: Into<i128>,
    {
        let len = array.len();
        let mut builder = BooleanBufferBuilder::new(len);
        for i in 0..len {
            if array.is_null(i) {
                builder.append(false);
            } else {
                builder.append(self.0.contains(&stored_key(array.value(i).into())));
            }
        }
        BooleanArray::new(builder.finish(), array.nulls().cloned())
    }
}

/// Membership of a text column's values in a set of strings, at whatever type
/// the column is stored. `None` when the column does not hold text.
fn in_list_text(array: &dyn Array, set: &HashSet<String>) -> Option<BooleanArray> {
    let column = StringColumn::resolve(array)?;

    let len = column.len();
    let mut builder = BooleanBufferBuilder::new(len);
    for i in 0..len {
        builder.append(column.value(i).is_some_and(|s| set.contains(s)));
    }
    Some(BooleanArray::new(builder.finish(), column.nulls()))
}

/// Membership of a bytes column's values in a set of byte strings. `None` when
/// the column does not hold bytes.
fn in_list_bytes(array: &dyn Array, set: &HashSet<Vec<u8>>) -> Option<BooleanArray> {
    macro_rules! keyed {
        ($($array:ty),+ $(,)?) => {
            $(if let Some(arr) = array.as_any().downcast_ref::<$array>() {
                return Some(BooleanArray::from_iter((0..arr.len()).map(|i| {
                    Some(!arr.is_null(i) && set.contains(arr.value(i)))
                })));
            })+
        };
    }

    keyed!(FixedSizeBinaryArray, BinaryArray, LargeBinaryArray);
    None
}

/// Where an integer threshold stands against a column at its stored type.
enum Threshold {
    /// Under every value the width can hold.
    Below,
    /// Held by the width; compare stored values against it directly.
    Within(i128),
    /// Over every value the width can hold.
    Above,
    /// Declared at the other signedness from the stored one, so the stored
    /// values have to be read at the declared signedness before comparing
    /// ([`declared_value`]).
    Reinterpreted {
        value: i128,
        bits: u32,
        signed: bool,
    },
}

impl Threshold {
    /// `None` when the column is not an integer.
    fn new(ints: &IntValues, stored: &DataType) -> Option<Self> {
        let (bits, stored_signed) = width_of(stored)?;
        let value = match ints {
            IntValues::Unsigned(v) => v[0] as i128,
            IntValues::Signed(v) => v[0] as i128,
        };

        if ints.signed() != stored_signed {
            return Some(Self::Reinterpreted {
                value,
                bits,
                signed: ints.signed(),
            });
        }

        let (min, max) = if stored_signed {
            (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
        } else {
            (0, (1i128 << bits) - 1)
        };

        Some(if value < min {
            Self::Below
        } else if value > max {
            Self::Above
        } else {
            Self::Within(value)
        })
    }
}

/// `array >= threshold` (`gte`) or `array <= threshold` over an integer column
/// of any width, the stored bits read at the declared signedness (INV-D7). A
/// threshold the width cannot hold sits above or below every stored value.
/// `None` when the column is not an integer.
fn compare_int(array: &dyn Array, ints: &IntValues, gte: bool) -> Option<BooleanArray> {
    let column = IntColumn::resolve(array)?;

    let mask = match Threshold::new(ints, array.data_type())? {
        // Every stored value is above the threshold.
        Threshold::Below => constant(array, gte),
        // Every stored value is below it.
        Threshold::Above => constant(array, !gte),
        Threshold::Within(value) => column.visit(CompareStored { value, gte }),
        Threshold::Reinterpreted {
            value,
            bits,
            signed,
        } => column.visit(CompareDeclared {
            value,
            bits,
            signed,
            gte,
        }),
    };
    Some(mask)
}

/// The arrow kernel, for a threshold the stored width holds.
struct CompareStored {
    value: i128,
    gte: bool,
}

impl IntVisitor for CompareStored {
    type Out = BooleanArray;

    fn visit<T>(self, array: &PrimitiveArray<T>) -> BooleanArray
    where
        T: ArrowPrimitiveType,
        T::Native: TryFrom<i128>,
    {
        let Ok(value) = T::Native::try_from(self.value) else {
            unreachable!("`Threshold::Within` holds only what the width holds");
        };
        let scalar = PrimitiveArray::<T>::new_scalar(value);

        let mask = if self.gte {
            gt_eq(array, &scalar)
        } else {
            lt_eq(array, &scalar)
        };
        mask.expect("same type on both sides")
    }
}

/// Row by row through [`declared_value`], for a column stored at the other
/// signedness.
struct CompareDeclared {
    value: i128,
    bits: u32,
    signed: bool,
    gte: bool,
}

impl IntVisitor for CompareDeclared {
    type Out = BooleanArray;

    fn visit<T>(self, array: &PrimitiveArray<T>) -> BooleanArray
    where
        T: ArrowPrimitiveType,
        T::Native: Into<i128>,
    {
        let values = array.values().iter().map(|&stored| {
            let value = declared_value(stored.into(), self.bits, self.signed);
            if self.gte {
                value >= self.value
            } else {
                value <= self.value
            }
        });
        BooleanArray::new(BooleanBuffer::from_iter(values), array.nulls().cloned())
    }
}

/// `array >= threshold` or `array <= threshold` over a text column, in byte
/// order. `None` when the column does not hold text.
fn compare_text(array: &dyn Array, threshold: &str, gte: bool) -> Option<BooleanArray> {
    macro_rules! compare {
        ($($array:ty),+ $(,)?) => {
            $(if let Some(arr) = array.as_any().downcast_ref::<$array>() {
                let scalar = <$array>::new_scalar(threshold);
                let mask = if gte { gt_eq(arr, &scalar) } else { lt_eq(arr, &scalar) };
                return Some(mask.expect("same type on both sides"));
            })+
        };
    }

    compare!(StringArray, LargeStringArray, StringViewArray);
    None
}

// ---------------------------------------------------------------------------
// EqPredicate
// ---------------------------------------------------------------------------

impl EqPredicate {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl ArrayPredicate for EqPredicate {
    fn evaluate(&self, array: &dyn Array) -> Mask {
        if let Some(mask) = through_dictionary(self, array) {
            return mask;
        }

        match &self.value {
            ScalarValue::Boolean(v) => {
                if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                    return Ok(eq(arr, &BooleanArray::new_scalar(*v)).expect("boolean equality"));
                }
                // A flag stored as a number, the way the reference reads one:
                // `true` is 1, and nothing else is.
                let keys = HashSet::from_iter([*v as u64]);
                in_list_int(array, &keys).ok_or_else(|| unsupported("a boolean", array))
            }
            ScalarValue::Utf8(v) => {
                let set = HashSet::from_iter([v.clone()]);
                in_list_text(array, &set).ok_or_else(|| unsupported("a string", array))
            }
            other => {
                let ints = other
                    .as_ints()
                    .expect("every non-boolean, non-text kind is an integer");
                let keys: HashSet<u64> = ints
                    .stored_keys(array.data_type())
                    .ok_or_else(|| unsupported("an integer", array))?
                    .collect();
                in_list_int(array, &keys).ok_or_else(|| unsupported("an integer", array))
            }
        }
    }

    fn can_skip(&self, stats: &StatRange) -> bool {
        match (&self.value, stats) {
            (ScalarValue::Utf8(v), StatRange::Text { min, max }) => v < min || v > max,
            // A flag stored as a number is compared as one, but a boolean
            // carries no width to narrow to; the group is read.
            (ScalarValue::Boolean(_), _) => false,
            (value, StatRange::Ints { stored, min, max }) => {
                let Some(ints) = value.as_ints() else {
                    return false;
                };
                ints_outside(&ints, stored, *min, *max).unwrap_or(false)
            }
            // For other types, don't skip
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// InListPredicate
// ---------------------------------------------------------------------------

impl InListPredicate {
    pub fn new(values: Arc<dyn Array>) -> Self {
        let string_set = values
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| (0..arr.len()).map(|i| arr.value(i).to_string()).collect());
        let ints = IntValues::from_array(values.as_ref());
        let fixed_binary_set = values
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .map(|arr| (0..arr.len()).map(|i| arr.value(i).to_vec()).collect());
        Self {
            values,
            string_set,
            ints,
            stored_keys: Default::default(),
            fixed_binary_set,
        }
    }

    /// Create an InList predicate from string values.
    pub fn from_strings(values: &[&str]) -> Self {
        let array: Arc<dyn Array> = Arc::new(StringArray::from(values.to_vec()));
        Self::new(array)
    }

    /// Create an InList predicate from u64 values.
    pub fn from_u64s(values: &[u64]) -> Self {
        let array: Arc<dyn Array> = Arc::new(UInt64Array::from(values.to_vec()));
        Self::new(array)
    }

    /// Create an InList predicate from u8 values.
    pub fn from_u8s(values: &[u8]) -> Self {
        let array: Arc<dyn Array> = Arc::new(UInt8Array::from(values.to_vec()));
        Self::new(array)
    }

    /// The integer keys at the stored width, built once per width.
    fn keys_at(&self, ints: &IntValues, stored: &DataType) -> Option<&HashSet<u64>> {
        let slot = slot_of(stored)?;
        Some(self.stored_keys[slot].get_or_init(|| {
            ints.stored_keys(stored)
                .expect("the slot exists only for integer types")
                .collect()
        }))
    }
}

impl ArrayPredicate for InListPredicate {
    fn evaluate(&self, array: &dyn Array) -> Mask {
        if let Some(mask) = through_dictionary(self, array) {
            return mask;
        }

        if let Some(set) = self.string_set.as_ref() {
            return in_list_text(array, set).ok_or_else(|| unsupported("a list of strings", array));
        }
        if let Some(ints) = self.ints.as_ref() {
            let keys = self
                .keys_at(ints, array.data_type())
                .ok_or_else(|| unsupported("a list of integers", array))?;
            return in_list_int(array, keys)
                .ok_or_else(|| unsupported("a list of integers", array));
        }
        if let Some(set) = self.fixed_binary_set.as_ref() {
            return in_list_bytes(array, set)
                .ok_or_else(|| unsupported("a list of byte strings", array));
        }

        Err(unsupported("a filter list", array))
    }

    fn can_skip(&self, stats: &StatRange) -> bool {
        match stats {
            StatRange::Text { min, max } => {
                let Some(list) = self.values.as_any().downcast_ref::<StringArray>() else {
                    return false;
                };
                (0..list.len()).all(|i| {
                    let v = list.value(i);
                    v < min.as_str() || v > max.as_str()
                })
            }
            StatRange::Ints { stored, min, max } => {
                let Some(ints) = self.ints.as_ref() else {
                    return false;
                };
                ints_outside(ints, stored, *min, *max).unwrap_or(false)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BloomFilterPredicate
// ---------------------------------------------------------------------------

impl BloomFilterPredicate {
    /// Create a bloom filter predicate.
    /// `needles` are the values to check for membership.
    /// The bloom filter column contains fixed-size binary arrays.
    pub fn new(needles: Vec<Vec<u8>>, num_bytes: usize, num_hashes: usize) -> Self {
        Self {
            needles,
            num_bytes,
            num_hashes,
        }
    }

    fn check_bloom(&self, bloom_bytes: &[u8]) -> bool {
        // Check if ANY needle might be in the bloom filter
        self.needles
            .iter()
            .any(|needle| bloom_contains(bloom_bytes, needle, self.num_hashes))
    }
}

/// The bit a value's `n`-th hash sets in a filter `num_bits` bits wide.
///
/// The value is hashed the way Rust hashes a `str`: the bytes, then a `0xff`
/// terminator, through XXH3 seeded with `n`.
///
/// Public because [INV-P9] is a claim about the *construction* — width, hash
/// count, hash function, value serialisation — and a construction reachable only
/// through a membership test is one nothing can compare against the archive
/// writer's. Too few hashes, or too narrow a filter, never produces a false
/// negative, so `contains` alone cannot see the mismatch that matters.
///
/// `num_bits` must be non-zero: a filter of no bits has no bit to name, and the
/// caller that can reach one — an empty stored array — answers that case for
/// itself. Stated here because the guard used to sit in the only caller, and a
/// bare `% 0` reports a contract violation as an arithmetic panic.
///
/// [INV-P9]: ../../spec/07-invariants.md#inv-p9
#[inline]
pub fn bloom_bit(value: &[u8], n: usize, num_bits: usize) -> usize {
    assert!(num_bits > 0, "a bloom filter of no bits has no bit {n}");

    let mut hasher = xxhash_rust::xxh3::Xxh3Builder::new()
        .with_seed(n as u64)
        .build();
    std::hash::Hasher::write(&mut hasher, value);
    std::hash::Hasher::write_u8(&mut hasher, 0xff);

    (std::hash::Hasher::finish(&hasher) as usize) % num_bits
}

/// Check if a bloom filter (byte array) might contain a value.
///
/// The width comes from the stored array rather than from the catalog: the bits
/// were set at whatever width the writer used, and that is the only width that
/// reads them back.
fn bloom_contains(filter: &[u8], value: &[u8], num_hashes: usize) -> bool {
    let num_bits = filter.len() * 8;
    if num_bits == 0 {
        return false;
    }

    (0..num_hashes).all(|n| {
        let bit = bloom_bit(value, n, num_bits);
        filter[bit / 8] & (1 << (bit % 8)) != 0
    })
}

impl ArrayPredicate for BloomFilterPredicate {
    fn evaluate(&self, array: &dyn Array) -> Mask {
        if let Some(mask) = through_dictionary(self, array) {
            return mask;
        }

        // The filter is a byte string; which bytes type carries it is the
        // writer's choice.
        macro_rules! checked {
            ($($array:ty),+ $(,)?) => {
                $(if let Some(arr) = array.as_any().downcast_ref::<$array>() {
                    return Ok(BooleanArray::from_iter((0..arr.len()).map(|i| {
                        Some(!arr.is_null(i) && self.check_bloom(arr.value(i)))
                    })));
                })+
            };
        }

        checked!(FixedSizeBinaryArray, BinaryArray, LargeBinaryArray);
        Err(unsupported("a bloom filter", array))
    }

    fn can_skip(&self, _stats: &StatRange) -> bool {
        // Cannot use row group stats to skip bloom filter columns
        false
    }
}

// ---------------------------------------------------------------------------
// Range predicates
// ---------------------------------------------------------------------------

/// `array >= threshold` (`gte`) or `array <= threshold`, for the two threshold
/// kinds the plan compiles: an integer compared by value at any stored width,
/// and text compared in byte order (the `*NonZero` flags, `call_value >= "0x1"`).
fn compare(threshold: &ScalarValue, array: &dyn Array, gte: bool) -> Mask {
    match threshold {
        ScalarValue::Utf8(v) => {
            compare_text(array, v, gte).ok_or_else(|| unsupported("a text threshold", array))
        }
        ScalarValue::Boolean(_) => Err(unsupported("a range threshold", array)),
        other => {
            let ints = other
                .as_ints()
                .expect("every non-boolean, non-text kind is an integer");
            compare_int(array, &ints, gte).ok_or_else(|| unsupported("an integer threshold", array))
        }
    }
}

impl RangeGtePredicate {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl ArrayPredicate for RangeGtePredicate {
    fn evaluate(&self, array: &dyn Array) -> Mask {
        if let Some(mask) = through_dictionary(self, array) {
            return mask;
        }
        compare(&self.value, array, true)
    }

    fn can_skip(&self, stats: &StatRange) -> bool {
        range_outside(&self.value, stats, true)
    }
}

impl RangeLtePredicate {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl ArrayPredicate for RangeLtePredicate {
    fn evaluate(&self, array: &dyn Array) -> Mask {
        if let Some(mask) = through_dictionary(self, array) {
            return mask;
        }
        compare(&self.value, array, false)
    }

    fn can_skip(&self, stats: &StatRange) -> bool {
        range_outside(&self.value, stats, false)
    }
}

// ---------------------------------------------------------------------------
// ListContainsAnyPredicate
// ---------------------------------------------------------------------------

/// The elements of a list column, resolved once for the whole column rather
/// than per row.
enum ListElements<'a> {
    Ints(IntColumn<'a>),
    Text(StringColumn<'a>),
}

impl ListContainsAnyPredicate {
    pub fn new_u32(values: Vec<u32>) -> Self {
        Self {
            u32_set: Some(values.into_iter().collect()),
            string_set: None,
        }
    }

    pub fn new_string(values: Vec<String>) -> Self {
        Self {
            u32_set: None,
            string_set: Some(values.into_iter().collect()),
        }
    }

    /// Evaluate against a list array of either offset width.
    fn evaluate_list<O: OffsetSizeTrait>(&self, list: &GenericListArray<O>) -> Mask {
        let values = list.values();
        let elements = if self.u32_set.is_some() {
            IntColumn::resolve(values.as_ref()).map(ListElements::Ints)
        } else {
            StringColumn::resolve(values.as_ref()).map(ListElements::Text)
        };
        let elements = elements.ok_or_else(|| unsupported(self.expected(), list))?;

        let offsets = list.value_offsets();
        let mut results = Vec::with_capacity(list.len());
        for i in 0..list.len() {
            if list.is_null(i) {
                results.push(false);
                continue;
            }

            let range = offsets[i].as_usize()..offsets[i + 1].as_usize();
            let matches = match &elements {
                // An element the declared width cannot hold is no id at all;
                // truncating it would make it a different one.
                ListElements::Ints(ints) => {
                    let set = self.u32_set.as_ref().expect("resolved as integers");
                    range.into_iter().any(|j| {
                        !ints.is_null(j)
                            && u32::try_from(ints.value(j)).is_ok_and(|v| set.contains(&v))
                    })
                }
                ListElements::Text(text) => {
                    let set = self.string_set.as_ref().expect("resolved as text");
                    range
                        .into_iter()
                        .any(|j| text.value(j).is_some_and(|s| set.contains(s)))
                }
            };
            results.push(matches);
        }
        Ok(BooleanArray::from(results))
    }

    fn expected(&self) -> &'static str {
        if self.u32_set.is_some() {
            "a list of integers"
        } else {
            "a list of strings"
        }
    }
}

impl ArrayPredicate for ListContainsAnyPredicate {
    fn evaluate(&self, array: &dyn Array) -> Mask {
        if let Some(list) = array.as_any().downcast_ref::<GenericListArray<i32>>() {
            self.evaluate_list(list)
        } else if let Some(list) = array.as_any().downcast_ref::<GenericListArray<i64>>() {
            self.evaluate_list(list)
        } else {
            Err(unsupported(self.expected(), array))
        }
    }

    fn can_skip(&self, _stats: &StatRange) -> bool {
        // No row group pruning for list-contains predicates
        false
    }
}

// ---------------------------------------------------------------------------
// NeverPredicate
// ---------------------------------------------------------------------------

/// A predicate no row can satisfy.
///
/// An empty filter list is not "no filter" — it is a filter nothing passes
/// (INV-P14, and the reference's `is_never`). Compiling it away instead widens
/// the query to the whole table, and the response says nothing about it.
///
/// Unlike an empty `InListPredicate` this does not depend on the column having
/// statistics: `can_skip` is unconditionally true, so the row group is pruned
/// whether or not a stat exists to prune it on.
pub struct NeverPredicate;

impl ArrayPredicate for NeverPredicate {
    fn evaluate(&self, array: &dyn Array) -> Mask {
        Ok(all(array.len(), false))
    }

    fn can_skip(&self, _stats: &StatRange) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// RowPredicate implementation
// ---------------------------------------------------------------------------

impl RowPredicate {
    pub fn new(columns: Vec<ColumnPredicate>) -> Self {
        Self { columns }
    }

    /// Returns the column names needed for this predicate.
    pub fn required_columns(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.column.as_str()).collect()
    }

    /// Evaluate the predicate on a RecordBatch, returning a boolean mask.
    /// All column predicates are ANDed together.
    ///
    /// A column absent from the batch is skipped rather than fatal, so that this
    /// kernel cannot panic on an unexpected batch. That tolerance is not the
    /// engine's policy: `scan` rejects a filter on a column the chunk does not
    /// have before any row is read, because skipping it silently widens the
    /// query to everything (INV-X3).
    pub fn evaluate(&self, batch: &RecordBatch) -> Mask {
        let mut result: Option<BooleanArray> = None;

        for col_pred in &self.columns {
            let Some(col) = batch.column_by_name(&col_pred.column) else {
                continue; // missing column → all-true (no filtering)
            };
            let mask = col_pred.predicate.evaluate(col.as_ref())?;
            result = Some(match result {
                None => mask,
                Some(prev) => and(&prev, &mask).unwrap(),
            });
        }

        Ok(result.unwrap_or_else(|| all(batch.num_rows(), true)))
    }

    /// Check if the entire row group can be skipped using column statistics.
    /// Returns true if no rows can match.
    pub fn can_skip_row_group(&self, stats_fn: &ColumnStats) -> bool {
        // ALL column predicates must pass for a row to match.
        // If ANY column predicate says it can skip (no matches possible), skip the group.
        for col_pred in &self.columns {
            if let Some(stats) = stats_fn(&col_pred.column) {
                if col_pred.predicate.can_skip(&stats) {
                    return true;
                }
            }
        }
        false
    }
}

/// Combine multiple row predicates with OR (multiple request items).
pub fn or_row_predicates(predicates: &[&RowPredicate], batch: &RecordBatch) -> Mask {
    let mut result: Option<BooleanArray> = None;
    for pred in predicates {
        let mask = pred.evaluate(batch)?;
        result = Some(match result {
            None => mask,
            Some(prev) => or_kleene(&prev, &mask).unwrap(),
        });
    }
    Ok(result.unwrap_or_else(|| all(batch.num_rows(), false)))
}

/// A row group's statistics for one column, normalized against the type the
/// column is stored at, or `None` where the writer recorded none.
pub type ColumnStats<'a> = dyn Fn(&str) -> Option<StatRange> + 'a;

/// Check if ALL row predicates (ORed) say skip for a row group.
pub fn can_skip_row_group_or(predicates: &[&RowPredicate], stats_fn: &ColumnStats) -> bool {
    // OR: skip only if ALL predicates say skip
    predicates.iter().all(|p| p.can_skip_row_group(stats_fn))
}

/// Filter a RecordBatch to only include rows matching any of the given predicates (OR'd).
/// Returns None if the filtered batch would be empty.
pub fn evaluate_predicates_on_batch(
    batch: &RecordBatch,
    predicates: &[RowPredicate],
) -> Result<Option<RecordBatch>, UnsupportedType> {
    if batch.num_rows() == 0 {
        return Ok(None);
    }
    let pred_refs: Vec<&RowPredicate> = predicates.iter().collect();
    let mask = or_row_predicates(&pred_refs, batch)?;
    let filtered =
        arrow::compute::filter_record_batch(batch, &mask).expect("mask sized to the batch");
    Ok((filtered.num_rows() > 0).then_some(filtered))
}

/// Whether a predicate can be evaluated against a column stored at
/// `data_type`, learned by evaluating it against an empty column of that type.
///
/// One question, one answer: `evaluate` is the only place that knows which
/// types a predicate reads, so asking it is what keeps the check and the
/// evaluation from drifting apart.
pub fn check_stored_type(
    predicate: &dyn ArrayPredicate,
    data_type: &DataType,
) -> Result<(), UnsupportedType> {
    predicate
        .evaluate(new_empty_array(data_type).as_ref())
        .map(|_| ())
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

/// Create a predicate on `column` that no row can satisfy.
pub fn col_never(column: &str) -> ColumnPredicate {
    ColumnPredicate {
        column: column.to_string(),
        predicate: Arc::new(NeverPredicate),
    }
}

/// Create an equality predicate for a column.
pub fn col_eq(column: &str, value: ScalarValue) -> ColumnPredicate {
    ColumnPredicate {
        column: column.to_string(),
        predicate: Arc::new(EqPredicate::new(value)),
    }
}

/// Create an IN-list predicate for a column.
pub fn col_in_list(column: &str, values: Arc<dyn Array>) -> ColumnPredicate {
    ColumnPredicate {
        column: column.to_string(),
        predicate: Arc::new(InListPredicate::new(values)),
    }
}

/// Create a list-contains-any predicate for a List<UInt32> column.
pub fn col_list_contains_any_u32(column: &str, values: Vec<u32>) -> ColumnPredicate {
    ColumnPredicate {
        column: column.to_string(),
        predicate: Arc::new(ListContainsAnyPredicate::new_u32(values)),
    }
}

/// Create a list-contains-any predicate for a List<String> column.
pub fn col_list_contains_any_string(column: &str, values: Vec<String>) -> ColumnPredicate {
    ColumnPredicate {
        column: column.to_string(),
        predicate: Arc::new(ListContainsAnyPredicate::new_string(values)),
    }
}

/// Create a bloom filter predicate for a column.
pub fn col_bloom(
    column: &str,
    needles: Vec<Vec<u8>>,
    num_bytes: usize,
    num_hashes: usize,
) -> ColumnPredicate {
    ColumnPredicate {
        column: column.to_string(),
        predicate: Arc::new(BloomFilterPredicate::new(needles, num_bytes, num_hashes)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eq_predicate_boolean() {
        let pred = EqPredicate::new(ScalarValue::Boolean(true));
        let array = BooleanArray::from(vec![true, false, true, false]);
        let result = pred.evaluate(&array).unwrap();
        assert_eq!(result, BooleanArray::from(vec![true, false, true, false]));
    }

    #[test]
    fn test_eq_predicate_uint64() {
        let pred = EqPredicate::new(ScalarValue::UInt64(42));
        let array = UInt64Array::from(vec![10, 42, 42, 100]);
        let result = pred.evaluate(&array).unwrap();
        assert_eq!(result, BooleanArray::from(vec![false, true, true, false]));
    }

    /// Covers CT-3 · INV-P2
    #[test]
    fn test_in_list_predicate_strings() {
        let pred = InListPredicate::from_strings(&["alice", "bob"]);
        let array = StringArray::from(vec!["alice", "charlie", "bob", "dave"]);
        let result = pred.evaluate(&array).unwrap();
        assert_eq!(result, BooleanArray::from(vec![true, false, true, false]));
    }

    /// Covers CT-3 · INV-P2
    #[test]
    fn test_in_list_predicate_u64() {
        let pred = InListPredicate::from_u64s(&[10, 30]);
        let array = UInt64Array::from(vec![10, 20, 30, 40]);
        let result = pred.evaluate(&array).unwrap();
        assert_eq!(result, BooleanArray::from(vec![true, false, true, false]));
    }

    /// Signed filters follow the declared signedness even when a writer stores
    /// the same values in an unsigned or differently sized physical array.
    #[test]
    fn test_signed_in_list_at_every_physical_integer_type() {
        let pred = InListPredicate::new(Arc::new(Int64Array::from(vec![-1, 7])));
        let arrays: [Arc<dyn Array>; 8] = [
            Arc::new(Int8Array::from(vec![-1, 0, 7])),
            Arc::new(Int16Array::from(vec![-1, 0, 7])),
            Arc::new(Int32Array::from(vec![-1, 0, 7])),
            Arc::new(Int64Array::from(vec![-1, 0, 7])),
            Arc::new(UInt8Array::from(vec![u8::MAX, 0, 7])),
            Arc::new(UInt16Array::from(vec![u16::MAX, 0, 7])),
            Arc::new(UInt32Array::from(vec![u32::MAX, 0, 7])),
            Arc::new(UInt64Array::from(vec![u64::MAX, 0, 7])),
        ];

        for array in arrays {
            assert_eq!(
                pred.evaluate(array.as_ref()).unwrap(),
                BooleanArray::from(vec![true, false, true]),
                "signed IN-list over {:?}",
                array.data_type()
            );
        }
    }

    /// Covers CT-3 · INV-P4
    #[test]
    fn test_row_predicate_and() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["x", "y", "x", "y"])),
            ],
        )
        .unwrap();

        let pred = RowPredicate::new(vec![
            col_eq("a", ScalarValue::UInt64(3)),
            col_eq("b", ScalarValue::Utf8("x".to_string())),
        ]);

        let mask = pred.evaluate(&batch).unwrap();
        // Only row 2 (0-indexed) has a=3 AND b="x"
        assert_eq!(mask, BooleanArray::from(vec![false, false, true, false]));
    }

    /// A row group's statistics as parquet records them: at the physical type,
    /// which is `INT32` for every arrow integer up to 32 bits, with an unsigned
    /// column's bits carried in a signed slot.
    fn parquet_stats(column: &dyn Array) -> (Arc<dyn Array>, Arc<dyn Array>) {
        macro_rules! bounds {
            ($($arr:ty => $mid:ty, $physical:ty, $slot:ty);+ $(;)?) => {
                $(if let Some(a) = column.as_any().downcast_ref::<$arr>() {
                    let mut values = (0..a.len()).map(|i| a.value(i) as $mid);
                    let first = values.next().expect("a non-empty column");
                    let (mut low, mut high) = (first, first);
                    for v in values {
                        low = low.min(v);
                        high = high.max(v);
                    }

                    return (
                        Arc::new(<$physical>::from(vec![low as $slot])) as Arc<dyn Array>,
                        Arc::new(<$physical>::from(vec![high as $slot])) as Arc<dyn Array>,
                    );
                })+
            };
        }

        bounds!(
            UInt8Array => u32, Int32Array, i32;
            UInt16Array => u32, Int32Array, i32;
            UInt32Array => u32, Int32Array, i32;
            UInt64Array => u64, Int64Array, i64;
            Int8Array => i32, Int32Array, i32;
            Int16Array => i32, Int32Array, i32;
            Int32Array => i32, Int32Array, i32;
            Int64Array => i64, Int64Array, i64;
        );
        unreachable!("the law is stated over integer columns")
    }

    /// The eight widths a writer may pick, each holding its own extremes and
    /// two ordinary values so a group is never all boundary.
    ///
    /// `-1` is there because it is what a signed column holds where an unsigned
    /// one of the same width holds its maximum, and because it is the value
    /// whose statistic a reader that guesses the signedness gets wrong.
    fn columns_at_every_width() -> Vec<Arc<dyn Array>> {
        vec![
            Arc::new(UInt8Array::from(vec![2u8, 7, u8::MAX])) as Arc<dyn Array>,
            Arc::new(UInt16Array::from(vec![2u16, 7, u16::MAX])),
            Arc::new(UInt32Array::from(vec![2u32, 7, u32::MAX])),
            Arc::new(UInt64Array::from(vec![2u64, 7, u64::MAX])),
            Arc::new(Int8Array::from(vec![2i8, 7, -1, i8::MIN, i8::MAX])),
            Arc::new(Int16Array::from(vec![2i16, 7, -1, i16::MIN, i16::MAX])),
            Arc::new(Int32Array::from(vec![2i32, 7, -1, i32::MIN, i32::MAX])),
            Arc::new(Int64Array::from(vec![2i64, 7, -1, i64::MIN, i64::MAX])),
        ]
    }

    /// Every filter kind the plan compiles over an integer column, at every
    /// value kind it compiles them with, across the boundaries of each width:
    /// the last value a width holds, the first it does not, and the point where
    /// a signed statistic slot goes negative.
    fn predicates_over_widths() -> Vec<(String, Arc<dyn ArrayPredicate>)> {
        const UNSIGNED: &[u64] = &[
            0,
            1,
            2,
            7,
            127,
            128,
            255,
            256,
            32767,
            32768,
            65535,
            65536,
            1 << 31,
            (1 << 31) - 1,
            u32::MAX as u64,
            1 << 63,
            u64::MAX,
        ];
        const SIGNED: &[i64] = &[
            -1,
            0,
            2,
            7,
            -128,
            127,
            128,
            -32768,
            32767,
            i32::MIN as i64,
            i32::MAX as i64,
            i64::MIN,
            i64::MAX,
        ];

        let mut out: Vec<(String, Arc<dyn ArrayPredicate>)> = Vec::new();

        let mut push = |what: String, scalar: ScalarValue| {
            out.push((
                format!("eq {what}"),
                Arc::new(EqPredicate::new(scalar.clone())) as Arc<dyn ArrayPredicate>,
            ));
            out.push((
                format!("gte {what}"),
                Arc::new(RangeGtePredicate::new(scalar.clone())),
            ));
            out.push((
                format!("lte {what}"),
                Arc::new(RangeLtePredicate::new(scalar)),
            ));
        };

        for &v in UNSIGNED {
            if let Ok(v8) = u8::try_from(v) {
                push(format!("u8 {v}"), ScalarValue::UInt8(v8));
            }
            if let Ok(v16) = u16::try_from(v) {
                push(format!("u16 {v}"), ScalarValue::UInt16(v16));
            }
            if let Ok(v32) = u32::try_from(v) {
                push(format!("u32 {v}"), ScalarValue::UInt32(v32));
            }
            push(format!("u64 {v}"), ScalarValue::UInt64(v));
        }
        for &v in SIGNED {
            if let Ok(v16) = i16::try_from(v) {
                push(format!("i16 {v}"), ScalarValue::Int16(v16));
            }
            push(format!("i64 {v}"), ScalarValue::Int64(v));
        }

        // The list forms, at both signednesses: a value alone, and beside one
        // the group holds, so a list is not pruned on its first element.
        for &v in UNSIGNED {
            out.push((
                format!("in-list u64 {v}"),
                Arc::new(InListPredicate::from_u64s(&[v])),
            ));
            out.push((
                format!("in-list u64 {v} + 7"),
                Arc::new(InListPredicate::from_u64s(&[v, 7])),
            ));
            if let Ok(v8) = u8::try_from(v) {
                out.push((
                    format!("in-list u8 {v}"),
                    Arc::new(InListPredicate::from_u8s(&[v8])),
                ));
            }
        }
        for &v in SIGNED {
            out.push((
                format!("in-list i64 {v}"),
                Arc::new(InListPredicate::new(Arc::new(Int64Array::from(vec![v])))),
            ));
            if let Ok(v16) = i16::try_from(v) {
                out.push((
                    format!("in-list i16 {v}"),
                    Arc::new(InListPredicate::new(Arc::new(Int16Array::from(vec![v16])))),
                ));
            }
        }

        out
    }

    /// Pruning may only drop a row group the mask would have emptied anyway.
    ///
    /// The two used to be written separately and disagreed: `evaluate` narrows
    /// a filter value to the column's stored width, `can_skip` compared the
    /// declared value against a statistic it read at the parquet physical type.
    /// A `uint8` 255 matched the `-1` of an `Int8` column and pruned the row
    /// group holding it, so the answer depended on whether the writer had
    /// recorded statistics at all.
    ///
    /// Covers CT-3 · INV-P16
    #[test]
    fn pruning_never_drops_a_row_the_mask_would_keep() {
        for column in columns_at_every_width() {
            let (min, max) = parquet_stats(column.as_ref());
            let stored = column.data_type();
            let range =
                StatRange::new(stored, min.as_ref(), max.as_ref()).expect("an integer statistic");

            for (what, predicate) in predicates_over_widths() {
                if !predicate.can_skip(&range) {
                    continue;
                }

                // A type no predicate can compare is refused before the scan
                // reaches row group selection, so only a mask can contradict.
                let Ok(mask) = predicate.evaluate(column.as_ref()) else {
                    continue;
                };
                let kept: Vec<usize> = (0..mask.len()).filter(|&i| mask.value(i)).collect();

                assert!(
                    kept.is_empty(),
                    "{what} on a column stored as {stored} pruned the group, \
                     but the mask keeps rows {kept:?}"
                );
            }
        }
    }

    /// The narrow case the law above generalizes, spelled out so a regression
    /// reads as itself: a `uint8` filter of 255 against the `-1` an `Int8`
    /// column stores it as.
    ///
    /// Covers CT-3 · INV-P16
    #[test]
    fn a_value_the_stored_width_makes_negative_still_reads_its_row_group() {
        let column = Int8Array::from(vec![2i8, 7, -1]);
        let (min, max) = parquet_stats(&column);
        let range = StatRange::new(&DataType::Int8, min.as_ref(), max.as_ref()).unwrap();

        for (what, predicate) in [
            (
                "eq",
                Arc::new(EqPredicate::new(ScalarValue::UInt8(255))) as Arc<dyn ArrayPredicate>,
            ),
            ("in-list", Arc::new(InListPredicate::from_u8s(&[255]))),
        ] {
            assert!(
                !predicate.can_skip(&range),
                "{what}: the group holds the row 255 matches"
            );

            let mask = predicate.evaluate(&column).unwrap();
            assert_eq!(
                mask,
                BooleanArray::from(vec![false, false, true]),
                "{what}: the mask keeps that row"
            );
        }

        // A value the group genuinely lacks still prunes.
        let absent = EqPredicate::new(ScalarValue::UInt8(9));
        assert!(absent.can_skip(&range));
    }

    /// Statistics as the scan hands them over: the parquet physical arrays,
    /// read back against the type the column is stored at.
    fn stats(stored: &DataType, min: &dyn Array, max: &dyn Array) -> StatRange {
        StatRange::new(stored, min, max).expect("a usable statistic")
    }

    #[test]
    fn test_can_skip_row_group() {
        let pred = RowPredicate::new(vec![ColumnPredicate {
            column: "value".to_string(),
            predicate: Arc::new(EqPredicate::new(ScalarValue::UInt64(100))),
        }]);

        // Row group with min=200, max=300 — should skip
        let can_skip = pred.can_skip_row_group(&|col| {
            if col == "value" {
                Some(stats(
                    &DataType::UInt64,
                    &UInt64Array::from(vec![200]),
                    &UInt64Array::from(vec![300]),
                ))
            } else {
                None
            }
        });
        assert!(can_skip);

        // Row group with min=50, max=150 — should NOT skip
        let can_skip = pred.can_skip_row_group(&|col| {
            if col == "value" {
                Some(stats(
                    &DataType::UInt64,
                    &UInt64Array::from(vec![50]),
                    &UInt64Array::from(vec![150]),
                ))
            } else {
                None
            }
        });
        assert!(!can_skip);
    }

    #[test]
    fn test_in_list_can_skip() {
        let pred = InListPredicate::from_strings(&["alice", "bob"]);
        // Row group with min="charlie", max="zoe" — all values < min
        let can_skip = pred.can_skip(&stats(
            &DataType::Utf8,
            &StringArray::from(vec!["charlie"]),
            &StringArray::from(vec!["zoe"]),
        ));
        assert!(can_skip);

        // Row group with min="aaa", max="bzzz" — "bob" is in range
        let can_skip = pred.can_skip(&stats(
            &DataType::Utf8,
            &StringArray::from(vec!["aaa"]),
            &StringArray::from(vec!["bzzz"]),
        ));
        assert!(!can_skip);
    }

    /// Row group pruning must work when parquet stores UInt64 as physical Int64.
    #[test]
    fn test_can_skip_uint64_with_int64_stats() {
        let eq = EqPredicate {
            value: ScalarValue::UInt64(100),
        };
        // Stats arrive as Int64Array (parquet physical type for UInt64)
        let min = Int64Array::from(vec![200i64]);
        let max = Int64Array::from(vec![300i64]);
        assert!(
            eq.can_skip(&stats(&DataType::UInt64, &min, &max)),
            "value=100 outside [200,300] — should skip"
        );

        let min2 = Int64Array::from(vec![50i64]);
        let max2 = Int64Array::from(vec![150i64]);
        assert!(
            !eq.can_skip(&stats(&DataType::UInt64, &min2, &max2)),
            "value=100 inside [50,150] — should NOT skip"
        );
    }

    /// InList row group pruning with Int64 stats (parquet physical for UInt64).
    #[test]
    fn test_in_list_can_skip_uint64_with_int64_stats() {
        let pred = InListPredicate::from_u64s(&[10, 20]);
        // Stats as Int64 — all values in [100, 200], our set {10, 20} is below
        let min = Int64Array::from(vec![100i64]);
        let max = Int64Array::from(vec![200i64]);
        assert!(pred.can_skip(&stats(&DataType::UInt64, &min, &max)));

        // Stats [5, 25] — 10 and 20 are in range
        let min2 = Int64Array::from(vec![5i64]);
        let max2 = Int64Array::from(vec![25i64]);
        assert!(!pred.can_skip(&stats(&DataType::UInt64, &min2, &max2)));
    }

    /// RangeGte pruning with Int64 stats.
    #[test]
    fn test_range_gte_can_skip_with_int64_stats() {
        let pred = RangeGtePredicate::new(ScalarValue::UInt64(100));
        // max=50 as Int64 — all values below 100, should skip
        let max = Int64Array::from(vec![50i64]);
        assert!(pred.can_skip(&stats(
            &DataType::UInt64,
            &Int64Array::from(vec![0i64]),
            &max
        )));

        // max=150 — some values >= 100, should NOT skip
        let max2 = Int64Array::from(vec![150i64]);
        assert!(!pred.can_skip(&stats(
            &DataType::UInt64,
            &Int64Array::from(vec![0i64]),
            &max2
        )));
    }

    /// RangeLte pruning with Int64 stats.
    #[test]
    fn test_range_lte_can_skip_with_int64_stats() {
        let pred = RangeLtePredicate::new(ScalarValue::UInt64(100));
        // min=200 as Int64 — all values above 100, should skip
        let min = Int64Array::from(vec![200i64]);
        assert!(pred.can_skip(&stats(
            &DataType::UInt64,
            &min,
            &Int64Array::from(vec![300i64])
        )));

        // min=50 — some values <= 100, should NOT skip
        let min2 = Int64Array::from(vec![50i64]);
        assert!(!pred.can_skip(&stats(
            &DataType::UInt64,
            &min2,
            &Int64Array::from(vec![300i64])
        )));
    }

    /// InList with u64 values must evaluate correctly against Int64Array (parquet physical type).
    #[test]
    fn test_in_list_u64_on_int64_array() {
        let pred = InListPredicate::from_u64s(&[10, 20, 30]);
        // Array is Int64 (parquet physical type for UInt64)
        let arr = Int64Array::from(vec![10i64, 15, 20, 25, 30]);
        let mask = pred.evaluate(&arr as &dyn Array).unwrap();
        let matches: Vec<bool> = (0..mask.len()).map(|i| mask.value(i)).collect();
        assert_eq!(matches, vec![true, false, true, false, true]);
    }

    /// The kernel must not panic on a batch lacking a predicate column. Whether
    /// such a scan is allowed at all is decided in `scan`, not here.
    #[test]
    fn test_row_predicate_missing_column_no_panic() {
        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(UInt32Array::from(vec![1, 2, 3]))]).unwrap();

        // Predicate on column "b" which doesn't exist in the batch
        let pred = RowPredicate::new(vec![ColumnPredicate {
            column: "b".to_string(),
            predicate: Arc::new(InListPredicate::from_u64s(&[1])),
        }]);

        // Should NOT panic — missing columns treated as all-true
        let mask = pred.evaluate(&batch).unwrap();
        assert_eq!(mask.len(), 3);
        // All true because the only predicate column is missing
        assert_eq!(mask.true_count(), 3);
    }

    /// Missing column in multi-predicate OR path must not panic either.
    #[test]
    fn test_or_row_predicates_missing_column() {
        use arrow::datatypes::{Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["x", "y", "z"]))],
        )
        .unwrap();

        // pred1: column "a" == "x" (exists)
        let pred1 = RowPredicate::new(vec![ColumnPredicate {
            column: "a".to_string(),
            predicate: Arc::new(EqPredicate {
                value: ScalarValue::Utf8("x".to_string()),
            }),
        }]);
        // pred2: column "missing" (doesn't exist)
        let pred2 = RowPredicate::new(vec![ColumnPredicate {
            column: "missing".to_string(),
            predicate: Arc::new(InListPredicate::from_strings(&["whatever"])),
        }]);

        let preds: Vec<&RowPredicate> = vec![&pred1, &pred2];
        let mask = or_row_predicates(&preds, &batch).unwrap();

        // pred2 evaluates to all-true (missing column), OR'd with pred1
        // → all rows match
        assert_eq!(mask.true_count(), 3);
    }

    /// Regression test: or_row_predicates must use or_kleene (not or) so that
    /// `true OR null = true`. When two RowPredicates filter on different columns
    /// and the batch has NULLs in a column not referenced by a predicate, rows
    /// matching predicate A must NOT be dropped because predicate B's column is
    /// NULL for that row.
    #[test]
    fn test_or_row_predicates_null_propagation() {
        // Simulate two filter groups on different columns (like d1 and d8).
        // d1 is UInt8 (nullable), d8 is UInt64 (nullable).
        // Row 0: d1=1,   d8=NULL → matches pred1 (d1==1), pred2 column is NULL
        // Row 1: d1=NULL, d8=100 → matches pred2 (d8==100), pred1 column is NULL
        // Row 2: d1=2,   d8=200 → matches neither
        // Row 3: d1=1,   d8=100 → matches both
        let schema = Arc::new(Schema::new(vec![
            Field::new("d1", DataType::UInt8, true),
            Field::new("d8", DataType::UInt64, true),
        ]));

        let d1_array = UInt8Array::from(vec![Some(1), None, Some(2), Some(1)]);
        let d8_array = UInt64Array::from(vec![None, Some(100), Some(200), Some(100)]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(d1_array), Arc::new(d8_array)]).unwrap();

        let pred1 = RowPredicate::new(vec![ColumnPredicate {
            column: "d1".to_string(),
            predicate: Arc::new(EqPredicate::new(ScalarValue::UInt8(1))),
        }]);
        let pred2 = RowPredicate::new(vec![ColumnPredicate {
            column: "d8".to_string(),
            predicate: Arc::new(EqPredicate::new(ScalarValue::UInt64(100))),
        }]);

        let mask = or_row_predicates(&[&pred1, &pred2], &batch).unwrap();

        // With or_kleene: rows 0, 1, 3 match. Row 2 does not.
        // With plain or: row 0 would be dropped (true OR null = null) — BUG.
        assert_eq!(mask, BooleanArray::from(vec![true, true, false, true]),);
    }

    /// Verify that or_row_predicates with all NULLs in one predicate's column
    /// still selects rows matched by the other predicate.
    /// Key: or_kleene(true, null) = true, or_kleene(false, null) = null.
    /// null in a filter mask is treated as false, which is correct.
    #[test]
    fn test_or_row_predicates_all_nulls_in_other_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, true),
            Field::new("b", DataType::UInt64, true),
        ]));

        // Column b is entirely NULL
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![Some(1), Some(2), Some(1)])),
                Arc::new(UInt64Array::from(vec![None, None, None])),
            ],
        )
        .unwrap();

        let pred_a = RowPredicate::new(vec![col_eq("a", ScalarValue::UInt64(1))]);
        let pred_b = RowPredicate::new(vec![col_eq("b", ScalarValue::UInt64(99))]);

        let mask = or_row_predicates(&[&pred_a, &pred_b], &batch).unwrap();

        // pred_a: [true, false, true], pred_b: [null, null, null]
        // or_kleene: [true, null, true]
        // When used as filter mask, null → false, so rows 0, 2 are selected.
        // Verify rows 0, 2 are true and row 1 is not true:
        assert!(mask.value(0));
        assert!(mask.is_null(1) || !mask.value(1)); // null or false — either way, excluded
        assert!(mask.value(2));

        // Verify via filter_record_batch that rows are correctly selected
        let filtered = arrow::compute::filter_record_batch(&batch, &mask).unwrap();
        assert_eq!(filtered.num_rows(), 2);
    }

    // --- A1: Range filter must not panic on physical-type divergence ---

    #[test]
    fn test_range_gte_uint64_threshold_on_int64_array() {
        // `nonce` is metadata uint64 but physically Int64. Previously `.unwrap()`
        // on a UInt64 downcast panicked the worker; now it must compare correctly.
        let pred = RangeGtePredicate::new(ScalarValue::UInt64(42));
        let array = Int64Array::from(vec![10, 42, 100]);
        let result = pred.evaluate(&array).unwrap();
        assert_eq!(result, BooleanArray::from(vec![false, true, true]));
    }

    #[test]
    fn test_range_lte_uint64_threshold_on_int64_array() {
        let pred = RangeLtePredicate::new(ScalarValue::UInt64(42));
        let array = Int64Array::from(vec![10, 42, 100]);
        let result = pred.evaluate(&array).unwrap();
        assert_eq!(result, BooleanArray::from(vec![true, true, false]));
    }

    #[test]
    fn test_range_gte_threshold_above_i64_max_on_int64_array() {
        // No Int64 value can be >= a threshold above i64::MAX.
        let pred = RangeGtePredicate::new(ScalarValue::UInt64(u64::MAX));
        let array = Int64Array::from(vec![0, i64::MAX]);
        let result = pred.evaluate(&array).unwrap();
        assert_eq!(result, BooleanArray::from(vec![false, false]));
    }

    #[test]
    fn test_range_lte_threshold_above_i64_max_on_int64_array() {
        // Every Int64 value is <= a threshold above i64::MAX.
        let pred = RangeLtePredicate::new(ScalarValue::UInt64(u64::MAX));
        let array = Int64Array::from(vec![0, i64::MAX]);
        let result = pred.evaluate(&array).unwrap();
        assert_eq!(result, BooleanArray::from(vec![true, true]));
    }

    #[test]
    fn test_range_gte_on_native_uint64_array() {
        let pred = RangeGtePredicate::new(ScalarValue::UInt64(42));
        let array = UInt64Array::from(vec![10, 42, 100]);
        assert_eq!(
            pred.evaluate(&array).unwrap(),
            BooleanArray::from(vec![false, true, true])
        );
    }

    // --- A2: ListContainsAny must handle List<UInt16> and LargeList ---

    fn list_u16(rows: &[&[u16]]) -> GenericListArray<i32> {
        let mut b =
            arrow::array::builder::ListBuilder::new(arrow::array::builder::UInt16Builder::new());
        for row in rows {
            for v in *row {
                b.values().append_value(*v);
            }
            b.append(true);
        }
        b.finish()
    }

    #[test]
    fn test_list_contains_any_on_list_uint16() {
        // `instruction_address` is metadata list_uint32 but physically List<UInt16>.
        // Previously every row resolved false → silent 0 results.
        let pred = ListContainsAnyPredicate::new_u32(vec![3, 7]);
        let array = list_u16(&[&[1, 2], &[3, 4], &[5, 7], &[]]);
        let result = pred.evaluate(&array).unwrap();
        assert_eq!(result, BooleanArray::from(vec![false, true, true, false]));
    }

    #[test]
    fn test_list_contains_any_large_list_does_not_panic() {
        // LargeList (i64 offsets) previously hit `.expect()` and panicked.
        let mut b = arrow::array::builder::LargeListBuilder::new(
            arrow::array::builder::UInt32Builder::new(),
        );
        for v in [&[10u32, 20][..], &[30][..]] {
            for x in v {
                b.values().append_value(*x);
            }
            b.append(true);
        }
        let array = b.finish();
        let pred = ListContainsAnyPredicate::new_u32(vec![30]);
        let result = pred.evaluate(&array).unwrap();
        assert_eq!(result, BooleanArray::from(vec![false, true]));
    }

    /// Covers CT-8 · INV-E7
    #[test]
    fn test_list_contains_any_on_a_non_list_is_refused() {
        let pred = ListContainsAnyPredicate::new_u32(vec![1]);
        let array = UInt32Array::from(vec![1, 2, 3]);
        assert!(pred.evaluate(&array).is_err());
    }

    // --- INV-D7 / INV-E7: every stored width answers, every other type refuses ---

    fn every_integer_width(values: &[i64]) -> Vec<Arc<dyn Array>> {
        vec![
            Arc::new(UInt8Array::from_iter_values(
                values.iter().map(|&v| v as u8),
            )),
            Arc::new(UInt16Array::from_iter_values(
                values.iter().map(|&v| v as u16),
            )),
            Arc::new(UInt32Array::from_iter_values(
                values.iter().map(|&v| v as u32),
            )),
            Arc::new(UInt64Array::from_iter_values(
                values.iter().map(|&v| v as u64),
            )),
            Arc::new(Int8Array::from_iter_values(values.iter().map(|&v| v as i8))),
            Arc::new(Int16Array::from_iter_values(
                values.iter().map(|&v| v as i16),
            )),
            Arc::new(Int32Array::from_iter_values(
                values.iter().map(|&v| v as i32),
            )),
            Arc::new(Int64Array::from_iter_values(values.iter().copied())),
        ]
    }

    /// A `uint8` discriminator list against the column at every width the
    /// writer could have narrowed or widened it to — Solana's `d1`.
    ///
    /// Covers CT-8 · INV-D7
    #[test]
    fn an_unsigned_list_reads_every_physical_width() {
        let pred = InListPredicate::from_u8s(&[42, 7]);
        for array in every_integer_width(&[42, 1, 7, 100]) {
            assert_eq!(
                pred.evaluate(array.as_ref()).unwrap(),
                BooleanArray::from(vec![true, false, true, false]),
                "u8 IN-list over {:?}",
                array.data_type()
            );
        }
    }

    /// A value the stored width cannot hold matches nothing at that width.
    ///
    /// Covers CT-8 · INV-D7
    #[test]
    fn a_value_the_stored_width_cannot_hold_matches_nothing() {
        let pred = InListPredicate::from_u64s(&[300, 5]);
        let narrow = UInt8Array::from(vec![44, 5]);
        assert_eq!(
            pred.evaluate(&narrow).unwrap(),
            BooleanArray::from(vec![false, true])
        );
    }

    /// Covers CT-8 · INV-D7
    #[test]
    fn a_scalar_reads_every_physical_width() {
        for value in [
            ScalarValue::UInt8(7),
            ScalarValue::UInt16(7),
            ScalarValue::UInt32(7),
            ScalarValue::UInt64(7),
            ScalarValue::Int16(7),
            ScalarValue::Int64(7),
        ] {
            let pred = EqPredicate::new(value.clone());
            for array in every_integer_width(&[7, 8, 7]) {
                assert_eq!(
                    pred.evaluate(array.as_ref()).unwrap(),
                    BooleanArray::from(vec![true, false, true]),
                    "{value:?} over {:?}",
                    array.data_type()
                );
            }
        }
    }

    /// A flag stored as a number reads the way the reference reads it.
    ///
    /// Covers CT-8 · INV-D7
    #[test]
    fn a_boolean_reads_a_numeric_column() {
        let pred = EqPredicate::new(ScalarValue::Boolean(true));
        for array in every_integer_width(&[1, 0, 1]) {
            assert_eq!(
                pred.evaluate(array.as_ref()).unwrap(),
                BooleanArray::from(vec![true, false, true]),
                "boolean over {:?}",
                array.data_type()
            );
        }
    }

    /// Covers CT-8 · INV-D7
    #[test]
    fn a_range_reads_every_physical_width() {
        let gte = RangeGtePredicate::new(ScalarValue::UInt64(7));
        let lte = RangeLtePredicate::new(ScalarValue::UInt64(7));
        for array in every_integer_width(&[6, 7, 8]) {
            assert_eq!(
                gte.evaluate(array.as_ref()).unwrap(),
                BooleanArray::from(vec![false, true, true]),
                ">= over {:?}",
                array.data_type()
            );
            assert_eq!(
                lte.evaluate(array.as_ref()).unwrap(),
                BooleanArray::from(vec![true, true, false]),
                "<= over {:?}",
                array.data_type()
            );
        }
    }

    /// A threshold the stored width cannot hold decides every row at once —
    /// except a null one, which is null under any comparison (INV-P7). The
    /// shortcut used to answer `true` for the nulls too.
    ///
    /// Covers CT-8 · INV-D7
    /// Covers CT-3 · INV-P7
    #[test]
    fn a_null_row_stays_null_under_an_out_of_width_threshold() {
        let array = UInt32Array::from(vec![Some(1), None, Some(4_000_000_000)]);
        let above = ScalarValue::UInt64(u64::from(u32::MAX) + 1);

        let kept = |mask: BooleanArray| -> Vec<usize> {
            (0..mask.len())
                .filter(|&i| mask.is_valid(i) && mask.value(i))
                .collect()
        };

        let gte = RangeGtePredicate::new(above.clone())
            .evaluate(&array)
            .unwrap();
        assert!(
            gte.is_null(1),
            "a null row is null, not above the threshold"
        );
        assert_eq!(kept(gte), Vec::<usize>::new());

        let lte = RangeLtePredicate::new(above).evaluate(&array).unwrap();
        assert!(
            lte.is_null(1),
            "a null row is null, not below the threshold"
        );
        assert_eq!(kept(lte), vec![0, 2]);
    }

    /// A column stored at the other signedness is read at the declared one by
    /// equality — `uint32` over `Int32` sees `-1` as `4294967295` — and a range
    /// has to read the same bits the same way, or `= v` and `>= v` disagree on
    /// one row. The group's statistics say nothing under that reading, so no
    /// group is skipped.
    ///
    /// Covers CT-8 · INV-D7
    #[test]
    fn a_range_reads_the_bits_an_equality_reads() {
        // 3_000_000_000 as the bits of an i32.
        let three_billion = 3_000_000_000u32 as i32;
        let array = Int32Array::from(vec![three_billion, 5, -1]);

        let equal = InListPredicate::from_u64s(&[3_000_000_000]);
        assert_eq!(
            equal.evaluate(&array).unwrap(),
            BooleanArray::from(vec![true, false, false])
        );

        let gte = RangeGtePredicate::new(ScalarValue::UInt64(3_000_000_000));
        assert_eq!(
            gte.evaluate(&array).unwrap(),
            BooleanArray::from(vec![true, false, true]),
            "-1 is 4294967295 at the declared type"
        );

        let lte = RangeLtePredicate::new(ScalarValue::UInt64(3_000_000_000));
        assert_eq!(
            lte.evaluate(&array).unwrap(),
            BooleanArray::from(vec![true, true, false])
        );

        let min = Int32Array::from(vec![-1]);
        let max = Int32Array::from(vec![5]);
        let stats = StatRange::new(&DataType::Int32, &min, &max).unwrap();
        assert!(
            !gte.can_skip(&stats),
            "[-1, 5] as stored is not an interval at the declared type"
        );
        assert!(!lte.can_skip(&stats));
    }

    /// Text at every type a writer stores it as, including a dictionary keyed
    /// by a width other than `Int32`. Bytes are not text: a string list over a
    /// `Binary` column is refused, not answered by comparing the raw bytes to
    /// the literal (which a `0x…` literal never equals).
    ///
    /// Covers CT-8 · INV-D7
    /// Covers CT-8 · INV-E7
    #[test]
    fn a_string_list_reads_every_text_type() {
        let pred = InListPredicate::from_strings(&["alice", "bob"]);
        let expected = BooleanArray::from(vec![true, false, true, false]);
        let rows = vec!["alice", "charlie", "bob", "dave"];

        let bytes = BinaryArray::from_iter_values(rows.iter().map(|s| s.as_bytes()));
        assert!(
            pred.evaluate(&bytes).is_err(),
            "a string list over bytes is refused"
        );

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(StringArray::from(rows.clone())),
            Arc::new(LargeStringArray::from(rows.clone())),
            Arc::new(StringViewArray::from(rows.clone())),
            Arc::new(DictionaryArray::<Int8Type>::from_iter(rows.iter().copied())),
            Arc::new(DictionaryArray::<UInt16Type>::from_iter(
                rows.iter().copied(),
            )),
        ];
        for array in arrays {
            assert_eq!(
                pred.evaluate(array.as_ref()).unwrap(),
                expected,
                "string IN-list over {:?}",
                array.data_type()
            );
        }
    }

    /// Covers CT-8 · INV-E7
    #[test]
    fn a_dictionary_keeps_its_null_keys_out_of_the_answer() {
        let pred = InListPredicate::from_strings(&["alice"]);
        let array: DictionaryArray<Int16Type> =
            vec![Some("alice"), None, Some("bob")].into_iter().collect();
        assert_eq!(
            pred.evaluate(&array).unwrap(),
            BooleanArray::from(vec![true, false, false])
        );
    }

    /// Covers CT-8 · INV-E7
    #[test]
    fn a_bloom_reads_every_bytes_type() {
        // A filter with every bit set contains everything.
        let filled = vec![0xffu8; 8];
        let pred = BloomFilterPredicate::new(vec![b"x".to_vec()], 8, 7);

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(vec![filled.clone()].into_iter()).unwrap(),
            ),
            Arc::new(BinaryArray::from_vec(vec![filled.as_slice()])),
            Arc::new(LargeBinaryArray::from_vec(vec![filled.as_slice()])),
        ];
        for array in arrays {
            assert_eq!(
                pred.evaluate(array.as_ref()).unwrap(),
                BooleanArray::from(vec![true]),
                "bloom over {:?}",
                array.data_type()
            );
        }
        assert!(pred.evaluate(&StringArray::from(vec!["x"])).is_err());
    }

    /// Every predicate kind refuses, rather than matches nothing on, a column
    /// it cannot compare its values against.
    ///
    /// Covers CT-8 · INV-E7
    #[test]
    fn an_uncomparable_column_is_refused_by_every_predicate_kind() {
        let text = StringArray::from(vec!["7"]);
        let number = UInt8Array::from(vec![7]);
        let flag = BooleanArray::from(vec![true]);

        let refusals: Vec<(&str, Box<dyn ArrayPredicate>, &dyn Array)> = vec![
            (
                "integer list on text",
                Box::new(InListPredicate::from_u8s(&[7])),
                &text,
            ),
            (
                "string list on a number",
                Box::new(InListPredicate::from_strings(&["7"])),
                &number,
            ),
            (
                "string list on a flag",
                Box::new(InListPredicate::from_strings(&["true"])),
                &flag,
            ),
            (
                "integer scalar on text",
                Box::new(EqPredicate::new(ScalarValue::UInt64(7))),
                &text,
            ),
            (
                "boolean on text",
                Box::new(EqPredicate::new(ScalarValue::Boolean(true))),
                &text,
            ),
            (
                "range on text",
                Box::new(RangeGtePredicate::new(ScalarValue::UInt64(7))),
                &text,
            ),
            (
                "range on a flag",
                Box::new(RangeLtePredicate::new(ScalarValue::UInt64(7))),
                &flag,
            ),
            (
                "text range on a number",
                Box::new(RangeGtePredicate::new(ScalarValue::Utf8("0x1".into()))),
                &number,
            ),
            (
                "list-contains on text",
                Box::new(ListContainsAnyPredicate::new_string(vec!["a".into()])),
                &text,
            ),
        ];

        for (case, pred, array) in refusals {
            let err = pred
                .evaluate(array)
                .expect_err(&format!("{case} must be refused"));
            assert_eq!(err.stored, array.data_type().clone(), "{case}");
            assert_eq!(
                check_stored_type(pred.as_ref(), array.data_type()),
                Err(err),
                "{case}"
            );
        }
    }

    /// The probe answers for the types evaluation answers for, and no others.
    ///
    /// Covers CT-8 · INV-E7
    #[test]
    fn the_stored_type_check_agrees_with_evaluation() {
        let pred = InListPredicate::from_u8s(&[7]);
        assert!(check_stored_type(&pred, &DataType::Int64).is_ok());
        assert!(check_stored_type(
            &pred,
            &DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Int16))
        )
        .is_ok());
        assert!(check_stored_type(&pred, &DataType::Utf8).is_err());
        assert!(check_stored_type(&pred, &DataType::Boolean).is_err());
    }
}
