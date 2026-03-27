use arrow::array::*;
use arrow::compute::kernels::boolean::{and, or_kleene};
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::*;
use std::collections::HashSet;
use std::sync::Arc;

/// A predicate that evaluates against an Arrow array, producing a boolean mask.
pub trait ArrayPredicate: Send + Sync {
    /// Evaluate this predicate against the given array, returning a boolean mask.
    fn evaluate(&self, array: &dyn Array) -> BooleanArray;

    /// Check if this predicate can definitely be skipped for a row group
    /// given the min/max statistics. Returns true if NO rows can match.
    fn can_skip(&self, min: &dyn Array, max: &dyn Array) -> bool;
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
/// Pre-builds HashSets at construction time to avoid per-evaluate allocation.
pub struct InListPredicate {
    values: Arc<dyn Array>,
    /// Pre-built string set (for Utf8 columns).
    string_set: Option<HashSet<String>>,
    /// Pre-built u64 set (for UInt64 columns).
    u64_set: Option<HashSet<u64>>,
    /// Pre-built i64 set (for Int64 columns).
    i64_set: Option<HashSet<i64>>,
    /// Pre-built u32 set.
    u32_set: Option<HashSet<u32>>,
    /// Pre-built u16 set.
    u16_set: Option<HashSet<u16>>,
    /// Pre-built u8 set.
    u8_set: Option<HashSet<u8>>,
    /// Pre-built fixed-size binary set (for FixedSizeBinary columns).
    fixed_binary_set: Option<HashSet<Vec<u8>>>,
}

/// Bloom filter predicate: check if any of the given values might be in the bloom filter.
pub struct BloomFilterPredicate {
    needles: Vec<Vec<u8>>,
    #[allow(dead_code)]
    num_bytes: usize,
    num_hashes: usize,
}

/// AND combinator: all sub-predicates must match.
pub struct AndPredicate {
    predicates: Vec<Arc<dyn ArrayPredicate>>,
}

/// OR combinator: at least one sub-predicate must match.
pub struct OrPredicate {
    predicates: Vec<Arc<dyn ArrayPredicate>>,
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

// ---------------------------------------------------------------------------
// EqPredicate
// ---------------------------------------------------------------------------

impl EqPredicate {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl ArrayPredicate for EqPredicate {
    fn evaluate(&self, array: &dyn Array) -> BooleanArray {
        match &self.value {
            ScalarValue::Boolean(v) => {
                let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                eq(arr, &BooleanArray::new_scalar(*v)).unwrap()
            }
            ScalarValue::UInt8(v) => {
                let Some(arr) = array.as_any().downcast_ref::<UInt8Array>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                eq(arr, &UInt8Array::new_scalar(*v)).unwrap()
            }
            ScalarValue::UInt16(v) => {
                let Some(arr) = array.as_any().downcast_ref::<UInt16Array>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                eq(arr, &UInt16Array::new_scalar(*v)).unwrap()
            }
            ScalarValue::UInt32(v) => {
                // Try exact type, then parquet physical types (Int32, UInt16, Int16)
                if let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() {
                    eq(arr, &UInt32Array::new_scalar(*v)).unwrap()
                } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                    eq(arr, &Int32Array::new_scalar(*v as i32)).unwrap()
                } else if let Some(arr) = array.as_any().downcast_ref::<UInt16Array>() {
                    if let Ok(v16) = u16::try_from(*v) {
                        eq(arr, &UInt16Array::new_scalar(v16)).unwrap()
                    } else {
                        BooleanArray::from(vec![false; array.len()])
                    }
                } else {
                    BooleanArray::from(vec![false; array.len()])
                }
            }
            ScalarValue::UInt64(v) => {
                // Try exact type, then parquet physical types (Int64, UInt32, Int32)
                if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
                    eq(arr, &UInt64Array::new_scalar(*v)).unwrap()
                } else if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                    eq(arr, &Int64Array::new_scalar(*v as i64)).unwrap()
                } else if let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() {
                    if let Ok(v32) = u32::try_from(*v) {
                        eq(arr, &UInt32Array::new_scalar(v32)).unwrap()
                    } else {
                        BooleanArray::from(vec![false; array.len()])
                    }
                } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                    if let Ok(v32) = u32::try_from(*v) {
                        eq(arr, &Int32Array::new_scalar(v32 as i32)).unwrap()
                    } else {
                        BooleanArray::from(vec![false; array.len()])
                    }
                } else {
                    BooleanArray::from(vec![false; array.len()])
                }
            }
            ScalarValue::Int16(v) => {
                let Some(arr) = array.as_any().downcast_ref::<Int16Array>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                eq(arr, &Int16Array::new_scalar(*v)).unwrap()
            }
            ScalarValue::Int64(v) => {
                let Some(arr) = array.as_any().downcast_ref::<Int64Array>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                eq(arr, &Int64Array::new_scalar(*v)).unwrap()
            }
            ScalarValue::Utf8(v) => {
                let Some(arr) = array.as_any().downcast_ref::<StringArray>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                eq(arr, &StringArray::new_scalar(v)).unwrap()
            }
        }
    }

    fn can_skip(&self, min: &dyn Array, max: &dyn Array) -> bool {
        // If value < min or value > max, then no rows can match
        match &self.value {
            ScalarValue::UInt64(v) => {
                if let (Some(min_arr), Some(max_arr)) = (
                    min.as_any().downcast_ref::<UInt64Array>(),
                    max.as_any().downcast_ref::<UInt64Array>(),
                ) {
                    if min_arr.len() > 0 && max_arr.len() > 0 {
                        return *v < min_arr.value(0) || *v > max_arr.value(0);
                    }
                }
                false
            }
            ScalarValue::Utf8(v) => {
                if let (Some(min_arr), Some(max_arr)) = (
                    min.as_any().downcast_ref::<StringArray>(),
                    max.as_any().downcast_ref::<StringArray>(),
                ) {
                    if min_arr.len() > 0 && max_arr.len() > 0 {
                        return v.as_str() < min_arr.value(0) || v.as_str() > max_arr.value(0);
                    }
                }
                false
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
        let u64_set = values
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|arr| arr.values().iter().copied().collect());
        let i64_set = values
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| arr.values().iter().copied().collect());
        let u32_set = values
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|arr| arr.values().iter().copied().collect());
        let u16_set = values
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|arr| arr.values().iter().copied().collect());
        let u8_set = values
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|arr| arr.values().iter().copied().collect());
        let fixed_binary_set = values
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .map(|arr| (0..arr.len()).map(|i| arr.value(i).to_vec()).collect());
        Self {
            values,
            string_set,
            u64_set,
            i64_set,
            u32_set,
            u16_set,
            u8_set,
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

    /// Evaluate against a dictionary-encoded column.
    /// Checks the dictionary values once, builds a key→match lookup, then maps through keys.
    fn evaluate_dictionary(&self, array: &dyn Array, value_type: &DataType) -> BooleanArray {
        match value_type {
            DataType::Utf8 => self.eval_dict_typed::<Int32Type, StringArray>(array),
            _ => {
                // Fallback: cast to values and evaluate normally
                let decoded = arrow::compute::cast(array, value_type).unwrap();
                self.evaluate(decoded.as_ref())
            }
        }
    }

    fn eval_dict_typed<K, V>(&self, array: &dyn Array) -> BooleanArray
    where
        K: ArrowDictionaryKeyType,
        K::Native: TryInto<usize>,
        V: 'static,
    {
        let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
        let values = dict_array.values();

        // Evaluate predicate against dictionary values (small array, e.g. 200 entries)
        let dict_mask = self.evaluate(values.as_ref());

        // Map the mask through keys to produce the final per-row mask
        let keys = dict_array.keys();
        let len = keys.len();
        let mut builder = BooleanBufferBuilder::new(len);
        for i in 0..len {
            if keys.is_null(i) {
                builder.append(false);
            } else {
                let key: usize = keys.value(i).try_into().unwrap_or(0);
                builder.append(dict_mask.value(key));
            }
        }
        BooleanArray::new(builder.finish(), dict_array.nulls().cloned())
    }
}

impl ArrayPredicate for InListPredicate {
    fn evaluate(&self, array: &dyn Array) -> BooleanArray {
        // Handle dictionary-encoded columns: filter against the dictionary,
        // then map through keys (much faster than per-row lookup).
        if let DataType::Dictionary(_, value_type) = array.data_type() {
            return self.evaluate_dictionary(array, value_type.as_ref());
        }

        match array.data_type() {
            DataType::Utf8 => {
                let Some(arr) = array.as_any().downcast_ref::<StringArray>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                let Some(set) = self.string_set.as_ref() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                in_list_string_fast(arr, set)
            }
            DataType::UInt8 => {
                let Some(arr) = array.as_any().downcast_ref::<UInt8Array>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                let Some(set) = self.u8_set.as_ref() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                in_list_primitive_fast(arr, set)
            }
            DataType::UInt16 => {
                let Some(arr) = array.as_any().downcast_ref::<UInt16Array>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                let Some(set) = self.u16_set.as_ref() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                in_list_primitive_fast(arr, set)
            }
            DataType::UInt32 => {
                let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                let Some(set) = self.u32_set.as_ref() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                in_list_primitive_fast(arr, set)
            }
            DataType::UInt64 => {
                let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                let Some(set) = self.u64_set.as_ref() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                in_list_primitive_fast(arr, set)
            }
            DataType::Int64 => {
                let Some(arr) = array.as_any().downcast_ref::<Int64Array>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                let Some(set) = self.i64_set.as_ref() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                in_list_primitive_fast(arr, set)
            }
            DataType::FixedSizeBinary(_) => {
                let Some(arr) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                let Some(set) = self.fixed_binary_set.as_ref() else {
                    return BooleanArray::from(vec![false; array.len()]);
                };
                in_list_fixed_binary(arr, set)
            }
            _ => BooleanArray::from(vec![false; array.len()]),
        }
    }

    fn can_skip(&self, min: &dyn Array, max: &dyn Array) -> bool {
        // For string IN-list: skip if all values < min or all values > max
        if let (Some(min_arr), Some(max_arr)) = (
            min.as_any().downcast_ref::<StringArray>(),
            max.as_any().downcast_ref::<StringArray>(),
        ) {
            if min_arr.len() > 0 && max_arr.len() > 0 {
                let rg_min = min_arr.value(0);
                let rg_max = max_arr.value(0);
                if let Some(list) = self.values.as_any().downcast_ref::<StringArray>() {
                    return (0..list.len()).all(|i| {
                        let v = list.value(i);
                        v < rg_min || v > rg_max
                    });
                }
            }
        }
        // For UInt64 IN-list
        if let (Some(min_arr), Some(max_arr)) = (
            min.as_any().downcast_ref::<UInt64Array>(),
            max.as_any().downcast_ref::<UInt64Array>(),
        ) {
            if min_arr.len() > 0 && max_arr.len() > 0 {
                let rg_min = min_arr.value(0);
                let rg_max = max_arr.value(0);
                if let Some(list) = self.values.as_any().downcast_ref::<UInt64Array>() {
                    return (0..list.len()).all(|i| {
                        let v = list.value(i);
                        v < rg_min || v > rg_max
                    });
                }
            }
        }
        false
    }
}

fn in_list_string_fast(array: &StringArray, set: &HashSet<String>) -> BooleanArray {
    let len = array.len();
    let mut builder = BooleanBufferBuilder::new(len);
    for i in 0..len {
        if array.is_null(i) {
            builder.append(false);
        } else {
            builder.append(set.contains(array.value(i)));
        }
    }
    BooleanArray::new(builder.finish(), array.nulls().cloned())
}

fn in_list_primitive_fast<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    set: &HashSet<T::Native>,
) -> BooleanArray
where
    T::Native: std::hash::Hash + Eq,
{
    let len = array.len();
    let mut builder = BooleanBufferBuilder::new(len);
    for i in 0..len {
        if array.is_null(i) {
            builder.append(false);
        } else {
            builder.append(set.contains(&array.value(i)));
        }
    }
    BooleanArray::new(builder.finish(), array.nulls().cloned())
}

fn in_list_fixed_binary(array: &FixedSizeBinaryArray, set: &HashSet<Vec<u8>>) -> BooleanArray {
    let bools: Vec<bool> = (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                false
            } else {
                set.contains(array.value(i))
            }
        })
        .collect();
    BooleanArray::from(bools)
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

/// Check if a bloom filter (byte array) might contain a value.
/// Uses the same XXHash3-based hashing scheme as the legacy sqd_bloom_filter.
/// Each hash is: xxh3(value_via_Hash_trait, seed=i) % num_bits
fn bloom_contains(filter: &[u8], value: &[u8], num_hashes: usize) -> bool {
    let num_bits = filter.len() * 8;
    if num_bits == 0 {
        return false;
    }

    for i in 0..num_hashes {
        // Match Rust's Hash trait for &str: writes bytes then 0xFF separator byte
        let mut hasher = xxhash_rust::xxh3::Xxh3Builder::new()
            .with_seed(i as u64)
            .build();
        std::hash::Hasher::write(&mut hasher, value);
        std::hash::Hasher::write_u8(&mut hasher, 0xff);
        let bit = (std::hash::Hasher::finish(&hasher) as usize) % num_bits;
        let byte_idx = bit / 8;
        let bit_idx = bit % 8;
        if filter[byte_idx] & (1 << bit_idx) == 0 {
            return false;
        }
    }
    true
}

impl ArrayPredicate for BloomFilterPredicate {
    fn evaluate(&self, array: &dyn Array) -> BooleanArray {
        let arr = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("BloomFilterPredicate requires FixedSizeBinaryArray");

        let bools: Vec<bool> = (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    false
                } else {
                    self.check_bloom(arr.value(i))
                }
            })
            .collect();
        BooleanArray::from(bools)
    }

    fn can_skip(&self, _min: &dyn Array, _max: &dyn Array) -> bool {
        // Cannot use row group stats to skip bloom filter columns
        false
    }
}

// ---------------------------------------------------------------------------
// And / Or combinators
// ---------------------------------------------------------------------------

impl AndPredicate {
    pub fn new(predicates: Vec<Arc<dyn ArrayPredicate>>) -> Self {
        Self { predicates }
    }
}

impl ArrayPredicate for AndPredicate {
    fn evaluate(&self, array: &dyn Array) -> BooleanArray {
        let mut result: Option<BooleanArray> = None;
        for pred in &self.predicates {
            let mask = pred.evaluate(array);
            result = Some(match result {
                None => mask,
                Some(prev) => and(&prev, &mask).unwrap(),
            });
        }
        result.unwrap_or_else(|| BooleanArray::from(vec![true; array.len()]))
    }

    fn can_skip(&self, min: &dyn Array, max: &dyn Array) -> bool {
        // If ANY sub-predicate says skip, we can skip (AND short-circuits)
        self.predicates.iter().any(|p| p.can_skip(min, max))
    }
}

impl OrPredicate {
    pub fn new(predicates: Vec<Arc<dyn ArrayPredicate>>) -> Self {
        Self { predicates }
    }
}

impl ArrayPredicate for OrPredicate {
    fn evaluate(&self, array: &dyn Array) -> BooleanArray {
        let mut result: Option<BooleanArray> = None;
        for pred in &self.predicates {
            let mask = pred.evaluate(array);
            result = Some(match result {
                None => mask,
                Some(prev) => or_kleene(&prev, &mask).unwrap(),
            });
        }
        result.unwrap_or_else(|| BooleanArray::from(vec![false; array.len()]))
    }

    fn can_skip(&self, min: &dyn Array, max: &dyn Array) -> bool {
        // Only skip if ALL sub-predicates say skip (OR requires all to fail)
        self.predicates.iter().all(|p| p.can_skip(min, max))
    }
}

// ---------------------------------------------------------------------------
// Range predicates
// ---------------------------------------------------------------------------

impl RangeGtePredicate {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl ArrayPredicate for RangeGtePredicate {
    fn evaluate(&self, array: &dyn Array) -> BooleanArray {
        use arrow::compute::kernels::cmp::gt_eq;
        match &self.value {
            ScalarValue::UInt64(v) => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                gt_eq(&arr, &UInt64Array::new_scalar(*v)).unwrap()
            }
            _ => BooleanArray::from(vec![true; array.len()]),
        }
    }

    fn can_skip(&self, _min: &dyn Array, max: &dyn Array) -> bool {
        // Skip if max < value (all values in group are below threshold)
        match &self.value {
            ScalarValue::UInt64(v) => {
                if let Some(max_arr) = max.as_any().downcast_ref::<UInt64Array>() {
                    if max_arr.len() > 0 {
                        return max_arr.value(0) < *v;
                    }
                }
                false
            }
            _ => false,
        }
    }
}

impl RangeLtePredicate {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl ArrayPredicate for RangeLtePredicate {
    fn evaluate(&self, array: &dyn Array) -> BooleanArray {
        use arrow::compute::kernels::cmp::lt_eq;
        match &self.value {
            ScalarValue::UInt64(v) => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                lt_eq(&arr, &UInt64Array::new_scalar(*v)).unwrap()
            }
            _ => BooleanArray::from(vec![true; array.len()]),
        }
    }

    fn can_skip(&self, min: &dyn Array, _max: &dyn Array) -> bool {
        // Skip if min > value (all values in group are above threshold)
        match &self.value {
            ScalarValue::UInt64(v) => {
                if let Some(min_arr) = min.as_any().downcast_ref::<UInt64Array>() {
                    if min_arr.len() > 0 {
                        return min_arr.value(0) > *v;
                    }
                }
                false
            }
            _ => false,
        }
    }
}

// ---------------------------------------------------------------------------
// ListContainsAnyPredicate
// ---------------------------------------------------------------------------

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
}

impl ArrayPredicate for ListContainsAnyPredicate {
    fn evaluate(&self, array: &dyn Array) -> BooleanArray {
        let list_array = array
            .as_any()
            .downcast_ref::<GenericListArray<i32>>()
            .expect("ListContainsAny requires a List column");

        let mut results = Vec::with_capacity(list_array.len());
        for i in 0..list_array.len() {
            if list_array.is_null(i) {
                results.push(false);
                continue;
            }
            let item = list_array.value(i);
            let matches = if let Some(u32_set) = &self.u32_set {
                if let Some(vals) = item.as_any().downcast_ref::<UInt32Array>() {
                    (0..vals.len()).any(|j| u32_set.contains(&vals.value(j)))
                } else {
                    false
                }
            } else if let Some(string_set) = &self.string_set {
                if let Some(vals) = item.as_any().downcast_ref::<StringArray>() {
                    (0..vals.len()).any(|j| string_set.contains(vals.value(j)))
                } else {
                    false
                }
            } else {
                false
            };
            results.push(matches);
        }
        BooleanArray::from(results)
    }

    fn can_skip(&self, _min: &dyn Array, _max: &dyn Array) -> bool {
        // No row group pruning for list-contains predicates
        false
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
    pub fn evaluate(&self, batch: &RecordBatch) -> BooleanArray {
        let mut result: Option<BooleanArray> = None;

        for col_pred in &self.columns {
            let col = batch
                .column_by_name(&col_pred.column)
                .unwrap_or_else(|| panic!("column '{}' not found in batch", col_pred.column));
            let mask = col_pred.predicate.evaluate(col.as_ref());
            result = Some(match result {
                None => mask,
                Some(prev) => and(&prev, &mask).unwrap(),
            });
        }

        result.unwrap_or_else(|| BooleanArray::from(vec![true; batch.num_rows()]))
    }

    /// Check if the entire row group can be skipped using column statistics.
    /// Returns true if no rows can match.
    pub fn can_skip_row_group(
        &self,
        stats_fn: &dyn Fn(&str) -> Option<(Arc<dyn Array>, Arc<dyn Array>)>,
    ) -> bool {
        // ALL column predicates must pass for a row to match.
        // If ANY column predicate says it can skip (no matches possible), skip the group.
        for col_pred in &self.columns {
            if let Some((min, max)) = stats_fn(&col_pred.column) {
                if col_pred.predicate.can_skip(min.as_ref(), max.as_ref()) {
                    return true;
                }
            }
        }
        false
    }
}

/// Combine multiple row predicates with OR (multiple request items).
pub fn or_row_predicates(predicates: &[&RowPredicate], batch: &RecordBatch) -> BooleanArray {
    let mut result: Option<BooleanArray> = None;
    for pred in predicates {
        let mask = pred.evaluate(batch);
        result = Some(match result {
            None => mask,
            Some(prev) => or_kleene(&prev, &mask).unwrap(),
        });
    }
    result.unwrap_or_else(|| BooleanArray::from(vec![false; batch.num_rows()]))
}

/// Check if ALL row predicates (ORed) say skip for a row group.
pub fn can_skip_row_group_or(
    predicates: &[&RowPredicate],
    stats_fn: &dyn Fn(&str) -> Option<(Arc<dyn Array>, Arc<dyn Array>)>,
) -> bool {
    // OR: skip only if ALL predicates say skip
    predicates.iter().all(|p| p.can_skip_row_group(stats_fn))
}

/// Filter a RecordBatch to only include rows matching any of the given predicates (OR'd).
/// Returns None if the filtered batch would be empty.
pub fn evaluate_predicates_on_batch(
    batch: &RecordBatch,
    predicates: &[RowPredicate],
) -> Option<RecordBatch> {
    if batch.num_rows() == 0 {
        return None;
    }
    let pred_refs: Vec<&RowPredicate> = predicates.iter().collect();
    let mask = or_row_predicates(&pred_refs, batch);
    let filtered = arrow::compute::filter_record_batch(batch, &mask).ok()?;
    if filtered.num_rows() > 0 {
        Some(filtered)
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

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
        let result = pred.evaluate(&array);
        assert_eq!(result, BooleanArray::from(vec![true, false, true, false]));
    }

    #[test]
    fn test_eq_predicate_uint64() {
        let pred = EqPredicate::new(ScalarValue::UInt64(42));
        let array = UInt64Array::from(vec![10, 42, 42, 100]);
        let result = pred.evaluate(&array);
        assert_eq!(result, BooleanArray::from(vec![false, true, true, false]));
    }

    #[test]
    fn test_in_list_predicate_strings() {
        let pred = InListPredicate::from_strings(&["alice", "bob"]);
        let array = StringArray::from(vec!["alice", "charlie", "bob", "dave"]);
        let result = pred.evaluate(&array);
        assert_eq!(result, BooleanArray::from(vec![true, false, true, false]));
    }

    #[test]
    fn test_in_list_predicate_u64() {
        let pred = InListPredicate::from_u64s(&[10, 30]);
        let array = UInt64Array::from(vec![10, 20, 30, 40]);
        let result = pred.evaluate(&array);
        assert_eq!(result, BooleanArray::from(vec![true, false, true, false]));
    }

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

        let mask = pred.evaluate(&batch);
        // Only row 2 (0-indexed) has a=3 AND b="x"
        assert_eq!(mask, BooleanArray::from(vec![false, false, true, false]));
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
                Some((
                    Arc::new(UInt64Array::from(vec![200])) as Arc<dyn Array>,
                    Arc::new(UInt64Array::from(vec![300])) as Arc<dyn Array>,
                ))
            } else {
                None
            }
        });
        assert!(can_skip);

        // Row group with min=50, max=150 — should NOT skip
        let can_skip = pred.can_skip_row_group(&|col| {
            if col == "value" {
                Some((
                    Arc::new(UInt64Array::from(vec![50])) as Arc<dyn Array>,
                    Arc::new(UInt64Array::from(vec![150])) as Arc<dyn Array>,
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
        let can_skip = pred.can_skip(
            &StringArray::from(vec!["charlie"]) as &dyn Array,
            &StringArray::from(vec!["zoe"]) as &dyn Array,
        );
        assert!(can_skip);

        // Row group with min="aaa", max="bzzz" — "bob" is in range
        let can_skip = pred.can_skip(
            &StringArray::from(vec!["aaa"]) as &dyn Array,
            &StringArray::from(vec!["bzzz"]) as &dyn Array,
        );
        assert!(!can_skip);
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

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(d1_array), Arc::new(d8_array)],
        )
        .unwrap();

        let pred1 = RowPredicate::new(vec![ColumnPredicate {
            column: "d1".to_string(),
            predicate: Arc::new(EqPredicate::new(ScalarValue::UInt8(1))),
        }]);
        let pred2 = RowPredicate::new(vec![ColumnPredicate {
            column: "d8".to_string(),
            predicate: Arc::new(EqPredicate::new(ScalarValue::UInt64(100))),
        }]);

        let mask = or_row_predicates(&[&pred1, &pred2], &batch);

        // With or_kleene: rows 0, 1, 3 match. Row 2 does not.
        // With plain or: row 0 would be dropped (true OR null = null) — BUG.
        assert_eq!(
            mask,
            BooleanArray::from(vec![true, true, false, true]),
        );
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

        let mask = or_row_predicates(&[&pred_a, &pred_b], &batch);

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
}
