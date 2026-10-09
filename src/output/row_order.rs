//! The order of a table's rows within a block: by the table's item order keys
//! and its address, then by file order.
//!
//! Comparing two rows column by column resolves each column's type on every
//! comparison. So a source's integer keys, with at most a trailing path of
//! integers, are prepared once at their stored types, in the same order the
//! comparator gives them; deep paths are written out as bytes, so that a long
//! shared prefix compares as one run of bytes. The comparator stays for any
//! other keys, and to order rows of different sources against each other.

use crate::integers::OwnedIntColumn;
use crate::metadata::TableDescription;
use crate::text::OwnedStringColumn;
use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::ops::Range;

/// One source's rows, ready to order: the key columns of each batch resolved
/// once, and the keys two rows compare by, where some form fits them.
pub(crate) struct RowOrder {
    columns: Vec<String>,
    resolved: Vec<Vec<Option<TypedSortColumn>>>,
    keys: Option<SortKeys>,
}

impl RowOrder {
    pub(crate) fn new(batches: &[RecordBatch], table: &TableDescription) -> Self {
        let columns = build_full_sort_columns(table);
        let resolved = resolve_sort_columns(batches, &columns);
        let keys = row_sort_keys(batches, &columns);

        Self {
            columns,
            resolved,
            keys,
        }
    }

    /// Whether the rows have keys to order by, beside their file order.
    pub(crate) fn is_keyed(&self) -> bool {
        !self.columns.is_empty()
    }

    /// Sort `(batch, row)` references by key, then by file order.
    pub(crate) fn sort(&self, rows: &mut [(usize, usize)]) {
        sort_rows_by_order_keys_indexed(rows, &self.resolved, self.keys.as_ref());
    }

    /// Row `a` of these rows against row `b` of `other`'s, another source of
    /// the same table, by key alone.
    pub(crate) fn compare(
        &self,
        (batch_a, row_a): (usize, usize),
        other: &RowOrder,
        (batch_b, row_b): (usize, usize),
    ) -> Ordering {
        let columns = self.resolved[batch_a].iter().zip(&other.resolved[batch_b]);
        columns
            .filter_map(|columns| match columns {
                (Some(a), Some(b)) => Some(a.cmp_rows(row_a, b, row_b)),
                _ => None,
            })
            .find(|order| order.is_ne())
            .unwrap_or(Ordering::Equal)
    }
}

/// Whether a key column stored as `data_type` can order rows.
pub(crate) fn orders_rows(data_type: &DataType) -> bool {
    let probe = new_empty_array(data_type);
    TypedSortColumn::resolve(probe.as_ref()).is_some()
}

/// Every batch's sort keys, in a form two rows compare by without resolving
/// their columns' types.
enum SortKeys {
    /// Integer columns and at most a trailing path of integers, read at their
    /// stored types, null slots as stored, as the typed columns read them.
    Typed(Vec<TypedKeys>),
    /// The same, written out as bytes for deep paths: each value at its
    /// stored width, big-endian, sign bit flipped.
    Packed(Vec<PackedKeys>),
}

struct PackedKeys {
    ends: Vec<usize>,
    bytes: Vec<u8>,
}

impl PackedKeys {
    #[inline]
    fn key(&self, row: usize) -> &[u8] {
        let start = if row == 0 { 0 } else { self.ends[row - 1] };
        &self.bytes[start..self.ends[row]]
    }
}

struct TypedKeys {
    fixed: Vec<Vec<i128>>,
    path: Option<(OffsetBuffer<i32>, PathValues)>,
}

/// A path's elements at their stored type, one variant per width of the
/// shared integer list, so that two paths of one type compare as two slices.
macro_rules! path_values {
    ($($variant:ident($array:ty, $native:ty, $unsigned:ty, $bits:literal, $signed:literal)),+ $(,)?) => {
        enum PathValues {
            $($variant(arrow::buffer::ScalarBuffer<$native>),)+
        }

        impl PathValues {
            fn resolve(values: &dyn Array) -> Option<Self> {
                $(if let Some(a) = values.as_any().downcast_ref::<$array>() {
                    return Some(Self::$variant(a.values().clone()));
                })+

                None
            }

            /// The stored width in bytes.
            fn width(&self) -> usize {
                match self {
                    $(Self::$variant(_) => $bits / 8,)+
                }
            }

            /// Write elements `range` as order-preserving bytes into `out`:
            /// big-endian, with a signed value's sign bit flipped.
            fn pack(&self, range: Range<usize>, out: &mut [u8]) {
                match self {
                    $(Self::$variant(values) => {
                        let bytes = out.chunks_exact_mut($bits / 8);
                        for (chunk, &value) in bytes.zip(&values[range]) {
                            let sign: $unsigned = if $signed { 1 << ($bits - 1) } else { 0 };
                            chunk.copy_from_slice(&((value as $unsigned) ^ sign).to_be_bytes());
                        }
                    })+
                }
            }

            fn value(&self, i: usize) -> i128 {
                match self {
                    $(Self::$variant(values) => values[i] as i128,)+
                }
            }

            /// Two paths compared element by element, then by length.
            fn compare(&self, a: Range<usize>, other: &Self, b: Range<usize>) -> Ordering {
                match (self, other) {
                    $((Self::$variant(x), Self::$variant(y)) => x[a].cmp(&y[b]),)+
                    _ => {
                        let pairs = a.clone().zip(b.clone());
                        pairs
                            .map(|(i, j)| self.value(i).cmp(&other.value(j)))
                            .find(|order| order.is_ne())
                            .unwrap_or(a.len().cmp(&b.len()))
                    }
                }
            }
        }
    };
}

crate::integers::for_each_int_type!(path_values);

impl SortKeys {
    #[inline]
    fn compare(&self, (ba, ra): (usize, usize), (bb, rb): (usize, usize)) -> Ordering {
        match self {
            Self::Typed(keys) => {
                let (a, b) = (&keys[ba], &keys[bb]);
                for (x, y) in a.fixed.iter().zip(&b.fixed) {
                    let order = x[ra].cmp(&y[rb]);
                    if order.is_ne() {
                        return order;
                    }
                }
                match (&a.path, &b.path) {
                    (Some((oa, va)), Some((ob, vb))) => va.compare(span(oa, ra), vb, span(ob, rb)),
                    _ => Ordering::Equal,
                }
            }
            Self::Packed(keys) => keys[ba].key(ra).cmp(keys[bb].key(rb)),
        }
    }
}

/// A key column resolved for comparisons. Arrow arrays are `Arc`-backed, so
/// holding one is a refcount.
enum TypedSortColumn {
    Int(OwnedIntColumn),
    Text(OwnedStringColumn),
    /// A list's offsets and, when they are integers, its elements. Elements
    /// that are not integers order as equal, which leaves the file-order
    /// tiebreaker to decide; there is no order to invent for them.
    List(OffsetBuffer<i32>, Option<PathValues>),
}

impl TypedSortColumn {
    fn resolve(col: &dyn Array) -> Option<Self> {
        if let Some(ints) = OwnedIntColumn::resolve(col) {
            return Some(Self::Int(ints));
        }
        if let Some(text) = OwnedStringColumn::resolve(col) {
            return Some(Self::Text(text));
        }
        if let Some(list) = col.as_any().downcast_ref::<GenericListArray<i32>>() {
            let elements = PathValues::resolve(list.values().as_ref());
            return Some(Self::List(list.offsets().clone(), elements));
        }

        None
    }

    /// Order two rows by this column.
    ///
    /// Integers compare by value, so two sides stored at different widths — or
    /// one signed and one not — order by value rather than by bit pattern.
    /// Text compares as text whichever type carries it. A path compares element
    /// by element, then by length; a null one as the elements its slot holds.
    fn cmp_rows(&self, row_a: usize, other: &TypedSortColumn, row_b: usize) -> Ordering {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a.value(row_a).cmp(&b.value(row_b)),
            (Self::Text(a), Self::Text(b)) => a.value(row_a).cmp(&b.value(row_b)),
            (Self::List(oa, Some(va)), Self::List(ob, Some(vb))) => {
                va.compare(span(oa, row_a), vb, span(ob, row_b))
            }
            (Self::List(..), Self::List(..)) => Ordering::Equal,
            _ => {
                debug_assert!(false, "TypedSortColumn type mismatch in cmp_rows");
                Ordering::Equal
            }
        }
    }
}

/// The elements of a list's row `row`.
fn span(offsets: &OffsetBuffer<i32>, row: usize) -> Range<usize> {
    offsets[row] as usize..offsets[row + 1] as usize
}

/// Build the full sort column list for a table: item_order_keys + address_column.
/// These match the legacy engine's primary key (minus block_number) for output ordering.
pub(crate) fn build_full_sort_columns(table_desc: &TableDescription) -> Vec<String> {
    let bn_col = table_desc.block_number_column.as_str();

    let mut used: HashSet<&str> = HashSet::new();
    used.insert(bn_col);

    let mut cols: Vec<String> = Vec::new();

    // Primary sort: item_order_keys
    for key in &table_desc.item_order_keys {
        if used.insert(key.as_str()) {
            cols.push(key.clone());
        }
    }

    // Secondary: address column
    if let Some(ac) = table_desc.address_column.as_deref() {
        if used.insert(ac) {
            cols.push(ac.to_string());
        }
    }

    cols
}

/// Pre-resolve typed sort columns for each batch (done once, reused per block).
fn resolve_sort_columns(
    batches: &[RecordBatch],
    sort_columns: &[String],
) -> Vec<Vec<Option<TypedSortColumn>>> {
    batches
        .iter()
        .map(|b| {
            sort_columns
                .iter()
                .map(|k| {
                    b.schema()
                        .index_of(k)
                        .ok()
                        .and_then(|i| TypedSortColumn::resolve(b.column(i).as_ref()))
                })
                .collect()
        })
        .collect()
}

/// The sort keys of every batch, when its key columns are integers with at most
/// a trailing path of integers, each column of one type across the batches.
fn row_sort_keys(batches: &[RecordBatch], sort_columns: &[String]) -> Option<SortKeys> {
    let first = batches.first()?;
    if sort_columns.is_empty() {
        return None;
    }
    let types: Vec<DataType> = sort_columns
        .iter()
        .map(|name| Some(first.column_by_name(name)?.data_type().clone()))
        .collect::<Option<_>>()?;
    let columns: Vec<Vec<ArrayRef>> = batches
        .iter()
        .map(|batch| {
            sort_columns
                .iter()
                .map(|name| batch.column_by_name(name).cloned())
                .collect::<Option<_>>()
        })
        .collect::<Option<_>>()?;
    let same_types = columns
        .iter()
        .all(|columns| columns.iter().zip(&types).all(|(c, t)| c.data_type() == t));
    if !same_types {
        return None;
    }

    let typed = columns
        .iter()
        .map(|columns| typed_keys(columns))
        .collect::<Option<Vec<_>>>()?;
    let deep = typed.iter().any(|keys| {
        keys.path.as_ref().is_some_and(|(offsets, _)| {
            let rows = offsets.len() - 1;
            let elements = (offsets[rows] - offsets[0]) as usize;
            elements >= PACKED_DEPTH * rows.max(1)
        })
    });

    Some(match deep {
        true => SortKeys::Packed(typed.iter().map(packed_keys).collect()),
        false => SortKeys::Typed(typed),
    })
}

/// Paths at least this deep on average are compared as bytes.
const PACKED_DEPTH: usize = 8;

/// Typed keys written out as bytes.
fn packed_keys(keys: &TypedKeys) -> PackedKeys {
    let rows = keys.fixed.first().map_or_else(
        || {
            keys.path
                .as_ref()
                .map_or(0, |(offsets, _)| offsets.len() - 1)
        },
        Vec::len,
    );
    // Fixed columns are compared at i128, sign included.
    let fixed_bytes = keys.fixed.len() * 16;
    let mut ends = Vec::with_capacity(rows);
    let mut total = 0;
    for row in 0..rows {
        total += fixed_bytes;
        if let Some((offsets, values)) = &keys.path {
            total += (offsets[row + 1] - offsets[row]) as usize * values.width();
        }
        ends.push(total);
    }

    let mut bytes = vec![0u8; total];
    let mut start = 0;
    for row in 0..rows {
        let mut at = start;
        for column in &keys.fixed {
            let ordered = (column[row] as u128) ^ (1 << 127);
            bytes[at..at + 16].copy_from_slice(&ordered.to_be_bytes());
            at += 16;
        }
        if let Some((offsets, values)) = &keys.path {
            let path = offsets[row] as usize..offsets[row + 1] as usize;
            values.pack(path, &mut bytes[at..ends[row]]);
        }
        start = ends[row];
    }

    PackedKeys { ends, bytes }
}

/// One batch's keys when its columns are integers with at most a trailing
/// list of integers.
fn typed_keys(columns: &[arrow::array::ArrayRef]) -> Option<TypedKeys> {
    let (last, leading) = columns.split_last()?;
    let list = last.as_any().downcast_ref::<GenericListArray<i32>>();
    let fixed = if list.is_some() { leading } else { columns };

    let fixed = fixed
        .iter()
        .map(|column| {
            let values = OwnedIntColumn::resolve(column.as_ref())?;
            Some((0..column.len()).map(|row| values.value(row)).collect())
        })
        .collect::<Option<_>>()?;
    let path = match list {
        Some(list) => Some((
            list.offsets().clone(),
            PathValues::resolve(list.values().as_ref())?,
        )),
        None => None,
    };

    Some(TypedKeys { fixed, path })
}

/// Sort (batch_idx, row) references by pre-resolved typed sort columns.
/// Uses (batch_idx, row_idx) as final tiebreaker to preserve parquet file order.
fn sort_rows_by_order_keys_indexed(
    rows: &mut [(usize, usize)],
    sort_col_resolved: &[Vec<Option<TypedSortColumn>>],
    sort_rows: Option<&SortKeys>,
) {
    if rows.is_empty() {
        return;
    }
    if let Some(keys) = sort_rows {
        rows.sort_unstable_by(|&a, &b| keys.compare(a, b).then(a.cmp(&b)));
        return;
    }
    let has_sort_cols = sort_col_resolved.first().is_some_and(|v| !v.is_empty());
    if !has_sort_cols {
        // Even with no sort columns, sort by (batch, row) for deterministic order
        rows.sort_by_key(|&(bi, ri)| (bi, ri));
        return;
    }

    // The file-order tiebreaker makes the order total, so stability buys nothing.
    rows.sort_unstable_by(|&(bi_a, row_a), &(bi_b, row_b)| {
        let res_a = &sort_col_resolved[bi_a];
        let res_b = &sort_col_resolved[bi_b];
        for (col_a, col_b) in res_a.iter().zip(res_b.iter()) {
            if let (Some(ca), Some(cb)) = (col_a, col_b) {
                let ord = ca.cmp_rows(row_a, cb, row_b);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
        }
        // Final tiebreaker: parquet file order (batch index, then row index)
        (bi_a, row_a).cmp(&(bi_b, row_b))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sort_column(array: impl Array + 'static) -> TypedSortColumn {
        TypedSortColumn::resolve(&array).expect("the array is a sortable type")
    }

    /// Covers CT-6 · INV-O5
    #[test]
    fn test_typed_sort_column_same_type_comparison() {
        let col_a = sort_column(UInt32Array::from(vec![10, 20, 30]));
        let col_b = sort_column(UInt32Array::from(vec![15, 25, 5]));
        assert_eq!(col_a.cmp_rows(0, &col_b, 0), std::cmp::Ordering::Less); // 10 < 15
        assert_eq!(col_a.cmp_rows(1, &col_b, 2), std::cmp::Ordering::Greater); // 20 > 5
        assert_eq!(col_a.cmp_rows(0, &col_b, 2), std::cmp::Ordering::Greater); // 10 > 5
    }

    /// Two sources of one table can be stored at different widths, and the
    /// items still have to come back in item-key order. This used to assert the
    /// opposite — that a width mismatch was a programming error worth a
    /// `debug_assert` — which is what let a chunk written in eight bits emit its
    /// items in file order.
    ///
    /// Covers CT-6 · INV-D7
    /// Covers CT-6 · INV-O5
    #[test]
    fn a_narrower_column_orders_against_a_wider_one_by_value() {
        let wide = sort_column(UInt64Array::from(vec![10, 300]));

        for narrow in [
            sort_column(UInt8Array::from(vec![9, 11])),
            sort_column(Int8Array::from(vec![9, 11])),
            sort_column(UInt16Array::from(vec![9, 11])),
            sort_column(Int32Array::from(vec![9, 11])),
        ] {
            assert_eq!(narrow.cmp_rows(0, &wide, 0), std::cmp::Ordering::Less); // 9 < 10
            assert_eq!(narrow.cmp_rows(1, &wide, 0), std::cmp::Ordering::Greater); // 11 > 10
            assert_eq!(narrow.cmp_rows(1, &wide, 1), std::cmp::Ordering::Less); // 11 < 300
        }
    }

    /// A text sort key stored wide still orders. It used to resolve to nothing,
    /// and a `None` sort column compares every pair as equal, so the items came
    /// out in file order without a word.
    ///
    /// Covers CT-6 · INV-D7
    /// Covers CT-6 · INV-O5
    #[test]
    fn a_text_sort_key_orders_at_every_text_type() {
        let rows = vec!["b", "a", "c"];
        let plain = sort_column(StringArray::from(rows.clone()));

        for wide in [
            sort_column(LargeStringArray::from(rows.clone())),
            sort_column(StringViewArray::from(rows.clone())),
        ] {
            assert_eq!(wide.cmp_rows(1, &plain, 0), std::cmp::Ordering::Less); // a < b
            assert_eq!(wide.cmp_rows(2, &plain, 0), std::cmp::Ordering::Greater); // c > b
            assert_eq!(wide.cmp_rows(0, &wide, 0), std::cmp::Ordering::Equal);
        }
    }

    /// A width mismatch is no longer a mismatch; a *kind* mismatch still is, and
    /// the `debug_assert` is what says so rather than an invented ordering.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "TypedSortColumn type mismatch")]
    fn test_typed_sort_column_type_mismatch_panics_in_debug() {
        let number = sort_column(UInt32Array::from(vec![10]));
        let text = sort_column(StringArray::from(vec!["10"]));
        number.cmp_rows(0, &text, 0);
    }

    mod order {
        use super::super::*;
        use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray, UInt64Array};
        use arrow::datatypes::{Field, Int32Type, Schema, UInt16Type, UInt32Type};
        use proptest::prelude::*;
        use std::sync::Arc;

        /// A path at element width `width`: signed and some below zero, or
        /// unsigned at 32 or 16 bits. With `nulls`, a value of 1 is stored as
        /// a null, in a slot that holds zero. With `deep`, every path starts
        /// with the same eight elements, deep enough to be compared as bytes.
        fn batch(
            rows: &[(i32, u64, String, Vec<u16>)],
            width: u8,
            nulls: bool,
            deep: bool,
        ) -> RecordBatch {
            let kept = |v: i64| (!nulls || v != 1).then_some(v);
            let rows: Vec<_> = rows
                .iter()
                .map(|r| {
                    let mut path = if deep { vec![258u16; 8] } else { Vec::new() };
                    path.extend(&r.3);
                    (r.0, r.1, r.2.clone(), path)
                })
                .collect();
            let signed: ArrayRef = Arc::new(Int32Array::from_iter(
                rows.iter().map(|r| kept(r.0 as i64).map(|v| v as i32)),
            ));
            let unsigned: ArrayRef =
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.1)));
            let text: ArrayRef = Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.2.clone()),
            ));
            let elements = |r: &(i32, u64, String, Vec<u16>)| {
                r.3.iter().map(|&e| kept(e as i64)).collect::<Vec<_>>()
            };
            let path: ArrayRef = match width {
                2 => Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                    rows.iter().map(|r| {
                        let path = elements(r).into_iter().map(|e| e.map(|v| v as i32 - 257));
                        Some(path.collect::<Vec<_>>())
                    }),
                )),
                1 => Arc::new(ListArray::from_iter_primitive::<UInt32Type, _, _>(
                    rows.iter().map(|r| {
                        let path = elements(r).into_iter().map(|e| e.map(|v| v as u32));
                        Some(path.collect::<Vec<_>>())
                    }),
                )),
                _ => Arc::new(ListArray::from_iter_primitive::<UInt16Type, _, _>(
                    rows.iter().map(|r| {
                        let path = elements(r).into_iter().map(|e| e.map(|v| v as u16));
                        Some(path.collect::<Vec<_>>())
                    }),
                )),
            };
            let schema = Schema::new(vec![
                Field::new("signed", signed.data_type().clone(), true),
                Field::new("unsigned", unsigned.data_type().clone(), false),
                Field::new("text", text.data_type().clone(), false),
                Field::new("path", path.data_type().clone(), true),
            ]);
            RecordBatch::try_new(Arc::new(schema), vec![signed, unsigned, text, path]).unwrap()
        }

        /// What a key column holds at a row, read without the engine: a null
        /// slot as the value it holds.
        #[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
        enum Key {
            Int(i128),
            Text(String),
            Path(Vec<i128>),
        }

        fn key(column: &ArrayRef, row: usize) -> Key {
            use arrow::array::AsArray;
            use arrow::datatypes::UInt64Type;

            let int = |array: &dyn Array, row: usize| -> i128 {
                match array.data_type() {
                    DataType::Int32 => array.as_primitive::<Int32Type>().values()[row].into(),
                    DataType::UInt32 => array.as_primitive::<UInt32Type>().values()[row].into(),
                    DataType::UInt16 => array.as_primitive::<UInt16Type>().values()[row].into(),
                    DataType::UInt64 => array.as_primitive::<UInt64Type>().values()[row].into(),
                    other => panic!("{other}"),
                }
            };
            match column.data_type() {
                DataType::Utf8 => Key::Text(column.as_string::<i32>().value(row).to_string()),
                DataType::List(_) => {
                    let list = column.as_list::<i32>();
                    let offsets = list.value_offsets();
                    let span = offsets[row] as usize..offsets[row + 1] as usize;
                    Key::Path(span.map(|i| int(list.values().as_ref(), i)).collect())
                }
                _ => Key::Int(int(column.as_ref(), row)),
            }
        }

        fn row() -> impl Strategy<Value = (i32, u64, String, Vec<u16>)> {
            (
                prop_oneof![
                    Just(i32::MIN),
                    Just(-1),
                    Just(0),
                    Just(1),
                    Just(i32::MAX),
                    -3i32..3
                ],
                prop_oneof![Just(0u64), Just(u64::MAX), Just(1 << 63), 0u64..3],
                "[a-c]{0,3}",
                // Both bytes of an element vary, so their order shows.
                prop::collection::vec(
                    prop_oneof![Just(0u16), Just(u16::MAX), 0u16..3, 254u16..259],
                    0..4,
                ),
            )
        }

        proptest! {
            /// Rows order by their key values, column by column, then by file
            /// order, whatever width each batch stores its path at; and the
            /// prepared keys, in the form the rule picks, order them the same.
            #[test]
            fn rows_order_by_their_key_values(
                parts in prop::collection::vec(
                    (prop::collection::vec(row(), 0..12), 0u8..3),
                    1..4,
                ),
                columns in prop::sample::subsequence(vec!["signed", "unsigned", "text", "path"], 1..=4),
                order in Just(()).prop_perturb(|_, mut rng| {
                    let mut all = vec!["signed", "unsigned", "text", "path"];
                    for i in (1..all.len()).rev() { all.swap(i, rng.random_range(0..=i)); }
                    all
                }),
                nulls in any::<bool>(),
                deep in any::<bool>(),
            ) {
                let batches: Vec<RecordBatch> = parts
                    .iter()
                    .map(|(rows, width)| batch(rows, *width, nulls, deep))
                    .collect();
                let columns: Vec<String> = order
                    .iter()
                    .filter(|name| columns.contains(name))
                    .map(|name| name.to_string())
                    .collect();
                let keys = row_sort_keys(&batches, &columns);
                let found = match &keys {
                    Some(SortKeys::Typed(_)) => "typed",
                    Some(SortKeys::Packed(_)) => "packed",
                    None => "none",
                };
                let keyed = RowOrder {
                    columns: columns.clone(),
                    resolved: resolve_sort_columns(&batches, &columns),
                    keys,
                };
                let compared = RowOrder {
                    columns: columns.clone(),
                    resolved: resolve_sort_columns(&batches, &columns),
                    keys: None,
                };

                let all: Vec<(usize, usize)> = batches
                    .iter()
                    .enumerate()
                    .flat_map(|(b, batch)| (0..batch.num_rows()).map(move |r| (b, r)))
                    .collect();
                let key_values = |(b, r): (usize, usize)| -> Vec<Key> {
                    let batch = &batches[b];
                    columns.iter().map(|name| key(batch.column_by_name(name).unwrap(), r)).collect()
                };
                let mut expected = all.clone();
                expected.sort_by(|&a, &b| key_values(a).cmp(&key_values(b)).then(a.cmp(&b)));
                let mut by_comparator = all.clone();
                compared.sort(&mut by_comparator);
                let mut by_keys = all;
                keyed.sort(&mut by_keys);
                prop_assert_eq!(&by_comparator, &expected);
                prop_assert_eq!(&by_keys, &expected);

                // Integers with a trailing path of one type are read at their
                // types, deep paths as bytes; the rest by the comparator alone.
                let has = |name: &str| columns.iter().any(|c| c == name);
                let path_last = columns
                    .iter()
                    .position(|c| c == "path")
                    .is_none_or(|at| at + 1 == columns.len());
                let widths: std::collections::HashSet<u8> =
                    parts.iter().map(|(_, width)| *width).collect();
                let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                let expected_form = if (has("path") && widths.len() > 1) || has("text") || !path_last {
                    "none"
                } else if has("path") && deep && rows > 0 {
                    "packed"
                } else {
                    "typed"
                };
                prop_assert_eq!(found, expected_form);
            }
        }

        #[test]
        fn a_null_path_sorts_as_the_empty_path_its_slot_holds() {
            let path: ArrayRef =
                Arc::new(ListArray::from_iter_primitive::<UInt16Type, _, _>(vec![
                    Some(vec![Some(1)]),
                    None,
                    Some(vec![]),
                    Some(vec![Some(0)]),
                ]));
            let schema = Schema::new(vec![Field::new("path", path.data_type().clone(), true)]);
            let batches = [RecordBatch::try_new(Arc::new(schema), vec![path]).unwrap()];
            let columns = vec!["path".to_string()];
            let keys = row_sort_keys(&batches, &columns);
            assert!(matches!(keys, Some(SortKeys::Typed(_))));

            for keys in [keys, None] {
                let order = RowOrder {
                    columns: columns.clone(),
                    resolved: resolve_sort_columns(&batches, &columns),
                    keys,
                };
                let mut rows: Vec<(usize, usize)> = (0..4).map(|row| (0, row)).collect();
                order.sort(&mut rows);

                assert_eq!(rows, vec![(0, 1), (0, 2), (0, 3), (0, 0)]);
            }
        }
    }
}
