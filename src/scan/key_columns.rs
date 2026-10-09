//! Key columns read at the type a chunk stores them, so that a key compares by
//! value whatever width each side wrote it at.

use crate::integers::IntColumn;
use crate::text::StringColumn;
use arrow::array::{Array, GenericListArray, RecordBatch};
use arrow::buffer::NullBuffer;

/// A typed column extractor that avoids per-row type dispatch.
pub(super) enum TypedKeyColumn<'a> {
    Int(IntColumn<'a>),
    Str(StringColumn<'a>),
    /// A path of item indices. `None` elements are not integers, so no row of
    /// the list has a key.
    List(&'a GenericListArray<i32>, Option<IntColumn<'a>>),
}

impl<'a> TypedKeyColumn<'a> {
    pub(super) fn resolve(col: &'a dyn Array) -> Option<Self> {
        if let Some(ints) = IntColumn::resolve(col) {
            return Some(Self::Int(ints));
        }
        if let Some(text) = StringColumn::resolve(col) {
            return Some(Self::Str(text));
        }
        if let Some(a) = col.as_any().downcast_ref::<GenericListArray<i32>>() {
            return Some(Self::List(a, IntColumn::resolve(a.values().as_ref())));
        }

        None
    }

    #[inline(always)]
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Int(a) => a.is_null(row),
            Self::Str(a) => a.value(row).is_none(),
            Self::List(a, _) => a.is_null(row),
        }
    }

    /// Append this column's value at `row`, or report that there is none. A null
    /// list serializes byte-for-byte like an empty one, so without this a row
    /// that says "no call" joins to the call at the empty address.
    #[inline(always)]
    pub(super) fn append_to(&self, buf: &mut Vec<u8>, row: usize) -> bool {
        if self.is_null(row) {
            return false;
        }

        match self {
            Self::Int(a) => buf.extend_from_slice(&a.join_key(row).to_le_bytes()),
            Self::Str(a) => {
                let v = a.value(row).expect("checked above");
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            Self::List(a, elements) => {
                // A list key is a path of item indices, so its elements are
                // integers at whatever width the writer chose, and each is
                // written the fixed eight bytes the scalar arm writes.
                let Some(elements) = elements else {
                    return false;
                };
                let offsets = a.value_offsets();
                let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);

                buf.extend_from_slice(&((end - start) as u32).to_le_bytes());
                for i in start..end {
                    buf.extend_from_slice(&elements.join_key(i).to_le_bytes());
                }
            }
        }

        true
    }

    /// True for integer key columns (eligible for the packed-u128 fast path).
    #[inline(always)]
    pub(super) fn is_integer(&self) -> bool {
        matches!(self, Self::Int(_))
    }
}

pub(super) fn typed_key_columns<'a>(
    batch: &'a RecordBatch,
    names: &[impl AsRef<str>],
) -> Vec<Option<TypedKeyColumn<'a>>> {
    names
        .iter()
        .map(|name| {
            batch
                .column_by_name(name.as_ref())
                .and_then(|c| TypedKeyColumn::resolve(c.as_ref()))
        })
        .collect()
}

/// The join keys of a batch's two integer key columns, and the rows where
/// either is null; `None` when they are not two integer columns.
pub(super) fn pair_keys<S: AsRef<str>>(
    batch: &RecordBatch,
    columns: &[S],
) -> Option<(Vec<u64>, Vec<u64>, Option<NullBuffer>)> {
    let [first, second] = columns else {
        return None;
    };
    let column = |name: &S| IntColumn::resolve(batch.column_by_name(name.as_ref())?.as_ref());
    let (first, second) = (column(first)?, column(second)?);
    let nulls = NullBuffer::union(first.nulls(), second.nulls());

    Some((first.join_keys(), second.join_keys(), nulls))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, ListArray};
    use std::sync::Arc;

    /// A list key reads its elements through the list's offsets, so it must
    /// write what the row's own slice holds: at every width, on a sliced list,
    /// for null and empty lists and a null element. Elements that are not
    /// integers give no row a key.
    #[test]
    fn a_list_key_writes_what_its_row_slice_holds() {
        use arrow::datatypes::*;

        fn lists<T: ArrowPrimitiveType>() -> ArrayRef {
            let value = |v: usize| T::Native::from_usize(v);
            Arc::new(ListArray::from_iter_primitive::<T, _, _>(vec![
                Some(vec![value(9)]),
                Some(vec![value(0), value(3)]),
                None,
                Some(vec![]),
                Some(vec![value(1), None, value(2)]),
                Some(vec![value(4)]),
            ]))
        }

        let oracle = |list: &GenericListArray<i32>, row: usize| -> Option<Vec<u8>> {
            if list.is_null(row) {
                return None;
            }
            let slice = list.value(row);
            let elements = IntColumn::resolve(slice.as_ref())?;
            let mut buf = (slice.len() as u32).to_le_bytes().to_vec();
            for i in 0..elements.len() {
                buf.extend_from_slice(&elements.join_key(i).to_le_bytes());
            }
            Some(buf)
        };

        let columns = [
            lists::<UInt8Type>(),
            lists::<UInt16Type>(),
            lists::<UInt32Type>(),
            lists::<UInt64Type>(),
            lists::<Int8Type>(),
            lists::<Int16Type>(),
            lists::<Int32Type>(),
            lists::<Int64Type>(),
            lists::<Float64Type>(),
        ];
        for column in &columns {
            for column in [column.clone(), column.slice(1, 5)] {
                let list = column.as_any().downcast_ref::<ListArray>().unwrap();
                let key = TypedKeyColumn::resolve(column.as_ref()).unwrap();
                for row in 0..list.len() {
                    let mut buf = Vec::new();
                    let written = key.append_to(&mut buf, row).then_some(buf);
                    assert_eq!(
                        written,
                        oracle(list, row),
                        "row {row} of a {} list",
                        column.data_type()
                    );
                }
            }
        }
    }
}
