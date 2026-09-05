//! The physical widths a declared integer column may be stored at.
//!
//! An archive writer narrows integers per chunk: a `uint64` block number arrives
//! in 32 bits, a `uint32` item index in 16, and a chunk written by an older
//! generation of the writer differs from one written by today's
//! ([INV-D7](../spec/07-invariants.md)). Every place that reads such a column
//! used to carry its own downcast chain, and the chains disagreed — four widths
//! in one, six in another, eight in a third. A width one of them had forgotten
//! did not raise anything; it returned no rows.
//!
//! So there is one list, here, and the sites resolve through it.

use arrow::array::*;
use arrow::datatypes::DataType;

/// An integer column, resolved once so a read costs a match rather than a
/// downcast chain.
pub(crate) enum IntColumn<'a> {
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

/// Whether a declared integer column may be stored at this physical type.
pub(crate) fn is_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
    )
}

macro_rules! int_column {
    ($($variant:ident($array:ty, $unsigned:ty)),+ $(,)?) => {
        impl<'a> IntColumn<'a> {
            /// The reader for a column's physical width, or `None` when the
            /// column is not an integer at all.
            pub(crate) fn resolve(col: &'a dyn Array) -> Option<Self> {
                $(if let Some(a) = col.as_any().downcast_ref::<$array>() {
                    return Some(Self::$variant(a));
                })+

                None
            }

            /// The value at `row`, exactly, sign and all.
            ///
            /// Use this wherever two values are compared for order or equality
            /// across columns that may be stored at different widths.
            #[inline]
            pub(crate) fn value(&self, row: usize) -> i128 {
                match self {
                    $(Self::$variant(a) => a.value(row) as i128,)+
                }
            }

            /// The value at `row` as a block number: reinterpreted as unsigned.
            ///
            /// A writer storing block numbers in `Int32` carries anything above
            /// 2³¹ as a negative value, and reading that signed would place it
            /// before every block instead of after. The rule has to be the same
            /// everywhere or two readers of one column disagree about which
            /// block a row belongs to.
            #[inline]
            pub(crate) fn block_number(&self, row: usize) -> u64 {
                match self {
                    $(Self::$variant(a) => (a.value(row) as $unsigned) as u64,)+
                }
            }

            /// The value at `row` as eight bytes of composite join key.
            ///
            /// Widened rather than reinterpreted, so that equal values encode
            /// equally whatever width each side of the join is stored at — which
            /// is the whole point, since the key is compared as bytes and a
            /// mismatch matches nothing and says nothing (INV-D7).
            #[inline]
            pub(crate) fn join_key(&self, row: usize) -> u64 {
                self.value(row) as i64 as u64
            }

            #[inline]
            pub(crate) fn is_null(&self, row: usize) -> bool {
                match self {
                    $(Self::$variant(a) => a.is_null(row),)+
                }
            }

            pub(crate) fn len(&self) -> usize {
                match self {
                    $(Self::$variant(a) => a.len(),)+
                }
            }
        }
    };
}

int_column!(
    UInt64(UInt64Array, u64),
    UInt32(UInt32Array, u32),
    UInt16(UInt16Array, u16),
    UInt8(UInt8Array, u8),
    Int64(Int64Array, u64),
    Int32(Int32Array, u32),
    Int16(Int16Array, u16),
    Int8(Int8Array, u8),
);

/// The block-number column of a batch: resolved once, and checked once.
///
/// A block number is what every layer places a row by — which row group can
/// still own it, what it weighs, which block it is emitted under. So a value no
/// reader can resolve is not something the reader that happened to notice gets
/// to decide about alone. Read through [`IntColumn`] a null returns the slot's
/// placeholder and the row quietly becomes block 0's; a column stored at no
/// integer width returns nothing at all. Both are corrupt input, and INV-E1 asks
/// for an error rather than an answer built on them.
///
/// The scan resolves this on every batch it hands out, so the readers behind it
/// work on a column already known to be whole.
pub struct BlockNumbers<'a>(IntColumn<'a>);

impl<'a> BlockNumbers<'a> {
    pub fn resolve(column: &'a dyn Array, name: &str) -> anyhow::Result<Self> {
        let Some(reader) = IntColumn::resolve(column) else {
            crate::engine_bail!(
                crate::error::ErrorKind::MalformedChunkData,
                "block-number column '{}' is stored as {}, which is not an integer",
                name,
                column.data_type()
            );
        };

        if column.null_count() > 0 {
            crate::engine_bail!(
                crate::error::ErrorKind::MalformedChunkData,
                "block-number column '{}' leaves {} of {} rows without a block",
                name,
                column.null_count(),
                column.len()
            );
        }

        Ok(Self(reader))
    }

    #[inline]
    pub fn at(&self, row: usize) -> u64 {
        self.0.block_number(row)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }
}

/// An owned reader, for the sites that keep one alive past the batch borrow.
///
/// Arrow arrays are `Arc`-backed, so the clone is a refcount bump.
pub(crate) enum OwnedIntColumn {
    UInt8(UInt8Array),
    UInt16(UInt16Array),
    UInt32(UInt32Array),
    UInt64(UInt64Array),
    Int8(Int8Array),
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(Int64Array),
}

macro_rules! owned_int_column {
    ($($variant:ident($array:ty)),+ $(,)?) => {
        impl OwnedIntColumn {
            pub(crate) fn resolve(col: &dyn Array) -> Option<Self> {
                $(if let Some(a) = col.as_any().downcast_ref::<$array>() {
                    return Some(Self::$variant(a.clone()));
                })+

                None
            }

            #[inline]
            pub(crate) fn value(&self, row: usize) -> i128 {
                match self {
                    $(Self::$variant(a) => a.value(row) as i128,)+
                }
            }

            pub(crate) fn len(&self) -> usize {
                match self {
                    $(Self::$variant(a) => a.len(),)+
                }
            }
        }
    };
}

owned_int_column!(
    UInt64(UInt64Array),
    UInt32(UInt32Array),
    UInt16(UInt16Array),
    UInt8(UInt8Array),
    Int64(Int64Array),
    Int32(Int32Array),
    Int16(Int16Array),
    Int8(Int8Array),
);
