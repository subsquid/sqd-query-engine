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
//! So there is one list, here — [`for_each_int_type`] — and everything that
//! enumerates integer types is generated from it.

use arrow::array::*;
use arrow::datatypes::{ArrowPrimitiveType, DataType};

/// The one list: every integer type a column may be stored at, as
/// `Variant(ArrayType, native, unsigned native, bits, signed)`, handed to a
/// callback macro.
macro_rules! for_each_int_type {
    ($callback:ident) => {
        $callback! {
            UInt64(UInt64Array, u64, u64, 64, false),
            UInt32(UInt32Array, u32, u32, 32, false),
            UInt16(UInt16Array, u16, u16, 16, false),
            UInt8(UInt8Array, u8, u8, 8, false),
            Int64(Int64Array, i64, u64, 64, true),
            Int32(Int32Array, i32, u32, 32, true),
            Int16(Int16Array, i16, u16, 16, true),
            Int8(Int8Array, i8, u8, 8, true),
        }
    };
}

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
    width_of(data_type).is_some()
}

/// A computation over an integer array of any width, monomorphized per width
/// by [`IntColumn::visit`] rather than dispatched per row.
pub(crate) trait IntVisitor {
    type Out;

    fn visit<T>(self, array: &PrimitiveArray<T>) -> Self::Out
    where
        T: ArrowPrimitiveType,
        T::Native: Into<i128> + TryFrom<i128>;
}

macro_rules! int_column {
    ($($variant:ident($array:ty, $native:ty, $unsigned:ty, $bits:literal, $signed:literal)),+ $(,)?) => {
        impl<'a> IntColumn<'a> {
            /// The reader for a column's physical width, or `None` when the
            /// column is not an integer at all.
            pub(crate) fn resolve(col: &'a dyn Array) -> Option<Self> {
                $(if let Some(a) = col.as_any().downcast_ref::<$array>() {
                    return Some(Self::$variant(a));
                })+

                None
            }

            /// Run a visitor over the array at its own width.
            #[inline]
            pub(crate) fn visit<V: IntVisitor>(&self, visitor: V) -> V::Out {
                match self {
                    $(Self::$variant(a) => visitor.visit(*a),)+
                }
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
                stored_key(self.value(row))
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

        /// The block number a row-group statistic states, widened by the rule
        /// [`IntColumn::block_number`] applies to a value of the same column.
        ///
        /// Which bits are the sign is the column's question, not the
        /// statistic's: parquet has no physical type narrower than 32 bits, so
        /// a `uint64` block number narrowed to a signed sixteen arrives here
        /// sign-extended into an `Int32`. Widened at the statistic's width
        /// instead of the column's, block 40 000 reads as 4 294 941 760 — a
        /// range four thousand million blocks from the rows underneath it, and
        /// the pruner drops the row group its own rows belong to.
        pub(crate) fn block_number_at(data_type: &DataType, stat: i64) -> Option<u64> {
            match data_type {
                $(DataType::$variant => Some((stat as $native as $unsigned) as u64),)+
                _ => None,
            }
        }

        /// The physical width in bits and the signedness of an integer type.
        pub(crate) fn width_of(data_type: &DataType) -> Option<(u32, bool)> {
            match data_type {
                $(DataType::$variant => Some(($bits, $signed)),)+
                _ => None,
            }
        }
    };
}

for_each_int_type!(int_column);

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
/// The scan refuses such a chunk at its entry, over every row group rather than
/// the selected ones, so the readers behind it work on a column already known to
/// be whole. What is left for them is the reading itself.
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
    ($($variant:ident($array:ty, $native:ty, $unsigned:ty, $bits:literal, $signed:literal)),+ $(,)?) => {
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

for_each_int_type!(owned_int_column);

/// The number of integer types, for a cache indexed by [`slot_of`].
pub(crate) const INT_TYPES: usize = 8;

/// The index of an integer type in a per-type cache of [`INT_TYPES`] slots.
pub(crate) fn slot_of(data_type: &DataType) -> Option<usize> {
    let (bits, signed) = width_of(data_type)?;

    let width = match bits {
        8 => 0,
        16 => 1,
        32 => 2,
        _ => 3,
    };
    Some(if signed { width + 4 } else { width })
}

/// The integer values a filter carries, at the signedness the catalog declares.
///
/// A filter is compiled from a declared type, and the declared type says how
/// the stored bits are to be read: an `int16` column stored as `UInt16` holds
/// `-1` as `65535`. So the filter keeps its values wide and at the declared
/// signedness, and narrows to the stored width when it meets the column.
#[derive(Debug, Clone)]
pub(crate) enum IntValues {
    Unsigned(Vec<u64>),
    Signed(Vec<i64>),
}

/// Widens an array's values at the array's signedness.
struct Widen;

impl IntVisitor for Widen {
    type Out = IntValues;

    fn visit<T>(self, array: &PrimitiveArray<T>) -> IntValues
    where
        T: ArrowPrimitiveType,
        T::Native: Into<i128>,
    {
        let wide = array.values().iter().map(|&v| v.into());
        let (_, signed) = width_of(array.data_type()).expect("visited as an integer");

        if signed {
            IntValues::Signed(wide.map(|v| v as i64).collect())
        } else {
            IntValues::Unsigned(wide.map(|v| v as u64).collect())
        }
    }
}

impl IntValues {
    /// The values of an integer array, widened at the array's signedness, or
    /// `None` when the array is not an integer one.
    pub(crate) fn from_array(values: &dyn Array) -> Option<Self> {
        Some(IntColumn::resolve(values)?.visit(Widen))
    }

    /// Whether the values are signed.
    pub(crate) fn signed(&self) -> bool {
        matches!(self, Self::Signed(_))
    }

    /// The keys, in [`stored_key`] terms, of the values that fit the stored
    /// width, in order. A value that does not fit matches no stored value, so
    /// it has no key. `None` when the stored type is not an integer.
    ///
    /// An iterator rather than a list: the pruner asks whether *every* key is
    /// outside a row group, once per group, and stops at the first that is not.
    pub(crate) fn stored_keys(&self, stored: &DataType) -> Option<impl Iterator<Item = u64> + '_> {
        let (bits, signed) = width_of(stored)?;

        let keys: Box<dyn Iterator<Item = u64>> = match self {
            Self::Unsigned(values) => Box::new(
                values
                    .iter()
                    .copied()
                    .filter(move |&v| fits_unsigned(v, bits))
                    .map(move |v| if signed { sign_extend(v, bits) } else { v }),
            ),
            Self::Signed(values) => Box::new(
                values
                    .iter()
                    .copied()
                    .filter(move |&v| fits_signed(v, bits))
                    .map(move |v| {
                        if signed {
                            v as u64
                        } else {
                            zero_extend(v as u64, bits)
                        }
                    }),
            ),
        };

        Some(keys)
    }
}

fn fits_unsigned(value: u64, bits: u32) -> bool {
    bits == 64 || value >> bits == 0
}

fn fits_signed(value: i64, bits: u32) -> bool {
    if bits == 64 {
        return true;
    }

    let half = 1i64 << (bits - 1);
    (-half..half).contains(&value)
}

fn sign_extend(bits_value: u64, bits: u32) -> u64 {
    let shift = 64 - bits;
    (((bits_value << shift) as i64) >> shift) as u64
}

fn zero_extend(bits_value: u64, bits: u32) -> u64 {
    if bits == 64 {
        bits_value
    } else {
        bits_value & ((1u64 << bits) - 1)
    }
}

/// The key of a stored integer: its value sign-extended to 64 bits, so that a
/// signed and an unsigned reading of the same bits produce different keys and
/// [`IntValues::stored_keys`] can pick the one the catalog declared.
#[inline]
pub(crate) fn stored_key(value: i128) -> u64 {
    value as i64 as u64
}

/// The value the catalog declares a stored integer to be: its bits, at the
/// width they were stored, read at the declared signedness.
///
/// This is the reading [`IntValues::stored_keys`] matches equality by, written
/// from the column's side so that an order comparison reads a value the same
/// way an equality does: an `int16` stored as `UInt16` holds `-1` as `65535`,
/// and `>= -1` has to find it where `= -1` does (INV-D7).
#[inline]
pub(crate) fn declared_value(stored: i128, bits: u32, declared_signed: bool) -> i128 {
    let key = stored_key(stored);

    if declared_signed {
        sign_extend(key, bits) as i64 as i128
    } else {
        zero_extend(key, bits) as i128
    }
}
