//! The physical types a declared string column may be stored at.
//!
//! The same reasoning as [`crate::integers`]: the catalog says a column holds
//! text, and the writer picks the Arrow type that carries it — offsets of one
//! width or the other, or a view. A reader that knows one of these and returns
//! "no match" on the rest fails silently on the chunk that happens to use
//! another.
//!
//! Bytes are not text. A `Binary` column is rendered as `0x…` hex, whatever
//! the catalog's encoding, and a string filter has no such value to compare;
//! reading its bytes as UTF-8 would answer "no rows" where the chunk has to be
//! refused (INV-E7). So `Binary` resolves to nothing here, and the refusal
//! follows.

use arrow::array::*;
use arrow::datatypes::DataType;

/// A text column, resolved once so a read costs a match rather than a downcast
/// chain.
pub(crate) enum StringColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

/// An owned reader, for the sites that keep one alive past the batch borrow.
///
/// Arrow arrays are `Arc`-backed, so the clone is a refcount bump.
pub(crate) enum OwnedStringColumn {
    Utf8(StringArray),
    LargeUtf8(LargeStringArray),
    Utf8View(StringViewArray),
}

macro_rules! string_column {
    ($($variant:ident($array:ty)),+ $(,)?) => {
        /// Whether a declared text column may be stored at this physical type.
        pub(crate) fn is_text(data_type: &DataType) -> bool {
            matches!(data_type, $(DataType::$variant)|+)
        }

        impl<'a> StringColumn<'a> {
            /// The reader for a column's physical type, or `None` when the
            /// column does not hold text.
            pub(crate) fn resolve(col: &'a dyn Array) -> Option<Self> {
                $(if let Some(a) = col.as_any().downcast_ref::<$array>() {
                    return Some(Self::$variant(a));
                })+

                None
            }

            /// The text at `row`, or `None` for a null.
            #[inline]
            pub(crate) fn value(&self, row: usize) -> Option<&'a str> {
                match self {
                    $(Self::$variant(a) => (!a.is_null(row)).then(|| a.value(row)),)+
                }
            }

            pub(crate) fn len(&self) -> usize {
                match self {
                    $(Self::$variant(a) => a.len(),)+
                }
            }

            pub(crate) fn nulls(&self) -> Option<arrow::buffer::NullBuffer> {
                match self {
                    $(Self::$variant(a) => a.nulls().cloned(),)+
                }
            }
        }

        impl OwnedStringColumn {
            pub(crate) fn resolve(col: &dyn Array) -> Option<Self> {
                $(if let Some(a) = col.as_any().downcast_ref::<$array>() {
                    return Some(Self::$variant(a.clone()));
                })+

                None
            }

            /// The text at `row`, or `None` for a null.
            #[inline]
            pub(crate) fn value(&self, row: usize) -> Option<&str> {
                match self {
                    $(Self::$variant(a) => (!a.is_null(row)).then(|| a.value(row)),)+
                }
            }
        }
    };
}

string_column!(
    Utf8(StringArray),
    LargeUtf8(LargeStringArray),
    Utf8View(StringViewArray),
);
