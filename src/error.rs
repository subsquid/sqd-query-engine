//! Error kinds.
//!
//! Every error a client can provoke carries one of the kinds of
//! `spec/06-errors.md`. Clients switch on the kind; only humans read the message
//! (INV-E6). A library that has to match on message text breaks the day someone
//! improves the wording, so the text is deliberately not part of the contract.
//!
//! Kinds ride along inside `anyhow::Error` rather than replacing it: the engine's
//! internals return `anyhow::Result` throughout, and a request error is one of
//! several things that can go wrong on the way. [`error_kind`] recovers the kind
//! at the boundary where a response is rendered.

use std::fmt;

/// The kinds of `spec/06-errors.md` §6.2 (validation) and §6.3 (execution).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The body is not a JSON object, or a value has the wrong JSON type.
    MalformedRequest,
    /// `type` is absent, or names no dataset.
    UnknownDataset,
    /// A top-level key is neither reserved nor a `queryName`.
    UnknownTable,
    /// An item-request key names neither a declared filter nor a declared
    /// relation of its table.
    UnknownFilter,
    /// A key of `fields` names no table's `fieldName`.
    UnknownFieldGroup,
    /// A key inside `fields.X` names no selectable field of `X`.
    UnknownField,
    /// `toBlock < fromBlock`.
    InvalidBlockRange,
    /// A block bound is not an unsigned 64-bit integer.
    InvalidBlockNumber,
    /// More than `P-MAX-ITEM-REQUESTS` item requests.
    TooManyItemRequests,
    /// More than `P-MAX-BLOOM-VALUES` values in one bloom filter.
    TooManyBloomValues,
    /// More than one discriminator-family filter in one item request.
    ConflictingFilters,
    /// A hex value lacks `0x`, has odd length, or holds a non-hex digit.
    InvalidHex,
    /// A discriminator value exceeds `P-MAX-DISCRIMINATOR-BYTES`.
    DiscriminatorTooLong,
    /// A filter value has a form the filter kind does not accept.
    InvalidFilterValue,
    /// The request exceeds `P-MAX-REQUEST-BYTES` or `P-MAX-IN-LIST`.
    RequestTooLarge,
    /// A reserved key the dataset cannot honour.
    UnsupportedRequestField,
    /// The chunk has no data for a table the query needs.
    TableNotFound,
    /// A column the query selects or filters on is absent from the chunk.
    ColumnNotFound,
    /// `parentBlockHash` does not match the hash of the block preceding
    /// `fromBlock`.
    UnexpectedBaseBlock,
    /// A relation, group or ordering key has a physical type the engine cannot
    /// compare.
    UnsupportedKeyType,
    /// A stored value violates the catalog.
    MalformedChunkData,
}

impl ErrorKind {
    /// The wire spelling, which is what a client switches on.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorKind::MalformedRequest => "MalformedRequest",
            ErrorKind::UnknownDataset => "UnknownDataset",
            ErrorKind::UnknownTable => "UnknownTable",
            ErrorKind::UnknownFilter => "UnknownFilter",
            ErrorKind::UnknownFieldGroup => "UnknownFieldGroup",
            ErrorKind::UnknownField => "UnknownField",
            ErrorKind::InvalidBlockRange => "InvalidBlockRange",
            ErrorKind::InvalidBlockNumber => "InvalidBlockNumber",
            ErrorKind::TooManyItemRequests => "TooManyItemRequests",
            ErrorKind::TooManyBloomValues => "TooManyBloomValues",
            ErrorKind::ConflictingFilters => "ConflictingFilters",
            ErrorKind::InvalidHex => "InvalidHex",
            ErrorKind::DiscriminatorTooLong => "DiscriminatorTooLong",
            ErrorKind::InvalidFilterValue => "InvalidFilterValue",
            ErrorKind::RequestTooLarge => "RequestTooLarge",
            ErrorKind::UnsupportedRequestField => "UnsupportedRequestField",
            ErrorKind::TableNotFound => "TableNotFound",
            ErrorKind::ColumnNotFound => "ColumnNotFound",
            ErrorKind::UnexpectedBaseBlock => "UnexpectedBaseBlock",
            ErrorKind::UnsupportedKeyType => "UnsupportedKeyType",
            ErrorKind::MalformedChunkData => "MalformedChunkData",
        }
    }

    /// Whether the kind is one §6.2 raises from the request and the catalog
    /// alone, before any chunk data is read (INV-E2).
    pub fn is_validation(self) -> bool {
        !matches!(
            self,
            ErrorKind::TableNotFound
                | ErrorKind::ColumnNotFound
                | ErrorKind::UnexpectedBaseBlock
                | ErrorKind::UnsupportedKeyType
                | ErrorKind::MalformedChunkData
        )
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An error with a kind attached.
#[derive(Debug, Clone)]
pub struct EngineError {
    pub kind: ErrorKind,
    pub message: String,
}

impl EngineError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        EngineError {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for EngineError {}

/// The kind of an error on its way out of the engine, if it has one.
///
/// `UnexpectedBaseBlock` carries its own type because a client needs the recent
/// block refs with it, so it is recognised here rather than being wrapped.
pub fn error_kind(err: &anyhow::Error) -> Option<ErrorKind> {
    if let Some(typed) = err.downcast_ref::<EngineError>() {
        return Some(typed.kind);
    }
    if err
        .downcast_ref::<crate::output::UnexpectedBaseBlock>()
        .is_some()
    {
        return Some(ErrorKind::UnexpectedBaseBlock);
    }
    None
}

/// Build an [`EngineError`] as an `anyhow::Error`.
#[macro_export]
macro_rules! engine_err {
    ($kind:expr, $($arg:tt)*) => {
        ::anyhow::Error::new($crate::error::EngineError::new($kind, format!($($arg)*)))
    };
}

/// Return an [`EngineError`] from the enclosing function.
#[macro_export]
macro_rules! engine_bail {
    ($kind:expr, $($arg:tt)*) => {
        return ::std::result::Result::Err($crate::engine_err!($kind, $($arg)*))
    };
}

/// Return an [`EngineError`] unless a condition holds.
#[macro_export]
macro_rules! engine_ensure {
    ($cond:expr, $kind:expr, $($arg:tt)*) => {
        if !$cond {
            $crate::engine_bail!($kind, $($arg)*);
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The kind survives the `anyhow` round trip the engine's own signatures put
    /// it through — that is the whole mechanism, and a `?` that loses it turns
    /// every typed error back into prose.
    #[test]
    fn a_kind_survives_being_carried_by_anyhow() {
        fn inner() -> anyhow::Result<()> {
            engine_bail!(ErrorKind::UnknownField, "unknown field 'logIndx'");
        }

        fn outer() -> anyhow::Result<()> {
            inner()?;
            Ok(())
        }

        let err = outer().unwrap_err();
        assert_eq!(error_kind(&err), Some(ErrorKind::UnknownField));
        assert_eq!(err.to_string(), "unknown field 'logIndx'");
    }

    /// An error with no kind is an engine bug rather than a client mistake, and
    /// must not be reported as one.
    #[test]
    fn an_untyped_error_has_no_kind() {
        let err = anyhow::anyhow!("the disk went away");
        assert_eq!(error_kind(&err), None);
    }

    #[test]
    fn validation_kinds_are_the_ones_raised_before_a_chunk_is_read() {
        assert!(ErrorKind::UnknownFilter.is_validation());
        assert!(!ErrorKind::ColumnNotFound.is_validation());
    }
}
