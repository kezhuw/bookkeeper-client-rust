use std::fmt::{self, Debug, Display, Formatter};

use static_assertions::assert_impl_all;

use crate::error::Error;

/// ErrorKind categorizes possible errors.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    BookieNotAvailable,
    BookieNotEnough,
    BookieUnexpectedResponse,
    BookieIoError,
    BookieBadVersion,
    BookieReadOnly,
    BookieTooManyRequests,

    EntryNotExisted,
    EntryInvalidData,

    InvalidMetadata,

    InvalidEntryId,
    InvalidLedgerId,
    InvalidServiceUri,

    LedgerConcurrentClose,

    LedgerFenced,
    LedgerClosed,
    LedgerClosing,
    LedgerExisted,
    LedgerNotExisted,
    LedgerForceFailed,
    LedgerIdOverflow,

    UnauthorizedAccess,
    UnexpectedError,
    Timeout,
    ReadExceedLastAddConfirmed,

    MetaClientError,
    MetaVersionMismatch,
    MetaInvalidData,
    MetaUnexpectedResponse,
    MetaConcurrentOperation,
    MetaClusterUninitialized,
}

impl ErrorKind {
    fn as_str(&self) -> &'static str {
        use ErrorKind::*;
        match *self {
            BookieNotAvailable => "bookie not available",
            BookieNotEnough => "no enough bookie available",
            BookieUnexpectedResponse => "unexpected bookie response",
            BookieIoError => "bookie io error",
            BookieBadVersion => "bookie protocol mismatch",
            BookieReadOnly => "write on readonly bookie",
            BookieTooManyRequests => "too many requests routed to bookie",

            EntryNotExisted => "entry not existed",
            EntryInvalidData => "invalid entry data",

            InvalidMetadata => "invalid metadata",

            InvalidEntryId => "invalid entry id",
            InvalidLedgerId => "invalid ledger id",
            InvalidServiceUri => "invalid service uri",

            LedgerConcurrentClose => "ledger concurrent close detected",

            LedgerFenced => "ledger fenced",
            LedgerClosed => "ledger closed",
            LedgerClosing => "ledger closing",
            LedgerExisted => "ledger already existed",
            LedgerNotExisted => "ledger not existed",
            LedgerForceFailed => "ledger force failed",
            LedgerIdOverflow => "ledger id overflow",

            UnauthorizedAccess => "unauthorized access",
            UnexpectedError => "unexpected error",
            Timeout => "timeouted",
            ReadExceedLastAddConfirmed => "read exceed last add confirmed entry",

            MetaClientError => "meta store client error",
            MetaVersionMismatch => "meta version mismatch",
            MetaInvalidData => "invalid meta data",
            MetaUnexpectedResponse => "unexpected meta response",
            MetaConcurrentOperation => "concurrent meta operation",
            MetaClusterUninitialized => "BookKeeper cluster not initialized",
        }
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        f.write_str(self.as_str())
    }
}

impl std::error::Error for ErrorKind {}

assert_impl_all!(ErrorKind: Display, std::error::Error, Send, Sync);

pub type BkError = Error<ErrorKind>;
pub type BkResult<T> = std::result::Result<T, BkError>;
pub type Result<T> = std::result::Result<T, BkError>;
