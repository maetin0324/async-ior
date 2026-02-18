use std::io;

use thiserror::Error;

/// IOR error type mapping common I/O error categories.
#[repr(C)]
#[derive(Debug, Error)]
pub enum IorError {
    /// OS-level I/O error with errno value
    #[error("I/O error (errno={0})")]
    Io(i32),

    /// Invalid argument provided
    #[error("invalid argument")]
    InvalidArgument,

    /// File or resource not found
    #[error("not found")]
    NotFound,

    /// Permission denied
    #[error("permission denied")]
    PermissionDenied,

    /// Operation was cancelled
    #[error("cancelled")]
    Cancelled,

    /// Operation not supported by this backend
    #[error("not supported")]
    NotSupported,

    /// Unknown or unclassified error
    #[error("unknown error")]
    Unknown,
}

impl From<io::Error> for IorError {
    fn from(e: io::Error) -> Self {
        match e.kind() {
            io::ErrorKind::NotFound => IorError::NotFound,
            io::ErrorKind::PermissionDenied => IorError::PermissionDenied,
            io::ErrorKind::InvalidInput => IorError::InvalidArgument,
            io::ErrorKind::Unsupported => IorError::NotSupported,
            _ => {
                if let Some(errno) = e.raw_os_error() {
                    IorError::Io(errno)
                } else {
                    IorError::Unknown
                }
            }
        }
    }
}
