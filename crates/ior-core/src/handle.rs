use std::any::Any;

use bitflags::bitflags;

/// Opaque file handle wrapping backend-specific state.
pub struct FileHandle {
    inner: Box<dyn Any + Send + Sync>,
}

impl FileHandle {
    /// Create a new file handle from any backend-specific type.
    pub fn new<T: Any + Send + Sync>(value: T) -> Self {
        Self {
            inner: Box::new(value),
        }
    }

    /// Attempt to downcast to the concrete type.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.inner.downcast_ref::<T>()
    }
}

/// Monotonic token identifying an async transfer operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct XferToken(pub u64);

/// Direction of a data transfer.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XferDir {
    Read = 0,
    Write = 1,
}

/// Result of a completed async transfer, passed to callbacks.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct XferResult {
    /// Token identifying the completed transfer
    pub token: XferToken,
    /// Number of bytes successfully transferred (negative on error)
    pub bytes_transferred: i64,
    /// Error code (0 = success, positive = errno)
    pub error: i32,
    /// User-supplied opaque data
    pub user_data: usize,
}

/// C-compatible callback function type for async transfer completion.
pub type XferCallback = extern "C" fn(*const XferResult);

/// File/directory stat result.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct StatResult {
    pub size: i64,
    pub mode: u32,
    pub nlink: u64,
    pub uid: u32,
    pub gid: u32,
    pub atime: i64,
    pub mtime: i64,
    pub ctime: i64,
}

bitflags! {
    /// File open flags matching C IOR's aiori.h definitions.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct OpenFlags: u32 {
        /// Read only
        const RDONLY  = 0x01;
        /// Write only
        const WRONLY  = 0x02;
        /// Read/write
        const RDWR    = 0x04;
        /// Append
        const APPEND  = 0x08;
        /// Create
        const CREAT   = 0x10;
        /// Truncate
        const TRUNC   = 0x20;
        /// Exclusive
        const EXCL    = 0x40;
        /// Bypass I/O buffers (O_DIRECT)
        const DIRECT  = 0x80;
    }
}
