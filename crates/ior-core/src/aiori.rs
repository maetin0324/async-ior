use std::cell::Cell;

use crate::error::IorError;
use crate::handle::{FileHandle, OpenFlags, StatResult, XferCallback, XferDir, XferResult, XferToken};

thread_local! {
    /// Per-thread monotonic counter for generating unique XferTokens.
    /// Only the main thread calls xfer_submit, so no cross-thread sharing needed.
    static NEXT_TOKEN: Cell<u64> = const { Cell::new(1) };
}

/// Generate the next unique transfer token.
pub fn next_xfer_token() -> XferToken {
    NEXT_TOKEN.with(|t| {
        let val = t.get();
        t.set(val + 1);
        XferToken(val)
    })
}

/// Abstract I/O interface matching C IOR's `ior_aiori_t`.
///
/// All metadata operations are synchronous. Data transfer supports both
/// sync and async modes.
pub trait Aiori {
    /// Backend name (e.g., "POSIX")
    fn name(&self) -> &str;

    /// Create a new file, returning an opaque handle.
    fn create(&self, path: &str, flags: OpenFlags) -> Result<FileHandle, IorError>;

    /// Open an existing file, returning an opaque handle.
    fn open(&self, path: &str, flags: OpenFlags) -> Result<FileHandle, IorError>;

    /// Close a file handle.
    fn close(&self, handle: FileHandle) -> Result<(), IorError>;

    /// Delete (unlink) a file.
    fn delete(&self, path: &str) -> Result<(), IorError>;

    /// Flush file data to storage.
    fn fsync(&self, handle: &FileHandle) -> Result<(), IorError>;

    /// Get the size of a file in bytes.
    fn get_file_size(&self, path: &str) -> Result<i64, IorError>;

    /// Check file accessibility. Returns true if accessible.
    fn access(&self, path: &str, mode: i32) -> Result<bool, IorError>;

    /// Submit an asynchronous data transfer.
    ///
    /// The callback will be invoked on the thread calling `poll()` when
    /// the transfer completes.
    ///
    /// # Safety
    /// `buf` must remain valid until the callback fires or the transfer is cancelled.
    fn xfer_submit(
        &self,
        handle: &FileHandle,
        dir: XferDir,
        buf: *mut u8,
        len: i64,
        offset: i64,
        user_data: usize,
        callback: XferCallback,
    ) -> Result<XferToken, IorError>;

    /// Poll for completed async transfers, invoking callbacks.
    /// Returns the number of completions processed.
    fn poll(&self, max_completions: usize) -> Result<usize, IorError>;

    /// Cancel a pending async transfer.
    fn cancel(&self, token: XferToken) -> Result<(), IorError>;

    /// Create a directory with given permissions.
    fn mkdir(&self, path: &str, mode: u32) -> Result<(), IorError> {
        let _ = (path, mode);
        Err(IorError::NotSupported)
    }

    /// Remove an empty directory.
    fn rmdir(&self, path: &str) -> Result<(), IorError> {
        let _ = path;
        Err(IorError::NotSupported)
    }

    /// Stat a file or directory.
    fn stat(&self, path: &str) -> Result<StatResult, IorError> {
        let _ = path;
        Err(IorError::NotSupported)
    }

    /// Rename a file or directory.
    fn rename(&self, old_path: &str, new_path: &str) -> Result<(), IorError> {
        let _ = (old_path, new_path);
        Err(IorError::NotSupported)
    }

    /// Create a file node (mknod). Fast alternative to open+close for file creation.
    fn mknod(&self, path: &str) -> Result<(), IorError> {
        let _ = path;
        Err(IorError::NotSupported)
    }

    /// Synchronous data transfer with retry loop.
    ///
    /// Default implementation: submit + poll loop. Backends should override
    /// for direct pread/pwrite.
    ///
    /// # Safety
    /// `buf` must point to at least `len` bytes of valid memory.
    fn xfer_sync(
        &self,
        handle: &FileHandle,
        dir: XferDir,
        buf: *mut u8,
        len: i64,
        offset: i64,
    ) -> Result<i64, IorError> {
        // Callbacks fire on the poll() caller thread (same thread), so a
        // plain local variable suffices — no Arc/Atomic needed.
        let mut result_bytes: i64 = -1;
        let result_ptr = &mut result_bytes as *mut i64 as usize;

        extern "C" fn sync_callback(result: *const XferResult) {
            unsafe {
                let res = &*result;
                let ptr = res.user_data as *mut i64;
                *ptr = res.bytes_transferred;
            }
        }

        self.xfer_submit(handle, dir, buf, len, offset, result_ptr, sync_callback)?;

        // Poll until completion
        loop {
            self.poll(1)?;
            if result_bytes >= 0 {
                return Ok(result_bytes);
            }
        }
    }
}
