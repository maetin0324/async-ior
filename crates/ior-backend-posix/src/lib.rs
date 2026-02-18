use std::collections::VecDeque;
use std::ffi::CString;
use std::os::raw::c_int;
use std::os::unix::io::RawFd;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use ior_core::aiori::next_xfer_token;
use ior_core::error::IorError;
use ior_core::handle::{FileHandle, OpenFlags, StatResult, XferCallback, XferDir, XferResult, XferToken};
use ior_core::Aiori;

/// Maximum number of retries for partial transfers (matching C IOR MAX_RETRY).
const MAX_RETRY: usize = 10_000;

/// Internal wrapper holding a POSIX file descriptor.
struct PosixFd {
    fd: RawFd,
}

// Safety: file descriptors are just integers; concurrent pread/pwrite on the
// same fd with different offsets is safe in POSIX.
unsafe impl Send for PosixFd {}
unsafe impl Sync for PosixFd {}

/// A pending async I/O operation.
struct PendingOp {
    token: XferToken,
    fd: RawFd,
    dir: XferDir,
    buf: *mut u8,
    len: i64,
    offset: i64,
    user_data: usize,
    callback: XferCallback,
}

// Safety: buf pointer is guaranteed valid by the caller until callback fires.
unsafe impl Send for PendingOp {}

/// A completed async I/O operation, awaiting callback dispatch.
struct CompletedOp {
    result: XferResult,
    callback: XferCallback,
}

/// Pending queue state, protected by a single Mutex.
struct PendingState {
    queue: VecDeque<PendingOp>,
    shutdown: bool,
}

/// Shared state between thread pool workers and the pool handle.
struct PoolShared {
    pending: Mutex<PendingState>,
    completed: Mutex<VecDeque<CompletedOp>>,
    condvar: Condvar,
}

/// Thread pool for async I/O operations.
struct ThreadPool {
    shared: Arc<PoolShared>,
    workers: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    fn new(num_threads: usize) -> Self {
        let shared = Arc::new(PoolShared {
            pending: Mutex::new(PendingState {
                queue: VecDeque::new(),
                shutdown: false,
            }),
            completed: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
        });

        let mut workers = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            let shared_ref = Arc::clone(&shared);
            workers.push(thread::spawn(move || {
                Self::worker_loop(&shared_ref);
            }));
        }

        Self { shared, workers }
    }

    fn worker_loop(shared: &PoolShared) {
        loop {
            let op = {
                let mut state = shared.pending.lock().unwrap();
                loop {
                    if state.shutdown {
                        return;
                    }
                    if let Some(op) = state.queue.pop_front() {
                        break op;
                    }
                    state = shared.condvar.wait(state).unwrap();
                }
            };

            // Execute the I/O operation
            let result = execute_posix_io(op.fd, op.dir, op.buf, op.len, op.offset);

            let completed = CompletedOp {
                result: XferResult {
                    token: op.token,
                    bytes_transferred: result.unwrap_or(-1),
                    error: if result.is_ok() {
                        0
                    } else {
                        unsafe { *libc::__errno_location() }
                    },
                    user_data: op.user_data,
                },
                callback: op.callback,
            };

            shared.completed.lock().unwrap().push_back(completed);
        }
    }

    fn submit(&self, op: PendingOp) {
        self.shared.pending.lock().unwrap().queue.push_back(op);
        self.shared.condvar.notify_one();
    }

    fn poll(&self, max_completions: usize) -> usize {
        let mut completed = self.shared.completed.lock().unwrap();
        let count = completed.len().min(max_completions);
        for _ in 0..count {
            if let Some(cop) = completed.pop_front() {
                // Fire callback on the polling (caller) thread
                (cop.callback)(&cop.result);
            }
        }
        count
    }

    fn cancel(&self, token: XferToken) -> bool {
        let mut state = self.shared.pending.lock().unwrap();
        if let Some(pos) = state.queue.iter().position(|op| op.token == token) {
            let op = state.queue.remove(pos).unwrap();
            let result = XferResult {
                token: op.token,
                bytes_transferred: 0,
                error: libc::ECANCELED,
                user_data: op.user_data,
            };
            (op.callback)(&result);
            true
        } else {
            false
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.shared.pending.lock().unwrap().shutdown = true;
        self.shared.condvar.notify_all();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

/// Perform a synchronous pread/pwrite with retry.
fn execute_posix_io(
    fd: RawFd,
    dir: XferDir,
    buf: *mut u8,
    len: i64,
    offset: i64,
) -> Result<i64, ()> {
    let mut remaining = len;
    let mut ptr = buf;
    let mut off = offset as libc::off_t;
    let mut retries = 0;

    while remaining > 0 {
        let rc = match dir {
            XferDir::Write => unsafe {
                libc::pwrite(fd, ptr as *const libc::c_void, remaining as usize, off)
            },
            XferDir::Read => unsafe {
                libc::pread(fd, ptr as *mut libc::c_void, remaining as usize, off)
            },
        };

        if rc < 0 {
            return Err(());
        }
        if rc == 0 {
            break;
        }

        let transferred = rc as i64;
        remaining -= transferred;
        ptr = unsafe { ptr.add(transferred as usize) };
        off += transferred as libc::off_t;

        if remaining > 0 {
            retries += 1;
            if retries >= MAX_RETRY {
                break;
            }
        }
    }

    Ok(len - remaining)
}

/// POSIX I/O backend implementing the Aiori trait.
///
/// Reference: `aiori-POSIX.c`
pub struct PosixBackend {
    /// Use O_DIRECT to bypass OS page cache.
    pub direct_io: bool,
    /// Thread pool for async I/O (None = async not supported).
    pool: Option<ThreadPool>,
}

impl PosixBackend {
    pub fn new(direct_io: bool) -> Self {
        Self {
            direct_io,
            pool: None,
        }
    }

    /// Create with an async thread pool of given size.
    pub fn with_pool(direct_io: bool, pool_size: usize) -> Self {
        Self {
            direct_io,
            pool: Some(ThreadPool::new(pool_size)),
        }
    }

    /// Convert IOR OpenFlags to libc O_* flags.
    fn to_libc_flags(&self, flags: OpenFlags) -> c_int {
        let mut oflags: c_int = 0;

        if flags.contains(OpenFlags::RDONLY)
            && !flags.contains(OpenFlags::WRONLY | OpenFlags::RDWR)
        {
            oflags |= libc::O_RDONLY;
        }
        if flags.contains(OpenFlags::WRONLY) {
            oflags |= libc::O_WRONLY;
        }
        if flags.contains(OpenFlags::RDWR) {
            oflags |= libc::O_RDWR;
        }
        if flags.contains(OpenFlags::APPEND) {
            oflags |= libc::O_APPEND;
        }
        if flags.contains(OpenFlags::CREAT) {
            oflags |= libc::O_CREAT;
        }
        if flags.contains(OpenFlags::TRUNC) {
            oflags |= libc::O_TRUNC;
        }
        if flags.contains(OpenFlags::EXCL) {
            oflags |= libc::O_EXCL;
        }
        if flags.contains(OpenFlags::DIRECT) || self.direct_io {
            oflags |= libc::O_DIRECT;
        }

        oflags
    }

    fn path_to_cstring(path: &str) -> Result<CString, IorError> {
        CString::new(path).map_err(|_| IorError::InvalidArgument)
    }

    fn errno() -> i32 {
        unsafe { *libc::__errno_location() }
    }
}

impl Aiori for PosixBackend {
    fn name(&self) -> &str {
        "POSIX"
    }

    /// Create a new file. Reference: `aiori-POSIX.c:POSIX_Create`
    fn create(&self, path: &str, flags: OpenFlags) -> Result<FileHandle, IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let oflags = self.to_libc_flags(flags | OpenFlags::CREAT | OpenFlags::RDWR);
        let mode: libc::mode_t = 0o664;

        let fd = unsafe { libc::open(cpath.as_ptr(), oflags, mode) };
        if fd < 0 {
            return Err(IorError::Io(Self::errno()));
        }

        Ok(FileHandle::new(PosixFd { fd }))
    }

    /// Open an existing file. Reference: `aiori-POSIX.c:POSIX_Open`
    fn open(&self, path: &str, flags: OpenFlags) -> Result<FileHandle, IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let oflags = self.to_libc_flags(flags);

        let fd = unsafe { libc::open(cpath.as_ptr(), oflags) };
        if fd < 0 {
            return Err(IorError::Io(Self::errno()));
        }

        Ok(FileHandle::new(PosixFd { fd }))
    }

    /// Close a file. Reference: `aiori-POSIX.c:POSIX_Close`
    fn close(&self, handle: FileHandle) -> Result<(), IorError> {
        let pfd = handle
            .downcast_ref::<PosixFd>()
            .ok_or(IorError::InvalidArgument)?;
        let rc = unsafe { libc::close(pfd.fd) };
        if rc < 0 {
            return Err(IorError::Io(Self::errno()));
        }
        Ok(())
    }

    /// Delete a file. Reference: `aiori-POSIX.c:POSIX_Delete`
    fn delete(&self, path: &str) -> Result<(), IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { libc::unlink(cpath.as_ptr()) };
        if rc < 0 {
            let errno = Self::errno();
            if errno != libc::ENOENT {
                return Err(IorError::Io(errno));
            }
        }
        Ok(())
    }

    /// Fsync a file. Reference: `aiori-POSIX.c:POSIX_Fsync`
    fn fsync(&self, handle: &FileHandle) -> Result<(), IorError> {
        let pfd = handle
            .downcast_ref::<PosixFd>()
            .ok_or(IorError::InvalidArgument)?;
        let rc = unsafe { libc::fsync(pfd.fd) };
        if rc < 0 {
            return Err(IorError::Io(Self::errno()));
        }
        Ok(())
    }

    /// Get file size via stat(). Reference: `aiori-POSIX.c:POSIX_GetFileSize`
    fn get_file_size(&self, path: &str) -> Result<i64, IorError> {
        let cpath = Self::path_to_cstring(path)?;
        unsafe {
            let mut st: libc::stat = std::mem::zeroed();
            let rc = libc::stat(cpath.as_ptr(), &mut st);
            if rc < 0 {
                return Err(IorError::Io(Self::errno()));
            }
            Ok(st.st_size)
        }
    }

    /// Check file accessibility. Reference: `aiori-POSIX.c:aiori_posix_access`
    fn access(&self, path: &str, mode: i32) -> Result<bool, IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { libc::access(cpath.as_ptr(), mode) };
        Ok(rc == 0)
    }

    /// Create a directory. Reference: `aiori.c:227-230`
    fn mkdir(&self, path: &str, mode: u32) -> Result<(), IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { libc::mkdir(cpath.as_ptr(), mode as libc::mode_t) };
        if rc < 0 {
            return Err(IorError::Io(Self::errno()));
        }
        Ok(())
    }

    /// Remove an empty directory. Reference: `aiori.c:232-235`
    fn rmdir(&self, path: &str) -> Result<(), IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { libc::rmdir(cpath.as_ptr()) };
        if rc < 0 {
            return Err(IorError::Io(Self::errno()));
        }
        Ok(())
    }

    /// Stat a file or directory. Reference: `aiori.c:242-245`
    fn stat(&self, path: &str) -> Result<StatResult, IorError> {
        let cpath = Self::path_to_cstring(path)?;
        unsafe {
            let mut st: libc::stat = std::mem::zeroed();
            let rc = libc::stat(cpath.as_ptr(), &mut st);
            if rc < 0 {
                return Err(IorError::Io(Self::errno()));
            }
            Ok(StatResult {
                size: st.st_size,
                mode: st.st_mode,
                nlink: st.st_nlink,
                uid: st.st_uid,
                gid: st.st_gid,
                atime: st.st_atime,
                mtime: st.st_mtime,
                ctime: st.st_ctime,
            })
        }
    }

    /// Rename a file or directory. Reference: `aiori-POSIX.c:844-853`
    fn rename(&self, old_path: &str, new_path: &str) -> Result<(), IorError> {
        let cold = Self::path_to_cstring(old_path)?;
        let cnew = Self::path_to_cstring(new_path)?;
        let rc = unsafe { libc::rename(cold.as_ptr(), cnew.as_ptr()) };
        if rc < 0 {
            return Err(IorError::Io(Self::errno()));
        }
        Ok(())
    }

    /// Create a file node (mknod). Reference: `aiori-POSIX.c:606-618`
    fn mknod(&self, path: &str) -> Result<(), IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let mode = libc::S_IFREG | libc::S_IRUSR;
        let rc = unsafe { libc::mknod(cpath.as_ptr(), mode, 0) };
        if rc < 0 {
            return Err(IorError::Io(Self::errno()));
        }
        Ok(())
    }

    /// Synchronous pread/pwrite with retry loop.
    /// Reference: `aiori-POSIX.c:POSIX_Xfer` (lines 671-793)
    fn xfer_sync(
        &self,
        handle: &FileHandle,
        dir: XferDir,
        buf: *mut u8,
        len: i64,
        offset: i64,
    ) -> Result<i64, IorError> {
        let pfd = handle
            .downcast_ref::<PosixFd>()
            .ok_or(IorError::InvalidArgument)?;

        execute_posix_io(pfd.fd, dir, buf, len, offset).map_err(|_| IorError::Io(Self::errno()))
    }

    /// Submit an async I/O operation to the thread pool.
    fn xfer_submit(
        &self,
        handle: &FileHandle,
        dir: XferDir,
        buf: *mut u8,
        len: i64,
        offset: i64,
        user_data: usize,
        callback: XferCallback,
    ) -> Result<XferToken, IorError> {
        let pfd = handle
            .downcast_ref::<PosixFd>()
            .ok_or(IorError::InvalidArgument)?;

        let pool = self.pool.as_ref().ok_or(IorError::NotSupported)?;
        let token = next_xfer_token();

        pool.submit(PendingOp {
            token,
            fd: pfd.fd,
            dir,
            buf,
            len,
            offset,
            user_data,
            callback,
        });

        Ok(token)
    }

    /// Poll for completed async operations, dispatching callbacks.
    fn poll(&self, max_completions: usize) -> Result<usize, IorError> {
        let pool = self.pool.as_ref().ok_or(IorError::NotSupported)?;
        Ok(pool.poll(max_completions))
    }

    /// Cancel a pending async operation.
    fn cancel(&self, token: XferToken) -> Result<(), IorError> {
        let pool = self.pool.as_ref().ok_or(IorError::NotSupported)?;
        if pool.cancel(token) {
            Ok(())
        } else {
            Err(IorError::NotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_write_read_delete() {
        let backend = PosixBackend::new(false);
        let path = "/tmp/ior_posix_test_basic";

        let handle = backend
            .create(path, OpenFlags::CREAT | OpenFlags::RDWR)
            .unwrap();
        let data = b"Hello, IOR!";
        let written = backend
            .xfer_sync(
                &handle,
                XferDir::Write,
                data.as_ptr() as *mut u8,
                data.len() as i64,
                0,
            )
            .unwrap();
        assert_eq!(written, data.len() as i64);
        backend.fsync(&handle).unwrap();
        backend.close(handle).unwrap();

        let size = backend.get_file_size(path).unwrap();
        assert_eq!(size, data.len() as i64);

        let handle = backend.open(path, OpenFlags::RDONLY).unwrap();
        let mut buf = vec![0u8; data.len()];
        let read_bytes = backend
            .xfer_sync(
                &handle,
                XferDir::Read,
                buf.as_mut_ptr(),
                buf.len() as i64,
                0,
            )
            .unwrap();
        assert_eq!(read_bytes, data.len() as i64);
        assert_eq!(&buf, data);
        backend.close(handle).unwrap();

        backend.delete(path).unwrap();
        assert!(!backend.access(path, libc::F_OK).unwrap());
    }

    #[test]
    fn test_large_transfer() {
        let backend = PosixBackend::new(false);
        let path = "/tmp/ior_posix_test_large";
        let size = 1_048_576i64;

        let handle = backend
            .create(path, OpenFlags::CREAT | OpenFlags::RDWR)
            .unwrap();

        let data: Vec<u8> = (0..size as usize).map(|i| (i % 256) as u8).collect();
        let written = backend
            .xfer_sync(
                &handle,
                XferDir::Write,
                data.as_ptr() as *mut u8,
                size,
                0,
            )
            .unwrap();
        assert_eq!(written, size);
        backend.close(handle).unwrap();

        let handle = backend.open(path, OpenFlags::RDONLY).unwrap();
        let mut buf = vec![0u8; size as usize];
        let read_bytes = backend
            .xfer_sync(&handle, XferDir::Read, buf.as_mut_ptr(), size, 0)
            .unwrap();
        assert_eq!(read_bytes, size);
        assert_eq!(buf, data);
        backend.close(handle).unwrap();

        backend.delete(path).unwrap();
    }

    #[test]
    fn test_mkdir_rmdir_stat() {
        let backend = PosixBackend::new(false);
        let dir = "/tmp/ior_posix_test_mkdir";

        // Clean up if exists
        let _ = backend.rmdir(dir);

        // mkdir
        backend.mkdir(dir, 0o755).unwrap();

        // stat
        let st = backend.stat(dir).unwrap();
        assert!(st.mode & libc::S_IFDIR != 0);

        // Create a file inside, stat it, then remove
        let file_path = format!("{}/testfile", dir);
        let handle = backend
            .create(&file_path, OpenFlags::CREAT | OpenFlags::RDWR)
            .unwrap();
        backend.close(handle).unwrap();

        let fst = backend.stat(&file_path).unwrap();
        assert!(fst.mode & libc::S_IFREG != 0);

        backend.delete(&file_path).unwrap();

        // rmdir
        backend.rmdir(dir).unwrap();
        assert!(!backend.access(dir, libc::F_OK).unwrap());
    }

    #[test]
    fn test_rename() {
        let backend = PosixBackend::new(false);
        let old_path = "/tmp/ior_posix_test_rename_old";
        let new_path = "/tmp/ior_posix_test_rename_new";

        // Clean up
        let _ = backend.delete(old_path);
        let _ = backend.delete(new_path);

        let handle = backend
            .create(old_path, OpenFlags::CREAT | OpenFlags::RDWR)
            .unwrap();
        backend.close(handle).unwrap();

        backend.rename(old_path, new_path).unwrap();
        assert!(!backend.access(old_path, libc::F_OK).unwrap());
        assert!(backend.access(new_path, libc::F_OK).unwrap());

        backend.delete(new_path).unwrap();
    }

    #[test]
    fn test_async_write_read() {
        let backend = PosixBackend::with_pool(false, 2);
        let path = "/tmp/ior_posix_test_async";

        let handle = backend
            .create(path, OpenFlags::CREAT | OpenFlags::RDWR)
            .unwrap();

        // Async write — callback fires on the poll() caller thread (same thread),
        // so a plain local variable suffices instead of Arc<Atomic>.
        let data = b"Async IOR test data!";
        let mut result_bytes: i64 = -1;
        let user_data = &mut result_bytes as *mut i64 as usize;

        extern "C" fn write_cb(result: *const XferResult) {
            unsafe {
                let res = &*result;
                let ptr = res.user_data as *mut i64;
                *ptr = res.bytes_transferred;
            }
        }

        let _token = backend
            .xfer_submit(
                &handle,
                XferDir::Write,
                data.as_ptr() as *mut u8,
                data.len() as i64,
                0,
                user_data,
                write_cb,
            )
            .unwrap();

        // Poll until done
        loop {
            backend.poll(10).unwrap();
            if result_bytes >= 0 {
                break;
            }
            std::thread::yield_now();
        }

        assert_eq!(result_bytes, data.len() as i64);

        backend.fsync(&handle).unwrap();
        backend.close(handle).unwrap();

        // Verify via sync read
        let handle = backend.open(path, OpenFlags::RDONLY).unwrap();
        let mut buf = vec![0u8; data.len()];
        let read_bytes = backend
            .xfer_sync(
                &handle,
                XferDir::Read,
                buf.as_mut_ptr(),
                buf.len() as i64,
                0,
            )
            .unwrap();
        assert_eq!(read_bytes, data.len() as i64);
        assert_eq!(&buf, data);
        backend.close(handle).unwrap();

        backend.delete(path).unwrap();
    }
}
