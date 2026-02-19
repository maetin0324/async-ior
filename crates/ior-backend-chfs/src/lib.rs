mod ffi;

use std::ffi::CString;
use std::os::raw::c_int;
use std::ptr;

use ior_core::backend_options::BackendOptions;
use ior_core::error::IorError;
use ior_core::handle::{
    FileHandle, OpenFlags, StatResult, XferCallback, XferDir, XferToken,
};
use ior_core::Aiori;

use ffi::*;

/// Maximum number of retries for partial transfers (matching C IOR MAX_RETRY).
const MAX_RETRY: usize = 10_000;

/// Wrapper holding a CHFS file descriptor.
struct ChfsFd {
    fd: c_int,
}

// Safety: CHFS fds are simple integers; concurrent pread/pwrite with different
// offsets is safe.
unsafe impl Send for ChfsFd {}
unsafe impl Sync for ChfsFd {}

/// CHFS I/O backend implementing the Aiori trait.
pub struct ChfsBackend {
    initialized: bool,
}

impl ChfsBackend {
    pub fn new() -> Self {
        Self { initialized: false }
    }

    /// Convert IOR OpenFlags to libc O_* flags (CHFS uses standard POSIX flags).
    fn to_libc_flags(flags: OpenFlags) -> c_int {
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

        oflags
    }

    fn path_to_cstring(path: &str) -> Result<CString, IorError> {
        CString::new(path).map_err(|_| IorError::InvalidArgument)
    }

    fn errno() -> i32 {
        unsafe { *libc::__errno_location() }
    }
}

impl Drop for ChfsBackend {
    fn drop(&mut self) {
        if self.initialized {
            unsafe {
                chfs_term();
            }
            self.initialized = false;
        }
    }
}

impl Aiori for ChfsBackend {
    fn name(&self) -> &str {
        "CHFS"
    }

    fn configure(&mut self, options: &BackendOptions) -> Result<(), IorError> {
        let mut server: Option<String> = None;

        for (key, value) in options.for_prefix("chfs") {
            match key {
                "server" => {
                    server = Some(value.as_str().unwrap_or("").to_string());
                }
                "chunk_size" => {
                    let size = value.as_i64()? as c_int;
                    unsafe {
                        chfs_set_chunk_size(size);
                    }
                }
                "buf_size" => {
                    let size = value.as_i64()? as c_int;
                    unsafe {
                        chfs_set_buf_size(size);
                    }
                }
                unknown => {
                    eprintln!("WARNING: unknown CHFS option: chfs.{}", unknown);
                }
            }
        }

        // Initialize CHFS
        let rc = match server {
            Some(ref addr) if !addr.is_empty() => {
                let c_server = CString::new(addr.as_str())
                    .map_err(|_| IorError::InvalidArgument)?;
                unsafe { chfs_init(c_server.as_ptr()) }
            }
            _ => {
                // Use CHFS_SERVER environment variable
                unsafe { chfs_init(ptr::null()) }
            }
        };

        if rc != 0 {
            return Err(IorError::Io(libc::EIO));
        }

        self.initialized = true;
        Ok(())
    }

    fn create(&self, path: &str, flags: OpenFlags) -> Result<FileHandle, IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let oflags = Self::to_libc_flags(flags | OpenFlags::CREAT | OpenFlags::RDWR);
        let mode: libc::mode_t = 0o664;

        let fd = unsafe { chfs_create(cpath.as_ptr(), oflags, mode) };
        if fd < 0 {
            return Err(IorError::Io(Self::errno()));
        }

        Ok(FileHandle::new(ChfsFd { fd }))
    }

    fn open(&self, path: &str, flags: OpenFlags) -> Result<FileHandle, IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let oflags = Self::to_libc_flags(flags);

        let fd = unsafe { chfs_open(cpath.as_ptr(), oflags) };
        if fd < 0 {
            return Err(IorError::Io(Self::errno()));
        }

        Ok(FileHandle::new(ChfsFd { fd }))
    }

    fn close(&self, handle: FileHandle) -> Result<(), IorError> {
        let cf = handle
            .downcast_ref::<ChfsFd>()
            .ok_or(IorError::InvalidArgument)?;
        let rc = unsafe { chfs_close(cf.fd) };
        if rc < 0 {
            return Err(IorError::Io(Self::errno()));
        }
        Ok(())
    }

    fn delete(&self, path: &str) -> Result<(), IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { chfs_unlink(cpath.as_ptr()) };
        if rc < 0 {
            let errno = Self::errno();
            if errno != libc::ENOENT {
                return Err(IorError::Io(errno));
            }
        }
        Ok(())
    }

    fn fsync(&self, handle: &FileHandle) -> Result<(), IorError> {
        let cf = handle
            .downcast_ref::<ChfsFd>()
            .ok_or(IorError::InvalidArgument)?;
        let rc = unsafe { chfs_fsync(cf.fd) };
        if rc < 0 {
            return Err(IorError::Io(Self::errno()));
        }
        Ok(())
    }

    fn get_file_size(&self, path: &str) -> Result<i64, IorError> {
        let cpath = Self::path_to_cstring(path)?;
        unsafe {
            let mut st: libc::stat = std::mem::zeroed();
            let rc = chfs_stat(cpath.as_ptr(), &mut st);
            if rc < 0 {
                return Err(IorError::Io(Self::errno()));
            }
            Ok(st.st_size)
        }
    }

    fn access(&self, path: &str, mode: i32) -> Result<bool, IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { chfs_access(cpath.as_ptr(), mode) };
        Ok(rc == 0)
    }

    fn xfer_sync(
        &self,
        handle: &FileHandle,
        dir: XferDir,
        buf: *mut u8,
        len: i64,
        offset: i64,
    ) -> Result<i64, IorError> {
        let cf = handle
            .downcast_ref::<ChfsFd>()
            .ok_or(IorError::InvalidArgument)?;

        let mut remaining = len;
        let mut ptr = buf;
        let mut off = offset;
        let mut retries = 0;

        while remaining > 0 {
            let rc = match dir {
                XferDir::Write => unsafe {
                    chfs_pwrite(
                        cf.fd,
                        ptr as *const libc::c_void,
                        remaining as usize,
                        off as libc::off_t,
                    )
                },
                XferDir::Read => unsafe {
                    chfs_pread(
                        cf.fd,
                        ptr as *mut libc::c_void,
                        remaining as usize,
                        off as libc::off_t,
                    )
                },
            };

            if rc < 0 {
                return Err(IorError::Io(Self::errno()));
            }
            if rc == 0 {
                break;
            }

            let transferred = rc as i64;
            remaining -= transferred;
            ptr = unsafe { ptr.add(transferred as usize) };
            off += transferred;

            if remaining > 0 {
                retries += 1;
                if retries >= MAX_RETRY {
                    break;
                }
            }
        }

        Ok(len - remaining)
    }

    fn xfer_submit(
        &self,
        _handle: &FileHandle,
        _dir: XferDir,
        _buf: *mut u8,
        _len: i64,
        _offset: i64,
        _user_data: usize,
        _callback: XferCallback,
    ) -> Result<XferToken, IorError> {
        Err(IorError::NotSupported)
    }

    fn poll(&self, _max_completions: usize) -> Result<usize, IorError> {
        Err(IorError::NotSupported)
    }

    fn cancel(&self, _token: XferToken) -> Result<(), IorError> {
        Err(IorError::NotSupported)
    }

    fn mkdir(&self, path: &str, mode: u32) -> Result<(), IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { chfs_mkdir(cpath.as_ptr(), mode as libc::mode_t) };
        if rc < 0 {
            return Err(IorError::Io(Self::errno()));
        }
        Ok(())
    }

    fn rmdir(&self, path: &str) -> Result<(), IorError> {
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { chfs_rmdir(cpath.as_ptr()) };
        if rc < 0 {
            return Err(IorError::Io(Self::errno()));
        }
        Ok(())
    }

    fn stat(&self, path: &str) -> Result<StatResult, IorError> {
        let cpath = Self::path_to_cstring(path)?;
        unsafe {
            let mut st: libc::stat = std::mem::zeroed();
            let rc = chfs_stat(cpath.as_ptr(), &mut st);
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
}
