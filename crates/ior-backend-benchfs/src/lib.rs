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

/// Wrapper holding a BenchFS file pointer.
struct BenchfsFile {
    ptr: *mut BenchfsFileHandle,
}

// Safety: BenchFS file handles are thread-safe opaque pointers.
unsafe impl Send for BenchfsFile {}
unsafe impl Sync for BenchfsFile {}

/// BenchFS I/O backend implementing the Aiori trait.
pub struct BenchfsBackend {
    ctx: *mut BenchfsContext,
    // Configuration (used for lazy init)
    registry_dir: String,
    data_dir: String,
    chunk_size: usize,
    is_server: bool,
    node_id: String,
}

// Safety: BenchFS context is thread-safe.
unsafe impl Send for BenchfsBackend {}
unsafe impl Sync for BenchfsBackend {}

impl BenchfsBackend {
    pub fn new() -> Self {
        Self {
            ctx: ptr::null_mut(),
            registry_dir: String::new(),
            data_dir: String::new(),
            chunk_size: 0,
            is_server: false,
            node_id: String::from("0"),
        }
    }

    /// Initialize the BenchFS context if not already done.
    fn ensure_init(&self) -> Result<*mut BenchfsContext, IorError> {
        if !self.ctx.is_null() {
            return Ok(self.ctx);
        }
        Err(IorError::NotSupported)
    }

    /// Perform lazy initialization, called from configure().
    fn init_ctx(&mut self) -> Result<(), IorError> {
        if !self.ctx.is_null() {
            return Ok(());
        }

        let c_node_id = CString::new(self.node_id.as_str())
            .map_err(|_| IorError::InvalidArgument)?;
        let c_registry = CString::new(self.registry_dir.as_str())
            .map_err(|_| IorError::InvalidArgument)?;
        let c_data = CString::new(self.data_dir.as_str())
            .map_err(|_| IorError::InvalidArgument)?;

        let ctx = unsafe {
            benchfs_init(
                c_node_id.as_ptr(),
                c_registry.as_ptr(),
                c_data.as_ptr(),
                if self.is_server { 1 } else { 0 },
                self.chunk_size,
            )
        };

        if ctx.is_null() {
            return Err(IorError::Io(libc::EIO));
        }

        self.ctx = ctx;
        Ok(())
    }

    /// Convert IOR OpenFlags to libc O_* flags (BenchFS uses Linux-compatible values).
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
}

impl Drop for BenchfsBackend {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            unsafe {
                benchfs_finalize(self.ctx);
            }
            self.ctx = ptr::null_mut();
        }
    }
}

impl Aiori for BenchfsBackend {
    fn name(&self) -> &str {
        "BENCHFS"
    }

    fn configure(&mut self, options: &BackendOptions) -> Result<(), IorError> {
        for (key, value) in options.for_prefix("benchfs") {
            match key {
                "registry" => {
                    self.registry_dir = value.as_str().unwrap_or("").to_string();
                }
                "data_dir" => {
                    self.data_dir = value.as_str().unwrap_or("").to_string();
                }
                "chunk_size" => {
                    self.chunk_size = value.as_i64()? as usize;
                }
                "server" => {
                    self.is_server = value.as_bool();
                }
                "node_id" => {
                    self.node_id = value.as_str().unwrap_or("0").to_string();
                }
                unknown => {
                    eprintln!("WARNING: unknown BENCHFS option: benchfs.{}", unknown);
                }
            }
        }

        // Initialize context after configuration
        self.init_ctx()
    }

    fn create(&self, path: &str, flags: OpenFlags) -> Result<FileHandle, IorError> {
        let ctx = self.ensure_init()?;
        let cpath = Self::path_to_cstring(path)?;
        let oflags = Self::to_libc_flags(flags | OpenFlags::CREAT | OpenFlags::RDWR);
        let mode: libc::mode_t = 0o664;

        let file = unsafe { benchfs_create(ctx, cpath.as_ptr(), oflags, mode) };
        if file.is_null() {
            return Err(IorError::Io(libc::EIO));
        }

        Ok(FileHandle::new(BenchfsFile { ptr: file }))
    }

    fn open(&self, path: &str, flags: OpenFlags) -> Result<FileHandle, IorError> {
        let ctx = self.ensure_init()?;
        let cpath = Self::path_to_cstring(path)?;
        let oflags = Self::to_libc_flags(flags);

        let file = unsafe { benchfs_open(ctx, cpath.as_ptr(), oflags) };
        if file.is_null() {
            return Err(IorError::Io(libc::EIO));
        }

        Ok(FileHandle::new(BenchfsFile { ptr: file }))
    }

    fn close(&self, handle: FileHandle) -> Result<(), IorError> {
        let bf = handle
            .downcast_ref::<BenchfsFile>()
            .ok_or(IorError::InvalidArgument)?;
        let rc = unsafe { benchfs_close(bf.ptr) };
        if rc != 0 {
            return Err(IorError::Io(libc::EIO));
        }
        Ok(())
    }

    fn delete(&self, path: &str) -> Result<(), IorError> {
        let ctx = self.ensure_init()?;
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { benchfs_remove(ctx, cpath.as_ptr()) };
        if rc != 0 {
            return Err(IorError::Io(libc::EIO));
        }
        Ok(())
    }

    fn fsync(&self, handle: &FileHandle) -> Result<(), IorError> {
        let bf = handle
            .downcast_ref::<BenchfsFile>()
            .ok_or(IorError::InvalidArgument)?;
        let rc = unsafe { benchfs_fsync(bf.ptr) };
        if rc != 0 {
            return Err(IorError::Io(libc::EIO));
        }
        Ok(())
    }

    fn get_file_size(&self, path: &str) -> Result<i64, IorError> {
        let ctx = self.ensure_init()?;
        let cpath = Self::path_to_cstring(path)?;
        let size = unsafe { benchfs_get_file_size(ctx, cpath.as_ptr()) };
        if size < 0 {
            return Err(IorError::Io(libc::EIO));
        }
        Ok(size)
    }

    fn access(&self, path: &str, mode: i32) -> Result<bool, IorError> {
        let ctx = self.ensure_init()?;
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { benchfs_access(ctx, cpath.as_ptr(), mode) };
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
        let bf = handle
            .downcast_ref::<BenchfsFile>()
            .ok_or(IorError::InvalidArgument)?;

        let mut remaining = len;
        let mut ptr = buf;
        let mut off = offset;
        let mut retries = 0;

        while remaining > 0 {
            let rc = match dir {
                XferDir::Write => unsafe {
                    benchfs_write(
                        bf.ptr,
                        ptr as *const libc::c_void,
                        remaining as usize,
                        off as libc::off_t,
                    )
                },
                XferDir::Read => unsafe {
                    benchfs_read(
                        bf.ptr,
                        ptr as *mut libc::c_void,
                        remaining as usize,
                        off as libc::off_t,
                    )
                },
            };

            if rc < 0 {
                return Err(IorError::Io(libc::EIO));
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
        let ctx = self.ensure_init()?;
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { benchfs_mkdir(ctx, cpath.as_ptr(), mode as libc::mode_t) };
        if rc != 0 {
            return Err(IorError::Io(libc::EIO));
        }
        Ok(())
    }

    fn rmdir(&self, path: &str) -> Result<(), IorError> {
        let ctx = self.ensure_init()?;
        let cpath = Self::path_to_cstring(path)?;
        let rc = unsafe { benchfs_rmdir(ctx, cpath.as_ptr()) };
        if rc != 0 {
            return Err(IorError::Io(libc::EIO));
        }
        Ok(())
    }

    fn stat(&self, path: &str) -> Result<StatResult, IorError> {
        let ctx = self.ensure_init()?;
        let cpath = Self::path_to_cstring(path)?;
        unsafe {
            let mut st: libc::stat = std::mem::zeroed();
            let rc = benchfs_stat(ctx, cpath.as_ptr(), &mut st);
            if rc != 0 {
                return Err(IorError::Io(libc::EIO));
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

    fn rename(&self, old_path: &str, new_path: &str) -> Result<(), IorError> {
        let ctx = self.ensure_init()?;
        let cold = Self::path_to_cstring(old_path)?;
        let cnew = Self::path_to_cstring(new_path)?;
        let rc = unsafe { benchfs_rename(ctx, cold.as_ptr(), cnew.as_ptr()) };
        if rc != 0 {
            return Err(IorError::Io(libc::EIO));
        }
        Ok(())
    }
}
