//! C FFI bridge for registering external backends.
//!
//! Provides `AioriVTable` for C backends to expose their functionality,
//! and `CAioriAdapter` to wrap a vtable into a Rust `Aiori` trait object.

use std::cell::RefCell;
use std::ffi::{c_char, CStr, CString};
use std::os::raw::c_void;

use crate::error::IorError;
use crate::handle::{FileHandle, OpenFlags, StatResult, XferCallback, XferDir, XferToken};
use crate::Aiori;

/// C-compatible vtable for an AIORI backend.
#[repr(C)]
pub struct AioriVTable {
    pub name: *const c_char,
    pub create: extern "C" fn(*const c_char, u32) -> *mut c_void,
    pub open: extern "C" fn(*const c_char, u32) -> *mut c_void,
    pub close: extern "C" fn(*mut c_void) -> i32,
    pub delete: extern "C" fn(*const c_char) -> i32,
    pub fsync: extern "C" fn(*mut c_void) -> i32,
    pub get_file_size: extern "C" fn(*const c_char) -> i64,
    pub access: extern "C" fn(*const c_char, i32) -> i32,
    pub xfer_submit:
        extern "C" fn(*mut c_void, XferDir, *mut u8, i64, i64, usize, XferCallback) -> u64,
    pub poll: extern "C" fn(usize) -> i64,
    pub cancel: extern "C" fn(u64) -> i32,
    pub xfer_sync: Option<extern "C" fn(*mut c_void, XferDir, *mut u8, i64, i64) -> i64>,
    pub mkdir: Option<extern "C" fn(*const c_char, u32) -> i32>,
    pub rmdir: Option<extern "C" fn(*const c_char) -> i32>,
    pub stat: Option<extern "C" fn(*const c_char, *mut StatResult) -> i32>,
    pub rename: Option<extern "C" fn(*const c_char, *const c_char) -> i32>,
    pub mknod: Option<extern "C" fn(*const c_char) -> i32>,
}

// Safety: The vtable contains only function pointers and a const char pointer.
unsafe impl Send for AioriVTable {}
unsafe impl Sync for AioriVTable {}

/// Opaque handle wrapping a C-side fd pointer.
struct CFdHandle {
    ptr: *mut c_void,
}

// Safety: the C side is responsible for thread-safety of the fd.
unsafe impl Send for CFdHandle {}
unsafe impl Sync for CFdHandle {}

/// Adapter that wraps a C `AioriVTable` into a Rust `Aiori` trait object.
pub struct CAioriAdapter {
    vtable: &'static AioriVTable,
    name: String,
}

impl CAioriAdapter {
    /// Create a new adapter from a static vtable reference.
    ///
    /// # Safety
    /// The vtable must remain valid for the lifetime of this adapter.
    /// All function pointers must be valid C functions matching the signatures.
    pub unsafe fn new(vtable: &'static AioriVTable) -> Self {
        let name = if vtable.name.is_null() {
            "C-backend".to_string()
        } else {
            unsafe {
                CStr::from_ptr(vtable.name)
                    .to_string_lossy()
                    .into_owned()
            }
        };
        Self { vtable, name }
    }
}

impl Aiori for CAioriAdapter {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, path: &str, flags: OpenFlags) -> Result<FileHandle, IorError> {
        let cpath = CString::new(path).map_err(|_| IorError::InvalidArgument)?;
        let ptr = (self.vtable.create)(cpath.as_ptr(), flags.bits());
        if ptr.is_null() {
            return Err(IorError::Unknown);
        }
        Ok(FileHandle::new(CFdHandle { ptr }))
    }

    fn open(&self, path: &str, flags: OpenFlags) -> Result<FileHandle, IorError> {
        let cpath = CString::new(path).map_err(|_| IorError::InvalidArgument)?;
        let ptr = (self.vtable.open)(cpath.as_ptr(), flags.bits());
        if ptr.is_null() {
            return Err(IorError::Unknown);
        }
        Ok(FileHandle::new(CFdHandle { ptr }))
    }

    fn close(&self, handle: FileHandle) -> Result<(), IorError> {
        let cfd = handle
            .downcast_ref::<CFdHandle>()
            .ok_or(IorError::InvalidArgument)?;
        let rc = (self.vtable.close)(cfd.ptr);
        if rc != 0 {
            return Err(IorError::Io(rc));
        }
        Ok(())
    }

    fn delete(&self, path: &str) -> Result<(), IorError> {
        let cpath = CString::new(path).map_err(|_| IorError::InvalidArgument)?;
        let rc = (self.vtable.delete)(cpath.as_ptr());
        if rc != 0 {
            return Err(IorError::Io(rc));
        }
        Ok(())
    }

    fn fsync(&self, handle: &FileHandle) -> Result<(), IorError> {
        let cfd = handle
            .downcast_ref::<CFdHandle>()
            .ok_or(IorError::InvalidArgument)?;
        let rc = (self.vtable.fsync)(cfd.ptr);
        if rc != 0 {
            return Err(IorError::Io(rc));
        }
        Ok(())
    }

    fn get_file_size(&self, path: &str) -> Result<i64, IorError> {
        let cpath = CString::new(path).map_err(|_| IorError::InvalidArgument)?;
        let size = (self.vtable.get_file_size)(cpath.as_ptr());
        if size < 0 {
            return Err(IorError::Io(size as i32));
        }
        Ok(size)
    }

    fn access(&self, path: &str, mode: i32) -> Result<bool, IorError> {
        let cpath = CString::new(path).map_err(|_| IorError::InvalidArgument)?;
        let rc = (self.vtable.access)(cpath.as_ptr(), mode);
        Ok(rc == 0)
    }

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
        let cfd = handle
            .downcast_ref::<CFdHandle>()
            .ok_or(IorError::InvalidArgument)?;
        let raw_token =
            (self.vtable.xfer_submit)(cfd.ptr, dir, buf, len, offset, user_data, callback);
        if raw_token == 0 {
            return Err(IorError::Unknown);
        }
        Ok(XferToken(raw_token))
    }

    fn poll(&self, max_completions: usize) -> Result<usize, IorError> {
        let rc = (self.vtable.poll)(max_completions);
        if rc < 0 {
            return Err(IorError::Io(rc as i32));
        }
        Ok(rc as usize)
    }

    fn cancel(&self, token: XferToken) -> Result<(), IorError> {
        let rc = (self.vtable.cancel)(token.0);
        if rc != 0 {
            return Err(IorError::Io(rc));
        }
        Ok(())
    }

    fn mkdir(&self, path: &str, mode: u32) -> Result<(), IorError> {
        if let Some(mkdir_fn) = self.vtable.mkdir {
            let cpath = CString::new(path).map_err(|_| IorError::InvalidArgument)?;
            let rc = mkdir_fn(cpath.as_ptr(), mode);
            if rc != 0 {
                return Err(IorError::Io(rc));
            }
            Ok(())
        } else {
            Err(IorError::NotSupported)
        }
    }

    fn rmdir(&self, path: &str) -> Result<(), IorError> {
        if let Some(rmdir_fn) = self.vtable.rmdir {
            let cpath = CString::new(path).map_err(|_| IorError::InvalidArgument)?;
            let rc = rmdir_fn(cpath.as_ptr());
            if rc != 0 {
                return Err(IorError::Io(rc));
            }
            Ok(())
        } else {
            Err(IorError::NotSupported)
        }
    }

    fn stat(&self, path: &str) -> Result<StatResult, IorError> {
        if let Some(stat_fn) = self.vtable.stat {
            let cpath = CString::new(path).map_err(|_| IorError::InvalidArgument)?;
            let mut result: StatResult = unsafe { std::mem::zeroed() };
            let rc = stat_fn(cpath.as_ptr(), &mut result);
            if rc != 0 {
                return Err(IorError::Io(rc));
            }
            Ok(result)
        } else {
            Err(IorError::NotSupported)
        }
    }

    fn rename(&self, old_path: &str, new_path: &str) -> Result<(), IorError> {
        if let Some(rename_fn) = self.vtable.rename {
            let cold = CString::new(old_path).map_err(|_| IorError::InvalidArgument)?;
            let cnew = CString::new(new_path).map_err(|_| IorError::InvalidArgument)?;
            let rc = rename_fn(cold.as_ptr(), cnew.as_ptr());
            if rc != 0 {
                return Err(IorError::Io(rc));
            }
            Ok(())
        } else {
            Err(IorError::NotSupported)
        }
    }

    fn mknod(&self, path: &str) -> Result<(), IorError> {
        if let Some(mknod_fn) = self.vtable.mknod {
            let cpath = CString::new(path).map_err(|_| IorError::InvalidArgument)?;
            let rc = mknod_fn(cpath.as_ptr());
            if rc != 0 {
                return Err(IorError::Io(rc));
            }
            Ok(())
        } else {
            Err(IorError::NotSupported)
        }
    }

    fn xfer_sync(
        &self,
        handle: &FileHandle,
        dir: XferDir,
        buf: *mut u8,
        len: i64,
        offset: i64,
    ) -> Result<i64, IorError> {
        // Use direct sync path if available, otherwise fall back to default
        if let Some(sync_fn) = self.vtable.xfer_sync {
            let cfd = handle
                .downcast_ref::<CFdHandle>()
                .ok_or(IorError::InvalidArgument)?;
            let rc = sync_fn(cfd.ptr, dir, buf, len, offset);
            if rc < 0 {
                return Err(IorError::Io(rc as i32));
            }
            Ok(rc)
        } else {
            // Default: submit + poll loop (from trait default)
            Aiori::xfer_sync(self, handle, dir, buf, len, offset)
        }
    }
}

// ============================================================================
// Global backend registry
// ============================================================================

thread_local! {
    /// Per-thread registry of C backends registered via FFI.
    /// Only the main thread calls register/find, so no cross-thread sharing needed.
    static REGISTERED_BACKENDS: RefCell<Vec<&'static AioriVTable>> = RefCell::new(Vec::new());
}

/// Register a C backend vtable. Called from C code.
///
/// # Safety
/// The vtable pointer must point to a valid `AioriVTable` with
/// a `'static` lifetime.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ior_register_backend(vtable: *const AioriVTable) {
    if vtable.is_null() {
        return;
    }
    let vtable_ref: &'static AioriVTable = unsafe { &*vtable };
    REGISTERED_BACKENDS.with(|backends| {
        backends.borrow_mut().push(vtable_ref);
    });
}

/// Look up a registered C backend by name.
pub fn find_registered_backend(name: &str) -> Option<CAioriAdapter> {
    REGISTERED_BACKENDS.with(|backends| {
        let backends = backends.borrow();
        for vtable in backends.iter() {
            if !vtable.name.is_null() {
                let cname = unsafe { CStr::from_ptr(vtable.name) };
                if cname.to_string_lossy() == name {
                    return Some(unsafe { CAioriAdapter::new(vtable) });
                }
            }
        }
        None
    })
}
