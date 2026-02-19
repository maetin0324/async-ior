use std::os::raw::{c_char, c_int, c_void};

/// Opaque BenchFS context handle.
pub enum BenchfsContext {}

/// Opaque BenchFS file handle.
pub enum BenchfsFileHandle {}

unsafe extern "C" {
    pub fn benchfs_init(
        node_id: *const c_char,
        registry_dir: *const c_char,
        data_dir: *const c_char,
        is_server: c_int,
        chunk_size: libc::size_t,
    ) -> *mut BenchfsContext;

    pub fn benchfs_finalize(ctx: *mut BenchfsContext);

    pub fn benchfs_create(
        ctx: *mut BenchfsContext,
        path: *const c_char,
        flags: c_int,
        mode: libc::mode_t,
    ) -> *mut BenchfsFileHandle;

    pub fn benchfs_open(
        ctx: *mut BenchfsContext,
        path: *const c_char,
        flags: c_int,
    ) -> *mut BenchfsFileHandle;

    pub fn benchfs_close(file: *mut BenchfsFileHandle) -> c_int;

    pub fn benchfs_write(
        file: *mut BenchfsFileHandle,
        buffer: *const c_void,
        size: libc::size_t,
        offset: libc::off_t,
    ) -> libc::ssize_t;

    pub fn benchfs_read(
        file: *mut BenchfsFileHandle,
        buffer: *mut c_void,
        size: libc::size_t,
        offset: libc::off_t,
    ) -> libc::ssize_t;

    pub fn benchfs_fsync(file: *mut BenchfsFileHandle) -> c_int;

    pub fn benchfs_remove(ctx: *mut BenchfsContext, path: *const c_char) -> c_int;

    pub fn benchfs_stat(
        ctx: *mut BenchfsContext,
        path: *const c_char,
        buf: *mut libc::stat,
    ) -> c_int;

    pub fn benchfs_get_file_size(
        ctx: *mut BenchfsContext,
        path: *const c_char,
    ) -> libc::off_t;

    pub fn benchfs_mkdir(
        ctx: *mut BenchfsContext,
        path: *const c_char,
        mode: libc::mode_t,
    ) -> c_int;

    pub fn benchfs_rmdir(ctx: *mut BenchfsContext, path: *const c_char) -> c_int;

    pub fn benchfs_rename(
        ctx: *mut BenchfsContext,
        oldpath: *const c_char,
        newpath: *const c_char,
    ) -> c_int;

    pub fn benchfs_access(
        ctx: *mut BenchfsContext,
        path: *const c_char,
        mode: c_int,
    ) -> c_int;
}
