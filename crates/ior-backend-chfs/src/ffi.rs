use std::os::raw::{c_char, c_int};

unsafe extern "C" {
    pub fn chfs_init(server: *const c_char) -> c_int;
    pub fn chfs_term() -> c_int;

    pub fn chfs_create(path: *const c_char, flags: i32, mode: libc::mode_t) -> c_int;
    pub fn chfs_open(path: *const c_char, flags: i32) -> c_int;
    pub fn chfs_close(fd: c_int) -> c_int;

    pub fn chfs_pwrite(
        fd: c_int,
        buf: *const libc::c_void,
        size: libc::size_t,
        offset: libc::off_t,
    ) -> libc::ssize_t;

    pub fn chfs_pread(
        fd: c_int,
        buf: *mut libc::c_void,
        size: libc::size_t,
        offset: libc::off_t,
    ) -> libc::ssize_t;

    pub fn chfs_fsync(fd: c_int) -> c_int;
    pub fn chfs_unlink(path: *const c_char) -> c_int;

    pub fn chfs_stat(path: *const c_char, st: *mut libc::stat) -> c_int;

    pub fn chfs_mkdir(path: *const c_char, mode: libc::mode_t) -> c_int;
    pub fn chfs_rmdir(path: *const c_char) -> c_int;
    pub fn chfs_access(path: *const c_char, mode: c_int) -> c_int;

    pub fn chfs_set_chunk_size(chunk_size: c_int);
    pub fn chfs_set_buf_size(buf_size: c_int);
}
