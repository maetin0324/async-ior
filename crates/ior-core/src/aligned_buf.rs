use std::alloc::{Layout, alloc_zeroed, dealloc};

/// Page-aligned buffer for O_DIRECT I/O.
///
/// Allocates memory aligned to the system page size (typically 4096 bytes),
/// which is required for O_DIRECT to avoid EINVAL errors.
///
/// Reference: C IOR `utilities.c:1040` (`aligned_buffer_alloc`)
pub struct AlignedBuffer {
    ptr: *mut u8,
    layout: Layout,
    len: usize,
}

impl AlignedBuffer {
    /// Create a new zero-filled buffer aligned to the system page size.
    pub fn new(size: usize) -> Self {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let layout = Layout::from_size_align(size, page_size)
            .expect("invalid layout for aligned buffer");
        let ptr = unsafe { alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Self { ptr, layout, len: size }
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl std::ops::Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl std::ops::DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

// SAFETY: The buffer owns its allocation and raw pointer operations
// are confined to its own data. No shared mutable state.
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_alignment() {
        let buf = AlignedBuffer::new(4096);
        assert_eq!(buf.as_ptr() as usize % 4096, 0);

        let buf2 = AlignedBuffer::new(8192);
        assert_eq!(buf2.as_ptr() as usize % 4096, 0);

        let buf3 = AlignedBuffer::new(1000);
        assert_eq!(buf3.as_ptr() as usize % 4096, 0);
    }

    #[test]
    fn test_zero_filled() {
        let buf = AlignedBuffer::new(4096);
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_deref_mut_roundtrip() {
        let mut buf = AlignedBuffer::new(256);
        for (i, byte) in buf.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        for (i, &byte) in buf.iter().enumerate() {
            assert_eq!(byte, (i % 256) as u8);
        }
    }

    #[test]
    fn test_len() {
        let buf = AlignedBuffer::new(1234);
        assert_eq!(buf.len(), 1234);
        assert!(!buf.is_empty());
    }
}
