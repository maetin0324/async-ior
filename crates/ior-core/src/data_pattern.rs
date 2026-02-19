/// Data pattern types for IOR buffer verification.
///
/// Reference: C IOR `utilities.c:94-170`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataPacketType {
    /// Timestamp-based pattern: `(rank << 32) | (seed + i)`
    Timestamp,
    /// Offset-based pattern: Timestamp + offset stamps at 512-word boundaries
    Offset,
}

impl Default for DataPacketType {
    fn default() -> Self {
        DataPacketType::Timestamp
    }
}

/// Generate the initial memory pattern for the write buffer.
///
/// Fills the buffer with 64-bit words: `(pretend_rank << 32) | (seed + word_index)`.
/// Called once at test start to set up the base pattern.
///
/// Reference: C IOR `utilities.c:94-112` (`generate_memory_pattern`)
pub fn generate_memory_pattern(
    buf: &mut [u8],
    seed: i32,
    pretend_rank: i32,
    _data_type: DataPacketType,
) {
    let words = buf.len() / 8;
    let rank_hi = (pretend_rank as u64) << 32;

    for i in 0..words {
        let val = rank_hi | ((seed as u64).wrapping_add(i as u64) & 0xFFFF_FFFF);
        buf[i * 8..(i + 1) * 8].copy_from_slice(&val.to_ne_bytes());
    }
}

/// Update the write buffer with offset-specific stamps before each transfer.
///
/// For `Offset` mode, stamps the transfer offset at every 512-word (4096-byte)
/// boundary within the buffer: `(offset * stride) | (rank << 32)`.
/// For `Timestamp` mode, this is a no-op since the base pattern is already per-rank unique.
///
/// Reference: C IOR `utilities.c:115-144` (`update_write_memory_pattern`)
pub fn update_write_pattern(
    offset: i64,
    buf: &mut [u8],
    _seed: i32,
    pretend_rank: i32,
    data_type: DataPacketType,
) {
    if data_type != DataPacketType::Offset {
        return;
    }

    let rank_hi = (pretend_rank as u64) << 32;
    let stride = 512; // words between stamps

    let words = buf.len() / 8;
    let mut pos = 0;
    let mut k: u64 = 0;

    while pos < words {
        let val = rank_hi | (((offset as u64).wrapping_mul(k.wrapping_add(1))) & 0xFFFF_FFFF);
        buf[pos * 8..(pos + 1) * 8].copy_from_slice(&val.to_ne_bytes());
        pos += stride;
        k += 1;
    }
}

/// Verify the buffer against the expected pattern. Returns number of errors.
///
/// Regenerates the expected pattern and compares word-by-word.
///
/// Reference: C IOR `utilities.c:147-170`
pub fn verify_pattern(
    offset: i64,
    buf: &[u8],
    seed: i32,
    pretend_rank: i32,
    data_type: DataPacketType,
) -> usize {
    let words = buf.len() / 8;
    let rank_hi = (pretend_rank as u64) << 32;
    let mut errors = 0;

    // Check base timestamp pattern
    for i in 0..words {
        let actual = u64::from_ne_bytes(buf[i * 8..(i + 1) * 8].try_into().unwrap());
        let expected = rank_hi | ((seed as u64).wrapping_add(i as u64) & 0xFFFF_FFFF);

        // For Offset mode, some positions are overwritten with offset stamps
        if data_type == DataPacketType::Offset {
            let stride = 512;
            if i % stride == 0 {
                let k = (i / stride) as u64;
                let expected_stamp =
                    rank_hi | (((offset as u64).wrapping_mul(k.wrapping_add(1))) & 0xFFFF_FFFF);
                if actual != expected_stamp {
                    errors += 1;
                }
                continue;
            }
        }

        if actual != expected {
            errors += 1;
        }
    }

    errors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_roundtrip() {
        let mut buf = vec![0u8; 4096];
        let seed = 42;
        let rank = 3;

        generate_memory_pattern(&mut buf, seed, rank, DataPacketType::Timestamp);
        let errors = verify_pattern(0, &buf, seed, rank, DataPacketType::Timestamp);
        assert_eq!(errors, 0);
    }

    #[test]
    fn test_offset_roundtrip() {
        let mut buf = vec![0u8; 8192];
        let seed = 7;
        let rank = 1;
        let offset = 4096;

        generate_memory_pattern(&mut buf, seed, rank, DataPacketType::Offset);
        update_write_pattern(offset, &mut buf, seed, rank, DataPacketType::Offset);
        let errors = verify_pattern(offset, &buf, seed, rank, DataPacketType::Offset);
        assert_eq!(errors, 0);
    }

    #[test]
    fn test_different_ranks() {
        let mut buf0 = vec![0u8; 256];
        let mut buf1 = vec![0u8; 256];
        let seed = 0;

        generate_memory_pattern(&mut buf0, seed, 0, DataPacketType::Timestamp);
        generate_memory_pattern(&mut buf1, seed, 1, DataPacketType::Timestamp);

        assert_ne!(buf0, buf1);
    }

    #[test]
    fn test_corruption_detected() {
        let mut buf = vec![0u8; 4096];
        let seed = 10;
        let rank = 2;

        generate_memory_pattern(&mut buf, seed, rank, DataPacketType::Timestamp);

        // Corrupt one byte
        buf[0] ^= 0xFF;

        let errors = verify_pattern(0, &buf, seed, rank, DataPacketType::Timestamp);
        assert!(errors > 0);
    }

    #[test]
    fn test_timestamp_no_update_needed() {
        let mut buf = vec![0u8; 4096];
        let seed = 5;
        let rank = 0;

        generate_memory_pattern(&mut buf, seed, rank, DataPacketType::Timestamp);
        let before = buf.clone();
        update_write_pattern(1024, &mut buf, seed, rank, DataPacketType::Timestamp);
        assert_eq!(buf, before, "Timestamp mode should not modify buffer in update");
    }
}
