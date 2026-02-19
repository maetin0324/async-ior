use clap::Parser;
use ior_core::IorParam;

/// Rust IOR benchmark — MPI-parallel I/O performance tester.
///
/// Reference: `parse_options.c:412-486`
#[derive(Parser, Debug)]
#[command(name = "ior-bench", about = "Async IOR benchmark")]
pub struct CliArgs {
    /// I/O backend API
    #[arg(short = 'a', long = "api", default_value = "POSIX")]
    pub api: String,

    /// Block size per task (supports k/m/g suffixes)
    #[arg(short = 'b', long = "block-size", default_value = "1m")]
    pub block_size: String,

    /// Number of segments
    #[arg(short = 's', long = "segment-count", default_value_t = 1)]
    pub segment_count: i64,

    /// Transfer size per I/O operation (supports k/m/g suffixes)
    #[arg(short = 't', long = "transfer-size", default_value = "256k")]
    pub transfer_size: String,

    /// Test file path
    #[arg(short = 'o', long = "test-file", default_value = "testFile")]
    pub test_file: String,

    /// Perform read phase
    #[arg(short = 'r', long = "read-file")]
    pub read_file: bool,

    /// Perform write phase
    #[arg(short = 'w', long = "write-file")]
    pub write_file: bool,

    /// Verify data after write
    #[arg(short = 'W', long = "check-write")]
    pub check_write: bool,

    /// Verify data after read
    #[arg(short = 'R', long = "check-read")]
    pub check_read: bool,

    /// File-per-process mode
    #[arg(short = 'F', long = "file-per-proc")]
    pub file_per_proc: bool,

    /// Random access offsets
    #[arg(short = 'z', long = "random-offset")]
    pub random_offset: bool,

    /// Number of repetitions
    #[arg(short = 'i', long = "repetitions", default_value_t = 1)]
    pub repetitions: i32,

    /// Delay between repetitions (seconds)
    #[arg(short = 'd', long = "inter-test-delay", default_value_t = 0)]
    pub inter_test_delay: i32,

    /// Stonewalling deadline (seconds, 0 = disabled)
    #[arg(short = 'D', long = "deadline", default_value_t = 0)]
    pub deadline_for_stonewalling: i32,

    /// fsync() after write phase
    #[arg(short = 'e', long = "fsync")]
    pub fsync: bool,

    /// fsync() after each write
    #[arg(short = 'Y', long = "fsync-per-write")]
    pub fsync_per_write: bool,

    /// Verbosity (repeat for higher levels)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Keep test file after completion
    #[arg(short = 'k', long = "keep-file")]
    pub keep_file: bool,

    /// Don't retry incomplete transfers
    #[arg(short = 'x', long = "single-xfer-attempt")]
    pub single_xfer_attempt: bool,

    /// Max time in minutes per test (0 = unlimited)
    #[arg(short = 'T', long = "max-time-duration", default_value_t = 0)]
    pub max_time_duration: i32,

    /// Use existing test file (don't delete before)
    #[arg(short = 'E', long = "use-existing")]
    pub use_existing: bool,

    /// Number of MPI tasks (-1 = use all)
    #[arg(short = 'N', long = "num-tasks", default_value_t = -1)]
    pub num_tasks: i32,

    /// Reorder tasks for read-back (deterministic shift)
    #[arg(short = 'C', long = "reorder-tasks")]
    pub reorder_tasks: bool,

    /// Random task reordering for reads
    #[arg(short = 'Z', long = "reorder-tasks-random")]
    pub reorder_tasks_random: bool,

    /// Enable intra-test barriers
    #[arg(short = 'g', long = "intra-test-barriers")]
    pub intra_test_barriers: bool,

    /// Use O_DIRECT (bypass OS cache)
    #[arg(long = "direct-io")]
    pub direct_io: bool,

    /// Async queue depth (1 = synchronous)
    #[arg(short = 'q', long = "queue-depth", default_value_t = 1)]
    pub queue_depth: i32,

    /// Output results as JSON to stdout (suppresses text output)
    #[arg(long = "json")]
    pub json: bool,

    /// Output results as JSON to file (text output still printed)
    #[arg(long = "json-file")]
    pub json_file: Option<String>,

    /// Timestamp signature value (seed for data pattern, C IOR: -G)
    #[arg(short = 'G', long = "timestamp-signature", default_value_t = 0)]
    pub timestamp_signature: i32,
}

/// Parse a size string with optional k/m/g/t suffix (case-insensitive).
pub fn parse_size(s: &str) -> i64 {
    let s = s.trim();
    if s.is_empty() {
        return 0;
    }

    let (num_str, multiplier) = match s.as_bytes().last() {
        Some(b'k' | b'K') => (&s[..s.len() - 1], 1024i64),
        Some(b'm' | b'M') => (&s[..s.len() - 1], 1024 * 1024),
        Some(b'g' | b'G') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        Some(b't' | b'T') => (&s[..s.len() - 1], 1024i64 * 1024 * 1024 * 1024),
        _ => (s, 1),
    };

    num_str
        .trim()
        .parse::<i64>()
        .unwrap_or_else(|_| panic!("invalid size: {s}"))
        * multiplier
}

impl CliArgs {
    /// Convert CLI arguments to an IorParam struct.
    pub fn into_ior_param(self) -> IorParam {
        let mut params = IorParam::default();

        params.set_api(&self.api);
        params.block_size = parse_size(&self.block_size);
        params.segment_count = self.segment_count;
        params.transfer_size = parse_size(&self.transfer_size);
        params.set_test_file_name(&self.test_file);

        // If neither -r nor -w specified, default to both
        if !self.read_file && !self.write_file {
            params.write_file = true;
            params.read_file = true;
        } else {
            params.write_file = self.write_file;
            params.read_file = self.read_file;
        }

        params.check_write = self.check_write;
        params.check_read = self.check_read;
        params.file_per_proc = self.file_per_proc;
        params.random_offset = self.random_offset;
        params.repetitions = self.repetitions;
        params.inter_test_delay = self.inter_test_delay;
        params.deadline_for_stonewalling = self.deadline_for_stonewalling;
        params.fsync = self.fsync;
        params.fsync_per_write = self.fsync_per_write;
        params.verbose = self.verbose as i32;
        params.keep_file = self.keep_file;
        params.single_xfer_attempt = self.single_xfer_attempt;
        params.max_time_duration = self.max_time_duration;
        params.use_existing_test_file = self.use_existing;
        params.num_tasks = self.num_tasks;
        params.reorder_tasks = self.reorder_tasks;
        params.reorder_tasks_random = self.reorder_tasks_random;
        params.intra_test_barriers = self.intra_test_barriers;
        params.direct_io = self.direct_io;
        params.queue_depth = self.queue_depth;
        params.time_stamp_signature_value = self.timestamp_signature;

        params
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1024"), 1024);
        assert_eq!(parse_size("1k"), 1024);
        assert_eq!(parse_size("1K"), 1024);
        assert_eq!(parse_size("1m"), 1_048_576);
        assert_eq!(parse_size("1M"), 1_048_576);
        assert_eq!(parse_size("1g"), 1_073_741_824);
        assert_eq!(parse_size("4k"), 4096);
        assert_eq!(parse_size("256k"), 262_144);
    }
}
