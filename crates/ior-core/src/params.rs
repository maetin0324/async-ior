use crate::data_pattern::DataPacketType;

/// Maximum length for test file name
pub const MAX_STR: usize = 1024;
/// Maximum length for API name
pub const MAX_API: usize = 64;

/// IOR benchmark parameters, matching C IOR's `IOR_param_t`.
///
/// Reference: `ior.h:77-167`, defaults from `ior.c:301-337`
#[repr(C)]
pub struct IorParam {
    // --- Transfer ---
    /// Contiguous bytes to write per task (default: 1 MiB)
    pub block_size: i64,
    /// Size of each transfer in bytes (default: 256 KiB)
    pub transfer_size: i64,
    /// Number of segments (default: 1)
    pub segment_count: i64,

    // --- Access ---
    /// Perform write phase
    pub write_file: bool,
    /// Perform read phase
    pub read_file: bool,
    /// One file per process (vs shared file)
    pub file_per_proc: bool,
    /// Use random offsets instead of sequential
    pub random_offset: bool,
    /// Verify data after write
    pub check_write: bool,
    /// Verify data after read
    pub check_read: bool,
    /// Random seed for data generation (-1 = use default)
    pub random_seed: i32,

    // --- Timing ---
    /// Number of test repetitions
    pub repetitions: i32,
    /// Delay between repetitions in seconds
    pub inter_test_delay: i32,
    /// Max seconds for stonewalling (0 = disabled)
    pub deadline_for_stonewalling: i32,
    /// Max time in minutes to run each test (0 = unlimited)
    pub max_time_duration: i32,
    /// Minimum runtime in seconds (0 = disabled)
    pub min_time_duration: i32,
    /// Wear out stonewalling: align all ranks to max pairs
    pub stonewall_wear_out: bool,
    /// Iteration count for stonewalling wear-out
    pub stonewall_wear_out_iterations: u64,

    // --- I/O behavior ---
    /// Verbosity level (0-5)
    pub verbose: i32,
    /// Keep test file after completion
    pub keep_file: bool,
    /// fsync() after write phase
    pub fsync: bool,
    /// fsync() after each individual write
    pub fsync_per_write: bool,
    /// Don't retry incomplete transfers
    pub single_xfer_attempt: bool,
    /// Don't delete test file before access
    pub use_existing_test_file: bool,

    // --- File identification ---
    /// Test file name
    pub test_file_name: [u8; MAX_STR],
    /// API name (e.g., "POSIX")
    pub api: [u8; MAX_API],

    // --- MPI ---
    /// Number of tasks (-1 = from MPI)
    pub num_tasks: i32,
    /// Number of nodes (-1 = from MPI)
    pub num_nodes: i32,
    /// Tasks on node 0 (-1 = auto-detect)
    pub num_tasks_on_node0: i32,
    /// Task offset for reorder reads (default: 1)
    pub task_per_node_offset: i32,
    /// Reorder tasks for read-back (deterministic shift)
    pub reorder_tasks: bool,
    /// Reorder tasks randomly for read-back
    pub reorder_tasks_random: bool,
    /// Seed for random task reordering
    pub reorder_tasks_random_seed: i32,
    /// Enable barriers between open/io and io/close
    pub intra_test_barriers: bool,

    // --- Async ---
    /// Number of outstanding async I/O operations (1 = sync)
    pub queue_depth: i32,

    // --- Backend ---
    /// Use O_DIRECT for bypass of OS caches
    pub direct_io: bool,

    // --- Data pattern ---
    /// Data packet type for write/verify (default: Timestamp)
    pub data_packet_type: DataPacketType,
    /// Timestamp signature seed value (default: 0)
    pub time_stamp_signature_value: i32,
}

impl Default for IorParam {
    fn default() -> Self {
        let mut test_file_name = [0u8; MAX_STR];
        let default_name = b"testFile";
        test_file_name[..default_name.len()].copy_from_slice(default_name);

        let mut api = [0u8; MAX_API];
        let default_api = b"POSIX";
        api[..default_api.len()].copy_from_slice(default_api);

        Self {
            block_size: 1_048_576,
            transfer_size: 262_144,
            segment_count: 1,

            write_file: false,
            read_file: false,
            file_per_proc: false,
            random_offset: false,
            check_write: false,
            check_read: false,
            random_seed: -1,

            repetitions: 1,
            inter_test_delay: 0,
            deadline_for_stonewalling: 0,
            max_time_duration: 0,
            min_time_duration: 0,
            stonewall_wear_out: false,
            stonewall_wear_out_iterations: 0,

            verbose: 0,
            keep_file: false,
            fsync: false,
            fsync_per_write: false,
            single_xfer_attempt: false,
            use_existing_test_file: false,

            test_file_name,
            api,

            num_tasks: -1,
            num_nodes: -1,
            num_tasks_on_node0: -1,
            task_per_node_offset: 1,
            reorder_tasks: false,
            reorder_tasks_random: false,
            reorder_tasks_random_seed: 0,
            intra_test_barriers: false,

            queue_depth: 1,
            direct_io: false,

            data_packet_type: DataPacketType::Timestamp,
            time_stamp_signature_value: 0,
        }
    }
}

impl IorParam {
    /// Get the test file name as a string slice.
    pub fn test_file_name_str(&self) -> &str {
        let len = self
            .test_file_name
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.test_file_name.len());
        std::str::from_utf8(&self.test_file_name[..len]).unwrap_or("testFile")
    }

    /// Get the API name as a string slice.
    pub fn api_str(&self) -> &str {
        let len = self
            .api
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(self.api.len());
        std::str::from_utf8(&self.api[..len]).unwrap_or("POSIX")
    }

    /// Set the test file name from a string.
    pub fn set_test_file_name(&mut self, name: &str) {
        self.test_file_name = [0u8; MAX_STR];
        let bytes = name.as_bytes();
        let len = bytes.len().min(MAX_STR - 1);
        self.test_file_name[..len].copy_from_slice(&bytes[..len]);
    }

    /// Set the API name from a string.
    pub fn set_api(&mut self, name: &str) {
        self.api = [0u8; MAX_API];
        let bytes = name.as_bytes();
        let len = bytes.len().min(MAX_API - 1);
        self.api[..len].copy_from_slice(&bytes[..len]);
    }

    /// Calculate expected aggregate file size.
    /// Reference: `ior.c` expected file size calculation
    pub fn expected_agg_file_size(&self) -> i64 {
        if self.file_per_proc {
            self.block_size * self.segment_count * self.num_tasks as i64
        } else {
            self.block_size * self.segment_count * self.num_tasks as i64
        }
    }
}
