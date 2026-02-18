use clap::Parser;

use crate::params::MdtestParam;

/// Rust mdtest benchmark — MPI-parallel metadata performance tester.
///
/// Reference: `mdtest.c:2264-2318`
#[derive(Parser, Debug)]
#[command(name = "mdtest-bench", about = "Metadata benchmark (mdtest)")]
pub struct CliArgs {
    /// I/O backend API
    #[arg(short = 'a', long = "api", default_value = "POSIX")]
    pub api: String,

    /// Branch factor of hierarchical directory structure
    #[arg(short = 'b', long = "branch-factor", default_value_t = 1)]
    pub branch_factor: u32,

    /// Test directory path
    #[arg(short = 'd', long = "test-dir", default_value = "./out")]
    pub test_dir: String,

    /// Disable barriers between phases
    #[arg(short = 'B', long = "no-barriers")]
    pub no_barriers: bool,

    /// Create only (no stat/read/remove)
    #[arg(short = 'C', long = "create-only")]
    pub create_only: bool,

    /// Stat only
    #[arg(short = 'T', long = "stat-only")]
    pub stat_only: bool,

    /// Read only
    #[arg(short = 'E', long = "read-only")]
    pub read_only: bool,

    /// Remove only
    #[arg(short = 'r', long = "remove-only")]
    pub remove_only: bool,

    /// Directories only (no files)
    #[arg(short = 'D', long = "dirs-only")]
    pub dirs_only: bool,

    /// Read bytes per file
    #[arg(short = 'e', long = "read-bytes", default_value_t = 0)]
    pub read_bytes: u64,

    /// First number of tasks
    #[arg(short = 'f', long = "first", default_value_t = 0)]
    pub first: i32,

    /// Files only (no directories)
    #[arg(short = 'F', long = "files-only")]
    pub files_only: bool,

    /// Number of iterations
    #[arg(short = 'i', long = "iterations", default_value_t = 1)]
    pub iterations: i32,

    /// Items per directory
    #[arg(short = 'I', long = "items-per-dir", default_value_t = 0)]
    pub items_per_dir: u64,

    /// Last number of tasks
    #[arg(short = 'l', long = "last", default_value_t = 0)]
    pub last: i32,

    /// Leaf only (create items in leaf nodes only)
    #[arg(short = 'L', long = "leaf-only")]
    pub leaf_only: bool,

    /// Total items per process
    #[arg(short = 'n', long = "items", default_value_t = 0)]
    pub items: u64,

    /// Neighbor stride
    #[arg(short = 'N', long = "nstride", default_value_t = 0)]
    pub nstride: i32,

    /// Stride between task counts
    #[arg(short = 's', long = "stride", default_value_t = 1)]
    pub stride: i32,

    /// Shared file
    #[arg(short = 'S', long = "shared-file")]
    pub shared_file: bool,

    /// Collective creates
    #[arg(short = 'c', long = "collective-creates")]
    pub collective_creates: bool,

    /// Unique directory per task
    #[arg(short = 'u', long = "unique-dir-per-task")]
    pub unique_dir_per_task: bool,

    /// Verbosity (repeat for higher levels)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Write bytes per file
    #[arg(short = 'w', long = "write-bytes", default_value_t = 0)]
    pub write_bytes: u64,

    /// Stonewall timer (seconds, 0 = disabled)
    #[arg(short = 'W', long = "stonewall-timer", default_value_t = 0)]
    pub stone_wall_timer: i32,

    /// Depth of directory tree
    #[arg(short = 'z', long = "depth", default_value_t = 0)]
    pub depth: i32,

    /// Print time instead of rate
    #[arg(short = 'Z', long = "print-time")]
    pub print_time: bool,

    /// Random stat access order
    #[arg(short = 'R', long = "random")]
    pub random: bool,

    /// Use mknod for file creation
    #[arg(short = 'k', long = "make-node")]
    pub make_node: bool,

    /// Sync file after write
    #[arg(short = 'y', long = "sync-file")]
    pub sync_file: bool,

    /// Rename directories in directory test
    #[arg(long = "rename-dirs")]
    pub rename_dirs: bool,
}

impl CliArgs {
    /// Convert CLI arguments to MdtestParam.
    pub fn into_mdtest_param(self) -> MdtestParam {
        let mut p = MdtestParam::default();

        p.api = self.api;
        p.branch_factor = self.branch_factor;
        p.test_dir = self.test_dir;
        p.barriers = !self.no_barriers;
        p.depth = self.depth;
        p.items = self.items;
        p.items_per_dir = self.items_per_dir;
        p.leaf_only = self.leaf_only;
        p.unique_dir_per_task = self.unique_dir_per_task;
        p.collective_creates = self.collective_creates;
        p.shared_file = self.shared_file;
        p.nstride = self.nstride;
        p.make_node = self.make_node;
        p.write_bytes = self.write_bytes;
        p.read_bytes = self.read_bytes;
        p.sync_file = self.sync_file;
        p.iterations = self.iterations;
        p.stone_wall_timer_seconds = self.stone_wall_timer;
        p.first = self.first;
        p.last = self.last;
        p.stride = self.stride;
        p.verbose = self.verbose as i32;
        p.print_time = self.print_time;
        p.rename_dirs = self.rename_dirs;

        // Default: if none of -C -T -E -r specified, enable all
        if !self.create_only && !self.stat_only && !self.read_only && !self.remove_only {
            p.create_only = true;
            p.stat_only = true;
            p.read_only = true;
            p.remove_only = true;
        } else {
            p.create_only = self.create_only;
            p.stat_only = self.stat_only;
            p.read_only = self.read_only;
            p.remove_only = self.remove_only;
        }

        // Default: if neither -D nor -F specified, enable both
        if !self.dirs_only && !self.files_only {
            p.dirs_only = true;
            p.files_only = true;
        } else {
            p.dirs_only = self.dirs_only;
            p.files_only = self.files_only;
        }

        // Random seed
        if self.random {
            p.random_seed = 1; // non-zero enables random
        }

        p
    }
}
