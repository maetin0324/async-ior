/// Mdtest benchmark parameters.
///
/// Reference: `mdtest.c:101-190` (mdtest_options_t)
pub struct MdtestParam {
    // Tree structure
    pub branch_factor: u32,
    pub depth: i32,
    pub num_dirs_in_tree: u64,
    pub items: u64,
    pub items_per_dir: u64,
    pub num_dirs_in_tree_calc: u64,
    pub directory_loops: i32,

    // Phase control
    pub create_only: bool,
    pub stat_only: bool,
    pub read_only: bool,
    pub remove_only: bool,
    pub dirs_only: bool,
    pub files_only: bool,
    pub leaf_only: bool,
    pub rename_dirs: bool,

    // Access patterns
    pub unique_dir_per_task: bool,
    pub collective_creates: bool,
    pub shared_file: bool,
    pub random_seed: i32,
    pub nstride: i32,
    pub make_node: bool,

    // Data
    pub write_bytes: u64,
    pub read_bytes: u64,
    pub sync_file: bool,

    // Timing
    pub iterations: i32,
    pub stone_wall_timer_seconds: i32,
    pub barriers: bool,

    // Task scaling
    pub first: i32,
    pub last: i32,
    pub stride: i32,

    // Output
    pub verbose: i32,
    pub print_time: bool,

    // Paths
    pub test_dir: String,
    pub api: String,

    // MPI (computed)
    pub num_tasks: i32,
}

impl Default for MdtestParam {
    fn default() -> Self {
        Self {
            branch_factor: 1,
            depth: 0,
            num_dirs_in_tree: 0,
            items: 0,
            items_per_dir: 0,
            num_dirs_in_tree_calc: 0,
            directory_loops: 1,

            create_only: false,
            stat_only: false,
            read_only: false,
            remove_only: false,
            dirs_only: false,
            files_only: false,
            leaf_only: false,
            rename_dirs: false,

            unique_dir_per_task: false,
            collective_creates: false,
            shared_file: false,
            random_seed: 0,
            nstride: 0,
            make_node: false,

            write_bytes: 0,
            read_bytes: 0,
            sync_file: false,

            iterations: 1,
            stone_wall_timer_seconds: 0,
            barriers: true,

            first: 0,
            last: 0,
            stride: 1,

            verbose: 0,
            print_time: false,

            test_dir: "./out".to_string(),
            api: "POSIX".to_string(),

            num_tasks: 0,
        }
    }
}

impl MdtestParam {
    /// Compute derived fields from primary parameters.
    ///
    /// Reference: `mdtest.c:2426-2460`
    pub fn compute_derived(&mut self) {
        // Compute num_dirs_in_tree
        if self.depth <= 0 {
            self.num_dirs_in_tree = 1;
        } else if self.branch_factor < 1 {
            self.num_dirs_in_tree = 1;
        } else if self.branch_factor == 1 {
            self.num_dirs_in_tree = (self.depth + 1) as u64;
        } else {
            let bf = self.branch_factor as f64;
            self.num_dirs_in_tree =
                ((bf.powi(self.depth + 1) - 1.0) / (bf - 1.0)) as u64;
        }

        // Compute items / items_per_dir relationship
        if self.items_per_dir > 0 {
            if self.items == 0 {
                if self.leaf_only {
                    self.items = self.items_per_dir
                        * (self.branch_factor as f64).powi(self.depth) as u64;
                } else {
                    self.items = self.items_per_dir * self.num_dirs_in_tree;
                }
            } else {
                self.num_dirs_in_tree_calc = self.num_dirs_in_tree;
            }
        } else if self.items > 0 {
            if self.leaf_only {
                if self.branch_factor <= 1 {
                    self.items_per_dir = self.items;
                } else {
                    let leaf_dirs =
                        (self.branch_factor as f64).powi(self.depth) as u64;
                    self.items_per_dir = self.items / leaf_dirs;
                    self.items = self.items_per_dir * leaf_dirs;
                }
            } else {
                self.items_per_dir = self.items / self.num_dirs_in_tree;
                self.items = self.items_per_dir * self.num_dirs_in_tree;
            }
        }

        // Compute directory_loops
        self.directory_loops = 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_derived_basic() {
        let mut p = MdtestParam::default();
        p.items = 100;
        p.compute_derived();
        assert_eq!(p.num_dirs_in_tree, 1);
        assert_eq!(p.items_per_dir, 100);
        assert_eq!(p.items, 100);
    }

    #[test]
    fn test_compute_derived_with_depth() {
        let mut p = MdtestParam::default();
        p.depth = 2;
        p.branch_factor = 2;
        p.items_per_dir = 10;
        p.compute_derived();
        // num_dirs = (2^3 - 1) / (2 - 1) = 7
        assert_eq!(p.num_dirs_in_tree, 7);
        assert_eq!(p.items, 70);
    }

    #[test]
    fn test_compute_derived_leaf_only() {
        let mut p = MdtestParam::default();
        p.depth = 2;
        p.branch_factor = 2;
        p.items_per_dir = 10;
        p.leaf_only = true;
        p.compute_derived();
        // leaf dirs = 2^2 = 4
        assert_eq!(p.items, 40);
    }
}
