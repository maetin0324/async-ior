use ior_core::timer::now;
use ior_core::{Aiori, AlignedBuffer};
use mpi::topology::SimpleCommunicator;
use mpi::traits::*;

use crate::params::MdtestParam;
use crate::tree;

/// Number of mdtest benchmark phases.
pub const MDTEST_NUM_PHASES: usize = 11;

/// Mdtest benchmark phase identifiers.
///
/// Reference: `mdtest.h:8-21` (mdtest_test_num_t)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum MdtestPhase {
    DirCreate = 0,
    DirStat = 1,
    DirRead = 2,
    DirRename = 3,
    DirRemove = 4,
    FileCreate = 5,
    FileStat = 6,
    FileRead = 7,
    FileRemove = 8,
    TreeCreate = 9,
    TreeRemove = 10,
}

/// Phase name for display.
///
/// Reference: `mdtest.c:1408-1424`
pub fn phase_name(phase: usize) -> &'static str {
    match phase {
        0 => "Directory creation",
        1 => "Directory stat",
        2 => "Directory read",
        3 => "Directory rename",
        4 => "Directory removal",
        5 => "File creation",
        6 => "File stat",
        7 => "File read",
        8 => "File removal",
        9 => "Tree creation",
        10 => "Tree removal",
        _ => "Unknown",
    }
}

/// Results from a single mdtest iteration.
///
/// Reference: `mdtest.h:23-37` (mdtest_results_t)
#[derive(Debug, Clone)]
pub struct MdtestResult {
    pub rate: [f64; MDTEST_NUM_PHASES],
    pub time: [f64; MDTEST_NUM_PHASES],
    pub items: [u64; MDTEST_NUM_PHASES],
    pub stonewall_time: [f64; MDTEST_NUM_PHASES],
    pub stonewall_last_item: [u64; MDTEST_NUM_PHASES],
}

impl Default for MdtestResult {
    fn default() -> Self {
        Self {
            rate: [0.0; MDTEST_NUM_PHASES],
            time: [0.0; MDTEST_NUM_PHASES],
            items: [0; MDTEST_NUM_PHASES],
            stonewall_time: [0.0; MDTEST_NUM_PHASES],
            stonewall_last_item: [0; MDTEST_NUM_PHASES],
        }
    }
}

/// Run a single mdtest iteration.
///
/// Reference: `mdtest.c:2004-2216` (mdtest_iteration)
pub fn mdtest_iteration(
    params: &MdtestParam,
    backend: &dyn Aiori,
    comm: &SimpleCommunicator,
    result: &mut MdtestResult,
    _iter_num: i32,
) {
    let rank = comm.rank();
    let ntasks = comm.size();
    let base_tree_name = format!("mdtest_tree.{}", rank);

    // Prepare test directory
    let test_dir = &params.test_dir;

    // === TREE CREATION ===
    if params.create_only {
        // Ensure test directory exists
        if backend.access(test_dir, 0).unwrap_or(false) == false {
            let _ = backend.mkdir(test_dir, 0o755);
        }

        comm.barrier();

        let start = now();

        if params.unique_dir_per_task {
            tree::create_remove_directory_tree(true, 0, test_dir, 0, params, backend);
        } else if rank == 0 {
            tree::create_remove_directory_tree(true, 0, test_dir, 0, params, backend);
        }

        comm.barrier();
        let elapsed = now() - start;

        result.rate[MdtestPhase::TreeCreate as usize] =
            params.num_dirs_in_tree as f64 / elapsed;
        result.time[MdtestPhase::TreeCreate as usize] = elapsed;
        result.items[MdtestPhase::TreeCreate as usize] = params.num_dirs_in_tree;
        result.stonewall_last_item[MdtestPhase::TreeCreate as usize] = params.num_dirs_in_tree;
    }

    // === SETUP NAMES ===
    let mk_name = format!("mdtest.{}.", (rank + 0 * params.nstride).rem_euclid(ntasks));
    let stat_name = format!("mdtest.{}.", (rank + 1 * params.nstride).rem_euclid(ntasks));
    let read_name = format!("mdtest.{}.", (rank + 2 * params.nstride).rem_euclid(ntasks));
    let rm_name = format!("mdtest.{}.", (rank + 3 * params.nstride).rem_euclid(ntasks));

    let unique_mk_dir = format!("{}.0", base_tree_name);

    // Prepare page-aligned write buffer (required for O_DIRECT)
    let write_buf: Option<AlignedBuffer> = if params.write_bytes > 0 {
        let mut buf = AlignedBuffer::new(params.write_bytes as usize);
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (i % 256) as u8;
        }
        Some(buf)
    } else {
        None
    };

    // Prepare page-aligned read buffer (required for O_DIRECT)
    let mut read_buf = AlignedBuffer::new(if params.read_bytes > 0 { params.read_bytes as usize } else { 1 });

    // Generate random array if needed
    let rand_array = if params.random_seed > 0 {
        Some(tree::generate_rand_array(params.items, params.random_seed))
    } else {
        None
    };

    // === DIRECTORY TEST ===
    if params.dirs_only && !params.shared_file {
        directory_test(
            params, backend, comm, result,
            &unique_mk_dir, &mk_name, &stat_name, &rm_name,
            rand_array.as_deref(),
        );
    }

    // === FILE TEST ===
    if params.files_only {
        file_test(
            params, backend, comm, result,
            &unique_mk_dir, &mk_name, &stat_name, &read_name, &rm_name,
            write_buf.as_deref(), &mut read_buf,
            rand_array.as_deref(),
        );
    }

    // === TREE REMOVAL ===
    comm.barrier();
    if params.remove_only {
        let start = now();

        if params.unique_dir_per_task {
            tree::create_remove_directory_tree(false, 0, test_dir, 0, params, backend);
        } else if rank == 0 {
            tree::create_remove_directory_tree(false, 0, test_dir, 0, params, backend);
        }

        comm.barrier();
        let elapsed = now() - start;

        result.rate[MdtestPhase::TreeRemove as usize] =
            params.num_dirs_in_tree as f64 / elapsed;
        result.time[MdtestPhase::TreeRemove as usize] = elapsed;
        result.items[MdtestPhase::TreeRemove as usize] = params.num_dirs_in_tree;
        result.stonewall_last_item[MdtestPhase::TreeRemove as usize] = params.num_dirs_in_tree;

        // Remove test directory
        if backend.access(test_dir, 0).unwrap_or(false) {
            let _ = backend.rmdir(test_dir);
        }
    }
}

/// Directory test: create/stat/read/rename/remove directories.
///
/// Reference: `mdtest.c:937-1117` (directory_test)
fn directory_test(
    params: &MdtestParam,
    backend: &dyn Aiori,
    comm: &SimpleCommunicator,
    result: &mut MdtestResult,
    path: &str,
    mk_name: &str,
    stat_name: &str,
    rm_name: &str,
    rand_array: Option<&[u64]>,
) {
    let test_dir = &params.test_dir;
    let full_path = format!("{}/{}", test_dir, path);

    comm.barrier();

    // Create phase
    if params.create_only {
        phase_prepare(params, comm);
        let start = now();

        let items_done = tree::create_remove_items(
            0, true, true, &full_path, 0, params, backend, mk_name, rm_name, None,
            start,
        );

        phase_end(params, comm);
        let elapsed = now() - start;

        let effective_items = if params.stone_wall_timer_seconds > 0 { items_done } else { params.items };
        result.rate[MdtestPhase::DirCreate as usize] = effective_items as f64 / elapsed;
        result.time[MdtestPhase::DirCreate as usize] = elapsed;
        result.items[MdtestPhase::DirCreate as usize] = effective_items;
        result.stonewall_last_item[MdtestPhase::DirCreate as usize] = items_done;
    }

    // Stat phase
    if params.stat_only {
        phase_prepare(params, comm);
        let start = now();

        tree::mdtest_stat(
            params.random_seed > 0, true, &full_path, params, backend, stat_name, rand_array,
        );

        phase_end(params, comm);
        let elapsed = now() - start;

        result.rate[MdtestPhase::DirStat as usize] = params.items as f64 / elapsed;
        result.time[MdtestPhase::DirStat as usize] = elapsed;
        result.items[MdtestPhase::DirStat as usize] = params.items;
    }

    // Read phase (N/A for directories in C mdtest, but we record time)
    if params.read_only {
        phase_prepare(params, comm);
        let start = now();
        // Directory read is N/A in C mdtest
        phase_end(params, comm);
        let elapsed = now() - start;

        result.rate[MdtestPhase::DirRead as usize] = params.items as f64 / elapsed;
        result.time[MdtestPhase::DirRead as usize] = elapsed;
        result.items[MdtestPhase::DirRead as usize] = params.items;
    }

    // Rename phase
    if params.rename_dirs && params.items > 1 {
        phase_prepare(params, comm);
        let start = now();

        tree::rename_dir_items(&full_path, params, backend, stat_name);

        phase_end(params, comm);
        let elapsed = now() - start;

        result.rate[MdtestPhase::DirRename as usize] = params.items as f64 / elapsed;
        result.time[MdtestPhase::DirRename as usize] = elapsed;
        result.items[MdtestPhase::DirRename as usize] = params.items;
    }

    // Remove phase
    if params.remove_only {
        phase_prepare(params, comm);
        let start = now();

        tree::create_remove_items(
            0, true, false, &full_path, 0, params, backend, mk_name, rm_name, None,
            0.0, // no stonewall for remove
        );

        phase_end(params, comm);
        let elapsed = now() - start;

        result.rate[MdtestPhase::DirRemove as usize] = params.items as f64 / elapsed;
        result.time[MdtestPhase::DirRemove as usize] = elapsed;
        result.items[MdtestPhase::DirRemove as usize] = params.items;
    }
}

/// File test: create/stat/read/remove files.
///
/// Reference: `mdtest.c:1229-1406` (file_test)
#[allow(clippy::too_many_arguments)]
fn file_test(
    params: &MdtestParam,
    backend: &dyn Aiori,
    comm: &SimpleCommunicator,
    result: &mut MdtestResult,
    path: &str,
    mk_name: &str,
    stat_name: &str,
    read_name: &str,
    rm_name: &str,
    write_buf: Option<&[u8]>,
    read_buf: &mut [u8],
    rand_array: Option<&[u64]>,
) {
    let test_dir = &params.test_dir;
    let full_path = format!("{}/{}", test_dir, path);

    comm.barrier();

    // Create phase
    if params.create_only {
        phase_prepare(params, comm);
        let start = now();

        let items_done = tree::create_remove_items(
            0, false, true, &full_path, 0, params, backend, mk_name, rm_name, write_buf,
            start,
        );

        phase_end(params, comm);
        let elapsed = now() - start;

        let effective_items = if params.stone_wall_timer_seconds > 0 { items_done } else { params.items };
        result.rate[MdtestPhase::FileCreate as usize] = effective_items as f64 / elapsed;
        result.time[MdtestPhase::FileCreate as usize] = elapsed;
        result.items[MdtestPhase::FileCreate as usize] = effective_items;
        result.stonewall_last_item[MdtestPhase::FileCreate as usize] = items_done;
    }

    // Stat phase
    if params.stat_only {
        phase_prepare(params, comm);
        let start = now();

        tree::mdtest_stat(
            params.random_seed > 0, false, &full_path, params, backend, stat_name, rand_array,
        );

        phase_end(params, comm);
        let elapsed = now() - start;

        result.rate[MdtestPhase::FileStat as usize] = params.items as f64 / elapsed;
        result.time[MdtestPhase::FileStat as usize] = elapsed;
        result.items[MdtestPhase::FileStat as usize] = params.items;
    }

    // Read phase
    if params.read_only {
        phase_prepare(params, comm);
        let start = now();

        tree::mdtest_read(
            params.random_seed > 0, false, &full_path, params, backend, read_name,
            rand_array, read_buf,
        );

        phase_end(params, comm);
        let elapsed = now() - start;

        result.rate[MdtestPhase::FileRead as usize] = params.items as f64 / elapsed;
        result.time[MdtestPhase::FileRead as usize] = elapsed;
        result.items[MdtestPhase::FileRead as usize] = params.items;
    }

    // Remove phase
    if params.remove_only {
        phase_prepare(params, comm);
        let start = now();

        tree::create_remove_items(
            0, false, false, &full_path, 0, params, backend, mk_name, rm_name, None,
            0.0, // no stonewall for remove
        );

        phase_end(params, comm);
        let elapsed = now() - start;

        result.rate[MdtestPhase::FileRemove as usize] = params.items as f64 / elapsed;
        result.time[MdtestPhase::FileRemove as usize] = elapsed;
        result.items[MdtestPhase::FileRemove as usize] = params.items;
    }
}

/// Prepare for a phase: optional barrier.
fn phase_prepare(params: &MdtestParam, comm: &SimpleCommunicator) {
    if params.barriers {
        comm.barrier();
    }
}

/// End a phase: optional barrier.
fn phase_end(params: &MdtestParam, comm: &SimpleCommunicator) {
    if params.barriers {
        comm.barrier();
    }
}
