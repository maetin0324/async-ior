use ior_core::handle::{OpenFlags, XferDir};
use ior_core::{now, Aiori};

use crate::params::MdtestParam;

/// Base tree name prefix used for directory hierarchy.
const BASE_TREE_NAME: &str = "mdtest_tree";

/// Create or remove the hierarchical directory tree.
///
/// Reference: `mdtest.c:1934-2002` (create_remove_directory_tree)
pub fn create_remove_directory_tree(
    create: bool,
    curr_depth: i32,
    base_path: &str,
    dir_num: u64,
    params: &MdtestParam,
    backend: &dyn Aiori,
) {
    if curr_depth == 0 {
        let dir = format!("{}/{}.{}/", base_path, BASE_TREE_NAME, dir_num);

        if create {
            let _ = backend.mkdir(&dir, 0o755);
        }

        create_remove_directory_tree(
            create,
            curr_depth + 1,
            &dir,
            dir_num + 1,
            params,
            backend,
        );

        if !create {
            let _ = backend.rmdir(&dir);
        }
    } else if curr_depth <= params.depth {
        let mut curr_dir = dir_num;

        for _ in 0..params.branch_factor {
            let dir_suffix = format!("{}.{}/", BASE_TREE_NAME, curr_dir);
            let temp_path = format!("{}{}", base_path, dir_suffix);

            if create {
                let _ = backend.mkdir(&temp_path, 0o755);
            }

            create_remove_directory_tree(
                create,
                curr_depth + 1,
                &temp_path,
                curr_dir * params.branch_factor as u64 + 1,
                params,
                backend,
            );

            if !create {
                let _ = backend.rmdir(&temp_path);
            }

            curr_dir += 1;
        }
    }
}

/// Build item path for a given item number.
///
/// Given an item number and items_per_dir, constructs the full path by
/// walking up the tree hierarchy.
///
/// Reference: `mdtest.c:622-636`
pub fn build_item_path(
    base_path: &str,
    _prefix: &str,
    item_name: &str,
    item_num: u64,
    params: &MdtestParam,
) -> String {
    if params.items_per_dir == 0 {
        return format!("{}/{}", base_path, item_name);
    }

    let parent_dir = item_num / params.items_per_dir;

    if parent_dir == 0 {
        // Item is in tree's root directory
        return format!("{}/{}", base_path, item_name);
    }

    // Build path by walking up the tree
    let mut path = item_name.to_string();
    let mut dir = parent_dir;

    // Prepend parent directory
    path = format!("{}.{}/{}", BASE_TREE_NAME, dir, path);

    // Walk up to tree root
    while dir > params.branch_factor as u64 {
        dir = (dir - 1) / params.branch_factor as u64;
        path = format!("{}.{}/{}", BASE_TREE_NAME, dir, path);
    }

    format!("{}/{}", base_path, path)
}

/// Create or remove items (files or directories) in the tree.
///
/// Returns the number of items processed. When stonewalling is active,
/// this may be less than the total.
///
/// Reference: `mdtest.c:436-566` (create_remove_items + create_remove_items_helper)
pub fn create_remove_items(
    curr_depth: i32,
    dirs: bool,
    create: bool,
    path: &str,
    dir_num: u64,
    params: &MdtestParam,
    backend: &dyn Aiori,
    mk_name: &str,
    rm_name: &str,
    write_buf: Option<&[u8]>,
    stonewall_start: f64,
) -> u64 {
    let mut count: u64 = 0;

    if curr_depth == 0 {
        // Create/remove items at this depth
        if !params.leaf_only || (params.depth == 0 && params.leaf_only) {
            count += create_remove_items_helper(
                dirs, create, path, 0, params, backend, mk_name, rm_name, write_buf,
                stonewall_start,
            );
        }

        if params.depth > 0 {
            count += create_remove_items(
                curr_depth + 1,
                dirs,
                create,
                path,
                dir_num + 1,
                params,
                backend,
                mk_name,
                rm_name,
                write_buf,
                stonewall_start,
            );
        }
    } else if curr_depth <= params.depth {
        let mut curr_dir = dir_num;

        for _ in 0..params.branch_factor {
            let temp_path = format!("{}/{}.{}/", path, BASE_TREE_NAME, curr_dir);

            // Create items in this branch
            if !params.leaf_only || (params.leaf_only && curr_depth == params.depth) {
                count += create_remove_items_helper(
                    dirs,
                    create,
                    &temp_path,
                    curr_dir * params.items_per_dir,
                    params,
                    backend,
                    mk_name,
                    rm_name,
                    write_buf,
                    stonewall_start,
                );
            }

            // Recurse to next level
            count += create_remove_items(
                curr_depth + 1,
                dirs,
                create,
                &temp_path,
                curr_dir * params.branch_factor as u64 + 1,
                params,
                backend,
                mk_name,
                rm_name,
                write_buf,
                stonewall_start,
            );

            curr_dir += 1;
        }
    }

    count
}

/// Helper: create or remove items at a single directory level.
///
/// Returns the number of items processed. May be less than `items_per_dir`
/// when stonewalling is active and the deadline has been reached.
///
/// Reference: `mdtest.c:436-459` (create_remove_items_helper)
fn create_remove_items_helper(
    dirs: bool,
    create: bool,
    path: &str,
    item_num: u64,
    params: &MdtestParam,
    backend: &dyn Aiori,
    mk_name: &str,
    rm_name: &str,
    write_buf: Option<&[u8]>,
    stonewall_start: f64,
) -> u64 {
    let name = if create { mk_name } else { rm_name };

    for i in 0..params.items_per_dir {
        // Stonewall check (ref: mdtest.c:451 CHECK_STONE_WALL)
        if params.stone_wall_timer_seconds > 0
            && (now() - stonewall_start) > params.stone_wall_timer_seconds as f64
        {
            return i;
        }

        if dirs {
            let item_path = format!("{}dir.{}{}", path, name, item_num + i);
            if create {
                let _ = backend.mkdir(&item_path, 0o755);
            } else {
                let _ = backend.rmdir(&item_path);
            }
        } else {
            let item_path = format!("{}file.{}{}", path, name, item_num + i);
            if create {
                create_file(&item_path, params, backend, write_buf);
            } else {
                let _ = backend.delete(&item_path);
            }
        }
    }

    params.items_per_dir
}

/// Create a single file, optionally writing data.
///
/// Uses mknod for fast creation when make_node is set and no data needs to be written.
fn create_file(
    path: &str,
    params: &MdtestParam,
    backend: &dyn Aiori,
    write_buf: Option<&[u8]>,
) {
    if params.make_node && params.write_bytes == 0 {
        let _ = backend.mknod(path);
        return;
    }

    let handle = match backend.create(path, OpenFlags::WRONLY | OpenFlags::CREAT) {
        Ok(h) => h,
        Err(_) => return,
    };

    if let Some(buf) = write_buf {
        if params.write_bytes > 0 {
            let _ = backend.xfer_sync(
                &handle,
                XferDir::Write,
                buf.as_ptr() as *mut u8,
                params.write_bytes as i64,
                0,
            );
        }
    }

    if params.sync_file {
        let _ = backend.fsync(&handle);
    }

    let _ = backend.close(handle);
}

/// Stat items in the tree, supporting random access order.
///
/// Reference: `mdtest.c:569-648` (mdtest_stat)
pub fn mdtest_stat(
    random: bool,
    dirs: bool,
    path: &str,
    params: &MdtestParam,
    backend: &dyn Aiori,
    stat_name: &str,
    rand_array: Option<&[u64]>,
) {
    let stop_items = if params.directory_loops != 1 {
        params.items_per_dir
    } else {
        params.items
    };

    for i in 0..stop_items {
        let item_num = if random {
            rand_array.map_or(i, |arr| arr[i as usize])
        } else {
            i
        };

        // Adjust for leaf_only
        let adjusted_num = if params.leaf_only {
            let leaf_offset = params.num_dirs_in_tree
                - (params.branch_factor as f64).powi(params.depth) as u64;
            item_num + params.items_per_dir * leaf_offset
        } else {
            item_num
        };

        let prefix = if dirs { "dir" } else { "file" };
        let item_name = format!("{}.{}{}", prefix, stat_name, adjusted_num);
        let full_path = build_item_path(path, prefix, &item_name, adjusted_num, params);

        let _ = backend.stat(&full_path);
    }
}

/// Read items in the tree.
///
/// Reference: `mdtest.c:651-768` (mdtest_read)
pub fn mdtest_read(
    random: bool,
    dirs: bool,
    path: &str,
    params: &MdtestParam,
    backend: &dyn Aiori,
    read_name: &str,
    rand_array: Option<&[u64]>,
    read_buf: &mut [u8],
) {
    if dirs || params.read_bytes == 0 {
        return; // No reading for directories or zero-byte reads
    }

    let stop_items = if params.directory_loops != 1 {
        params.items_per_dir
    } else {
        params.items
    };

    for i in 0..stop_items {
        let item_num = if random {
            rand_array.map_or(i, |arr| arr[i as usize])
        } else {
            i
        };

        let adjusted_num = if params.leaf_only {
            let leaf_offset = params.num_dirs_in_tree
                - (params.branch_factor as f64).powi(params.depth) as u64;
            item_num + params.items_per_dir * leaf_offset
        } else {
            item_num
        };

        let item_name = format!("file.{}{}", read_name, adjusted_num);
        let full_path = build_item_path(path, "file", &item_name, adjusted_num, params);

        let handle = match backend.open(&full_path, OpenFlags::RDONLY) {
            Ok(h) => h,
            Err(_) => continue,
        };

        let _ = backend.xfer_sync(
            &handle,
            XferDir::Read,
            read_buf.as_mut_ptr(),
            params.read_bytes as i64,
            0,
        );

        let _ = backend.close(handle);
    }
}

/// Rename directories in the tree.
///
/// Reference: `mdtest.c:1046-1068`
pub fn rename_dir_items(
    path: &str,
    params: &MdtestParam,
    backend: &dyn Aiori,
    stat_name: &str,
) {
    let stop_items = if params.directory_loops != 1 {
        params.items_per_dir
    } else {
        params.items
    };

    for i in 0..stop_items {
        let adjusted_num = if params.leaf_only {
            let leaf_offset = params.num_dirs_in_tree
                - (params.branch_factor as f64).powi(params.depth) as u64;
            i + params.items_per_dir * leaf_offset
        } else {
            i
        };

        let old_name = format!("dir.{}{}", stat_name, adjusted_num);
        let new_name = format!("dir.{}{}.renamed", stat_name, adjusted_num);
        let old_path = build_item_path(path, "dir", &old_name, adjusted_num, params);
        let new_path = build_item_path(path, "dir", &new_name, adjusted_num, params);

        let _ = backend.rename(&old_path, &new_path);
    }
}

/// Generate a shuffled array using Fisher-Yates algorithm.
///
/// Reference: `mdtest.c:2461-2495`
pub fn generate_rand_array(items: u64, seed: i32) -> Vec<u64> {
    let mut arr: Vec<u64> = (0..items).collect();
    let mut state = seed as u64;

    let n = arr.len();
    for i in (1..n).rev() {
        // Simple LCG for deterministic random
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let j = (state >> 33) as usize % (i + 1);
        arr.swap(i, j);
    }

    arr
}
