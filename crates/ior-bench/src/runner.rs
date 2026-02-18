use std::cell::Cell;

use ior_core::error::IorError;
use ior_core::handle::{OpenFlags, XferDir, XferResult};
use ior_core::params::IorParam;
use ior_core::timer::BenchTimers;
use ior_core::{now, Aiori};
use mpi::collective::SystemOperation;
use mpi::topology::SimpleCommunicator;
use mpi::traits::*;

use crate::report;

/// Run the full MPI-parallel benchmark loop.
///
/// Reference: `ior.c:1197-1490` (TestIoSys)
pub fn run_benchmark(
    params: &IorParam,
    backend: &dyn Aiori,
    comm: &SimpleCommunicator,
) -> Result<(), IorError> {
    let rank = comm.rank();
    let num_tasks = params.num_tasks;

    let mut write_results = Vec::new();
    let mut read_results = Vec::new();

    report::print_header(comm);

    for rep in 0..params.repetitions {
        let mut rank_offset: i32 = 0;

        // === WRITE PHASE === (ref: ior.c:1287-1340)
        if params.write_file {
            if !params.use_existing_test_file {
                remove_file(params, backend, rank, rank_offset, num_tasks);
            }

            comm.barrier(); // ior.c:1300

            let mut timers = BenchTimers::default();

            timers.timers[0] = now();
            let path = get_test_file_name(params, rank, rank_offset);
            let mut open_flags = OpenFlags::CREAT | OpenFlags::RDWR;
            if params.direct_io {
                open_flags |= OpenFlags::DIRECT;
            }
            let handle = backend.create(&path, open_flags)?;
            timers.timers[1] = now();

            if params.intra_test_barriers {
                comm.barrier(); // ior.c:1307
            }

            timers.timers[2] = now();
            let data_moved =
                write_or_read(&handle, XferDir::Write, params, backend, rank, rank_offset, comm)?;
            timers.timers[3] = now();

            if params.intra_test_barriers {
                comm.barrier(); // ior.c:1322
            }

            if params.fsync {
                backend.fsync(&handle)?;
            }

            timers.timers[4] = now();
            backend.close(handle)?;
            timers.timers[5] = now();

            comm.barrier(); // ior.c:1328
            check_file_size(params, backend, data_moved, rank, rank_offset, comm);

            let result =
                reduce_and_report("write", &timers, params, data_moved, comm, rep);
            if let Some(r) = result {
                write_results.push(r);
            }
        }

        // === READ PHASE === (ref: ior.c:1373-1459)
        if params.read_file {
            // Task reordering for read-back (ref: ior.c:1389-1421)
            if params.reorder_tasks {
                rank_offset = params.task_per_node_offset % num_tasks;
            } else if params.reorder_tasks_random {
                rank_offset = random_rank_offset(rank, num_tasks, params.reorder_tasks_random_seed);
            }

            comm.barrier(); // ior.c:1430

            let mut timers = BenchTimers::default();

            timers.timers[0] = now();
            let path = get_test_file_name(params, rank, rank_offset);
            let mut open_flags = OpenFlags::RDONLY;
            if params.direct_io {
                open_flags |= OpenFlags::DIRECT;
            }
            let handle = backend.open(&path, open_flags)?;
            timers.timers[1] = now();

            if params.intra_test_barriers {
                comm.barrier(); // ior.c:1437
            }

            timers.timers[2] = now();
            let data_moved =
                write_or_read(&handle, XferDir::Read, params, backend, rank, rank_offset, comm)?;
            timers.timers[3] = now();

            if params.intra_test_barriers {
                comm.barrier(); // ior.c:1448
            }

            timers.timers[4] = now();
            backend.close(handle)?;
            timers.timers[5] = now();

            let result =
                reduce_and_report("read", &timers, params, data_moved, comm, rep);
            if let Some(r) = result {
                read_results.push(r);
            }

            // Reset rank offset after read
        }

        // === CLEANUP === (ref: ior.c:1465-1467)
        if !params.keep_file {
            comm.barrier();
            remove_file(params, backend, rank, 0, num_tasks);
            comm.barrier();
        }

        if params.inter_test_delay > 0 {
            std::thread::sleep(std::time::Duration::from_secs(
                params.inter_test_delay as u64,
            ));
        }
    }

    // Print summary (rank 0 only)
    if !write_results.is_empty() {
        report::print_summary("write", &write_results, params.block_size, params.transfer_size, comm);
    }
    if !read_results.is_empty() {
        report::print_summary("read", &read_results, params.block_size, params.transfer_size, comm);
    }

    Ok(())
}

/// Inner I/O loop: write or read data for all segments and offsets.
///
/// Reference: `ior.c:1757-1914` (WriteOrRead)
fn write_or_read(
    handle: &ior_core::FileHandle,
    access: XferDir,
    params: &IorParam,
    backend: &dyn Aiori,
    rank: i32,
    rank_offset: i32,
    comm: &SimpleCommunicator,
) -> Result<i64, IorError> {
    let num_tasks = params.num_tasks;
    let pretend_rank = ((rank + rank_offset) % num_tasks + num_tasks) % num_tasks;
    let offsets_per_block = params.block_size / params.transfer_size;
    let mut data_moved: i64 = 0;

    // Allocate transfer buffer
    let buf_size = params.transfer_size as usize;
    let mut buffer = vec![0u8; buf_size];

    // Fill write buffer with pattern
    if access == XferDir::Write {
        for (i, byte) in buffer.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
    }

    let start = now();
    let mut hit_stonewall = false;

    loop {
        // min_time_duration loop (ior.c:1845)
        for seg in 0..params.segment_count {
            if hit_stonewall {
                break;
            }
            for j in 0..offsets_per_block {
                if hit_stonewall {
                    break;
                }

                // OFFSET CALCULATION (ref: ior.c:1823-1829)
                let offset = if params.file_per_proc {
                    j * params.transfer_size + seg * params.block_size
                } else {
                    // Shared file: interleaved blocks per rank
                    j * params.transfer_size
                        + seg * num_tasks as i64 * params.block_size
                        + pretend_rank as i64 * params.block_size
                };

                let transferred = backend.xfer_sync(
                    handle,
                    access,
                    buffer.as_mut_ptr(),
                    params.transfer_size,
                    offset,
                )?;
                data_moved += transferred;

                if params.fsync_per_write && access == XferDir::Write {
                    backend.fsync(handle)?;
                }

                // Stonewalling check (ref: ior.c:1834-1842)
                if params.deadline_for_stonewalling > 0 {
                    let elapsed = now() - start;
                    if elapsed > params.deadline_for_stonewalling as f64 {
                        hit_stonewall = true;
                    }
                }

                // Collective stonewalling broadcast to prevent deadlock
                if params.deadline_for_stonewalling > 0 {
                    let mut stonewall_flag = hit_stonewall as i32;
                    comm.process_at_rank(0)
                        .broadcast_into(&mut stonewall_flag);
                    hit_stonewall = stonewall_flag != 0;
                }
            }
        }

        // Check min_time_duration
        let elapsed = now() - start;
        if elapsed >= params.min_time_duration as f64 || params.min_time_duration == 0 {
            break;
        }
    }

    Ok(data_moved)
}

/// Generate test file name based on rank and offset.
///
/// Reference: `ior.c:682-731` (GetTestFileName)
pub fn get_test_file_name(params: &IorParam, rank: i32, rank_offset: i32) -> String {
    let effective_rank = ((rank + rank_offset) % params.num_tasks + params.num_tasks) % params.num_tasks;
    let base = params.test_file_name_str();

    if params.file_per_proc {
        format!("{}.{:08}", base, effective_rank)
    } else {
        base.to_string()
    }
}

/// Remove test files.
fn remove_file(
    params: &IorParam,
    backend: &dyn Aiori,
    rank: i32,
    rank_offset: i32,
    _num_tasks: i32,
) {
    if params.file_per_proc {
        let path = get_test_file_name(params, rank, rank_offset);
        let _ = backend.delete(&path);
    } else if rank == 0 {
        // Only rank 0 deletes shared file
        let path = get_test_file_name(params, rank, rank_offset);
        let _ = backend.delete(&path);
    }
}

/// Reduce timers and compute/print metrics.
///
/// Reference: `ior.c:790-845` (ReduceIterResults)
fn reduce_and_report(
    access: &str,
    timers: &BenchTimers,
    params: &IorParam,
    data_moved: i64,
    comm: &SimpleCommunicator,
    rep: i32,
) -> Option<report::IterResult> {
    // 1. Reduce timers across ranks
    let reduced = report::reduce_timers(timers, comm);

    // 2. Aggregate data moved
    let agg_data = report::reduce_data_moved(data_moved, comm);

    // 3. Compute metrics
    let result = report::compute_metrics(
        &reduced,
        timers,
        agg_data,
        params.transfer_size,
        params.block_size,
        comm,
        rep,
    );

    // 4. Print result (rank 0 only)
    report::print_result(access, &result, params.block_size, params.transfer_size, comm);

    if comm.rank() == 0 {
        Some(result)
    } else {
        // Non-root ranks still computed the result for local use but only rank 0
        // gets meaningful reduced values
        Some(result)
    }
}

/// Check file size consistency across ranks.
///
/// Reference: `ior.c:415-438` (CheckFileSize)
fn check_file_size(
    params: &IorParam,
    backend: &dyn Aiori,
    data_moved: i64,
    rank: i32,
    rank_offset: i32,
    comm: &SimpleCommunicator,
) {
    let path = get_test_file_name(params, rank, rank_offset);
    let local_size = backend.get_file_size(&path).unwrap_or(0);

    if params.file_per_proc {
        // Each rank checks its own file; aggregate with SUM
        let mut agg_size: i64 = 0;
        comm.all_reduce_into(&local_size, &mut agg_size, SystemOperation::sum());

        let mut agg_xfer: i64 = 0;
        comm.all_reduce_into(&data_moved, &mut agg_xfer, SystemOperation::sum());

        if comm.rank() == 0 && params.verbose > 0 && agg_size < agg_xfer {
            eprintln!(
                "WARNING: file size ({}) < expected ({})",
                agg_size, agg_xfer
            );
        }
    } else {
        // Shared file: verify consistency
        let mut min_size: i64 = 0;
        let mut max_size: i64 = 0;
        comm.all_reduce_into(&local_size, &mut min_size, SystemOperation::min());
        comm.all_reduce_into(&local_size, &mut max_size, SystemOperation::max());

        if comm.rank() == 0 && params.verbose > 0 && min_size != max_size {
            eprintln!(
                "WARNING: inconsistent file size across ranks: min={}, max={}",
                min_size, max_size
            );
        }

        let mut agg_xfer: i64 = 0;
        comm.all_reduce_into(&data_moved, &mut agg_xfer, SystemOperation::sum());

        if comm.rank() == 0 && params.verbose > 0 && min_size < agg_xfer {
            eprintln!(
                "WARNING: file size ({}) < expected ({})",
                min_size, agg_xfer
            );
        }
    }
}

/// Generate a pseudo-random rank offset for task reordering.
fn random_rank_offset(rank: i32, num_tasks: i32, seed: i32) -> i32 {
    // Simple LCG to get deterministic but shuffled offset per rank
    let mut state = (rank as u64).wrapping_add(seed as u64).wrapping_add(1);
    state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    ((state >> 33) as i32).rem_euclid(num_tasks)
}

// ============================================================================
// Async benchmark loop (Phase 6)
// ============================================================================

/// Run the MPI-parallel benchmark loop using async I/O (queue_depth > 1).
///
/// The outer structure (barriers, phases, reductions) is identical to the sync
/// version. Only the inner I/O loop uses pipelined async submit/poll.
pub fn run_benchmark_async(
    params: &IorParam,
    backend: &dyn Aiori,
    comm: &SimpleCommunicator,
) -> Result<(), IorError> {
    let rank = comm.rank();
    let num_tasks = params.num_tasks;

    let mut write_results = Vec::new();
    let mut read_results = Vec::new();

    report::print_header(comm);

    for rep in 0..params.repetitions {
        let mut rank_offset: i32 = 0;

        // === WRITE PHASE ===
        if params.write_file {
            if !params.use_existing_test_file {
                remove_file(params, backend, rank, rank_offset, num_tasks);
            }

            comm.barrier();

            let mut timers = BenchTimers::default();

            timers.timers[0] = now();
            let path = get_test_file_name(params, rank, rank_offset);
            let mut open_flags = OpenFlags::CREAT | OpenFlags::RDWR;
            if params.direct_io {
                open_flags |= OpenFlags::DIRECT;
            }
            let handle = backend.create(&path, open_flags)?;
            timers.timers[1] = now();

            if params.intra_test_barriers {
                comm.barrier();
            }

            timers.timers[2] = now();
            let data_moved = write_or_read_async(
                &handle,
                XferDir::Write,
                params,
                backend,
                rank,
                rank_offset,
                comm,
            )?;
            timers.timers[3] = now();

            if params.intra_test_barriers {
                comm.barrier();
            }

            if params.fsync {
                backend.fsync(&handle)?;
            }

            timers.timers[4] = now();
            backend.close(handle)?;
            timers.timers[5] = now();

            comm.barrier();
            check_file_size(params, backend, data_moved, rank, rank_offset, comm);

            let result = reduce_and_report("write", &timers, params, data_moved, comm, rep);
            if let Some(r) = result {
                write_results.push(r);
            }
        }

        // === READ PHASE ===
        if params.read_file {
            if params.reorder_tasks {
                rank_offset = params.task_per_node_offset % num_tasks;
            } else if params.reorder_tasks_random {
                rank_offset = random_rank_offset(rank, num_tasks, params.reorder_tasks_random_seed);
            }

            comm.barrier();

            let mut timers = BenchTimers::default();

            timers.timers[0] = now();
            let path = get_test_file_name(params, rank, rank_offset);
            let mut open_flags = OpenFlags::RDONLY;
            if params.direct_io {
                open_flags |= OpenFlags::DIRECT;
            }
            let handle = backend.open(&path, open_flags)?;
            timers.timers[1] = now();

            if params.intra_test_barriers {
                comm.barrier();
            }

            timers.timers[2] = now();
            let data_moved = write_or_read_async(
                &handle,
                XferDir::Read,
                params,
                backend,
                rank,
                rank_offset,
                comm,
            )?;
            timers.timers[3] = now();

            if params.intra_test_barriers {
                comm.barrier();
            }

            timers.timers[4] = now();
            backend.close(handle)?;
            timers.timers[5] = now();

            let result = reduce_and_report("read", &timers, params, data_moved, comm, rep);
            if let Some(r) = result {
                read_results.push(r);
            }
        }

        // === CLEANUP ===
        if !params.keep_file {
            comm.barrier();
            remove_file(params, backend, rank, 0, num_tasks);
            comm.barrier();
        }

        if params.inter_test_delay > 0 {
            std::thread::sleep(std::time::Duration::from_secs(
                params.inter_test_delay as u64,
            ));
        }
    }

    if !write_results.is_empty() {
        report::print_summary(
            "write",
            &write_results,
            params.block_size,
            params.transfer_size,
            comm,
        );
    }
    if !read_results.is_empty() {
        report::print_summary(
            "read",
            &read_results,
            params.block_size,
            params.transfer_size,
            comm,
        );
    }

    Ok(())
}

/// Completion state for async I/O tracking.
///
/// Callbacks fire on the poll() caller thread (same thread as the submit/poll
/// loop), so plain `Cell` suffices — no atomics needed.
struct AsyncState {
    completed_count: Cell<usize>,
    total_bytes: Cell<i64>,
    error: Cell<i64>,
}

/// C-compatible callback for async transfer completion.
extern "C" fn async_completion_callback(result: *const XferResult) {
    unsafe {
        let res = &*result;
        let state = &*(res.user_data as *const AsyncState);
        if res.error == 0 {
            state.total_bytes.set(state.total_bytes.get() + res.bytes_transferred);
        } else {
            state.error.set(res.error as i64);
        }
        state.completed_count.set(state.completed_count.get() + 1);
    }
}

/// Inner async I/O loop with pipeline pattern.
///
/// Each rank runs its own async pipeline independently; MPI synchronization
/// occurs at phase boundaries.
fn write_or_read_async(
    handle: &ior_core::FileHandle,
    access: XferDir,
    params: &IorParam,
    backend: &dyn Aiori,
    rank: i32,
    rank_offset: i32,
    _comm: &SimpleCommunicator,
) -> Result<i64, IorError> {
    let num_tasks = params.num_tasks;
    let pretend_rank = ((rank + rank_offset) % num_tasks + num_tasks) % num_tasks;
    let offsets_per_block = params.block_size / params.transfer_size;
    let queue_depth = params.queue_depth as usize;

    // Calculate total number of transfers
    let total_xfers = (params.segment_count * offsets_per_block) as usize;

    // Allocate queue_depth buffers
    let buf_size = params.transfer_size as usize;
    let mut buffers: Vec<Vec<u8>> = (0..queue_depth)
        .map(|_| {
            let mut buf = vec![0u8; buf_size];
            if access == XferDir::Write {
                for (i, byte) in buf.iter_mut().enumerate() {
                    *byte = (i % 256) as u8;
                }
            }
            buf
        })
        .collect();

    // Completion state — lives on the stack; callbacks fire on this same thread.
    let state = AsyncState {
        completed_count: Cell::new(0),
        total_bytes: Cell::new(0),
        error: Cell::new(0),
    };
    let state_ptr = &state as *const AsyncState as usize;

    let start = now();
    let mut submitted: usize = 0;
    let mut completed: usize = 0;
    let mut in_flight: usize = 0;
    let mut buf_idx: usize = 0;

    // Generate offset for a given linear transfer index
    let calc_offset = |xfer_idx: usize| -> i64 {
        let seg = xfer_idx as i64 / offsets_per_block;
        let j = xfer_idx as i64 % offsets_per_block;
        if params.file_per_proc {
            j * params.transfer_size + seg * params.block_size
        } else {
            j * params.transfer_size
                + seg * num_tasks as i64 * params.block_size
                + pretend_rank as i64 * params.block_size
        }
    };

    loop {
        // Submit burst: fill pipeline up to queue_depth
        while in_flight < queue_depth && submitted < total_xfers {
            // Check stonewalling
            if params.deadline_for_stonewalling > 0 {
                let elapsed = now() - start;
                if elapsed > params.deadline_for_stonewalling as f64 {
                    break;
                }
            }

            let offset = calc_offset(submitted);
            let buf = buffers[buf_idx].as_mut_ptr();

            backend.xfer_submit(
                handle,
                access,
                buf,
                params.transfer_size,
                offset,
                state_ptr,
                async_completion_callback,
            )?;

            submitted += 1;
            in_flight += 1;
            buf_idx = (buf_idx + 1) % queue_depth;
        }

        // No more work and nothing in flight
        if in_flight == 0 {
            break;
        }

        // Poll for completions
        let _n = backend.poll(queue_depth)?;
        let new_completed = state.completed_count.get();
        let delta = new_completed - completed;
        completed = new_completed;
        in_flight -= delta;

        // Check for errors
        let err = state.error.get();
        if err != 0 {
            return Err(IorError::Io(err as i32));
        }

        // Check min_time_duration restart
        if submitted >= total_xfers && in_flight == 0 {
            let elapsed = now() - start;
            if params.min_time_duration > 0 && elapsed < params.min_time_duration as f64 {
                // Reset for another pass
                submitted = 0;
            } else {
                break;
            }
        }
    }

    Ok(state.total_bytes.get())
}
