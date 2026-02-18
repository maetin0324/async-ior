use ior_core::timer::{BenchTimers, IOR_NB_TIMERS};
use mpi::collective::SystemOperation;
use mpi::topology::SimpleCommunicator;
use mpi::traits::*;

const MEBIBYTE: f64 = 1_048_576.0;
const KIBIBYTE: f64 = 1024.0;

/// Per-iteration result for one I/O phase (write or read).
#[derive(Debug, Clone)]
pub struct IterResult {
    /// Bandwidth in bytes/sec
    pub bw: f64,
    /// I/O operations per second
    pub iops: f64,
    /// Minimum latency across ranks
    pub latency: f64,
    /// Open phase time
    pub open_time: f64,
    /// Read/write phase time
    pub rdwr_time: f64,
    /// Close phase time
    pub close_time: f64,
    /// Total time (open_start to close_stop)
    pub total_time: f64,
    /// Aggregate data moved across all ranks
    pub data_moved: i64,
    /// Repetition number
    pub rep: i32,
}

/// Reduce timers across MPI ranks.
///
/// Even indices (starts) use MPI_MIN, odd indices (ends) use MPI_MAX.
/// Only rank 0 gets meaningful reduced values.
///
/// Reference: `ior.c:804-808`
pub fn reduce_timers(timers: &BenchTimers, comm: &SimpleCommunicator) -> BenchTimers {
    let rank = comm.rank();
    let root = comm.process_at_rank(0);

    let mut reduced = BenchTimers::default();

    for i in 0..IOR_NB_TIMERS {
        let val = timers.timers[i];
        if i % 2 == 0 {
            if rank == 0 {
                root.reduce_into_root(&val, &mut reduced.timers[i], SystemOperation::min());
            } else {
                root.reduce_into(&val, SystemOperation::min());
            }
        } else {
            if rank == 0 {
                root.reduce_into_root(&val, &mut reduced.timers[i], SystemOperation::max());
            } else {
                root.reduce_into(&val, SystemOperation::max());
            }
        }
    }

    reduced
}

/// Aggregate data moved across all ranks using MPI_Allreduce(SUM).
pub fn reduce_data_moved(local_data_moved: i64, comm: &SimpleCommunicator) -> i64 {
    let mut agg: i64 = 0;
    comm.all_reduce_into(&local_data_moved, &mut agg, SystemOperation::sum());
    agg
}

/// Compute performance metrics from reduced timers and aggregate data.
///
/// Reference: `ior.c:810-836`
pub fn compute_metrics(
    reduced: &BenchTimers,
    local_timers: &BenchTimers,
    agg_data: i64,
    transfer_size: i64,
    block_size: i64,
    comm: &SimpleCommunicator,
    rep: i32,
) -> IterResult {
    let rank = comm.rank();
    let root = comm.process_at_rank(0);

    let total_time = reduced.total_time();
    let access_time = reduced.rdwr_time();
    let open_time = reduced.open_time();
    let close_time = reduced.close_time();

    let bw = if total_time > 0.0 {
        agg_data as f64 / total_time
    } else {
        0.0
    };

    let iops = if access_time > 0.0 && transfer_size > 0 {
        (agg_data as f64 / transfer_size as f64) / access_time
    } else {
        0.0
    };

    // Latency: local access_time / number_of_IOs, then reduce MIN across ranks
    let ops_per_block = if transfer_size > 0 {
        block_size as f64 / transfer_size as f64
    } else {
        1.0
    };
    let local_latency = if ops_per_block > 0.0 {
        (local_timers.rdwr_stop() - local_timers.rdwr_start()) / ops_per_block
    } else {
        0.0
    };

    let mut min_latency = 0.0f64;
    if rank == 0 {
        root.reduce_into_root(&local_latency, &mut min_latency, SystemOperation::min());
    } else {
        root.reduce_into(&local_latency, SystemOperation::min());
    }

    IterResult {
        bw,
        iops,
        latency: min_latency,
        open_time,
        rdwr_time: access_time,
        close_time,
        total_time,
        data_moved: agg_data,
        rep,
    }
}

/// Print table header (rank 0 only).
///
/// Reference: `ior-output.c:21`
pub fn print_header(comm: &SimpleCommunicator) {
    if comm.rank() != 0 {
        return;
    }
    println!();
    println!(
        "{:<10} {:>10} {:>10} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>4}",
        "access",
        "bw(MiB/s)",
        "IOPS",
        "Latency(s)",
        "block(KiB)",
        "xfer(KiB)",
        "open(s)",
        "wr/rd(s)",
        "close(s)",
        "total(s)",
        "iter"
    );
    println!(
        "{:<10} {:>10} {:>10} {:>11} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>4}",
        "------",
        "---------",
        "----",
        "----------",
        "----------",
        "---------",
        "--------",
        "--------",
        "--------",
        "--------",
        "----"
    );
}

/// Print one result row (rank 0 only).
///
/// Reference: `ior-output.c:232` (PrintReducedResult)
pub fn print_result(
    access: &str,
    result: &IterResult,
    block_size: i64,
    transfer_size: i64,
    comm: &SimpleCommunicator,
) {
    if comm.rank() != 0 {
        return;
    }
    println!(
        "{:<10} {:>10.2} {:>10.2} {:>11.6} {:>10.2} {:>10.2} {:>10.6} {:>10.6} {:>10.6} {:>10.6} {:>4}",
        access,
        result.bw / MEBIBYTE,
        result.iops,
        result.latency,
        block_size as f64 / KIBIBYTE,
        transfer_size as f64 / KIBIBYTE,
        result.open_time,
        result.rdwr_time,
        result.close_time,
        result.total_time,
        result.rep,
    );
}

/// Summary statistics for multiple repetitions.
#[derive(Debug, Clone)]
pub struct SummaryStats {
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub stddev: f64,
}

impl SummaryStats {
    pub fn from_values(values: &[f64]) -> Self {
        if values.is_empty() {
            return Self {
                min: 0.0,
                max: 0.0,
                mean: 0.0,
                stddev: 0.0,
            };
        }
        let min = values.iter().cloned().reduce(f64::min).unwrap();
        let max = values.iter().cloned().reduce(f64::max).unwrap();
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance =
            values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
        Self {
            min,
            max,
            mean,
            stddev: variance.sqrt(),
        }
    }
}

/// Print summary of all repetitions (rank 0 only).
///
/// Matches C IOR's `PrintLongSummaryOneTest` format from `ior-output.c`.
pub fn print_summary(
    access: &str,
    results: &[IterResult],
    _block_size: i64,
    _transfer_size: i64,
    comm: &SimpleCommunicator,
) {
    if comm.rank() != 0 || results.is_empty() {
        return;
    }

    let bw_values: Vec<f64> = results.iter().map(|r| r.bw / MEBIBYTE).collect();
    let bw_stats = SummaryStats::from_values(&bw_values);

    let iops_values: Vec<f64> = results.iter().map(|r| r.iops).collect();
    let iops_stats = SummaryStats::from_values(&iops_values);

    let time_values: Vec<f64> = results.iter().map(|r| r.total_time).collect();
    let time_stats = SummaryStats::from_values(&time_values);

    let data_values: Vec<f64> = results.iter().map(|r| r.data_moved as f64 / MEBIBYTE).collect();
    let data_stats = SummaryStats::from_values(&data_values);

    println!();
    println!("Summary of all tests:");
    println!(
        "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Operation",
        "Max(MiB)",
        "Min(MiB)",
        "Mean(MiB)",
        "StdDev",
        "Max(OPs)",
        "Min(OPs)",
        "Mean(OPs)",
        "StdDev",
        "Mean(s)"
    );
    println!(
        "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "---------",
        "---------",
        "---------",
        "---------",
        "---------",
        "---------",
        "---------",
        "---------",
        "---------",
        "---------"
    );

    println!(
        "{:<10} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.2} {:>10.5}",
        access,
        bw_stats.max,
        bw_stats.min,
        bw_stats.mean,
        bw_stats.stddev,
        iops_stats.max,
        iops_stats.min,
        iops_stats.mean,
        iops_stats.stddev,
        time_stats.mean,
    );

    println!();
    println!(
        "Finished            : {:.6}",
        results.last().map_or(0.0, |r| r.total_time)
    );
    println!(
        "Data moved (MiB)    : {:.2}",
        data_stats.mean
    );
}
