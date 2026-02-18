use crate::params::MdtestParam;
use crate::runner::{MdtestResult, MDTEST_NUM_PHASES, MdtestPhase, phase_name};

/// Summarize and print mdtest results across iterations.
///
/// Reference: `mdtest.c:1485-1627` (summarize_results_rank0)
pub fn summarize_results(
    all_results: &[MdtestResult],
    params: &MdtestParam,
) {
    let iterations = all_results.len();
    if iterations == 0 {
        return;
    }

    // Determine which phases to display
    let (start, stop) = if params.files_only && !params.dirs_only {
        (MdtestPhase::FileCreate as usize, MdtestPhase::TreeCreate as usize)
    } else if params.dirs_only && !params.files_only {
        (MdtestPhase::DirCreate as usize, MdtestPhase::FileCreate as usize)
    } else {
        (MdtestPhase::DirCreate as usize, MdtestPhase::TreeCreate as usize)
    };

    let label = if params.print_time { "time" } else { "rate" };

    println!();
    println!("SUMMARY {}: (of {} iterations)", label, iterations);
    println!(
        "   {:<22} {:>14} {:>14} {:>14} {:>14}",
        "Operation", "Max", "Min", "Mean", "Std Dev"
    );
    println!(
        "   {:<22} {:>14} {:>14} {:>14} {:>14}",
        "---------", "---", "---", "----", "-------"
    );

    // Per-phase statistics for item-level operations
    for phase in start..stop {
        // Skip DirRead (phase 2) - N/A like C mdtest
        if phase == MdtestPhase::DirRead as usize {
            continue;
        }

        let mut iter_values: Vec<f64> = Vec::with_capacity(iterations);

        for result in all_results {
            let val = if params.print_time {
                result.time[phase]
            } else {
                result.rate[phase]
            };
            iter_values.push(val);
        }

        let stats = compute_stats(&iter_values);

        println!(
            "   {:<22} {:>14.3} {:>14.3} {:>14.3} {:>14.3}",
            phase_name(phase),
            stats.max,
            stats.min,
            stats.mean,
            if iterations > 1 { stats.stddev } else { 0.0 },
        );
    }

    // Tree create/remove rates (rank 0 only in C, but we're already rank 0)
    for phase in (MdtestPhase::TreeCreate as usize)..MDTEST_NUM_PHASES {
        let mut iter_values: Vec<f64> = Vec::with_capacity(iterations);

        for result in all_results {
            let val = if params.print_time {
                result.time[phase]
            } else {
                result.rate[phase]
            };
            iter_values.push(val);
        }

        let stats = compute_stats(&iter_values);

        println!(
            "   {:<22} {:>14.3} {:>14.3} {:>14.3} {:>14.3}",
            phase_name(phase),
            stats.max,
            stats.min,
            stats.mean,
            if iterations > 1 { stats.stddev } else { 0.0 },
        );
    }

    println!();
}

/// Print per-iteration verbose output.
pub fn print_iteration_result(result: &MdtestResult, iter_num: i32, verbose: i32) {
    if verbose < 1 {
        return;
    }

    println!("  -- iteration {} --", iter_num + 1);

    for phase in 0..MDTEST_NUM_PHASES {
        if result.time[phase] > 0.0 || result.rate[phase] > 0.0 {
            println!(
                "   {:<22}: {:>14.3} sec, {:>14.3} ops/sec",
                phase_name(phase),
                result.time[phase],
                result.rate[phase],
            );
        }
    }
}

struct Stats {
    min: f64,
    max: f64,
    mean: f64,
    stddev: f64,
}

fn compute_stats(values: &[f64]) -> Stats {
    if values.is_empty() {
        return Stats {
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            stddev: 0.0,
        };
    }

    let min = values.iter().cloned().reduce(f64::min).unwrap();
    let max = values.iter().cloned().reduce(f64::max).unwrap();
    let mean = values.iter().sum::<f64>() / values.len() as f64;

    let variance = if values.len() > 1 {
        values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64
    } else {
        0.0
    };

    Stats {
        min,
        max,
        mean,
        stddev: variance.sqrt(),
    }
}
