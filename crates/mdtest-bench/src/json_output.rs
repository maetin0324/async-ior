use serde::Serialize;

use crate::params::MdtestParam;
use crate::runner::{MdtestPhase, MdtestResult, MDTEST_NUM_PHASES, phase_name};

// ============================================================================
// JSON document structures (C mdtest compatible)
// ============================================================================

#[derive(Serialize)]
pub struct MdtestJsonDocument {
    pub version: String,
    pub began: String,
    pub command_line: String,
    pub machine: String,
    pub tests: Vec<MdtestJsonTest>,
    pub summary: Vec<MdtestJsonPhaseSummary>,
    pub finished: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MdtestJsonTest {
    pub num_tasks: i32,
    pub parameters: MdtestJsonParameters,
    pub iterations: Vec<MdtestJsonIteration>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MdtestJsonParameters {
    pub api: String,
    pub test_dir: String,
    pub branch_factor: u32,
    pub depth: i32,
    pub items: u64,
    pub items_per_dir: u64,
    pub num_dirs_in_tree: u64,
    pub unique_dir_per_task: bool,
    pub dirs_only: bool,
    pub files_only: bool,
    pub create_only: bool,
    pub stat_only: bool,
    pub read_only: bool,
    pub remove_only: bool,
    pub write_bytes: u64,
    pub read_bytes: u64,
    pub iterations: i32,
}

#[derive(Serialize)]
pub struct MdtestJsonIteration {
    pub iteration: i32,
    pub phases: Vec<MdtestJsonPhaseResult>,
}

#[derive(Serialize)]
pub struct MdtestJsonPhaseResult {
    pub phase: String,
    pub rate: f64,
    pub time: f64,
    pub items: u64,
}

#[derive(Serialize)]
pub struct MdtestJsonPhaseSummary {
    pub phase: String,
    pub max: f64,
    pub min: f64,
    pub mean: f64,
    pub stddev: f64,
}

// ============================================================================
// Builder
// ============================================================================

pub fn build_mdtest_json(
    params: &MdtestParam,
    all_results: &[MdtestResult],
    command_line: &str,
) -> MdtestJsonDocument {
    let began = current_time_string();
    let machine = get_machine_string();

    let json_params = MdtestJsonParameters {
        api: params.api.clone(),
        test_dir: params.test_dir.clone(),
        branch_factor: params.branch_factor,
        depth: params.depth,
        items: params.items,
        items_per_dir: params.items_per_dir,
        num_dirs_in_tree: params.num_dirs_in_tree,
        unique_dir_per_task: params.unique_dir_per_task,
        dirs_only: params.dirs_only,
        files_only: params.files_only,
        create_only: params.create_only,
        stat_only: params.stat_only,
        read_only: params.read_only,
        remove_only: params.remove_only,
        write_bytes: params.write_bytes,
        read_bytes: params.read_bytes,
        iterations: params.iterations,
    };

    // Build iterations
    let iterations: Vec<MdtestJsonIteration> = all_results
        .iter()
        .enumerate()
        .map(|(i, result)| {
            let mut phases = Vec::new();
            for phase_idx in 0..MDTEST_NUM_PHASES {
                if result.time[phase_idx] > 0.0 || result.rate[phase_idx] > 0.0 {
                    phases.push(MdtestJsonPhaseResult {
                        phase: phase_name(phase_idx).to_string(),
                        rate: result.rate[phase_idx],
                        time: result.time[phase_idx],
                        items: result.items[phase_idx],
                    });
                }
            }
            MdtestJsonIteration {
                iteration: i as i32,
                phases,
            }
        })
        .collect();

    let test = MdtestJsonTest {
        num_tasks: params.num_tasks,
        parameters: json_params,
        iterations,
    };

    // Build summary
    let summary = build_summary(params, all_results);

    let finished = current_time_string();

    MdtestJsonDocument {
        version: env!("CARGO_PKG_VERSION").to_string(),
        began,
        command_line: command_line.to_string(),
        machine,
        tests: vec![test],
        summary,
        finished,
    }
}

fn build_summary(params: &MdtestParam, all_results: &[MdtestResult]) -> Vec<MdtestJsonPhaseSummary> {
    if all_results.is_empty() {
        return Vec::new();
    }

    let iterations = all_results.len();

    // Determine phase range
    let (start, stop) = if params.files_only && !params.dirs_only {
        (MdtestPhase::FileCreate as usize, MdtestPhase::TreeCreate as usize)
    } else if params.dirs_only && !params.files_only {
        (MdtestPhase::DirCreate as usize, MdtestPhase::FileCreate as usize)
    } else {
        (MdtestPhase::DirCreate as usize, MdtestPhase::TreeCreate as usize)
    };

    let mut summaries = Vec::new();

    // Item-level phases
    for phase in start..stop {
        if phase == MdtestPhase::DirRead as usize {
            continue;
        }

        let values: Vec<f64> = if params.print_time {
            all_results.iter().map(|r| r.time[phase]).collect()
        } else {
            all_results.iter().map(|r| r.rate[phase]).collect()
        };

        let stats = compute_stats(&values);
        summaries.push(MdtestJsonPhaseSummary {
            phase: phase_name(phase).to_string(),
            max: stats.max,
            min: stats.min,
            mean: stats.mean,
            stddev: if iterations > 1 { stats.stddev } else { 0.0 },
        });
    }

    // Tree phases
    for phase in (MdtestPhase::TreeCreate as usize)..MDTEST_NUM_PHASES {
        let values: Vec<f64> = if params.print_time {
            all_results.iter().map(|r| r.time[phase]).collect()
        } else {
            all_results.iter().map(|r| r.rate[phase]).collect()
        };

        let stats = compute_stats(&values);
        summaries.push(MdtestJsonPhaseSummary {
            phase: phase_name(phase).to_string(),
            max: stats.max,
            min: stats.min,
            mean: stats.mean,
            stddev: if iterations > 1 { stats.stddev } else { 0.0 },
        });
    }

    summaries
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

// ============================================================================
// Helpers
// ============================================================================

fn current_time_string() -> String {
    unsafe {
        let mut t: libc::time_t = 0;
        libc::time(&mut t);
        let tm = libc::localtime(&t);
        if tm.is_null() {
            return String::new();
        }
        let mut buf = [0u8; 64];
        let fmt = b"%a %b %d %H:%M:%S %Y\0";
        let len = libc::strftime(
            buf.as_mut_ptr() as *mut libc::c_char,
            buf.len(),
            fmt.as_ptr() as *const libc::c_char,
            tm,
        );
        String::from_utf8_lossy(&buf[..len]).to_string()
    }
}

fn get_machine_string() -> String {
    unsafe {
        let mut uts: libc::utsname = std::mem::zeroed();
        if libc::uname(&mut uts) != 0 {
            return String::new();
        }
        let nodename = std::ffi::CStr::from_ptr(uts.nodename.as_ptr());
        let sysname = std::ffi::CStr::from_ptr(uts.sysname.as_ptr());
        let release = std::ffi::CStr::from_ptr(uts.release.as_ptr());
        format!(
            "{} {} {}",
            nodename.to_string_lossy(),
            sysname.to_string_lossy(),
            release.to_string_lossy(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_mdtest_json_serializes() {
        let params = MdtestParam {
            items: 100,
            items_per_dir: 100,
            num_dirs_in_tree: 1,
            files_only: true,
            create_only: true,
            stat_only: true,
            read_only: true,
            remove_only: true,
            num_tasks: 1,
            ..Default::default()
        };

        let mut result = MdtestResult::default();
        result.rate[MdtestPhase::FileCreate as usize] = 1000.0;
        result.time[MdtestPhase::FileCreate as usize] = 0.1;
        result.items[MdtestPhase::FileCreate as usize] = 100;

        let doc = build_mdtest_json(&params, &[result], "mdtest-bench -n 100 -F");
        let json = serde_json::to_string_pretty(&doc).unwrap();
        assert!(json.contains("\"version\""));
        assert!(json.contains("\"File creation\""));
    }
}
