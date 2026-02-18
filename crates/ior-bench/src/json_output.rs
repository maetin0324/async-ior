use serde::Serialize;

use crate::report::{IterResult, SummaryStats};
use crate::runner::BenchmarkResults;
use ior_core::params::IorParam;

const MEBIBYTE: f64 = 1_048_576.0;
const KIBIBYTE: f64 = 1024.0;

// ============================================================================
// JSON document structures (C IOR compatible)
// ============================================================================

#[derive(Serialize)]
pub struct IorJsonDocument {
    pub version: String,
    pub began: String,
    pub command_line: String,
    pub machine: String,
    pub tests: Vec<IorJsonTest>,
    pub summary: Vec<IorJsonSummary>,
    pub finished: String,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct IorJsonTest {
    #[serde(rename = "TestID")]
    pub test_id: i32,
    pub start_time: String,
    pub parameters: IorJsonParameters,
    pub options: IorJsonOptions,
    pub results: Vec<IorJsonResult>,
}

#[derive(Serialize)]
pub struct IorJsonParameters {
    pub api: String,
    #[serde(rename = "blockSize")]
    pub block_size: i64,
    #[serde(rename = "transferSize")]
    pub transfer_size: i64,
    #[serde(rename = "segmentCount")]
    pub segment_count: i64,
    #[serde(rename = "numTasks")]
    pub num_tasks: i32,
    pub repetitions: i32,
    #[serde(rename = "filePerProc")]
    pub file_per_proc: bool,
    #[serde(rename = "directIO")]
    pub direct_io: bool,
    #[serde(rename = "queueDepth")]
    pub queue_depth: i32,
    #[serde(rename = "testFileName")]
    pub test_file_name: String,
    #[serde(rename = "deadlineForStonewalling")]
    pub deadline_for_stonewalling: i32,
    #[serde(rename = "keepFile")]
    pub keep_file: bool,
    pub fsync: bool,
    #[serde(rename = "randomOffset")]
    pub random_offset: bool,
}

#[derive(Serialize)]
pub struct IorJsonOptions {
    pub api: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    #[serde(rename = "testFileName")]
    pub test_file_name: String,
    pub access: String,
    pub ordering: String,
    pub repetitions: i32,
    #[serde(rename = "xfersize")]
    pub xfer_size: String,
    #[serde(rename = "blocksize")]
    pub block_size: String,
    #[serde(rename = "aggregate filesize")]
    pub aggregate_file_size: String,
}

#[derive(Serialize)]
pub struct IorJsonResult {
    pub access: String,
    #[serde(rename = "bwMiB")]
    pub bw_mib: f64,
    #[serde(rename = "blockKiB")]
    pub block_kib: f64,
    #[serde(rename = "xferKiB")]
    pub xfer_kib: f64,
    pub iops: f64,
    pub latency: f64,
    #[serde(rename = "openTime")]
    pub open_time: f64,
    #[serde(rename = "wrRdTime")]
    pub wr_rd_time: f64,
    #[serde(rename = "closeTime")]
    pub close_time: f64,
    #[serde(rename = "totalTime")]
    pub total_time: f64,
    #[serde(rename = "numTasks")]
    pub num_tasks: i32,
    pub iter: i32,
}

#[derive(Serialize)]
pub struct IorJsonSummary {
    pub operation: String,
    #[serde(rename = "bwMaxMIB")]
    pub bw_max_mib: f64,
    #[serde(rename = "bwMinMIB")]
    pub bw_min_mib: f64,
    #[serde(rename = "bwMeanMIB")]
    pub bw_mean_mib: f64,
    #[serde(rename = "bwStdMIB")]
    pub bw_std_mib: f64,
    #[serde(rename = "OPsMax")]
    pub ops_max: f64,
    #[serde(rename = "OPsMin")]
    pub ops_min: f64,
    #[serde(rename = "OPsMean")]
    pub ops_mean: f64,
    #[serde(rename = "OPsStdDev")]
    pub ops_std_dev: f64,
    #[serde(rename = "MeanTime")]
    pub mean_time: f64,
}

// ============================================================================
// Builder
// ============================================================================

pub fn build_ior_json(
    params: &IorParam,
    results: &BenchmarkResults,
    command_line: &str,
) -> IorJsonDocument {
    let began = current_time_string();
    let machine = get_machine_string();

    let parameters = IorJsonParameters {
        api: params.api_str().to_string(),
        block_size: params.block_size,
        transfer_size: params.transfer_size,
        segment_count: params.segment_count,
        num_tasks: params.num_tasks,
        repetitions: params.repetitions,
        file_per_proc: params.file_per_proc,
        direct_io: params.direct_io,
        queue_depth: params.queue_depth,
        test_file_name: params.test_file_name_str().to_string(),
        deadline_for_stonewalling: params.deadline_for_stonewalling,
        keep_file: params.keep_file,
        fsync: params.fsync,
        random_offset: params.random_offset,
    };

    let agg_file_size = params.expected_agg_file_size();
    let options = IorJsonOptions {
        api: params.api_str().to_string(),
        api_version: String::new(),
        test_file_name: params.test_file_name_str().to_string(),
        access: if params.file_per_proc {
            "file-per-process".to_string()
        } else {
            "single-shared-file".to_string()
        },
        ordering: if params.random_offset {
            "random".to_string()
        } else {
            "sequential".to_string()
        },
        repetitions: params.repetitions,
        xfer_size: format_size(params.transfer_size),
        block_size: format_size(params.block_size),
        aggregate_file_size: format_size(agg_file_size),
    };

    // Build Results array: interleave write/read per iteration
    let mut json_results = Vec::new();
    let max_iters = std::cmp::max(results.write_results.len(), results.read_results.len());

    for i in 0..max_iters {
        if let Some(wr) = results.write_results.get(i) {
            json_results.push(iter_result_to_json("write", wr, params));
        }
        if let Some(rd) = results.read_results.get(i) {
            json_results.push(iter_result_to_json("read", rd, params));
        }
    }

    let test = IorJsonTest {
        test_id: 0,
        start_time: began.clone(),
        parameters,
        options,
        results: json_results,
    };

    // Build summary
    let mut summary = Vec::new();
    if !results.write_results.is_empty() {
        summary.push(build_summary("write", &results.write_results));
    }
    if !results.read_results.is_empty() {
        summary.push(build_summary("read", &results.read_results));
    }

    let finished = current_time_string();

    IorJsonDocument {
        version: env!("CARGO_PKG_VERSION").to_string(),
        began,
        command_line: command_line.to_string(),
        machine,
        tests: vec![test],
        summary,
        finished,
    }
}

fn iter_result_to_json(access: &str, r: &IterResult, params: &IorParam) -> IorJsonResult {
    IorJsonResult {
        access: access.to_string(),
        bw_mib: r.bw / MEBIBYTE,
        block_kib: params.block_size as f64 / KIBIBYTE,
        xfer_kib: params.transfer_size as f64 / KIBIBYTE,
        iops: r.iops,
        latency: r.latency,
        open_time: r.open_time,
        wr_rd_time: r.rdwr_time,
        close_time: r.close_time,
        total_time: r.total_time,
        num_tasks: params.num_tasks,
        iter: r.rep,
    }
}

fn build_summary(operation: &str, results: &[IterResult]) -> IorJsonSummary {
    let bw_values: Vec<f64> = results.iter().map(|r| r.bw / MEBIBYTE).collect();
    let bw_stats = SummaryStats::from_values(&bw_values);

    let iops_values: Vec<f64> = results.iter().map(|r| r.iops).collect();
    let iops_stats = SummaryStats::from_values(&iops_values);

    let time_values: Vec<f64> = results.iter().map(|r| r.total_time).collect();
    let time_stats = SummaryStats::from_values(&time_values);

    IorJsonSummary {
        operation: operation.to_string(),
        bw_max_mib: bw_stats.max,
        bw_min_mib: bw_stats.min,
        bw_mean_mib: bw_stats.mean,
        bw_std_mib: bw_stats.stddev,
        ops_max: iops_stats.max,
        ops_min: iops_stats.min,
        ops_mean: iops_stats.mean,
        ops_std_dev: iops_stats.stddev,
        mean_time: time_stats.mean,
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn format_size(bytes: i64) -> String {
    let abs = bytes.unsigned_abs();
    if abs >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if abs >= 1024 * 1024 {
        format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if abs >= 1024 {
        format!("{:.2} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{} bytes", bytes)
    }
}

pub fn current_time_string() -> String {
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

pub fn get_machine_string() -> String {
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
    fn test_format_size() {
        assert_eq!(format_size(512), "512 bytes");
        assert_eq!(format_size(1024), "1.00 KiB");
        assert_eq!(format_size(1048576), "1.00 MiB");
        assert_eq!(format_size(1073741824), "1.00 GiB");
    }

    #[test]
    fn test_build_ior_json_serializes() {
        let params = IorParam::default();
        let results = BenchmarkResults {
            write_results: vec![IterResult {
                bw: 100.0 * MEBIBYTE,
                iops: 400.0,
                latency: 0.001,
                open_time: 0.01,
                rdwr_time: 0.5,
                close_time: 0.01,
                total_time: 0.52,
                data_moved: 104857600,
                rep: 0,
            }],
            read_results: vec![],
        };

        let doc = build_ior_json(&params, &results, "ior-bench -w");
        let json = serde_json::to_string_pretty(&doc).unwrap();
        assert!(json.contains("\"version\""));
        assert!(json.contains("\"write\""));
        assert!(json.contains("\"bwMiB\""));
    }
}
