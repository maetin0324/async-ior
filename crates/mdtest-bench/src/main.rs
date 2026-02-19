mod cli;
mod json_output;
mod params;
mod report;
mod runner;
mod tree;

use clap::Parser;
use mpi::topology::Color;
use mpi::traits::*;

use cli::CliArgs;

fn main() {
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let rank = world.rank();
    let mpi_size = world.size();

    let raw_args: Vec<String> = std::env::args().collect();
    let (filtered_args, backend_options) = ior_core::extract_backend_options(raw_args);
    let args = CliArgs::parse_from(filtered_args);

    // Extract JSON flags before consuming args
    let json_stdout = args.json;
    let json_file = args.json_file.clone();
    let json_mode = json_stdout || json_file.is_some();
    let print_text = !json_stdout;

    // Save command line for JSON output
    let command_line = std::env::args().collect::<Vec<_>>().join(" ");

    let mut params = args.into_mdtest_param();
    params.num_tasks = mpi_size;
    params.compute_derived();

    // Task scaling defaults
    if params.first == 0 {
        params.first = mpi_size;
    }
    if params.last == 0 {
        params.last = mpi_size;
    }

    // Print configuration (rank 0 only)
    if rank == 0 && print_text {
        println!("mdtest-bench (Rust async-ior)");
        println!("  api                  = {}", params.api);
        println!("  num_tasks            = {}", params.num_tasks);
        println!("  test_dir             = {}", params.test_dir);
        println!("  branch_factor        = {}", params.branch_factor);
        println!("  depth                = {}", params.depth);
        println!("  items                = {}", params.items);
        println!("  items_per_dir        = {}", params.items_per_dir);
        println!("  num_dirs_in_tree     = {}", params.num_dirs_in_tree);
        println!("  iterations           = {}", params.iterations);
        println!("  unique_dir_per_task  = {}", params.unique_dir_per_task);
        println!("  dirs_only            = {}", params.dirs_only);
        println!("  files_only           = {}", params.files_only);
        println!("  create_only          = {}", params.create_only);
        println!("  stat_only            = {}", params.stat_only);
        println!("  read_only            = {}", params.read_only);
        println!("  remove_only          = {}", params.remove_only);

        if params.write_bytes > 0 {
            println!("  write_bytes          = {}", params.write_bytes);
        }
        if params.read_bytes > 0 {
            println!("  read_bytes           = {}", params.read_bytes);
        }

        // Print backend-specific options
        let prefix = params.api.to_lowercase();
        for (key, value) in backend_options.for_prefix(&prefix) {
            match value {
                ior_core::OptionValue::Flag => {
                    println!("  {}.{} = true", prefix, key);
                }
                ior_core::OptionValue::Str(s) => {
                    println!("  {}.{} = {}", prefix, key, s);
                }
            }
        }
        println!();
    }

    // Select backend and configure backend-specific options
    let mut backend = select_backend(&params);
    if let Err(e) = backend.as_mut().configure(&backend_options) {
        eprintln!("ERROR: invalid backend option: {}", e);
        world.barrier();
        return;
    }

    // Collect all results across task scaling for JSON
    let mut all_json_results: Vec<runner::MdtestResult> = Vec::new();

    // Task scaling loop: first..=last by stride
    // Reference: mdtest.c:2587-2641
    let mut ntasks = params.first;
    while ntasks <= params.last {
        // Create subcommunicator with ntasks ranks
        let color = if rank < ntasks {
            Color::with_value(0)
        } else {
            Color::undefined()
        };
        let test_comm = world.split_by_color(color);

        if rank >= ntasks {
            ntasks += params.stride;
            continue;
        }

        let test_comm = test_comm.expect("failed to create test communicator");

        if rank == 0 && print_text && (params.first != params.last) {
            println!("-- Running with {} tasks --", ntasks);
        }

        // Run iterations
        let mut all_results = Vec::new();
        for iter in 0..params.iterations {
            let mut result = runner::MdtestResult::default();
            runner::mdtest_iteration(&params, backend.as_ref(), &test_comm, &mut result, iter);

            if rank == 0 && print_text {
                report::print_iteration_result(&result, iter, params.verbose);
            }

            all_results.push(result);
        }

        // Report summary
        if rank == 0 && print_text {
            report::summarize_results(&all_results, &params);
        }

        if json_mode {
            all_json_results.extend(all_results);
        }

        ntasks += params.stride;
    }

    // JSON output (rank 0 only)
    if rank == 0 && json_mode {
        let doc = json_output::build_mdtest_json(&params, &all_json_results, &command_line);
        let json_str = serde_json::to_string_pretty(&doc)
            .expect("failed to serialize JSON");

        if json_stdout {
            println!("{}", json_str);
        }

        if let Some(ref path) = json_file {
            std::fs::write(path, &json_str)
                .unwrap_or_else(|e| eprintln!("ERROR: failed to write JSON file: {}", e));
        }
    }

    world.barrier();
}

/// Select I/O backend based on API name.
fn select_backend(params: &params::MdtestParam) -> Box<dyn ior_core::Aiori> {
    match params.api.as_str() {
        "POSIX" => Box::new(ior_backend_posix::PosixBackend::new(false)),
        other => {
            eprintln!("Unknown API: {}, falling back to POSIX", other);
            Box::new(ior_backend_posix::PosixBackend::new(false))
        }
    }
}
