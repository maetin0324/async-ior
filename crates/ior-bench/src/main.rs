mod cli;
mod json_output;
mod report;
mod runner;

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

    let mut params = args.into_ior_param();

    // Override num_tasks from MPI if not set (ref: ior.c:904-935)
    if params.num_tasks == -1 {
        params.num_tasks = mpi_size;
    } else if params.num_tasks > mpi_size {
        if rank == 0 {
            eprintln!(
                "WARNING: requested {} tasks but only {} available, using {}",
                params.num_tasks, mpi_size, mpi_size
            );
        }
        params.num_tasks = mpi_size;
    }

    // Print test configuration (rank 0 only)
    if rank == 0 && print_text {
        println!("IOR-bench (Rust async-ior)");
        println!(
            "  api            = {}",
            params.api_str()
        );
        println!("  num_tasks      = {}", params.num_tasks);
        println!("  block_size     = {}", params.block_size);
        println!("  transfer_size  = {}", params.transfer_size);
        println!("  segment_count  = {}", params.segment_count);
        println!("  repetitions    = {}", params.repetitions);
        println!(
            "  test_file      = {}",
            params.test_file_name_str()
        );
        println!("  file_per_proc  = {}", params.file_per_proc);
        println!("  direct_io      = {}", params.direct_io);
        println!("  queue_depth    = {}", params.queue_depth);

        // Print backend-specific options
        let prefix = params.api_str().to_lowercase();
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
    }

    // Create test subcommunicator for first num_tasks ranks (ref: ior.c:124-171)
    let color = if rank < params.num_tasks {
        Color::with_value(0)
    } else {
        Color::undefined()
    };
    let test_comm = world.split_by_color(color);

    if rank >= params.num_tasks {
        // Non-participating rank
        world.barrier();
        return;
    }

    let test_comm = test_comm.expect("failed to create test communicator");

    // Select backend and configure backend-specific options
    let mut backend = select_backend(&params);
    if let Err(e) = backend.as_mut().configure(&backend_options) {
        eprintln!("ERROR: invalid backend option: {}", e);
        world.barrier();
        return;
    }

    // Run the benchmark: async path for queue_depth > 1, sync path otherwise
    let result = if params.queue_depth > 1 {
        runner::run_benchmark_async(&params, backend.as_ref(), &test_comm, print_text)
    } else {
        runner::run_benchmark(&params, backend.as_ref(), &test_comm, print_text)
    };

    match result {
        Ok(bench_results) => {
            // JSON output (rank 0 only)
            if rank == 0 && json_mode {
                let doc = json_output::build_ior_json(&params, &bench_results, &command_line);
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
        }
        Err(e) => {
            eprintln!("ERROR [rank {}]: {}", rank, e);
        }
    }

    // Synchronize all ranks before exit
    world.barrier();
    // MPI_Finalize happens on drop of `universe`
}

/// Select I/O backend based on API name.
fn select_backend(params: &ior_core::IorParam) -> Box<dyn ior_core::Aiori> {
    let direct_io = params.direct_io;
    let queue_depth = params.queue_depth;

    match params.api_str() {
        "POSIX" => {
            if queue_depth > 1 {
                // Create with thread pool for async I/O
                Box::new(ior_backend_posix::PosixBackend::with_pool(
                    direct_io,
                    queue_depth as usize,
                ))
            } else {
                Box::new(ior_backend_posix::PosixBackend::new(direct_io))
            }
        }
        other => {
            eprintln!("Unknown API: {}, falling back to POSIX", other);
            Box::new(ior_backend_posix::PosixBackend::new(direct_io))
        }
    }
}
