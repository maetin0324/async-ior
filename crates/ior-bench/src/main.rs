mod cli;
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

    let args = CliArgs::parse();
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
    if rank == 0 {
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

    // Select backend
    let backend = select_backend(&params);

    // Run the benchmark: async path for queue_depth > 1, sync path otherwise
    let result = if params.queue_depth > 1 {
        runner::run_benchmark_async(&params, backend.as_ref(), &test_comm)
    } else {
        runner::run_benchmark(&params, backend.as_ref(), &test_comm)
    };

    if let Err(e) = result {
        eprintln!("ERROR [rank {}]: {}", rank, e);
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
