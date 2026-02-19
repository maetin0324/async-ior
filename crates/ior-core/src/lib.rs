pub mod aiori;
pub mod aligned_buf;
pub mod backend_options;
pub mod data_pattern;
pub mod error;
pub mod ffi;
pub mod handle;
pub mod params;
pub mod timer;

// Re-export primary types for convenience
pub use aiori::Aiori;
pub use aligned_buf::AlignedBuffer;
pub use backend_options::{BackendOptions, OptionValue, extract_backend_options};
pub use data_pattern::DataPacketType;
pub use error::IorError;
pub use handle::{FileHandle, OpenFlags, StatResult, XferCallback, XferDir, XferResult, XferToken};
pub use params::IorParam;
pub use timer::{BenchTimers, now};
