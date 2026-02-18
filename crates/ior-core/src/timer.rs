use std::cell::Cell;
use std::time::Instant;

/// Number of timer points (open_start, open_stop, rdwr_start, rdwr_stop, close_start, close_stop)
pub const IOR_NB_TIMERS: usize = 6;

/// Timer indices matching C IOR's convention.
pub const IOR_TIMER_OPEN_START: usize = 0;
pub const IOR_TIMER_OPEN_STOP: usize = 1;
pub const IOR_TIMER_RDWR_START: usize = 2;
pub const IOR_TIMER_RDWR_STOP: usize = 3;
pub const IOR_TIMER_CLOSE_START: usize = 4;
pub const IOR_TIMER_CLOSE_STOP: usize = 5;

/// Benchmark timer storage for one I/O phase (write or read).
#[derive(Debug, Clone, Copy)]
pub struct BenchTimers {
    /// Raw timer values: [open_start, open_stop, rdwr_start, rdwr_stop, close_start, close_stop]
    pub timers: [f64; IOR_NB_TIMERS],
}

impl Default for BenchTimers {
    fn default() -> Self {
        Self {
            timers: [0.0; IOR_NB_TIMERS],
        }
    }
}

impl BenchTimers {
    pub fn open_start(&self) -> f64 {
        self.timers[IOR_TIMER_OPEN_START]
    }
    pub fn open_stop(&self) -> f64 {
        self.timers[IOR_TIMER_OPEN_STOP]
    }
    pub fn rdwr_start(&self) -> f64 {
        self.timers[IOR_TIMER_RDWR_START]
    }
    pub fn rdwr_stop(&self) -> f64 {
        self.timers[IOR_TIMER_RDWR_STOP]
    }
    pub fn close_start(&self) -> f64 {
        self.timers[IOR_TIMER_CLOSE_START]
    }
    pub fn close_stop(&self) -> f64 {
        self.timers[IOR_TIMER_CLOSE_STOP]
    }

    /// Open phase duration (from reduced timers)
    pub fn open_time(&self) -> f64 {
        self.open_stop() - self.open_start()
    }
    /// Read/write phase duration (from reduced timers)
    pub fn rdwr_time(&self) -> f64 {
        self.rdwr_stop() - self.rdwr_start()
    }
    /// Close phase duration (from reduced timers)
    pub fn close_time(&self) -> f64 {
        self.close_stop() - self.close_start()
    }
    /// Total time from open_start to close_stop
    pub fn total_time(&self) -> f64 {
        self.close_stop() - self.open_start()
    }
}

thread_local! {
    /// Per-thread monotonic epoch, lazily initialized on first call to `now()`.
    static EPOCH: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// Get current timestamp in seconds (monotonic, relative to first call on this thread).
pub fn now() -> f64 {
    EPOCH.with(|cell| {
        let epoch = match cell.get() {
            Some(e) => e,
            None => {
                let e = Instant::now();
                cell.set(Some(e));
                e
            }
        };
        epoch.elapsed().as_secs_f64()
    })
}
