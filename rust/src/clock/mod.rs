//! `Clock` trait — monotonic time for event timestamps (not wall clock).

use crate::types::MonotonicInstant;

/// Monotonic clock. Implementations must be `Send + Sync`.
pub trait Clock: Send + Sync {
    /// The current monotonic instant.
    fn now(&self) -> MonotonicInstant;
}

/// Production clock backed by `std::time::Instant`.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::now()
    }
}
