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

/// Test clock pinned at one instant (a duration since the monotonic epoch).
#[cfg(test)]
pub(crate) struct FixedClock(pub(crate) std::time::Duration);

#[cfg(test)]
impl Clock for FixedClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant(self.0)
    }
}
