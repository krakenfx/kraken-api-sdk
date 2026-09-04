//! Monotonic timestamp for event payloads.

use std::time::Duration;

use serde::Serialize;

/// Monotonic timestamp for event payloads — I/O-loop monotonic clock, not wall
/// clock. Construct via [`now`](Self::now); read via [`as_duration`](Self::as_duration).
///
/// Serializes as integer milliseconds since the process epoch. Sub-millisecond
/// precision truncates (floor), so distinct instants in the same millisecond
/// emit equal values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MonotonicInstant(pub(crate) Duration);

impl Serialize for MonotonicInstant {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0.as_millis() as u64)
    }
}

impl MonotonicInstant {
    /// Sample the monotonic clock now. Returns the elapsed duration since the
    /// process-epoch `Instant`, which is fixed on first call.
    pub fn now() -> Self {
        use std::sync::OnceLock;
        static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
        let epoch = EPOCH.get_or_init(std::time::Instant::now);
        Self(std::time::Instant::now().saturating_duration_since(*epoch))
    }

    /// Elapsed since the process monotonic epoch. No public `Duration` constructor
    /// (that would let a caller forge a non-monotonic timestamp).
    #[must_use]
    pub fn as_duration(&self) -> Duration {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_instant_is_monotonic_and_nondecreasing() {
        let t1 = MonotonicInstant::now();
        std::thread::sleep(std::time::Duration::from_micros(10));
        let t2 = MonotonicInstant::now();
        assert!(t2 >= t1);
    }

    /// Scalar u64 milliseconds — the wire contract across ports.
    #[test]
    fn serializes_as_scalar_milliseconds() {
        let t = MonotonicInstant(Duration::new(2, 250_000_000));
        let v = serde_json::to_value(t).expect("MonotonicInstant serializes");
        assert_eq!(v, serde_json::json!(2_250_u64));
        // Sub-millisecond precision truncates (floor): 2000.6ms must emit 2000,
        // never the rounded 2001 — ports that round break emitted-shape parity.
        let t = MonotonicInstant(Duration::new(2, 600_000));
        assert_eq!(
            serde_json::to_value(t).expect("serializes"),
            serde_json::json!(2_000_u64)
        );
    }

    #[test]
    fn as_duration_reads_elapsed_since_epoch() {
        let d1 = MonotonicInstant::now().as_duration();
        std::thread::sleep(std::time::Duration::from_micros(10));
        let d2 = MonotonicInstant::now().as_duration();
        assert!(d2 >= d1);
    }
}
