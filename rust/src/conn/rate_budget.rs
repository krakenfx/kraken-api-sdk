//! Host-scoped sliding-window rate limiter for WS CONNECT/UPGRADE attempts.
//! One budget is shared across the Public + Auth connections to the same host;
//! the per-window cap and window length are caller-tunable knobs (no fixed cap).

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use crate::types::MonotonicInstant;

/// Default window: 10 minutes.
#[cfg(test)]
const DEFAULT_WINDOW: Duration = Duration::from_secs(600);
/// Default attempt cap per window: ~120 (headroom under Kraken's ~150 limit).
#[cfg(test)]
const DEFAULT_BUDGET: u32 = 120;

/// Error returned by `try_consume` when the budget is exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionRateThrottled {
    /// Number of attempts currently in the window.
    pub attempts_used: u32,
    /// Configured budget per window.
    pub budget: u32,
    /// When the oldest in-window attempt falls out — caller arms
    /// `TIMER.rate_budget_window_advanced` to this instant.
    pub throttle_until_monotonic: MonotonicInstant,
}

/// Host-scoped sliding-window rate limiter, consulted before each connect
/// attempt across all connections targeting the same host.
pub struct ConnectionRateBudget {
    inner: Mutex<Inner>,
    window: Duration,
    budget: u32,
}

struct Inner {
    attempts: VecDeque<MonotonicInstant>,
}

impl ConnectionRateBudget {
    /// Construct with default knobs (120 / 10 min target).
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::with_knobs(DEFAULT_BUDGET, DEFAULT_WINDOW)
    }

    /// Construct with a caller-configured per-window attempt cap AND window.
    /// The SDK assumes no fixed exchange cap — both are caller-tunable.
    pub(crate) fn with_knobs(budget: u32, window: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner {
                attempts: VecDeque::with_capacity(budget as usize),
            }),
            window,
            budget,
        }
    }

    /// Try to consume a budget slot at `now`; `Err(ConnectionRateThrottled)`
    /// when the window is full. On `Err` the caller MUST transition to
    /// `BackingOff` and re-arm the rate-budget timer, not drop the request.
    pub(crate) fn try_consume(&self, now: MonotonicInstant) -> Result<(), ConnectionRateThrottled> {
        let mut guard = self
            .inner
            .lock()
            .expect("ConnectionRateBudget lock poisoned");
        self.prune(&mut guard, now);
        if guard.attempts.len() as u32 >= self.budget {
            // `front()` is Some whenever budget >= 1 (config rejects a 0 budget).
            // The None arm falls through and records rather than panic the reactor.
            if let Some(oldest) = guard.attempts.front().copied() {
                return Err(ConnectionRateThrottled {
                    attempts_used: guard.attempts.len() as u32,
                    budget: self.budget,
                    throttle_until_monotonic: MonotonicInstant(oldest.0 + self.window),
                });
            }
        }
        guard.attempts.push_back(now);
        Ok(())
    }

    /// Configured window size in seconds. Surfaced for the
    /// `ConnectionRateThrottledEvent.window_seconds` event payload field.
    pub(crate) fn window_seconds(&self) -> u32 {
        self.window.as_secs() as u32
    }

    /// Drop attempts older than `now - self.window`.
    fn prune(&self, inner: &mut Inner, now: MonotonicInstant) {
        let cutoff = now.0.checked_sub(self.window).unwrap_or_default();
        while let Some(front) = inner.attempts.front() {
            if front.0 < cutoff {
                inner.attempts.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn inst(s: u64) -> MonotonicInstant {
        MonotonicInstant(Duration::from_secs(s))
    }

    #[test]
    fn try_consume_within_budget_returns_ok() {
        let b = ConnectionRateBudget::with_knobs(3, Duration::from_secs(60));
        assert!(b.try_consume(inst(0)).is_ok());
        assert!(b.try_consume(inst(1)).is_ok());
        assert!(b.try_consume(inst(2)).is_ok());
    }

    #[test]
    fn try_consume_at_budget_returns_throttled_with_throttle_until() {
        let b = ConnectionRateBudget::with_knobs(2, Duration::from_secs(60));
        b.try_consume(inst(0)).unwrap();
        b.try_consume(inst(10)).unwrap();
        let err = b.try_consume(inst(20)).unwrap_err();
        assert_eq!(err.attempts_used, 2);
        assert_eq!(err.budget, 2);
        assert_eq!(err.throttle_until_monotonic.0, Duration::from_secs(60));
    }

    #[test]
    fn try_consume_after_window_advances_succeeds() {
        let b = ConnectionRateBudget::with_knobs(1, Duration::from_secs(10));
        b.try_consume(inst(0)).unwrap();
        assert!(b.try_consume(inst(20)).is_ok());
    }
}
