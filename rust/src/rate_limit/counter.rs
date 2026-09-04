//! `RateLimitCounter` — decaying numeric counter and canonical decay arithmetic.

use std::time::Duration;

use crate::types::MonotonicInstant;

/// Minimum interval between successive `RateLimitWarning` emissions for one
/// scope while at/above threshold.
const WARNING_DEBOUNCE: Duration = Duration::from_secs(1);

/// Outcome of a [`RateLimitCounter::charge`] attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "an exceeded charge must be handled (surface RateLimitExceeded)"]
pub(crate) enum ChargeOutcome {
    /// `cost` fit under the cap; the counter was bumped.
    Charged,
    /// `decayed + cost > cap`; counter left uncharged.
    Exceeded,
}

/// Per-scope decaying counter. Mutated only inside a tracker `Mutex` critical section.
#[derive(Debug, Clone)]
pub(crate) struct RateLimitCounter {
    pub current: f64,
    pub cap: f64,
    pub decay_per_sec: f64,
    pub last_updated: MonotonicInstant,
    /// Last `RateLimitWarning` emission; `None` while below threshold (re-arms on drop-below).
    last_warning_at: Option<MonotonicInstant>,
}

impl RateLimitCounter {
    pub(crate) fn new(cap: f64, decay_per_sec: f64, now: MonotonicInstant) -> Self {
        debug_assert!(decay_per_sec >= 0.0, "decay_per_sec MUST be non-negative");
        debug_assert!(cap >= 0.0, "cap MUST be non-negative");
        Self {
            current: 0.0,
            cap,
            decay_per_sec,
            last_updated: now,
            last_warning_at: None,
        }
    }

    /// Level-triggered, debounced warning decision after a successful `charge`.
    pub(crate) fn should_emit_warning(&mut self, now: MonotonicInstant, warning_pct: f64) -> bool {
        let pct = self.current / self.cap;
        if pct < warning_pct {
            self.last_warning_at = None;
            return false;
        }
        match self.last_warning_at {
            Some(prev) if now.0.saturating_sub(prev.0) < WARNING_DEBOUNCE => false,
            _ => {
                self.last_warning_at = Some(now);
                true
            }
        }
    }

    /// Decay projection at `now` without mutating: `max(0, current - decay_per_sec * elapsed)`.
    pub(crate) fn peek_current(&self, now: MonotonicInstant) -> f64 {
        // `now` is sampled before the tracker lock, so concurrent charges can
        // present `now < last_updated`. `saturating_sub` clamps to zero elapsed.
        let elapsed = now.0.saturating_sub(self.last_updated.0).as_secs_f64();
        (self.current - self.decay_per_sec * elapsed).max(0.0)
    }

    /// Atomic `decay → check → charge → bump`.
    pub(crate) fn charge(&mut self, cost: f64, now: MonotonicInstant) -> ChargeOutcome {
        let decayed = self.peek_current(now);
        if decayed + cost > self.cap {
            // Surface the decay observation for subsequent `headroom()`, but do not charge.
            self.current = decayed;
            // Keep `last_updated` monotonic: a backwards `now` must not regress it.
            self.last_updated = MonotonicInstant(now.0.max(self.last_updated.0));
            return ChargeOutcome::Exceeded;
        }
        self.current = decayed + cost;
        self.last_updated = MonotonicInstant(now.0.max(self.last_updated.0));
        ChargeOutcome::Charged
    }

    /// Reverse a prior `charge` when the op never reached the wire.
    pub(crate) fn credit(&mut self, cost: f64, now: MonotonicInstant) {
        let decayed = self.peek_current(now);
        self.current = (decayed - cost).max(0.0);
        self.last_updated = MonotonicInstant(now.0.max(self.last_updated.0));
    }

    /// Non-rejecting charge clamped at `cap` (e.g. cancel_all must never block).
    pub(crate) fn charge_saturating(&mut self, cost: f64, now: MonotonicInstant) {
        let decayed = self.peek_current(now);
        self.current = (decayed + cost).min(self.cap);
        self.last_updated = MonotonicInstant(now.0.max(self.last_updated.0));
    }

    /// Force the counter to its cap (`EAPI:Rate limit exceeded` reconcile).
    pub(crate) fn snap_to_cap(&mut self, now: MonotonicInstant) {
        self.current = self.cap;
        self.last_updated = MonotonicInstant(now.0.max(self.last_updated.0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn inst(s: u64) -> MonotonicInstant {
        MonotonicInstant(Duration::from_secs(s))
    }

    fn frac_inst(s: f64) -> MonotonicInstant {
        MonotonicInstant(Duration::from_secs_f64(s))
    }

    #[test]
    fn fresh_counter_has_zero_current() {
        let c = RateLimitCounter::new(15.0, 0.33, inst(0));
        assert_eq!(c.current, 0.0);
        assert_eq!(c.peek_current(inst(10)), 0.0);
    }

    #[test]
    fn charge_within_cap_succeeds_and_mutates() {
        let mut c = RateLimitCounter::new(15.0, 0.33, inst(0));
        assert_eq!(c.charge(5.0, inst(0)), ChargeOutcome::Charged);
        assert!((c.current - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn charge_beyond_cap_is_exceeded_no_mutation_on_current() {
        let mut c = RateLimitCounter::new(15.0, 0.33, inst(0));
        assert_eq!(c.charge(14.0, inst(0)), ChargeOutcome::Charged);
        assert_eq!(c.charge(2.0, inst(0)), ChargeOutcome::Exceeded);
        // Exceeded still surfaces the decay (here zero elapsed) but does not charge the +2.
        assert!((c.current - 14.0).abs() < f64::EPSILON);
    }

    #[test]
    fn decay_reduces_current_proportionally_to_elapsed() {
        let mut c = RateLimitCounter::new(15.0, 1.0, inst(0));
        assert_eq!(c.charge(10.0, inst(0)), ChargeOutcome::Charged);
        let peeked = c.peek_current(inst(5));
        assert!((peeked - 5.0).abs() < 1e-9);
    }

    #[test]
    fn decay_clamps_at_zero() {
        let mut c = RateLimitCounter::new(15.0, 1.0, inst(0));
        assert_eq!(c.charge(5.0, inst(0)), ChargeOutcome::Charged);
        assert_eq!(c.peek_current(inst(100)), 0.0);
    }

    #[test]
    fn charge_after_decay_uses_decayed_baseline() {
        let mut c = RateLimitCounter::new(15.0, 1.0, inst(0));
        assert_eq!(c.charge(10.0, inst(0)), ChargeOutcome::Charged);
        assert_eq!(c.charge(8.0, inst(5)), ChargeOutcome::Charged);
        assert!((c.current - 13.0).abs() < 1e-9);
    }

    #[test]
    fn fractional_elapsed_decays_proportionally() {
        let mut c = RateLimitCounter::new(15.0, 2.0, inst(0));
        assert_eq!(c.charge(10.0, inst(0)), ChargeOutcome::Charged);
        let peeked = c.peek_current(frac_inst(0.5));
        assert!((peeked - 9.0).abs() < 1e-9);
    }

    #[test]
    fn snap_to_cap_forces_counter_to_cap() {
        let mut c = RateLimitCounter::new(15.0, 0.33, inst(0));
        c.snap_to_cap(inst(10));
        assert_eq!(c.current, 15.0);
        assert_eq!(c.last_updated, inst(10));
    }

    #[test]
    fn credit_subtracts_from_current_and_floors_at_zero() {
        let mut c = RateLimitCounter::new(60.0, 1.0, inst(0));
        assert_eq!(c.charge(10.0, inst(0)), ChargeOutcome::Charged);
        c.credit(10.0, inst(0));
        assert!((c.current - 0.0).abs() < 1e-9);
        assert_eq!(c.charge(3.0, inst(0)), ChargeOutcome::Charged);
        c.credit(9.0, inst(0));
        assert_eq!(c.current, 0.0);
    }

    #[test]
    fn should_emit_warning_is_level_triggered_and_debounced() {
        let mut c = RateLimitCounter::new(100.0, 1.0, inst(0));
        c.current = 50.0;
        assert!(!c.should_emit_warning(inst(0), 0.8));
        c.current = 85.0;
        assert!(c.should_emit_warning(inst(0), 0.8));
        assert!(!c.should_emit_warning(frac_inst(0.5), 0.8));
        assert!(c.should_emit_warning(frac_inst(1.5), 0.8));
        c.current = 50.0;
        assert!(!c.should_emit_warning(frac_inst(1.6), 0.8));
        c.current = 85.0;
        assert!(c.should_emit_warning(frac_inst(1.7), 0.8));
    }

    #[test]
    fn peek_current_tolerates_backwards_now_without_panic() {
        // Backwards `now` (sampled outside the lock) must clamp to zero elapsed.
        let mut c = RateLimitCounter::new(15.0, 1.0, inst(10));
        assert_eq!(c.charge(5.0, inst(10)), ChargeOutcome::Charged);
        assert!((c.peek_current(inst(5)) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn charge_keeps_last_updated_monotonic_under_backwards_now() {
        let mut c = RateLimitCounter::new(15.0, 1.0, inst(10));
        assert_eq!(c.charge(5.0, inst(10)), ChargeOutcome::Charged);
        assert_eq!(c.charge(1.0, inst(4)), ChargeOutcome::Charged);
        assert_eq!(c.last_updated, inst(10));
        assert!((c.current - 6.0).abs() < 1e-9);
    }
}
