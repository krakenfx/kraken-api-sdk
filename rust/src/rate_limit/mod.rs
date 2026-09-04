//! Rate-limit accounting: two independent Spot trackers —
//! [`SpotApiRateLimitTracker`] (non-trading REST, keyed by API key) and
//! [`SpotTradingRateLimitTracker`] (REST + WS v2, keyed by `(api_key, pair)`).

mod api;
mod cl_ord_id_index;
mod counter;
mod order_age_cost;
mod scope;
pub(crate) mod snap_classify;
mod tier;
mod trading;

pub use api::SpotApiRateLimitTracker;
pub use cl_ord_id_index::ClOrdIdPairIndex;
pub use order_age_cost::{DynamicCostOp, order_age_cost};
pub use trading::SpotTradingRateLimitTracker;

pub use scope::Scope;
pub use tier::Tier;

pub(crate) use snap_classify::{SnapTarget, classify_rate_limit_snap};

/// Raised by `consume` when the counter would exceed its cap after the proposed
/// charge. The `tracker` discriminator is "api" or "trading";
/// `ConnectionRateThrottled` covers the connection-rate case.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error(
    "Rate-limit exceeded: tracker={tracker}, scope={scope:?}, current={current:.2}, cap={cap:.2}."
)]
pub struct RateLimitExceeded {
    pub tracker: &'static str,
    pub scope: Scope,
    pub current: f64,
    pub cap: f64,
    /// When the next slot of headroom is expected (caller may surface as
    /// `retry_after` to the user).
    pub retry_at_monotonic: crate::types::MonotonicInstant,
}

/// Upper bound (seconds) on a projected retry-at horizon; keeps `Duration` build panic-free.
const MAX_RETRY_PROJECTION_SECS: f64 = 86_400.0;

/// Project when a counter regains ≥ 1.0 unit of headroom. Non-positive decay → "retry now".
fn compute_retry_at(
    current: f64,
    cap: f64,
    decay_per_sec: f64,
    now: crate::types::MonotonicInstant,
) -> crate::types::MonotonicInstant {
    // NaN or non-positive decay → retry immediately (NaN comparisons are always false).
    if decay_per_sec.is_nan() || decay_per_sec <= 0.0 {
        return now;
    }
    let needed = (current - (cap - 1.0)).max(0.0);
    // Clamp: unclamped +inf secs would panic `from_secs_f64` or overflow `now + dur`.
    let secs = (needed / decay_per_sec).clamp(0.0, MAX_RETRY_PROJECTION_SECS);
    crate::types::MonotonicInstant(
        now.0
            .saturating_add(std::time::Duration::from_secs_f64(secs)),
    )
}

/// Publish one `RateLimitWarning` for `scope`.
#[allow(clippy::too_many_arguments)]
fn publish_rate_limit_warning(
    bus: &crate::dispatch::DispatchEventBus,
    clock: &dyn crate::clock::Clock,
    tracker: &'static str,
    scope: &Scope,
    used: f64,
    cap: f64,
    pct: f64,
    decay_per_sec: f64,
    seconds_to_drain: f64,
) {
    let (key_id_fingerprint, pair) = scope.redacted_id();
    bus.publish(crate::dispatch::EventEnvelope {
        event_type: crate::dispatch::EventType::RateLimitWarning,
        event_version: 1,
        timestamp_monotonic: clock.now(),
        request_id: None,
        payload: crate::dispatch::EventPayload::RateLimitWarning {
            tracker,
            key_id_fingerprint,
            pair,
            used,
            cap,
            pct,
            decay_per_sec,
            seconds_to_drain,
        },
    });
}

/// Publish one `RateLimitExceededEvent` after a reactive `snap_to_cap`.
fn publish_rate_limit_exceeded(
    bus: &crate::dispatch::DispatchEventBus,
    clock: &dyn crate::clock::Clock,
    tracker: &'static str,
    scope: &Scope,
    kraken_error: &str,
    now: crate::types::MonotonicInstant,
) {
    let observed_at = clock.now();
    let (key_id_fingerprint, pair) = scope.redacted_id();
    bus.publish(crate::dispatch::EventEnvelope {
        event_type: crate::dispatch::EventType::RateLimitExceededEvent,
        event_version: 1,
        timestamp_monotonic: observed_at,
        request_id: None,
        payload: crate::dispatch::EventPayload::RateLimitExceededEvent {
            tracker,
            key_id_fingerprint,
            pair,
            kraken_error: kraken_error.to_owned(),
            observed_at_monotonic: now,
        },
    });
}

/// Crate-internal rate-limit error; never constructed in production. Retained
/// as the golden-vector census reference for the locked error taxonomy that
/// the other language ports must reproduce.
#[allow(dead_code)] // census-only reference.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum RateLimitError {
    #[error("API rate-limit counter exceeded.")]
    ApiCounterExceeded {
        scope: Option<String>,
        retry_after_ts: Option<u64>,
    },
    #[error("Trading rate-limit counter exceeded.")]
    TradingCounterExceeded {
        scope: Option<String>,
        retry_after_ts: Option<u64>,
    },
    #[error("Service throttled.")]
    ServiceThrottled { retry_after_ts: Option<u64> },
    #[error("Client is closed; no new operations accepted.")]
    ClientClosed,
    #[error("{kraken_message}")]
    Unknown {
        kraken_code: String,
        kraken_message: String,
    },
}

impl crate::error::sealed::Sealed for RateLimitError {}

impl crate::error::ApiError for RateLimitError {
    fn code(&self) -> &str {
        use RateLimitError::*;
        match self {
            ApiCounterExceeded { .. } | TradingCounterExceeded { .. } => "RATE_LIMIT_EXCEEDED",
            ServiceThrottled { .. } => "SERVICE_THROTTLED",
            ClientClosed => "CLIENT_CLOSED",
            Unknown { .. } => "UNKNOWN",
        }
    }
    fn category(&self) -> crate::error::ErrorCategory {
        use crate::error::ErrorCategory;
        match self {
            RateLimitError::ClientClosed => ErrorCategory::Client,
            RateLimitError::Unknown { .. } => ErrorCategory::Exchange,
            _ => ErrorCategory::RateLimit,
        }
    }
    fn retryable(&self) -> bool {
        !matches!(
            self,
            RateLimitError::ClientClosed | RateLimitError::Unknown { .. }
        )
    }
    crate::error::api_error_tail!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    fn now() -> MonotonicInstant {
        MonotonicInstant(Duration::from_secs(1_000))
    }

    #[test]
    fn compute_retry_at_projects_forward_by_needed_over_decay() {
        let r = compute_retry_at(100.0, 60.0, 1.0, now());
        assert_eq!(r.0, now().0 + Duration::from_secs_f64(41.0));
    }

    #[test]
    fn compute_retry_at_below_cap_is_now() {
        assert_eq!(compute_retry_at(10.0, 60.0, 1.0, now()).0, now().0);
    }

    #[test]
    fn compute_retry_at_nonpositive_or_nan_decay_is_now() {
        assert_eq!(compute_retry_at(100.0, 60.0, 0.0, now()).0, now().0);
        assert_eq!(compute_retry_at(100.0, 60.0, -1.0, now()).0, now().0);
        assert_eq!(compute_retry_at(100.0, 60.0, f64::NAN, now()).0, now().0);
    }

    #[test]
    fn compute_retry_at_clamps_overflowing_projection_no_panic() {
        // Denormal/non-finite decay would panic `from_secs_f64`; projection clamps instead.
        let horizon = now()
            .0
            .saturating_add(Duration::from_secs_f64(MAX_RETRY_PROJECTION_SECS));
        assert_eq!(
            compute_retry_at(f64::MAX, 60.0, f64::MIN_POSITIVE, now()).0,
            horizon
        );
        assert_eq!(compute_retry_at(f64::INFINITY, 60.0, 1.0, now()).0, horizon);
    }
}
