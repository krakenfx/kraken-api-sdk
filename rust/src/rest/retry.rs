//! REST transient-retry engine: pure classify + backoff. The Stage-6 loop that
//! drives it lives in [`crate::rest::surface`].

use std::sync::Arc;
use std::time::Duration;

use crate::build::knobs::Knobs;
use crate::jitter::JitterSource;
use crate::rest::surface::RestError;
use crate::transport::TransportErrorKind;

/// Typed retry reason — `reason` on `RestRetryAttempt`. Cross-binding contract:
/// all ports share this variant set; string form keeps PascalCase names.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[non_exhaustive]
pub enum RetryReason {
    /// Transport-transient: connect-timeout / refused, DNS, TLS handshake, socket reset.
    TransportError,
    /// Raw HTTP 5xx + retryable 4xx (408 / 425 / 429); rare (Kraken wraps errors in a 200).
    HttpServerError,
    /// Rate-limit breach → cooperative backoff (API counter; trading-engine on cancel path).
    RateLimitExceeded,
    /// `EService:Throttled: <ts>` — Kraken-specified delay; `<ts>` parsed by the envelope seam.
    ServiceThrottled,
    /// `EService:Unavailable` / `EService:Busy` — matching engine briefly offline / busy.
    ServiceUnavailable,
    /// `EAPI:Invalid nonce` — retry once with a fresh nonce, then bubble non-transient.
    InvalidNonce,
}

/// Backoff shape on a [`RetryPolicy`]. In v1 the Stage-6 loop derives wait from
/// [`RetryReason`]; this field lets never-retry declare `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffClass {
    /// Exponential full-jitter (default for retryable ops).
    Exponential,
    /// Never backs off because the op never retries.
    None,
}

/// Per-call retry policy for `RestSurface` entry points. Serves both retryable
/// reads/cancels and never-retry placements. `Copy` — cheap per call.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    /// Total attempt budget (first try = attempt 1). `None` → engine default.
    pub max_attempts: Option<u32>,
    /// Whether transient failures retry. `false` → single attempt.
    pub transient: bool,
    /// Declared backoff shape (see [`BackoffClass`]).
    pub backoff_class: BackoffClass,
    /// Allow one fresh-nonce re-sign on `EAPI:Invalid nonce` even when
    /// `transient` is `false` (validate-mode only — places nothing).
    pub nonce_heal: bool,
}

impl RetryPolicy {
    /// Retry transients with exponential backoff. Public GETs, idempotent
    /// private reads, and cancels. `max_attempts: None` → engine default (3).
    pub fn idempotent() -> Self {
        Self {
            max_attempts: None,
            transient: true,
            backoff_class: BackoffClass::Exponential,
            nonce_heal: true,
        }
    }

    /// Never retry. Used by AddOrder / AmendOrder / EditOrder. Pins `Some(1)`
    /// so a placement never auto-resends.
    pub fn never_retry() -> Self {
        Self {
            max_attempts: Some(1),
            transient: false,
            backoff_class: BackoffClass::None,
            nonce_heal: false,
        }
    }

    /// Never retry except one fresh-nonce re-sign on `EAPI:Invalid nonce`.
    /// Validate-mode only (places nothing). See docs/guides/error-handling.md.
    pub fn never_retry_except_nonce() -> Self {
        Self {
            max_attempts: Some(1),
            transient: false,
            backoff_class: BackoffClass::None,
            nonce_heal: true,
        }
    }
}

/// Outcome of [`RetryEngine::classify`]. `Retry` carries [`RetryReason`];
/// `classify` is clock-free — Stage-6 owns duration computation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Transient — retry. The loop computes backoff from the reason.
    Retry(RetryReason),
    /// Non-transient — bubble; do not retry.
    NonTransient,
    /// `AddOrder`/`AmendOrder` transport-drop ambiguity. Producer-less in v1
    /// (never-retry short-circuits before classify); kept for cross-binding parity.
    #[allow(dead_code)]
    AmbiguousOrderPlacement,
}

/// Pure retry decision + backoff. Built once at `.build()` from `rest_retry_*`
/// knobs (separate from WS reconnect `backoff_*`). Owned by `RestSurface`.
pub struct RetryEngine {
    backoff_base_ms: u32,
    backoff_max_ms: u32,
    backoff_factor: f64,
    /// Default budget when `idempotent()` leaves `max_attempts` as `None`.
    max_attempts: u32,
    /// Full-jitter source; `FixedJitter` makes `next_backoff` deterministic in tests.
    jitter: Arc<dyn JitterSource>,
    /// When set, persistent `EAPI:Invalid nonce` emits `NoncePoisonedEvent`.
    nonce_recovery: bool,
}

impl RetryEngine {
    /// Build from `rest_retry_*` knobs + jitter. Construction-only; no clock, no I/O.
    pub fn from_knobs(knobs: &Knobs, jitter: Arc<dyn JitterSource>) -> Self {
        Self {
            backoff_base_ms: knobs.rest_retry_base_ms,
            backoff_max_ms: knobs.rest_retry_max_ms,
            backoff_factor: knobs.rest_retry_factor,
            max_attempts: knobs.rest_retry_max_attempts,
            jitter,
            nonce_recovery: knobs.nonce_recovery,
        }
    }

    /// Knob-derived attempt budget for `idempotent()` without a pinned `max_attempts`.
    pub fn default_max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Whether `nonce_recovery` is on (emit `NoncePoisonedEvent` on persistent invalid nonce).
    pub fn nonce_recovery(&self) -> bool {
        self.nonce_recovery
    }

    /// Classify a failed attempt. Pure: no clock, no wire re-parse.
    /// `never_retry()` short-circuits unless validate-mode nonce heal applies.
    pub fn classify(&self, err: &RestError, policy: &RetryPolicy) -> RetryDecision {
        if !policy.transient {
            // Validate-mode alone may take the single fresh-nonce re-sign.
            if policy.nonce_heal {
                if let RestError::Kraken(codes) = err {
                    if classify_kraken(codes.first().map(String::as_str))
                        == RetryDecision::Retry(RetryReason::InvalidNonce)
                    {
                        return RetryDecision::Retry(RetryReason::InvalidNonce);
                    }
                }
            }
            return RetryDecision::NonTransient;
        }
        match err {
            RestError::Transport { kind, transient } => {
                if !*transient {
                    RetryDecision::NonTransient
                } else if is_http_status_kind(kind) {
                    RetryDecision::Retry(RetryReason::HttpServerError)
                } else {
                    RetryDecision::Retry(RetryReason::TransportError)
                }
            }
            RestError::Kraken(codes) => classify_kraken(codes.first().map(String::as_str)),
            // Local Stage-1 rejection: SDK never blocks on its own limit — bubble.
            RestError::RateLimit(_) => RetryDecision::NonTransient,
            RestError::Auth(_) | RestError::UnexpectedShape(_) => RetryDecision::NonTransient,
        }
    }

    /// Full-jitter backoff: `ceiling = min(base × factor^attempt, max)`, then
    /// `delay = ceiling × jitter.next_unit()`. `attempt` is 0-based.
    pub fn next_backoff(&self, attempt: u32) -> Duration {
        let base_ms = f64::from(self.backoff_base_ms);
        let max_ms = f64::from(self.backoff_max_ms);
        // Floor growth factor to >= 1 (non-finite → 2.0); mirrors WS reconnect.
        let factor = if self.backoff_factor.is_finite() {
            self.backoff_factor.max(1.0)
        } else {
            2.0
        };

        let exp = i32::try_from(attempt).unwrap_or(i32::MAX);
        let scaled = base_ms * factor.powi(exp);
        // Guard NaN / +inf from factor^huge — fall back to the ceiling.
        let computed = if scaled.is_finite() {
            scaled.min(max_ms)
        } else {
            max_ms
        };
        let delay_ms = computed * self.jitter.next_unit();
        Duration::from_millis(delay_ms as u64)
    }

    /// Backoff ceiling (`rest_retry_max_ms`) — bounds a `ServiceThrottled`
    /// wall-clock wait so a bogus far-future `retry_after_ts` cannot stall.
    pub fn backoff_ceiling(&self) -> Duration {
        Duration::from_millis(u64::from(self.backoff_max_ms))
    }
}

/// Map a Kraken error string to a retry decision. Unknown codes are non-transient.
fn classify_kraken(first: Option<&str>) -> RetryDecision {
    let Some(code) = first else {
        return RetryDecision::NonTransient;
    };
    use RetryReason::*;
    if code.starts_with("EAPI:Rate limit exceeded") || code.starts_with("EAuth:Rate limit exceeded")
    {
        RetryDecision::Retry(RateLimitExceeded)
    } else if code.starts_with("EOrder:Rate limit exceeded")
        || code.starts_with("EOrder:Domain rate limit exceeded")
    {
        // Trading-engine breach; reachable on cancel (placements are never_retry).
        RetryDecision::Retry(RateLimitExceeded)
    } else if code.starts_with("EGeneral:Too many requests") {
        RetryDecision::Retry(RateLimitExceeded)
    } else if code.starts_with("EService:Throttled") {
        RetryDecision::Retry(ServiceThrottled)
    } else if code.starts_with("EService:Unavailable") || code.starts_with("EService:Busy") {
        RetryDecision::Retry(ServiceUnavailable)
    } else if code.starts_with("EAPI:Invalid nonce") {
        RetryDecision::Retry(InvalidNonce)
    } else {
        // EService:Deadline elapsed, market gates, Invalid key/signature,
        // EOrder:* business rejections, Temporary lockout, unmapped → non-transient.
        RetryDecision::NonTransient
    }
}

/// Whether a transient transport came from a raw non-2xx status (observability reason).
fn is_http_status_kind(kind: &TransportErrorKind) -> bool {
    matches!(kind, TransportErrorKind::HttpStatus { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jitter::FixedJitter;

    fn engine() -> RetryEngine {
        RetryEngine::from_knobs(&Knobs::defaults(), Arc::new(FixedJitter(1.0)))
    }

    fn kraken(code: &str) -> RestError {
        RestError::Kraken(vec![code.to_string()])
    }

    #[test]
    fn transient_transport_retries_as_transport_error() {
        let e = engine();
        let err = RestError::Transport {
            kind: TransportErrorKind::TcpConnectTimeout,
            transient: true,
        };
        assert_eq!(
            e.classify(&err, &RetryPolicy::idempotent()),
            RetryDecision::Retry(RetryReason::TransportError)
        );
    }

    #[test]
    fn transient_http_5xx_retries_as_http_server_error() {
        let e = engine();
        let err = RestError::Transport {
            kind: TransportErrorKind::HttpStatus { status: 503 },
            transient: true,
        };
        assert_eq!(
            e.classify(&err, &RetryPolicy::idempotent()),
            RetryDecision::Retry(RetryReason::HttpServerError)
        );
    }

    #[test]
    fn transient_http_4xx_throttle_retries_as_http_server_error() {
        let e = engine();
        for code in [408, 425, 429] {
            let err = RestError::Transport {
                kind: TransportErrorKind::HttpStatus { status: code },
                transient: true,
            };
            assert_eq!(
                e.classify(&err, &RetryPolicy::idempotent()),
                RetryDecision::Retry(RetryReason::HttpServerError),
                "HTTP {code} should classify as HttpServerError"
            );
        }
    }

    #[test]
    fn non_transient_transport_does_not_retry() {
        let e = engine();
        let err = RestError::Transport {
            kind: TransportErrorKind::HttpStatus { status: 400 },
            transient: false,
        };
        assert_eq!(
            e.classify(&err, &RetryPolicy::idempotent()),
            RetryDecision::NonTransient
        );
    }

    #[test]
    fn kraken_codes_classify_as_retryable_reason() {
        use RetryReason::*;
        let e = engine();
        for (code, reason) in [
            ("EAPI:Rate limit exceeded", RateLimitExceeded),
            ("EOrder:Rate limit exceeded", RateLimitExceeded),
            ("EOrder:Domain rate limit exceeded", RateLimitExceeded),
            ("EGeneral:Too many requests", RateLimitExceeded),
            ("EAuth:Rate limit exceeded", RateLimitExceeded),
            ("EService:Throttled: 1716287000", ServiceThrottled),
            ("EService:Unavailable", ServiceUnavailable),
            ("EService:Busy", ServiceUnavailable),
            ("EAPI:Invalid nonce", InvalidNonce),
        ] {
            assert_eq!(
                e.classify(&kraken(code), &RetryPolicy::idempotent()),
                RetryDecision::Retry(reason),
                "{code} must retry as {reason:?}"
            );
        }
    }

    /// Strict never-retry blocks even the nonce heal.
    #[test]
    fn never_retry_blocks_invalid_nonce() {
        let e = engine();
        assert_eq!(
            e.classify(&kraken("EAPI:Invalid nonce"), &RetryPolicy::never_retry()),
            RetryDecision::NonTransient
        );
    }

    /// Validate-mode unlocks only the nonce heal.
    #[test]
    fn nonce_healed_policy_unlocks_only_invalid_nonce() {
        let e = engine();
        let p = RetryPolicy::never_retry_except_nonce();
        assert_eq!(
            e.classify(&kraken("EAPI:Invalid nonce"), &p),
            RetryDecision::Retry(RetryReason::InvalidNonce)
        );
        for code in [
            "EOrder:Insufficient funds",
            "EService:Unavailable",
            "EAPI:Rate limit exceeded",
        ] {
            assert_eq!(
                e.classify(&kraken(code), &p),
                RetryDecision::NonTransient,
                "{code} must not retry under a never-retry policy"
            );
        }
        let transport = RestError::Transport {
            kind: TransportErrorKind::TcpConnectTimeout,
            transient: true,
        };
        assert_eq!(e.classify(&transport, &p), RetryDecision::NonTransient);
    }

    #[test]
    fn egeneral_temporary_lockout_is_non_transient() {
        let e = engine();
        assert_eq!(
            e.classify(
                &kraken("EGeneral:Temporary lockout"),
                &RetryPolicy::idempotent()
            ),
            RetryDecision::NonTransient
        );
    }

    #[test]
    fn eorder_invalid_order_is_non_transient() {
        let e = engine();
        assert_eq!(
            e.classify(&kraken("EOrder:Invalid order"), &RetryPolicy::idempotent()),
            RetryDecision::NonTransient
        );
    }

    #[test]
    fn eservice_deadline_elapsed_is_non_transient() {
        let e = engine();
        assert_eq!(
            e.classify(
                &kraken("EService:Deadline elapsed"),
                &RetryPolicy::idempotent()
            ),
            RetryDecision::NonTransient
        );
    }

    #[test]
    fn market_gate_and_business_rejections_are_non_transient() {
        let e = engine();
        for code in [
            "EService:Market in cancel_only mode",
            "EService:Market in post_only mode",
            "EAPI:Invalid key",
            "EAPI:Invalid signature",
            "EOrder:Insufficient funds",
            "EGeneral:Permission denied",
            "EGeneral:Internal error",
            "EGeneral:Invalid arguments",
            "EGeneral:Temporary lockout",
            "EAuth:Some auth error",
        ] {
            assert_eq!(
                e.classify(&kraken(code), &RetryPolicy::idempotent()),
                RetryDecision::NonTransient,
                "{code} must be non-transient"
            );
        }
    }

    #[test]
    fn unmapped_code_is_non_transient() {
        let e = engine();
        assert_eq!(
            e.classify(&kraken("ESomething:Brand new"), &RetryPolicy::idempotent()),
            RetryDecision::NonTransient
        );
    }

    #[test]
    fn empty_kraken_array_is_non_transient() {
        let e = engine();
        assert_eq!(
            e.classify(&RestError::Kraken(vec![]), &RetryPolicy::idempotent()),
            RetryDecision::NonTransient
        );
    }

    #[test]
    fn never_retry_policy_short_circuits_even_on_transient() {
        let e = engine();
        assert_eq!(
            e.classify(
                &kraken("EAPI:Rate limit exceeded"),
                &RetryPolicy::never_retry()
            ),
            RetryDecision::NonTransient
        );
        assert_eq!(
            e.classify(
                &RestError::Transport {
                    kind: TransportErrorKind::TcpConnectTimeout,
                    transient: true
                },
                &RetryPolicy::never_retry()
            ),
            RetryDecision::NonTransient
        );
    }

    #[test]
    fn local_rate_limit_rejection_is_non_transient() {
        let e = engine();
        let err = RestError::RateLimit(crate::rate_limit::RateLimitExceeded {
            tracker: "api",
            scope: crate::rate_limit::Scope::ApiKey(crate::types::ApiKey::new("k")),
            current: 15.0,
            cap: 15.0,
            retry_at_monotonic: crate::types::MonotonicInstant::now(),
        });
        assert_eq!(
            e.classify(&err, &RetryPolicy::idempotent()),
            RetryDecision::NonTransient
        );
    }

    #[test]
    fn next_backoff_full_jitter_grows_then_caps() {
        let e = RetryEngine::from_knobs(&Knobs::defaults(), Arc::new(FixedJitter(1.0)));
        assert_eq!(e.next_backoff(0), Duration::from_millis(500));
        assert_eq!(e.next_backoff(1), Duration::from_millis(1_000));
        assert_eq!(e.next_backoff(2), Duration::from_millis(2_000));
        assert_eq!(e.next_backoff(20), Duration::from_millis(30_000));
        assert_eq!(e.next_backoff(u32::MAX), Duration::from_millis(30_000));
    }

    #[test]
    fn next_backoff_zero_jitter_is_zero() {
        let e = RetryEngine::from_knobs(&Knobs::defaults(), Arc::new(FixedJitter(0.0)));
        assert_eq!(e.next_backoff(3), Duration::ZERO);
    }

    #[test]
    fn next_backoff_floors_sub_unit_negative_and_non_finite_factor() {
        // Floor rest_retry_factor to >= 1.0 so bad knobs don't storm at 0ms.
        for bad in [-2.0, 0.0, 0.5] {
            let mut k = Knobs::defaults();
            k.rest_retry_factor = bad;
            let e = RetryEngine::from_knobs(&k, Arc::new(FixedJitter(1.0)));
            for attempt in [0, 1, 2, 5] {
                assert_eq!(
                    e.next_backoff(attempt),
                    Duration::from_millis(500),
                    "factor {bad} must floor to 1.0 (constant 500ms), not storm at 0ms; attempt {attempt}"
                );
            }
        }
        let mut k = Knobs::defaults();
        k.rest_retry_factor = f64::NAN;
        let e = RetryEngine::from_knobs(&k, Arc::new(FixedJitter(1.0)));
        assert_eq!(e.next_backoff(1), Duration::from_millis(1_000));
    }

    #[test]
    fn idempotent_defers_budget_to_engine_default() {
        let e = engine();
        assert_eq!(RetryPolicy::idempotent().max_attempts, None);
        assert_eq!(e.default_max_attempts(), 3);
        assert_eq!(RetryPolicy::never_retry().max_attempts, Some(1));
    }

    /// Display: PascalCase cross-binding discriminant.
    #[test]
    fn retry_reason_strum_display_matches_wire_strings() {
        for (reason, wire) in [
            (RetryReason::TransportError, "TransportError"),
            (RetryReason::HttpServerError, "HttpServerError"),
            (RetryReason::RateLimitExceeded, "RateLimitExceeded"),
            (RetryReason::ServiceThrottled, "ServiceThrottled"),
            (RetryReason::ServiceUnavailable, "ServiceUnavailable"),
            (RetryReason::InvalidNonce, "InvalidNonce"),
        ] {
            assert_eq!(reason.to_string(), wire);
        }
    }
}
