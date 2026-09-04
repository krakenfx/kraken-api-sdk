//! `Knobs` — central tunable-value holder (`Arc<Knobs>`). Construction-only
//! fields freeze at `.build()`; runtime-mutable fields are atomics.
//! See docs/guides/configuration.md.

use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use strum::IntoEnumIterator;

/// Lock-free `f64` cell over [`AtomicU64`]. `load` is Relaxed; Acquire/Release
/// pair carries the `ConfigChangedEvent` happens-before edge.
#[derive(Debug)]
pub struct AtomicPct(AtomicU64);

impl AtomicPct {
    fn new(v: f64) -> Self {
        Self(AtomicU64::new(v.to_bits()))
    }

    /// Current value (`Relaxed`).
    pub fn load(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }

    /// Current value with `Acquire` ordering (pairs with [`Self::swap_release`]).
    pub fn load_acquire(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Acquire))
    }

    /// Store `v` with `Release` ordering, returning the previous value.
    pub fn swap_release(&self, v: f64) -> f64 {
        f64::from_bits(self.0.swap(v.to_bits(), Ordering::Release))
    }
}

/// Lock-free `Option<u32>` over [`AtomicU64`]: present-flag in bit 32
/// (`None => 0`, `Some(n) => (1 << 32) | n`).
#[derive(Debug)]
pub struct AtomicOptU32(AtomicU64);

impl AtomicOptU32 {
    /// Present-flag bit (bit 32).
    const PRESENT: u64 = 1 << 32;
    /// Low 32 bits hold the `u32` payload when present.
    const VALUE_MASK: u64 = 0xFFFF_FFFF;

    fn new(v: Option<u32>) -> Self {
        Self(AtomicU64::new(Self::encode(v)))
    }

    fn encode(v: Option<u32>) -> u64 {
        match v {
            None => 0,
            Some(n) => Self::PRESENT | u64::from(n),
        }
    }

    fn decode(bits: u64) -> Option<u32> {
        if bits & Self::PRESENT != 0 {
            Some((bits & Self::VALUE_MASK) as u32)
        } else {
            None
        }
    }

    /// Current value (`Relaxed`).
    pub fn load(&self) -> Option<u32> {
        Self::decode(self.0.load(Ordering::Relaxed))
    }

    /// Current value with `Acquire` ordering (pairs with [`Self::swap_release`]).
    pub fn load_acquire(&self) -> Option<u32> {
        Self::decode(self.0.load(Ordering::Acquire))
    }

    /// Store `v` (`Relaxed`), returning the previous value.
    #[allow(dead_code)]
    pub fn swap(&self, v: Option<u32>) -> Option<u32> {
        Self::decode(self.0.swap(Self::encode(v), Ordering::Relaxed))
    }

    /// Store `v` with `Release` ordering, returning the previous value.
    pub fn swap_release(&self, v: Option<u32>) -> Option<u32> {
        Self::decode(self.0.swap(Self::encode(v), Ordering::Release))
    }
}

/// Tagged value for `with_knob` / `set_knob` and `ConfigChangedEvent` payloads.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum KnobValue {
    /// Unsigned integer knob.
    U32(u32),
    /// Optional `u32` knob; `None` means unbounded.
    OptU32(Option<u32>),
    /// Capacity-cap knob (entries).
    Usize(usize),
    /// Fractional knob (factors, jitter, warning-pct thresholds).
    F64(f64),
    /// On/off flag knob.
    Bool(bool),
    /// Free-form string knob (endpoint URLs).
    Str(String),
}

/// Configuration knob name (snake_case in env / TOML / `set_knob`).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    strum::Display,
    strum::AsRefStr,
    strum::IntoStaticStr,
    strum::EnumString,
    strum::EnumIter,
)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum KnobName {
    BackoffBaseMs,
    BackoffMaxMs,
    BackoffJitter,
    BackoffFactor,
    ReconnectAttempts,
    WsOrderResponseDeadlineMs,
    StalenessWindowMs,
    SubscribeAckAttempts,
    SubscribeAckTimeoutMs,
    MaxAuthHandshakeFailures,
    CloseTimeoutMs,
    UpgradeTimeoutMs,
    ConnectionRateBudget,
    ConnectionRateWindowSecs,
    PreferRestForOrders,
    SlowCallbackThresholdMs,
    RequestTimeoutMs,
    RestRetryMaxAttempts,
    RestRetryBaseMs,
    RestRetryMaxMs,
    RestRetryFactor,
    NonceRecovery,
    RateLimitApiWarningPct,
    RateLimitTradingWarningPct,
    RateLimitTradingScopeCap,
    ClOrdIdIndexCap,
    CallerToIoCapacity,
    IoToDispatchCapacity,
    RestBaseUrl,
    WsPublicUrl,
    WsAuthUrl,
}

impl KnobName {
    /// Snake_case name.
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    /// All knobs in declaration order.
    pub fn all() -> impl Iterator<Item = Self> {
        Self::iter()
    }

    /// Whether `Client::set_knob` accepts this knob.
    pub const fn is_runtime_mutable(self) -> bool {
        matches!(
            self,
            Self::ReconnectAttempts
                | Self::SubscribeAckAttempts
                | Self::SubscribeAckTimeoutMs
                | Self::MaxAuthHandshakeFailures
                | Self::SlowCallbackThresholdMs
                | Self::RateLimitApiWarningPct
                | Self::RateLimitTradingWarningPct
        )
    }

    /// Whether the knob is fixed after `.build()`.
    pub const fn is_construction_only(self) -> bool {
        !self.is_runtime_mutable()
    }

    /// Whether the knob is a URL string.
    pub const fn is_string_knob(self) -> bool {
        matches!(
            self,
            Self::RestBaseUrl | Self::WsPublicUrl | Self::WsAuthUrl
        )
    }
}

/// Central tunable-value holder (`Arc<Knobs>`). Runtime fields are atomics;
/// construction-only fields are plain values frozen at build.
#[derive(Debug)]
pub struct Knobs {
    /// CONSTRUCTION-ONLY. Reconnect backoff base.
    pub backoff_base_ms: u32,
    /// CONSTRUCTION-ONLY. Reconnect backoff ceiling.
    pub backoff_max_ms: u32,
    /// CONSTRUCTION-ONLY. Full-jitter fraction (0.0–1.0); 1.0 = full, 0.0 = none.
    pub backoff_jitter: f64,
    /// CONSTRUCTION-ONLY. Backoff growth factor.
    pub backoff_factor: f64,
    /// RUNTIME-MUTABLE. Reconnect attempt budget; `None` means unbounded.
    pub reconnect_attempts: AtomicOptU32,
    /// CONSTRUCTION-ONLY. WS staleness window (hard cap 55_000 ms).
    pub staleness_window_ms: u32,
    /// RUNTIME-MUTABLE. Per-entry subscribe-ack retry budget.
    pub subscribe_ack_attempts: AtomicU32,
    /// RUNTIME-MUTABLE. Per-entry subscribe-ack timer duration.
    pub subscribe_ack_timeout_ms: AtomicU32,
    /// RUNTIME-MUTABLE. Max retries for the token-stale auth-handshake loop.
    pub max_auth_handshake_failures: AtomicU32,
    /// CONSTRUCTION-ONLY. WS close handshake timeout.
    pub close_timeout_ms: u32,
    /// CONSTRUCTION-ONLY. WS upgrade handshake timeout.
    pub upgrade_timeout_ms: u32,
    /// CONSTRUCTION-ONLY. Connect/upgrade attempts per rolling window.
    pub connection_rate_budget: u32,
    /// CONSTRUCTION-ONLY. Rolling window (seconds) for `connection_rate_budget`.
    pub connection_rate_window_secs: u32,
    /// RUNTIME-MUTABLE. Dispatch-loop slow-callback threshold.
    pub slow_callback_threshold_ms: AtomicU32,
    /// CONSTRUCTION-ONLY. Optional client-side deadline (ms) on the WS order
    /// response await; `None`/`0` = unbounded.
    pub ws_order_response_deadline_ms: Option<u32>,
    /// CONSTRUCTION-ONLY. Session-wide order-transport default (`true` = REST);
    /// per-call `PendingTrade::via` wins.
    pub prefer_rest_for_orders: bool,

    /// CONSTRUCTION-ONLY. Per-request REST timeout.
    pub request_timeout_ms: u32,
    /// CONSTRUCTION-ONLY. REST transient-retry budget (total attempts incl. first).
    pub rest_retry_max_attempts: u32,
    /// CONSTRUCTION-ONLY. REST retry exponential-backoff base.
    pub rest_retry_base_ms: u32,
    /// CONSTRUCTION-ONLY. REST retry backoff ceiling.
    pub rest_retry_max_ms: u32,
    /// CONSTRUCTION-ONLY. REST retry backoff growth factor.
    pub rest_retry_factor: f64,
    /// CONSTRUCTION-ONLY. Opt-in nonce-poisoning detection (default false).
    pub nonce_recovery: bool,

    /// RUNTIME-MUTABLE. API rate-limit warning threshold.
    pub rate_limit_api_warning_pct: AtomicPct,
    /// RUNTIME-MUTABLE. Trading rate-limit warning threshold.
    pub rate_limit_trading_warning_pct: AtomicPct,
    /// CONSTRUCTION-ONLY. LRU cap on the trading tracker's per-pair scope map.
    pub rate_limit_trading_scope_cap: usize,
    /// CONSTRUCTION-ONLY. LRU cap on the `ClOrdIdPairIndex`.
    pub cl_ord_id_index_cap: usize,
    /// CONSTRUCTION-ONLY. Caller→I/O mpsc capacity.
    pub caller_to_io_capacity: u32,
    /// CONSTRUCTION-ONLY. I/O→dispatch ring capacity.
    pub io_to_dispatch_capacity: u32,

    /// CONSTRUCTION-ONLY. REST base URL (default `https://api.kraken.com`).
    pub rest_base_url: String,
    /// CONSTRUCTION-ONLY. Public WS endpoint (default `wss://ws.kraken.com/v2`).
    pub ws_public_url: String,
    /// CONSTRUCTION-ONLY. Auth WS endpoint (default `wss://ws-auth.kraken.com/v2`).
    pub ws_auth_url: String,
}

impl Knobs {
    /// All-defaults `Knobs`.
    pub fn defaults() -> Self {
        Self {
            backoff_base_ms: 500,
            backoff_max_ms: 30_000,
            backoff_jitter: 1.0,
            backoff_factor: 2.0,
            reconnect_attempts: AtomicOptU32::new(None),
            staleness_window_ms: 30_000,
            subscribe_ack_attempts: AtomicU32::new(3),
            subscribe_ack_timeout_ms: AtomicU32::new(5_000),
            max_auth_handshake_failures: AtomicU32::new(3),
            close_timeout_ms: 2_000,
            upgrade_timeout_ms: 10_000,
            connection_rate_budget: 120,
            connection_rate_window_secs: 600,
            slow_callback_threshold_ms: AtomicU32::new(50),
            ws_order_response_deadline_ms: None,
            prefer_rest_for_orders: false,
            request_timeout_ms: 30_000,
            rest_retry_max_attempts: 3,
            rest_retry_base_ms: 500,
            rest_retry_max_ms: 30_000,
            rest_retry_factor: 2.0,
            nonce_recovery: false,
            rate_limit_api_warning_pct: AtomicPct::new(0.80),
            rate_limit_trading_warning_pct: AtomicPct::new(0.80),
            rate_limit_trading_scope_cap: 256,
            cl_ord_id_index_cap: 1024,
            caller_to_io_capacity: 2048,
            io_to_dispatch_capacity: 8192,
            rest_base_url: "https://api.kraken.com".to_string(),
            ws_public_url: crate::types::WsUrl::Public.as_wire_url().to_string(),
            ws_auth_url: crate::types::WsUrl::Auth.as_wire_url().to_string(),
        }
    }

    /// `staleness_window_ms` as a [`Duration`].
    #[allow(dead_code)]
    pub fn staleness_window(&self) -> Duration {
        Duration::from_millis(u64::from(self.staleness_window_ms))
    }

    /// `request_timeout_ms` as a [`Duration`].
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.request_timeout_ms))
    }

    /// `close_timeout_ms` as a [`Duration`].
    pub fn close_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.close_timeout_ms))
    }

    /// `upgrade_timeout_ms` as a [`Duration`].
    pub fn upgrade_timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.upgrade_timeout_ms))
    }

    /// `connection_rate_window_secs` as a [`Duration`].
    pub fn connection_rate_window(&self) -> Duration {
        Duration::from_secs(u64::from(self.connection_rate_window_secs))
    }

    /// Returns `true` if `name` is a construction-only (immutable post-build) knob.
    pub fn is_construction_only(name: &str) -> bool {
        KnobName::from_str(name)
            .map(KnobName::is_construction_only)
            .unwrap_or(false)
    }

    /// Returns `true` if `name` is a runtime-mutable knob.
    pub fn is_runtime_mutable(name: &str) -> bool {
        KnobName::from_str(name)
            .map(KnobName::is_runtime_mutable)
            .unwrap_or(false)
    }

    /// Atomic point-in-time snapshot of knob `name` (`None` if unknown).
    pub fn snapshot(&self, name: &str) -> Option<KnobValue> {
        let knob = KnobName::from_str(name).ok()?;
        Some(self.snapshot_knob(knob))
    }

    /// Snapshot of `knob`.
    pub fn snapshot_knob(&self, knob: KnobName) -> KnobValue {
        match knob {
            KnobName::BackoffBaseMs => KnobValue::U32(self.backoff_base_ms),
            KnobName::BackoffMaxMs => KnobValue::U32(self.backoff_max_ms),
            KnobName::BackoffJitter => KnobValue::F64(self.backoff_jitter),
            KnobName::BackoffFactor => KnobValue::F64(self.backoff_factor),
            KnobName::StalenessWindowMs => KnobValue::U32(self.staleness_window_ms),
            KnobName::CloseTimeoutMs => KnobValue::U32(self.close_timeout_ms),
            KnobName::UpgradeTimeoutMs => KnobValue::U32(self.upgrade_timeout_ms),
            KnobName::ConnectionRateBudget => KnobValue::U32(self.connection_rate_budget),
            KnobName::ConnectionRateWindowSecs => KnobValue::U32(self.connection_rate_window_secs),
            KnobName::ReconnectAttempts => {
                KnobValue::OptU32(self.reconnect_attempts.load_acquire())
            }
            KnobName::SubscribeAckAttempts => {
                KnobValue::U32(self.subscribe_ack_attempts.load(Ordering::Acquire))
            }
            KnobName::SubscribeAckTimeoutMs => {
                KnobValue::U32(self.subscribe_ack_timeout_ms.load(Ordering::Acquire))
            }
            KnobName::MaxAuthHandshakeFailures => {
                KnobValue::U32(self.max_auth_handshake_failures.load(Ordering::Acquire))
            }
            KnobName::SlowCallbackThresholdMs => {
                KnobValue::U32(self.slow_callback_threshold_ms.load(Ordering::Acquire))
            }
            KnobName::WsOrderResponseDeadlineMs => {
                KnobValue::OptU32(self.ws_order_response_deadline_ms)
            }
            KnobName::PreferRestForOrders => KnobValue::Bool(self.prefer_rest_for_orders),
            KnobName::RequestTimeoutMs => KnobValue::U32(self.request_timeout_ms),
            KnobName::RestRetryMaxAttempts => KnobValue::U32(self.rest_retry_max_attempts),
            KnobName::RestRetryBaseMs => KnobValue::U32(self.rest_retry_base_ms),
            KnobName::RestRetryMaxMs => KnobValue::U32(self.rest_retry_max_ms),
            KnobName::RestRetryFactor => KnobValue::F64(self.rest_retry_factor),
            KnobName::NonceRecovery => KnobValue::Bool(self.nonce_recovery),
            KnobName::RateLimitApiWarningPct => {
                KnobValue::F64(self.rate_limit_api_warning_pct.load_acquire())
            }
            KnobName::RateLimitTradingWarningPct => {
                KnobValue::F64(self.rate_limit_trading_warning_pct.load_acquire())
            }
            KnobName::RateLimitTradingScopeCap => {
                KnobValue::Usize(self.rate_limit_trading_scope_cap)
            }
            KnobName::ClOrdIdIndexCap => KnobValue::Usize(self.cl_ord_id_index_cap),
            KnobName::CallerToIoCapacity => KnobValue::U32(self.caller_to_io_capacity),
            KnobName::IoToDispatchCapacity => KnobValue::U32(self.io_to_dispatch_capacity),
            KnobName::RestBaseUrl => KnobValue::Str(self.rest_base_url.clone()),
            KnobName::WsPublicUrl => KnobValue::Str(self.ws_public_url.clone()),
            KnobName::WsAuthUrl => KnobValue::Str(self.ws_auth_url.clone()),
        }
    }

    /// Apply a runtime-mutable knob change (`Some(previous)` on success).
    /// Warning-pct knobs reject non-finite / out-of-range values.
    pub fn set_runtime(&self, name: &str, value: &KnobValue) -> Option<KnobValue> {
        let knob = KnobName::from_str(name).ok()?;
        self.set_runtime_knob(knob, value)
    }

    /// Like [`Self::set_runtime`], with a typed name.
    pub fn set_runtime_knob(&self, knob: KnobName, value: &KnobValue) -> Option<KnobValue> {
        /// Validate a warning-pct in `[0.0, 1.0]`; `None` rejects.
        fn validate_pct(v: f64) -> Option<f64> {
            if v.is_finite() && (0.0..=1.0).contains(&v) {
                Some(v)
            } else {
                None
            }
        }

        Some(match (knob, value) {
            (KnobName::ReconnectAttempts, KnobValue::OptU32(v)) => {
                KnobValue::OptU32(self.reconnect_attempts.swap_release(*v))
            }
            (KnobName::SubscribeAckAttempts, KnobValue::U32(v)) => {
                KnobValue::U32(self.subscribe_ack_attempts.swap(*v, Ordering::Release))
            }
            (KnobName::SubscribeAckTimeoutMs, KnobValue::U32(v)) => {
                KnobValue::U32(self.subscribe_ack_timeout_ms.swap(*v, Ordering::Release))
            }
            (KnobName::MaxAuthHandshakeFailures, KnobValue::U32(v)) => {
                KnobValue::U32(self.max_auth_handshake_failures.swap(*v, Ordering::Release))
            }
            (KnobName::SlowCallbackThresholdMs, KnobValue::U32(v)) => {
                KnobValue::U32(self.slow_callback_threshold_ms.swap(*v, Ordering::Release))
            }
            (KnobName::RateLimitApiWarningPct, KnobValue::F64(v)) => {
                let validated = validate_pct(*v)?;
                KnobValue::F64(self.rate_limit_api_warning_pct.swap_release(validated))
            }
            (KnobName::RateLimitTradingWarningPct, KnobValue::F64(v)) => {
                let validated = validate_pct(*v)?;
                KnobValue::F64(self.rate_limit_trading_warning_pct.swap_release(validated))
            }
            _ => return None,
        })
    }
}

impl Default for Knobs {
    fn default() -> Self {
        Self::defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_pct_roundtrip() {
        let p = AtomicPct::new(0.80);
        assert!((p.load() - 0.80).abs() < f64::EPSILON);
        let prev = p.swap_release(0.5);
        assert!((prev - 0.80).abs() < f64::EPSILON);
        assert!((p.load() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn atomic_opt_u32_none_sentinel() {
        let a = AtomicOptU32::new(None);
        assert_eq!(a.load(), None);
        let prev = a.swap(Some(7));
        assert_eq!(prev, None);
        assert_eq!(a.load(), Some(7));
        let prev = a.swap(None);
        assert_eq!(prev, Some(7));
        assert_eq!(a.load(), None);
    }

    #[test]
    fn atomic_opt_u32_some_u32_max_roundtrips() {
        let a = AtomicOptU32::new(Some(u32::MAX));
        assert_eq!(a.load(), Some(u32::MAX));
        assert_eq!(
            AtomicOptU32::decode(AtomicOptU32::encode(Some(u32::MAX))),
            Some(u32::MAX)
        );
        assert_eq!(AtomicOptU32::decode(AtomicOptU32::encode(Some(0))), Some(0));
        assert_eq!(AtomicOptU32::decode(AtomicOptU32::encode(None)), None);
        let prev = a.swap(Some(1));
        assert_eq!(prev, Some(u32::MAX));
        assert_eq!(a.load(), Some(1));
        let prev = a.swap_release(Some(u32::MAX));
        assert_eq!(prev, Some(1));
        assert_eq!(a.load_acquire(), Some(u32::MAX));

        let k = Knobs::defaults();
        let prev = k
            .set_runtime("reconnect_attempts", &KnobValue::OptU32(Some(u32::MAX)))
            .unwrap();
        assert_eq!(prev, KnobValue::OptU32(None));
        assert_eq!(
            k.snapshot("reconnect_attempts"),
            Some(KnobValue::OptU32(Some(u32::MAX)))
        );
    }

    #[test]
    fn defaults_match_documented_values() {
        let k = Knobs::defaults();
        assert_eq!(k.backoff_base_ms, 500);
        assert_eq!(k.backoff_max_ms, 30_000);
        assert!((k.backoff_jitter - 1.0).abs() < f64::EPSILON);
        assert!((k.backoff_factor - 2.0).abs() < f64::EPSILON);
        assert_eq!(k.reconnect_attempts.load(), None);
        assert_eq!(k.staleness_window_ms, 30_000);
        assert_eq!(k.subscribe_ack_attempts.load(Ordering::Relaxed), 3);
        assert_eq!(k.subscribe_ack_timeout_ms.load(Ordering::Relaxed), 5_000);
        assert_eq!(k.max_auth_handshake_failures.load(Ordering::Relaxed), 3);
        assert_eq!(k.close_timeout_ms, 2_000);
        assert_eq!(k.upgrade_timeout_ms, 10_000);
        assert_eq!(k.connection_rate_budget, 120);
        assert_eq!(k.connection_rate_window_secs, 600);
        assert_eq!(k.slow_callback_threshold_ms.load(Ordering::Relaxed), 50);
        assert!(!k.prefer_rest_for_orders);

        assert_eq!(k.request_timeout_ms, 30_000);
        assert_eq!(k.rest_retry_max_attempts, 3);
        assert_eq!(k.rest_retry_base_ms, 500);
        assert_eq!(k.rest_retry_max_ms, 30_000);
        assert!((k.rest_retry_factor - 2.0).abs() < f64::EPSILON);
        assert!((k.rate_limit_api_warning_pct.load() - 0.80).abs() < f64::EPSILON);
        assert!((k.rate_limit_trading_warning_pct.load() - 0.80).abs() < f64::EPSILON);
        assert_eq!(k.rate_limit_trading_scope_cap, 256);
        assert_eq!(k.cl_ord_id_index_cap, 1024);
        assert_eq!(k.caller_to_io_capacity, 2048);
        assert_eq!(k.io_to_dispatch_capacity, 8192);
    }

    #[test]
    fn snapshot_returns_none_for_unknown() {
        let k = Knobs::defaults();
        assert_eq!(k.snapshot("nope"), None);
        assert_eq!(
            k.snapshot("request_timeout_ms"),
            Some(KnobValue::U32(30_000))
        );
    }

    #[test]
    fn bucket_classification_is_disjoint_and_total() {
        let mut n = 0;
        for knob in KnobName::all() {
            n += 1;
            assert!(
                knob.is_construction_only() ^ knob.is_runtime_mutable(),
                "{knob} must be in exactly one bucket"
            );
            assert!(
                KnobName::from_str(knob.as_str()).is_ok(),
                "{knob} must parse as known"
            );
            assert_eq!(
                Knobs::is_construction_only(knob.as_str()),
                knob.is_construction_only()
            );
            assert_eq!(
                Knobs::is_runtime_mutable(knob.as_str()),
                knob.is_runtime_mutable()
            );
            let _ = Knobs::defaults().snapshot_knob(knob);
        }
        assert_eq!(n, 31);
        assert!(KnobName::from_str("nope").is_err());
    }

    #[test]
    fn knob_name_roundtrips_snake_case() {
        for knob in KnobName::all() {
            let s = knob.as_str();
            assert_eq!(KnobName::from_str(s).unwrap(), knob);
            assert_eq!(knob.to_string(), s);
        }
    }

    #[test]
    fn set_runtime_rejects_construction_only_and_unknown() {
        let k = Knobs::defaults();
        assert!(
            k.set_runtime("request_timeout_ms", &KnobValue::U32(1))
                .is_none()
        );
        assert!(k.set_runtime("nope", &KnobValue::U32(1)).is_none());
        assert!(
            k.set_runtime("reconnect_attempts", &KnobValue::U32(1))
                .is_none()
        );
    }

    #[test]
    fn set_runtime_swaps_and_returns_previous() {
        let k = Knobs::defaults();
        let prev = k
            .set_runtime("reconnect_attempts", &KnobValue::OptU32(Some(5)))
            .unwrap();
        assert_eq!(prev, KnobValue::OptU32(None));
        assert_eq!(k.reconnect_attempts.load(), Some(5));
    }

    #[test]
    fn warning_pct_nan_rejected() {
        let k = Knobs::defaults();
        assert!(
            k.set_runtime("rate_limit_api_warning_pct", &KnobValue::F64(f64::NAN))
                .is_none(),
            "NaN must be rejected for rate_limit_api_warning_pct"
        );
        assert!(
            k.set_runtime("rate_limit_trading_warning_pct", &KnobValue::F64(f64::NAN))
                .is_none(),
            "NaN must be rejected for rate_limit_trading_warning_pct"
        );
        assert!((k.rate_limit_api_warning_pct.load() - 0.80).abs() < f64::EPSILON);
        assert!((k.rate_limit_trading_warning_pct.load() - 0.80).abs() < f64::EPSILON);
    }

    #[test]
    fn warning_pct_inf_rejected() {
        let k = Knobs::defaults();
        assert!(
            k.set_runtime("rate_limit_api_warning_pct", &KnobValue::F64(f64::INFINITY))
                .is_none(),
            "+inf must be rejected for rate_limit_api_warning_pct"
        );
        assert!(
            k.set_runtime(
                "rate_limit_trading_warning_pct",
                &KnobValue::F64(f64::NEG_INFINITY)
            )
            .is_none(),
            "-inf must be rejected for rate_limit_trading_warning_pct"
        );
        assert!((k.rate_limit_api_warning_pct.load() - 0.80).abs() < f64::EPSILON);
        assert!((k.rate_limit_trading_warning_pct.load() - 0.80).abs() < f64::EPSILON);
    }

    #[test]
    fn warning_pct_above_one_rejected() {
        let k = Knobs::defaults();
        assert!(
            k.set_runtime("rate_limit_api_warning_pct", &KnobValue::F64(1.5))
                .is_none(),
            "1.5 must be rejected for rate_limit_api_warning_pct"
        );
        assert!(
            (k.rate_limit_api_warning_pct.load() - 0.80).abs() < f64::EPSILON,
            "stored value must be unchanged after rejection"
        );

        let k2 = Knobs::defaults();
        assert!(
            k2.set_runtime("rate_limit_trading_warning_pct", &KnobValue::F64(2.0))
                .is_none(),
            "2.0 must be rejected for rate_limit_trading_warning_pct"
        );
        assert!((k2.rate_limit_trading_warning_pct.load() - 0.80).abs() < f64::EPSILON);
    }

    #[test]
    fn warning_pct_below_zero_rejected() {
        let k = Knobs::defaults();
        assert!(
            k.set_runtime("rate_limit_api_warning_pct", &KnobValue::F64(-0.2))
                .is_none(),
            "-0.2 must be rejected for rate_limit_api_warning_pct"
        );
        assert!(
            (k.rate_limit_api_warning_pct.load() - 0.80).abs() < f64::EPSILON,
            "stored value must be unchanged after rejection"
        );

        let k2 = Knobs::defaults();
        assert!(
            k2.set_runtime("rate_limit_trading_warning_pct", &KnobValue::F64(-1.0))
                .is_none(),
            "-1.0 must be rejected for rate_limit_trading_warning_pct"
        );
        assert!((k2.rate_limit_trading_warning_pct.load() - 0.80).abs() < f64::EPSILON);
    }

    #[test]
    fn warning_pct_valid_unchanged() {
        let k = Knobs::defaults();
        let prev = k
            .set_runtime("rate_limit_api_warning_pct", &KnobValue::F64(0.8))
            .unwrap();
        assert_eq!(prev, KnobValue::F64(0.80));
        assert!((k.rate_limit_api_warning_pct.load() - 0.8).abs() < f64::EPSILON);

        let k2 = Knobs::defaults();
        assert!(
            k2.set_runtime("rate_limit_trading_warning_pct", &KnobValue::F64(0.0))
                .is_some()
        );
        assert!(k2.rate_limit_trading_warning_pct.load().abs() < f64::EPSILON);

        let k3 = Knobs::defaults();
        assert!(
            k3.set_runtime("rate_limit_api_warning_pct", &KnobValue::F64(1.0))
                .is_some()
        );
        assert!((k3.rate_limit_api_warning_pct.load() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn resolve_rejects_nan_warning_pct_via_builder() {
        use crate::build::config_resolver::ConfigResolver;
        let mut r = ConfigResolver::new();
        r.with_builder_knob("rate_limit_api_warning_pct", KnobValue::F64(f64::NAN));
        let err = r.resolve().unwrap_err();
        assert!(
            matches!(err, crate::build::ConfigError::InvalidConfig { .. }),
            "NaN via builder must produce ConfigError::InvalidConfig at resolve()"
        );
    }

    #[test]
    fn resolve_rejects_out_of_range_warning_pct_via_builder() {
        use crate::build::config_resolver::ConfigResolver;

        let mut r = ConfigResolver::new();
        r.with_builder_knob("rate_limit_trading_warning_pct", KnobValue::F64(1.5));
        let err = r.resolve().unwrap_err();
        assert!(
            matches!(err, crate::build::ConfigError::InvalidConfig { .. }),
            "1.5 via builder must produce ConfigError::InvalidConfig at resolve()"
        );

        let mut r2 = ConfigResolver::new();
        r2.with_builder_knob("rate_limit_api_warning_pct", KnobValue::F64(-0.2));
        let err2 = r2.resolve().unwrap_err();
        assert!(
            matches!(err2, crate::build::ConfigError::InvalidConfig { .. }),
            "-0.2 via builder must produce ConfigError::InvalidConfig at resolve()"
        );
    }

    #[test]
    fn resolve_rejects_non_finite_warning_pct_via_env() {
        use crate::build::config_resolver::ConfigResolver;
        use crate::build::test_env::{EnvVarGuard, env_lock};
        // Shared crate-wide lock — serializes against the config_resolver env
        // tests, which also read this process env via resolve().
        let _g = env_lock();

        // Rust's `str::parse::<f64>()` accepts "NaN", "inf", "-inf" — ensure
        // the env path rejects them through validate_pct.
        for raw in &["NaN", "nan", "inf", "-inf"] {
            // RAII-restored each iteration so a panicking assertion can't leak.
            let _e = EnvVarGuard::set("KRAKEN_RATE_LIMIT_API_WARNING_PCT", raw);
            let err = ConfigResolver::new().resolve().unwrap_err();
            assert!(
                matches!(err, crate::build::ConfigError::InvalidConfig { .. }),
                "`{raw}` via env must produce ConfigError::InvalidConfig at resolve()"
            );
        }
    }

    #[test]
    fn set_runtime_current_equals_stored_value() {
        let k = Knobs::defaults();
        let new_val = 0.65_f64;
        let _prev = k
            .set_runtime("rate_limit_api_warning_pct", &KnobValue::F64(new_val))
            .expect("in-range value must be accepted");
        let stored = k.rate_limit_api_warning_pct.load();
        assert!(
            (stored - new_val).abs() < f64::EPSILON,
            "stored value ({stored}) must equal the requested value ({new_val}); \
             ConfigChangedEvent.current must match what is stored"
        );

        let new_val2 = 0.55_f64;
        let _prev2 = k
            .set_runtime("rate_limit_trading_warning_pct", &KnobValue::F64(new_val2))
            .expect("in-range value must be accepted");
        let stored2 = k.rate_limit_trading_warning_pct.load();
        assert!(
            (stored2 - new_val2).abs() < f64::EPSILON,
            "stored value ({stored2}) must equal the requested value ({new_val2})"
        );
    }
}
