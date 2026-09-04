//! Trading REST + Spot WS v2 rate accounting keyed by `(ApiKey, Symbol)`; both
//! transports MUST share one `Arc`. The scope map is LRU-capped (eviction event).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::build::knobs::Knobs;
use crate::clock::Clock;
use crate::dispatch::{DispatchEventBus, EventEnvelope, EventPayload, EventType};
use crate::rate_limit::counter::{ChargeOutcome, RateLimitCounter};
use crate::rate_limit::{RateLimitExceeded, Scope, Tier};
use crate::types::{ApiKey, MonotonicInstant, Symbol};

/// Per-`(ApiKey, Symbol)` trading counter, shared across REST and WS.
pub struct SpotTradingRateLimitTracker {
    tier: Tier,
    inner: Mutex<TradingInner>,
    bus: Arc<DispatchEventBus>,
    clock: Arc<dyn Clock>,
    /// `rate_limit_trading_warning_pct` is read fresh on each `consume` (runtime-mutable).
    knobs: Arc<Knobs>,
    /// LRU cap on the scope map; read once at construction and frozen.
    lru_cap: usize,
}

/// One `Mutex` so eviction + counter mutation are atomic. `access_order`:
/// most recently charged at the back, oldest at the front.
struct TradingInner {
    counter: HashMap<(ApiKey, Symbol), RateLimitCounter>,
    access_order: std::collections::VecDeque<(ApiKey, Symbol)>,
}

impl SpotTradingRateLimitTracker {
    /// Pure construction; LRU cap read from the `rate_limit_trading_scope_cap` knob.
    pub fn new(
        tier: Tier,
        bus: Arc<DispatchEventBus>,
        clock: Arc<dyn Clock>,
        knobs: Arc<Knobs>,
    ) -> Self {
        let lru_cap = knobs.rate_limit_trading_scope_cap;
        Self::with_lru_cap(tier, bus, clock, knobs, lru_cap)
    }

    /// As [`Self::new`] with an explicit tracked-scope LRU capacity.
    pub fn with_lru_cap(
        tier: Tier,
        bus: Arc<DispatchEventBus>,
        clock: Arc<dyn Clock>,
        knobs: Arc<Knobs>,
        lru_cap: usize,
    ) -> Self {
        // Clamp to >= 1: cap=0 violates the len<=cap post-condition on first insert.
        let lru_cap = lru_cap.max(1);
        debug_assert!(lru_cap >= 1, "trading LRU cap must be >= 1");
        Self {
            tier,
            inner: Mutex::new(TradingInner {
                counter: HashMap::with_capacity(lru_cap.min(64)),
                access_order: std::collections::VecDeque::with_capacity(lru_cap.min(64)),
            }),
            bus,
            clock,
            knobs,
            lru_cap,
        }
    }

    /// Atomic check-and-charge. See `api.rs::consume` for the full contract;
    /// same shape here keyed by `(ApiKey, Symbol)`.
    pub fn consume(
        &self,
        scope: Scope,
        cost: f64,
        now: MonotonicInstant,
    ) -> Result<(), RateLimitExceeded> {
        let key = match &scope {
            Scope::Pair(k, p) => (k.clone(), p.clone()),
            Scope::ApiKey(_) => {
                tracing::error!(
                    target: "kraken_sdk::rate_limit",
                    ?scope,
                    "SpotTradingRateLimitTracker received Scope::ApiKey — caller bug; rejecting with mis-routed sentinel"
                );
                return Err(RateLimitExceeded {
                    tracker: "trading",
                    scope: scope.clone(),
                    current: 0.0,
                    cap: 0.0,
                    retry_at_monotonic: now,
                });
            }
        };
        let tier = self.tier;
        let cap = tier.trading_cap();
        let decay = tier.trading_decay_per_sec();
        let warning_pct = self.knobs.rate_limit_trading_warning_pct.load();

        enum Emit {
            None,
            Warning {
                used: f64,
                pct: f64,
                decay_per_sec: f64,
                seconds_to_drain: f64,
            },
        }
        let (result, emit, evicted): (_, _, Option<Scope>) = {
            let mut inner = self.inner.lock().expect("trading inner lock poisoned");
            let evicted = self.touch_lru(&mut inner, &key, cap, decay, now);
            let counter = inner
                .counter
                .get_mut(&key)
                .expect("counter present after touch_lru");
            match counter.charge(cost, now) {
                ChargeOutcome::Charged => {
                    let new_pct = counter.current / cap;
                    let emit = if counter.should_emit_warning(now, warning_pct) {
                        let used = counter.current;
                        // Non-positive/non-finite decay → INFINITY; NaN/Inf must not reach the payload.
                        let seconds_to_drain =
                            if counter.decay_per_sec.is_finite() && counter.decay_per_sec > 0.0 {
                                used / counter.decay_per_sec
                            } else {
                                f64::INFINITY
                            };
                        Emit::Warning {
                            used,
                            pct: new_pct,
                            decay_per_sec: counter.decay_per_sec,
                            seconds_to_drain,
                        }
                    } else {
                        Emit::None
                    };
                    (Ok(()), emit, evicted)
                }
                ChargeOutcome::Exceeded => {
                    let current = counter.current;
                    let cap = counter.cap;
                    let retry_at =
                        super::compute_retry_at(current, cap, counter.decay_per_sec, now);
                    (
                        Err(RateLimitExceeded {
                            tracker: "trading",
                            scope: scope.clone(),
                            current,
                            cap,
                            retry_at_monotonic: retry_at,
                        }),
                        Emit::None,
                        evicted,
                    )
                }
            }
        };

        // Emit outside the lock; eviction first (old scope dropped → new scope charged).
        if let Some(evicted_scope) = evicted {
            let evicted_at = self.clock.now();
            let (key_id_fingerprint, pair) = evicted_scope.redacted_id();
            self.bus.publish(EventEnvelope {
                event_type: EventType::RateLimitCacheEvictionEvent,
                event_version: 1,
                timestamp_monotonic: evicted_at,
                request_id: None,
                payload: EventPayload::RateLimitCacheEvictionEvent {
                    key_id_fingerprint,
                    pair,
                    evicted_at_monotonic: evicted_at,
                    reason: "lru_cap_exceeded",
                },
            });
        }
        if let Emit::Warning {
            used,
            pct,
            decay_per_sec,
            seconds_to_drain,
        } = emit
        {
            super::publish_rate_limit_warning(
                &self.bus,
                self.clock.as_ref(),
                "trading",
                &scope,
                used,
                cap,
                pct,
                decay_per_sec,
                seconds_to_drain,
            );
        }

        result
    }

    /// Insert-or-promote `key` to the back of the access order; returns the
    /// evicted scope when the LRU cap forced a drop. Hand-rolled (unlike
    /// cl_ord_id_index): evict+emit is one critical section entangled with counter state.
    fn touch_lru(
        &self,
        inner: &mut TradingInner,
        key: &(ApiKey, Symbol),
        cap: f64,
        decay: f64,
        now: MonotonicInstant,
    ) -> Option<Scope> {
        if let Some(pos) = inner.access_order.iter().position(|k| k == key) {
            inner.access_order.remove(pos);
        }
        inner.access_order.push_back(key.clone());

        let mut evicted: Option<Scope> = None;
        if !inner.counter.contains_key(key) {
            // Evict BEFORE insert so `len <= lru_cap` holds.
            if inner.counter.len() >= self.lru_cap {
                let oldest_pos = inner.access_order.iter().position(|k| k != key);
                if let Some(pos) = oldest_pos {
                    let oldest_key = inner.access_order.remove(pos).expect("position valid");
                    inner.counter.remove(&oldest_key);
                    let (api_key, pair) = oldest_key;
                    evicted = Some(Scope::Pair(api_key, pair));
                }
            }
            inner
                .counter
                .insert(key.clone(), RateLimitCounter::new(cap, decay, now));
        }
        evicted
    }

    // Advisory inspection on the as-built interface; exercised only by tests.
    #[allow(dead_code)]
    /// Units of headroom remaining for `scope` at `now`.
    /// Returns `None` for caller mis-routing (`Scope::ApiKey`).
    pub fn headroom(&self, scope: Scope, now: MonotonicInstant) -> Option<f64> {
        let key = match scope {
            Scope::Pair(k, p) => (k, p),
            Scope::ApiKey(_) => {
                tracing::error!(
                    target: "kraken_sdk::rate_limit",
                    "SpotTradingRateLimitTracker::headroom received Scope::ApiKey — caller bug"
                );
                return None;
            }
        };
        let cap = self.tier.trading_cap();
        let inner = self.inner.lock().expect("trading inner lock poisoned");
        let h = match inner.counter.get(&key) {
            Some(counter) => (cap - counter.peek_current(now)).max(0.0),
            None => cap,
        };
        Some(h)
    }

    /// Refund a prior `consume` when a pre-charged WS trading op was never
    /// posted (queue full / closed). `Scope::Pair` only; a no-op if the counter
    /// was evicted between consume and credit (absent counter is already 0).
    pub fn credit(&self, scope: Scope, cost: f64, now: MonotonicInstant) {
        let Scope::Pair(k, p) = scope else {
            return;
        };
        let mut inner = self.inner.lock().expect("trading inner lock poisoned");
        if let Some(counter) = inner.counter.get_mut(&(k, p)) {
            counter.credit(cost, now);
        }
    }

    /// Non-rejecting account-wide charge (e.g. `cancel_all`: +1 per affected
    /// pair): charges each tracked `(api_key, *)` counter, clamped at cap, never
    /// rejects. `Scope::Pair` is ignored; untracked pairs are not created.
    pub fn charge_account_wide(&self, scope: Scope, cost: f64, now: MonotonicInstant) {
        let Scope::ApiKey(api_key) = scope else {
            return;
        };
        let mut inner = self.inner.lock().expect("trading inner lock poisoned");
        for ((k, _p), counter) in inner.counter.iter_mut() {
            if *k == api_key {
                counter.charge_saturating(cost, now);
            }
        }
    }

    /// Negative reconcile on a Kraken throttle error. `Scope::ApiKey` snaps ALL
    /// `(api_key, *)` counters to cap (account-level aggregate); `Scope::Pair`
    /// snaps that one. Emits one `RateLimitExceededEvent` with the wire error.
    pub fn snap_to_cap(&self, scope: Scope, kraken_error: &str, now: MonotonicInstant) {
        let cap = self.tier.trading_cap();
        let decay = self.tier.trading_decay_per_sec();
        match &scope {
            Scope::Pair(k, p) => {
                let key = (k.clone(), p.clone());
                let mut inner = self.inner.lock().expect("trading inner lock poisoned");
                let counter = inner
                    .counter
                    .entry(key.clone())
                    .or_insert_with(|| RateLimitCounter::new(cap, decay, now));
                counter.snap_to_cap(now);
                // Promote so the entry survives LRU during the throttled-recovery window.
                if let Some(pos) = inner.access_order.iter().position(|k| k == &key) {
                    inner.access_order.remove(pos);
                }
                inner.access_order.push_back(key);
            }
            Scope::ApiKey(api_key) => {
                // Absent (api_key, pair) entries are not created — a fresh counter is already below cap.
                let mut inner = self.inner.lock().expect("trading inner lock poisoned");
                for ((k, _p), counter) in inner.counter.iter_mut() {
                    if *k == *api_key {
                        counter.snap_to_cap(now);
                    }
                }
                let keys_to_promote: Vec<(ApiKey, Symbol)> = inner
                    .counter
                    .keys()
                    .filter(|(k, _)| *k == *api_key)
                    .cloned()
                    .collect();
                for key in &keys_to_promote {
                    if let Some(pos) = inner.access_order.iter().position(|k| k == key) {
                        inner.access_order.remove(pos);
                    }
                    inner.access_order.push_back(key.clone());
                }
            }
        }
        super::publish_rate_limit_exceeded(
            &self.bus,
            self.clock.as_ref(),
            "trading",
            &scope,
            kraken_error,
            now,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::SystemClock;
    use crate::dispatch::DispatchEventBusConfig;
    use std::time::Duration;

    fn fixture() -> (Arc<SpotTradingRateLimitTracker>, Arc<DispatchEventBus>) {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let tracker = Arc::new(SpotTradingRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(&bus),
            clock,
            Arc::new(Knobs::defaults()),
        ));
        (tracker, bus)
    }

    fn fixture_with_lru_cap(
        cap: usize,
    ) -> (Arc<SpotTradingRateLimitTracker>, Arc<DispatchEventBus>) {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let tracker = Arc::new(SpotTradingRateLimitTracker::with_lru_cap(
            Tier::Pro,
            Arc::clone(&bus),
            clock,
            Arc::new(Knobs::defaults()),
            cap,
        ));
        (tracker, bus)
    }

    fn key() -> ApiKey {
        ApiKey::new("test-api-key")
    }

    fn pair(s: &str) -> Symbol {
        Symbol::new(s).unwrap()
    }

    fn now(s: u64) -> MonotonicInstant {
        MonotonicInstant(Duration::from_secs(s))
    }

    #[test]
    fn consume_within_cap_succeeds_keyed_by_pair() {
        let (t, _bus) = fixture();
        t.consume(Scope::Pair(key(), pair("BTC/USD")), 10.0, now(0))
            .unwrap();
        let h = t
            .headroom(Scope::Pair(key(), pair("BTC/USD")), now(0))
            .unwrap();
        assert!((h - 50.0).abs() < 1e-6, "got {}", h);
    }

    #[test]
    fn consume_with_apikey_scope_returns_err_not_panic() {
        let (t, _bus) = fixture();
        let err = t.consume(Scope::ApiKey(key()), 1.0, now(0)).unwrap_err();
        assert_eq!(err.tracker, "trading");
        assert!(matches!(err.scope, Scope::ApiKey(_)));
        assert_eq!(err.cap, 0.0);
    }

    #[test]
    fn consume_beyond_cap_returns_exceeded_with_tracker_trading() {
        let (t, _bus) = fixture();
        t.consume(Scope::Pair(key(), pair("BTC/USD")), 60.0, now(0))
            .unwrap();
        let err = t
            .consume(Scope::Pair(key(), pair("BTC/USD")), 1.0, now(0))
            .unwrap_err();
        assert_eq!(err.tracker, "trading");
        assert!((err.cap - 60.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn lru_evicts_least_recently_charged_when_cap_exceeded() {
        let (t, bus) = fixture_with_lru_cap(2);
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let (tx, rx) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell = std::sync::Mutex::new(Some(tx));
        let _ = bus.subscribe(
            EventType::RateLimitCacheEvictionEvent,
            Arc::new(move |env| {
                if let Some(tx) = tx_cell.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            1,
        );
        t.consume(Scope::Pair(key(), pair("BTC/USD")), 1.0, now(0))
            .unwrap();
        t.consume(Scope::Pair(key(), pair("ETH/USD")), 1.0, now(1))
            .unwrap();
        t.consume(Scope::Pair(key(), pair("SOL/USD")), 1.0, now(2))
            .unwrap();
        let env = tokio::time::timeout(std::time::Duration::from_millis(200), rx)
            .await
            .expect("RateLimitCacheEvictionEvent not delivered")
            .expect("sender dropped");
        match env.payload {
            EventPayload::RateLimitCacheEvictionEvent {
                key_id_fingerprint,
                pair,
                reason,
                ..
            } => {
                assert_eq!(reason, "lru_cap_exceeded");
                assert_eq!(
                    pair.as_ref().map(|p| p.as_str()),
                    Some("BTC/USD"),
                    "eviction event must carry the evicted pair"
                );
                assert!(
                    !key_id_fingerprint.is_empty(),
                    "fingerprint must be non-empty"
                );
                assert_ne!(
                    key_id_fingerprint,
                    key().as_str(),
                    "payload must NOT expose the raw API key"
                );
                assert!(
                    !key_id_fingerprint.contains("test-api-key"),
                    "payload must NOT embed the raw API key: {key_id_fingerprint}"
                );
            }
            other => panic!("expected RateLimitCacheEvictionEvent, got {:?}", other),
        }
    }

    /// `seconds_to_drain = used / decay_per_sec` — time to decay to 0, not to cap.
    #[allow(non_snake_case)]
    #[tokio::test]
    async fn warning_carries_seconds_to_drain_equal_used_over_decay() {
        let (t, bus) = fixture();
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let (tx, rx_evt) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell = std::sync::Mutex::new(Some(tx));
        let _ = bus.subscribe(
            EventType::RateLimitWarning,
            Arc::new(move |env| {
                if let Some(tx) = tx_cell.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            1,
        );
        t.consume(Scope::Pair(key(), pair("BTC/USD")), 50.0, now(0))
            .unwrap();
        let env = tokio::time::timeout(std::time::Duration::from_millis(200), rx_evt)
            .await
            .expect("RateLimitWarning not delivered within 200ms")
            .expect("oneshot sender dropped");
        match env.payload {
            EventPayload::RateLimitWarning {
                tracker,
                key_id_fingerprint,
                used,
                decay_per_sec,
                seconds_to_drain,
                ..
            } => {
                assert_eq!(tracker, "trading");
                assert!(
                    !key_id_fingerprint.is_empty(),
                    "fingerprint must be non-empty"
                );
                assert_ne!(
                    key_id_fingerprint,
                    key().as_str(),
                    "payload must NOT expose the raw API key"
                );
                assert!((decay_per_sec - 1.00).abs() < 1e-9);
                assert!((used - 50.0).abs() < 1e-9, "used = {used}");
                assert!(
                    (seconds_to_drain - (used / decay_per_sec)).abs() < 1e-9,
                    "seconds_to_drain {seconds_to_drain} != used/decay {}",
                    used / decay_per_sec
                );
                assert!((seconds_to_drain - 50.0).abs() < 1e-9);
                assert!(
                    seconds_to_drain.is_finite(),
                    "must never be NaN/Inf for a positive finite decay"
                );
            }
            other => panic!("expected RateLimitWarning, got {:?}", other),
        }
        bus.stop_reactors();
    }

    /// Non-positive/non-finite `decay_per_sec` MUST yield `INFINITY`, never `NaN`.
    #[test]
    fn seconds_to_drain_guard_is_infinity_for_non_positive_decay() {
        let compute = |used: f64, decay: f64| -> f64 {
            if decay.is_finite() && decay > 0.0 {
                used / decay
            } else {
                f64::INFINITY
            }
        };
        assert_eq!(compute(50.0, 0.0), f64::INFINITY);
        assert_eq!(compute(50.0, -1.0), f64::INFINITY);
        assert_eq!(compute(50.0, f64::NAN), f64::INFINITY);
        assert_eq!(compute(50.0, f64::INFINITY), f64::INFINITY);
        let v = compute(50.0, 1.0);
        assert!(v.is_finite() && (v - 50.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn arc_clones_share_same_counter() {
        let (t, _bus) = fixture();
        let t_rest = Arc::clone(&t);
        let t_ws = Arc::clone(&t);
        t_rest
            .consume(Scope::Pair(key(), pair("BTC/USD")), 30.0, now(0))
            .unwrap();
        t_ws.consume(Scope::Pair(key(), pair("BTC/USD")), 20.0, now(0))
            .unwrap();
        let h = t
            .headroom(Scope::Pair(key(), pair("BTC/USD")), now(0))
            .unwrap();
        assert!((h - 10.0).abs() < 1e-6, "got {}", h);
    }

    #[test]
    fn credit_reverses_a_prior_consume() {
        let (t, _bus) = fixture();
        let scope = Scope::Pair(key(), pair("BTC/USD"));
        t.consume(scope.clone(), 8.0, now(0)).unwrap();
        t.credit(scope.clone(), 8.0, now(0));
        let h = t.headroom(scope, now(0)).unwrap();
        assert!(
            (h - 60.0).abs() < 1e-6,
            "credit must restore full headroom, got {h}"
        );
    }

    #[test]
    fn credit_on_absent_or_wrong_scope_is_noop() {
        let (t, _bus) = fixture();
        t.credit(Scope::Pair(key(), pair("ETH/USD")), 5.0, now(0));
        t.credit(Scope::ApiKey(key()), 5.0, now(0));
        let h = t
            .headroom(Scope::Pair(key(), pair("ETH/USD")), now(0))
            .unwrap();
        assert!(
            (h - 60.0).abs() < 1e-6,
            "absent scope stays at full cap, got {h}"
        );
    }

    #[test]
    fn charge_account_wide_bumps_every_tracked_pair_saturating_and_never_rejects() {
        let (t, _bus) = fixture();
        t.consume(Scope::Pair(key(), pair("BTC/USD")), 10.0, now(0))
            .unwrap();
        t.consume(Scope::Pair(key(), pair("ETH/USD")), 60.0, now(0))
            .unwrap();
        t.charge_account_wide(Scope::ApiKey(key()), 1.0, now(0));
        let h_btc = t
            .headroom(Scope::Pair(key(), pair("BTC/USD")), now(0))
            .unwrap();
        assert!((h_btc - 49.0).abs() < 1e-6, "BTC headroom {h_btc}");
        let h_eth = t
            .headroom(Scope::Pair(key(), pair("ETH/USD")), now(0))
            .unwrap();
        assert!((h_eth - 0.0).abs() < 1e-6, "ETH headroom {h_eth}");
        // An untracked pair is NOT created — stays at full cap.
        let h_sol = t
            .headroom(Scope::Pair(key(), pair("SOL/USD")), now(0))
            .unwrap();
        assert!((h_sol - 60.0).abs() < 1e-6, "SOL headroom {h_sol}");
    }

    #[test]
    fn charge_account_wide_ignores_pair_scope() {
        let (t, _bus) = fixture();
        t.consume(Scope::Pair(key(), pair("BTC/USD")), 5.0, now(0))
            .unwrap();
        t.charge_account_wide(Scope::Pair(key(), pair("BTC/USD")), 1.0, now(0));
        let h = t
            .headroom(Scope::Pair(key(), pair("BTC/USD")), now(0))
            .unwrap();
        assert!(
            (h - 55.0).abs() < 1e-6,
            "Pair scope must be ignored, got {h}"
        );
    }

    /// `consume()` past cap returns `Err` and MUST NOT emit `RateLimitExceededEvent`
    /// (reactive-only, via `snap_to_cap`).
    #[tokio::test]
    async fn consume_exceed_returns_err_and_does_not_emit_rate_limit_exceeded_event() {
        let (t, bus) = fixture();
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());

        let (tx, rx_evt) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell = std::sync::Mutex::new(Some(tx));
        let _ = bus.subscribe(
            EventType::RateLimitExceededEvent,
            Arc::new(move |env| {
                if let Some(tx) = tx_cell.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            1,
        );

        t.consume(Scope::Pair(key(), pair("BTC/USD")), 60.0, now(0))
            .unwrap();
        let err = t
            .consume(Scope::Pair(key(), pair("BTC/USD")), 1.0, now(0))
            .unwrap_err();
        assert_eq!(err.tracker, "trading", "tracker field");
        assert!(matches!(err.scope, Scope::Pair(_, _)), "scope field");

        let no_event = tokio::time::timeout(std::time::Duration::from_millis(200), rx_evt).await;
        assert!(
            no_event.is_err(),
            "consume-exceed MUST NOT emit RateLimitExceededEvent \
             (reactive-only; canon: errors are returned, not emitted); \
             got an event: {no_event:?}"
        );

        bus.stop_reactors();
    }
}
