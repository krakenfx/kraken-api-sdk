//! Non-trading REST rate-limit accounting keyed by API key. `consume()` is an
//! atomic decay → check → charge under the lock; warnings publish after it drops.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::build::knobs::Knobs;
use crate::clock::Clock;
use crate::dispatch::DispatchEventBus;
use crate::rate_limit::counter::{ChargeOutcome, RateLimitCounter};
use crate::rate_limit::{RateLimitExceeded, Scope, Tier};
use crate::types::{ApiKey, MonotonicInstant};

/// Per-API-key non-trading REST counter.
pub struct SpotApiRateLimitTracker {
    tier: Tier,
    counter: Mutex<HashMap<ApiKey, RateLimitCounter>>,
    bus: Arc<DispatchEventBus>,
    clock: Arc<dyn Clock>,
    /// `rate_limit_api_warning_pct` is read fresh on each `consume` (runtime-mutable).
    knobs: Arc<Knobs>,
}

impl SpotApiRateLimitTracker {
    /// Pure construction; no I/O.
    pub fn new(
        tier: Tier,
        bus: Arc<DispatchEventBus>,
        clock: Arc<dyn Clock>,
        knobs: Arc<Knobs>,
    ) -> Self {
        Self {
            tier,
            counter: Mutex::new(HashMap::new()),
            bus,
            clock,
            knobs,
        }
    }

    /// Atomic check-and-charge; `Err(RateLimitExceeded)` when cost would exceed
    /// cap (caller MUST reject). On `Err` the counter still decays to `now` (no
    /// charge). Emits `RateLimitWarning` only; the exceeded case returns, never emits.
    pub fn consume(
        &self,
        scope: Scope,
        cost: f64,
        now: MonotonicInstant,
    ) -> Result<(), RateLimitExceeded> {
        // Mis-route returns a typed error; `unreachable!()` would poison the tracker mutex.
        let api_key = match &scope {
            Scope::ApiKey(k) => k.clone(),
            Scope::Pair(_, _) => {
                tracing::error!(
                    target: "kraken_sdk::rate_limit",
                    ?scope,
                    "SpotApiRateLimitTracker received Scope::Pair — caller bug; rejecting with mis-routed sentinel"
                );
                return Err(RateLimitExceeded {
                    tracker: "api",
                    scope: scope.clone(),
                    current: 0.0,
                    cap: 0.0,
                    retry_at_monotonic: now,
                });
            }
        };
        let tier = self.tier;
        let cap = tier.api_cap();
        let decay = tier.api_decay_per_sec();
        let warning_pct = self.knobs.rate_limit_api_warning_pct.load();

        enum Emit {
            None,
            Warning {
                used: f64,
                pct: f64,
                decay_per_sec: f64,
                seconds_to_drain: f64,
            },
        }
        let (result, emit) = {
            let mut map = self.counter.lock().expect("api counter lock poisoned");
            let counter = map
                .entry(api_key.clone())
                .or_insert_with(|| RateLimitCounter::new(cap, decay, now));
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
                    (Ok(()), emit)
                }
                ChargeOutcome::Exceeded => {
                    let current = counter.current;
                    let cap = counter.cap;
                    (
                        Err(RateLimitExceeded {
                            tracker: "api",
                            scope: scope.clone(),
                            current,
                            cap,
                            retry_at_monotonic: super::compute_retry_at(
                                current,
                                cap,
                                counter.decay_per_sec,
                                now,
                            ),
                        }),
                        Emit::None,
                    )
                }
            }
        };

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
                "api",
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

    /// Units of headroom remaining for `scope` at `now`. Advisory only
    /// (wire-charging uses `consume()`). `None` on a `Pair` mis-route — a silent
    /// `0.0` would be ambiguous with genuine saturation.
    pub fn headroom(&self, scope: Scope, now: MonotonicInstant) -> Option<f64> {
        let api_key = match scope {
            Scope::ApiKey(k) => k,
            Scope::Pair(_, _) => {
                tracing::error!(
                    target: "kraken_sdk::rate_limit",
                    "SpotApiRateLimitTracker::headroom received Scope::Pair — caller bug"
                );
                return None;
            }
        };
        let cap = self.tier.api_cap();
        let map = self.counter.lock().expect("api counter lock poisoned");
        let h = match map.get(&api_key) {
            Some(counter) => (cap - counter.peek_current(now)).max(0.0),
            None => cap,
        };
        Some(h)
    }

    /// Time until `cost` units of headroom exist for `scope`, assuming no further
    /// charges. `Some(ZERO)` when already covered; `None` on a `Pair` mis-route or
    /// non-positive/non-finite decay. Pure linear-decay projection; advisory only.
    pub fn time_until_headroom(
        &self,
        scope: Scope,
        cost: f64,
        now: MonotonicInstant,
    ) -> Option<Duration> {
        let h = self.headroom(scope, now)?;
        if h >= cost {
            return Some(Duration::ZERO);
        }
        let decay = self.tier.api_decay_per_sec();
        if !decay.is_finite() || decay <= 0.0 {
            return None;
        }
        // try_from_secs_f64: a weakened upstream guard degrades to ZERO, not a panic.
        Some(Duration::try_from_secs_f64((cost - h) / decay).unwrap_or(Duration::ZERO))
    }

    /// Snap the counter to cap — reactive reconcile after a Kraken rate-limit
    /// rejection. Emits one `RateLimitExceededEvent`.
    pub fn snap_to_cap(&self, scope: Scope, kraken_error: &str, now: MonotonicInstant) {
        let api_key = match &scope {
            Scope::ApiKey(k) => k.clone(),
            Scope::Pair(_, _) => return,
        };
        let cap = self.tier.api_cap();
        let decay = self.tier.api_decay_per_sec();
        {
            let mut map = self.counter.lock().expect("api counter lock poisoned");
            let counter = map
                .entry(api_key)
                .or_insert_with(|| RateLimitCounter::new(cap, decay, now));
            counter.snap_to_cap(now);
        }
        super::publish_rate_limit_exceeded(
            &self.bus,
            self.clock.as_ref(),
            "api",
            &scope,
            kraken_error,
            now,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build::knobs::KnobValue;
    use crate::clock::SystemClock;
    use crate::dispatch::DispatchEventBusConfig;
    use crate::dispatch::{EventEnvelope, EventPayload, EventType};
    use std::time::Duration;

    fn fixture() -> (Arc<SpotApiRateLimitTracker>, Arc<DispatchEventBus>) {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let tracker = Arc::new(SpotApiRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(&bus),
            clock,
            Arc::new(Knobs::defaults()),
        ));
        (tracker, bus)
    }

    fn key() -> ApiKey {
        ApiKey::new("test-api-key")
    }

    fn now(s: u64) -> MonotonicInstant {
        MonotonicInstant(Duration::from_secs(s))
    }

    #[test]
    fn consume_within_cap_succeeds_and_advances_counter() {
        let (t, _bus) = fixture();
        for _ in 0..5 {
            t.consume(Scope::ApiKey(key()), 2.0, now(0)).unwrap();
        }
        let remaining = t.headroom(Scope::ApiKey(key()), now(0)).unwrap();
        assert!((remaining - 5.0).abs() < 1e-6, "got {}", remaining);
    }

    #[test]
    fn consume_beyond_cap_returns_exceeded_with_tracker_api() {
        let (t, _bus) = fixture();
        for _ in 0..15 {
            t.consume(Scope::ApiKey(key()), 1.0, now(0)).unwrap();
        }
        let err = t.consume(Scope::ApiKey(key()), 1.0, now(0)).unwrap_err();
        assert_eq!(err.tracker, "api");
        assert!(matches!(err.scope, Scope::ApiKey(_)));
        assert!(err.current >= 15.0);
        assert!((err.cap - 15.0).abs() < f64::EPSILON);
    }

    #[allow(non_snake_case)]
    #[tokio::test]
    async fn consume_crossing_warning_pct_emits_exactly_one_RateLimitWarning() {
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
        t.consume(Scope::ApiKey(key()), 11.0, now(0)).unwrap();
        t.consume(Scope::ApiKey(key()), 2.0, now(0)).unwrap();
        let env = tokio::time::timeout(std::time::Duration::from_millis(200), rx_evt)
            .await
            .expect("RateLimitWarning not delivered within 200ms")
            .expect("oneshot sender dropped");
        match env.payload {
            EventPayload::RateLimitWarning {
                tracker,
                pct,
                used,
                decay_per_sec,
                seconds_to_drain,
                ..
            } => {
                assert_eq!(tracker, "api");
                assert!(pct >= 0.80);
                // seconds_to_drain = used/decay (to 0), NOT (cap-used)/decay.
                assert!((decay_per_sec - 0.33).abs() < 1e-9);
                assert!((used - 13.0).abs() < 1e-9, "used = {used}");
                assert!(
                    (seconds_to_drain - (used / decay_per_sec)).abs() < 1e-6,
                    "seconds_to_drain {seconds_to_drain} != used/decay {}",
                    used / decay_per_sec
                );
                assert!(seconds_to_drain.is_finite());
            }
            other => panic!("expected RateLimitWarning, got {:?}", other),
        }
    }

    #[test]
    fn snap_to_cap_forces_counter_to_cap() {
        let (t, _bus) = fixture();
        t.snap_to_cap(Scope::ApiKey(key()), "EAPI:Rate limit exceeded", now(0));
        let remaining = t.headroom(Scope::ApiKey(key()), now(0)).unwrap();
        assert!(remaining < 1e-6, "expected ~0 remaining, got {}", remaining);
    }

    #[test]
    fn consume_with_pair_scope_returns_err_not_panic() {
        let (t, _bus) = fixture();
        let pair = crate::types::Symbol::new("BTC/USD").unwrap();
        let err = t
            .consume(Scope::Pair(key(), pair.clone()), 1.0, now(0))
            .unwrap_err();
        assert_eq!(err.tracker, "api");
        assert!(matches!(err.scope, Scope::Pair(_, _)));
        assert_eq!(err.cap, 0.0);
    }

    #[test]
    fn headroom_with_pair_scope_returns_none_not_zero() {
        let (t, _bus) = fixture();
        let pair = crate::types::Symbol::new("BTC/USD").unwrap();
        let h = t.headroom(Scope::Pair(key(), pair), now(0));
        assert!(
            h.is_none(),
            "expected None on Scope::Pair mis-route, got {:?}",
            h
        );
    }

    #[allow(non_snake_case)]
    #[tokio::test]
    async fn warning_pct_knob_change_is_honoured_on_next_consume() {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let knobs = Arc::new(Knobs::defaults());
        let t = Arc::new(SpotApiRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(&bus),
            clock,
            Arc::clone(&knobs),
        ));
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

        let prev = knobs
            .set_runtime("rate_limit_api_warning_pct", &KnobValue::F64(0.40))
            .expect("runtime knob set");
        assert_eq!(prev, KnobValue::F64(0.80));

        t.consume(Scope::ApiKey(key()), 5.0, now(0)).unwrap();
        t.consume(Scope::ApiKey(key()), 2.0, now(0)).unwrap();

        let env = tokio::time::timeout(std::time::Duration::from_secs(2), rx_evt)
            .await
            .expect("RateLimitWarning (at lowered threshold) not delivered")
            .expect("oneshot sender dropped");
        match env.payload {
            EventPayload::RateLimitWarning { tracker, pct, .. } => {
                assert_eq!(tracker, "api");
                assert!(
                    (0.40..0.80).contains(&pct),
                    "pct {pct} should be in [0.40,0.80)"
                );
            }
            other => panic!("expected RateLimitWarning, got {:?}", other),
        }
        bus.stop_reactors();
    }

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

        for _ in 0..15 {
            t.consume(Scope::ApiKey(key()), 1.0, now(0)).unwrap();
        }
        let err = t.consume(Scope::ApiKey(key()), 1.0, now(0)).unwrap_err();
        assert_eq!(err.tracker, "api", "tracker field");
        assert!(matches!(err.scope, Scope::ApiKey(_)), "scope field");

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
