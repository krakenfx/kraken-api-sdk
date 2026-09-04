use super::*;
use crate::clock::{Clock, SystemClock};
use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};
use crate::jitter::{FixedJitter, JitterSource, SplitMix64Jitter};
use std::sync::Arc;

/// Default test jitter — a seeded SplitMix64 (deterministic seed, no OS draw).
fn test_jitter() -> Arc<dyn JitterSource> {
    Arc::new(SplitMix64Jitter::with_seed(0xC0FFEE))
}

fn mc(url: WsUrl) -> ManagedConnection {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::defaults(),
        Arc::clone(&clock),
    ));
    let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
        Arc::new(crate::transport::MockWsSocketFactory::new());
    let rate_budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
    ManagedConnection::new(url, bus, factory, rate_budget, clock, test_jitter())
}

fn mc_with_budget(
    url: WsUrl,
    budget: Arc<crate::conn::rate_budget::ConnectionRateBudget>,
) -> ManagedConnection {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::defaults(),
        Arc::clone(&clock),
    ));
    let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
        Arc::new(crate::transport::MockWsSocketFactory::new());
    ManagedConnection::new(url, bus, factory, budget, clock, test_jitter())
}

/// A clock pinned to a fixed instant (`secs` seconds since the monotonic
/// epoch) — makes backoff/timer due-at deterministic in tests.
struct FixedClock(std::time::Duration);
impl Clock for FixedClock {
    fn now(&self) -> crate::types::MonotonicInstant {
        crate::types::MonotonicInstant(self.0)
    }
}

/// MC whose bus carries a custom `Knobs` + `FixedClock` + `FixedJitter`, for the backoff /
/// reconnect-cap / timer-knob tests.
fn mc_with_knobs_clock_jitter(
    url: WsUrl,
    knobs: crate::build::knobs::Knobs,
    now_secs: u64,
    jitter: Arc<dyn JitterSource>,
) -> ManagedConnection {
    let clock: Arc<dyn Clock> = Arc::new(FixedClock(std::time::Duration::from_secs(now_secs)));
    let mut bus = DispatchEventBus::new(DispatchEventBusConfig::defaults(), Arc::clone(&clock));
    bus.set_knobs(Arc::new(knobs));
    let bus = Arc::new(bus);
    let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
        Arc::new(crate::transport::MockWsSocketFactory::new());
    let rate_budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
    ManagedConnection::new(url, bus, factory, rate_budget, clock, jitter)
}

#[test]
fn idle_start_connect_transitions_to_connecting() {
    let mut c = mc(WsUrl::Public);
    assert_eq!(c.state(), ConnectionState::Idle);
    c.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Connecting);
    assert_eq!(c.test_attempt_count(), 1);
}

#[test]
fn token_refresh_timer_arms_disarms_via_next_timer_due() {
    let mut c = mc(WsUrl::Auth);
    assert!(
        c.next_timer_due().is_none(),
        "no timer armed on a fresh Idle MC"
    );
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(1000));
    c.arm_token_refresh(due);
    match c.next_timer_due() {
        Some((d, FsmEvent::TimerTokenRefreshDue)) => assert_eq!(d, due),
        other => panic!("expected TimerTokenRefreshDue at {due:?}, got {other:?}"),
    }
    c.disarm_token_refresh();
    assert!(c.next_timer_due().is_none(), "disarmed → no timer");
}

#[test]
fn idle_force_reconnect_resets_attempt_count_and_goes_to_connecting() {
    let mut c = mc(WsUrl::Public);
    c.test_set_attempt_count(42);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Connecting);
    assert_eq!(c.test_attempt_count(), 0);
    // ForceReconnect from Idle MUST open a socket + arm the upgrade timeout, or the reactor
    // projects `socket() == None`, wires no bridge/timeout, and the connection wedges
    assert!(
        c.socket().is_some(),
        "ForceReconnect from Idle must open a socket"
    );
    assert!(
        c.timers.upgrade_timeout_due_at.is_some(),
        "ForceReconnect from Idle must arm the upgrade timeout"
    );
}

#[test]
fn authenticating_handshake_ok_transitions_to_resubscribing() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.handle_event(FsmEvent::WireAuthHandshakeOk);
    assert_eq!(c.state(), ConnectionState::Resubscribing);
    assert!(!c.awaiting_token_refresh());
}

#[test]
fn authenticating_token_stale_re_enters_and_increments_attempt() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.test_set_attempt_count(0);
    c.test_set_auth_handshake_fail_count(0);
    c.handle_event(FsmEvent::WireAuthHandshakeFailed {
        kind: crate::conn::AuthErrorKind::TokenStale,
    });
    // Token-stale re-handshake bumps BOTH counters: the M-cap counter
    // (auth_handshake_fail_count, gates the cap) and attempt_count.
    assert_eq!(c.state(), ConnectionState::Authenticating);
    assert_eq!(c.test_auth_handshake_fail_count(), 1);
    assert_eq!(
        c.test_attempt_count(),
        1,
        "attempt_count must increment on token-stale re-handshake"
    );
    assert!(c.awaiting_token_refresh());
}

#[test]
fn authenticating_token_stale_handshake_refreshes_then_caps_to_failed() {
    let mut c = mc(WsUrl::Auth); // default knobs → max_auth_handshake_failures = 3
    c.test_set_state(ConnectionState::Authenticating);
    c.test_set_attempt_count(0);
    c.test_set_auth_handshake_fail_count(0);

    for expected_count in [1u32, 2u32] {
        c.handle_event(FsmEvent::WireAuthHandshakeFailed {
            kind: crate::conn::AuthErrorKind::TokenStale,
        });
        assert_eq!(
            c.state(),
            ConnectionState::Authenticating,
            "below-cap token-stale stays Authenticating (refresh+retry), NOT BackingOff (blind reconnect)"
        );
        assert!(
            c.awaiting_token_refresh(),
            "refresh-in-flight is set so the reactor drives force_refresh → fresh token re-handshake"
        );
        assert_eq!(c.test_auth_handshake_fail_count(), expected_count);
        c.test_set_state(ConnectionState::Authenticating);
    }

    let _ = c.test_bus().test_drain_published();

    c.handle_event(FsmEvent::WireAuthHandshakeFailed {
        kind: crate::conn::AuthErrorKind::TokenStale,
    });
    assert_eq!(
        c.state(),
        ConnectionState::Failed,
        "M-th token-stale handshake failure escalates to terminal Failed"
    );
    assert_eq!(c.test_auth_handshake_fail_count(), 3);
    assert!(
        !c.awaiting_token_refresh(),
        "no refresh left in flight on terminal Failed"
    );

    let events = c.test_bus().test_drain_published();
    let auth_failed = events.iter().find_map(|e| match &e.payload {
        crate::dispatch::EventPayload::AuthenticationFailedEvent {
            non_transient_class,
            ..
        } => Some(*non_transient_class),
        _ => None,
    });
    assert_eq!(
        auth_failed,
        Some(crate::dispatch::NonTransientClass::RetryCapExhausted),
        "cap-exhausted token-stale must emit AuthenticationFailedEvent{{RetryCapExhausted}}"
    );
}

#[test]
fn authenticating_transient_handshake_failure_is_distinct_backing_off() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.test_set_auth_handshake_fail_count(0);
    c.handle_event(FsmEvent::WireAuthHandshakeFailed {
        kind: crate::conn::AuthErrorKind::Transient,
    });
    assert_eq!(
        c.state(),
        ConnectionState::BackingOff,
        "Transient handshake failure → BackingOff (distinct from the token-stale refresh-and-retry arm)"
    );
    assert_eq!(
        c.test_auth_handshake_fail_count(),
        0,
        "the Transient arm does NOT touch the token-stale M-cap counter"
    );
}

#[test]
fn authenticating_handshake_ok_resets_auth_fail_count() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.test_set_auth_handshake_fail_count(2);
    c.handle_event(FsmEvent::WireAuthHandshakeOk);
    assert_eq!(c.state(), ConnectionState::Resubscribing);
    assert_eq!(
        c.test_auth_handshake_fail_count(),
        0,
        "auth-fail count reset on handshake_ok"
    );
}

#[test]
fn authenticating_order_auth_ok_transitions_to_open() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.test_set_attempt_count(4);
    c.test_set_auth_handshake_fail_count(2);
    c.handle_event(FsmEvent::WireOrderAuthOk);
    assert_eq!(
        c.state(),
        ConnectionState::Open,
        "WireOrderAuthOk drives Authenticating → Open directly (bypasses Resubscribing)"
    );
    assert_eq!(c.test_attempt_count(), 0, "enter_open resets attempt_count");
    assert_eq!(
        c.test_auth_handshake_fail_count(),
        0,
        "auth-fail streak cleared on token-accept (mirrors WireAuthHandshakeOk)"
    );
    assert!(c.has_been_open(), "has_been_open set on entering Open");
    assert!(!c.awaiting_token_refresh());
}

#[test]
fn authenticating_close_1008_transitions_to_failed_else_backing_off() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1008,
        reason: Some("policy violation".into()),
    });
    assert_eq!(c.state(), ConnectionState::Failed);

    let mut c2 = mc(WsUrl::Auth);
    c2.test_set_state(ConnectionState::Authenticating);
    c2.handle_event(FsmEvent::WireCloseReceived {
        code: 1006,
        reason: None,
    });
    assert_eq!(c2.state(), ConnectionState::BackingOff);
}

#[test]
fn authenticating_token_refreshed_clears_inflight_stays_authenticating() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.handle_event(FsmEvent::WireAuthHandshakeFailed {
        kind: crate::conn::AuthErrorKind::TokenStale,
    });
    assert!(c.awaiting_token_refresh());
    c.handle_event(FsmEvent::BusTokenRefreshed { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Authenticating);
    assert!(!c.awaiting_token_refresh());
}

/// The "this send would be the auth probe" predicate, table-driven over the FULL in-flight
/// axis: a bare-ORDER probe lives in the pending-request map, not the subscribe-ack map, so
/// checking only one of them lets a refresh re-issue duplicate an in-flight probe's subscribe.
#[test]
fn authenticating_nothing_in_flight_covers_every_probe_kind() {
    use super::inner::PendingRequest;
    use crate::types::ChannelName;
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    assert!(c.authenticating_nothing_in_flight(), "idle Authenticating");

    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.arm_subscribe_ack(
        ChannelName::Executions,
        None,
        crate::types::MonotonicInstant(std::time::Duration::from_secs(1)),
    );
    assert!(
        !c.authenticating_nothing_in_flight(),
        "a subscribe probe in flight must suppress a second send"
    );

    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    let (tx, _rx) = tokio::sync::oneshot::channel();
    c.record_pending_request(PendingRequest {
        req_id: 7,
        op: crate::dispatch::WsOp::AddOrder,
        sent_at: crate::types::MonotonicInstant(std::time::Duration::from_secs(0)),
        tx,
    });
    assert!(
        !c.authenticating_nothing_in_flight(),
        "an order probe in flight must suppress a second send"
    );

    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.set_awaiting_token_refresh(true);
    assert!(!c.authenticating_nothing_in_flight(), "refresh in flight");

    for st in [
        ConnectionState::Idle,
        ConnectionState::Connecting,
        ConnectionState::Resubscribing,
        ConnectionState::Open,
        ConnectionState::BackingOff,
        ConnectionState::Closing,
        ConnectionState::Closed,
        ConnectionState::Failed,
    ] {
        let mut c = mc(WsUrl::Auth);
        c.test_set_state(st);
        assert!(
            !c.authenticating_nothing_in_flight(),
            "{st:?} is not the auth-probe window"
        );
    }
}

#[test]
fn removed_key_drops_its_deferral_record() {
    use crate::conn::SubscribeParams;
    use crate::types::ChannelName;
    let mut c = mc(WsUrl::Auth);
    c.defer_open_auth_subscribe(ChannelName::Executions, None, SubscribeParams::Executions);
    c.defer_open_auth_subscribe(ChannelName::Balances, None, SubscribeParams::Balances);
    c.remove_deferred_open_auth_subscribe(ChannelName::Executions, &None);
    assert_eq!(
        c.drain_deferred_open_auth_subscribes(),
        vec![(ChannelName::Balances, None, SubscribeParams::Balances)],
        "only the removed key's record dies"
    );
}

#[test]
fn socket_teardown_resets_awaiting_token_refresh() {
    // A refresh outcome landing in BackingOff has no arm to clear the flag, so the flag must
    // die with the socket — else it suppresses every later force_refresh kick and the
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Resubscribing);
    c.set_awaiting_token_refresh(true);
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1006,
        reason: None,
    });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(
        !c.awaiting_token_refresh(),
        "the refresh-in-flight flag dies on the socket-teardown edge"
    );
}

#[test]
fn socket_teardown_clears_deferrals() {
    use crate::conn::SubscribeParams;
    use crate::types::ChannelName;
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Resubscribing);
    c.defer_open_auth_subscribe(ChannelName::Executions, None, SubscribeParams::Executions);
    // Transient close → BackingOff teardown: the reconnect replay re-issues the
    // whole registry, so the deferral must die with the socket.
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1006,
        reason: None,
    });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(
        c.drain_deferred_open_auth_subscribes().is_empty(),
        "deferrals die on the socket-teardown edge"
    );
}

#[test]
fn resubscribing_token_refreshed_clears_inflight_stays_resubscribing() {
    // A refresh kicked by a deferred register resolving mid-replay must clear the in-flight
    // flag (else it suppresses every later force_refresh kick and the refresh drain) without
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Resubscribing);
    c.set_awaiting_token_refresh(true);
    c.handle_event(FsmEvent::BusTokenRefreshed { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Resubscribing);
    assert!(
        !c.awaiting_token_refresh(),
        "refresh outcome mid-replay must clear the in-flight flag"
    );
}

#[test]
fn resubscribing_token_refresh_failed_clears_inflight_stays_resubscribing() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Resubscribing);
    c.set_awaiting_token_refresh(true);
    c.handle_event(FsmEvent::BusTokenRefreshFailed {
        request_id: 1,
        error: crate::auth::AuthError::TokenRefreshTransient,
    });
    assert_eq!(c.state(), ConnectionState::Resubscribing);
    assert!(!c.awaiting_token_refresh());
}

#[test]
fn authenticating_token_refresh_transient_transitions_to_backing_off() {
    // A transient (retryable) token-fetch failure auto-recovers: the FSM backs off and the
    // normal reconnect cycle re-fetches the token.
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.handle_event(FsmEvent::BusTokenRefreshFailed {
        request_id: 1,
        error: crate::auth::AuthError::TokenRefreshTransient,
    });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(
        c.timers.backoff_due_at.is_some(),
        "transient token-refresh failure arms the backoff timer"
    );
    assert!(
        c.socket().is_none(),
        "Authenticating teardown drops the socket on the transient path"
    );
}

#[test]
fn resubscribing_last_ack_transitions_to_open_and_resets_attempt_count() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    c.test_set_attempt_count(3);
    c.handle_event(FsmEvent::WireSubscribeAck {
        channel: ChannelName::Ticker,
        pair: None,
        last: true,
    });
    assert_eq!(c.state(), ConnectionState::Open);
    assert_eq!(c.test_attempt_count(), 0);
}

#[test]
fn backing_off_timer_elapsed_transitions_to_connecting() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::BackingOff);
    c.test_set_attempt_count(2);
    c.handle_event(FsmEvent::TimerBackoffElapsed);
    assert_eq!(c.state(), ConnectionState::Connecting);
    assert_eq!(c.test_attempt_count(), 3);
}

#[test]
fn backing_off_force_reconnect_resets_attempt_count() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::BackingOff);
    c.test_set_attempt_count(5);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Connecting);
    assert_eq!(c.test_attempt_count(), 0);
}

#[test]
fn closed_is_terminal_against_caller_events() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Closed);
    c.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Closed);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Closed);
}

#[test]
fn failed_force_reconnect_transitions_to_connecting() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Failed);
    c.test_set_attempt_count(7);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Connecting);
    assert_eq!(c.test_attempt_count(), 0);
    // `force_reconnect` is the documented recovery from terminal Failed; it MUST open a socket
    // + arm the upgrade timeout, or recovery wedges in Connecting forever
    assert!(
        c.socket().is_some(),
        "ForceReconnect from Failed must open a socket"
    );
    assert!(
        c.timers.upgrade_timeout_due_at.is_some(),
        "ForceReconnect from Failed must arm the upgrade timeout"
    );
}

#[test]
fn happy_path_idle_to_open_public_url() {
    let mut c = mc(WsUrl::Public);
    c.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Connecting);
    c.handle_event(FsmEvent::WireUpgradeOk {
        connection_id: Some(7),
    });
    assert_eq!(c.state(), ConnectionState::Resubscribing);
    c.handle_event(FsmEvent::WireSubscribeAck {
        channel: ChannelName::Ticker,
        pair: None,
        last: true,
    });
    assert_eq!(c.state(), ConnectionState::Open);
    assert_eq!(c.test_attempt_count(), 0);
}

#[test]
fn recovery_path_open_to_open_via_disconnect() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Open);
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1006,
        reason: None,
    });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    c.handle_event(FsmEvent::TimerBackoffElapsed);
    assert_eq!(c.state(), ConnectionState::Connecting);
    c.handle_event(FsmEvent::WireUpgradeOk {
        connection_id: Some(8),
    });
    assert_eq!(c.state(), ConnectionState::Resubscribing);
    c.handle_event(FsmEvent::WireSubscribeAck {
        channel: ChannelName::Ticker,
        pair: None,
        last: true,
    });
    assert_eq!(c.state(), ConnectionState::Open);
}

#[test]
fn idle_start_connect_throttled_lands_in_backing_off_without_socket() {
    let budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::with_knobs(
        1,
        std::time::Duration::from_secs(60),
    ));
    budget
        .try_consume(crate::types::MonotonicInstant::now())
        .unwrap();
    let mut c = mc_with_budget(WsUrl::Public, budget);
    c.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(c.socket().is_none());
    assert!(c.timers.rate_budget_window_advanced_at.is_some());
}

#[test]
fn backing_off_timer_elapsed_throttled_stays_in_backing_off_no_socket() {
    let budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::with_knobs(
        1,
        std::time::Duration::from_secs(60),
    ));
    budget
        .try_consume(crate::types::MonotonicInstant::now())
        .unwrap();
    let mut c = mc_with_budget(WsUrl::Public, budget);
    c.test_set_state(ConnectionState::BackingOff);
    c.handle_event(FsmEvent::TimerBackoffElapsed);
    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(c.socket().is_none());
    assert!(c.timers.rate_budget_window_advanced_at.is_some());
}

#[test]
fn backing_off_force_reconnect_cohorts_pending_connect_waiter() {
    // A throttled reconnect leg leaves BackingOff carrying a pending connect waiter; a second
    // force_reconnect must cohort it, not overwrite the latch (which would strand the first
    let budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::with_knobs(
        1,
        std::time::Duration::from_secs(60),
    ));
    budget
        .try_consume(crate::types::MonotonicInstant::now())
        .unwrap();
    let mut c = mc_with_budget(WsUrl::Public, budget);
    c.test_set_has_been_open(true);
    c.test_set_state(ConnectionState::Closing);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 7 });
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1000,
        reason: None,
    });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 9 });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert_eq!(
        c.test_additional_connect_request_ids(),
        vec![9],
        "second force_reconnect must cohort, not overwrite the pending connect waiter"
    );
}

#[test]
fn idle_start_connect_with_headroom_consumes_budget_and_opens_socket() {
    let budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::with_knobs(
        5,
        std::time::Duration::from_secs(60),
    ));
    let mut c = mc_with_budget(WsUrl::Public, Arc::clone(&budget));
    c.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Connecting);
    assert!(c.socket().is_some());
    assert!(
        budget
            .try_consume(crate::types::MonotonicInstant::now())
            .is_ok()
    );
}

// force_reconnect is NOT a budget bypass — a throttled force_reconnect from
// Idle/Failed lands in BackingOff WITHOUT opening a socket.
#[test]
fn idle_force_reconnect_throttled_lands_in_backing_off_without_socket() {
    let budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::with_knobs(
        1,
        std::time::Duration::from_secs(60),
    ));
    budget
        .try_consume(crate::types::MonotonicInstant::now())
        .unwrap();
    let mut c = mc_with_budget(WsUrl::Public, budget);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(c.socket().is_none());
    assert!(c.timers.rate_budget_window_advanced_at.is_some());
}

#[test]
fn failed_force_reconnect_throttled_lands_in_backing_off_without_socket() {
    let budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::with_knobs(
        1,
        std::time::Duration::from_secs(60),
    ));
    budget
        .try_consume(crate::types::MonotonicInstant::now())
        .unwrap();
    let mut c = mc_with_budget(WsUrl::Public, budget);
    c.test_set_state(ConnectionState::Failed);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(c.socket().is_none());
    assert!(c.timers.rate_budget_window_advanced_at.is_some());
}

/// MC on a real bus with the dispatch reactor running + a `subscribe_correlated` one-shot
/// keyed by `(event_type, expected_request_id)`.
fn mc_with_correlated_subscribe(
    url: WsUrl,
    event_type: crate::dispatch::EventType,
    expected_request_id: u64,
) -> (
    ManagedConnection,
    tokio::sync::oneshot::Receiver<crate::dispatch::EventEnvelope>,
) {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::defaults(),
        Arc::clone(&clock),
    ));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx_cell = std::sync::Mutex::new(Some(tx));
    let _ = bus.subscribe_correlated(
        event_type,
        expected_request_id,
        Box::new(move |env| match tx_cell.lock().unwrap().take() {
            Some(tx) => tx.send(env.clone()).is_ok(),
            None => false,
        }),
    );
    bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
    let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
        Arc::new(crate::transport::MockWsSocketFactory::new());
    let rate_budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
    let mc = ManagedConnection::new(url, bus, factory, rate_budget, clock, test_jitter());
    (mc, rx)
}

/// Like `mc_with_correlated_subscribe` but registers a long-lived (broadcast) subscriber — for
/// events emitted with `request_id: None` (e.g.
fn mc_with_long_lived_subscribe(
    url: WsUrl,
    event_type: crate::dispatch::EventType,
) -> (
    ManagedConnection,
    tokio::sync::oneshot::Receiver<crate::dispatch::EventEnvelope>,
) {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::defaults(),
        Arc::clone(&clock),
    ));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx_cell = std::sync::Mutex::new(Some(tx));
    let _ = bus.subscribe(
        event_type,
        Arc::new(move |env: &crate::dispatch::EventEnvelope| {
            if let Some(tx) = tx_cell.lock().unwrap().take() {
                let _ = tx.send(env.clone());
            }
        }),
        u16::MAX,
    );
    bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
    let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
        Arc::new(crate::transport::MockWsSocketFactory::new());
    let rate_budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
    let mc = ManagedConnection::new(url, bus, factory, rate_budget, clock, test_jitter());
    (mc, rx)
}

#[tokio::test]
async fn caller_driven_start_connect_emits_connecting_event_with_request_id() {
    let (mut c, rx) = mc_with_correlated_subscribe(
        WsUrl::Public,
        crate::dispatch::EventType::ConnectionConnectingEvent,
        99,
    );
    c.handle_event(FsmEvent::CallStartConnect { request_id: 99 });
    let env = tokio::time::timeout(std::time::Duration::from_millis(100), rx)
        .await
        .expect("ConnectionConnectingEvent not delivered")
        .expect("oneshot sender dropped");
    assert_eq!(env.request_id, Some(99));
    assert_eq!(
        env.event_type,
        crate::dispatch::EventType::ConnectionConnectingEvent
    );
}

#[tokio::test]
async fn caller_driven_close_emits_closed_event_with_request_id() {
    let (mut c, rx) = mc_with_correlated_subscribe(
        WsUrl::Public,
        crate::dispatch::EventType::ConnectionClosedEvent,
        7,
    );
    c.handle_event(FsmEvent::CallClose { request_id: 7 });
    let env = tokio::time::timeout(std::time::Duration::from_millis(100), rx)
        .await
        .expect("ConnectionClosedEvent not delivered")
        .expect("oneshot sender dropped");
    assert_eq!(env.request_id, Some(7));
}

#[tokio::test]
async fn open_event_carries_pending_connect_request_id_through_full_handshake() {
    let (mut c, rx) = mc_with_correlated_subscribe(
        WsUrl::Public,
        crate::dispatch::EventType::ConnectionOpenEvent,
        123,
    );
    c.handle_event(FsmEvent::CallStartConnect { request_id: 123 });
    c.handle_event(FsmEvent::WireUpgradeOk {
        connection_id: Some(42),
    });
    c.handle_event(FsmEvent::WireSubscribeAck {
        channel: ChannelName::Ticker,
        pair: None,
        last: true,
    });
    let env = tokio::time::timeout(std::time::Duration::from_millis(100), rx)
        .await
        .expect("ConnectionOpenEvent not delivered")
        .expect("oneshot sender dropped");
    assert_eq!(env.request_id, Some(123));
}

#[test]
fn first_open_emits_open_event_not_reopened() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    c.complete_resubscribe_if_empty();
    assert_eq!(c.state(), ConnectionState::Open);
    let events = c.test_bus().test_drain_published();
    assert!(
        events
            .iter()
            .any(|e| e.event_type == crate::dispatch::EventType::ConnectionOpenEvent),
        "first open must emit ConnectionOpenEvent"
    );
    assert!(
        events
            .iter()
            .all(|e| e.event_type != crate::dispatch::EventType::ConnectionReopenedEvent),
        "first open must NOT emit ConnectionReopenedEvent"
    );
}

#[test]
fn reopen_emits_reopened_event_with_attempt_count() {
    let mut c = mc(WsUrl::Public);
    c.test_set_has_been_open(true);
    c.test_set_state(ConnectionState::Resubscribing);
    c.test_set_attempt_count(3);
    c.complete_resubscribe_if_empty();
    assert_eq!(c.state(), ConnectionState::Open);
    assert_eq!(c.test_attempt_count(), 0, "enter_open resets attempt_count");
    let events = c.test_bus().test_drain_published();
    assert!(
        events
            .iter()
            .all(|e| e.event_type != crate::dispatch::EventType::ConnectionOpenEvent),
        "reopen must NOT emit ConnectionOpenEvent"
    );
    let reopened: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == crate::dispatch::EventType::ConnectionReopenedEvent)
        .collect();
    assert_eq!(
        reopened.len(),
        1,
        "reopen emits exactly one ConnectionReopenedEvent"
    );
    match &reopened[0].payload {
        crate::dispatch::EventPayload::ConnectionReopenedEvent { attempt_count, .. } => {
            assert_eq!(
                *attempt_count, 3,
                "Reopened reports the pre-reset succeeding attempt number"
            );
        }
        other => panic!("unexpected payload: {other:?}"),
    }
}

// force_reconnect() mints a handle that correlates on ConnectionReopenedEvent.
#[tokio::test]
async fn force_reconnect_await_resolves_via_reopened_event() {
    let (mut c, rx) = mc_with_correlated_subscribe(
        WsUrl::Public,
        crate::dispatch::EventType::ConnectionReopenedEvent,
        99,
    );
    c.test_set_has_been_open(true);
    c.test_set_state(ConnectionState::BackingOff);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 99 });
    assert_eq!(c.state(), ConnectionState::Connecting);
    c.handle_event(FsmEvent::WireUpgradeOk {
        connection_id: Some(7),
    });
    c.complete_resubscribe_if_empty();
    assert_eq!(c.state(), ConnectionState::Open);
    let env = tokio::time::timeout(std::time::Duration::from_millis(100), rx)
        .await
        .expect("ConnectionReopenedEvent not delivered — force_reconnect await would hang")
        .expect("oneshot sender dropped");
    assert_eq!(env.request_id, Some(99));
}

/// Test correlation key — a `(channel, pair)` tuple.
fn ack_key() -> (ChannelName, Option<Symbol>) {
    (ChannelName::Ticker, Some(Symbol::new("BTC/USD").unwrap()))
}

#[test]
fn arm_subscribe_ack_inserts_timer_and_seeds_attempts() {
    let mut c = mc(WsUrl::Public);
    let sub = ack_key();
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_subscribe_ack(sub.0, sub.1.clone(), due);
    assert!(c.timers.per_entry_subscribe_ack_timeouts.contains_key(&sub));
    assert_eq!(c.subscribe_ack_attempts.get(&sub).copied(), Some(3));
    let (next_due, ev) = c.next_timer_due().expect("timer armed");
    assert_eq!(next_due, due);
    assert!(matches!(
        ev,
        FsmEvent::TimerSubscribeAckTimeout { channel, pair } if channel == sub.0 && pair == sub.1
    ));
}

#[test]
fn arm_book_reseed_snapshot_surfaces_via_next_timer_due_and_clears_on_teardown() {
    let mut c = mc(WsUrl::Public);
    let key = (ChannelName::Book, Some(Symbol::new("BTC/USD").unwrap()));
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_book_reseed_snapshot(key.0, key.1.clone(), due);
    let (next_due, ev) = c.next_timer_due().expect("reseed timer armed");
    assert_eq!(next_due, due);
    assert!(matches!(
        ev,
        FsmEvent::TimerBookReseedSnapshot { channel, pair } if channel == key.0 && pair == key.1
    ));
    // A state-exit teardown (clear_subscribe_ack_state) clears the per-entry
    // reseed timer too, so a stale fire can't re-reseed mid-reconnect.
    c.clear_subscribe_ack_state();
    assert!(
        c.next_timer_due().is_none(),
        "reseed timer cleared on teardown"
    );
}

#[test]
fn wire_subscribe_ack_disarms_timer_and_clears_attempts() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    let sub = ack_key();
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_subscribe_ack(sub.0, sub.1.clone(), due);
    c.handle_event(FsmEvent::WireSubscribeAck {
        channel: sub.0,
        pair: sub.1.clone(),
        last: false,
    });
    assert!(!c.timers.per_entry_subscribe_ack_timeouts.contains_key(&sub));
    assert!(!c.subscribe_ack_attempts.contains_key(&sub));
    assert_eq!(c.state(), ConnectionState::Resubscribing);
}

#[test]
fn timer_subscribe_ack_timeout_within_budget_stays_resubscribing() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    let sub = ack_key();
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_subscribe_ack(sub.0, sub.1.clone(), due);
    c.handle_event(FsmEvent::TimerSubscribeAckTimeout {
        channel: sub.0,
        pair: sub.1.clone(),
    });
    assert_eq!(c.state(), ConnectionState::Resubscribing);
    assert_eq!(c.subscribe_ack_attempts.get(&sub).copied(), Some(2));
    assert!(!c.timers.per_entry_subscribe_ack_timeouts.contains_key(&sub));
}

#[allow(non_snake_case)]
#[tokio::test]
async fn non_transient_subscribe_failed_emits_SubscriptionTerminatedEvent() {
    let (mut c, rx) = mc_with_long_lived_subscribe(
        WsUrl::Public,
        crate::dispatch::EventType::SubscriptionTerminatedEvent,
    );
    c.test_set_state(ConnectionState::Resubscribing);
    let sub = ack_key();
    c.arm_subscribe_ack(
        sub.0,
        sub.1.clone(),
        crate::types::MonotonicInstant(std::time::Duration::from_secs(5)),
    );
    c.handle_event(FsmEvent::WireSubscribeFailed {
        channel: sub.0,
        pair: sub.1.clone(),
        error: crate::conn::SubscribeErrorKind::SubscribeRejected {
            kraken_code: "EOrder:Invalid market".into(),
            transient: false,
        },
    });
    // Per-entry only: the connection survives — and since this was the LAST pending entry, the
    // emptied ack set completes the replay → Open (staying in Resubscribing here would park
    assert_eq!(c.state(), ConnectionState::Open);
    let env = tokio::time::timeout(std::time::Duration::from_millis(100), rx)
        .await
        .expect("SubscriptionTerminatedEvent not delivered")
        .expect("sender dropped");
    if let crate::dispatch::EventPayload::SubscriptionTerminatedEvent { cause, .. } = env.payload {
        assert!(matches!(
            cause,
            crate::api::subscription::TerminationCause::NonTransientWireRejection
        ));
    } else {
        panic!("expected SubscriptionTerminatedEvent");
    }
}

#[test]
fn timer_subscribe_ack_timeout_exhausts_budget_teardown_to_backing_off() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    let sub = ack_key();
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_subscribe_ack(sub.0, sub.1.clone(), due);
    c.subscribe_ack_attempts.insert(sub.clone(), 1);
    c.handle_event(FsmEvent::TimerSubscribeAckTimeout {
        channel: sub.0,
        pair: sub.1.clone(),
    });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(c.timers.backoff_due_at.is_some());
    assert!(c.timers.per_entry_subscribe_ack_timeouts.is_empty());
    assert!(c.subscribe_ack_attempts.is_empty());
    assert!(c.socket.is_none());
}

#[test]
fn open_timer_subscribe_ack_timeout_is_transient_retry_not_stuck() {
    // A subscribe-ack TIMEOUT while Open (gap-recovery resubscribe or a lost
    // register-while-Open ack) must be a transient retry (resend under budget), NOT an
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Open);
    let sub = ack_key();
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_subscribe_ack(sub.0, sub.1.clone(), due);
    c.subscribe_ack_attempts.insert(sub.clone(), 2);
    c.handle_event(FsmEvent::TimerSubscribeAckTimeout {
        channel: sub.0,
        pair: sub.1.clone(),
    });
    assert_eq!(c.state(), ConnectionState::Open);
    assert!(
        c.timers.per_entry_subscribe_resend_due.contains_key(&sub),
        "Open subscribe-ack timeout must arm a resend timer (transient retry), not no-op"
    );
}

#[test]
fn authenticating_timer_subscribe_ack_timeout_is_transient_retry_not_stuck() {
    // A lost first-signed-subscribe ack while Authenticating must be a transient
    // retry (resend under budget), not the residual no-op that wedges the conn.
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    let sub = ack_key();
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_subscribe_ack(sub.0, sub.1.clone(), due);
    c.subscribe_ack_attempts.insert(sub.clone(), 2);
    c.handle_event(FsmEvent::TimerSubscribeAckTimeout {
        channel: sub.0,
        pair: sub.1.clone(),
    });
    assert_eq!(c.state(), ConnectionState::Authenticating);
    assert!(
        c.timers.per_entry_subscribe_resend_due.contains_key(&sub),
        "Authenticating subscribe-ack timeout must arm a resend timer (transient retry), not no-op"
    );
}

#[test]
fn open_subscribe_budget_exhaustion_drains_pending_and_backs_off() {
    // Exhausting the subscribe-ack budget while Open must escalate like a connection drop and
    // drain-before-emit: resolve every in-flight WS request Err so the caller's await can't
    use super::inner::PendingRequest;
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Open);
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    c.record_pending_request(PendingRequest {
        req_id: 42,
        op: crate::dispatch::WsOp::AddOrder,
        sent_at: crate::types::MonotonicInstant(std::time::Duration::from_secs(0)),
        tx,
    });
    let sub = ack_key();
    c.subscribe_ack_attempts.insert(sub.clone(), 1);
    c.handle_subscribe_failure(sub.0, sub.1.clone(), true, None);

    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(c.socket.is_none());
    assert!(c.timers.per_entry_subscribe_resend_due.is_empty());
    assert!(c.subscribe_ack_attempts.is_empty());
    match rx.try_recv() {
        Ok(Err(_)) => {}
        other => panic!(
            "Open exhaustion MUST drain the pending request Err (caller await must not hang); got {other:?}"
        ),
    }
}

#[test]
fn authenticating_subscribe_budget_exhaustion_drains_pending_bare_order() {
    // A bare order admitted directly in Authenticating: if first-signed-subscribe
    // backpressure-exhausts here, teardown must STILL drain the in-flight order Err — the
    use super::inner::PendingRequest;
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    let (tx, mut rx) = tokio::sync::oneshot::channel();
    c.record_pending_request(PendingRequest {
        req_id: 7,
        op: crate::dispatch::WsOp::AddOrder,
        sent_at: crate::types::MonotonicInstant(std::time::Duration::from_secs(0)),
        tx,
    });
    let sub = ack_key();
    c.subscribe_ack_attempts.insert(sub.clone(), 1);
    c.handle_subscribe_failure(sub.0, sub.1.clone(), true, None);

    assert_eq!(c.state(), ConnectionState::BackingOff);
    match rx.try_recv() {
        Ok(Err(_)) => {}
        other => panic!(
            "Authenticating exhaustion MUST drain the in-flight bare order Err \
             (await must not hang); got {other:?}"
        ),
    }
}

#[test]
fn authenticating_abnormal_close_clears_ack_timers() {
    // The auth handshake arms the first-signed-subscribe ack timer.
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_subscribe_ack(ChannelName::Executions, None, due);
    assert!(!c.timers.per_entry_subscribe_ack_timeouts.is_empty());
    c.handle_event(FsmEvent::WireAbnormalClose {
        error: crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        },
    });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(c.timers.per_entry_subscribe_ack_timeouts.is_empty());
    assert!(c.subscribe_ack_attempts.is_empty());
}

use crate::conn::{SubscribeParams, SubscriptionEntry, SubscriptionRegistry};

#[test]
fn replay_arms_one_timer_per_composed_frame_and_drains_to_open() {
    let mut reg = SubscriptionRegistry::new();
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Status,
            None,
            SubscribeParams::Status,
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Ticker,
            Some(Symbol::new("BTC/USD").unwrap()),
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Ticker,
            Some(Symbol::new("ETH/USD").unwrap()),
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );
    reg.register(
        SubscriptionEntry::new(
            WsUrl::Public,
            ChannelName::Book,
            Some(Symbol::new("BTC/USD").unwrap()),
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        ),
        crate::types::MonotonicInstant::now(),
        false,
    );

    let mut c = mc(WsUrl::Public);
    c.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
    c.handle_event(FsmEvent::WireUpgradeOk {
        connection_id: Some(1),
    });
    assert_eq!(c.state(), ConnectionState::Resubscribing);

    let frames = reg.compose_subscribe_frames_for_url(WsUrl::Public);
    assert_eq!(frames.len(), 4, "all 4 public entries composed");
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    let keys: Vec<(ChannelName, Option<Symbol>)> = frames
        .iter()
        .map(|(ch, pair, _)| (*ch, pair.clone()))
        .collect();
    for (ch, pair) in &keys {
        c.arm_subscribe_ack(*ch, pair.clone(), due);
    }
    assert_eq!(
        c.pending_subscribe_ack_count(),
        4,
        "one armed timer per frame"
    );

    for (ch, pair) in &keys {
        let last = c.pending_subscribe_ack_count() <= 1;
        c.handle_event(FsmEvent::WireSubscribeAck {
            channel: *ch,
            pair: pair.clone(),
            last,
        });
    }

    assert_eq!(
        c.state(),
        ConnectionState::Open,
        "draining all acks reaches Open"
    );
    assert_eq!(c.pending_subscribe_ack_count(), 0, "all timers disarmed");
}

#[test]
fn has_been_open_false_until_first_open_then_sticky() {
    let mut c = mc(WsUrl::Public);
    assert!(!c.has_been_open(), "false before first Open");
    c.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
    c.handle_event(FsmEvent::WireUpgradeOk {
        connection_id: Some(1),
    });
    assert!(!c.has_been_open(), "still false in Resubscribing");
    c.handle_event(FsmEvent::WireSubscribeAck {
        channel: ChannelName::Ticker,
        pair: None,
        last: true,
    });
    assert_eq!(c.state(), ConnectionState::Open);
    assert!(c.has_been_open(), "set on reaching Open");
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1006,
        reason: None,
    });
    assert!(c.has_been_open(), "never reset");
}

#[test]
fn empty_reconnect_short_circuits_resubscribing_to_open() {
    // Liveness: a reconnect with zero subscriptions composes 0 frames → 0 ack timers → no
    // last-ack transition.
    let reg = SubscriptionRegistry::new();
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    let frames = reg.compose_subscribe_frames_for_url(WsUrl::Public);
    assert!(frames.is_empty());
    assert_eq!(c.pending_subscribe_ack_count(), 0);
    c.complete_resubscribe_if_empty();
    assert_eq!(
        c.state(),
        ConnectionState::Open,
        "empty replay → Open, not stuck"
    );
}

#[test]
fn complete_resubscribe_if_empty_is_noop_with_pending_timers() {
    // With ≥1 armed ack timer, the short-circuit must NOT fire — the connection
    // reaches Open only via the real last-ack, not prematurely.
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_subscribe_ack(
        ChannelName::Ticker,
        Some(Symbol::new("BTC/USD").unwrap()),
        due,
    );
    c.complete_resubscribe_if_empty();
    assert_eq!(
        c.state(),
        ConnectionState::Resubscribing,
        "stays — ack still pending"
    );
}

#[test]
fn complete_resubscribe_if_empty_is_noop_with_pending_resend_timer() {
    // A backpressured replay leaves a RESEND timer pending (no ack timer); the short-circuit
    // must NOT fire — Reopened waits for the retry to ack, not for a not-yet-live stream.
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    let sub = ack_key();
    c.subscribe_ack_attempts.insert(sub.clone(), 2);
    c.handle_subscribe_failure(sub.0, sub.1.clone(), true, None);
    assert_eq!(
        c.pending_subscribe_ack_count(),
        0,
        "no ack timer on the backpressure path"
    );
    assert!(
        c.timers.per_entry_subscribe_resend_due.contains_key(&sub),
        "resend timer armed"
    );
    c.complete_resubscribe_if_empty();
    assert_eq!(
        c.state(),
        ConnectionState::Resubscribing,
        "stays — resend still pending, not prematurely Open"
    );
}

// (Closing, force_reconnect): no transition, no immediate event; rid latched for
// the eventual via-Idle re-entry.
#[test]
fn closing_force_reconnect_latches_with_no_immediate_event() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Closing);
    c.test_set_pending_close_request_id(Some(1));
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 7 });
    assert_eq!(
        c.state(),
        ConnectionState::Closing,
        "(Closing, force_reconnect) is a no-transition latch"
    );
    assert_eq!(
        c.test_pending_reconnect_request_id(),
        Some(7),
        "reconnect rid latched"
    );
    assert!(
        c.test_bus().test_drain_published().is_empty(),
        "no immediate event on the latch (canon §9.4.7)"
    );
}

#[test]
fn closing_close_idempotent_joins_cohort() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Closing);
    c.test_set_pending_close_request_id(Some(1));
    c.handle_event(FsmEvent::CallClose { request_id: 2 });
    c.handle_event(FsmEvent::CallClose { request_id: 3 });
    assert_eq!(c.state(), ConnectionState::Closing, "idempotent self-loop");
    assert_eq!(
        c.test_additional_close_request_ids(),
        vec![2, 3],
        "2nd+ close() join the cohort"
    );
    assert_eq!(
        c.test_pending_close_request_id(),
        Some(1),
        "primary close rid unchanged"
    );
    assert!(
        c.test_bus().test_drain_published().is_empty(),
        "no event until Closing→Closed"
    );
}

// A close() arriving in a Closing reached via force_reconnect (no primary close)
// adopts itself as primary so it still receives a ConnectionClosedEvent.
#[test]
fn closing_close_adopts_primary_when_no_prior_close() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Closing);
    c.handle_event(FsmEvent::CallClose { request_id: 5 });
    assert_eq!(
        c.test_pending_close_request_id(),
        Some(5),
        "adopted as primary (no prior close)"
    );
    assert!(c.test_additional_close_request_ids().is_empty());
}

// every close waiter (primary + cohort) gets its OWN correlated
// ConnectionClosedEvent on the single physical Closing→Closed.
#[test]
fn closing_to_closed_emits_one_event_per_close_waiter() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Closing);
    c.test_set_pending_close_request_id(Some(1));
    c.handle_event(FsmEvent::CallClose { request_id: 2 });
    c.handle_event(FsmEvent::CallClose { request_id: 3 });
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1000,
        reason: None,
    });
    assert_eq!(c.state(), ConnectionState::Closed);
    let closed_rids: Vec<_> = c
        .test_bus()
        .test_drain_published()
        .iter()
        .filter(|e| e.event_type == crate::dispatch::EventType::ConnectionClosedEvent)
        .map(|e| e.request_id)
        .collect();
    assert_eq!(
        closed_rids,
        vec![Some(1), Some(2), Some(3)],
        "one correlated ConnectionClosedEvent per close waiter (D1)"
    );
}

// pending close + latched reconnect → ConnectionClosedEvent first (resolves the close), then
// re-enter Connecting stamped with the reconnect rid.
#[test]
fn closing_to_reconnect_emits_closed_then_connecting() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Closing);
    c.test_set_pending_close_request_id(Some(1));
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 9 });
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1000,
        reason: None,
    });
    assert_eq!(
        c.state(),
        ConnectionState::Connecting,
        "re-entered Connecting via the Idle→Connecting entry-actions"
    );
    assert!(c.socket().is_some(), "fresh socket opened on re-entry");
    let seq: Vec<_> = c
        .test_bus()
        .test_drain_published()
        .iter()
        .map(|e| (e.event_type, e.request_id))
        .collect();
    assert_eq!(
        seq,
        vec![
            (crate::dispatch::EventType::ConnectionClosedEvent, Some(1)),
            (
                crate::dispatch::EventType::ConnectionConnectingEvent,
                Some(9)
            ),
        ],
        "D2: Closed (close waiter) then Connecting (reconnect rid), in that order"
    );
}

// A pure force_reconnect (no pending close) emits NO ConnectionClosedEvent —
// just ConnectionConnectingEvent once Closing completes.
#[test]
fn pure_force_reconnect_from_closing_emits_no_closed_event() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Closing);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 9 });
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1000,
        reason: None,
    });
    assert_eq!(c.state(), ConnectionState::Connecting);
    let events = c.test_bus().test_drain_published();
    assert!(
        events
            .iter()
            .all(|e| e.event_type != crate::dispatch::EventType::ConnectionClosedEvent),
        "pure force_reconnect emits no ConnectionClosedEvent"
    );
    let connecting: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == crate::dispatch::EventType::ConnectionConnectingEvent)
        .map(|e| e.request_id)
        .collect();
    assert_eq!(
        connecting,
        vec![Some(9)],
        "ConnectionConnectingEvent stamped with the reconnect rid"
    );
}

// reenter_connecting is budget-gated: force_reconnect is NOT a bypass.
#[test]
fn closing_reconnect_budget_exhausted_lands_in_backing_off() {
    let budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::with_knobs(
        1,
        std::time::Duration::from_secs(60),
    ));
    budget
        .try_consume(crate::types::MonotonicInstant::now())
        .unwrap();
    let mut c = mc_with_budget(WsUrl::Public, budget);
    c.test_set_state(ConnectionState::Closing);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 9 });
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1000,
        reason: None,
    });
    assert_eq!(
        c.state(),
        ConnectionState::BackingOff,
        "throttled reconnect lands in BackingOff (force_reconnect is NOT a budget bypass)"
    );
    assert!(c.socket().is_none(), "no socket opened when throttled");
    assert!(c.timers.rate_budget_window_advanced_at.is_some());
}

#[test]
fn open_force_reconnect_latches_reconnect_and_closes() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Open);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 4 });
    assert_eq!(c.state(), ConnectionState::Closing);
    assert_eq!(
        c.test_pending_reconnect_request_id(),
        Some(4),
        "reconnect rid latched on (Open, force_reconnect)"
    );
}

#[test]
fn resubscribing_force_reconnect_latches_to_closing() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 8 });
    assert_eq!(c.state(), ConnectionState::Closing);
    assert_eq!(c.test_pending_reconnect_request_id(), Some(8));
}

// a 2nd concurrent force_reconnect while Closing joins the connect cohort (not
// last-write-wins) → enter_open emits one ConnectionReopenedEvent per member, so both awaits
#[test]
fn double_force_reconnect_while_closing_reopens_resolve_both() {
    let mut c = mc(WsUrl::Public);
    c.test_set_has_been_open(true);
    c.test_set_state(ConnectionState::Closing);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 77 });
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 88 });
    assert_eq!(c.test_pending_reconnect_request_id(), Some(77));
    assert_eq!(c.test_additional_connect_request_ids(), vec![88]);
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1000,
        reason: None,
    });
    assert_eq!(c.state(), ConnectionState::Connecting);
    c.handle_event(FsmEvent::WireUpgradeOk {
        connection_id: Some(3),
    });
    c.complete_resubscribe_if_empty();
    assert_eq!(c.state(), ConnectionState::Open);
    let reopened_rids: Vec<_> = c
        .test_bus()
        .test_drain_published()
        .iter()
        .filter(|e| e.event_type == crate::dispatch::EventType::ConnectionReopenedEvent)
        .map(|e| e.request_id)
        .collect();
    assert_eq!(
        reopened_rids,
        vec![Some(77), Some(88)],
        "both concurrent force_reconnect awaits resolve (one Reopened each)"
    );
}

// force_reconnect while Connecting during a reconnect (has_been_open) is
// cohorted so its await resolves on the eventual reopen.
#[test]
fn connecting_force_reconnect_during_reconnect_is_cohorted() {
    let mut c = mc(WsUrl::Public);
    c.test_set_has_been_open(true);
    c.test_set_state(ConnectionState::Connecting);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 55 });
    assert_eq!(
        c.test_additional_connect_request_ids(),
        vec![55],
        "reconnect-Connecting force_reconnect joins the cohort"
    );
}

// force_reconnect while Connecting on a first bring-up (never opened) is NOT
// cohorted — the eventual event is ConnectionOpenEvent, not Reopened.
#[test]
fn connecting_force_reconnect_first_bringup_not_cohorted() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Connecting);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 55 });
    assert!(
        c.test_additional_connect_request_ids().is_empty(),
        "first-bringup force_reconnect is not cohorted (documented edge)"
    );
}

// The connect cohort is CLEARED on terminal Failed, so a cohorted force_reconnect stranded by
// a failed reconnect leg does NOT later resolve with a spurious Ok(Reopened) on an unrelated
#[test]
fn force_reconnect_cohort_cleared_on_failed() {
    let mut c = mc(WsUrl::Public);
    c.test_set_has_been_open(true);
    c.test_set_state(ConnectionState::Closing);
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 77 });
    c.handle_event(FsmEvent::CallForceReconnect { request_id: 88 });
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1000,
        reason: None,
    });
    assert_eq!(c.state(), ConnectionState::Connecting);
    assert_eq!(c.test_additional_connect_request_ids(), vec![88]);
    c.handle_event(FsmEvent::WireUpgradeFailed { http_status: 401 });
    assert_eq!(c.state(), ConnectionState::Failed);
    assert!(
        c.test_additional_connect_request_ids().is_empty(),
        "cohort cleared on Failed — no spurious late Reopened on a future reopen"
    );
}

/// Helper: a `Knobs` with `reconnect_attempts` set to `Some(n)` (else default).
fn knobs_with_reconnect_cap(n: Option<u32>) -> crate::build::knobs::Knobs {
    let k = crate::build::knobs::Knobs::defaults();
    k.reconnect_attempts.swap(n);
    k
}

/// Helper: defaults with `backoff_jitter` set to `frac`.
fn knobs_with_jitter_frac(frac: f64) -> crate::build::knobs::Knobs {
    let mut k = crate::build::knobs::Knobs::defaults();
    k.backoff_jitter = frac;
    k
}

#[test]
fn next_backoff_ceiling_at_full_jitter_draw() {
    // At jitter draw r=1.0 the partial blend collapses to the ceiling for ANY
    // frac: delay == min(base * factor^attempt, max). Uses the default frac (1.0).
    let k = crate::build::knobs::Knobs::defaults(); // base=500, factor=2.0, max=30_000
    let j = FixedJitter(1.0);
    assert_eq!(
        next_backoff(&k, 0, &j),
        std::time::Duration::from_millis(500)
    );
    assert_eq!(
        next_backoff(&k, 1, &j),
        std::time::Duration::from_millis(1_000)
    );
    assert_eq!(
        next_backoff(&k, 5, &j),
        std::time::Duration::from_millis(16_000)
    );
    assert_eq!(
        next_backoff(&k, 6, &j),
        std::time::Duration::from_millis(30_000)
    );
    // A very high attempt also caps at max (no overflow / NaN).
    assert_eq!(
        next_backoff(&k, 1000, &j),
        std::time::Duration::from_millis(30_000)
    );
    assert_eq!(
        next_backoff(&k, u32::MAX, &j),
        std::time::Duration::from_millis(30_000)
    );
}

#[test]
fn next_backoff_frac_zero_is_deterministic_no_jitter() {
    let k = knobs_with_jitter_frac(0.0);
    for r in [0.0, 0.5, 1.0] {
        assert_eq!(
            next_backoff(&k, 0, &FixedJitter(r)),
            std::time::Duration::from_millis(500),
            "frac=0 must ignore jitter draw {r}"
        );
        assert_eq!(
            next_backoff(&k, 6, &FixedJitter(r)),
            std::time::Duration::from_millis(30_000),
            "frac=0 capped ceiling ignores jitter draw {r}"
        );
    }
}

#[test]
fn next_backoff_frac_half_blends_fixed_and_jitter() {
    // frac=0.5 (set explicitly; the default is now 1.0) → delay = computed * (0.5 + 0.5*r).
    let mut k = crate::build::knobs::Knobs::defaults();
    k.backoff_jitter = 0.5;
    assert_eq!(
        next_backoff(&k, 0, &FixedJitter(0.0)),
        std::time::Duration::from_millis(250)
    );
    assert_eq!(
        next_backoff(&k, 0, &FixedJitter(0.5)),
        std::time::Duration::from_millis(375)
    );
    assert_eq!(
        next_backoff(&k, 0, &FixedJitter(1.0)),
        std::time::Duration::from_millis(500)
    );
    // Cap is applied BEFORE jitter: attempt 6 → 30_000 * 0.5 = 15_000ms at r=0.
    assert_eq!(
        next_backoff(&k, 6, &FixedJitter(0.0)),
        std::time::Duration::from_millis(15_000)
    );
}

#[test]
fn next_backoff_frac_one_is_full_jitter_spanning_the_range() {
    // frac=1.0 → pure full-jitter: delay = computed * r (0 at r=0, ceiling at r→1).
    let k = knobs_with_jitter_frac(1.0);
    assert_eq!(
        next_backoff(&k, 0, &FixedJitter(0.0)),
        std::time::Duration::ZERO
    );
    assert_eq!(
        next_backoff(&k, 0, &FixedJitter(1.0)),
        std::time::Duration::from_millis(500)
    );
    // At r=0 full-jitter (0ms) differs from frac=0.0's fixed delay (500ms).
    assert_ne!(
        next_backoff(&k, 0, &FixedJitter(0.0)),
        next_backoff(&knobs_with_jitter_frac(0.0), 0, &FixedJitter(0.0)),
    );
}

#[test]
fn next_backoff_clamps_out_of_range_and_non_finite_frac() {
    // frac>1.0 clamps to 1.0 → at r=0.5: computed*0.5 = 250ms (unclamped 5.0 would give 0).
    assert_eq!(
        next_backoff(&knobs_with_jitter_frac(5.0), 0, &FixedJitter(0.5)),
        std::time::Duration::from_millis(250)
    );
    // frac<0.0 clamps to 0.0 (fixed) → at r=0: computed = 500ms
    // (unclamped -3.0 → computed*4 = 2000, so this pins the clamp).
    assert_eq!(
        next_backoff(&knobs_with_jitter_frac(-3.0), 0, &FixedJitter(0.0)),
        std::time::Duration::from_millis(500)
    );
    // NaN → full-jitter fallback (frac→1.0) → at r=1.0: computed = 500ms
    // (an unhandled NaN → computed*NaN → 0, so this pins the fallback).
    assert_eq!(
        next_backoff(&knobs_with_jitter_frac(f64::NAN), 0, &FixedJitter(1.0)),
        std::time::Duration::from_millis(500)
    );
}

#[test]
fn next_backoff_floors_sub_unit_negative_and_non_finite_factor() {
    // A negative or sub-1 factor would drive computed negative → 0ms delay → reconnect storm.
    let mut k = crate::build::knobs::Knobs::defaults();
    k.backoff_jitter = 0.0;
    for bad in [-2.0, 0.0, 0.5] {
        k.backoff_factor = bad;
        for attempt in [0, 1, 2, 5] {
            assert_eq!(
                next_backoff(&k, attempt, &FixedJitter(1.0)),
                std::time::Duration::from_millis(500),
                "factor {bad} must floor to 1.0 (constant 500ms), not storm at 0ms; attempt {attempt}"
            );
        }
    }
    // A non-finite factor falls back to the 2.0 default growth.
    k.backoff_factor = f64::NAN;
    assert_eq!(
        next_backoff(&k, 1, &FixedJitter(1.0)),
        std::time::Duration::from_millis(1_000),
        "non-finite factor must fall back to 2.0 growth"
    );
}

#[test]
fn compute_backoff_due_attempt_count_one_is_500ms_ceiling_anchor() {
    // attempt_count == 1 → attempt index = 0 → factor^0 → base 500ms.
    // With FixedJitter(1.0) the due-at is now + 500ms.
    let k = crate::build::knobs::Knobs::defaults();
    let mut c = mc_with_knobs_clock_jitter(
        WsUrl::Public,
        k,
        /* now_secs */ 100,
        Arc::new(FixedJitter(1.0)),
    );
    c.test_set_state(ConnectionState::Connecting);
    c.test_set_attempt_count(1);
    c.handle_event(FsmEvent::WireConnectError {
        kind: crate::transport::TransportErrorKind::TcpRefused,
        transient: true,
    });
    assert_eq!(c.state(), ConnectionState::BackingOff);
    let due = c.timers.backoff_due_at.expect("backoff armed");
    assert_eq!(
        due,
        crate::types::MonotonicInstant(
            std::time::Duration::from_secs(100) + std::time::Duration::from_millis(500)
        )
    );
}

#[test]
fn reconnect_cap_some_n_escalates_to_failed_on_nth_transient() {
    // Some(3): 3 total attempts THEN escalate.
    let mut c = mc_with_knobs_clock_jitter(
        WsUrl::Auth,
        knobs_with_reconnect_cap(Some(3)),
        100,
        Arc::new(FixedJitter(0.0)),
    );
    for n in [1u32, 2u32] {
        c.test_set_state(ConnectionState::Connecting);
        c.test_set_attempt_count(n);
        c.handle_event(FsmEvent::WireConnectError {
            kind: crate::transport::TransportErrorKind::TcpRefused,
            transient: true,
        });
        assert_eq!(
            c.state(),
            ConnectionState::BackingOff,
            "n={n} below cap stays BackingOff"
        );
    }
    // attempt_count 3 → escalate to Failed.
    c.timers_mut().backoff_due_at = None;
    c.test_set_state(ConnectionState::Connecting);
    c.test_set_attempt_count(3);
    c.handle_event(FsmEvent::WireConnectError {
        kind: crate::transport::TransportErrorKind::TcpRefused,
        transient: true,
    });
    assert_eq!(c.state(), ConnectionState::Failed, "Nth attempt escalates");
    assert!(
        c.timers.backoff_due_at.is_none(),
        "escalation does not arm backoff"
    );
    assert!(c.socket().is_none());
}

#[tokio::test]
async fn reconnect_cap_escalation_emits_connection_failed_event() {
    use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig, EventPayload, EventType};
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let mut bus = DispatchEventBus::new(DispatchEventBusConfig::defaults(), Arc::clone(&clock));
    bus.set_knobs(Arc::new(knobs_with_reconnect_cap(Some(2))));
    let bus = Arc::new(bus);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx_cell = std::sync::Mutex::new(Some(tx));
    let _ = bus.subscribe(
        EventType::ConnectionFailedEvent,
        Arc::new(move |env: &crate::dispatch::EventEnvelope| {
            if let Some(tx) = tx_cell.lock().unwrap().take() {
                let _ = tx.send(env.clone());
            }
        }),
        u16::MAX,
    );
    bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
    let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
        Arc::new(crate::transport::MockWsSocketFactory::new());
    let rate_budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
    let mut c =
        ManagedConnection::new(WsUrl::Auth, bus, factory, rate_budget, clock, test_jitter());

    c.test_set_state(ConnectionState::Connecting);
    c.test_set_attempt_count(2);
    c.handle_event(FsmEvent::WireConnectError {
        kind: crate::transport::TransportErrorKind::TcpRefused,
        transient: true,
    });
    assert_eq!(c.state(), ConnectionState::Failed);

    let env = tokio::time::timeout(std::time::Duration::from_millis(200), rx)
        .await
        .expect("ConnectionFailedEvent not delivered")
        .expect("oneshot sender dropped");
    match env.payload {
        EventPayload::ConnectionFailedEvent {
            non_transient_class,
            attempt_count,
            transient,
            ..
        } => {
            assert_eq!(
                non_transient_class,
                Some(crate::dispatch::NonTransientClass::RetryCapExhausted)
            );
            assert_eq!(attempt_count, 2);
            assert!(!transient);
        }
        other => panic!("unexpected payload {other:?}"),
    }
}

#[tokio::test]
async fn caller_initiated_connect_failure_correlates_connection_failed_event() {
    // A caller-initiated connect (CallStartConnect{request_id}) that escalates to Failed must
    // emit ConnectionFailedEvent stamped with that request_id (so a correlated awaiter
    use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig, EventPayload, EventType};
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let mut bus = DispatchEventBus::new(DispatchEventBusConfig::defaults(), Arc::clone(&clock));
    bus.set_knobs(Arc::new(knobs_with_reconnect_cap(Some(2))));
    let bus = Arc::new(bus);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx_cell = std::sync::Mutex::new(Some(tx));
    let _ = bus.subscribe(
        EventType::ConnectionFailedEvent,
        Arc::new(move |env: &crate::dispatch::EventEnvelope| {
            if let Some(tx) = tx_cell.lock().unwrap().take() {
                let _ = tx.send(env.clone());
            }
        }),
        u16::MAX,
    );
    bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
    let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
        Arc::new(crate::transport::MockWsSocketFactory::new());
    let rate_budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
    let mut c =
        ManagedConnection::new(WsUrl::Auth, bus, factory, rate_budget, clock, test_jitter());

    const RID: u64 = 4242;
    c.handle_event(FsmEvent::CallStartConnect { request_id: RID });
    assert_eq!(c.state(), ConnectionState::Connecting);
    c.test_set_attempt_count(2);
    c.handle_event(FsmEvent::WireConnectError {
        kind: crate::transport::TransportErrorKind::TcpRefused,
        transient: true,
    });
    assert_eq!(c.state(), ConnectionState::Failed);

    let env = tokio::time::timeout(std::time::Duration::from_millis(200), rx)
        .await
        .expect("ConnectionFailedEvent not delivered")
        .expect("oneshot sender dropped");
    assert_eq!(
        env.request_id,
        Some(RID),
        "caller-initiated connect failure must correlate ConnectionFailedEvent to the connect request_id"
    );
    assert!(matches!(
        env.payload,
        EventPayload::ConnectionFailedEvent { .. }
    ));
}

#[test]
fn reconnect_cap_none_never_escalates() {
    let mut c = mc_with_knobs_clock_jitter(
        WsUrl::Auth,
        knobs_with_reconnect_cap(None),
        100,
        Arc::new(FixedJitter(0.0)),
    );
    c.test_set_state(ConnectionState::Connecting);
    c.test_set_attempt_count(10_000);
    c.handle_event(FsmEvent::WireConnectError {
        kind: crate::transport::TransportErrorKind::TcpRefused,
        transient: true,
    });
    assert_eq!(
        c.state(),
        ConnectionState::BackingOff,
        "None cap never escalates"
    );
}

#[test]
fn reconnect_cap_does_not_affect_auth_m_cap_path() {
    // The token-stale auth M-cap arm has its OWN cap (max_auth_handshake_failures) and goes
    // straight to Failed on M; the reconnect_attempts cap must NOT gate it (no
    let mut c = mc_with_knobs_clock_jitter(
        WsUrl::Auth,
        knobs_with_reconnect_cap(Some(50)),
        100,
        Arc::new(FixedJitter(0.0)),
    );
    c.test_set_state(ConnectionState::Authenticating);
    c.test_set_attempt_count(0);
    c.test_set_auth_handshake_fail_count(0);
    for _ in 0..2 {
        c.handle_event(FsmEvent::WireAuthHandshakeFailed {
            kind: AuthErrorKind::TokenStale,
        });
        assert_eq!(
            c.state(),
            ConnectionState::Authenticating,
            "below M-cap stays"
        );
        c.test_set_state(ConnectionState::Authenticating);
    }
    c.handle_event(FsmEvent::WireAuthHandshakeFailed {
        kind: AuthErrorKind::TokenStale,
    });
    assert_eq!(c.state(), ConnectionState::Failed, "M-cap escalates");
    assert_eq!(c.test_auth_handshake_fail_count(), 3);
    assert!(c.test_attempt_count() < 50);
}

#[test]
fn staleness_timer_armed_from_knob_on_enter_open() {
    let k = crate::build::knobs::Knobs::defaults(); // staleness 30_000ms default
    let mut c = mc_with_knobs_clock_jitter(WsUrl::Public, k, 100, test_jitter());
    c.test_set_state(ConnectionState::Resubscribing);
    c.handle_event(FsmEvent::WireSubscribeAck {
        channel: ChannelName::Ticker,
        pair: None,
        last: true,
    });
    assert_eq!(c.state(), ConnectionState::Open);
    let due = c.timers.staleness_due_at.expect("staleness armed on Open");
    assert_eq!(
        due,
        crate::types::MonotonicInstant(
            std::time::Duration::from_secs(100) + std::time::Duration::from_millis(30_000)
        )
    );
}

#[test]
fn staleness_timer_window_reflects_custom_knob() {
    let mut k = crate::build::knobs::Knobs::defaults();
    k.staleness_window_ms = 12_345;
    let mut c = mc_with_knobs_clock_jitter(WsUrl::Public, k, 100, test_jitter());
    c.test_set_state(ConnectionState::Resubscribing);
    c.handle_event(FsmEvent::WireSubscribeAck {
        channel: ChannelName::Ticker,
        pair: None,
        last: true,
    });
    let due = c.timers.staleness_due_at.expect("staleness armed");
    assert_eq!(
        due,
        crate::types::MonotonicInstant(
            std::time::Duration::from_secs(100) + std::time::Duration::from_millis(12_345)
        )
    );
}

#[test]
fn upgrade_timeout_armed_from_knob_on_connect() {
    let mut k = crate::build::knobs::Knobs::defaults();
    k.upgrade_timeout_ms = 7_777;
    let mut c = mc_with_knobs_clock_jitter(WsUrl::Public, k, 100, test_jitter());
    c.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Connecting);
    let due = c
        .timers
        .upgrade_timeout_due_at
        .expect("upgrade timeout armed");
    assert_eq!(
        due,
        crate::types::MonotonicInstant(
            std::time::Duration::from_secs(100) + std::time::Duration::from_millis(7_777)
        )
    );
}

#[test]
fn close_timeout_armed_from_knob_on_closing_entry() {
    let mut k = crate::build::knobs::Knobs::defaults();
    k.close_timeout_ms = 3_333;
    let mut c = mc_with_knobs_clock_jitter(WsUrl::Public, k, 100, test_jitter());
    c.test_set_state(ConnectionState::Open);
    c.handle_event(FsmEvent::CallClose { request_id: 9 });
    assert_eq!(c.state(), ConnectionState::Closing);
    let due = c
        .timers
        .close_timeout_due_at
        .expect("close timeout armed on Closing");
    assert_eq!(
        due,
        crate::types::MonotonicInstant(
            std::time::Duration::from_secs(100) + std::time::Duration::from_millis(3_333)
        )
    );
}

#[test]
fn path_c_open_entry_arms_staleness() {
    // Path C bare-order self-auth (Authenticating → Open via WireOrderAuthOk) routes through
    // enter_open, which arms staleness on Open entry — one of three arm sites {Authenticating,
    let mut k = crate::build::knobs::Knobs::defaults();
    k.staleness_window_ms = 20_000;
    let mut c = mc_with_knobs_clock_jitter(WsUrl::Auth, k, 100, test_jitter());
    c.test_set_state(ConnectionState::Authenticating);
    c.handle_event(FsmEvent::WireOrderAuthOk);
    assert_eq!(c.state(), ConnectionState::Open, "Path C → Open directly");
    let due = c
        .timers
        .staleness_due_at
        .expect("staleness armed via enter_open on Path C");
    assert_eq!(
        due,
        crate::types::MonotonicInstant(
            std::time::Duration::from_secs(100) + std::time::Duration::from_millis(20_000)
        )
    );
}

#[test]
fn connecting_upgrade_ok_arms_staleness_before_open() {
    let mut k = crate::build::knobs::Knobs::defaults();
    k.staleness_window_ms = 25_000;
    let mut c = mc_with_knobs_clock_jitter(WsUrl::Public, k, 100, test_jitter());
    c.test_set_state(ConnectionState::Connecting);
    c.handle_event(FsmEvent::WireUpgradeOk {
        connection_id: Some(7),
    });
    assert_eq!(c.state(), ConnectionState::Resubscribing);
    let due = c
        .timers
        .staleness_due_at
        .expect("staleness armed on entry to Resubscribing");
    assert_eq!(
        due,
        crate::types::MonotonicInstant(
            std::time::Duration::from_secs(100) + std::time::Duration::from_millis(25_000)
        )
    );
}

#[test]
fn authenticating_entry_arms_staleness_before_open() {
    let mut c = mc_with_knobs_clock_jitter(
        WsUrl::Auth,
        crate::build::knobs::Knobs::defaults(),
        100,
        test_jitter(),
    );
    c.test_set_state(ConnectionState::Connecting);
    c.handle_event(FsmEvent::WireUpgradeOk {
        connection_id: None,
    });
    assert_eq!(c.state(), ConnectionState::Authenticating);
    assert!(
        c.timers.staleness_due_at.is_some(),
        "staleness armed on entry to Authenticating"
    );
}

#[test]
fn staleness_disarmed_on_closing_exit_table() {
    // Each row reaches a staleness-armed state via entry events, then a trigger exits to
    // Closing and must disarm staleness.
    type Case = (
        &'static str,
        WsUrl,
        ConnectionState,
        Vec<FsmEvent>,
        ConnectionState,
        FsmEvent,
    );
    let cases: Vec<Case> = vec![
        (
            "open_call_close",
            WsUrl::Public,
            ConnectionState::Resubscribing,
            vec![FsmEvent::WireSubscribeAck {
                channel: ChannelName::Ticker,
                pair: None,
                last: true,
            }],
            ConnectionState::Open,
            FsmEvent::CallClose { request_id: 1 },
        ),
        (
            "open_force_reconnect",
            WsUrl::Public,
            ConnectionState::Resubscribing,
            vec![FsmEvent::WireSubscribeAck {
                channel: ChannelName::Ticker,
                pair: None,
                last: true,
            }],
            ConnectionState::Open,
            FsmEvent::CallForceReconnect { request_id: 1 },
        ),
        (
            "authenticating_call_close",
            WsUrl::Auth,
            ConnectionState::Connecting,
            vec![FsmEvent::WireUpgradeOk {
                connection_id: None,
            }],
            ConnectionState::Authenticating,
            FsmEvent::CallClose { request_id: 1 },
        ),
        (
            "authenticating_force_reconnect",
            WsUrl::Auth,
            ConnectionState::Connecting,
            vec![FsmEvent::WireUpgradeOk {
                connection_id: None,
            }],
            ConnectionState::Authenticating,
            FsmEvent::CallForceReconnect { request_id: 1 },
        ),
        (
            "resubscribing_call_close",
            WsUrl::Public,
            ConnectionState::Connecting,
            vec![FsmEvent::WireUpgradeOk {
                connection_id: Some(7),
            }],
            ConnectionState::Resubscribing,
            FsmEvent::CallClose { request_id: 1 },
        ),
    ];
    for (label, url, start, entry, pre_exit, trigger) in cases {
        let mut c = mc_with_knobs_clock_jitter(
            url,
            crate::build::knobs::Knobs::defaults(),
            100,
            test_jitter(),
        );
        c.test_set_state(start);
        for ev in entry {
            c.handle_event(ev);
        }
        assert_eq!(
            c.state(),
            pre_exit,
            "{label}: reached the armed pre-exit state"
        );
        assert!(
            c.timers.staleness_due_at.is_some(),
            "{label}: staleness armed before exit"
        );
        c.handle_event(trigger);
        assert_eq!(
            c.state(),
            ConnectionState::Closing,
            "{label}: exits to Closing"
        );
        assert!(
            c.timers.staleness_due_at.is_none(),
            "{label}: staleness disarmed on the Closing exit"
        );
    }
}

#[test]
fn resubscribing_call_close_clears_subscribe_ack_timers() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    let sub = ack_key();
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_subscribe_ack(sub.0, sub.1.clone(), due);
    assert!(
        c.timers.per_entry_subscribe_ack_timeouts.contains_key(&sub),
        "armed pre-close"
    );
    assert!(
        c.subscribe_ack_attempts.contains_key(&sub),
        "attempts seeded pre-close"
    );

    c.handle_event(FsmEvent::CallClose { request_id: 1 });

    assert_eq!(c.state(), ConnectionState::Closing);
    assert!(
        c.timers.per_entry_subscribe_ack_timeouts.is_empty(),
        "subscribe-ack timers must be cancelled on Resubscribing → Closing"
    );
    assert!(
        c.timers.per_entry_subscribe_resend_due.is_empty(),
        "resend timers must be cancelled on Resubscribing → Closing"
    );
    assert!(
        c.subscribe_ack_attempts.is_empty(),
        "ack-attempt budget must be cleared on Resubscribing → Closing"
    );
}

#[test]
fn open_staleness_clears_subscribe_ack_timers() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Open);
    let sub = ack_key();
    let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
    c.arm_subscribe_ack(sub.0, sub.1.clone(), due);
    assert!(
        c.timers.per_entry_subscribe_ack_timeouts.contains_key(&sub),
        "armed pre-staleness"
    );
    assert!(
        c.subscribe_ack_attempts.contains_key(&sub),
        "attempts seeded pre-staleness"
    );

    c.handle_event(FsmEvent::TimerStalenessElapsed);

    assert_eq!(c.state(), ConnectionState::BackingOff);
    assert!(
        c.timers.per_entry_subscribe_ack_timeouts.is_empty(),
        "subscribe-ack timers must be cancelled on Open → staleness teardown"
    );
    assert!(
        c.timers.per_entry_subscribe_resend_due.is_empty(),
        "resend timers must be cancelled on Open → staleness teardown"
    );
    assert!(
        c.subscribe_ack_attempts.is_empty(),
        "ack-attempt budget must be cleared on Open → staleness teardown"
    );
}

/// `(Open, TimerStalenessElapsed)` emits WebsocketStaleEvent THEN
/// ConnectionDroppedEvent on the BackingOff path.
#[test]
fn open_staleness_emits_websocket_stale_then_dropped() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Open);
    c.handle_event(FsmEvent::TimerStalenessElapsed);
    assert_eq!(
        c.state(),
        ConnectionState::BackingOff,
        "Open + TimerStalenessElapsed must transition to BackingOff"
    );

    let events = c.test_bus().test_drain_published();
    let types: Vec<crate::dispatch::EventType> = events.iter().map(|e| e.event_type).collect();

    let stale_pos = types
        .iter()
        .position(|&t| t == crate::dispatch::EventType::WebsocketStaleEvent)
        .expect("WebsocketStaleEvent must be emitted on (Open, TimerStalenessElapsed)");
    let dropped_pos = types.iter().position(
        |&t| t == crate::dispatch::EventType::ConnectionDroppedEvent
    ).expect("ConnectionDroppedEvent must be emitted on (Open, TimerStalenessElapsed) BackingOff path");
    assert!(
        stale_pos < dropped_pos,
        "WebsocketStaleEvent (pos {stale_pos}) must precede ConnectionDroppedEvent (pos {dropped_pos})"
    );

    let stale_env = &events[stale_pos];
    assert_eq!(
        stale_env.event_version, 1,
        "WebsocketStaleEvent event_version must be 1"
    );
    assert_eq!(
        stale_env.request_id, None,
        "WebsocketStaleEvent must be broadcast (request_id None)"
    );
    assert!(
        matches!(
            stale_env.payload,
            crate::dispatch::EventPayload::WebsocketStaleEvent {
                configured_window_ms: 30_000,
                ..
            }
        ),
        "WebsocketStaleEvent payload must carry the default staleness window (30 000 ms)"
    );
}

/// `(Authenticating, TimerStalenessElapsed)` emits WebsocketStaleEvent before
/// ConnectionAttemptFailedEvent (pre-Open path); no ConnectionDroppedEvent.
#[test]
fn authenticating_staleness_emits_websocket_stale() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    c.handle_event(FsmEvent::TimerStalenessElapsed);
    assert_eq!(
        c.state(),
        ConnectionState::BackingOff,
        "Authenticating + TimerStalenessElapsed must transition to BackingOff"
    );

    let events = c.test_bus().test_drain_published();
    let types: Vec<crate::dispatch::EventType> = events.iter().map(|e| e.event_type).collect();

    let stale_pos = types
        .iter()
        .position(|&t| t == crate::dispatch::EventType::WebsocketStaleEvent)
        .expect("WebsocketStaleEvent must be emitted on (Authenticating, TimerStalenessElapsed)");

    let attempt_pos = types.iter().position(
        |&t| t == crate::dispatch::EventType::ConnectionAttemptFailedEvent
    ).expect("ConnectionAttemptFailedEvent must be emitted on (Authenticating, TimerStalenessElapsed)");

    assert!(
        stale_pos < attempt_pos,
        "WebsocketStaleEvent (pos {stale_pos}) must precede ConnectionAttemptFailedEvent (pos {attempt_pos})"
    );

    assert!(
        !types.contains(&crate::dispatch::EventType::ConnectionDroppedEvent),
        "ConnectionDroppedEvent must NOT be emitted on the pre-Open Authenticating staleness path"
    );

    let stale_env = &events[stale_pos];
    assert_eq!(stale_env.event_version, 1);
    assert_eq!(stale_env.request_id, None);
}

/// `(Resubscribing, TimerStalenessElapsed)` emits WebsocketStaleEvent before
/// ConnectionAttemptFailedEvent (pre-Open path); no ConnectionDroppedEvent.
#[test]
fn resubscribing_staleness_emits_websocket_stale() {
    let mut c = mc(WsUrl::Public);
    c.test_set_state(ConnectionState::Resubscribing);
    c.handle_event(FsmEvent::TimerStalenessElapsed);
    assert_eq!(
        c.state(),
        ConnectionState::BackingOff,
        "Resubscribing + TimerStalenessElapsed must transition to BackingOff"
    );

    let events = c.test_bus().test_drain_published();
    let types: Vec<crate::dispatch::EventType> = events.iter().map(|e| e.event_type).collect();

    let stale_pos = types
        .iter()
        .position(|&t| t == crate::dispatch::EventType::WebsocketStaleEvent)
        .expect("WebsocketStaleEvent must be emitted on (Resubscribing, TimerStalenessElapsed)");

    let attempt_pos = types.iter().position(
        |&t| t == crate::dispatch::EventType::ConnectionAttemptFailedEvent
    ).expect("ConnectionAttemptFailedEvent must be emitted on (Resubscribing, TimerStalenessElapsed)");

    assert!(
        stale_pos < attempt_pos,
        "WebsocketStaleEvent (pos {stale_pos}) must precede ConnectionAttemptFailedEvent (pos {attempt_pos})"
    );

    assert!(
        !types.contains(&crate::dispatch::EventType::ConnectionDroppedEvent),
        "ConnectionDroppedEvent must NOT be emitted on the pre-Open Resubscribing staleness path"
    );

    let stale_env = &events[stale_pos];
    assert_eq!(stale_env.event_version, 1);
    assert_eq!(stale_env.request_id, None);
}

// WebsocketStaleEvent (the cause-marker) must be emitted in ALL staleness branches, not just
// BackingOff.
#[test]
fn staleness_cap_exhausted_emits_stale_then_failed_table() {
    // (label, url, start_state, assert_no_attempt_failed) The pre-Open
    // (Authenticating/Resubscribing) rows additionally assert the ConnectionAttemptFailedEvent
    let cases: Vec<(&str, WsUrl, ConnectionState, bool)> = vec![
        ("open", WsUrl::Public, ConnectionState::Open, false),
        (
            "authenticating",
            WsUrl::Auth,
            ConnectionState::Authenticating,
            true,
        ),
        (
            "resubscribing",
            WsUrl::Public,
            ConnectionState::Resubscribing,
            true,
        ),
    ];
    for (label, url, start, assert_no_attempt) in cases {
        let mut c =
            mc_with_knobs_clock_jitter(url, knobs_with_reconnect_cap(Some(2)), 100, test_jitter());
        c.test_set_state(start);
        c.test_set_attempt_count(2);
        c.handle_event(FsmEvent::TimerStalenessElapsed);
        assert_eq!(
            c.state(),
            ConnectionState::Failed,
            "{label}: staleness with attempt_count >= reconnect cap must escalate to Failed"
        );

        let events = c.test_bus().test_drain_published();
        let types: Vec<crate::dispatch::EventType> = events.iter().map(|e| e.event_type).collect();
        let stale_pos = types
            .iter()
            .position(|&t| t == crate::dispatch::EventType::WebsocketStaleEvent)
            .unwrap_or_else(|| {
                panic!(
                    "{label}: WebsocketStaleEvent MUST fire even on the cap-exhausted Failed path"
                )
            });
        let failed_pos = types
            .iter()
            .position(|&t| t == crate::dispatch::EventType::ConnectionFailedEvent)
            .unwrap_or_else(|| {
                panic!("{label}: ConnectionFailedEvent must be emitted on cap exhaustion")
            });
        assert!(
            stale_pos < failed_pos,
            "{label}: WebsocketStaleEvent (pos {stale_pos}) must precede ConnectionFailedEvent (pos {failed_pos})"
        );
        assert!(
            !types.contains(&crate::dispatch::EventType::ConnectionDroppedEvent),
            "{label}: no ConnectionDroppedEvent on the Failed escalation path"
        );
        assert_eq!(
            events[stale_pos].event_version, 1,
            "{label}: stale event_version 1"
        );
        assert_eq!(
            events[stale_pos].request_id, None,
            "{label}: stale event is broadcast (request_id None)"
        );
        if assert_no_attempt {
            assert!(
                !types.contains(&crate::dispatch::EventType::ConnectionAttemptFailedEvent),
                "{label}: the cap-exhausted path escalates to Failed; the pre-Open ConnectionAttemptFailedEvent is skipped"
            );
        }
    }
}

// Any Authenticating-exit that tears down the socket without being an Open-exit must drain the
// pending-request map, else a bare order's oneshot sender leaks and the caller's recv().await

/// Build a fake `PendingRequest` + return its receiver.
fn fake_pending(
    req_id: u64,
) -> (
    PendingRequest,
    tokio::sync::oneshot::Receiver<Result<WsResponse, crate::error::ConnectionError>>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let pending = PendingRequest {
        req_id,
        op: crate::dispatch::WsOp::AddOrder,
        sent_at: crate::types::MonotonicInstant::now(),
        tx,
    };
    (pending, rx)
}

/// Assert a `WsRequestFailedEvent` is present in `events` for the given
/// `req_id` with `WsFailReason::ConnectionLost`.
fn assert_ws_request_failed_published(events: &[crate::dispatch::EventEnvelope], req_id: u64) {
    let found = events.iter().any(|e| {
        e.event_type == crate::dispatch::EventType::WsRequestFailedEvent
            && matches!(
                &e.payload,
                crate::dispatch::EventPayload::WsRequestFailedEvent {
                    req_id: r,
                    reason: crate::dispatch::WsFailReason::ConnectionLost,
                    ..
                } if *r == req_id
            )
    });
    assert!(
        found,
        "expected WsRequestFailedEvent(req_id={req_id}, ConnectionLost) in published events"
    );
}

/// Assert a drained pending request resolved its oneshot to `Err` — the caller `await` returns
/// instead of hanging.
fn assert_pending_drained_err(
    rx: &mut tokio::sync::oneshot::Receiver<Result<WsResponse, crate::error::ConnectionError>>,
) {
    match rx.try_recv() {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("expected Err from drained pending, got Ok"),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
            panic!("oneshot not resolved — caller would hang forever (the bug)")
        }
        Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
            panic!("sender dropped without sending")
        }
    }
}

#[test]
fn authenticating_abnormal_close_drains_pending_request() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    let (pending, mut rx) = fake_pending(42);
    c.record_pending_request(pending);
    assert_eq!(
        c.pending_request_count(),
        1,
        "precondition: pending recorded"
    );

    c.handle_event(FsmEvent::WireAbnormalClose {
        error: crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        },
    });

    assert_eq!(
        c.pending_request_count(),
        0,
        "pending_request_count must be 0 after (Authenticating, WireAbnormalClose)"
    );

    let events = c.test_bus().test_drain_published();
    assert_ws_request_failed_published(&events, 42);

    assert_pending_drained_err(&mut rx);
}

#[test]
fn authenticating_call_close_drains_pending_request() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    let (pending, mut rx) = fake_pending(42);
    c.record_pending_request(pending);
    assert_eq!(
        c.pending_request_count(),
        1,
        "precondition: pending recorded"
    );

    c.handle_event(FsmEvent::CallClose { request_id: 9 });
    assert_eq!(c.state(), ConnectionState::Closing);
    c.handle_event(FsmEvent::TimerCloseTimeout);
    assert_eq!(c.state(), ConnectionState::Closed);

    assert_eq!(
        c.pending_request_count(),
        0,
        "pending must be drained on the Authenticating→Closing→Closed exit"
    );

    // WsRequestFailedEvent(ClientClosed) — a client close is non-retryable.
    let events = c.test_bus().test_drain_published();
    let found = events.iter().any(|e| {
        matches!(
            &e.payload,
            crate::dispatch::EventPayload::WsRequestFailedEvent {
                req_id: 42,
                reason: crate::dispatch::WsFailReason::ClientClosed,
                ..
            }
        )
    });
    assert!(
        found,
        "expected WsRequestFailedEvent(req_id=42, ClientClosed)"
    );

    assert_pending_drained_err(&mut rx);
}

#[test]
fn authenticating_call_force_reconnect_drains_pending_request() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    let (pending, mut rx) = fake_pending(43);
    c.record_pending_request(pending);
    assert_eq!(
        c.pending_request_count(),
        1,
        "precondition: pending recorded"
    );

    c.handle_event(FsmEvent::CallForceReconnect { request_id: 9 });
    assert_eq!(c.state(), ConnectionState::Closing);
    c.handle_event(FsmEvent::WireCloseReceived {
        code: 1000,
        reason: None,
    });

    assert_eq!(
        c.pending_request_count(),
        0,
        "pending must be drained on the Authenticating→Closing exit before reconnect"
    );

    // WsRequestFailedEvent(ConnectionLost, retryable) — force-reconnect is transient.
    let events = c.test_bus().test_drain_published();
    assert_ws_request_failed_published(&events, 43);

    assert_pending_drained_err(&mut rx);
}

#[test]
fn authenticating_staleness_drains_pending_request() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    let (pending, mut rx) = fake_pending(99);
    c.record_pending_request(pending);
    assert_eq!(
        c.pending_request_count(),
        1,
        "precondition: pending recorded"
    );

    c.handle_event(FsmEvent::TimerStalenessElapsed);
    assert_eq!(
        c.state(),
        ConnectionState::BackingOff,
        "Authenticating + TimerStalenessElapsed must transition to BackingOff"
    );

    assert_eq!(
        c.pending_request_count(),
        0,
        "pending_request_count must be 0 after (Authenticating, TimerStalenessElapsed)"
    );

    let events = c.test_bus().test_drain_published();
    assert_ws_request_failed_published(&events, 99);

    assert_pending_drained_err(&mut rx);

    // Ordering: WebsocketStaleEvent → WsRequestFailedEvent → ConnectionAttemptFailedEvent.
    let types: Vec<crate::dispatch::EventType> = events.iter().map(|e| e.event_type).collect();
    let stale_pos = types
        .iter()
        .position(|&t| t == crate::dispatch::EventType::WebsocketStaleEvent)
        .expect("WebsocketStaleEvent must be emitted");
    let ws_fail_pos = types
        .iter()
        .position(|&t| t == crate::dispatch::EventType::WsRequestFailedEvent)
        .expect("WsRequestFailedEvent must be emitted");
    let attempt_pos = types
        .iter()
        .position(|&t| t == crate::dispatch::EventType::ConnectionAttemptFailedEvent)
        .expect("ConnectionAttemptFailedEvent must be emitted on the BackingOff path");
    assert!(
        stale_pos < ws_fail_pos,
        "WebsocketStaleEvent (pos {stale_pos}) must precede WsRequestFailedEvent (pos {ws_fail_pos})"
    );
    assert!(
        ws_fail_pos < attempt_pos,
        "WsRequestFailedEvent (pos {ws_fail_pos}) must precede ConnectionAttemptFailedEvent (pos {attempt_pos})"
    );
}

#[test]
fn authenticating_bus_token_refresh_failed_drains_pending_request() {
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    let (pending, mut rx) = fake_pending(7);
    c.record_pending_request(pending);
    assert_eq!(
        c.pending_request_count(),
        1,
        "precondition: pending recorded"
    );

    c.handle_event(FsmEvent::BusTokenRefreshFailed {
        request_id: 1,
        error: crate::auth::AuthError::TokenRefreshFailed,
    });
    assert_eq!(
        c.state(),
        ConnectionState::Failed,
        "BusTokenRefreshFailed must transition Authenticating → Failed"
    );

    assert_eq!(
        c.pending_request_count(),
        0,
        "pending_request_count must be 0 after (Authenticating, BusTokenRefreshFailed → Failed)"
    );

    let events = c.test_bus().test_drain_published();
    assert_ws_request_failed_published(&events, 7);

    assert_pending_drained_err(&mut rx);

    // Drain-before-emit: WsRequestFailedEvent must precede ConnectionFailedEvent.
    let types: Vec<crate::dispatch::EventType> = events.iter().map(|e| e.event_type).collect();
    let ws_fail_pos = types
        .iter()
        .position(|&t| t == crate::dispatch::EventType::WsRequestFailedEvent)
        .expect("WsRequestFailedEvent must be emitted");
    let conn_fail_pos = types
        .iter()
        .position(|&t| t == crate::dispatch::EventType::ConnectionFailedEvent)
        .expect("ConnectionFailedEvent must be emitted on → Failed");
    assert!(
        ws_fail_pos < conn_fail_pos,
        "WsRequestFailedEvent (pos {ws_fail_pos}) must precede ConnectionFailedEvent (pos {conn_fail_pos}) — drain-before-emit"
    );
}

#[test]
fn authenticating_transient_handshake_failure_drains_pending_request() {
    // the (Authenticating, WireAuthHandshakeFailed{Transient}) arm tears down the socket and
    // must drain like every other Authenticating-exit arm, so an in-flight request can't leak
    let mut c = mc(WsUrl::Auth);
    c.test_set_state(ConnectionState::Authenticating);
    let (pending, mut rx) = fake_pending(11);
    c.record_pending_request(pending);
    assert_eq!(
        c.pending_request_count(),
        1,
        "precondition: pending recorded"
    );

    c.handle_event(FsmEvent::WireAuthHandshakeFailed {
        kind: crate::conn::AuthErrorKind::Transient,
    });
    assert_eq!(
        c.state(),
        ConnectionState::BackingOff,
        "transient auth-handshake failure (no cap) → BackingOff"
    );
    assert_eq!(
        c.pending_request_count(),
        0,
        "pending must be drained on the transient Authenticating-exit arm"
    );

    let events = c.test_bus().test_drain_published();
    assert_ws_request_failed_published(&events, 11);

    assert_pending_drained_err(&mut rx);
}

// Emitted event timestamps (envelope timestamp_monotonic + payload *_at_monotonic) must come
// from the injected clock, not MonotonicInstant::now().

/// FixedClock pinned at `now_secs`, driven through Connecting → BackingOff (transient upgrade
/// failure) so `emit_attempt_failed` fires; asserts the envelope + payload timestamps both
/// equal the injected FixedClock instant.
#[test]
fn m5_emitted_event_timestamps_use_injected_clock_not_real_clock() {
    const NOW_SECS: u64 = 12_345;
    let fixed_instant = crate::types::MonotonicInstant(std::time::Duration::from_secs(NOW_SECS));

    let mut c = mc_with_knobs_clock_jitter(
        WsUrl::Public,
        crate::build::knobs::Knobs::defaults(),
        NOW_SECS,
        Arc::new(FixedJitter(1.0)), // full-delay jitter so backoff_due is deterministic
    );

    c.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
    assert_eq!(c.state(), ConnectionState::Connecting);

    let _ = c.test_bus().test_drain_published();

    c.handle_event(FsmEvent::WireUpgradeFailed { http_status: 503 });
    assert_eq!(
        c.state(),
        ConnectionState::BackingOff,
        "transient 503 → BackingOff"
    );

    let events = c.test_bus().test_drain_published();
    let attempt_failed = events
        .iter()
        .find(|e| e.event_type == crate::dispatch::EventType::ConnectionAttemptFailedEvent)
        .expect("ConnectionAttemptFailedEvent must be emitted on transient upgrade failure");

    assert_eq!(
        attempt_failed.timestamp_monotonic, fixed_instant,
        "M5 regression: envelope timestamp_monotonic must equal the injected FixedClock instant, \
         not the real process clock (pre-fix: emit_lifecycle used MonotonicInstant::now())"
    );

    match &attempt_failed.payload {
        crate::dispatch::EventPayload::ConnectionAttemptFailedEvent {
            failed_at_monotonic,
            ..
        } => {
            assert_eq!(
                *failed_at_monotonic, fixed_instant,
                "M5 regression: payload failed_at_monotonic must equal the injected FixedClock instant"
            );
        }
        other => panic!("unexpected payload variant: {:?}", other),
    }
}

#[test]
fn single_transition_fsm_table() {
    // Pure single-transition FSM cases: drive one event from a start state and
    // assert the resulting state. start_state None means a fresh Idle MC.
    let cases: Vec<(
        &str,
        WsUrl,
        Option<ConnectionState>,
        FsmEvent,
        ConnectionState,
    )> = vec![
        (
            "idle_close",
            WsUrl::Public,
            None,
            FsmEvent::CallClose { request_id: 1 },
            ConnectionState::Closed,
        ),
        (
            "connecting_upgrade_ok_public",
            WsUrl::Public,
            Some(ConnectionState::Connecting),
            FsmEvent::WireUpgradeOk {
                connection_id: Some(42),
            },
            ConnectionState::Resubscribing,
        ),
        (
            "connecting_upgrade_ok_auth",
            WsUrl::Auth,
            Some(ConnectionState::Connecting),
            FsmEvent::WireUpgradeOk {
                connection_id: None,
            },
            ConnectionState::Authenticating,
        ),
        (
            "connecting_upgrade_failed_transient",
            WsUrl::Public,
            Some(ConnectionState::Connecting),
            FsmEvent::WireUpgradeFailed { http_status: 503 },
            ConnectionState::BackingOff,
        ),
        (
            "connecting_upgrade_failed_non_transient",
            WsUrl::Public,
            Some(ConnectionState::Connecting),
            FsmEvent::WireUpgradeFailed { http_status: 401 },
            ConnectionState::Failed,
        ),
        (
            "connecting_upgrade_timeout",
            WsUrl::Public,
            Some(ConnectionState::Connecting),
            FsmEvent::TimerUpgradeTimeout,
            ConnectionState::BackingOff,
        ),
        (
            "authenticating_bad_creds",
            WsUrl::Auth,
            Some(ConnectionState::Authenticating),
            FsmEvent::WireAuthHandshakeFailed {
                kind: crate::conn::AuthErrorKind::BadCreds,
            },
            ConnectionState::Failed,
        ),
        (
            "authenticating_permission_denied",
            WsUrl::Auth,
            Some(ConnectionState::Authenticating),
            FsmEvent::WireAuthHandshakeFailed {
                kind: crate::conn::AuthErrorKind::PermissionDenied,
            },
            ConnectionState::Failed,
        ),
        (
            "authenticating_abnormal_close",
            WsUrl::Auth,
            Some(ConnectionState::Authenticating),
            FsmEvent::WireAbnormalClose {
                error: crate::transport::TransportError {
                    kind: crate::transport::TransportErrorKind::SocketReset,
                    transient: true,
                },
            },
            ConnectionState::BackingOff,
        ),
        (
            "authenticating_staleness",
            WsUrl::Auth,
            Some(ConnectionState::Authenticating),
            FsmEvent::TimerStalenessElapsed,
            ConnectionState::BackingOff,
        ),
        (
            "authenticating_token_refresh_failed",
            WsUrl::Auth,
            Some(ConnectionState::Authenticating),
            FsmEvent::BusTokenRefreshFailed {
                request_id: 1,
                error: crate::auth::AuthError::TokenRefreshFailed,
            },
            ConnectionState::Failed,
        ),
        (
            "authenticating_call_close",
            WsUrl::Auth,
            Some(ConnectionState::Authenticating),
            FsmEvent::CallClose { request_id: 9 },
            ConnectionState::Closing,
        ),
        (
            "authenticating_force_reconnect",
            WsUrl::Auth,
            Some(ConnectionState::Authenticating),
            FsmEvent::CallForceReconnect { request_id: 9 },
            ConnectionState::Closing,
        ),
        (
            "resubscribing_intermediate_ack",
            WsUrl::Public,
            Some(ConnectionState::Resubscribing),
            FsmEvent::WireSubscribeAck {
                channel: ChannelName::Ticker,
                pair: None,
                last: false,
            },
            ConnectionState::Resubscribing,
        ),
        (
            "open_call_close",
            WsUrl::Public,
            Some(ConnectionState::Open),
            FsmEvent::CallClose { request_id: 1 },
            ConnectionState::Closing,
        ),
        (
            "open_wire_close_received",
            WsUrl::Public,
            Some(ConnectionState::Open),
            FsmEvent::WireCloseReceived {
                code: 1006,
                reason: None,
            },
            ConnectionState::BackingOff,
        ),
        (
            "open_wire_close_received_1008",
            WsUrl::Public,
            Some(ConnectionState::Open),
            FsmEvent::WireCloseReceived {
                code: 1008,
                reason: Some("policy violation".to_string()),
            },
            ConnectionState::Failed,
        ),
        (
            "open_staleness_elapsed",
            WsUrl::Public,
            Some(ConnectionState::Open),
            FsmEvent::TimerStalenessElapsed,
            ConnectionState::BackingOff,
        ),
        (
            "closing_wire_close_received",
            WsUrl::Public,
            Some(ConnectionState::Closing),
            FsmEvent::WireCloseReceived {
                code: 1000,
                reason: None,
            },
            ConnectionState::Closed,
        ),
        (
            "closing_timeout",
            WsUrl::Public,
            Some(ConnectionState::Closing),
            FsmEvent::TimerCloseTimeout,
            ConnectionState::Closed,
        ),
        (
            "failed_close",
            WsUrl::Public,
            Some(ConnectionState::Failed),
            FsmEvent::CallClose { request_id: 1 },
            ConnectionState::Closed,
        ),
        (
            "failed_ignores_wire_events",
            WsUrl::Public,
            Some(ConnectionState::Failed),
            FsmEvent::WireUpgradeOk {
                connection_id: None,
            },
            ConnectionState::Failed,
        ),
        (
            "resubscribing_staleness_elapsed",
            WsUrl::Public,
            Some(ConnectionState::Resubscribing),
            FsmEvent::TimerStalenessElapsed,
            ConnectionState::BackingOff,
        ),
    ];
    for (label, url, start, event, expected) in cases {
        let mut c = mc(url);
        if let Some(s) = start {
            c.test_set_state(s);
        }
        c.handle_event(event);
        assert_eq!(c.state(), expected, "case {label}");
    }
}

#[test]
fn connecting_exit_clears_upgrade_timeout_table() {
    // The upgrade-timeout timer armed on Connecting entry must be disarmed on every
    // Connecting-exit arm, else it leaks into the next state.
    let cases: Vec<(&str, FsmEvent, ConnectionState, bool)> = vec![
        (
            "upgrade_failed_non_transient",
            FsmEvent::WireUpgradeFailed { http_status: 401 },
            ConnectionState::Failed,
            false,
        ),
        (
            "upgrade_failed_transient",
            FsmEvent::WireUpgradeFailed { http_status: 503 },
            ConnectionState::BackingOff,
            false,
        ),
        (
            "call_close",
            FsmEvent::CallClose { request_id: 2 },
            ConnectionState::Closing,
            true,
        ),
        (
            "connect_error_non_transient",
            FsmEvent::WireConnectError {
                kind: crate::transport::TransportErrorKind::TcpRefused,
                transient: false,
            },
            ConnectionState::Failed,
            false,
        ),
    ];
    for (label, trigger, expected, close_armed) in cases {
        let mut c = mc_with_knobs_clock_jitter(
            WsUrl::Public,
            crate::build::knobs::Knobs::defaults(),
            100,
            test_jitter(),
        );
        c.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
        assert_eq!(
            c.state(),
            ConnectionState::Connecting,
            "{label}: precondition Connecting"
        );
        assert!(
            c.timers.upgrade_timeout_due_at.is_some(),
            "{label}: upgrade timeout armed on Connecting entry"
        );
        c.handle_event(trigger);
        assert_eq!(c.state(), expected, "{label}: expected exit state");
        assert_eq!(
            c.timers.upgrade_timeout_due_at, None,
            "{label}: upgrade timeout disarmed on the Connecting exit"
        );
        if close_armed {
            assert!(
                c.timers.close_timeout_due_at.is_some(),
                "{label}: close-handshake timeout armed on Closing entry"
            );
        }
    }
}
