use std::sync::Arc;

use tokio::sync::oneshot;

use crate::dispatch::{EventEnvelope, EventPayload, EventType};
use crate::error::ConnectionError;
use crate::types::{ConnectionState, WsUrl};

use super::WsSurface;
use super::guard::SubscriberGuard;

impl WsSurface {
    /// Backstop await budget for WS bring-up; FSM timers fail well before this.
    const ENSURE_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    /// Drive `url` to a state the pending order can be sent from.
    /// Idempotent and multi-order-race safe: only `Idle` triggers the connect.
    /// No REST fallback — failure surfaces as [`ConnectionError`].
    pub(crate) async fn ensure_order_sendable(&self, url: WsUrl) -> Result<(), ConnectionError> {
        // Concurrent subscribe + bare order can NotOpen during Authenticating;
        // NotOpen is retryable. Cold bare-order race closed by ConnectionSendReadyEvent.
        self.ensure_sendable(url).await
    }

    /// Bring-up driver. `Authenticating` is a valid completion target when the
    /// auth registry is empty (bare-order send-point).
    async fn ensure_sendable(&self, url: WsUrl) -> Result<(), ConnectionError> {
        // No supervisor (REST-only test path) → typed error, not a panic.
        let supervisor = self
            .supervisor
            .as_ref()
            .ok_or_else(ConnectionError::not_open)?;

        // WS op before `ready()`: I/O reactor isn't draining, so Idle connect would
        // stall to the backstop. Flag flips once; in-flight bring-up still awaits below.
        if !self.bus.is_io_reactor_started() {
            return Err(ConnectionError::not_open());
        }

        // `Authenticating` is a send-point only for bare-order (order is the auth probe).
        // With an auth subscription the order must wait for `Open`.
        let accept_authenticating = !supervisor.auth_has_subscriptions();

        match supervisor.current_state(url) {
            ConnectionState::Open => return Ok(()),
            // Bare `Authenticating` is not a short-circuit: token may not be cached yet.
            // Await `ConnectionSendReadyEvent` instead. Already in flight — not a connect trigger.
            ConnectionState::Authenticating if accept_authenticating => {}
            // Only Idle triggers connect. Queue-full maps to `not_open` (FSM stays Idle).
            ConnectionState::Idle => {
                supervisor
                    .connect(url)
                    .map_err(|_| ConnectionError::not_open())?;
            }
            // Bring-up already in flight — do not re-trigger; await the same Open event.
            ConnectionState::Connecting
            | ConnectionState::Authenticating
            | ConnectionState::Resubscribing => {}
            ConnectionState::BackingOff
            | ConnectionState::Failed
            | ConnectionState::Closing
            | ConnectionState::Closed => return Err(ConnectionError::not_open()),
        }

        self.await_open_by_url(url, Self::ENSURE_OPEN_TIMEOUT, accept_authenticating)
            .await
    }

    /// Await the first URL-matching open (Ok) or failed/closed (Err), bounded by
    /// `timeout`. URL-keyed so N racing orders resolve on one Open.
    async fn await_open_by_url(
        &self,
        url: WsUrl,
        timeout: std::time::Duration,
        accept_authenticating: bool,
    ) -> Result<(), ConnectionError> {
        let (tx, rx) = oneshot::channel::<Result<(), ConnectionError>>();
        let tx_cell = Arc::new(std::sync::Mutex::new(Some(tx)));

        // Guard before first subscribe so every exit path tears down subscribers.
        let mut guard = SubscriberGuard::new(self.weak_bus.clone());

        let tx_open = Arc::clone(&tx_cell);
        let open_sub = self.bus.subscribe(
            EventType::ConnectionOpenEvent,
            Arc::new(move |env: &EventEnvelope| {
                if let EventPayload::ConnectionOpenEvent { url: u, .. } = &env.payload {
                    if *u == url {
                        if let Some(tx) = tx_open.lock().expect("ensure_open tx lock").take() {
                            let _ = tx.send(Ok(()));
                        }
                    }
                }
            }),
            1,
        );
        guard.push(open_sub);
        let tx_fail = Arc::clone(&tx_cell);
        let fail_sub = self.bus.subscribe(
            EventType::ConnectionFailedEvent,
            Arc::new(move |env: &EventEnvelope| {
                if let EventPayload::ConnectionFailedEvent { url: u, .. } = &env.payload {
                    if *u == url {
                        if let Some(tx) = tx_fail.lock().expect("ensure_open tx lock").take() {
                            let _ = tx.send(Err(ConnectionError::not_open()));
                        }
                    }
                }
            }),
            1,
        );
        guard.push(fail_sub);

        // Concurrent close emits only ConnectionClosedEvent — resolve not_open immediately.
        let tx_closed = Arc::clone(&tx_cell);
        let closed_sub = self.bus.subscribe(
            EventType::ConnectionClosedEvent,
            Arc::new(move |env: &EventEnvelope| {
                if let EventPayload::ConnectionClosedEvent { url: u, .. } = &env.payload {
                    if *u == url {
                        if let Some(tx) = tx_closed.lock().expect("ensure_open tx lock").take() {
                            let _ = tx.send(Err(ConnectionError::not_open()));
                        }
                    }
                }
            }),
            1,
        );
        guard.push(closed_sub);

        // Send-ready subscriber (order path only); same oneshot as Open — both mean Ok.
        if accept_authenticating {
            let tx_ready = Arc::clone(&tx_cell);
            let send_ready_sub = self.bus.subscribe(
                EventType::ConnectionSendReadyEvent,
                Arc::new(move |env: &EventEnvelope| {
                    if let EventPayload::ConnectionSendReadyEvent { url: u, .. } = &env.payload {
                        if *u == url {
                            if let Some(tx) = tx_ready.lock().expect("ensure_open tx lock").take() {
                                let _ = tx.send(Ok(()));
                            }
                        }
                    }
                }),
                1,
            );
            guard.push(send_ready_sub);
        }

        // Re-check after subscribe to close the snapshot↔subscribe race.
        if let Some(sup) = self.supervisor.as_ref() {
            match sup.current_state(url) {
                ConnectionState::Open => {
                    return Ok(());
                }
                // Level-check auth_send_ready(): true recovers a missed edge; else fall through.
                ConnectionState::Authenticating if accept_authenticating => {
                    if sup.auth_send_ready() {
                        return Ok(());
                    }
                }
                ConnectionState::Failed | ConnectionState::Closing | ConnectionState::Closed => {
                    return Err(ConnectionError::not_open());
                }
                // Transient — await; do not re-trigger (Idle's connect already fired).
                ConnectionState::Idle
                | ConnectionState::Connecting
                | ConnectionState::Authenticating
                | ConnectionState::Resubscribing
                | ConnectionState::BackingOff => {}
            }
        }

        // Race oneshot against the backstop. Only send-ready (token cached) unblocks bare order.
        let outcome = match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ConnectionError::not_open()),
            Err(_) => Err(ConnectionError::not_open()),
        };
        drop(guard);
        outcome
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
    use std::time::Duration;

    use crate::clock::SystemClock;
    use crate::conn::ConnectionSupervisor;
    use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig, EventType, PresenceMirror};
    use crate::types::{ConnectionState, WsUrl};

    use super::super::WsSurface;

    const TEST_BUDGET: Duration = Duration::from_millis(1500);

    fn make_surface_with_supervisor() -> (
        Arc<WsSurface>,
        Arc<DispatchEventBus>,
        Arc<ConnectionSupervisor>,
        Arc<AtomicU8>,
    ) {
        use crate::auth::{AuthStack, SystemClockNonceSource, TokenLifecycleManager};
        use std::collections::HashMap;

        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let auth = Arc::new(AuthStack::new(
            None,
            None,
            Arc::new(SystemClockNonceSource::new()),
            HashMap::new(),
            TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
        ));
        let supervisor = Arc::new(ConnectionSupervisor::new(Arc::clone(&bus)));
        let auth_mirror = Arc::clone(&supervisor.state_mirror_clones()[&WsUrl::Auth]);
        let mirror: PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let alloc = Arc::new(AtomicU64::new(1));
        let trading_rl = Arc::new(crate::rate_limit::SpotTradingRateLimitTracker::new(
            crate::rate_limit::Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let surface = Arc::new(WsSurface::new_with_supervisor(
            Arc::clone(&bus),
            mirror,
            alloc,
            Arc::clone(&supervisor),
            trading_rl,
            auth,
            clock,
        ));
        (surface, bus, supervisor, auth_mirror)
    }

    #[tokio::test]
    async fn await_open_resolves_via_level_check_when_already_send_ready() {
        let (surface, bus, supervisor, auth_mirror) = make_surface_with_supervisor();
        auth_mirror.store(ConnectionState::Authenticating.as_u8(), Ordering::Release);
        supervisor
            .auth_send_ready_flag()
            .store(true, Ordering::Release);
        assert_eq!(bus.subscriber_count(EventType::ConnectionSendReadyEvent), 0);

        let res = tokio::time::timeout(
            TEST_BUDGET,
            surface.await_open_by_url(WsUrl::Auth, TEST_BUDGET, true),
        )
        .await
        .expect("level check must resolve immediately — NO hang");
        assert!(
            res.is_ok(),
            "send-ready level flag true → Ok via level check"
        );

        assert_eq!(
            bus.subscriber_count(EventType::ConnectionSendReadyEvent),
            0,
            "SubscriberGuard must tear down the send-ready subscriber on Ok exit"
        );
        assert_eq!(bus.subscriber_count(EventType::ConnectionOpenEvent), 0);
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn dropping_parked_await_tears_down_subscribers_no_leak() {
        let (surface, bus, _supervisor, auth_mirror) = make_surface_with_supervisor();
        auth_mirror.store(ConnectionState::Connecting.as_u8(), Ordering::Release);
        assert_eq!(
            bus.subscriber_count(EventType::ConnectionOpenEvent),
            0,
            "baseline"
        );

        let fut = surface.await_open_by_url(WsUrl::Auth, Duration::from_secs(30), true);
        let mut boxed = Box::pin(fut);
        let _ = tokio::time::timeout(Duration::from_millis(50), boxed.as_mut()).await;
        assert_eq!(
            bus.subscriber_count(EventType::ConnectionOpenEvent),
            1,
            "parked await registered the Open subscriber"
        );
        assert_eq!(bus.subscriber_count(EventType::ConnectionFailedEvent), 1);
        assert_eq!(bus.subscriber_count(EventType::ConnectionSendReadyEvent), 1);

        drop(boxed);

        assert_eq!(
            bus.subscriber_count(EventType::ConnectionOpenEvent),
            0,
            "drop-at-await tore down the Open subscriber (no leak)"
        );
        assert_eq!(
            bus.subscriber_count(EventType::ConnectionFailedEvent),
            0,
            "drop-at-await tore down the Failed subscriber (no leak)"
        );
        assert_eq!(
            bus.subscriber_count(EventType::ConnectionSendReadyEvent),
            0,
            "drop-at-await tore down the SendReady subscriber (no leak)"
        );
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn await_open_resolves_fast_on_concurrent_close() {
        use crate::dispatch::{AckSource, ClosedReason, EventEnvelope, EventPayload};
        use crate::types::MonotonicInstant;

        let (surface, bus, _supervisor, auth_mirror) = make_surface_with_supervisor();
        auth_mirror.store(ConnectionState::Connecting.as_u8(), Ordering::Release);

        let fut = surface.await_open_by_url(WsUrl::Auth, Duration::from_secs(30), true);
        let mut boxed = Box::pin(fut);
        let _ = tokio::time::timeout(Duration::from_millis(50), boxed.as_mut()).await;
        assert_eq!(
            bus.subscriber_count(EventType::ConnectionClosedEvent),
            1,
            "parked order await must register the ConnectionClosedEvent subscriber"
        );

        bus.publish(EventEnvelope {
            event_type: EventType::ConnectionClosedEvent,
            event_version: 1,
            timestamp_monotonic: MonotonicInstant(Duration::from_secs(0)),
            request_id: None,
            payload: EventPayload::ConnectionClosedEvent {
                url: WsUrl::Auth,
                closed_at_monotonic: MonotonicInstant(Duration::from_secs(0)),
                reason: ClosedReason::ClientInitiated,
                ack: AckSource::SocketDropped,
                server_connection_id: None,
            },
        });

        let res = tokio::time::timeout(TEST_BUDGET, boxed.as_mut())
            .await
            .expect("close event must resolve the await FAST — no 30s stall");
        assert!(
            res.is_err(),
            "a concurrent close resolves the order await as not-open"
        );
        bus.stop_reactors();
    }
}
