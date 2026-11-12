use super::*;
use crate::clock::{Clock, SystemClock};
use crate::dispatch::DispatchEventBusConfig;
use crate::transport::WsSocketFactoryLike;
use crate::transport::driveable_mock::DriveableWsSocketFactory;
use crate::types::{StreamSymbol, Symbol};
use std::time::Duration;

const BUDGET: Duration = Duration::from_millis(1500);

/// A token-refresh REST reply carrying `token`.
fn token_envelope(token: &str) -> serde_json::Value {
    serde_json::json!({ "error": [], "result": { "token": token, "expires": 900u32 } })
}

fn sym(s: &str) -> StreamSymbol {
    StreamSymbol::from(Symbol::new(s).expect("valid symbol"))
}

fn ticker_params() -> crate::conn::subscription_registry::SubscribeParams {
    crate::conn::subscription_registry::SubscribeParams::Ticker {
        snapshot: None,
        event_trigger: None,
    }
}

fn register_batch(
    bus: &Arc<DispatchEventBus>,
    url: WsUrl,
    entries: Vec<crate::conn::subscription_registry::SubscriptionEntry>,
) {
    bus.try_post_caller_inbound(crate::dispatch::CallerInbound::RegistryMutation {
        url,
        mutation: crate::dispatch::event_bus::RegistryMutationOp::RegisterBatch {
            ref_id: None,
            entries,
        },
    })
    .expect("post register batch");
}

fn bus_and_factory() -> (Arc<DispatchEventBus>, Arc<DriveableWsSocketFactory>) {
    bus_and_factory_with_clock(Arc::new(SystemClock))
}

fn bus_and_factory_with_clock(
    clock: Arc<dyn Clock>,
) -> (Arc<DispatchEventBus>, Arc<DriveableWsSocketFactory>) {
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::defaults(),
        clock,
    ));
    bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
    let factory = Arc::new(DriveableWsSocketFactory::new(Arc::clone(&bus)));
    (bus, factory)
}

fn empty_snapshot() -> crate::types::CapabilitySnapshot {
    crate::types::CapabilitySnapshot {
        declared_namespaces: std::collections::HashSet::new(),
        declared_ws_urls: std::collections::HashSet::new(),
        discovered_at_first_use: std::collections::HashSet::new(),
    }
}

#[test]
fn spot_auth_owns_the_proactive_token_timer_unless_only_unified_private_is_live() {
    // (auth active, unified private active, owner)
    let cases = [
        (false, false, WsUrl::Auth),
        (true, false, WsUrl::Auth),
        (true, true, WsUrl::Auth),
        (false, true, WsUrl::UnifiedPrivate),
    ];
    for (auth, unified, owner) in cases {
        assert_eq!(
            timer_owner_for(auth, unified),
            owner,
            "auth={auth} unified={unified}"
        );
    }
}

#[test]
fn token_refresh_outcome_is_delivered_only_to_the_matching_connection() {
    use ConnectionState::{Authenticating, Open, Resubscribing};
    // (url, outcome request id, state, id the connection awaits, delivered?)
    let cases = [
        (WsUrl::Auth, 41, Authenticating, Some(41), true),
        (WsUrl::Auth, 42, Authenticating, Some(41), false),
        (WsUrl::UnifiedPrivate, 42, Authenticating, Some(42), true),
        (WsUrl::UnifiedPrivate, 41, Authenticating, Some(42), false),
        (WsUrl::Auth, 52, Authenticating, Some(51), false),
        (WsUrl::UnifiedPrivate, 52, Authenticating, Some(52), true),
        (WsUrl::Auth, 61, Authenticating, Some(62), false),
        (WsUrl::UnifiedPrivate, 71, Open, Some(71), true),
        (WsUrl::UnifiedPrivate, 72, Open, Some(72), true),
        (WsUrl::UnifiedPrivate, 0, Open, Some(72), false),
        (WsUrl::Auth, 0, Open, None, true),
        (WsUrl::Auth, 0, Resubscribing, None, true),
        (WsUrl::Auth, 0, Authenticating, None, false),
        (WsUrl::UnifiedPrivate, 0, Resubscribing, None, false),
    ];
    for (url, id, state, awaited, expected) in cases {
        assert_eq!(
            should_deliver_token_refresh(url, id, state, awaited, WsUrl::Auth),
            expected,
            "{url:?} id={id} state={state:?} awaited={awaited:?}"
        );
    }
    assert!(should_deliver_token_refresh(
        WsUrl::UnifiedPrivate,
        0,
        Open,
        None,
        WsUrl::UnifiedPrivate
    ));
    assert!(!should_deliver_token_refresh(
        WsUrl::Auth,
        0,
        Open,
        None,
        WsUrl::UnifiedPrivate
    ));
}

#[tokio::test]
async fn read_frame_from_any_receives_a_unified_public_frame() {
    let (bus, factory) = bus_and_factory();
    let spot_socket = factory.open_socket(WsUrl::Public);
    let unified_socket = factory.open_socket(WsUrl::UnifiedPublic);
    let mut sockets: HashMap<WsUrl, Arc<dyn crate::transport::WsSocket>> = HashMap::new();
    sockets.insert(WsUrl::Public, spot_socket);
    sockets.insert(WsUrl::UnifiedPublic, unified_socket);

    factory
        .handle_for(WsUrl::UnifiedPublic)
        .expect("Unified public socket handle")
        .emit_text(include_str!(
            "../../../tests/fixtures/unified_ws/public_status_hello.json"
        ));

    let (url, frame) = tokio::time::timeout(BUDGET, read_frame_from_any(&sockets))
        .await
        .expect("Unified public frame was not received");
    let body: Value = serde_json::from_slice(&frame.expect("valid mock frame").payload)
        .expect("fixture is valid JSON");
    bus.stop_reactors();

    assert_eq!(url, WsUrl::UnifiedPublic);
    assert_eq!(body["channel"], "status");
}

async fn wait_for_state(mirror: &Arc<AtomicU8>, target: ConnectionState, what: &str) {
    let poll = async {
        loop {
            let s = ConnectionState::from_u8(mirror.load(Ordering::Acquire))
                .expect("valid mirror byte");
            if s == target {
                return;
            }
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(BUDGET, poll)
        .await
        .unwrap_or_else(|_| panic!("timeout: waiting for state {target:?} ({what})"));
}

/// Waits until the socket opened for `url` has sent exactly `n` text frames.
async fn socket_with_sent(
    factory: &DriveableWsSocketFactory,
    url: WsUrl,
    n: usize,
) -> crate::transport::driveable_mock::DriveableSocketHandle {
    tokio::time::timeout(BUDGET, async {
        loop {
            if let Some(handle) = factory.handle_for(url)
                && handle.sent_text().len() == n
            {
                return handle;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout: {url:?} socket to send {n} frame(s)"))
}

async fn socket_with_at_least(
    factory: &DriveableWsSocketFactory,
    url: WsUrl,
    min_sent: usize,
) -> crate::transport::driveable_mock::DriveableSocketHandle {
    tokio::time::timeout(BUDGET, async {
        loop {
            if let Some(handle) = factory.handle_for(url)
                && handle.sent_text().len() >= min_sent
            {
                return handle;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout: {url:?} socket to send at least {min_sent} frame(s)"))
}

async fn socket_at(
    factory: &DriveableWsSocketFactory,
    index: usize,
) -> crate::transport::driveable_mock::DriveableSocketHandle {
    tokio::time::timeout(BUDGET, async {
        loop {
            if let Some(handle) = factory.handle(index) {
                return handle;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout: socket #{index} never opened"))
}

async fn wait_for_frames(
    handle: &crate::transport::driveable_mock::DriveableSocketHandle,
    needle: &str,
    count: usize,
    what: &str,
) -> Vec<String> {
    tokio::time::timeout(BUDGET, async {
        loop {
            let frames: Vec<_> = handle
                .sent_text()
                .into_iter()
                .filter(|frame| frame.contains(needle))
                .collect();
            if frames.len() >= count {
                return frames;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout: {what}"))
}

type HttpMockFn = Box<dyn Fn() -> Result<Value, crate::transport::TransportError> + Send + Sync>;

pub(super) struct ClosureHttpMock {
    get_json: HttpMockFn,
    post_signed: HttpMockFn,
}

impl ClosureHttpMock {
    pub(super) fn new(get_json: HttpMockFn, post_signed: HttpMockFn) -> Self {
        Self {
            get_json,
            post_signed,
        }
    }

    pub(super) fn canned(envelope: serde_json::Value) -> Self {
        let g = envelope.clone();
        Self::new(
            Box::new(move || Ok(g.clone())),
            Box::new(move || Ok(envelope.clone())),
        )
    }

    /// Both methods panic: proves no REST fallback on the order path.
    pub(super) fn no_rest() -> Self {
        Self::new(
            Box::new(|| {
                panic!("NO REST fallback expected: get_json called on the order REST transport")
            }),
            Box::new(|| {
                panic!("NO REST fallback expected: signed POST called on the order REST transport")
            }),
        )
    }
}

#[async_trait::async_trait]
impl crate::transport::HttpTransport for ClosureHttpMock {
    async fn get_json(
        &self,
        _path: &str,
        _query: &[(&str, &str)],
    ) -> Result<Value, crate::transport::TransportError> {
        (self.get_json)()
    }
    async fn post_form_signed(
        &self,
        _path: &str,
        _body: &str,
        _api_key_header: &str,
        _api_sign_header: &str,
    ) -> Result<Value, crate::transport::TransportError> {
        (self.post_signed)()
    }
}

#[test]
fn upgrade_failure_event_maps_http_rejection_to_wire_upgrade_failed() {
    use crate::conn::managed_connection::FsmEvent;
    use crate::transport::TransportErrorKind;
    assert!(matches!(
        upgrade_failure_event(
            TransportErrorKind::HttpUpgradeRejected { status: 429 },
            true
        ),
        FsmEvent::WireUpgradeFailed { http_status: 429 }
    ));
    assert!(matches!(
        upgrade_failure_event(
            TransportErrorKind::HttpUpgradeRejected { status: 401 },
            false
        ),
        FsmEvent::WireUpgradeFailed { http_status: 401 }
    ));
    assert!(matches!(
        upgrade_failure_event(TransportErrorKind::TcpRefused, true),
        FsmEvent::WireConnectError {
            kind: TransportErrorKind::TcpRefused,
            transient: true
        }
    ));
}

#[cfg(test)]
mod reconnect_drive_tests {
    use super::auth_handshake_drive_tests::{ReactorHandles, spawn_unused_token_rig};
    use super::*;
    use crate::conn::ManagedConnection;
    use crate::conn::rate_budget::ConnectionRateBudget;
    use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
    use crate::dispatch::event_bus::CallerInbound;
    use crate::transport::{TransportError, TransportErrorKind};
    use crate::types::{ChannelName, ConnectionState, WsUrl};
    use std::time::Duration;

    pub(super) fn ack_text(channel: &str, symbol: Option<&str>) -> String {
        let mut result = serde_json::Map::new();
        result.insert("channel".into(), serde_json::json!(channel));
        if let Some(s) = symbol {
            result.insert("symbol".into(), serde_json::json!(s));
        }
        serde_json::json!({
            "method": "subscribe",
            "success": true,
            "result": serde_json::Value::Object(result),
        })
        .to_string()
    }

    #[tokio::test]
    async fn run_drives_full_reconnect_cycle_and_replays_surviving_subs_on_new_socket() {
        let (bus, factory) = bus_and_factory();

        let connecting_count = Arc::new(AtomicU64::new(0));
        let open_count = Arc::new(AtomicU64::new(0));
        let reopen_count = Arc::new(AtomicU64::new(0));
        {
            let c = Arc::clone(&connecting_count);
            let _ = bus.subscribe(
                crate::dispatch::EventType::ConnectionConnectingEvent,
                Arc::new(move |_env: &crate::dispatch::EventEnvelope| {
                    c.fetch_add(1, Ordering::Relaxed);
                }),
                u16::MAX,
            );
            let o = Arc::clone(&open_count);
            let _ = bus.subscribe(
                crate::dispatch::EventType::ConnectionOpenEvent,
                Arc::new(move |_env: &crate::dispatch::EventEnvelope| {
                    o.fetch_add(1, Ordering::Relaxed);
                }),
                u16::MAX,
            );
            let r = Arc::clone(&reopen_count);
            let _ = bus.subscribe(
                crate::dispatch::EventType::ConnectionReopenedEvent,
                Arc::new(move |_env: &crate::dispatch::EventEnvelope| {
                    r.fetch_add(1, Ordering::Relaxed);
                }),
                u16::MAX,
            );
        }

        let public_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        state_mirrors.insert(WsUrl::Public, Arc::clone(&public_mirror));
        state_mirrors.insert(
            WsUrl::Auth,
            Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())),
        );

        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(&factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let mut conns = HashMap::new();
        conns.insert(
            WsUrl::Public,
            ManagedConnection::new(
                WsUrl::Public,
                Arc::clone(&bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::new(crate::jitter::SplitMix64Jitter::with_seed(1)),
            ),
        );
        conns.insert(
            WsUrl::Auth,
            ManagedConnection::new(
                WsUrl::Auth,
                Arc::clone(&bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::new(crate::jitter::SplitMix64Jitter::with_seed(2)),
            ),
        );

        let caller_rx = bus
            .take_caller_to_io_rx()
            .expect("caller_to_io_rx available");
        let presence_mirror: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handler_registry = crate::dispatch::HandlerRegistry::new(presence_mirror);

        let init = IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::new(),
            handler_registry,
            auth_stack: Arc::new(crate::auth::AuthStack::new(
                None,
                None,
                Arc::new(crate::auth::SystemClockNonceSource::new()),
                std::collections::HashMap::new(),
                crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
            )),
            state_mirrors,
            bus_back_ref: Arc::downgrade(&bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::new(crate::conn::OrderGate::default()),
        };
        let reactor = tokio::spawn(run(init));

        for s in ["ETH/USD", "BTC/USD"] {
            register_batch(
                &bus,
                WsUrl::Public,
                vec![SubscriptionEntry::new(
                    WsUrl::Public,
                    ChannelName::Ticker,
                    Some(sym(s)),
                    ticker_params(),
                )],
            );
        }

        let s1 = socket_at(&factory, 0).await;

        use ConnectionState::*;
        let mut milestones: Vec<ConnectionState> = Vec::new();

        wait_for_state(&public_mirror, Resubscribing, "socket1 upgrade").await;
        milestones.push(Resubscribing);
        s1.emit_text(ack_text("ticker", Some("ETH/USD")));
        s1.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(&public_mirror, Open, "socket1 open").await;
        milestones.push(Open);

        s1.drop_with(TransportError {
            kind: TransportErrorKind::SocketReset,
            transient: true,
        });
        wait_for_state(&public_mirror, BackingOff, "socket1 drop").await;
        milestones.push(BackingOff);

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Public,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 42 },
        })
        .expect("post force reconnect");

        let s2 = {
            let poll = async {
                loop {
                    if factory.created_count() >= 2 {
                        return factory.handle(1).expect("socket2 handle");
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: socket2 never opened (reconnect)")
        };

        wait_for_state(&public_mirror, Resubscribing, "socket2 upgrade (replay)").await;
        milestones.push(Resubscribing);

        let replayed = {
            let poll = async {
                loop {
                    let sent = s2.sent_text();
                    if sent.len() >= 2 {
                        return sent;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: replay frames not sent on socket2")
        };
        assert_eq!(replayed.len(), 2, "both surviving subs replayed on socket2");
        let symbols: Vec<String> = replayed
            .iter()
            .map(|body| {
                let v: serde_json::Value = serde_json::from_str(body).expect("replay frame JSON");
                v["params"]["symbol"][0]
                    .as_str()
                    .expect("symbol in replay frame")
                    .to_string()
            })
            .collect();
        assert_eq!(
            symbols,
            vec!["BTC/USD".to_string(), "ETH/USD".to_string()],
            "replay frames emitted in canonical alphabetic-by-wire-string order on the NEW socket"
        );

        s2.emit_text(ack_text("ticker", Some("BTC/USD")));
        s2.emit_text(ack_text("ticker", Some("ETH/USD")));
        wait_for_state(&public_mirror, Open, "socket2 open").await;
        milestones.push(Open);

        assert_eq!(
            milestones,
            vec![Resubscribing, Open, BackingOff, Resubscribing, Open],
            "FSM passed through the full real reconnect milestones via run()'s select! loop"
        );
        assert_eq!(
            connecting_count.load(Ordering::Relaxed),
            2,
            "Connecting entered exactly twice (initial auto-connect + reconnect)"
        );
        assert_eq!(
            open_count.load(Ordering::Relaxed),
            1,
            "ConnectionOpenEvent fires once (initial bring-up only)"
        );
        assert_eq!(
            reopen_count.load(Ordering::Relaxed),
            1,
            "ConnectionReopenedEvent fires once (post-reconnect resteady)"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn timer_backoff_reconnect_wires_upgrade_bridge_and_advances_fsm() {
        let (bus, factory) = bus_and_factory();
        use crate::jitter::FixedJitter;

        let public_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        state_mirrors.insert(WsUrl::Public, Arc::clone(&public_mirror));
        state_mirrors.insert(
            WsUrl::Auth,
            Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())),
        );

        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(&factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let zero_jitter: Arc<dyn crate::jitter::JitterSource> = Arc::new(FixedJitter(0.0));
        let mut conns = HashMap::new();
        conns.insert(
            WsUrl::Public,
            ManagedConnection::new(
                WsUrl::Public,
                Arc::clone(&bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::clone(&zero_jitter),
            ),
        );
        conns.insert(
            WsUrl::Auth,
            ManagedConnection::new(
                WsUrl::Auth,
                Arc::clone(&bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::new(FixedJitter(0.0)),
            ),
        );

        let caller_rx = bus.take_caller_to_io_rx().expect("caller_to_io_rx");
        let presence_mirror: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handler_registry = crate::dispatch::HandlerRegistry::new(presence_mirror);

        let init = IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::new(),
            handler_registry,
            auth_stack: Arc::new(crate::auth::AuthStack::new(
                None,
                None,
                Arc::new(crate::auth::SystemClockNonceSource::new()),
                std::collections::HashMap::new(),
                crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
            )),
            state_mirrors,
            bus_back_ref: Arc::downgrade(&bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::new(crate::conn::OrderGate::default()),
        };
        let reactor = tokio::spawn(run(init));

        register_batch(
            &bus,
            WsUrl::Public,
            vec![SubscriptionEntry::new(
                WsUrl::Public,
                ChannelName::Ticker,
                Some(sym("BTC/USD")),
                ticker_params(),
            )],
        );

        let s1 = socket_at(&factory, 0).await;

        use ConnectionState::*;
        wait_for_state(&public_mirror, Resubscribing, "socket1 upgrade").await;
        s1.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(&public_mirror, Open, "socket1 open").await;

        s1.drop_with(TransportError {
            kind: TransportErrorKind::SocketReset,
            transient: true,
        });
        wait_for_state(&public_mirror, BackingOff, "socket1 drop → BackingOff").await;

        wait_for_state(
            &public_mirror,
            Resubscribing,
            "timer-driven reconnect: socket2 upgrade → Resubscribing",
        )
        .await;

        assert!(
            factory.created_count() >= 2,
            "timer-driven reconnect must open a second socket"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn force_reconnect_from_open_via_close_ack_wires_bridge_and_reopens() {
        use crate::jitter::FixedJitter;
        let (bus, factory) = bus_and_factory();

        let public_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        state_mirrors.insert(WsUrl::Public, Arc::clone(&public_mirror));
        state_mirrors.insert(
            WsUrl::Auth,
            Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())),
        );

        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(&factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let zero_jitter: Arc<dyn crate::jitter::JitterSource> = Arc::new(FixedJitter(0.0));
        let mut conns = HashMap::new();
        conns.insert(
            WsUrl::Public,
            ManagedConnection::new(
                WsUrl::Public,
                Arc::clone(&bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::clone(&zero_jitter),
            ),
        );
        conns.insert(
            WsUrl::Auth,
            ManagedConnection::new(
                WsUrl::Auth,
                Arc::clone(&bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::clone(&zero_jitter),
            ),
        );

        let caller_rx = bus.take_caller_to_io_rx().expect("caller_to_io_rx");
        let presence_mirror: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handler_registry = crate::dispatch::HandlerRegistry::new(presence_mirror);

        let init = IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::new(),
            handler_registry,
            auth_stack: Arc::new(crate::auth::AuthStack::new(
                None,
                None,
                Arc::new(crate::auth::SystemClockNonceSource::new()),
                std::collections::HashMap::new(),
                crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
            )),
            state_mirrors,
            bus_back_ref: Arc::downgrade(&bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::new(crate::conn::OrderGate::default()),
        };
        let reactor = tokio::spawn(run(init));

        register_batch(
            &bus,
            WsUrl::Public,
            vec![SubscriptionEntry::new(
                WsUrl::Public,
                ChannelName::Ticker,
                Some(sym("BTC/USD")),
                ticker_params(),
            )],
        );

        let s1 = socket_at(&factory, 0).await;

        use ConnectionState::*;
        wait_for_state(&public_mirror, Resubscribing, "socket1 upgrade").await;
        s1.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(&public_mirror, Open, "socket1 open").await;

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Public,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 77 },
        })
        .expect("post force reconnect");
        wait_for_state(
            &public_mirror,
            Closing,
            "force_reconnect from Open → Closing",
        )
        .await;

        s1.drop_with(TransportError {
            kind: TransportErrorKind::SocketReset,
            transient: true,
        });

        wait_for_state(
            &public_mirror,
            Resubscribing,
            "wire-close reconnect: socket2 upgrade → Resubscribing (bridge wired)",
        )
        .await;
        assert!(
            factory.created_count() >= 2,
            "wire-close reconnect must open a second socket"
        );
        let s2 = factory.handle(1).expect("socket2 handle");
        s2.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(&public_mirror, Open, "socket2 reopen").await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_spot_and_futures_tickers_keep_surfaces_lifecycle_and_teardown_isolated() {
        use crate::FuturesSubscriptionNamespace;
        use crate::api::market::{FuturesTickerUpdate, TickerUpdate, decode_futures_ticker};
        use crate::api::ws_decode::wrap_typed;
        use crate::api::ws_surface::WsSurface;
        use crate::conn::subscription_registry::EntrySubState;
        use crate::dispatch::event_bus::HandlerMutationOp;
        use crate::dispatch::{EventPayload, EventType, HandlerId};
        use crate::transport::{TransportError, TransportErrorKind};
        use std::sync::Mutex;

        let (bus, factory) = bus_and_factory();
        let (
            ReactorHandles {
                reactor,
                public: public_state,
                unified_public: unified_state,
                ..
            },
            mirror,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let public_futures_subscriptions =
            FuturesSubscriptionNamespace::new(Arc::new(WsSurface::new(
                Arc::clone(&bus),
                Arc::new(std::sync::RwLock::new(HashMap::new())),
                Arc::new(AtomicU64::new(47_104)),
            )));

        let spot_ticks = Arc::new(AtomicU64::new(0));
        let futures_ticks = Arc::new(AtomicU64::new(0));
        let spot_ticks_cb = Arc::clone(&spot_ticks);
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::Public,
            channel: ChannelName::Ticker,
            op: HandlerMutationOp::Register {
                id: HandlerId(47_101),
                callback: wrap_typed(
                    move |_: &TickerUpdate| {
                        spot_ticks_cb.fetch_add(1, Ordering::SeqCst);
                    },
                    crate::api::market::ws::decode_ticker as fn(_) -> Option<TickerUpdate>,
                ),
            },
        })
        .expect("register Spot ticker handler");
        let futures_ticks_cb = Arc::clone(&futures_ticks);
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::UnifiedPublic,
            channel: ChannelName::Ticker,
            op: HandlerMutationOp::Register {
                id: HandlerId(47_102),
                callback: wrap_typed(
                    move |_: &FuturesTickerUpdate| {
                        futures_ticks_cb.fetch_add(1, Ordering::SeqCst);
                    },
                    decode_futures_ticker,
                ),
            },
        })
        .expect("register Futures ticker handler");

        let reopened = Arc::new(Mutex::new(Vec::new()));
        let reopened_cb = Arc::clone(&reopened);
        let _reopen_subscription = bus.subscribe(
            EventType::ConnectionReopenedEvent,
            Arc::new(move |event| {
                if let EventPayload::ConnectionReopenedEvent { url, .. } = event.payload {
                    reopened_cb.lock().expect("reopened URLs").push(url);
                }
            }),
            u16::MAX,
        );
        let closed = Arc::new(Mutex::new(Vec::new()));
        let closed_cb = Arc::clone(&closed);
        let _close_subscription = bus.subscribe(
            EventType::ConnectionClosedEvent,
            Arc::new(move |event| {
                if let EventPayload::ConnectionClosedEvent { url, .. } = event.payload {
                    closed_cb.lock().expect("closed URLs").push(url);
                }
            }),
            u16::MAX,
        );
        let futures_terminations = Arc::new(Mutex::new(Vec::new()));
        let futures_terminations_cb = Arc::clone(&futures_terminations);
        let _futures_termination_subscription = bus.subscribe(
            EventType::FuturesSubscriptionTerminatedEvent,
            Arc::new(move |event| {
                if let EventPayload::FuturesSubscriptionTerminatedEvent {
                    channel,
                    symbol,
                    cause,
                    last_error,
                    ..
                } = &event.payload
                {
                    futures_terminations_cb.lock().expect("terminations").push((
                        *channel,
                        symbol.clone(),
                        *cause,
                        last_error.clone(),
                    ));
                }
            }),
            u16::MAX,
        );

        register_batch(
            &bus,
            WsUrl::Public,
            vec![SubscriptionEntry::new(
                WsUrl::Public,
                ChannelName::Ticker,
                Some(sym("BTC/USD")),
                SubscribeParams::Ticker {
                    snapshot: Some(true),
                    event_trigger: None,
                },
            )],
        );
        register_batch(
            &bus,
            WsUrl::UnifiedPublic,
            vec![SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                SubscribeParams::Ticker {
                    snapshot: Some(true),
                    event_trigger: None,
                },
            )],
        );

        let (spot_socket, futures_socket) = tokio::time::timeout(BUDGET, async {
            loop {
                if let (Some(spot), Some(futures)) = (
                    factory.handle_for(WsUrl::Public),
                    factory.handle_for(WsUrl::UnifiedPublic),
                ) && !spot.sent_text().is_empty()
                    && !futures.sent_text().is_empty()
                {
                    return (spot, futures);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Spot and Unified batches should send concurrently");
        let spot_frame: serde_json::Value =
            serde_json::from_str(&spot_socket.sent_text()[0]).expect("Spot subscribe JSON");
        let futures_frame: serde_json::Value =
            serde_json::from_str(&futures_socket.sent_text()[0]).expect("Futures subscribe JSON");
        assert_eq!(
            spot_frame["params"]["symbol"],
            serde_json::json!(["BTC/USD"])
        );
        assert!(spot_frame["params"].get("asset_class").is_none());
        assert_eq!(futures_frame["params"]["asset_class"], "derivatives");
        assert_eq!(
            futures_frame["params"]["symbol"],
            serde_json::json!(["PI_XBTUSD"])
        );

        wait_for_state(
            &public_state,
            ConnectionState::Resubscribing,
            "Spot ticker subscribe",
        )
        .await;
        wait_for_state(
            &unified_state,
            ConnectionState::Resubscribing,
            "Futures ticker subscribe",
        )
        .await;
        spot_socket.emit_text(super::reconnect_drive_tests::ack_text(
            "ticker",
            Some("BTC/USD"),
        ));
        futures_socket.emit_text(super::reconnect_drive_tests::ack_text(
            "ticker",
            Some("PI_XBTUSD"),
        ));
        tokio::time::timeout(BUDGET, async {
            loop {
                let (spot, futures) = {
                    let rows = mirror.read().expect("subscription mirror");
                    let spot = rows
                        .get(&(WsUrl::Public, ChannelName::Ticker, Some(sym("BTC/USD"))))
                        .is_some_and(|row| matches!(row.state, EntrySubState::Acked));
                    let futures = rows
                        .get(&(
                            WsUrl::UnifiedPublic,
                            ChannelName::Ticker,
                            Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                        ))
                        .is_some_and(|row| matches!(row.state, EntrySubState::Acked));
                    (spot, futures)
                };
                if spot && futures {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("each product owns an independently-acked registry row");
        wait_for_state(
            &public_state,
            ConnectionState::Open,
            "Spot ticker acknowledgement",
        )
        .await;

        let spot_ticker = serde_json::json!({
            "channel": "ticker", "type": "update", "data": [{
                "symbol": "BTC/USD", "bid": 100, "bid_qty": 1, "ask": 101, "ask_qty": 1,
                "last": 100, "volume": 1, "vwap": 100, "low": 99, "high": 102,
                "change": 1, "change_pct": 1
            }]
        });
        let futures_ticker = serde_json::json!({
            "channel": "ticker", "type": "update", "data": [{
                "symbol": "PI_XBTUSD", "timestamp": "2026-10-05T00:00:00Z",
                "next_funding_rate_time": "2026-10-05T08:00:00Z", "suspended": false,
                "trades": 1, "ask": 101, "ask_qty": 1, "bid": 100, "bid_qty": 1,
                "change": 1, "change_pct": 1, "funding_rate": 0, "funding_rate_prediction": 0,
                "high": 102, "index": 100, "last": 100, "low": 99, "mark": 100,
                "open": 100, "open_interest": 1, "premium": 0, "relative_funding_rate": 0,
                "relative_funding_rate_prediction": 0, "volume": 1, "volume_quote": 100,
                "vwap": 100
            }]
        });
        spot_socket.emit_text(futures_ticker.to_string());
        futures_socket.emit_text(spot_ticker.to_string());
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(spot_ticks.load(Ordering::SeqCst), 0);
        assert_eq!(futures_ticks.load(Ordering::SeqCst), 0);
        spot_socket.emit_text(spot_ticker.to_string());
        futures_socket.emit_text(futures_ticker.to_string());
        tokio::time::timeout(BUDGET, async {
            while spot_ticks.load(Ordering::SeqCst) != 1
                || futures_ticks.load(Ordering::SeqCst) != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Spot and Futures typed handlers receive only their own updates");

        futures_socket.drop_with(TransportError {
            kind: TransportErrorKind::SocketReset,
            transient: true,
        });
        wait_for_state(
            &unified_state,
            ConnectionState::BackingOff,
            "Unified drop must not tear down Spot",
        )
        .await;
        assert_eq!(
            ConnectionState::from_u8(public_state.load(Ordering::Acquire)),
            Some(ConnectionState::Open),
            "Unified failure must leave Spot Public open"
        );
        assert!(
            !closed.lock().expect("closed URLs").contains(&WsUrl::Public),
            "Unified failure must not publish a Spot close event"
        );
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::UnifiedPublic,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 47_103 },
        })
        .expect("force Unified reconnect");
        let replay_socket = tokio::time::timeout(BUDGET, async {
            loop {
                if factory.created_count() >= 3
                    && let Some(socket) = factory.handle_for(WsUrl::UnifiedPublic)
                    && !socket.sent_text().is_empty()
                {
                    return socket;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Unified socket should reconnect and replay independently");
        let replay: serde_json::Value =
            serde_json::from_str(&replay_socket.sent_text()[0]).expect("Unified replay JSON");
        assert_eq!(replay["params"]["channel"], "ticker");
        assert_eq!(replay["params"]["asset_class"], "derivatives");
        assert_eq!(replay["params"]["symbol"], serde_json::json!(["PI_XBTUSD"]));
        assert_eq!(spot_socket.sent_text().len(), 1, "Spot was not replayed");
        replay_socket.emit_text(
            serde_json::json!({
                "method": "subscribe",
                "success": true,
                "req_id": replay["req_id"],
                "result": { "channel": "ticker", "symbol": "PI_XBTUSD" }
            })
            .to_string(),
        );
        tokio::time::timeout(BUDGET, async {
            while !reopened
                .lock()
                .expect("reopened URLs")
                .contains(&WsUrl::UnifiedPublic)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("only Unified lifecycle should report a reconnect");
        assert!(
            !reopened
                .lock()
                .expect("reopened URLs")
                .contains(&WsUrl::Public)
        );

        public_futures_subscriptions
            .unsubscribe_all()
            .expect("public Futures aggregate teardown");
        tokio::time::timeout(BUDGET, async {
            loop {
                let torn_down = {
                    let rows = mirror.read().expect("subscription mirror");
                    !rows.contains_key(&(
                        WsUrl::UnifiedPublic,
                        ChannelName::Ticker,
                        Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                    )) && rows.contains_key(&(
                        WsUrl::Public,
                        ChannelName::Ticker,
                        Some(sym("BTC/USD")),
                    ))
                };
                if torn_down {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Futures aggregate teardown must preserve Spot registry state");
        tokio::time::timeout(BUDGET, async {
            loop {
                if !futures_terminations
                    .lock()
                    .expect("terminations")
                    .is_empty()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Futures teardown must emit the ticker termination");
        {
            let futures_terminations = futures_terminations.lock().expect("terminations");
            assert_eq!(futures_terminations.len(), 1);
            assert_eq!(futures_terminations[0].0, ChannelName::Ticker);
            assert_eq!(futures_terminations[0].1.as_deref(), Some("PI_XBTUSD"));
            assert_eq!(
                futures_terminations[0].2,
                crate::api::subscription::TerminationCause::ClientClosed
            );
        }
        assert_eq!(
            spot_socket.sent_text().len(),
            1,
            "Spot socket was unchanged"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    struct StalledSpot {
        bus: Arc<DispatchEventBus>,
        socket: crate::transport::driveable_mock::DriveableSocketHandle,
        mirror: Arc<AtomicU8>,
        rows: crate::conn::subscription_registry::SubscriptionMirror,
        terminated: Arc<AtomicU64>,
        reactor: tokio::task::JoinHandle<()>,
    }

    impl StalledSpot {
        async fn start(
            timeout_ms: u32,
            channel: ChannelName,
            pair: Option<StreamSymbol>,
            params: SubscribeParams,
        ) -> Self {
            use subscription_mirror_tests::{MirrorRig, spawn_rig};
            let MirrorRig {
                bus,
                factory,
                mirror: rows,
                public_state: mirror,
                terminated,
                reactor,
                ..
            } = spawn_rig(false, Some(timeout_ms));
            register_batch(
                &bus,
                WsUrl::Public,
                vec![SubscriptionEntry::new(WsUrl::Public, channel, pair, params)],
            );
            let socket = socket_with_sent(&factory, WsUrl::Public, 1).await;
            Self {
                bus,
                socket,
                mirror,
                rows,
                terminated,
                reactor,
            }
        }

        fn state(&self) -> ConnectionState {
            ConnectionState::from_u8(self.mirror.load(Ordering::Acquire)).expect("state byte")
        }

        fn req_id(&self) -> u64 {
            let frame: serde_json::Value =
                serde_json::from_str(&self.socket.sent_text()[0]).expect("subscribe frame");
            frame["req_id"].as_u64().expect("req_id")
        }

        fn row(
            &self,
            channel: ChannelName,
            pair: Option<StreamSymbol>,
        ) -> crate::conn::subscription_registry::EntrySubState {
            self.rows.read().expect("rows")[&(WsUrl::Public, channel, pair)]
                .state
                .clone()
        }

        fn release_upgrade(&self) {
            let counter = Arc::clone(&self.terminated);
            let _ = self.bus.subscribe(
                crate::dispatch::EventType::SubscriptionTerminatedEvent,
                Arc::new(move |_env: &crate::dispatch::EventEnvelope| {
                    counter.fetch_add(1, Ordering::Relaxed);
                }),
                u16::MAX,
            );
            self.bus
                .start_dispatch_reactor(&tokio::runtime::Handle::current());
        }

        fn stop(self) {
            self.reactor.abort();
            self.bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn spot_ack_timer_due_while_connecting_does_not_resend_after_upgrade() {
        let pair = Some(sym("BTC/USD"));
        let spot = StalledSpot::start(40, ChannelName::Ticker, pair.clone(), ticker_params()).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        spot.socket.emit_text(ack_text("ticker", Some("BTC/USD")));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(spot.state(), ConnectionState::Connecting);

        spot.release_upgrade();
        wait_for_state(&spot.mirror, ConnectionState::Open, "upgrade released").await;
        tokio::time::sleep(Duration::from_millis(600)).await;

        assert_eq!(spot.socket.sent_text().len(), 1);
        assert_eq!(
            spot.row(ChannelName::Ticker, pair),
            crate::conn::subscription_registry::EntrySubState::Acked
        );
        assert_eq!(spot.state(), ConnectionState::Open);
        spot.stop();
    }

    #[tokio::test]
    async fn spot_ack_while_connecting_opens_once_upgraded_without_resend() {
        let pair = Some(sym("BTC/USD"));
        let spot =
            StalledSpot::start(300, ChannelName::Ticker, pair.clone(), ticker_params()).await;
        spot.socket.emit_text(ack_text("ticker", Some("BTC/USD")));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(spot.state(), ConnectionState::Connecting);

        spot.release_upgrade();
        wait_for_state(&spot.mirror, ConnectionState::Open, "upgrade released").await;
        tokio::time::sleep(Duration::from_millis(700)).await;

        assert_eq!(spot.socket.sent_text().len(), 1);
        assert_eq!(
            spot.row(ChannelName::Ticker, pair),
            crate::conn::subscription_registry::EntrySubState::Acked
        );
        assert_eq!(spot.terminated.load(Ordering::Relaxed), 0);
        assert_eq!(spot.state(), ConnectionState::Open);
        spot.stop();
    }

    #[tokio::test]
    async fn spot_upgrade_slower_than_ack_timeout_keeps_the_ack_timer() {
        let pair = Some(sym("BTC/USD"));
        let spot = StalledSpot::start(40, ChannelName::Ticker, pair.clone(), ticker_params()).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(spot.state(), ConnectionState::Connecting);

        spot.release_upgrade();
        wait_for_frames(
            &spot.socket,
            "\"ticker\"",
            2,
            "ack timeout resends after upgrade",
        )
        .await;
        spot.socket.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(&spot.mirror, ConnectionState::Open, "ack after resend").await;

        assert_eq!(
            spot.row(ChannelName::Ticker, pair),
            crate::conn::subscription_registry::EntrySubState::Acked
        );
        spot.stop();
    }

    #[tokio::test]
    async fn spot_ack_after_its_timer_fired_marks_the_row_acked() {
        let pair = Some(sym("BTC/USD"));
        let spot = StalledSpot::start(40, ChannelName::Ticker, pair.clone(), ticker_params()).await;
        spot.release_upgrade();
        wait_for_state(
            &spot.mirror,
            ConnectionState::Resubscribing,
            "upgrade released",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        spot.socket.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(&spot.mirror, ConnectionState::Open, "late ack").await;

        assert_eq!(
            spot.row(ChannelName::Ticker, pair),
            crate::conn::subscription_registry::EntrySubState::Acked
        );
        spot.stop();
    }

    #[tokio::test]
    async fn spot_non_transient_reject_while_connecting_terminates_without_resend() {
        let pair = Some(sym("BTC/USD"));
        let spot =
            StalledSpot::start(300, ChannelName::Ticker, pair.clone(), ticker_params()).await;
        spot.socket.emit_text(
            serde_json::json!({
                "method": "subscribe",
                "success": false,
                "error": "Currency pair not supported BTC/USD",
                "symbol": "BTC/USD",
                "req_id": spot.req_id(),
            })
            .to_string(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(spot.state(), ConnectionState::Connecting);

        spot.release_upgrade();
        wait_for_state(&spot.mirror, ConnectionState::Open, "upgrade released").await;
        tokio::time::sleep(Duration::from_millis(700)).await;

        assert_eq!(spot.socket.sent_text().len(), 1);
        assert_eq!(spot.terminated.load(Ordering::Relaxed), 1);
        assert!(matches!(
            spot.row(ChannelName::Ticker, pair),
            crate::conn::subscription_registry::EntrySubState::Terminated { .. }
        ));
        spot.stop();
    }

    #[tokio::test]
    async fn spot_status_quirk_after_timer_due_while_connecting_completes_the_subscribe() {
        let spot = StalledSpot::start(40, ChannelName::Status, None, SubscribeParams::Status).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        spot.socket.emit_text(
            serde_json::json!({
                "method": "subscribe",
                "success": false,
                "error": "Symbol(s) not found",
                "req_id": spot.req_id(),
            })
            .to_string(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(spot.state(), ConnectionState::Connecting);

        spot.release_upgrade();
        wait_for_state(&spot.mirror, ConnectionState::Open, "upgrade released").await;
        tokio::time::sleep(Duration::from_millis(600)).await;

        assert_eq!(spot.socket.sent_text().len(), 1);
        assert_eq!(
            spot.row(ChannelName::Status, None),
            crate::conn::subscription_registry::EntrySubState::Acked
        );
        spot.stop();
    }

    #[tokio::test]
    async fn spot_rate_limited_reject_while_connecting_resends_then_backs_off() {
        let pair = Some(sym("BTC/USD"));
        let spot =
            StalledSpot::start(2_000, ChannelName::Ticker, pair.clone(), ticker_params()).await;
        for sent in 1..=3 {
            let frames = wait_for_frames(&spot.socket, "\"ticker\"", sent, "resend").await;
            assert_eq!(spot.state(), ConnectionState::Connecting);
            let frame: serde_json::Value =
                serde_json::from_str(&frames[sent - 1]).expect("subscribe frame");
            spot.socket.emit_text(
                serde_json::json!({
                    "method": "subscribe",
                    "success": false,
                    "error": "EGeneral:Too many requests",
                    "symbol": "BTC/USD",
                    "req_id": frame["req_id"],
                })
                .to_string(),
            );
        }
        wait_for_state(
            &spot.mirror,
            ConnectionState::BackingOff,
            "budget exhausted",
        )
        .await;
        assert_eq!(spot.socket.sent_text().len(), 3);
        assert_eq!(
            spot.row(ChannelName::Ticker, pair),
            crate::conn::subscription_registry::EntrySubState::Pending
        );

        let classes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&classes);
        let _attempts = spot.bus.subscribe(
            crate::dispatch::EventType::ConnectionAttemptFailedEvent,
            Arc::new(move |env: &crate::dispatch::EventEnvelope| {
                if let crate::dispatch::EventPayload::ConnectionAttemptFailedEvent {
                    url: WsUrl::Public,
                    transient_class,
                    ..
                } = &env.payload
                {
                    sink.lock().expect("classes").push(*transient_class);
                }
            }),
            u16::MAX,
        );
        tokio::time::timeout(BUDGET, async {
            while classes.lock().expect("classes").is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("attempt failed event");
        assert_eq!(
            classes.lock().expect("classes")[0],
            crate::dispatch::TransientClass::SubscribeBudgetExhausted
        );
        spot.stop();
    }

    #[tokio::test]
    async fn unified_ack_timer_due_while_connecting_still_retries_after_upgrade() {
        use crate::dispatch::{EventEnvelope, EventPayload, EventType};
        use std::sync::Mutex;

        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::new(SystemClock) as Arc<dyn Clock>,
        ));
        bus.knobs()
            .subscribe_ack_timeout_ms
            .store(40, Ordering::Relaxed);
        let factory = Arc::new(DriveableWsSocketFactory::new(Arc::clone(&bus)));
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_state,
                ..
            },
            _,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        register_batch(
            &bus,
            WsUrl::UnifiedPublic,
            vec![SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                ticker_params(),
            )],
        );
        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            ConnectionState::from_u8(unified_state.load(Ordering::Acquire)),
            Some(ConnectionState::Connecting),
            "ack timer came due while the upgrade is still pending"
        );
        let failed = Arc::new(Mutex::new(Vec::<EventEnvelope>::new()));
        let failed_sink = Arc::clone(&failed);
        let _failed_subscription = bus.subscribe(
            EventType::FuturesSubscribeFailedEvent,
            Arc::new(move |event: &EventEnvelope| {
                failed_sink
                    .lock()
                    .expect("failed events")
                    .push(event.clone());
            }),
            u16::MAX,
        );

        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let subscribes = wait_for_frames(
            &socket,
            "\"subscribe\"",
            2,
            "an unacknowledged Unified subscribe retries after the upgrade",
        )
        .await;

        let retry: serde_json::Value =
            serde_json::from_str(&subscribes[1]).expect("retry subscribe JSON");
        assert_eq!(retry["params"]["symbol"], serde_json::json!(["PI_XBTUSD"]));
        assert!(failed.lock().expect("failed events").iter().any(|event| {
            matches!(
                &event.payload,
                EventPayload::FuturesSubscribeFailedEvent {
                    channel: ChannelName::Ticker,
                    attempt: 1,
                    ..
                }
            )
        }));

        reactor.abort();
        bus.stop_reactors();
    }
}

#[cfg(test)]
mod subscribe_edge_gating_tests {
    use super::reconnect_drive_tests::ack_text;
    use super::*;
    use crate::conn::ManagedConnection;
    use crate::conn::rate_budget::ConnectionRateBudget;
    use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
    use crate::dispatch::DispatchEventBus;
    use crate::dispatch::event_bus::{CallerInbound, RegistryMutationOp};
    use crate::transport::driveable_mock::DriveableWsSocketFactory;
    use crate::types::{ChannelName, ConnectionState, WsUrl};
    use std::time::Duration;

    pub(super) fn spawn_public_reactor(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
    ) -> (tokio::task::JoinHandle<()>, Arc<AtomicU8>) {
        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let public_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        state_mirrors.insert(WsUrl::Public, Arc::clone(&public_mirror));
        state_mirrors.insert(
            WsUrl::Auth,
            Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())),
        );

        let mut conns = HashMap::new();
        for (url, seed) in [(WsUrl::Public, 1u64), (WsUrl::Auth, 2u64)] {
            conns.insert(
                url,
                ManagedConnection::new(
                    url,
                    Arc::clone(bus),
                    Arc::clone(&dyn_factory),
                    Arc::clone(&rate_budget),
                    Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                    Arc::new(crate::jitter::SplitMix64Jitter::with_seed(seed)),
                ),
            );
        }
        let caller_rx = bus.take_caller_to_io_rx().expect("caller_to_io_rx");
        let presence_mirror: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handler_registry = crate::dispatch::HandlerRegistry::new(presence_mirror);
        let init = IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::new(),
            handler_registry,
            auth_stack: Arc::new(crate::auth::AuthStack::new(
                None,
                None,
                Arc::new(crate::auth::SystemClockNonceSource::new()),
                std::collections::HashMap::new(),
                crate::auth::TokenLifecycleManager::new(Arc::clone(bus), "<test-key>".to_string()),
            )),
            state_mirrors,
            bus_back_ref: Arc::downgrade(bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::new(crate::conn::OrderGate::default()),
        };
        (tokio::spawn(run(init)), public_mirror)
    }

    pub(super) fn register_public(
        bus: &Arc<DispatchEventBus>,
        channel: ChannelName,
        pair: &str,
        params: SubscribeParams,
    ) {
        register_batch(
            bus,
            WsUrl::Public,
            vec![SubscriptionEntry::new(
                WsUrl::Public,
                channel,
                Some(sym(pair)),
                params,
            )],
        );
    }
    fn book_subscribe_count(
        sock: &crate::transport::driveable_mock::DriveableSocketHandle,
    ) -> usize {
        sock.sent_text()
            .iter()
            .filter(|body| {
                serde_json::from_str::<serde_json::Value>(body)
                    .ok()
                    .and_then(|v| {
                        let m = v.get("method")?.as_str()? == "subscribe";
                        let c = v.get("params")?.get("channel")?.as_str()? == "book";
                        Some(m && c)
                    })
                    .unwrap_or(false)
            })
            .count()
    }

    #[tokio::test]
    async fn duplicate_public_register_sends_one_wire_subscribe_and_one_ack_a372_a375() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        register_public(
            &bus,
            ChannelName::Book,
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        );
        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &public_mirror,
            ConnectionState::Resubscribing,
            "socket upgrade",
        )
        .await;

        register_public(
            &bus,
            ChannelName::Book,
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        );

        let one = async {
            loop {
                let n = book_subscribe_count(&sock);
                assert!(
                    n <= 1,
                    "duplicate (Book,pair) emitted {n} wire subscribes (expected 1)"
                );
                if n == 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, one)
            .await
            .expect("timeout: the single 0→1 book subscribe was never sent");
        let _ = tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                assert_eq!(
                    book_subscribe_count(&sock),
                    1,
                    "duplicate register must NOT send a second wire subscribe (A372/A375 edge-gate)"
                );
                tokio::task::yield_now().await;
            }
        })
        .await;

        sock.emit_text(ack_text("book", Some("BTC/USD")));
        wait_for_state(
            &public_mirror,
            ConnectionState::Open,
            "single ack drives Open",
        )
        .await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn book_caller_unsubscribe_emits_matching_wire_depth_md3883() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        register_public(
            &bus,
            ChannelName::Book,
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        );
        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &public_mirror,
            ConnectionState::Resubscribing,
            "socket upgrade",
        )
        .await;
        sock.emit_text(ack_text("book", Some("BTC/USD")));
        wait_for_state(
            &public_mirror,
            ConnectionState::Open,
            "subscribe ack drives Open",
        )
        .await;

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Public,
            mutation: RegistryMutationOp::Deregister {
                channel: ChannelName::Book,
                pair: Some(sym("BTC/USD")),
            },
        })
        .expect("post deregister");

        let find_unsub_depth = async {
            loop {
                if let Some(depth) = sock.sent_text().iter().find_map(|body| {
                    let v: serde_json::Value = serde_json::from_str(body).ok()?;
                    if v.get("method")?.as_str()? != "unsubscribe" {
                        return None;
                    }
                    let params = v.get("params")?;
                    if params.get("channel")?.as_str()? != "book" {
                        return None;
                    }
                    Some(params.get("depth").cloned())
                }) {
                    return depth;
                }
                tokio::task::yield_now().await;
            }
        };
        let depth = tokio::time::timeout(BUDGET, find_unsub_depth)
            .await
            .expect("timeout: book unsubscribe frame never sent");
        assert_eq!(
            depth,
            Some(serde_json::json!(25)),
            "book unsubscribe must echo the subscribe wire depth (D10→25), not a depthless frame (MD-3883)"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn book_guard_drop_unsubscribe_carries_matching_depth_md3883() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        register_public(
            &bus,
            ChannelName::Book,
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        );
        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &public_mirror,
            ConnectionState::Resubscribing,
            "socket upgrade",
        )
        .await;
        sock.emit_text(ack_text("book", Some("BTC/USD")));
        wait_for_state(
            &public_mirror,
            ConnectionState::Open,
            "subscribe ack drives Open",
        )
        .await;

        bus.try_post_caller_inbound(CallerInbound::SubscriptionGuardDrop {
            handler_id: crate::dispatch::HandlerId(1),
            url: WsUrl::Public,
            channel: ChannelName::Book,
            symbols: vec![sym("BTC/USD")],
        })
        .expect("post guard drop");

        let find_unsub_depth = async {
            loop {
                if let Some(depth) = sock.sent_text().iter().find_map(|body| {
                    let v: serde_json::Value = serde_json::from_str(body).ok()?;
                    if v.get("method")?.as_str()? != "unsubscribe" {
                        return None;
                    }
                    let params = v.get("params")?;
                    if params.get("channel")?.as_str()? != "book" {
                        return None;
                    }
                    Some(params.get("depth").cloned())
                }) {
                    return depth;
                }
                tokio::task::yield_now().await;
            }
        };
        let depth = tokio::time::timeout(BUDGET, find_unsub_depth)
            .await
            .expect("timeout: guard-drop book unsubscribe frame never sent");
        assert_eq!(
            depth,
            Some(serde_json::json!(25)),
            "guard-drop book unsubscribe must echo the subscribe wire depth (D10→25), not a depthless frame (MD-3883)"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn multi_pair_guard_drop_unsubscribes_every_pair() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        for pair in ["BTC/USD", "ETH/USD"] {
            register_public(
                &bus,
                ChannelName::Book,
                pair,
                SubscribeParams::Book {
                    depth: crate::types::BookDepth::D10,
                },
            );
        }
        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &public_mirror,
            ConnectionState::Resubscribing,
            "socket upgrade",
        )
        .await;
        sock.emit_text(ack_text("book", Some("BTC/USD")));
        sock.emit_text(ack_text("book", Some("ETH/USD")));
        wait_for_state(
            &public_mirror,
            ConnectionState::Open,
            "both acks drive Open",
        )
        .await;

        bus.try_post_caller_inbound(CallerInbound::SubscriptionGuardDrop {
            handler_id: crate::dispatch::HandlerId(1),
            url: WsUrl::Public,
            channel: ChannelName::Book,
            symbols: vec![sym("BTC/USD"), sym("ETH/USD")],
        })
        .expect("post guard drop");

        let both_unsubscribed = async {
            loop {
                let syms: std::collections::BTreeSet<String> = sock
                    .sent_text()
                    .iter()
                    .filter_map(|body| {
                        let v: serde_json::Value = serde_json::from_str(body).ok()?;
                        if v.get("method")?.as_str()? != "unsubscribe" {
                            return None;
                        }
                        let params = v.get("params")?;
                        if params.get("channel")?.as_str()? != "book" {
                            return None;
                        }
                        params
                            .get("symbol")?
                            .as_array()?
                            .first()?
                            .as_str()
                            .map(str::to_owned)
                    })
                    .collect();
                if syms.len() >= 2 {
                    return syms;
                }
                tokio::task::yield_now().await;
            }
        };
        let syms = tokio::time::timeout(BUDGET, both_unsubscribed)
            .await
            .expect("timeout: one multi-pair guard drop did not unsubscribe every pair");
        assert!(
            syms.contains("BTC/USD") && syms.contains("ETH/USD"),
            "one multi-pair guard drop must unsubscribe every pair; got {syms:?}"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn register_batch_registers_all_entries_atomically_a373() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let entries: Vec<SubscriptionEntry> = ["BTC/USD", "ETH/USD", "SOL/USD"]
            .iter()
            .map(|p| {
                SubscriptionEntry::new(
                    WsUrl::Public,
                    ChannelName::Ticker,
                    Some(sym(p)),
                    ticker_params(),
                )
            })
            .collect();
        register_batch(&bus, WsUrl::Public, entries);

        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &public_mirror,
            ConnectionState::Resubscribing,
            "batch upgrade",
        )
        .await;

        let three = async {
            loop {
                let tickers: Vec<String> = sock
                    .sent_text()
                    .iter()
                    .filter_map(|body| {
                        let v: serde_json::Value = serde_json::from_str(body).ok()?;
                        if v.get("params")?.get("channel")?.as_str()? != "ticker" {
                            return None;
                        }
                        Some(v["params"]["symbol"][0].as_str()?.to_string())
                    })
                    .collect();
                if tickers.len() >= 3 {
                    return tickers;
                }
                tokio::task::yield_now().await;
            }
        };
        let mut tickers = tokio::time::timeout(BUDGET, three)
            .await
            .expect("timeout: batch did not emit all three ticker subscribes");
        tickers.sort();
        assert_eq!(
            tickers,
            vec![
                "BTC/USD".to_string(),
                "ETH/USD".to_string(),
                "SOL/USD".to_string()
            ],
            "RegisterBatch registered + emitted all three distinct entries atomically"
        );

        sock.emit_text(ack_text("ticker", Some("BTC/USD")));
        sock.emit_text(ack_text("ticker", Some("ETH/USD")));
        sock.emit_text(ack_text("ticker", Some("SOL/USD")));
        wait_for_state(
            &public_mirror,
            ConnectionState::Open,
            "batch acks drive Open",
        )
        .await;

        reactor.abort();
        bus.stop_reactors();
    }
}

#[cfg(test)]
mod wave3_send_fail_resilience_tests {
    use super::reconnect_drive_tests::ack_text;
    use super::*;
    use crate::conn::ManagedConnection;
    use crate::conn::rate_budget::ConnectionRateBudget;
    use crate::conn::subscription_registry::SubscriptionEntry;
    use crate::dispatch::DispatchEventBus;
    use crate::dispatch::event_bus::CallerInbound;
    use crate::transport::driveable_mock::{
        DriveableSocketHandle, DriveableWsSocketFactory, SendFailMode,
    };
    use crate::types::{ChannelName, ConnectionState, WsUrl};
    use std::time::Duration;

    const BUDGET: Duration = Duration::from_millis(2500);

    fn spawn_reactor(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
    ) -> (tokio::task::JoinHandle<()>, Arc<AtomicU8>) {
        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let public_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        state_mirrors.insert(WsUrl::Public, Arc::clone(&public_mirror));
        state_mirrors.insert(
            WsUrl::Auth,
            Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())),
        );
        let mut conns = HashMap::new();
        for (url, seed) in [(WsUrl::Public, 1u64), (WsUrl::Auth, 2u64)] {
            conns.insert(
                url,
                ManagedConnection::new(
                    url,
                    Arc::clone(bus),
                    Arc::clone(&dyn_factory),
                    Arc::clone(&rate_budget),
                    Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                    Arc::new(crate::jitter::SplitMix64Jitter::with_seed(seed)),
                ),
            );
        }
        let caller_rx = bus.take_caller_to_io_rx().expect("caller_to_io_rx");
        let presence_mirror: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handler_registry = crate::dispatch::HandlerRegistry::new(presence_mirror);
        let init = IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::new(),
            handler_registry,
            auth_stack: Arc::new(crate::auth::AuthStack::new(
                None,
                None,
                Arc::new(crate::auth::SystemClockNonceSource::new()),
                std::collections::HashMap::new(),
                crate::auth::TokenLifecycleManager::new(Arc::clone(bus), "<test-key>".to_string()),
            )),
            state_mirrors,
            bus_back_ref: Arc::downgrade(bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::new(crate::conn::OrderGate::default()),
        };
        (tokio::spawn(run(init)), public_mirror)
    }

    fn register_ticker(bus: &Arc<DispatchEventBus>, pair: &str) {
        register_batch(
            bus,
            WsUrl::Public,
            vec![SubscriptionEntry::new(
                WsUrl::Public,
                ChannelName::Ticker,
                Some(sym(pair)),
                ticker_params(),
            )],
        );
    }

    async fn await_socket(
        factory: &Arc<DriveableWsSocketFactory>,
        index: usize,
    ) -> DriveableSocketHandle {
        let poll = async {
            loop {
                if let Some(h) = factory.handle(index) {
                    return h;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: socket never opened")
    }

    fn ticker_subscribe_count(sock: &DriveableSocketHandle, pair: &str) -> usize {
        sock.sent_text()
            .iter()
            .filter(|body| {
                serde_json::from_str::<serde_json::Value>(body)
                    .ok()
                    .and_then(|v| {
                        let is_sub = v.get("method")?.as_str()? == "subscribe";
                        let is_tick = v.get("params")?.get("channel")?.as_str()? == "ticker";
                        let has_pair = v
                            .get("params")
                            .and_then(|p| p.get("symbol"))
                            .and_then(|s| s.as_array())
                            .map(|arr| arr.iter().any(|x| x.as_str() == Some(pair)))
                            .unwrap_or(false);
                        Some(is_sub && is_tick && has_pair)
                    })
                    .unwrap_or(false)
            })
            .count()
    }

    async fn reach_open(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
        public_mirror: &Arc<AtomicU8>,
    ) -> DriveableSocketHandle {
        register_ticker(bus, "BTC/USD");
        let sock = await_socket(factory, 0).await;
        wait_for_state(
            public_mirror,
            ConnectionState::Resubscribing,
            "initial upgrade",
        )
        .await;
        sock.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(public_mirror, ConnectionState::Open, "initial ack → Open").await;
        sock
    }

    #[tokio::test]
    async fn reconnect_replay_writer_closed_backs_off_instead_of_declaring_open() {
        let (bus, factory) = bus_and_factory();
        let reopen = Arc::new(AtomicU64::new(0));
        {
            let r = Arc::clone(&reopen);
            let _ = bus.subscribe(
                crate::dispatch::EventType::ConnectionReopenedEvent,
                Arc::new(move |_e: &crate::dispatch::EventEnvelope| {
                    r.fetch_add(1, Ordering::Relaxed);
                }),
                u16::MAX,
            );
        }
        let (reactor, public_mirror) = spawn_reactor(&bus, &factory);
        let sock0 = reach_open(&bus, &factory, &public_mirror).await;

        sock0.drop_with(TransportError {
            kind: TransportErrorKind::SocketReset,
            transient: true,
        });
        wait_for_state(&public_mirror, ConnectionState::BackingOff, "socket0 drop").await;

        factory.set_next_send_fail_mode(SendFailMode::WriterClosed);
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Public,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 42 },
        })
        .expect("post force reconnect");

        let _s1 = await_socket(&factory, 1).await;
        wait_for_state(
            &public_mirror,
            ConnectionState::BackingOff,
            "replay WriterClosed → reconnect",
        )
        .await;
        tokio::task::yield_now().await;
        assert_eq!(
            reopen.load(Ordering::Relaxed),
            0,
            "WriterClosed replay must back off, not declare the connection reopened over a dead socket"
        );
        reactor.abort();
    }

    #[tokio::test]
    async fn reconnect_replay_stops_sending_after_budget_exhaust_teardown() {
        let (bus, factory) = bus_and_factory();
        bus.knobs()
            .subscribe_ack_attempts
            .store(1, Ordering::Relaxed);
        let (reactor, public_mirror) = spawn_reactor(&bus, &factory);

        let sock0 = reach_open(&bus, &factory, &public_mirror).await;
        register_ticker(&bus, "ETH/USD");
        let got_eth = async {
            loop {
                if ticker_subscribe_count(&sock0, "ETH/USD") >= 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, got_eth)
            .await
            .expect("timeout: ETH/USD subscribe never sent while Open");
        sock0.emit_text(ack_text("ticker", Some("ETH/USD")));

        sock0.drop_with(TransportError {
            kind: TransportErrorKind::SocketReset,
            transient: true,
        });
        wait_for_state(&public_mirror, ConnectionState::BackingOff, "socket0 drop").await;

        factory.set_next_send_fail_mode(SendFailMode::Backpressure);
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Public,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 7 },
        })
        .expect("post force reconnect");

        let sock1 = await_socket(&factory, 1).await;
        wait_for_state(
            &public_mirror,
            ConnectionState::BackingOff,
            "replay budget-exhaust → BackingOff",
        )
        .await;
        tokio::task::yield_now().await;

        assert_eq!(
            sock1.send_attempts(),
            1,
            "replay must stop after the budget-exhaust teardown, not keep sending \
             for later registry entries on the abandoned socket"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn register_batch_stops_sending_but_keeps_registering_after_teardown() {
        let (bus, factory) = bus_and_factory();
        bus.knobs()
            .subscribe_ack_attempts
            .store(1, Ordering::Relaxed);
        let (reactor, public_mirror) = spawn_reactor(&bus, &factory);

        fn ticker_entry(pair: &str) -> SubscriptionEntry {
            SubscriptionEntry::new(
                WsUrl::Public,
                ChannelName::Ticker,
                Some(sym(pair)),
                ticker_params(),
            )
        }

        let dropped = Arc::new(AtomicU64::new(0));
        {
            let d = Arc::clone(&dropped);
            let _ = bus.subscribe(
                crate::dispatch::EventType::ConnectionDroppedEvent,
                Arc::new(move |_e: &crate::dispatch::EventEnvelope| {
                    d.fetch_add(1, Ordering::Relaxed);
                }),
                u16::MAX,
            );
        }

        let sock = reach_open(&bus, &factory, &public_mirror).await;
        let base = sock.send_attempts();
        sock.set_send_fail_mode(SendFailMode::Backpressure);
        register_batch(
            &bus,
            WsUrl::Public,
            vec![ticker_entry("ETH/USD"), ticker_entry("XRP/USD")],
        );

        let saw_drop = async {
            while dropped.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, saw_drop)
            .await
            .expect("timeout: mid-batch teardown (ConnectionDropped) never fired");
        tokio::task::yield_now().await;

        assert_eq!(
            sock.send_attempts(),
            base + 1,
            "RegisterBatch must not send later entries onto the abandoned socket"
        );

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Public,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 9 },
        })
        .expect("post force reconnect");
        let sock1 = await_socket(&factory, 1).await;
        let both_replayed = async {
            loop {
                if ticker_subscribe_count(&sock1, "ETH/USD") >= 1
                    && ticker_subscribe_count(&sock1, "XRP/USD") >= 1
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, both_replayed).await.expect(
            "timeout: reconnect replay did not restore both batch subscriptions \
             (a batch entry was dropped from the registry)",
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn upgrade_ok_is_live_matches_only_the_current_socket_cid() {
        use crate::transport::WsSocketFactoryLike;
        let (_bus, factory) = bus_and_factory();
        let sock = factory.open_socket(WsUrl::Public);
        let cid = sock.connection_id();
        let mut sockets: HashMap<WsUrl, Arc<dyn crate::transport::WsSocket>> = HashMap::new();
        sockets.insert(WsUrl::Public, sock);

        let live = |u, c| crate::dispatch::io_reactor::upgrade_ok_is_live(&sockets, u, c);
        assert!(
            live(WsUrl::Public, cid),
            "matching cid on the mapped socket is live"
        );
        assert!(
            !live(WsUrl::Public, cid + 1),
            "stale cid (socket replaced) is dropped"
        );
        assert!(
            !live(WsUrl::Auth, cid),
            "no socket mapped for the url is dropped"
        );
    }

    #[tokio::test]
    async fn w3_6_t1_backpressure_then_success_while_open() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_reactor(&bus, &factory);
        let sock = reach_open(&bus, &factory, &public_mirror).await;

        sock.set_send_fail_mode(SendFailMode::Backpressure);
        register_ticker(&bus, "ETH/USD");

        tokio::task::yield_now().await;

        sock.set_send_fail_mode(SendFailMode::Ok);

        let got_eth_sub = async {
            loop {
                if ticker_subscribe_count(&sock, "ETH/USD") >= 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, got_eth_sub)
            .await
            .expect("timeout: ETH/USD resend subscribe never sent after backpressure cleared");

        sock.emit_text(ack_text("ticker", Some("ETH/USD")));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        let state = ConnectionState::from_u8(public_mirror.load(Ordering::Acquire))
            .expect("valid state byte");
        assert_eq!(
            state,
            ConnectionState::Open,
            "must stay Open after dormant resend + ack"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn w3_6_t2_backpressure_exhaustion_escalates_to_backing_off() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_reactor(&bus, &factory);
        let sock = reach_open(&bus, &factory, &public_mirror).await;

        sock.set_send_fail_mode(SendFailMode::Backpressure);
        register_ticker(&bus, "ETH/USD");

        wait_for_state(
            &public_mirror,
            ConnectionState::BackingOff,
            "budget exhausted → BackingOff",
        )
        .await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn w3_6_t3_writer_closed_on_resend_escalates_to_backing_off() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_reactor(&bus, &factory);
        let sock = reach_open(&bus, &factory, &public_mirror).await;

        sock.set_send_fail_mode(SendFailMode::Backpressure);
        register_ticker(&bus, "ETH/USD");

        tokio::task::yield_now().await;

        sock.set_send_fail_mode(SendFailMode::WriterClosed);

        wait_for_state(
            &public_mirror,
            ConnectionState::BackingOff,
            "writer-closed resend → BackingOff",
        )
        .await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn w3_6_t4_dormant_retry_in_resubscribing_recovers() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_reactor(&bus, &factory);

        register_ticker(&bus, "BTC/USD");
        let sock = await_socket(&factory, 0).await;
        wait_for_state(
            &public_mirror,
            ConnectionState::Resubscribing,
            "BTC upgrade ok",
        )
        .await;

        sock.set_send_fail_mode(SendFailMode::Backpressure);
        register_ticker(&bus, "ETH/USD");

        tokio::task::yield_now().await;

        sock.set_send_fail_mode(SendFailMode::Ok);

        sock.emit_text(ack_text("ticker", Some("BTC/USD")));

        let got_eth_sub = async {
            loop {
                if ticker_subscribe_count(&sock, "ETH/USD") >= 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, got_eth_sub)
            .await
            .expect("timeout: ETH/USD dormant resend never sent");

        sock.emit_text(ack_text("ticker", Some("ETH/USD")));
        wait_for_state(
            &public_mirror,
            ConnectionState::Open,
            "ETH/USD dormant resend ack → Open",
        )
        .await;

        reactor.abort();
        bus.stop_reactors();
    }
}

#[cfg(test)]
mod auth_handshake_drive_tests {
    use super::*;
    use crate::clock::{Clock, SystemClock};
    use crate::conn::ManagedConnection;
    use crate::conn::rate_budget::ConnectionRateBudget;
    use crate::conn::subscription_registry::{
        EntrySubState, SubscribeParams, SubscriptionEntry, SubscriptionMirror,
    };
    use crate::dispatch::DispatchEventBus;
    use crate::dispatch::event_bus::{CallerInbound, RegistryMutationOp};
    use crate::transport::HttpTransport;
    use crate::transport::driveable_mock::DriveableWsSocketFactory;
    use crate::types::{BookDepth, ChannelName, ConnectionState, WsUrl};
    use std::sync::Mutex;
    use std::time::Duration;

    pub(super) fn ack_ok(channel: &str) -> String {
        serde_json::json!({
            "method": "subscribe",
            "success": true,
            "result": { "channel": channel },
        })
        .to_string()
    }

    pub(super) fn ack_err(channel: &str, error: &str) -> String {
        serde_json::json!({
            "method": "subscribe",
            "success": false,
            "error": error,
            "result": { "channel": channel },
        })
        .to_string()
    }

    /// AuthStack backed by `transport`; the returned RestSurface Arc must outlive the caller.
    pub(super) fn auth_stack_with_transport(
        bus: &Arc<DispatchEventBus>,
        seeded_token: &str,
        transport: Arc<dyn HttpTransport>,
    ) -> (Arc<crate::auth::AuthStack>, Arc<crate::rest::RestSurface>) {
        use crate::auth::{
            AuthStack, SpotRestHmacSha512Signer, SystemClockNonceSource, TokenLifecycleManager,
        };
        use crate::rate_limit::{SpotApiRateLimitTracker, SpotTradingRateLimitTracker, Tier};
        use crate::types::{ApiKey, ApiSecret, AuthProfile};
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD as BASE64;
        use std::collections::HashMap;

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let api_key = ApiKey::new("test-api-key");
        let secret = ApiSecret::from_base64(&BASE64.encode(vec![0x01u8; 32])).unwrap();
        let signer = SpotRestHmacSha512Signer::new(api_key.clone(), secret);
        let mut signers: HashMap<AuthProfile, Arc<dyn crate::auth::AuthSigner>> = HashMap::new();
        signers.insert(AuthProfile::SpotV1, Arc::new(signer));

        let auth = Arc::new(AuthStack::new(
            Some(api_key),
            None,
            Arc::new(SystemClockNonceSource::new()),
            signers,
            TokenLifecycleManager::new(Arc::clone(bus), "<test-key>".to_string()),
        ));
        auth.token_lifecycle()
            .seed_cached_token_for_test(seeded_token);

        let api_rl = Arc::new(SpotApiRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let trading_rl = Arc::new(SpotTradingRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let rest = Arc::new(crate::rest::RestSurface::new(
            transport,
            Arc::clone(&auth),
            api_rl,
            trading_rl,
            clock,
            std::time::Duration::from_secs(30),
        ));
        auth.token_lifecycle().set_rest(Arc::downgrade(&rest));
        (auth, rest)
    }

    pub(super) fn auth_stack_with_seeded_token(
        bus: &Arc<DispatchEventBus>,
        seeded_token: &str,
        refresh_envelope: serde_json::Value,
    ) -> (Arc<crate::auth::AuthStack>, Arc<crate::rest::RestSurface>) {
        auth_stack_with_transport(
            bus,
            seeded_token,
            Arc::new(ClosureHttpMock::canned(refresh_envelope)) as Arc<dyn HttpTransport>,
        )
    }

    fn spawn_reactor(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
        auth_stack: Arc<crate::auth::AuthStack>,
    ) -> (tokio::task::JoinHandle<()>, Arc<AtomicU8>) {
        spawn_reactor_with_sub_mirror(
            bus,
            factory,
            auth_stack,
            crate::conn::subscription_registry::SubscriptionMirror::default(),
        )
    }

    fn spawn_reactor_with_sub_mirror(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
        auth_stack: Arc<crate::auth::AuthStack>,
        sub_mirror: crate::conn::subscription_registry::SubscriptionMirror,
    ) -> (tokio::task::JoinHandle<()>, Arc<AtomicU8>) {
        let ReactorHandles {
            reactor,
            auth: auth_mirror,
            ..
        } = spawn_reactor_handles(bus, factory, auth_stack, sub_mirror);
        (reactor, auth_mirror)
    }

    pub(super) struct ReactorHandles {
        pub(super) reactor: tokio::task::JoinHandle<()>,
        pub(super) auth: Arc<AtomicU8>,
        pub(super) public: Arc<AtomicU8>,
        pub(super) unified_public: Arc<AtomicU8>,
        pub(super) order_gate: Arc<crate::conn::OrderGate>,
    }

    /// Reactor whose seeded token the test never asserts; keep the returned `RestSurface` alive.
    pub(super) fn spawn_unused_token_rig(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
    ) -> (
        ReactorHandles,
        crate::conn::subscription_registry::SubscriptionMirror,
        Arc<crate::rest::RestSurface>,
    ) {
        let (auth_stack, rest) =
            auth_stack_with_seeded_token(bus, "unused-token", token_envelope("unused"));
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let handles = spawn_reactor_handles(bus, factory, auth_stack, Arc::clone(&mirror));
        (handles, mirror, rest)
    }

    pub(super) fn spawn_reactor_handles(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
        auth_stack: Arc<crate::auth::AuthStack>,
        sub_mirror: crate::conn::subscription_registry::SubscriptionMirror,
    ) -> ReactorHandles {
        let order_gate = Arc::new(crate::conn::OrderGate::default());
        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let auth_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        let public_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        state_mirrors.insert(WsUrl::Public, Arc::clone(&public_mirror));
        state_mirrors.insert(WsUrl::Auth, Arc::clone(&auth_mirror));
        state_mirrors.insert(
            WsUrl::UnifiedPrivate,
            Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())),
        );
        state_mirrors.insert(
            WsUrl::UnifiedPublic,
            Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())),
        );
        let unified_public_mirror = Arc::clone(
            state_mirrors
                .get(&WsUrl::UnifiedPublic)
                .expect("Unified Public state mirror"),
        );

        let mut conns = HashMap::new();
        conns.insert(
            WsUrl::Public,
            ManagedConnection::new(
                WsUrl::Public,
                Arc::clone(bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::new(crate::jitter::SplitMix64Jitter::with_seed(1)),
            ),
        );
        conns.insert(
            WsUrl::Auth,
            ManagedConnection::new(
                WsUrl::Auth,
                Arc::clone(bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::new(crate::jitter::SplitMix64Jitter::with_seed(2)),
            ),
        );
        conns.insert(
            WsUrl::UnifiedPrivate,
            ManagedConnection::new(
                WsUrl::UnifiedPrivate,
                Arc::clone(bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::new(crate::jitter::SplitMix64Jitter::with_seed(3)),
            ),
        );
        conns.insert(
            WsUrl::UnifiedPublic,
            ManagedConnection::new(
                WsUrl::UnifiedPublic,
                Arc::clone(bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::new(crate::jitter::SplitMix64Jitter::with_seed(4)),
            ),
        );

        let caller_rx = bus
            .take_caller_to_io_rx()
            .expect("caller_to_io_rx available");
        let presence_mirror: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handler_registry = crate::dispatch::HandlerRegistry::new(presence_mirror);

        let init = IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::with_mirror(sub_mirror),
            handler_registry,
            auth_stack,
            state_mirrors,
            bus_back_ref: Arc::downgrade(bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::clone(&order_gate),
        };
        ReactorHandles {
            reactor: tokio::spawn(run(init)),
            auth: auth_mirror,
            public: public_mirror,
            unified_public: unified_public_mirror,
            order_gate,
        }
    }

    #[tokio::test]
    async fn unified_status_register_and_connect_opens_without_subscribe_or_idle_close() {
        let (bus, factory) = bus_and_factory();
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_public_mirror,
                ..
            },
            _,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let id = crate::dispatch::HandlerId(91);
        let handler = crate::dispatch::HandlerHandle::new(
            id,
            WsUrl::UnifiedPublic,
            ChannelName::Status,
            Arc::downgrade(&bus),
        );
        let status_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let decoder_calls = Arc::clone(&status_calls);
        bus.try_post_caller_inbound(CallerInbound::RegisterHandlerAndConnect {
            url: WsUrl::UnifiedPublic,
            channel: ChannelName::Status,
            id,
            callback: crate::dispatch::HandlerCallback::new(
                Arc::new(move |_frame| {
                    decoder_calls.fetch_add(1, Ordering::Relaxed);
                    crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))
                }),
                Arc::new(|_payload| {}),
            ),
        })
        .expect("post atomic Unified status registration");

        let socket = socket_with_at_least(&factory, WsUrl::UnifiedPublic, 0).await;
        wait_for_state(
            &unified_public_mirror,
            ConnectionState::Open,
            "Unified status registration",
        )
        .await;
        assert!(
            socket.sent_text().is_empty(),
            "unsolicited status opens Unified Public without a subscribe frame"
        );

        socket.emit_text(
            serde_json::json!({
                "channel": "status",
                "type": "update",
                "data": [{
                    "api_version": "v2",
                    "futures_exchange": "online",
                    "spot_exchange": "online",
                    "version": "2.0"
                }]
            })
            .to_string(),
        );
        tokio::time::timeout(BUDGET, async {
            while status_calls.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("registered status handler receives a Unified status frame");

        drop(handler);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            ConnectionState::from_u8(unified_public_mirror.load(Ordering::Acquire)),
            Some(ConnectionState::Open),
            "dropping the final status handler must not idle-close Unified Public"
        );
        assert!(
            socket.sent_text().is_empty(),
            "dropping the final status handler must not send a teardown frame"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_public_register_batch_emits_one_multi_symbol_frame() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "unused-unified-public-token",
            token_envelope("unused"),
        );
        let (reactor, _auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);
        let entries = ["PI_XBTUSD", "PF_ETHUSD"]
            .into_iter()
            .map(|symbol| {
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ticker,
                    Some(StreamSymbol::Futures(symbol.to_string())),
                    SubscribeParams::Ticker {
                        snapshot: Some(true),
                        event_trigger: None,
                    },
                )
            })
            .collect();
        register_batch(&bus, WsUrl::UnifiedPublic, entries);

        let frame = tokio::time::timeout(BUDGET, async {
            loop {
                if let Some(socket) = factory.handle(0)
                    && let Some(body) = socket.sent_text().first()
                {
                    return serde_json::from_str::<serde_json::Value>(body)
                        .expect("Unified subscribe frame JSON");
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Unified batch must emit one frame");

        assert_eq!(frame["method"], "subscribe");
        assert_eq!(frame["params"]["channel"], "ticker");
        assert_eq!(frame["params"]["asset_class"], "derivatives");
        assert_eq!(
            frame["params"]["symbol"],
            serde_json::json!(["PI_XBTUSD", "PF_ETHUSD"])
        );
        let socket = factory.handle(0).expect("Unified socket opened");
        assert_eq!(
            socket.sent_text().len(),
            1,
            "same-channel/same-params Unified entries share one outgoing subscribe"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_initial_batch_backpressure_retries_every_symbol() {
        use crate::conn::subscription_registry::EntrySubState;

        let (bus, factory) = bus_and_factory();
        factory
            .set_next_send_fail_mode(crate::transport::driveable_mock::SendFailMode::Backpressure);
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_state,
                ..
            },
            mirror,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let symbols = ["PI_XBTUSD", "PF_ETHUSD"];
        let entries = symbols
            .iter()
            .map(|symbol| {
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ticker,
                    Some(StreamSymbol::Futures((*symbol).to_owned())),
                    SubscribeParams::Ticker {
                        snapshot: Some(true),
                        event_trigger: None,
                    },
                )
            })
            .collect();
        register_batch(&bus, WsUrl::UnifiedPublic, entries);

        let socket = socket_with_at_least(&factory, WsUrl::UnifiedPublic, 0).await;
        tokio::task::yield_now().await;
        socket.set_send_fail_mode(crate::transport::driveable_mock::SendFailMode::Ok);
        tokio::time::timeout(BUDGET, async {
            loop {
                let retries: Vec<serde_json::Value> = socket
                    .sent_text()
                    .iter()
                    .map(|frame| serde_json::from_str(frame).expect("retry frame JSON"))
                    .collect();
                if symbols.iter().all(|symbol| {
                    retries
                        .iter()
                        .any(|frame| frame["params"]["symbol"] == serde_json::json!([symbol]))
                }) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("initial backpressure must arm a retry for every Unified symbol");

        for symbol in symbols {
            socket.emit_text(super::reconnect_drive_tests::ack_text(
                "ticker",
                Some(symbol),
            ));
        }
        wait_for_state(
            &unified_state,
            ConnectionState::Open,
            "all initially retried Unified symbols acknowledged",
        )
        .await;
        for symbol in symbols {
            assert!(
                mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(
                        WsUrl::UnifiedPublic,
                        ChannelName::Ticker,
                        Some(StreamSymbol::Futures(symbol.to_owned())),
                    ))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked)),
                "{symbol} must recover from its own initial-send backpressure timer"
            );
        }

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_batch_backpressure_retries_every_symbol_after_reconnect() {
        use crate::conn::subscription_registry::EntrySubState;

        let (bus, factory) = bus_and_factory();
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_state,
                ..
            },
            mirror,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let symbols = ["PI_XBTUSD", "PF_ETHUSD"];
        let entries = symbols
            .iter()
            .map(|symbol| {
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ticker,
                    Some(StreamSymbol::Futures((*symbol).to_owned())),
                    SubscribeParams::Ticker {
                        snapshot: Some(true),
                        event_trigger: None,
                    },
                )
            })
            .collect();
        register_batch(&bus, WsUrl::UnifiedPublic, entries);

        let socket0 = socket_with_at_least(&factory, WsUrl::UnifiedPublic, 1).await;
        for symbol in symbols {
            socket0.emit_text(super::reconnect_drive_tests::ack_text(
                "ticker",
                Some(symbol),
            ));
        }
        wait_for_state(
            &unified_state,
            ConnectionState::Open,
            "initial Unified batch acks",
        )
        .await;

        socket0.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        wait_for_state(
            &unified_state,
            ConnectionState::BackingOff,
            "initial Unified socket drop",
        )
        .await;

        factory
            .set_next_send_fail_mode(crate::transport::driveable_mock::SendFailMode::Backpressure);
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::UnifiedPublic,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 7 },
        })
        .expect("force Unified reconnect");
        let socket1 = socket_at(&factory, 1).await;

        tokio::task::yield_now().await;
        socket1.set_send_fail_mode(crate::transport::driveable_mock::SendFailMode::Ok);
        tokio::time::timeout(BUDGET, async {
            loop {
                let retries: Vec<serde_json::Value> = socket1
                    .sent_text()
                    .iter()
                    .map(|frame| serde_json::from_str(frame).expect("retry frame JSON"))
                    .collect();
                if symbols.iter().all(|symbol| {
                    retries
                        .iter()
                        .any(|frame| frame["params"]["symbol"] == serde_json::json!([symbol]))
                }) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("backpressured Unified replay must arm a retry for every symbol");

        for symbol in symbols {
            socket1.emit_text(super::reconnect_drive_tests::ack_text(
                "ticker",
                Some(symbol),
            ));
        }
        wait_for_state(
            &unified_state,
            ConnectionState::Open,
            "all retried Unified symbols acknowledged",
        )
        .await;
        for symbol in symbols {
            let key = (
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(StreamSymbol::Futures(symbol.to_owned())),
            );
            assert!(
                mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&key)
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked)),
                "{symbol} must recover from its own replay backpressure timer"
            );
        }

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_ohlc_intervals_and_tick_types_subscribe_ack_unsubscribe_and_replay_apart() {
        use crate::types::{FuturesOhlcTickType, OhlcInterval};

        let (bus, factory) = bus_and_factory();
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_state,
                ..
            },
            mirror,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let key = |interval, tick_type| {
            Some(StreamSymbol::futures_ohlc(
                "PF_XBTUSD".to_string(),
                interval,
                tick_type,
            ))
        };
        let wire = |frames: Vec<String>| {
            let mut wire: Vec<(u64, Option<String>)> = frames
                .iter()
                .map(|frame| {
                    let frame: serde_json::Value =
                        serde_json::from_str(frame).expect("ohlc frame JSON");
                    assert_eq!(frame["params"]["symbol"], serde_json::json!(["PF_XBTUSD"]));
                    (
                        frame["params"]["interval"].as_u64().expect("interval"),
                        frame["params"]["tick_type"].as_str().map(str::to_string),
                    )
                })
                .collect();
            wire.sort();
            wire
        };
        let entries = [
            (OhlcInterval::M1, None),
            (OhlcInterval::M5, None),
            (OhlcInterval::M1, Some(FuturesOhlcTickType::Mark)),
        ]
        .into_iter()
        .map(|(interval, tick_type)| {
            let params = SubscribeParams::Ohlc {
                interval,
                snapshot: None,
                tick_type,
            };
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ohlc,
                key(interval, tick_type),
                params,
            )
        })
        .collect();
        register_batch(&bus, WsUrl::UnifiedPublic, entries);

        let socket0 = socket_with_at_least(&factory, WsUrl::UnifiedPublic, 0).await;
        let subscribes =
            wait_for_frames(&socket0, "\"subscribe\"", 3, "three OHLC subscribes").await;
        assert_eq!(
            wire(subscribes),
            vec![(1, None), (1, Some("mark".to_string())), (5, None)]
        );

        for (interval, tick_type) in [(1, "trade"), (5, "trade"), (1, "mark")] {
            socket0.emit_text(
                serde_json::json!({
                    "method": "subscribe",
                    "success": true,
                    "result": {
                        "channel": "ohlc",
                        "interval": interval,
                        "snapshot": true,
                        "symbol": "PF_XBTUSD",
                        "tick_type": tick_type,
                    },
                })
                .to_string(),
            );
        }
        wait_for_state(
            &unified_state,
            ConnectionState::Open,
            "every OHLC identity acknowledged",
        )
        .await;
        let acked = |interval, tick_type| {
            mirror
                .read()
                .expect("subscription mirror")
                .get(&(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ohlc,
                    key(interval, tick_type),
                ))
                .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
        };
        assert!(acked(OhlcInterval::M1, None));
        assert!(acked(OhlcInterval::M5, None));
        assert!(acked(OhlcInterval::M1, Some(FuturesOhlcTickType::Mark)));

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::UnifiedPublic,
            mutation: RegistryMutationOp::DeregisterBatch {
                channel: ChannelName::Ohlc,
                pairs: key(OhlcInterval::M5, None).into_iter().collect(),
            },
        })
        .expect("post M5 unsubscribe");
        let unsubscribes = wait_for_frames(&socket0, "unsubscribe", 1, "M5 unsubscribe").await;
        assert_eq!(wire(unsubscribes), vec![(5, None)]);
        assert!(acked(OhlcInterval::M1, None));
        assert!(acked(OhlcInterval::M1, Some(FuturesOhlcTickType::Mark)));
        assert_eq!(mirror.read().expect("subscription mirror").len(), 2);

        socket0.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        wait_for_state(
            &unified_state,
            ConnectionState::BackingOff,
            "Unified socket drop",
        )
        .await;
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::UnifiedPublic,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 7 },
        })
        .expect("force Unified reconnect");
        let socket1 = socket_at(&factory, 1).await;
        let replayed = wait_for_frames(&socket1, "\"subscribe\"", 2, "OHLC replay").await;
        assert_eq!(
            wire(replayed),
            vec![(1, None), (1, Some("mark".to_string()))]
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_writer_closed_batch_stops_groups_and_replays_every_entry() {
        use crate::conn::subscription_registry::EntrySubState;

        let (bus, factory) = bus_and_factory();
        factory
            .set_next_send_fail_mode(crate::transport::driveable_mock::SendFailMode::WriterClosed);
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_state,
                ..
            },
            mirror,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let ticker_symbols = ["PI_XBTUSD", "PF_ETHUSD"];
        let book_symbol = "PI_XBTUSD";
        let mut entries: Vec<_> = ticker_symbols
            .iter()
            .map(|symbol| {
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ticker,
                    Some(StreamSymbol::Futures((*symbol).to_owned())),
                    SubscribeParams::Ticker {
                        snapshot: Some(true),
                        event_trigger: None,
                    },
                )
            })
            .collect();
        entries.push(SubscriptionEntry::new(
            WsUrl::UnifiedPublic,
            ChannelName::Book,
            Some(StreamSymbol::Futures(book_symbol.to_owned())),
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        ));
        register_batch(&bus, WsUrl::UnifiedPublic, entries);

        let socket0 = socket_with_at_least(&factory, WsUrl::UnifiedPublic, 0).await;
        factory.set_next_send_fail_mode(crate::transport::driveable_mock::SendFailMode::Ok);
        wait_for_state(
            &unified_state,
            ConnectionState::BackingOff,
            "WriterClosed batch send",
        )
        .await;
        assert_eq!(
            socket0.send_attempts(),
            1,
            "WriterClosed must stop additional grouped sends on the dead socket"
        );

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::UnifiedPublic,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 9 },
        })
        .expect("force Unified reconnect");
        let socket1 = socket_at(&factory, 1).await;
        let replay_frames = tokio::time::timeout(BUDGET, async {
            loop {
                let frames: Vec<serde_json::Value> = socket1
                    .sent_text()
                    .iter()
                    .map(|frame| serde_json::from_str(frame).expect("replay frame JSON"))
                    .collect();
                if frames.len() == 2 {
                    return frames;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all grouped entries must replay on the reconnected socket");
        assert!(replay_frames.iter().any(|frame| {
            frame["params"]["channel"] == "ticker"
                && ticker_symbols.iter().all(|symbol| {
                    frame["params"]["symbol"]
                        .as_array()
                        .is_some_and(|symbols| symbols.iter().any(|value| value == symbol))
                })
        }));
        assert!(replay_frames.iter().any(|frame| {
            frame["params"]["channel"] == "book"
                && frame["params"]["symbol"] == serde_json::json!([book_symbol])
        }));
        for frame in replay_frames {
            let channel = frame["params"]["channel"].as_str().expect("replay channel");
            for symbol in frame["params"]["symbol"]
                .as_array()
                .expect("replay symbols")
                .iter()
                .filter_map(serde_json::Value::as_str)
            {
                socket1.emit_text(super::reconnect_drive_tests::ack_text(
                    channel,
                    Some(symbol),
                ));
            }
        }
        wait_for_state(
            &unified_state,
            ConnectionState::Open,
            "all replayed Unified entries acknowledged",
        )
        .await;
        for (channel, symbol) in [
            (ChannelName::Ticker, "PI_XBTUSD"),
            (ChannelName::Ticker, "PF_ETHUSD"),
            (ChannelName::Book, book_symbol),
        ] {
            assert!(
                mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(
                        WsUrl::UnifiedPublic,
                        channel,
                        Some(StreamSymbol::Futures(symbol.to_owned())),
                    ))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked)),
                "{channel:?}/{symbol} must survive WriterClosed and replay"
            );
        }

        reactor.abort();
        bus.stop_reactors();
    }

    async fn socket_lost_before_open_replays_the_queued_subscribe(
        url: WsUrl,
        pair: StreamSymbol,
        wire_symbol: &str,
    ) {
        let (bus, factory) = bus_and_factory();
        factory.set_next_upgrade_failure(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::HttpUpgradeRejected { status: 503 },
            transient: true,
        });
        let (
            ReactorHandles {
                reactor,
                public: public_state,
                unified_public: unified_state,
                ..
            },
            _,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let state = if url == WsUrl::Public {
            public_state
        } else {
            unified_state
        };

        register_batch(
            &bus,
            url,
            vec![SubscriptionEntry::new(
                url,
                ChannelName::Ticker,
                Some(pair),
                SubscribeParams::Ticker {
                    snapshot: Some(true),
                    event_trigger: None,
                },
            )],
        );
        wait_for_state(
            &state,
            ConnectionState::BackingOff,
            "first upgrade failure after the subscribe was queued",
        )
        .await;
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 2 },
        })
        .expect("reconnect");
        let socket = tokio::time::timeout(BUDGET, async {
            loop {
                if let Some(socket) = factory.handle(1)
                    && socket.sent_text().len() == 1
                {
                    return socket;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second socket replays the queued subscribe");

        assert!(socket.sent_text()[0].contains(wire_symbol));
        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_public_socket_lost_before_open_replays_the_queued_subscribe_on_the_next_upgrade()
     {
        socket_lost_before_open_replays_the_queued_subscribe(
            WsUrl::UnifiedPublic,
            StreamSymbol::Futures("PI_XBTUSD".to_owned()),
            "PI_XBTUSD",
        )
        .await;
    }

    #[tokio::test]
    async fn spot_public_socket_lost_before_open_replays_the_queued_subscribe_on_the_next_upgrade()
    {
        socket_lost_before_open_replays_the_queued_subscribe(
            WsUrl::Public,
            StreamSymbol::Spot(Symbol::new("BTC/USD").expect("valid pair")),
            "BTC/USD",
        )
        .await;
    }

    async fn spot_subscribe_frames_after_a_backpressured_first_send(pairs: &[&str]) -> Vec<String> {
        use crate::conn::subscription_registry::SubscriptionMirror;
        use crate::transport::driveable_mock::SendFailMode;

        let (bus, factory) = bus_and_factory();
        factory.set_next_send_fail_mode(SendFailMode::BackpressureOnce);
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unused-public-token", token_envelope("unused"));
        let ReactorHandles { reactor, .. } =
            spawn_reactor_handles(&bus, &factory, auth_stack, SubscriptionMirror::default());

        let entries = pairs
            .iter()
            .map(|pair| {
                SubscriptionEntry::new(
                    WsUrl::Public,
                    ChannelName::Ticker,
                    Some(StreamSymbol::Spot(Symbol::new(*pair).expect("valid pair"))),
                    SubscribeParams::Ticker {
                        snapshot: Some(true),
                        event_trigger: None,
                    },
                )
            })
            .collect();
        register_batch(&bus, WsUrl::Public, entries);
        let socket = socket_with_at_least(&factory, WsUrl::Public, 0).await;
        tokio::time::timeout(BUDGET, async {
            while socket.sent_text().len() < pairs.len() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("every subscribe reaches the socket");
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        let frames = socket.sent_text();
        reactor.abort();
        bus.stop_reactors();
        frames
    }

    fn frames_naming(frames: &[String], pair: &str) -> usize {
        frames.iter().filter(|frame| frame.contains(pair)).count()
    }

    #[tokio::test]
    async fn a_backpressured_first_spot_subscribe_is_sent_once_by_the_replay_with_no_leaked_resend()
    {
        let frames = spot_subscribe_frames_after_a_backpressured_first_send(&["BTC/USD"]).await;

        assert_eq!(
            frames_naming(&frames, "BTC/USD"),
            1,
            "the replay covers the armed resend, so the subscribe goes out once"
        );
    }

    #[tokio::test]
    async fn the_spot_replay_after_a_backpressured_first_send_skips_what_already_went_out() {
        let frames =
            spot_subscribe_frames_after_a_backpressured_first_send(&["BTC/USD", "ETH/USD"]).await;

        assert_eq!(
            (
                frames_naming(&frames, "BTC/USD"),
                frames_naming(&frames, "ETH/USD")
            ),
            (1, 1),
            "only the backpressured subscribe is replayed: {frames:?}"
        );
    }

    #[tokio::test]
    async fn guard_drop_of_a_maintained_book_keeps_the_shared_stream_while_a_raw_peer_is_live() {
        use crate::dispatch::HandlerId;

        let (bus, factory) = bus_and_factory();
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_state,
                ..
            },
            mirror,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let terminated = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&terminated);
        let _subscription = bus.subscribe(
            EventType::FuturesSubscriptionTerminatedEvent,
            Arc::new(move |env: &EventEnvelope| {
                if let EventPayload::FuturesSubscriptionTerminatedEvent { channel, .. } =
                    &env.payload
                {
                    sink.lock().expect("terminated").push(*channel);
                }
            }),
            u16::MAX,
        );
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_owned());
        for (id, channel, params) in [
            (
                1,
                ChannelName::Book,
                SubscribeParams::Book {
                    depth: BookDepth::D10,
                },
            ),
            (
                2,
                ChannelName::BookRaw,
                SubscribeParams::BookRaw {
                    depth: BookDepth::D10,
                    snapshot: None,
                },
            ),
        ] {
            bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::UnifiedPublic,
                mutation: RegistryMutationOp::RegisterBatch {
                    entries: vec![SubscriptionEntry::new(
                        WsUrl::UnifiedPublic,
                        channel,
                        Some(pair.clone()),
                        params,
                    )],
                    ref_id: Some(HandlerId(id)),
                },
            })
            .expect("register one owner of the shared book stream");
        }
        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
        socket.emit_text(super::reconnect_drive_tests::ack_text(
            "book",
            Some("PI_XBTUSD"),
        ));
        wait_for_state(&unified_state, ConnectionState::Open, "shared book acked").await;

        let drop_guard = |id: u64, channel: ChannelName| {
            bus.try_post_caller_inbound(CallerInbound::SubscriptionGuardDrop {
                handler_id: HandlerId(id),
                url: WsUrl::UnifiedPublic,
                channel,
                symbols: vec![pair.clone()],
            })
            .expect("post guard drop");
        };
        let unsubscribes = || {
            socket
                .sent_text()
                .iter()
                .filter(|frame| frame.contains("unsubscribe"))
                .count()
        };
        drop_guard(1, ChannelName::Book);
        tokio::time::timeout(BUDGET, async {
            while terminated.lock().expect("terminated").is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the released maintained book is terminated");
        assert_eq!(unsubscribes(), 0, "the raw peer still owns the wire stream");
        assert_eq!(
            *terminated.lock().expect("terminated"),
            vec![ChannelName::Book]
        );

        drop_guard(2, ChannelName::BookRaw);
        tokio::time::timeout(BUDGET, async {
            while terminated.lock().expect("terminated").len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the released raw book is terminated");
        assert_eq!(unsubscribes(), 1, "the last owner unsubscribes once");
        assert!(
            mirror
                .read()
                .expect("subscription mirror")
                .get(&(
                    WsUrl::UnifiedPublic,
                    ChannelName::BookRaw,
                    Some(pair.clone())
                ))
                .is_none()
        );
        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_seed_held_maintained_book_counts_as_acked_for_raw_release_and_late_raw_join() {
        use crate::conn::subscription_registry::EntrySubState;
        use crate::dispatch::HandlerId;

        let (bus, factory) = bus_and_factory();
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_state,
                ..
            },
            mirror,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let terminated = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&terminated);
        let _subscription = bus.subscribe(
            EventType::FuturesSubscriptionTerminatedEvent,
            Arc::new(move |env: &EventEnvelope| {
                if let EventPayload::FuturesSubscriptionTerminatedEvent { channel, .. } =
                    &env.payload
                {
                    sink.lock().expect("terminated").push(*channel);
                }
            }),
            u16::MAX,
        );
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_owned());
        let register = |id: u64, channel: ChannelName, params: SubscribeParams| {
            bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::UnifiedPublic,
                mutation: RegistryMutationOp::RegisterBatch {
                    entries: vec![SubscriptionEntry::new(
                        WsUrl::UnifiedPublic,
                        channel,
                        Some(pair.clone()),
                        params,
                    )],
                    ref_id: Some(HandlerId(id)),
                },
            })
            .expect("register one owner of the shared book stream");
        };
        register(
            2,
            ChannelName::BookRaw,
            SubscribeParams::BookRaw {
                depth: BookDepth::D10,
                snapshot: None,
            },
        );
        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
        socket.emit_text(super::reconnect_drive_tests::ack_text(
            "book",
            Some("PI_XBTUSD"),
        ));
        wait_for_state(&unified_state, ConnectionState::Open, "raw book acked").await;
        register(
            1,
            ChannelName::Book,
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        );
        wait_for_frames(
            &socket,
            "\"subscribe\"",
            2,
            "the late maintained join reseeds the stream",
        )
        .await;
        socket.emit_text(super::reconnect_drive_tests::ack_text(
            "book",
            Some("PI_XBTUSD"),
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let sent_before = socket.sent_text().len();

        bus.try_post_caller_inbound(CallerInbound::SubscriptionGuardDrop {
            handler_id: HandlerId(2),
            url: WsUrl::UnifiedPublic,
            channel: ChannelName::BookRaw,
            symbols: vec![pair.clone()],
        })
        .expect("release the raw owner");
        tokio::time::timeout(BUDGET, async {
            while terminated.lock().expect("terminated").is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the released raw book is terminated");

        let sent = socket.sent_text();
        assert!(
            !sent[sent_before..]
                .iter()
                .any(|frame| frame.contains("\"subscribe\"")),
            "a live stream is not subscribed again: {:?}",
            &sent[sent_before..]
        );

        register(
            3,
            ChannelName::BookRaw,
            SubscribeParams::BookRaw {
                depth: BookDepth::D10,
                snapshot: None,
            },
        );
        tokio::time::timeout(BUDGET, async {
            loop {
                if mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(
                        WsUrl::UnifiedPublic,
                        ChannelName::BookRaw,
                        Some(pair.clone()),
                    ))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the late raw owner attaches to the acked stream");
        assert_eq!(
            socket.sent_text().len(),
            sent.len(),
            "attaching sends nothing"
        );
        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_initial_absent_socket_replays_pending_ticker_trade_and_book() {
        use crate::conn::subscription_registry::EntrySubState;

        let (bus, factory) = bus_and_factory();
        factory.set_next_upgrade_failure(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::HttpUpgradeRejected { status: 503 },
            transient: true,
        });
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_state,
                ..
            },
            mirror,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::UnifiedPublic,
            event: crate::types::CallerEvent::StartConnect { request_id: 1 },
        })
        .expect("start failed first Unified connection");
        wait_for_state(
            &unified_state,
            ConnectionState::BackingOff,
            "first Unified connection upgrade failure",
        )
        .await;

        let entries = vec![
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_owned())),
                SubscribeParams::Ticker {
                    snapshot: Some(true),
                    event_trigger: None,
                },
            ),
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(StreamSymbol::Futures("PF_ETHUSD".to_owned())),
                SubscribeParams::Ticker {
                    snapshot: Some(true),
                    event_trigger: None,
                },
            ),
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Trade,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_owned())),
                SubscribeParams::Trade {
                    snapshot: Some(true),
                },
            ),
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Book,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_owned())),
                SubscribeParams::Book {
                    depth: BookDepth::D10,
                },
            ),
        ];
        register_batch(&bus, WsUrl::UnifiedPublic, entries);

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::UnifiedPublic,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 2 },
        })
        .expect("reconnect Unified after first failure");
        let socket = tokio::time::timeout(BUDGET, async {
            loop {
                if let Some(socket) = factory.handle(1)
                    && socket.sent_text().len() == 3
                {
                    return socket;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reconnected Unified socket replays pending entries");

        let frames: Vec<serde_json::Value> = socket
            .sent_text()
            .iter()
            .map(|frame| serde_json::from_str(frame).expect("replay frame JSON"))
            .collect();
        assert_eq!(
            frames
                .iter()
                .filter(|frame| frame["params"]["channel"] == "ticker")
                .count(),
            1,
            "both pending ticker entries must share one replay batch"
        );
        assert!(frames.iter().any(|frame| {
            frame["params"]["channel"] == "ticker"
                && frame["params"]["symbol"].as_array().is_some_and(|symbols| {
                    symbols.len() == 2
                        && symbols.iter().any(|symbol| symbol == "PI_XBTUSD")
                        && symbols.iter().any(|symbol| symbol == "PF_ETHUSD")
                })
        }));
        assert!(frames.iter().any(|frame| {
            frame["params"]["channel"] == "trade"
                && frame["params"]["symbol"] == serde_json::json!(["PI_XBTUSD"])
        }));
        assert!(frames.iter().any(|frame| {
            frame["params"]["channel"] == "book"
                && frame["params"]["symbol"] == serde_json::json!(["PI_XBTUSD"])
        }));

        for frame in frames {
            let channel = frame["params"]["channel"].as_str().expect("channel");
            for symbol in frame["params"]["symbol"]
                .as_array()
                .expect("symbols")
                .iter()
                .filter_map(serde_json::Value::as_str)
            {
                socket.emit_text(super::reconnect_drive_tests::ack_text(
                    channel,
                    Some(symbol),
                ));
            }
        }
        wait_for_state(
            &unified_state,
            ConnectionState::Open,
            "all initially absent-socket Unified entries acknowledged",
        )
        .await;

        for (channel, symbol) in [
            (ChannelName::Ticker, "PI_XBTUSD"),
            (ChannelName::Ticker, "PF_ETHUSD"),
            (ChannelName::Trade, "PI_XBTUSD"),
            (ChannelName::Book, "PI_XBTUSD"),
        ] {
            assert!(
                mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(
                        WsUrl::UnifiedPublic,
                        channel,
                        Some(StreamSymbol::Futures(symbol.to_owned())),
                    ))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked)),
                "{channel:?}/{symbol} must not remain pending after replay"
            );
        }

        reactor.abort();
        bus.stop_reactors();
    }

    async fn nth_sent_token(
        sock: &crate::transport::driveable_mock::DriveableSocketHandle,
        n: usize,
    ) -> (String, String) {
        let poll = async {
            loop {
                let sent = sock.sent_text();
                if sent.len() > n {
                    let v: serde_json::Value =
                        serde_json::from_str(&sent[n]).expect("sent frame JSON");
                    let token = v["params"]["token"].as_str().unwrap_or("").to_string();
                    let channel = v["params"]["channel"].as_str().unwrap_or("").to_string();
                    return (channel, token);
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: auth socket never sent frame #{n}"))
    }

    /// Asserts the sent-frame count is exactly `expected` through a settle window.
    async fn assert_exactly_n_sent(
        sock: &crate::transport::driveable_mock::DriveableSocketHandle,
        expected: usize,
        what: &str,
    ) {
        let reach = async {
            loop {
                let len = sock.sent_text().len();
                assert!(
                    len <= expected,
                    "{what}: sent {len} frames, expected exactly {expected} (overshoot — double-send regression)"
                );
                if len == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, reach)
            .await
            .unwrap_or_else(|_| {
                panic!("timeout: auth socket never reached {expected} sent frames ({what})")
            });
        let settle = async {
            loop {
                let len = sock.sent_text().len();
                assert_eq!(
                    len, expected,
                    "{what}: sent {len} frames after settle, expected exactly {expected}"
                );
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(100), settle).await;
        assert_eq!(
            sock.sent_text().len(),
            expected,
            "{what}: final sent-frame count must be exactly {expected}"
        );
    }

    #[tokio::test]
    async fn test_a_happy_auth_path_drives_to_open() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-A", token_envelope("unused"));
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);

        register_executions(&bus);

        let auth_sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;

        let (channel, token) = nth_sent_token(&auth_sock, 0).await;
        assert_eq!(
            channel, "executions",
            "first signed subscribe is executions"
        );
        assert_eq!(token, "seeded-token-A", "seeded token injected into params");

        assert_exactly_n_sent(&auth_sock, 1, "before success ack").await;

        auth_sock.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;
        assert_exactly_n_sent(&auth_sock, 1, "after open (single-sub handshake)").await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn first_signed_auth_ack_drives_mirror_row_acked() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-M", token_envelope("unused"));
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) =
            spawn_reactor_with_sub_mirror(&bus, &factory, auth_stack, Arc::clone(&sub_mirror));

        register_executions(&bus);

        let auth_sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        {
            let m = sub_mirror.read().unwrap();
            let row = m
                .get(&(WsUrl::Auth, ChannelName::Executions, None))
                .expect("row registered");
            assert!(matches!(row.state, EntrySubState::Pending));
        }

        auth_sock.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(WsUrl::Auth, ChannelName::Executions, None))
                        .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                    {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: first signed auth entry never reached Acked");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn channel_wide_guard_drop_releases_entry_end_to_end() {
        use crate::conn::subscription_registry::SubscriptionMirror;
        let (bus, factory) = bus_and_factory();
        let terminated = Arc::new(AtomicU64::new(0));
        {
            let t = Arc::clone(&terminated);
            let _ = bus.subscribe(
                crate::dispatch::EventType::SubscriptionTerminatedEvent,
                Arc::new(move |_env: &crate::dispatch::EventEnvelope| {
                    t.fetch_add(1, Ordering::Relaxed);
                }),
                u16::MAX,
            );
        }
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-G", token_envelope("unused"));
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) =
            spawn_reactor_with_sub_mirror(&bus, &factory, auth_stack, Arc::clone(&sub_mirror));

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: Some(crate::dispatch::HandlerId(1)),
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions {
                        snap_orders: None,
                        snap_trades: None,
                    },
                )],
            },
        })
        .expect("post register");

        let auth_sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        auth_sock.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;

        bus.try_post_caller_inbound(CallerInbound::SubscriptionGuardDrop {
            handler_id: crate::dispatch::HandlerId(1),
            url: WsUrl::Auth,
            channel: ChannelName::Executions,
            symbols: vec![],
        })
        .expect("post guard drop");

        let find_unsub = async {
            loop {
                if let Some(params) = auth_sock.sent_text().iter().find_map(|body| {
                    let v: serde_json::Value = serde_json::from_str(body).ok()?;
                    if v.get("method")?.as_str()? != "unsubscribe" {
                        return None;
                    }
                    let params = v.get("params")?;
                    if params.get("channel")?.as_str()? != "executions" {
                        return None;
                    }
                    Some(params.clone())
                }) {
                    return params;
                }
                tokio::task::yield_now().await;
            }
        };
        let params = tokio::time::timeout(BUDGET, find_unsub)
            .await
            .expect("timeout: channel-wide executions unsubscribe frame never sent");
        assert!(
            params.get("symbol").is_none(),
            "channel-wide unsubscribe must be symbol-less, got {params}"
        );

        let released = async {
            loop {
                let row_gone = !sub_mirror.read().expect("mirror lock").contains_key(&(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                ));
                if row_gone && terminated.load(Ordering::Relaxed) == 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, released)
            .await
            .expect("timeout: terminated event + mirror-row removal never observed");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn futures_unsubscribe_emits_native_symbol_termination_event() {
        use crate::FuturesSubscriptionNamespace;
        use crate::api::ws_surface::WsSurface;
        use crate::conn::subscription_registry::SubscriptionEntry;

        let (bus, factory) = bus_and_factory();
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_state,
                ..
            },
            mirror,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let terminated = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = Arc::clone(&terminated);
        let _termination_subscription = bus.subscribe(
            EventType::FuturesSubscriptionTerminatedEvent,
            Arc::new(move |event| {
                if let EventPayload::FuturesSubscriptionTerminatedEvent {
                    channel,
                    symbol,
                    cause,
                    last_error,
                    ..
                } = &event.payload
                {
                    captured.lock().unwrap().push((
                        *channel,
                        symbol.clone(),
                        *cause,
                        last_error.clone(),
                    ));
                }
            }),
            u16::MAX,
        );
        let futures = FuturesSubscriptionNamespace::new(Arc::new(WsSurface::new(
            Arc::clone(&bus),
            Arc::new(std::sync::RwLock::new(HashMap::new())),
            Arc::new(AtomicU64::new(1)),
        )));

        register_batch(
            &bus,
            WsUrl::UnifiedPublic,
            vec![SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                ticker_params(),
            )],
        );

        let socket = socket_with_at_least(&factory, WsUrl::UnifiedPublic, 0).await;
        wait_for_state(
            &unified_state,
            ConnectionState::Resubscribing,
            "Futures ticker subscribe",
        )
        .await;
        socket.emit_text(super::reconnect_drive_tests::ack_text(
            "ticker",
            Some("PI_XBTUSD"),
        ));
        wait_for_state(
            &unified_state,
            ConnectionState::Open,
            "Futures ticker acknowledgement",
        )
        .await;

        futures
            .unsubscribe_ticker(vec!["PI_XBTUSD".to_string()])
            .expect("normal Futures unsubscribe");

        tokio::time::timeout(BUDGET, async {
            loop {
                if terminated.lock().unwrap().len() == 1
                    && !mirror.read().unwrap().contains_key(&(
                        WsUrl::UnifiedPublic,
                        ChannelName::Ticker,
                        Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                    ))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Futures termination event and mirror teardown");
        assert_eq!(
            terminated.lock().unwrap().as_slice(),
            &[(
                ChannelName::Ticker,
                Some("PI_XBTUSD".to_string()),
                crate::api::subscription::TerminationCause::ClientClosed,
                None,
            )]
        );
        let unsubscribe = socket
            .sent_text()
            .into_iter()
            .map(|frame| serde_json::from_str::<serde_json::Value>(&frame).expect("frame JSON"))
            .find(|frame| frame["method"] == "unsubscribe")
            .expect("Futures unsubscribe frame");
        assert_eq!(
            unsubscribe["params"]["symbol"],
            serde_json::json!(["PI_XBTUSD"])
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn token_miss_replay_deferral_survives_open_and_drains() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-D", token_envelope("fresh-token-D"));
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![
                SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances { snapshot: None },
                ),
                SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions {
                        snap_orders: None,
                        snap_trades: None,
                    },
                ),
            ],
        );

        let auth_sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        let (channel, _token) = nth_sent_token(&auth_sock, 0).await;
        assert_eq!(channel, "balances", "sorted-first golden probe");

        auth_stack.token_lifecycle().clear_cached_token_for_test();
        auth_sock.emit_text(ack_ok("balances"));

        wait_for_state(&auth_mirror, ConnectionState::Open, "open with deferral").await;
        let (channel, token) = nth_sent_token(&auth_sock, 1).await;
        assert_eq!(channel, "executions", "drain re-issues the deferred entry");
        assert_eq!(token, "fresh-token-D", "re-issue rides the refreshed token");
        assert_exactly_n_sent(&auth_sock, 2, "no duplicate re-issue").await;

        auth_sock.emit_text(ack_ok("executions"));
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(WsUrl::Auth, ChannelName::Executions, None))
                        .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                    {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: drained entry never reached Acked");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn acked_last_session_deferral_reissues_after_reconnect() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-R", token_envelope("fresh-token-R"));
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![
                SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances { snapshot: None },
                ),
                SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions {
                        snap_orders: None,
                        snap_trades: None,
                    },
                ),
            ],
        );
        let s1 = socket_at(&factory, 0).await;
        let (channel, _) = nth_sent_token(&s1, 0).await;
        assert_eq!(channel, "balances");
        s1.emit_text(ack_ok("balances"));
        let (channel, _) = nth_sent_token(&s1, 1).await;
        assert_eq!(channel, "executions");
        s1.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "session 1 open").await;

        s1.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Auth,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 77 },
        })
        .expect("post force reconnect");
        let s2 = {
            let poll = async {
                loop {
                    if factory.created_count() >= 2 {
                        return factory.handle(1).expect("socket2");
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: reconnect socket")
        };
        let (channel, _) = nth_sent_token(&s2, 0).await;
        assert_eq!(channel, "balances", "handshake probe on the new socket");
        auth_stack.token_lifecycle().clear_cached_token_for_test();
        s2.emit_text(ack_ok("balances"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "session 2 open").await;

        let (channel, token) = nth_sent_token(&s2, 1).await;
        assert_eq!(channel, "executions", "deferred Acked entry re-issued");
        assert_eq!(token, "fresh-token-R");
        assert_exactly_n_sent(&s2, 2, "no duplicate re-issue").await;
        s2.emit_text(ack_ok("executions"));
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(WsUrl::Auth, ChannelName::Executions, None))
                        .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                    {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: re-issued entry never re-acked");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn reconnect_replay_drops_written_opt_outs_and_keeps_opt_ins_and_unwritten_opt_outs() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-S", token_envelope("fresh-token-S"));
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dropped_sink = Arc::clone(&dropped);
        let _dropped_subscription = bus.subscribe(
            EventType::ConnectionDroppedEvent,
            Arc::new(move |event: &EventEnvelope| {
                if matches!(
                    event.payload,
                    EventPayload::ConnectionDroppedEvent {
                        url: WsUrl::UnifiedPrivate,
                        ..
                    }
                ) {
                    dropped_sink.store(true, Ordering::SeqCst);
                }
            }),
            u16::MAX,
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, _auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );
        let register = |channel, params| {
            register_batch(
                &bus,
                WsUrl::UnifiedPrivate,
                vec![SubscriptionEntry::new(
                    WsUrl::UnifiedPrivate,
                    channel,
                    None,
                    params,
                )],
            );
        };
        let options = |frame: &str| {
            let v: serde_json::Value = serde_json::from_str(frame).expect("frame JSON");
            let p = &v["params"];
            serde_json::json!({
                "channel": p["channel"],
                "snapshot": p["snapshot"],
                "snap_orders": p["snap_orders"],
                "snap_trades": p["snap_trades"],
            })
        };

        register(
            ChannelName::Balances,
            SubscribeParams::Balances {
                snapshot: Some(false),
            },
        );
        let s1 = socket_with_sent(&factory, WsUrl::UnifiedPrivate, 1).await;
        assert_eq!(
            options(&s1.sent_text()[0]),
            serde_json::json!({ "channel": "balances", "snapshot": false, "snap_orders": null, "snap_trades": null }),
            "the first subscribe carries the caller's opt-out"
        );
        s1.emit_text(ack_ok("balances"));
        tokio::time::timeout(BUDGET, async {
            loop {
                if sub_mirror
                    .read()
                    .unwrap()
                    .get(&(WsUrl::UnifiedPrivate, ChannelName::Balances, None))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: balances never acked");

        register(
            ChannelName::Executions,
            SubscribeParams::Executions {
                snap_orders: Some(false),
                snap_trades: Some(true),
            },
        );
        let s1 = socket_with_sent(&factory, WsUrl::UnifiedPrivate, 2).await;
        assert_eq!(
            options(&s1.sent_text()[1]),
            serde_json::json!({ "channel": "executions", "snapshot": null, "snap_orders": false, "snap_trades": true }),
            "a subscribe on the open socket carries the caller's options"
        );
        let positions = SubscribeParams::Positions {
            snapshot: Some(false),
        };
        register(ChannelName::Positions, positions);
        socket_with_sent(&factory, WsUrl::UnifiedPrivate, 3).await;
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::UnifiedPrivate,
            mutation: RegistryMutationOp::Deregister {
                channel: ChannelName::Positions,
                pair: None,
            },
        })
        .expect("post deregister");

        s1.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        tokio::time::timeout(BUDGET, async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: private socket drop");
        register(ChannelName::Positions, positions);
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::UnifiedPrivate,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 78 },
        })
        .expect("post force reconnect");

        let s2 = socket_with_sent(&factory, WsUrl::UnifiedPrivate, 1).await;
        assert_eq!(
            options(&s2.sent_text()[0]),
            serde_json::json!({ "channel": "balances", "snapshot": null, "snap_orders": null, "snap_trades": null }),
            "an acked opt-out is not replayed"
        );
        s2.emit_text(ack_ok("balances"));
        let s2 = socket_with_sent(&factory, WsUrl::UnifiedPrivate, 3).await;
        assert_eq!(
            options(&s2.sent_text()[1]),
            serde_json::json!({ "channel": "executions", "snapshot": null, "snap_orders": null, "snap_trades": true }),
            "an unacked opt-out is not replayed; the opt-in is"
        );
        assert_eq!(
            options(&s2.sent_text()[2]),
            serde_json::json!({ "channel": "positions", "snapshot": false, "snap_orders": null, "snap_trades": null }),
            "an opt-out registered while down keeps it on its first subscribe"
        );

        s2.emit_text(ack_ok("executions"));
        s2.emit_text(ack_ok("positions"));
        dropped.store(false, Ordering::SeqCst);
        s2.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        tokio::time::timeout(BUDGET, async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: second private socket drop");
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::UnifiedPrivate,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 79 },
        })
        .expect("post second force reconnect");

        let s3 = socket_with_sent(&factory, WsUrl::UnifiedPrivate, 1).await;
        s3.emit_text(ack_ok("balances"));
        let s3 = socket_with_sent(&factory, WsUrl::UnifiedPrivate, 3).await;
        assert_eq!(
            options(&s3.sent_text()[2]),
            serde_json::json!({ "channel": "positions", "snapshot": null, "snap_orders": null, "snap_trades": null }),
            "a written positions opt-out is not replayed"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn deregister_all_drops_deferrals_no_zombie_reissue() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        use std::sync::atomic::AtomicUsize;
        let (bus, factory) = bus_and_factory();
        let calls = Arc::new(AtomicUsize::new(0));
        fn envelope(n: usize) -> Result<serde_json::Value, crate::transport::TransportError> {
            if n == 0 {
                Ok(serde_json::json!({ "error": ["EGeneral:Internal error"] }))
            } else {
                Ok(token_envelope("fresh-token-Z"))
            }
        }
        let (c1, c2) = (Arc::clone(&calls), Arc::clone(&calls));
        let (auth_stack, _rest) = auth_stack_with_transport(
            &bus,
            "seeded-token-Z",
            Arc::new(ClosureHttpMock::new(
                Box::new(move || envelope(c1.fetch_add(1, Ordering::Relaxed))),
                Box::new(move || envelope(c2.fetch_add(1, Ordering::Relaxed))),
            )) as Arc<dyn crate::transport::HttpTransport>,
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                SubscribeParams::Balances { snapshot: None },
            )],
        );
        let sock = socket_at(&factory, 0).await;
        let (channel, _) = nth_sent_token(&sock, 0).await;
        assert_eq!(channel, "balances");
        sock.emit_text(ack_ok("balances"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "open").await;

        auth_stack.token_lifecycle().clear_cached_token_for_test();
        register_executions(&bus);
        let poll = async {
            loop {
                if calls.load(Ordering::Relaxed) >= 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: refresh #1 never fired");
        assert_exactly_n_sent(&sock, 1, "deferred entry sends nothing").await;

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::DeregisterAll { channel: None },
        })
        .expect("post deregister-all");
        let poll = async {
            loop {
                let n = sock
                    .sent_text()
                    .iter()
                    .filter(|t| t.contains("unsubscribe"))
                    .count();
                if n >= 2 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: both wire unsubscribes");

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                SubscribeParams::Balances { snapshot: None },
            )],
        );
        let (channel, token) = nth_sent_token(&sock, 3).await;
        assert_eq!(channel, "balances", "drain re-issues only the live key");
        assert_eq!(token, "fresh-token-Z");
        assert_exactly_n_sent(&sock, 4, "no zombie re-issue for the removed key").await;
        let exec_frames = sock
            .sent_text()
            .iter()
            .filter(|t| t.contains("executions"))
            .count();
        assert_eq!(
            exec_frames, 1,
            "the removed key's only frame is its teardown unsubscribe"
        );

        sock.emit_text(ack_ok("balances"));
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(WsUrl::Auth, ChannelName::Balances, None))
                        .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                    {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: re-registered key never acked");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn deferral_satisfied_by_handshake_send_is_not_reissued() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        use std::sync::atomic::AtomicUsize;
        let (bus, factory) = bus_and_factory();
        let calls = Arc::new(AtomicUsize::new(0));
        let (c1, c2) = (Arc::clone(&calls), Arc::clone(&calls));
        fn token_ok() -> Result<serde_json::Value, crate::transport::TransportError> {
            Ok(token_envelope("fresh-D2"))
        }
        let (auth_stack, _rest) = auth_stack_with_transport(
            &bus,
            "seeded-token-D2",
            Arc::new(ClosureHttpMock::new(
                Box::new(move || {
                    c1.fetch_add(1, Ordering::Relaxed);
                    token_ok()
                }),
                Box::new(move || {
                    c2.fetch_add(1, Ordering::Relaxed);
                    token_ok()
                }),
            )) as Arc<dyn crate::transport::HttpTransport>,
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Auth,
            event: crate::types::CallerEvent::StartConnect { request_id: 910 },
        })
        .expect("post connect");
        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        auth_stack.token_lifecycle().clear_cached_token_for_test();

        register_executions(&bus);
        let (channel, token) = nth_sent_token(&sock, 0).await;
        assert_eq!(channel, "executions", "handshake send carries the entry");
        assert_eq!(token, "fresh-D2", "sent with the refreshed token");
        sock.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(WsUrl::Auth, ChannelName::Executions, None))
                        .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                    {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: entry never acked");

        auth_stack.token_lifecycle().clear_cached_token_for_test();
        let before = calls.load(Ordering::Relaxed);
        let _h = auth_stack.force_refresh(crate::auth::RefreshReason::AuthHandshakeFailed);
        let poll = async {
            loop {
                if calls.load(Ordering::Relaxed) > before {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: second refresh never fired");
        assert_exactly_n_sent(&sock, 1, "no duplicate subscribe for the live entry").await;
        {
            let m = sub_mirror.read().unwrap();
            assert!(
                m.get(&(WsUrl::Auth, ChannelName::Executions, None))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked)),
                "the live subscription stays Acked — never tombstoned by a duplicate reject"
            );
        }

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn late_unarmed_success_before_resend_kills_the_retry_chain() {
        late_unarmed_success_drives_no_resubscribe(150).await;
    }

    #[tokio::test]
    async fn late_unarmed_success_after_defer_drops_the_deferral() {
        late_unarmed_success_drives_no_resubscribe(400).await;
    }

    #[tokio::test]
    async fn resend_defer_on_token_miss_mid_resubscribing_completes_to_open() {
        use crate::conn::subscription_registry::SubscriptionMirror;
        use std::sync::atomic::AtomicBool;
        let (bus, factory) = bus_and_factory();
        bus.knobs()
            .subscribe_ack_timeout_ms
            .store(50, std::sync::atomic::Ordering::Relaxed);
        let released = Arc::new(AtomicBool::new(false));
        let gated = |flag: Arc<AtomicBool>| {
            move || {
                if flag.load(Ordering::Relaxed) {
                    Ok(serde_json::json!({
                        "error": [], "result": { "token": "fresh-RD", "expires": 900u32 }
                    }))
                } else {
                    Ok(serde_json::json!({ "error": ["EGeneral:Temporary lockout"] }))
                }
            }
        };
        let (auth_stack, _rest) = auth_stack_with_transport(
            &bus,
            "seeded-token-RD",
            Arc::new(ClosureHttpMock::new(
                Box::new(gated(Arc::clone(&released))),
                Box::new(gated(Arc::clone(&released))),
            )) as Arc<dyn crate::transport::HttpTransport>,
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![
                SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances { snapshot: None },
                ),
                SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions {
                        snap_orders: None,
                        snap_trades: None,
                    },
                ),
            ],
        );
        let s1 = socket_at(&factory, 0).await;
        let (c1, _) = nth_sent_token(&s1, 0).await;
        s1.emit_text(ack_ok(&c1));
        let (c2, _) = nth_sent_token(&s1, 1).await;
        s1.emit_text(ack_ok(&c2));
        wait_for_state(&auth_mirror, ConnectionState::Open, "session 1 open").await;

        s1.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Auth,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 79 },
        })
        .expect("post force reconnect");
        let s2 = socket_at(&factory, 1).await;
        let (r1, _) = nth_sent_token(&s2, 0).await;
        s2.emit_text(ack_ok(&r1));
        let (_r2, _) = nth_sent_token(&s2, 1).await;
        auth_stack.token_lifecycle().clear_cached_token_for_test();
        wait_for_state(
            &auth_mirror,
            ConnectionState::Open,
            "defer completes resubscribe",
        )
        .await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn proactive_refresh_outcome_drains_deferred_auth_subscribe() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        use std::sync::atomic::AtomicBool;
        let (bus, factory) = bus_and_factory();
        let released = Arc::new(AtomicBool::new(false));
        let gated = |flag: Arc<AtomicBool>| {
            move || {
                if flag.load(Ordering::Relaxed) {
                    Ok(serde_json::json!({
                        "error": [], "result": { "token": "fresh-PD", "expires": 900u32 }
                    }))
                } else {
                    Ok(serde_json::json!({ "error": ["EGeneral:Temporary lockout"] }))
                }
            }
        };
        let (auth_stack, _rest) = auth_stack_with_transport(
            &bus,
            "seeded-token-PD",
            Arc::new(ClosureHttpMock::new(
                Box::new(gated(Arc::clone(&released))),
                Box::new(gated(Arc::clone(&released))),
            )) as Arc<dyn crate::transport::HttpTransport>,
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Auth,
            event: crate::types::CallerEvent::StartConnect { request_id: 913 },
        })
        .expect("post connect");
        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        register_executions(&bus);
        let (c1, _) = nth_sent_token(&sock, 0).await;
        sock.emit_text(ack_ok(&c1));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;

        auth_stack.token_lifecycle().clear_cached_token_for_test();
        register_batch(
            &bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                SubscribeParams::Balances { snapshot: None },
            )],
        );
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_exactly_n_sent(&sock, 1, "deferred: no balances frame while token missing").await;

        released.store(true, Ordering::Relaxed);
        auth_stack.token_lifecycle().proactive_refresh();
        let (c2, token) = nth_sent_token(&sock, 1).await;
        assert_eq!(c2, "balances", "proactive outcome drained the deferral");
        assert_eq!(token, "fresh-PD", "drained send carries the tick's token");
        sock.emit_text(ack_ok(&c2));
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(WsUrl::Auth, ChannelName::Balances, None))
                        .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                    {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: drained entry never acked");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn proactive_refresh_failure_mid_handshake_is_not_delivered() {
        use crate::conn::subscription_registry::SubscriptionMirror;
        use std::sync::atomic::AtomicBool;
        let (bus, factory) = bus_and_factory();
        let released = Arc::new(AtomicBool::new(false));
        let gated = |flag: Arc<AtomicBool>| {
            move || {
                if flag.load(Ordering::Relaxed) {
                    Ok(serde_json::json!({
                        "error": [], "result": { "token": "fresh-PF", "expires": 900u32 }
                    }))
                } else {
                    Ok(serde_json::json!({ "error": ["EAPI:Invalid key"] }))
                }
            }
        };
        let (auth_stack, _rest) = auth_stack_with_transport(
            &bus,
            "seeded-token-PF",
            Arc::new(ClosureHttpMock::new(
                Box::new(gated(Arc::clone(&released))),
                Box::new(gated(Arc::clone(&released))),
            )) as Arc<dyn crate::transport::HttpTransport>,
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Auth,
            event: crate::types::CallerEvent::StartConnect { request_id: 914 },
        })
        .expect("post connect");
        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        register_executions(&bus);
        let (c1, _) = nth_sent_token(&sock, 0).await;
        assert_eq!(c1, "executions", "probe in flight");

        auth_stack.token_lifecycle().proactive_refresh();
        tokio::time::sleep(Duration::from_millis(100)).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "handshake survives the proactive failure",
        )
        .await;
        sock.emit_text(ack_ok(&c1));
        wait_for_state(&auth_mirror, ConnectionState::Open, "handshake completes").await;

        reactor.abort();
        bus.stop_reactors();
    }

    struct HeldFirstTokenFetch {
        calls: std::sync::atomic::AtomicUsize,
        release: tokio::sync::Semaphore,
    }

    impl HeldFirstTokenFetch {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                release: tokio::sync::Semaphore::new(0),
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        async fn wait_for_calls(&self, n: usize, what: &str) {
            let poll = async {
                while self.calls() < n {
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .unwrap_or_else(|_| panic!("timeout: {what}"));
        }
    }

    #[async_trait::async_trait]
    impl HttpTransport for HeldFirstTokenFetch {
        async fn get_json(
            &self,
            _path: &str,
            _query: &[(&str, &str)],
        ) -> Result<serde_json::Value, crate::transport::TransportError> {
            panic!("unexpected public REST call")
        }

        async fn post_form_signed(
            &self,
            _path: &str,
            _body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<serde_json::Value, crate::transport::TransportError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if n == 1 {
                self.release
                    .acquire()
                    .await
                    .expect("semaphore open")
                    .forget();
            }
            Ok(serde_json::json!({
                "error": [], "result": { "token": format!("tok-{n}"), "expires": 900u32 }
            }))
        }
    }

    struct OffsetClock(AtomicU64);

    impl Clock for OffsetClock {
        fn now(&self) -> crate::types::MonotonicInstant {
            crate::types::MonotonicInstant(
                crate::types::MonotonicInstant::now().0
                    + Duration::from_nanos(self.0.load(Ordering::SeqCst)),
            )
        }
    }

    #[tokio::test]
    async fn the_proactive_token_refresh_follows_the_one_live_owner_connection() {
        use crate::auth::token_lifecycle::WS_TOKEN_REFRESH_AT;
        let clock = Arc::new(OffsetClock(AtomicU64::new(0)));
        let bus = Arc::new(DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            Arc::clone(&clock) as Arc<dyn Clock>,
        ));
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let factory = Arc::new(DriveableWsSocketFactory::new(Arc::clone(&bus)));
        let transport = HeldFirstTokenFetch::new();
        transport.release.add_permits(1);
        let (auth_stack, _rest) = auth_stack_with_transport(
            &bus,
            "seeded-owner-token",
            Arc::clone(&transport) as Arc<dyn HttpTransport>,
        );
        let sub_mirror = crate::conn::subscription_registry::SubscriptionMirror::default();

        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        let mut conns = HashMap::new();
        for url in [WsUrl::Auth, WsUrl::UnifiedPrivate] {
            state_mirrors.insert(url, Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())));
            conns.insert(
                url,
                ManagedConnection::new(
                    url,
                    Arc::clone(&bus),
                    Arc::clone(&factory) as Arc<dyn WsSocketFactoryLike>,
                    Arc::new(ConnectionRateBudget::new()),
                    Arc::clone(&clock) as Arc<dyn Clock>,
                    Arc::new(crate::jitter::FixedJitter(1.0)),
                ),
            );
        }
        let auth_mirror = Arc::clone(&state_mirrors[&WsUrl::Auth]);
        let unified_mirror = Arc::clone(&state_mirrors[&WsUrl::UnifiedPrivate]);
        let reactor = tokio::spawn(run(IoReactorInit {
            conns,
            caller_rx: bus.take_caller_to_io_rx().expect("caller_to_io_rx"),
            registry: SubscriptionRegistry::with_mirror(sub_mirror),
            handler_registry: crate::dispatch::HandlerRegistry::new(Arc::new(
                std::sync::RwLock::new(HashMap::new()),
            )),
            auth_stack: Arc::clone(&auth_stack),
            state_mirrors,
            bus_back_ref: Arc::downgrade(&bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::new(crate::conn::OrderGate::default()),
        }));
        let advance = |by: Duration| {
            clock.0.fetch_add(by.as_nanos() as u64, Ordering::SeqCst);
        };
        let wake = |sock: &crate::transport::driveable_mock::DriveableSocketHandle| {
            sock.emit_text(serde_json::json!({ "channel": "heartbeat" }).to_string());
        };
        let period = WS_TOKEN_REFRESH_AT + Duration::from_secs(1);

        register_batch(
            &bus,
            WsUrl::UnifiedPrivate,
            vec![SubscriptionEntry::new(
                WsUrl::UnifiedPrivate,
                ChannelName::Executions,
                None,
                SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
            )],
        );
        let unified = socket_with_at_least(&factory, WsUrl::UnifiedPrivate, 1).await;
        unified.emit_text(include_str!(
            "../../../tests/fixtures/unified_ws/private_executions_subscribe_ack.json"
        ));
        wait_for_state(&unified_mirror, ConnectionState::Open, "Unified open").await;

        advance(period);
        wake(&unified);
        transport
            .wait_for_calls(
                1,
                "Unified private never refreshed the token while Auth was idle",
            )
            .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(transport.calls(), 1, "one refresh per period");

        register_executions(&bus);
        let auth = socket_with_at_least(&factory, WsUrl::Auth, 1).await;
        auth.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "Auth open").await;

        advance(period);
        wake(&auth);
        wake(&unified);
        transport
            .wait_for_calls(2, "Spot Auth never took the timer over")
            .await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            transport.calls(),
            2,
            "both connections are live, so exactly one of them refreshes"
        );

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Auth,
            event: crate::types::CallerEvent::Close { request_id: 900 },
        })
        .expect("post Auth close");
        wait_for_state(&auth_mirror, ConnectionState::Closing, "Auth closing").await;
        auth.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        wait_for_state(&auth_mirror, ConnectionState::Closed, "Auth closed").await;

        advance(period);
        wake(&unified);
        transport
            .wait_for_calls(3, "Unified private never took the timer back")
            .await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(transport.calls(), 3);

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn token_landing_after_handshake_teardown_still_arms_proactive_refresh() {
        use crate::auth::token_lifecycle::WS_TOKEN_REFRESH_AT;
        use crate::dispatch::event_bus::RefreshReasonPayload;
        let clock = Arc::new(OffsetClock(AtomicU64::new(0)));
        let bus = Arc::new(DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            Arc::clone(&clock) as Arc<dyn Clock>,
        ));
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let factory = Arc::new(DriveableWsSocketFactory::new(Arc::clone(&bus)));
        let reasons = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&reasons);
        let _sub = bus.subscribe(
            crate::dispatch::EventType::CredentialRefreshedEvent,
            Arc::new(move |env: &crate::dispatch::EventEnvelope| {
                if let crate::dispatch::EventPayload::CredentialRefreshedEvent {
                    refresh_reason,
                    ..
                } = &env.payload
                {
                    seen.lock().unwrap().push(*refresh_reason);
                }
            }),
            u16::MAX,
        );
        let transport = HeldFirstTokenFetch::new();
        let (auth_stack, _rest) = auth_stack_with_transport(
            &bus,
            "unused",
            Arc::clone(&transport) as Arc<dyn HttpTransport>,
        );
        auth_stack.token_lifecycle().clear_cached_token_for_test();

        let auth_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        state_mirrors.insert(WsUrl::Auth, Arc::clone(&auth_mirror));
        let mut conns = HashMap::new();
        conns.insert(
            WsUrl::Auth,
            ManagedConnection::new(
                WsUrl::Auth,
                Arc::clone(&bus),
                Arc::clone(&factory) as Arc<dyn WsSocketFactoryLike>,
                Arc::new(ConnectionRateBudget::new()),
                Arc::clone(&clock) as Arc<dyn Clock>,
                Arc::new(crate::jitter::FixedJitter(1.0)),
            ),
        );
        let reactor = tokio::spawn(run(IoReactorInit {
            conns,
            caller_rx: bus.take_caller_to_io_rx().expect("caller_to_io_rx"),
            registry: SubscriptionRegistry::new(),
            handler_registry: crate::dispatch::HandlerRegistry::new(Arc::new(
                std::sync::RwLock::new(HashMap::new()),
            )),
            auth_stack: Arc::clone(&auth_stack),
            state_mirrors,
            bus_back_ref: Arc::downgrade(&bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::new(crate::conn::OrderGate::default()),
        }));

        register_executions(&bus);
        let s0 = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        transport
            .wait_for_calls(1, "cold-start token fetch never started")
            .await;
        assert!(
            s0.sent_text().is_empty(),
            "nothing sent before a token exists"
        );

        s0.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        wait_for_state(
            &auth_mirror,
            ConnectionState::BackingOff,
            "handshake teardown",
        )
        .await;
        transport.release.add_permits(1);
        let landed = async {
            while auth_stack.cached_token().is_none() {
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, landed)
            .await
            .expect("timeout: held token never landed");

        let s1 = socket_at(&factory, 1).await;
        let (channel, token) = nth_sent_token(&s1, 0).await;
        assert_eq!(channel, "executions");
        assert_eq!(
            token, "tok-1",
            "the reopen reuses the token that landed in BackingOff"
        );
        assert_eq!(
            transport.calls(),
            1,
            "no second fetch before the proactive tick"
        );
        s1.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;

        let heartbeat = serde_json::json!({ "channel": "heartbeat" }).to_string();
        clock.0.store(
            (WS_TOKEN_REFRESH_AT - Duration::from_secs(5)).as_nanos() as u64,
            Ordering::SeqCst,
        );
        s1.emit_text(heartbeat.clone());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            transport.calls(),
            1,
            "no proactive refresh before TTL x 0.5"
        );

        clock.0.store(
            (WS_TOKEN_REFRESH_AT + Duration::from_secs(1)).as_nanos() as u64,
            Ordering::SeqCst,
        );
        s1.emit_text(heartbeat);
        transport
            .wait_for_calls(2, "proactive refresh never fired at TTL x 0.5")
            .await;
        let both = async {
            while reasons.lock().unwrap().len() < 2 {
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, both)
            .await
            .expect("timeout: proactive CredentialRefreshedEvent");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            *reasons.lock().unwrap(),
            vec![
                RefreshReasonPayload::Reactive,
                RefreshReasonPayload::Proactive
            ],
        );
        assert_eq!(transport.calls(), 2);
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Open,
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn late_unarmed_success_mid_resubscribing_completes_to_open() {
        use crate::conn::subscription_registry::SubscriptionMirror;
        let (bus, factory) = bus_and_factory();
        bus.knobs()
            .subscribe_ack_timeout_ms
            .store(50, std::sync::atomic::Ordering::Relaxed);
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-W", token_envelope("fresh-token-W"));
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![
                SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances { snapshot: None },
                ),
                SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions {
                        snap_orders: None,
                        snap_trades: None,
                    },
                ),
            ],
        );
        let s1 = socket_at(&factory, 0).await;
        let (c1, _) = nth_sent_token(&s1, 0).await;
        s1.emit_text(ack_ok(&c1));
        let (c2, _) = nth_sent_token(&s1, 1).await;
        s1.emit_text(ack_ok(&c2));
        wait_for_state(&auth_mirror, ConnectionState::Open, "session 1 open").await;

        s1.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Auth,
            event: crate::types::CallerEvent::ForceReconnect { request_id: 78 },
        })
        .expect("post force reconnect");
        let s2 = socket_at(&factory, 1).await;
        let (r1, _) = nth_sent_token(&s2, 0).await;
        s2.emit_text(ack_ok(&r1));
        let (r2, _) = nth_sent_token(&s2, 1).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        s2.emit_text(ack_ok(&r2));
        wait_for_state(&auth_mirror, ConnectionState::Open, "resubscribe completes").await;

        reactor.abort();
        bus.stop_reactors();
    }

    async fn late_unarmed_success_drives_no_resubscribe(late_ack_delay_ms: u64) {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        use std::sync::atomic::AtomicBool;
        let (bus, factory) = bus_and_factory();
        bus.knobs()
            .subscribe_ack_timeout_ms
            .store(50, std::sync::atomic::Ordering::Relaxed);
        let released = Arc::new(AtomicBool::new(false));
        let gated = |flag: Arc<AtomicBool>| {
            move || {
                if flag.load(Ordering::Relaxed) {
                    Ok(serde_json::json!({
                        "error": [], "result": { "token": "fresh-LA", "expires": 900u32 }
                    }))
                } else {
                    Ok(serde_json::json!({ "error": ["EGeneral:Temporary lockout"] }))
                }
            }
        };
        let (auth_stack, _rest) = auth_stack_with_transport(
            &bus,
            "seeded-token-LA",
            Arc::new(ClosureHttpMock::new(
                Box::new(gated(Arc::clone(&released))),
                Box::new(gated(Arc::clone(&released))),
            )) as Arc<dyn crate::transport::HttpTransport>,
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Auth,
            event: crate::types::CallerEvent::StartConnect { request_id: 912 },
        })
        .expect("post connect");
        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        register_executions(&bus);
        let (channel, _) = nth_sent_token(&sock, 0).await;
        assert_eq!(channel, "executions", "handshake send");
        sock.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                SubscribeParams::Balances { snapshot: None },
            )],
        );
        let (channel, _) = nth_sent_token(&sock, 1).await;
        assert_eq!(channel, "balances", "steady-state send");
        // Token dies before the ack timeout so the resend defers; the delay picks the ordering.
        auth_stack.token_lifecycle().clear_cached_token_for_test();
        tokio::time::sleep(Duration::from_millis(late_ack_delay_ms)).await;
        sock.emit_text(ack_ok("balances"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        {
            let m = sub_mirror.read().unwrap();
            assert!(
                m.get(&(WsUrl::Auth, ChannelName::Balances, None))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Pending)),
                "unarmed success must not drive Pending -> Acked"
            );
        }
        released.store(true, Ordering::Relaxed);
        let _h = auth_stack.force_refresh(crate::auth::RefreshReason::AuthHandshakeFailed);
        assert_exactly_n_sent(&sock, 2, "no drain re-subscribe of the live balances entry").await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn register_mid_authenticating_idle_sends_subscribe_as_probe() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-P", token_envelope("unused"));
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Auth,
            event: crate::types::CallerEvent::StartConnect { request_id: 900 },
        })
        .expect("post connect");
        let auth_sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        assert_exactly_n_sent(&auth_sock, 0, "empty composition sends nothing").await;

        register_executions(&bus);
        let (channel, token) = nth_sent_token(&auth_sock, 0).await;
        assert_eq!(channel, "executions", "register sends mid-Authenticating");
        assert_eq!(token, "seeded-token-P", "probe rides the cached token");

        auth_sock.emit_text(ack_ok("executions"));
        wait_for_state(
            &auth_mirror,
            ConnectionState::Open,
            "subscribe-as-probe drives to Open",
        )
        .await;
        assert_exactly_n_sent(&auth_sock, 1, "the probe is the only frame").await;
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(WsUrl::Auth, ChannelName::Executions, None))
                        .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                    {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: probe entry never reached Acked");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn auth_hint_ignores_reject_tombstone() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-H", token_envelope("unused"));
        let sub_mirror = SubscriptionMirror::default();
        let ReactorHandles {
            reactor,
            auth: auth_mirror,
            order_gate,
            ..
        } = spawn_reactor_handles(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );
        register_batch(
            &bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                SubscribeParams::Balances { snapshot: None },
            )],
        );
        let sock = socket_at(&factory, 0).await;
        let (channel, _) = nth_sent_token(&sock, 0).await;
        assert_eq!(channel, "balances");
        sock.emit_text(ack_ok("balances"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "open").await;
        register_executions(&bus);
        let (channel, _) = nth_sent_token(&sock, 1).await;
        assert_eq!(channel, "executions");
        sock.emit_text(ack_err("executions", "Currency pair not supported"));
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(WsUrl::Auth, ChannelName::Executions, None))
                        .is_some_and(|row| matches!(row.state, EntrySubState::Terminated { .. }))
                    {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: tombstone after reject");
        assert!(
            order_gate.has_subscriptions(WsUrl::Auth),
            "balances still live"
        );
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::Deregister {
                channel: ChannelName::Balances,
                pair: None,
            },
        })
        .expect("post deregister");
        let flag_false = async {
            loop {
                if !order_gate.has_subscriptions(WsUrl::Auth) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, flag_false)
            .await
            .expect("timeout: hint stayed true — tombstone counted as live?");
        {
            let m = sub_mirror.read().unwrap();
            assert!(
                m.get(&(WsUrl::Auth, ChannelName::Executions, None))
                    .is_some(),
                "tombstone row still listable while the hint reads false"
            );
        }

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_b_token_stale_refresh_resend_drives_to_open() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "stale-token-B", token_envelope("fresh-token"));
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);

        register_executions(&bus);

        let auth_sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;

        let (c0, t0) = nth_sent_token(&auth_sock, 0).await;
        assert_eq!(c0, "executions", "first frame is the executions subscribe");
        assert_eq!(
            t0, "stale-token-B",
            "first frame carries the seeded (stale) token"
        );
        assert_exactly_n_sent(&auth_sock, 1, "before stale-token failure ack").await;

        auth_sock.emit_text(ack_err("executions", "EAPI:Invalid token"));

        let (c1, t1) = nth_sent_token(&auth_sock, 1).await;
        assert_eq!(
            c1, "executions",
            "re-sent frame is the executions subscribe"
        );
        assert_eq!(
            t1, "fresh-token",
            "re-sent first signed subscribe carries the fresh token"
        );
        assert_exactly_n_sent(&auth_sock, 2, "after refresh re-send").await;
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Authenticating,
            "still Authenticating after the re-send (awaiting the fresh handshake ack)"
        );

        auth_sock.emit_text(ack_ok("executions"));
        wait_for_state(
            &auth_mirror,
            ConnectionState::Open,
            "auth open after refresh",
        )
        .await;
        assert_exactly_n_sent(&auth_sock, 2, "after open (post-refresh handshake)").await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_auth_register_while_open_sends_tokenized_subscribe_a374() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-M2", token_envelope("unused"));
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);

        let auth_sock = drive_auth_to_open(&bus, &factory, &auth_mirror).await;
        assert_exactly_n_sent(&auth_sock, 1, "after open (only the executions sub)").await;

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                SubscribeParams::Balances { snapshot: None },
            )],
        );

        let (channel, token) = nth_sent_token(&auth_sock, 1).await;
        assert_eq!(
            channel, "balances",
            "Register-while-Open sent the balances subscribe"
        );
        assert_eq!(
            token, "seeded-token-M2",
            "tokenized (token inside params, A277)"
        );
        assert_exactly_n_sent(&auth_sock, 2, "after balances Register-while-Open").await;

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                SubscribeParams::Balances { snapshot: None },
            )],
        );
        assert_exactly_n_sent(
            &auth_sock,
            2,
            "duplicate auth subscriber (1→2) sends NO frame",
        )
        .await;

        auth_sock.emit_text(ack_ok("balances"));
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Open,
            "auth conn stays Open after the balances ack (self-loop, no FSM move)"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_private_auto_connect_sends_token_and_derivatives() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unified-private-token", token_envelope("unused"));
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);

        register_batch(
            &bus,
            WsUrl::UnifiedPrivate,
            vec![SubscriptionEntry::new(
                WsUrl::UnifiedPrivate,
                ChannelName::Executions,
                None,
                SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
            )],
        );

        let socket = socket_with_at_least(&factory, WsUrl::UnifiedPrivate, 1).await;
        let frame: serde_json::Value =
            serde_json::from_str(&socket.sent_text()[0]).expect("sent frame JSON");

        assert_eq!(frame["params"]["token"], "unified-private-token");
        assert_eq!(frame["params"]["asset_class"], "derivatives");
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Idle,
            "Unified private auto-connect must not open or alter Spot Auth"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_public_ticker_fixture_ack_activates_valid_symbol_entry() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};

        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unused-token", token_envelope("unused"));
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, _auth_mirror) =
            spawn_reactor_with_sub_mirror(&bus, &factory, auth_stack, Arc::clone(&sub_mirror));
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());

        register_batch(
            &bus,
            WsUrl::UnifiedPublic,
            vec![
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ticker,
                    Some(pair.clone()),
                    ticker_params(),
                ),
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ticker,
                    Some(StreamSymbol::Futures("PF_ETHUSD".to_string())),
                    ticker_params(),
                ),
            ],
        );

        let socket = socket_with_at_least(&factory, WsUrl::UnifiedPublic, 1).await;
        let frame: serde_json::Value =
            serde_json::from_str(&socket.sent_text()[0]).expect("sent frame JSON");
        assert_eq!(frame["params"]["asset_class"], "derivatives");
        assert_eq!(
            frame["params"]["symbol"],
            serde_json::json!(["PI_XBTUSD", "PF_ETHUSD"]),
            "one RegisterBatch must send one Unified frame for every new symbol"
        );
        assert_eq!(
            socket.sent_text().len(),
            1,
            "batch subscription must not send one frame per registry entry"
        );

        let mut ack: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/public_ticker_pi_xbtusd_subscribe_ack.json"
        ))
        .expect("fixture is valid JSON");
        ack["req_id"] = frame["req_id"].clone();
        socket.emit_text(ack.to_string());

        tokio::time::timeout(BUDGET, async {
            loop {
                if sub_mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(
                        WsUrl::UnifiedPublic,
                        ChannelName::Ticker,
                        Some(pair.clone()),
                    ))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Unified public ticker fixture ack did not activate the entry");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_futures_raw_then_maintained_book_shares_compatible_wire_subscription() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unused-token", token_envelope("unused"));
        let gaps = Arc::new(Mutex::new(Vec::<EventEnvelope>::new()));
        let gap_sink = Arc::clone(&gaps);
        let _gap_subscription = bus.subscribe(
            EventType::FuturesOrderBookGapEvent,
            Arc::new(move |event: &EventEnvelope| {
                gap_sink.lock().expect("gaps").push(event.clone());
            }),
            u16::MAX,
        );
        let (reactor, _auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);
        let symbol = StreamSymbol::Futures("PI_XBTUSD".to_string());

        for (channel, params) in [
            (
                ChannelName::BookRaw,
                SubscribeParams::BookRaw {
                    depth: BookDepth::D25,
                    snapshot: Some(true),
                },
            ),
            (
                ChannelName::Book,
                SubscribeParams::Book {
                    depth: BookDepth::D10,
                },
            ),
        ] {
            register_batch(
                &bus,
                WsUrl::UnifiedPublic,
                vec![SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    channel,
                    Some(symbol.clone()),
                    params,
                )],
            );
        }

        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
        let shared: serde_json::Value =
            serde_json::from_str(&socket.sent_text()[0]).expect("shared JSON");
        assert_eq!(shared["params"]["channel"], "book");
        assert_eq!(shared["params"]["snapshot"], true);
        assert_eq!(shared["params"]["depth"], 25);

        let checksum =
            crate::book::compute_book_crc32([("101", "1")].into_iter(), [("100", "1")].into_iter());
        socket.emit_text(
            serde_json::json!({
                "channel": "book", "type": "snapshot", "data": [{
                    "symbol": "PI_XBTUSD", "bids": [{"price": "100", "qty": "1"}],
                    "asks": [{"price": "101", "qty": "1"}], "checksum": checksum
                }]
            })
            .to_string(),
        );
        socket.emit_text(
            serde_json::json!({
                "channel": "book", "type": "update", "data": [{
                    "symbol": "PI_XBTUSD", "bids": [], "asks": [], "checksum": 0
                }]
            })
            .to_string(),
        );
        tokio::time::timeout(BUDGET, async {
            loop {
                if gaps.lock().expect("gaps").iter().any(|event| {
                    matches!(event.payload, EventPayload::FuturesOrderBookGapEvent { .. })
                }) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("maintained Futures book did not emit a gap");
        assert!(
            socket.sent_text().len() >= 3,
            "CRC recovery must reseed the shared Futures book"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_futures_raw_first_late_maintained_book_reseeds_before_activation_and_reports_its_reply()
     {
        use crate::api::market::decode_futures_book_raw;
        use crate::api::ws_decode::wrap_typed;
        use crate::book::FuturesBookDelta;
        use crate::dispatch::HandlerId;
        use crate::dispatch::event_bus::HandlerMutationOp;
        use std::sync::atomic::AtomicUsize;

        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unused-token", token_envelope("unused"));
        let subscribed = Arc::new(Mutex::new(Vec::<EventEnvelope>::new()));
        let subscribed_sink = Arc::clone(&subscribed);
        let _subscribed_subscription = bus.subscribe(
            EventType::FuturesSubscribedEvent,
            Arc::new(move |event: &EventEnvelope| {
                subscribed_sink
                    .lock()
                    .expect("subscribed events")
                    .push(event.clone());
            }),
            u16::MAX,
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, _auth_mirror) =
            spawn_reactor_with_sub_mirror(&bus, &factory, auth_stack, Arc::clone(&sub_mirror));
        let symbol = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let raw_snapshots = Arc::new(AtomicUsize::new(0));
        let raw_snapshots_cb = Arc::clone(&raw_snapshots);
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::UnifiedPublic,
            channel: ChannelName::BookRaw,
            op: HandlerMutationOp::Register {
                id: HandlerId(47_108),
                callback: wrap_typed(
                    move |delta: &FuturesBookDelta| {
                        if delta.is_snapshot {
                            raw_snapshots_cb.fetch_add(1, Ordering::SeqCst);
                        }
                    },
                    decode_futures_book_raw,
                ),
            },
        })
        .expect("register raw-book handler");
        let register = |channel, params| {
            register_batch(
                &bus,
                WsUrl::UnifiedPublic,
                vec![SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    channel,
                    Some(symbol.clone()),
                    params,
                )],
            );
        };
        let raw = SubscribeParams::BookRaw {
            depth: BookDepth::D25,
            snapshot: Some(true),
        };
        let maintained = SubscribeParams::Book {
            depth: BookDepth::D10,
        };

        register(ChannelName::BookRaw, raw);
        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
        let raw_subscribe: serde_json::Value =
            serde_json::from_str(&socket.sent_text()[0]).expect("raw subscribe JSON");
        socket.emit_text(
            serde_json::json!({
                "method": "subscribe",
                "success": true,
                "req_id": raw_subscribe["req_id"],
                "result": { "channel": "book", "symbol": "PI_XBTUSD" },
            })
            .to_string(),
        );
        tokio::time::timeout(BUDGET, async {
            loop {
                if sub_mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(
                        WsUrl::UnifiedPublic,
                        ChannelName::BookRaw,
                        Some(symbol.clone()),
                    ))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("raw book acknowledgement");

        register(ChannelName::Book, maintained);
        tokio::time::timeout(BUDGET, async {
            loop {
                if socket.sent_text().len() == 3 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late maintained join reseed");
        let frames: Vec<serde_json::Value> = socket
            .sent_text()
            .iter()
            .map(|frame| serde_json::from_str(frame).expect("book frame JSON"))
            .collect();
        assert_eq!(frames[1]["method"], "unsubscribe");
        assert_eq!(frames[2]["method"], "subscribe");
        assert_eq!(frames[2]["params"]["snapshot"], true);
        assert!(
            sub_mirror
                .read()
                .expect("subscription mirror")
                .get(&(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    Some(symbol.clone())
                ))
                .is_some_and(|row| matches!(row.state, EntrySubState::Pending))
        );

        socket.emit_text(
            serde_json::json!({
                "method": "subscribe",
                "success": true,
                "req_id": frames[2]["req_id"],
                "time_in": "2026-11-05T10:00:00.000001Z",
                "time_out": "2026-11-05T10:00:00.000002Z",
                "result": {
                    "channel": "book", "depth": 25, "snapshot": true, "symbol": "PI_XBTUSD",
                    "warnings": ["checksum is deprecated"],
                },
            })
            .to_string(),
        );
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            sub_mirror
                .read()
                .expect("subscription mirror")
                .get(&(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    Some(symbol.clone())
                ))
                .is_some_and(|row| matches!(row.state, EntrySubState::Pending))
        );

        let checksum =
            crate::book::compute_book_crc32([("101", "1")].into_iter(), [("100", "1")].into_iter());
        socket.emit_text(
            serde_json::json!({
                "channel": "book", "type": "snapshot", "data": [{
                    "symbol": "PI_XBTUSD", "bids": [{"price": "100", "qty": "1"}],
                    "asks": [{"price": "101", "qty": "1"}], "checksum": checksum
                }]
            })
            .to_string(),
        );
        socket.emit_text(
            serde_json::json!({
                "channel": "book", "type": "update", "data": [{
                    "symbol": "PI_XBTUSD", "bids": [], "asks": [], "checksum": checksum
                }]
            })
            .to_string(),
        );
        tokio::time::timeout(BUDGET, async {
            loop {
                if sub_mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(
                        WsUrl::UnifiedPublic,
                        ChannelName::Book,
                        Some(symbol.clone()),
                    ))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                    && raw_snapshots.load(Ordering::SeqCst) == 1
                    && subscribed
                        .lock()
                        .expect("subscribed events")
                        .iter()
                        .any(|event| {
                            matches!(
                                event.payload,
                                EventPayload::FuturesSubscribedEvent {
                                    channel: ChannelName::Book,
                                    ..
                                }
                            )
                        })
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("snapshot did not seed maintained book and fan to raw");
        assert_eq!(
            socket.sent_text().len(),
            3,
            "one late join sends one reseed sequence"
        );
        let book_acks: Vec<crate::dispatch::FuturesSubscribeAck> = subscribed
            .lock()
            .expect("subscribed events")
            .iter()
            .filter_map(|event| match &event.payload {
                EventPayload::FuturesSubscribedEvent {
                    channel: ChannelName::Book,
                    ack,
                    ..
                } => Some(ack.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            book_acks,
            vec![crate::dispatch::FuturesSubscribeAck {
                snapshot: Some(true),
                depth: Some(25),
                warnings: vec!["checksum is deprecated".to_owned()],
                req_id: Some(frames[2]["req_id"].as_u64().expect("reseed req_id")),
                time_in: Some("2026-11-05T10:00:00.000001Z".to_owned()),
                time_out: Some("2026-11-05T10:00:00.000002Z".to_owned()),
                ..Default::default()
            }]
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_futures_maintained_first_late_raw_book_attaches_without_reseed() {
        use crate::api::market::{decode_futures_book, decode_futures_book_raw};
        use crate::api::ws_decode::wrap_typed;
        use crate::book::{FuturesBookDelta, FuturesOrderBookUpdate};
        use crate::dispatch::HandlerId;
        use crate::dispatch::event_bus::HandlerMutationOp;
        use std::sync::atomic::AtomicUsize;

        let at = crate::types::MonotonicInstant(Duration::from_secs(7));
        let (bus, factory) = bus_and_factory_with_clock(Arc::new(crate::clock::FixedClock(at.0)));
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unused-token", token_envelope("unused"));
        let subscribed = Arc::new(Mutex::new(Vec::<EventEnvelope>::new()));
        let subscribed_sink = Arc::clone(&subscribed);
        let _subscribed_subscription = bus.subscribe(
            EventType::FuturesSubscribedEvent,
            Arc::new(move |event: &EventEnvelope| {
                subscribed_sink
                    .lock()
                    .expect("subscribed events")
                    .push(event.clone());
            }),
            u16::MAX,
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, _auth_mirror) =
            spawn_reactor_with_sub_mirror(&bus, &factory, auth_stack, Arc::clone(&sub_mirror));
        let symbol = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let maintained_updates = Arc::new(AtomicUsize::new(0));
        let maintained_updates_cb = Arc::clone(&maintained_updates);
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::UnifiedPublic,
            channel: ChannelName::Book,
            op: HandlerMutationOp::Register {
                id: HandlerId(47_110),
                callback: wrap_typed(
                    move |_update: &FuturesOrderBookUpdate| {
                        maintained_updates_cb.fetch_add(1, Ordering::SeqCst);
                    },
                    decode_futures_book,
                ),
            },
        })
        .expect("register maintained-book handler");
        let register = |channel, params| {
            register_batch(
                &bus,
                WsUrl::UnifiedPublic,
                vec![SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    channel,
                    Some(symbol.clone()),
                    params,
                )],
            );
        };
        let row_acked = |channel| {
            sub_mirror
                .read()
                .expect("subscription mirror")
                .get(&(WsUrl::UnifiedPublic, channel, Some(symbol.clone())))
                .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
        };

        register(
            ChannelName::Book,
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        );
        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
        let subscribe: serde_json::Value =
            serde_json::from_str(&socket.sent_text()[0]).expect("maintained subscribe JSON");
        socket.emit_text(
            serde_json::json!({
                "method": "subscribe",
                "success": true,
                "req_id": subscribe["req_id"],
                "result": { "channel": "book", "symbol": "PI_XBTUSD" },
            })
            .to_string(),
        );
        let checksum =
            crate::book::compute_book_crc32([("101", "1")].into_iter(), [("100", "1")].into_iter());
        socket.emit_text(
            serde_json::json!({
                "channel": "book", "type": "snapshot", "data": [{
                    "symbol": "PI_XBTUSD", "bids": [{"price": "100", "qty": "1"}],
                    "asks": [{"price": "101", "qty": "1"}], "checksum": checksum
                }]
            })
            .to_string(),
        );
        tokio::time::timeout(BUDGET, async {
            loop {
                if row_acked(ChannelName::Book) && maintained_updates.load(Ordering::SeqCst) == 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("maintained book acknowledgement and seed");

        let raw_snapshots = Arc::new(AtomicUsize::new(0));
        let raw_updates = Arc::new(AtomicUsize::new(0));
        let raw_snapshots_cb = Arc::clone(&raw_snapshots);
        let raw_updates_cb = Arc::clone(&raw_updates);
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::UnifiedPublic,
            channel: ChannelName::BookRaw,
            op: HandlerMutationOp::Register {
                id: HandlerId(47_111),
                callback: wrap_typed(
                    move |delta: &FuturesBookDelta| {
                        if delta.is_snapshot {
                            raw_snapshots_cb.fetch_add(1, Ordering::SeqCst);
                        } else {
                            raw_updates_cb.fetch_add(1, Ordering::SeqCst);
                        }
                    },
                    decode_futures_book_raw,
                ),
            },
        })
        .expect("register raw-book handler");
        register(
            ChannelName::BookRaw,
            SubscribeParams::BookRaw {
                depth: BookDepth::D25,
                snapshot: Some(true),
            },
        );
        tokio::time::timeout(BUDGET, async {
            loop {
                let raw_subscribed =
                    subscribed
                        .lock()
                        .expect("subscribed events")
                        .iter()
                        .any(|event| {
                            matches!(
                                &event.payload,
                                EventPayload::FuturesSubscribedEvent {
                                    channel: ChannelName::BookRaw,
                                    symbol: Some(event_symbol),
                                    ..
                                } if event_symbol == "PI_XBTUSD"
                            )
                        });
                if row_acked(ChannelName::BookRaw) && raw_subscribed {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("late raw owner attaches as acked with a subscribed event");
        assert_eq!(
            sub_mirror
                .read()
                .expect("subscription mirror")
                .get(&(
                    WsUrl::UnifiedPublic,
                    ChannelName::BookRaw,
                    Some(symbol.clone())
                ))
                .and_then(|row| row.subscribed_at),
            Some(at),
            "the late raw attach is stamped from the injected clock"
        );
        assert!(row_acked(ChannelName::Book));
        assert_eq!(
            socket.sent_text().len(),
            1,
            "a late raw join sends no unsubscribe or resubscribe"
        );

        socket.emit_text(
            serde_json::json!({
                "channel": "book", "type": "update", "data": [{
                    "symbol": "PI_XBTUSD", "bids": [], "asks": [], "checksum": checksum
                }]
            })
            .to_string(),
        );
        tokio::time::timeout(BUDGET, async {
            loop {
                if maintained_updates.load(Ordering::SeqCst) == 2
                    && raw_updates.load(Ordering::SeqCst) == 1
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the next venue frame reaches both owners");
        assert_eq!(raw_snapshots.load(Ordering::SeqCst), 0);
        assert_eq!(socket.sent_text().len(), 1);

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_futures_shared_book_ack_activates_book_raw_then_book() {
        let registration_order = [ChannelName::BookRaw, ChannelName::Book];
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unused-token", token_envelope("unused"));
        let subscribed = Arc::new(Mutex::new(Vec::<EventEnvelope>::new()));
        let subscribed_sink = Arc::clone(&subscribed);
        let _subscribed_subscription = bus.subscribe(
            EventType::FuturesSubscribedEvent,
            Arc::new(move |event: &EventEnvelope| {
                subscribed_sink
                    .lock()
                    .expect("subscribed events")
                    .push(event.clone());
            }),
            u16::MAX,
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, _auth_mirror) =
            spawn_reactor_with_sub_mirror(&bus, &factory, auth_stack, Arc::clone(&sub_mirror));
        let symbol = StreamSymbol::Futures("PI_XBTUSD".to_string());

        let register = |channel, params| {
            register_batch(
                &bus,
                WsUrl::UnifiedPublic,
                vec![SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    channel,
                    Some(symbol.clone()),
                    params,
                )],
            );
        };
        let params_for = |channel| match channel {
            ChannelName::Book => SubscribeParams::Book {
                depth: BookDepth::D10,
            },
            ChannelName::BookRaw => SubscribeParams::BookRaw {
                depth: BookDepth::D25,
                snapshot: Some(true),
            },
            _ => unreachable!("test only registers logical book channels"),
        };
        register(registration_order[0], params_for(registration_order[0]));

        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
        register(registration_order[1], params_for(registration_order[1]));
        let frame: serde_json::Value =
            serde_json::from_str(&socket.sent_text()[0]).expect("Futures book subscribe JSON");
        socket.emit_text(
            serde_json::json!({
                "method": "subscribe",
                "success": true,
                "req_id": frame["req_id"],
                "result": { "channel": "book", "symbol": "PI_XBTUSD" },
            })
            .to_string(),
        );
        tokio::time::timeout(BUDGET, async {
            loop {
                let book_acked = sub_mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(
                        WsUrl::UnifiedPublic,
                        ChannelName::Book,
                        Some(symbol.clone()),
                    ))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked));
                let raw_acked = sub_mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(
                        WsUrl::UnifiedPublic,
                        ChannelName::BookRaw,
                        Some(symbol.clone()),
                    ))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked));
                let subscribed_channels: Vec<_> = subscribed
                    .lock()
                    .expect("subscribed events")
                    .iter()
                    .filter_map(|event| match &event.payload {
                        EventPayload::FuturesSubscribedEvent {
                            channel,
                            symbol: event_symbol,
                            ..
                        } if event_symbol.as_deref() == Some("PI_XBTUSD") => Some(*channel),
                        _ => None,
                    })
                    .collect();
                if book_acked
                    && raw_acked
                    && subscribed_channels.contains(&ChannelName::Book)
                    && subscribed_channels.contains(&ChannelName::BookRaw)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shared Futures book ack did not activate both logical rows");
        assert_eq!(
            socket.sent_text().len(),
            1,
            "logical book registrations must share one wire subscribe"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_futures_book_raw_teardown_waits_for_final_logical_owner() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unused-token", token_envelope("unused"));
        let (reactor, _auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);
        let symbol = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let register = |channel, params| {
            register_batch(
                &bus,
                WsUrl::UnifiedPublic,
                vec![SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    channel,
                    Some(symbol.clone()),
                    params,
                )],
            );
        };
        let deregister = |channel| {
            bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::UnifiedPublic,
                mutation: RegistryMutationOp::Deregister {
                    channel,
                    pair: Some(symbol.clone()),
                },
            })
            .expect("post Futures book release");
        };
        let raw = SubscribeParams::BookRaw {
            depth: BookDepth::D25,
            snapshot: Some(true),
        };
        let maintained = SubscribeParams::Book {
            depth: BookDepth::D10,
        };

        register(ChannelName::Book, maintained);
        register(ChannelName::BookRaw, raw);
        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
        let closed = Arc::new(Mutex::new(Vec::<crate::dispatch::EventEnvelope>::new()));
        let closed_sink = Arc::clone(&closed);
        let _closed_subscription = bus.subscribe(
            crate::dispatch::EventType::FuturesSubscriptionTerminatedEvent,
            Arc::new(move |event: &crate::dispatch::EventEnvelope| {
                closed_sink
                    .lock()
                    .expect("closed events")
                    .push(event.clone());
            }),
            u16::MAX,
        );

        deregister(ChannelName::Book);
        tokio::time::timeout(BUDGET, async {
            loop {
                if closed.lock().expect("closed events").len() == 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("releasing one book owner emits ClientClosed");
        assert!(closed.lock().expect("closed events").iter().any(|event| {
            matches!(
                &event.payload,
                crate::dispatch::EventPayload::FuturesSubscriptionTerminatedEvent {
                    channel: ChannelName::Book,
                    symbol: Some(symbol),
                    cause: crate::api::subscription::TerminationCause::ClientClosed,
                    ..
                } if symbol == "PI_XBTUSD"
            )
        }));
        assert_eq!(
            socket.sent_text().len(),
            1,
            "releasing maintained first must retain raw's wire subscription"
        );
        deregister(ChannelName::BookRaw);
        let _ = wait_for_frames(&socket, "unsubscribe", 1, "final owner unsubscribe").await;

        register(ChannelName::BookRaw, raw);
        register(ChannelName::Book, maintained);
        let _ = wait_for_frames(&socket, "\"subscribe\"", 2, "second shared subscribe").await;
        deregister(ChannelName::BookRaw);
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            socket
                .sent_text()
                .iter()
                .filter(|frame| frame.contains("unsubscribe"))
                .count(),
            1,
            "releasing raw first must retain maintained's wire subscription"
        );
        deregister(ChannelName::Book);
        let _ = wait_for_frames(&socket, "unsubscribe", 2, "reverse final owner unsubscribe").await;

        register(ChannelName::Book, maintained);
        register(ChannelName::BookRaw, raw);
        let _ = wait_for_frames(&socket, "\"subscribe\"", 3, "third shared subscribe").await;
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::UnifiedPublic,
            mutation: RegistryMutationOp::DeregisterAll {
                channel: Some(ChannelName::Book),
            },
        })
        .expect("channel teardown");
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            socket
                .sent_text()
                .iter()
                .filter(|frame| frame.contains("unsubscribe"))
                .count(),
            2,
            "channel teardown must leave the peer owner live"
        );
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::UnifiedPublic,
            mutation: RegistryMutationOp::DeregisterAll {
                channel: Some(ChannelName::BookRaw),
            },
        })
        .expect("final channel teardown");
        let _ = wait_for_frames(&socket, "unsubscribe", 3, "channel final owner unsubscribe").await;

        register(ChannelName::Book, maintained);
        register(ChannelName::BookRaw, raw);
        let _ = wait_for_frames(&socket, "\"subscribe\"", 4, "fourth shared subscribe").await;
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::UnifiedPublic,
            mutation: RegistryMutationOp::DeregisterAll { channel: None },
        })
        .expect("all teardown");
        let unsubs = wait_for_frames(&socket, "unsubscribe", 4, "all teardown unsubscribe").await;
        assert_eq!(
            unsubs.len(),
            4,
            "all teardown must produce one physical unsubscribe for Book/BookRaw"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_futures_released_pending_book_owner_leaves_peer_retrying() {
        released_book_owner_leaves_raw_peer_retrying(false).await;
    }

    #[tokio::test]
    async fn unified_futures_owner_released_in_resend_window_leaves_fresh_peer_retrying() {
        released_book_owner_leaves_raw_peer_retrying(true).await;
    }

    /// Releases the maintained owner while it is Pending (or, with `in_resend_window`, after its ack timer fired); the raw peer must retry on its own.
    async fn released_book_owner_leaves_raw_peer_retrying(in_resend_window: bool) {
        let (bus, factory) = bus_and_factory();
        bus.knobs()
            .subscribe_ack_timeout_ms
            .store(50, std::sync::atomic::Ordering::Relaxed);
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_public_mirror,
                ..
            },
            _,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        let failed = Arc::new(Mutex::new(Vec::<EventEnvelope>::new()));
        let failed_sink = Arc::clone(&failed);
        let _failed_subscription = bus.subscribe(
            EventType::FuturesSubscribeFailedEvent,
            Arc::new(move |event: &EventEnvelope| {
                failed_sink
                    .lock()
                    .expect("failed events")
                    .push(event.clone());
            }),
            u16::MAX,
        );
        let failed_on = |channel: ChannelName| {
            failed.lock().expect("failed events").iter().any(|event| {
                matches!(
                    &event.payload,
                    EventPayload::FuturesSubscribeFailedEvent {
                        channel: failed_channel,
                        symbol: Some(symbol),
                        ..
                    } if *failed_channel == channel && symbol == "PI_XBTUSD"
                )
            })
        };
        let symbol = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let register = |channel, params| {
            register_batch(
                &bus,
                WsUrl::UnifiedPublic,
                vec![SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    channel,
                    Some(symbol.clone()),
                    params,
                )],
            );
        };

        register(
            ChannelName::Book,
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        );
        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
        wait_for_state(
            &unified_public_mirror,
            ConnectionState::Resubscribing,
            "Unified Public awaits the book ack",
        )
        .await;
        if in_resend_window {
            tokio::time::timeout(BUDGET, async {
                while !failed_on(ChannelName::Book) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("maintained book ack timer fires");
        }
        register(
            ChannelName::BookRaw,
            SubscribeParams::BookRaw {
                depth: BookDepth::D25,
                snapshot: Some(true),
            },
        );
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::UnifiedPublic,
            mutation: RegistryMutationOp::Deregister {
                channel: ChannelName::Book,
                pair: Some(symbol.clone()),
            },
        })
        .expect("release the maintained owner");

        let expected = if in_resend_window { 3 } else { 2 };
        let subscribes = wait_for_frames(
            &socket,
            "\"subscribe\"",
            expected,
            "raw peer subscribe retries",
        )
        .await;
        for frame in &subscribes[1..] {
            let frame: serde_json::Value = serde_json::from_str(frame).expect("raw subscribe JSON");
            assert_eq!(frame["params"]["channel"], "book");
            assert_eq!(frame["params"]["depth"], 25);
        }
        assert!(failed_on(ChannelName::BookRaw));

        reactor.abort();
        bus.stop_reactors();
    }

    #[allow(clippy::type_complexity)]
    async fn reject_unified_subscribe_while_connecting(
        entries: Vec<SubscriptionEntry>,
        replies: impl FnOnce(u64) -> Vec<serde_json::Value>,
    ) -> (
        Vec<(ChannelName, Option<String>)>,
        Vec<(ChannelName, Option<String>, u32)>,
    ) {
        let bus = Arc::new(DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            Arc::new(SystemClock) as Arc<dyn Clock>,
        ));
        bus.knobs()
            .subscribe_ack_timeout_ms
            .store(40, std::sync::atomic::Ordering::Relaxed);
        let factory = Arc::new(DriveableWsSocketFactory::new(Arc::clone(&bus)));
        let (
            ReactorHandles {
                reactor,
                unified_public: unified_public_mirror,
                ..
            },
            _,
            _rest,
        ) = spawn_unused_token_rig(&bus, &factory);
        register_batch(&bus, WsUrl::UnifiedPublic, entries);
        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
        let frame: serde_json::Value =
            serde_json::from_str(&socket.sent_text()[0]).expect("subscribe JSON");
        tokio::time::sleep(Duration::from_millis(100)).await;
        for reply in replies(frame["req_id"].as_u64().expect("req_id")) {
            socket.emit_text(reply.to_string());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            ConnectionState::from_u8(
                unified_public_mirror.load(std::sync::atomic::Ordering::Acquire)
            ),
            Some(ConnectionState::Connecting),
            "reject handled while the upgrade is still pending"
        );
        let events = Arc::new(Mutex::new(Vec::<EventEnvelope>::new()));
        for event_type in [
            EventType::FuturesSubscriptionTerminatedEvent,
            EventType::FuturesSubscribeFailedEvent,
        ] {
            let sink = Arc::clone(&events);
            let _subscription = bus.subscribe(
                event_type,
                Arc::new(move |event: &EventEnvelope| {
                    sink.lock().expect("events").push(event.clone());
                }),
                u16::MAX,
            );
        }

        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        wait_for_state(
            &unified_public_mirror,
            ConnectionState::Open,
            "nothing is left pending",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            socket
                .sent_text()
                .iter()
                .filter(|frame| frame.contains("\"subscribe\""))
                .count(),
            1,
            "a rejected subscribe is not sent again"
        );
        reactor.abort();
        bus.stop_reactors();

        assert!(
            events
                .lock()
                .expect("events")
                .iter()
                .all(|event| event.event_version == 1),
            "Futures subscription lifecycle events are version 1"
        );
        let mut terminated = Vec::new();
        let mut failed = Vec::new();
        for event in events.lock().expect("events").iter() {
            match &event.payload {
                EventPayload::FuturesSubscriptionTerminatedEvent {
                    channel, symbol, ..
                } => terminated.push((*channel, symbol.clone())),
                EventPayload::FuturesSubscribeFailedEvent {
                    channel,
                    symbol,
                    attempt,
                    ..
                } => failed.push((*channel, symbol.clone(), *attempt)),
                _ => {}
            }
        }
        terminated.sort();
        failed.sort();
        (terminated, failed)
    }

    #[tokio::test]
    async fn unified_futures_ticker_rejected_while_connecting_terminates_without_resend() {
        let ticker = |symbol: &str| {
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(StreamSymbol::Futures(symbol.to_string())),
                ticker_params(),
            )
        };
        let (terminated, failed) = reject_unified_subscribe_while_connecting(
            vec![ticker("PI_XBTUSD"), ticker("PI_NOTAREAL")],
            |req_id| {
                vec![
                    serde_json::json!({
                        "method": "subscribe",
                        "req_id": req_id,
                        "success": true,
                        "result": { "channel": "ticker", "symbol": "PI_XBTUSD" }
                    }),
                    serde_json::json!({
                        "method": "subscribe",
                        "req_id": req_id,
                        "success": false,
                        "error": "Unknown symbol: PI_NOTAREAL",
                        "result": { "channel": "ticker", "symbol": "PI_NOTAREAL" }
                    }),
                ]
            },
        )
        .await;

        let rejected = Some("PI_NOTAREAL".to_string());
        assert_eq!(terminated, vec![(ChannelName::Ticker, rejected.clone())]);
        assert_eq!(failed, vec![(ChannelName::Ticker, rejected, 1)]);
    }

    #[tokio::test]
    async fn unified_futures_book_rejected_while_connecting_terminates_owner_and_pending_peer() {
        let pair = Some(StreamSymbol::Futures("PI_NOTAREAL".to_string()));
        let (terminated, failed) = reject_unified_subscribe_while_connecting(
            vec![
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    pair.clone(),
                    SubscribeParams::Book {
                        depth: BookDepth::D10,
                    },
                ),
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::BookRaw,
                    pair,
                    SubscribeParams::BookRaw {
                        depth: BookDepth::D25,
                        snapshot: None,
                    },
                ),
            ],
            |req_id| {
                vec![serde_json::json!({
                    "method": "subscribe",
                    "req_id": req_id,
                    "success": false,
                    "error": "Unknown symbol: PI_NOTAREAL",
                    "result": { "channel": "book", "symbol": "PI_NOTAREAL" }
                })]
            },
        )
        .await;

        let rejected = Some("PI_NOTAREAL".to_string());
        assert_eq!(
            terminated,
            vec![
                (ChannelName::Book, rejected.clone()),
                (ChannelName::BookRaw, rejected.clone()),
            ]
        );
        assert_eq!(
            failed,
            vec![
                (ChannelName::Book, rejected.clone(), 1),
                (ChannelName::BookRaw, rejected, 1),
            ]
        );
    }

    #[tokio::test]
    async fn unified_futures_raw_only_book_ignores_bad_checksums_without_recovery() {
        use crate::api::market::decode_futures_book_raw;
        use crate::api::ws_decode::wrap_typed;
        use crate::book::FuturesBookDelta;
        use crate::dispatch::HandlerId;
        use std::sync::atomic::AtomicUsize;

        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unused-token", token_envelope("unused"));
        let order_book_gaps = Arc::new(Mutex::new(Vec::<EventEnvelope>::new()));
        let subscription_gaps = Arc::new(Mutex::new(Vec::<EventEnvelope>::new()));
        let order_book_sink = Arc::clone(&order_book_gaps);
        let subscription_sink = Arc::clone(&subscription_gaps);
        let _order_book_subscription = bus.subscribe(
            EventType::FuturesOrderBookGapEvent,
            Arc::new(move |event: &EventEnvelope| {
                order_book_sink
                    .lock()
                    .expect("order-book gaps")
                    .push(event.clone());
            }),
            u16::MAX,
        );
        let _subscription_subscription = bus.subscribe(
            EventType::FuturesSubscriptionGapEvent,
            Arc::new(move |event: &EventEnvelope| {
                subscription_sink
                    .lock()
                    .expect("subscription gaps")
                    .push(event.clone());
            }),
            u16::MAX,
        );
        let seen = Arc::new(AtomicUsize::new(0));
        let raw_seen = Arc::clone(&seen);
        bus.try_post_caller_inbound(CallerInbound::RegisterHandlerAndConnect {
            url: WsUrl::UnifiedPublic,
            channel: ChannelName::BookRaw,
            id: HandlerId(47_010),
            callback: wrap_typed(
                move |_: &FuturesBookDelta| {
                    raw_seen.fetch_add(1, Ordering::SeqCst);
                },
                decode_futures_book_raw,
            ),
        })
        .expect("register raw Futures book handler");

        let (reactor, _auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);
        register_batch(
            &bus,
            WsUrl::UnifiedPublic,
            vec![SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                SubscribeParams::BookRaw {
                    depth: BookDepth::D10,
                    snapshot: Some(false),
                },
            )],
        );

        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;

        for kind in ["snapshot", "update"] {
            socket.emit_text(
                serde_json::json!({
                    "channel": "book", "type": kind, "data": [{
                        "symbol": "PI_XBTUSD",
                        "bids": [{"price": "100", "qty": "1"}],
                        "asks": [{"price": "101", "qty": "1"}],
                        "checksum": 0
                    }]
                })
                .to_string(),
            );
        }
        tokio::time::timeout(BUDGET, async {
            loop {
                if seen.load(Ordering::SeqCst) == 2 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("raw Futures book handler did not receive both frames");

        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        assert!(order_book_gaps.lock().expect("order-book gaps").is_empty());
        assert!(
            subscription_gaps
                .lock()
                .expect("subscription gaps")
                .is_empty()
        );
        assert_eq!(
            socket.sent_text().len(),
            1,
            "raw-only book must not send CRC recovery frames"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    fn register_counting_unified_handler(
        bus: &Arc<DispatchEventBus>,
        channel: ChannelName,
        id: u64,
        count: &Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let count = Arc::clone(count);
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::UnifiedPublic,
            channel,
            op: crate::dispatch::event_bus::HandlerMutationOp::Register {
                id: crate::dispatch::HandlerId(id),
                callback: crate::dispatch::HandlerCallback::new(
                    Arc::new(|_| crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))),
                    Arc::new(move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                    }),
                ),
            },
        })
        .expect("register counting Unified handler");
    }

    async fn wait_for_count(count: &Arc<std::sync::atomic::AtomicUsize>, want: usize, what: &str) {
        tokio::time::timeout(BUDGET, async {
            loop {
                if count.load(Ordering::SeqCst) == want {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}: expected {want} deliveries"));
    }

    #[tokio::test]
    async fn unified_bare_book_handler_ignores_raw_only_symbol() {
        use std::sync::atomic::AtomicUsize;

        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unused-token", token_envelope("unused"));
        let (reactor, _auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);
        let book = Arc::new(AtomicUsize::new(0));
        let raw = Arc::new(AtomicUsize::new(0));
        register_counting_unified_handler(&bus, ChannelName::Book, 47_201, &book);
        register_counting_unified_handler(&bus, ChannelName::BookRaw, 47_202, &raw);
        register_batch(
            &bus,
            WsUrl::UnifiedPublic,
            vec![SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                SubscribeParams::BookRaw {
                    depth: BookDepth::D10,
                    snapshot: Some(true),
                },
            )],
        );
        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;

        for kind in ["snapshot", "update"] {
            socket.emit_text(
                serde_json::json!({
                    "channel": "book", "type": kind, "data": [{
                        "symbol": "PI_XBTUSD",
                        "bids": [{"price": "100", "qty": "1"}],
                        "asks": [{"price": "101", "qty": "1"}],
                        "checksum": 0
                    }]
                })
                .to_string(),
            );
        }
        wait_for_count(&raw, 2, "raw handler").await;
        assert_eq!(
            book.load(Ordering::SeqCst),
            0,
            "a raw-only symbol must not reach a maintained on_book handler"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_book_release_beside_raw_owner_stops_book_delivery() {
        use std::sync::atomic::AtomicUsize;

        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "unused-token", token_envelope("unused"));
        let (reactor, _auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);
        let symbol = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let book = Arc::new(AtomicUsize::new(0));
        let raw = Arc::new(AtomicUsize::new(0));
        register_counting_unified_handler(&bus, ChannelName::Book, 47_211, &book);
        register_counting_unified_handler(&bus, ChannelName::BookRaw, 47_212, &raw);
        for (channel, params) in [
            (
                ChannelName::Book,
                SubscribeParams::Book {
                    depth: BookDepth::D10,
                },
            ),
            (
                ChannelName::BookRaw,
                SubscribeParams::BookRaw {
                    depth: BookDepth::D25,
                    snapshot: Some(true),
                },
            ),
        ] {
            register_batch(
                &bus,
                WsUrl::UnifiedPublic,
                vec![SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    channel,
                    Some(symbol.clone()),
                    params,
                )],
            );
        }
        let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;

        let checksum =
            crate::book::compute_book_crc32([("101", "1")].into_iter(), [("100", "1")].into_iter());
        socket.emit_text(
            serde_json::json!({
                "channel": "book", "type": "snapshot", "data": [{
                    "symbol": "PI_XBTUSD", "bids": [{"price": "100", "qty": "1"}],
                    "asks": [{"price": "101", "qty": "1"}], "checksum": checksum
                }]
            })
            .to_string(),
        );
        wait_for_count(&raw, 1, "raw handler before release").await;
        assert_eq!(
            book.load(Ordering::SeqCst),
            1,
            "maintained snapshot delivered"
        );

        let closed = Arc::new(Mutex::new(Vec::<EventEnvelope>::new()));
        let closed_sink = Arc::clone(&closed);
        let _closed_subscription = bus.subscribe(
            EventType::FuturesSubscriptionTerminatedEvent,
            Arc::new(move |event: &EventEnvelope| {
                closed_sink
                    .lock()
                    .expect("closed events")
                    .push(event.clone());
            }),
            u16::MAX,
        );
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::UnifiedPublic,
            mutation: RegistryMutationOp::DeregisterBatch {
                channel: ChannelName::Book,
                pairs: vec![symbol.clone()],
            },
        })
        .expect("post maintained book release");
        tokio::time::timeout(BUDGET, async {
            loop {
                if !closed.lock().expect("closed events").is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("maintained book release was not processed");

        for kind in ["update", "snapshot"] {
            socket.emit_text(
                serde_json::json!({
                    "channel": "book", "type": kind, "data": [{
                        "symbol": "PI_XBTUSD", "bids": [{"price": "99", "qty": "2"}],
                        "asks": [], "checksum": 0
                    }]
                })
                .to_string(),
            );
        }
        wait_for_count(&raw, 3, "raw handler after release").await;
        assert_eq!(
            book.load(Ordering::SeqCst),
            1,
            "a released maintained book must not receive the raw owner's frames"
        );
        assert_eq!(
            socket.sent_text().len(),
            1,
            "raw owner keeps the wire stream"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_explicit_unsubscribe_keeps_guarded_book_stream_and_intent() {
        use crate::api::subscription_types::SubscriptionError;
        use crate::api::ws_surface::WsSurface;

        for (channel, params, incompatible) in [
            (
                ChannelName::Book,
                SubscribeParams::Book {
                    depth: BookDepth::D10,
                },
                SubscribeParams::Book {
                    depth: BookDepth::D100,
                },
            ),
            (
                ChannelName::BookRaw,
                SubscribeParams::BookRaw {
                    depth: BookDepth::D10,
                    snapshot: Some(false),
                },
                SubscribeParams::BookRaw {
                    depth: BookDepth::D10,
                    snapshot: Some(true),
                },
            ),
        ] {
            let (bus, factory) = bus_and_factory();
            let (
                ReactorHandles {
                    reactor,
                    unified_public: unified_state,
                    ..
                },
                mirror,
                _rest,
            ) = spawn_unused_token_rig(&bus, &factory);
            let surface = WsSurface::new(
                Arc::clone(&bus),
                Arc::new(std::sync::RwLock::new(HashMap::new())),
                Arc::new(AtomicU64::new(1)),
            );
            let owned = StreamSymbol::Futures("PI_XBTUSD".to_string());
            let sentinel = StreamSymbol::Futures("PF_ETHUSD".to_string());
            let in_mirror = |symbol: &StreamSymbol| {
                mirror.read().unwrap().contains_key(&(
                    WsUrl::UnifiedPublic,
                    channel,
                    Some(symbol.clone()),
                ))
            };
            let guard = surface
                .register_and_subscribe_guarded_for(
                    WsUrl::UnifiedPublic,
                    channel,
                    vec![owned.clone()],
                    params,
                    crate::dispatch::HandlerCallback::new(
                        Arc::new(|_| crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))),
                        Arc::new(|_| {}),
                    ),
                )
                .expect("guarded book owner");
            let socket = socket_with_sent(&factory, WsUrl::UnifiedPublic, 1).await;
            socket.emit_text(super::reconnect_drive_tests::ack_text(
                "book",
                Some("PI_XBTUSD"),
            ));
            wait_for_state(&unified_state, ConnectionState::Open, "guarded book ack").await;

            surface
                .unsubscribe_for(WsUrl::UnifiedPublic, channel, vec![owned.clone()])
                .expect("explicit unsubscribe posts");
            surface
                .subscribe_for(
                    WsUrl::UnifiedPublic,
                    channel,
                    vec![sentinel.clone()],
                    params,
                )
                .expect("sentinel subscribe");
            tokio::time::timeout(BUDGET, async {
                while !in_mirror(&sentinel) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("sentinel was not registered");
            assert!(
                in_mirror(&owned),
                "{channel:?}: explicit unsubscribe must not release the guard's registry slot"
            );
            assert!(
                !socket
                    .sent_text()
                    .iter()
                    .any(|frame| frame.contains("unsubscribe")),
                "{channel:?}: the guarded stream stays on the wire"
            );
            let err = surface
                .subscribe_for(
                    WsUrl::UnifiedPublic,
                    channel,
                    vec![owned.clone()],
                    incompatible,
                )
                .expect_err("live guarded stream keeps its parameters");
            assert!(
                matches!(err, SubscriptionError::InvalidArguments { .. }),
                "{channel:?}: {err:?}"
            );

            drop(guard);
            tokio::time::timeout(BUDGET, async {
                while in_mirror(&owned) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("guard drop did not release the stream");
            surface
                .subscribe_for(
                    WsUrl::UnifiedPublic,
                    channel,
                    vec![owned.clone()],
                    incompatible,
                )
                .expect("released stream accepts new parameters");

            reactor.abort();
            bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn unified_private_missing_token_refresh_sends_token_and_derivatives() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};

        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "expired-token",
            token_envelope("refreshed-unified-token"),
        );
        auth_stack.token_lifecycle().clear_cached_token_for_test();
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, _auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        register_batch(
            &bus,
            WsUrl::UnifiedPrivate,
            vec![SubscriptionEntry::new(
                WsUrl::UnifiedPrivate,
                ChannelName::Executions,
                None,
                SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
            )],
        );

        let socket = socket_with_at_least(&factory, WsUrl::UnifiedPrivate, 1).await;
        let frame: serde_json::Value =
            serde_json::from_str(&socket.sent_text()[0]).expect("sent frame JSON");
        assert_eq!(frame["params"]["token"], "refreshed-unified-token");
        assert_eq!(frame["params"]["asset_class"], "derivatives");

        socket.emit_text(include_str!(
            "../../../tests/fixtures/unified_ws/private_executions_subscribe_ack.json"
        ));
        tokio::time::timeout(BUDGET, async {
            loop {
                if sub_mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(WsUrl::UnifiedPrivate, ChannelName::Executions, None))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Unified private refresh subscribe did not reach Acked");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn order_gate_tracks_subscriptions_per_token_url() {
        use crate::conn::subscription_registry::SubscriptionMirror;

        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "gate-token", token_envelope("unused"));
        let ReactorHandles {
            reactor,
            order_gate,
            ..
        } = spawn_reactor_handles(&bus, &factory, auth_stack, SubscriptionMirror::default());
        let register = |url: WsUrl, channel: ChannelName, params: SubscribeParams| {
            register_batch(
                &bus,
                url,
                vec![SubscriptionEntry::new(url, channel, None, params)],
            );
        };
        let wait = |url: WsUrl, want: bool, what: &'static str| {
            let order_gate = Arc::clone(&order_gate);
            async move {
                tokio::time::timeout(BUDGET, async {
                    while order_gate.has_subscriptions(url) != want {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap_or_else(|_| panic!("timeout: {what}"));
            }
        };

        register(
            WsUrl::Auth,
            ChannelName::Executions,
            SubscribeParams::Executions {
                snap_orders: None,
                snap_trades: None,
            },
        );
        wait(WsUrl::Auth, true, "Spot executions registered").await;
        assert!(!order_gate.has_subscriptions(WsUrl::UnifiedPrivate));

        register(
            WsUrl::UnifiedPrivate,
            ChannelName::Preferences,
            SubscribeParams::Preferences,
        );
        wait(
            WsUrl::UnifiedPrivate,
            true,
            "Unified preferences registered",
        )
        .await;

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::DeregisterAll { channel: None },
        })
        .expect("post deregister-all");
        wait(WsUrl::Auth, false, "Spot executions deregistered").await;
        assert!(
            order_gate.has_subscriptions(WsUrl::UnifiedPrivate),
            "clearing Spot must not clear the Unified private hint"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn unified_private_live_row_preserves_spot_auth_send_ready() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};

        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "spot-auth-ready-token", token_envelope("unused"));
        let sub_mirror = SubscriptionMirror::default();
        let ReactorHandles {
            reactor,
            auth: auth_mirror,
            order_gate,
            ..
        } = spawn_reactor_handles(&bus, &factory, auth_stack, Arc::clone(&sub_mirror));

        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::Auth,
            event: crate::types::CallerEvent::StartConnect { request_id: 1 },
        })
        .expect("start Spot Auth connection");
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "Spot Auth bare-order handshake",
        )
        .await;
        tokio::time::timeout(BUDGET, async {
            while !order_gate.send_ready(WsUrl::Auth) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Spot Auth never reached send-ready");

        register_batch(
            &bus,
            WsUrl::UnifiedPrivate,
            vec![SubscriptionEntry::new(
                WsUrl::UnifiedPrivate,
                ChannelName::Executions,
                None,
                SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
            )],
        );

        let socket = socket_with_at_least(&factory, WsUrl::UnifiedPrivate, 1).await;
        socket.emit_text(include_str!(
            "../../../tests/fixtures/unified_ws/private_executions_subscribe_ack.json"
        ));

        tokio::time::timeout(BUDGET, async {
            loop {
                if sub_mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(WsUrl::UnifiedPrivate, ChannelName::Executions, None))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Unified private executions row did not reach Acked");
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Authenticating,
            "Unified private subscription must not alter Spot Auth state"
        );
        assert!(
            order_gate.send_ready(WsUrl::Auth),
            "Unified private live row must not clear Spot Auth send-ready"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn spot_deregister_all_removes_auth_subscriptions() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "spot-auth-token", token_envelope("unused"));
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let (reactor, _auth_mirror) =
            spawn_reactor_with_sub_mirror(&bus, &factory, auth_stack, Arc::clone(&mirror));

        register_executions(&bus);

        let socket = socket_with_at_least(&factory, WsUrl::Auth, 1).await;

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Public,
            mutation: RegistryMutationOp::DeregisterAll { channel: None },
        })
        .expect("post Spot deregister-all");

        let frames = tokio::time::timeout(BUDGET, async {
            loop {
                let frames = socket.sent_text();
                if frames.iter().any(|frame| frame.contains("\"unsubscribe\"")) {
                    return frames;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Spot deregister-all did not unsubscribe Auth executions");

        assert!(frames.iter().any(|frame| frame.contains("\"unsubscribe\"")));
        assert!(
            mirror
                .read()
                .expect("subscription mirror")
                .get(&(WsUrl::Auth, ChannelName::Executions, None))
                .is_none()
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_auth_register_while_open_no_token_refreshes_and_reissues_m2() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-M3", token_envelope("fresh-token-M3"));
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, Arc::clone(&auth_stack));

        let auth_sock = drive_auth_to_open(&bus, &factory, &auth_mirror).await;
        assert_exactly_n_sent(&auth_sock, 1, "after open (only the executions sub)").await;

        auth_stack.token_lifecycle().clear_cached_token_for_test();
        assert!(
            auth_stack.cached_token().is_none(),
            "token cleared — simulates expiry-while-Open"
        );

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                SubscribeParams::Balances { snapshot: None },
            )],
        );

        let (channel, token) = nth_sent_token(&auth_sock, 1).await;
        assert_eq!(
            channel, "balances",
            "deferred balances subscribe re-issued after refresh"
        );
        assert_eq!(
            token, "fresh-token-M3",
            "re-issued subscribe carries the FRESH refreshed token (proves force_refresh ran)"
        );

        assert_exactly_n_sent(
            &auth_sock,
            2,
            "executions (acked) NOT re-subscribed on refresh",
        )
        .await;

        auth_sock.emit_text(ack_ok("balances"));
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Open,
            "auth conn stays Open after the re-issued balances ack (self-loop, no FSM move)"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_register_batch_expired_token_single_flight_force_refresh_w2_4() {
        use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};

        let call_count = Arc::new(AtomicUsize::new(0));
        let envelope = serde_json::json!({
            "error": [],
            "result": { "token": "fresh-token-W2-4", "expires": 900u32 }
        });
        let counting_transport: Arc<dyn HttpTransport> = {
            let calls = Arc::clone(&call_count);
            let env_get = envelope.clone();
            Arc::new(ClosureHttpMock::new(
                Box::new(move || Ok(env_get.clone())),
                Box::new(move || {
                    calls.fetch_add(1, AOrdering::SeqCst);
                    Ok(envelope.clone())
                }),
            ))
        };

        let (bus, factory) = bus_and_factory();
        let (auth_stack, rest_keepalive) =
            auth_stack_with_transport(&bus, "seeded-token-W2-4", counting_transport);
        std::mem::forget(rest_keepalive);

        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, Arc::clone(&auth_stack));

        let auth_sock = drive_auth_to_open(&bus, &factory, &auth_mirror).await;
        assert_exactly_n_sent(&auth_sock, 1, "after open (only the executions sub)").await;
        assert_eq!(
            call_count.load(AOrdering::SeqCst),
            0,
            "bring-up must NOT have called force_refresh (token was valid)"
        );

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::Deregister {
                channel: ChannelName::Executions,
                pair: None,
            },
        })
        .expect("deregister executions to clear refcount to 0");
        assert_exactly_n_sent(&auth_sock, 2, "after deregister: unsubscribe frame sent").await;
        let unsub = serde_json::from_str::<serde_json::Value>(&auth_sock.sent_text()[1])
            .expect("unsubscribe frame #1 is JSON");
        assert_eq!(unsub["method"], "unsubscribe");
        assert_eq!(
            unsub["params"]["token"], "seeded-token-W2-4",
            "auth unsubscribe must carry the cached token"
        );

        auth_stack.token_lifecycle().clear_cached_token_for_test();
        assert!(
            auth_stack.cached_token().is_none(),
            "token cleared — simulates expiry-while-Open"
        );

        register_batch(
            &bus,
            WsUrl::Auth,
            vec![
                SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions {
                        snap_orders: None,
                        snap_trades: None,
                    },
                ),
                SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances { snapshot: None },
                ),
            ],
        );

        let (ch2, tok2) = nth_sent_token(&auth_sock, 2).await;
        let (ch3, tok3) = nth_sent_token(&auth_sock, 3).await;
        assert_eq!(
            tok2, "fresh-token-W2-4",
            "first re-issued subscribe carries FRESH token"
        );
        assert_eq!(
            tok3, "fresh-token-W2-4",
            "second re-issued subscribe carries FRESH token"
        );
        let mut channels = vec![ch2, ch3];
        channels.sort();
        assert_eq!(
            channels,
            vec!["balances".to_string(), "executions".to_string()],
            "both deferred subscribes re-issued on refresh"
        );

        assert_exactly_n_sent(
            &auth_sock,
            4,
            "W2-4: exactly one force_refresh → exactly two re-issues (no double-refresh third re-issue)",
        )
        .await;

        assert_eq!(
            call_count.load(AOrdering::SeqCst),
            1,
            "W2-4 single-flight guard: force_refresh must fire EXACTLY ONCE even with two concurrent expired-token 0→1 edges in the same RegisterBatch"
        );

        auth_sock.emit_text(ack_ok("executions"));
        auth_sock.emit_text(ack_ok("balances"));
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Open,
            "auth conn stays Open after re-issued acks"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    use crate::conn::managed_connection::WsResponse;
    use crate::dispatch::{EventEnvelope, EventPayload, EventType, WsFailReason, WsOp};
    use crate::error::{ConnectionError, ConnectionErrorKind};

    async fn drive_auth_to_open(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
        auth_mirror: &Arc<AtomicU8>,
    ) -> crate::transport::driveable_mock::DriveableSocketHandle {
        register_executions(bus);
        let auth_sock = socket_at(factory, 0).await;
        wait_for_state(auth_mirror, ConnectionState::Authenticating, "auth upgrade").await;
        let _ = nth_sent_token(&auth_sock, 0).await;
        auth_sock.emit_text(ack_ok("executions"));
        wait_for_state(auth_mirror, ConnectionState::Open, "auth open").await;
        auth_sock
    }

    fn post_ws_request(
        bus: &Arc<DispatchEventBus>,
        url: WsUrl,
        req_id: u64,
        method: &str,
        op: WsOp,
        params: serde_json::Value,
    ) -> tokio::sync::oneshot::Receiver<Result<WsResponse, ConnectionError>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        bus.try_post_caller_inbound(CallerInbound::WsRequestFrame {
            url,
            req_id,
            method: method.to_string(),
            op,
            params,
            completion: tx,
        })
        .expect("post WsRequestFrame");
        rx
    }

    async fn await_completion(
        rx: tokio::sync::oneshot::Receiver<Result<WsResponse, ConnectionError>>,
        what: &str,
    ) -> Result<WsResponse, ConnectionError> {
        match tokio::time::timeout(BUDGET, rx).await {
            Ok(Ok(inner)) => inner,
            Ok(Err(_)) => Err(ConnectionError::loop_closed()),
            Err(_) => panic!("timeout: completion never resolved ({what})"),
        }
    }

    pub(super) fn register_executions(bus: &Arc<DispatchEventBus>) {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};

        use crate::types::ChannelName;
        register_batch(
            bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Executions,
                None,
                SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
            )],
        );
    }

    pub(super) async fn wait_for_sent_req(
        sock: &crate::transport::driveable_mock::DriveableSocketHandle,
        target: u64,
    ) -> serde_json::Value {
        let poll = async {
            loop {
                for body in sock.sent_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body)
                        && v.get("req_id").and_then(serde_json::Value::as_u64) == Some(target)
                    {
                        return v;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: auth socket never sent req_id={target}"))
    }

    /// The returned `RestSurface` must outlive the test.
    #[allow(clippy::type_complexity)]
    fn unified_private_rig(
        token: &str,
    ) -> (
        Arc<DispatchEventBus>,
        Arc<DriveableWsSocketFactory>,
        tokio::task::JoinHandle<()>,
        Arc<AtomicU8>,
        crate::conn::subscription_registry::SubscriptionMirror,
        Arc<crate::rest::RestSurface>,
    ) {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, rest) =
            auth_stack_with_seeded_token(&bus, token, token_envelope("unused"));
        let sub_mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let (reactor, auth_mirror) =
            spawn_reactor_with_sub_mirror(&bus, &factory, auth_stack, Arc::clone(&sub_mirror));
        (bus, factory, reactor, auth_mirror, sub_mirror, rest)
    }

    async fn drive_unified_private_to_open(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
        sub_mirror: &crate::conn::subscription_registry::SubscriptionMirror,
    ) -> crate::transport::driveable_mock::DriveableSocketHandle {
        use crate::conn::subscription_registry::EntrySubState;

        register_batch(
            bus,
            WsUrl::UnifiedPrivate,
            vec![SubscriptionEntry::new(
                WsUrl::UnifiedPrivate,
                ChannelName::Executions,
                None,
                SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
            )],
        );
        let socket = socket_with_at_least(factory, WsUrl::UnifiedPrivate, 1).await;
        socket.emit_text(include_str!(
            "../../../tests/fixtures/unified_ws/private_executions_subscribe_ack.json"
        ));
        tokio::time::timeout(BUDGET, async {
            loop {
                if sub_mirror
                    .read()
                    .expect("subscription mirror")
                    .get(&(WsUrl::UnifiedPrivate, ChannelName::Executions, None))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Unified private subscribe never reached Acked");
        socket
    }

    async fn probe_reject(
        url: WsUrl,
        entries: Vec<SubscriptionEntry>,
        probe: ChannelName,
        error: &str,
    ) -> (usize, usize, Vec<Option<String>>, usize) {
        let (bus, factory, reactor, _auth_mirror, _sub_mirror, _rest) =
            unified_private_rig("lone-reject-token");
        let opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let auth_failures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let terminations = Arc::new(std::sync::Mutex::new(Vec::new()));
        {
            let opens = Arc::clone(&opens);
            let _ = bus.subscribe(
                EventType::ConnectionOpenEvent,
                Arc::new(move |env: &EventEnvelope| {
                    if let EventPayload::ConnectionOpenEvent { url: opened, .. } = &env.payload
                        && *opened == url
                    {
                        opens.fetch_add(1, Ordering::Relaxed);
                    }
                }),
                1,
            );
            let auth_failures = Arc::clone(&auth_failures);
            let _ = bus.subscribe(
                EventType::AuthHandshakeFailedEvent,
                Arc::new(move |_env: &EventEnvelope| {
                    auth_failures.fetch_add(1, Ordering::Relaxed);
                }),
                1,
            );
            let terminations = Arc::clone(&terminations);
            let _ = bus.subscribe(
                EventType::FuturesSubscriptionTerminatedEvent,
                Arc::new(move |env: &EventEnvelope| {
                    if let EventPayload::FuturesSubscriptionTerminatedEvent { last_error, .. } =
                        &env.payload
                    {
                        terminations.lock().unwrap().push(last_error.clone());
                    }
                }),
                1,
            );
        }
        register_batch(&bus, url, entries);
        let socket = socket_with_at_least(&factory, url, 1).await;
        socket.emit_text(ack_err(probe.as_ref(), error));
        tokio::time::timeout(BUDGET, async {
            while terminations.lock().unwrap().is_empty()
                && auth_failures.load(Ordering::Relaxed) == 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: the reject was never processed");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let result = (
            opens.load(Ordering::Relaxed),
            auth_failures.load(Ordering::Relaxed),
            terminations.lock().unwrap().clone(),
            socket.sent_text().len(),
        );
        reactor.abort();
        bus.stop_reactors();
        result
    }

    fn preferences_entry() -> SubscriptionEntry {
        SubscriptionEntry::new(
            WsUrl::UnifiedPrivate,
            ChannelName::Preferences,
            None,
            SubscribeParams::Preferences,
        )
    }

    #[tokio::test]
    async fn a_rejected_unified_private_probe_still_sends_the_entries_behind_it() {
        let error = "EGeneral:Invalid arguments";
        let (_opens, auth_failures, terminations, frames) = probe_reject(
            WsUrl::UnifiedPrivate,
            vec![
                SubscriptionEntry::new(
                    WsUrl::UnifiedPrivate,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions {
                        snap_orders: None,
                        snap_trades: None,
                    },
                ),
                preferences_entry(),
            ],
            ChannelName::Executions,
            error,
        )
        .await;
        assert_eq!(auth_failures, 0);
        assert_eq!(terminations, vec![Some(error.to_string())]);
        assert_eq!(
            frames, 2,
            "the entry behind the rejected probe is subscribed"
        );
    }

    #[tokio::test]
    async fn a_lone_unified_private_subscribe_rejected_for_a_non_auth_reason_keeps_the_connection()
    {
        let error = "Preferences are not available yet, try again later";
        let (opens, auth_failures, terminations, frames) = probe_reject(
            WsUrl::UnifiedPrivate,
            vec![preferences_entry()],
            ChannelName::Preferences,
            error,
        )
        .await;
        assert_eq!(
            auth_failures, 0,
            "a non-auth reject is not a handshake failure"
        );
        assert_eq!(opens, 1, "the token was accepted, so the connection opens");
        assert_eq!(terminations, vec![Some(error.to_string())]);
        assert_eq!(frames, 1, "the rejected subscribe is not resent");
    }

    #[tokio::test]
    async fn a_spot_auth_probe_rejected_for_an_unrecognised_reason_still_fails_the_handshake() {
        let (opens, auth_failures, _, _) = probe_reject(
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Executions,
                None,
                SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
            )],
            ChannelName::Executions,
            "EService:Unavailable",
        )
        .await;
        assert_eq!(auth_failures, 1);
        assert_eq!(opens, 0);
    }

    #[tokio::test]
    async fn a_lone_unified_private_subscribe_rejected_for_a_stale_token_still_fails_the_handshake()
    {
        let (opens, auth_failures, terminations, _) = probe_reject(
            WsUrl::UnifiedPrivate,
            vec![preferences_entry()],
            ChannelName::Preferences,
            "Invalid session",
        )
        .await;
        assert_eq!(auth_failures, 1);
        assert_eq!(opens, 0);
        assert!(terminations.is_empty());
    }

    #[tokio::test]
    async fn unified_private_order_requests_carry_token_and_asset_class_and_resolve_on_their_acks()
    {
        let (bus, factory, reactor, auth_mirror, sub_mirror, _rest) =
            unified_private_rig("unified-order-token");
        let socket = drive_unified_private_to_open(&bus, &factory, &sub_mirror).await;

        for (req_id, method, op, params, result) in [
            (
                101,
                "add_order",
                WsOp::AddOrder,
                serde_json::json!({ "symbol": "PF_XBTUSD", "side": "buy", "order_qty": 1 }),
                serde_json::json!({ "cl_ord_id": "c1", "order_id": "O1" }),
            ),
            (
                102,
                "batch_cancel",
                WsOp::BatchCancel,
                serde_json::json!({ "cl_ord_id": ["a", "b"] }),
                serde_json::json!({ "count": 2 }),
            ),
        ] {
            let rx = post_ws_request(
                &bus,
                WsUrl::UnifiedPrivate,
                req_id,
                method,
                op,
                params.clone(),
            );
            let sent = wait_for_sent_req(&socket, req_id).await;
            let mut expected_params = params;
            expected_params["token"] = "unified-order-token".into();
            expected_params["asset_class"] = "derivatives".into();
            assert_eq!(
                sent,
                serde_json::json!({ "method": method, "req_id": req_id, "params": expected_params }),
                "{method}"
            );
            socket.emit_text(
                serde_json::json!({
                    "method": method, "req_id": req_id, "success": true, "result": result,
                })
                .to_string(),
            );
            let resp = await_completion(rx, method).await.expect("ack resolves");
            assert_eq!(resp.result, result, "{method}");
        }
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Idle,
            "Unified private orders must not open or alter Spot Auth"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_request_ack_only_resolves_on_its_own_connection() {
        let (bus, factory, reactor, auth_mirror, sub_mirror, _rest) =
            unified_private_rig("isolation-token");
        let spot = drive_auth_to_open(&bus, &factory, &auth_mirror).await;
        let futures = drive_unified_private_to_open(&bus, &factory, &sub_mirror).await;
        let ack = |req_id: u64| {
            serde_json::json!({
                "method": "cancel_order", "req_id": req_id, "success": true,
                "result": { "cl_ord_id": "c" },
            })
            .to_string()
        };
        let cancel = serde_json::json!({ "cl_ord_id": ["c"] });
        let post = |url, req_id| {
            post_ws_request(
                &bus,
                url,
                req_id,
                "cancel_order",
                WsOp::CancelOrder,
                cancel.clone(),
            )
        };

        let mut spot_rx = post(WsUrl::Auth, 10);
        let futures_rx = post(WsUrl::UnifiedPrivate, 11);
        wait_for_sent_req(&spot, 10).await;
        wait_for_sent_req(&futures, 11).await;
        futures.emit_text(ack(10));
        futures.emit_text(ack(11));
        await_completion(futures_rx, "Futures request")
            .await
            .expect("Futures ack resolves");
        assert!(
            spot_rx.try_recv().is_err(),
            "a Spot request must not resolve from a Unified private frame"
        );

        let futures_rx = post(WsUrl::UnifiedPrivate, 12);
        let spot_rx_2 = post(WsUrl::Auth, 13);
        wait_for_sent_req(&futures, 12).await;
        wait_for_sent_req(&spot, 13).await;
        spot.emit_text(ack(12));
        spot.emit_text(ack(13));
        await_completion(spot_rx_2, "Spot request")
            .await
            .expect("Spot ack resolves");
        let mut futures_rx = futures_rx;
        assert!(
            futures_rx.try_recv().is_err(),
            "a Futures request must not resolve from a Spot Auth frame"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn deregistering_a_unified_private_subscription_sends_a_tokenised_unsubscribe() {
        let (bus, factory, reactor, _auth_mirror, sub_mirror, _rest) =
            unified_private_rig("unsubscribe-token");
        let socket = drive_unified_private_to_open(&bus, &factory, &sub_mirror).await;

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::UnifiedPrivate,
            mutation: RegistryMutationOp::Deregister {
                channel: ChannelName::Executions,
                pair: None,
            },
        })
        .expect("post deregister");
        let frames =
            wait_for_frames(&socket, "\"unsubscribe\"", 1, "Unified private unsubscribe").await;

        let frame: serde_json::Value = serde_json::from_str(&frames[0]).expect("unsubscribe JSON");
        assert_eq!(frame["params"]["channel"], "executions");
        assert_eq!(frame["params"]["token"], "unsubscribe-token");
        assert_eq!(frame["params"]["asset_class"], "derivatives");
        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn abandoning_a_unified_private_request_clears_its_pending_entry() {
        let (bus, factory, reactor, _auth_mirror, sub_mirror, _rest) =
            unified_private_rig("abandon-token");
        let socket = drive_unified_private_to_open(&bus, &factory, &sub_mirror).await;

        let rx = post_ws_request(
            &bus,
            WsUrl::UnifiedPrivate,
            20,
            "cancel_all",
            WsOp::CancelAll,
            serde_json::json!({}),
        );
        wait_for_sent_req(&socket, 20).await;
        bus.try_post_caller_inbound(CallerInbound::AbandonWsRequest { req_id: 20 })
            .expect("post abandon");
        let err = await_completion(rx, "abandoned Unified private request")
            .await
            .expect_err("abandon resolves the pending entry");
        assert_eq!(err.kind(), ConnectionErrorKind::ResponseTimeout);

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_unified_private_socket_drop_drains_its_in_flight_requests_without_resending() {
        let (bus, factory, reactor, _auth_mirror, sub_mirror, _rest) =
            unified_private_rig("drain-token");
        let failed = Arc::new(std::sync::Mutex::new(Vec::new()));
        {
            let failed = Arc::clone(&failed);
            let _ = bus.subscribe(
                EventType::WsRequestFailedEvent,
                Arc::new(move |env: &EventEnvelope| {
                    if let EventPayload::WsRequestFailedEvent { req_id, url, .. } = &env.payload {
                        failed.lock().unwrap().push((*req_id, *url));
                    }
                }),
                1,
            );
        }
        let socket = drive_unified_private_to_open(&bus, &factory, &sub_mirror).await;

        let rx = post_ws_request(
            &bus,
            WsUrl::UnifiedPrivate,
            30,
            "add_order",
            WsOp::AddOrder,
            serde_json::json!({ "symbol": "PF_XBTUSD" }),
        );
        wait_for_sent_req(&socket, 30).await;
        let sent_before = socket.sent_text().len();
        socket.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::AbnormalClose {
                context: "unified private forced drop".into(),
            },
            transient: true,
        });
        let err = await_completion(rx, "drained Unified private order")
            .await
            .expect_err("a drop fails the in-flight order");
        assert_eq!(err.kind(), ConnectionErrorKind::RequestInFlightWhenDropped);
        assert_eq!(
            socket.sent_text().len(),
            sent_before,
            "a dropped order is never resent"
        );
        tokio::time::timeout(BUDGET, async {
            while failed.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: WsRequestFailedEvent never delivered");
        assert_eq!(*failed.lock().unwrap(), vec![(30, WsUrl::UnifiedPrivate)]);

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_c_ws_order_request_resolve_and_decode() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-C", token_envelope("unused"));
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);
        let auth_sock = drive_auth_to_open(&bus, &factory, &auth_mirror).await;
        let frames_before = auth_sock.sent_text().len();

        let req_id = 7;
        let params = serde_json::json!({
            "order_type": "limit",
            "side": "buy",
            "order_qty": 0.0001,
            "symbol": "BTC/USDC",
            "limit_price": 33512.0,
            "post_only": true,
            "cl_ord_id": "11111111-1111-4111-8111-111111111111",
        });
        let rx = post_ws_request(
            &bus,
            WsUrl::Auth,
            req_id,
            "add_order",
            WsOp::AddOrder,
            params,
        );

        let sent = wait_for_sent_req(&auth_sock, req_id).await;
        assert_eq!(
            sent,
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "params": {
                    "order_type": "limit",
                    "side": "buy",
                    "order_qty": 0.0001,
                    "symbol": "BTC/USDC",
                    "limit_price": 33512.0,
                    "post_only": true,
                    "cl_ord_id": "11111111-1111-4111-8111-111111111111",
                    "token": "seeded-token-C",
                },
            }),
            "the Spot auth frame is exactly the caller params plus the reactor-injected token, with no asset_class"
        );
        assert_eq!(
            auth_sock.sent_text().len(),
            frames_before + 1,
            "exactly one new frame sent for the request"
        );

        auth_sock.emit_text(
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "result": {
                    "cl_ord_id": "11111111-1111-4111-8111-111111111111",
                    "order_id": "TEST-123",
                },
                "success": true,
                "time_in": "2026-06-03T00:00:00.000000Z",
                "time_out": "2026-06-03T00:00:00.001000Z",
            })
            .to_string(),
        );

        let resp = await_completion(rx, "add_order response")
            .await
            .expect("Ok response");
        assert!(resp.success, "decoded success");
        assert_eq!(resp.req_id, req_id, "decoded req_id");
        assert_eq!(
            resp.result["order_id"], "TEST-123",
            "result carries order_id"
        );
        let cl = crate::types::ClOrdId::new("11111111-1111-4111-8111-111111111111").unwrap();
        let decoded = crate::api::trade::__test_decode_ws_add_order(resp, Some(cl.clone()))
            .expect("add_order decodes Ok");
        assert_eq!(
            decoded.txid.as_ref().map(|t| t.as_str()),
            Some("TEST-123"),
            "order_id → txid (WS reply's order id field is order_id, not txid)"
        );
        assert_eq!(
            decoded.descr.order, None,
            "WS omits descr → None (Hard Rule 11)"
        );
        assert_eq!(decoded.descr.close, None, "WS descr.close None");
        assert_eq!(decoded.cl_ord_id, Some(cl), "cl_ord_id echoed");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_d_drain_on_disconnect_emits_ws_request_failed_before_dropped() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-D", token_envelope("unused"));

        let order: Arc<std::sync::Mutex<Vec<&'static str>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let failed_payloads = Arc::new(std::sync::Mutex::new(Vec::new()));
        {
            let order_c = Arc::clone(&order);
            let payloads_c = Arc::clone(&failed_payloads);
            let _ = bus.subscribe(
                EventType::WsRequestFailedEvent,
                Arc::new(move |env: &EventEnvelope| {
                    order_c.lock().unwrap().push("failed");
                    if let EventPayload::WsRequestFailedEvent {
                        req_id,
                        op,
                        url,
                        reason,
                    } = &env.payload
                    {
                        payloads_c
                            .lock()
                            .unwrap()
                            .push((*req_id, *op, *url, *reason));
                    }
                }),
                1,
            );
            let order_d = Arc::clone(&order);
            let _ = bus.subscribe(
                EventType::ConnectionDroppedEvent,
                Arc::new(move |_env: &EventEnvelope| {
                    order_d.lock().unwrap().push("dropped");
                }),
                1,
            );
        }

        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);
        let auth_sock = drive_auth_to_open(&bus, &factory, &auth_mirror).await;

        let req_id_a = 42;
        let req_id_b = 43;
        let rx_a = post_ws_request(
            &bus,
            WsUrl::Auth,
            req_id_a,
            "add_order",
            WsOp::AddOrder,
            serde_json::json!({
                "order_type": "limit", "side": "buy", "order_qty": 0.001,
                "symbol": "BTC/USDC", "limit_price": 30000.0,
                "cl_ord_id": "22222222-2222-4222-8222-222222222222",
            }),
        );
        let rx_b = post_ws_request(
            &bus,
            WsUrl::Auth,
            req_id_b,
            "cancel_order",
            WsOp::CancelOrder,
            serde_json::json!({
                "order_id": ["TEST-XYZ"],
            }),
        );
        let _sent_a = wait_for_sent_req(&auth_sock, req_id_a).await;
        let _sent_b = wait_for_sent_req(&auth_sock, req_id_b).await;

        auth_sock.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::AbnormalClose {
                context: "test_d forced drop".into(),
            },
            transient: true,
        });

        let err_a = await_completion(rx_a, "drained add_order")
            .await
            .expect_err("Err on drop (a)");
        assert_eq!(
            err_a.kind(),
            ConnectionErrorKind::RequestInFlightWhenDropped,
            "drained add_order future carries RequestInFlightWhenDropped"
        );
        let err_b = await_completion(rx_b, "drained cancel_order")
            .await
            .expect_err("Err on drop (b)");
        assert_eq!(
            err_b.kind(),
            ConnectionErrorKind::RequestInFlightWhenDropped,
            "drained cancel_order future carries RequestInFlightWhenDropped"
        );

        wait_for_state(
            &auth_mirror,
            ConnectionState::BackingOff,
            "auth backing off",
        )
        .await;
        let payloads = {
            let poll = async {
                loop {
                    let p = failed_payloads.lock().unwrap().clone();
                    if p.len() >= 2 {
                        return p;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: both WsRequestFailedEvents never delivered")
        };
        assert_eq!(
            payloads.len(),
            2,
            "exactly one failed event PER in-flight request (2 pending → 2 events); got {payloads:?}"
        );
        assert!(
            payloads.contains(&(
                req_id_a,
                WsOp::AddOrder,
                WsUrl::Auth,
                WsFailReason::ConnectionLost
            )),
            "add_order pending failed with its req_id + AddOrder + ConnectionLost; got {payloads:?}"
        );
        assert!(
            payloads.contains(&(
                req_id_b,
                WsOp::CancelOrder,
                WsUrl::Auth,
                WsFailReason::ConnectionLost
            )),
            "cancel_order pending failed with its req_id + CancelOrder + ConnectionLost; got {payloads:?}"
        );

        let poll_dropped = async {
            loop {
                if order.lock().unwrap().contains(&"dropped") {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll_dropped)
            .await
            .expect("timeout: ConnectionDroppedEvent never delivered");
        let seq = order.lock().unwrap().clone();
        let dropped_idx = seq
            .iter()
            .position(|s| *s == "dropped")
            .expect("dropped present");
        let failed_indices: Vec<usize> = seq
            .iter()
            .enumerate()
            .filter(|(_, s)| **s == "failed")
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            failed_indices.len(),
            2,
            "both failed markers recorded in the ordered log; seq = {seq:?}"
        );
        let max_failed_idx = *failed_indices
            .iter()
            .max()
            .expect("at least one failed marker");
        assert!(
            max_failed_idx < dropped_idx,
            "EVERY WsRequestFailedEvent (drain) MUST precede ConnectionDroppedEvent (§6.3): \
             max failed idx {max_failed_idx} >= dropped idx {dropped_idx}; seq = {seq:?}"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_e_fsm_open_gate_rejects_when_not_open() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) =
            auth_stack_with_seeded_token(&bus, "seeded-token-E", token_envelope("unused"));
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);

        register_executions(&bus);
        let auth_sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "authenticating",
        )
        .await;
        let _ = nth_sent_token(&auth_sock, 0).await;
        let sent_before = auth_sock.sent_text().len();

        let rx = post_ws_request(
            &bus,
            WsUrl::Auth,
            99,
            "add_order",
            WsOp::AddOrder,
            serde_json::json!({ "order_type": "limit", "side": "buy", "order_qty": 0.001, "symbol": "BTC/USDC" }),
        );
        let err = await_completion(rx, "gated request")
            .await
            .expect_err("Err not_open");
        assert_eq!(err.kind(), ConnectionErrorKind::NotOpen, "gate → NotOpen");

        let settle = async {
            loop {
                for body in auth_sock.sent_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                        assert_ne!(
                            v.get("req_id").and_then(serde_json::Value::as_u64),
                            Some(99),
                            "gated request must NOT have been sent"
                        );
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(std::time::Duration::from_millis(100), settle).await;
        assert_eq!(
            auth_sock.sent_text().len(),
            sent_before,
            "no extra frame sent for the FSM-Open-gated request"
        );

        reactor.abort();
        bus.stop_reactors();

        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(&factory) as _;
        let mut conns: HashMap<WsUrl, ManagedConnection> = HashMap::new();
        conns.insert(
            WsUrl::Auth,
            ManagedConnection::new(
                WsUrl::Auth,
                Arc::clone(&bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::new(crate::jitter::SplitMix64Jitter::with_seed(3)),
            ),
        );
        assert_ne!(
            conns.get(&WsUrl::Auth).unwrap().state(),
            ConnectionState::Open,
            "freshly-built auth MC must be not-Open for the gate-reject path"
        );
        let mut sockets: HashMap<WsUrl, Arc<dyn WsSocket>> = HashMap::new();
        let direct_auth = Arc::new(crate::auth::AuthStack::new(
            None,
            None,
            Arc::new(crate::auth::SystemClockNonceSource::new()),
            std::collections::HashMap::new(),
            crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
        ));
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
        let direct_registry = SubscriptionRegistry::new();
        super::super::handle_ws_request_frame(
            WsUrl::Auth,
            99,
            "add_order".to_string(),
            WsOp::AddOrder,
            serde_json::json!({ "order_type": "limit", "side": "buy", "order_qty": 0.001, "symbol": "BTC/USDC" }),
            gate_tx,
            &mut conns,
            &mut sockets,
            &direct_auth,
            &direct_registry,
        );
        let gate_err = await_completion(gate_rx, "direct gate reject")
            .await
            .expect_err("Err not_open");
        assert_eq!(
            gate_err.kind(),
            ConnectionErrorKind::NotOpen,
            "direct gate reject → NotOpen"
        );
        assert_eq!(
            conns.get(&WsUrl::Auth).unwrap().pending_request_count(),
            0,
            "FSM-Open gate must reject WITHOUT recording a pending (no dangling entry)"
        );
    }

    #[tokio::test]
    async fn a_failed_order_send_resolves_the_pending_request_not_open() {
        use crate::transport::driveable_mock::SendFailMode;
        for mode in [SendFailMode::Backpressure, SendFailMode::WriterClosed] {
            let (bus, factory) = bus_and_factory();
            let (auth_stack, _rest) = auth_stack_with_seeded_token(
                &bus,
                "seeded-token-send-fail",
                token_envelope("unused"),
            );
            let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, Arc::clone(&auth_stack));
            let auth_sock = drive_auth_to_open(&bus, &factory, &auth_mirror).await;
            auth_sock.set_send_fail_mode(mode);

            let rx = post_ws_request(
                &bus,
                WsUrl::Auth,
                81,
                "add_order",
                WsOp::AddOrder,
                serde_json::json!({ "order_type": "limit", "side": "buy", "order_qty": 0.001, "symbol": "BTC/USDC" }),
            );
            let err = await_completion(rx, "failed order send")
                .await
                .expect_err("a failed send resolves the request Err");
            assert_eq!(err.kind(), ConnectionErrorKind::NotOpen, "{mode:?}");
            assert!(auth_sock.send_attempts() >= 1, "{mode:?}");

            reactor.abort();
            bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn presend_no_token_at_open_rejects_retryable_and_refreshes_no_defer() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-W2-2-T2",
            token_envelope("fresh-token-W2-2-T2"),
        );
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, Arc::clone(&auth_stack));

        let auth_sock = drive_auth_to_open(&bus, &factory, &auth_mirror).await;
        let frames_after_open = auth_sock.sent_text().len();

        auth_stack.token_lifecycle().clear_cached_token_for_test();
        assert!(
            auth_stack.cached_token().is_none(),
            "token cleared — simulates expiry-while-Open before the order compose"
        );

        let req_id = 71;
        let rx = post_ws_request(
            &bus,
            WsUrl::Auth,
            req_id,
            "add_order",
            WsOp::AddOrder,
            serde_json::json!({
                "order_type": "limit", "side": "buy", "order_qty": 0.0001,
                "symbol": "BTC/USDC", "limit_price": 33512.0, "post_only": true,
                "cl_ord_id": "b2220000-0000-4000-8000-000000000002",
            }),
        );
        let err = await_completion(rx, "A392 pre-send reject")
            .await
            .expect_err("Err session_token_unavailable");
        assert_eq!(
            err.kind(),
            ConnectionErrorKind::SessionTokenUnavailable,
            "A392: pre-send no-valid-token order rejected as a missing session token (definitely not sent)"
        );

        let refreshed = async {
            loop {
                if auth_stack.cached_token().map(|t| t.value().to_string())
                    == Some("fresh-token-W2-2-T2".to_string())
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, refreshed).await.expect(
            "timeout: A392 force_refresh never landed the fresh token (proves force_refresh fired)",
        );

        let settle = async {
            loop {
                for body in auth_sock.sent_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                        assert_ne!(
                            v.get("method").and_then(serde_json::Value::as_str),
                            Some("add_order"),
                            "A392: order must NOT be composed/deferred/re-issued (no order analogue of deferred subscribes)"
                        );
                        assert_ne!(
                            v.get("req_id").and_then(serde_json::Value::as_u64),
                            Some(req_id),
                            "A392: the rejected order's req_id frame must never be sent"
                        );
                    }
                }
                assert_eq!(
                    ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
                    ConnectionState::Open,
                    "A392: pre-send reject does NOT tear down the live Open session"
                );
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(200), settle).await;
        assert_eq!(
            auth_sock.sent_text().len(),
            frames_after_open,
            "A392: no extra frame sent for the rejected (never-deferred) order"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn presend_no_token_order_burst_fetches_one_token() {
        let (bus, factory) = bus_and_factory();
        let transport = HeldFirstTokenFetch::new();
        let (auth_stack, _rest) = auth_stack_with_transport(
            &bus,
            "seeded-token-burst",
            Arc::clone(&transport) as Arc<dyn HttpTransport>,
        );
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, Arc::clone(&auth_stack));
        let auth_sock = drive_auth_to_open(&bus, &factory, &auth_mirror).await;
        let frames_after_open = auth_sock.sent_text().len();
        auth_stack.token_lifecycle().clear_cached_token_for_test();

        let pending: Vec<_> = (0..5u64)
            .map(|i| {
                post_ws_request(
                    &bus,
                    WsUrl::Auth,
                    80 + i,
                    "add_order",
                    WsOp::AddOrder,
                    serde_json::json!({
                        "order_type": "limit", "side": "buy", "order_qty": 0.0001,
                        "symbol": "BTC/USDC", "limit_price": 33512.0, "post_only": true,
                        "cl_ord_id": format!("b2230000-0000-4000-8000-00000000000{i}"),
                    }),
                )
            })
            .collect();
        for rx in pending {
            let err = await_completion(rx, "burst pre-send reject")
                .await
                .expect_err("Err session_token_unavailable");
            assert_eq!(err.kind(), ConnectionErrorKind::SessionTokenUnavailable);
        }
        transport
            .wait_for_calls(1, "burst never started a token fetch")
            .await;
        transport.release.add_permits(1);
        let landed = async {
            while auth_stack.cached_token().is_none() {
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, landed)
            .await
            .expect("timeout: burst token never landed");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            transport.calls(),
            1,
            "five stale-token orders share one token fetch"
        );
        assert_eq!(auth_sock.sent_text().len(), frames_after_open);
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Open,
        );

        reactor.abort();
        bus.stop_reactors();
    }
}

#[cfg(test)]
mod order_autoconnect_tests {
    use super::auth_handshake_drive_tests::*;
    use super::*;
    use crate::Side;
    use crate::api::trade::{OrderRequest, OrderType, TradeError, TradeNamespace};
    use crate::api::ws_surface::WsSurface;
    use crate::clock::{Clock, SystemClock};
    use crate::conn::rate_budget::ConnectionRateBudget;
    use crate::conn::subscription_registry::SubscriptionRegistry;
    use crate::conn::{ConnectionSupervisor, ManagedConnection};
    use crate::dispatch::Transport;
    use crate::rest::RestSurface;
    use crate::transport::HttpTransport;
    use crate::transport::driveable_mock::{DriveableSocketHandle, DriveableWsSocketFactory};
    use crate::types::{ConnectionState, Symbol, WsUrl};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::time::Duration;

    struct WiredClient {
        ws: Arc<WsSurface>,
        trade: TradeNamespace,
        factory: Arc<DriveableWsSocketFactory>,
        bus: Arc<DispatchEventBus>,
        auth_mirror: Arc<AtomicU8>,
        auth_stack: Arc<crate::auth::AuthStack>,
        supervisor: Arc<ConnectionSupervisor>,
        reactor: tokio::task::JoinHandle<()>,
        trading_rl: Arc<crate::rate_limit::SpotTradingRateLimitTracker>,
        index: Option<Arc<crate::rate_limit::ClOrdIdPairIndex>>,
    }

    fn wire_client_core(
        seeded_token: &str,
        refresh_envelope: serde_json::Value,
        order_transport: Arc<dyn HttpTransport>,
        order_deadline_ms: Option<u32>,
        set_rest_bus: bool,
        with_index: bool,
    ) -> WiredClient {
        wire_client_core_limited(
            seeded_token,
            refresh_envelope,
            order_transport,
            order_deadline_ms,
            set_rest_bus,
            with_index,
            None,
        )
    }

    fn wire_client_core_limited(
        seeded_token: &str,
        refresh_envelope: serde_json::Value,
        order_transport: Arc<dyn HttpTransport>,
        order_deadline_ms: Option<u32>,
        set_rest_bus: bool,
        with_index: bool,
        futures_max_in_flight: Option<u32>,
    ) -> WiredClient {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest_keepalive) =
            auth_stack_with_seeded_token(&bus, seeded_token, refresh_envelope);
        std::mem::forget(_rest_keepalive);

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let supervisor = Arc::new(ConnectionSupervisor::new(Arc::clone(&bus)));
        let state_mirrors = supervisor.state_mirror_clones();
        let auth_mirror = Arc::clone(&state_mirrors[&WsUrl::Auth]);

        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(&factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let mut conns = HashMap::new();
        conns.insert(
            WsUrl::Public,
            ManagedConnection::new(
                WsUrl::Public,
                Arc::clone(&bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::new(crate::jitter::SplitMix64Jitter::with_seed(1)),
            ),
        );
        conns.insert(
            WsUrl::Auth,
            ManagedConnection::new(
                WsUrl::Auth,
                Arc::clone(&bus),
                Arc::clone(&dyn_factory),
                Arc::clone(&rate_budget),
                Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                Arc::new(crate::jitter::SplitMix64Jitter::with_seed(2)),
            ),
        );
        for (url, seed) in [(WsUrl::UnifiedPublic, 4), (WsUrl::UnifiedPrivate, 3)] {
            conns.insert(
                url,
                ManagedConnection::new(
                    url,
                    Arc::clone(&bus),
                    Arc::clone(&dyn_factory),
                    Arc::clone(&rate_budget),
                    Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                    Arc::new(crate::jitter::SplitMix64Jitter::with_seed(seed)),
                ),
            );
        }
        let caller_rx = bus
            .take_caller_to_io_rx()
            .expect("caller_to_io_rx available");
        let presence_mirror: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handler_registry = crate::dispatch::HandlerRegistry::new(Arc::clone(&presence_mirror));
        let init = IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::new(),
            handler_registry,
            auth_stack: Arc::clone(&auth_stack),
            state_mirrors,
            bus_back_ref: Arc::downgrade(&bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: supervisor.handle_id_allocator(),
            order_gate: supervisor.order_gate(),
        };
        let reactor = tokio::spawn(run(init));

        let shared_trading_rl = Arc::new(crate::rate_limit::SpotTradingRateLimitTracker::new(
            crate::rate_limit::Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let alloc = Arc::new(AtomicU64::new(1));
        let (ws_surface, index) = if with_index {
            let shared_index = Arc::new(crate::rate_limit::ClOrdIdPairIndex::new(1024));
            let ws = Arc::new(
                crate::api::ws_surface::WsSurface::new_with_supervisor_and_index(
                    Arc::clone(&bus),
                    presence_mirror,
                    crate::conn::subscription_registry::SubscriptionMirror::default(),
                    alloc,
                    Arc::clone(&supervisor),
                    Arc::clone(&shared_trading_rl),
                    Arc::clone(&auth_stack),
                    Arc::clone(&clock),
                    Arc::clone(&shared_index),
                ),
            );
            (ws, Some(shared_index))
        } else {
            let ws = Arc::new(WsSurface::new_with_supervisor(
                Arc::clone(&bus),
                presence_mirror,
                alloc,
                Arc::clone(&supervisor),
                Arc::clone(&shared_trading_rl),
                Arc::clone(&auth_stack),
                Arc::clone(&clock),
            ));
            (ws, None)
        };
        let order_rest = Arc::new(RestSurface::new(
            order_transport,
            Arc::clone(&auth_stack),
            Arc::new(crate::rate_limit::SpotApiRateLimitTracker::new(
                crate::rate_limit::Tier::Starter,
                Arc::clone(&bus),
                Arc::clone(&clock),
                Arc::new(crate::build::knobs::Knobs::defaults()),
            )),
            Arc::clone(&shared_trading_rl),
            Arc::clone(&clock),
            std::time::Duration::from_secs(30),
        ));
        if set_rest_bus {
            order_rest.set_bus(Arc::clone(&bus));
        }
        let trade = TradeNamespace::new_with_ws(
            order_rest,
            Arc::clone(&ws_surface),
            std::sync::Arc::new({
                let mut k = crate::build::knobs::Knobs::defaults();
                k.ws_order_response_deadline_ms = order_deadline_ms;
                k.futures_max_in_flight = futures_max_in_flight;
                k
            }),
            std::sync::Arc::new(
                crate::dispatch::dispatch_table::DispatchTable::with_default_table(),
            ),
        );
        WiredClient {
            ws: ws_surface,
            trade,
            factory,
            bus,
            auth_mirror,
            auth_stack,
            supervisor,
            reactor,
            trading_rl: shared_trading_rl,
            index,
        }
    }

    fn unused_refresh_envelope() -> serde_json::Value {
        token_envelope("unused")
    }

    /// Order REST transport panics: enforces the no-REST-fallback contract.
    pub(super) fn wire_client(
        seeded_token: &str,
    ) -> (
        TradeNamespace,
        Arc<DriveableWsSocketFactory>,
        Arc<DispatchEventBus>,
        Arc<AtomicU8>,
        tokio::task::JoinHandle<()>,
    ) {
        let w = wire_client_core(
            seeded_token,
            unused_refresh_envelope(),
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            None,
            false,
            false,
        );
        std::mem::forget(w.supervisor);
        (w.trade, w.factory, w.bus, w.auth_mirror, w.reactor)
    }

    /// An envelope with empty `token` makes the refresh decode-fail, leaving the cache `None`.
    #[allow(clippy::type_complexity)]
    fn wire_client_with_refresh(
        seeded_token: &str,
        refresh_envelope: serde_json::Value,
    ) -> (
        TradeNamespace,
        Arc<DriveableWsSocketFactory>,
        Arc<DispatchEventBus>,
        Arc<AtomicU8>,
        Arc<crate::auth::AuthStack>,
        tokio::task::JoinHandle<()>,
    ) {
        let w = wire_client_core(
            seeded_token,
            refresh_envelope,
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            None,
            true,
            false,
        );
        std::mem::forget(w.supervisor);
        (
            w.trade,
            w.factory,
            w.bus,
            w.auth_mirror,
            w.auth_stack,
            w.reactor,
        )
    }

    pub(super) fn buy_limit(pair: &str, vol: &str, price: &str) -> OrderRequest {
        let mut req =
            OrderRequest::new(Symbol::new(pair).unwrap(), vol.parse().unwrap(), Side::Buy)
                .order_type(OrderType::Limit);
        req.price = Some(crate::api::trade::Price::Absolute(price.parse().unwrap()));
        req
    }

    pub(super) fn count_events(
        bus: &Arc<DispatchEventBus>,
        event_type: crate::dispatch::EventType,
    ) -> Arc<std::sync::atomic::AtomicU64> {
        let count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter = Arc::clone(&count);
        let _ = bus.subscribe(
            event_type,
            Arc::new(move |_: &crate::dispatch::EventEnvelope| {
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }),
            1,
        );
        count
    }

    pub(super) fn capture_submitted_statuses(
        bus: &Arc<DispatchEventBus>,
    ) -> Arc<std::sync::Mutex<Vec<crate::dispatch::OrderSubmitStatus>>> {
        let statuses = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&statuses);
        let _ = bus.subscribe(
            crate::dispatch::EventType::OrderSubmittedEvent,
            Arc::new(move |env: &crate::dispatch::EventEnvelope| {
                if let crate::dispatch::EventPayload::OrderSubmittedEvent { status, .. } =
                    &env.payload
                {
                    sink.lock().unwrap().push(status.clone());
                }
            }),
            1,
        );
        statuses
    }

    pub(super) fn session_expired_status() -> crate::dispatch::OrderSubmitStatus {
        crate::dispatch::OrderSubmitStatus::WireError {
            code: "SESSION_EXPIRED".into(),
        }
    }

    pub(super) async fn drive_handshake_to_open(
        factory: &Arc<DriveableWsSocketFactory>,
        auth_mirror: &Arc<AtomicU8>,
    ) -> DriveableSocketHandle {
        let sock = socket_at(factory, 0).await;
        let _ = wait_for_first_subscribe(&sock).await;
        sock.emit_text(ack_ok("executions"));
        wait_for_state(
            auth_mirror,
            ConnectionState::Open,
            "auth open (auto-connect)",
        )
        .await;
        sock
    }

    async fn wait_for_first_subscribe(sock: &DriveableSocketHandle) {
        let poll = async {
            loop {
                if !sock.sent_text().is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: auth socket never sent the first signed subscribe");
    }

    pub(super) async fn wait_for_add_order_req(sock: &DriveableSocketHandle) -> u64 {
        wait_for_order_req(sock, "add_order").await
    }

    pub(super) fn emit_add_order_ok(sock: &DriveableSocketHandle, req_id: u64, cl_ord_id: &str) {
        sock.emit_text(
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "result": { "cl_ord_id": cl_ord_id, "order_id": "OAUTO-CONNECT-1" },
                "success": true,
            })
            .to_string(),
        );
    }

    #[tokio::test]
    async fn test_f_ws_default_order_autoconnects_then_sends() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-F");
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Idle,
            "fresh client: auth WS Idle before the first order"
        );

        register_executions(&bus);

        let cl = "f0000000-0000-4000-8000-000000000001";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;
        let req_id = wait_for_add_order_req(&sock).await;
        emit_add_order_ok(&sock, req_id, cl);

        let resp = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: order future never resolved")
            .expect("task panicked")
            .expect("order resolves Ok");
        assert_eq!(
            resp.txid.as_ref().map(|t| t.as_str()),
            Some("OAUTO-CONNECT-1")
        );
        assert_eq!(resp.cl_ord_id.as_ref().map(|c| c.as_str()), Some(cl));

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_g_connect_fail_is_trade_error_no_rest_fallback() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-G");
        register_executions(&bus);

        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id =
            Some(crate::types::ClOrdId::new("c0000000-0000-4000-8000-000000000001").unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = socket_at(&factory, 0).await;
        wait_for_first_subscribe(&sock).await;
        sock.emit_text(ack_err("executions", "EAPI:Invalid key"));

        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: order future never resolved")
            .expect("task panicked");
        let err = res.expect_err("WS connect failure must surface as Err (no REST fallback)");
        assert!(
            matches!(&err, TradeError::AuthenticationFailed { kraken_message, .. } if kraken_message == "Invalid API key or signature"),
            "a handshake the venue refused is an AuthenticationFailed, not a retryable transport error; got {err:?}"
        );
        assert_ne!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Open,
            "auth WS must NOT be Open after a handshake failure"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_h_multi_order_race_connects_once_both_resolve() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-H");
        let trade = Arc::new(trade);
        register_executions(&bus);

        let cl_a = "a0000000-0000-4000-8000-00000000000a";
        let cl_b = "b0000000-0000-4000-8000-00000000000b";
        let t1 = Arc::clone(&trade);
        let t2 = Arc::clone(&trade);
        let fut_a = tokio::spawn(async move {
            let mut r = buy_limit("BTC/USDC", "0.0001", "33512.0");
            r.cl_ord_id = Some(crate::types::ClOrdId::new(cl_a).unwrap());
            t1.order(r).await
        });
        let fut_b = tokio::spawn(async move {
            let mut r = buy_limit("BTC/USDC", "0.0002", "33500.0");
            r.cl_ord_id = Some(crate::types::ClOrdId::new(cl_b).unwrap());
            t2.order(r).await
        });

        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;

        let poll_two = async {
            loop {
                let ids: Vec<u64> = sock
                    .sent_text()
                    .iter()
                    .filter_map(|b| serde_json::from_str::<serde_json::Value>(b).ok())
                    .filter(|v| {
                        v.get("method").and_then(serde_json::Value::as_str) == Some("add_order")
                    })
                    .filter_map(|v| v.get("req_id").and_then(serde_json::Value::as_u64))
                    .collect();
                if ids.len() >= 2 {
                    return ids;
                }
                tokio::task::yield_now().await;
            }
        };
        let ids = tokio::time::timeout(BUDGET, poll_two)
            .await
            .expect("timeout: both order frames never sent on the single socket");
        for id in ids {
            emit_add_order_ok(&sock, id, cl_a);
        }

        let ra = tokio::time::timeout(BUDGET, fut_a)
            .await
            .expect("timeout: order A")
            .expect("task A panicked")
            .expect("order A Ok");
        let rb = tokio::time::timeout(BUDGET, fut_b)
            .await
            .expect("timeout: order B")
            .expect("task B panicked")
            .expect("order B Ok");
        assert_eq!(
            ra.txid.as_ref().map(|t| t.as_str()),
            Some("OAUTO-CONNECT-1")
        );
        assert_eq!(
            rb.txid.as_ref().map(|t| t.as_str()),
            Some("OAUTO-CONNECT-1")
        );

        assert_eq!(
            factory.created_count(),
            1,
            "multi-order race must connect EXACTLY ONCE (no double-connect) — \
             asserted after both orders resolved"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn open_order_ack_token_stale_invalidates_refreshes_no_teardown() {
        use crate::error::ApiError;
        let (trade, factory, bus, auth_mirror, auth_stack, reactor) =
            wire_client_with_refresh("seeded-token-W2-2-T1", token_envelope(""));
        let ambiguous = count_events(
            &bus,
            crate::dispatch::EventType::OrderPlacementAmbiguousEvent,
        );
        let failed = count_events(&bus, crate::dispatch::EventType::WsRequestFailedEvent);
        let submitted = capture_submitted_statuses(&bus);
        register_executions(&bus);

        let cl = "a1110000-0000-4000-8000-000000000001";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;
        let req_id = wait_for_add_order_req(&sock).await;
        assert!(
            auth_stack.cached_token().is_some(),
            "seeded token valid through bring-up + order send"
        );
        let order_frames_before = sock
            .sent_text()
            .iter()
            .filter(|b| {
                serde_json::from_str::<serde_json::Value>(b)
                    .ok()
                    .and_then(|v| v.get("method").and_then(|m| m.as_str()).map(String::from))
                    .as_deref()
                    == Some("add_order")
            })
            .count();
        assert_eq!(
            order_frames_before, 1,
            "exactly one Open order sent before the stale ack"
        );

        sock.emit_text(
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "success": false,
                "error": "EAPI:Invalid token",
            })
            .to_string(),
        );

        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: order future never resolved")
            .expect("task panicked");
        let err = res.expect_err("Open order-ack TokenStale must resolve the order Err");
        assert!(
            matches!(err, TradeError::SessionExpired { .. }),
            "an Open order-ack TokenStale is a definite rejection: SessionExpired; got {err:?}"
        );
        assert!(err.retryable(), "a rejected session token is retryable");
        assert_eq!(
            (err.code(), err.category()),
            ("SESSION_EXPIRED", crate::error::ErrorCategory::Auth)
        );

        assert!(
            auth_stack.cached_token().is_none(),
            "A393: the stale token was invalidated (None); refresh kept it None for the assertion"
        );

        let settle = async {
            loop {
                assert_eq!(
                    ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
                    ConnectionState::Open,
                    "A389: session stays Open (session + subs preserved, NO teardown)"
                );
                let order_count = sock
                    .sent_text()
                    .iter()
                    .filter(|b| {
                        serde_json::from_str::<serde_json::Value>(b)
                            .ok()
                            .and_then(|v| {
                                v.get("method").and_then(|m| m.as_str()).map(String::from)
                            })
                            .as_deref()
                            == Some("add_order")
                    })
                    .count();
                assert_eq!(
                    order_count, 1,
                    "A390/§11.7: Open order-ack TokenStale must NOT auto-resend the order"
                );
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(200), settle).await;
        assert_eq!(
            (
                ambiguous.load(Ordering::Relaxed),
                failed.load(Ordering::Relaxed)
            ),
            (0, 0),
            "a definite rejection fires neither OrderPlacementAmbiguousEvent nor WsRequestFailedEvent"
        );
        assert_eq!(
            *submitted.lock().unwrap(),
            vec![session_expired_status()],
            "the venue's answer is reported as OrderSubmittedEvent WireError SESSION_EXPIRED"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn open_order_ack_token_stale_ack_time_force_refresh_relands_token() {
        let (trade, factory, bus, auth_mirror, auth_stack, reactor) = wire_client_with_refresh(
            "seeded-token-W2-2-T2",
            token_envelope("fresh-token-W2-2-T2"),
        );
        register_executions(&bus);

        let cl = "a1170000-0000-4000-8000-000000000017";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;
        let req_id = wait_for_add_order_req(&sock).await;

        assert!(
            auth_stack.cached_token().is_some(),
            "seeded token valid through bring-up + order send"
        );

        sock.emit_text(
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "success": false,
                "error": "EAPI:Invalid token",
            })
            .to_string(),
        );

        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: order future never resolved")
            .expect("task panicked");
        let err = res.expect_err("Open order-ack TokenStale must resolve the order Err");
        assert!(
            matches!(err, TradeError::SessionExpired { .. }),
            "an Open order-ack TokenStale resolves SessionExpired; got {err:?}"
        );

        let fresh = {
            let poll = async {
                loop {
                    if let Some(t) = auth_stack.cached_token() {
                        return t;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: fresh token never landed in cache (force_refresh not fired?)")
        };
        assert_eq!(
            fresh.value(),
            "fresh-token-W2-2-T2",
            "the FRESH token value must match the canned refresh envelope (proves force_refresh ran)"
        );

        let settle = async {
            loop {
                assert_eq!(
                    ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
                    ConnectionState::Open,
                    "A389: session stays Open after Open ack-time TokenStale + force_refresh"
                );
                let order_count = sock
                    .sent_text()
                    .iter()
                    .filter(|b| {
                        serde_json::from_str::<serde_json::Value>(b)
                            .ok()
                            .and_then(|v| {
                                v.get("method").and_then(|m| m.as_str()).map(String::from)
                            })
                            .as_deref()
                            == Some("add_order")
                    })
                    .count();
                assert_eq!(
                    order_count, 1,
                    "A390/§11.7: Open order-ack TokenStale must NOT auto-resend the order (exactly 1 frame)"
                );
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(200), settle).await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn open_order_ack_success_does_not_trigger_force_refresh() {
        let (trade, factory, bus, auth_mirror, auth_stack, reactor) = wire_client_with_refresh(
            "seeded-token-success-guard-17",
            token_envelope("fresh-token-no-force-17"),
        );
        register_executions(&bus);

        let cl = "b1170000-0000-4000-8000-000000000017";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;
        let req_id = wait_for_add_order_req(&sock).await;

        assert_eq!(
            auth_stack.cached_token().as_ref().map(|t| t.value()),
            Some("seeded-token-success-guard-17"),
            "seeded token present before the success ack"
        );

        sock.emit_text(
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "success": true,
                "result": { "cl_ord_id": cl, "order_id": "O17-SUCCESS-GUARD" },
            })
            .to_string(),
        );

        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: order future never resolved")
            .expect("task panicked");
        let resp = res.expect("Open success ack must resolve the order Ok");
        assert_eq!(
            resp.txid.as_ref().map(|t| t.as_str()),
            Some("O17-SUCCESS-GUARD"),
            "order resolves with the exchange-assigned txid"
        );

        let settle = async {
            loop {
                let cached = auth_stack.cached_token().map(|t| t.value().to_string());
                assert_eq!(
                    cached.as_deref(),
                    Some("seeded-token-success-guard-17"),
                    "#17 guard: SUCCESS ack must NOT trigger force_refresh — \
                     token must remain the seeded value, not the fresh-token-no-force-17 \
                     the canned mock would inject"
                );
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(200), settle).await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_i_via_rest_routes_rest_default_is_ws() {
        let (trade, factory, bus, _auth_mirror, reactor) = wire_client("seeded-token-I-ws");
        let default_fut = tokio::spawn(async move {
            let mut r = buy_limit("BTC/USDC", "0.0001", "33512.0");
            r.cl_ord_id =
                Some(crate::types::ClOrdId::new("10000000-0000-4000-8000-000000000001").unwrap());
            trade.order(r).await
        });
        let connected = async {
            loop {
                if factory.created_count() >= 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, connected).await.expect(
            "timeout: default order never triggered the WS auto-connect (default must be WS)",
        );
        assert_eq!(
            factory.created_count(),
            1,
            "WS-default order opened the auth socket"
        );
        default_fut.abort();
        reactor.abort();
        bus.stop_reactors();

        let (rest_trade, rest_factory, rest_bus, rest_mirror, rest_reactor) =
            wire_client_capturing_rest("seeded-token-I-rest");
        let mut r = buy_limit("BTC/USDC", "0.0001", "33512.0");
        r.cl_ord_id =
            Some(crate::types::ClOrdId::new("20000000-0000-4000-8000-000000000002").unwrap());
        let resp = tokio::time::timeout(BUDGET, rest_trade.order(r).via(Transport::Rest))
            .await
            .expect("timeout: .via(Rest) order never resolved")
            .expect("REST order resolves Ok");
        assert_eq!(resp.txid.as_ref().map(|t| t.as_str()), Some("OREST-1"));
        assert_eq!(
            rest_factory.created_count(),
            0,
            ".via(Rest) must route REST — NO auth-WS auto-connect"
        );
        assert_eq!(
            ConnectionState::from_u8(rest_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Idle,
            ".via(Rest) leaves the auth WS Idle (untouched)"
        );

        rest_reactor.abort();
        rest_bus.stop_reactors();
    }

    fn wire_client_capturing_rest(
        seeded_token: &str,
    ) -> (
        TradeNamespace,
        Arc<DriveableWsSocketFactory>,
        Arc<DispatchEventBus>,
        Arc<AtomicU8>,
        tokio::task::JoinHandle<()>,
    ) {
        let canned_rest = ClosureHttpMock::new(
            Box::new(|| panic!("REST order never GETs")),
            Box::new(|| {
                Ok(serde_json::json!({
                    "error": [],
                    "result": { "descr": { "order": "buy" }, "txid": ["OREST-1"] }
                }))
            }),
        );
        let w = wire_client_core(
            seeded_token,
            unused_refresh_envelope(),
            Arc::new(canned_rest) as Arc<dyn HttpTransport>,
            None,
            false,
            false,
        );
        std::mem::forget(w.supervisor);
        (w.trade, w.factory, w.bus, w.auth_mirror, w.reactor)
    }

    async fn wait_for_sent_add_order(sock: &DriveableSocketHandle) -> serde_json::Value {
        let poll = async {
            loop {
                for body in sock.sent_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body)
                        && v.get("method").and_then(serde_json::Value::as_str) == Some("add_order")
                    {
                        return v;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: bare-order auth socket never sent the probe add_order frame")
    }
    async fn assert_cold_unified_private_order_self_auths(w: &WiredClient) {
        let ws = Arc::clone(&w.ws);
        let req_id = ws.next_req_id();
        let order = tokio::spawn(async move {
            ws.ensure_order_sendable(WsUrl::UnifiedPrivate).await?;
            ws.send_request_costed(
                WsUrl::UnifiedPrivate,
                "add_order".to_string(),
                req_id,
                serde_json::json!({
                    "symbol": "PF_XBTUSD", "side": "buy", "order_type": "limit",
                    "order_qty": 1, "limit_price": 100.0,
                    "cl_ord_id": "11111111-1111-4111-8111-111111111111",
                }),
                crate::rest::RateLimitCost::None,
            )
            .expect("untracked cost is never rate limited")
            .recv()
            .await
        });

        let sock = socket_with_at_least(&w.factory, WsUrl::UnifiedPrivate, 0).await;
        let sent = wait_for_sent_add_order(&sock).await;
        assert_eq!(sent["req_id"], req_id);
        assert_eq!(sent["params"]["token"], "seeded-token-UPC");
        assert_eq!(sent["params"]["asset_class"], "derivatives");
        assert_eq!(
            w.supervisor.current_state(WsUrl::UnifiedPrivate),
            ConnectionState::Authenticating,
            "the order is the auth probe, so the socket stays Authenticating until its ack"
        );

        sock.emit_text(
            serde_json::json!({
                "method": "add_order", "req_id": req_id, "success": true,
                "result": { "cl_ord_id": "11111111-1111-4111-8111-111111111111", "order_id": "OF-1" },
            })
            .to_string(),
        );
        let resp = tokio::time::timeout(BUDGET, order)
            .await
            .expect("timeout: the bare order never resolved")
            .expect("task panicked")
            .expect("the ack resolves the order");
        assert!(resp.success);
        tokio::time::timeout(BUDGET, async {
            while w.supervisor.current_state(WsUrl::UnifiedPrivate) != ConnectionState::Open {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: the order ack never opened the Unified private socket");
    }

    fn unified_wired_client(refresh_envelope: serde_json::Value) -> WiredClient {
        wire_client_core(
            "seeded-token-UPC",
            refresh_envelope,
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            None,
            true,
            false,
        )
    }

    type FuturesCall =
        fn(
            &crate::FuturesTradeNamespace,
        )
            -> futures_util::future::BoxFuture<'static, Result<serde_json::Value, TradeError>>;

    fn json_of<T: serde::Serialize>(value: T) -> serde_json::Value {
        serde_json::to_value(value).expect("responses serialise")
    }

    enum Reply {
        Result(serde_json::Value),
        Error(&'static str),
        Frame(serde_json::Value),
    }

    fn frame(raw: &str) -> serde_json::Value {
        serde_json::from_str(raw).expect("fixture is valid JSON")
    }

    enum Outcome {
        Response(serde_json::Value),
        Code(&'static str),
    }

    struct FuturesCase {
        label: &'static str,
        call: FuturesCall,
        method: &'static str,
        params: serde_json::Value,
        reply: Reply,
        outcome: Outcome,
    }

    const FUTURES_ID: &str = "11111111-1111-4111-8111-111111111111";
    const FUTURES_ID_2: &str = "22222222-2222-4222-8222-222222222222";
    const FUTURES_ORDER_ID: &str = "a3270acf-5190-41bd-83a0-4e433e852277";

    fn futures_id(id: &str) -> crate::types::ClOrdId {
        crate::types::ClOrdId::new(id).expect("valid test id")
    }

    fn futures_limit_buy(id: &str) -> crate::FuturesOrder {
        crate::FuturesOrder::new(crate::Side::Buy, rust_decimal::Decimal::from(1))
            .order_type(crate::OrderType::Limit)
            .price(rust_decimal::Decimal::from(100))
            .cl_ord_id(futures_id(id))
    }

    fn futures_cases() -> Vec<FuturesCase> {
        use serde_json::json;
        let limit_buy = |id: &str| {
            json!({
                "order_type": "limit", "side": "buy", "order_qty": 1, "symbol": "PF_XBTUSD",
                "cl_ord_id": id, "limit_price": 100, "limit_price_type": "static",
            })
        };
        vec![
            FuturesCase {
                label: "order",
                call: |f| {
                    let pending = f.order(crate::FuturesOrderRequest::new(
                        "PF_XBTUSD",
                        futures_limit_buy(FUTURES_ID),
                    ));
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "add_order",
                params: limit_buy(FUTURES_ID),
                reply: Reply::Frame(frame(include_str!(
                    "../../../tests/fixtures/unified_ws/order_add_ack.json"
                ))),
                outcome: Outcome::Response(json!({
                    "txid": "OF-1", "cl_ord_id": FUTURES_ID, "descr": {},
                })),
            },
            FuturesCase {
                label: "dry-run order",
                call: |f| {
                    let pending = f.order(
                        crate::FuturesOrderRequest::new("PF_XBTUSD", futures_limit_buy(FUTURES_ID))
                            .validate_only(true),
                    );
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "add_order",
                params: {
                    let mut params = limit_buy(FUTURES_ID);
                    params["validate"] = json!(true);
                    params
                },
                reply: Reply::Result(json!({ "cl_ord_id": FUTURES_ID, "order_id": "" })),
                outcome: Outcome::Response(json!({ "cl_ord_id": FUTURES_ID, "descr": {} })),
            },
            FuturesCase {
                label: "order the venue refuses",
                call: |f| {
                    let pending = f.order(crate::FuturesOrderRequest::new(
                        "PF_XBTUSD",
                        futures_limit_buy(FUTURES_ID).post_only(true),
                    ));
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "add_order",
                params: {
                    let mut params = limit_buy(FUTURES_ID);
                    params["post_only"] = json!(true);
                    params
                },
                reply: Reply::Error("Other:POST_WOULD_EXECUTE"),
                outcome: Outcome::Code("INVALID_ORDER"),
            },
            FuturesCase {
                label: "throttled order",
                call: |f| {
                    let pending = f.order(crate::FuturesOrderRequest::new(
                        "PF_XBTUSD",
                        futures_limit_buy(FUTURES_ID),
                    ));
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "add_order",
                params: limit_buy(FUTURES_ID),
                reply: Reply::Error("Rate limit exceeded"),
                outcome: Outcome::Code("RATE_LIMIT_EXCEEDED"),
            },
            FuturesCase {
                label: "amend",
                call: |f| {
                    let pending = f.order_amend(
                        crate::FuturesOrderAmendRequest::by_cl_ord_id(futures_id(FUTURES_ID))
                            .order_volume(rust_decimal::Decimal::from(2)),
                    );
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "amend_order",
                params: json!({ "cl_ord_id": FUTURES_ID, "order_qty": 2 }),
                reply: Reply::Frame(frame(include_str!(
                    "../../../tests/fixtures/unified_ws/order_amend_ack.json"
                ))),
                outcome: Outcome::Response(json!({ "amend_id": "" })),
            },
            FuturesCase {
                label: "amend with no effect",
                call: |f| {
                    let pending = f.order_amend(
                        crate::FuturesOrderAmendRequest::by_cl_ord_id(futures_id(FUTURES_ID))
                            .order_volume(rust_decimal::Decimal::from(2)),
                    );
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "amend_order",
                params: json!({ "cl_ord_id": FUTURES_ID, "order_qty": 2 }),
                reply: Reply::Error("Other:EDIT_HAS_NO_EFFECT"),
                outcome: Outcome::Code("NON_AMENDABLE_FIELD"),
            },
            FuturesCase {
                label: "amend by order id with a deadline",
                call: |f| {
                    let pending = f.order_amend(
                        crate::FuturesOrderAmendRequest::by_order_id(crate::TxId::new(
                            FUTURES_ORDER_ID,
                        ))
                        .order_volume(rust_decimal::Decimal::from(2))
                        .deadline(crate::TimeSpec::from_unix_secs(1_767_225_600)),
                    );
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "amend_order",
                params: json!({
                    "order_id": FUTURES_ORDER_ID, "order_qty": 2,
                    "deadline": "2026-01-01T00:00:00Z",
                }),
                reply: Reply::Frame(frame(include_str!(
                    "../../../tests/fixtures/unified_ws/order_amend_ack.json"
                ))),
                outcome: Outcome::Response(json!({ "amend_id": "" })),
            },
            FuturesCase {
                label: "cancel",
                call: |f| {
                    let pending = f.cancel(futures_id(FUTURES_ID));
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "cancel_order",
                params: json!({ "cl_ord_id": [FUTURES_ID] }),
                reply: Reply::Frame(frame(include_str!(
                    "../../../tests/fixtures/unified_ws/order_cancel_ack.json"
                ))),
                outcome: Outcome::Response(json!({ "count": 1, "pending": false })),
            },
            FuturesCase {
                label: "cancel by order id",
                call: |f| {
                    let pending = f.cancel(crate::TxId::new(FUTURES_ORDER_ID));
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "cancel_order",
                params: json!({ "order_id": [FUTURES_ORDER_ID] }),
                reply: Reply::Result(json!({ "order_id": FUTURES_ORDER_ID })),
                outcome: Outcome::Response(json!({ "count": 1, "pending": false })),
            },
            FuturesCase {
                label: "cancel of an unknown order id",
                call: |f| {
                    let pending = f.cancel(crate::TxId::new(FUTURES_ORDER_ID));
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "cancel_order",
                params: json!({ "order_id": [FUTURES_ORDER_ID] }),
                reply: Reply::Error("Other:ORDER_FOR_CANCELLATION_NOT_FOUND"),
                outcome: Outcome::Code("ORDER_NOT_FOUND"),
            },
            FuturesCase {
                label: "cancel of a gone order",
                call: |f| {
                    let pending = f.cancel(futures_id(FUTURES_ID));
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "cancel_order",
                params: json!({ "cl_ord_id": [FUTURES_ID] }),
                reply: Reply::Error("Other:ORDER_FOR_CANCELLATION_NOT_FOUND"),
                outcome: Outcome::Code("ORDER_NOT_FOUND"),
            },
            FuturesCase {
                label: "batch cancel",
                call: |f| {
                    let pending =
                        f.cancel_batch(vec![futures_id(FUTURES_ID), futures_id(FUTURES_ID_2)]);
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "batch_cancel",
                params: json!({ "cl_ord_id": [FUTURES_ID, FUTURES_ID_2] }),
                reply: Reply::Frame(frame(include_str!(
                    "../../../tests/fixtures/unified_ws/order_batch_cancel_ack.json"
                ))),
                outcome: Outcome::Response(json!({ "count": 2 })),
            },
            FuturesCase {
                label: "cancel all",
                call: |f| {
                    let pending = f.cancel_all();
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "cancel_all",
                params: json!({}),
                reply: Reply::Frame(frame(include_str!(
                    "../../../tests/fixtures/unified_ws/order_cancel_all_ack.json"
                ))),
                outcome: Outcome::Response(json!({ "count": 3 })),
            },
            FuturesCase {
                label: "dead-man arm",
                call: |f| {
                    let pending = f.cancel_all_orders_after(60);
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "cancel_all_orders_after",
                params: json!({ "timeout": 60 }),
                reply: Reply::Frame(frame(include_str!(
                    "../../../tests/fixtures/unified_ws/order_deadman_arm_ack.json"
                ))),
                outcome: Outcome::Response(json!({
                    "current_time": "2026-10-29T09:58:17Z",
                    "trigger_time": "2026-10-29T09:59:17Z",
                })),
            },
            FuturesCase {
                label: "dead-man disarm",
                call: |f| {
                    let pending = f.cancel_all_orders_after(0);
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "cancel_all_orders_after",
                params: json!({ "timeout": 0 }),
                reply: Reply::Frame(frame(include_str!(
                    "../../../tests/fixtures/unified_ws/order_deadman_disarm_ack.json"
                ))),
                outcome: Outcome::Response(json!({ "current_time": "2026-10-29T09:58:17Z" })),
            },
            FuturesCase {
                label: "batch order",
                call: |f| {
                    let pending = f.order_batch(crate::FuturesOrderBatchRequest::new(
                        "PF_XBTUSD",
                        vec![
                            futures_limit_buy(FUTURES_ID),
                            futures_limit_buy(FUTURES_ID_2),
                        ],
                    ));
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "batch_add",
                params: json!({
                    "symbol": "PF_XBTUSD",
                    "orders": [
                        { "order_type": "limit", "side": "buy", "order_qty": 1,
                          "cl_ord_id": FUTURES_ID, "limit_price": 100, "limit_price_type": "static" },
                        { "order_type": "limit", "side": "buy", "order_qty": 1,
                          "cl_ord_id": FUTURES_ID_2, "limit_price": 100, "limit_price_type": "static" },
                    ],
                }),
                reply: Reply::Frame(frame(include_str!(
                    "../../../tests/fixtures/unified_ws/order_batch_add_ack.json"
                ))),
                outcome: Outcome::Response(json!({ "orders": [
                    { "descr": {}, "txid": "OF-1" }, { "descr": {}, "txid": "OF-2" },
                ] })),
            },
            FuturesCase {
                label: "batch order with a failed row",
                call: |f| {
                    let pending = f.order_batch(crate::FuturesOrderBatchRequest::new(
                        "PF_XBTUSD",
                        vec![futures_limit_buy(FUTURES_ID), futures_limit_buy(FUTURES_ID)],
                    ));
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "batch_add",
                params: json!({
                    "symbol": "PF_XBTUSD",
                    "orders": [
                        { "order_type": "limit", "side": "buy", "order_qty": 1,
                          "cl_ord_id": FUTURES_ID, "limit_price": 100, "limit_price_type": "static" },
                        { "order_type": "limit", "side": "buy", "order_qty": 1,
                          "cl_ord_id": FUTURES_ID, "limit_price": 100, "limit_price_type": "static" },
                    ],
                }),
                reply: Reply::Frame(frame(include_str!(
                    "../../../tests/fixtures/unified_ws/order_batch_add_row_error.json"
                ))),
                outcome: Outcome::Response(json!({ "orders": [
                    { "descr": {}, "txid": "OF-1" },
                    { "descr": {}, "error": "EGeneral:Other:CLIENT_ORDER_ID_IN_USE" },
                ] })),
            },
            FuturesCase {
                label: "batch order refused whole",
                call: |f| {
                    let pending = f.order_batch(crate::FuturesOrderBatchRequest::new(
                        "PF_XBTUSD",
                        vec![
                            futures_limit_buy(FUTURES_ID),
                            futures_limit_buy(FUTURES_ID_2),
                        ],
                    ));
                    Box::pin(async move { pending.await.map(json_of) })
                },
                method: "batch_add",
                params: json!({
                    "symbol": "PF_XBTUSD",
                    "orders": [
                        { "order_type": "limit", "side": "buy", "order_qty": 1,
                          "cl_ord_id": FUTURES_ID, "limit_price": 100, "limit_price_type": "static" },
                        { "order_type": "limit", "side": "buy", "order_qty": 1,
                          "cl_ord_id": FUTURES_ID_2, "limit_price": 100, "limit_price_type": "static" },
                    ],
                }),
                reply: Reply::Error("Invalid arguments:price"),
                outcome: Outcome::Code("INVALID_ORDER"),
            },
        ]
    }

    async fn next_unseen_frame(
        sock: &DriveableSocketHandle,
        method: &str,
        seen: &mut std::collections::HashSet<u64>,
    ) -> serde_json::Value {
        let frame = tokio::time::timeout(BUDGET, async {
            loop {
                let hit = sock.sent_text().into_iter().find_map(|body| {
                    let frame: serde_json::Value = serde_json::from_str(&body).ok()?;
                    let id = frame["req_id"].as_u64()?;
                    (frame["method"] == method && !seen.contains(&id)).then_some(frame)
                });
                if let Some(frame) = hit {
                    return frame;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timeout: no new {method} frame was sent"));
        seen.insert(frame["req_id"].as_u64().expect("numeric req_id"));
        frame
    }

    #[derive(Default)]
    struct FuturesDriver {
        socket: Option<DriveableSocketHandle>,
        seen: std::collections::HashSet<u64>,
    }

    impl FuturesDriver {
        async fn run(
            &mut self,
            w: &WiredClient,
            case: &FuturesCase,
        ) -> Result<serde_json::Value, TradeError> {
            let call = tokio::spawn((case.call)(w.trade.futures()));
            if self.socket.is_none() {
                self.socket =
                    Some(socket_with_at_least(&w.factory, WsUrl::UnifiedPrivate, 0).await);
            }
            let sock = self.socket.as_ref().expect("socket opened above");
            let sent = next_unseen_frame(sock, case.method, &mut self.seen).await;
            let req_id = sent["req_id"].as_u64().expect("numeric req_id");

            let mut expected = case.params.clone();
            expected["token"] = "seeded-token-UPC".into();
            expected["asset_class"] = "derivatives".into();
            assert_eq!(sent["params"], expected, "{}", case.label);

            let mut reply = serde_json::json!({ "method": case.method, "req_id": req_id });
            match &case.reply {
                Reply::Result(result) => {
                    reply["success"] = true.into();
                    reply["result"] = result.clone();
                }
                Reply::Error(error) => {
                    reply["success"] = false.into();
                    reply["error"] = (*error).into();
                }
                Reply::Frame(captured) => {
                    reply = captured.clone();
                    reply["req_id"] = req_id.into();
                }
            }
            sock.emit_text(reply.to_string());
            tokio::time::timeout(BUDGET, call)
                .await
                .unwrap_or_else(|_| panic!("{}: never resolved", case.label))
                .expect("task panicked")
        }
    }

    #[tokio::test]
    async fn a_rejected_session_token_is_a_definite_session_expired_for_every_futures_op() {
        use crate::error::{ApiError, ErrorCategory};
        let ops = [
            "order",
            "amend",
            "cancel",
            "batch cancel",
            "cancel all",
            "dead-man arm",
            "batch order",
        ];
        let cases = futures_cases()
            .into_iter()
            .filter(|case| ops.contains(&case.label))
            .map(|case| FuturesCase {
                reply: Reply::Error("Invalid session"),
                outcome: Outcome::Code("SESSION_EXPIRED"),
                ..case
            });
        for case in cases {
            let w = unified_wired_client(token_envelope(""));
            let ambiguous = count_events(
                &w.bus,
                crate::dispatch::EventType::OrderPlacementAmbiguousEvent,
            );
            let failed = count_events(&w.bus, crate::dispatch::EventType::WsRequestFailedEvent);
            let error = FuturesDriver::default()
                .run(&w, &case)
                .await
                .expect_err("a token rejection fails the call");
            assert_eq!(
                (error.code(), error.category(), error.retryable()),
                ("SESSION_EXPIRED", ErrorCategory::Auth, true),
                "{}: {error:?}",
                case.label
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(
                (
                    ambiguous.load(Ordering::Relaxed),
                    failed.load(Ordering::Relaxed)
                ),
                (0, 0),
                "{}: a definite rejection fires no ambiguity or request-failed event",
                case.label
            );
            w.reactor.abort();
            w.bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn order_amend_and_cancel_type_an_insufficient_margin_reply() {
        use crate::error::ApiError;
        let ops = ["order", "amend", "cancel"];
        let w = unified_wired_client(unused_refresh_envelope());
        let mut driver = FuturesDriver::default();
        let mut ran = 0;
        for case in futures_cases()
            .into_iter()
            .filter(|case| ops.contains(&case.label))
        {
            ran += 1;
            let case = FuturesCase {
                reply: Reply::Error("Other:INSUFFICIENT_MARGIN"),
                ..case
            };
            let error = driver
                .run(&w, &case)
                .await
                .expect_err("a margin refusal fails the call");
            assert!(
                matches!(error, TradeError::InsufficientMargin { .. })
                    && error.request_id().is_some(),
                "{}: {error:?}",
                case.label
            );
        }
        assert_eq!(ran, ops.len(), "every listed op ran");
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_throttle_reply_sets_retry_after_ts_for_every_futures_op() {
        let ops = [
            "order",
            "amend",
            "cancel",
            "batch cancel",
            "cancel all",
            "dead-man arm",
            "batch order",
        ];
        let cases = futures_cases()
            .into_iter()
            .filter(|case| ops.contains(&case.label))
            .map(|case| FuturesCase {
                reply: Reply::Frame(serde_json::json!({
                    "method": case.method,
                    "success": false,
                    "error": "Rate limit exceeded",
                    "retry_after_ms": 588,
                })),
                ..case
            });
        let mut ran = 0;
        for case in cases {
            ran += 1;
            let w = unified_wired_client(token_envelope(""));
            let before = crate::rest::surface::unix_now_ms().expect("wall clock");
            let error = FuturesDriver::default()
                .run(&w, &case)
                .await
                .expect_err("a throttle reply fails the call");
            let after = crate::rest::surface::unix_now_ms().expect("wall clock");
            match error {
                TradeError::RateLimited {
                    retry_after_ts: Some(ts),
                    ..
                } => assert!(
                    ts * 1000 >= before + 588 && ts <= (after + 588).div_ceil(1000),
                    "{}: {ts} not in [{}ms, {}s]",
                    case.label,
                    before + 588,
                    (after + 588).div_ceil(1000)
                ),
                other => panic!(
                    "{}: expected RateLimited with a retry time, got {other:?}",
                    case.label
                ),
            }
            w.reactor.abort();
            w.bus.stop_reactors();
        }
        assert_eq!(ran, ops.len(), "every listed op ran");
    }

    #[tokio::test]
    async fn every_futures_order_op_sends_its_frame_and_decodes_the_venue_reply() {
        use crate::dispatch::{EventEnvelope, EventPayload, EventType, OrderOp, OrderSubmitStatus};
        let w = unified_wired_client(unused_refresh_envelope());
        let submitted = Arc::new(std::sync::Mutex::new(Vec::new()));
        {
            let submitted = Arc::clone(&submitted);
            let _ = w.bus.subscribe(
                EventType::OrderSubmittedEvent,
                Arc::new(move |env: &EventEnvelope| {
                    if let EventPayload::OrderSubmittedEvent {
                        op, url, status, ..
                    } = &env.payload
                    {
                        submitted.lock().unwrap().push((*op, *url, status.clone()));
                    }
                }),
                1,
            );
        }
        let mut driver = FuturesDriver::default();
        for case in futures_cases() {
            match (&case.outcome, driver.run(&w, &case).await) {
                (Outcome::Response(expected), Ok(got)) => {
                    assert_eq!(&got, expected, "{}", case.label)
                }
                (Outcome::Code(code), Err(error)) => {
                    use crate::error::ApiError;
                    assert_eq!(error.code(), *code, "{}: {error:?}", case.label);
                    assert!(error.request_id().is_some(), "{}: request id", case.label);
                }
                (_, other) => panic!("{}: unexpected result {other:?}", case.label),
            }
        }
        assert_eq!(
            w.supervisor.current_state(WsUrl::Auth),
            ConnectionState::Idle,
            "Futures orders never touch Spot Auth"
        );
        let wire_error = |code: &str| OrderSubmitStatus::WireError { code: code.into() };
        let expected = vec![
            (
                OrderOp::AddOrder,
                WsUrl::UnifiedPrivate,
                OrderSubmitStatus::WireSent {
                    txid: crate::types::TxId::new("OF-1"),
                },
            ),
            (
                OrderOp::AddOrder,
                WsUrl::UnifiedPrivate,
                wire_error("INVALID_ORDER"),
            ),
            (
                OrderOp::AddOrder,
                WsUrl::UnifiedPrivate,
                wire_error("RATE_LIMIT_EXCEEDED"),
            ),
            (
                OrderOp::AmendOrder,
                WsUrl::UnifiedPrivate,
                OrderSubmitStatus::WireAccepted,
            ),
            (
                OrderOp::AmendOrder,
                WsUrl::UnifiedPrivate,
                wire_error("NON_AMENDABLE_FIELD"),
            ),
            (
                OrderOp::CancelOrder,
                WsUrl::UnifiedPrivate,
                OrderSubmitStatus::WireAccepted,
            ),
            (
                OrderOp::CancelOrder,
                WsUrl::UnifiedPrivate,
                wire_error("ORDER_NOT_FOUND"),
            ),
        ];
        tokio::time::timeout(BUDGET, async {
            while submitted.lock().unwrap().len() < expected.len() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: lifecycle events never delivered");
        assert_eq!(
            *submitted.lock().unwrap(),
            expected,
            "dry runs and batches emit no OrderSubmittedEvent"
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    fn futures_deadman_case(timeout: u32, trigger_time: &'static str) -> FuturesCase {
        FuturesCase {
            label: "dead-man",
            call: |f| {
                let pending = f.cancel_all_orders_after(60);
                Box::pin(async move { pending.await.map(json_of) })
            },
            method: "cancel_all_orders_after",
            params: serde_json::json!({ "timeout": timeout }),
            reply: Reply::Result(
                serde_json::json!({ "currentTime": "t0", "triggerTime": trigger_time }),
            ),
            outcome: Outcome::Code(""),
        }
    }

    async fn arm_then_begin_close(
        w: &WiredClient,
        driver: &mut FuturesDriver,
    ) -> Option<impl std::future::Future<Output = Option<TradeError>> + Send + 'static> {
        driver
            .run(w, &futures_deadman_case(60, "t1"))
            .await
            .expect("arm");
        w.bus.begin_shutdown();
        w.trade.futures().begin_close_disarm(&w.bus)
    }

    async fn wait_for_disarm_frame(sock: &DriveableSocketHandle) -> serde_json::Value {
        tokio::time::timeout(BUDGET, async {
            loop {
                let hit = sock.sent_text().into_iter().rev().find_map(|body| {
                    let frame: serde_json::Value = serde_json::from_str(&body).ok()?;
                    (frame["method"] == "cancel_all_orders_after"
                        && frame["params"]["timeout"] == 0)
                        .then_some(frame)
                });
                if let Some(frame) = hit {
                    return frame;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: the disarm frame was never sent")
    }

    #[tokio::test]
    async fn a_disarm_queued_before_the_close_marker_is_written_and_then_drained() {
        let w = unified_wired_client(unused_refresh_envelope());
        let mut driver = FuturesDriver::default();
        let disarm = arm_then_begin_close(&w, &mut driver)
            .await
            .expect("an armed switch on an open socket is disarmed");
        w.bus
            .try_post_caller_inbound(crate::dispatch::CallerInbound::ClientClose {
                request_id: 9,
                initiated_at: crate::types::MonotonicInstant::now(),
            })
            .expect("post the close marker");

        let sock = driver.socket.as_ref().expect("socket");
        let frame = wait_for_disarm_frame(sock).await;
        assert_eq!(frame["params"]["asset_class"], "derivatives");
        assert!(
            tokio::time::timeout(BUDGET, disarm)
                .await
                .expect("timeout: the close never resolved the disarm")
                .is_none(),
            "a disarm the close drains after it was written is not a failure"
        );
        assert!(
            w.trade.futures().begin_close_disarm(&w.bus).is_none(),
            "a disarm is queued once"
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn begin_close_disarm_reports_a_venue_refusal_as_a_failure() {
        use crate::error::ApiError;
        let w = unified_wired_client(unused_refresh_envelope());
        let mut driver = FuturesDriver::default();
        let disarm = arm_then_begin_close(&w, &mut driver)
            .await
            .expect("an armed switch on an open socket is disarmed");
        let sock = driver.socket.as_ref().expect("socket");
        let frame = wait_for_disarm_frame(sock).await;
        sock.emit_text(
            serde_json::json!({
                "method": "cancel_all_orders_after", "req_id": frame["req_id"],
                "success": false, "error": "Rate limit exceeded",
            })
            .to_string(),
        );
        let error = tokio::time::timeout(BUDGET, disarm)
            .await
            .expect("timeout: the refusal never resolved")
            .expect("a refused disarm is a failure");
        assert_eq!(error.code(), "RATE_LIMIT_EXCEEDED");
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_close_has_nothing_to_disarm_unless_a_switch_was_armed() {
        // (label, arm, disarmed again)
        let cases = [
            ("never armed", false, false),
            ("armed then disarmed", true, true),
        ];
        for (label, arm, disarm_again) in cases {
            let w = unified_wired_client(unused_refresh_envelope());
            let mut driver = FuturesDriver::default();
            if arm {
                driver
                    .run(&w, &futures_deadman_case(60, "t1"))
                    .await
                    .expect("arm");
            }
            if disarm_again {
                let mut off = futures_deadman_case(0, "0");
                off.call = |f| {
                    let pending = f.cancel_all_orders_after(0);
                    Box::pin(async move { pending.await.map(json_of) })
                };
                driver.run(&w, &off).await.expect("disarm");
            }
            w.bus.begin_shutdown();
            assert!(
                w.trade.futures().begin_close_disarm(&w.bus).is_none(),
                "{label}"
            );
            w.reactor.abort();
            w.bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn an_arm_without_a_reply_is_still_disarmed_on_close() {
        let w = unified_wired_client(unused_refresh_envelope());
        let pending = w.trade.futures().cancel_all_orders_after(60);
        let arm = tokio::spawn(async move { pending.await });
        let sock = socket_with_at_least(&w.factory, WsUrl::UnifiedPrivate, 0).await;
        next_unseen_frame(
            &sock,
            "cancel_all_orders_after",
            &mut std::collections::HashSet::new(),
        )
        .await;
        arm.abort();
        w.bus.begin_shutdown();
        assert!(
            w.trade.futures().begin_close_disarm(&w.bus).is_some(),
            "an arm the caller never saw answered may still be live"
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn an_arm_requested_after_the_close_began_is_refused_unsent() {
        use crate::error::ApiError;
        let w = unified_wired_client(unused_refresh_envelope());
        w.bus.begin_shutdown();
        let error = w
            .trade
            .futures()
            .cancel_all_orders_after(60)
            .await
            .expect_err("an arm after the close began is refused");
        assert_eq!(error.code(), "CLIENT_CLOSED");
        assert!(
            w.factory.handle_for(WsUrl::UnifiedPrivate).is_none(),
            "nothing was sent"
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_disarm_on_a_private_socket_that_is_not_open_is_a_failure() {
        let w = unified_wired_client(unused_refresh_envelope());
        let mut driver = FuturesDriver::default();
        driver
            .run(&w, &futures_deadman_case(60, "t1"))
            .await
            .expect("arm");
        driver
            .socket
            .as_ref()
            .expect("socket")
            .drop_with(crate::transport::TransportError {
                kind: crate::transport::TransportErrorKind::SocketReset,
                transient: true,
            });
        tokio::time::timeout(BUDGET, async {
            while w.supervisor.current_state(WsUrl::UnifiedPrivate) == ConnectionState::Open {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: the dropped socket never left Open");
        w.bus.begin_shutdown();
        let disarm = w
            .trade
            .futures()
            .begin_close_disarm(&w.bus)
            .expect("an armed switch is always disarmed");
        let error = tokio::time::timeout(BUDGET, disarm)
            .await
            .expect("timeout: the disarm never resolved")
            .expect("a disarm that cannot be sent is a failure");
        assert!(
            matches!(error, TradeError::Transport { .. }),
            "got {error:?}"
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn spot_and_futures_orders_share_a_client_isolated_by_connection() {
        use crate::dispatch::{EventEnvelope, EventPayload, EventType};
        let w = unified_wired_client(unused_refresh_envelope());
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        for event_type in [
            EventType::OrderSubmittedEvent,
            EventType::WsRequestFailedEvent,
        ] {
            let events = Arc::clone(&events);
            let _ = w.bus.subscribe(
                event_type,
                Arc::new(move |env: &EventEnvelope| match &env.payload {
                    EventPayload::OrderSubmittedEvent { url, .. } => {
                        events.lock().unwrap().push(("submitted", *url));
                    }
                    EventPayload::WsRequestFailedEvent { url, .. } => {
                        events.lock().unwrap().push(("failed", *url));
                    }
                    _ => {}
                }),
                1,
            );
        }
        let place_spot = |id: &str| {
            let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
            req.cl_ord_id = Some(futures_id(id));
            let pending = w.trade.order(req);
            tokio::spawn(async move { pending.await })
        };
        let place_futures = |id: &str| {
            let pending = w.trade.futures().order(crate::FuturesOrderRequest::new(
                "PF_XBTUSD",
                futures_limit_buy(id),
            ));
            tokio::spawn(async move { pending.await })
        };
        let ack =
            |sock: &DriveableSocketHandle, frame: &serde_json::Value, id: &str, order: &str| {
                sock.emit_text(
                    serde_json::json!({
                        "method": "add_order", "req_id": frame["req_id"], "success": true,
                        "result": { "cl_ord_id": id, "order_id": order },
                    })
                    .to_string(),
                );
            };

        let (spot, futures) = (place_spot(FUTURES_ID), place_futures(FUTURES_ID_2));
        let mut seen = std::collections::HashSet::new();
        let (spot_sock, futures_sock) = (
            socket_with_sent(&w.factory, WsUrl::Auth, 1).await,
            socket_with_sent(&w.factory, WsUrl::UnifiedPrivate, 1).await,
        );
        let spot_frame = next_unseen_frame(&spot_sock, "add_order", &mut seen).await;
        let futures_frame = next_unseen_frame(&futures_sock, "add_order", &mut seen).await;
        assert!(
            spot_frame["params"].get("asset_class").is_none(),
            "a Spot frame never carries a Futures asset class"
        );
        assert_eq!(futures_frame["params"]["asset_class"], "derivatives");
        assert_eq!(futures_frame["params"]["symbol"], "PF_XBTUSD");

        ack(&spot_sock, &spot_frame, FUTURES_ID, "OS-1");
        ack(&futures_sock, &futures_frame, FUTURES_ID_2, "OF-1");
        let txid = |placed: crate::AddOrderResponse| placed.txid.map(|t| t.as_str().to_string());
        assert_eq!(
            txid(
                spot.await
                    .expect("task panicked")
                    .expect("Spot order placed")
            ),
            Some("OS-1".into())
        );
        assert_eq!(
            txid(
                futures
                    .await
                    .expect("task panicked")
                    .expect("Futures order placed")
            ),
            Some("OF-1".into())
        );

        let (spot, futures) = (place_spot(FUTURES_ID_2), place_futures(FUTURES_ID));
        let spot_frame = next_unseen_frame(&spot_sock, "add_order", &mut seen).await;
        next_unseen_frame(&futures_sock, "add_order", &mut seen).await;
        futures_sock.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        let dropped = futures
            .await
            .expect("task panicked")
            .expect_err("a drop fails the in-flight Futures order");
        assert!(
            matches!(dropped, TradeError::Transport { .. }),
            "{dropped:?}"
        );
        ack(&spot_sock, &spot_frame, FUTURES_ID_2, "OS-2");
        assert!(
            spot.await.expect("task panicked").is_ok(),
            "a Futures drop never fails a Spot request"
        );

        tokio::time::timeout(BUDGET, async {
            while events.lock().unwrap().len() < 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: lifecycle events never delivered");
        let mut seen_events = events.lock().unwrap().clone();
        seen_events.sort_by_key(|(kind, url)| (*kind, format!("{url:?}")));
        assert_eq!(
            seen_events,
            vec![
                ("failed", WsUrl::UnifiedPrivate),
                ("submitted", WsUrl::Auth),
                ("submitted", WsUrl::Auth),
                ("submitted", WsUrl::UnifiedPrivate),
            ]
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn futures_orders_leave_the_spot_tracker_and_index_untouched() {
        use crate::rate_limit::Scope;
        use crate::types::{ApiKey, MonotonicInstant, Symbol};
        use serde_json::json;
        let w = wire_client_core(
            "seeded-token-UPC",
            unused_refresh_envelope(),
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            None,
            false,
            true,
        );
        let index = w.index.clone().expect("with_index=true wires the index");
        let key = ApiKey::new("test-api-key");
        let pair = Symbol::new("BTC/USDC").unwrap();
        let now = MonotonicInstant::now();
        index.insert(futures_id(FUTURES_ID), pair.clone(), now);
        for _ in 0..53 {
            w.trading_rl
                .consume(Scope::Pair(key.clone(), pair.clone()), 1.0, now)
                .expect("pre-drain within the cap");
        }
        let mut driver = FuturesDriver::default();
        let cancel = FuturesCase {
            label: "cancel an id the Spot index knows",
            call: |f| {
                let pending = f.cancel(futures_id(FUTURES_ID));
                Box::pin(async move { pending.await.map(json_of) })
            },
            method: "cancel_order",
            params: json!({ "cl_ord_id": [FUTURES_ID] }),
            reply: Reply::Result(json!({ "cl_ord_id": FUTURES_ID })),
            outcome: Outcome::Code(""),
        };
        assert!(
            driver.run(&w, &cancel).await.is_ok(),
            "a drained Spot pair never blocks a Futures cancel"
        );
        let placed = FuturesCase {
            label: "place",
            call: |f| {
                let pending = f.order(crate::FuturesOrderRequest::new(
                    "PF_XBTUSD",
                    futures_limit_buy(FUTURES_ID_2),
                ));
                Box::pin(async move { pending.await.map(json_of) })
            },
            method: "add_order",
            params: json!({
                "order_type": "limit", "side": "buy", "order_qty": 1, "symbol": "PF_XBTUSD",
                "cl_ord_id": FUTURES_ID_2, "limit_price": 100, "limit_price_type": "static",
            }),
            reply: Reply::Result(json!({ "cl_ord_id": FUTURES_ID_2, "order_id": "OF-1" })),
            outcome: Outcome::Code(""),
        };
        assert!(driver.run(&w, &placed).await.is_ok());
        assert!(
            index
                .lookup_and_promote(&futures_id(FUTURES_ID_2))
                .is_none(),
            "a Futures order is never indexed against a Spot pair"
        );
        let other_pair = Symbol::new("ETH/USDC").unwrap();
        let other = Scope::Pair(key, other_pair);
        w.trading_rl
            .consume(other.clone(), 1.0, now)
            .expect("a fresh pair has headroom");
        let headroom = w.trading_rl.headroom(other.clone(), now);
        let cancel_all = FuturesCase {
            label: "cancel all",
            call: |f| {
                let pending = f.cancel_all();
                Box::pin(async move { pending.await.map(json_of) })
            },
            method: "cancel_all",
            params: json!({}),
            reply: Reply::Result(json!({ "count": 1 })),
            outcome: Outcome::Code(""),
        };
        assert!(driver.run(&w, &cancel_all).await.is_ok());
        assert_eq!(
            w.trading_rl.headroom(other, now),
            headroom,
            "a Futures cancel-all never charges Spot's counters"
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_rejected_futures_batch_with_rows_never_snaps_the_spot_trading_tracker() {
        use crate::rate_limit::Scope;
        use crate::types::{ApiKey, MonotonicInstant, Symbol};
        use serde_json::json;
        let w = wire_client_core(
            "seeded-token-UPC",
            unused_refresh_envelope(),
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            None,
            false,
            false,
        );
        let now = MonotonicInstant::now();
        let spot = Scope::Pair(
            ApiKey::new("test-api-key"),
            Symbol::new("ETH/USDC").unwrap(),
        );
        w.trading_rl
            .consume(spot.clone(), 1.0, now)
            .expect("a fresh pair has headroom");
        let headroom = w.trading_rl.headroom(spot.clone(), now);
        let mut rejected = frame(include_str!(
            "../../../tests/fixtures/unified_ws/order_batch_add_row_error.json"
        ));
        rejected["error"] = json!("EOrder:Domain rate limit exceeded");
        let batch = FuturesCase {
            label: "batch refused with rows and a throttle envelope",
            call: |f| {
                let pending = f.order_batch(crate::FuturesOrderBatchRequest::new(
                    "PF_XBTUSD",
                    vec![
                        futures_limit_buy(FUTURES_ID),
                        futures_limit_buy(FUTURES_ID_2),
                    ],
                ));
                Box::pin(async move { pending.await.map(json_of) })
            },
            method: "batch_add",
            params: json!({
                "symbol": "PF_XBTUSD",
                "orders": [
                    { "order_type": "limit", "side": "buy", "order_qty": 1,
                      "cl_ord_id": FUTURES_ID, "limit_price": 100, "limit_price_type": "static" },
                    { "order_type": "limit", "side": "buy", "order_qty": 1,
                      "cl_ord_id": FUTURES_ID_2, "limit_price": 100, "limit_price_type": "static" },
                ],
            }),
            reply: Reply::Frame(rejected),
            outcome: Outcome::Response(json!({ "orders": [
                { "descr": {}, "txid": "OF-1" },
                { "descr": {}, "error": "EGeneral:Other:CLIENT_ORDER_ID_IN_USE" },
            ] })),
        };
        assert!(FuturesDriver::default().run(&w, &batch).await.is_ok());
        assert_eq!(
            w.trading_rl.headroom(spot, now),
            headroom,
            "a Futures rejection never touches Spot's counters"
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_bare_order_on_a_cold_unified_private_socket_is_its_own_auth_probe() {
        let w = unified_wired_client(unused_refresh_envelope());
        assert_cold_unified_private_order_self_auths(&w).await;
        assert_eq!(
            w.supervisor.current_state(WsUrl::Auth),
            ConnectionState::Idle,
            "a Futures order must not open Spot Auth"
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    /// Sends a bare Futures order on a cold Unified private socket and has the venue refuse it with `error`.
    async fn reject_bare_unified_private_order(w: &WiredClient, error: &str) {
        let ws = Arc::clone(&w.ws);
        let req_id = ws.next_req_id();
        let order = tokio::spawn(async move {
            ws.ensure_order_sendable(WsUrl::UnifiedPrivate).await?;
            ws.send_request_costed(
                WsUrl::UnifiedPrivate,
                "add_order".to_string(),
                req_id,
                serde_json::json!({ "symbol": "PF_XBTUSD" }),
                crate::rest::RateLimitCost::None,
            )
            .expect("untracked cost is never rate limited")
            .recv()
            .await
        });
        let sock = socket_with_at_least(&w.factory, WsUrl::UnifiedPrivate, 0).await;
        wait_for_sent_add_order(&sock).await;
        sock.emit_text(
            serde_json::json!({
                "method": "add_order", "req_id": req_id, "success": false, "error": error,
            })
            .to_string(),
        );
        let resp = tokio::time::timeout(BUDGET, order)
            .await
            .expect("timeout: the refused order never resolved")
            .expect("task panicked")
            .expect("the venue's refusal is delivered, not drained");
        assert_eq!(
            (resp.success, resp.error.as_deref()),
            (false, Some(error)),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_bare_unified_private_order_rejected_for_its_token_never_opens_the_socket() {
        for error in [
            "Invalid session",
            "ESession:Invalid session token",
            "Missing or invalid 'params.token'",
        ] {
            let w = unified_wired_client(token_envelope(""));
            reject_bare_unified_private_order(&w, error).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_ne!(
                w.supervisor.current_state(WsUrl::UnifiedPrivate),
                ConnectionState::Open,
                "{error}: a rejected token must not authenticate the socket"
            );
            assert!(
                w.auth_stack.cached_token().is_none(),
                "{error}: the rejected token is invalidated and refreshed, not reused"
            );
            assert!(
                !w.supervisor.order_gate().send_ready(WsUrl::UnifiedPrivate),
                "{error}: no retry is admitted until the refresh lands"
            );
            w.reactor.abort();
            w.bus.stop_reactors();
        }
    }

    fn sent_with_method(sock: &DriveableSocketHandle, method: &str) -> Vec<u64> {
        sock.sent_text()
            .iter()
            .filter_map(|body| serde_json::from_str::<serde_json::Value>(body).ok())
            .filter(|v| v["method"] == method)
            .filter_map(|v| v["req_id"].as_u64())
            .collect()
    }

    async fn wait_for_method_frames(sock: &DriveableSocketHandle, method: &str, count: usize) {
        tokio::time::timeout(BUDGET, async {
            while sent_with_method(sock, method).len() < count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timeout: waiting for {count} {method} frame(s)"));
    }

    async fn unified_private_socket(w: &WiredClient) -> DriveableSocketHandle {
        socket_with_at_least(&w.factory, WsUrl::UnifiedPrivate, 0).await
    }

    fn futures_placement(
        w: &WiredClient,
        id: &str,
    ) -> impl Future<Output = Result<crate::AddOrderResponse, TradeError>> + use<> {
        w.trade
            .futures()
            .order(crate::FuturesOrderRequest::new(
                "PF_XBTUSD",
                futures_limit_buy(id),
            ))
            .into_future()
    }

    #[tokio::test]
    async fn futures_placements_beyond_the_in_flight_limit_fail_unsent_and_cancels_do_not() {
        let w = wire_client_core_limited(
            "seeded-token-UPC",
            unused_refresh_envelope(),
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            Some(60_000),
            true,
            false,
            Some(1),
        );
        let first = tokio::spawn(futures_placement(&w, FUTURES_ID));
        let sock = unified_private_socket(&w).await;
        wait_for_method_frames(&sock, "add_order", 1).await;

        let refused = tokio::time::timeout(BUDGET, futures_placement(&w, FUTURES_ID_2))
            .await
            .expect("timeout: a placement with no free slot must not wait");
        assert!(
            matches!(refused, Err(TradeError::InFlightLimitReached { .. })),
            "a placement with no free slot is refused: {refused:?}"
        );
        assert_eq!(
            sent_with_method(&sock, "add_order").len(),
            1,
            "the refusal sent nothing"
        );

        let cancel = w.trade.futures().cancel(crate::TxId::new(FUTURES_ORDER_ID));
        let cancel = tokio::spawn(async move { cancel.await });
        wait_for_method_frames(&sock, "cancel_order", 1).await;
        assert_eq!(
            sent_with_method(&sock, "add_order").len(),
            1,
            "a cancel takes no slot"
        );

        sock.emit_text(
            serde_json::json!({
                "method": "add_order", "req_id": sent_with_method(&sock, "add_order")[0],
                "success": true, "result": { "cl_ord_id": FUTURES_ID, "order_id": "OF-1" },
            })
            .to_string(),
        );
        sock.emit_text(
            serde_json::json!({
                "method": "cancel_order", "req_id": sent_with_method(&sock, "cancel_order")[0],
                "success": true, "result": { "order_id": FUTURES_ORDER_ID },
            })
            .to_string(),
        );
        tokio::time::timeout(BUDGET, first)
            .await
            .expect("timeout: the first placement never resolved")
            .expect("task panicked")
            .expect("the first placement is answered");
        tokio::time::timeout(BUDGET, cancel)
            .await
            .expect("timeout: the cancel never resolved")
            .expect("task panicked")
            .expect("the cancel is answered");

        let third = tokio::spawn(futures_placement(&w, FUTURES_ID_2));
        wait_for_method_frames(&sock, "add_order", 2).await;
        third.abort();
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn futures_in_flight_slots_free_on_the_deadline_and_on_drop() {
        let w = wire_client_core_limited(
            "seeded-token-UPC",
            unused_refresh_envelope(),
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            Some(100),
            true,
            false,
            Some(1),
        );
        let held = tokio::spawn(futures_placement(&w, FUTURES_ID));
        let sock = unified_private_socket(&w).await;
        wait_for_method_frames(&sock, "add_order", 1).await;
        tokio::time::timeout(BUDGET, held)
            .await
            .expect("timeout: the unanswered placement never hit its deadline")
            .expect("task panicked")
            .expect_err("an unanswered placement fails at the deadline");

        let dropped = tokio::spawn(futures_placement(&w, FUTURES_ID_2));
        wait_for_method_frames(&sock, "add_order", 2).await;
        dropped.abort();
        let _ = dropped.await;
        let next = tokio::spawn(futures_placement(&w, FUTURES_ID));
        wait_for_method_frames(&sock, "add_order", 3).await;
        next.abort();
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn the_futures_in_flight_limit_never_refuses_a_spot_order() {
        let w = wire_client_core_limited(
            "seeded-token-UPC",
            unused_refresh_envelope(),
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            Some(60_000),
            true,
            false,
            Some(1),
        );
        let held = tokio::spawn(futures_placement(&w, FUTURES_ID));
        let futures_sock = unified_private_socket(&w).await;
        wait_for_method_frames(&futures_sock, "add_order", 1).await;

        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id =
            Some(crate::types::ClOrdId::new("c0000000-0000-4000-8000-000000000009").unwrap());
        let spot_order = w.trade.order(req);
        let spot = tokio::spawn(async move { spot_order.await });
        let auth_sock = socket_with_at_least(&w.factory, WsUrl::Auth, 0).await;
        wait_for_method_frames(&auth_sock, "add_order", 1).await;
        assert!(!spot.is_finished(), "a Spot order is sent, not refused");
        spot.abort();
        held.abort();
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_bare_unified_private_order_refused_with_an_auth_string_still_opens_the_socket() {
        for error in [
            "Invalid key",
            "Permission denied",
            "EGeneral:Permission denied",
        ] {
            let w = unified_wired_client(unused_refresh_envelope());
            reject_bare_unified_private_order(&w, error).await;
            tokio::time::timeout(BUDGET, async {
                while w.supervisor.current_state(WsUrl::UnifiedPrivate) != ConnectionState::Open {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("{error}: a refused order must leave the socket Open"));
            assert!(
                w.ws.ensure_order_sendable(WsUrl::UnifiedPrivate)
                    .await
                    .is_ok(),
                "{error}: the next order is admitted on the same connection"
            );
            w.reactor.abort();
            w.bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn a_unified_private_order_whose_token_cannot_be_minted_fails_authentication() {
        use crate::api::trade::ws_conn_error_to_trade;
        use crate::error::ApiError;
        let w = wire_client_core(
            "seeded",
            serde_json::json!({ "error": ["EAPI:Invalid key"], "result": {} }),
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            None,
            true,
            false,
        );
        w.auth_stack.invalidate_cached_token();
        let first = tokio::time::timeout(BUDGET, w.ws.ensure_order_sendable(WsUrl::UnifiedPrivate))
            .await
            .expect("timeout: the failed bring-up never resolved")
            .expect_err("a key that cannot mint a token cannot send");
        tokio::time::timeout(BUDGET, async {
            while w.supervisor.current_state(WsUrl::UnifiedPrivate) != ConnectionState::Failed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: the connection never failed");
        let later =
            w.ws.ensure_order_sendable(WsUrl::UnifiedPrivate)
                .await
                .expect_err("the failed connection stays failed");
        let composed =
            w.ws.send_request_costed(
                WsUrl::UnifiedPrivate,
                "add_order".to_string(),
                w.ws.next_req_id(),
                serde_json::json!({ "symbol": "PF_XBTUSD" }),
                crate::rest::RateLimitCost::None,
            )
            .expect("untracked cost is never rate limited")
            .recv()
            .await
            .expect_err("a failed connection refuses a request already queued");
        for (when, e) in [
            ("first call", first),
            ("later call", later),
            ("compose gate", composed),
        ] {
            let e = ws_conn_error_to_trade(e);
            assert_eq!(e.code(), "AUTHENTICATION_FAILED", "{when}: {e:?}");
            assert!(!e.retryable(), "{when}: a rejected key never recovers");
            assert!(
                e.message() == "Authentication failed: Invalid API key.",
                "{when}: the venue's reason reaches the caller: {}",
                e.message()
            );
        }
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn an_open_unified_private_order_ack_token_stale_refreshes_without_teardown() {
        let w = unified_wired_client(token_envelope(""));
        assert_cold_unified_private_order_self_auths(&w).await;
        let sock = w
            .factory
            .handle_for(WsUrl::UnifiedPrivate)
            .expect("Unified private socket");
        let frames_before = sock.sent_text().len();

        let ws = Arc::clone(&w.ws);
        let req_id = ws.next_req_id();
        let order = tokio::spawn(async move {
            ws.send_request_costed(
                WsUrl::UnifiedPrivate,
                "add_order".to_string(),
                req_id,
                serde_json::json!({ "symbol": "PF_XBTUSD" }),
                crate::rest::RateLimitCost::None,
            )
            .expect("untracked cost is never rate limited")
            .recv()
            .await
        });
        wait_for_sent_req(&sock, req_id).await;
        assert!(w.auth_stack.cached_token().is_some());
        sock.emit_text(
            serde_json::json!({
                "method": "add_order", "req_id": req_id, "success": false,
                "error": "Invalid session",
            })
            .to_string(),
        );
        let resp = tokio::time::timeout(BUDGET, order)
            .await
            .expect("timeout: the stale order never resolved")
            .expect("task panicked")
            .expect("the venue's token rejection is delivered, not drained");
        assert_eq!(
            (resp.success, resp.error.as_deref()),
            (false, Some("Invalid session"))
        );
        assert!(
            w.auth_stack.cached_token().is_none(),
            "the stale token is invalidated and the refresh keeps it empty"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            w.supervisor.current_state(WsUrl::UnifiedPrivate),
            ConnectionState::Open,
            "a stale token keeps the session and its subscriptions"
        );
        assert_eq!(
            sock.sent_text().len(),
            frames_before + 1,
            "the stale order is never resent"
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_spot_auth_subscription_does_not_hold_back_a_cold_unified_private_order() {
        let w = unified_wired_client(unused_refresh_envelope());
        register_executions(&w.bus);
        tokio::time::timeout(BUDGET, async {
            while !w.supervisor.order_gate().has_subscriptions(WsUrl::Auth) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: the Spot subscription never reached the gate");
        assert!(
            !w.supervisor
                .order_gate()
                .has_subscriptions(WsUrl::UnifiedPrivate)
        );

        assert_cold_unified_private_order_self_auths(&w).await;
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_pc1_bare_order_self_auth_success() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-PC1");
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Idle,
            "fresh bare-order client: auth WS Idle before the order"
        );

        let cl = "11111111-1111-4111-8111-111111111111";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "bare-order authenticating",
        )
        .await;

        let sent = wait_for_sent_add_order(&sock).await;
        assert_eq!(
            sent["method"], "add_order",
            "the probe frame is the order itself"
        );
        assert_eq!(
            sent["params"]["token"], "seeded-token-PC1",
            "reactor injected the cached token into the probe order params (A277)"
        );
        assert_eq!(sent["params"]["symbol"], "BTC/USDC");
        assert_eq!(sent["params"]["cl_ord_id"], cl);
        let req_id = sent["req_id"]
            .as_u64()
            .expect("probe order has a numeric req_id");
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Authenticating,
            "FSM stays Authenticating until the order ACK authenticates"
        );

        emit_add_order_ok(&sock, req_id, cl);

        wait_for_state(
            &auth_mirror,
            ConnectionState::Open,
            "bare-order self-auth → Open",
        )
        .await;
        let resp = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: bare-order future never resolved")
            .expect("task panicked")
            .expect("bare order resolves Ok");
        assert_eq!(
            resp.txid.as_ref().map(|t| t.as_str()),
            Some("OAUTO-CONNECT-1"),
            "decoded WS add_order order_id → txid"
        );
        assert_eq!(
            resp.cl_ord_id.as_ref().map(|c| c.as_str()),
            Some(cl),
            "cl_ord_id echoed"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_pc2_order_rejection_still_authenticates() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-PC2");
        let cl = "22222222-2222-4222-8222-222222222222";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "pc2 authenticating",
        )
        .await;
        let sent = wait_for_sent_add_order(&sock).await;
        let req_id = sent["req_id"].as_u64().expect("req_id");

        sock.emit_text(
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "success": false,
                "error": "EOrder:Insufficient funds",
            })
            .to_string(),
        );

        wait_for_state(
            &auth_mirror,
            ConnectionState::Open,
            "pc2 authenticates despite order reject",
        )
        .await;
        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: pc2 order future never resolved")
            .expect("task panicked");
        let err = res.expect_err("order-level rejection must resolve Err");
        assert!(
            matches!(err, TradeError::InsufficientFunds { .. }),
            "EOrder:Insufficient funds → TradeError::InsufficientFunds; got {err:?}"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_failed_single_order_reply_with_an_array_result_still_resolves_err() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-PC2A");
        let order_fut = tokio::spawn(async move {
            trade
                .order(buy_limit("BTC/USDC", "0.0001", "33512.0"))
                .await
        });

        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "array-result reject authenticating",
        )
        .await;
        let sent = wait_for_sent_add_order(&sock).await;
        let req_id = sent["req_id"].as_u64().expect("req_id");
        sock.emit_text(
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "success": false,
                "error": "EOrder:Insufficient funds",
                "result": [],
            })
            .to_string(),
        );

        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: order future never resolved")
            .expect("task panicked");
        let err = res.expect_err("a success:false reply must never resolve Ok");
        assert!(
            matches!(err, TradeError::InsufficientFunds { .. }),
            "array-result rejection still classifies; got {err:?}"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_pc3_auth_rejection_does_not_authenticate() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-PC3");
        let cl = "33333333-3333-4333-8333-333333333333";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "pc3 authenticating",
        )
        .await;
        let sent = wait_for_sent_add_order(&sock).await;
        let req_id = sent["req_id"].as_u64().expect("req_id");

        sock.emit_text(
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "success": false,
                "error": "EAPI:Invalid key",
            })
            .to_string(),
        );

        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: pc3 order future never resolved")
            .expect("task panicked");
        let _err = res.expect_err("auth-class rejection must resolve the order Err");
        let st = ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap();
        assert_ne!(
            st,
            ConnectionState::Open,
            "auth-class rejection must NOT reach Open; was {st:?}"
        );
        let settle = async {
            loop {
                let s = ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap();
                assert_ne!(
                    s,
                    ConnectionState::Open,
                    "must never reach Open after auth-reject"
                );
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(100), settle).await;

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_pc4_with_subs_path_unchanged() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-PC4");
        register_executions(&bus);

        let cl = "44444444-4444-4444-8444-444444444444";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "pc4 authenticating",
        )
        .await;
        wait_for_first_subscribe(&sock).await;
        let first = {
            let bodies = sock.sent_text();
            serde_json::from_str::<serde_json::Value>(&bodies[0]).expect("first frame JSON")
        };
        assert_eq!(
            first["method"], "subscribe",
            "with-subs path: the FIRST frame is the executions subscribe (the probe), NOT the order"
        );
        assert_eq!(first["params"]["channel"], "executions");
        let no_order_during_auth = !sock.sent_text().iter().any(|b| {
            serde_json::from_str::<serde_json::Value>(b)
                .ok()
                .and_then(|v| {
                    v.get("method")
                        .and_then(|m| m.as_str())
                        .map(|s| s.to_string())
                })
                .as_deref()
                == Some("add_order")
        });
        assert!(
            no_order_during_auth,
            "with-subs path: NO add_order may be sent during Authenticating (it waits for Open)"
        );

        sock.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "pc4 with-subs open").await;
        let req_id = wait_for_add_order_req(&sock).await;
        emit_add_order_ok(&sock, req_id, cl);

        let resp = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: pc4 order future never resolved")
            .expect("task panicked")
            .expect("with-subs order resolves Ok");
        assert_eq!(
            resp.txid.as_ref().map(|t| t.as_str()),
            Some("OAUTO-CONNECT-1")
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_ws_order_dropped_in_flight_is_sent_ambiguous_but_a_cancel_stays_retryable() {
        use crate::api::trade::OrderAmendRequest;
        use crate::error::ApiError;
        use crate::transport::TransportErrorKind;
        #[derive(Clone, Copy)]
        enum Call {
            Add,
            Amend,
            Cancel,
        }
        // (call, wire method, sent-ambiguous, ambiguity events)
        let cases = [
            (Call::Add, "add_order", true, 1),
            (Call::Amend, "amend_order", true, 1),
            (Call::Cancel, "cancel_order", false, 0),
        ];
        for (call, method, ambiguous_drop, events) in cases {
            let (trade, factory, bus, auth_mirror, _auth_stack, reactor) =
                wire_client_with_refresh("seeded-token-DROP", token_envelope(""));
            let ambiguous = count_events(
                &bus,
                crate::dispatch::EventType::OrderPlacementAmbiguousEvent,
            );
            register_executions(&bus);
            let id = crate::types::ClOrdId::new("d0000000-0000-4000-8000-000000000001").unwrap();
            let task = tokio::spawn(async move {
                match call {
                    Call::Add => {
                        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
                        req.cl_ord_id = Some(id);
                        trade.order(req).await.map(|_| ())
                    }
                    Call::Amend => trade
                        .order_amend(
                            OrderAmendRequest::new(id).order_volume(rust_decimal::Decimal::ONE),
                        )
                        .await
                        .map(|_| ()),
                    Call::Cancel => trade.cancel(id).await.map(|_| ()),
                }
            });
            let sock = drive_handshake_to_open(&factory, &auth_mirror).await;
            next_unseen_frame(&sock, method, &mut std::collections::HashSet::new()).await;
            sock.drop_with(crate::transport::TransportError {
                kind: TransportErrorKind::AbnormalClose {
                    context: "dropped with the order in flight".into(),
                },
                transient: true,
            });
            let err = tokio::time::timeout(BUDGET, task)
                .await
                .expect("timeout: the dropped call never resolved")
                .expect("task panicked")
                .expect_err("a drop in flight fails the call");
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                (
                    matches!(
                        err,
                        TradeError::Transport {
                            kind: TransportErrorKind::RequestSentNoResponse,
                            ..
                        }
                    ),
                    err.retryable(),
                    ambiguous.load(Ordering::Relaxed)
                ),
                (ambiguous_drop, !ambiguous_drop, events),
                "{method}: {err:?}"
            );
            reactor.abort();
            bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn a_spot_ws_op_without_a_valid_session_token_is_a_definite_session_expired() {
        use crate::api::trade::OrderAmendRequest;
        use crate::error::{ApiError, ErrorCategory};
        #[derive(Clone, Copy)]
        enum Call {
            Add,
            Amend,
            Cancel,
            CancelAll,
        }
        for call in [Call::Add, Call::Amend, Call::Cancel, Call::CancelAll] {
            let (trade, factory, bus, auth_mirror, auth_stack, reactor) =
                wire_client_with_refresh("seeded-token-NOTOKEN", token_envelope(""));
            let ambiguous = count_events(
                &bus,
                crate::dispatch::EventType::OrderPlacementAmbiguousEvent,
            );
            let failed = count_events(&bus, crate::dispatch::EventType::WsRequestFailedEvent);
            let submitted = count_events(&bus, crate::dispatch::EventType::OrderSubmittedEvent);
            let cancel_attempted =
                count_events(&bus, crate::dispatch::EventType::OrderCancellationAttempted);
            register_executions(&bus);
            let sock = drive_handshake_to_open(&factory, &auth_mirror).await;
            let frames_after_open = sock.sent_text().len();
            auth_stack.token_lifecycle().clear_cached_token_for_test();
            let id = crate::types::ClOrdId::new("e1000000-0000-4000-8000-000000000001").unwrap();
            let err = tokio::time::timeout(BUDGET, async move {
                match call {
                    Call::Add => {
                        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
                        req.cl_ord_id = Some(id);
                        trade.order(req).await.map(|_| ())
                    }
                    Call::Amend => trade
                        .order_amend(
                            OrderAmendRequest::new(id).order_volume(rust_decimal::Decimal::ONE),
                        )
                        .await
                        .map(|_| ()),
                    Call::Cancel => trade.cancel(id).await.map(|_| ()),
                    Call::CancelAll => trade.cancel_all().await.map(|_| ()),
                }
            })
            .await
            .expect("timeout: the call without a token never resolved")
            .expect_err("no valid token fails the call");
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                (err.code(), err.category(), err.retryable()),
                ("SESSION_EXPIRED", ErrorCategory::Auth, true),
                "{err:?}"
            );
            assert_eq!(
                (
                    ambiguous.load(Ordering::Relaxed),
                    failed.load(Ordering::Relaxed),
                    submitted.load(Ordering::Relaxed),
                    cancel_attempted.load(Ordering::Relaxed),
                    sock.sent_text().len()
                ),
                (0, 0, 0, 0, frames_after_open),
                "a request refused before it is sent fires no event and writes no frame"
            );
            reactor.abort();
            bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn a_futures_ws_op_without_a_valid_session_token_is_a_definite_session_expired() {
        use crate::error::{ApiError, ErrorCategory};
        let ops = [
            "order",
            "amend",
            "cancel",
            "batch cancel",
            "cancel all",
            "dead-man arm",
            "batch order",
        ];
        let w = unified_wired_client(token_envelope(""));
        let ambiguous = count_events(
            &w.bus,
            crate::dispatch::EventType::OrderPlacementAmbiguousEvent,
        );
        let failed = count_events(&w.bus, crate::dispatch::EventType::WsRequestFailedEvent);
        let cases = futures_cases();
        let opener = cases
            .iter()
            .find(|case| case.label == "order")
            .expect("an order case opens the socket");
        let mut driver = FuturesDriver::default();
        driver
            .run(&w, opener)
            .await
            .expect("the first order opens the socket");
        tokio::time::timeout(BUDGET, async {
            while w.supervisor.current_state(WsUrl::UnifiedPrivate) != ConnectionState::Open {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: the order ack never opened the Unified private socket");
        let sock = driver.socket.as_ref().expect("socket opened above");
        let frames_after_open = sock.sent_text().len();
        let submitted = count_events(&w.bus, crate::dispatch::EventType::OrderSubmittedEvent);
        let cancel_attempted = count_events(
            &w.bus,
            crate::dispatch::EventType::OrderCancellationAttempted,
        );
        w.auth_stack.token_lifecycle().clear_cached_token_for_test();
        for case in cases.iter().filter(|case| ops.contains(&case.label)) {
            let error = tokio::time::timeout(BUDGET, (case.call)(w.trade.futures()))
                .await
                .unwrap_or_else(|_| panic!("{}: never resolved", case.label))
                .expect_err("no valid token fails the call");
            assert_eq!(
                (error.code(), error.category(), error.retryable()),
                ("SESSION_EXPIRED", ErrorCategory::Auth, true),
                "{}: {error:?}",
                case.label
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            (
                ambiguous.load(Ordering::Relaxed),
                failed.load(Ordering::Relaxed),
                submitted.load(Ordering::Relaxed),
                cancel_attempted.load(Ordering::Relaxed),
                sock.sent_text().len()
            ),
            (0, 0, 0, 0, frames_after_open),
            "a request refused before it is sent fires no event and writes no frame"
        );
        w.reactor.abort();
        w.bus.stop_reactors();
    }

    #[tokio::test]
    async fn an_answered_probe_order_survives_a_writer_closed_during_the_subscribe_replay() {
        use crate::transport::driveable_mock::SendFailMode;
        for accepted in [true, false] {
            let (trade, factory, bus, auth_mirror, _auth_stack, reactor) =
                wire_client_with_refresh("seeded-token-REPLAY", token_envelope(""));
            let ambiguous = count_events(
                &bus,
                crate::dispatch::EventType::OrderPlacementAmbiguousEvent,
            );
            let failed = count_events(&bus, crate::dispatch::EventType::WsRequestFailedEvent);
            let cl = "5c000000-0000-4000-8000-000000000001";
            let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
            req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
            let order = tokio::spawn(async move { trade.order(req).await });
            let sock = socket_at(&factory, 0).await;
            wait_for_state(&auth_mirror, ConnectionState::Authenticating, "probe").await;
            let req_id = wait_for_sent_add_order(&sock).await["req_id"]
                .as_u64()
                .expect("req_id");
            register_executions(&bus);
            tokio::time::sleep(Duration::from_millis(50)).await;
            sock.set_send_fail_mode(SendFailMode::WriterClosed);
            if accepted {
                emit_add_order_ok(&sock, req_id, cl);
            } else {
                sock.emit_text(
                    serde_json::json!({
                        "method": "add_order", "req_id": req_id, "success": false,
                        "error": "EOrder:Insufficient funds",
                    })
                    .to_string(),
                );
            }
            let result = tokio::time::timeout(BUDGET, order)
                .await
                .expect("timeout: the answered probe order never resolved")
                .expect("task panicked");
            wait_for_state(
                &auth_mirror,
                ConnectionState::BackingOff,
                "the replay's WriterClosed tears the connection down",
            )
            .await;
            match (&result, accepted) {
                (Ok(resp), true) => assert_eq!(
                    resp.txid.as_ref().map(|t| t.as_str()),
                    Some("OAUTO-CONNECT-1")
                ),
                (Err(TradeError::InsufficientFunds { .. }), false) => {}
                (other, _) => panic!("accepted={accepted}: unexpected result {other:?}"),
            }
            assert_eq!(
                (
                    ambiguous.load(Ordering::Relaxed),
                    failed.load(Ordering::Relaxed)
                ),
                (0, 0),
                "accepted={accepted}: the venue answered, so neither event fires"
            );
            reactor.abort();
            bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn an_auth_class_reply_to_the_probe_order_is_a_definite_non_retryable_error() {
        use crate::error::ApiError;
        for (error, code) in [
            ("EAPI:Invalid key", "UNKNOWN"),
            ("EGeneral:Permission denied", "PERMISSION_DENIED"),
        ] {
            let (trade, factory, bus, auth_mirror, _auth_stack, reactor) =
                wire_client_with_refresh("seeded-token-PROBE-AUTH", token_envelope(""));
            let ambiguous = count_events(
                &bus,
                crate::dispatch::EventType::OrderPlacementAmbiguousEvent,
            );
            let failed = count_events(&bus, crate::dispatch::EventType::WsRequestFailedEvent);
            let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
            req.cl_ord_id =
                Some(crate::types::ClOrdId::new("5a000000-0000-4000-8000-000000000001").unwrap());
            let order = tokio::spawn(async move { trade.order(req).await });
            let sock = socket_at(&factory, 0).await;
            wait_for_state(&auth_mirror, ConnectionState::Authenticating, "probe").await;
            let req_id = wait_for_sent_add_order(&sock).await["req_id"]
                .as_u64()
                .expect("req_id");
            sock.emit_text(
                serde_json::json!({
                    "method": "add_order", "req_id": req_id, "success": false, "error": error,
                })
                .to_string(),
            );
            let err = tokio::time::timeout(BUDGET, order)
                .await
                .expect("timeout: the refused probe order never resolved")
                .expect("task panicked")
                .expect_err("a credential refusal fails the order");
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                err.code() == code && !err.retryable(),
                "{error}: a definite, non-retryable refusal; got {err:?}"
            );
            assert_eq!(
                (
                    ambiguous.load(Ordering::Relaxed),
                    failed.load(Ordering::Relaxed)
                ),
                (0, 0),
                "{error}: the venue answered, so neither event fires"
            );
            reactor.abort();
            bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn test_pc5_token_stale_during_probe_resolves_session_expired_no_resend() {
        let (trade, factory, bus, auth_mirror, auth_stack, reactor) =
            wire_client_with_refresh("seeded-token-PC5", token_envelope(""));
        let ambiguous = count_events(
            &bus,
            crate::dispatch::EventType::OrderPlacementAmbiguousEvent,
        );
        let submitted = capture_submitted_statuses(&bus);
        let failed = Arc::new(std::sync::Mutex::new(Vec::new()));
        {
            let failed = Arc::clone(&failed);
            let _ = bus.subscribe(
                crate::dispatch::EventType::WsRequestFailedEvent,
                Arc::new(move |env: &crate::dispatch::EventEnvelope| {
                    if let crate::dispatch::EventPayload::WsRequestFailedEvent {
                        req_id, url, ..
                    } = &env.payload
                    {
                        failed.lock().unwrap().push((*req_id, *url));
                    }
                }),
                1,
            );
        }
        let cl = "55555555-5555-4555-8555-555555555555";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "pc5 authenticating",
        )
        .await;
        let sent = wait_for_sent_add_order(&sock).await;
        let req_id = sent["req_id"].as_u64().expect("req_id");
        let order_frames_before = sock
            .sent_text()
            .iter()
            .filter(|b| {
                serde_json::from_str::<serde_json::Value>(b)
                    .ok()
                    .and_then(|v| {
                        v.get("method")
                            .and_then(|m| m.as_str())
                            .map(|s| s.to_string())
                    })
                    .as_deref()
                    == Some("add_order")
            })
            .count();
        assert_eq!(
            order_frames_before, 1,
            "exactly one probe order sent before the stale ack"
        );

        sock.emit_text(
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "success": false,
                "error": "EAPI:Invalid token",
            })
            .to_string(),
        );

        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: pc5 order future never resolved")
            .expect("task panicked");
        let err = res.expect_err("token-stale probe must resolve the order Err");
        assert!(
            matches!(err, TradeError::SessionExpired { .. }),
            "a token-stale probe ack is a definite rejection: SessionExpired; got {err:?}"
        );

        let settle = async {
            loop {
                let order_count = sock
                    .sent_text()
                    .iter()
                    .filter(|b| {
                        serde_json::from_str::<serde_json::Value>(b)
                            .ok()
                            .and_then(|v| {
                                v.get("method")
                                    .and_then(|m| m.as_str())
                                    .map(|s| s.to_string())
                            })
                            .as_deref()
                            == Some("add_order")
                    })
                    .count();
                assert_eq!(
                    order_count, 1,
                    "token-stale probe must NOT auto-resend the order (never-auto-retry §11.7)"
                );
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(150), settle).await;
        assert!(
            auth_stack.cached_token().is_none(),
            "the rejected token is invalidated and refreshed, not reused"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            failed.lock().unwrap().is_empty(),
            "a definite rejection drains nothing, so no WsRequestFailedEvent"
        );
        assert_eq!(
            ambiguous.load(Ordering::Relaxed),
            0,
            "a definite rejection fires no OrderPlacementAmbiguousEvent"
        );
        assert_eq!(
            *submitted.lock().unwrap(),
            vec![session_expired_status()],
            "the venue's answer is reported as OrderSubmittedEvent WireError SESSION_EXPIRED"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[allow(clippy::type_complexity)]
    fn wire_client_keep_supervisor(
        seeded_token: &str,
    ) -> (
        TradeNamespace,
        Arc<DriveableWsSocketFactory>,
        Arc<DispatchEventBus>,
        Arc<AtomicU8>,
        Arc<ConnectionSupervisor>,
        tokio::task::JoinHandle<()>,
    ) {
        let w = wire_client_core(
            seeded_token,
            unused_refresh_envelope(),
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            None,
            false,
            false,
        );
        (
            w.trade,
            w.factory,
            w.bus,
            w.auth_mirror,
            w.supervisor,
            w.reactor,
        )
    }

    fn deregister_executions(bus: &Arc<DispatchEventBus>) {
        use crate::dispatch::CallerInbound;
        use crate::dispatch::event_bus::RegistryMutationOp;
        use crate::types::ChannelName;
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::Deregister {
                channel: ChannelName::Executions,
                pair: None,
            },
        })
        .expect("post executions deregister");
    }

    async fn wait_for_flag(sup: &Arc<ConnectionSupervisor>, target: bool, what: &str) {
        let poll = async {
            loop {
                if sup.order_gate().has_subscriptions(WsUrl::Auth) == target {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: auth_has_subscriptions != {target} ({what})"));
    }

    #[tokio::test]
    async fn test_fix1_flag_resets_on_unsub_then_bare_order_reaches_probe() {
        let (trade, factory, bus, auth_mirror, supervisor, reactor) =
            wire_client_keep_supervisor("seeded-token-FIX1");

        assert!(
            !supervisor.order_gate().has_subscriptions(WsUrl::Auth),
            "fresh client: auth_has_subscriptions starts false"
        );

        register_executions(&bus);
        wait_for_flag(&supervisor, true, "after auth subscribe").await;

        deregister_executions(&bus);
        wait_for_flag(&supervisor, false, "after auth unsubscribe (BUG#5 reset)").await;

        let cl = "f1000000-0000-4000-8000-000000000001";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let (sock, req_id) = {
            let poll = async {
                loop {
                    for i in 0..factory.created_count() {
                        if let Some(h) = factory.handle(i) {
                            for body in h.sent_text() {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body)
                                    && v.get("method").and_then(serde_json::Value::as_str)
                                        == Some("add_order")
                                {
                                    let rid = v
                                        .get("req_id")
                                        .and_then(serde_json::Value::as_u64)
                                        .expect("req_id");
                                    return (h, rid);
                                }
                            }
                        }
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: bare order never reached the probe (BUG#5 stale-flag hang)")
        };

        emit_add_order_ok(&sock, req_id, cl);
        wait_for_state(
            &auth_mirror,
            ConnectionState::Open,
            "fix1 bare-order → Open",
        )
        .await;
        let resp = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: fix1 order future never resolved")
            .expect("task panicked")
            .expect("bare order after unsub resolves Ok");
        assert_eq!(
            resp.cl_ord_id.as_ref().map(|c| c.as_str()),
            Some(cl),
            "cl_ord_id echoed"
        );

        std::mem::forget(supervisor);
        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_fix3_mcap_during_probe_resolves_session_expired_not_raw_ack() {
        let (trade, factory, bus, auth_mirror, supervisor, reactor) =
            wire_client_keep_supervisor("seeded-token-FIX3");
        let trade = Arc::new(trade);

        const CAP: u32 = 3; // mirrors MAX_CONSECUTIVE_HANDSHAKE_FAILS (private const)
        let mut last_err: Option<TradeError> = None;
        let mut acked: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for attempt in 1..=CAP {
            let cl = format!("f3000000-0000-4000-8000-00000000000{attempt}");
            let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
            req.cl_ord_id = Some(crate::types::ClOrdId::new(&cl).unwrap());
            let trade_c = Arc::clone(&trade);
            let order_fut = tokio::spawn(async move { trade_c.order(req).await });

            let (sock, req_id) = {
                let poll = async {
                    loop {
                        let mut best: Option<(DriveableSocketHandle, u64)> = None;
                        for i in 0..factory.created_count() {
                            if let Some(h) = factory.handle(i) {
                                for body in h.sent_text() {
                                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body)
                                        && v.get("method").and_then(serde_json::Value::as_str)
                                            == Some("add_order")
                                    {
                                        let rid = v
                                            .get("req_id")
                                            .and_then(serde_json::Value::as_u64)
                                            .expect("req_id");
                                        if acked.contains(&rid) {
                                            continue;
                                        }
                                        match &best {
                                            Some((_, b)) if *b <= rid => {}
                                            _ => best = Some((h.clone(), rid)),
                                        }
                                    }
                                }
                            }
                        }
                        if let Some((h, rid)) = best {
                            return (h, rid);
                        }
                        tokio::task::yield_now().await;
                    }
                };
                tokio::time::timeout(BUDGET, poll)
                    .await
                    .unwrap_or_else(|_| panic!("timeout: attempt {attempt} probe never sent"))
            };
            acked.insert(req_id);

            sock.emit_text(
                serde_json::json!({
                    "method": "add_order",
                    "req_id": req_id,
                    "success": false,
                    "error": "EAPI:Invalid token",
                })
                .to_string(),
            );

            let res = tokio::time::timeout(BUDGET, order_fut)
                .await
                .unwrap_or_else(|_| panic!("timeout: attempt {attempt} order never resolved"))
                .expect("task panicked");
            let err = res.expect_err("token-stale probe must resolve the order Err");
            assert!(
                matches!(err, TradeError::SessionExpired { .. }),
                "attempt {attempt}: token-stale must resolve SessionExpired, not the raw ack; got {err:?}"
            );
            last_err = Some(err);

            if attempt < CAP {
                wait_for_state(
                    &auth_mirror,
                    ConnectionState::Authenticating,
                    "fix3 re-enter Authenticating below cap",
                )
                .await;
            }
        }

        wait_for_state(&auth_mirror, ConnectionState::Failed, "fix3 M-cap → Failed").await;
        assert!(
            matches!(last_err, Some(TradeError::SessionExpired { .. })),
            "the M-cap (CAP-th) order resolved SessionExpired, not the raw token-stale ack"
        );

        std::mem::forget(supervisor);
        reactor.abort();
        bus.stop_reactors();
    }

    async fn wait_for_send_ready(sup: &Arc<ConnectionSupervisor>, target: bool, what: &str) {
        let poll = async {
            loop {
                if sup.order_gate().send_ready(WsUrl::Auth) == target {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: order_gate != {target} ({what})"));
    }

    #[tokio::test]
    async fn test_a342_send_ready_flag_set_on_cold_start_cleared_on_open() {
        let (trade, factory, bus, auth_mirror, supervisor, reactor) =
            wire_client_keep_supervisor("seeded-token-A342");
        assert!(
            !supervisor.order_gate().send_ready(WsUrl::Auth),
            "fresh client: send-ready flag false"
        );

        let cl = "a3420000-0000-4000-8000-000000000001";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "a342 authenticating",
        )
        .await;
        wait_for_send_ready(&supervisor, true, "a342 cold-start send-ready").await;

        let sent = wait_for_sent_add_order(&sock).await;
        let req_id = sent["req_id"].as_u64().expect("probe req_id");
        emit_add_order_ok(&sock, req_id, cl);
        wait_for_state(&auth_mirror, ConnectionState::Open, "a342 → Open").await;

        wait_for_send_ready(&supervisor, false, "a342 cleared on leaving Authenticating").await;

        let _ = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: a342 order future")
            .expect("task panicked")
            .expect("a342 bare order Ok");

        std::mem::forget(supervisor);
        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_a342_send_ready_flag_cleared_on_auth_register() {
        let (trade, factory, bus, auth_mirror, supervisor, reactor) =
            wire_client_keep_supervisor("seeded-token-A342R");

        let cl = "a342000a-0000-4000-8000-00000000000a";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let _order_fut = tokio::spawn(async move { trade.order(req).await });
        let _sock = socket_at(&factory, 0).await;
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "a342r authenticating",
        )
        .await;
        wait_for_send_ready(&supervisor, true, "a342r cold-start send-ready").await;

        register_executions(&bus);
        wait_for_flag(&supervisor, true, "a342r auth_has_subscriptions set").await;
        wait_for_send_ready(
            &supervisor,
            false,
            "a342r send-ready cleared on auth Register",
        )
        .await;

        std::mem::forget(supervisor);
        reactor.abort();
        bus.stop_reactors();
    }

    #[allow(clippy::type_complexity)]
    fn wire_client_with_tracker(
        seeded_token: &str,
        order_deadline_ms: Option<u32>,
    ) -> (
        TradeNamespace,
        Arc<DriveableWsSocketFactory>,
        Arc<DispatchEventBus>,
        Arc<AtomicU8>,
        tokio::task::JoinHandle<()>,
        Arc<crate::rate_limit::SpotTradingRateLimitTracker>,
    ) {
        let w = wire_client_core(
            seeded_token,
            unused_refresh_envelope(),
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            order_deadline_ms,
            true,
            false,
        );
        std::mem::forget(w.supervisor);
        (
            w.trade,
            w.factory,
            w.bus,
            w.auth_mirror,
            w.reactor,
            w.trading_rl,
        )
    }

    #[tokio::test]
    async fn test_ws18_buy_and_sell_arm_charge_trading_tracker_e2e() {
        use crate::api::trade::{OrderRequest, OrderType, Price, TradeError};
        use crate::rate_limit::Scope;
        use crate::types::{ApiKey, ClOrdId, MonotonicInstant, Symbol};

        for (side, seed, cl) in [
            (
                "buy",
                "seeded-token-ws18-buy",
                "18000000-0000-4000-8000-000000000001",
            ),
            (
                "sell",
                "seeded-token-ws18-sell",
                "18000000-0000-4000-8000-000000000002",
            ),
        ] {
            let (trade, factory, bus, auth_mirror, reactor, shared_tracker) =
                wire_client_with_tracker(seed, None);

            let api_key = ApiKey::new("test-api-key");
            let pair = Symbol::new("BTC/USDC").unwrap();
            let now = MonotonicInstant::now();

            for _ in 0..60 {
                shared_tracker
                    .consume(Scope::Pair(api_key.clone(), pair.clone()), 1.0, now)
                    .expect("pre-drain consume must succeed within cap");
            }

            register_executions(&bus);
            let sock = drive_handshake_to_open(&factory, &auth_mirror).await;

            let order_fut = match side {
                "buy" => {
                    let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
                    req.cl_ord_id = Some(ClOrdId::new(cl).unwrap());
                    tokio::spawn(async move { trade.order(req).await })
                }
                _ => {
                    let mut req = OrderRequest::new(
                        Symbol::new("BTC/USDC").unwrap(),
                        "0.0001".parse().unwrap(),
                        Side::Buy,
                    )
                    .order_type(OrderType::Limit);
                    req.price = Some(Price::Absolute("33512.0".parse().unwrap()));
                    req.cl_ord_id = Some(ClOrdId::new(cl).unwrap());
                    tokio::spawn(async move { trade.order(req).await })
                }
            };

            let res = tokio::time::timeout(BUDGET, order_fut)
                .await
                .unwrap_or_else(|_| {
                    panic!("timeout: {side} arm rate-limit must resolve quickly — no WsRequestFrame sent")
                })
                .expect("task panicked");
            assert!(
                matches!(res, Err(TradeError::RateLimited { .. })),
                "{side} arm with exhausted tracker must return Err(TradeError::RateLimited); got {res:?}"
            );

            let frames = sock.sent_text();
            let has_add_order = frames.iter().any(|b| {
                serde_json::from_str::<serde_json::Value>(b)
                    .ok()
                    .and_then(|v| {
                        v.get("method")
                            .and_then(serde_json::Value::as_str)
                            .map(|s| s == "add_order")
                    })
                    .unwrap_or(false)
            });
            assert!(
                !has_add_order,
                "{side}: no add_order WsRequestFrame must be posted when the tracker is exhausted (reject-before-send)"
            );

            reactor.abort();
            bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn test_ws_order_deadline_fires_sent_ambiguous_when_server_silent() {
        use crate::api::trade::TradeError;
        use crate::error::ApiError;

        let (trade, factory, bus, auth_mirror, reactor, _tracker) =
            wire_client_with_tracker("seeded-token-ws-deadline", Some(300));

        let ambiguous = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ambiguous_rid: Arc<std::sync::Mutex<Option<Option<String>>>> =
            Arc::new(std::sync::Mutex::new(None));
        {
            let a = Arc::clone(&ambiguous);
            let rid_slot = Arc::clone(&ambiguous_rid);
            let _ = bus.subscribe(
                crate::dispatch::EventType::OrderPlacementAmbiguousEvent,
                Arc::new(move |env: &crate::dispatch::EventEnvelope| {
                    if let crate::dispatch::EventPayload::OrderPlacementAmbiguousEvent {
                        ref request_id,
                        ..
                    } = env.payload
                    {
                        *rid_slot.lock().unwrap() = Some(request_id.clone());
                    }
                    a.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }),
                1,
            );
        }

        register_executions(&bus);
        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;
        let frames_before = sock.sent_text().len();

        let cl = "19000000-0000-4000-8000-000000000001";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("order must resolve via the per-request deadline — no hang")
            .expect("order task panicked");

        assert_eq!(
            sock.sent_text().len(),
            frames_before + 1,
            "the add_order frame must have been sent before the deadline fired"
        );

        let err = res.expect_err("silent server + deadline must resolve Err, not Ok");
        assert!(
            matches!(
                err,
                TradeError::Transport {
                    transient: true,
                    ..
                }
            ),
            "expected a sent-ambiguous Transport error, got {err:?}"
        );
        assert!(
            !err.retryable(),
            "a per-request-deadline timeout is SENT-ambiguous → retryable() must be false"
        );
        let req_id = wait_for_add_order_req(&sock).await;
        assert_eq!(
            err.request_id(),
            Some(req_id.to_string().as_str()),
            "WS order error carries the stringified wire req_id"
        );

        let saw = async {
            loop {
                if ambiguous.load(std::sync::atomic::Ordering::Relaxed) >= 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(Duration::from_millis(500), saw)
            .await
            .expect("deadline timeout must emit OrderPlacementAmbiguousEvent");
        assert_eq!(
            ambiguous_rid.lock().unwrap().clone().flatten().as_deref(),
            Some(req_id.to_string().as_str()),
            "ambiguous payload carries the same wire req_id"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_ws_order_deadline_abandons_pending_no_spurious_failed_event() {
        use crate::api::trade::TradeError;
        use std::sync::atomic::{AtomicU64, Ordering};

        let failed = Arc::new(AtomicU64::new(0));
        let (trade, factory, bus, auth_mirror, reactor, _t) =
            wire_client_with_tracker("seeded-token-ws-abandon", Some(200));
        {
            let f = Arc::clone(&failed);
            let _ = bus.subscribe(
                crate::dispatch::EventType::WsRequestFailedEvent,
                Arc::new(move |_env: &crate::dispatch::EventEnvelope| {
                    f.fetch_add(1, Ordering::Relaxed);
                }),
                1,
            );
        }
        register_executions(&bus);
        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;

        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id =
            Some(crate::types::ClOrdId::new("1a000000-0000-4000-8000-000000000001").unwrap());
        let res = tokio::time::timeout(BUDGET, tokio::spawn(async move { trade.order(req).await }))
            .await
            .expect("no hang")
            .expect("no panic");
        assert!(
            matches!(res, Err(TradeError::Transport { .. })),
            "sent-ambiguous, got {res:?}"
        );

        tokio::time::sleep(Duration::from_millis(150)).await;
        sock.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::AbnormalClose {
                context: "abandon test".into(),
            },
            transient: true,
        });
        tokio::time::sleep(Duration::from_millis(150)).await;

        assert_eq!(
            failed.load(Ordering::Relaxed),
            0,
            "abandoned order must not emit a spurious WsRequestFailedEvent on drain"
        );
        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_ws_order_with_deadline_resolves_ok_on_timely_reply() {
        let (trade, factory, bus, auth_mirror, reactor, _t) =
            wire_client_with_tracker("seeded-token-ws-deadline-ok", Some(5_000));
        register_executions(&bus);
        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;

        let cl = "1b000000-0000-4000-8000-000000000001";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let poll = async {
            loop {
                for body in sock.sent_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body)
                        && v.get("method").and_then(serde_json::Value::as_str) == Some("add_order")
                    {
                        return v["req_id"].as_u64().unwrap();
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        let req_id = tokio::time::timeout(BUDGET, poll)
            .await
            .expect("add_order frame sent");
        sock.emit_text(
            serde_json::json!({
                "method": "add_order",
                "req_id": req_id,
                "result": { "cl_ord_id": cl, "order_id": "OK-123" },
                "success": true,
                "time_in": "2026-06-03T00:00:00.000000Z",
                "time_out": "2026-06-03T00:00:00.001000Z",
            })
            .to_string(),
        );

        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("no hang")
            .expect("no panic");
        assert!(
            res.is_ok(),
            "a timely reply within the deadline must resolve Ok, got {res:?}"
        );
        reactor.abort();
        bus.stop_reactors();
    }

    #[allow(clippy::type_complexity)]
    fn wire_client_with_tracker_and_index(
        seeded_token: &str,
    ) -> (
        TradeNamespace,
        Arc<DriveableWsSocketFactory>,
        Arc<DispatchEventBus>,
        Arc<AtomicU8>,
        tokio::task::JoinHandle<()>,
        Arc<crate::rate_limit::SpotTradingRateLimitTracker>,
        Arc<crate::rate_limit::ClOrdIdPairIndex>,
    ) {
        let w = wire_client_core(
            seeded_token,
            unused_refresh_envelope(),
            Arc::new(ClosureHttpMock::no_rest()) as Arc<dyn HttpTransport>,
            None,
            false,
            true,
        );
        std::mem::forget(w.supervisor);
        let index = w.index.expect("with_index=true wires a ClOrdIdPairIndex");
        (
            w.trade,
            w.factory,
            w.bus,
            w.auth_mirror,
            w.reactor,
            w.trading_rl,
            index,
        )
    }

    #[tokio::test]
    async fn test_ws20a_cancel_ws_path_hit_rejects_when_at_cap() {
        use crate::api::trade::TradeError;
        use crate::dispatch::Transport;
        use crate::rate_limit::Scope;
        use crate::types::{ApiKey, ClOrdId, MonotonicInstant, Symbol};

        let (trade, factory, bus, auth_mirror, reactor, shared_tracker, shared_index) =
            wire_client_with_tracker_and_index("seeded-token-ws20a-cancel");

        let api_key = ApiKey::new("test-api-key");
        let pair = Symbol::new("BTC/USDC").unwrap();
        let cl = ClOrdId::new("20a00000-0000-4000-8000-000000000001").unwrap();

        let now = MonotonicInstant::now();
        shared_index.insert(cl.clone(), pair.clone(), now);

        for _ in 0..53 {
            shared_tracker
                .consume(Scope::Pair(api_key.clone(), pair.clone()), 1.0, now)
                .expect("pre-drain consume must succeed within cap");
        }

        register_executions(&bus);
        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;

        let trade_arc = std::sync::Arc::new(trade);
        let trade_clone = Arc::clone(&trade_arc);
        let cl_clone = cl.clone();
        let cancel_fut =
            tokio::spawn(
                async move { trade_clone.cancel(cl_clone).via(Transport::WsV2Auth).await },
            );

        let res = tokio::time::timeout(BUDGET, cancel_fut)
            .await
            .expect("timeout: WS cancel rate-limit must resolve quickly — no WsRequestFrame sent")
            .expect("task panicked");
        assert!(
            matches!(res, Err(TradeError::RateLimited { .. })),
            "WS cancel with pre-drained tracker must return Err(RateLimited); got {:?}",
            res
        );

        let frames = sock.sent_text();
        let has_cancel_order = frames.iter().any(|b| {
            serde_json::from_str::<serde_json::Value>(b)
                .ok()
                .and_then(|v| {
                    v.get("method")
                        .and_then(serde_json::Value::as_str)
                        .map(|s| s == "cancel_order")
                })
                .unwrap_or(false)
        });
        assert!(
            !has_cancel_order,
            "no cancel_order WsRequestFrame must be posted when the tracker is at cap (reject-before-send)"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    async fn wait_for_order_req(sock: &DriveableSocketHandle, method: &str) -> u64 {
        let poll = async {
            loop {
                for body in sock.sent_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body)
                        && v.get("method").and_then(serde_json::Value::as_str) == Some(method)
                    {
                        return v
                            .get("req_id")
                            .and_then(serde_json::Value::as_u64)
                            .expect("req_id");
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: auth socket never sent a {method} frame"))
    }

    #[tokio::test]
    async fn ws_amend_success_emits_wire_accepted_with_server_amend_id() {
        use crate::api::trade::OrderAmendRequest;
        use crate::dispatch::{EventPayload, EventType, OrderOp, OrderSubmitStatus};

        let (trade, factory, bus, auth_mirror, reactor, _rl) =
            wire_client_with_tracker("seeded-token-ws-amend-emit", None);
        register_executions(&bus);
        // Subscribe BEFORE the order so the emit can't race the capture.
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let _ = bus.subscribe(
            EventType::OrderSubmittedEvent,
            Arc::new(move |env| {
                let _ = tx.try_send(env.payload.clone());
            }),
            1,
        );

        let cl = crate::types::ClOrdId::new("ws-amend-emit-01").unwrap();
        let mut req = OrderAmendRequest::new(cl.clone());
        req.order_volume = Some("0.75".parse().unwrap());
        let order_fut =
            tokio::spawn(async move { trade.order_amend(req).via(Transport::WsV2Auth).await });

        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;
        let req_id = wait_for_order_req(&sock, "amend_order").await;
        sock.emit_text(
            serde_json::json!({
                "method": "amend_order",
                "req_id": req_id,
                "success": true,
                "result": { "amend_id": "TA-WS-EMIT-1", "cl_ord_id": "ws-amend-emit-01" },
            })
            .to_string(),
        );

        let resp = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: amend future never resolved")
            .expect("task panicked")
            .expect("success ack must resolve the amend Ok");
        assert_eq!(resp.amend_id.as_str(), "TA-WS-EMIT-1");

        let payload = tokio::time::timeout(BUDGET, rx.recv())
            .await
            .expect("no OrderSubmittedEvent on the WS amend path")
            .expect("channel closed");
        match payload {
            EventPayload::OrderSubmittedEvent {
                cl_ord_id,
                amend_id,
                op,
                url,
                status,
                ..
            } => {
                assert_eq!(cl_ord_id, cl);
                assert_eq!(
                    amend_id.as_ref().map(|a| a.as_str()),
                    Some("TA-WS-EMIT-1"),
                    "amend success carries the server-minted amend_id"
                );
                assert_eq!(op, OrderOp::AmendOrder);
                assert_eq!(url, WsUrl::Auth);
                assert_eq!(status, OrderSubmitStatus::WireAccepted);
            }
            other => panic!("expected OrderSubmittedEvent(WireAccepted), got {other:?}"),
        }

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn ws_cancel_success_emits_wire_accepted_without_stray_amend_id() {
        use crate::dispatch::{EventPayload, EventType, OrderOp, OrderSubmitStatus};

        let (trade, factory, bus, auth_mirror, reactor, _rl) =
            wire_client_with_tracker("seeded-token-ws-cancel-emit", None);
        register_executions(&bus);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let _ = bus.subscribe(
            EventType::OrderSubmittedEvent,
            Arc::new(move |env| {
                let _ = tx.try_send(env.payload.clone());
            }),
            1,
        );

        let cl = crate::types::ClOrdId::new("ws-cancel-emit-01").unwrap();
        let cl_task = cl.clone();
        let order_fut =
            tokio::spawn(async move { trade.cancel(cl_task).via(Transport::WsV2Auth).await });

        let sock = drive_handshake_to_open(&factory, &auth_mirror).await;
        let req_id = wait_for_order_req(&sock, "cancel_order").await;
        sock.emit_text(
            serde_json::json!({
                "method": "cancel_order",
                "req_id": req_id,
                "success": true,
                "result": { "cl_ord_id": "ws-cancel-emit-01", "amend_id": "TSTRAY-WS-1" },
            })
            .to_string(),
        );

        let resp = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: cancel future never resolved")
            .expect("task panicked")
            .expect("success ack must resolve the cancel Ok");
        assert_eq!(resp.count, 1);
        assert!(!resp.pending);

        let payload = tokio::time::timeout(BUDGET, rx.recv())
            .await
            .expect("no OrderSubmittedEvent on the WS cancel path")
            .expect("channel closed");
        match payload {
            EventPayload::OrderSubmittedEvent {
                cl_ord_id,
                amend_id,
                op,
                url,
                status,
                ..
            } => {
                assert_eq!(cl_ord_id, cl);
                assert_eq!(amend_id, None, "cancel success never carries an amend_id");
                assert_eq!(op, OrderOp::CancelOrder);
                assert_eq!(url, WsUrl::Auth);
                assert_eq!(status, OrderSubmitStatus::WireAccepted);
            }
            other => panic!("expected OrderSubmittedEvent(WireAccepted), got {other:?}"),
        }

        reactor.abort();
        bus.stop_reactors();
    }
}

#[cfg(test)]
mod send_ready_gate_tests {
    use super::*;
    use crate::clock::{Clock, SystemClock};
    use crate::conn::ManagedConnection;
    use crate::conn::SubscriptionRegistry;
    use crate::conn::rate_budget::ConnectionRateBudget;
    use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
    use crate::dispatch::{
        DispatchEventBus, DispatchEventBusConfig, EventEnvelope, EventPayload, EventType,
    };
    use crate::types::{ChannelName, ConnectionState, MonotonicInstant, WsUrl};
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    #[tokio::test]
    async fn timer_reconcile_leaves_exactly_the_active_owner_armed() {
        use crate::conn::managed_connection::FsmEvent;
        use ConnectionState::{Closed, Idle, Open};
        let bus = live_bus();
        let auth = auth_stack(&bus, Some("seeded-timer"));
        // (auth state, unified private state, previous owner, armed afterwards)
        let cases = [
            (Open, Open, WsUrl::UnifiedPrivate, vec![WsUrl::Auth]),
            (Idle, Open, WsUrl::Auth, vec![WsUrl::UnifiedPrivate]),
            (Closed, Closed, WsUrl::UnifiedPrivate, vec![]),
            (Idle, Idle, WsUrl::UnifiedPrivate, vec![]),
            (
                Open,
                Open,
                WsUrl::Auth,
                vec![WsUrl::Auth, WsUrl::UnifiedPrivate],
            ),
        ];
        for (auth_state, unified_state, last, expected) in cases {
            let mut conns = HashMap::new();
            for (url, state) in [
                (WsUrl::Auth, auth_state),
                (WsUrl::UnifiedPrivate, unified_state),
            ] {
                let mut mc = mc_in_state(&bus, url, state);
                mc.arm_token_refresh(MonotonicInstant::now());
                conns.insert(url, mc);
            }
            reconcile_token_timer(&mut conns, &auth, last);
            let armed: Vec<WsUrl> = [WsUrl::Auth, WsUrl::UnifiedPrivate]
                .into_iter()
                .filter(|url| {
                    matches!(
                        conns[url].next_timer_due(),
                        Some((_, FsmEvent::TimerTokenRefreshDue))
                    )
                })
                .collect();
            assert_eq!(
                armed, expected,
                "auth={auth_state:?} unified={unified_state:?} last={last:?}"
            );
        }
        bus.stop_reactors();
    }

    fn live_bus() -> Arc<DispatchEventBus> {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            clock,
        ));
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        bus
    }

    fn mc_in_state(
        bus: &Arc<DispatchEventBus>,
        url: WsUrl,
        state: ConnectionState,
    ) -> ManagedConnection {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
            Arc::new(crate::transport::MockWsSocketFactory::new());
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let mut mc = ManagedConnection::new(
            url,
            Arc::clone(bus),
            factory,
            rate_budget,
            clock,
            Arc::new(crate::jitter::SplitMix64Jitter::with_seed(7)),
        );
        mc.test_set_state(state);
        mc
    }

    fn auth_stack(
        bus: &Arc<DispatchEventBus>,
        seed_token: Option<&str>,
    ) -> Arc<crate::auth::AuthStack> {
        use crate::auth::{
            AuthStack, SpotRestHmacSha512Signer, SystemClockNonceSource, TokenLifecycleManager,
        };
        use crate::types::{ApiKey, ApiSecret, AuthProfile};
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD as BASE64;
        use std::collections::HashMap;

        let api_key = ApiKey::new("test-api-key");
        let secret = ApiSecret::from_base64(&BASE64.encode(vec![0x01u8; 32])).unwrap();
        let signer = SpotRestHmacSha512Signer::new(api_key.clone(), secret);
        let mut signers: HashMap<AuthProfile, Arc<dyn crate::auth::AuthSigner>> = HashMap::new();
        signers.insert(AuthProfile::SpotV1, Arc::new(signer));
        let auth = Arc::new(AuthStack::new(
            Some(api_key),
            None,
            Arc::new(SystemClockNonceSource::new()),
            signers,
            TokenLifecycleManager::new(Arc::clone(bus), "<test-key>".to_string()),
        ));
        if let Some(t) = seed_token {
            auth.token_lifecycle().seed_cached_token_for_test(t);
        }
        auth
    }

    fn record_send_ready(
        bus: &Arc<DispatchEventBus>,
    ) -> Arc<Mutex<Vec<(WsUrl, MonotonicInstant)>>> {
        let seen: Arc<Mutex<Vec<(WsUrl, MonotonicInstant)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let _ = bus.subscribe(
            EventType::ConnectionSendReadyEvent,
            Arc::new(move |env: &EventEnvelope| {
                if let EventPayload::ConnectionSendReadyEvent {
                    url,
                    ready_at_monotonic,
                } = &env.payload
                {
                    sink.lock()
                        .expect("seen lock")
                        .push((*url, *ready_at_monotonic));
                    assert_eq!(env.event_type, EventType::ConnectionSendReadyEvent);
                    assert_eq!(
                        env.event_version, 1,
                        "ConnectionSendReadyEvent is event_version 1"
                    );
                    assert!(
                        env.request_id.is_none(),
                        "broadcast event — request_id None"
                    );
                }
            }),
            u16::MAX,
        );
        seen
    }

    async fn wait_for_n(
        seen: &Arc<Mutex<Vec<(WsUrl, MonotonicInstant)>>>,
        n: usize,
        what: &str,
    ) -> Vec<(WsUrl, MonotonicInstant)> {
        let poll = async {
            loop {
                let v = seen.lock().expect("seen lock").clone();
                if v.len() >= n {
                    return v;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: never saw {n} send-ready events ({what})"))
    }

    /// Asserts no send-ready event within a settle window.
    async fn assert_none_within(seen: &Arc<Mutex<Vec<(WsUrl, MonotonicInstant)>>>, what: &str) {
        let settle = async {
            loop {
                assert!(
                    seen.lock().expect("seen lock").is_empty(),
                    "{what}: a ConnectionSendReadyEvent was emitted but should NOT have been"
                );
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(100), settle).await;
        assert!(
            seen.lock().expect("seen lock").is_empty(),
            "{what}: a ConnectionSendReadyEvent was emitted after settle but should NOT have been"
        );
    }

    #[tokio::test]
    async fn emits_on_cold_start_authenticating_empty_with_token() {
        let bus = live_bus();
        let seen = record_send_ready(&bus);
        let mc = mc_in_state(&bus, WsUrl::Auth, ConnectionState::Authenticating);
        let registry = SubscriptionRegistry::new();
        let auth = auth_stack(&bus, Some("seeded-cold"));
        let flag = Arc::new(crate::conn::OrderGate::default());
        let weak = Arc::downgrade(&bus);

        refresh_send_ready(WsUrl::Auth, &mc, &registry, &auth, &flag, &weak);

        assert!(
            flag.send_ready(WsUrl::Auth),
            "send-ready level flag set true on cold-start ready"
        );
        let v = wait_for_n(&seen, 1, "cold-start emit").await;
        assert_eq!(v[0].0, WsUrl::Auth, "send-ready is for the Auth url");
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn emits_on_warm_reconnect_authenticating_empty_with_cached_token() {
        let bus = live_bus();
        let seen = record_send_ready(&bus);
        let mc = mc_in_state(&bus, WsUrl::Auth, ConnectionState::Authenticating);
        let registry = SubscriptionRegistry::new();
        let auth = auth_stack(&bus, Some("seeded-warm"));
        let flag = Arc::new(crate::conn::OrderGate::default());
        let weak = Arc::downgrade(&bus);

        refresh_send_ready(WsUrl::Auth, &mc, &registry, &auth, &flag, &weak);

        assert!(
            flag.send_ready(WsUrl::Auth),
            "send-ready level flag set true on warm-reconnect ready"
        );
        let v = wait_for_n(&seen, 1, "warm-reconnect emit").await;
        assert_eq!(v[0].0, WsUrl::Auth);
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn refresh_send_ready_negatives_do_not_emit() {
        for (url, state, token, registry_non_empty, label) in [
            (
                WsUrl::Auth,
                ConnectionState::Open,
                Some("seeded-open"),
                false,
                "Open path",
            ),
            (
                WsUrl::Auth,
                ConnectionState::Authenticating,
                None,
                false,
                "no token cached",
            ),
            (
                WsUrl::Public,
                ConnectionState::Authenticating,
                Some("seeded-public"),
                false,
                "non-auth url",
            ),
            (
                WsUrl::Auth,
                ConnectionState::Authenticating,
                Some("seeded-nonempty"),
                true,
                "non-empty auth registry",
            ),
            (
                WsUrl::Auth,
                ConnectionState::Connecting,
                Some("seeded-connecting"),
                false,
                "Connecting (not Authenticating)",
            ),
        ] {
            let bus = live_bus();
            let seen = record_send_ready(&bus);
            let mc = mc_in_state(&bus, url, state);
            let mut registry = SubscriptionRegistry::new();
            if registry_non_empty {
                registry.register(
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Executions,
                        None,
                        SubscribeParams::Executions {
                            snap_orders: None,
                            snap_trades: None,
                        },
                    ),
                    crate::types::MonotonicInstant::now(),
                    false,
                );
            }
            let auth = auth_stack(&bus, token);
            let flag = Arc::new(crate::conn::OrderGate::default());
            flag.set_send_ready(url, true);
            let weak = Arc::downgrade(&bus);

            refresh_send_ready(url, &mc, &registry, &auth, &flag, &weak);

            assert!(
                !flag.send_ready(url),
                "{label}: send-ready level flag must self-clear to false"
            );
            assert_none_within(&seen, label).await;
            bus.stop_reactors();
        }
    }

    #[tokio::test]
    async fn idempotent_double_emit_is_harmless_awaiter_fires_once() {
        let bus = live_bus();
        let seen = record_send_ready(&bus);
        let mc = mc_in_state(&bus, WsUrl::Auth, ConnectionState::Authenticating);
        let registry = SubscriptionRegistry::new();
        let auth = auth_stack(&bus, Some("seeded-idem"));
        let flag = Arc::new(crate::conn::OrderGate::default());
        let weak = Arc::downgrade(&bus);

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let tx_cell = Arc::new(Mutex::new(Some(tx)));
        let fire_count = Arc::new(AtomicU64::new(0));
        let fc = Arc::clone(&fire_count);
        let txc = Arc::clone(&tx_cell);
        let _ = bus.subscribe(
            EventType::ConnectionSendReadyEvent,
            Arc::new(move |_env: &EventEnvelope| {
                fc.fetch_add(1, Ordering::Relaxed);
                if let Some(tx) = txc.lock().expect("tx lock").take() {
                    let _ = tx.send(());
                }
            }),
            u16::MAX,
        );

        refresh_send_ready(WsUrl::Auth, &mc, &registry, &auth, &flag, &weak);
        assert!(
            flag.send_ready(WsUrl::Auth),
            "level flag true after first ready re-check"
        );
        refresh_send_ready(WsUrl::Auth, &mc, &registry, &auth, &flag, &weak);
        assert!(
            flag.send_ready(WsUrl::Auth),
            "level flag stays true after redundant ready re-check"
        );

        let v = wait_for_n(&seen, 2, "idempotent double emit").await;
        assert_eq!(
            v.len(),
            2,
            "redundant emit is published (idempotent re-check)"
        );
        tokio::time::timeout(BUDGET, rx)
            .await
            .expect("timeout: send-ready awaiter never resolved")
            .expect("oneshot dropped without firing");
        bus.stop_reactors();
    }
}

#[cfg(test)]
mod client_close_tests {
    use super::*;
    use crate::clock::{Clock, SystemClock};
    use crate::conn::ManagedConnection;
    use crate::conn::rate_budget::ConnectionRateBudget;
    use crate::conn::subscription_registry::SubscriptionRegistry;
    use crate::dispatch::event_bus::{CallerInbound, EventEnvelope, EventPayload, EventType};
    use crate::dispatch::{ClientCloseReason, DispatchEventBus};
    use crate::transport::driveable_mock::DriveableWsSocketFactory;
    use crate::types::{ConnectionState, MonotonicInstant, WsUrl};

    fn spawn_two_conn_reactor(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
    ) -> (tokio::task::JoinHandle<()>, Arc<AtomicU8>, Arc<AtomicU8>) {
        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let public_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let auth_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        state_mirrors.insert(WsUrl::Public, Arc::clone(&public_mirror));
        state_mirrors.insert(WsUrl::Auth, Arc::clone(&auth_mirror));

        let mut conns = HashMap::new();
        for (url, seed) in [(WsUrl::Public, 1u64), (WsUrl::Auth, 2u64)] {
            conns.insert(
                url,
                ManagedConnection::new(
                    url,
                    Arc::clone(bus),
                    Arc::clone(&dyn_factory),
                    Arc::clone(&rate_budget),
                    Arc::new(SystemClock) as Arc<dyn Clock>,
                    Arc::new(crate::jitter::SplitMix64Jitter::with_seed(seed)),
                ),
            );
        }

        let caller_rx = bus
            .take_caller_to_io_rx()
            .expect("caller_to_io_rx available");
        let presence_mirror: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let handler_registry = crate::dispatch::HandlerRegistry::new(presence_mirror);

        let init = IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::new(),
            handler_registry,
            auth_stack: Arc::new(crate::auth::AuthStack::new(
                None,
                None,
                Arc::new(crate::auth::SystemClockNonceSource::new()),
                HashMap::new(),
                crate::auth::TokenLifecycleManager::new(Arc::clone(bus), "<test-key>".to_string()),
            )),
            state_mirrors,
            bus_back_ref: Arc::downgrade(bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::new(crate::conn::OrderGate::default()),
        };
        (tokio::spawn(run(init)), public_mirror, auth_mirror)
    }

    /// Registered synchronously before any post, so it is race-free.
    fn capture_client_closed(
        bus: &Arc<DispatchEventBus>,
    ) -> tokio::sync::oneshot::Receiver<EventEnvelope> {
        let (tx, rx) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let cell = std::sync::Mutex::new(Some(tx));
        let _ = bus.subscribe(
            EventType::ClientClosedEvent,
            Arc::new(move |env: &EventEnvelope| {
                if let Some(tx) = cell.lock().expect("recorder lock").take() {
                    let _ = tx.send(env.clone());
                }
            }),
            1,
        );
        rx
    }

    fn assert_client_closed(env: &EventEnvelope) {
        assert_eq!(env.event_type, EventType::ClientClosedEvent);
        assert_eq!(
            env.request_id, None,
            "broadcast ClientClosedEvent carries no correlation id (the id resolves the close() waiter)"
        );
        match env.payload {
            EventPayload::ClientClosedEvent { reason, .. } => {
                assert_eq!(reason, ClientCloseReason::UserClose);
            }
            ref other => panic!("unexpected payload {other:?}"),
        }
    }

    #[tokio::test]
    async fn client_close_marker_drives_all_conns_to_closed_and_self_exits() {
        let (bus, factory) = bus_and_factory();
        let closed_rx = capture_client_closed(&bus);
        let (reactor, public_mirror, auth_mirror) = spawn_two_conn_reactor(&bus, &factory);

        const RID: u64 = 77;
        bus.try_post_caller_inbound(CallerInbound::ClientClose {
            request_id: RID,
            initiated_at: MonotonicInstant::now(),
        })
        .expect("freshly-spawned reactor: caller_to_io queue is empty");

        let env = tokio::time::timeout(BUDGET, closed_rx)
            .await
            .expect("timeout: ClientClosedEvent never emitted")
            .expect("recorder dropped");
        assert_client_closed(&env);

        let handle = crate::types::RequestHandle {
            id: RID,
            expected_completion: crate::types::ExpectedCompletionEvent::ClientClosed,
        };
        let resolved = tokio::time::timeout(BUDGET, crate::api::await_request_handle(&bus, handle))
            .await
            .expect("timeout: correlated close() completion never resolved")
            .expect("await resolved with an event");
        assert_eq!(resolved.request_id, Some(RID));
        assert_eq!(resolved.event_type, EventType::ClientClosedEvent);

        assert_eq!(
            ConnectionState::from_u8(public_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Closed
        );
        assert_eq!(
            ConnectionState::from_u8(auth_mirror.load(Ordering::Acquire)).unwrap(),
            ConnectionState::Closed
        );
        tokio::time::timeout(BUDGET, reactor)
            .await
            .expect("timeout: reactor did not self-exit after client close")
            .expect("reactor task panicked");
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn client_close_emits_exactly_one_client_closed_event() {
        let (bus, factory) = bus_and_factory();
        let seen: Arc<std::sync::Mutex<Vec<EventEnvelope>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_cb = Arc::clone(&seen);
        let _h = bus.subscribe(
            EventType::ClientClosedEvent,
            Arc::new(move |env: &EventEnvelope| {
                seen_cb.lock().expect("seen lock").push(env.clone());
            }),
            1,
        );
        let (reactor, _public_mirror, _auth_mirror) = spawn_two_conn_reactor(&bus, &factory);

        const RID: u64 = 99;
        bus.try_post_caller_inbound(CallerInbound::ClientClose {
            request_id: RID,
            initiated_at: MonotonicInstant::now(),
        })
        .expect("freshly-spawned reactor: caller_to_io queue is empty");

        tokio::time::timeout(BUDGET, reactor)
            .await
            .expect("timeout: reactor did not self-exit")
            .expect("reactor task panicked");
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        let v = seen.lock().expect("seen lock");
        assert_eq!(
            v.len(),
            1,
            "exactly one client-level ClientClosedEvent for one close (got {})",
            v.len()
        );
        assert_client_closed(&v[0]);
        drop(v);
        bus.stop_reactors();
    }
}

#[cfg(test)]
mod data_callback_isolation_tests {
    use super::auth_handshake_drive_tests::register_executions;
    use super::order_autoconnect_tests::{
        buy_limit, drive_handshake_to_open, emit_add_order_ok, wait_for_add_order_req, wire_client,
    };
    use super::reconnect_drive_tests::ack_text;
    use super::subscribe_edge_gating_tests::{register_public, spawn_public_reactor};
    use super::*;
    use crate::api::market::ws_types::{SystemStatusUpdate, TickerUpdate};
    use crate::api::ws_decode::wrap_typed;
    use crate::book::OrderBookUpdate;
    use crate::conn::subscription_registry::SubscribeParams;
    use crate::dispatch::event_bus::{CallerInbound, HandlerMutationOp};
    use crate::dispatch::{DispatchEventBus, EventEnvelope, EventPayload, EventType, HandlerId};
    use crate::types::{ChannelName, ConnectionState};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    const BUDGET: Duration = Duration::from_millis(2000);

    fn ticker_frame(symbol: &str) -> String {
        serde_json::json!({
            "channel": "ticker",
            "type": "update",
            "data": [{
                "symbol": symbol,
                "bid": 30000.0, "bid_qty": 1.0,
                "ask": 30001.0, "ask_qty": 1.0,
                "last": 30000.5, "volume": 100.0,
                "vwap": 30000.0, "low": 29000.0, "high": 31000.0,
                "change": 1.0, "change_pct": 0.1,
            }],
        })
        .to_string()
    }

    fn malformed_ticker_frame(symbol: &str) -> String {
        serde_json::json!({
            "channel": "ticker",
            "type": "update",
            "data": [{ "symbol": symbol, "bid": "not-a-number-and-missing-fields" }],
        })
        .to_string()
    }

    fn register_ticker_handler<F>(bus: &Arc<DispatchEventBus>, id: u64, cb: F) -> HandlerId
    where
        F: Fn(&TickerUpdate) + Send + Sync + 'static,
    {
        let handler_id = HandlerId(id);
        let wrapped = wrap_typed(
            cb,
            crate::api::market::ws::decode_ticker as fn(_) -> Option<TickerUpdate>,
        );
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::Public,
            channel: ChannelName::Ticker,
            op: HandlerMutationOp::Register {
                id: handler_id,
                callback: wrapped,
            },
        })
        .expect("post ticker handler register");
        handler_id
    }

    fn register_book_handler<F>(bus: &Arc<DispatchEventBus>, id: u64, cb: F) -> HandlerId
    where
        F: Fn(&OrderBookUpdate) + Send + Sync + 'static,
    {
        let handler_id = HandlerId(id);
        let wrapped = wrap_typed(
            cb,
            crate::api::market::ws::decode_book as fn(_) -> Option<OrderBookUpdate>,
        );
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::Public,
            channel: ChannelName::Book,
            op: HandlerMutationOp::Register {
                id: handler_id,
                callback: wrapped,
            },
        })
        .expect("post book handler register");
        handler_id
    }

    fn register_status_handler<F>(bus: &Arc<DispatchEventBus>, id: u64, cb: F) -> HandlerId
    where
        F: Fn(&SystemStatusUpdate) + Send + Sync + 'static,
    {
        let handler_id = HandlerId(id);
        let wrapped = wrap_typed(
            cb,
            crate::api::market::ws::decode_system_status as fn(_) -> Option<SystemStatusUpdate>,
        );
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::Public,
            channel: ChannelName::Status,
            op: HandlerMutationOp::Register {
                id: handler_id,
                callback: wrapped,
            },
        })
        .expect("post status handler register");
        handler_id
    }

    fn status_frame(system: &str) -> String {
        serde_json::json!({
            "channel": "status",
            "type": "update",
            "data": [{
                "system": system,
                "version": "2.0.10",
                "api_version": "v2",
                "connection_id": 12_345_678_u64,
            }],
        })
        .to_string()
    }

    async fn open_public_on(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<crate::transport::driveable_mock::DriveableWsSocketFactory>,
        public_mirror: &Arc<AtomicU8>,
        channel: ChannelName,
        wire_channel: &str,
        pair: &str,
        params: SubscribeParams,
    ) -> crate::transport::driveable_mock::DriveableSocketHandle {
        register_public(bus, channel, pair, params);
        let sock = socket_at(factory, 0).await;
        wait_for_state(public_mirror, ConnectionState::Resubscribing, "upgrade").await;
        sock.emit_text(ack_text(wire_channel, Some(pair)));
        wait_for_state(public_mirror, ConnectionState::Open, "subscribe ack").await;
        sock
    }

    fn collect_events(
        bus: &Arc<DispatchEventBus>,
        event_type: EventType,
    ) -> Arc<Mutex<Vec<EventEnvelope>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        // Leak the handle intentionally — its Drop would unsubscribe.
        let _handle = bus.subscribe(
            event_type,
            Arc::new(move |ev: &EventEnvelope| {
                sink.lock().expect("seen lock").push(ev.clone());
            }),
            u16::MAX,
        );
        seen
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blocking_data_callback_does_not_stall_order_send() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-isolation");

        let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let entered_cb = Arc::clone(&entered);
        let done_cb = Arc::clone(&done);
        register_ticker_handler(&bus, 9001, move |_t: &TickerUpdate| {
            entered_cb.store(true, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(1500));
            done_cb.store(true, Ordering::SeqCst);
        });

        register_executions(&bus);

        register_public(&bus, ChannelName::Ticker, "BTC/USD", ticker_params());
        let auth_sock = drive_handshake_to_open(&factory, &auth_mirror).await;

        let public_sock = {
            let poll = async {
                loop {
                    let n = factory.created_count();
                    for i in 0..n {
                        if let Some(h) = factory.handle(i)
                            && h.sent_text().iter().any(|b| {
                                serde_json::from_str::<serde_json::Value>(b)
                                    .ok()
                                    .and_then(|v| {
                                        let m = v.get("method")?.as_str()? == "subscribe";
                                        let c =
                                            v.get("params")?.get("channel")?.as_str()? == "ticker";
                                        Some(m && c)
                                    })
                                    .unwrap_or(false)
                            })
                        {
                            return h;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: public ticker subscribe never sent")
        };
        public_sock.emit_text(ack_text("ticker", Some("BTC/USD")));
        public_sock.emit_text(ticker_frame("BTC/USD"));

        let wait_entered = async {
            loop {
                if entered.load(Ordering::SeqCst) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_entered)
            .await
            .expect("timeout: blocking ticker callback never entered");

        let cl = "a1000000-0000-4000-8000-000000000001";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let req_id = tokio::time::timeout(Duration::from_millis(800), wait_for_add_order_req(&auth_sock))
            .await
            .expect("ISOLATION REGRESSION: order frame not sent within 800ms — reactor stalled by the blocking data callback");
        assert!(
            !done.load(Ordering::SeqCst),
            "order frame must be sent WHILE the data callback is still blocked (concurrent isolation), not after it returns"
        );
        emit_add_order_ok(&auth_sock, req_id, cl);

        let resp = tokio::time::timeout(Duration::from_millis(800), order_fut)
            .await
            .expect(
                "ISOLATION REGRESSION: order future did not resolve within 800ms — reactor stalled",
            )
            .expect("order task panicked")
            .expect("order resolves Ok");
        assert_eq!(resp.cl_ord_id.as_ref().map(|c| c.as_str()), Some(cl));

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn slow_data_callback_emits_slow_callback_warning() {
        let (bus, factory) = bus_and_factory();
        let warnings = collect_events(&bus, EventType::SlowCallbackWarning);
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        register_ticker_handler(&bus, 7001, move |_t: &TickerUpdate| {
            std::thread::sleep(Duration::from_millis(120));
        });
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Ticker,
            "ticker",
            "BTC/USD",
            ticker_params(),
        )
        .await;
        sock.emit_text(ticker_frame("BTC/USD"));

        let wait_warn = async {
            loop {
                {
                    let v = warnings.lock().expect("warnings lock");
                    if v.iter().any(|e| {
                        matches!(
                            e.payload,
                            EventPayload::SlowCallbackWarning { latency_us, threshold_ms, .. }
                                if latency_us >= u64::from(threshold_ms) * 1_000
                        )
                    }) {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_warn)
            .await
            .expect("timeout: slow DATA callback did not emit SlowCallbackWarning");

        let slow_source = {
            let v = warnings.lock().expect("warnings lock");
            v.iter()
                .find_map(|e| match &e.payload {
                    EventPayload::SlowCallbackWarning {
                        slow_event_type, ..
                    } => Some(*slow_event_type),
                    _ => None,
                })
                .expect("a SlowCallbackWarning was collected")
        };
        assert_eq!(
            slow_source,
            crate::dispatch::CallbackSource::DataChannel(ChannelName::Ticker),
            "data-callback SlowCallbackWarning must be keyed by its channel"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn per_channel_symbol_delivery_order_preserved() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        register_ticker_handler(&bus, 6001, move |t: &TickerUpdate| {
            sink.lock()
                .expect("seen lock")
                .push(t.symbol.as_str().to_string());
        });
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Ticker,
            "ticker",
            "BTC/USD",
            ticker_params(),
        )
        .await;

        const N: usize = 50;
        for _ in 0..N {
            sock.emit_text(ticker_frame("BTC/USD"));
        }
        let wait_all = async {
            loop {
                if seen.lock().expect("seen lock").len() >= N {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_all)
            .await
            .expect("timeout: not all ticker frames delivered");
        {
            let v = seen.lock().expect("seen lock");
            assert_eq!(v.len(), N, "all frames delivered exactly once");
            assert!(
                v.iter().all(|s| s == "BTC/USD"),
                "all deliveries are the single (ticker, BTC/USD) stream, in FIFO order"
            );
        }
        let _ = public_mirror;
        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn symbol_less_status_channel_delivers_via_dispatch() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let seen: Arc<Mutex<Vec<SystemStatusUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        register_status_handler(&bus, 9001, move |u: &SystemStatusUpdate| {
            sink.lock().expect("seen lock").push(u.clone());
        });

        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Ticker,
            "ticker",
            "BTC/USD",
            ticker_params(),
        )
        .await;

        sock.emit_text(status_frame("online"));

        let wait_status = async {
            loop {
                if !seen.lock().expect("seen lock").is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_status)
            .await
            .expect("timeout: symbol-less status frame never delivered to the dispatch loop");

        {
            let v = seen.lock().expect("seen lock");
            assert_eq!(v.len(), 1, "exactly one status delivery");
            assert_eq!(
                v[0].system.as_deref(),
                Some("online"),
                "the decoded system-status payload reached the user callback"
            );
        }

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn decode_fail_still_emits_malformed_frame_from_reactor() {
        let (bus, factory) = bus_and_factory();
        let gaps = collect_events(&bus, EventType::SubscriptionGapEvent);
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let delivered = Arc::new(AtomicUsize::new(0));
        let d = Arc::clone(&delivered);
        register_ticker_handler(&bus, 5001, move |_t: &TickerUpdate| {
            d.fetch_add(1, Ordering::SeqCst);
        });
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Ticker,
            "ticker",
            "BTC/USD",
            ticker_params(),
        )
        .await;
        sock.emit_text(malformed_ticker_frame("BTC/USD"));

        let wait_gap = async {
            loop {
                {
                    let v = gaps.lock().expect("gaps lock");
                    if v.iter().any(|e| {
                        matches!(
                            e.payload,
                            EventPayload::SubscriptionGapEvent {
                                cause: crate::dispatch::GapCause::MalformedFrame,
                                ..
                            }
                        )
                    }) {
                        return;
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_gap)
            .await
            .expect("timeout: decode-fail did not emit SubscriptionGapEvent{MalformedFrame}");
        assert_eq!(
            delivered.load(Ordering::SeqCst),
            0,
            "malformed frame must not reach the user callback"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn non_json_text_frame_drops_silently_and_routing_survives() {
        let (bus, factory) = bus_and_factory();
        let gaps = collect_events(&bus, EventType::SubscriptionGapEvent);
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let delivered = Arc::new(AtomicUsize::new(0));
        let d = Arc::clone(&delivered);
        register_ticker_handler(&bus, 3001, move |_t: &TickerUpdate| {
            d.fetch_add(1, Ordering::SeqCst);
        });
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Ticker,
            "ticker",
            "BTC/USD",
            ticker_params(),
        )
        .await;

        sock.emit_text("not json{{");

        let _ = tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                assert_eq!(
                    delivered.load(Ordering::SeqCst),
                    0,
                    "non-JSON text frame must not reach the user callback"
                );
                assert!(
                    gaps.lock().expect("gaps lock").is_empty(),
                    "non-JSON text frame must not emit SubscriptionGapEvent"
                );
                tokio::task::yield_now().await;
            }
        })
        .await;

        sock.emit_text(ticker_frame("BTC/USD"));
        let wait_delivery = async {
            loop {
                if delivered.load(Ordering::SeqCst) == 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_delivery).await.expect(
            "timeout: valid frame after the dropped garbage frame never reached its handler",
        );
        assert!(
            gaps.lock().expect("gaps lock").is_empty(),
            "the dropped garbage frame must not emit a bus event"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    fn book_snapshot_frame(pair: &str, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> String {
        let crc = crate::book::compute_book_crc32(asks.iter().copied(), bids.iter().copied());
        let to_levels = |levels: &[(&str, &str)]| -> Vec<serde_json::Value> {
            levels
                .iter()
                .map(|(p, q)| {
                    serde_json::json!({
                        "price": p.parse::<f64>().unwrap(),
                        "qty": q.parse::<f64>().unwrap(),
                    })
                })
                .collect()
        };
        serde_json::json!({
            "channel": "book",
            "type": "snapshot",
            "data": [{
                "symbol": pair,
                "bids": to_levels(bids),
                "asks": to_levels(asks),
                "checksum": crc,
            }],
        })
        .to_string()
    }

    #[tokio::test]
    async fn maintained_book_snapshot_delivered_crc_valid_via_dispatch() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let seen: Arc<Mutex<Vec<OrderBookUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        register_book_handler(&bus, 4001, move |b: &OrderBookUpdate| {
            sink.lock().expect("seen lock").push(b.clone());
        });
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bids = [("30000.0", "1.0"), ("29999.0", "2.0")];
        let asks = [("30001.0", "1.5"), ("30002.0", "2.5")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids, &asks));

        let wait_book = async {
            loop {
                if !seen.lock().expect("seen lock").is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_book)
            .await
            .expect("timeout: maintained book never delivered to on_book");
        let v = seen.lock().expect("seen lock");
        let book = &v[0];
        assert_eq!(book.symbol.as_str(), "BTC/USD");
        assert!(!book.bids.is_empty(), "maintained book has bids");
        assert!(!book.asks.is_empty(), "maintained book has asks");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn gap_skips_book_fan_with_deferred_invocation() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let delivered = Arc::new(AtomicUsize::new(0));
        let d = Arc::clone(&delivered);
        register_book_handler(&bus, 3001, move |_b: &OrderBookUpdate| {
            d.fetch_add(1, Ordering::SeqCst);
        });
        let gaps = collect_events(&bus, EventType::OrderBookGapEvent);
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bad_delta = serde_json::json!({
            "channel": "book",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [{ "price": 30000.0, "qty": 1.0 }],
                "asks": [{ "price": 30001.0, "qty": 1.0 }],
                "checksum": 1u32,
            }],
        })
        .to_string();
        sock.emit_text(bad_delta);

        let _ = tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(
            delivered.load(Ordering::SeqCst),
            0,
            "maintained Book fan must be SKIPPED on a gap/awaiting-snapshot delta — no on_book delivery"
        );
        drop(gaps);

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn raw_only_book_subscriber_emits_no_book_no_handler_drop() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let raw_seen = Arc::new(AtomicUsize::new(0));
        let rs = Arc::clone(&raw_seen);
        let raw_cb: crate::dispatch::HandlerCallback = crate::dispatch::HandlerCallback::new(
            Arc::new(move |_v| {
                rs.fetch_add(1, Ordering::SeqCst);
                crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))
            }),
            Arc::new(|_p| {}),
        );
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::Public,
            channel: ChannelName::BookRaw,
            op: HandlerMutationOp::Register {
                id: HandlerId(5101),
                callback: raw_cb,
            },
        })
        .expect("post book_raw handler register");

        let drops = collect_events(&bus, EventType::MessageDroppedNoHandler);
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bids = [("30000.0", "1.0")];
        let asks = [("30001.0", "1.0")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids, &asks));

        let _ = tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                if raw_seen.load(Ordering::SeqCst) >= 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;

        assert_eq!(
            raw_seen.load(Ordering::SeqCst),
            1,
            "the book frame must fan to the registered BookRaw handler"
        );
        let collected = drops.lock().expect("drops lock");
        assert!(
            collected.is_empty(),
            "a frame delivered to BookRaw must NOT emit MessageDroppedNoHandler{{Book}}; got {collected:?}"
        );
        drop(collected);

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn orphan_book_frame_with_no_handler_emits_one_book_drop() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let drops = collect_events(&bus, EventType::MessageDroppedNoHandler);
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bids = [("30000.0", "1.0")];
        let asks = [("30001.0", "1.0")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids, &asks));

        let _ = tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                if !drops.lock().expect("drops lock").is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;

        let collected = drops.lock().expect("drops lock");
        assert_eq!(
            collected.len(),
            1,
            "an orphaned book frame (no Book/BookRaw handler) must emit exactly one drop; got {collected:?}"
        );
        assert!(
            matches!(
                &collected[0].payload,
                EventPayload::MessageDroppedNoHandler {
                    channel: ChannelName::Book,
                    count: 1,
                    ..
                }
            ),
            "drop must report channel Book, count 1; got {:?}",
            collected[0].payload
        );
        drop(collected);

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn book_crc_gap_reseed_unsubscribe_carries_matching_depth_md3883() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        register_book_handler(&bus, 3101, move |_b: &OrderBookUpdate| {});
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bids = [("30000.0", "1.0")];
        let asks = [("30001.0", "1.0")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids, &asks));
        let bad_delta = serde_json::json!({
            "channel": "book",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [{ "price": 30000.5, "qty": 2.0 }],
                "asks": [{ "price": 30001.5, "qty": 1.5 }],
                "checksum": 1u32,
            }],
        })
        .to_string();
        sock.emit_text(bad_delta);

        let find_reseed_unsub_depth = async {
            loop {
                if let Some(depth) = sock.sent_text().iter().find_map(|body| {
                    let v: serde_json::Value = serde_json::from_str(body).ok()?;
                    if v.get("method")?.as_str()? != "unsubscribe" {
                        return None;
                    }
                    let params = v.get("params")?;
                    if params.get("channel")?.as_str()? != "book" {
                        return None;
                    }
                    Some(params.get("depth").cloned())
                }) {
                    return depth;
                }
                tokio::task::yield_now().await;
            }
        };
        let depth = tokio::time::timeout(BUDGET, find_reseed_unsub_depth)
            .await
            .expect("timeout: CRC-gap reseed unsubscribe never sent");
        assert_eq!(
            depth,
            Some(serde_json::json!(25)),
            "CRC-gap reseed unsubscribe must echo the subscribe wire depth (D10→25), not depthless"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    // Multi-thread runtime: a busy-parked worker would freeze a current-thread runtime and deadlock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drop_oldest_warns_then_book_recovers_crc_valid() {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
        let mut cfg = crate::dispatch::DispatchEventBusConfig::defaults();
        cfg.io_to_dispatch_capacity = 8;
        let bus = Arc::new(DispatchEventBus::new(cfg, clock));
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let factory = Arc::new(
            crate::transport::driveable_mock::DriveableWsSocketFactory::new(Arc::clone(&bus)),
        );

        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release_cb = Arc::clone(&release);
        let book_seen: Arc<Mutex<Vec<OrderBookUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let book_sink = Arc::clone(&book_seen);
        register_ticker_handler(&bus, 2001, move |_t: &TickerUpdate| {
            // Hard cap: tokio cannot cancel a blocking task mid-sleep.
            let start = std::time::Instant::now();
            while !release_cb.load(Ordering::SeqCst) && start.elapsed() < Duration::from_secs(3) {
                std::thread::sleep(Duration::from_millis(2));
            }
        });
        register_book_handler(&bus, 2002, move |b: &OrderBookUpdate| {
            book_sink.lock().expect("book lock").push(b.clone());
        });

        let ticker_sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Ticker,
            "ticker",
            "BTC/USD",
            ticker_params(),
        )
        .await;
        register_public(
            &bus,
            ChannelName::Book,
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        );
        ticker_sock.emit_text(ack_text("book", Some("BTC/USD")));

        ticker_sock.emit_text(ticker_frame("BTC/USD"));
        for _ in 0..200 {
            ticker_sock.emit_text(ticker_frame("BTC/USD"));
        }

        let bus_probe = Arc::clone(&bus);
        let wait_drop = async {
            loop {
                if bus_probe.dropped_count() > 0 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_drop)
            .await
            .expect("timeout: drop-oldest overflow did not increment dropped_count");

        release.store(true, Ordering::SeqCst);

        let bids = [("30000.0", "1.0"), ("29999.0", "2.0")];
        let asks = [("30001.0", "1.5"), ("30002.0", "2.5")];
        ticker_sock.emit_text(book_snapshot_frame("BTC/USD", &bids, &asks));
        let wait_book = async {
            loop {
                if !book_seen.lock().expect("book lock").is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_book)
            .await
            .expect("timeout: book did not recover after overflow");
        let v = book_seen.lock().expect("book lock");
        assert_eq!(v[0].symbol.as_str(), "BTC/USD");
        assert!(!v[0].bids.is_empty() && !v[0].asks.is_empty());

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn idempotent_ready_resolves_off_the_ring_under_parked_loop() {
        use crate::api::await_request_handle;
        use crate::types::{ExpectedCompletionEvent, RequestHandle};

        let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
        let mut cfg = crate::dispatch::DispatchEventBusConfig::defaults();
        cfg.io_to_dispatch_capacity = 8;
        let bus = Arc::new(DispatchEventBus::new(cfg, clock));
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let factory = Arc::new(
            crate::transport::driveable_mock::DriveableWsSocketFactory::new(Arc::clone(&bus)),
        );
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        // Hard cap: tokio cannot cancel a blocking task mid-sleep.
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release_cb = Arc::clone(&release);
        register_ticker_handler(&bus, 8001, move |_t: &TickerUpdate| {
            let start = std::time::Instant::now();
            while !release_cb.load(Ordering::SeqCst) && start.elapsed() < Duration::from_secs(3) {
                std::thread::sleep(Duration::from_millis(2));
            }
        });

        let ticker_sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Ticker,
            "ticker",
            "BTC/USD",
            ticker_params(),
        )
        .await;

        ticker_sock.emit_text(ticker_frame("BTC/USD"));
        for _ in 0..200 {
            ticker_sock.emit_text(ticker_frame("BTC/USD"));
        }

        let bus_probe = Arc::clone(&bus);
        let wait_drop = async {
            loop {
                if bus_probe.dropped_count() > 0 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_drop)
            .await
            .expect("timeout: ring did not overflow under the parked loop");

        let request_id = 777_u64;
        let handle = RequestHandle {
            id: request_id,
            expected_completion: ExpectedCompletionEvent::ClientReady,
        };
        let bus_await = Arc::clone(&bus);
        let waiter = tokio::spawn(async move { await_request_handle(&bus_await, handle).await });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let synthetic = EventEnvelope {
            event_type: EventType::ClientReady,
            event_version: 1,
            timestamp_monotonic: bus.clock().now(),
            request_id: Some(request_id),
            payload: EventPayload::ClientReady {
                capability_snapshot: crate::types::CapabilitySnapshot {
                    declared_namespaces: std::collections::HashSet::new(),
                    declared_ws_urls: std::collections::HashSet::new(),
                    discovered_at_first_use: std::collections::HashSet::new(),
                },
            },
        };
        bus.deliver_correlated(&synthetic);

        let env = tokio::time::timeout(BUDGET, waiter)
            .await
            .expect("RING-EVICTION REGRESSION: idempotent ready() completion never resolved — synthetic ClientReady drop-oldest-evicted before the parked loop saw it")
            .expect("waiter task panicked")
            .expect("await resolved with an event");
        assert_eq!(env.request_id, Some(request_id));
        assert_eq!(env.event_type, EventType::ClientReady);

        release.store(true, Ordering::SeqCst);
        reactor.abort();
        bus.stop_reactors();
    }

    // Ignored by default (timing-sensitive): cargo test latency_bench -- --ignored --nocapture --test-threads=1
    mod latency_bench {
        use super::*;
        use crate::types::MonotonicInstant;
        use std::sync::atomic::AtomicU64;

        type Hist = crate::dispatch::io_reactor::latency_histogram::LatencyHistogram;

        #[inline]
        fn now_ns() -> u64 {
            MonotonicInstant::now().as_duration().as_nanos() as u64
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore = "latency bench, run with --ignored --nocapture"]
        async fn bench_book_delivery_latency() {
            const WARMUP: u64 = 200;
            const N: u64 = 2_000;

            let (bus, factory) = bus_and_factory();
            let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

            let end_to_end_hist = Arc::new(Mutex::new(Hist::new()));
            let emit_start = Arc::new(AtomicU64::new(0));
            let arrivals = Arc::new(AtomicU64::new(0));
            {
                let end_to_end_hist = Arc::clone(&end_to_end_hist);
                let emit_start = Arc::clone(&emit_start);
                let arrivals = Arc::clone(&arrivals);
                register_book_handler(&bus, 7777, move |_b: &OrderBookUpdate| {
                    let arrived = now_ns();
                    let emitted = emit_start.load(Ordering::Relaxed);
                    end_to_end_hist
                        .lock()
                        .unwrap()
                        .record(arrived.saturating_sub(emitted));
                    arrivals.fetch_add(1, Ordering::Relaxed);
                });
            }

            let sock = open_public_on(
                &bus,
                &factory,
                &public_mirror,
                ChannelName::Book,
                "book",
                "BTC/USD",
                SubscribeParams::Book {
                    depth: crate::types::BookDepth::D10,
                },
            )
            .await;

            let bids = [("30000.0", "1.0"), ("29999.0", "2.0")];
            let asks = [("30001.0", "1.5"), ("30002.0", "2.5")];
            let frame = book_snapshot_frame("BTC/USD", &bids, &asks);

            for i in 0..(WARMUP + N) {
                let expected = i + 1;
                emit_start.store(now_ns(), Ordering::Relaxed);
                sock.emit_text(frame.clone());
                let wait = async {
                    while arrivals.load(Ordering::Relaxed) < expected {
                        tokio::task::yield_now().await;
                    }
                };
                tokio::time::timeout(BUDGET, wait)
                    .await
                    .expect("book delivery timed out");
                if expected == WARMUP {
                    *end_to_end_hist.lock().unwrap() = Hist::new();
                    crate::dispatch::io_reactor::latency_histogram::reactor_probe_arm();
                    crate::dispatch::io_reactor::latency_histogram::typed_delivery_probe_arm();
                    crate::dispatch::io_reactor::latency_histogram::dispatch_probe_arm();
                }
            }

            let end_to_end = end_to_end_hist.lock().unwrap();
            println!("{}", end_to_end.report("book delivery (end-to-end)"));
            let reactor_hist = crate::dispatch::io_reactor::latency_histogram::reactor_probe_take()
                .expect("reactor probe was armed at the warm-up boundary");
            println!("{}", reactor_hist.report("reactor on-thread"));
            assert_eq!(
                reactor_hist.count(),
                N,
                "reactor probe must measure exactly N frames"
            );
            assert!(
                reactor_hist.percentile(99.0) < 5_000_000,
                "reactor compute p99 should be < 5ms on the in-process mock"
            );
            let typed_delivery_hist =
                crate::dispatch::io_reactor::latency_histogram::typed_delivery_probe_take()
                    .expect("typed-delivery probe was armed at the warm-up boundary");
            println!(
                "{}",
                typed_delivery_hist.report("book typed delivery (fan)")
            );
            assert_eq!(
                typed_delivery_hist.count(),
                N,
                "typed-delivery probe must measure exactly N frames"
            );
            let typed_delivery_share_pct = typed_delivery_hist.percentile(50.0) as f64
                / reactor_hist.percentile(50.0).max(1) as f64
                * 100.0;
            println!(
                "  typed delivery p50 is {typed_delivery_share_pct:.0}% of reactor p50 (encode/decode round-trip removed by MD-3896)"
            );
            let dispatch_hist =
                crate::dispatch::io_reactor::latency_histogram::dispatch_probe_take()
                    .expect("dispatch probe was armed at the warm-up boundary");
            println!("{}", dispatch_hist.report("dispatch on-thread"));
            assert_eq!(
                dispatch_hist.count(),
                N,
                "dispatch probe must measure exactly N deliveries"
            );
            assert!(
                dispatch_hist.percentile(99.0) < 1_000_000,
                "dispatch overhead p99 should be well under 1ms (thin invoker)"
            );
            assert_eq!(end_to_end.count(), N, "expected exactly N measured samples");

            reactor.abort();
            bus.stop_reactors();
        }

        async fn wait_for_nth_add_order(
            sock: &crate::transport::driveable_mock::DriveableSocketHandle,
            n: usize,
        ) -> u64 {
            let poll = async {
                loop {
                    let mut ids = Vec::new();
                    for body in sock.sent_text() {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body)
                            && v.get("method").and_then(serde_json::Value::as_str)
                                == Some("add_order")
                            && let Some(id) = v.get("req_id").and_then(serde_json::Value::as_u64)
                        {
                            ids.push(id);
                        }
                    }
                    if ids.len() >= n {
                        return ids[n - 1];
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: nth add_order frame never sent")
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore = "latency bench, run with --ignored --nocapture"]
        async fn bench_order_forward_compute() {
            const WARMUP: u64 = 5;
            const N: u64 = 35;

            let (trade, factory, bus, auth_mirror, reactor) =
                wire_client("seeded-token-order-bench");
            register_executions(&bus);

            let pending = trade.order(buy_limit("BTC/USDC", "0.0001", "33512.0"));
            tokio::spawn(async move {
                let _ = pending.await;
            });
            let sock = drive_handshake_to_open(&factory, &auth_mirror).await;

            for i in 0..(WARMUP + N) {
                if i > 0 {
                    let pending = trade.order(buy_limit("BTC/USDC", "0.0001", "33512.0"));
                    tokio::spawn(async move {
                        let _ = pending.await;
                    });
                }
                let req_id = wait_for_nth_add_order(&sock, (i + 1) as usize).await;
                emit_add_order_ok(&sock, req_id, "OBENCH-1");
                if i + 1 == WARMUP {
                    crate::dispatch::io_reactor::latency_histogram::order_forward_probe_arm();
                }
            }

            let order_forward_hist =
                crate::dispatch::io_reactor::latency_histogram::order_forward_probe_take()
                    .expect("order-forward probe was armed at the warm-up boundary");
            println!("{}", order_forward_hist.report("order-forward (reactor)"));
            assert_eq!(
                order_forward_hist.count(),
                N,
                "order-forward probe must measure exactly N orders"
            );

            reactor.abort();
            bus.stop_reactors();
        }
    }

    fn register_book_raw_handler<F>(bus: &Arc<DispatchEventBus>, id: u64, cb: F) -> HandlerId
    where
        F: Fn(&crate::book::BookDelta) + Send + Sync + 'static,
    {
        let handler_id = HandlerId(id);
        let wrapped = wrap_typed(
            cb,
            crate::api::market::ws::decode_book_raw as fn(_) -> Option<crate::book::BookDelta>,
        );
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::Public,
            channel: ChannelName::BookRaw,
            op: HandlerMutationOp::Register {
                id: handler_id,
                callback: wrapped,
            },
        })
        .expect("post book_raw handler register");
        handler_id
    }

    async fn await_first<T: Clone>(seen: &Arc<Mutex<Vec<T>>>, what: &str) -> T {
        let wait = async {
            loop {
                if let Some(v) = seen.lock().expect("seen lock").first() {
                    return v.clone();
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait)
            .await
            .unwrap_or_else(|_| panic!("timeout: {what} never delivered"))
    }

    #[tokio::test]
    async fn fast_path_book_equals_encode_decode_path() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let seen: Arc<Mutex<Vec<OrderBookUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        register_book_handler(&bus, 8801, move |b: &OrderBookUpdate| {
            sink.lock().expect("seen lock").push(b.clone());
        });
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bids = [("30000.0", "1.0"), ("29999.5", "2.25")];
        let asks = [("30001.0", "1.5"), ("30002.0", "0.75")];
        let frame = book_snapshot_frame("BTC/USD", &bids, &asks);
        sock.emit_text(frame.clone());

        let delivered = await_first(&seen, "fast-path maintained book").await;

        let wire: serde_json::Value = serde_json::from_str(&frame).unwrap();
        let entry = wire["data"][0].clone();
        let envelope = serde_json::json!({ "type": "snapshot", "data": entry });
        let old_path = crate::api::market::ws::decode_book(envelope)
            .expect("decode_book parses the same frame");

        assert_eq!(delivered.symbol, old_path.symbol);
        assert_eq!(delivered.checksum, old_path.checksum);
        assert_eq!(
            delivered.bids, old_path.bids,
            "bids incl. price_wire/qty_wire"
        );
        assert_eq!(
            delivered.asks, old_path.asks,
            "asks incl. price_wire/qty_wire"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn fast_path_respects_depth_cap() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let seen: Arc<Mutex<Vec<OrderBookUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        register_book_handler(&bus, 8802, move |b: &OrderBookUpdate| {
            sink.lock().expect("seen lock").push(b.clone());
        });
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bids: Vec<(String, String)> = (0..12)
            .map(|i| (format!("{}.0", 30000 - i), "1.0".to_string()))
            .collect();
        let asks: Vec<(String, String)> = (0..12)
            .map(|i| (format!("{}.0", 30001 + i), "1.0".to_string()))
            .collect();
        let bids_ref: Vec<(&str, &str)> =
            bids.iter().map(|(p, q)| (p.as_str(), q.as_str())).collect();
        let asks_ref: Vec<(&str, &str)> =
            asks.iter().map(|(p, q)| (p.as_str(), q.as_str())).collect();
        let crc = crate::book::compute_book_crc32(
            asks_ref.iter().take(10).copied(),
            bids_ref.iter().take(10).copied(),
        );
        let to_levels = |ls: &[(&str, &str)]| -> Vec<serde_json::Value> {
            ls.iter()
                .map(|(p, q)| serde_json::json!({ "price": p, "qty": q }))
                .collect()
        };
        let frame = serde_json::json!({
            "channel": "book",
            "type": "snapshot",
            "data": [{
                "symbol": "BTC/USD",
                "bids": to_levels(&bids_ref),
                "asks": to_levels(&asks_ref),
                "checksum": crc,
            }],
        })
        .to_string();
        sock.emit_text(frame);

        let delivered = await_first(&seen, "depth-capped maintained book").await;
        assert_eq!(delivered.bids.len(), 10, "bids capped to caller depth D10");
        assert_eq!(delivered.asks.len(), 10, "asks capped to caller depth D10");

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn fast_path_crc_mismatch_skips_and_gaps() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let seen: Arc<Mutex<Vec<OrderBookUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        register_book_handler(&bus, 8803, move |b: &OrderBookUpdate| {
            sink.lock().expect("seen lock").push(b.clone());
        });
        let gaps = collect_events(&bus, EventType::OrderBookGapEvent);
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bids = [("30000.0", "1.0"), ("29999.0", "2.0")];
        let asks = [("30001.0", "1.5"), ("30002.0", "2.5")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids, &asks));
        let wait_top_bid = |px: &'static str| {
            let seen = Arc::clone(&seen);
            async move {
                loop {
                    if seen.lock().expect("seen lock").iter().any(|b| {
                        b.bids
                            .first()
                            .is_some_and(|l| l.price == px.parse().unwrap())
                    }) {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            }
        };
        tokio::time::timeout(BUDGET, wait_top_bid("30000.0"))
            .await
            .expect("timeout: valid snapshot never delivered");

        let bad_delta = serde_json::json!({
            "channel": "book",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [{ "price": "29998.0", "qty": "3.0" }],
                "asks": [],
                "checksum": 42u32,
            }],
        })
        .to_string();
        sock.emit_text(bad_delta);

        let wait_gap = async {
            loop {
                if !gaps.lock().expect("gaps lock").is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_gap)
            .await
            .expect("timeout: OrderBookGapEvent never emitted on CRC mismatch");

        let bids2 = [("30010.0", "1.0"), ("30009.0", "2.0")];
        let asks2 = [("30011.0", "1.5"), ("30012.0", "2.5")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids2, &asks2));
        tokio::time::timeout(BUDGET, wait_top_bid("30010.0"))
            .await
            .expect("timeout: post-gap reseed snapshot never delivered");

        let books = seen.lock().expect("seen lock");
        assert_eq!(
            books.len(),
            2,
            "CRC-mismatch frame must be SKIPPED by the fast-path — only the two valid \
             snapshots deliver, never the stale gap frame; got {books:?}"
        );
        assert_eq!(
            books[0].bids[0].price,
            "30000.0".parse().unwrap(),
            "snapshot #1 first"
        );
        assert_eq!(
            books[1].bids[0].price,
            "30010.0".parse().unwrap(),
            "reseed snapshot #2 next"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn fast_path_book_and_book_raw_both_delivered() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let books: Arc<Mutex<Vec<OrderBookUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let raws: Arc<Mutex<Vec<crate::book::BookDelta>>> = Arc::new(Mutex::new(Vec::new()));
        let bsink = Arc::clone(&books);
        let rsink = Arc::clone(&raws);
        register_book_handler(&bus, 8804, move |b: &OrderBookUpdate| {
            bsink.lock().expect("books lock").push(b.clone());
        });
        register_book_raw_handler(&bus, 8805, move |d: &crate::book::BookDelta| {
            rsink.lock().expect("raws lock").push(d.clone());
        });
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bids = [("30000.0", "1.0")];
        let asks = [("30001.0", "1.5")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids, &asks));

        let book = await_first(&books, "maintained book (with raw also registered)").await;
        let raw = await_first(&raws, "raw book delta").await;
        assert_eq!(book.symbol.as_str(), "BTC/USD");
        assert_eq!(raw.symbol.as_str(), "BTC/USD");
        assert!(raw.is_snapshot, "raw path sees the snapshot flag");
        assert_eq!(
            raw.checksum, book.checksum,
            "same wire checksum on both paths"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    fn find_reseed_pair(frames: &[String]) -> Option<(serde_json::Value, serde_json::Value)> {
        let parsed: Vec<serde_json::Value> = frames
            .iter()
            .filter_map(|b| serde_json::from_str(b).ok())
            .collect();
        let is_book = |v: &serde_json::Value, method: &str| {
            v.get("method").and_then(|m| m.as_str()) == Some(method)
                && v["params"]["channel"] == "book"
        };
        let u_idx = parsed.iter().position(|v| is_book(v, "unsubscribe"))?;
        let s = parsed[u_idx + 1..]
            .iter()
            .find(|v| is_book(v, "subscribe"))?;
        Some((parsed[u_idx]["params"].clone(), s["params"].clone()))
    }

    fn bad_crc_book_delta(pair: &str) -> String {
        serde_json::json!({
            "channel": "book",
            "type": "update",
            "data": [{
                "symbol": pair,
                "bids": [{ "price": "29998.0", "qty": "3.0" }],
                "asks": [],
                "checksum": 1u32,
            }],
        })
        .to_string()
    }

    async fn wait_top_bid(books: &Arc<Mutex<Vec<OrderBookUpdate>>>, px: &'static str) {
        loop {
            if books.lock().expect("books lock").iter().any(|b| {
                b.bids
                    .first()
                    .is_some_and(|l| l.price == px.parse().unwrap())
            }) {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn book_crc_gap_reseed_closed_loop_redelivers_fresh_snapshot() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let books: Arc<Mutex<Vec<OrderBookUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let raws: Arc<Mutex<Vec<crate::book::BookDelta>>> = Arc::new(Mutex::new(Vec::new()));
        let bsink = Arc::clone(&books);
        let rsink = Arc::clone(&raws);
        register_book_handler(&bus, 9101, move |b: &OrderBookUpdate| {
            bsink.lock().expect("books lock").push(b.clone());
        });
        register_book_raw_handler(&bus, 9102, move |d: &crate::book::BookDelta| {
            rsink.lock().expect("raws lock").push(d.clone());
        });
        let gaps = collect_events(&bus, EventType::OrderBookGapEvent);
        let sub_gaps = collect_events(&bus, EventType::SubscriptionGapEvent);
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bids = [("30000.0", "1.0"), ("29999.0", "2.0")];
        let asks = [("30001.0", "1.5"), ("30002.0", "2.5")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids, &asks));
        tokio::time::timeout(BUDGET, wait_top_bid(&books, "30000.0"))
            .await
            .expect("timeout: snapshot #1 never delivered");

        sock.emit_text(bad_crc_book_delta("BTC/USD"));
        let wait_gap_pair = async {
            loop {
                if !gaps.lock().expect("gaps lock").is_empty()
                    && !sub_gaps.lock().expect("sub_gaps lock").is_empty()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_gap_pair)
            .await
            .expect("timeout: gap-event pair never emitted on CRC mismatch");
        assert!(
            matches!(
                &gaps.lock().expect("gaps lock")[0].payload,
                EventPayload::OrderBookGapEvent {
                    channel: ChannelName::Book,
                    cause: crate::dispatch::GapCause::OrderBookCrcMismatch,
                    symbol,
                } if symbol.as_str() == "BTC/USD"
            ),
            "OrderBookGapEvent must carry (Book, BTC/USD, OrderBookCrcMismatch)"
        );
        assert!(
            matches!(
                &sub_gaps.lock().expect("sub_gaps lock")[0].payload,
                EventPayload::SubscriptionGapEvent {
                    channel: ChannelName::Book,
                    cause: crate::dispatch::GapCause::OrderBookCrcMismatch,
                    symbol,
                    ..
                } if symbol.as_str() == "BTC/USD"
            ),
            "parallel SubscriptionGapEvent must carry (Book, BTC/USD, OrderBookCrcMismatch)"
        );

        let (unsub, resub) = tokio::time::timeout(BUDGET, async {
            loop {
                if let Some(pair) = find_reseed_pair(&sock.sent_text()) {
                    return pair;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timeout: reseed unsubscribe+resubscribe never sent");
        assert_eq!(
            unsub["depth"],
            serde_json::json!(25),
            "reseed unsubscribe echoes wire depth"
        );
        assert!(
            unsub.get("snapshot").is_none(),
            "reseed unsubscribe must not carry snapshot; got {unsub:?}"
        );
        assert_eq!(
            resub["depth"],
            serde_json::json!(25),
            "reseed resubscribe same wire depth"
        );
        assert_eq!(
            resub["snapshot"],
            serde_json::json!(true),
            "reseed resubscribe re-requests the opening snapshot"
        );

        let bids2 = [("30010.0", "1.0"), ("30009.0", "2.0")];
        let asks2 = [("30011.0", "1.5"), ("30012.0", "2.5")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids2, &asks2));
        tokio::time::timeout(BUDGET, wait_top_bid(&books, "30010.0"))
            .await
            .expect("timeout: fresh reseed snapshot never re-delivered to on_book");

        {
            let delivered = books.lock().expect("books lock");
            assert_eq!(
                delivered.len(),
                2,
                "maintained view: two valid snapshots only"
            );
            assert_eq!(delivered[0].bids[0].price, "30000.0".parse().unwrap());
            assert_eq!(delivered[1].bids[0].price, "30010.0".parse().unwrap());
        }

        let wait_raw = async {
            loop {
                if raws.lock().expect("raws lock").len() >= 3 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, wait_raw)
            .await
            .expect("timeout: raw view never received all three frames");
        let raw = raws.lock().expect("raws lock");
        assert_eq!(
            raw.len(),
            3,
            "raw view: snapshot, bad delta, fresh snapshot"
        );
        assert!(raw[0].is_snapshot);
        assert!(!raw[1].is_snapshot, "the bad-CRC delta still fans raw");
        assert!(raw[2].is_snapshot);
        assert_eq!(raw[2].bids[0].price, "30010.0".parse().unwrap());
        drop(raw);

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn book_reseed_timeout_ladder_retries_then_degrades() {
        let (bus, factory) = bus_and_factory();
        bus.knobs()
            .subscribe_ack_timeout_ms
            .store(50, std::sync::atomic::Ordering::Relaxed);
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let books: Arc<Mutex<Vec<OrderBookUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let bsink = Arc::clone(&books);
        register_book_handler(&bus, 9103, move |b: &OrderBookUpdate| {
            bsink.lock().expect("books lock").push(b.clone());
        });
        let gaps = collect_events(&bus, EventType::OrderBookGapEvent);
        let sub_gaps = collect_events(&bus, EventType::SubscriptionGapEvent);
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let bids = [("30000.0", "1.0")];
        let asks = [("30001.0", "1.0")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids, &asks));
        sock.emit_text(bad_crc_book_delta("BTC/USD"));

        let count_reseed_unsubs = |frames: &[String]| {
            frames
                .iter()
                .filter_map(|b| serde_json::from_str::<serde_json::Value>(b).ok())
                .filter(|v| {
                    v.get("method").and_then(|m| m.as_str()) == Some("unsubscribe")
                        && v["params"]["channel"] == "book"
                })
                .count()
        };
        let drive = async {
            let mut acked_subscribes = 0usize;
            loop {
                let subs = sock
                    .sent_text()
                    .iter()
                    .filter_map(|b| serde_json::from_str::<serde_json::Value>(b).ok())
                    .filter(|v| {
                        v.get("method").and_then(|m| m.as_str()) == Some("subscribe")
                            && v["params"]["channel"] == "book"
                    })
                    .count();
                while acked_subscribes < subs.saturating_sub(1) {
                    sock.emit_text(ack_text("book", Some("BTC/USD")));
                    acked_subscribes += 1;
                }
                if gaps.lock().expect("gaps lock").len() >= 2
                    && sub_gaps.lock().expect("sub_gaps lock").len() >= 2
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(Duration::from_millis(5000), drive)
            .await
            .expect("timeout: reseed ladder never reached the degrade gap-event pair");

        tokio::time::sleep(Duration::from_millis(120)).await;

        assert_eq!(
            count_reseed_unsubs(&sock.sent_text()),
            4,
            "reseed ladder: initial + 3 retries before the cap"
        );
        {
            let g = gaps.lock().expect("gaps lock");
            assert_eq!(
                g.len(),
                2,
                "exactly the initial gap + the final degrade pair"
            );
            assert!(
                matches!(
                    &g[1].payload,
                    EventPayload::OrderBookGapEvent {
                        cause: crate::dispatch::GapCause::OrderBookCrcMismatch,
                        ..
                    }
                ),
                "degrade reuses OrderBookCrcMismatch (no new GapCause in v1)"
            );
        }
        assert_eq!(
            sub_gaps.lock().expect("sub_gaps lock").len(),
            2,
            "SubscriptionGapEvent fires with each OrderBookGapEvent (initial + degrade)"
        );

        let bids2 = [("30020.0", "1.0")];
        let asks2 = [("30021.0", "1.0")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids2, &asks2));
        tokio::time::timeout(BUDGET, wait_top_bid(&books, "30020.0"))
            .await
            .expect("timeout: post-degrade frame never delivered per-frame");
        assert_eq!(
            books.lock().expect("books lock").len(),
            2,
            "snapshot #1 (maintained) + the post-degrade per-frame delivery"
        );
        assert_eq!(
            gaps.lock().expect("gaps lock").len(),
            2,
            "per-frame delivery emits no further gap events"
        );

        reactor.abort();
        bus.stop_reactors();
    }

    /// The strings are the CRC32 source of truth.
    fn wire_level(price: &str, qty: &str) -> crate::book::PriceLevel {
        crate::book::PriceLevel {
            price: price.parse().unwrap(),
            qty: qty.parse().unwrap(),
            price_wire: price.to_string(),
            qty_wire: qty.to_string(),
        }
    }

    fn book_delta_frame(
        pair: &str,
        prior_bids: &[(&str, &str)],
        prior_asks: &[(&str, &str)],
        bids: &[(&str, &str)],
        asks: &[(&str, &str)],
    ) -> String {
        let mut mirror =
            crate::book::OrderBookBuilder::new(crate::types::Symbol::new(pair).unwrap(), 25, 10);
        let to_levels = |ls: &[(&str, &str)]| ls.iter().map(|(p, q)| wire_level(p, q)).collect();
        let _ = mirror.apply_snapshot(to_levels(prior_bids), to_levels(prior_asks), 0);
        let _ = mirror.apply_delta(to_levels(bids), to_levels(asks), 0);
        let crc = mirror.compute_checksum();
        let json_levels = |ls: &[(&str, &str)]| -> Vec<serde_json::Value> {
            ls.iter()
                .map(|(p, q)| serde_json::json!({ "price": p, "qty": q }))
                .collect()
        };
        serde_json::json!({
            "channel": "book",
            "type": "update",
            "data": [{
                "symbol": pair,
                "bids": json_levels(bids),
                "asks": json_levels(asks),
                "checksum": crc,
            }],
        })
        .to_string()
    }

    #[tokio::test]
    async fn fast_path_book_delta_advances_maintained_top_of_book() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let seen: Arc<Mutex<Vec<OrderBookUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        register_book_handler(&bus, 8807, move |b: &OrderBookUpdate| {
            sink.lock().expect("seen lock").push(b.clone());
        });
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        )
        .await;

        let snap_bids = [("30000.0", "1.0"), ("29999.0", "2.0")];
        let snap_asks = [("30001.0", "1.5"), ("30002.0", "2.5")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &snap_bids, &snap_asks));

        let wait_top_bid = |px: &'static str| {
            let seen = Arc::clone(&seen);
            async move {
                loop {
                    if seen.lock().expect("seen lock").iter().any(|b| {
                        b.bids
                            .first()
                            .is_some_and(|l| l.price == px.parse().unwrap())
                    }) {
                        return;
                    }
                    tokio::task::yield_now().await;
                }
            }
        };
        tokio::time::timeout(BUDGET, wait_top_bid("30000.0"))
            .await
            .expect("timeout: snapshot never delivered");

        let delta = book_delta_frame(
            "BTC/USD",
            &snap_bids,
            &snap_asks,
            &[("30005.0", "0.5")],
            &[("30003.0", "0.9")],
        );
        sock.emit_text(delta);

        tokio::time::timeout(BUDGET, wait_top_bid("30005.0"))
            .await
            .expect("timeout: CRC-valid delta never advanced the maintained top-of-book");

        let books = seen.lock().expect("seen lock");
        let advanced = books
            .iter()
            .rev()
            .find(|b| {
                b.bids
                    .first()
                    .is_some_and(|l| l.price == "30005.0".parse().unwrap())
            })
            .expect("a delivered book with the delta's new best bid");
        assert_eq!(
            advanced.bids[0].price,
            "30005.0".parse().unwrap(),
            "delta's new bid is the cumulative best bid"
        );
        assert!(
            advanced
                .bids
                .iter()
                .any(|l| l.price == "30000.0".parse().unwrap()),
            "the pre-delta bid level survives — delta is cumulative, not a replace"
        );
        assert_eq!(
            advanced.asks[0].price,
            "30001.0".parse().unwrap(),
            "snapshot's best ask survives — delta ask is worse, doesn't displace it"
        );
        assert!(
            advanced
                .asks
                .iter()
                .any(|l| l.price == "30003.0".parse().unwrap()),
            "the delta's ask joined the book below the best ask — cumulative apply"
        );

        drop(books);
        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn degraded_on_book_delivers_via_decode_book() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let seen: Arc<Mutex<Vec<OrderBookUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        register_book_handler(&bus, 8806, move |b: &OrderBookUpdate| {
            sink.lock().expect("seen lock").push(b.clone());
        });
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Book,
            "book",
            "BTC/USD",
            SubscribeParams::BookRaw {
                depth: crate::types::BookDepth::D10,
                snapshot: None,
            },
        )
        .await;

        let bids = [("30000.0", "1.0"), ("29999.0", "2.0")];
        let asks = [("30001.0", "1.5")];
        sock.emit_text(book_snapshot_frame("BTC/USD", &bids, &asks));

        let book = await_first(&seen, "degraded (PassThrough) on_book via decode_book").await;
        assert_eq!(book.symbol.as_str(), "BTC/USD");
        assert_eq!(
            book.bids.len(),
            2,
            "per-frame decode_book carries the frame levels"
        );
        assert_eq!(book.asks.len(), 1);

        reactor.abort();
        bus.stop_reactors();
    }
}

mod subscription_mirror_tests {
    use super::*;
    use crate::conn::ManagedConnection;
    use crate::conn::rate_budget::ConnectionRateBudget;
    use crate::conn::subscription_registry::{
        EntrySubState, SubscribeParams, SubscriptionEntry, SubscriptionMirror,
    };
    use crate::dispatch::event_bus::{CallerInbound, RegistryMutationOp};
    use crate::types::{BookDepth, ChannelName, ConnectionState, OhlcInterval, WsUrl};
    use std::time::Duration;

    use super::reconnect_drive_tests::ack_text;

    fn reject_text(channel: &str, symbol: Option<&str>, error: &str) -> String {
        let mut frame = serde_json::Map::new();
        frame.insert("method".into(), serde_json::json!("subscribe"));
        frame.insert("success".into(), serde_json::json!(false));
        frame.insert("error".into(), serde_json::json!(error));
        frame.insert("channel".into(), serde_json::json!(channel));
        if let Some(s) = symbol {
            frame.insert("symbol".into(), serde_json::json!(s));
        }
        serde_json::Value::Object(frame).to_string()
    }

    pub(super) struct MirrorRig {
        pub(super) bus: Arc<DispatchEventBus>,
        pub(super) factory: Arc<DriveableWsSocketFactory>,
        pub(super) mirror: SubscriptionMirror,
        pub(super) public_state: Arc<AtomicU8>,
        pub(super) terminated: Arc<AtomicU64>,
        pub(super) order_gate: Arc<crate::conn::OrderGate>,
        pub(super) reactor: tokio::task::JoinHandle<()>,
    }

    fn spawn_mirror_rig() -> MirrorRig {
        spawn_rig(true, None)
    }

    /// `start_dispatch = false` leaves dispatch stopped and the terminated counter unsubscribed.
    pub(super) fn spawn_rig(start_dispatch: bool, ack_timeout_ms: Option<u32>) -> MirrorRig {
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::new(SystemClock) as Arc<dyn Clock>,
        ));
        if start_dispatch {
            bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        }
        if let Some(ms) = ack_timeout_ms {
            bus.knobs()
                .subscribe_ack_timeout_ms
                .store(ms, Ordering::Relaxed);
        }
        let factory = Arc::new(DriveableWsSocketFactory::new(Arc::clone(&bus)));
        let terminated = Arc::new(AtomicU64::new(0));
        if start_dispatch {
            let t = Arc::clone(&terminated);
            let _ = bus.subscribe(
                crate::dispatch::EventType::SubscriptionTerminatedEvent,
                Arc::new(move |_env: &crate::dispatch::EventEnvelope| {
                    t.fetch_add(1, Ordering::Relaxed);
                }),
                u16::MAX,
            );
        }
        let public_state = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        state_mirrors.insert(WsUrl::Public, Arc::clone(&public_state));
        state_mirrors.insert(
            WsUrl::Auth,
            Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())),
        );
        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(&factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let mut conns = HashMap::new();
        for (url, seed) in [(WsUrl::Public, 1), (WsUrl::Auth, 2)] {
            conns.insert(
                url,
                ManagedConnection::new(
                    url,
                    Arc::clone(&bus),
                    Arc::clone(&dyn_factory),
                    Arc::clone(&rate_budget),
                    Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                    Arc::new(crate::jitter::SplitMix64Jitter::with_seed(seed)),
                ),
            );
        }
        let caller_rx = bus
            .take_caller_to_io_rx()
            .expect("caller_to_io_rx available");
        let presence_mirror: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let mirror = SubscriptionMirror::default();
        let order_gate = Arc::new(crate::conn::OrderGate::default());
        let init = IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::with_mirror(Arc::clone(&mirror)),
            handler_registry: crate::dispatch::HandlerRegistry::new(presence_mirror),
            auth_stack: Arc::new(crate::auth::AuthStack::new(
                None,
                None,
                Arc::new(crate::auth::SystemClockNonceSource::new()),
                std::collections::HashMap::new(),
                crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
            )),
            state_mirrors,
            bus_back_ref: Arc::downgrade(&bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::clone(&order_gate),
        };
        let reactor = tokio::spawn(run(init));
        MirrorRig {
            bus,
            factory,
            mirror,
            public_state,
            terminated,
            order_gate,
            reactor,
        }
    }

    fn register(rig: &MirrorRig, channel: ChannelName, pair: &str, params: SubscribeParams) {
        register_batch(
            &rig.bus,
            WsUrl::Public,
            vec![SubscriptionEntry::new(
                WsUrl::Public,
                channel,
                Some(sym(pair)),
                params,
            )],
        );
    }

    async fn wait_for_row(
        mirror: &SubscriptionMirror,
        channel: ChannelName,
        pair: Option<&str>,
        want: Option<fn(&EntrySubState) -> bool>,
        what: &str,
    ) {
        let key = (WsUrl::Public, channel, pair.map(sym));
        let poll = async {
            loop {
                let hit = {
                    let m = mirror.read().expect("mirror lock");
                    match (&want, m.get(&key)) {
                        (Some(pred), Some(row)) => pred(&row.state),
                        (None, None) => true,
                        _ => false,
                    }
                };
                if hit {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: {what}"));
    }
    #[tokio::test]
    async fn mirror_tracks_pending_acked_and_reject_tombstone() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let s = socket_at(&rig.factory, 0).await;
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("BTC/USD"),
            Some(|st| matches!(st, EntrySubState::Pending)),
            "BTC/USD row Pending",
        )
        .await;

        s.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("BTC/USD"),
            Some(|st| matches!(st, EntrySubState::Acked)),
            "BTC/USD row Acked after ack",
        )
        .await;

        s.emit_text(reject_text(
            "ticker",
            Some("ETH/USD"),
            "Currency pair not supported ETH/USD",
        ));
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("ETH/USD"),
            Some(|st| {
                matches!(
                    st,
                    EntrySubState::Terminated {
                        cause:
                            crate::api::subscription::TerminationCause::NonTransientWireRejection,
                        ..
                    }
                )
            }),
            "ETH/USD tombstone after non-transient reject",
        )
        .await;
        assert_eq!(rig.mirror.read().unwrap().len(), 2);
        assert_eq!(rig.terminated.load(Ordering::Relaxed), 1);

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    #[tokio::test]
    async fn deregister_all_fans_out_per_entry_with_discriminator_echo() {
        let rig = spawn_mirror_rig();
        let causes = Arc::new(std::sync::Mutex::new(Vec::new()));
        {
            let c = Arc::clone(&causes);
            let _ = rig.bus.subscribe(
                crate::dispatch::EventType::SubscriptionTerminatedEvent,
                Arc::new(move |env: &crate::dispatch::EventEnvelope| {
                    if let crate::dispatch::EventPayload::SubscriptionTerminatedEvent {
                        cause,
                        ..
                    } = &env.payload
                    {
                        c.lock().unwrap().push(*cause);
                    }
                }),
                u16::MAX,
            );
        }
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        register(
            &rig,
            ChannelName::Book,
            "BTC/USD",
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        );
        register(
            &rig,
            ChannelName::Ohlc,
            "ETH/USD",
            SubscribeParams::Ohlc {
                interval: OhlcInterval::M5,
                snapshot: None,
                tick_type: None,
            },
        );
        let s = socket_at(&rig.factory, 0).await;
        let _ = wait_for_frames(&s, "\"subscribe\"", 3, "three wire subscribes").await;

        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Public,
                mutation: RegistryMutationOp::DeregisterAll { channel: None },
            })
            .expect("post deregister-all");

        let unsubs = wait_for_frames(&s, "unsubscribe", 3, "three wire unsubscribes").await;
        let book_unsub = unsubs
            .iter()
            .find(|t| t.contains("\"book\""))
            .expect("book unsubscribe");
        assert!(
            book_unsub.contains("\"depth\":25"),
            "book unsubscribe must echo wire depth 25, got {book_unsub}"
        );
        assert!(
            !book_unsub.contains("snapshot"),
            "unsubscribe must not carry snapshot: {book_unsub}"
        );
        let ohlc_unsub = unsubs
            .iter()
            .find(|t| t.contains("\"ohlc\""))
            .expect("ohlc unsubscribe");
        assert!(
            ohlc_unsub.contains("\"interval\":5"),
            "ohlc unsubscribe must echo interval 5, got {ohlc_unsub}"
        );

        let poll = async {
            loop {
                if rig.terminated.load(Ordering::Relaxed) >= 3 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: three terminated events");
        {
            let got = causes.lock().unwrap();
            assert_eq!(got.len(), 3);
            assert!(
                got.iter()
                    .all(|c| matches!(c, crate::api::subscription::TerminationCause::ClientClosed)),
                "forced teardown must emit cause ClientClosed: {got:?}"
            );
        }
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("BTC/USD"),
            None,
            "ticker row removed",
        )
        .await;
        assert!(rig.mirror.read().unwrap().is_empty());

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    #[tokio::test]
    async fn deregister_all_channel_scoped_normalizes_bookraw() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        register(
            &rig,
            ChannelName::Book,
            "BTC/USD",
            SubscribeParams::Book {
                depth: BookDepth::D10,
            },
        );
        let s = socket_at(&rig.factory, 0).await;
        let _ = wait_for_frames(&s, "\"subscribe\"", 2, "two wire subscribes").await;

        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Public,
                mutation: RegistryMutationOp::DeregisterAll {
                    channel: Some(ChannelName::BookRaw),
                },
            })
            .expect("post deregister-all bookraw");

        let unsubs = wait_for_frames(&s, "unsubscribe", 1, "one wire unsubscribe").await;
        assert_eq!(unsubs.len(), 1, "only the shared Book entry tears down");
        assert!(unsubs[0].contains("\"book\""));
        wait_for_row(
            &rig.mirror,
            ChannelName::Book,
            Some("BTC/USD"),
            None,
            "book row removed",
        )
        .await;
        assert!(rig.mirror.read().unwrap().contains_key(&(
            WsUrl::Public,
            ChannelName::Ticker,
            Some(sym("BTC/USD"))
        )));

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    #[tokio::test]
    async fn stale_guard_drop_after_deregister_all_is_noop() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        let s = socket_at(&rig.factory, 0).await;
        let _ = wait_for_frames(&s, "\"subscribe\"", 1, "wire subscribe").await;

        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Public,
                mutation: RegistryMutationOp::DeregisterAll { channel: None },
            })
            .expect("post deregister-all");
        let _ = wait_for_frames(&s, "unsubscribe", 1, "wire unsubscribe").await;

        rig.bus
            .try_post_caller_inbound(CallerInbound::SubscriptionGuardDrop {
                handler_id: crate::dispatch::HandlerId(99),
                url: WsUrl::Public,
                channel: ChannelName::Ticker,
                symbols: vec![sym("BTC/USD")],
            })
            .expect("post stale guard drop");

        let settle = async {
            loop {
                tokio::task::yield_now().await;
                if rig.terminated.load(Ordering::Relaxed) > 1 {
                    return false;
                }
                if s.sent_text()
                    .iter()
                    .filter(|t| t.contains("unsubscribe"))
                    .count()
                    > 1
                {
                    return false;
                }
            }
        };
        let ok = tokio::time::timeout(Duration::from_millis(300), settle)
            .await
            .unwrap_or(true);
        assert!(ok, "stale guard drop produced a duplicate teardown");
        assert_eq!(rig.terminated.load(Ordering::Relaxed), 1);
        assert_eq!(
            s.sent_text()
                .iter()
                .filter(|t| t.contains("unsubscribe"))
                .count(),
            1
        );

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    #[tokio::test]
    async fn reconnect_replay_preserves_acked_state() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        let s1 = socket_at(&rig.factory, 0).await;
        s1.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("BTC/USD"),
            Some(|st| matches!(st, EntrySubState::Acked)),
            "Acked before reconnect",
        )
        .await;

        s1.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        rig.bus
            .try_post_caller_inbound(CallerInbound::FsmEvent {
                url: WsUrl::Public,
                event: crate::types::CallerEvent::ForceReconnect { request_id: 7 },
            })
            .expect("post force reconnect");

        let s2 = {
            let poll = async {
                loop {
                    if rig.factory.created_count() >= 2 {
                        return rig.factory.handle(1).expect("socket2");
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: reconnect socket")
        };
        let _ = wait_for_frames(&s2, "\"subscribe\"", 1, "replayed subscribe").await;
        {
            let m = rig.mirror.read().unwrap();
            let row = m
                .get(&(WsUrl::Public, ChannelName::Ticker, Some(sym("BTC/USD"))))
                .expect("row survives reconnect");
            assert!(matches!(row.state, EntrySubState::Acked));
        }
        s2.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(&rig.public_state, ConnectionState::Open, "reopen").await;
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("BTC/USD"),
            Some(|st| matches!(st, EntrySubState::Acked)),
            "Acked after replay re-ack",
        )
        .await;

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }
    #[tokio::test]
    async fn tombstone_revival_reemits_wire_subscribe_and_acks() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let s = socket_at(&rig.factory, 0).await;
        let _ = wait_for_frames(&s, "\"subscribe\"", 1, "initial wire subscribe").await;
        s.emit_text(reject_text(
            "ticker",
            Some("ETH/USD"),
            "Currency pair not supported ETH/USD",
        ));
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("ETH/USD"),
            Some(|st| matches!(st, EntrySubState::Terminated { .. })),
            "tombstone after reject",
        )
        .await;

        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let subs = wait_for_frames(&s, "\"subscribe\"", 2, "revival wire subscribe").await;
        assert_eq!(subs.len(), 2, "revival must re-emit the wire frame");
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("ETH/USD"),
            Some(|st| matches!(st, EntrySubState::Pending)),
            "revived row Pending",
        )
        .await;
        s.emit_text(ack_text("ticker", Some("ETH/USD")));
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("ETH/USD"),
            Some(|st| matches!(st, EntrySubState::Acked)),
            "revived row Acked after ack",
        )
        .await;
        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Public,
                mutation: RegistryMutationOp::Deregister {
                    channel: ChannelName::Ticker,
                    pair: Some(sym("ETH/USD")),
                },
            })
            .expect("post deregister");
        let _ = wait_for_frames(&s, "unsubscribe", 1, "single unsubscribe tears down").await;
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("ETH/USD"),
            None,
            "revived row cleared by one unsubscribe",
        )
        .await;

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    #[tokio::test]
    async fn open_state_rate_limit_reject_retries_and_completes() {
        // Default ack timeout on purpose: the reject must land ARMED.
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        let s = socket_at(&rig.factory, 0).await;
        s.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(&rig.public_state, ConnectionState::Open, "open").await;

        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let _ = wait_for_frames(&s, "ETH/USD", 1, "late-add wire subscribe").await;
        s.emit_text(reject_text(
            "ticker",
            Some("ETH/USD"),
            "EGeneral:Too many requests",
        ));
        let _ = wait_for_frames(&s, "ETH/USD", 2, "transient-reject resend").await;
        s.emit_text(ack_text("ticker", Some("ETH/USD")));
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("ETH/USD"),
            Some(|st| matches!(st, EntrySubState::Acked)),
            "transient Open-state reject retries to Acked, never Terminated",
        )
        .await;
        assert_eq!(
            rig.terminated.load(Ordering::Relaxed),
            0,
            "no terminated event for a transient reject in Open"
        );

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    #[tokio::test]
    async fn reject_in_closing_after_close_neither_tombstones_nor_emits() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        let s = socket_at(&rig.factory, 0).await;
        s.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(&rig.public_state, ConnectionState::Open, "open").await;

        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let _ = wait_for_frames(&s, "ETH/USD", 1, "late-add wire subscribe").await;

        rig.bus
            .try_post_caller_inbound(CallerInbound::FsmEvent {
                url: WsUrl::Public,
                event: crate::types::CallerEvent::Close { request_id: 21 },
            })
            .expect("post close");
        wait_for_state(&rig.public_state, ConnectionState::Closing, "closing").await;
        s.emit_text(reject_text(
            "ticker",
            Some("ETH/USD"),
            "EGeneral:Permission denied",
        ));
        s.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        wait_for_state(&rig.public_state, ConnectionState::Closed, "closed").await;

        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("ETH/USD"),
            Some(|st| matches!(st, EntrySubState::Pending)),
            "reject in Closing must not tombstone (unarmed drop)",
        )
        .await;
        assert_eq!(rig.terminated.load(Ordering::Relaxed), 0);

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    async fn wait_for_flag(gate: &Arc<crate::conn::OrderGate>, want: bool, what: &str) {
        let poll = async {
            loop {
                if gate.has_subscriptions(WsUrl::Auth) == want {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: {what}"));
    }

    #[tokio::test]
    async fn guard_drop_after_forced_teardown_consumes_ref_record() {
        let (bus, factory) = bus_and_factory();
        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(&factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let mut conns = HashMap::new();
        for (url, seed) in [(WsUrl::Public, 1u64), (WsUrl::Auth, 2u64)] {
            conns.insert(
                url,
                ManagedConnection::new(
                    url,
                    Arc::clone(&bus),
                    Arc::clone(&dyn_factory),
                    Arc::clone(&rate_budget),
                    Arc::new(crate::clock::SystemClock) as Arc<dyn crate::clock::Clock>,
                    Arc::new(crate::jitter::SplitMix64Jitter::with_seed(seed)),
                ),
            );
        }
        let presence: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        let mut handler_registry = crate::dispatch::HandlerRegistry::new(presence);
        let mut registry = SubscriptionRegistry::new();
        let mut sockets: HashMap<WsUrl, Arc<dyn WsSocket>> = HashMap::new();
        let mut upgrade_guards = HashMap::new();
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        for url in [WsUrl::Public, WsUrl::Auth] {
            state_mirrors.insert(url, Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())));
        }
        let (upgrade_tx, _upgrade_rx) = tokio::sync::mpsc::channel(8);
        let alloc = Arc::new(AtomicU64::new(1));
        let auth_stack = Arc::new(crate::auth::AuthStack::new(
            None,
            None,
            Arc::new(crate::auth::SystemClockNonceSource::new()),
            std::collections::HashMap::new(),
            crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
        ));
        let order_gate = Arc::new(crate::conn::OrderGate::default());
        let weak = Arc::downgrade(&bus);

        macro_rules! drive {
            ($msg:expr) => {
                super::super::inbound_dispatch::handle_inbound(
                    $msg,
                    &mut conns,
                    &mut registry,
                    &mut handler_registry,
                    &mut sockets,
                    super::super::SharedHandles {
                        auth_stack: &auth_stack,
                        order_gate: &order_gate,
                        bus_back_ref: &weak,
                        state_mirrors: &state_mirrors,
                        upgrade_tx: &upgrade_tx,
                        connect_id_allocator: &alloc,
                    },
                    &mut upgrade_guards,
                )
                .await
            };
        }

        drive!(CallerInbound::RegistryMutation {
            url: WsUrl::Public,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: Some(crate::dispatch::HandlerId(7)),
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Public,
                    ChannelName::Ticker,
                    Some(sym("BTC/USD")),
                    ticker_params(),
                )],
            },
        });
        assert_eq!(registry.guard_ref_count(), 1, "ref recorded at register");
        drive!(CallerInbound::RegistryMutation {
            url: WsUrl::Public,
            mutation: RegistryMutationOp::DeregisterAll { channel: None },
        });
        drive!(CallerInbound::SubscriptionGuardDrop {
            handler_id: crate::dispatch::HandlerId(7),
            url: WsUrl::Public,
            channel: ChannelName::Ticker,
            symbols: vec![sym("BTC/USD")],
        });
        assert_eq!(
            registry.guard_ref_count(),
            0,
            "guard drop consumes its record even though the key is already gone"
        );
    }

    #[tokio::test]
    async fn auth_hint_tracks_liveness_through_loop_sync() {
        let rig = spawn_mirror_rig();
        assert!(!rig.order_gate.has_subscriptions(WsUrl::Auth));
        register_batch(
            &rig.bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Executions,
                None,
                SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
            )],
        );
        wait_for_flag(&rig.order_gate, true, "hint true after auth register").await;
        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Auth,
                mutation: RegistryMutationOp::Deregister {
                    channel: ChannelName::Executions,
                    pair: None,
                },
            })
            .expect("post single deregister");
        wait_for_flag(
            &rig.order_gate,
            false,
            "hint false after single-key deregister",
        )
        .await;
        register_batch(
            &rig.bus,
            WsUrl::Auth,
            vec![SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                SubscribeParams::Balances { snapshot: None },
            )],
        );
        wait_for_flag(&rig.order_gate, true, "hint true again").await;
        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Auth,
                mutation: RegistryMutationOp::DeregisterAll { channel: None },
            })
            .expect("post deregister-all");
        wait_for_flag(&rig.order_gate, false, "hint false after teardown").await;
        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    #[tokio::test]
    async fn last_entry_reject_mid_resubscribing_short_circuits_to_open() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let s1 = socket_at(&rig.factory, 0).await;
        s1.emit_text(ack_text("ticker", Some("BTC/USD")));
        s1.emit_text(ack_text("ticker", Some("ETH/USD")));
        wait_for_state(&rig.public_state, ConnectionState::Open, "first open").await;

        s1.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        rig.bus
            .try_post_caller_inbound(CallerInbound::FsmEvent {
                url: WsUrl::Public,
                event: crate::types::CallerEvent::ForceReconnect { request_id: 31 },
            })
            .expect("post force reconnect");
        let s2 = {
            let poll = async {
                loop {
                    if rig.factory.created_count() >= 2 {
                        return rig.factory.handle(1).expect("socket2");
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: reconnect socket")
        };
        let _ = wait_for_frames(&s2, "\"subscribe\"", 2, "both replayed subscribes").await;

        s2.emit_text(ack_text("ticker", Some("BTC/USD")));
        s2.emit_text(reject_text(
            "ticker",
            Some("ETH/USD"),
            "EGeneral:Permission denied",
        ));

        wait_for_state(
            &rig.public_state,
            ConnectionState::Open,
            "reopen after last-entry reject",
        )
        .await;
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("ETH/USD"),
            Some(|st| matches!(st, EntrySubState::Terminated { .. })),
            "rejected entry tombstoned in lockstep",
        )
        .await;
        assert_eq!(rig.terminated.load(Ordering::Relaxed), 1);

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    #[tokio::test]
    async fn deregister_all_mid_resubscribing_short_circuits_to_open() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        let s1 = socket_at(&rig.factory, 0).await;
        s1.emit_text(ack_text("ticker", Some("BTC/USD")));
        wait_for_state(&rig.public_state, ConnectionState::Open, "first open").await;

        s1.drop_with(crate::transport::TransportError {
            kind: crate::transport::TransportErrorKind::SocketReset,
            transient: true,
        });
        rig.bus
            .try_post_caller_inbound(CallerInbound::FsmEvent {
                url: WsUrl::Public,
                event: crate::types::CallerEvent::ForceReconnect { request_id: 11 },
            })
            .expect("post force reconnect");
        let s2 = {
            let poll = async {
                loop {
                    if rig.factory.created_count() >= 2 {
                        return rig.factory.handle(1).expect("socket2");
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: reconnect socket")
        };
        let _ = wait_for_frames(&s2, "\"subscribe\"", 1, "replayed subscribe").await;
        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Public,
                mutation: RegistryMutationOp::DeregisterAll { channel: None },
            })
            .expect("post deregister-all");
        wait_for_state(
            &rig.public_state,
            ConnectionState::Open,
            "short-circuit to Open after forced teardown mid-replay",
        )
        .await;

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    #[tokio::test]
    async fn deregister_all_skips_tombstones_silently() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let s = socket_at(&rig.factory, 0).await;
        s.emit_text(ack_text("ticker", Some("BTC/USD")));
        s.emit_text(reject_text(
            "ticker",
            Some("ETH/USD"),
            "Currency pair not supported ETH/USD",
        ));
        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("ETH/USD"),
            Some(|st| matches!(st, EntrySubState::Terminated { .. })),
            "tombstone before fan-out",
        )
        .await;
        assert_eq!(
            rig.terminated.load(Ordering::Relaxed),
            1,
            "reject event only"
        );

        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Public,
                mutation: RegistryMutationOp::DeregisterAll { channel: None },
            })
            .expect("post deregister-all");
        let unsubs = wait_for_frames(&s, "unsubscribe", 1, "live-entry unsubscribe").await;
        assert!(
            unsubs[0].contains("BTC/USD"),
            "teardown targets the live entry"
        );
        let settle = async {
            loop {
                tokio::task::yield_now().await;
                if rig.terminated.load(Ordering::Relaxed) > 2 {
                    return false;
                }
                if s.sent_text()
                    .iter()
                    .filter(|t| t.contains("unsubscribe"))
                    .count()
                    > 1
                {
                    return false;
                }
            }
        };
        let ok = tokio::time::timeout(Duration::from_millis(300), settle)
            .await
            .unwrap_or(true);
        assert!(ok, "tombstone produced a junk teardown");
        assert_eq!(rig.terminated.load(Ordering::Relaxed), 2);
        assert!(rig.mirror.read().unwrap().is_empty());

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    #[tokio::test]
    async fn single_key_teardown_of_tombstone_is_silent() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        register(
            &rig,
            ChannelName::Trade,
            "BTC/USD",
            SubscribeParams::Trade { snapshot: None },
        );
        let s = socket_at(&rig.factory, 0).await;
        let _ = wait_for_frames(&s, "\"subscribe\"", 2, "two wire subscribes").await;
        s.emit_text(reject_text(
            "ticker",
            Some("BTC/USD"),
            "Currency pair not supported BTC/USD",
        ));
        s.emit_text(reject_text(
            "trade",
            Some("BTC/USD"),
            "Currency pair not supported BTC/USD",
        ));
        for ch in [ChannelName::Ticker, ChannelName::Trade] {
            wait_for_row(
                &rig.mirror,
                ch,
                Some("BTC/USD"),
                Some(|st| matches!(st, EntrySubState::Terminated { .. })),
                "tombstone",
            )
            .await;
        }
        assert_eq!(
            rig.terminated.load(Ordering::Relaxed),
            2,
            "reject events only"
        );

        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Public,
                mutation: RegistryMutationOp::Deregister {
                    channel: ChannelName::Ticker,
                    pair: Some(sym("BTC/USD")),
                },
            })
            .expect("post deregister");
        rig.bus
            .try_post_caller_inbound(CallerInbound::SubscriptionGuardDrop {
                handler_id: crate::dispatch::HandlerId(41),
                url: WsUrl::Public,
                channel: ChannelName::Trade,
                symbols: vec![sym("BTC/USD")],
            })
            .expect("post guard drop");

        wait_for_row(
            &rig.mirror,
            ChannelName::Ticker,
            Some("BTC/USD"),
            None,
            "ticker row gone",
        )
        .await;
        wait_for_row(
            &rig.mirror,
            ChannelName::Trade,
            Some("BTC/USD"),
            None,
            "trade row gone",
        )
        .await;
        let settle = async {
            loop {
                tokio::task::yield_now().await;
                if rig.terminated.load(Ordering::Relaxed) > 2 {
                    return false;
                }
                if s.sent_text().iter().any(|t| t.contains("unsubscribe")) {
                    return false;
                }
            }
        };
        let ok = tokio::time::timeout(Duration::from_millis(300), settle)
            .await
            .unwrap_or(true);
        assert!(
            ok,
            "tombstone single-key teardown produced a junk frame or duplicate event"
        );
        assert_eq!(rig.terminated.load(Ordering::Relaxed), 2);
        assert!(!s.sent_text().iter().any(|t| t.contains("unsubscribe")));

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }
}

mod teardown_flush_reactor_tests {
    use super::*;
    use crate::conn::rate_budget::ConnectionRateBudget;
    use crate::dispatch::handler_registry::{
        HandlerCallback, HandlerHandle, HandlerRegistry, PresenceMirror, presence_has_handlers,
    };
    use crate::dispatch::{CallerInbound, HandlerId, HandlerMutationOp};
    use std::collections::HashMap;
    use std::sync::RwLock;

    #[tokio::test]
    async fn teardown_flush_driven_by_reactor_loop() {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig {
                caller_to_io_capacity: 2,
                ..DispatchEventBusConfig::defaults()
            },
            clock,
        ));
        let presence: PresenceMirror = Arc::new(RwLock::new(HashMap::new()));
        let mut handler_registry = HandlerRegistry::new(Arc::clone(&presence));
        let id = handler_registry.register(ChannelName::Ticker, HandlerCallback::noop());
        assert!(presence_has_handlers(
            &presence,
            WsUrl::Public,
            ChannelName::Ticker
        ));

        for filler in [900u64, 901] {
            bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
                url: WsUrl::Public,
                channel: ChannelName::Ticker,
                op: HandlerMutationOp::Deregister {
                    id: HandlerId(filler),
                },
            })
            .expect("filler");
        }
        drop(HandlerHandle::new(
            id,
            WsUrl::Public,
            ChannelName::Ticker,
            Arc::downgrade(&bus),
        ));
        assert!(
            presence_has_handlers(&presence, WsUrl::Public, ChannelName::Ticker),
            "deregister still queued — handler must remain until reactor flushes"
        );

        let factory = Arc::new(DriveableWsSocketFactory::new(Arc::clone(&bus)));
        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(&factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let public_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        state_mirrors.insert(WsUrl::Public, Arc::clone(&public_mirror));
        state_mirrors.insert(
            WsUrl::Auth,
            Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())),
        );
        let mut conns = HashMap::new();
        for (url, seed) in [(WsUrl::Public, 1u64), (WsUrl::Auth, 2u64)] {
            conns.insert(
                url,
                ManagedConnection::new(
                    url,
                    Arc::clone(&bus),
                    Arc::clone(&dyn_factory),
                    Arc::clone(&rate_budget),
                    Arc::new(SystemClock) as Arc<dyn crate::clock::Clock>,
                    Arc::new(crate::jitter::SplitMix64Jitter::with_seed(seed)),
                ),
            );
        }
        let caller_rx = bus.take_caller_to_io_rx().expect("rx");
        let reactor = tokio::spawn(run(IoReactorInit {
            conns,
            caller_rx,
            registry: SubscriptionRegistry::new(),
            handler_registry,
            auth_stack: Arc::new(crate::auth::AuthStack::new(
                None,
                None,
                Arc::new(crate::auth::SystemClockNonceSource::new()),
                HashMap::new(),
                crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
            )),
            state_mirrors,
            bus_back_ref: Arc::downgrade(&bus),
            ready_signal: ReadySignal {
                request_id: 0,
                capability_snapshot: empty_snapshot(),
            },
            connect_id_allocator: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::new(crate::conn::OrderGate::default()),
        }));

        let cleared = async {
            loop {
                if !presence_has_handlers(&presence, WsUrl::Public, ChannelName::Ticker) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, cleared)
            .await
            .expect("timeout: reactor never flushed the queued HandlerHandle Drop");

        reactor.abort();
    }
}
