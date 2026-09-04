use super::*;
use crate::clock::{Clock, SystemClock};
use crate::dispatch::DispatchEventBusConfig;
use crate::transport::WsSocketFactoryLike;
use crate::transport::driveable_mock::DriveableWsSocketFactory;
use crate::types::Symbol;
use std::time::Duration;

const BUDGET: Duration = Duration::from_millis(1500);

fn sym(s: &str) -> Symbol {
    Symbol::new(s).expect("valid symbol")
}

/// Build a bus with the dispatch reactor running + a driveable factory wired to it.
fn bus_and_factory() -> (Arc<DispatchEventBus>, Arc<DriveableWsSocketFactory>) {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
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

/// Poll the mirror until `target` (bounded); each poll yields so the reactor progresses.
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

type HttpMockFn = Box<dyn Fn() -> Result<Value, crate::transport::TransportError> + Send + Sync>;

/// HttpTransport double: each method runs a supplied closure (request args ignored).
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

    /// Both methods return `Ok(envelope.clone())`.
    pub(super) fn canned(envelope: serde_json::Value) -> Self {
        let g = envelope.clone();
        Self::new(
            Box::new(move || Ok(g.clone())),
            Box::new(move || Ok(envelope.clone())),
        )
    }

    /// Both methods panic — proves no REST fallback was taken on the order path.
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
    use super::*;
    use crate::conn::ManagedConnection;
    use crate::conn::rate_budget::ConnectionRateBudget;
    use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
    use crate::dispatch::event_bus::{CallerInbound, RegistryMutationOp};
    use crate::transport::{TransportError, TransportErrorKind};
    use crate::types::{ChannelName, ConnectionState, WsUrl};
    use std::time::Duration;

    const BUDGET: Duration = Duration::from_millis(1500);

    /// A subscribe-ack text frame in the shape `handle_sub_ack` parses.
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
            auth_has_subscriptions: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            auth_send_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let reactor = tokio::spawn(run(init));

        for s in ["ETH/USD", "BTC/USD"] {
            bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Public,
                mutation: RegistryMutationOp::RegisterBatch {
                    ref_id: None,
                    entries: vec![SubscriptionEntry::new(
                        WsUrl::Public,
                        ChannelName::Ticker,
                        Some(sym(s)),
                        SubscribeParams::Ticker {
                            snapshot: None,
                            event_trigger: None,
                        },
                    )],
                },
            })
            .expect("post register");
        }

        let s1 = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: socket1 never opened")
        };

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

    /// A backoff-timer reconnect must wire the fresh socket's upgrade bridge,
    /// else the FSM wedges in `Connecting`.
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
            auth_has_subscriptions: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            auth_send_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let reactor = tokio::spawn(run(init));

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Public,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Public,
                    ChannelName::Ticker,
                    Some(sym("BTC/USD")),
                    SubscribeParams::Ticker {
                        snapshot: None,
                        event_trigger: None,
                    },
                )],
            },
        })
        .expect("post register");

        let s1 = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: socket1 never opened")
        };

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

    /// force_reconnect FROM `Open` goes via `drive_wire_close`, which must wire
    /// the fresh socket's upgrade bridge.
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
            auth_has_subscriptions: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            auth_send_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let reactor = tokio::spawn(run(init));

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Public,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Public,
                    ChannelName::Ticker,
                    Some(sym("BTC/USD")),
                    SubscribeParams::Ticker {
                        snapshot: None,
                        event_trigger: None,
                    },
                )],
            },
        })
        .expect("post register");

        let s1 = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: socket1 never opened")
        };

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

    const BUDGET: Duration = Duration::from_millis(1500);

    /// Spawn a real reactor (Public + Auth MCs) on a driveable factory.
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
            auth_has_subscriptions: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            auth_send_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        (tokio::spawn(run(init)), public_mirror)
    }

    pub(super) fn register_public(
        bus: &Arc<DispatchEventBus>,
        channel: ChannelName,
        pair: &str,
        params: SubscribeParams,
    ) {
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Public,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Public,
                    channel,
                    Some(sym(pair)),
                    params,
                )],
            },
        })
        .expect("post register");
    }

    /// Wait until socket0 has been opened by the FSM connect arm.
    pub(super) async fn await_socket0(
        factory: &Arc<DriveableWsSocketFactory>,
    ) -> crate::transport::driveable_mock::DriveableSocketHandle {
        let poll = async {
            loop {
                if let Some(h) = factory.handle(0) {
                    return h;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: socket never opened")
    }

    /// Count the `book` subscribe frames the socket has sent.
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

    /// Registering the SAME (Book, BTC/USD) twice emits EXACTLY ONE wire
    /// subscribe (0->1 edge only); the single ack drives Resubscribing->Open.
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
        let sock = await_socket0(&factory).await;
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

    /// A caller-unsubscribe drives refcount to 0 and emits a wire `unsubscribe`
    /// echoing the subscribe depth (D10 -> 25).
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
        let sock = await_socket0(&factory).await;
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

    /// Guard-drop leg of the depth-echo unsubscribe (D10 -> 25).
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
        let sock = await_socket0(&factory).await;
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

    /// A MULTI-pair guard drop posts ONE `SubscriptionGuardDrop` and tears down
    /// ALL pairs from that single post.
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
        let sock = await_socket0(&factory).await;
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

    /// ONE `RegisterBatch` of three entries registers all three atomically —
    /// three wire subscribes, no partial-registration leak.
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
                    SubscribeParams::Ticker {
                        snapshot: None,
                        event_trigger: None,
                    },
                )
            })
            .collect();
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Public,
            mutation: RegistryMutationOp::RegisterBatch {
                entries,
                ref_id: None,
            },
        })
        .expect("post register batch");

        let sock = await_socket0(&factory).await;
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
    use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
    use crate::dispatch::DispatchEventBus;
    use crate::dispatch::event_bus::{CallerInbound, RegistryMutationOp};
    use crate::transport::driveable_mock::{
        DriveableSocketHandle, DriveableWsSocketFactory, SendFailMode,
    };
    use crate::types::{ChannelName, ConnectionState, WsUrl};
    use std::time::Duration;

    /// 2.5s budget — wide enough to absorb the 200ms resend backoff + retry chain.
    const BUDGET: Duration = Duration::from_millis(2500);

    /// Spawn the reactor with the public connection only.
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
            auth_has_subscriptions: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            auth_send_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        (tokio::spawn(run(init)), public_mirror)
    }

    fn register_ticker(bus: &Arc<DispatchEventBus>, pair: &str) {
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Public,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Public,
                    ChannelName::Ticker,
                    Some(sym(pair)),
                    SubscribeParams::Ticker {
                        snapshot: None,
                        event_trigger: None,
                    },
                )],
            },
        })
        .expect("post register");
    }

    /// Wait until socket at `index` has been opened by the factory.
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

    /// Bring the connection to Open with a single BTC/USD ticker subscription.
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

    /// A replay whose backpressured send exhausts its budget (-> BackingOff)
    /// must stop, not send the rest on the abandoned socket.
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

    /// A `RegisterBatch` whose first send exhausts its budget must not send the
    /// rest on the abandoned socket, yet still register them for reconnect replay.
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
                SubscribeParams::Ticker {
                    snapshot: None,
                    event_trigger: None,
                },
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
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Public,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![ticker_entry("ETH/USD"), ticker_entry("XRP/USD")],
            },
        })
        .expect("post register batch");

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

    /// Transient backpressure arms a resend timer; clearing the failure before
    /// it fires lets the resend succeed and the connection stays Open.
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

    /// Permanent backpressure exhausts the resend budget across retries,
    /// tearing the connection down to BackingOff.
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

    /// A resend that finds the writer closed synthesises `WireAbnormalClose`
    /// -> BackingOff.
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

    /// A backpressured subscribe mid-Resubscribing recovers via its resend timer
    /// and the last ack drives Open — no reconnect required.
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
    use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
    use crate::dispatch::DispatchEventBus;
    use crate::dispatch::event_bus::{CallerInbound, RegistryMutationOp};
    use crate::transport::HttpTransport;
    use crate::transport::driveable_mock::DriveableWsSocketFactory;
    use crate::types::{ChannelName, ConnectionState, WsUrl};
    use std::time::Duration;

    const BUDGET: Duration = Duration::from_millis(1500);

    /// A success subscribe-ack frame in the shape `handle_sub_ack` parses.
    pub(super) fn ack_ok(channel: &str) -> String {
        serde_json::json!({
            "method": "subscribe",
            "success": true,
            "result": { "channel": channel },
        })
        .to_string()
    }

    /// A failure subscribe-ack frame carrying a Kraken error string.
    pub(super) fn ack_err(channel: &str, error: &str) -> String {
        serde_json::json!({
            "method": "subscribe",
            "success": false,
            "error": error,
            "result": { "channel": channel },
        })
        .to_string()
    }

    /// AuthStack (seeded cached token) whose token-lifecycle RestSurface is
    /// backed by `transport`; the returned RestSurface Arc must outlive the caller.
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

    /// The RestSurface returns `refresh_envelope` on every call.
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

    /// Spawn `run()` with an auth + public MC, wired to the driveable factory.
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

    /// `spawn_reactor` with a caller-supplied subscription mirror.
    fn spawn_reactor_with_sub_mirror(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
        auth_stack: Arc<crate::auth::AuthStack>,
        sub_mirror: crate::conn::subscription_registry::SubscriptionMirror,
    ) -> (tokio::task::JoinHandle<()>, Arc<AtomicU8>) {
        let (reactor, auth_mirror, _hint) =
            spawn_reactor_with_sub_mirror_and_hint(bus, factory, auth_stack, sub_mirror);
        (reactor, auth_mirror)
    }

    /// Also returns the cached auth-has-subscriptions hint flag.
    fn spawn_reactor_with_sub_mirror_and_hint(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
        auth_stack: Arc<crate::auth::AuthStack>,
        sub_mirror: crate::conn::subscription_registry::SubscriptionMirror,
    ) -> (
        tokio::task::JoinHandle<()>,
        Arc<AtomicU8>,
        Arc<std::sync::atomic::AtomicBool>,
    ) {
        let auth_has_subscriptions = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dyn_factory: Arc<dyn WsSocketFactoryLike> = Arc::clone(factory) as _;
        let rate_budget = Arc::new(ConnectionRateBudget::new());
        let auth_mirror = Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8()));
        let mut state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        state_mirrors.insert(
            WsUrl::Public,
            Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())),
        );
        state_mirrors.insert(WsUrl::Auth, Arc::clone(&auth_mirror));

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
            auth_has_subscriptions: Arc::clone(&auth_has_subscriptions),
            auth_send_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        (tokio::spawn(run(init)), auth_mirror, auth_has_subscriptions)
    }

    /// The `params.token` of the n-th frame the auth socket has sent (bounded poll).
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

    /// Assert the sent-frame count is EXACTLY `expected` through a settle
    /// window, so a double-send regression surfaces without racing a frame in flight.
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
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-A",
            serde_json::json!({ "error": [], "result": { "token": "unused", "expires": 900u32 } }),
        );
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");

        let auth_sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
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

    /// The handshake-consumed first signed subscribe ack must also drive the
    /// mirror row Pending → Acked (no later ack ever covers it).
    #[tokio::test]
    async fn first_signed_auth_ack_drives_mirror_row_acked() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-M",
            serde_json::json!({ "error": [], "result": { "token": "unused", "expires": 900u32 } }),
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) =
            spawn_reactor_with_sub_mirror(&bus, &factory, auth_stack, Arc::clone(&sub_mirror));

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");

        let auth_sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        {
            let m = sub_mirror.read().unwrap();
            let row = m
                .get(&(ChannelName::Executions, None))
                .expect("row registered");
            assert!(matches!(row.state, EntrySubState::Pending));
        }

        auth_sock.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(ChannelName::Executions, None))
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

    /// A guard drop with EMPTY `symbols` releases the channel-wide (`pair = None`)
    /// entry: symbol-less wire unsubscribe + terminated event + mirror row removed.
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
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-G",
            serde_json::json!({ "error": [], "result": { "token": "unused", "expires": 900u32 } }),
        );
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
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");

        let auth_sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
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
                let row_gone = !sub_mirror
                    .read()
                    .expect("mirror lock")
                    .contains_key(&(ChannelName::Executions, None));
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

    /// A token-miss deferral SURVIVES the short-circuit to Open and the refresh
    /// drain re-issues it.
    #[tokio::test]
    async fn token_miss_replay_deferral_survives_open_and_drains() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-D",
            serde_json::json!({ "error": [], "result": { "token": "fresh-token-D", "expires": 900u32 } }),
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        // Balances is the sorted-first golden probe; the executions replay leg
        // is where the token miss lands.
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Balances,
                        None,
                        SubscribeParams::Balances,
                    ),
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Executions,
                        None,
                        SubscribeParams::Executions,
                    ),
                ],
            },
        })
        .expect("post register");

        let auth_sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        let (channel, _token) = nth_sent_token(&auth_sock, 0).await;
        assert_eq!(channel, "balances", "sorted-first golden probe");

        // The token dies between the probe send and its ack: the remaining
        // (executions) composition must defer + kick refresh, not strand.
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
                    if m.get(&(ChannelName::Executions, None))
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

    /// An entry Acked on the PREVIOUS socket, deferred on a token miss, MUST be
    /// re-issued — a Pending-gated drain would silently strand it.
    #[tokio::test]
    async fn acked_last_session_deferral_reissues_after_reconnect() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-R",
            serde_json::json!({ "error": [], "result": { "token": "fresh-token-R", "expires": 900u32 } }),
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Balances,
                        None,
                        SubscribeParams::Balances,
                    ),
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Executions,
                        None,
                        SubscribeParams::Executions,
                    ),
                ],
            },
        })
        .expect("post register");
        let s1 = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
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
        // The token dies before the probe's ack, so the replay leg defers
        // executions — an entry still Acked from session 1.
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
                    if m.get(&(ChannelName::Executions, None))
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

    /// A force-removed entry's token-deferred subscribe is never resurrected by
    /// a later refresh drain.
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
                Ok(
                    serde_json::json!({ "error": [], "result": { "token": "fresh-token-Z", "expires": 900u32 } }),
                )
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

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances,
                )],
            },
        })
        .expect("post register");
        let sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
        let (channel, _) = nth_sent_token(&sock, 0).await;
        assert_eq!(channel, "balances");
        sock.emit_text(ack_ok("balances"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "open").await;

        auth_stack.token_lifecycle().clear_cached_token_for_test();
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");
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

        // Refresh #2 (success) must drain ONLY the new key — a surviving stale
        // record would put a subscribe for the removed executions on the wire.
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances,
                )],
            },
        })
        .expect("post register");
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
                    if m.get(&(ChannelName::Balances, None))
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

    /// A deferral satisfied by the handshake send must NOT survive to a later
    /// drain: re-subscribing a live entry draws the venue's terminal duplicate reject.
    #[tokio::test]
    async fn deferral_satisfied_by_handshake_send_is_not_reissued() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        use std::sync::atomic::AtomicUsize;
        let (bus, factory) = bus_and_factory();
        let calls = Arc::new(AtomicUsize::new(0));
        let (c1, c2) = (Arc::clone(&calls), Arc::clone(&calls));
        fn token_ok() -> Result<serde_json::Value, crate::transport::TransportError> {
            Ok(
                serde_json::json!({ "error": [], "result": { "token": "fresh-D2", "expires": 900u32 } }),
            )
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
        let sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        auth_stack.token_lifecycle().clear_cached_token_for_test();

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");
        let (channel, token) = nth_sent_token(&sock, 0).await;
        assert_eq!(channel, "executions", "handshake send carries the entry");
        assert_eq!(token, "fresh-D2", "sent with the refreshed token");
        sock.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(ChannelName::Executions, None))
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
                m.get(&(ChannelName::Executions, None))
                    .is_some_and(|row| matches!(row.state, EntrySubState::Acked)),
                "the live subscription stays Acked — never tombstoned by a duplicate reject"
            );
        }

        reactor.abort();
        bus.stop_reactors();
    }

    /// Ordering A: a late success between the ack timeout and the resend must
    /// kill the retry chain, or the drain re-subscribes a satisfied intent.
    #[tokio::test]
    async fn late_unarmed_success_before_resend_kills_the_retry_chain() {
        late_unarmed_success_drives_no_resubscribe(150).await;
    }

    /// Ordering B: the resend already deferred on a token miss when the late
    /// success lands — the swallow must drop the deferral record.
    #[tokio::test]
    async fn late_unarmed_success_after_defer_drops_the_deferral() {
        late_unarmed_success_drives_no_resubscribe(400).await;
    }

    /// A resend deferring on a token miss mid-Resubscribing must complete the
    /// FSM to Open — else a failed refresh parks the connection with no driver left.
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

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Balances,
                        None,
                        SubscribeParams::Balances,
                    ),
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Executions,
                        None,
                        SubscribeParams::Executions,
                    ),
                ],
            },
        })
        .expect("post register");
        let s1 = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
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
        let s2 = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(1) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket 2 never opened")
        };
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

    /// A deferred auth subscribe drains on the PROACTIVE tick's outcome, so a
    /// deferral whose reactive kick failed is not stranded until socket teardown.
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
        let sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");
        let (c1, _) = nth_sent_token(&sock, 0).await;
        sock.emit_text(ack_ok(&c1));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;

        auth_stack.token_lifecycle().clear_cached_token_for_test();
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances,
                )],
            },
        })
        .expect("post register");
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
                    if m.get(&(ChannelName::Balances, None))
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

    /// A proactive-tick refresh FAILURE mid-handshake must not drive the
    /// terminal-capable refresh arms; the handshake completes on its own ack.
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
        let sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");
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

    /// A late success clearing the LAST outstanding work mid-Resubscribing must
    /// complete the FSM to Open — a chain-kill with no short-circuit wedges it.
    #[tokio::test]
    async fn late_unarmed_success_mid_resubscribing_completes_to_open() {
        use crate::conn::subscription_registry::SubscriptionMirror;
        let (bus, factory) = bus_and_factory();
        bus.knobs()
            .subscribe_ack_timeout_ms
            .store(50, std::sync::atomic::Ordering::Relaxed);
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-W",
            serde_json::json!({ "error": [], "result": { "token": "fresh-token-W", "expires": 900u32 } }),
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror) = spawn_reactor_with_sub_mirror(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Balances,
                        None,
                        SubscribeParams::Balances,
                    ),
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Executions,
                        None,
                        SubscribeParams::Executions,
                    ),
                ],
            },
        })
        .expect("post register");
        let s1 = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
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
        let s2 = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(1) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket 2 never opened")
        };
        let (r1, _) = nth_sent_token(&s2, 0).await;
        s2.emit_text(ack_ok(&r1));
        let (r2, _) = nth_sent_token(&s2, 1).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        s2.emit_text(ack_ok(&r2));
        wait_for_state(&auth_mirror, ConnectionState::Open, "resubscribe completes").await;

        reactor.abort();
        bus.stop_reactors();
    }

    /// A late unarmed success ack must leave NOTHING behind that re-subscribes
    /// the live entry (that draws the venue's terminal duplicate reject).
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
        let sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");
        let (channel, _) = nth_sent_token(&sock, 0).await;
        assert_eq!(channel, "executions", "handshake send");
        sock.emit_text(ack_ok("executions"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "auth open").await;

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances,
                )],
            },
        })
        .expect("post register");
        let (channel, _) = nth_sent_token(&sock, 1).await;
        assert_eq!(channel, "balances", "steady-state send");
        // Kill the token BEFORE the ack timeout so the resend defers. The delay
        // picks the ordering: 150ms lands the ack before the resend; 400ms after.
        auth_stack.token_lifecycle().clear_cached_token_for_test();
        tokio::time::sleep(Duration::from_millis(late_ack_delay_ms)).await;
        // The unarmed success must drop the record WITHOUT driving state — no
        // Acked transition, no FSM event (the armed gate's protection).
        sock.emit_text(ack_ok("balances"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        {
            let m = sub_mirror.read().unwrap();
            assert!(
                m.get(&(ChannelName::Balances, None))
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

    /// A register mid-Authenticating with nothing in flight sends its signed
    /// subscribe immediately — the subscribe IS the auth probe.
    #[tokio::test]
    async fn register_mid_authenticating_idle_sends_subscribe_as_probe() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-P",
            serde_json::json!({ "error": [], "result": { "token": "unused", "expires": 900u32 } }),
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
            event: crate::types::CallerEvent::StartConnect { request_id: 900 },
        })
        .expect("post connect");
        let auth_sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
        wait_for_state(
            &auth_mirror,
            ConnectionState::Authenticating,
            "auth upgrade",
        )
        .await;
        assert_exactly_n_sent(&auth_sock, 0, "empty composition sends nothing").await;

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");
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
                    if m.get(&(ChannelName::Executions, None))
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

    /// A tombstone is absent for hint liveness: releasing the last LIVE entry
    /// re-derives the auth hint to false even though the row stays listable.
    #[tokio::test]
    async fn auth_hint_ignores_reject_tombstone() {
        use crate::conn::subscription_registry::{EntrySubState, SubscriptionMirror};
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-H",
            serde_json::json!({ "error": [], "result": { "token": "unused", "expires": 900u32 } }),
        );
        let sub_mirror = SubscriptionMirror::default();
        let (reactor, auth_mirror, hint) = spawn_reactor_with_sub_mirror_and_hint(
            &bus,
            &factory,
            Arc::clone(&auth_stack),
            Arc::clone(&sub_mirror),
        );
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances,
                )],
            },
        })
        .expect("post register");
        let sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
        let (channel, _) = nth_sent_token(&sock, 0).await;
        assert_eq!(channel, "balances");
        sock.emit_text(ack_ok("balances"));
        wait_for_state(&auth_mirror, ConnectionState::Open, "open").await;
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");
        let (channel, _) = nth_sent_token(&sock, 1).await;
        assert_eq!(channel, "executions");
        sock.emit_text(ack_err("executions", "Currency pair not supported"));
        let poll = async {
            loop {
                {
                    let m = sub_mirror.read().unwrap();
                    if m.get(&(ChannelName::Executions, None))
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
        assert!(hint.load(Ordering::Acquire), "balances still live");
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
                if !hint.load(Ordering::Acquire) {
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
                m.get(&(ChannelName::Executions, None)).is_some(),
                "tombstone row still listable while the hint reads false"
            );
        }

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn test_b_token_stale_refresh_resend_drives_to_open() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "stale-token-B",
            serde_json::json!({ "error": [], "result": { "token": "fresh-token", "expires": 900u32 } }),
        );
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");

        let auth_sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
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

    /// A 2nd auth channel registered while ALREADY `Open` sends the tokenized
    /// subscribe — no reconnect needed.
    #[tokio::test]
    async fn test_auth_register_while_open_sends_tokenized_subscribe_a374() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-M2",
            serde_json::json!({ "error": [], "result": { "token": "unused", "expires": 900u32 } }),
        );
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);

        let auth_sock = drive_auth_to_open(&bus, &factory, &auth_mirror).await;
        assert_exactly_n_sent(&auth_sock, 1, "after open (only the executions sub)").await;

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances,
                )],
            },
        })
        .expect("post register balances while open");

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

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances,
                )],
            },
        })
        .expect("post duplicate balances register");
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

    /// Register-while-Open with an expired token defers + kicks `force_refresh`
    /// and re-issues on refresh — without re-subscribing already-acked channels.
    #[tokio::test]
    async fn test_auth_register_while_open_no_token_refreshes_and_reissues_m2() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-M3",
            serde_json::json!({ "error": [], "result": { "token": "fresh-token-M3", "expires": 900u32 } }),
        );
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, Arc::clone(&auth_stack));

        let auth_sock = drive_auth_to_open(&bus, &factory, &auth_mirror).await;
        assert_exactly_n_sent(&auth_sock, 1, "after open (only the executions sub)").await;

        auth_stack.token_lifecycle().clear_cached_token_for_test();
        assert!(
            auth_stack.cached_token().is_none(),
            "token cleared — simulates expiry-while-Open"
        );

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Balances,
                    None,
                    SubscribeParams::Balances,
                )],
            },
        })
        .expect("post register balances while open (no token)");

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

    /// A token-less `RegisterBatch` fires EXACTLY ONE `force_refresh` (later
    /// entries still defer); all defers re-issue on refresh.
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

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Executions,
                        None,
                        SubscribeParams::Executions,
                    ),
                    SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Balances,
                        None,
                        SubscribeParams::Balances,
                    ),
                ],
            },
        })
        .expect("post RegisterBatch {executions, balances} with expired token");

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

    /// Drive the auth MC to `Open` via the 3a path (Register-only auto-connect
    /// → handshake → Open). Returns the auth socket handle.
    async fn drive_auth_to_open(
        bus: &Arc<DispatchEventBus>,
        factory: &Arc<DriveableWsSocketFactory>,
        auth_mirror: &Arc<AtomicU8>,
    ) -> crate::transport::driveable_mock::DriveableSocketHandle {
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");
        let auth_sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened")
        };
        wait_for_state(auth_mirror, ConnectionState::Authenticating, "auth upgrade").await;
        let _ = nth_sent_token(&auth_sock, 0).await;
        auth_sock.emit_text(ack_ok("executions"));
        wait_for_state(auth_mirror, ConnectionState::Open, "auth open").await;
        auth_sock
    }

    /// Post a `WsRequestFrame` and return the completion oneshot's receiver.
    fn post_ws_request(
        bus: &Arc<DispatchEventBus>,
        req_id: u64,
        method: &str,
        op: WsOp,
        params: serde_json::Value,
    ) -> tokio::sync::oneshot::Receiver<Result<WsResponse, ConnectionError>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        bus.try_post_caller_inbound(CallerInbound::WsRequestFrame {
            req_id,
            method: method.to_string(),
            op,
            params,
            completion: tx,
        })
        .expect("post WsRequestFrame");
        rx
    }

    /// Bounded await of a completion oneshot.
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

    /// Poll the sent frames until one parses with `req_id == target`.
    async fn wait_for_sent_req(
        sock: &crate::transport::driveable_mock::DriveableSocketHandle,
        target: u64,
    ) -> serde_json::Value {
        let poll = async {
            loop {
                for body in sock.sent_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                        if v.get("req_id").and_then(serde_json::Value::as_u64) == Some(target) {
                            return v;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: auth socket never sent req_id={target}"))
    }

    #[tokio::test]
    async fn test_c_ws_order_request_resolve_and_decode() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-C",
            serde_json::json!({ "error": [], "result": { "token": "unused", "expires": 900u32 } }),
        );
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
        let rx = post_ws_request(&bus, req_id, "add_order", WsOp::AddOrder, params);

        let sent = wait_for_sent_req(&auth_sock, req_id).await;
        assert_eq!(sent["method"], "add_order", "composed method");
        assert_eq!(sent["req_id"], req_id, "echoed req_id");
        assert_eq!(
            sent["params"]["token"], "seeded-token-C",
            "reactor injected the cached token into params (A277)"
        );
        assert_eq!(sent["params"]["order_type"], "limit");
        assert_eq!(sent["params"]["side"], "buy");
        assert_eq!(sent["params"]["symbol"], "BTC/USDC");
        assert_eq!(sent["params"]["post_only"], true);
        assert_eq!(
            sent["params"]["cl_ord_id"],
            "11111111-1111-4111-8111-111111111111"
        );
        assert!(
            sent["params"]["order_qty"].is_number(),
            "order_qty is a JSON number"
        );
        assert!(
            sent["params"]["limit_price"].is_number(),
            "limit_price is a JSON number"
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
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-D",
            serde_json::json!({ "error": [], "result": { "token": "unused", "expires": 900u32 } }),
        );

        let order: Arc<std::sync::Mutex<Vec<&'static str>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let failed_payloads: Arc<std::sync::Mutex<Vec<(u64, WsOp, WsFailReason)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        {
            let order_c = Arc::clone(&order);
            let payloads_c = Arc::clone(&failed_payloads);
            let _ = bus.subscribe(
                EventType::WsRequestFailedEvent,
                Arc::new(move |env: &EventEnvelope| {
                    order_c.lock().unwrap().push("failed");
                    if let EventPayload::WsRequestFailedEvent { req_id, op, reason } = &env.payload
                    {
                        payloads_c.lock().unwrap().push((*req_id, *op, *reason));
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
            payloads.contains(&(req_id_a, WsOp::AddOrder, WsFailReason::ConnectionLost)),
            "add_order pending failed with its req_id + AddOrder + ConnectionLost; got {payloads:?}"
        );
        assert!(
            payloads.contains(&(req_id_b, WsOp::CancelOrder, WsFailReason::ConnectionLost)),
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
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-E",
            serde_json::json!({ "error": [], "result": { "token": "unused", "expires": 900u32 } }),
        );
        let (reactor, auth_mirror) = spawn_reactor(&bus, &factory, auth_stack);

        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post register");
        let auth_sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("auth socket")
        };
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
    async fn presend_no_token_at_open_rejects_retryable_and_refreshes_no_defer() {
        let (bus, factory) = bus_and_factory();
        let (auth_stack, _rest) = auth_stack_with_seeded_token(
            &bus,
            "seeded-token-W2-2-T2",
            serde_json::json!({ "error": [], "result": { "token": "fresh-token-W2-2-T2", "expires": 900u32 } }),
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
            .expect_err("Err not_open");
        assert_eq!(
            err.kind(),
            ConnectionErrorKind::NotOpen,
            "A392: pre-send no-valid-token order rejected NotOpen (retryable)"
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

    const BUDGET: Duration = Duration::from_millis(1500);

    /// Everything a wire_client fixture may hand back; each thin wrapper selects
    /// its fields and decides whether to `mem::forget` the supervisor.
    struct WiredClient {
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

    /// Full production wiring shared by every wire_client fixture.
    fn wire_client_core(
        seeded_token: &str,
        refresh_envelope: serde_json::Value,
        order_transport: Arc<dyn HttpTransport>,
        order_deadline_ms: Option<u32>,
        set_rest_bus: bool,
        with_index: bool,
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
            auth_has_subscriptions: supervisor.auth_has_subscriptions_hint(),
            auth_send_ready: supervisor.auth_send_ready_flag(),
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
            ws_surface,
            std::sync::Arc::new({
                let mut k = crate::build::knobs::Knobs::defaults();
                k.ws_order_response_deadline_ms = order_deadline_ms;
                k
            }),
            std::sync::Arc::new(
                crate::dispatch::dispatch_table::DispatchTable::with_default_spot_table(),
            ),
        );
        WiredClient {
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

    /// For fixtures that never exercise a refresh.
    fn unused_refresh_envelope() -> serde_json::Value {
        serde_json::json!({ "error": [], "result": { "token": "unused", "expires": 900u32 } })
    }

    /// Wiring whose order REST transport panics — enforces the no-REST-fallback contract.
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

    /// `wire_client` parameterised on the `force_refresh` envelope; a failing
    /// envelope (empty `token`) makes the refresh decode-fail so the cache stays `None`.
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
            false,
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

    /// Register an `executions` auth subscription — arms the auth-WS bring-up
    /// (v1 needs >=1 auth entry to compose the first signed subscribe).
    pub(super) fn register_executions(bus: &Arc<DispatchEventBus>) {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
        use crate::dispatch::CallerInbound;
        use crate::dispatch::event_bus::RegistryMutationOp;
        use crate::types::ChannelName;
        bus.try_post_caller_inbound(CallerInbound::RegistryMutation {
            url: WsUrl::Auth,
            mutation: RegistryMutationOp::RegisterBatch {
                ref_id: None,
                entries: vec![SubscriptionEntry::new(
                    WsUrl::Auth,
                    ChannelName::Executions,
                    None,
                    SubscribeParams::Executions,
                )],
            },
        })
        .expect("post executions register");
    }

    /// Drive the auth socket through the 3a handshake to Open; returns the
    /// socket handle.
    pub(super) async fn drive_handshake_to_open(
        factory: &Arc<DriveableWsSocketFactory>,
        auth_mirror: &Arc<AtomicU8>,
    ) -> DriveableSocketHandle {
        let sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("timeout: auth socket never opened (order auto-connect)")
        };
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

    /// Poll until the auth socket has sent its first (subscribe) frame.
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

    /// Poll for a sent `add_order` frame and return its `req_id`.
    pub(super) async fn wait_for_add_order_req(sock: &DriveableSocketHandle) -> u64 {
        let poll = async {
            loop {
                for body in sock.sent_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                        if v.get("method").and_then(serde_json::Value::as_str) == Some("add_order")
                        {
                            return v
                                .get("req_id")
                                .and_then(serde_json::Value::as_u64)
                                .expect("req_id");
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: auth socket never sent an add_order frame")
    }

    /// Emit a success `add_order` response correlated to `req_id`.
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

        let sock = {
            let poll = async {
                loop {
                    if let Some(h) = factory.handle(0) {
                        return h;
                    }
                    tokio::task::yield_now().await;
                }
            };
            tokio::time::timeout(BUDGET, poll)
                .await
                .expect("auth socket")
        };
        wait_for_first_subscribe(&sock).await;
        sock.emit_text(ack_err("executions", "EAPI:Invalid key"));

        let res = tokio::time::timeout(BUDGET, order_fut)
            .await
            .expect("timeout: order future never resolved")
            .expect("task panicked");
        let err = res.expect_err("WS connect failure must surface as Err (no REST fallback)");
        assert!(
            matches!(err, TradeError::Transport { .. }),
            "connection failure maps to TradeError::Transport (ConnectionError→TradeError); got {err:?}"
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
        let (trade, factory, bus, auth_mirror, auth_stack, reactor) = wire_client_with_refresh(
            "seeded-token-W2-2-T1",
            serde_json::json!({ "error": [], "result": { "token": "", "expires": 900u32 } }),
        );
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
        let err =
            res.expect_err("Open order-ack TokenStale must resolve the order Err (retryable)");
        assert!(
            matches!(err, TradeError::Transport { .. }),
            "A390 drain maps to TradeError::Transport (retryable); got {err:?}"
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

        reactor.abort();
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn open_order_ack_token_stale_ack_time_force_refresh_relands_token() {
        let (trade, factory, bus, auth_mirror, auth_stack, reactor) = wire_client_with_refresh(
            "seeded-token-W2-2-T2",
            serde_json::json!({ "error": [], "result": { "token": "fresh-token-W2-2-T2", "expires": 900u32 } }),
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
            matches!(err, TradeError::Transport { .. }),
            "A390 drain maps to TradeError::Transport (retryable); got {err:?}"
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
            serde_json::json!({ "error": [], "result": { "token": "fresh-token-no-force-17", "expires": 900u32 } }),
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

    /// `wire_client` whose order REST transport captures + returns a canned
    /// AddOrder response instead of panicking.
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

    /// Return the FIRST `add_order` frame's JSON — on the bare path the order is
    /// the only thing sent during Authenticating.
    async fn wait_for_sent_add_order(sock: &DriveableSocketHandle) -> serde_json::Value {
        let poll = async {
            loop {
                for body in sock.sent_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                        if v.get("method").and_then(serde_json::Value::as_str) == Some("add_order")
                        {
                            return v;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: bare-order auth socket never sent the probe add_order frame")
    }

    /// Wait for the (bare-order auto-connect) auth socket to be created.
    async fn wait_for_auth_socket(
        factory: &Arc<DriveableWsSocketFactory>,
    ) -> DriveableSocketHandle {
        let poll = async {
            loop {
                if let Some(h) = factory.handle(0) {
                    return h;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: bare-order auth socket never opened")
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

        let sock = wait_for_auth_socket(&factory).await;
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

        let sock = wait_for_auth_socket(&factory).await;
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
    async fn test_pc3_auth_rejection_does_not_authenticate() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-PC3");
        let cl = "33333333-3333-4333-8333-333333333333";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = wait_for_auth_socket(&factory).await;
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

        let sock = wait_for_auth_socket(&factory).await;
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
    async fn test_pc5_token_stale_during_probe_drains_retryable_no_resend() {
        let (trade, factory, bus, auth_mirror, reactor) = wire_client("seeded-token-PC5");
        let cl = "55555555-5555-4555-8555-555555555555";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = wait_for_auth_socket(&factory).await;
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
        let err = res.expect_err("token-stale probe must resolve the order Err (retryable)");
        assert!(
            matches!(err, TradeError::Transport { .. }),
            "token-stale drain maps to TradeError::Transport (retryable); got {err:?}"
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

        reactor.abort();
        bus.stop_reactors();
    }

    /// `wire_client` that RETURNS the `ConnectionSupervisor` so a test can read
    /// the `auth_has_subscriptions()` hint.
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

    /// Channel-wide Deregister taking the auth registry back to empty.
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

    /// Bounded poll of the supervisor's `auth_has_subscriptions()` hint.
    async fn wait_for_flag(sup: &Arc<ConnectionSupervisor>, target: bool, what: &str) {
        let poll = async {
            loop {
                if sup.auth_has_subscriptions() == target {
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
            !supervisor.auth_has_subscriptions(),
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
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                                    if v.get("method").and_then(serde_json::Value::as_str)
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
    async fn test_fix3_mcap_during_probe_resolves_retryable_not_raw_ack() {
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
                                    {
                                        if v.get("method").and_then(serde_json::Value::as_str)
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
                matches!(err, TradeError::Transport { .. }),
                "attempt {attempt}: token-stale must resolve RETRYABLE Transport (BUG#2); got {err:?}"
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
            matches!(last_err, Some(TradeError::Transport { .. })),
            "the M-cap (CAP-th) order resolved RETRYABLE, not the raw token-stale ack"
        );

        std::mem::forget(supervisor);
        reactor.abort();
        bus.stop_reactors();
    }

    /// Bounded poll of the supervisor's `auth_send_ready()` LEVEL flag.
    async fn wait_for_send_ready(sup: &Arc<ConnectionSupervisor>, target: bool, what: &str) {
        let poll = async {
            loop {
                if sup.auth_send_ready() == target {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: auth_send_ready != {target} ({what})"));
    }

    #[tokio::test]
    async fn test_a342_send_ready_flag_set_on_cold_start_cleared_on_open() {
        let (trade, factory, bus, auth_mirror, supervisor, reactor) =
            wire_client_keep_supervisor("seeded-token-A342");
        assert!(
            !supervisor.auth_send_ready(),
            "fresh client: send-ready flag false"
        );

        let cl = "a3420000-0000-4000-8000-000000000001";
        let mut req = buy_limit("BTC/USDC", "0.0001", "33512.0");
        req.cl_ord_id = Some(crate::types::ClOrdId::new(cl).unwrap());
        let order_fut = tokio::spawn(async move { trade.order(req).await });

        let sock = wait_for_auth_socket(&factory).await;
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
        let _sock = wait_for_auth_socket(&factory).await;
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

    /// Variant of `wire_client` that also returns the shared trading-rate tracker
    /// (the SAME Arc wired into both `WsSurface` and `RestSurface`).
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

    /// Pre-drain the shared trading tracker to the cap: the WS order must
    /// resolve `Err(RateLimited)` and post no frame. Flipping the cost arm to `None` -> RED.
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

    /// The WS order deadline fires on a silent server → the order resolves
    /// SENT-AMBIGUOUS (`retryable() == false`), never hanging.
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

    /// After a deadline timeout the pending entry is abandoned, so a later
    /// drain must NOT emit a spurious `WsRequestFailedEvent`.
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

    /// A deadline that is NOT exceeded resolves `Ok` — the timeout wrapper
    /// doesn't disturb the happy path.
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
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                        if v.get("method").and_then(serde_json::Value::as_str) == Some("add_order")
                        {
                            return v["req_id"].as_u64().unwrap();
                        }
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

    /// Also wires a shared `Arc<ClOrdIdPairIndex>` into the `WsSurface` so the
    /// WS cancel/amend charge arm can find an index entry.
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

    /// WS cancel charge guard: pre-drained to just below the cancel cost,
    /// `cancel(...).via(WsV2Auth)` must return `Err(RateLimited)` and post no frame.
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

    /// Poll for a sent frame with the given `method` and return its `req_id`.
    async fn wait_for_order_req(sock: &DriveableSocketHandle, method: &str) -> u64 {
        let poll = async {
            loop {
                for body in sock.sent_text() {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                        if v.get("method").and_then(serde_json::Value::as_str) == Some(method) {
                            return v
                                .get("req_id")
                                .and_then(serde_json::Value::as_u64)
                                .expect("req_id");
                        }
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
        // A stray amend_id in the reply must not leak into the event.
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
                status,
                ..
            } => {
                assert_eq!(cl_ord_id, cl);
                assert_eq!(amend_id, None, "cancel success never carries an amend_id");
                assert_eq!(op, OrderOp::CancelOrder);
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
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::time::Duration;

    const BUDGET: Duration = Duration::from_millis(1500);

    /// Bus with the dispatch reactor running.
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

    /// Optionally-seeded token cache; no RestSurface (never triggers `force_refresh`).
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

    /// Record each `ConnectionSendReadyEvent` `(url, ready_at)` into a shared Vec.
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

    /// Bounded poll until `seen` holds at least `n` events.
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

    /// Assert NO send-ready event within a settle window (can't prove a
    /// negative forever; the window catches a spurious emit).
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

    /// COLD-START: Authenticating + empty auth registry + token cached emits
    /// exactly one `ConnectionSendReadyEvent { url: Auth }`.
    #[tokio::test]
    async fn emits_on_cold_start_authenticating_empty_with_token() {
        let bus = live_bus();
        let seen = record_send_ready(&bus);
        let mc = mc_in_state(&bus, WsUrl::Auth, ConnectionState::Authenticating);
        let registry = SubscriptionRegistry::new();
        let auth = auth_stack(&bus, Some("seeded-cold"));
        let flag = Arc::new(AtomicBool::new(false));
        let weak = Arc::downgrade(&bus);

        refresh_send_ready(WsUrl::Auth, &mc, &registry, &auth, &flag, &weak);

        assert!(
            flag.load(Ordering::Acquire),
            "send-ready level flag set true on cold-start ready"
        );
        let v = wait_for_n(&seen, 1, "cold-start emit").await;
        assert_eq!(v[0].0, WsUrl::Auth, "send-ready is for the Auth url");
        bus.stop_reactors();
    }

    /// WARM-RECONNECT: the same predicate emits — the helper is state-driven.
    #[tokio::test]
    async fn emits_on_warm_reconnect_authenticating_empty_with_cached_token() {
        let bus = live_bus();
        let seen = record_send_ready(&bus);
        let mc = mc_in_state(&bus, WsUrl::Auth, ConnectionState::Authenticating);
        let registry = SubscriptionRegistry::new();
        let auth = auth_stack(&bus, Some("seeded-warm"));
        let flag = Arc::new(AtomicBool::new(false));
        let weak = Arc::downgrade(&bus);

        refresh_send_ready(WsUrl::Auth, &mc, &registry, &auth, &flag, &weak);

        assert!(
            flag.load(Ordering::Acquire),
            "send-ready level flag set true on warm-reconnect ready"
        );
        let v = wait_for_n(&seen, 1, "warm-reconnect emit").await;
        assert_eq!(v[0].0, WsUrl::Auth);
        bus.stop_reactors();
    }

    /// PREDICATE GATING: each row is a combination that must NOT emit and must
    /// self-clear the level flag to false.
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
                        SubscribeParams::Executions,
                    ),
                    crate::types::MonotonicInstant::now(),
                    false,
                );
            }
            let auth = auth_stack(&bus, token);
            let flag = Arc::new(AtomicBool::new(true));
            let weak = Arc::downgrade(&bus);

            refresh_send_ready(url, &mc, &registry, &auth, &flag, &weak);

            assert!(
                !flag.load(Ordering::Acquire),
                "{label}: send-ready level flag must self-clear to false"
            );
            assert_none_within(&seen, label).await;
            bus.stop_reactors();
        }
    }

    /// IDEMPOTENT: calling the helper twice emits twice (harmless — a fire-once
    /// oneshot bridge resolves exactly once).
    #[tokio::test]
    async fn idempotent_double_emit_is_harmless_awaiter_fires_once() {
        let bus = live_bus();
        let seen = record_send_ready(&bus);
        let mc = mc_in_state(&bus, WsUrl::Auth, ConnectionState::Authenticating);
        let registry = SubscriptionRegistry::new();
        let auth = auth_stack(&bus, Some("seeded-idem"));
        let flag = Arc::new(AtomicBool::new(false));
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
            flag.load(Ordering::Acquire),
            "level flag true after first ready re-check"
        );
        refresh_send_ready(WsUrl::Auth, &mc, &registry, &auth, &flag, &weak);
        assert!(
            flag.load(Ordering::Acquire),
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
    use std::time::Duration;

    const BUDGET: Duration = Duration::from_millis(1500);

    /// Spawn `run()` with two `Idle` connections (Public + Auth).
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
            auth_has_subscriptions: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            auth_send_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        (tokio::spawn(run(init)), public_mirror, auth_mirror)
    }

    /// One-shot recorder for the first `ClientClosedEvent`; registered
    /// synchronously BEFORE any post → race-free.
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
    use super::order_autoconnect_tests::{
        buy_limit, drive_handshake_to_open, emit_add_order_ok, register_executions,
        wait_for_add_order_req, wire_client,
    };
    use super::reconnect_drive_tests::ack_text;
    use super::subscribe_edge_gating_tests::{
        await_socket0, register_public, spawn_public_reactor,
    };
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

    /// A valid ticker `update` wire frame for `symbol`.
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

    /// A ticker frame missing required numeric fields → DecodeFailed on the reactor.
    fn malformed_ticker_frame(symbol: &str) -> String {
        serde_json::json!({
            "channel": "ticker",
            "type": "update",
            "data": [{ "symbol": symbol, "bid": "not-a-number-and-missing-fields" }],
        })
        .to_string()
    }

    /// Register a `ticker` handler wrapping `cb`; the presence gate is bypassed.
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
            channel: ChannelName::Ticker,
            op: HandlerMutationOp::Register {
                id: handler_id,
                callback: wrapped,
            },
        })
        .expect("post ticker handler register");
        handler_id
    }

    /// Register a `book` (maintained) handler wrapping `cb`.
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
            channel: ChannelName::Book,
            op: HandlerMutationOp::Register {
                id: handler_id,
                callback: wrapped,
            },
        })
        .expect("post book handler register");
        handler_id
    }

    /// Register a `status` handler wrapping `cb` (the symbol-less delivery path).
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
            channel: ChannelName::Status,
            op: HandlerMutationOp::Register {
                id: handler_id,
                callback: wrapped,
            },
        })
        .expect("post status handler register");
        handler_id
    }

    /// An unsolicited `status` wire frame; carries no `symbol`.
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

    /// Drive a freshly-spawned public reactor to `Open` on `(channel, pair)`.
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
        let sock = await_socket0(factory).await;
        wait_for_state(public_mirror, ConnectionState::Resubscribing, "upgrade").await;
        sock.emit_text(ack_text(wire_channel, Some(pair)));
        wait_for_state(public_mirror, ConnectionState::Open, "subscribe ack").await;
        sock
    }

    /// Subscribe to an event type and collect matching envelopes into a shared Vec.
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

    /// An order must resolve WHILE an `on_ticker` callback is still blocked,
    /// proving the callback runs on the dispatch loop, not the reactor task.
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

        register_public(
            &bus,
            ChannelName::Ticker,
            "BTC/USD",
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
        );
        let auth_sock = drive_handshake_to_open(&factory, &auth_mirror).await;

        let public_sock = {
            let poll = async {
                loop {
                    let n = factory.created_count();
                    for i in 0..n {
                        if let Some(h) = factory.handle(i) {
                            if h.sent_text().iter().any(|b| {
                                serde_json::from_str::<serde_json::Value>(b)
                                    .ok()
                                    .and_then(|v| {
                                        let m = v.get("method")?.as_str()? == "subscribe";
                                        let c =
                                            v.get("params")?.get("channel")?.as_str()? == "ticker";
                                        Some(m && c)
                                    })
                                    .unwrap_or(false)
                            }) {
                                return h;
                            }
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

    /// A slow data callback emits `SlowCallbackWarning` — data callbacks share
    /// the long-lived subscribers' observability.
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
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
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

    /// The single FIFO ring preserves per-(channel, symbol) delivery ordering.
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
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
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

    /// A symbol-less (`status`) data callback still reaches the user closure on
    /// the dispatch loop.
    #[tokio::test]
    async fn symbol_less_status_channel_delivers_via_dispatch() {
        let (bus, factory) = bus_and_factory();
        let (reactor, public_mirror) = spawn_public_reactor(&bus, &factory);

        let seen: Arc<Mutex<Vec<SystemStatusUpdate>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        register_status_handler(&bus, 9001, move |u: &SystemStatusUpdate| {
            sink.lock().expect("seen lock").push(u.clone());
        });

        // status frames arrive without a subscribe.
        let sock = open_public_on(
            &bus,
            &factory,
            &public_mirror,
            ChannelName::Ticker,
            "ticker",
            "BTC/USD",
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
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

    /// A decode-failed frame must STILL emit `SubscriptionGapEvent{MalformedFrame}`
    /// from the reactor — only the user-callback INVOCATION moved to dispatch.
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
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
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

    /// A non-JSON text frame is dropped silently — no bus event, no handler
    /// delivery, no reactor death — and the next valid frame still routes.
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
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
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

    /// A CRC-valid book snapshot frame, checksummed the way the maintained-book
    /// builder validates it.
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

    /// A maintained-book snapshot still arrives CRC-validated — book maintenance
    /// stays reactor-side; only the user callback runs on dispatch.
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

    /// Exhausted CRC-gap budget drops maintenance and SKIPS the maintained-`Book`
    /// fan — nothing is enqueued for `on_book` during the gap.
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

    /// With ONLY an `on_book_raw` handler, a `book` frame must NOT emit a
    /// spurious `MessageDroppedNoHandler{Book}`.
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

    /// Complement: with NEITHER handler the frame is truly unconsumed — exactly
    /// one `MessageDroppedNoHandler{Book}`.
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

    /// The reseed's UNSUBSCRIBE leg must echo the subscribe depth (D10 -> 25)
    /// or the stale sub keeps streaming.
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

    /// Fill the ring while dispatch is parked, forcing drop-oldest + a debounced
    /// QueueFullWarning; a later snapshot still arrives CRC-valid.
    // Multi-thread runtime: the busy-parked worker would freeze a current-thread
    // runtime (and its timeout timer) → deadlock rather than a clean bound.
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
            // Hard safety cap: tokio can't cancel a blocking task mid-sleep, so an
            // uncapped busy-loop would hang runtime shutdown after a failed assert.
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
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
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

    /// The `ready()` re-entry resolves via `deliver_correlated`, NOT the
    /// droppable ring — so it resolves even with the dispatch loop parked.
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

        // Park the dispatch loop on the FIRST ticker frame, with a hard safety
        // cap (tokio cannot cancel a blocking task mid-sleep).
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
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
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

    // Timing-sensitive; ignored by default. Run with:
    // cargo test latency_bench -- --ignored --nocapture --test-threads=1
    mod latency_bench {
        use super::*;
        use crate::types::MonotonicInstant;
        use std::sync::atomic::AtomicU64;

        type Hist = crate::dispatch::io_reactor::latency_histogram::LatencyHistogram;

        #[inline]
        fn now_ns() -> u64 {
            MonotonicInstant::now().as_duration().as_nanos() as u64
        }

        /// Measures the book delivery path: end-to-end (emit → on_book callback)
        /// plus the stable on-thread compute parts.
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

            // Race-free: one in-flight frame at a time means a single writer to
            // emit_start (the wait blocks until this frame's arrival is counted).
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

        /// Poll until at least n add_order frames were sent; return the nth req_id.
        async fn wait_for_nth_add_order(
            sock: &crate::transport::driveable_mock::DriveableSocketHandle,
            n: usize,
        ) -> u64 {
            let poll = async {
                loop {
                    let mut ids = Vec::new();
                    for body in sock.sent_text() {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                            if v.get("method").and_then(serde_json::Value::as_str)
                                == Some("add_order")
                            {
                                if let Some(id) =
                                    v.get("req_id").and_then(serde_json::Value::as_u64)
                                {
                                    ids.push(id);
                                }
                            }
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

        /// Order-forward compute: how long handle_ws_request_frame takes to turn
        /// one order into bytes on the wire (within-thread; no callback/network).
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

    /// Register a raw-delta `BookRaw` handler wrapping `cb`.
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
            channel: ChannelName::BookRaw,
            op: HandlerMutationOp::Register {
                id: handler_id,
                callback: wrapped,
            },
        })
        .expect("post book_raw handler register");
        handler_id
    }

    /// Await the first delivered value in `seen`, bounded by `BUDGET`.
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

    /// EQUIVALENCE: the fast-path delivers an `OrderBookUpdate` byte-identical
    /// to the decode path, verified by reparsing the same frame through `decode_book`.
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

    /// Depth-cap preserved through the fast-path: a D10 subscribe fed 12 levels
    /// delivers 10 per side (the cap is imposed by `build_update`, not decode).
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

    /// A CRC mismatch SKIPS the Book fan (never delivers stale/partial) and
    /// emits an `OrderBookGapEvent`; exactly two books deliver.
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

    /// One `book` frame feeds both handlers — the maintained one gets a typed
    /// `OrderBookUpdate`, the raw one a `BookDelta`.
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

    /// First book `unsubscribe` followed by a book `subscribe` in `frames` →
    /// `(unsubscribe_params, subscribe_params)`.
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

    /// A book delta whose `checksum` is deliberately wrong.
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

    /// Poll `books` until a delivered book's top bid equals `px`.
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

    /// Closed loop: bad CRC -> gap-event pair -> reseed on the wire (unsubscribe
    /// echoes depth 25, resubscribe with `snapshot: true`) -> the fresh snapshot re-
    /// delivers to `on_book`, while `on_book_raw` streams through the gap unsuppressed.
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

    /// Reseed-timeout ladder: when the fresh snapshot never arrives the liveness timer
    /// re-sends the reseed until the consecutive-gap cap, then degrades to per-frame
    /// delivery with one final gap-event pair.
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

    /// A `PriceLevel` from wire strings — the strings ARE the CRC32 source of truth.
    fn wire_level(price: &str, qty: &str) -> crate::book::PriceLevel {
        crate::book::PriceLevel {
            price: price.parse().unwrap(),
            qty: qty.parse().unwrap(),
            price_wire: price.to_string(),
            qty_wire: qty.to_string(),
        }
    }

    /// A CRC-valid delta frame checksummed on the cumulative post-apply book,
    /// via the same `OrderBookBuilder` the reactor uses.
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

    /// A CRC-valid delta advances the maintained book; asserts the new best bid
    /// AND that a pre-delta level survives (cumulative apply, not replace).
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

    /// With no maintained builder the Book fan runs `decode_book` per-frame
    /// (`PassThrough`) — the same mechanism the exhausted-budget degrade uses.
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

    const BUDGET: Duration = Duration::from_millis(1500);

    use super::reconnect_drive_tests::ack_text;

    /// A subscribe-REJECT ack frame: top-level channel/symbol + error, success:false.
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

    struct MirrorRig {
        bus: Arc<DispatchEventBus>,
        factory: Arc<DriveableWsSocketFactory>,
        mirror: SubscriptionMirror,
        public_state: Arc<AtomicU8>,
        terminated: Arc<AtomicU64>,
        auth_has_subscriptions: Arc<std::sync::atomic::AtomicBool>,
        reactor: tokio::task::JoinHandle<()>,
    }

    /// Full-loop rig: real `run()` over a driveable mock, with a caller-readable
    /// `SubscriptionMirror` and a `SubscriptionTerminatedEvent` counter.
    fn spawn_mirror_rig() -> MirrorRig {
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
        let auth_has_subscriptions = Arc::new(std::sync::atomic::AtomicBool::new(false));
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
            auth_has_subscriptions: Arc::clone(&auth_has_subscriptions),
            auth_send_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let reactor = tokio::spawn(run(init));
        MirrorRig {
            bus,
            factory,
            mirror,
            public_state,
            terminated,
            auth_has_subscriptions,
            reactor,
        }
    }

    fn register(rig: &MirrorRig, channel: ChannelName, pair: &str, params: SubscribeParams) {
        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Public,
                mutation: RegistryMutationOp::RegisterBatch {
                    ref_id: None,
                    entries: vec![SubscriptionEntry::new(
                        WsUrl::Public,
                        channel,
                        Some(sym(pair)),
                        params,
                    )],
                },
            })
            .expect("post register");
    }

    fn ticker_params() -> SubscribeParams {
        SubscribeParams::Ticker {
            snapshot: None,
            event_trigger: None,
        }
    }

    /// Poll the mirror until `key` reaches `want` (or `want = None` for row-gone).
    async fn wait_for_row(
        mirror: &SubscriptionMirror,
        channel: ChannelName,
        pair: Option<&str>,
        want: Option<fn(&EntrySubState) -> bool>,
        what: &str,
    ) {
        let key = (channel, pair.map(sym));
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

    /// Poll the captured sends until `n` frames containing `needle` were observed.
    async fn wait_for_frames(
        handle: &crate::transport::driveable_mock::DriveableSocketHandle,
        needle: &str,
        n: usize,
        what: &str,
    ) -> Vec<String> {
        let poll = async {
            loop {
                let hits: Vec<String> = handle
                    .sent_text()
                    .into_iter()
                    .filter(|t| t.contains(needle))
                    .collect();
                if hits.len() >= n {
                    return hits;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: {what}"))
    }

    async fn first_socket(
        rig: &MirrorRig,
    ) -> crate::transport::driveable_mock::DriveableSocketHandle {
        let poll = async {
            loop {
                if let Some(h) = rig.factory.handle(0) {
                    return h;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .expect("timeout: socket never opened")
    }

    /// Register → Pending; wire ack → Acked; non-transient reject → retained
    /// `Terminated` tombstone (entry stays listable).
    #[tokio::test]
    async fn mirror_tracks_pending_acked_and_reject_tombstone() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let s = first_socket(&rig).await;
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

    /// unsubscribe_all: every entry force-removed, one wire unsubscribe per
    /// entry echoing the stored discriminator, one ClientClosed terminated event each.
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
            },
        );
        let s = first_socket(&rig).await;
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

    /// unsubscribe_channel scopes the fan-out to one wire channel; a BookRaw
    /// filter normalizes to the shared Book entry; other channels survive.
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
        let s = first_socket(&rig).await;
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
        assert!(
            rig.mirror
                .read()
                .unwrap()
                .contains_key(&(ChannelName::Ticker, Some(sym("BTC/USD"))))
        );

        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    /// A guard drop AFTER unsubscribe_all force-removed its entry is a no-op:
    /// no junk depthless unsubscribe frame, no duplicate terminated event.
    #[tokio::test]
    async fn stale_guard_drop_after_deregister_all_is_noop() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        let s = first_socket(&rig).await;
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

    /// An Acked entry stays Acked through the Resubscribing window (no Pending
    /// churn) and re-acks on the new socket.
    #[tokio::test]
    async fn reconnect_replay_preserves_acked_state() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        let s1 = first_socket(&rig).await;
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
                .get(&(ChannelName::Ticker, Some(sym("BTC/USD"))))
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
    /// A re-subscribe over a Failed tombstone must RE-EMIT the wire subscribe —
    /// not just flip the mirror row.
    #[tokio::test]
    async fn tombstone_revival_reemits_wire_subscribe_and_acks() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let s = first_socket(&rig).await;
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

    /// A transient (rate-limit) reject RETRIES under the ack budget instead of
    /// tombstoning; only non-transient rejects terminate.
    #[tokio::test]
    async fn open_state_rate_limit_reject_retries_and_completes() {
        // Default ack timeout on purpose: the reject must land ARMED.
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        let s = first_socket(&rig).await;
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

    /// A non-transient reject landing in Closing must be consumed unarmed: no
    /// tombstone, no terminal event.
    #[tokio::test]
    async fn reject_in_closing_after_close_neither_tombstones_nor_emits() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        let s = first_socket(&rig).await;
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

    async fn wait_for_flag(flag: &Arc<std::sync::atomic::AtomicBool>, want: bool, what: &str) {
        let poll = async {
            loop {
                if flag.load(Ordering::Acquire) == want {
                    return;
                }
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(BUDGET, poll)
            .await
            .unwrap_or_else(|_| panic!("timeout: {what}"));
    }

    /// A guard drop AFTER a forced teardown must still consume the guard's ref
    /// record (else every force-torn guard leaks one).
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
        let auth_send_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let weak = Arc::downgrade(&bus);

        macro_rules! drive {
            ($msg:expr) => {
                super::super::inbound_dispatch::handle_inbound(
                    $msg,
                    &mut conns,
                    &mut registry,
                    &mut handler_registry,
                    &mut sockets,
                    &state_mirrors,
                    &weak,
                    &upgrade_tx,
                    &alloc,
                    &auth_stack,
                    &auth_send_ready,
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
            channel: ChannelName::Ticker,
            symbols: vec![sym("BTC/USD")],
        });
        assert_eq!(
            registry.guard_ref_count(),
            0,
            "guard drop consumes its record even though the key is already gone"
        );
    }

    /// The auth "has subscriptions" hint must track BOTH directions: true after
    /// an auth register, false after teardown.
    #[tokio::test]
    async fn auth_hint_tracks_liveness_through_loop_sync() {
        let rig = spawn_mirror_rig();
        assert!(!rig.auth_has_subscriptions.load(Ordering::Acquire));
        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Auth,
                mutation: RegistryMutationOp::RegisterBatch {
                    ref_id: None,
                    entries: vec![SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Executions,
                        None,
                        SubscribeParams::Executions,
                    )],
                },
            })
            .expect("post auth register");
        wait_for_flag(
            &rig.auth_has_subscriptions,
            true,
            "hint true after auth register",
        )
        .await;
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
            &rig.auth_has_subscriptions,
            false,
            "hint false after single-key deregister",
        )
        .await;
        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Auth,
                mutation: RegistryMutationOp::RegisterBatch {
                    ref_id: None,
                    entries: vec![SubscriptionEntry::new(
                        WsUrl::Auth,
                        ChannelName::Balances,
                        None,
                        SubscribeParams::Balances,
                    )],
                },
            })
            .expect("post auth re-register");
        wait_for_flag(&rig.auth_has_subscriptions, true, "hint true again").await;
        rig.bus
            .try_post_caller_inbound(CallerInbound::RegistryMutation {
                url: WsUrl::Auth,
                mutation: RegistryMutationOp::DeregisterAll { channel: None },
            })
            .expect("post deregister-all");
        wait_for_flag(
            &rig.auth_has_subscriptions,
            false,
            "hint false after teardown",
        )
        .await;
        rig.reactor.abort();
        rig.bus.stop_reactors();
    }

    /// A terminal reject on the LAST pending replay entry must short-circuit
    /// Resubscribing → Open, not park forever.
    #[tokio::test]
    async fn last_entry_reject_mid_resubscribing_short_circuits_to_open() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let s1 = first_socket(&rig).await;
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

    /// A DeregisterAll landing mid-Resubscribing empties the pending-ack set —
    /// the FSM must short-circuit to Open, not park forever.
    #[tokio::test]
    async fn deregister_all_mid_resubscribing_short_circuits_to_open() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        let s1 = first_socket(&rig).await;
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

    /// DeregisterAll removes Failed tombstones SILENTLY — no junk wire
    /// unsubscribe, no second terminal event.
    #[tokio::test]
    async fn deregister_all_skips_tombstones_silently() {
        let rig = spawn_mirror_rig();
        register(&rig, ChannelName::Ticker, "BTC/USD", ticker_params());
        register(&rig, ChannelName::Ticker, "ETH/USD", ticker_params());
        let s = first_socket(&rig).await;
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

    /// The tombstone-silent rule holds on the single-key teardown paths too
    /// (explicit unsubscribe and guard drop).
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
        let s = first_socket(&rig).await;
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
        assert!(presence_has_handlers(&presence, ChannelName::Ticker));

        for filler in [900u64, 901] {
            bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
                channel: ChannelName::Ticker,
                op: HandlerMutationOp::Deregister {
                    id: HandlerId(filler),
                },
            })
            .expect("filler");
        }
        drop(HandlerHandle::new(
            id,
            ChannelName::Ticker,
            Arc::downgrade(&bus),
        ));
        assert!(
            presence_has_handlers(&presence, ChannelName::Ticker),
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
            auth_has_subscriptions: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            auth_send_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }));

        let cleared = async {
            loop {
                if !presence_has_handlers(&presence, ChannelName::Ticker) {
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
