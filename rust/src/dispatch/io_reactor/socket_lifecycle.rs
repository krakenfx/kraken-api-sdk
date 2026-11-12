//! Socket and upgrade-bridge lifecycle helpers: socket-map projection, upgrade
//! bridging, channel/URL normalization, and WS `unsubscribe` for refcount-0 teardown.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Weak};

use serde_json::Value;
use tokio::sync::mpsc;

use crate::api::ws_surface::SubscriberGuard;
use crate::conn::ManagedConnection;
use crate::conn::subscription_registry::{SubscribeParams, build_subscribe_frame};
use crate::dispatch::event_bus::DispatchEventBus;
use crate::transport::{WsFrame, WsOpcode, WsSocket};
use crate::types::{ChannelName, ConnectionState, StreamSymbol, WsUrl};

use super::{SharedHandles, UpgradeOutcome};

pub(in crate::dispatch::io_reactor) fn write_mirror(
    mirrors: &HashMap<WsUrl, Arc<AtomicU8>>,
    url: WsUrl,
    state: ConnectionState,
) {
    if let Some(m) = mirrors.get(&url) {
        m.store(state.as_u8(), Ordering::Release);
    }
}

/// Bridge the correlated `WsUpgradeOk`/`WsUpgradeFailed` one-shots for a socket's
/// `connection_id` onto the reactor's `upgrade_tx` mpsc (FSM stays single-writer).
pub(in crate::dispatch::io_reactor) fn wire_upgrade_bridge(
    connection_id: u64,
    bus: &Arc<DispatchEventBus>,
    upgrade_tx: &mpsc::Sender<UpgradeOutcome>,
) -> SubscriberGuard {
    let mut guard = SubscriberGuard::new(Arc::downgrade(bus));
    let tx_ok = upgrade_tx.clone();
    let h_ok = bus.subscribe_correlated(
        crate::dispatch::EventType::WsUpgradeOk,
        connection_id,
        Box::new(move |env| {
            if let crate::dispatch::EventPayload::WsUpgradeOk { connection_id, url } = env.payload
                && let Err(e) = tx_ok.try_send(UpgradeOutcome::Ok { url, connection_id }) {
                    tracing::error!(
                        target: "kraken_sdk::io_reactor",
                        ?url, connection_id, error = ?e,
                        "WsUpgradeOk bridge channel full; falling back to synthetic transient error so FSM doesn't stick in Connecting"
                    );
                    let _ = tx_ok.try_send(UpgradeOutcome::Failed {
                        url,
                        kind: crate::transport::TransportErrorKind::AbnormalClose {
                            context: "upgrade-bridge channel full".into(),
                        },
                        transient: true,
                    });
                }
            true
        }),
    );
    guard.push(h_ok);
    let tx_fail = upgrade_tx.clone();
    let h_fail = bus.subscribe_correlated(
        crate::dispatch::EventType::WsUpgradeFailed,
        connection_id,
        Box::new(move |env| {
            if let crate::dispatch::EventPayload::WsUpgradeFailed { url, error, .. } = &env.payload
            {
                let url = *url;
                let kind = error.kind.clone();
                let transient = error.transient;
                if let Err(e) = tx_fail.try_send(UpgradeOutcome::Failed {
                    url,
                    kind,
                    transient,
                }) {
                    tracing::error!(
                        target: "kraken_sdk::io_reactor",
                        ?url, error = ?e,
                        "WsUpgradeFailed bridge channel full; FSM may stick in Connecting"
                    );
                }
            }
            true
        }),
    );
    guard.push(h_fail);
    guard
}

/// Lazy connect: when `mc` is `Idle`, start a connect and project the fresh socket.
pub(in crate::dispatch::io_reactor) fn start_connect_if_idle(
    mc: &mut ManagedConnection,
    url: WsUrl,
    handles: SharedHandles<'_>,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    upgrade_guards: &mut HashMap<WsUrl, SubscriberGuard>,
) {
    let SharedHandles {
        bus_back_ref,
        state_mirrors,
        upgrade_tx,
        connect_id_allocator,
        ..
    } = handles;
    if mc.state() != ConnectionState::Idle {
        return;
    }
    let prev_cid = mc.socket().map(|socket| socket.connection_id());
    let request_id = connect_id_allocator.fetch_add(1, Ordering::Relaxed);
    mc.handle_event(crate::conn::managed_connection::FsmEvent::CallStartConnect { request_id });
    write_mirror(state_mirrors, url, mc.state());
    project_socket_and_wire_bridge(
        mc,
        url,
        prev_cid,
        sockets,
        upgrade_guards,
        bus_back_ref,
        upgrade_tx,
    );
}

/// Project `mc.socket()` into `sockets`, wiring the upgrade bridge once per fresh
/// `connection_id`. Re-projecting the same `prev_cid` skips re-wiring.
pub(in crate::dispatch::io_reactor) fn project_socket_and_wire_bridge(
    mc: &ManagedConnection,
    url: WsUrl,
    prev_cid: Option<u64>,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    upgrade_guards: &mut HashMap<WsUrl, SubscriberGuard>,
    bus_back_ref: &Weak<DispatchEventBus>,
    upgrade_tx: &mpsc::Sender<UpgradeOutcome>,
) {
    match mc.socket() {
        Some(s) => {
            let cid = s.connection_id();
            sockets.insert(url, Arc::clone(s));
            if Some(cid) != prev_cid
                && let Some(bus) = bus_back_ref.upgrade()
            {
                upgrade_guards.insert(url, wire_upgrade_bridge(cid, &bus, upgrade_tx));
            }
        }
        None => {
            sockets.remove(&url);
            upgrade_guards.remove(&url);
        }
    }
}

/// Compose the Kraken WS v2 `unsubscribe` frame for `(channel, pair)`, routed
/// through `build_subscribe_frame` (depth/interval can't drift from subscribe);
/// `token` injected for auth channels.
pub(in crate::dispatch::io_reactor) fn compose_unsubscribe_frame(
    url: WsUrl,
    channel: ChannelName,
    pair: Option<&StreamSymbol>,
    params: Option<SubscribeParams>,
    token: Option<&str>,
) -> String {
    let pairs: &[StreamSymbol] = pair.map(std::slice::from_ref).unwrap_or_default();
    let mut frame = if let Some(params) = params {
        // req_id `0`: unsubscribe carries no reject-correlation id.
        build_subscribe_frame(url, "unsubscribe", channel, pairs, params, 0)
    } else {
        let p = crate::conn::subscription_registry::frame_params_base(url, channel, pairs);
        serde_json::json!({ "method": "unsubscribe", "params": Value::Object(p), "req_id": 0 })
    };
    // Auth-channel unsubscribe MUST carry the token — a tokenless auth unsubscribe is
    // silently ignored by Kraken and the sub keeps streaming.
    if let Some(tok) = token {
        crate::conn::subscription_registry::inject_token(&mut frame, tok);
    }
    frame.to_string()
}

/// On a `(channel, pair)` refcount-0 transition: send the wire `unsubscribe`
/// (best-effort) and emit the product-specific termination event with
/// `cause: ClientClosed`.
pub(in crate::dispatch::io_reactor) fn unsubscribe_and_terminate(
    url: WsUrl,
    channel: ChannelName,
    pair: Option<StreamSymbol>,
    params: Option<SubscribeParams>,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
    auth_stack: &Arc<crate::auth::AuthStack>,
) {
    // Empty cache → tokenless best-effort; teardown may not land until reconnect.
    let cached = url
        .requires_ws_token()
        .then(|| auth_stack.cached_token())
        .flatten();
    let token = cached.as_deref().map(|t| t.value());
    if let Some(socket) = sockets.get(&url) {
        let frame = WsFrame {
            opcode: WsOpcode::Text,
            payload: compose_unsubscribe_frame(url, channel, pair.as_ref(), params, token)
                .into_bytes(),
        };
        if let Err(e) = socket.send_frame(frame) {
            tracing::warn!(
                target: "kraken_sdk::io_reactor", ?url, ?channel, ?pair, error = ?e,
                "unsubscribe frame send failed on refcount-0 teardown (queue full or closed)"
            );
        }
    }
    emit_subscription_terminated(url, channel, pair, bus_back_ref);
}

/// Emit the logical termination half of a subscription teardown without sending
/// an unsubscribe frame. Unified Public maintained/raw books share one wire
/// stream but retain separate logical ownership rows.
pub(in crate::dispatch::io_reactor) fn emit_subscription_terminated(
    url: WsUrl,
    channel: ChannelName,
    pair: Option<StreamSymbol>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    if let Some(bus) = bus_back_ref.upgrade() {
        bus.publish_subscription_terminated(
            bus.clock().now(),
            url,
            channel,
            pair,
            crate::api::subscription::TerminationCause::ClientClosed,
            None,
        );
    }
}

/// Send a bare `unsubscribe` for the order-book gap reseed. Does NOT decrement
/// the refcount or emit `SubscriptionTerminatedEvent` — reseed, not teardown.
pub(in crate::dispatch::io_reactor) fn send_single_unsubscribe(
    url: WsUrl,
    channel: ChannelName,
    pair: Option<StreamSymbol>,
    params: SubscribeParams,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
) {
    if let Some(socket) = sockets.get(&url) {
        let frame = WsFrame {
            opcode: WsOpcode::Text,
            payload: compose_unsubscribe_frame(url, channel, pair.as_ref(), Some(params), None)
                .into_bytes(),
        };
        if let Err(e) = socket.send_frame(frame) {
            tracing::warn!(
                target: "kraken_sdk::io_reactor", ?url, ?channel, ?pair, error = ?e,
                "order-book gap-reseed unsubscribe send failed (queue full or closed); resubscribe still attempts"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{compose_unsubscribe_frame, emit_subscription_terminated};
    use crate::conn::subscription_registry::SubscribeParams;
    use crate::types::{BookDepth, ChannelName, StreamSymbol, Symbol, WsUrl};
    use std::sync::Arc;

    #[test]
    fn teardown_termination_event_follows_the_connection_family() {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
        let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            clock,
        ));
        let terminated = |url: WsUrl| {
            emit_subscription_terminated(url, ChannelName::Executions, None, &Arc::downgrade(&bus));
            bus.test_drain_published()
                .into_iter()
                .map(|event| event.event_type)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            terminated(WsUrl::UnifiedPrivate),
            vec![crate::dispatch::EventType::FuturesSubscriptionTerminatedEvent]
        );
        assert_eq!(
            terminated(WsUrl::UnifiedPublic),
            vec![crate::dispatch::EventType::FuturesSubscriptionTerminatedEvent]
        );
        assert_eq!(
            terminated(WsUrl::Auth),
            vec![crate::dispatch::EventType::SubscriptionTerminatedEvent]
        );
    }

    #[test]
    fn paramless_unsubscribe_sends_derivatives_except_for_unified_private_balances() {
        let asset_class = |url, channel| {
            let frame: serde_json::Value = serde_json::from_str(&compose_unsubscribe_frame(
                url,
                channel,
                None,
                None,
                Some("token"),
            ))
            .unwrap();
            frame["params"].get("asset_class").cloned()
        };
        assert_eq!(
            asset_class(WsUrl::UnifiedPrivate, ChannelName::Executions),
            Some(serde_json::json!("derivatives"))
        );
        assert_eq!(
            asset_class(WsUrl::UnifiedPublic, ChannelName::Instrument),
            Some(serde_json::json!("derivatives"))
        );
        assert_eq!(
            asset_class(WsUrl::UnifiedPrivate, ChannelName::Balances),
            None,
            "the venue rejects asset_class on the shared balances channel"
        );
        assert_eq!(asset_class(WsUrl::Auth, ChannelName::Executions), None);
    }

    #[test]
    fn unsubscribe_frame_carries_matching_depth_and_no_snapshot() {
        let sym = StreamSymbol::from(Symbol::new("BTC/USD").unwrap());
        let parse = |s: String| serde_json::from_str::<serde_json::Value>(&s).unwrap();

        let book = parse(compose_unsubscribe_frame(
            WsUrl::Public,
            ChannelName::Book,
            Some(&sym),
            Some(SubscribeParams::Book {
                depth: BookDepth::D10,
            }),
            None,
        ));
        assert_eq!(book["method"], "unsubscribe");
        assert_eq!(book["params"]["channel"], "book");
        assert_eq!(book["params"]["symbol"], serde_json::json!(["BTC/USD"]));
        assert_eq!(
            book["params"]["depth"], 25,
            "book unsubscribe must carry the wire depth (D10→25)"
        );
        assert!(
            book["params"].get("snapshot").is_none(),
            "unsubscribe must never carry snapshot"
        );

        let raw = parse(compose_unsubscribe_frame(
            WsUrl::Public,
            ChannelName::Book,
            Some(&sym),
            Some(SubscribeParams::BookRaw {
                depth: BookDepth::D10,
                snapshot: None,
            }),
            None,
        ));
        assert_eq!(
            raw["params"]["depth"], 10,
            "book_raw unsubscribe is exact depth"
        );

        let ticker = parse(compose_unsubscribe_frame(
            WsUrl::Public,
            ChannelName::Ticker,
            Some(&sym),
            Some(SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            }),
            None,
        ));
        assert_eq!(ticker["method"], "unsubscribe");
        assert!(ticker["params"].get("depth").is_none());

        let ohlc = parse(compose_unsubscribe_frame(
            WsUrl::Public,
            ChannelName::Ohlc,
            Some(&sym),
            Some(SubscribeParams::Ohlc {
                interval: crate::api::market::OhlcInterval::M5,
                snapshot: Some(false),
                tick_type: None,
            }),
            None,
        ));
        assert_eq!(ohlc["method"], "unsubscribe");
        assert_eq!(
            ohlc["params"]["interval"], 5,
            "OHLC unsubscribe must echo the subscribed interval (keyed per-interval)"
        );
        assert!(
            ohlc["params"].get("snapshot").is_none(),
            "snapshot is subscribe-only"
        );

        let fallback = parse(compose_unsubscribe_frame(
            WsUrl::Public,
            ChannelName::Book,
            Some(&sym),
            None,
            None,
        ));
        assert_eq!(fallback["method"], "unsubscribe");
        assert_eq!(fallback["params"]["channel"], "book");
        assert!(fallback["params"].get("depth").is_none());
    }

    #[test]
    fn auth_unsubscribe_frame_carries_token_public_does_not() {
        let parse = |s: String| serde_json::from_str::<serde_json::Value>(&s).unwrap();

        let exec = parse(compose_unsubscribe_frame(
            WsUrl::Auth,
            ChannelName::Executions,
            None,
            Some(SubscribeParams::Executions {
                snap_orders: None,
                snap_trades: None,
            }),
            Some("tok-XYZ"),
        ));
        assert_eq!(exec["method"], "unsubscribe");
        assert_eq!(exec["params"]["channel"], "executions");
        assert_eq!(
            exec["params"]["token"], "tok-XYZ",
            "auth unsubscribe must carry the token"
        );

        let sym = StreamSymbol::from(Symbol::new("BTC/USD").unwrap());
        let ticker = parse(compose_unsubscribe_frame(
            WsUrl::Public,
            ChannelName::Ticker,
            Some(&sym),
            None,
            None,
        ));
        assert!(
            ticker["params"].get("token").is_none(),
            "public unsubscribe must not carry a token"
        );
    }

    #[test]
    fn unified_ohlc_unsubscribe_frame_carries_the_given_interval_and_derivatives() {
        let sym = StreamSymbol::Futures("PF_XBTUSD".to_string());
        let frame = compose_unsubscribe_frame(
            WsUrl::UnifiedPublic,
            ChannelName::Ohlc,
            Some(&sym),
            Some(SubscribeParams::Ohlc {
                interval: crate::api::market::OhlcInterval::M15,
                snapshot: Some(true),
                tick_type: None,
            }),
            None,
        );
        let json: serde_json::Value = serde_json::from_str(&frame).unwrap();

        assert_eq!(json["method"], "unsubscribe");
        assert_eq!(json["params"]["asset_class"], "derivatives");
        assert_eq!(json["params"]["interval"], 15);
        assert_eq!(json["params"]["symbol"], serde_json::json!(["PF_XBTUSD"]));
        assert!(json["params"].get("snapshot").is_none());
    }

    #[test]
    fn unified_unsubscribe_frame_stamps_derivatives() {
        let sym = StreamSymbol::from(Symbol::new("BTC/USD").unwrap());
        let frame = compose_unsubscribe_frame(
            WsUrl::UnifiedPublic,
            ChannelName::Ticker,
            Some(&sym),
            Some(SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            }),
            None,
        );
        let json: serde_json::Value = serde_json::from_str(&frame).unwrap();

        assert_eq!(json["params"]["asset_class"], "derivatives");
    }

    #[test]
    fn a_unified_book_unsubscribe_carries_the_subscribed_wire_depth() {
        let sym = StreamSymbol::Futures("PF_XBTUSD".to_string());
        // (channel, stored subscribe params, wire depth the venue holds)
        let cases = [
            (
                ChannelName::Book,
                SubscribeParams::Book {
                    depth: BookDepth::D10,
                },
                25,
            ),
            (
                ChannelName::Book,
                SubscribeParams::Book {
                    depth: BookDepth::D100,
                },
                100,
            ),
            (
                ChannelName::BookRaw,
                SubscribeParams::BookRaw {
                    depth: BookDepth::D10,
                    snapshot: Some(true),
                },
                10,
            ),
            (
                ChannelName::BookRaw,
                SubscribeParams::BookRaw {
                    depth: BookDepth::D1000,
                    snapshot: None,
                },
                1000,
            ),
        ];
        for (channel, params, depth) in cases {
            let frame: serde_json::Value = serde_json::from_str(&compose_unsubscribe_frame(
                WsUrl::UnifiedPublic,
                channel,
                Some(&sym),
                Some(params),
                None,
            ))
            .unwrap();
            assert_eq!(
                (
                    frame["method"].as_str(),
                    frame["params"]["channel"].as_str(),
                    frame["params"]["depth"].as_u64(),
                    frame["params"]["symbol"].clone(),
                    frame["params"].get("snapshot").is_none(),
                ),
                (
                    Some("unsubscribe"),
                    Some("book"),
                    Some(depth),
                    serde_json::json!(["PF_XBTUSD"]),
                    true,
                ),
                "{channel:?} {params:?}"
            );
        }
    }
}
