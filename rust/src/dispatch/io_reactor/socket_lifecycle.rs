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
use crate::types::{ChannelName, ConnectionState, Symbol, WsUrl};

use super::UpgradeOutcome;

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
            if let crate::dispatch::EventPayload::WsUpgradeOk { connection_id, url } = env.payload {
                if let Err(e) = tx_ok.try_send(UpgradeOutcome::Ok { url, connection_id }) {
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
            if Some(cid) != prev_cid {
                if let Some(bus) = bus_back_ref.upgrade() {
                    upgrade_guards.insert(url, wire_upgrade_bridge(cid, &bus, upgrade_tx));
                }
            }
        }
        None => {
            sockets.remove(&url);
            upgrade_guards.remove(&url);
        }
    }
}

/// Resolve the owning WS connection for `channel`: `executions`/`balances` → Auth;
/// everything else → Public.
pub(in crate::dispatch::io_reactor) fn channel_ws_url(channel: ChannelName) -> WsUrl {
    match channel {
        ChannelName::Executions | ChannelName::Balances => WsUrl::Auth,
        _ => WsUrl::Public,
    }
}

/// Compose the Kraken WS v2 `unsubscribe` frame for `(channel, pair)`, routed
/// through `build_subscribe_frame` (depth/interval can't drift from subscribe);
/// `token` injected for auth channels.
pub(in crate::dispatch::io_reactor) fn compose_unsubscribe_frame(
    channel: ChannelName,
    pair: Option<&Symbol>,
    params: Option<SubscribeParams>,
    token: Option<&str>,
) -> String {
    let mut frame = if let Some(params) = params {
        let pairs: Vec<Symbol> = pair.cloned().into_iter().collect();
        // req_id `0`: unsubscribe carries no reject-correlation id.
        build_subscribe_frame("unsubscribe", channel, &pairs, params, 0)
    } else {
        let mut p = serde_json::Map::new();
        p.insert("channel".to_string(), Value::String(channel.to_string()));
        if let Some(sym) = pair {
            p.insert("symbol".to_string(), serde_json::json!([sym.as_str()]));
        }
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
/// (best-effort) and emit `SubscriptionTerminatedEvent { cause: ClientClosed }`.
pub(in crate::dispatch::io_reactor) fn unsubscribe_and_terminate(
    channel: ChannelName,
    pair: Option<Symbol>,
    params: Option<SubscribeParams>,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
    auth_stack: &Arc<crate::auth::AuthStack>,
) {
    let url = channel_ws_url(channel);
    // Empty cache → tokenless best-effort; teardown may not land until reconnect.
    let cached = (url == WsUrl::Auth)
        .then(|| auth_stack.cached_token())
        .flatten();
    let token = cached.as_deref().map(|t| t.value());
    if let Some(socket) = sockets.get(&url) {
        let frame = WsFrame {
            opcode: WsOpcode::Text,
            payload: compose_unsubscribe_frame(channel, pair.as_ref(), params, token).into_bytes(),
        };
        if let Err(e) = socket.send_frame(frame) {
            tracing::warn!(
                target: "kraken_sdk::io_reactor", ?url, ?channel, ?pair, error = ?e,
                "unsubscribe frame send failed on refcount-0 teardown (queue full or closed)"
            );
        }
    }
    if let Some(bus) = bus_back_ref.upgrade() {
        let now = bus.clock().now();
        bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::SubscriptionTerminatedEvent,
            event_version: 2,
            timestamp_monotonic: now,
            request_id: None,
            payload: crate::dispatch::EventPayload::SubscriptionTerminatedEvent {
                channel,
                pair,
                cause: crate::api::subscription::TerminationCause::ClientClosed,
                last_error: None,
                terminated_at_monotonic: now,
            },
        });
    }
}

/// Send a bare `unsubscribe` for the order-book gap reseed. Does NOT decrement
/// the refcount or emit `SubscriptionTerminatedEvent` — reseed, not teardown.
pub(in crate::dispatch::io_reactor) fn send_single_unsubscribe(
    url: WsUrl,
    channel: ChannelName,
    pair: Option<Symbol>,
    params: SubscribeParams,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
) {
    if let Some(socket) = sockets.get(&url) {
        let frame = WsFrame {
            opcode: WsOpcode::Text,
            payload: compose_unsubscribe_frame(channel, pair.as_ref(), Some(params), None)
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
    use super::compose_unsubscribe_frame;
    use crate::conn::subscription_registry::SubscribeParams;
    use crate::types::{BookDepth, ChannelName, Symbol};

    #[test]
    fn unsubscribe_frame_carries_matching_depth_and_no_snapshot() {
        let sym = Symbol::new("BTC/USD").unwrap();
        let parse = |s: String| serde_json::from_str::<serde_json::Value>(&s).unwrap();

        let book = parse(compose_unsubscribe_frame(
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
            ChannelName::Ohlc,
            Some(&sym),
            Some(SubscribeParams::Ohlc {
                interval: crate::api::market::OhlcInterval::M5,
                snapshot: Some(false),
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
            ChannelName::Executions,
            None,
            Some(SubscribeParams::Executions),
            Some("tok-XYZ"),
        ));
        assert_eq!(exec["method"], "unsubscribe");
        assert_eq!(exec["params"]["channel"], "executions");
        assert_eq!(
            exec["params"]["token"], "tok-XYZ",
            "auth unsubscribe must carry the token"
        );

        let sym = Symbol::new("BTC/USD").unwrap();
        let ticker = parse(compose_unsubscribe_frame(
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
}
