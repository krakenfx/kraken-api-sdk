//! Inbound-frame routing: classify a text frame as a WS request response, a
//! subscribe/unsubscribe ack, or data fan-out.

use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Weak};

use serde_json::Value;

use crate::api::ws_surface::SubscriberGuard;
use crate::conn::managed_connection::WsResponse;
use crate::conn::subscription_registry::{BookDriveOutcome, SubscriptionRegistry};
use crate::conn::{AuthErrorKind, ManagedConnection};
use crate::dispatch::WsOp;
use crate::dispatch::event_bus::{DispatchEventBus, truncate_panic_message};
use crate::dispatch::handler_registry::HandlerRegistry;
use crate::transport::{TransportError, TransportErrorKind, WsFrame, WsOpcode, WsSocket};
use crate::types::{ChannelName, ConnectionState, Symbol, WsUrl};

use tokio::sync::mpsc;

use super::UpgradeOutcome;
use super::socket_lifecycle::wire_upgrade_bridge;
use super::write_mirror;

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_frame(
    url: WsUrl,
    frame_result: Result<WsFrame, TransportError>,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    handler_registry: &HandlerRegistry,
    auth_stack: &Arc<crate::auth::AuthStack>,
    auth_send_ready: &Arc<std::sync::atomic::AtomicBool>,
    bus_back_ref: &Weak<DispatchEventBus>,
    drop_tracker: &mut HashMap<ChannelName, (crate::types::MonotonicInstant, u32)>,
    malformed_warn_tracker: &mut SuppressWarnTracker,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    state_mirrors: &HashMap<WsUrl, Arc<AtomicU8>>,
    upgrade_guards: &mut HashMap<WsUrl, SubscriberGuard>,
    upgrade_tx: &mpsc::Sender<UpgradeOutcome>,
) {
    match frame_result {
        Ok(WsFrame {
            opcode: WsOpcode::Text,
            payload,
        }) => {
            // Staleness reset BEFORE classification: every text frame counts as activity.
            if let Some(mc) = conns.get_mut(&url) {
                mc.note_inbound_activity();
            }
            let json: Value = match serde_json::from_slice(&payload) {
                Ok(v) => v,
                Err(_) => return,
            };
            // Request responses are checked FIRST, gated on a recorded pending;
            // status frames and subscribe acks fall through.
            if handle_request_response(
                url,
                &json,
                conns,
                registry,
                sockets,
                auth_stack,
                auth_send_ready,
                state_mirrors,
                bus_back_ref,
                malformed_warn_tracker,
            ) {
                return;
            }
            if handle_sub_ack(
                url,
                &json,
                conns,
                registry,
                sockets,
                auth_stack,
                auth_send_ready,
                bus_back_ref,
                state_mirrors,
                malformed_warn_tracker,
            ) {
                return;
            }
            let gaps = route_text_frame(
                url,
                &json,
                registry,
                handler_registry,
                bus_back_ref,
                drop_tracker,
                malformed_warn_tracker,
            );
            for gap in gaps {
                recover_book_gap(url, gap, conns, registry, sockets, bus_back_ref);
            }
        }
        Ok(_) => {
            if let Some(mc) = conns.get_mut(&url) {
                mc.note_inbound_activity();
            }
        }
        Err(TransportError {
            kind: TransportErrorKind::CloseFrame { code, reason },
            ..
        }) => {
            tracing::info!(
                target: "kraken_sdk::io_reactor",
                ?url, code, %reason,
                "socket closed; driving FSM"
            );
            drive_wire_close(
                url,
                crate::conn::managed_connection::FsmEvent::WireCloseReceived {
                    code,
                    reason: Some(reason),
                },
                conns,
                sockets,
                state_mirrors,
                upgrade_guards,
                bus_back_ref,
                upgrade_tx,
            );
        }
        Err(e) => {
            tracing::warn!(
                target: "kraken_sdk::io_reactor",
                ?url, error = ?e,
                "recv_frame error; driving FSM (abnormal close)"
            );
            drive_wire_close(
                url,
                crate::conn::managed_connection::FsmEvent::WireAbnormalClose { error: e },
                conns,
                sockets,
                state_mirrors,
                upgrade_guards,
                bus_back_ref,
                upgrade_tx,
            );
        }
    }
}

/// Route a WS request response to its pending [`WsResponse`] handle. Returns `true` iff consumed.
#[allow(clippy::too_many_arguments)]
fn handle_request_response(
    url: WsUrl,
    json: &Value,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    auth_stack: &Arc<crate::auth::AuthStack>,
    auth_send_ready: &Arc<std::sync::atomic::AtomicBool>,
    state_mirrors: &HashMap<WsUrl, Arc<AtomicU8>>,
    bus_back_ref: &Weak<DispatchEventBus>,
    malformed_warn_tracker: &mut SuppressWarnTracker,
) -> bool {
    if url != WsUrl::Auth {
        return false;
    }
    let is_request_method = json
        .get("method")
        .and_then(Value::as_str)
        .and_then(WsOp::from_method)
        .is_some();
    if !is_request_method {
        return false;
    }
    let Some(req_id) = json.get("req_id").and_then(Value::as_u64) else {
        suppress_warn_throttled(
            "req-resp-no-req-id",
            None,
            malformed_warn_tracker,
            bus_back_ref,
            || {
                tracing::warn!(
                    target: "kraken_sdk::io_reactor", ?url,
                    "request-response frame with no numeric req_id; consumed without resolve"
                )
            },
        );
        return true;
    };
    // Absent/non-bool `success` defaults to false — the SAFE direction (never
    // fabricate a success/fill).
    let success_opt = json.get("success").and_then(Value::as_bool);
    if success_opt.is_none() {
        suppress_warn_throttled(
            "req-resp-no-success",
            None,
            malformed_warn_tracker,
            bus_back_ref,
            || {
                tracing::warn!(
                    target: "kraken_sdk::io_reactor", ?url, req_id,
                    "request-response frame with absent/non-bool `success`; treating as failure"
                )
            },
        );
    }
    let success = success_opt.unwrap_or(false);
    let error = json
        .get("error")
        .and_then(Value::as_str)
        .map(|s| s.to_string());
    let result = json.get("result").cloned().unwrap_or(Value::Null);
    let resp = WsResponse {
        req_id,
        success,
        result,
        error,
    };
    let Some(mc) = conns.get_mut(&url) else {
        return true;
    };

    // Classified once per frame so the novel-string warn never double-fires on a
    // probe->Open frame.
    let order_ack_auth = classify_order_ack_warned(success, resp.error.as_deref());

    // An order ack mid-Authenticating IS the probe ack — drives the FSM,
    // resolves the future.
    let in_auth_probe = mc.state() == ConnectionState::Authenticating && url == WsUrl::Auth;
    if in_auth_probe {
        match order_ack_auth {
            OrderAckAuth::Authenticated => {
                if registry.has_url_entry(WsUrl::Auth) {
                    mc.handle_event(crate::conn::managed_connection::FsmEvent::WireAuthHandshakeOk);
                    if mc.state() == ConnectionState::Resubscribing {
                        super::send_remaining_signed_subscribes(
                            url,
                            mc,
                            registry,
                            sockets,
                            auth_stack,
                            bus_back_ref,
                            None,
                        );
                        mc.complete_resubscribe_if_empty();
                    }
                } else {
                    mc.handle_event(crate::conn::managed_connection::FsmEvent::WireOrderAuthOk);
                }
            }
            OrderAckAuth::AuthFailed(kind) => {
                // Socket-teardown arms drain the pending map before their emit; a
                // token-stale re-enter does NOT — drain retryable on every stale.
                let was_token_stale = kind == crate::conn::AuthErrorKind::TokenStale;
                mc.handle_event(
                    crate::conn::managed_connection::FsmEvent::WireAuthHandshakeFailed { kind },
                );
                // Drain this order RETRYABLE; never auto-resend (double-fill avoidance).
                if was_token_stale {
                    mc.drain_one_retryable(req_id);
                }
            }
        }
        write_mirror(state_mirrors, url, mc.state());
        // STORE-ONLY recompute: the genuine re-emit is owned by the token-refreshed
        // arm.
        super::store_send_ready(url, mc, registry, auth_stack, auth_send_ready);
    }

    // Steady-state Open token-stale recovery (must NOT widen in_auth_probe): FSM
    // self-loop (no teardown), invalidate token, force_refresh for the NEXT order,
    // drain this one RETRYABLE (never resent).
    if mc.state() == ConnectionState::Open && url == WsUrl::Auth {
        if let OrderAckAuth::AuthFailed(AuthErrorKind::TokenStale) = order_ack_auth {
            mc.handle_event(crate::conn::managed_connection::FsmEvent::WireOrderAckTokenStale);
            auth_stack.invalidate_cached_token();
            mc.set_awaiting_token_refresh(true);
            let _handle = auth_stack.force_refresh(crate::auth::RefreshReason::AuthHandshakeFailed);
            mc.drain_one_retryable(req_id);
            write_mirror(state_mirrors, url, mc.state());
        }
    }

    if mc.pending_requests_contains(req_id) {
        mc.resolve_pending_request(resp);
    }
    true
}

/// Dispatch a wire-close FSM event, then reproject the `sockets` race-set. The
/// dead socket is removed UP FRONT so a no-arm close can't busy-loop.
#[allow(clippy::too_many_arguments)]
fn drive_wire_close(
    url: WsUrl,
    event: crate::conn::managed_connection::FsmEvent,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    state_mirrors: &HashMap<WsUrl, Arc<AtomicU8>>,
    upgrade_guards: &mut HashMap<WsUrl, SubscriberGuard>,
    bus_back_ref: &Weak<DispatchEventBus>,
    upgrade_tx: &mpsc::Sender<UpgradeOutcome>,
) {
    let dead_connection_id = sockets.remove(&url).map(|s| s.connection_id());
    upgrade_guards.remove(&url);
    if let Some(mc) = conns.get_mut(&url) {
        mc.handle_event(event);
        write_mirror(state_mirrors, url, mc.state());
        // Re-insert only a genuinely DIFFERENT live socket: the != dead guard
        // stops a no-arm close's dead socket re-racing.
        if let Some(s) = mc.socket() {
            let cid = s.connection_id();
            if Some(cid) != dead_connection_id {
                sockets.insert(url, Arc::clone(s));
                if let Some(bus) = bus_back_ref.upgrade() {
                    upgrade_guards.insert(url, wire_upgrade_bridge(cid, &bus, upgrade_tx));
                }
            }
        }
    }
}

/// Classify an auth-handshake ack error string by substring match; unknown →
/// `Transient`.
fn classify_auth_handshake_error(err: &str) -> AuthErrorKind {
    classify_auth_class(err).unwrap_or(AuthErrorKind::Transient)
}

/// The recognized AUTH-class arms shared by the subscribe-ack classifier
/// (default `Transient`) and the order-ack classifier (INVERTED default).
fn classify_auth_class(err: &str) -> Option<AuthErrorKind> {
    // ORDER MATTERS: token-stale is checked FIRST. Families are broad so novel
    // auth strings are caught (the inverted default would drive -> Open on a bad token).
    if err.contains("EAPI:Invalid token")
        || err.contains("ESession:Invalid session")
        || (err.contains("token")
            && (err.contains("stale")
                || err.contains("expired")
                || err.contains("expir")
                || err.contains("Invalid")
                || err.contains("invalid")))
    {
        return Some(AuthErrorKind::TokenStale);
    }
    // Checked before the generic permission family so the EAPI prefix keeps
    // BadCreds (EGeneral:Permission denied is the distinct PermissionDenied row).
    if err.contains("EAPI:Permission denied") {
        return Some(AuthErrorKind::BadCreds);
    }
    if err.contains("Permission denied")
        || err.contains("permission denied")
        || err.contains("EGeneral:Permission")
    {
        return Some(AuthErrorKind::PermissionDenied);
    }
    if err.contains("EAPI:Invalid")
        || err.contains("EAuth:")
        || err.contains("EAPI:Bad")
        || err.contains("Invalid key")
        || err.contains("Invalid signature")
    {
        return Some(AuthErrorKind::BadCreds);
    }
    None
}

/// Order-ack-during-probe: whether the TOKEN was accepted, not whether the order filled.
#[derive(Clone, Copy)]
enum OrderAckAuth {
    /// The token was accepted (order ok, or rejected for a non-auth reason).
    Authenticated,
    /// The token was rejected — the FSM must NOT authenticate.
    AuthFailed(AuthErrorKind),
}

/// Classify an order-ack auth result. PURE — warn lives in [`classify_order_ack_warned`].
fn classify_order_ack_auth(success: bool, err: Option<&str>) -> OrderAckAuth {
    if success {
        return OrderAckAuth::Authenticated;
    }
    match err.and_then(classify_auth_class) {
        Some(kind) => OrderAckAuth::AuthFailed(kind),
        // INVERTED default: a non-auth order error still means the token was
        // accepted. RISK: a novel auth string lands here as Authenticated — warn at call site.
        None => OrderAckAuth::Authenticated,
    }
}

/// True iff the INVERTED default fires on an UNrecognized error worth a warn: a
/// non-`E*`-prefixed string OR no error string.
fn order_ack_default_authenticated_is_unrecognized(success: bool, err: Option<&str>) -> bool {
    if success {
        return false;
    }
    if err.and_then(classify_auth_class).is_some() {
        return false;
    }
    err.is_none() || err.is_some_and(|e| !e.starts_with('E'))
}

/// Classify AND emit the novel-string warn at most once per frame.
fn classify_order_ack_warned(success: bool, err: Option<&str>) -> OrderAckAuth {
    if order_ack_default_authenticated_is_unrecognized(success, err) {
        tracing::warn!(
            target: "kraken_sdk::io_reactor", error = ?err,
            "order-ack error not recognized as auth-class (non-`E*`-prefixed or absent); \
             defaulting to Authenticated — novel auth string would slip through"
        );
    }
    classify_order_ack_auth(success, err)
}

/// Emit the per-failure `AuthHandshakeFailedEvent`.
fn emit_auth_handshake_failed(
    bus_back_ref: &Weak<DispatchEventBus>,
    url: WsUrl,
    attempt: u32,
    kind: AuthErrorKind,
    err_msg: &str,
) {
    if let Some(bus) = bus_back_ref.upgrade() {
        bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::AuthHandshakeFailedEvent,
            event_version: 1,
            timestamp_monotonic: bus.clock().now(),
            request_id: None,
            payload: crate::dispatch::EventPayload::AuthHandshakeFailedEvent {
                url,
                attempt,
                transient: matches!(kind, AuthErrorKind::Transient),
                kraken_error: Some(err_msg.to_string()),
            },
        });
    }
}

/// Translate a subscribe/unsubscribe ack into FSM events. Returns `true` if consumed.
#[allow(clippy::too_many_arguments)]
fn handle_sub_ack(
    url: WsUrl,
    json: &Value,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    auth_stack: &Arc<crate::auth::AuthStack>,
    auth_send_ready: &Arc<std::sync::atomic::AtomicBool>,
    bus_back_ref: &Weak<DispatchEventBus>,
    state_mirrors: &HashMap<WsUrl, Arc<AtomicU8>>,
    malformed_warn_tracker: &mut SuppressWarnTracker,
) -> bool {
    let method = match json.get("method").and_then(Value::as_str) {
        Some(m @ ("subscribe" | "unsubscribe")) => m,
        _ => return false,
    };
    if method == "unsubscribe" {
        return true;
    }
    // Absent/non-bool `success` is a FAILURE (SAFE direction — never a silent
    // success).
    let success_opt = json.get("success").and_then(Value::as_bool);
    if success_opt.is_none() {
        let site = match url {
            WsUrl::Auth => "sub-ack-no-success-auth",
            _ => "sub-ack-no-success-public",
        };
        suppress_warn_throttled(site, None, malformed_warn_tracker, bus_back_ref, || {
            tracing::warn!(
                target: "kraken_sdk::io_reactor", ?url,
                "subscribe-ack frame with absent/non-bool `success`; treating as failure"
            )
        });
    }
    let success = success_opt.unwrap_or(false);
    // SUCCESS acks nest `channel`/`symbol` under `result`; a FAILURE may carry
    // them top-level. Read `result`-first.
    let result = json.get("result");
    let field = |name: &str| result.and_then(|r| r.get(name)).or_else(|| json.get(name));
    let wire_channel = field("channel")
        .and_then(Value::as_str)
        .and_then(ChannelName::from_wire_str);
    let echoed_pair = field("symbol")
        .and_then(Value::as_str)
        .and_then(|s| Symbol::new(s).ok());
    let req_id = json.get("req_id").and_then(Value::as_u64);
    let Some(mc) = conns.get_mut(&url) else {
        return true;
    };
    // Channel-less rejects correlate by req_id.
    let (channel, pair) = match wire_channel {
        Some(ch) => (ch, echoed_pair),
        None => match req_id.and_then(|id| mc.resolve_subscribe_req_id(id)) {
            Some((ch, mapped_pair)) => (ch, mapped_pair),
            None => {
                return true;
            }
        },
    };
    // While Authenticating this IS the first-signed-subscribe ack.
    if mc.state() == ConnectionState::Authenticating {
        if success {
            mc.handle_event(crate::conn::managed_connection::FsmEvent::WireAuthHandshakeOk);
            mc.disarm_subscribe_ack(channel, pair.clone());
            // No later ack covers the first signed (channel, pair) — drive its
            // entry state Acked here.
            registry.note_acked(channel, &pair);
            // Drop any deferral record, else a later refresh drain re-subscribes a
            // live entry.
            mc.remove_deferred_open_auth_subscribe(channel, &pair);
            if mc.state() == ConnectionState::Resubscribing {
                super::send_remaining_signed_subscribes(
                    url,
                    mc,
                    registry,
                    sockets,
                    auth_stack,
                    bus_back_ref,
                    Some((channel, pair.clone())),
                );
                mc.complete_resubscribe_if_empty();
            }
        } else {
            // On a token-stale re-enter the reactive force_refresh must spawn,
            // else nothing re-issues the first signed subscribe.
            let err_msg = json
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("auth handshake rejected");
            let kind = classify_auth_handshake_error(err_msg);
            emit_auth_handshake_failed(bus_back_ref, url, mc.attempt_count(), kind, err_msg);
            mc.handle_event(
                crate::conn::managed_connection::FsmEvent::WireAuthHandshakeFailed { kind },
            );
            mc.disarm_subscribe_ack(channel, pair.clone());
            if mc.state() == ConnectionState::Authenticating && mc.awaiting_token_refresh() {
                // The two force_refresh sites are MUTUALLY EXCLUSIVE per handshake
                // attempt.
                let _ = auth_stack.force_refresh(crate::auth::RefreshReason::AuthHandshakeFailed);
            }
        }
        write_mirror(state_mirrors, url, mc.state());
        super::refresh_send_ready(url, mc, registry, auth_stack, auth_send_ready, bus_back_ref);
        return true;
    }
    // A spurious/duplicate/late/never-armed ack must NOT drive the FSM; still
    // consume it.
    if !mc.is_subscribe_ack_armed(channel, pair.clone()) {
        // A late SUCCESS proves the wire subscription exists: kill the retry chain
        // + deferral record (else either re-subscribes a live entry).
        if success {
            mc.disarm_subscribe_ack(channel, pair.clone());
            mc.remove_deferred_open_auth_subscribe(channel, &pair);
            mc.complete_resubscribe_if_empty();
            write_mirror(state_mirrors, url, mc.state());
        }
        return true;
    }
    // status subscribe completes via a spurious "Symbol(s) not found" reject (no success ack) -- treat as the ack.
    let status_quirk_ack = channel == ChannelName::Status
        && !success
        && json
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(|e| e.contains("Symbol(s) not found"));
    if success || status_quirk_ack {
        let last = mc.pending_subscribe_ack_count() <= 1;
        // Pending → Acked (idempotent; tombstones are never replayed on reconnect).
        registry.note_acked(channel, &pair);
        mc.remove_deferred_open_auth_subscribe(channel, &pair);
        mc.handle_event(
            crate::conn::managed_connection::FsmEvent::WireSubscribeAck {
                channel,
                pair,
                last,
            },
        );
    } else {
        let err_msg = json
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("subscribe rejected")
            .to_string();
        let transient = subscribe_rejection_is_transient(&err_msg);
        // Tombstone only a truly non-transient reject: a transient one retries
        // under the ack budget.
        let terminal = !transient;
        if terminal {
            registry.note_terminated(
                channel,
                &pair,
                crate::api::subscription::TerminationCause::NonTransientWireRejection,
                Some(err_msg.clone()),
            );
            // The reseed liveness timer dies with the tombstone — a leftover timer
            // would fire into the tombstone.
            mc.disarm_book_reseed_snapshot(channel, pair.clone());
        }
        mc.handle_event(
            crate::conn::managed_connection::FsmEvent::WireSubscribeFailed {
                channel,
                pair,
                error: crate::conn::SubscribeErrorKind::SubscribeRejected {
                    kraken_code: err_msg,
                    transient,
                },
            },
        );
    }
    write_mirror(state_mirrors, url, mc.state());
    true
}

/// A rate-limit rejection is retryable; same substring set as the order classifiers.
fn subscribe_rejection_is_transient(err_msg: &str) -> bool {
    crate::error::is_kraken_rate_limit(err_msg)
}

/// Drive per-`(channel, pair)` subscription state, then fan out under panic isolation.
fn route_text_frame(
    _url: WsUrl,
    json: &Value,
    registry: &mut SubscriptionRegistry,
    handlers: &HandlerRegistry,
    bus_back_ref: &Weak<DispatchEventBus>,
    drop_tracker: &mut HashMap<ChannelName, (crate::types::MonotonicInstant, u32)>,
    malformed_warn_tracker: &mut SuppressWarnTracker,
) -> Vec<BookGap> {
    let mut gaps: Vec<BookGap> = Vec::new();
    #[cfg(test)]
    let reactor_start = super::latency_histogram::reactor_probe_start();

    let channel_str = json.get("channel").and_then(Value::as_str);
    let msg_type = json.get("type").and_then(Value::as_str);
    let Some(channel_str) = channel_str else {
        return gaps;
    };
    let Some(channel) = ChannelName::from_wire_str(channel_str) else {
        if matches!(msg_type, Some("update") | Some("snapshot")) {
            tracing::debug!(
                target: "kraken_sdk::io_reactor", channel = channel_str, msg_type,
                "unrecognized channel on a data frame; frame dropped (wire drift)"
            );
        }
        return gaps;
    };

    if !matches!(msg_type, Some("update") | Some("snapshot")) {
        return gaps;
    }

    // Sequence-gap detection BEFORE per-entry decode: a frame whose rows fail
    // decode was still received.
    if matches!(channel, ChannelName::Executions | ChannelName::Balances) {
        if let Some(seq) = json.get("sequence").and_then(Value::as_u64) {
            let is_snapshot = msg_type == Some("snapshot");
            if let Some(dropped) = registry.note_sequence(channel, seq, is_snapshot) {
                tracing::warn!(
                    target: "kraken_sdk::io_reactor", channel = channel_str, seq, dropped,
                    "sequence gap on account channel; frames lost mid-connection"
                );
                emit_channel_gap_event(channel, dropped, bus_back_ref);
            }
        }
    }

    let Some(data) = json.get("data").and_then(Value::as_array) else {
        // Non-array `data` = symbol-less malformed frame: throttled warn only; do
        // NOT emit MessageDroppedNoHandler.
        suppress_warn_throttled(
            "non-array-data",
            Some(channel),
            malformed_warn_tracker,
            bus_back_ref,
            || {
                tracing::warn!(
                    target: "kraken_sdk::io_reactor", ?channel, msg_type,
                    "channel frame with non-array `data`; no routing symbol — frame dropped, gap event suppressed"
                )
            },
        );
        return gaps;
    };
    let is_snapshot = msg_type == Some("snapshot");
    for entry in data {
        let symbol = entry
            .get("symbol")
            .and_then(Value::as_str)
            .and_then(|s| Symbol::new(s).ok());

        // Maintained-book state is driven once, on the WIRE channel (BookRaw has
        // no registry entry of its own).
        let outcome = registry.handle_update(channel, symbol.as_ref(), is_snapshot, entry);

        let mut with_type: Option<Value> = None;

        let maintained_update: Option<&crate::book::OrderBookUpdate> = match &outcome {
            BookDriveOutcome::Maintained(update) => Some(update),
            _ => None,
        };

        if matches!(
            outcome,
            BookDriveOutcome::Gap { .. } | BookDriveOutcome::GapBudgetExhausted
        ) {
            let will_resubscribe = matches!(outcome, BookDriveOutcome::Gap { .. });
            match symbol.clone() {
                Some(sym) => {
                    if let BookDriveOutcome::Gap { expected, computed } = outcome {
                        tracing::warn!(
                            target: "kraken_sdk::io_reactor", symbol = %sym.as_str(), expected, computed,
                            "order-book CRC32 mismatch; emitting gap + scheduling resubscribe-reseed"
                        );
                    } else {
                        tracing::warn!(
                            target: "kraken_sdk::io_reactor", symbol = %sym.as_str(),
                            "order-book CRC32 gap budget exhausted; maintenance dropped — on_book degraded to per-frame (un-validated)"
                        );
                    }
                    emit_book_gap_events(&sym, bus_back_ref);
                    if will_resubscribe {
                        gaps.push(BookGap { symbol: sym });
                    }
                }
                None => {
                    suppress_warn_throttled(
                        "crc-gap-no-symbol",
                        None,
                        malformed_warn_tracker,
                        bus_back_ref,
                        || {
                            tracing::warn!(
                                target: "kraken_sdk::io_reactor",
                                "order-book CRC32 gap with no routing symbol; gap event + resubscribe suppressed"
                            )
                        },
                    );
                }
            }
        }

        let book_targets = [ChannelName::Book, ChannelName::BookRaw];
        let targets: &[ChannelName] = if channel == ChannelName::Book {
            &book_targets
        } else {
            std::slice::from_ref(&channel)
        };
        for &fan_channel in targets {
            // Skip ONLY the maintained `Book` fan on a gap or during resync;
            // `BookRaw` still fans.
            if fan_channel == ChannelName::Book
                && matches!(
                    outcome,
                    BookDriveOutcome::Gap { .. }
                        | BookDriveOutcome::AwaitingSnapshot
                        | BookDriveOutcome::GapBudgetExhausted
                )
            {
                continue;
            }
            let handler_list = handlers.handlers_for(fan_channel);
            if handler_list.is_empty() {
                // No-handler drops count only for the PRIMARY wire channel; a book
                // frame drops only when BookRaw is ALSO unhandled. Status is suppressed
                // (auto-seeded, ~1/s).
                if fan_channel == channel
                    && channel != ChannelName::Status
                    && (channel != ChannelName::Book
                        || handlers.handlers_for(ChannelName::BookRaw).is_empty())
                {
                    record_drop_and_maybe_emit(channel, drop_tracker, bus_back_ref);
                }
                continue;
            }
            // Maintained Book fast-path: typed downcast, no encode/decode round-trip.
            if let (ChannelName::Book, Some(update)) = (fan_channel, maintained_update) {
                #[cfg(test)]
                let typed_delivery_start = super::latency_histogram::typed_delivery_probe_start();
                if let Some(bus) = bus_back_ref.upgrade() {
                    let payload: Arc<dyn std::any::Any + Send + Sync> = Arc::new(update.clone());
                    for h in handler_list {
                        bus.publish_data(
                            crate::dispatch::DataDelivery {
                                invoke: Arc::clone(&h.callback.invoke),
                                payload: Arc::clone(&payload),
                            },
                            crate::dispatch::event_bus::DataRouting {
                                channel: fan_channel,
                                handler_id: h.id,
                            },
                        );
                    }
                }
                #[cfg(test)]
                super::latency_histogram::typed_delivery_probe_end(typed_delivery_start);
                continue;
            }
            let Some(bus) = bus_back_ref.upgrade() else {
                continue;
            };
            let cb = Arc::clone(&handler_list[0].callback.decode);
            let build_envelope = || serde_json::json!({ "type": msg_type, "data": entry });
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                cb(with_type.take().unwrap_or_else(build_envelope))
            }));
            match result {
                Ok(crate::dispatch::HandlerDecodeOutcome::Deliver(payload)) => {
                    for h in handler_list {
                        bus.publish_data(
                            crate::dispatch::DataDelivery {
                                invoke: Arc::clone(&h.callback.invoke),
                                payload: Arc::clone(&payload),
                            },
                            crate::dispatch::event_bus::DataRouting {
                                channel: fan_channel,
                                handler_id: h.id,
                            },
                        );
                    }
                }
                Ok(crate::dispatch::HandlerDecodeOutcome::DecodeFailed) => match symbol.clone() {
                    Some(sym) => {
                        bus.publish(crate::dispatch::EventEnvelope {
                            event_type: crate::dispatch::EventType::SubscriptionGapEvent,
                            event_version: 2,
                            timestamp_monotonic: bus.clock().now(),
                            request_id: None,
                            payload: crate::dispatch::EventPayload::SubscriptionGapEvent {
                                channel: fan_channel,
                                symbol: sym,
                                dropped_count: 0,
                                cause: crate::dispatch::GapCause::MalformedFrame,
                            },
                        });
                    }
                    None => {
                        suppress_warn_throttled(
                            "decode-fail-no-symbol",
                            Some(fan_channel),
                            malformed_warn_tracker,
                            bus_back_ref,
                            || {
                                tracing::warn!(
                                    target: "kraken_sdk::io_reactor", ?fan_channel,
                                    "decode-failed frame with no routing symbol; gap event suppressed (symbol non-optional)"
                                )
                            },
                        );
                    }
                },
                Err(panic) => {
                    bus.publish(crate::dispatch::EventEnvelope {
                        event_type: crate::dispatch::EventType::HandlerPanicWarning,
                        event_version: 1,
                        timestamp_monotonic: bus.clock().now(),
                        request_id: None,
                        payload: crate::dispatch::EventPayload::HandlerPanicWarning {
                            source: crate::dispatch::CallbackSource::DataChannel(fan_channel),
                            handler_id: handler_list[0].id,
                            panic_message: truncate_panic_message(&panic),
                        },
                    });
                }
            }
        }
    }
    #[cfg(test)]
    super::latency_histogram::reactor_probe_end(reactor_start);
    gaps
}

/// A `(Book, symbol)` whose CRC32 mismatched this frame.
pub(super) struct BookGap {
    pub symbol: Symbol,
}

/// CRC32-gap recovery for one maintained book: drop local state, enter the
/// resync window, send a WS unsubscribe+resubscribe on the `(Book, symbol)`, and
/// arm the reseed snapshot-liveness timer.
fn recover_book_gap(
    url: WsUrl,
    gap: BookGap,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    // Drop the book + enter resync BEFORE the connection lookup, so the book is
    // dropped even if the connection is momentarily absent.
    registry.begin_book_resync(ChannelName::Book, &gap.symbol);
    let Some(params) = registry.params_for(ChannelName::Book, &Some(gap.symbol.clone())) else {
        return;
    };
    let Some(mc) = conns.get_mut(&url) else {
        return;
    };
    send_book_reseed(url, gap.symbol, params, mc, sockets, bus_back_ref);
}

/// Send the WS unsubscribe+resubscribe for a `(Book, symbol)` reseed and arm the
/// snapshot-liveness timer.
fn send_book_reseed(
    url: WsUrl,
    symbol: Symbol,
    params: crate::conn::subscription_registry::SubscribeParams,
    mc: &mut ManagedConnection,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    // The unsubscribe is a raw frame — no termination event, no refcount change
    // (a reseed, not a caller unsubscribe).
    super::send_single_unsubscribe(
        url,
        ChannelName::Book,
        Some(symbol.clone()),
        params,
        sockets,
    );
    super::send_single_subscribe(
        url,
        ChannelName::Book,
        Some(symbol.clone()),
        params,
        mc,
        sockets,
        bus_back_ref,
    );
    let timeout_ms = bus_back_ref
        .upgrade()
        .map(|bus| {
            bus.knobs()
                .subscribe_ack_timeout_ms
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .unwrap_or(5_000);
    let due_at = crate::types::MonotonicInstant(
        mc.clock_now().0 + std::time::Duration::from_millis(timeout_ms as u64),
    );
    mc.arm_book_reseed_snapshot(ChannelName::Book, Some(symbol), due_at);
}

/// `TimerBookReseedSnapshot` fire: StaleNoOp, Retry, or Exhausted.
pub(super) fn handle_book_reseed_timeout(
    url: WsUrl,
    pair: Option<Symbol>,
    mc: &mut ManagedConnection,
    registry: &mut SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    let Some(symbol) = pair else {
        return;
    };
    match registry.note_reseed_timeout(ChannelName::Book, &symbol) {
        crate::conn::subscription_registry::ReseedTimeoutOutcome::StaleNoOp => {}
        crate::conn::subscription_registry::ReseedTimeoutOutcome::Retry => {
            let Some(params) = registry.params_for(ChannelName::Book, &Some(symbol.clone())) else {
                return;
            };
            send_book_reseed(url, symbol, params, mc, sockets, bus_back_ref);
        }
        crate::conn::subscription_registry::ReseedTimeoutOutcome::Exhausted => {
            emit_book_gap_events(&symbol, bus_back_ref);
        }
    }
}

/// Emit the parallel `OrderBookGapEvent` + `SubscriptionGapEvent` pair.
fn emit_book_gap_events(symbol: &Symbol, bus_back_ref: &Weak<DispatchEventBus>) {
    if let Some(bus) = bus_back_ref.upgrade() {
        let now = bus.clock().now();
        bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::OrderBookGapEvent,
            event_version: 2,
            timestamp_monotonic: now,
            request_id: None,
            payload: crate::dispatch::EventPayload::OrderBookGapEvent {
                channel: ChannelName::Book,
                symbol: symbol.clone(),
                cause: crate::dispatch::GapCause::OrderBookCrcMismatch,
            },
        });
        bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::SubscriptionGapEvent,
            event_version: 2,
            timestamp_monotonic: now,
            request_id: None,
            payload: crate::dispatch::EventPayload::SubscriptionGapEvent {
                channel: ChannelName::Book,
                symbol: symbol.clone(),
                dropped_count: 0,
                cause: crate::dispatch::GapCause::OrderBookCrcMismatch,
            },
        });
    }
}

/// Emit `ChannelGapEvent`. No debounce — each jump is a distinct loss.
fn emit_channel_gap_event(
    channel: ChannelName,
    dropped_count: u32,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    if let Some(bus) = bus_back_ref.upgrade() {
        bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::ChannelGapEvent,
            event_version: 1,
            timestamp_monotonic: bus.clock().now(),
            request_id: None,
            payload: crate::dispatch::EventPayload::ChannelGapEvent {
                channel,
                dropped_count,
                cause: crate::dispatch::GapCause::SequenceGapDetected,
            },
        });
    }
}

/// Throttle window; the contract pins the semantics, not the exact duration.
const MESSAGE_DROPPED_PERIOD: std::time::Duration = std::time::Duration::from_secs(1);

/// Last-warned instant per (site, channel).
pub(super) type SuppressWarnTracker =
    HashMap<(&'static str, Option<ChannelName>), crate::types::MonotonicInstant>;

/// Run `warn` at most once per [`MESSAGE_DROPPED_PERIOD`] per `(site, channel)`.
fn suppress_warn_throttled(
    site: &'static str,
    channel: Option<ChannelName>,
    tracker: &mut SuppressWarnTracker,
    bus_back_ref: &Weak<DispatchEventBus>,
    warn: impl FnOnce(),
) {
    let now = bus_back_ref
        .upgrade()
        .map(|b| b.clock().now())
        .unwrap_or_else(crate::types::MonotonicInstant::now);
    let should_warn = match tracker.get_mut(&(site, channel)) {
        None => {
            tracker.insert((site, channel), now);
            true
        }
        Some(last) => {
            if now.0 >= last.0 + MESSAGE_DROPPED_PERIOD {
                *last = now;
                true
            } else {
                false
            }
        }
    };
    if should_warn {
        warn();
    }
}

/// Emit `MessageDroppedNoHandler` at most once per window, aggregating count.
fn record_drop_and_maybe_emit(
    channel: ChannelName,
    drop_tracker: &mut HashMap<ChannelName, (crate::types::MonotonicInstant, u32)>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    let now = bus_back_ref
        .upgrade()
        .map(|b| b.clock().now())
        .unwrap_or_else(crate::types::MonotonicInstant::now);
    let emit = |count: u32| {
        if let Some(bus) = bus_back_ref.upgrade() {
            bus.publish(crate::dispatch::EventEnvelope {
                event_type: crate::dispatch::EventType::MessageDroppedNoHandler,
                event_version: 1,
                timestamp_monotonic: now,
                request_id: None,
                payload: crate::dispatch::EventPayload::MessageDroppedNoHandler {
                    channel,
                    count,
                    period: MESSAGE_DROPPED_PERIOD,
                },
            });
        }
    };
    match drop_tracker.get_mut(&channel) {
        None => {
            drop_tracker.insert(channel, (now, 0));
            emit(1);
        }
        Some((last_emit, pending)) => {
            if now.0 >= last_emit.0 + MESSAGE_DROPPED_PERIOD {
                let count = *pending + 1;
                *last_emit = now;
                *pending = 0;
                emit(count);
            } else {
                *pending += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_rate_limit_rejection_is_transient() {
        for s in [
            "EGeneral:Too many requests",
            "EAPI:Rate limit exceeded",
            "EService:Throttled: 1700000000",
        ] {
            assert!(
                subscribe_rejection_is_transient(s),
                "rate-limit subscribe rejection should be transient: {s}"
            );
        }
    }

    #[test]
    fn subscribe_non_rate_limit_rejection_is_permanent() {
        for s in [
            "EGeneral:Invalid arguments",
            "EQuery:Unknown asset pair",
            "subscribe rejected",
        ] {
            assert!(
                !subscribe_rejection_is_transient(s),
                "non-rate-limit subscribe rejection should be permanent: {s}"
            );
        }
    }

    #[test]
    fn classify_token_stale_family() {
        assert_eq!(
            classify_auth_handshake_error("EAPI:Invalid token"),
            AuthErrorKind::TokenStale
        );
        assert_eq!(
            classify_auth_handshake_error("token expired"),
            AuthErrorKind::TokenStale
        );
        assert_eq!(
            classify_auth_handshake_error("auth token is stale"),
            AuthErrorKind::TokenStale
        );
    }

    #[test]
    fn classify_invalid_session_is_token_stale() {
        // `ESession:Invalid session` carries no `token` marker — it needs its own
        // arm.
        assert_eq!(
            classify_auth_class("ESession:Invalid session"),
            Some(AuthErrorKind::TokenStale),
        );
        assert_eq!(
            classify_auth_handshake_error("ESession:Invalid session"),
            AuthErrorKind::TokenStale,
        );
        assert_eq!(classify_auth_class("some unrelated error"), None);
    }

    #[test]
    fn classify_bad_creds_family() {
        assert_eq!(
            classify_auth_handshake_error("EAPI:Invalid key"),
            AuthErrorKind::BadCreds
        );
        assert_eq!(
            classify_auth_handshake_error("EAPI:Invalid signature"),
            AuthErrorKind::BadCreds
        );
        assert_eq!(
            classify_auth_handshake_error("EAPI:Permission denied"),
            AuthErrorKind::BadCreds
        );
    }

    #[test]
    fn classify_permission_denied_egeneral() {
        assert_eq!(
            classify_auth_handshake_error("EGeneral:Permission denied"),
            AuthErrorKind::PermissionDenied
        );
    }

    #[test]
    fn classify_unknown_defaults_transient() {
        assert_eq!(
            classify_auth_handshake_error("EService:Unavailable"),
            AuthErrorKind::Transient
        );
        assert_eq!(
            classify_auth_handshake_error("something totally unexpected"),
            AuthErrorKind::Transient
        );
    }

    #[test]
    fn classify_broadened_novel_token_variants_are_token_stale() {
        for s in [
            "EAPI:Invalid token: expired",
            "auth token invalid",
            "WS token expiration reached",
        ] {
            assert_eq!(
                classify_auth_class(s),
                Some(AuthErrorKind::TokenStale),
                "novel token string should classify TokenStale: {s}"
            );
        }
    }

    #[test]
    fn classify_broadened_novel_credential_variants_are_auth_class() {
        for s in [
            "EAuth:UnsupportedAuthMethod",
            "EAPI:Bad credentials",
            "Invalid signature for request",
        ] {
            assert_eq!(
                classify_auth_class(s),
                Some(AuthErrorKind::BadCreds),
                "novel credential string should classify BadCreds: {s}"
            );
        }
    }

    #[test]
    fn classify_broadened_novel_permission_variants_are_auth_class() {
        for s in [
            "EAccess:Permission denied",
            "permission denied for this key",
        ] {
            assert_eq!(
                classify_auth_class(s),
                Some(AuthErrorKind::PermissionDenied),
                "novel permission string should classify PermissionDenied: {s}"
            );
        }
    }

    #[test]
    fn classify_order_ack_novel_auth_string_fails_auth_not_authenticated() {
        // A novel auth-class string on success:false must classify AuthFailed —
        // a bad token cannot drive -> Open.
        for (s, expect) in [
            ("EAuth:UnsupportedAuthMethod", AuthErrorKind::BadCreds),
            ("EAPI:Invalid token: expired", AuthErrorKind::TokenStale),
            ("EAccess:Permission denied", AuthErrorKind::PermissionDenied),
        ] {
            match classify_order_ack_auth(false, Some(s)) {
                OrderAckAuth::AuthFailed(kind) => assert_eq!(
                    kind, expect,
                    "novel auth string {s} must be AuthFailed({expect:?})"
                ),
                OrderAckAuth::Authenticated => {
                    panic!("BUG#4 regression: novel auth string {s} defaulted to Authenticated")
                }
            }
        }
        assert!(matches!(
            classify_order_ack_auth(false, Some("EOrder:Insufficient funds")),
            OrderAckAuth::Authenticated
        ));
    }

    /// A bad-pair reject carries NO `channel` and NO `result` wrapper; it must
    /// correlate via `req_id` and terminate the entry.
    #[tokio::test]
    async fn channelless_reject_correlates_via_req_id_and_terminates_entry() {
        use crate::clock::{Clock, SystemClock};
        use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};
        use std::sync::atomic::AtomicBool;

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx_cell = std::sync::Mutex::new(Some(tx));
        let _ = bus.subscribe(
            crate::dispatch::EventType::SubscriptionTerminatedEvent,
            Arc::new(move |env: &crate::dispatch::EventEnvelope| {
                if let Some(tx) = tx_cell.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            u16::MAX,
        );
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());

        let url = WsUrl::Public;
        let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
            Arc::new(crate::transport::MockWsSocketFactory::new());
        let rate_budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
        let jitter: Arc<dyn crate::jitter::JitterSource> =
            Arc::new(crate::jitter::FixedJitter(0.0));
        let mut mc = ManagedConnection::new(
            url,
            Arc::clone(&bus),
            factory,
            rate_budget,
            Arc::clone(&clock),
            jitter,
        );
        mc.test_set_state(ConnectionState::Open);
        let channel = ChannelName::Ticker;
        let pair = Symbol::new("FAKECOIN/USD").ok();
        let req_id = 77u64;
        mc.record_subscribe_req_id(req_id, channel, pair.clone());
        mc.arm_subscribe_ack(
            channel,
            pair.clone(),
            crate::types::MonotonicInstant(std::time::Duration::from_secs(5)),
        );

        let mut conns: HashMap<WsUrl, ManagedConnection> = HashMap::new();
        conns.insert(url, mc);
        let mut registry = SubscriptionRegistry::new();
        let sockets: HashMap<WsUrl, Arc<dyn WsSocket>> = HashMap::new();
        let auth_stack = Arc::new(crate::auth::AuthStack::new(
            None,
            None,
            Arc::new(crate::auth::SystemClockNonceSource::new()),
            std::collections::HashMap::new(),
            crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
        ));
        let auth_send_ready = Arc::new(AtomicBool::new(false));
        let bus_back_ref = Arc::downgrade(&bus);
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        // The error text is exchange-owned; assert it is carried through VERBATIM.
        let reject_err = "Currency pair not supported FAKECOIN/USD";
        let reject = serde_json::json!({
            "error": reject_err,
            "method": "subscribe",
            "success": false,
            "symbol": "FAKECOIN/USD",
            "req_id": req_id,
        });

        let consumed = handle_sub_ack(
            url,
            &reject,
            &mut conns,
            &mut registry,
            &sockets,
            &auth_stack,
            &auth_send_ready,
            &bus_back_ref,
            &state_mirrors,
            &mut HashMap::new(),
        );
        assert!(consumed, "an ack frame is always consumed");

        // Generous timeout: the event rides the spawned dispatch task; a tight
        // window is flaky under load.
        let env = tokio::time::timeout(std::time::Duration::from_millis(2000), rx)
            .await
            .expect("SubscriptionTerminatedEvent not delivered (reject was silently dropped)")
            .expect("oneshot sender dropped");
        match env.payload {
            crate::dispatch::EventPayload::SubscriptionTerminatedEvent {
                channel: ev_ch,
                pair: ev_pair,
                cause,
                last_error,
                ..
            } => {
                assert_eq!(ev_ch, ChannelName::Ticker);
                assert_eq!(ev_pair, pair);
                assert!(matches!(
                    cause,
                    crate::api::subscription::TerminationCause::NonTransientWireRejection
                ));
                assert_eq!(
                    last_error.as_deref(),
                    Some(reject_err),
                    "the wire reject reason is carried through to the caller verbatim"
                );
            }
            other => panic!("expected SubscriptionTerminatedEvent, got {other:?}"),
        }

        let mc = conns.get(&url).unwrap();
        assert_eq!(
            mc.state(),
            ConnectionState::Open,
            "connection must stay up on a non-transient per-entry reject"
        );
        assert!(
            !mc.test_has_subscribe_resend_timer(channel, pair.clone()),
            "non-transient reject must NOT arm a resend timer"
        );
        assert!(
            !mc.is_subscribe_ack_armed(channel, pair.clone()),
            "the subscribe-ack timer must be disarmed (entry concluded)"
        );
        assert!(
            mc.resolve_subscribe_req_id(req_id).is_none(),
            "the pending req_id correlation entry must be removed (no leak)"
        );

        bus.stop_reactors();
    }

    /// An invalid-channel reject carries NEITHER `channel` NOR `symbol` — only
    /// `req_id` correlates it.
    #[tokio::test]
    async fn bad_channel_reject_correlates_via_req_id_only() {
        use crate::clock::{Clock, SystemClock};
        use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};
        use std::sync::atomic::AtomicBool;

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx_cell = std::sync::Mutex::new(Some(tx));
        let _ = bus.subscribe(
            crate::dispatch::EventType::SubscriptionTerminatedEvent,
            Arc::new(move |env: &crate::dispatch::EventEnvelope| {
                if let Some(tx) = tx_cell.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            u16::MAX,
        );
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());

        let url = WsUrl::Public;
        let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
            Arc::new(crate::transport::MockWsSocketFactory::new());
        let rate_budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
        let jitter: Arc<dyn crate::jitter::JitterSource> =
            Arc::new(crate::jitter::FixedJitter(0.0));
        let mut mc = ManagedConnection::new(
            url,
            Arc::clone(&bus),
            factory,
            rate_budget,
            Arc::clone(&clock),
            jitter,
        );
        mc.test_set_state(ConnectionState::Open);
        let channel = ChannelName::Ticker;
        let pair = Symbol::new("FAKECOIN/USD").ok();
        let req_id = 91u64;
        mc.record_subscribe_req_id(req_id, channel, pair.clone());
        mc.arm_subscribe_ack(
            channel,
            pair.clone(),
            crate::types::MonotonicInstant(std::time::Duration::from_secs(5)),
        );

        let mut conns: HashMap<WsUrl, ManagedConnection> = HashMap::new();
        conns.insert(url, mc);
        let mut registry = SubscriptionRegistry::new();
        let sockets: HashMap<WsUrl, Arc<dyn WsSocket>> = HashMap::new();
        let auth_stack = Arc::new(crate::auth::AuthStack::new(
            None,
            None,
            Arc::new(crate::auth::SystemClockNonceSource::new()),
            std::collections::HashMap::new(),
            crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
        ));
        let auth_send_ready = Arc::new(AtomicBool::new(false));
        let bus_back_ref = Arc::downgrade(&bus);
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        // A symbol-only fallback would fail here — only req_id correlates.
        let reject_err = "Subscription name invalid";
        let reject = serde_json::json!({
            "error": reject_err,
            "method": "subscribe",
            "success": false,
            "req_id": req_id,
        });

        let consumed = handle_sub_ack(
            url,
            &reject,
            &mut conns,
            &mut registry,
            &sockets,
            &auth_stack,
            &auth_send_ready,
            &bus_back_ref,
            &state_mirrors,
            &mut HashMap::new(),
        );
        assert!(consumed, "an ack frame is always consumed");

        let env = tokio::time::timeout(std::time::Duration::from_millis(2000), rx)
            .await
            .expect("SubscriptionTerminatedEvent not delivered (bad-channel reject dropped)")
            .expect("oneshot sender dropped");
        match env.payload {
            crate::dispatch::EventPayload::SubscriptionTerminatedEvent {
                channel: ev_ch,
                pair: ev_pair,
                cause,
                last_error,
                ..
            } => {
                assert_eq!(ev_ch, ChannelName::Ticker);
                assert_eq!(ev_pair, pair);
                assert!(matches!(
                    cause,
                    crate::api::subscription::TerminationCause::NonTransientWireRejection
                ));
                assert_eq!(last_error.as_deref(), Some(reject_err));
            }
            other => panic!("expected SubscriptionTerminatedEvent, got {other:?}"),
        }

        let mc = conns.get(&url).unwrap();
        assert_eq!(
            mc.state(),
            ConnectionState::Open,
            "connection must stay up on a non-transient per-entry reject"
        );
        assert!(
            !mc.test_has_subscribe_resend_timer(channel, pair.clone()),
            "non-transient reject must NOT arm a resend timer (no churn)"
        );
        assert!(
            !mc.is_subscribe_ack_armed(channel, pair.clone()),
            "the subscribe-ack timer must be disarmed (entry concluded)"
        );
        assert!(
            mc.resolve_subscribe_req_id(req_id).is_none(),
            "the pending req_id correlation entry must be removed (no leak)"
        );

        bus.stop_reactors();
    }

    /// The status subscribe gets a spurious `Symbol(s) not found` reject and NO
    /// success ack; the reject must COMPLETE the subscribe without teardown.
    #[tokio::test]
    async fn status_channel_spurious_reject_completes_without_teardown() {
        use crate::clock::{Clock, SystemClock};
        use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};
        use std::sync::atomic::AtomicBool;

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let url = WsUrl::Public;
        let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
            Arc::new(crate::transport::MockWsSocketFactory::new());
        let rate_budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
        let jitter: Arc<dyn crate::jitter::JitterSource> =
            Arc::new(crate::jitter::FixedJitter(0.0));
        let mut mc = ManagedConnection::new(
            url,
            Arc::clone(&bus),
            factory,
            rate_budget,
            Arc::clone(&clock),
            jitter,
        );
        mc.test_set_state(ConnectionState::Open);
        let channel = ChannelName::Status;
        let pair: Option<Symbol> = None;
        let req_id = 55u64;
        mc.record_subscribe_req_id(req_id, channel, pair.clone());
        mc.arm_subscribe_ack(
            channel,
            pair.clone(),
            crate::types::MonotonicInstant(std::time::Duration::from_secs(5)),
        );

        let mut conns: HashMap<WsUrl, ManagedConnection> = HashMap::new();
        conns.insert(url, mc);
        let mut registry = SubscriptionRegistry::new();
        let sockets: HashMap<WsUrl, Arc<dyn WsSocket>> = HashMap::new();
        let auth_stack = Arc::new(crate::auth::AuthStack::new(
            None,
            None,
            Arc::new(crate::auth::SystemClockNonceSource::new()),
            std::collections::HashMap::new(),
            crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
        ));
        let auth_send_ready = Arc::new(AtomicBool::new(false));
        let bus_back_ref = Arc::downgrade(&bus);
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        let reject = serde_json::json!({
            "error": "Symbol(s) not found",
            "method": "subscribe",
            "success": false,
            "req_id": req_id,
        });

        let consumed = handle_sub_ack(
            url,
            &reject,
            &mut conns,
            &mut registry,
            &sockets,
            &auth_stack,
            &auth_send_ready,
            &bus_back_ref,
            &state_mirrors,
            &mut HashMap::new(),
        );
        assert!(consumed, "an ack frame is always consumed");

        let mc = conns.get(&url).unwrap();
        assert_eq!(
            mc.state(),
            ConnectionState::Open,
            "status spurious reject must NOT tear the connection down"
        );
        assert!(
            !mc.is_subscribe_ack_armed(channel, pair.clone()),
            "status subscribe must be completed (ack timer disarmed), not left armed"
        );
        assert!(
            !mc.test_has_subscribe_resend_timer(channel, pair.clone()),
            "status spurious reject must not arm a resend timer"
        );
        assert!(
            mc.resolve_subscribe_req_id(req_id).is_none(),
            "the req_id correlation entry must be dropped (no leak)"
        );
    }

    #[test]
    fn classify_order_ack_novel_string_pins_authenticated() {
        assert!(matches!(
            classify_order_ack_auth(false, Some("some unrecognized text")),
            OrderAckAuth::Authenticated
        ));
    }

    use crate::clock::{Clock, SystemClock};
    use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};

    fn test_bus() -> Arc<DispatchEventBus> {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            clock,
        ))
    }

    fn test_handlers() -> HandlerRegistry {
        let presence_mirror: crate::dispatch::handler_registry::PresenceMirror =
            Arc::new(std::sync::RwLock::new(HashMap::new()));
        HandlerRegistry::new(presence_mirror)
    }

    fn test_auth_stack(bus: &Arc<DispatchEventBus>) -> Arc<crate::auth::AuthStack> {
        Arc::new(crate::auth::AuthStack::new(
            None,
            None,
            Arc::new(crate::auth::SystemClockNonceSource::new()),
            std::collections::HashMap::new(),
            crate::auth::TokenLifecycleManager::new(Arc::clone(bus), "<test-key>".to_string()),
        ))
    }

    fn test_mc(
        url: WsUrl,
        bus: &Arc<DispatchEventBus>,
        state: ConnectionState,
    ) -> ManagedConnection {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
            Arc::new(crate::transport::MockWsSocketFactory::new());
        let rate_budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
        let jitter: Arc<dyn crate::jitter::JitterSource> =
            Arc::new(crate::jitter::SplitMix64Jitter::with_seed(0xC0FFEE));
        let mut mc =
            ManagedConnection::new(url, Arc::clone(bus), factory, rate_budget, clock, jitter);
        mc.test_set_state(state);
        mc
    }

    use std::io::Write;
    use std::sync::Mutex as StdMutex;

    #[derive(Clone, Default)]
    struct CaptureWriter(Arc<StdMutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("capture lock poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    struct DynamicWarnInterest;

    impl tracing::Subscriber for DynamicWarnInterest {
        fn register_callsite(
            &self,
            meta: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            if *meta.level() <= tracing::Level::WARN {
                tracing::subscriber::Interest::sometimes()
            } else {
                tracing::subscriber::Interest::never()
            }
        }
        fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
            Some(tracing::level_filters::LevelFilter::WARN)
        }
        fn enabled(&self, _meta: &tracing::Metadata<'_>) -> bool {
            false
        }
        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
        fn event(&self, _event: &tracing::Event<'_>) {}
        fn enter(&self, _span: &tracing::span::Id) {}
        fn exit(&self, _span: &tracing::span::Id) {}
    }

    fn capture_logs(f: impl FnOnce()) -> String {
        static DYNAMIC_INTEREST: std::sync::Once = std::sync::Once::new();
        DYNAMIC_INTEREST.call_once(|| {
            let _ = tracing::subscriber::set_global_default(DynamicWarnInterest);
        });

        let buf = Arc::new(StdMutex::new(Vec::new()));
        let writer = CaptureWriter(Arc::clone(&buf));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer)
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            f();
        });
        let bytes = buf.lock().expect("capture lock poisoned").clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    #[test]
    fn non_array_data_frame_warns_and_suppresses_event() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let handlers = test_handlers();
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let bus_back = Arc::downgrade(&bus);

        let frame = serde_json::json!({
            "channel": "ticker",
            "type": "update",
            "data": { "symbol": "BTC/USD", "bid": 30000.0 },
        });

        let logs = capture_logs(|| {
            let gaps = route_text_frame(
                WsUrl::Public,
                &frame,
                &mut registry,
                &handlers,
                &bus_back,
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );
            assert!(gaps.is_empty(), "non-array data carries no book gaps");
        });

        let events = bus.test_drain_published();
        assert!(
            !events.iter().any(|e| matches!(
                e.event_type,
                crate::dispatch::EventType::MessageDroppedNoHandler
            )),
            "non-array `data` must NOT emit MessageDroppedNoHandler; got {events:?}"
        );
        assert!(
            !events.iter().any(|e| matches!(
                e.event_type,
                crate::dispatch::EventType::SubscriptionGapEvent
            )),
            "non-array `data` must NOT emit SubscriptionGapEvent; got {events:?}"
        );

        assert!(
            logs.contains("non-array `data`"),
            "non-array `data` must warn; captured logs: {logs:?}"
        );
    }

    #[test]
    fn status_frame_with_no_handler_does_not_flood_message_dropped() {
        // Public conn auto-seeds a status frame on connect; Kraken re-pushes ~1/sec.
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let handlers = test_handlers();
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let bus_back = Arc::downgrade(&bus);

        let frame = serde_json::json!({
            "channel": "status",
            "type": "update",
            "data": [ { "system": "online", "api_version": "v2" } ],
        });

        for _ in 0..10 {
            let gaps = route_text_frame(
                WsUrl::Public,
                &frame,
                &mut registry,
                &handlers,
                &bus_back,
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );
            assert!(gaps.is_empty(), "status frame carries no book gaps");
        }

        let events = bus.test_drain_published();
        assert!(
            !events.iter().any(|e| matches!(
                e.event_type,
                crate::dispatch::EventType::MessageDroppedNoHandler
            )),
            "status frame with no on_system_status handler must NOT emit \
             MessageDroppedNoHandler; got {events:?}"
        );
    }

    fn route_seq_frames(
        frames: &[(&str, serde_json::Value)],
        registry: &mut SubscriptionRegistry,
        bus: &Arc<DispatchEventBus>,
    ) {
        let handlers = test_handlers();
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let bus_back = Arc::downgrade(bus);
        for (_, frame) in frames {
            route_text_frame(
                WsUrl::Auth,
                frame,
                registry,
                &handlers,
                &bus_back,
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );
        }
    }

    /// A sequence jump emits `ChannelGapEvent`; contiguous frames stay silent.
    #[test]
    fn executions_sequence_jump_emits_channel_gap_event() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        registry.register(
            crate::conn::subscription_registry::SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Executions,
                None,
                crate::conn::subscription_registry::SubscribeParams::Executions,
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        let mk = |ty: &str, seq: u64| {
            serde_json::json!({
                "channel": "executions", "type": ty, "sequence": seq,
                "data": [ { "order_id": "X" } ],
            })
        };

        route_seq_frames(
            &[
                ("s", mk("snapshot", 1)),
                ("u", mk("update", 2)),
                ("u", mk("update", 3)),
            ],
            &mut registry,
            &bus,
        );
        let quiet = bus.test_drain_published();
        assert!(
            !quiet
                .iter()
                .any(|e| e.event_type == crate::dispatch::EventType::ChannelGapEvent),
            "contiguous frames must not gap; got {quiet:?}"
        );

        route_seq_frames(&[("u", mk("update", 7))], &mut registry, &bus);
        let events = bus.test_drain_published();
        let gap = events
            .iter()
            .find(|e| e.event_type == crate::dispatch::EventType::ChannelGapEvent)
            .expect("3→7 jump must emit ChannelGapEvent");
        assert_eq!(gap.event_version, 1);
        assert!(gap.request_id.is_none(), "broadcast event");
        assert!(
            matches!(
                gap.payload,
                crate::dispatch::EventPayload::ChannelGapEvent {
                    channel: ChannelName::Executions,
                    dropped_count: 3,
                    cause: crate::dispatch::GapCause::SequenceGapDetected,
                }
            ),
            "payload mismatch: {:?}",
            gap.payload
        );
    }

    /// A malformed-`data` frame with a valid `sequence` still advances the
    /// tracker; a sequence-less frame neither advances nor false-gaps.
    #[test]
    fn malformed_or_sequenceless_frames_do_not_false_gap() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        registry.register(
            crate::conn::subscription_registry::SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                crate::conn::subscription_registry::SubscribeParams::Balances,
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );

        route_seq_frames(
            &[
                (
                    "seed",
                    serde_json::json!({
                        "channel": "balances", "type": "snapshot", "sequence": 1,
                        "data": [ { "asset": "USD" } ],
                    }),
                ),
                (
                    "malformed",
                    serde_json::json!({
                        "channel": "balances", "type": "update", "sequence": 2,
                        "data": { "asset": "USD" },
                    }),
                ),
                (
                    "no-seq",
                    serde_json::json!({
                        "channel": "balances", "type": "update",
                        "data": [ { "asset": "USD" } ],
                    }),
                ),
                (
                    "good",
                    serde_json::json!({
                        "channel": "balances", "type": "update", "sequence": 3,
                        "data": [ { "asset": "USD" } ],
                    }),
                ),
            ],
            &mut registry,
            &bus,
        );

        let events = bus.test_drain_published();
        assert!(
            !events
                .iter()
                .any(|e| e.event_type == crate::dispatch::EventType::ChannelGapEvent),
            "no false gap across malformed/sequence-less frames; got {events:?}"
        );
    }

    /// Warns are throttled per (site, channel) per window.
    #[test]
    fn suppress_warn_is_throttled_per_site_and_channel_per_window() {
        use crate::clock::Clock;
        use std::sync::atomic::{AtomicU64, Ordering};

        struct ManualClock(AtomicU64);
        impl Clock for ManualClock {
            fn now(&self) -> crate::types::MonotonicInstant {
                crate::types::MonotonicInstant(std::time::Duration::from_nanos(
                    self.0.load(Ordering::Relaxed),
                ))
            }
        }

        let clock = Arc::new(ManualClock(AtomicU64::new(10_000_000_000))); // t = 10s
        let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            Arc::clone(&clock) as Arc<dyn Clock>,
        ));
        let weak = Arc::downgrade(&bus);
        let mut tracker: SuppressWarnTracker = HashMap::new();
        let warn = || tracing::warn!(target: "kraken_sdk::io_reactor", "no routing symbol — throttle probe");

        let logs = capture_logs(|| {
            suppress_warn_throttled("t", Some(ChannelName::Ticker), &mut tracker, &weak, warn);
            suppress_warn_throttled("t", Some(ChannelName::Ticker), &mut tracker, &weak, warn);
        });
        assert_eq!(
            logs.matches("no routing symbol").count(),
            1,
            "a second in-window frame must be throttled (one warn); got: {logs:?}"
        );

        clock.0.fetch_add(
            2 * MESSAGE_DROPPED_PERIOD.as_nanos() as u64,
            Ordering::Relaxed,
        );
        let logs2 = capture_logs(|| {
            suppress_warn_throttled("t", Some(ChannelName::Ticker), &mut tracker, &weak, warn);
        });
        assert_eq!(
            logs2.matches("no routing symbol").count(),
            1,
            "after the window the warn must re-fire; got: {logs2:?}"
        );

        let logs3 = capture_logs(|| {
            suppress_warn_throttled("t", Some(ChannelName::Trade), &mut tracker, &weak, warn);
        });
        assert_eq!(
            logs3.matches("no routing symbol").count(),
            1,
            "a different channel warns independently; got: {logs3:?}"
        );

        // Same channel, different site — channel-only keying would suppress this
        // call.
        let logs4 = capture_logs(|| {
            suppress_warn_throttled("u", Some(ChannelName::Ticker), &mut tracker, &weak, warn);
            suppress_warn_throttled("u", Some(ChannelName::Ticker), &mut tracker, &weak, warn);
        });
        assert_eq!(
            logs4.matches("no routing symbol").count(),
            1,
            "a distinct site gets its own window; got: {logs4:?}"
        );
    }

    #[test]
    fn sub_ack_missing_success_routes_as_failure() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let sockets: HashMap<WsUrl, Arc<dyn WsSocket>> = HashMap::new();
        let auth_stack = test_auth_stack(&bus);
        let auth_send_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let bus_back = Arc::downgrade(&bus);
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        let mut mc = test_mc(WsUrl::Public, &bus, ConnectionState::Resubscribing);
        let pair = Symbol::new("BTC/USD").expect("valid pair");
        mc.arm_subscribe_ack(
            ChannelName::Ticker,
            Some(pair),
            crate::types::MonotonicInstant(std::time::Duration::from_secs(60)),
        );
        let mut conns = HashMap::new();
        conns.insert(WsUrl::Public, mc);

        let frame = serde_json::json!({
            "method": "subscribe",
            "channel": "ticker",
            "symbol": "BTC/USD",
            "error": "subscribe rejected",
        });

        let consumed = handle_sub_ack(
            WsUrl::Public,
            &frame,
            &mut conns,
            &mut registry,
            &sockets,
            &auth_stack,
            &auth_send_ready,
            &bus_back,
            &state_mirrors,
            &mut HashMap::new(),
        );
        assert!(consumed, "a `subscribe` ack frame is always consumed");

        let events = bus.test_drain_published();
        assert!(
            events.iter().any(|e| matches!(
                e.event_type,
                crate::dispatch::EventType::SubscriptionTerminatedEvent
            )),
            "missing-`success` subscribe ack must route as FAILURE (MD-3865 safe direction); got {events:?}"
        );
    }

    #[test]
    fn sub_ack_missing_success_warns() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let sockets: HashMap<WsUrl, Arc<dyn WsSocket>> = HashMap::new();
        let auth_stack = test_auth_stack(&bus);
        let auth_send_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let bus_back = Arc::downgrade(&bus);
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        let mut mc = test_mc(WsUrl::Public, &bus, ConnectionState::Resubscribing);
        let pair = Symbol::new("BTC/USD").expect("valid pair");
        mc.arm_subscribe_ack(
            ChannelName::Ticker,
            Some(pair),
            crate::types::MonotonicInstant(std::time::Duration::from_secs(60)),
        );
        let mut conns = HashMap::new();
        conns.insert(WsUrl::Public, mc);

        let frame = serde_json::json!({
            "method": "subscribe",
            "channel": "ticker",
            "symbol": "BTC/USD",
            "error": "subscribe rejected",
        });

        let logs = capture_logs(|| {
            handle_sub_ack(
                WsUrl::Public,
                &frame,
                &mut conns,
                &mut registry,
                &sockets,
                &auth_stack,
                &auth_send_ready,
                &bus_back,
                &state_mirrors,
                &mut HashMap::new(),
            );
        });
        assert!(
            logs.contains("subscribe-ack frame with absent/non-bool `success`"),
            "absent-`success` subscribe ack must warn; captured logs: {logs:?}"
        );
    }

    #[test]
    fn request_response_missing_success_warns() {
        let bus = test_bus();
        let registry = SubscriptionRegistry::new();
        let auth_stack = test_auth_stack(&bus);
        let auth_send_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        let mc = test_mc(WsUrl::Auth, &bus, ConnectionState::Open);
        let mut conns = HashMap::new();
        conns.insert(WsUrl::Auth, mc);

        let frame = serde_json::json!({
            "method": "add_order",
            "req_id": 7777u64,
            "error": "EGeneral:Internal error",
        });

        let logs = capture_logs(|| {
            let consumed = handle_request_response(
                WsUrl::Auth,
                &frame,
                &mut conns,
                &registry,
                &HashMap::new(),
                &auth_stack,
                &auth_send_ready,
                &state_mirrors,
                &Weak::new(),
                &mut HashMap::new(),
            );
            assert!(consumed, "a known-method reply with a req_id is consumed");
        });
        assert!(
            logs.contains("request-response frame with absent/non-bool `success`"),
            "absent-`success` request response must warn; captured logs: {logs:?}"
        );
    }

    #[test]
    fn order_ack_novel_non_e_string_warns_on_default() {
        let logs = capture_logs(|| {
            let out = classify_order_ack_warned(false, Some("totally novel rejection text"));
            assert!(matches!(out, OrderAckAuth::Authenticated));
        });
        assert!(
            logs.contains("not recognized as auth-class"),
            "novel non-`E*` order-ack error must warn on the inverted default; captured logs: {logs:?}"
        );

        let logs = capture_logs(|| {
            let out = classify_order_ack_warned(false, Some("EOrder:Insufficient funds"));
            assert!(matches!(out, OrderAckAuth::Authenticated));
        });
        assert!(
            !logs.contains("not recognized as auth-class"),
            "an `E*`-prefixed order error must NOT warn (no-flood gate); captured logs: {logs:?}"
        );
    }

    #[test]
    fn order_ack_no_error_string_warns_on_default() {
        let logs = capture_logs(|| {
            let out = classify_order_ack_warned(false, None);
            assert!(matches!(out, OrderAckAuth::Authenticated));
        });
        assert!(
            logs.contains("not recognized as auth-class"),
            "a success:false ack with no error string must warn on the inverted default; \
             captured logs: {logs:?}"
        );
    }

    #[tokio::test]
    async fn order_auth_ok_with_live_entries_is_not_stranded() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        registry.register(
            crate::conn::SubscriptionEntry::new(
                crate::types::WsUrl::Auth,
                crate::types::ChannelName::Executions,
                None,
                crate::conn::SubscribeParams::Executions,
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        let auth_stack = test_auth_stack(&bus);
        let auth_send_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        let mc = test_mc(WsUrl::Auth, &bus, ConnectionState::Authenticating);
        let mut conns = HashMap::new();
        conns.insert(WsUrl::Auth, mc);

        let frame = serde_json::json!({
            "method": "add_order",
            "req_id": 7171u64,
            "success": true,
        });
        let consumed = handle_request_response(
            WsUrl::Auth,
            &frame,
            &mut conns,
            &registry,
            &HashMap::new(),
            &auth_stack,
            &auth_send_ready,
            &state_mirrors,
            &Weak::new(),
            &mut HashMap::new(),
        );
        assert!(consumed, "the probe ack is consumed");
        let mc = conns.get_mut(&WsUrl::Auth).expect("auth conn");
        assert_ne!(
            mc.state(),
            ConnectionState::Authenticating,
            "probe ack with live entries must drive the FSM onward"
        );
        assert!(
            mc.awaiting_token_refresh(),
            "token-miss replay must mark the refresh in flight"
        );
        assert_eq!(
            mc.drain_deferred_open_auth_subscribes(),
            vec![(
                crate::types::ChannelName::Executions,
                None,
                crate::conn::SubscribeParams::Executions
            )],
            "token-miss replay defers exactly the composed batch"
        );
    }

    #[test]
    fn order_ack_probe_to_open_warns_exactly_once() {
        // One frame can run both the probe branch and the Open branch; the
        // novel-string warn must fire EXACTLY once across both.
        let bus = test_bus();
        let registry = SubscriptionRegistry::new();
        let auth_stack = test_auth_stack(&bus);
        let auth_send_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        let mc = test_mc(WsUrl::Auth, &bus, ConnectionState::Authenticating);
        let mut conns = HashMap::new();
        conns.insert(WsUrl::Auth, mc);

        let frame = serde_json::json!({
            "method": "add_order",
            "req_id": 9191u64,
            "success": false,
            "error": "totally novel rejection text",
        });

        let logs = capture_logs(|| {
            let consumed = handle_request_response(
                WsUrl::Auth,
                &frame,
                &mut conns,
                &registry,
                &HashMap::new(),
                &auth_stack,
                &auth_send_ready,
                &state_mirrors,
                &Weak::new(),
                &mut HashMap::new(),
            );
            assert!(consumed, "a known-method reply with a req_id is consumed");
        });

        assert_eq!(
            conns.get(&WsUrl::Auth).expect("auth conn present").state(),
            ConnectionState::Open,
            "probe→Open: WireOrderAuthOk must have driven Authenticating → Open"
        );
        assert_eq!(
            logs.matches("not recognized as auth-class").count(),
            1,
            "novel-string warn must fire exactly once per probe→Open frame; captured logs: {logs:?}"
        );
    }

    #[tokio::test]
    async fn order_reject_reply_resolves_pending_not_hung() {
        let bus = test_bus();
        let registry = SubscriptionRegistry::new();
        let auth_stack = test_auth_stack(&bus);
        let auth_send_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        let mut mc = test_mc(WsUrl::Auth, &bus, ConnectionState::Open);
        let req_id: u64 = 4242;
        let (tx, rx) = tokio::sync::oneshot::channel();
        mc.record_pending_request(crate::conn::managed_connection::PendingRequest {
            req_id,
            op: crate::dispatch::WsOp::AddOrder,
            sent_at: crate::types::MonotonicInstant(std::time::Duration::from_secs(0)),
            tx,
        });
        let mut conns = HashMap::new();
        conns.insert(WsUrl::Auth, mc);

        let frame = serde_json::json!({
            "error": "EGeneral:Order type not supported",
            "method": "add_order",
            "req_id": req_id,
            "success": false,
        });

        let consumed = handle_request_response(
            WsUrl::Auth,
            &frame,
            &mut conns,
            &registry,
            &HashMap::new(),
            &auth_stack,
            &auth_send_ready,
            &state_mirrors,
            &Weak::new(),
            &mut HashMap::new(),
        );
        assert!(
            consumed,
            "an `add_order` reply with a recorded req_id is consumed"
        );

        // Bounded await: a stall means the pending was left unresolved.
        let resp = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("MD-3858: pending order future must resolve promptly, not hang")
            .expect("MD-3858: pending order future must be RESOLVED, not dropped/hung");
        let resp = resp.expect("Leg A resolves Ok(WsResponse) regardless of wire `success`");
        assert_eq!(resp.req_id, req_id);
        assert!(!resp.success, "the reply carried success:false");
        assert_eq!(
            resp.error.as_deref(),
            Some("EGeneral:Order type not supported"),
            "the top-level error string is carried through for per-op decode"
        );
    }
}
