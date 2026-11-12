//! Inbound-frame routing: classify a text frame as a WS request response, a
//! subscribe/unsubscribe ack, or data fan-out.

use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, Weak};

use serde_json::Value;

use crate::api::ws_surface::SubscriberGuard;
use crate::conn::SubscribeErrorKind;
use crate::conn::managed_connection::WsResponse;
use crate::conn::subscription_registry::{BookDriveOutcome, SubscriptionRegistry};
use crate::conn::{AuthErrorKind, ManagedConnection};
use crate::dispatch::event_bus::{DispatchEventBus, truncate_panic_message};
use crate::dispatch::handler_registry::HandlerRegistry;
use crate::dispatch::{FuturesSubscribeAck, WsOp};
use crate::transport::{TransportError, TransportErrorKind, WsFrame, WsOpcode, WsSocket};
use crate::types::{
    ChannelName, ConnectionState, FuturesOhlcTickType, OhlcInterval, StreamSymbol, Symbol, WsUrl,
};

use tokio::sync::mpsc;

use super::socket_lifecycle::wire_upgrade_bridge;
use super::write_mirror;
use super::{SharedHandles, UpgradeOutcome};

#[expect(
    clippy::too_many_arguments,
    reason = "shared handles travel in SharedHandles; `&mut` reactor state is passed explicitly"
)]
pub(super) async fn handle_frame(
    url: WsUrl,
    frame_result: Result<WsFrame, TransportError>,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    handler_registry: &HandlerRegistry,
    handles: SharedHandles<'_>,
    drop_tracker: &mut HashMap<(WsUrl, ChannelName), (crate::types::MonotonicInstant, u32)>,
    malformed_warn_tracker: &mut SuppressWarnTracker,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    upgrade_guards: &mut HashMap<WsUrl, SubscriberGuard>,
) {
    let SharedHandles {
        bus_back_ref,
        state_mirrors,
        upgrade_tx,
        ..
    } = handles;
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
            // Request responses are checked first, gated on a request method on a token-bearing URL;
            // only a recorded pending is resolved.
            if handle_request_response(
                url,
                &json,
                conns,
                registry,
                sockets,
                handles,
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
                handles,
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

fn handle_request_response(
    url: WsUrl,
    json: &Value,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    handles: SharedHandles<'_>,
    malformed_warn_tracker: &mut SuppressWarnTracker,
) -> bool {
    let SharedHandles {
        auth_stack,
        order_gate,
        bus_back_ref,
        state_mirrors,
        ..
    } = handles;
    if !url.requires_ws_token() {
        return false;
    }
    let is_request_method = json
        .get("method")
        .and_then(Value::as_str)
        .and_then(WsOp::from_method)
        .is_some();
    if !is_request_method {
        let is_throttle_refusal = json.get("req_id").is_none()
            && json
                .get("method")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
            && json.get("success").and_then(Value::as_bool) == Some(false)
            && json
                .get("error")
                .and_then(Value::as_str)
                .is_some_and(crate::error::is_kraken_rate_limit);
        if !is_throttle_refusal {
            return false;
        }
        let pending = conns
            .get(&url)
            .map_or(0, ManagedConnection::pending_request_count);
        suppress_warn_throttled(
            url,
            "unattributed-rate-limit-refusal",
            None,
            malformed_warn_tracker,
            bus_back_ref,
            || {
                tracing::warn!(
                    target: "kraken_sdk::io_reactor", ?url, pending,
                    "rate-limit refusal carried no req_id; it cannot be matched to a request, so pending requests stay unresolved until a reply, their deadline or a connection teardown"
                )
            },
        );
        return true;
    }
    let Some(req_id) = json.get("req_id").and_then(Value::as_u64) else {
        suppress_warn_throttled(
            url,
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
    // Absent/non-bool `success` defaults to false; a success or fill is never fabricated.
    let success_opt = json.get("success").and_then(Value::as_bool);
    if success_opt.is_none() {
        suppress_warn_throttled(
            url,
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
    // A missing or non-bool `success` counts as a failure.
    let success = success_opt.unwrap_or(false);
    let resp = WsResponse::from_frame(req_id, success, json);
    let Some(mc) = conns.get_mut(&url) else {
        return true;
    };

    let order_ack_auth = classify_order_ack_warned(url, success, resp.error.as_deref());

    let in_auth_probe = mc.state() == ConnectionState::Authenticating;
    if in_auth_probe {
        // The caller's reply resolves before any FSM event or replay can drain it.
        mc.resolve_pending_request(resp.clone());
        match order_ack_auth {
            OrderAckAuth::Authenticated => {
                if registry.has_url_entry(url) {
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
                let was_token_stale = kind == crate::conn::AuthErrorKind::TokenStale;
                mc.handle_event(
                    crate::conn::managed_connection::FsmEvent::WireAuthHandshakeFailed { kind },
                );
                if was_token_stale
                    && mc.state() == ConnectionState::Authenticating
                    && mc.awaiting_token_refresh()
                {
                    auth_stack.invalidate_cached_token();
                    mc.force_reactive_token_refresh(auth_stack);
                }
            }
        }
        write_mirror(state_mirrors, url, mc.state());
        // Store-only recompute; the token-refreshed arm owns the re-emit.
        super::store_send_ready(url, mc, registry, auth_stack, order_gate);
    }

    // Open token-stale recovery must not widen in_auth_probe: self-loop, invalidate token, force_refresh for the NEXT order; this order is never resent.
    if mc.state() == ConnectionState::Open
        && let OrderAckAuth::AuthFailed(AuthErrorKind::TokenStale) = order_ack_auth
    {
        mc.handle_event(crate::conn::managed_connection::FsmEvent::WireOrderAckTokenStale);
        auth_stack.invalidate_cached_token();
        mc.force_reactive_token_refresh(auth_stack);
        mc.resolve_pending_request(resp.clone());
        write_mirror(state_mirrors, url, mc.state());
    }

    // Closing and Closed are left alone: a refresh has no connection to serve.
    if mc.state() == ConnectionState::Resubscribing
        && let OrderAckAuth::AuthFailed(AuthErrorKind::TokenStale) = order_ack_auth
    {
        auth_stack.invalidate_cached_token();
        mc.force_reactive_token_refresh(auth_stack);
    }

    if mc.pending_requests_contains(req_id) {
        mc.resolve_pending_request(resp);
    }
    true
}

/// Dispatch a wire-close FSM event, then reproject the `sockets` race-set. The
/// dead socket is removed UP FRONT so a no-arm close can't busy-loop.
#[expect(
    clippy::too_many_arguments,
    reason = "takes only the shared handles it uses; mutable reactor state and per-call data are passed explicitly"
)]
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
        // Re-insert only a different live socket so a no-arm close's dead socket does not re-race.
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

fn classify_auth_handshake_error(url: WsUrl, err: &str) -> AuthErrorKind {
    classify_auth_class(url, err).unwrap_or(AuthErrorKind::Transient)
}

/// AUTH-class arms shared by the subscribe-ack classifier (default `Transient`) and the order-ack classifier (inverted default).
fn classify_auth_class(url: WsUrl, err: &str) -> Option<AuthErrorKind> {
    // Token-stale is checked first; families are broad because the inverted order-ack default would drive -> Open on a bad token.
    if crate::error::is_token_stale_reject(url, err) {
        return Some(AuthErrorKind::TokenStale);
    }
    // Checked before the generic permission family so the EAPI prefix keeps BadCreds.
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
    Authenticated,
    AuthFailed(AuthErrorKind),
}

fn classify_order_ack_auth(url: WsUrl, success: bool, err: Option<&str>) -> OrderAckAuth {
    if success {
        return OrderAckAuth::Authenticated;
    }
    match err.and_then(|e| classify_auth_class(url, e)) {
        // On Unified an auth-class order refusal comes from a session that accepted the
        // token; only a stale token fails the probe.
        Some(kind) if url.is_unified() && kind != AuthErrorKind::TokenStale => {
            OrderAckAuth::Authenticated
        }
        Some(kind) => OrderAckAuth::AuthFailed(kind),
        // Inverted default: a non-auth order error still means the token was accepted; a novel auth string lands here (warned at the call site).
        None => OrderAckAuth::Authenticated,
    }
}

fn order_ack_default_authenticated_is_unrecognized(
    url: WsUrl,
    success: bool,
    err: Option<&str>,
) -> bool {
    if success {
        return false;
    }
    if err.and_then(|e| classify_auth_class(url, e)).is_some() {
        return false;
    }
    if url.is_unified() {
        return err.is_none_or(|e| {
            let e = e.to_ascii_lowercase();
            e.contains("token") || e.contains("session")
        });
    }
    err.is_none() || err.is_some_and(|e| !e.starts_with('E'))
}

fn classify_order_ack_warned(url: WsUrl, success: bool, err: Option<&str>) -> OrderAckAuth {
    if order_ack_default_authenticated_is_unrecognized(url, success, err) {
        tracing::warn!(
            target: "kraken_sdk::io_reactor", ?url, error = ?err,
            "order-ack error not recognized as auth-class (non-`E*`-prefixed or absent); \
             defaulting to Authenticated — novel auth string would slip through"
        );
    }
    classify_order_ack_auth(url, success, err)
}

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

/// A non-transient reject of the shared book frame also ends a peer that never received its own ack.
fn terminate_pending_unified_book_peer(
    url: WsUrl,
    channel: ChannelName,
    pair: &Option<StreamSymbol>,
    rejection: &SubscribeErrorKind,
    mc: &mut ManagedConnection,
    registry: &mut SubscriptionRegistry,
) {
    if url != WsUrl::UnifiedPublic {
        return;
    }
    let Some(peer) = channel.book_peer() else {
        return;
    };
    if !registry.unified_book_peer_live(channel, pair)
        || registry.unified_book_peer_acked(channel, pair)
    {
        return;
    }
    let SubscribeErrorKind::SubscribeRejected {
        kraken_code, ack, ..
    } = rejection;
    registry.note_terminated_for(
        url,
        peer,
        pair,
        crate::api::subscription::TerminationCause::NonTransientWireRejection,
        Some(kraken_code.clone()),
    );
    mc.clear_initial_subscribe_replay(peer, std::slice::from_ref(pair));
    mc.disarm_book_reseed_snapshot(peer, pair.clone());
    if !subscribe_failure_follows_fsm(mc) {
        mc.emit_futures_subscribe_failed(peer, pair, false, Some(kraken_code), ack);
    }
    mc.handle_event(
        crate::conn::managed_connection::FsmEvent::WireSubscribeFailed {
            channel: peer,
            pair: pair.clone(),
            error: rejection.clone(),
        },
    );
}

#[expect(
    clippy::too_many_arguments,
    reason = "takes only the shared handles it uses; mutable reactor state and per-call data are passed explicitly"
)]
/// A subscribe reply that proved the token ends the auth probe; `acked` marks and announces the entry first.
fn accept_auth_probe_reply(
    url: WsUrl,
    channel: ChannelName,
    pair: &Option<StreamSymbol>,
    acked: Option<(&FuturesSubscribeAck, crate::types::MonotonicInstant)>,
    mc: &mut ManagedConnection,
    registry: &mut SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    auth_stack: &Arc<crate::auth::AuthStack>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    mc.handle_event(crate::conn::managed_connection::FsmEvent::WireAuthHandshakeOk);
    mc.disarm_subscribe_ack(channel, pair.clone());
    if let Some((ack, now)) = acked {
        // No later ack covers the first signed entry; mark it Acked here.
        announce_acked(url, channel, pair, ack, now, mc, registry);
    }
    // Drop the deferral record, else a refresh drain re-subscribes a live entry.
    mc.remove_deferred_open_auth_subscribe(channel, pair);
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
    }
}

/// Marks the acked row(s) and announces each logical channel the one wire ack covers.
fn announce_acked(
    url: WsUrl,
    channel: ChannelName,
    pair: &Option<StreamSymbol>,
    ack: &FuturesSubscribeAck,
    now: crate::types::MonotonicInstant,
    mc: &mut ManagedConnection,
    registry: &mut SubscriptionRegistry,
) {
    for acknowledged in registry.note_unified_book_acked_for(url, channel, pair, ack, now) {
        mc.emit_futures_subscribed(acknowledged, pair, ack);
        // A no-op on token-bearing URLs: they never record an initial replay.
        mc.clear_initial_subscribe_replay(acknowledged, std::slice::from_ref(pair));
    }
}

fn handle_sub_ack(
    url: WsUrl,
    json: &Value,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    handles: SharedHandles<'_>,
    malformed_warn_tracker: &mut SuppressWarnTracker,
) -> bool {
    let SharedHandles {
        auth_stack,
        order_gate,
        bus_back_ref,
        state_mirrors,
        ..
    } = handles;
    let method = match json.get("method").and_then(Value::as_str) {
        Some(m @ ("subscribe" | "unsubscribe")) => m,
        _ => return false,
    };
    if method == "unsubscribe" {
        return true;
    }
    // Absent/non-bool `success` is a failure, never a silent success.
    let success_opt = json.get("success").and_then(Value::as_bool);
    if success_opt.is_none() {
        suppress_warn_throttled(
            url,
            "sub-ack-no-success",
            None,
            malformed_warn_tracker,
            bus_back_ref,
            || {
                tracing::warn!(
                    target: "kraken_sdk::io_reactor", ?url,
                    "subscribe-ack frame with absent/non-bool `success`; treating as failure"
                )
            },
        );
    }
    // A missing or non-bool `success` counts as a failure.
    let success = success_opt.unwrap_or(false);
    let field = |name: &str| sub_ack_field(json, name);
    let wire_channel = field("channel")
        .and_then(Value::as_str)
        .and_then(ChannelName::from_wire_str);
    let echoed_pair = field("symbol").and_then(Value::as_str).and_then(|s| {
        if url == WsUrl::UnifiedPublic && wire_channel == Some(ChannelName::Ohlc) {
            Some(StreamSymbol::futures_ohlc(
                s.to_string(),
                OhlcInterval::from_minutes(field("interval")?.as_u64()?)?,
                match field("tick_type").and_then(Value::as_str) {
                    Some(wire) => Some(FuturesOhlcTickType::from_wire_str(wire)?),
                    None => None,
                },
            ))
        } else {
            stream_symbol_for_url(url, s)
        }
    });
    let req_id = json.get("req_id").and_then(Value::as_u64);
    let ack = futures_subscribe_ack(json);
    let Some(mc) = conns.get_mut(&url) else {
        return true;
    };
    let now = mc.clock_now();
    let req_correlation = req_id.and_then(|id| mc.resolve_subscribe_req_id(id));
    if !success
        && wire_channel.is_none()
        && echoed_pair.is_none()
        && let Some(candidates) = req_correlation.as_ref()
        && candidates.len() > 1
    {
        let rejection = reply_rejection(json, ack);
        for (channel, pair) in candidates.iter().cloned() {
            if !mc.is_subscribe_ack_armed(channel, pair.clone()) {
                continue;
            }
            apply_subscribe_rejection(url, channel, pair, &rejection, mc, registry);
        }
        write_mirror(state_mirrors, url, mc.state());
        return true;
    }
    // A Unified derivatives ack echoes a wire symbol that cannot construct a Spot `Symbol`; its req_id recovers the local key.
    let resolved = match (wire_channel, echoed_pair, req_correlation) {
        (Some(ChannelName::Instrument), Some(StreamSymbol::Futures(symbol)), Some(candidates))
            if url == WsUrl::UnifiedPublic && symbol == "*" =>
        {
            Some(
                candidates
                    .iter()
                    .find(|(channel, pair)| *channel == ChannelName::Instrument && pair.is_none())
                    .cloned()
                    .unwrap_or((ChannelName::Instrument, None)),
            )
        }
        (Some(ch), Some(pair), Some(candidates))
            if candidates.contains(&(ch, Some(pair.clone()))) =>
        {
            Some((ch, Some(pair)))
        }
        // Unified Futures keeps Book and BookRaw as distinct entries though the venue acks both as `book`; an unmatched symbol is not a book ack.
        (Some(_), Some(pair), Some(candidates)) if url == WsUrl::UnifiedPublic => candidates
            .iter()
            .find(|(_, candidate_pair)| candidate_pair.as_ref() == Some(&pair))
            .cloned(),
        // An unmatched symbol resolves only to a channel-wide entry this request subscribed; on a
        // per-symbol request it stays unmatched.
        (Some(ch), Some(_), Some(candidates)) => candidates
            .iter()
            .find(|(mapped_ch, mapped_pair)| *mapped_ch == ch && mapped_pair.is_none())
            .cloned(),
        (Some(ch), Some(pair), None) => Some((ch, Some(pair))),
        (Some(ch), None, Some(candidates)) => {
            let channel_wide = candidates
                .iter()
                .find(|(mapped_ch, mapped_pair)| *mapped_ch == ch && mapped_pair.is_none())
                .cloned();
            if let Some(found) = channel_wide {
                Some(found)
            } else if candidates.len() == 1 {
                // A reject naming the channel but no symbol belongs to the one in-flight subscription.
                candidates.first().cloned()
            } else {
                Some((ch, None))
            }
        }
        (Some(ch), None, None) => Some((ch, None)),
        (None, _, Some(candidates)) if candidates.len() == 1 => candidates.first().cloned(),
        (None, _, None) | (None, _, Some(_)) => None,
    };
    let Some((channel, pair)) = resolved else {
        return true;
    };
    if mc.state() == ConnectionState::Authenticating {
        if success {
            accept_auth_probe_reply(
                url,
                channel,
                &pair,
                Some((&ack, now)),
                mc,
                registry,
                sockets,
                auth_stack,
                bus_back_ref,
            );
            mc.complete_resubscribe_if_empty();
        } else if let Some(err_msg) = json
            .get("error")
            .and_then(Value::as_str)
            .filter(|e| url == WsUrl::UnifiedPrivate && classify_auth_class(url, e).is_none())
        {
            accept_auth_probe_reply(
                url,
                channel,
                &pair,
                None,
                mc,
                registry,
                sockets,
                auth_stack,
                bus_back_ref,
            );
            apply_subscribe_rejection(
                url,
                channel,
                pair,
                &subscribe_rejection(err_msg, ack),
                mc,
                registry,
            );
            mc.complete_resubscribe_if_empty();
        } else {
            // A token-stale re-enter must spawn the reactive force_refresh, else nothing re-issues the first signed subscribe.
            let err_msg = json
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("auth handshake rejected");
            let kind = classify_auth_handshake_error(url, err_msg);
            emit_auth_handshake_failed(bus_back_ref, url, mc.attempt_count(), kind, err_msg);
            mc.handle_event(
                crate::conn::managed_connection::FsmEvent::WireAuthHandshakeFailed { kind },
            );
            mc.disarm_subscribe_ack(channel, pair.clone());
            if mc.state() == ConnectionState::Authenticating && mc.awaiting_token_refresh() {
                // The two force_refresh sites are mutually exclusive per handshake attempt.
                mc.force_reactive_token_refresh(auth_stack);
            }
        }
        write_mirror(state_mirrors, url, mc.state());
        if url.requires_ws_token() {
            super::refresh_send_ready(url, mc, registry, auth_stack, order_gate, bus_back_ref);
        }
        return true;
    }
    // A spurious, duplicate, late or never-armed ack must not drive the FSM; it is still consumed.
    if !mc.is_subscribe_ack_armed(channel, pair.clone()) {
        // A late success proves the subscription exists: kill the retry chain and deferral record.
        if success {
            mc.disarm_subscribe_ack(channel, pair.clone());
            mc.remove_deferred_open_auth_subscribe(channel, &pair);
            // The ack timer may have popped during Connecting; the row is still Pending, so record the ack and emit subscribed.
            if !url.requires_ws_token() {
                announce_acked(url, channel, &pair, &ack, now, mc, registry);
            }
            mc.complete_resubscribe_if_empty();
            write_mirror(state_mirrors, url, mc.state());
        }
        return true;
    }
    // The status subscribe completes via a spurious "Symbol(s) not found" reject; treat it as the ack.
    let status_quirk_ack = channel == ChannelName::Status
        && !success
        && json
            .get("error")
            .and_then(Value::as_str)
            .is_some_and(|e| e.contains("Symbol(s) not found"));
    if success || status_quirk_ack {
        let last = mc.pending_subscribe_ack_count() <= 1;
        announce_acked(url, channel, &pair, &ack, now, mc, registry);
        mc.remove_deferred_open_auth_subscribe(channel, &pair);
        // Connecting has no subscribe-ack arm; disarm so the timer cannot burn retry budget for an Acked row.
        // disarm_subscribe_ack expands a shared Unified book to both logical rows.
        if !url.requires_ws_token() && mc.state() == ConnectionState::Connecting {
            mc.disarm_subscribe_ack(channel, pair);
            write_mirror(state_mirrors, url, mc.state());
            return true;
        }
        mc.handle_event(
            crate::conn::managed_connection::FsmEvent::WireSubscribeAck {
                channel,
                pair,
                last,
            },
        );
    } else {
        let rejection = reply_rejection(json, ack);
        apply_subscribe_rejection(url, channel, pair, &rejection, mc, registry);
    }
    write_mirror(state_mirrors, url, mc.state());
    true
}

fn subscribe_failure_follows_fsm(mc: &ManagedConnection) -> bool {
    matches!(
        mc.state(),
        ConnectionState::Connecting | ConnectionState::Open | ConnectionState::Resubscribing
    )
}

fn subscribe_rejection_is_transient(err_msg: &str) -> bool {
    crate::error::is_kraken_rate_limit(err_msg)
}

fn reply_rejection(json: &Value, ack: FuturesSubscribeAck) -> SubscribeErrorKind {
    subscribe_rejection(
        json.get("error")
            .and_then(Value::as_str)
            .unwrap_or("subscribe rejected"),
        ack,
    )
}

fn subscribe_rejection(err_msg: &str, ack: FuturesSubscribeAck) -> SubscribeErrorKind {
    SubscribeErrorKind::SubscribeRejected {
        kraken_code: err_msg.to_owned(),
        transient: subscribe_rejection_is_transient(err_msg),
        ack,
    }
}

// SUCCESS acks nest `channel`/`symbol` under `result`; a FAILURE may carry them top-level.
fn sub_ack_field<'a>(json: &'a Value, name: &str) -> Option<&'a Value> {
    json.get("result")
        .and_then(|result| result.get(name))
        .or_else(|| json.get(name))
}

fn futures_subscribe_ack(json: &Value) -> FuturesSubscribeAck {
    let text = |value: Option<&Value>| value.and_then(Value::as_str).map(str::to_owned);
    FuturesSubscribeAck {
        snapshot: sub_ack_field(json, "snapshot").and_then(Value::as_bool),
        snap_orders: sub_ack_field(json, "snap_orders").and_then(Value::as_bool),
        snap_trades: sub_ack_field(json, "snap_trades").and_then(Value::as_bool),
        depth: sub_ack_field(json, "depth")
            .and_then(Value::as_u64)
            .and_then(|n| u32::try_from(n).ok()),
        warnings: sub_ack_field(json, "warnings")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        req_id: json.get("req_id").and_then(Value::as_u64),
        time_in: text(json.get("time_in")),
        time_out: text(json.get("time_out")),
    }
}

/// Tombstone a non-transient reject and always report the failure; a transient reject keeps its retry budget.
fn apply_subscribe_rejection(
    url: WsUrl,
    channel: ChannelName,
    pair: Option<StreamSymbol>,
    rejection: &SubscribeErrorKind,
    mc: &mut ManagedConnection,
    registry: &mut SubscriptionRegistry,
) {
    let SubscribeErrorKind::SubscribeRejected {
        kraken_code,
        transient,
        ack,
    } = rejection;
    if !subscribe_failure_follows_fsm(mc) {
        mc.emit_futures_subscribe_failed(channel, &pair, *transient, Some(kraken_code), ack);
    }
    if !transient {
        registry.note_terminated_for(
            url,
            channel,
            &pair,
            crate::api::subscription::TerminationCause::NonTransientWireRejection,
            Some(kraken_code.clone()),
        );
        mc.disarm_book_reseed_snapshot(channel, pair.clone());
        mc.clear_initial_subscribe_replay(channel, std::slice::from_ref(&pair));
        terminate_pending_unified_book_peer(url, channel, &pair, rejection, mc, registry);
    }
    mc.handle_event(
        crate::conn::managed_connection::FsmEvent::WireSubscribeFailed {
            channel,
            pair,
            error: rejection.clone(),
        },
    );
}

fn stream_symbol_for_url(url: WsUrl, wire: &str) -> Option<StreamSymbol> {
    match url {
        WsUrl::UnifiedPublic | WsUrl::UnifiedPrivate => {
            Some(StreamSymbol::Futures(wire.to_string()))
        }
        WsUrl::Public | WsUrl::Auth => Symbol::new(wire).ok().map(StreamSymbol::Spot),
    }
}

fn route_text_frame(
    url: WsUrl,
    json: &Value,
    registry: &mut SubscriptionRegistry,
    handlers: &HandlerRegistry,
    bus_back_ref: &Weak<DispatchEventBus>,
    drop_tracker: &mut HashMap<(WsUrl, ChannelName), (crate::types::MonotonicInstant, u32)>,
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

    let futures_margin = url == WsUrl::UnifiedPrivate && channel == ChannelName::Margin;
    let futures_private_object = url == WsUrl::UnifiedPrivate
        && matches!(
            channel,
            ChannelName::Positions | ChannelName::Margin | ChannelName::Preferences
        );
    let unified_instrument = url == WsUrl::UnifiedPublic && channel == ChannelName::Instrument;
    let unified_book = url == WsUrl::UnifiedPublic && channel == ChannelName::Book;
    if !matches!(msg_type, Some("update") | Some("snapshot")) && !futures_margin {
        return gaps;
    }
    if futures_margin && let Some(error) = json.get("error").and_then(Value::as_str) {
        suppress_warn_throttled(
            url,
            "futures-margin-error",
            Some(channel),
            malformed_warn_tracker,
            bus_back_ref,
            || {
                tracing::warn!(
                    target: "kraken_sdk::io_reactor", ?channel, error,
                    "Unified Futures margin channel reported an error; no margin data delivered"
                )
            },
        );
        return gaps;
    }

    // Sequence-gap detection runs before per-entry decode: a frame whose rows fail decode was still received.
    if url.tracks_sequence(channel)
        && let Some(seq) = json.get("sequence").and_then(Value::as_u64)
    {
        let is_snapshot = msg_type == Some("snapshot");
        if let Some(dropped) = registry.note_sequence_for(url, channel, seq, is_snapshot) {
            tracing::warn!(
                target: "kraken_sdk::io_reactor", channel = channel_str, seq, dropped,
                "sequence gap on account channel; frames lost mid-connection"
            );
            emit_channel_gap_event(url, channel, dropped, bus_back_ref);
        }
    }

    if unified_instrument && msg_type != Some("snapshot") {
        suppress_warn_throttled(
            url,
            "unified-instrument-update",
            Some(channel),
            malformed_warn_tracker,
            bus_back_ref,
            || {
                tracing::warn!(
                    target: "kraken_sdk::io_reactor",
                    "unrecognized Unified Futures instrument update shape; frame dropped"
                );
            },
        );
        return gaps;
    }

    let Some(data) = json
        .get("data")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .or_else(|| {
            (unified_instrument || futures_private_object)
                .then(|| {
                    json.get("data")
                        .filter(|data| data.is_object())
                        .map(std::slice::from_ref)
                })
                .flatten()
        })
    else {
        // Non-array `data` is a symbol-less malformed frame: throttled warn only, no MessageDroppedNoHandler.
        suppress_warn_throttled(
            url,
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
    let handler_url = if url == WsUrl::Auth && channel == ChannelName::Status {
        WsUrl::Public
    } else {
        url
    };
    for entry in data {
        if futures_private_object
            && entry
                .get("options")
                .and_then(Value::as_array)
                .is_some_and(|rows| !rows.is_empty())
        {
            suppress_warn_throttled(
                url,
                "unmodelled-futures-private-array",
                Some(channel),
                malformed_warn_tracker,
                bus_back_ref,
                || {
                    tracing::warn!(
                        target: "kraken_sdk::io_reactor", ?channel,
                        "Unified Futures private frame carries a non-empty array the SDK does not model; ignored"
                    )
                },
            );
        }
        let symbol = entry
            .get("symbol")
            .and_then(Value::as_str)
            .and_then(|s| stream_symbol_for_url(url, s));

        // Maintained-book state is driven once per frame on the `book` wire channel; Spot BookRaw
        // shares Book's entry, while Unified BookRaw is a separate row fed by the same frame.
        let awaiting_seed = unified_book && registry.unified_book_awaiting_seed(&symbol);
        let outcome =
            registry.handle_update_for(url, channel, symbol.as_ref(), is_snapshot, entry, || {
                super::bus_clock_now(bus_back_ref)
            });

        if unified_book
            && is_snapshot
            && matches!(outcome, BookDriveOutcome::FuturesMaintained(_))
            && registry.note_unified_book_seeded_for(&symbol, super::bus_clock_now(bus_back_ref))
        {
            release_held_book_seed_ack(&symbol, registry, bus_back_ref);
        }

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
                    // The seed hold suppressed the subscribe ack; once maintenance drops, the subscribed event must still fire.
                    if awaiting_seed && !will_resubscribe {
                        release_held_book_seed_ack(&symbol, registry, bus_back_ref);
                    }
                    if will_resubscribe {
                        gaps.push(BookGap { symbol: sym });
                    }
                }
                None => {
                    suppress_warn_throttled(
                        url,
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
            // Skip only the maintained `Book` fan on a gap or resync; `BookRaw` still fans.
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
            let handler_list = handlers.handlers_for_url(handler_url, fan_channel);
            if handler_list.is_empty() {
                // No-handler drops count only for the primary wire channel; a book frame drops only when BookRaw is also unhandled.
                // Status is suppressed (auto-seeded).
                if fan_channel == channel
                    && channel != ChannelName::Status
                    && (channel != ChannelName::Book
                        || handlers
                            .handlers_for_url(handler_url, ChannelName::BookRaw)
                            .is_empty())
                {
                    record_drop_and_maybe_emit(url, channel, drop_tracker, bus_back_ref);
                }
                continue;
            }
            if fan_channel == ChannelName::Book && matches!(outcome, BookDriveOutcome::NoEntry) {
                continue;
            }
            if fan_channel == ChannelName::Book {
                #[cfg(test)]
                let typed_delivery_start = super::latency_histogram::typed_delivery_probe_start();
                let payload: Option<Arc<dyn std::any::Any + Send + Sync>> = match &outcome {
                    BookDriveOutcome::Maintained(update) => Some(Arc::new(update.clone())),
                    BookDriveOutcome::FuturesMaintained(update) => Some(Arc::clone(update) as _),
                    _ => None,
                };
                if let Some(payload) = payload {
                    if let Some(bus) = bus_back_ref.upgrade() {
                        fan_out_data(&bus, handler_list, &payload, url, fan_channel);
                    }
                    #[cfg(test)]
                    super::latency_histogram::typed_delivery_probe_end(typed_delivery_start);
                    continue;
                }
            }
            let Some(bus) = bus_back_ref.upgrade() else {
                continue;
            };
            let cb = Arc::clone(&handler_list[0].callback.decode);
            let build_envelope = || {
                if url.tracks_sequence(channel) {
                    serde_json::json!({ "type": msg_type, "sequence": json.get("sequence"), "data": entry })
                } else {
                    serde_json::json!({ "type": msg_type, "data": entry })
                }
            };
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb(build_envelope())));
            match result {
                Ok(crate::dispatch::HandlerDecodeOutcome::Deliver(payload)) => {
                    fan_out_data(&bus, handler_list, &payload, url, fan_channel);
                }
                Ok(crate::dispatch::HandlerDecodeOutcome::DecodeFailed) => match symbol.as_ref() {
                    Some(sym @ StreamSymbol::Spot(_)) => {
                        bus.publish_subscription_gap(
                            bus.clock().now(),
                            fan_channel,
                            sym,
                            None,
                            None,
                            crate::dispatch::GapCause::MalformedFrame,
                        );
                    }
                    Some(sym) if url == WsUrl::UnifiedPublic => {
                        let ohlc_field = |name: &str| {
                            entry.get(name).filter(|_| fan_channel == ChannelName::Ohlc)
                        };
                        bus.publish_subscription_gap(
                            bus.clock().now(),
                            fan_channel,
                            sym,
                            ohlc_field("interval")
                                .and_then(Value::as_u64)
                                .and_then(OhlcInterval::from_minutes),
                            ohlc_field("tick_type")
                                .and_then(Value::as_str)
                                .and_then(FuturesOhlcTickType::from_wire_str),
                            crate::dispatch::GapCause::MalformedFrame,
                        );
                    }
                    Some(_) => {
                        suppress_warn_throttled(
                            url,
                            "decode-fail-futures-private",
                            Some(fan_channel),
                            malformed_warn_tracker,
                            bus_back_ref,
                            || {
                                tracing::warn!(
                                    target: "kraken_sdk::io_reactor", ?fan_channel, ?url,
                                    "decode-failed Futures frame with no gap event for this connection; frame dropped"
                                )
                            },
                        );
                    }
                    None => {
                        suppress_warn_throttled(
                            url,
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
                            source: crate::dispatch::CallbackSource::for_data_channel(
                                url,
                                fan_channel,
                            ),
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

fn fan_out_data(
    bus: &DispatchEventBus,
    handler_list: &[crate::dispatch::handler_registry::HandlerEntry],
    payload: &Arc<dyn std::any::Any + Send + Sync>,
    url: WsUrl,
    channel: ChannelName,
) {
    for h in handler_list {
        bus.publish_data(
            crate::dispatch::DataDelivery {
                invoke: Arc::clone(&h.callback.invoke),
                payload: Arc::clone(payload),
            },
            crate::dispatch::event_bus::DataRouting {
                url,
                channel,
                handler_id: h.id,
            },
        );
    }
}

pub(super) struct BookGap {
    pub symbol: StreamSymbol,
}

/// CRC32-gap recovery: drop local state, enter resync, resubscribe the `(Book, symbol)`, arm the reseed liveness timer.
fn recover_book_gap(
    url: WsUrl,
    gap: BookGap,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    // Drop the book and enter resync before the connection lookup so it is dropped even if the connection is absent.
    registry.begin_book_resync_for(url, ChannelName::Book, &gap.symbol);
    let Some(params) = registry.params_for_url(url, ChannelName::Book, &Some(gap.symbol.clone()))
    else {
        return;
    };
    let Some(mc) = conns.get_mut(&url) else {
        return;
    };
    send_book_reseed(url, gap.symbol, params, mc, sockets, bus_back_ref);
}

pub(in crate::dispatch::io_reactor) fn send_book_reseed(
    url: WsUrl,
    symbol: StreamSymbol,
    params: crate::conn::subscription_registry::SubscribeParams,
    mc: &mut ManagedConnection,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    // Raw unsubscribe: no termination event, no refcount change (a reseed, not a caller unsubscribe).
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
    let due_at = super::due_after_ms(mc, super::subscribe_ack_timeout_ms(bus_back_ref));
    mc.arm_book_reseed_snapshot(ChannelName::Book, Some(symbol), due_at);
}

pub(super) fn handle_book_reseed_timeout(
    url: WsUrl,
    pair: Option<StreamSymbol>,
    mc: &mut ManagedConnection,
    registry: &mut SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    let Some(symbol) = pair else {
        return;
    };
    let awaiting_seed = registry.unified_book_awaiting_seed(&Some(symbol.clone()));
    match registry.note_reseed_timeout_for(url, ChannelName::Book, &symbol, mc.clock_now()) {
        crate::conn::subscription_registry::ReseedTimeoutOutcome::StaleNoOp => {}
        crate::conn::subscription_registry::ReseedTimeoutOutcome::Retry => {
            let Some(params) =
                registry.params_for_url(url, ChannelName::Book, &Some(symbol.clone()))
            else {
                return;
            };
            send_book_reseed(url, symbol, params, mc, sockets, bus_back_ref);
        }
        crate::conn::subscription_registry::ReseedTimeoutOutcome::Exhausted => {
            if awaiting_seed {
                release_held_book_seed_ack(&Some(symbol.clone()), registry, bus_back_ref);
            }
            emit_book_gap_events(&symbol, bus_back_ref);
        }
    }
}

/// The seed hold suppressed the maintained book's subscribe ack; announce it now.
fn release_held_book_seed_ack(
    pair: &Option<StreamSymbol>,
    registry: &mut SubscriptionRegistry,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    if let Some(bus) = bus_back_ref.upgrade() {
        bus.publish_futures_subscribed(
            bus.clock().now(),
            ChannelName::Book,
            pair.as_ref(),
            registry.take_unified_book_seed_ack(pair),
        );
    }
}

fn emit_book_gap_events(symbol: &StreamSymbol, bus_back_ref: &Weak<DispatchEventBus>) {
    use crate::dispatch::{EventEnvelope, EventPayload, EventType, GapCause};
    let Some(bus) = bus_back_ref.upgrade() else {
        return;
    };
    let now = bus.clock().now();
    let cause = GapCause::OrderBookCrcMismatch;
    let channel = ChannelName::Book;
    let (event_type, event_version, payload) = match symbol {
        StreamSymbol::Spot(symbol) => (
            EventType::OrderBookGapEvent,
            2,
            EventPayload::OrderBookGapEvent {
                channel,
                symbol: symbol.clone(),
                cause,
            },
        ),
        StreamSymbol::Futures(symbol) | StreamSymbol::FuturesOhlc { symbol, .. } => (
            EventType::FuturesOrderBookGapEvent,
            1,
            EventPayload::FuturesOrderBookGapEvent {
                channel,
                symbol: symbol.clone(),
                cause,
            },
        ),
    };
    bus.publish(EventEnvelope {
        event_type,
        event_version,
        timestamp_monotonic: now,
        request_id: None,
        payload,
    });
    bus.publish_subscription_gap(now, channel, symbol, None, None, cause);
}

/// Not debounced: each jump is a distinct loss.
fn emit_channel_gap_event(
    url: WsUrl,
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
                url,
                channel,
                dropped_count,
                cause: crate::dispatch::GapCause::SequenceGapDetected,
            },
        });
    }
}

/// Throttle window; the contract pins the semantics, not the exact duration.
const MESSAGE_DROPPED_PERIOD: std::time::Duration = std::time::Duration::from_secs(1);

pub(super) type SuppressWarnTracker =
    HashMap<(WsUrl, &'static str, Option<ChannelName>), crate::types::MonotonicInstant>;

fn suppress_warn_throttled(
    url: WsUrl,
    site: &'static str,
    channel: Option<ChannelName>,
    tracker: &mut SuppressWarnTracker,
    bus_back_ref: &Weak<DispatchEventBus>,
    warn: impl FnOnce(),
) {
    let now = super::bus_clock_now(bus_back_ref);
    let should_warn = match tracker.get_mut(&(url, site, channel)) {
        None => {
            tracker.insert((url, site, channel), now);
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

fn record_drop_and_maybe_emit(
    url: WsUrl,
    channel: ChannelName,
    drop_tracker: &mut HashMap<(WsUrl, ChannelName), (crate::types::MonotonicInstant, u32)>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    let now = super::bus_clock_now(bus_back_ref);
    let emit = |count: u32| {
        if let Some(bus) = bus_back_ref.upgrade() {
            bus.publish(crate::dispatch::EventEnvelope {
                event_type: crate::dispatch::EventType::MessageDroppedNoHandler,
                event_version: 1,
                timestamp_monotonic: now,
                request_id: None,
                payload: crate::dispatch::EventPayload::MessageDroppedNoHandler {
                    url,
                    channel,
                    count,
                    period: MESSAGE_DROPPED_PERIOD,
                },
            });
        }
    };
    match drop_tracker.get_mut(&(url, channel)) {
        None => {
            drop_tracker.insert((url, channel), (now, 0));
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
pub(super) mod tests {
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
            "Preferences are not available yet, try again later",
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
            classify_auth_handshake_error(WsUrl::Auth, "EAPI:Invalid token"),
            AuthErrorKind::TokenStale
        );
        assert_eq!(
            classify_auth_handshake_error(WsUrl::Auth, "token expired"),
            AuthErrorKind::TokenStale
        );
        assert_eq!(
            classify_auth_handshake_error(WsUrl::Auth, "auth token is stale"),
            AuthErrorKind::TokenStale
        );
    }

    #[test]
    fn classify_invalid_session_is_token_stale() {
        // `ESession:Invalid session` carries no `token` marker and needs its own arm.
        assert_eq!(
            classify_auth_class(WsUrl::Auth, "ESession:Invalid session"),
            Some(AuthErrorKind::TokenStale),
        );
        assert_eq!(
            classify_auth_handshake_error(WsUrl::Auth, "ESession:Invalid session"),
            AuthErrorKind::TokenStale,
        );
        assert_eq!(
            classify_auth_class(WsUrl::Auth, "some unrelated error"),
            None
        );
    }

    #[test]
    fn classify_bad_creds_family() {
        assert_eq!(
            classify_auth_handshake_error(WsUrl::Auth, "EAPI:Invalid key"),
            AuthErrorKind::BadCreds
        );
        assert_eq!(
            classify_auth_handshake_error(WsUrl::Auth, "EAPI:Invalid signature"),
            AuthErrorKind::BadCreds
        );
        assert_eq!(
            classify_auth_handshake_error(WsUrl::Auth, "EAPI:Permission denied"),
            AuthErrorKind::BadCreds
        );
    }

    #[test]
    fn classify_permission_denied_egeneral() {
        assert_eq!(
            classify_auth_handshake_error(WsUrl::Auth, "EGeneral:Permission denied"),
            AuthErrorKind::PermissionDenied
        );
    }

    #[test]
    fn classify_unknown_defaults_transient() {
        assert_eq!(
            classify_auth_handshake_error(WsUrl::Auth, "EService:Unavailable"),
            AuthErrorKind::Transient
        );
        assert_eq!(
            classify_auth_handshake_error(WsUrl::Auth, "something totally unexpected"),
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
                classify_auth_class(WsUrl::Auth, s),
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
                classify_auth_class(WsUrl::Auth, s),
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
                classify_auth_class(WsUrl::Auth, s),
                Some(AuthErrorKind::PermissionDenied),
                "novel permission string should classify PermissionDenied: {s}"
            );
        }
    }

    #[test]
    fn classify_order_ack_novel_auth_string_fails_auth_not_authenticated() {
        // A novel auth-class string on success:false classifies AuthFailed: a bad token cannot drive -> Open.
        for (s, expect) in [
            ("EAuth:UnsupportedAuthMethod", AuthErrorKind::BadCreds),
            ("EAPI:Invalid token: expired", AuthErrorKind::TokenStale),
            ("EAccess:Permission denied", AuthErrorKind::PermissionDenied),
        ] {
            match classify_order_ack_auth(WsUrl::Auth, false, Some(s)) {
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
            classify_order_ack_auth(WsUrl::Auth, false, Some("EOrder:Insufficient funds")),
            OrderAckAuth::Authenticated
        ));
    }

    #[tokio::test]
    async fn channelless_reject_correlates_via_req_id_and_terminates_entry() {
        use crate::clock::{Clock, SystemClock};
        use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};

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
        let pair = Symbol::new("FAKECOIN/USD").ok().map(StreamSymbol::from);
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
        let order_gate = Arc::new(crate::conn::OrderGate::default());
        let bus_back_ref = Arc::downgrade(&bus);
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

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
            test_handles(&auth_stack, &order_gate, &bus_back_ref, &state_mirrors),
            &mut HashMap::new(),
        );
        assert!(consumed, "an ack frame is always consumed");

        // Generous timeout: the event rides the spawned dispatch task.
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
                assert_eq!(ev_pair, pair.as_ref().and_then(StreamSymbol::spot).cloned());
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

    #[tokio::test]
    async fn bad_channel_reject_correlates_via_req_id_only() {
        use crate::clock::{Clock, SystemClock};
        use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};

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
        let pair = Symbol::new("FAKECOIN/USD").ok().map(StreamSymbol::from);
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
        let order_gate = Arc::new(crate::conn::OrderGate::default());
        let bus_back_ref = Arc::downgrade(&bus);
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        // A symbol-only fallback would fail here; only req_id correlates.
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
            test_handles(&auth_stack, &order_gate, &bus_back_ref, &state_mirrors),
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
                assert_eq!(ev_pair, pair.as_ref().and_then(StreamSymbol::spot).cloned());
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

    #[tokio::test]
    async fn status_channel_spurious_reject_completes_without_teardown() {
        use crate::clock::{Clock, SystemClock};
        use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};

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
        let pair: Option<StreamSymbol> = None;
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
        let order_gate = Arc::new(crate::conn::OrderGate::default());
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
            test_handles(&auth_stack, &order_gate, &bus_back_ref, &state_mirrors),
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
            classify_order_ack_auth(WsUrl::Auth, false, Some("some unrecognized text")),
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

    use crate::clock::FixedClock;

    fn fixed_clock_bus(
        at: crate::types::MonotonicInstant,
    ) -> (Arc<DispatchEventBus>, Arc<dyn Clock>) {
        let clock: Arc<dyn Clock> = Arc::new(FixedClock(at.0));
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        (bus, clock)
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

    fn test_handles<'a>(
        auth_stack: &'a Arc<crate::auth::AuthStack>,
        order_gate: &'a Arc<crate::conn::OrderGate>,
        bus_back_ref: &'a Weak<DispatchEventBus>,
        state_mirrors: &'a HashMap<WsUrl, Arc<AtomicU8>>,
    ) -> SharedHandles<'a> {
        static UPGRADE_TX: std::sync::LazyLock<mpsc::Sender<UpgradeOutcome>> =
            std::sync::LazyLock::new(|| mpsc::channel(1).0);
        static CONNECT_IDS: std::sync::LazyLock<Arc<std::sync::atomic::AtomicU64>> =
            std::sync::LazyLock::new(Arc::default);
        SharedHandles {
            auth_stack,
            order_gate,
            bus_back_ref,
            state_mirrors,
            upgrade_tx: &UPGRADE_TX,
            connect_id_allocator: &CONNECT_IDS,
        }
    }

    fn test_mc(
        url: WsUrl,
        bus: &Arc<DispatchEventBus>,
        state: ConnectionState,
    ) -> ManagedConnection {
        test_mc_with_clock(url, bus, state, Arc::new(SystemClock))
    }

    fn test_mc_with_clock(
        url: WsUrl,
        bus: &Arc<DispatchEventBus>,
        state: ConnectionState,
        clock: Arc<dyn Clock>,
    ) -> ManagedConnection {
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

    use crate::test_logs::capture_logs;
    use std::sync::Mutex as StdMutex;

    #[test]
    fn no_handler_drop_names_the_connection_for_a_shared_channel() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let handlers = test_handlers();
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let bus_back = Arc::downgrade(&bus);

        for (url, symbol) in [
            (WsUrl::Public, "BTC/USD"),
            (WsUrl::UnifiedPublic, "PI_XBTUSD"),
        ] {
            let frame = serde_json::json!({
                "channel": "ticker",
                "type": "update",
                "data": [{ "symbol": symbol }],
            });
            route_text_frame(
                url,
                &frame,
                &mut registry,
                &handlers,
                &bus_back,
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );
        }

        let drops: Vec<_> = bus
            .test_drain_published()
            .into_iter()
            .filter(|e| e.event_type == crate::dispatch::EventType::MessageDroppedNoHandler)
            .map(|e| e.payload)
            .collect();
        let expected = |url| crate::dispatch::EventPayload::MessageDroppedNoHandler {
            url,
            channel: ChannelName::Ticker,
            count: 1,
            period: MESSAGE_DROPPED_PERIOD,
        };
        assert_eq!(
            drops,
            vec![expected(WsUrl::Public), expected(WsUrl::UnifiedPublic)]
        );
    }

    #[test]
    fn an_uncorrelated_rate_limit_refusal_is_logged_once_and_claims_only_that_frame() {
        use serde_json::json;
        let bus = test_bus();
        let registry = SubscriptionRegistry::new();
        let sockets: HashMap<WsUrl, Arc<dyn WsSocket>> = HashMap::new();
        let auth_stack = test_auth_stack(&bus);
        let order_gate = Arc::new(crate::conn::OrderGate::default());
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        let bus_back = Arc::downgrade(&bus);
        let mut conns = HashMap::from([(
            WsUrl::UnifiedPrivate,
            test_mc(WsUrl::UnifiedPrivate, &bus, ConnectionState::Open),
        )]);
        let mut tracker: SuppressWarnTracker = HashMap::new();
        let mut handle = |frame: &Value| {
            handle_request_response(
                WsUrl::UnifiedPrivate,
                frame,
                &mut conns,
                &registry,
                &sockets,
                test_handles(&auth_stack, &order_gate, &bus_back, &state_mirrors),
                &mut tracker,
            )
        };

        let refusal = json!({"error": "Rate limit exceeded", "method": "", "success": false});
        let logs = capture_logs(|| {
            assert!(handle(&refusal), "the refusal is consumed here");
            assert!(handle(&refusal));
        });
        assert_eq!(
            logs.matches("rate-limit refusal carried no req_id").count(),
            1,
            "one warn per window; got: {logs:?}"
        );

        let other = json!({"error": "Something else", "method": "", "success": false});
        let logs = capture_logs(|| assert!(!handle(&other), "another failure is not claimed"));
        assert!(!logs.contains("carried no req_id"), "{logs:?}");

        let subscribe_throttle = json!({
            "error": "Rate limit exceeded", "method": "subscribe", "success": false,
        });
        let logs = capture_logs(|| {
            assert!(
                !handle(&subscribe_throttle),
                "a method-bearing frame is left to the subscribe handler"
            )
        });
        assert!(!logs.contains("carried no req_id"), "{logs:?}");
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
    fn futures_decode_failure_emits_one_gap_naming_the_ohlc_stream_the_row_carries() {
        use crate::dispatch::{EventPayload, GapCause, HandlerCallback, HandlerDecodeOutcome};
        use crate::types::{FuturesOhlcTickType as Tick, OhlcInterval};

        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let mut handlers = test_handlers();
        for (id, channel) in [
            (1, ChannelName::Ticker),
            (2, ChannelName::Ohlc),
            (3, ChannelName::Ohlc),
        ] {
            handlers.register_with_id_for_url(
                WsUrl::UnifiedPublic,
                channel,
                crate::dispatch::HandlerId(id),
                HandlerCallback::new(
                    Arc::new(|_| HandlerDecodeOutcome::DecodeFailed),
                    Arc::new(|_| {}),
                ),
            );
        }
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();

        let cases = [
            (
                ChannelName::Ohlc,
                5.into(),
                "mark".into(),
                Some(OhlcInterval::M5),
                Some(Tick::Mark),
            ),
            (
                ChannelName::Ohlc,
                1.into(),
                Value::Null,
                Some(OhlcInterval::M1),
                None,
            ),
            (ChannelName::Ohlc, Value::Null, Value::Null, None, None),
            (ChannelName::Ohlc, "5".into(), "index".into(), None, None),
            (ChannelName::Ticker, 5.into(), "mark".into(), None, None),
        ];
        for (channel, row_interval, row_tick_type, interval, tick_type) in cases {
            let mut row = serde_json::json!({
                "symbol": "PF_XBTUSD", "interval": row_interval, "tick_type": row_tick_type
            });
            row.as_object_mut()
                .expect("row object")
                .retain(|_, value| !value.is_null());
            let frame = serde_json::json!({
                "channel": channel.as_ref(),
                "type": "update",
                "data": [row],
            });

            route_text_frame(
                WsUrl::UnifiedPublic,
                &frame,
                &mut registry,
                &handlers,
                &Arc::downgrade(&bus),
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );

            let gaps: Vec<_> = bus
                .test_drain_published()
                .into_iter()
                .filter_map(|event| match event.payload {
                    EventPayload::FuturesSubscriptionGapEvent {
                        channel,
                        symbol,
                        interval,
                        tick_type,
                        dropped_count,
                        cause,
                    } => Some((channel, symbol, interval, tick_type, dropped_count, cause)),
                    _ => None,
                })
                .collect();
            assert_eq!(
                gaps,
                vec![(
                    channel,
                    "PF_XBTUSD".to_string(),
                    interval,
                    tick_type,
                    0,
                    GapCause::MalformedFrame
                )],
                "channel {channel:?}, row {row}"
            );
        }
    }

    #[test]
    fn unified_private_decode_failure_emits_no_public_futures_gap() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let mut handlers = test_handlers();
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPrivate,
            ChannelName::Ticker,
            crate::dispatch::HandlerId(1),
            crate::dispatch::HandlerCallback::new(
                Arc::new(|_| crate::dispatch::HandlerDecodeOutcome::DecodeFailed),
                Arc::new(|_| {}),
            ),
        );
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let frame = serde_json::json!({
            "channel": "ticker",
            "type": "update",
            "data": [{ "symbol": "PI_XBTUSD" }],
        });

        route_text_frame(
            WsUrl::UnifiedPrivate,
            &frame,
            &mut registry,
            &handlers,
            &Arc::downgrade(&bus),
            &mut drop_tracker,
            &mut malformed_warn_tracker,
        );

        let events = bus.test_drain_published();
        assert!(!events.iter().any(|event| matches!(
            event.event_type,
            crate::dispatch::EventType::FuturesSubscriptionGapEvent
        )));
        assert!(!events.iter().any(|event| matches!(
            event.event_type,
            crate::dispatch::EventType::SubscriptionGapEvent
        )));
    }

    fn recording_handler(store: Arc<StdMutex<Vec<Value>>>) -> crate::dispatch::HandlerCallback {
        crate::dispatch::HandlerCallback::new(
            Arc::new(move |envelope| {
                store.lock().unwrap().push(envelope);
                crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))
            }),
            Arc::new(|_| {}),
        )
    }

    fn route_one(url: WsUrl, channel: ChannelName, frame: &Value) -> Vec<Value> {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let mut handlers = test_handlers();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        handlers.register_with_id_for_url(
            url,
            channel,
            crate::dispatch::HandlerId(1),
            recording_handler(Arc::clone(&seen)),
        );
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        route_text_frame(
            url,
            frame,
            &mut registry,
            &handlers,
            &Arc::downgrade(&bus),
            &mut drop_tracker,
            &mut malformed_warn_tracker,
        );
        seen.lock().unwrap().clone()
    }

    #[test]
    fn unified_private_margin_frame_is_routed_by_its_type_with_object_data() {
        let frame = serde_json::json!({
            "channel": "margin",
            "type": "multi_collateral",
            "data": { "available_margin": "12.5", "cross": {}, "time": "t" },
        });
        let delivered = route_one(WsUrl::UnifiedPrivate, ChannelName::Margin, &frame);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0]["type"], "multi_collateral");
        assert_eq!(delivered[0]["data"]["available_margin"], "12.5");

        let single = serde_json::json!({
            "channel": "margin",
            "type": "single_collateral",
            "data": { "time": "t", "wallets": [] },
        });
        let delivered = route_one(WsUrl::UnifiedPrivate, ChannelName::Margin, &single);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0]["type"], "single_collateral");
    }

    #[test]
    fn unified_private_preferences_object_data_reaches_the_handler() {
        let frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/private_preferences_snapshot.json"
        ))
        .expect("checked-in preferences fixture is valid JSON");
        let snapshot = frames
            .iter()
            .find(|frame| frame["channel"] == "preferences")
            .expect("snapshot frame");

        let delivered = route_one(WsUrl::UnifiedPrivate, ChannelName::Preferences, snapshot);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0]["data"]["futures_trading"], "enabled");
        assert!(route_one(WsUrl::Auth, ChannelName::Preferences, snapshot).is_empty());
        assert!(route_one(WsUrl::UnifiedPublic, ChannelName::Preferences, snapshot).is_empty());
    }

    #[test]
    fn unified_private_positions_object_data_reaches_the_handler() {
        let frame = serde_json::json!({
            "channel": "positions",
            "type": "update",
            "data": { "time": "t", "futures": [], "options": [] },
        });
        let delivered = route_one(WsUrl::UnifiedPrivate, ChannelName::Positions, &frame);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0]["type"], "update");
        assert_eq!(delivered[0]["data"]["time"], "t");
    }

    #[test]
    fn unified_private_balances_rows_reach_the_handler_with_type_and_sequence() {
        let frame = serde_json::json!({
            "channel": "balances", "type": "snapshot", "sequence": 7,
            "data": [ { "asset": "USD" }, { "asset": "BTC" } ],
        });
        let delivered = route_one(WsUrl::UnifiedPrivate, ChannelName::Balances, &frame);
        assert_eq!(delivered.len(), 2);
        for envelope in &delivered {
            assert_eq!(envelope["type"], "snapshot");
            assert_eq!(envelope["sequence"], 7);
        }
    }

    #[test]
    fn channels_that_track_no_sequence_get_no_sequence_in_the_envelope() {
        let frame = serde_json::json!({
            "channel": "executions", "type": "update", "sequence": 5,
            "data": [ { "exec_type": "new" } ],
        });
        let delivered = route_one(WsUrl::UnifiedPrivate, ChannelName::Executions, &frame);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0]["type"], "update");
        assert!(delivered[0].get("sequence").is_none());
    }

    #[test]
    fn margin_type_bypass_and_object_data_are_scoped_to_unified_private() {
        let margin = serde_json::json!({
            "channel": "margin",
            "type": "multi_collateral",
            "data": { "time": "t" },
        });
        assert!(route_one(WsUrl::UnifiedPublic, ChannelName::Margin, &margin).is_empty());
        assert!(route_one(WsUrl::Auth, ChannelName::Margin, &margin).is_empty());

        let ticker = serde_json::json!({
            "channel": "ticker",
            "type": "multi_collateral",
            "data": [{ "symbol": "PF_XBTUSD" }],
        });
        assert!(route_one(WsUrl::UnifiedPrivate, ChannelName::Ticker, &ticker).is_empty());

        let object_ticker = serde_json::json!({
            "channel": "ticker",
            "type": "update",
            "data": { "symbol": "PF_XBTUSD" },
        });
        assert!(route_one(WsUrl::UnifiedPrivate, ChannelName::Ticker, &object_ticker).is_empty());
    }

    #[test]
    fn unified_private_non_empty_unmodelled_array_warns_once_and_still_delivers() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let mut handlers = test_handlers();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPrivate,
            ChannelName::Positions,
            crate::dispatch::HandlerId(1),
            recording_handler(Arc::clone(&seen)),
        );
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let populated = serde_json::json!({
            "channel": "positions",
            "type": "update",
            "data": { "time": "t", "futures": [], "options": [{ "secret": "x" }] },
        });
        let empty = serde_json::json!({
            "channel": "positions",
            "type": "update",
            "data": { "time": "t", "futures": [], "options": [] },
        });

        let logs = capture_logs(|| {
            for frame in [&populated, &populated, &empty] {
                route_text_frame(
                    WsUrl::UnifiedPrivate,
                    frame,
                    &mut registry,
                    &handlers,
                    &Arc::downgrade(&bus),
                    &mut drop_tracker,
                    &mut malformed_warn_tracker,
                );
            }
        });

        assert_eq!(
            logs.matches("non-empty array the SDK does not model")
                .count(),
            1,
            "warn is throttled per channel; captured logs: {logs:?}"
        );
        assert!(
            !logs.contains("secret"),
            "the warn must not echo frame content"
        );
        assert_eq!(seen.lock().unwrap().len(), 3, "frames are still delivered");
    }

    #[test]
    fn populated_single_collateral_wallets_are_delivered_without_the_unmodelled_warn() {
        let frame = serde_json::json!({
            "channel": "margin",
            "type": "single_collateral",
            "data": { "time": "t", "wallets": [{ "unit": "XBT", "balance": "1" }] },
        });
        let mut delivered = Vec::new();
        let logs = capture_logs(|| {
            delivered = route_one(WsUrl::UnifiedPrivate, ChannelName::Margin, &frame);
        });
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0]["data"]["wallets"][0]["unit"], "XBT");
        assert!(
            !logs.contains("non-empty array the SDK does not model"),
            "wallets are modelled; captured logs: {logs:?}"
        );
    }

    #[test]
    fn unified_private_margin_error_frame_warns_once_with_its_own_message() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let mut handlers = test_handlers();
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPrivate,
            ChannelName::Margin,
            crate::dispatch::HandlerId(1),
            recording_handler(Arc::clone(&seen)),
        );
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let frame = serde_json::json!({"channel": "margin", "error": "margin_unavailable"});

        let logs = capture_logs(|| {
            for _ in 0..2 {
                route_text_frame(
                    WsUrl::UnifiedPrivate,
                    &frame,
                    &mut registry,
                    &handlers,
                    &Arc::downgrade(&bus),
                    &mut drop_tracker,
                    &mut malformed_warn_tracker,
                );
            }
        });

        assert_eq!(
            logs.matches("margin channel reported an error").count(),
            1,
            "warn is throttled per channel; captured logs: {logs:?}"
        );
        assert!(logs.contains("margin_unavailable"), "logs: {logs:?}");
        assert!(
            !logs.contains("non-array"),
            "the error frame must not hit the malformed-data warn: {logs:?}"
        );
        assert!(seen.lock().unwrap().is_empty(), "no handler delivery");
    }

    #[test]
    fn unified_private_empty_arrays_do_not_warn() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let handlers = test_handlers();
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let frame = serde_json::json!({
            "channel": "margin",
            "type": "single_collateral",
            "data": { "time": "t", "wallets": [] },
        });
        let logs = capture_logs(|| {
            route_text_frame(
                WsUrl::UnifiedPrivate,
                &frame,
                &mut registry,
                &handlers,
                &Arc::downgrade(&bus),
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );
        });
        assert!(!logs.contains("does not model"), "logs: {logs:?}");
    }

    #[test]
    fn unified_private_futures_decode_failure_warns_instead_of_dropping_silently() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let mut handlers = test_handlers();
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPrivate,
            ChannelName::Executions,
            crate::dispatch::HandlerId(1),
            crate::dispatch::HandlerCallback::new(
                Arc::new(|_| crate::dispatch::HandlerDecodeOutcome::DecodeFailed),
                Arc::new(|_| {}),
            ),
        );
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let frame = serde_json::json!({
            "channel": "executions",
            "type": "update",
            "data": [{ "symbol": "PF_XBTUSD", "exec_type": 7 }],
        });

        let logs = capture_logs(|| {
            route_text_frame(
                WsUrl::UnifiedPrivate,
                &frame,
                &mut registry,
                &handlers,
                &Arc::downgrade(&bus),
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );
        });

        assert!(
            logs.contains("decode-failed Futures frame"),
            "an undecodable Futures private frame must warn; captured logs: {logs:?}"
        );
        assert!(
            !logs.contains("PF_XBTUSD"),
            "the warn must not echo frame content"
        );
    }

    #[test]
    fn status_frame_with_no_handler_does_not_flood_message_dropped() {
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

    #[test]
    fn unified_status_frame_does_not_invoke_spot_status_handler() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let mut handlers = test_handlers();
        let spot_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let spot_decoder_calls = Arc::clone(&spot_calls);
        handlers.register_with_id_for_url(
            WsUrl::Public,
            ChannelName::Status,
            crate::dispatch::HandlerId(1),
            crate::dispatch::HandlerCallback::new(
                Arc::new(move |_frame| {
                    spot_decoder_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))
                }),
                Arc::new(|_payload| {}),
            ),
        );
        let unified_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let unified_decoder_calls = Arc::clone(&unified_calls);
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPublic,
            ChannelName::Status,
            crate::dispatch::HandlerId(2),
            crate::dispatch::HandlerCallback::new(
                Arc::new(move |_frame| {
                    unified_decoder_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))
                }),
                Arc::new(|_payload| {}),
            ),
        );
        let frame = serde_json::json!({
            "channel": "status",
            "type": "update",
            "data": [{ "api_version": "v2", "futures_exchange": "online" }],
        });
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let bus_back = Arc::downgrade(&bus);

        route_text_frame(
            WsUrl::UnifiedPublic,
            &frame,
            &mut registry,
            &handlers,
            &bus_back,
            &mut drop_tracker,
            &mut malformed_warn_tracker,
        );
        assert_eq!(
            spot_calls.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "unsolicited Unified status must not reach Spot callbacks"
        );
        assert_eq!(
            unified_calls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "Unified status reaches only its Unified handler"
        );

        route_text_frame(
            WsUrl::Public,
            &frame,
            &mut registry,
            &handlers,
            &bus_back,
            &mut drop_tracker,
            &mut malformed_warn_tracker,
        );
        assert_eq!(spot_calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(unified_calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    fn counting_decoder(
        calls: &Arc<std::sync::atomic::AtomicUsize>,
    ) -> crate::dispatch::HandlerCallback {
        let calls = Arc::clone(calls);
        crate::dispatch::HandlerCallback::new(
            Arc::new(move |_frame| {
                calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))
            }),
            Arc::new(|_payload| {}),
        )
    }

    fn route_all(
        url: WsUrl,
        frames: &[Value],
        registry: &mut SubscriptionRegistry,
        handlers: &HandlerRegistry,
        bus: &Arc<DispatchEventBus>,
    ) {
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        for frame in frames {
            route_text_frame(
                url,
                frame,
                registry,
                handlers,
                &Arc::downgrade(bus),
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );
        }
    }

    #[test]
    fn spot_auth_status_frame_reaches_spot_status_handler_only() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let mut handlers = test_handlers();
        let spot = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let unified = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        handlers.register_with_id_for_url(
            WsUrl::Public,
            ChannelName::Status,
            crate::dispatch::HandlerId(1),
            counting_decoder(&spot),
        );
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPublic,
            ChannelName::Status,
            crate::dispatch::HandlerId(2),
            counting_decoder(&unified),
        );
        let frame = serde_json::json!({
            "channel": "status",
            "type": "update",
            "data": [{ "system": "online", "api_version": "v2" }],
        });

        route_all(
            WsUrl::Auth,
            std::slice::from_ref(&frame),
            &mut registry,
            &handlers,
            &bus,
        );
        assert_eq!(spot.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(unified.load(std::sync::atomic::Ordering::Relaxed), 0);

        route_all(
            WsUrl::UnifiedPrivate,
            std::slice::from_ref(&frame),
            &mut registry,
            &handlers,
            &bus,
        );
        route_all(
            WsUrl::UnifiedPublic,
            std::slice::from_ref(&frame),
            &mut registry,
            &handlers,
            &bus,
        );
        assert_eq!(spot.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(unified.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    fn futures_book_frame(kind: &str, checksum: u32) -> Value {
        serde_json::json!({
            "channel": "book", "type": kind, "data": [{
                "symbol": "PI_XBTUSD",
                "bids": [{ "price": "99.0", "qty": "2.0" }],
                "asks": [{ "price": "100.0", "qty": "1.5" }],
                "checksum": checksum,
            }],
        })
    }

    #[test]
    fn unified_degraded_book_still_delivers_per_frame() {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};

        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Book,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                SubscribeParams::Book {
                    depth: crate::types::BookDepth::D10,
                },
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        let mut handlers = test_handlers();
        let book = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPublic,
            ChannelName::Book,
            crate::dispatch::HandlerId(1),
            counting_decoder(&book),
        );
        let bad = vec![futures_book_frame("snapshot", 1); 5];
        route_all(WsUrl::UnifiedPublic, &bad, &mut registry, &handlers, &bus);
        assert_eq!(book.load(std::sync::atomic::Ordering::Relaxed), 0);

        route_all(
            WsUrl::UnifiedPublic,
            &[futures_book_frame("update", 1)],
            &mut registry,
            &handlers,
            &bus,
        );
        assert_eq!(
            book.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the A409 degrade keeps per-frame on_book delivery"
        );
    }

    #[test]
    fn unified_book_frame_with_no_handlers_still_counts_a_drop() {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};

        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D10,
                    snapshot: Some(true),
                },
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        route_all(
            WsUrl::UnifiedPublic,
            &[futures_book_frame("update", 0)],
            &mut registry,
            &test_handlers(),
            &bus,
        );
        assert!(bus.test_drain_published().iter().any(|event| matches!(
            event.payload,
            crate::dispatch::EventPayload::MessageDroppedNoHandler {
                channel: ChannelName::Book,
                ..
            }
        )));
    }

    #[test]
    fn spot_book_frame_without_entry_still_reaches_book_handler() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let mut handlers = test_handlers();
        let book = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let raw = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        handlers.register_with_id_for_url(
            WsUrl::Public,
            ChannelName::Book,
            crate::dispatch::HandlerId(1),
            counting_decoder(&book),
        );
        handlers.register_with_id_for_url(
            WsUrl::Public,
            ChannelName::BookRaw,
            crate::dispatch::HandlerId(2),
            counting_decoder(&raw),
        );
        let frame = serde_json::json!({
            "channel": "book", "type": "update", "data": [{
                "symbol": "BTC/USD",
                "bids": [{ "price": "99.0", "qty": "2.0" }],
                "asks": [],
                "checksum": 0,
            }],
        });
        route_all(WsUrl::Public, &[frame], &mut registry, &handlers, &bus);
        assert_eq!(book.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(raw.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn unified_instrument_snapshot_routes_its_object_to_unified_handler() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let mut handlers = test_handlers();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let decoder_calls = Arc::clone(&calls);
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPublic,
            ChannelName::Instrument,
            crate::dispatch::HandlerId(1),
            crate::dispatch::HandlerCallback::new(
                Arc::new(move |frame| {
                    decoder_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    assert!(frame["data"].is_object(), "snapshot stays object-shaped");
                    crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))
                }),
                Arc::new(|_payload| {}),
            ),
        );
        let frame: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_instrument_snapshot.json"
        ))
        .expect("checked-in Unified Futures instrument fixture is valid JSON");
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();

        route_text_frame(
            WsUrl::UnifiedPublic,
            &frame,
            &mut registry,
            &handlers,
            &Arc::downgrade(&bus),
            &mut drop_tracker,
            &mut malformed_warn_tracker,
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn a_unified_instrument_update_frame_is_dropped_with_a_warning() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let mut handlers = test_handlers();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let decoder_calls = Arc::clone(&calls);
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPublic,
            ChannelName::Instrument,
            crate::dispatch::HandlerId(1),
            crate::dispatch::HandlerCallback::new(
                Arc::new(move |_frame| {
                    decoder_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))
                }),
                Arc::new(|_payload| {}),
            ),
        );
        let mut frame: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_instrument_snapshot.json"
        ))
        .expect("checked-in Unified Futures instrument fixture is valid JSON");
        frame["type"] = Value::from("update");
        let logs = capture_logs(|| {
            route_text_frame(
                WsUrl::UnifiedPublic,
                &frame,
                &mut registry,
                &handlers,
                &Arc::downgrade(&bus),
                &mut HashMap::new(),
                &mut HashMap::new(),
            );
        });
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert!(
            logs.contains("unrecognized Unified Futures instrument update shape"),
            "{logs}"
        );
    }

    #[test]
    fn releasing_a_unified_book_owner_disarms_its_ack_and_reseed_timers() {
        let bus = test_bus();
        let pair = Some(futures_symbol("PI_XBTUSD"));
        let mut mc = test_mc(WsUrl::UnifiedPublic, &bus, ConnectionState::Open);
        let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(5));
        mc.arm_subscribe_ack(ChannelName::Book, pair.clone(), due);
        let earlier = crate::types::MonotonicInstant(std::time::Duration::from_secs(1));
        mc.arm_book_reseed_snapshot(ChannelName::Book, pair.clone(), earlier);
        let other = Some(futures_symbol("PF_ETHUSD"));
        mc.arm_subscribe_ack(ChannelName::Book, other.clone(), due);
        let mut conns = HashMap::new();
        conns.insert(WsUrl::UnifiedPublic, mc);

        super::super::inbound_dispatch::release_unified_book_owner(
            WsUrl::UnifiedPublic,
            ChannelName::Book,
            pair.clone(),
            true,
            false,
            &mut conns,
            &SubscriptionRegistry::new(),
            &HashMap::new(),
            &Arc::downgrade(&bus),
        );

        let mc = &conns[&WsUrl::UnifiedPublic];
        assert!(!mc.is_subscribe_ack_armed(ChannelName::Book, pair));
        assert!(mc.is_subscribe_ack_armed(ChannelName::Book, other));
        assert_eq!(mc.pending_subscribe_ack_count(), 1);
        let (_, event) = mc
            .next_timer_due()
            .expect("the other row's ack timer remains");
        assert!(
            matches!(
                event,
                crate::conn::managed_connection::FsmEvent::TimerSubscribeAckTimeout { ref pair, .. }
                    if *pair == Some(futures_symbol("PF_ETHUSD"))
            ),
            "no reseed timer is left for the released row: {event:?}"
        );
    }

    #[test]
    fn unified_ticker_data_routes_by_futures_wire_symbol() {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
        use crate::types::{MonotonicInstant, StreamSymbol};

        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                SubscribeParams::Ticker {
                    snapshot: None,
                    event_trigger: None,
                },
            ),
            MonotonicInstant(std::time::Duration::ZERO),
            false,
        );

        let mut handlers = test_handlers();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let decoder_calls = Arc::clone(&calls);
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPublic,
            ChannelName::Ticker,
            crate::dispatch::HandlerId(1),
            crate::dispatch::HandlerCallback::new(
                Arc::new(move |_frame| {
                    decoder_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))
                }),
                Arc::new(|_payload| {}),
            ),
        );

        let frame = serde_json::json!({
            "channel": "ticker",
            "type": "update",
            "data": [{"symbol": "PI_XBTUSD"}],
        });
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let bus_back = Arc::downgrade(&bus);

        let gaps = route_text_frame(
            WsUrl::UnifiedPublic,
            &frame,
            &mut registry,
            &handlers,
            &bus_back,
            &mut drop_tracker,
            &mut malformed_warn_tracker,
        );

        assert!(gaps.is_empty());
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn book_gap_events_pin_type_version_and_order_per_product() {
        use crate::dispatch::{EventPayload, EventType, GapCause};

        let bus = test_bus();
        let weak = Arc::downgrade(&bus);
        emit_book_gap_events(
            &StreamSymbol::Spot(crate::types::Symbol::new("BTC/USD").expect("symbol")),
            &weak,
        );
        emit_book_gap_events(&StreamSymbol::Futures("PF_XBTUSD".to_string()), &weak);

        let events = bus.test_drain_published();
        let shape: Vec<_> = events
            .iter()
            .map(|event| (event.event_type, event.event_version, event.request_id))
            .collect();
        assert_eq!(
            shape,
            vec![
                (EventType::OrderBookGapEvent, 2, None),
                (EventType::SubscriptionGapEvent, 2, None),
                (EventType::FuturesOrderBookGapEvent, 1, None),
                (EventType::FuturesSubscriptionGapEvent, 1, None),
            ]
        );
        assert!(matches!(
            events[1].payload,
            EventPayload::SubscriptionGapEvent {
                channel: ChannelName::Book,
                dropped_count: 0,
                cause: GapCause::OrderBookCrcMismatch,
                ..
            }
        ));
        assert!(matches!(
            &events[3].payload,
            EventPayload::FuturesSubscriptionGapEvent {
                channel: ChannelName::Book,
                symbol,
                interval: None,
                tick_type: None,
                dropped_count: 0,
                cause: GapCause::OrderBookCrcMismatch,
            } if symbol == "PF_XBTUSD"
        ));
    }

    #[test]
    fn unified_ohlc_frames_decode_every_captured_candle_on_the_unified_route() {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
        use crate::types::{MonotonicInstant, OhlcInterval, StreamSymbol};

        let frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_ohlc_candles.json"
        ))
        .expect("checked-in Unified Futures OHLC fixture is valid JSON");
        let expected = frames
            .iter()
            .map(|frame| frame["data"].as_array().expect("ohlc data").len())
            .sum::<usize>();

        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        for symbol in ["PF_XBTUSD", "PF_ETHUSD"] {
            registry.register(
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ohlc,
                    Some(StreamSymbol::Futures(symbol.to_string())),
                    SubscribeParams::Ohlc {
                        interval: OhlcInterval::M1,
                        snapshot: None,
                        tick_type: None,
                    },
                ),
                MonotonicInstant(std::time::Duration::ZERO),
                false,
            );
        }
        let mut handlers = test_handlers();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let decoder_calls = Arc::clone(&calls);
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPublic,
            ChannelName::Ohlc,
            crate::dispatch::HandlerId(1),
            crate::dispatch::HandlerCallback::new(
                Arc::new(move |frame| {
                    let candle = crate::api::market::decode_futures_ohlc(frame)
                        .expect("captured candle decodes");
                    decoder_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(candle))
                }),
                Arc::new(|_payload| {}),
            ),
        );
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let bus_back = Arc::downgrade(&bus);

        for frame in &frames {
            let gaps = route_text_frame(
                WsUrl::UnifiedPublic,
                frame,
                &mut registry,
                &handlers,
                &bus_back,
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );
            assert!(gaps.is_empty());
        }

        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), expected);
    }

    #[test]
    fn unified_trade_replay_routes_every_print_without_deduplication() {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
        use crate::types::{MonotonicInstant, StreamSymbol};

        let frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_trade_updates_without_trade_id.json"
        ))
        .expect("checked-in Unified Futures trade fixture is valid JSON");
        let expected_prints = frames
            .iter()
            .map(|frame| frame["data"].as_array().expect("trade data").len())
            .sum::<usize>();

        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Trade,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                SubscribeParams::Trade {
                    snapshot: Some(true),
                },
            ),
            MonotonicInstant(std::time::Duration::ZERO),
            false,
        );
        let mut handlers = test_handlers();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let decoder_calls = Arc::clone(&calls);
        handlers.register_with_id_for_url(
            WsUrl::UnifiedPublic,
            ChannelName::Trade,
            crate::dispatch::HandlerId(1),
            crate::dispatch::HandlerCallback::new(
                Arc::new(move |frame| {
                    let trade = crate::api::market::decode_futures_trade(frame)
                        .expect("captured trade print decodes");
                    decoder_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(trade))
                }),
                Arc::new(|_payload| {}),
            ),
        );
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let bus_back = Arc::downgrade(&bus);

        for frame in &frames {
            let gaps = route_text_frame(
                WsUrl::UnifiedPublic,
                frame,
                &mut registry,
                &handlers,
                &bus_back,
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );
            assert!(gaps.is_empty());
        }

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            expected_prints,
            "every captured print, including identical-looking repeats, reaches decoding"
        );
    }

    #[test]
    fn unified_batched_ack_updates_each_symbol_independently() {
        assert_mixed_ticker_batch(ConnectionState::Open);
    }

    #[test]
    fn unified_batched_ack_in_connecting_terminates_and_disarms_the_reject() {
        assert_mixed_ticker_batch(ConnectionState::Connecting);
    }

    fn assert_mixed_ticker_batch(state: ConnectionState) {
        use crate::conn::subscription_registry::{
            EntrySubState, SubscribeParams, SubscriptionEntry,
        };

        let bus = test_bus();
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let mut registry = SubscriptionRegistry::with_mirror(Arc::clone(&mirror));
        let now = crate::types::MonotonicInstant::now();
        let good = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let bad = StreamSymbol::Futures("PI_NOTAREAL".to_string());
        let frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_ticker_mixed_batch_acks.json"
        ))
        .expect("checked-in Unified Futures mixed ticker fixture is valid JSON");
        let req_id = frames[0]["req_id"].as_u64().expect("fixture req_id");
        assert!(
            frames.iter().all(|frame| frame["req_id"] == req_id),
            "mixed batch outcomes share one request id"
        );
        for symbol in [good.clone(), bad.clone()] {
            registry.register(
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ticker,
                    Some(symbol),
                    SubscribeParams::Ticker {
                        snapshot: None,
                        event_trigger: None,
                    },
                ),
                now,
                false,
            );
        }
        let mut mc = test_mc(WsUrl::UnifiedPublic, &bus, state);
        for symbol in [good.clone(), bad.clone()] {
            mc.record_subscribe_req_id(req_id, ChannelName::Ticker, Some(symbol.clone()));
            mc.arm_subscribe_ack(
                ChannelName::Ticker,
                Some(symbol),
                crate::types::MonotonicInstant(std::time::Duration::from_secs(60)),
            );
        }
        let mut conns = HashMap::from([(WsUrl::UnifiedPublic, mc)]);

        for frame in &frames {
            assert!(route_sub_ack(
                &bus,
                WsUrl::UnifiedPublic,
                &mut conns,
                &mut registry,
                frame
            ));
        }

        let mirror = mirror.read().expect("mirror");
        assert!(matches!(
            mirror[&(WsUrl::UnifiedPublic, ChannelName::Ticker, Some(good))].state,
            EntrySubState::Acked
        ));
        assert!(matches!(
            mirror[&(WsUrl::UnifiedPublic, ChannelName::Ticker, Some(bad))].state,
            EntrySubState::Terminated { .. }
        ));
        assert!(conns[&WsUrl::UnifiedPublic].next_timer_due().is_none());
        let events = bus.test_drain_published();
        assert!(events.iter().any(|event| matches!(
            event.payload,
            crate::dispatch::EventPayload::FuturesSubscribedEvent {
                channel: ChannelName::Ticker,
                symbol: Some(ref symbol),
                ..
            } if symbol == "PI_XBTUSD"
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event.payload,
                    crate::dispatch::EventPayload::FuturesSubscribeFailedEvent {
                        channel: ChannelName::Ticker,
                        symbol: Some(ref symbol),
                        ..
                    } if symbol == "PI_NOTAREAL"
                ))
                .count(),
            1
        );
        assert!(events.iter().any(|event| matches!(
            event.payload,
            crate::dispatch::EventPayload::FuturesSubscriptionTerminatedEvent {
                channel: ChannelName::Ticker,
                symbol: Some(ref symbol),
                ..
            } if symbol == "PI_NOTAREAL"
        )));
        let echo = crate::dispatch::FuturesSubscribeAck {
            req_id: Some(req_id),
            ..Default::default()
        };
        assert!(events.iter().any(|event| matches!(
            &event.payload,
            crate::dispatch::EventPayload::FuturesSubscribedEvent { symbol: Some(symbol), ack, .. }
                if symbol == "PI_XBTUSD" && *ack == echo
        )));
        assert!(events.iter().any(|event| matches!(
            &event.payload,
            crate::dispatch::EventPayload::FuturesSubscribeFailedEvent {
                symbol: Some(symbol), error, ack, ..
            } if symbol == "PI_NOTAREAL"
                && error.as_deref() == Some("Unknown symbol: PI_NOTAREAL")
                && *ack == echo
        )));
    }

    #[test]
    fn unified_late_success_ack_marks_pending_row_when_timer_is_unarmed() {
        use crate::conn::subscription_registry::{
            EntrySubState, SubscribeParams, SubscriptionEntry,
        };

        let bus = test_bus();
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let mut registry = SubscriptionRegistry::with_mirror(Arc::clone(&mirror));
        let symbol = StreamSymbol::Futures("PI_XBTUSD".to_string());
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(symbol.clone()),
                SubscribeParams::Ticker {
                    snapshot: Some(true),
                    event_trigger: None,
                },
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        let mut mc = test_mc(WsUrl::UnifiedPublic, &bus, ConnectionState::Resubscribing);
        mc.mark_initial_subscribe_replay(ChannelName::Ticker, &[Some(symbol.clone())]);
        let mut conns = HashMap::from([(WsUrl::UnifiedPublic, mc)]);

        assert!(route_sub_ack(
            &bus,
            WsUrl::UnifiedPublic,
            &mut conns,
            &mut registry,
            &serde_json::json!({
                "method": "subscribe",
                "success": true,
                "req_id": 31,
                "time_in": "2026-11-05T10:00:00.000001Z",
                "result": { "channel": "ticker", "symbol": "PI_XBTUSD", "snapshot": true },
            })
        ));

        assert!(
            mirror
                .read()
                .expect("mirror")
                .get(&(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ticker,
                    Some(symbol.clone())
                ))
                .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
        );
        let mc = conns.get(&WsUrl::UnifiedPublic).expect("connection");
        assert_eq!(mc.state(), ConnectionState::Open);
        assert!(!mc.needs_initial_subscribe_replay());
        let echo = crate::dispatch::FuturesSubscribeAck {
            snapshot: Some(true),
            req_id: Some(31),
            time_in: Some("2026-11-05T10:00:00.000001Z".to_owned()),
            ..Default::default()
        };
        assert!(bus.test_drain_published().iter().any(|event| matches!(
            &event.payload,
            crate::dispatch::EventPayload::FuturesSubscribedEvent {
                channel: ChannelName::Ticker,
                symbol: Some(got),
                ack,
                ..
            } if got == "PI_XBTUSD" && *ack == echo
        )));
    }

    fn unified_private_ack_fixture(
        state: ConnectionState,
    ) -> (
        Arc<DispatchEventBus>,
        crate::conn::subscription_registry::SubscriptionMirror,
        HashMap<WsUrl, ManagedConnection>,
        SubscriptionRegistry,
    ) {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};

        let bus = test_bus();
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let mut registry = SubscriptionRegistry::with_mirror(Arc::clone(&mirror));
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPrivate,
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
        let conns = HashMap::from([(
            WsUrl::UnifiedPrivate,
            test_mc(WsUrl::UnifiedPrivate, &bus, state),
        )]);
        (bus, mirror, conns, registry)
    }

    fn ack_unified_private(
        bus: &Arc<DispatchEventBus>,
        conns: &mut HashMap<WsUrl, ManagedConnection>,
        registry: &mut SubscriptionRegistry,
    ) {
        route_unified_private(
            bus,
            conns,
            registry,
            &serde_json::json!({
                "method": "subscribe",
                "success": true,
                "req_id": 41,
                "result": { "channel": "executions", "asset_class": "derivatives", "snapshot": true },
            }),
        );
    }

    fn route_unified_private(
        bus: &Arc<DispatchEventBus>,
        conns: &mut HashMap<WsUrl, ManagedConnection>,
        registry: &mut SubscriptionRegistry,
        frame: &Value,
    ) {
        assert!(route_sub_ack(
            bus,
            WsUrl::UnifiedPrivate,
            conns,
            registry,
            frame
        ));
    }

    fn route_sub_ack(
        bus: &Arc<DispatchEventBus>,
        url: WsUrl,
        conns: &mut HashMap<WsUrl, ManagedConnection>,
        registry: &mut SubscriptionRegistry,
        frame: &Value,
    ) -> bool {
        handle_sub_ack(
            url,
            frame,
            conns,
            registry,
            &HashMap::new(),
            test_handles(
                &test_auth_stack(bus),
                &Arc::new(crate::conn::OrderGate::default()),
                &Arc::downgrade(bus),
                &HashMap::new(),
            ),
            &mut HashMap::new(),
        )
    }

    fn assert_private_executions_acked_and_announced(
        bus: &Arc<DispatchEventBus>,
        mirror: &crate::conn::subscription_registry::SubscriptionMirror,
    ) {
        use crate::conn::subscription_registry::EntrySubState;

        assert!(
            mirror
                .read()
                .expect("mirror")
                .get(&(WsUrl::UnifiedPrivate, ChannelName::Executions, None))
                .is_some_and(|row| matches!(row.state, EntrySubState::Acked))
        );
        let echo = crate::dispatch::FuturesSubscribeAck {
            snapshot: Some(true),
            req_id: Some(41),
            ..Default::default()
        };
        assert!(bus.test_drain_published().iter().any(|event| matches!(
            &event.payload,
            crate::dispatch::EventPayload::FuturesSubscribedEvent {
                channel: ChannelName::Executions,
                symbol: None,
                ack,
                ..
            } if *ack == echo
        )));
    }

    #[tokio::test]
    async fn unified_private_first_ack_while_authenticating_publishes_futures_subscribed() {
        let (bus, mirror, mut conns, mut registry) =
            unified_private_ack_fixture(ConnectionState::Authenticating);
        ack_unified_private(&bus, &mut conns, &mut registry);
        assert_private_executions_acked_and_announced(&bus, &mirror);
    }

    #[tokio::test]
    async fn unified_private_ack_reads_snap_orders_and_snap_trades_from_a_synthetic_reply_as_booleans_result_first()
     {
        use serde_json::json;
        let synthetic_cases = [
            (
                json!({ "snap_orders": false, "snap_trades": true }),
                json!({}),
                (Some(false), Some(true)),
            ),
            (
                json!({}),
                json!({ "snap_orders": true, "snap_trades": false }),
                (Some(true), Some(false)),
            ),
            (
                json!({ "snap_orders": true }),
                json!({ "snap_orders": false, "snap_trades": true }),
                (Some(true), Some(true)),
            ),
            (json!({}), json!({}), (None, None)),
            (
                json!({ "snap_orders": "true", "snap_trades": 1 }),
                json!({}),
                (None, None),
            ),
        ];
        for (result_keys, top_level_keys, expected) in synthetic_cases {
            let (bus, _, mut conns, mut registry) =
                unified_private_ack_fixture(ConnectionState::Authenticating);
            let mut frame = json!({
                "method": "subscribe",
                "success": true,
                "req_id": 41,
                "result": { "channel": "executions", "asset_class": "derivatives" },
            });
            for (key, value) in result_keys.as_object().expect("result keys") {
                frame["result"][key] = value.clone();
            }
            for (key, value) in top_level_keys.as_object().expect("top-level keys") {
                frame[key] = value.clone();
            }
            route_unified_private(&bus, &mut conns, &mut registry, &frame);
            let echoed: Vec<_> = bus
                .test_drain_published()
                .into_iter()
                .filter_map(|event| match event.payload {
                    crate::dispatch::EventPayload::FuturesSubscribedEvent {
                        channel: ChannelName::Executions,
                        ack,
                        ..
                    } => Some((ack.snap_orders, ack.snap_trades)),
                    _ => None,
                })
                .collect();
            assert_eq!(
                echoed,
                vec![expected],
                "synthetic reply result={result_keys} top-level={top_level_keys}"
            );
        }
    }

    #[test]
    fn unified_private_unarmed_success_ack_disarms_and_leaves_the_row_pending() {
        use crate::conn::subscription_registry::EntrySubState;

        let (bus, mirror, mut conns, mut registry) =
            unified_private_ack_fixture(ConnectionState::Open);
        ack_unified_private(&bus, &mut conns, &mut registry);

        assert!(
            mirror
                .read()
                .expect("mirror")
                .get(&(WsUrl::UnifiedPrivate, ChannelName::Executions, None))
                .is_some_and(|row| matches!(row.state, EntrySubState::Pending)),
            "an unarmed success carries nothing to tie it to this lifetime, so it never acks"
        );
        assert!(
            !bus.test_drain_published().iter().any(|event| matches!(
                event.payload,
                crate::dispatch::EventPayload::FuturesSubscribedEvent { .. }
            )),
            "no subscribed event without an armed ack"
        );
    }

    #[tokio::test]
    async fn unified_private_non_auth_reject_while_authenticating_reports_the_venue_error_and_reply_echo()
     {
        let reject = |error: &str| {
            serde_json::json!({
                "method": "subscribe",
                "success": false,
                "req_id": 41,
                "error": error,
                "time_in": "2026-11-05T10:00:00.000001Z",
                "time_out": "2026-11-05T10:00:00.000002Z",
                "result": { "channel": "executions" },
            })
        };
        let (bus, _, mut conns, mut registry) =
            unified_private_ack_fixture(ConnectionState::Authenticating);
        route_unified_private(
            &bus,
            &mut conns,
            &mut registry,
            &reject("Subscription limit exceeded"),
        );
        let echo = FuturesSubscribeAck {
            req_id: Some(41),
            time_in: Some("2026-11-05T10:00:00.000001Z".to_owned()),
            time_out: Some("2026-11-05T10:00:00.000002Z".to_owned()),
            ..Default::default()
        };
        assert_eq!(
            subscribe_failures(bus.test_drain_published()),
            vec![(
                ChannelName::Executions,
                Some("Subscription limit exceeded".to_owned()),
                echo
            )]
        );

        for auth_class in ["EAPI:Invalid key", "Permission denied"] {
            let (bus, _, mut conns, mut registry) =
                unified_private_ack_fixture(ConnectionState::Authenticating);
            route_unified_private(&bus, &mut conns, &mut registry, &reject(auth_class));
            assert_eq!(
                subscribe_failures(bus.test_drain_published()),
                vec![],
                "{auth_class}: an auth-class reject fails the handshake instead"
            );
        }
    }

    fn drive_unified_ack(
        state: ConnectionState,
        entries: Vec<crate::conn::subscription_registry::SubscriptionEntry>,
        reqs: &[(u64, ChannelName, StreamSymbol)],
        extra_armed: &[(ChannelName, StreamSymbol)],
        replay: &[(ChannelName, StreamSymbol)],
        frame: Value,
    ) -> (
        crate::conn::subscription_registry::SubscriptionMirror,
        HashMap<WsUrl, ManagedConnection>,
        Vec<crate::dispatch::EventEnvelope>,
    ) {
        let bus = test_bus();
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let mut registry = SubscriptionRegistry::with_mirror(Arc::clone(&mirror));
        let now = crate::types::MonotonicInstant::now();
        let due = crate::types::MonotonicInstant(std::time::Duration::from_secs(60));
        for entry in entries {
            registry.register(entry, now, false);
        }
        let mut mc = test_mc(WsUrl::UnifiedPublic, &bus, state);
        for (req_id, channel, symbol) in reqs {
            mc.record_subscribe_req_id(*req_id, *channel, Some(symbol.clone()));
            mc.arm_subscribe_ack(*channel, Some(symbol.clone()), due);
        }
        for (channel, symbol) in extra_armed {
            mc.arm_subscribe_ack(*channel, Some(symbol.clone()), due);
        }
        for (channel, symbol) in replay {
            mc.mark_initial_subscribe_replay(*channel, &[Some(symbol.clone())]);
        }
        let mut conns = HashMap::from([(WsUrl::UnifiedPublic, mc)]);
        let auth_stack = test_auth_stack(&bus);
        let ready = Arc::new(crate::conn::OrderGate::default());
        let weak = Arc::downgrade(&bus);
        assert!(handle_sub_ack(
            WsUrl::UnifiedPublic,
            &frame,
            &mut conns,
            &mut registry,
            &HashMap::new(),
            test_handles(&auth_stack, &ready, &weak, &HashMap::new()),
            &mut HashMap::new(),
        ));
        (mirror, conns, bus.test_drain_published())
    }

    fn futures_symbol(wire: &str) -> StreamSymbol {
        StreamSymbol::Futures(wire.to_string())
    }

    fn ticker_entry(
        symbol: &StreamSymbol,
    ) -> crate::conn::subscription_registry::SubscriptionEntry {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
        SubscriptionEntry::new(
            WsUrl::UnifiedPublic,
            ChannelName::Ticker,
            Some(symbol.clone()),
            SubscribeParams::Ticker {
                snapshot: None,
                event_trigger: None,
            },
        )
    }

    fn book_entry(
        channel: ChannelName,
        symbol: &StreamSymbol,
    ) -> crate::conn::subscription_registry::SubscriptionEntry {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
        let params = match channel {
            ChannelName::BookRaw => SubscribeParams::BookRaw {
                depth: crate::types::BookDepth::D25,
                snapshot: Some(true),
            },
            _ => SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            },
        };
        SubscriptionEntry::new(WsUrl::UnifiedPublic, channel, Some(symbol.clone()), params)
    }

    fn row_matches(
        mirror: &crate::conn::subscription_registry::SubscriptionMirror,
        channel: ChannelName,
        symbol: &StreamSymbol,
        pred: impl FnOnce(&crate::conn::subscription_registry::EntrySubState) -> bool,
    ) -> bool {
        pred(
            &mirror.read().expect("mirror")[&(WsUrl::UnifiedPublic, channel, Some(symbol.clone()))]
                .state,
        )
    }

    #[test]
    fn unified_unmatched_ack_symbol_does_not_become_a_book_ack() {
        use crate::conn::subscription_registry::EntrySubState;
        let ticker = futures_symbol("PI_XBTUSD");
        let other = futures_symbol("PI_ETHUSD");
        let (mirror, _, _) = drive_unified_ack(
            ConnectionState::Open,
            vec![ticker_entry(&ticker), book_entry(ChannelName::Book, &other)],
            &[(7, ChannelName::Ticker, ticker.clone())],
            &[(ChannelName::Book, other.clone())],
            &[],
            serde_json::json!({
                "method": "subscribe",
                "success": true,
                "req_id": 7,
                "result": { "channel": "ticker", "symbol": "PI_ETHUSD" },
            }),
        );
        assert!(row_matches(
            &mirror,
            ChannelName::Ticker,
            &ticker,
            |state| { matches!(state, EntrySubState::Pending) }
        ));
        assert!(row_matches(&mirror, ChannelName::Book, &other, |state| {
            matches!(state, EntrySubState::Pending)
        }));
    }

    #[test]
    fn channel_only_reject_uses_the_single_req_id_candidate() {
        use crate::conn::subscription_registry::EntrySubState;
        let symbol = futures_symbol("PI_XBTUSD");
        let (mirror, conns, _) = drive_unified_ack(
            ConnectionState::Open,
            vec![ticker_entry(&symbol)],
            &[(8, ChannelName::Ticker, symbol.clone())],
            &[],
            &[(ChannelName::Ticker, symbol.clone())],
            serde_json::json!({
                "method": "subscribe",
                "success": false,
                "req_id": 8,
                "error": "EGeneral:Invalid arguments",
                "result": { "channel": "ticker" },
            }),
        );
        assert!(row_matches(
            &mirror,
            ChannelName::Ticker,
            &symbol,
            |state| { matches!(state, EntrySubState::Terminated { .. }) }
        ));
        assert!(!conns[&WsUrl::UnifiedPublic].needs_initial_subscribe_replay());
    }

    #[test]
    fn batched_book_reject_clears_replay_and_ends_the_pending_peer() {
        use crate::conn::subscription_registry::EntrySubState;
        let symbol = futures_symbol("PI_XBTUSD");
        let (mirror, conns, _) = drive_unified_ack(
            ConnectionState::Open,
            vec![
                book_entry(ChannelName::Book, &symbol),
                book_entry(ChannelName::BookRaw, &symbol),
            ],
            &[
                (9, ChannelName::Book, symbol.clone()),
                (9, ChannelName::BookRaw, symbol.clone()),
            ],
            &[],
            &[
                (ChannelName::Book, symbol.clone()),
                (ChannelName::BookRaw, symbol.clone()),
            ],
            serde_json::json!({
                "method": "subscribe",
                "success": false,
                "req_id": 9,
                "error": "EGeneral:Invalid arguments",
            }),
        );
        for channel in [ChannelName::Book, ChannelName::BookRaw] {
            assert!(row_matches(&mirror, channel, &symbol, |state| {
                matches!(state, EntrySubState::Terminated { .. })
            }));
        }
        assert!(!conns[&WsUrl::UnifiedPublic].needs_initial_subscribe_replay());
    }

    #[test]
    fn book_reject_terminates_a_pending_peer_that_did_not_send() {
        use crate::conn::subscription_registry::EntrySubState;
        let symbol = futures_symbol("PI_XBTUSD");
        let (mirror, conns, _) = drive_unified_ack(
            ConnectionState::Open,
            vec![
                book_entry(ChannelName::Book, &symbol),
                book_entry(ChannelName::BookRaw, &symbol),
            ],
            &[(11, ChannelName::Book, symbol.clone())],
            &[],
            &[(ChannelName::BookRaw, symbol.clone())],
            serde_json::json!({
                "method": "subscribe",
                "success": false,
                "req_id": 11,
                "error": "EGeneral:Invalid arguments",
                "result": { "channel": "book", "symbol": "PI_XBTUSD" },
            }),
        );
        assert!(row_matches(
            &mirror,
            ChannelName::BookRaw,
            &symbol,
            |state| { matches!(state, EntrySubState::Terminated { .. }) }
        ));
        assert!(!conns[&WsUrl::UnifiedPublic].needs_initial_subscribe_replay());
    }

    #[test]
    fn unified_ohlc_success_ack_carries_its_identity_and_only_the_allow_listed_reply_fields() {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};
        use crate::types::{FuturesOhlcTickType, OhlcInterval};
        let ohlc = |wire: &str| {
            StreamSymbol::futures_ohlc(
                wire.to_owned(),
                OhlcInterval::M5,
                Some(FuturesOhlcTickType::Mark),
            )
        };
        let entry = |symbol: &StreamSymbol| {
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ohlc,
                Some(symbol.clone()),
                SubscribeParams::Ohlc {
                    interval: OhlcInterval::M5,
                    snapshot: None,
                    tick_type: Some(FuturesOhlcTickType::Mark),
                },
            )
        };
        let (xbt, eth) = (ohlc("PF_XBTUSD"), ohlc("PF_ETHUSD"));
        let (_, _, events) = drive_unified_ack(
            ConnectionState::Open,
            vec![entry(&xbt), entry(&eth)],
            &[(7, ChannelName::Ohlc, xbt), (7, ChannelName::Ohlc, eth)],
            &[],
            &[],
            serde_json::json!({
                "method": "subscribe",
                "success": true,
                "req_id": 7,
                "token": "SECRET-TOKEN",
                "time_in": "2026-11-05T10:00:00.000001Z",
                "time_out": "2026-11-05T10:00:00.000002Z",
                "result": {
                    "channel": "ohlc",
                    "interval": 5,
                    "snapshot": true,
                    "symbol": "PF_XBTUSD",
                    "tick_type": "mark",
                    "token": "SECRET-TOKEN",
                    "warnings": ["timestamp is deprecated, use interval_begin"],
                },
            }),
        );
        let expected = crate::dispatch::FuturesSubscribeAck {
            snapshot: Some(true),
            depth: None,
            warnings: vec!["timestamp is deprecated, use interval_begin".to_owned()],
            req_id: Some(7),
            time_in: Some("2026-11-05T10:00:00.000001Z".to_owned()),
            time_out: Some("2026-11-05T10:00:00.000002Z".to_owned()),
            ..Default::default()
        };
        let subscribed: Vec<_> = events
            .iter()
            .filter_map(|event| match &event.payload {
                crate::dispatch::EventPayload::FuturesSubscribedEvent {
                    channel: ChannelName::Ohlc,
                    symbol,
                    interval,
                    tick_type,
                    ack,
                    ..
                } => Some((symbol.clone(), *interval, *tick_type, ack.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            subscribed,
            vec![(
                Some("PF_XBTUSD".to_owned()),
                Some(OhlcInterval::M5),
                Some(FuturesOhlcTickType::Mark),
                expected,
            )],
            "{events:?}"
        );
        assert!(!format!("{events:?}").contains("SECRET-TOKEN"));
    }

    #[test]
    fn unified_book_ack_echoes_depth_on_both_book_rows() {
        let symbol = futures_symbol("PI_XBTUSD");
        let (_, _, events) = drive_unified_ack(
            ConnectionState::Open,
            vec![
                book_entry(ChannelName::Book, &symbol),
                book_entry(ChannelName::BookRaw, &symbol),
            ],
            &[
                (12, ChannelName::Book, symbol.clone()),
                (12, ChannelName::BookRaw, symbol.clone()),
            ],
            &[],
            &[],
            serde_json::json!({
                "method": "subscribe",
                "success": true,
                "req_id": 12,
                "result": { "channel": "book", "depth": 25, "snapshot": true, "symbol": "PI_XBTUSD" },
            }),
        );
        let depths: Vec<(ChannelName, Option<u32>)> = events
            .iter()
            .filter_map(|event| match &event.payload {
                crate::dispatch::EventPayload::FuturesSubscribedEvent { channel, ack, .. } => {
                    Some((*channel, ack.depth))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            depths,
            vec![
                (ChannelName::Book, Some(25)),
                (ChannelName::BookRaw, Some(25))
            ]
        );
    }

    fn reject_failures(
        state: ConnectionState,
        entries: Vec<crate::conn::subscription_registry::SubscriptionEntry>,
        reqs: &[(u64, ChannelName, StreamSymbol)],
        result: Option<Value>,
    ) -> Vec<(ChannelName, Option<String>, FuturesSubscribeAck)> {
        let mut frame = serde_json::json!({
            "method": "subscribe",
            "success": false,
            "req_id": 8,
            "error": "Unknown symbol: PI_XBTUSD",
            "time_in": "2026-11-05T10:00:00.000001Z",
            "time_out": "2026-11-05T10:00:00.000002Z",
        });
        if let Some(result) = result {
            frame["result"] = result;
        }
        let (_, _, events) = drive_unified_ack(state, entries, reqs, &[], &[], frame);
        subscribe_failures(events)
    }

    fn subscribe_failures(
        events: Vec<crate::dispatch::EventEnvelope>,
    ) -> Vec<(ChannelName, Option<String>, FuturesSubscribeAck)> {
        events
            .into_iter()
            .filter_map(|event| match event.payload {
                crate::dispatch::EventPayload::FuturesSubscribeFailedEvent {
                    channel,
                    error,
                    ack,
                    ..
                } => Some((channel, error, ack)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn every_reject_site_reports_the_venue_error_and_reply_echo() {
        let symbol = futures_symbol("PI_XBTUSD");
        let books = || {
            vec![
                book_entry(ChannelName::Book, &symbol),
                book_entry(ChannelName::BookRaw, &symbol),
            ]
        };
        let ticker_req = [(8, ChannelName::Ticker, symbol.clone())];
        let ticker = serde_json::json!({ "channel": "ticker", "symbol": "PI_XBTUSD" });
        let echo = FuturesSubscribeAck {
            req_id: Some(8),
            time_in: Some("2026-11-05T10:00:00.000001Z".to_owned()),
            time_out: Some("2026-11-05T10:00:00.000002Z".to_owned()),
            ..Default::default()
        };
        let failed = |channels: &[ChannelName]| -> Vec<_> {
            channels
                .iter()
                .map(|channel| {
                    (
                        *channel,
                        Some("Unknown symbol: PI_XBTUSD".to_owned()),
                        echo.clone(),
                    )
                })
                .collect()
        };

        assert_eq!(
            reject_failures(
                ConnectionState::Closing,
                vec![ticker_entry(&symbol)],
                &ticker_req,
                Some(ticker.clone()),
            ),
            failed(&[ChannelName::Ticker]),
            "reject outside the FSM states"
        );
        assert_eq!(
            reject_failures(
                ConnectionState::Resubscribing,
                vec![ticker_entry(&symbol)],
                &ticker_req,
                Some(ticker),
            ),
            failed(&[ChannelName::Ticker]),
            "reject while resubscribing"
        );
        assert_eq!(
            reject_failures(
                ConnectionState::Open,
                books(),
                &[
                    (8, ChannelName::Book, symbol.clone()),
                    (8, ChannelName::BookRaw, symbol.clone()),
                ],
                None,
            ),
            failed(&[ChannelName::BookRaw, ChannelName::Book]),
            "batched channel-less reject"
        );
        assert_eq!(
            reject_failures(
                ConnectionState::Closing,
                books(),
                &[(8, ChannelName::Book, symbol.clone())],
                Some(serde_json::json!({ "channel": "book", "symbol": "PI_XBTUSD" })),
            ),
            failed(&[ChannelName::Book, ChannelName::BookRaw]),
            "pending book peer ended outside the FSM states"
        );
    }

    fn late_book_holding_its_reply() -> (
        SubscriptionRegistry,
        crate::conn::subscription_registry::SubscriptionMirror,
        StreamSymbol,
        FuturesSubscribeAck,
    ) {
        let symbol = futures_symbol("PI_XBTUSD");
        let pair = Some(symbol.clone());
        let reply = FuturesSubscribeAck {
            snapshot: Some(true),
            depth: Some(25),
            req_id: Some(5),
            ..Default::default()
        };
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let mut registry = SubscriptionRegistry::with_mirror(Arc::clone(&mirror));
        registry.register(
            book_entry(ChannelName::Book, &symbol),
            crate::types::MonotonicInstant::now(),
            false,
        );
        registry.require_unified_book_seed_for(&pair);
        registry.begin_book_resync_for(WsUrl::UnifiedPublic, ChannelName::Book, &symbol);
        assert!(
            registry
                .note_unified_book_acked_for(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    &pair,
                    &reply,
                    crate::types::MonotonicInstant::now()
                )
                .is_empty()
        );
        (registry, mirror, symbol, reply)
    }

    fn book_row_subscribed_at(
        mirror: &crate::conn::subscription_registry::SubscriptionMirror,
        symbol: &StreamSymbol,
    ) -> Option<crate::types::MonotonicInstant> {
        mirror.read().expect("mirror")[&(
            WsUrl::UnifiedPublic,
            ChannelName::Book,
            Some(symbol.clone()),
        )]
            .subscribed_at
    }

    fn book_subscribed_acks(bus: &DispatchEventBus) -> Vec<FuturesSubscribeAck> {
        bus.test_drain_published()
            .into_iter()
            .filter_map(|event| match event.payload {
                crate::dispatch::EventPayload::FuturesSubscribedEvent {
                    channel: ChannelName::Book,
                    ack,
                    ..
                } => Some(ack),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn late_book_that_exhausts_its_reseed_timeouts_reports_its_held_reply() {
        let (mut registry, mirror, symbol, reply) = late_book_holding_its_reply();
        let at = crate::types::MonotonicInstant(std::time::Duration::from_secs(7));
        let (bus, clock) = fixed_clock_bus(at);
        let mut mc = test_mc_with_clock(WsUrl::UnifiedPublic, &bus, ConnectionState::Open, clock);
        let acks = (0..10).find_map(|_| {
            handle_book_reseed_timeout(
                WsUrl::UnifiedPublic,
                Some(symbol.clone()),
                &mut mc,
                &mut registry,
                &HashMap::new(),
                &Arc::downgrade(&bus),
            );
            Some(book_subscribed_acks(&bus)).filter(|acks| !acks.is_empty())
        });
        assert_eq!(acks, Some(vec![reply]));
        assert_eq!(book_row_subscribed_at(&mirror, &symbol), Some(at));
    }

    #[test]
    fn a_late_book_seeded_by_a_valid_snapshot_stamps_the_injected_clock() {
        let (mut registry, mirror, symbol, reply) = late_book_holding_its_reply();
        let at = crate::types::MonotonicInstant(std::time::Duration::from_secs(7));
        let (bus, _) = fixed_clock_bus(at);
        let frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_futures_book_crc_sequence.json"
        ))
        .expect("checked-in Unified Futures book fixture is valid JSON");
        route_all(
            WsUrl::UnifiedPublic,
            &frames[..1],
            &mut registry,
            &test_handlers(),
            &bus,
        );
        assert_eq!(book_subscribed_acks(&bus), vec![reply]);
        assert_eq!(book_row_subscribed_at(&mirror, &symbol), Some(at));
    }

    #[test]
    fn late_book_that_exhausts_its_gap_budget_reports_its_held_reply() {
        let (mut registry, mirror, symbol, reply) = late_book_holding_its_reply();
        let at = crate::types::MonotonicInstant(std::time::Duration::from_secs(7));
        let (bus, _) = fixed_clock_bus(at);
        let bad_snapshot = serde_json::json!({
            "channel": "book", "type": "snapshot", "data": [{
                "symbol": "PI_XBTUSD", "bids": [{"price": "100", "qty": "1"}],
                "asks": [{"price": "101", "qty": "1"}], "checksum": 1
            }]
        });
        let handlers = test_handlers();
        let acks = (0..10).find_map(|_| {
            route_all(
                WsUrl::UnifiedPublic,
                std::slice::from_ref(&bad_snapshot),
                &mut registry,
                &handlers,
                &bus,
            );
            Some(book_subscribed_acks(&bus)).filter(|acks| !acks.is_empty())
        });
        assert_eq!(acks, Some(vec![reply]));
        assert_eq!(book_row_subscribed_at(&mirror, &symbol), Some(at));
    }

    #[test]
    fn unified_instrument_reject_fixture_terminates_channel_wide_entry() {
        use crate::conn::subscription_registry::{
            EntrySubState, SubscribeParams, SubscriptionEntry,
        };

        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_instrument_with_symbol_reject.json"
        ))
        .expect("checked-in Unified Futures instrument rejection fixture is valid JSON");
        let req_id = fixture["req_id"].as_u64().expect("fixture req_id");
        let bus = test_bus();
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let mut registry = SubscriptionRegistry::with_mirror(Arc::clone(&mirror));
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Instrument,
                None,
                SubscribeParams::Instrument,
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        let mut mc = test_mc(WsUrl::UnifiedPublic, &bus, ConnectionState::Open);
        mc.record_subscribe_req_id(req_id, ChannelName::Instrument, None);
        mc.arm_subscribe_ack(
            ChannelName::Instrument,
            None,
            crate::types::MonotonicInstant(std::time::Duration::from_secs(60)),
        );
        let mut conns = HashMap::from([(WsUrl::UnifiedPublic, mc)]);

        assert!(route_sub_ack(
            &bus,
            WsUrl::UnifiedPublic,
            &mut conns,
            &mut registry,
            &fixture
        ));

        let mirror = mirror.read().expect("mirror");
        assert!(matches!(
            mirror[&(WsUrl::UnifiedPublic, ChannelName::Instrument, None)].state,
            EntrySubState::Terminated { .. }
        ));
        let events = bus.test_drain_published();
        assert!(events.iter().any(|event| matches!(
            event.payload,
            crate::dispatch::EventPayload::FuturesSubscribeFailedEvent {
                channel: ChannelName::Instrument,
                symbol: None,
                ref error_class,
                ..
            } if *error_class == crate::dispatch::FuturesSubscribeFailureClass::NonTransient
        )));
        assert!(events.iter().any(|event| matches!(
            event.payload,
            crate::dispatch::EventPayload::FuturesSubscriptionTerminatedEvent {
                channel: ChannelName::Instrument,
                symbol: None,
                cause: crate::api::subscription::TerminationCause::NonTransientWireRejection,
                ref last_error,
                ..
            } if last_error.as_deref() == Some("instrument channel does not accept symbols")
        )));
    }

    #[test]
    fn unified_instrument_success_fixture_acks_channel_wide_entry() {
        use crate::conn::subscription_registry::{
            EntrySubState, SubscribeParams, SubscriptionEntry,
        };

        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_instrument_success_ack.json"
        ))
        .expect("checked-in Unified Futures instrument success fixture is valid JSON");
        let req_id = fixture["req_id"].as_u64().expect("fixture req_id");
        let bus = test_bus();
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let mut registry = SubscriptionRegistry::with_mirror(Arc::clone(&mirror));
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Instrument,
                None,
                SubscribeParams::Instrument,
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        let mut mc = test_mc(WsUrl::UnifiedPublic, &bus, ConnectionState::Open);
        mc.record_subscribe_req_id(req_id, ChannelName::Instrument, None);
        mc.arm_subscribe_ack(
            ChannelName::Instrument,
            None,
            crate::types::MonotonicInstant(std::time::Duration::from_secs(60)),
        );
        let mut conns = HashMap::from([(WsUrl::UnifiedPublic, mc)]);

        assert!(route_sub_ack(
            &bus,
            WsUrl::UnifiedPublic,
            &mut conns,
            &mut registry,
            &fixture
        ));

        let mirror = mirror.read().expect("mirror");
        assert!(matches!(
            mirror[&(WsUrl::UnifiedPublic, ChannelName::Instrument, None)].state,
            EntrySubState::Acked
        ));
        assert!(
            !conns[&WsUrl::UnifiedPublic].is_subscribe_ack_armed(ChannelName::Instrument, None),
            "channel-wide subscribe-ack timer must be disarmed"
        );
        let events = bus.test_drain_published();
        assert!(events.iter().any(|event| matches!(
            event.payload,
            crate::dispatch::EventPayload::FuturesSubscribedEvent {
                channel: ChannelName::Instrument,
                symbol: None,
                ..
            }
        )));
    }

    fn assert_ticker_reject_terminates_symbol_entry(fixture: &str, expected_error: &str) {
        use crate::conn::subscription_registry::{
            EntrySubState, SubscribeParams, SubscriptionEntry,
        };

        let fixture: Value = serde_json::from_str(fixture)
            .expect("checked-in Unified Futures ticker rejection fixture is valid JSON");
        let req_id = fixture["req_id"].as_u64().expect("fixture req_id");
        let symbol = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let bus = test_bus();
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let mut registry = SubscriptionRegistry::with_mirror(Arc::clone(&mirror));
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(symbol.clone()),
                SubscribeParams::Ticker {
                    snapshot: None,
                    event_trigger: None,
                },
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        let mut mc = test_mc(WsUrl::UnifiedPublic, &bus, ConnectionState::Open);
        mc.record_subscribe_req_id(req_id, ChannelName::Ticker, Some(symbol.clone()));
        mc.arm_subscribe_ack(
            ChannelName::Ticker,
            Some(symbol.clone()),
            crate::types::MonotonicInstant(std::time::Duration::from_secs(60)),
        );
        let mut conns = HashMap::from([(WsUrl::UnifiedPublic, mc)]);

        assert!(route_sub_ack(
            &bus,
            WsUrl::UnifiedPublic,
            &mut conns,
            &mut registry,
            &fixture
        ));

        let mirror = mirror.read().expect("mirror");
        assert!(matches!(
            mirror[&(WsUrl::UnifiedPublic, ChannelName::Ticker, Some(symbol))].state,
            EntrySubState::Terminated { .. }
        ));
        let events = bus.test_drain_published();
        assert!(events.iter().any(|event| matches!(
            event.payload,
            crate::dispatch::EventPayload::FuturesSubscribeFailedEvent {
                channel: ChannelName::Ticker,
                symbol: Some(ref symbol),
                ref error_class,
                ..
            } if symbol == "PI_XBTUSD"
                && *error_class == crate::dispatch::FuturesSubscribeFailureClass::NonTransient
        )));
        assert!(events.iter().any(|event| matches!(
            event.payload,
            crate::dispatch::EventPayload::FuturesSubscriptionTerminatedEvent {
                channel: ChannelName::Ticker,
                symbol: Some(ref symbol),
                cause: crate::api::subscription::TerminationCause::NonTransientWireRejection,
                ref last_error,
                ..
            } if symbol == "PI_XBTUSD"
                && last_error.as_deref() == Some(expected_error)
        )));
    }

    #[test]
    fn unified_ticker_event_trigger_reject_fixture_terminates_symbol_entry() {
        assert_ticker_reject_terminates_symbol_entry(
            include_str!(
                "../../../tests/fixtures/unified_ws/unified_ticker_event_trigger_reject.json"
            ),
            "Unknown 'params' key: event_trigger",
        );
    }

    #[test]
    fn public_ticker_missing_asset_class_reject_fixture_terminates_symbol_entry() {
        assert_ticker_reject_terminates_symbol_entry(
            include_str!(
                "../../../tests/fixtures/unified_ws/public_ticker_missing_asset_class_reject.json"
            ),
            "Missing 'params.asset_class'",
        );
    }

    #[test]
    fn unified_transient_subscribe_failures_increment_attempt_per_retry() {
        assert_transient_attempts_increment(ConnectionState::Open);
    }

    #[test]
    fn unified_transient_reject_in_connecting_counts_the_attempt() {
        assert_transient_attempts_increment(ConnectionState::Connecting);
    }

    fn assert_transient_attempts_increment(state: ConnectionState) {
        use crate::conn::subscription_registry::{SubscribeParams, SubscriptionEntry};

        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let symbol = StreamSymbol::Futures("PI_XBTUSD".to_string());
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(symbol.clone()),
                SubscribeParams::Ticker {
                    snapshot: None,
                    event_trigger: None,
                },
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        let mc = test_mc(WsUrl::UnifiedPublic, &bus, state);
        let mut conns = HashMap::from([(WsUrl::UnifiedPublic, mc)]);
        let failure = serde_json::json!({
            "method": "subscribe", "success": false, "req_id": 88,
            "error": "EAPI:Rate limit exceeded",
            "result": { "channel": "ticker", "symbol": "PI_XBTUSD" }
        });

        for _ in 0..2 {
            let mc = conns
                .get_mut(&WsUrl::UnifiedPublic)
                .expect("managed connection");
            mc.record_subscribe_req_id(88, ChannelName::Ticker, Some(symbol.clone()));
            mc.arm_subscribe_ack(
                ChannelName::Ticker,
                Some(symbol.clone()),
                crate::types::MonotonicInstant(std::time::Duration::from_secs(60)),
            );
            assert!(route_sub_ack(
                &bus,
                WsUrl::UnifiedPublic,
                &mut conns,
                &mut registry,
                &failure
            ));
        }

        let failures: Vec<(u32, Option<String>, Option<u64>)> = bus
            .test_drain_published()
            .into_iter()
            .filter_map(|event| match event.payload {
                crate::dispatch::EventPayload::FuturesSubscribeFailedEvent {
                    symbol: Some(symbol),
                    attempt,
                    error,
                    ack,
                    ..
                } if symbol == "PI_XBTUSD" => Some((attempt, error, ack.req_id)),
                _ => None,
            })
            .collect();
        let error = Some("EAPI:Rate limit exceeded".to_owned());
        assert_eq!(
            failures,
            vec![(1, error.clone(), Some(88)), (2, error, Some(88))]
        );
        let mc = &conns[&WsUrl::UnifiedPublic];
        assert!(!mc.is_subscribe_ack_armed(ChannelName::Ticker, Some(symbol.clone())));
        assert!(mc.test_has_subscribe_resend_timer(ChannelName::Ticker, Some(symbol)));
    }

    #[test]
    fn unified_batched_channelless_rejection_terminates_every_entry() {
        use crate::conn::subscription_registry::{
            EntrySubState, SubscribeParams, SubscriptionEntry,
        };

        let bus = test_bus();
        let mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let mut registry = SubscriptionRegistry::with_mirror(Arc::clone(&mirror));
        let now = crate::types::MonotonicInstant::now();
        let symbols = [
            StreamSymbol::Futures("PI_XBTUSD".to_string()),
            StreamSymbol::Futures("PI_NOTAREAL".to_string()),
        ];
        for symbol in symbols.clone() {
            registry.register(
                SubscriptionEntry::new(
                    WsUrl::UnifiedPublic,
                    ChannelName::Ticker,
                    Some(symbol),
                    SubscribeParams::Ticker {
                        snapshot: None,
                        event_trigger: None,
                    },
                ),
                now,
                false,
            );
        }

        let mut mc = test_mc(WsUrl::UnifiedPublic, &bus, ConnectionState::Open);
        for symbol in symbols.clone() {
            mc.record_subscribe_req_id(78, ChannelName::Ticker, Some(symbol.clone()));
            mc.arm_subscribe_ack(
                ChannelName::Ticker,
                Some(symbol),
                crate::types::MonotonicInstant(std::time::Duration::from_secs(60)),
            );
        }
        let mut conns = HashMap::from([(WsUrl::UnifiedPublic, mc)]);
        let reject = serde_json::json!({
            "method": "subscribe",
            "success": false,
            "req_id": 78,
            "error": "Subscription request rejected",
        });

        assert!(route_sub_ack(
            &bus,
            WsUrl::UnifiedPublic,
            &mut conns,
            &mut registry,
            &reject
        ));

        let failures: Vec<_> = bus
            .test_drain_published()
            .into_iter()
            .filter_map(|event| match event.payload {
                crate::dispatch::EventPayload::FuturesSubscribeFailedEvent {
                    channel,
                    symbol: Some(symbol),
                    error_class,
                    ..
                } => Some((channel, symbol, error_class)),
                _ => None,
            })
            .collect();
        assert_eq!(
            failures,
            vec![
                (
                    ChannelName::Ticker,
                    "PI_XBTUSD".to_string(),
                    crate::dispatch::FuturesSubscribeFailureClass::NonTransient,
                ),
                (
                    ChannelName::Ticker,
                    "PI_NOTAREAL".to_string(),
                    crate::dispatch::FuturesSubscribeFailureClass::NonTransient,
                ),
            ]
        );

        let mirror = mirror.read().expect("mirror");
        for symbol in symbols {
            assert!(matches!(
                mirror[&(WsUrl::UnifiedPublic, ChannelName::Ticker, Some(symbol))].state,
                EntrySubState::Terminated { .. }
            ));
        }
        assert!(
            conns
                .get(&WsUrl::UnifiedPublic)
                .expect("managed connection")
                .resolve_subscribe_req_id(78)
                .is_none()
        );
    }

    #[test]
    fn unified_batched_channelless_transient_rejection_emits_every_failure() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let symbols = [
            StreamSymbol::Futures("PI_XBTUSD".to_string()),
            StreamSymbol::Futures("PF_ETHUSD".to_string()),
        ];
        let mut mc = test_mc(WsUrl::UnifiedPublic, &bus, ConnectionState::Open);
        for symbol in symbols.clone() {
            mc.record_subscribe_req_id(79, ChannelName::Ticker, Some(symbol.clone()));
            mc.arm_subscribe_ack(
                ChannelName::Ticker,
                Some(symbol),
                crate::types::MonotonicInstant(std::time::Duration::from_secs(60)),
            );
        }
        let mut conns = HashMap::from([(WsUrl::UnifiedPublic, mc)]);
        let reject = serde_json::json!({
            "method": "subscribe",
            "success": false,
            "req_id": 79,
            "error": "EAPI:Rate limit exceeded",
        });

        assert!(route_sub_ack(
            &bus,
            WsUrl::UnifiedPublic,
            &mut conns,
            &mut registry,
            &reject
        ));

        let failures: Vec<_> = bus
            .test_drain_published()
            .into_iter()
            .filter_map(|event| match event.payload {
                crate::dispatch::EventPayload::FuturesSubscribeFailedEvent {
                    channel,
                    symbol: Some(symbol),
                    error_class,
                    ..
                } => Some((channel, symbol, error_class)),
                _ => None,
            })
            .collect();
        assert_eq!(
            failures,
            vec![
                (
                    ChannelName::Ticker,
                    "PI_XBTUSD".to_string(),
                    crate::dispatch::FuturesSubscribeFailureClass::Transient,
                ),
                (
                    ChannelName::Ticker,
                    "PF_ETHUSD".to_string(),
                    crate::dispatch::FuturesSubscribeFailureClass::Transient,
                ),
            ]
        );
    }

    fn route_seq_frames(
        url: WsUrl,
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
                url,
                frame,
                registry,
                &handlers,
                &bus_back,
                &mut drop_tracker,
                &mut malformed_warn_tracker,
            );
        }
    }

    #[test]
    fn executions_sequence_jump_emits_channel_gap_event() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        registry.register(
            crate::conn::subscription_registry::SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Executions,
                None,
                crate::conn::subscription_registry::SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
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
            WsUrl::Auth,
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

        route_seq_frames(WsUrl::Auth, &[("u", mk("update", 7))], &mut registry, &bus);
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
                    url: WsUrl::Auth,
                    channel: ChannelName::Executions,
                    dropped_count: 3,
                    cause: crate::dispatch::GapCause::SequenceGapDetected,
                }
            ),
            "payload mismatch: {:?}",
            gap.payload
        );
    }

    fn register_balances(registry: &mut SubscriptionRegistry, url: WsUrl) {
        registry.register(
            crate::conn::subscription_registry::SubscriptionEntry::new(
                url,
                ChannelName::Balances,
                None,
                crate::conn::subscription_registry::SubscribeParams::Balances { snapshot: None },
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
    }

    fn route_balances(
        url: WsUrl,
        ty: &str,
        seq: u64,
        registry: &mut SubscriptionRegistry,
        bus: &Arc<DispatchEventBus>,
    ) {
        let frame = serde_json::json!({
            "channel": "balances", "type": ty, "sequence": seq,
            "data": [ { "asset": "USD", "balance": 1.0 } ],
        });
        route_seq_frames(url, &[("", frame)], registry, bus);
    }

    fn channel_gaps(bus: &Arc<DispatchEventBus>) -> Vec<(WsUrl, ChannelName, u32)> {
        bus.test_drain_published()
            .into_iter()
            .filter_map(|e| match e.payload {
                crate::dispatch::EventPayload::ChannelGapEvent {
                    url,
                    channel,
                    dropped_count,
                    ..
                } => Some((url, channel, dropped_count)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn unified_private_executions_sequence_never_advances_a_tracker() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        registry.register(
            crate::conn::subscription_registry::SubscriptionEntry::new(
                WsUrl::UnifiedPrivate,
                ChannelName::Executions,
                None,
                crate::conn::subscription_registry::SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        let frame = |ty: &str, seq: u64| {
            serde_json::json!({
                "channel": "executions", "type": ty, "sequence": seq,
                "data": [ { "exec_type": "new" } ],
            })
        };

        route_seq_frames(
            WsUrl::UnifiedPrivate,
            &[("s", frame("snapshot", 1)), ("u", frame("update", 9))],
            &mut registry,
            &bus,
        );

        assert!(
            channel_gaps(&bus).is_empty(),
            "a jump on an untracked channel is not a gap"
        );
    }

    #[test]
    fn malformed_frame_warns_are_throttled_per_connection() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        let handlers = test_handlers();
        let mut drop_tracker = HashMap::new();
        let mut malformed_warn_tracker = HashMap::new();
        let frame = serde_json::json!({
            "channel": "balances", "type": "update",
            "data": { "asset": "USD" },
        });

        let logs = capture_logs(|| {
            for url in [
                WsUrl::Auth,
                WsUrl::UnifiedPrivate,
                WsUrl::Auth,
                WsUrl::UnifiedPrivate,
            ] {
                route_text_frame(
                    url,
                    &frame,
                    &mut registry,
                    &handlers,
                    &Arc::downgrade(&bus),
                    &mut drop_tracker,
                    &mut malformed_warn_tracker,
                );
            }
        });

        assert_eq!(
            logs.matches("non-array `data`").count(),
            2,
            "one warn per connection per window, not one shared across Spot and Unified: {logs:?}"
        );
    }

    #[test]
    fn unified_private_balances_gap_names_its_connection() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        register_balances(&mut registry, WsUrl::UnifiedPrivate);

        route_balances(WsUrl::UnifiedPrivate, "snapshot", 1, &mut registry, &bus);
        route_balances(WsUrl::UnifiedPrivate, "update", 2, &mut registry, &bus);
        assert!(
            channel_gaps(&bus).is_empty(),
            "contiguous frames must not gap"
        );

        route_balances(WsUrl::UnifiedPrivate, "update", 5, &mut registry, &bus);
        assert_eq!(
            channel_gaps(&bus),
            vec![(WsUrl::UnifiedPrivate, ChannelName::Balances, 2)]
        );
    }

    #[test]
    fn spot_auth_and_unified_private_balances_track_sequences_independently() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        register_balances(&mut registry, WsUrl::Auth);
        register_balances(&mut registry, WsUrl::UnifiedPrivate);

        route_balances(WsUrl::Auth, "snapshot", 100, &mut registry, &bus);
        route_balances(WsUrl::UnifiedPrivate, "snapshot", 1, &mut registry, &bus);
        route_balances(WsUrl::Auth, "update", 101, &mut registry, &bus);
        route_balances(WsUrl::UnifiedPrivate, "update", 2, &mut registry, &bus);
        assert!(
            channel_gaps(&bus).is_empty(),
            "interleaved contiguous streams on two connections must not gap"
        );

        route_balances(WsUrl::Auth, "update", 104, &mut registry, &bus);
        assert_eq!(
            channel_gaps(&bus),
            vec![(WsUrl::Auth, ChannelName::Balances, 2)]
        );
    }

    #[test]
    fn malformed_or_sequenceless_frames_do_not_false_gap() {
        let bus = test_bus();
        let mut registry = SubscriptionRegistry::new();
        registry.register(
            crate::conn::subscription_registry::SubscriptionEntry::new(
                WsUrl::Auth,
                ChannelName::Balances,
                None,
                crate::conn::subscription_registry::SubscribeParams::Balances { snapshot: None },
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );

        route_seq_frames(
            WsUrl::Auth,
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
            suppress_warn_throttled(
                WsUrl::Auth,
                "t",
                Some(ChannelName::Ticker),
                &mut tracker,
                &weak,
                warn,
            );
            suppress_warn_throttled(
                WsUrl::Auth,
                "t",
                Some(ChannelName::Ticker),
                &mut tracker,
                &weak,
                warn,
            );
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
            suppress_warn_throttled(
                WsUrl::Auth,
                "t",
                Some(ChannelName::Ticker),
                &mut tracker,
                &weak,
                warn,
            );
        });
        assert_eq!(
            logs2.matches("no routing symbol").count(),
            1,
            "after the window the warn must re-fire; got: {logs2:?}"
        );

        let logs3 = capture_logs(|| {
            suppress_warn_throttled(
                WsUrl::Auth,
                "t",
                Some(ChannelName::Trade),
                &mut tracker,
                &weak,
                warn,
            );
        });
        assert_eq!(
            logs3.matches("no routing symbol").count(),
            1,
            "a different channel warns independently; got: {logs3:?}"
        );

        // Same channel, different site: channel-only keying would suppress this call.
        let logs4 = capture_logs(|| {
            suppress_warn_throttled(
                WsUrl::Auth,
                "u",
                Some(ChannelName::Ticker),
                &mut tracker,
                &weak,
                warn,
            );
            suppress_warn_throttled(
                WsUrl::Auth,
                "u",
                Some(ChannelName::Ticker),
                &mut tracker,
                &weak,
                warn,
            );
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
        let order_gate = Arc::new(crate::conn::OrderGate::default());
        let bus_back = Arc::downgrade(&bus);
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        let mut mc = test_mc(WsUrl::Public, &bus, ConnectionState::Resubscribing);
        let pair = StreamSymbol::from(Symbol::new("BTC/USD").expect("valid pair"));
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
            test_handles(&auth_stack, &order_gate, &bus_back, &state_mirrors),
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
        let order_gate = Arc::new(crate::conn::OrderGate::default());
        let bus_back = Arc::downgrade(&bus);
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();

        let mut mc = test_mc(WsUrl::Public, &bus, ConnectionState::Resubscribing);
        let pair = StreamSymbol::from(Symbol::new("BTC/USD").expect("valid pair"));
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
                test_handles(&auth_stack, &order_gate, &bus_back, &state_mirrors),
                &mut HashMap::new(),
            );
        });
        assert!(
            logs.contains("subscribe-ack frame with absent/non-bool `success`"),
            "absent-`success` subscribe ack must warn; captured logs: {logs:?}"
        );
    }

    fn ack_a_spot_ticker_at(
        fixed: crate::types::MonotonicInstant,
        replay_marked: bool,
    ) -> (
        crate::conn::subscription_registry::SubscriptionMirror,
        HashMap<WsUrl, ManagedConnection>,
        Option<StreamSymbol>,
    ) {
        use crate::conn::subscription_registry::{
            SubscribeParams, SubscriptionEntry, SubscriptionMirror,
        };

        let (bus, clock) = fixed_clock_bus(fixed);
        let mirror = SubscriptionMirror::default();
        let mut registry = SubscriptionRegistry::with_mirror(Arc::clone(&mirror));
        let pair = Some(StreamSymbol::from(
            Symbol::new("BTC/USD").expect("valid pair"),
        ));
        registry.register(
            SubscriptionEntry::new(
                WsUrl::Public,
                ChannelName::Ticker,
                pair.clone(),
                SubscribeParams::Ticker {
                    snapshot: Some(true),
                    event_trigger: None,
                },
            ),
            fixed,
            false,
        );
        let mut mc = test_mc_with_clock(
            WsUrl::Public,
            &bus,
            ConnectionState::Resubscribing,
            Arc::clone(&clock),
        );
        mc.arm_subscribe_ack(
            ChannelName::Ticker,
            pair.clone(),
            crate::types::MonotonicInstant(std::time::Duration::from_secs(60)),
        );
        if replay_marked {
            mc.mark_initial_subscribe_replay(ChannelName::Ticker, std::slice::from_ref(&pair));
        }
        let mut conns = HashMap::new();
        conns.insert(WsUrl::Public, mc);

        let consumed = route_sub_ack(
            &bus,
            WsUrl::Public,
            &mut conns,
            &mut registry,
            &serde_json::json!({
                "method": "subscribe",
                "channel": "ticker",
                "symbol": "BTC/USD",
                "success": true,
            }),
        );
        assert!(consumed);
        (mirror, conns, pair)
    }

    #[test]
    fn a_venue_ack_stamps_subscribed_at_from_the_injected_clock() {
        let fixed = crate::types::MonotonicInstant(std::time::Duration::from_secs(7));
        let (mirror, _conns, pair) = ack_a_spot_ticker_at(fixed, false);

        assert_eq!(
            mirror.read().expect("mirror")[&(WsUrl::Public, ChannelName::Ticker, pair)]
                .subscribed_at,
            Some(fixed),
            "the ack is stamped from the connection clock, not the wall monotonic clock"
        );
    }

    #[test]
    fn a_venue_ack_clears_the_replay_mark_of_a_tokenless_connection() {
        let fixed = crate::types::MonotonicInstant(std::time::Duration::from_secs(7));
        let (_mirror, conns, _pair) = ack_a_spot_ticker_at(fixed, true);

        assert!(
            !conns[&WsUrl::Public].needs_initial_subscribe_replay(),
            "an acked key must not be replayed again"
        );
    }

    #[test]
    fn request_response_missing_success_warns() {
        let bus = test_bus();
        let registry = SubscriptionRegistry::new();
        let auth_stack = test_auth_stack(&bus);
        let order_gate = Arc::new(crate::conn::OrderGate::default());
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
                test_handles(&auth_stack, &order_gate, &Weak::new(), &state_mirrors),
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
    fn a_request_reply_without_success_resolves_the_pending_request_as_failed() {
        let bus = test_bus();
        let registry = SubscriptionRegistry::new();
        let auth_stack = test_auth_stack(&bus);
        let order_gate = Arc::new(crate::conn::OrderGate::default());
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        let mut mc = test_mc(WsUrl::Auth, &bus, ConnectionState::Open);
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        mc.record_pending_request(crate::conn::managed_connection::PendingRequest {
            req_id: 7777,
            op: crate::dispatch::WsOp::AddOrder,
            sent_at: crate::types::MonotonicInstant(std::time::Duration::from_secs(0)),
            tx,
        });
        let mut conns = HashMap::new();
        conns.insert(WsUrl::Auth, mc);
        let frame = serde_json::json!({
            "method": "add_order",
            "req_id": 7777u64,
            "error": "EGeneral:Internal error",
        });
        assert!(handle_request_response(
            WsUrl::Auth,
            &frame,
            &mut conns,
            &registry,
            &HashMap::new(),
            test_handles(&auth_stack, &order_gate, &Weak::new(), &state_mirrors),
            &mut HashMap::new(),
        ));
        let resp = rx
            .try_recv()
            .expect("the pending request resolved")
            .expect("a reply, not a connection error");
        assert!(!resp.success, "an absent success is never read as a fill");
        assert_eq!(resp.error.as_deref(), Some("EGeneral:Internal error"));
    }

    #[test]
    fn order_ack_novel_non_e_string_warns_on_default() {
        let logs = capture_logs(|| {
            let out =
                classify_order_ack_warned(WsUrl::Auth, false, Some("totally novel rejection text"));
            assert!(matches!(out, OrderAckAuth::Authenticated));
        });
        assert!(
            logs.contains("not recognized as auth-class"),
            "novel non-`E*` order-ack error must warn on the inverted default; captured logs: {logs:?}"
        );

        let logs = capture_logs(|| {
            let out =
                classify_order_ack_warned(WsUrl::Auth, false, Some("EOrder:Insufficient funds"));
            assert!(matches!(out, OrderAckAuth::Authenticated));
        });
        assert!(
            !logs.contains("not recognized as auth-class"),
            "an `E*`-prefixed order error must NOT warn (no-flood gate); captured logs: {logs:?}"
        );
    }

    #[test]
    fn unified_session_rejects_are_token_stale_on_unified_urls_only() {
        // (error, Unified private, Spot Auth)
        let cases = [
            ("Invalid session", Some(AuthErrorKind::TokenStale), None),
            (
                "ESession:Invalid session token",
                Some(AuthErrorKind::TokenStale),
                Some(AuthErrorKind::TokenStale),
            ),
            (
                "Missing or invalid 'params.token'",
                Some(AuthErrorKind::TokenStale),
                Some(AuthErrorKind::TokenStale),
            ),
            ("Token(s) not found", Some(AuthErrorKind::TokenStale), None),
            (
                "Token field must be a string",
                Some(AuthErrorKind::TokenStale),
                None,
            ),
            ("Other:EDIT_HAS_NO_EFFECT", None, None),
            ("Invalid arguments:price", None, None),
            ("Rate limit exceeded", None, None),
        ];
        for (err, unified, spot) in cases {
            assert_eq!(
                classify_auth_class(WsUrl::UnifiedPrivate, err),
                unified,
                "Unified private: {err}"
            );
            assert_eq!(
                classify_auth_class(WsUrl::Auth, err),
                spot,
                "Spot Auth: {err}"
            );
        }
    }

    #[test]
    fn unified_session_reject_on_an_order_ack_fails_auth_instead_of_authenticating() {
        for err in [
            "Invalid session",
            "ESession:Invalid session token",
            "Missing or invalid 'params.token'",
        ] {
            assert!(
                matches!(
                    classify_order_ack_auth(WsUrl::UnifiedPrivate, false, Some(err)),
                    OrderAckAuth::AuthFailed(AuthErrorKind::TokenStale)
                ),
                "{err}"
            );
        }
        assert!(matches!(
            classify_order_ack_auth(
                WsUrl::UnifiedPrivate,
                false,
                Some("Other:POST_WOULD_EXECUTE")
            ),
            OrderAckAuth::Authenticated
        ));
    }

    #[test]
    fn unified_order_rejects_do_not_trip_the_novel_auth_warning() {
        for (url, err, warns) in [
            (WsUrl::UnifiedPrivate, "Other:EDIT_HAS_NO_EFFECT", false),
            (WsUrl::UnifiedPrivate, "Invalid arguments:price", false),
            (WsUrl::UnifiedPrivate, "Rate limit exceeded", false),
            (
                WsUrl::UnifiedPrivate,
                "Session handshake went sideways",
                true,
            ),
            (WsUrl::UnifiedPrivate, "token rejected by gateway", true),
            (WsUrl::Auth, "Other:EDIT_HAS_NO_EFFECT", true),
        ] {
            let logs = capture_logs(|| {
                let _ = classify_order_ack_warned(url, false, Some(err));
            });
            assert_eq!(
                logs.contains("not recognized as auth-class"),
                warns,
                "{url:?} {err}; captured logs: {logs:?}"
            );
        }
    }

    #[test]
    fn order_ack_no_error_string_warns_on_default() {
        let logs = capture_logs(|| {
            let out = classify_order_ack_warned(WsUrl::Auth, false, None);
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
                crate::conn::SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None,
                },
            ),
            crate::types::MonotonicInstant::now(),
            false,
        );
        let auth_stack = test_auth_stack(&bus);
        let order_gate = Arc::new(crate::conn::OrderGate::default());
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
            test_handles(&auth_stack, &order_gate, &Weak::new(), &state_mirrors),
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
                crate::conn::SubscribeParams::Executions {
                    snap_orders: None,
                    snap_trades: None
                }
            )],
            "token-miss replay defers exactly the composed batch"
        );
    }

    /// Feeds one order reply to `handle_request_response` on a connection in `state` holding it as the only pending request.
    async fn route_order_reply(
        state: ConnectionState,
        frame: serde_json::Value,
    ) -> (
        ManagedConnection,
        Result<crate::conn::managed_connection::WsResponse, crate::error::ConnectionError>,
        Vec<crate::dispatch::EventEnvelope>,
    ) {
        let bus = test_bus();
        let registry = SubscriptionRegistry::new();
        let auth_stack = test_auth_stack(&bus);
        let order_gate = Arc::new(crate::conn::OrderGate::default());
        let state_mirrors: HashMap<WsUrl, Arc<AtomicU8>> = HashMap::new();
        let mut mc = test_mc(WsUrl::Auth, &bus, state);
        let (tx, rx) = tokio::sync::oneshot::channel();
        mc.record_pending_request(crate::conn::managed_connection::PendingRequest {
            req_id: 91,
            op: WsOp::AddOrder,
            sent_at: crate::types::MonotonicInstant::now(),
            tx,
        });
        let mut conns = HashMap::new();
        conns.insert(WsUrl::Auth, mc);
        let consumed = handle_request_response(
            WsUrl::Auth,
            &frame,
            &mut conns,
            &registry,
            &HashMap::new(),
            test_handles(&auth_stack, &order_gate, &Weak::new(), &state_mirrors),
            &mut HashMap::new(),
        );
        assert!(consumed, "the order reply is consumed");
        let mc = conns.remove(&WsUrl::Auth).expect("auth conn");
        let delivered = rx.await.expect("the sender is resolved, never dropped");
        (mc, delivered, bus.test_drain_published())
    }

    fn order_reject(error: &str) -> serde_json::Value {
        serde_json::json!({
            "method": "add_order", "req_id": 91u64, "success": false, "error": error,
        })
    }

    #[tokio::test]
    async fn a_throttle_reply_delivers_its_retry_after_ms() {
        let mut frame = order_reject("Rate limit exceeded");
        frame["retry_after_ms"] = serde_json::json!(588);
        let (_mc, delivered, _events) = route_order_reply(ConnectionState::Open, frame).await;
        let resp = delivered.expect("the throttle reply reaches the caller");
        assert_eq!(
            (resp.error.as_deref(), resp.retry_after_ms),
            (Some("Rate limit exceeded"), Some(588))
        );
    }

    #[tokio::test]
    async fn an_auth_class_reply_to_the_probe_order_is_delivered_not_drained() {
        for error in ["EAPI:Invalid key", "EGeneral:Permission denied"] {
            let (_mc, delivered, events) =
                route_order_reply(ConnectionState::Authenticating, order_reject(error)).await;
            let resp = delivered.expect("the venue's refusal reaches the caller");
            assert_eq!(resp.error.as_deref(), Some(error), "{error}");
            assert!(
                !events.iter().any(|e| matches!(
                    e.payload,
                    crate::dispatch::EventPayload::WsRequestFailedEvent { .. }
                )),
                "{error}: an answered order is not a failed request"
            );
        }
    }

    #[tokio::test]
    async fn a_token_stale_order_reply_refreshes_the_token_in_every_live_state() {
        // (state, venue error, a refresh is started)
        let cases = [
            (ConnectionState::Open, "EAPI:Invalid token", true),
            (ConnectionState::Resubscribing, "EAPI:Invalid token", true),
            (
                ConnectionState::Resubscribing,
                "EOrder:Insufficient funds",
                false,
            ),
            (ConnectionState::Closing, "EAPI:Invalid token", false),
        ];
        for (state, error, refresh) in cases {
            let (mc, delivered, _events) = route_order_reply(state, order_reject(error)).await;
            let resp = delivered.expect("the reply reaches the caller in every state");
            assert_eq!(resp.error.as_deref(), Some(error), "{state:?} {error}");
            assert_eq!(
                (
                    mc.pending_reactive_token_refresh_id().is_some(),
                    mc.awaiting_token_refresh()
                ),
                (refresh, refresh),
                "{state:?} {error}"
            );
        }
    }

    #[test]
    fn order_ack_probe_to_open_warns_exactly_once() {
        let bus = test_bus();
        let registry = SubscriptionRegistry::new();
        let auth_stack = test_auth_stack(&bus);
        let order_gate = Arc::new(crate::conn::OrderGate::default());
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
                test_handles(&auth_stack, &order_gate, &Weak::new(), &state_mirrors),
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
        let order_gate = Arc::new(crate::conn::OrderGate::default());
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
            test_handles(&auth_stack, &order_gate, &Weak::new(), &state_mirrors),
            &mut HashMap::new(),
        );
        assert!(
            consumed,
            "an `add_order` reply with a recorded req_id is consumed"
        );

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
