//! Routes `CallerInbound` variants and composes/sends WS request frames
//! (orders) on the auth connection.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use serde_json::Value;
use tokio::sync::mpsc;

use crate::api::ws_surface::SubscriberGuard;
use crate::conn::ManagedConnection;
use crate::conn::subscription_registry::{RemovedEntryKind, SubscriptionRegistry};
use crate::dispatch::event_bus::{
    CallerInbound, DispatchEventBus, HandlerMutationOp, RegistryMutationOp,
};
use crate::dispatch::handler_registry::HandlerRegistry;
use crate::transport::{WsFrame, WsOpcode, WsSocket};
use crate::types::{ConnectionState, WsUrl};

use super::UpgradeOutcome;
use super::send_ready::refresh_send_ready;
use super::socket_lifecycle::{
    channel_ws_url, project_socket_and_wire_bridge, unsubscribe_and_terminate, write_mirror,
};
use super::subscribe_send::register_one_and_emit;

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_inbound(
    msg: CallerInbound,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    handler_registry: &mut HandlerRegistry,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    state_mirrors: &HashMap<WsUrl, Arc<std::sync::atomic::AtomicU8>>,
    bus_back_ref: &Weak<DispatchEventBus>,
    upgrade_tx: &mpsc::Sender<UpgradeOutcome>,
    connect_id_allocator: &Arc<AtomicU64>,
    auth_stack: &Arc<crate::auth::AuthStack>,
    auth_send_ready: &Arc<std::sync::atomic::AtomicBool>,
    upgrade_guards: &mut HashMap<WsUrl, SubscriberGuard>,
) {
    match msg {
        CallerInbound::FsmEvent { url, event } => {
            // FSM is sole writer of state; the mirror is a strict observable.
            let fsm_event = match event {
                crate::types::CallerEvent::StartConnect { request_id } => {
                    crate::conn::managed_connection::FsmEvent::CallStartConnect { request_id }
                }
                crate::types::CallerEvent::Close { request_id } => {
                    crate::conn::managed_connection::FsmEvent::CallClose { request_id }
                }
                crate::types::CallerEvent::ForceReconnect { request_id } => {
                    crate::conn::managed_connection::FsmEvent::CallForceReconnect { request_id }
                }
            };
            if let Some(mc) = conns.get_mut(&url) {
                let prev_cid = mc.socket().map(|s| s.connection_id());
                mc.handle_event(fsm_event);
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
            } else {
                tracing::error!(
                    target: "kraken_sdk::io_reactor",
                    ?url,
                    "FsmEvent dispatched but no ManagedConnection present (invariant violation)"
                );
            }
        }
        CallerInbound::ClientClose { .. } => {
            debug_assert!(
                false,
                "CallerInbound::ClientClose must be handled in io_reactor::run, not handle_inbound"
            );
            tracing::error!(
                target: "kraken_sdk::io_reactor",
                "ClientClose reached handle_inbound (routing bug — handled in run())"
            );
        }
        CallerInbound::RegistryMutation { url, mutation } => {
            match mutation {
                RegistryMutationOp::RegisterBatch { entries, ref_id } => {
                    for entry in entries {
                        register_one_and_emit(
                            url,
                            entry,
                            ref_id,
                            conns,
                            registry,
                            sockets,
                            state_mirrors,
                            bus_back_ref,
                            upgrade_tx,
                            connect_id_allocator,
                            auth_stack,
                            auth_send_ready,
                            upgrade_guards,
                        );
                        // On a mid-batch teardown drop the stale socket so the rest
                        // still register (replayed on reconnect).
                        if conns.get(&url).is_some_and(|mc| mc.socket().is_none()) {
                            sockets.remove(&url);
                            upgrade_guards.remove(&url);
                        }
                    }
                }
                RegistryMutationOp::Deregister { channel, pair } => {
                    let _ = url; // url derived from channel inside the helper.
                    deregister_one(
                        channel,
                        pair,
                        conns,
                        registry,
                        sockets,
                        state_mirrors,
                        bus_back_ref,
                        auth_stack,
                    );
                }
                RegistryMutationOp::DeregisterBatch { channel, pairs } => {
                    let _ = url; // url derived from channel inside the helper.
                    for pair in pairs {
                        deregister_one(
                            channel,
                            Some(pair),
                            conns,
                            registry,
                            sockets,
                            state_mirrors,
                            bus_back_ref,
                            auth_stack,
                        );
                    }
                }
                RegistryMutationOp::DeregisterAll { channel } => {
                    let _ = url; // url derived per entry from its channel.
                    let removed = registry.deregister_all(channel.map(|c| c.wire_channel()));
                    let mut any_public = false;
                    let mut any_auth = false;
                    for (entry, kind) in removed {
                        let entry_url = channel_ws_url(entry.channel);
                        any_auth |= entry_url == WsUrl::Auth;
                        any_public |= entry_url == WsUrl::Public;
                        teardown_released_key(
                            entry.channel,
                            entry.pair,
                            Some(entry.params),
                            kind == RemovedEntryKind::Tombstone,
                            conns,
                            sockets,
                            bus_back_ref,
                            auth_stack,
                        );
                    }
                    // Emptying the pending-ack set mid-Resubscribing leaves no ack
                    // to drive → Open; short-circuit per touched URL.
                    for (touched, u) in [(any_public, WsUrl::Public), (any_auth, WsUrl::Auth)] {
                        if touched {
                            if let Some(mc) = conns.get_mut(&u) {
                                mc.complete_resubscribe_if_empty();
                                write_mirror(state_mirrors, u, mc.state());
                            }
                        }
                    }
                }
            }
        }
        CallerInbound::HandlerMutation { channel, op } => match op {
            HandlerMutationOp::Register { id, callback } => {
                handler_registry.register_with_id(channel, id, callback);
            }
            HandlerMutationOp::Deregister { id } => {
                handler_registry.deregister(channel, id);
            }
        },
        CallerInbound::SubscriptionGuardDrop {
            handler_id,
            channel,
            symbols,
        } => {
            // One post tears down the whole guard; the wire unsubscribe fires only
            // on the 0 transition.
            handler_registry.deregister(channel, handler_id);
            // Normalize to the WIRE channel: BookRaw shares Book's (Book, pair)
            // entry.
            let wire = channel.wire_channel();
            // Empty `symbols` = channel-wide guard.
            let targets: Vec<Option<crate::types::Symbol>> = if symbols.is_empty() {
                vec![None]
            } else {
                symbols.into_iter().map(Some).collect()
            };
            let mut any_terminated = false;
            for symbol in targets {
                // Params captured before release (the entry drops at 0); `None` ⇒ no
                // live entry — a missing-key release must not fake a 1→0 teardown.
                let params = registry.params_for(wire, &symbol);
                // A Failed tombstone released to 0 is removed SILENTLY: no live wire
                // subscription; its terminal event already fired at the reject.
                let was_tombstone = registry.is_terminated(wire, &symbol);
                // ALWAYS consume the guard's ref record, even when the key is gone
                // after a forced teardown — else guard_refs leaks.
                let remaining = registry.release_guard_ref(handler_id, wire, symbol.clone());
                // remaining == 0 also covers a tombstone countdown and a stale
                // no-op, so every effect below must stay tombstone-safe.
                if params.is_some() && remaining == 0 {
                    any_terminated = true;
                    teardown_released_key(
                        wire,
                        symbol,
                        params,
                        was_tombstone,
                        conns,
                        sockets,
                        bus_back_ref,
                        auth_stack,
                    );
                }
            }
            // Releasing the LAST pending entries mid-Resubscribing leaves no ack
            // to drive → Open; short-circuit.
            if any_terminated {
                if let Some(mc) = conns.get_mut(&channel_ws_url(wire)) {
                    mc.complete_resubscribe_if_empty();
                    write_mirror(state_mirrors, channel_ws_url(wire), mc.state());
                }
            }
        }
        CallerInbound::WsRequestFrame {
            req_id,
            method,
            op,
            params,
            completion,
        } => {
            handle_ws_request_frame(
                req_id, method, op, params, completion, conns, sockets, auth_stack, registry,
            );
        }
        CallerInbound::AbandonWsRequest { req_id } => {
            if let Some(mc) = conns.get_mut(&WsUrl::Auth) {
                mc.fail_pending_request(req_id, crate::error::ConnectionError::response_timeout());
            }
        }
    }
}

/// Re-derive the auth hints from post-mutation registry state (a stale `true`
/// lets a later bare order hang).
pub(in crate::dispatch::io_reactor) fn refresh_auth_send_ready_hint(
    conns: &HashMap<WsUrl, ManagedConnection>,
    registry: &SubscriptionRegistry,
    auth_stack: &Arc<crate::auth::AuthStack>,
    auth_has_subscriptions: &Arc<std::sync::atomic::AtomicBool>,
    auth_send_ready: &Arc<std::sync::atomic::AtomicBool>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    auth_has_subscriptions.store(registry.has_url_entry(WsUrl::Auth), Ordering::Release);
    if let Some(mc) = conns.get(&WsUrl::Auth) {
        refresh_send_ready(
            WsUrl::Auth,
            mc,
            registry,
            auth_stack,
            auth_send_ready,
            bus_back_ref,
        );
    }
}

/// Release one `(channel, pair)` refcount; on the 0 transition run the wire
/// teardown.
#[allow(clippy::too_many_arguments)]
fn deregister_one(
    channel: crate::types::ChannelName,
    pair: Option<crate::types::Symbol>,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    state_mirrors: &HashMap<WsUrl, Arc<std::sync::atomic::AtomicU8>>,
    bus_back_ref: &Weak<DispatchEventBus>,
    auth_stack: &Arc<crate::auth::AuthStack>,
) {
    // Normalize BookRaw→Book: both share one wire entry keyed on Book.
    let channel = channel.wire_channel();
    // Params captured before release echo the subscribed depth on the unsubscribe.
    let params = registry.params_for(channel, &pair);
    // Probed BEFORE release: a Failed entry released to 0 is removed SILENTLY
    // (its terminal event already fired at the reject).
    let was_tombstone = registry.is_terminated(channel, &pair);
    let remaining = registry.release(channel, pair.clone());
    if remaining != 0 {
        return;
    }
    // A missing-key release also returns 0: only a real 1→0 (params captured ⇒
    // entry existed) runs the teardown.
    if params.is_none() {
        return;
    }
    // The bare-slot gate's tombstone arm also returns 0 with the entry KEPT —
    // everything in teardown_released_key must stay tombstone-safe.
    teardown_released_key(
        channel,
        pair.clone(),
        params,
        was_tombstone,
        conns,
        sockets,
        bus_back_ref,
        auth_stack,
    );
    // Releasing the LAST pending entry mid-Resubscribing leaves no ack to
    // drive → Open; short-circuit.
    if let Some(mc) = conns.get_mut(&channel_ws_url(channel)) {
        mc.complete_resubscribe_if_empty();
        write_mirror(state_mirrors, channel_ws_url(channel), mc.state());
    }
}

/// Post-release teardown: disarm timers, drop the deferral record, and — unless
/// it was a tombstone — send the wire unsubscribe + terminal event.
#[allow(clippy::too_many_arguments)]
fn teardown_released_key(
    channel: crate::types::ChannelName,
    pair: Option<crate::types::Symbol>,
    params: Option<crate::conn::subscription_registry::SubscribeParams>,
    was_tombstone: bool,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
    auth_stack: &Arc<crate::auth::AuthStack>,
) {
    // Disarm timers so a later re-subscribe starts fresh; the deferral record
    // dies with the key so the refresh drain can never re-subscribe a removed entry.
    if let Some(mc) = conns.get_mut(&channel_ws_url(channel)) {
        mc.disarm_subscribe_ack(channel, pair.clone());
        mc.disarm_book_reseed_snapshot(channel, pair.clone());
        mc.remove_deferred_open_auth_subscribe(channel, &pair);
    }
    if !was_tombstone {
        unsubscribe_and_terminate(channel, pair, params, sockets, bus_back_ref, auth_stack);
    }
}

/// WS request/reply (orders) on the auth connection: gate on FSM state, inject
/// the cached token, record the pending, send. Never holds the token across await.
#[allow(clippy::too_many_arguments)]
pub(in crate::dispatch::io_reactor) fn handle_ws_request_frame(
    req_id: u64,
    method: String,
    op: crate::dispatch::WsOp,
    mut params: Value,
    completion: tokio::sync::oneshot::Sender<
        Result<crate::conn::managed_connection::WsResponse, crate::error::ConnectionError>,
    >,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    auth_stack: &Arc<crate::auth::AuthStack>,
    registry: &SubscriptionRegistry,
) {
    #[cfg(test)]
    let order_forward_start = super::latency_histogram::order_forward_probe_start();
    let url = WsUrl::Auth;
    let Some(mc) = conns.get_mut(&url) else {
        let _ = completion.send(Err(crate::error::ConnectionError::not_open()));
        return;
    };
    // FSM gate + Path C: Open always admits; a bare order is admitted mid-
    // Authenticating ONLY when the auth registry is EMPTY (the order IS the auth
    // probe) and op is an order method.
    let admit = match mc.state() {
        ConnectionState::Open => true,
        ConnectionState::Authenticating
            if url == WsUrl::Auth
                && !registry.has_url_entry(WsUrl::Auth)
                && op.is_order_method() =>
        {
            true
        }
        _ => false,
    };
    if !admit {
        // A racing subscribe can register first, so Path C won't admit — reject
        // NotOpen, retryable.
        let _ = completion.send(Err(crate::error::ConnectionError::not_open()));
        return;
    }
    // Token inject: read the cached token, inject into params.token, then DROP
    // the binding — never hold it past the compose.
    let now = mc.clock_now();
    let composed = {
        let token = match auth_stack.cached_token() {
            Some(t) if !t.is_expired(now) => t,
            _ => {
                // Orders are never auto-resent (double-fill risk): kick force_refresh
                // for the NEXT attempt and reject this one retryable.
                tracing::warn!(
                    target: "kraken_sdk::io_reactor", req_id, ?op,
                    "WsRequestFrame on Open auth conn but no valid cached token; force_refresh for NEXT order, rejecting THIS one retryable (NOT deferred — double-fill risk)"
                );
                auth_stack.invalidate_cached_token();
                mc.set_awaiting_token_refresh(true);
                let _handle =
                    auth_stack.force_refresh(crate::auth::RefreshReason::AuthHandshakeFailed);
                let _ = completion.send(Err(crate::error::ConnectionError::not_open()));
                return;
            }
        };
        if let Value::Object(ref mut map) = params {
            map.insert(
                "token".to_string(),
                Value::String(token.value().to_string()),
            );
        } else {
            let mut map = serde_json::Map::new();
            map.insert(
                "token".to_string(),
                Value::String(token.value().to_string()),
            );
            params = Value::Object(map);
        }
        serde_json::json!({ "method": method, "params": params, "req_id": req_id })
    };
    // Record the pending BEFORE the send so a synchronous drop / immediate
    // response can find it.
    mc.record_pending_request(crate::conn::managed_connection::PendingRequest {
        req_id,
        op,
        sent_at: now,
        tx: completion,
    });
    // On a send error, resolve the recorded pending with a connection error so
    // the caller's await doesn't hang.
    let send_result = match sockets.get(&url) {
        Some(socket) => {
            let frame = WsFrame {
                opcode: WsOpcode::Text,
                payload: composed.to_string().into_bytes(),
            };
            socket.send_frame(frame).map_err(|e| format!("{e:?}"))
        }
        None => Err("auth socket missing".to_string()),
    };
    #[cfg(test)]
    super::latency_histogram::order_forward_probe_end(order_forward_start);
    if let Err(e) = send_result {
        tracing::warn!(
            target: "kraken_sdk::io_reactor", req_id, ?op, error = %e,
            "WsRequestFrame send failed; failing the recorded pending immediately"
        );
        mc.fail_pending_request(req_id, crate::error::ConnectionError::not_open());
    }
}
