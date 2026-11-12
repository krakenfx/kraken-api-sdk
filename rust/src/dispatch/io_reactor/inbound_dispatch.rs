//! Routes caller inbound requests and sends WS request frames.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

use serde_json::Value;

use crate::api::ws_surface::SubscriberGuard;
use crate::conn::ManagedConnection;
use crate::conn::subscription_registry::{RemovedEntryKind, SubscriptionRegistry};
use crate::dispatch::event_bus::{
    CallerInbound, DispatchEventBus, HandlerMutationOp, RegistryMutationOp,
};
use crate::dispatch::handler_registry::HandlerRegistry;
use crate::transport::{WsFrame, WsOpcode, WsSocket};
use crate::types::{ChannelName, ConnectionState, WsUrl};

use super::SharedHandles;
use super::send_ready::refresh_send_ready;
use super::socket_lifecycle::{
    emit_subscription_terminated, project_socket_and_wire_bridge, start_connect_if_idle,
    unsubscribe_and_terminate, write_mirror,
};
use super::subscribe_send::{register_one_and_emit, send_single_subscribe, send_subscribe_batch};

pub(super) async fn handle_inbound(
    msg: CallerInbound,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    handler_registry: &mut HandlerRegistry,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    handles: SharedHandles<'_>,
    upgrade_guards: &mut HashMap<WsUrl, SubscriberGuard>,
) {
    let SharedHandles {
        auth_stack,
        bus_back_ref,
        state_mirrors,
        upgrade_tx,
        ..
    } = handles;
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
                    if url == WsUrl::UnifiedPublic {
                        register_unified_public_batch(
                            entries,
                            ref_id,
                            conns,
                            registry,
                            sockets,
                            handles,
                            upgrade_guards,
                        );
                        return;
                    }
                    for entry in entries {
                        register_one_and_emit(
                            url,
                            entry,
                            ref_id,
                            conns,
                            registry,
                            sockets,
                            handles,
                            upgrade_guards,
                        );
                        // Mid-batch teardown drops the stale socket; the rest still register and replay on reconnect.
                        if conns.get(&url).is_some_and(|mc| mc.socket().is_none()) {
                            sockets.remove(&url);
                            upgrade_guards.remove(&url);
                        }
                    }
                }
                RegistryMutationOp::Deregister { channel, pair } => {
                    deregister_one(
                        url,
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
                    for pair in pairs {
                        deregister_one(
                            url,
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
                    let urls: &[WsUrl] = match url {
                        // Spot's public subscription surface owns both the public and auth channels.
                        WsUrl::Public => &[WsUrl::Public, WsUrl::Auth],
                        _ => std::slice::from_ref(&url),
                    };
                    for &target_url in urls {
                        let removed = registry.deregister_all_for(
                            target_url,
                            channel.map(|c| c.registry_channel(target_url)),
                        );
                        let mut removed_any = false;
                        let mut unified_book_wire_teardowns = HashSet::new();
                        for (entry, kind) in removed {
                            removed_any = true;
                            let shared_unified_book =
                                entry.url.shares_unified_book_stream(entry.channel);
                            let peer_live = shared_unified_book
                                && registry.unified_book_peer_live(entry.channel, &entry.pair);
                            let first_wire_teardown = !shared_unified_book
                                || kind == RemovedEntryKind::Tombstone
                                || unified_book_wire_teardowns.insert(entry.pair.clone());
                            if shared_unified_book && (peer_live || !first_wire_teardown) {
                                // The remaining peer keeps the venue stream; this row's timers drop and the caller hears it closed.
                                release_unified_book_owner(
                                    entry.url,
                                    entry.channel,
                                    entry.pair,
                                    kind == RemovedEntryKind::Tombstone,
                                    peer_live,
                                    conns,
                                    registry,
                                    sockets,
                                    bus_back_ref,
                                );
                                continue;
                            }
                            teardown_released_key(
                                entry.url,
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
                        // Emptying the pending-ack set mid-Resubscribing leaves no ack to drive Open; short-circuit.
                        if removed_any && let Some(mc) = conns.get_mut(&target_url) {
                            mc.complete_resubscribe_if_empty();
                            write_mirror(state_mirrors, target_url, mc.state());
                        }
                    }
                }
            }
        }
        CallerInbound::HandlerMutation { url, channel, op } => match op {
            HandlerMutationOp::Register { id, callback } => {
                handler_registry.register_with_id_for_url(url, channel, id, callback);
            }
            HandlerMutationOp::Deregister { id } => {
                handler_registry.deregister_for_url(url, channel, id);
            }
        },
        CallerInbound::RegisterHandlerAndConnect {
            url,
            channel,
            id,
            callback,
        } => {
            handler_registry.register_with_id_for_url(url, channel, id, callback);
            if let Some(mc) = conns.get_mut(&url) {
                start_connect_if_idle(mc, url, handles, sockets, upgrade_guards);
            }
        }
        CallerInbound::SubscriptionGuardDrop {
            handler_id,
            url,
            channel,
            symbols,
        } => {
            // One post tears down the whole guard; the wire unsubscribe fires only
            // on the 0 transition.
            handler_registry.deregister_for_url(url, channel, handler_id);
            // Spot BookRaw shares Book's registry entry; Unified Futures keeps it separate.
            let wire = channel.registry_channel(url);
            let targets: Vec<Option<crate::types::StreamSymbol>> = if symbols.is_empty() {
                vec![None]
            } else {
                symbols.into_iter().map(Some).collect()
            };
            let mut any_terminated = false;
            for symbol in targets {
                // `None` = no live entry; a missing-key release must not fake a 1→0 teardown.
                let params = registry.params_for_url(url, wire, &symbol);
                // A Failed tombstone released to 0 is removed silently; its terminal event already fired.
                let was_tombstone = registry.is_terminated_for(url, wire, &symbol);
                // Always consume the guard's ref record, else guard_refs leaks.
                let remaining =
                    registry.release_guard_ref_for(handler_id, url, wire, symbol.clone());
                // remaining == 0 also covers tombstone countdown and stale no-op; effects below must stay tombstone-safe.
                if params.is_some() && remaining == 0 {
                    if url == WsUrl::UnifiedPublic && registry.unified_book_peer_live(wire, &symbol)
                    {
                        any_terminated = true;
                        release_unified_book_owner(
                            url,
                            wire,
                            symbol,
                            was_tombstone,
                            true,
                            conns,
                            registry,
                            sockets,
                            bus_back_ref,
                        );
                        continue;
                    }
                    any_terminated = true;
                    teardown_released_key(
                        url,
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
            // Releasing the last pending entry mid-Resubscribing leaves no ack to drive Open; short-circuit.
            if any_terminated && let Some(mc) = conns.get_mut(&url) {
                mc.complete_resubscribe_if_empty();
                write_mirror(state_mirrors, url, mc.state());
            }
        }
        CallerInbound::WsRequestFrame {
            url,
            req_id,
            method,
            op,
            params,
            completion,
        } => {
            handle_ws_request_frame(
                url, req_id, method, op, params, completion, conns, sockets, auth_stack, registry,
            );
        }
        CallerInbound::AbandonWsRequest { req_id } => {
            for mc in conns.values_mut() {
                mc.fail_pending_request(req_id, crate::error::ConnectionError::response_timeout());
            }
        }
    }
}

fn register_unified_public_batch(
    entries: Vec<crate::conn::subscription_registry::SubscriptionEntry>,
    ref_id: Option<crate::dispatch::HandlerId>,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    handles: SharedHandles<'_>,
    upgrade_guards: &mut HashMap<WsUrl, SubscriberGuard>,
) {
    let SharedHandles {
        bus_back_ref,
        state_mirrors,
        ..
    } = handles;
    let now = super::bus_clock_now(bus_back_ref);
    let mut new_entries = Vec::new();
    let mut late_reseeds = Vec::new();
    let mut late_raw_attaches = Vec::new();
    for entry in entries {
        let channel = entry.channel;
        let pair = entry.pair.clone();
        let params = entry.params;
        let count = registry.register(entry, now, ref_id.is_some());
        if let Some(id) = ref_id {
            registry.record_guard_ref_for(id, WsUrl::UnifiedPublic, channel, &pair);
        }
        let late_book_join = count == 1 && registry.unified_book_peer_acked(channel, &pair);
        if late_book_join && channel == ChannelName::BookRaw {
            if registry.note_acked_for(WsUrl::UnifiedPublic, channel, &pair, now) {
                late_raw_attaches.push(pair.clone());
            }
        } else if late_book_join {
            registry.require_unified_book_seed_for(&pair);
            if let Some(sym) = pair.as_ref() {
                registry.begin_book_resync_for(WsUrl::UnifiedPublic, ChannelName::Book, sym);
            }
            late_reseeds.push(pair.clone());
        }
        if count == 1 && !late_book_join && !registry.unified_book_peer_live(channel, &pair) {
            new_entries.push((channel, pair, params));
        }
    }
    let Some(mc) = conns.get_mut(&WsUrl::UnifiedPublic) else {
        return;
    };
    for pair in late_raw_attaches {
        mc.emit_futures_subscribed(
            ChannelName::BookRaw,
            &pair,
            &crate::dispatch::FuturesSubscribeAck::default(),
        );
    }
    for pair in late_reseeds {
        let Some(symbol) = pair else {
            continue;
        };
        let Some(params) = registry.params_for_url(
            WsUrl::UnifiedPublic,
            ChannelName::Book,
            &Some(symbol.clone()),
        ) else {
            continue;
        };
        super::frame_routing::send_book_reseed(
            WsUrl::UnifiedPublic,
            symbol,
            params,
            mc,
            sockets,
            bus_back_ref,
        );
    }
    start_connect_if_idle(mc, WsUrl::UnifiedPublic, handles, sockets, upgrade_guards);
    if !new_entries.is_empty() {
        let mut group_indexes: HashMap<
            (crate::types::ChannelName, crate::conn::SubscribeParams),
            usize,
        > = HashMap::new();
        let mut batches: Vec<(
            crate::types::ChannelName,
            Vec<Option<crate::types::StreamSymbol>>,
            crate::conn::SubscribeParams,
        )> = Vec::new();
        for (channel, pair, params) in new_entries {
            // Only per-symbol entries share a Unified frame.
            if pair.is_some()
                && let Some(&index) = group_indexes.get(&(channel, params))
            {
                batches[index].1.push(pair);
            } else {
                if pair.is_some() {
                    group_indexes.insert((channel, params), batches.len());
                }
                batches.push((channel, vec![pair], params));
            }
        }
        // Retain every group until its frame reaches the writer; WriterClosed can stop the loop early.
        if !mc.has_been_open() {
            for (channel, pairs, _) in &batches {
                mc.mark_initial_subscribe_replay(*channel, pairs);
            }
        }
        for (channel, pairs, params) in batches {
            send_subscribe_batch(
                WsUrl::UnifiedPublic,
                channel,
                pairs,
                params,
                mc,
                sockets,
                bus_back_ref,
            );
            if mc.socket().is_none() && mc.state() == ConnectionState::BackingOff {
                // WriterClosed already moved the connection to BackingOff; replay retains all entries.
                sockets.remove(&WsUrl::UnifiedPublic);
                upgrade_guards.remove(&WsUrl::UnifiedPublic);
                write_mirror(state_mirrors, WsUrl::UnifiedPublic, mc.state());
                break;
            }
        }
    }
}

/// Re-derives auth hints from post-mutation registry state; a stale `true` hangs later bare orders.
pub(in crate::dispatch::io_reactor) fn refresh_auth_send_ready_hint(
    conns: &HashMap<WsUrl, ManagedConnection>,
    registry: &SubscriptionRegistry,
    auth_stack: &Arc<crate::auth::AuthStack>,
    order_gate: &Arc<crate::conn::OrderGate>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    for url in WsUrl::ALL.into_iter().filter(|url| url.requires_ws_token()) {
        order_gate.set_has_subscriptions(url, registry.has_url_entry(url));
        if let Some(mc) = conns.get(&url) {
            refresh_send_ready(url, mc, registry, auth_stack, order_gate, bus_back_ref);
        }
    }
}

/// Drops one logical Unified book owner without unsubscribing the shared stream.
/// A Pending peer not owning the in-flight frame takes it over so it cannot stay Pending forever.
#[expect(
    clippy::too_many_arguments,
    reason = "takes only the shared handles it uses; mutable reactor state and per-call data are passed explicitly"
)]
pub(in crate::dispatch::io_reactor) fn release_unified_book_owner(
    url: WsUrl,
    channel: ChannelName,
    pair: Option<crate::types::StreamSymbol>,
    was_tombstone: bool,
    peer_keeps_wire: bool,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    if peer_keeps_wire
        && !registry.unified_book_peer_acked(channel, &pair)
        && let Some(peer) = channel.book_peer()
    {
        let peer_armed = conns
            .get(&url)
            .is_some_and(|mc| mc.is_subscribe_ack_armed(peer, pair.clone()));
        let handed_off = conns
            .get_mut(&url)
            .is_some_and(|mc| mc.handoff_subscribe_inflight(channel, peer, pair.clone()));
        if !handed_off
            && !peer_armed
            && let Some(params) = registry.params_for_url(url, peer, &pair)
            && let Some(mc) = conns.get_mut(&url)
        {
            send_single_subscribe(url, peer, pair.clone(), params, mc, sockets, bus_back_ref);
        }
    }
    if let Some(mc) = conns.get_mut(&url) {
        mc.disarm_subscribe_ack_key(channel, pair.clone());
        mc.forget_released_key(channel, &pair);
    }
    if !was_tombstone {
        emit_subscription_terminated(url, channel, pair, bus_back_ref);
    }
}

/// Releases one `(channel, pair)` refcount; the 0 transition runs the wire teardown.
#[expect(
    clippy::too_many_arguments,
    reason = "takes only the shared handles it uses; mutable reactor state and per-call data are passed explicitly"
)]
fn deregister_one(
    url: WsUrl,
    channel: crate::types::ChannelName,
    pair: Option<crate::types::StreamSymbol>,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    state_mirrors: &HashMap<WsUrl, Arc<std::sync::atomic::AtomicU8>>,
    bus_back_ref: &Weak<DispatchEventBus>,
    auth_stack: &Arc<crate::auth::AuthStack>,
) {
    let channel = channel.registry_channel(url);
    let params = registry.params_for_url(url, channel, &pair);
    // Probed before release: a Failed entry released to 0 is removed silently.
    let was_tombstone = registry.is_terminated_for(url, channel, &pair);
    let remaining = registry.release_for(url, channel, pair.clone());
    if remaining != 0 {
        return;
    }
    // A missing-key release also returns 0; only a captured-params release runs teardown.
    if params.is_none() {
        return;
    }
    // Unified Book and BookRaw share one venue subscription; the remaining peer keeps it live.
    if url == WsUrl::UnifiedPublic && registry.unified_book_peer_live(channel, &pair) {
        release_unified_book_owner(
            url,
            channel,
            pair,
            was_tombstone,
            true,
            conns,
            registry,
            sockets,
            bus_back_ref,
        );
        if let Some(mc) = conns.get_mut(&url) {
            mc.complete_resubscribe_if_empty();
            write_mirror(state_mirrors, url, mc.state());
        }
        return;
    }
    // The tombstone arm returns 0 with the entry kept; teardown_released_key must stay tombstone-safe.
    teardown_released_key(
        url,
        channel,
        pair.clone(),
        params,
        was_tombstone,
        conns,
        sockets,
        bus_back_ref,
        auth_stack,
    );
    // Releasing the last pending entry mid-Resubscribing leaves no ack to drive Open; short-circuit.
    if let Some(mc) = conns.get_mut(&url) {
        mc.complete_resubscribe_if_empty();
        write_mirror(state_mirrors, url, mc.state());
    }
}

/// Post-release teardown; tombstones skip the wire unsubscribe and terminal event.
#[expect(
    clippy::too_many_arguments,
    reason = "takes only the shared handles it uses; mutable reactor state and per-call data are passed explicitly"
)]
fn teardown_released_key(
    url: WsUrl,
    channel: crate::types::ChannelName,
    pair: Option<crate::types::StreamSymbol>,
    params: Option<crate::conn::subscription_registry::SubscribeParams>,
    was_tombstone: bool,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
    auth_stack: &Arc<crate::auth::AuthStack>,
) {
    // The deferral record dies with the key so the refresh drain cannot re-subscribe a removed entry.
    if let Some(mc) = conns.get_mut(&url) {
        mc.disarm_subscribe_ack(channel, pair.clone());
        mc.forget_released_key(channel, &pair);
    }
    if !was_tombstone {
        unsubscribe_and_terminate(
            url,
            channel,
            pair,
            params,
            sockets,
            bus_back_ref,
            auth_stack,
        );
    }
}

/// Sends a WS request on a private connection; never holds the token across await.
#[expect(
    clippy::too_many_arguments,
    reason = "takes only the shared handles it uses; mutable reactor state and per-call data are passed explicitly"
)]
pub(in crate::dispatch::io_reactor) fn handle_ws_request_frame(
    url: WsUrl,
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
    let Some(mc) = conns.get_mut(&url) else {
        let _ = completion.send(Err(crate::error::ConnectionError::not_open()));
        return;
    };
    // Open always admits; a bare order is admitted mid-Authenticating only when the auth registry is empty.
    let admit = match mc.state() {
        ConnectionState::Open => true,
        ConnectionState::Authenticating
            if url.requires_ws_token() && !registry.has_url_entry(url) && op.is_order_method() =>
        {
            true
        }
        _ => false,
    };
    if !admit {
        // A racing subscribe can register first, so the order is rejected NotOpen (retryable).
        let _ = completion.send(Err(mc.refusal_error()));
        return;
    }
    // Drop the token binding right after compose.
    let now = mc.clock_now();
    let composed = {
        let token = match auth_stack.cached_token() {
            Some(t) if !t.is_expired(now) => t,
            _ => {
                // Orders are never auto-resent (double-fill risk): refresh for the next attempt, reject this one.
                tracing::warn!(
                    target: "kraken_sdk::io_reactor", req_id, ?url, ?op,
                    "WsRequestFrame on Open private conn but no valid cached token; force_refresh for NEXT order, rejecting THIS one retryable (NOT deferred — double-fill risk)"
                );
                auth_stack.invalidate_cached_token();
                mc.force_reactive_token_refresh(auth_stack);
                let _ = completion.send(Err(
                    crate::error::ConnectionError::session_token_unavailable(),
                ));
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
        if let (Some(asset_class), Value::Object(map)) = (url.request_asset_class(), &mut params) {
            map.insert(
                "asset_class".to_string(),
                Value::String(asset_class.to_string()),
            );
        }
        serde_json::json!({ "method": method, "params": params, "req_id": req_id })
    };
    // Record the pending before the send so an immediate response finds it.
    mc.record_pending_request(crate::conn::managed_connection::PendingRequest {
        req_id,
        op,
        sent_at: now,
        tx: completion,
    });
    // On a send error, resolve the pending so the caller's await does not hang.
    let send_result = match sockets.get(&url) {
        Some(socket) => {
            let frame = WsFrame {
                opcode: WsOpcode::Text,
                payload: composed.to_string().into_bytes(),
            };
            socket.send_frame(frame).map_err(|e| format!("{e:?}"))
        }
        None => Err("socket missing".to_string()),
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
