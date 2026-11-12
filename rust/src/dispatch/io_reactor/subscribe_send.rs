//! Subscribe-frame emission helpers: signed/public wire subscribes,
//! register-and-emit, and the resend-timer handler.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::api::ws_surface::SubscriberGuard;
use crate::conn::ManagedConnection;
use crate::conn::subscription_registry::SubscriptionRegistry;
use crate::dispatch::event_bus::DispatchEventBus;
use crate::transport::{
    SendFrameError, TransportError, TransportErrorKind, WsFrame, WsOpcode, WsSocket,
};
use crate::types::{ChannelName, ConnectionState, StreamSymbol, WsUrl};

use super::SharedHandles;
use super::send_ready::refresh_send_ready;
use super::socket_lifecycle::start_connect_if_idle;

/// Arm the per-entry subscribe-ack timer and record the `req_id -> (channel,
/// pair)` correlation so a `channel`-less reject resolves back to this entry.
fn arm_subscribe_ack_timer_with_req_id(
    mc: &mut ManagedConnection,
    channel: ChannelName,
    pair: Option<StreamSymbol>,
    req_id: u64,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    let due_at = super::due_after_ms(mc, super::subscribe_ack_timeout_ms(bus_back_ref));
    mc.record_subscribe_req_id(req_id, channel, pair.clone());
    mc.arm_subscribe_ack(channel, pair, due_at);
}

/// Synthesise a transient `WireAbnormalClose` and drive it through the FSM
/// (→ BackingOff) when the outbound writer task is dead on a subscribe send.
fn drive_writer_closed_abnormal(mc: &mut ManagedConnection, context: &'static str) {
    let error = TransportError {
        kind: TransportErrorKind::AbnormalClose {
            context: context.into(),
        },
        transient: true,
    };
    let event = if mc.state() == ConnectionState::Connecting {
        // Initial RegisterBatch can send before the asynchronous upgrade-ok
        // reaches the reactor; Connecting handles this as a transient connect
        // failure, whereas WireAbnormalClose is only meaningful post-upgrade.
        crate::conn::managed_connection::FsmEvent::WireConnectError {
            kind: error.kind,
            transient: error.transient,
        }
    } else {
        crate::conn::managed_connection::FsmEvent::WireAbnormalClose { error }
    };
    mc.handle_event(event);
}

/// Disposition of one subscribe-frame send; control flow stays caller-side.
pub(in crate::dispatch::io_reactor) enum SendOutcome {
    Sent,
    Backpressured,
    WriterDead,
}

/// Which subscribe path is sending; owns the per-path log strings for
/// [`send_subscribe_frame`].
#[derive(Clone, Copy, Debug)]
pub(in crate::dispatch::io_reactor) enum SubscribeSend {
    Resend,
    FirstSigned,
    RemainingSigned,
    RegisterPublic,
    RegisterWhileOpenAuth,
    ResubscribeReplay,
}

impl SubscribeSend {
    fn backpressure_msg(self) -> &'static str {
        match self {
            SubscribeSend::Resend => {
                "resend: still backpressure; decrementing budget via handle_subscribe_failure"
            }
            SubscribeSend::FirstSigned => {
                "first signed subscribe send failed (backpressure); handle_subscribe_failure"
            }
            SubscribeSend::RemainingSigned => {
                "remaining signed subscribe send failed (backpressure); handle_subscribe_failure"
            }
            SubscribeSend::RegisterPublic => {
                "Register: wire subscribe send failed (backpressure); handle_subscribe_failure"
            }
            SubscribeSend::RegisterWhileOpenAuth => {
                "Register-while-Open: tokenized subscribe send failed (backpressure); handle_subscribe_failure"
            }
            SubscribeSend::ResubscribeReplay => {
                "resubscribe-replay send failed (backpressure); handle_subscribe_failure"
            }
        }
    }

    fn writer_closed_msg(self) -> &'static str {
        match self {
            SubscribeSend::Resend => "resend: writer closed; synthesising WireAbnormalClose",
            SubscribeSend::FirstSigned => {
                "first signed subscribe send failed (writer closed); synthesising WireAbnormalClose"
            }
            SubscribeSend::RemainingSigned => {
                "remaining signed subscribe send failed (writer closed); synthesising WireAbnormalClose"
            }
            SubscribeSend::RegisterPublic => {
                "Register: wire subscribe send failed (writer closed); synthesising WireAbnormalClose"
            }
            SubscribeSend::RegisterWhileOpenAuth => {
                "Register-while-Open: tokenized subscribe send failed (writer closed); synthesising WireAbnormalClose"
            }
            SubscribeSend::ResubscribeReplay => {
                "resubscribe-replay send failed (writer closed); synthesising WireAbnormalClose"
            }
        }
    }

    fn writer_closed_context(self) -> &'static str {
        match self {
            SubscribeSend::Resend => {
                "outbound writer task terminated (discovered on subscribe resend)"
            }
            SubscribeSend::FirstSigned => {
                "outbound writer task terminated (first signed subscribe)"
            }
            SubscribeSend::RemainingSigned => {
                "outbound writer task terminated (remaining signed subscribes)"
            }
            SubscribeSend::RegisterPublic => {
                "outbound writer task terminated (Register public subscribe)"
            }
            SubscribeSend::RegisterWhileOpenAuth => {
                "outbound writer task terminated (Register-while-Open auth subscribe)"
            }
            SubscribeSend::ResubscribeReplay => {
                "outbound writer task terminated (resubscribe replay)"
            }
        }
    }
}

/// Send one composed subscribe frame, mapping a failure onto the shared
/// side-effects (warn + backpressure budget / writer-dead teardown).
pub(in crate::dispatch::io_reactor) fn send_subscribe_frame(
    socket: &Arc<dyn WsSocket>,
    frame: WsFrame,
    url: WsUrl,
    channel: ChannelName,
    pair: &Option<StreamSymbol>,
    mc: &mut ManagedConnection,
    send: SubscribeSend,
) -> SendOutcome {
    send_subscribe_batch_frame(
        socket,
        frame,
        url,
        channel,
        std::slice::from_ref(pair),
        mc,
        send,
    )
}

/// Send one batched subscribe frame. A frame can represent independently-owned
/// entries, so a transient send failure must schedule recovery for every key.
pub(in crate::dispatch::io_reactor) fn send_subscribe_batch_frame(
    socket: &Arc<dyn WsSocket>,
    frame: WsFrame,
    url: WsUrl,
    channel: ChannelName,
    pairs: &[Option<StreamSymbol>],
    mc: &mut ManagedConnection,
    send: SubscribeSend,
) -> SendOutcome {
    match socket.send_frame(frame) {
        Ok(()) => {
            mc.note_snapshot_opt_out_written(channel, pairs);
            SendOutcome::Sent
        }
        Err(SendFrameError::Backpressure) => {
            tracing::warn!(
                target: "kraken_sdk::io_reactor", ?url, ?channel, ?pairs,
                "{}", send.backpressure_msg()
            );
            let state = mc.state();
            for pair in pairs {
                mc.handle_subscribe_failure(
                    channel,
                    pair.clone(),
                    true,
                    None,
                    &crate::dispatch::FuturesSubscribeAck::default(),
                );
                // An exhausted budget tore the connection down; replay owns
                // the remaining keys.
                if mc.state() != state {
                    break;
                }
            }
            mc.mark_initial_subscribe_replay(channel, pairs);
            SendOutcome::Backpressured
        }
        Err(SendFrameError::WriterClosed) => {
            tracing::warn!(
                target: "kraken_sdk::io_reactor", ?url, ?channel, ?pairs,
                "{}", send.writer_closed_msg()
            );
            // A writer-close tears the connection down once; retained registry
            // entries are restored together by reconnect replay.
            mc.mark_initial_subscribe_replay(channel, pairs);
            drive_writer_closed_abnormal(mc, send.writer_closed_context());
            SendOutcome::WriterDead
        }
    }
}

/// `TimerSubscribeResendDue` fire: re-issue the subscribe. A direct reactor
/// action — the FSM has no arm for this timer.
#[expect(
    clippy::too_many_arguments,
    reason = "takes only the shared handles it uses; mutable reactor state and per-call data are passed explicitly"
)]
pub(in crate::dispatch::io_reactor) fn handle_subscribe_resend(
    url: WsUrl,
    channel: ChannelName,
    pair: Option<StreamSymbol>,
    mc: &mut ManagedConnection,
    registry: &SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    auth_stack: &Arc<crate::auth::AuthStack>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    // The entry may have been deregistered while the timer was in flight; drop
    // silently.
    let Some(params) = registry.params_for_url(url, channel, &pair) else {
        tracing::debug!(
            target: "kraken_sdk::io_reactor", ?url, ?channel, ?pair,
            "resend: entry deregistered before timer fired; no-op"
        );
        return;
    };

    let is_auth = url.requires_ws_token();

    let token_val: Option<String> = if is_auth {
        let now = mc.clock_now();
        match auth_stack.cached_token() {
            Some(t) if !t.is_expired(now) => Some(t.value().to_owned()),
            _ => {
                tracing::debug!(
                    target: "kraken_sdk::io_reactor", ?url, ?channel, ?pair,
                    "resend: no valid cached token on auth resend; deferring to BusTokenRefreshed"
                );
                mc.defer_open_auth_subscribe(channel, pair.clone(), params);
                // The consumed timer may have been Resubscribing's last outstanding
                // work: complete to Open.
                mc.complete_resubscribe_if_empty();
                mc.force_reactive_token_refresh(auth_stack);
                return;
            }
        }
    } else {
        None
    };

    let Some(socket) = sockets.get(&url) else {
        tracing::debug!(
            target: "kraken_sdk::io_reactor", ?url, ?channel, ?pair,
            "resend: socket not present; no-op (reconnect-replay will recover)"
        );
        return;
    };

    let pairs = pair.as_slice();
    let req_id = mc.next_subscribe_req_id();
    let mut payload = crate::conn::subscription_registry::build_subscribe_frame(
        url,
        "subscribe",
        channel,
        pairs,
        params,
        req_id,
    );
    if let Some(ref tok) = token_val {
        crate::conn::subscription_registry::inject_token(&mut payload, tok);
    }
    let frame = WsFrame {
        opcode: WsOpcode::Text,
        payload: payload.to_string().into_bytes(),
    };

    match send_subscribe_frame(
        socket,
        frame,
        url,
        channel,
        &pair,
        mc,
        SubscribeSend::Resend,
    ) {
        SendOutcome::Sent => {
            mc.clear_initial_subscribe_replay(channel, std::slice::from_ref(&pair));
            arm_subscribe_ack_timer_with_req_id(mc, channel, pair.clone(), req_id, bus_back_ref);
            tracing::debug!(
                target: "kraken_sdk::io_reactor", ?url, ?channel, ?pair,
                "resend: subscribe re-issued successfully; ack timer re-armed"
            );
        }
        SendOutcome::Backpressured | SendOutcome::WriterDead => {}
    }
}

/// Authenticating entry action: send the first signed subscribe; its ack drives
/// `Authenticating → Resubscribing`. No token → `force_refresh` + deferred re-issue.
pub(in crate::dispatch::io_reactor) fn send_first_signed_subscribe(
    url: WsUrl,
    mc: &mut ManagedConnection,
    registry: &SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    auth_stack: &Arc<crate::auth::AuthStack>,
    order_gate: &Arc<crate::conn::OrderGate>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    let now = mc.clock_now();
    let token = match auth_stack.cached_token() {
        Some(t) if !t.is_expired(now) => t,
        _ => {
            // No valid token: start a connection-owned refresh; the outcome bridge
            // re-invokes this path.
            // One of two mutually-exclusive force_refresh sites per handshake:
            // this fires only when NO frame was sent.
            mc.force_reactive_token_refresh(auth_stack);
            tracing::debug!(
                target: "kraken_sdk::io_reactor", ?url,
                "Authenticating: no valid cached token; force_refresh in flight (lazy first fetch)"
            );
            return;
        }
    };

    let Some(socket) = sockets.get(&url) else {
        tracing::warn!(
            target: "kraken_sdk::io_reactor", ?url,
            "Authenticating entry: socket missing; cannot send first signed subscribe"
        );
        return;
    };
    // ONLY the first (golden-order) signed subscribe is sent here; the rest is
    // replayed in Resubscribing.
    let Some((channel, pair, mut payload)) =
        registry.compose_first_signed_subscribe_authed(url, token.value())
    else {
        tracing::warn!(
            target: "kraken_sdk::io_reactor", ?url,
            "Authenticating: no auth-URL entry to send as first signed subscribe (KeepaliveEntry is 3b)"
        );
        // Bare-order send-ready: token cached + empty auth registry —
        // refresh_send_ready stores + emits so an awaiting order send unblocks.
        if url.requires_ws_token() {
            refresh_send_ready(url, mc, registry, auth_stack, order_gate, bus_back_ref);
        }
        return;
    };
    let req_id = mc.next_subscribe_req_id();
    crate::conn::subscription_registry::stamp_req_id(&mut payload, req_id);
    let frame = WsFrame {
        opcode: WsOpcode::Text,
        payload: payload.to_string().into_bytes(),
    };
    match send_subscribe_frame(
        socket,
        frame,
        url,
        channel,
        &pair,
        mc,
        SubscribeSend::FirstSigned,
    ) {
        SendOutcome::Sent => {}
        SendOutcome::Backpressured | SendOutcome::WriterDead => return,
    }
    arm_subscribe_ack_timer_with_req_id(mc, channel, pair, req_id, bus_back_ref);
}

/// After the handshake ack, send the remaining token-injected subscribe frames,
/// each arming an ack timer.
pub(in crate::dispatch::io_reactor) fn send_remaining_signed_subscribes(
    url: WsUrl,
    mc: &mut ManagedConnection,
    registry: &SubscriptionRegistry,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    auth_stack: &Arc<crate::auth::AuthStack>,
    bus_back_ref: &Weak<DispatchEventBus>,
    already_sent: Option<(ChannelName, Option<StreamSymbol>)>,
) {
    let now = mc.clock_now();
    let token = match auth_stack.cached_token() {
        Some(t) if !t.is_expired(now) => t,
        _ => {
            // Token miss mid-replay: a silent return would strand every entry
            // Pending; defer the batch and kick the single-flight refresh.
            for (channel, pair, params) in
                registry.live_subscribe_keys_for_url(url, already_sent.as_ref())
            {
                mc.defer_open_auth_subscribe(channel, pair, params);
            }
            mc.force_reactive_token_refresh(auth_stack);
            return;
        }
    };
    let Some(socket) = sockets.get(&url) else {
        return;
    };
    // `None` composes every live entry — the bare-probe route sent no
    // subscribe, so nothing is excluded by identity.
    let frames = match &already_sent {
        Some(key) => registry.compose_remaining_signed_subscribes_authed(url, token.value(), key),
        None => registry.compose_subscribe_frames_for_url_authed(url, token.value()),
    };
    for (channel, pair, mut payload) in frames {
        let req_id = mc.next_subscribe_req_id();
        crate::conn::subscription_registry::stamp_req_id(&mut payload, req_id);
        let frame = WsFrame {
            opcode: WsOpcode::Text,
            payload: payload.to_string().into_bytes(),
        };
        match send_subscribe_frame(
            socket,
            frame,
            url,
            channel,
            &pair,
            mc,
            SubscribeSend::RemainingSigned,
        ) {
            SendOutcome::Sent => {}
            SendOutcome::Backpressured => {
                // Stop replaying if that teardown left Resubscribing.
                if mc.state() != ConnectionState::Resubscribing {
                    return;
                }
                continue;
            }
            SendOutcome::WriterDead => return,
        }
        arm_subscribe_ack_timer_with_req_id(mc, channel, pair, req_id, bus_back_ref);
    }
}

/// Emit the public (untokenized) wire SUBSCRIBE on the 0→1 refcount edge and
/// arm its ack timer. Socket missing → warn; reconnect-replay recovers the entry.
pub(in crate::dispatch::io_reactor) fn send_single_subscribe(
    url: WsUrl,
    channel: ChannelName,
    pair: Option<StreamSymbol>,
    params: crate::conn::subscription_registry::SubscribeParams,
    mc: &mut ManagedConnection,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    send_subscribe_batch(url, channel, vec![pair], params, mc, sockets, bus_back_ref);
}

/// Emit one public subscribe frame for independently-owned registry entries.
/// Spot callers continue to pass exactly one key; Unified Public callers may
/// pass a whole RegisterBatch and share the resulting request id.
pub(in crate::dispatch::io_reactor) fn send_subscribe_batch(
    url: WsUrl,
    channel: ChannelName,
    pairs: Vec<Option<StreamSymbol>>,
    params: crate::conn::subscription_registry::SubscribeParams,
    mc: &mut ManagedConnection,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    let Some(socket) = sockets.get(&url) else {
        // A tokenless socket may register while its very first connection is
        // backing off, before a socket exists. Flag that the upgrade path must
        // replay these pending registry entries.
        mc.mark_initial_subscribe_replay(channel, &pairs);
        tracing::warn!(
            target: "kraken_sdk::io_reactor", ?url, ?channel, ?pairs,
            "Register: socket not open; wire subscribe dropped (reconnect-replay will recover from registry)"
        );
        return;
    };
    let symbols: Vec<StreamSymbol> = pairs.iter().flatten().cloned().collect();
    let req_id = mc.next_subscribe_req_id();
    let payload = crate::conn::subscription_registry::build_subscribe_frame(
        url,
        "subscribe",
        channel,
        &symbols,
        params,
        req_id,
    );
    let frame = WsFrame {
        opcode: WsOpcode::Text,
        payload: payload.to_string().into_bytes(),
    };
    match send_subscribe_batch_frame(
        socket,
        frame,
        url,
        channel,
        &pairs,
        mc,
        SubscribeSend::RegisterPublic,
    ) {
        SendOutcome::Sent => {
            mc.clear_initial_subscribe_replay(channel, &pairs);
        }
        SendOutcome::Backpressured | SendOutcome::WriterDead => return,
    }
    for pair in pairs {
        arm_subscribe_ack_timer_with_req_id(mc, channel, pair, req_id, bus_back_ref);
    }
}

/// Emit the tokenized auth wire SUBSCRIBE for a `(channel, pair)` registered
/// while already `Open`. No valid token defers + kicks `force_refresh`.
#[expect(
    clippy::too_many_arguments,
    reason = "takes only the shared handles it uses; mutable reactor state and per-call data are passed explicitly"
)]
pub(in crate::dispatch::io_reactor) fn send_single_signed_subscribe(
    url: WsUrl,
    channel: ChannelName,
    pair: Option<StreamSymbol>,
    params: crate::conn::subscription_registry::SubscribeParams,
    mc: &mut ManagedConnection,
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    auth_stack: &Arc<crate::auth::AuthStack>,
    bus_back_ref: &Weak<DispatchEventBus>,
) {
    let now = mc.clock_now();
    let token = match auth_stack.cached_token() {
        Some(t) if !t.is_expired(now) => t,
        _ => {
            // Record the deferral BEFORE the single-flight guard (EVERY entry must
            // re-issue when the refresh lands); spawn only on the first occurrence.
            mc.defer_open_auth_subscribe(channel, pair.clone(), params);
            if mc.force_reactive_token_refresh(auth_stack) {
                tracing::debug!(
                    target: "kraken_sdk::io_reactor", ?url, ?channel, ?pair,
                    "Register-while-Open: no valid cached token; force_refresh in flight, tokenized subscribe deferred to BusTokenRefreshed re-issue"
                );
            } else {
                tracing::debug!(
                    target: "kraken_sdk::io_reactor", ?url, ?channel, ?pair,
                    "Register-while-Open: force_refresh already in flight; subscribe deferred, skipping redundant spawn"
                );
            }
            return;
        }
    };
    let Some(socket) = sockets.get(&url) else {
        tracing::warn!(
            target: "kraken_sdk::io_reactor", ?url, ?channel, ?pair,
            "Register-while-Open: socket not open; tokenized subscribe dropped"
        );
        return;
    };
    let pairs = pair.as_slice();
    let req_id = mc.next_subscribe_req_id();
    let mut payload = crate::conn::subscription_registry::build_subscribe_frame(
        url,
        "subscribe",
        channel,
        pairs,
        params,
        req_id,
    );
    crate::conn::subscription_registry::inject_token(&mut payload, token.value());
    let frame = WsFrame {
        opcode: WsOpcode::Text,
        payload: payload.to_string().into_bytes(),
    };
    match send_subscribe_frame(
        socket,
        frame,
        url,
        channel,
        &pair,
        mc,
        SubscribeSend::RegisterWhileOpenAuth,
    ) {
        SendOutcome::Sent => {}
        SendOutcome::Backpressured | SendOutcome::WriterDead => return,
    }
    arm_subscribe_ack_timer_with_req_id(mc, channel, pair, req_id, bus_back_ref);
}

/// Register one entry and, on the 0→1 refcount edge, emit the wire SUBSCRIBE.
#[expect(
    clippy::too_many_arguments,
    reason = "shared handles travel in SharedHandles; `&mut` reactor state is passed explicitly"
)]
pub(in crate::dispatch::io_reactor) fn register_one_and_emit(
    url: WsUrl,
    entry: crate::conn::subscription_registry::SubscriptionEntry,
    ref_id: Option<crate::dispatch::HandlerId>,
    conns: &mut HashMap<WsUrl, ManagedConnection>,
    registry: &mut SubscriptionRegistry,
    sockets: &mut HashMap<WsUrl, Arc<dyn WsSocket>>,
    handles: SharedHandles<'_>,
    upgrade_guards: &mut HashMap<WsUrl, SubscriberGuard>,
) {
    let SharedHandles {
        auth_stack,
        order_gate,
        bus_back_ref,
        ..
    } = handles;
    let channel = entry.channel;
    let pair = entry.pair.clone();
    let params = entry.params;
    let now = super::bus_clock_now(bus_back_ref);
    let count = registry.register(entry, now, ref_id.is_some());
    // Record the guard ref against the entry's live generation so the guard's
    // later drop releases exactly this lifetime.
    if let Some(id) = ref_id {
        registry.record_guard_ref_for(id, url, channel, &pair);
    }

    if let Some(mc) = conns.get_mut(&url) {
        start_connect_if_idle(mc, url, handles, sockets, upgrade_guards);
    }

    // 0→1 edge gate: a duplicate subscriber emits NO wire frame. A tombstone
    // revival lands here too (termination zeroed the refcount).
    if count == 1
        && let Some(mc) = conns.get_mut(&url)
    {
        mc.forget_snapshot_opt_out_written(channel, &pair);
        if url.requires_ws_token() {
            // Open|Resubscribing send at once; Authenticating with nothing
            // in flight sends too — the signed subscribe IS the auth probe.
            if matches!(
                mc.state(),
                ConnectionState::Open | ConnectionState::Resubscribing
            ) || mc.authenticating_nothing_in_flight()
            {
                send_single_signed_subscribe(
                    url,
                    channel,
                    pair.clone(),
                    params,
                    mc,
                    sockets,
                    auth_stack,
                    bus_back_ref,
                );
            }
        } else {
            send_single_subscribe(
                url,
                channel,
                pair.clone(),
                params,
                mc,
                sockets,
                bus_back_ref,
            );
        }
    }

    // A non-empty auth registry falsifies the bare-order predicate; recompute
    // to clear a stale true.
    if url.requires_ws_token()
        && let Some(mc) = conns.get(&url)
    {
        refresh_send_ready(url, mc, registry, auth_stack, order_gate, bus_back_ref);
    }
}
