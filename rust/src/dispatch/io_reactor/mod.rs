//! I/O Reactor task body: single-writer owner of connections, subscription
//! registry, and live sockets, driving a `biased` `select!` loop.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

#[cfg(test)]
use serde_json::Value;
use tokio::sync::mpsc;

use crate::api::ws_surface::SubscriberGuard;
use crate::conn::ManagedConnection;
use crate::conn::subscription_registry::SubscriptionRegistry;
use crate::dispatch::event_bus::DispatchEventBus;
use crate::dispatch::handler_registry::HandlerRegistry;
use crate::transport::{TransportError, TransportErrorKind, WsFrame, WsOpcode, WsSocket};
use crate::types::{ChannelName, ConnectionState, WsUrl};

mod frame_routing;
use frame_routing::handle_frame;

mod inbound_dispatch;
mod send_ready;
mod socket_lifecycle;
mod subscribe_send;
mod timer;

#[cfg(test)]
pub(in crate::dispatch::io_reactor) use inbound_dispatch::handle_ws_request_frame;
pub(in crate::dispatch::io_reactor) use send_ready::{refresh_send_ready, store_send_ready};
pub(in crate::dispatch::io_reactor) use socket_lifecycle::{send_single_unsubscribe, write_mirror};
pub(in crate::dispatch::io_reactor) use subscribe_send::{
    send_remaining_signed_subscribes, send_single_subscribe,
};

/// Init bundle moved by value into the spawned reactor task at `Client::ready()`.
pub struct IoReactorInit {
    pub conns: HashMap<WsUrl, ManagedConnection>,
    pub caller_rx: mpsc::Receiver<crate::dispatch::event_bus::CallerInbound>,
    pub registry: SubscriptionRegistry,
    pub auth_stack: Arc<crate::auth::AuthStack>,
    /// Single-writer here; shares its presence mirror with the caller-side `WsSurface`.
    pub handler_registry: HandlerRegistry,
    pub state_mirrors: HashMap<WsUrl, Arc<AtomicU8>>,
    pub bus_back_ref: Weak<DispatchEventBus>,
    /// Snapshot published as `BUS.ClientReady` on the first `select!` iteration.
    pub ready_signal: ReadySignal,
    /// Shared with the supervisor so auto-connect draws `request_id`s from the
    /// same space as caller `connect()`.
    pub connect_id_allocator: Arc<AtomicU64>,
    /// "auth has ≥1 subscription" hint; reactor single-writer.
    pub auth_has_subscriptions: Arc<std::sync::atomic::AtomicBool>,
    /// "auth bare-order send-ready" level flag; reactor single-writer.
    pub auth_send_ready: Arc<std::sync::atomic::AtomicBool>,
}

/// Request-id + capability snapshot for the first-iteration `BUS.ClientReady` emit.
pub struct ReadySignal {
    pub request_id: u64,
    pub capability_snapshot: crate::types::CapabilitySnapshot,
}

/// Upgrade outcome bridged back onto the reactor task so `mc.handle_event(...)`
/// stays single-writer.
enum UpgradeOutcome {
    Ok {
        url: WsUrl,
        connection_id: u64,
    },
    Failed {
        url: WsUrl,
        kind: TransportErrorKind,
        transient: bool,
    },
}

fn upgrade_failure_event(
    kind: TransportErrorKind,
    transient: bool,
) -> crate::conn::managed_connection::FsmEvent {
    match kind {
        TransportErrorKind::HttpUpgradeRejected { status } => {
            crate::conn::managed_connection::FsmEvent::WireUpgradeFailed {
                http_status: status,
            }
        }
        kind => crate::conn::managed_connection::FsmEvent::WireConnectError { kind, transient },
    }
}

/// A queued `WsUpgradeOk` that outlived its socket's close is stale and must be
/// dropped, else the FSM enters Open on a removed socket.
fn upgrade_ok_is_live(
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
    url: WsUrl,
    connection_id: u64,
) -> bool {
    sockets.get(&url).map(|s| s.connection_id()) == Some(connection_id)
}

/// The spawned reactor task body driving the `select!` loop.
pub async fn run(init: IoReactorInit) {
    let IoReactorInit {
        mut conns,
        mut caller_rx,
        mut registry,
        mut handler_registry,
        auth_stack,
        state_mirrors,
        bus_back_ref,
        ready_signal,
        connect_id_allocator,
        auth_has_subscriptions,
        auth_send_ready,
    } = init;

    // Fails in-flight correlated awaits + latches LoopDead on abnormal exit;
    // disarmed on clean exit.
    let mut death_guard = crate::dispatch::event_bus::LoopDeathGuard::new(
        bus_back_ref.clone(),
        crate::dispatch::event_bus::ReactorName::Io,
    );

    let mut sockets: HashMap<WsUrl, Arc<dyn WsSocket>> = HashMap::new();
    // Upgrade one-shot guards; must be removed on every close path that removes
    // from `sockets`.
    let mut upgrade_guards: HashMap<WsUrl, SubscriberGuard> = HashMap::new();

    let mut drop_tracker: HashMap<ChannelName, (crate::types::MonotonicInstant, u32)> =
        HashMap::new();
    let mut malformed_warn_tracker: frame_routing::SuppressWarnTracker = HashMap::new();

    // Set by a `ClientClose` marker; once BOTH conns reach `Closed`, emit one
    // ClientClosedEvent and self-exit.
    let mut pending_client_close: Option<(u64, crate::types::MonotonicInstant)> = None;

    let (upgrade_tx, mut upgrade_rx) = mpsc::channel::<UpgradeOutcome>(64);

    // force_refresh outcomes bridged onto this task (single-writer FSM); not
    // bus-routed — unregistered emit is disallowed.
    let (refresh_tx, mut refresh_rx) = mpsc::channel::<crate::auth::RefreshOutcome>(16);

    auth_stack
        .token_lifecycle()
        .set_refresh_tx(refresh_tx.clone());

    // Emit ClientReady FIRST so subscribe_correlated awaits resolve before any
    // other event.
    if let Some(bus) = bus_back_ref.upgrade() {
        let env = crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::ClientReady,
            event_version: 1,
            timestamp_monotonic: bus.clock().now(),
            request_id: Some(ready_signal.request_id),
            payload: crate::dispatch::EventPayload::ClientReady {
                capability_snapshot: ready_signal.capability_snapshot,
            },
        };
        // Resolve the ready() waiter DIRECTLY so its completion never depends
        // on the ring drain.
        bus.deliver_correlated(&env);
        bus.publish(crate::dispatch::EventEnvelope {
            request_id: None,
            ..env
        });
    }

    let mut last_liveness_epoch = registry.liveness_epoch();
    loop {
        let Some(bus) = bus_back_ref.upgrade() else {
            tracing::debug!(target: "kraken_sdk::io_reactor", "bus dropped; reactor exiting");
            break;
        };
        let reserve = usize::from(!caller_rx.is_empty());
        bus.flush_pending_teardowns_leaving(reserve);
        drop(bus);

        // Liveness-epoch sync: re-derive the cached auth hints here, exactly once,
        // after ANY registry-liveness mutation.
        if registry.liveness_epoch() != last_liveness_epoch {
            last_liveness_epoch = registry.liveness_epoch();
            inbound_dispatch::refresh_auth_send_ready_hint(
                &conns,
                &registry,
                &auth_stack,
                &auth_has_subscriptions,
                &auth_send_ready,
                &bus_back_ref,
            );
        }

        tokio::select! {
            biased;

            inbound = caller_rx.recv() => {
                match inbound {
                    // Intercepted here (not handle_inbound): the post-select check fires
                    // one ClientClosedEvent once BOTH conns reach Closed.
                    Some(crate::dispatch::CallerInbound::ClientClose { request_id, initiated_at }) => {
                        pending_client_close = Some((request_id, initiated_at));
                        // ONE marker arms the whole client close; fan CallClose to every
                        // connection so a dropped marker can't leave a half-closed wedge.
                        for url in [WsUrl::Public, WsUrl::Auth] {
                            let per_conn_id = connect_id_allocator.fetch_add(1, Ordering::Relaxed);
                            inbound_dispatch::handle_inbound(
                                crate::dispatch::CallerInbound::FsmEvent {
                                    url,
                                    event: crate::types::CallerEvent::Close {
                                        request_id: per_conn_id,
                                    },
                                },
                                &mut conns,
                                &mut registry,
                                &mut handler_registry,
                                &mut sockets,
                                &state_mirrors,
                                &bus_back_ref,
                                &upgrade_tx,
                                &connect_id_allocator,
                                &auth_stack,
                                &auth_send_ready,
                                &mut upgrade_guards,
                            )
                            .await;
                        }
                        tracing::debug!(
                            target: "kraken_sdk::io_reactor",
                            request_id,
                            "client-close marker: fanned CallClose to all connections; awaiting all Closed"
                        );
                    }
                    Some(msg) => inbound_dispatch::handle_inbound(
                        msg,
                        &mut conns,
                        &mut registry,
                        &mut handler_registry,
                        &mut sockets,
                        &state_mirrors,
                        &bus_back_ref,
                        &upgrade_tx,
                        &connect_id_allocator,
                        &auth_stack,
                        &auth_send_ready,
                        &mut upgrade_guards,
                    ).await,
                    None => {
                        tracing::debug!(target: "kraken_sdk::io_reactor", "caller channel closed; reactor exiting");
                        break;
                    }
                }
            }

            Some(outcome) = upgrade_rx.recv() => {
                match outcome {
                    UpgradeOutcome::Ok { url, connection_id } => {
                        if !upgrade_ok_is_live(&sockets, url, connection_id) {
                            tracing::debug!(
                                target: "kraken_sdk::io_reactor",
                                ?url, connection_id,
                                "stale WsUpgradeOk (connection_id no longer live); dropping"
                            );
                        } else if let Some(mc) = conns.get_mut(&url) {
                            mc.handle_event(
                                crate::conn::managed_connection::FsmEvent::WireUpgradeOk {
                                    connection_id: Some(connection_id),
                                },
                            );
                            write_mirror(&state_mirrors, url, mc.state());

                            // Reconnect resubscribe-replay. Gated on has_been_open() so the
                            // initial-connect Register path (and auth->Authenticating) don't re-sub.
                            if mc.state() == ConnectionState::Resubscribing
                                && mc.has_been_open()
                            {
                                let frames = registry.compose_subscribe_frames_for_url(url);
                                if let Some(socket) = sockets.get(&url) {
                                    let timeout_ms = bus_back_ref
                                        .upgrade()
                                        .map(|bus| bus.knobs().subscribe_ack_timeout_ms.load(Ordering::Relaxed))
                                        .unwrap_or(5_000);
                                    for (channel, pair, mut payload) in frames {
                                        // Correlate a per-frame req_id so a channel-less
                                        // reject on replay terminates cleanly.
                                        let req_id = mc.next_subscribe_req_id();
                                        crate::conn::subscription_registry::stamp_req_id(
                                            &mut payload,
                                            req_id,
                                        );
                                        let frame = WsFrame {
                                            opcode: WsOpcode::Text,
                                            payload: payload.to_string().into_bytes(),
                                        };
                                        match subscribe_send::send_subscribe_frame(
                                            socket,
                                            frame,
                                            url,
                                            channel,
                                            &pair,
                                            mc,
                                            subscribe_send::SubscribeSend::ResubscribeReplay,
                                        ) {
                                            subscribe_send::SendOutcome::Sent => {}
                                            subscribe_send::SendOutcome::Backpressured => {
                                                // Armed resend owns the retry; stop replaying
                                                // if the teardown left Resubscribing.
                                                if mc.state() != ConnectionState::Resubscribing {
                                                    break;
                                                }
                                                continue;
                                            }
                                            subscribe_send::SendOutcome::WriterDead => {
                                                // Reconnect via BackingOff, not Open over a
                                                // dead socket.
                                                break;
                                            }
                                        }
                                        let due_at = crate::types::MonotonicInstant(
                                            mc.clock_now().0
                                                + std::time::Duration::from_millis(
                                                    timeout_ms as u64,
                                                ),
                                        );
                                        // A book still resyncing needs its liveness timer re-armed,
                                        // else a lost fresh snapshot re-freezes it silently.
                                        let arm_reseed = channel == ChannelName::Book
                                            && registry.is_book_resyncing(channel, &pair);
                                        mc.record_subscribe_req_id(req_id, channel, pair.clone());
                                        mc.arm_subscribe_ack(channel, pair.clone(), due_at);
                                        if arm_reseed {
                                            mc.arm_book_reseed_snapshot(channel, pair, due_at);
                                        }
                                    }
                                }
                                // No acks arrive if nothing was sent; short-circuit
                                // Resubscribing → Open (no-op if any timer armed).
                                mc.complete_resubscribe_if_empty();
                                write_mirror(&state_mirrors, url, mc.state());
                                // A WriterClosed replay send may have cleared the socket;
                                // reconcile the reactor socket map.
                                let cur_cid = mc.socket().map(|s| s.connection_id());
                                socket_lifecycle::project_socket_and_wire_bridge(
                                    mc,
                                    url,
                                    cur_cid,
                                    &mut sockets,
                                    &mut upgrade_guards,
                                    &bus_back_ref,
                                    &upgrade_tx,
                                );
                            }

                            if mc.state() == ConnectionState::Authenticating {
                                subscribe_send::send_first_signed_subscribe(
                                    url, mc, &registry, &sockets, &auth_stack,
                                    &auth_send_ready, &bus_back_ref,
                                );
                                write_mirror(&state_mirrors, url, mc.state());
                            }
                        }
                    }
                    UpgradeOutcome::Failed { url, kind, transient } => {
                        if let Some(mc) = conns.get_mut(&url) {
                            mc.handle_event(upgrade_failure_event(kind, transient));
                            write_mirror(&state_mirrors, url, mc.state());
                            if mc.socket().is_none() {
                                sockets.remove(&url);
                                upgrade_guards.remove(&url);
                            }
                        }
                    }
                }
            }

            Some(outcome) = refresh_rx.recv() => {
                let crate::auth::RefreshOutcome { request_id, result } = outcome;
                // v1: the token-bearing connection is ALWAYS Auth, so the correlation
                // event hard-routes there.
                let url = WsUrl::Auth;
                if let Some(mc) = conns.get_mut(&url) {
                    // A proactive tick's outcome (request_id 0) drives the FSM only in
                    // the drain-bearing states — never a handshake's terminal refresh arms.
                    let deliver = request_id != 0
                        || matches!(
                            mc.state(),
                            ConnectionState::Open | ConnectionState::Resubscribing
                        );
                    if deliver {
                        let fsm_event = match &result {
                            Ok(_) => crate::conn::managed_connection::FsmEvent::BusTokenRefreshed {
                                request_id,
                            },
                            Err(e) => {
                                crate::conn::managed_connection::FsmEvent::BusTokenRefreshFailed {
                                    request_id,
                                    error: e.clone(),
                                }
                            }
                        };
                        mc.handle_event(fsm_event);
                        write_mirror(&state_mirrors, url, mc.state());
                        // Re-issue the first signed subscribe mid-handshake — unless ANY
                        // probe is in flight (its own ack drives the handshake).
                        if result.is_ok() && mc.authenticating_nothing_in_flight() {
                            subscribe_send::send_first_signed_subscribe(
                                url, mc, &registry, &sockets, &auth_stack,
                                &auth_send_ready, &bus_back_ref,
                            );
                            write_mirror(&state_mirrors, url, mc.state());
                        }
                    }
                    // Drain deferred auth subscribes: re-issue each in Open OR Resubscribing.
                    if result.is_ok()
                        && matches!(
                            mc.state(),
                            ConnectionState::Open | ConnectionState::Resubscribing
                        )
                        && !mc.awaiting_token_refresh()
                    {
                        let deferred = mc.drain_deferred_open_auth_subscribes();
                        for (channel, pair, params) in deferred {
                            // Skip removed/tombstoned keys and in-flight frames. An
                            // Acked-last-session entry MUST re-issue — its subscription died with the socket.
                            if registry.params_for(channel, &pair).is_none()
                                || registry.is_terminated(channel, &pair)
                                || mc.is_subscribe_ack_armed(channel, pair.clone())
                            {
                                continue;
                            }
                            subscribe_send::send_single_signed_subscribe(
                                url, channel, pair, params, mc, &sockets, &auth_stack,
                                &bus_back_ref,
                            );
                            // Stop draining if that send's teardown left the drainable states.
                            if !matches!(
                                mc.state(),
                                ConnectionState::Open | ConnectionState::Resubscribing
                            ) {
                                break;
                            }
                        }
                        write_mirror(&state_mirrors, url, mc.state());
                    }
                    // Cold-start unblock: recompute + emit send-ready now the lazy first
                    // token fetch landed.
                    refresh_send_ready(url, mc, &registry, &auth_stack, &auth_send_ready, &bus_back_ref);
                    // Only a SUCCESS realigns the proactive timer; a FAILURE must not
                    // (the stale instant is already past — hot refetch loop).
                    if result.is_ok() || auth_stack.cached_token().is_none() {
                        rearm_token_refresh(mc, &auth_stack);
                    }
                    match mc.socket() {
                        Some(s) => { sockets.insert(url, Arc::clone(s)); }
                        None => { sockets.remove(&url); upgrade_guards.remove(&url); }
                    }
                }
            }

            (url, frame_result) = read_frame_from_any(&sockets) => {
                handle_frame(
                    url,
                    frame_result,
                    &mut conns,
                    &mut registry,
                    &handler_registry,
                    &auth_stack,
                    &auth_send_ready,
                    &bus_back_ref,
                    &mut drop_tracker,
                    &mut malformed_warn_tracker,
                    &mut sockets,
                    &state_mirrors,
                    &mut upgrade_guards,
                    &upgrade_tx,
                ).await;
            }

            (url, fsm_event) = timer::next_timer_arm(&conns) => {
                if let Some(mc) = conns.get_mut(&url) {
                    // Timers MUST be popped BEFORE FSM dispatch: an event with no FSM arm
                    // would otherwise re-fire in a tight loop.
                    timer::pop_timer_for_event(mc, &fsm_event);

                    // Reactor-side resend, intercepted BEFORE FSM dispatch (no FSM arm
                    // for this timer).
                    if let crate::conn::managed_connection::FsmEvent::TimerSubscribeResendDue {
                        channel, pair,
                    } = &fsm_event
                    {
                        let channel = *channel;
                        let pair = pair.clone();
                        subscribe_send::handle_subscribe_resend(
                            url, channel, pair.clone(),
                            mc, &registry, &sockets,
                            &auth_stack, &bus_back_ref,
                        );
                        // The resend CAN transition state (token-miss defer completes → Open);
                        // this write_mirror is that transition's only projection.
                        write_mirror(&state_mirrors, url, mc.state());
                        let cur_cid = mc.socket().map(|s| s.connection_id());
                        socket_lifecycle::project_socket_and_wire_bridge(
                            mc, url, cur_cid,
                            &mut sockets, &mut upgrade_guards,
                            &bus_back_ref, &upgrade_tx,
                        );
                        continue;
                    }

                    // Reactor-side reseed snapshot-liveness timer, intercepted BEFORE FSM
                    // dispatch (not an FSM event).
                    if let crate::conn::managed_connection::FsmEvent::TimerBookReseedSnapshot {
                        pair,
                        ..
                    } = &fsm_event
                    {
                        let pair = pair.clone();
                        frame_routing::handle_book_reseed_timeout(
                            url,
                            pair,
                            mc,
                            &mut registry,
                            &sockets,
                            &bus_back_ref,
                        );
                        // A WriterClosed resubscribe send may have driven → BackingOff;
                        // mirror + project.
                        write_mirror(&state_mirrors, url, mc.state());
                        let cur_cid = mc.socket().map(|s| s.connection_id());
                        socket_lifecycle::project_socket_and_wire_bridge(
                            mc,
                            url,
                            cur_cid,
                            &mut sockets,
                            &mut upgrade_guards,
                            &bus_back_ref,
                            &upgrade_tx,
                        );
                        continue;
                    }

                    // Reactor-side proactive token-refresh tick, intercepted BEFORE FSM
                    // dispatch.
                    if let crate::conn::managed_connection::FsmEvent::TimerTokenRefreshDue =
                        &fsm_event
                    {
                        debug_assert_eq!(
                            url,
                            WsUrl::Auth,
                            "proactive token-refresh timer fired on a non-auth MC"
                        );
                        if !matches!(mc.state(), ConnectionState::Closed | ConnectionState::Failed) {
                            auth_stack.proactive_refresh();
                            // Arm off `now`, never the stale token's past-due instant (hot-loop).
                            let due = crate::types::MonotonicInstant(
                                mc.clock_now().0
                                    + crate::auth::token_lifecycle::WS_TOKEN_REFRESH_AT,
                            );
                            mc.arm_token_refresh(due);
                        }
                        continue;
                    }

                    // Capture prev_cid BEFORE dispatch: a timer-driven backoff reconnect
                    // opens a fresh socket inside handle_event.
                    let prev_cid = mc.socket().map(|s| s.connection_id());
                    mc.handle_event(fsm_event);
                    write_mirror(&state_mirrors, url, mc.state());
                    socket_lifecycle::project_socket_and_wire_bridge(
                        mc, url, prev_cid,
                        &mut sockets, &mut upgrade_guards,
                        &bus_back_ref, &upgrade_tx,
                    );
                }
            }
        }

        // Once the ClientClose marker is set AND every conn is == Closed
        // (Failed/Idle don't count), emit one ClientClosedEvent and self-exit.
        if let Some((request_id, initiated_at)) = pending_client_close {
            if conns
                .values()
                .all(|mc| mc.state() == ConnectionState::Closed)
            {
                if let Some(bus) = bus_back_ref.upgrade() {
                    let env = crate::dispatch::EventEnvelope {
                        event_type: crate::dispatch::EventType::ClientClosedEvent,
                        event_version: 1,
                        timestamp_monotonic: bus.clock().now(),
                        request_id: Some(request_id),
                        payload: crate::dispatch::EventPayload::ClientClosedEvent {
                            reason: crate::dispatch::ClientCloseReason::UserClose,
                            initiated_at_monotonic: initiated_at,
                        },
                    };
                    // Resolve the close() waiter DIRECTLY so its completion never depends
                    // on the ring drain.
                    bus.deliver_correlated(&env);
                    // The broadcast copy carries request_id: None — a Some would re-run
                    // correlation and latch a dead miss-buffer entry.
                    bus.publish(crate::dispatch::EventEnvelope {
                        request_id: None,
                        ..env
                    });
                    bus.close_dispatch_and_drain().await;
                }
                tracing::debug!(
                    target: "kraken_sdk::io_reactor",
                    request_id,
                    "client close complete (all connections Closed); reactor self-exiting"
                );
                break;
            }
        }
    }

    death_guard.disarm();
}

/// (Re)arm the proactive token-refresh timer at the cached token's
/// `refresh_due_at()`, or disarm if none is cached.
fn rearm_token_refresh(mc: &mut ManagedConnection, auth_stack: &Arc<crate::auth::AuthStack>) {
    match auth_stack.cached_token() {
        Some(tok) => mc.arm_token_refresh(tok.refresh_due_at()),
        None => mc.disarm_token_refresh(),
    }
}

/// Race `recv_frame()` across the fixed two-slot v1 topology (Public + Auth);
/// an absent socket's arm pends forever.
async fn read_frame_from_any(
    sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
) -> (WsUrl, Result<WsFrame, TransportError>) {
    // Adding a WsUrl variant must fail compilation here: the race below polls
    // exactly these arms, so a new socket key would silently never be read.
    const _: fn(WsUrl) = |u| match u {
        WsUrl::Public | WsUrl::Auth => (),
    };
    async fn recv_or_pend(
        sockets: &HashMap<WsUrl, Arc<dyn WsSocket>>,
        url: WsUrl,
    ) -> (WsUrl, Result<WsFrame, TransportError>) {
        match sockets.get(&url) {
            Some(socket) => (url, socket.recv_frame().await),
            None => std::future::pending().await,
        }
    }
    tokio::select! {
        out = recv_or_pend(sockets, WsUrl::Public) => out,
        out = recv_or_pend(sockets, WsUrl::Auth) => out,
    }
}

#[cfg(test)]
pub(crate) mod latency_histogram; // pub(crate): the dispatch-loop probe is stamped from event_bus too
#[cfg(test)]
mod tests;
