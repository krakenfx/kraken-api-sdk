//! Bare-order send-ready flag helpers for the I/O Reactor: `refresh_send_ready`
//! emits `ConnectionSendReadyEvent` when true; `store_send_ready` stores without emitting.

use std::sync::{Arc, Weak};

use crate::conn::ManagedConnection;
use crate::conn::subscription_registry::SubscriptionRegistry;
use crate::dispatch::event_bus::DispatchEventBus;
use crate::types::{ConnectionState, WsUrl};

/// Emit `ConnectionSendReadyEvent` iff a private connection is bare-order
/// send-ready; not for `Open`. Idempotent; call after the state transition settles.
pub(in crate::dispatch::io_reactor) fn refresh_send_ready(
    url: WsUrl,
    mc: &ManagedConnection,
    registry: &SubscriptionRegistry,
    auth_stack: &Arc<crate::auth::AuthStack>,
    order_gate: &Arc<crate::conn::OrderGate>,
    bus_weak: &Weak<DispatchEventBus>,
) {
    if !store_send_ready(url, mc, registry, auth_stack, order_gate) {
        return;
    }
    if let Some(bus) = bus_weak.upgrade() {
        let now = bus.clock().now();
        bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::ConnectionSendReadyEvent,
            event_version: 1,
            timestamp_monotonic: now,
            request_id: None,
            payload: crate::dispatch::EventPayload::ConnectionSendReadyEvent {
                url,
                ready_at_monotonic: now,
            },
        });
    }
}

/// Store-only sibling of [`refresh_send_ready`]: recompute + store WITHOUT
/// emitting the edge event — used where the token-refresh arm owns the re-emit.
pub(in crate::dispatch::io_reactor) fn store_send_ready(
    url: WsUrl,
    mc: &ManagedConnection,
    registry: &SubscriptionRegistry,
    auth_stack: &Arc<crate::auth::AuthStack>,
    order_gate: &Arc<crate::conn::OrderGate>,
) -> bool {
    let now = mc.clock_now();
    let send_ready = url.requires_ws_token()
        && mc.state() == ConnectionState::Authenticating
        && !registry.has_url_entry(url)
        && matches!(auth_stack.cached_token(), Some(t) if !t.is_expired(now));
    order_gate.set_send_ready(url, send_ready);
    send_ready
}
