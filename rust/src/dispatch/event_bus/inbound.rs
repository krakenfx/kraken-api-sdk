//! Caller-inbound items and the registry/handler mutation ops carried on the
//! `caller_to_io` channel.

use crate::dispatch::handler_registry::HandlerId;
use crate::types::{CallerEvent, ChannelName, MonotonicInstant, WsUrl};

/// Why a `caller_to_io` post was rejected. A named reason (not a bare `bool`) so
/// the Full-vs-Closed mapping cannot be silently inverted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostReject {
    /// The `caller_to_io` queue was at capacity (back-pressure). The item was
    /// never enqueued.
    Full,
    /// The `caller_to_io` receiver was dropped (reactor gone). The item was
    /// never enqueued.
    Closed,
}

/// What the `caller_to_io` channel carries — each variant is one class of
/// caller-initiated op the I/O reactor drains and dispatches. `Debug` is
/// hand-written (below): `WsRequestFrame` carries a non-`Debug` sender + token.
pub enum CallerInbound {
    /// Caller-initiated FSM event.
    FsmEvent { url: WsUrl, event: CallerEvent },

    /// Client-level graceful-close marker posted ONCE by `Client::close()` with
    /// the client-level `request_id` + close instant. Fires one `ClientClosedEvent`
    /// after BOTH WS reach closed.
    ClientClose {
        request_id: u64,
        initiated_at: MonotonicInstant,
    },

    /// Registry mutation request from the namespace layer. The registry lives
    /// single-writer inside the reactor task.
    RegistryMutation {
        url: WsUrl,
        mutation: RegistryMutationOp,
    },

    /// Handler register / deregister. Keyed by `ChannelName`
    /// (handler storage is per-channel, not per-`WsUrl`).
    HandlerMutation {
        channel: ChannelName,
        op: HandlerMutationOp,
    },

    /// Composite drop posted by `SubscriptionGuard::Drop`: handler id + every
    /// (channel, symbol) scoped, so ONE post deregisters the handler and decrements
    /// each refcount (empty `symbols` = channel-wide) with no partial-teardown leak.
    SubscriptionGuardDrop {
        handler_id: HandlerId,
        channel: ChannelName,
        symbols: Vec<crate::types::Symbol>,
    },

    /// WS request/reply frame. The reactor applies the open gate, records by
    /// `req_id`, injects the token reactor-side (`params` arrive token-free) and
    /// sends on the auth socket.
    WsRequestFrame {
        req_id: u64,
        method: String,
        op: crate::dispatch::WsOp,
        /// Token-free op params; the reactor injects `params.token` at compose
        /// time. NEVER carries a credential on the caller→reactor hop.
        params: serde_json::Value,
        /// Completion oneshot for the recorded request. `Ok` on the matching
        /// response; `Err(ConnectionError)` on the open gate, a compose/send
        /// failure, or a drain.
        completion: tokio::sync::oneshot::Sender<
            Result<crate::conn::managed_connection::WsResponse, crate::error::ConnectionError>,
        >,
    },

    /// Drop `req_id`'s pending entry after its caller-side deadline elapsed.
    /// The reactor removes it silently (no event); no-op if already resolved.
    AbandonWsRequest { req_id: u64 },
}

impl std::fmt::Debug for CallerInbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallerInbound::FsmEvent { url, event } => f
                .debug_struct("FsmEvent")
                .field("url", url)
                .field("event", event)
                .finish(),
            CallerInbound::ClientClose {
                request_id,
                initiated_at,
            } => f
                .debug_struct("ClientClose")
                .field("request_id", request_id)
                .field("initiated_at", initiated_at)
                .finish(),
            CallerInbound::RegistryMutation { url, mutation } => f
                .debug_struct("RegistryMutation")
                .field("url", url)
                .field("mutation", mutation)
                .finish(),
            CallerInbound::HandlerMutation { channel, op } => f
                .debug_struct("HandlerMutation")
                .field("channel", channel)
                .field("op", op)
                .finish(),
            CallerInbound::SubscriptionGuardDrop {
                handler_id,
                channel,
                symbols,
            } => f
                .debug_struct("SubscriptionGuardDrop")
                .field("handler_id", handler_id)
                .field("channel", channel)
                .field("symbols", symbols)
                .finish(),
            CallerInbound::WsRequestFrame {
                req_id, method, op, ..
            } => f
                .debug_struct("WsRequestFrame")
                .field("req_id", req_id)
                .field("method", method)
                .field("op", op)
                // `params` redacted (may carry op data); `completion` is a
                // oneshot sender (no Debug). Token is never present here.
                .field("params", &"<params>")
                .field("completion", &"<oneshot::Sender>")
                .finish(),
            CallerInbound::AbandonWsRequest { req_id } => f
                .debug_struct("AbandonWsRequest")
                .field("req_id", req_id)
                .finish(),
        }
    }
}

/// Handler mutation op — paired with [`CallerInbound::HandlerMutation`].
pub enum HandlerMutationOp {
    /// Register `callback` for `channel` under the pre-allocated `id` (allocated
    /// caller-side so the `HandlerHandle` can be returned synchronously).
    Register {
        id: HandlerId,
        callback: crate::dispatch::handler_registry::HandlerCallback,
    },
    /// Remove the handler with `id`. Idempotent.
    Deregister { id: HandlerId },
}

impl std::fmt::Debug for HandlerMutationOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerMutationOp::Register { id, .. } => f
                .debug_struct("Register")
                .field("id", id)
                .field("callback", &"<HandlerCallback>")
                .finish(),
            HandlerMutationOp::Deregister { id } => {
                f.debug_struct("Deregister").field("id", id).finish()
            }
        }
    }
}

/// Mutation ops on `SubscriptionRegistry`, delivered via
/// `CallerInbound::RegistryMutation`. Subscriptions are atomic per
/// `(channel, pair)` — no extend-an-entry mutation; add/remove is register/deregister.
#[derive(Debug)]
pub enum RegistryMutationOp {
    /// Atomic N-entry register — one caller→reactor message creating N independently
    /// refcounted entries. One post: all N land or the post is rejected whole
    /// (registry untouched), so a mid-batch `QueueFull` can't leak refcounts.
    /// `ref_id` is the registering guard's handler id when the batch backs a
    /// `SubscriptionGuard`; `None` on the bare `subscribe_*` path.
    RegisterBatch {
        entries: Vec<crate::conn::subscription_registry::SubscriptionEntry>,
        ref_id: Option<HandlerId>,
    },
    /// Remove the subscription for `(channel, pair)`. `pair = None` is the
    /// channel-wide entry. Idempotent.
    Deregister {
        channel: ChannelName,
        pair: Option<crate::types::Symbol>,
    },
    /// Atomic N-pair deregister for one channel — teardown mirror of
    /// `RegisterBatch`. One post: all N decrements land or the post is rejected
    /// whole, so a mid-batch `QueueFull` can't leave a wire sub alive with no guard.
    DeregisterBatch {
        channel: ChannelName,
        pairs: Vec<crate::types::Symbol>,
    },
    /// Forced deregister of every entry (`channel = None`) or one channel's,
    /// ignoring refcounts — backs `unsubscribe_all` / `unsubscribe_channel`.
    /// Live entries tear down like refcount-0; tombstones drop silently.
    DeregisterAll { channel: Option<ChannelName> },
}
