//! Opaque handle tokens, caller events, capability snapshot, and queue backpressure error.

use super::WsUrl;

/// Caller-initiated event posted to the connection supervisor via the
/// `caller_to_io` queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallerEvent {
    /// Request the managed connection to start connecting.
    StartConnect {
        /// Supervisor-allocated id echoed as `EventEnvelope.request_id` on the completion event.
        request_id: u64,
    },
    /// Request a graceful close of the managed connection.
    Close {
        /// Supervisor-allocated id echoed as `EventEnvelope.request_id` on the completion event.
        request_id: u64,
    },
    /// Request a reconnect of the managed connection: clean close, then redial (rate-budgeted).
    // Consumed by inbound dispatch; no production constructor until a caller-facing
    // force-reconnect verb lands.
    #[allow(dead_code)]
    ForceReconnect {
        /// Supervisor-allocated id echoed as `EventEnvelope.request_id` on the completion event.
        request_id: u64,
    },
}

/// Token returned by a post-and-correlate operation. The caller observes the
/// matching completion event on the `DispatchEventBus` by filtering on `id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestHandle {
    /// Monotonic id allocated by the supervisor / namespace.
    pub id: u64,
    /// Expected completion-event discriminator the caller should subscribe to.
    pub expected_completion: ExpectedCompletionEvent,
}

/// Discriminator on the completion event a caller awaits. Caller correlates
/// by the `id` field of the accompanying [`RequestHandle`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExpectedCompletionEvent {
    /// Connection reached open. Correlates to `EventType::ConnectionOpenEvent`.
    ConnectionOpen,
    /// Reserved (no v1 producer). Correlates to `EventType::ConnectionClosedEvent`.
    ConnectionClosed,
    /// Reserved (no v1 producer). Correlates to `EventType::ConnectionReopenedEvent`.
    ConnectionReopened,
    /// Reserved (no v1 producer). Correlates to `EventType::ConnectionFailedEvent`.
    ConnectionFailed,
    /// Order-placement completion; resolves via a per-request one-shot, not a bus await.
    OrderAdded,
    /// Order-amend completion; resolves via a per-request one-shot, not a bus await.
    OrderEdited,
    /// Order-cancel completion; resolves via a per-request one-shot, not a bus await.
    OrderCancelled,
    /// Success completion of a channel `subscribe` request; reserved, no v1 producer.
    SubscribeAcknowledged,
    /// Failure completion of a channel `subscribe` request; reserved, no v1 producer.
    SubscribeFailed,
    /// Channel-unsubscribe completion. Correlates to the
    /// `SubscriptionTerminatedEvent`, keyed by `(channel, pair)`.
    SubscriptionTerminated,
    /// `Client::ready` completion. Correlates to `EventType::ClientReady` (success)
    /// or `EventType::ClientFailed` (failure) — both carry the same `request_id`.
    ClientReady,
    /// `Client::close` completion. Correlates to `EventType::ClientClosedEvent`, emitted
    /// once BOTH WS connections have drained and closed.
    ClientClosed,
}

impl ExpectedCompletionEvent {
    /// The matching `EventType` for correlated-subscriber lookup.
    pub const fn to_event_type(self) -> crate::dispatch::EventType {
        use crate::dispatch::EventType;
        match self {
            Self::ConnectionOpen => EventType::ConnectionOpenEvent,
            Self::ConnectionClosed => EventType::ConnectionClosedEvent,
            Self::ConnectionReopened => EventType::ConnectionReopenedEvent,
            Self::ConnectionFailed => EventType::ConnectionFailedEvent,
            // Order ops resolve via per-request one-shot; never key on (ClientFailed, id).
            Self::OrderAdded | Self::OrderEdited | Self::OrderCancelled => EventType::ClientFailed,
            Self::SubscribeAcknowledged | Self::SubscribeFailed => {
                EventType::SubscriptionTerminatedEvent
            }
            Self::SubscriptionTerminated => EventType::SubscriptionTerminatedEvent,
            Self::ClientReady => EventType::ClientReady,
            Self::ClientClosed => EventType::ClientClosedEvent,
        }
    }

    /// Failure-terminal completion event, if distinct — registers a secondary
    /// one-shot so a fault resolves the await instead of hanging.
    pub const fn failure_event_type(self) -> Option<crate::dispatch::EventType> {
        use crate::dispatch::EventType;
        match self {
            Self::ClientReady => Some(EventType::ClientFailed),
            _ => None,
        }
    }
}

/// Snapshot of which namespaces, WS connections, and dispatch keys were
/// configured at `.build()` time. Carried in the `ClientReady` event payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilitySnapshot {
    /// Namespaces enabled at `.build()` time.
    pub declared_namespaces: std::collections::HashSet<NamespaceName>,
    /// WS endpoint URLs configured at `.build()`; the connections themselves are lazy.
    pub declared_ws_urls: std::collections::HashSet<WsUrl>,
    /// Lazily discovered at first use; empty at `.ready()` time.
    pub discovered_at_first_use: std::collections::HashSet<DispatchKeyStub>,
}

/// Closed enum of namespace identifiers. Adding a new namespace is a MAJOR
/// SemVer change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NamespaceName {
    /// `client.market()` — public market data.
    Market,
    /// `client.account()` — balances, orders, positions, ledgers.
    Account,
    /// `client.trade()` — order placement, amend, cancel.
    Trade,
    /// `client.funding.*` — deposits and withdrawals; reserved, no v1 surface.
    Funding,
    /// `client.earn.*` — staking; reserved, no v1 surface.
    Earn,
    /// `client.subaccount.*` — subaccount management; reserved, no v1 surface.
    Subaccount,
    /// WebSocket streaming / subscription namespace.
    Ws,
    /// Futures REST namespaces; reserved, no v1 surface.
    Futures,
    /// Futures WebSocket namespace; reserved, no v1 surface.
    FuturesWs,
}

/// Placeholder for `DispatchKey` in `CapabilitySnapshot` until the full
/// dispatch table-key shape is finalised.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DispatchKeyStub(pub u64);

/// Identity token for a single managed WebSocket connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionHandle {
    /// Endpoint this connection targets; part of the handle's identity.
    pub url: WsUrl,
    /// Monotonic id making the handle unique across connections to the same `url`.
    pub connection_id: u64,
}

/// Token returned by `DispatchEventBus::subscribe`.
#[must_use = "pass this id to unsubscribe()"]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriberHandle {
    /// Bus-allocated id identifying this subscriber to `unsubscribe`.
    pub id: u64,
}

/// Returned when the `caller_to_io` queue is full — the SDK rejects rather than
/// blocks. Treat as backpressure and retry later.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Queue full: {queue} (try again later).")]
pub struct QueueFullError {
    /// Static name of the queue that rejected the event (e.g. `caller_to_io`).
    pub queue: &'static str,
}
