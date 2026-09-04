//! Event types, payloads, discriminator enums, and the event envelope.

use crate::build::config_resolver::SourceMap;
use crate::build::knobs::{KnobName, KnobValue};
use crate::dispatch::handler_registry::HandlerId;
use crate::types::{MonotonicInstant, WsUrl};

// EventType: every variant must exist in the event registry enum and have a
// corresponding registry entry.

/// Discriminator for every typed event the SDK emits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum EventType {
    /// The I/O Reactor entered its main loop and is draining caller commands.
    ClientReady,
    /// `close()` completion. The correlated waiter's copy carries `request_id`
    /// Some; the broadcast copy carries `request_id` None.
    ClientClosedEvent,
    /// Terminal client failure (reactor spawn / config / bus init / dead loop) — see [`ClientFailureCause`].
    ClientFailed,
    /// Best-effort signal that the dead-man disarm (`CancelAllOrdersAfter(0)`)
    /// issued during `Client::close()` failed; the close proceeds regardless.
    DeadmanDisarmFailedEvent,

    /// A WS connection — initial dial or reconnect attempt.
    ConnectionConnectingEvent,
    /// A WS connection completed its upgrade/handshake.
    ConnectionOpenEvent,
    /// Emitted when the auth connection reached bare-order send-readiness.
    ConnectionSendReadyEvent,
    /// A WS connection reached terminal closed (see `ClosedReason` / `AckSource`).
    ConnectionClosedEvent,
    /// A previously dropped WS connection re-reached open after reconnect.
    ConnectionReopenedEvent,
    /// A WS connection gave up — retries exhausted or a non-transient cause.
    ConnectionFailedEvent,
    /// Mid-stream drop: an open connection fell to backoff.
    ConnectionDroppedEvent,
    /// Emitted when waiting for the rate-budget window to advance.
    ConnectionRateThrottledEvent,
    /// Every pre-Open transient transition to backoff.
    ConnectionAttemptFailedEvent,

    /// Emitted when the staleness monitor expires (no inbound frame within the
    /// configured window), triggering reconnect.
    WebsocketStaleEvent,

    /// Terminal typed auth failure (revoked credentials / M consecutive handshake
    /// failures / refresh-REST failure). Emitted alongside the generic
    /// `ConnectionFailedEvent` for an auth cause.
    AuthenticationFailedEvent,
    /// Per-failure auth-handshake signal (including token-stale and transient).
    AuthHandshakeFailedEvent,
    /// A Spot WS v2 token refresh succeeded (proactive TTL×0.5 / reactive
    /// force_refresh / manual).
    CredentialRefreshedEvent,
    /// A Spot WS v2 token refresh failed (transient or non-transient).
    CredentialRefreshFailedEvent,
    /// A signed REST call kept getting `EAPI:Invalid nonce` after the fresh-nonce
    /// retry — a co-signer on the same key is using a higher nonce scale. Opt-in
    /// (the `nonce_recovery` knob); never emitted for order placement.
    NoncePoisonedEvent,

    /// Transport-level HTTP→WS upgrade succeeded; correlated via `connection_id`.
    WsUpgradeOk,
    /// Transport-level HTTP→WS upgrade failed; carries the transport error.
    WsUpgradeFailed,
    /// Transport-level socket close observed, with the close code and reason.
    WsClosed,

    /// An internal dispatch queue hit capacity and dropped items; the payload's `queue` says which.
    QueueFullWarning,
    /// A reactor loop (I/O or dispatch) terminated abnormally — see [`LoopFailureCause`].
    LoopFailedEvent,
    /// Emitted when a long-lived subscriber callback exceeds
    /// `slow_callback_threshold_ms`.
    SlowCallbackWarning,
    /// Sampled `io_to_dispatch` queue depth, at the configured interval
    /// (or every 1000 dequeues, whichever first).
    QueueDepthSample,
    /// Per-callback execution latency on every long-lived subscriber invocation.
    CallbackLatency,

    /// Rate-limit observability.
    RateLimitWarning,
    /// A Kraken wire rate-limit rejection was observed (reactive; the counter snaps to cap).
    RateLimitExceededEvent,
    /// A per-pair trading rate counter was LRU-evicted from the scope cache.
    RateLimitCacheEvictionEvent,

    /// Data loss on a subscription stream — dropped frames, CRC mismatch, or decode failure.
    SubscriptionGapEvent,

    /// Book-specific gap event. On CRC32 mismatch the SDK emits this and
    /// `SubscriptionGapEvent` (which carries `dropped_count`) in parallel, so book
    /// callers get a typed event without filtering the generic gap stream.
    OrderBookGapEvent,

    /// A `sequence` jump on a channel-wide account subscription
    /// (`executions`/`balances`) — frames lost mid-connection.
    ChannelGapEvent,

    /// Wire-side subscription termination. Rekeyed to `(channel, pair)`.
    SubscriptionTerminatedEvent,

    /// A registered handler panicked during fan-out; other handlers on the
    /// channel continue.
    HandlerPanicWarning,

    /// Inbound frame arrived for a channel with zero registered handlers.
    /// Rate-limited once per `period` per channel; `count` aggregates over the
    /// window. The `status` channel is exempt (auto-seeded system-status push).
    MessageDroppedNoHandler,

    /// An in-flight WS request was abandoned because the auth connection dropped or
    /// the client closed while pending. Emitted once per pending request
    /// (before `ConnectionDroppedEvent`); no request body.
    WsRequestFailedEvent,

    /// A runtime-mutable knob was successfully changed via `Client::set_knob`.
    ConfigChangedEvent,

    /// Config-source resolution completed at `.build()`. Emitted first on the bus,
    /// before any other event.
    ConfigResolved,

    /// The `find_order_by_cl_ord_id` `OpenOrders + ClosedOrders` walk resolved
    /// (Found / NotPlaced / Unknown). Emitted exactly once per walk, not per-leg.
    OrderReconciliationEvent,

    /// Fires on a definitive wire response (`WireSent` / `WireAccepted` /
    /// `WireError`) for an add/amend/cancel order op.
    OrderSubmittedEvent,

    /// Fires on transport-drop-mid-send for `AddOrder`/`AmendOrder` only (a dropped
    /// cancel placed nothing to reconcile). The wire send started but no definitive
    /// answer arrived, so placement is ambiguous (`last_known_status`).
    OrderPlacementAmbiguousEvent,

    /// Fires when the caller drops an add/amend/cancel order-method `await`
    /// mid-flight (never cancel_all). Cancelling the await does not guarantee the
    /// order wasn't sent; this event lets the caller reconcile.
    OrderCancellationAttempted,

    /// Emitted once per REST transient-retry attempt of an idempotent op (the retry,
    /// not the first try), just before the retried wire send (after backoff, never
    /// for an attempt cancelled during backoff). Broadcast: envelope `request_id`
    /// None; the per-dispatch correlation id rides the payload.
    RestRetryAttempt,
}

/// Per-event payload. Paired with [`EventType`]. Field schema is a
/// cross-binding contract shared across all SDK language ports.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum EventPayload {
    /// Fires when the I/O Reactor has entered its main loop and is
    /// draining `caller_to_io`. Does NOT wait for WS sockets to reach Open
    /// (multi-WS lazy lock).
    ClientReady {
        /// Namespaces and WS URLs declared at `.build()`; connections stay lazy.
        capability_snapshot: crate::types::CapabilitySnapshot,
    },

    /// The client finished closing; carries why and when the close was initiated.
    ClientClosedEvent {
        /// Whether the close was user-initiated or forced by a fatal error.
        reason: ClientCloseReason,
        /// Monotonic instant `close()` was initiated (not when it completed).
        initiated_at_monotonic: MonotonicInstant,
    },

    /// Terminal client failure; `cause` classifies which stage died.
    ClientFailed {
        /// Which stage failed: reactor spawn, config validation, bus init, or a dead loop.
        cause: ClientFailureCause,
    },

    /// The `CancelAllOrdersAfter(0)` dead-man disarm during `Client::close()`
    /// failed; the close completes regardless. `retry_at_monotonic` is `None` in v1
    /// (a wall-clock hint has no lossless conversion to the monotonic clock).
    DeadmanDisarmFailedEvent {
        /// Why the disarm call failed (network / rate limit / timeout).
        cause: DeadmanDisarmCause,
        /// Always `None` in v1 — reserved for a future retry hint.
        retry_at_monotonic: Option<MonotonicInstant>,
        /// Monotonic instant the disarm failure was observed.
        monotonic_ts: MonotonicInstant,
    },

    /// `EventEnvelope.request_id = Some(connection_id)` when initiated by a
    /// caller `connect()`/`force_reconnect()`; `None` for reconnect-after-backoff.
    ConnectionConnectingEvent {
        /// The WS endpoint being dialled.
        url: WsUrl,
        /// Attempts made so far in this connect/reconnect cycle.
        attempt_count: u32,
    },

    /// The WS upgrade/handshake completed.
    ConnectionOpenEvent {
        /// The WS endpoint that was opened.
        url: WsUrl,
        /// Monotonic instant the connection reached open.
        opened_at_monotonic: MonotonicInstant,
        /// Server-assigned connection id, when the server reported one.
        server_connection_id: Option<String>,
    },

    /// `ready_at_monotonic` is the instant send-readiness was reached (parallel to
    /// `opened_at_monotonic`); the envelope `timestamp_monotonic` owns the
    /// emit-stamp.
    ConnectionSendReadyEvent {
        /// The auth WS endpoint that reached send-readiness.
        url: WsUrl,
        /// Monotonic instant bare-order send-readiness was reached.
        ready_at_monotonic: MonotonicInstant,
    },

    /// The connection reached terminal closed; `ack` records how the close handshake ended.
    ConnectionClosedEvent {
        /// The WS endpoint that was closed.
        url: WsUrl,
        /// Monotonic instant the connection reached closed.
        closed_at_monotonic: MonotonicInstant,
        /// Whether the close was client-initiated or forced.
        reason: ClosedReason,
        /// How the close ended: server ack, socket drop, ack timeout, or never opened.
        ack: AckSource,
        /// Server-assigned connection id, when one was learned before the close.
        server_connection_id: Option<String>,
    },

    /// A previously dropped connection re-reached open after reconnect.
    ConnectionReopenedEvent {
        /// The WS endpoint that was reopened.
        url: WsUrl,
        /// Monotonic instant the connection re-reached open.
        reopened_at_monotonic: MonotonicInstant,
        /// Attempts consumed by the reconnect cycle before this reopen.
        attempt_count: u32,
    },

    /// `last_error` is a string description; a full typed error hierarchy
    /// is not yet landed.
    ConnectionFailedEvent {
        /// The WS endpoint that failed.
        url: WsUrl,
        /// Total connection attempts made before giving up.
        attempt_count: u32,
        /// Human-readable description of the last error before the failure.
        last_error: String,
        /// Whether the final error was classified transient (currently always `false`; retry exhaustion reports `RetryCapExhausted`).
        transient: bool,
        /// Classified non-transient cause; `None` when no class applies.
        non_transient_class: Option<NonTransientClass>,
    },

    /// An open connection dropped mid-stream and is heading to backoff.
    ConnectionDroppedEvent {
        /// The WS endpoint that dropped.
        url: WsUrl,
        /// Monotonic instant the drop was observed.
        dropped_at_monotonic: MonotonicInstant,
        /// WebSocket close code, when the peer sent a Close frame.
        close_code: Option<u16>,
        /// Close-frame reason text, when the peer supplied one.
        reason: Option<String>,
    },

    /// Emitted when the connection rate budget is exhausted.
    ConnectionRateThrottledEvent {
        /// The WS endpoint whose connection attempts are being throttled.
        url: WsUrl,
        /// Connection attempts consumed within the current budget window.
        attempts_used: u32,
        /// Configured budget window in seconds (e.g. 600 for the default
        /// 10-minute rolling window). Cross-binding payload field.
        window_seconds: u32,
        /// Attempts left in the current window.
        window_remaining_attempts: u32,
        /// Monotonic instant the budget window advances and dialling may resume.
        throttle_until_monotonic: MonotonicInstant,
    },

    /// Every pre-Open transient transition to backoff.
    ConnectionAttemptFailedEvent {
        /// The WS endpoint the failed attempt targeted.
        url: WsUrl,
        /// Ordinal of the failed attempt within this connect cycle.
        attempt: u32,
        /// Which transient failure source tripped — see `TransientClass`.
        transient_class: TransientClass,
        /// Kraken error string from the handshake, when one was received.
        kraken_error: Option<String>,
        /// HTTP status of a rejected upgrade, when the failure was HTTP-level.
        http_status: Option<u16>,
        /// WebSocket close code, when the failure was a close during the handshake.
        close_code: Option<u16>,
        /// Backoff delay in milliseconds before the next attempt.
        backoff_ms: u64,
        /// Monotonic instant the attempt failed.
        failed_at_monotonic: MonotonicInstant,
    },

    /// Emitted on every staleness teardown arm, whether the teardown recovers
    /// or escalates. `last_inbound_at_monotonic` is read from the staleness monitor.
    WebsocketStaleEvent {
        /// The WS endpoint whose inbound stream went stale.
        url: WsUrl,
        /// Monotonic instant of the last inbound frame before staleness fired.
        last_inbound_at_monotonic: MonotonicInstant,
        /// The configured staleness window in milliseconds that expired.
        configured_window_ms: u32,
    },

    /// Terminal typed auth failure, emitted alongside the generic
    /// `ConnectionFailedEvent` for an auth cause (M-cap / bad-creds /
    /// permission-denied / refresh-REST failure).
    AuthenticationFailedEvent {
        /// The auth WS endpoint that failed.
        url: WsUrl,
        /// Terminal cause: bad creds, permission denied, client error, or retry-cap.
        non_transient_class: NonTransientClass,
        /// Raw Kraken error response, when one was received.
        kraken_response: Option<String>,
    },

    /// Per-failure auth-handshake signal (including token-stale and transient).
    AuthHandshakeFailedEvent {
        /// The auth WS endpoint whose handshake failed.
        url: WsUrl,
        /// The connection's overall attempt tally at this failure — not the consecutive-auth-fail counter.
        attempt: u32,
        /// Whether this failure was classified transient (retrying continues).
        transient: bool,
        /// Kraken error string from the handshake response, when present.
        kraken_error: Option<String>,
    },

    /// A WS token refresh succeeded. `key_id_fingerprint` is a non-reversible
    /// derivation of the API key — never the raw key.
    CredentialRefreshedEvent {
        /// Non-reversible derivation of the API key — never the raw key.
        key_id_fingerprint: String,
        /// Monotonic instant the fresh WS token expires.
        token_expires_at_monotonic: MonotonicInstant,
        /// What triggered the refresh: proactive TTL, reactive handshake failure, or manual.
        refresh_reason: RefreshReasonPayload,
    },

    /// A WS token refresh failed (transient or non-transient).
    CredentialRefreshFailedEvent {
        /// Non-reversible derivation of the API key — never the raw key.
        key_id_fingerprint: String,
        /// Ordinal of this refresh attempt.
        attempt: u32,
        /// Coarse failure class: creds error, client closed, or unknown/transport.
        error_class: ErrorClass,
        /// Delay in milliseconds before the next retry; `None` when no retry is scheduled.
        retry_in_ms: Option<u64>,
    },

    /// Opt-in nonce-poisoning detection (the `nonce_recovery` knob): a signed REST
    /// call was still rejected `EAPI:Invalid nonce` after the fresh-nonce retry — a
    /// co-signer on the same key is using a higher nonce scale. Rotate or align.
    NoncePoisonedEvent {
        /// Non-reversible derivation of the affected API key — never the raw key.
        key_id_fingerprint: String,
        /// The signed REST endpoint whose call kept hitting the nonce rejection.
        endpoint: String,
    },

    /// `queue` serialises to the string discriminators `"io_to_dispatch"` /
    /// `"caller_to_io"` — the `QueueName` `Display` form.
    QueueFullWarning {
        /// Which queue overflowed.
        queue: QueueName,
        /// Number of items dropped due to the overflow.
        dropped_count: u64,
        /// Configured capacity of the queue that overflowed.
        capacity: usize,
    },

    /// A reactor loop terminated abnormally; the loop is not restarted.
    LoopFailedEvent {
        /// Which loop died: `"io"` or `"dispatch"` on the wire.
        loop_name: ReactorName,
        /// How it died: panic, unhandled error, or cancellation.
        cause: LoopFailureCause,
        /// Monotonic instant the loop terminated.
        failed_at_monotonic: MonotonicInstant,
    },

    /// Emitted when a dispatch-loop callback's execution time met or exceeded
    /// `slow_callback_threshold_ms` — for lifecycle subscribers and WS data
    /// callbacks.
    SlowCallbackWarning {
        /// What was slow: a lifecycle subscriber (`Event`) or a WS data callback
        /// (`DataChannel`). See [`CallbackSource`].
        slow_event_type: CallbackSource,
        /// Observed callback execution time in microseconds.
        latency_us: u64,
        /// The configured `slow_callback_threshold_ms` value that was met or exceeded.
        threshold_ms: u32,
    },

    /// Sampled depth of a dispatch queue. v1 emits `queue = IoToDispatch` only; the
    /// `CallerToIo` discriminator is retained for cross-binding enum stability.
    QueueDepthSample {
        /// Which queue was sampled (v1: always `IoToDispatch`).
        queue: QueueName,
        /// Number of items in the queue at sample time.
        depth: usize,
        /// Configured capacity of the sampled queue.
        capacity: usize,
        /// Monotonic instant the sample was taken.
        sampled_at_monotonic: MonotonicInstant,
    },

    /// Per-callback execution latency. `handler_id` gives per-handler resolution;
    /// `downgrade_to(1)` aggregates by setting it to `None`.
    CallbackLatency {
        /// What ran: a lifecycle subscriber or a WS data callback.
        callback_event_type: CallbackSource,
        /// Per-handler id; `None` after a downgrade to v=1.
        handler_id: Option<HandlerId>,
        /// Callback execution time in microseconds.
        latency_us: u64,
    },

    /// Level-triggered + debounced: emitted while a counter is at/above
    /// `warning_pct` (default 0.80) on either tracker, at most once per key/pair
    /// per 1s. `tracker` discriminator carries `"api"` or `"trading"`.
    RateLimitWarning {
        /// Which tracker crossed the threshold: `"api"` or `"trading"`.
        tracker: &'static str,
        /// Non-reversible SHA-256 fingerprint of the API key — NEVER the raw key.
        key_id_fingerprint: String,
        /// The pair this counter is scoped to, or `None` for the api-key tracker.
        pair: Option<crate::types::Symbol>,
        /// Current counter value.
        used: f64,
        /// Counter ceiling for this tier.
        cap: f64,
        /// `used / cap` at emit time (at or above the configured `warning_pct`).
        pct: f64,
        /// Counter decay rate per second for this tier.
        decay_per_sec: f64,
        /// Seconds for the counter to decay to `0` at `decay_per_sec`
        /// (`used / decay_per_sec`) — time-to-empty, not time-to-full. `INFINITY`
        /// when `decay_per_sec` is non-positive or non-finite (never `NaN`).
        seconds_to_drain: f64,
    },

    /// Reactive-only — emitted when a Kraken wire rate-limit rejection is observed;
    /// the proactive `consume()` path returns `Err(RateLimitExceeded)` instead.
    /// Wire codes: docs/guides/rate-limits.md.
    RateLimitExceededEvent {
        /// Which tracker was snapped to cap: `"api"` or `"trading"`.
        tracker: &'static str,
        /// Non-reversible SHA-256 fingerprint of the API key — NEVER the raw key.
        key_id_fingerprint: String,
        /// The pair this counter is scoped to, or `None` for the api-key tracker.
        pair: Option<crate::types::Symbol>,
        /// The observed Kraken wire error string that triggered this snap (e.g.
        /// `"EAPI:Rate limit exceeded"`). Non-optional — only emitted on a real
        /// wire rejection, so the string is always present.
        kraken_error: String,
        /// Monotonic instant the wire rejection was observed.
        observed_at_monotonic: MonotonicInstant,
    },

    /// LRU eviction on the trading tracker when the scope cache exceeds
    /// `rate_limit_trading_scope_cap`. Payload fields are a cross-binding contract.
    RateLimitCacheEvictionEvent {
        /// Non-reversible SHA-256 fingerprint of the API key — NEVER the raw key.
        key_id_fingerprint: String,
        /// The pair the evicted counter was scoped to (`Some` for the trading tracker).
        pair: Option<crate::types::Symbol>,
        /// Monotonic instant the eviction happened.
        evicted_at_monotonic: MonotonicInstant,
        /// Why the entry was evicted (string discriminator).
        reason: &'static str, // currently "lru_cap_exceeded"
    },

    /// **Correlation:** `EventEnvelope.request_id == connection_id`.
    WsUpgradeOk {
        /// SDK-local connection id; mirrored into the envelope `request_id`.
        connection_id: u64,
        /// The WS endpoint that was upgraded.
        url: WsUrl,
    },

    /// The HTTP→WS upgrade failed at the transport layer.
    WsUpgradeFailed {
        /// SDK-local id of the connection whose upgrade failed.
        connection_id: u64,
        /// The WS endpoint the failed upgrade targeted.
        url: WsUrl,
        /// The transport error that failed the upgrade.
        error: crate::transport::TransportError,
    },

    /// The underlying WS socket closed at the transport layer.
    WsClosed {
        /// SDK-local id of the closed connection.
        connection_id: u64,
        /// The WS endpoint that closed.
        url: WsUrl,
        /// WebSocket close code; `1006` synthesized when none arrived (empty Close / error path).
        code: u16,
        /// Close reason: wire text (may be empty), or the transport-error description on an abnormal close.
        reason: String,
    },

    /// `symbol` is non-optional: a gap is always per-`(channel, symbol)`.
    /// `request_id` is `None` (broadcast) until a correlation key is pinned;
    /// no v1 caller awaits gap events by sub_id.
    SubscriptionGapEvent {
        /// The WS channel the gap occurred on.
        channel: crate::types::ChannelName,
        /// The pair the gapped subscription is keyed to.
        symbol: crate::types::Symbol,
        /// Number of frames dropped in the gap.
        dropped_count: u32,
        /// What produced the gap — see `GapCause`.
        cause: GapCause,
    },

    /// Book-specific flavour — emitted in parallel with `SubscriptionGapEvent` on
    /// CRC32 mismatch (no `dropped_count`; the parallel event carries that).
    OrderBookGapEvent {
        /// The book channel the gap occurred on.
        channel: crate::types::ChannelName,
        /// The pair whose book gapped.
        symbol: crate::types::Symbol,
        /// What produced the gap — see `GapCause`.
        cause: GapCause,
    },

    /// A `sequence` jump on a channel-wide account subscription. No symbol —
    /// a lost frame's symbols are unknowable. Broadcast; no auto-resubscribe.
    ChannelGapEvent {
        /// The account channel whose sequence jumped.
        channel: crate::types::ChannelName,
        /// Frames lost: the observed sequence delta minus one.
        dropped_count: u32,
        /// Always `GapCause::SequenceGapDetected`.
        cause: GapCause,
    },

    /// Rekeyed to `(channel, pair)`; `pair: Option<Symbol>`
    /// (`None` = channel-wide). `cause` is the 4-variant `TerminationCause`.
    SubscriptionTerminatedEvent {
        /// The WS channel the terminated subscription was on.
        channel: crate::types::ChannelName,
        /// The pair the subscription was keyed to; `None` = channel-wide.
        pair: Option<crate::types::Symbol>,
        /// Why it terminated: wire rejection, ack-budget exhaustion, revocation, or client close.
        cause: crate::api::subscription::TerminationCause,
        /// Last error string associated with the termination, when known.
        last_error: Option<String>,
        /// Monotonic instant the termination was observed.
        terminated_at_monotonic: MonotonicInstant,
    },

    /// Handler panic caught during fan-out. `panic_message` is truncated to
    /// ≤1KB at the emit-site.
    HandlerPanicWarning {
        /// What the panicking handler was subscribed to — a lifecycle
        /// `EventType` or a data `ChannelName` (honest, never a fabricated channel).
        source: CallbackSource,
        /// Registry id of the handler that panicked.
        handler_id: HandlerId,
        /// The caught panic payload as text, truncated to ≤1KB.
        panic_message: String,
    },

    /// Inbound frame dropped because `channel` had zero handlers. Rate-limited once
    /// per `period` per channel; `count` aggregates over the window. The `status`
    /// channel is exempt (its auto-seeded system-status push).
    MessageDroppedNoHandler {
        /// The channel whose frames had no registered handler.
        channel: crate::types::ChannelName,
        /// Frames dropped for this channel within the aggregation window.
        count: u32,
        /// The aggregation window `count` covers.
        period: std::time::Duration,
    },

    /// An in-flight WS request was abandoned on connection drop / client close.
    /// Carries no request body — only `req_id`, `op`, `reason`. Emitted once per
    /// pending request.
    WsRequestFailedEvent {
        /// The WS v2 `req_id` of the abandoned request.
        req_id: u64,
        /// Which order op the abandoned request carried.
        op: WsOp,
        /// Coarse drain cause: connection lost or client closed.
        reason: WsFailReason,
    },

    /// Emitted on any successful runtime knob change via `Client::set_knob`.
    /// `previous`/`current` are [`KnobValue`] (the cross-binding `T`).
    ConfigChangedEvent {
        /// The knob that changed.
        knob: KnobName,
        /// Value before the change.
        previous: KnobValue,
        /// Value after the change.
        current: KnobValue,
    },

    /// Emitted first at `.build()` completion, before any other event.
    /// `source_map` keys are knob names (`ConfigKey`).
    ConfigResolved {
        /// Per-knob record of which source (builder / env / file / default) won.
        source_map: SourceMap,
    },

    /// Emitted once when `find_order_by_cl_ord_id`'s `OpenOrders + ClosedOrders`
    /// walk resolves the ambiguity. The same `outcome` is returned synchronously as
    /// the helper's `Result`; passive subscribers mirror it to their own ledger.
    OrderReconciliationEvent {
        /// The client order id the walk searched for.
        cl_ord_id: crate::types::ClOrdId,
        /// Resolution of the walk: Found / NotPlaced / Unknown.
        outcome: crate::api::account::ReconciliationOutcome,
    },

    /// `{ cl_ord_id, amend_id, op, status, request_id }`. Emitted on a definitive wire
    /// response for an add/amend/cancel op ([`OrderSubmitStatus`] carries the
    /// outcome); `amend_id` is `Some` only on an amend that minted one.
    OrderSubmittedEvent {
        /// Client order id of the submitted op.
        cl_ord_id: crate::types::ClOrdId,
        /// `Some` only on an amend that minted a new amend id.
        amend_id: Option<crate::api::trade::AmendId>,
        /// Which order op this wire response answers.
        op: OrderOp,
        /// Definitive wire outcome — see [`OrderSubmitStatus`].
        status: OrderSubmitStatus,
        /// Per-dispatch correlation id (REST UUID / WS req_id, stringified);
        /// payload-carried — the envelope stays `None` (never latched).
        request_id: Option<String>,
    },

    /// Emitted on transport-drop-mid-send for `AddOrder`/`AmendOrder` only. Carries
    /// the fixed literal `last_known_status: "sent_no_response"`, exposed via
    /// [`Self::PLACEMENT_AMBIGUOUS_LAST_KNOWN_STATUS`] rather than stored per-event.
    OrderPlacementAmbiguousEvent {
        /// Client order id of the ambiguous op — the reconciliation key.
        cl_ord_id: crate::types::ClOrdId,
        /// Amend id, when the op was an amend and one was already minted.
        amend_id: Option<crate::api::trade::AmendId>,
        /// Which order op was mid-send when the transport dropped.
        op: OrderOp,
        /// Monotonic instant the wire send started.
        sent_at_monotonic: MonotonicInstant,
        /// Per-dispatch correlation id (REST UUID / WS req_id, stringified).
        request_id: Option<String>,
    },

    /// Emitted when the caller drops an add/amend/cancel order-method await
    /// mid-flight (never cancel_all / cancel_all_orders_after). Carries the fixed literal
    /// `cancellation_source: "caller_initiated"` via [`Self::CANCELLATION_SOURCE_CALLER_INITIATED`].
    OrderCancellationAttempted {
        /// Client order id of the abandoned op — the reconciliation key.
        cl_ord_id: crate::types::ClOrdId,
        /// Which order op's await was dropped.
        op: OrderOp,
        /// Per-dispatch correlation id (REST UUID / WS req_id, stringified).
        request_id: Option<String>,
    },

    /// One per REST transient-retry. `attempt` is the 1-based ordinal of the retry
    /// (the original try is attempt 1, so the first retry carries `attempt == 2`);
    /// `backoff_ms` is the slept delay before it; `reason` is the classified reason.
    RestRetryAttempt {
        /// The REST endpoint being retried.
        endpoint: String,
        /// Per-dispatch correlation id — constant across every attempt of one logical call.
        request_id: Option<String>,
        /// 1-based attempt ordinal; the first retry carries `attempt == 2`.
        attempt: u32,
        /// Backoff delay in milliseconds slept before this retry.
        backoff_ms: u64,
        /// Classified reason the previous attempt is being retried.
        reason: crate::rest::RetryReason,
    },
}

impl EventPayload {
    /// Downgrades the payload to a subscriber's `max_version` before the bus invokes
    /// the handler. All payloads are v=1 except `CallbackLatency` (v=2); its v2→v1
    /// branch drops `handler_id`. All other variants are identity.
    pub fn downgrade_to(self, target_version: u16) -> Self {
        match self {
            EventPayload::CallbackLatency {
                callback_event_type,
                handler_id: _,
                latency_us,
            } if target_version <= 1 => EventPayload::CallbackLatency {
                callback_event_type,
                handler_id: None, // v1 shape: per-handler resolution removed
                latency_us,
            },
            other => other,
        }
    }

    /// The fixed literal `last_known_status` carried by
    /// [`Self::OrderPlacementAmbiguousEvent`] — a constant discriminator surfaced
    /// here for golden-vector / serde fidelity.
    pub const PLACEMENT_AMBIGUOUS_LAST_KNOWN_STATUS: &'static str = "sent_no_response";

    /// The fixed literal `cancellation_source` carried by
    /// [`Self::OrderCancellationAttempted`]. Not a runtime field — a constant
    /// discriminator surfaced here for golden-vector / serde fidelity.
    pub const CANCELLATION_SOURCE_CALLER_INITIATED: &'static str = "caller_initiated";
}

/// Discriminator for the latency subject in `CallbackLatency` / `SlowCallbackWarning`:
/// a lifecycle event-bus subscriber (keyed by its `EventType`) or a WS data
/// callback (keyed by its originating `ChannelName`, e.g. `on_ticker`/`on_book`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CallbackSource {
    /// A lifecycle event-bus subscriber, keyed by the `EventType` it subscribed to.
    Event(EventType),
    /// A WS data callback, keyed by the channel whose decoded frames it consumes.
    DataChannel(crate::types::ChannelName),
}

/// `SubscriptionGapEvent.cause` / `OrderBookGapEvent.cause` /
/// `ChannelGapEvent.cause`. Locked set: `MalformedFrame` = typed-T decode
/// failure; `FrameDropDueToQueueFull` = per-subscription mpsc overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum GapCause {
    /// Per-subscription `DropOldestRing<T>` overflow.
    FrameDropDueToQueueFull,
    /// Order-book CRC32 mismatch.
    OrderBookCrcMismatch,
    /// A per-frame `sequence` jump on a channel-wide account subscription
    /// (`executions` / `balances`); carried by `ChannelGapEvent`.
    SequenceGapDetected,
    /// Typed-T decode failure inside `Callback<JsonValue>`.
    MalformedFrame,
}

/// `ClientFailed.cause`. New variants are additive (`#[non_exhaustive]`) MINOR bumps.
#[allow(clippy::enum_variant_names)] // canonical names; renaming would break the cross-binding contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientFailureCause {
    /// The reactor task could not be spawned — the loop never started.
    ReactorSpawnFailed,
    /// Configuration validation failed during client startup.
    ConfigValidationFailed,
    /// The event bus could not be initialised during client startup.
    BusInitFailed,
    /// A reactor loop that WAS running has since died (panic / unhandled error /
    /// cancel). Carried on `ClientFailed` when a post-loop-death `ready()`
    /// re-entry resolves its await.
    LoopFailed,
}

/// `ClientClosedEvent.reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientCloseReason {
    UserClose,
    FatalError,
}

/// `DeadmanDisarmFailedEvent.cause`. Why the `CancelAllOrdersAfter(0)` disarm
/// during `Client::close()` failed (non-classifiable errors are logged, not
/// emitted). `AuthConnectionFailed` is unreachable on this REST-HMAC path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeadmanDisarmCause {
    /// The disarm REST call failed at the network/transport layer.
    NetworkError,
    /// The disarm call was rejected by rate limiting.
    RateLimitExceeded,
    /// Auth-connection failure — unreachable on this REST-HMAC path (kept for contract parity).
    AuthConnectionFailed,
    /// The disarm call timed out.
    Timeout,
}

/// `ConnectionClosedEvent.reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClosedReason {
    ClientInitiated,
    Forced,
}

/// `ConnectionClosedEvent.ack`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AckSource {
    /// Peer sent a Close frame; the close was acknowledged.
    Server,
    /// Peer disconnected without a Close frame (TCP RST / abnormal close)
    /// (a socket once existed).
    SocketDropped,
    /// Local close but peer didn't ack within the close-timeout window.
    CloseTimeout,
    /// No socket was ever opened (distinct from `SocketDropped`).
    NeverOpened,
}

/// `ConnectionFailedEvent.non_transient_class` /
/// `AuthenticationFailedEvent.non_transient_class`. Classifies the non-transient
/// cause (BadCreds / PermissionDenied / ClientError / RetryCapExhausted).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NonTransientClass {
    /// `EAPI:Invalid key` / `EAPI:Invalid signature` — bad credentials.
    BadCreds,
    /// `EGeneral:Permission denied`.
    PermissionDenied,
    /// Non-creds client-side terminal (e.g. a `force_refresh` REST failure).
    ClientError,
    /// M-consecutive auth-handshake retry cap exhausted.
    RetryCapExhausted,
}

/// `ConnectionAttemptFailedEvent.transient_class`. Classifies pre-Open transient
/// failure sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransientClass {
    /// HTTP upgrade rejected with a transient status (408/409/425/429/503/504/507).
    HttpTransient,
    /// Transport connect error classified transient.
    ConnectError,
    /// Upgrade handshake timed out.
    UpgradeTimeout,
    /// Auth-handshake transient (`EService:*` / unrecognized → default-transient).
    AuthHandshakeTransient,
    /// Server close (non-1008) / abnormal close / staleness during the handshake.
    ClosedDuringHandshake,
    /// Subscribe-ack retry budget exhausted during cold replay.
    SubscribeBudgetExhausted,
    /// Transient token-refresh (GetWebSocketsToken) transport failure during
    /// the auth handshake.
    TokenRefreshTransient,
}

/// `CredentialRefreshFailedEvent.error_class`. Classifies the auth error from a
/// failed `GetWebSocketsToken` fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorClass {
    /// Invalid key / signature / nonce / permission / token-stale — creds error.
    Auth,
    /// Client closed mid-fetch.
    ClientClosed,
    /// Token-refresh transport failure.
    Network,
    /// Anything else.
    Unknown,
}

/// `CredentialRefreshedEvent.refresh_reason`. Mapping: `Scheduled → Proactive`,
/// `AuthHandshakeFailed → Reactive`, `Manual → Manual`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RefreshReasonPayload {
    Proactive,
    Reactive,
    Manual,
}

impl From<crate::auth::RefreshReason> for RefreshReasonPayload {
    fn from(r: crate::auth::RefreshReason) -> Self {
        use crate::auth::RefreshReason::*;
        match r {
            Scheduled => RefreshReasonPayload::Proactive,
            AuthHandshakeFailed => RefreshReasonPayload::Reactive,
            Manual => RefreshReasonPayload::Manual,
        }
    }
}

/// `QueueFullWarning.queue` discriminator. Serialises to the string values
/// `"io_to_dispatch"` / `"caller_to_io"`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum QueueName {
    CallerToIo,
    IoToDispatch,
}

/// `LoopFailedEvent.loop_name` discriminator. Serialises to the string values
/// `"io"` / `"dispatch"`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum ReactorName {
    /// The I/O reactor loop — wire string `"io"`.
    Io,
    /// The dispatch reactor loop — wire string `"dispatch"`.
    Dispatch,
}

/// `LoopFailedEvent.cause`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LoopFailureCause {
    /// The loop task panicked.
    Panic,
    /// The loop exited on an error it could not handle.
    UnhandledError,
    /// The loop task was cancelled while still expected to run.
    Cancelled,
}

/// WS request op discriminator carried on [`EventPayload::WsRequestFailedEvent`].
/// Maps 1:1 to the WS v2 request `method` string — the snake_case string form
/// renders exactly that method.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum WsOp {
    /// `add_order` — order placement (buy/sell share the wire method).
    AddOrder,
    /// `amend_order` — in-place amend.
    AmendOrder,
    /// `cancel_order` — cancel a single order.
    CancelOrder,
    /// `cancel_all` — cancel every open order.
    CancelAll,
    /// `cancel_all_orders_after` — dead-man's-switch.
    CancelAllOrdersAfter,
    /// `batch_add` — place 2-15 orders in one frame (per-row results).
    BatchAdd,
}

impl WsOp {
    /// Returns `true` if this is an order method. Used to admit a bare order as
    /// the auth probe. Every current `WsOp` is an order method; the predicate is
    /// explicit so a future non-order op is excluded.
    pub fn is_order_method(self) -> bool {
        matches!(
            self,
            WsOp::AddOrder
                | WsOp::AmendOrder
                | WsOp::CancelOrder
                | WsOp::CancelAll
                | WsOp::CancelAllOrdersAfter
                | WsOp::BatchAdd
        )
    }

    /// Parse a WS v2 request `method` string into a [`WsOp`]. Returns `None` for any
    /// method that is not a trade request op (e.g. `subscribe`, data frames).
    pub fn from_method(method: &str) -> Option<Self> {
        match method {
            "add_order" => Some(WsOp::AddOrder),
            "amend_order" => Some(WsOp::AmendOrder),
            "cancel_order" => Some(WsOp::CancelOrder),
            "cancel_all" => Some(WsOp::CancelAll),
            "cancel_all_orders_after" => Some(WsOp::CancelAllOrdersAfter),
            "batch_add" => Some(WsOp::BatchAdd),
            _ => None,
        }
    }
}

/// Transport-neutral order-op discriminator on the order-lifecycle events.
/// Buy and sell both collapse into `AddOrder`. Mirrors `WsOp`'s variants so the
/// `From<OrderOp>` bridge is 1:1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum OrderOp {
    /// Order placement (buy/sell collapse here).
    AddOrder,
    /// In-place amend.
    AmendOrder,
    /// Cancel a single order by `cl_ord_id`.
    CancelOrder,
    /// Cancel every open order.
    CancelAll,
    /// Dead-man's-switch — cancel all after a timeout.
    CancelAllOrdersAfter,
    /// Batch placement — 2-15 orders in one frame, per-row results.
    OrderBatch,
}

impl From<OrderOp> for WsOp {
    /// 1:1 bridge — every [`OrderOp`] has the same-named [`WsOp`].
    fn from(op: OrderOp) -> Self {
        match op {
            OrderOp::AddOrder => WsOp::AddOrder,
            OrderOp::AmendOrder => WsOp::AmendOrder,
            OrderOp::CancelOrder => WsOp::CancelOrder,
            OrderOp::CancelAll => WsOp::CancelAll,
            OrderOp::CancelAllOrdersAfter => WsOp::CancelAllOrdersAfter,
            OrderOp::OrderBatch => WsOp::BatchAdd,
        }
    }
}

/// `OrderSubmittedEvent.status`: `WireSent { txid }` = placement accepted (txid
/// minted); `WireAccepted` = amend/cancel accepted (no txid); `WireError { code }`
/// = wire rejection (`code` is the classified error's screaming-snake string).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum OrderSubmitStatus {
    /// The wire accepted the op and returned a `TxId`.
    WireSent {
        /// The exchange-minted transaction id for the placed order.
        txid: crate::types::TxId,
    },
    /// Amend/cancel accepted by the wire — no txid is minted.
    WireAccepted,
    /// The wire rejected the op.
    WireError {
        /// The classified error code in screaming-snake form.
        code: String,
    },
}

/// `WsRequestFailedEvent.reason`. The coarse drain cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WsFailReason {
    /// The auth connection dropped (server/abnormal close, staleness,
    /// force-reconnect) with the request in flight.
    ConnectionLost,
    /// The client was closed while the request was in flight.
    ClientClosed,
}

/// Envelope every event flows through. The payload field schema is a
/// cross-binding contract shared across all SDK language ports.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct EventEnvelope {
    /// Discriminator matching the `payload` variant.
    pub event_type: EventType,
    /// Payload schema version; bumped on breaking payload changes.
    pub event_version: u16,
    /// Emit instant on the I/O loop's monotonic clock — not wall clock.
    pub timestamp_monotonic: MonotonicInstant,
    /// Correlation key for `subscribe_correlated`. `None` for broadcast events.
    pub request_id: Option<u64>,
    /// The typed per-event payload.
    pub payload: EventPayload,
}
