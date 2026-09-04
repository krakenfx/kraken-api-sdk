# Events

Subscribe with `client.events().on(EventType::…, callback)` and hold the returned `EventSubscription` guard. Handlers run on the dispatch loop. Keep them short.

Emission is not gated on a metrics flag. The exception is `CallbackLatency` samples for WebSocket data callbacks, which are emitted only while a `CallbackLatency` subscriber is registered.

## The envelope

Every event is an `EventEnvelope`:

| Field | Type | Meaning |
|---|---|---|
| `event_type` | `EventType` | Discriminant used for subscribe. |
| `event_version` | `u16` | Payload shape. Bumped on breaking field changes. Each entry states the shipped version. |
| `timestamp_monotonic` | `MonotonicInstant` | Emit time on the I/O loop's monotonic clock. Not wall clock. Comparable only to other SDK timestamps. |
| `request_id` | `Option<u64>` | `Some` on the copy that resolves an awaited `ready()` / `close()` (or an internal waiter). `None` on broadcast copies. |
| `payload` | `EventPayload` | Typed payload below. |

Per-channel order is preserved. There is no deduplication. Events cross a bounded drop-oldest queue (`io_to_dispatch_capacity`, default 8192). Under saturation an event can be evicted before delivery.

## Reading payload fields

Several payload types are not exported at the crate root (`NonTransientClass`, `TransientClass`, `GapCause`, `ErrorClass`, and similar). Format them for logging. Do not match variants by path.

Unused fields in this release (for example `CredentialRefreshFailedEvent.retry_in_ms`) are noted on that event.

## Client lifecycle and configuration

`ready()` and `close()` completions are delivered to the correlated waiter. Several of these events are not broadcast.

### `ClientReady`

Emitted when the I/O reactor has entered its main loop. WebSocket connections stay lazy and open on first use.

The first `client.ready()` starts the reactor and emits two copies: the awaited completion (`request_id: Some`) is delivered directly to the waiter; the broadcast copy (`request_id: None`) is for `client.events().on(EventType::ClientReady, …)`. Later `ready()` calls on a running client resolve only the waiter (synthetic envelope, new `request_id`). Broadcast subscribers see one `ClientReady` per client. If a reactor loop has already died, the call resolves with `ClientFailed`.

`ready().await` may resolve as `ClientFailed`. Sockets open later; use `ConnectionOpenEvent`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `capability_snapshot` | `CapabilitySnapshot` | Build-time snapshot. In this version `declared_namespaces` and `declared_ws_urls` are fixed, and `discovered_at_first_use` is empty. |

### `ClientFailed`

Failure terminal of `client.ready()`. Causes: the reactor cannot be given its command receiver (`ReactorSpawnFailed`); `ready()` after a loop death (`LoopFailed`); a loop death while `ready()` is in flight (`LoopFailed`).

Not broadcast. `client.events().on(EventType::ClientFailed, …)` receives nothing in this version. Loop death itself is `LoopFailedEvent`. A death during `close()` is `Err(CloseError::Interrupted)` with no broadcast `LoopFailedEvent`.

The client cannot serve streaming or dispatch callbacks. Build a new client.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `cause` | `ClientFailureCause` | `ReactorSpawnFailed` or `LoopFailed`. `ConfigValidationFailed` and `BusInitFailed` are never emitted. |

### `ClientClosedEvent`

Emitted at most once when `client.close()` completes: both connections have drained in-flight WebSocket requests and closed, then the reactor exits.

Two copies: awaited completion (`request_id: Some`) and broadcast (`request_id: None`). The broadcast is published just before the dispatch loop is drained and is best-effort.

If the caller-command queue is full, or the I/O loop is already gone, there is no `ClientClosedEvent` and `close().await` is `Err(CloseError::Interrupted)`. A loop death recorded before a close terminal still resolves the await; a terminal produced after a recorded death is dropped.

Dead-man disarm is not awaited. This event can resolve before that REST call finishes.

Await `client.close()` for a graceful shutdown. A loop death mid-close is `Err(CloseError::Interrupted)`. Shutdown duration is `timestamp_monotonic − initiated_at_monotonic`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `reason` | `ClientCloseReason` | Always `UserClose`. `FatalError` is never emitted. |
| `initiated_at_monotonic` | `MonotonicInstant` | When `client.close()` was called. |

### `DeadmanDisarmFailedEvent`

During `close()` the SDK sends one best-effort `cancel_all_orders_after(0)` over REST in a detached task. This event fires if that call fails with a classified cause (`RateLimitExceeded`, `Timeout`, `NetworkError`). Auth and other unmapped exchange errors are logged only.

At most once per `close()`. The call has already spent the REST retry budget. Broadcast (`request_id: None`). If the dispatch loop has already drained, subscribers may miss it; the log line still exists.

An armed dead-man timer may still be running. To confirm disarm, await `client.trade().cancel_all_orders_after(0)` before `close()`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `cause` | `DeadmanDisarmCause` | `RateLimitExceeded`, `Timeout`, or `NetworkError`. `AuthConnectionFailed` is never emitted. |
| `retry_at_monotonic` | `Option<MonotonicInstant>` | Always `None`. |
| `monotonic_ts` | `MonotonicInstant` | When the failure was observed. |

### `ConfigResolved`

Emitted once at the end of a successful `ClientBuilder::build()`, after builder, `KRAKEN_*` env, optional TOML, and defaults are merged. First event on the bus. Broadcast (`request_id: None`). Failed `build()` emits nothing.

The dispatch loop is not running yet. Registering `client.events().on(EventType::ConfigResolved, …)` starts the loop and delivers the buffered event. `ready()` is not required.

Values are not in the payload. Read them with `client.knob(name)`. `staleness_window_ms` is capped at 55000 ms after resolution; the map still records the original source. The buffer is the drop-oldest dispatch ring.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `source_map` | `BTreeMap<String, ConfigSource>` | Winning source per knob: `Builder`, `Env`, `File`, or `Default`. Every knob is present. |

### `ConfigChangedEvent`

Emitted after a successful `client.set_knob(name, value)`. Broadcast (`request_id: None`). Runtime-mutable knobs only: `reconnect_attempts`, `subscribe_ack_attempts`, `subscribe_ack_timeout_ms`, `max_auth_handshake_failures`, `slow_callback_threshold_ms`, `rate_limit_api_warning_pct`, `rate_limit_trading_warning_pct`. See [Configuration](guides/configuration.md#reference-the-knobs).

Errors emit nothing (`ImmutableKnob`, unknown name, type mismatch, warning pct outside `0.0..=1.0`). Setting a knob to its current value still emits (`previous == current`). `client.knob(name)` after the event sees `current`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `knob` | `KnobName` | Changed knob. `knob.as_str()` is the name passed to `set_knob`. |
| `previous` | `KnobValue` | Value before the write. |
| `current` | `KnobValue` | Value stored. Accepted values are not clamped. |

## Connection and transport

Connection events are the state-machine decision. `WsUpgradeOk` / `WsUpgradeFailed` / `WsClosed` are socket facts.

### `ConnectionConnectingEvent`

Emitted when a connection opens a socket and enters connecting. In v1 that is the automatic connect on first subscribe for that endpoint, or first WebSocket order on auth. Also emitted on reconnect backoff and when the connect-rate window advances.

Does not fire when the connect-rate budget refuses the attempt (`ConnectionRateThrottledEvent`). Forced reconnect has no public producer in v1.

`request_id` is `Some` for a connect triggered by first use (or an internal force) and `None` for timer-driven retries.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | `WsUrl::Public` or `WsUrl::Auth`. |
| `attempt_count` | `u32` | Attempt tally for this connect cycle at dial start. `0` on a forced-reconnect path. |

### `ConnectionOpenEvent`

First time this endpoint reaches open in the client's life. Later opens are `ConnectionReopenedEvent`.

Open is reached when the last outstanding subscribe ack arrives, immediately if there is no subscribe work, or — on auth with no auth subscriptions — when an order ack shows the token was accepted.

Streaming and WebSocket orders can proceed. Subscriptions registered earlier are replayed; callbacks need not wait for this event.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Endpoint that opened. |
| `opened_at_monotonic` | `MonotonicInstant` | Instant the connection reached open. |
| `server_connection_id` | `Option<String>` | Socket connection id after upgrade, stringified. |

### `ConnectionSendReadyEvent`

Auth endpoint only, while authenticating: a one-off (bare) order is the authentication probe. Conditions: auth URL, authenticating, no auth subscription registered, non-expired token cached.

Broadcast (`request_id: None`). Consecutive true re-evaluations emit more than once. Once open, use `ConnectionOpenEvent` / `ConnectionReopenedEvent`.

The order path waits on this internally.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Always `WsUrl::Auth`. |
| `ready_at_monotonic` | `MonotonicInstant` | Instant send-readiness was reached. |

### `ConnectionClosedEvent`

Emitted when that endpoint finishes closing. `Client::close()` is the v1 trigger: one event per endpoint.

Idle → `ack: NeverOpened`. Backing-off or failed → `ack: SocketDropped`. Otherwise after the close handshake: peer close frame (`Server`), peer gone (`SocketDropped`), or close timeout (`reason: Forced`, `ack: CloseTimeout`).

In-flight WebSocket requests fail first (`WsRequestFailedEvent`), then this event, then a reconnect if one was latched. A forced reconnect with no close pending does not emit this event.

`Client::close()` itself completes on `ClientClosedEvent` after both endpoints close.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Endpoint that closed. |
| `closed_at_monotonic` | `MonotonicInstant` | Instant the connection reached closed. |
| `reason` | `ClosedReason` | `ClientInitiated`, or `Forced` if the close handshake timed out. |
| `ack` | `AckSource` | `Server`, `SocketDropped`, `CloseTimeout`, or `NeverOpened`. |
| `server_connection_id` | `Option<String>` | Upgraded socket id, or `None` if none. |

### `ConnectionReopenedEvent`

Endpoint that has been open before reaches open again (automatic reconnect in v1). Mutually exclusive with `ConnectionOpenEvent` for a given open.

Subscriptions registered before the drop have already been replayed. Reseed a raw book (`on_book_raw`) built from deltas. The maintained book (`on_book`) is re-validated by the SDK.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Endpoint that reopened. |
| `reopened_at_monotonic` | `MonotonicInstant` | Instant open was reached again. |
| `attempt_count` | `u32` | Attempts consumed by this reconnect cycle, captured before the counter resets. |

### `ConnectionFailedEvent`

Terminal give-up for that endpoint. The SDK stops retrying. A later subscribe or order does not revive it. Rebuild the client.

Also emits `AuthenticationFailedEvent` for: consecutive-auth-handshake cap, non-transient handshake rejection, non-retryable token fetch. A `1008` during auth and `reconnect_attempts` exhaustion do not.

`reconnect_attempts` defaults to unbounded, so `RetryCapExhausted` from that knob occurs only after you set it. The auth-handshake cap is bounded by default.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Endpoint that gave up. |
| `attempt_count` | `u32` | Attempts before give-up. |
| `last_error` | `String` | Human-readable failure. Not for parsing. |
| `transient` | `bool` | Whether the final error was classified transient. |
| `non_transient_class` | `Option<NonTransientClass>` | `BadCreds`, `PermissionDenied`, `ClientError`, or `RetryCapExhausted`. `None` when no class applies. |

### `ConnectionDroppedEvent`

Open connection falling back to reconnect. Triggers: peer close other than `1008`; abnormal close with no frame; staleness window while open (`WebsocketStaleEvent` first); subscribe-ack budget exhausted while open.

Pre-open failures are `ConnectionAttemptFailedEvent`. Exhausting `reconnect_attempts` is `ConnectionFailedEvent`. Broadcast (`request_id: None`). In-flight WebSocket requests are failed first.

The SDK reconnects and replays subscriptions (`ConnectionReopenedEvent`). A `1000` shortly after a one-off auth order with no remaining subscriptions is expected.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Endpoint that dropped. |
| `dropped_at_monotonic` | `MonotonicInstant` | Instant the drop was observed. |
| `close_code` | `Option<u16>` | Peer close code, or `None`. |
| `reason` | `Option<String>` | Close-frame text, SDK description on the subscribe-budget path, or `None`. |

### `ConnectionRateThrottledEvent`

Connect attempt refused by the connect-rate window shared by both sockets to the host (`connection_rate_budget` default 120, `connection_rate_window_secs` default 600; both construction-only). No socket is opened. One event per refused attempt.

`request_id` is `Some` when the attempt was caller-driven and `None` for timer-driven retries.

The SDK resumes after `throttle_until_monotonic`. Raise the budget or shorten the window at build time.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Refused endpoint. |
| `attempts_used` | `u32` | Attempts in the current window, both endpoints. |
| `window_seconds` | `u32` | `connection_rate_window_secs`. |
| `window_remaining_attempts` | `u32` | Remaining attempts in the window. |
| `throttle_until_monotonic` | `MonotonicInstant` | When dialling may resume. |

### `ConnectionAttemptFailedEvent`

Transient failure before open. The connection backs off. Not used for mid-stream drops (`ConnectionDroppedEvent`). Exhausting `reconnect_attempts` is `ConnectionFailedEvent`, except subscribe-budget exhaustion, which always backs off. Broadcast (`request_id: None`).

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Target endpoint. |
| `attempt` | `u32` | Connection attempt tally. `0` after a forced reconnect reset. |
| `transient_class` | `TransientClass` | `HttpTransient`, `ConnectError`, `UpgradeTimeout`, `AuthHandshakeTransient`, `ClosedDuringHandshake`, `SubscribeBudgetExhausted`, or `TokenRefreshTransient`. |
| `kraken_error` | `Option<String>` | Handshake or close text, or SDK description on the subscribe-budget path. |
| `http_status` | `Option<u16>` | HTTP upgrade rejection status, or `None`. |
| `close_code` | `Option<u16>` | Close during handshake, or `None`. |
| `backoff_ms` | `u64` | Delay before the next attempt. |
| `failed_at_monotonic` | `MonotonicInstant` | Instant of failure. |

### `WebsocketStaleEvent`

No inbound frame within `staleness_window_ms`. Published before teardown. Followed by `ConnectionDroppedEvent` (from open), `ConnectionAttemptFailedEvent` (pre-open), or `ConnectionFailedEvent` if the reconnect budget is spent. Broadcast (`request_id: None`).

`staleness_window_ms` is construction-only and clamped to 55000 ms.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Silent endpoint. |
| `last_inbound_at_monotonic` | `MonotonicInstant` | Last inbound frame, or emit time if none. |
| `configured_window_ms` | `u32` | Window that expired (`staleness_window_ms`, default 30000). |

### `WsUpgradeOk`

Transport: HTTP-to-WebSocket upgrade succeeded and reader/writer tasks were spawned. Suppressed if the socket was discarded in flight. Not a connection-open signal. Wait for `ConnectionOpenEvent` or `ConnectionReopenedEvent`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `connection_id` | `u64` | SDK socket id. Also the envelope `request_id`. |
| `url` | `WsUrl` | Upgraded endpoint. |

### `WsUpgradeFailed`

Transport: upgrade failed (DNS, TCP, TLS, HTTP, protocol). Suppressed if the socket was discarded in flight. Exactly one of `WsUpgradeOk` or `WsUpgradeFailed` per live connect attempt.

Retry vs terminal is published as `ConnectionAttemptFailedEvent` or `ConnectionFailedEvent`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `connection_id` | `u64` | SDK socket id. Also the envelope `request_id`. |
| `url` | `WsUrl` | Target endpoint. |
| `error` | `TransportError` | `kind` (`TransportErrorKind`) and `transient`. |

### `WsClosed`

Socket reader: peer close frame, or a read error ending the stream. Empty close frame is reported as `code` `1006` with empty `reason`. Read error is `code` `1006` and `reason` the transport description.

Does not fire when the stream ends without a close frame, or when the socket is closed locally. Broadcast (`request_id: None`). Absence of this event is not proof the socket is alive.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `connection_id` | `u64` | SDK socket id. |
| `url` | `WsUrl` | Endpoint. |
| `code` | `u16` | Close code. `1006` if none arrived. |
| `reason` | `String` | Close-frame text (may be empty) or transport error description. |

## Authentication and credentials

Payloads never contain credentials. Keys appear only as SHA-256 fingerprints.

### `AuthenticationFailedEvent`

Private WebSocket authentication moved to terminal failed. Always paired with `ConnectionFailedEvent`. In-flight requests on that connection have already been failed.

Paths: credential or permission rejection (signed subscribe ack or bare-order probe); consecutive token-stale rejections reaching `max_auth_handshake_failures` (default 3); non-retryable token-refresh failure during authenticating.

Does not fire for transient handshake, token-stale below the cap, or retryable refresh (`ConnectionAttemptFailedEvent`). Exhausting `reconnect_attempts` on a transient handshake emits only `ConnectionFailedEvent`.

The private connection does not reconnect. No further WebSocket orders or private streams. Rebuild the client.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Always the auth endpoint in v1. |
| `non_transient_class` | `NonTransientClass` | Bad credentials, permission denied, non-retryable token refresh, or handshake retry-cap. |
| `kraken_response` | `Option<String>` | Set only on the non-retryable token-refresh path. Otherwise `None`. Use `ConnectionFailedEvent.last_error` for a diagnostic string. |

### `AuthHandshakeFailedEvent`

One event per rejected signed-subscribe handshake (transient, token-stale, and terminal). Does not fire for the bare-order probe or for a token-refresh transport failure.

`AuthenticationFailedEvent` is the terminal signal.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `url` | `WsUrl` | Always the auth endpoint in v1. |
| `attempt` | `u32` | Connection attempt tally, not the consecutive-auth-failure counter. |
| `transient` | `bool` | SDK classification. `false` for credential, permission, and token-stale (the SDK still retries token-stale). `true` only for unrecognised strings. |
| `kraken_error` | `Option<String>` | Exchange rejection text. A placeholder is substituted if the reply had none. |

### `CredentialRefreshedEvent`

Spot WebSocket token fetch succeeded and the cache was swapped. Broadcast. Cannot be awaited.

Proactive: timer at half the 15-minute lifetime, armed once a token is cached, dropped if the auth connection is closed or failed. Reactive: stale/invalid token on handshake or order, or a signed call with no valid cached token.

`Manual` is never emitted. Compare `token_expires_at_monotonic` only to other SDK timestamps. A proactive refresh does not re-authenticate the live socket; later frames use the new cached token.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `key_id_fingerprint` | `String` | SHA-256 fingerprint of the API key (hex, 16 characters). |
| `token_expires_at_monotonic` | `MonotonicInstant` | Fetch time plus the documented 15-minute lifetime. |
| `refresh_reason` | `RefreshReasonPayload` | `Proactive` or `Reactive`. |

### `CredentialRefreshFailedEvent`

Spot WebSocket token fetch failed (proactive or reactive). The refresh itself does not retry. A retryable failure during authenticating backs the connection off; a non-retryable failure fails the connection and also emits `AuthenticationFailedEvent`. A failed proactive refresh leaves the cached token unchanged. Broadcast.

`error_class` matches the typed error:

| Class | Meaning |
|---|---|
| `Auth` | Invalid key, signature, nonce, permission, lockout, or stale token. |
| `Network` | `GetWebSocketsToken` transport (`TokenRefreshTransient` or `TokenRefreshFailed`), including throttle / rate-limit. |
| `ClientClosed` | Shutdown mid-fetch. |
| `Unknown` | Unmapped Kraken string or undecodable reply. |

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `key_id_fingerprint` | `String` | SHA-256 fingerprint of the API key (hex, 16 characters). |
| `attempt` | `u32` | Always `1`. The refresh has no retry loop. |
| `error_class` | `ErrorClass` | See table above. |
| `retry_in_ms` | `Option<u64>` | Always `None`. |

### `NoncePoisonedEvent`

A signed REST call was rejected for invalid nonce, the SDK re-signed once with a fresh nonce, and the second call was also rejected. Requires `.with_nonce_recovery(true)` (construction-only, default off). Without it the error returns with no event.

Emitted for idempotent signed reads and cancels, and for validate-mode orders and batches. Not emitted for live add, live batch, or amend. One event per poisoned call, just before the error is returned. Broadcast.

The account nonce high-water mark is ahead of this signer. Rotate the key or align nonce generation across every client using it. The failing call still returns its typed error.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `key_id_fingerprint` | `String` | SHA-256 fingerprint of the API key (hex, 16 characters). |
| `endpoint` | `String` | REST path of the call. |

## Rate limiting

The SDK does not block or sleep on rate limits. Counter model: [Rate limits](guides/rate-limits.md).

### `RateLimitWarning`

A successful charge left a counter at or above the warning threshold (default 80%; `rate_limit_api_warning_pct` / `rate_limit_trading_warning_pct`, read on every charge). Level-triggered, at most once per second per scope. A failed charge that would breach the cap returns an error and does not warn.

Spot REST and Spot WebSocket orders share the trading counter. Unsigned public REST is not charged. No API key means no charge. Broadcast.

`tracker` is the string `"api"` or `"trading"`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `tracker` | `&'static str` | `"api"` or `"trading"`. |
| `key_id_fingerprint` | `String` | SHA-256 fingerprint of the API key (hex, 16 characters). |
| `pair` | `Option<Symbol>` | `Some` for trading. `None` for api. |
| `used` | `f64` | Counter after the charge. |
| `cap` | `f64` | Tier ceiling. |
| `pct` | `f64` | `used / cap`. |
| `decay_per_sec` | `f64` | Drain rate for the tier. |
| `seconds_to_drain` | `f64` | Seconds to empty (`used / decay_per_sec`). `INFINITY` if decay is non-positive or non-finite. Never `NaN`. |

### `RateLimitExceededEvent`

The exchange rejected an operation for rate limiting and the SDK snapped the owning counter to cap. One event per snap. REST and WebSocket order rejections both apply.

Does not fire when: no API key; a per-pair amend/cancel whose pair could not be resolved; a local pre-wire prediction (that path returns a rate-limited error). Broadcast.

`pair: None` is either an api-tracker breach or an account-wide trading breach. Use `tracker` to distinguish. Absence of this event is not proof the call was not rate limited.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `tracker` | `&'static str` | `"api"` or `"trading"`. |
| `key_id_fingerprint` | `String` | SHA-256 fingerprint of the API key (hex, 16 characters). |
| `pair` | `Option<Symbol>` | `Some` for a per-pair trading breach. `None` for api or account-wide trading. |
| `kraken_error` | `String` | Exchange rate-limit string. |
| `observed_at_monotonic` | `MonotonicInstant` | Instant the rejection was observed. |

### `RateLimitCacheEvictionEvent`

Charging a previously unseen pair would exceed `rate_limit_trading_scope_cap` (default 256). The least-recently-charged trading-scope entry is dropped. Only the trading tracker, only the pre-wire charge path. One event per eviction, before any warning for that charge. Broadcast.

The evicted counter resets to zero. The exchange remains authoritative. A later breach surfaces as `RateLimitExceededEvent`. Raise the cap at build time. `0` is rejected at `.build()` (`ConfigError::InvalidConfig`). The live map cannot be resized.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `key_id_fingerprint` | `String` | SHA-256 fingerprint of the API key (hex, 16 characters). |
| `pair` | `Option<Symbol>` | Always `Some`. |
| `evicted_at_monotonic` | `MonotonicInstant` | Instant of eviction. |
| `reason` | `&'static str` | Always `"lru_cap_exceeded"`. |

## Dispatcher, queue, and handler health

### `QueueFullWarning`

An item was pushed onto a full I/O→dispatch queue. The oldest item is dropped. Debounced to at most one warning per 100 ms. A full caller→I/O queue does not emit this; that path returns `TradeError::QueueFull` / `SubscriptionError::QueueFull` (`QUEUE_FULL`).

Raw-book consumers should resubscribe. Maintained `on_book` is CRC-validated independently.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `queue` | `QueueName` | Always `IoToDispatch` in v1 (`"io_to_dispatch"`). |
| `dropped_count` | `u64` | Cumulative evictions since the client was built. |
| `capacity` | `usize` | `io_to_dispatch_capacity` (default 8192). |

### `QueueDepthSample`

Dispatch loop sample: every 100 ms or 1000 dequeues, whichever first. Unconditional. An idle loop emits nothing. The sample is taken after the current item was dequeued.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `queue` | `QueueName` | Always `IoToDispatch` in v1. |
| `depth` | `usize` | Queued items at sample time. |
| `capacity` | `usize` | `io_to_dispatch_capacity` (default 8192). |
| `sampled_at_monotonic` | `MonotonicInstant` | Equal to the envelope timestamp. |

### `CallbackLatency`

After a dispatch-loop callback returns normally. Lifecycle subscribers: one sample per invocation, except dispatcher-observability event types. WebSocket data callbacks: only while a `CallbackLatency` subscriber is registered. A panicking callback emits `HandlerPanicWarning` instead.

On a busy stream this fires once per frame per handler.

Payload (`event_version` 2):

| Field | Type | Meaning |
|---|---|---|
| `callback_event_type` | `CallbackSource` | `Event(EventType)` or `DataChannel(ChannelName)`. |
| `handler_id` | `Option<HandlerId>` | `Some` for `client.events().on(...)` at the latest version. `None` for an explicit version-1 subscriber. |
| `latency_us` | `u64` | Execution time in whole microseconds (truncated). |

### `SlowCallbackWarning`

Callback execution ≥ `slow_callback_threshold_ms` (default 50), read on every invocation. No debounce. Observability event types do not trigger this. A panic emits `HandlerPanicWarning` instead.

Callbacks must not `.await`, sleep, or take contended locks. Condition: `latency_us >= threshold_ms * 1000`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `slow_event_type` | `CallbackSource` | `Event(EventType)` or `DataChannel(ChannelName)`. |
| `latency_us` | `u64` | Observed execution time, microseconds (truncated). |
| `threshold_ms` | `u32` | Threshold in force at that invocation. |

### `HandlerPanicWarning`

A callback panicked. Other handlers still run. Neither reactor loop dies. No debounce.

A panic inside a synchronous latched-completion subscribe, or on a loop-death drain waiter, is isolated and not reported. Observability event types are suppressed for lifecycle/waiter panics. Data-channel panics are not.

The handler stays registered.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `source` | `CallbackSource` | Lifecycle/waiter or data channel / decode. |
| `handler_id` | `HandlerId` | Registry id of the handler. |
| `panic_message` | `String` | Panic payload as text, truncated to 1 KB on a UTF-8 boundary. Non-string payloads are `"<non-string panic payload>"`. |

### `MessageDroppedNoHandler`

Inbound `update` or `snapshot` row for a channel with zero handlers (typically a dropped `HandlerHandle` without `unsubscribe_*`). At most one emission per channel per 1 second. `count` accumulates inside the window. Tail drops may never flush.

Exempt: `status`; `book` until both maintained and raw handler sets are empty; malformed non-array `data`.

Call the matching `unsubscribe_*`. `on_*_for` combiners drop both the handler and the wire subscription.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `channel` | `ChannelName` | Channel with no handler. |
| `count` | `u32` | Unhandled rows since the previous emission. First emission is `1`. |
| `period` | `Duration` | Always 1 second in this release. |

### `LoopFailedEvent`

First genuine death of either reactor loop (panic or abnormal exit). Intended teardown (`close()`, drop client) is not a death and is not broadcast.

In-flight correlated awaits are resolved with a copy of this event (except `ready()`, which resolves as `ClientFailed` / `LoopFailed`). One broadcast copy is best-effort; if the dispatch loop died, subscribers may not see it. Subsequent deaths are suppressed.

v1 does not restart a loop. New WebSocket work and new event subscriptions fail with a loop-dead error. REST remains usable. Build a new client.

A death during `close()` is `Err(CloseError::Interrupted)`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `loop_name` | `ReactorName` | `Io` or `Dispatch` (`"io"` / `"dispatch"`). |
| `cause` | `LoopFailureCause` | `Panic` or `UnhandledError`. `Cancelled` is never emitted here. |
| `failed_at_monotonic` | `MonotonicInstant` | Equal to the envelope timestamp. |

### `WsRequestFailedEvent`

In-flight request abandoned on the authenticated WebSocket (drop, staleness, forced reconnect, terminal failure, or client close). Also the in-flight order drained on a stale-token rejection. Public connection never emits this.

The awaited future is resolved with its error before this event. The drain finishes before the matching connection event. Within one drain, events are in ascending `req_id`. Caller-side deadline and compose/send failure after record resolve the future with no event.

The frame may have reached the exchange. Reconcile with `client.account().find_order_by_cl_ord_id(...)` before resending.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `req_id` | `u64` | WebSocket v2 `req_id`. |
| `op` | `WsOp` | `AddOrder`, `AmendOrder`, `CancelOrder`, `CancelAll`, `CancelAllOrdersAfter`, or `BatchAdd`. |
| `reason` | `WsFailReason` | `ClientClosed` or `ConnectionLost`. |

## Subscription and data integrity

### `SubscriptionTerminatedEvent`

1. Caller teardown, `cause: ClientClosed`, `last_error: None`. Single-key `unsubscribe_*` or `SubscriptionGuard` drop: only on refcount 0 for that `(channel, pair)`. `unsubscribe_all` / `unsubscribe_channel`: one event per removed live entry; `Failed` rows are silent. Dropping a bare `on_*` handle does not emit. A surplus plain `unsubscribe_*` can terminate another plain subscriber's slot; guard-held subscriptions are protected.
2. Non-transient subscribe rejection, `cause: NonTransientWireRejection`. That entry only. Connection stays up.

Transient subscribe failures retry, then escalate to connection events. Book reseed unsubscribe/resubscribe does not emit. `Client::close()` does not emit per-stream terminations.

Callbacks for that key stop. Call `subscribe_*` again to restore. A `subscribe_*` that returned `Ok` may still never go live; this event is how that is observed.

Payload (`event_version` 2):

| Field | Type | Meaning |
|---|---|---|
| `channel` | `ChannelName` | Terminated channel. `Book` and `BookRaw` share wire key `Book`. |
| `pair` | `Option<Symbol>` | Pair, or `None` for channel-wide (`executions`, `balances`). |
| `cause` | `TerminationCause` | `ClientClosed` or `NonTransientWireRejection`. Other variants exist; match with `_`. `ClientClosed` is unsubscribe / guard drop, not `Client::close()`. |
| `last_error` | `Option<String>` | Kraken reject string, or `None` on caller teardown. |
| `terminated_at_monotonic` | `MonotonicInstant` | Instant observed. |

Broadcast (`request_id: None`).

### `SubscriptionGapEvent`

1. Typed decode failure for a registered `on_*` (`cause: MalformedFrame`). Requires a parseable `symbol`.
2. Maintained book CRC mismatch or exhausted reseed budget (`cause: OrderBookCrcMismatch`), in parallel with `OrderBookGapEvent`.

Channels without a symbol on the row (for example `balances` decode failure) do not emit this.

Maintained book reseeds itself. `on_book_raw` and other channels need a resubscribe or REST backfill. `dropped_count` is `0` on every shipped path. `FrameDropDueToQueueFull` is never this event (`QueueFullWarning`). `SequenceGapDetected` is `ChannelGapEvent` only.

Payload (`event_version` 2):

| Field | Type | Meaning |
|---|---|---|
| `channel` | `ChannelName` | Fan target on decode failure; always `Book` on a CRC gap. |
| `symbol` | `Symbol` | Pair. Always present. |
| `dropped_count` | `u32` | Always `0` as shipped. |
| `cause` | `GapCause` | `MalformedFrame` or `OrderBookCrcMismatch`. |

Broadcast (`request_id: None`).

### `OrderBookGapEvent`

Maintained book only (`on_book` / `on_book_for`). CRC mismatch: update withheld, local book dropped, unsubscribe + resubscribe for a snapshot. Missed reseed snapshot after the recovery budget (5 consecutive failures per pair): maintenance dropped; `on_book` then delivers un-validated per-frame data.

Paired with `SubscriptionGapEvent`. Raw-delta (`on_book_raw`) never emits this. Deltas in the post-gap resync window are dropped silently.

Payload (`event_version` 2):

| Field | Type | Meaning |
|---|---|---|
| `channel` | `ChannelName` | Always `Book`. |
| `symbol` | `Symbol` | Pair. |
| `cause` | `GapCause` | Always `OrderBookCrcMismatch`. |

Broadcast (`request_id: None`). No `dropped_count`; the parallel `SubscriptionGapEvent` carries `0`.

### `ChannelGapEvent`

`executions` or `balances` frame `sequence` more than one ahead of the last seen value. Requires a registered channel-wide subscription and a parseable numeric `sequence`. Snapshots reseed the epoch and never gap. Duplicate or regressed sequence resets tracking without an event.

The SDK does not auto-resubscribe. Re-sync over REST (`open_orders`, `trades_history`, `balance`).

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `channel` | `ChannelName` | `Executions` or `Balances`. |
| `dropped_count` | `u32` | Observed delta minus one, saturating at `u32::MAX`. |
| `cause` | `GapCause` | Always `SequenceGapDetected`. |

No `symbol` field. Broadcast (`request_id: None`).

## Order lifecycle and REST

The awaited method `Result` is authoritative. Drive control flow from it.

### `OrderSubmittedEvent`

Definitive wire response for `add` / `amend` / `cancel_order` (REST or WS auth). `cancel_batch` emits one event per leg.

- Placement accepted with an order id → `WireSent { txid }`.
- Amend or cancel accepted → `WireAccepted`. Amend may carry `amend_id`; cancel never does.
- Definitive rejection → `WireError { code }`.

Does not fire for: `cancel_all`, `cancel_all_orders_after`, `order_batch`; ops with no `cl_ord_id` (auto-allocation suppressed when `userref` or conditional-close is set); successful validate-mode placement; pre-wire failures; send with no answer (`OrderPlacementAmbiguousEvent`).

At most one event per op (per batch leg). Envelope `request_id` is `None`; correlation is the payload field.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `cl_ord_id` | `ClOrdId` | Client order id. |
| `amend_id` | `Option<AmendId>` | Set only when an amend reply included one. |
| `op` | `OrderOp` | `AddOrder`, `AmendOrder`, or `CancelOrder`. `#[non_exhaustive]`. |
| `status` | `OrderSubmitStatus` | `WireSent { txid }`, `WireAccepted`, or `WireError { code }`. `#[non_exhaustive]`. |
| `request_id` | `Option<String>` | REST UUID or WS `req_id`. Populated on every shipped path. |

### `OrderPlacementAmbiguousEvent`

`add` or `amend` reached the wire with no definitive answer.

REST: sent-but-unanswered transport (including timeout after send), socket reset, abnormal close, close frame mid-request. WS: deadline, in-flight drop, client closing, or loop death after the frame was recorded.

Does not fire for cancels, `order_batch`, pre-send failures, or ops with no `cl_ord_id`. Never alongside `OrderSubmittedEvent` for the same send.

Do not resend. Reconcile with `client.account().find_order_by_cl_ord_id(cl_ord_id)`. `last_known_status` is the literal `"sent_no_response"` (`EventPayload::PLACEMENT_AMBIGUOUS_LAST_KNOWN_STATUS`). Envelope `request_id` is `None`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `cl_ord_id` | `ClOrdId` | Client order id. |
| `amend_id` | `Option<AmendId>` | Amend id if already minted. |
| `op` | `OrderOp` | `AddOrder` or `AmendOrder`. |
| `sent_at_monotonic` | `MonotonicInstant` | Sampled immediately before send. Not comparable to exchange timestamps. |
| `request_id` | `Option<String>` | REST UUID or WS `req_id`. |

### `OrderCancellationAttempted`

The in-flight order future was dropped (cancelled task, `select!`, timeout) after the send guard was armed and before the send resolved. Retryable cancel paths include backoff sleep.

Does not fire for a completed call, a drop before send, `cancel_all` / `cancel_all_orders_after` / `order_batch`, or ops with no `cl_ord_id`. A dropped `cancel_batch` await emits one event per in-flight leg.

Dropping the `await` does not cancel the order. Reconcile with `find_order_by_cl_ord_id`. `cancellation_source` is `"caller_initiated"` (`EventPayload::CANCELLATION_SOURCE_CALLER_INITIATED`). Envelope `request_id` is `None`.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `cl_ord_id` | `ClOrdId` | Client order id. |
| `op` | `OrderOp` | `AddOrder`, `AmendOrder`, or `CancelOrder`. |
| `request_id` | `Option<String>` | REST UUID or WS `req_id`. |

### `OrderReconciliationEvent`

Once per completed `client.account().find_order_by_cl_ord_id(..)`: `Found` or `NotPlaced`. A failed REST leg is the method `Err` and publishes nothing. Envelope `request_id` is `None`. `ReconciliationOutcome::Unknown` is never produced by the shipped walk.

The caller already has the same `Result`. The bus copy can be evicted under saturation.

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `cl_ord_id` | `ClOrdId` | Searched id. |
| `outcome` | `ReconciliationOutcome` | `Found { txid, status, lifecycle }` or `NotPlaced`. `lifecycle` is authoritative. |

### `RestRetryAttempt`

One event per REST retry, never the original try, after backoff and immediately before the retried send. Dropping the future during backoff publishes nothing.

Emitted for public GETs, idempotent private reads (including reconciliation legs), and the cancel family. Live add / amend / batch placement are not auto-retried. Validate-mode placement may emit `reason: InvalidNonce` for the single fresh-nonce re-sign.

Envelope `request_id` is `None`. Join retries of one call on the payload `request_id`. Persistent invalid nonce after the re-sign is `NoncePoisonedEvent` (opt-in).

Payload (`event_version` 1):

| Field | Type | Meaning |
|---|---|---|
| `endpoint` | `String` | REST path. |
| `request_id` | `Option<String>` | Constant across attempts of one logical call. |
| `attempt` | `u32` | 1-based ordinal of the attempt about to run. The original try is 1, so the first retry is `2`. |
| `backoff_ms` | `u64` | Sleep already elapsed. |
| `reason` | `RetryReason` | `TransportError`, `HttpServerError`, `RateLimitExceeded`, `ServiceThrottled`, `ServiceUnavailable`, or `InvalidNonce`. `#[non_exhaustive]`. |

## Related

- [Streaming](guides/streaming.md)
- [Error handling](guides/error-handling.md)
- [Rate limits](guides/rate-limits.md)
- [Configuration](guides/configuration.md)
- [Architecture](architecture.md)
