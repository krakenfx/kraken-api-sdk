# Architecture

The SDK is an async client over Spot REST and Spot WebSocket v2. Callers use domain methods (`client.market()`, `client.trade()`, …). Transport, credentials, queues, and reconnect policy are wired at `.build()`.

WebSocket work runs on two loops: an I/O loop that owns the sockets, and a dispatch loop that runs callbacks. Connections open on first use. Operational signals go to the event bus.

Module layout and internals: [Development](development.md). Task-level frame and order path: [Dispatch flow](dispatch-flow.md).

## Configuration

`ClientBuilder::build()` does not call the network. It merges Builder > environment > config file > SDK defaults, validates, and constructs the `Client`. A config file, if set, is a local read.

Locked at build:

- Credentials
- Endpoint URLs (`rest_base_url`, `ws_public_url`, `ws_auth_url`), request timeout, TLS
- Rate-limit tier
- Reconnect backoff (`backoff_base_ms`, `backoff_max_ms`, `backoff_factor`, `backoff_jitter`)
- REST retry (`rest_retry_max_attempts`, `rest_retry_base_ms`, …)
- Queue capacities (`caller_to_io_capacity`, `io_to_dispatch_capacity`) and other construction-only knobs

Mutable at runtime (`client.set_knob`):

- Rate-limit warning thresholds (`rate_limit_api_warning_pct`, `rate_limit_trading_warning_pct`)
- `reconnect_attempts` (`None` = unlimited)
- `subscribe_ack_attempts`, `subscribe_ack_timeout_ms`
- `max_auth_handshake_failures`
- `slow_callback_threshold_ms`

A successful `set_knob` takes effect immediately and emits `ConfigChangedEvent`. A construction-only name is `ConfigError::ImmutableKnob`. Knob table: [Configuration](guides/configuration.md#reference-the-knobs).

## Dual loop

WebSocket users run two Tokio tasks.

**I/O loop** — owns the WebSocket sockets. Reads and writes frames, decodes inbound data, maintains the per-pair order book (CRC, gap, reseed), and accepts outbound subscribe and order frames. It does not run `on_*` callbacks.

**Dispatch loop** — runs `on_*` data callbacks and `client.events().on(...)` handlers. Data callbacks are `Fn(&T)`: one decode, borrowed by every handler. Clone inside a callback only if the payload is retained.

Decoded payloads cross a bounded drop-oldest queue (`io_to_dispatch`). Callbacks run sequentially on the dispatch loop. A slow callback does not stall socket reads, order sends, timers, or reconnect. It may emit `SlowCallbackWarning` (default threshold 50 ms) and `CallbackLatency`. If the queue fills, the oldest delivery is dropped and `QueueFullWarning` is emitted. Maintained `on_book` is CRC-validated on the I/O loop regardless. Raw-book consumers should resubscribe after a gap.

WebSocket orders (`Transport::WsV2Auth`) go from the caller's `await` onto the I/O outbound queue. They do not enter the dispatch loop.

A full caller-to-I/O queue rejects before send: `TradeError::QueueFull` / `SubscriptionError::QueueFull` (code `QUEUE_FULL`, retryable).

## Connections

Two WebSocket connections:

| Role | Default URL |
|------|-------------|
| Public | `wss://ws.kraken.com/v2` |
| Auth | `wss://ws-auth.kraken.com/v2` |

Override with construction-only `ws_public_url` / `ws_auth_url`. `client.ready()` starts the I/O loop. TCP/TLS starts on the first subscribe or WebSocket order for that endpoint. Failure of one connection does not close the other.

## Event bus

`client.events().on(EventType, callback)` receives lifecycle and operational events on the dispatch loop. Categories: connection, rate limit, subscription and book integrity, order lifecycle, config, dispatcher observability, client ready / close / loop death.

Each event is an envelope: `event_type`, `event_version`, `timestamp_monotonic`, optional `request_id`, typed `payload`. Full catalog: [Events](events.md).

## Symbols

Pair strings (`"BTC/USD"`) are method arguments. They are not declared or checked at `.build()`. The exchange validates at first use; an unknown pair is `MarketError::SymbolNotFound`.

Asset codes in balance and ledger responses are modern form (`BTC`, `USD`). Legacy wire codes (`XXBT`, `ZUSD`) are stripped on decode.

## Order routing

Trade methods return `PendingTrade` before any wire call. Default transport is authenticated WebSocket (`Transport::WsV2Auth`). Per-call `.via(Transport::Rest)` or build-time `prefer_rest_for_orders` selects REST. `POST /AddOrder` is never auto-retried. [Placing orders](guides/placing-orders.md).

## Client order id

Shorthands and `order(OrderRequest)` allocate a UUID v4 `cl_ord_id` on the `PendingTrade` before `.await`. Use it for `cancel`, `order_amend`, and `find_order_by_cl_ord_id`. Auto-allocation is suppressed when the request has `userref` or a conditional-close clause. [Placing orders → Client order id](guides/placing-orders.md#client-order-id-cl_ord_id).

## Credentials

- Credentials stay in memory. They are not logged, written to disk, or included in error messages.
- The base64 secret passed to `with_api_key` is zeroed after decode, success or failure.
- Logs redact key, secret, token, and nonce unconditionally.
- TLS 1.2+ with certificate verification. Cleartext `rest_base_url` / `ws_public_url` / `ws_auth_url` is `ConfigError::InsecureEndpointScheme` at `.build()`.
- No cancel-on-disconnect API. Venue-side protection is `trade().cancel_all_orders_after(seconds)`.
