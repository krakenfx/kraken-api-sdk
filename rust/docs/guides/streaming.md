# Streaming

Call `client.ready().await` once before any WebSocket operation. The call is idempotent. Connections open on first subscribe.

```rust
client.ready().await?;
```

## Combiner (`on_*_for`)

Registers a handler and subscribes. Returns `SubscriptionGuard`. Drop unsubscribes and deregisters.

```rust
use kraken_sdk::{Symbol, TickerUpdate};

client.ready().await?;

let _guard = client.market().on_ticker_for(
    &[Symbol::new("BTC/USD")?],
    None, // snapshot
    None, // event_trigger
    |update: &TickerUpdate| {
        println!("{} last={}", update.symbol.as_str(), update.last);
    },
)?;
```

Market combiners: `on_ticker_for`, `on_trade_for`, `on_book_for`, `on_book_raw_for`, `on_ohlc_for`. Options precede the callback. The callback is filtered to that call's pairs.

Account combiners: `on_executions_for`, `on_balances_for`. Channel-wide (no pairs). Credentials required; they use the auth socket.

```rust
use kraken_sdk::{ApiKey, Client, ExecutionUpdate, BalanceUpdate};

let client = Client::builder()
    .with_api_key(ApiKey::new(key), secret)
    .build()?;

client.ready().await?;

let _exec = client.account().on_executions_for(|update: ExecutionUpdate| {
    println!("[exec] exec_type={:?}", update.exec_type);
})?;

let _bal = client.account().on_balances_for(|update: BalanceUpdate| {
    println!("[balance] update received");
})?;
```

`on_executions` + `subscription().subscribe_executions()` remains available when registration and subscribe happen at different times.

### Subscribe options

Ticker, trade, ohlc, and `book_raw` take `snapshot: Option<bool>`. Ticker also takes `event_trigger: Option<TickerTrigger>`. Maintained book (`subscribe_book` / `on_book_for`) always requests a snapshot.

- `snapshot = None` — Kraken default. `Some(false)` skips the opening snapshot.
- Trade `Some(true)` — recent-trades history.
- `event_trigger = Some(TickerTrigger::Bbo)` — update on every top-of-book change. Default is trades.

```rust
use kraken_sdk::{Symbol, TickerTrigger, TickerUpdate};

let _guard = client.market().on_ticker_for(
    &[Symbol::new("BTC/USD")?],
    Some(false),
    Some(TickerTrigger::Bbo),
    |update: &TickerUpdate| {
        println!("{} bbo last={}", update.symbol.as_str(), update.last);
    },
)?;
```

## Handler then subscribe

`subscribe_*` requires a registered handler.

```rust
use kraken_sdk::{OhlcInterval, OhlcUpdate, Symbol};

let _handle = client.market().on_ohlc(|update: &OhlcUpdate| {
    println!("{} close={}", update.symbol.as_str(), update.close);
});

client.ready().await?;

client.subscription().subscribe_ohlc(
    vec![Symbol::new("BTC/USD")?],
    OhlcInterval::M1,
    None, // snapshot
)?;
```

Channel-wide `on_*` returns `HandlerHandle`. Drop deregisters the callback only. The wire subscription stays up. Frames with no handler emit rate-limited `MessageDroppedNoHandler`. Unsubscribe separately, or use a combiner.

## Guard vs handle

`SubscriptionGuard` (`on_*_for`): drop sends unsubscribe, then deregisters.

`HandlerHandle` (`on_*`): drop deregisters only.

`let _ =` drops immediately:

```rust
let _ = client.market().on_ticker_for(&pairs, None, None, cb)?;  // no data
let _guard = client.market().on_ticker_for(&pairs, None, None, cb)?;
```

## Subscribe and unsubscribe

Kraken keys a live subscription by params, not channel alone: book by `(channel, symbol, depth)`, OHLC by `(channel, symbol, interval)`. Unsubscribe must echo that key. The SDK tracks it.

`snapshot` and `event_trigger` are subscribe-only. `interval` and book `depth` must be echoed. Omitted subscribe-only params use Kraken defaults:

| Channel | `snapshot` default | `event_trigger` default |
|-----------|--------------------|-------------------------|
| book | `true` | — |
| book_raw | `true` | — |
| ohlc | `true` | — |
| ticker | `true` | `trades` |
| trade | `false` | — |

Maintained `book` always sends `snapshot: true`.

Every subscribe/unsubscribe frame carries a monotonic `req_id`. Kraken echoes it. A reject that omits `channel` and `result` still correlates on `req_id`.

### Shared wire subscription

Same pair on the same channel shares one wire subscription. The first live subscriber sets wire options for that lifetime (and reconnect replay). Later differing options are ignored. After `SubscriptionTerminatedEvent`, the next subscribe starts a new lifetime.

- **ticker** — first writer sets `snapshot` + `event_trigger`.
- **book** — maintained always snapshots. Raw `snapshot: Some(false)` is delta-only. Raw and maintained share `(book, pair)`; first depth wins. [Order book → Depth coalescing](order-book.md#depth-coalescing).

Combiner pair filters are fixed for the handler lifetime. `unsubscribe_*` of one pair changes the wire refcount, not the filter. If the subscribe post fails, the new handler is deregistered and the error is returned. If the caller→I/O queue is full at guard drop, teardown is queued and retried; a closed queue is terminal.

### Subscribe errors

- `NoHandlerRegistered` — register `on_*` first.
- `QueueFull` — registry unchanged; retry.
- `ClientClosed` — after `close()`; not retryable.
- `LoopDead` — rebuild the client.

### Unsubscribe contract

`unsubscribe_*` decrements the shared `(channel, pair)` refcount. Call once per matching `subscribe_*`. A surplus call can end another plain subscriber's stream. Guard-held subscriptions are not consumed by plain `unsubscribe_*`; drop the guard. `QueueFull` leaves the registry unmutated; retry that call.

### Inspecting subscriptions

Snapshots may trail in-flight posts.

- `list_active()` — one row per `(channel, pair)` with `SubscriptionState` (`Active` / `Pending` / `Failed`). Includes retained `Failed` rows.
- `find_by_channel(channel)` — same rows for one channel, plus `registered_at_monotonic`. `BookRaw` returns shared `Book` rows.
- `status_summary()` — counts (`total = active + pending + failed`) and a per-channel histogram.

`LoopDead` if a reactor loop has died. `ClientClosed` after `close()`.

## Callbacks

Data callbacks are `Fn(&T) + Send + Sync + 'static`. They run on the dispatch loop. No `.await`. Clone only to retain the payload. Slow delivery: [Architecture → Dual loop](../architecture.md#dual-loop).

```rust
use std::sync::{Arc, Mutex};
use kraken_sdk::TickerUpdate;

let last_price = Arc::new(Mutex::new(None));
let last_price_cb = Arc::clone(&last_price);

let _guard = client.market().on_ticker_for(
    &[Symbol::new("BTC/USD")?],
    None,
    None,
    move |update: &TickerUpdate| {
        *last_price_cb.lock().unwrap() = Some(update.last);
    },
)?;
```

```rust
use tokio::sync::mpsc;
use kraken_sdk::TickerUpdate;

let (tx, mut rx) = mpsc::channel::<TickerUpdate>(32);

let _guard = client.market().on_ticker_for(
    &[Symbol::new("BTC/USD")?],
    None,
    None,
    move |update: &TickerUpdate| {
        let _ = tx.try_send(update.clone());
    },
)?;

while let Some(update) = rx.recv().await {
    println!("{} last={}", update.symbol.as_str(), update.last);
}
```

## Multi-channel

```rust
use kraken_sdk::{ClientBuilder, OhlcInterval, OhlcUpdate, Symbol, TradeUpdate};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = ClientBuilder::new().build()?;

    let _trades = client.market().on_trade(|t: &TradeUpdate| {
        println!("[trade] {} {} px={} qty={}", t.symbol.as_str(), t.trade_id, t.price, t.qty);
    });
    let _ohlc = client.market().on_ohlc(|c: &OhlcUpdate| {
        println!("[ohlc] {} O={} H={} L={} C={}", c.symbol.as_str(), c.open, c.high, c.low, c.close);
    });

    client.ready().await?;

    let pair = Symbol::new("BTC/USD")?;
    client.subscription().subscribe_trade(vec![pair.clone()], None)?;
    client.subscription().subscribe_ohlc(vec![pair.clone()], OhlcInterval::M1, None)?;

    let ticker_guard = client.market().on_ticker_for(
        &[pair.clone()],
        None,
        None,
        |t| println!("[ticker] {} last={}", t.symbol.as_str(), t.last),
    )?;

    tokio::time::sleep(Duration::from_secs(10)).await;
    drop(ticker_guard);
    tokio::time::sleep(Duration::from_secs(5)).await;
    Ok(())
}
```

Runnable: [`examples/live_streams.rs`](../../examples/live_streams.rs).

## System status

Channel-wide, no pair. Kraken pushes status when the public socket opens (any subscribe opens it) and roughly once per second. Register `on_system_status` before the socket opens to catch the first frame. A later handler misses that frame and receives the next periodic push.

```rust
use kraken_sdk::SystemStatusUpdate;

let _ss = client.market().on_system_status(|s: SystemStatusUpdate| {
    println!("system={:?} version={:?}", s.system, s.version);
});

client.ready().await?;
client.subscription().subscribe_system_status()?;
```

`on_system_status` is optional. Unhandled status frames are not reported as `MessageDroppedNoHandler`.

`subscribe_system_status()` does not return `success: true`. Kraken returns the status payload plus `{"error": "Symbol(s) not found", "success": false}` with no `channel` and no `result`. The SDK treats that reject as subscribe completion: ack timer disarmed, correlation dropped, subscription stays up.

## Reconnect

Connect: `ConnectionConnectingEvent`, `ConnectionOpenEvent`, `WsUpgradeOk`. Drop: `ConnectionDroppedEvent` / `ConnectionAttemptFailedEvent`, then reconnect (bounded by `reconnect_attempts`), replay, `ConnectionReopenedEvent`. Exhaustion: `ConnectionFailedEvent`. Backoff: `backoff_base_ms` / `backoff_max_ms` / `backoff_factor` / `backoff_jitter`.

Cloudflare `429`/`503` on upgrade, TLS/DNS/TCP flaps: retryable. Terminal: upgrade `401`/`403`/`404`/`400`, malformed URL, unclassifiable protocol error. Close code **1008** is terminal. **1006** and other codes reconnect. [Error handling → WebSocket server close codes](error-handling.md#websocket-server-close-codes).

Kraken closes a socket with zero subscriptions. After a one-off auth-WS order with nothing left subscribed, the next order reconnects and re-authenticates.

On reconnect, subscribe frames replay in a fixed order: channel-wide (no symbol) by wire channel name, then per-pair by wire channel then symbol. `Book` / `BookRaw` share wire `"book"`; declaration order is the tiebreak. Auth keepalive is a separate leading step.

## Ready and close

`ready()` starts the I/O loop. It does not wait for sockets to reach Open. First-start `ClientReady` also fans a broadcast copy (`request_id: None`) to `events().on(...)`. `ClientFailed` is correlated-only. [Getting Started → WebSocket streams](../getting-started.md#websocket-streams).

`close()` is terminal, consumes the client, and is `#[must_use]`. It drains public and auth WebSocket connections. Both `ready()` and `close()` require a Tokio runtime.

```no_run
# async fn ex(client: kraken_sdk::Client) -> Result<(), kraken_sdk::CloseError> {
client.close().await?;
# Ok(())
# }
```

On close the SDK fire-and-forgets REST `CancelAllOrdersAfter(0)` to disarm a dead-man. `close()` does not await it. `ClientClosedEvent` is WS drain only. A classifiable disarm failure is `DeadmanDisarmFailedEvent` (logged, not retried). For a confirmed disarm, await `cancel_all_orders_after(0)` before `close()`. `Err(CloseError::Interrupted)`: re-check venue-side orders. [Getting Started → Shutdown](../getting-started.md#shutdown).

Spot WS v2 uses a REST `GetWebSocketsToken` token (~15 min TTL), fetched on first auth-WS use, refreshed at TTL × 0.5. A stale token mid-session does not close the socket. Failed refresh leaves the cache and existing sessions. Token errors: [Error handling](error-handling.md#authentication-and-token-errors).

Bus internals: [Development → Dispatch event bus](../development.md#dispatch-event-bus).

## Related

- [Ticker](ticker.md)
- [Order book](order-book.md)
- [Features — subscription](../features.md#subscription-namespace)
- [`examples/live_streams.rs`](../../examples/live_streams.rs)
- [`examples/reconnect_resilience.rs`](../../examples/reconnect_resilience.rs)
