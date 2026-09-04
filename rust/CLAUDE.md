# Working with the Kraken SDK (guide for coding agents)

This file orients an AI coding agent that has pulled this repository and needs to
**generate correct application code using the SDK**. It is not the SDK's design
spec — it's the rules and patterns for *using* the library. Human docs live in
[`README.md`](README.md) and [`docs/`](docs/); runnable programs in
[`examples/`](examples/).

`kraken-sdk` is a library crate (`kraken_sdk`). There is **no CLI binary** — you
generate Rust that links the crate.

## Shape of the API

One `Client`, built once, then five namespaces:

```rust
use kraken_sdk::{ClientBuilder, Symbol, Transport};

let client = ClientBuilder::new().build()?;   // add .with_api_key(key, secret) for private calls
client.ready().await?;                          // resolves once the reactor loop is live
```

Embedding apps can attribute REST traffic via `.with_headers(HeaderMap)` — a
`user-agent` entry is the app token; the SDK token is always appended (see
`docs/features.md` → "Embedding identity").

- `client.market()` — public data. REST (async): `ticker`, `orderbook`,
  `trades` (takes `TradesRequest::new(pair)`), `ohlc` (takes
  `OhlcRequest::new(pair, interval)`), `spreads`, `assets` (`Option<&[&str]>`),
  `pairs`, `server_time`, `status`.
  WebSocket callbacks (sync; the bare `on_*` forms return a handle, the
  `*_for` combiners return `Result<SubscriptionGuard, _>`):
  `on_ticker[_for]`, `on_book[_raw][_for]`, `on_trade[_for]`, `on_ohlc[_for]`,
  `on_system_status`.
- `client.account()` — private reads (async): `balance`, `extended_balance`,
  `trade_balance`, `open_orders`, `closed_orders`, `ledgers`, `query_ledgers`,
  `trades_history`, `positions`, `volume`, `find_order_by_cl_ord_id`. WebSocket:
  `on_executions`, `on_balances`. The four filtered reads take one request
  struct built from `Default` — e.g.
  `closed_orders(ClosedOrdersRequest::default().trades(true))`; unset filters
  are not sent.
- `client.trade()` — orders. Each returns a `PendingTrade`:
  `market_buy/sell(pair, volume)`, `limit_buy/sell(pair, volume, price)`,
  `stop_loss_buy/sell(pair, volume, trigger)`, generic `order(OrderRequest)`,
  `order_amend(request)`, `cancel(cl_ord_id)`, `cancel_batch`, `cancel_all`,
  `cancel_all_orders_after(timeout)`, `order_batch`.
- `client.subscription()` — manage WebSocket streams: `subscribe_*` /
  `unsubscribe_*` (ticker, book, book_raw, trade, ohlc, system_status,
  executions, balances), plus bulk `unsubscribe_all` / `unsubscribe_channel`
  and the read models `list_active` / `find_by_channel` / `status_summary`.
- `client.events()` / `client.bus()` — lifecycle event bus
  (`on(EventType, cb)`): connection state, rate-limit warnings, order-book gaps,
  queue-depth samples, slow-callback warnings.

## Hard rules — follow these or the code is wrong

1. **Symbols are the modern form only.** Build with `Symbol::new("BTC/USD")?`
   (returns `Result<Symbol, SymbolError>`). NEVER use legacy codes (`XBT`,
   `XXBT`, `ZUSD`, `XXBTZUSD`, wsname) — passing one raises a typed error. Asset
   codes returned in balances/ledgers are likewise modern (`"BTC"`, `"USD"`).
2. **Money is `rust_decimal::Decimal`, never `f64`.** Parse from string:
   `"0.001".parse()?`. Do not construct prices/volumes from floats.
3. **Trading is two-step: `PendingTrade` → (optional transport) → await.**
   `client.trade().limit_buy(pair, vol, px).await?` sends over the default
   transport (Spot **WebSocket v2 auth**); call `.via(Transport::Rest)` before
   `.await` to force REST instead. A client-order-id (UUID v4) is auto-set as the
   idempotency key. Note: a plain `AddOrder` is **never auto-retried** — if a
   send fails ambiguously, query order status rather than blindly resending.
4. **Everything fallible returns `Result` with a typed error.** Per-namespace
   enums (`MarketError`, `AccountError`, `TradeError`, `SubscriptionError`, …),
   each exposing `code`, `category` (config/auth/network/exchange/client),
   `retryable`, `request_id`, `message`. Propagate with `?` and match on
   variants where the caller must react (e.g. `TradeError::InsufficientFunds`,
   `MarketError::SymbolNotFound`).
5. **The SDK never blocks on rate limits.** It emits a `RateLimitWarning` event
   at a threshold; it will not sleep for you. If you need backpressure, subscribe
   to that event and pace your own calls. Details:
   [`docs/guides/rate-limits.md`](docs/guides/rate-limits.md).
6. **WebSocket data callbacks run on the dispatch loop — keep them fast.**
   Signature is `Fn(&T) + Send + Sync + 'static`. Do not `.await`, sleep, or do
   heavy work inside an `on_*` callback; hand off to a channel/task. Clone only
   when retaining the payload. Slow / saturated delivery behaviour (and the
   dual-loop model) is defined in
   [`docs/architecture.md`](docs/architecture.md#dual-loop).
   A subscription is two parts: register the callback (`on_ticker(...)`) **and**
   subscribe (`subscription().subscribe_ticker(vec![pair], None, None)?`).
7. **Credentials come from the environment or a secrets manager — never
   hard-code them**, never log them, never commit them. Pass via
   `.with_api_key(key, secret)` — infallible; a malformed secret reports as
   `ConfigError::InvalidCredentials` from `.build()`.
8. **`.build()` does no network I/O.** Don't expect it to validate that a pair
   exists or that the market is reachable — that happens at first use.

## Copy-paste patterns

Public REST call (`ticker` takes `Option<&[Symbol]>` and returns a `TickerResult` map; `None` = all pairs):
```rust
let pair = Symbol::new("BTC/USD")?;
let tickers = client.market().ticker(Some(&[pair.clone()])).await?;
if let Some(t) = tickers.get(&pair) {
    println!("{}", t.last_price);
}
```

Stream a channel (register callback, then subscribe):
```rust
let _h = client.market().on_trade(|tr| { /* fast, non-blocking */ });
client.subscription().subscribe_trade(vec![Symbol::new("BTC/USD")?], None)?;
```

Place and cancel an order:
```rust
let resp = client.trade()
    .limit_buy(Symbol::new("BTC/USD")?, "0.001".parse()?, "50000.0".parse()?)
    .via(Transport::Rest)
    .await?;
if let Some(id) = resp.cl_ord_id {            // or use resp.txid
    client.trade().cancel(id).via(Transport::Rest).await?;
}
```

Maintained order book (CRC-validated, gap-recovered):
```rust
let _h = client.market().on_book(|book| {
    // book.bids / book.asks are the current top-N, already validated
});
client.subscription().subscribe_book(vec![Symbol::new("BTC/USD")?], kraken_sdk::BookDepth::D10)?;
```

React to lifecycle events:
```rust
// keep the returned guard alive — dropping it unsubscribes the handler
let _sub = client.events().on(kraken_sdk::EventType::RateLimitWarning, |ev| { /* slow down */ })?;
```

## Don'ts

- Don't use `f64` for any price/volume/amount.
- Don't `.await` or block inside an `on_*` callback.
- Don't pass legacy symbol/asset codes.
- Don't retry a failed `AddOrder` blindly — reconcile via order status.
- Don't expect `.build()` to reach the network or validate live data.
- Don't hard-code or log API keys/secrets.

## When unsure

Read the matching program in [`examples/`](examples/) (e.g. `quickstart.rs`,
`ticker.rs`, `orderbook.rs`, `advanced_orders.rs`, `live_streams.rs`) and the
recipe in [`docs/guides/`](docs/guides/) — they show the current, correct usage.
