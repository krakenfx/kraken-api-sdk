# Kraken SDK (Rust)

An async-native Rust SDK for the [Kraken](https://www.kraken.com) cryptocurrency
exchange. One unified, domain-grouped interface over Kraken's **Spot REST** and
**Spot WebSocket v2** APIs and the **Unified WebSocket** for Futures. You write
`client.market().ticker(...)` or `client.trade().futures().order(...)` and never
touch transport-specific wiring.

## Highlights

- **One client, five namespaces** — `market` (public data), `account`
  (balances/orders/ledgers), `trade` (order placement), `subscription`
  (WebSocket streams), and `events` (lifecycle bus). Each data namespace has a
  `.futures()` view for the Futures product.
- **Async where it matters.** REST calls are `async`. WebSocket `on_*` data
  callbacks run on the dispatch loop (the I/O loop decodes frames and maintains
  the order book, then hands the decoded value over), so a slow callback no
  longer stalls socket I/O or order sends — but it still emits a
  `SlowCallbackWarning` and can gap data under load, so keep it fast and
  non-blocking, handing heavy work off to your own task. (Lifecycle handlers via
  `client.events()` run on the same dispatch loop.)
- **Typed everything.** Strongly-typed requests, responses, and a structured
  error taxonomy (`code` · `category` · `retryable` · `request_id` · `message`).
- **Exact money.** All monetary values are `rust_decimal::Decimal` — never
  `f64`.
- **Maintained order book.** The `book` channel is reconstructed into a live
  top-N book, CRC32-validated against the exchange on every update, with
  automatic gap recovery.
- **Rate-limit aware, never blocking.** The SDK tracks your usage and emits a
  warning event at a configurable threshold; it never silently sleeps.
- **Modern symbols only.** Spot pairs are the modern form (`BTC/USD`); legacy
  wire codes (`XBT`, `XXBT`, `ZUSD`) never appear in caller-facing strings.
  Futures symbols are the venue wire strings (`PF_XBTUSD`, `PI_XBTUSD`), checked
  at the call site.

## Install

Depend on the tagged release:

```toml
[dependencies]
kraken-sdk = { git = "https://github.com/krakenfx/kraken-api-sdk", tag = "v0.2.0" }
tokio = { version = "1", features = ["full"] }
rust_decimal = "1"
```

## Quickstart: Spot

```rust
use kraken_sdk::{ClientBuilder, Symbol};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Public-data client — no credentials needed.
    let client = ClientBuilder::new().build()?;
    client.ready().await?;

    // REST: fetch a ticker. `ticker` returns a map keyed by pair; look yours up.
    let pair = Symbol::new("BTC/USD")?;
    let tickers = client.market().ticker(Some(&[pair.clone()])).await?;
    if let Some(t) = tickers.get(&pair) {
        println!("BTC/USD last = {}", t.last_price);
    }

    // WebSocket: stream live tickers (keep the callback fast — it runs on the dispatch loop).
    let _handle = client.market().on_ticker(|t| {
        println!("[stream] {} = {}", t.symbol.as_str(), t.last);
    })?;
    client.subscription().subscribe_ticker(vec![pair], None, None)?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    Ok(())
}
```

## Quickstart: Futures

Futures data streams on the Unified WebSocket; the Futures surface has no REST
methods. Symbols are venue wire strings, not `Symbol`. Each `on_*_for` call
registers the handler and subscribes in one step, and returns a guard that
unsubscribes when dropped.

```rust
use kraken_sdk::{BookDepth, ClientBuilder, FuturesTickerUpdate};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Public-data client — no credentials needed.
    let client = ClientBuilder::new().build()?;
    client.ready().await?;
    let futures = client.market().futures();
    let symbols = vec!["PF_XBTUSD".to_string()];

    // Ticker: perpetual, fixed-expiry and inverse-expiry contracts have their own shapes.
    let _ticker = futures.on_ticker_for(symbols.clone(), None, |t| {
        if let FuturesTickerUpdate::Perpetual(p) = t {
            println!("[ticker] {} mark = {} funding = {}", p.symbol, p.mark, p.funding_rate);
        }
    })?;

    // Maintained top-10 book, CRC32-checked like Spot.
    let _book = futures.on_book_for(symbols, BookDepth::D10, |b| {
        if let (Some(bid), Some(ask)) = (b.bids.first(), b.asks.first()) {
            println!("[book] {} {} / {}", b.symbol, bid.price, ask.price);
        }
    })?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    Ok(())
}
```

`client.market().futures()` also streams system status, instrument snapshots,
trades, OHLC candles and raw books. Runnable:
[`examples/futures_streams.rs`](examples/futures_streams.rs). More:
[Streaming → Futures streams](docs/guides/streaming.md#futures-streams).

## Authenticated calls

Private endpoints (balances, orders, trading) need an API key/secret. Provide
them on the builder — credentials are held in memory only, never logged or
written to disk, and zeroed on close. One key covers Spot and Futures.

### Spot

```rust
use kraken_sdk::{ApiKey, ClientBuilder, Symbol, Transport};

let client = ClientBuilder::new()
    .with_api_key(ApiKey::new(api_key), api_secret) // creds from env / a secrets manager, never hard-coded
    .build()?;
client.ready().await?;

let balance = client.account().balance().await?;
println!("{:?}", balance.assets); // keys are modern codes: "BTC", "USD", ...

// Order methods return a `PendingTrade`; pick the transport with `.via(...)`,
// then `.await`. The default is the authenticated WebSocket (`Transport::WsV2Auth`).
let order = client.trade()
    .limit_buy(Symbol::new("BTC/USD")?, "0.001".parse()?, "50000.0".parse()?)
    .via(Transport::Rest)
    .await?;
println!("placed order: {:?}", order.txid);
```

### Futures

Futures orders and private streams run on the Unified private WebSocket.
`FuturesOrder::new` is a market order; set `.order_type(..)` for anything else.
`.validate_only(true)` asks the venue to check the order without placing it.

```rust
use kraken_sdk::{ApiKey, ClientBuilder, FuturesOrder, FuturesOrderRequest, OrderType, Side};
use rust_decimal::Decimal;

let client = ClientBuilder::new()
    .with_api_key(ApiKey::new(api_key), api_secret)
    .build()?;
client.ready().await?;

// Private streams: executions, positions, margin, balances, preferences.
let _executions = client.account().futures().on_executions_for(None, None, |u| {
    println!("[executions] {} snapshot={}", u.exec_type, u.is_snapshot);
})?;

// Orders: a limit buy, validated by the venue but not placed.
let order = FuturesOrder::new(Side::Buy, "0.01".parse()?)
    .order_type(OrderType::Limit)
    .price("1500".parse::<Decimal>()?);
let checked = client
    .trade()
    .futures()
    .order(FuturesOrderRequest::new("PF_ETHUSD", order).validate_only(true))
    .await?;
println!("validated: {:?}", checked.txid);
```

Amend, cancel, batch and the dead-man timer are on the same namespace. Runnable:
[`examples/futures_orders.rs`](examples/futures_orders.rs). More:
[Placing orders → Futures orders](docs/guides/placing-orders.md#futures-orders).

## Documentation & examples

- **[`docs/`](docs/)** — getting started, the [namespace + feature
  reference](docs/features.md), the [architecture / concurrency model](docs/architecture.md),
  and recipe guides (ticker · placing orders · order book · streaming · errors ·
  configuration).
- **[`examples/`](examples/)** — runnable programs. Start with
  [`quickstart.rs`](examples/quickstart.rs), [`ticker.rs`](examples/ticker.rs),
  and [`orderbook.rs`](examples/orderbook.rs) for Spot, and
  [`futures_streams.rs`](examples/futures_streams.rs) and
  [`futures_orders.rs`](examples/futures_orders.rs) for Futures. The [examples table](examples/README.md)
  marks which need an API key: the **creds** ones read private data or can place
  orders.

```sh
cargo run --example quickstart
```

## Contributing

- **[`CONTRIBUTING.md`](CONTRIBUTING.md)** — building, testing, and code style
  for this crate.
- **[`../CONTRIBUTING.md`](../CONTRIBUTING.md)** — issue reporting and PR policy,
  shared across all language bindings.
