# Kraken SDK (Rust)

An async-native Rust SDK for the [Kraken](https://www.kraken.com) cryptocurrency
exchange. One unified, domain-grouped interface over Kraken's **Spot REST** and
**Spot WebSocket v2** APIs — you write `client.market().ticker(...)` or
`client.trade().limit_buy(...)` and never touch transport-specific wiring.

## Highlights

- **One client, five namespaces** — `market` (public data), `account`
  (balances/orders/ledgers), `trade` (order placement), `subscription`
  (WebSocket streams), and `events` (lifecycle bus).
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
- **Modern symbols only.** Pairs are the modern form (`BTC/USD`); legacy wire
  codes (`XBT`, `XXBT`, `ZUSD`) never appear in caller-facing strings.

## Install

```toml
[dependencies]
kraken-sdk = "0.1.0"
tokio = { version = "1", features = ["full"] }
```

## Quickstart

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
    });
    client.subscription().subscribe_ticker(vec![pair], None, None)?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    Ok(())
}
```

### Authenticated calls

Private endpoints (balances, orders, trading) need an API key/secret. Provide
them on the builder — credentials are held in memory only, never logged or
written to disk, and zeroed on close.

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

## Documentation & examples

- **[`docs/`](docs/)** — getting started, the [namespace + feature
  reference](docs/features.md), the [architecture / concurrency model](docs/architecture.md),
  and recipe guides (ticker · placing orders · order book · streaming · errors ·
  configuration).
- **[`examples/`](examples/)** — runnable programs. Start with
  [`quickstart.rs`](examples/quickstart.rs), [`ticker.rs`](examples/ticker.rs),
  and [`orderbook.rs`](examples/orderbook.rs). Examples prefixed `live_` hit the
  real exchange; the public ones (`live_streams`, `live_book`)
  need no credentials, the rest do.

```sh
cargo run --example quickstart
```

## Contributing

- **[`CONTRIBUTING.md`](CONTRIBUTING.md)** — building, testing, and code style
  for this crate.
- **[`../CONTRIBUTING.md`](../CONTRIBUTING.md)** — issue reporting and PR policy,
  shared across all language bindings.
