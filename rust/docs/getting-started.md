# Getting Started

## Installation

Add the crate to `Cargo.toml`:

```toml
[dependencies]
kraken-sdk = "0.1.0"
tokio = { version = "1", features = ["full"] }
rust_decimal = "1"
```

Requires Rust 1.85 or later (edition 2024) and the tokio async runtime.

## Create a client

`Client::builder()` (or `ClientBuilder::new()`) chains options and `.build()`. Build validates configuration only. It does not open sockets or call the exchange.

```rust
use kraken_sdk::{ApiKey, Client, ClientBuilder};

// Public data
let client = ClientBuilder::new().build()?;

// Authenticated — credentials from the environment
let client = Client::builder()
    .with_api_key(
        ApiKey::new(std::env::var("KRAKEN_API_KEY")?),
        std::env::var("KRAKEN_API_SECRET")?,
    )
    .build()?;
```

A malformed secret is `ConfigError::InvalidCredentials` at `.build()`. Credentials are not written to disk or logs, and are zeroed on close. Full option set: [Configuration](guides/configuration.md).

## Public REST

```rust
use kraken_sdk::{Client, Symbol};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::builder().build()?;

    let btc_usd = Symbol::new("BTC/USD")?;
    let tr = client.market().ticker(Some(&[btc_usd.clone()])).await?;
    let ticker = tr
        .get(&btc_usd)
        .ok_or_else(|| anyhow::anyhow!("ticker BTC/USD missing"))?;

    println!(
        "BTC/USD last={} bid={} ask={}",
        ticker.last_price, ticker.bid_price, ticker.ask_price
    );
    Ok(())
}
```

Runnable copy: [`examples/ticker.rs`](../examples/ticker.rs).

## Private REST

Read credentials from the environment or a secrets manager. Do not hard-code them. See [Configuration → credentials](guides/configuration.md#credentials).

```rust
use kraken_sdk::{ApiKey, Client};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::builder()
        .with_api_key(
            ApiKey::new(std::env::var("KRAKEN_API_KEY")?),
            std::env::var("KRAKEN_API_SECRET")?,
        )
        .build()?;

    let balance = client.account().balance().await?;
    println!("{} assets held", balance.assets.len());
    Ok(())
}
```

REST does not require `client.ready()`.

## WebSocket streams

Subscriptions and WebSocket order placement need the I/O reactor. Start it with `client.ready()` before the first WebSocket operation.

```rust
client.ready().await?; // Ok(()) when the loop is up
```

`ready()` returns `Err(ReadyError)` if the reactor cannot start (`ReactorSpawnFailed`) or a loop has died (`LoopFailed`). Connections open on first use. A second call is a no-op for the already-running loop. Bound the wait with `tokio::time::timeout(dur, client.ready())`.

```rust
use std::time::Duration;

use kraken_sdk::{ClientBuilder, Symbol, TickerUpdate};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = ClientBuilder::new().build()?;

    let _guard = client.market().on_ticker_for(
        &[Symbol::new("BTC/USD")?],
        None, // Kraken default snapshot
        None, // default trigger (each trade)
        |update: &TickerUpdate| {
            println!("{} last={}", update.symbol.as_str(), update.last);
        },
    )?;

    client.ready().await?;
    tokio::time::sleep(Duration::from_secs(10)).await;
    Ok(())
    // Dropping `_guard` unsubscribes and deregisters the handler
}
```

`on_ticker_for` registers the handler and sends the subscribe frame. Dropping the guard sends unsubscribe.

Authenticate, balance, WS ticker, and a validate-mode order: [`examples/quickstart.rs`](../examples/quickstart.rs).

## Shutdown

Dropping `Client` aborts the reactor. For a drain of in-flight WebSocket requests, a clean socket close, and a best-effort dead-man disarm over REST, call `client.close()`. It consumes the client.

```rust
client.close().await?;
```

`Ok(())` on a clean drain. `Err(CloseError::Interrupted)` if a reactor loop dies during close or is already dead. Call `close()` from a tokio runtime. To start the drain without waiting: `let _ = client.close();`.
