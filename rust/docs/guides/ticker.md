# Ticker

REST one-shot or WebSocket stream for a pair's last, bid, and ask.

## REST

`client.market().ticker(pairs)` is `GET /0/public/Ticker`. No credentials. `Some(&[symbol])` for one pair; `None` for all. Returns `TickerResult` with `.get(&symbol)`.

```rust
use kraken_sdk::{Client, Symbol};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::builder().build()?;
    let symbol = Symbol::new("BTC/USD")?;

    let tr = client.market().ticker(Some(&[symbol.clone()])).await?;
    let t = tr
        .get(&symbol)
        .ok_or_else(|| anyhow::anyhow!("ticker BTC/USD missing"))?;

    println!("ask={} bid={} last={}", t.ask_price, t.bid_price, t.last_price);
    println!("volume_24h={} vwap_24h={}", t.volume_24h, t.vwap_24h);
    Ok(())
}
```

`Ticker` prices are `Decimal`. `*_24h` is a rolling 24-hour window; `*_today` is the current day. Ask/bid also have `*_whole_lot_volume` / `*_lot_volume`; last trade has `last_volume`.

Runnable: [`examples/ticker.rs`](../../examples/ticker.rs).

## WebSocket — combiner

`on_ticker_for` registers and subscribes. Drop the `SubscriptionGuard` to unsubscribe.

```rust
use std::time::Duration;

use kraken_sdk::{ClientBuilder, Symbol, TickerUpdate};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = ClientBuilder::new().build()?;

    let _guard = client.market().on_ticker_for(
        &[Symbol::new("BTC/USD")?, Symbol::new("ETH/USD")?],
        None, // snapshot
        None, // event_trigger
        |update: &TickerUpdate| {
            println!(
                "{} last={} bid={} ask={}",
                update.symbol.as_str(),
                update.last,
                update.bid,
                update.ask
            );
        },
    )?;

    client.ready().await?;
    tokio::time::sleep(Duration::from_secs(10)).await;
    Ok(())
}
```

`snapshot: Some(false)` skips the opening snapshot. `event_trigger: Some(TickerTrigger::Bbo)` updates on every top-of-book change. `None` keeps Kraken defaults (snapshot on, updates on trades).

WS `TickerUpdate` uses short names (`last`, `volume`). REST `Ticker` uses `last_price`, `volume_24h`. Other fields: `bid`, `bid_qty`, `ask`, `ask_qty`, `vwap`, `low`, `high`, `change`, `change_pct`, `timestamp` (`String`).

## WebSocket — handler then subscribe

```rust
let _handle = client.market().on_ticker(|update: &TickerUpdate| {
    println!("{} last={}", update.symbol.as_str(), update.last);
})?;
client.ready().await?;
client
    .subscription()
    .subscribe_ticker(vec![Symbol::new("BTC/USD")?], None, None)?;
```

No handler: `SubscriptionError::NoHandlerRegistered`. Handler lifetime: [Streaming](streaming.md).

## Futures

`client.market().futures().on_ticker_for` is the Futures combiner. Symbols are native wire strings (`Vec<String>`), not `Symbol`. There is no REST ticker for Futures.

```rust
use kraken_sdk::FuturesTickerUpdate;

client.ready().await?;

let _guard = client.market().futures().on_ticker_for(
    vec!["PF_XBTUSD".to_owned(), "PF_ETHUSD".to_owned()],
    None, // snapshot
    |update: &FuturesTickerUpdate| {
        if let FuturesTickerUpdate::Perpetual(p) = update {
            println!(
                "{} last={:?} bid={:?} ask={:?} funding_rate={}",
                p.symbol, p.last, p.bid, p.ask, p.funding_rate
            );
        }
    },
)?;
```

`snapshot: Some(false)` skips the opening snapshot. `None` keeps Kraken's default. There is no `event_trigger`.

`FuturesTickerUpdate` is an enum by symbol prefix. Match the variant:

| Variant | Symbols | Payload |
|---------|---------|---------|
| `Perpetual` | `PI_`, `PF_` | `FuturesPerpetualTickerUpdate` |
| `ExpiringFixed` | `FF_` | `FuturesExpiringTickerUpdate` |
| `ExpiringInverse` | `FI_` | `FuturesExpiringTickerUpdate` |

- All variants: `symbol`, `timestamp` (RFC 3339), `suspended`, `trades` (`u64`), `index`, `mark`, `open_interest`, `premium`, `volume`, `volume_quote` (`Decimal`).
- Window fields are `Option<Decimal>`, `None` when the venue omits them (no trades in the window, or an empty book side): `ask`, `ask_qty`, `bid`, `bid_qty`, `change`, `change_pct`, `high`, `last`, `low`, `open`, `vwap`.
- `Perpetual` only: `next_funding_rate_time`, `funding_rate`, `funding_rate_prediction`, `relative_funding_rate`, `relative_funding_rate_prediction`.
- `update.symbol()` returns the wire symbol on every variant.

Handler then subscribe works as on Spot: `on_ticker(cb)`, then `subscription().futures().subscribe_ticker(symbols, snapshot)`. No handler: `SubscriptionError::NoHandlerRegistered`.

## Related

- [Streaming](streaming.md)
- [Market data → Unified Futures tickers](market-data.md#unified-futures-tickers)
- [Error handling](error-handling.md)
- [`examples/live_streams.rs`](../../examples/live_streams.rs)
