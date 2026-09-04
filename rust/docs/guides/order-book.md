# Order Book

REST snapshot, maintained WebSocket book, or raw deltas.

## REST snapshot

`client.market().orderbook(symbol, count)` returns top-N levels. No credentials. `count` defaults to 100.

```rust
use kraken_sdk::{Client, Symbol};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::builder().build()?;
    let symbol = Symbol::new("BTC/USD")?;
    let book = client.market().orderbook(&symbol, Some(10)).await?;

    println!("ASKS (best first):");
    for level in book.asks.iter().rev() {
        println!("  {:>14} @ {}", level.volume, level.price);
    }
    println!("BIDS (best first):");
    for level in book.bids.iter() {
        println!("  {:>14} @ {}", level.volume, level.price);
    }
    Ok(())
}
```

`OrderBookLevel`: `price` and `volume` (`Decimal`), `timestamp: u64` (Unix seconds). Runnable: [`examples/orderbook.rs`](../../examples/orderbook.rs).

## Maintained stream

The SDK applies each frame, checks CRC32 on the cumulative top-N, and delivers `OrderBookUpdate`.

```rust
use std::time::Duration;

use kraken_sdk::{BookDepth, ClientBuilder, OrderBookUpdate, Symbol};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = ClientBuilder::new().build()?;

    let _guard = client.market().on_book_for(
        &[Symbol::new("BTC/USD")?],
        BookDepth::D10,
        |update: OrderBookUpdate| {
            let best_bid = update.bids.first().map(|l| l.price);
            let best_ask = update.asks.first().map(|l| l.price);
            println!(
                "{} bids={} asks={} best_bid={:?} best_ask={:?}",
                update.symbol.as_str(),
                update.bids.len(),
                update.asks.len(),
                best_bid,
                best_ask
            );
        },
    )?;

    client.ready().await?;
    tokio::time::sleep(Duration::from_secs(15)).await;
    Ok(())
}
```

`OrderBookUpdate`: `symbol`, `bids` / `asks` (`Vec<BookLevel>` with `price`, `qty`), `checksum: u32`, `timestamp: Option<MonotonicInstant>` (always `None` in v1), `exchange_timestamp: String` (RFC 3339; `""` if absent).

`BookDepth`: `D10`, `D25`, `D100`, `D500`, `D1000`. Depth is fixed at first subscribe for the pair. [Depth coalescing](#depth-coalescing).

### Gap recovery

CRC mismatch: update withheld, `OrderBookGapEvent` and `SubscriptionGapEvent` emitted, unsubscribe + resubscribe for a snapshot. A snapshot-liveness timer counts toward the same budget as a CRC miss.

While waiting for that snapshot, interim deltas are dropped (no fan-out, no extra gap event).

After 5 consecutive recovery failures, maintenance stops. `on_book` then delivers un-validated per-frame data. Subscribe-ack success does not count; only snapshot content mismatch does.

## Raw deltas

`on_book_raw_for` delivers `BookDelta`. `is_snapshot` is the opening frame vs later deltas. No CRC check, no maintained state.

```rust
use kraken_sdk::{BookDelta, BookDepth, ClientBuilder, Symbol};

let _guard = client.market().on_book_raw_for(
    &[Symbol::new("ETH/USD")?],
    BookDepth::D25,
    None, // snapshot
    |delta: BookDelta| {
        if delta.is_snapshot {
            println!(
                "{} SNAPSHOT: {} bids, {} asks",
                delta.symbol.as_str(),
                delta.bids.len(),
                delta.asks.len()
            );
        } else {
            println!(
                "{} DELTA: {} bid changes, {} ask changes",
                delta.symbol.as_str(),
                delta.bids.len(),
                delta.asks.len()
            );
        }
    },
)?;
```

`snapshot: None` keeps Kraken's opening snapshot. `Some(false)` is delta-only. Maintained `on_book_for` / `subscribe_book` always request a snapshot (CRC baseline).

`BookDelta`: `symbol`, `bids` / `asks` (`PriceLevel` with `price`/`qty` plus `price_wire`/`qty_wire`), `checksum: u32` (missing checksum is `SubscriptionGapEvent` / `MalformedFrame`, not `OrderBookGapEvent`), `is_snapshot`, `timestamp` (`None` in v1), `exchange_timestamp`.

Wire channel is always `"book"`. One subscription feeds both handler sets.

### Depth coalescing

First live subscribe for a pair sets wire depth. Later `on_book` / `on_book_raw` at another depth join that subscription; v1 does not widen. After termination, the next subscribe starts a new lifetime.

To change depth: drop every `SubscriptionGuard` and call `unsubscribe_book` or `unsubscribe_book_raw` once per matching plain `subscribe_*` (same `(book, pair)` key — not both for one subscribe). Dropping a bare `on_book` handle does not unsubscribe.

Unsubscribe must echo the subscribed depth. The SDK tracks it. `D10` is subscribed as wire depth 25 (CRC covers top 10; D10 has no headroom). Deeper tiers pass through. The builder still caps to the requested depth.

## Checksum

CRC32 (IEEE) over the post-apply top 10 levels per side: asks (lowest first) then bids (highest first). Concatenate wire `price_wire` + `qty_wire` with decimal point and leading zeros stripped. Do not use `Decimal::to_string()`. Mismatch: drop local book and resubscribe. Maintained mode does this; raw mode exposes the strings.

## Channel-wide handler

```rust
let _handle = client.market().on_book(|update: OrderBookUpdate| {
    println!("{} book update: {} bids", update.symbol.as_str(), update.bids.len());
});
client.ready().await?;
client
    .subscription()
    .subscribe_book(vec![Symbol::new("BTC/USD")?], BookDepth::D10)?;
```

## Related

- [Streaming](streaming.md)
- [`examples/live_book.rs`](../../examples/live_book.rs)
