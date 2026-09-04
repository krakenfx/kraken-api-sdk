//! Live: stream the `book` channel for BTC/USD via both `on_book` (maintained
//! `OrderBookUpdate`) and `on_book_raw` (`BookDelta`). Run: `cargo run --example live_book`.
//! Maintained book: CRC32-validated, mismatch triggers reseed — docs/guides/order-book.md.

use std::time::Duration;

use kraken_sdk::{BookDelta, BookDepth, ClientBuilder, OrderBookUpdate, Symbol};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kraken_sdk=info".into()),
        )
        .init();

    let client = ClientBuilder::new().build().expect("build");
    client.ready();

    let _h = client.market().on_book(|b: &OrderBookUpdate| {
        let top_bid = b.bids.first().map(|l| (l.price, l.qty));
        let top_ask = b.asks.first().map(|l| (l.price, l.qty));
        println!(
            "[book] {} bids={} asks={} top_bid={:?} top_ask={:?} crc={} exch_ts={}",
            b.symbol.as_str(),
            b.bids.len(),
            b.asks.len(),
            top_bid,
            top_ask,
            b.checksum,
            b.exchange_timestamp
        );
    });

    // Raw-delta variant on the same `book` subscription; `is_snapshot` marks the first frame.
    let _hr = client.market().on_book_raw(|d: &BookDelta| {
        println!(
            "[book_raw] {} snapshot={} bids={} asks={} crc={} exch_ts={}",
            d.symbol.as_str(),
            d.is_snapshot,
            d.bids.len(),
            d.asks.len(),
            d.checksum,
            d.exchange_timestamp
        );
    });

    match client
        .subscription()
        .subscribe_book(vec![Symbol::new("BTC/USD").unwrap()], BookDepth::D10)
    {
        Ok(()) => {
            println!("[main] subscribe_book(BTC/USD, D10) OK — first [book] line is the snapshot")
        }
        Err(e) => println!("[main] subscribe error: {e:?}"),
    }

    let secs: u64 = std::env::var("KRAKEN_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(14);
    tokio::time::sleep(Duration::from_secs(secs)).await;
    println!("[main] done");
}
