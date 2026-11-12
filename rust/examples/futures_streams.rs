//! Futures streams on the Unified WebSocket: public ticker, order book and status;
//! with credentials also executions, positions and balances. Places no orders and
//! prints markers only, never prices or balance amounts. Run: `cargo run --example futures_streams`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use kraken_sdk::{ApiKey, BookDepth, Client, FuturesTickerUpdate};

mod common;
use common::dotenv_lookup;

const SYMBOL: &str = "PF_XBTUSD";

#[tokio::main]
async fn main() {
    let mut builder = Client::builder();
    let credentials = dotenv_lookup("KRAKEN_API_KEY").zip(dotenv_lookup("KRAKEN_API_SECRET"));
    if let Some((key, secret)) = &credentials {
        builder = builder.with_api_key(ApiKey::new(key.clone()), secret.clone());
    }
    let client = builder.build().expect("build");
    client.ready();

    let _status = client
        .market()
        .futures()
        .on_system_status(|s| println!("[status] futures_exchange={}", s.futures_exchange))
        .expect("status handler");
    let _ticker = client
        .market()
        .futures()
        .on_ticker_for(vec![SYMBOL.to_owned()], None, |t| {
            if let FuturesTickerUpdate::Perpetual(p) = t {
                println!("[ticker] {} has_bid={}", p.symbol, p.bid.is_some());
            }
        })
        .expect("ticker guard");
    let printed = AtomicBool::new(false);
    let _book = client
        .market()
        .futures()
        .on_book_for(vec![SYMBOL.to_owned()], BookDepth::D10, move |b| {
            if !printed.swap(true, Ordering::Relaxed) {
                println!(
                    "[book] {} bids={} asks={}",
                    b.symbol,
                    b.bids.len(),
                    b.asks.len()
                );
            }
        })
        .expect("book guard");

    let _private = credentials.is_some().then(|| {
        let account = client.account().futures();
        (
            account
                .on_executions_for(None, None, |u| {
                    println!(
                        "[executions] exec_type={} snapshot={}",
                        u.exec_type, u.is_snapshot
                    )
                })
                .expect("executions guard"),
            account
                .on_positions_for(None, |u| {
                    println!(
                        "[positions] open={} snapshot={}",
                        u.futures.len(),
                        u.is_snapshot
                    )
                })
                .expect("positions guard"),
            account
                .on_balances_for(None, |u| {
                    println!(
                        "[balances] asset={} snapshot={}",
                        u.asset.as_deref().unwrap_or("<none>"),
                        u.is_snapshot
                    )
                })
                .expect("balances guard"),
        )
    });
    if credentials.is_none() {
        println!("[main] no credentials: public streams only");
    }

    println!("[main] streaming for 15s...");
    tokio::time::sleep(Duration::from_secs(15)).await;
    println!("[main] done");
}
