//! Smoke test: fetch the order book top for one symbol via
//! GET api.kraken.com/0/public/Depth?pair=<symbol>&count=<count>.
//! Public endpoint, no API keys. Args: <SYMBOL> [COUNT].

use std::process::ExitCode;

use kraken_sdk::{Client, Symbol};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let symbol_str = match args.get(1) {
        Some(s) => s.clone(),
        None => {
            eprintln!("usage: cargo run --example orderbook -- <SYMBOL> [COUNT]");
            eprintln!("       e.g. cargo run --example orderbook -- BTC/USD 10");
            return ExitCode::from(2);
        }
    };
    let count: u32 = args.get(2).map(|s| s.parse().unwrap_or(10)).unwrap_or(10);

    let symbol = match Symbol::new(&symbol_str) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: invalid symbol {:?} — {}", symbol_str, e);
            return ExitCode::from(2);
        }
    };

    let client = Client::builder()
        .build()
        .expect("build is infallible at M2 with default config");

    println!(
        "fetching orderbook for {} (count={}) ...",
        symbol.as_str(),
        count
    );

    match client.market().orderbook(&symbol, Some(count)).await {
        Ok(book) => {
            println!();
            println!("  ASKS (top {})", book.asks.len());
            for lvl in book.asks.iter().rev() {
                println!("    {:>14} @ {}", lvl.volume, lvl.price);
            }
            println!("  --------");
            println!("  BIDS (top {})", book.bids.len());
            for lvl in book.bids.iter() {
                println!("    {:>14} @ {}", lvl.volume, lvl.price);
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {}", e);
            ExitCode::from(1)
        }
    }
}
