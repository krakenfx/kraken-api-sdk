//! Fetch and pretty-print a ticker for one symbol from the live public REST
//! endpoint (no API keys). Pass the symbol as an argument:
//! `cargo run --example ticker -- BTC/USD`.

use std::process::ExitCode;

use kraken_sdk::{Client, Symbol};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let symbol_str = match args.get(1) {
        Some(s) => s.clone(),
        None => {
            eprintln!("usage: cargo run --example ticker -- <SYMBOL>");
            eprintln!("       e.g. cargo run --example ticker -- BTC/USD");
            return ExitCode::from(2);
        }
    };

    let symbol = match Symbol::new(&symbol_str) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: invalid symbol {:?} — {}", symbol_str, e);
            return ExitCode::from(2);
        }
    };

    let client = Client::builder()
        .build()
        .expect("default config passes build-time validation");

    println!("fetching ticker for {} ...", symbol.as_str());

    match client
        .market()
        .ticker(Some(std::slice::from_ref(&symbol)))
        .await
    {
        Ok(tr) => {
            let t = tr.get(&symbol).expect("ticker present");
            println!();
            println!(
                "  ask    : {} (whole lot {}, lot {})",
                t.ask_price, t.ask_whole_lot_volume, t.ask_lot_volume
            );
            println!(
                "  bid    : {} (whole lot {}, lot {})",
                t.bid_price, t.bid_whole_lot_volume, t.bid_lot_volume
            );
            println!("  last   : {} ({})", t.last_price, t.last_volume);
            println!("  volume : {} today / {} 24h", t.volume_today, t.volume_24h);
            println!("  vwap   : {} today / {} 24h", t.vwap_today, t.vwap_24h);
            println!("  high   : {} today / {} 24h", t.high_today, t.high_24h);
            println!("  low    : {} today / {} 24h", t.low_today, t.low_24h);
            println!("  trades : {} today / {} 24h", t.trades_today, t.trades_24h);
            println!("  open   : {}", t.open);
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {}", e);
            ExitCode::from(1)
        }
    }
}
