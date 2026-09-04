//! Live public REST smoke: walks every public REST method on `MarketNamespace`
//! against `api.kraken.com` end-to-end (no credentials).
//! Run: `cargo run --example rest_market`.

use std::process::ExitCode;

use kraken_sdk::{Client, OhlcInterval, OhlcRequest, Symbol, TradesRequest};

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("kraken_sdk=warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let client = Client::builder().build().expect("default config validates");

    let pair = Symbol::new("BTC/USD").expect("BTC/USD parses");
    let mut failures: Vec<&str> = Vec::new();

    println!("REST public smoke against api.kraken.com\n");

    match client.market().server_time().await {
        Ok(t) => println!(
            "[OK] server_time     unixtime={} rfc1123={}",
            t.unixtime, t.rfc1123
        ),
        Err(e) => {
            println!("[FAIL] server_time     {:?}", e);
            failures.push("server_time");
        }
    }

    match client.market().status().await {
        Ok(s) => println!(
            "[OK] status          status={} timestamp={}",
            s.status, s.timestamp
        ),
        Err(e) => {
            println!("[FAIL] status          {:?}", e);
            failures.push("status");
        }
    }

    // Asset filters accept modern or legacy; response keys always normalise to modern.
    match client.market().assets(Some(&["XBT", "USD"])).await {
        Ok(a) => println!("[OK] assets          got {} entries", a.assets.len()),
        Err(e) => {
            println!("[FAIL] assets          {:?}", e);
            failures.push("assets");
        }
    }

    match client
        .market()
        .pairs(Some(std::slice::from_ref(&pair)))
        .await
    {
        Ok(p) => println!("[OK] pairs           got {} entries", p.pairs.len()),
        Err(e) => {
            println!("[FAIL] pairs           {:?}", e);
            failures.push("pairs");
        }
    }

    match client
        .market()
        .ticker(Some(std::slice::from_ref(&pair)))
        .await
    {
        Ok(tr) => match tr.get(&pair) {
            Some(t) => println!(
                "[OK] ticker          bid={} ask={} last={}",
                t.bid_price, t.ask_price, t.last_price
            ),
            None => {
                println!("[FAIL] ticker          ticker missing from result");
                failures.push("ticker");
            }
        },
        Err(e) => {
            println!("[FAIL] ticker          {:?}", e);
            failures.push("ticker");
        }
    }

    match client
        .market()
        .ohlc(OhlcRequest::new(pair.clone(), OhlcInterval::M1))
        .await
    {
        Ok(o) => println!(
            "[OK] ohlc            got {} candles, last={}",
            o.candles.len(),
            o.last
        ),
        Err(e) => {
            println!("[FAIL] ohlc            {:?}", e);
            failures.push("ohlc");
        }
    }

    match client.market().orderbook(&pair, Some(10)).await {
        Ok(b) => println!(
            "[OK] orderbook       {} bids, {} asks",
            b.bids.len(),
            b.asks.len()
        ),
        Err(e) => {
            println!("[FAIL] orderbook       {:?}", e);
            failures.push("orderbook");
        }
    }

    match client
        .market()
        .trades(TradesRequest::new(pair.clone()))
        .await
    {
        Ok(r) => println!(
            "[OK] trades          got {} trades, last={}",
            r.trades.len(),
            r.last
        ),
        Err(e) => {
            println!("[FAIL] trades          {:?}", e);
            failures.push("trades");
        }
    }

    match client.market().spreads(pair.clone(), None).await {
        Ok(s) => println!(
            "[OK] spreads         got {} spreads, last={}",
            s.spreads.len(),
            s.last
        ),
        Err(e) => {
            println!("[FAIL] spreads         {:?}", e);
            failures.push("spreads");
        }
    }

    println!();
    if failures.is_empty() {
        println!("RESULT: 9/9 public REST endpoints PASS");
        ExitCode::SUCCESS
    } else {
        println!("RESULT: {} failure(s): {:?}", failures.len(), failures);
        ExitCode::from(1)
    }
}
