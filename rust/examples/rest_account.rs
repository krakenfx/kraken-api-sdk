//! Live private REST smoke against api.kraken.com/0/private — walks the
//! read-only AccountNamespace methods (no order placement). Credential-safe:
//! no key/secret echoed, no account amounts printed.

use std::process::ExitCode;

use kraken_sdk::{
    ApiKey, Client, ClosedOrdersRequest, LedgersRequest, OpenOrdersRequest, TradesHistoryRequest,
};

mod common;
use common::load_env_var;

#[tokio::main]
async fn main() -> ExitCode {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("kraken_sdk=warn")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let api_key = match load_env_var("KRAKEN_API_KEY") {
        Some(k) if !k.is_empty() => k,
        _ => {
            eprintln!("error: KRAKEN_API_KEY not found in ~/projects/kraken-sdk/.env");
            return ExitCode::from(2);
        }
    };
    let api_secret = match load_env_var("KRAKEN_API_SECRET") {
        Some(s) if !s.is_empty() => s,
        _ => {
            eprintln!("error: KRAKEN_API_SECRET not found in ~/projects/kraken-sdk/.env");
            return ExitCode::from(2);
        }
    };

    let client = match Client::builder()
        .with_api_key(ApiKey::new(api_key), api_secret)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };

    let mut failures: Vec<&str> = Vec::new();

    println!("REST private smoke against api.kraken.com (read-only methods)\n");

    match client.account().balance().await {
        Ok(b) => println!("[OK] balance         {} asset entries", b.assets.len()),
        Err(e) => {
            println!("[FAIL] balance         {:?}", e);
            failures.push("balance");
        }
    }

    match client.account().extended_balance().await {
        Ok(b) => println!("[OK] extended_balance  {} entries", b.assets.len()),
        Err(e) => {
            println!("[FAIL] extended_balance  {:?}", e);
            failures.push("extended_balance");
        }
    }

    match client
        .account()
        .trade_balance(Some("USD".to_string()))
        .await
    {
        Ok(_) => println!("[OK] trade_balance     decoded ok (amounts redacted)"),
        Err(e) => {
            println!("[FAIL] trade_balance     {:?}", e);
            failures.push("trade_balance");
        }
    }

    match client
        .account()
        .open_orders(OpenOrdersRequest::default())
        .await
    {
        Ok(o) => println!("[OK] open_orders       {} entries", o.open.len()),
        Err(e) => {
            println!("[FAIL] open_orders       {:?}", e);
            failures.push("open_orders");
        }
    }

    match client
        .account()
        .closed_orders(ClosedOrdersRequest::default())
        .await
    {
        Ok(c) => println!(
            "[OK] closed_orders     count={} entries={}",
            c.count,
            c.closed.len()
        ),
        Err(e) => {
            println!("[FAIL] closed_orders     {:?}", e);
            failures.push("closed_orders");
        }
    }

    match client.account().ledgers(LedgersRequest::default()).await {
        Ok(l) => println!(
            "[OK] ledgers           count={} entries={}",
            l.count,
            l.ledger.len()
        ),
        Err(e) => {
            println!("[FAIL] ledgers           {:?}", e);
            failures.push("ledgers");
        }
    }

    // docalcs=true → value/net decode as Some on open positions.
    match client.account().positions(None, true).await {
        Ok(p) => println!("[OK] positions         {} entries", p.positions.len()),
        Err(e) => {
            println!("[FAIL] positions         {:?}", e);
            failures.push("positions");
        }
    }

    match client.account().volume(None, false).await {
        Ok(tv) => {
            println!(
                "[OK] volume            currency={} (volume amount redacted)",
                tv.currency
            );
            println!("       The wire response also carries fee maps (fees, fees_maker) — not");
            println!("       surfaced on the typed struct.");
        }
        Err(e) => {
            println!("[FAIL] volume            {:?}", e);
            failures.push("volume");
        }
    }

    match client
        .account()
        .trades_history(TradesHistoryRequest::default())
        .await
    {
        Ok(t) => println!(
            "[OK] trades_history    count={} entries={}",
            t.count,
            t.trades.len()
        ),
        Err(e) => {
            println!("[FAIL] trades_history    {:?}", e);
            failures.push("trades_history");
        }
    }

    println!();
    if failures.is_empty() {
        println!("RESULT: 9/9 private REST read-only endpoints PASS");
        ExitCode::SUCCESS
    } else {
        println!("RESULT: {} failure(s): {:?}", failures.len(), failures);
        ExitCode::from(1)
    }
}
