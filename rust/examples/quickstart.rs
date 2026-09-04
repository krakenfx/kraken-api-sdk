//! START HERE — a 60-second tour of the SDK. Public market data needs no
//! credentials; with KRAKEN_API_KEY + KRAKEN_API_SECRET set, the tour continues
//! into account balance, a live WS ticker, and a validate-mode order (nothing placed).

use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use kraken_sdk::{
    ApiKey, Client, ClientBuilder, OrderRequest, OrderType, Side, Symbol, TickerUpdate,
};
use rust_decimal::Decimal;

#[tokio::main]
async fn main() -> ExitCode {
    let btc_usd = Symbol::new("BTC/USD").expect("BTC/USD is a valid symbol");

    println!("1) REST market data (no credentials)");
    let client = ClientBuilder::new()
        .build()
        .expect("unauthenticated build is infallible");
    match client
        .market()
        .ticker(Some(std::slice::from_ref(&btc_usd)))
        .await
    {
        Ok(tr) => {
            let t = tr.get(&btc_usd).expect("ticker present");
            println!(
                "   BTC/USD  last={}  bid={}  ask={}",
                t.last_price, t.bid_price, t.ask_price
            );
        }
        Err(e) => {
            eprintln!("   ticker failed: {e:?}");
            return ExitCode::from(1);
        }
    }

    let (key, secret) = match (
        std::env::var("KRAKEN_API_KEY")
            .ok()
            .filter(|s| !s.is_empty()),
        std::env::var("KRAKEN_API_SECRET")
            .ok()
            .filter(|s| !s.is_empty()),
    ) {
        (Some(k), Some(s)) => (k, s),
        _ => {
            println!(
                "\n(set KRAKEN_API_KEY + KRAKEN_API_SECRET to continue the authenticated tour)"
            );
            return ExitCode::SUCCESS;
        }
    };
    let client = match Client::builder()
        .with_api_key(ApiKey::new(key), secret)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };

    // ready() starts the I/O reactor — required before any WebSocket use.
    client.ready().await.expect("client ready");

    println!("\n2) Account balance (authenticated REST)");
    match client.account().balance().await {
        Ok(b) => println!("   balance OK — {} assets held", b.assets.len()),
        Err(e) => println!("   balance failed: {e:?}"),
    }

    // on_ticker_for registers + subscribes; hold the SubscriptionGuard (drop tears both down).
    println!("\n3) Live WebSocket ticker (~5s)");
    let ticker_guard = client.market().on_ticker_for(
        std::slice::from_ref(&btc_usd),
        None,
        None,
        |t: &TickerUpdate| println!("   [ws] {} last={}", t.symbol.as_str(), t.last),
    );
    if let Err(e) = &ticker_guard {
        println!("   subscribe failed: {e:?}");
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

    println!("\n4) Validate-mode order (nothing is placed)");
    let req = OrderRequest::new(
        btc_usd.clone(),
        Decimal::from_str("0.0001").unwrap(),
        Side::Buy,
    )
    .order_type(OrderType::Limit)
    .price(Decimal::from_str("20000").unwrap())
    .validate_only(true);
    match client.trade().order(req).await {
        Ok(_) => println!("   validate OK — well-formed; Kraken accepts it"),
        Err(e) => println!("   validate failed: {e:?}"),
    }

    println!(
        "\nTour complete. Next: examples/advanced_orders.rs \
         (conditional-close, trailing-stop, real place+cancel)."
    );
    ExitCode::SUCCESS
}
