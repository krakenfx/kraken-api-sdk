//! Futures order entry over the Unified private WebSocket; dry-runs by default (`validate_only`).
//! `KRAKEN_PLACE_REAL=1` rests, amends and cancels a far take-profit.

use std::process::ExitCode;
use std::str::FromStr;

use kraken_sdk::{
    ApiKey, ClOrdId, Client, FuturesOrder, FuturesOrderAmendRequest, FuturesOrderBatchRequest,
    FuturesOrderRequest, OrderType, Side, TriggerKind,
};
use rust_decimal::Decimal;

mod common;
use common::load_env_var;

const SYMBOL: &str = "PF_ETHUSD";

fn dec(value: &str) -> Decimal {
    Decimal::from_str(value).expect("decimal literal")
}

fn resting_order() -> FuturesOrder {
    FuturesOrder::new(Side::Sell, dec("0.01"))
        .order_type(OrderType::TakeProfit)
        .trigger(TriggerKind::Last)
        .price(dec("20000"))
        .cl_ord_id(ClOrdId::allocate_v4())
}

async fn run(client: &Client, place_real: bool) -> Result<(), Box<dyn std::error::Error>> {
    let futures = client.trade().futures();

    let dry = futures
        .order(FuturesOrderRequest::new(SYMBOL, resting_order()).validate_only(true))
        .await?;
    println!("dry run: txid={:?}", dry.txid);
    let rows = vec![resting_order(), resting_order()];
    let batch = futures
        .order_batch(FuturesOrderBatchRequest::new(SYMBOL, rows).validate_only(true))
        .await?;
    println!("dry-run batch: {} rows", batch.orders.len());

    if !place_real {
        println!("set KRAKEN_PLACE_REAL=1 to rest, amend and cancel a real order");
        return Ok(());
    }
    let order = resting_order();
    let id = order
        .cl_ord_id
        .clone()
        .ok_or("resting order has no cl_ord_id")?;
    let placed = match futures.order(FuturesOrderRequest::new(SYMBOL, order)).await {
        Ok(placed) => placed,
        Err(e) => {
            let _ = futures.cancel(id).await;
            eprintln!("placement failed: {e} (a live order may still rest; cancel it manually)");
            return Err(e.into());
        }
    };
    println!("placed: txid={:?}", placed.txid);
    let amended = futures
        .order_amend(FuturesOrderAmendRequest::by_cl_ord_id(id.clone()).trigger_price(dec("20500")))
        .await;
    let cancelled = futures.cancel(id).await;
    match &cancelled {
        Ok(c) => println!("cancelled {}", c.count),
        Err(e) => eprintln!("cancel failed: {e} (a live order may still rest; cancel it manually)"),
    }
    amended?;
    cancelled?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let (Some(key), Some(secret)) = (
        load_env_var("KRAKEN_API_KEY").filter(|s| !s.is_empty()),
        load_env_var("KRAKEN_API_SECRET").filter(|s| !s.is_empty()),
    ) else {
        eprintln!("error: KRAKEN_API_KEY/SECRET missing in ~/projects/kraken-sdk/.env");
        return ExitCode::from(2);
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
    if let Err(e) = client.ready().await {
        eprintln!("error: {e}");
        return ExitCode::from(1);
    }
    let place_real = std::env::var("KRAKEN_PLACE_REAL").as_deref() == Ok("1");
    let outcome = run(&client, place_real).await;
    let _ = client.close().await;
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(1)
        }
    }
}
