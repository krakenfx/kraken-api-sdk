//! Caller demo of the shorthand order methods
//! (`client.trade().{market,limit,stop_loss}_{buy,sell}`). Default run builds each
//! PendingTrade without sending; KRAKEN_PLACE_REAL=1 adds a live place-far/cancel.

use std::process::ExitCode;
use std::str::FromStr;

use kraken_sdk::{ApiKey, Client, OrderRequest, OrderType, Side, Symbol, Transport};
use rust_decimal::Decimal;

mod common;
use common::load_env_var;

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

    let pair = Symbol::new("BTC/USDC").expect("valid pair");
    let vol = Decimal::from_str("0.0001").unwrap();
    let limit_px = Decimal::from_str("33000").unwrap();
    let stop_px = Decimal::from_str("200000").unwrap();

    // Surface demo (no network): shorthand returns PendingTrade; cl_ord_id readable before .await.
    println!("== shorthand surface (PendingTrade built, cl_ord_id pre-allocated, NOT sent) ==");
    let demos = [
        (
            "market_buy",
            client
                .trade()
                .market_buy(pair.clone(), vol)
                .cl_ord_id()
                .cloned(),
        ),
        (
            "market_sell",
            client
                .trade()
                .market_sell(pair.clone(), vol)
                .cl_ord_id()
                .cloned(),
        ),
        (
            "limit_buy",
            client
                .trade()
                .limit_buy(pair.clone(), vol, limit_px)
                .cl_ord_id()
                .cloned(),
        ),
        (
            "limit_sell",
            client
                .trade()
                .limit_sell(pair.clone(), vol, limit_px)
                .cl_ord_id()
                .cloned(),
        ),
        (
            "stop_loss_buy",
            client
                .trade()
                .stop_loss_buy(pair.clone(), vol, stop_px)
                .cl_ord_id()
                .cloned(),
        ),
        (
            "stop_loss_sell",
            client
                .trade()
                .stop_loss_sell(pair.clone(), vol, stop_px)
                .cl_ord_id()
                .cloned(),
        ),
    ];
    for (name, cl) in &demos {
        match cl {
            Some(id) => println!("  client.trade().{name}(..)  →  cl_ord_id={}", id.as_str()),
            None => {
                println!("  [FAIL] {name} produced no cl_ord_id");
                return ExitCode::from(1);
            }
        }
    }
    println!();

    // Wire-acceptance via validate=true — docs/guides/placing-orders.md.
    println!("== validate=true wire-acceptance (limit + stop-loss shapes; nothing placed) ==");
    for (label, ot, px) in [
        ("limit_buy shape", OrderType::Limit, limit_px),
        ("stop_loss_buy shape", OrderType::StopLoss, stop_px),
    ] {
        // Shorthands set `price` for both limit and stop-loss.
        let req = OrderRequest::new(pair.clone(), vol, Side::Buy)
            .order_type(ot)
            .price(px)
            .validate_only(true);
        match client.trade().order(req).via(Transport::Rest).await {
            Ok(resp) => println!(
                "  [OK] {label}: txid={:?} (None expected) descr={:?}",
                resp.txid.as_ref().map(|t| t.as_str()),
                resp.descr.order
            ),
            Err(e) => {
                println!("  [FAIL] {label} validate errored: {e:?}");
                return ExitCode::from(1);
            }
        }
    }
    println!();

    if std::env::var("KRAKEN_PLACE_REAL")
        .unwrap_or_default()
        .is_empty()
    {
        println!(
            "Mode 3 SKIPPED — set KRAKEN_PLACE_REAL=1 for the live place-far/cancel round-trip."
        );
        return ExitCode::SUCCESS;
    }

    println!(
        "== live: limit_buy 0.0001 BTC/USDC @ 33000 (far below market — rests, won't fill) =="
    );
    let pending = client.trade().limit_buy(pair.clone(), vol, limit_px);
    let cl_ord_id = pending.cl_ord_id().cloned().expect("cl_ord_id allocated");
    match pending.via(Transport::Rest).await {
        Ok(resp) => {
            println!(
                "  [OK] placed: txid={:?} descr={:?} cl_ord_id={}",
                resp.txid.as_ref().map(|t| t.as_str()),
                resp.descr.order,
                cl_ord_id.as_str()
            );
        }
        Err(e) => {
            println!("  [FAIL] place errored: {e:?}");
            return ExitCode::from(1);
        }
    }

    println!("== cancel the resting order by cl_ord_id ==");
    match client.trade().cancel(cl_ord_id).via(Transport::Rest).await {
        Ok(resp) => {
            println!("  [OK] cancelled: count={}", resp.count);
            ExitCode::SUCCESS
        }
        Err(e) => {
            println!("  [FAIL] cancel errored: {e:?} — CHECK FOR A RESTING ORDER MANUALLY");
            ExitCode::from(1)
        }
    }
}
