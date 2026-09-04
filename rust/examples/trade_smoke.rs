//! Live trade-path smoke: orders go over the default WS v2 transport, OpenOrders
//! over signed REST. Stage 1 runs validate-only (nothing placed); Stage 2, gated
//! by KRAKEN_PLACE_REAL=1, places a far-from-market order and cancels it.

use std::process::ExitCode;
use std::str::FromStr;

use kraken_sdk::{ApiKey, Client, OpenOrdersRequest, OrderRequest, OrderType, Side, Symbol};
use rust_decimal::Decimal;

mod common;
use common::{env_or, load_env_var};

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

    // Orders default to WS v2; ready() required before any WS order.
    client.ready().await.expect("client ready");

    let pair_str = env_or("KRAKEN_PAIR", "BTC/USD");
    let side = env_or("KRAKEN_SIDE", "buy").to_lowercase();
    let is_sell = side == "sell";
    let volume_str = env_or("KRAKEN_VOLUME", "0.0001");
    let price_str = env_or(
        "KRAKEN_LIMIT_PRICE",
        if is_sell { "150000" } else { "20000" },
    );

    let pair = match Symbol::new(&pair_str) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: bad pair {pair_str:?} — {e:?}");
            return ExitCode::from(2);
        }
    };
    let volume = Decimal::from_str(&volume_str).expect("KRAKEN_VOLUME must parse as Decimal");
    let price = Decimal::from_str(&price_str).expect("KRAKEN_LIMIT_PRICE must parse as Decimal");

    println!("trade-path smoke (orders via WS v2; OpenOrders via REST)");
    println!(
        "  params: side={side} pair={pair_str} volume={volume_str} limit_price={price_str} (far from market — should NOT fill)\n"
    );

    let mut failures: Vec<&str> = Vec::new();
    let order_side = if is_sell { Side::Sell } else { Side::Buy };

    println!("STAGE 1 — validate-mode AddOrder (validate=true, nothing is placed)");
    {
        let req = OrderRequest::new(pair.clone(), volume, order_side)
            .order_type(OrderType::Limit)
            .price(price)
            .validate_only(true);
        let outcome = client.trade().order(req).await;
        match outcome {
            Ok(resp) => {
                // validate: descr-only result, txid=None — docs/guides/placing-orders.md.
                println!(
                    "  [OK]   validate decoded: txid={:?} (None expected) descr.order={:?}",
                    resp.txid.as_ref().map(|t| t.as_str()),
                    resp.descr.order
                );
                if resp.txid.is_some() {
                    println!(
                        "  [WARN] validate response carried a txid — unexpected; did a real order slip through?"
                    );
                    failures.push("validate");
                }
            }
            Err(e) => {
                println!("  [FAIL] validate-mode order errored: {e:?}");
                failures.push("validate");
            }
        }
    }
    println!();

    if env_or("KRAKEN_PLACE_REAL", "").is_empty() {
        println!(
            "STAGE 2 — SKIPPED (set KRAKEN_PLACE_REAL=1 to place a real far-from-market order)"
        );
        println!();
        return finish(&failures);
    }

    println!(
        "STAGE 2 — REAL order lifecycle (place far-from-market → verify in OpenOrders → cancel)"
    );

    let req = OrderRequest::new(pair.clone(), volume, order_side)
        .order_type(OrderType::Limit)
        .price(price);
    let placed = client.trade().order(req).await;
    let resp = match placed {
        Ok(r) => {
            println!(
                "  [OK]   placed: txid={:?} cl_ord_id={:?} descr={:?}",
                r.txid.as_ref().map(|t| t.as_str()),
                r.cl_ord_id.as_ref().map(|c| c.as_str()),
                r.descr.order
            );
            r
        }
        Err(e) => {
            println!("  [FAIL] real AddOrder failed: {e:?}");
            failures.push("add_order");
            return finish(&failures);
        }
    };

    // Real placement (validate=false) always carries a txid; guard anyway.
    let txid = match resp.txid.as_ref() {
        Some(t) => t.as_str(),
        None => {
            println!("  [FAIL] real AddOrder returned no txid (unexpected for validate=false)");
            failures.push("add_order_txid");
            return finish(&failures);
        }
    };

    match client
        .account()
        .open_orders(OpenOrdersRequest::default())
        .await
    {
        Ok(oo) => {
            if oo.open.contains_key(txid) {
                println!("  [OK]   open_orders contains the resting order (by txid)");
            } else {
                println!(
                    "  [WARN] open_orders did not contain txid {txid} ({} open orders) — it may have filled or been rejected; will still attempt cancel",
                    oo.open.len()
                );
            }
        }
        Err(e) => {
            println!("  [FAIL] open_orders query failed: {e:?}");
            failures.push("open_orders");
        }
    }

    // Cancel by cl_ord_id (auto-allocated on plain orders; else use OpenOrders/ClosedOrders).
    let Some(cl_ord_id) = resp.cl_ord_id.clone() else {
        println!(
            "  [WARN] no cl_ord_id on the response (suppressed?) — skipping cancel-by-cl_ord_id; cancel manually if it rests!"
        );
        return finish(&failures);
    };
    match client.trade().cancel(cl_ord_id).await {
        Ok(c) => {
            println!("  [OK]   cancel: count={} pending={}", c.count, c.pending);
            if c.count == 0 {
                println!(
                    "  [WARN] cancel count=0 — order already gone (filled/expired?). Check the account."
                );
            }
        }
        Err(e) => {
            println!(
                "  [FAIL] cancel failed: {e:?} — ⚠️ a live order may still rest; cancel it manually!"
            );
            failures.push("cancel");
        }
    }
    println!();

    finish(&failures)
}

fn finish(failures: &[&str]) -> ExitCode {
    if failures.is_empty() {
        println!("RESULT: trade-path smoke completed with no hard failures.");
        ExitCode::SUCCESS
    } else {
        println!("RESULT: {} failure(s): {:?}", failures.len(), failures);
        ExitCode::from(1)
    }
}
