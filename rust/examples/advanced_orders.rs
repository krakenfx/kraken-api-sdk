//! Advanced order showcase using only the public `kraken_sdk::` surface.
//! Section 1 runs `validate=true` (nothing placed); Section 2, gated behind
//! `KRAKEN_PLACE_REAL=1`, places ONE real far-from-market order then cancels it.

use std::process::ExitCode;
use std::str::FromStr;

use kraken_sdk::{
    AddOrderResponse, ApiKey, Client, CloseOrderType, ConditionalClose, OpenOrdersRequest,
    OrderAmendRequest, OrderRequest, OrderType, Price, PriceUnit, Side, StpType, Symbol,
    TimeInForce, TradeError, TriggerKind,
};
use rust_decimal::Decimal;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn dec(s: &str) -> Decimal {
    Decimal::from_str(s).expect("valid decimal literal")
}

/// Print a validate-mode outcome legibly for a demo.
fn show(label: &str, res: Result<AddOrderResponse, TradeError>) {
    match res {
        Ok(_) => println!("  [ACCEPTED] {label}"),
        Err(e) => println!("  [REJECTED] {label} — {e:?}"),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let (key, secret) = match (env("KRAKEN_API_KEY"), env("KRAKEN_API_SECRET")) {
        (Some(k), Some(s)) => (k, s),
        _ => {
            eprintln!("set KRAKEN_API_KEY + KRAKEN_API_SECRET to run this example");
            return ExitCode::from(2);
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

    // Orders default to WS v2; ready() brings the I/O reactor up.
    client.ready().await.expect("client ready");
    println!("client built + ready (WS v2 order transport live)\n");

    let pair = Symbol::new("BTC/USDC").expect("BTC/USDC is valid");
    let qty = dec("0.0001"); // exchange minimum
    let far_buy = dec("20000"); // far BELOW market — a buy here rests, never fills

    println!("── Section 1 — advanced order fields (validate=true; nothing is placed) ──\n");

    let r = OrderRequest::new(pair.clone(), qty, Side::Buy)
        .order_type(OrderType::Limit)
        .price(far_buy)
        .time_in_force(TimeInForce::Gtc)
        .post_only(true)
        .validate_only(true);
    show(
        "limit + time_in_force=GTC + post_only",
        client.trade().order(r).await,
    );

    let r = OrderRequest::new(pair.clone(), qty, Side::Buy)
        .order_type(OrderType::Limit)
        .price(far_buy)
        .stp_type(StpType::CancelNewest)
        .validate_only(true);
    show(
        "limit + stp_type=cancel_newest",
        client.trade().order(r).await,
    );

    // Conditional close (OTO): take-profit attached to the entry.
    let r = OrderRequest::new(pair.clone(), qty, Side::Buy)
        .order_type(OrderType::Limit)
        .price(far_buy)
        .conditional_close(ConditionalClose::new(
            CloseOrderType::Limit,
            dec("30000"), // take-profit above the entry
        ))
        .validate_only(true);
    show(
        "limit + conditional close (OTO bracket)",
        client.trade().order(r).await,
    );

    // Trailing stop: RELATIVE quote offset (not absolute); direction from buy/sell.
    let r = OrderRequest::new(pair.clone(), qty, Side::Buy)
        .order_type(OrderType::TrailingStop)
        .price(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("100"), // +100 quote-units trailing offset
        })
        .trigger(TriggerKind::Last)
        .validate_only(true);
    show(
        "trailing-stop (relative quote offset)",
        client.trade().order(r).await,
    );

    // Iceberg: only `display_vol` shows; rest stays hidden until the visible slice fills.
    let r = OrderRequest::new(pair.clone(), dec("0.0003"), Side::Buy)
        .order_type(OrderType::Iceberg)
        .price(far_buy)
        .display_vol(qty)
        .validate_only(true);
    show(
        "iceberg + display_vol (partial book visibility)",
        client.trade().order(r).await,
    );

    // Fill-or-kill: fill entirely now or cancel; limit types only.
    let r = OrderRequest::new(pair.clone(), qty, Side::Buy)
        .order_type(OrderType::Limit)
        .price(far_buy)
        .time_in_force(TimeInForce::Fok)
        .validate_only(true);
    show(
        "limit + time_in_force=FOK (fill-or-kill)",
        client.trade().order(r).await,
    );

    // Margin at the pair's max leverage (WS-only; REST can't express it).
    let r = OrderRequest::new(pair.clone(), qty, Side::Buy)
        .order_type(OrderType::Limit)
        .price(far_buy)
        .margin(true)
        .validate_only(true);
    show(
        "limit + margin=true (WS-native toggle)",
        client.trade().order(r).await,
    );

    if env("KRAKEN_PLACE_REAL").is_none() {
        println!(
            "\n── Section 2 — SKIPPED (set KRAKEN_PLACE_REAL=1 to place ONE real \
             far-from-market order, confirm it rests, then cancel it) ──"
        );
        return ExitCode::SUCCESS;
    }
    println!(
        "\n── Section 2 — REAL order lifecycle (place far-from-market → verify → amend → cancel; never fills) ──\n"
    );
    place_real_and_cancel(&client, &pair, qty).await
}

async fn place_real_and_cancel(client: &Client, pair: &Symbol, qty: Decimal) -> ExitCode {
    let far_buy = dec("20000"); // far BELOW market → buy rests, never fills
    let far_sell = dec("300000"); // far ABOVE market → sell rests, never fills

    // Try buy (needs USDC); fall back to sell (needs BTC). Far-from-market → rests, never fills.
    let buy = OrderRequest::new(pair.clone(), qty, Side::Buy)
        .order_type(OrderType::Limit)
        .price(far_buy);
    // `amend_pct`: relative offset signed to stay far out on the resting side.
    let (resp, side, amend_pct) = match client.trade().order(buy).await {
        Ok(r) => (r, "BUY @ 20000 (far below market)", dec("-50")),
        Err(e_buy) => {
            println!("  buy not placeable ({e_buy:?}); trying the sell side…");
            let sell = OrderRequest::new(pair.clone(), qty, Side::Sell)
                .order_type(OrderType::Limit)
                .price(far_sell);
            match client.trade().order(sell).await {
                Ok(r) => (r, "SELL @ 300000 (far above market)", dec("50")),
                Err(e_sell) => {
                    eprintln!(
                        "  [FAIL] neither side placeable (buy: {e_buy:?} | sell: {e_sell:?}) — \
                         fund the account with a little USDC or BTC, then retry"
                    );
                    return ExitCode::from(1);
                }
            }
        }
    };

    let txid = resp.txid.as_ref().map(|t| t.as_str().to_string());
    println!(
        "  [OK] placed REAL {side} — txid={:?} cl_ord_id={:?}",
        txid,
        resp.cl_ord_id.as_ref().map(|c| c.as_str())
    );

    if let Some(t) = txid.as_deref() {
        match client
            .account()
            .open_orders(OpenOrdersRequest::default())
            .await
        {
            Ok(oo) => println!(
                "  [OK] open_orders: the order {} resting ({} open total)",
                if oo.open.contains_key(t) {
                    "IS"
                } else {
                    "is NOT (filled/gone?)"
                },
                oo.open.len()
            ),
            Err(e) => println!("  [WARN] open_orders query failed: {e:?}"),
        }
    }

    // Amend + cancel by cl_ord_id (plain limit gets an auto id).
    let Some(cl) = resp.cl_ord_id.clone() else {
        eprintln!("  [WARN] response carried no cl_ord_id — cancel manually if it rests!");
        return ExitCode::from(1);
    };

    // Amend in place (∓50% stays far out); identity + queue priority preserved.
    let amend = OrderAmendRequest::new(cl.clone()).limit_price(Price::Offset {
        unit: PriceUnit::Percent,
        value: amend_pct,
    });
    match client.trade().order_amend(amend).await {
        Ok(a) => println!(
            "  [OK] amended limit price to {amend_pct}% off market (amend_id={})",
            a.amend_id.as_str()
        ),
        Err(e) => println!("  [WARN] amend failed: {e:?} — order rests at its original price"),
    }

    match client.trade().cancel(cl).await {
        Ok(c) => {
            println!("  [OK] cancelled — count={} pending={}", c.count, c.pending);
            if c.count == 0 {
                println!("  [WARN] cancel count=0 — order already gone; check the account.");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!(
                "  [FAIL] cancel failed: {e:?} — ⚠️ a live order may still rest; cancel it manually!"
            );
            ExitCode::from(1)
        }
    }
}
