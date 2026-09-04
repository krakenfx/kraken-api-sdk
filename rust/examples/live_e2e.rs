//! Live end-to-end "day in the life" across the market, account and trade
//! namespaces plus order-lifecycle bus events. Stages 1-4 are read-only or
//! validate=true (no money); stage 5 places REAL filling orders, gated by
//! KRAKEN_PLACE_REAL=1.

use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use kraken_sdk::{
    ApiKey, Client, OpenOrdersRequest, OrderRequest, OrderType, Side, Symbol, TickerUpdate,
};
use kraken_sdk::{EventEnvelope, EventPayload, EventType, Transport};
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

    let (Some(api_key), Some(api_secret)) = (
        load_env_var("KRAKEN_API_KEY").filter(|s| !s.is_empty()),
        load_env_var("KRAKEN_API_SECRET").filter(|s| !s.is_empty()),
    ) else {
        eprintln!(
            "error: KRAKEN_API_KEY / KRAKEN_API_SECRET not found in ~/projects/kraken-sdk/.env"
        );
        return ExitCode::from(2);
    };

    let pair_str = env_or("KRAKEN_PAIR", "BTC/USDC");
    let volume_str = env_or("KRAKEN_VOLUME", "0.0001");
    let pair = match Symbol::new(&pair_str) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: bad pair {pair_str:?} — {e:?}");
            return ExitCode::from(2);
        }
    };
    let volume = Decimal::from_str(&volume_str).expect("KRAKEN_VOLUME must parse as Decimal");
    // Order transport for real fills. Default "ws"; "rest" forces HMAC-SHA512 REST.
    let order_via: Option<Transport> = match env_or("KRAKEN_ORDER_TRANSPORT", "ws").as_str() {
        "rest" => Some(Transport::Rest),
        _ => None,
    };

    let mut failures: Vec<&str> = Vec::new();

    println!("== live_e2e against api.kraken.com / ws.kraken.com ==");
    println!("   pair={pair_str} volume={volume_str}\n");

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
    // ready() starts the I/O reactor; dispatch lazy-starts on first events().on().
    client.ready().await.expect("client ready");
    println!("STAGE 1 — client built + ready (I/O reactor running)\n");

    // Subscribe to order-lifecycle events (RAII guard; drop = unsubscribe).
    let submitted = Arc::new(AtomicUsize::new(0));
    let ambiguous = Arc::new(AtomicUsize::new(0));
    let cancel_attempted = Arc::new(AtomicUsize::new(0));

    let _h_sub = {
        let n = submitted.clone();
        client
            .events()
            .on(
                EventType::OrderSubmittedEvent,
                move |env: &EventEnvelope| {
                    if let EventPayload::OrderSubmittedEvent {
                        cl_ord_id,
                        op,
                        status,
                        ..
                    } = &env.payload
                    {
                        n.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "   [event] OrderSubmitted   op={op:?} cl_ord_id={} status={status:?}",
                            cl_ord_id.as_str()
                        );
                    }
                },
            )
            .expect("no reactor loop has died this early")
    };
    let _h_amb = {
        let n = ambiguous.clone();
        client
            .events()
            .on(
                EventType::OrderPlacementAmbiguousEvent,
                move |env: &EventEnvelope| {
                    if let EventPayload::OrderPlacementAmbiguousEvent { cl_ord_id, op, .. } =
                        &env.payload
                    {
                        n.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "   [event] PlacementAmbiguous op={op:?} cl_ord_id={}",
                            cl_ord_id.as_str()
                        );
                    }
                },
            )
            .expect("no reactor loop has died this early")
    };
    let _h_can = {
        let n = cancel_attempted.clone();
        client
            .events()
            .on(
                EventType::OrderCancellationAttempted,
                move |env: &EventEnvelope| {
                    if let EventPayload::OrderCancellationAttempted { cl_ord_id, op, .. } =
                        &env.payload
                    {
                        n.fetch_add(1, Ordering::Relaxed);
                        println!(
                            "   [event] CancellationAttempted op={op:?} cl_ord_id={}",
                            cl_ord_id.as_str()
                        );
                    }
                },
            )
            .expect("no reactor loop has died this early")
    };

    println!("STAGE 2 — market data");
    let last_price = match client
        .market()
        .ticker(Some(std::slice::from_ref(&pair)))
        .await
    {
        Ok(tr) => match tr.get(&pair) {
            Some(t) => {
                println!(
                    "   [OK]   REST ticker {pair_str}: last={} bid={} ask={}",
                    t.last_price, t.bid_price, t.ask_price
                );
                t.last_price
            }
            None => {
                println!("   [FAIL] REST ticker: ticker missing from result");
                failures.push("ticker");
                Decimal::from_str("60000").unwrap()
            }
        },
        Err(e) => {
            println!("   [FAIL] REST ticker: {e:?}");
            failures.push("ticker");
            Decimal::from_str("60000").unwrap()
        }
    };
    {
        let seen = Arc::new(AtomicUsize::new(0));
        let n = seen.clone();
        let guard = client.market().on_ticker_for(
            std::slice::from_ref(&pair),
            None,
            None,
            move |t: &TickerUpdate| {
                if n.fetch_add(1, Ordering::Relaxed) == 0 {
                    println!(
                        "   [OK]   live WS ticker {} last={}",
                        t.symbol.as_str(),
                        t.last
                    );
                }
            },
        );
        match guard {
            Ok(g) => {
                tokio::time::sleep(Duration::from_secs(3)).await;
                drop(g); // last guard → wire unsubscribe
                if seen.load(Ordering::Relaxed) == 0 {
                    println!("   [WARN] no live WS ticker update in 3s");
                }
            }
            Err(e) => {
                println!("   [WARN] on_ticker_for: {e:?}");
            }
        }
    }
    println!();

    println!("STAGE 3 — account snapshot");
    let mut have_usdc = false;
    let mut have_btc = false;
    match client.account().balance().await {
        Ok(b) => {
            // Balance asset keys are normalized to modern form on decode (see docs/features.md).
            let usdc = b.assets.get("USDC").copied().unwrap_or_default();
            let btc = b.assets.get("BTC").copied().unwrap_or_default();
            let need_usdc = volume * last_price;
            have_usdc = usdc >= need_usdc;
            have_btc = btc >= volume;
            println!(
                "   [OK]   balance fetched ({} assets). USDC sufficient for buy: {have_usdc}; BTC sufficient for sell: {have_btc}",
                b.assets.len()
            );
        }
        Err(e) => {
            println!("   [FAIL] balance: {e:?}");
            failures.push("balance");
        }
    }
    match client.account().trade_balance(None).await {
        Ok(_tb) => println!("   [OK]   trade_balance fetched (amounts withheld)"),
        Err(e) => {
            println!("   [FAIL] trade_balance: {e:?}");
            failures.push("trade_balance");
        }
    }
    match client
        .account()
        .open_orders(OpenOrdersRequest::default())
        .await
    {
        Ok(oo) => println!("   [OK]   open_orders: {} resting", oo.open.len()),
        Err(e) => {
            println!("   [FAIL] open_orders: {e:?}");
            failures.push("open_orders");
        }
    }
    println!();

    println!("STAGE 4 — validate=true AddOrder (nothing placed)");
    {
        let far = (last_price * Decimal::from_str("0.5").unwrap()).round_dp(1);
        let req = OrderRequest::new(pair.clone(), volume, Side::Buy)
            .order_type(OrderType::Limit)
            .price(far)
            .validate_only(true);
        match client.trade().order(req).via(Transport::Rest).await {
            Ok(resp) => println!(
                "   [OK]   validate decoded: txid={:?} (None expected) cl_ord_id={:?}",
                resp.txid.as_ref().map(|t| t.as_str()),
                resp.cl_ord_id.as_ref().map(|c| c.as_str())
            ),
            Err(e) => {
                println!("   [FAIL] validate order: {e:?}");
                failures.push("validate");
            }
        }
    }
    println!();

    // OrderCancellationAttempted on mid-flight await drop: docs/guides/placing-orders.md.
    println!(
        "STAGE 4b — OrderCancellationAttempted observer registered (fires on mid-flight drop)\n"
    );

    if env_or("KRAKEN_PLACE_REAL", "").is_empty() {
        println!("STAGE 5 — SKIPPED (set KRAKEN_PLACE_REAL=1 to place real, FILLING orders)\n");
        return finish(client, &failures, &submitted, &cancel_attempted).await;
    }

    println!("STAGE 5 — REAL trading (market fill round-trip + far-from-market limit cancel)");
    if !have_usdc && !have_btc {
        println!(
            "   [SKIP] neither USDC (to buy) nor BTC (to sell) is sufficient for {volume_str}; skipping real fills"
        );
        return finish(client, &failures, &submitted, &cancel_attempted).await;
    }

    let buy_first = have_usdc;

    let bought = if buy_first {
        println!("   5a market BUY {volume_str} {pair_str} (fills at market)");
        let req = OrderRequest::new(pair.clone(), volume, Side::Buy).order_type(OrderType::Market);
        let pt = client.trade().order(req);
        let pt = match order_via {
            Some(t) => pt.via(t),
            None => pt,
        };
        match pt.await {
            Ok(r) => {
                println!(
                    "      [OK] buy txid={:?} cl_ord_id={:?}",
                    r.txid.as_ref().map(|t| t.as_str()),
                    r.cl_ord_id.as_ref().map(|c| c.as_str())
                );
                Some(r)
            }
            Err(e) => {
                println!("      [FAIL] market buy: {e:?}");
                failures.push("market_buy");
                None
            }
        }
    } else {
        println!("   5a market SELL {volume_str} {pair_str} (fills at market)");
        let req = OrderRequest::new(pair.clone(), volume, Side::Sell).order_type(OrderType::Market);
        let pt = client.trade().order(req);
        let pt = match order_via {
            Some(t) => pt.via(t),
            None => pt,
        };
        match pt.await {
            Ok(r) => {
                println!(
                    "      [OK] sell txid={:?} cl_ord_id={:?}",
                    r.txid.as_ref().map(|t| t.as_str()),
                    r.cl_ord_id.as_ref().map(|c| c.as_str())
                );
                Some(r)
            }
            Err(e) => {
                println!("      [FAIL] market sell: {e:?}");
                failures.push("market_sell");
                None
            }
        }
    };
    tokio::time::sleep(Duration::from_millis(500)).await; // let the bus deliver OrderSubmittedEvent

    if let Some(cl) = bought.as_ref().and_then(|r| r.cl_ord_id.clone()) {
        match client.account().find_order_by_cl_ord_id(&cl).await {
            Ok(outcome) => println!("   5b find_order_by_cl_ord_id → {outcome:?}"),
            Err(e) => {
                println!("   5b [WARN] reconciliation walk: {e:?}");
            }
        }
    }

    if bought.is_some() {
        if buy_first {
            println!("   5c market SELL {volume_str} to flatten");
            let req =
                OrderRequest::new(pair.clone(), volume, Side::Sell).order_type(OrderType::Market);
            let pt = client.trade().order(req);
            let pt = match order_via {
                Some(t) => pt.via(t),
                None => pt,
            };
            match pt.await {
                Ok(r) => println!(
                    "      [OK] flatten-sell txid={:?}",
                    r.txid.as_ref().map(|t| t.as_str())
                ),
                Err(e) => {
                    println!(
                        "      [FAIL] flatten sell: {e:?} — ⚠️ position NOT flat, check the account!"
                    );
                    failures.push("flatten");
                }
            }
        } else {
            println!("   5c market BUY {volume_str} to flatten");
            let req =
                OrderRequest::new(pair.clone(), volume, Side::Buy).order_type(OrderType::Market);
            let pt = client.trade().order(req);
            let pt = match order_via {
                Some(t) => pt.via(t),
                None => pt,
            };
            match pt.await {
                Ok(r) => println!(
                    "      [OK] flatten-buy txid={:?}",
                    r.txid.as_ref().map(|t| t.as_str())
                ),
                Err(e) => {
                    println!(
                        "      [FAIL] flatten buy: {e:?} — ⚠️ position NOT flat, check the account!"
                    );
                    failures.push("flatten");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    println!("   5d far-from-market limit place → cancel (no fill)");
    {
        let far = (last_price * Decimal::from_str("0.5").unwrap()).round_dp(1);
        let req = OrderRequest::new(pair.clone(), volume, Side::Buy)
            .order_type(OrderType::Limit)
            .price(far);
        let pt = client.trade().order(req);
        let pt = match order_via {
            Some(t) => pt.via(t),
            None => pt,
        };
        match pt.await {
            Ok(r) => {
                println!(
                    "      [OK] resting limit placed txid={:?} cl_ord_id={:?}",
                    r.txid.as_ref().map(|t| t.as_str()),
                    r.cl_ord_id.as_ref().map(|c| c.as_str())
                );
                if let Some(cl) = r.cl_ord_id.clone() {
                    let pt = client.trade().cancel(cl);
                    let pt = match order_via {
                        Some(t) => pt.via(t),
                        None => pt,
                    };
                    match pt.await {
                        Ok(c) => {
                            println!("      [OK] cancel count={} pending={}", c.count, c.pending);
                            if c.count == 0 {
                                println!(
                                    "      [WARN] cancel count=0 — order already gone; check the account"
                                );
                            }
                        }
                        Err(e) => {
                            println!(
                                "      [FAIL] cancel: {e:?} — ⚠️ a live limit order may still rest; cancel it manually!"
                            );
                            failures.push("cancel");
                        }
                    }
                } else {
                    println!(
                        "      [WARN] no cl_ord_id (suppressed?) — cancel manually if it rests!"
                    );
                }
            }
            Err(e) => {
                println!("      [FAIL] resting limit place: {e:?}");
                failures.push("limit_place");
            }
        }
    }
    println!();

    finish(client, &failures, &submitted, &cancel_attempted).await
}

async fn finish(
    client: Client,
    failures: &[&str],
    submitted: &Arc<AtomicUsize>,
    cancel: &Arc<AtomicUsize>,
) -> ExitCode {
    // close() drains both WS connections; completion resolves on ClientClosedEvent.
    let _ = client.close().await;
    println!("STAGE 6 — client closed (graceful drain complete)\n");
    println!(
        "events observed on the bus: OrderSubmitted={} OrderCancellationAttempted={}",
        submitted.load(Ordering::Relaxed),
        cancel.load(Ordering::Relaxed)
    );
    if failures.is_empty() {
        println!("RESULT: live_e2e completed with no hard failures.");
        ExitCode::SUCCESS
    } else {
        println!("RESULT: {} failure(s): {:?}", failures.len(), failures);
        ExitCode::from(1)
    }
}
