//! Live Futures order lifecycle (places real orders, including a fill); `#[ignore]`d by default.
//! Run against a test environment: `KRAKEN_REST_BASE_URL=.. KRAKEN_UNIFIED_WS_PRIVATE_URL=.. KRAKEN_API_KEY=..
//! KRAKEN_API_SECRET=.. KRAKEN_E2E_TRADE_REAL=1 cargo test --test futures_orders_live -- --ignored`

use std::str::FromStr;

use futures_util::FutureExt;
use kraken_sdk::{
    ApiError, ApiKey, ClOrdId, Client, FuturesOrder, FuturesOrderAmendRequest,
    FuturesOrderBatchRequest, FuturesOrderRequest, OrderType, Side, TradeError, TriggerKind,
};
use rust_decimal::Decimal;

const SYMBOL: &str = "PF_ETHUSD";

fn dec(value: &str) -> Decimal {
    Decimal::from_str(value).expect("decimal literal")
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

const REQUIRED: [&str; 5] = [
    "KRAKEN_API_KEY",
    "KRAKEN_API_SECRET",
    "KRAKEN_REST_BASE_URL",
    "KRAKEN_UNIFIED_WS_PRIVATE_URL",
    "KRAKEN_E2E_TRADE_REAL",
];

fn gate(get: impl Fn(&str) -> Option<String>) -> Option<(String, String)> {
    let [Some(key), Some(secret), Some(_), Some(_), Some(_)] = REQUIRED.map(get) else {
        return None;
    };
    Some((key, secret))
}

#[derive(Default)]
struct Cleanup {
    open_ids: Vec<ClOrdId>,
    armed: bool,
    long: bool,
}

fn resting_order() -> FuturesOrder {
    FuturesOrder::new(Side::Sell, dec("0.01"))
        .order_type(OrderType::TakeProfit)
        .trigger(TriggerKind::Last)
        .price(dec("20000"))
        .cl_ord_id(ClOrdId::allocate_v4())
}

async fn lifecycle(client: &Client, cleanup: &mut Cleanup) {
    let futures = client.trade().futures();

    let dry = futures
        .order(FuturesOrderRequest::new(SYMBOL, resting_order()).validate_only(true))
        .await
        .expect("dry run");
    assert!(dry.txid.is_none(), "a dry run places nothing");

    let order = resting_order();
    let id = order.cl_ord_id.clone().expect("explicit id");
    cleanup.open_ids.push(id.clone());
    let placed = futures
        .order(FuturesOrderRequest::new(SYMBOL, order))
        .await
        .expect("place");
    assert!(placed.txid.is_some());
    futures
        .order_amend(FuturesOrderAmendRequest::by_cl_ord_id(id.clone()).trigger_price(dec("20500")))
        .await
        .expect("amend the trigger");
    let cancelled = futures.cancel(id.clone()).await.expect("cancel");
    assert_eq!(cancelled.count, 1);
    let gone = futures.cancel(id).await.expect_err("already cancelled");
    assert!(matches!(gone, TradeError::UnknownOrder { .. }), "{gone:?}");

    let rows = vec![resting_order(), resting_order()];
    let ids: Vec<ClOrdId> = rows.iter().filter_map(|r| r.cl_ord_id.clone()).collect();
    cleanup.open_ids.extend(ids.clone());
    let batch = futures
        .order_batch(FuturesOrderBatchRequest::new(SYMBOL, rows))
        .await
        .expect("batch");
    assert!(batch.orders.iter().all(|row| row.txid.is_some()));
    let counted = futures.cancel_batch(ids).await.expect("batch cancel");
    assert_eq!(counted.count, 2);

    let duplicate = resting_order();
    let duplicate_id = duplicate.cl_ord_id.clone().expect("explicit id");
    cleanup.open_ids.push(duplicate_id.clone());
    let mixed = futures
        .order_batch(FuturesOrderBatchRequest::new(
            SYMBOL,
            vec![duplicate.clone(), duplicate],
        ))
        .await
        .expect("a row failure is not a batch failure");
    assert_eq!(
        mixed
            .orders
            .iter()
            .filter(|row| row.error.is_some())
            .count(),
        1,
        "the duplicate id fails one row and places the other"
    );
    futures.cancel_all().await.expect("cancel all");
    let _ = futures.cancel(duplicate_id).await;

    cleanup.armed = true;
    let armed = futures
        .cancel_all_orders_after(60)
        .await
        .expect("arm the dead-man's switch");
    assert!(armed.trigger_time.is_some());
    let disarmed = futures.cancel_all_orders_after(0).await.expect("disarm");
    assert!(disarmed.trigger_time.is_none());
    cleanup.armed = false;

    cleanup.long = true;
    let buy = futures
        .order(FuturesOrderRequest::new(
            SYMBOL,
            FuturesOrder::new(Side::Buy, dec("0.01")),
        ))
        .await
        .expect("market buy fills");
    assert!(buy.txid.is_some());
    let sell = futures
        .order(FuturesOrderRequest::new(
            SYMBOL,
            FuturesOrder::new(Side::Sell, dec("0.01")).reduce_only(true),
        ))
        .await
        .expect("reduce-only market sell flattens");
    assert!(sell.txid.is_some());
    cleanup.long = false;

    let refused = futures
        .order(FuturesOrderRequest::new(
            SYMBOL,
            FuturesOrder::new(Side::Sell, dec("0.01")).reduce_only(true),
        ))
        .await;
    let error = refused.expect_err("a reduce-only order with no position is refused");
    assert_eq!(error.code(), "INVALID_ORDER", "{error:?}");
}

#[tokio::test]
#[ignore = "live: places real orders on Futures; run with --ignored and test-environment credentials"]
async fn futures_order_lifecycle_and_a_filled_round_trip() {
    let Some((key, secret)) = gate(env) else {
        eprintln!("[skip] needs {REQUIRED:?}");
        return;
    };
    let client = Client::builder()
        .with_api_key(ApiKey::new(key), secret)
        .build()
        .expect("build");
    client.ready().await.expect("client ready");

    let mut cleanup = Cleanup::default();
    let outcome = std::panic::AssertUnwindSafe(lifecycle(&client, &mut cleanup))
        .catch_unwind()
        .await;

    let futures = client.trade().futures();
    for id in cleanup.open_ids {
        let _ = futures.cancel(id).await;
    }
    if cleanup.long {
        let _ = futures
            .order(FuturesOrderRequest::new(
                SYMBOL,
                FuturesOrder::new(Side::Sell, dec("0.01")).reduce_only(true),
            ))
            .await;
    }
    if cleanup.armed {
        let _ = futures.cancel_all_orders_after(0).await;
    }
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[test]
fn the_live_gate_needs_credentials_endpoints_and_the_real_order_opt_in() {
    fn all_but(missing: &'static str) -> impl Fn(&str) -> Option<String> {
        move |name| (name != missing).then(|| "x".to_string())
    }
    assert!(gate(|_| Some("x".to_string())).is_some());
    for missing in [
        "KRAKEN_API_KEY",
        "KRAKEN_API_SECRET",
        "KRAKEN_REST_BASE_URL",
        "KRAKEN_UNIFIED_WS_PRIVATE_URL",
        "KRAKEN_E2E_TRADE_REAL",
    ] {
        assert!(gate(all_but(missing)).is_none(), "{missing}");
    }
}
