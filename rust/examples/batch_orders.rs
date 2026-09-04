//! End-to-end demo of batch ops: `order_batch(...)` + `cancel_batch(...)`.
//! Default mode builds requests offline; `validate=true` signs the wire but
//! places nothing; `KRAKEN_PLACE_REAL=1` places 2 far-from-market limits then cancels.

use std::process::ExitCode;
use std::str::FromStr;

use kraken_sdk::{
    AddOrderBatchRequest, ApiError, ApiKey, BatchOrderEntry, BatchResult, ClOrdId, Client,
    OrderType, Side, Symbol, Transport,
};
use rust_decimal::Decimal;

mod common;
use common::load_env_var;

fn entry(side: Side, vol: &str, price: &str, cl: ClOrdId) -> BatchOrderEntry {
    BatchOrderEntry::new(OrderType::Limit, side, Decimal::from_str(vol).unwrap())
        .price(Decimal::from_str(price).unwrap())
        .cl_ord_id(cl)
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

    let pair = Symbol::new("BTC/USDC").expect("valid pair");
    // Pre-allocate cl_ord_ids for cancel_batch later.
    let cl_buy = ClOrdId::allocate_v4();
    let cl_sell = ClOrdId::allocate_v4();

    // Two far-below-market buys (USDC-funded); neither can fill.
    let make_req = |validate: bool| {
        AddOrderBatchRequest::new(
            pair.clone(),
            vec![
                entry(Side::Buy, "0.0001", "20000", cl_buy.clone()),
                entry(Side::Buy, "0.0001", "21000", cl_sell.clone()),
            ],
        )
        .validate_only(validate)
    };

    println!("== batch surface ==");
    println!("  order_batch: 2 far-below-market buy limits on BTC/USDC (won't fill)");
    println!(
        "    entry[0] buy 0.0001 @ 20000  cl_ord_id={}",
        cl_buy.as_str()
    );
    println!(
        "    entry[1] buy 0.0001 @ 21000  cl_ord_id={}",
        cl_sell.as_str()
    );
    println!("  cancel_batch([cl0, cl1]) cancels both by cl_ord_id\n");

    println!("== validate=true order_batch (signed wire, nothing placed) ==");
    match client
        .trade()
        .order_batch(make_req(true))
        .via(Transport::Rest)
        .await
    {
        Ok(resp) => {
            for (i, o) in resp.orders.iter().enumerate() {
                println!(
                    "  [OK] entry[{i}] descr={:?} txid={:?} (None expected)",
                    o.descr.order, o.txid
                );
            }
        }
        Err(e) => {
            println!("  [FAIL] order_batch validate errored: {e:?}");
            return ExitCode::from(1);
        }
    }
    println!();

    if std::env::var("KRAKEN_PLACE_REAL")
        .unwrap_or_default()
        .is_empty()
    {
        println!(
            "Mode 3 SKIPPED — set KRAKEN_PLACE_REAL=1 for the real batch place -> cancel_batch."
        );
        return ExitCode::SUCCESS;
    }

    println!("== live: order_batch (2 far-from-market limits) ==");
    match client
        .trade()
        .order_batch(make_req(false))
        .via(Transport::Rest)
        .await
    {
        Ok(resp) => {
            // Batch can partially place — inspect each entry, not just Ok.
            for (i, o) in resp.orders.iter().enumerate() {
                match (&o.txid, &o.error) {
                    (Some(txid), _) => {
                        println!(
                            "  [placed]   entry[{i}] txid={txid} descr={:?}",
                            o.descr.order
                        )
                    }
                    (None, Some(err)) => {
                        println!("  [rejected] entry[{i}] {err} (placed siblings stay live)")
                    }
                    (None, None) => println!("  [dry-run]  entry[{i}] (validate mode)"),
                }
            }
        }
        Err(e) => {
            println!("  [FAIL] order_batch errored: {e:?}");
            return ExitCode::from(1);
        }
    }

    println!("== cancel_batch by the two cl_ord_ids ==");
    match client
        .trade()
        .cancel_batch(vec![cl_buy.clone(), cl_sell.clone()])
        .via(Transport::Rest)
        .await
    {
        Ok(resp) => {
            for (i, r) in resp.results.iter().enumerate() {
                match r {
                    BatchResult::Ok(c) => println!("  [OK] line[{i}] cancelled: count={}", c.count),
                    BatchResult::Err(e) => println!(
                        "  [ERR] line[{i}] cl_ord_id={} {}",
                        e.cl_ord_id.as_str(),
                        e.message()
                    ),
                    _ => {}
                }
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            println!("  [FAIL] cancel_batch errored: {e:?} — CHECK FOR RESTING ORDERS MANUALLY");
            ExitCode::from(1)
        }
    }
}
