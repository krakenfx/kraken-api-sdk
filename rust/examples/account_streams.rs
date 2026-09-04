//! Live auth-WS account-stream smoke: `executions` + `balances`.
//! Two-step form (register, then subscribe); combiners preferred —
//! docs/guides/streaming.md. Places NO orders; never prints balance amounts.
//! Run: `cargo run --example account_streams`.

use std::time::Duration;

use kraken_sdk::{ApiKey, BalanceUpdate, Client, ExecutionUpdate};

mod common;
use common::load_env_var;

#[tokio::main]
async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("kraken_sdk=info")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let api_key = load_env_var("KRAKEN_API_KEY").expect("KRAKEN_API_KEY not found in .env");
    let api_secret =
        load_env_var("KRAKEN_API_SECRET").expect("KRAKEN_API_SECRET not found in .env");

    let client = Client::builder()
        .with_api_key(ApiKey::new(api_key), api_secret)
        .build()
        .expect("build");

    client.ready();

    // Prints markers only — never qty, price, cost, or fee amounts.
    let _exec = client.account().on_executions(|u: &ExecutionUpdate| {
        let sym = u.symbol.as_ref().map(|s| s.as_str()).unwrap_or("<none>");
        let status = format!("{:?}", u.order_status);
        let fee_assets = u
            .fees
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|f| f.asset.as_str())
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "[executions] exec_type={:?} order_status={} symbol={} cost_set={} fee_assets=[{}]",
            u.exec_type,
            status,
            sym,
            u.cost.is_some(),
            fee_assets
        );
    });

    // Prints asset name only — never balance amounts.
    let _bal = client.account().on_balances(|u: &BalanceUpdate| {
        let asset = u.asset.as_ref().map(|a| a.as_str()).unwrap_or("<none>");
        println!("[balances] asset={}", asset);
    });

    if let Err(e) = client.subscription().subscribe_executions() {
        eprintln!("[main] subscribe_executions error: {e:?}");
    }
    if let Err(e) = client.subscription().subscribe_balances() {
        eprintln!("[main] subscribe_balances error: {e:?}");
    }

    println!("[main] streaming executions + balances for 10s...");
    tokio::time::sleep(Duration::from_secs(10)).await;
    println!("[main] done");
}
