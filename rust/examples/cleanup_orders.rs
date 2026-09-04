//! One-shot cleanup: cancel ALL open orders on the account, then report the
//! count. Used to recover after a batch placed orders but the SDK errored on
//! decode (leaving orders resting). Creds from ~/.env; secret never echoed.

use std::process::ExitCode;

use kraken_sdk::{ApiKey, Client, Transport};

mod common;
use common::load_env_var;

#[tokio::main]
async fn main() -> ExitCode {
    let (Some(key), Some(secret)) = (
        load_env_var("KRAKEN_API_KEY").filter(|s| !s.is_empty()),
        load_env_var("KRAKEN_API_SECRET").filter(|s| !s.is_empty()),
    ) else {
        eprintln!("error: creds missing");
        return ExitCode::from(2);
    };
    let client = Client::builder()
        .with_api_key(ApiKey::new(key), secret)
        .build()
        .expect("valid credentials + default config");

    // SAFETY GATE: cancel-all is destructive — require explicit opt-in.
    if std::env::var("KRAKEN_CLEANUP_CONFIRMED")
        .ok()
        .filter(|s| !s.is_empty())
        .is_none()
    {
        eprintln!(
            "REFUSING to cancel orders without confirmation.\n\
             This cancels EVERY open order on the account.\n\
             Re-run with KRAKEN_CLEANUP_CONFIRMED=1 to proceed."
        );
        return ExitCode::from(2);
    }

    match client.trade().cancel_all().via(Transport::Rest).await {
        Ok(resp) => {
            println!("cancel_all OK: cancelled count={}", resp.count);
            ExitCode::SUCCESS
        }
        Err(e) => {
            println!("cancel_all FAILED: {e:?} — CHECK ACCOUNT MANUALLY");
            ExitCode::from(1)
        }
    }
}
