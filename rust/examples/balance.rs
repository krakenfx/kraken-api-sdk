//! Live verify `account.balance()` against api.kraken.com using credentials
//! from the repo `.env`. Credential-safe: never prints the key, secret, or balance
//! amounts — only asset codes and a decode-ok marker. Run: `cargo run --example balance`.

use std::process::ExitCode;

use kraken_sdk::{ApiKey, Client};

mod common;
use common::load_env_var;

#[tokio::main]
async fn main() -> ExitCode {
    let api_key_str = match load_env_var("KRAKEN_API_KEY") {
        Some(v) => v,
        None => {
            eprintln!("error: KRAKEN_API_KEY missing from .env");
            return ExitCode::from(2);
        }
    };
    let api_secret_str = match load_env_var("KRAKEN_API_SECRET") {
        Some(v) => v,
        None => {
            eprintln!("error: KRAKEN_API_SECRET missing from .env");
            return ExitCode::from(2);
        }
    };

    // ApiSecret zeroes on drop; ApiKey Debug redacts.
    let client = match Client::builder()
        .with_api_key(ApiKey::new(api_key_str), api_secret_str)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };

    println!("live signed POST: /0/private/Balance");
    println!();

    match client.account().balance().await {
        Ok(balance) => {
            // Never print actual amounts — codes + decode marker only.
            println!("  signed POST succeeded.");
            println!("  assets returned: {}", balance.assets.len());
            println!();
            let mut codes: Vec<&str> = balance.assets.keys().map(|k| k.as_str()).collect();
            codes.sort();
            for code in codes {
                println!("    {:>8}   decoded ok", code);
            }
            println!();
            println!("RESULT: end-to-end signed pipeline verified against live Kraken.");
            ExitCode::SUCCESS
        }
        Err(e) => {
            println!("  signed POST FAILED: {}", e);
            println!();
            println!("RESULT: end-to-end signed pipeline failed.");
            ExitCode::from(1)
        }
    }
}
