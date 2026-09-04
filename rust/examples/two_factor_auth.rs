//! Live signed private REST against api.kraken.com with a 2FA one-time password
//! (`with_otp`) — the same otp authenticates the WS session too, since the WS v2
//! token is minted by a signed REST call. Static 2FA passwords only; read-only, no
//! orders placed. Credential-safe: no key/secret/otp echoed, no amounts printed.

use std::process::ExitCode;

use kraken_sdk::{ApiKey, Client};

mod common;
use common::load_env_var;

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
            eprintln!("error: KRAKEN_API_KEY missing from .env");
            return ExitCode::from(2);
        }
    };
    let api_secret = match load_env_var("KRAKEN_API_SECRET") {
        Some(s) if !s.is_empty() => s,
        _ => {
            eprintln!("error: KRAKEN_API_SECRET missing from .env");
            return ExitCode::from(2);
        }
    };
    // A 2FA key is rejected on every signed call without an otp — require it here.
    let otp = match load_env_var("KRAKEN_OTP") {
        Some(o) if !o.is_empty() => o,
        _ => {
            eprintln!("error: KRAKEN_OTP missing from .env");
            eprintln!("       set your 2FA one-time password (static-password mode) to run this.");
            return ExitCode::from(2);
        }
    };

    let client = match Client::builder()
        .with_api_key(ApiKey::new(api_key), api_secret)
        .with_otp(otp)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };

    println!("2FA signed REST against api.kraken.com (read-only)\n");

    // `Balance` is the cheapest signed probe that Kraken accepted the otp.
    match client.account().balance().await {
        Ok(b) => {
            println!(
                "[OK] balance    {} asset entries (amounts redacted)",
                b.assets.len()
            );
            println!();
            println!("RESULT: 2FA signed pipeline verified against live Kraken.");
            ExitCode::SUCCESS
        }
        Err(e) => {
            // A wrong/expired otp surfaces here as a permission/auth error.
            println!("[FAIL] balance    {:?}", e);
            println!();
            println!("RESULT: signed call failed — check the otp, key, and secret.");
            ExitCode::from(1)
        }
    }
}
