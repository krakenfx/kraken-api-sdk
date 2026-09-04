//! Config / knob flow demo using only the public API, no creds or network
//! (`.build()` is config-validation only). Shows TOML + env + builder knob
//! layering, resolved-source reporting, and runtime `set_knob`. Run: `cargo run --example config_knobs`.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kraken_sdk::Client;
use kraken_sdk::{ConfigSource, EventEnvelope, EventPayload, EventType, KnobValue};

/// Tiny RAII temp file (no extra dependency). Written to the OS temp dir;
/// removed on drop.
struct TmpToml {
    path: std::path::PathBuf,
}
impl Drop for TmpToml {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
fn write_temp_toml(contents: &str) -> TmpToml {
    let mut p = std::env::temp_dir();
    let unique = format!(
        "kraken_config_knobs_example_{}_{}.toml",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    p.push(unique);
    let mut f = std::fs::File::create(&p).expect("create temp toml");
    f.write_all(contents.as_bytes()).expect("write temp toml");
    f.flush().expect("flush temp toml");
    TmpToml { path: p }
}

#[tokio::main]
async fn main() {
    println!("=== Kraken SDK config / knob demo (no creds, no network) ===\n");

    // TOML knobs map to env as KRAKEN_ + SCREAMING_SNAKE; ${VAR} resolved at file-read.
    let toml = "\
# config_knobs.rs demo config file
connection_rate_budget = ${KRAKEN_DEMO_BUDGET}
rate_limit_api_warning_pct = 0.60
";
    let cfg = write_temp_toml(toml);
    println!("[1] wrote temp TOML config -> {}", cfg.path.display());
    println!("      connection_rate_budget = ${{KRAKEN_DEMO_BUDGET}}   (interpolated)");
    println!("      rate_limit_api_warning_pct = 0.60\n");

    // SAFETY: single-threaded demo; vars restored before exit.
    unsafe {
        std::env::set_var("KRAKEN_DEMO_BUDGET", "150");
        std::env::set_var("KRAKEN_RATE_LIMIT_API_WARNING_PCT", "0.75");
    }
    println!("[2] set env vars:");
    println!("      KRAKEN_DEMO_BUDGET=150                       (feeds the TOML interpolation)");
    println!("      KRAKEN_RATE_LIMIT_API_WARNING_PCT=0.75       (Env beats File 0.60)\n");

    let client = Client::builder()
        .with_knob("rate_limit_api_warning_pct", KnobValue::F64(0.90))
        .with_knob("reconnect_attempts", KnobValue::OptU32(Some(3)))
        .with_knob(
            "rest_base_url",
            KnobValue::Str("https://beta-api.kraken.com".to_string()),
        )
        .with_config_file(cfg.path.clone())
        .build()
        .expect("build is config-validation only (no network)");
    println!("[3] Client::builder()");
    println!("      .with_knob(rate_limit_api_warning_pct, 0.90)   (Builder beats Env 0.75)");
    println!("      .with_knob(reconnect_attempts, Some(3))        (resilience knob)");
    println!("      .with_knob(rest_base_url, \"https://beta-api.kraken.com\")  (string knob)");
    println!("      .with_config_file(<temp toml>)");
    println!("      .build()\n");

    // .build() buffers ConfigResolved; subscribe lazy-starts dispatch. Keep the guard alive.
    let resolved_cell: Arc<Mutex<Option<EventEnvelope>>> = Arc::new(Mutex::new(None));
    {
        let (tx, rx) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell: Arc<Mutex<Option<_>>> = Arc::new(Mutex::new(Some(tx)));
        let tx_for_cb = Arc::clone(&tx_cell);
        let _guard = client
            .events()
            .on(EventType::ConfigResolved, move |env: &EventEnvelope| {
                if let Some(tx) = tx_for_cb.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            })
            .expect("events().on only fails after a reactor loop death");
        tokio::time::sleep(Duration::from_millis(300)).await;

        match tokio::time::timeout(Duration::from_secs(2), rx).await {
            Ok(Ok(env)) => *resolved_cell.lock().unwrap() = Some(env),
            _ => println!("    (note: ConfigResolved not observed within 2s)"),
        }
    }

    let source_of = |name: &str| -> Option<ConfigSource> {
        let guard = resolved_cell.lock().unwrap();
        let env = guard.as_ref()?;
        match &env.payload {
            EventPayload::ConfigResolved { source_map } => source_map.get(name).copied(),
            _ => None,
        }
    };
    let fmt_source = |name: &str| match source_of(name) {
        Some(s) => s.to_string(),
        None => "?".to_string(),
    };

    println!("[4] Resolved knob values (Builder > Env > File > Default):");
    for name in [
        "rate_limit_api_warning_pct", // Builder wins
        "connection_rate_budget",     // File wins (interpolated)
        "reconnect_attempts",         // Builder wins
        "rest_base_url",              // Builder wins (string knob)
        "staleness_window_ms",        // Default
        "request_timeout_ms",         // Default
    ] {
        let value = format!("{:?}", client.knob(name));
        println!(
            "      {:<30} = {:<22}  [won by: {}]",
            name,
            value,
            fmt_source(name)
        );
    }
    println!();

    if let Some(env) = resolved_cell.lock().unwrap().as_ref() {
        if let EventPayload::ConfigResolved { source_map } = &env.payload {
            let non_default: Vec<_> = source_map
                .iter()
                .filter(|(_, s)| **s != ConfigSource::Default)
                .map(|(k, s)| format!("{k}={s}"))
                .collect();
            println!(
                "[5a] observed ConfigResolved at build — non-default sources: {:?}",
                non_default
            );
        }
    }
    println!();

    {
        let (tx, rx) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell: Arc<Mutex<Option<_>>> = Arc::new(Mutex::new(Some(tx)));
        let tx_for_cb = Arc::clone(&tx_cell);
        let _guard = client
            .events()
            .on(EventType::ConfigChangedEvent, move |env: &EventEnvelope| {
                if let Some(tx) = tx_for_cb.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            })
            .expect("events().on only fails after a reactor loop death");

        println!("[5b] set_knob(rate_limit_api_warning_pct, 0.90 -> 0.95) at runtime...");
        client
            .set_knob("rate_limit_api_warning_pct", KnobValue::F64(0.95))
            .expect("runtime-mutable knob");

        match tokio::time::timeout(Duration::from_secs(2), rx).await {
            Ok(Ok(env)) => match env.payload {
                EventPayload::ConfigChangedEvent {
                    knob,
                    previous,
                    current,
                } => {
                    println!(
                        "      observed ConfigChangedEvent {{ knob: {}, previous: {:?}, current: {:?} }}",
                        knob.as_str(),
                        previous,
                        current
                    );
                }
                other => println!("      unexpected payload: {other:?}"),
            },
            _ => {
                println!(
                    "      (event not observed; knob() now = {:?})",
                    client.knob("rate_limit_api_warning_pct")
                );
            }
        }

        client
            .set_knob("reconnect_attempts", KnobValue::OptU32(Some(7)))
            .expect("reconnect_attempts is runtime-mutable");
        println!(
            "      set_knob(reconnect_attempts, Some(7)) -> knob() now = {:?}",
            client.knob("reconnect_attempts")
        );
    }
    println!();

    // Construction-only knob is immutable post-build.
    match client.set_knob("request_timeout_ms", KnobValue::U32(1)) {
        Err(e) => println!("[6] set_knob(request_timeout_ms, ..) rejected as expected: {e}"),
        Ok(()) => println!("[6] UNEXPECTED: construction-only knob accepted at runtime!"),
    }

    // SAFETY: undoes the sets above before process exit.
    unsafe {
        std::env::remove_var("KRAKEN_DEMO_BUDGET");
        std::env::remove_var("KRAKEN_RATE_LIMIT_API_WARNING_PCT");
    }
    println!("\n=== done (clean exit) ===");
}
