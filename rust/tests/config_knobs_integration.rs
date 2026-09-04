//! External-consumer integration test exercising config-knob source precedence,
//! runtime mutation, and the build-time events through only the crate's `pub`
//! surface. All env mutation is confined to one test fn to avoid cross-test races.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kraken_sdk::{
    Client, ConfigError, ConfigSource, EventEnvelope, EventPayload, EventType, KnobValue,
};

/// Tiny RAII temp file (no extra dev-dependency). Writes `contents` to a
/// uniquely-named file in the OS temp dir and removes it on drop.
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
        "kraken_cfg_it_{}_{}.toml",
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

/// `multi_thread` flavor: the dispatch reactor is spawned via `tokio::spawn`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_knob_precedence_and_runtime_mutation_via_public_api() {
    let toml = "\
# integration-test config
connection_rate_budget = ${KRAKEN_IT_BUDGET}
rate_limit_api_warning_pct = 0.60
request_timeout_ms = 11000
";
    let cfg = write_temp_toml(toml);

    // Env var name for a knob == KRAKEN_ + SCREAMING_SNAKE(knob name).
    // SAFETY: process-wide env; this integration test owns these keys for its body.
    unsafe {
        std::env::set_var("KRAKEN_IT_BUDGET", "150");
        std::env::set_var("KRAKEN_RATE_LIMIT_API_WARNING_PCT", "0.75");
    }

    let client = Client::builder()
        .with_knob("rate_limit_api_warning_pct", KnobValue::F64(0.90))
        .with_knob("reconnect_attempts", KnobValue::OptU32(Some(3)))
        .with_config_file(cfg.path.clone())
        .build()
        .expect("build is config-validation only — no network");

    assert_eq!(
        client.knob("rate_limit_api_warning_pct"),
        Some(KnobValue::F64(0.90)),
        "Builder must beat Env+File"
    );

    assert_eq!(
        client.knob("connection_rate_budget"),
        Some(KnobValue::U32(150)),
        "File (interpolated) value must win when neither Env nor Builder set it"
    );

    assert_eq!(
        client.knob("request_timeout_ms"),
        Some(KnobValue::U32(11_000)),
        "File value must win over Default"
    );

    assert_eq!(
        client.knob("reconnect_attempts"),
        Some(KnobValue::OptU32(Some(3))),
        "Builder value must win over Default"
    );

    assert_eq!(
        client.knob("staleness_window_ms"),
        Some(KnobValue::U32(30_000)),
        "Untouched knob must read the SDK default"
    );

    assert_eq!(client.knob("does_not_exist"), None);

    // Subscribe lazy-starts the dispatch reactor after the record is in place.
    let bus = client.bus();
    {
        let (tx, rx) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell: Arc<Mutex<Option<_>>> = Arc::new(Mutex::new(Some(tx)));
        let tx_for_cb = Arc::clone(&tx_cell);
        let _ = bus.subscribe(
            EventType::ConfigResolved,
            Arc::new(move |env: &EventEnvelope| {
                if let Some(tx) = tx_for_cb.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            1,
        );
        let env = tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("ConfigResolved not received within 2s")
            .expect("ConfigResolved sender dropped");
        assert_eq!(env.event_type, EventType::ConfigResolved);
        match env.payload {
            EventPayload::ConfigResolved { ref source_map } => {
                assert_eq!(
                    source_map.get("rate_limit_api_warning_pct"),
                    Some(&ConfigSource::Builder)
                );
                assert_eq!(
                    source_map.get("reconnect_attempts"),
                    Some(&ConfigSource::Builder)
                );
                assert_eq!(
                    source_map.get("connection_rate_budget"),
                    Some(&ConfigSource::File)
                );
                assert_eq!(
                    source_map.get("request_timeout_ms"),
                    Some(&ConfigSource::File)
                );
                assert_eq!(
                    source_map.get("staleness_window_ms"),
                    Some(&ConfigSource::Default)
                );
            }
            ref other => panic!("unexpected ConfigResolved payload: {other:?}"),
        }
    }

    {
        let (tx, rx) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell: Arc<Mutex<Option<_>>> = Arc::new(Mutex::new(Some(tx)));
        let tx_for_cb = Arc::clone(&tx_cell);
        let _ = bus.subscribe(
            EventType::ConfigChangedEvent,
            Arc::new(move |env: &EventEnvelope| {
                if let Some(tx) = tx_for_cb.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            1,
        );

        client
            .set_knob("rate_limit_api_warning_pct", KnobValue::F64(0.5))
            .expect("runtime-mutable knob set must succeed");

        assert_eq!(
            client.knob("rate_limit_api_warning_pct"),
            Some(KnobValue::F64(0.5))
        );

        let env = tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("ConfigChangedEvent not received within 2s")
            .expect("ConfigChangedEvent sender dropped");
        assert_eq!(env.event_type, EventType::ConfigChangedEvent);
        match env.payload {
            EventPayload::ConfigChangedEvent {
                ref knob,
                ref previous,
                ref current,
            } => {
                assert_eq!(knob.as_str(), "rate_limit_api_warning_pct");
                assert_eq!(*previous, KnobValue::F64(0.90));
                assert_eq!(*current, KnobValue::F64(0.5));
            }
            ref other => panic!("unexpected ConfigChangedEvent payload: {other:?}"),
        }
    }

    let err = client
        .set_knob("request_timeout_ms", KnobValue::U32(1))
        .expect_err("construction-only knob must be rejected at runtime");
    assert!(
        matches!(err, ConfigError::ImmutableKnob { ref knob } if knob == "request_timeout_ms"),
        "expected ImmutableKnob, got {err:?}"
    );

    client
        .set_knob("reconnect_attempts", KnobValue::OptU32(Some(7)))
        .expect("reconnect_attempts is runtime-mutable");
    assert_eq!(
        client.knob("reconnect_attempts"),
        Some(KnobValue::OptU32(Some(7)))
    );

    let _ = client.close();
    // SAFETY: undoes the sets above; test process exits after this.
    unsafe {
        std::env::remove_var("KRAKEN_IT_BUDGET");
        std::env::remove_var("KRAKEN_RATE_LIMIT_API_WARNING_PCT");
    }
}
