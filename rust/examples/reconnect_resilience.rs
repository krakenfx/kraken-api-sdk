//! Reconnect-resilience config + observation demo (public WS, no creds, ~5s):
//! configure the reconnect/backoff/staleness knobs and a custom JitterSource,
//! then subscribe to connection-lifecycle events and print them as they occur.

use std::sync::Arc;
use std::time::Duration;

use kraken_sdk::{
    Client, EventEnvelope, EventPayload, EventType, JitterSource, KnobValue, Symbol, TickerUpdate,
};

/// A user-supplied deterministic jitter source: cycles through a fixed table of
/// `[0,1)` values, making backoff timing reproducible. The SDK blends the draw
/// into the capped exponential backoff per `backoff_jitter` (0.5 here → 0.5 + 0.5×draw).
struct CyclicJitter {
    table: Vec<f64>,
    idx: std::sync::atomic::AtomicUsize,
}
impl CyclicJitter {
    fn new(table: Vec<f64>) -> Self {
        Self {
            table,
            idx: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}
impl JitterSource for CyclicJitter {
    fn next_unit(&self) -> f64 {
        let i = self.idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.table[i % self.table.len()]
    }
}

/// Print a one-line summary of a connection-lifecycle event.
fn print_conn_event(env: &EventEnvelope) {
    match &env.payload {
        EventPayload::ConnectionConnectingEvent { url, attempt_count } => {
            println!("[conn] Connecting       url={url:?} attempt={attempt_count}");
        }
        EventPayload::ConnectionOpenEvent { url, .. } => {
            println!("[conn] Open            url={url:?}");
        }
        EventPayload::ConnectionDroppedEvent {
            url, close_code, ..
        } => {
            println!("[conn] Dropped         url={url:?} close_code={close_code:?}");
        }
        EventPayload::ConnectionAttemptFailedEvent {
            url,
            attempt,
            backoff_ms,
            transient_class,
            ..
        } => {
            println!(
                "[conn] AttemptFailed    url={url:?} attempt={attempt} backoff_ms={backoff_ms} class={transient_class:?}"
            );
        }
        EventPayload::ConnectionFailedEvent {
            url,
            attempt_count,
            non_transient_class,
            ..
        } => {
            println!(
                "[conn] Failed (terminal) url={url:?} attempts={attempt_count} class={non_transient_class:?}"
            );
        }
        EventPayload::ConnectionReopenedEvent {
            url, attempt_count, ..
        } => {
            println!("[conn] Reopened        url={url:?} attempt={attempt_count}");
        }
        EventPayload::WsUpgradeOk { url, .. } => {
            println!("[conn] WsUpgradeOk      url={url:?}");
        }
        EventPayload::WsUpgradeFailed { url, .. } => {
            println!("[conn] WsUpgradeFailed  url={url:?}");
        }
        other => {
            println!("[conn] {:?}", env.event_type);
            let _ = other;
        }
    }
}

#[tokio::main]
async fn main() {
    println!("=== reconnect-resilience config + observe demo ===\n");

    let custom_jitter = Arc::new(CyclicJitter::new(vec![0.1, 0.4, 0.7, 0.9]));
    let client = Client::builder()
        .with_jitter_source(custom_jitter)
        .with_knob("reconnect_attempts", KnobValue::OptU32(Some(3)))
        .with_knob("backoff_base_ms", KnobValue::U32(250))
        .with_knob("backoff_max_ms", KnobValue::U32(2_000))
        .with_knob("backoff_factor", KnobValue::F64(2.0))
        .with_knob("backoff_jitter", KnobValue::F64(0.5))
        .with_knob("staleness_window_ms", KnobValue::U32(20_000))
        // Deterministic failure demo: uncomment to point the public WS at an
        // unreachable endpoint and watch backoff → terminal ConnectionFailedEvent.
        // .with_knob("ws_public_url", KnobValue::Str("wss://127.0.0.1:9".into()))
        .build()
        .expect("build (config-validation only)");

    println!("[1] built with resilience knobs:");
    for name in [
        "reconnect_attempts",
        "backoff_base_ms",
        "backoff_max_ms",
        "backoff_factor",
        "backoff_jitter",
        "staleness_window_ms",
    ] {
        println!("      {:<22} = {:?}", name, client.knob(name));
    }
    println!("      + injected custom JitterSource (CyclicJitter)\n");

    // Subscribe to lifecycle events before connecting; keep RAII guards alive.
    let _h_connecting = client
        .events()
        .on(
            EventType::ConnectionConnectingEvent,
            |env: &EventEnvelope| print_conn_event(env),
        )
        .expect("fresh client: no reactor loop has died");
    let _h_open = client
        .events()
        .on(EventType::ConnectionOpenEvent, |env: &EventEnvelope| {
            print_conn_event(env)
        })
        .expect("fresh client: no reactor loop has died");
    let _h_reopened = client
        .events()
        .on(EventType::ConnectionReopenedEvent, |env: &EventEnvelope| {
            print_conn_event(env)
        })
        .expect("fresh client: no reactor loop has died");
    let _h_dropped = client
        .events()
        .on(EventType::ConnectionDroppedEvent, |env: &EventEnvelope| {
            print_conn_event(env)
        })
        .expect("fresh client: no reactor loop has died");
    let _h_attempt_failed = client
        .events()
        .on(
            EventType::ConnectionAttemptFailedEvent,
            |env: &EventEnvelope| print_conn_event(env),
        )
        .expect("fresh client: no reactor loop has died");
    let _h_failed = client
        .events()
        .on(EventType::ConnectionFailedEvent, |env: &EventEnvelope| {
            print_conn_event(env)
        })
        .expect("fresh client: no reactor loop has died");
    let _h_upgrade_ok = client
        .events()
        .on(EventType::WsUpgradeOk, |env: &EventEnvelope| {
            print_conn_event(env)
        })
        .expect("fresh client: no reactor loop has died");
    let _h_upgrade_fail = client
        .events()
        .on(EventType::WsUpgradeFailed, |env: &EventEnvelope| {
            print_conn_event(env)
        })
        .expect("fresh client: no reactor loop has died");

    // Spawn the I/O reactor (dispatch already lazy-started at first events().on).
    client.ready();
    println!("[2] client.ready() — reactor spawned; subscribed to connection events\n");

    // Register a ticker handler — required before subscribe.
    let _h = client.market().on_ticker(|t: &TickerUpdate| {
        println!("[ticker] {} last={}", t.symbol.as_str(), t.last);
    });

    println!("[3] subscribe_ticker(BTC/USD) — triggers the public WS connection");
    match client
        .subscription()
        .subscribe_ticker(vec![Symbol::new("BTC/USD").unwrap()], None, None)
    {
        Ok(()) => println!("      subscribe posted OK\n"),
        Err(e) => println!("      subscribe error: {e:?}\n"),
    }

    println!("[4] observing connection lifecycle for ~5s...\n");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Guards drop → unsubscribe; Client Drop tears reactors down.
    println!("\n=== done (clean exit) ===");
    println!(
        "(tip: point the `ws_public_url` knob at an unreachable endpoint to watch the\n \
         deterministic backoff + terminal-failure path.)"
    );
}
