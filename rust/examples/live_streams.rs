//! Multi-channel streaming + guard teardown: channel-wide on_trade/on_ohlc plus
//! a guard-scoped on_ticker_for dropped mid-run — the wire unsubscribe fires on
//! the last guard drop, so ticker lines stop. Public WS, no credentials.

use std::time::Duration;

use kraken_sdk::{
    ClientBuilder, OhlcInterval, OhlcUpdate, Symbol, SystemStatusUpdate, TickerTrigger,
    TickerUpdate, TradeUpdate,
};

fn btc() -> Symbol {
    Symbol::new("BTC/USD").unwrap()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kraken_sdk=info".into()),
        )
        .init();

    let client = ClientBuilder::new().build().expect("build");
    client.ready();

    // Register system-status first — status arrives as the first public-WS frame once trade/ohlc connect.
    let _ss = client.market().on_system_status(|s: &SystemStatusUpdate| {
        println!(
            "[status] system={} version={} api_version={} (connection_id omitted)",
            s.system.as_deref().unwrap_or("?"),
            s.version.as_deref().unwrap_or("?"),
            s.api_version.as_deref().unwrap_or("?"),
        );
    });

    // Channel-wide handlers must be registered before subscribe.
    let _t = client.market().on_trade(|t: &TradeUpdate| {
        println!(
            "[trade] {} {:?} px={} qty={} id={}",
            t.symbol.as_str(),
            t.side,
            t.price,
            t.qty,
            t.trade_id
        );
    });
    let _o = client.market().on_ohlc(|c: &OhlcUpdate| {
        println!(
            "[ohlc] {} O={} H={} L={} C={} vol={} ({}m @ {})",
            c.symbol.as_str(),
            c.open,
            c.high,
            c.low,
            c.close,
            c.volume,
            c.interval,
            c.interval_begin
        );
    });

    if let Err(e) = client.subscription().subscribe_trade(vec![btc()], None) {
        println!("[main] trade sub error: {e:?}");
    }
    if let Err(e) = client
        .subscription()
        .subscribe_ohlc(vec![btc()], OhlcInterval::M1, None)
    {
        println!("[main] ohlc sub error: {e:?}");
    }

    // Guard-scoped ticker (self-registers + subscribes); drop mid-run for teardown. Bbo = top-of-book updates.
    let ticker_guard = client
        .market()
        .on_ticker_for(
            &[btc()],
            None,
            Some(TickerTrigger::Bbo),
            |t: &TickerUpdate| {
                println!("[ticker] {} last={}", t.symbol.as_str(), t.last);
            },
        )
        .expect("on_ticker_for");

    println!("[main] streaming trade + ohlc + ticker for 10s (Ctrl-C to stop early)...");
    // Dropping the guard unsubscribes; dropping `client` tears connections down.
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(10)) => {}
        r = tokio::signal::ctrl_c() => {
            if r.is_ok() {
                println!("[main] Ctrl-C received — shutting down cleanly");
            }
            drop(ticker_guard);
            let _ = client.close().await;
            println!("[main] done");
            return;
        }
    }

    println!(
        "[main] >>> dropping the ticker guard — expect [ticker] lines to STOP (wire unsubscribe), trade+ohlc continue <<<"
    );
    drop(ticker_guard);

    println!("[main] streaming trade + ohlc (ticker torn down) for 8s...");
    tokio::time::sleep(Duration::from_secs(8)).await;
    let _ = client.close().await;
    println!("[main] done");
}
