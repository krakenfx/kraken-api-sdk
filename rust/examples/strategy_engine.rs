//! A deliberately dumb strategy engine: one decision loop fed by three SDK
//! surfaces at once — public ticker updates, private execution updates, and the
//! lifecycle event bus. Every callback does one thing: forward a message into a
//! channel. All state and all decisions live in the owner loop. With
//! KRAKEN_API_KEY + KRAKEN_API_SECRET set, a decision fires a validate-only
//! order (nothing is ever placed); without them it runs public-data-only.

use std::collections::VecDeque;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use kraken_sdk::{
    AddOrderResponse, ApiKey, Client, ClientBuilder, EventType, ExecType, ExecutionUpdate,
    OrderRequest, OrderType, Side, Symbol, TickerUpdate, TradeError,
};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

mod common;

/// Rolling window of mids the mean is taken over.
const WINDOW: usize = 30;
/// Deviation from that mean, in basis points, that makes the engine act.
const THRESHOLD_BPS: &str = "0.3";
/// Bounded run — stop after this many ticks.
const MAX_TICKS: usize = 60;
/// Order size for the validate-only sends.
const CLIP: &str = "0.0001";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dir {
    Long,
    Short,
}

/// The only thing the callbacks are allowed to produce.
enum Msg {
    Tick(Decimal),
    /// Signed fill quantity — positive bought, negative sold.
    Fill(Decimal),
}

enum Action {
    Shoot { dir: Dir, price: Decimal },
    Hold,
}

struct StrategyEngine {
    mids: VecDeque<Decimal>,
    position: Decimal,
    threshold_bps: Decimal,
    paced: Arc<AtomicBool>,
}

impl StrategyEngine {
    fn new(paced: Arc<AtomicBool>) -> Self {
        Self {
            mids: VecDeque::with_capacity(WINDOW),
            position: Decimal::ZERO,
            threshold_bps: THRESHOLD_BPS.parse().expect("THRESHOLD_BPS is a decimal"),
            paced,
        }
    }

    fn push(&mut self, mid: Decimal) {
        if self.mids.len() == WINDOW {
            self.mids.pop_front();
        }
        self.mids.push_back(mid);
    }

    /// Deviation of the newest mid from the window mean, in bps; `None` until
    /// the window fills.
    fn dev_bps(&self) -> Option<Decimal> {
        if self.mids.len() < WINDOW {
            return None;
        }
        let last = *self.mids.back()?;
        let mean = self.mids.iter().copied().sum::<Decimal>() / Decimal::from(self.mids.len());
        if mean.is_zero() {
            return None;
        }
        Some((last - mean) / mean * Decimal::from(10_000))
    }

    /// Mean reversion and nothing else: lean against a runaway mid, never add
    /// to a position already leaning that way, sit out while paced.
    fn decide(&self) -> Action {
        if self.paced.load(Ordering::SeqCst) {
            return Action::Hold;
        }
        let (Some(dev_bps), Some(&last)) = (self.dev_bps(), self.mids.back()) else {
            return Action::Hold;
        };
        let dir = if dev_bps > self.threshold_bps {
            Dir::Short
        } else if dev_bps < -self.threshold_bps {
            Dir::Long
        } else {
            return Action::Hold;
        };
        match dir {
            Dir::Long if self.position > Decimal::ZERO => Action::Hold,
            Dir::Short if self.position < Decimal::ZERO => Action::Hold,
            _ => Action::Shoot { dir, price: last },
        }
    }
}

fn env(name: &str) -> Option<String> {
    // Creds: process env first, then repo .env (non-empty only).
    std::env::var(name)
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| common::dotenv_lookup(name).filter(|s| !s.is_empty()))
}

type OrderFuture = Pin<Box<dyn Future<Output = Result<AddOrderResponse, TradeError>> + Send>>;

/// Build the send for a decision. `validate = true` means Kraken shape-checks
/// the order and discards it — no order reaches the book from this example.
fn validate_only(
    client: &Client,
    pair: &Symbol,
    dir: Dir,
    price: Decimal,
    qty: Decimal,
) -> OrderFuture {
    // BTC/USD takes one decimal; a real strategy would read pair metadata.
    let price = price.round_dp(1);
    match dir {
        Dir::Long => {
            let r = OrderRequest::new(pair.clone(), qty, Side::Buy)
                .order_type(OrderType::Limit)
                .price(price)
                .validate_only(true);
            Box::pin(client.trade().order(r).into_future())
        }
        Dir::Short => {
            let r = OrderRequest::new(pair.clone(), qty, Side::Sell)
                .order_type(OrderType::Limit)
                .price(price)
                .validate_only(true);
            Box::pin(client.trade().order(r).into_future())
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pair = Symbol::new("BTC/USD")?;
    let clip: Decimal = CLIP.parse()?;

    let (client, authed) = match (env("KRAKEN_API_KEY"), env("KRAKEN_API_SECRET")) {
        (Some(k), Some(s)) => (
            Client::builder().with_api_key(ApiKey::new(k), s).build()?,
            true,
        ),
        _ => {
            println!("[main] no credentials — public data only; decisions are logged, never sent");
            (ClientBuilder::new().build()?, false)
        }
    };
    client.ready();

    let (tx, mut rx) = mpsc::channel::<Msg>(256);
    let paced = Arc::new(AtomicBool::new(false));

    // SDK warns on rate limits but never sleeps; one warning parks this run.
    let _paced_sub = {
        let flag = paced.clone();
        client.events().on(EventType::RateLimitWarning, move |_| {
            flag.store(true, Ordering::SeqCst);
        })?
    };

    // `on_ticker_for` registers + subscribes; drop the guard to unsubscribe.
    let ticker_tx = tx.clone();
    let _ticker = client.market().on_ticker_for(
        std::slice::from_ref(&pair),
        None,
        // Default trades trigger — BBO would fire on quantity churn at a pinned top.
        None,
        move |t: &TickerUpdate| {
            // Dispatch loop: compute a mid, forward, return. Nothing else.
            let _ = ticker_tx.try_send(Msg::Tick((t.bid + t.ask) / Decimal::from(2)));
        },
    )?;

    // Register, then subscribe. Validate-only never fills; a fill means trading elsewhere.
    let _exec = if authed {
        let exec_tx = tx.clone();
        let handle = client.account().on_executions(move |u: &ExecutionUpdate| {
            if u.exec_type != ExecType::Trade {
                return;
            }
            let (Some(side), Some(qty)) = (u.side, u.last_qty) else {
                return;
            };
            let signed = match side {
                Side::Buy => qty,
                Side::Sell => -qty,
                _ => return,
            };
            let _ = exec_tx.try_send(Msg::Fill(signed));
        });
        client.subscription().subscribe_executions()?;
        Some(handle)
    } else {
        None
    };

    let mut engine = StrategyEngine::new(paced.clone());
    let in_flight = Arc::new(AtomicBool::new(false));
    let (mut ticks, mut shots, mut skipped) = (0usize, 0usize, 0usize);

    println!(
        "[main] window={WINDOW} threshold={THRESHOLD_BPS}bps max_ticks={MAX_TICKS} authed={authed} (Ctrl-C to stop)"
    );

    while ticks < MAX_TICKS {
        let msg = tokio::select! {
            m = rx.recv() => match m { Some(m) => m, None => break },
            _ = tokio::signal::ctrl_c() => { println!("[main] Ctrl-C"); break }
            _ = tokio::time::sleep(Duration::from_secs(20)) => { println!("[main] 20s with no update"); break }
        };
        match msg {
            Msg::Fill(qty) => {
                engine.position += qty;
                println!("[fill] position now {}", engine.position);
            }
            Msg::Tick(mid) => {
                ticks += 1;
                engine.push(mid);
                if ticks % 10 == 0 {
                    println!(
                        "[tick {ticks}] mid={mid} dev={}bps",
                        engine.dev_bps().unwrap_or_default().round_dp(2)
                    );
                }
                let Action::Shoot { dir, price } = engine.decide() else {
                    continue;
                };
                shots += 1;
                println!("[decide] {dir:?} @ {price} (tick {ticks})");
                if !authed {
                    continue;
                }
                // One attempt at a time; mid-flight decisions are dropped, not queued.
                if in_flight.swap(true, Ordering::SeqCst) {
                    skipped += 1;
                    continue;
                }
                let fut = validate_only(&client, &pair, dir, price, clip);
                let done = in_flight.clone();
                tokio::spawn(async move {
                    match fut.await {
                        Ok(_) => println!("[order] validate accepted"),
                        Err(e) => println!("[order] validate rejected: {e:?}"),
                    }
                    done.store(false, Ordering::SeqCst);
                });
            }
        }
    }

    // Let a final in-flight validate settle before close.
    for _ in 0..20 {
        if !in_flight.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!(
        "[summary] ticks={ticks} shoot={shots} hold={} skipped={skipped} position={} paced={}",
        ticks - shots,
        engine.position,
        paced.load(Ordering::SeqCst)
    );

    client.close().await?;
    Ok(())
}
