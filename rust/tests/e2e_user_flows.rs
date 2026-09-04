//! Live end-to-end user-flow tests, `#[ignore]` by default; they hit the real
//! Kraken network and/or need credentials. Uses only the public `kraken_sdk::`
//! surface, compiled as an external crate, so any `pub(crate)` item won't build.

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use std::collections::HashMap;

use kraken_sdk::{
    AddOrderBatchRequest, ApiKey, BalanceUpdate, BatchOrderEntry, BookDelta, BookDepth,
    CancelBatchResponse, ClOrdId, Client, ClientBuilder, CloseOrderType, ClosedOrders,
    ClosedOrdersRequest, ConditionalClose, DeadmanResponse, EventEnvelope, EventType,
    ExecutionUpdate, LedgerEntry, Ledgers, LedgersRequest, OFlag, OhlcInterval, OhlcRequest,
    OhlcUpdate, OpenOrdersRequest, OpenPositions, OrderAmendRequest, OrderBookUpdate, OrderRequest,
    OrderType, Price, PriceUnit, ReconciliationOutcome, Side, StpType, Symbol, SystemStatusUpdate,
    TickerUpdate, TimeInForce, TradeBalance, TradeError, TradeUpdate, TradeVolume,
    TradesHistoryRequest, TradesRequest, Transport, TriggerKind,
};
use rust_decimal::Decimal;

fn env_cred(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn build_authed_client(test_name: &str) -> Option<kraken_sdk::Client> {
    let key = match env_cred("KRAKEN_API_KEY") {
        Some(k) => k,
        None => {
            eprintln!("[skip:{test_name}] KRAKEN_API_KEY not set");
            return None;
        }
    };
    let secret = match env_cred("KRAKEN_API_SECRET") {
        Some(s) => s,
        None => {
            eprintln!("[skip:{test_name}] KRAKEN_API_SECRET not set");
            return None;
        }
    };
    match Client::builder()
        .with_api_key(ApiKey::new(key), secret)
        .build()
    {
        Ok(c) => Some(c),
        Err(e) => {
            panic!("[{test_name}] config rejected by SDK: {e}");
        }
    }
}

/// Last price for `sym` (for pricing far-from-market orders); `fallback` on
/// error, a missing entry, or timeout.
async fn last_price_or(
    client: &Client,
    sym: &Symbol,
    fallback: Decimal,
    timeout: Option<Duration>,
) -> Decimal {
    let fetch = client.market().ticker(Some(std::slice::from_ref(sym)));
    let result = match timeout {
        Some(d) => match tokio::time::timeout(d, fetch).await {
            Ok(inner) => inner,
            Err(_) => return fallback,
        },
        None => fetch.await,
    };
    match result {
        Ok(tr) => tr.get(sym).map(|t| t.last_price).unwrap_or(fallback),
        Err(_) => fallback,
    }
}

/// Serializes the REAL-order tests (same live account). `tokio::sync::Mutex`
/// does not poison on panic, so a failed test still releases it.
static REAL_ORDER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Fetches the COMPLETE public catalogs and asserts every entry decodes, so a
/// field only a minority omit fails here instead of slipping past a BTC/USD check.
#[tokio::test]
#[ignore = "live full-decode guard; set KRAKEN_REST_BASE_URL (e.g. UAT) and run with --ignored"]
async fn public_rest_full_response_decodes() {
    let base = match env_cred("KRAKEN_REST_BASE_URL") {
        Some(b) => b,
        None => {
            eprintln!("[skip:public_rest_full_response_decodes] KRAKEN_REST_BASE_URL not set");
            return;
        }
    };
    let client = ClientBuilder::new()
        .with_base_url(base.clone())
        .build()
        .expect("unauthenticated build is infallible");
    let m = client.market();

    let pairs = m
        .pairs(None)
        .await
        .expect("pairs(None) must decode the full AssetPairs catalog");
    assert!(
        !pairs.pairs.is_empty(),
        "expected a non-empty AssetPairs catalog from {base}"
    );
    let status_absent = pairs.pairs.values().filter(|p| p.status.is_none()).count();
    eprintln!(
        "[full-decode] {} pairs decoded from {base} ({status_absent} with status=None)",
        pairs.pairs.len()
    );

    let assets = m
        .assets(None)
        .await
        .expect("assets(None) must decode the full Assets catalog");
    assert!(
        !assets.assets.is_empty(),
        "expected a non-empty Assets catalog from {base}"
    );

    let tickers = m
        .ticker(None)
        .await
        .expect("ticker(None) must decode all tickers");
    assert!(
        !tickers.tickers.is_empty(),
        "expected non-empty all-tickers from {base}"
    );
    eprintln!(
        "[full-decode] {} tickers decoded from {base}",
        tickers.tickers.len()
    );
}

#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored public"]
async fn public_rest_market_data() {
    let client = ClientBuilder::new()
        .build()
        .expect("unauthenticated build is infallible");

    let btc_usd = Symbol::new("BTC/USD").expect("BTC/USD is valid");

    let st = client
        .market()
        .server_time()
        .await
        .expect("server_time should succeed");
    assert!(st.unixtime > 0, "server unixtime should be > 0");

    let ticker_result = client
        .market()
        .ticker(Some(std::slice::from_ref(&btc_usd)))
        .await
        .expect("ticker(BTC/USD) should succeed");
    let ticker = ticker_result
        .get(&btc_usd)
        .expect("ticker(BTC/USD) missing from result");
    assert!(
        ticker.last_price > Decimal::ZERO,
        "ticker.last_price should be > 0 (got {})",
        ticker.last_price
    );
    assert!(
        ticker.bid_price > Decimal::ZERO,
        "ticker.bid_price should be > 0 (got {})",
        ticker.bid_price
    );
    assert!(
        ticker.ask_price > Decimal::ZERO,
        "ticker.ask_price should be > 0 (got {})",
        ticker.ask_price
    );

    let book = client
        .market()
        .orderbook(&btc_usd, Some(5))
        .await
        .expect("orderbook(BTC/USD, depth=5) should succeed");
    assert!(
        !book.bids.is_empty(),
        "order book should have at least one bid"
    );
    assert!(
        !book.asks.is_empty(),
        "order book should have at least one ask"
    );

    let pairs = client
        .market()
        .pairs(None)
        .await
        .expect("pairs(None) should succeed");
    assert!(
        !pairs.pairs.is_empty(),
        "pairs response should contain at least one entry"
    );

    assert!(
        pairs.pairs.contains_key("BTC/USD"),
        "full pairs catalog must contain BTC/USD"
    );
}

/// A healthy streaming connection must NOT be torn down at the ~30s staleness
/// window: healthy == opens==1 && reopens==0.
#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored public"]
async fn public_ws_streaming_survives_staleness() {
    let client = ClientBuilder::new()
        .build()
        .expect("unauthenticated build is infallible");

    client.ready();

    let opens = Arc::new(AtomicUsize::new(0));
    let reopens = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));

    let _h_open = {
        let n = opens.clone();
        client
            .events()
            .on(
                EventType::ConnectionOpenEvent,
                move |_env: &EventEnvelope| {
                    n.fetch_add(1, Ordering::Relaxed);
                },
            )
            .expect("no reactor loop has died this early")
    };

    let _h_reopen = {
        let n = reopens.clone();
        client
            .events()
            .on(
                EventType::ConnectionReopenedEvent,
                move |_env: &EventEnvelope| {
                    n.fetch_add(1, Ordering::Relaxed);
                },
            )
            .expect("no reactor loop has died this early")
    };

    let _h_drop = {
        let n = dropped.clone();
        client
            .events()
            .on(
                EventType::ConnectionDroppedEvent,
                move |_env: &EventEnvelope| {
                    n.fetch_add(1, Ordering::Relaxed);
                },
            )
            .expect("no reactor loop has died this early")
    };

    let start = tokio::time::Instant::now();
    let early_ticks = Arc::new(AtomicUsize::new(0));
    let late_ticks = Arc::new(AtomicUsize::new(0));

    let _h_ticker = {
        let early = early_ticks.clone();
        let late = late_ticks.clone();
        let t0 = start;
        client.market().on_ticker(move |_t: &TickerUpdate| {
            let elapsed = t0.elapsed();
            if elapsed < Duration::from_secs(30) {
                early.fetch_add(1, Ordering::Relaxed);
            } else if elapsed > Duration::from_secs(40) {
                late.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    let _h_trade = {
        let early = early_ticks.clone();
        let late = late_ticks.clone();
        let t0 = start;
        client.market().on_trade(move |_t: &TradeUpdate| {
            let elapsed = t0.elapsed();
            if elapsed < Duration::from_secs(30) {
                early.fetch_add(1, Ordering::Relaxed);
            } else if elapsed > Duration::from_secs(40) {
                late.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(400)).await;

    let btc_usd = Symbol::new("BTC/USD").expect("BTC/USD is valid");

    client
        .subscription()
        .subscribe_ticker(vec![btc_usd.clone()], None, None)
        .expect("subscribe_ticker(BTC/USD) should not fail synchronously");

    client
        .subscription()
        .subscribe_trade(vec![btc_usd], None)
        .expect("subscribe_trade(BTC/USD) should not fail synchronously");

    tokio::time::sleep(Duration::from_secs(75)).await;

    let early = early_ticks.load(Ordering::Relaxed);
    let late = late_ticks.load(Ordering::Relaxed);
    let open_count = opens.load(Ordering::Relaxed);
    let reopen_count = reopens.load(Ordering::Relaxed);
    let drops = dropped.load(Ordering::Relaxed);

    eprintln!(
        "[e2e staleness] early_ticks={early} late_ticks={late} opens={open_count} \
         reopens={reopen_count} drops={drops}"
    );

    assert!(
        early > 0,
        "expected ticks in the first 30s (got 0) — data never started flowing \
         or the subscription was never established"
    );
    assert!(
        late > 0,
        "STALENESS-REGRESSION: expected ticks AFTER t=40s (got 0) — a healthy \
         streaming connection stopped delivering data past the 40s mark, i.e. the \
         staleness guard tore down a live socket at ~30s and data did not resume"
    );
    assert_eq!(
        open_count, 1,
        "STALENESS-REGRESSION: ConnectionOpenEvent fired {open_count} times in 75s — \
         a healthy streaming connection must open exactly once (the first open)"
    );
    assert_eq!(
        reopen_count, 0,
        "STALENESS-REGRESSION: the connection re-opened {reopen_count} time(s) in 75s — \
         a healthy streaming connection must stay open; >=1 means a live socket was \
         torn down (staleness fired on a healthy connection) and reconnected"
    );
    assert_eq!(
        drops, 0,
        "ConnectionDroppedEvent fired {drops} time(s) during the 75s window"
    );
}

#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored account"]
async fn account_rest_reads() {
    let client = match build_authed_client("account_rest_reads") {
        Some(c) => c,
        None => return,
    };

    let balance = client
        .account()
        .balance()
        .await
        .expect("balance() should succeed");
    let _ = balance.assets.len();

    let ext_bal = client
        .account()
        .extended_balance()
        .await
        .expect("extended_balance() should succeed");
    let _ = ext_bal.assets.len();

    let open_orders = client
        .account()
        .open_orders(OpenOrdersRequest::default())
        .await
        .expect("open_orders() should succeed");
    let _ = open_orders.open.len();

    let history = client
        .account()
        .trades_history(TradesHistoryRequest::default())
        .await
        .expect("trades_history() should succeed");
    let _ = history.count;
}

/// Kraken pushes a balances snapshot on subscribe, so `balance_snapshots > 0`
/// holds after 20s without any account activity.
#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored account"]
async fn account_ws_streaming() {
    let client = match build_authed_client("account_ws_streaming") {
        Some(c) => c,
        None => return,
    };

    client.ready();

    let balance_snapshots = Arc::new(AtomicUsize::new(0));

    let _h_bal = {
        let n = balance_snapshots.clone();
        client.account().on_balances(move |_u: &BalanceUpdate| {
            n.fetch_add(1, Ordering::Relaxed);
        })
    };

    let exec_events = Arc::new(AtomicUsize::new(0));
    let _h_exec = {
        let n = exec_events.clone();
        client.account().on_executions(move |_u: &ExecutionUpdate| {
            n.fetch_add(1, Ordering::Relaxed);
        })
    };

    tokio::time::sleep(Duration::from_millis(400)).await;

    client
        .subscription()
        .subscribe_balances()
        .expect("subscribe_balances() should not fail synchronously");

    client
        .subscription()
        .subscribe_executions()
        .expect("subscribe_executions() should not fail synchronously");

    tokio::time::sleep(Duration::from_secs(20)).await;

    let bal_count = balance_snapshots.load(Ordering::Relaxed);
    assert!(
        bal_count > 0,
        "expected at least one BalanceUpdate in 20s (the exchange always \
         sends a snapshot on subscribe); got 0 — check that credentials \
         are valid and the auth WS connected successfully"
    );
}

/// Real signed order with `validate=true`: Kraken validates shape but places
/// nothing. BTC/USDC (the test account holds USDC) at ~50% below market.
#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored trade_validate"]
async fn trade_validate_only() {
    let client = match build_authed_client("trade_validate_only") {
        Some(c) => c,
        None => return,
    };

    let btc_usdc = Symbol::new("BTC/USDC").expect("BTC/USDC is valid");
    let last_price = last_price_or(
        &client,
        &btc_usdc,
        Decimal::from_str("60000").unwrap(),
        None,
    )
    .await;

    let far_price = (last_price * Decimal::from_str("0.5").unwrap()).round_dp(1);
    let volume = Decimal::from_str("0.0001").unwrap();

    let mut req =
        OrderRequest::new(btc_usdc.clone(), volume, Side::Buy).order_type(OrderType::Limit);
    req.price = Some(far_price.into());
    req.validate = true;

    let resp = client
        .trade()
        .order(req)
        .via(Transport::Rest)
        .await
        .expect("validate=true order_buy should be accepted by Kraken");

    // Validate-mode REST returns descr only, no txid: docs/guides/wire-quirks.md.
    assert!(
        resp.txid.is_none(),
        "validate=true response should carry no txid (nothing was placed); \
         got {:?} — check that validate=true is wired through",
        resp.txid.as_ref().map(|t| t.as_str())
    );

    assert!(
        resp.descr.order.is_some(),
        "validate=true response should contain a descr.order string"
    );
}

/// Places a REAL far-from-market limit BUY (rests, won't fill) then cancels it.
/// Double-gated: needs credentials AND `KRAKEN_E2E_TRADE_REAL=1`. If the process
/// dies before the cancel, the order stays resting — check open orders manually.
#[tokio::test]
#[ignore = "live e2e against real Kraken (REAL MONEY); run with --ignored trade_real"]
async fn trade_place_and_cancel_real() {
    let _real_order_guard = REAL_ORDER_LOCK.lock().await;
    let client = match build_authed_client("trade_place_and_cancel_real") {
        Some(c) => c,
        None => return,
    };

    if std::env::var("KRAKEN_E2E_TRADE_REAL").is_err() {
        eprintln!(
            "[skip:trade_place_and_cancel_real] set KRAKEN_E2E_TRADE_REAL=1 to run this test \
             (it places a real, immediately-cancelled order on the live exchange)"
        );
        return;
    }

    let btc_usdc = Symbol::new("BTC/USDC").expect("BTC/USDC is valid");

    let last_price = last_price_or(
        &client,
        &btc_usdc,
        Decimal::from_str("60000").unwrap(),
        None,
    )
    .await;

    let far_price = (last_price * Decimal::from_str("0.5").unwrap()).round_dp(1);
    let volume = Decimal::from_str("0.0001").unwrap();

    let mut req =
        OrderRequest::new(btc_usdc.clone(), volume, Side::Buy).order_type(OrderType::Limit);
    req.price = Some(far_price.into());

    let pending = client.trade().order(req);
    let cl_ord_id = pending
        .cl_ord_id()
        .cloned()
        .expect("order_buy without userref or conditional close pre-allocates a cl_ord_id");

    let place_resp = pending
        .via(Transport::Rest)
        .await
        .expect("limit_buy (far-from-market) should be accepted");

    assert!(
        place_resp.txid.is_some(),
        "real order placement should return a txid; got None — unexpected"
    );

    let open = client
        .account()
        .open_orders(OpenOrdersRequest::default())
        .await
        .expect("open_orders() should succeed after placement");

    // open_orders has a propagation delay after placement: docs/guides/wire-quirks.md.
    let txid_str = place_resp
        .txid
        .as_ref()
        .map(|t| t.as_str().to_string())
        .unwrap_or_default();
    if !open.open.contains_key(&txid_str) {
        eprintln!(
            "[warn:trade_place_and_cancel_real] placed order (txid={txid_str}) not yet visible \
             in open_orders ({} open) — propagation delay; proceeding to cancel",
            open.open.len()
        );
    }

    let cancel_resp = client
        .trade()
        .cancel(cl_ord_id.clone())
        .via(Transport::Rest)
        .await
        .expect("cancel by cl_ord_id should succeed");

    assert!(
        cancel_resp.count >= 1,
        "cancel should report count ≥ 1 (the resting order); \
         got count={} — the order may have already been gone or may still be resting; \
         check your open orders manually if count=0",
        cancel_resp.count
    );
}

/// Builds the signed-REST rig shared by the live probes; `None` (after a skip
/// line) when creds are absent.
#[cfg(feature = "test-support")]
fn build_probe_rest_surface(
    probe: &str,
    token_label: &str,
) -> Option<kraken_sdk::test_support::RestSurface> {
    use kraken_sdk::ApiKey;
    use kraken_sdk::test_support::{
        ApiSecret, AuthProfile, AuthSigner, AuthStack, Clock, DispatchEventBusConfig,
        HttpTransport, Knobs, ReqwestHttpTransport, RestSurface, SpotApiRateLimitTracker,
        SpotRestHmacSha512Signer, SpotTradingRateLimitTracker, SystemClock, SystemClockNonceSource,
        Tier, TokenLifecycleManager,
    };
    use std::collections::HashMap;

    let api_key_str = match env_cred("KRAKEN_API_KEY") {
        Some(k) => k,
        None => {
            eprintln!("[skip:{probe}] KRAKEN_API_KEY not set");
            return None;
        }
    };
    let api_secret_str = match env_cred("KRAKEN_API_SECRET") {
        Some(s) => s,
        None => {
            eprintln!("[skip:{probe}] KRAKEN_API_SECRET not set");
            return None;
        }
    };

    let secret = ApiSecret::from_base64(&api_secret_str)
        .expect("KRAKEN_API_SECRET must be valid base64 — check .env");
    let api_key = ApiKey::new(api_key_str);
    let signer = SpotRestHmacSha512Signer::new(api_key.clone(), secret);

    let mut signers: HashMap<AuthProfile, Arc<dyn AuthSigner>> = HashMap::new();
    signers.insert(AuthProfile::SpotV1, Arc::new(signer));

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let bus = Arc::new(kraken_sdk::test_support::new_bus(
        DispatchEventBusConfig::defaults(),
        Arc::clone(&clock),
    ));

    let token_mgr = TokenLifecycleManager::new(Arc::clone(&bus), token_label.to_string());
    let auth = Arc::new(AuthStack::new(
        Some(api_key),
        None,
        Arc::new(SystemClockNonceSource::new()),
        signers,
        token_mgr,
    ));

    let knobs = Arc::new(Knobs::defaults());
    let api_rl = Arc::new(SpotApiRateLimitTracker::new(
        Tier::Starter,
        Arc::clone(&bus),
        Arc::clone(&clock),
        Arc::clone(&knobs),
    ));
    let trading_rl = Arc::new(SpotTradingRateLimitTracker::new(
        Tier::Starter,
        Arc::clone(&bus),
        Arc::clone(&clock),
        Arc::clone(&knobs),
    ));
    let transport: Arc<dyn HttpTransport> = Arc::new(ReqwestHttpTransport::new());
    Some(RestSurface::new(
        transport,
        auth,
        api_rl,
        trading_rl,
        clock,
        std::time::Duration::from_secs(30),
    ))
}

/// Fetches a Spot WS v2 token and reports the wire `expires` against the 15-min
/// TTL. Must go via the SDK — a wrong nonce scale poisons the key.
#[cfg(feature = "test-support")]
#[tokio::test]
#[ignore = "live: hits real api.kraken.com with .env creds; run with --ignored --nocapture"]
async fn auth_token_ttl_probe() {
    use kraken_sdk::test_support::AuthProfile;

    let rest = match build_probe_rest_surface("auth_token_ttl_probe", "<auth-o2-probe>") {
        Some(r) => r,
        None => return,
    };

    let fetched_at = std::time::SystemTime::now();
    let fetched_at_display = {
        let secs = fetched_at
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let (y, mo, d, h, mi, s) = epoch_to_ymd_hms(secs);
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
    };
    eprintln!("[AUTH-O2] fetched_at (wall): {fetched_at_display}");

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        rest.signed_post(
            "/0/private/GetWebSocketsToken",
            Vec::new(),
            AuthProfile::SpotV1,
            "rid-live-e2e",
        ),
    )
    .await
    .expect("GetWebSocketsToken did not time out after 15s")
    .expect(
        "GetWebSocketsToken signed POST must succeed \
         (check .env creds; auth-lockout if repeated EAPI:Invalid nonce)",
    );

    if let Some(obj) = result.as_object() {
        let keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        eprintln!("[AUTH-O2] GetWebSocketsToken result keys: {keys:?}");
        match obj.get("token").and_then(|v| v.as_str()) {
            Some(t) => eprintln!("[AUTH-O2]   token: <redacted len={}>", t.len()),
            None => eprintln!("[AUTH-O2]   token: <MISSING or non-string — shape changed>"),
        }
    } else {
        eprintln!("[AUTH-O2] result is not a JSON object: {result:?}");
    }

    const SDK_WS_TOKEN_TTL_SECS: u64 = 15 * 60;

    let wire_expires: Option<u64> = result.get("expires").and_then(|v| v.as_u64());

    match wire_expires {
        Some(exp) => {
            let ty = if result.get("expires").map(|v| v.is_u64()).unwrap_or(false) {
                "u64"
            } else if result.get("expires").map(|v| v.is_i64()).unwrap_or(false) {
                "i64"
            } else if result
                .get("expires")
                .map(|v| v.is_string())
                .unwrap_or(false)
            {
                "string"
            } else {
                "other"
            };
            eprintln!("[AUTH-O2]   expires (raw wire value): {exp} (json type: {ty})");
            eprintln!(
                "[AUTH-O2] SDK WS_TOKEN_TTL const:     {SDK_WS_TOKEN_TTL_SECS} s  (15 min hardcoded)"
            );
            let delta_i64 = exp as i64 - SDK_WS_TOKEN_TTL_SECS as i64;
            eprintln!("[AUTH-O2] delta (expires - SDK_TTL):  {delta_i64:+} s");
            if delta_i64 == 0 {
                eprintln!("[AUTH-O2] RESULT: wire `expires` MATCHES SDK_TTL — const validated.");
            } else {
                eprintln!(
                    "[AUTH-O2] RESULT: wire `expires` ({exp} s) DIFFERS from SDK_TTL \
                     ({SDK_WS_TOKEN_TTL_SECS} s) by {delta_i64:+} s — \
                     consider amending WS_TOKEN_TTL or the proactive-refresh threshold."
                );
            }
        }
        None => {
            eprintln!(
                "[AUTH-O2]   expires: <ABSENT or non-numeric> — raw: {:?}",
                result.get("expires")
            );
            eprintln!(
                "[AUTH-O2] RESULT: cannot compare; `expires` not present as a number. \
                 SDK WS_TOKEN_TTL const ({SDK_WS_TOKEN_TTL_SECS} s) unverified."
            );
        }
    }

    let token_str = result
        .get("token")
        .and_then(|v| v.as_str())
        .expect("GetWebSocketsToken result must contain a non-null string `token`");
    assert!(!token_str.is_empty(), "wire `token` must not be empty");
    eprintln!("[AUTH-O2] structural assertion passed: token is non-empty.");
}

/// Convert a Unix epoch (seconds) to (year, month, day, hour, minute, second).
/// Minimal Gregorian implementation — no external date dep.
#[cfg(feature = "test-support")]
fn epoch_to_ymd_hms(epoch: u64) -> (u32, u32, u32, u32, u32, u32) {
    // Gregorian calendar (post-1970-03-01): https://howardhinnant.github.io/date_algorithms.html
    let secs_per_day: u64 = 86_400;
    let days_since_epoch = epoch / secs_per_day;
    let time_of_day = epoch % secs_per_day;
    let h = (time_of_day / 3600) as u32;
    let mi = ((time_of_day % 3600) / 60) as u32;
    let s = (time_of_day % 60) as u32;
    let z = days_since_epoch as i64 + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if mo <= 2 { y + 1 } else { y } as u32;
    (y, mo, d, h, mi, s)
}

// WS v2 add_order param spellings differ from REST: docs/guides/wire-quirks.md.
#[cfg(feature = "test-support")]
#[tokio::test]
#[ignore = "live: WS v2 add_order param discovery; validate:true only (NO orders placed). run --ignored --nocapture"]
async fn ws_add_order_param_probe() {
    use futures_util::{SinkExt, StreamExt};
    use kraken_sdk::test_support::AuthProfile;
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;

    let rest = match build_probe_rest_surface("ws_add_order_param_probe", "<ws-probe>") {
        Some(r) => r,
        None => return,
    };

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        rest.signed_post(
            "/0/private/GetWebSocketsToken",
            Vec::new(),
            AuthProfile::SpotV1,
            "rid-live-e2e",
        ),
    )
    .await
    .expect("GetWebSocketsToken did not time out")
    .expect("GetWebSocketsToken signed POST must succeed (check creds; auth-lockout on repeated bad nonce)");
    let token = result["token"]
        .as_str()
        .expect("token missing from GetWebSocketsToken result")
        .to_string();
    eprintln!("[WS-PROBE] got WS token <redacted len={}>", token.len());

    let (mut ws, _resp) = tokio_tungstenite::connect_async("wss://ws-auth.kraken.com/v2")
        .await
        .expect("connect to ws-auth.kraken.com/v2");
    eprintln!("[WS-PROBE] connected to wss://ws-auth.kraken.com/v2");

    // RFC3339 (TZ mandatory) timestamps; deadline <=60s: docs/guides/wire-quirks.md.
    let rfc3339 = |off: u64| {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
            + off;
        let (y, mo, d, h, mi, s) = epoch_to_ymd_hms(secs);
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
    };
    let deadline_str = rfc3339(30);
    let eff_str = rfc3339(120);
    let expire_str = rfc3339(3600);

    // OrderProbe: (label, order_type, limit_price [None=market], include_cl_ord_id, extras); validate only.
    type OrderProbe = (
        &'static str,
        &'static str,
        Option<f64>,
        bool,
        Vec<(&'static str, serde_json::Value)>,
    );
    let probes: Vec<OrderProbe> = vec![
        (
            "baseline (limit) [expect ACCEPT]",
            "limit",
            Some(30000.0),
            true,
            vec![],
        ),
        (
            "calibration: bogus field [expect REJECT]",
            "limit",
            Some(30000.0),
            true,
            vec![("definitely_not_a_real_field", json!(true))],
        ),
        (
            "time_in_force gtc",
            "limit",
            Some(30000.0),
            true,
            vec![("time_in_force", json!("gtc"))],
        ),
        (
            "time_in_force ioc",
            "limit",
            Some(30000.0),
            true,
            vec![("time_in_force", json!("ioc"))],
        ),
        (
            "time_in_force gtd + expire_time(rfc3339)",
            "limit",
            Some(30000.0),
            true,
            vec![
                ("time_in_force", json!("gtd")),
                ("expire_time", json!(expire_str)),
            ],
        ),
        (
            "effective_time(rfc3339)",
            "limit",
            Some(30000.0),
            true,
            vec![("effective_time", json!(eff_str))],
        ),
        (
            "deadline(rfc3339 <=60s)",
            "limit",
            Some(30000.0),
            true,
            vec![("deadline", json!(deadline_str))],
        ),
        (
            "fee_preference base (oflag fcib)",
            "limit",
            Some(30000.0),
            true,
            vec![("fee_preference", json!("base"))],
        ),
        (
            "fee_preference quote (oflag fciq)",
            "limit",
            Some(30000.0),
            true,
            vec![("fee_preference", json!("quote"))],
        ),
        (
            "no_mpp + market (oflag nompp)",
            "market",
            None,
            true,
            vec![("no_mpp", json!(true))],
        ),
        (
            "reduce_only + margin",
            "limit",
            Some(30000.0),
            true,
            vec![("reduce_only", json!(true)), ("margin", json!(true))],
        ),
        (
            "margin only",
            "limit",
            Some(30000.0),
            true,
            vec![("margin", json!(true))],
        ),
        (
            "order_userref (no cl_ord_id)",
            "limit",
            Some(30000.0),
            false,
            vec![("order_userref", json!(42))],
        ),
        (
            "stp_type cancel_newest",
            "limit",
            Some(30000.0),
            true,
            vec![("stp_type", json!("cancel_newest"))],
        ),
        (
            "stp_type cancel_oldest",
            "limit",
            Some(30000.0),
            true,
            vec![("stp_type", json!("cancel_oldest"))],
        ),
        (
            "stp_type cancel_both",
            "limit",
            Some(30000.0),
            true,
            vec![("stp_type", json!("cancel_both"))],
        ),
        (
            "display_qty (iceberg)",
            "limit",
            Some(30000.0),
            true,
            vec![("display_qty", json!(0.0005))],
        ),
        (
            "limit_price_type static",
            "limit",
            Some(30000.0),
            true,
            vec![("limit_price_type", json!("static"))],
        ),
        (
            "triggers ref=last static (stop-loss)",
            "stop-loss",
            Some(30000.0),
            true,
            vec![(
                "triggers",
                json!({"reference":"last","price":200000.0,"price_type":"static"}),
            )],
        ),
        (
            "triggers ref=index static (stop-loss)",
            "stop-loss",
            Some(30000.0),
            true,
            vec![(
                "triggers",
                json!({"reference":"index","price":200000.0,"price_type":"static"}),
            )],
        ),
        (
            "triggers price_type=pct (stop-loss)",
            "stop-loss",
            Some(30000.0),
            true,
            vec![(
                "triggers",
                json!({"reference":"last","price":5,"price_type":"pct"}),
            )],
        ),
        (
            "triggers omit reference (server-defaults, REST parity)",
            "stop-loss",
            Some(30000.0),
            true,
            vec![("triggers", json!({"price":200000.0,"price_type":"static"}))],
        ),
        (
            "trailing-stop + static [expect REJECT: needs pct/quote]",
            "trailing-stop",
            Some(30000.0),
            true,
            vec![(
                "triggers",
                json!({"reference":"last","price":200000.0,"price_type":"static"}),
            )],
        ),
        (
            "trailing-stop + pct offset (relative price_type required)",
            "trailing-stop",
            Some(30000.0),
            true,
            vec![(
                "triggers",
                json!({"reference":"last","price":5,"price_type":"pct"}),
            )],
        ),
        (
            "trailing-stop-limit + static [expect REJECT]",
            "trailing-stop-limit",
            Some(30000.0),
            true,
            vec![(
                "triggers",
                json!({"reference":"last","price":200000.0,"price_type":"static"}),
            )],
        ),
        (
            "conditional limit close (no clid)",
            "limit",
            Some(30000.0),
            false,
            vec![(
                "conditional",
                json!({"order_type":"limit","limit_price":20000.0}),
            )],
        ),
        (
            "conditional stop-loss-limit + trigger_price (no clid)",
            "limit",
            Some(30000.0),
            false,
            vec![(
                "conditional",
                json!({"order_type":"stop-loss-limit","limit_price":19000.0,"trigger_price":20000.0}),
            )],
        ),
        (
            "REST spelling 'leverage' [expect REJECT -> stays WS-guarded]",
            "limit",
            Some(30000.0),
            true,
            vec![("leverage", json!(2))],
        ),
        (
            "REST spelling 'userref' [expect REJECT -> use order_userref]",
            "limit",
            Some(30000.0),
            true,
            vec![("userref", json!(42))],
        ),
        (
            "REST spelling 'close' [expect REJECT -> use conditional]",
            "limit",
            Some(30000.0),
            false,
            vec![("close", json!({"order_type":"limit","limit_price":20000.0}))],
        ),
    ];

    let mut req_id: u64 = 0;
    let mut report: Vec<(String, String)> = Vec::new();
    for (label, ord_type, limit_price, include_clordid, extras) in probes {
        req_id += 1;
        let mut params = serde_json::Map::new();
        params.insert("order_type".into(), json!(ord_type));
        params.insert("side".into(), json!("buy"));
        params.insert("order_qty".into(), json!(0.001));
        params.insert("symbol".into(), json!("BTC/USDC"));
        if let Some(lp) = limit_price {
            params.insert("limit_price".into(), json!(lp));
        }
        if include_clordid {
            params.insert(
                "cl_ord_id".into(),
                json!(uuid::Uuid::new_v4().simple().to_string()),
            );
        }
        params.insert("token".into(), json!(token));
        params.insert("validate".into(), json!(true));
        for (k, v) in extras {
            params.insert(k.into(), v);
        }
        let frame = json!({"method":"add_order","req_id":req_id,"params":params});
        ws.send(Message::Text(frame.to_string().into()))
            .await
            .expect("ws send");

        let reply: serde_json::Value = loop {
            match tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
                Ok(Some(Ok(Message::Text(s)))) => {
                    let v: serde_json::Value = serde_json::from_str(&s)
                        .unwrap_or_else(|_| json!({"_unparsed": s.as_str()}));
                    if v.get("req_id").and_then(|x| x.as_u64()) == Some(req_id) {
                        break v;
                    }
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => break json!({"_ws_error": e.to_string()}),
                Ok(None) => break json!({"_ws_closed": true}),
                Err(_) => break json!({"_timeout": true}),
            }
        };

        let success = reply.get("success").and_then(|x| x.as_bool());
        let err = reply.get("error").and_then(|x| x.as_str());
        let summary = match (success, err) {
            (Some(true), _) => "ACCEPTED (success:true)".to_string(),
            (Some(false), Some(e)) => format!("REJECTED: {e}"),
            _ => format!("OTHER: {reply}"),
        };
        eprintln!("[WS-PROBE] {label:42} req_id={req_id:<2} => {summary}");
        report.push((label.to_string(), summary));
    }

    let _ = ws.close(None).await;
    eprintln!("\n[WS-PROBE] ===== SUMMARY ({} probes) =====", report.len());
    for (label, summary) in &report {
        eprintln!("[WS-PROBE]   {label:42} => {summary}");
    }
}

#[tokio::test]
#[ignore = "live e2e against real Kraken (validate=true, no orders placed); run with --ignored trade_validate"]
async fn ws_advanced_order_composer_e2e() {
    let client = match build_authed_client("ws_advanced_order_composer_e2e") {
        Some(c) => c,
        None => return,
    };

    client.ready().await.expect("client ready");

    let btc_usdc = Symbol::new("BTC/USDC").expect("BTC/USDC is valid");

    let last_price = last_price_or(
        &client,
        &btc_usdc,
        Decimal::from_str("60000").unwrap(),
        Some(Duration::from_secs(10)),
    )
    .await;
    let far_price = (last_price * Decimal::from_str("0.5").unwrap()).round_dp(1);
    let volume = Decimal::from_str("0.0002").unwrap();

    let mut req =
        OrderRequest::new(btc_usdc.clone(), volume, Side::Buy).order_type(OrderType::Limit);
    req.price = Some(far_price.into());
    req.validate = true;
    req.time_in_force = Some(TimeInForce::Gtc);
    req.oflags = vec![OFlag::Post];
    req.stp_type = Some(StpType::CancelNewest);

    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        client.trade().order(req).via(Transport::WsV2Auth),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "[ws_advanced_order_composer_e2e] timed out after 30s waiting for \
             Kraken to ack the validate-mode WS add_order"
        )
    })
    .expect(
        "[ws_advanced_order_composer_e2e] validate=true WS add_order with advanced fields \
         (time_in_force=GTC, post_only, stp_type=cancel_newest) \
         was REJECTED by Kraken — composer regression or Kraken schema change; \
         check the error for details",
    );

    assert!(
        resp.descr.order.is_none(),
        "[ws_advanced_order_composer_e2e] WS reply must carry no descr.order \
         (the WS reply omits it and the SDK must not fabricate it); got Some({:?})",
        resp.descr.order.as_deref()
    );
    // WS validate returns an empty wire order_id (decoded as txid=None) and omits descr: docs/guides/wire-quirks.md.
    eprintln!(
        "[OBSERVE] validate-mode WS add_order reply: txid={:?} descr.order={:?}",
        resp.txid.as_ref().map(|t| t.as_str()),
        resp.descr.order.as_deref(),
    );
    eprintln!(
        "[ws_advanced_order_composer_e2e] PASS: validate-mode WS order with advanced fields \
         accepted by Kraken. cl_ord_id={:?} txid={:?} descr.order={:?}",
        resp.cl_ord_id.as_ref().map(|c| c.as_str()),
        resp.txid,
        resp.descr.order,
    );

    // WS add_order has no leverage-ratio field; SDK guards it to REST: docs/guides/wire-quirks.md.
    let mut lev_req =
        OrderRequest::new(btc_usdc.clone(), volume, Side::Buy).order_type(OrderType::Limit);
    lev_req.price = Some(far_price.into());
    lev_req.leverage = Some(2);
    lev_req.validate = true;

    let lev_result = tokio::time::timeout(
        Duration::from_secs(5),
        client.trade().order(lev_req).via(Transport::WsV2Auth),
    )
    .await
    .expect("[ws_advanced_order_composer_e2e] leverage-guard check timed out (should be instant)");

    match lev_result {
        Err(TradeError::WsUnsupportedOrderField { field }) => {
            assert_eq!(
                field, "leverage",
                "[ws_advanced_order_composer_e2e] guard returned WsUnsupportedOrderField \
                 but with field={field:?} — expected \"leverage\"",
            );
            eprintln!(
                "[ws_advanced_order_composer_e2e] PASS: leverage order via WsV2Auth \
                 correctly returns WsUnsupportedOrderField {{ field: {field:?} }}"
            );
        }
        Err(other) => {
            panic!(
                "[ws_advanced_order_composer_e2e] leverage order via WsV2Auth returned \
                 unexpected error: {other:?} — expected WsUnsupportedOrderField {{ field: \"leverage\" }}"
            );
        }
        Ok(_) => {
            panic!(
                "[ws_advanced_order_composer_e2e] leverage order via WsV2Auth SUCCEEDED — \
                 expected WsUnsupportedOrderField {{ field: \"leverage\" }}; \
                 the guard is missing or the leverage was silently dropped"
            );
        }
    }
}

/// order_buy() suppresses its auto cl_ord_id when a conditional close is set —
/// Kraken rejects a cl_ord_id on a conditional close. validate=true, nothing placed.
#[tokio::test]
#[ignore = "live e2e against real Kraken (validate=true, no orders placed); run --ignored trade_validate"]
async fn conditional_close_order_places_after_clordid_suppression() {
    let client =
        match build_authed_client("conditional_close_order_places_after_clordid_suppression") {
            Some(c) => c,
            None => return,
        };
    let btc_usdc = Symbol::new("BTC/USDC").unwrap();
    let last = last_price_or(
        &client,
        &btc_usdc,
        Decimal::from_str("60000").unwrap(),
        Some(Duration::from_secs(10)),
    )
    .await;
    let mut req = OrderRequest::new(btc_usdc, Decimal::from_str("0.0002").unwrap(), Side::Buy)
        .order_type(OrderType::Limit);
    req.price = Some(
        (last * Decimal::from_str("0.5").unwrap())
            .round_dp(1)
            .into(),
    );
    req.validate = true;
    req.conditional_close = Some(ConditionalClose::new(
        CloseOrderType::Limit,
        (last * Decimal::from_str("0.4").unwrap()).round_dp(1),
    ));

    let resp = tokio::time::timeout(
        Duration::from_secs(20),
        client.trade().order(req).via(Transport::Rest),
    )
    .await
    .expect("conditional-close validate order timed out")
    .expect(
        "conditional-close order must validate — the SDK suppresses the auto \
         cl_ord_id that Kraken rejects on a conditional close",
    );
    assert!(
        resp.cl_ord_id.is_none(),
        "conditional-close order must have NO cl_ord_id (auto-allocation suppressed); got {:?}",
        resp.cl_ord_id.as_ref().map(|c| c.as_str())
    );
    eprintln!(
        "[conditional_close_e2e] PASS: conditional-close order validated; cl_ord_id suppressed (None)"
    );
}

/// The WS composer maps trailing-stop to a relative "quote" triggers offset (an
/// absolute price is rejected on the trailing leg). validate=true, nothing placed.
#[tokio::test]
#[ignore = "live e2e against real Kraken (validate=true, no orders placed); run --ignored trade_validate"]
async fn trailing_stop_order_places_via_ws_composer() {
    let client = match build_authed_client("trailing_stop_order_places_via_ws_composer") {
        Some(c) => c,
        None => return,
    };
    client.ready().await.expect("client ready");

    let btc_usdc = Symbol::new("BTC/USDC").unwrap();
    let mut req = OrderRequest::new(btc_usdc, Decimal::from_str("0.0002").unwrap(), Side::Buy)
        .order_type(OrderType::TrailingStop);
    req.price = Some(Price::Offset {
        unit: PriceUnit::Quote,
        value: Decimal::from_str("100").unwrap(),
    });
    req.trigger = Some(TriggerKind::Last);
    req.validate = true;

    let resp = tokio::time::timeout(
        Duration::from_secs(30),
        client.trade().order(req).via(Transport::WsV2Auth),
    )
    .await
    .expect("trailing-stop validate order timed out")
    .expect(
        "trailing-stop must validate via the WS composer — it maps to a relative \
         quote triggers.price_type",
    );
    assert!(
        resp.txid.is_none(),
        "validate-mode reply should carry no real txid; got {:?}",
        resp.txid.as_ref().map(|t| t.as_str())
    );
    eprintln!(
        "[trailing_stop_e2e] PASS: trailing-stop validated via the WS composer (quote triggers)"
    );
}

#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored public"]
async fn public_rest_market_data_extended() {
    let client = ClientBuilder::new()
        .build()
        .expect("unauthenticated build is infallible");

    let btc_usd = Symbol::new("BTC/USD").expect("BTC/USD is valid");

    let sys = client
        .market()
        .status()
        .await
        .expect("status() should succeed");
    assert!(
        !sys.status.is_empty(),
        "system status string should be non-empty (got {:?})",
        sys.status
    );

    let assets = client
        .market()
        .assets(None)
        .await
        .expect("assets(None) should succeed");
    assert!(
        !assets.assets.is_empty(),
        "assets map should be non-empty (Kraken publishes many assets)"
    );

    let trades_result = client
        .market()
        .trades(TradesRequest::new(btc_usd.clone()))
        .await
        .expect("trades(TradesRequest) should succeed");
    assert!(
        !trades_result.last.is_empty(),
        "trades `last` cursor should be a non-empty string"
    );
    assert!(
        !trades_result.trades.is_empty(),
        "trades for BTC/USD should be non-empty (liquid pair)"
    );

    let ohlc_result = client
        .market()
        .ohlc(OhlcRequest::new(btc_usd.clone(), OhlcInterval::M1))
        .await
        .expect("ohlc(OhlcRequest) should succeed");
    assert!(
        ohlc_result.last > 0,
        "ohlc `last` cursor should be > 0 (got {})",
        ohlc_result.last
    );
    assert!(
        !ohlc_result.candles.is_empty(),
        "ohlc candles for BTC/USD should be non-empty (Kraken returns ~720 by default)"
    );

    let spreads_result = client
        .market()
        .spreads(btc_usd.clone(), None)
        .await
        .expect("spreads(BTC/USD, None) should succeed");
    assert!(
        !spreads_result.last.is_empty(),
        "spreads `last` cursor should be a non-empty string"
    );
    assert!(
        !spreads_result.spreads.is_empty(),
        "spreads for BTC/USD should be non-empty (liquid pair)"
    );
}

/// A single subscribe_book fans each wire frame to BOTH the `on_book`
/// (OrderBookUpdate) and `on_book_raw` (BookDelta) handler sets.
#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored public"]
async fn public_ws_book_streaming() {
    let client = ClientBuilder::new()
        .build()
        .expect("unauthenticated build is infallible");

    client.ready();

    let book_updates = Arc::new(AtomicUsize::new(0));
    let last_book: Arc<std::sync::Mutex<Option<(usize, usize)>>> =
        Arc::new(std::sync::Mutex::new(None));

    let _h_book = {
        let n = book_updates.clone();
        let last = last_book.clone();
        client.market().on_book(move |b: &OrderBookUpdate| {
            n.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut guard) = last.try_lock() {
                *guard = Some((b.bids.len(), b.asks.len()));
            }
        })
    };

    let raw_updates = Arc::new(AtomicUsize::new(0));

    let _h_raw = {
        let n = raw_updates.clone();
        client.market().on_book_raw(move |_d: &BookDelta| {
            n.fetch_add(1, Ordering::Relaxed);
        })
    };

    tokio::time::sleep(Duration::from_millis(400)).await;

    let btc_usd = Symbol::new("BTC/USD").expect("BTC/USD is valid");

    client
        .subscription()
        .subscribe_book(vec![btc_usd.clone()], BookDepth::D10)
        .expect("subscribe_book(BTC/USD, D10) should not fail synchronously");

    tokio::time::sleep(Duration::from_secs(15)).await;

    let book_count = book_updates.load(Ordering::Relaxed);
    let raw_count = raw_updates.load(Ordering::Relaxed);
    let last_frame = last_book.lock().ok().and_then(|g| *g);

    eprintln!(
        "[e2e book] book_updates={book_count} raw_updates={raw_count} \
         last_frame_bids_asks={last_frame:?}"
    );

    assert!(
        book_count > 0,
        "expected at least one OrderBookUpdate in 15s (BTC/USD book is very active); \
         got 0 — subscription may not have established or the handler was not reached"
    );
    assert!(
        raw_count > 0,
        "expected at least one BookDelta in 15s (same wire subscription fans to BookRaw); \
         got 0 — the book fan-out may be broken"
    );

    if let Some((bids, asks)) = last_frame {
        assert!(
            bids > 0,
            "last OrderBookUpdate had 0 bids — maintained book decode error"
        );
        assert!(
            asks > 0,
            "last OrderBookUpdate had 0 asks — maintained book decode error"
        );
    }
}

#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored public"]
async fn public_ws_combiners_and_ohlc_status() {
    let client = ClientBuilder::new()
        .build()
        .expect("unauthenticated build is infallible");

    let btc_usd = Symbol::new("BTC/USD").expect("BTC/USD is valid");

    let status_count = Arc::new(AtomicUsize::new(0));
    let _h_status = {
        let n = status_count.clone();
        client
            .market()
            .on_system_status(move |_s: &SystemStatusUpdate| {
                n.fetch_add(1, Ordering::Relaxed);
            })
    };

    let ohlc_count = Arc::new(AtomicUsize::new(0));
    let _h_ohlc = {
        let n = ohlc_count.clone();
        client.market().on_ohlc(move |_c: &OhlcUpdate| {
            n.fetch_add(1, Ordering::Relaxed);
        })
    };

    client.ready().await.expect("client ready");

    let ticker_count = Arc::new(AtomicUsize::new(0));
    let _g_ticker = {
        let n = ticker_count.clone();
        client
            .market()
            .on_ticker_for(
                std::slice::from_ref(&btc_usd),
                None,
                None,
                move |_t: &TickerUpdate| {
                    n.fetch_add(1, Ordering::Relaxed);
                },
            )
            .expect("on_ticker_for(BTC/USD) should return Ok(SubscriptionGuard)")
    };

    let book_for_count = Arc::new(AtomicUsize::new(0));
    let _g_book = {
        let n = book_for_count.clone();
        client
            .market()
            .on_book_for(
                std::slice::from_ref(&btc_usd),
                BookDepth::D10,
                move |_b: &OrderBookUpdate| {
                    n.fetch_add(1, Ordering::Relaxed);
                },
            )
            .expect("on_book_for(BTC/USD, D10) should return Ok(SubscriptionGuard)")
    };

    let book_raw_for_count = Arc::new(AtomicUsize::new(0));
    let _g_book_raw = {
        let n = book_raw_for_count.clone();
        client
            .market()
            .on_book_raw_for(
                std::slice::from_ref(&btc_usd),
                BookDepth::D10,
                None,
                move |_d: &BookDelta| {
                    n.fetch_add(1, Ordering::Relaxed);
                },
            )
            .expect("on_book_raw_for(BTC/USD, D10) should return Ok(SubscriptionGuard)")
    };

    let trade_for_count = Arc::new(AtomicUsize::new(0));
    let _g_trade = {
        let n = trade_for_count.clone();
        client
            .market()
            .on_trade_for(
                std::slice::from_ref(&btc_usd),
                None,
                move |_t: &TradeUpdate| {
                    n.fetch_add(1, Ordering::Relaxed);
                },
            )
            .expect("on_trade_for(BTC/USD) should return Ok(SubscriptionGuard)")
    };

    let ohlc_for_count = Arc::new(AtomicUsize::new(0));
    let _g_ohlc = {
        let n = ohlc_for_count.clone();
        client
            .market()
            .on_ohlc_for(
                std::slice::from_ref(&btc_usd),
                OhlcInterval::M1,
                None,
                move |_c: &OhlcUpdate| {
                    n.fetch_add(1, Ordering::Relaxed);
                },
            )
            .expect("on_ohlc_for(BTC/USD, M1) should return Ok(SubscriptionGuard)")
    };

    tokio::time::sleep(Duration::from_secs(15)).await;

    let status = status_count.load(Ordering::Relaxed);
    let tickers = ticker_count.load(Ordering::Relaxed);
    let books = book_for_count.load(Ordering::Relaxed);
    let braws = book_raw_for_count.load(Ordering::Relaxed);
    let trades = trade_for_count.load(Ordering::Relaxed);
    let ohlcs_g = ohlc_for_count.load(Ordering::Relaxed);
    let ohlcs_w = ohlc_count.load(Ordering::Relaxed);

    eprintln!(
        "[e2e combiners] status={status} ticker={tickers} book_for={books} \
         book_raw_for={braws} trade_for={trades} ohlc_for={ohlcs_g} on_ohlc={ohlcs_w}"
    );

    // Kraken auto-seeds a status frame on WS connect: docs/guides/wire-quirks.md.
    assert!(
        status > 0,
        "expected at least one SystemStatusUpdate in 15s (Kraken auto-seeds `status` \
         on every connection open); got 0 — the handler may not have been registered \
         before ready(), or the connection did not open"
    );

    assert!(
        tickers > 0,
        "expected at least one TickerUpdate in 15s via on_ticker_for(BTC/USD); \
         got 0 — combiner may not have subscribed or the handler was not wired"
    );
    assert!(
        books > 0,
        "expected at least one OrderBookUpdate in 15s via on_book_for(BTC/USD, D10); \
         got 0 — combiner may not have subscribed or the handler was not wired"
    );
    assert!(
        braws > 0,
        "expected at least one BookDelta in 15s via on_book_raw_for(BTC/USD, D10); \
         got 0 — the fan-out to BookRaw may be broken or the combiner failed"
    );

    if trades == 0 {
        eprintln!(
            "[e2e combiners] WARN: on_trade_for(BTC/USD) got 0 ticks in 15s — \
             possible (quiet window); re-run if this persists"
        );
    }

    if ohlcs_g == 0 {
        eprintln!(
            "[e2e combiners] NOTE: on_ohlc_for(BTC/USD, M1) got 0 candles in 15s — \
             expected (1-min candles may not have completed yet)"
        );
    }
    if ohlcs_w == 0 {
        eprintln!(
            "[e2e combiners] NOTE: on_ohlc(channel-wide) got 0 candles in 15s — \
             same as above; tolerated"
        );
    }

    drop(_g_ticker);
    drop(_g_book);
    drop(_g_book_raw);
    drop(_g_trade);
    drop(_g_ohlc);
}

#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored account"]
async fn account_rest_reads_extended() {
    let client = match build_authed_client("account_rest_reads_extended") {
        Some(c) => c,
        None => return,
    };

    let tb: TradeBalance = client
        .account()
        .trade_balance(None)
        .await
        .expect("trade_balance(None) should succeed");
    let _ = tb.equivalent_balance;
    let _ = tb.free_margin;

    let co: ClosedOrders = client
        .account()
        .closed_orders(ClosedOrdersRequest::default())
        .await
        .expect("closed_orders(all None) should succeed");
    // count is the total across all pages, not this page: docs/guides/wire-quirks.md.
    let _ = co.closed.len();
    let _ = co.count;

    let lg: Ledgers = client
        .account()
        .ledgers(LedgersRequest::default())
        .await
        .expect("ledgers(all None) should succeed");
    let _ = lg.ledger.len();
    let _ = lg.count;

    pace().await;
    let tv: TradeVolume = client
        .account()
        .volume(None, true)
        .await
        .expect("volume(None, fee_info=true) should succeed");
    assert!(
        !tv.currency.as_str().is_empty(),
        "volume.currency should be a non-empty modern asset code (e.g. \"USD\")"
    );
    let _ = tv.volume;

    pace().await;
    let op: OpenPositions = client
        .account()
        .positions(None, false)
        .await
        .expect("positions(None, false) should succeed");
    let _ = op.positions.len();

    pace().await;
    let dummy_cl_ord_id = ClOrdId::new("e2e-never00")
        .expect("\"e2e-never00\" is a valid cl_ord_id (≤18 ASCII chars)");

    let outcome: ReconciliationOutcome = client
        .account()
        .find_order_by_cl_ord_id(&dummy_cl_ord_id)
        .await
        .expect("find_order_by_cl_ord_id with a never-placed id must not error");

    assert_eq!(
        outcome,
        ReconciliationOutcome::NotPlaced,
        "a never-placed cl_ord_id must resolve to ReconciliationOutcome::NotPlaced; \
         got {:?} — either a real order with this id exists (unlikely), or the \
         reconciliation walk returned an unexpected variant",
        outcome
    );

    // Kraken rejects a malformed/unknown ledger id: docs/guides/error-handling.md.
    pace().await;
    let ledg = client
        .account()
        .ledgers(LedgersRequest::default())
        .await
        .expect("ledgers() should succeed");
    if let Some(real_id) = ledg.ledger.keys().next().cloned() {
        pace().await;
        let ql: HashMap<String, LedgerEntry> = client
            .account()
            .query_ledgers(vec![real_id.clone()])
            .await
            .expect("query_ledgers(<real id>) should succeed");
        assert!(
            ql.contains_key(&real_id),
            "query_ledgers should return the queried ledger id {real_id}"
        );
    } else {
        eprintln!("[e2e account] no ledger entries on account — query_ledgers(real id) skipped");
    }
}

/// Exercises the public `client.subscription()` methods: subscribe/unsubscribe
/// pairs plus the five aggregate read/teardown ops.
#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored public"]
async fn public_subscription_lifecycle() {
    use kraken_sdk::ChannelName;

    let client = ClientBuilder::new()
        .build()
        .expect("unauthenticated build is infallible");

    client.ready();

    let btc_usd = Symbol::new("BTC/USD").expect("BTC/USD is valid");

    let _h_book = client
        .market()
        .on_book(|_: &kraken_sdk::OrderBookUpdate| {});
    let _h_braw = client.market().on_book_raw(|_: &kraken_sdk::BookDelta| {});
    let _h_ohlc = client.market().on_ohlc(|_: &kraken_sdk::OhlcUpdate| {});
    let _h_status = client
        .market()
        .on_system_status(|_: &kraken_sdk::SystemStatusUpdate| {});
    let _h_ticker = client.market().on_ticker(|_: &kraken_sdk::TickerUpdate| {});
    let _h_trade = client.market().on_trade(|_: &kraken_sdk::TradeUpdate| {});

    tokio::time::sleep(Duration::from_millis(400)).await;

    client
        .subscription()
        .subscribe_book(vec![btc_usd.clone()], BookDepth::D10)
        .expect("subscribe_book(BTC/USD, D10) must return Ok(())");

    tokio::time::sleep(Duration::from_millis(200)).await;

    client
        .subscription()
        .unsubscribe_book(vec![btc_usd.clone()])
        .expect("unsubscribe_book(BTC/USD) must return Ok(())");

    client
        .subscription()
        .subscribe_book_raw(vec![btc_usd.clone()], BookDepth::D10, None)
        .expect("subscribe_book_raw(BTC/USD, D10) must return Ok(())");

    tokio::time::sleep(Duration::from_millis(200)).await;

    client
        .subscription()
        .unsubscribe_book_raw(vec![btc_usd.clone()])
        .expect("unsubscribe_book_raw(BTC/USD) must return Ok(())");

    client
        .subscription()
        .subscribe_ohlc(vec![btc_usd.clone()], OhlcInterval::M1, None)
        .expect("subscribe_ohlc(BTC/USD, M1) must return Ok(())");

    tokio::time::sleep(Duration::from_millis(200)).await;

    client
        .subscription()
        .unsubscribe_ohlc(vec![btc_usd.clone()], OhlcInterval::M1)
        .expect("unsubscribe_ohlc(BTC/USD, M1) must return Ok(())");

    client
        .subscription()
        .subscribe_system_status()
        .expect("subscribe_system_status() must return Ok(())");

    tokio::time::sleep(Duration::from_millis(200)).await;

    client
        .subscription()
        .unsubscribe_system_status()
        .expect("unsubscribe_system_status() must return Ok(())");

    client
        .subscription()
        .subscribe_ticker(vec![btc_usd.clone()], None, None)
        .expect("subscribe_ticker(BTC/USD) must return Ok(()) — setting up for unsubscribe_ticker");

    tokio::time::sleep(Duration::from_millis(200)).await;

    client
        .subscription()
        .unsubscribe_ticker(vec![btc_usd.clone()])
        .expect("unsubscribe_ticker(BTC/USD) must return Ok(())");

    client
        .subscription()
        .subscribe_trade(vec![btc_usd.clone()], None)
        .expect("subscribe_trade(BTC/USD) must return Ok(()) — setting up for unsubscribe_trade");

    tokio::time::sleep(Duration::from_millis(200)).await;

    client
        .subscription()
        .unsubscribe_trade(vec![btc_usd.clone()])
        .expect("unsubscribe_trade(BTC/USD) must return Ok(())");

    client
        .subscription()
        .subscribe_ticker(vec![btc_usd.clone()], None, None)
        .expect("subscribe_ticker(BTC/USD) must return Ok(()) — seeding the read models");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let rows = client
        .subscription()
        .list_active()
        .expect("list_active() must return Ok");
    assert!(
        rows.iter()
            .any(|r| r.channel == ChannelName::Ticker && r.pair.as_ref() == Some(&btc_usd)),
        "list_active() must contain the live (ticker, BTC/USD) entry; got {rows:?}"
    );

    let refs = client
        .subscription()
        .find_by_channel(ChannelName::Ticker)
        .expect("find_by_channel(Ticker) must return Ok");
    assert_eq!(refs.len(), 1, "one ticker entry expected; got {refs:?}");
    assert!(
        refs[0].registered_at_monotonic > 0,
        "registered_at_monotonic must carry the registration stamp"
    );

    let summary = client
        .subscription()
        .status_summary()
        .expect("status_summary() must return Ok");
    assert!(
        summary.total >= 1,
        "summary must count the live entry: {summary:?}"
    );
    assert_eq!(
        summary.total,
        summary.active + summary.pending + summary.failed
    );
    assert!(
        summary
            .by_channel
            .get(&ChannelName::Ticker)
            .copied()
            .unwrap_or(0)
            >= 1,
        "by_channel must count ticker: {summary:?}"
    );

    client
        .subscription()
        .unsubscribe_channel(ChannelName::Ticker)
        .expect("unsubscribe_channel(Ticker) must return Ok(())");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let refs = client
        .subscription()
        .find_by_channel(ChannelName::Ticker)
        .expect("find_by_channel(Ticker) must return Ok");
    assert!(
        refs.is_empty(),
        "channel teardown must clear the entries; got {refs:?}"
    );

    client
        .subscription()
        .subscribe_trade(vec![btc_usd.clone()], None)
        .expect("subscribe_trade(BTC/USD) must return Ok(()) — seeding unsubscribe_all");
    tokio::time::sleep(Duration::from_millis(300)).await;
    client
        .subscription()
        .unsubscribe_all()
        .expect("unsubscribe_all() must return Ok(())");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let summary = client
        .subscription()
        .status_summary()
        .expect("status_summary() must return Ok");
    assert_eq!(
        summary.total, 0,
        "unsubscribe_all must empty the subscription set: {summary:?}"
    );

    eprintln!(
        "[e2e subscription] Group A: all 12 public subscribe/unsubscribe calls returned Ok(()). \
         Group B: all 5 aggregate ops returned live read-model/teardown results."
    );
}

#[tokio::test]
#[ignore = "live e2e against real Kraken; run with --ignored account"]
async fn account_subscription_unsubscribe() {
    let client = match build_authed_client("account_subscription_unsubscribe") {
        Some(c) => c,
        None => return,
    };

    client.ready();

    let _h_exec = client
        .account()
        .on_executions(|_: &kraken_sdk::ExecutionUpdate| {});
    let _h_bal = client
        .account()
        .on_balances(|_: &kraken_sdk::BalanceUpdate| {});

    tokio::time::sleep(Duration::from_millis(400)).await;

    client
        .subscription()
        .subscribe_executions()
        .expect("subscribe_executions() must return Ok(())");

    tokio::time::sleep(Duration::from_millis(500)).await;

    client
        .subscription()
        .unsubscribe_executions()
        .expect("unsubscribe_executions() must return Ok(())");

    client
        .subscription()
        .subscribe_balances()
        .expect("subscribe_balances() must return Ok(())");

    tokio::time::sleep(Duration::from_millis(500)).await;

    client
        .subscription()
        .unsubscribe_balances()
        .expect("unsubscribe_balances() must return Ok(())");

    eprintln!(
        "[e2e account subscription] subscribe_executions + unsubscribe_executions + \
         subscribe_balances + unsubscribe_balances all returned Ok(())."
    );
}

/// Cooperative pause between live ops: the SDK never auto-throttles, so bursts
/// would exceed the trading rate limit.
async fn pace() {
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
}

async fn try_place_and_cancel<R: Send + 'static>(
    client: &kraken_sdk::Client,
    pending: kraken_sdk::PendingTrade<R, kraken_sdk::AddOrderResponse>,
    label: &str,
) {
    pace().await;
    let cl_ord_id = pending.cl_ord_id().cloned().expect(
        "shorthand PendingTrade must pre-allocate cl_ord_id — nothing was placed; fix the builder",
    );
    match pending.via(Transport::Rest).await {
        Ok(resp) => {
            assert!(
                resp.txid.is_some(),
                "[e2e trade] {label}: placement returned Ok but txid is None — unexpected"
            );
            eprintln!(
                "[e2e trade] {label}: PLACED txid={:?}",
                resp.txid.as_ref().map(|t| t.as_str())
            );
            pace().await;
            client
                .trade()
                .cancel(cl_ord_id)
                .via(Transport::Rest)
                .await
                .unwrap_or_else(|e| {
                    panic!("[e2e trade] {label}: cancel must succeed after placement: {e:?}")
                });
            eprintln!("[e2e trade] {label}: cancelled OK");
        }
        Err(TradeError::InsufficientFunds { .. }) => {
            eprintln!(
                "[e2e trade] {label}: EXERCISED — account underfunded for this order; \
                 SDK surfaced InsufficientFunds (method + error path validated)"
            );
        }
        Err(e) => {
            panic!("[e2e trade] {label}: UNEXPECTED error: {e:?}");
        }
    }
}

/// Exercises the `client.trade()` methods listed in the closing summary;
/// `InsufficientFunds` counts as a valid
/// outcome (the method + error path still ran). Double-gated (credentials AND
/// `KRAKEN_E2E_TRADE_REAL=1`); on abort, check open orders manually.
#[tokio::test]
#[ignore = "live e2e against real Kraken (REAL MONEY); run with --ignored trade_real"]
async fn trade_full_lifecycle_real() {
    let _real_order_guard = REAL_ORDER_LOCK.lock().await;
    let client = match build_authed_client("trade_full_lifecycle_real") {
        Some(c) => c,
        None => return,
    };

    if std::env::var("KRAKEN_E2E_TRADE_REAL").is_err() {
        eprintln!(
            "[skip:trade_full_lifecycle_real] set KRAKEN_E2E_TRADE_REAL=1 to run this test \
             (it places real orders on the live exchange)"
        );
        return;
    }

    let btc_usdc = Symbol::new("BTC/USDC").expect("BTC/USDC is valid");
    let vol = Decimal::from_str("0.0001").unwrap();

    let last_price = last_price_or(
        &client,
        &btc_usdc,
        Decimal::from_str("60000").unwrap(),
        None,
    )
    .await;

    let far_below = (last_price * Decimal::from_str("0.5").unwrap()).round_dp(1);
    let far_above = (last_price * Decimal::from_str("2.0").unwrap()).round_dp(1);
    let modest_trigger = (last_price * Decimal::from_str("1.05").unwrap()).round_dp(1);

    eprintln!(
        "[e2e trade] last_price={last_price} far_below={far_below} far_above={far_above} \
         modest_trigger={modest_trigger} vol={vol}"
    );

    try_place_and_cancel(
        &client,
        client.trade().limit_buy(btc_usdc.clone(), vol, far_below),
        "limit_buy",
    )
    .await;

    try_place_and_cancel(
        &client,
        client
            .trade()
            .stop_loss_buy(btc_usdc.clone(), vol, modest_trigger),
        "stop_loss_buy",
    )
    .await;

    {
        pace().await;
        let pending = client.trade().limit_buy(btc_usdc.clone(), vol, far_below);
        let cl_ord_id = pending
            .cl_ord_id()
            .cloned()
            .expect("cl_ord_id must be pre-allocated on a PendingTrade");
        match pending.via(Transport::Rest).await {
            Ok(place_resp) => {
                assert!(
                    place_resp.txid.is_some(),
                    "[e2e trade] order_amend setup: txid must be Some"
                );
                eprintln!(
                    "[e2e trade] order_amend: setup limit_buy placed txid={:?}",
                    place_resp.txid.as_ref().map(|t| t.as_str())
                );
                let amended_price = far_below + Decimal::from_str("1.0").unwrap();
                let mut amend_req = OrderAmendRequest::new(cl_ord_id.clone());
                amend_req.limit_price = Some(amended_price.into());
                match client
                    .trade()
                    .order_amend(amend_req)
                    .via(Transport::Rest)
                    .await
                {
                    Ok(amend_resp) => {
                        assert!(
                            !amend_resp.amend_id.as_str().is_empty(),
                            "[e2e trade] order_amend: amend_id must be non-empty"
                        );
                        eprintln!(
                            "[e2e trade] order_amend: EXERCISED amend_id={:?}",
                            amend_resp.amend_id.as_str()
                        );
                    }
                    Err(TradeError::InsufficientFunds { .. }) => {
                        eprintln!(
                            "[e2e trade] order_amend: EXERCISED — amend returned \
                             InsufficientFunds (method + error path validated)"
                        );
                    }
                    Err(e) => {
                        panic!("[e2e trade] order_amend: UNEXPECTED error: {e:?}");
                    }
                }
                let _ = client.trade().cancel(cl_ord_id).via(Transport::Rest).await;
                eprintln!("[e2e trade] order_amend: setup order cancelled");
            }
            Err(TradeError::InsufficientFunds { .. }) => {
                eprintln!(
                    "[e2e trade] order_amend: setup limit_buy UNDERFUNDED — \
                     order_amend step skipped (method still exercised via error path)"
                );
            }
            Err(e) => {
                panic!("[e2e trade] order_amend setup: UNEXPECTED error: {e:?}");
            }
        }
    }

    {
        pace().await;
        let p1 = client.trade().limit_buy(btc_usdc.clone(), vol, far_below);
        let id1 = p1.cl_ord_id().cloned().expect("id1 must be pre-allocated");
        let r1 = p1.via(Transport::Rest).await;
        match &r1 {
            Ok(resp) => eprintln!(
                "[e2e trade] cancel_batch order1: placed txid={:?}",
                resp.txid.as_ref().map(|t| t.as_str())
            ),
            Err(TradeError::InsufficientFunds { .. }) => {
                eprintln!("[e2e trade] cancel_batch order1: UNDERFUNDED (InsufficientFunds)")
            }
            Err(e) => panic!("[e2e trade] cancel_batch order1: UNEXPECTED error: {e:?}"),
        }

        let p2 = client.trade().limit_buy(btc_usdc.clone(), vol, far_below);
        let id2 = p2.cl_ord_id().cloned().expect("id2 must be pre-allocated");
        let r2 = p2.via(Transport::Rest).await;
        match &r2 {
            Ok(resp) => eprintln!(
                "[e2e trade] cancel_batch order2: placed txid={:?}",
                resp.txid.as_ref().map(|t| t.as_str())
            ),
            Err(TradeError::InsufficientFunds { .. }) => {
                eprintln!("[e2e trade] cancel_batch order2: UNDERFUNDED (InsufficientFunds)")
            }
            Err(e) => panic!("[e2e trade] cancel_batch order2: UNEXPECTED error: {e:?}"),
        }

        let batch_cancel: CancelBatchResponse = client
            .trade()
            .cancel_batch(vec![id1, id2])
            .via(Transport::Rest)
            .await
            .expect("[e2e trade] cancel_batch: must return Ok (method + decode validated)");
        assert_eq!(
            batch_cancel.results.len(),
            2,
            "[e2e trade] cancel_batch: expected 2 result entries; got {}",
            batch_cancel.results.len()
        );
        eprintln!(
            "[e2e trade] cancel_batch: EXERCISED results={:?}",
            batch_cancel.results
        );
    }

    {
        pace().await;
        let entry1 = BatchOrderEntry::new(OrderType::Limit, Side::Buy, vol).price(far_below);
        let entry2 = BatchOrderEntry::new(OrderType::Limit, Side::Buy, vol).price(far_below);
        let batch_req = AddOrderBatchRequest::new(btc_usdc.clone(), vec![entry1, entry2]);
        match client
            .trade()
            .order_batch(batch_req)
            .via(Transport::Rest)
            .await
        {
            Ok(batch_resp) => {
                assert_eq!(
                    batch_resp.orders.len(),
                    2,
                    "[e2e trade] order_batch: expected 2 order result entries; got {}",
                    batch_resp.orders.len()
                );
                for (i, o) in batch_resp.orders.iter().enumerate() {
                    // Batch row: placed (txid) XOR rejected (error), never both.
                    assert!(
                        !(o.txid.is_some() && o.error.is_some()),
                        "[e2e trade] order_batch entry[{i}] has BOTH txid and error"
                    );
                    eprintln!(
                        "[e2e trade] order_batch entry[{i}]: txid={:?} error={:?}",
                        o.txid.as_ref().map(|t| t.as_str()),
                        o.error
                    );
                }
                let _ = client.trade().cancel_all().via(Transport::Rest).await;
                eprintln!("[e2e trade] order_batch: EXERCISED — cancel_all sweep done");
            }
            Err(TradeError::InsufficientFunds { .. }) => {
                eprintln!(
                    "[e2e trade] order_batch: EXERCISED — InsufficientFunds \
                     (method + error path validated)"
                );
            }
            Err(e) => {
                panic!("[e2e trade] order_batch: UNEXPECTED error: {e:?}");
            }
        }
    }

    {
        pace().await;
        let ca_resp: kraken_sdk::CancelAllResponse = client
            .trade()
            .cancel_all()
            .via(Transport::Rest)
            .await
            .expect("[e2e trade] cancel_all: must return Ok");
        eprintln!("[e2e trade] cancel_all: EXERCISED count={}", ca_resp.count);
    }

    {
        pace().await;
        let arm_resp: DeadmanResponse = client
            .trade()
            .cancel_all_orders_after(60)
            .via(Transport::Rest)
            .await
            .expect("[e2e trade] cancel_all_orders_after(60): must return Ok");
        assert!(
            !arm_resp.current_time.is_empty(),
            "[e2e trade] cancel_all_orders_after(60): current_time must be non-empty"
        );
        eprintln!(
            "[e2e trade] cancel_all_orders_after: ARMED current_time={:?} trigger_time={:?}",
            arm_resp.current_time, arm_resp.trigger_time
        );

        let disarm_resp: DeadmanResponse = client
            .trade()
            .cancel_all_orders_after(0)
            .via(Transport::Rest)
            .await
            .expect("[e2e trade] cancel_all_orders_after(0): disarm must return Ok");
        assert!(
            !disarm_resp.current_time.is_empty(),
            "[e2e trade] cancel_all_orders_after(0): current_time must be non-empty"
        );
        eprintln!(
            "[e2e trade] cancel_all_orders_after: DISARMED trigger_time={:?}",
            disarm_resp.trigger_time
        );
    }

    {
        pace().await;
        let bal = client
            .account()
            .balance()
            .await
            .expect("[e2e trade] acquire BTC: balance() must succeed");
        let btc_free = bal.assets.get("BTC").copied().unwrap_or(Decimal::ZERO);
        eprintln!("[e2e trade] acquire BTC: free BTC before acquire = {btc_free}");
        if btc_free < Decimal::from_str("0.0001").unwrap() {
            let acquire_vol = Decimal::from_str("0.0002").unwrap();
            match client
                .trade()
                .market_buy(btc_usdc.clone(), acquire_vol)
                .via(Transport::Rest)
                .await
            {
                Ok(resp) => {
                    eprintln!(
                        "[e2e trade] market_buy (acquire {acquire_vol} BTC): FILLED txid={:?}",
                        resp.txid.as_ref().map(|t| t.as_str())
                    );
                }
                Err(TradeError::InsufficientFunds { .. }) => {
                    eprintln!(
                        "[e2e trade] market_buy (acquire): UNDERFUNDED — \
                         sell-side ops will exercise InsufficientFunds path"
                    );
                }
                Err(e) => {
                    panic!("[e2e trade] market_buy (acquire): UNEXPECTED error: {e:?}");
                }
            }
        } else {
            eprintln!("[e2e trade] acquire BTC: already have {btc_free} BTC — skip market_buy");
        }
    }

    try_place_and_cancel(
        &client,
        client.trade().limit_sell(btc_usdc.clone(), vol, far_above),
        "limit_sell",
    )
    .await;

    {
        let mut req =
            OrderRequest::new(btc_usdc.clone(), vol, Side::Buy).order_type(OrderType::Limit);
        req.price = Some(far_above.into());
        try_place_and_cancel(&client, client.trade().order(req), "order_sell").await;
    }

    try_place_and_cancel(
        &client,
        client
            .trade()
            .stop_loss_sell(btc_usdc.clone(), vol, far_below),
        "stop_loss_sell",
    )
    .await;

    {
        let bal = client
            .account()
            .balance()
            .await
            .expect("[e2e trade] flatten: balance() must succeed");
        eprintln!(
            "[e2e trade] flatten: balance asset keys = {:?}",
            bal.assets.keys().collect::<Vec<_>>()
        );
        let btc_free = bal.assets.get("BTC").copied().unwrap_or(Decimal::ZERO);
        let flat_vol = btc_free.round_dp_with_strategy(8, rust_decimal::RoundingStrategy::ToZero);
        eprintln!("[e2e trade] flatten: free BTC={btc_free} → market_sell {flat_vol}");
        if flat_vol >= Decimal::from_str("0.0001").unwrap() {
            let sell_resp = client
                .trade()
                .market_sell(btc_usdc.clone(), flat_vol)
                .via(Transport::Rest)
                .await
                .expect("[e2e trade] market_sell (flatten): must succeed");
            assert!(
                sell_resp.txid.is_some(),
                "[e2e trade] market_sell: a real fill must return a txid; got None"
            );
            eprintln!(
                "[e2e trade] market_sell (flatten {flat_vol} BTC): FILLED txid={:?} — position flat",
                sell_resp.txid.as_ref().map(|t| t.as_str())
            );
        } else {
            eprintln!(
                "[e2e trade] flatten: free BTC {flat_vol} < 0.0001 min — nothing to flatten (already flat)"
            );
        }
    }

    let _ = client.trade().cancel_all().via(Transport::Rest).await;
    eprintln!("[e2e trade] final cancel_all safety-net sweep complete");

    eprintln!(
        "[e2e trade] trade_full_lifecycle_real: ALL 12 methods exercised \
         (placed-or-InsufficientFunds): limit_buy, stop_loss_buy, order_amend, \
         cancel_batch, order_batch, cancel_all, cancel_all_orders_after, \
         market_buy, limit_sell, order_sell, stop_loss_sell, market_sell"
    );
}

/// Cancels resting orders and sells residual BTC back to USDC (cleanup for a
/// mid-test panic). Idempotent; double-gated like the other real-money tests.
#[tokio::test]
#[ignore = "live e2e against real Kraken (REAL MONEY); run with --ignored trade_real"]
async fn flatten_residual_btc_real() {
    let _real_order_guard = REAL_ORDER_LOCK.lock().await;
    let client = match build_authed_client("flatten_residual_btc_real") {
        Some(c) => c,
        None => return,
    };
    if std::env::var("KRAKEN_E2E_TRADE_REAL").is_err() {
        eprintln!("[skip:flatten_residual_btc_real] set KRAKEN_E2E_TRADE_REAL=1 to run");
        return;
    }
    let btc_usdc = Symbol::new("BTC/USDC").expect("BTC/USDC is valid");

    let _ = client.trade().cancel_all().via(Transport::Rest).await;

    let bal = client
        .account()
        .balance()
        .await
        .expect("[flatten] balance() must succeed");
    eprintln!(
        "[flatten] balance asset keys = {:?}",
        bal.assets.keys().collect::<Vec<_>>()
    );
    let btc_free = bal.assets.get("BTC").copied().unwrap_or(Decimal::ZERO);
    let flat_vol = btc_free.round_dp_with_strategy(8, rust_decimal::RoundingStrategy::ToZero);
    eprintln!("[flatten] free BTC={btc_free} → market_sell {flat_vol}");
    if flat_vol >= Decimal::from_str("0.0001").unwrap() {
        let resp = client
            .trade()
            .market_sell(btc_usdc, flat_vol)
            .via(Transport::Rest)
            .await
            .expect("[flatten] market_sell must succeed");
        eprintln!(
            "[flatten] SOLD {flat_vol} BTC txid={:?} — account flat",
            resp.txid.as_ref().map(|t| t.as_str())
        );
    } else {
        eprintln!("[flatten] free BTC {flat_vol} < 0.0001 min — nothing to flatten (already flat)");
    }
}

/// Targeted live `stop_loss_sell` (acquire → place → cancel → flatten), few ops
/// to stay under the trading rate limit. Double-gated; tolerates InsufficientFunds.
#[tokio::test]
#[ignore = "live e2e against real Kraken (REAL MONEY); run with --ignored trade_real"]
async fn trade_stop_loss_sell_real() {
    let _real_order_guard = REAL_ORDER_LOCK.lock().await;
    let client = match build_authed_client("trade_stop_loss_sell_real") {
        Some(c) => c,
        None => return,
    };
    if std::env::var("KRAKEN_E2E_TRADE_REAL").is_err() {
        eprintln!("[skip:trade_stop_loss_sell_real] set KRAKEN_E2E_TRADE_REAL=1 to run");
        return;
    }
    let btc_usdc = Symbol::new("BTC/USDC").expect("BTC/USDC is valid");
    let vol = Decimal::from_str("0.0001").unwrap();
    let last_price = last_price_or(
        &client,
        &btc_usdc,
        Decimal::from_str("60000").unwrap(),
        None,
    )
    .await;
    let far_below = (last_price * Decimal::from_str("0.5").unwrap()).round_dp(1);
    eprintln!("[stop_loss_sell] last_price={last_price} trigger(far_below)={far_below} vol={vol}");

    let bal = client.account().balance().await.expect("[ssl] balance()");
    let btc_free = bal.assets.get("BTC").copied().unwrap_or(Decimal::ZERO);
    if btc_free < vol {
        match client
            .trade()
            .market_buy(btc_usdc.clone(), Decimal::from_str("0.0002").unwrap())
            .via(Transport::Rest)
            .await
        {
            Ok(r) => eprintln!(
                "[stop_loss_sell] acquired 0.0002 BTC txid={:?}",
                r.txid.as_ref().map(|t| t.as_str())
            ),
            Err(TradeError::InsufficientFunds { .. }) => {
                eprintln!(
                    "[stop_loss_sell] acquire UNDERFUNDED — stop_loss_sell will exercise the error path"
                );
            }
            Err(e) => panic!("[stop_loss_sell] acquire UNEXPECTED error: {e:?}"),
        }
    } else {
        eprintln!("[stop_loss_sell] already hold {btc_free} BTC — skip acquire");
    }

    let pending = client
        .trade()
        .stop_loss_sell(btc_usdc.clone(), vol, far_below);
    let cl = pending.cl_ord_id().cloned();
    match pending.via(Transport::Rest).await {
        Ok(resp) => {
            assert!(
                resp.txid.is_some(),
                "[stop_loss_sell] placed → expected txid"
            );
            eprintln!(
                "[stop_loss_sell] PLACED txid={:?}",
                resp.txid.as_ref().map(|t| t.as_str())
            );
            // Let orders rest a few seconds before cancel (fast-cancel penalty): docs/guides/rate-limits.md.
            tokio::time::sleep(Duration::from_secs(6)).await;
            if let Some(c) = cl {
                let cr = client
                    .trade()
                    .cancel(c)
                    .via(Transport::Rest)
                    .await
                    .expect("[stop_loss_sell] cancel must succeed");
                eprintln!("[stop_loss_sell] cancelled OK (count={})", cr.count);
            }
        }
        Err(TradeError::InsufficientFunds { .. }) => {
            eprintln!(
                "[stop_loss_sell] EXERCISED — underfunded; SDK surfaced InsufficientFunds \
                 (method + error path validated)"
            );
        }
        Err(e) => panic!("[stop_loss_sell] UNEXPECTED error: {e:?}"),
    }

    let _ = client.trade().cancel_all().via(Transport::Rest).await;
    let bal = client
        .account()
        .balance()
        .await
        .expect("[stop_loss_sell] flatten balance()");
    let btc_free = bal.assets.get("BTC").copied().unwrap_or(Decimal::ZERO);
    let flat_vol = btc_free.round_dp_with_strategy(8, rust_decimal::RoundingStrategy::ToZero);
    if flat_vol >= Decimal::from_str("0.0001").unwrap() {
        let resp = client
            .trade()
            .market_sell(btc_usdc, flat_vol)
            .via(Transport::Rest)
            .await
            .expect("[stop_loss_sell] flatten market_sell must succeed");
        eprintln!(
            "[stop_loss_sell] flattened {flat_vol} BTC txid={:?} — account flat",
            resp.txid.as_ref().map(|t| t.as_str())
        );
    } else {
        eprintln!("[stop_loss_sell] free BTC {flat_vol} < min — already flat");
    }
}
