//! Unit tests for the market namespace.

use std::sync::Arc;

use rust_decimal::Decimal;

use crate::book::{BookDelta, OrderBookUpdate};
use crate::rest::RestSurface;
use crate::types::{BookDepth, ChannelName, Symbol};

use super::ws::{
    decode_book, decode_book_raw, decode_ohlc, decode_system_status, decode_ticker, decode_trade,
    filter_by_pairs,
};
use super::ws_types::{RawTickerUpdate, parse_wire_decimal};
use super::{
    MarketError, MarketNamespace, OhlcInterval, OhlcRequest, OhlcUpdate, SystemStatusUpdate,
    TickerUpdate, TradeSide, TradeUpdate, TradesRequest,
};

use crate::auth::{AuthStack, SystemClockNonceSource};
use crate::transport::{HttpTransport, TransportError};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex;

/// In-process mock transport: records the last GET and returns a canned JSON response.
struct MockHttp {
    canned: serde_json::Value,
    #[allow(clippy::type_complexity)]
    last_request: Mutex<Option<(String, Vec<(String, String)>)>>,
}

#[async_trait::async_trait]
impl HttpTransport for MockHttp {
    async fn get_json(
        &self,
        path: &str,
        query_params: &[(&str, &str)],
    ) -> Result<serde_json::Value, TransportError> {
        *self.last_request.lock().unwrap() = Some((
            path.into(),
            query_params
                .iter()
                .map(|(k, v)| ((*k).into(), (*v).into()))
                .collect(),
        ));
        Ok(self.canned.clone())
    }

    async fn post_form_signed(
        &self,
        _path: &str,
        _body: &str,
        _api_key_header: &str,
        _api_sign_header: &str,
    ) -> Result<serde_json::Value, TransportError> {
        panic!("market-namespace tests never hit signed POST");
    }
}

/// Path-routing mock: canned response per endpoint path; counts hits.
struct RoutingMock {
    responses: HashMap<String, serde_json::Value>,
    calls: Mutex<HashMap<String, usize>>,
}

impl RoutingMock {
    fn count(&self, path: &str) -> usize {
        self.calls.lock().unwrap().get(path).copied().unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl HttpTransport for RoutingMock {
    async fn get_json(
        &self,
        path: &str,
        _query_params: &[(&str, &str)],
    ) -> Result<serde_json::Value, TransportError> {
        *self.calls.lock().unwrap().entry(path.into()).or_insert(0) += 1;
        let v = self
            .responses
            .get(path)
            .unwrap_or_else(|| panic!("RoutingMock: no canned response for {path}"))
            .clone();
        Ok(v)
    }

    async fn post_form_signed(
        &self,
        _path: &str,
        _body: &str,
        _api_key_header: &str,
        _api_sign_header: &str,
    ) -> Result<serde_json::Value, TransportError> {
        panic!("market-namespace tests never hit signed POST");
    }
}

fn make_market(canned: serde_json::Value) -> (MarketNamespace, Arc<MockHttp>) {
    let mock = Arc::new(MockHttp {
        canned,
        last_request: Mutex::new(None),
    });
    (
        wire_namespace(Arc::clone(&mock) as Arc<dyn HttpTransport>),
        mock,
    )
}

fn make_market_routed(
    responses: Vec<(&str, serde_json::Value)>,
) -> (MarketNamespace, Arc<RoutingMock>) {
    let mock = Arc::new(RoutingMock {
        responses: responses.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        calls: Mutex::new(HashMap::new()),
    });
    (
        wire_namespace(Arc::clone(&mock) as Arc<dyn HttpTransport>),
        mock,
    )
}

fn wire_namespace(transport: Arc<dyn HttpTransport>) -> MarketNamespace {
    let auth = Arc::new(AuthStack::new(
        None,
        None,
        Arc::new(SystemClockNonceSource::new()),
        HashMap::new(),
        crate::auth::TokenLifecycleManager::for_test(),
    ));
    let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
    let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
        crate::dispatch::DispatchEventBusConfig::defaults(),
        Arc::clone(&clock),
    ));
    let api_rl = Arc::new(crate::rate_limit::SpotApiRateLimitTracker::new(
        crate::rate_limit::Tier::Starter,
        Arc::clone(&bus),
        Arc::clone(&clock),
        Arc::new(crate::build::knobs::Knobs::defaults()),
    ));
    let trading_rl = Arc::new(crate::rate_limit::SpotTradingRateLimitTracker::new(
        crate::rate_limit::Tier::Starter,
        Arc::clone(&bus),
        Arc::clone(&clock),
        Arc::new(crate::build::knobs::Knobs::defaults()),
    ));
    let rest = Arc::new(RestSurface::new(
        transport,
        auth,
        api_rl,
        trading_rl,
        Arc::clone(&clock),
        std::time::Duration::from_secs(30),
    ));
    let mirror: crate::dispatch::PresenceMirror =
        Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
    let alloc = Arc::new(std::sync::atomic::AtomicU64::new(1));
    let ws = Arc::new(crate::api::ws_surface::WsSurface::new(
        Arc::clone(&bus),
        mirror,
        alloc,
    ));
    let dispatch =
        Arc::new(crate::dispatch::dispatch_table::DispatchTable::with_default_spot_table());
    MarketNamespace::new(rest, ws, dispatch)
}

fn canned_ticker_response() -> serde_json::Value {
    json!({
        "error": [],
        "result": {
            "BTC/USD": {
                "a": ["50000.5", "1", "1.000"],
                "b": ["49999.0", "1", "1.000"],
                "c": ["50001.0", "0.001"],
                "v": ["100.0", "200.5"],
                "p": ["49950.0", "49500.0"],
                "t": [1000, 2000],
                "l": ["49000.0", "48500.0"],
                "h": ["51000.0", "51500.0"],
                "o": "49800.0"
            }
        }
    })
}

fn ticker_row() -> serde_json::Value {
    json!({
        "a": ["50000.5", "1", "1.000"],
        "b": ["49999.0", "1", "1.000"],
        "c": ["50001.0", "0.001"],
        "v": ["100.0", "200.5"],
        "p": ["49950.0", "49500.0"],
        "t": [1000, 2000],
        "l": ["49000.0", "48500.0"],
        "h": ["51000.0", "51500.0"],
        "o": "49800.0"
    })
}

fn asset_pair_row(base: &str, quote: &str) -> serde_json::Value {
    json!({
        "aclass_base": "currency", "aclass_quote": "currency",
        "base": base, "quote": quote,
        "tick_size": "0.1", "pair_decimals": 1, "cost_decimals": 5,
        "lot_decimals": 8, "ordermin": "0.0001", "costmin": "0.5",
        "fees": [["0", "0.26"]]
    })
}

#[tokio::test]
async fn ticker_all_pairs_rekeys_legacy_via_asset_pairs() {
    let asset_pairs = json!({ "error": [], "result": {
        "XXBTZUSD": asset_pair_row("XXBT", "ZUSD"),
        "ETH/USD": asset_pair_row("XETH", "ZUSD"),
    }});
    let tickers = json!({ "error": [], "result": {
        "XXBTZUSD": ticker_row(),
        "ETH/USD": ticker_row(),
    }});
    let (market, _mock) = make_market_routed(vec![
        ("/0/public/AssetPairs", asset_pairs),
        ("/0/public/Ticker", tickers),
    ]);

    let tr = market.ticker(None).await.unwrap();
    assert!(
        tr.tickers.contains_key("BTC/USD"),
        "legacy key re-keyed to modern"
    );
    assert!(!tr.tickers.contains_key("XXBTZUSD"), "legacy key gone");
    assert!(
        tr.tickers.contains_key("ETH/USD"),
        "already-modern key stays"
    );
    assert_eq!(tr.tickers.len(), 2);
}

/// A legacy row re-keying onto an already-modern row collides: the natively-
/// keyed row wins (never a silent random drop).
#[tokio::test]
async fn ticker_all_pairs_rekey_collision_keeps_native_row() {
    let asset_pairs = json!({ "error": [], "result": {
        "XXBTZUSD": asset_pair_row("XXBT", "ZUSD"),
        "BTC/USD": asset_pair_row("XXBT", "ZUSD"),
    }});
    let mut native = ticker_row();
    native["a"][0] = json!("111.0");
    let tickers = json!({ "error": [], "result": {
        "XXBTZUSD": ticker_row(),
        "BTC/USD": native,
    }});
    let (market, _mock) = make_market_routed(vec![
        ("/0/public/AssetPairs", asset_pairs),
        ("/0/public/Ticker", tickers),
    ]);

    let tr = market.ticker(None).await.unwrap();
    assert_eq!(tr.tickers.len(), 1, "collision folds to one row");
    assert_eq!(
        tr.tickers.get("BTC/USD").unwrap().ask_price,
        "111.0".parse::<rust_decimal::Decimal>().unwrap(),
        "the natively-keyed row wins"
    );
}

/// Two legacy rows re-keying to the same modern symbol: the winner is picked
/// by wire-key order, deterministic across runs.
#[tokio::test]
async fn ticker_all_pairs_rekey_collision_between_rekeyed_rows_is_deterministic() {
    let asset_pairs = json!({ "error": [], "result": {
        "XBTUSD": asset_pair_row("XXBT", "ZUSD"),
        "XXBTZUSD": asset_pair_row("XXBT", "ZUSD"),
    }});
    let mut first = ticker_row();
    first["a"][0] = json!("222.0");
    let tickers = json!({ "error": [], "result": {
        "XBTUSD": first,
        "XXBTZUSD": ticker_row(),
    }});
    let (market, _mock) = make_market_routed(vec![
        ("/0/public/AssetPairs", asset_pairs),
        ("/0/public/Ticker", tickers),
    ]);

    let tr = market.ticker(None).await.unwrap();
    assert_eq!(tr.tickers.len(), 1, "collision folds to one row");
    assert_eq!(
        tr.tickers.get("BTC/USD").unwrap().ask_price,
        "222.0".parse::<rust_decimal::Decimal>().unwrap(),
        "wire-key order picks XBTUSD over XXBTZUSD, every run"
    );
}

/// A failed nested AssetPairs fetch surfaces as this call's error with a
/// correlation id, and the failure is NOT cached — the next call retries.
#[tokio::test]
async fn ticker_all_pairs_asset_pairs_failure_carries_id_and_is_not_cached() {
    use crate::error::ApiError;
    let (market, mock) = make_market_routed(vec![
        (
            "/0/public/Ticker",
            json!({ "error": [], "result": { "XXBTZUSD": ticker_row() } }),
        ),
        (
            "/0/public/AssetPairs",
            json!({ "error": ["EGeneral:Invalid arguments"], "result": {} }),
        ),
    ]);

    let err = market.ticker(None).await.unwrap_err();
    assert!(
        err.request_id().is_some(),
        "nested-fetch failure must carry a correlation id: {err:?}"
    );

    let err2 = market.ticker(None).await.unwrap_err();
    assert!(err2.request_id().is_some());
    assert_eq!(
        mock.count("/0/public/AssetPairs"),
        2,
        "a failed index fetch must not be cached"
    );
}

#[tokio::test]
async fn ticker_all_pairs_fetches_asset_pairs_once() {
    let asset_pairs = json!({ "error": [], "result": {
        "XXBTZUSD": asset_pair_row("XXBT", "ZUSD"),
    }});
    let tickers = json!({ "error": [], "result": { "XXBTZUSD": ticker_row() }});
    let (market, mock) = make_market_routed(vec![
        ("/0/public/AssetPairs", asset_pairs),
        ("/0/public/Ticker", tickers),
    ]);

    market.ticker(None).await.unwrap();
    market.ticker(None).await.unwrap();
    assert_eq!(
        mock.count("/0/public/AssetPairs"),
        1,
        "index cached after first fetch"
    );
    assert_eq!(mock.count("/0/public/Ticker"), 2);
}

#[tokio::test]
async fn ticker_explicit_pairs_skips_asset_pairs_fetch() {
    let tickers = json!({ "error": [], "result": { "BTC/USD": ticker_row() }});
    let (market, mock) = make_market_routed(vec![("/0/public/Ticker", tickers)]);
    let symbol = Symbol::new("BTC/USD").unwrap();

    market
        .ticker(Some(std::slice::from_ref(&symbol)))
        .await
        .unwrap();
    assert_eq!(
        mock.count("/0/public/AssetPairs"),
        0,
        "explicit-pairs path never fetches AssetPairs"
    );
}

#[tokio::test]
async fn ticker_all_pairs_unmapped_key_passes_through_verbatim() {
    let asset_pairs = json!({ "error": [], "result": {
        "XXBTZUSD": asset_pair_row("XXBT", "ZUSD"),
    }});
    let tickers = json!({ "error": [], "result": {
        "XXBTZUSD": ticker_row(),
        "ADA/USD": ticker_row(),
    }});
    let (market, _mock) = make_market_routed(vec![
        ("/0/public/AssetPairs", asset_pairs),
        ("/0/public/Ticker", tickers),
    ]);

    let tr = market.ticker(None).await.unwrap();
    assert_eq!(
        tr.tickers.len(),
        2,
        "unmapped key neither dropped nor panics"
    );
    assert!(tr.tickers.contains_key("BTC/USD"), "mapped key re-keyed");
    assert!(tr.tickers.contains_key("ADA/USD"), "unmapped key verbatim");
}

#[tokio::test]
async fn ticker_hits_canonical_path() {
    let (market, mock) = make_market(canned_ticker_response());
    let symbol = Symbol::new("BTC/USD").unwrap();

    let _ticker = market
        .ticker(Some(std::slice::from_ref(&symbol)))
        .await
        .unwrap();

    let (path, query) = mock.last_request.lock().unwrap().clone().unwrap();
    assert_eq!(path, "/0/public/Ticker");
    assert_eq!(query, vec![("pair".to_string(), "BTC/USD".to_string())]);
}

#[tokio::test]
async fn ticker_parses_canned_response() {
    let (market, _mock) = make_market(canned_ticker_response());
    let symbol = Symbol::new("BTC/USD").unwrap();

    let tr = market
        .ticker(Some(std::slice::from_ref(&symbol)))
        .await
        .unwrap();
    let ticker = tr.get(&symbol).expect("BTC/USD ticker present");

    assert_eq!(ticker.ask_price, "50000.5".parse::<Decimal>().unwrap());
    assert_eq!(ticker.bid_price, "49999.0".parse::<Decimal>().unwrap());
    assert_eq!(ticker.last_price, "50001.0".parse::<Decimal>().unwrap());
    assert_eq!(ticker.last_volume, "0.001".parse::<Decimal>().unwrap());
    assert_eq!(ticker.volume_24h, "200.5".parse::<Decimal>().unwrap());
    assert_eq!(ticker.vwap_24h, "49500.0".parse::<Decimal>().unwrap());
    assert_eq!(ticker.high_24h, "51500.0".parse::<Decimal>().unwrap());
    assert_eq!(ticker.low_24h, "48500.0".parse::<Decimal>().unwrap());
    assert_eq!(ticker.open, "49800.0".parse::<Decimal>().unwrap());
    assert_eq!(ticker.trades_24h, 2000);
    let d = |s: &str| s.parse::<Decimal>().unwrap();
    assert_eq!(ticker.ask_whole_lot_volume, d("1"));
    assert_eq!(ticker.ask_lot_volume, d("1.000"));
    assert_eq!(ticker.bid_whole_lot_volume, d("1"));
    assert_eq!(ticker.bid_lot_volume, d("1.000"));
    assert_eq!(ticker.volume_today, d("100.0"));
    assert_eq!(ticker.vwap_today, d("49950.0"));
    assert_eq!(ticker.trades_today, 1000);
    assert_eq!(ticker.low_today, d("49000.0"));
    assert_eq!(ticker.high_today, d("51000.0"));
}

#[test]
fn orderbook_level_timestamp_decode_is_tolerant() {
    let raw: super::types::RawOrderBook = serde_json::from_value(json!({
        "asks": [["50000.0", "1.5", 1781079637u64]],
        "bids": [["49999.0", "2.0", 1781079637.9], ["49998.0", "3.0", "1781079600"]],
    }))
    .unwrap();
    let book = super::types::OrderBookSnapshot::from_raw(raw).unwrap();
    assert_eq!(book.asks[0].timestamp, 1781079637);
    assert_eq!(book.bids[0].timestamp, 1781079637);
    assert_eq!(book.bids[1].timestamp, 1781079600);
}

#[tokio::test]
async fn orderbook_kraken_error_carries_request_id() {
    let (market, _mock) = make_market(json!({
        "error": ["EQuery:Unknown asset pair"],
        "result": {}
    }));
    let symbol = Symbol::new("FOO/BAR").unwrap();
    let err = market.orderbook(&symbol, None).await.unwrap_err();
    use crate::error::ApiError;
    let rid = err
        .request_id()
        .expect("kraken-rejected orderbook carries the dispatch id");
    assert!(
        uuid::Uuid::parse_str(rid).is_ok(),
        "correlation id is a UUID"
    );
}

#[tokio::test]
async fn ticker_surfaces_kraken_errors() {
    let (market, _mock) = make_market(json!({
        "error": ["EQuery:Unknown asset pair"],
        "result": {}
    }));
    let symbol = Symbol::new("FOO/BAR").unwrap();

    let err = market
        .ticker(Some(std::slice::from_ref(&symbol)))
        .await
        .unwrap_err();
    match err {
        MarketError::SymbolNotFound {
            symbol,
            ref request_id,
        } => {
            assert_eq!(symbol, "FOO/BAR");
            let rid = request_id.as_deref().expect("lifted error keeps the id");
            assert!(
                uuid::Uuid::parse_str(rid).is_ok(),
                "correlation id is a UUID"
            );
        }
        other => panic!("expected SymbolNotFound, got {:?}", other),
    }
}

#[tokio::test]
async fn assets_normalises_wire_codes() {
    let (market, _mock) = make_market(json!({
        "error": [],
        "result": {
            "XXBT": { "aclass": "currency", "altname": "XBT", "decimals": 10, "display_decimals": 5, "status": "enabled" },
            "ZUSD": { "aclass": "currency", "altname": "USD", "decimals": 4, "display_decimals": 2, "status": "enabled" },
            "USDC": { "aclass": "currency", "altname": "USDC", "decimals": 8, "display_decimals": 4, "status": "enabled" }
        }
    }));

    let assets = market.assets(None).await.unwrap();

    assert!(assets.assets.contains_key("BTC"));
    assert!(assets.assets.contains_key("USD"));
    assert!(assets.assets.contains_key("USDC"));
    assert!(!assets.assets.contains_key("XXBT"));
    assert!(!assets.assets.contains_key("ZUSD"));
    assert_eq!(assets.assets.get("BTC").unwrap().altname, "XBT");
}

#[tokio::test]
async fn asset_pairs_tolerates_missing_status() {
    // AssetPairs omits status on many live pairs.
    let (market, _mock) = make_market(json!({
        "error": [],
        "result": {
            "BTC/USD": {
                "aclass_base": "currency", "aclass_quote": "currency",
                "tick_size": "0.1", "pair_decimals": 1, "cost_decimals": 5,
                "lot_decimals": 8, "ordermin": "0.0001", "costmin": "0.5",
                "status": "online",
                "fees": [["0", "0.26"]]
            },
            "ETH/USD": {
                "aclass_base": "currency", "aclass_quote": "currency",
                "tick_size": "0.01", "pair_decimals": 2, "cost_decimals": 5,
                "lot_decimals": 8, "ordermin": "0.01", "costmin": "0.5",
                "fees": [["0", "0.26"]]
            }
        }
    }));

    let pairs = market.pairs(None).await.unwrap();

    assert_eq!(pairs.pairs.len(), 2);
    assert_eq!(
        pairs.pairs.get("BTC/USD").unwrap().status.as_deref(),
        Some("online")
    );
    assert_eq!(pairs.pairs.get("ETH/USD").unwrap().status, None);
}

#[tokio::test]
async fn asset_pairs_slashless_key_rebuilt_to_modern() {
    let (market, _mock) = make_market(json!({
        "error": [],
        "result": {
            "RENDERUSD": {
                "aclass_base": "currency", "aclass_quote": "currency",
                "base": "RENDER", "quote": "ZUSD",
                "tick_size": "0.001", "pair_decimals": 3, "cost_decimals": 5,
                "lot_decimals": 8, "ordermin": "1", "costmin": "0.5",
                "fees": [["0", "0.26"]]
            },
            "BTC/USD": {
                "aclass_base": "currency", "aclass_quote": "currency",
                "base": "XXBT", "quote": "ZUSD",
                "tick_size": "0.1", "pair_decimals": 1, "cost_decimals": 5,
                "lot_decimals": 8, "ordermin": "0.0001", "costmin": "0.5",
                "fees": [["0", "0.26"]]
            }
        }
    }));

    let pairs = market.pairs(None).await.unwrap();
    assert_eq!(pairs.pairs.len(), 2);
    assert!(
        pairs.pairs.contains_key("RENDER/USD"),
        "slashless key rebuilt"
    );
    assert!(!pairs.pairs.contains_key("RENDERUSD"));
    assert!(pairs.pairs.contains_key("BTC/USD"), "slashed key verbatim");
}

#[tokio::test]
async fn asset_pairs_decodes_newly_exposed_optional_fields() {
    let (market, _mock) = make_market(json!({
        "error": [],
        "result": {
            "BTC/USD": {
                "aclass_base": "currency", "aclass_quote": "currency",
                "tick_size": "0.1", "pair_decimals": 1, "cost_decimals": 5,
                "lot_decimals": 8, "ordermin": "0.0001", "costmin": "0.5",
                "status": "online", "fees": [["0", "0.26"]],
                "lot_multiplier": 1,
                "fee_volume_currency": "ZUSD",
                "long_position_limit": 5_000_000_000_u64,
                "short_position_limit": 300,
                "execution_venue": "international"
            },
            "XRP/USD": {
                "aclass_base": "currency", "aclass_quote": "currency",
                "tick_size": "0.0001", "pair_decimals": 4, "cost_decimals": 5,
                "lot_decimals": 8, "ordermin": "1", "costmin": "0.5",
                "status": "online", "fees": [["0", "0.26"]]
            }
        }
    }));

    let pairs = market.pairs(None).await.unwrap();

    assert_eq!(pairs.pairs.len(), 2);

    let btc = pairs.pairs.get("BTC/USD").unwrap();
    assert_eq!(btc.lot_multiplier, Some(1));
    assert_eq!(
        btc.fee_volume_currency.as_ref().map(|c| c.as_str()),
        Some("USD")
    );
    // Position limits can exceed u32 (in lots).
    assert_eq!(btc.long_position_limit, Some(5_000_000_000));
    assert_eq!(btc.short_position_limit, Some(300));
    assert_eq!(btc.execution_venue.as_deref(), Some("international"));

    let xrp = pairs.pairs.get("XRP/USD").unwrap();
    assert_eq!(xrp.lot_multiplier, None);
    assert_eq!(xrp.fee_volume_currency, None);
    assert_eq!(xrp.long_position_limit, None);
    assert_eq!(xrp.short_position_limit, None);
    assert_eq!(xrp.execution_venue, None);
}

#[test]
fn market_system_status_message_is_plain_language() {
    let current = super::types::SystemStatus {
        status: "maintenance".to_string(),
        timestamp: String::new(),
    };
    let required = super::types::SystemStatus {
        status: "online".to_string(),
        timestamp: String::new(),
    };
    let msg = MarketError::SystemStatus { current, required }.to_string();
    assert!(
        msg.contains("Operation blocked: exchange system status is")
            && msg.contains("retry once the status returns"),
        "message lost its plain-language phrasing: {msg}"
    );
    assert!(
        msg.contains("\"maintenance\"") && msg.contains("\"online\""),
        "message lost the status strings: {msg}"
    );
    assert!(
        !msg.contains("SystemStatus {"),
        "regressed to a Debug dump: {msg}"
    );
}

#[tokio::test]
async fn orderbook_hits_canonical_path_with_default_count() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": {
                "asks": [["50000.0", "1.5", 1700000000_u64]],
                "bids": [["49900.0", "2.0", 1700000001_u64]]
            }
        }
    });
    let (market, mock) = make_market(canned);
    let symbol = Symbol::new("BTC/USD").unwrap();

    let book = market.orderbook(&symbol, None).await.unwrap();

    let (path, query) = mock.last_request.lock().unwrap().clone().unwrap();
    assert_eq!(path, "/0/public/Depth");
    assert_eq!(
        query,
        vec![
            ("pair".to_string(), "BTC/USD".to_string()),
            ("count".to_string(), "100".to_string()),
        ]
    );

    assert_eq!(book.asks.len(), 1);
    assert_eq!(book.asks[0].price, "50000.0".parse::<Decimal>().unwrap());
    assert_eq!(book.asks[0].volume, "1.5".parse::<Decimal>().unwrap());
    assert_eq!(book.asks[0].timestamp, 1700000000);
    assert_eq!(book.bids[0].price, "49900.0".parse::<Decimal>().unwrap());
}

#[tokio::test]
async fn trades_returns_result_with_pagination_cursor() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                ["50000.0", "0.001", 1700000000.1234, "b", "m", "", 123_u64],
                ["50001.0", "0.002", 1700000001.5, "s", "l", "misc", 124_u64]
            ],
            "last": "1700000002000000000"
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let result = market.trades(TradesRequest::new(pair)).await.unwrap();

    assert_eq!(result.last, "1700000002000000000");
    assert_eq!(result.pair, Symbol::new("BTC/USD").unwrap());
    assert_eq!(result.trades.len(), 2);
    assert_eq!(
        result.trades[0].price,
        "50000.0".parse::<Decimal>().unwrap()
    );
    assert_eq!(result.trades[0].side, TradeSide::Buy);
    assert_eq!(result.trades[0].order_type, "m");
    assert_eq!(result.trades[0].trade_id, 123);
    assert_eq!(result.trades[1].side, TradeSide::Sell);
    assert_eq!(result.trades[1].order_type, "l");
    assert_eq!(result.trades[1].misc, "misc");
    assert_eq!(result.trades[1].trade_id, 124);
}

#[tokio::test]
async fn trades_unknown_side_or_type_do_not_drop_batch() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [["50000.0", "0.001", 1700000000.0, "x", "z", "", 1_u64]],
            "last": "1"
        }
    });
    let (market, _mock) = make_market(canned);
    let result = market
        .trades(TradesRequest::new(Symbol::new("BTC/USD").unwrap()))
        .await
        .unwrap();
    assert_eq!(result.trades.len(), 1);
    assert_eq!(result.trades[0].side, TradeSide::Unknown);
    assert_eq!(result.trades[0].order_type, "z");
}

#[tokio::test]
async fn spreads_stringifies_wire_integer_cursor() {
    // Spreads result.last is a JSON integer.
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                [1700000000_u64, "49999.0", "50000.0"],
                [1700000001_u64, "49998.5", "50001.5"]
            ],
            "last": 1700000001_u64
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let result = market.spreads(pair, None).await.unwrap();

    assert_eq!(result.last, "1700000001");
    assert_eq!(result.pair, Symbol::new("BTC/USD").unwrap());
    assert_eq!(result.spreads.len(), 2);
    assert_eq!(result.spreads[0].time, 1700000000);
    assert_eq!(result.spreads[0].bid, "49999.0".parse::<Decimal>().unwrap());
    assert_eq!(result.spreads[0].ask, "50000.0".parse::<Decimal>().unwrap());
    assert_eq!(result.spreads[1].bid, "49998.5".parse::<Decimal>().unwrap());
}

#[tokio::test]
async fn status_parses_canonical_shape() {
    let canned = json!({
        "error": [],
        "result": {
            "status": "online",
            "timestamp": "2026-05-24T12:34:56Z"
        }
    });
    let (market, _mock) = make_market(canned);

    let status = market.status().await.unwrap();
    assert_eq!(status.status, "online");
    assert_eq!(status.timestamp, "2026-05-24T12:34:56Z");
}

#[test]
fn ticker_update_from_wire_accepts_strings_and_numbers() {
    // Ticker docs say decimal-strings but the wire emits JSON numbers.
    let raw: RawTickerUpdate = serde_json::from_value(json!({
        "symbol": "BTC/USD",
        "bid": "49999.0",
        "bid_qty": 1.5,
        "ask": "50000.5",
        "ask_qty": 2.0,
        "last": "50001.0",
        "volume": 200.5,
        "vwap": "49500.0",
        "low": "48500.0",
        "high": "51500.0",
        "change": 100.0,
        "change_pct": "0.2"
    }))
    .unwrap();

    let update = TickerUpdate::from_wire(raw).unwrap();
    assert_eq!(update.symbol, Symbol::new("BTC/USD").unwrap());
    assert_eq!(update.bid, "49999.0".parse::<Decimal>().unwrap());
    assert_eq!(update.bid_qty, "1.5".parse::<Decimal>().unwrap());
    assert_eq!(update.ask, "50000.5".parse::<Decimal>().unwrap());
    assert_eq!(update.volume, "200.5".parse::<Decimal>().unwrap());
    assert_eq!(update.change_pct, "0.2".parse::<Decimal>().unwrap());
}

#[test]
fn on_ticker_returns_handle_and_decode_helper_unwraps_envelope() {
    let (market, _mock) = make_market(canned_ticker_response());
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hits_cb = Arc::clone(&hits);
    let handle = market.on_ticker(move |_u: &TickerUpdate| {
        hits_cb.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
    assert_eq!(handle.channel(), ChannelName::Ticker);

    let envelope = json!({
        "type": "update",
        "data": {
            "symbol": "BTC/USD",
            "bid": "49999.0", "bid_qty": "1", "ask": "50000.5", "ask_qty": "1",
            "last": "50001.0", "volume": "1", "vwap": "1", "low": "1",
            "high": "1", "change": "1", "change_pct": "1",
            "timestamp": "2026-07-10T09:44:42.160635Z"
        }
    });
    let update = decode_ticker(envelope).expect("decode ticker envelope");
    assert_eq!(update.symbol, Symbol::new("BTC/USD").unwrap());
    assert_eq!(update.timestamp, "2026-07-10T09:44:42.160635Z");

    assert!(decode_ticker(json!("not an object")).is_none());
    assert!(decode_ticker(json!({ "type": "update" })).is_none());
}

#[test]
fn on_ticker_for_returns_guard_on_success() {
    let (market, _mock) = make_market(canned_ticker_response());
    let pairs = vec![Symbol::new("BTC/USD").unwrap()];
    let guard = market
        .on_ticker_for(&pairs, None, None, |_u: &TickerUpdate| {})
        .expect("subscribe posts succeed on a live bus");
    assert_eq!(guard.channel(), ChannelName::Ticker);
    drop(guard);
}

#[test]
fn on_ticker_for_dedupes_pairs_before_register_and_guard() {
    // A duplicate pair must not register twice under one guard — drop would over-release.
    let (market, _mock) = make_market(canned_ticker_response());
    let pairs = vec![
        Symbol::new("BTC/USD").unwrap(),
        Symbol::new("BTC/USD").unwrap(),
        Symbol::new("ETH/USD").unwrap(),
    ];
    let guard = market
        .on_ticker_for(&pairs, None, None, |_u: &TickerUpdate| {})
        .expect("subscribe posts succeed on a live bus");
    assert_eq!(
        guard.pairs(),
        &[
            Symbol::new("BTC/USD").unwrap(),
            Symbol::new("ETH/USD").unwrap()
        ],
        "order-preserving dedupe, first occurrence wins"
    );
    drop(guard);
}

#[test]
fn filter_by_pairs_delivers_only_requested_pairs() {
    // Handler that asked for BTC/USD must not see ETH/USD (registry is channel-wide).
    fn ticker(sym: &str) -> TickerUpdate {
        decode_ticker(json!({
            "type": "update",
            "data": {
                "symbol": sym,
                "bid": "1", "bid_qty": "1", "ask": "1", "ask_qty": "1",
                "last": "1", "volume": "1", "vwap": "1", "low": "1",
                "high": "1", "change": "1", "change_pct": "1"
            }
        }))
        .expect("decode ticker")
    }

    let seen = Arc::new(Mutex::new(Vec::<Symbol>::new()));
    let seen_cb = Arc::clone(&seen);
    let cb = filter_by_pairs(
        vec![Symbol::new("BTC/USD").unwrap()],
        move |u: &TickerUpdate| seen_cb.lock().unwrap().push(u.symbol.clone()),
    );

    cb(&ticker("BTC/USD"));
    cb(&ticker("ETH/USD"));
    cb(&ticker("BTC/USD"));

    let got = seen.lock().unwrap();
    assert_eq!(*got, vec![Symbol::new("BTC/USD").unwrap(); 2]);
}

#[test]
fn ticker_update_from_wire_rejects_legacy_symbol() {
    let raw: RawTickerUpdate = serde_json::from_value(json!({
        "symbol": "XXBTZUSD",
        "bid": "1", "bid_qty": "1", "ask": "1", "ask_qty": "1",
        "last": "1", "volume": "1", "vwap": "1", "low": "1",
        "high": "1", "change": "1", "change_pct": "1"
    }))
    .unwrap();
    assert!(TickerUpdate::from_wire(raw).is_err());
}

fn book_envelope(msg_type: &str) -> serde_json::Value {
    json!({
        "type": msg_type,
        "data": {
            "symbol": "BTC/USD",
            "bids": [
                { "price": "49999.0", "qty": "1.5" },
                { "price": "49998.0", "qty": "2.0" }
            ],
            "asks": [
                { "price": "50000.5", "qty": "0.75" }
            ],
            "checksum": 1234567890_u32,
            "timestamp": "2026-07-10T09:44:44.411241Z"
        }
    })
}

#[test]
fn decode_book_happy_path_maps_levels_and_checksum() {
    let update = decode_book(book_envelope("snapshot")).expect("decode book");
    assert_eq!(update.symbol, Symbol::new("BTC/USD").unwrap());
    assert_eq!(update.checksum, 1234567890);
    assert_eq!(update.timestamp, None);
    assert_eq!(update.exchange_timestamp, "2026-07-10T09:44:44.411241Z");

    assert_eq!(update.bids.len(), 2);
    assert_eq!(update.bids[0].price, "49999.0".parse::<Decimal>().unwrap());
    assert_eq!(update.bids[0].qty, "1.5".parse::<Decimal>().unwrap());

    assert_eq!(update.asks.len(), 1);
    assert_eq!(update.asks[0].price, "50000.5".parse::<Decimal>().unwrap());
    assert_eq!(update.asks[0].qty, "0.75".parse::<Decimal>().unwrap());
}

#[test]
fn decode_book_raw_reads_is_snapshot_from_envelope_type() {
    let snap = decode_book_raw(book_envelope("snapshot")).expect("decode snapshot");
    assert!(snap.is_snapshot);
    assert_eq!(snap.symbol, Symbol::new("BTC/USD").unwrap());
    assert_eq!(snap.checksum, 1234567890);
    assert_eq!(snap.exchange_timestamp, "2026-07-10T09:44:44.411241Z");

    let upd = decode_book_raw(book_envelope("update")).expect("decode update");
    assert!(!upd.is_snapshot);
    assert_eq!(upd.bids.len(), 2);
    assert_eq!(upd.asks.len(), 1);
}

#[test]
fn decode_book_returns_none_on_malformed_frame() {
    assert!(decode_book(json!({ "type": "snapshot" })).is_none());
    assert!(decode_book(json!("not an object")).is_none());
    assert!(
        decode_book(json!({
            "type": "snapshot",
            "data": { "symbol": "BTC/USD", "bids": [], "asks": [] }
        }))
        .is_none()
    );
    assert!(
        decode_book_raw(json!({
            "type": "update",
            "data": {
                "symbol": "BTC/USD",
                "bids": [{ "price": "not-a-number", "qty": "1.0" }],
                "asks": [],
                "checksum": 1_u32
            }
        }))
        .is_none()
    );
    assert!(
        decode_book(json!({
            "type": "snapshot",
            "data": { "symbol": "XXBTZUSD", "bids": [], "asks": [], "checksum": 1_u32 }
        }))
        .is_none()
    );
}

#[test]
fn on_book_and_on_book_raw_register_under_correct_channels() {
    let (market, _mock) = make_market(canned_ticker_response());
    let h_book = market.on_book(|_u: &OrderBookUpdate| {});
    assert_eq!(h_book.channel(), ChannelName::Book);
    let h_raw = market.on_book_raw(|_d: &BookDelta| {});
    assert_eq!(h_raw.channel(), ChannelName::BookRaw);
}

#[test]
fn on_book_for_and_raw_for_return_guards_on_correct_channels() {
    let (market, _mock) = make_market(canned_ticker_response());
    let pairs = vec![Symbol::new("BTC/USD").unwrap()];

    let guard = market
        .on_book_for(&pairs, BookDepth::D10, |_u: &OrderBookUpdate| {})
        .expect("subscribe posts succeed on a live bus");
    assert_eq!(guard.channel(), ChannelName::Book);
    drop(guard);

    let raw_guard = market
        .on_book_raw_for(&pairs, BookDepth::D10, None, |_d: &BookDelta| {})
        .expect("subscribe posts succeed on a live bus");
    assert_eq!(raw_guard.channel(), ChannelName::BookRaw);
    drop(raw_guard);
}

fn trade_envelope(side: &str, qty: serde_json::Value) -> serde_json::Value {
    json!({
        "type": "update",
        "data": {
            "symbol": "BTC/USD",
            "side": side,
            "price": 68000.1,
            "qty": qty,
            "ord_type": "limit",
            "trade_id": 123456789_u64,
            "timestamp": "2026-06-02T12:00:00.000000Z"
        }
    })
}

#[test]
fn decode_trade_happy_path_buy_and_sell_and_sci_notation_qty() {
    let buy = decode_trade(trade_envelope("buy", json!(5.1e-05))).expect("decode buy trade");
    assert_eq!(buy.symbol, Symbol::new("BTC/USD").unwrap());
    assert_eq!(buy.side, TradeSide::Buy);
    assert_eq!(buy.price, "68000.1".parse::<Decimal>().unwrap());
    assert_eq!(buy.qty, "0.000051".parse::<Decimal>().unwrap());
    assert_eq!(buy.ord_type, "limit");
    assert_eq!(buy.trade_id, 123456789);
    assert_eq!(buy.timestamp, "2026-06-02T12:00:00.000000Z");

    let sell = decode_trade(trade_envelope("sell", json!("0.25"))).expect("decode sell trade");
    assert_eq!(sell.side, TradeSide::Sell);
    assert_eq!(sell.qty, "0.25".parse::<Decimal>().unwrap());
}

#[test]
fn decode_trade_unknown_side_does_not_drop_frame() {
    let t = decode_trade(trade_envelope("flash", json!("0.25")))
        .expect("unknown side should still decode the trade");
    assert_eq!(t.side, TradeSide::Unknown);
}

#[test]
fn decode_trade_returns_none_on_malformed_frame() {
    assert!(decode_trade(json!({ "type": "update" })).is_none());
    assert!(decode_trade(json!("not an object")).is_none());
    assert!(
        decode_trade(json!({
            "type": "update",
            "data": {
                "symbol": "XXBTZUSD", "side": "buy", "price": 1.0, "qty": 1.0,
                "ord_type": "limit", "trade_id": 1_u64, "timestamp": "t"
            }
        }))
        .is_none()
    );
}

fn ohlc_envelope(msg_type: &str) -> serde_json::Value {
    json!({
        "type": msg_type,
        "data": {
            "symbol": "BTC/USD",
            "open": 67900.0,
            "high": 68100.5,
            "low": 67800.0,
            "close": 68000.1,
            "trades": 42_u32,
            "volume": 12.5,
            "vwap": 67950.3,
            "interval_begin": "2026-06-02T12:00:00.000000Z",
            "interval": 5_u32,
            "timestamp": "2026-06-02T12:05:00.000000Z"
        }
    })
}

#[test]
fn decode_ohlc_happy_path_captures_interval_begin() {
    let snap = decode_ohlc(ohlc_envelope("snapshot")).expect("decode ohlc snapshot");
    assert_eq!(snap.symbol, Symbol::new("BTC/USD").unwrap());
    assert_eq!(snap.open, "67900.0".parse::<Decimal>().unwrap());
    assert_eq!(snap.high, "68100.5".parse::<Decimal>().unwrap());
    assert_eq!(snap.low, "67800.0".parse::<Decimal>().unwrap());
    assert_eq!(snap.close, "68000.1".parse::<Decimal>().unwrap());
    assert_eq!(snap.trades, 42);
    assert_eq!(snap.volume, "12.5".parse::<Decimal>().unwrap());
    assert_eq!(snap.vwap, "67950.3".parse::<Decimal>().unwrap());
    assert_eq!(snap.interval_begin, "2026-06-02T12:00:00.000000Z");
    assert_eq!(snap.interval, 5);

    let upd = decode_ohlc(ohlc_envelope("update")).expect("decode ohlc update");
    assert_eq!(upd.interval, 5);
}

#[test]
fn decode_ohlc_returns_none_on_malformed_frame() {
    assert!(decode_ohlc(json!({ "type": "update" })).is_none());
    assert!(decode_ohlc(json!("not an object")).is_none());
    assert!(
        decode_ohlc(json!({
            "type": "update",
            "data": {
                "symbol": "BTC/USD", "open": "nan", "high": 1.0, "low": 1.0, "close": 1.0,
                "trades": 1_u32, "volume": 1.0, "vwap": 1.0,
                "interval_begin": "t", "interval": 5_u32
            }
        }))
        .is_none()
    );
    assert!(
        decode_ohlc(json!({
            "type": "snapshot",
            "data": {
                "symbol": "XXBTZUSD", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
                "trades": 1_u32, "volume": 1.0, "vwap": 1.0,
                "interval_begin": "t", "interval": 5_u32
            }
        }))
        .is_none()
    );
}

#[test]
fn on_trade_and_on_ohlc_register_under_correct_channels() {
    let (market, _mock) = make_market(canned_ticker_response());
    let h_trade = market.on_trade(|_u: &TradeUpdate| {});
    assert_eq!(h_trade.channel(), ChannelName::Trade);
    let h_ohlc = market.on_ohlc(|_u: &OhlcUpdate| {});
    assert_eq!(h_ohlc.channel(), ChannelName::Ohlc);
}

#[test]
fn decode_system_status_full_auto_seed_frame() {
    // Large connection_id (14276769864174618252) overflows i64, so must parse as u64.
    let envelope = json!({
        "type": "update",
        "data": {
            "system": "online",
            "version": "2.0.10",
            "api_version": "v2",
            "connection_id": 14276769864174618252_u64
        }
    });
    let update = decode_system_status(envelope).expect("decode full status frame");
    assert_eq!(update.system, Some("online".to_string()));
    assert_eq!(update.version, Some("2.0.10".to_string()));
    assert_eq!(update.api_version, Some("v2".to_string()));
    assert_eq!(update.connection_id, Some(14276769864174618252_u64));
}

#[test]
fn decode_system_status_lenient_partial_frame() {
    let envelope = json!({
        "type": "update",
        "data": { "system": "maintenance" }
    });
    let update = decode_system_status(envelope).expect("decode partial status frame");
    assert_eq!(update.system, Some("maintenance".to_string()));
    assert_eq!(update.version, None);
    assert_eq!(update.api_version, None);
    assert_eq!(update.connection_id, None);
}

#[test]
fn decode_system_status_unknown_system_token_passthrough() {
    let envelope = json!({
        "type": "update",
        "data": { "system": "some_future_mode" }
    });
    let update = decode_system_status(envelope).expect("unknown system token passes through");
    assert_eq!(update.system, Some("some_future_mode".to_string()));
}

#[test]
fn decode_system_status_non_object_data_returns_none() {
    let envelope = json!({ "type": "update", "data": "x" });
    assert!(decode_system_status(envelope).is_none());
}

#[test]
fn decode_system_status_lenient_connection_id() {
    let envelope = json!({
        "type": "update",
        "data": { "system": "online", "connection_id": "14276769864174618252" }
    });
    let update = decode_system_status(envelope).expect("string connection_id must not drop frame");
    assert_eq!(update.system, Some("online".to_string()));
    assert_eq!(update.connection_id, None);
    let envelope =
        json!({ "type": "update", "data": { "system": "online", "connection_id": 1.5 } });
    let update = decode_system_status(envelope).expect("float connection_id must not drop frame");
    assert_eq!(update.connection_id, None);
    let envelope = json!({ "type": "update", "data": { "connection_id": 42_u64 } });
    let update = decode_system_status(envelope).expect("integer connection_id decodes");
    assert_eq!(update.connection_id, Some(42));
}

#[test]
fn on_system_status_returns_handle_under_status_channel() {
    let (market, _mock) = make_market(canned_ticker_response());
    let handle = market.on_system_status(|_u: &SystemStatusUpdate| {});
    assert_eq!(handle.channel(), ChannelName::Status);
}

#[test]
fn on_trade_for_and_ohlc_for_return_guards_on_correct_channels() {
    let (market, _mock) = make_market(canned_ticker_response());
    let pairs = vec![Symbol::new("BTC/USD").unwrap()];

    let trade_guard = market
        .on_trade_for(&pairs, None, |_u: &TradeUpdate| {})
        .expect("subscribe posts succeed on a live bus");
    assert_eq!(trade_guard.channel(), ChannelName::Trade);
    drop(trade_guard);

    let ohlc_guard = market
        .on_ohlc_for(&pairs, OhlcInterval::M5, None, |_u: &OhlcUpdate| {})
        .expect("subscribe posts succeed on a live bus");
    assert_eq!(ohlc_guard.channel(), ChannelName::Ohlc);
    drop(ohlc_guard);
}

#[tokio::test]
async fn trades_missing_last_returns_malformed_error() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                ["50000.0", "0.001", 1700000000.1234, "b", "m", "", 123_u64]
            ]
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let err = market.trades(TradesRequest::new(pair)).await.unwrap_err();
    assert!(
        matches!(err, MarketError::MalformedResponse { .. }),
        "trades with absent `last` must return MalformedResponse; got {err:?}"
    );
    {
        use crate::error::ApiError;
        let rid = err
            .request_id()
            .expect("decode error carries the dispatch id");
        assert!(
            uuid::Uuid::parse_str(rid).is_ok(),
            "correlation id is a UUID"
        );
    }
}

#[tokio::test]
async fn trades_last_as_number_returns_malformed_error() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                ["50000.0", "0.001", 1700000000.1234, "b", "m", "", 123_u64]
            ],
            "last": 1700000002000000000_u64
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let err = market.trades(TradesRequest::new(pair)).await.unwrap_err();
    assert!(
        matches!(err, MarketError::MalformedResponse { .. }),
        "trades with numeric `last` must return MalformedResponse (expects string); got {err:?}"
    );
}

#[tokio::test]
async fn spreads_missing_last_returns_malformed_error() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                [1700000000_u64, "49999.0", "50000.0"]
            ]
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let err = market.spreads(pair, None).await.unwrap_err();
    assert!(
        matches!(err, MarketError::MalformedResponse { .. }),
        "spreads with absent `last` must return MalformedResponse; got {err:?}"
    );
}

#[tokio::test]
async fn spreads_last_as_string_returns_malformed_error() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                [1700000000_u64, "49999.0", "50000.0"]
            ],
            "last": "1700000001"
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let err = market.spreads(pair, None).await.unwrap_err();
    assert!(
        matches!(err, MarketError::MalformedResponse { .. }),
        "spreads with string `last` must return MalformedResponse (expects integer); got {err:?}"
    );
}

// Pagination cursor `last` typing differs by endpoint.

#[tokio::test]
async fn trades_last_negative_string_passes_through_as_cursor() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                ["50000.0", "0.001", 1700000000.0_f64, "b", "m", "", 123_u64]
            ],
            "last": "-1"
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let result = market.trades(TradesRequest::new(pair)).await.unwrap();
    assert_eq!(
        result.last, "-1",
        "trades `last` = \"-1\" round-trips verbatim (decimal ns-string cursor, \
         TradesResult.last: String)"
    );
}

#[tokio::test]
async fn trades_last_non_numeric_string_passes_through_as_cursor() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                ["50000.0", "0.001", 1700000000.0_f64, "b", "m", "", 123_u64]
            ],
            "last": "abc"
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let result = market.trades(TradesRequest::new(pair)).await.unwrap();
    assert_eq!(
        result.last, "abc",
        "trades `last` = \"abc\" round-trips verbatim (opaque decimal ns-string \
         cursor)"
    );
}

#[tokio::test]
async fn trades_last_empty_string_passes_through_as_cursor() {
    // Empty `last` may be Kraken's pagination-end sentinel.
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                ["50000.0", "0.001", 1700000000.0_f64, "b", "m", "", 123_u64]
            ],
            "last": ""
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let result = market.trades(TradesRequest::new(pair)).await.unwrap();
    assert_eq!(
        result.last, "",
        "trades `last` = \"\" round-trips verbatim (may be the pagination-end \
         sentinel; must NOT be rejected)"
    );
}

#[tokio::test]
async fn ohlc_malformed_last_values_return_malformed_error() {
    // `last` must be a plain in-range u64; every other wire shape rejects.
    let cases: [(&str, serde_json::Value); 4] = [
        ("negative integer (as_u64 rejects negatives)", json!(-1_i64)),
        ("non-numeric string", json!("abc")),
        ("float", json!(1700000060.5_f64)),
        (
            "out-of-u64-range (no silent truncation)",
            serde_json::from_str("18446744073709551616").unwrap(),
        ),
    ];

    for (label, last) in cases {
        let canned = json!({
            "error": [],
            "result": {
                "BTC/USD": [
                    [1700000000_u64, "50000.0", "51000.0", "49000.0", "50500.0", "50250.0", "100.5", 42_u32]
                ],
                "last": last
            }
        });
        let (market, _mock) = make_market(canned);
        let pair = Symbol::new("BTC/USD").unwrap();

        let err = market
            .ohlc(OhlcRequest::new(pair, OhlcInterval::M1))
            .await
            .unwrap_err();
        assert!(
            matches!(err, MarketError::MalformedResponse { .. }),
            "ohlc `last` = {label} must return MalformedResponse; got {err:?}"
        );
    }
}

// arbitrary_precision keeps JSON number text; Decimal recovers precision beyond f64.

#[test]
fn parse_wire_decimal_number_preserves_full_precision() {
    let v: serde_json::Value =
        serde_json::from_str("0.123456789012345678").expect("valid JSON number");
    let dec = parse_wire_decimal(&v).expect("parse_wire_decimal must succeed");
    let expected = "0.123456789012345678".parse::<Decimal>().unwrap();
    assert_eq!(
        dec, expected,
        "parse_wire_decimal lost precision: got {dec}, want {expected}"
    );
}

#[test]
fn ticker_from_wire_number_preserves_full_precision() {
    let raw: RawTickerUpdate = serde_json::from_str(
        r#"{
        "symbol": "BTC/USD",
        "bid": 123456789.123456789,
        "bid_qty": "1",
        "ask": "50000.5",
        "ask_qty": "1",
        "last": "50001.0",
        "volume": "1",
        "vwap": "1",
        "low": "1",
        "high": "1",
        "change": "0",
        "change_pct": "0"
    }"#,
    )
    .expect("valid ticker JSON");
    let update = TickerUpdate::from_wire(raw).expect("from_wire must succeed");
    let expected = "123456789.123456789".parse::<Decimal>().unwrap();
    assert_eq!(
        update.bid, expected,
        "TickerUpdate::from_wire lost precision: got {}, want {}",
        update.bid, expected
    );
}

// /Trades side/type are single chars.

#[tokio::test]
async fn trades_single_char_side_and_type_decode_correctly() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                ["50000.0", "0.001", 1781079637.7188106_f64, "b", "l", "", 200_u64]
            ],
            "last": "1781079637718810624"
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let result = market.trades(TradesRequest::new(pair)).await.unwrap();
    assert_eq!(result.trades.len(), 1);
    let t = &result.trades[0];
    assert_eq!(t.side, TradeSide::Buy);
    assert_eq!(t.order_type, "l");
    assert_eq!(t.time, "1781079637.7188106");
    assert_eq!(t.trade_id, 200);
}

#[tokio::test]
async fn trade_time_preserved_verbatim_beyond_f64_precision() {
    // /Trades `time` exceeds f64 precision; preserved verbatim.
    let canned: serde_json::Value = serde_json::from_str(
        r#"{"error":[],"result":{"BTC/USD":[["50000.0","0.001",1781079637.718810699999,"b","l","",200]],"last":"1"}}"#,
    )
    .unwrap();
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let result = market.trades(TradesRequest::new(pair)).await.unwrap();
    let t = &result.trades[0];
    assert_eq!(t.time, "1781079637.718810699999");
    assert_ne!(
        t.time,
        "1781079637.718810699999"
            .parse::<f64>()
            .unwrap()
            .to_string()
    );
}

#[tokio::test]
async fn trades_sell_market_single_char_decodes_correctly() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                ["49000.0", "0.5", 1700000001.5_f64, "s", "m", "misc", 201_u64]
            ],
            "last": "1700000001500000000"
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let result = market.trades(TradesRequest::new(pair)).await.unwrap();
    let t = &result.trades[0];
    assert_eq!(t.side, TradeSide::Sell);
    assert_eq!(t.order_type, "m");
    assert_eq!(t.misc, "misc");
}

#[tokio::test]
async fn ohlc_result_carries_pair_and_count_as_u32() {
    let canned = json!({
        "error": [],
        "result": {
            "BTC/USD": [
                [1700000000_u64, "50000.0", "51000.0", "49000.0", "50500.0", "50250.0", "100.5", 42_u32]
            ],
            "last": 1700000060_u64
        }
    });
    let (market, _mock) = make_market(canned);
    let pair = Symbol::new("BTC/USD").unwrap();

    let result = market
        .ohlc(OhlcRequest::new(pair.clone(), OhlcInterval::M1))
        .await
        .unwrap();
    assert_eq!(result.pair, pair);
    assert_eq!(result.last, 1700000060_u64);
    assert_eq!(result.candles.len(), 1);
    let c = &result.candles[0];
    assert_eq!(c.count, 42_u32);
    assert_eq!(c.time, 1700000000);
    assert_eq!(c.open, "50000.0".parse::<Decimal>().unwrap());
    assert_eq!(c.close, "50500.0".parse::<Decimal>().unwrap());
}

/// A consumer republishing frames has no other way to render them, and a
/// hand-written serializer would silently drop fields added later.
#[test]
fn ws_trade_update_serializes_under_its_descriptive_field_names() {
    let update = decode_trade(trade_envelope("buy", json!("0.25"))).expect("decode trade");
    let v = serde_json::to_value(&update).expect("TradeUpdate serializes");
    assert_eq!(v["symbol"], json!("BTC/USD"));
    assert_eq!(v["side"], json!("buy"));
    assert_eq!(v["ord_type"], json!("limit"));
    assert_eq!(v["trade_id"], json!(123456789_u64));
    assert_eq!(v["timestamp"], json!("2026-06-02T12:00:00.000000Z"));
}

/// Kraken sends `status` partially populated; what it omitted stays omitted.
#[test]
fn ws_system_status_update_omits_absent_optional_fields() {
    let update = decode_system_status(json!({
        "type": "update",
        "data": { "system": "maintenance" }
    }))
    .expect("decode partial status frame");
    let v = serde_json::to_value(&update).expect("SystemStatusUpdate serializes");
    let obj = v.as_object().expect("serializes to an object");
    assert_eq!(v["system"], json!("maintenance"));
    assert_eq!(obj.len(), 1, "only the present field survives: {v}");
}

/// Display comes from strum; every variant must render exactly what serde emits.
#[test]
fn trade_side_strum_display_matches_serde_emission() {
    use super::TradeSide;
    for v in [TradeSide::Buy, TradeSide::Sell, TradeSide::Unknown] {
        assert_eq!(json!(v), json!(v.to_string()));
    }
}
