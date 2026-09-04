//! Account-namespace unit tests.

use super::*;
use crate::api::trade::Side;
use crate::auth::{AuthStack, SpotRestHmacSha512Signer, SystemClockNonceSource};
use crate::transport::{HttpTransport, TransportError};
use crate::types::{ApiKey, ApiSecret, AuthProfile};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::json;
use std::sync::Mutex;

struct CapturingMock {
    canned: serde_json::Value,
    last_post: Mutex<Option<CapturedPost>>,
}

#[derive(Debug, Clone)]
struct CapturedPost {
    path: String,
    body: String,
    api_key_header: String,
    api_sign_header: String,
}

#[async_trait::async_trait]
impl HttpTransport for CapturingMock {
    async fn get_json(
        &self,
        _path: &str,
        _query_params: &[(&str, &str)],
    ) -> Result<serde_json::Value, TransportError> {
        panic!("account-namespace tests never hit GET");
    }

    async fn post_form_signed(
        &self,
        path: &str,
        body: &str,
        api_key_header: &str,
        api_sign_header: &str,
    ) -> Result<serde_json::Value, TransportError> {
        *self.last_post.lock().unwrap() = Some(CapturedPost {
            path: path.into(),
            body: body.into(),
            api_key_header: api_key_header.into(),
            api_sign_header: api_sign_header.into(),
        });
        Ok(self.canned.clone())
    }
}

fn make_namespace_with_creds(canned: serde_json::Value) -> (AccountNamespace, Arc<CapturingMock>) {
    let api_key = ApiKey::new("test-api-key-1234567890");
    let secret = ApiSecret::from_base64(&BASE64.encode(vec![0x01u8; 32])).unwrap();
    let signer = SpotRestHmacSha512Signer::new(api_key.clone(), secret);
    let mut signers: HashMap<AuthProfile, Arc<dyn crate::auth::AuthSigner>> = HashMap::new();
    signers.insert(AuthProfile::SpotV1, Arc::new(signer));
    let auth = Arc::new(AuthStack::new(
        Some(api_key),
        None,
        Arc::new(SystemClockNonceSource::new()),
        signers,
        crate::auth::TokenLifecycleManager::for_test(),
    ));
    make_namespace(canned, auth)
}

fn make_namespace_no_creds(canned: serde_json::Value) -> (AccountNamespace, Arc<CapturingMock>) {
    let auth = Arc::new(AuthStack::new(
        None,
        None,
        Arc::new(SystemClockNonceSource::new()),
        HashMap::new(),
        crate::auth::TokenLifecycleManager::for_test(),
    ));
    make_namespace(canned, auth)
}

fn make_namespace(
    canned: serde_json::Value,
    auth: Arc<AuthStack>,
) -> (AccountNamespace, Arc<CapturingMock>) {
    let (ns, mock, _bus) = make_namespace_with_bus(canned, auth);
    (ns, mock)
}

fn make_namespace_with_bus(
    canned: serde_json::Value,
    auth: Arc<AuthStack>,
) -> (
    AccountNamespace,
    Arc<CapturingMock>,
    Arc<crate::dispatch::DispatchEventBus>,
) {
    let mock = Arc::new(CapturingMock {
        canned,
        last_post: Mutex::new(None),
    });
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
        Arc::clone(&mock) as Arc<dyn HttpTransport>,
        auth,
        api_rl,
        trading_rl,
        clock,
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
    (AccountNamespace::new(rest, ws, dispatch), mock, bus)
}

#[tokio::test]
async fn balance_hits_canonical_path_with_signed_headers() {
    let (account, mock) = make_namespace_with_creds(json!({
        "error": [],
        "result": {
            "XXBT": "1.50000000",
            "ZUSD": "10000.50"
        }
    }));

    let _balance = account.balance().await.unwrap();

    let captured = mock.last_post.lock().unwrap().clone().unwrap();
    assert_eq!(captured.path, "/0/private/Balance");
    assert!(
        captured.body.starts_with("nonce="),
        "body should embed nonce — got {}",
        captured.body
    );
    assert_eq!(captured.api_key_header, "test-api-key-1234567890");
    assert_eq!(captured.api_sign_header.len(), 88); // HMAC-SHA512 base64
}

#[tokio::test]
async fn balance_parses_per_asset_decimals() {
    let (account, _mock) = make_namespace_with_creds(json!({
        "error": [],
        "result": {
            "XXBT": "1.50000000",
            "ZUSD": "10000.50"
        }
    }));

    let balance = account.balance().await.unwrap();

    assert_eq!(balance.assets.len(), 2);
    assert_eq!(
        balance.assets.get("BTC"),
        Some(&"1.50000000".parse::<Decimal>().unwrap())
    );
    assert_eq!(
        balance.assets.get("USD"),
        Some(&"10000.50".parse::<Decimal>().unwrap())
    );
    assert_eq!(balance.assets.get("XXBT"), None);
    assert_eq!(balance.assets.get("ZUSD"), None);
}

#[tokio::test]
async fn extended_balance_normalises_asset_keys() {
    let (account, _mock) = make_namespace_with_creds(json!({
        "error": [],
        "result": {
            "XXBT": { "balance": "1.5", "hold_trade": "0" },
            "ZUSD": { "balance": "100", "hold_trade": "0" }
        }
    }));

    let result = account.extended_balance().await.unwrap();

    assert!(result.assets.contains_key("BTC"));
    assert!(result.assets.contains_key("USD"));
    assert!(!result.assets.contains_key("XXBT"));
    assert!(!result.assets.contains_key("ZUSD"));
}

#[tokio::test]
async fn ledgers_normalises_entry_asset() {
    let (account, _mock) = make_namespace_with_creds(json!({
        "error": [],
        "result": {
            "ledger": {
                "L1": {
                    "refid": "RID-1",
                    "time": 1716800000.0_f64,
                    "type": "trade",
                    "subtype": "",
                    "aclass": "currency",
                    "asset": "ZUSD",
                    "amount": "-100.0",
                    "fee": "0.16",
                    "balance": "9900.0"
                }
            },
            "count": 1
        }
    }));

    let result = account.ledgers(LedgersRequest::default()).await.unwrap();

    assert_eq!(result.ledger.get("L1").unwrap().asset.as_str(), "USD");
}

#[tokio::test]
async fn query_ledgers_normalises_entry_asset() {
    let (account, _mock) = make_namespace_with_creds(json!({
        "error": [],
        "result": {
            "L1": {
                "refid": "RID-2",
                "time": 1716800000.0_f64,
                "type": "trade",
                "subtype": "",
                "aclass": "currency",
                "asset": "XXBT",
                "amount": "0.5",
                "fee": "0.001",
                "balance": "1.5"
            }
        }
    }));

    let result = account.query_ledgers(vec!["L1".to_string()]).await.unwrap();

    assert_eq!(result.get("L1").unwrap().asset.as_str(), "BTC");
}

#[tokio::test]
async fn balance_without_credentials_surfaces_auth_error() {
    let (account, _mock) = make_namespace_no_creds(json!({"error": [], "result": {}}));

    let err = account.balance().await.unwrap_err();
    match err {
        AccountError::Unknown { kraken_code, .. } => assert_eq!(kraken_code, "AUTH"),
        other => panic!("expected Unknown(AUTH), got {:?}", other),
    }
}

#[tokio::test]
async fn balance_surfaces_kraken_errors() {
    let (account, _mock) = make_namespace_with_creds(json!({
        "error": ["EAPI:Invalid nonce"],
        "result": {}
    }));
    let err = account.balance().await.unwrap_err();
    {
        use crate::error::ApiError;
        let rid = err
            .request_id()
            .expect("kraken-rejected call carries the dispatch id");
        assert!(
            uuid::Uuid::parse_str(rid).is_ok(),
            "correlation id is a UUID"
        );
    }
    match err {
        AccountError::Unknown {
            kraken_code,
            kraken_message,
            ..
        } => {
            assert_eq!(kraken_code, "EAPI");
            assert_eq!(kraken_message, "EAPI:Invalid nonce");
        }
        other => panic!("expected Unknown, got {:?}", other),
    }
}

#[tokio::test]
async fn balance_decode_failure_carries_request_id() {
    let (account, _mock) = make_namespace_with_creds(json!({
        "error": [],
        "result": []
    }));
    let err = account.balance().await.unwrap_err();
    assert!(matches!(err, AccountError::MalformedResponse { .. }));
    use crate::error::ApiError;
    let rid = err
        .request_id()
        .expect("decode error carries the dispatch id");
    assert!(
        uuid::Uuid::parse_str(rid).is_ok(),
        "correlation id is a UUID"
    );
}

#[tokio::test]
async fn closed_orders_posts_trades_true() {
    let (account, mock) = make_namespace_with_creds(json!({
        "error": [],
        "result": { "closed": {}, "count": 0 }
    }));

    account
        .closed_orders(ClosedOrdersRequest::default().trades(true))
        .await
        .unwrap();

    let captured = mock.last_post.lock().unwrap().clone().unwrap();
    assert_eq!(captured.path, "/0/private/ClosedOrders");
    assert!(
        captured.body.contains("trades=true"),
        "body should request inline fills — got {}",
        captured.body
    );
}

#[tokio::test]
async fn closed_orders_posts_trades_false() {
    let (account, mock) = make_namespace_with_creds(json!({
        "error": [],
        "result": { "closed": {}, "count": 0 }
    }));

    account
        .closed_orders(ClosedOrdersRequest::default())
        .await
        .unwrap();

    let captured = mock.last_post.lock().unwrap().clone().unwrap();
    assert_eq!(captured.path, "/0/private/ClosedOrders");
    assert!(
        captured.body.contains("trades=false"),
        "body should not request inline fills — got {}",
        captured.body
    );
}

#[tokio::test]
async fn closed_orders_posts_userref_filter() {
    let (account, mock) = make_namespace_with_creds(json!({
        "error": [],
        "result": { "closed": {}, "count": 0 }
    }));

    account
        .closed_orders(ClosedOrdersRequest::default().userref(-7))
        .await
        .unwrap();

    let body = mock.last_post.lock().unwrap().clone().unwrap().body;
    assert!(
        body.contains("userref=-7"),
        "body should carry the signed userref filter — got {body}"
    );
}

/// `ledgers=true` goes on the wire; the default (false) omits the key entirely.
#[tokio::test]
async fn trades_history_posts_ledgers_only_when_requested() {
    let fixture = json!({
        "error": [],
        "result": { "trades": {}, "count": 0 }
    });

    let (account, mock) = make_namespace_with_creds(fixture.clone());
    account
        .trades_history(TradesHistoryRequest::default().ledgers(true))
        .await
        .unwrap();
    let body = mock.last_post.lock().unwrap().clone().unwrap().body;
    assert!(
        body.contains("ledgers=true"),
        "body should request inlined ledger ids — got {body}"
    );

    let (account2, mock2) = make_namespace_with_creds(fixture);
    account2
        .trades_history(TradesHistoryRequest::default())
        .await
        .unwrap();
    let body2 = mock2.last_post.lock().unwrap().clone().unwrap().body;
    assert!(
        !body2.contains("ledgers"),
        "default must omit the ledgers key — got {body2}"
    );
}

mod decode {
    use super::super::types::*;
    use crate::api::trade::{OrderType, Side, TimeInForce};
    use rust_decimal::Decimal;
    use serde_json::json;

    fn dec(s: &str) -> Decimal {
        s.parse::<Decimal>().unwrap()
    }

    #[test]
    fn balance_ex_entry_omits_credit_for_plain_spot_key() {
        let e: ExtendedBalanceEntry =
            serde_json::from_value(json!({ "balance": "1.5", "hold_trade": "0.25" })).unwrap();
        assert_eq!(e.balance, dec("1.5"));
        assert_eq!(e.hold_trade, dec("0.25"));
        assert_eq!(e.credit, None);
        assert_eq!(e.credit_used, None);
    }

    #[test]
    fn balance_ex_entry_decodes_credit_for_credit_key() {
        let e: ExtendedBalanceEntry = serde_json::from_value(json!({
            "balance": "1.5", "hold_trade": "0.25", "credit": "100", "credit_used": "10"
        }))
        .unwrap();
        assert_eq!(e.credit, Some(dec("100")));
        assert_eq!(e.credit_used, Some(dec("10")));
    }

    #[test]
    fn trade_balance_renames_short_keys_and_tolerates_missing_margin_fields() {
        let tb: TradeBalance = serde_json::from_value(json!({
            "eb": "1000", "tb": "900", "m": "0", "n": "0", "c": "0",
            "v": "0", "e": "1000", "mf": "900"
        }))
        .unwrap();
        assert_eq!(tb.equivalent_balance, dec("1000"));
        assert_eq!(tb.trade_balance, dec("900"));
        assert_eq!(tb.free_margin, dec("900"));
        assert_eq!(tb.margin_level_pct, None);
        assert_eq!(tb.unrealized_value, None);
        assert_eq!(tb.free_margin_original, None);
    }

    #[test]
    fn trade_balance_decodes_margin_only_fields_when_present() {
        let tb: TradeBalance = serde_json::from_value(json!({
            "eb": "1000", "tb": "900", "m": "50", "n": "5", "c": "100",
            "v": "105", "e": "1005", "mf": "850", "ml": "2010", "uv": "5", "mfo": "850"
        }))
        .unwrap();
        assert_eq!(tb.margin_amount, dec("50"));
        assert_eq!(tb.unrealized_pnl, dec("5"));
        assert_eq!(tb.margin_level_pct, Some(dec("2010")));
        assert_eq!(tb.unrealized_value, Some(dec("5")));
        assert_eq!(tb.free_margin_original, Some(dec("850")));
    }

    #[test]
    fn order_info_open_row_decodes_descr_and_first_class_fields() {
        let oi: OrderInfo = serde_json::from_value(json!({
            "refid": null,
            "userref": 0,
            "cl_ord_id": "my-order-1",
            "status": "open",
            "opentm": 1716800000.123_f64,
            "descr": {
                "pair": "XBTUSD",
                "aclass": "forex",
                "type": "buy",
                "ordertype": "limit",
                "price": "50000.0",
                "price2": "0",
                "leverage": "none",
                "order": "buy 0.5 XBTUSD @ limit 50000.0",
                "close": ""
            },
            "vol": "0.5",
            "vol_exec": "0",
            "cost": "0",
            "fee": "0",
            "price": "0",
            "misc": "",
            "oflags": "fciq",
            "time_in_force": "gtc"
        }))
        .unwrap();
        assert_eq!(oi.status, OrderStatus::Open);
        assert_eq!(
            oi.cl_ord_id.as_ref().map(|c| c.as_str()),
            Some("my-order-1")
        );
        assert_eq!(oi.time_in_force, TimeInForce::Gtc);
        assert_eq!(oi.descr.side, Side::Buy);
        assert_eq!(oi.descr.ordertype, OrderType::Limit);
        assert_eq!(oi.descr.leverage, "none");
        assert_eq!(oi.descr.price, dec("50000.0"));
        assert_eq!(oi.trades, None);
        assert_eq!(oi.closetm, None);
        assert_eq!(oi.reason, None);
        assert_eq!(oi.oflags, "fciq");
        assert_eq!(oi.stopprice, None);
        assert_eq!(oi.limitprice, None);
    }

    // descr carries `aclass`; `close` is `""` (present, not absent); `leverage`
    // is the string `"none"`; price/price2/stopprice/limitprice are `"0"`/`"0.00000"` strings.
    #[test]
    fn order_descr_locked_against_live_openorders_wire_2026_06_04() {
        let oi: OrderInfo = serde_json::from_value(json!({
            "cl_ord_id": "9ce3b826-7c70-4bb6-a89e-d6370e5378f6",
            "cost": "0.00000",
            "descr": {
                "aclass": "forex", "close": "", "leverage": "none",
                "order": "buy 0.00010000 XBTUSDC @ limit 20000.00",
                "ordertype": "limit", "pair": "XBTUSDC",
                "price": "20000.00", "price2": "0", "type": "buy"
            },
            "expiretm": 0, "fee": "0.00000", "limitprice": "0.00000", "misc": "",
            "oflags": "post,fciq", "opentm": 1780582233.729133_f64, "price": "0.00000",
            "refid": null, "starttm": 0, "status": "open", "stopprice": "0.00000",
            "time_in_force": "gtc", "userref": null,
            "vol": "0.00010000", "vol_exec": "0.00000000"
        }))
        .unwrap();
        assert_eq!(oi.descr.aclass, "forex");
        assert_eq!(oi.descr.close, "");
        assert_eq!(oi.descr.leverage, "none");
        assert_eq!(oi.descr.pair, "XBTUSDC");
        assert_eq!(oi.descr.side, Side::Buy);
        assert_eq!(oi.descr.ordertype, OrderType::Limit);
        assert_eq!(oi.descr.price, dec("20000.00"));
        assert_eq!(oi.descr.price2, dec("0"));
        assert_eq!(oi.oflags, "post,fciq");
        assert_eq!(oi.stopprice, Some(dec("0.00000")));
        assert_eq!(oi.limitprice, Some(dec("0.00000")));
        assert_eq!(oi.closetm, None);
        assert_eq!(oi.trades, None);
    }

    #[test]
    fn order_info_decodes_stopprice_and_limitprice_when_present() {
        let oi: OrderInfo = serde_json::from_value(json!({
            "status": "open",
            "opentm": 1716800000.0_f64,
            "descr": {
                "pair": "XBTUSD", "aclass": "forex", "type": "sell", "ordertype": "stop-loss-limit",
                "price": "12000.0", "price2": "11500.0", "leverage": "none",
                "order": "sell 0.5 XBTUSD @ stop loss 12000.0 -> limit 11500.0"
            },
            "vol": "0.5",
            "vol_exec": "0",
            "cost": "0",
            "fee": "0",
            "price": "0",
            "stopprice": "12000.0",
            "limitprice": "11500.0",
            "misc": "",
            "oflags": "fciq"
        }))
        .unwrap();
        assert_eq!(oi.stopprice, Some(dec("12000.0")));
        assert_eq!(oi.limitprice, Some(dec("11500.0")));
    }

    /// A present-but-non-string `trigger` must NOT sink the order row (lenient
    /// contract); it maps to `None`. A valid string still decodes to `Some`.
    #[test]
    fn order_info_non_string_trigger_survives() {
        let base = json!({
            "status": "open", "opentm": 1716800000.0_f64,
            "descr": { "pair": "XBTUSD", "aclass": "forex", "type": "sell",
                "ordertype": "stop-loss", "price": "12000.0", "leverage": "none",
                "order": "sell 0.5 XBTUSD @ stop loss 12000.0" },
            "vol": "0.5", "vol_exec": "0", "cost": "0", "fee": "0", "price": "0",
            "misc": "", "oflags": "fciq"
        });
        let mut bad = base.clone();
        bad["trigger"] = json!(42);
        let oi: OrderInfo =
            serde_json::from_value(bad).expect("non-string trigger must not sink the row");
        assert_eq!(oi.trigger, None);

        let mut good = base;
        good["trigger"] = json!("index");
        let oi2: OrderInfo = serde_json::from_value(good).unwrap();
        assert!(oi2.trigger.is_some(), "valid string trigger decodes");
    }

    #[test]
    fn order_info_closed_row_decodes_closetm_reason_and_trades() {
        let oi: OrderInfo = serde_json::from_value(json!({
            "status": "closed",
            "opentm": 1716800000.0_f64,
            "closetm": 1716800100.5_f64,
            "reason": "User requested",
            "descr": {
                "pair": "XBTUSD", "aclass": "forex", "type": "sell", "ordertype": "stop-loss-limit",
                "price": "49000.0", "price2": "48000.0", "leverage": "5:1",
                "order": "sell 0.5 XBTUSD @ stop loss 49000.0 -> limit 48000.0"
            },
            "vol": "0.5",
            "vol_exec": "0.5",
            "cost": "24500",
            "fee": "12.25",
            "price": "49000",
            "misc": "",
            "oflags": "fciq",
            "trades": ["TX-A", "TX-B"]
        }))
        .unwrap();
        assert_eq!(oi.status, OrderStatus::Closed);
        assert_eq!(
            oi.closetm.as_ref().map(KrakenTimestamp::as_str),
            Some("1716800100.5")
        );
        assert_eq!(oi.reason.as_deref(), Some("User requested"));
        assert_eq!(oi.descr.ordertype, OrderType::StopLossLimit);
        assert_eq!(oi.descr.leverage, "5:1");
        let trades = oi.trades.expect("vol_exec > 0 → trades present");
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].as_str(), "TX-A");
        assert_eq!(oi.time_in_force, TimeInForce::Gtc);
    }

    #[test]
    fn kraken_timestamp_preserves_verbatim_wire_digits() {
        let raw = "1716800100.123456789012345";
        let ts: KrakenTimestamp = serde_json::from_str(raw).unwrap();
        assert_eq!(ts.as_str(), raw);
        assert_ne!(ts.as_str(), raw.parse::<f64>().unwrap().to_string());
        assert!(ts.to_f64().is_some());
    }

    #[test]
    fn kraken_timestamp_tolerates_string_wire_form() {
        let ts: KrakenTimestamp = serde_json::from_str("\"1716800100.5\"").unwrap();
        assert_eq!(ts.as_str(), "1716800100.5");
    }

    // Verbatim digits as a JSON STRING, even though the wire carried a number.
    // Newtypes emit as their bare inner string.
    #[test]
    fn kraken_timestamp_serializes_verbatim_digits_as_json_string() {
        let ts: KrakenTimestamp = serde_json::from_str("1716800000.123").unwrap();
        let v = serde_json::to_value(&ts).unwrap();
        assert_eq!(v, json!("1716800000.123"));
        assert!(v.is_string(), "must be a JSON string, not a number");

        let sym = crate::types::Symbol::new("BTC/USD").unwrap();
        assert_eq!(serde_json::to_value(&sym).unwrap(), json!("BTC/USD"));
        let asset = crate::types::AssetCode::from_wire("XXBT");
        assert_eq!(serde_json::to_value(&asset).unwrap(), json!("BTC"));
    }

    #[test]
    fn trade_history_entry_spot_row_anomalies() {
        let t: TradeHistoryEntry = serde_json::from_value(json!({
            "ordertxid": "OID-1",
            "postxid": "TKH0000-0000-00000",
            "pair": "XBTUSDC",
            "aclass": "forex",
            "time": 1716800000.5_f64,
            "type": "buy",
            "ordertype": "limit",
            "tradeordertype": "market",
            "price": "50000.0",
            "cost": "25000",
            "fee": "12.5",
            "vol": "0.5",
            "margin": "0.00000",
            "leverage": "0",
            "misc": "",
            "trade_id": 123456789_u64,
            "maker": true
        }))
        .unwrap();
        assert_eq!(t.ordertxid.as_str(), "OID-1");
        assert_eq!(t.aclass, "forex");
        assert_eq!(t.side, Side::Buy);
        assert_eq!(t.ordertype, OrderType::Limit);
        assert_eq!(t.tradeordertype, OrderType::Market);
        assert_eq!(t.trade_id, 123_456_789);
        assert!(t.maker);
        assert_eq!(t.leverage, dec("0"));
        assert_eq!(t.posstatus, None);
        assert_eq!(t.ledgers, None);
    }

    /// `ledgers=true` inlines an array of ledger-entry ids per row (two legs
    /// per spot fill).
    #[test]
    fn trade_history_entry_decodes_inlined_ledgers() {
        let t: TradeHistoryEntry = serde_json::from_value(json!({
            "ordertxid": "OSGGLD-7L567-4KBIUT", "postxid": "TKH2SE-M7IF5-CFI7LT",
            "pair": "XBTUSDC", "aclass": "forex", "time": 1782587828.681409_f64,
            "type": "sell", "ordertype": "market", "tradeordertype": "market",
            "price": "60508.75000", "cost": "3.63053", "fee": "0.02904",
            "vol": "0.00006000", "margin": "0.00000", "leverage": "0", "misc": "",
            "trade_id": 6820254_u64, "maker": false,
            "ledgers": ["LZWWII-S4VAD-FTRBPE", "LTKOJ5-JWDZ4-3EQOER"]
        }))
        .unwrap();
        assert_eq!(
            t.ledgers.as_deref(),
            Some(
                &[
                    "LZWWII-S4VAD-FTRBPE".to_string(),
                    "LTKOJ5-JWDZ4-3EQOER".to_string()
                ][..]
            )
        );
    }

    #[test]
    fn trade_history_entry_margin_row_has_posstatus() {
        let t: TradeHistoryEntry = serde_json::from_value(json!({
            "ordertxid": "OID-2", "postxid": "POS-2", "posstatus": "open",
            "pair": "XBTUSDC", "aclass": "forex", "time": 1.0_f64, "type": "sell",
            "ordertype": "limit", "tradeordertype": "limit", "price": "1", "cost": "1",
            "fee": "0", "vol": "1", "margin": "0.5", "leverage": "2", "misc": "",
            "trade_id": 7_u64, "maker": false
        }))
        .unwrap();
        assert_eq!(t.posstatus, Some(PositionStatus::Open));
        assert_eq!(t.leverage, dec("2"));
        assert!(!t.maker);
    }

    #[test]
    fn ledger_entry_decodes_type_enum_and_optional_subtype() {
        let l: LedgerEntry = serde_json::from_value(json!({
            "refid": "RID-1",
            "time": 1716800000.0_f64,
            "type": "trade",
            "subtype": "",
            "aclass": "currency",
            "asset": "ZUSD",
            "amount": "-100.0",
            "fee": "0.16",
            "balance": "9900.0"
        }))
        .unwrap();
        assert_eq!(l.ledger_type, LedgerType::Trade);
        assert_eq!(l.subtype.as_deref(), Some(""));
        assert_eq!(l.amount, dec("-100.0"));
        let l2: LedgerEntry = serde_json::from_value(json!({
            "refid": "RID-2", "time": 1.0_f64, "type": "rollover", "aclass": "currency",
            "asset": "XXBT", "amount": "0", "fee": "0", "balance": "1"
        }))
        .unwrap();
        assert_eq!(l2.ledger_type, LedgerType::Rollover);
        assert_eq!(l2.subtype, None);
    }

    #[test]
    fn open_position_entry_class_key_and_rollovertm_string() {
        let p: OpenPositionEntry = serde_json::from_value(json!({
            "ordertxid": "OID-3",
            "posstatus": "open",
            "pair": "XBTUSDC",
            "class": "forex",
            "time": 1716800000.0_f64,
            "type": "buy",
            "ordertype": "limit",
            "cost": "25000",
            "fee": "12.5",
            "vol": "0.5",
            "vol_closed": "0.00000000",
            "margin": "5000",
            "terms": "0.0100% per 4 hours",
            "rollovertm": "1716803600",
            "misc": "",
            "oflags": ""
        }))
        .unwrap();
        assert_eq!(p.posstatus, PositionStatus::Open);
        assert_eq!(p.asset_class, "forex");
        assert_eq!(p.rollovertm, "1716803600");
        assert_eq!(p.terms, "0.0100% per 4 hours");
        assert_eq!(p.side, Side::Buy);
    }

    #[test]
    fn open_positions_snapshot_is_a_bare_map() {
        let raw = json!({
            "TPOS-1": {
                "ordertxid": "O-1", "posstatus": "open", "pair": "XBTUSDC",
                "class": "forex", "time": 1.0_f64, "type": "buy", "ordertype": "market",
                "cost": "1", "fee": "0", "vol": "1", "vol_closed": "0", "margin": "0.5",
                "terms": "x", "rollovertm": "0", "misc": "", "oflags": ""
            }
        });
        let obj = raw.as_object().unwrap();
        let mut positions = std::collections::HashMap::new();
        for (id, row) in obj {
            positions.insert(
                id.clone(),
                serde_json::from_value::<OpenPositionEntry>(row.clone()).unwrap(),
            );
        }
        let snap = OpenPositions { positions };
        assert_eq!(snap.positions.len(), 1);
        assert!(snap.positions.contains_key("TPOS-1"));
    }

    #[test]
    fn lifecycle_position_serializes_lowercase() {
        assert_eq!(
            serde_json::to_value(LifecyclePosition::Open).unwrap(),
            json!("open")
        );
        assert_eq!(
            serde_json::to_value(LifecyclePosition::Closed).unwrap(),
            json!("closed")
        );
    }

    // Cross-binding event wire form: snake_case tags, named `found`.
    #[test]
    fn reconciliation_outcome_serializes_snake_case_named_form() {
        let found = ReconciliationOutcome::Found {
            txid: crate::types::TxId::new("TX-1"),
            status: OrderStatus::Closed,
            lifecycle: LifecyclePosition::Closed,
        };
        let v = serde_json::to_value(&found).unwrap();
        assert_eq!(v["found"]["txid"], "TX-1");
        assert_eq!(v["found"]["status"], "closed");
        assert_eq!(v["found"]["lifecycle"], "closed");
        assert_eq!(
            serde_json::to_value(ReconciliationOutcome::NotPlaced).unwrap(),
            json!("not_placed")
        );
        assert_eq!(
            serde_json::to_value(ReconciliationOutcome::Unknown).unwrap(),
            json!("unknown")
        );
    }

    // Wire keys never serialize; absent margin optionals are omitted (not null).
    #[test]
    fn trade_balance_serializes_descriptive_names_and_omits_absent_margin() {
        let tb: TradeBalance = serde_json::from_value(json!({
            "eb": "100.5", "tb": "90.1", "m": "0", "n": "0", "c": "0",
            "v": "0", "e": "90.1", "mf": "90.1"
        }))
        .unwrap();
        let v = serde_json::to_value(&tb).unwrap();
        let obj = v.as_object().unwrap();
        for name in [
            "equivalent_balance",
            "trade_balance",
            "margin_amount",
            "unrealized_pnl",
            "cost_basis",
            "floating_valuation",
            "equity",
            "free_margin",
        ] {
            assert!(
                obj.contains_key(name),
                "descriptive key {name} must serialize"
            );
        }
        for wire in ["eb", "tb", "m", "n", "c", "v", "e", "mf", "ml", "uv", "mfo"] {
            assert!(
                !obj.contains_key(wire),
                "wire key {wire} must not serialize"
            );
        }
        for absent in [
            "margin_level_pct",
            "unrealized_value",
            "free_margin_original",
        ] {
            assert!(!obj.contains_key(absent), "{absent} is None -> omitted");
        }

        let with_margin: TradeBalance = serde_json::from_value(json!({
            "eb": "100.5", "tb": "90.1", "m": "0", "n": "0", "c": "0",
            "v": "0", "e": "90.1", "mf": "90.1", "ml": "150.0"
        }))
        .unwrap();
        let v = serde_json::to_value(&with_margin).unwrap();
        assert!(v.as_object().unwrap().contains_key("margin_level_pct"));
    }
}

mod find_order {
    use super::*;
    use crate::api::account::{LifecyclePosition, ReconciliationOutcome};
    use crate::dispatch::{EventPayload, EventType};
    use crate::transport::TransportErrorKind;
    use crate::types::ClOrdId;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock routing by request path; records per-path hits.
    /// An optional per-path error injects a transport failure or canned envelope.
    struct RoutingMock {
        open_orders: serde_json::Value,
        closed_orders: serde_json::Value,
        /// When set, the ClosedOrders leg returns this transport error instead.
        closed_transport_err: Option<TransportError>,
        open_hits: AtomicUsize,
        closed_hits: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl HttpTransport for RoutingMock {
        async fn get_json(
            &self,
            _path: &str,
            _query: &[(&str, &str)],
        ) -> Result<serde_json::Value, TransportError> {
            panic!("walk never hits GET");
        }
        async fn post_form_signed(
            &self,
            path: &str,
            _body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<serde_json::Value, TransportError> {
            if path == "/0/private/OpenOrders" {
                self.open_hits.fetch_add(1, Ordering::SeqCst);
                Ok(self.open_orders.clone())
            } else if path == "/0/private/ClosedOrders" {
                self.closed_hits.fetch_add(1, Ordering::SeqCst);
                if let Some(e) = &self.closed_transport_err {
                    return Err(e.clone());
                }
                Ok(self.closed_orders.clone())
            } else {
                panic!("unexpected path in walk: {path}");
            }
        }
    }

    fn build_walk_rest(
        mock: Arc<RoutingMock>,
    ) -> (Arc<RestSurface>, Arc<crate::dispatch::DispatchEventBus>) {
        let api_key = ApiKey::new("test-api-key-1234567890");
        let secret = ApiSecret::from_base64(&BASE64.encode(vec![0x01u8; 32])).unwrap();
        let signer = SpotRestHmacSha512Signer::new(api_key.clone(), secret);
        let mut signers: HashMap<AuthProfile, Arc<dyn crate::auth::AuthSigner>> = HashMap::new();
        signers.insert(AuthProfile::SpotV1, Arc::new(signer));
        let auth = Arc::new(AuthStack::new(
            Some(api_key),
            None,
            Arc::new(SystemClockNonceSource::new()),
            signers,
            crate::auth::TokenLifecycleManager::for_test(),
        ));
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
        let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let knobs = Arc::new(crate::build::knobs::Knobs::defaults());
        let api_rl = Arc::new(crate::rate_limit::SpotApiRateLimitTracker::new(
            crate::rate_limit::Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::clone(&knobs),
        ));
        let trading_rl = Arc::new(crate::rate_limit::SpotTradingRateLimitTracker::new(
            crate::rate_limit::Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::clone(&knobs),
        ));
        // Zero-jitter retry engine — walk tests stay deterministic and fast.
        let engine = crate::rest::retry::RetryEngine::from_knobs(
            &knobs,
            Arc::new(crate::jitter::FixedJitter(0.0)),
        );
        let cl_ord_id_index = Arc::new(crate::rate_limit::ClOrdIdPairIndex::new(1024));
        let rest = Arc::new(RestSurface::new_with_index(
            Arc::clone(&mock) as Arc<dyn HttpTransport>,
            auth,
            api_rl,
            trading_rl,
            clock,
            cl_ord_id_index,
            engine,
            std::time::Duration::from_secs(30),
        ));
        rest.set_bus(Arc::clone(&bus));
        (rest, bus)
    }

    fn order_map(wrapper: &str, txid: &str, status: &str) -> serde_json::Value {
        serde_json::json!({ "error": [], "result": { wrapper: { txid: { "status": status } } } })
    }
    fn empty_map(wrapper: &str) -> serde_json::Value {
        serde_json::json!({ "error": [], "result": { wrapper: {} } })
    }

    /// Subscribe a counting capture for `OrderReconciliationEvent`, start the
    /// reactor, and return the receiver.
    fn capture_recon_events(
        bus: &Arc<crate::dispatch::DispatchEventBus>,
    ) -> tokio::sync::mpsc::Receiver<EventPayload> {
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let (tx, rx) = tokio::sync::mpsc::channel::<EventPayload>(8);
        let _ = bus.subscribe(
            EventType::OrderReconciliationEvent,
            Arc::new(move |env| {
                let _ = tx.try_send(env.payload.clone());
            }),
            1,
        );
        rx
    }

    #[tokio::test]
    async fn open_match_short_circuits_to_found_open_without_calling_closed() {
        let mock = Arc::new(RoutingMock {
            open_orders: order_map("open", "TX-OPEN", "open"),
            closed_orders: empty_map("closed"),
            closed_transport_err: None,
            open_hits: AtomicUsize::new(0),
            closed_hits: AtomicUsize::new(0),
        });
        let (rest, _bus) = build_walk_rest(Arc::clone(&mock));
        let outcome = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            rest.find_order_by_cl_ord_id(&ClOrdId::allocate_v4()),
        )
        .await
        .expect("walk timed out")
        .unwrap();
        match outcome {
            ReconciliationOutcome::Found {
                txid,
                status,
                lifecycle,
            } => {
                assert_eq!(txid.as_str(), "TX-OPEN");
                assert_eq!(status, OrderStatus::Open);
                assert_eq!(lifecycle, LifecyclePosition::Open);
            }
            other => panic!("expected Found(Open), got {other:?}"),
        }
        assert_eq!(mock.open_hits.load(Ordering::SeqCst), 1);
        assert_eq!(mock.closed_hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn open_empty_then_closed_match_is_found_closed() {
        let mock = Arc::new(RoutingMock {
            open_orders: empty_map("open"),
            closed_orders: order_map("closed", "TX-CLOSED", "closed"),
            closed_transport_err: None,
            open_hits: AtomicUsize::new(0),
            closed_hits: AtomicUsize::new(0),
        });
        let (rest, _bus) = build_walk_rest(Arc::clone(&mock));
        let outcome = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            rest.find_order_by_cl_ord_id(&ClOrdId::allocate_v4()),
        )
        .await
        .expect("walk timed out")
        .unwrap();
        match outcome {
            ReconciliationOutcome::Found {
                txid,
                status,
                lifecycle,
            } => {
                assert_eq!(txid.as_str(), "TX-CLOSED");
                assert_eq!(status, OrderStatus::Closed);
                assert_eq!(lifecycle, LifecyclePosition::Closed);
            }
            other => panic!("expected Found(Closed), got {other:?}"),
        }
        assert_eq!(mock.open_hits.load(Ordering::SeqCst), 1);
        assert_eq!(mock.closed_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn both_empty_is_not_placed() {
        let mock = Arc::new(RoutingMock {
            open_orders: empty_map("open"),
            closed_orders: empty_map("closed"),
            closed_transport_err: None,
            open_hits: AtomicUsize::new(0),
            closed_hits: AtomicUsize::new(0),
        });
        let (rest, _bus) = build_walk_rest(Arc::clone(&mock));
        let outcome = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            rest.find_order_by_cl_ord_id(&ClOrdId::allocate_v4()),
        )
        .await
        .expect("walk timed out")
        .unwrap();
        assert_eq!(outcome, ReconciliationOutcome::NotPlaced);
    }

    #[tokio::test]
    async fn closed_leg_transient_transport_error_retries_to_budget_then_err() {
        let mock = Arc::new(RoutingMock {
            open_orders: empty_map("open"),
            closed_orders: empty_map("closed"),
            closed_transport_err: Some(TransportError {
                kind: TransportErrorKind::HttpStatus { status: 503 },
                transient: true,
            }),
            open_hits: AtomicUsize::new(0),
            closed_hits: AtomicUsize::new(0),
        });
        let (rest, _bus) = build_walk_rest(Arc::clone(&mock));
        let res = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            rest.find_order_by_cl_ord_id(&ClOrdId::allocate_v4()),
        )
        .await
        .expect("walk timed out");
        assert!(
            res.is_err(),
            "transient transport error must propagate as Err after budget exhaustion, got {res:?}"
        );
        assert_eq!(
            mock.closed_hits.load(Ordering::SeqCst),
            3,
            "closed leg retried to the 3-attempt budget"
        );
    }

    #[tokio::test]
    async fn closed_leg_clean_kraken_error_propagates_as_err() {
        let mock = Arc::new(RoutingMock {
            open_orders: empty_map("open"),
            closed_orders: serde_json::json!({ "error": ["EGeneral:Invalid arguments"], "result": {} }),
            closed_transport_err: None,
            open_hits: AtomicUsize::new(0),
            closed_hits: AtomicUsize::new(0),
        });
        let (rest, _bus) = build_walk_rest(Arc::clone(&mock));
        let res = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            rest.find_order_by_cl_ord_id(&ClOrdId::allocate_v4()),
        )
        .await
        .expect("walk timed out");
        assert!(
            res.is_err(),
            "clean Kraken error must propagate as Err, got {res:?}"
        );
        let (rid, _e) = res.unwrap_err();
        assert!(uuid::Uuid::parse_str(&rid).is_ok(), "leg id is a UUID");
    }

    #[tokio::test]
    async fn emit_fires_exactly_once_with_correct_outcome_and_lowercase_lifecycle() {
        let mock = Arc::new(RoutingMock {
            open_orders: order_map("open", "TX-OPEN", "open"),
            closed_orders: empty_map("closed"),
            closed_transport_err: None,
            open_hits: AtomicUsize::new(0),
            closed_hits: AtomicUsize::new(0),
        });
        let (rest, bus) = build_walk_rest(Arc::clone(&mock));
        let mut rx = capture_recon_events(&bus);

        let cl = ClOrdId::allocate_v4();
        let returned = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            rest.find_order_by_cl_ord_id(&cl),
        )
        .await
        .expect("walk timed out")
        .unwrap();

        let payload = tokio::time::timeout(std::time::Duration::from_millis(300), rx.recv())
            .await
            .expect("event not delivered")
            .expect("channel closed");
        match payload {
            EventPayload::OrderReconciliationEvent { cl_ord_id, outcome } => {
                assert_eq!(cl_ord_id.as_str(), cl.as_str());
                assert_eq!(outcome, returned);
                match outcome {
                    ReconciliationOutcome::Found { lifecycle, .. } => {
                        assert_eq!(
                            serde_json::to_value(lifecycle).unwrap(),
                            serde_json::json!("open")
                        );
                    }
                    other => panic!("expected Found, got {other:?}"),
                }
            }
            other => panic!("expected OrderReconciliationEvent, got {other:?}"),
        }
        let second = tokio::time::timeout(std::time::Duration::from_millis(80), rx.recv()).await;
        assert!(second.is_err(), "emit must fire EXACTLY ONCE per walk");
    }

    use crate::api::trade::OrderType;

    #[tokio::test]
    async fn trades_history_decodes_snapshot_with_one_entry() {
        let (account, _mock) = make_namespace_with_creds(json!({
            "error": [],
            "result": {
                "trades": {
                    "TID-0001": {
                        "ordertxid": "OID-1",
                        "postxid": "TKH0000-0000-00000",
                        "pair": "XBTUSDC",
                        "aclass": "forex",
                        "time": 1716800000.5_f64,
                        "type": "buy",
                        "ordertype": "limit",
                        "tradeordertype": "market",
                        "price": "50000.0",
                        "cost": "25000",
                        "fee": "12.5",
                        "vol": "0.5",
                        "margin": "0.00000",
                        "leverage": "0",
                        "misc": "",
                        "trade_id": 123456789_u64,
                        "maker": true
                    }
                },
                "count": 7
            }
        }));

        let snapshot = account
            .trades_history(TradesHistoryRequest::default())
            .await
            .unwrap();

        assert_eq!(snapshot.count, 7);
        assert_eq!(snapshot.trades.len(), 1);
        let entry = snapshot.trades.get("TID-0001").expect("TID-0001 missing");
        assert_eq!(entry.pair, "XBTUSDC");
        assert_eq!(entry.side, Side::Buy);
        assert_eq!(entry.ordertype, OrderType::Limit);
        assert_eq!(entry.tradeordertype, OrderType::Market);
        assert_eq!(entry.trade_id, 123_456_789);
        assert!(entry.maker);
        assert_eq!(entry.posstatus, None);
    }

    #[tokio::test]
    async fn trades_history_malformed_missing_trades_key() {
        let (account, _mock) = make_namespace_with_creds(json!({
            "error": [],
            "result": {
                "count": 1
            }
        }));

        let err = account
            .trades_history(TradesHistoryRequest::default())
            .await
            .unwrap_err();
        match err {
            AccountError::MalformedResponse { .. } => {}
            other => panic!("expected MalformedResponse, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn trades_history_malformed_missing_count_key() {
        let (account, _mock) = make_namespace_with_creds(json!({
            "error": [],
            "result": {
                "trades": {}
            }
        }));

        let err = account
            .trades_history(TradesHistoryRequest::default())
            .await
            .unwrap_err();
        match err {
            AccountError::MalformedResponse { .. } => {}
            other => panic!("expected MalformedResponse, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn trades_history_empty_snapshot_decodes() {
        let (account, _mock) = make_namespace_with_creds(json!({
            "error": [],
            "result": {
                "trades": {},
                "count": 0
            }
        }));

        let snapshot = account
            .trades_history(TradesHistoryRequest::default())
            .await
            .unwrap();

        assert_eq!(snapshot.count, 0);
        assert!(snapshot.trades.is_empty());
    }

    /// An unrecognized `status` value on an OrderInfo row decodes to
    /// `OrderStatus::Unknown` — never errors.
    #[test]
    fn order_status_unknown_wire_value_decodes_to_unknown() {
        let oi: OrderInfo = serde_json::from_value(json!({
            "status": "some_future_status",
            "opentm": 1716800000.0_f64,
            "descr": {
                "pair": "XBTUSDC", "aclass": "forex", "type": "buy",
                "ordertype": "limit", "price": "0", "price2": "0",
                "leverage": "none", "order": "buy 0.1 XBTUSDC @ limit 50000",
                "close": ""
            },
            "vol": "0.1", "vol_exec": "0", "cost": "0", "fee": "0",
            "price": "0", "misc": "", "oflags": ""
        }))
        .unwrap();
        assert_eq!(
            oi.status,
            OrderStatus::Unknown,
            "unrecognized status must decode to OrderStatus::Unknown, not error"
        );
    }

    /// An unrecognized `type` value on a LedgerEntry row decodes to
    /// `LedgerType::Unknown` — never errors.
    #[test]
    fn ledger_type_unknown_wire_value_decodes_to_unknown() {
        let l: LedgerEntry = serde_json::from_value(json!({
            "refid": "RID-UK",
            "time": 1716800000.0_f64,
            "type": "some_future_ledger_type",
            "aclass": "currency",
            "asset": "USDC",
            "amount": "0",
            "fee": "0",
            "balance": "100"
        }))
        .unwrap();
        assert_eq!(
            l.ledger_type,
            LedgerType::Unknown,
            "unrecognized type must decode to LedgerType::Unknown, not error"
        );
    }

    /// Named ledger subtypes decode to their variant (not Unknown).
    #[test]
    fn ledger_type_named_variants_decode() {
        for (wire, want) in [
            ("conversion", LedgerType::Conversion),
            ("reward", LedgerType::Reward),
            ("dividend", LedgerType::Dividend),
            ("sale", LedgerType::Sale),
        ] {
            let got: LedgerType = serde_json::from_value(json!(wire)).unwrap();
            assert_eq!(got, want, "wire `{wire}` must decode to its named variant");
        }
    }

    /// `open_orders` sends `cl_ord_id` in the request body when set, omits it when `None`.
    #[tokio::test]
    async fn open_orders_cl_ord_id_filter_hits_the_wire() {
        let cl = "3915aaaa-0000-4000-8000-000000000001";
        let (account, mock) =
            make_namespace_with_creds(json!({"error": [], "result": {"open": {}}}));
        account
            .open_orders(
                OpenOrdersRequest::default()
                    .cl_ord_id(crate::types::ClOrdId::new(cl.to_string()).unwrap()),
            )
            .await
            .unwrap();
        let body = mock.last_post.lock().unwrap().clone().unwrap().body;
        assert!(
            body.contains(&format!("cl_ord_id={cl}")),
            "cl_ord_id in body: {body}"
        );

        let (account2, mock2) =
            make_namespace_with_creds(json!({"error": [], "result": {"open": {}}}));
        account2
            .open_orders(OpenOrdersRequest::default())
            .await
            .unwrap();
        let body2 = mock2.last_post.lock().unwrap().clone().unwrap().body;
        assert!(
            !body2.contains("cl_ord_id"),
            "no cl_ord_id when None: {body2}"
        );
    }

    /// The top-level `trigger` field decodes to `Last`/`Index`; absent or an
    /// unrecognized value is lenient `None` (never sinks the row).
    #[test]
    fn order_info_decodes_top_level_trigger() {
        use crate::api::trade::TriggerKind;
        let base = json!({
            "status": "open", "opentm": 1716800000.0_f64,
            "descr": {"pair": "XBTUSDC", "aclass": "forex", "type": "buy",
                      "ordertype": "stop-loss", "price": "70689.0", "price2": "0",
                      "leverage": "none", "order": "buy stop", "close": ""},
            "vol": "0.0001", "vol_exec": "0", "cost": "0", "fee": "0", "price": "0",
            "misc": "", "oflags": "fciq"
        });
        let mk = |trig: Option<&str>| {
            let mut v = base.clone();
            if let Some(t) = trig {
                v.as_object_mut()
                    .unwrap()
                    .insert("trigger".into(), json!(t));
            }
            serde_json::from_value::<OrderInfo>(v).unwrap()
        };
        assert_eq!(mk(Some("index")).trigger, Some(TriggerKind::Index));
        assert_eq!(mk(Some("last")).trigger, Some(TriggerKind::Last));
        assert_eq!(mk(None).trigger, None, "absent trigger -> None");
        assert_eq!(
            mk(Some("bogus")).trigger,
            None,
            "unknown trigger -> None, not error"
        );
    }

    /// OpenPositions docalcs `value`/`net` decode when present, `None` when absent.
    #[test]
    fn open_position_entry_decodes_docalcs_value_net() {
        let base = json!({
            "ordertxid": "OABC-1", "posstatus": "open", "pair": "XBTUSDC",
            "class": "forex", "time": 1716800000.0_f64, "type": "buy",
            "ordertype": "limit", "cost": "10", "fee": "0.02", "vol": "0.0001",
            "vol_closed": "0", "margin": "5", "terms": "0.01% per 4 hours",
            "rollovertm": "1716800000", "misc": "", "oflags": ""
        });
        let without: OpenPositionEntry = serde_json::from_value(base.clone()).unwrap();
        assert_eq!(without.value, None);
        assert_eq!(without.net, None);
        let mut with = base;
        let o = with.as_object_mut().unwrap();
        o.insert("value".into(), json!("10.50"));
        o.insert("net".into(), json!("0.50"));
        let p: OpenPositionEntry = serde_json::from_value(with).unwrap();
        assert_eq!(p.value, Some("10.50".parse().unwrap()));
        assert_eq!(p.net, Some("0.50".parse().unwrap()));
    }

    /// Per-fill `cost` and `fees[]` decode off a WS executions entry.
    #[test]
    fn executions_decode_cost_and_fees() {
        let out = super::ws_types::decode_executions(json!({"data": {
            "exec_type": "trade", "order_id": "OMLBFK-UK377-RI2QF4",
            "exec_id": "TS6H3N", "trade_id": 6916073, "symbol": "BTC/USDC",
            "side": "sell", "last_qty": "0.0001", "last_price": "64000",
            "cost": "6.40", "fee_usd_equiv": "0.0",
            "fees": [{"asset": "USDC", "qty": "0.10018"}]
        }}))
        .expect("trade exec decodes");
        assert_eq!(out.cost, Some("6.40".parse().unwrap()));
        let fees = out.fees.expect("fees present on a fill");
        assert_eq!(fees.len(), 1);
        assert_eq!(fees[0].asset, crate::types::AssetCode::from_wire("USDC"));
        assert_eq!(fees[0].qty, "0.10018".parse().unwrap());
    }

    /// A surprise `fees` shape must NOT sink the whole (money-touching) fill —
    /// same leniency contract as `side`/`trade_id`.
    #[test]
    fn executions_malformed_fees_do_not_drop_the_fill() {
        let out = super::ws_types::decode_executions(json!({"data": {
            "exec_type": "trade", "order_id": "O1", "trade_id": 42,
            "symbol": "BTC/USDC", "cost": "64", "fees": "unexpected"
        }}))
        .expect("non-array fees must not sink the fill");
        assert_eq!(out.order_id.as_deref(), Some("O1"));
        assert_eq!(out.cost, Some("64".parse().unwrap()));
        assert!(out.fees.is_none());

        let out = super::ws_types::decode_executions(json!({"data": {
            "exec_type": "trade", "order_id": "O2", "trade_id": 43,
            "symbol": "BTC/USDC", "cost": "64",
            "fees": [{"asset": 123, "qty": "0.1"}, {"asset": "USDC", "qty": "0.2"}]
        }}))
        .expect("a bad fee entry must not sink the fill");
        assert_eq!(out.order_id.as_deref(), Some("O2"));
        let fees = out.fees.expect("fees present");
        assert_eq!(fees.len(), 1);
        assert_eq!(fees[0].asset, crate::types::AssetCode::from_wire("USDC"));

        // All-unparseable fees decode to None (not Some([])).
        let out = super::ws_types::decode_executions(json!({"data": {
            "exec_type": "trade", "order_id": "O3", "trade_id": 44,
            "symbol": "BTC/USDC", "cost": "64",
            "fees": [{"asset": 123, "qty": "0.1"}]
        }}))
        .expect("all-malformed fees must not sink the fill");
        assert_eq!(out.order_id.as_deref(), Some("O3"));
        assert!(
            out.fees.is_none(),
            "all-unparseable fees → None, not Some([])"
        );
    }
}

mod ws_combiners {
    use super::*;
    use crate::api::subscription_types::SubscriptionError;
    use crate::dispatch::{CallerInbound, HandlerMutationOp, RegistryMutationOp};
    use crate::types::{ChannelName, WsUrl};

    fn make_ws_namespace() -> (AccountNamespace, Arc<crate::dispatch::DispatchEventBus>) {
        let auth = Arc::new(AuthStack::new(
            None,
            None,
            Arc::new(SystemClockNonceSource::new()),
            HashMap::new(),
            crate::auth::TokenLifecycleManager::for_test(),
        ));
        let (ns, _mock, bus) = make_namespace_with_bus(json!({"error": [], "result": {}}), auth);
        (ns, bus)
    }

    /// The one-call contract end to end: register posted, ONE channel-wide
    /// subscribe posted with the guard ref attached, guard drop releases.
    #[test]
    fn on_executions_for_registers_subscribes_and_guard_drop_releases() {
        let (account, bus) = make_ws_namespace();
        let mut rx = bus.take_caller_to_io_rx().expect("rx present");

        let guard = account
            .on_executions_for(|_u: &ExecutionUpdate| {})
            .expect("subscribe posts succeed on a live bus");
        assert_eq!(guard.channel(), ChannelName::Executions);

        match rx.try_recv().expect("register posted first") {
            CallerInbound::HandlerMutation {
                channel,
                op: HandlerMutationOp::Register { .. },
            } => assert_eq!(channel, ChannelName::Executions),
            other => panic!("expected HandlerMutation::Register, got {other:?}"),
        }
        let ref_id = match rx.try_recv().expect("subscribe posted second") {
            CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::RegisterBatch { entries, ref_id },
            } => {
                assert_eq!(url, WsUrl::Auth, "executions routes to the auth URL");
                assert_eq!(entries.len(), 1, "one channel-wide entry");
                assert!(
                    entries[0].pair.is_none(),
                    "channel-wide entry has pair = None"
                );
                ref_id.expect("combiner attaches its handler id as the guard ref")
            }
            other => panic!("expected a single RegisterBatch, got {other:?}"),
        };

        drop(guard);
        match rx.try_recv().expect("guard drop posts teardown") {
            CallerInbound::SubscriptionGuardDrop {
                handler_id,
                channel,
                symbols,
            } => {
                assert_eq!(handler_id, ref_id, "drop releases the registering ref");
                assert_eq!(channel, ChannelName::Executions);
                assert!(symbols.is_empty(), "channel-wide guard carries no pairs");
            }
            other => panic!("expected SubscriptionGuardDrop, got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "exactly three posts");
    }

    #[test]
    fn on_balances_for_returns_guard_and_posts_channel_wide_subscribe() {
        let (account, bus) = make_ws_namespace();
        let mut rx = bus.take_caller_to_io_rx().expect("rx present");

        let guard = account
            .on_balances_for(|_u: &BalanceUpdate| {})
            .expect("subscribe posts succeed on a live bus");
        assert_eq!(guard.channel(), ChannelName::Balances);

        let _register = rx.try_recv().expect("register posted first");
        match rx.try_recv().expect("subscribe posted second") {
            CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::RegisterBatch { entries, ref_id },
            } => {
                assert_eq!(url, WsUrl::Auth, "balances routes to the auth URL");
                assert_eq!(entries.len(), 1, "one channel-wide entry");
                assert!(
                    entries[0].pair.is_none(),
                    "channel-wide entry has pair = None"
                );
                assert!(ref_id.is_some(), "guard ref attached");
            }
            other => panic!("expected a single RegisterBatch, got {other:?}"),
        }
        drop(guard);
    }

    /// Same gate order as the market combiners: LoopDead wins ahead of closed-channel.
    #[test]
    fn combiners_after_loop_death_reject_loopdead() {
        let (account, bus) = make_ws_namespace();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::Panic,
        );
        assert!(matches!(
            account.on_executions_for(|_u: &ExecutionUpdate| {}),
            Err(SubscriptionError::LoopDead)
        ));
        assert!(matches!(
            account.on_balances_for(|_u: &BalanceUpdate| {}),
            Err(SubscriptionError::LoopDead)
        ));
    }

    /// Receiver gone with the loop alive is a clean close, not a retryable QueueFull.
    #[test]
    fn combiners_after_close_reject_client_closed() {
        let (account, bus) = make_ws_namespace();
        drop(bus.take_caller_to_io_rx().expect("rx present"));
        assert!(matches!(
            account.on_executions_for(|_u: &ExecutionUpdate| {}),
            Err(SubscriptionError::ClientClosed)
        ));
        assert!(matches!(
            account.on_balances_for(|_u: &BalanceUpdate| {}),
            Err(SubscriptionError::ClientClosed)
        ));
    }
}
