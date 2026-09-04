use super::*;
use crate::auth::{AuthStack, SpotRestHmacSha512Signer, SystemClockNonceSource};
use crate::dispatch::Transport;
use crate::transport::{HttpTransport, TransportError};
use crate::types::{ApiKey, ApiSecret, ClOrdId, Symbol};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rust_decimal::Decimal;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex;

/// A retry-disabled `RetryEngine` for the pre-retry trade tests: single attempt,
/// zero jitter, so no backoff sleep runs and single-attempt timing is preserved.
fn test_no_retry_engine() -> crate::rest::retry::RetryEngine {
    let mut knobs = crate::build::knobs::Knobs::defaults();
    knobs.rest_retry_max_attempts = 1;
    crate::rest::retry::RetryEngine::from_knobs(&knobs, Arc::new(crate::jitter::FixedJitter(0.0)))
}

struct CapturingMock {
    canned: serde_json::Value,
    last_post: Mutex<Option<(String, String)>>,
}

#[async_trait::async_trait]
impl HttpTransport for CapturingMock {
    async fn get_json(
        &self,
        _path: &str,
        _query_params: &[(&str, &str)],
    ) -> Result<serde_json::Value, TransportError> {
        panic!("trade tests never hit GET");
    }

    async fn post_form_signed(
        &self,
        path: &str,
        body: &str,
        _api_key_header: &str,
        _api_sign_header: &str,
    ) -> Result<serde_json::Value, TransportError> {
        *self.last_post.lock().unwrap() = Some((path.into(), body.into()));
        Ok(self.canned.clone())
    }
}

fn make_namespace(canned: serde_json::Value) -> (TradeNamespace, Arc<CapturingMock>) {
    let mock = Arc::new(CapturingMock {
        canned,
        last_post: Mutex::new(None),
    });
    let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
    let (auth, _bus, api_rl, trading_rl) = build_deps(&clock);
    let rest = Arc::new(RestSurface::new_with_index(
        Arc::clone(&mock) as Arc<dyn HttpTransport>,
        auth,
        api_rl,
        trading_rl,
        clock,
        Arc::new(crate::rate_limit::ClOrdIdPairIndex::new(1024)),
        test_no_retry_engine(),
        std::time::Duration::from_secs(30),
    ));
    (TradeNamespace::new(rest), mock)
}

fn dec(s: &str) -> Decimal {
    s.parse::<Decimal>().unwrap()
}

fn buy_limit(pair: &str, vol: &str, price: &str) -> OrderRequest {
    let mut req = OrderRequest::new(Symbol::new(pair).unwrap(), dec(vol), Side::Buy)
        .order_type(OrderType::Limit);
    req.price = Some(dec(price).into());
    req
}

/// Base `AddOrderWsView` with every optional field defaulted to None/false/empty.
/// Call sites use functional-record-update to override only their meaningful fields:
/// `AddOrderWsView { price: &p, ..base_add_view("buy", &sym, &vol, OrderType::Limit) }`.
fn base_add_view<'a>(
    side: &'a str,
    pair: &'a Symbol,
    volume: &'a Decimal,
    order_type: OrderType,
) -> AddOrderWsView<'a> {
    AddOrderWsView {
        side,
        pair,
        volume,
        order_type,
        price: &None,
        price2: &None,
        oflags: &[],
        margin: false,
        reduce_only: &None,
        stp_type: &None,
        trigger: &None,
        time_in_force: &None,
        display_vol: &None,
        start_time: &None,
        expire_time: &None,
        userref: &None,
        cl_ord_id: &None,
        close_ordertype: &None,
        close_price: &None,
        close_price2: &None,
        validate: false,
    }
}

/// Shared auth + event-bus + rate-limit trackers for the make_namespace_* builders,
/// all bound to `clock`. Each builder wires these into its own transport + surface.
fn build_deps(
    clock: &Arc<dyn crate::clock::Clock>,
) -> (
    Arc<AuthStack>,
    Arc<crate::dispatch::DispatchEventBus>,
    Arc<crate::rate_limit::SpotApiRateLimitTracker>,
    Arc<crate::rate_limit::SpotTradingRateLimitTracker>,
) {
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
    let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
        crate::dispatch::DispatchEventBusConfig::defaults(),
        Arc::clone(clock),
    ));
    let api_rl = Arc::new(crate::rate_limit::SpotApiRateLimitTracker::new(
        crate::rate_limit::Tier::Starter,
        Arc::clone(&bus),
        Arc::clone(clock),
        Arc::new(crate::build::knobs::Knobs::defaults()),
    ));
    let trading_rl = Arc::new(crate::rate_limit::SpotTradingRateLimitTracker::new(
        crate::rate_limit::Tier::Starter,
        Arc::clone(&bus),
        Arc::clone(clock),
        Arc::new(crate::build::knobs::Knobs::defaults()),
    ));
    (auth, bus, api_rl, trading_rl)
}

/// A `BatchOrderEntry` with the four meaningful fields set and the rest defaulted.
/// `Side`/`OrderType` have no `Default`, so they are required args; call sites use
/// functional-record-update to set any remaining fields.
fn base_entry(
    side: Side,
    ordertype: OrderType,
    volume: Decimal,
    price: Option<Price>,
) -> BatchOrderEntry {
    let e = BatchOrderEntry::new(ordertype, side, volume);
    match price {
        Some(p) => e.price(p),
        None => e,
    }
}

#[tokio::test]
async fn order_buy_sends_canonical_form_and_allocates_cl_ord_id_when_omitted() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": {
            "descr": { "order": "buy 0.001 BTC/USD @ limit 50000" },
            "txid": ["OBE25Z-GZRP2-6YWYZI"]
        }
    }));
    let resp = trade
        .order(buy_limit("BTC/USD", "0.001", "50000"))
        .via(Transport::Rest)
        .await
        .unwrap();

    let (path, body) = mock.last_post.lock().unwrap().clone().unwrap();
    assert_eq!(path, "/0/private/AddOrder");
    assert!(body.contains("pair=BTC%2FUSD"));
    assert!(body.contains("type=buy"));
    assert!(body.contains("ordertype=limit"));
    assert!(body.contains("volume=0.001"));
    assert!(body.contains("price=50000"));
    assert!(body.contains("cl_ord_id="), "cl_ord_id should be embedded");
    assert!(body.contains("nonce="));

    assert_eq!(resp.txid.as_ref().unwrap().as_str(), "OBE25Z-GZRP2-6YWYZI");
    assert_eq!(resp.cl_ord_id.as_ref().unwrap().as_str().len(), 36);
    assert_eq!(
        resp.descr.order.as_deref(),
        Some("buy 0.001 BTC/USD @ limit 50000")
    );
    assert_eq!(resp.descr.close, None);
}

#[tokio::test]
async fn order_sell_sends_type_sell() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": {
            "descr": { "order": "sell 0.001 BTC/USD @ limit 60000" },
            "txid": ["OSELL-1234-ABCDEF"]
        }
    }));
    let mut req = OrderRequest::new(Symbol::new("BTC/USD").unwrap(), dec("0.001"), Side::Sell)
        .order_type(OrderType::Limit);
    req.price = Some(dec("60000").into());
    let resp = trade.order(req).via(Transport::Rest).await.unwrap();
    let (path, body) = mock.last_post.lock().unwrap().clone().unwrap();
    assert_eq!(path, "/0/private/AddOrder");
    assert!(body.contains("type=sell"));
    assert_eq!(resp.txid.as_ref().unwrap().as_str(), "OSELL-1234-ABCDEF");
    assert_eq!(resp.cl_ord_id.as_ref().unwrap().as_str().len(), 36);
}

#[tokio::test]
async fn order_buy_validate_mode_decodes_with_none_txid() {
    let (trade, _) = make_namespace(json!({
        "error": [],
        "result": {
            "descr": { "order": "buy 0.0001 BTC/USD @ limit 20000" }
        }
    }));
    let mut req = OrderRequest::new(Symbol::new("BTC/USD").unwrap(), dec("0.0001"), Side::Buy)
        .order_type(OrderType::Limit);
    req.price = Some(dec("20000").into());
    req.validate = true;
    let resp = trade.order(req).via(Transport::Rest).await.unwrap();
    assert!(resp.txid.is_none());
    assert_eq!(
        resp.descr.order.as_deref(),
        Some("buy 0.0001 BTC/USD @ limit 20000")
    );
}

#[tokio::test]
async fn cl_ord_id_accessor_returns_allocated_id_before_await_and_matches_response() {
    let (trade, _) = make_namespace(json!({
        "error": [],
        "result": {
            "descr": { "order": "buy 0.001 BTC/USD @ limit 50000" },
            "txid": ["OBE25Z-GZRP2-6YWYZI"]
        }
    }));
    let pending = trade
        .order(buy_limit("BTC/USD", "0.001", "50000"))
        .via(Transport::Rest);
    let pre = pending.cl_ord_id().cloned();
    assert!(
        pre.is_some(),
        "order_buy must synchronously allocate a cl_ord_id"
    );
    let resp = pending.await.unwrap();
    assert_eq!(Some(pre.unwrap()), resp.cl_ord_id);
}

#[tokio::test]
async fn cl_ord_id_accessor_honours_caller_supplied_id() {
    let (trade, _) = make_namespace(json!({
        "error": [],
        "result": {
            "descr": { "order": "buy 0.001 BTC/USD @ limit 50000" },
            "txid": ["OBE25Z-GZRP2-6YWYZI"]
        }
    }));
    let mine = ClOrdId::new("my-order-01").unwrap();
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.cl_ord_id = Some(mine.clone());
    let pending = trade.order(req).via(Transport::Rest);
    assert_eq!(pending.cl_ord_id(), Some(&mine));
    let resp = pending.await.unwrap();
    assert_eq!(resp.cl_ord_id, Some(mine));
}

#[tokio::test]
async fn userref_order_composes_without_conflicting_identifiers_and_no_auto_cl_ord_id() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": {
            "descr": { "order": "buy 0.001 BTC/USD @ limit 50000" },
            "txid": ["OUSERREF-1-ABCDEF"]
        }
    }));
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.userref = Some(424242);

    let pending = trade.order(req).via(Transport::Rest);
    assert!(
        pending.cl_ord_id().is_none(),
        "userref order must NOT get an auto cl_ord_id (would trip userref ⊕ cl_ord_id)"
    );

    let resp = pending
        .await
        .expect("userref order must place without ConflictingOrderIdentifiers");

    let (_, body) = mock.last_post.lock().unwrap().clone().unwrap();
    assert!(
        body.contains("userref=424242"),
        "userref on the wire: {body}"
    );
    assert!(
        !body.contains("cl_ord_id="),
        "cl_ord_id must be ABSENT from the wire when userref is set: {body}"
    );
    assert_eq!(resp.cl_ord_id, None);
}

#[tokio::test]
async fn conditional_close_order_gets_no_auto_cl_ord_id() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": {
            "descr": {
                "order": "buy 0.001 BTC/USD @ limit 50000",
                "close": "close position @ stop loss 48000"
            },
            "txid": ["OCLOSE-1-ABCDEF"]
        }
    }));
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.conditional_close = Some(ConditionalClose::new(
        CloseOrderType::StopLoss,
        dec("48000"),
    ));

    let pending = trade.order(req).via(Transport::Rest);
    assert!(
        pending.cl_ord_id().is_none(),
        "conditional-close order must NOT get an auto cl_ord_id (Kraken rejects it)"
    );

    let resp = pending
        .await
        .expect("conditional-close order composes + places");

    let (_, body) = mock.last_post.lock().unwrap().clone().unwrap();
    assert!(
        body.contains("close%5Bordertype%5D=stop-loss")
            || body.contains("close[ordertype]=stop-loss"),
        "conditional-close clause on the wire: {body}"
    );
    assert!(
        !body.contains("cl_ord_id="),
        "cl_ord_id must be ABSENT from the wire on a conditional-close order: {body}"
    );
    assert_eq!(resp.cl_ord_id, None);
}

#[tokio::test]
async fn normal_order_still_gets_auto_cl_ord_id_idempotency_unchanged() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": {
            "descr": { "order": "buy 0.001 BTC/USD @ limit 50000" },
            "txid": ["ONORMAL-1-ABCDEF"]
        }
    }));
    let pending = trade
        .order(buy_limit("BTC/USD", "0.001", "50000"))
        .via(Transport::Rest);
    let pre = pending.cl_ord_id().cloned();
    assert!(
        pre.is_some(),
        "an ordinary order MUST still get a synchronously-allocated cl_ord_id (idempotency)"
    );

    let resp = pending.await.unwrap();
    assert_eq!(resp.cl_ord_id, pre);
    let (_, body) = mock.last_post.lock().unwrap().clone().unwrap();
    assert!(
        body.contains("cl_ord_id="),
        "auto cl_ord_id on the wire: {body}"
    );
}

#[tokio::test]
async fn cancel_all_and_deadman_have_no_cl_ord_id() {
    let (trade, _) = make_namespace(json!({ "error": [], "result": { "count": 0 } }));
    assert!(trade.cancel_all().cl_ord_id().is_none());
    assert!(trade.cancel_all_orders_after(0).cl_ord_id().is_none());
}

#[tokio::test]
async fn cancel_sends_cl_ord_id_form() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": { "count": 1, "pending": false }
    }));
    let id = ClOrdId::new("cancel-me-01").unwrap();
    let pending = trade.cancel(id.clone()).via(Transport::Rest);
    assert_eq!(pending.cl_ord_id(), Some(&id));
    let _ = pending.await.unwrap();
    let (path, body) = mock.last_post.lock().unwrap().clone().unwrap();
    assert_eq!(path, "/0/private/CancelOrder");
    assert!(body.contains("cl_ord_id=cancel-me-01"));
    assert!(!body.contains("txid="));
}

#[tokio::test]
async fn order_amend_single_arg_works() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": { "amend_id": "TJLGNA-SGKZ7-STOQMD" }
    }));
    let id = ClOrdId::new("amend-me-01").unwrap();
    let mut req = OrderAmendRequest::new(id.clone());
    req.order_volume = Some(dec("0.75"));
    let pending = trade.order_amend(req).via(Transport::Rest);
    assert_eq!(pending.cl_ord_id(), Some(&id));
    let resp = pending.await.unwrap();
    assert_eq!(resp.amend_id.as_str(), "TJLGNA-SGKZ7-STOQMD");
    let (path, body) = mock.last_post.lock().unwrap().clone().unwrap();
    assert_eq!(path, "/0/private/AmendOrder");
    assert!(body.contains("cl_ord_id=amend-me-01"));
    assert!(body.contains("order_qty=0.75"));
}

#[tokio::test]
async fn cancel_all_sends_no_form_params_beyond_nonce() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": { "count": 5 }
    }));
    let resp = trade.cancel_all().via(Transport::Rest).await.unwrap();
    assert_eq!(resp.count, 5);
    let (path, body) = mock.last_post.lock().unwrap().clone().unwrap();
    assert_eq!(path, "/0/private/CancelAll");
    assert!(
        body.starts_with("nonce="),
        "body should be nonce-only — got {}",
        body
    );
}

fn ok(result: serde_json::Value) -> serde_json::Value {
    json!({ "error": [], "result": result })
}

#[tokio::test]
async fn add_order_decodes_descr_with_close() {
    let (trade, _) = make_namespace(ok(json!({
        "descr": {
            "order": "buy 1.0 XBTUSD @ limit 50000",
            "close": "close position @ stop loss 48000"
        },
        "txid": ["OABC-1234-ABCDEF"]
    })));
    let resp = trade
        .order(buy_limit("BTC/USD", "1.0", "50000"))
        .via(Transport::Rest)
        .await
        .unwrap();
    assert_eq!(resp.txid.as_ref().unwrap().as_str(), "OABC-1234-ABCDEF");
    assert_eq!(
        resp.descr.order.as_deref(),
        Some("buy 1.0 XBTUSD @ limit 50000")
    );
    assert_eq!(
        resp.descr.close,
        Some("close position @ stop loss 48000".to_string())
    );
    assert_eq!(resp.cl_ord_id.as_ref().unwrap().as_str().len(), 36);
}

#[tokio::test]
async fn add_order_decodes_descr_without_close() {
    let (trade, _) = make_namespace(ok(json!({
        "descr": { "order": "buy 1.0 XBTUSD @ limit 50000" },
        "txid": ["OABC-1234-ABCDEF"]
    })));
    let resp = trade
        .order(buy_limit("BTC/USD", "1.0", "50000"))
        .via(Transport::Rest)
        .await
        .unwrap();
    assert_eq!(
        resp.descr.order.as_deref(),
        Some("buy 1.0 XBTUSD @ limit 50000")
    );
    assert_eq!(resp.descr.close, None);
}

#[tokio::test]
async fn add_order_empty_txid_array_decodes_to_none_not_panic() {
    let (trade, _) = make_namespace(ok(json!({
        "descr": { "order": "buy 1.0 XBTUSD @ limit 50000" },
        "txid": []
    })));
    let resp = trade
        .order(buy_limit("BTC/USD", "1.0", "50000"))
        .via(Transport::Rest)
        .await
        .unwrap();
    assert!(resp.txid.is_none());
}

#[tokio::test]
async fn add_order_missing_descr_is_malformed_not_panic() {
    let (trade, _) = make_namespace(ok(json!({ "txid": ["OABC-1234-ABCDEF"] })));
    let err = trade
        .order(buy_limit("BTC/USD", "1.0", "50000"))
        .via(Transport::Rest)
        .await
        .unwrap_err();
    assert!(matches!(err, TradeError::MalformedResponse { .. }));
}

fn amend_req(vol: Option<&str>, price: Option<&str>) -> OrderAmendRequest {
    let mut req = OrderAmendRequest::new(ClOrdId::new("amend-target-1").unwrap());
    req.order_volume = vol.map(dec);
    req.limit_price = price.map(|p| dec(p).into());
    req
}

#[tokio::test]
async fn amend_decodes_amend_id() {
    let (trade, _) = make_namespace(ok(json!({ "amend_id": "TJLGNA-SGKZ7-STOQMD" })));
    let resp = trade
        .order_amend(amend_req(Some("0.75"), None))
        .via(Transport::Rest)
        .await
        .unwrap();
    assert_eq!(resp.amend_id.as_str(), "TJLGNA-SGKZ7-STOQMD");
}

#[tokio::test]
async fn amend_tolerates_unknown_wire_fields() {
    let (trade, _) = make_namespace(ok(json!({
        "amend_id": "TJLGNA-SGKZ7-STOQMD",
        "some_future_field": "ONEW01-2345-FEDCBA"
    })));
    let resp = trade
        .order_amend(amend_req(None, Some("51000")))
        .via(Transport::Rest)
        .await
        .unwrap();
    assert_eq!(resp.amend_id.as_str(), "TJLGNA-SGKZ7-STOQMD");
}

#[tokio::test]
async fn amend_missing_amend_id_is_malformed_not_panic() {
    let (trade, _) = make_namespace(ok(json!({ "unrelated": "x" })));
    let err = trade
        .order_amend(amend_req(Some("0.5"), None))
        .via(Transport::Rest)
        .await
        .unwrap_err();
    assert!(matches!(err, TradeError::MalformedResponse { .. }));
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

fn cancel_id() -> ClOrdId {
    ClOrdId::new("cancel-target-1").unwrap()
}

#[tokio::test]
async fn cancel_order_decodes_count_u32_and_pending() {
    let (trade, _) = make_namespace(ok(json!({ "count": 1, "pending": false })));
    let resp = trade
        .cancel(cancel_id())
        .via(Transport::Rest)
        .await
        .unwrap();
    let count: u32 = resp.count;
    assert_eq!(count, 1);
    assert!(!resp.pending);
}

#[tokio::test]
async fn cancel_order_pending_defaults_false_when_absent() {
    let (trade, _) = make_namespace(ok(json!({ "count": 1 })));
    let resp = trade
        .cancel(cancel_id())
        .via(Transport::Rest)
        .await
        .unwrap();
    assert_eq!(resp.count, 1);
    assert!(!resp.pending);
}

#[tokio::test]
async fn cancel_order_missing_count_is_malformed_not_panic() {
    let (trade, _) = make_namespace(ok(json!({ "pending": true })));
    let err = trade
        .cancel(cancel_id())
        .via(Transport::Rest)
        .await
        .unwrap_err();
    assert!(matches!(err, TradeError::MalformedResponse { .. }));
}

#[tokio::test]
async fn cancel_all_decodes_count_u32() {
    let (trade, _) = make_namespace(ok(json!({ "count": 3 })));
    let resp = trade.cancel_all().via(Transport::Rest).await.unwrap();
    let count: u32 = resp.count;
    assert_eq!(count, 3);
}

#[tokio::test]
async fn cancel_all_missing_count_is_malformed_not_panic() {
    let (trade, _) = make_namespace(ok(json!({})));
    let err = trade.cancel_all().via(Transport::Rest).await.unwrap_err();
    assert!(matches!(err, TradeError::MalformedResponse { .. }));
}

#[tokio::test]
async fn deadman_armed_decodes_both_times() {
    let (trade, _) = make_namespace(ok(json!({
        "currentTime": "2026-06-02T12:00:00Z",
        "triggerTime": "2026-06-02T12:01:00Z"
    })));
    let resp = trade
        .cancel_all_orders_after(60)
        .via(Transport::Rest)
        .await
        .unwrap();
    assert_eq!(resp.current_time, "2026-06-02T12:00:00Z");
    assert_eq!(resp.trigger_time, Some("2026-06-02T12:01:00Z".to_string()));
}

#[tokio::test]
async fn deadman_disarmed_trigger_time_zero_string_is_none() {
    let (trade, _) = make_namespace(ok(json!({
        "currentTime": "2026-06-02T12:00:00Z",
        "triggerTime": "0"
    })));
    let resp = trade
        .cancel_all_orders_after(0)
        .via(Transport::Rest)
        .await
        .unwrap();
    assert_eq!(resp.trigger_time, None);
}

#[tokio::test]
async fn deadman_trigger_time_absent_is_none() {
    let (trade, _) = make_namespace(ok(json!({ "currentTime": "2026-06-02T12:00:00Z" })));
    let resp = trade
        .cancel_all_orders_after(0)
        .via(Transport::Rest)
        .await
        .unwrap();
    assert_eq!(resp.trigger_time, None);
}

#[tokio::test]
async fn deadman_missing_current_time_is_malformed_not_panic() {
    let (trade, _) = make_namespace(ok(json!({ "triggerTime": "0" })));
    let err = trade
        .cancel_all_orders_after(0)
        .via(Transport::Rest)
        .await
        .unwrap_err();
    assert!(matches!(err, TradeError::MalformedResponse { .. }));
}

#[tokio::test]
async fn via_ws_transport_on_rest_only_namespace_returns_typed_error_not_panic() {
    let (trade, _) = make_namespace(json!({ "error": [], "result": { "count": 0 } }));
    let err = trade
        .cancel_all()
        .via(Transport::WsV2Auth)
        .await
        .unwrap_err();
    assert!(matches!(err, TradeError::WsSurfaceUnavailable));
}

#[tokio::test]
async fn via_unroutable_transport_returns_unsupported_transport_error() {
    let (trade, _) = make_namespace(json!({ "error": [], "result": { "count": 0 } }));
    let err = trade
        .cancel_all()
        .via(Transport::WsV2Public)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            TradeError::UnsupportedTransport {
                transport: Transport::WsV2Public,
                ..
            }
        ),
        "expected UnsupportedTransport(WsV2Public), got: {err:?}"
    );
}

#[tokio::test]
async fn cancel_batch_rejects_explicit_ws_with_rest_only_remedy() {
    use crate::error::ApiError;
    let (trade, _) = make_namespace(json!({ "error": [], "result": { "count": 1 } }));
    let ids = vec![ClOrdId::new("44444444-4444-4444-8444-444444444444").unwrap()];
    let err = trade
        .cancel_batch(ids.clone())
        .via(Transport::WsV2Auth)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &err,
            TradeError::UnsupportedTransport {
                transport: Transport::WsV2Auth,
                legal: [Transport::Rest],
            }
        ),
        "expected REST-only UnsupportedTransport, got: {err:?}"
    );
    assert_eq!(
        err.message(),
        "Transport WsV2Auth cannot serve this spot order operation; use `.via(Transport::Rest)`."
    );
    let err = trade
        .cancel_batch(ids)
        .via(Transport::WsV2Public)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &err,
            TradeError::UnsupportedTransport {
                transport: Transport::WsV2Public,
                legal: [Transport::Rest],
            }
        ),
        "expected REST-only remedy for WsV2Public too, got: {err:?}"
    );
}

#[tokio::test]
async fn cancel_batch_bare_await_still_serves_rest() {
    let (trade, _) = make_namespace(json!({ "error": [], "result": { "count": 1 } }));
    let ids = vec![ClOrdId::new("55555555-5555-4555-8555-555555555555").unwrap()];
    let resp = trade
        .cancel_batch(ids)
        .await
        .expect("bare await must serve REST");
    assert_eq!(resp.results.len(), 1);
}

#[test]
fn ws_add_order_params_use_ws_names_and_array_free_scalar_cl_ord_id() {
    let cl = Some(ClOrdId::new("33333333-3333-4333-8333-333333333333").unwrap());
    let sym = Symbol::new("BTC/USDC").unwrap();
    let vol = dec("0.0001");
    let price = Some(dec("33512.0").into());
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        oflags: &[OFlag::Post],
        cl_ord_id: &cl,
        ..base_add_view("buy", &sym, &vol, OrderType::Limit)
    });
    assert_eq!(params["order_type"], "limit");
    assert_eq!(params["side"], "buy");
    assert_eq!(params["symbol"], "BTC/USDC");
    assert_eq!(params["post_only"], true);
    // WS cl_ord_id is scalar for add/amend, array for cancel.
    assert_eq!(params["cl_ord_id"], "33333333-3333-4333-8333-333333333333");
    assert!(!params["cl_ord_id"].is_array());
    // order_qty / limit_price are JSON numbers (no f64 rounding).
    assert!(params["order_qty"].is_number());
    assert_eq!(params["order_qty"].as_str(), None);
    assert_eq!(params["order_qty"].to_string(), "0.0001");
    assert_eq!(params["limit_price"].to_string(), "33512.0");
    // Composer never carries a token (reactor injects it).
    assert!(params.get("token").is_none());
}

#[test]
fn ws_add_order_params_omit_post_only_and_limit_price_when_unset() {
    let cl = Some(ClOrdId::allocate_v4());
    let sym = Symbol::new("BTC/USDC").unwrap();
    let vol = dec("1");
    let params = compose_add_order_params(AddOrderWsView {
        cl_ord_id: &cl,
        ..base_add_view("sell", &sym, &vol, OrderType::Market)
    });
    assert_eq!(params["side"], "sell");
    assert!(
        params.get("limit_price").is_none(),
        "no limit_price when None"
    );
    assert!(
        params.get("post_only").is_none(),
        "post_only omitted when false"
    );
}

#[test]
fn ws_add_order_params_emit_margin_only_when_set() {
    let sym = Symbol::new("BTC/USDC").unwrap();
    let vol = dec("1");
    let view = |margin: bool, reduce_only: &'static Option<bool>| AddOrderWsView {
        margin,
        reduce_only,
        ..base_add_view("buy", &sym, &vol, OrderType::Market)
    };
    let p = compose_add_order_params(view(true, &Some(true)));
    assert_eq!(p["margin"], true);
    assert_eq!(p["reduce_only"], true);
    let p = compose_add_order_params(view(false, &None));
    assert!(p.get("margin").is_none(), "margin omitted when false");
}

#[test]
fn ws_cancel_order_cl_ord_id_is_an_array() {
    let cl = ClOrdId::new("44444444-4444-4444-8444-444444444444").unwrap();
    let params = compose_cancel_order_params(&CancelRequest::new(cl));
    assert!(params["cl_ord_id"].is_array());
    assert_eq!(
        params["cl_ord_id"][0],
        "44444444-4444-4444-8444-444444444444"
    );
}

#[test]
fn ws_amend_params_scalar_cl_ord_id_plus_mutables() {
    let mut req =
        OrderAmendRequest::new(ClOrdId::new("55555555-5555-4555-8555-555555555555").unwrap());
    req.limit_price = Some(dec("31000.0").into());
    let params = compose_amend_order_params(&req);
    assert_eq!(params["cl_ord_id"], "55555555-5555-4555-8555-555555555555");
    assert!(!params["cl_ord_id"].is_array(), "amend cl_ord_id is SCALAR");
    assert_eq!(params["limit_price"].to_string(), "31000.0");
    assert_eq!(params["limit_price_type"], "static");
}

#[test]
fn ws_amend_params_relative_prices_use_flat_type_keys() {
    let mut req =
        OrderAmendRequest::new(ClOrdId::new("55555555-5555-4555-8555-555555555555").unwrap());
    req.limit_price = Some(Price::Offset {
        unit: PriceUnit::Quote,
        value: dec("150"),
    });
    req.trigger_price = Some(Price::Offset {
        unit: PriceUnit::Percent,
        value: dec("2.0"),
    });
    let params = compose_amend_order_params(&req);
    assert_eq!(params["limit_price"].to_string(), "150");
    assert_eq!(params["limit_price_type"], "quote");
    assert_eq!(params["trigger_price"].to_string(), "2.0");
    assert_eq!(params["trigger_price_type"], "pct");
    assert!(params.get("triggers").is_none());
}

#[test]
fn ws_add_order_decode_maps_order_id_to_txid_and_descr_none() {
    let cl = ClOrdId::new("66666666-6666-4666-8666-666666666666").unwrap();
    let resp = WsResponse {
        req_id: 1,
        success: true,
        result: json!({ "cl_ord_id": "66666666-6666-4666-8666-666666666666", "order_id": "OBQUSP-5FD4N-JYTGWJ" }),
        error: None,
    };
    let decoded = decode_ws_add_order(resp, Some(cl.clone())).unwrap();
    assert_eq!(
        decoded.txid.as_ref().map(|t| t.as_str()),
        Some("OBQUSP-5FD4N-JYTGWJ")
    );
    assert_eq!(
        decoded.descr.order, None,
        "WS omits descr → None (Hard Rule 11)"
    );
    assert_eq!(decoded.cl_ord_id, Some(cl));
}

#[test]
fn ws_add_order_decode_failure_classifies_error_string() {
    let resp = WsResponse {
        req_id: 2,
        success: false,
        result: json!(null),
        error: Some("EOrder:Insufficient funds".to_string()),
    };
    let err = decode_ws_add_order(resp, Some(ClOrdId::allocate_v4())).unwrap_err();
    assert!(matches!(err, TradeError::InsufficientFunds { .. }));
}

#[test]
fn ws_add_order_validate_mode_empty_order_id_yields_none_txid() {
    let cl = ClOrdId::new("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa").unwrap();
    let resp = WsResponse {
        req_id: 10,
        success: true,
        result: json!({ "cl_ord_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "order_id": "" }),
        error: None,
    };
    let decoded = decode_ws_add_order(resp, Some(cl)).unwrap();
    assert!(
        decoded.txid.is_none(),
        "empty order_id in WS validate reply must decode to txid = None, got {:?}",
        decoded.txid
    );
}

#[test]
fn ws_add_order_nonempty_order_id_yields_some_txid() {
    let cl = ClOrdId::new("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb").unwrap();
    let resp = WsResponse {
        req_id: 11,
        success: true,
        result: json!({ "cl_ord_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "order_id": "OABC-1234-DEFGHI" }),
        error: None,
    };
    let decoded = decode_ws_add_order(resp, Some(cl)).unwrap();
    assert_eq!(
        decoded.txid.as_ref().map(|t| t.as_str()),
        Some("OABC-1234-DEFGHI"),
        "non-empty order_id must decode to Some(TxId(..))"
    );
}

#[test]
fn ws_cancel_decode_derives_count_one_pending_false() {
    let resp = WsResponse {
        req_id: 3,
        success: true,
        result: json!({ "cl_ord_id": "77777777-7777-4777-8777-777777777777" }),
        error: None,
    };
    let decoded = decode_ws_cancel_order(resp).unwrap();
    assert_eq!(decoded.count, 1);
    assert!(!decoded.pending);
}

#[test]
fn ws_cancel_decode_missing_cl_ord_id_is_malformed_not_fabricated() {
    let resp = WsResponse {
        req_id: 5,
        success: true,
        result: json!({}),
        error: None,
    };
    let err = decode_ws_cancel_order(resp).unwrap_err();
    assert!(
        matches!(err, TradeError::MalformedResponse { .. }),
        "expected MalformedResponse, got {:?}",
        err
    );
}

#[test]
fn ws_cancel_decode_failure_classifies_error_string() {
    let resp = WsResponse {
        req_id: 6,
        success: false,
        result: json!(null),
        error: Some("EOrder:Invalid order".to_string()),
    };
    let err = decode_ws_cancel_order(resp).unwrap_err();
    assert!(
        matches!(err, TradeError::InvalidOrder { .. }),
        "expected InvalidOrder for EOrder:Invalid order, got {:?}",
        err
    );
}

#[test]
fn ws_cancel_all_decode_reads_count() {
    let resp = WsResponse {
        req_id: 7,
        success: true,
        result: json!({ "count": 4 }),
        error: None,
    };
    let decoded = decode_ws_cancel_all(resp).unwrap();
    assert_eq!(decoded.count, 4);
}

#[test]
fn ws_cancel_all_decode_missing_count_is_malformed_not_fabricated_zero() {
    let resp = WsResponse {
        req_id: 8,
        success: true,
        result: json!({}),
        error: None,
    };
    let err = decode_ws_cancel_all(resp).unwrap_err();
    assert!(
        matches!(err, TradeError::MalformedResponse { .. }),
        "expected MalformedResponse, got {:?}",
        err
    );
}

#[test]
fn ws_cancel_all_decode_non_integer_count_is_malformed() {
    let resp = WsResponse {
        req_id: 9,
        success: true,
        result: json!({ "count": "lots" }),
        error: None,
    };
    let err = decode_ws_cancel_all(resp).unwrap_err();
    assert!(matches!(err, TradeError::MalformedResponse { .. }));
}

#[test]
fn ws_amend_decode_extracts_amend_id() {
    let resp = WsResponse {
        req_id: 4,
        success: true,
        result: json!({ "amend_id": "TA-ABC-123", "cl_ord_id": "x" }),
        error: None,
    };
    let decoded = decode_ws_amend_order(resp).unwrap();
    assert_eq!(decoded.amend_id.as_str(), "TA-ABC-123");
}

#[test]
fn system_status_gate_blocks_add_when_not_online_allows_cancel() {
    let maintenance = SystemStatus {
        status: "maintenance".to_string(),
        timestamp: String::new(),
    };
    let err = gate_system_status(&maintenance, WsOp::AddOrder).unwrap_err();
    assert!(matches!(err, TradeError::SystemStatusBlocked { .. }));
    let cancel_only = SystemStatus {
        status: "cancel_only".to_string(),
        timestamp: String::new(),
    };
    assert!(gate_system_status(&cancel_only, WsOp::CancelOrder).is_ok());
    assert!(gate_system_status(&cancel_only, WsOp::AddOrder).is_err());
    let online = SystemStatus {
        status: "online".to_string(),
        timestamp: String::new(),
    };
    assert!(gate_system_status(&online, WsOp::AddOrder).is_ok());
    assert!(gate_system_status(&online, WsOp::CancelAll).is_ok());
}

#[test]
fn system_status_blocked_message_is_plain_language() {
    let current = SystemStatus {
        status: "maintenance".to_string(),
        timestamp: String::new(),
    };
    let required = SystemStatus {
        status: "online".to_string(),
        timestamp: String::new(),
    };
    let msg = TradeError::SystemStatusBlocked { current, required }.to_string();
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

use crate::transport::TransportErrorKind;
use std::future::IntoFuture;

/// A transport mock returning a configurable `Result` on every POST; optionally
/// blocks on a `Notify` before returning so a test can drop the order future
/// mid-`.await`.
struct ProgrammableMock {
    /// `Ok(envelope)` (full `{error,result}`) or `Err(transport error)`.
    response: Result<serde_json::Value, TransportError>,
    /// When set, `post_form_signed` awaits this before returning — lets a test
    /// hold the wire await open and drop the future (mid-flight cancellation).
    block_until: Option<Arc<tokio::sync::Notify>>,
}

#[async_trait::async_trait]
impl HttpTransport for ProgrammableMock {
    async fn get_json(
        &self,
        _path: &str,
        _query: &[(&str, &str)],
    ) -> Result<serde_json::Value, TransportError> {
        panic!("ProgrammableMock tests never hit GET");
    }
    async fn post_form_signed(
        &self,
        _path: &str,
        _body: &str,
        _api_key_header: &str,
        _api_sign_header: &str,
    ) -> Result<serde_json::Value, TransportError> {
        if let Some(n) = &self.block_until {
            n.notified().await;
        }
        match &self.response {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(e.clone()),
        }
    }
}

/// Build a REST-only `TradeNamespace` with the bus wired (so `emit_*` helpers
/// publish) and start the dispatch reactor. Returns the namespace + the bus.
fn make_namespace_with_bus(
    mock: Arc<dyn HttpTransport>,
) -> (TradeNamespace, Arc<DispatchEventBus>) {
    let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
    let (auth, bus, api_rl, trading_rl) = build_deps(&clock);
    let rest = Arc::new(RestSurface::new_with_index(
        Arc::clone(&mock),
        auth,
        api_rl,
        trading_rl,
        clock,
        Arc::new(crate::rate_limit::ClOrdIdPairIndex::new(1024)),
        test_no_retry_engine(),
        std::time::Duration::from_secs(30),
    ));
    rest.set_bus(Arc::clone(&bus));
    bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
    (TradeNamespace::new(rest), bus)
}

/// Counts POSTs; every call rejects with `EAPI:Invalid nonce`.
struct NonceRejectingMock {
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl HttpTransport for NonceRejectingMock {
    async fn get_json(
        &self,
        _path: &str,
        _query: &[(&str, &str)],
    ) -> Result<serde_json::Value, TransportError> {
        panic!("nonce-heal tests never hit GET");
    }
    async fn post_form_signed(
        &self,
        _path: &str,
        _body: &str,
        _api_key_header: &str,
        _api_sign_header: &str,
    ) -> Result<serde_json::Value, TransportError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(json!({ "error": ["EAPI:Invalid nonce"], "result": {} }))
    }
}

/// `validate=true` routes the REST placement to the nonce-healed policy (one
/// fresh-nonce re-sign); a live placement keeps the strict never-retry (zero).
#[tokio::test]
async fn rest_add_order_validate_mode_takes_the_nonce_heal() {
    let mock = Arc::new(NonceRejectingMock {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let (trade, _bus) = make_namespace_with_bus(Arc::clone(&mock) as Arc<dyn HttpTransport>);
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.validate = true;
    tokio::time::timeout(TIMEOUT, trade.order(req).via(Transport::Rest))
        .await
        .expect("validate order timed out")
        .expect_err("persistent invalid nonce bubbles");
    assert_eq!(
        mock.calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "validate mode: original + exactly one fresh-nonce re-sign"
    );

    let mock2 = Arc::new(NonceRejectingMock {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let (trade2, _bus2) = make_namespace_with_bus(Arc::clone(&mock2) as Arc<dyn HttpTransport>);
    tokio::time::timeout(
        TIMEOUT,
        trade2
            .order(buy_limit("BTC/USD", "0.001", "50000"))
            .via(Transport::Rest),
    )
    .await
    .expect("live order timed out")
    .expect_err("invalid nonce bubbles unretried");
    assert_eq!(
        mock2.calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "live placement: fail fast, no re-sign"
    );
}

/// The sell and batch REST arms carry the same validate-mode selection as buy.
#[tokio::test]
async fn rest_sell_and_batch_validate_mode_take_the_nonce_heal() {
    let mock = Arc::new(NonceRejectingMock {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let (trade, _bus) = make_namespace_with_bus(Arc::clone(&mock) as Arc<dyn HttpTransport>);
    let mut sell = OrderRequest::new(Symbol::new("BTC/USD").unwrap(), dec("0.001"), Side::Buy)
        .order_type(OrderType::Limit);
    sell.price = Some(dec("60000").into());
    sell.validate = true;
    tokio::time::timeout(TIMEOUT, trade.order(sell).via(Transport::Rest))
        .await
        .expect("validate sell timed out")
        .expect_err("persistent invalid nonce bubbles");
    assert_eq!(
        mock.calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "validate sell: original + exactly one fresh-nonce re-sign"
    );

    let mock2 = Arc::new(NonceRejectingMock {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let (trade2, _bus2) = make_namespace_with_bus(Arc::clone(&mock2) as Arc<dyn HttpTransport>);
    let mut batch = two_entry_batch();
    batch.validate = true;
    tokio::time::timeout(TIMEOUT, trade2.order_batch(batch).via(Transport::Rest))
        .await
        .expect("validate batch timed out")
        .expect_err("persistent invalid nonce bubbles");
    assert_eq!(
        mock2.calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "validate batch: original + exactly one fresh-nonce re-sign"
    );
}

/// Subscribe a counting capture for `et`, returning the receiver of payloads.
fn capture(
    bus: &Arc<DispatchEventBus>,
    et: EventType,
) -> tokio::sync::mpsc::Receiver<EventPayload> {
    let (tx, rx) = tokio::sync::mpsc::channel::<EventPayload>(8);
    let _ = bus.subscribe(
        et,
        Arc::new(move |env| {
            let _ = tx.try_send(env.payload.clone());
        }),
        1,
    );
    rx
}

fn ok_envelope(result: serde_json::Value) -> serde_json::Value {
    json!({ "error": [], "result": result })
}

/// A sent-ambiguous transport drop (request reached the wire).
fn transport_err() -> TransportError {
    TransportError {
        kind: TransportErrorKind::RequestSentNoResponse,
        transient: true,
    }
}

/// A not-sent transport failure (never reached the wire).
fn pre_send_transport_err() -> TransportError {
    TransportError {
        kind: TransportErrorKind::TcpRefused,
        transient: true,
    }
}

const TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);
const SHORT: std::time::Duration = std::time::Duration::from_millis(120);

#[test]
fn order_op_to_ws_op_bridge_is_1_to_1() {
    assert_eq!(WsOp::from(OrderOp::AddOrder), WsOp::AddOrder);
    assert_eq!(WsOp::from(OrderOp::AmendOrder), WsOp::AmendOrder);
    assert_eq!(WsOp::from(OrderOp::CancelOrder), WsOp::CancelOrder);
    assert_eq!(WsOp::from(OrderOp::CancelAll), WsOp::CancelAll);
    assert_eq!(
        WsOp::from(OrderOp::CancelAllOrdersAfter),
        WsOp::CancelAllOrdersAfter
    );
    assert_eq!(WsOp::from(OrderOp::OrderBatch), WsOp::BatchAdd);
}

#[tokio::test]
async fn rest_add_order_success_emits_order_submitted_wire_sent() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({
            "descr": { "order": "buy 0.001 BTC/USD @ limit 50000" },
            "txid": ["OBE25Z-GZRP2-6YWYZI"]
        }))),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderSubmittedEvent);

    let cl = ClOrdId::allocate_v4();
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.cl_ord_id = Some(cl.clone());
    let _ = tokio::time::timeout(TIMEOUT, trade.order(req).via(Transport::Rest))
        .await
        .expect("order timed out")
        .unwrap();

    let payload = tokio::time::timeout(SHORT, rx.recv())
        .await
        .expect("no event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderSubmittedEvent {
            cl_ord_id,
            amend_id,
            op,
            status,
            ..
        } => {
            assert_eq!(cl_ord_id, cl);
            assert_eq!(amend_id, None);
            assert_eq!(op, OrderOp::AddOrder);
            assert_eq!(
                status,
                OrderSubmitStatus::WireSent {
                    txid: TxId::new("OBE25Z-GZRP2-6YWYZI")
                }
            );
        }
        other => panic!("expected OrderSubmittedEvent, got {other:?}"),
    }
}

#[tokio::test]
async fn rest_add_order_wire_rejection_emits_order_submitted_wire_error() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(json!({ "error": ["EOrder:Insufficient funds"], "result": {} })),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderSubmittedEvent);

    let _ = tokio::time::timeout(
        TIMEOUT,
        trade
            .order(buy_limit("BTC/USD", "0.001", "50000"))
            .via(Transport::Rest),
    )
    .await
    .expect("order timed out")
    .unwrap_err();

    let payload = tokio::time::timeout(SHORT, rx.recv())
        .await
        .expect("no event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderSubmittedEvent { op, status, .. } => {
            assert_eq!(op, OrderOp::AddOrder);
            assert_eq!(
                status,
                OrderSubmitStatus::WireError {
                    code: "INSUFFICIENT_FUNDS".to_string()
                }
            );
        }
        other => panic!("expected OrderSubmittedEvent(WireError), got {other:?}"),
    }
}

/// The cancel_all REST decode-failure exit stamps the dispatch id (a garbage
/// result shape fails typed decode AFTER the wire call was correlated).
#[tokio::test]
async fn rest_cancel_all_decode_failure_error_carries_request_id() {
    use crate::error::ApiError;
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!([]))),
        block_until: None,
    });
    let (trade, _bus) = make_namespace_with_bus(mock);
    let err = tokio::time::timeout(TIMEOUT, trade.cancel_all().via(Transport::Rest))
        .await
        .expect("cancel_all timed out")
        .unwrap_err();
    assert!(matches!(err, TradeError::MalformedResponse { .. }));
    let rid = err
        .request_id()
        .expect("decode-failure error carries the dispatch id");
    assert!(
        uuid::Uuid::parse_str(rid).is_ok(),
        "correlation id is a UUID"
    );
}

/// Same contract on the cancel_all_orders_after decode-failure exit.
#[tokio::test]
async fn rest_deadman_decode_failure_error_carries_request_id() {
    use crate::error::ApiError;
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!([]))),
        block_until: None,
    });
    let (trade, _bus) = make_namespace_with_bus(mock);
    let err = tokio::time::timeout(
        TIMEOUT,
        trade.cancel_all_orders_after(60).via(Transport::Rest),
    )
    .await
    .expect("cancel_all_orders_after timed out")
    .unwrap_err();
    assert!(matches!(err, TradeError::MalformedResponse { .. }));
    let rid = err
        .request_id()
        .expect("decode-failure error carries the dispatch id");
    assert!(
        uuid::Uuid::parse_str(rid).is_ok(),
        "correlation id is a UUID"
    );
}

#[tokio::test]
async fn rest_wire_rejection_error_and_event_carry_same_request_id() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(json!({ "error": ["EOrder:Insufficient funds"], "result": {} })),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderSubmittedEvent);

    let err = tokio::time::timeout(
        TIMEOUT,
        trade
            .order(buy_limit("BTC/USD", "0.001", "50000"))
            .via(Transport::Rest),
    )
    .await
    .expect("order timed out")
    .unwrap_err();
    use crate::error::ApiError;
    let rid = err
        .request_id()
        .expect("wire-rejected order error carries the dispatch id")
        .to_string();
    assert!(
        uuid::Uuid::parse_str(&rid).is_ok(),
        "correlation id is a UUID"
    );

    let payload = tokio::time::timeout(SHORT, rx.recv())
        .await
        .expect("no event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderSubmittedEvent { request_id, .. } => {
            assert_eq!(
                request_id.as_deref(),
                Some(rid.as_str()),
                "error and event carry the SAME per-dispatch id"
            );
        }
        other => panic!("expected OrderSubmittedEvent, got {other:?}"),
    }
}

#[tokio::test]
async fn rest_amend_success_emits_wire_accepted_with_server_amend_id() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({ "amend_id": "TJLGNA-SGKZ7-STOQMD" }))),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderSubmittedEvent);

    let cl = ClOrdId::new("amend-emit-01").unwrap();
    let mut req = OrderAmendRequest::new(cl.clone());
    req.order_volume = Some(dec("0.75"));
    let _ = tokio::time::timeout(TIMEOUT, trade.order_amend(req).via(Transport::Rest))
        .await
        .expect("amend timed out")
        .unwrap();

    let payload = tokio::time::timeout(SHORT, rx.recv())
        .await
        .expect("no event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderSubmittedEvent {
            cl_ord_id,
            amend_id,
            op,
            status,
            ..
        } => {
            assert_eq!(cl_ord_id, cl);
            assert_eq!(
                amend_id,
                Some(AmendId::from("TJLGNA-SGKZ7-STOQMD".to_string())),
                "amend success must carry the server-minted amend_id"
            );
            assert_eq!(op, OrderOp::AmendOrder);
            assert_eq!(status, OrderSubmitStatus::WireAccepted);
        }
        other => panic!("expected OrderSubmittedEvent(WireAccepted), got {other:?}"),
    }
}

#[tokio::test]
async fn rest_cancel_success_emits_wire_accepted() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({ "count": 1, "pending": false }))),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderSubmittedEvent);

    let cl = ClOrdId::new("cancel-emit-01").unwrap();
    let _ = tokio::time::timeout(TIMEOUT, trade.cancel(cl.clone()).via(Transport::Rest))
        .await
        .expect("cancel timed out")
        .unwrap();

    let payload = tokio::time::timeout(SHORT, rx.recv())
        .await
        .expect("no event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderSubmittedEvent {
            cl_ord_id,
            amend_id,
            op,
            status,
            ..
        } => {
            assert_eq!(cl_ord_id, cl);
            assert_eq!(amend_id, None, "cancel success carries no amend_id");
            assert_eq!(op, OrderOp::CancelOrder);
            assert_eq!(status, OrderSubmitStatus::WireAccepted);
        }
        other => panic!("expected OrderSubmittedEvent(WireAccepted), got {other:?}"),
    }
}

#[tokio::test]
async fn rest_cancel_success_with_stray_amend_id_still_emits_none() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(
            json!({ "count": 1, "amend_id": "TSTRAY-XXXXX-YYYYYY" }),
        )),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderSubmittedEvent);

    let cl = ClOrdId::new("cancel-emit-02").unwrap();
    let _ = tokio::time::timeout(TIMEOUT, trade.cancel(cl.clone()).via(Transport::Rest))
        .await
        .expect("cancel timed out")
        .unwrap();

    let payload = tokio::time::timeout(SHORT, rx.recv())
        .await
        .expect("no event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderSubmittedEvent {
            amend_id, status, ..
        } => {
            assert_eq!(amend_id, None, "cancel must never carry an amend_id");
            assert_eq!(status, OrderSubmitStatus::WireAccepted);
        }
        other => panic!("expected OrderSubmittedEvent(WireAccepted), got {other:?}"),
    }
}

#[tokio::test]
async fn rest_cancel_batch_emits_wire_accepted_per_leg() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({ "count": 1, "pending": false }))),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderSubmittedEvent);

    let ids: Vec<ClOrdId> = ["cb-emit-01", "cb-emit-02", "cb-emit-03"]
        .iter()
        .map(|s| ClOrdId::new(*s).unwrap())
        .collect();
    let _ = tokio::time::timeout(
        TIMEOUT,
        trade.cancel_batch(ids.clone()).via(Transport::Rest),
    )
    .await
    .expect("cancel_batch timed out")
    .unwrap();

    let mut seen: Vec<ClOrdId> = Vec::new();
    for _ in 0..ids.len() {
        let payload = tokio::time::timeout(SHORT, rx.recv())
            .await
            .expect("missing per-leg event")
            .expect("channel closed");
        match payload {
            EventPayload::OrderSubmittedEvent {
                cl_ord_id,
                amend_id,
                op,
                status,
                ..
            } => {
                assert_eq!(amend_id, None, "cancel success carries no amend_id");
                assert_eq!(op, OrderOp::CancelOrder);
                assert_eq!(status, OrderSubmitStatus::WireAccepted);
                seen.push(cl_ord_id);
            }
            other => panic!("expected OrderSubmittedEvent(WireAccepted), got {other:?}"),
        }
    }
    seen.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(
        seen, ids,
        "one event per leg, keyed by that leg's cl_ord_id"
    );
    assert!(
        tokio::time::timeout(SHORT, rx.recv()).await.is_err(),
        "exactly one event per leg — no extras"
    );
}

#[tokio::test]
async fn rest_amend_success_with_empty_amend_id_emits_accepted_and_fails_decode() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({ "amend_id": "" }))),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderSubmittedEvent);

    let cl = ClOrdId::new("amend-empty-01").unwrap();
    let mut req = OrderAmendRequest::new(cl.clone());
    req.order_volume = Some(dec("0.75"));
    let err = tokio::time::timeout(TIMEOUT, trade.order_amend(req).via(Transport::Rest))
        .await
        .expect("amend timed out")
        .unwrap_err();
    assert!(
        matches!(err, TradeError::MalformedResponse { .. }),
        "expected MalformedResponse, got {err:?}"
    );

    let payload = tokio::time::timeout(SHORT, rx.recv())
        .await
        .expect("no event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderSubmittedEvent {
            amend_id,
            cl_ord_id,
            status,
            ..
        } => {
            assert!(amend_id.is_none(), "empty amend_id must surface as None");
            assert_eq!(cl_ord_id, cl);
            assert!(matches!(status, OrderSubmitStatus::WireAccepted));
        }
        other => panic!("wrong payload: {other:?}"),
    }
}

#[tokio::test]
async fn rest_amend_success_without_amend_id_emits_accepted_and_fails_decode() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({}))),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderSubmittedEvent);

    let cl = ClOrdId::new("amend-emit-02").unwrap();
    let mut req = OrderAmendRequest::new(cl.clone());
    req.order_volume = Some(dec("0.75"));
    let err = tokio::time::timeout(TIMEOUT, trade.order_amend(req).via(Transport::Rest))
        .await
        .expect("amend timed out")
        .unwrap_err();
    assert!(
        matches!(err, TradeError::MalformedResponse { .. }),
        "expected MalformedResponse, got {err:?}"
    );

    let payload = tokio::time::timeout(SHORT, rx.recv())
        .await
        .expect("no event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderSubmittedEvent {
            amend_id,
            op,
            status,
            ..
        } => {
            assert_eq!(op, OrderOp::AmendOrder);
            assert_eq!(amend_id, None);
            assert_eq!(status, OrderSubmitStatus::WireAccepted);
        }
        other => panic!("expected OrderSubmittedEvent(WireAccepted), got {other:?}"),
    }
}

#[test]
fn ws_txid_in_result_filters_empty_validate_mode_order_id() {
    use super::events::ws_txid_in_result;
    assert_eq!(ws_txid_in_result(&json!({ "order_id": "" })), None);
    assert_eq!(
        ws_txid_in_result(&json!({ "order_id": "OABC12-XXXXX-YYYYYY" })),
        Some(TxId::new("OABC12-XXXXX-YYYYYY"))
    );
}

#[tokio::test]
async fn rest_add_order_validate_mode_emits_no_event() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({
            "descr": { "order": "buy 0.001 BTC/USD @ limit 50000" }
        }))),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderSubmittedEvent);

    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.validate = true;
    let _ = tokio::time::timeout(TIMEOUT, trade.order(req).via(Transport::Rest))
        .await
        .expect("order timed out")
        .unwrap();

    tokio::time::timeout(SHORT, rx.recv())
        .await
        .expect_err("validate-mode AddOrder must not emit OrderSubmittedEvent");
}

#[tokio::test]
async fn rest_add_order_transport_drop_emits_placement_ambiguous() {
    let mock = Arc::new(ProgrammableMock {
        response: Err(transport_err()),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut amb_rx = capture(&bus, EventType::OrderPlacementAmbiguousEvent);
    let mut sub_rx = capture(&bus, EventType::OrderSubmittedEvent);

    let cl = ClOrdId::allocate_v4();
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.cl_ord_id = Some(cl.clone());
    let _ = tokio::time::timeout(TIMEOUT, trade.order(req).via(Transport::Rest))
        .await
        .expect("order timed out")
        .unwrap_err();

    let payload = tokio::time::timeout(SHORT, amb_rx.recv())
        .await
        .expect("no ambiguous event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderPlacementAmbiguousEvent {
            cl_ord_id,
            amend_id,
            op,
            ..
        } => {
            assert_eq!(cl_ord_id, cl);
            assert_eq!(amend_id, None);
            assert_eq!(op, OrderOp::AddOrder);
        }
        other => panic!("expected OrderPlacementAmbiguousEvent, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(SHORT, sub_rx.recv()).await.is_err(),
        "transport drop must NOT emit OrderSubmittedEvent"
    );
}

#[tokio::test]
async fn rest_add_order_pre_send_transport_failure_does_not_emit_ambiguous() {
    let mock = Arc::new(ProgrammableMock {
        response: Err(pre_send_transport_err()),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut amb_rx = capture(&bus, EventType::OrderPlacementAmbiguousEvent);

    let cl = ClOrdId::allocate_v4();
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.cl_ord_id = Some(cl.clone());
    let _ = tokio::time::timeout(TIMEOUT, trade.order(req).via(Transport::Rest))
        .await
        .expect("order timed out")
        .unwrap_err();

    assert!(
        tokio::time::timeout(SHORT, amb_rx.recv()).await.is_err(),
        "pre-send transport failure must NOT emit OrderPlacementAmbiguousEvent"
    );
}

#[tokio::test]
async fn rest_add_order_mid_response_drop_is_sent_ambiguous_and_not_retryable() {
    use crate::error::ApiError;
    let mock = Arc::new(ProgrammableMock {
        response: Err(TransportError {
            kind: TransportErrorKind::RequestSentNoResponse,
            transient: true,
        }),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut amb_rx = capture(&bus, EventType::OrderPlacementAmbiguousEvent);

    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.cl_ord_id = Some(ClOrdId::allocate_v4());
    let err = tokio::time::timeout(TIMEOUT, trade.order(req).via(Transport::Rest))
        .await
        .expect("order timed out")
        .unwrap_err();
    assert_eq!(err.code(), "CONNECTION_ERROR");
    assert!(
        !err.retryable(),
        "a sent-ambiguous order failure must NOT be retryable"
    );

    let payload = tokio::time::timeout(SHORT, amb_rx.recv())
        .await
        .expect("no ambiguous event")
        .expect("channel closed");
    assert!(matches!(
        payload,
        EventPayload::OrderPlacementAmbiguousEvent { .. }
    ));
}

#[tokio::test]
async fn rest_amend_transport_drop_emits_placement_ambiguous() {
    let mock = Arc::new(ProgrammableMock {
        response: Err(transport_err()),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut amb_rx = capture(&bus, EventType::OrderPlacementAmbiguousEvent);

    let cl = ClOrdId::new("amend-drop-01").unwrap();
    let mut req = OrderAmendRequest::new(cl.clone());
    req.order_volume = Some(dec("0.5"));
    let _ = tokio::time::timeout(TIMEOUT, trade.order_amend(req).via(Transport::Rest))
        .await
        .expect("amend timed out")
        .unwrap_err();

    let payload = tokio::time::timeout(SHORT, amb_rx.recv())
        .await
        .expect("no ambiguous event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderPlacementAmbiguousEvent { cl_ord_id, op, .. } => {
            assert_eq!(cl_ord_id, cl);
            assert_eq!(op, OrderOp::AmendOrder);
        }
        other => panic!("expected OrderPlacementAmbiguousEvent, got {other:?}"),
    }
}

#[tokio::test]
async fn rest_cancel_transport_drop_does_not_emit_ambiguous() {
    let mock = Arc::new(ProgrammableMock {
        response: Err(transport_err()),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut amb_rx = capture(&bus, EventType::OrderPlacementAmbiguousEvent);

    let _ = tokio::time::timeout(
        TIMEOUT,
        trade
            .cancel(ClOrdId::new("cancel-drop-01").unwrap())
            .via(Transport::Rest),
    )
    .await
    .expect("cancel timed out")
    .unwrap_err();

    assert!(
        tokio::time::timeout(SHORT, amb_rx.recv()).await.is_err(),
        "cancel transport-drop must NOT emit OrderPlacementAmbiguousEvent"
    );
}

#[tokio::test]
async fn rest_cancel_all_transport_drop_does_not_emit_ambiguous() {
    let mock = Arc::new(ProgrammableMock {
        response: Err(transport_err()),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut amb_rx = capture(&bus, EventType::OrderPlacementAmbiguousEvent);
    let mut sub_rx = capture(&bus, EventType::OrderSubmittedEvent);

    let _ = tokio::time::timeout(TIMEOUT, trade.cancel_all().via(Transport::Rest))
        .await
        .expect("cancel_all timed out")
        .unwrap_err();

    assert!(
        tokio::time::timeout(SHORT, amb_rx.recv()).await.is_err(),
        "cancel_all must NOT emit OrderPlacementAmbiguousEvent"
    );
    assert!(
        tokio::time::timeout(SHORT, sub_rx.recv()).await.is_err(),
        "cancel_all must NOT emit OrderSubmittedEvent"
    );
}

#[tokio::test]
async fn cancel_emit_guard_fires_on_mid_flight_drop() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({ "txid": ["X"] }))),
        block_until: Some(Arc::clone(&gate)),
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderCancellationAttempted);

    let cl = ClOrdId::allocate_v4();
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.cl_ord_id = Some(cl.clone());
    let fut = trade.order(req).via(Transport::Rest);
    {
        let mut boxed = Box::pin(fut.into_future());
        let _ = tokio::time::timeout(SHORT, &mut boxed).await;
        drop(boxed);
    }

    let payload = tokio::time::timeout(SHORT, rx.recv())
        .await
        .expect("no cancellation event")
        .expect("channel closed");
    match payload {
        EventPayload::OrderCancellationAttempted { cl_ord_id, op, .. } => {
            assert_eq!(cl_ord_id, cl);
            assert_eq!(op, OrderOp::AddOrder);
        }
        other => panic!("expected OrderCancellationAttempted, got {other:?}"),
    }
    assert!(
        tokio::time::timeout(SHORT, rx.recv()).await.is_err(),
        "cancellation must fire EXACTLY once"
    );
}

#[tokio::test]
async fn cancel_emit_guard_silent_on_normal_completion() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({
            "descr": { "order": "buy 0.001 BTC/USD @ limit 50000" },
            "txid": ["OK-TX"]
        }))),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderCancellationAttempted);

    let _ = tokio::time::timeout(
        TIMEOUT,
        trade
            .order(buy_limit("BTC/USD", "0.001", "50000"))
            .via(Transport::Rest),
    )
    .await
    .expect("order timed out")
    .unwrap();

    assert!(
        tokio::time::timeout(SHORT, rx.recv()).await.is_err(),
        "a completed order must NOT emit OrderCancellationAttempted"
    );
}

#[tokio::test]
async fn cancel_emit_guard_silent_on_never_polled_pending() {
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({ "txid": ["X"] }))),
        block_until: None,
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderCancellationAttempted);

    {
        let pending = trade
            .order(buy_limit("BTC/USD", "0.001", "50000"))
            .via(Transport::Rest);
        drop(pending);
    }

    assert!(
        tokio::time::timeout(SHORT, rx.recv()).await.is_err(),
        "a never-polled PendingTrade must NOT emit OrderCancellationAttempted"
    );
}

#[tokio::test]
async fn cancel_all_mid_flight_drop_does_not_emit_cancellation() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let mock = Arc::new(ProgrammableMock {
        response: Ok(ok_envelope(json!({ "count": 0 }))),
        block_until: Some(Arc::clone(&gate)),
    });
    let (trade, bus) = make_namespace_with_bus(mock);
    let mut rx = capture(&bus, EventType::OrderCancellationAttempted);

    {
        let mut boxed = Box::pin(trade.cancel_all().via(Transport::Rest).into_future());
        let _ = tokio::time::timeout(SHORT, &mut boxed).await;
        drop(boxed);
    }

    assert!(
        tokio::time::timeout(SHORT, rx.recv()).await.is_err(),
        "cancel_all mid-flight drop must NOT emit OrderCancellationAttempted"
    );
}

fn shorthand_sym() -> Symbol {
    Symbol::new("BTC/USDC").unwrap()
}

fn shorthand_form_val<'a>(form: &'a [(String, String)], k: &str) -> Option<&'a str> {
    form.iter()
        .find(|(key, _)| key == k)
        .map(|(_, v)| v.as_str())
}

fn shorthand_form_has(form: &[(String, String)], k: &str) -> bool {
    form.iter().any(|(key, _)| key == k)
}

// Stop-loss trigger rides in wire `price`, not `trigger`.
#[test]
fn shorthand_form_shapes() {
    type FormCase = (
        &'static str,
        Vec<(String, String)>,
        &'static str,
        Option<&'static str>,
        &'static str,
        bool,
    );
    let cases: Vec<FormCase> = vec![
        (
            "market_buy",
            OrderRequest::new(shorthand_sym(), dec("0.001"), Side::Buy)
                .order_type(OrderType::Market)
                .to_form(),
            "market",
            None,
            "0.001",
            true,
        ),
        (
            "market_sell",
            OrderRequest::new(shorthand_sym(), dec("0.5"), Side::Buy)
                .order_type(OrderType::Market)
                .to_form(),
            "market",
            None,
            "0.5",
            false,
        ),
        (
            "limit_buy",
            {
                let mut r = OrderRequest::new(shorthand_sym(), dec("0.001"), Side::Buy)
                    .order_type(OrderType::Limit);
                r.price = Some(dec("33000").into());
                r.to_form()
            },
            "limit",
            Some("33000"),
            "0.001",
            false,
        ),
        (
            "limit_sell",
            {
                let mut r = OrderRequest::new(shorthand_sym(), dec("0.2"), Side::Buy)
                    .order_type(OrderType::Limit);
                r.price = Some(dec("35000").into());
                r.to_form()
            },
            "limit",
            Some("35000"),
            "0.2",
            false,
        ),
        (
            "stop_loss_buy",
            {
                let mut r = OrderRequest::new(shorthand_sym(), dec("0.0001"), Side::Buy)
                    .order_type(OrderType::StopLoss);
                r.price = Some(dec("200000").into());
                r.to_form()
            },
            "stop-loss",
            Some("200000"),
            "0.0001",
            true,
        ),
        (
            "stop_loss_sell",
            {
                let mut r = OrderRequest::new(shorthand_sym(), dec("0.0001"), Side::Buy)
                    .order_type(OrderType::StopLoss);
                r.price = Some(dec("15000").into());
                r.to_form()
            },
            "stop-loss",
            Some("15000"),
            "0.0001",
            true,
        ),
    ];

    for (label, form, ordertype, price, volume, no_trigger) in &cases {
        assert_eq!(
            shorthand_form_val(form, "ordertype"),
            Some(*ordertype),
            "{label}: ordertype"
        );
        assert_eq!(
            shorthand_form_val(form, "pair"),
            Some("BTC/USDC"),
            "{label}: pair"
        );
        assert_eq!(
            shorthand_form_val(form, "volume"),
            Some(*volume),
            "{label}: volume"
        );
        match price {
            Some(p) => assert_eq!(
                shorthand_form_val(form, "price"),
                Some(*p),
                "{label}: price"
            ),
            None => assert!(
                !shorthand_form_has(form, "price"),
                "{label}: must have no price key"
            ),
        }
        if *no_trigger {
            assert!(
                !shorthand_form_has(form, "trigger"),
                "{label}: must have no trigger key"
            );
        }
    }
}

fn shorthand_canned() -> serde_json::Value {
    json!({
        "error": [],
        "result": {
            "descr": { "order": "buy 0.001 BTC/USDC @ market" },
            "txid": ["OSRT1-XXXXX-YYYYYY"]
        }
    })
}

#[test]
fn shorthand_builder_shapes() {
    let (trade, _) = make_namespace(shorthand_canned());

    type Shape = (OrderType, Option<Price>, Option<TriggerKind>, Option<usize>);
    let rows: Vec<(&str, Shape, OrderType, Option<&str>)> = vec![
        {
            let p = trade.market_buy(shorthand_sym(), dec("0.001"));
            let r = p.request();
            (
                "market_buy",
                (
                    r.order_type,
                    r.price,
                    r.trigger,
                    p.cl_ord_id().map(|c| c.as_str().len()),
                ),
                OrderType::Market,
                None,
            )
        },
        {
            let p = trade.market_sell(shorthand_sym(), dec("0.5"));
            let r = p.request();
            (
                "market_sell",
                (
                    r.order_type,
                    r.price,
                    r.trigger,
                    p.cl_ord_id().map(|c| c.as_str().len()),
                ),
                OrderType::Market,
                None,
            )
        },
        {
            let p = trade.limit_buy(shorthand_sym(), dec("0.001"), dec("33000"));
            let r = p.request();
            (
                "limit_buy",
                (
                    r.order_type,
                    r.price,
                    r.trigger,
                    p.cl_ord_id().map(|c| c.as_str().len()),
                ),
                OrderType::Limit,
                Some("33000"),
            )
        },
        {
            let p = trade.limit_sell(shorthand_sym(), dec("0.2"), dec("35000"));
            let r = p.request();
            (
                "limit_sell",
                (
                    r.order_type,
                    r.price,
                    r.trigger,
                    p.cl_ord_id().map(|c| c.as_str().len()),
                ),
                OrderType::Limit,
                Some("35000"),
            )
        },
        {
            let p = trade.stop_loss_buy(shorthand_sym(), dec("0.0001"), dec("200000"));
            let r = p.request();
            (
                "stop_loss_buy",
                (
                    r.order_type,
                    r.price,
                    r.trigger,
                    p.cl_ord_id().map(|c| c.as_str().len()),
                ),
                OrderType::StopLoss,
                Some("200000"),
            )
        },
        {
            let p = trade.stop_loss_sell(shorthand_sym(), dec("0.0001"), dec("15000"));
            let r = p.request();
            (
                "stop_loss_sell",
                (
                    r.order_type,
                    r.price,
                    r.trigger,
                    p.cl_ord_id().map(|c| c.as_str().len()),
                ),
                OrderType::StopLoss,
                Some("15000"),
            )
        },
    ];

    for (label, (ot, price, trig, cl_len), exp_ot, exp_price) in rows {
        assert_eq!(ot, exp_ot, "{label}: order_type");
        match exp_price {
            Some(p) => assert_eq!(price, Some(dec(p).into()), "{label}: price"),
            None => assert_eq!(price, None, "{label}: price must be None"),
        }
        assert_eq!(trig, None, "{label}: trigger must be None");
        assert_eq!(cl_len, Some(36), "{label}: cl_ord_id must be a 36-char v4");
    }
}

fn batch_val<'a>(form: &'a [(String, String)], k: &str) -> Option<&'a str> {
    form.iter()
        .find(|(key, _)| key == k)
        .map(|(_, v)| v.as_str())
}

fn batch_has(form: &[(String, String)], k: &str) -> bool {
    form.iter().any(|(key, _)| key == k)
}

#[tokio::test]
async fn batch_rest_rejects_margin_entry() {
    let (trade, _) = make_namespace(json!({ "error": [], "result": { "orders": [] } }));
    let mut req = two_entry_batch();
    req.orders[1].margin = true;
    let err = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(req).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .unwrap_err();
    assert!(
        matches!(&err, TradeError::RestUnsupportedOrderField { field } if *field == "margin"),
        "expected RestUnsupportedOrderField{{field:\"margin\"}}, got {err:?}"
    );
}

fn two_entry_batch() -> AddOrderBatchRequest {
    AddOrderBatchRequest {
        pair: Symbol::new("BTC/USD").unwrap(),
        orders: vec![
            base_entry(
                Side::Buy,
                OrderType::Limit,
                dec("0.1"),
                Some(dec("50000").into()),
            ),
            base_entry(
                Side::Sell,
                OrderType::Limit,
                dec("0.2"),
                Some(dec("60000").into()),
            ),
        ],
        deadline: None,
        validate: false,
    }
}

#[test]
fn batch_to_form_bracket_round_trips_two_entries() {
    let req = two_entry_batch();
    let form = req.to_form();

    assert_eq!(batch_val(&form, "pair"), Some("BTC/USD"));

    assert_eq!(batch_val(&form, "orders[0][type]"), Some("buy"));
    assert_eq!(batch_val(&form, "orders[0][ordertype]"), Some("limit"));
    assert_eq!(batch_val(&form, "orders[0][volume]"), Some("0.1"));
    assert_eq!(batch_val(&form, "orders[0][price]"), Some("50000"));

    assert_eq!(batch_val(&form, "orders[1][type]"), Some("sell"));
    assert_eq!(batch_val(&form, "orders[1][ordertype]"), Some("limit"));
    assert_eq!(batch_val(&form, "orders[1][volume]"), Some("0.2"));
    assert_eq!(batch_val(&form, "orders[1][price]"), Some("60000"));

    assert!(!batch_has(&form, "validate"));
    assert!(!batch_has(&form, "deadline"));
}

#[test]
fn batch_to_form_conditional_close_on_entry_emits_bracket_keys() {
    let mut req = two_entry_batch();
    req.orders[0].conditional_close = Some(ConditionalClose {
        ordertype: CloseOrderType::StopLoss,
        price: dec("45000").into(),
        price2: None,
    });
    let form = req.to_form();

    assert_eq!(
        batch_val(&form, "orders[0][close][ordertype]"),
        Some("stop-loss")
    );
    assert_eq!(batch_val(&form, "orders[0][close][price]"), Some("45000"));
    assert!(!batch_has(&form, "orders[0][close][price2]"));
    assert!(!batch_has(&form, "orders[1][close][ordertype]"));
}

#[test]
fn batch_validate_size_guard_n_1_is_err_n_2_ok_n_15_ok_n_16_err() {
    use crate::api::trade::error::TradeError;

    let make = |n: usize| AddOrderBatchRequest {
        pair: Symbol::new("BTC/USD").unwrap(),
        orders: (0..n)
            .map(|_| base_entry(Side::Buy, OrderType::Market, dec("0.001"), None))
            .collect(),
        deadline: None,
        validate: false,
    };

    assert_eq!(
        make(1).validate(),
        Err(TradeError::BatchSizeOutOfRange { min: 2, max: 15 })
    );
    assert_eq!(make(2).validate(), Ok(()));
    assert_eq!(make(15).validate(), Ok(()));
    assert_eq!(
        make(16).validate(),
        Err(TradeError::BatchSizeOutOfRange { min: 2, max: 15 })
    );
}

#[tokio::test]
async fn order_batch_row_count_mismatch_is_malformed_not_miszipped() {
    let (trade, _mock) = make_namespace(json!({
        "error": [],
        "result": {
            "orders": [
                {
                    "descr": { "order": "buy 0.1 BTC/USD @ limit 50000" },
                    "txid": "OBATCH1-AAAA-BBBBBB"
                }
            ]
        }
    }));
    let err = tokio::time::timeout(
        TIMEOUT,
        trade.order_batch(two_entry_batch()).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .expect_err("count mismatch must error");
    assert!(matches!(err, TradeError::MalformedResponse { .. }));
}

#[test]
fn decode_ws_batch_row_count_mismatch_is_malformed_not_miszipped() {
    let resp = crate::conn::managed_connection::WsResponse {
        req_id: 14,
        success: true,
        result: json!([
            {"cl_ord_id": "3923aaaa-0000-4000-8000-000000000001", "order_id": "OA5C2Q-2QTWU-BSR23N"}
        ]),
        error: None,
    };
    let err = super::executors::decode_and_guard_ws_batch(2, resp)
        .expect_err("count mismatch must error on the WS arm");
    assert!(matches!(err, TradeError::MalformedResponse { .. }));
}

#[tokio::test]
async fn order_batch_happy_2_orders() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": {
            "orders": [
                {
                    "descr": { "order": "buy 0.1 BTC/USD @ limit 50000" },
                    "txid": "OBATCH1-AAAA-BBBBBB"
                },
                {
                    "descr": { "order": "sell 0.2 BTC/USD @ limit 60000" },
                    "txid": "OBATCH2-CCCC-DDDDDD"
                }
            ]
        }
    }));
    let resp = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(two_entry_batch()).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .expect("order_batch should succeed");

    assert_eq!(resp.orders.len(), 2);
    assert_eq!(
        resp.orders[0].txid.as_ref().unwrap().as_str(),
        "OBATCH1-AAAA-BBBBBB"
    );
    assert_eq!(
        resp.orders[0].descr.order.as_deref(),
        Some("buy 0.1 BTC/USD @ limit 50000")
    );
    assert_eq!(
        resp.orders[1].txid.as_ref().unwrap().as_str(),
        "OBATCH2-CCCC-DDDDDD"
    );
    assert_eq!(
        resp.orders[1].descr.order.as_deref(),
        Some("sell 0.2 BTC/USD @ limit 60000")
    );
    assert!(resp.orders[0].error.is_none());
    assert!(resp.orders[1].error.is_none());

    let (path, body) = mock.last_post.lock().unwrap().clone().unwrap();
    assert_eq!(path, "/0/private/AddOrderBatch");
    assert!(
        body.contains("pair=BTC%2FUSD"),
        "body should have pair: {}",
        body
    );
    assert!(
        body.contains("orders%5B0%5D%5Btype%5D=buy") || body.contains("orders[0][type]=buy"),
        "body should have orders[0][type]: {}",
        body
    );
    assert!(
        body.contains("orders%5B1%5D%5Btype%5D=sell") || body.contains("orders[1][type]=sell"),
        "body should have orders[1][type]: {}",
        body
    );
}

#[tokio::test]
async fn order_batch_default_transport_routes_to_ws() {
    let (trade, _) = make_namespace(json!({ "error": [], "result": { "orders": [] } }));
    let err = trade.order_batch(two_entry_batch()).await.unwrap_err();
    assert!(
        matches!(err, TradeError::WsSurfaceUnavailable),
        "expected the default to route to WS (no-surface here), got: {err:?}"
    );
}

#[tokio::test]
async fn prefer_rest_for_orders_knob_routes_default_order_to_rest() {
    let (mut trade, mock) = make_namespace(json!({
        "error": [],
        "result": {
            "descr": { "order": "buy 0.001 BTC/USD @ limit 50000" },
            "txid": ["OBE25Z-GZRP2-6YWYZI"]
        }
    }));
    let mut knobs = crate::build::knobs::Knobs::defaults();
    knobs.prefer_rest_for_orders = true;
    trade.knobs = Arc::new(knobs);

    trade
        .order(buy_limit("BTC/USD", "0.001", "50000"))
        .await
        .expect("knob=true routes the default order to the REST path");
    assert!(
        mock.last_post.lock().unwrap().is_some(),
        "prefer_rest_for_orders=true must send the default order over REST"
    );
}

#[test]
fn decode_ws_batch_add_partial_placement_returns_ok_with_live_sibling() {
    let resp = crate::conn::managed_connection::WsResponse {
        req_id: 13,
        success: false,
        result: json!([
            {"cl_ord_id": "3923aaaa-0000-4000-8000-000000000001", "order_id": "OA5C2Q-2QTWU-BSR23N"},
            {"error": "EOrder:Insufficient funds"}
        ]),
        error: None,
    };
    let decoded = super::ws_compose::decode_ws_batch_add(resp).expect("partial placement is Ok");
    assert_eq!(decoded.orders.len(), 2);
    assert_eq!(
        decoded.orders[0].txid.as_ref().unwrap().as_str(),
        "OA5C2Q-2QTWU-BSR23N"
    );
    assert!(decoded.orders[0].error.is_none());
    assert!(decoded.orders[1].txid.is_none());
    assert_eq!(
        decoded.orders[1].error.as_deref(),
        Some("EOrder:Insufficient funds")
    );
}

#[test]
fn decode_ws_batch_add_validate_mode_is_ok_with_no_txid() {
    let resp = crate::conn::managed_connection::WsResponse {
        req_id: 11,
        success: true,
        result: json!([
            {"validation": "Validated Successfully"},
            {"validation": "Validated Successfully"}
        ]),
        error: None,
    };
    let decoded = super::ws_compose::decode_ws_batch_add(resp).expect("validate success is Ok");
    assert_eq!(decoded.orders.len(), 2);
    assert!(
        decoded
            .orders
            .iter()
            .all(|o| o.txid.is_none() && o.error.is_none())
    );
}

#[test]
fn decode_ws_batch_add_validation_reject_is_err() {
    let resp = crate::conn::managed_connection::WsResponse {
        req_id: 12,
        success: false,
        result: serde_json::Value::Null,
        error: Some("Limit_price(s) not found".to_string()),
    };
    assert!(super::ws_compose::decode_ws_batch_add(resp).is_err());
}

#[test]
fn compose_batch_add_hoists_symbol_and_strips_it_per_order() {
    let params = super::ws_compose::compose_batch_add_params(&two_entry_batch()).expect("compose");
    assert_eq!(
        params.get("symbol").and_then(|v| v.as_str()),
        Some("BTC/USD")
    );
    let orders = params
        .get("orders")
        .and_then(|v| v.as_array())
        .expect("orders array");
    assert_eq!(orders.len(), 2);
    for o in orders {
        assert!(
            o.get("symbol").is_none(),
            "per-order object must not repeat symbol"
        );
        assert!(o.get("order_type").is_some());
        assert!(o.get("side").is_some());
    }
}

#[tokio::test]
async fn order_batch_validate_mode_no_txid() {
    let (trade, _) = make_namespace(json!({
        "error": [],
        "result": {
            "orders": [
                { "descr": { "order": "buy 0.1 BTC/USD @ limit 50000" } },
                { "descr": { "order": "sell 0.2 BTC/USD @ limit 60000" } }
            ]
        }
    }));
    let mut req = two_entry_batch();
    req.validate = true;
    let resp = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(req).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .expect("validate-mode batch should succeed");

    assert_eq!(resp.orders.len(), 2);
    assert!(
        resp.orders[0].txid.is_none(),
        "validate mode must have no txid"
    );
    assert!(
        resp.orders[1].txid.is_none(),
        "validate mode must have no txid"
    );
    assert_eq!(
        resp.orders[0].descr.order.as_deref(),
        Some("buy 0.1 BTC/USD @ limit 50000")
    );
}

#[tokio::test]
async fn order_batch_per_line_error_surfaces_per_row_not_whole_batch_err() {
    let (trade, _) = make_namespace(json!({
        "error": [],
        "result": {
            "orders": [
                {},
                { "error": "EGeneral:Invalid arguments:volume minimum not met" }
            ]
        }
    }));
    let resp = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(two_entry_batch()).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .expect("a per-line error MUST NOT fail the whole batch");

    assert_eq!(resp.orders.len(), 2);
    assert!(resp.orders[0].txid.is_none());
    assert!(resp.orders[0].error.is_none());
    assert!(resp.orders[1].txid.is_none());
    assert_eq!(
        resp.orders[1].error.as_deref(),
        Some("EGeneral:Invalid arguments:volume minimum not met")
    );
}

#[tokio::test]
async fn order_batch_allocates_cl_ord_id_per_entry() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": {
            "orders": [
                {
                    "descr": { "order": "buy 0.1 BTC/USD @ limit 50000" },
                    "txid": "OBATCH1-AAAA-BBBBBB"
                },
                {
                    "descr": { "order": "sell 0.2 BTC/USD @ limit 60000" },
                    "txid": "OBATCH2-CCCC-DDDDDD"
                }
            ]
        }
    }));
    let req = two_entry_batch();
    let pending = trade.order_batch(req).via(Transport::Rest);
    assert!(
        pending.cl_ord_id().is_none(),
        "batch cl_ord_id() is None at batch level"
    );

    let _ = tokio::time::timeout(std::time::Duration::from_millis(500), pending)
        .await
        .expect("order_batch timed out")
        .expect("should succeed");

    let (_, body) = mock.last_post.lock().unwrap().clone().unwrap();
    let has_entry0_cl_ord_id =
        body.contains("orders%5B0%5D%5Bcl_ord_id%5D=") || body.contains("orders[0][cl_ord_id]=");
    let has_entry1_cl_ord_id =
        body.contains("orders%5B1%5D%5Bcl_ord_id%5D=") || body.contains("orders[1][cl_ord_id]=");
    assert!(
        has_entry0_cl_ord_id,
        "entry 0 must have an allocated cl_ord_id in body: {}",
        body
    );
    assert!(
        has_entry1_cl_ord_id,
        "entry 1 must have an allocated cl_ord_id in body: {}",
        body
    );
}

#[tokio::test]
async fn order_batch_suppresses_auto_cl_ord_id_for_conditional_close_and_userref_entries() {
    let (trade, mock) = make_namespace(json!({
        "error": [],
        "result": {
            "orders": [
                { "descr": { "order": "buy 0.1 BTC/USD @ limit 50000" }, "txid": "OBATCH0" },
                { "descr": { "order": "sell 0.2 BTC/USD @ limit 60000" }, "txid": "OBATCH1" },
                { "descr": { "order": "buy 0.3 BTC/USD @ limit 55000" }, "txid": "OBATCH2" }
            ]
        }
    }));

    let mut req = two_entry_batch();
    req.orders[0].conditional_close = Some(ConditionalClose {
        ordertype: CloseOrderType::StopLoss,
        price: dec("45000").into(),
        price2: None,
    });
    req.orders[1].userref = Some(42);
    req.orders.push(base_entry(
        Side::Buy,
        OrderType::Limit,
        dec("0.3"),
        Some(dec("55000").into()),
    ));

    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(req).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .expect("should succeed");

    let (_, body) = mock.last_post.lock().unwrap().clone().unwrap();
    let has_cl = |i: usize| {
        body.contains(&format!("orders%5B{i}%5D%5Bcl_ord_id%5D="))
            || body.contains(&format!("orders[{i}][cl_ord_id]="))
    };

    assert!(
        !has_cl(0),
        "conditional-close entry must NOT get an auto cl_ord_id: {body}"
    );
    assert!(
        !has_cl(1),
        "userref entry must NOT get an auto cl_ord_id: {body}"
    );
    assert!(
        has_cl(2),
        "ordinary entry MUST still get an auto cl_ord_id: {body}"
    );

    assert!(
        body.contains("orders%5B0%5D%5Bclose%5D%5Bordertype%5D=")
            || body.contains("orders[0][close][ordertype]="),
        "entry 0 must carry its conditional-close clause: {body}"
    );
    assert!(
        body.contains("orders%5B1%5D%5Buserref%5D=42") || body.contains("orders[1][userref]=42"),
        "entry 1 must carry userref: {body}"
    );
}

#[test]
fn batch_to_form_cl_ord_id_in_entry_emits_key() {
    let mut req = two_entry_batch();
    let id = ClOrdId::new("batch-entry-01").unwrap();
    req.orders[0].cl_ord_id = Some(id.clone());
    let form = req.to_form();
    assert_eq!(batch_val(&form, "orders[0][cl_ord_id]"), Some(id.as_str()));
    assert!(!batch_has(&form, "orders[1][cl_ord_id]"));
}

/// A transport mock that returns a different canned response per `cl_ord_id`
/// value found in the request body. Unrecognised cl_ord_ids return an error
/// envelope. Used only by the cancel_batch partial-failure test.
struct PerIdMock {
    /// Maps the string value of `cl_ord_id=<val>` in the POST body to the
    /// response to return for that call.
    responses: HashMap<String, serde_json::Value>,
}

#[async_trait::async_trait]
impl HttpTransport for PerIdMock {
    async fn get_json(
        &self,
        _path: &str,
        _query: &[(&str, &str)],
    ) -> Result<serde_json::Value, TransportError> {
        panic!("cancel_batch tests never hit GET");
    }

    async fn post_form_signed(
        &self,
        _path: &str,
        body: &str,
        _api_key_header: &str,
        _api_sign_header: &str,
    ) -> Result<serde_json::Value, TransportError> {
        let id = body
            .split('&')
            .find(|seg| seg.starts_with("cl_ord_id="))
            .map(|seg| seg.trim_start_matches("cl_ord_id=").to_string())
            .unwrap_or_default();
        if let Some(resp) = self.responses.get(&id) {
            Ok(resp.clone())
        } else {
            Ok(json!({ "error": ["EOrder:Invalid order"], "result": {} }))
        }
    }
}

fn make_namespace_per_id(responses: HashMap<String, serde_json::Value>) -> TradeNamespace {
    let mock = Arc::new(PerIdMock { responses });
    let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
    let (auth, _bus, api_rl, trading_rl) = build_deps(&clock);
    let rest = Arc::new(RestSurface::new_with_index(
        Arc::clone(&mock) as Arc<dyn HttpTransport>,
        auth,
        api_rl,
        trading_rl,
        clock,
        Arc::new(crate::rate_limit::ClOrdIdPairIndex::new(1024)),
        test_no_retry_engine(),
        std::time::Duration::from_secs(30),
    ));
    TradeNamespace::new(rest)
}

#[tokio::test]
async fn cancel_batch_all_succeed_preserves_order() {
    let canned = ok(json!({ "count": 1, "pending": false }));
    let (trade, _) = make_namespace(canned);

    let ids: Vec<ClOrdId> = ["cb-01", "cb-02", "cb-03"]
        .iter()
        .map(|s| ClOrdId::new(*s).unwrap())
        .collect();

    let resp = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.cancel_batch(ids.clone()).via(Transport::Rest),
    )
    .await
    .expect("cancel_batch timed out")
    .expect("cancel_batch returned Err");

    assert_eq!(
        resp.results.len(),
        3,
        "must have one result per input cl_ord_id"
    );
    for (i, result) in resp.results.iter().enumerate() {
        match result {
            BatchResult::Ok(r) => {
                assert_eq!(r.count, 1, "slot {i}: count should be 1");
                assert!(!r.pending, "slot {i}: pending should be false");
            }
            BatchResult::Err(e) => panic!("slot {i}: expected Ok, got Err({e:?})"),
        }
    }
}

#[tokio::test]
async fn cancel_batch_partial_failure_per_line() {
    let mut responses = HashMap::new();
    responses.insert(
        "ok-1".to_string(),
        ok(json!({ "count": 1, "pending": false })),
    );
    responses.insert(
        "ok-2".to_string(),
        ok(json!({ "count": 1, "pending": false })),
    );

    let trade = make_namespace_per_id(responses);

    let ids = vec![
        ClOrdId::new("ok-1").unwrap(),
        ClOrdId::new("fail-1").unwrap(),
        ClOrdId::new("ok-2").unwrap(),
    ];

    let resp = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.cancel_batch(ids.clone()).via(Transport::Rest),
    )
    .await
    .expect("cancel_batch timed out")
    .expect("cancel_batch must return Ok even with per-line failures");

    assert_eq!(resp.results.len(), 3, "must preserve input count");

    match &resp.results[0] {
        BatchResult::Ok(r) => assert_eq!(r.count, 1),
        BatchResult::Err(e) => panic!("slot 0 (ok-1): expected Ok, got Err({e:?})"),
    }

    match &resp.results[1] {
        BatchResult::Err(e) => {
            assert_eq!(
                e.cl_ord_id,
                ClOrdId::new("fail-1").unwrap(),
                "Err must carry the failing cl_ord_id"
            );
        }
        BatchResult::Ok(r) => panic!("slot 1 (fail-1): expected Err, got Ok({r:?})"),
    }

    match &resp.results[2] {
        BatchResult::Ok(r) => assert_eq!(r.count, 1),
        BatchResult::Err(e) => panic!("slot 2 (ok-2): expected Ok, got Err({e:?})"),
    }
}

#[tokio::test]
async fn cancel_batch_empty_is_vacuous_success() {
    let (trade, _) = make_namespace(ok(json!({ "count": 0 })));
    let resp = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        trade.cancel_batch(vec![]).via(Transport::Rest),
    )
    .await
    .expect("cancel_batch timed out")
    .expect("empty cancel_batch must return Ok");
    assert!(
        resp.results.is_empty(),
        "empty cancel_batch yields zero results, got {:?}",
        resp.results
    );
}

/// Returns a canned error response (never used for WS-guarded paths).
fn ws_guard_canned() -> serde_json::Value {
    json!({ "error": [], "result": { "descr": { "order": "x" }, "txid": ["X"] } })
}

#[tokio::test]
async fn ws_guard_validate_buy_passes_guard() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.validate = true;
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        !matches!(&err, TradeError::WsUnsupportedOrderField { field } if *field == "validate"),
        "validate must NOT be guarded on the WS path; got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_validate_sell_passes_guard() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = OrderRequest::new(Symbol::new("BTC/USD").unwrap(), dec("0.001"), Side::Buy)
        .order_type(OrderType::Limit);
    req.price = Some(dec("60000").into());
    req.validate = true;
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        !matches!(&err, TradeError::WsUnsupportedOrderField { field } if *field == "validate"),
        "validate must NOT be guarded on the sell WS path; got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_leverage_returns_typed_error() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.leverage = Some(5);
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        matches!(&err, TradeError::WsUnsupportedOrderField { field } if *field == "leverage"),
        "expected WsUnsupportedOrderField{{field:\"leverage\"}}, got {err:?}"
    );
}

/// REST amend renders `display_qty` on the wire; `None` omits the key.
#[test]
fn amend_to_form_renders_display_qty() {
    let mut req = OrderAmendRequest::new(ClOrdId::new("amend-dq-1").unwrap());
    req.order_volume = Some(dec("0.002"));
    req.display_qty = Some(dec("0.001"));
    assert!(
        req.to_form()
            .contains(&("display_qty".to_string(), "0.001".to_string()))
    );

    let mut none = OrderAmendRequest::new(ClOrdId::new("amend-dq-2").unwrap());
    none.order_volume = Some(dec("0.002"));
    assert!(none.to_form().iter().all(|(k, _)| k != "display_qty"));
}

#[tokio::test]
async fn ws_guard_amend_display_qty_returns_typed_error() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = OrderAmendRequest::new(crate::types::ClOrdId::allocate_v4());
    req.order_volume = Some("0.002".parse().unwrap());
    req.display_qty = Some("0.001".parse().unwrap());
    let err = trade
        .order_amend(req)
        .via(Transport::WsV2Auth)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, TradeError::WsUnsupportedOrderField { field } if *field == "display_qty"),
        "expected WsUnsupportedOrderField{{field:\"display_qty\"}}, got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_reduce_only_still_rejected_via_leverage_guard() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.leverage = Some(5);
    req.reduce_only = Some(true);
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        matches!(&err, TradeError::WsUnsupportedOrderField { field } if *field == "leverage"),
        "reduce-only/margin order must be rejected via leverage guard; got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_relative_close_price_returns_typed_error() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.conditional_close = Some(ConditionalClose::new(
        CloseOrderType::Limit,
        Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("150"),
        },
    ));
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        matches!(&err, TradeError::WsUnsupportedOrderField { field } if *field == "close[price]"),
        "a relative close price must be WS-guarded; got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_absolute_close_price_passes_guard() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.conditional_close = Some(ConditionalClose::new(
        CloseOrderType::Limit,
        Price::Absolute(dec("60000")),
    ));
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    // An absolute close price is WS-representable — the guard must NOT fire.
    assert!(
        !matches!(&err, TradeError::WsUnsupportedOrderField { .. }),
        "an absolute close price must NOT be WS-guarded; got {err:?}"
    );
}

#[tokio::test]
async fn rest_rejects_margin_order() {
    let (trade, _) = make_namespace(json!({ "error": [], "result": {} }));
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.margin = true;
    let err = trade.order(req).via(Transport::Rest).await.unwrap_err();
    assert!(
        matches!(&err, TradeError::RestUnsupportedOrderField { field } if *field == "margin"),
        "expected RestUnsupportedOrderField{{field:\"margin\"}}, got {err:?}"
    );
}

#[tokio::test]
async fn ws_margin_reduce_only_order_passes_guard() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.margin = true;
    req.reduce_only = Some(true);
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        !matches!(&err, TradeError::WsUnsupportedOrderField { .. }),
        "a margin+reduce_only order must pass the WS guard (WS-native); got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_trigger_order_type_passes_guard() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.order_type = OrderType::StopLoss;
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        !matches!(&err, TradeError::WsUnsupportedOrderField { field } if *field == "order_type"),
        "trigger order_type must NOT be guarded on WS path; got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_price2_passes_guard() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.price2 = Some(dec("49000").into());
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        !matches!(&err, TradeError::WsUnsupportedOrderField { field } if *field == "price2"),
        "price2 must NOT be guarded on WS path; got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_non_post_oflag_passes_guard() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.oflags = vec![OFlag::Fcib];
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        !matches!(&err, TradeError::WsUnsupportedOrderField { field } if *field == "oflags"),
        "non-Post oflags must NOT be guarded on WS path; got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_time_in_force_passes_guard() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.time_in_force = Some(TimeInForce::Ioc);
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        !matches!(&err, TradeError::WsUnsupportedOrderField { field } if *field == "time_in_force"),
        "time_in_force must NOT be guarded on WS path; got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_representable_only_fields_pass_guard() {
    let (trade, _) = make_namespace(ws_guard_canned());
    let mut req = buy_limit("BTC/USD", "0.001", "50000");
    req.oflags = vec![OFlag::Post];
    let err = trade.order(req).via(Transport::WsV2Auth).await.unwrap_err();
    assert!(
        !matches!(&err, TradeError::WsUnsupportedOrderField { .. }),
        "guard must NOT fire for representable-only fields; got {err:?}"
    );
}

#[tokio::test]
async fn ws_guard_does_not_fire_on_rest_path_for_validate() {
    let (trade, _) = make_namespace(json!({
        "error": [],
        "result": {
            "descr": { "order": "buy 0.0001 BTC/USD @ limit 20000" }
        }
    }));
    let mut req = buy_limit("BTC/USD", "0.0001", "20000");
    req.validate = true;
    let resp = trade.order(req).via(Transport::Rest).await.unwrap();
    assert!(resp.txid.is_none(), "validate mode returns no txid");
}

/// A clock pinned to a fixed `Duration` since the monotonic epoch.
/// Used by rate-limit tests so `clock_now()` in production code
/// returns a deterministic instant with no process-elapsed decay.
struct FixedClock(std::time::Duration);
impl crate::clock::Clock for FixedClock {
    fn now(&self) -> crate::types::MonotonicInstant {
        crate::types::MonotonicInstant(self.0)
    }
}

/// Build a namespace exposing its `ClOrdIdPairIndex` and trading tracker so tests
/// can seed the index and pre-drain the counter. `clock` is injected so pre-drain,
/// index `sent_at` and production `clock_now()` share one deterministic instant.
fn make_namespace_with_index_refs(
    canned: serde_json::Value,
    clock: Arc<dyn crate::clock::Clock>,
) -> (
    TradeNamespace,
    Arc<crate::rate_limit::ClOrdIdPairIndex>,
    Arc<crate::rate_limit::SpotTradingRateLimitTracker>,
) {
    let mock = Arc::new(CapturingMock {
        canned,
        last_post: Mutex::new(None),
    });
    let (auth, _bus, api_rl, trading_rl) = build_deps(&clock);
    let cl_ord_id_index = Arc::new(crate::rate_limit::ClOrdIdPairIndex::new(1024));
    let rest = Arc::new(RestSurface::new_with_index(
        Arc::clone(&mock) as Arc<dyn HttpTransport>,
        auth,
        api_rl,
        Arc::clone(&trading_rl),
        clock,
        Arc::clone(&cl_ord_id_index),
        test_no_retry_engine(),
        std::time::Duration::from_secs(30),
    ));
    (TradeNamespace::new(rest), cl_ord_id_index, trading_rl)
}

/// `cancel_all` charges every tracked pair's trading counter +1, account-wide,
/// saturating and non-rejecting. Two pairs seeded; both charged +1 after a REST
/// `cancel_all`, which is never rate-limit-blocked.
#[tokio::test]
async fn cancel_all_rest_charges_every_tracked_pair_account_wide() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (trade, _idx, tracker) = make_namespace_with_index_refs(
        json!({ "error": [], "result": { "count": 3 } }),
        Arc::clone(&fixed_clock),
    );

    let api_key = ApiKey::new("test-api-key-1234567890");
    let btc = Symbol::new("BTC/USD").unwrap();
    let eth = Symbol::new("ETH/USD").unwrap();
    let t0 = MonotonicInstant(Duration::from_secs(0));

    tracker
        .consume(Scope::Pair(api_key.clone(), btc.clone()), 10.0, t0)
        .unwrap();
    tracker
        .consume(Scope::Pair(api_key.clone(), eth.clone()), 20.0, t0)
        .unwrap();

    let resp = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.cancel_all().via(Transport::Rest),
    )
    .await
    .expect("cancel_all did not resolve in time")
    .expect("cancel_all must not be rate-limit-blocked");
    assert_eq!(resp.count, 3);

    let h_btc = tracker
        .headroom(Scope::Pair(api_key.clone(), btc), t0)
        .unwrap();
    assert!((h_btc - 49.0).abs() < 1e-6, "BTC headroom {h_btc}");
    let h_eth = tracker.headroom(Scope::Pair(api_key, eth), t0).unwrap();
    assert!((h_eth - 39.0).abs() < 1e-6, "ETH headroom {h_eth}");
}

/// `AddOrderBatch` charges the trading counter n/2 (not n) for an n-order batch.
/// A 2-order batch drops headroom by exactly 1.0 (= 2/2); a cost=n charge would
/// drop it by 2.0.
#[tokio::test]
async fn order_batch_rest_charges_trading_counter_n_over_2() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (trade, _idx, tracker) = make_namespace_with_index_refs(
        json!({ "error": [], "result": { "orders": [
            { "descr": { "order": "buy 0.1 BTC/USD @ limit 50000" }, "txid": "OB1-AAAA-BBBBBB" },
            { "descr": { "order": "sell 0.2 BTC/USD @ limit 60000" }, "txid": "OB2-CCCC-DDDDDD" }
        ] } }),
        Arc::clone(&fixed_clock),
    );

    let api_key = ApiKey::new("test-api-key-1234567890");
    let pair = Symbol::new("BTC/USD").unwrap();
    let t0 = MonotonicInstant(Duration::from_secs(0));

    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(two_entry_batch()).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .expect("order_batch should succeed");

    let h = tracker.headroom(Scope::Pair(api_key, pair), t0).unwrap();
    assert!(
        (h - 59.0).abs() < 1e-6,
        "2-order batch must charge n/2 = 1.0 (headroom 59), got {h}"
    );
}

/// REST `cancel_all` with `EOrder:Domain rate limit exceeded` reactively snaps
/// the trading tracker at `Scope::ApiKey` — the same domain backstop the WS path
/// carries.
#[tokio::test]
async fn cancel_all_rest_domain_rate_limit_snaps_trading_tracker() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (trade, _idx, trading, _api) = make_namespace_for_snap_tests(
        vec!["EOrder:Domain rate limit exceeded"],
        Arc::clone(&fixed_clock),
    );

    let pair = Symbol::new("BTC/USD").unwrap();
    let api_key = ApiKey::new("test-api-key-1234567890");
    let t0 = MonotonicInstant(Duration::from_secs(0));

    trading
        .consume(Scope::Pair(api_key.clone(), pair.clone()), 1.0, t0)
        .expect("initial consume must succeed");

    let _err = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.cancel_all().via(Transport::Rest),
    )
    .await
    .expect("cancel_all timed out")
    .unwrap_err();

    let headroom_after = trading
        .headroom(Scope::Pair(api_key, pair), t0)
        .unwrap_or(60.0);
    assert!(
        headroom_after < f64::EPSILON,
        "cancel_all REST domain rejection MUST snap the trading tracker \
         (headroom_after={headroom_after})"
    );
}

/// HIT: cancelling a known cl_ord_id when the trading tracker is at (cap - cancel
/// cost) pre-charges over cap and returns `TradeError::RateLimited` before the wire
/// send. `FixedClock` at T=0 pins the age so cost is deterministic (no decay flake).
#[tokio::test]
async fn cancel_rest_hit_pre_charges_and_rejects_when_at_cap() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));

    let (trade, idx, tracker) = make_namespace_with_index_refs(
        json!({ "error": [], "result": { "count": 1 } }),
        Arc::clone(&fixed_clock),
    );

    let pair = Symbol::new("BTC/USD").unwrap();
    let cl = ClOrdId::allocate_v4();
    let api_key = ApiKey::new("test-api-key-1234567890");

    let t0 = MonotonicInstant(Duration::from_secs(0));
    idx.insert(cl.clone(), pair.clone(), t0);

    tracker
        .consume(Scope::Pair(api_key, pair), 55.0, t0)
        .expect("pre-drain must succeed");

    let err = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.cancel(cl).via(Transport::Rest),
    )
    .await
    .expect("cancel did not resolve in time")
    .unwrap_err();

    assert!(
        matches!(&err, TradeError::RateLimited { .. }),
        "expected TradeError::RateLimited from pre-charge, got {err:?}"
    );
    {
        use crate::error::ApiError;
        let rid = err
            .request_id()
            .expect("pre-charge reject carries the dispatch id");
        assert!(
            uuid::Uuid::parse_str(rid).is_ok(),
            "correlation id is a UUID"
        );
    }
}

/// MISS: cancelling an UNKNOWN cl_ord_id (not indexed) skips the pre-charge, the
/// cancel succeeds, and the trading tracker shows zero consumption (the miss path
/// charges no scope).
#[tokio::test]
async fn cancel_rest_miss_skips_pre_charge_and_succeeds() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));

    let (trade, _idx, tracker) = make_namespace_with_index_refs(
        json!({ "error": [], "result": { "count": 1 } }),
        Arc::clone(&fixed_clock),
    );

    let unknown_cl = ClOrdId::allocate_v4();
    let api_key = ApiKey::new("test-api-key-1234567890");
    let pair = Symbol::new("BTC/USD").unwrap();
    let t0 = MonotonicInstant(Duration::from_secs(0));

    let headroom_before = tracker
        .headroom(Scope::Pair(api_key.clone(), pair.clone()), t0)
        .unwrap_or(60.0);

    let resp = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.cancel(unknown_cl).via(Transport::Rest),
    )
    .await
    .expect("cancel did not resolve in time")
    .expect("MISS cancel must succeed (no pre-charge)");

    assert_eq!(resp.count, 1, "cancel should return count=1");

    let headroom_after = tracker
        .headroom(Scope::Pair(api_key, pair), t0)
        .unwrap_or(60.0);

    assert!(
        (headroom_after - headroom_before).abs() < f64::EPSILON,
        "MISS cancel must leave trading tracker UNCHANGED \
         (before: {headroom_before}, after: {headroom_after})"
    );
}

/// A mock transport that returns a canned Kraken `error` array (no `result`).
/// Used to simulate a wire rejection (e.g. `EOrder:Rate limit exceeded`) so the
/// REST executor's `Err(RestError::Kraken(...))` branch fires.
struct KrakenErrorMock {
    /// The error codes returned in the `"error"` array.
    error_codes: Vec<&'static str>,
}

#[async_trait::async_trait]
impl HttpTransport for KrakenErrorMock {
    async fn get_json(
        &self,
        _path: &str,
        _query_params: &[(&str, &str)],
    ) -> Result<serde_json::Value, TransportError> {
        panic!("KrakenErrorMock: GET not used in these tests");
    }

    async fn post_form_signed(
        &self,
        _path: &str,
        _body: &str,
        _api_key_header: &str,
        _api_sign_header: &str,
    ) -> Result<serde_json::Value, TransportError> {
        let errors: Vec<serde_json::Value> = self
            .error_codes
            .iter()
            .map(|s| serde_json::Value::String(s.to_string()))
            .collect();
        Ok(serde_json::json!({ "error": errors }))
    }
}

/// Build a namespace exposing its index, trading tracker, and api tracker. The
/// transport is `KrakenErrorMock`, so the wire call returns a Kraken error
/// (the `Err(RestError::Kraken)` branch).
fn make_namespace_for_snap_tests(
    error_codes: Vec<&'static str>,
    clock: Arc<dyn crate::clock::Clock>,
) -> (
    TradeNamespace,
    Arc<crate::rate_limit::ClOrdIdPairIndex>,
    Arc<crate::rate_limit::SpotTradingRateLimitTracker>,
    Arc<crate::rate_limit::SpotApiRateLimitTracker>,
) {
    let mock = Arc::new(KrakenErrorMock { error_codes });
    let (auth, _bus, api_rl, trading_rl) = build_deps(&clock);
    let cl_ord_id_index = Arc::new(crate::rate_limit::ClOrdIdPairIndex::new(1024));
    let rest = Arc::new(RestSurface::new_with_index(
        Arc::clone(&mock) as Arc<dyn HttpTransport>,
        auth,
        Arc::clone(&api_rl),
        Arc::clone(&trading_rl),
        clock,
        Arc::clone(&cl_ord_id_index),
        test_no_retry_engine(),
        std::time::Duration::from_secs(30),
    ));
    (
        TradeNamespace::new(rest),
        cl_ord_id_index,
        trading_rl,
        api_rl,
    )
}

/// REST cancel whose cl_ord_id IS in the index → `EOrder:Rate limit exceeded`
/// → trading tracker snapped at `Scope::Pair`.
/// Confirms the snapped tracker reads headroom == 0 (counter == cap).
#[tokio::test]
async fn snap_20b_rest_eorder_rate_limit_snaps_trading_pair_scope_on_hit() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (trade, idx, trading, _api) =
        make_namespace_for_snap_tests(vec!["EOrder:Rate limit exceeded"], Arc::clone(&fixed_clock));

    let pair = Symbol::new("BTC/USD").unwrap();
    let cl = ClOrdId::allocate_v4();
    let api_key = ApiKey::new("test-api-key-1234567890");
    let t0 = MonotonicInstant(Duration::from_secs(0));

    idx.insert(cl.clone(), pair.clone(), t0);

    let headroom_before = trading
        .headroom(Scope::Pair(api_key.clone(), pair.clone()), t0)
        .unwrap_or(60.0);
    assert!(
        headroom_before > 0.0,
        "trading counter should start with headroom"
    );

    let _err = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.cancel(cl).via(Transport::Rest),
    )
    .await
    .expect("cancel timed out")
    .unwrap_err();

    let headroom_after = trading
        .headroom(Scope::Pair(api_key.clone(), pair.clone()), t0)
        .unwrap_or(60.0);
    assert!(
        headroom_after < f64::EPSILON,
        "trading tracker MUST be snapped to cap on EOrder:Rate limit exceeded \
         (headroom_after={headroom_after})"
    );
}

/// REST cancel with `EOrder:Domain rate limit exceeded` → trading tracker
/// snapped at `Scope::ApiKey`. Confirms all known pair counters are at cap.
#[tokio::test]
async fn snap_20b_rest_eorder_domain_rate_limit_snaps_trading_api_key_scope() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (trade, idx, trading, _api) = make_namespace_for_snap_tests(
        vec!["EOrder:Domain rate limit exceeded"],
        Arc::clone(&fixed_clock),
    );

    let pair = Symbol::new("BTC/USD").unwrap();
    let cl = ClOrdId::allocate_v4();
    let api_key = ApiKey::new("test-api-key-1234567890");
    let t0 = MonotonicInstant(Duration::from_secs(0));

    idx.insert(cl.clone(), pair.clone(), t0);
    trading
        .consume(Scope::Pair(api_key.clone(), pair.clone()), 1.0, t0)
        .expect("initial consume must succeed");

    let _err = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.cancel(cl).via(Transport::Rest),
    )
    .await
    .expect("cancel timed out")
    .unwrap_err();

    let headroom_after = trading
        .headroom(Scope::Pair(api_key.clone(), pair.clone()), t0)
        .unwrap_or(60.0);
    assert!(
        headroom_after < f64::EPSILON,
        "domain snap MUST cap ALL pair counters for the api_key \
         (headroom_after={headroom_after})"
    );
}

/// REST cancel with `EAPI:Rate limit exceeded` snaps the API tracker but MUST NOT
/// snap the trading tracker. The cl_ord_id is not indexed (MISS) so the trading
/// counter is untouched, isolating the snap from the pre-charge.
#[tokio::test]
async fn snap_20b_rest_eapi_rate_limit_snaps_api_tracker_not_trading() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (trade, _idx, trading, api) =
        make_namespace_for_snap_tests(vec!["EAPI:Rate limit exceeded"], Arc::clone(&fixed_clock));

    let unknown_cl = ClOrdId::allocate_v4();
    let api_key = ApiKey::new("test-api-key-1234567890");
    let pair = Symbol::new("BTC/USD").unwrap();
    let t0 = MonotonicInstant(Duration::from_secs(0));

    let trading_before = trading
        .headroom(Scope::Pair(api_key.clone(), pair.clone()), t0)
        .unwrap_or(60.0);

    let _err = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.cancel(unknown_cl).via(Transport::Rest),
    )
    .await
    .expect("cancel timed out")
    .unwrap_err();

    let api_headroom_after = api.headroom(Scope::ApiKey(api_key.clone()), t0);
    assert!(
        api_headroom_after.is_some_and(|h| h < f64::EPSILON),
        "API tracker MUST be snapped on EAPI:Rate limit exceeded \
         (api_headroom_after={api_headroom_after:?})"
    );
    let trading_after = trading
        .headroom(Scope::Pair(api_key.clone(), pair.clone()), t0)
        .unwrap_or(60.0);
    assert!(
        (trading_after - trading_before).abs() < f64::EPSILON,
        "EAPI snap MUST NOT touch the trading tracker \
         (before={trading_before}, after={trading_after})"
    );
}

/// A trade EAPI rejection through the full REST executor must emit exactly ONE
/// `RateLimitExceededEvent`: the snap lives only in the `signed_post_costed` chokepoint;
/// `snap_to_cap` is idempotent, so only the event count catches a double-fire.
#[tokio::test]
async fn snap_15_trade_eapi_emits_exactly_one_rate_limit_exceeded_event() {
    use crate::dispatch::EventType;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (trade_ns, bus, _idx, _trading, _api) = make_namespace_for_snap_tests_with_bus(
        vec!["EAPI:Rate limit exceeded"],
        Arc::clone(&fixed_clock),
    );
    bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
    let mut rx = capture(&bus, EventType::RateLimitExceededEvent);

    let _ = tokio::time::timeout(
        Duration::from_millis(500),
        trade_ns
            .order(buy_limit("BTC/USD", "0.001", "50000"))
            .via(Transport::Rest),
    )
    .await
    .expect("order_buy timed out")
    .unwrap_err();

    let _first = tokio::time::timeout(Duration::from_millis(300), rx.recv())
        .await
        .expect("#15: RateLimitExceededEvent not delivered within 300ms")
        .expect("event channel closed");
    let second = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(
        second.is_err(),
        "#15 double-fire: a trade EAPI rejection MUST emit EXACTLY ONE \
         RateLimitExceededEvent (chokepoint only); a second event means the \
         executor's SnapTarget::Api branch was re-added (double-snap). got={second:?}"
    );
}

/// REST cancel with index MISS (cl_ord_id unknown) + `EOrder:Rate limit exceeded`
/// → trading Pair snap SKIPPED (no valid pair); API tracker NOT touched.
#[tokio::test]
async fn snap_20b_rest_eorder_index_miss_skips_pair_snap() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (trade, _idx, trading, _api) =
        make_namespace_for_snap_tests(vec!["EOrder:Rate limit exceeded"], Arc::clone(&fixed_clock));

    let unknown_cl = ClOrdId::allocate_v4();
    let api_key = ApiKey::new("test-api-key-1234567890");
    let pair = Symbol::new("BTC/USD").unwrap();
    let t0 = MonotonicInstant(Duration::from_secs(0));

    let trading_before = trading
        .headroom(Scope::Pair(api_key.clone(), pair.clone()), t0)
        .unwrap_or(60.0);

    let _err = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.cancel(unknown_cl).via(Transport::Rest),
    )
    .await
    .expect("cancel timed out")
    .unwrap_err();

    let trading_after = trading
        .headroom(Scope::Pair(api_key.clone(), pair.clone()), t0)
        .unwrap_or(60.0);
    assert!(
        (trading_after - trading_before).abs() < f64::EPSILON,
        "index MISS MUST leave trading tracker UNCHANGED \
         (before={trading_before}, after={trading_after})"
    );
}

/// Build a snap-test namespace that also wires the bus to the RestSurface so
/// `snap_to_cap` event emissions reach subscribers. Returns the namespace, bus,
/// index, trading tracker, and api tracker.
fn make_namespace_for_snap_tests_with_bus(
    error_codes: Vec<&'static str>,
    clock: Arc<dyn crate::clock::Clock>,
) -> (
    TradeNamespace,
    Arc<crate::dispatch::DispatchEventBus>,
    Arc<crate::rate_limit::ClOrdIdPairIndex>,
    Arc<crate::rate_limit::SpotTradingRateLimitTracker>,
    Arc<crate::rate_limit::SpotApiRateLimitTracker>,
) {
    let mock = Arc::new(KrakenErrorMock { error_codes });
    let (auth, bus, api_rl, trading_rl) = build_deps(&clock);
    let cl_ord_id_index = Arc::new(crate::rate_limit::ClOrdIdPairIndex::new(1024));
    let rest = Arc::new(RestSurface::new_with_index(
        Arc::clone(&mock) as Arc<dyn HttpTransport>,
        auth,
        Arc::clone(&api_rl),
        Arc::clone(&trading_rl),
        Arc::clone(&clock),
        Arc::clone(&cl_ord_id_index),
        test_no_retry_engine(),
        std::time::Duration::from_secs(30),
    ));
    rest.set_bus(Arc::clone(&bus));
    (
        TradeNamespace::new(rest),
        bus,
        cl_ord_id_index,
        trading_rl,
        api_rl,
    )
}

/// A REST rejection with the real wire form `EService:Throttled: <ts>` (timestamp
/// suffix) MUST snap the API tracker to cap; an exact `==` match against
/// `"EService:Throttled"` would miss the suffixed form.
#[tokio::test]
async fn snap_20b_eservice_throttled_with_timestamp_snaps_api_tracker() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (_trade_ns, _bus, _idx, _trading, api) = make_namespace_for_snap_tests_with_bus(
        vec!["EService:Throttled: 1716287000"],
        Arc::clone(&fixed_clock),
    );

    let api_key = ApiKey::new("test-api-key-1234567890");
    let t0 = MonotonicInstant(Duration::from_secs(0));

    let before = api
        .headroom(Scope::ApiKey(api_key.clone()), t0)
        .unwrap_or(15.0);
    assert!(before > 0.0, "api tracker should start with headroom");

    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        _trade_ns
            .order(buy_limit("BTC/USD", "0.001", "50000"))
            .via(Transport::Rest),
    )
    .await
    .expect("order_buy timed out")
    .unwrap_err();

    let after = api
        .headroom(Scope::ApiKey(api_key.clone()), t0)
        .unwrap_or(15.0);
    assert!(
        after < f64::EPSILON,
        "EService:Throttled: <ts> MUST snap the api tracker to cap \
         end-to-end (headroom_after={after})"
    );
}

/// EVENT guard: `snap_to_cap` MUST emit `RateLimitExceededEvent` with the
/// correct `tracker`, `scope`, and `kraken_error` fields.
#[tokio::test]
async fn snap_20b_snap_to_cap_emits_rate_limit_exceeded_event() {
    use crate::dispatch::EventType;
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (_trade_ns, bus, _idx, _trading, api) = make_namespace_for_snap_tests_with_bus(
        vec!["EAPI:Rate limit exceeded"],
        Arc::clone(&fixed_clock),
    );
    bus.start_dispatch_reactor(&tokio::runtime::Handle::current());

    let mut rx = capture(&bus, EventType::RateLimitExceededEvent);

    let api_key = ApiKey::new("test-api-key-1234567890");
    let t0 = MonotonicInstant(Duration::from_secs(0));

    api.snap_to_cap(
        Scope::ApiKey(api_key.clone()),
        "EAPI:Rate limit exceeded",
        t0,
    );

    let payload = tokio::time::timeout(std::time::Duration::from_millis(300), rx.recv())
        .await
        .expect("RateLimitExceededEvent not delivered within 300ms")
        .expect("channel closed");

    match payload {
        EventPayload::RateLimitExceededEvent {
            tracker,
            key_id_fingerprint,
            pair,
            kraken_error,
            ..
        } => {
            assert_eq!(tracker, "api", "tracker field must be \"api\"");
            assert_eq!(pair, None, "api-tracker scope carries no pair");
            assert!(
                !key_id_fingerprint.is_empty(),
                "fingerprint must be non-empty"
            );
            assert_ne!(
                key_id_fingerprint,
                api_key.as_str(),
                "payload must NOT expose the raw API key"
            );
            assert_eq!(
                kraken_error, "EAPI:Rate limit exceeded",
                "kraken_error must carry the wire code"
            );
        }
        other => panic!("expected RateLimitExceededEvent, got {other:?}"),
    }
}

/// Error codes that are NOT rate-limit signals MUST NOT classify as a snap,
/// guarding against a broadened prefix (e.g. `starts_with("EAPI:")` wrongly firing
/// on `EAPI:Invalid key` / `EAPI:Invalid nonce`).
#[test]
fn snap_20b_non_ratelimit_codes_do_not_snap_either_tracker() {
    use super::classify_rate_limit_snap;

    let non_ratelimit_codes = [
        "EAPI:Invalid key",
        "EAPI:Invalid nonce",
        "EOrder:Insufficient funds",
        "EOrder:Invalid order",
        "EGeneral:Invalid arguments",
    ];

    for code in &non_ratelimit_codes {
        assert!(
            classify_rate_limit_snap(code).is_none(),
            "classify_rate_limit_snap({code:?}) MUST return None (no snap) \
             to guard against broad-prefix regression"
        );
    }

    assert!(
        classify_rate_limit_snap("EService:Throttled").is_some(),
        "EService:Throttled (bare) must still classify as Api snap"
    );
    assert!(
        classify_rate_limit_snap("EService:Throttled: 1716287000").is_some(),
        "EService:Throttled: <ts> must classify as Api snap (this was broken before Fix 1)"
    );
}

/// A batch of two accepted entries (both with txid) MUST populate the
/// ClOrdIdPairIndex for each; `lookup_and_promote` returns Some(_) for both after
/// the call. FixedClock@T=0 for determinism.
#[tokio::test]
async fn order_batch_rest_accept_populates_cl_ord_id_index_per_entry() {
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));

    let (trade, idx, _trading) = make_namespace_with_index_refs(
        json!({
            "error": [],
            "result": {
                "orders": [
                    {
                        "descr": { "order": "buy 0.1 BTC/USD @ limit 50000" },
                        "txid": "OBATCH1-AAAA-BBBBBB"
                    },
                    {
                        "descr": { "order": "sell 0.2 BTC/USD @ limit 60000" },
                        "txid": "OBATCH2-CCCC-DDDDDD"
                    }
                ]
            }
        }),
        Arc::clone(&fixed_clock),
    );

    let cl0 = ClOrdId::new("batch-a395-entry-0").unwrap();
    let cl1 = ClOrdId::new("batch-a395-entry-1").unwrap();

    let mut req = two_entry_batch();
    req.orders[0].cl_ord_id = Some(cl0.clone());
    req.orders[1].cl_ord_id = Some(cl1.clone());

    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(req).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .expect("order_batch should succeed");

    let entry0 = idx.lookup_and_promote(&cl0);
    let entry1 = idx.lookup_and_promote(&cl1);

    assert!(
        entry0.is_some(),
        "cl0 MUST be in the index after batch accept (got None)"
    );
    assert!(
        entry1.is_some(),
        "cl1 MUST be in the index after batch accept (got None)"
    );

    assert_eq!(
        entry0.unwrap().pair.as_str(),
        "BTC/USD",
        "index entry0 must record the batch pair"
    );
    assert_eq!(
        entry1.unwrap().pair.as_str(),
        "BTC/USD",
        "index entry1 must record the batch pair"
    );
}

/// validate=true entries carry no txid and MUST NOT be added to the
/// ClOrdIdPairIndex (enforcing the `line.txid.is_some()` gate). FixedClock@T=0.
#[tokio::test]
async fn order_batch_rest_validate_mode_does_not_populate_index() {
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));

    let (trade, idx, _trading) = make_namespace_with_index_refs(
        json!({
            "error": [],
            "result": {
                "orders": [
                    { "descr": { "order": "buy 0.1 BTC/USD @ limit 50000" } },
                    { "descr": { "order": "sell 0.2 BTC/USD @ limit 60000" } }
                ]
            }
        }),
        Arc::clone(&fixed_clock),
    );

    let cl0 = ClOrdId::new("a395-validate-0").unwrap();
    let cl1 = ClOrdId::new("a395-validate-1").unwrap();

    let mut req = two_entry_batch();
    req.validate = true;
    req.orders[0].cl_ord_id = Some(cl0.clone());
    req.orders[1].cl_ord_id = Some(cl1.clone());

    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(req).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .expect("validate-mode batch should succeed");

    assert!(
        idx.lookup_and_promote(&cl0).is_none(),
        "validate-mode entry MUST NOT be inserted into the index (cl0 found)"
    );
    assert!(
        idx.lookup_and_promote(&cl1).is_none(),
        "validate-mode entry MUST NOT be inserted into the index (cl1 found)"
    );
}

/// A top-level `EOrder:Rate limit exceeded` on `order_batch` via REST snaps the
/// trading tracker to cap on `Scope::Pair`; the API tracker MUST NOT snap (EOrder,
/// not EAPI). FixedClock@T=0 for determinism.
#[tokio::test]
async fn order_batch_rest_eorder_rate_limit_snaps_trading_pair_scope() {
    use crate::rate_limit::Scope;
    use crate::types::MonotonicInstant;
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));

    let (trade, _idx, trading, api) =
        make_namespace_for_snap_tests(vec!["EOrder:Rate limit exceeded"], Arc::clone(&fixed_clock));

    let api_key = ApiKey::new("test-api-key-1234567890");
    let pair = Symbol::new("BTC/USD").unwrap();
    let t0 = MonotonicInstant(Duration::from_secs(0));

    let trading_before = trading
        .headroom(Scope::Pair(api_key.clone(), pair.clone()), t0)
        .unwrap_or(60.0);
    assert!(
        trading_before > 0.0,
        "trading counter must start with headroom"
    );

    let api_before = api
        .headroom(Scope::ApiKey(api_key.clone()), t0)
        .unwrap_or(15.0);
    assert!(api_before > 0.0, "api tracker must start with headroom");

    let _err = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(two_entry_batch()).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .unwrap_err();

    let trading_after = trading
        .headroom(Scope::Pair(api_key.clone(), pair.clone()), t0)
        .unwrap_or(60.0);
    assert!(
        trading_after < f64::EPSILON,
        "trading tracker MUST be snapped to cap on EOrder:Rate limit exceeded \
         (headroom_after={trading_after})"
    );

    let api_after = api
        .headroom(Scope::ApiKey(api_key.clone()), t0)
        .unwrap_or(15.0);
    assert!(
        (api_after - api_before).abs() < f64::EPSILON,
        "EAPI snap MUST NOT fire on EOrder:Rate limit exceeded \
         (api_before={api_before}, api_after={api_after})"
    );
}

/// A mixed batch (entry 0 placed, entry 1 rejected per-line) returns `Ok` with
/// per-row results: the placed sibling keeps its txid and is indexed (cancellable
/// by cl_ord_id); the rejected entry is not. FixedClock@T=0.
#[tokio::test]
async fn order_batch_rest_mixed_partial_placement_surfaces_txid_and_indexes_placed() {
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));

    let (trade, idx, _trading) = make_namespace_with_index_refs(
        json!({
            "error": [],
            "result": {
                "orders": [
                    {
                        "descr": { "order": "buy 0.1 BTC/USD @ limit 50000" },
                        "txid": "OBATCH-PLACED-0001"
                    },
                    { "error": "EOrder:Insufficient funds" }
                ]
            }
        }),
        Arc::clone(&fixed_clock),
    );

    let cl0 = ClOrdId::new("a354-placed-0").unwrap();
    let cl1 = ClOrdId::new("a354-rejected-1").unwrap();

    let mut req = two_entry_batch();
    req.orders[0].cl_ord_id = Some(cl0.clone());
    req.orders[1].cl_ord_id = Some(cl1.clone());

    let resp = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(req).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .expect("a mixed partial-placement batch MUST return Ok with per-row results");

    assert_eq!(
        resp.orders[0].txid.as_ref().map(|t| t.as_str()),
        Some("OBATCH-PLACED-0001")
    );
    assert!(resp.orders[0].error.is_none());
    assert!(resp.orders[1].txid.is_none());
    assert_eq!(
        resp.orders[1].error.as_deref(),
        Some("EOrder:Insufficient funds")
    );

    assert!(
        idx.lookup_and_promote(&cl0).is_some(),
        "the placed entry of a partial batch MUST be indexed (cancellable by cl_ord_id)"
    );
    assert!(
        idx.lookup_and_promote(&cl1).is_none(),
        "the rejected entry MUST NOT be indexed"
    );
}

/// An ALL-rejected batch (every entry carries an `error`, none placed) MUST still
/// return `Ok` with each row's error surfaced — never a whole-batch Err — and
/// index nothing.
#[tokio::test]
async fn order_batch_rest_all_rejected_surfaces_each_error_and_indexes_nothing() {
    use std::time::Duration;

    let fixed_clock: Arc<dyn crate::clock::Clock> = Arc::new(FixedClock(Duration::from_secs(0)));
    let (trade, idx, _trading) = make_namespace_with_index_refs(
        json!({
            "error": [],
            "result": {
                "orders": [
                    { "error": "EOrder:Insufficient funds" },
                    { "error": "EGeneral:Invalid arguments:volume" }
                ]
            }
        }),
        Arc::clone(&fixed_clock),
    );

    let cl0 = ClOrdId::new("a354-rej-0").unwrap();
    let cl1 = ClOrdId::new("a354-rej-1").unwrap();
    let mut req = two_entry_batch();
    req.orders[0].cl_ord_id = Some(cl0.clone());
    req.orders[1].cl_ord_id = Some(cl1.clone());

    let resp = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        trade.order_batch(req).via(Transport::Rest),
    )
    .await
    .expect("order_batch timed out")
    .expect("an all-rejected batch MUST still return Ok with per-row errors");

    assert_eq!(resp.orders.len(), 2);
    assert_eq!(
        resp.orders[0].error.as_deref(),
        Some("EOrder:Insufficient funds")
    );
    assert_eq!(
        resp.orders[1].error.as_deref(),
        Some("EGeneral:Invalid arguments:volume")
    );
    assert!(resp.orders.iter().all(|o| o.txid.is_none()));
    assert!(idx.lookup_and_promote(&cl0).is_none());
    assert!(idx.lookup_and_promote(&cl1).is_none());
}

/// The 7 per-order params each render under the `orders[N]` bracket.
#[test]
fn batch_to_form_renders_all_seven_per_order_params() {
    let req = AddOrderBatchRequest {
        pair: Symbol::new("BTC/USD").unwrap(),
        orders: vec![BatchOrderEntry {
            leverage: Some(5),
            reduce_only: Some(true),
            stp_type: Some(StpType::CancelNewest),
            trigger: Some(TriggerKind::Index),
            display_vol: Some(dec("0.01")),
            start_time: Some(TimeSpec::from_unix_secs(1_783_681_271)),
            expire_time: Some(TimeSpec::from_unix_secs(1_783_767_671)),
            ..base_entry(
                Side::Buy,
                OrderType::Limit,
                dec("0.1"),
                Some(dec("50000").into()),
            )
        }],
        deadline: None,
        validate: false,
    };
    let form = req.to_form();
    let has = |k: &str, v: &str| form.iter().any(|(fk, fv)| fk == k && fv == v);
    assert!(has("orders[0][leverage]", "5"));
    assert!(has("orders[0][reduce_only]", "true"));
    assert!(has("orders[0][stptype]", StpType::CancelNewest.as_ref()));
    assert!(has("orders[0][trigger]", TriggerKind::Index.as_ref()));
    assert!(has("orders[0][displayvol]", "0.01"));
    assert!(has("orders[0][starttm]", "1783681271"));
    assert!(has("orders[0][expiretm]", "1783767671"));
}

/// TimeSpec renders the epoch integer on REST and RFC 3339 + `Z` on WS.
/// Vectors: the epoch, the unix "billennium", a leap day, and the live-probe instant.
#[test]
fn timespec_renders_both_transports() {
    let cases = [
        (0_i64, "1970-01-01T00:00:00Z"),
        (1_000_000_000, "2001-09-09T01:46:40Z"),
        (1_709_164_800, "2024-02-29T00:00:00Z"), // leap day
        (1_783_681_271, "2026-07-10T11:01:11Z"), // time-format probe vector
    ];
    for (secs, rfc) in cases {
        let t = TimeSpec::from_unix_secs(secs);
        assert_eq!(t.to_rest_form(), secs.to_string());
        assert_eq!(t.to_ws(), rfc);
    }
    let clamped = TimeSpec::from_unix_secs(-5);
    assert_eq!(clamped.to_rest_form(), "0");
    assert_eq!(clamped.to_ws(), "1970-01-01T00:00:00Z");
}

fn make_view_buy(
    pair: &str,
    vol: &str,
) -> (crate::types::Symbol, rust_decimal::Decimal, Option<ClOrdId>) {
    let sym = Symbol::new(pair).unwrap();
    let volume = dec(vol);
    let id = Some(ClOrdId::new("test-cl-ord-id-001").unwrap());
    (sym, volume, id)
}

#[test]
fn compose_validate_true_emits_validate_field() {
    // validate=true must appear in WS params or a dry-run places live.
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let price = Some(dec("50000").into());
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        cl_ord_id: &id,
        validate: true,
        ..base_add_view("buy", &sym, &vol, OrderType::Limit)
    });
    assert_eq!(
        params["validate"],
        serde_json::Value::Bool(true),
        "validate=true must be emitted on WS params (money-safety fix)"
    );
}

#[test]
fn compose_limit_order_emits_limit_price_no_triggers() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let price = Some(dec("50000").into());
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        cl_ord_id: &id,
        ..base_add_view("buy", &sym, &vol, OrderType::Limit)
    });
    assert!(
        params.get("limit_price").is_some(),
        "Limit order must have limit_price"
    );
    assert!(
        params.get("triggers").is_none(),
        "Limit order must NOT have triggers"
    );
    assert_eq!(
        params["limit_price_type"],
        serde_json::Value::String("static".to_string())
    );
}

#[test]
fn compose_stop_loss_limit_emits_triggers_and_limit_price() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.1");
    let trigger_price = Some(dec("45000").into());
    let limit_price = Some(dec("44900").into());
    let trigger = Some(TriggerKind::Last);
    let params = compose_add_order_params(AddOrderWsView {
        price: &trigger_price,
        price2: &limit_price,
        trigger: &trigger,
        cl_ord_id: &id,
        ..base_add_view("sell", &sym, &vol, OrderType::StopLossLimit)
    });
    let trig_obj = params
        .get("triggers")
        .expect("StopLossLimit must have triggers");
    assert_eq!(
        trig_obj["price"],
        serde_json::json!(45000),
        "triggers.price must equal request.price"
    );
    assert_eq!(
        trig_obj["reference"],
        serde_json::Value::String("last".to_string()),
        "triggers.reference must match TriggerKind::Last"
    );
    assert_eq!(
        trig_obj["price_type"],
        serde_json::Value::String("static".to_string())
    );
    assert_eq!(
        params["limit_price"],
        serde_json::json!(44900),
        "limit_price must equal request.price2"
    );
}

#[test]
fn compose_oflags_fan_out() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.01");
    let price = Some(dec("50000").into());

    for (flag, key, expected) in [
        (OFlag::Fcib, "fee_preference", json!("base")),
        (OFlag::Post, "post_only", json!(true)),
        (OFlag::Nompp, "no_mpp", json!(true)),
    ] {
        let oflags = [flag];
        let p = compose_add_order_params(AddOrderWsView {
            price: &price,
            oflags: &oflags,
            cl_ord_id: &id,
            ..base_add_view("buy", &sym, &vol, OrderType::Limit)
        });
        assert_eq!(p[key], expected, "oflag {flag:?}");
    }
}

#[test]
fn compose_userref_maps_to_order_userref_not_userref() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let userref = Some(7i32);
    let params = compose_add_order_params(AddOrderWsView {
        userref: &userref,
        cl_ord_id: &id,
        ..base_add_view("buy", &sym, &vol, OrderType::Market)
    });
    assert_eq!(
        params["order_userref"],
        serde_json::json!(7),
        "userref must be emitted as order_userref (WS key)"
    );
    assert!(
        params.get("userref").is_none(),
        "WS must NOT emit REST key 'userref'"
    );
}

#[test]
fn compose_stp_type_cancel_both_underscore() {
    // WS stp_type uses underscore; REST uses hyphen.
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let price = Some(dec("50000").into());
    let stp = Some(StpType::CancelBoth);
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        stp_type: &stp,
        cl_ord_id: &id,
        ..base_add_view("buy", &sym, &vol, OrderType::Limit)
    });
    assert_eq!(
        params["stp_type"],
        serde_json::Value::String("cancel_both".to_string()),
        "WS stp_type must use underscore spelling"
    );
}

#[test]
fn compose_conditional_close_stop_loss_limit_maps_trigger_and_limit() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.1");
    let price = Some(dec("50000").into());
    let close_tp = dec("44000");
    let close_lp = dec("43900");
    let close_ot = Some(OrderType::StopLossLimit);
    let close_p = Some(close_tp.into());
    let close_p2 = Some(close_lp.into());
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        cl_ord_id: &id,
        close_ordertype: &close_ot,
        close_price: &close_p,
        close_price2: &close_p2,
        ..base_add_view("buy", &sym, &vol, OrderType::Limit)
    });
    let cond = params
        .get("conditional")
        .expect("conditional must be present");
    assert_eq!(
        cond["trigger_price"],
        serde_json::json!(44000),
        "conditional.trigger_price must equal close_price"
    );
    assert_eq!(
        cond["limit_price"],
        serde_json::json!(43900),
        "conditional.limit_price must equal close_price2"
    );
    assert!(
        cond.get("cl_ord_id").is_none(),
        "conditional must NOT have cl_ord_id"
    );
}

#[test]
fn compose_deadline_is_not_emitted() {
    use crate::api::trade::DeadlineSpec;
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let price = Some(dec("50000").into());
    let _ = DeadlineSpec::after(std::time::Duration::from_secs(30));
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        cl_ord_id: &id,
        ..base_add_view("buy", &sym, &vol, OrderType::Limit)
    });
    assert!(
        params.get("deadline").is_none(),
        "deadline must NOT be emitted (no server-clock renderer yet)"
    );
}

#[test]
fn ws_unrepresentable_field_leverage_guarded() {
    assert_eq!(
        ws_unrepresentable_field(true, None, None),
        Some("leverage"),
        "a leverage ratio is WS-unrepresentable (WS has no ratio slot)"
    );
}

#[test]
fn ws_unrepresentable_field_no_leverage_is_none() {
    assert_eq!(
        ws_unrepresentable_field(false, None, None),
        None,
        "without a leverage ratio the order is WS-representable (incl. trailing-stop)"
    );
}

#[test]
fn ws_unrepresentable_field_absolute_close_price_is_representable() {
    let abs = Price::Absolute(dec("30000"));
    assert_eq!(
        ws_unrepresentable_field(false, Some(&abs), None),
        None,
        "an absolute close price is WS-representable"
    );
}

#[test]
fn ws_unrepresentable_field_relative_close_price_guarded() {
    let rel = Price::Offset {
        unit: PriceUnit::Quote,
        value: dec("150"),
    };
    assert_eq!(
        ws_unrepresentable_field(false, Some(&rel), None),
        Some("close[price]"),
        "a relative close price is WS-unrepresentable (no price_type slot)"
    );
    assert_eq!(
        ws_unrepresentable_field(false, None, Some(&rel)),
        Some("close[price2]"),
        "a relative close price2 is WS-unrepresentable (no price_type slot)"
    );
}

#[test]
fn compose_trailing_stop_uses_quote_price_type() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let price = Some(Price::Offset {
        unit: PriceUnit::Quote,
        value: dec("100"),
    });
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        cl_ord_id: &id,
        ..base_add_view("buy", &sym, &vol, OrderType::TrailingStop)
    });
    let trig = params
        .get("triggers")
        .expect("trailing-stop must emit triggers");
    assert_eq!(
        trig["price_type"], "quote",
        "trailing-stop triggers.price_type must be quote (relative), not static"
    );
    assert_eq!(trig["price"], serde_json::json!(100));
    assert!(
        params.get("limit_price").is_none(),
        "plain trailing-stop has no limit leg"
    );
}

#[test]
fn compose_trailing_stop_limit_quote_trigger_and_limit() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let price = Some(Price::Offset {
        unit: PriceUnit::Quote,
        value: dec("100"),
    });
    let price2 = Some(Price::Offset {
        unit: PriceUnit::Quote,
        value: dec("50"),
    });
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        price2: &price2,
        cl_ord_id: &id,
        ..base_add_view("buy", &sym, &vol, OrderType::TrailingStopLimit)
    });
    assert_eq!(params["triggers"]["price_type"], "quote");
    assert_eq!(params["limit_price"], serde_json::json!(50));
    assert_eq!(
        params["limit_price_type"], "quote",
        "trailing-stop-limit limit leg must be relative quote"
    );
}

#[test]
fn compose_stop_loss_still_uses_static_price_type() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let price = Some(dec("45000").into());
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        cl_ord_id: &id,
        ..base_add_view("sell", &sym, &vol, OrderType::StopLoss)
    });
    assert_eq!(
        params["triggers"]["price_type"], "static",
        "an ABSOLUTE (Price::Absolute) stop-loss trigger keeps static price_type"
    );
}

#[test]
fn compose_non_trailing_stop_loss_relative_offset_emits_quote_price_type() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let price = Some(Price::Offset {
        unit: PriceUnit::Quote,
        value: dec("150"),
    });
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        cl_ord_id: &id,
        ..base_add_view("sell", &sym, &vol, OrderType::StopLoss)
    });
    assert_eq!(
        params["triggers"]["price_type"], "quote",
        "a relative-offset stop-loss trigger emits quote price_type (Price is the single source of truth)"
    );
    assert_eq!(params["triggers"]["price"], serde_json::json!(150));
}

#[test]
fn compose_stop_loss_trigger_none_omits_reference() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let price = Some(dec("45000").into());
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        cl_ord_id: &id,
        ..base_add_view("sell", &sym, &vol, OrderType::StopLoss)
    });
    let trig = params
        .get("triggers")
        .expect("StopLoss must have triggers object");
    assert_eq!(
        trig["price"],
        serde_json::json!(45000),
        "triggers.price must equal request.price"
    );
    assert!(
        trig.get("reference").is_none(),
        "triggers.reference must be OMITTED when trigger=None (server defaults)"
    );
    assert!(
        params.get("limit_price").is_none(),
        "StopLoss must NOT have top-level limit_price"
    );
}

#[test]
fn compose_stop_loss_trigger_some_last_emits_reference() {
    let (sym, vol, id) = make_view_buy("BTC/USD", "0.001");
    let price = Some(dec("45000").into());
    let trigger = Some(TriggerKind::Last);
    let params = compose_add_order_params(AddOrderWsView {
        price: &price,
        trigger: &trigger,
        cl_ord_id: &id,
        ..base_add_view("sell", &sym, &vol, OrderType::StopLoss)
    });
    let trig = params
        .get("triggers")
        .expect("StopLoss must have triggers object");
    assert_eq!(
        trig["reference"],
        serde_json::Value::String("last".to_string()),
        "triggers.reference must be 'last' when TriggerKind::Last"
    );
}

#[test]
fn add_order_response_serializes_with_field_names() {
    let resp = AddOrderResponse {
        txid: Some(crate::types::TxId::new("OU22CG-KLAF2-FWUDD7")),
        cl_ord_id: None,
        descr: AddOrderDescr {
            order: Some("buy 1.25 XBTUSD @ limit 27500.0".to_string()),
            close: None,
        },
    };
    let v = serde_json::to_value(&resp).expect("serializes");
    assert_eq!(v["txid"], "OU22CG-KLAF2-FWUDD7");
    assert_eq!(v["descr"]["order"], "buy 1.25 XBTUSD @ limit 27500.0");
    assert!(!v.as_object().unwrap().contains_key("cl_ord_id"));
    assert!(!v["descr"].as_object().unwrap().contains_key("close"));
}

#[test]
fn batch_result_serializes_with_lowercase_tags() {
    let ok: BatchResult<CancelOrderResponse, OrderError> = BatchResult::Ok(CancelOrderResponse {
        count: 1,
        pending: false,
    });
    let v = serde_json::to_value(&ok).expect("serializes");
    assert_eq!(v["ok"]["count"], 1);

    let err: BatchResult<CancelOrderResponse, OrderError> =
        BatchResult::Err(OrderError::from_trade_error(
            crate::types::ClOrdId::allocate_v4(),
            TradeError::ClientClosed,
        ));
    let v = serde_json::to_value(&err).expect("serializes");
    assert_eq!(v["err"]["category"], "client");
    assert!(v["err"]["message"].is_string());
    assert!(!v["err"].as_object().unwrap().contains_key("request_id"));

    let rate_limited: BatchResult<CancelOrderResponse, OrderError> =
        BatchResult::Err(OrderError::from_trade_error(
            crate::types::ClOrdId::allocate_v4(),
            TradeError::RateLimited {
                retry_after_ts: None,
                request_id: None,
            },
        ));
    let v = serde_json::to_value(&rate_limited).expect("serializes");
    assert_eq!(v["err"]["category"], "rate_limit");
}

/// REST `timeinforce` is the lowercase token for every variant, incl. `fok`,
/// pinned on the `to_form` output (not just the enum's string form).
#[test]
fn to_form_pins_timeinforce_rest_tokens() {
    let cases = [
        (TimeInForce::Gtc, "gtc"),
        (TimeInForce::Ioc, "ioc"),
        (TimeInForce::Gtd, "gtd"),
        (TimeInForce::Fok, "fok"),
    ];
    for (tif, token) in cases {
        let mut req = OrderRequest::new(Symbol::new("BTC/USD").unwrap(), dec("1.0"), Side::Buy)
            .order_type(OrderType::Limit);
        req.time_in_force = Some(tif);
        let form = req.to_form();
        let has = |k: &str, v: &str| form.iter().any(|(fk, fv)| fk == k && fv == v);
        assert!(has("timeinforce", token), "timeinforce token for {tif:?}");
    }
}

/// REST `oflags` renders as a comma-joined CSV in slice order; a single flag
/// renders bare. Pins the multi-flag token set post/fcib/fciq/nompp.
#[test]
fn to_form_pins_oflags_csv_rendering() {
    let val = |form: &[(String, String)], k: &str| {
        form.iter().find(|(fk, _)| fk == k).map(|(_, v)| v.clone())
    };

    let mut req = OrderRequest::new(Symbol::new("BTC/USD").unwrap(), dec("1.0"), Side::Buy)
        .order_type(OrderType::Limit);
    req.oflags = vec![OFlag::Post, OFlag::Fcib, OFlag::Fciq, OFlag::Nompp];
    assert_eq!(
        val(&req.to_form(), "oflags").as_deref(),
        Some("post,fcib,fciq,nompp")
    );

    let mut single = OrderRequest::new(Symbol::new("BTC/USD").unwrap(), dec("1.0"), Side::Buy)
        .order_type(OrderType::Limit);
    single.oflags = vec![OFlag::Post];
    assert_eq!(val(&single.to_form(), "oflags").as_deref(), Some("post"));
}

/// `Price::to_rest_form` pins the relative-price grammar: absolute is bare,
/// quote offsets carry an explicit sign, percent offsets add a `%` suffix.
#[test]
fn price_to_rest_form_pins_relative_grammar() {
    assert_eq!(Price::Absolute(dec("30000")).to_rest_form(), "30000");
    assert_eq!(
        Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("150")
        }
        .to_rest_form(),
        "+150"
    );
    assert_eq!(
        Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("-150")
        }
        .to_rest_form(),
        "-150"
    );
    assert_eq!(
        Price::Offset {
            unit: PriceUnit::Percent,
            value: dec("1.5")
        }
        .to_rest_form(),
        "+1.5%"
    );
    assert_eq!(
        Price::Offset {
            unit: PriceUnit::Percent,
            value: dec("-2.0")
        }
        .to_rest_form(),
        "-2.0%"
    );
}
