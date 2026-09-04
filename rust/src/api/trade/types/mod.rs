//! Request/response data types + enums for the trade namespace.
//! Public enums, typed response structs, and private `Raw*` wire decoders.

mod enums;
mod requests;

#[cfg(test)]
pub(crate) use enums::oflags_to_wire;
pub(crate) use enums::stp_type_ws;
pub use enums::{AmendId, OFlag, OrderType, Side, StpType, TimeInForce, TriggerKind};
pub use requests::{
    AddOrderBatchRequest, BatchOrderEntry, CancelAllRequest, CancelRequest, CloseOrderType,
    ConditionalClose, DeadlineSpec, DeadmanRequest, OrderAmendRequest, OrderRequest, Price,
    PriceUnit, TimeSpec,
};

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

/// Per-line result of a batch operation. Each request line in a batch resolves
/// independently to either a success payload or an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum BatchResult<T, E> {
    /// Line succeeded; serialises externally tagged under `"ok"`.
    Ok(T),
    /// Line failed; serialises externally tagged under `"err"`.
    Err(E),
}

/// AddOrder response `descr` — Kraken's human-readable order description.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct AddOrderDescr {
    /// Kraken's human-readable order string. `Some` on REST; `None` on WS
    /// (`add_order` carries no descr — not fabricated).
    pub order: Option<String>,
    /// Human-readable conditional-close string when a `close[*]` clause was attached.
    /// `None` on WS.
    pub close: Option<String>,
}

/// Typed order-placement result, uniform across REST `AddOrder` and WS `add_order`.
/// Placement and reconciliation: docs/guides/placing-orders.md.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct AddOrderResponse {
    /// Kraken-assigned order id. `Some` for a real placement; `None` in
    /// validate mode (`validate=true`), where Kraken returns a descr-only
    /// result with no `txid`.
    pub txid: Option<crate::types::TxId>,
    /// Client order id for this placement. `None` when the SDK suppressed
    /// auto-allocation (request carried `userref` or a conditional-close clause).
    /// Reconciliation and suppression rules: docs/guides/placing-orders.md.
    pub cl_ord_id: Option<crate::types::ClOrdId>,
    /// Human-readable order description; per-transport presence on [`AddOrderDescr`].
    pub descr: AddOrderDescr,
}

/// Typed `CancelOrder` result, uniform across the REST and WS cancel paths
/// (the WS reply carries no counts; the SDK derives `count: 1, pending: false`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct CancelOrderResponse {
    /// Number of orders cancelled (wire `count`).
    pub count: u32,
    /// `true` when cancellation was accepted but is still settling; wire `pending`, absent → `false`.
    pub pending: bool,
}

/// Typed `CancelAll` result — cancels every open order on the account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct CancelAllResponse {
    /// Number of orders cancelled (wire `count`); `0` when none were open.
    pub count: u32,
}

/// Typed `AmendOrder` result, uniform across the REST and WS amend paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct AmendOrderResponse {
    /// Kraken-assigned amend reference. Opaque — see [`AmendId`].
    /// Amends preserve the order's `txid` (no `new_txid`).
    pub amend_id: AmendId,
}

/// Typed `CancelAllOrdersAfter` (dead-man's-switch) result.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct DeadmanResponse {
    /// ISO 8601 server time when the dead-man's-switch was armed
    /// (`currentTime` in the wire response).
    pub current_time: String,
    /// ISO 8601 server time when orders cancel if no further
    /// `cancel_all_orders_after` lands (`triggerTime`). `None` when disarmed
    /// (`timeout=0` sent; wire `triggerTime` is `"0"` or absent).
    pub trigger_time: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RawAddOrderResponse {
    pub(super) descr: RawAddOrderDescr,
    /// Absent in validate mode (`validate=true`) — Kraken returns descr-only
    /// with no `txid`. `#[serde(default)]` lets decode succeed; the typed
    /// `AddOrderResponse.txid` is then `None`.
    #[serde(default)]
    pub(super) txid: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RawAddOrderDescr {
    pub(super) order: String,
    /// Conditional-close human-readable string; absent unless a `close[*]`
    /// clause was attached to the request.
    pub(super) close: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RawCancelOrderResponse {
    pub(super) count: u32,
    #[serde(default)]
    pub(super) pending: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct RawCancelAllResponse {
    pub(super) count: u32,
}

/// Wire shape of `/0/private/AmendOrder`. The live reply carries `amend_id`
/// only — amends are atomic and preserve the order's `txid`, so there is no
/// `new_txid`/`original_txid` to decode.
#[derive(Debug, Deserialize)]
pub(super) struct RawAmendOrderResponse {
    pub(super) amend_id: String,
}

#[derive(Debug, Deserialize)]
#[allow(non_snake_case)]
pub(super) struct RawDeadmanResponse {
    pub(super) currentTime: String,
    /// `"0"` (literal string) encodes disarmed; defaulted to empty when absent
    /// so the Raw→typed map can normalise both to `None`.
    #[serde(default)]
    pub(super) triggerTime: String,
}

/// Per-line `AddOrderBatch` result (wire-native shape; NOT `BatchResult`).
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct BatchOrderResult {
    /// Human-readable order description for this line; same shape as single `AddOrder`.
    pub descr: AddOrderDescr,
    /// The placed order's txid. A SCALAR — unlike single `/AddOrder` whose
    /// top-level `txid` is an array. Absent under `validate=true` (descr-only).
    pub txid: Option<crate::types::TxId>,
    /// Per-line engine rejection message; `None` on success.
    pub error: Option<String>,
}

/// `POST /0/private/AddOrderBatch` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct AddOrderBatchResponse {
    /// Per-line results (wire `orders`), positionally 1:1 with the request's entries.
    pub orders: Vec<BatchOrderResult>,
}

/// `POST /0/private/CancelBatch` request. No `asset_class` field.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CancelBatchRequest {
    /// Client order ids to cancel; each resolves independently, 1:1 with `results`.
    pub cl_ord_ids: Vec<crate::types::ClOrdId>,
}

impl CancelBatchRequest {
    /// Construct from the client order ids to cancel.
    pub fn new(cl_ord_ids: Vec<crate::types::ClOrdId>) -> Self {
        Self { cl_ord_ids }
    }
}

/// `POST /0/private/CancelBatch` response.
/// `results` is 1:1 with the input `cl_ord_ids` order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct CancelBatchResponse {
    /// Per-line outcome; one line's failure never fails the other lines.
    pub results: Vec<BatchResult<CancelOrderResponse, super::error::OrderError>>,
}

#[cfg(test)]
mod types_tests {
    use super::*;

    #[test]
    fn order_type_exact_wire_strings() {
        assert_eq!(OrderType::Market.to_string(), "market");
        assert_eq!(OrderType::Limit.to_string(), "limit");
        assert_eq!(OrderType::Iceberg.to_string(), "iceberg");
        assert_eq!(OrderType::StopLoss.to_string(), "stop-loss");
        assert_eq!(OrderType::TakeProfit.to_string(), "take-profit");
        assert_eq!(OrderType::StopLossLimit.to_string(), "stop-loss-limit");
        assert_eq!(OrderType::TakeProfitLimit.to_string(), "take-profit-limit");
        assert_eq!(OrderType::TrailingStop.to_string(), "trailing-stop");
        assert_eq!(
            OrderType::TrailingStopLimit.to_string(),
            "trailing-stop-limit"
        );
        assert_eq!(OrderType::SettlePosition.to_string(), "settle-position");
    }

    #[test]
    fn oflag_exact_wire_strings() {
        assert_eq!(OFlag::Post.to_string(), "post");
        assert_eq!(OFlag::Fcib.to_string(), "fcib");
        assert_eq!(OFlag::Fciq.to_string(), "fciq");
        assert_eq!(OFlag::Nompp.to_string(), "nompp");
    }

    #[test]
    fn oflags_to_wire_set_semantics() {
        assert_eq!(oflags_to_wire(&[]), None);
        assert_eq!(oflags_to_wire(&[OFlag::Post]), Some("post".to_string()));
        assert_eq!(
            oflags_to_wire(&[OFlag::Post, OFlag::Fcib]),
            Some("post,fcib".to_string())
        );
    }

    #[test]
    fn stp_type_wire_strings() {
        assert_eq!(StpType::CancelNewest.to_string(), "cancel-newest");
        assert_eq!(StpType::CancelOldest.to_string(), "cancel-oldest");
        assert_eq!(StpType::CancelBoth.to_string(), "cancel-both");
    }

    #[test]
    fn trigger_kind_roundtrips_and_wire_strings() {
        let all = [TriggerKind::Last, TriggerKind::Index];
        for v in all {
            assert_eq!(TriggerKind::from_wire_str(v.as_ref()), Some(v));
        }
        assert_eq!(TriggerKind::from_wire_str("bogus"), None);
        assert_eq!(TriggerKind::Last.to_string(), "last");
        assert_eq!(TriggerKind::Index.to_string(), "index");
    }

    #[test]
    fn time_in_force_wire_strings() {
        assert_eq!(TimeInForce::Gtc.to_string(), "gtc");
        assert_eq!(TimeInForce::Ioc.to_string(), "ioc");
        assert_eq!(TimeInForce::Gtd.to_string(), "gtd");
        assert_eq!(TimeInForce::Fok.to_string(), "fok");
    }

    #[test]
    fn side_wire_strings() {
        assert_eq!(Side::Buy.to_string(), "buy");
        assert_eq!(Side::Sell.to_string(), "sell");
    }

    /// WS v2 `stp_type` uses the UNDERSCORE spelling — distinct from the REST
    /// kebab-case `Display` form.
    #[test]
    fn stp_type_ws_spellings() {
        assert_eq!(stp_type_ws(StpType::CancelNewest), "cancel_newest");
        assert_eq!(stp_type_ws(StpType::CancelOldest), "cancel_oldest");
        assert_eq!(stp_type_ws(StpType::CancelBoth), "cancel_both");
    }

    /// The serde-backed enums must render exactly what serde emits — Display
    /// and the wire form share one strum table; the `*_wire_strings` tests
    /// above pin the literals.
    #[test]
    fn strum_display_matches_serde_emission() {
        for ot in [
            OrderType::Market,
            OrderType::Limit,
            OrderType::Iceberg,
            OrderType::StopLoss,
            OrderType::TakeProfit,
            OrderType::StopLossLimit,
            OrderType::TakeProfitLimit,
            OrderType::TrailingStop,
            OrderType::TrailingStopLimit,
            OrderType::SettlePosition,
        ] {
            assert_eq!(serde_json::json!(ot), serde_json::json!(ot.to_string()));
        }
        for tif in [
            TimeInForce::Gtc,
            TimeInForce::Ioc,
            TimeInForce::Gtd,
            TimeInForce::Fok,
        ] {
            assert_eq!(serde_json::json!(tif), serde_json::json!(tif.to_string()));
        }
        for tk in [TriggerKind::Last, TriggerKind::Index] {
            assert_eq!(serde_json::json!(tk), serde_json::json!(tk.to_string()));
        }
    }

    #[test]
    fn amend_id_roundtrips_inner_string() {
        let id = AmendId::from("x".to_string());
        assert_eq!(id.as_str(), "x");
        assert_eq!(id, AmendId::from("x".to_string()));
    }

    #[test]
    fn batch_result_variants_construct() {
        let ok: BatchResult<u32, String> = BatchResult::Ok(7);
        let err: BatchResult<u32, String> = BatchResult::Err("nope".into());
        assert_eq!(ok, BatchResult::Ok(7));
        assert_eq!(err, BatchResult::Err("nope".to_string()));
    }

    use crate::api::trade::error::TradeError;
    use crate::types::{ClOrdId, Symbol};

    fn sym() -> Symbol {
        Symbol::new("BTC/USD").unwrap()
    }
    fn dec(s: &str) -> rust_decimal::Decimal {
        s.parse::<rust_decimal::Decimal>().unwrap()
    }
    fn cl() -> ClOrdId {
        ClOrdId::allocate_v4()
    }
    fn has(form: &[(String, String)], k: &str) -> bool {
        form.iter().any(|(key, _)| key == k)
    }
    fn val<'a>(form: &'a [(String, String)], k: &str) -> Option<&'a str> {
        form.iter()
            .find(|(key, _)| key == k)
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn validate_conflicting_userref_and_cl_ord_id_errors() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        req.userref = Some(42);
        req.cl_ord_id = Some(cl());
        assert_eq!(req.validate(), Err(TradeError::ConflictingOrderIdentifiers));
    }

    #[test]
    fn validate_settle_position_without_leverage_errors() {
        let req =
            OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::SettlePosition);
        assert_eq!(
            req.validate(),
            Err(TradeError::SettlePositionRequiresLeverage)
        );
        let mut req0 =
            OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::SettlePosition);
        req0.leverage = Some(0);
        assert_eq!(
            req0.validate(),
            Err(TradeError::SettlePositionRequiresLeverage)
        );
    }

    #[test]
    fn validate_settle_position_with_leverage_ok() {
        let mut req =
            OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::SettlePosition);
        req.leverage = Some(2);
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn validate_normal_limit_buy_ok() {
        let mut req = OrderRequest::new(sym(), dec("0.1"), Side::Buy).order_type(OrderType::Limit);
        req.price = Some(dec("50000").into());
        req.cl_ord_id = Some(cl());
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn validate_sell_symmetric_with_buy() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        req.userref = Some(1);
        req.cl_ord_id = Some(cl());
        assert_eq!(req.validate(), Err(TradeError::ConflictingOrderIdentifiers));
    }

    #[test]
    fn validate_reduce_only_without_leverage_errors() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.reduce_only = Some(true);
        assert_eq!(req.validate(), Err(TradeError::ReduceOnlyRequiresLeverage));
    }

    #[test]
    fn validate_reduce_only_with_leverage_one_errors() {
        // leverage == 1 is "no margin"; reduce_only needs > 1.
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.reduce_only = Some(true);
        req.leverage = Some(1);
        assert_eq!(req.validate(), Err(TradeError::ReduceOnlyRequiresLeverage));
    }

    #[test]
    fn validate_reduce_only_with_leverage_gt_one_ok() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.reduce_only = Some(true);
        req.leverage = Some(2);
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn validate_reduce_only_with_margin_ok() {
        // The WS-native `margin` toggle satisfies the reduce_only gate without a
        // leverage ratio — this is the WS-placeable margin+reduce_only combo.
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.reduce_only = Some(true);
        req.margin = true;
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn validate_rejects_margin_with_leverage() {
        // margin (WS-native) ⊕ leverage (REST ratio): both set is un-placeable on
        // either transport, so validate() rejects it client-side.
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.price = Some(dec("50000").into());
        req.margin = true;
        req.leverage = Some(3);
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_rejects_margin_with_relative_close() {
        // margin (WS-only) + a relative-offset conditional-close price (REST-only):
        // un-placeable on either transport → rejected client-side.
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.price = Some(dec("50000").into());
        req.margin = true;
        req.conditional_close = Some(ConditionalClose::new(
            CloseOrderType::Limit,
            Price::Offset {
                unit: PriceUnit::Quote,
                value: dec("100"),
            },
        ));
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_reduce_only_false_or_absent_needs_no_leverage() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.reduce_only = Some(false);
        assert_eq!(req.validate(), Ok(()));
        let req_none = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        assert_eq!(req_none.validate(), Ok(()));
    }

    #[test]
    fn validate_reduce_only_gate_is_symmetric_on_sell() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.reduce_only = Some(true);
        assert_eq!(req.validate(), Err(TradeError::ReduceOnlyRequiresLeverage));
        req.leverage = Some(3);
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn validate_fok_on_market_rejected() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        req.time_in_force = Some(TimeInForce::Fok);
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_fok_on_limit_accepted() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.price = Some(dec("50000").into());
        req.time_in_force = Some(TimeInForce::Fok);
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn validate_fok_on_stop_loss_rejected() {
        // Plain stop-loss carries no limit price → FOK is invalid there.
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::StopLoss);
        req.price = Some(dec("50000").into());
        req.time_in_force = Some(TimeInForce::Fok);
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_amend_empty_errors_one_field_ok() {
        let req = OrderAmendRequest::new(cl());
        assert_eq!(req.validate(), Err(TradeError::EmptyAmendRequest));
        let mut r2 = OrderAmendRequest::new(cl());
        r2.order_volume = Some(dec("0.5"));
        assert_eq!(r2.validate(), Ok(()));
        let mut r3 = OrderAmendRequest::new(cl());
        r3.post_only = Some(false); // even Some(false) counts as "set"
        assert_eq!(r3.validate(), Ok(()));
    }

    #[test]
    fn to_form_minimal_market_buy_includes_side() {
        let req = OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::Market);
        let form = req.to_form();
        assert_eq!(form.len(), 4, "got: {:?}", form);
        assert_eq!(val(&form, "pair"), Some("BTC/USD"));
        assert_eq!(val(&form, "volume"), Some("0.001"));
        assert_eq!(val(&form, "ordertype"), Some("market"));
        assert_eq!(val(&form, "type"), Some("buy"));
        for k in [
            "price",
            "price2",
            "leverage",
            "oflags",
            "reduce_only",
            "stptype",
            "trigger",
            "timeinforce",
            "displayvol",
            "starttm",
            "expiretm",
            "userref",
            "cl_ord_id",
            "close[ordertype]",
            "close[price]",
            "close[price2]",
            "validate",
            "deadline",
        ] {
            assert!(!has(&form, k), "unexpected key present: {}", k);
        }
    }

    #[test]
    fn to_form_fully_populated_limit_order() {
        let mut req = OrderRequest::new(sym(), dec("0.5"), Side::Buy).order_type(OrderType::Limit);
        req.price = Some(dec("50000").into());
        req.price2 = Some(dec("49000").into());
        req.leverage = Some(3);
        req.oflags = vec![OFlag::Post, OFlag::Fcib];
        req.reduce_only = Some(true);
        req.stp_type = Some(StpType::CancelNewest);
        req.trigger = Some(TriggerKind::Index);
        req.time_in_force = Some(TimeInForce::Gtc);
        req.display_vol = Some(dec("0.1"));
        req.start_time = Some(TimeSpec::from_unix_secs(0));
        req.expire_time = Some(TimeSpec::from_unix_secs(1_783_767_671));
        req.userref = Some(-7);
        req.conditional_close = Some(
            ConditionalClose::new(CloseOrderType::StopLoss, dec("48000")).price2(dec("47000")),
        );
        req.validate = true;
        let form = req.to_form();

        assert_eq!(val(&form, "pair"), Some("BTC/USD"));
        assert_eq!(val(&form, "volume"), Some("0.5"));
        assert_eq!(val(&form, "ordertype"), Some("limit"));
        assert_eq!(val(&form, "price"), Some("50000"));
        assert_eq!(val(&form, "price2"), Some("49000"));
        assert_eq!(val(&form, "leverage"), Some("3"));
        assert_eq!(val(&form, "oflags"), Some("post,fcib"));
        assert_eq!(val(&form, "reduce_only"), Some("true"));
        assert_eq!(val(&form, "stptype"), Some("cancel-newest"));
        assert_eq!(val(&form, "trigger"), Some("index"));
        assert_eq!(val(&form, "timeinforce"), Some("gtc"));
        assert_eq!(val(&form, "displayvol"), Some("0.1"));
        assert_eq!(val(&form, "starttm"), Some("0"));
        assert_eq!(val(&form, "expiretm"), Some("1783767671"));
        assert_eq!(val(&form, "userref"), Some("-7"));
        assert_eq!(val(&form, "close[ordertype]"), Some("stop-loss"));
        assert_eq!(val(&form, "close[price]"), Some("48000"));
        assert_eq!(val(&form, "close[price2]"), Some("47000"));
        assert_eq!(val(&form, "validate"), Some("true"));
        assert!(!has(&form, "cl_ord_id"));
        assert!(!has(&form, "deadline"));
    }

    #[test]
    fn to_form_empty_oflags_omits_key() {
        let req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        assert!(!has(&req.to_form(), "oflags"));
    }

    #[test]
    fn to_form_validate_false_omits_key() {
        let req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        assert!(!has(&req.to_form(), "validate"));
    }

    #[test]
    fn to_form_reduce_only_some_false_is_absent() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        req.reduce_only = Some(false);
        assert!(!has(&req.to_form(), "reduce_only"));
        let none_req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        assert!(!has(&none_req.to_form(), "reduce_only"));
    }

    #[test]
    fn to_form_cl_ord_id_emitted_when_set() {
        let id = cl();
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        req.cl_ord_id = Some(id.clone());
        assert_eq!(val(&req.to_form(), "cl_ord_id"), Some(id.as_str()));
    }

    #[test]
    fn to_form_deadline_skipped_when_set() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        req.deadline = Some(DeadlineSpec::after(std::time::Duration::from_secs(30)));
        assert!(!has(&req.to_form(), "deadline"));
    }

    #[test]
    fn validate_rejects_set_deadline() {
        // A set deadline is rejected loudly (server-clock render deferred) rather
        // than silently dropped — transport-neutral (validate() runs on both).
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        req.deadline = Some(DeadlineSpec::after(std::time::Duration::from_secs(30)));
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
        // absent deadline validates fine.
        let ok = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
        assert_eq!(ok.validate(), Ok(()));
    }

    #[test]
    fn batch_validate_rejects_set_deadline() {
        let mut req = AddOrderBatchRequest {
            pair: sym(),
            orders: vec![plain_batch_entry(), plain_batch_entry()],
            deadline: Some(DeadlineSpec::after(std::time::Duration::from_secs(30))),
            validate: false,
        };
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
        req.deadline = None;
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn to_form_buy_and_sell_emit_identical_shape() {
        let mut b = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        b.price = Some(dec("100").into());
        let mut s = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        s.price = Some(dec("100").into());
        assert_eq!(b.to_form(), s.to_form());
    }

    #[test]
    fn to_form_amend_emits_set_mutable_fields_only() {
        let id = cl();
        let mut req = OrderAmendRequest::new(id.clone());
        req.order_volume = Some(dec("0.75"));
        req.limit_price = Some(dec("51000").into());
        req.post_only = Some(true);
        let form = req.to_form();
        assert_eq!(val(&form, "cl_ord_id"), Some(id.as_str()));
        assert_eq!(val(&form, "order_qty"), Some("0.75"));
        assert_eq!(val(&form, "limit_price"), Some("51000"));
        assert_eq!(val(&form, "post_only"), Some("true"));
        assert!(!has(&form, "trigger_price"));
    }

    #[test]
    fn to_form_amend_renders_relative_prices() {
        let mut req = OrderAmendRequest::new(cl());
        req.limit_price = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("150"),
        });
        req.trigger_price = Some(Price::Offset {
            unit: PriceUnit::Percent,
            value: dec("1.0"),
        });
        let form = req.to_form();
        assert_eq!(val(&form, "limit_price"), Some("+150"));
        assert_eq!(val(&form, "trigger_price"), Some("+1.0%"));
    }

    #[test]
    fn to_form_amend_post_only_false_emitted() {
        let mut req = OrderAmendRequest::new(cl());
        req.post_only = Some(false);
        assert_eq!(val(&req.to_form(), "post_only"), Some("false"));
    }

    #[test]
    fn to_form_cancel_request_single_key() {
        let id = cl();
        let req = CancelRequest::new(id.clone());
        let form = req.to_form();
        assert_eq!(form.len(), 1);
        assert_eq!(val(&form, "cl_ord_id"), Some(id.as_str()));
    }

    #[test]
    fn to_form_cancel_all_is_empty() {
        assert!(CancelAllRequest.to_form().is_empty());
    }

    #[test]
    fn to_form_deadman_emits_timeout() {
        let arm = DeadmanRequest::new(60);
        assert_eq!(val(&arm.to_form(), "timeout"), Some("60"));
        let disarm = DeadmanRequest::new(0);
        assert_eq!(val(&disarm.to_form(), "timeout"), Some("0"));
    }

    #[test]
    fn deadline_spec_after_constructs() {
        let d = DeadlineSpec::after(std::time::Duration::from_secs(30));
        assert_eq!(d, DeadlineSpec::after(std::time::Duration::from_secs(30)));
    }

    #[test]
    fn price_to_rest_form_absolute_is_plain() {
        assert_eq!(Price::Absolute(dec("30000")).to_rest_form(), "30000");
    }

    #[test]
    fn price_to_rest_form_quote_offset_carries_explicit_sign() {
        // Kraken requires an explicit `+` on a relative price; no `%` for quote offsets.
        assert_eq!(
            Price::Offset {
                unit: PriceUnit::Quote,
                value: dec("150"),
            }
            .to_rest_form(),
            "+150"
        );
        assert_eq!(
            Price::Offset {
                unit: PriceUnit::Quote,
                value: dec("-150"),
            }
            .to_rest_form(),
            "-150"
        );
    }

    #[test]
    fn price_to_rest_form_percent_offset_has_percent_suffix() {
        assert_eq!(
            Price::Offset {
                unit: PriceUnit::Percent,
                value: dec("1.0"),
            }
            .to_rest_form(),
            "+1.0%"
        );
        assert_eq!(
            Price::Offset {
                unit: PriceUnit::Percent,
                value: dec("-2.0"),
            }
            .to_rest_form(),
            "-2.0%"
        );
    }

    #[test]
    fn price_to_ws_maps_variant_to_price_type_and_preserves_sign() {
        assert_eq!(
            Price::Absolute(dec("30000")).to_ws(),
            (dec("30000"), "static")
        );
        assert_eq!(
            Price::Offset {
                unit: PriceUnit::Quote,
                value: dec("150"),
            }
            .to_ws(),
            (dec("150"), "quote")
        );
        assert_eq!(
            Price::Offset {
                unit: PriceUnit::Percent,
                value: dec("-2.0"),
            }
            .to_ws(),
            (dec("-2.0"), "pct")
        );
    }

    #[test]
    fn to_form_trailing_stop_emits_relative_price() {
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::TrailingStop);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("100"),
        });
        let form = req.to_form();
        assert_eq!(val(&form, "ordertype"), Some("trailing-stop"));
        assert_eq!(val(&form, "price"), Some("+100"));
    }

    #[test]
    fn to_form_trailing_stop_limit_emits_relative_price_and_price2() {
        let mut req = OrderRequest::new(sym(), dec("0.001"), Side::Buy)
            .order_type(OrderType::TrailingStopLimit);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("100"),
        });
        // price2 (limit leg) allows `-`; direction is not automatic.
        req.price2 = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("-50"),
        });
        let form = req.to_form();
        assert_eq!(val(&form, "ordertype"), Some("trailing-stop-limit"));
        assert_eq!(val(&form, "price"), Some("+100"));
        assert_eq!(val(&form, "price2"), Some("-50"));
    }

    #[test]
    fn validate_rejects_absolute_price_on_trailing_stop() {
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::TrailingStop);
        req.price = Some(Price::Absolute(dec("100")));
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_rejects_absolute_price_on_trailing_stop_limit() {
        let mut req = OrderRequest::new(sym(), dec("0.001"), Side::Buy)
            .order_type(OrderType::TrailingStopLimit);
        req.price = Some(Price::Absolute(dec("100")));
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_rejects_negative_quote_offset_on_trailing_price() {
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::TrailingStop);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("-100"),
        });
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_rejects_negative_percent_offset_on_trailing_price() {
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::TrailingStop);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Percent,
            value: dec("-1.0"),
        });
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_rejects_zero_offset_on_trailing_price() {
        // A zero trail is degenerate; the offset must be strictly positive
        // (direction is automatic from the side, so sign carries no info).
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::TrailingStop);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("0"),
        });
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_accepts_positive_offset_on_trailing_price() {
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::TrailingStop);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("100"),
        });
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn validate_allows_both_signs_on_trailing_price2_limit_leg() {
        // trailing-stop-limit price2 is not sign-guarded.
        for value in [dec("50"), dec("-50")] {
            let mut req = OrderRequest::new(sym(), dec("0.001"), Side::Buy)
                .order_type(OrderType::TrailingStopLimit);
            req.price = Some(Price::Offset {
                unit: PriceUnit::Quote,
                value: dec("100"),
            });
            req.price2 = Some(Price::Offset {
                unit: PriceUnit::Quote,
                value,
            });
            assert_eq!(req.validate(), Ok(()), "price2 = {value} must be accepted");
        }
    }

    #[test]
    fn validate_rejects_none_price_on_trailing_stop() {
        // A trailing order REQUIRES a price offset — a missing trigger leg is
        // rejected client-side (Kraken has nothing to trail).
        let req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::TrailingStop);
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_rejects_absolute_price2_on_trailing_stop_limit() {
        // The limit leg (price2) allows either sign but must be RELATIVE — an
        // absolute price2 is rejected.
        let mut req = OrderRequest::new(sym(), dec("0.001"), Side::Buy)
            .order_type(OrderType::TrailingStopLimit);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("100"),
        });
        req.price2 = Some(Price::Absolute(dec("50")));
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_rejects_none_price2_on_trailing_stop_limit() {
        // A trailing-stop-limit REQUIRES its limit leg (price2) — a valid trigger
        // leg alone is not enough; Kraken rejects a missing limit leg server-side.
        let mut req = OrderRequest::new(sym(), dec("0.001"), Side::Buy)
            .order_type(OrderType::TrailingStopLimit);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("100"),
        });
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_accepts_negative_offset_price2_on_trailing_stop_limit() {
        let mut req = OrderRequest::new(sym(), dec("0.001"), Side::Buy)
            .order_type(OrderType::TrailingStopLimit);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("100"),
        });
        req.price2 = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("-50"),
        });
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn validate_rejects_absolute_close_price_on_trailing_close_leg() {
        // A single-order conditional close whose close order type is trailing is
        // guarded on the CLOSE leg too (not only the primary leg).
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::Limit);
        req.price = Some(Price::Absolute(dec("50000")));
        req.conditional_close = Some(ConditionalClose::new(
            CloseOrderType::TrailingStop,
            Price::Absolute(dec("100")),
        ));
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn validate_accepts_take_profit_limit_close_type() {
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::Limit);
        req.price = Some(Price::Absolute(dec("50000")));
        req.conditional_close = Some(
            ConditionalClose::new(
                CloseOrderType::TakeProfitLimit,
                Price::Absolute(dec("55000")),
            )
            .price2(Price::Absolute(dec("55100"))),
        );
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn to_form_renders_relative_close_price_as_signed_bare_string() {
        // REST CAN carry a relative close price (general REST grammar). It
        // renders bare + signed — distinct from WS, which rejects it (no
        // price_type slot on the WS conditional object).
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::Limit);
        req.price = Some(Price::Absolute(dec("50000")));
        req.conditional_close = Some(ConditionalClose::new(
            CloseOrderType::Limit,
            Price::Offset {
                unit: PriceUnit::Quote,
                value: dec("150"),
            },
        ));
        let form = req.to_form();
        assert_eq!(val(&form, "close[price]"), Some("+150"));
    }

    fn trailing_batch_entry(price: Price) -> BatchOrderEntry {
        BatchOrderEntry::new(OrderType::TrailingStop, Side::Buy, dec("0.001")).price(price)
    }

    fn plain_batch_entry() -> BatchOrderEntry {
        let mut e = trailing_batch_entry(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("100"),
        });
        e.ordertype = OrderType::Limit;
        e.price = Some(Price::Absolute(dec("49000")));
        e
    }

    #[test]
    fn batch_validate_rejects_mixed_margin_and_leverage_entries() {
        let mut lev = plain_batch_entry();
        lev.leverage = Some(3);
        let mut marg = plain_batch_entry();
        marg.margin = true;
        let req = AddOrderBatchRequest {
            pair: sym(),
            orders: vec![lev, marg],
            deadline: None,
            validate: false,
        };
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn batch_validate_rejects_margin_entry_with_relative_close_entry() {
        let mut marg = plain_batch_entry();
        marg.margin = true;
        let mut rel_close = plain_batch_entry();
        rel_close.conditional_close = Some(ConditionalClose {
            ordertype: CloseOrderType::Limit,
            price: Price::Offset {
                unit: PriceUnit::Quote,
                value: dec("100"),
            },
            price2: None,
        });
        let req = AddOrderBatchRequest {
            pair: sym(),
            orders: vec![marg, rel_close],
            deadline: None,
            validate: false,
        };
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn batch_validate_rejects_absolute_price_on_trailing_entry() {
        let req = AddOrderBatchRequest {
            pair: sym(),
            orders: vec![
                plain_batch_entry(),
                trailing_batch_entry(Price::Absolute(dec("100"))),
            ],
            deadline: None,
            validate: false,
        };
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn batch_validate_rejects_negative_offset_on_trailing_entry() {
        let req = AddOrderBatchRequest {
            pair: sym(),
            orders: vec![
                plain_batch_entry(),
                trailing_batch_entry(Price::Offset {
                    unit: PriceUnit::Quote,
                    value: dec("-100"),
                }),
            ],
            deadline: None,
            validate: false,
        };
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn batch_validate_accepts_positive_offset_on_trailing_entry() {
        let req = AddOrderBatchRequest {
            pair: sym(),
            orders: vec![
                plain_batch_entry(),
                trailing_batch_entry(Price::Offset {
                    unit: PriceUnit::Quote,
                    value: dec("100"),
                }),
            ],
            deadline: None,
            validate: false,
        };
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn close_order_type_wire_strings() {
        assert_eq!(CloseOrderType::Limit.to_string(), "limit");
        assert_eq!(CloseOrderType::StopLoss.to_string(), "stop-loss");
        assert_eq!(CloseOrderType::TakeProfit.to_string(), "take-profit");
        assert_eq!(CloseOrderType::StopLossLimit.to_string(), "stop-loss-limit");
        assert_eq!(
            CloseOrderType::TakeProfitLimit.to_string(),
            "take-profit-limit"
        );
        assert_eq!(CloseOrderType::TrailingStop.to_string(), "trailing-stop");
        assert_eq!(
            CloseOrderType::TrailingStopLimit.to_string(),
            "trailing-stop-limit"
        );
    }

    #[test]
    fn batch_validate_rejects_absolute_trailing_close() {
        let mut e = plain_batch_entry();
        e.conditional_close = Some(ConditionalClose {
            ordertype: CloseOrderType::TrailingStop,
            price: Price::Absolute(dec("100")),
            price2: None,
        });
        let req = AddOrderBatchRequest {
            pair: sym(),
            orders: vec![plain_batch_entry(), e],
            deadline: None,
            validate: false,
        };
        assert!(matches!(
            req.validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn batch_validate_accepts_relative_trailing_close() {
        let mut e = plain_batch_entry();
        e.conditional_close = Some(ConditionalClose {
            ordertype: CloseOrderType::TrailingStop,
            price: Price::Offset {
                unit: PriceUnit::Quote,
                value: dec("100"),
            },
            price2: None,
        });
        let req = AddOrderBatchRequest {
            pair: sym(),
            orders: vec![plain_batch_entry(), e],
            deadline: None,
            validate: false,
        };
        assert_eq!(req.validate(), Ok(()));
    }

    #[test]
    fn batch_validate_enforces_margin_and_tif_gates_per_entry() {
        let batch = |e: BatchOrderEntry| AddOrderBatchRequest {
            pair: sym(),
            orders: vec![plain_batch_entry(), e],
            deadline: None,
            validate: false,
        };

        let mut e = plain_batch_entry();
        e.ordertype = OrderType::SettlePosition;
        assert!(matches!(
            batch(e).validate(),
            Err(TradeError::SettlePositionRequiresLeverage)
        ));

        let mut e = plain_batch_entry();
        e.reduce_only = Some(true);
        assert!(matches!(
            batch(e).validate(),
            Err(TradeError::ReduceOnlyRequiresLeverage)
        ));

        let mut e = plain_batch_entry();
        e.ordertype = OrderType::Market;
        e.price = None;
        e.time_in_force = Some(TimeInForce::Fok);
        assert!(matches!(
            batch(e).validate(),
            Err(TradeError::InvalidOrder { .. })
        ));

        let mut e = plain_batch_entry();
        e.reduce_only = Some(true);
        e.leverage = Some(2);
        assert_eq!(batch(e).validate(), Ok(()));

        let mut e = plain_batch_entry();
        e.reduce_only = Some(true);
        e.margin = true;
        assert_eq!(batch(e).validate(), Ok(()));

        let mut e = plain_batch_entry();
        e.margin = true;
        e.leverage = Some(3);
        assert!(matches!(
            batch(e).validate(),
            Err(TradeError::InvalidOrder { .. })
        ));
    }

    #[test]
    fn to_form_conformance_trailing_stop_price_token() {
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::TrailingStop);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Quote,
            value: dec("150"),
        });
        let form = req.to_form();
        assert_eq!(val(&form, "ordertype"), Some("trailing-stop"));
        assert_eq!(val(&form, "price"), Some("+150"));
    }

    #[test]
    fn to_form_conformance_trailing_stop_percent_price_token() {
        let mut req =
            OrderRequest::new(sym(), dec("0.001"), Side::Buy).order_type(OrderType::TrailingStop);
        req.price = Some(Price::Offset {
            unit: PriceUnit::Percent,
            value: dec("1.5"),
        });
        let form = req.to_form();
        assert_eq!(val(&form, "ordertype"), Some("trailing-stop"));
        assert_eq!(val(&form, "price"), Some("+1.5%"));
    }

    #[test]
    fn to_form_conformance_stptype_tokens() {
        for (stp, wire) in [
            (StpType::CancelNewest, "cancel-newest"),
            (StpType::CancelOldest, "cancel-oldest"),
            (StpType::CancelBoth, "cancel-both"),
        ] {
            let mut req =
                OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Market);
            req.stp_type = Some(stp);
            assert_eq!(val(&req.to_form(), "stptype"), Some(wire));
        }
    }

    #[test]
    fn to_form_conformance_timeinforce_tokens() {
        for (tif, wire) in [
            (TimeInForce::Gtc, "gtc"),
            (TimeInForce::Ioc, "ioc"),
            (TimeInForce::Gtd, "gtd"),
            (TimeInForce::Fok, "fok"),
        ] {
            let mut req =
                OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
            req.price = Some(dec("50000").into());
            req.time_in_force = Some(tif);
            assert_eq!(val(&req.to_form(), "timeinforce"), Some(wire));
        }
    }

    #[test]
    fn to_form_conformance_trigger_tokens() {
        for (trig, wire) in [(TriggerKind::Last, "last"), (TriggerKind::Index, "index")] {
            let mut req =
                OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::StopLoss);
            req.price = Some(dec("50000").into());
            req.trigger = Some(trig);
            assert_eq!(val(&req.to_form(), "trigger"), Some(wire));
        }
    }

    #[test]
    fn to_form_conformance_oflags_tokens() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.price = Some(dec("50000").into());
        req.oflags = vec![OFlag::Post, OFlag::Fcib, OFlag::Fciq, OFlag::Nompp];
        assert_eq!(val(&req.to_form(), "oflags"), Some("post,fcib,fciq,nompp"));
    }

    #[test]
    fn post_only_convenience_matches_manual_post_oflag() {
        let base = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        let via_setter = base.clone().post_only(true);
        let mut via_field = base;
        via_field.oflags = vec![OFlag::Post];
        assert_eq!(via_setter.oflags, via_field.oflags);
        assert_eq!(val(&via_setter.to_form(), "oflags"), Some("post"));
    }

    #[test]
    fn post_only_false_removes_post_and_is_idempotent() {
        let req = OrderRequest::new(sym(), dec("1"), Side::Buy)
            .order_type(OrderType::Limit)
            .post_only(true)
            .post_only(true)
            .post_only(false);
        assert!(!req.oflags.contains(&OFlag::Post));
        assert!(!has(&req.to_form(), "oflags"));
        let twice = OrderRequest::new(sym(), dec("1"), Side::Buy)
            .order_type(OrderType::Limit)
            .post_only(true)
            .post_only(true);
        assert_eq!(twice.oflags, vec![OFlag::Post]);
    }

    #[test]
    fn post_only_false_preserves_other_oflags() {
        let mut req = OrderRequest::new(sym(), dec("1"), Side::Buy).order_type(OrderType::Limit);
        req.oflags = vec![OFlag::Fcib, OFlag::Post];
        let req = req.post_only(false);
        assert_eq!(req.oflags, vec![OFlag::Fcib]);
    }
}
