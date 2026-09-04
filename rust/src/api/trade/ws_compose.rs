//! WS composition/decoding layer for the trade namespace: `compose_*_params`
//! request builders, `decode_ws_*` reply parsers, and WS error classification.

use crate::conn::managed_connection::WsResponse;
use crate::dispatch::OrderSubmitStatus;
use crate::error::ApiError;
use crate::rest::RestError;
use crate::types::ClOrdId;

use super::error::{TradeError, classify_kraken_error_string};
use super::types::{
    AddOrderDescr, AddOrderResponse, AmendId, AmendOrderResponse, CancelAllResponse,
    CancelOrderResponse, DeadmanResponse, OFlag, OrderAmendRequest, OrderType, Price, StpType,
    TimeInForce, TimeSpec, TriggerKind, stp_type_ws,
};

/// Map a definitive `RestError` (wire accept-then-reject or malformed envelope)
/// to a `WireError` code via `TradeError`. The payload `code` is the
/// `ApiError::code()` string, not an enum.
pub(crate) fn wire_error_status(err: RestError) -> OrderSubmitStatus {
    let code = TradeError::from(err).code().to_string();
    OrderSubmitStatus::WireError { code }
}

/// Classify a definitive `WS` failure (`resp.success == false`) into a
/// `WireError` code via the same `classify_ws_error` → `code()` path REST uses.
pub(crate) fn ws_wire_error_status(error: Option<String>) -> OrderSubmitStatus {
    let code = classify_ws_error(error).code().to_string();
    OrderSubmitStatus::WireError { code }
}

/// Convert a `Decimal` to a JSON number (not string) for the WS wire, parsing
/// the exact decimal string so no f64 precision is lost.
pub(crate) fn decimal_to_json(d: &rust_decimal::Decimal) -> serde_json::Value {
    let s = d.to_string();
    match s.parse::<serde_json::Number>() {
        Ok(n) => serde_json::Value::Number(n),
        Err(_) => serde_json::Value::String(s),
    }
}

/// Returns `Some(field)` for an order the WS `add_order` path cannot faithfully
/// represent (leverage; a relative conditional-close price) → caller must use
/// `.via(Transport::Rest)`. See docs/guides/placing-orders.md.
pub(crate) fn ws_unrepresentable_field(
    has_leverage: bool,
    close_price: Option<&Price>,
    close_price2: Option<&Price>,
) -> Option<&'static str> {
    if has_leverage {
        return Some("leverage");
    }
    if matches!(close_price, Some(Price::Offset { .. })) {
        return Some("close[price]");
    }
    if matches!(close_price2, Some(Price::Offset { .. })) {
        return Some("close[price2]");
    }
    None
}

/// Returns `Some(field)` for an order the REST `add_order` path cannot faithfully
/// represent (`margin`, the WS-native max-leverage toggle) → caller must use
/// `.via(Transport::WsV2Auth)`. See docs/guides/placing-orders.md.
pub(crate) fn rest_unrepresentable_field(has_margin: bool) -> Option<&'static str> {
    has_margin.then_some("margin")
}

pub(crate) struct AddOrderWsView<'a> {
    pub(crate) side: &'a str,
    pub(crate) pair: &'a crate::types::Symbol,
    pub(crate) volume: &'a rust_decimal::Decimal,
    pub(crate) order_type: OrderType,
    pub(crate) price: &'a Option<Price>,
    pub(crate) price2: &'a Option<Price>,
    pub(crate) oflags: &'a [OFlag],
    /// WS-native margin toggle → `margin:true`. REST-rejected upstream.
    pub(crate) margin: bool,
    pub(crate) reduce_only: &'a Option<bool>,
    pub(crate) stp_type: &'a Option<StpType>,
    pub(crate) trigger: &'a Option<TriggerKind>,
    pub(crate) time_in_force: &'a Option<TimeInForce>,
    pub(crate) display_vol: &'a Option<rust_decimal::Decimal>,
    pub(crate) start_time: &'a Option<TimeSpec>,
    pub(crate) expire_time: &'a Option<TimeSpec>,
    pub(crate) userref: &'a Option<i32>,
    /// `None` when auto-allocation was suppressed (userref / conditional-close);
    /// Kraken rejects `cl_ord_id` together with `userref`.
    pub(crate) cl_ord_id: &'a Option<ClOrdId>,
    pub(crate) close_ordertype: &'a Option<OrderType>,
    pub(crate) close_price: &'a Option<Price>,
    pub(crate) close_price2: &'a Option<Price>,
    pub(crate) validate: bool,
}

/// Compose the token-free `add_order` params from an `AddOrderWsView`.
/// The reactor injects the token after this returns. `deadline` is skipped
/// (naive now() causes clock-skew rejects).
pub(crate) fn compose_add_order_params(view: AddOrderWsView<'_>) -> serde_json::Value {
    let mut p = serde_json::Map::new();

    p.insert(
        "order_type".to_string(),
        serde_json::Value::String(view.order_type.to_string()),
    );
    p.insert(
        "side".to_string(),
        serde_json::Value::String(view.side.to_string()),
    );
    p.insert("order_qty".to_string(), decimal_to_json(view.volume));
    p.insert(
        "symbol".to_string(),
        serde_json::Value::String(view.pair.as_str().to_string()),
    );
    if let Some(c) = view.cl_ord_id {
        p.insert(
            "cl_ord_id".to_string(),
            serde_json::Value::String(c.as_str().to_string()),
        );
    }

    // Price fields by order type; exhaustive so a new OrderType is a compile error.
    enum WsPriceShape {
        NoPrice,
        Limit,
        Trigger { limit_leg: bool },
    }
    let shape = match view.order_type {
        OrderType::Market | OrderType::SettlePosition => WsPriceShape::NoPrice,
        OrderType::Limit | OrderType::Iceberg => WsPriceShape::Limit,
        OrderType::StopLoss | OrderType::TakeProfit => WsPriceShape::Trigger { limit_leg: false },
        OrderType::StopLossLimit | OrderType::TakeProfitLimit => {
            WsPriceShape::Trigger { limit_leg: true }
        }
        OrderType::TrailingStop => WsPriceShape::Trigger { limit_leg: false },
        OrderType::TrailingStopLimit => WsPriceShape::Trigger { limit_leg: true },
        OrderType::Unknown => WsPriceShape::NoPrice,
    };

    match shape {
        WsPriceShape::NoPrice => {}
        WsPriceShape::Limit => {
            if let Some(lp) = view.price {
                let (value, price_type) = lp.to_ws();
                p.insert("limit_price".to_string(), decimal_to_json(&value));
                p.insert(
                    "limit_price_type".to_string(),
                    serde_json::Value::String(price_type.to_string()),
                );
            }
        }
        WsPriceShape::Trigger { limit_leg } => {
            let mut trig = serde_json::Map::new();
            if let Some(tk) = view.trigger {
                trig.insert(
                    "reference".to_string(),
                    serde_json::Value::String(tk.to_string()),
                );
            }
            if let Some(tp) = view.price {
                let (value, price_type) = tp.to_ws();
                trig.insert("price".to_string(), decimal_to_json(&value));
                trig.insert(
                    "price_type".to_string(),
                    serde_json::Value::String(price_type.to_string()),
                );
            }
            p.insert("triggers".to_string(), serde_json::Value::Object(trig));

            if limit_leg {
                if let Some(p2) = view.price2 {
                    let (value, price_type) = p2.to_ws();
                    p.insert("limit_price".to_string(), decimal_to_json(&value));
                    p.insert(
                        "limit_price_type".to_string(),
                        serde_json::Value::String(price_type.to_string()),
                    );
                }
            }
        }
    }

    for flag in view.oflags {
        match flag {
            OFlag::Post => {
                p.insert("post_only".to_string(), serde_json::Value::Bool(true));
            }
            OFlag::Fcib => {
                p.insert(
                    "fee_preference".to_string(),
                    serde_json::Value::String("base".to_string()),
                );
            }
            OFlag::Fciq => {
                if !p.contains_key("fee_preference") {
                    p.insert(
                        "fee_preference".to_string(),
                        serde_json::Value::String("quote".to_string()),
                    );
                }
            }
            OFlag::Nompp => {
                p.insert("no_mpp".to_string(), serde_json::Value::Bool(true));
            }
        }
    }

    if view.margin {
        p.insert("margin".to_string(), serde_json::Value::Bool(true));
    }
    if *view.reduce_only == Some(true) {
        p.insert("reduce_only".to_string(), serde_json::Value::Bool(true));
    }

    // stp_type: WS uses underscore spelling, not the REST Display (hyphen).
    if let Some(s) = view.stp_type {
        p.insert(
            "stp_type".to_string(),
            serde_json::Value::String(stp_type_ws(*s).to_string()),
        );
    }

    if let Some(tif) = view.time_in_force {
        p.insert(
            "time_in_force".to_string(),
            serde_json::Value::String(tif.to_string()),
        );
    }

    if let Some(d) = view.display_vol {
        p.insert("display_qty".to_string(), decimal_to_json(d));
    }

    if let Some(s) = view.start_time {
        p.insert(
            "effective_time".to_string(),
            serde_json::Value::String(s.to_ws()),
        );
    }

    if let Some(e) = view.expire_time {
        p.insert(
            "expire_time".to_string(),
            serde_json::Value::String(e.to_ws()),
        );
    }

    // deadline skipped — naive now() causes clock-skew rejects.

    if let Some(u) = view.userref {
        p.insert(
            "order_userref".to_string(),
            serde_json::Value::Number(serde_json::Number::from(*u)),
        );
    }

    // WS close prices are absolute-only.
    if let Some(ct) = view.close_ordertype {
        let mut cond = serde_json::Map::new();
        cond.insert(
            "order_type".to_string(),
            serde_json::Value::String(ct.to_string()),
        );
        match *ct {
            OrderType::StopLoss
            | OrderType::TakeProfit
            | OrderType::StopLossLimit
            | OrderType::TakeProfitLimit => {
                if let Some(tp) = view.close_price {
                    cond.insert("trigger_price".to_string(), decimal_to_json(&tp.to_ws().0));
                }
                if let Some(lp) = view.close_price2 {
                    cond.insert("limit_price".to_string(), decimal_to_json(&lp.to_ws().0));
                }
            }
            OrderType::Limit => {
                if let Some(lp) = view.close_price {
                    cond.insert("limit_price".to_string(), decimal_to_json(&lp.to_ws().0));
                }
            }
            OrderType::Market
            | OrderType::Iceberg
            | OrderType::SettlePosition
            | OrderType::TrailingStop
            | OrderType::TrailingStopLimit
            | OrderType::Unknown => {}
        }
        p.insert("conditional".to_string(), serde_json::Value::Object(cond));
    }

    // Emit validate only when true — omitting it would place a live order.
    if view.validate {
        p.insert("validate".to_string(), serde_json::Value::Bool(true));
    }

    serde_json::Value::Object(p)
}

/// Compose the token-free `batch_add` params. Each entry reuses
/// [`compose_add_order_params`] with `symbol` hoisted batch-level and `validate`
/// set batch-wide; an entry WS can't represent is rejected for REST fallback.
pub(crate) fn compose_batch_add_params(
    req: &super::types::AddOrderBatchRequest,
) -> Result<serde_json::Value, super::TradeError> {
    let mut orders = Vec::with_capacity(req.orders.len());
    for e in &req.orders {
        let (close_ordertype, close_price, close_price2) =
            super::types::ConditionalClose::flat_legs(&e.conditional_close);
        if let Some(field) = ws_unrepresentable_field(
            e.leverage.is_some(),
            close_price.as_ref(),
            close_price2.as_ref(),
        ) {
            return Err(super::TradeError::WsUnsupportedOrderField { field });
        }
        let oflags: &[OFlag] = e.oflags.as_deref().unwrap_or(&[]);
        let mut obj = compose_add_order_params(AddOrderWsView {
            side: <&str>::from(e.side),
            pair: &req.pair,
            volume: &e.volume,
            order_type: e.ordertype,
            price: &e.price,
            price2: &e.price2,
            oflags,
            margin: e.margin,
            reduce_only: &e.reduce_only,
            stp_type: &e.stp_type,
            trigger: &e.trigger,
            time_in_force: &e.time_in_force,
            display_vol: &e.display_vol,
            start_time: &e.start_time,
            expire_time: &e.expire_time,
            userref: &e.userref,
            cl_ord_id: &e.cl_ord_id,
            close_ordertype: &close_ordertype,
            close_price: &close_price,
            close_price2: &close_price2,
            validate: false, // batch-level (below), not per-order
        });
        if let serde_json::Value::Object(ref mut m) = obj {
            m.remove("symbol");
        }
        orders.push(obj);
    }
    let mut params = serde_json::Map::new();
    params.insert(
        "symbol".to_string(),
        serde_json::Value::String(req.pair.as_str().to_string()),
    );
    params.insert("orders".to_string(), serde_json::Value::Array(orders));
    if req.validate {
        params.insert("validate".to_string(), serde_json::Value::Bool(true));
    }
    Ok(serde_json::Value::Object(params))
}

/// Compose the token-free `amend_order` params. `cl_ord_id` is a scalar
/// (cancel uses an array).
pub(crate) fn compose_amend_order_params(req: &OrderAmendRequest) -> serde_json::Value {
    let mut p = serde_json::Map::new();
    p.insert(
        "cl_ord_id".to_string(),
        serde_json::Value::String(req.cl_ord_id.as_str().to_string()),
    );
    if let Some(v) = &req.order_volume {
        p.insert("order_qty".to_string(), decimal_to_json(v));
    }
    if let Some(price) = &req.limit_price {
        let (value, price_type) = price.to_ws();
        p.insert("limit_price".to_string(), decimal_to_json(&value));
        p.insert(
            "limit_price_type".to_string(),
            serde_json::Value::String(price_type.to_string()),
        );
    }
    if let Some(po) = req.post_only {
        p.insert("post_only".to_string(), serde_json::Value::Bool(po));
    }
    if let Some(tp) = &req.trigger_price {
        // Amend uses flat trigger keys (not the add_order `triggers` object).
        let (value, price_type) = tp.to_ws();
        p.insert("trigger_price".to_string(), decimal_to_json(&value));
        p.insert(
            "trigger_price_type".to_string(),
            serde_json::Value::String(price_type.to_string()),
        );
    }
    serde_json::Value::Object(p)
}

/// Compose the token-free `cancel_order` params. `cl_ord_id` is an array on the wire.
pub(crate) fn compose_cancel_order_params(req: &super::types::CancelRequest) -> serde_json::Value {
    serde_json::json!({
        "cl_ord_id": [req.cl_ord_id.as_str()],
    })
}

/// Decode a WS `batch_add` reply. A per-row array is `Ok` even when top-level
/// `success` is false (placed siblings stay live); null `result` is `Err`.
pub(crate) fn decode_ws_batch_add(
    resp: WsResponse,
) -> Result<super::types::AddOrderBatchResponse, TradeError> {
    if let serde_json::Value::Array(rows) = &resp.result {
        let orders = rows
            .iter()
            .map(|row| {
                let txid = row
                    .get("order_id")
                    .and_then(serde_json::Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(|s| crate::types::TxId::new(s.to_string()));
                let error = row
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .map(|s| s.to_string());
                super::types::BatchOrderResult {
                    descr: AddOrderDescr {
                        order: None,
                        close: None,
                    },
                    txid,
                    error,
                }
            })
            .collect();
        return Ok(super::types::AddOrderBatchResponse { orders });
    }
    Err(classify_ws_error(resp.error))
}

/// Decode a WS `add_order` reply. Order id is `order_id` (not `txid`);
/// `descr` is `None` (WS omits it).
pub(crate) fn decode_ws_add_order(
    resp: WsResponse,
    fallback_cl_ord_id: Option<ClOrdId>,
) -> Result<AddOrderResponse, TradeError> {
    if !resp.success {
        return Err(classify_ws_error(resp.error));
    }
    // Validate-mode replies carry empty `order_id` — treat as absent.
    let txid = resp
        .result
        .get("order_id")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|s| crate::types::TxId::new(s.to_string()));
    let cl_ord_id = resp
        .result
        .get("cl_ord_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| ClOrdId::new(s).ok())
        .or(fallback_cl_ord_id);
    Ok(AddOrderResponse {
        txid,
        cl_ord_id,
        descr: AddOrderDescr {
            order: None,
            close: None,
        },
    })
}

/// Decode a WS `amend_order` reply into the shared [`AmendOrderResponse`].
/// `result.amend_id` → `AmendId`. Never unwraps.
pub(crate) fn decode_ws_amend_order(resp: WsResponse) -> Result<AmendOrderResponse, TradeError> {
    if !resp.success {
        return Err(classify_ws_error(resp.error));
    }
    let amend_id = resp
        .result
        .get("amend_id")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| TradeError::malformed("ws amend response missing amend_id".to_string()))?;
    Ok(AmendOrderResponse {
        amend_id: AmendId::from(amend_id.to_string()),
    })
}

/// Decode a WS `cancel_order` reply into the shared [`CancelOrderResponse`].
/// The reply carries only `cl_ord_id`; absent → `MalformedResponse`. Derives
/// `count:1, pending:false`. See docs/guides/placing-orders.md. Never unwraps.
pub(crate) fn decode_ws_cancel_order(resp: WsResponse) -> Result<CancelOrderResponse, TradeError> {
    if !resp.success {
        return Err(classify_ws_error(resp.error));
    }
    let _cl_ord_id = resp
        .result
        .get("cl_ord_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            TradeError::malformed("ws cancel_order success missing cl_ord_id".to_string())
        })?;
    Ok(CancelOrderResponse {
        count: 1,
        pending: false,
    })
}

/// Decode a WS `cancel_all` reply. Missing/non-integer `count` → `MalformedResponse`
/// (never fabricate `0`).
pub(crate) fn decode_ws_cancel_all(resp: WsResponse) -> Result<CancelAllResponse, TradeError> {
    if !resp.success {
        return Err(classify_ws_error(resp.error));
    }
    let count = resp
        .result
        .get("count")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            TradeError::malformed("cancel_all WS reply: missing or non-integer `count`".to_string())
        })? as u32;
    Ok(CancelAllResponse { count })
}

/// Decode a WS `cancel_all_orders_after` reply into the shared
/// [`DeadmanResponse`]. Maps `currentTime`/`triggerTime`; `"0"`/absent
/// triggerTime → disarmed → `None`.
pub(crate) fn decode_ws_deadman(resp: WsResponse) -> Result<DeadmanResponse, TradeError> {
    if !resp.success {
        return Err(classify_ws_error(resp.error));
    }
    let current_time = resp
        .result
        .get("currentTime")
        .or_else(|| resp.result.get("current_time"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let trigger_raw = resp
        .result
        .get("triggerTime")
        .or_else(|| resp.result.get("trigger_time"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let trigger_time = match trigger_raw {
        "" | "0" => None,
        s => Some(s.to_string()),
    };
    Ok(DeadmanResponse {
        current_time,
        trigger_time,
    })
}

/// Classify a WS failure `error` string into a [`TradeError`]. WS carries a
/// single top-level `error` string (not an array); reuses the REST substring
/// families. Unmapped or absent strings degrade to `Unknown`.
pub(crate) fn classify_ws_error(error: Option<String>) -> TradeError {
    let msg = error.unwrap_or_else(|| "WS request rejected".to_string());
    classify_kraken_error_string(msg)
}
