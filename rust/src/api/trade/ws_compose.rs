//! WS request composition and reply decoding for the trade namespace.

use crate::conn::managed_connection::WsResponse;
use crate::dispatch::OrderSubmitStatus;
use crate::error::ApiError;
use crate::rest::RestError;
use crate::types::{ClOrdId, WsUrl};

use super::error::{TradeError, classify_futures_ws_error, classify_kraken_error_string};
use super::types::{
    AddOrderDescr, AddOrderResponse, AmendId, AmendOrderResponse, CancelAllResponse,
    CancelOrderResponse, DeadmanResponse, FuturesOrder, FuturesOrderAmendRequest,
    FuturesOrderBatchRequest, FuturesOrderRef, OFlag, OrderAmendRequest, OrderType, Price, StpType,
    TimeInForce, TimeSpec, TriggerKind, stp_type_ws,
};

pub(crate) fn wire_error_status(err: RestError) -> OrderSubmitStatus {
    let code = TradeError::from(err).code().to_string();
    OrderSubmitStatus::WireError { code }
}

pub(crate) fn ws_wire_error_status(url: WsUrl, resp: &WsResponse) -> OrderSubmitStatus {
    let code = classify_ws_failure(url, resp.error.clone(), resp.retry_after_ms)
        .code()
        .to_string();
    OrderSubmitStatus::WireError { code }
}

pub(crate) fn classify_ws_failure(
    url: WsUrl,
    error: Option<String>,
    retry_after_ms: Option<u64>,
) -> TradeError {
    if !url.is_unified() {
        return classify_ws_error(error);
    }
    match classify_futures_ws_error(error.unwrap_or_else(|| "WS request rejected".to_string())) {
        TradeError::RateLimited {
            retry_after_ts,
            request_id,
        } => TradeError::RateLimited {
            retry_after_ts: retry_after_ms
                .and_then(|ms| {
                    crate::rest::surface::unix_now_ms().map(|now| retry_after_ts_at(now, ms))
                })
                .or(retry_after_ts),
            request_id,
        },
        other => other,
    }
}

pub(crate) fn retry_after_ts_at(now_ms: u64, retry_after_ms: u64) -> u64 {
    now_ms.saturating_add(retry_after_ms).div_ceil(1000)
}

/// Parses the exact decimal string so no f64 precision is lost.
pub(crate) fn decimal_to_json(d: &rust_decimal::Decimal) -> serde_json::Value {
    let s = d.to_string();
    match s.parse::<serde_json::Number>() {
        Ok(n) => serde_json::Value::Number(n),
        Err(_) => serde_json::Value::String(s),
    }
}

/// Returns the field WS `add_order` cannot represent (leverage; relative conditional-close price); callers use REST.
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

/// Returns the field REST `add_order` cannot represent (`margin`); callers use WS.
pub(crate) fn rest_unrepresentable_field(has_margin: bool) -> Option<&'static str> {
    has_margin.then_some("margin")
}

pub(crate) struct AddOrderWsView<'a> {
    pub(crate) side: &'a str,
    pub(crate) symbol: &'a str,
    pub(crate) volume: &'a rust_decimal::Decimal,
    pub(crate) order_type: OrderType,
    pub(crate) price: &'a Option<Price>,
    pub(crate) price2: &'a Option<Price>,
    pub(crate) oflags: &'a [OFlag],
    pub(crate) margin: bool,
    pub(crate) reduce_only: &'a Option<bool>,
    pub(crate) stp_type: &'a Option<StpType>,
    pub(crate) trigger: &'a Option<TriggerKind>,
    pub(crate) time_in_force: &'a Option<TimeInForce>,
    pub(crate) display_vol: &'a Option<rust_decimal::Decimal>,
    pub(crate) start_time: &'a Option<TimeSpec>,
    pub(crate) expire_time: &'a Option<TimeSpec>,
    pub(crate) userref: &'a Option<i32>,
    /// `None` when suppressed: Kraken rejects `cl_ord_id` together with `userref`.
    pub(crate) cl_ord_id: &'a Option<ClOrdId>,
    pub(crate) close_ordertype: &'a Option<OrderType>,
    pub(crate) close_price: &'a Option<Price>,
    pub(crate) close_price2: &'a Option<Price>,
    pub(crate) validate: bool,
}

impl<'a> AddOrderWsView<'a> {
    pub(crate) fn new(
        side: &'a str,
        symbol: &'a str,
        volume: &'a rust_decimal::Decimal,
        order_type: OrderType,
    ) -> Self {
        Self {
            side,
            symbol,
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
}

/// The reactor injects the token; `deadline` is skipped (naive now() causes clock-skew rejects).
pub(crate) fn compose_add_order_params(view: AddOrderWsView<'_>) -> serde_json::Value {
    serde_json::Value::Object(add_order_map(view))
}

fn add_order_map(view: AddOrderWsView<'_>) -> serde_json::Map<String, serde_json::Value> {
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
        serde_json::Value::String(view.symbol.to_string()),
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

            if limit_leg && let Some(p2) = view.price2 {
                let (value, price_type) = p2.to_ws();
                p.insert("limit_price".to_string(), decimal_to_json(&value));
                p.insert(
                    "limit_price_type".to_string(),
                    serde_json::Value::String(price_type.to_string()),
                );
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

    // WS spells `stp_type` with underscores, not the REST hyphens.
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

    // Emit validate only when true: omitting it would place a live order.
    if view.validate {
        p.insert("validate".to_string(), serde_json::Value::Bool(true));
    }

    p
}

/// Entries WS cannot represent are rejected for REST fallback.
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
        let obj = add_order_map(AddOrderWsView {
            side: <&str>::from(e.side),
            symbol: req.pair.as_str(),
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
        orders.push(without_symbol(obj));
    }
    Ok(batch_add_envelope(req.pair.as_str(), orders, req.validate))
}

fn without_symbol(mut order: serde_json::Map<String, serde_json::Value>) -> serde_json::Value {
    order.remove("symbol");
    serde_json::Value::Object(order)
}

pub(crate) fn compose_futures_order_params(
    symbol: &str,
    order: &FuturesOrder,
    validate: bool,
) -> serde_json::Value {
    serde_json::Value::Object(futures_order_map(symbol, order, validate))
}

fn futures_order_map(
    symbol: &str,
    order: &FuturesOrder,
    validate: bool,
) -> serde_json::Map<String, serde_json::Value> {
    let mut params = add_order_map(AddOrderWsView {
        price: &order.price,
        price2: &order.price2,
        oflags: if order.post_only { &[OFlag::Post] } else { &[] },
        reduce_only: &order.reduce_only,
        stp_type: &order.stp_type,
        trigger: &order.trigger,
        time_in_force: &order.time_in_force,
        cl_ord_id: &order.cl_ord_id,
        validate,
        ..AddOrderWsView::new(order.side.as_ref(), symbol, &order.volume, order.order_type)
    });
    if let Some(broker) = &order.broker {
        params.insert("broker".to_string(), broker.clone().into());
    }
    if let Some(deadline) = order.deadline {
        params.insert("deadline".to_string(), deadline.to_ws().into());
    }
    params
}

pub(crate) fn compose_futures_batch_params(req: &FuturesOrderBatchRequest) -> serde_json::Value {
    let orders = req
        .orders
        .iter()
        .map(|order| without_symbol(futures_order_map(&req.symbol, order, false)))
        .collect();
    batch_add_envelope(&req.symbol, orders, req.validate)
}

fn batch_add_envelope(
    symbol: &str,
    orders: Vec<serde_json::Value>,
    validate: bool,
) -> serde_json::Value {
    let mut params = serde_json::Map::new();
    params.insert(
        "symbol".to_string(),
        serde_json::Value::String(symbol.to_string()),
    );
    params.insert("orders".to_string(), serde_json::Value::Array(orders));
    if validate {
        params.insert("validate".to_string(), serde_json::Value::Bool(true));
    }
    serde_json::Value::Object(params)
}

/// `cl_ord_id` is a scalar here (cancel uses an array).
pub(crate) fn compose_amend_order_params(req: &OrderAmendRequest) -> serde_json::Value {
    let mut p = serde_json::Map::new();
    p.insert(
        "cl_ord_id".to_string(),
        serde_json::Value::String(req.cl_ord_id.as_str().to_string()),
    );
    insert_amend_fields(
        &mut p,
        &req.order_volume,
        &req.limit_price,
        req.post_only,
        &req.trigger_price,
    );
    serde_json::Value::Object(p)
}

pub(crate) fn compose_futures_amend_params(req: &FuturesOrderAmendRequest) -> serde_json::Value {
    let mut p = serde_json::Map::new();
    let (key, id) = req.order.wire();
    p.insert(key.to_string(), serde_json::Value::String(id.to_string()));
    insert_amend_fields(
        &mut p,
        &req.order_volume,
        &req.limit_price,
        req.post_only,
        &req.trigger_price,
    );
    if let Some(deadline) = req.deadline {
        p.insert(
            "deadline".to_string(),
            serde_json::Value::String(deadline.to_ws()),
        );
    }
    serde_json::Value::Object(p)
}

pub(crate) fn compose_futures_cancel_params(order: &FuturesOrderRef) -> serde_json::Value {
    let (key, id) = order.wire();
    serde_json::json!({ key: [id] })
}

fn insert_amend_fields(
    p: &mut serde_json::Map<String, serde_json::Value>,
    order_volume: &Option<rust_decimal::Decimal>,
    limit_price: &Option<Price>,
    post_only: Option<bool>,
    trigger_price: &Option<Price>,
) {
    if let Some(v) = order_volume {
        p.insert("order_qty".to_string(), decimal_to_json(v));
    }
    if let Some(price) = limit_price {
        let (value, price_type) = price.to_ws();
        p.insert("limit_price".to_string(), decimal_to_json(&value));
        p.insert(
            "limit_price_type".to_string(),
            serde_json::Value::String(price_type.to_string()),
        );
    }
    if let Some(po) = post_only {
        p.insert("post_only".to_string(), serde_json::Value::Bool(po));
    }
    if let Some(tp) = trigger_price {
        // Amend uses flat trigger keys, not the `triggers` object.
        let (value, price_type) = tp.to_ws();
        p.insert("trigger_price".to_string(), decimal_to_json(&value));
        p.insert(
            "trigger_price_type".to_string(),
            serde_json::Value::String(price_type.to_string()),
        );
    }
}

pub(crate) fn compose_cancel_order_params(req: &super::types::CancelRequest) -> serde_json::Value {
    serde_json::json!({
        "cl_ord_id": [req.cl_ord_id.as_str()],
    })
}

/// A per-row array is `Ok` even when top-level `success` is false (placed siblings stay live); null `result` is `Err`.
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
    if resp.success {
        return Err(TradeError::malformed(
            "batch_add reply carried no row array".into(),
        ));
    }
    Err(classify_ws_error(resp.error))
}

/// Order id is `order_id` (not `txid`); `descr` is `None`.
pub(crate) fn decode_ws_add_order(
    resp: WsResponse,
    fallback_cl_ord_id: Option<ClOrdId>,
) -> Result<AddOrderResponse, TradeError> {
    // Validate-mode replies carry an empty `order_id`: treated as absent.
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

/// The Futures venue assigns no amend id: an empty or absent `amend_id` on Unified decodes to an
/// empty id.
pub(crate) fn decode_ws_amend_order(
    url: WsUrl,
    resp: WsResponse,
) -> Result<AmendOrderResponse, TradeError> {
    let amend_id = resp
        .result
        .get("amend_id")
        .and_then(serde_json::Value::as_str)
        .or(url.is_unified().then_some(""))
        .filter(|s| url.is_unified() || !s.is_empty())
        .ok_or_else(|| TradeError::malformed("ws amend response missing amend_id".to_string()))?;
    Ok(AmendOrderResponse {
        amend_id: AmendId::from(amend_id.to_string()),
    })
}

/// The reply carries only `cl_ord_id` (absent is `MalformedResponse`); derives `count:1, pending:false`.
pub(crate) fn decode_ws_cancel_order(
    identity_key: &'static str,
    resp: WsResponse,
) -> Result<CancelOrderResponse, TradeError> {
    resp.result
        .get(identity_key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            TradeError::malformed(format!("ws cancel_order success missing {identity_key}"))
        })?;
    Ok(CancelOrderResponse {
        count: 1,
        pending: false,
    })
}

/// Missing or non-integer `count` is `MalformedResponse`; never fabricates 0.
pub(crate) fn decode_ws_cancel_all(resp: WsResponse) -> Result<CancelAllResponse, TradeError> {
    let count = resp
        .result
        .get("count")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            TradeError::malformed("cancel_all WS reply: missing or non-integer `count`".to_string())
        })? as u32;
    Ok(CancelAllResponse { count })
}

/// `"0"` or absent `triggerTime` means disarmed: `None`.
pub(crate) fn decode_ws_deadman(resp: WsResponse) -> Result<DeadmanResponse, TradeError> {
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

/// WS carries one top-level `error` string; unmapped or absent degrades to `Unknown`.
pub(crate) fn classify_ws_error(error: Option<String>) -> TradeError {
    let msg = error.unwrap_or_else(|| "WS request rejected".to_string());
    if crate::error::is_token_stale_reject(WsUrl::Auth, &msg) {
        return TradeError::SessionExpired { request_id: None };
    }
    classify_kraken_error_string(msg)
}
