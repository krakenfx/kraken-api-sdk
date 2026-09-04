//! Per-op executor closures/fns for the trade namespace. Each matches on
//! transport: `Rest` runs the REST body, `WsV2Auth` the WS req/decode path,
//! anything else a typed unsupported-transport error.

use std::sync::Arc;

use crate::api::WireRequest;
use crate::dispatch::{OrderOp, OrderSubmitStatus, Transport, WsOp};
use crate::rate_limit::{DynamicCostOp, Scope, order_age_cost};
use crate::rest::{RestError, RestSurface};
use crate::types::{AuthProfile, ClOrdId, Symbol, TxId};

use super::error::{OrderError, TradeError};
use super::events::{
    CancelEmitGuard, amend_id_in_result, emit_rest_submit_outcome, op_emits_ambiguous,
    op_emits_cancellation, op_emits_submitted, ws_txid_in_result,
};
use super::pending::{ExecFut, WsOrderCtx};
use super::snap::{snap_on_rest_rejection, snap_on_ws_rejection};
use super::types::{
    AddOrderBatchRequest, AddOrderBatchResponse, AddOrderDescr, AddOrderResponse, AmendId,
    AmendOrderResponse, BatchOrderEntry, BatchOrderResult, BatchResult, CancelAllRequest,
    CancelAllResponse, CancelBatchRequest, CancelBatchResponse, CancelOrderResponse, CancelRequest,
    DeadmanRequest, DeadmanResponse, OrderAmendRequest, OrderRequest,
};
use super::types::{
    RawAddOrderResponse, RawAmendOrderResponse, RawCancelAllResponse, RawCancelOrderResponse,
    RawDeadmanResponse,
};
use super::ws_compose::{
    AddOrderWsView, compose_add_order_params, compose_amend_order_params, compose_batch_add_params,
    compose_cancel_order_params, decode_ws_add_order, decode_ws_amend_order, decode_ws_batch_add,
    decode_ws_cancel_all, decode_ws_cancel_order, decode_ws_deadman, ws_wire_error_status,
};
use super::{
    gate_system_status, unsupported_transport, ws_conn_error_to_trade, ws_surface_unavailable,
};

/// Resolve this order's REST endpoint + auth from the frozen dispatch table.
/// A missing entry is a build-time table bug → typed error, never a panic.
fn rest_route(ctx: &WsOrderCtx) -> Result<(&'static str, AuthProfile), TradeError> {
    let entry = ctx.dispatch.resolve(
        ctx.op,
        crate::dispatch::dispatch_table::Product::Spot,
        Transport::Rest,
    )?;
    let auth = if entry.requires_auth {
        AuthProfile::SpotV1
    } else {
        AuthProfile::Public
    };
    Ok((entry.wire_target, auth))
}

/// REST pre-send charge from the cl_ord_id index; cost decays with order age.
/// HIT → charge the tracker (reject at cap, stamped with the dispatch id);
/// MISS → log and skip (snap_to_cap covers drift).
fn charge_pre_send_rest(
    rest: &RestSurface,
    cl_ord_id: &ClOrdId,
    op: DynamicCostOp,
    miss_log: &'static str,
    request_id: &str,
) -> Result<(), TradeError> {
    let now = rest.clock_now();
    if let (Some(entry), Some(key)) = (
        rest.cl_ord_id_index().lookup_and_promote(cl_ord_id),
        rest.api_key(),
    ) {
        let age = now.0.saturating_sub(entry.sent_at.0);
        let cost = order_age_cost(op, age);
        rest.trading_tracker()
            .consume(Scope::Pair(key, entry.pair.clone()), cost, now)
            .map_err(|e| TradeError::from(RestError::RateLimit(e)).with_request_id(request_id))?;
    } else {
        tracing::debug!(cl_ord_id = %cl_ord_id, "{}", miss_log);
    }
    Ok(())
}

/// WS pre-send charge from the shared index; cost decays with order age.
/// HIT → (Trading cost, pair for the reactive snap); MISS/no-WS → (None, None).
fn charge_pre_send_ws(
    ctx: &WsOrderCtx,
    rest: &RestSurface,
    cl_ord_id: &ClOrdId,
    op: DynamicCostOp,
    miss_log: &'static str,
) -> (crate::rest::RateLimitCost, Option<Symbol>) {
    if let Some(ws_ref) = ctx.ws.as_ref() {
        if let Some(entry) = ws_ref
            .cl_ord_id_index()
            .and_then(|idx| idx.lookup_and_promote(cl_ord_id))
        {
            let now = rest.clock_now();
            let age = now.0.saturating_sub(entry.sent_at.0);
            let cost = order_age_cost(op, age);
            let pair = entry.pair.clone();
            (
                crate::rest::RateLimitCost::Trading {
                    cost,
                    pair: entry.pair,
                },
                Some(pair),
            )
        } else {
            tracing::debug!(cl_ord_id = %cl_ord_id, "{}", miss_log);
            (crate::rest::RateLimitCost::None, None)
        }
    } else {
        (crate::rest::RateLimitCost::None, None)
    }
}

pub(crate) fn add_order_exec(
    req: OrderRequest,
    rest: Arc<RestSurface>,
    transport: Transport,
    ctx: WsOrderCtx,
) -> ExecFut<AddOrderResponse> {
    // Buy/sell are distinct request types (can't share a generic executor);
    // both share the decode_add_order Raw→typed decode. Wire differs only by
    // the type=buy|sell field.
    match transport {
        Transport::Rest => Box::pin(async move {
            req.validate()?;
            // margin is WS-native (REST has no `margin` field); reject rather
            // than silently drop it — a dropped margin flag would fill the order
            // unleveraged (a money-path defect).
            if let Some(field) = super::ws_compose::rest_unrepresentable_field(req.margin) {
                return Err(TradeError::RestUnsupportedOrderField { field });
            }
            let form = req.wire_params();
            // cl_ord_id: caller id, SDK UUID v4, or None when auto-alloc
            // suppressed (userref / conditional-close). When None the cancel
            // guard, index, and OrderSubmittedEvent are all skipped.
            let cl_ord_id = req.cl_ord_id.clone();
            let pair = req.pair.clone();
            // Arm the cancellation guard immediately before the wire await;
            // disarm whichever way the await resolves. Only when there is a
            // cl_ord_id to reconcile by (add/amend/cancel ops only).
            let (path, auth) = rest_route(&ctx)?;
            let request_id = crate::rest::mint_request_id();
            let mut guard = cl_ord_id.clone().map(|c| {
                CancelEmitGuard::armed(&rest, c, OrderOp::AddOrder, Some(request_id.clone()))
            });
            let sent_at = rest.clock_now();
            let result = rest
                .signed_post_costed(
                    path,
                    form,
                    auth,
                    crate::rest::RateLimitCost::Trading {
                        cost: 1.0,
                        pair: pair.clone(),
                    },
                    // AddOrder placement: never auto-retry; a failed send is reconciled
                    // by the caller. Validate mode places nothing, so it alone takes
                    // the single fresh-nonce heal.
                    if req.validate {
                        crate::rest::RetryPolicy::never_retry_except_nonce()
                    } else {
                        crate::rest::RetryPolicy::never_retry()
                    },
                    &request_id,
                )
                .await;
            if let Some(g) = guard.as_mut() {
                g.disarm();
            }
            // Reactive snap BEFORE From<RestError> conversion. pair is in-hand
            // for add_order (no index lookup needed).
            if let Err(RestError::Kraken(ref codes)) = result {
                snap_on_rest_rejection(&rest, codes, Some(&pair), sent_at);
            }
            // Populate the cl_ord_id→pair index on engine-accept (Ok branch only).
            if result.is_ok() {
                if let Some(c) = cl_ord_id.clone() {
                    rest.cl_ord_id_index().insert(c, pair, sent_at);
                }
            }
            if let Some(c) = cl_ord_id.clone() {
                emit_rest_submit_outcome(
                    &rest,
                    &result,
                    c,
                    None,
                    OrderOp::AddOrder,
                    sent_at,
                    &request_id,
                );
            }
            let result = result.map_err(|e| TradeError::from(e).with_request_id(&request_id))?;
            decode_add_order(result, cl_ord_id).map_err(|e| e.with_request_id(&request_id))
        }),
        Transport::WsV2Auth => Box::pin(async move {
            req.validate()?;
            // WS can't represent a leverage ratio or a RELATIVE conditional-close
            // price (WS conditional has no price_type slot → would ship as
            // absolute); reject. All other advanced fields are WS-mapped.
            let (close_ordertype, close_price, close_price2) =
                super::types::ConditionalClose::flat_legs(&req.conditional_close);
            if let Some(field) = super::ws_compose::ws_unrepresentable_field(
                req.leverage.is_some(),
                close_price.as_ref(),
                close_price2.as_ref(),
            ) {
                return Err(TradeError::WsUnsupportedOrderField { field });
            }
            // None when auto-alloc suppressed: composer omits cl_ord_id and
            // index/emit threading is skipped (mirrors the REST arm).
            let cl_ord_id = req.cl_ord_id.clone();
            let params = compose_add_order_params(AddOrderWsView {
                side: req.side.as_ref(),
                pair: &req.pair,
                volume: &req.volume,
                order_type: req.order_type,
                price: &req.price,
                price2: &req.price2,
                oflags: &req.oflags,
                margin: req.margin,
                reduce_only: &req.reduce_only,
                stp_type: &req.stp_type,
                trigger: &req.trigger,
                time_in_force: &req.time_in_force,
                display_vol: &req.display_vol,
                start_time: &req.start_time,
                expire_time: &req.expire_time,
                userref: &req.userref,
                cl_ord_id: &cl_ord_id,
                close_ordertype: &close_ordertype,
                close_price: &close_price,
                close_price2: &close_price2,
                validate: req.validate,
            });
            let emit_cl = cl_ord_id.clone();
            let pair = req.pair.clone();
            ws_send_and_decode(
                ctx,
                &rest,
                WsOp::AddOrder,
                OrderOp::AddOrder,
                emit_cl,
                None,
                params,
                // Charge the same SpotTradingRateLimitTracker REST uses.
                // +1 per add_order (same cost as the REST arm).
                crate::rest::RateLimitCost::Trading {
                    cost: 1.0,
                    pair: pair.clone(),
                },
                // Thread (cl_ord_id, pair) so ws_send_and_decode can populate
                // the index on accept (skipped when no cl_ord_id).
                cl_ord_id.clone().map(|c| (c, pair.clone())),
                // pair is in-hand for add_order snap.
                Some(pair),
                move |resp| decode_ws_add_order(resp, cl_ord_id),
            )
            .await
        }),
        other => {
            let err = unsupported_transport(ctx.op, &ctx.dispatch, other);
            Box::pin(async move { Err(err) })
        }
    }
}

/// Shared AddOrder Raw→typed decode (identical for buy + sell).
pub(crate) fn decode_add_order(
    result: serde_json::Value,
    cl_ord_id: Option<ClOrdId>,
) -> Result<AddOrderResponse, TradeError> {
    let raw: RawAddOrderResponse = serde_json::from_value(result)
        .map_err(|e| TradeError::malformed(format!("add_order decode: {}", e)))?;
    // Validate mode returns descr-only (no txid) → None.
    let txid = raw
        .txid
        .into_iter()
        .next()
        .filter(|s| !s.is_empty())
        .map(TxId::new);
    Ok(AddOrderResponse {
        txid,
        // None when auto-alloc suppressed — do not fabricate a cl_ord_id Kraken never saw.
        cl_ord_id,
        descr: AddOrderDescr {
            order: Some(raw.descr.order),
            close: raw.descr.close,
        },
    })
}

pub(crate) fn amend_order_exec(
    req: OrderAmendRequest,
    rest: Arc<RestSurface>,
    transport: Transport,
    ctx: WsOrderCtx,
) -> ExecFut<AmendOrderResponse> {
    match transport {
        Transport::Rest => Box::pin(async move {
            req.validate()?;
            let form = req.wire_params();
            let cl_ord_id = req.cl_ord_id.clone();
            // Resolve the route before pre-charge/guard so a table miss can't leak budget.
            let (path, auth) = rest_route(&ctx)?;
            let request_id = crate::rest::mint_request_id();
            charge_pre_send_rest(
                &rest,
                &cl_ord_id,
                DynamicCostOp::AmendOrder,
                "amend_order REST: cl_ord_id index MISS — skipping pre-charge",
                &request_id,
            )?;
            let mut guard = CancelEmitGuard::armed(
                &rest,
                cl_ord_id.clone(),
                OrderOp::AmendOrder,
                Some(request_id.clone()),
            );
            let sent_at = rest.clock_now();
            let result = rest
                .signed_post_costed(
                    path,
                    form,
                    auth,
                    crate::rest::RateLimitCost::None,
                    // AmendOrder: never auto-retry.
                    crate::rest::RetryPolicy::never_retry(),
                    &request_id,
                )
                .await;
            guard.disarm();
            // Snap on rejection; index MISS → skip Pair snap.
            if let Err(RestError::Kraken(ref codes)) = result {
                let pair_opt = rest
                    .cl_ord_id_index()
                    .lookup_and_promote(&cl_ord_id)
                    .map(|e| e.pair);
                snap_on_rest_rejection(&rest, codes, pair_opt.as_ref(), sent_at);
            }
            emit_rest_submit_outcome(
                &rest,
                &result,
                cl_ord_id,
                None,
                OrderOp::AmendOrder,
                sent_at,
                &request_id,
            );
            let result = result.map_err(|e| TradeError::from(e).with_request_id(&request_id))?;
            let raw: RawAmendOrderResponse = serde_json::from_value(result)
                .map_err(|e| TradeError::malformed(format!("amend decode: {}", e)))
                .map_err(|e| e.with_request_id(&request_id))?;
            if raw.amend_id.is_empty() {
                return Err(
                    TradeError::malformed("amend decode: empty amend_id".to_string())
                        .with_request_id(&request_id),
                );
            }
            // Amends preserve txid — reply carries amend_id only.
            Ok(AmendOrderResponse {
                amend_id: AmendId::from(raw.amend_id),
            })
        }),
        Transport::WsV2Auth => Box::pin(async move {
            req.validate()?;
            // display_qty is REST-only on amend; reject rather than silently drop.
            if req.display_qty.is_some() {
                return Err(TradeError::WsUnsupportedOrderField {
                    field: "display_qty",
                });
            }
            let cl_ord_id = req.cl_ord_id.clone();
            let params = compose_amend_order_params(&req);
            let (ws_cost, ws_snap_pair) = charge_pre_send_ws(
                &ctx,
                &rest,
                &cl_ord_id,
                DynamicCostOp::AmendOrder,
                "amend_order WS: cl_ord_id index MISS — skipping pre-charge",
            );
            ws_send_and_decode(
                ctx,
                &rest,
                WsOp::AmendOrder,
                OrderOp::AmendOrder,
                Some(cl_ord_id),
                None,
                params,
                ws_cost,
                None,
                ws_snap_pair,
                decode_ws_amend_order,
            )
            .await
        }),
        other => {
            let err = unsupported_transport(ctx.op, &ctx.dispatch, other);
            Box::pin(async move { Err(err) })
        }
    }
}

/// Core REST cancel-one path, shared by single-cancel and the cancel_batch
/// fan-out so both run one wire path (per-call guard/emit/decode preserved).
/// REST only — WS cancel stays in `cancel_exec`.
pub(crate) async fn cancel_one(
    req: CancelRequest,
    rest: Arc<RestSurface>,
    path: &'static str,
    auth: AuthProfile,
) -> Result<CancelOrderResponse, TradeError> {
    let form = req.wire_params();
    let cl_ord_id = req.cl_ord_id.clone();
    let request_id = crate::rest::mint_request_id();
    charge_pre_send_rest(
        &rest,
        &cl_ord_id,
        DynamicCostOp::CancelOrder,
        "cancel_one REST: cl_ord_id index MISS — skipping pre-charge",
        &request_id,
    )?;
    let mut guard = CancelEmitGuard::armed(
        &rest,
        cl_ord_id.clone(),
        OrderOp::CancelOrder,
        Some(request_id.clone()),
    );
    let sent_at = rest.clock_now();
    let result = rest
        .signed_post_costed(
            path,
            form,
            auth,
            crate::rest::RateLimitCost::None,
            crate::rest::RetryPolicy::idempotent(),
            &request_id,
        )
        .await;
    guard.disarm();
    if let Err(RestError::Kraken(ref codes)) = result {
        let pair_opt = rest
            .cl_ord_id_index()
            .lookup_and_promote(&cl_ord_id)
            .map(|e| e.pair);
        snap_on_rest_rejection(&rest, codes, pair_opt.as_ref(), sent_at);
    }
    // A transport drop on cancel emits nothing (ambiguous is add/amend only).
    emit_rest_submit_outcome(
        &rest,
        &result,
        cl_ord_id,
        None,
        OrderOp::CancelOrder,
        sent_at,
        &request_id,
    );
    let result = result.map_err(|e| TradeError::from(e).with_request_id(&request_id))?;
    let raw: RawCancelOrderResponse = serde_json::from_value(result)
        .map_err(|e| TradeError::malformed(format!("cancel decode: {}", e)))
        .map_err(|e| e.with_request_id(&request_id))?;
    Ok(CancelOrderResponse {
        count: raw.count,
        pending: raw.pending,
    })
}

pub(crate) fn cancel_exec(
    req: CancelRequest,
    rest: Arc<RestSurface>,
    transport: Transport,
    ctx: WsOrderCtx,
) -> ExecFut<CancelOrderResponse> {
    match transport {
        Transport::Rest => Box::pin(async move {
            let (path, auth) = rest_route(&ctx)?;
            cancel_one(req, rest, path, auth).await
        }),
        Transport::WsV2Auth => Box::pin(async move {
            // cancel wire `cl_ord_id` is an array (add/amend use a scalar).
            let cl_ord_id = req.cl_ord_id.clone();
            let params = compose_cancel_order_params(&req);
            let (ws_cost, ws_snap_pair) = charge_pre_send_ws(
                &ctx,
                &rest,
                &cl_ord_id,
                DynamicCostOp::CancelOrder,
                "cancel_order WS: cl_ord_id index MISS — skipping pre-charge",
            );
            ws_send_and_decode(
                ctx,
                &rest,
                WsOp::CancelOrder,
                OrderOp::CancelOrder,
                Some(cl_ord_id),
                None,
                params,
                ws_cost,
                None,
                ws_snap_pair,
                decode_ws_cancel_order,
            )
            .await
        }),
        other => {
            let err = unsupported_transport(ctx.op, &ctx.dispatch, other);
            Box::pin(async move { Err(err) })
        }
    }
}

/// Fan-out executor for `cancel_batch`: each cl_ord_id cancelled via a
/// concurrent `cancel_one`, results assembled 1:1 in input order. Per-line
/// independent — one line's error becomes its `BatchResult::Err`, overall Ok.
pub(crate) fn cancel_batch_exec(
    req: CancelBatchRequest,
    rest: Arc<RestSurface>,
    // Unused: cancel_batch is REST-only; pre-wire gate already rejected non-REST.
    _transport: Transport,
    ctx: WsOrderCtx,
) -> ExecFut<CancelBatchResponse> {
    Box::pin(async move {
        let (path, auth) = rest_route(&ctx)?;
        let futs = req.cl_ord_ids.into_iter().map(|cl| {
            let rest = Arc::clone(&rest);
            async move {
                let cancel_req = CancelRequest::new(cl.clone());
                match cancel_one(cancel_req, rest, path, auth).await {
                    Ok(resp) => BatchResult::Ok(resp),
                    Err(e) => BatchResult::Err(OrderError::from_trade_error(cl, e)),
                }
            }
        });
        let results = futures_util::future::join_all(futs).await;
        Ok(CancelBatchResponse { results })
    })
}

pub(crate) fn cancel_all_exec(
    req: CancelAllRequest,
    rest: Arc<RestSurface>,
    transport: Transport,
    ctx: WsOrderCtx,
) -> ExecFut<CancelAllResponse> {
    match transport {
        Transport::Rest => Box::pin(async move {
            let form = req.wire_params();
            let (path, auth) = rest_route(&ctx)?;
            let sent_at = rest.clock_now();
            // CancelAll: +1 per tracked pair (proxy for affected); never blocked.
            let request_id = crate::rest::mint_request_id();
            let result = rest
                .signed_post_costed(
                    path,
                    form,
                    auth,
                    crate::rest::RateLimitCost::TradingAccountWide { cost: 1.0 },
                    crate::rest::RetryPolicy::idempotent(),
                    &request_id,
                )
                .await;
            // Domain rate-limit snap (account-wide).
            if let Err(RestError::Kraken(ref codes)) = result {
                snap_on_rest_rejection(&rest, codes, None, sent_at);
            }
            let result = result.map_err(|e| TradeError::from(e).with_request_id(&request_id))?;
            let raw: RawCancelAllResponse = serde_json::from_value(result)
                .map_err(|e| TradeError::malformed(format!("cancel_all decode: {}", e)))
                .map_err(|e| e.with_request_id(&request_id))?;
            Ok(CancelAllResponse { count: raw.count })
        }),
        Transport::WsV2Auth => Box::pin(async move {
            let _ = &req; // unit request — cancel_all takes only the token
            ws_send_and_decode(
                ctx,
                &rest,
                WsOp::CancelAll,
                OrderOp::CancelAll,
                None,
                None,
                serde_json::json!({}),
                crate::rest::RateLimitCost::TradingAccountWide { cost: 1.0 },
                None,
                None,
                decode_ws_cancel_all,
            )
            .await
        }),
        other => {
            let err = unsupported_transport(ctx.op, &ctx.dispatch, other);
            Box::pin(async move { Err(err) })
        }
    }
}

pub(crate) fn deadman_exec(
    req: DeadmanRequest,
    rest: Arc<RestSurface>,
    transport: Transport,
    ctx: WsOrderCtx,
) -> ExecFut<DeadmanResponse> {
    match transport {
        Transport::Rest => Box::pin(async move {
            let form = req.wire_params();
            let (path, auth) = rest_route(&ctx)?;
            let sent_at = rest.clock_now();
            // Deadman: no proactive pair charge; reactive domain snap backstops.
            let request_id = crate::rest::mint_request_id();
            let result = rest
                .signed_post_costed(
                    path,
                    form,
                    auth,
                    crate::rest::RateLimitCost::None,
                    crate::rest::RetryPolicy::idempotent(),
                    &request_id,
                )
                .await;
            if let Err(RestError::Kraken(ref codes)) = result {
                snap_on_rest_rejection(&rest, codes, None, sent_at);
            }
            let result = result.map_err(|e| TradeError::from(e).with_request_id(&request_id))?;
            let raw: RawDeadmanResponse = serde_json::from_value(result).map_err(|e| {
                TradeError::malformed(format!("cancel_all_orders_after decode: {}", e))
                    .with_request_id(&request_id)
            })?;
            // triggerTime "0" or absent = disarmed.
            let trigger_time = match raw.triggerTime.as_str() {
                "" | "0" => None,
                _ => Some(raw.triggerTime),
            };
            Ok(DeadmanResponse {
                current_time: raw.currentTime,
                trigger_time,
            })
        }),
        Transport::WsV2Auth => Box::pin(async move {
            let params = serde_json::json!({ "timeout": req.timeout_secs });
            ws_send_and_decode(
                ctx,
                &rest,
                WsOp::CancelAllOrdersAfter,
                OrderOp::CancelAllOrdersAfter,
                None,
                None,
                params,
                crate::rest::RateLimitCost::None,
                None,
                None,
                decode_ws_deadman,
            )
            .await
        }),
        other => {
            let err = unsupported_transport(ctx.op, &ctx.dispatch, other);
            Box::pin(async move { Err(err) })
        }
    }
}

/// Index each placed batch row (txid present, no error) by its cl_ord_id so a
/// live sibling stays cancellable. Suppressed entries carry no cl_ord_id and are
/// skipped. Rows are 1:1 positional with the request.
fn index_placed_batch_rows(
    rest: &RestSurface,
    pair: &Symbol,
    entries: &[BatchOrderEntry],
    rows: &[BatchOrderResult],
    sent_at: crate::types::MonotonicInstant,
) {
    debug_assert_eq!(
        entries.len(),
        rows.len(),
        "AddOrderBatch response MUST be 1:1 positional with the request"
    );
    for (entry, line) in entries.iter().zip(rows.iter()) {
        if line.txid.is_some() && line.error.is_none() {
            if let Some(cl_ord_id) = entry.cl_ord_id.clone() {
                rest.cl_ord_id_index()
                    .insert(cl_ord_id, pair.clone(), sent_at);
            }
        }
    }
}

pub(crate) fn order_batch_exec(
    req: AddOrderBatchRequest,
    rest: Arc<RestSurface>,
    transport: Transport,
    ctx: WsOrderCtx,
) -> ExecFut<AddOrderBatchResponse> {
    match transport {
        Transport::Rest => Box::pin(async move {
            req.validate()?;
            if let Some(field) = req
                .orders
                .iter()
                .find_map(|e| super::ws_compose::rest_unrepresentable_field(e.margin))
            {
                return Err(TradeError::RestUnsupportedOrderField { field });
            }
            let form = req.wire_params();
            // Batch add costs n/2 on the trading counter.
            let cost = req.orders.len() as f64 / 2.0;
            let pair = req.pair.clone();
            let (path, auth) = rest_route(&ctx)?;
            let sent_at = rest.clock_now(); // one shared timestamp for every batch entry
            let request_id = crate::rest::mint_request_id();
            let result = rest
                .signed_post_costed(
                    path,
                    form,
                    auth,
                    crate::rest::RateLimitCost::Trading {
                        cost,
                        pair: pair.clone(),
                    },
                    // Batch: never auto-retry. Validate mode alone takes the fresh-nonce heal.
                    if req.validate {
                        crate::rest::RetryPolicy::never_retry_except_nonce()
                    } else {
                        crate::rest::RetryPolicy::never_retry()
                    },
                    &request_id,
                )
                .await;
            if let Err(RestError::Kraken(ref codes)) = result {
                snap_on_rest_rejection(&rest, codes, Some(&pair), sent_at);
            }
            let result = result.map_err(|e| TradeError::from(e).with_request_id(&request_id))?;
            let resp =
                decode_add_order_batch(result).map_err(|e| e.with_request_id(&request_id))?;
            guard_batch_row_count(req.orders.len(), resp.orders.len())
                .map_err(|e| e.with_request_id(&request_id))?;
            index_placed_batch_rows(&rest, &pair, &req.orders, &resp.orders, sent_at);
            Ok(resp)
        }),
        Transport::WsV2Auth => Box::pin(async move {
            req.validate()?;
            let params = compose_batch_add_params(&req)?;
            let pair = req.pair.clone();
            let cost = req.orders.len() as f64 / 2.0;
            let rest_idx = Arc::clone(&rest);
            let sent_at = rest.clock_now();
            ws_send_and_decode(
                ctx,
                &rest,
                WsOp::BatchAdd,
                OrderOp::OrderBatch,
                None,
                None,
                params,
                crate::rest::RateLimitCost::Trading {
                    cost,
                    pair: pair.clone(),
                },
                None, // per-row index handled in the decode closure below
                Some(pair.clone()),
                move |resp| {
                    let decoded = decode_and_guard_ws_batch(req.orders.len(), resp)?;
                    index_placed_batch_rows(
                        &rest_idx,
                        &req.pair,
                        &req.orders,
                        &decoded.orders,
                        sent_at,
                    );
                    Ok(decoded)
                },
            )
            .await
        }),
        other => {
            let err = unsupported_transport(ctx.op, &ctx.dispatch, other);
            Box::pin(async move { Err(err) })
        }
    }
}

/// WS batch decode seam: decode + positional row-count guard, exactly what the
/// WS arm's closure runs before indexing.
pub(super) fn decode_and_guard_ws_batch(
    expected: usize,
    resp: crate::conn::managed_connection::WsResponse,
) -> Result<super::types::AddOrderBatchResponse, TradeError> {
    let decoded = decode_ws_batch_add(resp)?;
    guard_batch_row_count(expected, decoded.orders.len())?;
    Ok(decoded)
}

/// Batch reply rows can only be attributed to request orders POSITIONALLY —
/// rejected rows carry no cl_ord_id or index. A count mismatch
/// makes attribution unsafe, so it surfaces as malformed, never a wrong zip.
fn guard_batch_row_count(expected: usize, got: usize) -> Result<(), TradeError> {
    if expected != got {
        return Err(TradeError::malformed(format!(
            "batch reply carried {got} rows for {expected} orders — rows cannot be attributed"
        )));
    }
    Ok(())
}

/// Wire-shape decoder for `POST /0/private/AddOrderBatch`. Each row maps 1:1 to
/// a `BatchOrderResult` populated independently — a per-line rejection never
/// discards a placed sibling's txid. Wire shapes: docs/guides/wire-quirks.md.
fn decode_add_order_batch(result: serde_json::Value) -> Result<AddOrderBatchResponse, TradeError> {
    #[derive(serde::Deserialize)]
    struct RawBatchDescr {
        #[serde(default)]
        order: Option<String>,
        #[serde(default)]
        close: Option<String>,
    }

    #[derive(serde::Deserialize)]
    struct RawBatchOrderResult {
        descr: Option<RawBatchDescr>,
        // Per-entry txid is a SCALAR string (unlike single AddOrder's array).
        #[serde(default)]
        txid: Option<String>,
        error: Option<String>,
    }

    #[derive(serde::Deserialize)]
    struct RawBatchResponse {
        orders: Vec<RawBatchOrderResult>,
    }

    let raw: RawBatchResponse = serde_json::from_value(result)
        .map_err(|e| TradeError::malformed(format!("add_order_batch decode: {}", e)))?;

    let orders = raw
        .orders
        .into_iter()
        .map(|o| {
            let descr = match o.descr {
                Some(d) => AddOrderDescr {
                    order: d.order,
                    close: d.close,
                },
                None => AddOrderDescr {
                    order: None,
                    close: None,
                },
            };
            BatchOrderResult {
                descr,
                txid: o.txid.filter(|s| !s.is_empty()).map(TxId::new),
                error: o.error,
            }
        })
        .collect();

    Ok(AddOrderBatchResponse { orders })
}

/// Shared WS order-send body: gate SystemStatus, post the token-free request,
/// await the reactor-decoded `WsResponse`, decode per-op, and emit lifecycle
/// events (success → `WireSent`/`WireAccepted`, rejection → `WireError`).
#[allow(clippy::too_many_arguments)] // emit + send + rate-limit + index + snap context all ride together
pub(crate) async fn ws_send_and_decode<Resp>(
    ctx: WsOrderCtx,
    rest: &Arc<RestSurface>,
    op: WsOp,
    order_op: OrderOp,
    emit_cl_ord_id: Option<ClOrdId>,
    emit_amend_id: Option<AmendId>,
    params: serde_json::Value,
    cost: crate::rest::RateLimitCost,
    // `Some((cl_ord_id, pair))` for add_order; `None` for other ops.
    index_on_accept: Option<(ClOrdId, Symbol)>,
    // Pair for reactive snap; None on index MISS or ops with no pair.
    snap_pair: Option<Symbol>,
    decode: impl FnOnce(crate::conn::managed_connection::WsResponse) -> Result<Resp, TradeError>,
) -> Result<Resp, TradeError> {
    gate_system_status(&ctx.system_status, op)?;
    let order_deadline = ctx.order_deadline;
    let ws = ctx.ws.ok_or_else(ws_surface_unavailable)?;
    // Dead reactor → LoopDead (non-retryable), not a misleading retryable Transport.
    if ws.is_loop_failed() {
        return Err(TradeError::LoopDead);
    }
    // Bring auth WS to Open before send; no REST fallback.
    ws.ensure_order_sendable(crate::types::WsUrl::Auth)
        .await
        .map_err(ws_conn_error_to_trade)?;
    // Charge after ensure_order_sendable, before guard/sent_at (rate-limit reject = never-sent).
    let req_id = ws.next_req_id();
    let request_id = req_id.to_string();
    let handle = ws
        .send_request_costed(op.to_string(), req_id, params, cost)
        .map_err(|e| TradeError::from(RestError::RateLimit(e)).with_request_id(&request_id))?;
    // Arm cancel guard + sent_at immediately before the wire .await.
    let mut guard = match (&emit_cl_ord_id, op_emits_cancellation(order_op)) {
        (Some(cl), true) => Some(CancelEmitGuard::armed(
            rest,
            cl.clone(),
            order_op,
            Some(request_id.clone()),
        )),
        _ => None,
    };
    let sent_at = rest.clock_now();
    // Wire .await; deadline expiry is sent-ambiguous.
    let recv = match order_deadline {
        Some(d) => match tokio::time::timeout(d, handle.recv()).await {
            Ok(r) => r,
            Err(_elapsed) => {
                // Deadline expiry — abandon the pending entry (sent-ambiguous).
                ws.abandon_request(req_id);
                Err(crate::error::ConnectionError::response_timeout())
            }
        },
        None => handle.recv().await,
    };
    if let Some(g) = guard.as_mut() {
        g.disarm();
    }
    if let Some(cl) = emit_cl_ord_id {
        match &recv {
            Ok(resp) if resp.success => {
                // Index on accept before emit (immediate amend/cancel needs it).
                if let Some((index_cl, index_pair)) = index_on_accept {
                    ws.index_on_accept(index_cl, index_pair, sent_at);
                }
                // Placements → WireSent; amend/cancel success → WireAccepted.
                if op_emits_submitted(order_op) {
                    match order_op {
                        OrderOp::AmendOrder => {
                            let amend_id =
                                amend_id_in_result(&resp.result).or_else(|| emit_amend_id.clone());
                            rest.emit_order_submitted(
                                cl.clone(),
                                amend_id,
                                order_op,
                                OrderSubmitStatus::WireAccepted,
                                Some(request_id.clone()),
                            );
                        }
                        OrderOp::CancelOrder => {
                            rest.emit_order_submitted(
                                cl.clone(),
                                None,
                                order_op,
                                OrderSubmitStatus::WireAccepted,
                                Some(request_id.clone()),
                            );
                        }
                        _ => {
                            if let Some(txid) = ws_txid_in_result(&resp.result) {
                                rest.emit_order_submitted(
                                    cl.clone(),
                                    emit_amend_id.clone(),
                                    order_op,
                                    OrderSubmitStatus::WireSent { txid },
                                    Some(request_id.clone()),
                                );
                            }
                        }
                    }
                }
            }
            Ok(resp) => {
                if op_emits_submitted(order_op) {
                    rest.emit_order_submitted(
                        cl.clone(),
                        emit_amend_id.clone(),
                        order_op,
                        ws_wire_error_status(resp.error.clone()),
                        Some(request_id.clone()),
                    );
                }
            }
            Err(e) if e.is_definitely_not_sent() => { /* not sent → no event */ }
            Err(_) => {
                // In-flight/unknown drop → ambiguous (add/amend only).
                if op_emits_ambiguous(order_op) {
                    rest.emit_placement_ambiguous(
                        cl.clone(),
                        emit_amend_id.clone(),
                        order_op,
                        sent_at,
                        Some(request_id.clone()),
                    );
                }
            }
        }
    }
    let resp = recv.map_err(|e| ws_conn_error_to_trade(e).with_request_id(&request_id))?;
    // Snap before decode (decode also classifies the error).
    if !resp.success {
        snap_on_ws_rejection(rest, &resp.error, snap_pair.as_ref(), sent_at);
    }
    decode(resp).map_err(|e| e.with_request_id(&request_id))
}
