//! `TradeNamespace` — Spot private REST/WS trading. Every order method returns
//! a [`PendingTrade<Req, Resp>`]; the wire call fires on `.await`. Defaults to WS v2.

use std::sync::Arc;

use crate::api::SystemStatus;
use crate::dispatch::{Op, Transport, WsOp};
use crate::error::ConnectionError;
use crate::rest::RestSurface;
use crate::types::ClOrdId;

mod error;
mod events;
mod executors;
mod pending;
mod snap;
mod types;
mod ws_compose;

#[cfg(test)]
mod tests;

pub use error::{OrderError, TradeError};
pub use pending::PendingTrade;
pub use types::{
    AddOrderBatchRequest, AddOrderBatchResponse, AddOrderDescr, AddOrderResponse, AmendId,
    AmendOrderResponse, BatchOrderEntry, BatchOrderResult, BatchResult, CancelAllRequest,
    CancelAllResponse, CancelBatchRequest, CancelBatchResponse, CancelOrderResponse, CancelRequest,
    CloseOrderType, ConditionalClose, DeadlineSpec, DeadmanRequest, DeadmanResponse, OFlag,
    OrderAmendRequest, OrderRequest, OrderType, Price, PriceUnit, Side, StpType, TimeInForce,
    TimeSpec, TriggerKind,
};

#[cfg(test)]
pub(crate) use ws_compose::{
    AddOrderWsView, compose_add_order_params, compose_amend_order_params,
    compose_cancel_order_params, decode_ws_add_order, decode_ws_amend_order, decode_ws_cancel_all,
    decode_ws_cancel_order, ws_unrepresentable_field,
};

#[cfg(test)]
#[allow(unused_imports)]
use crate::conn::managed_connection::WsResponse;
#[cfg(test)]
#[allow(unused_imports)]
use crate::dispatch::{
    DispatchEventBus, EventEnvelope, EventPayload, EventType, OrderOp, OrderSubmitStatus,
};
#[cfg(test)]
#[allow(unused_imports)]
use crate::rate_limit::{
    DynamicCostOp, Scope, SnapTarget, classify_rate_limit_snap, order_age_cost,
};
#[cfg(test)]
#[allow(unused_imports)]
use crate::rest::RestError;
#[cfg(test)]
#[allow(unused_imports)]
use crate::types::{AuthProfile, MonotonicInstant, Symbol, TxId};

use executors::{
    add_order_exec, amend_order_exec, cancel_all_exec, cancel_batch_exec, cancel_exec,
    deadman_exec, order_batch_exec,
};

/// Typed reject for an explicit `.via(...)` naming a transport with no
/// dispatch entry for this op.
pub(crate) fn unsupported_transport(
    op: crate::dispatch::Op,
    dispatch: &crate::dispatch::dispatch_table::DispatchTable,
    t: Transport,
) -> TradeError {
    use crate::dispatch::dispatch_table::Product;
    const BOTH: &[Transport] = &[Transport::Rest, Transport::WsV2Auth];
    const REST_ONLY: &[Transport] = &[Transport::Rest];
    const WS_ONLY: &[Transport] = &[Transport::WsV2Auth];
    let ok = |tr| dispatch.resolve(op, Product::Spot, tr).is_ok();
    let legal = match (ok(Transport::Rest), ok(Transport::WsV2Auth)) {
        (true, true) => BOTH,
        (true, false) => REST_ONLY,
        (false, true) => WS_ONLY,
        // Every seeded trade op has at least one leg; REST is the floor.
        (false, false) => REST_ONLY,
    };
    TradeError::UnsupportedTransport {
        transport: t,
        legal,
    }
}

/// Error when no `WsSurface` was wired — typed, never a panic.
pub(crate) fn ws_surface_unavailable() -> TradeError {
    TradeError::WsSurfaceUnavailable
}

/// Map a WS-path connection failure into a [`TradeError`]. The SDK never
/// auto-retries an `add_order`; `retryable` tells the caller if re-issuing is safe.
pub(crate) fn ws_conn_error_to_trade(e: ConnectionError) -> TradeError {
    // Full caller→I/O queue → not sent → QueueFull (retryable).
    if e.is_queue_full() {
        return TradeError::QueueFull { request_id: None };
    }
    if e.is_response_timeout() {
        // Deadline timeout is sent-ambiguous → never auto-retry.
        return TradeError::Transport {
            kind: crate::transport::TransportErrorKind::RequestSentNoResponse,
            transient: true,
            request_id: None,
        };
    }
    // Down/not-open WS is retryable; send-ambiguity is handled by the executor.
    TradeError::Transport {
        kind: crate::transport::TransportErrorKind::Other(e.to_string()),
        transient: true,
        request_id: None,
    }
}

/// Caller-side SystemStatus pre-flight gate. Fast-fail only — wire rejection
/// stays authoritative; no auto-retry.
pub(crate) fn gate_system_status(status: &SystemStatus, op: WsOp) -> Result<(), TradeError> {
    let norm = status.status.to_ascii_lowercase().replace('-', "_");
    let online = norm == "online";
    let cancel_or_post = matches!(norm.as_str(), "cancel_only" | "post_only");
    let allowed = match op {
        WsOp::AddOrder | WsOp::AmendOrder | WsOp::BatchAdd => online,
        WsOp::CancelOrder | WsOp::CancelAll | WsOp::CancelAllOrdersAfter => {
            online || cancel_or_post
        }
    };
    if allowed {
        return Ok(());
    }
    Err(TradeError::SystemStatusBlocked {
        current: status.clone(),
        required: SystemStatus {
            status: "online".to_string(),
            timestamp: status.timestamp.clone(),
        },
    })
}

/// True if the SDK must NOT auto-allocate a `cl_ord_id`: Kraken rejects one
/// alongside a `userref` or conditional-close. Caller-supplied ids are never suppressed.
fn suppress_auto_cl_ord_id(userref: &Option<i32>, has_conditional_close: bool) -> bool {
    userref.is_some() || has_conditional_close
}

/// Spot private trading namespace (REST + auth WS v2). Accessed via `client.trade()`.
pub struct TradeNamespace {
    rest: Arc<RestSurface>,
    /// Internal WS facade for the `.via(Transport::WsV2Auth)` branch.
    /// `None` in REST-only construction — returns a typed error, never a panic.
    ws: Option<Arc<crate::api::ws_surface::WsSurface>>,
    /// Shared knob holder handed to each `PendingTrade`.
    knobs: Arc<crate::build::knobs::Knobs>,
    /// Frozen dispatch table handed to each `PendingTrade`.
    dispatch: Arc<crate::dispatch::dispatch_table::DispatchTable>,
}

impl std::fmt::Debug for TradeNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TradeNamespace").finish_non_exhaustive()
    }
}

impl TradeNamespace {
    /// REST-only constructor for unit tests. No WS surface → typed error on WS.
    #[cfg(test)]
    pub(crate) fn new(rest: Arc<RestSurface>) -> Self {
        Self {
            rest,
            ws: None,
            knobs: Arc::new(crate::build::knobs::Knobs::defaults()),
            dispatch: Arc::new(
                crate::dispatch::dispatch_table::DispatchTable::with_default_spot_table(),
            ),
        }
    }

    /// Full constructor — wires the WS facade for the auth-WS order path.
    pub(crate) fn new_with_ws(
        rest: Arc<RestSurface>,
        ws: Arc<crate::api::ws_surface::WsSurface>,
        knobs: Arc<crate::build::knobs::Knobs>,
        dispatch: Arc<crate::dispatch::dispatch_table::DispatchTable>,
    ) -> Self {
        Self {
            rest,
            ws: Some(ws),
            knobs,
            dispatch,
        }
    }

    /// SystemStatus snapshot for the pre-flight gate. v1 returns a permissive
    /// `Online` placeholder; wire rejection stays authoritative.
    fn system_status_snapshot(&self) -> crate::api::SystemStatus {
        crate::api::SystemStatus {
            status: "online".to_string(),
            timestamp: String::new(),
        }
    }

    /// Private constructor for a [`PendingTrade`].
    fn make_pending<Req, Resp>(
        &self,
        req: Req,
        op: Op,
        cl_ord_id: Option<ClOrdId>,
        exec: pending::ExecFn<Req, Resp>,
    ) -> PendingTrade<Req, Resp> {
        PendingTrade::new(
            req,
            Arc::clone(&self.rest),
            self.ws.clone(),
            self.system_status_snapshot(),
            Arc::clone(&self.knobs),
            op,
            Arc::clone(&self.dispatch),
            cl_ord_id,
            exec,
        )
    }

    /// Place an order (`POST /0/private/AddOrder`).
    pub fn order(&self, mut req: OrderRequest) -> PendingTrade<OrderRequest, AddOrderResponse> {
        if req.cl_ord_id.is_none()
            && !suppress_auto_cl_ord_id(&req.userref, req.conditional_close.is_some())
        {
            req.cl_ord_id = Some(ClOrdId::allocate_v4());
        }
        let cl = req.cl_ord_id.clone();
        self.make_pending(req, Op::TradeOrder, cl, Box::new(add_order_exec))
    }

    /// Atomically amend an order in-place (`POST /0/private/AmendOrder`).
    /// Preserves order identity + queue priority (not the deprecated `EditOrder`).
    ///
    /// # Errors
    /// Surfaced on `.await`:
    /// - [`TradeError::EmptyAmendRequest`] — no mutable field was set; nothing was sent.
    /// - [`TradeError::UnknownOrder`] — no open order matches the request's `cl_ord_id`.
    /// - [`TradeError::NoAmendableParameters`] — the amend was a no-op (every new value matched the current, e.g. a sub-tick price move).
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the amend (`EOrder:*` / invalid arguments).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn order_amend(
        &self,
        req: OrderAmendRequest,
    ) -> PendingTrade<OrderAmendRequest, AmendOrderResponse> {
        let cl = Some(req.cl_ord_id.clone());
        self.make_pending(req, Op::TradeOrderAmend, cl, Box::new(amend_order_exec))
    }

    /// Cancel a single order by `cl_ord_id`.
    ///
    /// Hits `POST /0/private/CancelOrder`.
    ///
    /// # Errors
    /// Surfaced on `.await`:
    /// - [`TradeError::UnknownOrder`] — no open order matches `cl_ord_id`.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the cancel; `EOrder:Invalid order` covers both an already-gone order and a garbage id.
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn cancel(&self, cl_ord_id: ClOrdId) -> PendingTrade<CancelRequest, CancelOrderResponse> {
        let req = CancelRequest::new(cl_ord_id.clone());
        self.make_pending(
            req,
            Op::TradeOrderCancel,
            Some(cl_ord_id),
            Box::new(cancel_exec),
        )
    }

    /// Cancel N orders by `cl_ord_id`. Fans out N concurrent single `/CancelOrder`
    /// calls (unbounded by design); each line's result is independent — one failing
    /// never fails the others. See docs/guides/placing-orders.md.
    ///
    /// # Errors
    /// Per-order failures land in the response rows as [`BatchResult::Err`] —
    /// they never fail the batch. Surfaced on `.await`:
    /// - [`TradeError::Unknown`] — internal dispatch-table miss (`kraken_code = "INTERNAL"`).
    /// - [`TradeError::UnsupportedTransport`] — an explicit `.via(...)` named a transport this REST-only op cannot serve.
    pub fn cancel_batch(
        &self,
        cl_ord_ids: Vec<ClOrdId>,
    ) -> PendingTrade<CancelBatchRequest, CancelBatchResponse> {
        let req = CancelBatchRequest::new(cl_ord_ids);
        self.make_pending(
            req,
            Op::TradeOrderCancelBatch,
            None,
            Box::new(cancel_batch_exec),
        )
    }

    /// Cancel ALL of the account's open orders across ALL pairs
    /// (`POST /0/private/CancelAll`). Returns total count cancelled.
    ///
    /// # Errors
    /// Surfaced on `.await`:
    /// - [`TradeError::RateLimited`] — Kraken's throttle family rejected the call; retryable (the SDK's own pre-send charge never blocks a cancel-all).
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the request (`EOrder:*` / invalid arguments).
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn cancel_all(&self) -> PendingTrade<CancelAllRequest, CancelAllResponse> {
        self.make_pending(
            CancelAllRequest,
            Op::TradeOrderCancelAll,
            None,
            Box::new(cancel_all_exec),
        )
    }

    /// Dead-man's-switch — auto-cancel all orders after `timeout` seconds of
    /// inactivity (`POST /0/private/CancelAllOrdersAfter`). Each call resets the
    /// timer; pass `0` to disarm.
    ///
    /// # Errors
    /// Surfaced on `.await`:
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the request (`EOrder:*` / invalid arguments).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn cancel_all_orders_after(
        &self,
        timeout: u32,
    ) -> PendingTrade<DeadmanRequest, DeadmanResponse> {
        let req = DeadmanRequest::new(timeout);
        self.make_pending(
            req,
            Op::TradeOrderCancelAllAfter,
            None,
            Box::new(deadman_exec),
        )
    }

    /// Place 2-15 orders; each row returns its own result (`txid` or `error`) —
    /// a rejected entry never fails placed siblings. WS `batch_add` by default or
    /// REST via `.via(Transport::Rest)`. See docs/guides/placing-orders.md.
    ///
    /// # Errors
    /// A per-row rejection populates that row's `error`, never a whole-batch
    /// failure. Surfaced on `.await`:
    /// - [`TradeError::BatchSizeOutOfRange`] — fewer than 2 or more than 15 entries; nothing was sent.
    /// - [`TradeError::SettlePositionRequiresLeverage`] / [`TradeError::ReduceOnlyRequiresLeverage`] — an entry failed the field-consistency gates; nothing was sent.
    /// - [`TradeError::InvalidOrder`] — pre-send validation failure (un-placeable transport mix, malformed trailing/close leg, `fok` on a non-limit type, `deadline` set) or a whole-batch Kraken rejection (`EOrder:*`).
    /// - [`TradeError::WsUnsupportedOrderField`] / [`TradeError::RestUnsupportedOrderField`] — an entry carries a field the resolved transport cannot express; route with `.via(...)`.
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn order_batch(
        &self,
        mut req: AddOrderBatchRequest,
    ) -> PendingTrade<AddOrderBatchRequest, AddOrderBatchResponse> {
        for e in req.orders.iter_mut() {
            if e.cl_ord_id.is_none()
                && !suppress_auto_cl_ord_id(&e.userref, e.conditional_close.is_some())
            {
                e.cl_ord_id = Some(ClOrdId::allocate_v4());
            }
        }
        self.make_pending(
            req,
            Op::TradeOrderBatch,
            None, // batch has no single cl_ord_id for PendingTrade::cl_ord_id()
            Box::new(order_batch_exec),
        )
    }

    /// Market buy of `volume` of `pair`. Shorthand for `order` with
    /// `OrderType::Market` (`POST /0/private/AddOrder`, `ordertype=market`).
    ///
    /// # Errors
    /// Surfaced on `.await` (the request is SDK-composed, so `order`'s client-side validation variants cannot fire):
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. volume below the pair minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn market_buy(
        &self,
        pair: crate::types::Symbol,
        volume: rust_decimal::Decimal,
    ) -> PendingTrade<OrderRequest, AddOrderResponse> {
        self.order(OrderRequest::new(pair, volume, Side::Buy).order_type(OrderType::Market))
    }

    /// Market sell of `volume` of `pair`. Shorthand for `order` with
    /// `OrderType::Market` (`POST /0/private/AddOrder`, `ordertype=market`).
    ///
    /// # Errors
    /// Surfaced on `.await` (the request is SDK-composed, so `order`'s client-side validation variants cannot fire):
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. volume below the pair minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn market_sell(
        &self,
        pair: crate::types::Symbol,
        volume: rust_decimal::Decimal,
    ) -> PendingTrade<OrderRequest, AddOrderResponse> {
        self.order(OrderRequest::new(pair, volume, Side::Sell).order_type(OrderType::Market))
    }

    /// Limit buy of `volume` of `pair` at `price`. Shorthand for `order` with
    /// `OrderType::Limit` + `price` (`POST /0/private/AddOrder`, `ordertype=limit`).
    ///
    /// # Errors
    /// Surfaced on `.await` (the request is SDK-composed, so `order`'s client-side validation variants cannot fire):
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. a price off the pair's tick or volume below the minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn limit_buy(
        &self,
        pair: crate::types::Symbol,
        volume: rust_decimal::Decimal,
        price: rust_decimal::Decimal,
    ) -> PendingTrade<OrderRequest, AddOrderResponse> {
        let mut req = OrderRequest::new(pair, volume, Side::Buy).order_type(OrderType::Limit);
        req.price = Some(price.into());
        self.order(req)
    }

    /// Limit sell of `volume` of `pair` at `price`. Shorthand for `order` with
    /// `OrderType::Limit` + `price` (`POST /0/private/AddOrder`, `ordertype=limit`).
    ///
    /// # Errors
    /// Surfaced on `.await` (the request is SDK-composed, so `order`'s client-side validation variants cannot fire):
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. a price off the pair's tick or volume below the minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn limit_sell(
        &self,
        pair: crate::types::Symbol,
        volume: rust_decimal::Decimal,
        price: rust_decimal::Decimal,
    ) -> PendingTrade<OrderRequest, AddOrderResponse> {
        let mut req = OrderRequest::new(pair, volume, Side::Sell).order_type(OrderType::Limit);
        req.price = Some(price.into());
        self.order(req)
    }

    /// Stop-loss buy: market buy triggered at `trigger_price`. Shorthand for
    /// `order` with `OrderType::StopLoss`; the wire `price` carries the stop
    /// trigger (`POST /0/private/AddOrder`, `ordertype=stop-loss`).
    ///
    /// # Errors
    /// Surfaced on `.await` (the request is SDK-composed, so `order`'s client-side validation variants cannot fire):
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. a trigger the engine refuses or volume below the minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn stop_loss_buy(
        &self,
        pair: crate::types::Symbol,
        volume: rust_decimal::Decimal,
        trigger_price: rust_decimal::Decimal,
    ) -> PendingTrade<OrderRequest, AddOrderResponse> {
        let mut req = OrderRequest::new(pair, volume, Side::Buy).order_type(OrderType::StopLoss);
        req.price = Some(trigger_price.into());
        self.order(req)
    }

    /// Stop-loss sell: market sell triggered at `trigger_price`. Shorthand for
    /// `order` with `OrderType::StopLoss` (`POST /0/private/AddOrder`,
    /// `ordertype=stop-loss`).
    ///
    /// # Errors
    /// Surfaced on `.await` (the request is SDK-composed, so `order`'s client-side validation variants cannot fire):
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. a trigger the engine refuses or volume below the minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, auth-layer failures carry `AUTH`.
    pub fn stop_loss_sell(
        &self,
        pair: crate::types::Symbol,
        volume: rust_decimal::Decimal,
        trigger_price: rust_decimal::Decimal,
    ) -> PendingTrade<OrderRequest, AddOrderResponse> {
        let mut req = OrderRequest::new(pair, volume, Side::Sell).order_type(OrderType::StopLoss);
        req.price = Some(trigger_price.into());
        self.order(req)
    }
}

#[cfg(test)]
pub(crate) fn __test_decode_ws_add_order(
    resp: crate::conn::managed_connection::WsResponse,
    fallback_cl_ord_id: Option<ClOrdId>,
) -> Result<AddOrderResponse, TradeError> {
    ws_compose::decode_ws_add_order(resp, fallback_cl_ord_id)
}
