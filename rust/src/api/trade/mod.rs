//! `TradeNamespace` — Spot private REST/WS trading, and Futures trading via
//! [`TradeNamespace::futures`]. Every order method returns
//! a [`PendingTrade<Req, Resp>`]; the wire call fires on `.await`. Defaults to WS v2.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering, fence};

use crate::api::SystemStatus;
use crate::dispatch::dispatch_table::Product;
use crate::dispatch::{Op, Transport, WsOp};
use crate::error::ConnectionError;
use crate::rest::RestSurface;
use crate::types::ClOrdId;

mod error;
mod events;
mod executors;
mod futures;
mod pending;
mod snap;
mod types;
mod ws_compose;

#[cfg(test)]
mod tests;

pub use error::{OrderError, TradeError};
pub use futures::FuturesTradeNamespace;
pub use pending::PendingTrade;
pub use types::{
    AddOrderBatchRequest, AddOrderBatchResponse, AddOrderDescr, AddOrderResponse, AmendId,
    AmendOrderResponse, BatchOrderEntry, BatchOrderResult, BatchResult, CancelAllRequest,
    CancelAllResponse, CancelBatchRequest, CancelBatchResponse, CancelOrderResponse, CancelRequest,
    CloseOrderType, ConditionalClose, DeadlineSpec, DeadmanRequest, DeadmanResponse, FuturesOrder,
    FuturesOrderAmendRequest, FuturesOrderBatchRequest, FuturesOrderRef, FuturesOrderRequest,
    OFlag, OrderAmendRequest, OrderRequest, OrderType, Price, PriceUnit, Side, StpType,
    TimeInForce, TimeSpec, TriggerKind,
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

pub(crate) fn unsupported_transport(
    op: crate::dispatch::Op,
    product: Product,
    dispatch: &crate::dispatch::dispatch_table::DispatchTable,
    t: Transport,
) -> TradeError {
    const BY_ROUTES: [&[Transport]; 8] = [
        &[],
        &[Transport::Rest],
        &[Transport::WsV2Auth],
        &[Transport::Rest, Transport::WsV2Auth],
        &[Transport::FuturesWs],
        &[Transport::Rest, Transport::FuturesWs],
        &[Transport::WsV2Auth, Transport::FuturesWs],
        &[Transport::Rest, Transport::WsV2Auth, Transport::FuturesWs],
    ];
    let routed = |tr| usize::from(dispatch.resolve(op, product, tr).is_ok());
    let routes = routed(Transport::Rest)
        | (routed(Transport::WsV2Auth) << 1)
        | (routed(Transport::FuturesWs) << 2);
    let legal = match (routes, product) {
        (0, Product::Spot) => BY_ROUTES[1],
        (0, Product::Futures) => BY_ROUTES[4],
        (routes, _) => BY_ROUTES[routes],
    };
    TradeError::UnsupportedTransport {
        transport: t,
        legal,
    }
}

pub(crate) fn ws_surface_unavailable() -> TradeError {
    TradeError::WsSurfaceUnavailable
}

/// Maps a WS connection failure to a [`TradeError`]; an `add_order` is never auto-retried.
pub(crate) fn ws_conn_error_to_trade(e: ConnectionError) -> TradeError {
    if e.is_session_token_unavailable() {
        return TradeError::SessionExpired { request_id: None };
    }
    ws_conn_error_to_transport(e)
}

/// [`ws_conn_error_to_trade`] without the session-token case; the close-time dead-man
/// disarm uses it so an unsendable disarm still reports as `Transport`, not `SessionExpired`.
pub(crate) fn ws_conn_error_to_transport(e: ConnectionError) -> TradeError {
    if e.is_queue_full() {
        return TradeError::QueueFull { request_id: None };
    }
    if e.is_permission_denied() {
        return TradeError::PermissionDenied { request_id: None };
    }
    if let Some(cause) = e.authentication_failure() {
        return TradeError::AuthenticationFailed {
            kraken_message: cause.to_owned(),
            request_id: None,
        };
    }
    if e.is_response_timeout() {
        // Sent-ambiguous: never auto-retried.
        return TradeError::Transport {
            kind: crate::transport::TransportErrorKind::RequestSentNoResponse,
            transient: true,
            request_id: None,
        };
    }
    TradeError::Transport {
        kind: crate::transport::TransportErrorKind::Other(e.to_string()),
        transient: true,
        request_id: None,
    }
}

/// Maps a WS error that arrived after the frame was written. A dropped placement, amend or
/// batch may have reached the venue, so it is sent-ambiguous and not retryable; every other
/// op, and any error that proves nothing was sent, keeps [`ws_conn_error_to_trade`].
pub(crate) fn ws_sent_error_to_trade(
    op: crate::dispatch::OrderOp,
    e: ConnectionError,
) -> TradeError {
    if !crate::dispatch::WsOp::from(op).is_placement()
        || e.is_definitely_not_sent()
        || e.is_response_timeout()
    {
        return ws_conn_error_to_trade(e);
    }
    TradeError::Transport {
        kind: crate::transport::TransportErrorKind::RequestSentNoResponse,
        transient: true,
        request_id: None,
    }
}

/// Fast-fail pre-flight gate; the wire rejection stays authoritative.
pub(crate) fn gate_system_status(status: &SystemStatus, op: WsOp) -> Result<(), TradeError> {
    let norm = status.status.to_ascii_lowercase().replace('-', "_");
    let online = norm == "online";
    let cancel_or_post = matches!(norm.as_str(), "cancel_only" | "post_only");
    let allowed = match op {
        WsOp::AddOrder | WsOp::AmendOrder | WsOp::BatchAdd => online,
        WsOp::CancelOrder | WsOp::BatchCancel | WsOp::CancelAll | WsOp::CancelAllOrdersAfter => {
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

/// Wiring shared by the Spot and Futures trade namespaces.
#[derive(Clone)]
struct TradeCore {
    rest: Arc<RestSurface>,
    /// `None` in REST-only construction; WS methods then return a typed error.
    ws: Option<Arc<crate::api::ws_surface::WsSurface>>,
    knobs: Arc<crate::build::knobs::Knobs>,
    dispatch: Arc<crate::dispatch::dispatch_table::DispatchTable>,
    /// Whether this client has armed a Futures dead-man's switch that `close()` must disarm.
    futures_deadman_armed: Arc<DeadmanFlag>,
    spot_deadman_armed: Arc<DeadmanFlag>,
    futures_in_flight: Option<Arc<tokio::sync::Semaphore>>,
}

#[derive(Default)]
struct DeadmanFlag(AtomicU64);

impl DeadmanFlag {
    const fn armed_next(s: u64) -> u64 {
        (((s >> 1) + 1) << 1) | 1
    }

    fn cas_update(&self, f: impl Fn(u64) -> Option<u64>) -> Result<u64, u64> {
        let mut cur = self.0.load(Ordering::SeqCst);
        loop {
            let Some(next) = f(cur) else {
                return Err(cur);
            };
            match self
                .0
                .compare_exchange_weak(cur, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(prev) => return Ok(prev),
                Err(seen) => cur = seen,
            }
        }
    }

    fn arm(&self) -> u64 {
        let (Ok(prev) | Err(prev)) = self.cas_update(|s| Some(Self::armed_next(s)));
        Self::armed_next(prev)
    }

    fn seen(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }

    fn clear_if_unchanged(&self, seen: u64) {
        self.0
            .compare_exchange(seen, seen & !1, Ordering::AcqRel, Ordering::Acquire)
            .ok();
    }

    fn rearm_if_latest(&self, mine: u64) {
        self.cas_update(|s| (s >> 1 == mine >> 1).then_some(s | 1))
            .ok();
    }

    fn take(&self) -> bool {
        fence(Ordering::SeqCst);
        self.0.fetch_and(!1, Ordering::SeqCst) & 1 != 0
    }
}

fn in_flight_slots(knobs: &crate::build::knobs::Knobs) -> Option<Arc<tokio::sync::Semaphore>> {
    knobs
        .futures_max_in_flight
        .filter(|&n| n > 0)
        .map(|n| Arc::new(tokio::sync::Semaphore::new(n as usize)))
}

impl TradeCore {
    /// Permissive `Online` placeholder; the wire rejection stays authoritative.
    fn system_status_snapshot(&self) -> crate::api::SystemStatus {
        crate::api::SystemStatus {
            status: "online".to_string(),
            timestamp: String::new(),
        }
    }

    fn make_pending<Req, Resp>(
        &self,
        product: Product,
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
            self.futures_in_flight.clone(),
            op,
            product,
            Arc::clone(&self.dispatch),
            cl_ord_id,
            exec,
        )
    }

    fn deadman_armed(&self, product: Product) -> &Arc<DeadmanFlag> {
        match product {
            Product::Spot => &self.spot_deadman_armed,
            Product::Futures => &self.futures_deadman_armed,
        }
    }

    fn cancel_all_orders_after(
        &self,
        product: Product,
        timeout: u32,
    ) -> PendingTrade<DeadmanRequest, DeadmanResponse> {
        let armed = Arc::clone(self.deadman_armed(product));
        self.make_pending(
            product,
            DeadmanRequest::new(timeout),
            Op::TradeOrderCancelAllAfter,
            None,
            Box::new(move |req, rest, transport, ctx| {
                let arming = req.timeout_secs > 0;
                let seen = if arming { armed.arm() } else { armed.seen() };
                let closing = arming && {
                    fence(Ordering::SeqCst);
                    ctx.ws.as_ref().is_some_and(|ws| ws.is_shutting_down())
                };
                let run = deadman_exec(req, rest, transport, ctx);
                Box::pin(async move {
                    if closing {
                        return Err(TradeError::ClientClosed);
                    }
                    let response = run.await?;
                    if response.trigger_time.is_none() {
                        armed.clear_if_unchanged(seen);
                    } else if arming {
                        armed.rearm_if_latest(seen);
                    }
                    Ok(response)
                })
            }),
        )
    }

    fn take_deadman_armed(&self, product: Product) -> bool {
        self.deadman_armed(product).take()
    }
}

/// Spot private trading namespace (REST + auth WS v2); Futures orders via [`Self::futures`].
/// Accessed via `client.trade()`.
pub struct TradeNamespace {
    core: TradeCore,
    futures: FuturesTradeNamespace,
}

impl std::fmt::Debug for TradeNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TradeNamespace").finish_non_exhaustive()
    }
}

impl TradeNamespace {
    fn with_core(core: TradeCore) -> Self {
        Self {
            futures: FuturesTradeNamespace::new(core.clone()),
            core,
        }
    }

    #[cfg(test)]
    pub(crate) fn new(rest: Arc<RestSurface>) -> Self {
        Self::with_core(TradeCore {
            rest,
            ws: None,
            knobs: Arc::new(crate::build::knobs::Knobs::defaults()),
            dispatch: Arc::new(
                crate::dispatch::dispatch_table::DispatchTable::with_default_table(),
            ),
            futures_deadman_armed: Arc::default(),
            spot_deadman_armed: Arc::default(),
            futures_in_flight: None,
        })
    }

    pub(crate) fn new_with_ws(
        rest: Arc<RestSurface>,
        ws: Arc<crate::api::ws_surface::WsSurface>,
        knobs: Arc<crate::build::knobs::Knobs>,
        dispatch: Arc<crate::dispatch::dispatch_table::DispatchTable>,
    ) -> Self {
        let futures_in_flight = in_flight_slots(&knobs);
        Self::with_core(TradeCore {
            rest,
            ws: Some(ws),
            knobs,
            dispatch,
            futures_deadman_armed: Arc::default(),
            spot_deadman_armed: Arc::default(),
            futures_in_flight,
        })
    }

    /// The Futures trade namespace.
    pub fn futures(&self) -> &FuturesTradeNamespace {
        &self.futures
    }

    fn make_pending<Req, Resp>(
        &self,
        req: Req,
        op: Op,
        cl_ord_id: Option<ClOrdId>,
        exec: pending::ExecFn<Req, Resp>,
    ) -> PendingTrade<Req, Resp> {
        self.core
            .make_pending(Product::Spot, req, op, cl_ord_id, exec)
    }

    /// Place a Spot order.
    pub fn order(&self, mut req: OrderRequest) -> PendingTrade<OrderRequest, AddOrderResponse> {
        if req.cl_ord_id.is_none()
            && !suppress_auto_cl_ord_id(&req.userref, req.conditional_close.is_some())
        {
            req.cl_ord_id = Some(ClOrdId::allocate_v4());
        }
        let cl = req.cl_ord_id.clone();
        self.make_pending(req, Op::TradeOrder, cl, Box::new(add_order_exec))
    }

    /// Atomically amend a Spot order in place by `cl_ord_id`; identity and queue priority are kept.
    ///
    /// Spot sends the `amend_order` WebSocket request by default, or `POST /0/private/AmendOrder` over REST.
    ///
    /// # Errors
    /// Surfaced on `.await`:
    /// - [`TradeError::EmptyAmendRequest`] — no mutable field was set; nothing was sent.
    /// - [`TradeError::UnknownOrder`] — no open order matches the request's `cl_ord_id`.
    /// - [`TradeError::NoAmendableParameters`] — the amend was a no-op (every new value matched the current, e.g. a sub-tick price move).
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the amend (`EOrder:*` / invalid arguments).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
    pub fn order_amend(
        &self,
        req: OrderAmendRequest,
    ) -> PendingTrade<OrderAmendRequest, AmendOrderResponse> {
        let cl = Some(req.cl_ord_id.clone());
        self.make_pending(req, Op::TradeOrderAmend, cl, Box::new(amend_order_exec))
    }

    /// Cancel a single order by `cl_ord_id`.
    ///
    /// Spot sends the `cancel_order` WebSocket request by default, or `POST /0/private/CancelOrder` over REST.
    ///
    /// # Errors
    /// Surfaced on `.await`:
    /// - [`TradeError::UnknownOrder`] — no open order matches `cl_ord_id`.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the cancel; `EOrder:Invalid order` covers both an already-gone order and a garbage id.
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
    pub fn cancel(&self, cl_ord_id: ClOrdId) -> PendingTrade<CancelRequest, CancelOrderResponse> {
        let req = CancelRequest::new(cl_ord_id.clone());
        self.make_pending(
            req,
            Op::TradeOrderCancel,
            Some(cl_ord_id),
            Box::new(cancel_exec),
        )
    }

    /// Cancel N orders by `cl_ord_id` as N concurrent single `/CancelOrder` calls; each
    /// row's result is independent and one failure never fails the others.
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
    /// (the `cancel_all` WebSocket request by default, or `POST /0/private/CancelAll` over
    /// REST). Returns total count cancelled.
    ///
    /// # Errors
    /// Surfaced on `.await`:
    /// - [`TradeError::RateLimited`] — Kraken's throttle family rejected the call; retryable (the SDK's own pre-send charge never blocks a cancel-all).
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the request (`EOrder:*` / invalid arguments).
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
    pub fn cancel_all(&self) -> PendingTrade<CancelAllRequest, CancelAllResponse> {
        self.make_pending(
            CancelAllRequest,
            Op::TradeOrderCancelAll,
            None,
            Box::new(cancel_all_exec),
        )
    }

    /// Dead-man's-switch — auto-cancel all orders after `timeout` seconds of
    /// inactivity (the `cancel_all_orders_after` WebSocket request by default, or
    /// `POST /0/private/CancelAllOrdersAfter` over REST). Each call resets the
    /// timer; pass `0` to disarm.
    ///
    /// The timer covers every Spot order on the account. `Client::close()` disarms it only if
    /// this client requested an arm no later confirmed disarm cleared; the timer is
    /// account-wide, so that disarm also clears a switch another client set.
    ///
    /// # Errors
    /// Surfaced on `.await`:
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the request (`EOrder:*` / invalid arguments).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family rejected the call; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
    /// - [`TradeError::ClientClosed`] — arming once `close()` has begun; nothing was sent.
    pub fn cancel_all_orders_after(
        &self,
        timeout: u32,
    ) -> PendingTrade<DeadmanRequest, DeadmanResponse> {
        self.core.cancel_all_orders_after(Product::Spot, timeout)
    }

    pub(crate) fn begin_close_disarm(
        &self,
    ) -> Option<impl Future<Output = Option<TradeError>> + Send + 'static> {
        if !self.core.take_deadman_armed(Product::Spot) {
            return None;
        }
        let disarm = self.cancel_all_orders_after(0).via(Transport::Rest);
        Some(async move { disarm.await.err() })
    }

    /// Place 2-15 orders; each row returns its own result (`txid` or `error`) and a
    /// rejected row never fails the others.
    ///
    /// # Errors
    /// Per-row rejections populate that row's `error`. Surfaced on `.await`:
    /// - [`TradeError::BatchSizeOutOfRange`] — fewer than 2 or more than 15 entries; nothing was sent.
    /// - [`TradeError::SettlePositionRequiresLeverage`] / [`TradeError::ReduceOnlyRequiresLeverage`] — an entry failed the field-consistency gates; nothing was sent.
    /// - [`TradeError::InvalidOrder`] — pre-send validation failure (un-placeable transport mix, malformed trailing/close leg, `fok` on a non-limit type, `deadline` set) or a whole-batch Kraken rejection (`EOrder:*`).
    /// - [`TradeError::WsUnsupportedOrderField`] / [`TradeError::RestUnsupportedOrderField`] — an entry carries a field the resolved transport cannot express; route with `.via(...)`.
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
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
    /// Surfaced on `.await`:
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. volume below the pair minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
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
    /// Surfaced on `.await`:
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. volume below the pair minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
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
    /// Surfaced on `.await`:
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. a price off the pair's tick or volume below the minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
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
    /// Surfaced on `.await`:
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. a price off the pair's tick or volume below the minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
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
    /// Surfaced on `.await`:
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. a trigger the engine refuses or volume below the minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
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
    /// Surfaced on `.await`:
    /// - [`TradeError::InsufficientFunds`] — the account balance cannot cover the order.
    /// - [`TradeError::InvalidOrder`] — Kraken rejected the order (`EOrder:*`, e.g. a trigger the engine refuses or volume below the minimum).
    /// - [`TradeError::RateLimited`] — Kraken's throttle family or the SDK's pre-send trading tracker rejected the call; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply; unmapped Kraken strings degrade to `Unknown`, a local signing failure carries `AUTH`.
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
