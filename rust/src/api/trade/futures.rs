//! `FuturesTradeNamespace` — Unified Futures order entry over the private
//! WebSocket. Accessed via `client.trade().futures()`; every method returns a
//! [`PendingTrade`] that sends on `.await`. Orders are never auto-retried.

use std::future::Future;

use crate::dispatch::dispatch_table::Product;
use crate::dispatch::{Op, WsOp};
use crate::rest::{RateLimitCost, RestError};
use crate::types::{ClOrdId, WsUrl};

#[cfg(doc)]
use super::FuturesOrder;
use super::executors::{
    cancel_all_exec, futures_add_order_exec, futures_amend_order_exec, futures_cancel_batch_exec,
    futures_cancel_exec, futures_order_batch_exec,
};
use super::pending::PendingTrade;
use super::types::{
    AddOrderBatchResponse, AddOrderResponse, AmendOrderResponse, CancelAllRequest,
    CancelAllResponse, CancelBatchRequest, CancelOrderResponse, DeadmanRequest, DeadmanResponse,
    FuturesOrderAmendRequest, FuturesOrderBatchRequest, FuturesOrderRef, FuturesOrderRequest,
};
use super::ws_compose::classify_ws_failure;
use super::ws_conn_error_to_transport;
use super::{TradeCore, TradeError};

/// Futures private trading namespace (Unified WebSocket), via `client.trade().futures()`.
pub struct FuturesTradeNamespace {
    core: TradeCore,
}

impl std::fmt::Debug for FuturesTradeNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuturesTradeNamespace")
            .finish_non_exhaustive()
    }
}

impl FuturesTradeNamespace {
    pub(super) fn new(core: TradeCore) -> Self {
        Self { core }
    }

    /// Place an order; a UUID v4 `cl_ord_id` is allocated when none is set.
    ///
    /// # Errors
    /// Surfaced on `.await`:
    /// - [`TradeError::InvalidOrder`] — the symbol is not a Futures wire symbol, or the order uses a feature the venue rejects (see [`FuturesOrder::validate`]), both before anything is sent; or the venue refuses a value it validates itself, such as a malformed `broker`.
    /// - [`TradeError::RateLimited`] — the venue throttled the request; retryable.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::PermissionDenied`] / [`TradeError::AuthenticationFailed`] — the key lacks a permission, or the connection could not authenticate; nothing was sent.
    /// - [`TradeError::Transport`] / [`TradeError::QueueFull`] / [`TradeError::LoopDead`] / [`TradeError::MalformedResponse`] / [`TradeError::Unknown`] — connection failure, full send queue, dead reactor, undecodable reply.
    /// - [`TradeError::InFlightLimitReached`] — `futures_max_in_flight` placements already await a reply; nothing was sent.
    pub fn order(
        &self,
        mut req: FuturesOrderRequest,
    ) -> PendingTrade<FuturesOrderRequest, AddOrderResponse> {
        let cl_ord_id = req
            .order
            .cl_ord_id
            .get_or_insert_with(ClOrdId::allocate_v4)
            .clone();
        self.core.make_pending(
            Product::Futures,
            req,
            Op::TradeOrder,
            Some(cl_ord_id),
            Box::new(futures_add_order_exec),
        )
    }

    /// Amend an order in place, by `cl_ord_id` or the venue's `order_id`. A quantity of
    /// zero, or `post_only(false)` as the only change, is refused locally. An amend by
    /// `order_id` emits no order lifecycle event.
    ///
    /// # Errors
    /// Surfaced on `.await`:
    /// - [`TradeError::EmptyAmendRequest`] / [`TradeError::InvalidOrder`] — nothing to amend, or a field the venue rejects; nothing was sent.
    /// - [`TradeError::UnknownOrder`] — no open order matches (a filled or cancelled order is gone).
    /// - [`TradeError::NoAmendableParameters`] — the new values equal the current ones.
    /// - [`TradeError::SessionExpired`] — the venue rejected the session token or none was held; nothing is resent. Rebuild the client once the connection has failed.
    /// - [`TradeError::InFlightLimitReached`] — `futures_max_in_flight` placements already await a reply; nothing was sent.
    pub fn order_amend(
        &self,
        req: FuturesOrderAmendRequest,
    ) -> PendingTrade<FuturesOrderAmendRequest, AmendOrderResponse> {
        let cl_ord_id = req.order.cl_ord_id().cloned();
        self.core.make_pending(
            Product::Futures,
            req,
            Op::TradeOrderAmend,
            cl_ord_id,
            Box::new(futures_amend_order_exec),
        )
    }

    /// Cancel one order by [`ClOrdId`] or the venue's order id. A cancel by order id
    /// emits no order lifecycle event.
    ///
    /// # Errors
    /// Surfaced on `.await`: [`TradeError::UnknownOrder`] when no open order matches
    /// (a filled order is gone); otherwise as for [`Self::order`], except that a cancel takes no in-flight
    /// slot and never returns [`TradeError::InFlightLimitReached`].
    pub fn cancel(
        &self,
        order: impl Into<FuturesOrderRef>,
    ) -> PendingTrade<FuturesOrderRef, CancelOrderResponse> {
        let order = order.into();
        let cl_ord_id = order.cl_ord_id().cloned();
        self.core.make_pending(
            Product::Futures,
            order,
            Op::TradeOrderCancel,
            cl_ord_id,
            Box::new(futures_cancel_exec),
        )
    }

    /// Cancel 2 to 50 orders by `cl_ord_id`; the venue refuses `order_id` here. The reply
    /// is the number cancelled; a lower count than ids means some were unknown or gone.
    ///
    /// # Errors
    /// Surfaced on `.await`: [`TradeError::BatchSizeOutOfRange`] for fewer than 2
    /// or more than 50 ids (nothing was sent); otherwise as for [`Self::order`], except that a cancel takes
    /// no in-flight slot and never returns [`TradeError::InFlightLimitReached`].
    pub fn cancel_batch(
        &self,
        cl_ord_ids: Vec<ClOrdId>,
    ) -> PendingTrade<CancelBatchRequest, CancelAllResponse> {
        self.core.make_pending(
            Product::Futures,
            CancelBatchRequest::new(cl_ord_ids),
            Op::TradeOrderCancelBatch,
            None,
            Box::new(futures_cancel_batch_exec),
        )
    }

    /// Cancel every open Futures order. Spot orders are not affected.
    ///
    /// # Errors
    /// Surfaced on `.await` as for [`Self::order`], except that a cancel takes no in-flight slot and
    /// never returns [`TradeError::InFlightLimitReached`].
    pub fn cancel_all(&self) -> PendingTrade<CancelAllRequest, CancelAllResponse> {
        self.core.make_pending(
            Product::Futures,
            CancelAllRequest,
            Op::TradeOrderCancelAll,
            None,
            Box::new(cancel_all_exec),
        )
    }

    /// Dead-man's switch for Futures orders: cancel all after `timeout` seconds (0 to 86400);
    /// each call resets the timer and `0` disarms. Independent of Spot's. `Client::close()`
    /// queues a best-effort disarm ahead of the close frame.
    ///
    /// # Errors
    /// Surfaced on `.await` as for [`Self::order`], except that it takes no in-flight slot and never
    /// returns [`TradeError::InFlightLimitReached`]; arming once `close()` has begun is
    /// [`TradeError::ClientClosed`] and nothing is sent.
    pub fn cancel_all_orders_after(
        &self,
        timeout: u32,
    ) -> PendingTrade<DeadmanRequest, DeadmanResponse> {
        self.core.cancel_all_orders_after(Product::Futures, timeout)
    }

    /// Queues, for `Client::close()`, the disarm of a dead-man's switch this client may have armed: armed from the
    /// arm request until a disarm is confirmed, so a lost arm reply is still disarmed. `None` if nothing is armed,
    /// else a future for the disarm's failure; with no caller slot beyond the close marker's it yields `QueueFull` unsent.
    pub(crate) fn begin_close_disarm(
        &self,
        bus: &crate::dispatch::DispatchEventBus,
    ) -> Option<impl Future<Output = Option<TradeError>> + Send + 'static> {
        if !self.core.take_deadman_armed(Product::Futures) {
            return None;
        }
        let ws = self.core.ws.as_ref()?;
        let handle = if bus.caller_inbound_room_beyond(1) {
            Some(ws.send_request_costed(
                WsUrl::UnifiedPrivate,
                WsOp::CancelAllOrdersAfter.to_string(),
                ws.next_req_id(),
                serde_json::json!({ "timeout": 0 }),
                RateLimitCost::None,
            ))
        } else {
            None
        };
        Some(async move {
            let handle = match handle {
                Some(Ok(handle)) => handle,
                // `RateLimitCost::None` never charges, so this arm is defensive only.
                Some(Err(e)) => return Some(RestError::RateLimit(e).into()),
                None => return Some(TradeError::QueueFull { request_id: None }),
            };
            match handle.recv().await {
                Ok(resp) if !resp.success => Some(classify_ws_failure(
                    WsUrl::UnifiedPrivate,
                    resp.error,
                    resp.retry_after_ms,
                )),
                Err(e) if e.is_definitely_not_sent() => Some(ws_conn_error_to_transport(e)),
                Ok(_) | Err(_) => None,
            }
        })
    }

    /// Place 2 to 15 orders on one symbol in one `batch_add`; each row resolves on its own
    /// (`txid` or `error`) and a rejected row never fails the others.
    ///
    /// # Errors
    /// Surfaced on `.await`: [`TradeError::BatchSizeOutOfRange`] for fewer than 2
    /// or more than 15 rows, [`TradeError::InvalidOrder`] for a bad symbol or row
    /// (nothing was sent), or a whole-batch venue rejection such as a malformed
    /// `broker` on any row; otherwise as for [`Self::order`].
    pub fn order_batch(
        &self,
        mut req: FuturesOrderBatchRequest,
    ) -> PendingTrade<FuturesOrderBatchRequest, AddOrderBatchResponse> {
        for order in &mut req.orders {
            order.cl_ord_id.get_or_insert_with(ClOrdId::allocate_v4);
        }
        self.core.make_pending(
            Product::Futures,
            req,
            Op::TradeOrderBatch,
            None,
            Box::new(futures_order_batch_exec),
        )
    }
}
