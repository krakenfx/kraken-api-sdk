//! `PendingTrade<Req, Resp>` — deferred-execution handle returned by every
//! `client.trade()` order method. The wire call fires only when the caller
//! `.await`s the handle (via [`IntoFuture`]); chain methods refine the call first.

use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use crate::api::SystemStatus;
use crate::api::ws_surface::WsSurface;
use crate::dispatch::dispatch_table::Product;
use crate::dispatch::{Op, Transport};
use crate::rest::RestSurface;
use crate::types::ClOrdId;

use super::error::TradeError;

/// Boxed future produced by a per-op executor closure.
pub(super) type ExecFut<Resp> = Pin<Box<dyn Future<Output = Result<Resp, TradeError>> + Send>>;

/// WS-branch context an executor needs when the resolved transport is `WsV2Auth`.
pub(super) struct WsOrderCtx {
    /// The internal WS facade (`send_request` / `next_req_id`). `None` when no
    /// WS surface was wired at construction.
    pub ws: Option<Arc<WsSurface>>,
    /// SystemStatus snapshot at method-call time for the caller-side pre-flight gate.
    pub system_status: SystemStatus,
    /// The dispatch op for this order; the REST arm resolves it to the endpoint.
    pub op: Op,
    /// Frozen dispatch table — resolves the REST endpoint + auth for this op.
    pub dispatch: Arc<crate::dispatch::dispatch_table::DispatchTable>,
    /// Per-request WS order-response deadline from the
    /// `ws_order_response_deadline_ms` knob; `None` = unbounded.
    pub order_deadline: Option<std::time::Duration>,
}

/// Per-op executor: consumes the request + owned `Arc<RestSurface>` + resolved
/// [`Transport`] + [`WsOrderCtx`], returns the boxed future.
pub(super) type ExecFn<Req, Resp> =
    Box<dyn FnOnce(Req, Arc<RestSurface>, Transport, WsOrderCtx) -> ExecFut<Resp> + Send>;

/// Deferred-execution handle for a single `client.trade()` order op. The
/// wire call fires when the caller `.await`s (via [`IntoFuture`]); chain methods
/// refine the call first.
#[must_use = "a PendingTrade does nothing unless you `.await` it (optionally after `.via(...)`)"]
pub struct PendingTrade<Req, Resp> {
    req: Req,
    transport_override: Option<Transport>,
    knobs: Arc<crate::build::knobs::Knobs>,
    rest: Arc<RestSurface>,
    ws: Option<Arc<WsSurface>>,
    system_status: SystemStatus,
    /// Snapshot of the session-wide `prefer_rest_for_orders` knob at method-call
    /// time. `false` (default) resolves orders to `WsV2Auth`; `true` forces REST.
    /// A caller overrides per-call via `.via(...)`.
    prefer_rest_for_orders: bool,
    op: Op,
    dispatch: Arc<crate::dispatch::dispatch_table::DispatchTable>,
    exec: ExecFn<Req, Resp>,
    /// `Some` for `order_buy`/`order_sell` (synchronously allocated), the input
    /// id for `cancel`/`order_amend`, `None` for `cancel_all`/deadman.
    cl_ord_id: Option<ClOrdId>,
    _resp: PhantomData<Resp>,
}

impl<Req, Resp> PendingTrade<Req, Resp> {
    /// Internal constructor — used only by [`super::TradeNamespace`].
    #[allow(clippy::too_many_arguments)] // internal constructor; args mirror the
    // per-op call context (req + 3 shared Arcs + status + knobs + op + id + exec).
    pub(super) fn new(
        req: Req,
        rest: Arc<RestSurface>,
        ws: Option<Arc<WsSurface>>,
        system_status: SystemStatus,
        knobs: Arc<crate::build::knobs::Knobs>,
        op: Op,
        dispatch: Arc<crate::dispatch::dispatch_table::DispatchTable>,
        cl_ord_id: Option<ClOrdId>,
        exec: ExecFn<Req, Resp>,
    ) -> Self {
        Self {
            req,
            transport_override: None,
            // Read the Copy bool before `knobs` is moved into the struct below.
            prefer_rest_for_orders: knobs.prefer_rest_for_orders,
            knobs,
            rest,
            ws,
            system_status,
            op,
            dispatch,
            exec,
            cl_ord_id,
            _resp: PhantomData,
        }
    }

    /// Force the wire transport for THIS call only (overrides the
    /// `prefer_rest_for_orders` knob). Consumes `self`. Selecting a transport
    /// the op has no route for rejects at `.await` with
    /// [`TradeError::UnsupportedTransport`], before any wire work.
    pub fn via(mut self, t: Transport) -> Self {
        self.transport_override = Some(t);
        self
    }

    /// Read the client order id this call will send, BEFORE `.await` — lets a
    /// caller durably record it for crash-safe reconciliation. `Some` for
    /// `order_buy`/`order_sell`/`cancel`/`order_amend`, `None` for `cancel_all`/deadman.
    pub fn cl_ord_id(&self) -> Option<&ClOrdId> {
        self.cl_ord_id.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn request(&self) -> &Req {
        &self.req
    }
}

impl<Req, Resp> std::fmt::Debug for PendingTrade<Req, Resp> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingTrade")
            .field("cl_ord_id", &self.cl_ord_id)
            .field("transport_override", &self.transport_override)
            .finish_non_exhaustive()
    }
}

impl<Req: Send + 'static, Resp: Send + 'static> IntoFuture for PendingTrade<Req, Resp> {
    type Output = Result<Resp, TradeError>;
    type IntoFuture = ExecFut<Resp>;

    fn into_future(self) -> Self::IntoFuture {
        // Transport precedence: transport_override, else prefer_rest_for_orders ?
        // Rest : WsV2Auth. Default knob (false) -> WsV2Auth; .via(...) overrides per-call.
        let transport = self.transport_override.unwrap_or({
            if self.prefer_rest_for_orders {
                Transport::Rest
            } else {
                Transport::WsV2Auth
            }
        });
        tracing::trace!(
            target: "kraken_sdk::trade",
            ?transport,
            "PendingTrade resolved transport"
        );
        // An explicit `.via(...)` with no dispatch entry for this op rejects
        // pre-wire. The knob default is a preference, not a selection — it
        // never rejects; a single-transport op serves its only leg.
        if let Some(explicit) = self.transport_override {
            if self
                .dispatch
                .resolve(self.op, Product::Spot, explicit)
                .is_err()
            {
                let err = super::unsupported_transport(self.op, &self.dispatch, explicit);
                return Box::pin(async move { Err(err) });
            }
        }
        let ctx = WsOrderCtx {
            ws: self.ws,
            system_status: self.system_status,
            op: self.op,
            dispatch: self.dispatch,
            // Treat 0 as unset (no instant-timeout footgun).
            order_deadline: self
                .knobs
                .ws_order_response_deadline_ms
                .filter(|&ms| ms > 0)
                .map(|ms| std::time::Duration::from_millis(u64::from(ms))),
        };
        (self.exec)(self.req, self.rest, transport, ctx)
    }
}
