//! Event-emission helpers for the trade namespace: order-lifecycle events and
//! the `CancelEmitGuard` drop-guard for `OrderCancellationAttempted`.

use std::sync::{Arc, Weak};

use crate::clock::Clock;
use crate::dispatch::{
    DispatchEventBus, EventEnvelope, EventPayload, EventType, OrderOp, OrderSubmitStatus,
};
use crate::rest::RestError;
use crate::types::{ClOrdId, MonotonicInstant, TxId};

use super::types::AmendId;
use super::ws_compose::wire_error_status;

/// Emit the order-lifecycle event for a REST order-write `Result`, BEFORE the
/// `?`-propagation re-maps the error (borrows, so the executor keeps the
/// original error).
pub(crate) fn emit_rest_submit_outcome(
    rest: &Arc<crate::rest::RestSurface>,
    result: &Result<serde_json::Value, RestError>,
    cl_ord_id: ClOrdId,
    amend_id: Option<AmendId>,
    op: OrderOp,
    sent_at: MonotonicInstant,
    request_id: &str,
) {
    match result {
        Ok(value) => match op {
            // Amend success mints no txid → WireAccepted with the reply's amend_id.
            OrderOp::AmendOrder => {
                let amend_id = amend_id_in_result(value).or(amend_id);
                rest.emit_order_submitted(
                    cl_ord_id,
                    amend_id,
                    op,
                    OrderSubmitStatus::WireAccepted,
                    Some(request_id.to_string()),
                );
            }
            // Cancel success → WireAccepted; never an amend_id.
            OrderOp::CancelOrder => {
                rest.emit_order_submitted(
                    cl_ord_id,
                    None,
                    op,
                    OrderSubmitStatus::WireAccepted,
                    Some(request_id.to_string()),
                );
            }
            _ => {
                // WireSent only with a txid; validate-mode AddOrder is descr-only
                // → no event (no fabricated placeholder).
                if let Some(txid) = first_txid_in_rest_result(value) {
                    rest.emit_order_submitted(
                        cl_ord_id,
                        amend_id,
                        op,
                        OrderSubmitStatus::WireSent { txid },
                        Some(request_id.to_string()),
                    );
                }
            }
        },
        Err(err @ (RestError::Kraken(_) | RestError::UnexpectedShape(_))) => {
            rest.emit_order_submitted(
                cl_ord_id,
                amend_id,
                op,
                wire_error_status(err.clone()),
                Some(request_id.to_string()),
            );
        }
        Err(RestError::Transport { kind, .. }) => {
            if op_emits_ambiguous(op) && kind.is_sent_ambiguous() {
                rest.emit_placement_ambiguous(
                    cl_ord_id,
                    amend_id,
                    op,
                    sent_at,
                    Some(request_id.to_string()),
                );
            }
        }
        // RateLimit / Auth → the request was NOT sent → no event.
        Err(RestError::RateLimit(_)) | Err(RestError::Auth(_)) => {}
    }
}

/// `OrderPlacementAmbiguousEvent` fires on transport-drop ONLY for
/// `AddOrder`/`AmendOrder` — a dropped cancel placed nothing to reconcile.
pub(crate) fn op_emits_ambiguous(op: OrderOp) -> bool {
    matches!(op, OrderOp::AddOrder | OrderOp::AmendOrder)
}

/// `OrderSubmittedEvent` fires on a definitive wire response for
/// `AddOrder`/`AmendOrder`/`CancelOrder` only; `CancelAll`/`CancelAllOrdersAfter`
/// are excluded.
pub(crate) fn op_emits_submitted(op: OrderOp) -> bool {
    matches!(
        op,
        OrderOp::AddOrder | OrderOp::AmendOrder | OrderOp::CancelOrder
    )
}

/// `OrderCancellationAttempted` fires for single-`cl_ord_id` ops only:
/// `AddOrder`/`AmendOrder`/`CancelOrder`.
pub(crate) fn op_emits_cancellation(op: OrderOp) -> bool {
    matches!(
        op,
        OrderOp::AddOrder | OrderOp::AmendOrder | OrderOp::CancelOrder
    )
}

/// Extract a `TxId` from a successful REST order-write result. Only AddOrder
/// carries `{ txid: [..] }`; cancel / amend don't, so `None` ⇒ no WireSent
/// event (no placeholder fabricated).
pub(crate) fn first_txid_in_rest_result(result: &serde_json::Value) -> Option<TxId> {
    result
        .get("txid")
        .and_then(serde_json::Value::as_array)
        .and_then(|a| a.first())
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .map(TxId::new)
}

/// Extract the server-minted `amend_id` from a successful amend reply —
/// `{ amend_id: "TA..." }` on both REST and WS result shapes.
pub(crate) fn amend_id_in_result(result: &serde_json::Value) -> Option<AmendId> {
    result
        .get("amend_id")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|s| AmendId::from(s.to_string()))
}

/// Extract a `TxId` from a successful WS order reply — the add reply's id field
/// is `order_id` (txid-format, NOT `txid`), with a `txid` fallback.
/// Empty = validate-mode → `None` (no event), matching `decode_ws_add_order`.
pub(crate) fn ws_txid_in_result(result: &serde_json::Value) -> Option<TxId> {
    result
        .get("order_id")
        .or_else(|| result.get("txid"))
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .map(|s| TxId::new(s.to_string()))
}

/// Future-local drop guard for `OrderCancellationAttempted`. Armed just before
/// the wire `.await`, disarmed on Ok or Err, so only a caller-cancelled (dropped)
/// future fires the event in `Drop`. Weak bus ⇒ inert if closed; no `.await` in Drop.
pub(crate) struct CancelEmitGuard {
    pub(crate) armed: bool,
    pub(crate) cl_ord_id: ClOrdId,
    /// Add/Amend/CancelOrder only — never CancelAll/CancelAllOrdersAfter.
    pub(crate) op: OrderOp,
    pub(crate) bus: Option<Weak<DispatchEventBus>>,
    pub(crate) clock: Arc<dyn Clock>,
    /// Per-dispatch correlation id, payload-carried on the drop emit.
    pub(crate) request_id: Option<String>,
}

impl CancelEmitGuard {
    /// Build an armed guard for an add/amend/cancel_order executor.
    pub(crate) fn armed(
        rest: &Arc<crate::rest::RestSurface>,
        cl_ord_id: ClOrdId,
        op: OrderOp,
        request_id: Option<String>,
    ) -> Self {
        Self {
            armed: true,
            cl_ord_id,
            op,
            bus: rest.bus_weak(),
            clock: rest.clock_arc(),
            request_id,
        }
    }

    /// Disarm after the wire await resolves (Ok OR Err) so a completed call
    /// does NOT fire the cancellation event.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelEmitGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Some(weak) = self.bus.clone() else {
            return;
        };
        let Some(bus) = weak.upgrade() else {
            return;
        };
        bus.publish(EventEnvelope {
            event_type: EventType::OrderCancellationAttempted,
            event_version: 1,
            timestamp_monotonic: self.clock.now(),
            request_id: None,
            payload: EventPayload::OrderCancellationAttempted {
                cl_ord_id: self.cl_ord_id.clone(),
                op: self.op,
                request_id: self.request_id.clone(),
            },
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Kraken echoes "" for absent ids on some shapes; empty must decode as absent.
    #[test]
    fn id_extractors_treat_empty_as_absent() {
        assert!(amend_id_in_result(&serde_json::json!({ "amend_id": "" })).is_none());
        assert!(amend_id_in_result(&serde_json::json!({ "amend_id": "TA123" })).is_some());
        assert!(first_txid_in_rest_result(&serde_json::json!({ "txid": [""] })).is_none());
        assert!(first_txid_in_rest_result(&serde_json::json!({ "txid": ["OABC"] })).is_some());
        assert!(ws_txid_in_result(&serde_json::json!({ "order_id": "" })).is_none());
    }
}
