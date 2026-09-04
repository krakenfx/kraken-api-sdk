// PendingRequestMap: reactor-single-writer. Drained on every exit from Open
// BEFORE ConnectionDroppedEvent — drain-before-emit.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use crate::dispatch::event_bus::{DispatchEventBus, WsFailReason, WsOp};
use crate::error::ConnectionError;
use crate::types::{ChannelName, MonotonicInstant, Symbol};

/// Op-agnostic decoded shape of an inbound `req_id`-bearing WS response frame.
/// On failure `error` is a single wire string (not an array).
#[derive(Debug, Clone)]
pub struct WsResponse {
    /// Echoed `req_id` (correlation key).
    pub req_id: u64,
    pub success: bool,
    /// Wire `result` object. `Null` when absent.
    pub result: serde_json::Value,
    /// Top-level `error` STRING on `success:false` (single string, not an array).
    pub error: Option<String>,
}

/// One in-flight WS request awaiting its `req_id`-matched response.
pub(crate) struct PendingRequest {
    /// Correlation key — redundant with the map key, kept for the drain event.
    pub req_id: u64,
    /// Op discriminator — carried onto `WsRequestFailedEvent` on drain.
    pub op: WsOp,
    /// Monotonic instant the request was recorded.
    #[allow(dead_code)] // observability hook (CallbackLatency-adjacent); not read yet
    pub sent_at: MonotonicInstant,
    pub tx: oneshot::Sender<Result<WsResponse, ConnectionError>>,
}

/// What caused a [`PendingRequestMap::drain`]. Maps to a
/// `(ConnectionError, WsFailReason)` pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainCause {
    /// An `Open` connection dropped with the request in flight
    /// → `(RequestInFlightWhenDropped, ConnectionLost)`.
    RequestInFlightWhenDropped,
    /// The client closed the connection (`CallClose`) →
    /// `(ClientClosed, ClientClosed)`.
    ClientClosed,
}

impl DrainCause {
    fn resolve(self) -> (ConnectionError, WsFailReason) {
        match self {
            DrainCause::RequestInFlightWhenDropped => (
                ConnectionError::request_in_flight_when_dropped(),
                WsFailReason::ConnectionLost,
            ),
            DrainCause::ClientClosed => {
                (ConnectionError::client_closed(), WsFailReason::ClientClosed)
            }
        }
    }
}

/// Maps `req_id` → pending caller correlation; reactor single-writer. Drained
/// before `ConnectionDroppedEvent` on every exit from `Open`.
#[derive(Default)]
pub(crate) struct PendingRequestMap {
    entries: HashMap<u64, PendingRequest>,
}

impl PendingRequestMap {
    pub(crate) fn record(&mut self, pending: PendingRequest) {
        self.entries.insert(pending.req_id, pending);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Resolve the pending request for `resp.req_id` with `Ok(resp)`. No-op if no
    /// entry. A dropped receiver is ignored per the async-cancellation contract.
    pub(crate) fn resolve(&mut self, resp: WsResponse) {
        if let Some(p) = self.entries.remove(&resp.req_id) {
            let _ = p.tx.send(Ok(resp));
        }
    }

    /// Drain EVERY pending request, resolving each future THEN emitting a
    /// `WsRequestFailedEvent` per pending (future before bus event).
    pub(crate) fn drain(&mut self, cause: DrainCause, bus: &Arc<DispatchEventBus>) {
        if self.entries.is_empty() {
            return;
        }
        let (conn_err, reason) = cause.resolve();
        // Deterministic drain order by req_id (map iteration order is unspecified).
        let mut pending: Vec<PendingRequest> = self.entries.drain().map(|(_k, v)| v).collect();
        pending.sort_by_key(|p| p.req_id);
        for p in pending {
            let req_id = p.req_id;
            let op = p.op;
            let _ = p.tx.send(Err(conn_err.clone()));
            bus.publish(crate::dispatch::EventEnvelope {
                event_type: crate::dispatch::EventType::WsRequestFailedEvent,
                event_version: 1,
                timestamp_monotonic: bus.clock().now(),
                request_id: None, // not awaited — req_id is in the payload
                payload: crate::dispatch::EventPayload::WsRequestFailedEvent { req_id, op, reason },
            });
        }
    }

    /// Guards the dual-leg order-auth demux against a double-resolve: on
    /// auth-failure the FSM drain already resolved the order, so Leg A must skip it.
    pub(crate) fn contains(&self, req_id: u64) -> bool {
        self.entries.contains_key(&req_id)
    }

    /// Fail a single in-flight request. Per-entry; no `WsRequestFailedEvent`.
    pub(crate) fn fail_one(&mut self, req_id: u64, err: ConnectionError) {
        if let Some(p) = self.entries.remove(&req_id) {
            let _ = p.tx.send(Err(err));
        }
    }

    /// Drain a single order's pending entry with a retryable error and emit its
    /// `WsRequestFailedEvent`. Orders are never auto-resent.
    pub(crate) fn drain_one_retryable(&mut self, req_id: u64, bus: &Arc<DispatchEventBus>) {
        let Some(p) = self.entries.remove(&req_id) else {
            return;
        };
        let op = p.op;
        // Resolve the future FIRST (matching `drain`'s order), then emit.
        let _ =
            p.tx.send(Err(ConnectionError::request_in_flight_when_dropped()));
        bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::WsRequestFailedEvent,
            event_version: 1,
            timestamp_monotonic: bus.clock().now(),
            request_id: None, // not awaited — req_id is in the payload
            payload: crate::dispatch::EventPayload::WsRequestFailedEvent {
                req_id,
                op,
                reason: WsFailReason::ConnectionLost,
            },
        });
    }

    /// Number of currently-recorded pending requests.
    #[cfg(test)]
    pub(crate) fn pending_request_count(&self) -> usize {
        self.entries.len()
    }
}

/// The awaitable `WsSurface::send_request` returns. A dropped sender maps to
/// `ConnectionError::loop_closed()`.
pub(crate) struct RequestHandle {
    rx: oneshot::Receiver<Result<WsResponse, ConnectionError>>,
}

impl RequestHandle {
    pub(crate) fn new(rx: oneshot::Receiver<Result<WsResponse, ConnectionError>>) -> Self {
        Self { rx }
    }

    /// Await the reactor's resolution. A `RecvError` maps to
    /// `ConnectionError::loop_closed()`.
    pub(crate) async fn recv(self) -> Result<WsResponse, ConnectionError> {
        match self.rx.await {
            Ok(inner) => inner,
            Err(_recv_err) => Err(ConnectionError::loop_closed()),
        }
    }
}

/// Inbound-activity watchdog for one `ManagedConnection`. Armed on entering
/// `Open`, disarmed on exit. `WebsocketStaleEvent` is emitted by the FSM teardown, not here.
pub(crate) struct StalenessMonitor {
    /// Duration window in ms (clamped ≤55_000 at `.build()`).
    window_ms: u32,
    last_inbound_at: Option<MonotonicInstant>,
    armed: bool,
}

impl StalenessMonitor {
    pub(crate) fn new(window_ms: u32) -> Self {
        Self {
            window_ms,
            last_inbound_at: None,
            armed: false,
        }
    }

    pub(crate) fn arm(&mut self, now: MonotonicInstant) {
        self.armed = true;
        self.last_inbound_at = Some(now);
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
        self.last_inbound_at = None;
    }

    pub(crate) fn note_inbound(&mut self, at: MonotonicInstant) {
        if self.armed {
            self.last_inbound_at = Some(at);
        }
    }

    /// Last inbound-frame instant while armed. Read BEFORE disarm.
    pub(crate) fn last_inbound_at(&self) -> Option<MonotonicInstant> {
        self.last_inbound_at
    }

    pub(crate) fn window_ms(&self) -> u32 {
        self.window_ms
    }

    /// Derived deadline. `None` when disarmed.
    pub(crate) fn deadline(&self) -> Option<MonotonicInstant> {
        if self.armed {
            self.last_inbound_at
                .map(|last| MonotonicInstant(last.0 + Duration::from_millis(self.window_ms as u64)))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod staleness_monitor_tests {
    use super::*;

    fn at(ms: u64) -> MonotonicInstant {
        MonotonicInstant(Duration::from_millis(ms))
    }

    #[test]
    fn unarmed_has_no_deadline_and_never_expires() {
        let m = StalenessMonitor::new(30_000);
        assert_eq!(m.deadline(), None);
    }

    #[test]
    fn arm_sets_deadline_at_now_plus_window() {
        let mut m = StalenessMonitor::new(30_000);
        m.arm(at(1_000));
        assert_eq!(m.deadline(), Some(at(31_000)));
    }

    #[test]
    fn note_inbound_pushes_deadline_out() {
        let mut m = StalenessMonitor::new(30_000);
        m.arm(at(1_000));
        assert_eq!(m.deadline(), Some(at(31_000)));
        m.note_inbound(at(11_000));
        assert_eq!(m.deadline(), Some(at(41_000)));
    }

    #[test]
    fn note_inbound_is_noop_when_disarmed() {
        let mut m = StalenessMonitor::new(30_000);
        m.note_inbound(at(5_000));
        assert_eq!(m.deadline(), None);
    }

    #[test]
    fn disarm_clears_deadline_and_expiry() {
        let mut m = StalenessMonitor::new(30_000);
        m.arm(at(1_000));
        m.disarm();
        assert_eq!(m.deadline(), None);
    }
}

/// Per-MC timer wheel; each field is `None` when not armed.
#[derive(Default)]
pub(crate) struct TimerSet {
    pub backoff_due_at: Option<crate::types::MonotonicInstant>,
    pub upgrade_timeout_due_at: Option<crate::types::MonotonicInstant>,
    pub staleness_due_at: Option<crate::types::MonotonicInstant>,
    pub close_timeout_due_at: Option<crate::types::MonotonicInstant>,
    pub rate_budget_window_advanced_at: Option<crate::types::MonotonicInstant>,
    pub per_entry_subscribe_ack_timeouts:
        std::collections::HashMap<(ChannelName, Option<Symbol>), crate::types::MonotonicInstant>,
    /// Per-entry subscribe resend timers. Armed on `Backpressure` send failure.
    /// Single-shot.
    pub per_entry_subscribe_resend_due:
        std::collections::HashMap<(ChannelName, Option<Symbol>), crate::types::MonotonicInstant>,
    /// Per-`(Book, symbol)` reseed snapshot-liveness timers. Degrades to
    /// PassThrough after `MAX_CONSECUTIVE_BOOK_GAPS`. Single-shot.
    pub per_entry_book_reseed_snapshot:
        std::collections::HashMap<(ChannelName, Option<Symbol>), crate::types::MonotonicInstant>,
    /// Proactive WS-token refresh tick (auth MC only). Kept armed through
    /// `BackingOff` so the warm cache survives reconnect.
    pub token_refresh_due_at: Option<crate::types::MonotonicInstant>,
}
