//! Per-`WsUrl` FSM driver for one WebSocket connection. All state mutation
//! happens on the I/O Reactor (single-writer).

mod fsm_event;
mod handle_event;
mod inner;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use crate::clock::Clock;
use crate::conn::rate_budget::ConnectionRateBudget;
use crate::dispatch::DispatchEventBus;
use crate::error::ConnectionError;
use crate::jitter::JitterSource;
use crate::transport::{WsSocket, WsSocketFactoryLike};
use crate::types::{ChannelName, ConnectionState, Symbol, WsUrl};

pub use fsm_event::{AuthErrorKind, FsmEvent};
pub(crate) use inner::{
    DrainCause, PendingRequest, PendingRequestMap, RequestHandle, StalenessMonitor, TimerSet,
    WsResponse,
};

/// Next reconnect backoff: `min(base * factor^attempt, max)`, then partial
/// jitter (`frac = backoff_jitter`). Pure; all params read from `Knobs`.
pub(crate) fn next_backoff(
    knobs: &crate::build::knobs::Knobs,
    attempt: u32,
    jitter: &dyn JitterSource,
) -> std::time::Duration {
    let base_ms = f64::from(knobs.backoff_base_ms);
    let max_ms = f64::from(knobs.backoff_max_ms);
    // Factor < 1 or non-finite collapses the delay toward 0 and spins the reconnect loop.
    let factor = if knobs.backoff_factor.is_finite() {
        knobs.backoff_factor.max(1.0)
    } else {
        2.0
    };

    // Clamp the u32 attempt into `powi`'s i32 so it saturates rather than wraps.
    let exp = i32::try_from(attempt).unwrap_or(i32::MAX);
    let scaled = base_ms * factor.powi(exp);
    // NaN/+inf (f64 overflow) falls back to the ceiling.
    let computed = if scaled.is_finite() {
        scaled.min(max_ms)
    } else {
        max_ms
    };

    // Non-finite frac falls back to full jitter rather than a hot-loop 0ms delay.
    let frac = if knobs.backoff_jitter.is_finite() {
        knobs.backoff_jitter.clamp(0.0, 1.0)
    } else {
        1.0
    };
    let delay_ms = computed * ((1.0 - frac) + frac * jitter.next_unit());
    std::time::Duration::from_millis(delay_ms as u64)
}

/// Per-`WsUrl` FSM driver. Mutated only by the I/O Reactor.
pub struct ManagedConnection {
    url: WsUrl,

    state: ConnectionState,

    /// Reset when the connection reaches `Open`.
    attempt_count: u32,

    /// Separate from `attempt_count`; reset on `auth_handshake_ok`, not `enter_open`.
    auth_handshake_fail_count: u32,

    /// Set on first `Open`, never reset; gates resubscribe-replay to reconnects only.
    has_been_open: bool,

    awaiting_token_refresh: bool,

    server_connection_id: Option<u64>,

    socket: Option<Arc<dyn WsSocket>>,

    /// Drained before any disconnect lifecycle emit.
    pending_requests: PendingRequestMap,

    staleness_monitor: StalenessMonitor,

    bus: Arc<DispatchEventBus>,

    clock: Arc<dyn Clock>,

    jitter: Arc<dyn JitterSource>,

    /// FSM-owned so socket opens preserve the single-writer rule.
    factory: Arc<dyn WsSocketFactoryLike>,

    /// Host-scoped connect-rate budget; consumed on every `Connecting` entry.
    rate_budget: Arc<ConnectionRateBudget>,

    timers: TimerSet,

    subscribe_ack_attempts: std::collections::HashMap<(ChannelName, Option<Symbol>), u8>,

    /// `request_id` that drove `Connecting`; stamps the open event.
    pending_connect_request_id: Option<u64>,

    pending_close_request_id: Option<u64>,

    /// Latched during a clean close; re-enters `Connecting` only after `Closing → Closed`.
    pending_reconnect_request_id: Option<u64>,

    /// Extra concurrent `close()` waiters; each gets its own correlated event.
    additional_close_request_ids: Vec<u64>,

    /// Extra concurrent `force_reconnect()` waiters; each gets its own reopen event.
    additional_connect_request_ids: Vec<u64>,

    /// Auth subscribes deferred on a token miss; cleared on socket teardown.
    deferred_open_auth_subscribes: Vec<(
        ChannelName,
        Option<Symbol>,
        crate::conn::subscription_registry::SubscribeParams,
    )>,

    /// Starts at 1 (`0` is the unstamped sentinel).
    next_subscribe_req_id: u64,

    /// Correlates `channel`-less rejects.
    pending_subscribe_req_ids: std::collections::HashMap<u64, (ChannelName, Option<Symbol>)>,

    /// Reverse index so disarm/teardown drops the forward entry in O(1).
    subscribe_req_id_by_key: std::collections::HashMap<(ChannelName, Option<Symbol>), u64>,
}

impl ManagedConnection {
    pub(crate) fn new(
        url: WsUrl,
        bus: Arc<DispatchEventBus>,
        factory: Arc<dyn WsSocketFactoryLike>,
        rate_budget: Arc<ConnectionRateBudget>,
        clock: Arc<dyn Clock>,
        jitter: Arc<dyn JitterSource>,
    ) -> Self {
        let staleness_window_ms = bus.knobs().staleness_window_ms;
        Self {
            url,
            state: ConnectionState::Idle,
            attempt_count: 0,
            auth_handshake_fail_count: 0,
            has_been_open: false,
            awaiting_token_refresh: false,
            server_connection_id: None,
            socket: None,
            pending_requests: PendingRequestMap::default(),
            staleness_monitor: StalenessMonitor::new(staleness_window_ms),
            bus,
            clock,
            jitter,
            factory,
            rate_budget,
            timers: TimerSet::default(),
            subscribe_ack_attempts: std::collections::HashMap::new(),
            pending_connect_request_id: None,
            pending_close_request_id: None,
            pending_reconnect_request_id: None,
            additional_close_request_ids: Vec::new(),
            additional_connect_request_ids: Vec::new(),
            deferred_open_auth_subscribes: Vec::new(),
            next_subscribe_req_id: 1,
            pending_subscribe_req_ids: std::collections::HashMap::new(),
            subscribe_req_id_by_key: std::collections::HashMap::new(),
        }
    }

    /// Saturates rather than wrapping to the `0` sentinel.
    pub(crate) fn next_subscribe_req_id(&mut self) -> u64 {
        let id = self.next_subscribe_req_id;
        self.next_subscribe_req_id = self.next_subscribe_req_id.saturating_add(1);
        id
    }

    pub(crate) fn record_subscribe_req_id(
        &mut self,
        req_id: u64,
        channel: ChannelName,
        pair: Option<Symbol>,
    ) {
        let key = (channel, pair);
        // Evict any lingering prior req_id for this key so neither map leaks.
        if let Some(prev) = self.subscribe_req_id_by_key.insert(key.clone(), req_id) {
            self.pending_subscribe_req_ids.remove(&prev);
        }
        self.pending_subscribe_req_ids.insert(req_id, key);
    }

    pub(crate) fn resolve_subscribe_req_id(
        &self,
        req_id: u64,
    ) -> Option<(ChannelName, Option<Symbol>)> {
        self.pending_subscribe_req_ids.get(&req_id).cloned()
    }

    fn forget_subscribe_req_id_for_key(&mut self, key: &(ChannelName, Option<Symbol>)) {
        if let Some(req_id) = self.subscribe_req_id_by_key.remove(key) {
            self.pending_subscribe_req_ids.remove(&req_id);
        }
    }

    pub(crate) fn socket(&self) -> Option<&Arc<dyn WsSocket>> {
        self.socket.as_ref()
    }

    pub(crate) fn clock_now(&self) -> crate::types::MonotonicInstant {
        self.clock.now()
    }

    /// Transient: burn retry budget or escalate to teardown; non-transient:
    /// terminate only that entry.
    pub(crate) fn handle_subscribe_failure(
        &mut self,
        channel: ChannelName,
        pair: Option<Symbol>,
        transient: bool,
        last_error: Option<String>,
    ) {
        use ConnectionState::*;
        let key = (channel, pair.clone());
        self.timers.per_entry_subscribe_ack_timeouts.remove(&key);
        self.timers.per_entry_subscribe_resend_due.remove(&key);
        self.forget_subscribe_req_id_for_key(&key);

        if !transient {
            self.subscribe_ack_attempts.remove(&key);
            let now = self.clock.now();
            self.bus.publish(crate::dispatch::EventEnvelope {
                event_type: crate::dispatch::EventType::SubscriptionTerminatedEvent,
                event_version: 2,
                timestamp_monotonic: now,
                request_id: None,
                payload: crate::dispatch::EventPayload::SubscriptionTerminatedEvent {
                    channel,
                    pair: pair.clone(),
                    cause: crate::api::subscription::TerminationCause::NonTransientWireRejection,
                    last_error,
                    terminated_at_monotonic: now,
                },
            });
            tracing::warn!(
                target: "kraken_sdk::fsm",
                url = ?self.url,
                ?channel,
                ?pair,
                state = ?self.state,
                "non-transient subscribe failure — entry terminated (SubscriptionTerminatedEvent emitted); connection stays up"
            );
            // Terminating the LAST entry mid-Resubscribing leaves no ack to drive → Open.
            self.complete_resubscribe_if_empty();
            return;
        }

        // Absent entry = initial send failed before an ack timer armed; seed at budget-1.
        let remaining = match self.subscribe_ack_attempts.get_mut(&key) {
            Some(n) => {
                *n = n.saturating_sub(1);
                *n
            }
            None => {
                let budget = self
                    .bus
                    .knobs()
                    .subscribe_ack_attempts
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .min(u8::MAX as u32) as u8;
                let seeded = budget.saturating_sub(1);
                self.subscribe_ack_attempts.insert(key.clone(), seeded);
                seeded
            }
        };

        if remaining > 0 {
            // Fixed 200ms resend backoff; must be non-zero to avoid a hot loop.
            let resend_backoff = std::time::Duration::from_millis(200);
            let due_at = crate::types::MonotonicInstant(self.clock.now().0 + resend_backoff);
            self.timers
                .per_entry_subscribe_resend_due
                .insert(key, due_at);
            tracing::debug!(
                target: "kraken_sdk::fsm",
                url = ?self.url,
                ?channel,
                ?pair,
                remaining,
                "transient subscribe failure — resend timer armed"
            );
            return;
        }

        // Budget exhausted: teardown drains in-flight requests BEFORE any lifecycle emit.
        self.exit_resubscribing_teardown();

        if self.state == Open {
            tracing::warn!(
                target: "kraken_sdk::fsm", url = ?self.url, ?channel, ?pair,
                "Open: subscribe_ack budget exhausted — drained + ConnectionDropped → BackingOff"
            );
            if self.cap_exhausted_then_escalate(|_| {}) {
                return;
            }
            self.state = BackingOff;
            self.timers.backoff_due_at = Some(self.compute_backoff_due());
            self.emit_lifecycle(
                crate::dispatch::EventType::ConnectionDroppedEvent,
                crate::dispatch::EventPayload::ConnectionDroppedEvent {
                    url: self.url,
                    dropped_at_monotonic: self.clock.now(),
                    close_code: None,
                    reason: Some(format!(
                        "subscribe_ack budget exhausted for ({channel:?}, {pair:?})"
                    )),
                },
                None,
            );
        } else {
            tracing::warn!(
                target: "kraken_sdk::fsm", url = ?self.url, ?channel, ?pair,
                "pre-Open: subscribe_ack budget exhausted — AttemptFailed → BackingOff"
            );
            self.state = BackingOff;
            let backoff_due = self.compute_backoff_due();
            self.timers.backoff_due_at = Some(backoff_due);
            self.emit_attempt_failed(
                backoff_due,
                crate::dispatch::TransientClass::SubscribeBudgetExhausted,
                Some(format!(
                    "subscribe_ack budget exhausted for ({channel:?}, {pair:?})"
                )),
                None,
                None,
            );
        }
    }

    /// Clear all per-entry subscribe-ack state on exits that bypass
    /// `disarm_subscribe_ack` (stale timers must not leak).
    fn clear_subscribe_ack_state(&mut self) {
        self.timers.per_entry_subscribe_ack_timeouts.clear();
        self.timers.per_entry_subscribe_resend_due.clear();
        self.subscribe_ack_attempts.clear();
        self.pending_subscribe_req_ids.clear();
        self.subscribe_req_id_by_key.clear();
        // A stale reseed timer must not fire mid-reconnect.
        self.timers.per_entry_book_reseed_snapshot.clear();
        // A deferral surviving teardown would double-subscribe on the next refresh drain.
        self.deferred_open_auth_subscribes.clear();
        // A stale-true refresh flag would suppress every later force_refresh kick.
        self.awaiting_token_refresh = false;
    }

    /// Drains the pending map LAST so drain-before-emit holds for every caller.
    fn exit_authenticating_teardown(&mut self) {
        self.socket = None;
        self.staleness_monitor.disarm();
        self.timers.staleness_due_at = None;
        self.clear_subscribe_ack_state();
        self.pending_requests
            .drain(DrainCause::RequestInFlightWhenDropped, &self.bus);
    }

    /// Socket KEPT for the close handshake; NO drain here (the `Closing` exit
    /// resolves in-flight requests).
    fn exit_authenticating_to_closing(&mut self) {
        self.staleness_monitor.disarm();
        self.timers.staleness_due_at = None;
        self.clear_subscribe_ack_state();
    }

    /// Same shape as the `Authenticating` exit (drain LAST).
    fn exit_resubscribing_teardown(&mut self) {
        self.socket = None;
        self.staleness_monitor.disarm();
        self.timers.staleness_due_at = None;
        self.clear_subscribe_ack_state();
        self.pending_requests
            .drain(DrainCause::RequestInFlightWhenDropped, &self.bus);
    }

    /// Drains FIRST (drain-before-emit). Subscribe-ack clears stay at each caller.
    fn exit_open_teardown(&mut self, cause: DrainCause) {
        self.pending_requests.drain(cause, &self.bus);
        self.socket = None;
        self.staleness_monitor.disarm();
        self.timers.staleness_due_at = None;
    }

    /// Backoff due instant with `attempt = attempt_count − 1` (count 1 yields `base_ms`).
    fn compute_backoff_due(&self) -> crate::types::MonotonicInstant {
        let attempt = self.attempt_count.saturating_sub(1);
        let delay = next_backoff(self.bus.knobs(), attempt, self.jitter.as_ref());
        crate::types::MonotonicInstant(self.clock.now().0 + delay)
    }

    /// Reads `staleness_window_ms` fresh off `Knobs`.
    fn arm_staleness(&mut self) {
        let window_ms = self.bus.knobs().staleness_window_ms;
        self.staleness_monitor = StalenessMonitor::new(window_ms);
        let now = self.clock.now();
        self.staleness_monitor.arm(now);
        self.timers.staleness_due_at = self.staleness_monitor.deadline();
    }

    pub(crate) fn note_inbound_activity(&mut self) {
        let now = self.clock.now();
        self.staleness_monitor.note_inbound(now);
        self.timers.staleness_due_at = self.staleness_monitor.deadline();
    }

    fn arm_upgrade_timeout(&mut self) {
        let due =
            crate::types::MonotonicInstant(self.clock.now().0 + self.bus.knobs().upgrade_timeout());
        self.timers.upgrade_timeout_due_at = Some(due);
    }

    fn arm_close_timeout(&mut self) {
        let due =
            crate::types::MonotonicInstant(self.clock.now().0 + self.bus.knobs().close_timeout());
        self.timers.close_timeout_due_at = Some(due);
    }

    pub(crate) fn arm_token_refresh(&mut self, due: crate::types::MonotonicInstant) {
        self.timers.token_refresh_due_at = Some(due);
    }

    pub(crate) fn disarm_token_refresh(&mut self) {
        self.timers.token_refresh_due_at = None;
    }

    /// Earliest pending timer due instant + the `FsmEvent` to dispatch.
    pub(crate) fn next_timer_due(&self) -> Option<(crate::types::MonotonicInstant, FsmEvent)> {
        // `make_ev` is lazy so a losing per-entry timer never clones its `Symbol`.
        fn consider(
            earliest: &mut Option<(crate::types::MonotonicInstant, FsmEvent)>,
            due: Option<crate::types::MonotonicInstant>,
            make_ev: impl FnOnce() -> FsmEvent,
        ) {
            let Some(due) = due else { return };
            let replace = match earliest.as_ref() {
                Some((curr, _)) => due.0 < curr.0,
                None => true,
            };
            if replace {
                *earliest = Some((due, make_ev()));
            }
        }

        let mut earliest: Option<(crate::types::MonotonicInstant, FsmEvent)> = None;
        consider(&mut earliest, self.timers.backoff_due_at, || {
            FsmEvent::TimerBackoffElapsed
        });
        consider(&mut earliest, self.timers.upgrade_timeout_due_at, || {
            FsmEvent::TimerUpgradeTimeout
        });
        consider(&mut earliest, self.timers.staleness_due_at, || {
            FsmEvent::TimerStalenessElapsed
        });
        consider(&mut earliest, self.timers.close_timeout_due_at, || {
            FsmEvent::TimerCloseTimeout
        });
        consider(
            &mut earliest,
            self.timers.rate_budget_window_advanced_at,
            || FsmEvent::TimerRateBudgetWindowAdvanced,
        );
        consider(&mut earliest, self.timers.token_refresh_due_at, || {
            FsmEvent::TimerTokenRefreshDue
        });
        for ((channel, pair), due) in &self.timers.per_entry_subscribe_ack_timeouts {
            consider(&mut earliest, Some(*due), || {
                FsmEvent::TimerSubscribeAckTimeout {
                    channel: *channel,
                    pair: pair.clone(),
                }
            });
        }
        for ((channel, pair), due) in &self.timers.per_entry_subscribe_resend_due {
            consider(&mut earliest, Some(*due), || {
                FsmEvent::TimerSubscribeResendDue {
                    channel: *channel,
                    pair: pair.clone(),
                }
            });
        }
        for ((channel, pair), due) in &self.timers.per_entry_book_reseed_snapshot {
            consider(&mut earliest, Some(*due), || {
                FsmEvent::TimerBookReseedSnapshot {
                    channel: *channel,
                    pair: pair.clone(),
                }
            });
        }
        earliest
    }

    /// Pop the timer field BEFORE FSM dispatch so a non-matching arm doesn't
    /// leave a past-due timer.
    pub(crate) fn timers_mut(&mut self) -> &mut TimerSet {
        &mut self.timers
    }

    pub(crate) fn arm_subscribe_ack(
        &mut self,
        channel: ChannelName,
        pair: Option<Symbol>,
        due_at: crate::types::MonotonicInstant,
    ) {
        let key = (channel, pair);
        self.timers
            .per_entry_subscribe_ack_timeouts
            .insert(key.clone(), due_at);
        // First arm seeds the budget; re-arms leave it alone so the decrement walks toward 0.
        let budget = self
            .bus
            .knobs()
            .subscribe_ack_attempts
            .load(std::sync::atomic::Ordering::Relaxed)
            .min(u8::MAX as u32) as u8;
        self.subscribe_ack_attempts.entry(key).or_insert(budget);
    }

    pub(crate) fn disarm_subscribe_ack(&mut self, channel: ChannelName, pair: Option<Symbol>) {
        let key = (channel, pair);
        self.timers.per_entry_subscribe_ack_timeouts.remove(&key);
        self.timers.per_entry_subscribe_resend_due.remove(&key);
        self.subscribe_ack_attempts.remove(&key);
        self.forget_subscribe_req_id_for_key(&key);
    }

    pub(crate) fn disarm_book_reseed_snapshot(
        &mut self,
        channel: ChannelName,
        pair: Option<Symbol>,
    ) {
        self.timers
            .per_entry_book_reseed_snapshot
            .remove(&(channel, pair));
    }

    /// The reactor uses it to set `WireSubscribeAck.last` (final ack drives
    /// `Resubscribing → Open`).
    pub(crate) fn pending_subscribe_ack_count(&self) -> usize {
        self.timers.per_entry_subscribe_ack_timeouts.len()
    }

    /// A spurious/duplicate/never-armed ack must NOT drive the FSM toward `Open`.
    pub(crate) fn is_subscribe_ack_armed(
        &self,
        channel: ChannelName,
        pair: Option<Symbol>,
    ) -> bool {
        self.timers
            .per_entry_subscribe_ack_timeouts
            .contains_key(&(channel, pair))
    }

    pub(crate) fn arm_book_reseed_snapshot(
        &mut self,
        channel: ChannelName,
        pair: Option<Symbol>,
        due_at: crate::types::MonotonicInstant,
    ) {
        self.timers
            .per_entry_book_reseed_snapshot
            .insert((channel, pair), due_at);
    }

    /// Fires on reconnect only, never the initial bring-up.
    pub(crate) fn has_been_open(&self) -> bool {
        self.has_been_open
    }

    pub(crate) fn awaiting_token_refresh(&self) -> bool {
        self.awaiting_token_refresh
    }

    /// `Authenticating` with nothing in flight — the ONE auth-probe predicate.
    pub(crate) fn authenticating_nothing_in_flight(&self) -> bool {
        self.state == ConnectionState::Authenticating
            && self.pending_subscribe_ack_count() == 0
            && self.pending_requests.is_empty()
            && !self.awaiting_token_refresh
    }

    pub(crate) fn set_awaiting_token_refresh(&mut self, v: bool) {
        self.awaiting_token_refresh = v;
    }

    /// A removed key's queued re-subscribe must die with it.
    pub(crate) fn remove_deferred_open_auth_subscribe(
        &mut self,
        channel: ChannelName,
        pair: &Option<Symbol>,
    ) {
        self.deferred_open_auth_subscribes
            .retain(|(c, p, _)| !(*c == channel && p == pair));
    }

    /// De-duplicated by `(channel, pair)`.
    pub(crate) fn defer_open_auth_subscribe(
        &mut self,
        channel: ChannelName,
        pair: Option<Symbol>,
        params: crate::conn::subscription_registry::SubscribeParams,
    ) {
        if self
            .deferred_open_auth_subscribes
            .iter()
            .any(|(c, p, _)| *c == channel && *p == pair)
        {
            return;
        }
        self.deferred_open_auth_subscribes
            .push((channel, pair, params));
    }

    /// Drain (not clone) so a later refresh can't re-send an entry already re-issued.
    pub(crate) fn drain_deferred_open_auth_subscribes(
        &mut self,
    ) -> Vec<(
        ChannelName,
        Option<Symbol>,
        crate::conn::subscription_registry::SubscribeParams,
    )> {
        std::mem::take(&mut self.deferred_open_auth_subscribes)
    }

    /// Short-circuit `Resubscribing → Open` when no resubscribe work is outstanding.
    pub(crate) fn complete_resubscribe_if_empty(&mut self) {
        if self.state == ConnectionState::Resubscribing
            && self.pending_subscribe_ack_count() == 0
            && self.timers.per_entry_subscribe_resend_due.is_empty()
        {
            self.enter_open();
        }
    }

    fn enter_open(&mut self) {
        // Captured BEFORE the resets: is_reopen picks the event kind.
        let is_reopen = self.has_been_open;
        let reopen_attempt_count = self.attempt_count;
        self.attempt_count = 0;
        self.has_been_open = true;
        self.state = ConnectionState::Open;
        // Deferred auth subscribes from THIS session survive into Open for the refresh drain.
        self.arm_staleness();
        let pending_rid = self.pending_connect_request_id.take();
        let cohort = std::mem::take(&mut self.additional_connect_request_ids);
        self.emit_open_or_reopened(is_reopen, reopen_attempt_count, pending_rid);
        for rid in cohort {
            self.emit_open_or_reopened(is_reopen, reopen_attempt_count, Some(rid));
        }
        tracing::debug!(
            target: "kraken_sdk::fsm",
            url = ?self.url,
            is_reopen,
            "→ Open (attempt_count reset)"
        );
    }

    fn emit_open_or_reopened(&self, is_reopen: bool, reopen_attempt_count: u32, rid: Option<u64>) {
        if is_reopen {
            self.emit_lifecycle(
                crate::dispatch::EventType::ConnectionReopenedEvent,
                crate::dispatch::EventPayload::ConnectionReopenedEvent {
                    url: self.url,
                    reopened_at_monotonic: self.clock.now(),
                    attempt_count: reopen_attempt_count,
                },
                rid,
            );
        } else {
            self.emit_lifecycle(
                crate::dispatch::EventType::ConnectionOpenEvent,
                crate::dispatch::EventPayload::ConnectionOpenEvent {
                    url: self.url,
                    opened_at_monotonic: self.clock.now(),
                    server_connection_id: self.server_connection_id.map(|id| id.to_string()),
                },
                rid,
            );
        }
    }

    /// One event per waiter (primary + cohort), or a single broadcast when there is none.
    fn emit_connection_failed(
        &mut self,
        last_error: String,
        transient: bool,
        non_transient_class: Option<crate::dispatch::NonTransientClass>,
    ) {
        let primary = self.pending_connect_request_id.take();
        let cohort = std::mem::take(&mut self.additional_connect_request_ids);
        let attempt_count = self.attempt_count;
        let url = self.url;
        let emit = |request_id: Option<u64>, last_error: String| {
            self.emit_lifecycle(
                crate::dispatch::EventType::ConnectionFailedEvent,
                crate::dispatch::EventPayload::ConnectionFailedEvent {
                    url,
                    attempt_count,
                    last_error,
                    transient,
                    non_transient_class,
                },
                request_id,
            );
        };
        if primary.is_none() && cohort.is_empty() {
            emit(None, last_error);
        } else {
            for rid in primary.into_iter().chain(cohort) {
                emit(Some(rid), last_error.clone());
            }
        }
    }

    /// Every `close()` waiter gets its own correlated `ConnectionClosedEvent` first,
    /// then a latched `force_reconnect()` re-enters `Connecting`.
    fn enter_closed_or_reconnect(
        &mut self,
        reason: crate::dispatch::ClosedReason,
        ack: crate::dispatch::AckSource,
    ) {
        self.timers.close_timeout_due_at = None;

        // Sole drain point for Authenticating/Resubscribing→Closing exits
        // (a no-op after Open→Closing).
        let drain_cause = if self.pending_reconnect_request_id.is_some() {
            DrainCause::RequestInFlightWhenDropped
        } else {
            DrainCause::ClientClosed
        };
        self.pending_requests.drain(drain_cause, &self.bus);

        let primary_close = self.pending_close_request_id.take();
        let additional_closes = std::mem::take(&mut self.additional_close_request_ids);
        let reconnect = self.pending_reconnect_request_id.take();

        // Snapshot once so all N close events describe the same physical close.
        let now = self.clock.now();
        let server_connection_id = self.server_connection_id.map(|id| id.to_string());
        for rid in primary_close.into_iter().chain(additional_closes) {
            self.emit_lifecycle(
                crate::dispatch::EventType::ConnectionClosedEvent,
                crate::dispatch::EventPayload::ConnectionClosedEvent {
                    url: self.url,
                    closed_at_monotonic: now,
                    reason,
                    ack,
                    server_connection_id: server_connection_id.clone(),
                },
                Some(rid),
            );
        }

        if let Some(rid) = reconnect {
            self.attempt_count = 0;
            self.reenter_connecting(rid);
        } else {
            self.state = ConnectionState::Closed;
            self.socket = None;
        }
    }

    /// Open a socket, arm the upgrade timeout, emit `ConnectionConnectingEvent`.
    fn enter_connecting(&mut self, rid: Option<u64>) {
        self.socket = Some(self.factory.open_socket(self.url));
        self.arm_upgrade_timeout();
        self.emit_lifecycle(
            crate::dispatch::EventType::ConnectionConnectingEvent,
            crate::dispatch::EventPayload::ConnectionConnectingEvent {
                url: self.url,
                attempt_count: self.attempt_count,
            },
            rid,
        );
    }

    /// Consult the connect-rate budget (force_reconnect is NOT a bypass); on
    /// `Err` land `BackingOff`.
    fn reenter_connecting(&mut self, rid: u64) {
        let now = self.clock.now();
        if let Err(crate::conn::rate_budget::ConnectionRateThrottled {
            attempts_used,
            budget,
            throttle_until_monotonic,
        }) = self.rate_budget.try_consume(now)
        {
            self.state = ConnectionState::BackingOff;
            self.socket = None;
            self.pending_connect_request_id = Some(rid);
            self.timers.rate_budget_window_advanced_at = Some(throttle_until_monotonic);
            self.emit_lifecycle(
                crate::dispatch::EventType::ConnectionRateThrottledEvent,
                crate::dispatch::EventPayload::ConnectionRateThrottledEvent {
                    url: self.url,
                    attempts_used,
                    window_seconds: self.rate_budget.window_seconds(),
                    window_remaining_attempts: budget.saturating_sub(attempts_used),
                    throttle_until_monotonic,
                },
                Some(rid),
            );
            return;
        }
        self.state = ConnectionState::Connecting;
        self.pending_connect_request_id = Some(rid);
        self.enter_connecting(Some(rid));
    }

    /// Pre-Open transient → `BackingOff` (`ConnectionDroppedEvent` is for mid-stream drops).
    fn emit_attempt_failed(
        &self,
        backoff_due: crate::types::MonotonicInstant,
        transient_class: crate::dispatch::TransientClass,
        kraken_error: Option<String>,
        http_status: Option<u16>,
        close_code: Option<u16>,
    ) {
        let now = self.clock.now();
        let backoff_ms = backoff_due.0.saturating_sub(now.0).as_millis() as u64;
        self.emit_lifecycle(
            crate::dispatch::EventType::ConnectionAttemptFailedEvent,
            crate::dispatch::EventPayload::ConnectionAttemptFailedEvent {
                url: self.url,
                attempt: self.attempt_count,
                transient_class,
                kraken_error,
                http_status,
                close_code,
                backoff_ms,
                failed_at_monotonic: now,
            },
            None,
        );
    }

    /// If budget `Some(N)` and `attempt_count >= N`: escalate to `Failed`, return
    /// `true`. Not for the auth cap arm (double-capping).
    fn cap_exhausted_then_escalate(&mut self, cleanup: impl FnOnce(&mut Self)) -> bool {
        use ConnectionState::*;
        let cap = self.bus.knobs().reconnect_attempts.load();
        match cap {
            Some(n) if self.attempt_count >= n => {
                cleanup(self);
                self.state = Failed;
                self.socket = None;
                // Backoff is NOT armed — terminal, not retrying.
                self.staleness_monitor.disarm();
                self.timers.staleness_due_at = None;
                self.timers.upgrade_timeout_due_at = None;
                self.emit_connection_failed(
                    format!(
                        "reconnect attempt budget exhausted ({} attempts)",
                        self.attempt_count
                    ),
                    false,
                    Some(crate::dispatch::NonTransientClass::RetryCapExhausted),
                );
                tracing::warn!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    attempt_count = self.attempt_count,
                    reconnect_attempts_cap = n,
                    "→ Failed (reconnect_attempts cap exhausted; RetryCapExhausted)"
                );
                true
            }
            _ => false,
        }
    }

    fn emit_lifecycle(
        &self,
        event_type: crate::dispatch::EventType,
        payload: crate::dispatch::EventPayload,
        request_id: Option<u64>,
    ) {
        self.bus.publish(crate::dispatch::EventEnvelope {
            event_type,
            event_version: 1,
            timestamp_monotonic: self.clock.now(),
            request_id,
            payload,
        });
    }

    pub(crate) fn state(&self) -> ConnectionState {
        self.state
    }

    /// Connection-backoff attempt counter (NOT the auth handshake counter).
    pub(crate) fn attempt_count(&self) -> u32 {
        self.attempt_count
    }

    pub(crate) fn record_pending_request(&mut self, pending: PendingRequest) {
        self.pending_requests.record(pending);
    }

    pub(crate) fn resolve_pending_request(&mut self, resp: WsResponse) {
        self.pending_requests.resolve(resp);
    }

    pub(crate) fn fail_pending_request(&mut self, req_id: u64, err: ConnectionError) {
        self.pending_requests.fail_one(req_id, err);
    }

    /// Guards the bare-order dual-leg demux against double-resolving an already-drained entry.
    pub(crate) fn pending_requests_contains(&self, req_id: u64) -> bool {
        self.pending_requests.contains(req_id)
    }

    /// The SDK never auto-resends; the caller reconciles.
    pub(crate) fn drain_one_retryable(&mut self, req_id: u64) {
        self.pending_requests.drain_one_retryable(req_id, &self.bus);
    }

    #[cfg(test)]
    pub(crate) fn pending_request_count(&self) -> usize {
        self.pending_requests.pending_request_count()
    }
}

#[cfg(test)]
impl ManagedConnection {
    /// Bypasses the transition table.
    pub(crate) fn test_set_state(&mut self, s: ConnectionState) {
        self.state = s;
    }

    pub(crate) fn test_set_attempt_count(&mut self, n: u32) {
        self.attempt_count = n;
    }

    /// A non-transient reject must NOT arm a resend timer.
    pub(crate) fn test_has_subscribe_resend_timer(
        &self,
        channel: ChannelName,
        pair: Option<Symbol>,
    ) -> bool {
        self.timers
            .per_entry_subscribe_resend_due
            .contains_key(&(channel, pair))
    }

    pub(crate) fn test_set_has_been_open(&mut self, v: bool) {
        self.has_been_open = v;
    }

    pub(crate) fn test_set_pending_close_request_id(&mut self, rid: Option<u64>) {
        self.pending_close_request_id = rid;
    }

    pub(crate) fn test_pending_close_request_id(&self) -> Option<u64> {
        self.pending_close_request_id
    }

    pub(crate) fn test_pending_reconnect_request_id(&self) -> Option<u64> {
        self.pending_reconnect_request_id
    }

    pub(crate) fn test_additional_close_request_ids(&self) -> Vec<u64> {
        self.additional_close_request_ids.clone()
    }

    pub(crate) fn test_additional_connect_request_ids(&self) -> Vec<u64> {
        self.additional_connect_request_ids.clone()
    }

    pub(crate) fn test_attempt_count(&self) -> u32 {
        self.attempt_count
    }

    pub(crate) fn test_set_auth_handshake_fail_count(&mut self, n: u32) {
        self.auth_handshake_fail_count = n;
    }

    pub(crate) fn test_auth_handshake_fail_count(&self) -> u32 {
        self.auth_handshake_fail_count
    }

    /// Does NOT start the dispatch reactor — events sit in the ring until drained.
    pub(crate) fn test_bus(&self) -> &Arc<crate::dispatch::DispatchEventBus> {
        &self.bus
    }
}
