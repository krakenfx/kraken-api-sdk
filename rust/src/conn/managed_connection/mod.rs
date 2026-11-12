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
#[cfg(test)]
use crate::types::Symbol;
use crate::types::{ChannelName, ConnectionState, StreamSymbol, WsUrl};

pub use fsm_event::{AuthErrorKind, FsmEvent};
pub(crate) use inner::{
    DrainCause, PendingRequest, PendingRequestMap, RequestHandle, StalenessMonitor, TimerSet,
    WsResponse,
};

/// Reconnect backoff: `min(base * factor^attempt, max)` with partial jitter (`frac = backoff_jitter`).
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

fn rekey<V>(
    map: &mut std::collections::HashMap<(ChannelName, Option<StreamSymbol>), V>,
    from: &(ChannelName, Option<StreamSymbol>),
    to: &(ChannelName, Option<StreamSymbol>),
) {
    if let Some(value) = map.remove(from) {
        map.entry(to.clone()).or_insert(value);
    }
}

pub(crate) type SubscribeKeys = Arc<Vec<(ChannelName, Option<StreamSymbol>)>>;

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

    /// Entries of a tokenless connection whose initial subscribe did not hand off to
    /// the writer, or was lost with its socket. They replay after the first upgrade only.
    initial_subscribe_replay: std::collections::HashSet<(ChannelName, Option<StreamSymbol>)>,

    snapshot_opt_out_written: std::collections::HashSet<ChannelName>,

    /// The first socket died before `Open` after a subscribe was queued; pending Unified entries
    /// are restored into [`Self::initial_subscribe_replay`] on the next upgrade.
    pre_open_subscribe_lost: bool,

    awaiting_token_refresh: bool,

    pending_reactive_token_refresh_id: Option<u64>,

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

    rate_budget: Arc<ConnectionRateBudget>,

    timers: TimerSet,

    subscribe_ack_attempts: std::collections::HashMap<(ChannelName, Option<StreamSymbol>), u8>,

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
        Option<StreamSymbol>,
        crate::conn::subscription_registry::SubscribeParams,
    )>,

    /// Starts at 1 (`0` is the unstamped sentinel).
    next_subscribe_req_id: u64,

    pending_subscribe_req_ids: std::collections::HashMap<u64, SubscribeKeys>,

    /// Reverse index so disarm and teardown find a key's req_id without scanning.
    subscribe_req_id_by_key: std::collections::HashMap<(ChannelName, Option<StreamSymbol>), u64>,
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
            initial_subscribe_replay: std::collections::HashSet::new(),
            snapshot_opt_out_written: std::collections::HashSet::new(),
            pre_open_subscribe_lost: false,
            awaiting_token_refresh: false,
            pending_reactive_token_refresh_id: None,
            server_connection_id: None,
            socket: None,
            pending_requests: PendingRequestMap::new(url),
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
        pair: Option<StreamSymbol>,
    ) {
        let key = (channel, pair);
        // Evict any lingering prior req_id for this key so neither map leaks.
        if let Some(prev) = self.subscribe_req_id_by_key.insert(key.clone(), req_id) {
            self.detach_req_key(prev, &key);
        }
        Arc::make_mut(self.pending_subscribe_req_ids.entry(req_id).or_default()).push(key);
    }

    /// Move an in-flight subscribe from a released Unified book row onto its peer.
    /// Returns `false` when no frame is outstanding and the caller must subscribe the peer itself.
    pub(crate) fn handoff_subscribe_inflight(
        &mut self,
        from: ChannelName,
        to: ChannelName,
        pair: Option<StreamSymbol>,
    ) -> bool {
        let from_key = (from, pair.clone());
        let to_key = (to, pair);
        if self.subscribe_req_id_by_key.contains_key(&to_key) {
            return true;
        }
        if self.initial_subscribe_replay.remove(&from_key) {
            self.initial_subscribe_replay.insert(to_key.clone());
        }
        let Some(req_id) = self.subscribe_req_id_by_key.remove(&from_key) else {
            return false;
        };
        if let Some(entries) = self.pending_subscribe_req_ids.get_mut(&req_id) {
            let entries = Arc::make_mut(entries);
            entries.retain(|entry| entry != &from_key);
            if !entries.iter().any(|entry| entry == &to_key) {
                entries.push(to_key.clone());
            }
        }
        self.subscribe_req_id_by_key.insert(to_key.clone(), req_id);
        rekey(
            &mut self.timers.per_entry_subscribe_ack_timeouts,
            &from_key,
            &to_key,
        );
        rekey(&mut self.subscribe_ack_attempts, &from_key, &to_key);
        rekey(
            &mut self.timers.per_entry_subscribe_resend_due,
            &from_key,
            &to_key,
        );
        true
    }

    fn detach_req_key(&mut self, req_id: u64, key: &(ChannelName, Option<StreamSymbol>)) {
        if let Some(entries) = self.pending_subscribe_req_ids.get_mut(&req_id) {
            let entries = Arc::make_mut(entries);
            entries.retain(|entry| entry != key);
            if entries.is_empty() {
                self.pending_subscribe_req_ids.remove(&req_id);
            }
        }
    }

    pub(crate) fn resolve_subscribe_req_id(&self, req_id: u64) -> Option<SubscribeKeys> {
        self.pending_subscribe_req_ids.get(&req_id).cloned()
    }

    fn forget_subscribe_req_id_for_key(&mut self, key: &(ChannelName, Option<StreamSymbol>)) {
        if let Some(req_id) = self.subscribe_req_id_by_key.remove(key) {
            self.detach_req_key(req_id, key);
        }
    }

    pub(crate) fn socket(&self) -> Option<&Arc<dyn WsSocket>> {
        self.socket.as_ref()
    }

    pub(crate) fn clock_now(&self) -> crate::types::MonotonicInstant {
        self.clock.now()
    }

    pub(crate) fn emit_futures_subscribed(
        &self,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
        ack: &crate::dispatch::FuturesSubscribeAck,
    ) {
        if !self.url.is_unified() {
            return;
        }
        self.bus
            .publish_futures_subscribed(self.clock.now(), channel, pair.as_ref(), ack.clone());
    }

    pub(crate) fn emit_futures_subscribe_failed(
        &self,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
        transient: bool,
        error: Option<&str>,
        ack: &crate::dispatch::FuturesSubscribeAck,
    ) {
        if !self.url.is_unified() {
            return;
        }
        let (symbol, interval, tick_type) = StreamSymbol::futures_identity(pair.as_ref());
        self.bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::FuturesSubscribeFailedEvent,
            event_version: 1,
            timestamp_monotonic: self.clock.now(),
            request_id: None,
            payload: crate::dispatch::EventPayload::FuturesSubscribeFailedEvent {
                channel,
                symbol,
                interval,
                tick_type,
                attempt: self.subscribe_attempt(channel, pair),
                error_class: if transient {
                    crate::dispatch::FuturesSubscribeFailureClass::Transient
                } else {
                    crate::dispatch::FuturesSubscribeFailureClass::NonTransient
                },
                error: error.map(str::to_owned),
                ack: ack.clone(),
            },
        });
    }

    fn subscribe_ack_budget(&self) -> u8 {
        self.bus
            .knobs()
            .subscribe_ack_attempts
            .load(std::sync::atomic::Ordering::Relaxed)
            .min(u8::MAX as u32) as u8
    }

    fn subscribe_attempt(&self, channel: ChannelName, pair: &Option<StreamSymbol>) -> u32 {
        let budget = self.subscribe_ack_budget();
        let remaining = self
            .subscribe_ack_attempts
            .get(&(channel, pair.clone()))
            .copied()
            .unwrap_or(budget);
        u32::from(budget.saturating_sub(remaining).saturating_add(1))
    }

    fn handle_subscribe_rejected(
        &mut self,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
        error: crate::conn::SubscribeErrorKind,
    ) {
        let crate::conn::SubscribeErrorKind::SubscribeRejected {
            kraken_code,
            transient,
            ack,
        } = error;
        self.handle_subscribe_failure(channel, pair, transient, Some(kraken_code), &ack);
    }

    fn handle_subscribe_ack_timeout(&mut self, channel: ChannelName, pair: Option<StreamSymbol>) {
        self.handle_subscribe_failure(
            channel,
            pair,
            true,
            None,
            &crate::dispatch::FuturesSubscribeAck::default(),
        );
    }

    pub(crate) fn handle_subscribe_failure(
        &mut self,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
        transient: bool,
        last_error: Option<String>,
        ack: &crate::dispatch::FuturesSubscribeAck,
    ) {
        use ConnectionState::*;
        self.emit_futures_subscribe_failed(channel, &pair, transient, last_error.as_deref(), ack);
        let key = (channel, pair.clone());
        self.timers.per_entry_subscribe_ack_timeouts.remove(&key);
        self.timers.per_entry_subscribe_resend_due.remove(&key);
        self.forget_subscribe_req_id_for_key(&key);

        if !transient {
            self.subscribe_ack_attempts.remove(&key);
            self.bus.publish_subscription_terminated(
                self.clock.now(),
                self.url,
                channel,
                pair.clone(),
                crate::api::subscription::TerminationCause::NonTransientWireRejection,
                last_error,
            );
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
                let budget = self.subscribe_ack_budget();
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
            self.timers.upgrade_timeout_due_at = None;
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
            self.note_pre_open_socket_lost();
        }
    }

    /// Clear per-entry subscribe-ack state on exits that bypass `disarm_subscribe_ack`,
    /// so stale timers do not leak.
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
        self.pending_reactive_token_refresh_id = None;
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

    /// Drains the pending map LAST.
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

    fn compute_backoff_due(&self) -> crate::types::MonotonicInstant {
        let attempt = self.attempt_count.saturating_sub(1);
        let delay = next_backoff(self.bus.knobs(), attempt, self.jitter.as_ref());
        crate::types::MonotonicInstant(self.clock.now().0 + delay)
    }

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
        pair: Option<StreamSymbol>,
        due_at: crate::types::MonotonicInstant,
    ) {
        let key = (channel, pair);
        self.timers
            .per_entry_subscribe_ack_timeouts
            .insert(key.clone(), due_at);
        // First arm seeds the budget; re-arms leave it alone so the decrement walks toward 0.
        let budget = self.subscribe_ack_budget();
        self.subscribe_ack_attempts.entry(key).or_insert(budget);
    }

    pub(crate) fn disarm_subscribe_ack(
        &mut self,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
    ) {
        for channel in self.wire_stream_channels(channel) {
            self.disarm_subscribe_ack_key(channel, pair.clone());
        }
    }

    pub(crate) fn disarm_subscribe_ack_key(
        &mut self,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
    ) {
        let key = (channel, pair);
        self.timers.per_entry_subscribe_ack_timeouts.remove(&key);
        self.timers.per_entry_subscribe_resend_due.remove(&key);
        self.subscribe_ack_attempts.remove(&key);
        self.forget_subscribe_req_id_for_key(&key);
    }

    /// Cancels a resend made redundant by another route; the attempt budget is kept.
    pub(crate) fn cancel_subscribe_resend(
        &mut self,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) {
        for channel in self.wire_stream_channels(channel) {
            self.timers
                .per_entry_subscribe_resend_due
                .remove(&(channel, pair.clone()));
        }
    }

    /// Logical channels sharing one wire subscription (Unified Public: maintained and raw book share one `book` stream).
    fn wire_stream_channels(&self, channel: ChannelName) -> Vec<ChannelName> {
        if self.url.shares_unified_book_stream(channel) {
            vec![ChannelName::Book, ChannelName::BookRaw]
        } else {
            vec![channel]
        }
    }

    pub(crate) fn disarm_book_reseed_snapshot(
        &mut self,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
    ) {
        self.timers
            .per_entry_book_reseed_snapshot
            .remove(&(channel, pair));
    }

    /// Used to set `WireSubscribeAck.last`; the final ack drives `Resubscribing → Open`.
    pub(crate) fn pending_subscribe_ack_count(&self) -> usize {
        self.timers.per_entry_subscribe_ack_timeouts.len()
    }

    /// A spurious/duplicate/never-armed ack must NOT drive the FSM toward `Open`.
    pub(crate) fn is_subscribe_ack_armed(
        &self,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
    ) -> bool {
        self.timers
            .per_entry_subscribe_ack_timeouts
            .contains_key(&(channel, pair))
    }

    pub(crate) fn arm_book_reseed_snapshot(
        &mut self,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
        due_at: crate::types::MonotonicInstant,
    ) {
        self.timers
            .per_entry_book_reseed_snapshot
            .insert((channel, pair), due_at);
    }

    pub(crate) fn has_been_open(&self) -> bool {
        self.has_been_open
    }

    pub(crate) fn mark_initial_subscribe_replay(
        &mut self,
        channel: ChannelName,
        pairs: &[Option<StreamSymbol>],
    ) {
        if !self.has_been_open && !self.url.requires_ws_token() {
            self.initial_subscribe_replay
                .extend(pairs.iter().cloned().map(|pair| (channel, pair)));
        }
    }

    pub(crate) fn note_snapshot_opt_out_written(
        &mut self,
        channel: ChannelName,
        pairs: &[Option<StreamSymbol>],
    ) {
        if pairs.iter().any(Option::is_none) {
            self.snapshot_opt_out_written.insert(channel);
        }
    }

    pub(crate) fn forget_snapshot_opt_out_written(
        &mut self,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) {
        if pair.is_none() {
            self.snapshot_opt_out_written.remove(&channel);
        }
    }

    pub(crate) fn take_snapshot_opt_out_written(
        &mut self,
    ) -> std::collections::HashSet<ChannelName> {
        std::mem::take(&mut self.snapshot_opt_out_written)
    }

    pub(crate) fn needs_initial_subscribe_replay(&self) -> bool {
        !self.initial_subscribe_replay.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn initial_subscribe_replay_keys(
        &self,
    ) -> &std::collections::HashSet<(ChannelName, Option<StreamSymbol>)> {
        &self.initial_subscribe_replay
    }

    pub(crate) fn initial_replay_filter(
        &self,
    ) -> Option<&std::collections::HashSet<(ChannelName, Option<StreamSymbol>)>> {
        (!self.has_been_open && !self.url.requires_ws_token())
            .then_some(&self.initial_subscribe_replay)
    }

    pub(crate) fn clear_initial_subscribe_replay(
        &mut self,
        channel: ChannelName,
        pairs: &[Option<StreamSymbol>],
    ) {
        if self.initial_subscribe_replay.is_empty() {
            return;
        }
        for pair in pairs {
            self.initial_subscribe_replay
                .remove(&(channel, pair.clone()));
        }
    }

    /// Records a discarded pre-`Open` socket so a queued subscribe is replayed.
    pub(crate) fn note_pre_open_socket_lost(&mut self) {
        if self.url.requires_ws_token() || self.has_been_open {
            return;
        }
        if !matches!(
            self.state,
            ConnectionState::BackingOff | ConnectionState::Failed
        ) {
            return;
        }
        self.pre_open_subscribe_lost = true;
        self.clear_subscribe_ack_state();
    }

    pub(crate) fn take_pre_open_subscribe_lost(&mut self) -> bool {
        std::mem::take(&mut self.pre_open_subscribe_lost)
    }

    pub(crate) fn awaiting_token_refresh(&self) -> bool {
        self.awaiting_token_refresh
    }

    pub(crate) fn pending_reactive_token_refresh_id(&self) -> Option<u64> {
        self.pending_reactive_token_refresh_id
    }

    /// Single-flight: returns whether this call spawned the refresh.
    pub(crate) fn force_reactive_token_refresh(
        &mut self,
        auth_stack: &crate::auth::AuthStack,
    ) -> bool {
        if self.pending_reactive_token_refresh_id.is_some() {
            return false;
        }
        let handle = auth_stack.force_refresh(crate::auth::RefreshReason::AuthHandshakeFailed);
        self.awaiting_token_refresh = true;
        self.pending_reactive_token_refresh_id = Some(handle.id);
        true
    }

    pub(crate) fn complete_reactive_token_refresh(&mut self, request_id: u64) {
        debug_assert_eq!(self.pending_reactive_token_refresh_id, Some(request_id));
        self.awaiting_token_refresh = false;
        self.pending_reactive_token_refresh_id = None;
    }

    pub(crate) fn authenticating_nothing_in_flight(&self) -> bool {
        self.state == ConnectionState::Authenticating
            && self.pending_subscribe_ack_count() == 0
            && self.pending_requests.is_empty()
            && !self.awaiting_token_refresh
    }

    #[cfg(test)]
    pub(crate) fn set_awaiting_token_refresh(&mut self, v: bool) {
        self.awaiting_token_refresh = v;
    }

    /// Drops a released key's reseed timer, deferred re-subscribe and initial replay; the caller disarms its ack.
    pub(crate) fn forget_released_key(
        &mut self,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) {
        self.disarm_book_reseed_snapshot(channel, pair.clone());
        self.remove_deferred_open_auth_subscribe(channel, pair);
        self.clear_initial_subscribe_replay(channel, std::slice::from_ref(pair));
    }

    /// A removed key's queued re-subscribe must die with it.
    pub(crate) fn remove_deferred_open_auth_subscribe(
        &mut self,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) {
        self.deferred_open_auth_subscribes
            .retain(|(c, p, _)| !(*c == channel && p == pair));
    }

    pub(crate) fn defer_open_auth_subscribe(
        &mut self,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
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
        Option<StreamSymbol>,
        crate::conn::subscription_registry::SubscribeParams,
    )> {
        std::mem::take(&mut self.deferred_open_auth_subscribes)
    }

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

    /// The error for a refused request: the recorded cause once failed, else `NotOpen`.
    pub(crate) fn refusal_error(&self) -> crate::error::ConnectionError {
        if self.state == ConnectionState::Failed {
            self.bus.failed_connection_error(self.url)
        } else {
            crate::error::ConnectionError::not_open()
        }
    }

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
        self.bus
            .record_connection_failure(url, non_transient_class, &last_error);
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

    pub(crate) fn pending_request_count(&self) -> usize {
        self.pending_requests.pending_request_count()
    }
}

#[cfg(test)]
impl ManagedConnection {
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
        pair: Option<StreamSymbol>,
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
