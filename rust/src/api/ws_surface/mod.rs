//! Internal `WsSurface` facade: handler registration, the `has_handlers` gate,
//! subscribe/unsubscribe, and subscription-mirror read models.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use tokio::sync::oneshot;

use crate::api::subscription_guard::SubscriptionGuard;
use crate::api::subscription_types::{
    SubscribeFailureCause, SubscriptionError, SubscriptionInfo, SubscriptionRef, SubscriptionState,
    SubscriptionsSummary,
};
use crate::auth::AuthStack;
use crate::clock::Clock;
use crate::conn::managed_connection::RequestHandle;
use crate::conn::subscription_registry::{MirrorRow, SubscriptionMirror, SubscriptionRegistry};
use crate::conn::{ConnectionSupervisor, SubscribeParams, SubscriptionEntry};
use crate::dispatch::handler_registry::presence_has_handlers;
use crate::dispatch::{
    CallerInbound, DispatchEventBus, HandlerCallback, HandlerHandle, HandlerId, HandlerMutationOp,
    PostReject, PresenceMirror, RegistryMutationOp, WsOp,
};
use crate::error::ConnectionError;
use crate::rate_limit::{ClOrdIdPairIndex, RateLimitExceeded, Scope, SpotTradingRateLimitTracker};
use crate::rest::RateLimitCost;
use crate::types::{ChannelName, ClOrdId, MonotonicInstant, Symbol, WsUrl};

mod connect;
mod guard;

pub(crate) use guard::SubscriberGuard;

type MirrorReadGuard<'a> = std::sync::RwLockReadGuard<
    'a,
    std::collections::HashMap<(ChannelName, Option<Symbol>), MirrorRow>,
>;

/// Internal WS facade. Constructed at `.build()`, shared as `Arc<WsSurface>`.
pub(crate) struct WsSurface {
    bus: Arc<DispatchEventBus>,
    /// Weak bus for HandlerHandle / SubscriptionGuard Drop (cycle-break).
    weak_bus: Weak<DispatchEventBus>,
    presence_mirror: PresenceMirror,
    subscription_mirror: SubscriptionMirror,
    handler_id_alloc: Arc<AtomicU64>,
    /// Monotonic request-ID allocator; starts at 1 (`0` reserved for unsubscribe).
    req_id_counter: Arc<AtomicU64>,
    /// `None` on the REST-only test path — yields typed `ConnectionError`, never panic.
    supervisor: Option<Arc<ConnectionSupervisor>>,
    /// Same tracker as RestSurface; `None` on the test-only `new()` path.
    trading_rate_limit: Option<Arc<SpotTradingRateLimitTracker>>,
    auth: Option<Arc<AuthStack>>,
    clock: Option<Arc<dyn Clock>>,
    /// Same index as RestSurface; `None` on the test-only path.
    cl_ord_id_index: Option<Arc<ClOrdIdPairIndex>>,
}

impl WsSurface {
    /// Construct without a supervisor (test-only).
    #[cfg(test)]
    pub(crate) fn new(
        bus: Arc<DispatchEventBus>,
        presence_mirror: PresenceMirror,
        handler_id_alloc: Arc<AtomicU64>,
    ) -> Self {
        Self::build(
            bus,
            presence_mirror,
            SubscriptionMirror::default(),
            handler_id_alloc,
            None,
            None,
            None,
            None,
            None,
        )
    }

    /// Construct with a supervisor but without `ClOrdIdPairIndex` (test-only).
    #[cfg(test)]
    pub(crate) fn new_with_supervisor(
        bus: Arc<DispatchEventBus>,
        presence_mirror: PresenceMirror,
        handler_id_alloc: Arc<AtomicU64>,
        supervisor: Arc<ConnectionSupervisor>,
        trading_rate_limit: Arc<SpotTradingRateLimitTracker>,
        auth: Arc<AuthStack>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self::build(
            bus,
            presence_mirror,
            SubscriptionMirror::default(),
            handler_id_alloc,
            Some(supervisor),
            Some(trading_rate_limit),
            Some(auth),
            Some(clock),
            None,
        )
    }

    /// Full production constructor with shared `ClOrdIdPairIndex`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_supervisor_and_index(
        bus: Arc<DispatchEventBus>,
        presence_mirror: PresenceMirror,
        subscription_mirror: SubscriptionMirror,
        handler_id_alloc: Arc<AtomicU64>,
        supervisor: Arc<ConnectionSupervisor>,
        trading_rate_limit: Arc<SpotTradingRateLimitTracker>,
        auth: Arc<AuthStack>,
        clock: Arc<dyn Clock>,
        cl_ord_id_index: Arc<ClOrdIdPairIndex>,
    ) -> Self {
        Self::build(
            bus,
            presence_mirror,
            subscription_mirror,
            handler_id_alloc,
            Some(supervisor),
            Some(trading_rate_limit),
            Some(auth),
            Some(clock),
            Some(cl_ord_id_index),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        bus: Arc<DispatchEventBus>,
        presence_mirror: PresenceMirror,
        subscription_mirror: SubscriptionMirror,
        handler_id_alloc: Arc<AtomicU64>,
        supervisor: Option<Arc<ConnectionSupervisor>>,
        trading_rate_limit: Option<Arc<SpotTradingRateLimitTracker>>,
        auth: Option<Arc<AuthStack>>,
        clock: Option<Arc<dyn Clock>>,
        cl_ord_id_index: Option<Arc<ClOrdIdPairIndex>>,
    ) -> Self {
        let weak_bus = Arc::downgrade(&bus);
        Self {
            bus,
            weak_bus,
            presence_mirror,
            subscription_mirror,
            handler_id_alloc,
            // Start at 1; `req_id: 0` is reserved for unsubscribe / "no id yet".
            req_id_counter: Arc::new(AtomicU64::new(1)),
            supervisor,
            trading_rate_limit,
            auth,
            clock,
            cl_ord_id_index,
        }
    }

    /// Register a type-erased callback for `channel`. On a failed post the handle
    /// is still returned but presence won't reflect it, so a later subscribe rejects.
    pub(crate) fn register_handler(
        &self,
        channel: ChannelName,
        cb: HandlerCallback,
    ) -> HandlerHandle {
        let id = self.register_handler_id(channel, cb);
        HandlerHandle::new(id, channel, self.weak_bus.clone())
    }

    /// Like `register_handler` but returns just the `HandlerId`. Bare `on_*` is
    /// infallible — a rejected post compensates the mirror and the id never enqueued.
    pub(crate) fn register_handler_id(
        &self,
        channel: ChannelName,
        cb: HandlerCallback,
    ) -> HandlerId {
        match self.register_handler_id_checked(channel, cb) {
            Ok(id) | Err((id, _)) => id,
        }
    }

    /// `on_*_for` funnel: a rejected Register aborts with typed error. Bumps
    /// presence synchronously; a failed post compensates.
    fn register_handler_id_checked(
        &self,
        channel: ChannelName,
        cb: HandlerCallback,
    ) -> Result<HandlerId, (HandlerId, SubscriptionError)> {
        self.bus.flush_pending_teardowns();
        let id = HandlerId(self.handler_id_alloc.fetch_add(1, Ordering::Relaxed));
        if let Ok(mut mirror) = self.presence_mirror.write() {
            *mirror.entry(channel).or_insert(0) += 1;
        }
        if let Err((_, e)) =
            self.bus
                .try_post_caller_inbound_recovering_kind(CallerInbound::HandlerMutation {
                    channel,
                    op: HandlerMutationOp::Register { id, callback: cb },
                })
        {
            // Register never enqueued — compensate the caller-side +1 so has_handlers
            // does not stay true for a channel with no live handler.
            if let Ok(mut mirror) = self.presence_mirror.write() {
                if let Some(count) = mirror.get_mut(&channel) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        mirror.remove(&channel);
                    }
                }
            }
            tracing::warn!(
                target: "kraken_sdk::ws_surface", channel = ?channel, error = ?e,
                "register_handler_id: HandlerMutation::Register post failed — handler will not receive updates"
            );
            return Err((id, self.post_reject_to_error(e)));
        }
        Ok(id)
    }

    /// Deregister handler `id` on `channel`.
    pub(crate) fn deregister_handler(&self, channel: ChannelName, id: HandlerId) {
        self.bus.post_teardown(CallerInbound::HandlerMutation {
            channel,
            op: HandlerMutationOp::Deregister { id },
        });
    }

    /// Weak bus ref for [`SubscriptionGuard`] Drop.
    pub(crate) fn weak_bus(&self) -> Weak<DispatchEventBus> {
        self.weak_bus.clone()
    }

    /// Claim the next monotonic request ID for a WS request/reply.
    pub(crate) fn next_req_id(&self) -> u64 {
        self.req_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Borrow the shared `ClOrdIdPairIndex` (`None` on the test-only path).
    pub(crate) fn cl_ord_id_index(&self) -> Option<&Arc<ClOrdIdPairIndex>> {
        self.cl_ord_id_index.as_ref()
    }

    /// Populate the index on a successful WS add-order accept.
    pub(crate) fn index_on_accept(
        &self,
        cl_ord_id: ClOrdId,
        pair: Symbol,
        sent_at: MonotonicInstant,
    ) {
        if let Some(idx) = &self.cl_ord_id_index {
            idx.insert(cl_ord_id, pair, sent_at);
        }
    }

    /// Post a WS request frame; `Some(reject)` when the frame never reached the
    /// reactor so a rate-limited caller can refund. Rejects resolve definitely-not-sent.
    fn send_request_inner(
        &self,
        method: String,
        req_id: u64,
        params: serde_json::Value,
    ) -> (RequestHandle, Option<PostReject>) {
        let (tx, rx) = oneshot::channel();
        // Non-trade method fallback; op only stamps the drain event.
        let op = WsOp::from_method(&method).unwrap_or(WsOp::AddOrder);
        // On reject: resolve completion directly as definitely-not-sent
        // (Full → queue_full, Closed → not_open), not ambiguous loop_closed.
        if let Err((CallerInbound::WsRequestFrame { completion, .. }, reject)) = self
            .bus
            .try_post_caller_inbound_recovering_kind(CallerInbound::WsRequestFrame {
                req_id,
                method,
                op,
                params,
                completion: tx,
            })
        {
            let err = match reject {
                PostReject::Full => ConnectionError::queue_full(),
                PostReject::Closed => ConnectionError::not_open(),
            };
            tracing::warn!(
                target: "kraken_sdk::ws_surface", req_id, ?reject,
                "send_request: WsRequestFrame post rejected (definitely-not-sent)"
            );
            let _ = completion.send(Err(err));
            return (RequestHandle::new(rx), Some(reject));
        }
        (RequestHandle::new(rx), None)
    }

    /// Rate-limited `send_request`: charges the same trading tracker as REST.
    /// Atomic consume rejects before posting; no-op when tracker/auth/clock are None.
    pub(crate) fn send_request_costed(
        &self,
        method: String,
        req_id: u64,
        params: serde_json::Value,
        cost: RateLimitCost,
    ) -> Result<RequestHandle, RateLimitExceeded> {
        // Charge before posting; remember so a reject-before-send can refund.
        let mut charged = None;
        if let (Some(tracker), Some(auth), Some(clock)) =
            (&self.trading_rate_limit, &self.auth, &self.clock)
        {
            if let Some(key) = auth.api_key() {
                let now = clock.now();
                match &cost {
                    RateLimitCost::Trading { cost, pair } => {
                        let scope = Scope::Pair(key.clone(), pair.clone());
                        tracker.consume(scope.clone(), *cost, now)?;
                        charged = Some((scope, *cost, now));
                    }
                    RateLimitCost::TradingAccountWide { cost } => {
                        // Account-wide charge (cancel_all); not refunded — saturating
                        // per-pair bump self-heals via decay.
                        tracker.charge_account_wide(Scope::ApiKey(key.clone()), *cost, now);
                    }
                    RateLimitCost::Api { .. } | RateLimitCost::None => {}
                }
            }
        }
        let (handle, reject) = self.send_request_inner(method, req_id, params);
        if reject.is_some() {
            if let (Some(tracker), Some((scope, cost, now))) = (&self.trading_rate_limit, charged) {
                // Frame never posted — reverse the pre-charge at the same instant.
                tracker.credit(scope, cost, now);
            }
        }
        Ok(handle)
    }

    /// Best-effort: drop `req_id`'s pending entry after a caller-side deadline timeout.
    pub(crate) fn abandon_request(&self, req_id: u64) {
        let _ = self
            .bus
            .try_post_caller_inbound(CallerInbound::AbandonWsRequest { req_id });
    }

    /// Returns `true` iff `channel` has at least one registered handler.
    pub(crate) fn has_handlers(&self, channel: ChannelName) -> bool {
        presence_has_handlers(&self.presence_mirror, channel)
    }

    /// Returns `true` iff a reactor loop has died. WS ops gate on this for typed
    /// `LoopDead`; REST ops are not gated.
    pub(crate) fn is_loop_failed(&self) -> bool {
        self.bus.is_loop_failed()
    }

    /// Map a rejected caller→I/O post to its typed error. Loop death outranks
    /// closed (a crash must never read as a clean close).
    fn post_reject_to_error(&self, reason: PostReject) -> SubscriptionError {
        match reason {
            PostReject::Full => SubscriptionError::QueueFull,
            PostReject::Closed if self.is_loop_failed() => SubscriptionError::LoopDead,
            PostReject::Closed => SubscriptionError::ClientClosed,
        }
    }

    /// Register and subscribe `channel` over `pairs` as one atomic `RegisterBatch`.
    pub(crate) fn subscribe(
        &self,
        channel: ChannelName,
        pairs: Vec<Symbol>,
        params: SubscribeParams,
    ) -> Result<(), SubscriptionError> {
        self.subscribe_with_ref(channel, pairs, params, None)
    }

    /// [`Self::subscribe`] with the registering guard's handler id attached so
    /// that guard's drop releases exactly the lifetime it registered.
    pub(crate) fn subscribe_with_ref(
        &self,
        channel: ChannelName,
        pairs: Vec<Symbol>,
        params: SubscribeParams,
        ref_id: Option<HandlerId>,
    ) -> Result<(), SubscriptionError> {
        // Shared choke for subscribe_* and on_*_for: dead reactor → LoopDead, not QueueFull.
        if self.is_loop_failed() {
            return Err(SubscriptionError::LoopDead);
        }
        self.bus.flush_pending_teardowns();
        let url = ws_url_for(channel);
        let entries: Vec<SubscriptionEntry> = if pairs.is_empty() {
            vec![SubscriptionEntry::new(url, channel, None, params)]
        } else {
            pairs
                .into_iter()
                .map(|pair| SubscriptionEntry::new(url, channel, Some(pair), params))
                .collect()
        };
        self.bus
            .try_post_caller_inbound_recovering_kind(CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::RegisterBatch { entries, ref_id },
            })
            .map_err(|(_, reason)| self.post_reject_to_error(reason))
    }

    /// `on_*_for` funnel: register, subscribe with guard ref, mint the guard.
    /// On reject the handler unwinds; a rejected unwind is retried on the next registry call.
    pub(crate) fn register_and_subscribe_guarded(
        &self,
        channel: ChannelName,
        pairs: Vec<Symbol>,
        params: SubscribeParams,
        cb: HandlerCallback,
    ) -> Result<SubscriptionGuard, SubscriptionError> {
        let id = self
            .register_handler_id_checked(channel, cb)
            .map_err(|(_, e)| e)?;
        if let Err(e) =
            self.subscribe_with_ref(channel.wire_channel(), pairs.clone(), params, Some(id))
        {
            // Register batch is atomic: on Err only the handler unwinds (unsubscribe
            // would bare-release other subscribers' refcounts).
            self.deregister_handler(channel, id);
            return Err(e);
        }
        Ok(SubscriptionGuard::new(id, channel, pairs, self.weak_bus()))
    }

    /// Deregister and unsubscribe `channel` over `pairs` as one atomic batch.
    pub(crate) fn unsubscribe(
        &self,
        channel: ChannelName,
        pairs: Vec<Symbol>,
    ) -> Result<(), SubscriptionError> {
        // Shared choke for unsubscribe_*: dead reactor → LoopDead, not QueueFull.
        if self.is_loop_failed() {
            return Err(SubscriptionError::LoopDead);
        }
        self.bus.flush_pending_teardowns();
        let url = ws_url_for(channel);
        if pairs.is_empty() {
            return self.unsubscribe_one(url, channel, None);
        }
        self.bus
            .try_post_caller_inbound_recovering_kind(CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::DeregisterBatch { channel, pairs },
            })
            .map_err(|(_, reason)| self.post_reject_to_error(reason))
    }

    fn unsubscribe_one(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pair: Option<Symbol>,
    ) -> Result<(), SubscriptionError> {
        // Refcount-decrement only; reactor sends wire unsubscribe only at refcount 0.
        self.bus
            .try_post_caller_inbound_recovering_kind(CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::Deregister { channel, pair },
            })
            .map_err(|(_, reason)| self.post_reject_to_error(reason))
    }

    /// Forced teardown of every subscription on `channel` (refcounts ignored).
    pub(crate) fn unsubscribe_channel(
        &self,
        channel: ChannelName,
    ) -> Result<(), SubscriptionError> {
        if self.is_loop_failed() {
            return Err(SubscriptionError::LoopDead);
        }
        self.bus.flush_pending_teardowns();
        self.post_deregister_all(Some(channel))
    }

    /// Forced teardown of every subscription across all channels.
    pub(crate) fn unsubscribe_all(&self) -> Result<(), SubscriptionError> {
        if self.is_loop_failed() {
            return Err(SubscriptionError::LoopDead);
        }
        self.bus.flush_pending_teardowns();
        self.post_deregister_all(None)
    }

    /// Post one atomic DeregisterAll; the reactor iterates the registry.
    fn post_deregister_all(&self, channel: Option<ChannelName>) -> Result<(), SubscriptionError> {
        self.bus
            .try_post_caller_inbound_recovering_kind(CallerInbound::RegistryMutation {
                // Wrapper url unused by DeregisterAll — per-entry urls come from each entry.
                url: WsUrl::Public,
                mutation: RegistryMutationOp::DeregisterAll { channel },
            })
            .map_err(|(_, reason)| self.post_reject_to_error(reason))
    }

    /// Shared mirror access for read models: dead/poisoned → LoopDead; closing → ClientClosed.
    fn read_model_mirror(&self) -> Result<MirrorReadGuard<'_>, SubscriptionError> {
        if self.is_loop_failed() {
            return Err(SubscriptionError::LoopDead);
        }
        if self.bus.is_shutting_down() {
            return Err(SubscriptionError::ClientClosed);
        }
        self.subscription_mirror
            .read()
            .map_err(|_| SubscriptionError::LoopDead)
    }

    /// Point-in-time snapshot of every entry (retained `Failed` rows included).
    pub(crate) fn list_active(&self) -> Result<Vec<SubscriptionInfo>, SubscriptionError> {
        let conn_failed = self.conn_failed_sample();
        let mut rows: Vec<SubscriptionInfo> = {
            let mirror = self.read_model_mirror()?;
            mirror
                .iter()
                .map(|((channel, pair), row)| SubscriptionInfo {
                    channel: *channel,
                    pair: pair.clone(),
                    state: Self::project_with_connection(
                        conn_failed,
                        *channel,
                        SubscriptionRegistry::project_state(&row.state),
                    ),
                })
                .collect()
        };
        rows.sort_by(|a, b| {
            subscription_order((a.channel, a.pair.as_ref()), (b.channel, b.pair.as_ref()))
        });
        Ok(rows)
    }

    /// Point-in-time refs for one channel. `BookRaw` returns shared `Book` entries.
    pub(crate) fn find_by_channel(
        &self,
        channel: ChannelName,
    ) -> Result<Vec<SubscriptionRef>, SubscriptionError> {
        let wire = channel.wire_channel();
        let conn_failed = self.conn_failed_sample();
        let mut rows: Vec<SubscriptionRef> = {
            let mirror = self.read_model_mirror()?;
            mirror
                .iter()
                .filter(|((ch, _), _)| *ch == wire)
                .map(|((ch, pair), row)| SubscriptionRef {
                    channel: *ch,
                    symbol: pair.clone(),
                    state: Self::project_with_connection(
                        conn_failed,
                        *ch,
                        SubscriptionRegistry::project_state(&row.state),
                    ),
                    registered_at_monotonic: row.registered_at.as_duration().as_millis() as u64,
                })
                .collect()
        };
        rows.sort_by(|a, b| {
            subscription_order(
                (a.channel, a.symbol.as_ref()),
                (b.channel, b.symbol.as_ref()),
            )
        });
        Ok(rows)
    }

    /// Point-in-time per-state counts and per-channel histogram.
    pub(crate) fn status_summary(&self) -> Result<SubscriptionsSummary, SubscriptionError> {
        let mut summary = SubscriptionsSummary {
            total: 0,
            active: 0,
            pending: 0,
            failed: 0,
            by_channel: std::collections::HashMap::new(),
        };
        let conn_failed = self.conn_failed_sample();
        {
            let mirror = self.read_model_mirror()?;
            for ((channel, _), row) in mirror.iter() {
                summary.total += 1;
                // Same projection as list_active so Failed connections count as failed.
                match Self::project_with_connection(
                    conn_failed,
                    *channel,
                    SubscriptionRegistry::project_state(&row.state),
                ) {
                    SubscriptionState::Active => summary.active += 1,
                    SubscriptionState::Pending => summary.pending += 1,
                    SubscriptionState::Failed(_) => summary.failed += 1,
                }
                *summary.by_channel.entry(*channel).or_insert(0) += 1;
            }
        }
        Ok(summary)
    }

    #[cfg(test)]
    pub(crate) fn subscription_mirror_handle(&self) -> SubscriptionMirror {
        Arc::clone(&self.subscription_mirror)
    }

    /// Live row on a terminally Failed connection → Failed(ConnectionFailed).
    /// Entry-local Terminated causes win.
    fn project_with_connection(
        conn_failed: (bool, bool),
        channel: ChannelName,
        state: SubscriptionState,
    ) -> SubscriptionState {
        if matches!(state, SubscriptionState::Failed(_)) {
            return state;
        }
        let failed = match ws_url_for(channel) {
            WsUrl::Public => conn_failed.0,
            WsUrl::Auth => conn_failed.1,
        };
        if failed {
            SubscriptionState::Failed(SubscribeFailureCause::ConnectionFailed)
        } else {
            state
        }
    }

    /// Sampled once per read-model call: `(public failed, auth failed)`.
    fn conn_failed_sample(&self) -> (bool, bool) {
        let failed = |url: WsUrl| {
            self.supervisor
                .as_ref()
                .is_some_and(|s| s.current_state(url) == crate::types::ConnectionState::Failed)
        };
        (failed(WsUrl::Public), failed(WsUrl::Auth))
    }
}

/// Compare read-model rows by subscription replay order.
fn subscription_order(
    a: (ChannelName, Option<&crate::types::Symbol>),
    b: (ChannelName, Option<&crate::types::Symbol>),
) -> std::cmp::Ordering {
    crate::conn::subscription_registry::subscription_order_key(a.0, a.1).cmp(
        &crate::conn::subscription_registry::subscription_order_key(b.0, b.1),
    )
}

/// Auth URL for executions/balances; public URL otherwise.
fn ws_url_for(channel: ChannelName) -> WsUrl {
    match channel {
        ChannelName::Executions | ChannelName::Balances => WsUrl::Auth,
        _ => WsUrl::Public,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::SystemClock;
    use crate::dispatch::DispatchEventBusConfig;

    fn make_surface() -> (Arc<WsSurface>, Arc<DispatchEventBus>, PresenceMirror) {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            clock,
        ));
        let mirror: PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let alloc = Arc::new(AtomicU64::new(1));
        let surface = Arc::new(WsSurface::new(Arc::clone(&bus), Arc::clone(&mirror), alloc));
        (surface, bus, mirror)
    }

    #[test]
    fn rejected_deregister_is_retried_on_the_next_registry_call() {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig {
                caller_to_io_capacity: 2,
                ..DispatchEventBusConfig::defaults()
            },
            clock,
        ));
        let mirror: PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let alloc = Arc::new(AtomicU64::new(1));
        let surface = Arc::new(WsSurface::new(Arc::clone(&bus), Arc::clone(&mirror), alloc));
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");

        for _ in 0..2 {
            bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
                channel: ChannelName::Ticker,
                op: HandlerMutationOp::Deregister { id: HandlerId(999) },
            })
            .expect("filler post fits");
        }
        surface.deregister_handler(ChannelName::Executions, HandlerId(7));

        let _ = rx.try_recv().expect("filler drained");
        let _ = rx.try_recv().expect("filler drained");
        surface.deregister_handler(ChannelName::Executions, HandlerId(8));

        let first = rx.try_recv().expect("the queued deregister is flushed");
        assert!(
            matches!(
                first,
                CallerInbound::HandlerMutation {
                    channel: ChannelName::Executions,
                    op: HandlerMutationOp::Deregister { id: HandlerId(7) },
                }
            ),
            "the previously rejected deregister must land first, got {first:?}"
        );
        let second = rx.try_recv().expect("the current deregister follows");
        assert!(matches!(
            second,
            CallerInbound::HandlerMutation {
                op: HandlerMutationOp::Deregister { id: HandlerId(8) },
                ..
            }
        ));
    }

    #[test]
    fn subscribe_flushes_a_pending_deregister_first() {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig {
                caller_to_io_capacity: 2,
                ..DispatchEventBusConfig::defaults()
            },
            clock,
        ));
        let mirror: PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let alloc = Arc::new(AtomicU64::new(1));
        let surface = Arc::new(WsSurface::new(Arc::clone(&bus), Arc::clone(&mirror), alloc));
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");

        for _ in 0..2 {
            bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
                channel: ChannelName::Ticker,
                op: HandlerMutationOp::Deregister { id: HandlerId(999) },
            })
            .expect("filler post fits");
        }
        surface.deregister_handler(ChannelName::Executions, HandlerId(7));
        let _ = rx.try_recv().expect("filler drained");
        let _ = rx.try_recv().expect("filler drained");

        surface
            .subscribe(
                ChannelName::Ticker,
                vec![Symbol::new("BTC/USD").unwrap()],
                SubscribeParams::Ticker {
                    snapshot: None,
                    event_trigger: None,
                },
            )
            .expect("subscribe posts");
        let first = rx.try_recv().expect("flushed deregister lands first");
        assert!(
            matches!(
                first,
                CallerInbound::HandlerMutation {
                    op: HandlerMutationOp::Deregister { id: HandlerId(7) },
                    ..
                }
            ),
            "expected the queued deregister before the subscribe, got {first:?}"
        );
        assert!(matches!(
            rx.try_recv().expect("the subscribe follows"),
            CallerInbound::RegistryMutation { .. }
        ));
    }

    #[test]
    fn guard_drop_when_full_lands_on_next_flush() {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig {
                caller_to_io_capacity: 1,
                ..DispatchEventBusConfig::defaults()
            },
            clock,
        ));
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");

        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            channel: ChannelName::Ticker,
            op: HandlerMutationOp::Deregister { id: HandlerId(999) },
        })
        .expect("filler occupies the only slot");

        let guard = SubscriptionGuard::new(
            HandlerId(42),
            ChannelName::Ticker,
            vec![Symbol::new("BTC/USD").unwrap()],
            Arc::downgrade(&bus),
        );
        drop(guard);

        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());

        bus.flush_pending_teardowns();

        match rx.try_recv().expect("deferred SubscriptionGuardDrop lands") {
            CallerInbound::SubscriptionGuardDrop {
                handler_id,
                channel,
                symbols,
            } => {
                assert_eq!(handler_id, HandlerId(42));
                assert_eq!(channel, ChannelName::Ticker);
                assert_eq!(symbols.len(), 1);
            }
            other => panic!("expected SubscriptionGuardDrop, got {other:?}"),
        }
    }

    #[test]
    fn ws_url_routing_auth_vs_public() {
        assert_eq!(ws_url_for(ChannelName::Ticker), WsUrl::Public);
        assert_eq!(ws_url_for(ChannelName::Book), WsUrl::Public);
        assert_eq!(ws_url_for(ChannelName::Executions), WsUrl::Auth);
        assert_eq!(ws_url_for(ChannelName::Balances), WsUrl::Auth);
    }

    #[test]
    fn subscribe_posts_one_register_batch() {
        let (surface, bus, _mirror) = make_surface();
        let rx = bus.take_caller_to_io_rx().expect("rx available");

        surface
            .subscribe(
                ChannelName::Ticker,
                vec![
                    Symbol::new("BTC/USD").unwrap(),
                    Symbol::new("ETH/USD").unwrap(),
                    Symbol::new("SOL/USD").unwrap(),
                ],
                SubscribeParams::Ticker {
                    snapshot: None,
                    event_trigger: None,
                },
            )
            .expect("subscribe posts");

        let mut rx = rx;
        let first = rx.try_recv().expect("one message posted");
        match first {
            CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::RegisterBatch { entries, .. },
            } => {
                assert_eq!(url, WsUrl::Public);
                assert_eq!(entries.len(), 3, "all three pairs in ONE atomic batch");
            }
            other => panic!("expected a single RegisterBatch, got {other:?}"),
        }
        assert!(
            rx.try_recv().is_err(),
            "subscribe must post EXACTLY ONE message (no per-pair Register loop)"
        );
    }

    #[test]
    fn channel_wide_subscribe_posts_one_register_batch_single_entry() {
        let (surface, bus, _mirror) = make_surface();
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");

        surface
            .subscribe(ChannelName::Balances, Vec::new(), SubscribeParams::Balances)
            .expect("subscribe posts");

        match rx.try_recv().expect("one message posted") {
            CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::RegisterBatch { entries, .. },
            } => {
                assert_eq!(url, WsUrl::Auth, "balances routes to the auth URL");
                assert_eq!(entries.len(), 1, "one channel-wide entry");
                assert!(
                    entries[0].pair.is_none(),
                    "channel-wide entry has pair = None"
                );
            }
            other => panic!("expected a single RegisterBatch, got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "exactly one message");
    }

    #[test]
    fn unsubscribe_posts_one_deregister_batch() {
        let (surface, bus, _mirror) = make_surface();
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");

        surface
            .unsubscribe(
                ChannelName::Ticker,
                vec![
                    Symbol::new("BTC/USD").unwrap(),
                    Symbol::new("ETH/USD").unwrap(),
                    Symbol::new("SOL/USD").unwrap(),
                ],
            )
            .expect("unsubscribe posts");

        match rx.try_recv().expect("one message posted") {
            CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::DeregisterBatch { channel, pairs },
            } => {
                assert_eq!(url, WsUrl::Public);
                assert_eq!(channel, ChannelName::Ticker);
                assert_eq!(pairs.len(), 3, "all three pairs in ONE atomic batch");
            }
            other => panic!("expected a single DeregisterBatch, got {other:?}"),
        }
        assert!(
            rx.try_recv().is_err(),
            "unsubscribe must post EXACTLY ONE message (no per-pair Deregister loop)"
        );
    }

    #[test]
    fn channel_wide_unsubscribe_posts_one_deregister_none() {
        let (surface, bus, _mirror) = make_surface();
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");

        surface
            .unsubscribe(ChannelName::Balances, Vec::new())
            .expect("unsubscribe posts");

        match rx.try_recv().expect("one message posted") {
            CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::Deregister { channel, pair },
            } => {
                assert_eq!(url, WsUrl::Auth, "balances routes to the auth URL");
                assert_eq!(channel, ChannelName::Balances);
                assert!(pair.is_none(), "channel-wide deregister has pair = None");
            }
            other => panic!("expected a single Deregister, got {other:?}"),
        }
        assert!(rx.try_recv().is_err(), "exactly one message");
    }

    #[test]
    fn register_handler_bumps_presence_mirror_and_returns_handle() {
        let (surface, _bus, _mirror) = make_surface();
        assert!(!surface.has_handlers(ChannelName::Ticker));
        let cb: HandlerCallback = HandlerCallback::noop();
        let handle = surface.register_handler(ChannelName::Ticker, cb);
        assert_eq!(handle.channel(), ChannelName::Ticker);
        assert!(
            surface.has_handlers(ChannelName::Ticker),
            "presence must be true immediately after register"
        );
        let cb2: HandlerCallback = HandlerCallback::noop();
        let handle2 = surface.register_handler(ChannelName::Ticker, cb2);
        assert!(handle2.id().raw() > handle.id().raw());
        drop(handle);
        drop(handle2);
    }

    #[test]
    fn register_handler_presence_visible_synchronously_no_await() {
        let (surface, _bus, _mirror) = make_surface();
        assert!(
            !surface.has_handlers(ChannelName::Trade),
            "baseline: no handler yet"
        );
        let cb: HandlerCallback = HandlerCallback::noop();
        let _handle = surface.register_handler(ChannelName::Trade, cb);
        assert!(
            surface.has_handlers(ChannelName::Trade),
            "presence must be true immediately — gate must not spuriously reject"
        );
    }

    #[test]
    fn register_then_drop_handle_does_not_corrupt_mirror() {
        let (surface, _bus, mirror) = make_surface();
        let cb: HandlerCallback = HandlerCallback::noop();
        let handle = surface.register_handler(ChannelName::Book, cb);
        assert!(
            surface.has_handlers(ChannelName::Book),
            "present after register"
        );
        drop(handle);
        let count = mirror
            .read()
            .unwrap()
            .get(&ChannelName::Book)
            .copied()
            .unwrap_or(0);
        assert_eq!(
            count, 1,
            "mirror stays +1 until reactor drains Deregister (no corruption)"
        );
    }

    #[test]
    fn two_handlers_same_channel_count_is_two() {
        let (surface, _bus, mirror) = make_surface();
        let cb1: HandlerCallback = HandlerCallback::noop();
        let cb2: HandlerCallback = HandlerCallback::noop();
        let _h1 = surface.register_handler(ChannelName::Ohlc, cb1);
        let _h2 = surface.register_handler(ChannelName::Ohlc, cb2);
        assert!(
            surface.has_handlers(ChannelName::Ohlc),
            "presence true with two handlers"
        );
        let count = mirror
            .read()
            .unwrap()
            .get(&ChannelName::Ohlc)
            .copied()
            .unwrap_or(0);
        assert_eq!(count, 2, "mirror count = 2 for two handlers");
    }

    #[test]
    fn read_models_project_connection_failed_for_live_rows() {
        use crate::auth::{AuthStack, SystemClockNonceSource, TokenLifecycleManager};
        use crate::conn::ConnectionSupervisor;
        use crate::conn::subscription_registry::{EntrySubState, MirrorRow};
        use crate::types::{ConnectionState, MonotonicInstant};
        use std::collections::HashMap;

        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let auth = Arc::new(AuthStack::new(
            None,
            None,
            Arc::new(SystemClockNonceSource::new()),
            HashMap::new(),
            TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
        ));
        let supervisor = Arc::new(ConnectionSupervisor::new(Arc::clone(&bus)));
        let presence: PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let alloc = Arc::new(AtomicU64::new(1));
        let trading_rl = Arc::new(crate::rate_limit::SpotTradingRateLimitTracker::new(
            crate::rate_limit::Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let surface = WsSurface::new_with_supervisor(
            Arc::clone(&bus),
            presence,
            alloc,
            Arc::clone(&supervisor),
            trading_rl,
            auth,
            clock,
        );
        {
            let mirror = surface.subscription_mirror_handle();
            let mut m = mirror.write().unwrap();
            m.insert(
                (ChannelName::Executions, None),
                MirrorRow {
                    state: EntrySubState::Pending,
                    registered_at: MonotonicInstant::now(),
                },
            );
            m.insert(
                (ChannelName::Ticker, Some(Symbol::new("BTC/USD").unwrap())),
                MirrorRow {
                    state: EntrySubState::Acked,
                    registered_at: MonotonicInstant::now(),
                },
            );
        }
        supervisor.state_mirror_clones()[&WsUrl::Auth]
            .store(ConnectionState::Failed.as_u8(), Ordering::Release);

        let rows = surface.list_active().expect("list_active");
        let exec = rows
            .iter()
            .find(|r| r.channel == ChannelName::Executions)
            .expect("executions row");
        assert!(
            matches!(
                exec.state,
                SubscriptionState::Failed(SubscribeFailureCause::ConnectionFailed)
            ),
            "live row on a Failed connection projects Failed(ConnectionFailed), got {:?}",
            exec.state
        );
        let tick = rows
            .iter()
            .find(|r| r.channel == ChannelName::Ticker)
            .expect("ticker row");
        assert!(matches!(tick.state, SubscriptionState::Active));
        let summary = surface.status_summary().expect("summary");
        assert_eq!(
            (summary.failed, summary.active, summary.pending),
            (1, 1, 0),
            "summary routes through the same projection"
        );
    }

    #[tokio::test]
    async fn ws_op_before_ready_fails_fast_not_open_no_30s_stall() {
        use crate::auth::{AuthStack, SystemClockNonceSource, TokenLifecycleManager};
        use crate::conn::ConnectionSupervisor;
        use crate::types::ConnectionState;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicU8;
        use std::time::Duration;

        const TEST_BUDGET: Duration = Duration::from_millis(1500);

        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let auth = Arc::new(AuthStack::new(
            None,
            None,
            Arc::new(SystemClockNonceSource::new()),
            HashMap::new(),
            TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
        ));
        let supervisor = Arc::new(ConnectionSupervisor::new(Arc::clone(&bus)));
        let auth_mirror = Arc::clone(&supervisor.state_mirror_clones()[&WsUrl::Auth]);
        let mirror: PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let alloc = Arc::new(AtomicU64::new(1));
        let trading_rl = Arc::new(crate::rate_limit::SpotTradingRateLimitTracker::new(
            crate::rate_limit::Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let surface = Arc::new(WsSurface::new_with_supervisor(
            Arc::clone(&bus),
            mirror,
            alloc,
            Arc::clone(&supervisor),
            trading_rl,
            auth,
            clock,
        ));

        // I/O reactor never started; auth Idle.
        auth_mirror.store(ConnectionState::Idle.as_u8(), Ordering::Release);

        let started = std::time::Instant::now();
        let res = tokio::time::timeout(TEST_BUDGET, surface.ensure_order_sendable(WsUrl::Auth))
            .await
            .expect("must fail FAST (well under the 30s ENSURE_OPEN_TIMEOUT) — NO stall");
        let elapsed = started.elapsed();

        let err = res.expect_err("WS op before `.ready()` must Err, not Ok");
        assert_eq!(
            err.kind(),
            crate::error::ConnectionErrorKind::NotOpen,
            "reactor-not-started fast-path reuses the NotOpen variant"
        );
        assert!(
            err.is_definitely_not_sent(),
            "nothing was sent — the request was rejected before any wire write"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "fast-path must not approach the 30s backstop (elapsed: {elapsed:?})"
        );
        let _ = AtomicU8::new(0); // suppress unused import lint
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn send_request_queue_closed_resolves_not_open_not_loop_closed() {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig {
                caller_to_io_capacity: 1,
                ..DispatchEventBusConfig::defaults()
            },
            clock,
        ));
        let _rx = bus.take_caller_to_io_rx();
        drop(_rx);

        let mirror: PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let alloc = Arc::new(AtomicU64::new(1));
        let surface = WsSurface::new(Arc::clone(&bus), mirror, alloc);

        let (handle, reject) =
            surface.send_request_inner("addOrder".to_string(), 1, serde_json::json!({}));
        assert_eq!(
            reject,
            Some(PostReject::Closed),
            "closed queue must surface a reject"
        );
        let err = tokio::time::timeout(std::time::Duration::from_millis(100), handle.recv())
            .await
            .expect("recv must resolve immediately — handle pre-resolved at send_request call site")
            .expect_err("closed queue → Err(ConnectionError)");

        assert_eq!(
            err.kind(),
            crate::error::ConnectionErrorKind::NotOpen,
            "pre-record queue-full rejection must be NotOpen (definitely-not-sent), not LoopClosed (ambiguous)"
        );
        assert!(
            err.is_definitely_not_sent(),
            "is_definitely_not_sent() must return true for a pre-record queue-full rejection"
        );
    }

    #[tokio::test]
    async fn send_request_queue_full_resolves_queue_full_not_loop_closed() {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig {
                caller_to_io_capacity: 1,
                ..DispatchEventBusConfig::defaults()
            },
            clock,
        ));
        let fill_result = bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: crate::types::WsUrl::Auth,
            event: crate::types::CallerEvent::StartConnect { request_id: 0 },
        });
        assert!(
            fill_result.is_ok(),
            "first post must succeed to fill the queue"
        );

        let mirror: PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let alloc = Arc::new(AtomicU64::new(1));
        let surface = WsSurface::new(Arc::clone(&bus), mirror, alloc);

        let (handle, reject) =
            surface.send_request_inner("addOrder".to_string(), 2, serde_json::json!({}));
        assert_eq!(
            reject,
            Some(PostReject::Full),
            "full queue must surface a reject"
        );
        let err = tokio::time::timeout(std::time::Duration::from_millis(100), handle.recv())
            .await
            .expect("recv must resolve immediately — handle pre-resolved at send_request call site")
            .expect_err("full queue → Err(ConnectionError)");

        assert_eq!(
            err.kind(),
            crate::error::ConnectionErrorKind::QueueFull,
            "pre-record queue-full (Full) must be QueueFull (back-pressure, definitely-not-sent)"
        );
        assert!(
            err.is_definitely_not_sent(),
            "is_definitely_not_sent() must return true for TrySendError::Full pre-record rejection"
        );
    }

    #[test]
    fn ws_conn_error_to_trade_maps_queue_full_to_typed_queue_full() {
        use crate::api::trade::ws_conn_error_to_trade;
        use crate::error::{ApiError, ConnectionError, ErrorCategory};

        let te = ws_conn_error_to_trade(ConnectionError::queue_full());
        assert!(
            matches!(te, crate::api::trade::TradeError::QueueFull { .. }),
            "full-queue ConnectionError must convert to TradeError::QueueFull"
        );
        assert_eq!(te.code(), "QUEUE_FULL");
        assert_eq!(te.category(), ErrorCategory::Client);
        assert!(
            te.retryable(),
            "QueueFull is retryable (caller may re-issue)"
        );
    }

    #[test]
    fn ws_conn_error_to_trade_keeps_closed_as_transport() {
        use crate::api::trade::ws_conn_error_to_trade;
        use crate::error::ConnectionError;

        let te = ws_conn_error_to_trade(ConnectionError::not_open());
        assert!(
            matches!(te, crate::api::trade::TradeError::Transport { .. }),
            "a closed/not-open ConnectionError must remain TradeError::Transport, not QueueFull"
        );
    }

    fn make_costed_surface() -> (
        WsSurface,
        Arc<crate::rate_limit::SpotTradingRateLimitTracker>,
        tokio::sync::mpsc::Receiver<CallerInbound>,
        crate::types::ApiKey,
    ) {
        use crate::auth::{
            AuthStack, SpotRestHmacSha512Signer, SystemClockNonceSource, TokenLifecycleManager,
        };
        use crate::rate_limit::{SpotTradingRateLimitTracker, Tier};
        use crate::types::{ApiKey, ApiSecret};
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD as BASE64;
        use std::collections::HashMap;

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let rx = bus.take_caller_to_io_rx().expect("rx available");

        let api_key = ApiKey::new("test-key-costed");
        let secret = ApiSecret::from_base64(&BASE64.encode(vec![0x02u8; 32])).unwrap();
        let signer = SpotRestHmacSha512Signer::new(api_key.clone(), secret);
        let mut signers: HashMap<crate::types::AuthProfile, Arc<dyn crate::auth::AuthSigner>> =
            HashMap::new();
        signers.insert(crate::types::AuthProfile::SpotV1, Arc::new(signer));
        let auth = Arc::new(AuthStack::new(
            Some(api_key.clone()),
            None,
            Arc::new(SystemClockNonceSource::new()),
            signers,
            TokenLifecycleManager::for_test(),
        ));

        let tracker = Arc::new(SpotTradingRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));

        let mirror: PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let alloc = Arc::new(AtomicU64::new(1));

        let surface = WsSurface::build(
            Arc::clone(&bus),
            mirror,
            SubscriptionMirror::default(),
            alloc,
            None,
            Some(Arc::clone(&tracker)),
            Some(auth),
            Some(clock),
            None,
        );

        (surface, tracker, rx, api_key)
    }

    #[test]
    fn ws_order_rejected_before_send_refunds_the_pre_charge() {
        use crate::rate_limit::Scope;
        use crate::rest::RateLimitCost;

        let (surface, tracker, rx, api_key) = make_costed_surface();
        let pair = Symbol::new("BTC/USD").unwrap();
        let now = crate::types::MonotonicInstant::now();

        tracker
            .consume(Scope::Pair(api_key.clone(), pair.clone()), 5.0, now)
            .unwrap();

        drop(rx);

        let handle = surface
            .send_request_costed(
                "addOrder".to_string(),
                1,
                serde_json::json!({}),
                RateLimitCost::Trading {
                    cost: 1.0,
                    pair: pair.clone(),
                },
            )
            .expect("send_request_costed returns Ok even when the post is rejected");
        drop(handle);

        let h = tracker.headroom(Scope::Pair(api_key, pair), now).unwrap();
        assert!(
            (h - 55.0).abs() < 0.1,
            "reject-before-send must refund the pre-charge (headroom ~55), got {h}"
        );
    }

    #[test]
    fn ws_add_order_charges_trading_tracker_and_rejects_at_cap() {
        use crate::rate_limit::Scope;
        use crate::rest::RateLimitCost;

        let (surface, tracker, mut rx, api_key) = make_costed_surface();
        let pair = Symbol::new("BTC/USD").unwrap();
        let now = crate::types::MonotonicInstant::now();

        for _ in 0..60 {
            tracker
                .consume(Scope::Pair(api_key.clone(), pair.clone()), 1.0, now)
                .expect("consume within cap must succeed");
        }

        let result = surface.send_request_costed(
            "addOrder".to_string(),
            1,
            serde_json::json!({}),
            RateLimitCost::Trading {
                cost: 1.0,
                pair: pair.clone(),
            },
        );
        match result {
            Err(e) => {
                assert_eq!(e.tracker, "trading", "rejected by the trading tracker");
                match e.scope {
                    Scope::Pair(_, p) => assert_eq!(p.as_str(), "BTC/USD"),
                    other => panic!("expected Pair scope, got {:?}", other),
                }
            }
            Ok(_) => panic!("send_request_costed must return Err when trading cap is exhausted"),
        }

        match rx.try_recv() {
            Err(_) => {}
            Ok(msg) => panic!(
                "WsRequestFrame was posted despite rate-limit rejection: {:?}",
                msg
            ),
        }
    }
}
