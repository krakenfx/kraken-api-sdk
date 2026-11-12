//! Internal `WsSurface` facade: handler registration, the `require_handler` gate,
//! subscribe/unsubscribe, and subscription-mirror read models.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use tokio::sync::oneshot;

use crate::api::subscription_guard::{
    SubscriptionGuard, UnifiedBookIntent, UnifiedBookIntents, UnifiedBookReservation,
};
use crate::api::subscription_types::{
    FuturesSubscriptionInfo, FuturesSubscriptionRef, SubscribeFailureCause, SubscriptionError,
    SubscriptionInfo, SubscriptionRef, SubscriptionState, SubscriptionsSummary,
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
use crate::types::{ChannelName, ClOrdId, MonotonicInstant, StreamSymbol, Symbol, WsUrl};

mod connect;
mod guard;

pub(crate) use guard::SubscriberGuard;

type MirrorReadGuard<'a> = std::sync::RwLockReadGuard<
    'a,
    std::collections::HashMap<(WsUrl, ChannelName, Option<StreamSymbol>), MirrorRow>,
>;

/// Internal WS facade. Constructed at `.build()`, shared as `Arc<WsSurface>`.
pub(crate) struct WsSurface {
    bus: Arc<DispatchEventBus>,
    /// Weak bus for HandlerHandle / SubscriptionGuard Drop (cycle-break).
    weak_bus: Weak<DispatchEventBus>,
    presence_mirror: PresenceMirror,
    subscription_mirror: SubscriptionMirror,
    unified_book_intents: UnifiedBookIntents,
    unified_book_generation: AtomicU64,
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

    /// Default-config bus plus a supervisor-less surface on it; the
    /// caller-to-io receiver is still untaken (test-only).
    #[cfg(test)]
    pub(crate) fn on_default_bus() -> (Arc<DispatchEventBus>, Arc<Self>) {
        Self::on_bus_with_caller_capacity(
            crate::dispatch::DispatchEventBusConfig::defaults().caller_to_io_capacity,
        )
    }

    #[cfg(test)]
    pub(crate) fn on_bus_with_caller_capacity(
        capacity: usize,
    ) -> (Arc<DispatchEventBus>, Arc<Self>) {
        let bus = Arc::new(DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig {
                caller_to_io_capacity: capacity,
                ..crate::dispatch::DispatchEventBusConfig::defaults()
            },
            Arc::new(crate::clock::SystemClock),
        ));
        let ws = Arc::new(Self::new(
            Arc::clone(&bus),
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
            Arc::new(AtomicU64::new(1)),
        ));
        (bus, ws)
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
            unified_book_intents: Arc::default(),
            unified_book_generation: AtomicU64::new(1),
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

    /// Register a type-erased callback for `channel` on `url`; a rejected post
    /// returns the typed error.
    pub(crate) fn register_handler_for_checked(
        &self,
        url: WsUrl,
        channel: ChannelName,
        cb: HandlerCallback,
    ) -> Result<HandlerHandle, SubscriptionError> {
        self.register_handler_id_checked_for(url, channel, cb)
            .map(|id| HandlerHandle::new(id, url, channel, self.weak_bus.clone()))
            .map_err(|(_, error, _)| error)
    }

    fn register_handler_id_checked_for(
        &self,
        url: WsUrl,
        channel: ChannelName,
        cb: HandlerCallback,
    ) -> Result<HandlerId, (HandlerId, SubscriptionError, Box<CallerInbound>)> {
        self.post_handler_registration(url, channel, |id| CallerInbound::HandlerMutation {
            url,
            channel,
            op: HandlerMutationOp::Register { id, callback: cb },
        })
    }

    /// Allocate an id, bump caller-side presence and post `inbound(id)`; a rejected
    /// post compensates presence and returns a typed error with the rejected inbound.
    fn post_handler_registration(
        &self,
        url: WsUrl,
        channel: ChannelName,
        inbound: impl FnOnce(HandlerId) -> CallerInbound,
    ) -> Result<HandlerId, (HandlerId, SubscriptionError, Box<CallerInbound>)> {
        let id = HandlerId(self.handler_id_alloc.fetch_add(1, Ordering::Relaxed));
        if self.is_loop_failed() {
            return Err((id, SubscriptionError::LoopDead, Box::new(inbound(id))));
        }
        self.bus.flush_pending_teardowns();
        if let Ok(mut mirror) = self.presence_mirror.write() {
            *mirror.entry((url, channel)).or_insert(0) += 1;
        }
        if let Err((rejected, e)) = self
            .bus
            .try_post_caller_inbound_recovering_kind(inbound(id))
        {
            // Register never enqueued — compensate the caller-side +1 so has_handlers_for
            // does not stay true for a channel with no live handler.
            if let Ok(mut mirror) = self.presence_mirror.write()
                && let Some(count) = mirror.get_mut(&(url, channel))
            {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    mirror.remove(&(url, channel));
                }
            }
            let error = self.post_reject_to_error(e);
            tracing::warn!(
                target: "kraken_sdk::ws_surface", channel = ?channel, error = ?error,
                "handler registration post failed — handler will not receive updates"
            );
            return Err((id, error, Box::new(rejected)));
        }
        Ok(id)
    }

    /// Register Unified unsolicited status and start its lazy socket in one I/O
    /// command. A rejected post compensates presence and returns a typed error.
    pub(crate) fn register_unified_status_handler(
        &self,
        cb: HandlerCallback,
    ) -> Result<HandlerHandle, SubscriptionError> {
        let url = WsUrl::UnifiedPublic;
        let channel = ChannelName::Status;
        self.post_handler_registration(url, channel, |id| {
            CallerInbound::RegisterHandlerAndConnect {
                url,
                channel,
                id,
                callback: cb,
            }
        })
        .map(|id| HandlerHandle::new(id, url, channel, self.weak_bus.clone()))
        .map_err(|(_, error, _)| error)
    }

    /// URL-explicit internal deregistration used by product-specific surfaces.
    pub(crate) fn deregister_handler_for(&self, url: WsUrl, channel: ChannelName, id: HandlerId) {
        self.bus.post_teardown(CallerInbound::HandlerMutation {
            url,
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
        url: WsUrl,
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
                url,
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
        url: WsUrl,
        method: String,
        req_id: u64,
        params: serde_json::Value,
        cost: RateLimitCost,
    ) -> Result<RequestHandle, RateLimitExceeded> {
        // Charge before posting; remember so a reject-before-send can refund.
        let mut charged = None;
        if let (Some(tracker), Some(auth), Some(clock)) =
            (&self.trading_rate_limit, &self.auth, &self.clock)
            && let Some(key) = auth.api_key()
        {
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
        let (handle, reject) = self.send_request_inner(url, method, req_id, params);
        if reject.is_some()
            && let (Some(tracker), Some((scope, cost, now))) = (&self.trading_rate_limit, charged)
        {
            // Frame never posted — reverse the pre-charge at the same instant.
            tracker.credit(scope, cost, now);
        }
        Ok(handle)
    }

    /// Best-effort: drop `req_id`'s pending entry after a caller-side deadline timeout.
    pub(crate) fn abandon_request(&self, req_id: u64) {
        let _ = self
            .bus
            .try_post_caller_inbound(CallerInbound::AbandonWsRequest { req_id });
    }

    pub(crate) fn has_handlers_for(&self, url: WsUrl, channel: ChannelName) -> bool {
        presence_has_handlers(&self.presence_mirror, url, channel)
    }

    pub(crate) fn require_handler(
        &self,
        url: WsUrl,
        channel: ChannelName,
    ) -> Result<(), SubscriptionError> {
        // Subscriptions are WS-only: a dead reactor means no streaming — reject
        // with `LoopDead` rather than an opaque closed-channel error.
        if self.is_loop_failed() {
            return Err(SubscriptionError::LoopDead);
        }
        if !self.has_handlers_for(url, channel) {
            return Err(SubscriptionError::NoHandlerRegistered { channel });
        }
        Ok(())
    }

    /// Whether the client's `close()` has begun.
    pub(crate) fn is_shutting_down(&self) -> bool {
        self.bus.is_shutting_down()
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
        self.subscribe_for(
            channel.spot_url(),
            channel,
            pairs.into_iter().map(StreamSymbol::from).collect(),
            params,
        )
    }

    /// URL-explicit internal subscribe path. Existing public Spot wrappers
    /// remain routed through [`ChannelName::spot_url`].
    pub(crate) fn subscribe_for(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pairs: Vec<StreamSymbol>,
        params: SubscribeParams,
    ) -> Result<(), SubscriptionError> {
        let (posted, reservation) =
            self.with_unified_book_reservation(url, channel, pairs, params, false, |pairs| {
                self.post_register_batch(url, channel, pairs, params, None)
            })?;
        posted?;
        if let Some(reservation) = reservation {
            reservation.commit();
        }
        Ok(())
    }

    /// Reserve any Unified book intent, then run `post`, under one intents lock.
    /// The lock is released before `post`'s result or the reservation can drop.
    fn with_unified_book_reservation<R>(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pairs: Vec<StreamSymbol>,
        params: SubscribeParams,
        guarded: bool,
        post: impl FnOnce(Vec<StreamSymbol>) -> R,
    ) -> Result<(R, Option<UnifiedBookReservation>), SubscriptionError> {
        // Shared choke for subscribe_* and on_*_for: dead reactor → LoopDead, not QueueFull.
        if self.is_loop_failed() {
            return Err(SubscriptionError::LoopDead);
        }
        self.bus.flush_pending_teardowns();
        let mut reservation = None;
        // Reserve before posting the mutation so incompatible Unified book intents
        // fail synchronously, before any wire work can be scheduled.
        let mut book_scope = url.shares_unified_book_stream(channel).then(|| {
            crate::api::subscription_guard::lock_unified_book_intents(&self.unified_book_intents)
        });
        if let Some(intents) = book_scope.as_mut() {
            reservation =
                self.reserve_unified_book_intent_locked(intents, channel, &pairs, params, guarded)?;
        }
        let posted = post(pairs);
        drop(book_scope);
        Ok((posted, reservation))
    }

    fn post_register_batch(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pairs: Vec<StreamSymbol>,
        params: SubscribeParams,
        ref_id: Option<HandlerId>,
    ) -> Result<(), SubscriptionError> {
        let entries: Vec<SubscriptionEntry> = if pairs.is_empty() {
            vec![SubscriptionEntry::new(
                url,
                channel,
                None::<StreamSymbol>,
                params,
            )]
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

    /// `on_*_for` funnel: reserve any Unified book intent, register, subscribe with guard ref,
    /// mint the guard.
    /// On reject the handler unwinds; a rejected unwind is retried on the next registry call.
    pub(crate) fn register_and_subscribe_guarded(
        &self,
        channel: ChannelName,
        pairs: Vec<Symbol>,
        params: SubscribeParams,
        cb: HandlerCallback,
    ) -> Result<SubscriptionGuard, SubscriptionError> {
        self.register_and_subscribe_guarded_for(
            channel.spot_url(),
            channel,
            pairs.into_iter().map(StreamSymbol::from).collect(),
            params,
            cb,
        )
    }

    /// URL-explicit guarded registration for a product-specific stream.
    pub(crate) fn register_and_subscribe_guarded_for(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pairs: Vec<StreamSymbol>,
        params: SubscribeParams,
        cb: HandlerCallback,
    ) -> Result<SubscriptionGuard, SubscriptionError> {
        let registry_channel = channel.registry_channel(url);
        let (registered, reservation) = self.with_unified_book_reservation(
            url,
            registry_channel,
            pairs,
            params,
            true,
            |pairs| {
                let id = self
                    .register_handler_id_checked_for(url, channel, cb)
                    .map_err(|(_, e, rejected)| (e, rejected))?;
                let posted = self.post_register_batch(
                    url,
                    registry_channel,
                    pairs.clone(),
                    params,
                    Some(id),
                );
                Ok((id, posted, pairs))
            },
        )?;
        let (id, posted, pairs) = registered.map_err(|(e, _rejected)| e)?;
        if let Err(e) = posted {
            // Register batch is atomic: on Err only the handler unwinds (unsubscribe
            // would bare-release other subscribers' refcounts).
            self.deregister_handler_for(url, channel, id);
            return Err(e);
        }
        let guard = SubscriptionGuard::new(id, url, channel, pairs, self.weak_bus());
        Ok(match reservation {
            Some(reservation) => guard.with_unified_book_reservation(reservation),
            None => guard,
        })
    }

    fn reserve_unified_book_intent_locked(
        &self,
        intents: &mut std::collections::HashMap<String, UnifiedBookIntent>,
        channel: ChannelName,
        pairs: &[StreamSymbol],
        params: SubscribeParams,
        guarded: bool,
    ) -> Result<Option<UnifiedBookReservation>, SubscriptionError> {
        let (depth, snapshot, maintained_depth) = match params {
            SubscribeParams::Book { depth } => (
                depth.wire_depth().as_wire_u32(),
                true,
                Some(depth.as_wire_u32()),
            ),
            SubscribeParams::BookRaw { depth, snapshot } => {
                (depth.as_wire_u32(), snapshot.unwrap_or(true), None)
            }
            _ => return Ok(None),
        };
        let pairs: Vec<(String, ChannelName)> = pairs
            .iter()
            .map(|pair| (pair.as_str().to_string(), channel))
            .collect();
        if let Some((pair, _)) = pairs.iter().find(|(pair, _)| {
            intents.get(pair).is_some_and(|existing| {
                (existing.depth, existing.snapshot) != (depth, snapshot)
                    || matches!(
                        (existing.live_maintained_depth(), maintained_depth),
                        (Some(live), Some(requested)) if live != requested
                    )
            })
        }) {
            return Err(SubscriptionError::InvalidArguments {
                detail: format!(
                    "Unified Public book for {pair} already has incompatible depth/snapshot parameters"
                ),
            });
        }
        let mut reservations = Vec::with_capacity(pairs.len());
        for (pair, channel) in &pairs {
            let intent = intents
                .entry(pair.clone())
                .or_insert_with(|| UnifiedBookIntent {
                    depth,
                    snapshot,
                    maintained_depth: None,
                    bare: [0; 2],
                    guarded: [0; 2],
                    generation: self.next_book_generations(),
                });
            intent.add(*channel, guarded);
            if maintained_depth.is_some() {
                intent.maintained_depth = maintained_depth;
            }
            if let Some(generation) = intent.generation(*channel) {
                reservations.push((pair.clone(), *channel, generation));
            }
        }
        Ok(Some(UnifiedBookReservation::new(
            Arc::clone(&self.unified_book_intents),
            reservations,
            guarded,
        )))
    }

    fn release_unified_book_bare(&self, url: WsUrl, channel: ChannelName, pairs: &[StreamSymbol]) {
        if !url.shares_unified_book_stream(channel) {
            return;
        }
        let mut intents =
            crate::api::subscription_guard::lock_unified_book_intents(&self.unified_book_intents);
        for pair in pairs {
            let key = pair.as_str();
            if let Some(intent) = intents.get_mut(key) {
                intent.release_bare(channel);
                if intent.total() == 0 {
                    intents.remove(key);
                }
            }
        }
    }

    fn next_book_generations(&self) -> [u64; 2] {
        [
            self.unified_book_generation.fetch_add(1, Ordering::Relaxed),
            self.unified_book_generation.fetch_add(1, Ordering::Relaxed),
        ]
    }

    fn clear_unified_book_intents(
        &self,
        intents: &mut std::collections::HashMap<String, UnifiedBookIntent>,
        channel: Option<ChannelName>,
    ) {
        intents.retain(|_, intent| {
            match channel {
                Some(channel @ (ChannelName::Book | ChannelName::BookRaw)) => {
                    intent.clear_channel(Some(channel));
                    intent.set_generation(
                        channel,
                        self.unified_book_generation.fetch_add(1, Ordering::Relaxed),
                    );
                }
                Some(_) => return true,
                None => {
                    intent.clear_channel(None);
                    intent.generation = self.next_book_generations();
                }
            }
            intent.total() != 0
        });
    }

    /// Deregister and unsubscribe `channel` over `pairs` as one atomic batch.
    pub(crate) fn unsubscribe(
        &self,
        channel: ChannelName,
        pairs: Vec<Symbol>,
    ) -> Result<(), SubscriptionError> {
        let pairs = pairs.into_iter().map(StreamSymbol::from).collect();
        self.unsubscribe_for(channel.spot_url(), channel, pairs)
    }

    /// URL-explicit internal unsubscribe path for product-specific streams.
    pub(crate) fn unsubscribe_for(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pairs: Vec<StreamSymbol>,
    ) -> Result<(), SubscriptionError> {
        // Shared choke for unsubscribe_*: dead reactor → LoopDead, not QueueFull.
        if self.is_loop_failed() {
            return Err(SubscriptionError::LoopDead);
        }
        self.bus.flush_pending_teardowns();
        if pairs.is_empty() {
            return self.unsubscribe_one(url, channel, None);
        }
        self.bus
            .try_post_caller_inbound_recovering_kind(CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::DeregisterBatch {
                    channel,
                    pairs: pairs.clone(),
                },
            })
            .map_err(|(_, reason)| self.post_reject_to_error(reason))?;
        self.release_unified_book_bare(url, channel, &pairs);
        Ok(())
    }

    fn unsubscribe_one(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
    ) -> Result<(), SubscriptionError> {
        // Refcount-decrement only; reactor sends wire unsubscribe only at refcount 0.
        self.bus
            .try_post_caller_inbound_recovering_kind(CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::Deregister {
                    channel,
                    pair: pair.clone(),
                },
            })
            .map_err(|(_, reason)| self.post_reject_to_error(reason))?;
        if let Some(pair) = pair.as_ref() {
            self.release_unified_book_bare(url, channel, std::slice::from_ref(pair));
        }
        Ok(())
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

    /// Forced teardown of every Unified Futures subscription (`None`) or one channel's.
    /// The public post and, if it succeeds, the book-intent clear share one intents lock.
    pub(crate) fn unsubscribe_futures(
        &self,
        channel: Option<ChannelName>,
    ) -> Result<(), SubscriptionError> {
        if self.is_loop_failed() {
            return Err(SubscriptionError::LoopDead);
        }
        self.bus.flush_pending_teardowns();
        let mut intents =
            crate::api::subscription_guard::lock_unified_book_intents(&self.unified_book_intents);
        let public = self.post_deregister_all_for(WsUrl::UnifiedPublic, channel);
        if public.is_ok() {
            self.clear_unified_book_intents(&mut intents, channel);
        }
        drop(intents);
        let private = self.post_deregister_all_for(WsUrl::UnifiedPrivate, channel);
        public.and(private)
    }

    /// Post one atomic DeregisterAll; the reactor iterates the registry.
    fn post_deregister_all(&self, channel: Option<ChannelName>) -> Result<(), SubscriptionError> {
        self.post_deregister_all_for(WsUrl::Public, channel)
    }

    /// Post one atomic URL-scoped DeregisterAll; the reactor iterates the registry.
    fn post_deregister_all_for(
        &self,
        url: WsUrl,
        channel: Option<ChannelName>,
    ) -> Result<(), SubscriptionError> {
        self.bus
            .try_post_caller_inbound_recovering_kind(CallerInbound::RegistryMutation {
                // Public preserves Spot's unified teardown semantics: the reactor fans it
                // out to Public and Auth. Any other URL is torn down independently.
                url,
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
                .filter(|((url, _, _), _)| !url.is_unified())
                .map(|((url, channel, pair), row)| SubscriptionInfo {
                    channel: *channel,
                    pair: pair.as_ref().and_then(StreamSymbol::spot).cloned(),
                    state: Self::project_with_connection(
                        &conn_failed,
                        *url,
                        SubscriptionRegistry::project_state(&row.state),
                    ),
                })
                .collect()
        };
        rows.sort_by(|a, b| {
            subscription_order(
                (a.channel, a.pair.as_ref().map(Symbol::as_str)),
                (b.channel, b.pair.as_ref().map(Symbol::as_str)),
            )
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
                .filter(|((url, ch, _), _)| !url.is_unified() && *ch == wire)
                .map(|((url, ch, pair), row)| SubscriptionRef {
                    channel: *ch,
                    symbol: pair.as_ref().and_then(StreamSymbol::spot).cloned(),
                    state: Self::project_with_connection(
                        &conn_failed,
                        *url,
                        SubscriptionRegistry::project_state(&row.state),
                    ),
                    registered_at_monotonic: row.registered_at.as_duration().as_millis() as u64,
                })
                .collect()
        };
        rows.sort_by(|a, b| {
            subscription_order(
                (a.channel, a.symbol.as_ref().map(Symbol::as_str)),
                (b.channel, b.symbol.as_ref().map(Symbol::as_str)),
            )
        });
        Ok(rows)
    }

    /// Point-in-time per-state counts and per-channel histogram.
    pub(crate) fn status_summary(&self) -> Result<SubscriptionsSummary, SubscriptionError> {
        self.summary_where(|url| !url.is_unified())
    }

    fn summary_where(
        &self,
        keep: impl Fn(WsUrl) -> bool,
    ) -> Result<SubscriptionsSummary, SubscriptionError> {
        let mut summary = SubscriptionsSummary {
            total: 0,
            active: 0,
            pending: 0,
            failed: 0,
            by_channel: std::collections::HashMap::new(),
        };
        let conn_failed = self.conn_failed_sample();
        let mirror = self.read_model_mirror()?;
        for ((url, channel, _), row) in mirror.iter() {
            if !keep(*url) {
                continue;
            }
            summary.total += 1;
            // Same projection as list_active so Failed connections count as failed.
            match Self::project_with_connection(
                &conn_failed,
                *url,
                SubscriptionRegistry::project_state(&row.state),
            ) {
                SubscriptionState::Active => summary.active += 1,
                SubscriptionState::Pending => summary.pending += 1,
                SubscriptionState::Failed(_) => summary.failed += 1,
            }
            *summary.by_channel.entry(*channel).or_insert(0) += 1;
        }
        Ok(summary)
    }

    /// Point-in-time snapshot of Unified Futures entries only.
    pub(crate) fn list_active_futures(
        &self,
    ) -> Result<Vec<FuturesSubscriptionInfo>, SubscriptionError> {
        Ok(self
            .futures_rows(None)?
            .into_iter()
            .map(|row| FuturesSubscriptionInfo {
                channel: row.channel,
                symbol: row.symbol,
                interval: row.interval,
                tick_type: row.tick_type,
                state: row.state,
                subscribed_at: row.subscribed_at,
            })
            .collect())
    }

    /// Point-in-time Unified Futures refs for one logical channel.
    pub(crate) fn find_futures_by_channel(
        &self,
        channel: ChannelName,
    ) -> Result<Vec<FuturesSubscriptionRef>, SubscriptionError> {
        Ok(self
            .futures_rows(Some(channel))?
            .into_iter()
            .map(|row| FuturesSubscriptionRef {
                channel: row.channel,
                symbol: row.symbol,
                interval: row.interval,
                tick_type: row.tick_type,
                state: row.state,
                registered_at_monotonic: row.registered_at.as_duration().as_millis() as u64,
            })
            .collect())
    }

    fn futures_rows(
        &self,
        channel: Option<ChannelName>,
    ) -> Result<Vec<FuturesRow>, SubscriptionError> {
        let conn_failed = self.conn_failed_sample();
        let mut rows: Vec<FuturesRow> = {
            let mirror = self.read_model_mirror()?;
            mirror
                .iter()
                .filter(|((url, ch, _), _)| url.is_unified() && channel.is_none_or(|c| *ch == c))
                .map(|((url, ch, symbol), row)| {
                    let (symbol, interval, tick_type) =
                        StreamSymbol::futures_identity(symbol.as_ref());
                    FuturesRow {
                        channel: *ch,
                        symbol,
                        interval,
                        tick_type,
                        state: Self::project_with_connection(
                            &conn_failed,
                            *url,
                            SubscriptionRegistry::project_state(&row.state),
                        ),
                        registered_at: row.registered_at,
                        subscribed_at: row.subscribed_at,
                    }
                })
                .collect()
        };
        rows.sort_by(|a, b| {
            subscription_order(
                (
                    a.channel,
                    a.symbol.as_deref().map(|s| (s, a.interval, a.tick_type)),
                ),
                (
                    b.channel,
                    b.symbol.as_deref().map(|s| (s, b.interval, b.tick_type)),
                ),
            )
        });
        Ok(rows)
    }

    /// Point-in-time Unified Futures per-state counts and channel histogram.
    pub(crate) fn futures_status_summary(&self) -> Result<SubscriptionsSummary, SubscriptionError> {
        self.summary_where(|url| url.is_unified())
    }

    #[cfg(test)]
    pub(crate) fn subscription_mirror_handle(&self) -> SubscriptionMirror {
        Arc::clone(&self.subscription_mirror)
    }

    /// Live row on a terminally Failed connection → Failed(ConnectionFailed).
    /// Entry-local Terminated causes win.
    fn project_with_connection(
        conn_failed: &std::collections::HashSet<WsUrl>,
        url: WsUrl,
        state: SubscriptionState,
    ) -> SubscriptionState {
        if matches!(state, SubscriptionState::Failed(_)) || !conn_failed.contains(&url) {
            return state;
        }
        SubscriptionState::Failed(SubscribeFailureCause::ConnectionFailed)
    }

    /// Sampled once per read-model call: the connections currently terminally Failed.
    fn conn_failed_sample(&self) -> std::collections::HashSet<WsUrl> {
        let Some(supervisor) = self.supervisor.as_ref() else {
            return std::collections::HashSet::new();
        };
        WsUrl::ALL
            .into_iter()
            .filter(|&url| supervisor.current_state(url) == crate::types::ConnectionState::Failed)
            .collect()
    }
}

/// Unified read-model row shared by the list and find-by-channel queries.
struct FuturesRow {
    channel: ChannelName,
    symbol: Option<String>,
    interval: Option<crate::types::OhlcInterval>,
    tick_type: Option<crate::types::FuturesOhlcTickType>,
    state: SubscriptionState,
    registered_at: MonotonicInstant,
    subscribed_at: Option<MonotonicInstant>,
}

/// Compare read-model rows by subscription replay order.
fn subscription_order<P: Ord>(
    a: (ChannelName, Option<P>),
    b: (ChannelName, Option<P>),
) -> std::cmp::Ordering {
    crate::conn::subscription_registry::subscription_order_key(a.0, a.1).cmp(
        &crate::conn::subscription_registry::subscription_order_key(b.0, b.1),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::SystemClock;
    use crate::dispatch::DispatchEventBusConfig;

    impl WsSurface {
        fn reserve_unified_book_intent(
            &self,
            url: WsUrl,
            channel: ChannelName,
            pairs: &[StreamSymbol],
            params: SubscribeParams,
            guarded: bool,
        ) -> Result<Option<UnifiedBookReservation>, SubscriptionError> {
            if !url.shares_unified_book_stream(channel) {
                return Ok(None);
            }
            let mut intents = crate::api::subscription_guard::lock_unified_book_intents(
                &self.unified_book_intents,
            );
            self.reserve_unified_book_intent_locked(&mut intents, channel, pairs, params, guarded)
        }
    }

    fn has_spot_handlers(surface: &WsSurface, channel: ChannelName) -> bool {
        surface.has_handlers_for(channel.spot_url(), channel)
    }

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
    fn unified_book_intents_reject_incompatible_params_and_release_on_guard_drop() {
        let (surface, _bus, _mirror) = make_surface();
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let raw = surface
            .reserve_unified_book_intent(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                std::slice::from_ref(&pair),
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D10,
                    snapshot: Some(false),
                },
                true,
            )
            .expect("first intent accepted")
            .expect("book reservation");
        let err = surface
            .reserve_unified_book_intent(
                WsUrl::UnifiedPublic,
                ChannelName::Book,
                std::slice::from_ref(&pair),
                SubscribeParams::Book {
                    depth: crate::types::BookDepth::D10,
                },
                true,
            )
            .expect_err("raw(false,D10) and maintained(D10) conflict");
        assert!(matches!(err, SubscriptionError::InvalidArguments { .. }));
        drop(raw);

        let maintained = surface
            .reserve_unified_book_intent(
                WsUrl::UnifiedPublic,
                ChannelName::Book,
                std::slice::from_ref(&pair),
                SubscribeParams::Book {
                    depth: crate::types::BookDepth::D10,
                },
                true,
            )
            .expect("released intent accepted")
            .expect("book reservation");
        let err = surface
            .reserve_unified_book_intent(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                std::slice::from_ref(&pair),
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D10,
                    snapshot: Some(false),
                },
                true,
            )
            .expect_err("maintained(D10) and raw(false,D10) conflict");
        assert!(matches!(err, SubscriptionError::InvalidArguments { .. }));
        let raw = surface
            .reserve_unified_book_intent(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                std::slice::from_ref(&pair),
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D25,
                    snapshot: Some(true),
                },
                true,
            )
            .expect("raw(true,D25) shares maintained(D10)");
        drop((maintained, raw));
    }

    fn reserve_book(
        surface: &WsSurface,
        pair: &StreamSymbol,
        channel: ChannelName,
        params: SubscribeParams,
    ) -> Result<Option<UnifiedBookReservation>, SubscriptionError> {
        surface.reserve_unified_book_intent(
            WsUrl::UnifiedPublic,
            channel,
            std::slice::from_ref(pair),
            params,
            true,
        )
    }

    fn book_guard(
        id: u64,
        channel: ChannelName,
        pair: &StreamSymbol,
        reservation: UnifiedBookReservation,
    ) -> SubscriptionGuard {
        SubscriptionGuard::new(
            HandlerId(id),
            WsUrl::UnifiedPublic,
            channel,
            vec![pair.clone()],
            Weak::new(),
        )
        .with_unified_book_reservation(reservation)
    }

    #[test]
    fn same_slot_stale_guard_cannot_release_new_owner() {
        let (surface, _bus, _mirror) = make_surface();
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let maintained = |depth| SubscribeParams::Book { depth };
        let old = reserve_book(
            &surface,
            &pair,
            ChannelName::Book,
            maintained(crate::types::BookDepth::D10),
        )
        .expect("old intent accepted")
        .expect("old reservation");
        let old_guard = book_guard(1, ChannelName::Book, &pair, old);
        surface
            .unsubscribe_futures(Some(ChannelName::Book))
            .expect("teardown clears the old intent");
        let new = reserve_book(
            &surface,
            &pair,
            ChannelName::Book,
            maintained(crate::types::BookDepth::D10),
        )
        .expect("re-registered intent accepted")
        .expect("new reservation");

        drop(old_guard);
        let err = reserve_book(
            &surface,
            &pair,
            ChannelName::Book,
            maintained(crate::types::BookDepth::D25),
        )
        .expect_err("a stale guard must not release the new owner");
        assert!(matches!(err, SubscriptionError::InvalidArguments { .. }));
        drop(new);
    }

    #[test]
    fn partial_teardown_keeps_a_stale_guard_from_releasing_the_new_owner() {
        let (surface, _bus, _mirror) = make_surface();
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let maintained = |depth| SubscribeParams::Book { depth };
        let old = reserve_book(
            &surface,
            &pair,
            ChannelName::Book,
            maintained(crate::types::BookDepth::D10),
        )
        .expect("old intent accepted")
        .expect("old reservation");
        let old_guard = book_guard(1, ChannelName::Book, &pair, old);
        let raw = reserve_book(
            &surface,
            &pair,
            ChannelName::BookRaw,
            SubscribeParams::BookRaw {
                depth: crate::types::BookDepth::D25,
                snapshot: Some(true),
            },
        )
        .expect("raw shares the maintained intent")
        .expect("raw reservation");
        surface
            .unsubscribe_futures(Some(ChannelName::Book))
            .expect("teardown of the maintained channel only");
        let new = reserve_book(
            &surface,
            &pair,
            ChannelName::Book,
            maintained(crate::types::BookDepth::D10),
        )
        .expect("re-registered intent accepted")
        .expect("new reservation");

        drop(old_guard);
        let err = reserve_book(
            &surface,
            &pair,
            ChannelName::Book,
            maintained(crate::types::BookDepth::D25),
        )
        .expect_err("a stale guard must not release the new owner");
        assert!(matches!(err, SubscriptionError::InvalidArguments { .. }));
        drop((new, raw));
    }

    #[test]
    fn stale_unified_book_guard_cannot_release_reregistered_intent() {
        let (surface, _bus, _mirror) = make_surface();
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let old_reservation = surface
            .reserve_unified_book_intent(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                std::slice::from_ref(&pair),
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D10,
                    snapshot: Some(false),
                },
                true,
            )
            .expect("old intent accepted")
            .expect("old reservation");
        let old_guard = SubscriptionGuard::new(
            HandlerId(1),
            WsUrl::UnifiedPublic,
            ChannelName::BookRaw,
            vec![pair.clone()],
            Weak::new(),
        )
        .with_unified_book_reservation(old_reservation);

        surface
            .unsubscribe_futures(None)
            .expect("forced teardown clears old intent");
        let new_reservation = surface
            .reserve_unified_book_intent(
                WsUrl::UnifiedPublic,
                ChannelName::Book,
                std::slice::from_ref(&pair),
                SubscribeParams::Book {
                    depth: crate::types::BookDepth::D10,
                },
                true,
            )
            .expect("re-registered intent accepted")
            .expect("new reservation");

        drop(old_guard);
        let err = surface
            .reserve_unified_book_intent(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                std::slice::from_ref(&pair),
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D10,
                    snapshot: Some(false),
                },
                true,
            )
            .expect_err("stale guard must not erase re-registered intent");
        assert!(matches!(err, SubscriptionError::InvalidArguments { .. }));
        drop(new_reservation);
    }

    #[test]
    fn stale_unified_book_guard_cannot_release_intent_after_channel_teardown() {
        for old_channel in [ChannelName::Book, ChannelName::BookRaw] {
            let (surface, _bus, _mirror) = make_surface();
            let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
            let (old_params, new_channel, new_params, later_params) = match old_channel {
                ChannelName::Book => (
                    SubscribeParams::Book {
                        depth: crate::types::BookDepth::D10,
                    },
                    ChannelName::BookRaw,
                    SubscribeParams::BookRaw {
                        depth: crate::types::BookDepth::D10,
                        snapshot: Some(false),
                    },
                    SubscribeParams::Book {
                        depth: crate::types::BookDepth::D10,
                    },
                ),
                ChannelName::BookRaw => (
                    SubscribeParams::BookRaw {
                        depth: crate::types::BookDepth::D10,
                        snapshot: Some(false),
                    },
                    ChannelName::Book,
                    SubscribeParams::Book {
                        depth: crate::types::BookDepth::D10,
                    },
                    SubscribeParams::BookRaw {
                        depth: crate::types::BookDepth::D10,
                        snapshot: Some(false),
                    },
                ),
                _ => unreachable!("test covers only Unified book channels"),
            };
            let old_reservation = surface
                .reserve_unified_book_intent(
                    WsUrl::UnifiedPublic,
                    old_channel,
                    std::slice::from_ref(&pair),
                    old_params,
                    true,
                )
                .expect("old intent accepted")
                .expect("old reservation");
            let old_guard = SubscriptionGuard::new(
                HandlerId(1),
                WsUrl::UnifiedPublic,
                old_channel,
                vec![pair.clone()],
                Weak::new(),
            )
            .with_unified_book_reservation(old_reservation);

            surface
                .unsubscribe_futures(Some(old_channel))
                .expect("channel teardown clears old intent");
            let new_reservation = surface
                .reserve_unified_book_intent(
                    WsUrl::UnifiedPublic,
                    new_channel,
                    std::slice::from_ref(&pair),
                    new_params,
                    true,
                )
                .expect("incompatible re-registered intent accepted")
                .expect("new reservation");

            drop(old_guard);
            let err = surface
                .reserve_unified_book_intent(
                    WsUrl::UnifiedPublic,
                    old_channel,
                    std::slice::from_ref(&pair),
                    later_params,
                    true,
                )
                .expect_err("stale guard must not erase re-registered intent");
            assert!(matches!(err, SubscriptionError::InvalidArguments { .. }));
            drop(new_reservation);
        }
    }

    #[test]
    fn unified_book_direct_subscribes_share_only_matching_effective_intent() {
        let (surface, bus, _mirror) = make_surface();
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
        surface
            .subscribe_for(
                WsUrl::UnifiedPublic,
                ChannelName::Book,
                vec![pair.clone()],
                SubscribeParams::Book {
                    depth: crate::types::BookDepth::D10,
                },
            )
            .expect("maintained intent reserves D25 snapshot");
        surface
            .subscribe_for(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                vec![pair.clone()],
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D25,
                    snapshot: Some(true),
                },
            )
            .expect("matching raw intent shares maintained wire stream");
        let err = surface
            .subscribe_for(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                vec![pair.clone()],
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D10,
                    snapshot: Some(true),
                },
            )
            .expect_err("incompatible direct intent must fail before posting");
        assert!(matches!(err, SubscriptionError::InvalidArguments { .. }));

        surface
            .unsubscribe_for(WsUrl::UnifiedPublic, ChannelName::Book, vec![pair.clone()])
            .expect("first owner releases");
        surface
            .unsubscribe_for(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                vec![pair.clone()],
            )
            .expect("last owner releases");
        surface
            .subscribe_for(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                vec![pair],
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D10,
                    snapshot: Some(true),
                },
            )
            .expect("explicit releases clear the direct reservation");
        drop(bus);
    }

    #[test]
    fn unified_book_raw_default_snapshot_shares_maintained_intent() {
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let maintained = SubscribeParams::Book {
            depth: crate::types::BookDepth::D10,
        };
        let raw_default = SubscribeParams::BookRaw {
            depth: crate::types::BookDepth::D25,
            snapshot: None,
        };
        for order in [
            [
                (ChannelName::BookRaw, raw_default),
                (ChannelName::Book, maintained),
            ],
            [
                (ChannelName::Book, maintained),
                (ChannelName::BookRaw, raw_default),
            ],
        ] {
            let (surface, bus, _mirror) = make_surface();
            let mut rx = bus.take_caller_to_io_rx().expect("receiver");
            for (channel, params) in order {
                surface
                    .subscribe_for(WsUrl::UnifiedPublic, channel, vec![pair.clone()], params)
                    .expect("raw D25 None shares maintained D10");
            }
            for (depth, snapshot) in [
                (crate::types::BookDepth::D25, Some(false)),
                (crate::types::BookDepth::D10, None),
            ] {
                let err = surface
                    .subscribe_for(
                        WsUrl::UnifiedPublic,
                        ChannelName::BookRaw,
                        vec![pair.clone()],
                        SubscribeParams::BookRaw { depth, snapshot },
                    )
                    .expect_err("depth or explicit no-snapshot still conflicts");
                assert!(matches!(err, SubscriptionError::InvalidArguments { .. }));
            }
            let mut raw_params = Vec::new();
            while let Ok(message) = rx.try_recv() {
                if let CallerInbound::RegistryMutation {
                    mutation: RegistryMutationOp::RegisterBatch { entries, .. },
                    ..
                } = message
                {
                    raw_params.extend(
                        entries
                            .iter()
                            .filter(|entry| entry.channel == ChannelName::BookRaw)
                            .map(|entry| entry.params),
                    );
                }
            }
            assert_eq!(raw_params, vec![raw_default]);
        }
    }

    #[test]
    fn unified_maintained_book_owners_must_share_caller_depth() {
        use crate::types::BookDepth;
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let book = |depth| SubscribeParams::Book { depth };
        let raw = SubscribeParams::BookRaw {
            depth: BookDepth::D25,
            snapshot: Some(true),
        };
        for (live, other) in [
            (BookDepth::D10, BookDepth::D25),
            (BookDepth::D25, BookDepth::D10),
        ] {
            let (surface, bus, _mirror) = make_surface();
            let mut rx = bus.take_caller_to_io_rx().expect("receiver");
            surface
                .subscribe_for(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    vec![pair.clone()],
                    book(live),
                )
                .expect("first maintained owner");
            let guarded = surface
                .reserve_unified_book_intent(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    std::slice::from_ref(&pair),
                    book(live),
                    true,
                )
                .expect("same caller depth shares")
                .expect("book reservation");
            let bare_err = surface
                .subscribe_for(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    vec![pair.clone()],
                    book(other),
                )
                .expect_err("a different caller depth must not share the builder");
            let guarded_err = surface
                .reserve_unified_book_intent(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    std::slice::from_ref(&pair),
                    book(other),
                    true,
                )
                .expect_err("a guarded owner at a different caller depth is rejected too");
            assert!(matches!(
                (bare_err, guarded_err),
                (
                    SubscriptionError::InvalidArguments { .. },
                    SubscriptionError::InvalidArguments { .. }
                )
            ));
            surface
                .subscribe_for(
                    WsUrl::UnifiedPublic,
                    ChannelName::BookRaw,
                    vec![pair.clone()],
                    raw,
                )
                .expect("raw D25 still shares the maintained stream");
            let mut posted = Vec::new();
            while let Ok(message) = rx.try_recv() {
                if let CallerInbound::RegistryMutation {
                    mutation: RegistryMutationOp::RegisterBatch { entries, .. },
                    ..
                } = message
                {
                    posted.extend(entries.iter().map(|entry| entry.params));
                }
            }
            assert_eq!(posted, vec![book(live), raw]);
            let intents = crate::api::subscription_guard::lock_unified_book_intents(
                &surface.unified_book_intents,
            );
            let intent = intents.get("PI_XBTUSD").expect("live intent");
            assert_eq!((intent.bare, intent.guarded), ([1, 1], [1, 0]));
            drop(intents);
            drop(guarded);
        }
    }

    #[test]
    fn unified_maintained_book_caller_depth_frees_with_its_last_owner() {
        use crate::types::BookDepth;
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let book = |depth| SubscribeParams::Book { depth };
        for keep_raw in [false, true] {
            let (surface, _bus, _mirror) = make_surface();
            let subscribe = |channel, params| {
                surface.subscribe_for(WsUrl::UnifiedPublic, channel, vec![pair.clone()], params)
            };
            if keep_raw {
                subscribe(
                    ChannelName::BookRaw,
                    SubscribeParams::BookRaw {
                        depth: BookDepth::D25,
                        snapshot: Some(true),
                    },
                )
                .expect("raw D25 owner");
            }
            subscribe(ChannelName::Book, book(BookDepth::D10)).expect("bare D10 owner");
            assert!(matches!(
                subscribe(ChannelName::Book, book(BookDepth::D25)),
                Err(SubscriptionError::InvalidArguments { .. })
            ));
            surface
                .unsubscribe_for(WsUrl::UnifiedPublic, ChannelName::Book, vec![pair.clone()])
                .expect("bare release");
            let guarded = surface
                .reserve_unified_book_intent(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    std::slice::from_ref(&pair),
                    book(BookDepth::D25),
                    true,
                )
                .expect("D25 accepted after the bare D10 owner leaves")
                .expect("book reservation");
            drop(guarded);
            subscribe(ChannelName::Book, book(BookDepth::D10))
                .expect("D10 accepted after the guarded D25 owner drops");
            surface
                .unsubscribe_futures(Some(ChannelName::Book))
                .expect("channel teardown");
            subscribe(ChannelName::Book, book(BookDepth::D25))
                .expect("D25 accepted after channel teardown");
            surface.unsubscribe_futures(None).expect("full teardown");
            subscribe(ChannelName::Book, book(BookDepth::D10))
                .expect("D10 accepted after full teardown");
        }
    }

    #[test]
    fn unified_book_queue_full_rolls_back_direct_reservation() {
        let (bus, surface) = WsSurface::on_bus_with_caller_capacity(1);
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::Public,
            channel: ChannelName::Ticker,
            op: HandlerMutationOp::Deregister { id: HandlerId(1) },
        })
        .expect("fill queue");
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let err = surface
            .subscribe_for(
                WsUrl::UnifiedPublic,
                ChannelName::Book,
                vec![pair.clone()],
                SubscribeParams::Book {
                    depth: crate::types::BookDepth::D10,
                },
            )
            .expect_err("full queue rejects before register");
        assert!(matches!(err, SubscriptionError::QueueFull));
        let _ = rx.try_recv().expect("drain filler");
        surface
            .subscribe_for(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                vec![pair],
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D10,
                    snapshot: Some(false),
                },
            )
            .expect("failed maintained post must not retain its reservation");
    }

    #[test]
    fn unified_book_handler_post_failure_releases_guarded_reservation() {
        let (bus, surface) = WsSurface::on_bus_with_caller_capacity(1);
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");
        bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
            url: WsUrl::Public,
            channel: ChannelName::Ticker,
            op: HandlerMutationOp::Deregister { id: HandlerId(1) },
        })
        .expect("fill queue");
        let pair = StreamSymbol::Futures("PI_XBTUSD".to_string());
        let err = surface
            .register_and_subscribe_guarded_for(
                WsUrl::UnifiedPublic,
                ChannelName::Book,
                vec![pair.clone()],
                SubscribeParams::Book {
                    depth: crate::types::BookDepth::D10,
                },
                crate::dispatch::HandlerCallback::noop(),
            )
            .expect_err("full queue rejects the handler register");
        assert!(matches!(err, SubscriptionError::QueueFull));
        let _ = rx.try_recv().expect("drain filler");
        assert!(
            rx.try_recv().is_err(),
            "no handler or registry work was posted"
        );
        surface
            .subscribe_for(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                vec![pair],
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D10,
                    snapshot: Some(false),
                },
            )
            .expect("failed handler post must not retain its reservation");
    }

    #[test]
    fn a_rejected_handler_post_drops_a_callback_owning_a_book_guard_outside_the_intents_lock() {
        for reject in [PostReject::Full, PostReject::Closed] {
            let (bus, surface) = WsSurface::on_bus_with_caller_capacity(2);
            let mut rx = bus.take_caller_to_io_rx().expect("receiver");
            let book = SubscribeParams::Book {
                depth: crate::types::BookDepth::D10,
            };
            let sibling = std::sync::Mutex::new(
                surface
                    .register_and_subscribe_guarded_for(
                        WsUrl::UnifiedPublic,
                        ChannelName::Book,
                        vec![StreamSymbol::Futures("PI_ETHUSD".to_string())],
                        book,
                        crate::dispatch::HandlerCallback::noop(),
                    )
                    .expect("sibling guard"),
            );
            while rx.try_recv().is_ok() {}
            match reject {
                PostReject::Full => {
                    for _ in 0..2 {
                        bus.try_post_caller_inbound(CallerInbound::AbandonWsRequest { req_id: 0 })
                            .expect("fill queue");
                    }
                }
                PostReject::Closed => drop(rx),
            }
            let callback = crate::dispatch::HandlerCallback::new(
                Arc::new(|_v| crate::dispatch::HandlerDecodeOutcome::Deliver(Arc::new(()))),
                Arc::new(move |_p| {
                    let _ = &sibling;
                }),
            );
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let subscriber = Arc::clone(&surface);
            std::thread::spawn(move || {
                let result = subscriber.register_and_subscribe_guarded_for(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    vec![StreamSymbol::Futures("PI_XBTUSD".to_string())],
                    book,
                    callback,
                );
                let _ = done_tx.send(result.map(drop));
            });
            let result = done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap_or_else(|_| panic!("{reject:?}: the rejected registration must not hang"));
            assert!(
                matches!(
                    (reject, &result),
                    (PostReject::Full, Err(SubscriptionError::QueueFull))
                        | (PostReject::Closed, Err(SubscriptionError::ClientClosed))
                ),
                "{reject:?}: {result:?}"
            );
        }
    }

    #[test]
    fn unsubscribe_futures_posts_the_unified_public_teardown_only_while_holding_the_intents_lock() {
        let (surface, bus, _mirror) = make_surface();
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");
        let held = crate::api::subscription_guard::lock_unified_book_intents(
            &surface.unified_book_intents,
        );
        let unsubscriber = {
            let surface = Arc::clone(&surface);
            std::thread::spawn(move || surface.unsubscribe_futures(None))
        };
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(
            rx.try_recv().is_err(),
            "the teardown waits for the intents lock"
        );

        drop(held);
        unsubscriber
            .join()
            .expect("unsubscribe thread")
            .expect("teardown posted");
        assert!(matches!(
            rx.try_recv(),
            Ok(CallerInbound::RegistryMutation {
                url: WsUrl::UnifiedPublic,
                mutation: RegistryMutationOp::DeregisterAll { channel: None },
            })
        ));
    }

    #[test]
    fn a_futures_teardown_cannot_land_between_a_book_reservation_and_its_registration() {
        type Subscribe = fn(
            &WsSurface,
            Vec<StreamSymbol>,
        ) -> Result<Option<SubscriptionGuard>, SubscriptionError>;
        const BOOK: SubscribeParams = SubscribeParams::Book {
            depth: crate::types::BookDepth::D10,
        };
        let bare: Subscribe = |surface, pairs| {
            surface
                .subscribe_for(WsUrl::UnifiedPublic, ChannelName::Book, pairs, BOOK)
                .map(|()| None)
        };
        let guarded: Subscribe = |surface, pairs| {
            surface
                .register_and_subscribe_guarded_for(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    pairs,
                    BOOK,
                    crate::dispatch::HandlerCallback::noop(),
                )
                .map(Some)
        };
        for (label, subscribe) in [("bare", bare), ("guarded", guarded)] {
            let (surface, bus, _mirror) = make_surface();
            let mut rx = bus.take_caller_to_io_rx().expect("rx available");
            let pairs: Vec<StreamSymbol> = (0..20_000)
                .map(|i| StreamSymbol::Futures(format!("PI_{i}")))
                .collect();
            let expected = pairs.len();
            let subscriber = {
                let surface = Arc::clone(&surface);
                std::thread::spawn(move || subscribe(&surface, pairs))
            };
            loop {
                let reserved = crate::api::subscription_guard::lock_unified_book_intents(
                    &surface.unified_book_intents,
                )
                .len();
                if reserved == expected {
                    break;
                }
                std::thread::yield_now();
            }
            surface.unsubscribe_futures(None).expect("teardown");
            let _guard = subscriber
                .join()
                .expect("subscriber thread")
                .expect("subscribe");

            let mut registered = false;
            while let Ok(message) = rx.try_recv() {
                match message {
                    CallerInbound::RegistryMutation {
                        url: WsUrl::UnifiedPublic,
                        mutation: RegistryMutationOp::RegisterBatch { .. },
                    } => registered = true,
                    CallerInbound::RegistryMutation {
                        url: WsUrl::UnifiedPublic,
                        mutation: RegistryMutationOp::DeregisterAll { .. },
                    } => registered = false,
                    _ => {}
                }
            }
            let reserved = crate::api::subscription_guard::lock_unified_book_intents(
                &surface.unified_book_intents,
            )
            .values()
            .any(|intent| intent.total() != 0);
            assert_eq!(
                reserved, registered,
                "{label}: the intents track the reactor's live owners"
            );
        }
    }

    #[test]
    fn unified_book_concurrent_incompatible_registration_has_one_winner() {
        for round in 0..200 {
            let (surface, _bus, _mirror) = make_surface();
            let start = Arc::new(std::sync::Barrier::new(2));
            let maintained_surface = Arc::clone(&surface);
            let maintained_start = Arc::clone(&start);
            let maintained = std::thread::spawn(move || {
                maintained_start.wait();
                maintained_surface.subscribe_for(
                    WsUrl::UnifiedPublic,
                    ChannelName::Book,
                    vec![StreamSymbol::Futures("PI_XBTUSD".to_string())],
                    SubscribeParams::Book {
                        depth: crate::types::BookDepth::D10,
                    },
                )
            });
            start.wait();
            let raw = surface.subscribe_for(
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                vec![StreamSymbol::Futures("PI_XBTUSD".to_string())],
                SubscribeParams::BookRaw {
                    depth: crate::types::BookDepth::D10,
                    snapshot: Some(false),
                },
            );
            let maintained = maintained.join().expect("registration thread");
            assert!(
                matches!(
                    (maintained, raw),
                    (Ok(()), Err(SubscriptionError::InvalidArguments { .. }))
                        | (Err(SubscriptionError::InvalidArguments { .. }), Ok(()))
                ),
                "round {round}: the reservation lock must admit exactly one incompatible intent"
            );
        }
    }

    #[test]
    fn rejected_deregister_is_retried_on_the_next_registry_call() {
        let (bus, surface) = WsSurface::on_bus_with_caller_capacity(2);
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");

        for _ in 0..2 {
            bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
                url: WsUrl::Public,
                channel: ChannelName::Ticker,
                op: HandlerMutationOp::Deregister { id: HandlerId(999) },
            })
            .expect("filler post fits");
        }
        surface.deregister_handler_for(WsUrl::Auth, ChannelName::Executions, HandlerId(7));

        let _ = rx.try_recv().expect("filler drained");
        let _ = rx.try_recv().expect("filler drained");
        surface.deregister_handler_for(WsUrl::Auth, ChannelName::Executions, HandlerId(8));

        let first = rx.try_recv().expect("the queued deregister is flushed");
        assert!(
            matches!(
                first,
                CallerInbound::HandlerMutation {
                    url: WsUrl::Auth,
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
        let (bus, surface) = WsSurface::on_bus_with_caller_capacity(2);
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");

        for _ in 0..2 {
            bus.try_post_caller_inbound(CallerInbound::HandlerMutation {
                url: WsUrl::Public,
                channel: ChannelName::Ticker,
                op: HandlerMutationOp::Deregister { id: HandlerId(999) },
            })
            .expect("filler post fits");
        }
        surface.deregister_handler_for(WsUrl::Auth, ChannelName::Executions, HandlerId(7));
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
            url: WsUrl::Public,
            channel: ChannelName::Ticker,
            op: HandlerMutationOp::Deregister { id: HandlerId(999) },
        })
        .expect("filler occupies the only slot");

        let guard = SubscriptionGuard::new(
            HandlerId(42),
            WsUrl::Public,
            ChannelName::Ticker,
            vec![Symbol::new("BTC/USD").unwrap().into()],
            Arc::downgrade(&bus),
        );
        drop(guard);

        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());

        bus.flush_pending_teardowns();

        match rx.try_recv().expect("deferred SubscriptionGuardDrop lands") {
            CallerInbound::SubscriptionGuardDrop {
                handler_id,
                url,
                channel,
                symbols,
            } => {
                assert_eq!(handler_id, HandlerId(42));
                assert_eq!(url, WsUrl::Public);
                assert_eq!(channel, ChannelName::Ticker);
                assert_eq!(symbols.len(), 1);
            }
            other => panic!("expected SubscriptionGuardDrop, got {other:?}"),
        }
    }

    #[test]
    fn ws_url_routing_auth_vs_public() {
        assert_eq!(ChannelName::Ticker.spot_url(), WsUrl::Public);
        assert_eq!(ChannelName::Book.spot_url(), WsUrl::Public);
        assert_eq!(ChannelName::Executions.spot_url(), WsUrl::Auth);
        assert_eq!(ChannelName::Balances.spot_url(), WsUrl::Auth);
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
            .subscribe(
                ChannelName::Balances,
                Vec::new(),
                SubscribeParams::Balances { snapshot: None },
            )
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
    fn unified_status_registration_is_one_atomic_connect_operation() {
        let (surface, bus, _mirror) = make_surface();
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");

        let handle = surface.register_unified_status_handler(HandlerCallback::noop());
        assert!(surface.has_handlers_for(WsUrl::UnifiedPublic, ChannelName::Status));
        assert!(matches!(
            rx.try_recv().expect("one atomic operation"),
            CallerInbound::RegisterHandlerAndConnect {
                url: WsUrl::UnifiedPublic,
                channel: ChannelName::Status,
                ..
            }
        ));
        assert!(
            rx.try_recv().is_err(),
            "status emits no wire subscribe operation"
        );
        drop(handle);
    }

    #[test]
    fn unified_status_registration_rejection_compensates_presence() {
        let (bus, surface) = WsSurface::on_bus_with_caller_capacity(1);
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::UnifiedPublic,
            event: crate::types::CallerEvent::StartConnect { request_id: 1 },
        })
        .expect("fill caller queue");

        let handle = surface.register_unified_status_handler(HandlerCallback::noop());
        drop(handle);
        assert!(
            !surface.has_handlers_for(WsUrl::UnifiedPublic, ChannelName::Status),
            "a rejected atomic post must compensate caller-side presence"
        );
        assert!(rx.try_recv().is_ok(), "only the filler was queued");
        assert!(
            rx.try_recv().is_err(),
            "no inert handler command was queued"
        );
    }

    #[test]
    fn unified_instrument_unsubscribe_uses_channel_wide_identity() {
        let (surface, bus, _mirror) = make_surface();
        let mut rx = bus.take_caller_to_io_rx().expect("rx available");
        surface
            .unsubscribe_for(WsUrl::UnifiedPublic, ChannelName::Instrument, Vec::new())
            .expect("unsubscribe posts");
        assert!(matches!(
            rx.try_recv().expect("one mutation"),
            CallerInbound::RegistryMutation {
                url: WsUrl::UnifiedPublic,
                mutation: RegistryMutationOp::Deregister {
                    channel: ChannelName::Instrument,
                    pair: None
                }
            }
        ));
    }

    #[test]
    fn checked_register_bumps_presence_mirror_and_returns_handle() {
        let (surface, _bus, _mirror) = make_surface();
        assert!(!has_spot_handlers(&surface, ChannelName::Ticker));
        let cb: HandlerCallback = HandlerCallback::noop();
        let handle = surface
            .register_handler_for_checked(ChannelName::Ticker.spot_url(), ChannelName::Ticker, cb)
            .expect("register");
        assert_eq!(handle.channel(), ChannelName::Ticker);
        assert!(
            has_spot_handlers(&surface, ChannelName::Ticker),
            "presence must be true immediately after register"
        );
        let cb2: HandlerCallback = HandlerCallback::noop();
        let handle2 = surface
            .register_handler_for_checked(ChannelName::Ticker.spot_url(), ChannelName::Ticker, cb2)
            .expect("register");
        assert!(handle2.id().raw() > handle.id().raw());
        drop(handle);
        drop(handle2);
    }

    #[test]
    fn checked_register_presence_visible_synchronously_no_await() {
        let (surface, _bus, _mirror) = make_surface();
        assert!(
            !has_spot_handlers(&surface, ChannelName::Trade),
            "baseline: no handler yet"
        );
        let cb: HandlerCallback = HandlerCallback::noop();
        let _handle = surface
            .register_handler_for_checked(ChannelName::Trade.spot_url(), ChannelName::Trade, cb)
            .expect("register");
        assert!(
            has_spot_handlers(&surface, ChannelName::Trade),
            "presence must be true immediately — gate must not spuriously reject"
        );
    }

    #[test]
    fn register_then_drop_handle_does_not_corrupt_mirror() {
        let (surface, _bus, mirror) = make_surface();
        let cb: HandlerCallback = HandlerCallback::noop();
        let handle = surface
            .register_handler_for_checked(ChannelName::Book.spot_url(), ChannelName::Book, cb)
            .expect("register");
        assert!(
            has_spot_handlers(&surface, ChannelName::Book),
            "present after register"
        );
        drop(handle);
        let count = mirror
            .read()
            .unwrap()
            .get(&(WsUrl::Public, ChannelName::Book))
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
        let _h1 = surface
            .register_handler_for_checked(ChannelName::Ohlc.spot_url(), ChannelName::Ohlc, cb1)
            .expect("register");
        let _h2 = surface
            .register_handler_for_checked(ChannelName::Ohlc.spot_url(), ChannelName::Ohlc, cb2)
            .expect("register");
        assert!(
            has_spot_handlers(&surface, ChannelName::Ohlc),
            "presence true with two handlers"
        );
        let count = mirror
            .read()
            .unwrap()
            .get(&(WsUrl::Public, ChannelName::Ohlc))
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
                (WsUrl::Auth, ChannelName::Executions, None),
                MirrorRow {
                    state: EntrySubState::Pending,
                    registered_at: MonotonicInstant::now(),
                    subscribed_at: None,
                },
            );
            m.insert(
                (
                    WsUrl::Public,
                    ChannelName::Ticker,
                    Some(Symbol::new("BTC/USD").unwrap().into()),
                ),
                MirrorRow {
                    state: EntrySubState::Acked,
                    registered_at: MonotonicInstant::now(),
                    subscribed_at: None,
                },
            );
            m.insert(
                (
                    WsUrl::UnifiedPublic,
                    ChannelName::Trade,
                    Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                ),
                MirrorRow {
                    state: EntrySubState::Acked,
                    registered_at: MonotonicInstant::now(),
                    subscribed_at: None,
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

        let futures = surface.list_active_futures().expect("Futures list_active");
        assert_eq!(futures.len(), 1);
        assert_eq!(futures[0].symbol.as_deref(), Some("PI_XBTUSD"));
        assert!(matches!(futures[0].state, SubscriptionState::Active));
        supervisor.state_mirror_clones()[&WsUrl::UnifiedPublic]
            .store(ConnectionState::Failed.as_u8(), Ordering::Release);
        let futures = surface
            .list_active_futures()
            .expect("failed Futures list_active");
        let futures_summary = surface.futures_status_summary().expect("Futures summary");
        assert!(matches!(
            futures[0].state,
            SubscriptionState::Failed(SubscribeFailureCause::ConnectionFailed)
        ));
        assert_eq!(
            (
                futures_summary.failed,
                futures_summary.active,
                futures_summary.pending
            ),
            (1, 0, 0),
            "Futures projection samples Unified Public, not Spot state"
        );
    }

    #[test]
    fn futures_read_models_and_teardown_cover_both_unified_sockets() {
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
        let trading_rl = Arc::new(crate::rate_limit::SpotTradingRateLimitTracker::new(
            crate::rate_limit::Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let surface = WsSurface::new_with_supervisor(
            Arc::clone(&bus),
            presence,
            Arc::new(AtomicU64::new(1)),
            Arc::clone(&supervisor),
            trading_rl,
            auth,
            clock,
        );
        let row = |state| MirrorRow {
            state,
            registered_at: MonotonicInstant::now(),
            subscribed_at: None,
        };
        {
            let mirror = surface.subscription_mirror_handle();
            let mut m = mirror.write().unwrap();
            m.insert(
                (
                    WsUrl::UnifiedPublic,
                    ChannelName::Trade,
                    Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
                ),
                row(EntrySubState::Acked),
            );
            m.insert(
                (WsUrl::UnifiedPrivate, ChannelName::Executions, None),
                row(EntrySubState::Acked),
            );
            m.insert(
                (WsUrl::UnifiedPrivate, ChannelName::Positions, None),
                row(EntrySubState::Pending),
            );
            m.insert(
                (WsUrl::Auth, ChannelName::Executions, None),
                row(EntrySubState::Acked),
            );
        }

        let rows = surface.list_active_futures().expect("Futures list_active");
        let channels: Vec<_> = rows.iter().map(|r| r.channel).collect();
        assert_eq!(
            rows.len(),
            3,
            "Spot Auth row excluded, both Unified included"
        );
        assert!(channels.contains(&ChannelName::Executions));
        assert!(channels.contains(&ChannelName::Positions));
        assert!(rows.iter().filter(|r| r.symbol.is_none()).count() == 2);
        assert_eq!(
            surface
                .find_futures_by_channel(ChannelName::Executions)
                .expect("find")
                .len(),
            1,
            "the Spot Auth executions row is not a Futures row"
        );
        let summary = surface.futures_status_summary().expect("summary");
        assert_eq!((summary.total, summary.active, summary.pending), (3, 2, 1));
        assert_eq!(
            surface.list_active().expect("Spot list_active").len(),
            1,
            "Spot read models keep to Public and Auth"
        );

        supervisor.state_mirror_clones()[&WsUrl::UnifiedPrivate]
            .store(ConnectionState::Failed.as_u8(), Ordering::Release);
        let rows = surface.list_active_futures().expect("failed-private list");
        let failed = |channel| {
            rows.iter()
                .find(|r| r.channel == channel)
                .map(|r| r.state.clone())
        };
        assert!(matches!(
            failed(ChannelName::Executions),
            Some(SubscriptionState::Failed(
                SubscribeFailureCause::ConnectionFailed
            ))
        ));
        assert!(matches!(
            failed(ChannelName::Trade),
            Some(SubscriptionState::Active)
        ));

        let mut rx = bus.take_caller_to_io_rx().expect("receiver");
        surface.unsubscribe_futures(None).expect("unsubscribe_all");
        surface
            .unsubscribe_futures(Some(ChannelName::Executions))
            .expect("unsubscribe_channel");
        let mut posted = Vec::new();
        while let Ok(inbound) = rx.try_recv() {
            if let CallerInbound::RegistryMutation {
                url,
                mutation: RegistryMutationOp::DeregisterAll { channel },
            } = inbound
            {
                posted.push((url, channel));
            }
        }
        for expected in [
            (WsUrl::UnifiedPublic, None),
            (WsUrl::UnifiedPrivate, None),
            (WsUrl::UnifiedPublic, Some(ChannelName::Executions)),
            (WsUrl::UnifiedPrivate, Some(ChannelName::Executions)),
        ] {
            assert!(
                posted.contains(&expected),
                "missing {expected:?} in {posted:?}"
            );
        }
        assert!(
            !posted.iter().any(|(url, _)| !url.is_unified()),
            "Futures teardown never touches Spot sockets: {posted:?}"
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

        let (handle, reject) = surface.send_request_inner(
            WsUrl::Auth,
            "addOrder".to_string(),
            1,
            serde_json::json!({}),
        );
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

        let (handle, reject) = surface.send_request_inner(
            WsUrl::Auth,
            "addOrder".to_string(),
            2,
            serde_json::json!({}),
        );
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
    fn a_missing_session_token_is_not_sent_and_maps_to_session_expired() {
        use crate::api::trade::{ws_conn_error_to_trade, ws_sent_error_to_trade};
        use crate::dispatch::OrderOp;
        use crate::error::{ApiError, ErrorCategory};
        let err = ConnectionError::session_token_unavailable();
        assert!(err.is_definitely_not_sent());
        assert_eq!(
            err.to_string(),
            "No valid session token is available; the request was not sent."
        );
        for te in [
            ws_conn_error_to_trade(ConnectionError::session_token_unavailable()),
            ws_sent_error_to_trade(
                OrderOp::AddOrder,
                ConnectionError::session_token_unavailable(),
            ),
        ] {
            assert_eq!(
                (te.code(), te.category(), te.retryable()),
                ("SESSION_EXPIRED", ErrorCategory::Auth, true),
                "{te:?}"
            );
        }
        assert!(
            !ws_conn_error_to_trade(ConnectionError::not_open())
                .code()
                .eq("SESSION_EXPIRED")
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
                WsUrl::Auth,
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
            WsUrl::Auth,
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
