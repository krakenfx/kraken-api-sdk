//! `SubscriptionNamespace` — Spot WS v2 subscribe/unsubscribe lifecycle via
//! `client.subscription()`. Each `subscribe_*` gates on a registered handler;
//! all methods are synchronous (no await).
//!
//! # Subscription outcome visibility
//!
//! `subscribe_*` returning `Ok` means the request was accepted for dispatch —
//! not that the stream is live. Outcomes surface on [`client.events()`](crate::Client::events):
//! a failed or torn-down subscription emits `SubscriptionTerminatedEvent`;
//! connection health arrives as connection-lifecycle events. Watch the events
//! bus — a data handler alone will wait forever on a dead connection:
//!
//! ```ignore
//! let _watch = client.events().on(EventType::SubscriptionTerminatedEvent, |ev| {
//!     // react: log, alert, resubscribe elsewhere, or shut down
//! })?;
//! client.subscription().subscribe_ticker(vec![pair], None, None)?;
//! ```

use std::sync::Arc;

use crate::api::market::OhlcInterval;
use crate::api::subscription_types::{
    SubscriptionError, SubscriptionInfo, SubscriptionRef, SubscriptionsSummary,
};
use crate::api::ws_surface::WsSurface;
use crate::conn::SubscribeParams;
use crate::types::{BookDepth, ChannelName, Symbol, TickerTrigger};

/// Spot WS v2 subscription-lifecycle namespace. Accessed via `client.subscription()`.
pub struct SubscriptionNamespace {
    ws: Arc<WsSurface>,
}

impl std::fmt::Debug for SubscriptionNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionNamespace")
            .finish_non_exhaustive()
    }
}

impl SubscriptionNamespace {
    /// Construct over the shared internal [`WsSurface`]. Crate-internal.
    pub(crate) fn new(ws: Arc<WsSurface>) -> Self {
        Self { ws }
    }

    /// Reject with `NoHandlerRegistered` when `channel` has no handler at call time.
    fn gate(&self, channel: ChannelName) -> Result<(), SubscriptionError> {
        // Subscriptions are WS-only: a dead reactor means no streaming — reject
        // with `LoopDead` rather than an opaque closed-channel error.
        if self.ws.is_loop_failed() {
            return Err(SubscriptionError::LoopDead);
        }
        if !self.ws.has_handlers(channel) {
            return Err(SubscriptionError::NoHandlerRegistered { channel });
        }
        Ok(())
    }

    /// Subscribe to the `ticker` channel for `pairs`. `snapshot: Some(false)`
    /// suppresses the opening snapshot; `event_trigger: Some(Bbo)` fires on every
    /// top-of-book change. Semantics: docs/guides/streaming.md.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::NoHandlerRegistered`] — no `ticker` handler at call time; register via `on_ticker` first.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; nothing was registered, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn subscribe_ticker(
        &self,
        pairs: Vec<Symbol>,
        snapshot: Option<bool>,
        event_trigger: Option<TickerTrigger>,
    ) -> Result<(), SubscriptionError> {
        self.gate(ChannelName::Ticker)?;
        self.ws.subscribe(
            ChannelName::Ticker,
            pairs,
            SubscribeParams::Ticker {
                snapshot,
                event_trigger,
            },
        )
    }

    /// Subscribe to the `book` channel (maintained mode) for `pairs` at `depth`.
    /// Always requests an opening snapshot; for delta-only use `subscribe_book_raw`.
    /// Semantics: docs/guides/streaming.md.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::NoHandlerRegistered`] — no maintained-book handler at call time; register via `on_book` first.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; nothing was registered, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn subscribe_book(
        &self,
        pairs: Vec<Symbol>,
        depth: BookDepth,
    ) -> Result<(), SubscriptionError> {
        self.gate(ChannelName::Book)?;
        self.ws
            .subscribe(ChannelName::Book, pairs, SubscribeParams::Book { depth })
    }

    /// Subscribe to the `book` channel (raw mode) for `pairs`. `snapshot: None`
    /// keeps Kraken's default (snapshot sent); `Some(false)` requests delta-only.
    /// Raw and maintained share one wire subscription: docs/guides/streaming.md.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::NoHandlerRegistered`] — no raw-book handler at call time; register via `on_book_raw` first.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; nothing was registered, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn subscribe_book_raw(
        &self,
        pairs: Vec<Symbol>,
        depth: BookDepth,
        snapshot: Option<bool>,
    ) -> Result<(), SubscriptionError> {
        // Gate on `BookRaw` (where `on_book_raw` registers); wire channel is still `book`.
        self.gate(ChannelName::BookRaw)?;
        self.ws.subscribe(
            ChannelName::Book,
            pairs,
            SubscribeParams::BookRaw { depth, snapshot },
        )
    }

    /// Subscribe to the `trade` channel for `pairs`.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::NoHandlerRegistered`] — no `trade` handler at call time; register via `on_trade` first.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; nothing was registered, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn subscribe_trade(
        &self,
        pairs: Vec<Symbol>,
        snapshot: Option<bool>,
    ) -> Result<(), SubscriptionError> {
        self.gate(ChannelName::Trade)?;
        // `Some(true)` pulls recent-trades history; `None` keeps Kraken's default (no snapshot).
        self.ws.subscribe(
            ChannelName::Trade,
            pairs,
            SubscribeParams::Trade { snapshot },
        )
    }

    /// Subscribe to the `ohlc` channel for `pairs` at `interval`. Coalesces per
    /// `(channel, pair)` — first interval wins; release all and resubscribe to change it.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::NoHandlerRegistered`] — no `ohlc` handler at call time; register via `on_ohlc` first.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; nothing was registered, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn subscribe_ohlc(
        &self,
        pairs: Vec<Symbol>,
        interval: OhlcInterval,
        snapshot: Option<bool>,
    ) -> Result<(), SubscriptionError> {
        self.gate(ChannelName::Ohlc)?;
        self.ws.subscribe(
            ChannelName::Ohlc,
            pairs,
            SubscribeParams::Ohlc { interval, snapshot },
        )
    }

    // No subscribe_level3: deferred to v3 (needs the L3 WS connection). No
    // `instrument` channel either — use REST `/0/public/AssetPairs` and
    // `/0/public/Assets` for catalog data.

    /// Subscribe to the channel-wide `status` (system status) channel.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::NoHandlerRegistered`] — no `status` handler at call time; register via `on_system_status` first.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; nothing was registered, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn subscribe_system_status(&self) -> Result<(), SubscriptionError> {
        self.gate(ChannelName::Status)?;
        self.ws
            .subscribe(ChannelName::Status, Vec::new(), SubscribeParams::Status)
    }

    /// Subscribe to the channel-wide `executions` channel (auth WS).
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::NoHandlerRegistered`] — no `executions` handler at call time; register via `on_executions` first.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; nothing was registered, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn subscribe_executions(&self) -> Result<(), SubscriptionError> {
        self.gate(ChannelName::Executions)?;
        self.ws.subscribe(
            ChannelName::Executions,
            Vec::new(),
            SubscribeParams::Executions,
        )
    }

    /// Subscribe to the channel-wide `balances` channel (auth WS).
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::NoHandlerRegistered`] — no `balances` handler at call time; register via `on_balances` first.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; nothing was registered, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn subscribe_balances(&self) -> Result<(), SubscriptionError> {
        self.gate(ChannelName::Balances)?;
        self.ws
            .subscribe(ChannelName::Balances, Vec::new(), SubscribeParams::Balances)
    }

    /// Unsubscribe `pairs` from the `ticker` channel.
    ///
    /// Decrements each `(channel, pair)` refcount — call once per matching
    /// `subscribe_*`. Not idempotent: a surplus call can consume another plain
    /// subscriber's slot; guard-held refs are never consumed by a bare release.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; the registry was not mutated, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn unsubscribe_ticker(&self, pairs: Vec<Symbol>) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe(ChannelName::Ticker, pairs)
    }

    /// Unsubscribe `pairs` from the `book` channel.
    ///
    /// Decrements each `(book, pair)` refcount. [`Self::unsubscribe_book_raw`]
    /// releases the same key — call one per subscribe, never both.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; the registry was not mutated, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn unsubscribe_book(&self, pairs: Vec<Symbol>) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe(ChannelName::Book, pairs)
    }

    /// Unsubscribe `pairs` from the `book` (raw) channel.
    ///
    /// Decrements each `(book, pair)` refcount. [`Self::unsubscribe_book`]
    /// releases the same key — call one per subscribe, never both.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; the registry was not mutated, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn unsubscribe_book_raw(&self, pairs: Vec<Symbol>) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe(ChannelName::Book, pairs)
    }

    /// Unsubscribe `pairs` from the `trade` channel.
    ///
    /// Decrements each `(channel, pair)` refcount — call once per matching
    /// `subscribe_*`. Not idempotent: a surplus call can consume another plain
    /// subscriber's slot; guard-held refs are never consumed by a bare release.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; the registry was not mutated, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn unsubscribe_trade(&self, pairs: Vec<Symbol>) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe(ChannelName::Trade, pairs)
    }

    /// Unsubscribe `pairs` from the `ohlc` channel. Kraken matches teardown on
    /// `(channel, symbol, interval)`; the wire frame echoes the registry-held
    /// subscribe interval, so `_interval` only documents intent.
    ///
    /// Decrements each `(channel, pair)` refcount — call once per matching
    /// `subscribe_*`. Not idempotent: a surplus call can consume another plain
    /// subscriber's slot; guard-held refs are never consumed by a bare release.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; the registry was not mutated, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn unsubscribe_ohlc(
        &self,
        pairs: Vec<Symbol>,
        _interval: OhlcInterval,
    ) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe(ChannelName::Ohlc, pairs)
    }

    /// Unsubscribe from the channel-wide `status` channel.
    ///
    /// Decrements the channel-wide refcount — call once per matching
    /// `subscribe_*`. Not idempotent: a surplus call can consume another plain
    /// subscriber's slot; guard-held refs are never consumed by a bare release.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; the registry was not mutated, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn unsubscribe_system_status(&self) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe(ChannelName::Status, Vec::new())
    }

    /// Unsubscribe from the channel-wide `executions` channel.
    ///
    /// Decrements the channel-wide refcount — call once per matching
    /// `subscribe_*`. Not idempotent: a surplus call can consume another plain
    /// subscriber's slot; guard-held refs are never consumed by a bare release.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; the registry was not mutated, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn unsubscribe_executions(&self) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe(ChannelName::Executions, Vec::new())
    }

    /// Unsubscribe from the channel-wide `balances` channel.
    ///
    /// Decrements the channel-wide refcount — call once per matching
    /// `subscribe_*`. Not idempotent: a surplus call can consume another plain
    /// subscriber's slot; guard-held refs are never consumed by a bare release.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; the registry was not mutated, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn unsubscribe_balances(&self) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe(ChannelName::Balances, Vec::new())
    }

    /// Force-unsubscribe everything, ignoring subscriber refcounts; `Failed`
    /// entries are removed silently.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; nothing was torn down, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn unsubscribe_all(&self) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe_all()
    }

    /// Force-unsubscribe every subscription on `channel` — semantics as
    /// [`Self::unsubscribe_all`]. `BookRaw` tears down the shared `Book` entries.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — caller→I/O queue full; nothing was torn down, safe to retry.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn unsubscribe_channel(&self, channel: ChannelName) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe_channel(channel)
    }

    /// Point-in-time snapshot, one row per `(channel, pair)`: channel-wide
    /// rows first, then per-pair by wire name + symbol. May trail in-flight
    /// calls; retained `Failed` rows are included.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died (or the mirror writer panicked); streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`; a stale snapshot would lie.
    pub fn list_active(&self) -> Result<Vec<SubscriptionInfo>, SubscriptionError> {
        self.ws.list_active()
    }

    /// Point-in-time refs for one channel, sorted with the channel-wide row
    /// first; `BookRaw` returns the shared `Book` rows.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died (or the mirror writer panicked); streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`; a stale snapshot would lie.
    pub fn find_by_channel(
        &self,
        channel: ChannelName,
    ) -> Result<Vec<SubscriptionRef>, SubscriptionError> {
        self.ws.find_by_channel(channel)
    }

    /// Point-in-time per-state counts plus a per-channel histogram; histogram
    /// keys are wire channels (`BookRaw` counts under `Book`).
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died (or the mirror writer panicked); streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`; a stale snapshot would lie.
    pub fn status_summary(&self) -> Result<SubscriptionsSummary, SubscriptionError> {
        self.ws.status_summary()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::SystemClock;
    use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig, PresenceMirror};
    use std::sync::atomic::AtomicU64;

    fn make_namespace_with_bus() -> (SubscriptionNamespace, Arc<WsSurface>, Arc<DispatchEventBus>) {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            clock,
        ));
        let mirror: PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let alloc = Arc::new(AtomicU64::new(1));
        let ws = Arc::new(WsSurface::new(Arc::clone(&bus), mirror, alloc));
        (SubscriptionNamespace::new(Arc::clone(&ws)), ws, bus)
    }

    fn make_namespace() -> (SubscriptionNamespace, Arc<WsSurface>) {
        let (ns, ws, _bus) = make_namespace_with_bus();
        (ns, ws)
    }

    #[test]
    fn subscribe_after_loop_death_rejects_loopdead() {
        use crate::error::ApiError;
        let (ns, _ws, bus) = make_namespace_with_bus();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::Panic,
        );
        let err = ns
            .subscribe_ticker(vec![Symbol::new("BTC/USD").unwrap()], None, None)
            .unwrap_err();
        assert!(matches!(err, SubscriptionError::LoopDead));
        assert_eq!(err.code(), "LOOP_DEAD");
        assert_eq!(err.category(), crate::error::ErrorCategory::Client);
        assert!(!err.retryable());
    }

    #[test]
    fn ws_subscribe_unsubscribe_after_loop_death_reject_loopdead() {
        use crate::error::ApiError;
        let (_ns, ws, bus) = make_namespace_with_bus();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::Panic,
        );
        let pair = vec![Symbol::new("BTC/USD").unwrap()];
        let sub_err = ws
            .subscribe(
                ChannelName::Ticker,
                pair.clone(),
                SubscribeParams::Ticker {
                    snapshot: None,
                    event_trigger: None,
                },
            )
            .unwrap_err();
        assert!(matches!(sub_err, SubscriptionError::LoopDead));
        assert!(!sub_err.retryable());
        assert!(matches!(
            ws.unsubscribe(ChannelName::Ticker, pair),
            Err(SubscriptionError::LoopDead)
        ));
    }

    #[test]
    fn subscribe_ticker_without_handler_rejects() {
        let (ns, _ws) = make_namespace();
        let err = ns
            .subscribe_ticker(vec![Symbol::new("BTC/USD").unwrap()], None, None)
            .unwrap_err();
        assert!(matches!(
            err,
            SubscriptionError::NoHandlerRegistered {
                channel: ChannelName::Ticker
            }
        ));
    }

    #[test]
    fn channel_wide_subscribe_without_handler_rejects() {
        let (ns, _ws) = make_namespace();
        let err = ns.subscribe_balances().unwrap_err();
        assert!(matches!(
            err,
            SubscriptionError::NoHandlerRegistered {
                channel: ChannelName::Balances
            }
        ));
    }

    #[test]
    fn read_models_on_empty_mirror_return_ok_empty() {
        let (ns, _ws) = make_namespace();
        assert_eq!(ns.list_active().unwrap(), Vec::new());
        assert_eq!(ns.find_by_channel(ChannelName::Book).unwrap(), Vec::new());
        let summary = ns.status_summary().unwrap();
        assert_eq!(summary.total, 0);
        assert_eq!(summary.active, 0);
        assert_eq!(summary.pending, 0);
        assert_eq!(summary.failed, 0);
        assert!(summary.by_channel.is_empty());
    }

    #[test]
    fn read_models_project_and_sort_mirror_rows() {
        use crate::conn::subscription_registry::{EntrySubState, MirrorRow};
        use crate::types::MonotonicInstant;
        let (ns, ws) = make_namespace();
        let mirror = ws.subscription_mirror_handle();
        let now = MonotonicInstant::now();
        {
            let mut m = mirror.write().unwrap();
            m.insert(
                (ChannelName::Ticker, Some(Symbol::new("ETH/USD").unwrap())),
                MirrorRow {
                    state: EntrySubState::Acked,
                    registered_at: now,
                },
            );
            m.insert(
                (ChannelName::Book, Some(Symbol::new("BTC/USD").unwrap())),
                MirrorRow {
                    state: EntrySubState::Pending,
                    registered_at: now,
                },
            );
            m.insert(
                (ChannelName::Executions, None),
                MirrorRow {
                    state: EntrySubState::Terminated {
                        cause:
                            crate::api::subscription::TerminationCause::NonTransientWireRejection,
                        last_error: Some("EGeneral:Permission denied".to_string()),
                    },
                    registered_at: now,
                },
            );
        }

        let rows = ns.list_active().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].channel, ChannelName::Executions);
        assert_eq!(rows[0].pair, None);
        assert!(matches!(
            rows[0].state,
            crate::api::subscription_types::SubscriptionState::Failed(
                crate::api::subscription_types::SubscribeFailureCause::NonTransientWireRejection { .. }
            )
        ));
        assert_eq!(rows[1].channel, ChannelName::Book);
        assert_eq!(
            rows[1].state,
            crate::api::subscription_types::SubscriptionState::Pending
        );
        assert_eq!(rows[2].channel, ChannelName::Ticker);
        assert_eq!(
            rows[2].state,
            crate::api::subscription_types::SubscriptionState::Active
        );

        let refs = ns.find_by_channel(ChannelName::BookRaw).unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].channel, ChannelName::Book);
        assert_eq!(
            refs[0].registered_at_monotonic,
            now.as_duration().as_millis() as u64
        );

        let summary = ns.status_summary().unwrap();
        assert_eq!(summary.total, 3);
        assert_eq!(summary.active, 1);
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.by_channel.len(), 3);
        assert_eq!(summary.by_channel[&ChannelName::Book], 1);
    }

    #[test]
    fn unsubscribe_all_and_channel_post_deregister_all() {
        use crate::dispatch::CallerInbound;
        let (ns, _ws, bus) = make_namespace_with_bus();
        let mut rx = bus.take_caller_to_io_rx().expect("rx present");

        ns.unsubscribe_all().unwrap();
        match rx.try_recv().expect("one post") {
            CallerInbound::RegistryMutation {
                mutation: crate::dispatch::RegistryMutationOp::DeregisterAll { channel },
                ..
            } => assert_eq!(channel, None),
            other => panic!("expected DeregisterAll, got {other:?}"),
        }

        ns.unsubscribe_channel(ChannelName::Ohlc).unwrap();
        match rx.try_recv().expect("one post") {
            CallerInbound::RegistryMutation {
                mutation: crate::dispatch::RegistryMutationOp::DeregisterAll { channel },
                ..
            } => assert_eq!(channel, Some(ChannelName::Ohlc)),
            other => panic!("expected DeregisterAll, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_ops_after_loop_death_reject_loopdead() {
        let (ns, _ws, bus) = make_namespace_with_bus();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::Panic,
        );
        assert!(matches!(
            ns.unsubscribe_all(),
            Err(SubscriptionError::LoopDead)
        ));
        assert!(matches!(
            ns.unsubscribe_channel(ChannelName::Book),
            Err(SubscriptionError::LoopDead)
        ));
        assert!(matches!(ns.list_active(), Err(SubscriptionError::LoopDead)));
        assert!(matches!(
            ns.find_by_channel(ChannelName::Book),
            Err(SubscriptionError::LoopDead)
        ));
        assert!(matches!(
            ns.status_summary(),
            Err(SubscriptionError::LoopDead)
        ));
    }

    #[test]
    fn single_key_posts_after_close_reject_client_closed() {
        let (ns, ws, bus) = make_namespace_with_bus();
        drop(bus.take_caller_to_io_rx().expect("rx present"));
        assert!(matches!(
            ws.subscribe_with_ref(
                ChannelName::Executions,
                Vec::new(),
                crate::conn::SubscribeParams::Executions,
                None,
            ),
            Err(SubscriptionError::ClientClosed)
        ));
        assert!(matches!(
            ns.unsubscribe_ticker(vec![Symbol::new("BTC/USD").unwrap()]),
            Err(SubscriptionError::ClientClosed)
        ));
        assert!(matches!(
            ns.unsubscribe_balances(),
            Err(SubscriptionError::ClientClosed)
        ));
    }

    #[test]
    fn single_key_posts_on_full_queue_reject_queue_full() {
        use crate::dispatch::CallerInbound;
        let (ns, _ws, bus) = make_namespace_with_bus();
        let _rx = bus.take_caller_to_io_rx().expect("rx present");
        let mut filled = false;
        for _ in 0..1_000_000 {
            if bus
                .try_post_caller_inbound(CallerInbound::RegistryMutation {
                    url: crate::types::WsUrl::Public,
                    mutation: crate::dispatch::RegistryMutationOp::Deregister {
                        channel: ChannelName::Status,
                        pair: None,
                    },
                })
                .is_err()
            {
                filled = true;
                break;
            }
        }
        assert!(filled, "caller queue never saturated");
        assert!(matches!(
            ns.unsubscribe_ticker(vec![Symbol::new("BTC/USD").unwrap()]),
            Err(SubscriptionError::QueueFull)
        ));
        assert!(matches!(
            ns.unsubscribe_balances(),
            Err(SubscriptionError::QueueFull)
        ));
    }

    #[test]
    fn post_close_reads_reject_client_closed() {
        let (ns, _ws, bus) = make_namespace_with_bus();
        bus.begin_shutdown();
        assert!(matches!(
            ns.list_active(),
            Err(SubscriptionError::ClientClosed)
        ));
        assert!(matches!(
            ns.find_by_channel(ChannelName::Book),
            Err(SubscriptionError::ClientClosed)
        ));
        assert!(matches!(
            ns.status_summary(),
            Err(SubscriptionError::ClientClosed)
        ));
    }

    #[test]
    fn crashed_loop_posts_reject_loopdead_not_client_closed() {
        let (ns, _ws, bus) = make_namespace_with_bus();
        drop(bus.take_caller_to_io_rx().expect("rx present"));
        bus.on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::Panic,
        );
        assert!(matches!(
            ns.unsubscribe_ticker(vec![Symbol::new("BTC/USD").unwrap()]),
            Err(SubscriptionError::LoopDead)
        ));
        assert!(matches!(
            ns.unsubscribe_balances(),
            Err(SubscriptionError::LoopDead)
        ));
    }

    #[test]
    fn poisoned_mirror_reads_reject_loopdead() {
        let (ns, ws, _bus) = make_namespace_with_bus();
        let mirror = ws.subscription_mirror_handle();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mirror.write().unwrap();
            panic!("poison the mirror");
        }));
        assert!(matches!(ns.list_active(), Err(SubscriptionError::LoopDead)));
        assert!(matches!(
            ns.find_by_channel(ChannelName::Book),
            Err(SubscriptionError::LoopDead)
        ));
        assert!(matches!(
            ns.status_summary(),
            Err(SubscriptionError::LoopDead)
        ));
    }
}
