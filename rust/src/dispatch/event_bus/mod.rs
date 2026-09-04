//! `DispatchEventBus` — the async Reactor boundary and pub-sub event bus.
//! Channel discipline, correlation, cycle-break: docs/guides/streaming.md.

mod events;
mod inbound;

pub use events::*;
pub use inbound::*;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::sync::mpsc;

use crate::clock::Clock;
use crate::dispatch::handler_registry::{DataDelivery, HandlerId};
use crate::types::{ChannelName, MonotonicInstant, QueueFullError, SubscriberHandle};

/// Long-lived subscriber callback; runs synchronously on the dispatch reactor.
pub type EventCallback = Arc<dyn Fn(&EventEnvelope) + Send + Sync + 'static>;

/// One-shot subscriber callback for `subscribe_correlated`; removed after the
/// first match.
/// Returns whether it ACCEPTED the envelope. A waiter cancelled between being
/// removed for delivery and being fired declines, and the caller must then not
/// drop the envelope on the floor.
pub type CorrelatedCallback = Box<dyn FnOnce(&EventEnvelope) -> bool + Send + Sync + 'static>;

enum SubscriberHandlerKind {
    LongLived(EventCallback),
    OneShot(CorrelatedCallback),
}

struct SubscriberRecord {
    handle: SubscriberHandle,
    max_version: u16,
    handler: SubscriberHandlerKind,
}

/// Correlated miss-buffer cap (drop-oldest FIFO); the low-volume,
/// consumed-promptly property is a load-bearing invariant — docs/guides/streaming.md.
const CORRELATED_MISS_BUFFER_CAP: usize = 256;

/// Bound on the close-path dispatch-ring drain.
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Live one-shot waiters PLUS a bounded miss-buffer of completions dispatched
/// before their waiter registered. ONE `Mutex` so take-or-insert stays atomic.
struct CorrelatedRegistry {
    /// All waiters on a key resolve together on the first match — observing one
    /// operation two ways must not strand either observer.
    live: HashMap<(EventType, u64), Vec<SubscriberRecord>>,
    /// Dispatched-but-unwaited completions; a later `subscribe_correlated`
    /// for a matching key consumes its entry.
    missed: VecDeque<((EventType, u64), EventEnvelope)>,
    missed_cap: usize,
    /// Set when a loop death commits, intended teardown included: past it only
    /// the incident's own terminal shapes resolve an await. Mid-close keeps
    /// latched misses; genuine clears.
    death_committed: bool,
    /// Set with the purge above, so "was this a GENUINE death" is readable under
    /// this lock — `loop_failed` latches later.
    death_purged: bool,
}

impl CorrelatedRegistry {
    fn new(missed_cap: usize) -> Self {
        Self {
            live: HashMap::new(),
            missed: VecDeque::new(),
            missed_cap,
            death_committed: false,
            death_purged: false,
        }
    }

    /// Latch a completion that found no live waiter; drop-oldest on overflow.
    fn latch_missed(&mut self, key: (EventType, u64), event: EventEnvelope) {
        if self.missed.len() >= self.missed_cap {
            // Overflow breaks the consumed-promptly invariant: name the evicted
            // completion so a lost correlated await is diagnosable.
            if let Some(((evicted, corr), _)) = self.missed.front() {
                tracing::warn!(
                    target: "kraken_sdk::dispatch_reactor",
                    evicted_event_type = ?evicted,
                    correlation_key = corr,
                    cap = self.missed_cap,
                    "correlated miss-buffer full; evicting oldest completion"
                );
            }
            self.missed.pop_front();
        }
        self.missed.push_back((key, event));
    }

    /// Consume a latched completion for `key`, if present.
    fn take_missed(&mut self, key: (EventType, u64)) -> Option<EventEnvelope> {
        let idx = self.missed.iter().position(|(k, _)| *k == key)?;
        self.missed.remove(idx).map(|(_, env)| env)
    }
}

/// State of one of the two reactor loops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReactorState {
    NotStarted,
    Running,
    Stopped,
    Failed(LoopFailureCause),
}

/// One item on the `io_to_dispatch` ring. ONE FIFO, so data ordering is
/// preserved and the drop-oldest cap is shared across events and data.
pub(crate) enum DispatchItem {
    Event(EventEnvelope),
    Data(DataDelivery, DataRouting),
}

/// Routing + instrumentation metadata for a [`DispatchItem::Data`].
pub(crate) struct DataRouting {
    pub channel: ChannelName,
    pub handler_id: HandlerId,
}

/// Bounded drop-oldest ring (`tokio::mpsc` is reject-only). The lock is held
/// only across push/pop, never across `.await`.
pub(crate) struct DropOldestRing<T> {
    inner: Mutex<VecDeque<T>>,
    capacity: usize,
    notify: Notify,
    closed: std::sync::atomic::AtomicBool,
}

impl<T> DropOldestRing<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            notify: Notify::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Signal a graceful close: `recv` drains the remaining items, then returns
    /// `None`. `notify_one` stores a permit so an in-flight `recv` never misses it.
    pub(crate) fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        self.notify.notify_one();
    }

    /// Push an item; drop oldest if full. Returns `true` if an item was dropped —
    /// caller emits the (debounced) `QueueFullWarning`.
    pub(crate) fn push(&self, item: T) -> bool {
        // Poison-tolerant: reachable from CancelEmitGuard::Drop — recover,
        // never panic in Drop.
        let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let dropped = if guard.len() >= self.capacity {
            guard.pop_front();
            true
        } else {
            false
        };
        guard.push_back(item);
        drop(guard);
        self.notify.notify_one();
        dropped
    }

    /// Current queued depth, for the `QueueDepthSample` emit.
    pub(crate) fn depth(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Async receive. Returns `None` only once the ring is closed AND drained,
    /// so a just-published terminal event is still delivered before exit.
    pub(crate) async fn recv(&self) -> Option<T> {
        loop {
            {
                let mut guard = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(item) = guard.pop_front() {
                    return Some(item);
                }
            }
            // Empty: exit only once closed (drain complete); else wait for more.
            if self.closed.load(std::sync::atomic::Ordering::Acquire) {
                return None;
            }
            self.notify.notified().await;
        }
    }
}

/// Bus construction knobs.
#[derive(Debug, Clone, Copy)]
pub struct DispatchEventBusConfig {
    /// I/O→dispatch ring capacity (events). Default 8_192.
    pub io_to_dispatch_capacity: usize,
    /// Caller→I/O queue capacity (messages). Default 2_048.
    pub caller_to_io_capacity: usize,
    /// `QueueDepthSample` emission interval (ms). Default 100.
    pub queue_depth_sample_interval_ms: u32,
    /// `SlowCallbackWarning` threshold (ms). Default 50.
    // Live reads go through Knobs; the config copy is read via test-support only.
    #[allow(dead_code)]
    pub slow_callback_threshold_ms: u32,
    /// Per-entry subscribe-ack timeout (ms), armed in the I/O Reactor. Default 5_000.
    #[allow(dead_code)]
    pub subscribe_ack_timeout_ms: u32,
}

impl DispatchEventBusConfig {
    /// The defaults documented on each field.
    pub fn defaults() -> Self {
        Self {
            io_to_dispatch_capacity: 8192,
            caller_to_io_capacity: 2048,
            queue_depth_sample_interval_ms: 100,
            slow_callback_threshold_ms: 50,
            subscribe_ack_timeout_ms: 5_000,
        }
    }

    /// Build from the resolved `Knobs`. Queue capacities are construction-only;
    /// the timing knobs seed a snapshot.
    pub fn from_knobs(knobs: &crate::build::knobs::Knobs) -> Self {
        use std::sync::atomic::Ordering;
        Self {
            io_to_dispatch_capacity: knobs.io_to_dispatch_capacity as usize,
            caller_to_io_capacity: knobs.caller_to_io_capacity as usize,
            queue_depth_sample_interval_ms: 100,
            slow_callback_threshold_ms: knobs.slow_callback_threshold_ms.load(Ordering::Relaxed),
            subscribe_ack_timeout_ms: knobs.subscribe_ack_timeout_ms.load(Ordering::Relaxed),
        }
    }
}

impl Default for DispatchEventBusConfig {
    fn default() -> Self {
        Self::defaults()
    }
}

/// Pub-sub event bus and reactor host. `io_to_dispatch` is the drop-oldest
/// ring; `caller_to_io` is reject-on-full `tokio::mpsc`.
pub struct DispatchEventBus {
    io_to_dispatch: Arc<DropOldestRing<DispatchItem>>,
    caller_to_io_tx: mpsc::Sender<CallerInbound>,
    caller_to_io_rx: Mutex<Option<mpsc::Receiver<CallerInbound>>>,

    subscribers: RwLock<HashMap<EventType, Vec<SubscriberRecord>>>,
    correlated: Mutex<CorrelatedRegistry>,

    dispatch_reactor_state: Mutex<ReactorState>,

    dispatch_reactor_join: Mutex<Option<tokio::task::JoinHandle<()>>>,

    /// Terminal "a reactor loop died" flag; latched once, never cleared (v1 has
    /// no auto-restart). Entry points read it to reject synchronously.
    loop_failed: std::sync::atomic::AtomicBool,

    /// Latched by the intentional-teardown paths BEFORE they abort a reactor,
    /// so `on_loop_death` does not mis-report the abort as a loop death.
    shutting_down: std::sync::atomic::AtomicBool,

    /// Latched on EVERY reactor-loop death, intended teardown included (unlike
    /// `loop_failed`). A correlated await armed after the death drain has run
    /// reads this to self-resolve instead of hanging.
    loop_death_observed: std::sync::atomic::AtomicBool,

    clock: Arc<dyn Clock>,
    next_subscriber_id: AtomicU64,

    /// Cumulative `io_to_dispatch` drops, surfaced in `QueueFullWarning`.
    dropped_count: AtomicU64,
    /// Last `QueueFullWarning` emit — the debounce gate.
    last_warning_at: Mutex<Option<MonotonicInstant>>,

    cfg: DispatchEventBusConfig,

    /// Shared knob holder; runtime-mutable timing knobs read fresh so
    /// `Client::set_knob` is honoured.
    knobs: Arc<crate::build::knobs::Knobs>,

    /// Full-rejected Drop teardowns waiting for a free `caller_to_io` slot.
    pending_teardowns: Mutex<Vec<CallerInbound>>,
    has_pending_teardowns: std::sync::atomic::AtomicBool,
    last_teardown_flush_warn_at: Mutex<Option<MonotonicInstant>>,
}

impl DispatchEventBus {
    /// Construct with placeholder `Knobs::defaults()`. `set_knobs()` is MANDATORY
    /// before starting any reactor — the defaults are not production-safe.
    pub(crate) fn new(cfg: DispatchEventBusConfig, clock: Arc<dyn Clock>) -> Self {
        let (caller_to_io_tx, caller_to_io_rx) = mpsc::channel(cfg.caller_to_io_capacity);
        Self {
            io_to_dispatch: Arc::new(DropOldestRing::new(cfg.io_to_dispatch_capacity)),
            caller_to_io_tx,
            caller_to_io_rx: Mutex::new(Some(caller_to_io_rx)),
            subscribers: RwLock::new(HashMap::new()),
            correlated: Mutex::new(CorrelatedRegistry::new(CORRELATED_MISS_BUFFER_CAP)),
            dispatch_reactor_state: Mutex::new(ReactorState::NotStarted),
            dispatch_reactor_join: Mutex::new(None),
            loop_failed: std::sync::atomic::AtomicBool::new(false),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
            loop_death_observed: std::sync::atomic::AtomicBool::new(false),
            clock,
            next_subscriber_id: AtomicU64::new(1),
            dropped_count: AtomicU64::new(0),
            last_warning_at: Mutex::new(None),
            cfg,
            knobs: Arc::new(crate::build::knobs::Knobs::defaults()),
            pending_teardowns: Mutex::new(Vec::new()),
            has_pending_teardowns: std::sync::atomic::AtomicBool::new(false),
            last_teardown_flush_warn_at: Mutex::new(None),
        }
    }

    /// Install the resolved `Arc<Knobs>` (at `.build()`, BEFORE the reactors start).
    pub(crate) fn set_knobs(&mut self, knobs: Arc<crate::build::knobs::Knobs>) {
        self.knobs = knobs;
    }

    pub(crate) fn knobs(&self) -> &Arc<crate::build::knobs::Knobs> {
        &self.knobs
    }

    pub(crate) fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// Publish onto `io_to_dispatch`; subscribers fire on the Dispatch Reactor,
    /// NOT here. On full: drop-oldest + debounced `QueueFullWarning`.
    pub(crate) fn publish(&self, event: EventEnvelope) {
        let dropped = self.io_to_dispatch.push(DispatchItem::Event(event));
        if !dropped {
            return;
        }
        self.record_io_to_dispatch_drop();
    }

    /// Enqueue a decoded user-data callback. Same drop-oldest + warning as
    /// `publish`, so a `QueueFullWarning` means user data was gapped.
    pub(crate) fn publish_data(&self, delivery: DataDelivery, routing: DataRouting) {
        let dropped = self
            .io_to_dispatch
            .push(DispatchItem::Data(delivery, routing));
        if !dropped {
            return;
        }
        self.record_io_to_dispatch_drop();
    }

    /// Cumulative drop-oldest evictions, incremented producer-side so it is
    /// observable even while the dispatch loop is parked.
    #[cfg(test)]
    pub(crate) fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// Drop-accounting + debounced `QueueFullWarning` emit, shared by `publish`
    /// and `publish_data`.
    fn record_io_to_dispatch_drop(&self) {
        let total = self.dropped_count.fetch_add(1, Ordering::Relaxed) + 1;

        // Debounce: a drop storm's warning can itself drop — avoid recursive
        // ring saturation.
        let now = self.clock.now();
        let should_emit = {
            let mut last = self
                .last_warning_at
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let interval =
                Duration::from_millis(u64::from(self.cfg.queue_depth_sample_interval_ms));
            let elapsed_ok = match *last {
                None => true,
                Some(prev) => now.0.saturating_sub(prev.0) >= interval,
            };
            if elapsed_ok {
                *last = Some(now);
            }
            elapsed_ok
        };
        if !should_emit {
            return;
        }

        let warning = EventEnvelope {
            event_type: EventType::QueueFullWarning,
            event_version: 1,
            timestamp_monotonic: now,
            request_id: None,
            payload: EventPayload::QueueFullWarning {
                queue: QueueName::IoToDispatch,
                dropped_count: total,
                capacity: self.io_to_dispatch.capacity,
            },
        };
        // Direct ring push — never recurse through publish. Still count the drop.
        if self.io_to_dispatch.push(DispatchItem::Event(warning)) {
            self.dropped_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Register a long-lived subscriber; fires on the Dispatch Reactor for every
    /// matching event with `event_version <= max_version`, until `unsubscribe`.
    pub fn subscribe(
        self: &Arc<Self>,
        event_type: EventType,
        handler: EventCallback,
        max_version: u16,
    ) -> SubscriberHandle {
        let id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        let handle = SubscriberHandle { id };
        let record = SubscriberRecord {
            handle,
            max_version,
            handler: SubscriberHandlerKind::LongLived(handler),
        };
        self.subscribers
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(event_type)
            .or_default()
            .push(record);
        // Lazy-start AFTER registering so a never-ready() client still gets
        // events. Idempotent; skipped off-runtime (events buffer until a later start).
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            self.start_dispatch_reactor(&rt);
        }
        handle
    }

    /// One-shot correlation subscriber for `(event_type, correlation_key)`, matched
    /// against `EventEnvelope.request_id`, auto-unsubscribed on first fire. Past a
    /// committed death only the incident's terminal arrives — arm off
    /// [`Self::loop_death_observed`] to self-resolve.
    pub(crate) fn subscribe_correlated(
        &self,
        event_type: EventType,
        correlation_key: u64,
        handler: CorrelatedCallback,
    ) -> SubscriberHandle {
        let id = self.next_subscriber_id.fetch_add(1, Ordering::Relaxed);
        let handle = SubscriberHandle { id };
        let record = SubscriberRecord {
            handle,
            max_version: u16::MAX,
            handler: SubscriberHandlerKind::OneShot(handler),
        };
        let key = (event_type, correlation_key);
        // Atomic under ONE lock: consume a latched miss OR register the live
        // waiter. Fire outside the lock — the handler may re-enter the bus.
        let to_fire: Option<(EventEnvelope, SubscriberRecord)> = {
            let mut reg = self
                .correlated
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match reg.take_missed(key) {
                Some(env) => Some((env, record)),
                None => {
                    reg.live.entry(key).or_default().push(record);
                    None
                }
            }
        };
        if let Some((env, record)) = to_fire {
            // Same accept/decline contract as deliver_correlated: a cancelled or
            // panicking waiter must not drop the terminal on the floor.
            if !self.fire_correlated(&env, record) {
                self.relatch_correlated(&env);
            }
        }
        handle
    }

    /// Remove a subscriber. Idempotent — calling twice with the same handle is fine.
    pub fn unsubscribe(&self, handle: SubscriberHandle) {
        {
            let mut subs = self
                .subscribers
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            for vec in subs.values_mut() {
                vec.retain(|r| r.handle != handle);
            }
        }
        {
            let mut reg = self
                .correlated
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            // Only live waiters are keyed by handle; the miss-buffer is
            // pre-subscription, untouched by unsubscribe.
            for records in reg.live.values_mut() {
                records.retain(|r| r.handle != handle);
            }
            reg.live.retain(|_, records| !records.is_empty());
        }
    }

    /// Drain all `EventEnvelope`s from the ring (`Data` items filtered out).
    /// Never call from a running reactor context.
    #[cfg(test)]
    pub(crate) fn test_drain_published(&self) -> Vec<EventEnvelope> {
        let mut guard = self
            .io_to_dispatch
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .drain(..)
            .filter_map(|item| match item {
                DispatchItem::Event(env) => Some(env),
                DispatchItem::Data(..) => None,
            })
            .collect()
    }

    pub(crate) fn subscriber_count(&self, event_type: EventType) -> usize {
        self.subscribers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&event_type)
            .map_or(0, |v| v.len())
    }

    /// Latched completions awaiting a late arm.
    #[cfg(test)]
    pub(crate) fn correlated_missed_count(&self) -> usize {
        self.correlated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .missed
            .len()
    }

    /// Count of live correlated one-shot waiters across all keys.
    #[cfg(test)]
    pub(crate) fn correlated_live_count(&self) -> usize {
        self.correlated
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .live
            .values()
            .map(Vec::len)
            .sum()
    }

    pub(crate) fn try_post_caller_inbound(
        &self,
        item: CallerInbound,
    ) -> Result<(), QueueFullError> {
        self.caller_to_io_tx.try_send(item).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => QueueFullError {
                queue: "caller_to_io",
            },
            mpsc::error::TrySendError::Closed(_) => QueueFullError {
                queue: "caller_to_io (closed)",
            },
        })
    }

    /// Like `try_post_caller_inbound` but returns the rejected item on failure.
    /// Also reports WHY: `Full` = back-pressure (maps to retryable
    /// `queue_full()`), `Closed` = reactor receiver dropped (`not_open()`).
    pub(crate) fn try_post_caller_inbound_recovering_kind(
        &self,
        item: CallerInbound,
    ) -> Result<(), (CallerInbound, PostReject)> {
        self.caller_to_io_tx.try_send(item).map_err(|e| match e {
            mpsc::error::TrySendError::Full(item) => (item, PostReject::Full),
            mpsc::error::TrySendError::Closed(item) => (item, PostReject::Closed),
        })
    }

    /// Flush queued Drop teardowns into `caller_to_io`, leaving `reserve` free slots.
    pub(crate) fn flush_pending_teardowns_leaving(&self, reserve: usize) {
        if !self
            .has_pending_teardowns
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return;
        }
        if self.caller_to_io_tx.capacity() <= reserve {
            return;
        }
        let drained: Vec<CallerInbound> = {
            let mut pending = self
                .pending_teardowns
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if pending.is_empty() {
                self.has_pending_teardowns
                    .store(false, std::sync::atomic::Ordering::Release);
                return;
            }
            let drained = std::mem::take(&mut *pending);
            // Clearing under the lock keeps a concurrent `requeue_pending_teardowns`
            // from having its `store(true)` clobbered back to false.
            self.has_pending_teardowns
                .store(false, std::sync::atomic::Ordering::Release);
            drained
        };
        let mut iter = drained.into_iter();
        while let Some(item) = iter.next() {
            if self.caller_to_io_tx.capacity() <= reserve {
                self.requeue_pending_teardowns(std::iter::once(item).chain(iter));
                return;
            }
            match self.caller_to_io_tx.try_send(item) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    return;
                }
                Err(mpsc::error::TrySendError::Full(item)) => {
                    let pending_len =
                        self.requeue_pending_teardowns(std::iter::once(item).chain(iter));
                    self.maybe_warn_teardown_flush_full(pending_len);
                    return;
                }
            }
        }
    }

    pub(crate) fn flush_pending_teardowns(&self) {
        self.flush_pending_teardowns_leaving(0);
    }

    fn requeue_pending_teardowns(&self, items: impl IntoIterator<Item = CallerInbound>) -> usize {
        let mut pending = self
            .pending_teardowns
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        pending.extend(items);
        let len = pending.len();
        self.has_pending_teardowns
            .store(!pending.is_empty(), std::sync::atomic::Ordering::Release);
        len
    }

    fn maybe_warn_teardown_flush_full(&self, pending_len: usize) {
        let now = self.clock.now();
        let should_emit = {
            let mut last = self
                .last_teardown_flush_warn_at
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let interval =
                Duration::from_millis(u64::from(self.cfg.queue_depth_sample_interval_ms));
            let elapsed_ok = match *last {
                None => true,
                Some(prev) => now.0.saturating_sub(prev.0) >= interval,
            };
            if elapsed_ok {
                *last = Some(now);
            }
            elapsed_ok
        };
        if !should_emit {
            return;
        }
        tracing::warn!(
            target: "kraken_sdk::dispatch",
            pending = pending_len,
            "teardown flush still Full; re-queued for a later flush"
        );
    }

    pub(crate) fn post_teardown(&self, item: CallerInbound) {
        self.flush_pending_teardowns();
        match self.try_post_caller_inbound_recovering_kind(item) {
            Ok(()) => {}
            Err((_, PostReject::Closed)) => {}
            Err((item, PostReject::Full)) => {
                let pending_len = self.requeue_pending_teardowns(std::iter::once(item));
                tracing::warn!(
                    target: "kraken_sdk::dispatch",
                    pending = pending_len,
                    "teardown post rejected (Full); queued for retry"
                );
            }
        }
    }

    /// Take the `caller_to_io` receiver half; `None` if already taken.
    pub(crate) fn take_caller_to_io_rx(&self) -> Option<mpsc::Receiver<CallerInbound>> {
        self.caller_to_io_rx
            .lock()
            .expect("caller_to_io_rx lock poisoned — fatal")
            .take()
    }

    /// `true` IFF the I/O Reactor has been started. Flips once and never back —
    /// NOT a mid-bring-up signal; used to fail-fast a WS op before `.ready()`.
    pub(crate) fn is_io_reactor_started(&self) -> bool {
        self.caller_to_io_rx
            .lock()
            .expect("caller_to_io_rx lock poisoned — fatal")
            .is_none()
    }

    /// Spawn the Dispatch Reactor task; it holds a `Weak` to break the bus↔task cycle.
    pub(crate) fn start_dispatch_reactor(self: &Arc<Self>, runtime: &tokio::runtime::Handle) {
        {
            // Spawn only from NotStarted so a lazy `on()` start and the `ready()`
            // start can't double-spawn (leaking the first loop).
            let mut state = self
                .dispatch_reactor_state
                .lock()
                .expect("dispatch_reactor_state lock poisoned — fatal");
            if !matches!(*state, ReactorState::NotStarted) {
                return;
            }
            *state = ReactorState::Running;
        }

        let weak = Arc::downgrade(self);
        let join = runtime.spawn(dispatch_reactor_loop(weak));
        *self
            .dispatch_reactor_join
            .lock()
            .expect("dispatch_reactor_join lock poisoned — fatal") = Some(join);
    }

    /// Graceful close: drain the ring to empty then exit — a hard abort could
    /// drop the `ClientClosedEvent`.
    pub(crate) async fn close_dispatch_and_drain(&self) {
        self.begin_shutdown();
        *self
            .dispatch_reactor_state
            .lock()
            .expect("dispatch_reactor_state lock poisoned — fatal") = ReactorState::Stopped;
        self.io_to_dispatch.close();
        let join = self
            .dispatch_reactor_join
            .lock()
            .expect("dispatch_reactor_join lock poisoned — fatal")
            .take();
        if let Some(join) = join {
            // Bounded drain: on overrun abort so close() returns (terminal event
            // may be lost).
            let abort = join.abort_handle();
            if tokio::time::timeout(CLOSE_DRAIN_TIMEOUT, join)
                .await
                .is_err()
            {
                abort.abort();
            }
        }
    }

    /// Stop the dispatch reactor. (The I/O reactor's `JoinHandle` is owned +
    /// aborted by `Client`, not the bus.)
    #[cfg(test)]
    pub(crate) fn stop_reactors(&self) {
        // Latch teardown + Stopped BEFORE aborting so the guard's drop does not
        // mis-latch LoopDead on an intended stop.
        self.begin_shutdown();
        *self
            .dispatch_reactor_state
            .lock()
            .expect("dispatch_reactor_state lock poisoned — fatal") = ReactorState::Stopped;
        if let Some(join) = self
            .dispatch_reactor_join
            .lock()
            .expect("dispatch_reactor_join lock poisoned — fatal")
            .take()
        {
            join.abort();
        }
    }

    #[cfg(test)]
    pub(crate) fn dispatch_reactor_state(&self) -> ReactorState {
        *self
            .dispatch_reactor_state
            .lock()
            .expect("dispatch_reactor_state lock poisoned — fatal")
    }

    /// Whether a reactor loop has died (terminal; never cleared — v1 does not
    /// auto-restart).
    pub(crate) fn is_loop_failed(&self) -> bool {
        self.loop_failed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Latch the intentional-teardown flag BEFORE tearing a reactor down, so the
    /// abort is NOT mis-reported as a loop death. Idempotent; never cleared.
    pub(crate) fn begin_shutdown(&self) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Has an intentional teardown begun? Read models gate on this so a
    /// post-close snapshot reads as `ClientClosed`, never as stale live rows.
    pub(crate) fn is_shutting_down(&self) -> bool {
        self.shutting_down
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Has ANY reactor loop died — intended teardown included? Unlike
    /// [`Self::is_loop_failed`], this also latches on a mid-close death, where
    /// no further completion events can arrive but `loop_failed` stays false.
    pub(crate) fn loop_death_observed(&self) -> bool {
        self.loop_death_observed
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Reactor-loop death. Idempotent (emit-once-per-incident) and PANIC-SAFE —
    /// called from `LoopDeathGuard::drop`, possibly mid-unwind. Fails every
    /// in-flight correlated await.
    pub(crate) fn on_loop_death(&self, loop_name: ReactorName, cause: LoopFailureCause) {
        use std::sync::atomic::Ordering;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let shutting_down = self.shutting_down.load(Ordering::Acquire);
            // A Stopped dispatch loop outside shutdown is an intended stop, not a
            // death: nothing to fail and nothing to suppress.
            if !shutting_down
                && loop_name == ReactorName::Dispatch
                && self
                    .dispatch_reactor_state
                    .lock()
                    .map(|g| *g == ReactorState::Stopped)
                    .unwrap_or(false)
            {
                self.loop_death_observed.store(true, Ordering::Release);
                return;
            }
            // Commit FIRST: past here only its own terminal resolves an await.
            // Mid-close keeps latched misses; a genuine death clears.
            {
                let mut reg = match self.correlated.lock() {
                    Ok(reg) => reg,
                    Err(poisoned) => poisoned.into_inner(),
                };
                reg.death_committed = true;
                if !shutting_down {
                    reg.missed.clear();
                    reg.death_purged = true;
                }
            }
            // After commit, before drain/return — late-armed await self-resolves.
            self.loop_death_observed.store(true, Ordering::Release);
            // Intended teardown: no LoopDead latch, no bus-wide event — but a
            // mid-close death must still resolve in-flight waiters.
            if shutting_down {
                self.resolve_live_correlated_on_death(loop_name, cause);
                return;
            }
            // Emit-once-per-incident: only the first death drains + emits.
            if self.loop_failed.swap(true, Ordering::AcqRel) {
                return;
            }
            if loop_name == ReactorName::Dispatch {
                if let Ok(mut g) = self.dispatch_reactor_state.lock() {
                    *g = ReactorState::Failed(cause);
                }
            }
            self.resolve_live_correlated_on_death(loop_name, cause);
            // Best-effort broadcast (undelivered if the dispatch loop is the dead one).
            if self
                .io_to_dispatch
                .push(DispatchItem::Event(Self::loop_failed_env(
                    loop_name,
                    cause,
                    self.clock.now(),
                    None,
                )))
            {
                self.dropped_count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    /// `LoopFailedEvent` envelope for one death incident.
    fn loop_failed_env(
        loop_name: ReactorName,
        cause: LoopFailureCause,
        now: MonotonicInstant,
        request_id: Option<u64>,
    ) -> EventEnvelope {
        EventEnvelope {
            event_type: EventType::LoopFailedEvent,
            event_version: 1,
            timestamp_monotonic: now,
            request_id,
            payload: EventPayload::LoopFailedEvent {
                loop_name,
                cause,
                failed_at_monotonic: now,
            },
        }
    }

    /// Drain live correlated waiters with a per-key death envelope.
    /// Leaves the miss-buffer alone — callers own that policy.
    fn resolve_live_correlated_on_death(&self, loop_name: ReactorName, cause: LoopFailureCause) {
        let now = self.clock.now();
        let by_key: Vec<((EventType, u64), Vec<SubscriberRecord>)> = match self.correlated.lock() {
            Ok(mut reg) => reg.live.drain().collect(),
            Err(poisoned) => poisoned.into_inner().live.drain().collect(),
        };
        let drained: Vec<((EventType, u64), SubscriberRecord)> = by_key
            .into_iter()
            .flat_map(|(key, records)| records.into_iter().map(move |r| (key, r)))
            .collect();
        for ((evt, key), record) in drained {
            if let SubscriberHandlerKind::OneShot(handler) = record.handler {
                // A `ready()` await matches ClientReady ⊕ ClientFailed — resolve
                // via ClientFailed; all other waiters get the generic LoopFailedEvent.
                let env = match evt {
                    EventType::ClientReady | EventType::ClientFailed => EventEnvelope {
                        event_type: EventType::ClientFailed,
                        event_version: 1,
                        timestamp_monotonic: now,
                        request_id: Some(key),
                        payload: EventPayload::ClientFailed {
                            cause: ClientFailureCause::LoopFailed,
                        },
                    },
                    _ => Self::loop_failed_env(loop_name, cause, now, Some(key)),
                };
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(&env)));
            }
        }
    }

    /// Resolve a correlated completion DIRECTLY, bypassing the ring, so a
    /// synchronous `Client::ready()` re-entry can't be drop-evicted.
    pub(crate) fn deliver_correlated(&self, event: &EventEnvelope) {
        let Some(key) = event.request_id else {
            return;
        };
        let one_shot = {
            let mut reg = self
                .correlated
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            // Past a committed death the incident's terminal is the outcome — a
            // completion behind it would report health the client no longer has.
            if reg.death_committed
                && !matches!(
                    event.event_type,
                    EventType::ClientFailed | EventType::LoopFailedEvent
                )
            {
                drop(reg);
                tracing::debug!(
                    target: "kraken_sdk::dispatch_reactor",
                    event_type = ?event.event_type,
                    correlation_key = key,
                    "completion produced after a loop death; dropped, the death is the outcome"
                );
                return;
            }
            match reg.live.remove(&(event.event_type, key)) {
                Some(records) => records,
                None => {
                    // No waiter yet — latch so a late subscribe_correlated still
                    // resolves (publish-before-subscribe race).
                    reg.latch_missed((event.event_type, key), event.clone());
                    Vec::new()
                }
            }
        };
        // Every waiter on the key resolves — one operation may be observed by the
        // completion await AND by a handle observer, and neither may be stranded.
        let had_waiters = !one_shot.is_empty();
        let mut accepted = false;
        for record in one_shot {
            accepted |= self.fire_correlated(event, record);
        }
        // Removing a waiter is not delivery: it may have been cancelled in the
        // gap before it fired. Latch rather than lose the terminal.
        if had_waiters && !accepted {
            self.relatch_correlated(event);
        }
    }

    /// Hand back a completion a discarded waiter had already captured, so a later
    /// observer of the same handle can still consume it. Refused once a GENUINE
    /// death has latched — there the death terminal outranks it.
    pub(crate) fn relatch_correlated(&self, event: &EventEnvelope) {
        let Some(key) = event.request_id else {
            return;
        };
        let waiters = {
            let mut reg = match self.correlated.lock() {
                Ok(reg) => reg,
                Err(poisoned) => poisoned.into_inner(),
            };
            // Read the discriminator under the SAME lock that sets it: a genuine
            // death purged the buffer, and its terminal outranks this envelope.
            if reg.death_purged {
                return;
            }
            match reg.live.remove(&(event.event_type, key)) {
                // An observer is already waiting — hand it over rather than
                // latching behind it, which would park that waiter forever.
                Some(records) => records,
                None => {
                    reg.latch_missed((event.event_type, key), event.clone());
                    Vec::new()
                }
            }
        };
        let had_waiters = !waiters.is_empty();
        let mut accepted = false;
        for record in waiters {
            accepted |= self.fire_correlated(event, record);
        }
        if had_waiters && !accepted {
            self.relatch_correlated(event);
        }
    }

    /// Resolve ONE correlated waiter with `event`; panic-isolated. Reports
    /// whether that waiter accepted the envelope.
    fn fire_correlated(&self, event: &EventEnvelope, record: SubscriberRecord) -> bool {
        if event.event_version > record.max_version {
            return false;
        }
        match record.handler {
            SubscriberHandlerKind::OneShot(handler) => {
                // Skip HandlerPanicWarning emit if THIS delivery is one (recursion-break).
                let subscriber_id = HandlerId(record.handle.id);
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(event)));
                let accepted = matches!(&result, Ok(true));
                if let Err(panic) = result {
                    tracing::warn!(
                        target: "kraken_sdk::dispatch_reactor",
                        event_type = ?event.event_type,
                        handler_id = subscriber_id.raw(),
                        "correlated one-shot handler panicked; caught by deliver_correlated"
                    );
                    if !is_observability_event(event.event_type) {
                        emit_handler_panic_warning(
                            self,
                            CallbackSource::Event(event.event_type),
                            subscriber_id,
                            truncate_panic_message(&*panic),
                        );
                    }
                }
                accepted
            }
            SubscriberHandlerKind::LongLived(_) => {
                tracing::warn!(
                    target: "kraken_sdk::dispatch_reactor",
                    event_type = ?event.event_type,
                    "LongLived handler found in correlated map (impossible by API)"
                );
                false
            }
        }
    }
}

impl Drop for DispatchEventBus {
    fn drop(&mut self) {
        if let Ok(mut g) = self.dispatch_reactor_join.lock() {
            if let Some(join) = g.take() {
                join.abort();
            }
        }
    }
}

/// RAII guard at the top of each reactor loop: dropped while ARMED, it reports
/// the death via `on_loop_death`. Clean exits call `disarm()`.
pub(crate) struct LoopDeathGuard {
    bus: Weak<DispatchEventBus>,
    loop_name: ReactorName,
    armed: bool,
}

impl LoopDeathGuard {
    pub(crate) fn new(bus: Weak<DispatchEventBus>, loop_name: ReactorName) -> Self {
        Self {
            bus,
            loop_name,
            armed: true,
        }
    }

    /// Mark a clean exit — `drop` becomes a no-op.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for LoopDeathGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(bus) = self.bus.upgrade() {
            let cause = if std::thread::panicking() {
                LoopFailureCause::Panic
            } else {
                LoopFailureCause::UnhandledError
            };
            bus.on_loop_death(self.loop_name, cause);
        }
    }
}

/// Observability event types, excluded from the warning/latency emits to break
/// the feedback loop.
fn is_observability_event(et: EventType) -> bool {
    matches!(
        et,
        EventType::QueueFullWarning
            | EventType::LoopFailedEvent
            | EventType::SlowCallbackWarning
            | EventType::HandlerPanicWarning
            | EventType::QueueDepthSample
            | EventType::CallbackLatency
    )
}

/// Truncate a panic payload to ≤1 KB (the `HandlerPanicWarning` contract).
pub(crate) fn truncate_panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    let mut s = if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    };
    const MAX: usize = 1024;
    if s.len() > MAX {
        let mut end = MAX;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    s
}

/// Push a `CallbackLatency` envelope; account a drop on ring overflow.
fn emit_callback_latency(
    bus: &DispatchEventBus,
    source: CallbackSource,
    handler_id: HandlerId,
    latency_us: u64,
) {
    let env = EventEnvelope {
        event_type: EventType::CallbackLatency,
        event_version: 2,
        timestamp_monotonic: bus.clock.now(),
        request_id: None,
        payload: EventPayload::CallbackLatency {
            callback_event_type: source,
            handler_id: Some(handler_id),
            latency_us,
        },
    };
    if bus.io_to_dispatch.push(DispatchItem::Event(env)) {
        bus.dropped_count.fetch_add(1, Ordering::Relaxed);
    }
}

fn emit_slow_callback_warning(
    bus: &DispatchEventBus,
    source: CallbackSource,
    latency_us: u64,
    threshold_ms: u32,
) {
    let env = EventEnvelope {
        event_type: EventType::SlowCallbackWarning,
        event_version: 1,
        timestamp_monotonic: bus.clock.now(),
        request_id: None,
        payload: EventPayload::SlowCallbackWarning {
            slow_event_type: source,
            latency_us,
            threshold_ms,
        },
    };
    if bus.io_to_dispatch.push(DispatchItem::Event(env)) {
        bus.dropped_count.fetch_add(1, Ordering::Relaxed);
    }
}

fn emit_handler_panic_warning(
    bus: &DispatchEventBus,
    source: CallbackSource,
    handler_id: HandlerId,
    panic_message: String,
) {
    let env = EventEnvelope {
        event_type: EventType::HandlerPanicWarning,
        event_version: 1,
        timestamp_monotonic: bus.clock.now(),
        request_id: None,
        payload: EventPayload::HandlerPanicWarning {
            source,
            handler_id,
            panic_message,
        },
    };
    if bus.io_to_dispatch.push(DispatchItem::Event(env)) {
        bus.dropped_count.fetch_add(1, Ordering::Relaxed);
    }
}

async fn dispatch_reactor_loop(weak_bus: Weak<DispatchEventBus>) {
    // Take a strong Arc on the ring at startup; ring lives independent of bus.
    let ring = match weak_bus.upgrade() {
        Some(bus) => Arc::clone(&bus.io_to_dispatch),
        None => return,
    };

    // Sample every interval OR every 1000 dequeues. Cadence uses raw Instant;
    // the payload timestamp uses the injected clock.
    const DEQUEUE_SAMPLE_THRESHOLD: u32 = 1000;
    let sample_interval = weak_bus
        .upgrade()
        .map(|b| Duration::from_millis(u64::from(b.cfg.queue_depth_sample_interval_ms)))
        .unwrap_or_else(|| Duration::from_millis(100));
    let mut dequeues_since_sample: u32 = 0;
    let mut last_sample = std::time::Instant::now();

    // Fails in-flight correlated awaits on abnormal exit; disarmed on clean exit.
    let mut death_guard = LoopDeathGuard::new(weak_bus.clone(), ReactorName::Dispatch);

    loop {
        let item = match ring.recv().await {
            Some(e) => e,
            None => break,
        };
        // Stamp only for Data (the await is idle wait; only Data feeds the end-probe).
        #[cfg(test)]
        if matches!(item, DispatchItem::Data(..)) {
            crate::dispatch::io_reactor::latency_histogram::dispatch_probe_start_at_recv();
        }

        let bus = match weak_bus.upgrade() {
            Some(b) => b,
            None => break, // bus dropped; exit cleanly
        };

        dequeues_since_sample += 1;
        if dequeues_since_sample >= DEQUEUE_SAMPLE_THRESHOLD
            || last_sample.elapsed() >= sample_interval
        {
            let now = bus.clock.now();
            let sample = EventEnvelope {
                event_type: EventType::QueueDepthSample,
                event_version: 1,
                timestamp_monotonic: now,
                request_id: None,
                payload: EventPayload::QueueDepthSample {
                    queue: QueueName::IoToDispatch,
                    depth: ring.depth(),
                    capacity: ring.capacity(),
                    sampled_at_monotonic: now,
                },
            };
            // Self-observability: account a drop, never recurse a warning.
            if bus.io_to_dispatch.push(DispatchItem::Event(sample)) {
                bus.dropped_count.fetch_add(1, Ordering::Relaxed);
            }
            dequeues_since_sample = 0;
            last_sample = std::time::Instant::now();
        }

        let event = match item {
            DispatchItem::Data(delivery, routing) => {
                dispatch_data_delivery(&bus, delivery, routing);
                continue;
            }
            DispatchItem::Event(env) => env,
        };

        if event.request_id.is_some() {
            bus.deliver_correlated(&event);
        }

        // Snapshot under a short read-lock, dropped BEFORE invoking so handlers
        // can subscribe without deadlock. Skip the alloc when no subscribers.
        let has_subs = {
            let subs = bus
                .subscribers
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            subs.get(&event.event_type)
                .map(|recs| !recs.is_empty())
                .unwrap_or(false)
        };
        let handlers: Vec<(SubscriberHandle, u16, EventCallback)> = if has_subs {
            let subs = bus
                .subscribers
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            subs.get(&event.event_type)
                .map(|recs| {
                    recs.iter()
                        .filter_map(|r| match &r.handler {
                            SubscriberHandlerKind::LongLived(cb) => {
                                Some((r.handle, r.max_version, Arc::clone(cb)))
                            }
                            SubscriberHandlerKind::OneShot(_) => None,
                        })
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        if !handlers.is_empty() {
            for (handle, max_version, cb) in handlers {
                let to_deliver = if event.event_version > max_version {
                    let mut downgraded = event.clone();
                    downgraded.payload = downgraded.payload.downgrade_to(max_version);
                    downgraded.event_version = max_version;
                    std::borrow::Cow::Owned(downgraded)
                } else {
                    std::borrow::Cow::Borrowed(&event)
                };
                let started = std::time::Instant::now();
                let subscriber_id = HandlerId(handle.id);
                let to_deliver_ref: &EventEnvelope = &to_deliver;
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cb(to_deliver_ref)));
                match result {
                    Ok(()) => {
                        let elapsed_us = started.elapsed().as_micros() as u64;
                        if !is_observability_event(event.event_type) {
                            emit_callback_latency(
                                &bus,
                                CallbackSource::Event(event.event_type),
                                subscriber_id,
                                elapsed_us,
                            );
                        }
                        // Threshold read FRESH from Knobs (runtime-mutable).
                        let threshold_ms = bus
                            .knobs
                            .slow_callback_threshold_ms
                            .load(std::sync::atomic::Ordering::Relaxed);
                        if elapsed_us >= u64::from(threshold_ms) * 1_000
                            && !is_observability_event(event.event_type)
                        {
                            emit_slow_callback_warning(
                                &bus,
                                CallbackSource::Event(event.event_type),
                                elapsed_us,
                                threshold_ms,
                            );
                            tracing::warn!(
                                target: "kraken_sdk::dispatch_reactor",
                                event_type = ?event.event_type,
                                elapsed_us,
                                threshold_ms,
                                "long-lived subscriber callback exceeded slow_callback_threshold_ms"
                            );
                        }
                    }
                    Err(panic) => {
                        tracing::warn!(
                            target: "kraken_sdk::dispatch_reactor",
                            event_type = ?event.event_type,
                            handler_id = subscriber_id.raw(),
                            "long-lived subscriber panicked; caught by dispatch reactor"
                        );
                        if !is_observability_event(event.event_type) {
                            emit_handler_panic_warning(
                                &bus,
                                CallbackSource::Event(event.event_type),
                                subscriber_id,
                                truncate_panic_message(&*panic),
                            );
                        }
                    }
                }
            }
        }
        tracing::trace!(
            target: "kraken_sdk::dispatch_reactor",
            event_type = ?event.event_type,
            "dispatch_reactor: event drained"
        );
    }

    death_guard.disarm();
}

/// Run one decoded user-data callback under the same instrumentation as a
/// long-lived subscriber.
fn dispatch_data_delivery(
    bus: &Arc<DispatchEventBus>,
    delivery: DataDelivery,
    routing: DataRouting,
) {
    let DataDelivery { invoke, payload } = delivery;
    let started = std::time::Instant::now();
    // End the span just before invoke (counts only the loop's own work).
    #[cfg(test)]
    crate::dispatch::io_reactor::latency_histogram::dispatch_probe_end_before_invoke();
    // A panicking user data callback must not unwind the dispatch task.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| invoke(&payload)));
    match result {
        Ok(()) => {
            let elapsed_us = started.elapsed().as_micros() as u64;
            // Gate on ≥1 CallbackLatency subscriber — else every decoded frame
            // pushes a second ring item, doubling traffic when unobserved.
            if bus.subscriber_count(EventType::CallbackLatency) > 0 {
                emit_callback_latency(
                    bus,
                    CallbackSource::DataChannel(routing.channel),
                    routing.handler_id,
                    elapsed_us,
                );
            }
            // Threshold read FRESH so `set_knob` is honoured.
            let threshold_ms = bus
                .knobs
                .slow_callback_threshold_ms
                .load(std::sync::atomic::Ordering::Relaxed);
            if elapsed_us >= u64::from(threshold_ms) * 1_000 {
                emit_slow_callback_warning(
                    bus,
                    CallbackSource::DataChannel(routing.channel),
                    elapsed_us,
                    threshold_ms,
                );
                tracing::warn!(
                    target: "kraken_sdk::dispatch_reactor",
                    channel = ?routing.channel,
                    elapsed_us,
                    threshold_ms,
                    "user data callback exceeded slow_callback_threshold_ms"
                );
            }
        }
        Err(panic) => {
            tracing::warn!(
                target: "kraken_sdk::dispatch_reactor",
                channel = ?routing.channel,
                handler_id = routing.handler_id.raw(),
                "user data callback panicked; caught by dispatch reactor"
            );
            emit_handler_panic_warning(
                bus,
                CallbackSource::DataChannel(routing.channel),
                routing.handler_id,
                truncate_panic_message(&*panic),
            );
        }
    }
}
