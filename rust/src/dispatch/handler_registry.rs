//! Caller-facing channel-wide handler store, keyed by `ChannelName`.
//! Single-writer on the I/O reactor; `has_handlers` reads a shared presence mirror.

use std::any::Any;
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};

use serde_json::Value;

use crate::dispatch::DispatchEventBus;
use crate::types::ChannelName;

/// Opaque, never-recycled handler id. Not user-constructable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HandlerId(pub(crate) u64);

impl HandlerId {
    /// Borrow the raw id (for event-payload correlation only).
    pub fn raw(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for HandlerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(crate) type SharedData = Arc<dyn Any + Send + Sync>;

/// Type-erased dispatch-loop-side user-callback invoker: downcasts the shared
/// decoded payload to its concrete type and calls the user closure.
pub(crate) type DataInvoker = Arc<dyn Fn(&SharedData) + Send + Sync + 'static>;

/// A data delivery: the shared decoded payload plus the `invoke` that downcasts
/// it back to its concrete type and runs the user closure.
pub(crate) struct DataDelivery {
    pub invoke: DataInvoker,
    pub payload: SharedData,
}

impl std::fmt::Debug for DataDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataDelivery").finish_non_exhaustive()
    }
}

/// Outcome of one reactor-side handler-callback decode. The typed decode runs
/// on the I/O reactor; `Deliver` carries the shared payload, `DecodeFailed`
/// lets the reactor emit the routing-symbol gap event.
pub(crate) enum HandlerDecodeOutcome {
    /// The closure decoded `Value` into its typed `T`; the user callback has NOT
    /// run yet — the dispatch loop fans this payload out to every handler.
    Deliver(SharedData),
    /// `serde_json::from_value::<T>` (or symbol parse) failed; nothing is
    /// enqueued. The reactor emits the gap event for the routing symbol.
    DecodeFailed,
}

impl std::fmt::Debug for HandlerDecodeOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerDecodeOutcome::Deliver(_) => f.debug_tuple("Deliver").finish(),
            HandlerDecodeOutcome::DecodeFailed => f.write_str("DecodeFailed"),
        }
    }
}

/// Type-erased per-frame wire decoder → [`HandlerDecodeOutcome`].
pub(crate) type HandlerDecoder = Arc<dyn Fn(Value) -> HandlerDecodeOutcome + Send + Sync + 'static>;

/// Registered handler: `decode` (reactor) + `invoke` (dispatch). The maintained-book
/// fast-path calls `invoke` directly.
#[derive(Clone)]
pub(crate) struct HandlerCallback {
    pub decode: HandlerDecoder,
    pub invoke: DataInvoker,
}

impl HandlerCallback {
    pub(crate) fn new(decode: HandlerDecoder, invoke: DataInvoker) -> Self {
        Self { decode, invoke }
    }

    /// No-op handler for tests. Independent decode/invoke Arcs — not for typed
    /// fast-path delivery tests; use a `wrap_typed` handler there.
    #[cfg(test)]
    pub(crate) fn noop() -> Self {
        Self::new(
            Arc::new(|_v: Value| HandlerDecodeOutcome::Deliver(Arc::new(()))),
            Arc::new(|_p: &SharedData| {}),
        )
    }
}

/// Shared, caller-readable presence projection backing `has_handlers`.
/// Stored as a count (not a bool) so concurrent register/deregister of
/// sibling handlers on one channel never races the presence signal.
pub type PresenceMirror = Arc<RwLock<HashMap<ChannelName, usize>>>;

/// One registered callback, stored in registration order (deterministic fan-out).
#[derive(Clone)]
pub struct HandlerEntry {
    pub id: HandlerId,
    pub callback: HandlerCallback,
}

impl std::fmt::Debug for HandlerEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerEntry")
            .field("id", &self.id)
            .field("callback", &"<HandlerCallback>")
            .finish()
    }
}

/// Single-writer (I/O reactor task) registry of channel-wide handlers.
pub struct HandlerRegistry {
    /// Channel → ordered handlers. Reactor-owned, single-writer.
    handlers: HashMap<ChannelName, Vec<HandlerEntry>>,

    /// Id allocator for the test `register` path; live path uses `register_with_id`.
    #[cfg(test)]
    next_id: AtomicU64,

    /// Caller-readable handler-count projection for `has_handlers`.
    presence_mirror: PresenceMirror,
}

impl HandlerRegistry {
    /// Construct sharing `presence_mirror` with the caller-side surface.
    pub fn new(presence_mirror: PresenceMirror) -> Self {
        Self {
            handlers: HashMap::new(),
            #[cfg(test)]
            next_id: AtomicU64::new(1),
            presence_mirror,
        }
    }

    /// Self-allocating register that bumps the presence mirror. Test/legacy only —
    /// live path uses `register_with_id` (mirror-silent) to avoid double-count.
    #[cfg(test)]
    pub fn register(&mut self, channel: ChannelName, callback: HandlerCallback) -> HandlerId {
        let id = HandlerId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.handlers
            .entry(channel)
            .or_default()
            .push(HandlerEntry { id, callback });
        if let Ok(mut mirror) = self.presence_mirror.write() {
            *mirror.entry(channel).or_insert(0) += 1;
        }
        id
    }

    /// Register with a caller-allocated id. Reactor task only. Does NOT touch the
    /// presence mirror — the caller already bumped it at id-allocation.
    pub fn register_with_id(
        &mut self,
        channel: ChannelName,
        id: HandlerId,
        callback: HandlerCallback,
    ) {
        self.handlers
            .entry(channel)
            .or_default()
            .push(HandlerEntry { id, callback });
    }

    /// Remove the handler with `id` from `channel`. Idempotent. Decrements the
    /// presence mirror on confirmed removal. Reactor task only.
    pub fn deregister(&mut self, channel: ChannelName, id: HandlerId) {
        let removed = match self.handlers.get_mut(&channel) {
            Some(vec) => {
                let before = vec.len();
                vec.retain(|e| e.id != id);
                before != vec.len()
            }
            None => false,
        };
        if !removed {
            return;
        }
        if let Ok(mut mirror) = self.presence_mirror.write() {
            if let Some(count) = mirror.get_mut(&channel) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    mirror.remove(&channel);
                }
            }
        }
    }

    /// Borrow registered handlers for `channel`. Reactor task, lock-free.
    /// Empty slice when none.
    pub fn handlers_for(&self, channel: ChannelName) -> &[HandlerEntry] {
        self.handlers
            .get(&channel)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

/// Caller-thread presence gate. Does NOT touch the reactor-owned `handlers` map.
pub fn presence_has_handlers(mirror: &PresenceMirror, channel: ChannelName) -> bool {
    match mirror.read() {
        Ok(guard) => guard.get(&channel).is_some_and(|&c| c > 0),
        Err(_) => false,
    }
}

/// Registration token from bare `on_*` registrars. `Drop` deregisters the
/// callback only — not the wire subscription (docs/guides/streaming.md).
#[must_use = "dropping the handle deregisters the callback"]
pub struct HandlerHandle {
    id: HandlerId,
    channel: ChannelName,
    weak_bus: Weak<DispatchEventBus>,
}

impl HandlerHandle {
    pub(crate) fn new(
        id: HandlerId,
        channel: ChannelName,
        weak_bus: Weak<DispatchEventBus>,
    ) -> Self {
        Self {
            id,
            channel,
            weak_bus,
        }
    }

    /// Opaque handler id.
    pub fn id(&self) -> HandlerId {
        self.id
    }

    /// Channel this handler is registered against.
    pub fn channel(&self) -> ChannelName {
        self.channel
    }
}

impl std::fmt::Debug for HandlerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandlerHandle")
            .field("id", &self.id)
            .field("channel", &self.channel)
            .finish()
    }
}

impl Drop for HandlerHandle {
    fn drop(&mut self) {
        let Some(bus) = self.weak_bus.upgrade() else {
            return;
        };
        let handler_id = self.id;
        let channel = self.channel;
        tracing::debug!(
            target: "kraken_sdk::dispatch",
            %handler_id,
            %channel,
            "handler {handler_id} for {channel} deregistered on HandlerHandle drop; the wire subscription persists — \
             use on_*_for / SubscriptionGuard for atomic teardown",
        );
        bus.post_teardown(crate::dispatch::CallerInbound::HandlerMutation {
            channel: self.channel,
            op: crate::dispatch::HandlerMutationOp::Deregister { id: self.id },
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn empty_mirror() -> PresenceMirror {
        Arc::new(RwLock::new(HashMap::new()))
    }

    fn counting_cb(sink: Arc<Mutex<Vec<Value>>>) -> HandlerCallback {
        let invoke: DataInvoker = Arc::new(|_p: &SharedData| {});
        HandlerCallback::new(
            Arc::new(move |v: Value| {
                sink.lock().unwrap().push(v);
                HandlerDecodeOutcome::Deliver(Arc::new(()))
            }),
            invoke,
        )
    }

    #[test]
    fn register_allocates_monotonic_ids_and_bumps_mirror() {
        let mirror = empty_mirror();
        let mut reg = HandlerRegistry::new(Arc::clone(&mirror));
        let sink = Arc::new(Mutex::new(Vec::new()));
        let a = reg.register(ChannelName::Ticker, counting_cb(Arc::clone(&sink)));
        let b = reg.register(ChannelName::Ticker, counting_cb(Arc::clone(&sink)));
        assert!(b.0 > a.0);
        assert!(presence_has_handlers(&mirror, ChannelName::Ticker));
        assert_eq!(
            *mirror.read().unwrap().get(&ChannelName::Ticker).unwrap(),
            2
        );
    }

    #[test]
    fn handlers_for_returns_registration_order() {
        let mut reg = HandlerRegistry::new(empty_mirror());
        let sink = Arc::new(Mutex::new(Vec::new()));
        let a = reg.register(ChannelName::Book, counting_cb(Arc::clone(&sink)));
        let b = reg.register(ChannelName::Book, counting_cb(Arc::clone(&sink)));
        let ids: Vec<_> = reg
            .handlers_for(ChannelName::Book)
            .iter()
            .map(|e| e.id)
            .collect();
        assert_eq!(ids, vec![a, b]);
    }

    #[test]
    fn deregister_is_idempotent_and_decrements_mirror() {
        let mirror = empty_mirror();
        let mut reg = HandlerRegistry::new(Arc::clone(&mirror));
        let sink = Arc::new(Mutex::new(Vec::new()));
        let a = reg.register(ChannelName::Trade, counting_cb(Arc::clone(&sink)));
        let b = reg.register(ChannelName::Trade, counting_cb(Arc::clone(&sink)));
        reg.deregister(ChannelName::Trade, a);
        assert_eq!(*mirror.read().unwrap().get(&ChannelName::Trade).unwrap(), 1);
        assert_eq!(reg.handlers_for(ChannelName::Trade).len(), 1);
        reg.deregister(ChannelName::Trade, a);
        assert_eq!(*mirror.read().unwrap().get(&ChannelName::Trade).unwrap(), 1);
        reg.deregister(ChannelName::Trade, b);
        assert!(!presence_has_handlers(&mirror, ChannelName::Trade));
    }

    #[test]
    fn presence_gate_false_for_unregistered_channel() {
        let mirror = empty_mirror();
        let _reg = HandlerRegistry::new(Arc::clone(&mirror));
        assert!(!presence_has_handlers(&mirror, ChannelName::Ohlc));
    }

    #[test]
    fn handler_handle_drop_is_safe_with_dead_bus() {
        let handle = HandlerHandle::new(HandlerId(1), ChannelName::Ticker, Weak::new());
        drop(handle);
    }

    fn noop_cb() -> HandlerCallback {
        HandlerCallback::noop()
    }

    #[test]
    fn register_with_id_does_not_double_count_mirror() {
        let mirror = empty_mirror();
        let mut reg = HandlerRegistry::new(Arc::clone(&mirror));

        {
            let mut m = mirror.write().unwrap();
            *m.entry(ChannelName::Ticker).or_insert(0) += 1;
        }
        assert_eq!(
            *mirror.read().unwrap().get(&ChannelName::Ticker).unwrap(),
            1,
            "after caller bump"
        );

        let id = HandlerId(42);
        reg.register_with_id(ChannelName::Ticker, id, noop_cb());
        assert_eq!(
            *mirror.read().unwrap().get(&ChannelName::Ticker).unwrap(),
            1,
            "A371: register_with_id must not double-count the presence mirror"
        );
        assert_eq!(
            reg.handlers_for(ChannelName::Ticker).len(),
            1,
            "callback stored"
        );
    }

    #[test]
    fn register_with_id_then_deregister_reaches_zero() {
        let mirror = empty_mirror();
        let mut reg = HandlerRegistry::new(Arc::clone(&mirror));

        {
            let mut m = mirror.write().unwrap();
            *m.entry(ChannelName::Trade).or_insert(0) += 1;
        }
        let id = HandlerId(7);
        reg.register_with_id(ChannelName::Trade, id, noop_cb());
        assert_eq!(*mirror.read().unwrap().get(&ChannelName::Trade).unwrap(), 1);

        reg.deregister(ChannelName::Trade, id);
        assert!(
            !presence_has_handlers(&mirror, ChannelName::Trade),
            "mirror must reach 0 after deregister — no phantom presence"
        );
    }

    #[test]
    fn two_register_with_id_then_deregister_both_reaches_zero() {
        let mirror = empty_mirror();
        let mut reg = HandlerRegistry::new(Arc::clone(&mirror));

        {
            let mut m = mirror.write().unwrap();
            *m.entry(ChannelName::Book).or_insert(0) += 1;
            *m.entry(ChannelName::Book).or_insert(0) += 1;
        }
        let id_a = HandlerId(1);
        let id_b = HandlerId(2);
        reg.register_with_id(ChannelName::Book, id_a, noop_cb());
        reg.register_with_id(ChannelName::Book, id_b, noop_cb());
        assert_eq!(*mirror.read().unwrap().get(&ChannelName::Book).unwrap(), 2);

        reg.deregister(ChannelName::Book, id_a);
        assert_eq!(
            *mirror.read().unwrap().get(&ChannelName::Book).unwrap(),
            1,
            "one remaining"
        );
        assert!(
            presence_has_handlers(&mirror, ChannelName::Book),
            "still present"
        );

        reg.deregister(ChannelName::Book, id_b);
        assert!(
            !presence_has_handlers(&mirror, ChannelName::Book),
            "both dropped → zero"
        );
    }

    #[test]
    fn no_register_yields_no_presence() {
        let mirror = empty_mirror();
        let reg = HandlerRegistry::new(Arc::clone(&mirror));
        assert!(!presence_has_handlers(&mirror, ChannelName::Ticker));
        assert!(reg.handlers_for(ChannelName::Ticker).is_empty());
    }
}
