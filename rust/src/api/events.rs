//! [`EventsNamespace`] — the unified `client.events()` lifecycle and operational
//! event-observability namespace, plus its RAII [`EventSubscription`] guard.
//! Thin wrapper over [`DispatchEventBus::subscribe`]; dropping the guard unsubscribes.

use std::sync::{Arc, Weak};

use crate::dispatch::{DispatchEventBus, EventCallback, EventEnvelope, EventType};
use crate::error::EventsError;
use crate::types::SubscriberHandle;

/// "Latest producer version" sentinel for [`EventsNamespace::on`]: the subscriber
/// never requests a downgrade, so it always sees the producer's current
/// `event_version`. The downgrade gate only fires when `event_version > max_version`.
const LATEST_EVENT_VERSION: u16 = u16::MAX;

/// Unified lifecycle and operational event-observability namespace, accessed via
/// `client.events()`. Thin wrapper over [`DispatchEventBus::subscribe`] — the single
/// entry point for connection, rate-limit, error, subscription, and dispatcher events.
pub struct EventsNamespace {
    bus: Arc<DispatchEventBus>,
}

impl std::fmt::Debug for EventsNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventsNamespace").finish_non_exhaustive()
    }
}

impl EventsNamespace {
    /// Construct with the shared dispatch bus. Crate-internal.
    pub(crate) fn new(bus: Arc<DispatchEventBus>) -> Self {
        Self { bus }
    }

    /// Subscribe to a system event; the handler runs on the dispatch reactor for every
    /// matching event until the returned RAII [`EventSubscription`] guard is dropped.
    /// Returns [`EventsError::LoopDead`] if a reactor loop has died.
    ///
    /// # Errors
    ///
    /// - [`EventsError::LoopDead`] — a reactor loop has died, so dispatch can no longer
    ///   fire callbacks and new subscriptions are rejected; rebuild the client.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # fn ex() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = kraken_sdk::ClientBuilder::new().build()?;
    /// // Bind the guard: it must stay alive — dropping it unsubscribes the handler.
    /// let _sub = client
    ///     .events()
    ///     .on(kraken_sdk::EventType::ConnectionOpenEvent, |env| {
    ///         println!("{:?}", env.event_type);
    ///     })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn on<F>(&self, event_type: EventType, cb: F) -> Result<EventSubscription, EventsError>
    where
        F: Fn(&EventEnvelope) + Send + Sync + 'static,
    {
        // A dead reactor leaves dispatch unable to fire callbacks — reject rather
        // than register a silently inert subscriber.
        if self.bus.is_loop_failed() {
            return Err(EventsError::LoopDead);
        }
        let callback: EventCallback = Arc::new(cb);
        // Registers the handler then lazy-starts the dispatch reactor, so a
        // REST-only caller (which never calls `ready()`) still receives events.
        let handle = self
            .bus
            .subscribe(event_type, callback, LATEST_EVENT_VERSION);
        Ok(EventSubscription::new(Arc::downgrade(&self.bus), handle))
    }
}

/// RAII subscription guard returned by [`EventsNamespace::on`]; dropping it
/// unsubscribes the handler. `Drop` is synchronous — no block, no await, no panic.
#[must_use = "dropping the EventSubscription immediately unsubscribes the handler; \
              bind it to a variable to keep the subscription live"]
pub struct EventSubscription {
    /// Weak so a dropped bus is a clean no-op.
    weak_bus: Weak<DispatchEventBus>,
    handle: SubscriberHandle,
}

impl EventSubscription {
    /// Construct from the downgraded bus ref and the bus-minted handle.
    /// Crate-internal — minted by [`EventsNamespace::on`].
    pub(crate) fn new(weak_bus: Weak<DispatchEventBus>, handle: SubscriberHandle) -> Self {
        Self { weak_bus, handle }
    }
}

impl std::fmt::Debug for EventSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventSubscription")
            .field("handle", &self.handle)
            .finish()
    }
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        let Some(bus) = self.weak_bus.upgrade() else {
            return;
        };
        bus.unsubscribe(self.handle);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::clock::SystemClock;
    use crate::dispatch::{
        DispatchEventBus, DispatchEventBusConfig, EventEnvelope, EventPayload, EventType,
    };
    use crate::types::{MonotonicInstant, SubscriberHandle};

    fn new_bus_with_reactor() -> Arc<DispatchEventBus> {
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::default(),
            Arc::new(SystemClock),
        ));
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        bus
    }

    fn client_ready_envelope() -> EventEnvelope {
        EventEnvelope {
            event_type: EventType::ClientReady,
            event_version: 1,
            timestamp_monotonic: MonotonicInstant::now(),
            request_id: None,
            payload: EventPayload::ClientReady {
                capability_snapshot: crate::types::CapabilitySnapshot {
                    declared_namespaces: std::collections::HashSet::new(),
                    declared_ws_urls: std::collections::HashSet::new(),
                    discovered_at_first_use: std::collections::HashSet::new(),
                },
            },
        }
    }

    #[tokio::test]
    async fn on_after_loop_death_rejects_loopdead() {
        use crate::error::ApiError;
        let bus = new_bus_with_reactor();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Dispatch,
            crate::dispatch::LoopFailureCause::Panic,
        );
        let ns = EventsNamespace::new(Arc::clone(&bus));
        let err = ns
            .on(EventType::ClientReady, |_| {})
            .expect_err("on() after loop death must reject");
        assert!(matches!(err, EventsError::LoopDead));
        assert_eq!(err.code(), "LOOP_DEAD");
        assert_eq!(err.category(), crate::error::ErrorCategory::Client);
        assert!(!err.retryable());
    }

    #[tokio::test]
    async fn pre_ready_event_delivers_via_on_without_ready() {
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::default(),
            Arc::new(SystemClock),
        ));
        bus.publish(client_ready_envelope());

        let ns = EventsNamespace::new(Arc::clone(&bus));
        let (tx, mut rx) = tokio::sync::mpsc::channel::<EventEnvelope>(8);
        let _guard = ns
            .on(EventType::ClientReady, move |env| {
                let _ = tx.try_send(env.clone());
            })
            .expect("on() must register");

        let env = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("pre-ready event not delivered — reactor not lazy-started or drained before subscribe")
            .expect("channel closed");
        assert_eq!(env.event_type, EventType::ClientReady);
    }

    #[test]
    fn dropping_guard_with_dead_bus_does_not_panic() {
        let guard = EventSubscription::new(Weak::new(), SubscriberHandle { id: 1 });
        drop(guard);
    }

    #[tokio::test]
    async fn on_registers_then_drop_unsubscribes() {
        let bus = new_bus_with_reactor();
        let ns = EventsNamespace::new(Arc::clone(&bus));

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(4);

        let guard = ns
            .on(EventType::ClientReady, move |_env| {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                let _ = tx.try_send(());
            })
            .expect("on() must return Ok");

        assert_eq!(
            bus.subscriber_count(EventType::ClientReady),
            1,
            "on() must register exactly one subscriber"
        );

        bus.publish(client_ready_envelope());
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("handler must fire within 2s")
            .expect("channel was closed before the handler fired");
        assert_eq!(counter.load(Ordering::SeqCst), 1, "handler must fire once");

        drop(guard);
        assert_eq!(
            bus.subscriber_count(EventType::ClientReady),
            0,
            "dropping the EventSubscription must unsubscribe synchronously"
        );
    }

    #[test]
    fn events_namespace_on_returns_ok() {
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::default(),
            Arc::new(SystemClock),
        ));
        let ns = EventsNamespace::new(Arc::clone(&bus));
        let result = ns.on(EventType::QueueFullWarning, |_env| {});
        assert!(result.is_ok(), "on() must return Ok for a valid EventType");
    }
}
