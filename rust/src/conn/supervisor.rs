//! `ConnectionSupervisor` — thin caller-side facade over managed connections:
//! snapshot state reads and post-and-correlate `connect`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use crate::conn::rate_budget::ConnectionRateBudget;
use crate::dispatch::{CallerInbound, DispatchEventBus};
use crate::types::{CallerEvent, ConnectionHandle, ConnectionState, QueueFullError, WsUrl};

/// Bare-order readiness hints the reactor maintains per token-requiring url for
/// the caller-side `ensure_order_sendable`; reactor single-writer.
#[derive(Default)]
pub(crate) struct OrderGate {
    auth: UrlGate,
    unified_private: UrlGate,
}

#[derive(Default)]
struct UrlGate {
    has_subscriptions: AtomicBool,
    send_ready: AtomicBool,
}

impl OrderGate {
    fn url_gate(&self, url: WsUrl) -> Option<&UrlGate> {
        match url {
            WsUrl::Auth => Some(&self.auth),
            WsUrl::UnifiedPrivate => Some(&self.unified_private),
            WsUrl::Public | WsUrl::UnifiedPublic => None,
        }
    }

    /// Does `url` have at least one subscription entry?
    pub(crate) fn has_subscriptions(&self, url: WsUrl) -> bool {
        self.url_gate(url)
            .is_some_and(|gate| gate.has_subscriptions.load(Ordering::Acquire))
    }

    pub(crate) fn set_has_subscriptions(&self, url: WsUrl, value: bool) {
        if let Some(gate) = self.url_gate(url) {
            gate.has_subscriptions.store(value, Ordering::Release);
        }
    }

    /// Is `url` bare-order send-ready (`Authenticating` + empty registry for
    /// the url + valid cached token)?
    pub(crate) fn send_ready(&self, url: WsUrl) -> bool {
        self.url_gate(url)
            .is_some_and(|gate| gate.send_ready.load(Ordering::Acquire))
    }

    pub(crate) fn set_send_ready(&self, url: WsUrl, value: bool) {
        if let Some(gate) = self.url_gate(url) {
            gate.send_ready.store(value, Ordering::Release);
        }
    }
}

/// Thin coordinator. Holds only the read-side state mirrors + caller-facing
/// methods. Lifetime: Client-scoped.
pub struct ConnectionSupervisor {
    /// Per-WsUrl `Arc<AtomicU8>` state mirrors. Reactor writes (Release),
    /// supervisor reads (Acquire).
    state_mirrors: HashMap<WsUrl, Arc<AtomicU8>>,

    /// Sliding-window rate limiter: one instance for all four connections. Shared with the reactor's
    /// `ManagedConnection`s — every connection consults the SAME budget.
    rate_budget: Arc<ConnectionRateBudget>,

    bus: Arc<DispatchEventBus>,

    /// Monotonic handle-id allocator. Shared with the I/O Reactor so
    /// auto-connect-on-first-subscribe draws from the SAME id space as `connect()`.
    next_handle_id: Arc<AtomicU64>,

    /// Reactor-maintained per-url bare-order hints.
    order_gate: Arc<OrderGate>,
}

impl ConnectionSupervisor {
    /// Freshly-initialised state mirrors (all `Idle`) with the default rate budget.
    #[cfg(test)]
    pub fn new(bus: Arc<DispatchEventBus>) -> Self {
        Self::with_rate_budget(bus, ConnectionRateBudget::new())
    }

    /// Construct with the `connection_rate_budget` + `connection_rate_window_secs`
    /// knobs (construction-only).
    pub(crate) fn new_with_knobs(
        bus: Arc<DispatchEventBus>,
        knobs: &crate::build::knobs::Knobs,
    ) -> Self {
        Self::with_rate_budget(
            bus,
            ConnectionRateBudget::with_knobs(
                knobs.connection_rate_budget,
                knobs.connection_rate_window(),
            ),
        )
    }

    fn with_rate_budget(bus: Arc<DispatchEventBus>, rate_budget: ConnectionRateBudget) -> Self {
        let mut state_mirrors = HashMap::with_capacity(WsUrl::ALL.len());
        for url in WsUrl::ALL {
            state_mirrors.insert(url, Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())));
        }
        Self {
            state_mirrors,
            rate_budget: Arc::new(rate_budget),
            bus,
            next_handle_id: Arc::new(AtomicU64::new(1)),
            order_gate: Arc::new(OrderGate::default()),
        }
    }

    /// Clone the reactor-maintained per-url bare-order hints.
    pub(crate) fn order_gate(&self) -> Arc<OrderGate> {
        Arc::clone(&self.order_gate)
    }

    /// Hand out clones of the state mirrors for the I/O Reactor task.
    pub(crate) fn state_mirror_clones(&self) -> HashMap<WsUrl, Arc<AtomicU8>> {
        self.state_mirrors
            .iter()
            .map(|(k, v)| (*k, Arc::clone(v)))
            .collect()
    }

    /// Clone the handle-id allocator for sharing with the I/O Reactor task.
    pub(crate) fn handle_id_allocator(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.next_handle_id)
    }

    /// Clone the shared `ConnectionRateBudget` (one instance across all managed
    /// connections) for the I/O Reactor's `ManagedConnection`s.
    pub(crate) fn rate_budget_clone(&self) -> Arc<ConnectionRateBudget> {
        Arc::clone(&self.rate_budget)
    }

    /// Snapshot read of the FSM state for the given WS URL. May be stale by at
    /// most one FSM transition (single-writer atomic snapshot semantics).
    pub fn current_state(&self, url: WsUrl) -> ConnectionState {
        let mirror = self
            .state_mirrors
            .get(&url)
            .expect("state_mirrors invariant: every managed WsUrl is present at construction");
        let byte = mirror.load(Ordering::Acquire);
        ConnectionState::from_u8(byte)
            .expect("state_mirrors invariant: only valid ConnectionState bytes are written")
    }

    /// Initiate connection. `Ok(ConnectionHandle)` on successful post;
    /// `Err(QueueFullError)` when `caller_to_io` is full.
    pub fn connect(&self, url: WsUrl) -> Result<ConnectionHandle, QueueFullError> {
        let connection_id = self.next_handle_id.fetch_add(1, Ordering::Relaxed);
        let handle = ConnectionHandle { url, connection_id };
        self.bus
            .try_post_caller_inbound(CallerInbound::FsmEvent {
                url,
                event: CallerEvent::StartConnect {
                    request_id: connection_id,
                },
            })
            .map_err(|_| QueueFullError {
                queue: "caller_to_io",
            })?;
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::SystemClock;
    use crate::dispatch::DispatchEventBusConfig;

    fn fixture() -> ConnectionSupervisor {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(SystemClock);
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            clock,
        ));
        ConnectionSupervisor::new(bus)
    }

    #[test]
    fn current_state_returns_idle_for_all_urls_at_construction() {
        let sup = fixture();
        for url in WsUrl::ALL {
            assert_eq!(sup.current_state(url), ConnectionState::Idle);
        }
    }

    #[test]
    fn connect_returns_handle_with_unique_connection_id() {
        let sup = fixture();
        let a = sup.connect(WsUrl::Public).unwrap();
        let b = sup.connect(WsUrl::Public).unwrap();
        assert_ne!(a.connection_id, b.connection_id);
        assert_eq!(a.url, WsUrl::Public);
    }

    #[test]
    fn state_mirror_clones_returns_every_managed_url() {
        let sup = fixture();
        let mirrors = sup.state_mirror_clones();
        assert_eq!(mirrors.len(), WsUrl::ALL.len());
        for url in WsUrl::ALL {
            assert!(mirrors.contains_key(&url), "missing mirror for {url:?}");
        }
        mirrors[&WsUrl::Public].store(ConnectionState::Open.as_u8(), Ordering::Release);
        assert_eq!(sup.current_state(WsUrl::Public), ConnectionState::Open);
    }
}
