//! `ConnectionSupervisor` — thin caller-side facade over the Public + Auth
//! managed connections: snapshot state reads and post-and-correlate `connect`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use crate::conn::rate_budget::ConnectionRateBudget;
use crate::dispatch::{CallerInbound, DispatchEventBus};
use crate::types::{CallerEvent, ConnectionHandle, ConnectionState, QueueFullError, WsUrl};

/// Thin coordinator. Holds only the read-side state mirrors + caller-facing
/// methods. Lifetime: Client-scoped.
pub struct ConnectionSupervisor {
    /// Per-WsUrl `Arc<AtomicU8>` state mirrors. Reactor writes (Release),
    /// supervisor reads (Acquire).
    state_mirrors: HashMap<WsUrl, Arc<AtomicU8>>,

    /// Host-scoped sliding-window rate limiter. Shared with the reactor's
    /// `ManagedConnection`s — every connection consults the SAME budget.
    rate_budget: Arc<ConnectionRateBudget>,

    bus: Arc<DispatchEventBus>,

    /// Monotonic handle-id allocator. Shared with the I/O Reactor so
    /// auto-connect-on-first-subscribe draws from the SAME id space as `connect()`.
    next_handle_id: Arc<AtomicU64>,

    /// Reactor-maintained flag: the AUTH connection has ≥1 active subscription.
    auth_has_subscriptions: Arc<AtomicBool>,

    /// Reactor-maintained flag: the auth connection is bare-order send-ready
    /// (`Authenticating` + empty auth registry + valid cached token).
    auth_send_ready: Arc<AtomicBool>,
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
        // v1 = 2-WS topology (Public + Auth).
        let mut state_mirrors = HashMap::with_capacity(2);
        for url in [WsUrl::Public, WsUrl::Auth] {
            state_mirrors.insert(url, Arc::new(AtomicU8::new(ConnectionState::Idle.as_u8())));
        }
        Self {
            state_mirrors,
            rate_budget: Arc::new(rate_budget),
            bus,
            next_handle_id: Arc::new(AtomicU64::new(1)),
            auth_has_subscriptions: Arc::new(AtomicBool::new(false)),
            auth_send_ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Clone the reactor-maintained "auth has ≥1 subscription" flag.
    pub(crate) fn auth_has_subscriptions_hint(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.auth_has_subscriptions)
    }

    /// Does the auth connection have ≥1 subscription?
    pub(crate) fn auth_has_subscriptions(&self) -> bool {
        self.auth_has_subscriptions.load(Ordering::Acquire)
    }

    /// Clone the reactor-maintained "auth bare-order send-ready" flag.
    pub(crate) fn auth_send_ready_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.auth_send_ready)
    }

    /// Is the auth connection bare-order send-ready now (`Authenticating` +
    /// empty auth registry + valid cached token)?
    pub(crate) fn auth_send_ready(&self) -> bool {
        self.auth_send_ready.load(Ordering::Acquire)
    }

    /// Hand out clones of both state mirrors for the I/O Reactor task.
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

    /// Clone the host-scoped `ConnectionRateBudget` for the I/O Reactor's
    /// `ManagedConnection`s.
    pub(crate) fn rate_budget_clone(&self) -> Arc<ConnectionRateBudget> {
        Arc::clone(&self.rate_budget)
    }

    /// Snapshot read of the FSM state for the given WS URL. May be stale by at
    /// most one FSM transition (single-writer atomic snapshot semantics).
    pub fn current_state(&self, url: WsUrl) -> ConnectionState {
        let mirror = self
            .state_mirrors
            .get(&url)
            .expect("state_mirrors invariant: both WsUrl present at construction");
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
        assert_eq!(sup.current_state(WsUrl::Public), ConnectionState::Idle);
        assert_eq!(sup.current_state(WsUrl::Auth), ConnectionState::Idle);
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
    fn state_mirror_clones_returns_both_v1_urls() {
        let sup = fixture();
        let mirrors = sup.state_mirror_clones();
        assert_eq!(mirrors.len(), 2);
        assert!(mirrors.contains_key(&WsUrl::Public));
        assert!(mirrors.contains_key(&WsUrl::Auth));
        mirrors[&WsUrl::Public].store(ConnectionState::Open.as_u8(), Ordering::Release);
        assert_eq!(sup.current_state(WsUrl::Public), ConnectionState::Open);
    }
}
