//! Timer helpers for the I/O Reactor select! loop: pick the earliest-due timer
//! across all connections, and pop a single-shot timer before FSM dispatch so a
//! past-due timer can't re-fire in a tight loop.

use std::collections::HashMap;

use crate::conn::ManagedConnection;
use crate::types::WsUrl;

/// Sleep until the earliest-due timer across all connections fires; returns its
/// URL + FsmEvent. Blocks forever if no connection has a timer armed
/// (`pending()` semantics).
pub(super) async fn next_timer_arm(
    conns: &HashMap<WsUrl, ManagedConnection>,
) -> (WsUrl, crate::conn::managed_connection::FsmEvent) {
    let (url, delay, ev) = match earliest_timer_delay(conns) {
        Some(t) => t,
        None => {
            std::future::pending::<()>().await;
            unreachable!()
        }
    };
    tokio::time::sleep(delay).await;
    (url, ev)
}

/// Earliest-due timer across all connections as a relative `delay` + its
/// `FsmEvent` (`None` if none armed). `now` is sampled from the owning
/// connection's injected clock.
fn earliest_timer_delay(
    conns: &HashMap<WsUrl, ManagedConnection>,
) -> Option<(
    WsUrl,
    std::time::Duration,
    crate::conn::managed_connection::FsmEvent,
)> {
    let mut earliest: Option<(
        WsUrl,
        crate::types::MonotonicInstant,
        crate::conn::managed_connection::FsmEvent,
    )> = None;
    for (url, mc) in conns {
        if let Some((due, ev)) = mc.next_timer_due() {
            match &earliest {
                Some((_, curr, _)) if curr.0 <= due.0 => {}
                _ => earliest = Some((*url, due, ev)),
            }
        }
    }
    let (url, due, ev) = earliest?;
    // Sample `now` from the owning MC's injected clock (the one the due-time
    // was computed against), not the wall clock — else determinism breaks under
    // a FixedClock.
    let now = conns
        .get(&url)
        .map(|mc| mc.clock_now())
        .unwrap_or_else(crate::types::MonotonicInstant::now);
    let delay = due.0.saturating_sub(now.0);
    Some((url, delay, ev))
}

pub(in crate::dispatch::io_reactor) fn pop_timer_for_event(
    mc: &mut ManagedConnection,
    event: &crate::conn::managed_connection::FsmEvent,
) {
    use crate::conn::managed_connection::FsmEvent;
    let timers = mc.timers_mut();
    match event {
        FsmEvent::TimerBackoffElapsed => {
            timers.backoff_due_at = None;
        }
        FsmEvent::TimerUpgradeTimeout => {
            timers.upgrade_timeout_due_at = None;
        }
        FsmEvent::TimerStalenessElapsed => {
            timers.staleness_due_at = None;
        }
        FsmEvent::TimerCloseTimeout => {
            timers.close_timeout_due_at = None;
        }
        FsmEvent::TimerRateBudgetWindowAdvanced => {
            timers.rate_budget_window_advanced_at = None;
        }
        FsmEvent::TimerTokenRefreshDue => {
            timers.token_refresh_due_at = None;
        }
        FsmEvent::TimerSubscribeAckTimeout { channel, pair } => {
            timers
                .per_entry_subscribe_ack_timeouts
                .remove(&(*channel, pair.clone()));
        }
        FsmEvent::TimerSubscribeResendDue { channel, pair } => {
            timers
                .per_entry_subscribe_resend_due
                .remove(&(*channel, pair.clone()));
        }
        FsmEvent::TimerBookReseedSnapshot { channel, pair } => {
            timers
                .per_entry_book_reseed_snapshot
                .remove(&(*channel, pair.clone()));
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Clock;
    use crate::conn::managed_connection::FsmEvent;
    use crate::types::MonotonicInstant;
    use std::sync::Arc;
    use std::time::Duration;

    struct FixedClock(Duration);
    impl Clock for FixedClock {
        fn now(&self) -> MonotonicInstant {
            MonotonicInstant(self.0)
        }
    }

    #[test]
    fn earliest_timer_delay_uses_owning_mc_injected_clock() {
        use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};

        let t = Duration::from_secs(1_000_000);
        let clock: Arc<dyn Clock> = Arc::new(FixedClock(t));
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let factory: Arc<dyn crate::transport::WsSocketFactoryLike> =
            Arc::new(crate::transport::MockWsSocketFactory::new());
        let budget = Arc::new(crate::conn::rate_budget::ConnectionRateBudget::new());
        let jitter: Arc<dyn crate::jitter::JitterSource> =
            Arc::new(crate::jitter::FixedJitter(0.0));
        let mut mc = ManagedConnection::new(
            WsUrl::Public,
            Arc::clone(&bus),
            factory,
            budget,
            Arc::clone(&clock),
            jitter,
        );
        mc.handle_event(FsmEvent::CallStartConnect { request_id: 1 });
        let expected = bus.knobs().upgrade_timeout();

        let mut conns = HashMap::new();
        conns.insert(WsUrl::Public, mc);

        let (url, delay, ev) =
            earliest_timer_delay(&conns).expect("upgrade-timeout timer is armed");
        assert_eq!(url, WsUrl::Public);
        assert!(
            matches!(ev, FsmEvent::TimerUpgradeTimeout),
            "expected upgrade-timeout event, got {ev:?}"
        );
        assert_eq!(
            delay, expected,
            "delay must be measured against the injected clock"
        );
    }

    #[test]
    fn earliest_timer_delay_none_when_no_timers() {
        let conns: HashMap<WsUrl, ManagedConnection> = HashMap::new();
        assert!(earliest_timer_delay(&conns).is_none());
    }
}
