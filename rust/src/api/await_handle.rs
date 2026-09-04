//! Bridge from a `RequestHandle` to an awaitable future via a correlated
//! one-shot on the bus keyed by `(EventType, handle.id)`.

use std::sync::{Arc, PoisonError};

use tokio::sync::oneshot;

use crate::dispatch::{DispatchEventBus, EventEnvelope};
use crate::types::RequestHandle;

/// Error returned by [`await_request_handle`] when a reactor loop was already
/// dead at arm time, or the bus is torn down mid-await.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AwaitError {
    /// The dispatch loop / event bus was torn down before the awaited completion
    /// event was delivered.
    #[error("event bus was torn down before the awaited completion event was delivered")]
    LoopDead,
}

/// Completion arm plus optional fault arm for one awaited handle.
#[derive(Clone, Copy)]
pub(crate) struct CorrelatedArms {
    success: crate::types::SubscriberHandle,
    failure: Option<crate::types::SubscriberHandle>,
}

/// Sender both arms race to claim; taking it proves no arm fired.
pub(crate) type TxCell = Arc<std::sync::Mutex<Option<oneshot::Sender<EventEnvelope>>>>;

/// Arm the correlated waiter(s) for `handle`; both arms share one one-shot.
pub(crate) fn arm_correlated(
    bus: &Arc<DispatchEventBus>,
    handle: RequestHandle,
) -> (oneshot::Receiver<EventEnvelope>, CorrelatedArms, TxCell) {
    let (tx, rx) = oneshot::channel::<EventEnvelope>();
    let tx_cell: TxCell = Arc::new(std::sync::Mutex::new(Some(tx)));
    let event_type = handle.expected_completion.to_event_type();
    let send_arm = |cell: TxCell| {
        Box::new(move |env: &EventEnvelope| {
            // Declines if the waiter was released first — caller must keep the envelope.
            match cell.lock().unwrap_or_else(PoisonError::into_inner).take() {
                Some(tx) => tx.send(env.clone()).is_ok(),
                None => false,
            }
        }) as crate::dispatch::event_bus::CorrelatedCallback
    };
    let success = bus.subscribe_correlated(event_type, handle.id, send_arm(Arc::clone(&tx_cell)));
    // Fault arm never self-heals — must be unsubscribed explicitly.
    let failure = handle
        .expected_completion
        .failure_event_type()
        .map(|fail_type| {
            bus.subscribe_correlated(fail_type, handle.id, send_arm(Arc::clone(&tx_cell)))
        });
    (rx, CorrelatedArms { success, failure }, tx_cell)
}

/// Tear down arms held for a handle. Idempotent.
pub(crate) fn unarm_correlated(bus: &DispatchEventBus, arms: CorrelatedArms) {
    bus.unsubscribe(arms.success);
    if let Some(h) = arms.failure {
        bus.unsubscribe(h);
    }
}

struct ArmGuard<'a> {
    bus: &'a Arc<DispatchEventBus>,
    arms: CorrelatedArms,
}

impl Drop for ArmGuard<'_> {
    fn drop(&mut self) {
        unarm_correlated(self.bus, self.arms);
    }
}

/// Await a [`RequestHandle`] by correlating to its completion event on the bus;
/// a cancelled await leaks nothing. Semantics: docs/guides/error-handling.md.
///
/// # Errors
///
/// Returns [`AwaitError::LoopDead`] if a reactor loop had already died when the
/// await armed, or if the bus dropped the correlated one-shot during teardown.
pub async fn await_request_handle(
    bus: &Arc<DispatchEventBus>,
    handle: RequestHandle,
) -> Result<EventEnvelope, AwaitError> {
    // Order ops must not use this path: `to_event_type` collapses them onto
    // `ClientFailed`, aliasing a `ready()`-failure waiter.
    debug_assert!(
        !matches!(
            handle.expected_completion,
            crate::types::ExpectedCompletionEvent::OrderAdded
                | crate::types::ExpectedCompletionEvent::OrderEdited
                | crate::types::ExpectedCompletionEvent::OrderCancelled
        ),
        "order-op ExpectedCompletionEvent reached await_request_handle; orders must \
         resolve via connection teardown, not bus correlation"
    );
    let (mut rx, arms, tx_cell) = arm_correlated(bus, handle);
    let _guard = ArmGuard { bus, arms };
    resolve_armed(bus, &mut rx, &tx_cell).await
}

/// Await an already-armed correlated one-shot. A death that precedes the arm
/// never fires it — self-resolve instead of hanging.
pub(crate) async fn resolve_armed(
    bus: &Arc<DispatchEventBus>,
    rx: &mut oneshot::Receiver<EventEnvelope>,
    tx_cell: &TxCell,
) -> Result<EventEnvelope, AwaitError> {
    // Claim the sender rather than polling the receiver: taking it proves no arm
    // fired (an empty receiver could also mean an arm is mid-send).
    if bus.loop_death_observed()
        && tx_cell
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .is_some()
    {
        return Err(AwaitError::LoopDead);
    }
    rx.await.map_err(|_| AwaitError::LoopDead)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::{
        DispatchEventBus, DispatchEventBusConfig, EventEnvelope, EventPayload, EventType,
    };
    use crate::types::{ExpectedCompletionEvent, MonotonicInstant, RequestHandle};
    use std::sync::Arc;

    fn new_bus() -> Arc<DispatchEventBus> {
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::default(),
            Arc::new(crate::clock::SystemClock),
        ));
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        bus
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "order-op ExpectedCompletionEvent reached await_request_handle")]
    async fn order_op_completion_trips_the_cv1_guard() {
        let bus = new_bus();
        let handle = RequestHandle {
            id: 1,
            expected_completion: ExpectedCompletionEvent::OrderAdded,
        };
        let _ = await_request_handle(&bus, handle).await;
    }

    #[tokio::test]
    async fn resolves_when_matching_correlated_event_is_published() {
        let bus = new_bus();
        let handle = RequestHandle {
            id: 42,
            expected_completion: ExpectedCompletionEvent::SubscriptionTerminated,
        };

        let bus_pub = Arc::clone(&bus);
        let fut = tokio::spawn(async move { await_request_handle(&bus, handle).await });

        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        bus_pub.publish(EventEnvelope {
            event_type: EventType::SubscriptionTerminatedEvent,
            event_version: 2,
            timestamp_monotonic: MonotonicInstant::now(),
            request_id: Some(42),
            payload: EventPayload::SubscriptionTerminatedEvent {
                channel: crate::types::ChannelName::Ticker,
                pair: None,
                cause: crate::api::subscription::TerminationCause::ClientClosed,
                last_error: None,
                terminated_at_monotonic: MonotonicInstant::now(),
            },
        });

        let env = fut
            .await
            .expect("task panicked")
            .expect("await resolved with an event");
        assert_eq!(env.request_id, Some(42));
        assert_eq!(env.event_type, EventType::SubscriptionTerminatedEvent);
    }

    #[tokio::test]
    async fn ignores_unrelated_request_ids() {
        let bus = new_bus();
        let handle = RequestHandle {
            id: 100,
            expected_completion: ExpectedCompletionEvent::ClientReady,
        };

        let bus_pub = Arc::clone(&bus);
        let fut = tokio::spawn(async move { await_request_handle(&bus, handle).await });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        bus_pub.publish(EventEnvelope {
            event_type: EventType::ClientReady,
            event_version: 1,
            timestamp_monotonic: MonotonicInstant::now(),
            request_id: Some(999),
            payload: EventPayload::ClientReady {
                capability_snapshot: crate::types::CapabilitySnapshot {
                    declared_namespaces: std::collections::HashSet::new(),
                    declared_ws_urls: std::collections::HashSet::new(),
                    discovered_at_first_use: std::collections::HashSet::new(),
                },
            },
        });

        let timeout = tokio::time::timeout(std::time::Duration::from_millis(20), fut).await;
        assert!(timeout.is_err(), "future resolved against wrong request_id");
    }

    #[tokio::test]
    async fn ready_await_resolves_on_clientfailed_failure_arm() {
        let bus = new_bus();
        let handle = RequestHandle {
            id: 7,
            expected_completion: ExpectedCompletionEvent::ClientReady,
        };
        let bus_pub = Arc::clone(&bus);
        let fut = tokio::spawn(async move { await_request_handle(&bus, handle).await });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        bus_pub.publish(EventEnvelope {
            event_type: EventType::ClientFailed,
            event_version: 1,
            timestamp_monotonic: MonotonicInstant::now(),
            request_id: Some(7),
            payload: EventPayload::ClientFailed {
                cause: crate::dispatch::ClientFailureCause::ReactorSpawnFailed,
            },
        });

        let env = tokio::time::timeout(std::time::Duration::from_millis(200), fut)
            .await
            .expect("ready() await hung on failure — the ClientFailed arm is not wired")
            .expect("task panicked")
            .expect("await resolved with an event");
        assert_eq!(env.event_type, EventType::ClientFailed);
        assert_eq!(env.request_id, Some(7));
    }

    #[test]
    fn await_error_loop_dead_display_names_the_teardown_cause() {
        assert_eq!(
            AwaitError::LoopDead.to_string(),
            "event bus was torn down before the awaited completion event was delivered"
        );
    }

    #[tokio::test]
    async fn arming_after_loop_death_resolves_loop_dead_not_hang() {
        let bus = new_bus();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::UnhandledError,
        );

        let handle = RequestHandle {
            id: 21,
            expected_completion: ExpectedCompletionEvent::SubscriptionTerminated,
        };
        let res = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            await_request_handle(&bus, handle),
        )
        .await
        .expect("await armed after loop death hung — the self-resolve check is not wired");
        assert!(matches!(res, Err(AwaitError::LoopDead)));
    }

    #[tokio::test]
    async fn mid_close_loop_death_resolves_a_client_closed_waiter() {
        let bus = new_bus();
        let handle = RequestHandle {
            id: 33,
            expected_completion: ExpectedCompletionEvent::ClientClosed,
        };
        let bus_task = Arc::clone(&bus);
        let fut = tokio::spawn(async move { await_request_handle(&bus_task, handle).await });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Reactor panic after close() latched shutdown: no ClientClosedEvent for this id.
        bus.begin_shutdown();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::Panic,
        );

        let env = tokio::time::timeout(std::time::Duration::from_millis(200), fut)
            .await
            .expect("close() await hung on a mid-close loop death")
            .expect("task panicked")
            .expect("await resolved with an event");
        assert_eq!(env.event_type, EventType::LoopFailedEvent);
        assert_eq!(env.request_id, Some(33));
    }

    #[tokio::test]
    async fn completion_latched_before_death_beats_the_death_path() {
        let bus = new_bus();
        bus.deliver_correlated(&EventEnvelope {
            event_type: EventType::ClientClosedEvent,
            event_version: 1,
            timestamp_monotonic: MonotonicInstant::now(),
            request_id: Some(5),
            payload: EventPayload::ClientClosedEvent {
                reason: crate::dispatch::ClientCloseReason::UserClose,
                initiated_at_monotonic: MonotonicInstant::now(),
            },
        });
        bus.begin_shutdown();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::Panic,
        );

        let handle = RequestHandle {
            id: 5,
            expected_completion: ExpectedCompletionEvent::ClientClosed,
        };
        let env = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            await_request_handle(&bus, handle),
        )
        .await
        .expect("late arm hung despite a latched completion")
        .expect("await resolved with an event");
        assert_eq!(env.event_type, EventType::ClientClosedEvent);
        assert_eq!(env.request_id, Some(5));
    }

    #[tokio::test]
    async fn dropped_await_tears_down_both_arms_no_leak() {
        let bus = new_bus();
        let baseline = bus.correlated_live_count();
        let handle = RequestHandle {
            id: 7,
            expected_completion: ExpectedCompletionEvent::ClientReady,
        };
        let bus_task = Arc::clone(&bus);
        let task = tokio::spawn(async move { await_request_handle(&bus_task, handle).await });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(
            bus.correlated_live_count(),
            baseline + 2,
            "both arms (ClientReady + ClientFailed) should be live while awaiting"
        );

        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;
        assert_eq!(
            bus.correlated_live_count(),
            baseline,
            "dropping the await must tear down BOTH arms (the failure arm does not self-heal)"
        );
    }

    #[tokio::test]
    async fn dropped_single_arm_await_tears_down_no_leak() {
        let bus = new_bus();
        let baseline = bus.correlated_live_count();
        let handle = RequestHandle {
            id: 11,
            expected_completion: ExpectedCompletionEvent::SubscriptionTerminated,
        };
        let bus_task = Arc::clone(&bus);
        let task = tokio::spawn(async move { await_request_handle(&bus_task, handle).await });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(
            bus.correlated_live_count(),
            baseline + 1,
            "a single-arm variant registers exactly one correlated waiter"
        );

        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;
        assert_eq!(
            bus.correlated_live_count(),
            baseline,
            "dropping a single-arm await must tear down its sole arm"
        );
    }

    #[tokio::test]
    async fn poisoned_tx_cell_still_delivers_and_self_resolves() {
        let bus = new_bus();
        let handle = RequestHandle {
            id: 19,
            expected_completion: ExpectedCompletionEvent::ClientReady,
        };
        let (mut rx, arms, tx_cell) = arm_correlated(&bus, handle);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = tx_cell.lock().unwrap();
            panic!("poison tx cell");
        }));

        bus.deliver_correlated(&EventEnvelope {
            event_type: EventType::ClientReady,
            event_version: 1,
            timestamp_monotonic: MonotonicInstant::now(),
            request_id: Some(19),
            payload: EventPayload::ClientReady {
                capability_snapshot: crate::types::CapabilitySnapshot {
                    declared_namespaces: std::collections::HashSet::new(),
                    declared_ws_urls: std::collections::HashSet::new(),
                    discovered_at_first_use: std::collections::HashSet::new(),
                },
            },
        });
        let env = tokio::time::timeout(std::time::Duration::from_millis(200), &mut rx)
            .await
            .expect("poisoned tx cell blocked correlated delivery")
            .expect("oneshot closed unexpectedly");
        assert_eq!(env.request_id, Some(19));
        unarm_correlated(&bus, arms);

        // Death observed before resolve: claim path must recover from poison too.
        let bus = new_bus();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::UnhandledError,
        );
        let handle = RequestHandle {
            id: 20,
            expected_completion: ExpectedCompletionEvent::ClientReady,
        };
        let (mut rx, arms, tx_cell) = arm_correlated(&bus, handle);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = tx_cell.lock().unwrap();
            panic!("poison tx cell again");
        }));
        let res = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            resolve_armed(&bus, &mut rx, &tx_cell),
        )
        .await
        .expect("poisoned tx cell hung on LoopDead self-resolve");
        assert!(matches!(res, Err(AwaitError::LoopDead)));
        unarm_correlated(&bus, arms);
    }
}
