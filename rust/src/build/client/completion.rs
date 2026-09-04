//! Awaitable lifecycle completion returned by [`Client::ready`] and
//! [`Client::close`]. The effect is already in flight; awaiting only observes it.
//!
//! [`Client::ready`]: super::Client::ready
//! [`Client::close`]: super::Client::close

use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::{Arc, PoisonError};

use tokio::sync::oneshot;

use crate::dispatch::{ClientFailureCause, DispatchEventBus, EventEnvelope, EventPayload};
use crate::error::{ApiError, ErrorCategory, sealed};
use crate::types::RequestHandle;

/// Armed waiter; `Drop` is the sole release policy (await, discard, or cancel).
struct ArmedState {
    bus: Arc<DispatchEventBus>,
    arms: Option<crate::api::CorrelatedArms>,
    rx: Option<oneshot::Receiver<EventEnvelope>>,
    /// Claiming this proves no arm fired.
    tx_cell: crate::api::TxCell,
    /// Whether the caller took the handle (needed to re-latch a discarded terminal).
    handle_taken: std::sync::atomic::AtomicBool,
}

impl Drop for ArmedState {
    fn drop(&mut self) {
        if let Some(arms) = self.arms.take() {
            crate::api::unarm_correlated(&self.bus, arms);
        }
        if !self.handle_taken.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        // Failing to claim proves the terminal landed; claiming proves no arm sent
        // (delivery may still latch when the arm declines).
        if self
            .tx_cell
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .is_some()
        {
            return;
        }
        if let Some(mut rx) = self.rx.take() {
            if let Ok(env) = rx.try_recv() {
                self.bus.relatch_correlated(&env);
            }
        }
    }
}

/// Awaitable completion of a lifecycle operation. Discarding is fire-and-forget;
/// [`Self::handle`] observes the same outcome on the bus.
pub struct Completion<E> {
    handle: RequestHandle,
    /// Armed at construction so an early terminal still resolves this value.
    state: Option<ArmedState>,
    classify: fn(&EventEnvelope) -> Result<(), E>,
    loop_dead: fn() -> E,
}

impl<E> Completion<E> {
    /// Arm the correlated waiter now; the caller may await later, or never.
    fn armed(
        handle: RequestHandle,
        bus: Arc<DispatchEventBus>,
        classify: fn(&EventEnvelope) -> Result<(), E>,
        loop_dead: fn() -> E,
    ) -> Self {
        let (rx, arms, tx_cell) = crate::api::arm_correlated(&bus, handle);
        Self {
            handle,
            state: Some(ArmedState {
                bus,
                arms: Some(arms),
                rx: Some(rx),
                tx_cell,
                handle_taken: std::sync::atomic::AtomicBool::new(false),
            }),
            classify,
            loop_dead,
        }
    }

    /// Correlated handle for event-bus observation of the same outcome.
    pub fn handle(&self) -> RequestHandle {
        if let Some(state) = self.state.as_ref() {
            state
                .handle_taken
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        self.handle
    }

    /// Poison the one-shot claim cell so Drop's recover path can be regression-tested.
    #[cfg(test)]
    fn poison_tx_cell_for_test(&self) {
        let Some(state) = self.state.as_ref() else {
            return;
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = state.tx_cell.lock().unwrap();
            panic!("poison completion tx");
        }));
    }
}

impl<E> std::fmt::Debug for Completion<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Completion")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl<E: Send + 'static> IntoFuture for Completion<E> {
    type Output = Result<(), E>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<(), E>> + Send>>;

    fn into_future(mut self) -> Self::IntoFuture {
        // Move armed state into the future so Drop runs even on cancel.
        let state = self.state.take();
        let classify = self.classify;
        let loop_dead = self.loop_dead;
        Box::pin(async move {
            let Some(mut state) = state else {
                return Err(loop_dead());
            };
            let Some(rx) = state.rx.as_mut() else {
                return Err(loop_dead());
            };
            match crate::api::resolve_armed(&state.bus, rx, &state.tx_cell).await {
                Ok(env) => classify(&env),
                Err(_) => Err(loop_dead()),
            }
        })
    }
}

pub(crate) fn ready_completion(
    handle: RequestHandle,
    bus: Arc<DispatchEventBus>,
) -> Completion<ReadyError> {
    Completion::armed(
        handle,
        bus,
        |env| match &env.payload {
            EventPayload::ClientReady { .. } => Ok(()),
            EventPayload::ClientFailed {
                cause: ClientFailureCause::ReactorSpawnFailed,
            } => Err(ReadyError::ReactorSpawnFailed),
            _ => Err(ReadyError::LoopFailed),
        },
        || ReadyError::LoopFailed,
    )
}

pub(crate) fn close_completion(
    handle: RequestHandle,
    bus: Arc<DispatchEventBus>,
) -> Completion<CloseError> {
    Completion::armed(
        handle,
        bus,
        |env| match &env.payload {
            EventPayload::ClientClosedEvent { .. } => Ok(()),
            _ => Err(CloseError::Interrupted),
        },
        || CloseError::Interrupted,
    )
}

/// Error resolving [`Client::ready`](super::Client::ready)'s completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ReadyError {
    /// The I/O reactor task could not be started.
    #[error("The client's internal loop could not be started.")]
    ReactorSpawnFailed,
    /// A reactor loop died; the client is not ready. Terminal — v1 never restarts.
    #[error("The client's internal loop has failed; the client is not ready. Rebuild the client.")]
    LoopFailed,
}

impl sealed::Sealed for ReadyError {}

impl ApiError for ReadyError {
    fn code(&self) -> &str {
        match self {
            ReadyError::ReactorSpawnFailed => "REACTOR_SPAWN_FAILED",
            ReadyError::LoopFailed => "LOOP_DEAD",
        }
    }
    fn category(&self) -> ErrorCategory {
        ErrorCategory::Client
    }
    fn retryable(&self) -> bool {
        false
    }
    fn request_id(&self) -> Option<&str> {
        None
    }
    fn message(&self) -> String {
        self.to_string()
    }
    fn kraken_code(&self) -> Option<&str> {
        None
    }
}

/// Error resolving [`Client::close`](super::Client::close)'s completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CloseError {
    /// A reactor loop died mid-close (or was already dead): the drain may not
    /// have completed — re-check venue-side state.
    #[error(
        "The client's internal loop has failed during shutdown; the connection drain may not have completed."
    )]
    Interrupted,
}

impl sealed::Sealed for CloseError {}

impl ApiError for CloseError {
    fn code(&self) -> &str {
        match self {
            CloseError::Interrupted => "CLOSE_INTERRUPTED",
        }
    }
    fn category(&self) -> ErrorCategory {
        ErrorCategory::Client
    }
    fn retryable(&self) -> bool {
        false
    }
    fn request_id(&self) -> Option<&str> {
        None
    }
    fn message(&self) -> String {
        self.to_string()
    }
    fn kraken_code(&self) -> Option<&str> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::{
        ClientCloseReason, DispatchEventBus, DispatchEventBusConfig, EventType, LoopFailureCause,
        ReactorName,
    };
    use crate::types::{ExpectedCompletionEvent, MonotonicInstant, RequestHandle};

    fn bus() -> Arc<DispatchEventBus> {
        Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::default(),
            Arc::new(crate::clock::SystemClock),
        ))
    }

    fn handle(expected: ExpectedCompletionEvent) -> RequestHandle {
        RequestHandle {
            id: 1,
            expected_completion: expected,
        }
    }

    fn envelope(event_type: EventType, payload: EventPayload) -> EventEnvelope {
        EventEnvelope {
            event_type,
            event_version: 1,
            timestamp_monotonic: MonotonicInstant::now(),
            request_id: Some(1),
            payload,
        }
    }

    fn loop_failed_envelope() -> EventEnvelope {
        envelope(
            EventType::LoopFailedEvent,
            EventPayload::LoopFailedEvent {
                loop_name: ReactorName::Io,
                cause: LoopFailureCause::Panic,
                failed_at_monotonic: MonotonicInstant::now(),
            },
        )
    }

    #[test]
    fn ready_classify_maps_terminals_onto_the_per_op_outcome() {
        let c = ready_completion(handle(ExpectedCompletionEvent::ClientReady), bus());
        assert_eq!(
            (c.classify)(&envelope(
                EventType::ClientReady,
                EventPayload::ClientReady {
                    capability_snapshot: crate::types::CapabilitySnapshot {
                        declared_namespaces: std::collections::HashSet::new(),
                        declared_ws_urls: std::collections::HashSet::new(),
                        discovered_at_first_use: std::collections::HashSet::new(),
                    },
                },
            )),
            Ok(())
        );
        assert_eq!(
            (c.classify)(&envelope(
                EventType::ClientFailed,
                EventPayload::ClientFailed {
                    cause: ClientFailureCause::ReactorSpawnFailed,
                },
            )),
            Err(ReadyError::ReactorSpawnFailed)
        );
        assert_eq!(
            (c.classify)(&envelope(
                EventType::ClientFailed,
                EventPayload::ClientFailed {
                    cause: ClientFailureCause::LoopFailed,
                },
            )),
            Err(ReadyError::LoopFailed)
        );
        assert_eq!(
            (c.classify)(&loop_failed_envelope()),
            Err(ReadyError::LoopFailed)
        );
        assert_eq!((c.loop_dead)(), ReadyError::LoopFailed);
    }

    #[test]
    fn close_classify_maps_terminals_onto_the_per_op_outcome() {
        let c = close_completion(handle(ExpectedCompletionEvent::ClientClosed), bus());
        assert_eq!(
            (c.classify)(&envelope(
                EventType::ClientClosedEvent,
                EventPayload::ClientClosedEvent {
                    reason: ClientCloseReason::UserClose,
                    initiated_at_monotonic: MonotonicInstant::now(),
                },
            )),
            Ok(())
        );
        assert_eq!(
            (c.classify)(&loop_failed_envelope()),
            Err(CloseError::Interrupted)
        );
        assert_eq!((c.loop_dead)(), CloseError::Interrupted);
    }

    #[test]
    fn lifecycle_errors_pin_the_locked_apierror_accessors() {
        for (e, code) in [
            (
                &ReadyError::ReactorSpawnFailed as &dyn ApiError,
                "REACTOR_SPAWN_FAILED",
            ),
            (&ReadyError::LoopFailed as &dyn ApiError, "LOOP_DEAD"),
            (
                &CloseError::Interrupted as &dyn ApiError,
                "CLOSE_INTERRUPTED",
            ),
        ] {
            assert_eq!(e.code(), code);
            assert_eq!(e.category(), ErrorCategory::Client);
            assert!(!e.retryable());
            assert_eq!(e.request_id(), None);
            assert_eq!(e.kraken_code(), None);
        }
    }

    #[test]
    fn completion_debug_shows_the_handle_not_the_bus() {
        let c = ready_completion(handle(ExpectedCompletionEvent::ClientReady), bus());
        let rendered = format!("{c:?}");
        assert!(rendered.contains("handle"), "debug output: {rendered}");
    }

    #[test]
    fn discarded_completion_recovers_from_poisoned_tx_cell() {
        let c = ready_completion(handle(ExpectedCompletionEvent::ClientReady), bus());
        let _ = c.handle();
        c.poison_tx_cell_for_test();
        // Drop must claim via into_inner — a poisoned cell must not panic here.
        drop(c);
    }
}
