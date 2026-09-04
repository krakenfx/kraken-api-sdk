use std::sync::Weak;

use crate::dispatch::DispatchEventBus;
use crate::types::SubscriberHandle;

/// RAII teardown for the broadcast subscribers `await_open_by_url` registers.
/// `Drop` runs on every exit including drop-at-`.await` (cancel/timeout), so a
/// parked await never leaks subscribers; `unsubscribe` is sync and idempotent.
pub(crate) struct SubscriberGuard {
    weak_bus: Weak<DispatchEventBus>,
    handles: Vec<SubscriberHandle>,
}

impl SubscriberGuard {
    pub(crate) fn new(weak_bus: Weak<DispatchEventBus>) -> Self {
        // Capacity 3 — max subscriber count (Open + Failed + SendReady).
        Self {
            weak_bus,
            handles: Vec::with_capacity(3),
        }
    }

    pub(crate) fn push(&mut self, h: SubscriberHandle) {
        self.handles.push(h);
    }
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        let Some(bus) = self.weak_bus.upgrade() else {
            return;
        };
        for h in self.handles.drain(..) {
            bus.unsubscribe(h);
        }
    }
}
