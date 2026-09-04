//! Bus-level contract tests for subscriber fanout and panic isolation.

use super::*;
use crate::clock::SystemClock;

fn new_bus() -> Arc<DispatchEventBus> {
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::default(),
        Arc::new(SystemClock),
    ));
    bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
    bus
}

#[tokio::test]
async fn long_lived_subscriber_receives_published_events() {
    let bus = new_bus();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<EventEnvelope>(8);
    let _ = bus.subscribe(
        EventType::ClientReady,
        Arc::new(move |env| {
            let _ = tx.try_send(env.clone());
        }),
        1,
    );
    bus.publish(EventEnvelope {
        event_type: EventType::ClientReady,
        event_version: 1,
        timestamp_monotonic: crate::types::MonotonicInstant::now(),
        request_id: None,
        payload: EventPayload::ClientReady {
            capability_snapshot: crate::types::CapabilitySnapshot {
                declared_namespaces: std::collections::HashSet::new(),
                declared_ws_urls: std::collections::HashSet::new(),
                discovered_at_first_use: std::collections::HashSet::new(),
            },
        },
    });
    let env = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
        .await
        .expect("event not delivered within 200ms")
        .expect("channel closed");
    assert_eq!(env.event_type, EventType::ClientReady);
}

#[tokio::test]
async fn long_lived_subscriber_ignores_unmatched_event_type() {
    let bus = new_bus();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<EventEnvelope>(8);
    let _ = bus.subscribe(
        EventType::ConnectionOpenEvent,
        Arc::new(move |env| {
            let _ = tx.try_send(env.clone());
        }),
        1,
    );
    bus.publish(EventEnvelope {
        event_type: EventType::ClientReady,
        event_version: 1,
        timestamp_monotonic: crate::types::MonotonicInstant::now(),
        request_id: None,
        payload: EventPayload::ClientReady {
            capability_snapshot: crate::types::CapabilitySnapshot {
                declared_namespaces: std::collections::HashSet::new(),
                declared_ws_urls: std::collections::HashSet::new(),
                discovered_at_first_use: std::collections::HashSet::new(),
            },
        },
    });
    let timeout = tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await;
    assert!(
        timeout.is_err(),
        "ConnectionOpenEvent subscriber must not receive ClientReady"
    );
}

#[tokio::test]
async fn downgrade_to_is_identity_for_v1_payloads() {
    let payload = EventPayload::ClientReady {
        capability_snapshot: crate::types::CapabilitySnapshot {
            declared_namespaces: std::collections::HashSet::new(),
            declared_ws_urls: std::collections::HashSet::new(),
            discovered_at_first_use: std::collections::HashSet::new(),
        },
    };
    let downgraded = payload.clone().downgrade_to(0);
    assert!(matches!(downgraded, EventPayload::ClientReady { .. }));
}

#[test]
fn downgrade_to_drops_handler_id_for_callback_latency() {
    use crate::dispatch::handler_registry::HandlerId;
    let v2 = EventPayload::CallbackLatency {
        callback_event_type: CallbackSource::Event(EventType::ClientReady),
        handler_id: Some(HandlerId(7)),
        latency_us: 42,
    };
    match v2.clone().downgrade_to(1) {
        EventPayload::CallbackLatency {
            callback_event_type,
            handler_id,
            latency_us,
        } => {
            assert_eq!(
                callback_event_type,
                CallbackSource::Event(EventType::ClientReady)
            );
            assert_eq!(handler_id, None, "v1 drops per-handler resolution");
            assert_eq!(latency_us, 42);
        }
        other => panic!("expected CallbackLatency, got {other:?}"),
    }
    match v2.downgrade_to(2) {
        EventPayload::CallbackLatency { handler_id, .. } => {
            assert_eq!(handler_id, Some(HandlerId(7)));
        }
        other => panic!("expected CallbackLatency, got {other:?}"),
    }
}

#[test]
fn callback_source_keeps_each_channel_distinct() {
    use crate::types::ChannelName;
    // Book and BookRaw share the wire "book" subscribe-frame name but are distinct
    // callbacks (on_book vs on_book_raw); their latency samples must not collapse
    // into one bucket.
    assert_ne!(
        CallbackSource::DataChannel(ChannelName::Book),
        CallbackSource::DataChannel(ChannelName::BookRaw)
    );
    assert_ne!(
        CallbackSource::DataChannel(ChannelName::Ticker),
        CallbackSource::Event(EventType::SubscriptionGapEvent)
    );
}

fn client_ready_envelope() -> EventEnvelope {
    EventEnvelope {
        event_type: EventType::ClientReady,
        event_version: 1,
        timestamp_monotonic: crate::types::MonotonicInstant::now(),
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
async fn callback_latency_emitted_on_long_lived_invocation() {
    let bus = new_bus();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<EventEnvelope>(8);
    let _metric = bus.subscribe(
        EventType::CallbackLatency,
        Arc::new(move |env| {
            let _ = tx.try_send(env.clone());
        }),
        2,
    );
    // The worker sleeps a known duration so the microsecond resolution is
    // pinned below: an as_millis() revert at the lifecycle emit site would
    // report 2, not >= 2_000.
    let _worker = bus.subscribe(
        EventType::ClientReady,
        Arc::new(|_env| std::thread::sleep(std::time::Duration::from_millis(2))),
        1,
    );
    bus.publish(client_ready_envelope());

    let env = tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
        .await
        .expect("CallbackLatency not emitted within 200ms")
        .expect("channel closed");
    assert_eq!(env.event_type, EventType::CallbackLatency);
    assert_eq!(env.event_version, 2);
    match env.payload {
        EventPayload::CallbackLatency {
            callback_event_type,
            handler_id,
            latency_us,
        } => {
            assert_eq!(
                callback_event_type,
                CallbackSource::Event(EventType::ClientReady)
            );
            assert!(handler_id.is_some(), "v2 subscriber sees handler_id");
            // Monotonic elapsed >= the 2ms sleep, so the bound is not
            // timing-fragile; no upper bound to keep CI calm.
            assert!(
                latency_us >= 2_000,
                "a 2ms callback must report >= 2000us, got {latency_us}"
            );
        }
        other => panic!("expected CallbackLatency, got {other:?}"),
    }
}

#[tokio::test]
async fn queue_depth_sample_emitted_on_interval_cadence() {
    let bus = new_bus();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<EventEnvelope>(8);
    let _sub = bus.subscribe(
        EventType::QueueDepthSample,
        Arc::new(move |env| {
            let _ = tx.try_send(env.clone());
        }),
        1,
    );
    bus.publish(client_ready_envelope());
    tokio::time::sleep(std::time::Duration::from_millis(130)).await;
    bus.publish(client_ready_envelope());

    let env = tokio::time::timeout(std::time::Duration::from_millis(400), rx.recv())
        .await
        .expect("QueueDepthSample not emitted on cadence")
        .expect("channel closed");
    assert_eq!(env.event_type, EventType::QueueDepthSample);
    match env.payload {
        EventPayload::QueueDepthSample {
            queue, capacity, ..
        } => {
            assert_eq!(queue, QueueName::IoToDispatch);
            assert!(capacity > 0);
        }
        other => panic!("expected QueueDepthSample, got {other:?}"),
    }
}

/// Panic hook that silences only the one deliberate panic (exact payload match)
/// and delegates all others to the previous hook.
fn install_marker_filtering_hook(
    expected_panic: &'static str,
) -> Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send + 'static> {
    let prev_hook = std::panic::take_hook();
    let prev = std::sync::Arc::new(prev_hook);
    let prev_for_hook = std::sync::Arc::clone(&prev);
    std::panic::set_hook(Box::new(move |info| {
        if panic_info_message(info) == expected_panic {
            return;
        }
        prev_for_hook(info);
    }));
    Box::new(move |info: &std::panic::PanicHookInfo<'_>| prev(info))
}

fn panic_info_message(info: &std::panic::PanicHookInfo<'_>) -> String {
    let p = info.payload();
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        String::new()
    }
}

fn make_client_ready_event() -> EventEnvelope {
    EventEnvelope {
        event_type: EventType::ClientReady,
        event_version: 1,
        timestamp_monotonic: crate::types::MonotonicInstant::now(),
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
async fn panicking_long_lived_subscriber_does_not_kill_reactor() {
    let prev_hook =
        install_marker_filtering_hook("intentional subscriber panic for isolation test");

    let bus = new_bus();

    let (second_tx, mut second_rx) = tokio::sync::mpsc::channel::<EventEnvelope>(8);
    let (warn_tx, mut warn_rx) = tokio::sync::mpsc::channel::<EventEnvelope>(8);

    let _ = bus.subscribe(
        EventType::ClientReady,
        Arc::new(move |_env| {
            panic!("intentional subscriber panic for isolation test");
        }),
        1,
    );

    {
        let tx = second_tx.clone();
        let _ = bus.subscribe(
            EventType::ClientReady,
            Arc::new(move |env| {
                let _ = tx.try_send(env.clone());
            }),
            1,
        );
    }

    {
        let tx = warn_tx.clone();
        let _ = bus.subscribe(
            EventType::HandlerPanicWarning,
            Arc::new(move |env| {
                let _ = tx.try_send(env.clone());
            }),
            1,
        );
    }

    bus.publish(make_client_ready_event());

    let second_delivery =
        tokio::time::timeout(std::time::Duration::from_millis(500), second_rx.recv())
            .await
            .expect("second subscriber timed out — reactor may have died")
            .expect("second_rx closed");
    assert_eq!(second_delivery.event_type, EventType::ClientReady);

    let warn_delivery = tokio::time::timeout(std::time::Duration::from_millis(500), warn_rx.recv())
        .await
        .expect("HandlerPanicWarning not delivered within 500ms — panic not caught or bus dead")
        .expect("warn_rx closed");
    assert_eq!(warn_delivery.event_type, EventType::HandlerPanicWarning);
    if let EventPayload::HandlerPanicWarning {
        panic_message,
        source,
        ..
    } = &warn_delivery.payload
    {
        assert!(
            panic_message.contains("intentional subscriber panic"),
            "unexpected panic_message: {panic_message}"
        );
        assert_eq!(
            *source,
            CallbackSource::Event(EventType::ClientReady),
            "a lifecycle-handler panic must carry the honest EventType source"
        );
    } else {
        panic!("HandlerPanicWarning payload shape mismatch");
    }

    bus.publish(make_client_ready_event());
    let third_delivery =
        tokio::time::timeout(std::time::Duration::from_millis(500), second_rx.recv())
            .await
            .expect("third publish timed out — reactor died after first panic")
            .expect("second_rx closed on third publish");
    assert_eq!(third_delivery.event_type, EventType::ClientReady);

    std::panic::set_hook(prev_hook);
}

#[tokio::test]
async fn panicking_one_shot_correlated_handler_does_not_kill_reactor() {
    let prev_hook = install_marker_filtering_hook("intentional one-shot panic for isolation test");

    let bus = new_bus();

    let (alive_tx, mut alive_rx) = tokio::sync::mpsc::channel::<EventEnvelope>(8);
    let _ = bus.subscribe(
        EventType::ClientReady,
        Arc::new(move |env| {
            let _ = alive_tx.try_send(env.clone());
        }),
        1,
    );

    let (warn_tx, mut warn_rx) = tokio::sync::mpsc::channel::<EventEnvelope>(8);
    {
        let tx = warn_tx.clone();
        let _ = bus.subscribe(
            EventType::HandlerPanicWarning,
            Arc::new(move |env| {
                let _ = tx.try_send(env.clone());
            }),
            1,
        );
    }

    let correlation_key: u64 = 0xdeadbeef;
    let _ = bus.subscribe_correlated(
        EventType::ClientReady,
        correlation_key,
        Box::new(move |_env| {
            panic!("intentional one-shot panic for isolation test");
        }),
    );

    bus.publish(EventEnvelope {
        event_type: EventType::ClientReady,
        event_version: 1,
        timestamp_monotonic: crate::types::MonotonicInstant::now(),
        request_id: Some(correlation_key),
        payload: EventPayload::ClientReady {
            capability_snapshot: crate::types::CapabilitySnapshot {
                declared_namespaces: std::collections::HashSet::new(),
                declared_ws_urls: std::collections::HashSet::new(),
                discovered_at_first_use: std::collections::HashSet::new(),
            },
        },
    });

    let delivery = tokio::time::timeout(std::time::Duration::from_millis(500), alive_rx.recv())
        .await
        .expect("long-lived subscriber timed out after one-shot panic — reactor may have died")
        .expect("alive_rx closed");
    assert_eq!(delivery.event_type, EventType::ClientReady);

    let warn = tokio::time::timeout(std::time::Duration::from_millis(500), warn_rx.recv())
        .await
        .expect("HandlerPanicWarning not delivered within 500ms")
        .expect("warn_rx closed");
    if let EventPayload::HandlerPanicWarning { source, .. } = &warn.payload {
        assert_eq!(
            *source,
            CallbackSource::Event(EventType::ClientReady),
            "a correlated one-shot panic must carry the honest EventType source"
        );
    } else {
        panic!("HandlerPanicWarning payload shape mismatch");
    }

    bus.publish(make_client_ready_event());
    let second_delivery =
        tokio::time::timeout(std::time::Duration::from_millis(500), alive_rx.recv())
            .await
            .expect("second publish timed out — reactor died after one-shot panic")
            .expect("alive_rx closed on second publish");
    assert_eq!(second_delivery.event_type, EventType::ClientReady);

    std::panic::set_hook(prev_hook);
}

#[test]
fn drop_oldest_ring_push_returns_drop_flag() {
    let ring: DropOldestRing<u32> = DropOldestRing::new(2);

    let d1 = ring.push(1);
    assert!(!d1, "first push into empty ring must not drop");

    let d2 = ring.push(2);
    assert!(!d2, "second push (ring now full) must not drop");

    let d3 = ring.push(3);
    assert!(d3, "third push into a full ring-of-2 must report a drop");

    let d4 = ring.push(4);
    assert!(d4, "fourth push must report another drop");
}

#[test]
fn drop_oldest_ring_drop_count_accumulates_correctly() {
    let ring: DropOldestRing<u32> = DropOldestRing::new(1);
    let drop_count = std::sync::atomic::AtomicU64::new(0);

    let d1 = ring.push(10);
    if d1 {
        drop_count.fetch_add(1, Ordering::Relaxed);
    }
    assert_eq!(
        drop_count.load(Ordering::Relaxed),
        0,
        "first push must not drop"
    );

    for i in 0..5_u32 {
        let dropped = ring.push(i);
        if dropped {
            drop_count.fetch_add(1, Ordering::Relaxed);
        }
    }
    assert_eq!(
        drop_count.load(Ordering::Relaxed),
        5,
        "five overflow pushes must accumulate five drops"
    );
}

#[tokio::test]
async fn drop_oldest_ring_close_drains_then_returns_none() {
    let ring: DropOldestRing<u32> = DropOldestRing::new(8);
    ring.push(1);
    ring.push(2);
    ring.push(3);
    ring.close();
    assert_eq!(ring.recv().await, Some(1));
    assert_eq!(ring.recv().await, Some(2));
    assert_eq!(ring.recv().await, Some(3));
    let end = tokio::time::timeout(std::time::Duration::from_millis(200), ring.recv())
        .await
        .expect("recv on a closed+drained ring must not hang");
    assert_eq!(end, None);
}

#[tokio::test]
async fn close_drain_delivers_a_just_published_terminal_event() {
    let bus = new_bus();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<EventEnvelope>(8);
    let _ = bus.subscribe(
        EventType::ClientClosedEvent,
        Arc::new(move |env| {
            let _ = tx.try_send(env.clone());
        }),
        1,
    );
    bus.publish(EventEnvelope {
        event_type: EventType::ClientClosedEvent,
        event_version: 1,
        timestamp_monotonic: crate::types::MonotonicInstant::now(),
        request_id: None,
        payload: EventPayload::ClientClosedEvent {
            reason: ClientCloseReason::UserClose,
            initiated_at_monotonic: crate::types::MonotonicInstant::now(),
        },
    });
    bus.close_dispatch_and_drain().await;
    // Await returns only after the loop drained and exited — no post-await race.
    let delivered = rx
        .try_recv()
        .expect("terminal event was dropped on close instead of drained");
    assert_eq!(delivered.event_type, EventType::ClientClosedEvent);
}

fn client_ready_env(rid: u64) -> EventEnvelope {
    EventEnvelope {
        event_type: EventType::ClientReady,
        event_version: 1,
        timestamp_monotonic: crate::types::MonotonicInstant::now(),
        request_id: Some(rid),
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
async fn poisoned_registry_locks_recover_without_panicking() {
    let bus = new_bus();

    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = bus.subscribers.write().unwrap();
        panic!("poison subscribers");
    }));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let handle = bus.subscribe(
        EventType::ClientReady,
        Arc::new(move |env| {
            let _ = tx.try_send(env.clone());
        }),
        1,
    );
    assert_eq!(bus.subscriber_count(EventType::ClientReady), 1);
    bus.publish(client_ready_envelope());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv())
            .await
            .expect("subscriber delivery timed out")
            .is_some()
    );
    bus.unsubscribe(handle);

    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = bus.correlated.lock().unwrap();
        panic!("poison correlated");
    }));
    let (corr_tx, mut corr_rx) = tokio::sync::mpsc::channel(1);
    let handle = bus.subscribe_correlated(
        EventType::ClientReady,
        7,
        Box::new(move |env| {
            let _ = corr_tx.try_send(env.clone());
            true
        }),
    );
    bus.deliver_correlated(&client_ready_env(7));
    let corr = tokio::time::timeout(std::time::Duration::from_millis(200), corr_rx.recv())
        .await
        .expect("correlated delivery timed out")
        .expect("correlated callback did not fire");
    assert_eq!(corr.request_id, Some(7));
    bus.unsubscribe(handle);
    bus.stop_reactors();
}

#[test]
fn correlated_miss_buffer_drops_oldest_on_overflow() {
    let mut reg = CorrelatedRegistry::new(3);
    for rid in 0..5u64 {
        reg.latch_missed((EventType::ClientReady, rid), client_ready_env(rid));
    }
    assert!(reg.take_missed((EventType::ClientReady, 0)).is_none());
    assert!(reg.take_missed((EventType::ClientReady, 1)).is_none());
    assert!(reg.take_missed((EventType::ClientReady, 2)).is_some());
    assert!(reg.take_missed((EventType::ClientReady, 3)).is_some());
    assert!(reg.take_missed((EventType::ClientReady, 4)).is_some());
}

#[test]
fn correlated_miss_buffer_take_consumes_once_and_keys_by_type() {
    let mut reg = CorrelatedRegistry::new(8);
    reg.latch_missed((EventType::ClientReady, 7), client_ready_env(7));
    assert!(reg.take_missed((EventType::ClientReady, 7)).is_some());
    assert!(reg.take_missed((EventType::ClientReady, 7)).is_none());
    reg.latch_missed((EventType::ClientReady, 9), client_ready_env(9));
    assert!(reg.take_missed((EventType::ClientClosedEvent, 9)).is_none());
    assert!(reg.take_missed((EventType::ClientReady, 9)).is_some());
}

/// Block until the reactor latches `(ClientReady, rid)` into the miss-buffer.
async fn wait_until_latched(bus: &Arc<DispatchEventBus>, rid: u64) {
    let poll = async {
        loop {
            {
                let reg = bus.correlated.lock().expect("correlated lock");
                if reg
                    .missed
                    .iter()
                    .any(|((t, k), _)| *t == EventType::ClientReady && *k == rid)
                {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), poll)
        .await
        .expect("timeout: event was never latched into the miss-buffer");
}

#[tokio::test]
async fn late_subscribe_correlated_resolves_from_latched_completion() {
    let bus = new_bus();
    bus.publish(client_ready_env(42));
    wait_until_latched(&bus, 42).await;

    let handle = crate::types::RequestHandle {
        id: 42,
        expected_completion: crate::types::ExpectedCompletionEvent::ClientReady,
    };
    let env = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        crate::api::await_request_handle(&bus, handle),
    )
    .await
    .expect("late subscriber must resolve from the latch")
    .expect("await resolved with an event");
    assert_eq!(env.request_id, Some(42));
    assert_eq!(env.event_type, EventType::ClientReady);
    bus.stop_reactors();
}

#[tokio::test]
async fn latched_completion_is_consumed_exactly_once() {
    let bus = new_bus();
    bus.publish(client_ready_env(55));
    wait_until_latched(&bus, 55).await;

    let h1 = crate::types::RequestHandle {
        id: 55,
        expected_completion: crate::types::ExpectedCompletionEvent::ClientReady,
    };
    let env = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        crate::api::await_request_handle(&bus, h1),
    )
    .await
    .expect("first late subscriber resolves from the latch")
    .expect("await resolved with an event");
    assert_eq!(env.request_id, Some(55));

    let h2 = crate::types::RequestHandle {
        id: 55,
        expected_completion: crate::types::ExpectedCompletionEvent::ClientReady,
    };
    let second = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        crate::api::await_request_handle(&bus, h2),
    )
    .await;
    assert!(
        second.is_err(),
        "latch must be consumed exactly once — the second await must not resolve"
    );
    bus.stop_reactors();
}

#[tokio::test]
async fn deliver_correlated_resolves_close_completion_without_ring_drain() {
    let bus = new_bus();
    let env = EventEnvelope {
        event_type: EventType::ClientClosedEvent,
        event_version: 1,
        timestamp_monotonic: crate::types::MonotonicInstant::now(),
        request_id: Some(7),
        payload: EventPayload::ClientClosedEvent {
            reason: ClientCloseReason::UserClose,
            initiated_at_monotonic: crate::types::MonotonicInstant::now(),
        },
    };
    bus.deliver_correlated(&env);
    let handle = crate::types::RequestHandle {
        id: 7,
        expected_completion: crate::types::ExpectedCompletionEvent::ClientClosed,
    };
    let resolved = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        crate::api::await_request_handle(&bus, handle),
    )
    .await
    .expect("close() completion must resolve from the direct correlated delivery")
    .expect("await resolved with an event");
    assert_eq!(resolved.request_id, Some(7));
    assert_eq!(resolved.event_type, EventType::ClientClosedEvent);
    bus.stop_reactors();
}

/// Registry death state — axis for correlated delivery rules.
#[derive(Clone, Copy, Debug)]
enum DeathState {
    Alive,
    /// Intended teardown: recorded, but already-latched misses are KEPT.
    Teardown,
    /// A real crash: recorded, and the miss-buffer purged.
    Genuine,
}

fn bus_in(state: DeathState) -> Arc<DispatchEventBus> {
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::default(),
        Arc::new(SystemClock),
    ));
    match state {
        DeathState::Alive => {}
        DeathState::Teardown => {
            bus.begin_shutdown();
            bus.on_loop_death(ReactorName::Io, LoopFailureCause::Cancelled);
        }
        DeathState::Genuine => bus.on_loop_death(ReactorName::Io, LoopFailureCause::Panic),
    }
    bus
}

/// Correlated delivery matrix vs death state / waiters / terminal shape.
#[test]
fn correlated_delivery_matrix() {
    use DeathState::{Alive, Genuine, Teardown};
    let cases = [
        (Alive, 0, false, 0, 1),
        (Alive, 1, false, 1, 0),
        (Alive, 2, false, 2, 0),
        // Past a committed death an ordinary terminal is dropped — neither
        // delivered nor latched.
        (Teardown, 0, false, 0, 0),
        (Teardown, 1, false, 0, 0),
        (Genuine, 0, false, 0, 0),
        (Genuine, 1, false, 0, 0),
        // The incident's own terminal still gets through.
        (Teardown, 0, true, 0, 1),
        (Teardown, 1, true, 1, 0),
        (Genuine, 1, true, 1, 0),
    ];

    for (death, waiters, incident, want_fired, want_latched) in cases {
        let label = format!("{death:?}/{waiters} waiters/incident={incident}");
        let bus = bus_in(death);
        let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let event_type = if incident {
            EventType::ClientFailed
        } else {
            EventType::ClientClosedEvent
        };
        let subs: Vec<_> = (0..waiters)
            .map(|_| {
                let hits = Arc::clone(&fired);
                bus.subscribe_correlated(
                    event_type,
                    1,
                    Box::new(move |_env| {
                        hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        true
                    }),
                )
            })
            .collect();
        assert_eq!(subs.len(), waiters, "{label}: arms registered");

        let env = if incident {
            EventEnvelope {
                event_type: EventType::ClientFailed,
                event_version: 1,
                timestamp_monotonic: crate::types::MonotonicInstant::now(),
                request_id: Some(1),
                payload: EventPayload::ClientFailed {
                    cause: crate::dispatch::ClientFailureCause::LoopFailed,
                },
            }
        } else {
            closed_env(1)
        };
        bus.deliver_correlated(&env);

        assert_eq!(
            fired.load(std::sync::atomic::Ordering::Relaxed),
            want_fired,
            "{label}: waiters fired"
        );
        assert_eq!(
            bus.correlated_missed_count(),
            want_latched,
            "{label}: entries latched"
        );
    }
}

/// Hand-back matrix: returned terminal reaches an armed waiter; genuine death refuses it.
#[test]
fn relatch_hand_back_matrix() {
    use DeathState::{Alive, Genuine, Teardown};

    let cases = [
        (Alive, false, 0, 1),
        (Alive, true, 1, 0),
        (Teardown, false, 0, 1),
        (Teardown, true, 1, 0),
        (Genuine, false, 0, 0),
        (Genuine, true, 0, 0),
    ];

    for (death, armed, want_fired, want_latched) in cases {
        let label = format!("{death:?}/armed={armed}");
        let bus = bus_in(death);
        let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let _sub = armed.then(|| {
            let hits = Arc::clone(&fired);
            bus.subscribe_correlated(
                EventType::ClientClosedEvent,
                3,
                Box::new(move |_env| {
                    hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    true
                }),
            )
        });

        bus.relatch_correlated(&closed_env(3));

        assert_eq!(
            fired.load(std::sync::atomic::Ordering::Relaxed),
            want_fired,
            "{label}: waiters fired"
        );
        assert_eq!(
            bus.correlated_missed_count(),
            want_latched,
            "{label}: entries latched"
        );
    }
}

/// Removing a waiter for delivery is not delivering: a decline in the gap must
/// re-latch the terminal, or a later observer hangs.
#[test]
fn a_declined_delivery_is_latched_not_lost() {
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::default(),
        Arc::new(SystemClock),
    ));

    let _sub = bus.subscribe_correlated(
        EventType::ClientClosedEvent,
        11,
        Box::new(move |_env| false),
    );
    assert_eq!(bus.correlated_live_count(), 1, "the waiter is armed");

    bus.deliver_correlated(&closed_env(11));

    assert_eq!(
        bus.correlated_missed_count(),
        1,
        "a declined terminal must stay claimable by a later observer"
    );
    assert!(
        arm_and_report_fired(&bus, EventType::ClientClosedEvent, 11),
        "and a later observer must actually get it"
    );
}

#[test]
fn a_sibling_acceptance_discharges_the_envelope() {
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::default(),
        Arc::new(SystemClock),
    ));
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hits = Arc::clone(&accepted);
    let _ok = bus.subscribe_correlated(
        EventType::ClientClosedEvent,
        17,
        Box::new(move |_env| {
            hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            true
        }),
    );
    let _decline = bus.subscribe_correlated(
        EventType::ClientClosedEvent,
        17,
        Box::new(move |_env| false),
    );

    bus.deliver_correlated(&closed_env(17));

    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the accepting waiter must fire"
    );
    assert_eq!(
        bus.correlated_missed_count(),
        0,
        "a terminal a waiter accepted is delivered, not latched for replay"
    );
    assert!(
        !arm_and_report_fired(&bus, EventType::ClientClosedEvent, 17),
        "a later arm must not be handed a terminal that was already claimed"
    );
}

/// Miss-buffer arm path must honour accept/decline like deliver: decline re-latches.
#[test]
fn a_declined_miss_fire_is_latched_not_lost() {
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::default(),
        Arc::new(SystemClock),
    ));

    bus.deliver_correlated(&closed_env(23));
    assert_eq!(bus.correlated_missed_count(), 1);

    let _sub = bus.subscribe_correlated(
        EventType::ClientClosedEvent,
        23,
        Box::new(move |_env| false),
    );
    assert_eq!(
        bus.correlated_missed_count(),
        1,
        "a declined miss-fire must leave the terminal latched"
    );
    assert!(
        arm_and_report_fired(&bus, EventType::ClientClosedEvent, 23),
        "a later accepting observer must still get it"
    );
}

fn closed_env(id: u64) -> EventEnvelope {
    EventEnvelope {
        event_type: EventType::ClientClosedEvent,
        event_version: 1,
        timestamp_monotonic: crate::types::MonotonicInstant::now(),
        request_id: Some(id),
        payload: EventPayload::ClientClosedEvent {
            reason: ClientCloseReason::UserClose,
            initiated_at_monotonic: crate::types::MonotonicInstant::now(),
        },
    }
}

/// Arm a one-shot for `(event_type, id)`; returns whether a latched miss fired it.
fn arm_and_report_fired(bus: &DispatchEventBus, event_type: EventType, id: u64) -> bool {
    let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&fired);
    let _sub = bus.subscribe_correlated(
        event_type,
        id,
        Box::new(move |_env| {
            flag.store(true, std::sync::atomic::Ordering::Relaxed);
            true
        }),
    );
    fired.load(std::sync::atomic::Ordering::Relaxed)
}

/// A terminal produced behind a death never resolves a correlated await.
#[tokio::test]
async fn a_terminal_behind_a_death_never_resolves_the_await() {
    for (mid_close, id) in [(false, 11), (true, 77)] {
        let bus = new_bus();
        if mid_close {
            bus.begin_shutdown();
        }
        bus.on_loop_death(ReactorName::Dispatch, LoopFailureCause::Panic);
        bus.deliver_correlated(&closed_env(id));
        let handle = crate::types::RequestHandle {
            id,
            expected_completion: crate::types::ExpectedCompletionEvent::ClientClosed,
        };
        let res = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            crate::api::await_request_handle(&bus, handle),
        )
        .await
        .expect("an arm after a death must not hang");
        assert!(
            matches!(res, Err(crate::api::AwaitError::LoopDead)),
            "a terminal behind the death must not report a clean completion (mid_close={mid_close})"
        );
        bus.stop_reactors();
    }
}

/// Two observers of one operation both resolve; a displaced waiter would hang.
#[tokio::test]
async fn every_waiter_on_a_handle_resolves() {
    let bus = new_bus();
    let handle = crate::types::RequestHandle {
        id: 42,
        expected_completion: crate::types::ExpectedCompletionEvent::ClientClosed,
    };
    let (b1, b2) = (Arc::clone(&bus), Arc::clone(&bus));
    let first = tokio::spawn(async move { crate::api::await_request_handle(&b1, handle).await });
    let second = tokio::spawn(async move { crate::api::await_request_handle(&b2, handle).await });
    // Both arms registered before the terminal lands. If one displaces the other,
    // the count never reaches 2 — fail, don't spin.
    for _ in 0..1_000 {
        if bus.correlated_live_count() >= 2 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        bus.correlated_live_count(),
        2,
        "both arms must coexist; a displaced waiter can never resolve"
    );
    bus.deliver_correlated(&closed_env(42));

    for (label, task) in [("first", first), ("second", second)] {
        let env = tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap_or_else(|_| panic!("{label} observer never resolved"))
            .expect("task panicked")
            .expect("await resolved with an event");
        assert_eq!(env.event_type, EventType::ClientClosedEvent);
    }
    bus.stop_reactors();
}

/// The incident's own terminal shapes stay deliverable past the commit —
/// `ready()` re-entry after a death resolves on a synthesized `ClientFailed`.
#[test]
fn the_incident_terminal_still_reaches_a_waiter_past_the_commit() {
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::default(),
        Arc::new(SystemClock),
    ));
    bus.on_loop_death(ReactorName::Io, LoopFailureCause::Panic);
    bus.deliver_correlated(&EventEnvelope {
        event_type: EventType::ClientFailed,
        event_version: 1,
        timestamp_monotonic: crate::types::MonotonicInstant::now(),
        request_id: Some(5),
        payload: EventPayload::ClientFailed {
            cause: crate::dispatch::ClientFailureCause::LoopFailed,
        },
    });
    assert!(
        arm_and_report_fired(&bus, EventType::ClientFailed, 5),
        "the death terminal must still latch and fire past the commit"
    );
}

/// Death commits to the registry before it is observable: pre-death latches
/// do not survive; post-death completions are dropped, not latched.
#[test]
fn a_genuine_death_commits_the_registry_in_both_directions() {
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::default(),
        Arc::new(SystemClock),
    ));
    bus.deliver_correlated(&closed_env(2)); // latched pre-death
    bus.on_loop_death(ReactorName::Io, LoopFailureCause::Panic);
    bus.deliver_correlated(&closed_env(3)); // produced post-death

    assert!(
        bus.loop_death_observed(),
        "the death must be observable once it has committed"
    );
    assert!(
        !arm_and_report_fired(&bus, EventType::ClientClosedEvent, 2),
        "a completion latched before the death must not outlive it"
    );
    assert!(
        !arm_and_report_fired(&bus, EventType::ClientClosedEvent, 3),
        "a completion produced after the death must not be latched for replay"
    );
}

#[test]
fn loop_death_guard_latches_loopdead_on_armed_drop() {
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::default(),
        Arc::new(SystemClock),
    ));
    {
        let _g = LoopDeathGuard::new(Arc::downgrade(&bus), ReactorName::Io);
    }
    assert!(
        bus.is_loop_failed(),
        "armed guard drop must latch the terminal LoopDead state"
    );
}

#[test]
fn loop_death_guard_disarmed_is_noop() {
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::default(),
        Arc::new(SystemClock),
    ));
    {
        let mut g = LoopDeathGuard::new(Arc::downgrade(&bus), ReactorName::Dispatch);
        g.disarm();
    }
    assert!(
        !bus.is_loop_failed(),
        "a disarmed (clean-exit) guard must not latch LoopDead"
    );
}

#[tokio::test]
async fn on_loop_death_fails_inflight_correlated_await() {
    let bus = new_bus();
    let handle = crate::types::RequestHandle {
        id: 99,
        expected_completion: crate::types::ExpectedCompletionEvent::ClientReady,
    };
    let bus2 = Arc::clone(&bus);
    let fut = tokio::spawn(async move { crate::api::await_request_handle(&bus2, handle).await });
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    bus.on_loop_death(ReactorName::Dispatch, LoopFailureCause::Panic);

    let env = tokio::time::timeout(std::time::Duration::from_millis(200), fut)
        .await
        .expect("in-flight await HUNG across loop death — the drain is not wired")
        .expect("task panicked")
        .expect("await resolved with an event");
    // ready() dual-arm resolves via ClientFailed (loop-death cause), not LoopFailedEvent.
    assert_eq!(env.event_type, EventType::ClientFailed);
    assert_eq!(env.request_id, Some(99));
    match env.payload {
        EventPayload::ClientFailed { cause } => {
            assert_eq!(cause, crate::dispatch::ClientFailureCause::LoopFailed);
        }
        other => panic!("expected ClientFailed{{LoopFailed}} payload, got {other:?}"),
    }
    assert!(bus.is_loop_failed());
    assert_eq!(
        bus.dispatch_reactor_state(),
        ReactorState::Failed(LoopFailureCause::Panic)
    );
}

#[test]
fn on_loop_death_after_begin_shutdown_neither_latches_nor_broadcasts() {
    // begin_shutdown() before abort suppresses LoopDead latch and bus-wide broadcast;
    // in-flight waiters still resolve.
    let bus = Arc::new(DispatchEventBus::new(
        DispatchEventBusConfig::default(),
        Arc::new(SystemClock),
    ));
    bus.begin_shutdown();
    bus.on_loop_death(ReactorName::Io, LoopFailureCause::Panic);
    assert!(
        !bus.is_loop_failed(),
        "a death during intended teardown must not latch loop_failed"
    );
    assert_eq!(
        bus.io_to_dispatch.depth(),
        0,
        "a death during intended teardown must not broadcast a LoopFailedEvent"
    );
}

/// Display must render the documented wire discriminators.
#[test]
fn queue_and_reactor_names_strum_display_matches_wire_strings() {
    use super::{QueueName, ReactorName};
    assert_eq!(QueueName::CallerToIo.to_string(), "caller_to_io");
    assert_eq!(QueueName::IoToDispatch.to_string(), "io_to_dispatch");
    assert_eq!(ReactorName::Io.to_string(), "io");
    assert_eq!(ReactorName::Dispatch.to_string(), "dispatch");
}

/// Display must render the WS v2 `method` string; `from_method` must round-trip it.
#[test]
fn ws_op_strum_display_round_trips_method_strings() {
    use super::WsOp;
    for (op, method) in [
        (WsOp::AddOrder, "add_order"),
        (WsOp::AmendOrder, "amend_order"),
        (WsOp::CancelOrder, "cancel_order"),
        (WsOp::CancelAll, "cancel_all"),
        (WsOp::CancelAllOrdersAfter, "cancel_all_orders_after"),
        (WsOp::BatchAdd, "batch_add"),
    ] {
        assert_eq!(op.to_string(), method);
        assert_eq!(WsOp::from_method(method), Some(op));
    }
}
