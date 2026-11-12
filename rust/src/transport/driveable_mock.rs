//! Driveable mock `WsSocket` for I/O Reactor tests: script inbound, capture
//! outbound, fresh socket per `open_socket` (reconnect gets a new one).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::{Mutex as TokioMutex, mpsc};

use crate::dispatch::DispatchEventBus;
use crate::transport::{SendFrameError, TransportError, WsFrame, WsSocket, WsSocketFactoryLike};
use crate::types::WsUrl;

/// One scripted inbound event for a [`DriveableWsSocket`].
#[derive(Debug)]
pub enum MockRecvItem {
    /// Emit from `recv_frame`.
    Frame(WsFrame),
    /// `recv_frame` resolves with this error (socket "drops").
    Drop(TransportError),
}

const MOCK_INBOX_CAPACITY: usize = 256;

/// How `send_frame` behaves when simulating a local send failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SendFailMode {
    /// Capture the frame normally.
    #[default]
    Ok,
    /// Return `Backpressure`.
    Backpressure,
    /// Return `Backpressure` for the next send, then behave as `Ok`.
    BackpressureOnce,
    /// Return `WriterClosed`.
    WriterClosed,
}

/// Driveable socket: test holds [`DriveableSocketHandle`]; reactor holds `Arc<dyn WsSocket>`.
pub struct DriveableWsSocket {
    connection_id: u64,
    inbox_rx: TokioMutex<mpsc::Receiver<MockRecvItem>>,
    sent: Arc<Mutex<Vec<WsFrame>>>,
    send_fail_mode: Arc<Mutex<SendFailMode>>,
    send_attempts: Arc<AtomicUsize>,
}

/// Test-side handle: push inbound, snapshot outbound, set send-fail mode.
#[derive(Clone)]
pub struct DriveableSocketHandle {
    inbox_tx: mpsc::Sender<MockRecvItem>,
    sent: Arc<Mutex<Vec<WsFrame>>>,
    send_fail_mode: Arc<Mutex<SendFailMode>>,
    send_attempts: Arc<AtomicUsize>,
}

impl DriveableSocketHandle {
    /// Push a frame onto the inbound stream.
    pub fn emit_frame(&self, frame: WsFrame) {
        self.push(MockRecvItem::Frame(frame));
    }

    /// Push a text frame.
    pub fn emit_text(&self, body: impl Into<String>) {
        self.emit_frame(WsFrame {
            opcode: crate::transport::WsOpcode::Text,
            payload: body.into().into_bytes(),
        });
    }

    /// Make `recv_frame` resolve with `err`.
    pub fn drop_with(&self, err: TransportError) {
        self.push(MockRecvItem::Drop(err));
    }

    /// Enqueue; panics on Full rather than silently dropping.
    fn push(&self, item: MockRecvItem) {
        if let Err(mpsc::error::TrySendError::Full(_)) = self.inbox_tx.try_send(item) {
            panic!(
                "driveable mock inbox full (capacity {MOCK_INBOX_CAPACITY}) — raise MOCK_INBOX_CAPACITY"
            );
        }
    }

    /// Captured outbound text bodies, in send order.
    pub fn sent_text(&self) -> Vec<String> {
        self.sent
            .lock()
            .expect("sent capture lock poisoned")
            .iter()
            .map(|f| String::from_utf8_lossy(&f.payload).into_owned())
            .collect()
    }

    /// Set the send-fail mode for subsequent `send_frame` calls.
    pub fn set_send_fail_mode(&self, mode: SendFailMode) {
        *self
            .send_fail_mode
            .lock()
            .expect("send_fail_mode lock poisoned") = mode;
    }

    /// Total `send_frame` calls, including fail-knob rejects.
    pub fn send_attempts(&self) -> usize {
        self.send_attempts.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl WsSocket for DriveableWsSocket {
    fn connection_id(&self) -> u64 {
        self.connection_id
    }

    fn send_frame(&self, frame: WsFrame) -> Result<(), SendFrameError> {
        self.send_attempts.fetch_add(1, Ordering::Relaxed);
        let mode = {
            let mut guard = self
                .send_fail_mode
                .lock()
                .expect("send_fail_mode lock poisoned");
            let mode = *guard;
            if mode == SendFailMode::BackpressureOnce {
                *guard = SendFailMode::Ok;
            }
            mode
        };
        match mode {
            SendFailMode::Ok => {}
            SendFailMode::Backpressure | SendFailMode::BackpressureOnce => {
                return Err(SendFrameError::Backpressure);
            }
            SendFailMode::WriterClosed => return Err(SendFrameError::WriterClosed),
        }
        self.sent
            .lock()
            .expect("sent capture lock poisoned")
            .push(frame);
        Ok(())
    }

    async fn recv_frame(&self) -> Result<WsFrame, TransportError> {
        let mut guard = self.inbox_rx.lock().await;
        match guard.recv().await {
            Some(MockRecvItem::Frame(f)) => Ok(f),
            Some(MockRecvItem::Drop(e)) => Err(e),
            // Script exhausted: park forever (no spurious drop / busy-loop).
            None => {
                std::future::pending::<()>().await;
                unreachable!("DriveableWsSocket inbox closed but pending() returned")
            }
        }
    }

    fn close(&self, _code: u16) {}
}

/// Fresh [`DriveableWsSocket`] per `open_socket`; publishes `WsUpgradeOk`.
pub struct DriveableWsSocketFactory {
    bus: Arc<DispatchEventBus>,
    next_connection_id: AtomicU64,
    handles: Mutex<Vec<DriveableSocketHandle>>,
    latest_handle_by_url: Mutex<HashMap<WsUrl, DriveableSocketHandle>>,
    next_send_fail_mode: Mutex<SendFailMode>,
    next_upgrade_failure: Mutex<Option<TransportError>>,
}

impl DriveableWsSocketFactory {
    /// Construct a factory wired to `bus`.
    pub fn new(bus: Arc<DispatchEventBus>) -> Self {
        Self {
            bus,
            next_connection_id: AtomicU64::new(1),
            handles: Mutex::new(Vec::new()),
            latest_handle_by_url: Mutex::new(HashMap::new()),
            next_send_fail_mode: Mutex::new(SendFailMode::Ok),
            next_upgrade_failure: Mutex::new(None),
        }
    }

    /// Send-fail mode inherited by sockets opened after this call.
    pub fn set_next_send_fail_mode(&self, mode: SendFailMode) {
        *self
            .next_send_fail_mode
            .lock()
            .expect("next_send_fail_mode lock poisoned") = mode;
    }

    /// Make the next opened socket publish this handshake failure instead of
    /// `WsUpgradeOk`.
    pub fn set_next_upgrade_failure(&self, error: TransportError) {
        *self
            .next_upgrade_failure
            .lock()
            .expect("next_upgrade_failure lock poisoned") = Some(error);
    }

    /// Number of sockets created so far.
    pub fn created_count(&self) -> usize {
        self.handles.lock().expect("handles lock poisoned").len()
    }

    /// Handle to the n-th created socket (0-based), if opened.
    pub fn handle(&self, index: usize) -> Option<DriveableSocketHandle> {
        self.handles
            .lock()
            .expect("handles lock poisoned")
            .get(index)
            .cloned()
    }

    /// Handle for the most recently opened socket at `url`.
    ///
    /// A reconnect replaces the previous handle for that URL. `handle(index)`
    /// remains available for tests that intentionally assert creation order.
    pub fn handle_for(&self, url: WsUrl) -> Option<DriveableSocketHandle> {
        self.latest_handle_by_url
            .lock()
            .expect("latest-handle map lock poisoned")
            .get(&url)
            .cloned()
    }
}

impl WsSocketFactoryLike for DriveableWsSocketFactory {
    fn open_socket(&self, url: WsUrl) -> Arc<dyn WsSocket> {
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let (inbox_tx, inbox_rx) = mpsc::channel::<MockRecvItem>(MOCK_INBOX_CAPACITY);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let send_fail_mode = Arc::new(Mutex::new(
            *self
                .next_send_fail_mode
                .lock()
                .expect("next_send_fail_mode lock poisoned"),
        ));
        let send_attempts = Arc::new(AtomicUsize::new(0));

        let handle = DriveableSocketHandle {
            inbox_tx,
            sent: Arc::clone(&sent),
            send_fail_mode: Arc::clone(&send_fail_mode),
            send_attempts: Arc::clone(&send_attempts),
        };
        self.handles
            .lock()
            .expect("handles lock poisoned")
            .push(handle.clone());
        self.latest_handle_by_url
            .lock()
            .expect("latest-handle map lock poisoned")
            .insert(url, handle);

        let upgrade_failure = self
            .next_upgrade_failure
            .lock()
            .expect("next_upgrade_failure lock poisoned")
            .take();
        let (event_type, payload) = match upgrade_failure {
            Some(error) => (
                crate::dispatch::EventType::WsUpgradeFailed,
                crate::dispatch::EventPayload::WsUpgradeFailed {
                    connection_id,
                    url,
                    error,
                },
            ),
            None => (
                crate::dispatch::EventType::WsUpgradeOk,
                crate::dispatch::EventPayload::WsUpgradeOk { connection_id, url },
            ),
        };
        self.bus.publish(crate::dispatch::EventEnvelope {
            event_type,
            event_version: 1,
            timestamp_monotonic: self.bus.clock().now(),
            request_id: Some(connection_id),
            payload,
        });

        Arc::new(DriveableWsSocket {
            connection_id,
            inbox_rx: TokioMutex::new(inbox_rx),
            sent,
            send_fail_mode,
            send_attempts,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::clock::{Clock, SystemClock};
    use crate::dispatch::DispatchEventBusConfig;
    use crate::transport::WsSocketFactoryLike;

    use super::DriveableWsSocketFactory;

    fn factory() -> DriveableWsSocketFactory {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            clock,
        ));
        DriveableWsSocketFactory::new(bus)
    }

    #[test]
    fn handle_for_returns_none_before_the_url_is_opened() {
        let factory = factory();

        assert!(factory.handle_for(crate::types::WsUrl::Public).is_none());
    }

    #[tokio::test]
    async fn handle_for_returns_the_latest_socket_after_a_reconnect() {
        let factory = factory();
        let _first = factory.open_socket(crate::types::WsUrl::Public);
        let latest = factory.open_socket(crate::types::WsUrl::Public);

        factory
            .handle_for(crate::types::WsUrl::Public)
            .expect("latest public handle")
            .emit_text("latest public socket");

        let frame = latest
            .recv_frame()
            .await
            .expect("frame sent to latest socket");

        assert_eq!(frame.payload, b"latest public socket");
    }
}
