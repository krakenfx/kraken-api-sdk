//! Driveable mock `WsSocket` for I/O Reactor tests: script inbound, capture
//! outbound, fresh socket per `open_socket` (reconnect gets a new one).

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
        let mode = *self
            .send_fail_mode
            .lock()
            .expect("send_fail_mode lock poisoned");
        match mode {
            SendFailMode::Ok => {}
            SendFailMode::Backpressure => return Err(SendFrameError::Backpressure),
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
    next_send_fail_mode: Mutex<SendFailMode>,
}

impl DriveableWsSocketFactory {
    /// Construct a factory wired to `bus`.
    pub fn new(bus: Arc<DispatchEventBus>) -> Self {
        Self {
            bus,
            next_connection_id: AtomicU64::new(1),
            handles: Mutex::new(Vec::new()),
            next_send_fail_mode: Mutex::new(SendFailMode::Ok),
        }
    }

    /// Send-fail mode inherited by sockets opened after this call.
    pub fn set_next_send_fail_mode(&self, mode: SendFailMode) {
        *self
            .next_send_fail_mode
            .lock()
            .expect("next_send_fail_mode lock poisoned") = mode;
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
            .push(handle);

        self.bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::WsUpgradeOk,
            event_version: 1,
            timestamp_monotonic: self.bus.clock().now(),
            request_id: Some(connection_id),
            payload: crate::dispatch::EventPayload::WsUpgradeOk { connection_id, url },
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
