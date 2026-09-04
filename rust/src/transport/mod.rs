//! Transport layer — `HttpTransport` (REST) and `WsSocket` traits plus
//! production impls, and `WsSocketFactory` for opening sockets per `WsUrl`.

pub mod http;
pub mod ws;

/// Driveable mock socket + factory for I/O Reactor integration tests
/// (distinct from [`MockWsSocketFactory`] below).
#[cfg(test)]
pub mod driveable_mock;

pub use http::{HttpTransport, ReqwestHttpTransport};
pub use ws::{TokioTungsteniteWsSocket, WsSocket};

use std::sync::Arc;

use crate::types::WsUrl;

/// Factory for opening fresh `WsSocket` connections. Production spawns a tokio
/// task for the TLS+HTTP upgrade; tests inject a mock. Holds the bus strongly.
pub trait WsSocketFactoryLike: Send + Sync {
    fn open_socket(&self, url: WsUrl) -> Arc<dyn WsSocket>;
}

pub struct WsSocketFactory {
    bus: Arc<crate::dispatch::DispatchEventBus>,
    next_connection_id: std::sync::atomic::AtomicU64,
    /// Public WS URL from the `ws_public_url` knob at `.build()`.
    public_url: String,
    /// Auth WS URL from the `ws_auth_url` knob.
    auth_url: String,
}

impl WsSocketFactoryLike for WsSocketFactory {
    fn open_socket(&self, url: WsUrl) -> Arc<dyn WsSocket> {
        WsSocketFactory::open_socket(self, url)
    }
}

/// Test-only factory that produces no-op sockets without touching the network.
#[cfg(test)]
pub struct MockWsSocketFactory {
    next_connection_id: std::sync::atomic::AtomicU64,
}

#[cfg(test)]
impl MockWsSocketFactory {
    pub fn new() -> Self {
        Self {
            next_connection_id: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

#[cfg(test)]
impl Default for MockWsSocketFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl WsSocketFactoryLike for MockWsSocketFactory {
    fn open_socket(&self, _url: WsUrl) -> Arc<dyn WsSocket> {
        let id = self
            .next_connection_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Arc::new(MockWsSocket { connection_id: id })
    }
}

#[cfg(test)]
struct MockWsSocket {
    connection_id: u64,
}

#[cfg(test)]
#[async_trait::async_trait]
impl WsSocket for MockWsSocket {
    fn connection_id(&self) -> u64 {
        self.connection_id
    }
    fn send_frame(&self, _frame: WsFrame) -> Result<(), SendFrameError> {
        Ok(())
    }
    async fn recv_frame(&self) -> Result<WsFrame, TransportError> {
        std::future::pending::<()>().await;
        unreachable!("MockWsSocket::recv_frame never returns")
    }
    fn close(&self, _code: u16) {}
}

impl WsSocketFactory {
    /// Construct with resolved endpoint URLs from the WS URL knobs at `.build()`.
    pub fn new_with_urls(
        bus: Arc<crate::dispatch::DispatchEventBus>,
        public_url: String,
        auth_url: String,
    ) -> Self {
        Self {
            bus,
            next_connection_id: std::sync::atomic::AtomicU64::new(1),
            public_url,
            auth_url,
        }
    }

    /// Open a fresh socket and kick off the handshake, returning immediately;
    /// the bus receives `WsUpgradeOk` or `WsUpgradeFailed` on completion.
    pub fn open_socket(&self, url: WsUrl) -> Arc<dyn WsSocket> {
        let connection_id = self
            .next_connection_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let url_str = match url {
            WsUrl::Public => self.public_url.clone(),
            WsUrl::Auth => self.auth_url.clone(),
        };
        Arc::new(TokioTungsteniteWsSocket::open_and_spawn(
            url,
            url_str,
            connection_id,
            Arc::clone(&self.bus),
        ))
    }
}

/// Single WS frame received from or sent to the wire.
#[derive(Debug, Clone)]
pub struct WsFrame {
    pub opcode: WsOpcode,
    pub payload: Vec<u8>,
}

/// WS frame opcode per RFC 6455.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsOpcode {
    // WS protocol opcode; not constructed on the current send/recv path. Retained for protocol completeness.
    #[allow(dead_code)]
    Continuation,
    Text,
    Binary,
    // WS protocol opcode; not constructed on the current send/recv path. Retained for protocol completeness.
    #[allow(dead_code)]
    Close,
    Ping,
    Pong,
}

/// Transport-layer error.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct TransportError {
    /// Closed classification driving retry and ambiguity decisions.
    pub kind: TransportErrorKind,
    /// True when safe to retry with backoff; REST retry engine gates on this.
    pub transient: bool,
}

/// Closed classification of transport failures. Transient vs non-transient
/// follows the reconnect retry policy.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportErrorKind {
    /// TCP connect timed out before the socket opened; nothing was sent.
    TcpConnectTimeout,
    /// Peer refused the TCP connection; nothing was sent.
    TcpRefused,
    /// Hostname resolution failed before any connection was attempted.
    DnsFailure,
    /// TLS negotiation failed after TCP connected; no request reached the wire.
    TlsHandshakeFailure,
    /// Socket reset mid-stream; in-flight request outcome unknown.
    SocketReset,
    /// Request reached the wire but no response; outcome unknown (sent-ambiguous).
    RequestSentNoResponse,
    /// SSL-layer failure outside the handshake; non-transient.
    Ssl,
    /// Clean WS close from the peer. Surfaces from `recv_frame` as a typed error.
    CloseFrame {
        /// Close status code (RFC 6455; 4xxx Kraken-specific).
        code: u16,
        /// UTF-8 close reason; may be empty.
        reason: String,
    },
    /// Abnormal close — TCP reset, TLS error, or non-close I/O mid-stream.
    AbnormalClose {
        /// Human-readable description of the underlying I/O failure.
        context: String,
    },
    /// Peer rejected the WS HTTP upgrade with this status.
    HttpUpgradeRejected {
        /// HTTP status; classified transient vs permanent per status.
        status: u16,
    },
    /// Raw non-2xx REST response; transient stamped at construction from status.
    HttpStatus {
        /// HTTP status the REST call returned.
        status: u16,
    },
    /// Catch-all for unclassified failures.
    Other(String),
}

/// Which HTTP upgrade-rejection statuses are transient vs permanent.
pub(crate) fn http_upgrade_status_transient(status: u16) -> bool {
    matches!(status, 408 | 409 | 425 | 429 | 503 | 504 | 507)
}

impl TransportErrorKind {
    /// Request may have reached the wire — never blindly retry; order writes
    /// emit the ambiguous event.
    pub(crate) fn is_sent_ambiguous(&self) -> bool {
        use TransportErrorKind::*;
        matches!(
            self,
            RequestSentNoResponse | SocketReset | AbnormalClose { .. } | CloseFrame { .. }
        )
    }
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&crate::error::ApiError::message(self))
    }
}

impl std::error::Error for TransportError {}

impl crate::error::sealed::Sealed for TransportError {}

impl crate::error::ApiError for TransportError {
    fn code(&self) -> &str {
        "CONNECTION_ERROR"
    }
    fn category(&self) -> crate::error::ErrorCategory {
        crate::error::ErrorCategory::Network
    }
    fn retryable(&self) -> bool {
        // Idempotency-unknown and non-transient kinds are not auto-retryable.
        use TransportErrorKind::*;
        match &self.kind {
            RequestSentNoResponse | Ssl | Other(_) => false,
            HttpUpgradeRejected { status } => http_upgrade_status_transient(*status),
            // Defers to the per-status classification stamped at construction.
            HttpStatus { .. } => self.transient,
            _ => true,
        }
    }
    fn request_id(&self) -> Option<&str> {
        None
    }
    fn message(&self) -> String {
        use TransportErrorKind::*;
        match &self.kind {
            TcpConnectTimeout => "TCP connect timeout.".into(),
            TcpRefused => "TCP connection refused.".into(),
            DnsFailure => "DNS resolution failed.".into(),
            TlsHandshakeFailure => "TLS handshake failed.".into(),
            SocketReset => "Socket reset.".into(),
            RequestSentNoResponse => "Request sent; no response.".into(),
            Ssl => "SSL error.".into(),
            CloseFrame { code, reason } => format!("WS close {code}: {reason}."),
            AbnormalClose { context } => format!("WS abnormal close: {context}."),
            HttpUpgradeRejected { status } => format!("WS upgrade rejected: HTTP {status}."),
            HttpStatus { status } => format!("HTTP {status}."),
            Other(s) => format!("Transport: {s}"),
        }
    }
    fn kraken_code(&self) -> Option<&str> {
        None
    }
}

/// Send-frame failure: `Backpressure` (outbound full, writer alive) is
/// transient; `WriterClosed` (writer dead) drives reconnect-replay.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SendFrameError {
    /// Outbound mpsc full — writer alive but queue at capacity.
    #[error("WS outbound queue full (backpressure).")]
    Backpressure,
    /// Outbound sender closed — writer task dead.
    #[error("WS outbound writer task closed.")]
    WriterClosed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ApiError;

    #[test]
    fn upgrade_rejection_retryable_follows_status() {
        let err = |status: u16| TransportError {
            kind: TransportErrorKind::HttpUpgradeRejected { status },
            transient: http_upgrade_status_transient(status),
        };
        assert!(err(503).retryable(), "transient rejection retries");
        assert!(err(429).retryable(), "throttled rejection retries");
        assert!(!err(401).retryable(), "unauthorized is permanent");
        assert!(!err(403).retryable(), "forbidden is permanent");
    }

    #[test]
    fn http_status_retryable_follows_status() {
        // Constructed as the REST path does: transient from the shared classifier.
        let err = |status: u16| TransportError {
            kind: TransportErrorKind::HttpStatus { status },
            transient: super::http::is_transient_status(
                &reqwest::StatusCode::from_u16(status).unwrap(),
            ),
        };
        assert!(err(503).retryable(), "server error retries");
        assert!(err(429).retryable(), "throttled retries");
        assert!(err(408).retryable(), "request timeout retries");
        assert!(err(425).retryable(), "too-early retries");
        assert!(!err(400).retryable(), "bad request is permanent");
        assert!(!err(401).retryable(), "unauthorized is permanent");
    }

    #[test]
    fn display_is_the_message_verbatim() {
        let err = TransportError {
            kind: TransportErrorKind::HttpStatus { status: 503 },
            transient: true,
        };
        assert_eq!(err.to_string(), crate::error::ApiError::message(&err));
        let boxed: Box<dyn std::error::Error> = Box::new(err);
        assert_eq!(boxed.to_string(), "HTTP 503.");
    }
}
