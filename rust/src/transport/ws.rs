//! WS transport over `tokio-tungstenite`: sync non-blocking `send_frame` via a
//! writer task, async `recv_frame` via a reader task.

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as TokioMutex, mpsc};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderValue, header::USER_AGENT};
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use crate::dispatch::DispatchEventBus;
use crate::transport::http::{SDK_KORIGIN_HEADER, SDK_KORIGIN_WS, SDK_USER_AGENT};
use crate::transport::{SendFrameError, TransportError, TransportErrorKind, WsFrame, WsOpcode};
use crate::types::WsUrl;

/// WebSocket adapter. Called only from the I/O reactor (single-writer);
/// reader/writer tasks are reactor extensions.
#[async_trait]
pub trait WsSocket: Send + Sync {
    /// Factory-allocated connection id (bus event correlation).
    fn connection_id(&self) -> u64;

    /// Sync non-blocking push to the outbound queue.
    fn send_frame(&self, frame: WsFrame) -> Result<(), SendFrameError>;

    /// Async pull of the next inbound frame. Clean close → `CloseFrame` error.
    async fn recv_frame(&self) -> Result<WsFrame, TransportError>;

    /// Sync fire-and-forget close intent; reader emits `WsClosed` on peer ack/FIN.
    fn close(&self, code: u16);
}

type TungsteniteStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug)]
enum WsOutbound {
    Frame(WsFrame),
    Close { code: u16 },
}

const DEFAULT_OUTBOUND_CAPACITY: usize = 256;
const DEFAULT_INBOUND_CAPACITY: usize = 256;

pub struct TokioTungsteniteWsSocket {
    connection_id: u64,
    outbox_tx: mpsc::Sender<WsOutbound>,
    inbox_rx: TokioMutex<mpsc::Receiver<Result<WsFrame, TransportError>>>,
    /// Shared with the opener task. Drop aborts as a fast path; if a drop races
    /// connect, reader/writer still self-terminate when channels close.
    reader_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    writer_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl TokioTungsteniteWsSocket {
    /// Construct the socket, spawn the opener, return immediately.
    pub(crate) fn open_and_spawn(
        url: WsUrl,
        url_str: String,
        connection_id: u64,
        bus: Arc<DispatchEventBus>,
    ) -> Self {
        let (outbox_tx, outbox_rx) = mpsc::channel::<WsOutbound>(DEFAULT_OUTBOUND_CAPACITY);
        let (inbox_tx, inbox_rx) =
            mpsc::channel::<Result<WsFrame, TransportError>>(DEFAULT_INBOUND_CAPACITY);

        let reader_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>> =
            Arc::new(std::sync::Mutex::new(None));
        let writer_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>> =
            Arc::new(std::sync::Mutex::new(None));

        let reader_task_for_opener = Arc::clone(&reader_task);
        let writer_task_for_opener = Arc::clone(&writer_task);
        let bus_for_opener = Arc::clone(&bus);
        let ws_config = WebSocketConfig::default()
            .max_message_size(Some(4 * 1024 * 1024))
            .max_frame_size(Some(4 * 1024 * 1024));
        tokio::spawn(async move {
            // Bare User-Agent + WS x-korigin on the upgrade (no app compose).
            let connect_result = match url_str.as_str().into_client_request() {
                Err(e) => Err(e),
                Ok(mut request) => {
                    let headers = request.headers_mut();
                    headers.insert(USER_AGENT, HeaderValue::from_static(SDK_USER_AGENT));
                    headers.insert(SDK_KORIGIN_HEADER, HeaderValue::from_static(SDK_KORIGIN_WS));
                    let connect = connect_async_with_config(request, Some(ws_config), false);
                    tokio::pin!(connect);
                    tokio::select! {
                        // Socket dropped mid-connect: abandon (don't park forever).
                        _ = inbox_tx.closed() => {
                            tracing::debug!(
                                target: "kraken_sdk::ws_socket",
                                ?url, connection_id,
                                "socket dropped during connect; abandoning the attempt"
                            );
                            return;
                        }
                        r = &mut connect => r,
                    }
                }
            };
            match connect_result {
                Ok((stream, _resp)) => {
                    if inbox_tx.is_closed() {
                        // Dropped mid-connect: discard stream; nobody owns it.
                        tracing::debug!(
                            target: "kraken_sdk::ws_socket",
                            ?url, connection_id,
                            "socket dropped during connect; discarding upgraded stream"
                        );
                        return;
                    }
                    let (sink, rx_stream) = stream.split();
                    let reader_handle = tokio::spawn(reader_loop(
                        rx_stream,
                        inbox_tx,
                        Arc::clone(&bus_for_opener),
                        connection_id,
                        url,
                    ));
                    let writer_handle = tokio::spawn(writer_loop(sink, outbox_rx));
                    *reader_task_for_opener
                        .lock()
                        .expect("reader_task poisoned — fatal") = Some(reader_handle);
                    *writer_task_for_opener
                        .lock()
                        .expect("writer_task poisoned — fatal") = Some(writer_handle);
                    bus_for_opener.publish(crate::dispatch::EventEnvelope {
                        event_type: crate::dispatch::EventType::WsUpgradeOk,
                        event_version: 1,
                        timestamp_monotonic: bus_for_opener.clock().now(),
                        request_id: Some(connection_id),
                        payload: crate::dispatch::EventPayload::WsUpgradeOk { connection_id, url },
                    });
                    tracing::info!(
                        target: "kraken_sdk::ws_socket",
                        ?url, connection_id,
                        "WsUpgradeOk"
                    );
                }
                Err(e) => {
                    if inbox_tx.is_closed() {
                        tracing::debug!(
                            target: "kraken_sdk::ws_socket",
                            ?url, connection_id,
                            "socket dropped during connect; suppressing WsUpgradeFailed"
                        );
                        return;
                    }
                    let te = classify_tungstenite_error(e);
                    bus_for_opener.publish(crate::dispatch::EventEnvelope {
                        event_type: crate::dispatch::EventType::WsUpgradeFailed,
                        event_version: 1,
                        timestamp_monotonic: bus_for_opener.clock().now(),
                        request_id: Some(connection_id),
                        payload: crate::dispatch::EventPayload::WsUpgradeFailed {
                            connection_id,
                            url,
                            error: te.clone(),
                        },
                    });
                    tracing::warn!(
                        target: "kraken_sdk::ws_socket",
                        ?url, connection_id, error = ?te,
                        "WsUpgradeFailed"
                    );
                }
            }
        });

        Self {
            connection_id,
            outbox_tx,
            inbox_rx: TokioMutex::new(inbox_rx),
            reader_task,
            writer_task,
        }
    }
}

#[async_trait]
impl WsSocket for TokioTungsteniteWsSocket {
    fn connection_id(&self) -> u64 {
        self.connection_id
    }

    fn send_frame(&self, frame: WsFrame) -> Result<(), SendFrameError> {
        self.outbox_tx
            .try_send(WsOutbound::Frame(frame))
            .map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => SendFrameError::Backpressure,
                tokio::sync::mpsc::error::TrySendError::Closed(_) => SendFrameError::WriterClosed,
            })
    }

    async fn recv_frame(&self) -> Result<WsFrame, TransportError> {
        let mut guard = self.inbox_rx.lock().await;
        match guard.recv().await {
            Some(result) => result,
            None => Err(TransportError {
                kind: TransportErrorKind::AbnormalClose {
                    context: "reader task terminated".into(),
                },
                transient: false,
            }),
        }
    }

    fn close(&self, code: u16) {
        let _ = self.outbox_tx.try_send(WsOutbound::Close { code });
    }
}

impl Drop for TokioTungsteniteWsSocket {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.reader_task.lock() {
            if let Some(h) = guard.take() {
                h.abort();
            }
        }
        if let Ok(mut guard) = self.writer_task.lock() {
            if let Some(h) = guard.take() {
                h.abort();
            }
        }
    }
}

async fn reader_loop(
    mut stream: futures_util::stream::SplitStream<TungsteniteStream>,
    inbox_tx: mpsc::Sender<Result<WsFrame, TransportError>>,
    bus: Arc<DispatchEventBus>,
    connection_id: u64,
    url: WsUrl,
) {
    // Pin once: re-creating closed() per frame would re-register every message.
    let socket_dropped = inbox_tx.closed();
    tokio::pin!(socket_dropped);
    loop {
        let msg_result = tokio::select! {
            // Receiver closed ⇒ socket dropped; exit so the TCP/TLS connection dies.
            _ = &mut socket_dropped => return,
            next = stream.next() => match next {
                Some(r) => r,
                None => break,
            },
        };
        let payload: Result<WsFrame, TransportError> = match msg_result {
            Ok(Message::Close(cf)) => {
                let (code, reason) = match cf {
                    Some(CloseFrame { code, reason }) => (u16::from(code), reason.to_string()),
                    None => (1006u16, String::new()),
                };
                let _ = inbox_tx
                    .send(Err(TransportError {
                        kind: TransportErrorKind::CloseFrame {
                            code,
                            reason: reason.clone(),
                        },
                        transient: false,
                    }))
                    .await;
                bus.publish(crate::dispatch::EventEnvelope {
                    event_type: crate::dispatch::EventType::WsClosed,
                    event_version: 1,
                    timestamp_monotonic: bus.clock().now(),
                    // None: connection_id is in the payload; keep out of miss-buffer.
                    request_id: None,
                    payload: crate::dispatch::EventPayload::WsClosed {
                        connection_id,
                        url,
                        code,
                        reason: reason.clone(),
                    },
                });
                tracing::info!(
                    target: "kraken_sdk::ws_socket",
                    ?url, connection_id, code, %reason,
                    "WsClosed"
                );
                return;
            }
            Ok(Message::Text(s)) => Ok(WsFrame {
                opcode: WsOpcode::Text,
                payload: s.as_bytes().to_vec(),
            }),
            Ok(Message::Binary(b)) => Ok(WsFrame {
                opcode: WsOpcode::Binary,
                payload: b.into(),
            }),
            Ok(Message::Ping(b)) => Ok(WsFrame {
                opcode: WsOpcode::Ping,
                payload: b.into(),
            }),
            Ok(Message::Pong(b)) => Ok(WsFrame {
                opcode: WsOpcode::Pong,
                payload: b.into(),
            }),
            Ok(Message::Frame(_)) => continue,
            Err(e) => {
                let te = classify_tungstenite_error(e);
                let _ = inbox_tx.send(Err(te.clone())).await;
                bus.publish(crate::dispatch::EventEnvelope {
                    event_type: crate::dispatch::EventType::WsClosed,
                    event_version: 1,
                    timestamp_monotonic: bus.clock().now(),
                    request_id: None,
                    payload: crate::dispatch::EventPayload::WsClosed {
                        connection_id,
                        url,
                        code: 1006,
                        reason: format!("{:?}", te.kind),
                    },
                });
                tracing::warn!(
                    target: "kraken_sdk::ws_socket",
                    ?url, connection_id, error = ?te,
                    "WsClosed (abnormal)"
                );
                return;
            }
        };
        if inbox_tx.send(payload).await.is_err() {
            return;
        }
    }
    let _ = inbox_tx
        .send(Err(TransportError {
            kind: TransportErrorKind::AbnormalClose {
                context: "stream ended without close frame".into(),
            },
            transient: false,
        }))
        .await;
}

async fn writer_loop(
    mut sink: futures_util::stream::SplitSink<TungsteniteStream, Message>,
    mut outbox_rx: mpsc::Receiver<WsOutbound>,
) {
    while let Some(out) = outbox_rx.recv().await {
        match out {
            WsOutbound::Frame(frame) => {
                let msg = match frame.opcode {
                    WsOpcode::Text => match String::from_utf8(frame.payload) {
                        Ok(s) => Message::Text(s.into()),
                        Err(_) => continue,
                    },
                    WsOpcode::Binary => Message::Binary(frame.payload.into()),
                    WsOpcode::Ping => Message::Ping(frame.payload.into()),
                    WsOpcode::Pong => Message::Pong(frame.payload.into()),
                    WsOpcode::Close => Message::Close(None),
                    WsOpcode::Continuation => continue,
                };
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
            WsOutbound::Close { code } => {
                let cf = CloseFrame {
                    code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(
                        code,
                    ),
                    reason: Utf8Bytes::from_static(""),
                };
                let _ = sink.send(Message::Close(Some(cf))).await;
                break;
            }
        }
    }
}

fn classify_tungstenite_error(e: tokio_tungstenite::tungstenite::Error) -> TransportError {
    use tokio_tungstenite::tungstenite::Error as TError;
    // Connect-phase defaults transient; permanent HTTP rejections are terminal.
    let (kind, transient) = match &e {
        TError::ConnectionClosed | TError::AlreadyClosed => (
            TransportErrorKind::AbnormalClose {
                context: "connection closed".into(),
            },
            true,
        ),
        TError::Io(io_err) => {
            let kind = match io_err.kind() {
                std::io::ErrorKind::TimedOut => TransportErrorKind::TcpConnectTimeout,
                std::io::ErrorKind::ConnectionRefused => TransportErrorKind::TcpRefused,
                std::io::ErrorKind::ConnectionReset => TransportErrorKind::SocketReset,
                // No dedicated ErrorKind for getaddrinfo; DNS lands here.
                _ => TransportErrorKind::AbnormalClose {
                    context: format!("io: {}", io_err),
                },
            };
            (kind, true)
        }
        TError::Tls(_) => (TransportErrorKind::TlsHandshakeFailure, true),
        TError::Http(resp) => {
            let status = resp.status().as_u16();
            (
                TransportErrorKind::HttpUpgradeRejected { status },
                crate::transport::http_upgrade_status_transient(status),
            )
        }
        TError::Url(_) | TError::HttpFormat(_) => {
            (TransportErrorKind::Other(format!("{}", e)), false)
        }
        other => (TransportErrorKind::Other(format!("{}", other)), false),
    };
    TransportError { kind, transient }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::TokioTungsteniteWsSocket;
    use crate::clock::{Clock, SystemClock};
    use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};
    use crate::types::WsUrl;
    use futures_util::StreamExt;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio_tungstenite::tungstenite::Message;

    fn test_bus() -> Arc<DispatchEventBus> {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            clock,
        ))
    }

    /// Peer observes death as stream end, error, or close frame.
    fn assert_connection_died(
        outcome: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    ) {
        assert!(
            matches!(outcome, None | Some(Err(_)) | Some(Ok(Message::Close(_)))),
            "unexpected frame from a dropped client: {outcome:?}"
        );
    }

    /// Upgrade carries bare User-Agent and WS x-korigin.
    #[tokio::test]
    #[allow(clippy::result_large_err)] // accept_hdr_async's Callback dictates the Err type
    async fn upgrade_request_carries_sdk_identity_headers() {
        use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (hdr_tx, hdr_rx) = tokio::sync::oneshot::channel::<(String, String)>();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let _ws = tokio_tungstenite::accept_hdr_async(tcp, |req: &Request, resp: Response| {
                let get = |name: &str| {
                    req.headers()
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_string()
                };
                let _ = hdr_tx.send((get("user-agent"), get("x-korigin")));
                Ok(resp)
            })
            .await
            .expect("handshake");
        });

        let _socket = TokioTungsteniteWsSocket::open_and_spawn(
            WsUrl::Public,
            format!("ws://{addr}"),
            9,
            test_bus(),
        );
        let (ua, korigin) = tokio::time::timeout(Duration::from_secs(5), hdr_rx)
            .await
            .expect("upgrade within budget")
            .expect("headers captured");
        assert_eq!(ua, crate::transport::http::SDK_USER_AGENT);
        assert_eq!(korigin, crate::transport::http::SDK_KORIGIN_WS);
        server.await.expect("server task");
    }

    /// Unparseable URL publishes WsUpgradeFailed (opener does not die silently).
    #[tokio::test]
    async fn invalid_url_publishes_upgrade_failed() {
        let bus = test_bus();
        let (fail_tx, fail_rx) = tokio::sync::oneshot::channel::<()>();
        let fail_tx = std::sync::Mutex::new(Some(fail_tx));
        let _sub = bus.subscribe(
            crate::dispatch::EventType::WsUpgradeFailed,
            Arc::new(move |_env| {
                if let Some(tx) = fail_tx.lock().expect("failure signal lock").take() {
                    let _ = tx.send(());
                }
            }),
            1,
        );
        let _socket = TokioTungsteniteWsSocket::open_and_spawn(
            WsUrl::Public,
            "not a url".to_string(),
            10,
            Arc::clone(&bus),
        );
        tokio::time::timeout(Duration::from_secs(5), fail_rx)
            .await
            .expect("WsUpgradeFailed within budget")
            .expect("failure signal");
    }

    /// Drop mid-connect must close the connection (abandon or discard path).
    #[tokio::test]
    async fn drop_during_connect_closes_the_upgraded_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel::<()>();
        let (go_tx, go_rx) = tokio::sync::oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let _ = accepted_tx.send(());
            go_rx.await.expect("go signal");
            match tokio_tungstenite::accept_async(tcp).await {
                Err(_) => None,
                Ok(mut ws) => Some(
                    tokio::time::timeout(Duration::from_secs(5), ws.next())
                        .await
                        .expect("connection should close after the client socket is dropped"),
                ),
            }
        });

        let socket = TokioTungsteniteWsSocket::open_and_spawn(
            WsUrl::Public,
            format!("ws://{addr}"),
            7,
            test_bus(),
        );
        tokio::time::timeout(Duration::from_secs(5), accepted_rx)
            .await
            .expect("dial within budget")
            .expect("accepted signal");
        drop(socket);
        go_tx.send(()).expect("server alive");

        match server.await.expect("server task") {
            None => {} // handshake never completed — connection already dead
            Some(outcome) => assert_connection_died(outcome),
        }
    }

    /// After WsUpgradeOk (handles stored), drop closes even with no server frame.
    #[tokio::test]
    async fn drop_of_idle_established_socket_closes_the_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(tcp)
                .await
                .expect("handshake");
            tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("connection should close after the client socket is dropped")
        });

        let bus = test_bus();
        let (up_tx, up_rx) = tokio::sync::oneshot::channel::<()>();
        let up_tx = std::sync::Mutex::new(Some(up_tx));
        let _sub = bus.subscribe(
            crate::dispatch::EventType::WsUpgradeOk,
            Arc::new(move |_env| {
                if let Some(tx) = up_tx.lock().expect("upgrade signal lock").take() {
                    let _ = tx.send(());
                }
            }),
            1,
        );

        let socket = TokioTungsteniteWsSocket::open_and_spawn(
            WsUrl::Public,
            format!("ws://{addr}"),
            8,
            Arc::clone(&bus),
        );
        tokio::time::timeout(Duration::from_secs(5), up_rx)
            .await
            .expect("WsUpgradeOk within budget")
            .expect("upgrade signal");
        drop(socket);

        assert_connection_died(server.await.expect("server task"));
    }

    /// Reader parked on idle stream exits when the inbox receiver drops.
    #[tokio::test]
    async fn reader_parked_on_idle_stream_exits_when_the_socket_side_closes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.expect("accept");
            let mut ws = tokio_tungstenite::accept_async(tcp)
                .await
                .expect("handshake");
            tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("connection should close after the reader exits")
        });

        let (client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .expect("connect");
        let (sink, rx_stream) = client.split();
        let (inbox_tx, inbox_rx) = tokio::sync::mpsc::channel::<
            Result<crate::transport::WsFrame, crate::transport::TransportError>,
        >(4);

        let reader = tokio::spawn(super::reader_loop(
            rx_stream,
            inbox_tx,
            test_bus(),
            9,
            WsUrl::Public,
        ));
        drop(inbox_rx);

        tokio::time::timeout(Duration::from_secs(2), reader)
            .await
            .expect("reader must exit when the receiver drops")
            .expect("reader join");

        drop(sink);
        assert_connection_died(server.await.expect("server task"));
    }

    /// Drop during unresponsive connect abandons the half-open attempt.
    #[tokio::test]
    async fn drop_during_unresponsive_connect_abandons_the_attempt() {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");

        let server = tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.expect("accept");
            let mut buf = [0u8; 4096];
            loop {
                let n = tokio::time::timeout(Duration::from_secs(5), tcp.read(&mut buf))
                    .await
                    .expect("connection should close after the socket is dropped")
                    .expect("read");
                if n == 0 {
                    return;
                }
            }
        });

        let socket = TokioTungsteniteWsSocket::open_and_spawn(
            WsUrl::Public,
            format!("ws://{addr}"),
            10,
            test_bus(),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(socket);

        server.await.expect("server task");
    }
}

#[cfg(test)]
mod classify_tests {
    use super::classify_tungstenite_error;
    use tokio_tungstenite::tungstenite::Error as TError;
    use tokio_tungstenite::tungstenite::http::Response;

    fn io(kind: std::io::ErrorKind) -> TError {
        TError::Io(std::io::Error::new(kind, "test"))
    }

    fn http(status: u16) -> TError {
        TError::Http(Box::new(
            Response::builder()
                .status(status)
                .body(Option::<Vec<u8>>::None)
                .unwrap(),
        ))
    }

    #[test]
    fn tcp_failures_are_transient() {
        for kind in [
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::ConnectionReset,
        ] {
            assert!(
                classify_tungstenite_error(io(kind)).transient,
                "{kind:?} should be transient"
            );
        }
    }

    #[test]
    fn dns_class_io_errors_are_transient() {
        for kind in [
            std::io::ErrorKind::NotFound,
            std::io::ErrorKind::Other,
            std::io::ErrorKind::AddrNotAvailable,
        ] {
            assert!(
                classify_tungstenite_error(io(kind)).transient,
                "{kind:?} (DNS-class) should be transient"
            );
        }
    }

    #[test]
    fn connection_closed_during_connect_is_transient() {
        assert!(classify_tungstenite_error(TError::ConnectionClosed).transient);
        assert!(classify_tungstenite_error(TError::AlreadyClosed).transient);
    }

    #[test]
    fn http_upgrade_rejection_carries_status_and_transient_set() {
        use crate::transport::TransportErrorKind;
        for status in [408, 409, 425, 429, 503, 504, 507] {
            let te = classify_tungstenite_error(http(status));
            assert_eq!(te.kind, TransportErrorKind::HttpUpgradeRejected { status });
            assert!(te.transient, "HTTP {status} should be transient");
        }
        for status in [400, 401, 403, 404, 426, 501] {
            let te = classify_tungstenite_error(http(status));
            assert_eq!(te.kind, TransportErrorKind::HttpUpgradeRejected { status });
            assert!(!te.transient, "HTTP {status} should be permanent");
        }
    }
}
