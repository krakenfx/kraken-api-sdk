//! HTTP transport — REST adapter. `HttpTransport` is the wire-level boundary;
//! impls live behind `Arc<dyn HttpTransport>` on the REST pipeline.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use reqwest::header::HeaderValue;

use crate::transport::{TransportError, TransportErrorKind};

/// SDK User-Agent token — always on the wire (alone, or appended after the app token).
pub(crate) const SDK_USER_AGENT: &str = concat!("kraken-sdk-rust/", env!("CARGO_PKG_VERSION"));

/// Attribution header, always on the wire and reserved against `with_headers`.
/// Packed korigin (radix-32): channel 3=REST / 4=WS, interface ID 17.
pub(crate) const SDK_KORIGIN_HEADER: &str = "x-korigin";
/// `x-korigin` value for REST requests (Channel = REST API).
pub(crate) const SDK_KORIGIN_REST: &str = "12003";
/// `x-korigin` value for the WS upgrade requests (Channel = WebSocket).
pub(crate) const SDK_KORIGIN_WS: &str = "12004";

/// REST transport interface (`async_trait` for dyn-safety). Invoked via `RestSurface`.
#[async_trait]
pub trait HttpTransport: Send + Sync {
    /// GET path (appended to base URL) with query params; response as JSON.
    async fn get_json(
        &self,
        path: &str,
        query_params: &[(&str, &str)],
    ) -> Result<Value, TransportError>;

    /// POST form body (nonce included) with pre-computed `API-Key`/`API-Sign`.
    async fn post_form_signed(
        &self,
        path: &str,
        body: &str,
        api_key_header: &str,
        api_sign_header: &str,
    ) -> Result<Value, TransportError>;
}

/// Production `HttpTransport` backed by `reqwest`.
pub struct ReqwestHttpTransport {
    client: reqwest::Client,
    base_url: String,
}

impl ReqwestHttpTransport {
    /// Default base URL `https://api.kraken.com`, 30s timeout. No I/O.
    pub fn new() -> Self {
        Self::with_base_url("https://api.kraken.com")
    }

    /// Custom base URL, 30s timeout.
    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        Self::with_base_url_and_timeout(base_url, Duration::from_secs(30))
    }

    /// Custom base URL and per-request timeout (baked into the reqwest client).
    pub fn with_base_url_and_timeout(base_url: impl Into<String>, timeout: Duration) -> Self {
        Self::with_headers(base_url, timeout, &reqwest::header::HeaderMap::new())
    }

    /// Extra default headers (embedding identity). App `user-agent` gets
    /// [`SDK_USER_AGENT`] appended, never replaced. Signing headers are never shadowed.
    pub(crate) fn with_headers(
        base_url: impl Into<String>,
        timeout: Duration,
        headers: &reqwest::header::HeaderMap,
    ) -> Self {
        // Bound connect separately so a connect/TLS stall isn't mislabeled sent-ambiguous.
        let connect_timeout = std::cmp::min(timeout, Duration::from_secs(10));
        // UA pre-composed — no reliance on reqwest builder-call ordering.
        let mut defaults = headers.clone();
        defaults.insert(reqwest::header::USER_AGENT, compose_user_agent(headers));
        // `x-korigin` is reserved; caller map cannot override.
        defaults.insert(
            reqwest::header::HeaderName::from_static(SDK_KORIGIN_HEADER),
            HeaderValue::from_static(SDK_KORIGIN_REST),
        );
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(connect_timeout)
            .default_headers(defaults)
            .build()
            .expect("reqwest::Client::build() — invariant: rustls feature available");
        Self {
            client,
            base_url: base_url.into(),
        }
    }
}

impl Default for ReqwestHttpTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HttpTransport for ReqwestHttpTransport {
    async fn get_json(
        &self,
        path: &str,
        query_params: &[(&str, &str)],
    ) -> Result<Value, TransportError> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .get(&url)
            .query(query_params)
            .send()
            .await
            .map_err(classify_reqwest_error)?;

        let status = response.status();
        if !status.is_success() {
            return Err(TransportError {
                kind: TransportErrorKind::HttpStatus {
                    status: status.as_u16(),
                },
                transient: is_transient_status(&status),
            });
        }

        response
            .json::<Value>()
            .await
            .map_err(classify_reqwest_error)
    }

    async fn post_form_signed(
        &self,
        path: &str,
        body: &str,
        api_key_header: &str,
        api_sign_header: &str,
    ) -> Result<Value, TransportError> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .post(&url)
            .header("API-Key", api_key_header)
            .header("API-Sign", api_sign_header)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body.to_string())
            .send()
            .await
            .map_err(classify_reqwest_error)?;

        let status = response.status();
        if !status.is_success() {
            return Err(TransportError {
                kind: TransportErrorKind::HttpStatus {
                    status: status.as_u16(),
                },
                transient: is_transient_status(&status),
            });
        }

        response
            .json::<Value>()
            .await
            .map_err(classify_reqwest_error)
    }
}

/// Retryable non-2xx: 5xx plus 408/425/429.
pub(crate) fn is_transient_status(status: &reqwest::StatusCode) -> bool {
    status.is_server_error() || matches!(status.as_u16(), 408 | 425 | 429)
}

/// Map a `reqwest::Error` to `TransportError`.
fn classify_reqwest_error(e: reqwest::Error) -> TransportError {
    classify_error_flags(
        e.is_connect(),
        e.is_timeout(),
        e.is_body(),
        e.is_request(),
        e.is_decode(),
        e.to_string(),
    )
}

/// Phase flags → `TransportError`. `is_connect` first (connect timeout is not-sent);
/// decode failure (2xx body) is sent-ambiguous.
fn classify_error_flags(
    is_connect: bool,
    is_timeout: bool,
    is_body: bool,
    is_request: bool,
    is_decode: bool,
    fallback: String,
) -> TransportError {
    let kind = if is_connect {
        if is_timeout {
            TransportErrorKind::TcpConnectTimeout
        } else {
            TransportErrorKind::TcpRefused
        }
    } else if is_timeout || is_body || is_request || is_decode {
        TransportErrorKind::RequestSentNoResponse
    } else {
        TransportErrorKind::Other(fallback)
    };
    let transient = is_timeout || is_connect || is_body || is_request;
    TransportError { kind, transient }
}

/// Compose User-Agent: app token first, [`SDK_USER_AGENT`] appended (curl convention).
fn compose_user_agent(headers: &reqwest::header::HeaderMap) -> HeaderValue {
    match headers.get(reqwest::header::USER_AGENT) {
        Some(app) => {
            let mut ua = app.as_bytes().to_vec();
            ua.push(b' ');
            ua.extend_from_slice(SDK_USER_AGENT.as_bytes());
            HeaderValue::from_bytes(&ua)
                .expect("a valid header value + space + ASCII is a valid header value")
        }
        None => HeaderValue::from_static(SDK_USER_AGENT),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SDK_KORIGIN_REST, SDK_KORIGIN_WS, SDK_USER_AGENT, classify_error_flags, compose_user_agent,
        is_transient_status,
    };
    use crate::transport::TransportErrorKind;
    use reqwest::StatusCode;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn classify_error_flags_maps_phases() {
        let e = classify_error_flags(true, true, false, false, false, "x".into());
        assert_eq!(e.kind, TransportErrorKind::TcpConnectTimeout);
        assert!(e.transient);
        let e = classify_error_flags(true, false, false, false, false, "x".into());
        assert_eq!(e.kind, TransportErrorKind::TcpRefused);
        assert!(e.transient);
        for (t, b, r) in [
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            let e = classify_error_flags(false, t, b, r, false, "x".into());
            assert_eq!(e.kind, TransportErrorKind::RequestSentNoResponse);
            assert!(e.transient);
        }
        let e = classify_error_flags(false, false, false, false, true, "x".into());
        assert_eq!(e.kind, TransportErrorKind::RequestSentNoResponse);
        assert!(!e.transient);
        let e = classify_error_flags(false, false, false, false, false, "weird".into());
        assert_eq!(e.kind, TransportErrorKind::Other("weird".into()));
        assert!(!e.transient);
    }

    #[test]
    fn transient_status_covers_5xx_and_4xx_throttle_timeout() {
        for code in [500, 502, 503, 408, 425, 429] {
            assert!(
                is_transient_status(&StatusCode::from_u16(code).unwrap()),
                "HTTP {code} must be transient (retryable)"
            );
        }
        for code in [400, 401, 403, 404, 422] {
            assert!(
                !is_transient_status(&StatusCode::from_u16(code).unwrap()),
                "HTTP {code} must be non-transient"
            );
        }
    }

    /// Identity headers and composed UA reach the wire.
    #[tokio::test]
    async fn with_headers_stamps_identity_headers_on_the_wire() {
        use super::ReqwestHttpTransport;
        use crate::transport::HttpTransport;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                head.extend_from_slice(&chunk[..n]);
                if n == 0 || head.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let resp = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                        content-length: 2\r\nconnection: close\r\n\r\n{}";
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.shutdown().await.unwrap();
            String::from_utf8_lossy(&head).to_lowercase()
        });

        let mut headers = HeaderMap::new();
        headers.insert("x-kraken-client", HeaderValue::from_static("kraken-cli"));
        headers.insert(
            reqwest::header::USER_AGENT,
            HeaderValue::from_static("kraken-cli/0.2.0"),
        );
        let transport = ReqwestHttpTransport::with_headers(
            format!("http://{addr}"),
            std::time::Duration::from_secs(5),
            &headers,
        );
        transport.get_json("/0/public/Time", &[]).await.unwrap();

        let head = server.await.unwrap();
        assert!(head.contains("x-kraken-client: kraken-cli"), "{head}");
        let want_ua = format!("user-agent: kraken-cli/0.2.0 {SDK_USER_AGENT}").to_lowercase();
        assert!(head.contains(&want_ua), "{head}");
        assert!(head.contains("x-korigin: 12003"), "{head}");
    }

    /// x-korigin: Channel 3/4, Interface 17, other dimensions zero.
    #[test]
    fn korigin_constants_encode_channel_and_interface() {
        for (value, channel) in [(SDK_KORIGIN_REST, 3u64), (SDK_KORIGIN_WS, 4u64)] {
            let packed = u64::from_str_radix(value, 32).unwrap();
            assert_eq!(packed, channel | (17 << 16), "{value}");
        }
    }

    /// App user-agent gets SDK token appended; alone if unset.
    #[test]
    fn compose_user_agent_appends_sdk_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::USER_AGENT,
            HeaderValue::from_static("kraken-cli/0.2.0"),
        );
        assert_eq!(
            compose_user_agent(&headers),
            format!("kraken-cli/0.2.0 {SDK_USER_AGENT}").as_str()
        );
        assert_eq!(compose_user_agent(&HeaderMap::new()), SDK_USER_AGENT);
    }
}
