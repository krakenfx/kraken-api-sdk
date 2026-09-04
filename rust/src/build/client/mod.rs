//! `ClientBuilder` + `Client` — construction and wiring.

use std::sync::Arc;

use crate::api::{
    AccountNamespace, EventsNamespace, MarketNamespace, SubscriptionNamespace, TradeNamespace,
};
use crate::auth::{AuthStack, NonceSource};
use crate::build::config_resolver::ConfigResolver;
use crate::build::knobs::Knobs;
use crate::clock::Clock;
use crate::conn::ConnectionSupervisor;
use crate::dispatch::DispatchEventBus;
use crate::jitter::JitterSource;
use crate::transport::{HttpTransport, WsSocketFactory};
use crate::types::{ApiKey, ApiSecret, Otp};

mod accessors;
mod builder;
mod completion;

pub use completion::{CloseError, Completion, ReadyError};

/// Fluent builder for [`Client`]. Resolves into an immutable [`Client`] at
/// [`Self::build`]; config is validated at build time with no network calls.
/// Only credentials are required; everything else has an SDK default.
#[must_use = "a ClientBuilder does nothing until `.build()`"]
pub struct ClientBuilder {
    transport: Option<Arc<dyn HttpTransport>>,
    /// Eager credential decode from `with_api_key`; `build()` reports any error.
    credentials: Option<Result<(ApiKey, ApiSecret), ConfigError>>,
    /// 2FA one-time password; moved into the `AuthStack` at `.build()`.
    otp: Option<Otp>,
    /// Opt-in rate-limit tier override (`None` → `Tier::Starter`).
    tier_override: Option<crate::rate_limit::Tier>,
    /// Injected monotonic clock (`None` → system clock).
    clock: Option<Arc<dyn Clock>>,
    /// Injected nonce source (`None` → system-clock nonces).
    nonce_source: Option<Arc<dyn NonceSource>>,
    /// Injected jitter source (`None` → OS-seeded SplitMix64).
    jitter_source: Option<Arc<dyn JitterSource>>,
    /// Embedding-app identity headers for the SDK-built REST transport.
    headers: reqwest::header::HeaderMap,
    /// Knob-resolution accumulator (builder overrides + optional config file).
    config_resolver: ConfigResolver,
}

impl std::fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("credentials", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// Error type for [`ClientBuilder::build`] config-validation failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// Required credentials absent; `code()` is `BAD_CREDS`. Not currently
    /// produced — a credential-less private call surfaces an `AuthError` instead.
    #[error("Missing credentials.")]
    MissingCredentials,
    /// The API secret failed base64 decoding or was empty; `code()` is `BAD_CREDS`.
    #[error("Invalid credentials.")]
    InvalidCredentials,
    /// Authenticated-WS feature without credentials; `code()` is `BAD_CREDS`.
    /// Not currently produced.
    #[error("WS auth requires credentials.")]
    WsAuthRequiresCredentials,
    /// A runtime `set_knob` targeted a construction-only knob.
    #[error("Knob {knob} is immutable post-build.")]
    ImmutableKnob {
        /// Name of the construction-only knob.
        knob: String,
    },
    /// A caller-supplied `cl_ord_id` failed format validation. Not currently
    /// produced — `ClOrdId::new` raises `ClOrdIdError` instead.
    #[error("Invalid cl_ord_id.")]
    InvalidCLOrdId,
    /// Nonce ordering conflict for the configured API key. Not currently produced.
    #[error("Nonce conflict.")]
    NonceConflict,
    /// The requested operation depends on a disabled feature. Not currently produced.
    #[error("Feature {feature} is disabled.")]
    FeatureDisabled {
        /// Name of the disabled feature.
        feature: String,
    },
    /// An order-amend request carried no fields to change. Not currently produced —
    /// the amend path raises `TradeError::EmptyAmendRequest` instead.
    #[error("Amend request is empty.")]
    EmptyAmendRequest,
    /// The client has been `close()`d. Not currently produced.
    #[error("Client is closed; no new operations accepted.")]
    ClientClosed,
    /// An endpoint URL knob lacked the required TLS scheme prefix.
    #[error(
        "`{knob}` must use the `{scheme}` (TLS) scheme; cleartext or non-TLS endpoint URLs are refused at build time."
    )]
    InsecureEndpointScheme {
        /// Endpoint knob: `rest_base_url`, `ws_public_url`, or `ws_auth_url`.
        knob: String,
        /// Required scheme: `https://` for REST, `wss://` for WS.
        scheme: String,
    },
    /// `.with_headers` combined with an injected transport.
    #[error(
        "Custom headers cannot be combined with an injected transport; stamp them on the injected transport instead."
    )]
    HeadersRequireSdkTransport,
    /// A `with_headers` entry uses a protocol header name the SDK owns
    /// (case-insensitive: `API-Key`, `API-Sign`, `Content-Type`, `Accept`, `Host`,
    /// `X-KOrigin`). `User-Agent` is allowed; `x-korigin` is not.
    #[error("Header `{name}` is reserved by the SDK and cannot be set via with_headers.")]
    ReservedHeaderName {
        /// The offending header name, lowercased.
        name: String,
    },
    /// A configuration value was structurally invalid at `.build()`/`set_knob`
    /// time. Detail never echoes a credential.
    #[error("Invalid configuration: {detail}.")]
    InvalidConfig {
        /// SDK-composed description (never echoes a credential).
        detail: String,
    },
    /// Catch-all; `code()` is `UNKNOWN`.
    #[error("{kraken_message}")]
    Unknown {
        /// Machine-readable fault code, carried verbatim from the producing site.
        kraken_code: String,
        /// Human-readable description; serves as this variant's entire `Display` output.
        kraken_message: String,
    },
}

impl crate::error::sealed::Sealed for ConfigError {}

impl crate::error::ApiError for ConfigError {
    fn code(&self) -> &str {
        use ConfigError::*;
        match self {
            MissingCredentials | InvalidCredentials | WsAuthRequiresCredentials => "BAD_CREDS",
            ImmutableKnob { .. } | EmptyAmendRequest => "INVALID_ARGUMENTS",
            HeadersRequireSdkTransport => "HEADERS_REQUIRE_SDK_TRANSPORT",
            ReservedHeaderName { .. } => "RESERVED_HEADER_NAME",
            InvalidCLOrdId => "INVALID_CL_ORD_ID",
            NonceConflict => "NONCE_CONFLICT",
            FeatureDisabled { .. } => "FEATURE_DISABLED",
            ClientClosed => "CLIENT_CLOSED",
            InsecureEndpointScheme { .. } => "INSECURE_ENDPOINT_SCHEME",
            InvalidConfig { .. } => "INVALID_CONFIG",
            Unknown { .. } => "UNKNOWN",
        }
    }
    fn category(&self) -> crate::error::ErrorCategory {
        use crate::error::ErrorCategory;
        match self {
            ConfigError::ClientClosed => ErrorCategory::Client,
            _ => ErrorCategory::Config,
        }
    }
    fn retryable(&self) -> bool {
        false
    }
    crate::error::api_error_tail!();
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The Kraken Unified SDK client. Construct via [`Client::builder()`] or
/// [`ClientBuilder::new()`], then call `.build()` to finalize.
pub struct Client {
    auth: Arc<AuthStack>,
    market: MarketNamespace,
    account: AccountNamespace,
    trade: TradeNamespace,
    subscription: SubscriptionNamespace,
    events: EventsNamespace,
    presence_mirror: crate::dispatch::PresenceMirror,
    subscription_mirror: crate::conn::subscription_registry::SubscriptionMirror,
    bus: Arc<DispatchEventBus>,
    supervisor: Arc<ConnectionSupervisor>,
    ws_factory: Arc<WsSocketFactory>,
    clock: Arc<dyn Clock>,
    /// Reconnect-backoff jitter; override via [`ClientBuilder::with_jitter_source`].
    jitter: Arc<dyn JitterSource>,
    /// Shared tunable-value holder; runtime `set_knob` is visible everywhere.
    knobs: Arc<Knobs>,
    /// IoReactor join handle; `Some` after `ready()`.
    io_reactor_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    next_client_request_id: std::sync::atomic::AtomicU64,
}

impl Drop for Client {
    fn drop(&mut self) {
        // begin_shutdown first so abort is an intended stop, not LoopFailedEvent.
        self.bus.begin_shutdown();
        if let Ok(mut g) = self.io_reactor_handle.lock() {
            if let Some(join) = g.take() {
                join.abort();
                // Abort before first poll never arms LoopDeathGuard — engage death path.
                self.bus.on_loop_death(
                    crate::dispatch::ReactorName::Io,
                    crate::dispatch::LoopFailureCause::Cancelled,
                );
            }
        }
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("credentials", &"<redacted>")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;

    #[test]
    fn default_builder_constructs_client_with_namespaces() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder().build().unwrap();
        let _market = client.market();
        let _account = client.account();
    }

    #[test]
    #[allow(drop_bounds)] // intentional: a `T: Drop` bound is exactly how we assert an EXPLICIT Drop impl
    fn client_has_explicit_drop_impl() {
        fn assert_explicit_drop<T: Drop>() {}
        assert_explicit_drop::<Client>();
    }

    #[test]
    fn drop_without_ready_is_clean() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder().build().unwrap();
        drop(client);
    }

    #[test]
    fn builder_accepts_custom_base_url() {
        let _g = crate::build::test_env::env_lock();
        let _client = Client::builder()
            .with_base_url("https://beta-api.kraken.com")
            .build()
            .unwrap();
    }

    struct NoopTransport;

    #[async_trait::async_trait]
    impl HttpTransport for NoopTransport {
        async fn get_json(
            &self,
            _path: &str,
            _query_params: &[(&str, &str)],
        ) -> Result<serde_json::Value, crate::transport::TransportError> {
            Ok(serde_json::Value::Null)
        }
        async fn post_form_signed(
            &self,
            _path: &str,
            _body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<serde_json::Value, crate::transport::TransportError> {
            Ok(serde_json::Value::Null)
        }
    }

    #[test]
    fn headers_with_injected_transport_are_rejected_at_build() {
        let _g = crate::build::test_env::env_lock();

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "x-kraken-client",
            reqwest::header::HeaderValue::from_static("kraken-cli"),
        );
        let err = Client::builder()
            .with_transport(Arc::new(NoopTransport))
            .with_headers(headers)
            .build()
            .unwrap_err();
        assert!(matches!(err, ConfigError::HeadersRequireSdkTransport));
    }

    #[test]
    fn reserved_header_names_are_rejected_at_build() {
        let _g = crate::build::test_env::env_lock();
        for (input, expect) in [
            ("API-Key", "api-key"),
            ("api-sign", "api-sign"),
            ("Content-Type", "content-type"),
            ("ACCEPT", "accept"),
            ("Host", "host"),
            ("X-KOrigin", "x-korigin"),
        ] {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::HeaderName::from_bytes(input.as_bytes()).unwrap(),
                reqwest::header::HeaderValue::from_static("x"),
            );
            let err = Client::builder().with_headers(headers).build().unwrap_err();
            match err {
                ConfigError::ReservedHeaderName { ref name } => assert_eq!(name, expect),
                other => panic!("{input}: expected ReservedHeaderName, got {other:?}"),
            }
        }
    }

    #[test]
    fn transport_gate_preempts_reserved_header_name() {
        let _g = crate::build::test_env::env_lock();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("api-key", reqwest::header::HeaderValue::from_static("x"));
        let err = Client::builder()
            .with_transport(Arc::new(NoopTransport))
            .with_headers(headers)
            .build()
            .unwrap_err();
        assert!(matches!(err, ConfigError::HeadersRequireSdkTransport));
    }

    #[test]
    fn user_agent_header_is_accepted_at_build() {
        let _g = crate::build::test_env::env_lock();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "user-agent",
            reqwest::header::HeaderValue::from_static("my-app/1.0"),
        );
        assert!(Client::builder().with_headers(headers).build().is_ok());
    }

    #[test]
    fn with_headers_repeated_calls_replace_per_name() {
        let mut first = reqwest::header::HeaderMap::new();
        first.insert(
            "x-kraken-client",
            reqwest::header::HeaderValue::from_static("first"),
        );
        let mut second = reqwest::header::HeaderMap::new();
        second.insert(
            "x-kraken-client",
            reqwest::header::HeaderValue::from_static("second"),
        );
        let builder = Client::builder().with_headers(first).with_headers(second);
        assert_eq!(
            builder.headers.get("x-kraken-client"),
            Some(&reqwest::header::HeaderValue::from_static("second"))
        );
        assert_eq!(builder.headers.len(), 1, "no duplicate values accumulate");
    }

    #[test]
    fn builder_rejects_cleartext_base_url() {
        let _g = crate::build::test_env::env_lock();
        let err = Client::builder()
            .with_base_url("http://api.kraken.com")
            .build()
            .unwrap_err();
        use crate::error::ApiError;
        assert_eq!(err.code(), "INSECURE_ENDPOINT_SCHEME");
        assert!(matches!(
            err.category(),
            crate::error::ErrorCategory::Config
        ));
        assert!(
            matches!(&err, ConfigError::InsecureEndpointScheme { .. }),
            "expected InsecureEndpointScheme, got {err:?}"
        );
    }

    #[test]
    fn unknown_config_error_categorises_as_config() {
        use crate::error::{ApiError, ErrorCategory};
        let err = ConfigError::Unknown {
            kraken_code: "INTERNAL".into(),
            kraken_message: "config file parse failed".into(),
        };
        assert_eq!(err.category(), ErrorCategory::Config);
    }

    #[test]
    fn invalid_config_error_classifies_as_config_non_retryable() {
        use crate::error::{ApiError, ErrorCategory};
        let err = ConfigError::InvalidConfig {
            detail: "config file parse failed".into(),
        };
        assert_eq!(err.code(), "INVALID_CONFIG");
        assert_eq!(err.category(), ErrorCategory::Config);
        assert!(!err.retryable());
    }

    #[test]
    fn builder_accepts_injected_clock_and_nonce_source() {
        let _g = crate::build::test_env::env_lock();
        struct FixedClock;
        impl Clock for FixedClock {
            fn now(&self) -> crate::types::MonotonicInstant {
                crate::types::MonotonicInstant::now()
            }
        }
        struct FixedNonce;
        impl NonceSource for FixedNonce {
            fn next_nonce(&self, _key: &ApiKey) -> crate::types::Nonce {
                crate::types::Nonce(42)
            }
        }

        let client = Client::builder()
            .with_clock(Arc::new(FixedClock))
            .with_nonce_source(Arc::new(FixedNonce))
            .with_api_key(ApiKey::new("test-key"), BASE64.encode(vec![0x01u8; 32]))
            .build()
            .unwrap();
        assert!(client.auth.api_key().is_some());
    }

    #[test]
    fn builder_accepts_injected_jitter_source() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder()
            .with_jitter_source(Arc::new(crate::jitter::FixedJitter(0.0)))
            .build()
            .unwrap();
        assert_eq!(client.jitter.next_unit(), 0.0);
    }

    #[test]
    fn build_clamps_staleness_window_to_55s() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder()
            .with_knob("staleness_window_ms", KnobValue::U32(90_000))
            .build()
            .unwrap();
        assert_eq!(client.knobs.staleness_window_ms, 55_000);
    }

    #[test]
    fn with_api_key_accepts_valid_base64_secret() {
        let _g = crate::build::test_env::env_lock();
        let secret_b64 = BASE64.encode(vec![0x01u8; 32]);
        let client = Client::builder()
            .with_api_key(ApiKey::new("test-key"), secret_b64)
            .build()
            .unwrap();
        assert!(client.auth.api_key().is_some());
    }

    #[test]
    fn build_rejects_invalid_base64_secret() {
        let _g = crate::build::test_env::env_lock();
        let result = Client::builder()
            .with_api_key(ApiKey::new("test-key"), "not!valid!base64!!".to_string())
            .build();
        assert!(matches!(result, Err(ConfigError::InvalidCredentials)));
    }

    #[test]
    fn build_rejects_empty_secret() {
        let _g = crate::build::test_env::env_lock();
        let result = Client::builder()
            .with_api_key(ApiKey::new("test-key"), String::new())
            .build();
        assert!(matches!(result, Err(ConfigError::InvalidCredentials)));
    }

    #[test]
    fn with_api_key_zeroize_source_string_success_path() {
        let _g = crate::build::test_env::env_lock();
        let secret_b64 = BASE64.encode(vec![0xABu8; 32]);
        let client = Client::builder()
            .with_api_key(ApiKey::new("k"), secret_b64)
            .build()
            .expect("decode must succeed; ApiSecret must be extracted before source String wipe");
        assert!(client.auth.api_key().is_some());
    }

    #[test]
    fn with_api_key_zeroize_source_string_error_path() {
        let _g = crate::build::test_env::env_lock();
        let result = Client::builder()
            .with_api_key(ApiKey::new("k"), "!!!not-base64!!!".to_string())
            .build();
        assert!(
            matches!(result, Err(ConfigError::InvalidCredentials)),
            "error path must still surface ConfigError::InvalidCredentials at build() after source String wipe"
        );
    }

    /// Records the last signed body, so a test can read the wire form.
    #[derive(Default)]
    struct BodyCapturingTransport {
        last_body: std::sync::Mutex<Option<String>>,
    }

    #[async_trait::async_trait]
    impl HttpTransport for BodyCapturingTransport {
        async fn get_json(
            &self,
            _path: &str,
            _query_params: &[(&str, &str)],
        ) -> Result<serde_json::Value, crate::transport::TransportError> {
            Ok(serde_json::Value::Null)
        }
        async fn post_form_signed(
            &self,
            _path: &str,
            body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<serde_json::Value, crate::transport::TransportError> {
            *self.last_body.lock().expect("capture lock") = Some(body.to_string());
            Ok(serde_json::json!({ "error": [], "result": {} }))
        }
    }

    /// `with_otp` → `build()` → signed wire body. The signing tests build an
    /// `AuthStack` directly, so only this one fails if the builder drops the otp.
    #[tokio::test]
    async fn with_otp_reaches_the_signed_wire_body() {
        let transport = Arc::new(BodyCapturingTransport::default());
        // Scoped to `build()`, the only phase reading `KRAKEN_*`, so the blocking
        // guard is released before the await.
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder()
                .with_transport(Arc::clone(&transport) as Arc<dyn HttpTransport>)
                .with_api_key(ApiKey::new("test-key"), BASE64.encode(vec![0x01u8; 32]))
                .with_otp("static-2fa-password")
                .build()
                .expect("valid credentials and default config")
        };

        client
            .account()
            .balance()
            .await
            .expect("canned empty balance decodes");

        let body = transport
            .last_body
            .lock()
            .expect("capture lock")
            .clone()
            .expect("a signed POST was sent");
        assert!(
            body.contains("otp=static-2fa-password"),
            "builder-configured otp must reach the signed body — got {body}"
        );
    }

    /// No `with_otp` must mean no `otp` key at all — Kraken rejects a spurious one
    /// on a key without 2FA.
    #[tokio::test]
    async fn build_without_otp_sends_no_otp_field() {
        let transport = Arc::new(BodyCapturingTransport::default());
        // Scoped to `build()`, the only phase reading `KRAKEN_*`, so the blocking
        // guard is released before the await.
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder()
                .with_transport(Arc::clone(&transport) as Arc<dyn HttpTransport>)
                .with_api_key(ApiKey::new("test-key"), BASE64.encode(vec![0x01u8; 32]))
                .build()
                .expect("valid credentials and default config")
        };

        client
            .account()
            .balance()
            .await
            .expect("canned empty balance decodes");

        let body = transport
            .last_body
            .lock()
            .expect("capture lock")
            .clone()
            .expect("a signed POST was sent");
        assert!(!body.contains("otp"), "unexpected otp in body — got {body}");
    }

    #[test]
    fn subscription_namespace_gate_rejects_without_handler() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder().build().unwrap();
        let err = client
            .subscription()
            .subscribe_ticker(
                vec![crate::types::Symbol::new("BTC/USD").unwrap()],
                None,
                None,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            crate::api::SubscriptionError::NoHandlerRegistered {
                channel: crate::types::ChannelName::Ticker
            }
        ));
    }

    #[test]
    fn presence_mirror_is_the_same_arc_in_registry_and_ws_surface() {
        let _g = crate::build::test_env::env_lock();
        use crate::dispatch::{HandlerCallback, HandlerRegistry};
        use crate::types::ChannelName;

        let client = Client::builder().build().unwrap();

        let mut reg = HandlerRegistry::new(std::sync::Arc::clone(&client.presence_mirror));
        let cb: HandlerCallback = HandlerCallback::noop();
        reg.register(ChannelName::Ticker, cb);

        let ok = client.subscription().subscribe_ticker(
            vec![crate::types::Symbol::new("BTC/USD").unwrap()],
            None,
            None,
        );
        assert!(
            ok.is_ok(),
            "gate should pass once a handler is registered, got {:?}",
            ok
        );

        let err = client
            .subscription()
            .subscribe_trade(vec![], None)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::api::SubscriptionError::NoHandlerRegistered {
                channel: ChannelName::Trade
            }
        ));
    }

    #[test]
    fn subscribe_book_raw_gates_on_bookraw_not_book() {
        let _g = crate::build::test_env::env_lock();
        use crate::dispatch::{HandlerCallback, HandlerRegistry};
        use crate::types::{BookDepth, ChannelName, Symbol};

        let client = Client::builder().build().unwrap();
        let mut reg = HandlerRegistry::new(std::sync::Arc::clone(&client.presence_mirror));
        let cb: HandlerCallback = HandlerCallback::noop();
        let pairs = vec![Symbol::new("BTC/USD").unwrap()];

        let err = client
            .subscription()
            .subscribe_book_raw(pairs.clone(), BookDepth::D10, None)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::api::SubscriptionError::NoHandlerRegistered {
                channel: ChannelName::BookRaw
            }
        ));

        reg.register(ChannelName::BookRaw, cb);
        assert!(
            client
                .subscription()
                .subscribe_book_raw(pairs.clone(), BookDepth::D10, None)
                .is_ok(),
            "subscribe_book_raw must pass once a BookRaw handler is registered"
        );

        let err = client
            .subscription()
            .subscribe_book(pairs, BookDepth::D10)
            .unwrap_err();
        assert!(matches!(
            err,
            crate::api::SubscriptionError::NoHandlerRegistered {
                channel: ChannelName::Book
            }
        ));
    }

    use crate::build::knobs::KnobValue;
    use crate::dispatch::{EventEnvelope, EventPayload, EventType};
    use std::time::Duration as StdDuration;

    #[test]
    fn knob_reads_both_buckets_at_defaults() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder().build().unwrap();
        assert_eq!(
            client.knob("request_timeout_ms"),
            Some(KnobValue::U32(30_000))
        );
        assert_eq!(
            client.knob("rate_limit_api_warning_pct"),
            Some(KnobValue::F64(0.80))
        );
        assert_eq!(client.knob("nope"), None);
    }

    #[test]
    fn with_knob_builder_override_construction_only() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder()
            .with_knob("request_timeout_ms", KnobValue::U32(12_000))
            .build()
            .unwrap();
        assert_eq!(
            client.knob("request_timeout_ms"),
            Some(KnobValue::U32(12_000))
        );
    }

    #[test]
    fn nonce_recovery_builder_setter_sets_the_knob() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder().with_nonce_recovery(true).build().unwrap();
        assert_eq!(client.knob("nonce_recovery"), Some(KnobValue::Bool(true)));
    }

    #[test]
    fn prefer_rest_for_orders_builder_setter_sets_the_knob() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder()
            .with_prefer_rest_for_orders(true)
            .build()
            .unwrap();
        assert_eq!(
            client.knob("prefer_rest_for_orders"),
            Some(KnobValue::Bool(true))
        );
        assert!(matches!(
            client.set_knob("prefer_rest_for_orders", KnobValue::Bool(false)),
            Err(ConfigError::ImmutableKnob { ref knob }) if knob == "prefer_rest_for_orders"
        ));
    }

    #[test]
    fn set_knob_construction_only_rejected_immutable() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder().build().unwrap();
        let err = client
            .set_knob("request_timeout_ms", KnobValue::U32(1))
            .unwrap_err();
        assert!(matches!(
            err,
            ConfigError::ImmutableKnob { ref knob } if knob == "request_timeout_ms"
        ));
    }

    #[test]
    fn set_knob_unknown_and_type_mismatch_rejected_invalid_config() {
        let _g = crate::build::test_env::env_lock();
        let client = Client::builder().build().unwrap();
        assert!(matches!(
            client.set_knob("nope", KnobValue::U32(1)),
            Err(ConfigError::InvalidConfig { .. })
        ));
        assert!(matches!(
            client.set_knob("reconnect_attempts", KnobValue::U32(1)),
            Err(ConfigError::InvalidConfig { .. })
        ));
    }

    #[tokio::test]
    async fn set_knob_runtime_changes_value_and_emits_config_changed() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        let bus = client.bus();

        let (tx, rx) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell = std::sync::Mutex::new(Some(tx));
        let _ = bus.subscribe(
            EventType::ConfigChangedEvent,
            Arc::new(move |env| {
                if let Some(tx) = tx_cell.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            1,
        );
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());

        client
            .set_knob("rate_limit_api_warning_pct", KnobValue::F64(0.5))
            .unwrap();

        assert_eq!(
            client.knob("rate_limit_api_warning_pct"),
            Some(KnobValue::F64(0.5))
        );

        let env = tokio::time::timeout(StdDuration::from_secs(2), rx)
            .await
            .expect("ConfigChangedEvent not received within timeout")
            .expect("sender dropped");
        assert_eq!(env.event_type, EventType::ConfigChangedEvent);
        assert_eq!(env.event_version, 1);
        assert_eq!(env.request_id, None);
        match env.payload {
            EventPayload::ConfigChangedEvent {
                ref knob,
                ref previous,
                ref current,
            } => {
                assert_eq!(knob.as_str(), "rate_limit_api_warning_pct");
                assert_eq!(*previous, KnobValue::F64(0.80));
                assert_eq!(*current, KnobValue::F64(0.5));
            }
            ref other => panic!("unexpected payload {other:?}"),
        }

        bus.stop_reactors();
    }

    #[tokio::test]
    async fn ready_await_and_close_await_resolve_ok() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        tokio::time::timeout(StdDuration::from_secs(2), client.ready())
            .await
            .expect("ready().await hung")
            .expect("ready resolves Ok");
        tokio::time::timeout(StdDuration::from_secs(2), client.ready())
            .await
            .expect("ready() re-entry await hung")
            .expect("ready re-entry resolves Ok");
        tokio::time::timeout(StdDuration::from_secs(2), client.close())
            .await
            .expect("close().await hung")
            .expect("close resolves Ok");
    }

    #[tokio::test]
    async fn first_start_client_ready_broadcast_carries_no_request_id() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        let seen: Arc<std::sync::Mutex<Option<Option<u64>>>> =
            Arc::new(std::sync::Mutex::new(None));
        let seen_cb = Arc::clone(&seen);
        let _watch = client
            .events()
            .on(crate::dispatch::EventType::ClientReady, move |env| {
                *seen_cb.lock().expect("seen lock") = Some(env.request_id);
            })
            .expect("register ClientReady handler");

        client.ready().await.expect("ready resolves Ok");

        let deadline = std::time::Instant::now() + StdDuration::from_secs(2);
        let observed = loop {
            if let Some(observed) = *seen.lock().expect("seen lock") {
                break observed;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "broadcast ClientReady never reached the subscriber"
            );
            tokio::task::yield_now().await;
        };
        assert_eq!(
            observed, None,
            "broadcast ClientReady must carry request_id: None (two-copy terminal shape)"
        );
    }

    #[tokio::test]
    async fn client_drop_before_first_poll_resolves_a_held_ready_completion() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        let held = client.ready();
        drop(client); // no yield: the spawned reactor task may never have been polled
        let res = tokio::time::timeout(StdDuration::from_secs(2), held)
            .await
            .expect("held ready() completion hung after Client::drop");
        if let Err(e) = res {
            assert_eq!(e, crate::build::client::ReadyError::LoopFailed);
        }
    }

    #[tokio::test]
    async fn ready_await_after_loop_death_resolves_typed_err() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        client.ready().await.expect("first ready resolves Ok");
        client.bus().on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::UnhandledError,
        );
        let res = tokio::time::timeout(StdDuration::from_secs(2), client.ready())
            .await
            .expect("post-death ready().await hung");
        assert_eq!(res, Err(crate::build::client::ReadyError::LoopFailed));
    }

    #[tokio::test]
    async fn close_before_ready_resolves_client_closed() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        let bus = client.bus().clone();
        let handle = client.close().handle();
        assert_eq!(
            handle.expected_completion,
            crate::types::ExpectedCompletionEvent::ClientClosed
        );

        let env = tokio::time::timeout(
            StdDuration::from_secs(2),
            crate::api::await_request_handle(&bus, handle),
        )
        .await
        .expect("close() before ready() must resolve ClientClosedEvent")
        .expect("await resolved with an event");

        assert_eq!(env.event_type, EventType::ClientClosedEvent);
        assert_eq!(env.request_id, Some(handle.id));
        match env.payload {
            EventPayload::ClientClosedEvent { reason, .. } => {
                assert_eq!(reason, crate::dispatch::ClientCloseReason::UserClose);
            }
            ref other => panic!("unexpected payload {other:?}"),
        }
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn close_with_full_caller_to_io_resolves_interrupted() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder()
                .with_knob("caller_to_io_capacity", KnobValue::U32(1))
                .build()
                .unwrap()
        };
        let bus = client.bus().clone();

        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let _held_rx = bus
            .take_caller_to_io_rx()
            .expect("caller_to_io_rx available");

        {
            let mut g = client.io_reactor_handle.lock().expect("handle lock");
            *g = Some(tokio::spawn(std::future::pending::<()>()));
        }

        bus.try_post_caller_inbound(crate::dispatch::CallerInbound::FsmEvent {
            url: crate::types::WsUrl::Public,
            event: crate::types::CallerEvent::Close { request_id: 0 },
        })
        .expect("first post fills the single queue slot");

        let res = tokio::time::timeout(StdDuration::from_secs(2), client.close())
            .await
            .expect("close() with a full caller_to_io must not hang");
        assert_eq!(res, Err(crate::build::client::CloseError::Interrupted));
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn close_after_loop_death_resolves_interrupted() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        let bus = client.bus().clone();
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());

        drop(
            bus.take_caller_to_io_rx()
                .expect("caller_to_io_rx available"),
        );

        {
            let mut g = client.io_reactor_handle.lock().expect("handle lock");
            *g = Some(tokio::spawn(std::future::pending::<()>()));
        }

        let res = tokio::time::timeout(StdDuration::from_secs(2), client.close())
            .await
            .expect("close() on a dead loop must not hang");
        assert_eq!(res, Err(crate::build::client::CloseError::Interrupted));
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_cancelled_completion_await_releases_its_arms() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        let bus = client.bus().clone();
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());

        let handle = crate::types::RequestHandle {
            id: 4_242,
            expected_completion: crate::types::ExpectedCompletionEvent::ClientClosed,
        };
        {
            let completion = super::completion::close_completion(handle, bus.clone());
            assert_eq!(bus.correlated_live_count(), 1, "construction must arm");
            let mut fut = Box::pin(std::future::IntoFuture::into_future(completion));
            let _ = futures_util::poll!(&mut fut);
        }
        assert_eq!(
            bus.correlated_live_count(),
            0,
            "a cancelled completion await must release its correlated arms"
        );
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn ready_re_entry_after_a_recorded_death_reports_failed() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        let bus = client.bus().clone();
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        {
            let mut g = client.io_reactor_handle.lock().expect("handle lock");
            *g = Some(tokio::spawn(std::future::pending::<()>()));
        }

        bus.begin_shutdown();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Io,
            crate::dispatch::LoopFailureCause::Cancelled,
        );
        assert!(
            !bus.is_loop_failed(),
            "a teardown death must not latch loop_failed — the point of this test"
        );

        let res = tokio::time::timeout(StdDuration::from_secs(2), client.ready())
            .await
            .expect("ready() re-entry must not hang");
        assert_eq!(res, Err(crate::build::client::ReadyError::LoopFailed));
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_cancelled_await_hands_the_terminal_to_a_handle_observer() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        let bus = client.bus().clone();
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());

        let handle = crate::types::RequestHandle {
            id: 7_007,
            expected_completion: crate::types::ExpectedCompletionEvent::ClientClosed,
        };
        let observed = {
            let completion = super::completion::close_completion(handle, bus.clone());
            let observed = completion.handle();
            let mut fut = Box::pin(std::future::IntoFuture::into_future(completion));
            let _ = futures_util::poll!(&mut fut);
            bus.deliver_correlated(&EventEnvelope {
                event_type: EventType::ClientClosedEvent,
                event_version: 1,
                timestamp_monotonic: crate::types::MonotonicInstant::now(),
                request_id: Some(observed.id),
                payload: EventPayload::ClientClosedEvent {
                    reason: crate::dispatch::ClientCloseReason::UserClose,
                    initiated_at_monotonic: crate::types::MonotonicInstant::now(),
                },
            });
            observed
        };

        let env = tokio::time::timeout(
            StdDuration::from_secs(2),
            crate::api::await_request_handle(&bus, observed),
        )
        .await
        .expect("a handle observer must still resolve after the await was cancelled")
        .expect("await resolved with an event");
        assert_eq!(env.request_id, Some(observed.id));
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn a_discarded_ready_does_not_strand_its_terminal() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        let bus = client.bus().clone();
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        {
            let mut g = client.io_reactor_handle.lock().expect("handle lock");
            *g = Some(tokio::spawn(std::future::pending::<()>()));
        }

        for _ in 0..8 {
            drop(client.ready());
        }
        assert_eq!(
            bus.correlated_missed_count(),
            0,
            "discarded ready() completions stranded terminals in the miss buffer"
        );
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn close_reports_a_death_that_lands_mid_drain() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder().build().unwrap()
        };
        let bus = client.bus().clone();
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        {
            let mut g = client.io_reactor_handle.lock().expect("handle lock");
            *g = Some(tokio::spawn(std::future::pending::<()>()));
        }

        let completion = client.close();
        let handle = completion.handle();
        bus.on_loop_death(
            crate::dispatch::ReactorName::Dispatch,
            crate::dispatch::LoopFailureCause::Panic,
        );
        bus.deliver_correlated(&EventEnvelope {
            event_type: EventType::ClientClosedEvent,
            event_version: 1,
            timestamp_monotonic: crate::types::MonotonicInstant::now(),
            request_id: Some(handle.id),
            payload: EventPayload::ClientClosedEvent {
                reason: crate::dispatch::ClientCloseReason::UserClose,
                initiated_at_monotonic: crate::types::MonotonicInstant::now(),
            },
        });

        let res = tokio::time::timeout(StdDuration::from_secs(2), completion)
            .await
            .expect("a mid-drain death must not hang the close await");
        assert_eq!(res, Err(crate::build::client::CloseError::Interrupted));
        bus.stop_reactors();
    }

    #[tokio::test]
    async fn config_resolved_is_emitted_first_at_build() {
        let client = {
            let _g = crate::build::test_env::env_lock();
            Client::builder()
                .with_knob("reconnect_attempts", KnobValue::OptU32(Some(7)))
                .build()
                .unwrap()
        };
        let bus = client.bus();

        let (tx, rx) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell = std::sync::Mutex::new(Some(tx));
        let _ = bus.subscribe(
            EventType::ConfigResolved,
            Arc::new(move |env| {
                if let Some(tx) = tx_cell.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            1,
        );
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());

        let env = tokio::time::timeout(StdDuration::from_secs(2), rx)
            .await
            .expect("ConfigResolved not received within timeout")
            .expect("sender dropped");
        assert_eq!(env.event_type, EventType::ConfigResolved);
        assert_eq!(env.event_version, 1);
        match env.payload {
            EventPayload::ConfigResolved { ref source_map } => {
                assert_eq!(
                    source_map.get("reconnect_attempts"),
                    Some(&crate::build::ConfigSource::Builder)
                );
                assert_eq!(
                    source_map.get("request_timeout_ms"),
                    Some(&crate::build::ConfigSource::Default)
                );
            }
            ref other => panic!("unexpected payload {other:?}"),
        }

        bus.stop_reactors();
    }
}
