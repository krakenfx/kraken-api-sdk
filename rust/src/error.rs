//! Typed error hierarchy. Per-namespace closed-set enums implementing the
//! shared [`ApiError`] cross-binding contract.

use serde::Serialize;

/// Sealed so downstream crates cannot implement [`ApiError`].
pub(crate) mod sealed {
    pub trait Sealed {}
}

/// Boilerplate `ApiError` tail (`request_id`/`message`/`kraken_code`) for
/// namespaced enums with an `Unknown` variant.
macro_rules! api_error_tail {
    () => {
        fn request_id(&self) -> Option<&str> {
            None
        }
        fn message(&self) -> String {
            self.to_string()
        }
        fn kraken_code(&self) -> Option<&str> {
            match self {
                Self::Unknown { kraken_code, .. } => Some(kraken_code),
                _ => None,
            }
        }
    };
    // `carried`: enum hand-writes `request_id()`; emit shared message/kraken_code only.
    (carried) => {
        fn message(&self) -> String {
            self.to_string()
        }
        fn kraken_code(&self) -> Option<&str> {
            match self {
                Self::Unknown { kraken_code, .. } => Some(kraken_code),
                _ => None,
            }
        }
    };
}
pub(crate) use api_error_tail;

/// Stamp per-dispatch correlation id onto listed request-scoped variants.
macro_rules! impl_with_request_id {
    ($enum:ident: $($variant:ident),+ $(,)?) => {
        impl $enum {
            /// Stamp correlation id onto a request-scoped variant (first stamp wins).
            pub(crate) fn with_request_id(mut self, id: &str) -> Self {
                match &mut self {
                    $( $enum::$variant { request_id, .. } )|+
                        if request_id.is_none() =>
                    {
                        *request_id = Some(id.to_string());
                    }
                    _ => {}
                }
                self
            }
        }
    };
}
pub(crate) use impl_with_request_id;

/// Shared constructor + dispatch-miss conversion for namespaced error enums.
macro_rules! impl_error_common {
    ($enum:ident) => {
        impl $enum {
            /// Construct a `MalformedResponse` from a detail string.
            pub(crate) fn malformed(detail: String) -> Self {
                Self::MalformedResponse {
                    detail,
                    request_id: None,
                }
            }
        }

        impl From<crate::dispatch::dispatch_table::DispatchError> for $enum {
            fn from(e: crate::dispatch::dispatch_table::DispatchError) -> Self {
                $enum::Unknown {
                    kraken_code: "INTERNAL".into(),
                    kraken_message: e.to_string(),
                    request_id: None,
                }
            }
        }
    };
}
pub(crate) use impl_error_common;

/// Shared `From<RestError>` for Market/Account (optional namespace-specific arm).
macro_rules! impl_from_rest_error {
    ($enum:ident $(, $extra_pat:literal => $extra:expr)?) => {
        impl From<crate::rest::RestError> for $enum {
            fn from(e: crate::rest::RestError) -> Self {
                use crate::rest::RestError;
                match e {
                    RestError::Transport { kind, transient } => $enum::Transport {
                        kind,
                        transient,
                        request_id: None,
                    },
                    RestError::UnexpectedShape(s) => $enum::malformed(s),
                    RestError::RateLimit(_) => $enum::RateLimited {
                        retry_after_ts: None,
                        request_id: None,
                    },
                    RestError::Auth(a) => $enum::Unknown {
                        kraken_code: "AUTH".into(),
                        kraken_message: a.to_string(),
                        request_id: None,
                    },
                    RestError::Kraken(strings) => {
                        let first = strings.into_iter().next().unwrap_or_default();
                        if crate::error::is_kraken_rate_limit(&first) {
                            $enum::RateLimited {
                                retry_after_ts: crate::error::parse_throttle_until(&first),
                                request_id: None,
                            }
                        } $( else if first.contains($extra_pat) {
                            $extra
                        } )? else if first.contains("Invalid arguments") {
                            // Trim trailing period so the template's `.` is not doubled.
                            $enum::InvalidArguments {
                                detail: first.trim_end_matches('.').to_string(),
                                request_id: None,
                            }
                        } else {
                            $enum::Unknown {
                                kraken_code: crate::error::kraken_eclass_code(&first),
                                kraken_message: first,
                                request_id: None,
                            }
                        }
                    }
                }
            }
        }
    };
}
pub(crate) use impl_from_rest_error;

/// Shared `From<RestError>` taxonomy tests per namespaced enum.
#[cfg(test)]
macro_rules! from_rest_taxonomy_tests {
    ($enum:ident) => {
        fn kraken(msg: &str) -> crate::rest::RestError {
            crate::rest::RestError::Kraken(vec![msg.to_string()])
        }

        #[test]
        fn rate_limit_family_all_map_to_rate_limited() {
            for code in [
                "EAPI:Rate limit exceeded",
                "EAuth:Rate limit exceeded",
                "EOrder:Rate limit exceeded",
                "EService:Throttled: 1700000000",
                "EGeneral:Too many requests",
            ] {
                assert!(
                    matches!($enum::from(kraken(code)), $enum::RateLimited { .. }),
                    "{}: {code} should map to RateLimited",
                    stringify!($enum)
                );
            }
        }

        #[test]
        fn transport_retryable_tracks_transient_flag() {
            use crate::error::ApiError;
            let ambiguous = $enum::from(crate::rest::RestError::Transport {
                kind: crate::transport::TransportErrorKind::RequestSentNoResponse,
                transient: true,
            });
            assert!(
                ambiguous.retryable(),
                "idempotent read: a sent-ambiguous drop is retryable"
            );
            let http_4xx = $enum::from(crate::rest::RestError::Transport {
                kind: crate::transport::TransportErrorKind::HttpStatus { status: 400 },
                transient: false,
            });
            assert!(
                !http_4xx.retryable(),
                "non-transient HTTP 4xx is not retryable"
            );
        }

        #[test]
        fn throttled_rate_limit_carries_retry_after_ts() {
            assert!(matches!(
                $enum::from(kraken("EService:Throttled: 1700000000")),
                $enum::RateLimited {
                    retry_after_ts: Some(1_700_000_000),
                    ..
                }
            ));
            assert!(matches!(
                $enum::from(kraken("EAPI:Rate limit exceeded")),
                $enum::RateLimited {
                    retry_after_ts: None,
                    ..
                }
            ));
        }

        #[test]
        fn invalid_arguments_period_terminated_detail_is_not_doubled() {
            let msg = $enum::from(kraken("EGeneral:Invalid arguments: ordertype must be set."))
                .to_string();
            assert!(
                msg.ends_with('.') && !msg.ends_with(".."),
                "double terminal period: {msg:?}"
            );
        }
    };
}
#[cfg(test)]
pub(crate) use from_rest_taxonomy_tests;

/// Cross-binding error contract: `code` · `category` · `retryable` ·
/// `request_id` · `message` (+ `kraken_code`). Sealed to SDK types.
pub trait ApiError: sealed::Sealed {
    /// Stable machine-readable id (screaming-snake, e.g. `RATE_LIMIT_EXCEEDED`).
    fn code(&self) -> &str;
    /// Coarse category for caller dispatch.
    fn category(&self) -> ErrorCategory;
    /// Whether cooperative retry is appropriate (caller decides; SDK never auto-blocks).
    fn retryable(&self) -> bool;
    /// Per-request correlation id; `None` for builder-time / non-request-scoped.
    fn request_id(&self) -> Option<&str>;
    /// Single-line human message; identical wording across language bindings.
    fn message(&self) -> String;
    /// Raw Kraken wire code; `Some` only on `Unknown`.
    fn kraken_code(&self) -> Option<&str>;
}

/// Coarse error category. `RateLimit` is split from `Network` (different recovery).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    strum::Display,
    strum::AsRefStr,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCategory {
    /// Invalid builder input or rejected runtime knob mutation.
    Config,
    /// Credential, signature, nonce, session-token, or permission failure.
    Auth,
    /// Transport failure (TCP/DNS/TLS/WebSocket); may be transient.
    Network,
    /// Rejection from the Kraken exchange.
    Exchange,
    /// Client-side state or usage error.
    Client,
    /// Rate-limit or throttle rejection.
    RateLimit,
}

/// Events-namespace errors — primarily registration rejections. Closed set.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum EventsError {
    /// Event bus torn down; no further registrations accepted.
    #[error("Event bus is closed.")]
    BusClosed,
    /// Event-type name not in the SDK catalogue.
    #[error("Invalid event type: {event_type}.")]
    InvalidEventType {
        /// Rejected event-type string.
        event_type: String,
    },
    /// Client closed; no new event operations accepted.
    #[error("Client is closed; no new operations accepted.")]
    ClientClosed,
    /// Reactor loop dead; rebuild the client.
    #[error(
        "The client's internal loop has failed; no new event subscriptions are accepted. Rebuild the client."
    )]
    LoopDead,
    /// Unclassified Kraken error; raw passthrough text.
    #[error("{kraken_message}")]
    Unknown {
        /// Raw Kraken error code.
        kraken_code: String,
        /// Raw Kraken error text (display message).
        kraken_message: String,
    },
}

impl sealed::Sealed for EventsError {}

impl ApiError for EventsError {
    fn code(&self) -> &str {
        match self {
            EventsError::BusClosed | EventsError::ClientClosed => "CLIENT_CLOSED",
            EventsError::LoopDead => "LOOP_DEAD",
            EventsError::InvalidEventType { .. } => "INVALID_ARGUMENTS",
            EventsError::Unknown { .. } => "UNKNOWN",
        }
    }
    fn category(&self) -> ErrorCategory {
        match self {
            EventsError::Unknown { .. } => ErrorCategory::Exchange,
            _ => ErrorCategory::Client,
        }
    }
    fn retryable(&self) -> bool {
        false
    }
    api_error_tail!();
}

/// Connection-layer failure for an in-flight WS request awaiter.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind}")]
pub struct ConnectionError {
    kind: ConnectionErrorKind,
}

/// Cause of a [`ConnectionError`]. Closed set; stable cross-binding messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectionErrorKind {
    /// Auth connection not open at compose time; nothing was sent.
    #[error("Connection is not open; the request was not sent.")]
    NotOpen,
    /// Connection dropped while the request was in flight.
    #[error("Connection dropped with the request in flight.")]
    RequestInFlightWhenDropped,
    /// Client closed while the request was in flight.
    #[error("Client is closed; the in-flight request was abandoned.")]
    ClientClosed,
    /// I/O loop went away before answering.
    #[error("The connection loop closed before the request was answered.")]
    LoopClosed,
    /// `caller_to_io` full at post — definitely-not-sent (distinct from `NotOpen`).
    #[error("The operation queue is full; the request was rejected.")]
    QueueFull,
    /// Response deadline elapsed. SENT-AMBIGUOUS — may have reached the wire.
    #[error("The request deadline elapsed before a response was received.")]
    ResponseTimeout,
}

impl ConnectionError {
    pub(crate) fn not_open() -> Self {
        Self {
            kind: ConnectionErrorKind::NotOpen,
        }
    }

    pub(crate) fn request_in_flight_when_dropped() -> Self {
        Self {
            kind: ConnectionErrorKind::RequestInFlightWhenDropped,
        }
    }

    pub(crate) fn client_closed() -> Self {
        Self {
            kind: ConnectionErrorKind::ClientClosed,
        }
    }

    pub(crate) fn loop_closed() -> Self {
        Self {
            kind: ConnectionErrorKind::LoopClosed,
        }
    }

    pub(crate) fn queue_full() -> Self {
        Self {
            kind: ConnectionErrorKind::QueueFull,
        }
    }

    pub(crate) fn response_timeout() -> Self {
        Self {
            kind: ConnectionErrorKind::ResponseTimeout,
        }
    }

    #[cfg(test)]
    pub(crate) fn kind(&self) -> ConnectionErrorKind {
        self.kind
    }

    /// Definitely not sent (`NotOpen` or `QueueFull`).
    pub(crate) fn is_definitely_not_sent(&self) -> bool {
        matches!(
            self.kind,
            ConnectionErrorKind::NotOpen | ConnectionErrorKind::QueueFull
        )
    }

    /// Full `caller_to_io` queue rejection.
    pub(crate) fn is_queue_full(&self) -> bool {
        matches!(self.kind, ConnectionErrorKind::QueueFull)
    }

    /// Per-request response-deadline timeout.
    pub(crate) fn is_response_timeout(&self) -> bool {
        matches!(self.kind, ConnectionErrorKind::ResponseTimeout)
    }
}

/// `EClass` token from `EClass:Detail`; whole string if no `:`.
pub(crate) fn kraken_eclass_code(msg: &str) -> String {
    msg.split_once(':')
        .map_or_else(|| msg.to_string(), |(eclass, _)| eclass.to_string())
}

/// Trailing Unix-seconds from `EService:Throttled: <ts>`, else `None`.
pub(crate) fn parse_throttle_until(code: &str) -> Option<u64> {
    code.strip_prefix("EService:Throttled:")
        .and_then(|suffix| suffix.trim().parse::<u64>().ok())
}

/// Canonical rate-limit / throttle substring check (shared by all classifiers).
pub(crate) fn is_kraken_rate_limit(msg: &str) -> bool {
    msg.contains("Rate limit") || msg.contains("Throttled") || msg.contains("Too many requests")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_error_apierror_mapping_matches_a218() {
        let busclosed = EventsError::BusClosed;
        assert_eq!(busclosed.code(), "CLIENT_CLOSED");
        assert_eq!(busclosed.category(), ErrorCategory::Client);
        assert!(!busclosed.retryable());
        assert_eq!(busclosed.message(), "Event bus is closed.");
        assert_eq!(busclosed.request_id(), None);

        let bad = EventsError::InvalidEventType {
            event_type: "frobnicate".into(),
        };
        assert_eq!(bad.code(), "INVALID_ARGUMENTS");
        assert_eq!(bad.message(), "Invalid event type: frobnicate.");

        let unk = EventsError::Unknown {
            kraken_code: "EWeird:Thing".into(),
            kraken_message: "weird thing".into(),
        };
        assert_eq!(unk.code(), "UNKNOWN");
        assert_eq!(unk.category(), ErrorCategory::Exchange);
        assert_eq!(unk.kraken_code(), Some("EWeird:Thing"));
        assert_eq!(unk.message(), "weird thing");
    }

    #[test]
    fn kraken_eclass_code_splits_on_first_colon() {
        assert_eq!(kraken_eclass_code("EOrder:Insufficient funds"), "EOrder");
        assert_eq!(
            kraken_eclass_code("EGeneral:Invalid arguments:volume"),
            "EGeneral"
        );
        assert_eq!(kraken_eclass_code("no colon here"), "no colon here");
        assert_eq!(kraken_eclass_code(""), "");
    }
}

/// Golden-vector `message()` quality contract across the `ApiError` surface.
#[cfg(test)]
mod golden_vectors {
    use super::{ApiError, ErrorCategory, EventsError};
    use crate::api::account::AccountError;
    use crate::api::market::MarketError;
    use crate::api::trade::{OrderError, TradeError};
    use crate::api::{SubscriptionError, SystemStatus};
    use crate::rate_limit::RateLimitError;
    use crate::transport::{TransportError, TransportErrorKind};
    use crate::types::{ChannelName, ClOrdId, Symbol};
    use crate::{AuthError, CloseError, ConfigError, ReadyError};

    #[test]
    fn error_category_strum_display_matches_serde_emission() {
        for c in [
            ErrorCategory::Config,
            ErrorCategory::Auth,
            ErrorCategory::Network,
            ErrorCategory::Exchange,
            ErrorCategory::Client,
            ErrorCategory::RateLimit,
        ] {
            assert_eq!(serde_json::json!(c), serde_json::json!(c.to_string()));
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Class {
        /// SDK-composed message held to the quality contract.
        Contract,
        /// Passthrough / Debug-dump / foreign-lib text the SDK does not own.
        Excluded,
    }

    struct Vector {
        name: &'static str,
        code: String,
        message: String,
        class: Class,
    }

    fn vector<E: ApiError>(name: &'static str, e: E, class: Class) -> Vector {
        Vector {
            name,
            code: e.code().to_string(),
            message: e.message(),
            class,
        }
    }

    fn starts_ok(msg: &str) -> bool {
        matches!(msg.chars().next(), Some(c) if c.is_ascii_uppercase() || c == '`')
    }

    fn ends_ok(msg: &str) -> bool {
        let last = msg.rsplit(char::is_whitespace).next().unwrap_or("");
        if last.starts_with("http://") || last.starts_with("https://") {
            return true;
        }
        msg.ends_with('.') && !msg.ends_with("..")
    }

    fn assert_contract(name: &str, msg: &str) {
        assert!(!msg.is_empty(), "{name}: message is empty");
        assert_eq!(
            msg.trim(),
            msg,
            "{name}: leading/trailing whitespace: {msg:?}"
        );
        assert!(
            !msg.contains('\n'),
            "{name}: message is not single-line: {msg:?}"
        );
        assert!(
            !msg.contains('{') && !msg.contains('}'),
            "{name}: unsubstituted template brace: {msg:?}"
        );
        assert!(
            starts_ok(msg),
            "{name}: must start with a capital letter or a `code-span`: {msg:?}"
        );
        assert!(
            ends_ok(msg),
            "{name}: must end with exactly one '.' or an actionable URL: {msg:?}"
        );
    }

    fn assert_excluded(name: &str, msg: &str) {
        assert!(!msg.is_empty(), "{name}: excluded message is empty");
    }

    fn check(vectors: &[Vector]) {
        for v in vectors {
            match v.class {
                Class::Contract => assert_contract(v.name, &v.message),
                Class::Excluded => assert_excluded(v.name, &v.message),
            }
        }
    }

    fn events_class(e: &EventsError) -> Class {
        use EventsError::*;
        match e {
            Unknown { .. } => Class::Excluded,
            BusClosed | InvalidEventType { .. } | ClientClosed | LoopDead => Class::Contract,
        }
    }

    fn ready_class(e: &ReadyError) -> Class {
        use ReadyError::*;
        match e {
            ReactorSpawnFailed | LoopFailed => Class::Contract,
        }
    }

    fn close_class(e: &CloseError) -> Class {
        use CloseError::*;
        match e {
            Interrupted => Class::Contract,
        }
    }

    fn transport_class(k: &TransportErrorKind) -> Class {
        use TransportErrorKind::*;
        match k {
            Other(_) => Class::Excluded,
            TcpConnectTimeout
            | TcpRefused
            | DnsFailure
            | TlsHandshakeFailure
            | SocketReset
            | RequestSentNoResponse
            | Ssl
            | CloseFrame { .. }
            | AbnormalClose { .. }
            | HttpUpgradeRejected { .. }
            | HttpStatus { .. } => Class::Contract,
        }
    }

    fn auth_class(e: &AuthError) -> Class {
        use AuthError::*;
        match e {
            Unknown { .. } => Class::Excluded,
            InvalidKey
            | InvalidSignature
            | InvalidNonce
            | PermissionDenied { .. }
            | TemporaryLockout
            | TokenStale
            | TokenRefreshFailed
            | TokenRefreshTransient
            | ClientClosed => Class::Contract,
        }
    }

    fn rate_limit_class(e: &RateLimitError) -> Class {
        use RateLimitError::*;
        match e {
            Unknown { .. } => Class::Excluded,
            ApiCounterExceeded { .. }
            | TradingCounterExceeded { .. }
            | ServiceThrottled { .. }
            | ClientClosed => Class::Contract,
        }
    }

    fn subscription_class(e: &SubscriptionError) -> Class {
        use SubscriptionError::*;
        match e {
            Unknown { .. } => Class::Excluded,
            NoHandlerRegistered { .. }
            | QueueFull
            | ClientClosed
            | LoopDead
            | NotImplemented { .. } => Class::Contract,
        }
    }

    fn trade_class(e: &TradeError) -> Class {
        use TradeError::*;
        match e {
            Transport { .. } | Unknown { .. } => Class::Excluded,
            InsufficientFunds { .. }
            | InsufficientMargin { .. }
            | InvalidOrder { .. }
            | ConflictingOrderIdentifiers
            | SettlePositionRequiresLeverage
            | ReduceOnlyRequiresLeverage
            | EmptyAmendRequest
            | NoAmendableParameters { .. }
            | UnknownOrder { .. }
            | BatchSizeOutOfRange { .. }
            | SystemStatusBlocked { .. }
            | RateLimited { .. }
            | MalformedResponse { .. }
            | ClientClosed
            | LoopDead
            | WsUnsupportedOrderField { .. }
            | RestUnsupportedOrderField { .. }
            | UnsupportedTransport { .. }
            | WsSurfaceUnavailable
            | QueueFull { .. } => Class::Contract,
        }
    }

    fn market_class(e: &MarketError) -> Class {
        use MarketError::*;
        match e {
            Transport { .. } | Unknown { .. } => Class::Excluded,
            SymbolNotFound { .. }
            | InvalidArguments { .. }
            | SystemStatus { .. }
            | RateLimited { .. }
            | MalformedResponse { .. }
            | ClientClosed => Class::Contract,
        }
    }

    fn account_class(e: &AccountError) -> Class {
        use AccountError::*;
        match e {
            Transport { .. } | Unknown { .. } => Class::Excluded,
            PermissionDenied { .. }
            | InvalidArguments { .. }
            | RateLimited { .. }
            | MalformedResponse { .. }
            | ClientClosed => Class::Contract,
        }
    }

    fn config_class(e: &ConfigError) -> Class {
        use ConfigError::*;
        match e {
            Unknown { .. } => Class::Excluded,
            MissingCredentials
            | InvalidCredentials
            | WsAuthRequiresCredentials
            | ImmutableKnob { .. }
            | InvalidCLOrdId
            | NonceConflict
            | FeatureDisabled { .. }
            | EmptyAmendRequest
            | InsecureEndpointScheme { .. }
            | HeadersRequireSdkTransport
            | ReservedHeaderName { .. }
            | InvalidConfig { .. }
            | ClientClosed => Class::Contract,
        }
    }

    fn passthrough(code: &str, msg: &str) -> (String, String) {
        (code.to_string(), msg.to_string())
    }

    fn system_status() -> SystemStatus {
        SystemStatus {
            status: "maintenance".into(),
            timestamp: "2026-06-26T00:00:00Z".into(),
        }
    }

    fn online_status() -> SystemStatus {
        SystemStatus {
            status: "online".into(),
            timestamp: "2026-06-26T00:00:00Z".into(),
        }
    }

    fn all_vectors() -> Vec<Vector> {
        let mut out: Vec<Vector> = Vec::new();
        let (kc, km) = passthrough("EGeneral:Internal error", "Kraken said no.");

        for e in [
            EventsError::BusClosed,
            EventsError::InvalidEventType {
                event_type: "frobnicate".into(),
            },
            EventsError::ClientClosed,
            EventsError::LoopDead,
            EventsError::Unknown {
                kraken_code: kc.clone(),
                kraken_message: km.clone(),
            },
        ] {
            let c = events_class(&e);
            out.push(vector("EventsError", e, c));
        }

        for k in [
            TransportErrorKind::TcpConnectTimeout,
            TransportErrorKind::TcpRefused,
            TransportErrorKind::DnsFailure,
            TransportErrorKind::TlsHandshakeFailure,
            TransportErrorKind::SocketReset,
            TransportErrorKind::RequestSentNoResponse,
            TransportErrorKind::Ssl,
            TransportErrorKind::CloseFrame {
                code: 1006,
                reason: "going away".into(),
            },
            TransportErrorKind::AbnormalClose {
                context: "tcp reset".into(),
            },
            TransportErrorKind::HttpUpgradeRejected { status: 503 },
            TransportErrorKind::HttpStatus { status: 503 },
            TransportErrorKind::Other("hyper: connection closed".into()),
        ] {
            let c = transport_class(&k);
            out.push(vector(
                "TransportError",
                TransportError {
                    kind: k,
                    transient: true,
                },
                c,
            ));
        }

        for e in [
            AuthError::InvalidKey,
            AuthError::InvalidSignature,
            AuthError::InvalidNonce,
            AuthError::PermissionDenied { scope: None },
            AuthError::TemporaryLockout,
            AuthError::TokenStale,
            AuthError::TokenRefreshFailed,
            AuthError::TokenRefreshTransient,
            AuthError::ClientClosed,
            AuthError::Unknown {
                kraken_code: kc.clone(),
                kraken_message: km.clone(),
            },
        ] {
            let c = auth_class(&e);
            out.push(vector("AuthError", e, c));
        }

        for e in [
            RateLimitError::ApiCounterExceeded {
                scope: None,
                retry_after_ts: None,
            },
            RateLimitError::TradingCounterExceeded {
                scope: Some("BTC/USD".into()),
                retry_after_ts: Some(1_700_000_000),
            },
            RateLimitError::ServiceThrottled {
                retry_after_ts: None,
            },
            RateLimitError::ClientClosed,
            RateLimitError::Unknown {
                kraken_code: kc.clone(),
                kraken_message: km.clone(),
            },
        ] {
            let c = rate_limit_class(&e);
            out.push(vector("RateLimitError", e, c));
        }

        for e in [
            SubscriptionError::NoHandlerRegistered {
                channel: ChannelName::Ticker,
            },
            SubscriptionError::QueueFull,
            SubscriptionError::ClientClosed,
            SubscriptionError::LoopDead,
            SubscriptionError::NotImplemented {
                method: "subscribe_level3",
            },
            SubscriptionError::Unknown {
                kraken_code: kc.clone(),
                kraken_message: km.clone(),
            },
        ] {
            let c = subscription_class(&e);
            out.push(vector("SubscriptionError", e, c));
        }

        out.push(vector(
            "OrderError",
            OrderError {
                request_id: None,
                cl_ord_id: ClOrdId::new("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                code: "EOrder:Insufficient funds".into(),
                message: "Insufficient funds.".into(),
                retryable: false,
                category: ErrorCategory::Exchange,
            },
            Class::Excluded,
        ));

        for e in [
            TradeError::InsufficientFunds { request_id: None },
            TradeError::InsufficientMargin { request_id: None },
            TradeError::InvalidOrder {
                request_id: None,
                detail: "volume too small".into(),
            },
            TradeError::ConflictingOrderIdentifiers,
            TradeError::SettlePositionRequiresLeverage,
            TradeError::ReduceOnlyRequiresLeverage,
            TradeError::EmptyAmendRequest,
            TradeError::NoAmendableParameters { request_id: None },
            TradeError::UnknownOrder { request_id: None },
            TradeError::BatchSizeOutOfRange { min: 1, max: 100 },
            TradeError::SystemStatusBlocked {
                current: system_status(),
                required: online_status(),
            },
            TradeError::RateLimited {
                request_id: None,
                retry_after_ts: None,
            },
            TradeError::Transport {
                request_id: None,
                kind: TransportErrorKind::Ssl,
                transient: false,
            },
            TradeError::MalformedResponse {
                request_id: None,
                detail: "bad json".into(),
            },
            TradeError::ClientClosed,
            TradeError::LoopDead,
            TradeError::WsUnsupportedOrderField { field: "validate" },
            TradeError::RestUnsupportedOrderField { field: "margin" },
            TradeError::UnsupportedTransport {
                transport: crate::dispatch::Transport::WsV2Public,
                legal: &[
                    crate::dispatch::Transport::Rest,
                    crate::dispatch::Transport::WsV2Auth,
                ],
            },
            TradeError::UnsupportedTransport {
                transport: crate::dispatch::Transport::WsV2Auth,
                legal: &[crate::dispatch::Transport::Rest],
            },
            TradeError::WsSurfaceUnavailable,
            TradeError::QueueFull { request_id: None },
            TradeError::Unknown {
                request_id: None,
                kraken_code: kc.clone(),
                kraken_message: km.clone(),
            },
        ] {
            let c = trade_class(&e);
            out.push(vector("TradeError", e, c));
        }

        for e in [
            MarketError::SymbolNotFound {
                request_id: None,
                symbol: "BTC/USD".into(),
            },
            MarketError::InvalidArguments {
                request_id: None,
                detail: "bad pair".into(),
            },
            MarketError::SystemStatus {
                current: system_status(),
                required: online_status(),
            },
            MarketError::RateLimited {
                request_id: None,
                retry_after_ts: None,
            },
            MarketError::Transport {
                request_id: None,
                kind: TransportErrorKind::Ssl,
                transient: false,
            },
            MarketError::MalformedResponse {
                request_id: None,
                detail: "bad json".into(),
            },
            MarketError::ClientClosed,
            MarketError::Unknown {
                request_id: None,
                kraken_code: kc.clone(),
                kraken_message: km.clone(),
            },
        ] {
            let c = market_class(&e);
            out.push(vector("MarketError", e, c));
        }

        for e in [
            AccountError::PermissionDenied {
                request_id: None,
                scope: None,
            },
            AccountError::InvalidArguments {
                request_id: None,
                detail: "bad ofs".into(),
            },
            AccountError::RateLimited {
                request_id: None,
                retry_after_ts: None,
            },
            AccountError::Transport {
                request_id: None,
                kind: TransportErrorKind::Ssl,
                transient: false,
            },
            AccountError::MalformedResponse {
                request_id: None,
                detail: "bad json".into(),
            },
            AccountError::ClientClosed,
            AccountError::Unknown {
                request_id: None,
                kraken_code: kc.clone(),
                kraken_message: km.clone(),
            },
        ] {
            let c = account_class(&e);
            out.push(vector("AccountError", e, c));
        }

        for e in [
            ConfigError::MissingCredentials,
            ConfigError::InvalidCredentials,
            ConfigError::WsAuthRequiresCredentials,
            ConfigError::ImmutableKnob {
                knob: "max_in_flight".into(),
            },
            ConfigError::InvalidCLOrdId,
            ConfigError::NonceConflict,
            ConfigError::FeatureDisabled {
                feature: "paper_trading".into(),
            },
            ConfigError::EmptyAmendRequest,
            ConfigError::ClientClosed,
            ConfigError::InsecureEndpointScheme {
                knob: "rest_base_url".into(),
                scheme: "https".into(),
            },
            ConfigError::HeadersRequireSdkTransport,
            ConfigError::ReservedHeaderName {
                name: "api-key".into(),
            },
            ConfigError::InvalidConfig {
                detail: "config file parse failed".into(),
            },
            ConfigError::Unknown {
                kraken_code: kc.clone(),
                kraken_message: km.clone(),
            },
        ] {
            let c = config_class(&e);
            out.push(vector("ConfigError", e, c));
        }

        for e in [ReadyError::ReactorSpawnFailed, ReadyError::LoopFailed] {
            let c = ready_class(&e);
            out.push(vector("ReadyError", e, c));
        }

        let close_err = CloseError::Interrupted;
        let close_c = close_class(&close_err);
        out.push(vector("CloseError", close_err, close_c));

        out
    }

    #[test]
    fn message_quality_contract_holds_across_apierror_surface() {
        let vectors = all_vectors();
        // Exact variant count of the full ApiError surface — a dropped or
        // un-pinned addition trips this.
        assert_eq!(
            vectors.len(),
            94,
            "ApiError surface variant count changed — add the new variant's instance to all_vectors() and pin its message"
        );
        check(&vectors);
    }

    #[test]
    fn symbol_display_renders_wire_string() {
        // Symbol::Display renders the bare wire string (no Debug wrapper) — a
        // cross-binding surface used inside error messages.
        assert_eq!(Symbol::new("BTC/USD").unwrap().to_string(), "BTC/USD");
    }

    /// The byte-identical cross-binding reference: every rendered Contract-class
    /// `message()` is pinned here; the Phase-2 ports MUST reproduce these strings
    /// exactly. A diff is a contract change, not a test fix.
    #[test]
    fn contract_message_set_is_byte_pinned() {
        use std::collections::BTreeSet;

        // Pin the (code, message) PAIR, not just the message, so a variant
        // remapped onto another's code/string is caught. Residual: this is a SET,
        // so remaps onto intentionally-shared pairs (CLIENT_CLOSED, MALFORMED_RESPONSE) dedup.
        let expected: BTreeSet<(&str, &str)> = [
            ("AUTH_REFRESH_FAILED", "Token refresh failed."),
            ("AUTH_REFRESH_FAILED", "Token stale; refresh required."),
            (
                "AUTH_REFRESH_TRANSIENT",
                "Transient network failure during token refresh; retrying.",
            ),
            ("BAD_CREDS", "Invalid credentials."),
            ("BAD_CREDS", "Missing credentials."),
            ("BAD_CREDS", "WS auth requires credentials."),
            ("CLIENT_CLOSED", "Client is closed; no new operations accepted."),
            ("CLIENT_CLOSED", "Event bus is closed."),
            (
                "CLOSE_INTERRUPTED",
                "The client's internal loop has failed during shutdown; the connection drain may not have completed.",
            ),
            ("CONNECTION_ERROR", "DNS resolution failed."),
            ("CONNECTION_ERROR", "Request sent; no response."),
            ("CONNECTION_ERROR", "SSL error."),
            ("CONNECTION_ERROR", "Socket reset."),
            ("CONNECTION_ERROR", "TCP connect timeout."),
            ("CONNECTION_ERROR", "TCP connection refused."),
            ("CONNECTION_ERROR", "TLS handshake failed."),
            ("CONNECTION_ERROR", "WS abnormal close: tcp reset."),
            ("CONNECTION_ERROR", "WS close 1006: going away."),
            ("CONNECTION_ERROR", "WS upgrade rejected: HTTP 503."),
            ("CONNECTION_ERROR", "HTTP 503."),
            ("FEATURE_DISABLED", "Feature paper_trading is disabled."),
            (
                "HEADERS_REQUIRE_SDK_TRANSPORT",
                "Custom headers cannot be combined with an injected transport; stamp them on the injected transport instead.",
            ),
            (
                "INSECURE_ENDPOINT_SCHEME",
                "`rest_base_url` must use the `https` (TLS) scheme; cleartext or non-TLS endpoint URLs are refused at build time.",
            ),
            ("INSUFFICIENT_FUNDS", "Insufficient funds."),
            ("INSUFFICIENT_MARGIN", "Insufficient margin."),
            (
                "INVALID_ARGUMENTS",
                "`reduce_only` requires a margin order or leverage > 1.",
            ),
            (
                "INVALID_ARGUMENTS",
                "`settle-position` requires leverage >= 1.",
            ),
            ("INVALID_ARGUMENTS", "Amend request is empty."),
            (
                "INVALID_ARGUMENTS",
                "Batch size out of range; require 1-100 orders.",
            ),
            ("INVALID_ARGUMENTS", "Invalid arguments: bad ofs."),
            ("INVALID_ARGUMENTS", "Invalid arguments: bad pair."),
            ("INVALID_ARGUMENTS", "Invalid event type: frobnicate."),
            (
                "INVALID_ARGUMENTS",
                "Knob max_in_flight is immutable post-build.",
            ),
            (
                "INVALID_ARGUMENTS",
                "No handler registered for the ticker channel; register one before subscribing.",
            ),
            (
                "INVALID_ARGUMENTS",
                "Order field `validate` is not supported on the WS transport in v1; use `.via(Transport::Rest)`.",
            ),
            (
                "INVALID_ARGUMENTS",
                "Order field `margin` is not supported on the REST transport in v1; use `.via(Transport::WsV2Auth)`.",
            ),
            ("INVALID_ARGUMENTS", "Symbol not found: BTC/USD."),
            (
                "INVALID_ARGUMENTS",
                "Transport WsV2Public cannot serve this spot order operation; use `.via(Transport::Rest)` or `.via(Transport::WsV2Auth)`.",
            ),
            (
                "INVALID_ARGUMENTS",
                "Transport WsV2Auth cannot serve this spot order operation; use `.via(Transport::Rest)`.",
            ),
            (
                "INVALID_ARGUMENTS",
                "No WS surface is wired on this client; use `.via(Transport::Rest)` for order operations.",
            ),
            (
                "INVALID_CL_ORD_ID",
                "Both `userref` and `cl_ord_id` set; pick one.",
            ),
            ("INVALID_CL_ORD_ID", "Invalid cl_ord_id."),
            (
                "INVALID_CONFIG",
                "Invalid configuration: config file parse failed.",
            ),
            ("INVALID_KEY", "Invalid API key."),
            ("INVALID_NONCE", "Invalid nonce."),
            ("INVALID_ORDER", "Invalid order: volume too small."),
            ("INVALID_SIGNATURE", "Invalid signature."),
            (
                "LOOP_DEAD",
                "The client's internal loop has failed; no new event subscriptions are accepted. Rebuild the client.",
            ),
            (
                "LOOP_DEAD",
                "The client's internal loop has failed; streaming is unavailable. Rebuild the client.",
            ),
            (
                "LOOP_DEAD",
                "The client's internal loop has failed; new WS operations are rejected. Use `.via(Transport::Rest)` to cancel or de-risk, then rebuild the client.",
            ),
            (
                "LOOP_DEAD",
                "The client's internal loop has failed; the client is not ready. Rebuild the client.",
            ),
            ("MALFORMED_RESPONSE", "The server response could not be parsed."),
            ("NONCE_CONFLICT", "Nonce conflict."),
            (
                "NON_AMENDABLE_FIELD",
                "Amend rejected: no amendable parameters (new value matched current, e.g. a sub-tick price).",
            ),
            (
                "NOT_IMPLEMENTED",
                "Method `subscribe_level3` is not implemented in v1 (deferred to a v1.x slice).",
            ),
            (
                "ORDER_NOT_FOUND",
                "Order not found; no open order matches the cl_ord_id.",
            ),
            ("PERMISSION_DENIED", "Permission denied."),
            ("QUEUE_FULL", "Order send queue is full; back off and retry."),
            (
                "QUEUE_FULL",
                "The operation queue is full; the subscribe request was rejected.",
            ),
            ("RATE_LIMIT_EXCEEDED", "API rate-limit counter exceeded."),
            ("RATE_LIMIT_EXCEEDED", "Rate limited."),
            ("RATE_LIMIT_EXCEEDED", "Trading rate-limit counter exceeded."),
            (
                "REACTOR_SPAWN_FAILED",
                "The client's internal loop could not be started.",
            ),
            (
                "RESERVED_HEADER_NAME",
                "Header `api-key` is reserved by the SDK and cannot be set via with_headers.",
            ),
            ("SERVICE_THROTTLED", "Service throttled."),
            (
                "SYSTEM_STATUS_MAINTENANCE",
                "Operation blocked: exchange system status is \"maintenance\", but this operation requires \"online\" — retry once the status returns to the required level.",
            ),
            (
                "TEMPORARY_LOCKOUT",
                "Temporary lockout; too many sequential auth failures — retry after the cooldown (~15 min).",
            ),
        ]
        .into_iter()
        .collect();

        let vectors = all_vectors();
        let got: BTreeSet<(&str, &str)> = vectors
            .iter()
            .filter(|v| v.class == Class::Contract)
            .map(|v| (v.code.as_str(), v.message.as_str()))
            .collect();

        assert_eq!(
            got, expected,
            "Contract-class (code, message) set drifted from the byte-identical cross-binding reference (§8.A460). \
             A diff is a CONTRACT CHANGE: update the pinned set here AND propagate to the 5 language ports."
        );
    }
}
