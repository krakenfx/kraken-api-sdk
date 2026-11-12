//! [`TradeError`] — trade-namespace error taxonomy + classification.

use super::super::SystemStatus;
use crate::dispatch::Transport;
use crate::rest::RestError;
use crate::transport::TransportErrorKind;
use serde::Serialize;
use serde_with::skip_serializing_none;

fn remedy(legal: &[Transport]) -> String {
    legal
        .iter()
        .map(|t| format!("`.via(Transport::{t:?})`"))
        .collect::<Vec<_>>()
        .join(" or ")
}

/// Errors returned by trade-namespace operations.
// `Eq` is not derived: `Transport { kind }` carries non-Eq context in some kinds.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum TradeError {
    /// Exchange rejected the order: the account balance cannot cover it (`EOrder:Insufficient funds`).
    #[error("Insufficient funds.")]
    InsufficientFunds {
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// Exchange rejected the order: free margin cannot cover it (Spot `EOrder:Insufficient margin`,
    /// Futures `Other:INSUFFICIENT_MARGIN`).
    #[error("Insufficient margin.")]
    InsufficientMargin {
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// The request is invalid — Kraken `EOrder:*` / `Invalid arguments` rejections or an SDK-side pre-send rejection. Non-retryable.
    #[error("Invalid order: {detail}.")]
    InvalidOrder {
        /// Rejection detail: Kraken's error string (trailing period trimmed) or an SDK-composed reason.
        detail: String,
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// Client-side rejection before send: set `userref` or `cl_ord_id`, never both.
    #[error("Both `userref` and `cl_ord_id` set; pick one.")]
    ConflictingOrderIdentifiers,
    /// Client-side rejection before send: `settle-position` orders need leverage of at least 1.
    #[error("`settle-position` requires leverage >= 1.")]
    SettlePositionRequiresLeverage,
    /// `reduce_only` only makes sense against an open margin position.
    #[error("`reduce_only` requires a margin order or leverage > 1.")]
    ReduceOnlyRequiresLeverage,
    /// Client-side rejection before send: the amend carried no changes; nothing was sent.
    #[error("Amend request is empty.")]
    EmptyAmendRequest,
    /// Amend was a no-op — the new value equalled the current (e.g. sub-tick). Non-retryable.
    #[error(
        "Amend rejected: no amendable parameters (new value matched current, e.g. a sub-tick price)."
    )]
    NoAmendableParameters {
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// An amend or cancel targeted a `cl_ord_id` (on Futures, or an `order_id`) matching no
    /// open order (on Futures, also a filled or already-cancelled one). Non-retryable.
    #[error("Order not found; no open order matches the given cl_ord_id or order_id.")]
    UnknownOrder {
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// Client-side rejection before send: batch entry count is outside the accepted range.
    #[error("Batch size out of range; require {min}-{max} orders.")]
    BatchSizeOutOfRange {
        /// Smallest accepted batch size (inclusive).
        min: u32,
        /// Largest accepted batch size (inclusive).
        max: u32,
    },
    /// The exchange system status forbids this operation (e.g. maintenance); retry after recovery.
    #[error(
        "Operation blocked: exchange system status is \"{}\", but this operation \
         requires \"{}\" — retry once the status returns to the required level.",
        current.status,
        required.status
    )]
    SystemStatusBlocked {
        /// Exchange system status observed when the operation was attempted.
        current: SystemStatus,
        /// System status the operation requires to proceed.
        required: SystemStatus,
    },
    /// Rate limited — Kraken `Rate limit` / `Throttled` / `Too many requests` strings or the SDK's own pre-send limiter; back off, then retry.
    #[error("Rate limited.")]
    RateLimited {
        /// Unix seconds to retry after: parsed from `EService:Throttled: <ts>`, or local wall clock plus a Futures
        /// reply's `retry_after_ms`, the sum rounded up to whole seconds; `None` when neither is available.
        retry_after_ts: Option<u64>,
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// Transport-layer failure; retryable only when `transient` and not sent-ambiguous.
    #[error("Transport: {kind:?}")]
    Transport {
        /// Failure classification; sent-ambiguous kinds are never auto-retried.
        kind: TransportErrorKind,
        /// Whether the transport judged the failure transient (a retry could plausibly succeed).
        transient: bool,
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// SDK-side decode failure — the server response could not be parsed.
    #[error("The server response could not be parsed.")]
    MalformedResponse {
        /// Decoder's description of what failed to parse; diagnostic only, not part of the Display message.
        detail: String,
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// The client was closed by the caller; the operation was rejected before send.
    #[error("Client is closed; no new operations accepted.")]
    ClientClosed,
    /// The reactor loop has died; new WS operations are rejected. REST stays
    /// available so the caller can de-risk before rebuilding.
    #[error(
        "The client's internal loop has failed; new WS operations are rejected. \
         Use `.via(Transport::Rest)` to cancel or de-risk, then rebuild the client."
    )]
    LoopDead,
    /// Order field `{field}` cannot be expressed on the WS v2 transport in v1.
    /// Use `.via(Transport::Rest)` to send this order.
    #[error(
        "Order field `{field}` is not supported on the WS transport in v1; \
         use `.via(Transport::Rest)`."
    )]
    WsUnsupportedOrderField {
        /// Name of the order field that cannot be expressed on this transport.
        field: &'static str,
    },
    /// Order field `{field}` is WS-native (e.g. `margin`) and cannot be expressed
    /// on the REST transport. Use `.via(Transport::WsV2Auth)`.
    #[error(
        "Order field `{field}` is not supported on the REST transport in v1; \
         use `.via(Transport::WsV2Auth)`."
    )]
    RestUnsupportedOrderField {
        /// Name of the order field that cannot be expressed on this transport.
        field: &'static str,
    },
    /// An explicit `.via(...)` selected a [`Transport`] with no dispatch entry
    /// for this op. Never sent; the message names only legal transports.
    #[error(
        "Transport {transport:?} cannot serve this order operation; use {}.",
        remedy(.legal)
    )]
    UnsupportedTransport {
        /// The transport that cannot serve this operation.
        transport: Transport,
        /// The op's legal transports, in remedy order.
        legal: &'static [Transport],
    },
    /// A WS order send was requested but no `WsSurface` is wired on this client.
    /// Recover with `.via(Transport::Rest)`.
    #[error(
        "No WS surface is wired on this client; \
         use `.via(Transport::Rest)` for order operations."
    )]
    WsSurfaceUnavailable,
    /// The WebSocket session token was rejected by the venue, or was missing or expired at compose time (then not sent).
    /// A refresh starts while the connection is live; after `AuthenticationFailedEvent` or `RetryCapExhausted` the client must be rebuilt.
    /// Retryable; the SDK never resends an order.
    #[error(
        "The session token was rejected and the request was not accepted; retry once the token is refreshed."
    )]
    SessionExpired {
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// The caller→I/O queue was full at post time; the order was NOT sent. The SDK never
    /// auto-retries an order.
    #[error("Order send queue is full; back off and retry.")]
    QueueFull {
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// The Futures placement limit (`futures_max_in_flight`) was reached; the order was NOT
    /// sent. Retryable once a pending placement is answered, its deadline passes, or its await is
    /// dropped (the venue may still hold a dropped order).
    #[error(
        "Futures in-flight placement limit reached; the order was not sent. Wait for a reply or raise futures_max_in_flight."
    )]
    InFlightLimitReached {
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// The API key lacks a required permission (`Permission denied`). Non-retryable.
    #[error("Permission denied.")]
    PermissionDenied {
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// The private WebSocket connection could not authenticate; nothing was sent.
    /// Non-retryable: rebuild the client with a working key.
    #[error("Authentication failed: {kraken_message}")]
    AuthenticationFailed {
        /// Why authentication failed.
        kraken_message: String,
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// Catch-all for Kraken error strings with no typed mapping; the wire message passes through verbatim.
    #[error("{kraken_message}")]
    Unknown {
        /// Kraken error class prefix before the first `:` (e.g. `EGeneral`), the whole string if none, or `AUTH` for remapped auth failures.
        kraken_code: String,
        /// Raw Kraken error string passed through verbatim; auth remaps carry the auth error's text instead.
        kraken_message: String,
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
}

crate::error::impl_error_common!(TradeError);

impl TradeError {
    pub(crate) fn invalid_order(detail: impl Into<String>) -> Self {
        Self::InvalidOrder {
            detail: detail.into(),
            request_id: None,
        }
    }

    pub(crate) fn check_batch_size(
        len: usize,
        range: std::ops::RangeInclusive<usize>,
    ) -> Result<(), Self> {
        if range.contains(&len) {
            Ok(())
        } else {
            Err(Self::BatchSizeOutOfRange {
                min: *range.start() as u32,
                max: *range.end() as u32,
            })
        }
    }
}

crate::error::impl_with_request_id!(
    TradeError: InsufficientFunds, InsufficientMargin, NoAmendableParameters,
    UnknownOrder, InvalidOrder, RateLimited, Transport, MalformedResponse,
    QueueFull, InFlightLimitReached, SessionExpired, PermissionDenied, AuthenticationFailed,
    Unknown
);

/// Baseline classification; context variants are built by callers holding request context.
impl From<RestError> for TradeError {
    fn from(e: RestError) -> Self {
        match e {
            RestError::Transport { kind, transient } => TradeError::Transport {
                kind,
                transient,
                request_id: None,
            },
            RestError::UnexpectedShape(s) => TradeError::malformed(s),
            RestError::RateLimit(_) => TradeError::RateLimited {
                retry_after_ts: None,
                request_id: None,
            },
            RestError::Auth(a) => TradeError::Unknown {
                kraken_code: "AUTH".into(),
                kraken_message: a.to_string(),
                request_id: None,
            },
            RestError::Kraken(strings) => {
                let first = strings.into_iter().next().unwrap_or_default();
                classify_kraken_error_string(first)
            }
        }
    }
}

/// Classify one Kraken error string into the baseline family shared by the REST and WS paths.
pub(crate) fn classify_kraken_error_string(msg: String) -> TradeError {
    if crate::error::is_kraken_rate_limit(&msg) {
        TradeError::RateLimited {
            retry_after_ts: crate::error::parse_throttle_until(&msg),
            request_id: None,
        }
    } else if msg.contains("Permission denied") {
        TradeError::PermissionDenied { request_id: None }
    } else if msg.contains("Insufficient funds") {
        TradeError::InsufficientFunds { request_id: None }
    } else if msg.contains("Insufficient") && msg.contains("margin") {
        TradeError::InsufficientMargin { request_id: None }
    } else if msg.contains("No amendable parameters") {
        // Must precede the EOrder catch (wire string carries EOrder:).
        TradeError::NoAmendableParameters { request_id: None }
    } else if msg.trim_end_matches('.').ends_with("Unknown order") {
        // Terminal-phrase match so `Unknown order action.` stays InvalidOrder.
        TradeError::UnknownOrder { request_id: None }
    } else if msg.contains("EOrder")
        || msg.contains("Invalid arguments")
        // Bare validate-mode price rejections; price-anchored so "Symbol(s) not found" stays out.
        || msg.contains("price(s) not found")
        || msg.contains("Price(s) not found")
        // REST phrasing for a cancel_all_orders_after timeout above 24h.
        || msg.contains("Invalid timeout argument")
    {
        // Trim the trailing period so `{detail}.` renders exactly one.
        TradeError::invalid_order(msg.trim_end_matches('.'))
    } else {
        TradeError::Unknown {
            kraken_code: crate::error::kraken_eclass_code(&msg),
            kraken_message: msg,
            request_id: None,
        }
    }
}

/// Classify a Unified (Futures) WS error string. The venue words rejects as
/// `Other:<CODE>` or bare parameter messages rather than `EOrder:` strings, so
/// those map here; everything else falls through to [`classify_kraken_error_string`].
pub(crate) fn classify_futures_ws_error(msg: String) -> TradeError {
    const INVALID_ORDER: &[&str] = &[
        "CLIENT_ORDER_ID_IN_USE",
        "CANNOT_EDIT_TRIGGER_PRICE_OF_TRAILING_STOP",
        "ORDER_CANNOT_HAVE_TRIGGER_PRICE",
        "CANNOT_EDIT_LIMIT_PRICE_OF_TRAILING_STOP",
        "PRICE_COLLAR",
        "SMALL_ORDER_LIMIT_EXCEEDED",
        "TRAILING_STOP_PERCENT_DEVIATION_EXCEEDS_MAX_DECIMAL_PLACES",
        "TRAILING_STOP_QUOTE_DEVIATION_NOT_MULTIPLE_OF_TICK_SIZE",
        "TRAILING_STOP_MAX_DEVIATION_TOO_LARGE",
        "TRAILING_STOP_MAX_DEVIATION_TOO_SMALL",
        "CLIENT_ORDER_ID_TOO_LONG",
        "CLIENT_ORDER_ID_INVALID",
        "VIOLATES_REDUCE_ONLY",
        "POST_WOULD_EXECUTE",
        "IOC_WOULD_ENTER_BOOK",
        "Missing arguments",
        "Missing or invalid 'params.",
        "' must be ",
        "Unknown 'params",
        "Invalid 'params.",
        "Bad request",
        "Unknown asset",
        "only supported for spot orders",
    ];
    if crate::error::is_token_stale_reject(crate::types::WsUrl::UnifiedPrivate, &msg) {
        TradeError::SessionExpired { request_id: None }
    } else if msg.contains("ORDER_FOR_EDIT_NOT_FOUND")
        || msg.contains("ORDER_FOR_CANCELLATION_NOT_FOUND")
    {
        TradeError::UnknownOrder { request_id: None }
    } else if msg.contains("EDIT_HAS_NO_EFFECT") {
        TradeError::NoAmendableParameters { request_id: None }
    } else if msg.contains("INSUFFICIENT_MARGIN") || msg.contains("INSUFFICIENT_CLOSING_MARGIN") {
        TradeError::InsufficientMargin { request_id: None }
    } else if INVALID_ORDER.iter().any(|needle| msg.contains(needle)) {
        TradeError::invalid_order(msg.trim_end_matches('.'))
    } else {
        classify_kraken_error_string(msg)
    }
}

/// Per-entry order error from `AddOrderBatch` / `CancelBatch`.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize)]
#[non_exhaustive]
#[error("{message}")]
pub struct OrderError {
    /// Client order id of the batch entry this error belongs to.
    pub cl_ord_id: crate::types::ClOrdId,
    /// Machine-readable error code copied from the source `TradeError` (e.g. `INVALID_ORDER`).
    pub code: String,
    /// Human-readable message from the source error; also the Display output.
    pub message: String,
    /// Whether the source error was classified as retryable.
    pub retryable: bool,
    /// Error category (client / exchange / network / rate limit) from the source error.
    pub category: crate::error::ErrorCategory,
    /// Per-dispatch correlation id carried over from the source `TradeError`.
    pub request_id: Option<String>,
}

impl OrderError {
    /// Build an `OrderError` from a `TradeError` + the failed entry's `cl_ord_id`.
    pub(super) fn from_trade_error(cl_ord_id: crate::types::ClOrdId, e: TradeError) -> Self {
        use crate::error::ApiError;
        Self {
            cl_ord_id,
            code: e.code().to_string(),
            message: e.message(),
            retryable: e.retryable(),
            category: e.category(),
            request_id: e.request_id().map(str::to_string),
        }
    }
}

impl crate::error::sealed::Sealed for OrderError {}

impl crate::error::ApiError for OrderError {
    fn code(&self) -> &str {
        &self.code
    }
    fn category(&self) -> crate::error::ErrorCategory {
        self.category
    }
    fn retryable(&self) -> bool {
        self.retryable
    }
    fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
    fn message(&self) -> String {
        self.message.clone()
    }
    fn kraken_code(&self) -> Option<&str> {
        None
    }
}

impl crate::error::sealed::Sealed for TradeError {}

impl crate::error::ApiError for TradeError {
    fn code(&self) -> &str {
        use TradeError::*;
        match self {
            InsufficientFunds { .. } => "INSUFFICIENT_FUNDS",
            InsufficientMargin { .. } => "INSUFFICIENT_MARGIN",
            InvalidOrder { .. } => "INVALID_ORDER",
            ConflictingOrderIdentifiers => "INVALID_CL_ORD_ID",
            SettlePositionRequiresLeverage
            | ReduceOnlyRequiresLeverage
            | BatchSizeOutOfRange { .. }
            | EmptyAmendRequest
            | WsUnsupportedOrderField { .. }
            | RestUnsupportedOrderField { .. }
            | UnsupportedTransport { .. }
            | WsSurfaceUnavailable => "INVALID_ARGUMENTS",
            NoAmendableParameters { .. } => "NON_AMENDABLE_FIELD",
            UnknownOrder { .. } => "ORDER_NOT_FOUND",
            SystemStatusBlocked { .. } => "SYSTEM_STATUS_MAINTENANCE",
            RateLimited { .. } => "RATE_LIMIT_EXCEEDED",
            QueueFull { .. } => "QUEUE_FULL",
            InFlightLimitReached { .. } => "IN_FLIGHT_LIMIT_REACHED",
            SessionExpired { .. } => "SESSION_EXPIRED",
            PermissionDenied { .. } => "PERMISSION_DENIED",
            AuthenticationFailed { .. } => "AUTHENTICATION_FAILED",
            Transport { .. } => "CONNECTION_ERROR",
            MalformedResponse { .. } => "MALFORMED_RESPONSE",
            ClientClosed => "CLIENT_CLOSED",
            LoopDead => "LOOP_DEAD",
            Unknown { .. } => "UNKNOWN",
        }
    }
    fn category(&self) -> crate::error::ErrorCategory {
        use crate::error::ErrorCategory;
        use TradeError::*;
        match self {
            InsufficientFunds { .. }
            | InsufficientMargin { .. }
            | SystemStatusBlocked { .. }
            | Unknown { .. } => ErrorCategory::Exchange,
            InvalidOrder { .. }
            | ConflictingOrderIdentifiers
            | SettlePositionRequiresLeverage
            | ReduceOnlyRequiresLeverage
            | EmptyAmendRequest
            | NoAmendableParameters { .. }
            | UnknownOrder { .. }
            | BatchSizeOutOfRange { .. }
            | MalformedResponse { .. }
            | ClientClosed
            | LoopDead
            | QueueFull { .. }
            | InFlightLimitReached { .. }
            | WsUnsupportedOrderField { .. }
            | RestUnsupportedOrderField { .. }
            | UnsupportedTransport { .. }
            | WsSurfaceUnavailable => ErrorCategory::Client,
            RateLimited { .. } => ErrorCategory::RateLimit,
            SessionExpired { .. } | PermissionDenied { .. } | AuthenticationFailed { .. } => {
                ErrorCategory::Auth
            }
            Transport { .. } => ErrorCategory::Network,
        }
    }
    fn retryable(&self) -> bool {
        use TradeError::*;
        match self {
            RateLimited { .. }
            | QueueFull { .. }
            | InFlightLimitReached { .. }
            | SessionExpired { .. } => true,
            // Never retry a sent-ambiguous drop (duplicate-fill risk).
            Transport {
                kind, transient, ..
            } => *transient && !kind.is_sent_ambiguous(),
            _ => false,
        }
    }
    fn request_id(&self) -> Option<&str> {
        use TradeError::*;
        match self {
            InsufficientFunds { request_id }
            | InsufficientMargin { request_id }
            | NoAmendableParameters { request_id }
            | UnknownOrder { request_id }
            | InvalidOrder { request_id, .. }
            | RateLimited { request_id, .. }
            | Transport { request_id, .. }
            | MalformedResponse { request_id, .. }
            | QueueFull { request_id }
            | InFlightLimitReached { request_id }
            | SessionExpired { request_id }
            | PermissionDenied { request_id }
            | AuthenticationFailed { request_id, .. }
            | Unknown { request_id, .. } => request_id.as_deref(),
            ConflictingOrderIdentifiers
            | SettlePositionRequiresLeverage
            | ReduceOnlyRequiresLeverage
            | EmptyAmendRequest
            | BatchSizeOutOfRange { .. }
            | SystemStatusBlocked { .. }
            | ClientClosed
            | LoopDead
            | WsUnsupportedOrderField { .. }
            | RestUnsupportedOrderField { .. }
            | UnsupportedTransport { .. }
            | WsSurfaceUnavailable => None,
        }
    }
    crate::error::api_error_tail!(carried);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_request_id_stamps_wire_variants_and_never_overwrites() {
        use crate::error::ApiError;
        let e = TradeError::malformed("bad body".into()).with_request_id("rid-1");
        assert_eq!(e.request_id(), Some("rid-1"));
        let e = e.with_request_id("rid-2");
        assert_eq!(e.request_id(), Some("rid-1"));
        let e = TradeError::EmptyAmendRequest.with_request_id("rid-3");
        assert_eq!(e.request_id(), None);
    }

    #[test]
    fn loop_dead_maps_to_client_non_retryable() {
        use crate::error::ApiError;
        let e = TradeError::LoopDead;
        assert_eq!(e.code(), "LOOP_DEAD");
        assert_eq!(e.category(), crate::error::ErrorCategory::Client);
        assert!(!e.retryable(), "LoopDead is terminal — never retryable");
        assert!(
            e.message()
                .starts_with("The client's internal loop has failed")
        );
        assert!(e.message().ends_with("rebuild the client."));
        assert!(e.message().contains("`.via(Transport::Rest)`"));
    }

    #[test]
    fn futures_ws_rejects_classify_by_the_venue_vocabulary() {
        use crate::error::ApiError;
        // (venue string, code, retryable)
        let cases = [
            ("Other:ORDER_FOR_EDIT_NOT_FOUND", "ORDER_NOT_FOUND", false),
            (
                "Other:ORDER_FOR_CANCELLATION_NOT_FOUND",
                "ORDER_NOT_FOUND",
                false,
            ),
            ("Other:EDIT_HAS_NO_EFFECT", "NON_AMENDABLE_FIELD", false),
            ("Other:INSUFFICIENT_MARGIN", "INSUFFICIENT_MARGIN", false),
            ("Rate limit exceeded", "RATE_LIMIT_EXCEEDED", true),
            ("Other:CLIENT_ORDER_ID_IN_USE", "INVALID_ORDER", false),
            (
                "EGeneral:Other:CLIENT_ORDER_ID_IN_USE",
                "INVALID_ORDER",
                false,
            ),
            ("Other:VIOLATES_REDUCE_ONLY", "INVALID_ORDER", false),
            (
                "Other:CANNOT_EDIT_TRIGGER_PRICE_OF_TRAILING_STOP",
                "INVALID_ORDER",
                false,
            ),
            (
                "Other:ORDER_CANNOT_HAVE_TRIGGER_PRICE",
                "INVALID_ORDER",
                false,
            ),
            (
                "Other:CANNOT_EDIT_LIMIT_PRICE_OF_TRAILING_STOP",
                "INVALID_ORDER",
                false,
            ),
            ("Other:PRICE_COLLAR", "INVALID_ORDER", false),
            ("Other:SMALL_ORDER_LIMIT_EXCEEDED", "INVALID_ORDER", false),
            (
                "Other:TRAILING_STOP_PERCENT_DEVIATION_EXCEEDS_MAX_DECIMAL_PLACES",
                "INVALID_ORDER",
                false,
            ),
            (
                "Other:TRAILING_STOP_QUOTE_DEVIATION_NOT_MULTIPLE_OF_TICK_SIZE",
                "INVALID_ORDER",
                false,
            ),
            (
                "Other:TRAILING_STOP_MAX_DEVIATION_TOO_LARGE",
                "INVALID_ORDER",
                false,
            ),
            (
                "Other:TRAILING_STOP_MAX_DEVIATION_TOO_SMALL",
                "INVALID_ORDER",
                false,
            ),
            ("Other:CLIENT_ORDER_ID_TOO_LONG", "INVALID_ORDER", false),
            ("Other:CLIENT_ORDER_ID_INVALID", "INVALID_ORDER", false),
            (
                "Other:INSUFFICIENT_CLOSING_MARGIN",
                "INSUFFICIENT_MARGIN",
                false,
            ),
            ("Other:POST_WOULD_EXECUTE", "INVALID_ORDER", false),
            ("Other:IOC_WOULD_ENTER_BOOK", "INVALID_ORDER", false),
            ("EGeneral:Invalid arguments:price", "INVALID_ORDER", false),
            ("Invalid arguments:volume", "INVALID_ORDER", false),
            ("Invalid arguments:cl_ord_id", "INVALID_ORDER", false),
            ("Missing arguments:no arguments", "INVALID_ORDER", false),
            ("Missing or invalid 'params.symbol'", "INVALID_ORDER", false),
            (
                "'params.timeout' must be between 0 and 86400 seconds",
                "INVALID_ORDER",
                false,
            ),
            ("Unknown 'params' key: leverage", "INVALID_ORDER", false),
            (
                "Unknown 'params.orders' key: symbol",
                "INVALID_ORDER",
                false,
            ),
            ("Invalid 'params.order_type'", "INVALID_ORDER", false),
            (
                "Bad request:`fee_preference` not supported for Futures",
                "INVALID_ORDER",
                false,
            ),
            (
                "'params.display_qty' is only supported for spot orders",
                "INVALID_ORDER",
                false,
            ),
            ("Unknown asset", "INVALID_ORDER", false),
            ("Unknown asset pair", "INVALID_ORDER", false),
            ("Invalid session", "SESSION_EXPIRED", true),
            ("ESession:Invalid session token", "SESSION_EXPIRED", true),
            ("Missing or invalid 'params.token'", "SESSION_EXPIRED", true),
            ("Token(s) not found", "SESSION_EXPIRED", true),
            ("Token field must be a string", "SESSION_EXPIRED", true),
        ];
        for (msg, code, retryable) in cases {
            let e = classify_futures_ws_error(msg.to_string());
            assert_eq!((e.code(), e.retryable()), (code, retryable), "{msg}");
        }
    }

    #[test]
    fn a_unified_throttle_reply_sets_retry_after_ts_from_retry_after_ms() {
        use super::super::ws_compose::classify_ws_failure;
        use crate::conn::managed_connection::WsResponse;
        use crate::types::WsUrl;
        // (url, wire retry_after_ms, decoded wait in ms)
        let cases = [
            (
                WsUrl::UnifiedPrivate,
                Some(serde_json::json!(588)),
                Some(588),
            ),
            (
                WsUrl::UnifiedPrivate,
                Some(serde_json::json!(1000)),
                Some(1000),
            ),
            (
                WsUrl::UnifiedPrivate,
                Some(serde_json::json!(1001)),
                Some(1001),
            ),
            (WsUrl::UnifiedPrivate, Some(serde_json::json!(0)), Some(0)),
            (WsUrl::UnifiedPrivate, None, None),
            (WsUrl::UnifiedPrivate, Some(serde_json::json!("588")), None),
            (WsUrl::UnifiedPrivate, Some(serde_json::json!(-588)), None),
            (WsUrl::UnifiedPrivate, Some(serde_json::json!(588.5)), None),
            (WsUrl::Auth, Some(serde_json::json!(588)), None),
        ];
        for (url, retry_after_ms, wait_ms) in cases {
            let mut frame = serde_json::json!({
                "error": "Rate limit exceeded", "method": "add_order", "req_id": 7, "success": false,
            });
            if let Some(ms) = &retry_after_ms {
                frame["retry_after_ms"] = ms.clone();
            }
            let resp = WsResponse::from_frame(7, false, &frame);
            let before = crate::rest::surface::unix_now_ms().expect("clock after epoch");
            let e = classify_ws_failure(url, resp.error, resp.retry_after_ms);
            let after = crate::rest::surface::unix_now_ms().expect("clock after epoch");
            let TradeError::RateLimited { retry_after_ts, .. } = e else {
                panic!("{url:?} {retry_after_ms:?}: not RateLimited: {e:?}");
            };
            match wait_ms {
                Some(ms) => {
                    let ts = retry_after_ts.unwrap_or_else(|| panic!("{url:?} {retry_after_ms:?}"));
                    assert!(
                        ts * 1000 >= before + ms && ts <= (after + ms).div_ceil(1000),
                        "{url:?} {retry_after_ms:?}: {ts} not in [{}ms, {}s]",
                        before + ms,
                        (after + ms).div_ceil(1000)
                    );
                }
                None => assert_eq!(retry_after_ts, None, "{url:?} {retry_after_ms:?}"),
            }
        }
    }

    #[test]
    fn retry_after_ts_rounds_the_retry_instant_up_to_whole_seconds() {
        use super::super::ws_compose::retry_after_ts_at;
        // (now_ms, retry_after_ms, retry_after_ts)
        let cases = [
            (1_500, 588, 3),
            (1_412, 588, 2),
            (1_000, 0, 1),
            (1_001, 0, 2),
            (1_999, 1_002, 4),
            (u64::MAX - 10, 588, u64::MAX.div_ceil(1000)),
        ];
        for (now_ms, retry_after_ms, ts) in cases {
            assert_eq!(
                retry_after_ts_at(now_ms, retry_after_ms),
                ts,
                "now_ms={now_ms} retry_after_ms={retry_after_ms}"
            );
        }
    }

    #[test]
    fn a_rejected_session_token_is_session_expired_on_both_ws_products() {
        use super::super::ws_compose::classify_ws_failure;
        use crate::error::{ApiError, ErrorCategory};
        use crate::types::WsUrl;
        // (url, venue string, session expired)
        let cases = [
            (WsUrl::Auth, "EAPI:Invalid token", true),
            (WsUrl::Auth, "ESession:Invalid session", true),
            (WsUrl::UnifiedPrivate, "Invalid session", true),
            (WsUrl::UnifiedPrivate, "Token(s) not found", true),
            (WsUrl::Auth, "EAPI:Invalid key", false),
            (WsUrl::Auth, "EOrder:Insufficient funds", false),
            (
                WsUrl::UnifiedPrivate,
                "Unknown 'params' key: leverage",
                false,
            ),
        ];
        for (url, msg, expired) in cases {
            let e = classify_ws_failure(url, Some(msg.to_string()), None);
            assert_eq!(
                matches!(e, TradeError::SessionExpired { .. }),
                expired,
                "{url:?} {msg}"
            );
            if expired {
                assert_eq!(
                    (e.code(), e.category(), e.retryable()),
                    ("SESSION_EXPIRED", ErrorCategory::Auth, true),
                    "{url:?} {msg}"
                );
            }
        }
    }

    #[test]
    fn validate_mode_price_rejections_classify_as_invalid_order() {
        for msg in [
            "Limit_price(s) not found",
            "Stop_price(s) not found",
            "Price(s) not found",
            "Price(s) not found.",
        ] {
            assert!(
                matches!(
                    classify_kraken_error_string(msg.to_string()),
                    TradeError::InvalidOrder { .. }
                ),
                "{msg} must classify as InvalidOrder"
            );
        }
        assert!(matches!(
            classify_kraken_error_string("EGeneral:Invalid timeout argument:above max".to_string()),
            TradeError::InvalidOrder { .. }
        ));
        assert!(matches!(
            classify_kraken_error_string("Symbol(s) not found".to_string()),
            TradeError::Unknown { .. }
        ));
    }

    #[test]
    fn insufficient_margin_phrasings_classify_as_insufficient_margin() {
        for msg in [
            "EOrder:Insufficient margin",
            "EOrder:Insufficient initial margin",
        ] {
            assert!(
                matches!(
                    classify_kraken_error_string(msg.to_string()),
                    TradeError::InsufficientMargin { .. }
                ),
                "{msg} must classify as InsufficientMargin"
            );
        }
        assert!(matches!(
            classify_kraken_error_string("EOrder:Insufficient funds".to_string()),
            TradeError::InsufficientFunds { .. }
        ));
        for msg in [
            "EOrder:Margin allowance exceeded",
            "EOrder:Margin level too low",
            "EOrder:Order cancelled:Account has insufficient initial margin to support this resting order",
        ] {
            assert!(
                matches!(
                    classify_kraken_error_string(msg.to_string()),
                    TradeError::InvalidOrder { .. }
                ),
                "{msg} must stay on the EOrder catch"
            );
        }
    }

    #[test]
    fn too_many_requests_classifies_as_rate_limited() {
        assert!(matches!(
            classify_kraken_error_string("EGeneral:Too many requests".to_string()),
            TradeError::RateLimited { .. }
        ));
        assert!(matches!(
            classify_kraken_error_string("EAPI:Rate limit exceeded".to_string()),
            TradeError::RateLimited { .. }
        ));
        assert!(matches!(
            classify_kraken_error_string("EOrder:Insufficient funds".to_string()),
            TradeError::InsufficientFunds { .. }
        ));
    }

    #[test]
    fn amend_rejections_classify_to_typed_variants() {
        use crate::error::ApiError;
        let e =
            classify_kraken_error_string("EOrder:No amendable parameters specified".to_string());
        assert!(matches!(e, TradeError::NoAmendableParameters { .. }));
        assert_eq!(e.code(), "NON_AMENDABLE_FIELD");
        assert_eq!(e.category(), crate::error::ErrorCategory::Client);
        assert!(!e.retryable());
        let cap = TradeError::InFlightLimitReached { request_id: None };
        assert_eq!(cap.code(), "IN_FLIGHT_LIMIT_REACHED");
        assert_eq!(cap.category(), crate::error::ErrorCategory::Client);
        assert!(cap.retryable());
        assert_ne!(
            cap.code(),
            TradeError::QueueFull { request_id: None }.code(),
            "the cap refusal is not the SDK queue being full"
        );
        assert_eq!(
            cap.clone().with_request_id("7").request_id(),
            Some("7"),
            "the cap refusal carries its dispatch correlation id"
        );
        let u = classify_kraken_error_string("EOrder:Unknown order".to_string());
        assert!(matches!(u, TradeError::UnknownOrder { .. }));
        assert_eq!(u.code(), "ORDER_NOT_FOUND");
        assert_eq!(u.category(), crate::error::ErrorCategory::Client);
        assert!(!u.retryable());
        assert!(matches!(
            classify_kraken_error_string(
                "EOrder:Invalid price:Invalid price argument.".to_string()
            ),
            TradeError::InvalidOrder { .. }
        ));
        assert!(matches!(
            classify_kraken_error_string("EOrder:Unknown order action.".to_string()),
            TradeError::InvalidOrder { .. }
        ));
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
                matches!(
                    classify_kraken_error_string(code.to_string()),
                    TradeError::RateLimited { .. }
                ),
                "trade: {code} should map to RateLimited"
            );
        }
    }

    #[test]
    fn eorder_invalid_order_pins_as_client_invalid_order() {
        use crate::error::ApiError;
        use crate::error::ErrorCategory;

        let rest = classify_kraken_error_string("EOrder:Invalid order".to_string());
        assert!(matches!(rest, TradeError::InvalidOrder { .. }));
        assert_eq!(rest.code(), "INVALID_ORDER");
        assert_eq!(rest.category(), ErrorCategory::Client);
        assert!(!rest.retryable());

        let ws =
            super::super::ws_compose::classify_ws_error(Some("EOrder:Invalid order".to_string()));
        assert!(matches!(ws, TradeError::InvalidOrder { .. }));
        assert_eq!(ws.code(), "INVALID_ORDER");
        assert_eq!(ws.category(), ErrorCategory::Client);
        assert!(!ws.retryable());
    }

    #[test]
    fn permission_refusals_classify_as_a_typed_auth_error() {
        use crate::error::{ApiError, ErrorCategory};
        for msg in [
            "Permission denied",
            "EGeneral:Permission denied",
            "EAPI:Permission denied",
        ] {
            for e in [
                classify_kraken_error_string(msg.to_string()),
                classify_futures_ws_error(msg.to_string()),
                super::super::ws_compose::classify_ws_error(Some(msg.to_string())),
            ] {
                assert_eq!(
                    (e.code(), e.category(), e.retryable(), e.message()),
                    (
                        "PERMISSION_DENIED",
                        ErrorCategory::Auth,
                        false,
                        "Permission denied.".to_string()
                    ),
                    "{msg}: {e:?}"
                );
            }
        }
    }

    #[test]
    fn a_failed_authentication_is_a_non_retryable_auth_error_carrying_its_cause() {
        use crate::error::{ApiError, ConnectionError, ErrorCategory};
        let e = super::super::ws_conn_error_to_trade(ConnectionError::authentication_failed(
            "EAPI:Invalid key".into(),
        ));
        assert_eq!(
            (e.code(), e.category(), e.retryable(), e.message()),
            (
                "AUTHENTICATION_FAILED",
                ErrorCategory::Auth,
                false,
                "Authentication failed: EAPI:Invalid key".to_string()
            ),
            "{e:?}"
        );
        assert_eq!(e.with_request_id("rid-1").request_id(), Some("rid-1"));
    }

    #[test]
    fn a_failed_connection_reports_its_cause_by_class() {
        use crate::dispatch::NonTransientClass::*;
        use crate::error::{ApiError, ConnectionError};
        // (class, last_error, code, message)
        let cases = [
            (
                Some(BadCreds),
                "auth handshake failed: BadCreds",
                "AUTHENTICATION_FAILED",
                "Authentication failed: Invalid API key or signature",
            ),
            (
                Some(PermissionDenied),
                "auth handshake failed: PermissionDenied",
                "PERMISSION_DENIED",
                "Permission denied.",
            ),
            (
                Some(ClientError),
                "AuthRefreshFailed: Invalid API key.",
                "AUTHENTICATION_FAILED",
                "Authentication failed: Invalid API key.",
            ),
            (
                Some(RetryCapExhausted),
                "token-stale handshake cap reached",
                "CONNECTION_ERROR",
                "Transport: Other(\"Connection is not open; the request was not sent.\")",
            ),
            (
                None,
                "ws close 1008",
                "CONNECTION_ERROR",
                "Transport: Other(\"Connection is not open; the request was not sent.\")",
            ),
        ];
        for (class, last_error, code, message) in cases {
            let e = super::super::ws_conn_error_to_trade(ConnectionError::connection_failed(
                class, last_error,
            ));
            assert_eq!(
                (e.code(), e.message()),
                (code, message.to_string()),
                "{class:?}"
            );
        }
    }

    #[test]
    fn transport_retryable_is_per_kind() {
        use crate::error::ApiError;
        let ambiguous = TradeError::from(RestError::Transport {
            kind: TransportErrorKind::RequestSentNoResponse,
            transient: true,
        });
        assert_eq!(ambiguous.code(), "CONNECTION_ERROR");
        assert!(
            !ambiguous.retryable(),
            "RequestSentNoResponse is sent-ambiguous → NOT retryable"
        );

        for kind in [
            TransportErrorKind::TcpConnectTimeout,
            TransportErrorKind::TcpRefused,
            TransportErrorKind::DnsFailure,
            TransportErrorKind::TlsHandshakeFailure,
        ] {
            let e = TradeError::from(RestError::Transport {
                kind: kind.clone(),
                transient: true,
            });
            assert!(e.retryable(), "not-sent kind {kind:?} must be retryable");
        }

        let http_4xx = TradeError::from(RestError::Transport {
            kind: TransportErrorKind::HttpStatus { status: 400 },
            transient: false,
        });
        assert!(
            !http_4xx.retryable(),
            "non-transient HTTP 4xx must NOT be retryable"
        );
    }

    #[test]
    fn throttled_rate_limit_carries_retry_after_ts() {
        let throttled = classify_kraken_error_string("EService:Throttled: 1700000000".to_string());
        assert!(matches!(
            throttled,
            TradeError::RateLimited {
                retry_after_ts: Some(1_700_000_000),
                ..
            }
        ));
        let plain = classify_kraken_error_string("EAPI:Rate limit exceeded".to_string());
        assert!(matches!(
            plain,
            TradeError::RateLimited {
                retry_after_ts: None,
                ..
            }
        ));
    }

    #[test]
    fn invalid_order_period_terminated_detail_is_not_doubled() {
        let msg =
            classify_kraken_error_string("EOrder:Unknown order action.".to_string()).to_string();
        assert!(
            msg.ends_with('.') && !msg.ends_with(".."),
            "double terminal period: {msg:?}"
        );
    }

    #[test]
    fn transport_routing_faults_classify_as_client_non_retryable() {
        use crate::error::{ApiError, ErrorCategory};

        let unsupported = TradeError::UnsupportedTransport {
            transport: crate::dispatch::Transport::WsV2Public,
            legal: &[
                crate::dispatch::Transport::Rest,
                crate::dispatch::Transport::WsV2Auth,
            ],
        };
        assert_eq!(unsupported.code(), "INVALID_ARGUMENTS");
        assert_eq!(unsupported.category(), ErrorCategory::Client);
        assert!(!unsupported.retryable());
        assert_eq!(unsupported.request_id(), None);

        let no_surface = TradeError::WsSurfaceUnavailable;
        assert_eq!(no_surface.code(), "INVALID_ARGUMENTS");
        assert_eq!(no_surface.category(), ErrorCategory::Client);
        assert!(!no_surface.retryable());
        assert_eq!(no_surface.request_id(), None);
    }
}
