//! [`TradeError`] — trade-namespace error taxonomy + classification.

use super::super::SystemStatus;
use crate::dispatch::Transport;
use crate::rest::RestError;
use crate::transport::TransportErrorKind;
use serde::Serialize;
use serde_with::skip_serializing_none;

/// Or-join the op's legal transports into the `UnsupportedTransport` remedy.
fn remedy(legal: &[Transport]) -> String {
    legal
        .iter()
        .map(|t| format!("`.via(Transport::{t:?})`"))
        .collect::<Vec<_>>()
        .join(" or ")
}

/// Errors returned by trade-namespace operations.
// `Eq` not derived — `Transport { kind }` carries non-Eq context in some kinds.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum TradeError {
    /// Exchange rejected the order: the account balance cannot cover it (`EOrder:Insufficient funds`).
    #[error("Insufficient funds.")]
    InsufficientFunds {
        /// Per-dispatch correlation id; stamped once at dispatch, first stamp wins.
        request_id: Option<String>,
    },
    /// Exchange rejected the order: free margin cannot cover it (`EOrder:Insufficient margin`).
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
    /// Amend targeted a `cl_ord_id` matching no open order. Non-retryable.
    #[error("Order not found; no open order matches the cl_ord_id.")]
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
        /// Unix seconds to retry after, parsed from `EService:Throttled: <ts>`; `None` when no throttle timestamp is available.
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
        "Transport {transport:?} cannot serve this spot order operation; use {}.",
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
    /// The caller→I/O queue was full at post time; the order was NOT sent.
    /// The SDK never auto-retries an order.
    #[error("Order send queue is full; back off and retry.")]
    QueueFull {
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

crate::error::impl_with_request_id!(
    TradeError: InsufficientFunds, InsufficientMargin, NoAmendableParameters,
    UnknownOrder, InvalidOrder, RateLimited, Transport, MalformedResponse,
    QueueFull, Unknown
);

/// Classify a `RestError` into a namespace variant. Baseline only —
/// context variants are built by callers holding request context.
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

/// Classify a single Kraken error string into the baseline `TradeError` family
/// shared by the REST and WS paths.
pub(crate) fn classify_kraken_error_string(msg: String) -> TradeError {
    if crate::error::is_kraken_rate_limit(&msg) {
        TradeError::RateLimited {
            retry_after_ts: crate::error::parse_throttle_until(&msg),
            request_id: None,
        }
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
        // Validate-mode price rejections arrive bare (non-E-prefixed);
        // price-anchored so "Symbol(s) not found" stays out.
        || msg.contains("price(s) not found")
        || msg.contains("Price(s) not found")
        // REST >24h cancel_all_orders_after deadline phrasing.
        || msg.contains("Invalid timeout argument")
    {
        // Trim trailing period so `{detail}.` renders exactly one.
        TradeError::InvalidOrder {
            detail: msg.trim_end_matches('.').to_string(),
            request_id: None,
        }
    } else {
        TradeError::Unknown {
            kraken_code: crate::error::kraken_eclass_code(&msg),
            kraken_message: msg,
            request_id: None,
        }
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
            | WsUnsupportedOrderField { .. }
            | RestUnsupportedOrderField { .. }
            | UnsupportedTransport { .. }
            | WsSurfaceUnavailable => ErrorCategory::Client,
            RateLimited { .. } => ErrorCategory::RateLimit,
            Transport { .. } => ErrorCategory::Network,
        }
    }
    fn retryable(&self) -> bool {
        use TradeError::*;
        match self {
            RateLimited { .. } | QueueFull { .. } => true,
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
        // First stamp wins; later stamps are no-ops.
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
    fn validate_mode_price_rejections_classify_as_invalid_order() {
        // Bare validate-mode price rejections must not fall through to Unknown.
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
        // Symbol(s) not found stays out of the price family.
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
        // "Unknown order action." is InvalidOrder, not UnknownOrder.
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
        // EOrder:Invalid order covers both gone and garbage ids.
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
        // Period-terminated passthrough must not render "..".
        let msg =
            classify_kraken_error_string("EOrder:Unknown order action.".to_string()).to_string();
        assert!(
            msg.ends_with('.') && !msg.ends_with(".."),
            "double terminal period: {msg:?}"
        );
    }

    #[test]
    fn transport_routing_faults_classify_as_client_non_retryable() {
        // Wrong/unavailable transport: never sent, non-retryable, no request_id.
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
