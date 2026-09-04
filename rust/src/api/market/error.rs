//! `MarketError` — the market-namespace error enum.

use crate::rest::RestError;
use crate::transport::TransportErrorKind;

use super::types::SystemStatus;

/// Errors returned by market-namespace operations. The `#[error]` format strings
/// are the canonical human-readable message templates; `ApiError::message()`
/// delegates to `Display`.
// `Eq` not derived — `Transport { kind }` carries non-Eq context in some variants.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum MarketError {
    /// The queried asset pair is not known to the exchange (Kraken `EQuery:Unknown asset pair`).
    #[error("Symbol not found: {symbol}.")]
    SymbolNotFound {
        /// The symbol exactly as passed at the call site (e.g. `BTC/USD`).
        symbol: String,
        /// SDK-generated correlation id for the originating request, when available.
        request_id: Option<String>,
    },
    /// The exchange rejected the request parameters (Kraken `EGeneral:Invalid arguments`).
    #[error("Invalid arguments: {detail}.")]
    InvalidArguments {
        /// Raw Kraken error string passed through verbatim (trailing period trimmed).
        detail: String,
        /// SDK-generated correlation id for the originating request, when available.
        request_id: Option<String>,
    },
    /// The operation is blocked because the exchange is not at the system status it requires.
    #[error(
        "Operation blocked: exchange system status is \"{}\", but this operation \
         requires \"{}\" — retry once the status returns to the required level.",
        current.status,
        required.status
    )]
    SystemStatus {
        /// System status the exchange reported at the time of the call.
        current: SystemStatus,
        /// System status the operation requires to proceed.
        required: SystemStatus,
    },
    /// Rate-limit rejection — Kraken's throttle family or the SDK's own pre-flight tracker; retryable, and the SDK never blocks — the caller decides.
    #[error("Rate limited.")]
    RateLimited {
        /// Unix-seconds timestamp from Kraken's `EService:Throttled: <ts>` code; `None` when the throttle carried none.
        retry_after_ts: Option<u64>,
        /// SDK-generated correlation id for the originating request, when available.
        request_id: Option<String>,
    },
    /// Network-level failure before a well-formed exchange response arrived.
    #[error("Transport: {kind:?}")]
    Transport {
        /// What failed on the wire; see [`TransportErrorKind`].
        kind: TransportErrorKind,
        /// Whether a cooperative retry is worthwhile; feeds `retryable()`.
        transient: bool,
        /// SDK-generated correlation id for the originating request, when available.
        request_id: Option<String>,
    },
    /// SDK-side decode failure: HTTP 200, no Kraken error, but the `result`
    /// field could not be parsed into the expected typed shape.
    #[error("The server response could not be parsed.")]
    MalformedResponse {
        /// Decoder error text for diagnostics; not a stable format.
        detail: String,
        /// SDK-generated correlation id for the originating request, when available.
        request_id: Option<String>,
    },
    /// The client has been closed; the operation was rejected without touching the network.
    #[error("Client is closed; no new operations accepted.")]
    ClientClosed,
    /// Fallback for an error string with no typed mapping — unmapped Kraken codes, plus SDK auth/dispatch-miss failures.
    #[error("{kraken_message}")]
    Unknown {
        /// Stable `EClass` token from the `EClass:Detail` wire string (e.g. `EQuery`), or a synthetic `AUTH`/`INTERNAL` code for SDK-side failures.
        kraken_code: String,
        /// Full raw error string, passed through verbatim as the `Display` message.
        kraken_message: String,
        /// SDK-generated correlation id for the originating request, when available.
        request_id: Option<String>,
    },
}

crate::error::impl_with_request_id!(
    MarketError: SymbolNotFound, InvalidArguments, RateLimited, Transport,
    MalformedResponse, Unknown
);

crate::error::impl_error_common!(MarketError);

impl MarketError {
    /// Classify a [`RestError`] using the queried `symbol` as request context. Like
    /// `From<RestError>` but lifts Kraken's bad-pair rejection (`EQuery:Unknown asset
    /// pair`) to [`MarketError::SymbolNotFound`], which the symbol-less `From` cannot.
    pub(crate) fn from_rest_for_symbol(e: RestError, symbol: &str) -> Self {
        match Self::from(e) {
            MarketError::Unknown {
                ref kraken_message,
                ref request_id,
                ..
            } if kraken_message.contains("Unknown asset pair") => MarketError::SymbolNotFound {
                symbol: symbol.to_string(),
                request_id: request_id.clone(),
            },
            other => other,
        }
    }
}

crate::error::impl_from_rest_error!(MarketError);

impl crate::error::sealed::Sealed for MarketError {}

impl crate::error::ApiError for MarketError {
    fn code(&self) -> &str {
        use MarketError::*;
        match self {
            SymbolNotFound { .. } | InvalidArguments { .. } => "INVALID_ARGUMENTS",
            SystemStatus { .. } => "SYSTEM_STATUS_MAINTENANCE",
            RateLimited { .. } => "RATE_LIMIT_EXCEEDED",
            Transport { .. } => "CONNECTION_ERROR",
            MalformedResponse { .. } => "MALFORMED_RESPONSE",
            ClientClosed => "CLIENT_CLOSED",
            Unknown { .. } => "UNKNOWN",
        }
    }
    fn category(&self) -> crate::error::ErrorCategory {
        use crate::error::ErrorCategory;
        use MarketError::*;
        match self {
            SymbolNotFound { .. } | SystemStatus { .. } | Unknown { .. } => ErrorCategory::Exchange,
            InvalidArguments { .. } | MalformedResponse { .. } | ClientClosed => {
                ErrorCategory::Client
            }
            RateLimited { .. } => ErrorCategory::RateLimit,
            Transport { .. } => ErrorCategory::Network,
        }
    }
    fn retryable(&self) -> bool {
        use MarketError::*;
        match self {
            RateLimited { .. } => true,
            Transport { transient, .. } => *transient,
            _ => false,
        }
    }
    fn request_id(&self) -> Option<&str> {
        use MarketError::*;
        match self {
            SymbolNotFound { request_id, .. }
            | InvalidArguments { request_id, .. }
            | RateLimited { request_id, .. }
            | Transport { request_id, .. }
            | MalformedResponse { request_id, .. }
            | Unknown { request_id, .. } => request_id.as_deref(),
            SystemStatus { .. } | ClientClosed => None,
        }
    }
    crate::error::api_error_tail!(carried);
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::error::from_rest_taxonomy_tests!(MarketError);

    #[test]
    fn unknown_asset_pair_lifts_to_symbol_not_found_with_queried_symbol() {
        let e = MarketError::from_rest_for_symbol(kraken("EQuery:Unknown asset pair"), "FAKE/USD");
        assert_eq!(
            e,
            MarketError::SymbolNotFound {
                symbol: "FAKE/USD".to_string(),
                request_id: None,
            }
        );
    }

    #[test]
    fn plain_from_lacks_symbol_so_bad_pair_stays_unknown() {
        let e = MarketError::from(kraken("EQuery:Unknown asset pair"));
        assert!(matches!(e, MarketError::Unknown { .. }));
    }

    #[test]
    fn from_rest_for_symbol_passes_through_non_pair_errors() {
        assert!(matches!(
            MarketError::from_rest_for_symbol(kraken("EAPI:Rate limit exceeded"), "BTC/USD"),
            MarketError::RateLimited { .. }
        ));
        assert!(matches!(
            MarketError::from_rest_for_symbol(kraken("EGeneral:Invalid arguments"), "BTC/USD"),
            MarketError::InvalidArguments { .. }
        ));
        assert!(matches!(
            MarketError::from_rest_for_symbol(kraken("EService:Unavailable"), "BTC/USD"),
            MarketError::Unknown { .. }
        ));
    }

    #[test]
    fn too_many_requests_maps_to_rate_limited() {
        assert!(matches!(
            MarketError::from_rest_for_symbol(kraken("EGeneral:Too many requests"), "BTC/USD"),
            MarketError::RateLimited { .. }
        ));
    }
}
