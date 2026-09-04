//! `AccountError` — error set for account-namespace operations.

use crate::transport::TransportErrorKind;

/// Errors returned by account-namespace (authenticated REST) operations.
/// The `#[error]` format strings are the canonical message templates;
/// `ApiError::message()` delegates to `Display`.
// `Eq` not derived — `Transport { kind }` carries non-Eq context in some variants.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum AccountError {
    /// Kraken rejected the request (`EGeneral:Permission denied`) — the API key lacks a required permission. Non-retryable.
    #[error("Permission denied.")]
    PermissionDenied {
        /// Reserved for the lacking API-key permission; always `None` in v1
        /// (no producer identifies the scope from the wire rejection).
        scope: Option<String>,
        /// Per-dispatch correlation id from the originating request; `None` until stamped.
        request_id: Option<String>,
    },
    /// Kraken rejected the request parameters (`Invalid arguments` error family). Non-retryable.
    #[error("Invalid arguments: {detail}.")]
    InvalidArguments {
        /// Raw Kraken error string passed through verbatim (trailing period trimmed).
        detail: String,
        /// Per-dispatch correlation id from the originating request; `None` until stamped.
        request_id: Option<String>,
    },
    /// Rate-limit rejection — Kraken's rate-limit/throttle family (`Rate limit`, `Throttled`, `Too many requests`) or the SDK's own pre-flight tracker. Retryable.
    #[error("Rate limited.")]
    RateLimited {
        /// Unix-seconds timestamp parsed from `EService:Throttled: <ts>`; `None` when the message carries none.
        retry_after_ts: Option<u64>,
        /// Per-dispatch correlation id from the originating request; `None` until stamped.
        request_id: Option<String>,
    },
    /// Network-layer failure with no Kraken-level response; retryable iff `transient`.
    #[error("Transport: {kind:?}")]
    Transport {
        /// Classification of the transport failure (DNS, TLS, timeout, reset, ...).
        kind: TransportErrorKind,
        /// Whether the failure is worth retrying; feeds `retryable()`.
        transient: bool,
        /// Per-dispatch correlation id from the originating request; `None` until stamped.
        request_id: Option<String>,
    },
    /// SDK-side decode failure: HTTP 200, no Kraken error, but the `result`
    /// field could not be parsed into the expected typed shape.
    #[error("The server response could not be parsed.")]
    MalformedResponse {
        /// Description of the decode failure (unexpected shape or serde error); not shown in `Display`.
        detail: String,
        /// Per-dispatch correlation id from the originating request; `None` until stamped.
        request_id: Option<String>,
    },
    /// Operation attempted after client shutdown; client-side, never sent to Kraken.
    #[error("Client is closed; no new operations accepted.")]
    ClientClosed,
    /// Fallback for an error with no typed mapping — unmapped Kraken string, auth-layer failure, or internal dispatch miss; message passed through verbatim.
    #[error("{kraken_message}")]
    Unknown {
        /// Stable `EClass` token from the `EClass:Detail` wire string (e.g. `EAccount`); `AUTH` for auth-layer errors, `INTERNAL` for dispatch misses.
        kraken_code: String,
        /// Raw upstream error message (Kraken wire string, or auth/internal error text), shown verbatim as `Display`.
        kraken_message: String,
        /// Per-dispatch correlation id from the originating request; `None` until stamped.
        request_id: Option<String>,
    },
}

crate::error::impl_with_request_id!(
    AccountError: PermissionDenied, InvalidArguments, RateLimited, Transport,
    MalformedResponse, Unknown
);

crate::error::impl_error_common!(AccountError);

crate::error::impl_from_rest_error!(
    AccountError,
    "Permission denied" => AccountError::PermissionDenied {
        scope: None,
        request_id: None,
    }
);

impl crate::error::sealed::Sealed for AccountError {}

impl crate::error::ApiError for AccountError {
    fn code(&self) -> &str {
        use AccountError::*;
        match self {
            PermissionDenied { .. } => "PERMISSION_DENIED",
            InvalidArguments { .. } => "INVALID_ARGUMENTS",
            RateLimited { .. } => "RATE_LIMIT_EXCEEDED",
            Transport { .. } => "CONNECTION_ERROR",
            MalformedResponse { .. } => "MALFORMED_RESPONSE",
            ClientClosed => "CLIENT_CLOSED",
            Unknown { .. } => "UNKNOWN",
        }
    }
    fn category(&self) -> crate::error::ErrorCategory {
        use crate::error::ErrorCategory;
        use AccountError::*;
        match self {
            PermissionDenied { .. } => ErrorCategory::Auth,
            InvalidArguments { .. } | MalformedResponse { .. } | ClientClosed => {
                ErrorCategory::Client
            }
            RateLimited { .. } => ErrorCategory::RateLimit,
            Transport { .. } => ErrorCategory::Network,
            Unknown { .. } => ErrorCategory::Exchange,
        }
    }
    fn retryable(&self) -> bool {
        use AccountError::*;
        match self {
            RateLimited { .. } => true,
            Transport { transient, .. } => *transient,
            _ => false,
        }
    }
    fn request_id(&self) -> Option<&str> {
        use AccountError::*;
        match self {
            PermissionDenied { request_id, .. }
            | InvalidArguments { request_id, .. }
            | RateLimited { request_id, .. }
            | Transport { request_id, .. }
            | MalformedResponse { request_id, .. }
            | Unknown { request_id, .. } => request_id.as_deref(),
            ClientClosed => None,
        }
    }
    crate::error::api_error_tail!(carried);
}

#[cfg(test)]
mod tests {
    use super::*;

    crate::error::from_rest_taxonomy_tests!(AccountError);

    #[test]
    fn permission_and_invalid_args_classify() {
        assert!(matches!(
            AccountError::from(kraken("EGeneral:Permission denied")),
            AccountError::PermissionDenied { .. }
        ));
        assert!(matches!(
            AccountError::from(kraken("EGeneral:Invalid arguments")),
            AccountError::InvalidArguments { .. }
        ));
    }
}
