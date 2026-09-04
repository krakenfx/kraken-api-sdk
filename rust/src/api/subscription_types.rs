//! Subscription auxiliary read-models and [`SubscriptionError`], the error enum
//! returned by [`SubscriptionNamespace`](crate::api::SubscriptionNamespace)
//! subscribe/unsubscribe methods.

use std::collections::HashMap;

use crate::types::{ChannelName, Symbol};

/// Errors from `SubscriptionNamespace` subscribe and unsubscribe operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SubscriptionError {
    /// `subscribe_<channel>(...)` rejects when no handler is registered for
    /// `channel` at call time. Register a handler before subscribing.
    #[error("No handler registered for the {channel} channel; register one before subscribing.")]
    NoHandlerRegistered {
        /// Channel the rejected subscribe targeted.
        channel: ChannelName,
    },
    /// The internal operation queue was full; the subscribe request was rejected.
    /// The caller may retry; the subscription registry was not mutated.
    #[error("The operation queue is full; the subscribe request was rejected.")]
    QueueFull,
    /// The client has been closed and no longer accepts new operations.
    #[error("Client is closed; no new operations accepted.")]
    ClientClosed,
    /// The reactor loop has died; streaming is unavailable. Rebuild the client.
    #[error("The client's internal loop has failed; streaming is unavailable. Rebuild the client.")]
    LoopDead,
    /// Reserved for deferred methods; kept for cross-binding contract stability.
    #[error("Method `{method}` is not implemented in v1 (deferred to a v1.x slice).")]
    NotImplemented {
        /// Name of the deferred method that was called.
        method: &'static str,
    },
    /// Unmapped error from the exchange. The raw wire code and message are preserved.
    #[error("{kraken_message}")]
    Unknown {
        /// Raw Kraken wire error code (e.g. `EGeneral:Internal error`), preserved verbatim.
        kraken_code: String,
        /// Raw Kraken wire error message, preserved verbatim; also the `Display` output.
        kraken_message: String,
    },
}

impl crate::error::sealed::Sealed for SubscriptionError {}

impl crate::error::ApiError for SubscriptionError {
    fn code(&self) -> &str {
        use SubscriptionError::*;
        match self {
            NoHandlerRegistered { .. } => "INVALID_ARGUMENTS",
            QueueFull => "QUEUE_FULL",
            ClientClosed => "CLIENT_CLOSED",
            LoopDead => "LOOP_DEAD",
            NotImplemented { .. } => "NOT_IMPLEMENTED",
            Unknown { .. } => "UNKNOWN",
        }
    }
    fn category(&self) -> crate::error::ErrorCategory {
        use crate::error::ErrorCategory;
        use SubscriptionError::*;
        match self {
            NoHandlerRegistered { .. }
            | QueueFull
            | ClientClosed
            | LoopDead
            | NotImplemented { .. } => ErrorCategory::Client,
            Unknown { .. } => ErrorCategory::Exchange,
        }
    }
    fn retryable(&self) -> bool {
        matches!(self, SubscriptionError::QueueFull)
    }
    crate::error::api_error_tail!();
}

/// Public lifecycle state of a subscription entry.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionState {
    /// Subscribe acknowledged by Kraken; data flowing.
    Active,
    /// Subscribe frame posted, awaiting wire ack.
    Pending,
    /// Subscribe terminated; carries the public-surface cause.
    Failed(SubscribeFailureCause),
}

/// Public failure cause carried in [`SubscriptionState::Failed`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscribeFailureCause {
    /// The subscribe-ack timer exhausted its retry budget.
    SubscribeAckBudgetExhausted,
    /// Wire-side rejection from Kraken (e.g. `"EChannel:Invalid pair"`).
    NonTransientWireRejection {
        /// Raw Kraken wire error code from the rejection, preserved verbatim.
        code: String,
        /// Raw Kraken wire error message accompanying the rejection, preserved verbatim.
        message: String,
    },
    /// Capability was downgraded or revoked mid-flight.
    CapabilityRevoked,
    /// `Client::close()` was called during subscribe.
    ClientClosed,
    /// The subscription's connection is terminally failed — at bring-up or after
    /// the reconnect cap; also applied as a read-time overlay to live rows.
    ConnectionFailed,
    /// Token-gated subscribe failed during the auth handshake before the subscribe was sent.
    AuthHandshakeFailed,
}

/// Public read-model returned by `SubscriptionNamespace::list_active()`.
/// `pair` is `None` for channel-wide subscriptions (status, executions, balances).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionInfo {
    /// Channel this subscription is registered on.
    pub channel: ChannelName,
    /// Subscribed pair; `None` for channel-wide subscriptions (status, executions, balances).
    pub pair: Option<Symbol>,
    /// Current lifecycle state of the entry.
    pub state: SubscriptionState,
}

/// Lightweight read-only view returned by `SubscriptionNamespace::find_by_channel()`.
/// Adds `registered_at_monotonic` for caller-observable ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionRef {
    /// Channel this subscription is registered on.
    pub channel: ChannelName,
    /// Subscribed symbol; `None` for channel-wide subscriptions (status, executions, balances).
    pub symbol: Option<Symbol>,
    /// Current lifecycle state of the entry.
    pub state: SubscriptionState,
    /// Monotonic timestamp in milliseconds.
    pub registered_at_monotonic: u64,
}

/// Aggregate subscription counts returned by `SubscriptionNamespace::status_summary()`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionsSummary {
    /// Sum of `active + pending + failed`.
    pub total: u32,
    /// Count in [`SubscriptionState::Active`].
    pub active: u32,
    /// Count in [`SubscriptionState::Pending`].
    pub pending: u32,
    /// Count in [`SubscriptionState::Failed`].
    pub failed: u32,
    /// Per-channel count (sum across states).
    pub by_channel: HashMap<ChannelName, u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ApiError, ErrorCategory};

    #[test]
    fn no_handler_registered_api_error_mapping() {
        let err = SubscriptionError::NoHandlerRegistered {
            channel: ChannelName::Ticker,
        };
        assert_eq!(err.code(), "INVALID_ARGUMENTS");
        assert_eq!(err.category(), ErrorCategory::Client);
        assert!(!err.retryable());
        assert_eq!(err.kraken_code(), None);
        assert_eq!(err.request_id(), None);
    }

    #[test]
    fn queue_full_api_error_mapping() {
        let err = SubscriptionError::QueueFull;
        assert_eq!(err.code(), "QUEUE_FULL");
        assert_eq!(err.category(), ErrorCategory::Client);
        assert!(err.retryable());
        assert_eq!(err.kraken_code(), None);
    }

    #[test]
    fn not_implemented_api_error_mapping() {
        let err = SubscriptionError::NotImplemented {
            method: "subscribe_level3",
        };
        assert_eq!(err.code(), "NOT_IMPLEMENTED");
        assert_eq!(err.category(), ErrorCategory::Client);
        assert!(!err.retryable(), "a v1-deferral is never retryable");
        assert_eq!(err.kraken_code(), None);
        assert_eq!(err.request_id(), None);
        assert!(
            err.message().contains("subscribe_level3"),
            "the message names the deferred method"
        );
    }

    #[test]
    fn unknown_api_error_mapping_carries_kraken_code() {
        let err = SubscriptionError::Unknown {
            kraken_code: "EChannel:Unknown".into(),
            kraken_message: "channel rejected".into(),
        };
        assert_eq!(err.code(), "UNKNOWN");
        assert_eq!(err.category(), ErrorCategory::Exchange);
        assert!(!err.retryable());
        assert_eq!(err.kraken_code(), Some("EChannel:Unknown"));
        assert_eq!(err.message(), "channel rejected");
    }
}
