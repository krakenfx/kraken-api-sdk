//! [`TerminationCause`] — subscription termination reason for the reactor,
//! event bus, managed connection, and await-handle layers.

/// Reason a subscription was terminated. Extending the set requires updating
/// the corresponding reactor and event-bus arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TerminationCause {
    /// Wire-side subscribe-ack retry budget exhausted.
    SubscribeAckBudgetExhausted,
    /// Wire-side rejection from the exchange that is non-transient.
    NonTransientWireRejection,
    /// Auth lost or capability revoked.
    CapabilityRevoked,
    /// Caller invoked `unsubscribe()` or dropped the subscription handle.
    ClientClosed,
}
