//! Connection FSM state.

/// FSM state for a single managed WebSocket connection. Exactly 9 states.
/// Reconnect flows `BackingOff → Connecting → Authenticating → Resubscribing → Open`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ConnectionState {
    /// Initial state before any connect attempt; no socket exists yet.
    Idle = 0,
    /// TCP/TLS + WebSocket handshake in progress.
    Connecting = 1,
    /// Handshake done; exchanging the auth token (auth connection only).
    Authenticating = 2,
    /// Replaying prior channel subscriptions after a (re)connect.
    Resubscribing = 3,
    /// Fully established and subscribed; traffic flows.
    Open = 4,
    /// Waiting out a backoff or rate-budget delay before the next connect attempt.
    BackingOff = 5,
    /// Graceful shutdown in progress; exits on server close ack, socket drop, or close timeout.
    Closing = 6,
    /// Terminal state after a caller-requested close; no reconnect.
    Closed = 7,
    /// Reconnect cap exhausted or fatal error; recoverable only via `force_reconnect` or `close`.
    Failed = 8,
}

impl ConnectionState {
    /// Discriminant value for `AtomicU8` storage.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Reverse of [`as_u8`](Self::as_u8). Returns `None` for bytes outside the valid range.
    pub const fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Idle),
            1 => Some(Self::Connecting),
            2 => Some(Self::Authenticating),
            3 => Some(Self::Resubscribing),
            4 => Some(Self::Open),
            5 => Some(Self::BackingOff),
            6 => Some(Self::Closing),
            7 => Some(Self::Closed),
            8 => Some(Self::Failed),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_state_is_copy_and_eq() {
        let s = ConnectionState::Open;
        let copied = s;
        assert_eq!(s, copied);
        assert_eq!(ConnectionState::Idle, ConnectionState::Idle);
        assert_ne!(ConnectionState::Idle, ConnectionState::Open);
    }
}
