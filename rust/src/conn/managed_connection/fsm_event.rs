use crate::auth::AuthError;
use crate::conn::SubscribeErrorKind;
use crate::transport::{TransportError, TransportErrorKind};
use crate::types::{ChannelName, Symbol};

/// Closed enum of every event that drives the per-`ManagedConnection` FSM.
#[derive(Debug)]
pub enum FsmEvent {
    // Carry the supervisor handle ID, which the FSM stamps on
    // `EventEnvelope.request_id` so callers correlate response to request.
    CallStartConnect {
        request_id: u64,
    },
    CallClose {
        request_id: u64,
    },
    CallForceReconnect {
        request_id: u64,
    },

    WireUpgradeOk {
        connection_id: Option<u64>,
    },
    WireUpgradeFailed {
        http_status: u16,
    },
    WireConnectError {
        kind: TransportErrorKind,
        transient: bool,
    },
    WireCloseReceived {
        code: u16,
        reason: Option<String>,
    },
    WireAbnormalClose {
        // Part of the locked cross-binding FSM event shape the other language ports mirror.
        #[allow(dead_code)]
        error: TransportError,
    },
    // WireFrameIn/PingIn/PongIn/FragmentedFrameIn intentionally absent: inbound
    // frames are non-FSM-moving — they never become FsmEvents.
    WireAuthHandshakeOk,
    // Order ack accepted the token -> Authenticating->Open.
    WireOrderAuthOk,
    // EAPI:Invalid token on an Open session -> refresh, stay Open.
    WireOrderAckTokenStale,
    WireAuthHandshakeFailed {
        kind: AuthErrorKind,
    },
    WireSubscribeAck {
        channel: ChannelName,
        pair: Option<Symbol>,
        last: bool,
    },
    WireSubscribeFailed {
        channel: ChannelName,
        pair: Option<Symbol>,
        error: SubscribeErrorKind,
    },

    TimerBackoffElapsed,
    TimerStalenessElapsed,
    TimerUpgradeTimeout,
    TimerCloseTimeout,
    TimerSubscribeAckTimeout {
        channel: ChannelName,
        pair: Option<Symbol>,
    },
    TimerRateBudgetWindowAdvanced,
    /// Per-entry subscribe resend timer. Armed when a subscribe send fails with
    /// `Backpressure`. MUST be non-zero delay to avoid a hot-loop.
    TimerSubscribeResendDue {
        channel: ChannelName,
        pair: Option<Symbol>,
    },
    /// Per-`(Book, symbol)` reseed snapshot-liveness timer. Consecutive timeouts
    /// degrade to PassThrough. Single-shot.
    TimerBookReseedSnapshot {
        channel: ChannelName,
        pair: Option<Symbol>,
    },
    /// Proactive WS-token refresh tick (TTL × 0.5). Reactor-side, not a state change.
    TimerTokenRefreshDue,

    BusTokenRefreshed {
        // FSM arm ignores it. Part of the locked cross-binding event shape.
        #[allow(dead_code)]
        request_id: u64,
    },
    BusTokenRefreshFailed {
        // FSM arm ignores it. Part of the locked cross-binding event shape.
        #[allow(dead_code)]
        request_id: u64,
        error: AuthError,
    },
}

/// Auth-handshake error classification for FSM routing. Narrow discriminator for
/// the first-signed-subscribe ack rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthErrorKind {
    /// `EAPI:Invalid key` / `EAPI:Invalid signature` — non-transient → `Failed`.
    BadCreds,
    /// `EGeneral:Permission denied` — non-transient → `Failed`.
    PermissionDenied,
    /// Token-stale signal — re-enter `Authenticating` after `force_refresh`.
    TokenStale,
    /// `EService:*` / unrecognized — transient → `BackingOff`.
    Transient,
}
