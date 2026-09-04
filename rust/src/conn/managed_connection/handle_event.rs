use crate::conn::rate_budget::ConnectionRateThrottled;
use crate::types::{ConnectionState, WsUrl};

use super::{AuthErrorKind, DrainCause, FsmEvent, ManagedConnection};

impl ManagedConnection {
    /// Drive the FSM by consuming one event. Bounded sync work per arm; the I/O
    /// Reactor is the sole caller.
    pub fn handle_event(&mut self, event: FsmEvent) {
        use ConnectionState::*;
        use FsmEvent::*;

        let from = self.state;
        tracing::trace!(
            target: "kraken_sdk::fsm",
            url = ?self.url,
            ?from,
            "handle_event entered"
        );

        let _ = (&self.awaiting_token_refresh, &self.socket, &self.bus);

        match (from, event) {
            (Idle, CallStartConnect { request_id }) => {
                let now = self.clock.now();
                if let Err(ConnectionRateThrottled {
                    attempts_used,
                    budget,
                    throttle_until_monotonic,
                }) = self.rate_budget.try_consume(now)
                {
                    self.state = BackingOff;
                    self.pending_connect_request_id = Some(request_id);
                    self.timers.rate_budget_window_advanced_at = Some(throttle_until_monotonic);
                    self.emit_rate_throttled(
                        Some(request_id),
                        attempts_used,
                        budget,
                        throttle_until_monotonic,
                    );
                    tracing::debug!(
                        target: "kraken_sdk::fsm",
                        url = ?self.url,
                        attempts_used,
                        budget,
                        "Idle → BackingOff (StartConnect throttled by ConnectionRateBudget)"
                    );
                    return;
                }
                self.attempt_count = self.attempt_count.saturating_add(1);
                self.state = Connecting;
                self.pending_connect_request_id = Some(request_id);
                self.enter_connecting(Some(request_id));
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    attempt_count = self.attempt_count,
                    "Idle → Connecting (StartConnect; budget consumed, socket opened via factory)"
                );
            }
            (Idle, CallClose { request_id }) => {
                self.state = Closed;
                self.emit_lifecycle(
                    crate::dispatch::EventType::ConnectionClosedEvent,
                    crate::dispatch::EventPayload::ConnectionClosedEvent {
                        url: self.url,
                        closed_at_monotonic: self.clock.now(),
                        reason: crate::dispatch::ClosedReason::ClientInitiated,
                        ack: crate::dispatch::AckSource::NeverOpened,
                        server_connection_id: None,
                    },
                    Some(request_id),
                );
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Idle → Closed (Close)"
                );
            }
            (Idle, CallForceReconnect { request_id }) => {
                self.attempt_count = 0;
                // force_reconnect is NOT a budget bypass: consult the rate budget like
                // every re-attempt path.
                let now = self.clock.now();
                if let Err(ConnectionRateThrottled {
                    attempts_used,
                    budget,
                    throttle_until_monotonic,
                }) = self.rate_budget.try_consume(now)
                {
                    self.state = BackingOff;
                    self.pending_connect_request_id = Some(request_id);
                    self.timers.rate_budget_window_advanced_at = Some(throttle_until_monotonic);
                    self.emit_rate_throttled(
                        Some(request_id),
                        attempts_used,
                        budget,
                        throttle_until_monotonic,
                    );
                    tracing::debug!(
                        target: "kraken_sdk::fsm",
                        url = ?self.url,
                        "Idle → BackingOff (ForceReconnect throttled by budget)"
                    );
                    return;
                }
                self.state = Connecting;
                self.pending_connect_request_id = Some(request_id);
                // Without these the reactor sees socket()==None, wires no bridge/timeout,
                // and the connection wedges permanently in Connecting.
                self.enter_connecting(Some(request_id));
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Idle → Connecting (ForceReconnect; attempt_count reset, socket opened)"
                );
            }

            (Connecting, WireUpgradeOk { connection_id }) => {
                self.server_connection_id = connection_id;
                // Disarm the upgrade-timeout; else it leaks into Resubscribing/Authenticating.
                self.timers.upgrade_timeout_due_at = None;
                let next = match self.url {
                    WsUrl::Public => Resubscribing,
                    WsUrl::Auth => Authenticating,
                };
                self.state = next;
                self.arm_staleness();
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    ?next,
                    "Connecting → next (UpgradeOk; staleness armed)"
                );
            }
            (Connecting, WireUpgradeFailed { http_status }) => {
                let transient = crate::transport::http_upgrade_status_transient(http_status);
                if transient {
                    if self.cap_exhausted_then_escalate(|_| {}) {
                        return;
                    }
                    self.state = BackingOff;
                    self.socket = None;
                    self.timers.upgrade_timeout_due_at = None;
                    let backoff_due = self.compute_backoff_due();
                    self.timers.backoff_due_at = Some(backoff_due);
                    self.emit_attempt_failed(
                        backoff_due,
                        crate::dispatch::TransientClass::HttpTransient,
                        None,
                        Some(http_status),
                        None,
                    );
                } else {
                    self.state = Failed;
                    self.socket = None;
                    // Direct Connecting → Failed bypasses cap_exhausted_then_escalate's
                    // timer teardown; clear the upgrade-timeout here or it leaks into Failed.
                    self.timers.upgrade_timeout_due_at = None;
                    self.emit_connection_failed(
                        format!("ws upgrade failed: HTTP {http_status} (non-transient)"),
                        false,
                        None,
                    );
                }
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    http_status,
                    transient,
                    "Connecting → BackingOff/Failed (UpgradeFailed)"
                );
            }
            (Connecting, WireConnectError { kind, transient }) => {
                if transient {
                    if self.cap_exhausted_then_escalate(|_| {}) {
                        return;
                    }
                    self.state = BackingOff;
                    self.socket = None;
                    self.timers.upgrade_timeout_due_at = None;
                    let backoff_due = self.compute_backoff_due();
                    self.timers.backoff_due_at = Some(backoff_due);
                    self.emit_attempt_failed(
                        backoff_due,
                        crate::dispatch::TransientClass::ConnectError,
                        None,
                        None,
                        None,
                    );
                } else {
                    self.state = Failed;
                    self.socket = None;
                    self.timers.upgrade_timeout_due_at = None;
                    self.emit_connection_failed(
                        format!("connect error (non-transient): {kind:?}"),
                        false,
                        None,
                    );
                }
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    transient,
                    "Connecting → BackingOff/Failed (ConnectError)"
                );
            }
            (Connecting, TimerUpgradeTimeout) => {
                self.socket = None;
                self.timers.upgrade_timeout_due_at = None;
                if self.cap_exhausted_then_escalate(|_| {}) {
                    return;
                }
                self.state = BackingOff;
                let backoff_due = self.compute_backoff_due();
                self.timers.backoff_due_at = Some(backoff_due);
                self.emit_attempt_failed(
                    backoff_due,
                    crate::dispatch::TransientClass::UpgradeTimeout,
                    None,
                    None,
                    None,
                );
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Connecting → BackingOff (UpgradeTimeout — treated as transient)"
                );
            }
            (Connecting, CallClose { request_id }) => {
                self.state = Closing;
                self.timers.upgrade_timeout_due_at = None;
                self.arm_close_timeout();
                self.pending_close_request_id = Some(request_id);
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Connecting → Closing (Close)"
                );
            }
            (Connecting, CallForceReconnect { request_id }) => {
                self.attempt_count = 0;
                // First bring-up (never opened) is NOT cohorted (its event is
                // ConnectionOpenEvent, not Reopened).
                if self.has_been_open {
                    self.additional_connect_request_ids.push(request_id);
                }
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Connecting → Connecting (ForceReconnect; restart)"
                );
            }

            (Authenticating, WireAuthHandshakeOk) => {
                self.awaiting_token_refresh = false;
                // Reset the consecutive-handshake-fail counter only on a SUCCESSFUL
                // handshake, not on enter_open — a reconnect must not clear the streak.
                self.auth_handshake_fail_count = 0;
                self.state = Resubscribing;
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Authenticating → Resubscribing (auth_handshake_ok)"
                );
            }

            // Gated on TOKEN-ACCEPTED, not order-succeeded: an order rejection on an
            // accepted token still auths.
            (Authenticating, WireOrderAuthOk) => {
                self.awaiting_token_refresh = false;
                self.auth_handshake_fail_count = 0;
                self.clear_subscribe_ack_state();
                self.enter_open();
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Authenticating → Open (WireOrderAuthOk; bare-order self-auth, Path C)"
                );
            }

            (
                Authenticating,
                WireAuthHandshakeFailed {
                    kind: AuthErrorKind::TokenStale,
                },
            ) => {
                // auth_handshake_fail_count (gates the cap) is separate from
                // attempt_count (overall attempt tally). Both bump on token-stale.
                self.auth_handshake_fail_count = self.auth_handshake_fail_count.saturating_add(1);
                let m_cap = self
                    .bus
                    .knobs()
                    .max_auth_handshake_failures
                    .load(std::sync::atomic::Ordering::Relaxed);
                if self.auth_handshake_fail_count >= m_cap {
                    // Drain resolves all in-flight requests Err BEFORE the lifecycle emit below.
                    self.exit_authenticating_teardown();
                    self.state = Failed;
                    self.emit_connection_failed(
                        format!(
                            "auth handshake token-stale retried {} times (cap)",
                            self.auth_handshake_fail_count
                        ),
                        false,
                        Some(crate::dispatch::NonTransientClass::RetryCapExhausted),
                    );
                    self.emit_lifecycle(
                        crate::dispatch::EventType::AuthenticationFailedEvent,
                        crate::dispatch::EventPayload::AuthenticationFailedEvent {
                            url: self.url,
                            non_transient_class:
                                crate::dispatch::NonTransientClass::RetryCapExhausted,
                            kraken_response: None,
                        },
                        None,
                    );
                    tracing::warn!(
                        target: "kraken_sdk::fsm", url = ?self.url,
                        auth_handshake_fail_count = self.auth_handshake_fail_count,
                        "Authenticating → Failed (token-stale handshake cap reached)"
                    );
                    return;
                }
                self.awaiting_token_refresh = true;
                self.attempt_count = self.attempt_count.saturating_add(1);
                tracing::debug!(
                    target: "kraken_sdk::fsm", url = ?self.url,
                    auth_handshake_fail_count = self.auth_handshake_fail_count,
                    attempt_count = self.attempt_count,
                    "Authenticating (re-enter) — token-stale; reactor force_refresh in flight (+1 auth-fail, +1 attempt)"
                );
            }

            (
                Authenticating,
                WireAuthHandshakeFailed {
                    kind: kind @ (AuthErrorKind::BadCreds | AuthErrorKind::PermissionDenied),
                },
            ) => {
                self.exit_authenticating_teardown();
                self.state = Failed;
                let cause = match kind {
                    AuthErrorKind::BadCreds => "BadCreds",
                    AuthErrorKind::PermissionDenied => "PermissionDenied",
                    _ => "AuthHandshakeFailed",
                };
                let non_transient_class = match kind {
                    AuthErrorKind::PermissionDenied => {
                        crate::dispatch::NonTransientClass::PermissionDenied
                    }
                    _ => crate::dispatch::NonTransientClass::BadCreds,
                };
                self.emit_connection_failed(
                    format!("auth handshake failed: {cause}"),
                    false,
                    Some(non_transient_class),
                );
                self.emit_lifecycle(
                    crate::dispatch::EventType::AuthenticationFailedEvent,
                    crate::dispatch::EventPayload::AuthenticationFailedEvent {
                        url: self.url,
                        non_transient_class,
                        kraken_response: None,
                    },
                    None,
                );
                tracing::warn!(
                    target: "kraken_sdk::fsm", url = ?self.url, cause,
                    "Authenticating → Failed (auth_handshake_failed non-transient)"
                );
            }

            (
                Authenticating,
                WireAuthHandshakeFailed {
                    kind: AuthErrorKind::Transient,
                },
            ) => {
                self.exit_authenticating_teardown();
                // The token-stale arm has its own consecutive-failure cap
                // (max_auth_handshake_failures) and is NOT gated here, avoiding double-capping.
                if self.cap_exhausted_then_escalate(|_| {}) {
                    return;
                }
                self.state = BackingOff;
                let backoff_due = self.compute_backoff_due();
                self.timers.backoff_due_at = Some(backoff_due);
                // PRE-Open transient → BackingOff. Emits AttemptFailed instead of
                // ConnectionDroppedEvent.
                self.emit_attempt_failed(
                    backoff_due,
                    crate::dispatch::TransientClass::AuthHandshakeTransient,
                    None,
                    None,
                    None,
                );
                tracing::debug!(
                    target: "kraken_sdk::fsm", url = ?self.url,
                    "Authenticating → BackingOff (auth_handshake_failed transient; backoff armed)"
                );
            }

            // Close-code rule: 1008 → Failed, all other codes → default-transient → BackingOff.
            (Authenticating, WireCloseReceived { code, reason }) => {
                self.exit_authenticating_teardown();
                if code == 1008 {
                    self.state = Failed;
                    self.emit_connection_failed(
                        format!("ws close {code} during auth (policy violation)"),
                        false,
                        None,
                    );
                } else {
                    if self.cap_exhausted_then_escalate(|_| {}) {
                        return;
                    }
                    self.state = BackingOff;
                    let backoff_due = self.compute_backoff_due();
                    self.timers.backoff_due_at = Some(backoff_due);
                    self.emit_attempt_failed(
                        backoff_due,
                        crate::dispatch::TransientClass::ClosedDuringHandshake,
                        reason,
                        None,
                        Some(code),
                    );
                }
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url, code,
                    "Authenticating → BackingOff/Failed (close during handshake)");
            }
            (Authenticating, WireAbnormalClose { .. }) => {
                self.exit_authenticating_teardown();
                if self.cap_exhausted_then_escalate(|_| {}) {
                    return;
                }
                self.state = BackingOff;
                let backoff_due = self.compute_backoff_due();
                self.timers.backoff_due_at = Some(backoff_due);
                self.emit_attempt_failed(
                    backoff_due,
                    crate::dispatch::TransientClass::ClosedDuringHandshake,
                    None,
                    None,
                    None,
                );
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Authenticating → BackingOff (abnormal close during handshake)");
            }
            (Authenticating, TimerStalenessElapsed) => {
                // Emit WebsocketStaleEvent on EVERY staleness teardown before the cap
                // check. Read last_inbound_at/window from the monitor BEFORE it is
                // disarmed below.
                let stale_last_inbound = self
                    .staleness_monitor
                    .last_inbound_at()
                    .unwrap_or_else(crate::types::MonotonicInstant::now);
                let stale_window_ms = self.staleness_monitor.window_ms();
                self.emit_lifecycle(
                    crate::dispatch::EventType::WebsocketStaleEvent,
                    crate::dispatch::EventPayload::WebsocketStaleEvent {
                        url: self.url,
                        last_inbound_at_monotonic: stale_last_inbound,
                        configured_window_ms: stale_window_ms,
                    },
                    None,
                );
                self.exit_authenticating_teardown();
                if self.cap_exhausted_then_escalate(|_| {}) {
                    return;
                }
                self.state = BackingOff;
                let backoff_due = self.compute_backoff_due();
                self.timers.backoff_due_at = Some(backoff_due);
                self.emit_attempt_failed(
                    backoff_due,
                    crate::dispatch::TransientClass::ClosedDuringHandshake,
                    None,
                    None,
                    None,
                );
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Authenticating → BackingOff (staleness during handshake)");
            }

            // Branches on error.retryable(): retryable → BackingOff; non-retryable → Failed.
            (Authenticating, BusTokenRefreshFailed { error, .. }) => {
                // Without the drain a bare order recorded during Authenticating hangs forever.
                self.exit_authenticating_teardown();
                if crate::error::ApiError::retryable(&error) {
                    if self.cap_exhausted_then_escalate(|_| {}) {
                        return;
                    }
                    self.state = BackingOff;
                    let backoff_due = self.compute_backoff_due();
                    self.timers.backoff_due_at = Some(backoff_due);
                    self.emit_attempt_failed(
                        backoff_due,
                        crate::dispatch::TransientClass::TokenRefreshTransient,
                        None,
                        None,
                        None,
                    );
                    tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                        "Authenticating → BackingOff (token refresh transient; retrying)");
                } else {
                    self.state = Failed;
                    self.emit_connection_failed(
                        format!("AuthRefreshFailed: {error}"),
                        false,
                        Some(crate::dispatch::NonTransientClass::ClientError),
                    );
                    self.emit_lifecycle(
                        crate::dispatch::EventType::AuthenticationFailedEvent,
                        crate::dispatch::EventPayload::AuthenticationFailedEvent {
                            url: self.url,
                            non_transient_class: crate::dispatch::NonTransientClass::ClientError,
                            kraken_response: Some(error.to_string()),
                        },
                        None,
                    );
                    tracing::warn!(target: "kraken_sdk::fsm", url = ?self.url,
                        "Authenticating → Failed (force_refresh REST failure; AuthRefreshFailed)");
                }
            }
            (Authenticating, BusTokenRefreshed { .. }) => {
                self.awaiting_token_refresh = false;
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Authenticating: fresh token cached (BusTokenRefreshed); reactor re-issues signed subscribe");
            }
            (Authenticating, TimerSubscribeAckTimeout { channel, pair }) => {
                // First signed subscribe is the auth probe; a lost ack is a
                // per-entry transient — retry under budget, else tear down.
                self.handle_subscribe_failure(channel, pair, true, None);
            }

            (Authenticating, CallClose { request_id }) => {
                // Socket is KEPT for the close handshake and there is NO drain (the
                // close path resolves in-flight requests on the Closing exit).
                self.exit_authenticating_to_closing();
                self.state = Closing;
                self.arm_close_timeout();
                self.pending_close_request_id = Some(request_id);
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Authenticating → Closing (Close)"
                );
            }
            (Authenticating, CallForceReconnect { request_id }) => {
                // Socket KEPT, NO drain, NO close frame (Closing exits via close-timeout).
                self.exit_authenticating_to_closing();
                self.state = Closing;
                self.arm_close_timeout();
                self.pending_reconnect_request_id = Some(request_id);
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Authenticating → Closing (ForceReconnect; abandon handshake, reconnect latched)");
            }

            (
                Resubscribing,
                WireSubscribeAck {
                    last: false,
                    channel,
                    pair,
                },
            ) => {
                self.disarm_subscribe_ack(channel, pair);
            }
            (
                Resubscribing,
                WireSubscribeAck {
                    last: true,
                    channel,
                    pair,
                },
            ) => {
                self.disarm_subscribe_ack(channel, pair);
                self.enter_open();
            }
            (Resubscribing, CallClose { request_id }) => {
                self.state = Closing;
                self.staleness_monitor.disarm();
                self.timers.staleness_due_at = None;
                // Else a mid-bring-up ack timer leaks into Closing.
                self.clear_subscribe_ack_state();
                self.arm_close_timeout();
                self.pending_close_request_id = Some(request_id);
            }
            (Resubscribing, CallForceReconnect { request_id }) => {
                // The close frame drives the server-ack fast path (socket is live
                // post-upgrade), not just the timeout.
                self.state = Closing;
                self.staleness_monitor.disarm();
                self.timers.staleness_due_at = None;
                self.clear_subscribe_ack_state();
                self.arm_close_timeout();
                self.pending_reconnect_request_id = Some(request_id);
                if let Some(s) = self.socket.as_ref() {
                    s.close(1000);
                }
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Resubscribing → Closing (ForceReconnect; close frame sent, reconnect latched)"
                );
            }
            (Resubscribing, TimerSubscribeAckTimeout { channel, pair }) => {
                self.handle_subscribe_failure(channel, pair, true, None);
            }
            (
                Resubscribing,
                WireSubscribeFailed {
                    channel,
                    pair,
                    error,
                },
            ) => {
                let crate::conn::SubscribeErrorKind::SubscribeRejected {
                    transient,
                    kraken_code,
                } = error;
                self.handle_subscribe_failure(channel, pair, transient, Some(kraken_code));
            }
            (Resubscribing, WireCloseReceived { code, reason }) => {
                self.exit_resubscribing_teardown();
                if code == 1008 {
                    self.state = Failed;
                    self.emit_connection_failed(
                        format!("ws close {code} during resubscribe (policy violation)"),
                        false,
                        None,
                    );
                } else {
                    if self.cap_exhausted_then_escalate(|_| {}) {
                        return;
                    }
                    self.state = BackingOff;
                    let backoff_due = self.compute_backoff_due();
                    self.timers.backoff_due_at = Some(backoff_due);
                    self.emit_attempt_failed(
                        backoff_due,
                        crate::dispatch::TransientClass::ClosedDuringHandshake,
                        reason,
                        None,
                        Some(code),
                    );
                }
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url, code,
                    "Resubscribing → BackingOff/Failed (close during subscribe replay)");
            }
            (Resubscribing, WireAbnormalClose { .. }) => {
                self.exit_resubscribing_teardown();
                if self.cap_exhausted_then_escalate(|_| {}) {
                    return;
                }
                self.state = BackingOff;
                let backoff_due = self.compute_backoff_due();
                self.timers.backoff_due_at = Some(backoff_due);
                self.emit_attempt_failed(
                    backoff_due,
                    crate::dispatch::TransientClass::ClosedDuringHandshake,
                    None,
                    None,
                    None,
                );
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Resubscribing → BackingOff (abnormal close during subscribe replay)");
            }
            (Resubscribing, TimerStalenessElapsed) => {
                let stale_last_inbound = self
                    .staleness_monitor
                    .last_inbound_at()
                    .unwrap_or_else(crate::types::MonotonicInstant::now);
                let stale_window_ms = self.staleness_monitor.window_ms();
                self.emit_lifecycle(
                    crate::dispatch::EventType::WebsocketStaleEvent,
                    crate::dispatch::EventPayload::WebsocketStaleEvent {
                        url: self.url,
                        last_inbound_at_monotonic: stale_last_inbound,
                        configured_window_ms: stale_window_ms,
                    },
                    None,
                );
                self.exit_resubscribing_teardown();
                if self.cap_exhausted_then_escalate(|_| {}) {
                    return;
                }
                self.state = BackingOff;
                let backoff_due = self.compute_backoff_due();
                self.timers.backoff_due_at = Some(backoff_due);
                self.emit_attempt_failed(
                    backoff_due,
                    crate::dispatch::TransientClass::ClosedDuringHandshake,
                    None,
                    None,
                    None,
                );
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Resubscribing → BackingOff (StalenessElapsed during subscribe replay)");
            }

            (Resubscribing, BusTokenRefreshed { .. }) => {
                self.awaiting_token_refresh = false;
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Resubscribing: fresh token cached (BusTokenRefreshed); reactor drains deferred auth subscribe(s)");
            }
            (Resubscribing, BusTokenRefreshFailed { .. }) => {
                self.awaiting_token_refresh = false;
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Resubscribing: token refresh failed; entries stay deferred for a later attempt");
            }

            (Open, WireSubscribeAck { channel, pair, .. }) => {
                self.disarm_subscribe_ack(channel, pair);
            }
            (Open, BusTokenRefreshed { .. }) => {
                self.awaiting_token_refresh = false;
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Open: fresh token cached (BusTokenRefreshed); reactor re-issues deferred auth subscribe(s)");
            }
            // No teardown — must NOT drop the connection or other subs.
            (Open, BusTokenRefreshFailed { .. }) => {
                self.awaiting_token_refresh = false;
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Open: token refresh failed for deferred auth subscribe; staying Open, entry remains deferred");
            }
            // STAY Open (session + subs preserved; no teardown). Refreshed token
            // serves the NEXT order; stale order never resent.
            (Open, WireOrderAckTokenStale) => {
                self.awaiting_token_refresh = true;
                tracing::debug!(target: "kraken_sdk::fsm", url = ?self.url,
                    "Open: order-ack TokenStale; force_refresh in flight, staying Open (session + subs preserved, NO teardown)");
            }
            (
                Open,
                WireSubscribeFailed {
                    channel,
                    pair,
                    error,
                },
            ) => {
                // Stay Open — one entry's failure never recycles the healthy connection.
                let crate::conn::SubscribeErrorKind::SubscribeRejected {
                    kraken_code,
                    transient,
                } = error;
                self.handle_subscribe_failure(channel, pair, transient, Some(kraken_code));
            }
            (Open, TimerSubscribeAckTimeout { channel, pair }) => {
                // Per-entry TRANSIENT (frame out, no response), NOT a wire rejection.
                self.handle_subscribe_failure(channel, pair, true, None);
            }
            (Open, WireCloseReceived { code, reason }) => {
                // Drain in-flight WS requests BEFORE the ConnectionDroppedEvent below.
                self.exit_open_teardown(DrainCause::RequestInFlightWhenDropped);
                self.clear_subscribe_ack_state();
                if code == 1008 {
                    // Registry preserved — only → Closed clears it.
                    self.state = Failed;
                    self.emit_connection_failed(
                        format!("ws close {code} (policy violation; non-transient)"),
                        false,
                        None,
                    );
                    tracing::debug!(
                        target: "kraken_sdk::fsm",
                        url = ?self.url, code,
                        "Open → Failed (CloseReceived 1008 policy violation; non-transient)"
                    );
                } else {
                    if self.cap_exhausted_then_escalate(|_| {}) {
                        return;
                    }
                    self.state = BackingOff;
                    self.timers.backoff_due_at = Some(self.compute_backoff_due());
                    self.emit_lifecycle(
                        crate::dispatch::EventType::ConnectionDroppedEvent,
                        crate::dispatch::EventPayload::ConnectionDroppedEvent {
                            url: self.url,
                            dropped_at_monotonic: self.clock.now(),
                            close_code: Some(code),
                            reason,
                        },
                        None,
                    );
                    tracing::debug!(
                        target: "kraken_sdk::fsm",
                        url = ?self.url, code,
                        "Open → BackingOff (CloseReceived transient; drained, emitted, backoff armed)"
                    );
                }
            }
            (Open, WireAbnormalClose { .. }) => {
                // Abnormal close is ALWAYS default-transient → BackingOff.
                self.exit_open_teardown(DrainCause::RequestInFlightWhenDropped);
                self.clear_subscribe_ack_state();
                if self.cap_exhausted_then_escalate(|_| {}) {
                    return;
                }
                self.state = BackingOff;
                self.timers.backoff_due_at = Some(self.compute_backoff_due());
                self.emit_lifecycle(
                    crate::dispatch::EventType::ConnectionDroppedEvent,
                    crate::dispatch::EventPayload::ConnectionDroppedEvent {
                        url: self.url,
                        dropped_at_monotonic: self.clock.now(),
                        close_code: None, // no server frame ⇒ no code/reason
                        reason: None,
                    },
                    None,
                );
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Open → BackingOff (AbnormalClose transient; drained, emitted, backoff armed)"
                );
            }
            (Open, TimerStalenessElapsed) => {
                let stale_last_inbound = self
                    .staleness_monitor
                    .last_inbound_at()
                    .unwrap_or_else(crate::types::MonotonicInstant::now);
                let stale_window_ms = self.staleness_monitor.window_ms();
                self.emit_lifecycle(
                    crate::dispatch::EventType::WebsocketStaleEvent,
                    crate::dispatch::EventPayload::WebsocketStaleEvent {
                        url: self.url,
                        last_inbound_at_monotonic: stale_last_inbound,
                        configured_window_ms: stale_window_ms,
                    },
                    None,
                );
                self.exit_open_teardown(DrainCause::RequestInFlightWhenDropped);
                self.clear_subscribe_ack_state();
                if self.cap_exhausted_then_escalate(|_| {}) {
                    return;
                }
                self.state = BackingOff;
                self.timers.backoff_due_at = Some(self.compute_backoff_due());
                self.emit_lifecycle(
                    crate::dispatch::EventType::ConnectionDroppedEvent,
                    crate::dispatch::EventPayload::ConnectionDroppedEvent {
                        url: self.url,
                        dropped_at_monotonic: self.clock.now(),
                        close_code: None,
                        reason: None,
                    },
                    None,
                );
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Open → BackingOff (StalenessElapsed; emitted WebsocketStaleEvent + ConnectionDroppedEvent, drained in-flight requests, backoff armed)"
                );
            }
            (Open, CallClose { request_id }) => {
                // Drain in-flight WS requests BEFORE sending the close frame.
                self.pending_requests
                    .drain(DrainCause::ClientClosed, &self.bus);
                self.state = Closing;
                self.staleness_monitor.disarm();
                self.timers.staleness_due_at = None;
                self.clear_subscribe_ack_state();
                self.arm_close_timeout();
                self.pending_close_request_id = Some(request_id);
                if let Some(s) = self.socket.as_ref() {
                    s.close(1000);
                }
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Open → Closing (Close; drained in-flight requests, close frame sent)"
                );
            }
            (Open, CallForceReconnect { request_id }) => {
                self.pending_requests
                    .drain(DrainCause::RequestInFlightWhenDropped, &self.bus);
                self.state = Closing;
                self.staleness_monitor.disarm();
                self.timers.staleness_due_at = None;
                self.clear_subscribe_ack_state();
                self.arm_close_timeout();
                self.pending_reconnect_request_id = Some(request_id);
                // Send the close frame so Closing exits via the server-ack fast path,
                // not only the close-timeout.
                if let Some(s) = self.socket.as_ref() {
                    s.close(1000);
                }
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Open → Closing (ForceReconnect; drained, close frame sent, reconnect latched)"
                );
            }

            (BackingOff, TimerBackoffElapsed) => {
                // On Err, stay BackingOff and re-arm the rate-budget timer; do NOT
                // consume a regular backoff retry.
                let now = self.clock.now();
                if let Err(ConnectionRateThrottled {
                    attempts_used,
                    budget,
                    throttle_until_monotonic,
                }) = self.rate_budget.try_consume(now)
                {
                    self.timers.rate_budget_window_advanced_at = Some(throttle_until_monotonic);
                    self.emit_rate_throttled(None, attempts_used, budget, throttle_until_monotonic);
                    tracing::debug!(
                        target: "kraken_sdk::fsm",
                        url = ?self.url,
                        attempts_used,
                        budget,
                        "BackingOff stays (TimerBackoffElapsed but budget exhausted; re-armed rate-budget timer)"
                    );
                    return;
                }
                self.timers.backoff_due_at = None;
                self.attempt_count = self.attempt_count.saturating_add(1);
                self.state = Connecting;
                self.enter_connecting(None);
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    attempt_count = self.attempt_count,
                    "BackingOff → Connecting (BackoffElapsed; budget consumed)"
                );
            }
            (BackingOff, TimerRateBudgetWindowAdvanced) => {
                // MUST consult the budget; if still exhausted (race between consult and
                // fire), re-arm and stay.
                let now = self.clock.now();
                if let Err(ConnectionRateThrottled {
                    attempts_used,
                    budget,
                    throttle_until_monotonic,
                }) = self.rate_budget.try_consume(now)
                {
                    self.timers.rate_budget_window_advanced_at = Some(throttle_until_monotonic);
                    self.emit_rate_throttled(None, attempts_used, budget, throttle_until_monotonic);
                    return;
                }
                self.timers.rate_budget_window_advanced_at = None;
                self.attempt_count = self.attempt_count.saturating_add(1);
                self.state = Connecting;
                self.enter_connecting(None);
            }
            (BackingOff, CallClose { request_id }) => {
                self.state = Closed;
                self.emit_lifecycle(
                    crate::dispatch::EventType::ConnectionClosedEvent,
                    crate::dispatch::EventPayload::ConnectionClosedEvent {
                        url: self.url,
                        closed_at_monotonic: self.clock.now(),
                        reason: crate::dispatch::ClosedReason::ClientInitiated,
                        ack: crate::dispatch::AckSource::SocketDropped,
                        server_connection_id: None,
                    },
                    Some(request_id),
                );
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "BackingOff → Closed (Close)"
                );
            }
            (BackingOff, CallForceReconnect { request_id }) => {
                let now = self.clock.now();
                if let Err(ConnectionRateThrottled {
                    attempts_used,
                    budget,
                    throttle_until_monotonic,
                }) = self.rate_budget.try_consume(now)
                {
                    // Overwriting a prior waiter's latch would strand its await.
                    match self.pending_connect_request_id {
                        None => self.pending_connect_request_id = Some(request_id),
                        Some(_) => self.additional_connect_request_ids.push(request_id),
                    }
                    self.timers.rate_budget_window_advanced_at = Some(throttle_until_monotonic);
                    self.emit_rate_throttled(
                        Some(request_id),
                        attempts_used,
                        budget,
                        throttle_until_monotonic,
                    );
                    tracing::debug!(
                        target: "kraken_sdk::fsm",
                        url = ?self.url,
                        "BackingOff stays (ForceReconnect throttled by budget)"
                    );
                    return;
                }
                self.timers.backoff_due_at = None;
                self.attempt_count = 0;
                self.state = Connecting;
                match self.pending_connect_request_id {
                    None => self.pending_connect_request_id = Some(request_id),
                    Some(_) => self.additional_connect_request_ids.push(request_id),
                }
                self.enter_connecting(Some(request_id));
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "BackingOff → Connecting (ForceReconnect; budget consumed)"
                );
            }

            (Closing, WireCloseReceived { .. }) => {
                self.enter_closed_or_reconnect(
                    crate::dispatch::ClosedReason::ClientInitiated,
                    crate::dispatch::AckSource::Server,
                );
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Closing → Closed/Connecting (CloseReceived; server ack)"
                );
            }
            (Closing, WireAbnormalClose { .. }) => {
                self.enter_closed_or_reconnect(
                    crate::dispatch::ClosedReason::ClientInitiated,
                    crate::dispatch::AckSource::SocketDropped,
                );
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Closing → Closed/Connecting (AbnormalClose)"
                );
            }
            (Closing, TimerCloseTimeout) => {
                self.enter_closed_or_reconnect(
                    crate::dispatch::ClosedReason::Forced,
                    crate::dispatch::AckSource::CloseTimeout,
                );
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Closing → Closed/Connecting (CloseTimeout — forced)"
                );
            }
            (Closing, CallClose { request_id }) => {
                // Join the close cohort so Closing→Closed resolves this 2nd+ close().await too.
                match self.pending_close_request_id {
                    Some(_) => self.additional_close_request_ids.push(request_id),
                    None => self.pending_close_request_id = Some(request_id),
                }
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    request_id,
                    "Closing → Closing (Close idempotent; rid joined close cohort)"
                );
            }
            (Closing, CallForceReconnect { request_id }) => {
                // A 2nd+ concurrent force_reconnect joins the connect cohort (not overwrite
                // the latch), so its await is not stranded.
                match self.pending_reconnect_request_id {
                    None => self.pending_reconnect_request_id = Some(request_id),
                    Some(_) => self.additional_connect_request_ids.push(request_id),
                }
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    request_id,
                    "Closing → Closing (ForceReconnect latched/cohorted; re-enter on Closing→Closed)"
                );
            }

            (Closed, _) => {
                tracing::trace!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Closed: event ignored (terminal state)"
                );
            }

            (Failed, CallForceReconnect { request_id }) => {
                self.attempt_count = 0;
                let now = self.clock.now();
                if let Err(ConnectionRateThrottled {
                    attempts_used,
                    budget,
                    throttle_until_monotonic,
                }) = self.rate_budget.try_consume(now)
                {
                    self.state = BackingOff;
                    self.pending_connect_request_id = Some(request_id);
                    self.timers.rate_budget_window_advanced_at = Some(throttle_until_monotonic);
                    self.emit_rate_throttled(
                        Some(request_id),
                        attempts_used,
                        budget,
                        throttle_until_monotonic,
                    );
                    tracing::debug!(
                        target: "kraken_sdk::fsm",
                        url = ?self.url,
                        "Failed → BackingOff (ForceReconnect throttled by budget)"
                    );
                    return;
                }
                self.state = Connecting;
                self.pending_connect_request_id = Some(request_id);
                self.enter_connecting(Some(request_id));
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Failed → Connecting (ForceReconnect; socket opened)"
                );
            }
            (Failed, CallClose { request_id }) => {
                self.state = Closed;
                self.emit_lifecycle(
                    crate::dispatch::EventType::ConnectionClosedEvent,
                    crate::dispatch::EventPayload::ConnectionClosedEvent {
                        url: self.url,
                        closed_at_monotonic: self.clock.now(),
                        reason: crate::dispatch::ClosedReason::ClientInitiated,
                        ack: crate::dispatch::AckSource::SocketDropped,
                        server_connection_id: None,
                    },
                    Some(request_id),
                );
                tracing::debug!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Failed → Closed (Close)"
                );
            }
            (Failed, _) => {
                tracing::trace!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    "Failed: event ignored (terminal except for CallForceReconnect/CallClose)"
                );
            }

            (state, event) => {
                tracing::warn!(
                    target: "kraken_sdk::fsm",
                    url = ?self.url,
                    ?state,
                    ?event,
                    "handle_event: unexpected (state, event) combination — no-op (likely caller bug)"
                );
            }
        }
    }

    fn emit_rate_throttled(
        &self,
        request_id: Option<u64>,
        attempts_used: u32,
        budget: u32,
        throttle_until_monotonic: crate::types::MonotonicInstant,
    ) {
        self.emit_lifecycle(
            crate::dispatch::EventType::ConnectionRateThrottledEvent,
            crate::dispatch::EventPayload::ConnectionRateThrottledEvent {
                url: self.url,
                attempts_used,
                window_seconds: self.rate_budget.window_seconds(),
                window_remaining_attempts: budget.saturating_sub(attempts_used),
                throttle_until_monotonic,
            },
            request_id,
        );
    }
}
