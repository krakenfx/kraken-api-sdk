//! Spot WS v2 token cache. Lazy `GetWebSocketsToken` (~15min TTL), proactive
//! refresh at TTL × 0.5, reactive on stale-handshake. See docs/guides/streaming.md.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use serde_json::Value;

use crate::auth::AuthError;
use crate::rest::{RestError, RestSurface};
use crate::types::{AuthProfile, MonotonicInstant};

/// Documented Spot WS v2 token TTL (~15min). TTL math anchors here, not wire `expires`.
const WS_TOKEN_TTL: Duration = Duration::from_secs(15 * 60);

/// Proactive-refresh threshold: TTL × 0.5 (≈7.5min).
pub(crate) const WS_TOKEN_REFRESH_AT: Duration = Duration::from_secs(7 * 60 + 30);

/// Spot WS v2 per-message auth token. `value` is a credential — hand-redact `Debug`;
/// clone the cache `Arc`, not the inner token.
#[derive(Clone, zeroize::ZeroizeOnDrop)]
pub struct WsToken {
    /// Opaque token — credential; redacted in `Debug` / logs.
    value: String,
    #[zeroize(skip)]
    /// Fetch time (monotonic) for TTL math — not wall-clock (suspend-safe).
    fetched_at: MonotonicInstant,
}

impl std::fmt::Debug for WsToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsToken")
            .field("value", &"<redacted>")
            .field("fetched_at", &self.fetched_at)
            .finish()
    }
}

impl WsToken {
    /// Opaque token value for auth subscribe `params.token`.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// True once past the ~15min TTL.
    pub fn is_expired(&self, now: MonotonicInstant) -> bool {
        now.0.saturating_sub(self.fetched_at.0) >= WS_TOKEN_TTL
    }

    /// When proactive refresh is due (`fetched_at + TTL × 0.5`).
    pub fn refresh_due_at(&self) -> MonotonicInstant {
        MonotonicInstant(self.fetched_at.0 + WS_TOKEN_REFRESH_AT)
    }
}

/// Why a token refresh was triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshReason {
    /// Proactive TTL×0.5 scheduled refresh.
    Scheduled,
    /// Reactive refresh on a token-stale auth-handshake failure.
    AuthHandshakeFailed,
    /// Caller-initiated (`AuthStack::force_refresh`).
    Manual,
}

/// Correlates a `force_refresh` to its async completion via `id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshHandle {
    /// Correlation id for the reactor bridge outcome.
    pub id: u64,
}

/// Outcome a spawned refresh delivers on the reactor bridge.
#[derive(Debug)]
pub struct RefreshOutcome {
    /// Correlating handle id.
    pub request_id: u64,
    /// Fresh token or typed failure.
    pub result: Result<WsToken, AuthError>,
}

/// Client-scoped token cache slot. `.expect` on poison is permitted (SDK invariant).
#[derive(Clone)]
struct WsTokenCache {
    slot: Arc<Mutex<Option<Arc<WsToken>>>>,
}

impl WsTokenCache {
    fn new() -> Self {
        Self {
            slot: Arc::new(Mutex::new(None)),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<Arc<WsToken>>> {
        self.slot
            .lock()
            .expect("token cache lock poisoned (SDK invariant)")
    }

    /// Clone the `Arc` handle, not the plaintext buffer.
    fn get(&self) -> Option<Arc<WsToken>> {
        self.lock().clone()
    }

    fn store(&self, token: WsToken) {
        *self.lock() = Some(Arc::new(token));
    }

    fn clear(&self) {
        *self.lock() = None;
    }
}

/// Spot WS v2 token cache. Lazy: no network at `.build()`. Composed in [`AuthStack`](crate::auth::AuthStack).
pub struct TokenLifecycleManager {
    cached_token: WsTokenCache,
    /// `Weak` breaks `AuthStack → RestSurface → AuthStack`; late-bound via [`set_rest`](Self::set_rest).
    rest: OnceLock<Weak<RestSurface>>,
    bus: Arc<crate::dispatch::DispatchEventBus>,
    refresh_handle_seq: Arc<AtomicU64>,
    /// Reactor refresh-outcome bridge; unset in unit tests.
    refresh_tx: OnceLock<tokio::sync::mpsc::Sender<RefreshOutcome>>,
    /// Non-reversible key fingerprint for credential events — NEVER the raw key.
    key_id_fingerprint: String,
    /// Single-flight for proactive refresh only (reactive is ungated).
    proactive_inflight: Arc<AtomicBool>,
}

impl TokenLifecycleManager {
    /// Empty lazy manager; `rest` / `refresh_tx` late-bound.
    pub fn new(bus: Arc<crate::dispatch::DispatchEventBus>, key_id_fingerprint: String) -> Self {
        Self {
            cached_token: WsTokenCache::new(),
            rest: OnceLock::new(),
            bus,
            refresh_handle_seq: Arc::new(AtomicU64::new(1)),
            refresh_tx: OnceLock::new(),
            key_id_fingerprint,
            proactive_inflight: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Test-only manager with a throwaway bus + dummy fingerprint.
    #[cfg(test)]
    pub fn for_test() -> Self {
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
        let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            clock,
        ));
        Self::new(bus, "<test-key>".to_string())
    }

    /// Install the reactor refresh-outcome bridge (idempotent).
    pub fn set_refresh_tx(&self, tx: tokio::sync::mpsc::Sender<RefreshOutcome>) {
        let _ = self.refresh_tx.set(tx);
    }

    /// Test-only: seed a cached token without a REST round-trip.
    #[cfg(test)]
    pub(crate) fn seed_cached_token_for_test(&self, value: &str) {
        self.cached_token.store(WsToken {
            value: value.to_string(),
            fetched_at: MonotonicInstant::now(),
        });
    }

    /// Test-only: drop the cached token.
    #[cfg(test)]
    pub(crate) fn clear_cached_token_for_test(&self) {
        self.cached_token.clear();
    }

    /// Late-bind `RestSurface` weak ref (idempotent).
    pub fn set_rest(&self, rest: Weak<RestSurface>) {
        let _ = self.rest.set(rest);
    }

    /// Cached token (`Arc` handle); `None` until first successful fetch.
    pub fn current_token(&self) -> Option<Arc<WsToken>> {
        self.cached_token.get()
    }

    /// Drop the cache. `is_expired` is time-only — a rejected token still reads valid.
    pub fn invalidate_cached_token(&self) {
        self.cached_token.clear();
    }
}

/// Signed POST `GetWebSocketsToken`, decode, cache swap. Clock sampled AFTER
/// the round-trip so `fetched_at` matches freshness-check clock source.
async fn fetch_and_cache(
    rest: &RestSurface,
    cache: &WsTokenCache,
    bus: &crate::dispatch::DispatchEventBus,
) -> Result<WsToken, AuthError> {
    let result = rest
        .signed_post(
            "/0/private/GetWebSocketsToken",
            Vec::new(),
            AuthProfile::SpotV1,
            &crate::rest::mint_request_id(),
        )
        .await
        .map_err(map_rest_to_auth)?;
    let token = decode_token_result(&result, bus.clock().now())?;
    cache.store(token.clone());
    Ok(token)
}

/// Decode `GetWebSocketsToken` result. `now` MUST be the injected clock.
pub(crate) fn decode_token_result(
    result: &Value,
    now: crate::types::MonotonicInstant,
) -> Result<WsToken, AuthError> {
    let token = result
        .get("token")
        .and_then(Value::as_str)
        .ok_or_else(|| AuthError::Unknown {
            kraken_code: "INTERNAL".into(),
            kraken_message: "GetWebSocketsToken response missing string `token`".into(),
        })?;
    // Empty token is a shape error — never cache (would wedge the handshake).
    if token.is_empty() {
        return Err(AuthError::Unknown {
            kraken_code: "INTERNAL".into(),
            kraken_message: "GetWebSocketsToken returned an empty `token`".into(),
        });
    }
    Ok(WsToken {
        value: token.to_string(),
        fetched_at: now,
    })
}

/// Map token-fetch [`RestError`] into the closed [`AuthError`] set.
fn map_rest_to_auth(e: RestError) -> AuthError {
    match e {
        RestError::Auth(a) => a,
        RestError::Kraken(codes) => {
            let joined = codes.join(",");
            if joined.contains("Invalid key") {
                AuthError::InvalidKey
            } else if joined.contains("Invalid signature") {
                AuthError::InvalidSignature
            } else if joined.contains("Invalid nonce") {
                AuthError::InvalidNonce
            } else if joined.contains("Permission denied") {
                AuthError::PermissionDenied { scope: None }
            } else if joined.contains("Temporary lockout") {
                // Auth-failure lockout (~15min), not a rate-limit escalation.
                AuthError::TemporaryLockout
            } else if crate::error::is_kraken_rate_limit(&joined) || joined.contains("EService:") {
                // Throttle / EService: transient — retry rather than fail auth.
                AuthError::TokenRefreshTransient
            } else {
                AuthError::Unknown {
                    kraken_code: joined,
                    kraken_message: "GetWebSocketsToken rejected".into(),
                }
            }
        }
        RestError::Transport {
            transient: true, ..
        } => AuthError::TokenRefreshTransient,
        RestError::Transport {
            transient: false, ..
        } => AuthError::TokenRefreshFailed,
        // Pre-wire rate-limit rejection: counter decays → retry.
        RestError::RateLimit(_) => AuthError::TokenRefreshTransient,
        other => AuthError::Unknown {
            kraken_code: "INTERNAL".into(),
            kraken_message: format!("token fetch transport/shape error: {other}"),
        },
    }
}

/// Spawned refresh context (`Weak<RestSurface>` cycle-break → `ClientClosed` if gone).
struct TokenRefreshTask {
    cached_token: WsTokenCache,
    rest: Weak<RestSurface>,
    request_id: u64,
    bus: Arc<crate::dispatch::DispatchEventBus>,
    /// NEVER the raw key.
    key_id_fingerprint: String,
    reason: RefreshReason,
}

impl TokenRefreshTask {
    /// Fetch, publish credential event, return [`RefreshOutcome`].
    async fn run(self) -> RefreshOutcome {
        let result = self.fetch().await;
        let now = self.bus.clock().now();
        match &result {
            Ok(token) => {
                self.bus.publish(crate::dispatch::EventEnvelope {
                    event_type: crate::dispatch::EventType::CredentialRefreshedEvent,
                    event_version: 1,
                    timestamp_monotonic: now,
                    request_id: None,
                    payload: crate::dispatch::EventPayload::CredentialRefreshedEvent {
                        key_id_fingerprint: self.key_id_fingerprint.clone(),
                        // Expiry = fetched_at + documented 15-min TTL.
                        token_expires_at_monotonic: crate::types::MonotonicInstant(
                            token.fetched_at.0 + WS_TOKEN_TTL,
                        ),
                        refresh_reason: self.reason.into(),
                    },
                });
            }
            Err(e) => {
                self.bus.publish(crate::dispatch::EventEnvelope {
                    event_type: crate::dispatch::EventType::CredentialRefreshFailedEvent,
                    event_version: 1,
                    timestamp_monotonic: now,
                    request_id: None,
                    payload: crate::dispatch::EventPayload::CredentialRefreshFailedEvent {
                        key_id_fingerprint: self.key_id_fingerprint.clone(),
                        attempt: 1,
                        error_class: classify_auth_error(e),
                        retry_in_ms: None,
                    },
                });
            }
        }
        RefreshOutcome {
            request_id: self.request_id,
            result,
        }
    }

    async fn fetch(&self) -> Result<WsToken, AuthError> {
        let rest = self.rest.upgrade().ok_or(AuthError::ClientClosed)?;
        fetch_and_cache(&rest, &self.cached_token, &self.bus).await
    }
}

impl TokenLifecycleManager {
    /// Reactive refresh; outcome on the reactor bridge, correlated by handle id.
    pub fn reactive_refresh(&self, reason: RefreshReason) -> RefreshHandle {
        let id = self.refresh_handle_seq.fetch_add(1, Ordering::Relaxed);
        let outcome_tx = self.refresh_tx.get().cloned();
        let task = TokenRefreshTask {
            cached_token: self.cached_token.clone(),
            rest: self.rest.get().cloned().unwrap_or_else(Weak::new),
            request_id: id,
            bus: Arc::clone(&self.bus),
            key_id_fingerprint: self.key_id_fingerprint.clone(),
            reason,
        };
        tokio::spawn(async move {
            let outcome = task.run().await;
            if let Some(tx) = outcome_tx {
                // `.await` not `try_send`: back-pressure blocks only this task.
                let _ = tx.send(outcome).await;
            }
        });
        RefreshHandle { id }
    }

    /// Proactive refresh (`request_id` 0). Single-flight; outcome on reactor bridge.
    pub fn proactive_refresh(&self) {
        if self
            .proactive_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let outcome_tx = self.refresh_tx.get().cloned();
        let task = TokenRefreshTask {
            cached_token: self.cached_token.clone(),
            rest: self.rest.get().cloned().unwrap_or_else(Weak::new),
            request_id: 0, // the proactive marker: reactive handle ids start at 1
            bus: Arc::clone(&self.bus),
            key_id_fingerprint: self.key_id_fingerprint.clone(),
            reason: RefreshReason::Scheduled,
        };
        // Clear single-flight on Drop so a panic doesn't wedge proactive refresh.
        let guard = SingleFlightGuard(Arc::clone(&self.proactive_inflight));
        tokio::spawn(async move {
            let _guard = guard;
            let outcome = task.run().await;
            if let Some(tx) = outcome_tx {
                let _ = tx.send(outcome).await;
            }
        });
    }
}

/// Reset single-flight flag on drop (including panic unwind).
struct SingleFlightGuard(Arc<AtomicBool>);

impl Drop for SingleFlightGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Non-reversible key fingerprint: SHA-256 hex truncated to 16 chars. NEVER the raw key.
pub(crate) fn derive_key_fingerprint(api_key: &crate::types::ApiKey) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(api_key.as_str().as_bytes());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// Map failed-fetch [`AuthError`] → `CredentialRefreshFailedEvent` error_class
/// (exhaustive — new variants break compile).
fn classify_auth_error(e: &AuthError) -> crate::dispatch::ErrorClass {
    use crate::dispatch::ErrorClass;
    match e {
        AuthError::InvalidKey
        | AuthError::InvalidSignature
        | AuthError::InvalidNonce
        | AuthError::PermissionDenied { .. }
        | AuthError::TemporaryLockout
        | AuthError::TokenStale => ErrorClass::Auth,
        AuthError::TokenRefreshFailed | AuthError::TokenRefreshTransient => ErrorClass::Network,
        AuthError::ClientClosed => ErrorClass::ClientClosed,
        AuthError::Unknown { .. } => ErrorClass::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: u64) -> MonotonicInstant {
        MonotonicInstant(Duration::from_secs(secs))
    }

    fn token_fetched_at(secs: u64) -> WsToken {
        WsToken {
            value: "fake-token-value".into(),
            fetched_at: at(secs),
        }
    }

    #[test]
    fn decode_token_result_extracts_token() {
        let body = serde_json::json!({ "token": "abc123", "expires": 900u32 });
        let tok = decode_token_result(&body, MonotonicInstant::now()).expect("decode");
        assert_eq!(tok.value(), "abc123");
    }

    #[test]
    fn decode_token_result_missing_token_is_shape_error() {
        let body = serde_json::json!({ "expires": 900u32 });
        let err = decode_token_result(&body, MonotonicInstant::now()).unwrap_err();
        assert!(matches!(
            err,
            AuthError::Unknown { ref kraken_code, .. } if kraken_code == "INTERNAL"
        ));
    }

    #[test]
    fn decode_token_result_empty_token_is_shape_error() {
        let body = serde_json::json!({ "token": "", "expires": 900u32 });
        let err = decode_token_result(&body, MonotonicInstant::now()).unwrap_err();
        assert!(matches!(
            err,
            AuthError::Unknown { ref kraken_code, .. } if kraken_code == "INTERNAL"
        ));
    }

    #[test]
    fn decode_token_result_nonstring_token_is_shape_error() {
        let body = serde_json::json!({ "token": 42, "expires": 900u32 });
        let err = decode_token_result(&body, MonotonicInstant::now()).unwrap_err();
        assert!(matches!(err, AuthError::Unknown { .. }));
    }

    #[test]
    fn decode_token_result_stamps_fetched_at_from_passed_clock() {
        let fixed_now = at(42_000);
        let body = serde_json::json!({ "token": "clock-test-token", "expires": 900u32 });
        let tok = decode_token_result(&body, fixed_now).expect("decode");
        assert_eq!(
            tok.fetched_at, fixed_now,
            "fetched_at must equal the passed `now`, not the real clock"
        );
    }

    #[test]
    fn token_expiry_math_uses_documented_ttl() {
        let tok = token_fetched_at(1000);
        assert!(!tok.is_expired(at(1000)));
        assert!(at(1000).0 < tok.refresh_due_at().0);
        assert_eq!(tok.refresh_due_at(), at(1000 + 450));
        assert!(!tok.is_expired(at(1000 + 450)));
        assert!(tok.is_expired(at(1000 + 900)));
    }

    #[test]
    fn refresh_due_at_anchors_to_fetched_at_plus_ttl_half() {
        let tok = token_fetched_at(1000);
        assert_eq!(
            tok.refresh_due_at(),
            MonotonicInstant(at(1000).0 + WS_TOKEN_REFRESH_AT)
        );
    }

    #[test]
    fn token_expiry_saturates_on_clock_regression() {
        let tok = token_fetched_at(1000);
        assert!(!tok.is_expired(at(500)));
    }

    #[test]
    fn ws_token_debug_redacts_value() {
        let tok = token_fetched_at(0);
        let dbg = format!("{tok:?}");
        assert!(dbg.contains("<redacted>"), "Debug must redact: {dbg}");
        assert!(
            !dbg.contains("fake-token-value"),
            "Debug must NOT leak the token value: {dbg}"
        );
    }

    use crate::transport::{HttpTransport, TransportError};

    struct CannedTokenMock {
        envelope: serde_json::Value,
        /// `(path, body)` of the last signed POST, for wire assertions.
        sent_post: Mutex<Option<(String, String)>>,
    }
    #[async_trait::async_trait]
    impl HttpTransport for CannedTokenMock {
        async fn get_json(
            &self,
            _path: &str,
            _query: &[(&str, &str)],
        ) -> Result<Value, TransportError> {
            Ok(self.envelope.clone())
        }
        async fn post_form_signed(
            &self,
            path: &str,
            body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<Value, TransportError> {
            *self.sent_post.lock().expect("mock post lock") =
                Some((path.to_string(), body.to_string()));
            Ok(self.envelope.clone())
        }
    }

    /// Manager + RestSurface Arc (must outlive; manager holds Weak).
    fn manager_with_canned(
        envelope: serde_json::Value,
    ) -> (
        TokenLifecycleManager,
        Arc<RestSurface>,
        Arc<CannedTokenMock>,
    ) {
        manager_with_canned_otp(envelope, None)
    }

    /// As [`manager_with_canned`], with a 2FA otp installed on the signing stack.
    fn manager_with_canned_otp(
        envelope: serde_json::Value,
        otp: Option<crate::types::Otp>,
    ) -> (
        TokenLifecycleManager,
        Arc<RestSurface>,
        Arc<CannedTokenMock>,
    ) {
        use crate::auth::{AuthStack, SpotRestHmacSha512Signer, SystemClockNonceSource};
        use crate::clock::{Clock, SystemClock};
        use crate::rate_limit::{SpotApiRateLimitTracker, SpotTradingRateLimitTracker, Tier};
        use crate::types::{ApiKey, ApiSecret, AuthProfile};
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD as BASE64;
        use std::collections::HashMap;

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let api_key = ApiKey::new("test-api-key");
        let secret = ApiSecret::from_base64(&BASE64.encode(vec![0x01u8; 32])).unwrap();
        let signer = SpotRestHmacSha512Signer::new(api_key.clone(), secret);
        let mut signers: HashMap<AuthProfile, Arc<dyn crate::auth::AuthSigner>> = HashMap::new();
        signers.insert(AuthProfile::SpotV1, Arc::new(signer));
        let manager = TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string());
        let auth = Arc::new(AuthStack::new(
            Some(api_key),
            otp,
            Arc::new(SystemClockNonceSource::new()),
            signers,
            TokenLifecycleManager::new(Arc::clone(&bus), "<test-key>".to_string()),
        ));
        let api_rl = Arc::new(SpotApiRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let trading_rl = Arc::new(SpotTradingRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let mock = Arc::new(CannedTokenMock {
            envelope,
            sent_post: Mutex::new(None),
        });
        let rest = Arc::new(RestSurface::new(
            Arc::clone(&mock) as Arc<dyn HttpTransport>,
            auth,
            api_rl,
            trading_rl,
            clock,
            std::time::Duration::from_secs(30),
        ));
        manager.set_rest(Arc::downgrade(&rest));
        (manager, rest, mock)
    }

    async fn direct_fetch(mgr: &TokenLifecycleManager) -> Result<WsToken, AuthError> {
        let rest = mgr
            .rest
            .get()
            .and_then(Weak::upgrade)
            .ok_or(AuthError::ClientClosed)?;
        fetch_and_cache(&rest, &mgr.cached_token, &mgr.bus).await
    }

    #[tokio::test]
    async fn direct_fetch_populates_cache() {
        let envelope = serde_json::json!({
            "error": [],
            "result": { "token": "live-token-abc", "expires": 900u32 }
        });
        let (mgr, _rest, _mock) = manager_with_canned(envelope);
        assert!(
            mgr.current_token().is_none(),
            "cache empty before fetch (lazy)"
        );
        let tok = tokio::time::timeout(std::time::Duration::from_secs(2), direct_fetch(&mgr))
            .await
            .expect("fetch did not hang")
            .expect("fetch ok");
        assert_eq!(tok.value(), "live-token-abc");
        assert_eq!(
            mgr.current_token().expect("cached").value(),
            "live-token-abc"
        );
    }

    #[tokio::test]
    async fn direct_fetch_kraken_error_maps_to_auth_error() {
        let envelope = serde_json::json!({ "error": ["EAPI:Invalid key"], "result": {} });
        let (mgr, _rest, _mock) = manager_with_canned(envelope);
        let err = tokio::time::timeout(std::time::Duration::from_secs(2), direct_fetch(&mgr))
            .await
            .expect("fetch did not hang")
            .unwrap_err();
        assert!(matches!(err, AuthError::InvalidKey));
        assert!(
            mgr.current_token().is_none(),
            "failed fetch leaves cache empty"
        );
    }

    #[tokio::test]
    async fn ws_token_fetch_carries_the_otp_in_its_signed_body() {
        // Pins `fetch_and_cache` to `RestSurface::signed_post`: rerouting around it
        // would unauthenticate every 2FA WS session with the signing tests still green.
        let envelope = serde_json::json!({
            "error": [],
            "result": { "token": "ws-token-xyz", "expires": 900u32 }
        });
        let (mgr, _rest, mock) = manager_with_canned_otp(
            envelope,
            Some(crate::types::Otp::new("static-2fa-password")),
        );
        tokio::time::timeout(std::time::Duration::from_secs(2), direct_fetch(&mgr))
            .await
            .expect("fetch did not hang")
            .expect("fetch ok");

        let (path, body) = mock
            .sent_post
            .lock()
            .expect("mock post lock")
            .clone()
            .expect("a signed POST was sent");
        assert_eq!(path, "/0/private/GetWebSocketsToken");
        assert!(
            body.contains("otp=static-2fa-password"),
            "GetWebSocketsToken body must carry the otp — got {body}"
        );
    }

    #[tokio::test]
    async fn refresh_task_client_closed_when_rest_dropped() {
        use crate::dispatch::{ErrorClass, EventPayload, EventType};
        let envelope = serde_json::json!({
            "error": [],
            "result": { "token": "unused", "expires": 900u32 }
        });
        let (task, _cache, bus, rest) =
            refresh_task_with_canned(envelope, RefreshReason::AuthHandshakeFailed, "feedbeef", 9);
        drop(rest);
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), task.run())
            .await
            .expect("run did not hang");
        assert_eq!(outcome.request_id, 9);
        assert!(matches!(outcome.result, Err(AuthError::ClientClosed)));
        let failed: Vec<_> = bus
            .test_drain_published()
            .into_iter()
            .filter(|e| e.event_type == EventType::CredentialRefreshFailedEvent)
            .collect();
        assert_eq!(failed.len(), 1, "exactly one failed event");
        match &failed[0].payload {
            EventPayload::CredentialRefreshFailedEvent { error_class, .. } => {
                assert_eq!(*error_class, ErrorClass::ClientClosed);
            }
            other => panic!("wrong payload: {other:?}"),
        }
    }

    #[test]
    fn map_rest_to_auth_classifies_kraken_codes() {
        assert!(matches!(
            map_rest_to_auth(RestError::Kraken(vec!["EAPI:Invalid key".into()])),
            AuthError::InvalidKey
        ));
        assert!(matches!(
            map_rest_to_auth(RestError::Kraken(vec!["EAPI:Invalid signature".into()])),
            AuthError::InvalidSignature
        ));
        assert!(matches!(
            map_rest_to_auth(RestError::Kraken(vec!["EAPI:Invalid nonce".into()])),
            AuthError::InvalidNonce
        ));
        assert!(matches!(
            map_rest_to_auth(RestError::Kraken(vec!["EGeneral:Permission denied".into()])),
            AuthError::PermissionDenied { .. }
        ));
        let lockout =
            map_rest_to_auth(RestError::Kraken(vec!["EGeneral:Temporary lockout".into()]));
        assert!(matches!(lockout, AuthError::TemporaryLockout));
        assert_eq!(crate::error::ApiError::code(&lockout), "TEMPORARY_LOCKOUT");
        assert_eq!(
            crate::error::ApiError::category(&lockout),
            crate::error::ErrorCategory::Auth
        );
        assert!(!crate::error::ApiError::retryable(&lockout));
        for code in [
            "EService:Unavailable",
            "EService:Throttled",
            "EGeneral:Too many requests",
            "EAPI:Rate limit exceeded",
            "EAuth:Rate limit exceeded",
        ] {
            let e = map_rest_to_auth(RestError::Kraken(vec![code.into()]));
            assert!(
                matches!(e, AuthError::TokenRefreshTransient),
                "{code} must map to TokenRefreshTransient, got {e:?}"
            );
            assert!(crate::error::ApiError::retryable(&e));
        }
        assert!(matches!(
            map_rest_to_auth(RestError::RateLimit(crate::rate_limit::RateLimitExceeded {
                tracker: "api",
                scope: crate::rate_limit::Scope::ApiKey(crate::types::ApiKey::new("k".to_string())),
                current: 100.0,
                cap: 60.0,
                retry_at_monotonic: crate::types::MonotonicInstant::now(),
            })),
            AuthError::TokenRefreshTransient
        ));
    }

    #[test]
    fn single_flight_guard_resets_flag_on_panic_unwind() {
        let flag = Arc::new(AtomicBool::new(true));
        let f = Arc::clone(&flag);
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = SingleFlightGuard(Arc::clone(&f));
            assert!(
                f.load(Ordering::Acquire),
                "flag set while the guard is live"
            );
            panic!("simulated refresh-task panic");
        }));
        assert!(res.is_err(), "the closure panicked");
        assert!(
            !flag.load(Ordering::Acquire),
            "SingleFlightGuard Drop MUST reset the flag even on panic-unwind"
        );
    }

    #[test]
    fn map_rest_to_auth_transient_transport_is_retryable_refresh() {
        assert!(matches!(
            map_rest_to_auth(RestError::Transport {
                kind: crate::transport::TransportErrorKind::TcpConnectTimeout,
                transient: true
            }),
            AuthError::TokenRefreshTransient
        ));
    }

    #[test]
    fn map_rest_to_auth_non_transient_transport_is_terminal_refresh() {
        assert!(matches!(
            map_rest_to_auth(RestError::Transport {
                kind: crate::transport::TransportErrorKind::Ssl,
                transient: false
            }),
            AuthError::TokenRefreshFailed
        ));
    }

    #[test]
    fn ws_token_drop_runs_cleanly_and_clone_is_independent() {
        let original = WsToken {
            value: "test-zeroize-drop-token".into(),
            fetched_at: at(0),
        };
        let cloned = original.clone();
        assert_eq!(original.value(), "test-zeroize-drop-token");
        assert_eq!(cloned.value(), "test-zeroize-drop-token");
        drop(original);
        assert_eq!(cloned.value(), "test-zeroize-drop-token");
    }

    #[test]
    fn ws_token_wipe_on_drop_is_pinned() {
        // Compile-time pin: removing ZeroizeOnDrop fails this at build.
        fn assert_wipes_on_drop<T: zeroize::ZeroizeOnDrop>() {}
        assert_wipes_on_drop::<WsToken>();
    }

    /// Build a `TokenRefreshTask` wired to a `CannedTokenMock` returning `envelope`,
    /// plus an independent capture bus and a clone-handle to the task's cache slot.
    /// The returned `Arc<RestSurface>` must outlive the task (it holds only a `Weak`).
    fn refresh_task_with_canned(
        envelope: serde_json::Value,
        reason: RefreshReason,
        fingerprint: &str,
        request_id: u64,
    ) -> (
        TokenRefreshTask,
        WsTokenCache,
        Arc<crate::dispatch::DispatchEventBus>,
        Arc<RestSurface>,
    ) {
        let (_mgr, rest, _mock) = manager_with_canned(envelope);
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
        let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            clock,
        ));
        let cache = WsTokenCache::new();
        let task = TokenRefreshTask {
            cached_token: cache.clone(),
            rest: Arc::downgrade(&rest),
            request_id,
            bus: Arc::clone(&bus),
            key_id_fingerprint: fingerprint.to_string(),
            reason,
        };
        (task, cache, bus, rest)
    }

    #[tokio::test]
    async fn token_refresh_run_failure_emits_credential_refresh_failed_event() {
        use crate::dispatch::{ErrorClass, EventEnvelope, EventPayload, EventType};
        let envelope = serde_json::json!({ "error": ["EAPI:Invalid key"], "result": {} });
        let (task, cache, bus, _rest) = refresh_task_with_canned(
            envelope,
            RefreshReason::AuthHandshakeFailed,
            "abcd1234deadbeef",
            77,
        );
        // Failed refresh must not clobber a live cached token.
        cache.store(WsToken {
            value: "pre-existing-live-token".into(),
            fetched_at: at(1000),
        });

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), task.run())
            .await
            .expect("run did not hang");

        assert_eq!(outcome.request_id, 77);
        let err = outcome
            .result
            .expect_err("refresh must fail on EAPI:Invalid key");
        assert!(matches!(err, AuthError::InvalidKey));
        assert_eq!(
            cache.get().expect("pre-seeded token retained").value(),
            "pre-existing-live-token",
            "a failed refresh must not clobber the live cached token"
        );

        let events = bus.test_drain_published();
        let failed: Vec<&EventEnvelope> = events
            .iter()
            .filter(|e| e.event_type == EventType::CredentialRefreshFailedEvent)
            .collect();
        assert_eq!(failed.len(), 1, "exactly one failed event: {events:?}");
        assert!(
            !events
                .iter()
                .any(|e| e.event_type == EventType::CredentialRefreshedEvent),
            "no success event on failure"
        );

        let ev = failed[0];
        assert_eq!(ev.event_version, 1);
        assert_eq!(
            ev.request_id, None,
            "credential events broadcast (no awaiter)"
        );
        match &ev.payload {
            EventPayload::CredentialRefreshFailedEvent {
                key_id_fingerprint,
                attempt,
                error_class,
                retry_in_ms,
            } => {
                assert_eq!(key_id_fingerprint, "abcd1234deadbeef");
                assert_eq!(*attempt, 1, "v1 is one-shot; honest partial body");
                assert_eq!(*error_class, ErrorClass::Auth);
                assert_eq!(*retry_in_ms, None);
            }
            other => panic!("wrong payload: {other:?}"),
        }

        use crate::error::{ApiError, ErrorCategory};
        assert_eq!(ApiError::category(&err), ErrorCategory::Auth);
        assert!(!ApiError::retryable(&err));
    }

    #[tokio::test]
    async fn token_refresh_run_success_emits_credential_refreshed_event() {
        use crate::dispatch::event_bus::RefreshReasonPayload;
        use crate::dispatch::{EventEnvelope, EventPayload, EventType};
        let envelope = serde_json::json!({
            "error": [],
            "result": { "token": "live-tok-xyz", "expires": 900u32 }
        });
        let (task, cache, bus, _rest) =
            refresh_task_with_canned(envelope, RefreshReason::Scheduled, "feedface00c0ffee", 0);

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), task.run())
            .await
            .expect("run did not hang");

        let tok = outcome.result.expect("refresh ok");
        assert_eq!(tok.value(), "live-tok-xyz");
        assert_eq!(outcome.request_id, 0);
        assert_eq!(cache.get().expect("cached").value(), "live-tok-xyz");

        let events = bus.test_drain_published();
        let ok: Vec<&EventEnvelope> = events
            .iter()
            .filter(|e| e.event_type == EventType::CredentialRefreshedEvent)
            .collect();
        assert_eq!(ok.len(), 1, "exactly one success event: {events:?}");
        assert!(
            !events
                .iter()
                .any(|e| e.event_type == EventType::CredentialRefreshFailedEvent),
            "no failed event on success"
        );
        let ev = ok[0];
        assert_eq!(ev.event_version, 1);
        assert_eq!(
            ev.request_id, None,
            "credential events broadcast (no awaiter)"
        );
        match &ev.payload {
            EventPayload::CredentialRefreshedEvent {
                key_id_fingerprint,
                token_expires_at_monotonic,
                refresh_reason,
            } => {
                assert_eq!(key_id_fingerprint, "feedface00c0ffee");
                assert_eq!(*refresh_reason, RefreshReasonPayload::Proactive);
                assert_eq!(
                    *token_expires_at_monotonic,
                    MonotonicInstant(tok.fetched_at.0 + WS_TOKEN_TTL),
                    "expiry must anchor to WS_TOKEN_TTL, not the wire `expires`"
                );
            }
            other => panic!("wrong payload: {other:?}"),
        }
    }

    #[test]
    fn classify_auth_error_maps_each_error_class() {
        use crate::dispatch::ErrorClass;
        for e in &[
            AuthError::InvalidKey,
            AuthError::InvalidSignature,
            AuthError::InvalidNonce,
            AuthError::PermissionDenied { scope: None },
            AuthError::TemporaryLockout,
            AuthError::TokenStale,
        ] {
            assert_eq!(classify_auth_error(e), ErrorClass::Auth, "{e:?} → Auth");
        }
        assert_eq!(
            classify_auth_error(&AuthError::ClientClosed),
            ErrorClass::ClientClosed
        );
        assert_eq!(
            classify_auth_error(&AuthError::TokenRefreshTransient),
            ErrorClass::Network
        );
        assert_eq!(
            classify_auth_error(&AuthError::TokenRefreshFailed),
            ErrorClass::Network
        );
        assert_eq!(
            classify_auth_error(&AuthError::Unknown {
                kraken_code: "EService:Unavailable".into(),
                kraken_message: "x".into(),
            }),
            ErrorClass::Unknown
        );

        use crate::error::{ApiError, ErrorCategory};
        assert!(ApiError::retryable(&AuthError::TokenRefreshTransient));
        assert_eq!(
            ApiError::category(&AuthError::TokenRefreshTransient),
            ErrorCategory::Network
        );
        assert!(!ApiError::retryable(&AuthError::TokenRefreshFailed));
        assert_eq!(
            ApiError::category(&AuthError::TokenRefreshFailed),
            ErrorCategory::Network
        );
        assert!(!ApiError::retryable(&AuthError::InvalidKey));
        assert_eq!(
            ApiError::category(&AuthError::InvalidKey),
            ErrorCategory::Auth
        );
    }

    #[test]
    fn refresh_reason_payload_maps_each_arm() {
        use crate::dispatch::event_bus::RefreshReasonPayload;
        assert_eq!(
            RefreshReasonPayload::from(RefreshReason::Scheduled),
            RefreshReasonPayload::Proactive
        );
        assert_eq!(
            RefreshReasonPayload::from(RefreshReason::AuthHandshakeFailed),
            RefreshReasonPayload::Reactive
        );
        assert_eq!(
            RefreshReasonPayload::from(RefreshReason::Manual),
            RefreshReasonPayload::Manual
        );
    }

    // Live probe — one shot; do NOT re-run on failure (~15min auth lockout).
    #[tokio::test]
    #[ignore = "live: hits real api.kraken.com with .env creds; run with --ignored --nocapture"]
    async fn ws_token_live_probe() {
        use crate::auth::{
            AuthSigner, AuthStack, SpotRestHmacSha512Signer, SystemClockNonceSource,
        };
        use crate::clock::{Clock, SystemClock};
        use crate::rate_limit::{SpotApiRateLimitTracker, SpotTradingRateLimitTracker, Tier};
        use crate::transport::{HttpTransport, ReqwestHttpTransport};
        use crate::types::{ApiKey, ApiSecret, AuthProfile};
        use std::collections::HashMap;

        fn load_env_var(name: &str) -> Option<String> {
            let path = format!(
                "{}/projects/kraken-sdk/.env",
                std::env::var("HOME").unwrap_or_else(|_| ".".into())
            );
            let contents = std::fs::read_to_string(&path).ok()?;
            for line in contents.lines() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                if let Some((k, v)) = line.split_once('=') {
                    if k.trim() == name {
                        return Some(v.trim().trim_matches('"').trim_matches('\'').to_string());
                    }
                }
            }
            None
        }

        let api_key = load_env_var("KRAKEN_API_KEY").expect("KRAKEN_API_KEY in .env");
        let api_secret_b64 = load_env_var("KRAKEN_API_SECRET").expect("KRAKEN_API_SECRET in .env");
        assert!(
            !api_key.is_empty() && !api_secret_b64.is_empty(),
            "creds present"
        );

        let secret =
            ApiSecret::from_base64(&api_secret_b64).expect("KRAKEN_API_SECRET valid base64");
        let key = ApiKey::new(api_key);
        let signer = SpotRestHmacSha512Signer::new(key.clone(), secret);
        let mut signers: HashMap<AuthProfile, Arc<dyn AuthSigner>> = HashMap::new();
        signers.insert(AuthProfile::SpotV1, Arc::new(signer));

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let auth = Arc::new(AuthStack::new(
            Some(key),
            None,
            Arc::new(SystemClockNonceSource::new()),
            signers,
            TokenLifecycleManager::new(Arc::clone(&bus), "<probe>".into()),
        ));
        let api_rl = Arc::new(SpotApiRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let trading_rl = Arc::new(SpotTradingRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        ));
        let transport: Arc<dyn HttpTransport> = Arc::new(ReqwestHttpTransport::new());
        let rest = RestSurface::new(
            transport,
            auth,
            api_rl,
            trading_rl,
            clock,
            std::time::Duration::from_secs(30),
        );

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            rest.signed_post(
                "/0/private/GetWebSocketsToken",
                Vec::new(),
                AuthProfile::SpotV1,
                "rid-test",
            ),
        )
        .await
        .expect("GetWebSocketsToken did not hang")
        .expect("GetWebSocketsToken signed POST succeeded (check .env creds / auth-lockout)");

        if let Some(obj) = result.as_object() {
            let keys: Vec<&str> = obj.keys().map(String::as_str).collect();
            eprintln!("[probe] GetWebSocketsToken `result` keys: {keys:?}");
            match obj.get("token").and_then(|v| v.as_str()) {
                Some(t) => eprintln!("[probe]   token: <redacted len={}>", t.len()),
                None => eprintln!("[probe]   token: <MISSING or non-string>"),
            }
            match obj.get("expires") {
                Some(e) => {
                    let ty = if e.is_u64() {
                        "u64"
                    } else if e.is_i64() {
                        "i64"
                    } else if e.is_string() {
                        "string"
                    } else {
                        "other"
                    };
                    eprintln!("[probe]   expires: {e} (json type: {ty})");
                }
                None => eprintln!("[probe]   expires: <ABSENT>"),
            }
        } else {
            eprintln!("[probe] `result` is not a JSON object: {result:?}");
        }

        let token = decode_token_result(&result, crate::types::MonotonicInstant::now())
            .expect("decode_token_result handles the live GetWebSocketsToken shape");
        assert!(!token.value().is_empty(), "live token non-empty");
        eprintln!("[probe] decode_token_result OK — WsToken built (Debug redacts): {token:?}");
    }
}
