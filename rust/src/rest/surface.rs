//! `RestSurface` — REST pipeline: rate-limit, sign, send, parse, classify,
//! retry, in that order (cross-binding contract).

use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use serde_json::Value;

use crate::api::account::{LifecyclePosition, OrderStatus, ReconciliationOutcome};
use crate::api::trade::AmendId;
use crate::auth::{AuthError, AuthStack};
use crate::clock::Clock;
use crate::dispatch::{
    DispatchEventBus, EventEnvelope, EventPayload, EventType, OrderOp, OrderSubmitStatus,
};
#[cfg(any(test, feature = "test-support"))]
use crate::jitter::{JitterSource, SplitMix64Jitter};
use crate::rate_limit::{
    ClOrdIdPairIndex, RateLimitExceeded, Scope, SnapTarget, SpotApiRateLimitTracker,
    SpotTradingRateLimitTracker, classify_rate_limit_snap,
};
use crate::rest::retry::{RetryDecision, RetryEngine, RetryPolicy, RetryReason};
use crate::transport::HttpTransport;
use crate::types::{ApiKey, AuthProfile, ClOrdId, MonotonicInstant, Symbol, TxId};

/// Per-endpoint rate-limit cost; `None` bypasses both trackers.
#[derive(Debug, Clone, PartialEq)]
pub enum RateLimitCost {
    /// Non-trading REST endpoint. Costs +1 or +2 depending on the endpoint.
    Api { cost: f64 },
    /// Trading REST or WS; keyed by `(ApiKey, Symbol)`.
    Trading { cost: f64, pair: Symbol },
    /// Account-wide trading op (`cancel_all`): charges every tracked pair,
    /// saturating and non-rejecting.
    TradingAccountWide { cost: f64 },
    /// Public endpoint — bypasses both trackers.
    None,
}

/// REST pipeline for Spot. Stage 1 consumes tracker headroom before signing so
/// exhaustion rejects with [`RestError::RateLimit`] without burning a nonce;
/// the SDK never blocks on limits.
pub struct RestSurface {
    transport: Arc<dyn HttpTransport>,
    auth: Arc<AuthStack>,
    api_rate_limit: Arc<SpotApiRateLimitTracker>,
    trading_rate_limit: Arc<SpotTradingRateLimitTracker>,
    clock: Arc<dyn Clock>,
    /// Wired via [`RestSurface::set_bus`]; unset skips emission.
    bus: OnceLock<Arc<DispatchEventBus>>,
    /// Bounded-LRU `cl_ord_id → (pair, sent_at)`, shared with `WsSurface`.
    cl_ord_id_index: Arc<ClOrdIdPairIndex>,
    /// Stage-6 transient-retry engine from `rest_retry_*` knobs at `.build()`.
    retry_engine: RetryEngine,
    /// Held across sign+send `.await` (per attempt) so nonces arrive in allocation order.
    nonce_send_gate: tokio::sync::Mutex<()>,
    /// Bound on the gated signed send; see [`GATE_BACKSTOP_GRACE`].
    request_timeout: std::time::Duration,
}

/// Grace over `request_timeout` so the transport's own timeout resolves first.
const GATE_BACKSTOP_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

impl RestSurface {
    #[cfg(any(test, feature = "test-support"))]
    /// Construct with a default `RetryEngine`. Production uses [`RestSurface::new_with_index`].
    pub fn new(
        transport: Arc<dyn HttpTransport>,
        auth: Arc<AuthStack>,
        api_rate_limit: Arc<SpotApiRateLimitTracker>,
        trading_rate_limit: Arc<SpotTradingRateLimitTracker>,
        clock: Arc<dyn Clock>,
        request_timeout: std::time::Duration,
    ) -> Self {
        let cl_ord_id_index = Arc::new(ClOrdIdPairIndex::new(1024));
        let retry_engine = RetryEngine::from_knobs(
            &crate::build::knobs::Knobs::defaults(),
            Arc::new(SplitMix64Jitter::from_os()) as Arc<dyn JitterSource>,
        );
        Self {
            transport,
            auth,
            api_rate_limit,
            trading_rate_limit,
            clock,
            bus: OnceLock::new(),
            cl_ord_id_index,
            retry_engine,
            nonce_send_gate: tokio::sync::Mutex::new(()),
            request_timeout,
        }
    }

    /// Like [`RestSurface::new`] but injects the shared index, [`RetryEngine`],
    /// and `request_timeout`.
    // Flat args: a params struct adds nothing at the single call site.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_index(
        transport: Arc<dyn HttpTransport>,
        auth: Arc<AuthStack>,
        api_rate_limit: Arc<SpotApiRateLimitTracker>,
        trading_rate_limit: Arc<SpotTradingRateLimitTracker>,
        clock: Arc<dyn Clock>,
        cl_ord_id_index: Arc<ClOrdIdPairIndex>,
        retry_engine: RetryEngine,
        request_timeout: std::time::Duration,
    ) -> Self {
        Self {
            transport,
            auth,
            api_rate_limit,
            trading_rate_limit,
            clock,
            bus: OnceLock::new(),
            cl_ord_id_index,
            retry_engine,
            nonce_send_gate: tokio::sync::Mutex::new(()),
            request_timeout,
        }
    }

    /// Wire the event bus. Idempotent — a second call is ignored.
    pub fn set_bus(&self, bus: Arc<DispatchEventBus>) {
        let _ = self.bus.set(bus);
    }

    /// Unauthenticated GET for public endpoints, wrapped in the retry loop.
    /// `request_id` is constant across retries.
    pub async fn public_get(
        &self,
        path: &str,
        query: &[(&str, &str)],
        retry: RetryPolicy,
        request_id: &str,
    ) -> Result<Value, RestError> {
        self.run_retry_loop(path, &RateLimitCost::None, &retry, request_id, || {
            self.public_get_once(path, query)
        })
        .await
    }

    /// One public-GET attempt; re-invoked per retry.
    async fn public_get_once(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<Value, RestError> {
        let response =
            self.transport
                .get_json(path, query)
                .await
                .map_err(|e| RestError::Transport {
                    kind: e.kind,
                    transient: e.transient,
                })?;

        parse_kraken_envelope(response)
    }

    /// Signed POST; convenience over [`RestSurface::signed_post_costed`] with
    /// Api cost 1.0 + idempotent retry.
    pub async fn signed_post(
        &self,
        path: &str,
        form: Vec<(String, String)>,
        profile: AuthProfile,
        request_id: &str,
    ) -> Result<Value, RestError> {
        self.signed_post_costed(
            path,
            form,
            profile,
            RateLimitCost::Api { cost: 1.0 },
            RetryPolicy::idempotent(),
            request_id,
        )
        .await
    }

    /// Signed POST with explicit rate-limit cost and [`RetryPolicy`]
    /// (trade endpoints must pass `RateLimitCost::Trading`).
    pub async fn signed_post_costed(
        &self,
        path: &str,
        form: Vec<(String, String)>,
        profile: AuthProfile,
        cost: RateLimitCost,
        retry: RetryPolicy,
        request_id: &str,
    ) -> Result<Value, RestError> {
        self.run_retry_loop(path, &cost, &retry, request_id, || {
            self.signed_post_costed_once(path, form.clone(), profile, cost.clone())
        })
        .await
    }

    /// One signed-POST attempt (Stages 1–5b); each retry re-enters with a fresh nonce.
    async fn signed_post_costed_once(
        &self,
        path: &str,
        form: Vec<(String, String)>,
        profile: AuthProfile,
        cost: RateLimitCost,
    ) -> Result<Value, RestError> {
        // Rate-limit before signing so a rejected request never burns a nonce.
        if let Some(key) = self.auth.api_key() {
            let now = self.clock.now();
            match &cost {
                RateLimitCost::Api { cost } => {
                    self.api_rate_limit
                        .consume(Scope::ApiKey(key.clone()), *cost, now)
                        .map_err(RestError::RateLimit)?;
                }
                RateLimitCost::Trading { cost, pair } => {
                    self.trading_rate_limit
                        .consume(Scope::Pair(key.clone(), pair.clone()), *cost, now)
                        .map_err(RestError::RateLimit)?;
                }
                RateLimitCost::TradingAccountWide { cost } => {
                    // Non-rejecting: cancel-all must never be gated by a prediction.
                    self.trading_rate_limit.charge_account_wide(
                        Scope::ApiKey(key.clone()),
                        *cost,
                        now,
                    );
                }
                RateLimitCost::None => {}
            }
        }

        // Serialize sign+send so nonces arrive in allocation order (per attempt).
        let _gate = self.nonce_send_gate.lock().await;

        let signed = self
            .auth
            .sign_form(profile, path, form)
            .map_err(RestError::Auth)?;

        let send = self.transport.post_form_signed(
            path,
            &signed.body,
            &signed.api_key_header,
            &signed.api_sign_header,
        );
        let response =
            match tokio::time::timeout(self.request_timeout + GATE_BACKSTOP_GRACE, send).await {
                Ok(send_result) => send_result.map_err(|e| RestError::Transport {
                    kind: e.kind,
                    transient: e.transient,
                })?,
                Err(_elapsed) => {
                    // Backstop fired: sent-ambiguous.
                    return Err(RestError::Transport {
                        kind: crate::transport::TransportErrorKind::RequestSentNoResponse,
                        transient: true,
                    });
                }
            };

        let result = parse_kraken_envelope(response);

        // Reactive API-counter snap. TradingPair/TradingDomain snap in the executor.
        if let Err(RestError::Kraken(ref codes)) = result {
            if let Some(key) = self.auth.api_key() {
                let now = self.clock.now();
                for code in codes {
                    if let Some(SnapTarget::Api) = classify_rate_limit_snap(code) {
                        self.api_rate_limit
                            .snap_to_cap(Scope::ApiKey(key.clone()), code, now);
                        break;
                    }
                }
            }
        }

        result
    }

    /// Stage 6: classify → budget gate → reason-aware backoff → emit → re-run.
    /// Cancel-during-backoff is terminal.
    async fn run_retry_loop<F, Fut>(
        &self,
        endpoint: &str,
        cost: &RateLimitCost,
        retry: &RetryPolicy,
        request_id: &str,
        mut run_once: F,
    ) -> Result<Value, RestError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<Value, RestError>>,
    {
        // Clamp to >= 1 so a 0 budget still runs the first attempt.
        let max_attempts = retry
            .max_attempts
            .unwrap_or_else(|| self.retry_engine.default_max_attempts())
            .max(1);
        let mut attempt: u32 = 0;
        let mut nonce_retries: u32 = 0;
        loop {
            attempt += 1;
            let err = match run_once().await {
                Ok(value) => return Ok(value),
                Err(e) => e,
            };
            let reason = match self.retry_engine.classify(&err, retry) {
                RetryDecision::Retry(reason) => reason,
                RetryDecision::NonTransient | RetryDecision::AmbiguousOrderPlacement => {
                    return Err(err);
                }
            };
            // Invalid nonce: exactly one fresh-nonce re-sign, not budget-governed.
            if reason == RetryReason::InvalidNonce {
                if nonce_retries >= 1 {
                    // Persistent invalid nonce after re-sign → another signer on the key.
                    if self.retry_engine.nonce_recovery() {
                        self.emit_nonce_poisoned(endpoint);
                    }
                    return Err(err);
                }
                nonce_retries += 1;
            } else if attempt >= max_attempts {
                return Err(err);
            }
            let backoff = self.compute_retry_backoff(attempt, reason, &err, cost);
            // Sleep first (cancellable). A drop here is terminal — no emit.
            tokio::time::sleep(backoff).await;
            self.emit_rest_retry_attempt(endpoint, request_id, attempt + 1, backoff, reason);
        }
    }

    /// Reason-aware backoff. `attempt` is the just-failed 1-based count
    /// (first retry uses backoff index 0).
    fn compute_retry_backoff(
        &self,
        attempt: u32,
        reason: RetryReason,
        err: &RestError,
        cost: &RateLimitCost,
    ) -> Duration {
        let base = self.retry_engine.next_backoff(attempt - 1);
        match reason {
            // Defer until API counter has headroom. Trading snap is executor-owned.
            RetryReason::RateLimitExceeded => {
                if let RateLimitCost::Api { cost: units } = cost {
                    if let Some(key) = self.auth.api_key() {
                        if let Some(t2h) = self.api_rate_limit.time_until_headroom(
                            Scope::ApiKey(key.clone()),
                            *units,
                            self.clock.now(),
                        ) {
                            return base.max(t2h);
                        }
                    }
                }
                base
            }
            // Honour EService:Throttled retry-after, bounded by the backoff ceiling.
            RetryReason::ServiceThrottled => {
                if let RestError::Kraken(codes) = err {
                    if let Some(ts) = parse_throttle_until(codes) {
                        let wait = wall_seconds_until(ts).min(self.retry_engine.backoff_ceiling());
                        return base.max(wait);
                    }
                }
                base
            }
            _ => base,
        }
    }

    /// Publish one envelope (skipped when no bus). Envelope `request_id` stays
    /// `None` so a never-awaited event isn't latched into the miss-buffer.
    fn publish_event(&self, event_type: EventType, payload: EventPayload) {
        if let Some(bus) = self.bus.get() {
            bus.publish(EventEnvelope {
                event_type,
                event_version: 1,
                timestamp_monotonic: self.clock.now(),
                request_id: None,
                payload,
            });
        }
    }

    /// Emit one `RestRetryAttempt`; correlation id rides the payload.
    fn emit_rest_retry_attempt(
        &self,
        endpoint: &str,
        request_id: &str,
        attempt: u32,
        backoff: Duration,
        reason: RetryReason,
    ) {
        self.publish_event(
            EventType::RestRetryAttempt,
            EventPayload::RestRetryAttempt {
                endpoint: endpoint.to_string(),
                request_id: Some(request_id.to_string()),
                attempt,
                backoff_ms: backoff.as_millis() as u64,
                reason,
            },
        );
    }

    /// Emit `NoncePoisonedEvent` (opt-in via `nonce_recovery`).
    /// `key_id_fingerprint` is non-reversible, never the raw key.
    fn emit_nonce_poisoned(&self, endpoint: &str) {
        let Some(key) = self.auth.api_key() else {
            return;
        };
        self.publish_event(
            EventType::NoncePoisonedEvent,
            EventPayload::NoncePoisonedEvent {
                key_id_fingerprint: crate::auth::derive_key_fingerprint(key),
                endpoint: endpoint.to_string(),
            },
        );
    }

    /// Locate an order by `cl_ord_id`: OpenOrders then ClosedOrders →
    /// `Found`/`NotPlaced`; emits `OrderReconciliationEvent` once.
    pub async fn find_order_by_cl_ord_id(
        &self,
        cl_ord_id: &ClOrdId,
    ) -> Result<ReconciliationOutcome, (String, RestError)> {
        let outcome = self.walk_reconciliation(cl_ord_id).await?;
        self.emit_reconciliation(cl_ord_id.clone(), outcome.clone());
        Ok(outcome)
    }

    /// Inner walk without emission so the event fires once at the boundary.
    async fn walk_reconciliation(
        &self,
        cl_ord_id: &ClOrdId,
    ) -> Result<ReconciliationOutcome, (String, RestError)> {
        let request_id = mint_request_id();
        let open = self
            .signed_post_costed(
                "/0/private/OpenOrders",
                vec![("cl_ord_id".to_string(), cl_ord_id.as_str().to_string())],
                AuthProfile::SpotV1,
                RateLimitCost::Api { cost: 2.0 },
                RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| (request_id.clone(), e))?;
        if let Some(found) = first_order_in_map(&open, "open") {
            // Open match → short-circuit; do NOT call ClosedOrders.
            return Ok(ReconciliationOutcome::Found {
                txid: found.0,
                status: found.1,
                lifecycle: LifecyclePosition::Open,
            });
        }

        let request_id = mint_request_id();
        let closed = self
            .signed_post_costed(
                "/0/private/ClosedOrders",
                vec![("cl_ord_id".to_string(), cl_ord_id.as_str().to_string())],
                AuthProfile::SpotV1,
                RateLimitCost::Api { cost: 2.0 },
                RetryPolicy::idempotent(),
                &request_id,
            )
            .await
            .map_err(|e| (request_id.clone(), e))?;
        if let Some(found) = first_order_in_map(&closed, "closed") {
            return Ok(ReconciliationOutcome::Found {
                txid: found.0,
                status: found.1,
                lifecycle: LifecyclePosition::Closed,
            });
        }

        Ok(ReconciliationOutcome::NotPlaced)
    }

    /// Emit `OrderReconciliationEvent` exactly once (skipped if no bus).
    fn emit_reconciliation(&self, cl_ord_id: ClOrdId, outcome: ReconciliationOutcome) {
        self.publish_event(
            EventType::OrderReconciliationEvent,
            EventPayload::OrderReconciliationEvent { cl_ord_id, outcome },
        );
    }

    /// Emit `OrderSubmittedEvent` on a definitive wire response.
    pub(crate) fn emit_order_submitted(
        &self,
        cl_ord_id: ClOrdId,
        amend_id: Option<AmendId>,
        op: OrderOp,
        status: OrderSubmitStatus,
        request_id: Option<String>,
    ) {
        self.publish_event(
            EventType::OrderSubmittedEvent,
            EventPayload::OrderSubmittedEvent {
                cl_ord_id,
                amend_id,
                op,
                status,
                request_id,
            },
        );
    }

    /// Emit `OrderPlacementAmbiguousEvent` when transport drops mid-send for
    /// `AddOrder`/`AmendOrder` (never for cancel).
    pub(crate) fn emit_placement_ambiguous(
        &self,
        cl_ord_id: ClOrdId,
        amend_id: Option<AmendId>,
        op: OrderOp,
        sent_at_monotonic: MonotonicInstant,
        request_id: Option<String>,
    ) {
        self.publish_event(
            EventType::OrderPlacementAmbiguousEvent,
            EventPayload::OrderPlacementAmbiguousEvent {
                cl_ord_id,
                amend_id,
                op,
                sent_at_monotonic,
                request_id,
            },
        );
    }

    /// `Weak` bus handle for a future-local `CancelEmitGuard`.
    pub(crate) fn bus_weak(&self) -> Option<Weak<DispatchEventBus>> {
        self.bus.get().map(Arc::downgrade)
    }

    /// Clone of the shared clock for a future-local `CancelEmitGuard`.
    pub(crate) fn clock_arc(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }

    /// Sample the shared monotonic clock.
    pub(crate) fn clock_now(&self) -> MonotonicInstant {
        self.clock.now()
    }

    /// Borrow the shared `ClOrdIdPairIndex`.
    pub(crate) fn cl_ord_id_index(&self) -> &Arc<ClOrdIdPairIndex> {
        &self.cl_ord_id_index
    }

    /// Borrow the shared `SpotTradingRateLimitTracker`.
    pub(crate) fn trading_tracker(&self) -> &Arc<SpotTradingRateLimitTracker> {
        &self.trading_rate_limit
    }

    /// Borrow the shared `SpotApiRateLimitTracker`.
    pub(crate) fn api_tracker(&self) -> &Arc<SpotApiRateLimitTracker> {
        &self.api_rate_limit
    }

    /// Configured `ApiKey`, or `None` if no credentials are wired.
    pub(crate) fn api_key(&self) -> Option<ApiKey> {
        self.auth.api_key().cloned()
    }
}

/// First `(txid, status)` from a server-filtered order map, or `None` if empty;
/// unknown `status` → [`OrderStatus::Unknown`].
fn first_order_in_map(result: &Value, wrapper: &str) -> Option<(TxId, OrderStatus)> {
    let map = result.get(wrapper).and_then(Value::as_object)?;
    let (txid, row) = map.iter().next()?;
    if txid.is_empty() {
        return None;
    }
    let status = row
        .get("status")
        .and_then(Value::as_str)
        .map(parse_order_status)
        .unwrap_or(OrderStatus::Unknown);
    Some((TxId::new(txid.clone()), status))
}

/// Parse a wire `status` string; unrecognized → [`OrderStatus::Unknown`].
fn parse_order_status(s: &str) -> OrderStatus {
    match s {
        "pending" => OrderStatus::Pending,
        "open" => OrderStatus::Open,
        "closed" => OrderStatus::Closed,
        "canceled" => OrderStatus::Canceled,
        "expired" => OrderStatus::Expired,
        _ => OrderStatus::Unknown,
    }
}

/// Mint a per-dispatch REST correlation id (UUID v4), constant across retries.
pub(crate) fn mint_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Parse the Kraken `{ error, result }` envelope into `result` or a [`RestError`].
fn parse_kraken_envelope(json: Value) -> Result<Value, RestError> {
    if let Some(errors) = json.get("error").and_then(Value::as_array) {
        if !errors.is_empty() {
            let strs: Vec<String> = errors
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if strs.is_empty() {
                // Empty Kraken(vec![]) would degrade to an uninformative Unknown.
                return Err(RestError::UnexpectedShape(format!(
                    "kraken error array contained no string elements: {errors:?}"
                )));
            }
            return Err(RestError::Kraken(strs));
        }
    }

    json.get("result")
        .cloned()
        .ok_or_else(|| RestError::UnexpectedShape("missing `result` field".into()))
}

/// First `EService:Throttled: <ts>` timestamp in the array, else `None`.
fn parse_throttle_until(codes: &[String]) -> Option<u64> {
    codes
        .iter()
        .find_map(|code| crate::error::parse_throttle_until(code))
}

/// Wall-clock seconds until absolute Unix-seconds `ts`, clamped at zero
/// (`SystemTime` because the throttle timestamp is wall-clock).
fn wall_seconds_until(ts: u64) -> Duration {
    use std::time::{SystemTime, UNIX_EPOCH};
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(now) => Duration::from_secs(ts.saturating_sub(now.as_secs())),
        Err(_) => Duration::ZERO,
    }
}

/// Errors from `RestSurface`. No `Eq` (`RateLimitExceeded` carries `f64`);
/// `PartialEq` kept for tests (`f64` never `NaN` by construction).
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RestError {
    /// Transport failure with typed kind + `transient` flag.
    #[error("Transport: {kind:?}.")]
    Transport {
        /// Failure classification; sent-ambiguous kinds mark requests that may have reached the wire.
        kind: crate::transport::TransportErrorKind,
        /// `true` → retry-eligible in Stage 6; `false` bubbles without retry.
        transient: bool,
    },

    /// Authentication failure. No terminal period — inner `AuthError` Display already has one.
    #[error("Auth: {0}")]
    Auth(AuthError),

    /// Kraken returned a non-empty `error` array (raw codes such as `EAPI:*`).
    #[error("Kraken: {0:?}")]
    Kraken(Vec<String>),

    /// Response envelope did not match `{ error, result }`.
    #[error("Unexpected shape: {0}.")]
    UnexpectedShape(String),

    /// Stage 1 rate-limit rejection. SDK does not auto-block — caller decides.
    /// Inner `tracker` is `"api"` or `"trading"`.
    #[error("Rate limit exceeded ({}): scope={:?}, current={}, cap={}.", .0.tracker, .0.scope, .0.current, .0.cap)]
    RateLimit(RateLimitExceeded),
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::transport::TransportError;

    const TEST_API_KEY: &str = "test-api-key";

    #[test]
    fn parse_envelope_extracts_result_on_success() {
        let json = serde_json::json!({
            "error": [],
            "result": { "BTC/USD": { "a": ["50000"] } }
        });
        let result = parse_kraken_envelope(json).unwrap();
        assert!(result.get("BTC/USD").is_some());
    }

    #[test]
    fn parse_envelope_surfaces_kraken_errors() {
        let json = serde_json::json!({
            "error": ["EAPI:Invalid nonce"],
            "result": {}
        });
        let err = parse_kraken_envelope(json).unwrap_err();
        match err {
            RestError::Kraken(codes) => assert_eq!(codes, vec!["EAPI:Invalid nonce"]),
            other => panic!("expected Kraken, got {:?}", other),
        }
    }

    #[test]
    fn parse_envelope_guards_nonstring_error_array() {
        let json = serde_json::json!({
            "error": [{ "code": 500 }, 42],
            "result": {}
        });
        let err = parse_kraken_envelope(json).unwrap_err();
        match err {
            RestError::UnexpectedShape(detail) => {
                assert!(detail.contains("no string elements"), "detail: {detail}")
            }
            other => panic!("expected UnexpectedShape, got {:?}", other),
        }
    }

    use crate::auth::AuthStack;
    use crate::clock::SystemClock;
    use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};
    use crate::rate_limit::{SpotApiRateLimitTracker, SpotTradingRateLimitTracker, Tier};
    use crate::transport::HttpTransport;
    use crate::types::{ApiKey, ApiSecret};
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use std::collections::HashMap;

    struct NeverCalledMock;
    #[async_trait::async_trait]
    impl HttpTransport for NeverCalledMock {
        async fn get_json(
            &self,
            _path: &str,
            _query: &[(&str, &str)],
        ) -> Result<Value, TransportError> {
            panic!("HTTP must NOT be called when Stage 1 rate-limit rejects");
        }
        async fn post_form_signed(
            &self,
            _path: &str,
            _body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<Value, TransportError> {
            panic!("HTTP must NOT be called when Stage 1 rate-limit rejects");
        }
    }

    fn test_auth_and_trackers(
        clock: Arc<dyn Clock>,
        knobs: Arc<crate::build::knobs::Knobs>,
    ) -> (
        Arc<AuthStack>,
        Arc<DispatchEventBus>,
        Arc<SpotApiRateLimitTracker>,
        Arc<SpotTradingRateLimitTracker>,
    ) {
        let api_key = ApiKey::new(TEST_API_KEY);
        let secret = ApiSecret::from_base64(&BASE64.encode(vec![0x01u8; 32])).unwrap();
        let signer = crate::auth::SpotRestHmacSha512Signer::new(api_key.clone(), secret);
        let mut signers: HashMap<AuthProfile, Arc<dyn crate::auth::AuthSigner>> = HashMap::new();
        signers.insert(AuthProfile::SpotV1, Arc::new(signer));
        let auth = Arc::new(AuthStack::new(
            Some(api_key),
            None,
            Arc::new(crate::auth::SystemClockNonceSource::new()),
            signers,
            crate::auth::TokenLifecycleManager::for_test(),
        ));
        let bus = Arc::new(DispatchEventBus::new(
            DispatchEventBusConfig::defaults(),
            Arc::clone(&clock),
        ));
        let api_rl = Arc::new(SpotApiRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::clone(&knobs),
        ));
        let trading_rl = Arc::new(SpotTradingRateLimitTracker::new(
            Tier::Starter,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::clone(&knobs),
        ));
        (auth, bus, api_rl, trading_rl)
    }

    fn build_rest_with_credentials() -> (Arc<RestSurface>, Arc<SpotApiRateLimitTracker>) {
        let mock: Arc<dyn HttpTransport> = Arc::new(NeverCalledMock);
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let (auth, _bus, api_rl, trading_rl) = test_auth_and_trackers(
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        );
        let rest = Arc::new(RestSurface::new(
            mock,
            auth,
            Arc::clone(&api_rl),
            trading_rl,
            clock,
            std::time::Duration::from_secs(30),
        ));
        (rest, api_rl)
    }

    #[allow(non_snake_case)]
    #[tokio::test]
    async fn signed_post_rejects_with_RateLimit_error_when_api_counter_exhausted() {
        let (rest, api_rl) = build_rest_with_credentials();
        let now = crate::types::MonotonicInstant::now();
        for _ in 0..15 {
            api_rl
                .consume(
                    crate::rate_limit::Scope::ApiKey(ApiKey::new(TEST_API_KEY)),
                    1.0,
                    now,
                )
                .unwrap();
        }
        let err = rest
            .signed_post(
                "/0/private/Balance",
                vec![],
                AuthProfile::SpotV1,
                "rid-test",
            )
            .await
            .unwrap_err();
        match err {
            RestError::RateLimit(re) => {
                assert_eq!(re.tracker, "api");
                assert!(matches!(re.scope, Scope::ApiKey(_)));
            }
            other => panic!("expected RateLimit error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn signed_post_costed_routes_trading_cost_to_trading_tracker() {
        let mock: Arc<dyn HttpTransport> = Arc::new(NeverCalledMock);
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let (auth, _bus, api_rl, trading_rl) = test_auth_and_trackers(
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        );
        let rest = Arc::new(RestSurface::new(
            mock,
            auth,
            api_rl,
            Arc::clone(&trading_rl),
            clock,
            std::time::Duration::from_secs(30),
        ));

        let now = crate::types::MonotonicInstant::now();
        let api_key = ApiKey::new(TEST_API_KEY);
        let pair = Symbol::new("BTC/USD").unwrap();
        for _ in 0..60 {
            trading_rl
                .consume(Scope::Pair(api_key.clone(), pair.clone()), 1.0, now)
                .unwrap();
        }
        let err = rest
            .signed_post_costed(
                "/0/private/AddOrder",
                vec![],
                AuthProfile::SpotV1,
                RateLimitCost::Trading {
                    cost: 1.0,
                    pair: pair.clone(),
                },
                RetryPolicy::never_retry(),
                "rid-test",
            )
            .await
            .unwrap_err();
        match err {
            RestError::RateLimit(re) => {
                assert_eq!(re.tracker, "trading");
                if let Scope::Pair(_, p) = re.scope {
                    assert_eq!(p.as_str(), "BTC/USD");
                } else {
                    panic!("expected Pair scope");
                }
            }
            other => panic!("expected RateLimit error, got {:?}", other),
        }
    }

    struct CannedMock {
        canned: Value,
    }
    #[async_trait::async_trait]
    impl HttpTransport for CannedMock {
        async fn get_json(
            &self,
            _path: &str,
            _query: &[(&str, &str)],
        ) -> Result<Value, TransportError> {
            Ok(self.canned.clone())
        }
        async fn post_form_signed(
            &self,
            _path: &str,
            _body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<Value, TransportError> {
            Ok(self.canned.clone())
        }
    }

    /// Clock pinned at T=0 (no headroom decay flake).
    struct FixedClock(std::time::Duration);
    impl crate::clock::Clock for FixedClock {
        fn now(&self) -> crate::types::MonotonicInstant {
            crate::types::MonotonicInstant(self.0)
        }
    }

    fn build_rest_with_canned(
        canned: Value,
    ) -> (
        Arc<RestSurface>,
        Arc<SpotApiRateLimitTracker>,
        Arc<SpotTradingRateLimitTracker>,
    ) {
        let mock: Arc<dyn HttpTransport> = Arc::new(CannedMock { canned });
        let clock: Arc<dyn Clock> = Arc::new(FixedClock(std::time::Duration::from_secs(0)));
        let (auth, _bus, api_rl, trading_rl) = test_auth_and_trackers(
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        );
        let rest = Arc::new(RestSurface::new(
            mock,
            auth,
            Arc::clone(&api_rl),
            Arc::clone(&trading_rl),
            clock,
            std::time::Duration::from_secs(30),
        ));
        (rest, api_rl, trading_rl)
    }

    #[tokio::test]
    async fn non_trade_rest_eapi_rejection_snaps_api_tracker() {
        let canned = serde_json::json!({
            "error": ["EAPI:Rate limit exceeded"],
            "result": {}
        });
        let (rest, api_rl, _trading_rl) = build_rest_with_canned(canned);

        let t0 = crate::types::MonotonicInstant(std::time::Duration::from_secs(0));
        let headroom_before = api_rl
            .headroom(Scope::ApiKey(ApiKey::new(TEST_API_KEY)), t0)
            .unwrap();
        assert!(
            headroom_before > 0.0,
            "expected positive headroom before snap"
        );

        // never_retry isolates Stage-5b snap; idempotent would retry into Stage-1.
        let err = rest
            .signed_post_costed(
                "/0/private/Balance",
                vec![],
                AuthProfile::SpotV1,
                RateLimitCost::Api { cost: 1.0 },
                RetryPolicy::never_retry(),
                "rid-test",
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, RestError::Kraken(_)),
            "expected Kraken error, got {err:?}"
        );

        let headroom_after = api_rl
            .headroom(Scope::ApiKey(ApiKey::new(TEST_API_KEY)), t0)
            .unwrap();
        assert!(
            headroom_after < f64::EPSILON,
            "API tracker should be snapped to cap (headroom==0), got {headroom_after}"
        );
    }

    #[tokio::test]
    async fn trade_eapi_still_snaps_api_once() {
        let canned = serde_json::json!({
            "error": ["EAPI:Rate limit exceeded"],
            "result": {}
        });
        let (rest, api_rl, _trading_rl) = build_rest_with_canned(canned);

        let t0 = crate::types::MonotonicInstant(std::time::Duration::from_secs(0));
        let headroom_before = api_rl
            .headroom(Scope::ApiKey(ApiKey::new(TEST_API_KEY)), t0)
            .unwrap();
        assert!(headroom_before > 0.0);

        let pair = crate::types::Symbol::new("BTC/USD").unwrap();
        let err = rest
            .signed_post_costed(
                "/0/private/AddOrder",
                vec![],
                AuthProfile::SpotV1,
                RateLimitCost::Trading {
                    cost: 1.0,
                    pair: pair.clone(),
                },
                RetryPolicy::never_retry(),
                "rid-test",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, RestError::Kraken(_)), "got {err:?}");

        let headroom_after = api_rl
            .headroom(Scope::ApiKey(ApiKey::new(TEST_API_KEY)), t0)
            .unwrap();
        assert!(
            headroom_after < f64::EPSILON,
            "API tracker should be snapped to cap, got {headroom_after}"
        );
    }

    #[tokio::test]
    async fn trade_eorder_snaps_trading_not_api() {
        let canned = serde_json::json!({
            "error": ["EOrder:Rate limit exceeded"],
            "result": {}
        });
        let (rest, api_rl, _trading_rl) = build_rest_with_canned(canned);

        let pair = crate::types::Symbol::new("BTC/USD").unwrap();
        let err = rest
            .signed_post_costed(
                "/0/private/AddOrder",
                vec![],
                AuthProfile::SpotV1,
                RateLimitCost::Trading {
                    cost: 1.0,
                    pair: pair.clone(),
                },
                RetryPolicy::never_retry(),
                "rid-test",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, RestError::Kraken(_)), "got {err:?}");

        let t0 = crate::types::MonotonicInstant(std::time::Duration::from_secs(0));
        let headroom_after = api_rl
            .headroom(Scope::ApiKey(ApiKey::new(TEST_API_KEY)), t0)
            .unwrap();
        assert!(
            headroom_after > 0.0,
            "API tracker must NOT be snapped for EOrder: rejection, got headroom={headroom_after}"
        );
    }

    use crate::jitter::FixedJitter;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// Scripted mock; calls beyond the script panic.
    struct SeqMock {
        responses: Mutex<VecDeque<Result<Value, TransportError>>>,
        calls: AtomicUsize,
    }
    impl SeqMock {
        fn new(responses: Vec<Result<Value, TransportError>>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(responses.into_iter().collect()),
                calls: AtomicUsize::new(0),
            })
        }
        fn next_response(&self) -> Result<Value, TransportError> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            self.responses
                .lock()
                .expect("seqmock lock")
                .pop_front()
                .unwrap_or_else(|| panic!("SeqMock: more calls than scripted responses"))
        }
        fn calls(&self) -> usize {
            self.calls.load(AtomicOrdering::SeqCst)
        }
    }
    #[async_trait::async_trait]
    impl HttpTransport for SeqMock {
        async fn get_json(
            &self,
            _path: &str,
            _query: &[(&str, &str)],
        ) -> Result<Value, TransportError> {
            self.next_response()
        }
        async fn post_form_signed(
            &self,
            _path: &str,
            _body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<Value, TransportError> {
            self.next_response()
        }
    }

    fn transient_transport_err() -> Result<Value, TransportError> {
        Err(TransportError {
            kind: crate::transport::TransportErrorKind::TcpConnectTimeout,
            transient: true,
        })
    }
    fn ok_value() -> Result<Value, TransportError> {
        Ok(serde_json::json!({ "error": [], "result": { "ok": true } }))
    }
    fn kraken_err(code: &str) -> Result<Value, TransportError> {
        Ok(serde_json::json!({ "error": [code], "result": {} }))
    }

    /// Credentialed RestSurface over SeqMock; FixedClock at T=0.
    fn build_rest_retry(
        responses: Vec<Result<Value, TransportError>>,
        jitter: Arc<dyn JitterSource>,
        max_attempts: u32,
    ) -> (
        Arc<RestSurface>,
        Arc<SeqMock>,
        Arc<SpotApiRateLimitTracker>,
        Arc<DispatchEventBus>,
    ) {
        build_rest_retry_cfg(responses, jitter, max_attempts, false)
    }

    /// As [`build_rest_retry`] with optional `nonce_recovery`.
    fn build_rest_retry_cfg(
        responses: Vec<Result<Value, TransportError>>,
        jitter: Arc<dyn JitterSource>,
        max_attempts: u32,
        nonce_recovery: bool,
    ) -> (
        Arc<RestSurface>,
        Arc<SeqMock>,
        Arc<SpotApiRateLimitTracker>,
        Arc<DispatchEventBus>,
    ) {
        let mock = SeqMock::new(responses);
        let clock: Arc<dyn Clock> = Arc::new(FixedClock(std::time::Duration::from_secs(0)));
        let mut knobs = crate::build::knobs::Knobs::defaults();
        knobs.rest_retry_max_attempts = max_attempts;
        knobs.nonce_recovery = nonce_recovery;
        let knobs = Arc::new(knobs);
        let (auth, bus, api_rl, trading_rl) =
            test_auth_and_trackers(Arc::clone(&clock), Arc::clone(&knobs));
        let engine = RetryEngine::from_knobs(&knobs, jitter);
        let cl_ord_id_index = Arc::new(ClOrdIdPairIndex::new(1024));
        let rest = Arc::new(RestSurface::new_with_index(
            Arc::clone(&mock) as Arc<dyn HttpTransport>,
            auth,
            Arc::clone(&api_rl),
            Arc::clone(&trading_rl),
            clock,
            cl_ord_id_index,
            engine,
            std::time::Duration::from_secs(30),
        ));
        rest.set_bus(Arc::clone(&bus));
        (rest, mock, api_rl, bus)
    }

    #[tokio::test]
    async fn transient_failures_retry_then_succeed() {
        let (rest, mock, _api, _bus) = build_rest_retry(
            vec![
                transient_transport_err(),
                transient_transport_err(),
                ok_value(),
            ],
            Arc::new(FixedJitter(0.0)),
            3,
        );
        let result = rest
            .public_get("/0/public/Time", &[], RetryPolicy::idempotent(), "rid-test")
            .await;
        assert!(
            result.is_ok(),
            "should succeed on the 3rd attempt: {result:?}"
        );
        assert_eq!(mock.calls(), 3, "two retries after the first failure");
    }

    #[tokio::test]
    async fn transient_failures_exhaust_budget_then_bubble() {
        let (rest, mock, _api, _bus) = build_rest_retry(
            vec![
                transient_transport_err(),
                transient_transport_err(),
                transient_transport_err(),
            ],
            Arc::new(FixedJitter(0.0)),
            3,
        );
        let result = rest
            .public_get("/0/public/Time", &[], RetryPolicy::idempotent(), "rid-test")
            .await;
        assert!(
            matches!(
                result,
                Err(RestError::Transport {
                    transient: true,
                    ..
                })
            ),
            "budget exhausted → last error bubbles: {result:?}"
        );
        assert_eq!(mock.calls(), 3, "exactly max_attempts attempts, no more");
    }

    #[tokio::test]
    async fn never_retry_policy_makes_single_attempt() {
        let (rest, mock, _api, _bus) = build_rest_retry(
            vec![transient_transport_err()],
            Arc::new(FixedJitter(0.0)),
            3,
        );
        let result = rest
            .public_get(
                "/0/public/Time",
                &[],
                RetryPolicy::never_retry(),
                "rid-test",
            )
            .await;
        assert!(
            result.is_err(),
            "transient failure still bubbles under never_retry"
        );
        assert_eq!(mock.calls(), 1, "never_retry → single attempt");
    }

    /// Cancel during backoff: no further attempt (emit is after sleep).
    #[tokio::test]
    async fn cancel_during_backoff_makes_no_further_attempt() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};

        let (rest, mock, _api, _bus) = build_rest_retry(
            vec![transient_transport_err()],
            Arc::new(FixedJitter(1.0)),
            3,
        );
        let mut cx = Context::from_waker(Waker::noop());
        let query: &[(&str, &str)] = &[];
        let mut fut = Box::pin(rest.public_get(
            "/0/public/Time",
            query,
            RetryPolicy::idempotent(),
            "rid-test",
        ));
        assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Pending));
        assert_eq!(
            mock.calls(),
            1,
            "only the first attempt ran before the backoff"
        );
        drop(fut);
        assert_eq!(
            mock.calls(),
            1,
            "cancellation during backoff starts no retry"
        );
    }

    #[tokio::test]
    async fn invalid_nonce_retries_exactly_once_then_bubbles() {
        let (rest, mock, _api, _bus) = build_rest_retry(
            vec![
                kraken_err("EAPI:Invalid nonce"),
                kraken_err("EAPI:Invalid nonce"),
            ],
            Arc::new(FixedJitter(0.0)),
            3,
        );
        let result = rest
            .signed_post(
                "/0/private/Balance",
                vec![],
                AuthProfile::SpotV1,
                "rid-test",
            )
            .await;
        assert!(
            matches!(result, Err(RestError::Kraken(_))),
            "second invalid nonce bubbles non-transient: {result:?}"
        );
        assert_eq!(
            mock.calls(),
            2,
            "retry-once: original + one fresh-nonce retry, then bubble"
        );
    }

    #[tokio::test]
    async fn invalid_nonce_self_heals_once_even_when_budget_is_one() {
        let (rest, mock, _api, _bus) = build_rest_retry(
            vec![
                kraken_err("EAPI:Invalid nonce"),
                kraken_err("EAPI:Invalid nonce"),
            ],
            Arc::new(FixedJitter(0.0)),
            1, // transient retries disabled — but the nonce self-heal still fires
        );
        let result = rest
            .signed_post(
                "/0/private/Balance",
                vec![],
                AuthProfile::SpotV1,
                "rid-test",
            )
            .await;
        assert!(matches!(result, Err(RestError::Kraken(_))));
        assert_eq!(
            mock.calls(),
            2,
            "invalid-nonce self-heal is budget-independent: 1 original + 1 retry"
        );
    }

    /// never_retry: no fresh-nonce re-sign on invalid nonce.
    #[tokio::test]
    async fn never_retry_takes_no_nonce_heal() {
        let (rest, mock, _api, _bus) = build_rest_retry(
            vec![kraken_err("EAPI:Invalid nonce")],
            Arc::new(FixedJitter(0.0)),
            3,
        );
        let result = rest
            .signed_post_costed(
                "/0/private/AddOrder",
                vec![],
                AuthProfile::SpotV1,
                RateLimitCost::Api { cost: 1.0 },
                RetryPolicy::never_retry(),
                "rid-test",
            )
            .await;
        assert!(matches!(result, Err(RestError::Kraken(_))));
        assert_eq!(mock.calls(), 1, "never-retry: no fresh-nonce re-sign");
    }

    /// Validate-mode: one fresh-nonce re-sign then succeed.
    #[tokio::test]
    async fn nonce_healed_never_retry_heals_once_then_succeeds() {
        let (rest, mock, _api, _bus) = build_rest_retry(
            vec![kraken_err("EAPI:Invalid nonce"), ok_value()],
            Arc::new(FixedJitter(0.0)),
            3,
        );
        let result = rest
            .signed_post_costed(
                "/0/private/AddOrder",
                vec![],
                AuthProfile::SpotV1,
                RateLimitCost::Api { cost: 1.0 },
                RetryPolicy::never_retry_except_nonce(),
                "rid-test",
            )
            .await;
        assert!(result.is_ok(), "healed second attempt succeeds: {result:?}");
        assert_eq!(mock.calls(), 2, "exactly one fresh-nonce re-sign");
    }

    /// Validate-mode heal is exactly-once and nonce-only.
    #[tokio::test]
    async fn nonce_healed_never_retry_stays_exactly_once_and_nonce_only() {
        let (rest, mock, _api, _bus) = build_rest_retry(
            vec![
                kraken_err("EAPI:Invalid nonce"),
                kraken_err("EAPI:Invalid nonce"),
            ],
            Arc::new(FixedJitter(0.0)),
            3,
        );
        let result = rest
            .signed_post_costed(
                "/0/private/AddOrder",
                vec![],
                AuthProfile::SpotV1,
                RateLimitCost::Api { cost: 1.0 },
                RetryPolicy::never_retry_except_nonce(),
                "rid-test",
            )
            .await;
        assert!(matches!(result, Err(RestError::Kraken(_))));
        assert_eq!(
            mock.calls(),
            2,
            "second invalid nonce bubbles, no third try"
        );

        let (rest2, mock2, _api2, _bus2) = build_rest_retry(
            vec![kraken_err("EService:Unavailable")],
            Arc::new(FixedJitter(0.0)),
            3,
        );
        let result2 = rest2
            .signed_post_costed(
                "/0/private/AddOrder",
                vec![],
                AuthProfile::SpotV1,
                RateLimitCost::Api { cost: 1.0 },
                RetryPolicy::never_retry_except_nonce(),
                "rid-test",
            )
            .await;
        assert!(matches!(result2, Err(RestError::Kraken(_))));
        assert_eq!(
            mock2.calls(),
            1,
            "a non-nonce failure must not retry under the validate-mode policy"
        );
    }

    /// Persistent invalid nonce with nonce_recovery emits NoncePoisonedEvent.
    #[tokio::test]
    async fn nonce_poisoning_emits_event_when_recovery_enabled() {
        let (rest, _mock, _api, bus) = build_rest_retry_cfg(
            vec![
                kraken_err("EAPI:Invalid nonce"),
                kraken_err("EAPI:Invalid nonce"),
            ],
            Arc::new(FixedJitter(0.0)),
            3,
            true, // nonce_recovery ON
        );
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let (tx, rx) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell = std::sync::Mutex::new(Some(tx));
        let _h = bus.subscribe(
            EventType::NoncePoisonedEvent,
            Arc::new(move |env| {
                if let Some(tx) = tx_cell.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            1,
        );
        let result = rest
            .signed_post(
                "/0/private/Balance",
                vec![],
                AuthProfile::SpotV1,
                "rid-test",
            )
            .await;
        assert!(matches!(result, Err(RestError::Kraken(_))));
        let env = tokio::time::timeout(std::time::Duration::from_millis(2000), rx)
            .await
            .expect("NoncePoisonedEvent not delivered within 2s")
            .expect("oneshot sender dropped");
        match env.payload {
            EventPayload::NoncePoisonedEvent {
                key_id_fingerprint,
                endpoint,
            } => {
                assert_eq!(endpoint, "/0/private/Balance");
                assert!(
                    !key_id_fingerprint.is_empty(),
                    "fingerprint must be present (never the raw key)"
                );
            }
            other => panic!("expected NoncePoisonedEvent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn nonce_poisoning_emits_no_event_when_recovery_disabled() {
        let (rest, _mock, _api, bus) = build_rest_retry(
            vec![
                kraken_err("EAPI:Invalid nonce"),
                kraken_err("EAPI:Invalid nonce"),
            ],
            Arc::new(FixedJitter(0.0)),
            3,
        );
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count_cb = Arc::clone(&count);
        let _h = bus.subscribe(
            EventType::NoncePoisonedEvent,
            Arc::new(move |_env| {
                count_cb.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }),
            1,
        );
        let result = rest
            .signed_post(
                "/0/private/Balance",
                vec![],
                AuthProfile::SpotV1,
                "rid-test",
            )
            .await;
        assert!(matches!(result, Err(RestError::Kraken(_))));
        // Settle: a stray emit would land a beat later.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            count.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "no NoncePoisonedEvent when nonce_recovery is off"
        );
    }

    #[tokio::test]
    async fn non_nonce_transient_respects_budget_of_one() {
        let (rest, mock, _api, _bus) = build_rest_retry(
            vec![transient_transport_err()],
            Arc::new(FixedJitter(0.0)),
            1,
        );
        let result = rest
            .public_get("/0/public/Time", &[], RetryPolicy::idempotent(), "rid-test")
            .await;
        assert!(result.is_err());
        assert_eq!(
            mock.calls(),
            1,
            "budget=1 → single attempt for a transport transient"
        );
    }

    /// Zero jitter: backoff equals headroom-recovery duration.
    #[test]
    fn rate_limit_backoff_defers_to_headroom() {
        let (rest, _mock, api_rl, _bus) = build_rest_retry(vec![], Arc::new(FixedJitter(0.0)), 3);
        let t0 = MonotonicInstant(std::time::Duration::from_secs(0));
        let scope = Scope::ApiKey(ApiKey::new(TEST_API_KEY));
        api_rl.snap_to_cap(scope.clone(), "EAPI:Rate limit exceeded", t0);
        let expected = api_rl.time_until_headroom(scope, 1.0, t0).unwrap();
        assert!(
            expected > Duration::ZERO,
            "a snapped tracker should require recovery time"
        );
        let err = RestError::Kraken(vec!["EAPI:Rate limit exceeded".into()]);
        let backoff = rest.compute_retry_backoff(
            1,
            RetryReason::RateLimitExceeded,
            &err,
            &RateLimitCost::Api { cost: 1.0 },
        );
        assert_eq!(
            backoff, expected,
            "rate-limit backoff defers to time-until-headroom (zero jitter)"
        );
    }

    #[tokio::test]
    async fn retry_emits_rest_retry_attempt_event() {
        let (rest, _mock, _api, bus) = build_rest_retry(
            vec![transient_transport_err(), ok_value()],
            Arc::new(FixedJitter(0.0)),
            3,
        );
        bus.start_dispatch_reactor(&tokio::runtime::Handle::current());
        let (tx, rx) = tokio::sync::oneshot::channel::<EventEnvelope>();
        let tx_cell = std::sync::Mutex::new(Some(tx));
        let _h = bus.subscribe(
            EventType::RestRetryAttempt,
            Arc::new(move |env| {
                if let Some(tx) = tx_cell.lock().unwrap().take() {
                    let _ = tx.send(env.clone());
                }
            }),
            4,
        );
        let result = rest
            .public_get("/0/public/Time", &[], RetryPolicy::idempotent(), "rid-test")
            .await;
        assert!(result.is_ok());
        let env = tokio::time::timeout(std::time::Duration::from_millis(500), rx)
            .await
            .expect("RestRetryAttempt not delivered within 500ms")
            .expect("oneshot sender dropped");
        assert_eq!(
            env.request_id, None,
            "envelope stays None — the correlation id rides the payload"
        );
        match env.payload {
            EventPayload::RestRetryAttempt {
                endpoint,
                attempt,
                reason,
                request_id,
                ..
            } => {
                assert_eq!(endpoint, "/0/public/Time");
                assert_eq!(attempt, 2, "first retry is attempt 2 (original was 1)");
                assert_eq!(reason, RetryReason::TransportError);
                assert_eq!(
                    request_id.as_deref(),
                    Some("rid-test"),
                    "payload carries the caller's per-dispatch correlation id"
                );
            }
            other => panic!("expected RestRetryAttempt, got {other:?}"),
        }
    }

    /// Records nonce arrival order; yields so senders can interleave.
    struct NonceRecordingMock {
        arrivals: Mutex<Vec<u64>>,
    }
    impl NonceRecordingMock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                arrivals: Mutex::new(Vec::new()),
            })
        }
        fn parse_nonce(body: &str) -> u64 {
            body.split('&')
                .find_map(|kv| kv.strip_prefix("nonce="))
                .expect("signed body must carry a nonce field")
                .parse::<u64>()
                .expect("nonce must be a u64")
        }
        fn arrivals(&self) -> Vec<u64> {
            self.arrivals.lock().expect("arrivals lock").clone()
        }
    }
    #[async_trait::async_trait]
    impl HttpTransport for NonceRecordingMock {
        async fn get_json(
            &self,
            _path: &str,
            _query: &[(&str, &str)],
        ) -> Result<Value, TransportError> {
            tokio::task::yield_now().await;
            Ok(serde_json::json!({ "error": [], "result": { "ok": true } }))
        }
        async fn post_form_signed(
            &self,
            _path: &str,
            body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<Value, TransportError> {
            let nonce = Self::parse_nonce(body);
            self.arrivals.lock().expect("arrivals lock").push(nonce);
            tokio::task::yield_now().await;
            Ok(serde_json::json!({ "error": [], "result": { "ok": true } }))
        }
    }

    fn build_rest_with_transport(transport: Arc<dyn HttpTransport>) -> Arc<RestSurface> {
        build_rest_with_transport_timeout(transport, std::time::Duration::from_secs(30))
    }

    fn build_rest_with_transport_timeout(
        transport: Arc<dyn HttpTransport>,
        request_timeout: std::time::Duration,
    ) -> Arc<RestSurface> {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let (auth, _bus, api_rl, trading_rl) = test_auth_and_trackers(
            Arc::clone(&clock),
            Arc::new(crate::build::knobs::Knobs::defaults()),
        );
        Arc::new(RestSurface::new(
            transport,
            auth,
            api_rl,
            trading_rl,
            clock,
            request_timeout,
        ))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn signed_sends_arrive_in_strict_nonce_order_under_concurrency() {
        const N: usize = 32;
        let mock = NonceRecordingMock::new();
        let rest = build_rest_with_transport(Arc::clone(&mock) as Arc<dyn HttpTransport>);

        let mut handles = Vec::with_capacity(N);
        for _ in 0..N {
            let rest = Arc::clone(&rest);
            handles.push(tokio::spawn(async move {
                rest.signed_post_costed(
                    "/0/private/Balance",
                    Vec::new(),
                    AuthProfile::SpotV1,
                    RateLimitCost::None,
                    RetryPolicy::idempotent(),
                    "rid-test",
                )
                .await
            }));
        }
        for h in handles {
            h.await.expect("task panicked").expect("signed POST failed");
        }

        let arrivals = mock.arrivals();
        assert_eq!(
            arrivals.len(),
            N,
            "every signed call must reach the transport"
        );
        for w in arrivals.windows(2) {
            assert!(
                w[0] < w[1],
                "nonces must ARRIVE strictly increasing (serialized); got {arrivals:?}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn public_reads_do_not_take_the_signed_gate() {
        let mock = NonceRecordingMock::new();
        let rest = build_rest_with_transport(Arc::clone(&mock) as Arc<dyn HttpTransport>);

        let mut handles = Vec::new();
        for _ in 0..8 {
            let rest_pub = Arc::clone(&rest);
            handles.push(tokio::spawn(async move {
                rest_pub
                    .public_get("/0/public/Time", &[], RetryPolicy::idempotent(), "rid-test")
                    .await
                    .map(|_| ())
            }));
            let rest_sig = Arc::clone(&rest);
            handles.push(tokio::spawn(async move {
                rest_sig
                    .signed_post_costed(
                        "/0/private/Balance",
                        Vec::new(),
                        AuthProfile::SpotV1,
                        RateLimitCost::None,
                        RetryPolicy::idempotent(),
                        "rid-test",
                    )
                    .await
                    .map(|_| ())
            }));
        }
        for h in handles {
            h.await.expect("task panicked").expect("call failed");
        }

        assert_eq!(
            mock.arrivals().len(),
            8,
            "only signed POSTs may reach post_form_signed; public GETs must not"
        );
    }

    /// First post_form_signed hangs; later calls succeed (proves gate release).
    struct FirstCallHangsMock {
        calls: AtomicUsize,
    }
    impl FirstCallHangsMock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
            })
        }
        fn calls(&self) -> usize {
            self.calls.load(AtomicOrdering::SeqCst)
        }
    }
    #[async_trait::async_trait]
    impl HttpTransport for FirstCallHangsMock {
        async fn get_json(
            &self,
            _path: &str,
            _query: &[(&str, &str)],
        ) -> Result<Value, TransportError> {
            panic!("public GET not used in this test");
        }
        async fn post_form_signed(
            &self,
            _path: &str,
            _body: &str,
            _api_key_header: &str,
            _api_sign_header: &str,
        ) -> Result<Value, TransportError> {
            let n = self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            if n == 0 {
                std::future::pending::<()>().await;
                unreachable!("first post_form_signed must be cancelled by the timeout");
            }
            Ok(serde_json::json!({ "error": [], "result": { "ok": true } }))
        }
    }

    /// Stuck signed send times out and releases the gate.
    #[tokio::test]
    async fn gate_released_when_signed_send_times_out() {
        let mock = FirstCallHangsMock::new();
        let rest = build_rest_with_transport_timeout(
            Arc::clone(&mock) as Arc<dyn HttpTransport>,
            std::time::Duration::from_millis(100),
        );

        let first = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            rest.signed_post_costed(
                "/0/private/Balance",
                Vec::new(),
                AuthProfile::SpotV1,
                RateLimitCost::None,
                RetryPolicy::never_retry(),
                "rid-test",
            ),
        )
        .await
        .expect(
            "first call must resolve within the outer bound — gate-scoped timeout did not fire",
        );
        match first {
            Err(RestError::Transport { transient, kind }) => {
                assert!(transient, "transport timeout must be transient");
                assert_eq!(
                    kind,
                    crate::transport::TransportErrorKind::RequestSentNoResponse,
                    "our own request timeout is sent-ambiguous"
                );
            }
            other => panic!("expected a transport timeout error, got {other:?}"),
        }

        let second = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            rest.signed_post_costed(
                "/0/private/Balance",
                Vec::new(),
                AuthProfile::SpotV1,
                RateLimitCost::None,
                RetryPolicy::never_retry(),
                "rid-test",
            ),
        )
        .await
        .expect("second call hung — the gate was NOT released on the first timeout (deadlock)");
        assert!(
            second.is_ok(),
            "second signed call must succeed after the gate released: {second:?}"
        );
        assert_eq!(
            mock.calls(),
            2,
            "both signed sends reached the transport — first (hung) + second (succeeded)"
        );
    }
}
