//! Cross-cutting auth: nonce source, per-scheme signer multiton, and Spot WS v2
//! token lifecycle. [`AuthStack::sign_form`] is the canonical entry (nonce +
//! encode + HMAC as one op).

pub mod nonce;
pub mod signer;
pub mod token_lifecycle;

pub use nonce::{NonceSource, SystemClockNonceSource};
pub use signer::{AuthSigner, SpotRestHmacSha512Signer};
pub(crate) use token_lifecycle::derive_key_fingerprint;
pub use token_lifecycle::{
    RefreshHandle, RefreshOutcome, RefreshReason, TokenLifecycleManager, WsToken,
};

use std::collections::HashMap;
use std::sync::Arc;

use crate::types::{ApiKey, AuthProfile, Otp};

/// Composes the nonce source, signer multiton, and token lifecycle.
/// [`AuthStack::sign_form`] is the canonical entry; without credentials a call
/// surfaces [`AuthError::Unknown`] (`INTERNAL`), never panics.
pub struct AuthStack {
    api_key: Option<ApiKey>,
    /// 2FA one-time password. When present, every signed form gains an `otp` field
    /// inside the HMAC'd body (Kraken's REST 2FA contract).
    otp: Option<Otp>,
    nonce_source: Arc<dyn NonceSource>,
    signers: HashMap<AuthProfile, Arc<dyn AuthSigner>>,
    /// Spot WS v2 token cache. Lazy; `set_rest` is late-bound to break the
    /// `AuthStack ↔ RestSurface` ownership cycle.
    token_lifecycle: TokenLifecycleManager,
}

impl AuthStack {
    /// Construct from optional credentials (API key, 2FA otp), nonce source, signer
    /// multiton, and token lifecycle (its `RestSurface` ref is late-bound).
    pub fn new(
        api_key: Option<ApiKey>,
        otp: Option<Otp>,
        nonce_source: Arc<dyn NonceSource>,
        signers: HashMap<AuthProfile, Arc<dyn AuthSigner>>,
        token_lifecycle: TokenLifecycleManager,
    ) -> Self {
        Self {
            api_key,
            otp,
            nonce_source,
            signers,
            token_lifecycle,
        }
    }

    /// Allocate a nonce, embed it, URL-encode the form, and HMAC-SHA512-sign
    /// as one op — a retry re-entry gets a fresh nonce. Non-blocking (~µs).
    pub fn sign_form(
        &self,
        profile: AuthProfile,
        path: &str,
        fields: Vec<(String, String)>,
    ) -> Result<SignedForm, AuthError> {
        // Creds are guaranteed by construction; a miss is an SDK invariant.
        let api_key = self.api_key.as_ref().ok_or_else(|| AuthError::Unknown {
            kraken_code: "INTERNAL".into(),
            kraken_message: "sign attempted without configured credentials".into(),
        })?;
        let signer = self
            .signers
            .get(&profile)
            .ok_or_else(|| AuthError::Unknown {
                kraken_code: "INTERNAL".into(),
                kraken_message: format!("no signer registered for {profile:?}"),
            })?;

        let nonce = self.nonce_source.next_nonce(api_key);
        let nonce_value = nonce.to_string();

        // Borrowed, not copied: the otp lands only in the `Otp` and the signed body,
        // both of which zeroize on drop.
        let mut all_fields: Vec<(&str, &str)> = fields
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        // Caller MUST NOT pre-include a `nonce` field.
        all_fields.push(("nonce", &nonce_value));
        // Step 2b — 2FA: the otp rides inside the signed body so the HMAC covers it.
        // Fill-if-absent: a caller's own `otp` in `fields` wins and is never doubled.
        if !all_fields.iter().any(|(key, _)| *key == "otp") {
            if let Some(otp) = self.otp.as_ref() {
                all_fields.push(("otp", otp.as_str()));
            }
        }

        let body = encode_form(&all_fields);

        let signed = signer
            .sign(nonce, path, &body)
            .map_err(|e| AuthError::Unknown {
                kraken_code: "INTERNAL".into(),
                kraken_message: format!("signing failed: {e}"),
            })?;

        Ok(SignedForm {
            body,
            api_key_header: signed.api_key_header,
            api_sign_header: signed.api_sign_header,
        })
    }

    /// Configured API key, if any (for rate-limit `Scope::ApiKey`).
    pub fn api_key(&self) -> Option<&ApiKey> {
        self.api_key.as_ref()
    }

    /// Cached Spot WS v2 token. `None` until the first fetch.
    pub fn cached_token(&self) -> Option<std::sync::Arc<WsToken>> {
        self.token_lifecycle.current_token()
    }

    /// Drop the cached token. A server-rejected token can still read as valid
    /// locally (`is_expired` is time-only), so invalidate before refresh.
    pub fn invalidate_cached_token(&self) {
        self.token_lifecycle.invalidate_cached_token()
    }

    /// Force a reactive token refresh; returns a [`RefreshHandle`]. Outcome
    /// arrives on the reactor bridge, correlated by `handle.id`.
    pub fn force_refresh(&self, reason: RefreshReason) -> RefreshHandle {
        self.token_lifecycle.reactive_refresh(reason)
    }

    /// Proactive WS-token refresh at TTL × 0.5. Single-flight-guarded; outcome
    /// rides the reactor bridge (request_id 0). Sessions never re-auth.
    pub fn proactive_refresh(&self) {
        self.token_lifecycle.proactive_refresh()
    }

    /// Composed [`TokenLifecycleManager`] (late-bind `set_rest`, proactive tick).
    pub fn token_lifecycle(&self) -> &TokenLifecycleManager {
        &self.token_lifecycle
    }
}

/// Output of [`AuthStack::sign_form`]: encoded body (nonce embedded) plus
/// `API-Key` / `API-Sign` headers for the REST layer. `Drop` zeroes the body —
/// with 2FA configured it carries the `otp`.
#[derive(Clone, PartialEq, Eq)]
pub struct SignedForm {
    /// URL-encoded form body, including the `nonce=<value>` field.
    pub body: String,
    pub api_key_header: String,
    /// Base64-encoded HMAC-SHA512 for the `API-Sign` header.
    pub api_sign_header: String,
}

impl Drop for SignedForm {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.body.zeroize();
    }
}

// Hand-written Debug: redact key, signature, and nonce-bearing body.
impl std::fmt::Debug for SignedForm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignedForm")
            .field("body", &"<redacted>")
            .field("api_key_header", &"<redacted>")
            .field("api_sign_header", &"<redacted>")
            .finish()
    }
}

/// Form-encode (key, value) pairs into `application/x-www-form-urlencoded`, in ONE
/// pre-sized allocation: per-field temporaries and a mid-build growth realloc would
/// each abandon an unzeroized copy of the body, which carries the 2FA `otp`.
fn encode_form(fields: &[(&str, &str)]) -> String {
    // `=` after each key, plus `&` between fields (one spare byte for the last).
    const SEPARATORS_PER_FIELD: usize = 2;
    let capacity = fields
        .iter()
        .map(|(key, value)| encoded_len(key) + encoded_len(value) + SEPARATORS_PER_FIELD)
        .sum();

    let mut out = String::with_capacity(capacity);
    for (i, (key, value)) in fields.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        encode_into(&mut out, key);
        out.push('=');
        encode_into(&mut out, value);
    }
    debug_assert!(
        out.len() <= capacity,
        "encoded_len MUST cover every byte written: {} > {capacity}",
        out.len()
    );
    out
}

/// Append `s` to `out`, URL-encoded for a form field (RFC 3986 unreserved + form
/// rules). Kraken body values are ASCII: alphanumerics + `-_.~` pass; else `%XX`.
/// Appends rather than returning, so a credential never lands in a temporary.
fn encode_into(out: &mut String, s: &str) {
    for b in s.bytes() {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            // One escape per raw byte, so multi-byte UTF-8 becomes several. Both
            // nibbles are 4-bit, so neither index can be out of bounds.
            out.push('%');
            out.push(HEX_UPPER[usize::from(b >> 4)] as char);
            out.push(HEX_UPPER[usize::from(b & 0x0F)] as char);
        }
    }
}

/// Encoded byte length of `s` under [`encode_into`]: 1 byte per unreserved byte,
/// 3 per escaped one.
fn encoded_len(s: &str) -> usize {
    s.bytes()
        .map(|b| if is_unreserved(b) { 1 } else { 3 })
        .sum()
}

/// RFC 3986 unreserved set, the bytes that pass through unescaped.
fn is_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~')
}

/// Percent-escape hex digits, indexed by nibble.
const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

/// Errors from auth-stack (signing / token-lifecycle) operations. Closed set.
/// Sign-time miss (no cred, no signer, compute failure) → [`AuthError::Unknown`] (`INTERNAL`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum AuthError {
    /// Kraken rejected the API key (`EAPI:Invalid key`) — bad or revoked; not retryable.
    #[error("Invalid API key.")]
    InvalidKey,
    /// Kraken rejected the signature (`EAPI:Invalid signature`) — wrong secret or body/path drift.
    #[error("Invalid signature.")]
    InvalidSignature,
    /// Kraken rejected the nonce (`EAPI:Invalid nonce`) — not strictly increasing for this key.
    #[error("Invalid nonce.")]
    InvalidNonce,
    /// Kraken `Permission denied` family — key lacks the required permission; not retryable.
    #[error("Permission denied.")]
    PermissionDenied {
        /// Denied scope; Kraken's string does not identify one today, so always `None`.
        scope: Option<String>,
    },
    /// `EGeneral:Temporary lockout` after too many sequential `EAPI:Invalid key`
    /// failures (~15-min cooldown). Not auto-retried; caller waits it out.
    #[error(
        "Temporary lockout; too many sequential auth failures — retry after the cooldown (~15 min)."
    )]
    TemporaryLockout,
    /// Cached Spot WS v2 token is no longer valid; refresh required first (retryable).
    #[error("Token stale; refresh required.")]
    TokenStale,
    /// Spot WS v2 token refresh failed terminally; not retryable.
    #[error("Token refresh failed.")]
    TokenRefreshFailed,
    /// Transient token-refresh failure; retryable, categorised as network rather than auth.
    #[error("Transient network failure during token refresh; retrying.")]
    TokenRefreshTransient,
    /// Client closed or dropped mid-operation; no new auth ops accepted.
    #[error("Client is closed; no new operations accepted.")]
    ClientClosed,
    /// Unmapped auth failure — Kraken's raw code(s), or `INTERNAL` for SDK invariant misses.
    #[error("{kraken_message}")]
    Unknown {
        /// Raw Kraken error code(s), comma-joined, or `INTERNAL` for SDK invariant misses.
        kraken_code: String,
        /// Human-readable failure description; doubles as this error's `Display` text.
        kraken_message: String,
    },
}

impl crate::error::sealed::Sealed for AuthError {}

impl crate::error::ApiError for AuthError {
    fn code(&self) -> &str {
        use AuthError::*;
        match self {
            InvalidKey => "INVALID_KEY",
            InvalidSignature => "INVALID_SIGNATURE",
            InvalidNonce => "INVALID_NONCE",
            PermissionDenied { .. } => "PERMISSION_DENIED",
            TemporaryLockout => "TEMPORARY_LOCKOUT",
            TokenStale => "AUTH_REFRESH_FAILED",
            TokenRefreshFailed => "AUTH_REFRESH_FAILED",
            TokenRefreshTransient => "AUTH_REFRESH_TRANSIENT",
            ClientClosed => "CLIENT_CLOSED",
            Unknown { .. } => "UNKNOWN",
        }
    }
    fn category(&self) -> crate::error::ErrorCategory {
        use crate::error::ErrorCategory;
        use AuthError::*;
        match self {
            InvalidKey
            | InvalidSignature
            | InvalidNonce
            | PermissionDenied { .. }
            | TemporaryLockout
            | TokenStale => ErrorCategory::Auth,
            TokenRefreshFailed | TokenRefreshTransient => ErrorCategory::Network,
            ClientClosed => ErrorCategory::Client,
            Unknown { .. } => ErrorCategory::Exchange,
        }
    }
    fn retryable(&self) -> bool {
        matches!(
            self,
            AuthError::TokenStale | AuthError::TokenRefreshTransient
        )
    }
    crate::error::api_error_tail!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ApiSecret;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;

    fn make_stack_with_creds() -> AuthStack {
        make_stack_with_otp(None)
    }

    fn make_stack_with_otp(otp: Option<Otp>) -> AuthStack {
        let api_key = ApiKey::new("test-api-key-1234567890");
        let secret = ApiSecret::from_base64(&BASE64.encode(vec![0x01u8; 32])).unwrap();
        let signer = SpotRestHmacSha512Signer::new(api_key.clone(), secret);
        let mut signers: HashMap<AuthProfile, Arc<dyn AuthSigner>> = HashMap::new();
        signers.insert(AuthProfile::SpotV1, Arc::new(signer));
        AuthStack::new(
            Some(api_key),
            otp,
            Arc::new(SystemClockNonceSource::new()),
            signers,
            TokenLifecycleManager::for_test(),
        )
    }

    #[test]
    fn sign_form_without_credentials_is_internal_invariant() {
        let stack = AuthStack::new(
            None,
            None,
            Arc::new(SystemClockNonceSource::new()),
            HashMap::new(),
            TokenLifecycleManager::for_test(),
        );
        let err = stack
            .sign_form(AuthProfile::SpotV1, "/0/private/Balance", Vec::new())
            .unwrap_err();
        assert!(matches!(
            err,
            AuthError::Unknown { ref kraken_code, .. } if kraken_code == "INTERNAL"
        ));
    }

    #[test]
    fn signed_form_debug_redacts_key_sign_and_body() {
        let form = SignedForm {
            body: "nonce=1717171717&pair=BTC/USD&volume=0.001".to_string(),
            api_key_header: "RAW-API-KEY-must-not-leak".to_string(),
            api_sign_header: "RAW-HMAC-SIGN-must-not-leak".to_string(),
        };
        let dbg = format!("{form:?}");
        assert!(dbg.contains("<redacted>"), "expected redaction: {dbg}");
        assert!(
            !dbg.contains("RAW-API-KEY-must-not-leak"),
            "raw API key leaked in Debug: {dbg}"
        );
        assert!(
            !dbg.contains("RAW-HMAC-SIGN-must-not-leak"),
            "raw signature leaked in Debug: {dbg}"
        );
        assert!(
            !dbg.contains("1717171717"),
            "nonce (via body) leaked in Debug: {dbg}"
        );
        assert!(!dbg.contains("BTC/USD"), "body leaked in Debug: {dbg}");
    }

    #[test]
    fn sign_form_without_registered_profile_is_internal_invariant() {
        let api_key = ApiKey::new("test");
        let stack = AuthStack::new(
            Some(api_key),
            None,
            Arc::new(SystemClockNonceSource::new()),
            HashMap::new(),
            TokenLifecycleManager::for_test(),
        );
        let err = stack
            .sign_form(AuthProfile::SpotV1, "/0/private/Balance", Vec::new())
            .unwrap_err();
        assert!(matches!(
            err,
            AuthError::Unknown { ref kraken_code, .. } if kraken_code == "INTERNAL"
        ));
    }

    #[test]
    fn sign_form_embeds_nonce_and_produces_signed_headers() {
        let stack = make_stack_with_creds();
        let signed = stack
            .sign_form(AuthProfile::SpotV1, "/0/private/Balance", Vec::new())
            .expect("signing should succeed");

        assert_eq!(signed.api_key_header, "test-api-key-1234567890");
        assert_eq!(signed.api_sign_header.len(), 88); // HMAC-SHA512 base64
        assert!(
            signed.body.starts_with("nonce="),
            "body should embed nonce — got {}",
            signed.body
        );
    }

    #[test]
    fn sign_form_encodes_caller_fields_alongside_nonce() {
        let stack = make_stack_with_creds();
        let fields = vec![("pair".to_string(), "BTC/USD".to_string())];
        let signed = stack
            .sign_form(AuthProfile::SpotV1, "/0/private/AddOrder", fields)
            .expect("signing should succeed");

        assert!(signed.body.starts_with("pair=BTC%2FUSD&nonce="));
    }

    #[test]
    fn sign_form_emits_fresh_nonce_per_call() {
        let stack = make_stack_with_creds();
        let a = stack
            .sign_form(AuthProfile::SpotV1, "/0/private/Balance", Vec::new())
            .unwrap();
        let b = stack
            .sign_form(AuthProfile::SpotV1, "/0/private/Balance", Vec::new())
            .unwrap();
        assert_ne!(
            a.body, b.body,
            "consecutive calls must produce different bodies"
        );
        assert_ne!(
            a.api_sign_header, b.api_sign_header,
            "consecutive calls must produce different signatures"
        );
    }

    #[test]
    fn has_credentials_reflects_construction() {
        let with = make_stack_with_creds();
        assert!(with.api_key().is_some());
        let without = AuthStack::new(
            None,
            None,
            Arc::new(SystemClockNonceSource::new()),
            HashMap::new(),
            TokenLifecycleManager::for_test(),
        );
        assert!(without.api_key().is_none());
    }

    fn urlencode(s: &str) -> String {
        let mut out = String::new();
        encode_into(&mut out, s);
        out
    }

    #[test]
    fn urlencode_passes_through_unreserved() {
        assert_eq!(urlencode("BTC-USD"), "BTC-USD");
        assert_eq!(urlencode("abc.123_xyz~"), "abc.123_xyz~");
    }

    #[test]
    fn urlencode_percent_encodes_reserved() {
        assert_eq!(urlencode("BTC/USD"), "BTC%2FUSD");
        assert_eq!(urlencode("a&b=c"), "a%26b%3Dc");
        assert_eq!(urlencode("hello world"), "hello%20world");
    }

    /// An under-count would grow the body mid-build and abandon an unzeroized copy.
    /// Multi-byte input is the case a char-based count gets wrong.
    #[test]
    fn encoded_len_matches_what_encode_into_writes() {
        for input in [
            "BTC-USD",
            "BTC/USD",
            "a&b=c",
            "hello world",
            "üñî",
            "🔑",
            "",
        ] {
            assert_eq!(
                encoded_len(input),
                urlencode(input).len(),
                "length disagreement on {input:?}"
            );
        }
    }

    #[test]
    fn token_refresh_transient_is_retryable_network_error() {
        use crate::error::{ApiError, ErrorCategory};
        let e = AuthError::TokenRefreshTransient;
        assert!(e.retryable(), "TokenRefreshTransient must be retryable");
        assert_eq!(e.category(), ErrorCategory::Network);
        assert_eq!(e.code(), "AUTH_REFRESH_TRANSIENT");
    }

    #[test]
    fn token_refresh_failed_is_terminal_network_error() {
        use crate::error::{ApiError, ErrorCategory};
        let e = AuthError::TokenRefreshFailed;
        assert!(
            !e.retryable(),
            "TokenRefreshFailed remains terminal (non-retryable)"
        );
        assert_eq!(e.category(), ErrorCategory::Network);
        assert_eq!(e.code(), "AUTH_REFRESH_FAILED");
    }

    #[test]
    fn encode_form_joins_with_ampersands() {
        let fields = [("nonce", "1000"), ("pair", "BTC/USD")];
        assert_eq!(encode_form(&fields), "nonce=1000&pair=BTC%2FUSD");
    }

    #[test]
    fn sign_form_appends_configured_otp_to_the_signed_body() {
        // A 2FA key must carry the otp inside the HMAC'd body. Same seam covers WS:
        // the token is minted by the signed `GetWebSocketsToken` call.
        let stack = make_stack_with_otp(Some(Otp::new("static-2fa-password")));
        let signed = stack
            .sign_form(AuthProfile::SpotV1, "/0/private/Balance", Vec::new())
            .expect("signing should succeed");
        assert!(
            signed.body.contains("otp=static-2fa-password"),
            "signed body must carry the otp — got {}",
            signed.body
        );
    }

    #[test]
    fn sign_form_omits_otp_when_none_is_configured() {
        // Default (no 2FA): the body must not carry a spurious `otp` field.
        let stack = make_stack_with_creds();
        let signed = stack
            .sign_form(AuthProfile::SpotV1, "/0/private/Balance", Vec::new())
            .unwrap();
        assert!(
            !signed.body.contains("otp="),
            "unexpected otp in body: {}",
            signed.body
        );
    }

    #[test]
    fn sign_form_does_not_duplicate_a_caller_supplied_otp() {
        // Fill-if-absent: an `otp` the caller already put in `fields` wins and the
        // configured value never doubles it.
        let stack = make_stack_with_otp(Some(Otp::new("configured")));
        let fields = vec![("otp".to_string(), "caller".to_string())];
        let signed = stack
            .sign_form(AuthProfile::SpotV1, "/0/private/Balance", fields)
            .unwrap();
        assert_eq!(
            signed.body.matches("otp=").count(),
            1,
            "otp field doubled: {}",
            signed.body
        );
        assert!(
            signed.body.contains("otp=caller") && !signed.body.contains("configured"),
            "caller-supplied otp must win: {}",
            signed.body
        );
    }
}
