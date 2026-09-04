//! Credential / id newtypes and auth-scheme discriminators.

use serde::Serialize;
/// Monotonic nonce used in Spot REST and Spot WS auth signatures. Unsigned
/// 64-bit integer, strictly increasing per API key, encoded as ASCII decimal
/// on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Nonce(pub u64);

impl Nonce {
    /// Wrap a raw nonce value. No monotonicity check here — strict per-key
    /// ordering is the nonce generator's responsibility.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw u64 value, as encoded (ASCII decimal) in the signed request body.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for Nonce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Public API key string, sent in the `API-Key` HTTP header on signed REST
/// requests. Debug redacts to at most the last 4 chars (never the prefix); keys
/// of 8 chars or fewer print fully redacted.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ApiKey(String);

impl ApiKey {
    /// Wrap the raw key string verbatim — no format validation; a bad key
    /// surfaces as an auth error on the first signed request.
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// Wire-form string for the `API-Key` header value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = &self.0;
        // Count CHARS not bytes: ≤8 chars fully redacted; longer shows last 4
        // without splitting a multi-byte char.
        let mut tail = s.char_indices().rev();
        match tail.nth(3) {
            Some((i, _)) if tail.nth(4).is_some() => write!(f, "ApiKey(…{})", &s[i..]),
            _ => write!(f, "ApiKey(<redacted>)"),
        }
    }
}

/// Base64-decoded API secret, held only as raw bytes. No `Debug`/`Display`/
/// `Clone` impls. `Zeroize` is deliberately NOT derived (only `ZeroizeOnDrop`
/// is): a public `.zeroize()` that empties a live secret in place is a footgun.
#[derive(zeroize::ZeroizeOnDrop)]
pub struct ApiSecret {
    bytes: Vec<u8>,
}

impl ApiSecret {
    /// Decode a base64-encoded secret into raw bytes. The decoded bytes are
    /// used as the HMAC-SHA512 key for request signing.
    pub fn from_base64(b64: &str) -> Result<Self, ApiSecretError> {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;
        let bytes = STANDARD
            .decode(b64)
            // Strip base64::DecodeError's own terminal period so wrapping it in the
            // InvalidBase64 template ("...: {0}.") does not produce a doubled period.
            .map_err(|e| {
                ApiSecretError::InvalidBase64(e.to_string().trim_end_matches('.').to_string())
            })?;
        if bytes.is_empty() {
            return Err(ApiSecretError::Empty);
        }
        Ok(Self { bytes })
    }

    /// Borrow the raw bytes (for HMAC-SHA512 key input).
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Errors from constructing an [`ApiSecret`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ApiSecretError {
    /// Input was not valid standard-alphabet base64; payload is the decode detail.
    #[error("Invalid base64: {0}.")]
    InvalidBase64(String),
    /// Input decoded to zero bytes — unusable as an HMAC key.
    #[error("API secret is empty.")]
    Empty,
}

/// Account 2FA one-time password, folded into the HMAC'd body of every signed
/// REST form. Held verbatim, with no `Debug`/`Display`/`Clone` impls (cannot be
/// printed or duplicated); `Drop` zeroes the buffer before deallocation
/// (derived `ZeroizeOnDrop` — a newly added secret field cannot be forgotten).
/// `Zeroize` itself is deliberately NOT derived: a public `.zeroize()` that
/// empties a live secret in place is a footgun.
#[derive(zeroize::ZeroizeOnDrop)]
pub struct Otp {
    value: String,
}

impl Otp {
    /// Wrap the password verbatim — no format validation; a bad otp surfaces as an
    /// auth error on the first signed request. An owned `String` is moved, never
    /// re-allocated, so no unzeroized copy is left outside this newtype.
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    /// Borrow the password for the signed form body.
    pub(crate) fn as_str(&self) -> &str {
        &self.value
    }
}

/// Kraken transaction id, e.g. `"OBE25Z-GZRP2-6YWYZI"`. Opaque wire token
/// returned in `AddOrderResponse::txid`; open/closed order maps key on the same
/// token as a plain `String`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct TxId(String);

impl TxId {
    /// Wrap a txid string verbatim — opaque wire token, no format validation.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// The txid exactly as Kraken sent it, for use as a map key or request param.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for TxId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Transparent deserialize — opaque wire token arriving as a JSON string.
impl<'de> serde::Deserialize<'de> for TxId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self(s))
    }
}

/// Client order id. Accepted formats: 36-char UUID v4 (SDK-allocated default),
/// 32-char hex (no dashes), or up to 18 ASCII alphanumerics plus `_`/`-`.
/// Allocate via [`ClOrdId::allocate_v4`]; validate caller input via [`ClOrdId::new`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ClOrdId(String);

impl ClOrdId {
    /// Allocate a fresh UUID v4 as the client order id.
    pub fn allocate_v4() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Construct from a caller-supplied string with format validation.
    pub fn new(s: impl Into<String>) -> Result<Self, ClOrdIdError> {
        let s = s.into();
        if s.is_empty() {
            return Err(ClOrdIdError::Empty);
        }
        if s.len() == 36 && uuid::Uuid::parse_str(&s).is_ok() {
            return Ok(Self(s));
        }
        if s.len() == 32 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(Self(s));
        }
        if s.len() <= 18
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Ok(Self(s));
        }
        Err(ClOrdIdError::InvalidFormat)
    }

    /// Wire-form string, sent as `cl_ord_id` on order requests and echoed back
    /// in responses.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ClOrdId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Upper bound on a server-echoed `cl_ord_id` (bytes). Lenient vs
/// [`ClOrdId::new`]'s strict caller-input formats, but bounded so a
/// hostile/buggy response cannot inject an unbounded id.
const MAX_WIRE_CL_ORD_ID_LEN: usize = 128;

/// Permissive deserialize: server-echoed `cl_ord_id` is not re-validated through
/// [`ClOrdId::new`]'s caller-input rules (Kraken may echo ids outside the SDK's
/// set). Charset unvalidated, but empty and over-128-byte ids are rejected.
impl<'de> serde::Deserialize<'de> for ClOrdId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.is_empty() {
            return Err(serde::de::Error::custom("cl_ord_id is empty"));
        }
        if s.len() > MAX_WIRE_CL_ORD_ID_LEN {
            return Err(serde::de::Error::custom(format!(
                "cl_ord_id exceeds {MAX_WIRE_CL_ORD_ID_LEN} bytes (got {})",
                s.len()
            )));
        }
        Ok(Self(s))
    }
}

/// Errors from validating a caller-supplied id in [`ClOrdId::new`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ClOrdIdError {
    /// Input string was empty.
    #[error("Client order ID is empty.")]
    Empty,
    /// Input matched none of the accepted formats (UUID v4 / 32-char hex / ≤18-char ASCII subset).
    #[error(
        "Client order ID does not match any accepted format (UUID v4 / 32-char hex / ≤18-char ASCII subset)."
    )]
    InvalidFormat,
}

/// Auth scheme discriminator — the key in the `AuthStack` signer multiton.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthProfile {
    /// Public REST + WS — no signing required.
    Public,
    /// Spot REST private — HMAC-SHA512.
    SpotV1,
    /// Spot WS v2 — token-based authentication.
    // Reserved profiles: constructed once the WS-token / L3 / Futures signers land.
    #[allow(dead_code)]
    SpotWsV2,
    /// L3 WS — uses the same token as Spot WS v2.
    #[allow(dead_code)]
    L3WsV2,
    /// Futures REST — HMAC-SHA256 (reserved for v2).
    #[allow(dead_code)]
    FuturesV3,
}

/// Output of `AuthSigner::sign`. Carries the HTTP header values
/// to inject and the nonce that was used.
#[derive(Clone)]
pub struct SignedRequest {
    /// Value for the `API-Key` HTTP header.
    pub api_key_header: String,
    /// Value for the `API-Sign` HTTP header. Base64-encoded HMAC-SHA512.
    pub api_sign_header: String,
    /// The nonce embedded in the request body. Echoed here for caller
    /// audit / correlation.
    // Read only through the test-support harness surface.
    #[allow(dead_code)]
    pub nonce: Nonce,
}

impl std::fmt::Debug for SignedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignedRequest")
            .field("api_key_header", &"<redacted>")
            .field("api_sign_header", &"<redacted>")
            .field("nonce", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_request_debug_redacts_key_sign_and_nonce() {
        let req = SignedRequest {
            api_key_header: "RAW-API-KEY-must-not-leak".to_string(),
            api_sign_header: "RAW-HMAC-SIGN-must-not-leak".to_string(),
            nonce: Nonce(1717171717),
        };
        let dbg = format!("{req:?}");
        assert!(dbg.contains("<redacted>"), "expected redaction: {dbg}");
        assert!(
            !dbg.contains("RAW-API-KEY-must-not-leak"),
            "raw API key leaked in Debug: {dbg}"
        );
        assert!(
            !dbg.contains("RAW-HMAC-SIGN-must-not-leak"),
            "raw signature leaked in Debug: {dbg}"
        );
        assert!(!dbg.contains("1717171717"), "nonce leaked in Debug: {dbg}");
    }

    #[test]
    fn api_key_debug_redacts_prefix_and_short_keys() {
        let k = ApiKey::new("ABCDEFGHIJKLMNOP");
        let dbg = format!("{k:?}");
        assert_eq!(dbg, "ApiKey(…MNOP)");
        assert!(!dbg.contains("ABCD"), "must never reveal the prefix");
        assert_eq!(
            format!("{:?}", ApiKey::new("SHORTKEY")),
            "ApiKey(<redacted>)"
        );
        assert_eq!(format!("{:?}", ApiKey::new("abc")), "ApiKey(<redacted>)");
    }

    #[test]
    fn api_key_debug_is_char_boundary_safe() {
        // A multi-byte char spanning the 4-bytes-from-the-end boundary must not
        // panic; the suffix is the last 4 characters.
        assert_eq!(
            format!("{:?}", ApiKey::new("AAAAAAAAAA日本語")),
            "ApiKey(…A日本語)"
        );
        // ≤8 chars (any byte length) is fully redacted.
        assert_eq!(format!("{:?}", ApiKey::new("🔑🔑🔑")), "ApiKey(<redacted>)");
        assert_eq!(
            format!("{:?}", ApiKey::new("🔑🔑🔑🔑")),
            "ApiKey(<redacted>)"
        );
        assert_eq!(
            format!("{:?}", ApiKey::new("🔑🔑🔑🔑🔑🔑🔑🔑")),
            "ApiKey(<redacted>)"
        );
    }

    #[test]
    fn api_secret_wipe_on_drop_is_pinned() {
        // Compile-time pin: removing ZeroizeOnDrop fails this at build.
        fn assert_wipes_on_drop<T: zeroize::ZeroizeOnDrop>() {}
        assert_wipes_on_drop::<ApiSecret>();
    }

    #[test]
    fn otp_wipe_on_drop_is_pinned() {
        // Compile-time pin: removing ZeroizeOnDrop fails this at build.
        fn assert_wipes_on_drop<T: zeroize::ZeroizeOnDrop>() {}
        assert_wipes_on_drop::<Otp>();
    }

    #[test]
    fn cl_ord_id_deserialize_accepts_valid_and_lenient_charset() {
        let id: ClOrdId = serde_json::from_str("\"550e8400-e29b-41d4-a716-446655440000\"").unwrap();
        assert_eq!(id.as_str(), "550e8400-e29b-41d4-a716-446655440000");
        let lenient: ClOrdId = serde_json::from_str("\"kraken/internal:id#42\"").unwrap();
        assert_eq!(lenient.as_str(), "kraken/internal:id#42");
        let max = "a".repeat(MAX_WIRE_CL_ORD_ID_LEN);
        let at_cap: ClOrdId = serde_json::from_str(&format!("\"{max}\"")).unwrap();
        assert_eq!(at_cap.as_str().len(), MAX_WIRE_CL_ORD_ID_LEN);
    }

    #[test]
    fn cl_ord_id_deserialize_rejects_empty_and_oversized() {
        assert!(serde_json::from_str::<ClOrdId>("\"\"").is_err());
        let too_long = "a".repeat(MAX_WIRE_CL_ORD_ID_LEN + 1);
        assert!(serde_json::from_str::<ClOrdId>(&format!("\"{too_long}\"")).is_err());
    }

    #[test]
    fn invalid_base64_secret_has_single_terminal_period() {
        // `match` not `unwrap_err`: `ApiSecret` has no `Debug` (credential safety).
        let msg = match ApiSecret::from_base64("!") {
            Ok(_) => panic!("expected an error for invalid base64 input"),
            Err(e) => e.to_string(),
        };
        assert!(!msg.ends_with(".."), "double terminal period: {msg:?}");
        assert!(msg.ends_with('.'), "missing terminal period: {msg:?}");
        assert!(
            msg.starts_with("Invalid base64: "),
            "unexpected prefix: {msg:?}"
        );
    }
}
