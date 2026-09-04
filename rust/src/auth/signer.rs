//! `AuthSigner` — Multiton per auth scheme; v1 has one impl,
//! `SpotRestHmacSha512Signer` (Kraken Spot REST). Byte ordering is a
//! silent-failure hazard — the golden-vector tests below pin the algorithm.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256, Sha512};

use crate::types::{ApiKey, ApiSecret, Nonce, SignedRequest};

type HmacSha512 = Hmac<Sha512>;

/// Pluggable auth signer. `AuthStack` keys a `HashMap<AuthProfile, Arc<dyn
/// AuthSigner>>`; v1 registers one entry (`SpotV1`). Signing is bounded and
/// non-blocking (~µs).
pub trait AuthSigner: Send + Sync {
    /// Sign `body` for `path`. The nonce is already embedded in `body` as a
    /// `nonce=` field and also passed separately, because the algorithm hashes
    /// the ASCII-decimal nonce first, before the body bytes.
    fn sign(&self, nonce: Nonce, path: &str, body: &str) -> Result<SignedRequest, AuthSignerError>;
}

/// HMAC-SHA512 signer for Kraken Spot REST private endpoints. Constructed once
/// per Client at `.build()`; holds the base64-decoded secret via `ApiSecret`,
/// zeroed on drop.
pub struct SpotRestHmacSha512Signer {
    api_key: ApiKey,
    secret: ApiSecret,
}

impl SpotRestHmacSha512Signer {
    /// Construct from a (key, base64-decoded-secret) pair.
    pub fn new(api_key: ApiKey, secret: ApiSecret) -> Self {
        Self { api_key, secret }
    }

    /// Pure signing algorithm (golden-vector tests).
    pub(crate) fn compute_api_sign(
        secret_bytes: &[u8],
        nonce: Nonce,
        path: &str,
        body: &str,
    ) -> Result<String, AuthSignerError> {
        // SHA256(nonce_as_ascii_decimal || body_bytes)
        let mut sha256 = Sha256::new();
        sha256.update(nonce.to_string().as_bytes());
        sha256.update(body.as_bytes());
        let sha256_payload = sha256.finalize();

        // HMAC-SHA512(secret_bytes, path_bytes || sha256_payload)
        let mut hmac = HmacSha512::new_from_slice(secret_bytes)
            .map_err(|_| AuthSignerError::InvalidSecretKeyLength)?;
        hmac.update(path.as_bytes());
        hmac.update(&sha256_payload);
        let signature = hmac.finalize().into_bytes();

        Ok(BASE64.encode(signature))
    }
}

impl AuthSigner for SpotRestHmacSha512Signer {
    fn sign(&self, nonce: Nonce, path: &str, body: &str) -> Result<SignedRequest, AuthSignerError> {
        let api_sign = Self::compute_api_sign(self.secret.as_bytes(), nonce, path, body)?;
        Ok(SignedRequest {
            api_key_header: self.api_key.as_str().to_string(),
            api_sign_header: api_sign,
            nonce,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthSignerError {
    /// HMAC-SHA512 accepts any key length; this only fires if the secret
    /// byte slice has length 0 (rejected earlier at `ApiSecret::from_base64`).
    #[error("Invalid HMAC key length.")]
    InvalidSecretKeyLength,
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;

    /// Golden vector: byte-ordering regression fails this test.
    #[test]
    fn hmac_signature_pins_canon_algorithm_against_fixed_inputs() {
        let secret_bytes = vec![0x01u8; 32];
        let nonce = Nonce::new(1_700_000_000_000_000_000);
        let path = "/0/private/Balance";
        let body = "nonce=1700000000000000000";

        let api_sign =
            SpotRestHmacSha512Signer::compute_api_sign(&secret_bytes, nonce, path, body).unwrap();

        assert_eq!(
            api_sign.len(),
            88,
            "API-Sign should be 88 base64 chars (64-byte HMAC-SHA512)"
        );

        let decoded = BASE64
            .decode(&api_sign)
            .expect("API-Sign should be valid base64");
        assert_eq!(decoded.len(), 64, "HMAC-SHA512 output is 64 bytes");

        // Cross-checked against a Python hashlib/hmac/base64 reference.
        let expected = "6i7+cpXVLDDnDOekgsRcgFQhkYFyPDSSxhSVA8OrA6vjziXcy2K1DP3n7f9B/Kgni71WjBSQusZKxduRsdF20Q==";
        assert_eq!(
            api_sign, expected,
            "API-Sign drifted from the Kraken signing algorithm. Steps must be:\n\
             1. SHA256(nonce_ascii ‖ body_bytes)\n\
             2. HMAC-SHA512(secret, path_bytes ‖ sha256_output)\n\
             3. base64_encode(hmac_output)\n\
             Got: {}\n\
             Expected: {}",
            api_sign, expected
        );
    }

    #[test]
    fn signature_changes_when_any_input_changes() {
        let secret = vec![0x01u8; 32];

        let base = SpotRestHmacSha512Signer::compute_api_sign(
            &secret,
            Nonce::new(1000),
            "/0/private/Balance",
            "nonce=1000",
        )
        .unwrap();

        let diff_nonce = SpotRestHmacSha512Signer::compute_api_sign(
            &secret,
            Nonce::new(1001),
            "/0/private/Balance",
            "nonce=1000",
        )
        .unwrap();
        assert_ne!(base, diff_nonce);

        let diff_path = SpotRestHmacSha512Signer::compute_api_sign(
            &secret,
            Nonce::new(1000),
            "/0/private/AddOrder",
            "nonce=1000",
        )
        .unwrap();
        assert_ne!(base, diff_path);

        let diff_body = SpotRestHmacSha512Signer::compute_api_sign(
            &secret,
            Nonce::new(1000),
            "/0/private/Balance",
            "nonce=1000&extra=x",
        )
        .unwrap();
        assert_ne!(base, diff_body);

        let diff_secret = SpotRestHmacSha512Signer::compute_api_sign(
            &[0x02u8; 32],
            Nonce::new(1000),
            "/0/private/Balance",
            "nonce=1000",
        )
        .unwrap();
        assert_ne!(base, diff_secret);
    }

    #[test]
    fn signed_request_carries_api_key_and_nonce() {
        let api_key = ApiKey::new("test-api-key-1234567890");
        let secret = ApiSecret::from_base64(&BASE64.encode(vec![0x01u8; 32])).unwrap();
        let signer = SpotRestHmacSha512Signer::new(api_key, secret);

        let result = signer
            .sign(Nonce::new(42), "/0/private/Balance", "nonce=42")
            .unwrap();

        assert_eq!(result.api_key_header, "test-api-key-1234567890");
        assert_eq!(result.nonce.as_u64(), 42);
        assert_eq!(result.api_sign_header.len(), 88);
    }
}
