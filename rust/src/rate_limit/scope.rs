//! `Scope` keys the rate-limit counter map: api tracker by `ApiKey`, trading by `(ApiKey, Pair)`.

use crate::types::{ApiKey, Symbol};

/// Per-tracker scope discriminator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Scope {
    /// Non-trading REST: one counter per API key. Public REST bypasses the tracker.
    ApiKey(ApiKey),
    /// Trading REST + Spot WS v2: one counter per `(api_key, pair)`, shared at the exchange engine.
    Pair(ApiKey, Symbol),
}

impl Scope {
    /// Non-reversible identity for event payloads: key fingerprint (NEVER the raw key) + pair.
    pub(crate) fn redacted_id(&self) -> (String, Option<crate::types::Symbol>) {
        match self {
            Scope::ApiKey(k) => (crate::auth::derive_key_fingerprint(k), None),
            Scope::Pair(k, s) => (crate::auth::derive_key_fingerprint(k), Some(s.clone())),
        }
    }
}
