//! `NonceSource` — monotonic-u64 nonce for Kraken REST + WS auth (strictly
//! increasing per key, else `EAPI:Invalid nonce`). Multi-signer scale:
//! docs/guides/error-handling.md.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::{ApiKey, Nonce};

/// Pluggable nonce source.
///
/// MUST be sync-non-blocking — atomic increment, no I/O.
pub trait NonceSource: Send + Sync {
    /// Allocate the next nonce for `key`, strictly greater than every prior
    /// return for the same key within this process.
    fn next_nonce(&self, key: &ApiKey) -> Nonce;
}

/// Process-local nonce source seeded from `Unix time × 10⁹` (full nanoseconds),
/// with an atomic floor so the value never regresses on wall-clock skew.
/// Separate processes sharing a key will collide.
pub struct SystemClockNonceSource {
    /// Monotonic floor, seeded lazily on the first call.
    counter: AtomicU64,
}

impl SystemClockNonceSource {
    /// Construct a fresh source; the atomic floor starts at 0 and is seeded
    /// from wall time on the first `next_nonce` call.
    pub fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    /// Wall-clock candidate: `Unix time × 10⁹` (nanoseconds since epoch).
    fn wall_candidate() -> u64 {
        // Saturate at 0 if the clock is before UNIX_EPOCH rather than panic.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        // Full-nanosecond scale; saturating cast (wraps ~year 2554).
        nanos.min(u128::from(u64::MAX)) as u64
    }
}

impl Default for SystemClockNonceSource {
    fn default() -> Self {
        Self::new()
    }
}

impl NonceSource for SystemClockNonceSource {
    fn next_nonce(&self, _key: &ApiKey) -> Nonce {
        // `_key` unused in v1; process-global monotonic value for any single key.
        let candidate = Self::wall_candidate();
        // CAS for max(previous+1, wall): `fetch_max` alone can't give max-then-+1
        // atomicity, so the nonce stays strictly increasing on clock skew.
        loop {
            let current = self.counter.load(Ordering::Acquire);
            let next = std::cmp::max(current.saturating_add(1), candidate);
            match self.counter.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Nonce(next),
                Err(_) => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_key() -> ApiKey {
        ApiKey::new("dummy-key-abc1234")
    }

    #[test]
    fn nonces_are_strictly_increasing() {
        let src = SystemClockNonceSource::new();
        let key = dummy_key();
        let n1 = src.next_nonce(&key);
        let n2 = src.next_nonce(&key);
        let n3 = src.next_nonce(&key);
        assert!(n2.as_u64() > n1.as_u64(), "n2={} must be > n1={}", n2, n1);
        assert!(n3.as_u64() > n2.as_u64(), "n3={} must be > n2={}", n3, n2);
    }

    #[test]
    fn nonces_under_rapid_sequential_calls_stay_unique_and_monotonic() {
        let src = SystemClockNonceSource::new();
        let key = dummy_key();
        let mut prev = 0u64;
        for _ in 0..1000 {
            let n = src.next_nonce(&key).as_u64();
            assert!(n > prev, "nonce regression: n={} prev={}", n, prev);
            prev = n;
        }
    }

    #[test]
    fn nonces_under_parallel_calls_remain_unique() {
        use std::sync::Arc;
        use std::thread;

        let src = Arc::new(SystemClockNonceSource::new());
        let key = Arc::new(dummy_key());
        let mut handles = vec![];

        for _ in 0..4 {
            let src = Arc::clone(&src);
            let key = Arc::clone(&key);
            handles.push(thread::spawn(move || {
                let mut out = Vec::with_capacity(256);
                for _ in 0..256 {
                    out.push(src.next_nonce(&key).as_u64());
                }
                out
            }));
        }

        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort_unstable();
        let original_len = all.len();
        all.dedup();
        assert_eq!(
            all.len(),
            original_len,
            "duplicate nonces detected under parallel load"
        );
    }

    #[test]
    fn nonce_is_full_nanosecond_scale() {
        let src = SystemClockNonceSource::new();
        let n = src.next_nonce(&dummy_key()).as_u64();
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos() as u64;
        assert!(
            n.abs_diff(now_nanos) < 1_000_000_000,
            "nonce {n} not within 1s of wall-clock nanos {now_nanos} — scale regressed (×10⁸?)"
        );
    }
}
