//! `JitterSource` — uniform `[0,1)` source for reconnect-backoff full-jitter.

use std::sync::atomic::{AtomicU64, Ordering};

/// Uniform `[0.0, 1.0)` source for reconnect-backoff full-jitter.
pub trait JitterSource: Send + Sync {
    /// Draw the next uniform value in `[0.0, 1.0)`.
    fn next_unit(&self) -> f64;
}

/// Production jitter — lock-free SplitMix64 over an `AtomicU64`. Not cryptographic
/// (kept hand-rolled: needs `Send`/`Sync` shared source; `rand::ThreadRng` is not).
pub struct SplitMix64Jitter {
    state: AtomicU64,
}

impl SplitMix64Jitter {
    /// SplitMix64 increment (golden-ratio fractional constant).
    const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

    /// Construct with an explicit seed for a reproducible but non-fixed sequence.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            state: AtomicU64::new(seed),
        }
    }

    /// Seed from OS entropy via a fresh UUID v4.
    pub fn from_os() -> Self {
        let bytes = uuid::Uuid::new_v4().into_bytes();
        let lo = u64::from_le_bytes(bytes[0..8].try_into().expect("16-byte uuid"));
        let hi = u64::from_le_bytes(bytes[8..16].try_into().expect("16-byte uuid"));
        // Mix hi through gamma before combining — plain `lo ^ hi` halves entropy
        // and degenerates to seed 0 when lo == hi.
        Self::with_seed(lo.wrapping_add(hi.wrapping_mul(Self::GAMMA)))
    }

    /// One SplitMix64 step on a captured pre-increment state value.
    fn mix(mut z: u64) -> u64 {
        z = z.wrapping_add(Self::GAMMA);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

impl Default for SplitMix64Jitter {
    fn default() -> Self {
        Self::from_os()
    }
}

impl JitterSource for SplitMix64Jitter {
    fn next_unit(&self) -> f64 {
        // fetch_add returns the PREVIOUS state, which we finalize.
        let prev = self.state.fetch_add(Self::GAMMA, Ordering::Relaxed);
        let bits = Self::mix(prev);
        // Top 53 bits → f64 mantissa → uniform [0.0, 1.0).
        ((bits >> 11) as f64) / ((1u64 << 53) as f64)
    }
}

/// Test jitter — constant `[0,1)` each call. `1.0` → capped exponential ceiling; `0.0` → zero delay.
pub struct FixedJitter(pub f64);

impl JitterSource for FixedJitter {
    fn next_unit(&self) -> f64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_jitter_returns_its_value() {
        assert_eq!(FixedJitter(0.0).next_unit(), 0.0);
        assert_eq!(FixedJitter(0.5).next_unit(), 0.5);
        // 1.0 is out of the production `[0,1)` contract but valid for FixedJitter.
        assert_eq!(FixedJitter(1.0).next_unit(), 1.0);
    }

    #[test]
    fn splitmix64_values_are_in_unit_interval() {
        let j = SplitMix64Jitter::with_seed(0xDEAD_BEEF);
        for _ in 0..10_000 {
            let v = j.next_unit();
            assert!((0.0..1.0).contains(&v), "value {v} out of [0,1)");
        }
    }

    #[test]
    fn splitmix64_seed_is_deterministic() {
        let a = SplitMix64Jitter::with_seed(42);
        let b = SplitMix64Jitter::with_seed(42);
        for _ in 0..100 {
            assert_eq!(a.next_unit(), b.next_unit());
        }
    }

    #[test]
    fn splitmix64_distinct_seeds_diverge() {
        let a = SplitMix64Jitter::with_seed(1);
        let b = SplitMix64Jitter::with_seed(2);
        assert_ne!(a.next_unit(), b.next_unit());
    }

    #[test]
    fn from_os_does_not_panic_and_is_in_range() {
        let j = SplitMix64Jitter::from_os();
        let v = j.next_unit();
        assert!((0.0..1.0).contains(&v));
    }
}
