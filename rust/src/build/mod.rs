//! Construction layer — [`Client`] + [`ClientBuilder`]. Owns the builder, client
//! struct, knob resolution, and shared test utilities for environment
//! serialization.

pub mod client;
pub mod config_resolver;
pub mod knobs;

pub use client::{Client, ClientBuilder, CloseError, Completion, ConfigError, ReadyError};
pub use config_resolver::ConfigSource;
#[cfg(feature = "test-support")]
pub use knobs::Knobs;
pub use knobs::{KnobName, KnobValue};

/// Shared test-only env serialization. `config_resolver` and `knobs` tests both
/// mutate and read the process-wide `KRAKEN_*` env, so the lock and an env-
/// restoring RAII guard live here once.
#[cfg(test)]
#[allow(unsafe_code)] // edition 2024: `set_var`/`remove_var` are `unsafe fn`
pub(crate) mod test_env {
    /// Crate-wide lock for env-touching `build` tests; every test that mutates
    /// or reads the environment must hold this for its whole body.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Acquire the env lock, recovering from poison so a prior panic does not
    /// cascade into every later env test.
    pub(crate) fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// RAII guard for a single env var, restoring (or removing) the prior value
    /// on drop so a mid-test panic never leaks env state.
    pub(crate) struct EnvVarGuard {
        key: &'static str,
        prev: Option<String>,
    }
    impl EnvVarGuard {
        pub(crate) fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var(key).ok();
            // SAFETY: test-only; callers hold `ENV_LOCK` for the whole body.
            unsafe { std::env::set_var(key, value) };
            Self { key, prev }
        }
        pub(crate) fn unset(key: &'static str) -> Self {
            let prev = std::env::var(key).ok();
            // SAFETY: test-only; callers hold `ENV_LOCK` for the whole body.
            unsafe { std::env::remove_var(key) };
            Self { key, prev }
        }
    }
    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: restores the prior value under the same lock contract as set/unset.
            match &self.prev {
                Some(v) => unsafe { std::env::set_var(self.key, v) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }
}
