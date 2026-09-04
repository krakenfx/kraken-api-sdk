//! `ConfigResolver` — resolves builder > env > TOML file > defaults into
//! [`Knobs`] plus a per-knob [`ConfigSource`] map. Local file read only; no network.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;

use crate::build::ConfigError;
use crate::build::knobs::{KnobName, KnobValue, Knobs};

/// Which config source supplied a knob's final value.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[non_exhaustive]
pub enum ConfigSource {
    /// Builder `.with_knob(...)` override — highest priority.
    Builder,
    /// `KRAKEN_<SCREAMING_SNAKE>` environment variable.
    Env,
    /// TOML config file passed to the builder.
    File,
    /// SDK default (lowest priority).
    Default,
}

/// Knob-name → winning source for the `ConfigResolved` event (`BTreeMap` for order).
pub type SourceMap = BTreeMap<String, ConfigSource>;

/// Accumulates per-source knob overrides; merges at [`Self::resolve`].
/// Priority: builder > env > TOML file > defaults.
pub struct ConfigResolver {
    builder_overrides: Vec<(String, KnobValue)>,
    config_file: Option<PathBuf>,
}

impl ConfigResolver {
    /// Fresh resolver — no builder overrides, no config file.
    pub fn new() -> Self {
        Self {
            builder_overrides: Vec::new(),
            config_file: None,
        }
    }

    /// Record a builder override (highest priority); later same-name wins.
    pub fn with_builder_knob(&mut self, name: impl Into<String>, value: KnobValue) {
        self.builder_overrides.push((name.into(), value));
    }

    /// Record the config-file path (read at `resolve()`). No I/O here.
    pub fn with_config_file(&mut self, path: impl Into<PathBuf>) {
        self.config_file = Some(path.into());
    }

    /// Merge all four sources into `(Knobs, SourceMap)`. Only I/O is a local file read.
    pub fn resolve(&self) -> Result<(Knobs, SourceMap), ConfigError> {
        let mut knobs = Knobs::defaults();
        let mut source_map: SourceMap = SourceMap::new();

        if let Some(path) = &self.config_file {
            let raw = std::fs::read_to_string(path).map_err(|e| ConfigError::InvalidConfig {
                detail: format!("config file read failed: {e}"),
            })?;
            let interpolated = interpolate_env(&raw)?;
            // Emit parse position only — never source text (may carry credentials).
            let table: toml::Table = interpolated.parse().map_err(|e: toml::de::Error| {
                let at = e.span().map_or_else(String::new, |s| {
                    // Byte count — span offset need not land on a char boundary.
                    let end = s.start.min(interpolated.len());
                    let head = &interpolated.as_bytes()[..end];
                    let line = head.iter().filter(|&&b| b == b'\n').count() + 1;
                    let col = end - head.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1) + 1;
                    format!(" at line {line}, column {col}")
                });
                ConfigError::InvalidConfig {
                    detail: format!("config file parse failed{at}"),
                }
            })?;
            for (name, tv) in table.iter() {
                let knob = KnobName::from_str(name).map_err(|_| ConfigError::InvalidConfig {
                    detail: format!("config file: unknown or wrong-typed knob `{name}`"),
                })?;
                let value =
                    toml_to_knob_value(knob, tv).ok_or_else(|| ConfigError::InvalidConfig {
                        detail: format!("config file: unknown or wrong-typed knob `{name}`"),
                    })?;
                apply(
                    &mut knobs,
                    &mut source_map,
                    knob,
                    &value,
                    ConfigSource::File,
                )?;
            }
        }

        for knob in KnobName::all() {
            let name = knob.as_str();
            let env_key = format!("KRAKEN_{}", name.to_uppercase());
            if let Ok(raw) = std::env::var(&env_key) {
                // Blank URL knob = unset; numeric/bool still error on bad values.
                if raw.trim().is_empty() && knob.is_string_knob() {
                    continue;
                }
                let value =
                    parse_env_value(knob, &raw).ok_or_else(|| ConfigError::InvalidConfig {
                        detail: format!("env {env_key}: cannot parse value for knob `{name}`"),
                    })?;
                apply(&mut knobs, &mut source_map, knob, &value, ConfigSource::Env)?;
            }
        }

        for (name, value) in &self.builder_overrides {
            let knob = KnobName::from_str(name).map_err(|_| ConfigError::InvalidConfig {
                detail: format!("unknown or wrong-typed knob `{name}` from Builder"),
            })?;
            apply(
                &mut knobs,
                &mut source_map,
                knob,
                value,
                ConfigSource::Builder,
            )?;
        }

        for knob in KnobName::all() {
            source_map
                .entry(knob.as_str().to_string())
                .or_insert(ConfigSource::Default);
        }

        // Hard-cap staleness_window_ms at 55_000 ms; source_map entry unchanged.
        const STALENESS_WINDOW_HARD_CAP_MS: u32 = 55_000;
        if knobs.staleness_window_ms > STALENESS_WINDOW_HARD_CAP_MS {
            knobs.staleness_window_ms = STALENESS_WINDOW_HARD_CAP_MS;
        }

        // Reject zero capacities/backoff bases (would panic or reconnect-storm).
        for (name, value) in [
            ("caller_to_io_capacity", knobs.caller_to_io_capacity),
            ("io_to_dispatch_capacity", knobs.io_to_dispatch_capacity),
            ("connection_rate_budget", knobs.connection_rate_budget),
            ("backoff_base_ms", knobs.backoff_base_ms),
            ("backoff_max_ms", knobs.backoff_max_ms),
            ("rest_retry_base_ms", knobs.rest_retry_base_ms),
            ("rest_retry_max_ms", knobs.rest_retry_max_ms),
        ] {
            if value == 0 {
                return Err(ConfigError::InvalidConfig {
                    detail: format!("knob `{name}` must be >= 1 (got 0)"),
                });
            }
        }
        for (name, value) in [
            (
                "rate_limit_trading_scope_cap",
                knobs.rate_limit_trading_scope_cap,
            ),
            ("cl_ord_id_index_cap", knobs.cl_ord_id_index_cap),
        ] {
            if value == 0 {
                return Err(ConfigError::InvalidConfig {
                    detail: format!("knob `{name}` must be >= 1 (got 0)"),
                });
            }
        }

        Ok((knobs, source_map))
    }
}

impl Default for ConfigResolver {
    fn default() -> Self {
        Self::new()
    }
}

/// Apply one override onto `knobs` and stamp `source_map`.
fn apply(
    knobs: &mut Knobs,
    source_map: &mut SourceMap,
    knob: KnobName,
    value: &KnobValue,
    source: ConfigSource,
) -> Result<(), ConfigError> {
    apply_value(knobs, knob, value).map_err(|()| ConfigError::InvalidConfig {
        detail: format!(
            "unknown or wrong-typed knob `{}` from {source}",
            knob.as_str()
        ),
    })?;
    source_map.insert(knob.as_str().to_string(), source);
    Ok(())
}

/// Write `value` into `knobs` for `knob`, covering both buckets (runtime
/// knobs via the atomics' `swap`, construction-only knobs in place). Returns
/// `Err(())` on a value-type mismatch.
fn apply_value(knobs: &mut Knobs, knob: KnobName, value: &KnobValue) -> Result<(), ()> {
    if knob.is_runtime_mutable() {
        return knobs
            .set_runtime_knob(knob, value)
            .map(|_prev| ())
            .ok_or(());
    }
    match (knob, value) {
        (KnobName::BackoffBaseMs, KnobValue::U32(v)) => knobs.backoff_base_ms = *v,
        (KnobName::BackoffMaxMs, KnobValue::U32(v)) => knobs.backoff_max_ms = *v,
        (KnobName::BackoffJitter, KnobValue::F64(v)) => knobs.backoff_jitter = *v,
        (KnobName::BackoffFactor, KnobValue::F64(v)) => knobs.backoff_factor = *v,
        (KnobName::StalenessWindowMs, KnobValue::U32(v)) => knobs.staleness_window_ms = *v,
        (KnobName::CloseTimeoutMs, KnobValue::U32(v)) => knobs.close_timeout_ms = *v,
        (KnobName::UpgradeTimeoutMs, KnobValue::U32(v)) => knobs.upgrade_timeout_ms = *v,
        (KnobName::ConnectionRateBudget, KnobValue::U32(v)) => knobs.connection_rate_budget = *v,
        (KnobName::ConnectionRateWindowSecs, KnobValue::U32(v)) => {
            knobs.connection_rate_window_secs = *v
        }
        (KnobName::RequestTimeoutMs, KnobValue::U32(v)) => knobs.request_timeout_ms = *v,
        (KnobName::WsOrderResponseDeadlineMs, KnobValue::OptU32(v)) => {
            knobs.ws_order_response_deadline_ms = *v
        }
        (KnobName::RestRetryMaxAttempts, KnobValue::U32(v)) => knobs.rest_retry_max_attempts = *v,
        (KnobName::RestRetryBaseMs, KnobValue::U32(v)) => knobs.rest_retry_base_ms = *v,
        (KnobName::RestRetryMaxMs, KnobValue::U32(v)) => knobs.rest_retry_max_ms = *v,
        (KnobName::RestRetryFactor, KnobValue::F64(v)) => knobs.rest_retry_factor = *v,
        (KnobName::NonceRecovery, KnobValue::Bool(v)) => knobs.nonce_recovery = *v,
        (KnobName::PreferRestForOrders, KnobValue::Bool(v)) => knobs.prefer_rest_for_orders = *v,
        (KnobName::RateLimitTradingScopeCap, KnobValue::Usize(v)) => {
            knobs.rate_limit_trading_scope_cap = *v
        }
        (KnobName::ClOrdIdIndexCap, KnobValue::Usize(v)) => knobs.cl_ord_id_index_cap = *v,
        (KnobName::CallerToIoCapacity, KnobValue::U32(v)) => knobs.caller_to_io_capacity = *v,
        (KnobName::IoToDispatchCapacity, KnobValue::U32(v)) => knobs.io_to_dispatch_capacity = *v,
        (KnobName::RestBaseUrl, KnobValue::Str(v)) => knobs.rest_base_url = v.clone(),
        (KnobName::WsPublicUrl, KnobValue::Str(v)) => knobs.ws_public_url = v.clone(),
        (KnobName::WsAuthUrl, KnobValue::Str(v)) => knobs.ws_auth_url = v.clone(),
        _ => return Err(()),
    }
    Ok(())
}

/// Reject non-TLS endpoint URLs at `.build()`. `check_rest` is false when a
/// transport is injected. The error never echoes the URL.
pub(crate) fn validate_endpoint_schemes(
    knobs: &Knobs,
    check_rest: bool,
) -> Result<(), ConfigError> {
    fn require_scheme(knob: &str, url: &str, scheme: &str) -> Result<(), ConfigError> {
        // No trim (connector uses verbatim). Byte-wise prefix avoids multi-byte panic.
        let has_scheme = url
            .as_bytes()
            .get(..scheme.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(scheme.as_bytes()));
        if has_scheme {
            Ok(())
        } else {
            Err(ConfigError::InsecureEndpointScheme {
                knob: knob.to_string(),
                scheme: scheme.to_string(),
            })
        }
    }

    if check_rest {
        require_scheme(
            KnobName::RestBaseUrl.as_str(),
            &knobs.rest_base_url,
            "https://",
        )?;
    }
    require_scheme(
        KnobName::WsPublicUrl.as_str(),
        &knobs.ws_public_url,
        "wss://",
    )?;
    require_scheme(KnobName::WsAuthUrl.as_str(), &knobs.ws_auth_url, "wss://")?;
    Ok(())
}

/// Parse a raw env/TOML-string value into the typed [`KnobValue`] for `knob`.
pub(crate) fn parse_env_value(knob: KnobName, raw: &str) -> Option<KnobValue> {
    let raw = raw.trim();
    match knob {
        KnobName::BackoffJitter
        | KnobName::BackoffFactor
        | KnobName::RestRetryFactor
        | KnobName::RateLimitApiWarningPct
        | KnobName::RateLimitTradingWarningPct => raw.parse::<f64>().ok().map(KnobValue::F64),
        KnobName::RateLimitTradingScopeCap | KnobName::ClOrdIdIndexCap => {
            raw.parse::<usize>().ok().map(KnobValue::Usize)
        }
        KnobName::ReconnectAttempts | KnobName::WsOrderResponseDeadlineMs => {
            if raw.is_empty() || raw.eq_ignore_ascii_case("none") {
                Some(KnobValue::OptU32(None))
            } else {
                raw.parse::<u32>().ok().map(|v| KnobValue::OptU32(Some(v)))
            }
        }
        KnobName::NonceRecovery | KnobName::PreferRestForOrders => {
            raw.parse::<bool>().ok().map(KnobValue::Bool)
        }
        KnobName::RestBaseUrl | KnobName::WsPublicUrl | KnobName::WsAuthUrl => {
            if raw.is_empty() {
                None
            } else {
                Some(KnobValue::Str(raw.to_string()))
            }
        }
        _ => raw.parse::<u32>().ok().map(KnobValue::U32),
    }
}

/// Convert a TOML value into a [`KnobValue`] for `knob` (same typing as env).
fn toml_to_knob_value(knob: KnobName, tv: &toml::Value) -> Option<KnobValue> {
    match knob {
        KnobName::BackoffJitter
        | KnobName::BackoffFactor
        | KnobName::RestRetryFactor
        | KnobName::RateLimitApiWarningPct
        | KnobName::RateLimitTradingWarningPct => tv.as_float().map(KnobValue::F64),
        KnobName::RateLimitTradingScopeCap | KnobName::ClOrdIdIndexCap => tv
            .as_integer()
            .and_then(|i| usize::try_from(i).ok())
            .map(KnobValue::Usize),
        KnobName::ReconnectAttempts | KnobName::WsOrderResponseDeadlineMs => {
            if let Some(i) = tv.as_integer() {
                u32::try_from(i).ok().map(|v| KnobValue::OptU32(Some(v)))
            } else if let Some(s) = tv.as_str() {
                parse_env_value(knob, s)
            } else {
                None
            }
        }
        KnobName::NonceRecovery | KnobName::PreferRestForOrders => {
            tv.as_bool().map(KnobValue::Bool)
        }
        KnobName::RestBaseUrl | KnobName::WsPublicUrl | KnobName::WsAuthUrl => tv
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| KnobValue::Str(s.to_string())),
        _ => tv
            .as_integer()
            .and_then(|i| u32::try_from(i).ok())
            .map(KnobValue::U32),
    }
}

/// Only `KRAKEN_*` may be interpolated — refuse other names before reading them.
const INTERPOLATION_ENV_PREFIX: &str = "KRAKEN_";

/// Expand one `${KRAKEN_*}` at `raw[dollar..]`; returns value and index past `}`.
fn expand_interpolation(raw: &str, dollar: usize) -> Result<(String, usize), ConfigError> {
    let start = dollar + 2;
    let end = raw[start..]
        .find('}')
        .map(|rel| start + rel)
        .ok_or_else(|| ConfigError::InvalidConfig {
            detail: "config file: unterminated `${` interpolation".into(),
        })?;
    let var = &raw[start..end];
    // Refuse non-KRAKEN_* before reading its value.
    if !var.starts_with(INTERPOLATION_ENV_PREFIX) {
        return Err(ConfigError::InvalidConfig {
            detail: format!(
                "config file: refusing to interpolate `${{{var}}}` — only \
                 `${{{INTERPOLATION_ENV_PREFIX}*}}` env vars may be interpolated"
            ),
        });
    }
    let val = std::env::var(var).map_err(|_| ConfigError::InvalidConfig {
        detail: format!("config file: unresolved interpolation `${{{var}}}`"),
    })?;
    Ok((val, end + 1))
}

/// Substitute `${KRAKEN_*}` in `raw`; comment- and string-aware (TOML `#` left alone).
fn interpolate_env(raw: &str) -> Result<String, ConfigError> {
    #[derive(Clone, Copy, PartialEq)]
    enum Ctx {
        Code,
        Comment,
        Basic,
        Literal,
        BasicMulti,
        LiteralMulti,
    }
    let mut out = String::with_capacity(raw.len());
    let bytes = raw.as_bytes();
    // `run_start` marks the current literal byte run, copied as whole `&str`
    // slices so multi-byte UTF-8 is preserved. Slices only cut at an ASCII `$`
    // or the end, so a char is never split.
    let mut run_start = 0;
    let mut i = 0;
    let mut ctx = Ctx::Code;
    while i < bytes.len() {
        let b = bytes[i];
        match ctx {
            // Comment runs to end-of-line; nothing inside it is interpolated.
            Ctx::Comment => {
                if b == b'\n' {
                    ctx = Ctx::Code;
                }
                i += 1;
            }
            // Escaped byte in a basic string: neither closes the string nor
            // starts a token. (`\` is only special in basic strings.)
            Ctx::Basic | Ctx::BasicMulti if b == b'\\' => {
                i += if i + 1 < bytes.len() { 2 } else { 1 };
            }
            _ => {
                match ctx {
                    Ctx::Basic if b == b'"' => {
                        ctx = Ctx::Code;
                        i += 1;
                        continue;
                    }
                    Ctx::Literal if b == b'\'' => {
                        ctx = Ctx::Code;
                        i += 1;
                        continue;
                    }
                    Ctx::BasicMulti if bytes[i..].starts_with(b"\"\"\"") => {
                        ctx = Ctx::Code;
                        i += 3;
                        continue;
                    }
                    Ctx::LiteralMulti if bytes[i..].starts_with(b"'''") => {
                        ctx = Ctx::Code;
                        i += 3;
                        continue;
                    }
                    _ => {}
                }
                // Interpolate `${...}` — allowed in code and inside strings, but
                // NOT in comments (handled above).
                if b == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
                    out.push_str(&raw[run_start..i]);
                    let (val, next) = expand_interpolation(raw, i)?;
                    out.push_str(&val);
                    i = next;
                    run_start = i;
                    continue;
                }
                // Only code can open a comment or a string.
                if ctx == Ctx::Code {
                    if b == b'#' {
                        ctx = Ctx::Comment;
                    } else if bytes[i..].starts_with(b"\"\"\"") {
                        ctx = Ctx::BasicMulti;
                        i += 3;
                        continue;
                    } else if b == b'"' {
                        ctx = Ctx::Basic;
                    } else if bytes[i..].starts_with(b"'''") {
                        ctx = Ctx::LiteralMulti;
                        i += 3;
                        continue;
                    } else if b == b'\'' {
                        ctx = Ctx::Literal;
                    }
                }
                i += 1;
            }
        }
    }
    out.push_str(&raw[run_start..]);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // Env-touching tests share a crate-wide lock + RAII restore guard
    // (build::test_env) to serialize against each other and knobs' env
    // tests — all read/write the same process KRAKEN_* env.
    use crate::build::test_env::{EnvVarGuard, env_lock};

    #[test]
    fn all_knob_names_match_snapshot_surface() {
        let k = Knobs::defaults();
        for knob in KnobName::all() {
            assert!(
                k.snapshot(knob.as_str()).is_some(),
                "{} missing from snapshot",
                knob.as_str()
            );
        }
    }

    #[test]
    fn every_known_knob_is_settable_via_apply_value() {
        let defaults = Knobs::defaults();
        let mut k = Knobs::defaults();
        for knob in KnobName::all() {
            let val = defaults.snapshot_knob(knob);
            assert!(
                apply_value(&mut k, knob, &val).is_ok(),
                "{} rejected by apply_value (half-wired knob)",
                knob.as_str()
            );
        }
    }

    #[test]
    fn cl_ord_id_index_cap_is_settable_via_builder_knob() {
        let _g = env_lock();
        let mut r = ConfigResolver::new();
        r.with_builder_knob("cl_ord_id_index_cap", KnobValue::Usize(4096));
        let (knobs, _map) = r.resolve().unwrap();
        assert_eq!(knobs.cl_ord_id_index_cap, 4096);
    }

    #[test]
    fn defaults_only_all_default_source() {
        let _g = env_lock();
        let (_knobs, map) = ConfigResolver::new().resolve().unwrap();
        assert_eq!(map.len(), KnobName::all().count());
        assert!(map.values().all(|s| *s == ConfigSource::Default));
    }

    #[test]
    fn endpoint_urls_default_to_production() {
        let _g = env_lock();
        let _r = EnvVarGuard::unset("KRAKEN_REST_BASE_URL");
        let _p = EnvVarGuard::unset("KRAKEN_WS_PUBLIC_URL");
        let _a = EnvVarGuard::unset("KRAKEN_WS_AUTH_URL");
        let (k, _m) = ConfigResolver::new().resolve().unwrap();
        assert_eq!(k.rest_base_url, "https://api.kraken.com");
        assert_eq!(k.ws_public_url, "wss://ws.kraken.com/v2");
        assert_eq!(k.ws_auth_url, "wss://ws-auth.kraken.com/v2");
    }

    #[test]
    fn endpoint_urls_resolve_from_env() {
        let _g = env_lock();
        let _r = EnvVarGuard::set("KRAKEN_REST_BASE_URL", "https://uat-api.example.test");
        let _p = EnvVarGuard::set("KRAKEN_WS_PUBLIC_URL", "wss://uat-ws.example.test/v2");
        let _a = EnvVarGuard::set("KRAKEN_WS_AUTH_URL", "wss://uat-ws-auth.example.test/v2");
        let (k, _m) = ConfigResolver::new().resolve().unwrap();
        assert_eq!(k.rest_base_url, "https://uat-api.example.test");
        assert_eq!(k.ws_public_url, "wss://uat-ws.example.test/v2");
        assert_eq!(k.ws_auth_url, "wss://uat-ws-auth.example.test/v2");
    }

    #[test]
    fn blank_endpoint_url_env_falls_back_to_default() {
        let _g = env_lock();
        let _r = EnvVarGuard::set("KRAKEN_REST_BASE_URL", "   ");
        let _p = EnvVarGuard::unset("KRAKEN_WS_PUBLIC_URL");
        let _a = EnvVarGuard::unset("KRAKEN_WS_AUTH_URL");
        let (k, map) = ConfigResolver::new().resolve().unwrap();
        assert_eq!(k.rest_base_url, "https://api.kraken.com");
        // Blank → skipped, so the source stays Default (not Env).
        assert_eq!(map.get("rest_base_url"), Some(&ConfigSource::Default));
    }

    #[test]
    fn builder_base_url_knob_wins_over_env() {
        let _g = env_lock();
        let _r = EnvVarGuard::set("KRAKEN_REST_BASE_URL", "https://env.example.test");
        let mut res = ConfigResolver::new();
        res.with_builder_knob(
            "rest_base_url",
            KnobValue::Str("https://builder.example.test".to_string()),
        );
        let (k, map) = res.resolve().unwrap();
        assert_eq!(k.rest_base_url, "https://builder.example.test");
        assert_eq!(map.get("rest_base_url"), Some(&ConfigSource::Builder));
    }

    // TLS endpoint-scheme validation: these call validate_endpoint_schemes
    // directly with hand-set URL knobs, so they touch no env and need no
    // env_lock.
    fn assert_insecure_scheme(err: &ConfigError, knob: &str) {
        match err {
            ConfigError::InsecureEndpointScheme { knob: named, .. } => {
                assert_eq!(named.as_str(), knob, "wrong knob named in error");
            }
            other => panic!("expected ConfigError::InsecureEndpointScheme, got {other:?}"),
        }
    }

    #[test]
    fn tls_schemes_accept_production_defaults() {
        let k = Knobs::defaults();
        assert!(validate_endpoint_schemes(&k, true).is_ok());
    }

    #[test]
    fn tls_schemes_reject_cleartext_rest() {
        let mut k = Knobs::defaults();
        k.rest_base_url = "http://api.kraken.com".to_string();
        let err = validate_endpoint_schemes(&k, true).unwrap_err();
        assert_insecure_scheme(&err, "rest_base_url");
    }

    #[test]
    fn tls_schemes_reject_cleartext_ws_public() {
        let mut k = Knobs::defaults();
        k.ws_public_url = "ws://ws.kraken.com/v2".to_string();
        let err = validate_endpoint_schemes(&k, true).unwrap_err();
        assert_insecure_scheme(&err, "ws_public_url");
    }

    #[test]
    fn tls_schemes_reject_cleartext_ws_auth() {
        let mut k = Knobs::defaults();
        k.ws_auth_url = "ws://ws-auth.kraken.com/v2".to_string();
        let err = validate_endpoint_schemes(&k, true).unwrap_err();
        assert_insecure_scheme(&err, "ws_auth_url");
    }

    #[test]
    fn tls_schemes_skip_rest_when_transport_injected_but_still_check_ws() {
        let mut k = Knobs::defaults();
        k.rest_base_url = "http://api.kraken.com".to_string();
        assert!(validate_endpoint_schemes(&k, false).is_ok());
        k.ws_public_url = "ws://ws.kraken.com/v2".to_string();
        assert!(validate_endpoint_schemes(&k, false).is_err());
    }

    #[test]
    fn tls_schemes_are_case_insensitive() {
        let mut k = Knobs::defaults();
        k.rest_base_url = "HTTPS://api.kraken.com".to_string();
        k.ws_public_url = "WSS://ws.kraken.com/v2".to_string();
        k.ws_auth_url = "Wss://ws-auth.kraken.com/v2".to_string();
        assert!(validate_endpoint_schemes(&k, true).is_ok());
    }

    #[test]
    fn tls_schemes_reject_bare_host_empty_and_lookalike_scheme() {
        let mut k = Knobs::defaults();
        k.rest_base_url = "api.kraken.com".to_string();
        assert!(validate_endpoint_schemes(&k, true).is_err());
        let mut k_empty = Knobs::defaults();
        k_empty.ws_public_url = String::new();
        assert!(validate_endpoint_schemes(&k_empty, true).is_err());
        let mut k_look = Knobs::defaults();
        k_look.ws_public_url = "wssx://ws.kraken.com/v2".to_string();
        assert!(validate_endpoint_schemes(&k_look, true).is_err());
        // Leading whitespace is rejected — connector uses the un-trimmed value.
        let mut k_ws = Knobs::defaults();
        k_ws.rest_base_url = "  https://api.kraken.com".to_string();
        assert!(validate_endpoint_schemes(&k_ws, true).is_err());
    }

    #[test]
    fn tls_scheme_error_names_knob_and_scheme_but_not_the_url_value() {
        use crate::error::ApiError;
        let mut k = Knobs::defaults();
        k.ws_auth_url = "ws://user:s3cr3t@ws-auth.kraken.com/v2".to_string();
        let err = validate_endpoint_schemes(&k, true).unwrap_err();
        let ConfigError::InsecureEndpointScheme { knob, scheme } = &err else {
            panic!("expected ConfigError::InsecureEndpointScheme, got {err:?}");
        };
        assert_eq!(knob.as_str(), "ws_auth_url");
        assert_eq!(scheme.as_str(), "wss://");
        assert_eq!(err.code(), "INSECURE_ENDPOINT_SCHEME");
        assert!(matches!(
            err.category(),
            crate::error::ErrorCategory::Config
        ));
        let msg = err.to_string();
        assert!(msg.contains("ws_auth_url") && msg.contains("wss://"));
        assert!(
            !msg.contains("s3cr3t") && !msg.contains("user:"),
            "must not echo the URL value: {msg:?}"
        );
    }

    #[test]
    fn tls_schemes_reject_non_ascii_url_without_panicking() {
        // Byte-wise scheme check must reject non-ASCII URLs cleanly, never
        // panic: a naive url[..scheme.len()] slice panics when the length lands
        // inside a multi-byte char.
        let mut k = Knobs::defaults();
        k.rest_base_url = "日日日日".to_string();
        assert!(validate_endpoint_schemes(&k, true).is_err());

        let mut k2 = Knobs::defaults();
        k2.ws_public_url = "café://ws".to_string();
        assert!(validate_endpoint_schemes(&k2, true).is_err());

        // Value shorter (in bytes) than the scheme must not panic either.
        let mut k3 = Knobs::defaults();
        k3.ws_auth_url = "ws".to_string();
        assert!(validate_endpoint_schemes(&k3, true).is_err());
    }

    #[test]
    fn staleness_window_clamped_to_55s_hard_cap() {
        let _g = env_lock();
        let mut r = ConfigResolver::new();
        r.with_builder_knob("staleness_window_ms", KnobValue::U32(90_000));
        let (knobs, _map) = r.resolve().unwrap();
        assert_eq!(knobs.staleness_window_ms, 55_000);
    }

    #[test]
    fn staleness_window_at_or_below_cap_is_unchanged() {
        let _g = env_lock();
        let mut r = ConfigResolver::new();
        r.with_builder_knob("staleness_window_ms", KnobValue::U32(55_000));
        assert_eq!(r.resolve().unwrap().0.staleness_window_ms, 55_000);

        let mut r2 = ConfigResolver::new();
        r2.with_builder_knob("staleness_window_ms", KnobValue::U32(10_000));
        assert_eq!(r2.resolve().unwrap().0.staleness_window_ms, 10_000);

        assert_eq!(
            ConfigResolver::new()
                .resolve()
                .unwrap()
                .0
                .staleness_window_ms,
            30_000
        );
    }

    #[test]
    fn builder_beats_env_beats_file_beats_default() {
        let _g = env_lock();

        let f = tempfile_with(
            "reconnect_attempts = 1\nrequest_timeout_ms = 1000\nsubscribe_ack_attempts = 7\n",
        );
        let path = f.path_buf.clone();

        let _e1 = EnvVarGuard::set("KRAKEN_RECONNECT_ATTEMPTS", "2");
        let _e2 = EnvVarGuard::set("KRAKEN_SUBSCRIBE_ACK_ATTEMPTS", "8");

        let mut r = ConfigResolver::new();
        r.with_config_file(&path);
        r.with_builder_knob("reconnect_attempts", KnobValue::OptU32(Some(3)));

        let (knobs, map) = r.resolve().unwrap();

        assert_eq!(knobs.reconnect_attempts.load(), Some(3));
        assert_eq!(map["reconnect_attempts"], ConfigSource::Builder);
        assert_eq!(
            knobs
                .subscribe_ack_attempts
                .load(std::sync::atomic::Ordering::Relaxed),
            8
        );
        assert_eq!(map["subscribe_ack_attempts"], ConfigSource::Env);
        assert_eq!(knobs.request_timeout_ms, 1000);
        assert_eq!(map["request_timeout_ms"], ConfigSource::File);

        drop(f);
    }

    #[test]
    fn prefer_rest_for_orders_builder_over_env_over_file_over_default() {
        let _g = env_lock();
        let _clear = EnvVarGuard::unset("KRAKEN_PREFER_REST_FOR_ORDERS");
        let (k, map) = ConfigResolver::new().resolve().unwrap();
        assert!(!k.prefer_rest_for_orders);
        assert_eq!(map["prefer_rest_for_orders"], ConfigSource::Default);

        let f = tempfile_with("prefer_rest_for_orders = true\n");
        let mut r = ConfigResolver::new();
        r.with_config_file(&f.path_buf);
        let (kf, mapf) = r.resolve().unwrap();
        assert!(kf.prefer_rest_for_orders);
        assert_eq!(mapf["prefer_rest_for_orders"], ConfigSource::File);

        let _e = EnvVarGuard::set("KRAKEN_PREFER_REST_FOR_ORDERS", "false");
        let mut r2 = ConfigResolver::new();
        r2.with_config_file(&f.path_buf);
        let (ke, mape) = r2.resolve().unwrap();
        assert!(!ke.prefer_rest_for_orders);
        assert_eq!(mape["prefer_rest_for_orders"], ConfigSource::Env);

        let mut r3 = ConfigResolver::new();
        r3.with_config_file(&f.path_buf);
        r3.with_builder_knob("prefer_rest_for_orders", KnobValue::Bool(true));
        let (kb, mapb) = r3.resolve().unwrap();
        assert!(kb.prefer_rest_for_orders);
        assert_eq!(mapb["prefer_rest_for_orders"], ConfigSource::Builder);

        drop(f);
    }

    #[test]
    fn toml_env_interpolation() {
        let _g = env_lock();
        let _e = EnvVarGuard::set("KRAKEN_TEST_BUDGET", "99");
        let f = tempfile_with("connection_rate_budget = ${KRAKEN_TEST_BUDGET}\n");
        let mut r = ConfigResolver::new();
        r.with_config_file(&f.path_buf);
        let (knobs, map) = r.resolve().unwrap();
        assert_eq!(knobs.connection_rate_budget, 99);
        assert_eq!(map["connection_rate_budget"], ConfigSource::File);
        drop(f);
    }

    #[test]
    fn connection_rate_window_secs_resolves_from_file() {
        let _g = env_lock();
        let _e = EnvVarGuard::unset("KRAKEN_CONNECTION_RATE_WINDOW_SECS");
        let f = tempfile_with("connection_rate_window_secs = 300\n");
        let mut r = ConfigResolver::new();
        r.with_config_file(&f.path_buf);
        let (knobs, map) = r.resolve().unwrap();
        assert_eq!(knobs.connection_rate_window_secs, 300);
        assert_eq!(knobs.connection_rate_window().as_secs(), 300);
        assert_eq!(map["connection_rate_window_secs"], ConfigSource::File);
        drop(f);
    }

    #[test]
    fn interpolate_env_preserves_non_ascii_literals() {
        let _g = env_lock();
        let _e = EnvVarGuard::set("KRAKEN_TEST_INTERP", "42");
        let input = "# café ☕ 日本\nbudget = ${KRAKEN_TEST_INTERP} # naïve ✓\n";
        let out = interpolate_env(input).unwrap();
        assert_eq!(out, "# café ☕ 日本\nbudget = 42 # naïve ✓\n");
        assert!(out.contains("café ☕ 日本"));
        assert!(out.contains("naïve ✓"));
    }

    #[test]
    fn toml_non_ascii_value_roundtrips_through_resolve() {
        let _g = env_lock();
        let _e = EnvVarGuard::set("KRAKEN_TEST_BUDGET2", "77");
        let f = tempfile_with(
            "# réglage du débit — 日本語コメント\nconnection_rate_budget = ${KRAKEN_TEST_BUDGET2}\n",
        );
        let mut r = ConfigResolver::new();
        r.with_config_file(&f.path_buf);
        let (knobs, _map) = r.resolve().unwrap();
        assert_eq!(knobs.connection_rate_budget, 77);
        drop(f);
    }

    #[test]
    fn toml_unresolved_interpolation_errors() {
        let _g = env_lock();
        let _e = EnvVarGuard::unset("KRAKEN_DEFINITELY_UNSET_VAR");
        let f = tempfile_with("connection_rate_budget = ${KRAKEN_DEFINITELY_UNSET_VAR}\n");
        let mut r = ConfigResolver::new();
        r.with_config_file(&f.path_buf);
        let err = r.resolve().unwrap_err();
        assert!(matches!(err, ConfigError::InvalidConfig { .. }));
        drop(f);
    }

    #[test]
    fn interpolate_env_refuses_non_allowlisted_var() {
        let _g = env_lock();
        let _e = EnvVarGuard::set("EXTERNAL_SECRET_TOKEN", "s3cr3t-leaked-value");
        let err =
            interpolate_env("connection_rate_budget = ${EXTERNAL_SECRET_TOKEN}\n").unwrap_err();
        let ConfigError::InvalidConfig { detail } = err else {
            panic!("expected ConfigError::InvalidConfig, got {err:?}");
        };
        assert!(
            detail.contains("EXTERNAL_SECRET_TOKEN"),
            "should name the refused var; got: {detail:?}"
        );
        assert!(
            !detail.contains("s3cr3t-leaked-value"),
            "must not echo the refused var's value; got: {detail:?}"
        );
    }

    #[test]
    fn interpolate_env_prefix_boundary() {
        let _g = env_lock();
        let _e1 = EnvVarGuard::set("KRAKEN", "1");
        let _e2 = EnvVarGuard::set("KRAKENISH", "2");
        assert!(interpolate_env("connection_rate_budget = ${KRAKEN}\n").is_err());
        assert!(interpolate_env("connection_rate_budget = ${KRAKENISH}\n").is_err());
    }

    #[test]
    fn interpolate_env_leaves_dollar_brace_in_comment_verbatim() {
        let _g = env_lock();
        let _e = EnvVarGuard::unset("AWS_PROFILE");
        let input = "# see ${AWS_PROFILE} for details\nconnection_rate_budget = 5\n";
        assert_eq!(interpolate_env(input).unwrap(), input);
        // A trailing comment on a value line is skipped too; the value still expands.
        let _k = EnvVarGuard::set("KRAKEN_TEST_TAIL", "8");
        assert_eq!(
            interpolate_env("connection_rate_budget = ${KRAKEN_TEST_TAIL} # ${AWS_PROFILE}\n")
                .unwrap(),
            "connection_rate_budget = 8 # ${AWS_PROFILE}\n"
        );
    }

    #[test]
    fn interpolate_env_hash_inside_string_is_not_a_comment() {
        let _g = env_lock();
        let _e = EnvVarGuard::set("KRAKEN_TEST_HASHSTR", "9");
        assert_eq!(
            interpolate_env("rest_base_url = \"http://x/#f${KRAKEN_TEST_HASHSTR}\"\n").unwrap(),
            "rest_base_url = \"http://x/#f9\"\n"
        );
    }

    #[test]
    fn innocent_comment_with_interpolation_does_not_fail_resolve() {
        let _g = env_lock();
        let _e = EnvVarGuard::unset("AWS_PROFILE");
        let f = tempfile_with("# see ${AWS_PROFILE}\nconnection_rate_budget = 7\n");
        let mut r = ConfigResolver::new();
        r.with_config_file(&f.path_buf);
        let (knobs, _) = r.resolve().unwrap();
        assert_eq!(knobs.connection_rate_budget, 7);
    }

    #[test]
    fn parse_env_value_typed() {
        assert_eq!(
            parse_env_value(KnobName::ReconnectAttempts, "5"),
            Some(KnobValue::OptU32(Some(5)))
        );
        assert_eq!(
            parse_env_value(KnobName::ReconnectAttempts, "none"),
            Some(KnobValue::OptU32(None))
        );
        assert_eq!(
            parse_env_value(KnobName::NonceRecovery, "true"),
            Some(KnobValue::Bool(true))
        );
        assert_eq!(
            parse_env_value(KnobName::RateLimitApiWarningPct, "0.9"),
            Some(KnobValue::F64(0.9))
        );
        assert_eq!(
            parse_env_value(KnobName::RateLimitTradingScopeCap, "128"),
            Some(KnobValue::Usize(128))
        );
        assert_eq!(
            parse_env_value(KnobName::RequestTimeoutMs, "12000"),
            Some(KnobValue::U32(12000))
        );
        assert_eq!(parse_env_value(KnobName::RequestTimeoutMs, "abc"), None);
    }

    #[test]
    fn env_parse_error_does_not_echo_raw_value() {
        let _g = env_lock();
        let _e = EnvVarGuard::set("KRAKEN_REQUEST_TIMEOUT_MS", "s3cr3t-credential-value");
        let err = ConfigResolver::new().resolve().unwrap_err();

        let ConfigError::InvalidConfig { detail } = err else {
            panic!("expected ConfigError::InvalidConfig, got {err:?}");
        };
        assert!(
            !detail.contains("s3cr3t-credential-value"),
            "error message must not echo raw env value; got: {detail:?}"
        );
        assert!(
            detail.contains("KRAKEN_REQUEST_TIMEOUT_MS"),
            "error message should name the env key; got: {detail:?}"
        );
        assert!(
            detail.contains("request_timeout_ms"),
            "error message should name the knob; got: {detail:?}"
        );
    }

    #[test]
    fn config_parse_error_does_not_echo_interpolated_secret() {
        let _g = env_lock();
        let _e = EnvVarGuard::set("KRAKEN_API_SECRET", "s3cr3t-credential-value");
        // Interpolates to a bare unquoted word → invalid TOML → parse error.
        let f = tempfile_with("connection_rate_budget = ${KRAKEN_API_SECRET}\n");
        let mut r = ConfigResolver::new();
        r.with_config_file(&f.path_buf);
        let err = r.resolve().unwrap_err();
        let ConfigError::InvalidConfig { detail } = err else {
            panic!("expected ConfigError::InvalidConfig, got {err:?}");
        };
        assert!(
            !detail.contains("s3cr3t-credential-value"),
            "parse error must not echo the interpolated secret; got: {detail:?}"
        );
    }

    #[test]
    fn zero_capacity_budget_or_backoff_bound_errors_not_panics() {
        for knob in [
            "caller_to_io_capacity",
            "io_to_dispatch_capacity",
            "connection_rate_budget",
            "backoff_base_ms",
            "backoff_max_ms",
            "rest_retry_base_ms",
            "rest_retry_max_ms",
            "rate_limit_trading_scope_cap",
            "cl_ord_id_index_cap",
        ] {
            let f = tempfile_with(&format!("{knob} = 0\n"));
            let mut r = ConfigResolver::new();
            r.with_config_file(&f.path_buf);
            let err = r.resolve().unwrap_err();
            assert!(
                matches!(err, ConfigError::InvalidConfig { .. }),
                "{knob}=0 must be a ConfigError, got {err:?}"
            );
        }
    }

    #[test]
    fn rest_retry_factor_settable_from_config_file() {
        let _g = env_lock();
        let f = tempfile_with("rest_retry_factor = 2.5\n");
        let mut r = ConfigResolver::new();
        r.with_config_file(&f.path_buf);
        let (k, _) = r.resolve().unwrap();
        assert!(
            (k.rest_retry_factor - 2.5).abs() < f64::EPSILON,
            "rest_retry_factor from TOML should be 2.5, got {}",
            k.rest_retry_factor
        );
    }

    #[test]
    fn missing_config_file_errors_as_invalid_config() {
        // A config-file path that can't be read surfaces a typed InvalidConfig,
        // not a panic. The read fails before the env loop, so no env_lock needed.
        let mut r = ConfigResolver::new();
        r.with_config_file("/nonexistent/kraken-sdk-p7-does-not-exist.toml");
        let err = r.resolve().unwrap_err();
        assert!(matches!(err, ConfigError::InvalidConfig { .. }));
    }

    #[test]
    fn toml_unknown_knob_errors_as_invalid_config() {
        let _g = env_lock();
        let f = tempfile_with("this_is_not_a_real_knob = 5\n");
        let mut r = ConfigResolver::new();
        r.with_config_file(&f.path_buf);
        let err = r.resolve().unwrap_err();
        assert!(matches!(err, ConfigError::InvalidConfig { .. }));
        drop(f);
    }

    #[test]
    fn unterminated_interpolation_errors_as_invalid_config() {
        // `${` with no closing `}` is a structural config fault, not a panic.
        let err = interpolate_env("connection_rate_budget = ${KRAKEN_OPEN\n").unwrap_err();
        assert!(matches!(err, ConfigError::InvalidConfig { .. }));
    }

    struct TmpFile {
        path_buf: PathBuf,
    }
    impl Drop for TmpFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path_buf);
        }
    }
    fn tempfile_with(contents: &str) -> TmpFile {
        let mut p = std::env::temp_dir();
        let unique = format!(
            "kraken_knobs_test_{}_{}.toml",
            std::process::id(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        p.push(unique);
        let mut file = std::fs::File::create(&p).unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file.flush().unwrap();
        TmpFile { path_buf: p }
    }
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// Display comes from strum; every variant must render the PascalCase
    /// `source_map` token.
    #[test]
    fn strum_display_matches_wire_strings() {
        for (s, wire) in [
            (ConfigSource::Builder, "Builder"),
            (ConfigSource::Env, "Env"),
            (ConfigSource::File, "File"),
            (ConfigSource::Default, "Default"),
        ] {
            assert_eq!(s.to_string(), wire);
        }
    }
}
