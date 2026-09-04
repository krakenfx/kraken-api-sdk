use std::collections::HashMap;
use std::sync::Arc;

use crate::api::ws_surface::WsSurface;
use crate::api::{
    AccountNamespace, EventsNamespace, MarketNamespace, SubscriptionNamespace, TradeNamespace,
};
use crate::auth::{
    AuthSigner, AuthStack, NonceSource, SpotRestHmacSha512Signer, SystemClockNonceSource,
};
use crate::build::config_resolver::ConfigResolver;
use crate::build::knobs::KnobValue;
use crate::clock::{Clock, SystemClock};
use crate::conn::ConnectionSupervisor;
use crate::dispatch::{DispatchEventBus, DispatchEventBusConfig};
use crate::jitter::{JitterSource, SplitMix64Jitter};
use crate::rate_limit::ClOrdIdPairIndex;
use crate::rest::RestSurface;
use crate::transport::{HttpTransport, ReqwestHttpTransport, WsSocketFactory};
use crate::types::{ApiKey, ApiSecret, AuthProfile, Otp};

use super::{Client, ClientBuilder, ConfigError};

impl ClientBuilder {
    /// Start a new builder with all defaults.
    pub fn new() -> Self {
        Self {
            transport: None,
            credentials: None,
            otp: None,
            tier_override: None,
            clock: None,
            nonce_source: None,
            jitter_source: None,
            headers: reqwest::header::HeaderMap::new(),
            config_resolver: ConfigResolver::new(),
        }
    }

    /// Set a knob value. Builder overrides beat env, file, and defaults.
    /// Unknown names and type mismatches are rejected at `.build()`, not here.
    pub fn with_knob(mut self, name: impl Into<String>, value: KnobValue) -> Self {
        self.config_resolver.with_builder_knob(name, value);
        self
    }

    /// Enable opt-in nonce-poisoning detection (off by default): a persistent
    /// `EAPI:Invalid nonce` after the fresh-nonce retry emits `NoncePoisonedEvent`.
    /// Never affects order placement. See docs/guides/error-handling.md.
    pub fn with_nonce_recovery(mut self, enable: bool) -> Self {
        self.config_resolver
            .with_builder_knob("nonce_recovery", KnobValue::Bool(enable));
        self
    }

    /// Route order ops to REST instead of the WS-v2 default (off by default).
    /// Per-call [`PendingTrade::via`](crate::api::PendingTrade::via) still wins;
    /// build-time only, immutable at runtime.
    pub fn with_prefer_rest_for_orders(mut self, enable: bool) -> Self {
        self.config_resolver
            .with_builder_knob("prefer_rest_for_orders", KnobValue::Bool(enable));
        self
    }

    /// Point the builder at a TOML config file (read at `.build()`, no auto-discovery).
    /// `${VAR}` interpolation is restricted to `KRAKEN_*`. See docs/guides/configuration.md.
    pub fn with_config_file(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.config_resolver.with_config_file(path);
        self
    }

    /// Override the rate-limit tier (default `Tier::Starter`). Fixed for the
    /// `Client` lifetime. See docs/guides/rate-limits.md.
    pub fn with_tier_override(mut self, tier: crate::rate_limit::Tier) -> Self {
        self.tier_override = Some(tier);
        self
    }

    /// Inject a monotonic clock (default: system). A fake clock makes backoff /
    /// staleness timing deterministic in tests.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Inject a nonce source (default: system clock). Override for multi-process coordination.
    pub fn with_nonce_source(mut self, nonce_source: Arc<dyn NonceSource>) -> Self {
        self.nonce_source = Some(nonce_source);
        self
    }

    /// Inject a [`JitterSource`] (default: OS-seeded SplitMix64). Full-jitter
    /// reconnect backoff; [`FixedJitter`](crate::FixedJitter) for deterministic tests.
    pub fn with_jitter_source(mut self, jitter_source: Arc<dyn JitterSource>) -> Self {
        self.jitter_source = Some(jitter_source);
        self
    }

    /// Override the REST base URL (default `https://api.kraken.com`). Must use
    /// `https://` — cleartext is rejected at `.build()`. Sugar for `rest_base_url`.
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.config_resolver
            .with_builder_knob("rest_base_url", KnobValue::Str(url.into()));
        self
    }

    /// Inject a custom transport (typically a mock). When set, [`Self::with_base_url`]
    /// is ignored; combining with [`Self::with_headers`] is rejected at `.build()`.
    pub fn with_transport(mut self, transport: Arc<dyn HttpTransport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Append embedding-app identity headers to every SDK-built REST request.
    /// A `user-agent` entry is the app token (SDK token always appended).
    /// Rejected with [`Self::with_transport`]; reserved protocol names raise
    /// [`ConfigError::ReservedHeaderName`] at `.build()`.
    pub fn with_headers(mut self, headers: reqwest::header::HeaderMap) -> Self {
        self.headers.extend(headers);
        self
    }

    /// Configure API credentials for signed (private) endpoints. Infallible:
    /// the base64 `secret` is decoded eagerly; a bad one fails at [`Self::build`].
    pub fn with_api_key(mut self, key: ApiKey, secret: String) -> Self {
        let secret = zeroize::Zeroizing::new(secret);
        let decode_result = ApiSecret::from_base64(&secret);
        self.credentials = Some(
            decode_result
                .map(|api_secret| (key, api_secret))
                .map_err(|_| ConfigError::InvalidCredentials),
        );
        self
    }

    /// Set the account's 2FA one-time password, required for keys with two-factor
    /// authentication enabled. Rides inside the HMAC'd body of every signed form, so
    /// it authenticates the WS session too (the WS v2 token is minted by a signed
    /// REST call). Static passwords only — a rotating code is rejected on reuse.
    /// See docs/guides/configuration.md.
    pub fn with_otp(mut self, otp: impl Into<String>) -> Self {
        self.otp = Some(Otp::new(otp));
        self
    }

    /// Finalize the builder into a [`Client`]. Validates config with no network I/O.
    ///
    /// # Errors
    /// - [`ConfigError::InvalidCredentials`] — [`Self::with_api_key`] was given a secret that is not valid base64.
    /// - [`ConfigError::InsecureEndpointScheme`] — a non-TLS endpoint URL.
    /// - [`ConfigError::HeadersRequireSdkTransport`] — custom headers combined with an injected transport.
    /// - [`ConfigError::ReservedHeaderName`] — a custom header used an SDK-owned protocol name.
    /// - [`ConfigError::Unknown`] — unreadable/invalid config file or knob.
    pub fn build(self) -> Result<Client, ConfigError> {
        // No credential-presence gate (private calls fail at use). Malformed secret fails here.
        let credentials = self.credentials.transpose()?;
        // Builder > Env > File > Default; file read is the only I/O.
        let (resolved_knobs, source_map) = self.config_resolver.resolve()?;

        // Refuse non-TLS endpoints so creds never go cleartext.
        crate::build::config_resolver::validate_endpoint_schemes(
            &resolved_knobs,
            self.transport.is_none(),
        )?;

        let knobs = Arc::new(resolved_knobs);

        if self.transport.is_some() && !self.headers.is_empty() {
            return Err(ConfigError::HeadersRequireSdkTransport);
        }
        // Reserved protocol names are SDK-owned; `user-agent` is allowed.
        for name in self.headers.keys() {
            if matches!(
                name.as_str(),
                "api-key" | "api-sign" | "content-type" | "accept" | "host" | "x-korigin"
            ) {
                return Err(ConfigError::ReservedHeaderName {
                    name: name.as_str().to_string(),
                });
            }
        }
        let headers = self.headers;
        let transport: Arc<dyn HttpTransport> = self.transport.unwrap_or_else(|| {
            Arc::new(ReqwestHttpTransport::with_headers(
                knobs.rest_base_url.clone(),
                knobs.request_timeout(),
                &headers,
            ))
        });

        let clock: Arc<dyn Clock> = self.clock.unwrap_or_else(|| Arc::new(SystemClock));
        let jitter: Arc<dyn JitterSource> = self
            .jitter_source
            .unwrap_or_else(|| Arc::new(SplitMix64Jitter::from_os()));
        let bus = {
            let mut bus = DispatchEventBus::new(
                DispatchEventBusConfig::from_knobs(&knobs),
                Arc::clone(&clock),
            );
            // Shared knobs so runtime set_knob is visible to both reactors.
            bus.set_knobs(Arc::clone(&knobs));
            Arc::new(bus)
        };

        let nonce_source: Arc<dyn NonceSource> = self
            .nonce_source
            .unwrap_or_else(|| Arc::new(SystemClockNonceSource::new()));
        let mut signers: HashMap<AuthProfile, Arc<dyn AuthSigner>> = HashMap::new();
        let api_key_opt = credentials.map(|(key, secret)| {
            let signer = SpotRestHmacSha512Signer::new(key.clone(), secret);
            signers.insert(AuthProfile::SpotV1, Arc::new(signer));
            key
        });
        // Fingerprint for credential events; `<no-key>` when credentials absent.
        let key_id_fingerprint = api_key_opt
            .as_ref()
            .map(crate::auth::derive_key_fingerprint)
            .unwrap_or_else(|| "<no-key>".to_string());
        let token_lifecycle =
            crate::auth::TokenLifecycleManager::new(Arc::clone(&bus), key_id_fingerprint);
        let auth = Arc::new(AuthStack::new(
            api_key_opt,
            self.otp,
            nonce_source,
            signers,
            token_lifecycle,
        ));

        let tier = self.tier_override.unwrap_or_default();
        let spot_api_rate_limit = Arc::new(crate::rate_limit::SpotApiRateLimitTracker::new(
            tier,
            Arc::clone(&bus),
            Arc::clone(&clock),
            Arc::clone(&knobs),
        ));
        let spot_trading_rate_limit =
            Arc::new(crate::rate_limit::SpotTradingRateLimitTracker::new(
                tier,
                Arc::clone(&bus),
                Arc::clone(&clock),
                Arc::clone(&knobs),
            ));

        // Shared across RestSurface and WsSurface.
        let cl_ord_id_index = Arc::new(ClOrdIdPairIndex::new(knobs.cl_ord_id_index_cap));

        let retry_engine = crate::rest::retry::RetryEngine::from_knobs(&knobs, Arc::clone(&jitter));
        let rest = Arc::new(RestSurface::new_with_index(
            Arc::clone(&transport),
            Arc::clone(&auth),
            Arc::clone(&spot_api_rate_limit),
            Arc::clone(&spot_trading_rate_limit),
            Arc::clone(&clock),
            Arc::clone(&cl_ord_id_index),
            retry_engine,
            // Same timeout as the transport; bounds the signed-send (nonce lock-hold) window.
            knobs.request_timeout(),
        ));

        // Weak back-wire so lazy token fetch can sign without an ownership cycle.
        auth.token_lifecycle().set_rest(Arc::downgrade(&rest));

        rest.set_bus(Arc::clone(&bus));

        let ws_factory = Arc::new(WsSocketFactory::new_with_urls(
            Arc::clone(&bus),
            knobs.ws_public_url.clone(),
            knobs.ws_auth_url.clone(),
        ));
        let supervisor = Arc::new(ConnectionSupervisor::new_with_knobs(
            Arc::clone(&bus),
            &knobs,
        ));

        let presence_mirror: crate::dispatch::PresenceMirror =
            Arc::new(std::sync::RwLock::new(std::collections::HashMap::new()));
        let subscription_mirror = crate::conn::subscription_registry::SubscriptionMirror::default();
        let handler_id_alloc = Arc::new(std::sync::atomic::AtomicU64::new(1));
        let ws_surface = Arc::new(WsSurface::new_with_supervisor_and_index(
            Arc::clone(&bus),
            Arc::clone(&presence_mirror),
            Arc::clone(&subscription_mirror),
            Arc::clone(&handler_id_alloc),
            Arc::clone(&supervisor),
            Arc::clone(&spot_trading_rate_limit),
            Arc::clone(&auth),
            Arc::clone(&clock),
            Arc::clone(&cl_ord_id_index),
        ));
        let subscription = SubscriptionNamespace::new(Arc::clone(&ws_surface));

        let dispatch =
            Arc::new(crate::dispatch::dispatch_table::DispatchTable::with_default_spot_table());

        let account = AccountNamespace::new(
            Arc::clone(&rest),
            Arc::clone(&ws_surface),
            Arc::clone(&dispatch),
        );

        let market = MarketNamespace::new(
            Arc::clone(&rest),
            Arc::clone(&ws_surface),
            Arc::clone(&dispatch),
        );

        let trade = TradeNamespace::new_with_ws(
            Arc::clone(&rest),
            Arc::clone(&ws_surface),
            Arc::clone(&knobs),
            Arc::clone(&dispatch),
        );

        let events = EventsNamespace::new(Arc::clone(&bus));

        let client = Client {
            auth,
            market,
            account,
            trade,
            subscription,
            events,
            presence_mirror,
            subscription_mirror,
            bus,
            supervisor,
            ws_factory,
            clock,
            jitter,
            knobs,
            io_reactor_handle: std::sync::Mutex::new(None),
            next_client_request_id: std::sync::atomic::AtomicU64::new(1),
        };

        // First bus event; buffers in the ring until .ready() starts the reactor.
        client.bus.publish(crate::dispatch::EventEnvelope {
            event_type: crate::dispatch::EventType::ConfigResolved,
            event_version: 1,
            timestamp_monotonic: client.clock.now(),
            request_id: None,
            payload: crate::dispatch::EventPayload::ConfigResolved { source_map },
        });

        Ok(client)
    }
}
