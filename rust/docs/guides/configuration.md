# Configuration

## Priority

Resolved at `.build()`:

```
Builder overrides   (highest)
    ↓
Environment variables
    ↓
TOML config file
    ↓
SDK defaults       (lowest)
```

First source that provides a value wins. Construction-only knobs are then immutable.

## Builder

```rust
use kraken_sdk::{ApiKey, Client, KnobValue};

let client = Client::builder()
    .with_api_key(ApiKey::new(key), secret)
    .with_tier_override(kraken_sdk::Tier::Intermediate)
    .with_config_file("/etc/kraken/sdk.toml")
    .with_knob("rate_limit_api_warning_pct", KnobValue::F64(0.85))
    .with_knob("request_timeout_ms", KnobValue::U32(15_000))
    .with_knob("reconnect_attempts", KnobValue::OptU32(Some(5)))
    .with_base_url("https://beta-api.kraken.com")
    .with_headers(identity_headers)
    .build()?;
```

## Rate-limit tier

`Starter` / `Intermediate` / `Pro` select API and trading caps and trading decay. Kraken assigns this from KYC, not 30-day volume (volume is a fee tier). Default `Starter`. `.with_tier_override(Tier)`. The exchange enforces real caps regardless. Local warnings use the configured tier.

## Credentials

Read key and secret from the environment or a secrets manager. The SDK does not read `KRAKEN_API_KEY` / `KRAKEN_API_SECRET` itself.

```rust
let key = std::env::var("KRAKEN_API_KEY")?;
let secret = std::env::var("KRAKEN_API_SECRET")?;
let client = Client::builder()
    .with_api_key(ApiKey::new(key), secret)
    .build()?;
```

`with_api_key` is infallible. A malformed secret is `ConfigError::InvalidCredentials` from `build()`. The encoded string is zeroed after decode. Decoded bytes stay in memory, zeroed on drop, never logged.

### Two-factor (2FA)

If the API key has 2FA, Kraken requires `otp` on every signed REST body. Set once with `with_otp`:

```rust
let client = Client::builder()
    .with_api_key(ApiKey::new(key), secret)
    .with_otp(otp)
    .build()?;
```

The otp is HMAC'd into every signed REST form, including `GetWebSocketsToken`. WS v2 has no per-message otp. Static password only: reused on every signed form and on token refresh (~7.5 min). A rotating TOTP is rejected on reuse. Held as a credential: no `Debug` or `Clone`; zeroed on drop. [`examples/two_factor_auth.rs`](../../examples/two_factor_auth.rs).

## TOML

Knob names as keys. `${ENV_VAR}` interpolation at read, `KRAKEN_*` only. `${...}` in a `#` comment is left verbatim.

```toml
# kraken-sdk.toml
rate_limit_api_warning_pct = 0.75
reconnect_attempts = 5
connection_rate_budget = ${KRAKEN_CONNECTION_RATE_BUDGET}
connection_rate_window_secs = ${KRAKEN_CONNECTION_RATE_WINDOW_SECS}
```

```rust
let client = Client::builder()
    .with_config_file("/etc/kraken/sdk.toml")
    .build()?;
```

No auto-discovery. Env overrides the file.

## Environment variables

`with_headers` has no env or file source.

### Knobs

`KRAKEN_<KNOB_NAME_UPPERCASED>`:

| Knob | Environment variable |
|------|----------------------|
| `request_timeout_ms` | `KRAKEN_REQUEST_TIMEOUT_MS` |
| `rate_limit_api_warning_pct` | `KRAKEN_RATE_LIMIT_API_WARNING_PCT` |
| `reconnect_attempts` | `KRAKEN_RECONNECT_ATTEMPTS` |
| `slow_callback_threshold_ms` | `KRAKEN_SLOW_CALLBACK_THRESHOLD_MS` |
| …every other knob | `KRAKEN_<NAME>` |

### Endpoint URLs

String knobs. TLS required: `https://` REST, `wss://` WS. Cleartext is `ConfigError` with code `INSECURE_ENDPOINT_SCHEME`. Injected `with_transport` skips the REST scheme check. WS URLs have no escape hatch. Malformed-but-TLS URLs fail on first use.

| Endpoint | Environment variable | Default |
|----------|----------------------|---------|
| REST base | `KRAKEN_REST_BASE_URL` | `https://api.kraken.com` |
| Public WS | `KRAKEN_WS_PUBLIC_URL` | `wss://ws.kraken.com/v2` |
| Auth WS | `KRAKEN_WS_AUTH_URL` | `wss://ws-auth.kraken.com/v2` |

Also settable via file or `with_knob`. REST has `with_base_url`. Blank env leaves the production default.

## Reading resolved values

```rust
println!("{:?}", client.knob("rate_limit_api_warning_pct"));
// Some(KnobValue::F64(0.85))

println!("{:?}", client.knob("request_timeout_ms"));
// Some(KnobValue::U32(30000))

println!("{:?}", client.knob("unknown_name"));
// None
```

## Runtime mutation

```rust
use kraken_sdk::KnobValue;

client.set_knob("rate_limit_api_warning_pct", KnobValue::F64(0.60))?;
client.set_knob("slow_callback_threshold_ms", KnobValue::U32(200))?;
```

Construction-only names:

```rust
match client.set_knob("request_timeout_ms", KnobValue::U32(5_000)) {
    Err(ConfigError::ImmutableKnob { knob }) => {
        eprintln!("{knob} is construction-only — set it in the builder");
    }
    _ => {}
}
```

## Config events

`ConfigResolved` at `.build()`, before `client.ready()`. `.on()` starts the dispatch reactor and delivers the buffered event.

```rust
use kraken_sdk::{ConfigSource, EventPayload, EventType};

let _guard = client.events().on(EventType::ConfigResolved, |env| {
    if let EventPayload::ConfigResolved { source_map } = &env.payload {
        for (knob, source) in source_map {
            if *source != ConfigSource::Default {
                println!("{knob} won by: {source:?}");
            }
        }
    }
})?;
```

```rust
let _guard = client.events().on(EventType::ConfigChangedEvent, |env| {
    if let EventPayload::ConfigChangedEvent { knob, previous, current } = &env.payload {
        println!("{} changed: {:?} → {:?}", knob.as_str(), previous, current);
    }
})?;
```

## Nonces

Every signed call carries a strictly-increasing `u64` nonce. SDK scale is nanoseconds (`Unix × 10⁹`). One key per signer, or the same scale on every signer. A larger nonce from another tool starves this client until key rotation (`EAPI:Invalid nonce`). One automatic fresh-nonce retry; order placement is never retried. [Error handling → Nonce](error-handling.md#nonce).

```rust,ignore
let client = Client::builder()
    .with_api_key(ApiKey::new(key), secret)
    .with_nonce_recovery(true)
    .build()?;
```

When on, persistent `EAPI:Invalid nonce` emits `NoncePoisonedEvent` (key fingerprint + endpoint). Detection only.

## Knob mutability

Construction-only knobs are plain values frozen at `.build()`. Runtime knobs are atomics; the next read sees `set_knob`. Time knobs are `_ms` integers; helpers return `Duration`.

Warning percentages (`rate_limit_api_warning_pct`, `rate_limit_trading_warning_pct`): reject, never clamp. `NaN`, ±infinity, and values outside `[0.0, 1.0]` are `ConfigError`. `0.0` and `1.0` are valid. `ConfigChangedEvent.current` equals the stored value.

Zero `caller_to_io_capacity`, `io_to_dispatch_capacity`, `connection_rate_budget`, `backoff_base_ms`, `backoff_max_ms`, `rest_retry_base_ms`, `rest_retry_max_ms` is `InvalidConfig`. `rate_limit_trading_scope_cap` and `cl_ord_id_index_cap` of `0` are `InvalidConfig`.

## Reference: the knobs

Runtime-mutable (`set_knob` → `ConfigChangedEvent`):

| Knob name | Default | Description |
|-----------|---------|-------------|
| `rate_limit_api_warning_pct` | 0.80 | API counter warning (`0.0..=1.0`) |
| `rate_limit_trading_warning_pct` | 0.80 | Trading counter warning |
| `reconnect_attempts` | `None` (unlimited) | Max WS reconnects |
| `subscribe_ack_attempts` | 3 | Subscribe-ack retries |
| `subscribe_ack_timeout_ms` | 5000 | Per-attempt subscribe-ack timeout |
| `max_auth_handshake_failures` | 3 | Auth-WS handshake failures before giving up |
| `slow_callback_threshold_ms` | 50 | `SlowCallbackWarning` threshold |

Construction-only:

| Knob name | Default | Description |
|-----------|---------|-------------|
| `request_timeout_ms` | 30000 | REST timeout |
| `backoff_base_ms` | 500 | WS reconnect base delay |
| `backoff_max_ms` | 30000 | WS reconnect max delay |
| `backoff_factor` | 2.0 | Exponential multiplier |
| `backoff_jitter` | 1.0 | Jitter fraction (1.0 = full) |
| `connection_rate_budget` | 120 | Connect attempts per host per window |
| `connection_rate_window_secs` | 600 | Window for that budget; `0` disables |
| `rest_retry_max_attempts` | 3 | Cooperative REST retries (idempotent ops) |
| `nonce_recovery` | false | Emit `NoncePoisonedEvent` on persistent `Invalid nonce` |
| `prefer_rest_for_orders` | false | REST default for orders; `.via(...)` still overrides |
| `rate_limit_trading_scope_cap` | 256 | Trading-tracker LRU; `0` rejected at build |
| `cl_ord_id_index_cap` | 1024 | `cl_ord_id` index LRU; `0` rejected at build |
| `caller_to_io_capacity` | 2048 | Caller→I/O queue bound |
| `io_to_dispatch_capacity` | 8192 | I/O→dispatch queue bound |

Subset. Source of truth: [`KnobName`](../../src/build/knobs.rs). Env: `KRAKEN_` + screaming snake case. Demo: [`examples/config_knobs.rs`](../../examples/config_knobs.rs).

## Related

- [Architecture — configuration](../architecture.md#configuration)
- [Getting Started — private REST](../getting-started.md#private-rest)
