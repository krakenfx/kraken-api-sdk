# Error Handling

## ApiError

Every SDK error implements `ApiError`:

```rust
pub trait ApiError {
    fn code(&self) -> &str;                 // e.g. "RATE_LIMIT_EXCEEDED"
    fn category(&self) -> ErrorCategory;    // Config / Auth / Network / Exchange / Client / RateLimit
    fn retryable(&self) -> bool;
    fn request_id(&self) -> Option<&str>;
    fn message(&self) -> String;
    fn kraken_code(&self) -> Option<&str>;
}
```

| Category | Meaning |
|----------|---------|
| `Config` | Build or knob problem |
| `Auth` | Credential missing or invalid |
| `Network` | Transport failure (may be transient) |
| `Exchange` | Exchange rejected the request |
| `Client` | SDK state |
| `RateLimit` | Rate limit exceeded |

## Per-namespace enums

| Namespace | Error type |
|-----------|-----------|
| `client.market().*` | `MarketError` |
| `client.account().*` | `AccountError` |
| `client.trade().*` | `TradeError` |
| `client.subscription().*` | `SubscriptionError` |
| `client.events().*` | `EventsError` |
| `Client::builder().build()` | `ConfigError` |
| `client.ready().await` | `ReadyError` |
| `client.close().await` | `CloseError` |

No top-level `SdkError`.

## Matching

```rust
use kraken_sdk::{ApiError, MarketError, Symbol};

let sym = Symbol::new("BTC/USD")?;
match client.market().ticker(Some(&[sym.clone()])).await {
    Ok(tr) => {
        let t = tr.get(&sym).ok_or_else(|| MarketError::SymbolNotFound { symbol: sym.to_string() })?;
        println!("last={}", t.last_price);
    }
    Err(e) => {
        eprintln!("code={} category={:?} retryable={} msg={}",
            e.code(), e.category(), e.retryable(), e.message());

        match e {
            MarketError::SymbolNotFound { symbol } => {
                eprintln!("no such pair: {symbol}");
            }
            MarketError::Transport { .. } => { /* retry if retryable() */ }
            MarketError::RateLimited { .. } => { /* back off */ }
            MarketError::Unknown { kraken_code, .. } => {
                eprintln!("kraken error code: {kraken_code}");
            }
            _ => {}
        }
    }
}
```

`MarketError`: `SymbolNotFound` · `InvalidArguments` · `SystemStatus` · `RateLimited` · `Transport` · `MalformedResponse` · `ClientClosed` · `Unknown`.

Unrecognised pair wire: `EQuery:Unknown asset pair`. Symbol-taking market methods lift to `SymbolNotFound { symbol }`. A context-free path with no symbol is `Unknown`.

## TradeError

```rust
use kraken_sdk::TradeError;

match client.trade().limit_buy(pair, vol, price).await {
    Err(TradeError::InvalidOrder { detail }) => {
        eprintln!("order rejected: {detail}");
    }
    Err(TradeError::Transport { kind, .. }) => {
        // retryable() is true only when the request was never sent
        eprintln!("transport error: {kind:?}");
    }
    Err(TradeError::SystemStatusBlocked { current, required }) => {
        eprintln!("exchange is in {:?} mode — need {:?}", current.status, required.status);
    }
    Err(TradeError::ConflictingOrderIdentifiers) => {
        eprintln!("cannot set both userref and cl_ord_id on the same order");
    }
    Err(TradeError::WsUnsupportedOrderField { field })
    | Err(TradeError::RestUnsupportedOrderField { field }) => {
        eprintln!("field not supported on this transport: {field}");
    }
    Err(TradeError::QueueFull { .. }) => {
        // not sent; retry after back-off
    }
    Err(e) => eprintln!("error: {e:?}"),
    Ok(resp) => println!("placed: {:?}", resp.txid),
}
```

Amend: `NoAmendableParameters` (code `NON_AMENDABLE_FIELD`), `UnknownOrder` (code `ORDER_NOT_FOUND`). Both non-retryable `Client`.

## Invalid order (`EOrder:Invalid order`)

Same wire string for already-gone (cancelled/filled) and never-valid ids.

- Cancel: `TradeError::InvalidOrder` (`INVALID_ORDER`, `Client`, non-retryable) on REST and WS. Not a successful cancel. Not auto-retried.
- Lookup of a bad ledger/order id (e.g. `query_ledgers`): `AccountError::Unknown`. Query ids from a prior list (`ledgers()`).

## ConfigError on build

Every setter is infallible. `build()` is the fallible step.

```rust
use kraken_sdk::{ApiKey, Client, ConfigError};

let result = Client::builder()
    .with_api_key(ApiKey::new("mykey"), "not-valid-base64!".to_string())
    .build();

match result {
    Err(ConfigError::InvalidCredentials) => eprintln!("bad base64 secret"),
    Err(e) => eprintln!("config error: {e}"),
    Ok(client) => { /* proceed */ }
}
```

`rate_limit_trading_scope_cap = 0` and `cl_ord_id_index_cap = 0` are `ConfigError::InvalidConfig`.

## SubscriptionError — NoHandlerRegistered

`subscribe_*` before a handler: `Client`, `retryable() == false`, code `INVALID_ARGUMENTS`.

```rust
use kraken_sdk::SubscriptionError;

if let Err(e) = client.subscription().subscribe_ticker(pairs.clone(), None, None) {
    if matches!(e, SubscriptionError::NoHandlerRegistered { .. }) {
        // register on_* first
    }
}

let _handle = client.market().on_ticker(|update| { /* ... */ });
client.subscription().subscribe_ticker(pairs, None, None)?;
```

Or use `on_*_for`.

## QueueFull and LoopDead

- **`QueueFull`** (`QUEUE_FULL`) — caller→I/O full, not sent. The only retryable `Client` error. SDK does not auto-retry.
- **`LoopDead`** (`LOOP_DEAD`) — reactor died. Not retryable. New WS ops rejected; REST stays up.

## Cancelled order await

Save `cl_ord_id` before `.await`. Listen for `OrderCancellationAttempted` / `OrderPlacementAmbiguousEvent`. Reconcile with `find_order_by_cl_ord_id`. [Placing orders → Idempotency](placing-orders.md#idempotency-and-reconciliation).

## Authentication and token errors

| Wire string | Meaning | Recoverable? |
|---|---|---|
| `EAPI:Invalid token` | session token stale | yes — refresh + resend |
| `ESession:Invalid session` | invalid session (no `token` in the text) | yes — refresh + resend |
| `EAPI:Invalid key` | bad API key | no |
| `EAPI:Invalid signature` | bad signature | no |
| `EAuth:*` | credential failure | no |
| `EAPI:Permission denied` / `EGeneral:Permission denied` | key lacks permission | no |

Text containing `token` plus a stale/expired/invalid marker is treated as a stale token.

`GetWebSocketsToken` transport maps to `AuthError::TokenRefreshTransient` or `TokenRefreshFailed`, category `Network`. Transient is retryable; failed is not. Event: [CredentialRefreshFailedEvent](../events.md#credentialrefreshfailedevent).

Auth WS has no dedicated handshake response. Authentication is the ack of the first signed request (subscribe or order). An order-level reject (e.g. insufficient funds) does not drop the session. Mid-session `EAPI:Invalid token` does not close the socket; the SDK refreshes in the background; the in-flight order is retryable.

Handshake: token-stale stays in authenticating and re-presents a fresh token (bounded; Nth failure → terminal + `AuthenticationFailedEvent` / `RetryCapExhausted`). Transient: back off and reconnect, not bounded by that cap.

Token-fetch throttle / `EService:` is retryable refresh-transient. `EGeneral:Temporary lockout` (~15 min ban) is not.

## Nonce

Private REST nonce is strictly greater than the last seen for that key. SDK uses wall-clock nanoseconds (`Unix × 10⁹`) and a process-global floor. Sign-and-send is serialized per key.

Do not share one key across different nonce scales. `EAPI:Invalid nonce` gets one automatic fresh-nonce retry (independent of `rest_retry_max_attempts`). A second rejection bubbles as that namespace's error (or `AuthError::InvalidNonce` on token refresh). Live `AddOrder` / `AddOrderBatch` are never retried. Validate-mode placement takes the single nonce re-sign.

Persistent `Invalid nonce`: rotate the key or align scales. `nonce_recovery` emits `NoncePoisonedEvent` (key fingerprint + endpoint). Detection only.

## WebSocket error shapes

WS v2 `error` is a **string**, not a REST array. Same typed taxonomy.

Subscribe success nests `channel` / `symbol` under `result`. A rejection may drop `result`. Unsupported pair (`Currency pair not supported <SYM>`): no `channel`, no `result`. Bad channel (`Subscription name invalid`): neither `channel` nor `symbol`. Correlation is `req_id`. Error text is verbatim. Absent or non-boolean `success` is failure.

## WebSocket server close codes

**1008** (policy violation): terminal. Every other code, including **1006**, reconnects. Disposition is the close code only.

## Rate limit errors

Warning: `RateLimitWarning`. Hard limit: namespace `RateLimited` (`category() == RateLimit`, `retryable() == true`, `code() == "RATE_LIMIT_EXCEEDED"`, `kraken_code()` `None`). Optional `retry_after_ts`. SDK does not sleep.

```rust
use kraken_sdk::{EventType, KnobValue};

let _guard = client.events().on(EventType::RateLimitWarning, |env| {
    eprintln!("rate limit warning: {:?}", env.payload);
})?;

client.set_knob("rate_limit_api_warning_pct", KnobValue::F64(0.6))?;
```

### Rate-limit error codes

| Wire string | Counter |
|---|---|
| `EAPI:Rate limit exceeded` | general REST |
| `EAuth:Rate limit exceeded` | general REST (auth-adjacent) |
| `EGeneral:Too many requests` | general REST |
| `EService:Throttled` | general REST |
| `EOrder:Rate limit exceeded` | trading (per-pair) |
| `EOrder:Domain rate limit exceeded` | trading (account) |

General-API strings match by prefix (`: <timestamp>` still matches). `EOrder:` codes match exactly. Matching tracker snaps to cap, then `RateLimited`. `EAPI:Invalid key` and `EAPI:Invalid nonce` are not rate-limit codes.

`EGeneral:Temporary lockout` is a hard auth ban, not flood control. Non-transient. Not auto-retried.

## Lifecycle awaits

`ready().await` → `Ok(())` or `Err(ReadyError)`. `close().await` → `Ok(())` or `Err(CloseError::Interrupted)`. Codes: `REACTOR_SPAWN_FAILED` / `LOOP_DEAD` / `CLOSE_INTERRUPTED`. `Client`, not retryable. Loop death resolves the await.

## Request handle

`RequestHandle` correlates to a completion event. Success and (when present) failure arms share `request_id`. First wins. Drop tears both arms down. Long-lived bus subscribers still receive the event.

`.ready()` dual-arm: `{ClientReady, ClientFailed}`. Loop death: `ClientFailed` with loop-failed cause on that `request_id`. Single-arm: loop-failed envelope.

`await_request_handle` → `Result<EventEnvelope, AwaitError>`. Completions above are `Ok`. `AwaitError::LoopDead`: death drain already ran, or the correlated one-shot dropped during teardown. Distinct from namespace `LOOP_DEAD` (rejects new ops).

## Reactor loop death

Panic or abnormal exit: `LoopFailedEvent`, in-flight awaits fail, new WS is `LoopDead`, REST stays. Intended teardown does not broadcast `LoopFailedEvent`. Death during `close()`: `Err(CloseError::Interrupted)`.

Internals: [Development → Reactor loop death](../development.md#reactor-loop-death). Payloads: [LoopFailedEvent](../events.md#loopfailedevent), [ClientFailed](../events.md#clientfailed).

### Internal lock failures

| Lock role | Behavior |
|---|---|
| Event-bus registry, ring, or loop-death lock | Recover; callbacks run after the registry lock is released. |
| Correlated-await one-shot (`TxCell`) | Recover. |
| Caller-facing subscription mirror read | `LoopDead`. |
| Best-effort projection write | Skip the update. |
| Reactor lifecycle / setup invariant | Fail fast. |

## REST retry and cancellation

Transient REST retries with reason-aware backoff and `RestRetryAttempt`. `AddOrder` never auto-retries. Drop during backoff cancels further attempts; last wire request is not rolled back. [RestRetryAttempt](../events.md#restretryattempt).

## Related

- [Features — error taxonomy](../features.md#error-taxonomy)
- [Architecture — credentials](../architecture.md#credentials)
