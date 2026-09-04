# Development

Map of the crate for people changing it. Callers: [Getting Started](getting-started.md) and the [guides](README.md). PR checklist: [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## Repository layout

```
.
├── Cargo.toml          crate manifest (internal-layering comment is a module map)
├── README.md           overview + quickstart
├── CHANGELOG.md        public-behaviour changes
├── CONTRIBUTING.md     PR checklist + code style
├── CLAUDE.md           guide for agents generating application code
├── docs/               this documentation set
├── examples/           runnable programs (`live_*` hit the exchange)
└── src/                the crate
```

## Build, test, lint

From the crate root, aliases match CI ([`CONTRIBUTING.md`](../CONTRIBUTING.md)):

```sh
cargo ci-check     # library + examples + tests
cargo ci-test      # full suite (`--lib` is the fast subset)
cargo ci-clippy    # lint gate
cargo ci-fmt       # formatting
```

`ci-check` and `ci-clippy` use `--all-targets`. `ci-test` is `test --locked`. `ci-fmt` is `fmt --all -- --check`.

Live e2e tests (`#[ignore]`) need `KRAKEN_API_KEY` / `KRAKEN_API_SECRET`. Order-placing tests also need `KRAKEN_E2E_TRADE_REAL=1`. Gating: [`tests/e2e_user_flows.rs`](../tests/e2e_user_flows.rs).

## Source module map

Public surface: `pub use` in `src/lib.rs`, plus `src/api/`, `src/types/`, `src/error.rs`. Everything else is `pub(crate)`. Prefer crate-root names (`kraken_sdk::Symbol`). Exceptions: [`AssetClass`](#assetclass).

| Module | Visibility | Responsibility |
|--------|-----------|----------------|
| `src/lib.rs` | — | Crate root `pub use` |
| `src/api/` | public | Namespaces + request/response/event types |
| `src/build/` | crate | `ClientBuilder`, `Client`, config resolver, knobs |
| `src/dispatch/` | crate | I/O reactor, event bus, handler registry |
| `src/conn/` | crate | Connection FSM, subscription registry, connect-rate budget |
| `src/book/` | crate | Maintained book, CRC32, wire parse |
| `src/rate_limit/` | crate | API and trading counters |
| `src/auth/` | crate | HMAC-SHA512, WS token, nonce |
| `src/transport/` | crate | `reqwest` / `tokio-tungstenite` adapters |
| `src/rest/` | crate | REST surface + retry |
| `src/types/` | mixed | Newtypes, channel/connection enums, handles |
| `src/error.rs` | public | Sealed `ApiError`, `ErrorCategory` |
| `src/clock/`, `src/jitter/` | crate | Injectable clock and jitter |

## Runtime

Caller model: [Architecture → Dual loop](architecture.md#dual-loop). Frame path: [dispatch-flow.md](dispatch-flow.md).

- **I/O reactor** — one task from `client.ready()` (`run_io_reactor`). Single writer for sockets, handler registry, and per-pair book builders. Decodes frames and maintains books. Does not run `on_*`. Payloads cross a drop-oldest queue to dispatch. Rate-limit trackers are shared `Arc`s charged on REST and WS send paths.
- **Dispatch loop** — drains that queue; runs `on_*` and `client.events()` handlers. Slow callbacks: `SlowCallbackWarning`. Full queue: drop-oldest, `QueueFullWarning`.
- **Single-writer / mirror** — caller threads do not mutate reactor state. Registry mutations are reactor-only. `subscribe_*` reads an `RwLock` handler-count mirror.
- **Queues** — caller→I/O rejects when full. I/O→dispatch is drop-oldest. Capacities: `caller_to_io_capacity` / `io_to_dispatch_capacity`.

State machines:

- **Connection FSM** (`src/conn`): `Idle → Connecting → Authenticating → Resubscribing → Open`, `BackingOff` between attempts, terminal `Closing` / `Closed` / `Failed`. Transient TLS/DNS/Cloudflare `429`/`503`/token-fetch blips retry. Permanent rejections go to `Failed`.
- **Maintained book** (`src/book`): one `OrderBookBuilder` per subscription. Reactor applies frames, CRC32 matching Kraken, forwards top-N to `on_book`. Mismatch withholds the book, emits `OrderBookGapEvent` + `SubscriptionGapEvent`, reseeds via unsubscribe/resubscribe. `on_book_raw` skips the builder.

## Dispatch event bus

Components call `publish` / `subscribe` / `post_caller_*` without `.await`. Callers see `client.events().on(...)` and correlated namespace awaits. The I/O reactor drains `caller_to_io`. The dispatch task drains `io_to_dispatch`. REST, token refresh, and socket I/O are separate async paths.

- `caller_to_io` — bounded `tokio::mpsc` (default 2048). Full: `PostReject::Full` → `TradeError::QueueFull` / `SubscriptionError::QueueFull`.
- `io_to_dispatch` — drop-oldest ring (`tokio::mpsc` cannot drop-oldest). Full: pop head, push, increment dropped, `QueueFullWarning` (debounced to the 100 ms queue-depth sample interval).

**Correlation.** `subscribe_correlated` keys `(EventType, correlation_key)`. The key is envelope `request_id`, set by the waiter to the same value it registered.

**Miss-buffer.** Completions that arrive before the waiter latch in a drop-oldest buffer (default 256). Live waiters and the buffer share one lock. Loop death closes the buffer to new entries. Genuine death clears latched entries; intended teardown keeps them. High-frequency emits use `request_id: None` and are not latched. Caller-visible death: [Error handling → Reactor loop death](guides/error-handling.md#reactor-loop-death).

**Delivery matrix** (asserted as `correlated_delivery_matrix`):

| Death state | Waiters armed | Event | Result |
|---|---|---|---|
| none | 0 | terminal | latched |
| none | N | terminal | all N resolve |
| teardown | any | terminal | dropped |
| genuine | any | terminal | dropped |
| teardown / genuine | 0 | incident terminal | latched |
| teardown / genuine | N | incident terminal | all N resolve |

A latched entry is consumed by the first late arm. It is not a broadcast log. Genuine death clears the latch; intended teardown keeps it (so a completion recorded before a mid-close death can still resolve).

**Cycle-break.** The reactor holds `Weak` to the bus. `Drop` on the last bus handle aborts the dispatch task.

**Handler store.** Caller handlers are keyed by channel; wire subscriptions by channel+symbol. Multiple handlers per channel, opaque ids. Authoritative map is reactor single-writer, mutated via drained messages; fan-out on that task is lock-free. `has_handlers(channel)` reads the count mirror, not the map.

**Collapsed EventTypes.** Several completions share one `EventType` (e.g. subscribe ack / fail / terminated). Correlation is `(EventType, request_id)` only. Those events currently use `request_id: None` or are unwired. Wiring a real `request_id` onto a collapsed group requires distinct `EventType`s (or a sub-discriminator), or one waiter can consume another's envelope.

Diagrams: [dispatch-flow.md](dispatch-flow.md). Probes: [dispatch-latency-bench.md](dispatch-latency-bench.md).

## Where to add things

| To add | Touch |
|--------|-------|
| Market REST method | `api/market/rest.rs` + `types.rs` + `error.rs`; re-export `api/mod.rs` → `lib.rs` |
| Account REST method | `api/account/mod.rs` + `requests.rs` + `types.rs` |
| Trade op | `api/trade/mod.rs` + `types/requests.rs` / `types/mod.rs` + `executors.rs` + `ws_compose.rs` |
| WS channel | `types/channel.rs` + decode + `on_*` (`api/<ns>/ws.rs`) + `subscribe_*` / `unsubscribe_*` + `frame_routing.rs` + `subscription_registry.rs` |
| Event type | `dispatch/event_bus/events.rs` + emit site; re-export `lib.rs` |
| Knob | `build/knobs.rs` + `config_resolver.rs` + [Configuration](guides/configuration.md) |
| Error variant | `api/<ns>/error.rs` (`code` / `category` / `retryable`) |

Public error enums are `#[non_exhaustive]`. Wire-open data enums (`OrderType`, `LedgerType`, `TimeInForce`, …) and `ChannelName` / `TickerTrigger` likewise. Request structs are `#[non_exhaustive]` with `Default` + setters. Closed vocabularies (`Price`, `BookDepth`, `ConnectionState`, …) stay exhaustive; adding a variant there is breaking.

## Testing

- Unit tests: `#[cfg(test)]` next to the module (`checksum_tests.rs`, `io_reactor/tests.rs`, …).
- In-crate tests see `pub(crate)` under `#[cfg(test)]`. `test-support` re-exports internals for `tests/` and other crates; off unless `--features test-support`. `ci-clippy` uses `--all-features`; `ci-test` does not.
- Cross-crate tests: [`tests/`](../tests).
- Live e2e: gated as above.

Env-touching tests must use `env_lock()` / `EnvVarGuard` in `src/build`. Tests that drive `DispatchEventBus` or the reactor must start the required loop and wrap awaits in `tokio::time::timeout`.

## Reactor loop death

On panic or abnormal exit the death handler runs once (panic-safe, idempotent). Intended teardown does not broadcast `LoopFailedEvent`. A death during `close()` still resolves waiters: `Err(CloseError::Interrupted)`.

First genuine death: latch loop-failed, fail in-flight correlated awaits (`ClientFailed` / `LoopFailedEvent`), best-effort broadcast `LoopFailedEvent`. Later WS work is `LoopDead`. REST stays up.

Death commits to the correlated registry before the observed-death flag is readable. After that commit, only the incident's own terminal events resolve waiters. Matrix: [Dispatch event bus](#dispatch-event-bus). Errors: [Error handling](guides/error-handling.md#reactor-loop-death), [Events → `LoopFailedEvent`](events.md#loopfailedevent).

## Wire decode — `AssetClass`

`kraken_sdk::types::AssetClass` (`src/types/channel.rs`; not crate-root re-exported). Used on account filters (`LedgersRequest::aclass`). Variants include `Forex`, `TokenizedAsset`, `SyntheticPair`, `FuturesContract`. v1 Spot trading uses `Forex` for crypto/fiat. The SDK does not add `asset_class` on every private endpoint and does not filter `descr.aclass` client-side. [Features → account](features.md#account-namespace).

## Public contracts

Errors: `code`, `category`, `retryable`, `request_id`, `message`, `kraken_code`. Events: `event_type`, `event_version`, `timestamp_monotonic`, optional `request_id`, typed `payload`. From 1.0, bump `event_version` on a breaking payload change. Pre-1.0, fields may change in place at the same version (CHANGELOG).

## Conventions

- Money is `rust_decimal::Decimal`, never `f64`. Parse from strings.
- Symbols are `BASE/QUOTE`. `Symbol::new` rejects `XBT` / `XXBT` / `ZUSD` / wsname. Asset codes on output are modern form.
- Wire-string enums use `strum` (`Display` / `AsRefStr` / `IntoStaticStr`) with `serialize_all` matching serde. Pin every variant in a test. Numeric tokens: per-variant `#[strum(serialize)]` (`OhlcInterval`). A second wire spelling that cannot be `Display` is a pinned helper (`stp_type_ws`). Domain enums that never serialize get no derives until they do.
- No `.unwrap()` on wire data. Network parse returns a typed error.
- Credentials are redacted, never logged or written to disk. Base64 secret is zeroed after decode.
- SemVer: MAJOR = rename / remove / narrow or observable default change. `#[non_exhaustive]` keeps additive changes minor; `match` needs `_`.

Style: [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## Related

- [Architecture](architecture.md)
- [Features](features.md)
- [`CONTRIBUTING.md`](../CONTRIBUTING.md)
