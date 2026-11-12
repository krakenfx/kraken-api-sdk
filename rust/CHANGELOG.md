# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0]

Adds Kraken **Futures** on the Unified WebSocket API: market data, private account
streams and order entry, alongside the existing Spot surface. Futures symbols are
venue strings such as `PF_XBTUSD`; Spot keeps `BTC/USD`. See
[`docs/features.md`](docs/features.md) for the full method and handler tables.

### Added

- **Futures market data.** `client.market().futures()` streams system status,
  instruments, ticker, trades, OHLC candles (trade or mark price) and order books,
  maintained or raw. `client.subscription().futures()` lists, finds and
  unsubscribes Futures streams. Fields the venue may omit are `Option`.
- **Futures account streams.** `client.account().futures()` delivers executions,
  positions, margin (multi- and single-collateral), balances and the account's
  trading permission, each with an `on_*` handler and matching `subscribe_*` /
  `unsubscribe_*` methods. Rows from an opening snapshot carry `is_snapshot`.
- **Futures order entry.** `client.trade().futures()` places, amends and cancels
  orders over the private WebSocket: `order`, `order_batch`, `order_amend`,
  `cancel`, `cancel_batch`, `cancel_all` and the dead-man's switch
  `cancel_all_orders_after`. Orders the venue would reject fail locally before
  anything is sent. A venue throttle fails with `TradeError::RateLimited` and
  `retry_after_ts`. `Client::close()` disarms a dead-man's switch this client armed.
- **Futures lifecycle events.** `FuturesSubscribedEvent`,
  `FuturesSubscribeFailedEvent` and `FuturesSubscriptionTerminatedEvent` report
  Futures stream outcomes; `WsOp::BatchCancel` / `OrderOp::BatchCancel` name the
  batch cancel.
- **Configuration.** `unified_ws_public_url` / `unified_ws_private_url`
  (`KRAKEN_UNIFIED_WS_PUBLIC_URL` / `KRAKEN_UNIFIED_WS_PRIVATE_URL`) override the
  Futures endpoints. `futures_max_in_flight` (`KRAKEN_FUTURES_MAX_IN_FLIGHT`) caps
  Futures orders awaiting a reply, unlimited by default; an order over the cap fails
  unsent with `TradeError::InFlightLimitReached`. It requires
  `ws_order_response_deadline_ms`.
- **TLS trust settings.** `tls_extra_root_certs_file`
  (`KRAKEN_TLS_EXTRA_ROOT_CERTS_FILE`) names a PEM file of extra root CAs, for
  endpoints signed by a private CA; a file that cannot be read or holds no
  certificate fails `.build()` with `ConfigError::InvalidConfig`.
  `ConfigError::UntrustedEndpointHost` (`UNTRUSTED_ENDPOINT_HOST`) reports an
  endpoint URL outside the Kraken domains.
- **New `TradeError` variants**, for Spot and Futures: `SessionExpired`
  (retryable; the session token was rejected or missing, and the order is never
  resent), `PermissionDenied` (the API key lacks the permission) and
  `AuthenticationFailed` (the private WebSocket could not authenticate).
- `OhlcInterval` implements `Ord`, shortest interval first.

### Changed

- **Minimum supported Rust version is 1.88** (was 1.85).
- **Connections trust only the certificate authorities Kraken uses.** REST and
  WebSocket trust the bundled roots of the CAs in the `kraken.com` CAA record
  (Google Trust Services, DigiCert, Sectigo, Let's Encrypt, Amazon, SSL.com), plus
  `tls_extra_root_certs_file`. REST no longer reads the OS trust store, so a CA
  installed on the machine (a TLS-inspecting proxy, for example) is not trusted.
- **Endpoint URLs must be Kraken hosts.** An endpoint URL setting must name a
  `kraken.com` or `kraken.zone` host; any other host, or one that cannot be
  parsed, fails `.build()` with `ConfigError::UntrustedEndpointHost`.
- **Spot `on_*` handler registrations return `Result<HandlerHandle,
  SubscriptionError>`.** A registration that could not be posted used to return a
  handle that never fired. Add `?` at the call site.
- `ExecutionUpdate` and `TradeUpdate` carry `is_snapshot`, `true` for entries from
  the opening snapshot (for trades, the history that `snapshot: Some(true)`
  requests).
- Spot trade errors are more specific. A call refused for a missing permission
  fails with `PermissionDenied` (was `Unknown`). A WebSocket order on a connection
  that failed authentication fails with `AuthenticationFailed`, and one rejected for
  a stale or invalid session token with `SessionExpired`; both used to be a
  retryable `Transport` error, and the token case no longer fires
  `OrderPlacementAmbiguousEvent`.
- `OrderSubmittedEvent`, `OrderPlacementAmbiguousEvent`,
  `OrderCancellationAttempted`, `WsRequestFailedEvent`, `DeadmanDisarmFailedEvent`,
  `ChannelGapEvent` and `MessageDroppedNoHandler` carry `url`, the connection they
  concern. Exhaustive patterns on these need the field or `..`.

### Fixed

- REST requests no longer follow HTTP redirects. A redirect reply fails as an HTTP
  status error, so the signed `API-Key` / `API-Sign` headers never reach another host.
- TLS dependencies (`rustls`, `rustls-webpki`, `webpki-roots`) are updated past
  the open advisories RUSTSEC-2026-0285, -0346, -0348, -0349, -0381, -0382 and
  -0383.
- A WebSocket `order`, `order_amend` or `order_batch` lost in flight after it was
  written now fails with a non-retryable `Transport` error (was retryable), since
  the venue may have received it. Cancels stay retryable.
- A Spot public subscription requested before the socket first opened is replayed
  on the next connection instead of staying `Pending`.
- Spot public subscriptions answered while the socket is still connecting are
  tracked correctly: no false `Already subscribed`, no rows left `Pending`, and no
  resend of a rejected symbol. A permanent rejection emits
  `SubscriptionTerminatedEvent`.
- A Spot WebSocket order whose session token was rejected refreshes the token
  before it retries.
- `Client::close()` disarms the Spot dead-man's switch only if this client armed
  it, so it no longer clears a switch set elsewhere on the account, and skips it on
  a client without credentials. A dead-man arm awaited after `close()` began fails
  with `TradeError::ClientClosed` and is not sent.

### Known limitations

- Tokenized / xStocks pairs surface as `MarketError::SymbolNotFound`.
- Futures orders are not counted by the Spot rate-limit tracker.

## [0.1.0] - 2026-08-14

First public release. An async-native client covering Kraken **Spot** trading
and portfolio data across Spot REST and Spot WebSocket v2.

### Added

- **Two-factor authentication.** `ClientBuilder::with_otp(otp)` supplies the
  one-time password required by API keys with 2FA enabled. It rides inside the
  HMAC'd body of every signed REST form, and because the Spot WS v2 token is
  minted by a signed call, the same otp authenticates the WebSocket session —
  nothing extra to wire for streams or WS orders. Static passwords only; a
  rotating code is rejected on reuse. Cross-binding: every port folds the otp in
  at its signing boundary, not per call site, so the signed body is identical.
- **One client, five namespaces.** `market` (public data), `account` (balances,
  orders, ledgers), `trade` (order placement), `subscription` (WebSocket stream
  lifecycle), and `events` (lifecycle bus), all reached through a `Client` built
  by `ClientBuilder`. `.build()` performs no network I/O.
- **Market data.** REST `ticker`, `orderbook`, `trades`, `ohlc`, `spreads`,
  `assets`, `pairs`, `server_time`, `status`; WebSocket ticker, book, trade,
  OHLC, and system-status channels. `assets` borrows an optional code slice
  (`Option<&[&str]>`, `None` = all) and `ohlc` takes an `OhlcRequest::new(pair,
  interval)` with optional `.since(secs)`.
- **Account data.** REST balance, extended balance, trade balance, open and
  closed orders, ledgers, trades history, positions, and 30-day volume;
  WebSocket `executions` and `balances`. Read filters are typed, not stringly:
  `CloseTime`, `TradeTypeFilter`, `LedgerTypeFilter`, and `aclass(AssetClass)`
  (with `AssetClass::Forex` sent as wire `aclass=currency`).
- **Trading.** Market, limit, and stop-loss shorthands; full-control
  `order(OrderRequest)` (buy or sell selected by `Side` on the request)
  spanning all ten order types; `order_amend`; batch placement of 2–15 orders;
  single, batch, and cancel-all cancellation; and `cancel_all_orders_after`
  (dead-man's switch). Every order operation returns a `PendingTrade` —
  `.via(Transport)` selects REST or the authenticated WebSocket, which is the
  default.
- **Streaming callbacks borrow.** All `on_*` / `on_*_for` data handlers take
  `Fn(&T)` — each frame is decoded once and the same value is lent to every
  handler; clone inside a callback only when it needs to retain the payload.
- **Maintained order book.** The `book` channel is reconstructed into a live
  top-N book, CRC32-validated against the exchange on every update, with
  automatic gap recovery and reseeding. `on_book_raw` opts out and delivers the
  verbatim wire deltas.
- **Exact money.** Every monetary value is a `rust_decimal::Decimal`. The SDK
  never uses `f64` for a price, volume, or amount.
- **Toolchain floor.** Rust **1.85** / edition **2024** (`rust-version` in
  `Cargo.toml`).
- **Modern symbols only.** Pairs are built with `Symbol::new("BTC/USD")`.
  Legacy wire codes (`XBT`, `XXBT`, `ZUSD`, `XXBTZUSD`) are rejected on input
  and normalized away on decode, so they never reach caller code.
- **Typed error taxonomy.** Per-namespace error enums behind the sealed
  `ApiError` trait, each exposing `code`, `category`, `retryable`,
  `request_id`, `message`, and `kraken_code`.
- **Layered configuration.** Builder overrides > environment (`KRAKEN_*`) >
  TOML file > defaults, with runtime-tunable knobs via `set_knob` and a
  `ConfigResolved` event reporting which source won for each knob.
- **Rate-limit awareness without blocking.** The SDK tracks counter usage and
  emits `RateLimitWarning` at a configurable threshold. It never sleeps or
  queues on your behalf — pacing is the caller's decision.
- **Embedding identity.** `with_headers` attributes REST traffic to a host
  application; the SDK's own attribution token is always appended and can never
  be dropped.

### Known limitations

- **Spot only; crypto and forex assets only.** Kraken Futures and a
  cross-product unified API are planned for v2. Tokenized / xStocks pairs
  surface as `MarketError::SymbolNotFound`.

[0.2.0]: https://github.com/krakenfx/kraken-api-sdk/releases/tag/v0.2.0
[0.1.0]: https://github.com/krakenfx/kraken-api-sdk/releases/tag/v0.1.0
