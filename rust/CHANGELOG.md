# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/krakenfx/kraken-api-sdk/releases/tag/v0.1.0
