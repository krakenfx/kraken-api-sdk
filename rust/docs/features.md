# Features

## Namespaces

| Accessor | Type | Description |
|----------|------|-------------|
| `client.market()` | `&MarketNamespace` | Public market data — REST and WS |
| `client.account()` | `&AccountNamespace` | Private account data — REST and WS |
| `client.trade()` | `&TradeNamespace` | Order placement and cancellation |
| `client.subscription()` | `&SubscriptionNamespace` | WS subscribe / unsubscribe |
| `client.events()` | `&EventsNamespace` | SDK lifecycle and operational events |

## market namespace

### REST methods

| Method | Description |
|--------|-------------|
| `ticker(pairs: Option<&[Symbol]>)` | Latest bid, ask, last, volume, VWAP. `None` fetches all pairs. Returns `TickerResult` |
| `orderbook(symbol: &Symbol, count: Option<u32>)` | Top-N snapshot. `count` defaults to 100 |
| `trades(req: TradesRequest)` | Recent trades. `TradesRequest::new(pair)` plus `.since(cursor)` (nanosecond string) and `.count(n)` (max 1000) |
| `ohlc(req: OhlcRequest)` | OHLCV. `OhlcRequest::new(pair, interval)` plus optional `.since(secs)` |
| `spreads(pair: Symbol, since: Option<String>)` | Recent best bid/ask |
| `assets(assets: Option<&[&str]>)` | Asset metadata. `None` = all |
| `pairs(pairs: Option<&[Symbol]>)` | Pair metadata. `None` = all |
| `server_time()` | Exchange server time |
| `status()` | System status string: `online` / `post_only` / `cancel_only` / `maintenance` |

`TickerResult` is a map keyed by pair string with `.get(&Symbol) -> Option<&Ticker>`. `Ticker` fields: `ask_price`, `ask_whole_lot_volume`, `ask_lot_volume`, `bid_price`, `bid_whole_lot_volume`, `bid_lot_volume`, `last_price`, `last_volume`, `volume_today`, `volume_24h`, `vwap_today`, `vwap_24h`, `trades_today`, `trades_24h`, `low_today`, `low_24h`, `high_today`, `high_24h`, `open`. All `Decimal` except `trades_today` / `trades_24h` (`u64`). `*_today` is the current day; `*_24h` is a rolling 24-hour window.

`OhlcInterval`: `M1`, `M5`, `M15`, `M30`, `H1`, `H4`, `D1`, `D7`, `D15` (15 days).

### WebSocket streaming handlers

| Method | Trigger | Description |
|--------|---------|-------------|
| `on_ticker(cb)` | Channel-wide | `TickerUpdate` for subscribed pairs |
| `on_ticker_for(pairs, snapshot, event_trigger, cb)` | Register + subscribe | Returns `SubscriptionGuard` |
| `on_trade(cb)` | Channel-wide | One `TradeUpdate` per print |
| `on_trade_for(pairs, snapshot, cb)` | Register + subscribe | Returns `SubscriptionGuard` |
| `on_book(cb)` | Channel-wide | `OrderBookUpdate` (maintained) |
| `on_book_for(pairs, depth, cb)` | Register + subscribe | Returns `SubscriptionGuard` |
| `on_book_raw(cb)` | Channel-wide | `BookDelta` (raw) |
| `on_book_raw_for(pairs, depth, snapshot, cb)` | Register + subscribe | Returns `SubscriptionGuard` |
| `on_ohlc(cb)` | Channel-wide | One `OhlcUpdate` per candle |
| `on_ohlc_for(pairs, interval, snapshot, cb)` | Register + subscribe | Returns `SubscriptionGuard` |
| `on_system_status(cb)` | Channel-wide | `SystemStatusUpdate` on open and status changes |

`on_*` returns `HandlerHandle`. Drop deregisters the callback; the wire subscription stays until `unsubscribe_*`. `on_*_for` returns `Result<SubscriptionGuard, SubscriptionError>`. Drop the guard to unsubscribe and deregister. Refcount and force-unsubscribe: [Streaming](guides/streaming.md), [Events → `SubscriptionTerminatedEvent`](events.md#subscriptionterminatedevent).

`snapshot: Option<bool>` is on `ticker`, `book_raw`, `trade`, and `ohlc` `subscribe_*` / `on_*_for`. `None` keeps Kraken's default. `Some(false)` suppresses the opening snapshot. `Some(true)` on `trade` pulls recent-trades history. Ticker also takes `event_trigger: Option<TickerTrigger>` (`Trades` or `Bbo`). Default is `Trades`. `on_book` / `subscribe_book` have no `snapshot` argument; they always request the opening snapshot. For delta-only use `on_book_raw_for(pairs, depth, Some(false), cb)`.

Book depth is fixed at first subscribe for that pair. A later subscribe at a different depth joins the existing subscription. After a termination, the next subscribe starts a new lifetime.

`TickerUpdate.timestamp` is the exchange RFC 3339 wall clock (`""` if omitted). Book payloads use `exchange_timestamp`. See [Order book](#order-book).

Callback type: `Fn(&T) + Send + Sync + 'static` (not `FnMut`). Handlers run on the dispatch loop. Clone the payload only if the callback retains it. Dual-loop contract: [Architecture](architecture.md#dual-loop).

## account namespace

### REST methods

| Method | Description |
|--------|-------------|
| `balance()` | Per-asset balance map |
| `extended_balance()` | Balance + `hold_trade` per asset |
| `trade_balance(asset: Option<String>)` | Consolidated margin/trade balance in a quote currency |
| `open_orders(req: OpenOrdersRequest)` | Open orders. Setters: `.trades`, `.userref`, `.cl_ord_id` |
| `closed_orders(req: ClosedOrdersRequest)` | Closed orders, paginated. Setters: `.trades`, `.userref`, `.start`, `.end`, `.ofs`, `.closetime`, `.cl_ord_id` |
| `ledgers(req: LedgersRequest)` | Ledgers, paginated. Setters: `.asset`, `.aclass`, `.ledger_type`, `.start`, `.end`, `.ofs` |
| `query_ledgers(ids: Vec<String>)` | Ledgers by id |
| `trades_history(req: TradesHistoryRequest)` | Fills. Setters: `.trade_type`, `.trades`, `.start`, `.end`, `.ofs`, `.ledgers` |
| `positions(txids, docalcs)` | Open margin positions |
| `volume(pair, fee_info)` | 30-day rolling volume. `fee_info` is sent; the fee schedule is not decoded |
| `find_order_by_cl_ord_id(cl_ord_id: &ClOrdId)` | Order lookup by client order id |

Request structs are `Default` and chain: `ClosedOrdersRequest::default().trades(true).ofs(50)`. Setters take a plain value, not `Option`. Unset fields are omitted, except `trades` on `OpenOrdersRequest` / `ClosedOrdersRequest` (always sent, default `false`). `TradesHistoryRequest.trades` stays omitted when unset. Filters use `CloseTime`, `TradeTypeFilter`, `LedgerTypeFilter`, and `types::AssetClass`. `LedgerTypeFilter` is outbound-only and has no `Unknown`.

Asset codes in responses are modern form (`BTC`, `USD`, `USDC`). Legacy wire codes (`XXBT`, `ZUSD`) are stripped on decode. Look up `balance.assets.get("USDC")`.

### WebSocket streaming handlers

| Method | Description |
|--------|-------------|
| `on_executions(cb)` | `ExecutionUpdate` (auth WS) |
| `on_executions_for(cb)` | Register + subscribe. Returns `SubscriptionGuard` |
| `on_balances(cb)` | `BalanceUpdate` (auth WS) |
| `on_balances_for(cb)` | Register + subscribe. Returns `SubscriptionGuard` |

Execution and balance channels are account-wide. Pair the plain `on_*` forms with `subscribe_executions()` / `subscribe_balances()`.

## trade namespace

All trade methods return `PendingTrade<Req, Resp>`. Chain `.via(Transport)` before awaiting. [Placing orders](guides/placing-orders.md).

### Shorthand methods

| Method | Description |
|--------|-------------|
| `market_buy(pair, volume)` | Market buy |
| `market_sell(pair, volume)` | Market sell |
| `limit_buy(pair, volume, price)` | Limit buy |
| `limit_sell(pair, volume, price)` | Limit sell |
| `stop_loss_buy(pair, volume, trigger_price)` | Stop-loss buy |
| `stop_loss_sell(pair, volume, trigger_price)` | Stop-loss sell |

### Full-control methods

| Method | Description |
|--------|-------------|
| `order(req: OrderRequest)` | Any order type (`Side` on the request) |
| `order_amend(req: OrderAmendRequest)` | Amend volume, limit price, post-only, trigger price, display qty. `display_qty` is REST-only; WS returns `TradeError::WsUnsupportedOrderField` |
| `cancel(cl_ord_id: ClOrdId)` | Cancel one order |
| `cancel_batch(cl_ord_ids: Vec<ClOrdId>)` | Cancel N orders (REST fan-out of `/CancelOrder`). `.via(Transport::WsV2Auth)` is `TradeError::UnsupportedTransport` |
| `cancel_all()` | Cancel all open orders |
| `cancel_all_orders_after(timeout: u32)` | Dead-man: cancel all after N seconds of inactivity |
| `order_batch(req: AddOrderBatchRequest)` | Place 2–15 orders. Per-row `txid` or `error` |

Targeted cancel and amend use `cl_ord_id` only. Orders without one are reachable only via `cancel_all` / `cancel_all_orders_after`. No txid-keyed cancel/amend in v1.

### Order types (`OrderType`)

`Market`, `Limit`, `Iceberg`, `StopLoss`, `StopLossLimit`, `TakeProfit`, `TakeProfitLimit`, `TrailingStop`, `TrailingStopLimit`, `SettlePosition`, `Unknown`.

`Unknown` is decode-only. Do not send it.

`Iceberg` is a hidden-size limit: primary-only, limit-priced, fill-or-kill eligible, with `display_vol`. Not valid as a conditional-close type.

### Order prices (`Price` / `PriceUnit`)

`price`, `price2`, and `ConditionalClose` legs take `Price`:

| Value | Meaning | REST | WS (`price_type`) |
|-------|---------|------|-------------------|
| `Price::Absolute(amount)` | Fixed price. `Decimal` converts via `.into()`. | `30000` | `static` |
| `Price::Offset { unit: PriceUnit::Quote, value }` | Signed quote offset | `+150` / `-150` | `quote` |
| `Price::Offset { unit: PriceUnit::Percent, value }` | Signed percent offset | `+1.0%` / `-2.0%` | `pct` |

`Price::Offset` on a conditional-close is REST-only. WS v2 has no `price_type` on the conditional object; relative close on WS is `TradeError::WsUnsupportedOrderField`. Absolute close prices work on both transports. Trailing/primary legs carry `price_type` on WS.

### Trailing stops

`TrailingStop` / `TrailingStopLimit` go through `order(req)`. Trailing `price` must be a positive `Price::Offset`. Direction follows `Side`. For `TrailingStopLimit`, `price2` is the limit-leg offset and may be `+` or `-`. Absolute or negative trailing `price` is `TradeError::InvalidOrder` before send.

### Margin orders

`margin: bool` on `OrderRequest` / `BatchOrderEntry` funds at the pair's maximum leverage. WS-only. REST uses numeric `leverage`; `margin: true` on REST is `TradeError::RestUnsupportedOrderField`. `reduce_only: Some(true)` requires `leverage > 1` or `margin: true`.

### PendingTrade

| Method | Description |
|--------|-------------|
| `.via(Transport)` | `Transport::Rest` or `Transport::WsV2Auth` |
| `.cl_ord_id()` | `Option<&ClOrdId>` before `.await`. `Some` for place / cancel / amend. `None` for `cancel_all`, deadman, and `order_batch` |

Default transport is auth WS. `.with_prefer_rest_for_orders(true)` flips the session default. Per-call `.via(...)` wins.

## subscription namespace

Use `on_*_for` when handler and subscribe should be one call.

| Method | Description |
|--------|-------------|
| `subscribe_ticker(pairs, snapshot, event_trigger)` | Ticker |
| `subscribe_book(pairs, depth)` | Maintained book |
| `subscribe_book_raw(pairs, depth, snapshot)` | Raw book deltas |
| `subscribe_trade(pairs, snapshot)` | Trades. `snapshot = Some(true)` pulls recent history |
| `subscribe_ohlc(pairs, interval, snapshot)` | OHLC |
| `subscribe_system_status()` | System status |
| `subscribe_executions()` | Execution reports (auth WS) |
| `subscribe_balances()` | Balance updates (auth WS) |
| `unsubscribe_ticker(pairs)` | Unsubscribe |
| `unsubscribe_book(pairs)` | Unsubscribe |
| `unsubscribe_book_raw(pairs)` | Unsubscribe |
| `unsubscribe_trade(pairs)` | Unsubscribe |
| `unsubscribe_ohlc(pairs, interval)` | Unsubscribe |
| `unsubscribe_system_status()` | Unsubscribe |
| `unsubscribe_executions()` | Unsubscribe |
| `unsubscribe_balances()` | Unsubscribe |
| `unsubscribe_channel(channel)` | Force-unsubscribe one channel. Ignores refcounts. `BookRaw` tears down shared `Book` entries |
| `unsubscribe_all()` | Force-unsubscribe all channels |
| `list_active()` | Snapshot: channel, pair, state (`Active` / `Pending` / `Failed`) |
| `find_by_channel(channel)` | Per-channel refs plus `registered_at_monotonic` |
| `status_summary()` | Counts (`total = active + pending + failed`) and per-channel histogram |

`subscribe_*` returns `Err(SubscriptionError::NoHandlerRegistered)` if no handler is registered for the channel.

Per-channel `unsubscribe_*` decrements the `(channel, pair)` refcount. Call once per matching `subscribe_*`. `unsubscribe_book` and `unsubscribe_book_raw` share `(book, pair)`; call the one that matches the subscribe. `unsubscribe_channel` and `unsubscribe_all` ignore refcounts.

Inspect: [Streaming → Inspecting subscriptions](guides/streaming.md#inspecting-subscriptions). Termination and gaps: [Events](events.md) (`SubscriptionTerminatedEvent`, `SubscriptionGapEvent`).

## events namespace

`client.events().on(EventType, callback)` returns `Result<EventSubscription, EventsError>`. Drop the guard to unsubscribe.

| EventType | Description |
|-----------|-------------|
| `ConfigResolved` | Winning source per knob at `.build()` |
| `ConfigChangedEvent` | After `client.set_knob(...)` |
| `ClientReady` | I/O reactor started |
| `ClientFailed` | `ready()` failure path (`ClientFailureCause`). `ready().await` resolves on `ClientReady` or `ClientFailed` |
| `LoopFailedEvent` | First reactor-loop death. In-flight lifecycle awaits fail |
| `RateLimitWarning` | Counter at or above warning threshold |
| `QueueFullWarning` | I/O→dispatch drop-oldest. Caller→I/O full is `QueueFull`, not this event |
| `ConnectionConnectingEvent` / `ConnectionOpenEvent` / `ConnectionClosedEvent` / `ConnectionReopenedEvent` / `ConnectionFailedEvent` | Connection lifecycle |
| `SubscriptionTerminatedEvent` / `SubscriptionGapEvent` / `OrderBookGapEvent` | Subscription and book integrity |
| `ChannelGapEvent` | Sequence jump on `executions` / `balances`. SDK does not auto-resubscribe |
| `OrderSubmittedEvent` / `OrderCancellationAttempted` / `OrderReconciliationEvent` / `OrderPlacementAmbiguousEvent` | Order lifecycle |
| `SlowCallbackWarning` | Callback ≥ `slow_callback_threshold_ms` (default 50). Observability handlers exempt |
| `QueueDepthSample` | `io_to_dispatch` depth (100 ms or 1000 dequeues) |
| `CallbackLatency` | After a callback returns. Data-callback samples only while subscribed |
| `HandlerPanicWarning` | Callback or reactor-side decode panicked. Other handlers still run |

`EventType` is `#[non_exhaustive]`. Full list and payloads: [Events](events.md). Match with `_`.

## Order book

- Maintained (`on_book` / `on_book_for`): `OrderBookUpdate` (`symbol`, `bids`, `asks`, `checksum`, `exchange_timestamp`). `timestamp` is always `None` in v1.
- Raw (`on_book_raw` / `on_book_raw_for`): `BookDelta` with `is_snapshot` and `PriceLevel`. Same timestamp split.

Both share one wire subscription per pair. CRC, gaps, depth: [Order book](guides/order-book.md).

## Rate limit behaviour

The SDK does not block on rate limits. It emits `RateLimitWarning` at a configurable threshold (default 80%). [Rate limits](guides/rate-limits.md).

## Embedding identity

REST requests from the SDK-built transport can carry extra headers:

```rust
use kraken_sdk::header::{HeaderMap, HeaderValue, USER_AGENT};

let mut headers = HeaderMap::new();
headers.insert("x-kraken-client", HeaderValue::from_static("my-app"));
headers.insert(USER_AGENT, HeaderValue::from_static("my-app/1.4.0"));

let client = Client::builder().with_headers(headers).build()?;
```

Default without `with_headers`: `User-Agent: kraken-sdk-rust/<version>` and `x-korigin: 12003`. A caller `user-agent` is prepended; the SDK token is always appended (`my-app/1.4.0 kraken-sdk-rust/<version>`). WebSocket upgrades carry the SDK token and `x-korigin: 12004`. `with_headers` is REST-only.

`HeaderMap` / `HeaderValue` reject CR/LF and invalid names. Headers are telemetry and must not confer privilege. Not env- or file-sourceable. Combined with `with_transport`: `ConfigError::HeadersRequireSdkTransport` (`HEADERS_REQUIRE_SDK_TRANSPORT`). Reserved names `api-key`, `api-sign`, `content-type`, `accept`, `host`, `x-korigin` are rejected at `.build()` (`ConfigError::ReservedHeaderName`, `RESERVED_HEADER_NAME`). The transport gate wins if both apply. `reqwest::header` is re-exported as `kraken_sdk::header`.

## Symbols

Modern slash form only: `"BTC/USD"`. `Symbol::new("XXBTZUSD")` is `Err(SymbolError::LegacyPrefix)`. `XBT`, `XXBT`, `ZUSD` are not accepted.

Pair existence is checked by the exchange at first use (`MarketError::SymbolNotFound`), not at `.build()`.

```rust
let symbol = Symbol::new("BTC/USD")?;  // Ok
let symbol = Symbol::new("XXBTZUSD");  // Err(SymbolError::LegacyPrefix)
```

## Error taxonomy

Each namespace has a typed error. All implement sealed `ApiError`:

| Method | Type | Description |
|--------|------|-------------|
| `code()` | `&str` | Stable identifier (e.g. `RATE_LIMIT_EXCEEDED`) |
| `category()` | `ErrorCategory` | `Config`, `Auth`, `Network`, `Exchange`, `Client`, or `RateLimit` |
| `retryable()` | `bool` | Cooperative retry is appropriate |
| `request_id()` | `Option<&str>` | REST UUID / WS `req_id`. `None` pre-dispatch |
| `message()` | `String` | Human-readable |
| `kraken_code()` | `Option<&str>` | Raw Kraken wire code, if any |

Types: `MarketError`, `AccountError`, `TradeError`, `SubscriptionError`, `EventsError`, `ConfigError`, `AuthError`, `RestError`, `TransportError` (`TransportErrorKind`), `SymbolError`. Public error enums are `#[non_exhaustive]`. [Error handling](guides/error-handling.md).

## Out of scope in v1

v1 is Spot trading and portfolio, crypto/forex only. These absences are intentional.

### Endpoints not exposed

| Kraken endpoint | Why |
|---|---|
| `GetApiKeyInfo` | API-key metadata |
| `OrderAmends` | Amend audit trail. Use `order_amend`; no history query |
| `transparency/*` — `PreTrade`, `PostTrade` | Outside v1 |
| `QueryOrders` / `QueryTrades` / `CreditLines` / `Export*` | Order identity is `cl_ord_id`, not txid |

### Parameters the SDK does not send

| Endpoint(s) | Param | Why |
|---|---|---|
| Balance · BalanceEx · TradeBalance · OpenOrders · ClosedOrders · Ledgers · QueryLedgers · TradesHistory · OpenPositions · TradeVolume | `rebase_multiplier` | xStocks / tokenized display. Those pairs are `MarketError::SymbolNotFound` |
| AddOrder · AddOrderBatch | `broker` | Partner IIBAN |
| ClosedOrders · Ledgers · TradesHistory | `without_count` | Not in v1 |
| ClosedOrders · TradesHistory | `consolidate_taker` | Not in v1 |
| OpenPositions | `consolidation` | Different row model |
| TradeVolume | `fee_schedule` | SDK sends `fee-info` only |
| AmendOrder | `deadline` | Not in v1 (AddOrder `deadline` likewise) |
