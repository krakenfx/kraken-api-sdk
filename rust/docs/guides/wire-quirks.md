# Wire quirks

REST and WebSocket edges the SDK absorbs. Use this when debugging a decode or comparing a raw capture.

## Connection

**Pair casing.** Subscription acks match the exact pair string. `Symbol` uppercases before it becomes the registry key, so `btc/usd` matches Kraken's `BTC/USD` echo.

**Pair in errors.** `Display` is the bare wire string (`BTC/USD`), same as `as_str`. `Debug` keeps the newtype.

**Auto-seeded status.** Kraken sends a `status` frame on every public WS open, before subscribe. A handler registered before the socket opens receives it:

```rust
use kraken_sdk::SystemStatusUpdate;

let _ss = client.market().on_system_status(|s: SystemStatusUpdate| {
    println!("system={:?} version={:?}", s.system, s.version);
});
client.ready().await?;
```

A later handler misses that frame. Kraken re-pushes status periodically. Subscribe quirk: [Streaming → System status](streaming.md#system-status).

## Market-data streaming

**Ticker numbers.** Docs say decimal-strings; live wire often uses JSON numbers. The decoder accepts both.

**Book checksum.** CRC32 uses `price_wire` / `qty_wire`, not `Decimal::to_string()`. [Order book → Checksum](order-book.md#checksum).

## Order placement

**Validate responses.** `validate=true` places nothing.

- REST `AddOrder`: description, no `txid`. A real place always has `txid`.
- WS v2 `add_order`: `order_id=""` and no `descr`. Empty id → `txid = None`.

**WS `add_order` spellings.** `order_userref` (not `userref`), `conditional` (not `close`). `leverage` is rejected. Probe with `validate:true`. Unsupported field: `Unsupported field: '<name>'`.

**Leverage on WS.** Schema has `margin: true` and no leverage ratio. Pre-send: `WsUnsupportedOrderField { field: "leverage" }`. Use REST.

**`deadline`.** RFC3339 with timezone; wire `deadline` must be within 60 s. A set `deadline` is rejected at `validate()` in v1 on every transport. [Placing orders](placing-orders.md).

**`starttm` / `expiretm`.** Stored once as Unix seconds. REST sends the epoch integer. WS v2 sends RFC 3339 with timezone (`effective_time` / `expire_time`). Relative `+60` is not modelled. Negative epoch is clamped to the epoch; the exchange then rejects the past time.

**Price absolute vs offset.** One `Price` type; each transport renders:

| Intent | REST | WS |
|--------|------|----|
| Absolute | `"30000"` | value + `price_type = "static"` |
| Quote offset | `"+150"` / `"-150"` | signed numeric + `price_type = "quote"` |
| Percent offset | `"+1.0%"` / `"-2.0%"` | signed numeric + `price_type = "pct"` |

Non-negative relative REST prices need an explicit `+`. REST `#` auto-direction is not modelled.

**AddOrderBatch.** Placed: `{"orders":[{"descr":{"order":"..."},"txid":"O..."}, ...]}`. Per-entry `txid` is a **scalar** (single `/AddOrder` uses a `txid` array). Validate: no `txid`, descr-only. Per-line rejection: top-level `error` empty; that entry has `{"error":"..."}`; siblings stay live. Rows map 1:1 by position.

## REST decode — account

**`leverage`.** String. Non-margin: `"none"`. Margin: `"5:1"`. Kept as `String`.

**`aclass` vs `class`.** TradesHistory: `aclass`. OpenPositions: `class` → `asset_class`.

**`rollovertm`.** Stringified integer epoch (`"1716803600"`). Kept as `String`.

**`KrakenTimestamp`.** REST timestamps are JSON numbers (epoch seconds with fraction). Stored as the exact digit string, not `f64`. `as_str()` for reconciliation; `to_f64()` is lossy. Distinct from event monotonic time. Exception: `OpenPositionEntry::rollovertm` above.

**TradesHistory.** 15 fields spot, 16 margin (`posstatus` = `"open"`). `tradeordertype` ≠ `ordertype` on triggered fills. `trade_id` is `u64`. `maker` is bool. Asset-class key is `aclass`. `leverage` is `Decimal` (`"0"` for spot).

**OpenPositions.** Bare `result` map by position id (no `open` wrapper). `terms` is free-text (`"0.0100% per 4 hours"`). `value` and `net` are `Some` only when `docalcs` was requested.

**`open_orders` delay.** A just-placed order may be absent briefly.

**Paginated `count`.** Grand total across pages, not page size (`closed_orders` and similar).

**Volume.** `account().volume(pair, fee_info)` sends `fee_info`. The fee schedule is not decoded.

## REST decode — market

**Legacy X/Z codes.** Decode: `XXBT` → `BTC`, `ZUSD` → `USD`; `USDC` unchanged. Legacy codes are not map keys. `altname` may still hold the short form. Input: table-driven reject of legacy forms. Codes are uppercased first. Modern codes that start with `X` or `Z` (`XRP`, `XLM`, `XMR`, `ZEC`, `XTZ`) are accepted.

**AssetPairs `status` / slashless keys.** [Market data](market-data.md#assetpairs-optional-fields).

**`last` cursors.**

- Trades: decimal nanosecond string, opaque. Empty `last` is end-of-pagination. Round-trips as `String`.
- OHLC: `u64`. Malformed → `MalformedResponse`.
- Spreads: JSON integer (epoch seconds), exposed as a string cursor.

## Related

- [Streaming](streaming.md)
- [Placing orders](placing-orders.md)
- [Ticker](ticker.md)
- [Order book](order-book.md)
- [Error handling](error-handling.md)
