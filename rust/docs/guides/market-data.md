# Market Data

Public REST decoders absorb wire mismatches: omitted “required” fields, mixed string/number types, legacy asset codes. Typed models after decode:

## AssetPairs optional fields

`/AssetPairs` documents some fields as always present. Live rows can omit them. The SDK decodes those as optional so one incomplete pair does not fail the batch:

- `status` — omitted on a minority of listed pairs (observed ~13%, 2026-06). Absence is tolerated.
- `lot_multiplier` — omission is tolerated. A present but wrong type or range still fails that row's decode.

## AssetPairs position limits

`long_position_limit` / `short_position_limit` are absent on non-marginable pairs (`Option`). Present values are in lots and can exceed `u32::MAX` (observed in the billions). Decoded as `u64`.

## AssetPairs legacy codes and slashless keys

- `base` / `quote` / `fee_volume_currency` arrive as X/Z codes (`ZUSD`, `XXBT`). Normalized to `AssetCode`. Legacy aliases are not surfaced.
- A new pair can use a slashless map key (`RENDERUSD`). The SDK rebuilds `BASE/QUOTE` from base and quote; if those are missing it keeps the wire key.

## Assets

`collateral_value` on `/Assets` is a JSON string or number. Both decode. Anything else is malformed.

## Trades rows

Each `/Trades` row is `[price, volume, time, side, type, misc, trade_id]`.

- `time` has more digits than `f64`. Kept as the exact wire token (string).
- `side` is `"b"` or `"s"`.
- `type` is a single-character order-type token, kept raw.

## Unified Futures order books

Futures symbols are native wire strings such as `PI_XBTUSD`. Handler registration is fallible: keep the `HandlerHandle` only after `on_book` / `on_book_raw` returns `Ok`.

- `on_book` yields `FuturesOrderBookUpdate`, a snapshot-seeded, CRC32-maintained book. A checksum mismatch emits `FuturesOrderBookGapEvent` and `FuturesSubscriptionGapEvent`, then reseeds.
- `on_book_raw` yields `FuturesBookDelta` exactly per frame. It keeps no state, validates no checksum and does not recover gaps.

`on_book_for(symbols, depth, handler)` always requests `snapshot: true`. `on_book_raw_for(symbols, depth, snapshot, handler)` passes the snapshot option through. Both use Unified Public only.

### Maintained/raw coexistence

Unified Public has one `book` wire subscription per Futures symbol. The SDK reserves its effective wire intent before it posts `subscribe_book`, `subscribe_book_raw`, `on_book_for` or `on_book_raw_for`.

| Consumer | Effective wire depth | Snapshot | Coexists with |
| --- | --- | --- | --- |
| Maintained `D10` | `25` (caller output stays capped at `10`) | `true` | Raw `D25` + `Some(true)` or `None` |
| Maintained other depth | caller depth | `true` | Raw with the same depth + `Some(true)` or `None` |
| Raw | caller depth | caller option, unchanged (`None` matches as `true`, the venue default) | An exactly matching effective intent |

- Matching parameters share the physical stream.
- Maintained owners of a symbol share one book and must use the same depth.
- An incompatible depth or snapshot returns `SubscriptionError::InvalidArguments` before a frame is queued.
- Raw requests keep their depth and snapshot; they are never widened to match a maintained request.
- Dropping or unsubscribing one owner leaves the stream live while another owner remains. The last owner sends the physical unsubscribe. `unsubscribe_channel` and `unsubscribe_all` follow the same rule.
- A raw owner that joins a live maintained stream attaches at once and receives updates from the join point, with no opening snapshot. Subscribe the raw owner first if its opening snapshot matters.
- A maintained owner that joins later reseeds the shared stream.

## Unified Futures candles

`FuturesOhlcUpdate` fields:

- `symbol`: native `String` wire symbol.
- `open`, `high`, `low`, `close`, `volume`, `vwap`: `Decimal`. `trades`: `u64`.
- `interval`: minutes. `interval_begin`: RFC 3339 candle start. `tick_type`.
- The venue's deprecated per-candle end time is not exposed.

Register `on_ohlc` / `on_ohlc_for`, then subscribe with `subscription().futures().subscribe_ohlc(symbols, interval, snapshot, tick_type)`. `interval` is one of the `OhlcInterval` values Spot uses.

- Updates arrive on every trade, not once per candle.
- The opening snapshot is one row per symbol: the current candle.
- `tick_type` `None` or `Some(FuturesOhlcTickType::Trade)`: trade-priced candles. `Some(FuturesOhlcTickType::Mark)`: mark-price candles, with `volume` and `vwap` of zero.

Each symbol, interval and tick type is its own subscription, so one symbol can stream several intervals and both tick types at once. `on_ohlc_for` delivers only its own symbols, interval and tick type; `on_ohlc` delivers every candle. `unsubscribe_ohlc(symbols, interval, tick_type)` releases one. A subscription held by an `on_ohlc_for` guard is not released by `unsubscribe_ohlc`; drop the guard. [Unsubscribe contract](streaming.md#unsubscribe-contract).

## Unified Futures tickers

`FuturesTickerUpdate` is a non-exhaustive enum keyed by the native symbol prefix:

| Variant | Symbols | Notes |
| --- | --- | --- |
| `Perpetual` | `PI_`, `PF_` | Has funding fields |
| `ExpiringFixed` | `FF_` | `FuturesExpiringTickerUpdate` |
| `ExpiringInverse` | `FI_` | `FuturesExpiringTickerUpdate` |

Match the variant; the variants do not share a common shape. On every variant, `ask`, `ask_qty`, `bid`, `bid_qty`, `change`, `change_pct`, `high`, `last`, `low`, `open` and `vwap` are `Option<Decimal>`. `None` means the venue omitted the field (no trades in the window, or an empty book side) or sent `null`. `symbol()` returns the native `String` wire symbol.

## Related

- [Ticker](ticker.md)
- [Order book](order-book.md)
- [Wire quirks](wire-quirks.md)
