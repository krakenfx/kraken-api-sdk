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

## Related

- [Ticker](ticker.md)
- [Order book](order-book.md)
- [Wire quirks](wire-quirks.md)
