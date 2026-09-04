# Placing Orders

## PendingTrade

Every trade method returns `PendingTrade<Req, Resp>`. The wire call fires on `.await`. Chain before awaiting:

```rust
let pending = client.trade().limit_buy(pair, volume, price);

let id = pending.cl_ord_id().cloned();

let resp = pending
    .via(Transport::Rest)
    .await?;
```

Default transport is WebSocket v2. Await `client.ready()` before a WS order.

Session-wide REST: `Client::builder().with_prefer_rest_for_orders(true)` (or `prefer_rest_for_orders` / `KRAKEN_PREFER_REST_FOR_ORDERS`). Construction-only. Per-call `.via(...)` still wins.

## Shorthand methods

```rust
use kraken_sdk::{Client, Symbol};
use rust_decimal::Decimal;
use std::str::FromStr;

let pair = Symbol::new("BTC/USDC")?;
let vol = Decimal::from_str("0.0001")?;

client.trade().market_buy(pair.clone(), vol).await?;
client.trade().market_sell(pair.clone(), vol).await?;

let price = Decimal::from_str("50000")?;
client.trade().limit_buy(pair.clone(), vol, price).await?;
client.trade().limit_sell(pair.clone(), vol, price).await?;

let trigger = Decimal::from_str("45000")?;
client.trade().stop_loss_buy(pair.clone(), vol, trigger).await?;
client.trade().stop_loss_sell(pair.clone(), vol, trigger).await?;
```

## Full-control placement

`order(req)` with `OrderRequest` and `Side::{Buy, Sell}`. `OrderType::Unknown` is decode-only; placing it is `TradeError::InvalidOrder`.

### Limit with time-in-force and post-only

```rust
use kraken_sdk::{Client, OrderRequest, OrderType, Side, Symbol, TimeInForce};
use rust_decimal::Decimal;
use std::str::FromStr;

let req = OrderRequest::new(
    Symbol::new("BTC/USD")?,
    Decimal::from_str("0.0001")?,
    Side::Buy,
)
.order_type(OrderType::Limit)
.price(Decimal::from_str("50000")?)
.time_in_force(TimeInForce::Gtc)
.post_only(true);
// Equivalent to `.oflags(vec![OFlag::Post])` (import `OFlag`).

let resp = client.trade().order(req).await?;
println!("placed txid={:?} cl_ord_id={:?}",
    resp.txid.as_ref().map(|t| t.as_str()),
    resp.cl_ord_id.as_ref().map(|c| c.as_str()));
```

`TimeInForce`: `Ioc`, `Gtd`, `Fok`. FOK is valid only on limit-priced types (limit, iceberg, stop-loss-limit, take-profit-limit, trailing-stop-limit). Otherwise `TradeError::InvalidOrder`.

### Iceberg

Limit that shows `display_vol`. Both transports. Not a valid conditional-close type.

```rust
let req = OrderRequest::new(pair.clone(), vol, Side::Buy)
    .order_type(OrderType::Iceberg)
    .price(Decimal::from_str("50000")?)
    .display_vol(Decimal::from_str("0.0001")?);
let resp = client.trade().order(req).await?;
```

### Self-trade prevention

```rust
use kraken_sdk::{OrderRequest, OrderType, Side, StpType, Symbol};

let req = OrderRequest::new(pair.clone(), vol, Side::Buy)
    .order_type(OrderType::Limit)
    .price(price)
    .stp_type(StpType::CancelNewest);
let resp = client.trade().order(req).await?;
```

### Conditional close (OTO bracket)

Auto-allocation of `cl_ord_id` is suppressed when a conditional close is present (the exchange rejects both together).

```rust
let req = OrderRequest::new(pair.clone(), vol, Side::Buy)
    .order_type(OrderType::Limit)
    .price(Decimal::from_str("50000")?)
    .conditional_close(ConditionalClose::new(
        CloseOrderType::Limit,
        Decimal::from_str("60000")?,
    ));

let resp = client.trade().order(req).await?;
```

`CloseOrderType`: `Limit`, `StopLoss`, `TakeProfit`, `StopLossLimit`, `TakeProfitLimit`, `TrailingStop`, `TrailingStopLimit`. Trigger price is required. A trailing close follows the primary trailing rule (positive relative offset → REST-only).

Relative close (`Price::Offset`) is REST-only. WS v2 conditional has no `price_type`; WS path is `TradeError::WsUnsupportedOrderField`. Use `.via(Transport::Rest)` or an absolute close on WS.

Batch: `.conditional_close(ConditionalClose::new(CloseOrderType::Limit, price))` on `BatchOrderEntry`.

### Trailing stop

Trailing `price` must be a positive `Price::Offset`. Direction follows buy/sell. Absolute or negative offset: `TradeError::InvalidOrder` before send. For `TrailingStopLimit`, `price2` is the limit-leg offset and may be `+` or `-`.

```rust
use kraken_sdk::{OrderRequest, OrderType, Price, PriceUnit, Side, TriggerKind};

let req = OrderRequest::new(pair.clone(), vol, Side::Buy)
    .order_type(OrderType::TrailingStop)
    .price(Price::Offset {
        unit: PriceUnit::Quote,
        value: Decimal::from_str("100")?,
    })
    .trigger(TriggerKind::Last);

let resp = client.trade().order(req).await?;
```

### Margin

- `leverage: Option<u8>` — REST-only. WS: `TradeError::WsUnsupportedOrderField`.
- `margin: bool` — pair max leverage. WS-only. REST: `TradeError::RestUnsupportedOrderField`.

Both, or `margin` plus another REST-only field: `TradeError::InvalidOrder`. `reduce_only` requires `leverage > 1` or `margin: true`.

### Validate mode

```rust
let req = OrderRequest::new(pair.clone(), vol, Side::Buy)
    .order_type(OrderType::Limit)
    .price(price)
    .validate_only(true);

let resp = client.trade().order(req).await?;
```

Acceptance: parsed `descr`, `txid` is `None`. WS validate: `order_id: ""` → `txid = None`. Shorthands have no `validate` flag; use the equivalent `OrderRequest`.

Batch `validate_only(true)`: per-row `{ validation: ... }` (no `txid`, no `error`). Whole-batch validation failure: null result, `Err`.

### Client-side validation

`validate()` runs before serialization:

- **`ConflictingOrderIdentifiers`** — both `userref` and `cl_ord_id`.
- **`SettlePositionRequiresLeverage`** — `settle-position` without leverage ≥ 1.
- **`ReduceOnlyRequiresLeverage`** — `reduce_only` without margin.
- **`InvalidOrder` (transport conflict)** — WS-only `margin` plus REST-only `leverage` or relative close offset.
- **`InvalidOrder` (fill-or-kill)** — `fok` on a non-limit-price type.
- **`InvalidOrder` (`OrderType::Unknown`)** — decode-only; cannot be placed.

All gates except `ConflictingOrderIdentifiers` apply per `AddOrderBatchRequest` entry, plus batch size 2–15 (`BatchSizeOutOfRange`). Transport conflict is batch-wide: any `margin` plus any `leverage` / relative close fails the whole batch.

## Amend

Changes limit price, volume, post-only, trigger price, or iceberg display qty. Same txid and queue priority. Identity is `cl_ord_id`. Empty amend: `TradeError::EmptyAmendRequest`.

```rust
use kraken_sdk::{OrderAmendRequest, Price, PriceUnit};

let req = OrderAmendRequest::new(existing_cl_ord_id)
    .limit_price(Decimal::from_str("51000")?);
// .limit_price(Price::Offset { unit: PriceUnit::Quote, value: Decimal::from_str("150")? })

let resp = client.trade().order_amend(req).await?;
```

Fields: `order_volume`, `limit_price`, `post_only`, `trigger_price`, `display_qty` (REST-only; WS: `WsUnsupportedOrderField`). `limit_price` and `trigger_price` take `Price`. `deadline` on `OrderRequest` / `AddOrderBatchRequest` is rejected (`InvalidOrder`); no setter.

Reply carries `amend_id`. `TradeError::NoAmendableParameters` — no-op (e.g. sub-tick). `TradeError::UnknownOrder` — no open order for that `cl_ord_id`.

## Cancel

### One

```rust
let resp = client.trade().cancel(cl_ord_id).await?;
println!("cancelled count={}", resp.count);
```

### Batch

`cancel_batch` is REST-only fan-out: N concurrent `/CancelOrder` by `cl_ord_id` (Kraken's batch-cancel is txid-only). Results 1:1 in input order. No wire size limit. No throttle. Per-line rate-limit is that line's `BatchResult::Err`. Nonces stay monotonic. No validate mode.

```rust
use kraken_sdk::{BatchResult, ClOrdId};

let results = client.trade()
    .cancel_batch(vec![cl_a, cl_b, cl_c])
    .await?;

for (i, r) in results.results.iter().enumerate() {
    match r {
        BatchResult::Ok(c)  => println!("line[{i}] cancelled: count={}", c.count),
        BatchResult::Err(e) => println!("line[{i}] failed: {}", e.message()),
        _ => {} // BatchResult is #[non_exhaustive]
    }
}
```

### All

```rust
let resp = client.trade().cancel_all().await?;
println!("cancelled {} orders", resp.count);
```

### Dead-man

Reset by calling again. Disarm with `0`.

```rust
client.trade().cancel_all_orders_after(60).await?;
client.trade().cancel_all_orders_after(0).await?;
```

Armed reply: RFC 3339 `trigger_time`. Disarmed: `"0"` or omitted → `trigger_time = None`.

## Batch place

2–15 orders. Per-line: rejected row has `error`; siblings keep `txid`. Call may be `Ok` on a partial place; inspect each row.

Default WS (`batch_add`). `.via(Transport::Rest)` → `POST /0/private/AddOrderBatch`. WS leaves each row's `descr` empty. `leverage` or relative close: whole call `WsUnsupportedOrderField` unless REST. `margin: true`: `RestUnsupportedOrderField` on REST. Mix: `InvalidOrder`.

WS await timeout mid-batch is send-ambiguous. Reconcile, or use REST. Per-entry `cl_ord_id` auto-allocates unless `userref` or `conditional_close`. `AddOrderBatchRequest::validate()` does not check identifier conflicts per entry; that suppression is the only guard.

```rust
use kraken_sdk::{AddOrderBatchRequest, BatchOrderEntry, ClOrdId, OrderType, Side, Symbol};
use rust_decimal::Decimal;
use std::str::FromStr;

let pair = Symbol::new("BTC/USDC")?;
let req = AddOrderBatchRequest::new(
    pair.clone(),
    vec![
        BatchOrderEntry::new(OrderType::Limit, Side::Buy, Decimal::from_str("0.0001")?)
            .price(Decimal::from_str("49000")?)
            .cl_ord_id(ClOrdId::allocate_v4()),
        BatchOrderEntry::new(OrderType::Limit, Side::Buy, Decimal::from_str("0.0001")?)
            .price(Decimal::from_str("48000")?)
            .cl_ord_id(ClOrdId::allocate_v4()),
    ],
);

let resp = client.trade().order_batch(req).await?;
for (i, o) in resp.orders.iter().enumerate() {
    println!("entry[{i}] txid={:?}", o.txid.as_ref().map(|t| t.as_str()));
}
```

[`examples/batch_orders.rs`](../../examples/batch_orders.rs).

## Client order id (cl_ord_id)

`order(req)` and each `order_batch` entry allocate a UUID v4 `cl_ord_id` unless one is supplied. Read it on `PendingTrade` before `.await`.

```rust
let pending = client.trade().limit_buy(pair.clone(), vol, price);
let id = pending.cl_ord_id().cloned().expect("auto-allocated");
let resp = pending.await?;
assert_eq!(resp.cl_ord_id, Some(id));
```

Suppressed when `userref` or a conditional-close is present. WS `add_order` echoes the sent `cl_ord_id`; none if suppressed.

## Idempotency and reconciliation

An order is not idempotent. The SDK does not resend. A transient send failure (e.g. token expired at compose) refreshes state and returns a retryable `Network` error; the caller decides whether to resubmit. Subscribe frames replay after reconnect; orders do not.

Dropping an in-flight order `await` after first poll does not guarantee the order was not sent. The bus emits `OrderCancellationAttempted` with `cl_ord_id` (single-`cl_ord_id` ops only; never `cancel_all` / `cancel_all_orders_after`).

```rust
use kraken_sdk::EventType;

let _guard = client.events().on(EventType::OrderCancellationAttempted, |env| {
    eprintln!("cancellation attempted: {:?}", env.event_type);
})?;
```

Reconcile with `client.account().find_order_by_cl_ord_id(cl_ord_id)`.

### Walk

1. `POST /OpenOrders { cl_ord_id }`. Non-empty `open` → `Found(txid, status, Open)`.
2. Else `POST /ClosedOrders { cl_ord_id }`. Non-empty `closed` → `Found(txid, status, Closed)`.
3. Both empty → `NotPlaced`.

Match is server-filtered map non-empty. Unrecognized status → `Unknown`. Each leg cost +2. Emits `OrderReconciliationEvent { cl_ord_id, outcome }` once.

### Events

Cancellation guard arms immediately before the wire `.await` for `add` / `amend` / `cancel_order`. Drop before that await: no event. Drop in flight: `OrderCancellationAttempted`.

WS reply:

- Place with txid → `WireSent { txid }`. Validate (no txid): no event.
- Amend / cancel success → `WireAccepted` (amend carries `amend_id`).
- `success == false` → `WireError { code }`.
- Definitely-not-sent (including pre-record queue-full) → no event.
- In-flight / unknown (`add` / `amend`) → `OrderPlacementAmbiguousEvent`.

## Transport notes

| Topic | REST | WS v2 |
|-------|------|-------|
| Limit price | `price` | top-level `limit_price` |
| Trigger types | trigger in `price`; no separate `trigger` key | nested `triggers`; `*Limit` also sets `limit_price` |
| `price_type` | n/a | per-`Price`: `static` / `quote` / `pct` |
| Unset trigger reference | omit | omit; Kraken defaults `last` |
| `cl_ord_id` | string | scalar on add/amend; **array** on cancel |
| Qty / price JSON | numbers | numbers |
| STP spelling | `cancel-both` | `cancel_both` (`StpType` is the same) |
| Conditional close | REST `close` | `trigger_price` + `limit_price` (stop types); `limit_price` only (Limit). Trailing close REST-only. `Market` / `SettlePosition` not valid closes. |
| Place id | `txid` array | `order_id` → SDK `txid` |
| Cancel success | count/pending | `{ "cl_ord_id": "<echoed>" }` → `count: 1`, `pending: false`. Missing echo: `MalformedResponse`. Already-gone: `success: false`, `EOrder:Invalid order`. |
| `req_id` | n/a | JSON number; string echo does not correlate (timeout). |
| `cancel_all` | `count` required | `result.count` required; missing/non-integer: `MalformedResponse`, not `0`. |

Relative Quote offset emits `price_type: "quote"` even on a non-trailing stop-loss.

## Related

- [`examples/advanced_orders.rs`](../../examples/advanced_orders.rs)
- [`examples/shorthand_orders.rs`](../../examples/shorthand_orders.rs)
- [`examples/batch_orders.rs`](../../examples/batch_orders.rs)
- [Error handling](error-handling.md)
