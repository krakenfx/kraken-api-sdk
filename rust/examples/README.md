# Examples

Runnable programs demonstrating the SDK. Run any with:

```sh
cargo run --example <name>
```

Examples marked **creds** need a Kraken API key/secret (read from the
environment / a secrets manager — never hard-code them). Examples marked
**public** need no credentials. WebSocket examples connect to Kraken's public or
authenticated streams as noted.

## Start here

| Example | Needs | Shows |
|---|---|---|
| `quickstart` | public | Four-step tour: REST ticker, then (with creds set) balance, live WebSocket ticker, and a validate-mode order. |
| `ticker` | public | Fetch a ticker over REST. |
| `orderbook` | public | Fetch an order-book snapshot over REST. |
| `balance` | creds | Read account balances. |

## REST

| Example | Needs | Shows |
|---|---|---|
| `rest_market` | public | The public market-data REST surface. |
| `rest_account` | creds | The private account-read REST surface. |
| `two_factor_auth` | creds | Signed REST + WS with a 2FA one-time password (`with_otp`). |

## Trading

| Example | Needs | Shows |
|---|---|---|
| `shorthand_orders` | creds | `market_buy/sell`, `limit_buy/sell`, `stop_loss_*` shorthands. |
| `advanced_orders` | creds | Generic `order(req)`, conditional close, order flags, amend. |
| `batch_orders` | creds | Placing and cancelling orders in batches. |
| `cleanup_orders` | creds | Cancel resting orders (a handy account-reset utility). |

## Streaming (WebSocket)

| Example | Needs | Shows |
|---|---|---|
| `live_streams` | public | Multiple public channels at once, including a Bbo-triggered ticker. |
| `live_book` | public | The maintained, CRC-validated order book (`on_book`). |
| `account_streams` | creds | Authenticated executions / balances streams. |

## Putting it together

| Example | Needs | Shows |
|---|---|---|
| `strategy_engine` | public | A deliberately dumb mean-reversion loop composing ticker updates, execution updates, and lifecycle events into one decision loop. With creds set it sends validate-only orders — nothing is ever placed. |

## Configuration & resilience

| Example | Needs | Shows |
|---|---|---|
| `config_knobs` | public | Runtime knobs (`knob` / `set_knob`) and config sources. |
| `reconnect_resilience` | public | Reconnect / resubscribe behaviour on disconnect. |

## Internal harnesses (not teaching examples)

These exercise the SDK end-to-end for development/QA rather than illustrating
idiomatic usage. They require credentials, and `live_e2e` / `trade_smoke` can
**place real orders** — do not run them casually.

| Example | Needs | Notes |
|---|---|---|
| `trade_smoke` | creds | Trading smoke check — **places real orders**. |
| `live_e2e` | creds | Full end-to-end harness — **places real orders**. |
