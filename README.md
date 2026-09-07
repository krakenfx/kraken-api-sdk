# Kraken Unified SDK

Client libraries for Kraken's APIs. Each SDK is a typed, async client with
methods grouped into market data, account, trading, and subscriptions. It takes
care of request signing, connections and reconnects, and order-book maintenance.

One SDK design, implemented per language under its own top-level directory.

## Features

- One client for market data, account, trading, and subscriptions
- Spot REST and Spot WebSocket v2
- Request signing, nonce handling, and WebSocket token refresh
- Reconnect with resubscribe
- Order books built from snapshot and deltas, CRC32-checked on each update
- Prices, volumes, and balances as decimals, not floating point
- Errors with `code`, `category`, `retryable`, `request_id`, and `message`
- Events for connection and client status, order lifecycle, rate limits,
  stream and book gaps, queue depth, and callback latency
- API keys kept in memory and cleared when the client closes
- Pair names in modern format (`BTC/USD`)

## Languages

| Language | Path | Status |
|----------|------|--------|
| Rust + tokio | [`rust/`](rust/) | Spot trading + portfolio. |
| C++ · Python · Go · TypeScript/JS | _(coming)_ | Coming soon. |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for general contribution guidelines.
Build, test and code-style rules are per binding and live in that language's own
guide.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Disclaimer

See [DISCLAIMER.md](DISCLAIMER.md).

## Trademark

See [TRADEMARK.md](TRADEMARK.md).
