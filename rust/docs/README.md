# Documentation

Async Rust client for Kraken Spot REST and Spot WebSocket v2.

| Page | Description |
|------|-------------|
| [Getting Started](getting-started.md) | Install, build a client, first REST and WS calls |
| [Features](features.md) | Namespaces, methods, order types, v1 scope |
| [Events](events.md) | Event catalog: when each fires, payload fields |
| [Architecture](architecture.md) | Two loops, lazy sockets, config lock, routing |
| [Development](development.md) | Module map, bus internals, where to add things |

## Guides

| Guide | Description |
|-------|-------------|
| [Ticker](guides/ticker.md) | REST and WebSocket ticker |
| [Market Data](guides/market-data.md) | Public REST decode quirks |
| [Placing Orders](guides/placing-orders.md) | Place, amend, cancel, batch, reconcile |
| [Order Book](guides/order-book.md) | REST snapshot; maintained and raw streams |
| [Streaming](guides/streaming.md) | Subscribe patterns, guards, reconnect |
| [Error Handling](guides/error-handling.md) | `ApiError`, namespace enums, retry |
| [Configuration](guides/configuration.md) | Priority, knobs, env, TOML |
| [Rate Limits](guides/rate-limits.md) | Counters, tiers, costs, snap-to-cap |
| [Wire Quirks](guides/wire-quirks.md) | REST/WS edges the SDK absorbs |

## Scope (v1)

Spot trading and portfolio: REST and WebSocket v2, four namespaces (market, account, trade, subscription). Futures and a unified API are v2.

## Working on the SDK

[Development](development.md), then [`CONTRIBUTING.md`](../CONTRIBUTING.md). Task path and probes: [dispatch-flow.md](dispatch-flow.md), [dispatch-latency-bench.md](dispatch-latency-bench.md).
