# Dispatch flow

Three Tokio tasks carry a book frame and a WebSocket order. Latency numbers: [dispatch-latency-bench.md](dispatch-latency-bench.md). Caller-facing model: [architecture.md](architecture.md). Internals: [development.md](development.md).

## Tasks

The I/O reactor is full-duplex: one `select!` loop, no wait on a slow consumer. Tasks are Tokio-spawned, not pinned OS threads.

| Task | Role | Caller-visible delivery |
|------|------|-------------------------|
| Caller (T1) | `try_send` onto `caller_to_io`, then `.await` | Order ack / result |
| Reactor (T2) | Sole reader and writer of every socket | — |
| Dispatch (T3) | Drain `io_to_dispatch`, invoke callbacks | `on_book` and other `on_*` |

The reactor writes a frame and returns to `select!`. Orders in flight are matched by `req_id`. Book frames continue while orders are outstanding. The same task reads and writes each socket. The only cross-task hop for book data is the `io_to_dispatch` ring.

- Order ack completes the caller's `.await` on T1.
- `on_book` runs on T3.

## Book delivery

Parse, maintain, and CRC run on T2. The callback runs on T3.

```mermaid
flowchart TB
    WS["WebSocket frame, JSON text<br/>(delta or snapshot)"]
    subgraph T2["I/O reactor (T2)"]
      P["parse JSON"]
      M["apply_delta / apply_snapshot + CRC<br/>cumulative top-N, typed"]
      PUB["publish_data: OrderBookUpdate<br/>(no JSON round-trip)"]
    end
    RING["io_to_dispatch ring<br/>DataDelivery: typed book + callback"]
    subgraph T3["dispatch loop (T3)"]
      CB["on_book(full top-N)"]
    end
    WS --> P --> M --> PUB --> RING --> CB
```

- The reactor holds the full book. `apply_snapshot` / `apply_delta` fold into the cumulative top-N and re-check CRC.
- A `Book` frame fans to `[Book, BookRaw]` only.
- `on_book` receives the full `OrderBookUpdate` (`Arc` clone onto the ring). `BookRaw` builds a small JSON envelope and decodes `BookDelta` in the handler.

## Order path

WS trade methods post a `WsRequestFrame` on `caller_to_io` and `.await` a oneshot. That hop does not enter the dispatch loop. REST (`.via(Transport::Rest)` / `prefer_rest_for_orders`) is HTTP.

On T1 the trading tracker is charged, then `try_send`. Full or closed rejects immediately (`TradeError::QueueFull` / not-open); the frame is not sent. T2 runs `handle_ws_request_frame`: cached token, `{method, params, req_id}`, record pending, `send_frame` (`try_send` on the auth outbox). A send error fails the pending.

```mermaid
flowchart LR
    OB["trade WS .await<br/>(caller, T1)"] -->|"caller_to_io<br/>(try_send)"| HW
    subgraph IO["I/O reactor (T2)"]
      HW["handle_ws_request_frame:<br/>token + compose + record + send_frame"]
    end
    HW -->|"method bytes"| W["auth socket outbox"]
```

The reply is matched by `req_id` on T2 and completes the oneshot. A blocked `on_*` callback cannot stall an order send (`blocking_data_callback_does_not_stall_order_send`).
