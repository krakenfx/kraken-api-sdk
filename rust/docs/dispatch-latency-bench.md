# Dispatch latency

Test-only histogram, probes, and two benches for the dispatch layer. Production builds compile this out (`#[cfg(test)]`). Task model: [dispatch-flow.md](dispatch-flow.md).

## Histogram

`LatencyHistogram` (`src/dispatch/io_reactor/latency_histogram.rs`) buckets each sample by power-of-two band plus 16 linear sub-buckets (`SUB_BITS = 4`). Worst-case bucket error is 1/16 (~6.25%). `counts: [u64; 976]` is inline. `record()` is a shift, a mask, and one increment.

`percentile()` scans to the bucket whose cumulative count crosses the rank and returns that bucket's midpoint. `count`, `min`, and `max` are exact fields; they are not reconstructed from buckets.

Proofs (`cargo test -p kraken-sdk latency_histogram`):

| Test | Guarantee |
|------|-----------|
| `every_bucket_round_trips` | Each of 976 bucket midpoints maps back to the same bucket |
| `ramp_percentiles_within_error_bound` | Uniform `1..=1e6`: p50/p90/p99 within 6.25% of analytic ranks |
| `count_min_max_are_exact` | Count, min, and max stay exact; only percentiles snap to a bucket |

Probes use `std::time::Instant` (OS monotonic clock). There is no clock-calibration test.

## Probes

Four process-global slots, armed only in the measured window:

| Slot | Span |
|------|------|
| `REACTOR_SPAN` | Reactor per-frame compute |
| `TYPED_DELIVERY_SPAN` | Box `OrderBookUpdate` and hand to each Book handler (part of reactor compute) |
| `DISPATCH_SPAN` | Ring dequeue to just before the client callback. Start stamp is `DISPATCH_RECV_START` (`AtomicU64`). Dispatch is a single task with no `.await` between dequeue and invoke |
| `ORDER_FORWARD_SPAN` | `handle_ws_request_frame` entry to just after send |

`arm()` / `record_since(start)` / `take()`. One writer per slot. Benches must run `--test-threads=1`. Both benches are `#[ignore]`.

Clock reads are a meaningful fraction of the ~82 ns dispatch span. Treat that number as an upper bound.

Locations: histogram in `latency_histogram.rs`; book/order benches in `io_reactor/tests.rs`; stamps in `frame_routing.rs`, `event_bus/mod.rs`, `inbound_dispatch.rs`.

## Book delivery (`bench_book_delivery_latency`)

Path: [dispatch-flow.md](dispatch-flow.md#book-delivery). WARMUP=200 discarded, N=2000, one frame in flight.

| Metric | Span | Threads |
|--------|------|---------|
| `book delivery (end-to-end)` | emit to `on_book` entry | Cross-task |
| `reactor on-thread` | Frame entry to end of `route_text_frame` | Reactor |
| `book typed delivery (fan)` | Typed fan start to end | Reactor |
| `dispatch on-thread` | Dequeue to before invoke | Dispatch |

Asserts sit on within-task compute: reactor `count == N`, `p99 < 5ms`; dispatch `count == N`, `p99 < 1ms`. End-to-end and residual (mock ingress, JSON parse in `handle_frame`, ring, scheduler wakeups) are printed, not gated. The client callback body is not in the e2e stamp.

Illustrative split from one debug in-process run: e2e ~50 µs ≈ reactor ~28 µs + residual ~22 µs + dispatch ~82 ns. Typed fan ~0.5 µs p50.

## Order forward (`bench_order_forward_compute`)

Path: [dispatch-flow.md](dispatch-flow.md#order-path). `handle_ws_request_frame` is synchronous. Isolation under a stuck consumer is `blocking_data_callback_does_not_stall_order_send`, not this bench.

Starter-tier order cap keeps N small (35 in the recorded run). Headline is p50. Debug in-process numbers are a regression baseline, not release latency.

## Limitations

- In-process mock only. No real socket, TLS, or contention.
- `#[cfg(test)]` stamps add noise in test builds.
- Global probe slots: `--test-threads=1`.
- Percentiles are bucket-quantized; at small N, p99 can exceed exact max.
- Elapsed time, not CPU. Prefer p50 on a loaded laptop.

**In scope:** reactor per-frame compute, typed fan-out, dispatch pre-callback, book e2e, order-forward compute.

**Out of scope:** live WebSocket/TLS, REST, reconnect / resubscribe / token refresh, caller→I/O enqueue, order-ack wake, callback body, multi-handler fan-out, queue-full, throughput.

## Commands

From `rust/`:

```sh
cargo test -p kraken-sdk latency_histogram
cargo test -p kraken-sdk latency_bench -- --ignored --nocapture --test-threads=1
```

Recorded debug run (in-process mock; timings vary):

```
book delivery (end-to-end) n=2000   min=  40.79us p50=  50.18us p90=  60.42us p99=  75.78us max= 110.88us
reactor on-thread        n=2000   min=  24.62us p50=  28.16us p90=  33.79us p99=  37.89us max=  55.33us
book typed delivery (fan) n=2000   min=    416ns p50=    528ns p90=    720ns p99=   2.37us max=   9.12us
dispatch on-thread       n=2000   min=     41ns p50=     82ns p90=    126ns p99=    126ns max=    458ns
order-forward (reactor)  n=35     min=   9.46us p50=  11.01us p90=  12.54us p99=  14.59us max=  14.38us
```

`count == N` is asserted. `p99 > max` on order-forward is bucket snap at small N.
