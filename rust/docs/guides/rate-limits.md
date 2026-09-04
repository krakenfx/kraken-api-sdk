# Rate Limits

The SDK charges local counters and rejects a call that would breach before it hits the wire. It does not sleep or throttle. Two counters: non-trading REST (API) and per-pair trading (Spot REST and Spot WS share the trading counter).

## Tier

Kraken assigns the rate-limit tier from KYC, not volume. The SDK cannot read it from responses. Set it at build:

```rust
use kraken_sdk::{ClientBuilder, Tier};

let client = ClientBuilder::new()
    .with_tier_override(Tier::Intermediate)
    .build()?;
```

Default `Tier::Starter`. Caps and decay are fixed for the `Client` lifetime. The only runtime counter change is snap-to-cap on a wire rate-limit rejection.

## Trading costs (Starter)

Per-pair cap 60, decay 1.0 / s (higher tiers: larger caps). Place costs 1.0. Cancel costs more on a young order (~8.0) and falls as it rests. Exhaustion is a rate-limited error; nothing is sent.

| Order age (s) | AmendOrder | CancelOrder |
|---------------|-----------:|------------:|
| < 5           | 4.0        | 8.0         |
| 5 – <10       | 3.0        | 6.0         |
| 10 – <15      | 2.0        | 5.0         |
| 15 – <45      | 1.0        | 4.0         |
| 45 – <90      | 1.0        | 2.0         |
| 90 – <300     | 1.0        | 1.0         |
| ≥ 300         | 1.0        | 0.0         |

Amend never below 1.0. Cancel can be 0.0 after 300 s.

A cancel immediately after place inflates the trading counter.

- `cancel_all`: +1 trading per affected pair (set unknown until the reply).
- `cancel_all_orders_after`: +1 trading, no pair.

Wire scope: `EOrder:Rate limit exceeded` is per-pair. `EOrder:Domain rate limit exceeded` is account-wide.

## REST retry-after

`EService:Throttled: <timestamp>` is an absolute wall-clock instant. The SDK waits until then, capped by the backoff ceiling. Compared to the system clock, not the injected monotonic clock.

## Snap-to-cap

On a classified rate-limit wire rejection the matching counter(s) snap to cap and `RateLimitExceededEvent` fires. Trading: per-pair snap for `EOrder:Rate limit exceeded`; all pairs for `EOrder:Domain rate limit exceeded` (LRU-promoted). Codes: [Error handling](error-handling.md#rate-limit-error-codes).

The pre-wire `consume()` path returns `Err` and does not emit the event.

## Connect budget

`connection_rate_budget` / `connection_rate_window_secs` cap connect/upgrade attempts. Cloudflare's per-IP cap varies; set both to the observed window. Window `0` (or shorter than connect cadence) disables the budget.

## Related

- [Error handling](error-handling.md)
- [Configuration](configuration.md)
