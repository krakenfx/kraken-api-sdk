//! [`SubscriptionGuard`] — RAII guard from the market and account `on_*_for` combiners.
//! Holds a `Weak<DispatchEventBus>` so `Drop` posts teardown without keeping the bus
//! alive; `Drop` posts one message carrying every pair (best-effort, never panics).

use std::sync::Weak;

use crate::dispatch::{CallerInbound, DispatchEventBus, HandlerId};
use crate::types::{ChannelName, Symbol};

/// RAII subscription guard. Dropping it tears down the underlying handler and
/// per-pair subscriptions. `Drop` is synchronous: no block, no await, no panic.
#[must_use = "dropping the guard immediately tears down the subscription; bind it to keep the stream live"]
pub struct SubscriptionGuard {
    handler_id: HandlerId,
    channel: ChannelName,
    pairs: Vec<Symbol>,
    /// Weak ref so `Drop` can post teardown without keeping the bus alive.
    weak_bus: Weak<DispatchEventBus>,
}

impl SubscriptionGuard {
    /// Construct from the registered `handler_id`, `channel`, the `pairs` it covers
    /// (empty = channel-wide), and a weak bus ref. Crate-internal — minted by the
    /// `on_*_for` combiners.
    pub(crate) fn new(
        handler_id: HandlerId,
        channel: ChannelName,
        pairs: Vec<Symbol>,
        weak_bus: Weak<DispatchEventBus>,
    ) -> Self {
        Self {
            handler_id,
            channel,
            pairs,
            weak_bus,
        }
    }

    /// The channel this guard subscribes to.
    pub fn channel(&self) -> ChannelName {
        self.channel
    }

    #[cfg(test)]
    pub(crate) fn pairs(&self) -> &[Symbol] {
        &self.pairs
    }
}

impl std::fmt::Debug for SubscriptionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionGuard")
            .field("handler_id", &self.handler_id)
            .field("channel", &self.channel)
            .field("pairs", &self.pairs)
            .finish()
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        let Some(bus) = self.weak_bus.upgrade() else {
            return;
        };
        bus.post_teardown(CallerInbound::SubscriptionGuardDrop {
            handler_id: self.handler_id,
            channel: self.channel,
            symbols: std::mem::take(&mut self.pairs),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_with_dead_bus_drops_without_panic() {
        let guard = SubscriptionGuard::new(
            HandlerId(1),
            ChannelName::Ticker,
            vec![Symbol::new("BTC/USD").unwrap()],
            Weak::new(),
        );
        assert_eq!(guard.channel(), ChannelName::Ticker);
        drop(guard);
    }

    #[test]
    fn channel_wide_guard_with_dead_bus_drops_without_panic() {
        let guard =
            SubscriptionGuard::new(HandlerId(2), ChannelName::Balances, Vec::new(), Weak::new());
        drop(guard);
    }

    #[test]
    fn multi_pair_guard_drop_posts_one_message_for_all_pairs() {
        use std::sync::Arc;
        let clock: Arc<dyn crate::clock::Clock> = Arc::new(crate::clock::SystemClock);
        let bus = Arc::new(crate::dispatch::DispatchEventBus::new(
            crate::dispatch::DispatchEventBusConfig::defaults(),
            clock,
        ));
        let mut rx = bus.take_caller_to_io_rx().expect("caller_to_io rx present");

        let btc = Symbol::new("BTC/USD").unwrap();
        let eth = Symbol::new("ETH/USD").unwrap();
        let guard = SubscriptionGuard::new(
            HandlerId(7),
            ChannelName::Book,
            vec![btc.clone(), eth.clone()],
            Arc::downgrade(&bus),
        );
        drop(guard);

        match rx.try_recv().expect("exactly one message posted") {
            CallerInbound::SubscriptionGuardDrop {
                handler_id,
                channel,
                symbols,
            } => {
                assert_eq!(handler_id, HandlerId(7));
                assert_eq!(channel, ChannelName::Book);
                assert_eq!(symbols, vec![btc, eth], "one message carries every pair");
            }
            other => panic!("expected SubscriptionGuardDrop, got {other:?}"),
        }
        assert!(
            rx.try_recv().is_err(),
            "a multi-pair guard drop must post exactly ONE message, not one per pair"
        );
    }
}
