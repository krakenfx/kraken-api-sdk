//! [`SubscriptionGuard`] — RAII guard from the market and account `on_*_for` combiners.
//! Holds a `Weak<DispatchEventBus>` so `Drop` posts teardown without keeping the bus
//! alive; `Drop` posts one message carrying every pair (best-effort, never panics).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use crate::dispatch::{CallerInbound, DispatchEventBus, HandlerId};
use crate::types::{ChannelName, StreamSymbol, WsUrl};

/// RAII subscription guard. Dropping it tears down the underlying handler and
/// per-pair subscriptions. `Drop` is synchronous: no block, no await, no panic.
#[must_use = "dropping the guard immediately tears down the subscription; bind it to keep the stream live"]
pub struct SubscriptionGuard {
    handler_id: HandlerId,
    url: WsUrl,
    channel: ChannelName,
    pairs: Vec<StreamSymbol>,
    /// Weak ref so `Drop` can post teardown without keeping the bus alive.
    weak_bus: Weak<DispatchEventBus>,
    unified_book_reservation: Option<UnifiedBookReservation>,
}

pub(crate) type UnifiedBookIntents = Arc<Mutex<HashMap<String, UnifiedBookIntent>>>;

#[derive(Debug)]
pub(crate) struct UnifiedBookIntent {
    pub(crate) depth: u32,
    pub(crate) snapshot: bool,
    pub(crate) maintained_depth: Option<u32>,
    pub(crate) bare: [u32; 2],
    pub(crate) guarded: [u32; 2],
    pub(crate) generation: [u64; 2],
}

impl UnifiedBookIntent {
    pub(crate) fn total(&self) -> u32 {
        self.bare.into_iter().sum::<u32>() + self.guarded.into_iter().sum::<u32>()
    }

    pub(crate) fn live_maintained_depth(&self) -> Option<u32> {
        let slot = unified_book_slot(ChannelName::Book)?;
        self.maintained_depth
            .filter(|_| self.bare[slot] + self.guarded[slot] > 0)
    }

    pub(crate) fn generation(&self, channel: ChannelName) -> Option<u64> {
        self.generation.get(unified_book_slot(channel)?).copied()
    }

    pub(crate) fn set_generation(&mut self, channel: ChannelName, generation: u64) {
        if let Some(slot) = unified_book_slot(channel) {
            self.generation[slot] = generation;
        }
    }

    pub(crate) fn add(&mut self, channel: ChannelName, guarded: bool) {
        let Some(slot) = unified_book_slot(channel) else {
            return;
        };
        let count = if guarded {
            &mut self.guarded[slot]
        } else {
            &mut self.bare[slot]
        };
        *count += 1;
    }

    pub(crate) fn release_bare(&mut self, channel: ChannelName) {
        let Some(slot) = unified_book_slot(channel) else {
            return;
        };
        self.bare[slot] = self.bare[slot].saturating_sub(1);
    }

    fn release_guarded(&mut self, channel: ChannelName, generation: u64) {
        let Some(slot) = unified_book_slot(channel) else {
            return;
        };
        if self.generation[slot] == generation {
            self.guarded[slot] = self.guarded[slot].saturating_sub(1);
        }
    }

    pub(crate) fn clear_channel(&mut self, channel: Option<ChannelName>) {
        match channel {
            Some(channel) => {
                let Some(slot) = unified_book_slot(channel) else {
                    return;
                };
                self.bare[slot] = 0;
                self.guarded[slot] = 0;
            }
            None => {
                self.bare = [0; 2];
                self.guarded = [0; 2];
            }
        }
    }
}

fn unified_book_slot(channel: ChannelName) -> Option<usize> {
    match channel {
        ChannelName::Book => Some(0),
        ChannelName::BookRaw => Some(1),
        _ => None,
    }
}

pub(crate) fn lock_unified_book_intents(
    intents: &UnifiedBookIntents,
) -> std::sync::MutexGuard<'_, HashMap<String, UnifiedBookIntent>> {
    intents
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug)]
pub(crate) struct UnifiedBookReservation {
    intents: UnifiedBookIntents,
    pairs: Vec<(String, ChannelName, u64)>,
    guarded: bool,
    committed: bool,
}

impl UnifiedBookReservation {
    pub(crate) fn new(
        intents: UnifiedBookIntents,
        pairs: Vec<(String, ChannelName, u64)>,
        guarded: bool,
    ) -> Self {
        Self {
            intents,
            pairs,
            guarded,
            committed: false,
        }
    }

    /// Retain a bare subscription reservation after its register mutation posts.
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for UnifiedBookReservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut intents = lock_unified_book_intents(&self.intents);
        for (pair, channel, generation) in &self.pairs {
            if let Some(intent) = intents.get_mut(pair) {
                if self.guarded {
                    intent.release_guarded(*channel, *generation);
                } else {
                    intent.release_bare(*channel);
                }
                if intent.total() == 0 {
                    intents.remove(pair);
                }
            }
        }
    }
}

impl SubscriptionGuard {
    /// Construct from the registered `handler_id`, `channel`, the `pairs` it covers
    /// (empty = channel-wide), and a weak bus ref. Crate-internal — minted by the
    /// `on_*_for` combiners.
    pub(crate) fn new(
        handler_id: HandlerId,
        url: WsUrl,
        channel: ChannelName,
        pairs: Vec<StreamSymbol>,
        weak_bus: Weak<DispatchEventBus>,
    ) -> Self {
        Self {
            handler_id,
            url,
            channel,
            pairs,
            weak_bus,
            unified_book_reservation: None,
        }
    }

    pub(crate) fn with_unified_book_reservation(
        mut self,
        reservation: UnifiedBookReservation,
    ) -> Self {
        self.unified_book_reservation = Some(reservation);
        self
    }

    /// The channel this guard subscribes to.
    pub fn channel(&self) -> ChannelName {
        self.channel
    }

    #[cfg(test)]
    pub(crate) fn pairs(&self) -> &[StreamSymbol] {
        &self.pairs
    }
}

impl std::fmt::Debug for SubscriptionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionGuard")
            .field("handler_id", &self.handler_id)
            .field("url", &self.url)
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
            url: self.url,
            channel: self.channel,
            symbols: std::mem::take(&mut self.pairs),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Symbol;

    #[test]
    fn guard_with_dead_bus_drops_without_panic() {
        let guard = SubscriptionGuard::new(
            HandlerId(1),
            WsUrl::Public,
            ChannelName::Ticker,
            vec![crate::types::Symbol::new("BTC/USD").unwrap().into()],
            Weak::new(),
        );
        assert_eq!(guard.channel(), ChannelName::Ticker);
        drop(guard);
    }

    #[test]
    fn channel_wide_guard_with_dead_bus_drops_without_panic() {
        let guard = SubscriptionGuard::new(
            HandlerId(2),
            WsUrl::Auth,
            ChannelName::Balances,
            Vec::new(),
            Weak::new(),
        );
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
            WsUrl::Public,
            ChannelName::Book,
            vec![btc.clone().into(), eth.clone().into()],
            Arc::downgrade(&bus),
        );
        drop(guard);

        match rx.try_recv().expect("exactly one message posted") {
            CallerInbound::SubscriptionGuardDrop {
                handler_id,
                url,
                channel,
                symbols,
            } => {
                assert_eq!(handler_id, HandlerId(7));
                assert_eq!(url, WsUrl::Public);
                assert_eq!(channel, ChannelName::Book);
                assert_eq!(
                    symbols,
                    vec![btc.into(), eth.into()],
                    "one message carries every pair"
                );
            }
            other => panic!("expected SubscriptionGuardDrop, got {other:?}"),
        }
        assert!(
            rx.try_recv().is_err(),
            "a multi-pair guard drop must post exactly ONE message, not one per pair"
        );
    }
}
