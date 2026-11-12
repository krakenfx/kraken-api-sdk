//! Declared-intent subscription store keyed by `(url, channel, pair)`. Single-writer:
//! all mutators take `&mut self` and run only on the I/O reactor task.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde_json::Value;

use crate::api::subscription::TerminationCause;
use crate::api::subscription_types::{SubscribeFailureCause, SubscriptionState};
use crate::dispatch::HandlerId;
use crate::types::{
    BookDepth, ChannelName, FuturesOhlcTickType, MonotonicInstant, OhlcInterval, StreamSymbol,
    TickerTrigger, WsUrl,
};

/// Retained on each [`SubscriptionEntry`] so the resubscribe composer can rebuild
/// the wire frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscribeParams {
    Ticker {
        snapshot: Option<bool>,
        event_trigger: Option<TickerTrigger>,
    },
    Book {
        depth: BookDepth,
    },
    BookRaw {
        depth: BookDepth,
        snapshot: Option<bool>,
    },
    Trade {
        snapshot: Option<bool>,
    },
    Ohlc {
        interval: OhlcInterval,
        snapshot: Option<bool>,
        tick_type: Option<FuturesOhlcTickType>,
    },
    Status,
    Instrument,
    Executions {
        snap_orders: Option<bool>,
        snap_trades: Option<bool>,
    },
    Balances {
        snapshot: Option<bool>,
    },
    Positions {
        snapshot: Option<bool>,
    },
    Margin,
    Preferences,
}

impl SubscribeParams {
    pub(crate) fn futures_symbol(self, symbol: String) -> StreamSymbol {
        match self {
            Self::Ohlc {
                interval,
                tick_type,
                ..
            } => StreamSymbol::futures_ohlc(symbol, interval, tick_type),
            Self::Ticker { .. }
            | Self::Book { .. }
            | Self::BookRaw { .. }
            | Self::Trade { .. }
            | Self::Status
            | Self::Instrument
            | Self::Executions { .. }
            | Self::Balances { .. }
            | Self::Positions { .. }
            | Self::Margin
            | Self::Preferences => StreamSymbol::Futures(symbol),
        }
    }
}

/// Outcome of driving maintained-book state for one inbound frame, so the reactor
/// knows which payload to fan to the `Book` handler set.
#[derive(Debug)]
pub enum BookDriveOutcome {
    /// No maintained builder for this `(channel, pair)`. The reactor fans the
    /// ORIGINAL per-frame payload (unchanged).
    PassThrough,
    NoEntry,
    /// Maintained book advanced and its CRC32 validated. Delivered to `on_book` as
    /// a typed `OrderBookUpdate` — no JSON re-encode.
    Maintained(crate::book::OrderBookUpdate),
    /// Maintained Futures book advanced and its CRC32 validated.
    FuturesMaintained(Arc<crate::book::FuturesOrderBookUpdate>),
    /// CRC32 mismatch (the gap signal). The reactor skips the `Book` fan; the
    /// gap-recovery slice owns the gap event + resubscribe.
    Gap {
        expected: u32,
        computed: u32,
    },
    /// A delta arriving during the post-gap resync window is dropped silently — no
    /// fan, no gap event, no resubscribe — so a still-live old subscription can't
    /// storm. Cleared when the fresh snapshot lands.
    AwaitingSnapshot,
    /// Consecutive-gap cap hit — even fresh snapshots keep mismatching. The reactor
    /// emits a final gap event and stops resubscribing; the builder is dropped so
    /// `Book` frames flow through as un-validated per-frame `on_book`.
    GapBudgetExhausted,
}

/// Outcome of [`SubscriptionRegistry::note_reseed_timeout_for`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReseedTimeoutOutcome {
    /// Stale timer — entry gone, already degraded, or snapshot already landed.
    StaleNoOp,
    /// Still resyncing past the window — caller re-requests the reseed.
    Retry,
    /// Consecutive-gap cap hit via snapshot-timeouts — maintenance dropped.
    Exhausted,
}

/// Per-entry subscription lifecycle state, owned by the reactor on
/// `SubscriptionEntry`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EntrySubState {
    /// Subscribe frame sent (or queued); wire ack not yet received.
    Pending,
    /// Subscribe acknowledged; data flowing.
    Acked,
    /// Retained as a visible tombstone until re-subscribe, last release, or teardown.
    Terminated {
        cause: TerminationCause,
        /// Verbatim Kraken reject string, when present.
        last_error: Option<String>,
    },
}

/// One caller-readable mirror row per registry entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MirrorRow {
    pub(crate) state: EntrySubState,
    /// Sampled when the entry was first registered (refreshed on tombstone revival).
    pub(crate) registered_at: MonotonicInstant,
    /// Venue acknowledgement time. `None` until the entry first becomes `Acked`.
    pub(crate) subscribed_at: Option<MonotonicInstant>,
}

impl MirrorRow {
    /// Record the first transition into `Acked`. A repeat ack keeps the original time.
    fn mark_acked(&mut self, now: MonotonicInstant) {
        if !matches!(self.state, EntrySubState::Acked) {
            self.subscribed_at = Some(now);
        }
        self.state = EntrySubState::Acked;
    }
}

/// Classification of an entry removed by [`SubscriptionRegistry::deregister_all_for`]:
/// a `Live` entry gets wire teardown; a `Tombstone` is removed silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemovedEntryKind {
    Live,
    Tombstone,
}

/// Shared caller-readable projection of the reactor-owned registry. The registry
/// is the single writer; caller reads may trail.
pub(crate) type SubscriptionMirror =
    Arc<RwLock<HashMap<(WsUrl, ChannelName, Option<StreamSymbol>), MirrorRow>>>;

/// Cap on consecutive CRC32 gap-recovery failures for one maintained book before
/// the SDK drops maintenance and `on_book` degrades to per-frame delivery.
const MAX_CONSECUTIVE_BOOK_GAPS: u32 = 5;

/// Wire-sequence continuity tracker. Overflow-proof via `checked_sub`.
#[derive(Debug, Clone, Copy, Default)]
pub struct SequenceTracker(Option<u64>);

impl SequenceTracker {
    /// A snapshot (re)seeds the epoch; a duplicate/regressed sequence resets it
    /// (server-side restart, never a loss); a jump returns the dropped-frame count.
    pub fn observe(&mut self, seq: u64, is_snapshot: bool) -> Option<u32> {
        let last = self.0.replace(seq);
        if is_snapshot {
            return None;
        }
        match seq.checked_sub(last?) {
            Some(delta) if delta > 1 => Some(u32::try_from(delta - 1).unwrap_or(u32::MAX)),
            // Contiguous, duplicate, or regression: no loss signal.
            _ => None,
        }
    }
}

/// One subscription entry, self-keyed by `(url, channel, pair)` (`pair = None` is the
/// channel-wide entry). State only — no callback, no entry-id. `builder` holds
/// maintained-`OrderBook` state for a maintained `Book` entry; `None` otherwise.
#[derive(Debug, Clone)]
pub struct SubscriptionEntry {
    /// Target WS URL (per-url routing for resubscribe frame composition).
    pub url: WsUrl,
    pub channel: ChannelName,
    /// `Some(sym)` for a per-pair subscription; `None` for a channel-wide one.
    pub pair: Option<StreamSymbol>,
    /// Caller-supplied channel-specific subscribe options; read by the resubscribe
    /// composer to rebuild the frame on reconnect. First-writer wins among live
    /// subscribers; a revival takes the reviving caller's params — see `register`.
    pub params: SubscribeParams,
    /// Subscriber refcount for this `(channel, pair)`: +1 per `register`, -1 per
    /// `release`. The wire unsubscribe fires only when this reaches 0.
    pub count: u32,
    /// Maintained-book state — `Some` only for a maintained `Book` entry, `None`
    /// otherwise. `Box`ed so the entry stays small in the caller→reactor channel
    /// (the builder's `BTreeMap`s would otherwise bloat every queued message).
    pub builder: Option<Box<crate::book::OrderBookBuilder>>,
    /// Wire-sequence continuity — active only for the channel-wide
    /// `executions`/`balances` entries.
    pub seq_tracker: SequenceTracker,
    /// Lifetime generation, registry-assigned on insert and on tombstone revival.
    /// Guard-ref releases are validated against it so a ref from a dead lifetime
    /// cannot tear down a revived subscription. `0` until the registry stamps it.
    pub(crate) generation: u64,
    /// Lifecycle state, reactor-owned. The caller mirror carries a write-through
    /// copy; nothing internal reads the mirror back.
    pub(crate) state: EntrySubState,
    /// Holders remaining at termination — `count` snapshotted by
    /// `note_terminated` before zeroing. The tombstone (entry + mirror row)
    /// clears when the last holder releases.
    pub(crate) tombstone_holders: u32,
    /// Dead-lifetime bare releases still owed after a revival: the remaining
    /// countdown converts here (guard holders excluded — their stale drops
    /// no-op upstream). Consumed one per bare release as a pure no-op.
    pub(crate) stale_release_credits: u32,
    /// A maintained Unified book that joined an already-acked raw stream must
    /// receive a fresh snapshot before it becomes caller-visible as active.
    pub(crate) awaiting_seed_snapshot: bool,
    pub(crate) seed_ack: Option<crate::dispatch::FuturesSubscribeAck>,
}

impl SubscriptionEntry {
    /// `true` iff this entry is a `Terminated` tombstone.
    pub(crate) fn is_tombstone(&self) -> bool {
        matches!(self.state, EntrySubState::Terminated { .. })
    }

    pub(crate) fn wire_acked(&self) -> bool {
        matches!(self.state, EntrySubState::Acked)
            || (self.awaiting_seed_snapshot && self.seed_ack.is_some())
    }

    /// Construct a state-only entry with refcount 1.
    pub fn new(
        url: WsUrl,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
        params: SubscribeParams,
    ) -> Self {
        // `on_book` and `on_book_raw` share wire key (Book, sym); register keeps the
        // FIRST entry's builder. Only `SubscribeParams::Book` seeds one.
        let builder = match (channel, &pair, params) {
            (
                ChannelName::Book,
                Some(sym @ (StreamSymbol::Spot(_) | StreamSymbol::Futures(_))),
                SubscribeParams::Book { depth },
            ) => {
                // Maintain/trim at the (possibly bumped) wire depth for CRC headroom.
                Some(Box::new(crate::book::OrderBookBuilder::new(
                    sym.as_str(),
                    depth.wire_depth().as_wire_u32() as usize,
                    depth.as_wire_u32() as usize,
                )))
            }
            _ => None,
        };
        Self {
            url,
            channel,
            pair,
            params,
            count: 1,
            builder,
            seq_tracker: SequenceTracker::default(),
            generation: 0,
            state: EntrySubState::Pending,
            tombstone_holders: 0,
            stale_release_credits: 0,
            awaiting_seed_snapshot: false,
            seed_ack: None,
        }
    }

    /// Drive maintained-book state from an inbound frame: decode, apply
    /// snapshot/delta, validate CRC32. Returns the [`BookDriveOutcome`]. Called by
    /// the reactor before fan-out; single-writer, no lock.
    pub fn drive_update(
        &mut self,
        is_snapshot: bool,
        data: &serde_json::Value,
    ) -> BookDriveOutcome {
        // Borrow the builder ONLY for the apply; the borrow must end before the
        // exhaustion path mutates `self.builder` (drop-to-degrade) below.
        let outcome = {
            let Some(builder) = self.builder.as_mut() else {
                return BookDriveOutcome::PassThrough;
            };
            // While awaiting the post-gap fresh snapshot, drop deltas — only a snapshot
            // reseeds and clears the window.
            if builder.is_resyncing() && !is_snapshot {
                return BookDriveOutcome::AwaitingSnapshot;
            }
            let Some(parsed) = crate::book::parse_book_frame(data) else {
                return BookDriveOutcome::PassThrough;
            };
            // Symbol mismatch is a routing bug — drop to PassThrough and debug-assert.
            if parsed.symbol != builder.symbol() {
                debug_assert!(
                    false,
                    "book frame for {} mis-routed to {} builder",
                    parsed.symbol,
                    builder.symbol()
                );
                return BookDriveOutcome::PassThrough;
            }
            let checksum = parsed.checksum;
            let result = if is_snapshot {
                builder.apply_snapshot(parsed.bids, parsed.asks, checksum)
            } else {
                builder.apply_delta(parsed.bids, parsed.asks, checksum)
            };
            match result {
                Ok(()) => match self.pair.as_ref() {
                    Some(StreamSymbol::Futures(_) | StreamSymbol::FuturesOhlc { .. }) => {
                        BookDriveOutcome::FuturesMaintained(Arc::new(
                            builder.build_futures_update(checksum, parsed.timestamp),
                        ))
                    }
                    Some(StreamSymbol::Spot(symbol)) => BookDriveOutcome::Maintained(
                        builder.build_update(symbol.clone(), checksum, None, parsed.timestamp),
                    ),
                    None => BookDriveOutcome::PassThrough,
                },
                Err(crate::book::ApplyError::ChecksumMismatch { expected, computed }) => {
                    if builder.consecutive_gaps() >= MAX_CONSECUTIVE_BOOK_GAPS {
                        BookDriveOutcome::GapBudgetExhausted
                    } else {
                        BookDriveOutcome::Gap { expected, computed }
                    }
                }
            }
        };
        // At the consecutive-gap cap, drop the builder — future Book frames PassThrough
        // instead of storming resubscribes forever. A book still waiting for its
        // seeding snapshot becomes an active unvalidated row; leaving the seed
        // hold set would keep it Pending with no builder to clear it.
        if matches!(outcome, BookDriveOutcome::GapBudgetExhausted) {
            self.degrade_to_passthrough();
        }
        outcome
    }

    /// Drop the maintained builder (future frames PassThrough). A book still
    /// waiting for its seed snapshot becomes an active unvalidated row; returns
    /// whether that promotion happened (the caller then refreshes the mirror).
    fn degrade_to_passthrough(&mut self) -> bool {
        self.builder = None;
        let promote = self.awaiting_seed_snapshot && matches!(self.state, EntrySubState::Pending);
        self.awaiting_seed_snapshot = false;
        if promote {
            self.state = EntrySubState::Acked;
        }
        promote
    }
}

/// Single-writer registry of declared subscriptions, owned by-value inside the
/// I/O reactor task. Callers mutate via `RegistryMutation` posted to the
/// caller→reactor channel.
#[derive(Default)]
pub struct SubscriptionRegistry {
    /// Per-pair subscriptions, nested `(url, channel) → (symbol → entry)` so a per-frame
    /// lookup borrows the symbol instead of cloning it into a composite key.
    entries: HashMap<(WsUrl, ChannelName), HashMap<StreamSymbol, SubscriptionEntry>>,
    /// Channel-wide (symbol-less) subscriptions: status / executions /
    /// balances + heartbeat keepalive.
    channel_wide: HashMap<(WsUrl, ChannelName), SubscriptionEntry>,
    /// Caller-readable lifecycle projection; every mutation of the maps above
    /// updates it in the same method, keyed identically on `(url, channel, pair)`.
    mirror: SubscriptionMirror,
    /// Monotonic lifetime-generation allocator; stamped onto entries on insert
    /// and on tombstone revival (see [`SubscriptionEntry::generation`]).
    next_generation: u64,
    /// Guard refs by `(handler_id, url, channel, pair)` → the entry generation the
    /// ref was registered against. A guard drop whose recorded generation no
    /// longer matches the live entry is a stale ref and releases nothing.
    guard_refs: HashMap<(HandlerId, WsUrl, ChannelName, Option<StreamSymbol>), u64>,
    /// Bumped by every liveness-changing mutation (register, release,
    /// deregister incl. the DeregisterBatch fan-out, note_terminated); the
    /// reactor loop re-derives the cached auth hints when it moves.
    liveness_epoch: u64,
}

impl SubscriptionRegistry {
    /// Construct an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct an empty registry sharing `mirror` with caller-side readers
    /// (the `WsSurface` read models). Same Arc-threading shape as
    /// `HandlerRegistry::new(presence_mirror)`.
    pub(crate) fn with_mirror(mirror: SubscriptionMirror) -> Self {
        Self {
            mirror,
            ..Self::new()
        }
    }

    /// Register an entry keyed by `(channel, pair)` (`None` → channel-wide):
    /// +1 if present, else insert at count 1; returns the post-register count
    /// (the reactor gates the wire subscribe on 0→1). `now` stamps the mirror
    /// row on fresh insert and revival. `guard_backed`: the caller holds a
    /// guard — on revival it decides reserving the reviver's bare slot.
    pub fn register(
        &mut self,
        entry: SubscriptionEntry,
        now: MonotonicInstant,
        guard_backed: bool,
    ) -> u32 {
        use std::collections::hash_map::Entry as MapEntry;
        self.liveness_epoch += 1;
        self.next_generation += 1;
        let generation = self.next_generation;
        // A revival takes the reviving caller's params/builder and starts a NEW
        // generation, so refs from the dead lifetime can no longer release it.
        fn bump_or_revive<K: std::hash::Hash + Eq>(
            slot: MapEntry<'_, K, SubscriptionEntry>,
            mut entry: SubscriptionEntry,
            generation: u64,
        ) -> (u32, Option<(u32, u64)>) {
            match slot {
                MapEntry::Occupied(mut o) => {
                    let e = o.get_mut();
                    let revived = if e.count == 0 {
                        // Dying lifetime's generation, captured before the restamp:
                        // only ITS guard refs convert into credits below.
                        let dying_generation = e.generation;
                        e.params = entry.params;
                        e.builder = entry.builder;
                        e.generation = generation;
                        // Fresh sequence epoch: the dead lifetime's last-seen seq
                        // must not gap the revival.
                        e.seq_tracker = SequenceTracker::default();
                        e.state = EntrySubState::Pending;
                        // The dead lifetime's seed hold must not keep the revival Pending.
                        e.awaiting_seed_snapshot = false;
                        e.seed_ack = None;
                        Some((std::mem::take(&mut e.tombstone_holders), dying_generation))
                    } else {
                        None
                    };
                    e.count += 1;
                    (e.count, revived)
                }
                MapEntry::Vacant(v) => {
                    entry.generation = generation;
                    (v.insert(entry).count, None)
                }
            }
        }
        let key = (entry.url, entry.channel, entry.pair.clone());
        let (count, revived) = match entry.pair.clone() {
            Some(sym) => bump_or_revive(
                self.entries
                    .entry((entry.url, entry.channel))
                    .or_default()
                    .entry(sym),
                entry,
                generation,
            ),
            None => bump_or_revive(
                self.channel_wide.entry((entry.url, entry.channel)),
                entry,
                generation,
            ),
        };
        // Revival converts the dead countdown into credits, excluding the DYING
        // lifetime's guard rows and (bare reviver only) one reviver slot.
        if let Some((holders, dying_generation)) = revived {
            let dying_guard_refs = self.guard_refs_at(key.0, key.1, &key.2, dying_generation);
            let reviver_slot = if guard_backed { 0 } else { 1 };
            let owed = holders
                .saturating_sub(dying_guard_refs)
                .saturating_sub(reviver_slot);
            if let Some(e) = self.entry_mut(key.0, key.1, &key.2) {
                // A later revival adds to the ledger, never overwrites — an
                // assignment would strand older unconsumed credits.
                e.stale_release_credits = e.stale_release_credits.saturating_add(owed);
            }
        }
        // Project only the fresh-insert / revival transition (count 0 -> 1).
        if count == 1 {
            self.mirror_write(|mirror| {
                mirror.insert(
                    key,
                    MirrorRow {
                        state: EntrySubState::Pending,
                        registered_at: now,
                        subscribed_at: None,
                    },
                );
            });
        }
        count
    }

    /// Note a channel-wide entry's frame `sequence`; returns the dropped-frame
    /// count on a gap (see [`SequenceTracker::observe`]). No entry → `None`.
    pub fn note_sequence_for(
        &mut self,
        url: WsUrl,
        channel: ChannelName,
        seq: u64,
        is_snapshot: bool,
    ) -> Option<u32> {
        self.channel_wide
            .get_mut(&(url, channel))?
            .seq_tracker
            .observe(seq, is_snapshot)
    }

    /// Decrement the refcount for `(channel, pair)`, removing the entry at 0.
    /// Returns the remaining count. The reactor emits the wire unsubscribe +
    /// terminated event only on the 0 transition.
    ///
    /// A `Terminated` entry counts holders down instead (tombstone visible until
    /// the LAST holder releases; no frame, no second event). Pure no-ops: a
    /// credited dead-lifetime release, and one with no unmatched bare slot.
    pub fn release_for(
        &mut self,
        url: WsUrl,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
    ) -> u32 {
        self.release_inner(url, channel, pair, true)
    }

    /// [`Self::release_for`] with BOTH bare-only arms scoped by `allow_credit` —
    /// the credit arm and the bare-slot gate. The guard-drop delegation passes
    /// `false`: a matching guard drop never burns a credit and never gates.
    fn release_inner(
        &mut self,
        url: WsUrl,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
        allow_credit: bool,
    ) -> u32 {
        // Credit arm FIRST: a dead-lifetime bare release is owed to the ledger
        // — else stragglers would count a NEW tombstone down before its own holders.
        if allow_credit
            && let Some(e) = self.entry_mut(url, channel, &pair)
            && e.stale_release_credits > 0
        {
            e.stale_release_credits -= 1;
            return e.count;
        }
        // A bare release may consume a slot only while an unmatched BARE slot
        // exists; remaining holders all guard-backed means the release is a stray.
        if allow_credit && let Some(e) = self.entry(url, channel, &pair) {
            let holders = if e.is_tombstone() {
                e.tombstone_holders
            } else {
                e.count
            };
            let guard_refs = self.guard_refs_at(url, channel, &pair, e.generation);
            if holders.saturating_sub(guard_refs) == 0 {
                if guard_refs > 0 {
                    tracing::debug!(
                        target: "kraken_sdk::conn",
                        ?url, ?channel, ?pair,
                        "plain unsubscribe ignored: every remaining holder is guard-backed; drop the guard to release"
                    );
                }
                return e.count;
            }
        }
        // Countdown arm is a liveness no-op: no epoch bump; final removal bumps
        // inside `deregister`. Only the live path bumps here.
        let tombstone_cleared = match self.entry_mut(url, channel, &pair) {
            Some(e) if e.is_tombstone() => {
                e.tombstone_holders = e.tombstone_holders.saturating_sub(1);
                Some(e.tombstone_holders == 0)
            }
            _ => None,
        };
        if let Some(cleared) = tombstone_cleared {
            if cleared {
                self.deregister_for(url, channel, pair);
            }
            return 0;
        }
        if self.entry(url, channel, &pair).is_none() {
            return 0;
        }
        self.liveness_epoch += 1;
        let remaining = self.entry_mut(url, channel, &pair).map(|e| {
            e.count = e.count.saturating_sub(1);
            e.count
        });
        match remaining {
            Some(0) => {
                self.deregister_for(url, channel, pair);
                0
            }
            Some(n) => n,
            None => 0,
        }
    }

    /// Record a guard ref: `handler_id` holds one ref on the live `(channel, pair)`
    /// entry's current generation. Called by the reactor when a `RegisterBatch`
    /// carries the registering guard's id; the matching guard drop releases via
    /// [`Self::release_guard_ref_for`], which validates this generation.
    pub(crate) fn record_guard_ref_for(
        &mut self,
        handler_id: HandlerId,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) {
        if let Some(generation) = self.entry_generation(url, channel, pair) {
            self.guard_refs
                .insert((handler_id, url, channel, pair.clone()), generation);
        }
    }

    /// Release one guard ref — generation-validated: a stale-lifetime drop
    /// releases NOTHING. A ref with no record falls back to the bare-arm-exempt
    /// release (`allow_credit = false`): never burns a credit, never gates.
    pub(crate) fn release_guard_ref_for(
        &mut self,
        handler_id: HandlerId,
        url: WsUrl,
        channel: ChannelName,
        pair: Option<StreamSymbol>,
    ) -> u32 {
        // No leading epoch bump: the stale-generation arm is a deliberate no-op;
        // the matching arm bumps inside `release`.
        let recorded = self
            .guard_refs
            .remove(&(handler_id, url, channel, pair.clone()));
        let live = self
            .entry(url, channel, &pair)
            .map(|e| (e.generation, e.count));
        match (recorded, live) {
            (Some(rec), Some((generation, count))) if rec != generation => count,
            _ => self.release_inner(url, channel, pair, false),
        }
    }

    /// The live entry's generation for `(channel, pair)`, if the key is occupied.
    fn entry_generation(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) -> Option<u64> {
        self.entry(url, channel, pair).map(|e| e.generation)
    }

    /// Guard-ref rows recorded for `(channel, pair)` at `generation`.
    fn guard_refs_at(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
        generation: u64,
    ) -> u32 {
        self.guard_refs
            .iter()
            .filter(|((_, entry_url, ch, p), entry_gen)| {
                *entry_url == url && *ch == channel && *p == *pair && **entry_gen == generation
            })
            .count() as u32
    }

    /// The entry for `(channel, pair)`, if the key is occupied.
    fn entry(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) -> Option<&SubscriptionEntry> {
        match pair {
            Some(sym) => self.entries.get(&(url, channel)).and_then(|m| m.get(sym)),
            None => self.channel_wide.get(&(url, channel)),
        }
    }

    /// Mutable [`Self::entry`].
    fn entry_mut(
        &mut self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) -> Option<&mut SubscriptionEntry> {
        match pair {
            Some(sym) => self
                .entries
                .get_mut(&(url, channel))
                .and_then(|m| m.get_mut(sym)),
            None => self.channel_wide.get_mut(&(url, channel)),
        }
    }

    /// Write-through to the caller mirror. Nothing internal reads it back; a
    /// poisoned lock skips the write LOUDLY (caller reads on poison already
    /// fail as `LoopDead`, so the projection is dead either way).
    fn mirror_write(
        &self,
        f: impl FnOnce(&mut HashMap<(WsUrl, ChannelName, Option<StreamSymbol>), MirrorRow>),
    ) {
        match self.mirror.write() {
            Ok(mut mirror) => f(&mut mirror),
            Err(_) => tracing::warn!(
                target: "kraken_sdk::conn",
                "subscription mirror lock poisoned; projection write skipped"
            ),
        }
    }

    /// Liveness-mutation counter for the reactor loop's per-iteration hint sync.
    pub(crate) fn liveness_epoch(&self) -> u64 {
        self.liveness_epoch
    }

    /// Remove the subscription for `(channel, pair)` outright (ignores refcount).
    /// `pair = None` removes the channel-wide entry. Idempotent. Also drops the
    /// mirror row — a caller-driven removal leaves no tombstone.
    pub fn deregister_for(&mut self, url: WsUrl, channel: ChannelName, pair: Option<StreamSymbol>) {
        self.liveness_epoch += 1;
        match &pair {
            Some(sym) => {
                if let Some(inner) = self.entries.get_mut(&(url, channel)) {
                    inner.remove(sym);
                    if inner.is_empty() {
                        self.entries.remove(&(url, channel));
                    }
                }
            }
            None => {
                self.channel_wide.remove(&(url, channel));
            }
        }
        self.mirror_write(|mirror| {
            mirror.remove(&(url, channel, pair));
        });
    }

    /// URL-scoped forced deregistration used by an internal socket-specific
    /// teardown. Public Spot bulk operations route to `WsUrl::Public`. Ignores refcounts and
    /// tombstone holders; returns this socket's removed entries in teardown order, tagged
    /// [`RemovedEntryKind`].
    pub(crate) fn deregister_all_for(
        &mut self,
        url: WsUrl,
        channel: Option<ChannelName>,
    ) -> Vec<(SubscriptionEntry, RemovedEntryKind)> {
        self.liveness_epoch += 1;
        let matches = |(entry_url, ch): &(WsUrl, ChannelName)| {
            *entry_url == url && channel.is_none_or(|wanted| wanted == *ch)
        };
        let mut removed: Vec<_> = self
            .channel_wide
            .extract_if(|key, _| matches(key))
            .map(|(_, entry)| entry)
            .collect();
        removed.extend(
            self.entries
                .extract_if(|key, _| matches(key))
                .flat_map(|(_, entries)| entries.into_values()),
        );
        removed.sort_by(|a, b| {
            subscription_order_key(a.channel, a.pair.as_ref())
                .cmp(&subscription_order_key(b.channel, b.pair.as_ref()))
        });
        self.mirror_write(|mirror| {
            for entry in &removed {
                mirror.remove(&(entry.url, entry.channel, entry.pair.clone()));
            }
        });
        removed
            .into_iter()
            .map(|entry| {
                let kind = if entry.is_tombstone() {
                    RemovedEntryKind::Tombstone
                } else {
                    RemovedEntryKind::Live
                };
                (entry, kind)
            })
            .collect()
    }

    /// `true` iff the entry for `(channel, pair)` is a `Terminated` tombstone.
    /// Reads reactor-owned entry state — lock-free, never the mirror. Probed by
    /// the teardown paths so a `Failed` entry goes silently (no wire frame, no
    /// second terminal event).
    pub(crate) fn is_terminated_for(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) -> bool {
        self.entry(url, channel, pair)
            .is_some_and(|e| e.is_tombstone())
    }

    /// Mark `(channel, pair)` acknowledged (`Pending` → `Acked` only). No-op if
    /// the entry is gone (deregistered mid-flight) or `Terminated` — an ack is
    /// not a revival, so a stray ack cannot resurrect a tombstone (a genuine
    /// revival was already reset to `Pending` by `register` before its fresh ack).
    pub(crate) fn note_acked_for(
        &mut self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
        now: MonotonicInstant,
    ) -> bool {
        match self.entry_mut(url, channel, pair) {
            Some(e) if matches!(e.state, EntrySubState::Pending) => {
                e.state = EntrySubState::Acked;
            }
            _ => return false,
        }
        self.mirror_mark_acked(url, channel, pair, now);
        true
    }

    fn mirror_mark_acked(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
        now: MonotonicInstant,
    ) {
        self.mirror_write(|mirror| {
            if let Some(row) = mirror.get_mut(&(url, channel, pair.clone())) {
                row.mark_acked(now);
            }
        });
    }

    /// Unified Public has one venue `book` subscription per symbol.  The
    /// maintained and raw logical rows therefore become active together when
    /// that one wire subscription is acknowledged.
    pub(crate) fn note_unified_book_acked_for(
        &mut self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
        ack: &crate::dispatch::FuturesSubscribeAck,
        now: MonotonicInstant,
    ) -> Vec<ChannelName> {
        if url.shares_unified_book_stream(channel) {
            if let Some(entry) = self.entry_mut(url, ChannelName::Book, pair)
                && entry.awaiting_seed_snapshot
            {
                entry.seed_ack = Some(ack.clone());
            }
            let mut acked = Vec::with_capacity(2);
            for channel in [ChannelName::Book, ChannelName::BookRaw] {
                if channel == ChannelName::Book
                    && self
                        .entry(url, channel, pair)
                        .is_some_and(|entry| entry.awaiting_seed_snapshot)
                {
                    continue;
                }
                if self.note_acked_for(url, channel, pair, now) {
                    acked.push(channel);
                }
            }
            acked
        } else {
            self.note_acked_for(url, channel, pair, now)
                .then_some(channel)
                .into_iter()
                .collect()
        }
    }

    /// Whether a distinct Unified `book` logical row still owns the same
    /// symbol.  Used to gate the single venue unsubscribe.
    pub(crate) fn unified_book_peer_live(
        &self,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) -> bool {
        let Some(peer) = channel.book_peer() else {
            return false;
        };
        self.entry(WsUrl::UnifiedPublic, peer, pair)
            .is_some_and(|entry| !entry.is_tombstone() && entry.count > 0)
    }

    /// Whether the shared Unified `book` wire subscription has already been
    /// acknowledged through the other logical book row.
    pub(crate) fn unified_book_peer_acked(
        &self,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) -> bool {
        let Some(peer) = channel.book_peer() else {
            return false;
        };
        self.entry(WsUrl::UnifiedPublic, peer, pair)
            .is_some_and(SubscriptionEntry::wire_acked)
    }

    /// `true` while a maintained Unified book is still waiting for the snapshot
    /// that makes it caller-visible.
    pub(crate) fn unified_book_awaiting_seed(&self, pair: &Option<StreamSymbol>) -> bool {
        self.entry(WsUrl::UnifiedPublic, ChannelName::Book, pair)
            .is_some_and(|entry| {
                entry.awaiting_seed_snapshot && matches!(entry.state, EntrySubState::Pending)
            })
    }

    /// Hold a late maintained Unified book in `Pending` until its reseed
    /// snapshot has been validated. The raw peer remains active throughout.
    pub(crate) fn require_unified_book_seed_for(&mut self, pair: &Option<StreamSymbol>) {
        if let Some(entry) = self.entry_mut(WsUrl::UnifiedPublic, ChannelName::Book, pair) {
            entry.awaiting_seed_snapshot = true;
        }
    }

    pub(crate) fn take_unified_book_seed_ack(
        &mut self,
        pair: &Option<StreamSymbol>,
    ) -> crate::dispatch::FuturesSubscribeAck {
        self.entry_mut(WsUrl::UnifiedPublic, ChannelName::Book, pair)
            .and_then(|entry| entry.seed_ack.take())
            .unwrap_or_default()
    }

    /// Promote a late maintained Unified book only after its fresh snapshot was
    /// successfully applied to the builder. Returns whether the transition ran.
    pub(crate) fn note_unified_book_seeded_for(
        &mut self,
        pair: &Option<StreamSymbol>,
        now: MonotonicInstant,
    ) -> bool {
        let promoted = match self.entry_mut(WsUrl::UnifiedPublic, ChannelName::Book, pair) {
            Some(entry)
                if entry.awaiting_seed_snapshot
                    && matches!(entry.state, EntrySubState::Pending) =>
            {
                entry.awaiting_seed_snapshot = false;
                entry.state = EntrySubState::Acked;
                true
            }
            _ => false,
        };
        if promoted {
            self.mirror_mark_acked(WsUrl::UnifiedPublic, ChannelName::Book, pair, now);
        }
        promoted
    }

    /// Mark `(channel, pair)` terminated with `cause`, keeping the row (and the
    /// authoritative entry) as a visible tombstone until a re-subscribe revives
    /// it or a caller-driven removal clears the key.
    pub(crate) fn note_terminated_for(
        &mut self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
        cause: TerminationCause,
        last_error: Option<String>,
    ) {
        // One terminal transition per lifetime: keep the tombstone visible until
        // the last holder releases. A repeat terminal is a pure no-op.
        match self.entry_mut(url, channel, pair) {
            Some(e) if !matches!(e.state, EntrySubState::Terminated { .. }) => {
                e.tombstone_holders = e.count;
                e.count = 0;
                // Countdown from THIS lifetime's holders only; outstanding credits
                // survive as a separate ledger.
                e.state = EntrySubState::Terminated {
                    cause,
                    last_error: last_error.clone(),
                };
            }
            _ => return,
        }
        self.liveness_epoch += 1;
        self.mirror_write(|mirror| {
            if let Some(row) = mirror.get_mut(&(url, channel, pair.clone())) {
                row.state = EntrySubState::Terminated { cause, last_error };
            }
        });
    }

    /// Project the internal per-entry state to the public [`SubscriptionState`].
    /// The internal [`TerminationCause`] maps 1:1 into [`SubscribeFailureCause`];
    /// Kraken's single verbatim reject string fills both `code` and `message`.
    pub(crate) fn project_state(state: &EntrySubState) -> SubscriptionState {
        match state {
            EntrySubState::Pending => SubscriptionState::Pending,
            EntrySubState::Acked => SubscriptionState::Active,
            EntrySubState::Terminated { cause, last_error } => {
                SubscriptionState::Failed(match cause {
                    TerminationCause::SubscribeAckBudgetExhausted => {
                        SubscribeFailureCause::SubscribeAckBudgetExhausted
                    }
                    TerminationCause::NonTransientWireRejection => {
                        let err = last_error.clone().unwrap_or_default();
                        SubscribeFailureCause::NonTransientWireRejection {
                            code: err.clone(),
                            message: err,
                        }
                    }
                    TerminationCause::CapabilityRevoked => SubscribeFailureCause::CapabilityRevoked,
                    TerminationCause::ClientClosed => SubscribeFailureCause::ClientClosed,
                })
            }
        }
    }

    /// Drive maintained-book state for a live `(channel, pair)` from an inbound
    /// frame; does not invoke handlers (fan-out is the reactor's job). A non-live
    /// entry yields [`BookDriveOutcome::PassThrough`].
    pub fn handle_update_for(
        &mut self,
        url: WsUrl,
        channel: ChannelName,
        pair: Option<&StreamSymbol>,
        is_snapshot: bool,
        data: &serde_json::Value,
        now: impl FnOnce() -> MonotonicInstant,
    ) -> BookDriveOutcome {
        let entry = match pair {
            Some(sym) => self
                .entries
                .get_mut(&(url, channel))
                .and_then(|m| m.get_mut(sym)),
            None => self.channel_wide.get_mut(&(url, channel)),
        };
        let outcome = match entry {
            Some(e) => e.drive_update(is_snapshot, data),
            None if url == WsUrl::UnifiedPublic && channel == ChannelName::Book => {
                BookDriveOutcome::NoEntry
            }
            None => BookDriveOutcome::PassThrough,
        };
        if matches!(outcome, BookDriveOutcome::GapBudgetExhausted) {
            let owned = pair.cloned();
            let acked = self
                .entry(url, channel, &owned)
                .is_some_and(|entry| matches!(entry.state, EntrySubState::Acked));
            if acked {
                self.mirror_mark_acked(url, channel, &owned, now());
            }
        }
        outcome
    }

    /// Drop the maintained book and enter the resync window after a CRC32 gap.
    /// No-op if the entry is absent or builder-less. Called before the WS
    /// unsubscribe+resubscribe so interim deltas drop until the fresh snapshot.
    pub fn begin_book_resync_for(&mut self, url: WsUrl, channel: ChannelName, pair: &StreamSymbol) {
        if let Some(e) = self
            .entries
            .get_mut(&(url, channel))
            .and_then(|m| m.get_mut(pair))
            && let Some(b) = e.builder.as_mut()
        {
            b.begin_resync();
        }
    }

    /// `true` if the maintained book for `(channel, pair)` is in the post-gap
    /// resync window. The reconnect replay uses it to arm a reseed-liveness timer
    /// so a lost post-reconnect snapshot can't silently re-freeze `on_book`.
    pub fn is_book_resyncing_for(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) -> bool {
        let Some(sym) = pair.as_ref() else {
            return false;
        };
        self.entries
            .get(&(url, channel))
            .and_then(|m| m.get(sym))
            .and_then(|e| e.builder.as_ref())
            .is_some_and(|b| b.is_resyncing())
    }

    /// Reseed liveness-timer fired: if still resyncing, count a consecutive
    /// failure — at the cap, drop maintenance (degrade to PassThrough) and return
    /// `Exhausted`, else `Retry`. A stale timer (recovered/gone) is `StaleNoOp`.
    pub fn note_reseed_timeout_for(
        &mut self,
        url: WsUrl,
        channel: ChannelName,
        pair: &StreamSymbol,
        now: MonotonicInstant,
    ) -> ReseedTimeoutOutcome {
        let (outcome, promoted) = {
            let Some(entry) = self
                .entries
                .get_mut(&(url, channel))
                .and_then(|m| m.get_mut(pair))
            else {
                return ReseedTimeoutOutcome::StaleNoOp;
            };
            if entry.is_tombstone() {
                // Revival is caller-re-subscribe only — a leftover reseed timer must
                // never replay a tombstone's subscribe.
                return ReseedTimeoutOutcome::StaleNoOp;
            }
            let Some(builder) = entry.builder.as_mut() else {
                return ReseedTimeoutOutcome::StaleNoOp; // already degraded to PassThrough
            };
            if !builder.is_resyncing() {
                return ReseedTimeoutOutcome::StaleNoOp; // snapshot landed — stale timer
            }
            if builder.note_reseed_failure() >= MAX_CONSECUTIVE_BOOK_GAPS {
                (
                    ReseedTimeoutOutcome::Exhausted,
                    entry.degrade_to_passthrough(),
                )
            } else {
                (ReseedTimeoutOutcome::Retry, false)
            }
        };
        if promoted {
            self.mirror_mark_acked(url, channel, &Some(pair.clone()), now);
        }
        outcome
    }

    /// Every per-pair entry across all channel buckets (the nested map flattened).
    fn pair_entries(&self) -> impl Iterator<Item = &SubscriptionEntry> + '_ {
        self.entries.values().flat_map(HashMap::values)
    }

    /// Entries routing to `url` in the locked replay order, single-sourced
    /// from `subscription_order_key` — a cross-binding contract. Terminated
    /// tombstones are excluded: revival is caller-re-subscribe only.
    fn entries_for_resubscribe_sorted(&self, url: WsUrl) -> Vec<&SubscriptionEntry> {
        let mut entries: Vec<&SubscriptionEntry> = self
            .channel_wide
            .values()
            .chain(self.pair_entries())
            .filter(|e| e.url == url && !e.is_tombstone())
            .collect();
        entries.sort_by(|a, b| {
            subscription_order_key(a.channel, a.pair.as_ref())
                .cmp(&subscription_order_key(b.channel, b.pair.as_ref()))
        });
        entries
    }

    /// Compose subscribe frames for every entry routing to `url`, in deterministic
    /// replay order (post-reconnect replay). Returns `(channel, pair, frame)` so
    /// the reactor can arm ack timers without re-parsing the JSON.
    pub(crate) fn compose_subscribe_frames_for_url(
        &self,
        url: WsUrl,
    ) -> Vec<(ChannelName, Option<StreamSymbol>, Value)> {
        self.compose_subscribe_frames_for_url_only(url, None)
    }

    pub(crate) fn compose_subscribe_frames_for_url_only(
        &self,
        url: WsUrl,
        only_keys: Option<&std::collections::HashSet<(ChannelName, Option<StreamSymbol>)>>,
    ) -> Vec<(ChannelName, Option<StreamSymbol>, Value)> {
        // Keepalive FIRST. v1 public edge has no keepalive (auth-connection-only);
        // the structural placeholder preserves the cross-binding deterministic order.
        let mut out = Vec::new();
        for e in self.entries_for_resubscribe_sorted(url) {
            if only_keys.is_some_and(|keys| !keys.contains(&(e.channel, e.pair.clone()))) {
                continue;
            }
            let pairs = e.pair.as_slice();
            // `req_id` placeholder `0` — reactor stamps a real id at send time.
            let frame = build_subscribe_frame(url, "subscribe", e.channel, pairs, e.params, 0);
            out.push((e.channel, e.pair.clone(), frame));
        }
        out
    }

    /// Compose Unified Public replay frames, batching live per-symbol entries
    /// with identical channel/options. Registry ownership remains per symbol.
    pub(crate) fn compose_unified_public_subscribe_batches(
        &self,
        only_keys: Option<&std::collections::HashSet<(ChannelName, Option<StreamSymbol>)>>,
    ) -> Vec<(ChannelName, Vec<Option<StreamSymbol>>, Value)> {
        let mut out = Vec::new();
        let mut entries = self.entries_for_resubscribe_sorted(WsUrl::UnifiedPublic);
        if let Some(keys) = only_keys {
            entries.retain(|entry| {
                keys.contains(&(entry.channel, entry.pair.clone()))
                    // A BookRaw key is the only replay mark when the maintained
                    // row joined while that raw row was still pending. The
                    // filter below drops BookRaw once Book is live, so keep the
                    // Book row or the shared venue subscribe is never sent.
                    || (entry.channel == ChannelName::Book
                        && keys.contains(&(ChannelName::BookRaw, entry.pair.clone())))
            });
        }
        // `Book` and `BookRaw` are two logical consumers of one venue book
        // stream. Keep the maintained row when present; compatible requests
        // have the same effective wire parameters.
        entries.retain(|entry| {
            !self.unified_book_peer_live(entry.channel, &entry.pair)
                || entry.channel == ChannelName::Book
        });
        let mut index = 0;
        while index < entries.len() {
            let first = entries[index];
            let channel = first.channel;
            let params = first.params;
            let mut pairs = Vec::new();
            while let Some(pair) = entries
                .get(index)
                .filter(|entry| entry.channel == channel && entry.params == params)
                .and_then(|entry| entry.pair.clone())
            {
                pairs.push(pair);
                index += 1;
            }
            let frame = build_subscribe_frame(
                WsUrl::UnifiedPublic,
                "subscribe",
                channel,
                &pairs,
                params,
                0,
            );
            let keys = if pairs.is_empty() {
                // Channel-wide Unified entries (for example instrument) never
                // share a symbol batch.
                index += 1;
                vec![None]
            } else {
                pairs.into_iter().map(Some).collect()
            };
            out.push((channel, keys, frame));
        }
        out
    }

    /// Like [`compose_subscribe_frames_for_url`](Self::compose_subscribe_frames_for_url)
    /// but injects `token` into each frame's `params` object (the token rides inside
    /// `params`). Used by the Auth edge; public-URL frames use the plain composer.
    pub(crate) fn compose_subscribe_frames_for_url_authed(
        &self,
        url: WsUrl,
        token: &str,
    ) -> Vec<(ChannelName, Option<StreamSymbol>, Value)> {
        let mut frames = self.compose_subscribe_frames_for_url(url);
        for (_channel, _pair, payload) in frames.iter_mut() {
            inject_token(payload, token);
        }
        frames
    }

    /// Compose only the first (deterministic-order) Auth-URL subscribe frame,
    /// token-injected — its ack gates `Authenticating → Resubscribing`. `None` when
    /// no entry routes to `url`. The rest of the batch is replayed afterward.
    pub(crate) fn compose_first_signed_subscribe_authed(
        &self,
        url: WsUrl,
        token: &str,
    ) -> Option<(ChannelName, Option<StreamSymbol>, Value)> {
        let first = self
            .entries_for_resubscribe_sorted(url)
            .into_iter()
            .next()?;
        let pairs = first.pair.as_slice();
        // `req_id` placeholder `0` — stamped reactor-side at send time.
        let mut frame =
            build_subscribe_frame(url, "subscribe", first.channel, pairs, first.params, 0);
        inject_token(&mut frame, token);
        Some((first.channel, first.pair.clone(), frame))
    }

    /// The Auth-URL frames except the already-sent first one — the remainder
    /// replayed in `Resubscribing` after the handshake. Token-injected. Empty when
    /// the auth registry has ≤1 entry.
    pub(crate) fn compose_remaining_signed_subscribes_authed(
        &self,
        url: WsUrl,
        token: &str,
        already_sent: &(ChannelName, Option<StreamSymbol>),
    ) -> Vec<(ChannelName, Option<StreamSymbol>, Value)> {
        let mut frames = self.compose_subscribe_frames_for_url_authed(url, token);
        // Exclude the already-sent first subscribe by (channel, pair) identity, not
        // position: `remove(0)` could skip a channel and re-send the acked one.
        frames
            .retain(|(channel, pair, _)| !(*channel == already_sent.0 && pair == &already_sent.1));
        frames
    }

    /// The `(channel, pair, params)` keys the authed replay composition would
    /// send for `url`, minus `exclude` (the already-sent first subscribe).
    /// Used to defer exactly that set when the cached token is missing/expired.
    pub(crate) fn live_subscribe_keys_for_url(
        &self,
        url: WsUrl,
        exclude: Option<&(ChannelName, Option<StreamSymbol>)>,
    ) -> Vec<(ChannelName, Option<StreamSymbol>, SubscribeParams)> {
        self.entries_for_resubscribe_sorted(url)
            .into_iter()
            .filter(|e| !exclude.is_some_and(|x| e.channel == x.0 && e.pair == x.1))
            .map(|e| (e.channel, e.pair.clone(), e.params))
            .collect()
    }

    /// Mark every live entry of `url` for the first-open replay.
    /// Used after the pre-`Open` socket is discarded: a `Sent` result only means
    /// the frame reached that socket's outbox, and an ack on that socket is not
    /// a subscription on its replacement. An `Acked` row is demoted to `Pending`
    /// so the replacement ack is observed again.
    pub(crate) fn mark_pending_entries_for_initial_replay(
        &mut self,
        url: WsUrl,
        mc: &mut crate::conn::ManagedConnection,
    ) {
        let keys: Vec<(ChannelName, Option<StreamSymbol>)> = self
            .entries_for_resubscribe_sorted(url)
            .into_iter()
            .filter(|entry| matches!(entry.state, EntrySubState::Pending | EntrySubState::Acked))
            .map(|entry| (entry.channel, entry.pair.clone()))
            .collect();
        for (channel, pair) in keys {
            if let Some(entry) = self.entry_mut(url, channel, &pair)
                && matches!(entry.state, EntrySubState::Acked)
            {
                entry.state = EntrySubState::Pending;
                self.mirror_write(|mirror| {
                    if let Some(row) = mirror.get_mut(&(url, channel, pair.clone())) {
                        row.state = EntrySubState::Pending;
                        row.subscribed_at = None;
                    }
                });
            }
            mc.mark_initial_subscribe_replay(channel, std::slice::from_ref(&pair));
        }
    }

    pub(crate) fn drop_snapshot_opt_outs(
        &mut self,
        url: WsUrl,
        written: &std::collections::HashSet<ChannelName>,
    ) {
        let keep = |requested: Option<bool>| requested.filter(|&on| on);
        for e in self
            .channel_wide
            .values_mut()
            .filter(|e| e.url == url && written.contains(&e.channel))
        {
            e.params = match e.params {
                SubscribeParams::Executions {
                    snap_orders,
                    snap_trades,
                } => SubscribeParams::Executions {
                    snap_orders: keep(snap_orders),
                    snap_trades: keep(snap_trades),
                },
                SubscribeParams::Balances { snapshot } => SubscribeParams::Balances {
                    snapshot: keep(snapshot),
                },
                SubscribeParams::Positions { snapshot } => SubscribeParams::Positions {
                    snapshot: keep(snapshot),
                },
                params @ (SubscribeParams::Ticker { .. }
                | SubscribeParams::Book { .. }
                | SubscribeParams::BookRaw { .. }
                | SubscribeParams::Trade { .. }
                | SubscribeParams::Ohlc { .. }
                | SubscribeParams::Status
                | SubscribeParams::Instrument
                | SubscribeParams::Margin
                | SubscribeParams::Preferences) => params,
            };
        }
    }

    /// Whether any non-terminated entry on `url` is still waiting for its ack.
    pub(crate) fn has_pending_entry(&self, url: WsUrl) -> bool {
        self.pair_entries()
            .chain(self.channel_wide.values())
            .any(|entry| {
                entry.url == url
                    && !entry.is_tombstone()
                    && matches!(entry.state, EntrySubState::Pending)
            })
    }

    /// Does any LIVE (non-terminated) entry route to `url`? Gates the order
    /// self-auth probe and auth send-ready: tombstones are never replayed on
    /// reconnect, so an all-tombstone registry counts as empty.
    pub(crate) fn has_url_entry(&self, url: WsUrl) -> bool {
        self.pair_entries()
            .chain(self.channel_wide.values())
            .any(|e| e.url == url && !e.is_tombstone())
    }

    /// Look up the `SubscribeParams` for `(channel, pair)`, used to recompose a
    /// resend frame. `None` if the entry was deregistered while the resend timer
    /// was pending (the resend then no-ops).
    pub(crate) fn params_for_url(
        &self,
        url: WsUrl,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) -> Option<SubscribeParams> {
        match pair {
            Some(sym) => self
                .entries
                .get(&(url, channel))
                .and_then(|m| m.get(sym))
                .map(|e| e.params),
            None => self.channel_wide.get(&(url, channel)).map(|e| e.params),
        }
    }

    /// Total registered entries (per-pair + channel-wide). Test helper; the
    /// live emptiness check is `has_url_entry`.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.values().map(HashMap::len).sum::<usize>() + self.channel_wide.len()
    }

    /// Outstanding guard-ref records. Test helper: pins that every guard drop
    /// consumes its record (no leak after a forced teardown removed the key).
    #[cfg(test)]
    pub(crate) fn guard_ref_count(&self) -> usize {
        self.guard_refs.len()
    }

    /// Test-only: the tombstone-holder countdown for `(channel, pair)`.
    #[cfg(test)]
    pub(crate) fn tombstone_holders(
        &self,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) -> u32 {
        self.entry(channel.spot_url(), channel, pair)
            .map(|e| e.tombstone_holders)
            .unwrap_or(0)
    }

    /// Test-only: the stale-release credits for `(channel, pair)`.
    #[cfg(test)]
    pub(crate) fn stale_release_credits(
        &self,
        channel: ChannelName,
        pair: &Option<StreamSymbol>,
    ) -> u32 {
        self.entry(channel.spot_url(), channel, pair)
            .map(|e| e.stale_release_credits)
            .unwrap_or(0)
    }

    /// Is the registry empty? Test helper twin of `len`.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.entries.values().all(HashMap::is_empty) && self.channel_wide.is_empty()
    }
}

/// Deterministic subscription ordering: channel-wide rows first (by wire
/// channel name), then per-pair rows by wire name + symbol. `Book` and
/// `BookRaw` share the wire name `book`; the tie-break keeps the maintained
/// row before the raw row. Single source for the reconnect replay,
/// `list_active` read, and `DeregisterAll` order within each socket.
pub(crate) fn subscription_order_key<P>(
    channel: ChannelName,
    pair: Option<P>,
) -> (bool, &'static str, u8, Option<P>) {
    let tie = u8::from(channel == ChannelName::BookRaw);
    (pair.is_some(), <&str>::from(channel), tie, pair)
}

pub(crate) fn frame_params_base(
    url: WsUrl,
    channel: ChannelName,
    pairs: &[StreamSymbol],
) -> serde_json::Map<String, Value> {
    let symbols_wire: Vec<String> = pairs.iter().map(|s| s.as_str().to_string()).collect();
    let mut p = serde_json::Map::new();
    p.insert("channel".to_string(), Value::String(channel.to_string()));
    if let Some(asset_class) = url.subscription_asset_class(channel) {
        p.insert(
            "asset_class".to_string(),
            Value::String(asset_class.to_string()),
        );
    }
    if !symbols_wire.is_empty() {
        p.insert("symbol".to_string(), serde_json::json!(symbols_wire));
    }
    p
}

/// Build a Kraken WS v2 subscribe / unsubscribe frame (`method` is `"subscribe"`
/// or `"unsubscribe"`). Lives in `conn/` so the initial-connect path and the
/// reconnect composer build frames identically — recompose-from-params.
pub(crate) fn build_subscribe_frame(
    url: WsUrl,
    method: &str,
    channel: ChannelName,
    pairs: &[StreamSymbol],
    params: SubscribeParams,
    req_id: u64,
) -> Value {
    let mut p = frame_params_base(url, channel, pairs);
    match params {
        SubscribeParams::Ticker {
            snapshot,
            event_trigger,
        } => {
            // Ticker params sent only on subscribe; omitted params take Kraken defaults.
            if method == "subscribe" {
                if let Some(s) = snapshot {
                    p.insert("snapshot".to_string(), Value::Bool(s));
                }
                if let Some(t) = event_trigger {
                    p.insert("event_trigger".to_string(), Value::String(t.to_string()));
                }
            }
        }
        SubscribeParams::Book { depth } => {
            // Maintained book subscribes at the bumped wire depth for CRC headroom
            // (BookDepth::wire_depth); the caller sees their requested depth via build_update.
            p.insert(
                "depth".to_string(),
                serde_json::json!(depth.wire_depth().as_wire_u32()),
            );
            // Maintained book ALWAYS requests the opening snapshot (seeds the builder + CRC
            // baseline). snapshot is therefore not caller-settable — use book_raw for delta-only.
            if method == "subscribe" {
                p.insert("snapshot".to_string(), Value::Bool(true));
            }
        }
        SubscribeParams::BookRaw { depth, snapshot } => {
            // Raw deltas send the caller's exact depth; snapshot omitted → Kraken default
            p.insert("depth".to_string(), serde_json::json!(depth.as_wire_u32()));
            if method == "subscribe"
                && let Some(s) = snapshot
            {
                p.insert("snapshot".to_string(), Value::Bool(s));
            }
        }
        SubscribeParams::Trade { snapshot }
        | SubscribeParams::Balances { snapshot }
        | SubscribeParams::Positions { snapshot } => {
            // snapshot omitted → Kraken default
            if method == "subscribe"
                && let Some(s) = snapshot
            {
                p.insert("snapshot".to_string(), Value::Bool(s));
            }
        }
        SubscribeParams::Executions {
            snap_orders,
            snap_trades,
        } => {
            if method == "subscribe" {
                for (key, value) in [("snap_orders", snap_orders), ("snap_trades", snap_trades)] {
                    if let Some(v) = value {
                        p.insert(key.to_string(), Value::Bool(v));
                    }
                }
            }
        }
        SubscribeParams::Ohlc {
            interval,
            snapshot,
            tick_type,
        } => {
            // Unsubscribe must echo the subscribed interval or it fails with
            // "Subscription Not Found" and the stream stays live (wire-verified).
            p.insert(
                "interval".to_string(),
                serde_json::json!(u64::from(interval)),
            );
            if let Some(t) = tick_type {
                p.insert("tick_type".to_string(), Value::String(t.to_string()));
            }
            // `snapshot` is subscribe-only; omitted when None → Kraken's default.
            if method == "subscribe"
                && let Some(s) = snapshot
            {
                p.insert("snapshot".to_string(), Value::Bool(s));
            }
        }
        SubscribeParams::Status
        | SubscribeParams::Instrument
        | SubscribeParams::Margin
        | SubscribeParams::Preferences => {}
    }
    serde_json::json!({
        "method": method,
        "params": Value::Object(p),
        // req_id: Kraken echoes it, used to correlate channel-less rejects
        "req_id": req_id,
    })
}

/// Stamp a monotonic `req_id` into a composed subscribe frame. Batch composers
/// build frames with a `0` placeholder; the reactor calls this per frame at send
/// time so each in-flight subscribe is correlatable. No-op (warns) on a non-object.
pub(crate) fn stamp_req_id(frame: &mut Value, req_id: u64) {
    if let Some(obj) = frame.as_object_mut() {
        obj.insert("req_id".to_string(), serde_json::json!(req_id));
    } else {
        tracing::warn!(
            target: "kraken_sdk::conn",
            "stamp_req_id: subscribe frame is not a JSON object; req_id not stamped"
        );
    }
}

/// Inject `token` into a frame's `params` object, alongside `channel` / `symbol`,
/// as the Kraken WS v2 auth protocol expects. Shared by the Auth-edge composers.
/// No-op (warns) if `params` is not an object.
pub(crate) fn inject_token(frame: &mut Value, token: &str) {
    if let Some(params) = frame.get_mut("params").and_then(Value::as_object_mut) {
        params.insert("token".to_string(), Value::String(token.to_string()));
    } else {
        tracing::warn!(
            target: "kraken_sdk::conn",
            "inject_token: frame has no `params` object; token not injected"
        );
    }
}

#[cfg(test)]
#[path = "subscription_registry_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "subscription_registry_sweep_tests.rs"]
mod sweep_tests;
