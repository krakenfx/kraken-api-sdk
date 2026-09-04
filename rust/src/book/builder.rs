//! `OrderBookBuilder` — maintained top-N book with CRC32 validation.
//! Wire decimal strings are stored alongside typed `Decimal` so the checksum is byte-equal.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::Serialize;
use serde_with::skip_serializing_none;

use crate::types::{MonotonicInstant, Symbol};

/// One price level — typed `Decimal` plus the exact wire string (CRC32 source).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct PriceLevel {
    /// Price of this level, parsed from `price_wire`.
    pub price: Decimal,
    /// Quantity at this price; `0` in a delta means remove the level.
    pub qty: Decimal,
    /// Exact price string as Kraken sent it — CRC32 source (never rebuilt from `Decimal`).
    pub price_wire: String,
    /// Exact quantity string as Kraken sent it — CRC32 source (never rebuilt from `Decimal`).
    pub qty_wire: String,
}

/// One price level on the maintained [`OrderBookUpdate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct BookLevel {
    /// Price of this level; unique within its side.
    pub price: Decimal,
    /// Quantity at this price; never zero (zero-qty levels are removed).
    pub qty: Decimal,
}

impl From<&PriceLevel> for BookLevel {
    fn from(l: &PriceLevel) -> Self {
        BookLevel {
            price: l.price,
            qty: l.qty,
        }
    }
}

/// Caller-facing maintained-book update; both sides capped to the requested depth.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct OrderBookUpdate {
    /// Pair the book belongs to (e.g. `BTC/USD`).
    pub symbol: Symbol,
    /// Bid levels, highest price first, capped to requested depth.
    pub bids: Vec<BookLevel>,
    /// Ask levels, lowest price first, capped to requested depth.
    pub asks: Vec<BookLevel>,
    /// Kraken's CRC32 for this update — validated in maintained mode.
    pub checksum: u32,
    /// Reserved for the SDK monotonic clock; always `None` in v1.
    pub timestamp: Option<MonotonicInstant>,
    /// Exchange wall-clock (RFC 3339) from the wire; `""` when absent.
    pub exchange_timestamp: String,
}

/// Raw-mode frame — no CRC validation, no maintained state. Snapshot replaces;
/// delta applies. See docs/guides/order-book.md.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct BookDelta {
    /// Pair the frame belongs to (e.g. `BTC/USD`).
    pub symbol: Symbol,
    /// Bid levels: full depth on snapshot, changed levels on delta (`qty == 0` removes).
    pub bids: Vec<PriceLevel>,
    /// Ask levels: full depth on snapshot, changed levels on delta (`qty == 0` removes).
    pub asks: Vec<PriceLevel>,
    /// Kraken's CRC32 over the top-10 post-apply book; unvalidated in raw mode.
    pub checksum: u32,
    /// `true` for snapshot (replace); `false` for delta (apply).
    pub is_snapshot: bool,
    /// Reserved for the SDK monotonic clock; always `None` in v1.
    pub timestamp: Option<MonotonicInstant>,
    /// Exchange wall-clock (RFC 3339) from the wire; `""` when absent.
    pub exchange_timestamp: String,
}

/// Errors from `apply_snapshot` / `apply_delta`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyError {
    /// CRC32 mismatch; reactor must emit `SubscriptionGapEvent` and resubscribe.
    ChecksumMismatch { expected: u32, computed: u32 },
}

/// Bid-side sort key — inverts `Decimal` so `BTreeMap` yields highest-price-first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DescPrice(Decimal);

impl Ord for DescPrice {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.0.cmp(&self.0)
    }
}

impl PartialOrd for DescPrice {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A delta deletes only when the WIRE qty is an exact zero form. A wire-nonzero
/// qty that underflows `Decimal` to zero (sub-1e-28) must NOT delete.
fn delta_level_deletes(lvl: &PriceLevel) -> bool {
    let wire_zero = !lvl
        .qty_wire
        .bytes()
        .any(|b| b.is_ascii_digit() && b != b'0');
    if !wire_zero && lvl.qty.is_zero() {
        tracing::warn!(
            target: "kraken_sdk::book",
            price = %lvl.price_wire,
            qty = %lvl.qty_wire,
            "book delta qty underflowed Decimal to zero but wire is non-zero; keeping level"
        );
    }
    wire_zero
}

/// Maintained book state for one `(channel, symbol)`. Mutated only on the I/O reactor.
#[derive(Debug, Clone)]
pub struct OrderBookBuilder {
    symbol: Symbol,
    /// Depth maintained + trimmed to (possibly-bumped wire depth). Must be `> CHECKSUM_DEPTH`.
    maintained_depth: usize,
    /// Depth the caller requested — the cap `build_update` applies.
    caller_depth: usize,
    bids: BTreeMap<DescPrice, PriceLevel>,
    asks: BTreeMap<Decimal, PriceLevel>,
    last_checksum: Option<u32>,
    /// `true` between [`begin_resync`] and the next [`apply_snapshot`]; deltas are dropped.
    resyncing: bool,
    /// Consecutive CRC32 mismatches; bounds the gap-recovery loop.
    consecutive_gaps: u32,
}

impl OrderBookBuilder {
    /// `maintained_depth` must be `> CHECKSUM_DEPTH`; `caller_depth <= maintained_depth`.
    pub fn new(symbol: Symbol, maintained_depth: usize, caller_depth: usize) -> Self {
        debug_assert!(
            caller_depth <= maintained_depth,
            "caller_depth {caller_depth} must not exceed maintained_depth {maintained_depth}",
        );
        Self {
            symbol,
            maintained_depth,
            caller_depth,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_checksum: None,
            resyncing: false,
            consecutive_gaps: 0,
        }
    }

    /// Consecutive CRC32 mismatches since the last validating apply.
    pub fn consecutive_gaps(&self) -> u32 {
        self.consecutive_gaps
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// Depth the book is maintained + trimmed to (the wire depth).
    #[cfg(test)]
    pub fn maintained_depth(&self) -> usize {
        self.maintained_depth
    }

    /// Depth the caller requested — the cap on the emitted update.
    #[cfg(test)]
    pub fn caller_depth(&self) -> usize {
        self.caller_depth
    }

    /// Drop local book state on a CRC32 gap and enter the resync window.
    pub fn begin_resync(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_checksum = None;
        self.resyncing = true;
    }

    /// `true` while awaiting the post-gap fresh snapshot.
    pub fn is_resyncing(&self) -> bool {
        self.resyncing
    }

    /// Register a reseed failure that is not a CRC mismatch; returns the new gap count.
    pub fn note_reseed_failure(&mut self) -> u32 {
        self.consecutive_gaps = self.consecutive_gaps.saturating_add(1);
        self.consecutive_gaps
    }

    /// Seed from a fresh snapshot, replacing prior state; builder re-sorts for CRC32.
    pub fn apply_snapshot(
        &mut self,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        checksum: u32,
    ) -> Result<(), ApplyError> {
        self.bids.clear();
        self.asks.clear();
        for lvl in bids {
            self.bids.insert(DescPrice(lvl.price), lvl);
        }
        for lvl in asks {
            self.asks.insert(lvl.price, lvl);
        }
        // Snapshot ends the resync window even on mismatch (then re-gaps, bounded by retry budget).
        self.resyncing = false;
        self.trim_to_depth();
        self.validate_and_record(checksum)
    }

    /// Apply an incremental delta: wire-zero qty removes; else insert/replace.
    pub fn apply_delta(
        &mut self,
        bid_updates: Vec<PriceLevel>,
        ask_updates: Vec<PriceLevel>,
        checksum: u32,
    ) -> Result<(), ApplyError> {
        for lvl in bid_updates {
            if delta_level_deletes(&lvl) {
                self.bids.remove(&DescPrice(lvl.price));
            } else {
                self.bids.insert(DescPrice(lvl.price), lvl);
            }
        }
        for lvl in ask_updates {
            if delta_level_deletes(&lvl) {
                self.asks.remove(&lvl.price);
            } else {
                self.asks.insert(lvl.price, lvl);
            }
        }
        self.trim_to_depth();
        self.validate_and_record(checksum)
    }

    /// Trim both sides to `maintained_depth`. Call after inserts, before validate.
    fn trim_to_depth(&mut self) {
        while self.bids.len() > self.maintained_depth {
            let _ = self.bids.pop_last();
        }
        while self.asks.len() > self.maintained_depth {
            let _ = self.asks.pop_last();
        }
    }

    fn validate_and_record(&mut self, expected: u32) -> Result<(), ApplyError> {
        let computed = self.compute_checksum();
        if computed != expected {
            // Streak is NOT reset by `begin_resync` — only a validating apply ends it.
            self.consecutive_gaps = self.consecutive_gaps.saturating_add(1);
            return Err(ApplyError::ChecksumMismatch { expected, computed });
        }
        self.last_checksum = Some(computed);
        self.consecutive_gaps = 0;
        Ok(())
    }

    /// CRC32 over the current top-`CHECKSUM_DEPTH` levels.
    pub fn compute_checksum(&self) -> u32 {
        use super::checksum::CHECKSUM_DEPTH;
        // Correct only while each side keeps strictly more than CHECKSUM_DEPTH (promotion headroom).
        debug_assert!(
            self.maintained_depth > CHECKSUM_DEPTH,
            "maintained depth {} <= CHECKSUM_DEPTH {}: no promotion headroom, \
             trim_to_depth drops levels the checksum needs across boundary removals",
            self.maintained_depth,
            CHECKSUM_DEPTH,
        );
        crate::book::compute_book_crc32(
            self.asks
                .values()
                .take(CHECKSUM_DEPTH)
                .map(|l| (l.price_wire.as_str(), l.qty_wire.as_str())),
            self.bids
                .values()
                .take(CHECKSUM_DEPTH)
                .map(|l| (l.price_wire.as_str(), l.qty_wire.as_str())),
        )
    }

    /// Caller-facing update, depth-capped to `caller_depth`.
    pub fn build_update(
        &self,
        checksum: u32,
        timestamp: Option<MonotonicInstant>,
        exchange_timestamp: String,
    ) -> OrderBookUpdate {
        let bids = self
            .bids
            .values()
            .take(self.caller_depth)
            .map(BookLevel::from)
            .collect();
        let asks = self
            .asks
            .values()
            .take(self.caller_depth)
            .map(BookLevel::from)
            .collect();
        OrderBookUpdate {
            symbol: self.symbol.clone(),
            bids,
            asks,
            checksum,
            timestamp,
            exchange_timestamp,
        }
    }
}

#[cfg(test)]
#[path = "builder_tests.rs"]
mod tests;
