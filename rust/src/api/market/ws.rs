//! WS streaming handler methods for [`MarketNamespace`] plus the per-channel decode glue.

use serde_json::Value;

use crate::api::subscription_guard::SubscriptionGuard;
use crate::api::subscription_types::SubscriptionError;
use crate::book::{BookDelta, BookLevel, OrderBookUpdate};
use crate::conn::SubscribeParams;
use crate::dispatch::HandlerHandle;

use crate::api::ws_decode::{extract_data, wrap_typed};
use crate::types::{BookDepth, ChannelName, Symbol, TickerTrigger};

use super::MarketNamespace;
use super::types::OhlcInterval;
use super::ws_types::{
    OhlcUpdate, RawOhlcData, RawSystemStatusUpdate, RawTickerUpdate, RawTradeData,
    SystemStatusUpdate, TickerUpdate, TradeUpdate, parse_wire_decimal,
};

impl MarketNamespace {
    /// Register a streaming `ticker` handler for all subscribed pairs; the
    /// returned handle's `Drop` deregisters it. Does not subscribe any pair itself.
    pub fn on_ticker<F>(&self, cb: F) -> HandlerHandle
    where
        F: Fn(&TickerUpdate) + Send + Sync + 'static,
    {
        let wrapped = wrap_typed(cb, decode_ticker);
        self.ws.register_handler(ChannelName::Ticker, wrapped)
    }

    /// Register a `ticker` handler and subscribe `pairs` in one call, returning a
    /// [`SubscriptionGuard`] whose `Drop` tears both down. Fires only for `pairs`.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — nothing was subscribed; a retry may duplicate delivery if the unwind post was also rejected.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn on_ticker_for<F>(
        &self,
        pairs: &[Symbol],
        snapshot: Option<bool>,
        event_trigger: Option<TickerTrigger>,
        cb: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&TickerUpdate) + Send + Sync + 'static,
    {
        self.register_and_subscribe(
            decode_ticker,
            cb,
            ChannelName::Ticker,
            pairs,
            SubscribeParams::Ticker {
                snapshot,
                event_trigger,
            },
        )
    }

    /// Register a maintained-book handler for all subscribed pairs; the returned
    /// handle's `Drop` deregisters it. Delivers the reactor-maintained, CRC-validated
    /// [`OrderBookUpdate`].
    pub fn on_book<F>(&self, cb: F) -> HandlerHandle
    where
        F: Fn(&OrderBookUpdate) + Send + Sync + 'static,
    {
        let wrapped = wrap_typed(cb, decode_book);
        self.ws.register_handler(ChannelName::Book, wrapped)
    }

    /// Register a raw-delta book handler ([`BookDelta`], no CRC maintenance); the
    /// returned handle's `Drop` deregisters it.
    pub fn on_book_raw<F>(&self, cb: F) -> HandlerHandle
    where
        F: Fn(&BookDelta) + Send + Sync + 'static,
    {
        let wrapped = wrap_typed(cb, decode_book_raw);
        self.ws.register_handler(ChannelName::BookRaw, wrapped)
    }

    /// Register a maintained-book handler and subscribe `pairs` at `depth`, returning
    /// a [`SubscriptionGuard`] whose `Drop` tears both down. Fires only for `pairs`;
    /// first depth per pair wins.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — nothing was subscribed; a retry may duplicate delivery if the unwind post was also rejected.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn on_book_for<F>(
        &self,
        pairs: &[Symbol],
        depth: BookDepth,
        cb: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&OrderBookUpdate) + Send + Sync + 'static,
    {
        // Maintained book always seeds — no `snapshot` knob; use `on_book_raw_for` for delta-only.
        self.register_and_subscribe(
            decode_book,
            cb,
            ChannelName::Book,
            pairs,
            SubscribeParams::Book { depth },
        )
    }

    /// Register a raw-delta handler and subscribe `pairs` at `depth`, returning a
    /// [`SubscriptionGuard`] whose `Drop` tears both down. Fires only for `pairs`.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — nothing was subscribed; a retry may duplicate delivery if the unwind post was also rejected.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn on_book_raw_for<F>(
        &self,
        pairs: &[Symbol],
        depth: BookDepth,
        snapshot: Option<bool>,
        cb: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&BookDelta) + Send + Sync + 'static,
    {
        self.register_and_subscribe(
            decode_book_raw,
            cb,
            ChannelName::BookRaw,
            pairs,
            SubscribeParams::BookRaw { depth, snapshot },
        )
    }

    /// Register a streaming `trade` handler for all subscribed pairs; the returned
    /// handle's `Drop` deregisters it. A `trade` frame carries an array of prints;
    /// the callback fires once per print.
    pub fn on_trade<F>(&self, cb: F) -> HandlerHandle
    where
        F: Fn(&TradeUpdate) + Send + Sync + 'static,
    {
        let wrapped = wrap_typed(cb, decode_trade);
        self.ws.register_handler(ChannelName::Trade, wrapped)
    }

    /// Register a streaming `ohlc` (candle) handler for all subscribed pairs; the
    /// returned handle's `Drop` deregisters it. `snapshot`/`update` frames carry an
    /// array of candles; the callback fires once per candle.
    pub fn on_ohlc<F>(&self, cb: F) -> HandlerHandle
    where
        F: Fn(&OhlcUpdate) + Send + Sync + 'static,
    {
        let wrapped = wrap_typed(cb, decode_ohlc);
        self.ws.register_handler(ChannelName::Ohlc, wrapped)
    }

    /// Register a `trade` handler and subscribe `pairs` in one call, returning a
    /// [`SubscriptionGuard`] whose `Drop` tears both down. Fires only for `pairs`.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — nothing was subscribed; a retry may duplicate delivery if the unwind post was also rejected.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn on_trade_for<F>(
        &self,
        pairs: &[Symbol],
        snapshot: Option<bool>,
        cb: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&TradeUpdate) + Send + Sync + 'static,
    {
        self.register_and_subscribe(
            decode_trade,
            cb,
            ChannelName::Trade,
            pairs,
            SubscribeParams::Trade { snapshot },
        )
    }

    /// Register a `status` handler for the exchange system-status channel; the
    /// returned handle's `Drop` deregisters it. Channel-wide (no `symbol`, no `_for`).
    pub fn on_system_status<F>(&self, cb: F) -> HandlerHandle
    where
        F: Fn(&SystemStatusUpdate) + Send + Sync + 'static,
    {
        let wrapped = wrap_typed(cb, decode_system_status);
        self.ws.register_handler(ChannelName::Status, wrapped)
    }

    /// Register an `ohlc` handler and subscribe `pairs` at `interval`, returning a
    /// [`SubscriptionGuard`] whose `Drop` tears both down. Fires only for `pairs`.
    ///
    /// # Errors
    /// - [`SubscriptionError::LoopDead`] — a reactor loop has died; streaming is unavailable, rebuild the client.
    /// - [`SubscriptionError::QueueFull`] — nothing was subscribed; a retry may duplicate delivery if the unwind post was also rejected.
    /// - [`SubscriptionError::ClientClosed`] — called after a clean `close()`.
    pub fn on_ohlc_for<F>(
        &self,
        pairs: &[Symbol],
        interval: OhlcInterval,
        snapshot: Option<bool>,
        cb: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&OhlcUpdate) + Send + Sync + 'static,
    {
        self.register_and_subscribe(
            decode_ohlc,
            cb,
            ChannelName::Ohlc,
            pairs,
            SubscribeParams::Ohlc { interval, snapshot },
        )
    }

    /// Pair-filtering wrapper over the shared guarded funnel on `WsSurface`.
    fn register_and_subscribe<T: HasSymbol + Send + Sync + 'static>(
        &self,
        decode_fn: impl Fn(Value) -> Option<T> + Send + Sync + 'static,
        cb: impl Fn(&T) + Send + Sync + 'static,
        channel: ChannelName,
        pairs: &[Symbol],
        params: SubscribeParams,
    ) -> Result<SubscriptionGuard, SubscriptionError> {
        // Dedupe: a duplicate pair would register twice under one guard record and over-release on drop.
        let mut pairs = pairs.to_vec();
        let mut seen = std::collections::HashSet::new();
        pairs.retain(|p| seen.insert(p.clone()));
        let wrapped = wrap_typed(filter_by_pairs(pairs.clone(), cb), decode_fn);
        self.ws
            .register_and_subscribe_guarded(channel, pairs, params, wrapped)
    }
}

pub(crate) trait HasSymbol {
    fn symbol(&self) -> &Symbol;
}

impl HasSymbol for TickerUpdate {
    fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}
impl HasSymbol for OrderBookUpdate {
    fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}
impl HasSymbol for BookDelta {
    fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}
impl HasSymbol for TradeUpdate {
    fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}
impl HasSymbol for OhlcUpdate {
    fn symbol(&self) -> &Symbol {
        &self.symbol
    }
}

/// Wrap `cb` so it fires only for updates whose symbol is in `want`.
pub(crate) fn filter_by_pairs<T: HasSymbol + 'static>(
    want: Vec<Symbol>,
    cb: impl Fn(&T) + Send + Sync + 'static,
) -> impl Fn(&T) + Send + Sync + 'static {
    move |update: &T| {
        if want.contains(update.symbol()) {
            cb(update);
        }
    }
}

pub(crate) fn decode_ticker(v: Value) -> Option<TickerUpdate> {
    let data = extract_data(v)?;
    let raw: RawTickerUpdate = serde_json::from_value(data).ok()?;
    TickerUpdate::from_wire(raw).ok()
}

/// Lenient — every field `Option`, so the auto-seed frame is never dropped.
pub(crate) fn decode_system_status(v: Value) -> Option<SystemStatusUpdate> {
    let data = extract_data(v)?;
    let raw: RawSystemStatusUpdate = serde_json::from_value(data).ok()?;
    Some(SystemStatusUpdate {
        system: raw.system,
        version: raw.version,
        api_version: raw.api_version,
        // A surprise `connection_id` shape maps to None; it must not sink the status frame.
        connection_id: raw
            .connection_id
            .as_ref()
            .and_then(serde_json::Value::as_u64),
    })
}

/// Per-frame only: no builder state, no CRC validation, `checksum` verbatim.
pub(crate) fn decode_book(v: Value) -> Option<OrderBookUpdate> {
    let data = extract_data(v)?;
    let parsed = crate::book::parse_book_frame_owned(data)?;
    Some(OrderBookUpdate {
        symbol: Symbol::new(&parsed.symbol).ok()?,
        bids: parsed.bids.iter().map(BookLevel::from).collect(),
        asks: parsed.asks.iter().map(BookLevel::from).collect(),
        checksum: parsed.checksum,
        timestamp: None,
        exchange_timestamp: parsed.timestamp,
    })
}

/// The envelope `type` must be read before [`extract_data`] consumes the value.
pub(crate) fn decode_book_raw(v: Value) -> Option<BookDelta> {
    let is_snapshot = v
        .get("type")
        .and_then(Value::as_str)
        .map(|t| t == "snapshot")
        .unwrap_or(false);
    let data = extract_data(v)?;
    let parsed = crate::book::parse_book_frame_owned(data)?;
    Some(BookDelta {
        symbol: Symbol::new(&parsed.symbol).ok()?,
        bids: parsed.bids,
        asks: parsed.asks,
        checksum: parsed.checksum,
        is_snapshot,
        timestamp: None,
        exchange_timestamp: parsed.timestamp,
    })
}

/// One print per invocation — the reactor unpacks the frame's print array.
pub(super) fn decode_trade(v: Value) -> Option<TradeUpdate> {
    let data = extract_data(v)?;
    let raw: RawTradeData = serde_json::from_value(data).ok()?;
    let price = parse_wire_decimal(&raw.price)?;
    let qty = parse_wire_decimal(&raw.qty)?;
    Some(TradeUpdate {
        symbol: Symbol::new(&raw.symbol).ok()?,
        side: raw.side,
        price,
        qty,
        ord_type: raw.ord_type,
        trade_id: raw.trade_id,
        timestamp: raw.timestamp,
    })
}

/// The deprecated per-candle end-time is dropped; `interval_begin` is canonical.
pub(super) fn decode_ohlc(v: Value) -> Option<OhlcUpdate> {
    let data = extract_data(v)?;
    let raw: RawOhlcData = serde_json::from_value(data).ok()?;
    let open = parse_wire_decimal(&raw.open)?;
    let high = parse_wire_decimal(&raw.high)?;
    let low = parse_wire_decimal(&raw.low)?;
    let close = parse_wire_decimal(&raw.close)?;
    let volume = parse_wire_decimal(&raw.volume)?;
    let vwap = parse_wire_decimal(&raw.vwap)?;
    Some(OhlcUpdate {
        symbol: Symbol::new(&raw.symbol).ok()?,
        open,
        high,
        low,
        close,
        trades: raw.trades,
        volume,
        vwap,
        interval_begin: raw.interval_begin,
        interval: raw.interval,
    })
}
