//! Futures WebSocket subscription-lifecycle namespace.

use std::sync::Arc;

use crate::api::subscription_types::{
    FuturesSubscriptionInfo, FuturesSubscriptionRef, SubscriptionError, SubscriptionsSummary,
};
use crate::api::ws_surface::WsSurface;
use crate::conn::SubscribeParams;
use crate::types::{
    BookDepth, ChannelName, FuturesOhlcTickType, OhlcInterval, StreamSymbol, WsUrl,
    is_futures_wire_symbol,
};

/// Reject an empty list or any symbol that is not a Futures wire symbol.
pub(crate) fn validate_futures_symbols(symbols: &[String]) -> Result<(), SubscriptionError> {
    if symbols.is_empty() {
        return Err(SubscriptionError::InvalidArguments {
            detail: "Futures subscription symbols must not be empty".to_string(),
        });
    }
    for symbol in symbols {
        if !is_futures_wire_symbol(symbol) {
            return Err(SubscriptionError::UnsupportedFuturesSymbol {
                symbol: symbol.clone(),
            });
        }
    }
    Ok(())
}

/// Futures WebSocket subscription-lifecycle namespace.
pub struct FuturesSubscriptionNamespace {
    ws: Arc<WsSurface>,
}

impl std::fmt::Debug for FuturesSubscriptionNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuturesSubscriptionNamespace")
            .finish_non_exhaustive()
    }
}

impl FuturesSubscriptionNamespace {
    pub(crate) fn new(ws: Arc<WsSurface>) -> Self {
        Self { ws }
    }

    /// Subscribe to channel-wide Unified Futures instrument snapshots.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::NoHandlerRegistered`] until
    /// [`crate::FuturesMarketNamespace::on_instrument`] succeeds. It can also
    /// return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`].
    pub fn subscribe_instrument(&self) -> Result<(), SubscriptionError> {
        self.subscribe_wide(
            WsUrl::UnifiedPublic,
            ChannelName::Instrument,
            SubscribeParams::Instrument,
        )
    }

    /// Unsubscribe from channel-wide Unified Futures instrument snapshots.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_instrument(&self) -> Result<(), SubscriptionError> {
        self.unsubscribe_wide(WsUrl::UnifiedPublic, ChannelName::Instrument)
    }

    /// Subscribe to Unified Futures private execution updates.
    ///
    /// `None` is the venue default, `Some(false)` skips and `Some(true)` requests the opening snapshot.
    /// While the channel is live, a later subscriber's options are ignored. A reconnect forgets a
    /// `Some(false)` already sent, so the venue's default snapshot is sent again; `Some(true)` is kept.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::NoHandlerRegistered`] until
    /// [`crate::FuturesAccountNamespace::on_executions`] succeeds. It can also
    /// return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`].
    pub fn subscribe_executions(
        &self,
        snap_orders: Option<bool>,
        snap_trades: Option<bool>,
    ) -> Result<(), SubscriptionError> {
        self.subscribe_wide(
            WsUrl::UnifiedPrivate,
            ChannelName::Executions,
            SubscribeParams::Executions {
                snap_orders,
                snap_trades,
            },
        )
    }

    /// Unsubscribe from Unified Futures private execution updates.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_executions(&self) -> Result<(), SubscriptionError> {
        self.unsubscribe_wide(WsUrl::UnifiedPrivate, ChannelName::Executions)
    }

    /// Subscribe to Unified Futures private position snapshots and updates.
    ///
    /// `None` is the venue default, `Some(false)` skips and `Some(true)` requests the opening snapshot.
    /// While the channel is live, a later subscriber's options are ignored. A reconnect forgets a
    /// `Some(false)` already sent, so the venue's default snapshot is sent again; `Some(true)` is kept.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::NoHandlerRegistered`] until
    /// [`crate::FuturesAccountNamespace::on_positions`] succeeds. It can also
    /// return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`].
    pub fn subscribe_positions(&self, snapshot: Option<bool>) -> Result<(), SubscriptionError> {
        self.subscribe_wide(
            WsUrl::UnifiedPrivate,
            ChannelName::Positions,
            SubscribeParams::Positions { snapshot },
        )
    }

    /// Unsubscribe from Unified Futures private position updates.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_positions(&self) -> Result<(), SubscriptionError> {
        self.unsubscribe_wide(WsUrl::UnifiedPrivate, ChannelName::Positions)
    }

    /// Subscribe to Unified Futures private margin updates.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::NoHandlerRegistered`] until
    /// [`crate::FuturesAccountNamespace::on_margin`] succeeds. It can also
    /// return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`].
    pub fn subscribe_margin(&self) -> Result<(), SubscriptionError> {
        self.subscribe_wide(
            WsUrl::UnifiedPrivate,
            ChannelName::Margin,
            SubscribeParams::Margin,
        )
    }

    /// Unsubscribe from Unified Futures private margin updates.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_margin(&self) -> Result<(), SubscriptionError> {
        self.unsubscribe_wide(WsUrl::UnifiedPrivate, ChannelName::Margin)
    }

    /// Subscribe to the Unified private account-preferences channel.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::NoHandlerRegistered`] until
    /// [`crate::FuturesAccountNamespace::on_preferences`] succeeds. It can also
    /// return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`].
    pub fn subscribe_preferences(&self) -> Result<(), SubscriptionError> {
        self.subscribe_wide(
            WsUrl::UnifiedPrivate,
            ChannelName::Preferences,
            SubscribeParams::Preferences,
        )
    }

    /// Unsubscribe from the Unified private account-preferences channel.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_preferences(&self) -> Result<(), SubscriptionError> {
        self.unsubscribe_wide(WsUrl::UnifiedPrivate, ChannelName::Preferences)
    }

    /// Subscribe to the shared Unified private balances channel.
    ///
    /// Unlike the other Futures private channels, the venue rejects an
    /// `asset_class` parameter for balances and preferences. The registry composes that
    /// difference from the channel's URL and parameters.
    ///
    /// `None` is the venue default, `Some(false)` skips and `Some(true)` requests the opening snapshot.
    /// While the channel is live, a later subscriber's options are ignored. A reconnect forgets a
    /// `Some(false)` already sent, so the venue's default snapshot is sent again; `Some(true)` is kept.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::NoHandlerRegistered`] until
    /// [`crate::FuturesAccountNamespace::on_balances`] succeeds. It can also
    /// return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`].
    pub fn subscribe_balances(&self, snapshot: Option<bool>) -> Result<(), SubscriptionError> {
        self.subscribe_wide(
            WsUrl::UnifiedPrivate,
            ChannelName::Balances,
            SubscribeParams::Balances { snapshot },
        )
    }

    /// Unsubscribe from the shared Unified private balances channel.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_balances(&self) -> Result<(), SubscriptionError> {
        self.unsubscribe_wide(WsUrl::UnifiedPrivate, ChannelName::Balances)
    }

    /// Subscribe to Unified Futures ticker updates for native Futures wire symbols.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] or [`SubscriptionError::UnsupportedFuturesSymbol`]
    /// for an empty or non-Futures `symbols` list, and [`SubscriptionError::NoHandlerRegistered`] until a
    /// matching handler is registered. It can also return [`SubscriptionError::QueueFull`],
    /// [`SubscriptionError::LoopDead`], or [`SubscriptionError::ClientClosed`].
    pub fn subscribe_ticker(
        &self,
        symbols: Vec<String>,
        snapshot: Option<bool>,
    ) -> Result<(), SubscriptionError> {
        self.subscribe_symbols(
            ChannelName::Ticker,
            symbols,
            SubscribeParams::Ticker {
                snapshot,
                event_trigger: None,
            },
        )
    }

    /// Unsubscribe from Unified Futures ticker updates.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] or [`SubscriptionError::UnsupportedFuturesSymbol`]
    /// for an empty or non-Futures `symbols` list. It can also return [`SubscriptionError::QueueFull`],
    /// [`SubscriptionError::LoopDead`], or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_ticker(&self, symbols: Vec<String>) -> Result<(), SubscriptionError> {
        self.unsubscribe_symbols(ChannelName::Ticker, symbols, StreamSymbol::Futures)
    }

    /// Subscribe to Unified Futures trade updates for native Futures wire symbols.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] or [`SubscriptionError::UnsupportedFuturesSymbol`]
    /// for an empty or non-Futures `symbols` list, and [`SubscriptionError::NoHandlerRegistered`] until a
    /// matching handler is registered. It can also return [`SubscriptionError::QueueFull`],
    /// [`SubscriptionError::LoopDead`], or [`SubscriptionError::ClientClosed`].
    pub fn subscribe_trade(
        &self,
        symbols: Vec<String>,
        snapshot: Option<bool>,
    ) -> Result<(), SubscriptionError> {
        self.subscribe_symbols(
            ChannelName::Trade,
            symbols,
            SubscribeParams::Trade { snapshot },
        )
    }

    /// Unsubscribe from Unified Futures trade updates.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] or [`SubscriptionError::UnsupportedFuturesSymbol`]
    /// for an empty or non-Futures `symbols` list. It can also return [`SubscriptionError::QueueFull`],
    /// [`SubscriptionError::LoopDead`], or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_trade(&self, symbols: Vec<String>) -> Result<(), SubscriptionError> {
        self.unsubscribe_symbols(ChannelName::Trade, symbols, StreamSymbol::Futures)
    }

    /// Subscribe to Unified Futures OHLC candles at one interval for native wire symbols.
    ///
    /// Each symbol, interval and tick type is its own subscription; `tick_type`
    /// `None` subscribes the venue default, [`FuturesOhlcTickType::Trade`].
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] or [`SubscriptionError::UnsupportedFuturesSymbol`]
    /// for an empty or non-Futures `symbols` list, and [`SubscriptionError::NoHandlerRegistered`] until a
    /// matching handler is registered. It can also return [`SubscriptionError::QueueFull`],
    /// [`SubscriptionError::LoopDead`], or [`SubscriptionError::ClientClosed`].
    pub fn subscribe_ohlc(
        &self,
        symbols: Vec<String>,
        interval: OhlcInterval,
        snapshot: Option<bool>,
        tick_type: Option<FuturesOhlcTickType>,
    ) -> Result<(), SubscriptionError> {
        self.subscribe_symbols(
            ChannelName::Ohlc,
            symbols,
            SubscribeParams::Ohlc {
                interval,
                snapshot,
                tick_type,
            },
        )
    }

    /// Unsubscribe from the Unified Futures OHLC candles at `interval` and `tick_type`.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] or [`SubscriptionError::UnsupportedFuturesSymbol`]
    /// for an empty or non-Futures `symbols` list. It can also return [`SubscriptionError::QueueFull`],
    /// [`SubscriptionError::LoopDead`], or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_ohlc(
        &self,
        symbols: Vec<String>,
        interval: OhlcInterval,
        tick_type: Option<FuturesOhlcTickType>,
    ) -> Result<(), SubscriptionError> {
        self.unsubscribe_symbols(ChannelName::Ohlc, symbols, |symbol| {
            StreamSymbol::futures_ohlc(symbol, interval, tick_type)
        })
    }

    /// Subscribe to a CRC-validated maintained Futures book. An opening snapshot
    /// is always requested to seed the local book.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] or [`SubscriptionError::UnsupportedFuturesSymbol`]
    /// for an empty or non-Futures `symbols` list, and [`SubscriptionError::NoHandlerRegistered`] until a
    /// matching handler is registered. It can also return [`SubscriptionError::QueueFull`],
    /// [`SubscriptionError::LoopDead`], or [`SubscriptionError::ClientClosed`].
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] also when a Futures book for a symbol
    /// already exists with different depth or snapshot parameters.
    pub fn subscribe_book(
        &self,
        symbols: Vec<String>,
        depth: BookDepth,
    ) -> Result<(), SubscriptionError> {
        self.subscribe_symbols(ChannelName::Book, symbols, SubscribeParams::Book { depth })
    }

    /// Subscribe to raw Futures book frames. `snapshot` is passed through to the
    /// Unified wire request; raw mode creates no maintained-book state.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] or [`SubscriptionError::UnsupportedFuturesSymbol`]
    /// for an empty or non-Futures `symbols` list, and [`SubscriptionError::NoHandlerRegistered`] until a
    /// matching handler is registered. It can also return [`SubscriptionError::QueueFull`],
    /// [`SubscriptionError::LoopDead`], or [`SubscriptionError::ClientClosed`].
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] also when a Futures book for a symbol
    /// already exists with different depth or snapshot parameters.
    pub fn subscribe_book_raw(
        &self,
        symbols: Vec<String>,
        depth: BookDepth,
        snapshot: Option<bool>,
    ) -> Result<(), SubscriptionError> {
        self.subscribe_symbols(
            ChannelName::BookRaw,
            symbols,
            SubscribeParams::BookRaw { depth, snapshot },
        )
    }

    /// Unsubscribe from the shared Futures book wire stream. The registry owns
    /// the original subscription parameters for wire teardown and replay.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] or [`SubscriptionError::UnsupportedFuturesSymbol`]
    /// for an empty or non-Futures `symbols` list. It can also return [`SubscriptionError::QueueFull`],
    /// [`SubscriptionError::LoopDead`], or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_book(&self, symbols: Vec<String>) -> Result<(), SubscriptionError> {
        self.unsubscribe_symbols(ChannelName::Book, symbols, StreamSymbol::Futures)
    }

    /// Unsubscribe from the shared Futures raw-book wire stream.
    ///
    /// Releases one reference taken by a matching `subscribe_*`. A subscription held by an
    /// `on_*_for` guard is released only by dropping that guard; this call is then a no-op.
    /// [`Self::unsubscribe_channel`] and [`Self::unsubscribe_all`] tear down regardless.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] or [`SubscriptionError::UnsupportedFuturesSymbol`]
    /// for an empty or non-Futures `symbols` list. It can also return [`SubscriptionError::QueueFull`],
    /// [`SubscriptionError::LoopDead`], or [`SubscriptionError::ClientClosed`] when teardown cannot be posted.
    pub fn unsubscribe_book_raw(&self, symbols: Vec<String>) -> Result<(), SubscriptionError> {
        self.unsubscribe_symbols(ChannelName::BookRaw, symbols, StreamSymbol::Futures)
    }

    /// Force-unsubscribe every Unified Futures subscription on `channel`,
    /// ignoring subscriber refcounts. This never affects Spot Public or Auth entries.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when teardown cannot be posted. Public and
    /// private teardown are posted separately, so an error can follow a partial teardown;
    /// retrying is safe.
    pub fn unsubscribe_channel(&self, channel: ChannelName) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe_futures(Some(channel))
    }

    /// Force-unsubscribe every Unified Futures subscription, ignoring
    /// subscriber refcounts. This never affects Spot Public or Auth entries.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when teardown cannot be posted. Public and
    /// private teardown are posted separately, so an error can follow a partial teardown;
    /// retrying is safe.
    pub fn unsubscribe_all(&self) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe_futures(None)
    }

    /// Point-in-time snapshot of Unified Futures subscription entries.
    /// Native Futures wire symbols are returned unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::LoopDead`] or [`SubscriptionError::ClientClosed`] when the read model is unavailable.
    pub fn list_active(&self) -> Result<Vec<FuturesSubscriptionInfo>, SubscriptionError> {
        self.ws.list_active_futures()
    }

    /// Point-in-time Unified Futures entries for one logical channel.
    /// `Book` and `BookRaw` remain distinct Futures channels.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::LoopDead`] or [`SubscriptionError::ClientClosed`] when the read model is unavailable.
    pub fn find_by_channel(
        &self,
        channel: ChannelName,
    ) -> Result<Vec<FuturesSubscriptionRef>, SubscriptionError> {
        self.ws.find_futures_by_channel(channel)
    }

    /// Point-in-time Unified Futures lifecycle counts by logical channel.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::LoopDead`] or [`SubscriptionError::ClientClosed`] when the read model is unavailable.
    pub fn status_summary(&self) -> Result<SubscriptionsSummary, SubscriptionError> {
        self.ws.futures_status_summary()
    }

    fn subscribe_symbols(
        &self,
        channel: ChannelName,
        symbols: Vec<String>,
        params: SubscribeParams,
    ) -> Result<(), SubscriptionError> {
        validate_futures_symbols(&symbols)?;
        self.ws.require_handler(WsUrl::UnifiedPublic, channel)?;
        self.ws.subscribe_for(
            WsUrl::UnifiedPublic,
            channel,
            symbols
                .into_iter()
                .map(|symbol| params.futures_symbol(symbol))
                .collect(),
            params,
        )
    }

    fn unsubscribe_symbols(
        &self,
        channel: ChannelName,
        symbols: Vec<String>,
        key: impl FnMut(String) -> StreamSymbol,
    ) -> Result<(), SubscriptionError> {
        validate_futures_symbols(&symbols)?;
        self.ws.unsubscribe_for(
            WsUrl::UnifiedPublic,
            channel,
            symbols.into_iter().map(key).collect(),
        )
    }

    fn subscribe_wide(
        &self,
        url: WsUrl,
        channel: ChannelName,
        params: SubscribeParams,
    ) -> Result<(), SubscriptionError> {
        self.ws.require_handler(url, channel)?;
        self.ws.subscribe_for(url, channel, Vec::new(), params)
    }

    fn unsubscribe_wide(&self, url: WsUrl, channel: ChannelName) -> Result<(), SubscriptionError> {
        self.ws.unsubscribe_for(url, channel, Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::FuturesSubscriptionNamespace;
    use crate::api::subscription_types::SubscriptionError;
    use crate::api::ws_surface::WsSurface;
    use crate::conn::SubscribeParams;
    use crate::types::{ChannelName, OhlcInterval, StreamSymbol, WsUrl};

    #[test]
    fn invalid_symbols_reject_before_handler_gate_or_registry_post() {
        let (bus, ws) = WsSurface::on_default_bus();
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");
        let futures = FuturesSubscriptionNamespace::new(ws);

        assert!(matches!(
            futures.subscribe_ticker(Vec::new(), None),
            Err(SubscriptionError::InvalidArguments { .. })
        ));
        assert!(matches!(
            futures.subscribe_trade(vec!["BTC/USD".to_string()], None),
            Err(SubscriptionError::UnsupportedFuturesSymbol { .. })
        ));
        assert!(matches!(
            futures.subscribe_ohlc(Vec::new(), OhlcInterval::M1, None, None),
            Err(SubscriptionError::InvalidArguments { .. })
        ));
        assert!(matches!(
            futures.subscribe_ohlc(vec!["BTC/USD".to_string()], OhlcInterval::M1, None, None),
            Err(SubscriptionError::UnsupportedFuturesSymbol { .. })
        ));
        assert!(matches!(
            futures.unsubscribe_ohlc(vec!["OF_XBTUSD".to_string()], OhlcInterval::M1, None),
            Err(SubscriptionError::UnsupportedFuturesSymbol { .. })
        ));
        assert!(rx.try_recv().is_err(), "invalid symbols must not post work");

        assert!(matches!(
            futures.subscribe_ohlc(vec!["PF_XBTUSD".to_string()], OhlcInterval::M1, None, None),
            Err(SubscriptionError::NoHandlerRegistered {
                channel: ChannelName::Ohlc
            })
        ));
        assert!(rx.try_recv().is_err(), "handler gate runs after validation");
        assert!(matches!(
            futures.subscribe_ticker(vec!["PI_XBTUSD".to_string()], Some(true)),
            Err(SubscriptionError::NoHandlerRegistered {
                channel: ChannelName::Ticker
            })
        ));
        assert!(rx.try_recv().is_err(), "handler gate runs after validation");
    }

    #[test]
    fn ohlc_subscribe_and_unsubscribe_key_each_symbol_interval_and_tick_type() {
        use crate::api::market::FuturesMarketNamespace;
        use crate::dispatch::{CallerInbound, RegistryMutationOp};
        use crate::types::FuturesOhlcTickType;

        let (bus, ws) = WsSurface::on_default_bus();
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");
        let market = FuturesMarketNamespace::new(Arc::clone(&ws));
        let futures = FuturesSubscriptionNamespace::new(ws);
        let _handle = market.on_ohlc(|_| {}).expect("register ohlc handler");
        assert!(matches!(
            rx.try_recv().expect("handler registration"),
            CallerInbound::HandlerMutation { .. }
        ));
        let symbol = || vec!["PF_XBTUSD".to_string()];
        let key = |interval, tick_type| StreamSymbol::FuturesOhlc {
            symbol: "PF_XBTUSD".to_string(),
            interval,
            tick_type,
        };

        futures
            .subscribe_ohlc(symbol(), OhlcInterval::H4, Some(false), None)
            .expect("subscribe trade candles");
        futures
            .subscribe_ohlc(
                symbol(),
                OhlcInterval::M5,
                None,
                Some(FuturesOhlcTickType::Mark),
            )
            .expect("subscribe mark candles");
        let registered: Vec<_> = registry_mutations(&mut rx)
            .into_iter()
            .flat_map(|(url, mutation)| {
                assert_eq!(url, WsUrl::UnifiedPublic);
                let RegistryMutationOp::RegisterBatch { entries, .. } = mutation else {
                    panic!("expected RegisterBatch, got {mutation:?}");
                };
                entries
                    .into_iter()
                    .map(|entry| (entry.channel, entry.pair, entry.params))
            })
            .collect();
        assert_eq!(
            registered,
            vec![
                (
                    ChannelName::Ohlc,
                    Some(key(OhlcInterval::H4, FuturesOhlcTickType::Trade)),
                    SubscribeParams::Ohlc {
                        interval: OhlcInterval::H4,
                        snapshot: Some(false),
                        tick_type: None,
                    },
                ),
                (
                    ChannelName::Ohlc,
                    Some(key(OhlcInterval::M5, FuturesOhlcTickType::Mark)),
                    SubscribeParams::Ohlc {
                        interval: OhlcInterval::M5,
                        snapshot: None,
                        tick_type: Some(FuturesOhlcTickType::Mark),
                    },
                ),
            ]
        );

        futures
            .unsubscribe_ohlc(symbol(), OhlcInterval::M5, Some(FuturesOhlcTickType::Mark))
            .expect("unsubscribe mark candles");
        match rx.try_recv().expect("deregistration") {
            CallerInbound::RegistryMutation {
                url: WsUrl::UnifiedPublic,
                mutation: RegistryMutationOp::DeregisterBatch { channel, pairs },
            } => {
                assert_eq!(channel, ChannelName::Ohlc);
                assert_eq!(
                    pairs,
                    vec![key(OhlcInterval::M5, FuturesOhlcTickType::Mark)]
                );
            }
            other => panic!("expected DeregisterBatch, got {other:?}"),
        }
    }

    #[test]
    fn read_models_project_native_symbols_and_only_unified_public_entries() {
        use crate::conn::subscription_registry::{EntrySubState, MirrorRow};
        use crate::types::MonotonicInstant;

        let (_bus, ws) = WsSurface::on_default_bus();
        let futures = FuturesSubscriptionNamespace::new(Arc::clone(&ws));
        let now = MonotonicInstant::now();
        let mirror = ws.subscription_mirror_handle();
        let mut mirror = mirror.write().unwrap();
        mirror.insert(
            (
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some(StreamSymbol::Futures("PI_XBTUSD".to_string())),
            ),
            MirrorRow {
                state: EntrySubState::Acked,
                registered_at: now,
                subscribed_at: Some(now),
            },
        );
        mirror.insert(
            (WsUrl::UnifiedPublic, ChannelName::Instrument, None),
            MirrorRow {
                state: EntrySubState::Pending,
                registered_at: now,
                subscribed_at: None,
            },
        );
        mirror.insert(
            (
                WsUrl::UnifiedPublic,
                ChannelName::BookRaw,
                Some(StreamSymbol::Futures("PI_ETHUSD".to_string())),
            ),
            MirrorRow {
                state: EntrySubState::Terminated {
                    cause: crate::api::subscription::TerminationCause::ClientClosed,
                    last_error: None,
                },
                registered_at: now,
                subscribed_at: None,
            },
        );
        mirror.insert(
            (
                WsUrl::Public,
                ChannelName::Ticker,
                Some(StreamSymbol::Spot(
                    crate::types::Symbol::new("BTC/USD").unwrap(),
                )),
            ),
            MirrorRow {
                state: EntrySubState::Acked,
                registered_at: now,
                subscribed_at: None,
            },
        );
        drop(mirror);

        let rows = futures.list_active().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].channel, ChannelName::Instrument);
        assert_eq!(rows[0].symbol, None);
        assert_eq!(rows[1].channel, ChannelName::BookRaw);
        assert_eq!(rows[2].symbol.as_deref(), Some("PI_XBTUSD"));
        assert_eq!(rows[2].subscribed_at, Some(now));
        assert_eq!(rows[0].subscribed_at, None);
        assert_eq!(rows[1].subscribed_at, None);

        let ticker = futures.find_by_channel(ChannelName::Ticker).unwrap();
        assert_eq!(ticker.len(), 1);
        assert_eq!(ticker[0].symbol.as_deref(), Some("PI_XBTUSD"));
        assert_eq!(
            ticker[0].registered_at_monotonic,
            now.as_duration().as_millis() as u64
        );
        assert!(
            futures
                .find_by_channel(ChannelName::Book)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            futures.find_by_channel(ChannelName::BookRaw).unwrap().len(),
            1
        );

        let summary = futures.status_summary().unwrap();
        assert_eq!(
            (
                summary.total,
                summary.active,
                summary.pending,
                summary.failed
            ),
            (3, 1, 1, 1)
        );
        assert_eq!(summary.by_channel[&ChannelName::Instrument], 1);
        assert_eq!(summary.by_channel[&ChannelName::Ticker], 1);
        assert_eq!(summary.by_channel[&ChannelName::BookRaw], 1);
    }

    #[test]
    fn ohlc_read_models_expose_interval_and_tick_type_in_identity_order() {
        use crate::conn::subscription_registry::{EntrySubState, MirrorRow};
        use crate::types::{FuturesOhlcTickType, MonotonicInstant};

        let (_bus, ws) = WsSurface::on_default_bus();
        let futures = FuturesSubscriptionNamespace::new(Arc::clone(&ws));
        let now = MonotonicInstant::now();
        let identities = [
            (OhlcInterval::M5, FuturesOhlcTickType::Trade),
            (OhlcInterval::M1, FuturesOhlcTickType::Mark),
            (OhlcInterval::M1, FuturesOhlcTickType::Trade),
        ];
        let mirror = ws.subscription_mirror_handle();
        for (interval, tick_type) in identities {
            mirror.write().unwrap().insert(
                (
                    WsUrl::UnifiedPublic,
                    ChannelName::Ohlc,
                    Some(StreamSymbol::futures_ohlc(
                        "PF_XBTUSD".to_string(),
                        interval,
                        Some(tick_type),
                    )),
                ),
                MirrorRow {
                    state: EntrySubState::Pending,
                    registered_at: now,
                    subscribed_at: None,
                },
            );
        }

        let expected = vec![
            (OhlcInterval::M1, FuturesOhlcTickType::Trade),
            (OhlcInterval::M1, FuturesOhlcTickType::Mark),
            (OhlcInterval::M5, FuturesOhlcTickType::Trade),
        ];
        let listed: Vec<_> = futures
            .list_active()
            .unwrap()
            .into_iter()
            .map(|row| {
                assert_eq!(row.symbol.as_deref(), Some("PF_XBTUSD"));
                (row.interval.unwrap(), row.tick_type.unwrap())
            })
            .collect();
        assert_eq!(listed, expected);
        let found: Vec<_> = futures
            .find_by_channel(ChannelName::Ohlc)
            .unwrap()
            .into_iter()
            .map(|row| (row.interval.unwrap(), row.tick_type.unwrap()))
            .collect();
        assert_eq!(found, expected);
    }

    #[test]
    fn acknowledgement_stamps_subscribed_at_once() {
        use crate::conn::SubscribeParams;
        use crate::conn::subscription_registry::{SubscriptionEntry, SubscriptionRegistry};
        use crate::types::MonotonicInstant;

        let (_bus, ws) = WsSurface::on_default_bus();
        let futures = FuturesSubscriptionNamespace::new(Arc::clone(&ws));
        let mut registry = SubscriptionRegistry::with_mirror(ws.subscription_mirror_handle());
        let symbol = Some(StreamSymbol::Futures("PI_XBTUSD".to_string()));
        registry.register(
            SubscriptionEntry::new(
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                symbol.clone(),
                SubscribeParams::Ticker {
                    snapshot: Some(true),
                    event_trigger: None,
                },
            ),
            MonotonicInstant::now(),
            false,
        );
        assert!(
            futures.list_active().unwrap()[0].subscribed_at.is_none(),
            "a pending entry has no acknowledgement time"
        );

        let at = |secs| MonotonicInstant(std::time::Duration::from_secs(secs));
        assert!(registry.note_acked_for(WsUrl::UnifiedPublic, ChannelName::Ticker, &symbol, at(5)));
        assert_eq!(
            futures.list_active().unwrap()[0].subscribed_at,
            Some(at(5)),
            "the venue ack stamps the supplied time"
        );
        assert!(!registry.note_acked_for(
            WsUrl::UnifiedPublic,
            ChannelName::Ticker,
            &symbol,
            at(9)
        ));
        assert_eq!(futures.list_active().unwrap()[0].subscribed_at, Some(at(5)));
    }

    fn namespace_with_inbound() -> (
        crate::api::account::FuturesAccountNamespace,
        FuturesSubscriptionNamespace,
        Arc<WsSurface>,
        tokio::sync::mpsc::Receiver<crate::dispatch::CallerInbound>,
    ) {
        let (bus, ws) = WsSurface::on_default_bus();
        let rx = bus.take_caller_to_io_rx().expect("receiver");
        (
            crate::api::account::FuturesAccountNamespace::new(Arc::clone(&ws)),
            FuturesSubscriptionNamespace::new(Arc::clone(&ws)),
            ws,
            rx,
        )
    }

    fn registry_mutations(
        rx: &mut tokio::sync::mpsc::Receiver<crate::dispatch::CallerInbound>,
    ) -> Vec<(WsUrl, crate::dispatch::RegistryMutationOp)> {
        let mut out = Vec::new();
        while let Ok(inbound) = rx.try_recv() {
            if let crate::dispatch::CallerInbound::RegistryMutation { url, mutation } = inbound {
                out.push((url, mutation));
            }
        }
        out
    }

    #[test]
    fn private_subscribe_is_gated_on_a_unified_private_handler_and_targets_that_socket() {
        use crate::conn::SubscribeParams;
        use crate::dispatch::{HandlerCallback, HandlerDecodeOutcome, RegistryMutationOp};

        type Case = (
            ChannelName,
            SubscribeParams,
            fn(&crate::api::account::FuturesAccountNamespace),
            fn(&FuturesSubscriptionNamespace) -> Result<(), SubscriptionError>,
            fn(&FuturesSubscriptionNamespace) -> Result<(), SubscriptionError>,
        );
        let cases: [Case; 5] = [
            (
                ChannelName::Executions,
                SubscribeParams::Executions {
                    snap_orders: Some(true),
                    snap_trades: Some(false),
                },
                |a| drop(a.on_executions(|_| {}).expect("handler")),
                |f| f.subscribe_executions(Some(true), Some(false)),
                FuturesSubscriptionNamespace::unsubscribe_executions,
            ),
            (
                ChannelName::Positions,
                SubscribeParams::Positions { snapshot: None },
                |a| drop(a.on_positions(|_| {}).expect("handler")),
                |f| f.subscribe_positions(None),
                FuturesSubscriptionNamespace::unsubscribe_positions,
            ),
            (
                ChannelName::Margin,
                SubscribeParams::Margin,
                |a| drop(a.on_margin(|_| {}).expect("handler")),
                FuturesSubscriptionNamespace::subscribe_margin,
                FuturesSubscriptionNamespace::unsubscribe_margin,
            ),
            (
                ChannelName::Balances,
                SubscribeParams::Balances {
                    snapshot: Some(false),
                },
                |a| drop(a.on_balances(|_| {}).expect("handler")),
                |f| f.subscribe_balances(Some(false)),
                FuturesSubscriptionNamespace::unsubscribe_balances,
            ),
            (
                ChannelName::Preferences,
                SubscribeParams::Preferences,
                |a| drop(a.on_preferences(|_| {}).expect("handler")),
                FuturesSubscriptionNamespace::subscribe_preferences,
                FuturesSubscriptionNamespace::unsubscribe_preferences,
            ),
        ];

        for (channel, params, register, subscribe, unsubscribe) in cases {
            let (account, futures, ws, mut rx) = namespace_with_inbound();

            assert!(matches!(
                subscribe(&futures),
                Err(SubscriptionError::NoHandlerRegistered { channel: got }) if got == channel
            ));
            assert!(
                registry_mutations(&mut rx).is_empty(),
                "{channel:?}: nothing is posted ungated"
            );

            let spot = HandlerCallback::new(
                Arc::new(|_| HandlerDecodeOutcome::DecodeFailed),
                Arc::new(|_| {}),
            );
            let _spot_handler = ws
                .register_handler_for_checked(WsUrl::Auth, channel, spot)
                .expect("spot handler");
            assert!(
                matches!(
                    subscribe(&futures),
                    Err(SubscriptionError::NoHandlerRegistered { .. })
                ),
                "{channel:?}: a Spot Auth handler must not satisfy the Unified private gate"
            );

            register(&account);
            subscribe(&futures).expect("gated subscribe");
            let posted = registry_mutations(&mut rx);
            assert_eq!(posted.len(), 1, "{channel:?}");
            let (url, mutation) = &posted[0];
            assert_eq!(*url, WsUrl::UnifiedPrivate, "{channel:?}");
            let RegistryMutationOp::RegisterBatch { entries, .. } = mutation else {
                panic!("{channel:?}: expected RegisterBatch, got {mutation:?}");
            };
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].channel, channel);
            assert_eq!(entries[0].url, WsUrl::UnifiedPrivate);
            assert!(entries[0].pair.is_none(), "{channel:?}: channel-wide entry");
            assert_eq!(entries[0].params, params);

            unsubscribe(&futures).expect("unsubscribe");
            let posted = registry_mutations(&mut rx);
            assert!(
                matches!(
                    posted.as_slice(),
                    [(WsUrl::UnifiedPrivate, RegistryMutationOp::Deregister { channel: got, pair: None })]
                        if *got == channel
                ),
                "{channel:?}: unsubscribe targets Unified private: {posted:?}"
            );
        }
    }

    #[test]
    fn aggregate_teardown_posts_to_both_unified_sockets_only() {
        use crate::dispatch::{CallerInbound, RegistryMutationOp};

        let (bus, ws) = WsSurface::on_default_bus();
        let futures = FuturesSubscriptionNamespace::new(ws);
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");

        futures.unsubscribe_all().unwrap();
        for url in [WsUrl::UnifiedPublic, WsUrl::UnifiedPrivate] {
            assert!(matches!(
                rx.try_recv().expect("all teardown"),
                CallerInbound::RegistryMutation {
                    url: posted,
                    mutation: RegistryMutationOp::DeregisterAll { channel: None },
                } if posted == url
            ));
        }
        futures.unsubscribe_channel(ChannelName::BookRaw).unwrap();
        for url in [WsUrl::UnifiedPublic, WsUrl::UnifiedPrivate] {
            assert!(matches!(
                rx.try_recv().expect("channel teardown"),
                CallerInbound::RegistryMutation {
                    url: posted,
                    mutation: RegistryMutationOp::DeregisterAll {
                        channel: Some(ChannelName::BookRaw),
                    },
                } if posted == url
            ));
        }
        assert!(rx.try_recv().is_err(), "no Spot socket is ever posted to");
    }
}
