//! Futures public market-data namespace. Payload types and decoders live in
//! [`super::futures_ws_types`].

use std::sync::Arc;

use serde_json::Value;

use super::futures_ws_types::{
    FuturesInstrumentSnapshot, FuturesOhlcUpdate, FuturesSystemStatusUpdate, FuturesTickerUpdate,
    FuturesTradeUpdate, decode_book, decode_book_raw, decode_instrument_snapshot, decode_ohlc,
    decode_system_status, decode_ticker, decode_trade,
};
use crate::api::futures_subscription::validate_futures_symbols;
use crate::api::subscription_guard::SubscriptionGuard;
use crate::api::subscription_types::SubscriptionError;
use crate::api::ws_decode::{dedupe_first_seen, wrap_typed};
use crate::api::ws_surface::WsSurface;
use crate::book::{FuturesBookDelta, FuturesOrderBookUpdate};
use crate::conn::SubscribeParams;
use crate::dispatch::HandlerHandle;
use crate::types::{ChannelName, FuturesOhlcTickType, OhlcInterval, StreamSymbol, WsUrl};

/// Futures public market-data namespace.
pub struct FuturesMarketNamespace {
    ws: Arc<WsSurface>,
}

impl std::fmt::Debug for FuturesMarketNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuturesMarketNamespace")
            .finish_non_exhaustive()
    }
}

impl FuturesMarketNamespace {
    pub(crate) fn new(ws: Arc<WsSurface>) -> Self {
        Self { ws }
    }

    /// Register an unsolicited Unified Futures system-status handler and lazily
    /// connect the Unified Public socket. Status has no subscribe operation.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_system_status<F>(&self, cb: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesSystemStatusUpdate) + Send + Sync + 'static,
    {
        self.ws
            .register_unified_status_handler(wrap_typed(cb, decode_system_status))
    }

    /// Register a Unified Futures instrument snapshot handler. Call
    /// `subscription().futures().subscribe_instrument()` to start delivery.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_instrument<F>(&self, cb: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesInstrumentSnapshot) + Send + Sync + 'static,
    {
        self.ws.register_handler_for_checked(
            WsUrl::UnifiedPublic,
            ChannelName::Instrument,
            wrap_typed(cb, decode_instrument_snapshot),
        )
    }

    /// Register a Unified Futures ticker handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_ticker<F>(&self, cb: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesTickerUpdate) + Send + Sync + 'static,
    {
        self.ws.register_handler_for_checked(
            WsUrl::UnifiedPublic,
            ChannelName::Ticker,
            wrap_typed(cb, decode_ticker),
        )
    }

    /// Register a Futures ticker handler and subscribe the supplied native wire symbols.
    ///
    /// Dropping the returned guard unsubscribes and deregisters the handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] when `symbols` is empty and
    /// [`SubscriptionError::UnsupportedFuturesSymbol`] for a non-Futures wire symbol. It can
    /// also return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`], or
    /// [`SubscriptionError::ClientClosed`] when the registration cannot be posted.
    pub fn on_ticker_for<F>(
        &self,
        symbols: Vec<String>,
        snapshot: Option<bool>,
        cb: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&FuturesTickerUpdate) + Send + Sync + 'static,
    {
        self.register_and_subscribe(
            symbols,
            ChannelName::Ticker,
            SubscribeParams::Ticker {
                snapshot,
                event_trigger: None,
            },
            decode_ticker,
            cb,
        )
    }

    /// Register a Unified Futures trade handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_trade<F>(&self, cb: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesTradeUpdate) + Send + Sync + 'static,
    {
        self.ws.register_handler_for_checked(
            WsUrl::UnifiedPublic,
            ChannelName::Trade,
            wrap_typed(cb, decode_trade),
        )
    }

    /// Register a Futures trade handler and subscribe the supplied native wire symbols.
    ///
    /// Dropping the returned guard unsubscribes and deregisters the handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] when `symbols` is empty and
    /// [`SubscriptionError::UnsupportedFuturesSymbol`] for a non-Futures wire symbol. It can
    /// also return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`], or
    /// [`SubscriptionError::ClientClosed`] when the registration cannot be posted.
    pub fn on_trade_for<F>(
        &self,
        symbols: Vec<String>,
        snapshot: Option<bool>,
        cb: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&FuturesTradeUpdate) + Send + Sync + 'static,
    {
        self.register_and_subscribe(
            symbols,
            ChannelName::Trade,
            SubscribeParams::Trade { snapshot },
            decode_trade,
            cb,
        )
    }

    /// Register a Unified Futures OHLC handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_ohlc<F>(&self, cb: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesOhlcUpdate) + Send + Sync + 'static,
    {
        self.ws.register_handler_for_checked(
            WsUrl::UnifiedPublic,
            ChannelName::Ohlc,
            wrap_typed(cb, decode_ohlc),
        )
    }

    /// Register a Futures OHLC handler and subscribe the supplied wire symbols at one interval.
    ///
    /// Dropping the returned guard unsubscribes and deregisters the handler.
    ///
    /// `cb` receives only candles of this interval and tick type; `tick_type`
    /// `None` subscribes the venue default, [`FuturesOhlcTickType::Trade`].
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] when `symbols` is empty and
    /// [`SubscriptionError::UnsupportedFuturesSymbol`] for a non-Futures wire symbol. It can
    /// also return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`], or
    /// [`SubscriptionError::ClientClosed`] when the registration cannot be posted.
    pub fn on_ohlc_for<F>(
        &self,
        symbols: Vec<String>,
        interval: OhlcInterval,
        snapshot: Option<bool>,
        tick_type: Option<FuturesOhlcTickType>,
        cb: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&FuturesOhlcUpdate) + Send + Sync + 'static,
    {
        self.register_and_subscribe(
            symbols,
            ChannelName::Ohlc,
            SubscribeParams::Ohlc {
                interval,
                snapshot,
                tick_type,
            },
            decode_ohlc,
            cb,
        )
    }

    /// Register a CRC-validated, maintained Unified Futures book handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_book<F>(&self, cb: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesOrderBookUpdate) + Send + Sync + 'static,
    {
        self.ws.register_handler_for_checked(
            WsUrl::UnifiedPublic,
            ChannelName::Book,
            wrap_typed(cb, decode_book),
        )
    }

    /// Register a raw Unified Futures book-frame handler. Raw mode maintains no
    /// local state and performs no checksum recovery.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`],
    /// or [`SubscriptionError::ClientClosed`] when the handler cannot be posted.
    pub fn on_book_raw<F>(&self, cb: F) -> Result<HandlerHandle, SubscriptionError>
    where
        F: Fn(&FuturesBookDelta) + Send + Sync + 'static,
    {
        self.ws.register_handler_for_checked(
            WsUrl::UnifiedPublic,
            ChannelName::BookRaw,
            wrap_typed(cb, decode_book_raw),
        )
    }

    /// Register a maintained Futures book handler and subscribe native symbols.
    ///
    /// Dropping the returned guard unsubscribes and deregisters the handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] when `symbols` is empty or a maintained
    /// book would conflict with an existing subscription, and
    /// [`SubscriptionError::UnsupportedFuturesSymbol`] for a non-Futures wire symbol. It can
    /// also return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`], or
    /// [`SubscriptionError::ClientClosed`] when the registration cannot be posted.
    pub fn on_book_for<F>(
        &self,
        symbols: Vec<String>,
        depth: crate::types::BookDepth,
        cb: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&FuturesOrderBookUpdate) + Send + Sync + 'static,
    {
        self.register_and_subscribe(
            symbols,
            ChannelName::Book,
            SubscribeParams::Book { depth },
            decode_book,
            cb,
        )
    }

    /// Register a raw Futures book handler and subscribe native symbols.
    ///
    /// Dropping the returned guard unsubscribes and deregisters the handler.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::InvalidArguments`] when `symbols` is empty or a Futures
    /// book for a symbol already exists with different depth or snapshot parameters, and
    /// [`SubscriptionError::UnsupportedFuturesSymbol`] for a non-Futures wire symbol. It can
    /// also return [`SubscriptionError::QueueFull`], [`SubscriptionError::LoopDead`], or
    /// [`SubscriptionError::ClientClosed`] when the registration cannot be posted.
    pub fn on_book_raw_for<F>(
        &self,
        symbols: Vec<String>,
        depth: crate::types::BookDepth,
        snapshot: Option<bool>,
        cb: F,
    ) -> Result<SubscriptionGuard, SubscriptionError>
    where
        F: Fn(&FuturesBookDelta) + Send + Sync + 'static,
    {
        self.register_and_subscribe(
            symbols,
            ChannelName::BookRaw,
            SubscribeParams::BookRaw { depth, snapshot },
            decode_book_raw,
            cb,
        )
    }

    fn register_and_subscribe<T: FuturesHasSymbol + Send + Sync + 'static>(
        &self,
        symbols: Vec<String>,
        channel: ChannelName,
        params: SubscribeParams,
        decode: impl Fn(Value) -> Option<T> + Send + Sync + 'static,
        cb: impl Fn(&T) + Send + Sync + 'static,
    ) -> Result<SubscriptionGuard, SubscriptionError> {
        validate_futures_symbols(&symbols)?;
        let wanted: Vec<StreamSymbol> = dedupe_first_seen(symbols)
            .into_iter()
            .map(|symbol| params.futures_symbol(symbol))
            .collect();
        self.ws.register_and_subscribe_guarded_for(
            WsUrl::UnifiedPublic,
            channel,
            wanted.clone(),
            params,
            wrap_typed(
                move |update: &T| {
                    if wanted.iter().any(|key| update.is_for(key)) {
                        cb(update);
                    }
                },
                decode,
            ),
        )
    }
}

trait FuturesHasSymbol {
    fn symbol(&self) -> &str;

    fn is_for(&self, key: &StreamSymbol) -> bool {
        key.as_str() == self.symbol()
    }
}

impl FuturesHasSymbol for FuturesTickerUpdate {
    fn symbol(&self) -> &str {
        FuturesTickerUpdate::symbol(self)
    }
}

impl FuturesHasSymbol for FuturesTradeUpdate {
    fn symbol(&self) -> &str {
        &self.symbol
    }
}

impl FuturesHasSymbol for FuturesOhlcUpdate {
    fn symbol(&self) -> &str {
        &self.symbol
    }

    fn is_for(&self, key: &StreamSymbol) -> bool {
        key.as_str() == self.symbol
            && key
                .ohlc_interval()
                .is_some_and(|interval| u64::from(interval) == u64::from(self.interval))
            && key.ohlc_tick_type() == Some(self.tick_type)
    }
}

impl FuturesHasSymbol for FuturesOrderBookUpdate {
    fn symbol(&self) -> &str {
        &self.symbol
    }
}

impl FuturesHasSymbol for FuturesBookDelta {
    fn symbol(&self) -> &str {
        &self.symbol
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::{CallerInbound, HandlerMutationOp};

    #[test]
    fn on_ticker_for_deduplicates_symbols_in_first_seen_order() {
        let (bus, ws) = WsSurface::on_default_bus();
        let market = FuturesMarketNamespace::new(ws);
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");

        let _guard = market
            .on_ticker_for(
                vec![
                    "PI_XBTUSD".to_string(),
                    "PF_ETHUSD".to_string(),
                    "PI_XBTUSD".to_string(),
                ],
                None,
                |_| {},
            )
            .expect("deduplicated symbols register");

        assert!(matches!(
            rx.try_recv().expect("handler registration"),
            CallerInbound::HandlerMutation { .. }
        ));
        match rx.try_recv().expect("subscription registration") {
            CallerInbound::RegistryMutation {
                mutation: crate::dispatch::RegistryMutationOp::RegisterBatch { entries, .. },
                ..
            } => {
                assert_eq!(
                    entries
                        .into_iter()
                        .map(|entry| entry.pair.expect("symbol").as_str().to_string())
                        .collect::<Vec<_>>(),
                    vec!["PI_XBTUSD", "PF_ETHUSD"]
                );
            }
            other => panic!("expected RegisterBatch, got {other:?}"),
        }
    }

    #[test]
    fn raw_snapshot_false_then_maintained_book_rejects_before_posting_maintained_wire_work() {
        use crate::dispatch::RegistryMutationOp;
        use crate::types::BookDepth;

        let (bus, ws) = WsSurface::on_default_bus();
        let market = FuturesMarketNamespace::new(ws);
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");

        let _raw = market
            .on_book_raw_for(
                vec!["PI_XBTUSD".to_string()],
                BookDepth::D10,
                Some(false),
                |_update: &FuturesBookDelta| {},
            )
            .expect("raw registration");
        let err = market
            .on_book_for(
                vec!["PI_XBTUSD".to_string()],
                BookDepth::D10,
                |_update: &FuturesOrderBookUpdate| {},
            )
            .expect_err("incompatible maintained request must reject locally");
        assert!(matches!(err, SubscriptionError::InvalidArguments { .. }));

        let mut maintained_registers = 0;
        let mut handler_mutations = Vec::new();
        while let Ok(message) = rx.try_recv() {
            match message {
                CallerInbound::RegistryMutation {
                    mutation: RegistryMutationOp::RegisterBatch { entries, .. },
                    ..
                } => {
                    maintained_registers += entries
                        .iter()
                        .filter(|entry| entry.channel == ChannelName::Book)
                        .count();
                }
                CallerInbound::HandlerMutation { channel, op, .. } => {
                    handler_mutations
                        .push((channel, matches!(op, HandlerMutationOp::Register { .. })));
                }
                _ => {}
            }
        }
        assert_eq!(
            maintained_registers, 0,
            "the incompatible maintained request must not schedule registry or wire work"
        );
        assert_eq!(
            handler_mutations,
            vec![(ChannelName::BookRaw, true)],
            "the rejected maintained request must not post a handler Register or Deregister"
        );
    }

    #[test]
    fn maintained_book_at_another_caller_depth_rejects_while_the_first_guard_lives() {
        use crate::dispatch::RegistryMutationOp;
        use crate::types::BookDepth;

        let (bus, ws) = WsSurface::on_default_bus();
        let market = FuturesMarketNamespace::new(ws);
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");
        let on_book = |depth| {
            market.on_book_for(
                vec!["PI_XBTUSD".to_string()],
                depth,
                |_update: &FuturesOrderBookUpdate| {},
            )
        };

        let d10 = on_book(BookDepth::D10).expect("D10 owner");
        let err = on_book(BookDepth::D25).expect_err("D25 must not receive the D10 book");
        assert!(matches!(err, SubscriptionError::InvalidArguments { .. }));
        let mut maintained = Vec::new();
        let mut handler_mutations = Vec::new();
        while let Ok(message) = rx.try_recv() {
            match message {
                CallerInbound::RegistryMutation {
                    mutation: RegistryMutationOp::RegisterBatch { entries, .. },
                    ..
                } => {
                    maintained.extend(
                        entries
                            .iter()
                            .filter(|entry| entry.channel == ChannelName::Book)
                            .map(|entry| entry.params),
                    );
                }
                CallerInbound::HandlerMutation { channel, op, .. } => {
                    handler_mutations
                        .push((channel, matches!(op, HandlerMutationOp::Register { .. })));
                }
                _ => {}
            }
        }
        assert_eq!(
            maintained,
            vec![SubscribeParams::Book {
                depth: BookDepth::D10
            }]
        );
        assert_eq!(
            handler_mutations,
            vec![(ChannelName::Book, true)],
            "the rejected D25 request must not post a handler Register or Deregister"
        );
        drop(d10);
        let _d25 = on_book(BookDepth::D25).expect("D25 accepted once the D10 guard drops");
    }

    #[test]
    fn on_ohlc_for_posts_the_interval_and_deduplicates_symbols() {
        use crate::conn::SubscribeParams;
        use crate::dispatch::RegistryMutationOp;
        use crate::types::OhlcInterval;

        let (bus, ws) = WsSurface::on_default_bus();
        let market = FuturesMarketNamespace::new(ws);
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");

        let _guard = market
            .on_ohlc_for(
                vec![
                    "PF_XBTUSD".to_string(),
                    "PF_ETHUSD".to_string(),
                    "PF_XBTUSD".to_string(),
                ],
                OhlcInterval::M5,
                Some(false),
                None,
                |_| {},
            )
            .expect("deduplicated symbols register");

        assert!(matches!(
            rx.try_recv().expect("handler registration"),
            CallerInbound::HandlerMutation { .. }
        ));
        match rx.try_recv().expect("subscription registration") {
            CallerInbound::RegistryMutation {
                mutation: RegistryMutationOp::RegisterBatch { entries, .. },
                ..
            } => {
                assert_eq!(
                    entries
                        .iter()
                        .map(|entry| entry.pair.as_ref().expect("symbol").as_str().to_string())
                        .collect::<Vec<_>>(),
                    vec!["PF_XBTUSD", "PF_ETHUSD"]
                );
                assert!(
                    entries
                        .iter()
                        .all(|entry| entry.channel == ChannelName::Ohlc
                            && matches!(
                                entry.params,
                                SubscribeParams::Ohlc {
                                    interval: OhlcInterval::M5,
                                    snapshot: Some(false),
                                    tick_type: None,
                                }
                            ))
                );
            }
            other => panic!("expected RegisterBatch, got {other:?}"),
        }
    }

    #[test]
    fn on_ohlc_for_delivers_only_its_own_symbol_interval_and_tick_type() {
        use crate::dispatch::{HandlerDecodeOutcome, HandlerMutationOp};
        use crate::types::{FuturesOhlcTickType, OhlcInterval};

        let (bus, ws) = WsSurface::on_default_bus();
        let market = FuturesMarketNamespace::new(ws);
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");
        let delivered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut guards = Vec::new();
        let mut callbacks = Vec::new();
        for (owner, (interval, tick_type)) in [
            (OhlcInterval::M1, None),
            (OhlcInterval::M5, None),
            (OhlcInterval::M1, Some(FuturesOhlcTickType::Mark)),
        ]
        .into_iter()
        .enumerate()
        {
            let delivered = Arc::clone(&delivered);
            guards.push(
                market
                    .on_ohlc_for(
                        vec!["PF_XBTUSD".to_string()],
                        interval,
                        None,
                        tick_type,
                        move |candle: &FuturesOhlcUpdate| {
                            delivered.lock().unwrap().push((
                                owner,
                                candle.interval,
                                candle.tick_type,
                            ));
                        },
                    )
                    .expect("register OHLC combiner"),
            );
            while let Ok(message) = rx.try_recv() {
                if let CallerInbound::HandlerMutation {
                    op: HandlerMutationOp::Register { callback, .. },
                    ..
                } = message
                {
                    callbacks.push(callback);
                }
            }
        }

        for (symbol, interval, tick_type) in [
            ("PF_XBTUSD", 1, "trade"),
            ("PF_XBTUSD", 5, "trade"),
            ("PF_XBTUSD", 1, "mark"),
            ("PF_ETHUSD", 1, "trade"),
        ] {
            let candle = serde_json::json!({ "type": "update", "data": {
                "symbol": symbol, "tick_type": tick_type, "open": 1, "high": 1, "low": 1,
                "close": 1, "trades": 1, "volume": 0.0, "vwap": 0.0,
                "interval_begin": "2026-10-29T10:59:00.000000000Z", "interval": interval
            }});
            let HandlerDecodeOutcome::Deliver(payload) = (callbacks[0].decode)(candle) else {
                panic!("candle decodes");
            };
            for callback in &callbacks {
                (callback.invoke)(&payload);
            }
        }

        assert_eq!(
            *delivered.lock().unwrap(),
            vec![
                (0, 1, FuturesOhlcTickType::Trade),
                (1, 5, FuturesOhlcTickType::Trade),
                (2, 1, FuturesOhlcTickType::Mark),
            ]
        );
    }

    #[test]
    fn on_trade_for_delivers_only_its_own_symbol() {
        use crate::dispatch::{HandlerDecodeOutcome, HandlerMutationOp};

        let (bus, ws) = WsSurface::on_default_bus();
        let market = FuturesMarketNamespace::new(ws);
        let mut rx = bus.take_caller_to_io_rx().expect("receiver");
        let delivered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&delivered);
        let _guard = market
            .on_trade_for(vec!["PI_XBTUSD".to_string()], None, move |trade| {
                sink.lock().unwrap().push(trade.symbol.clone());
            })
            .expect("register trade combiner");
        let callback = std::iter::from_fn(|| rx.try_recv().ok())
            .find_map(|message| match message {
                CallerInbound::HandlerMutation {
                    op: HandlerMutationOp::Register { callback, .. },
                    ..
                } => Some(callback),
                _ => None,
            })
            .expect("trade handler registered");

        for symbol in ["PF_ETHUSD", "PI_XBTUSD"] {
            let trade = serde_json::json!({ "type": "update", "data": {
                "symbol": symbol, "side": "buy", "price": 1, "qty": 1,
                "ord_type": "market", "timestamp": "2026-10-05T00:00:00Z"
            }});
            let HandlerDecodeOutcome::Deliver(payload) = (callback.decode)(trade) else {
                panic!("trade decodes");
            };
            (callback.invoke)(&payload);
        }

        assert_eq!(*delivered.lock().unwrap(), vec!["PI_XBTUSD".to_string()]);
    }

    #[test]
    fn instrument_registration_returns_queue_full() {
        use crate::api::ws_surface::WsSurface;
        use crate::dispatch::CallerInbound;
        use crate::types::CallerEvent;

        let (bus, ws) = WsSurface::on_bus_with_caller_capacity(1);
        let _rx = bus.take_caller_to_io_rx().expect("caller receiver");
        bus.try_post_caller_inbound(CallerInbound::FsmEvent {
            url: WsUrl::UnifiedPublic,
            event: CallerEvent::StartConnect { request_id: 1 },
        })
        .expect("fill caller queue");

        let futures = FuturesMarketNamespace::new(ws);
        assert!(matches!(
            futures.on_instrument(|_snapshot: &FuturesInstrumentSnapshot| {}),
            Err(SubscriptionError::QueueFull)
        ));
    }
}
