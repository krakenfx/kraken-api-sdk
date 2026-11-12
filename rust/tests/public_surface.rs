//! Offline public-surface proofs, compiled as an external crate: every type a
//! caller must hold is nameable through the crate root. No network, no credentials.

use kraken_sdk::{
    BookLevel, ChannelName, ClientBuilder, FuturesBookDelta, FuturesExpiringTickerUpdate,
    FuturesInstrumentAsset, FuturesInstrumentPair, FuturesInstrumentSnapshot,
    FuturesMarketNamespace, FuturesOhlcUpdate, FuturesOrderBookUpdate,
    FuturesPerpetualTickerUpdate, FuturesSubscriptionInfo, FuturesSubscriptionNamespace,
    FuturesSubscriptionRef, FuturesSystemStatusUpdate, FuturesTickerUpdate, FuturesTradeUpdate,
    SubscriptionsSummary, TickerUpdate,
};

#[tokio::test]
async fn handler_handle_is_nameable_through_the_crate_root() {
    let client = ClientBuilder::new()
        .build()
        .expect("unauthenticated build is infallible");
    let registered: Result<kraken_sdk::HandlerHandle, kraken_sdk::SubscriptionError> =
        client.market().on_ticker(|_t: &TickerUpdate| {});
    let handle = registered.expect("register Spot ticker handler");
    assert_eq!(handle.channel(), ChannelName::Ticker);
}

#[tokio::test]
async fn futures_market_and_subscription_flow_is_usable_alongside_spot() {
    let client = ClientBuilder::new().build().expect("build");
    let futures = client.market().futures();
    let subscriptions = client.subscription().futures();
    let symbols = vec!["PI_XBTUSD".to_string()];

    let _status = futures
        .on_system_status(|_: &FuturesSystemStatusUpdate| {})
        .expect("register Futures status handler");
    let _instrument = futures
        .on_instrument(|_: &FuturesInstrumentSnapshot| {})
        .expect("register Futures instrument handler");
    let _ticker = futures
        .on_ticker(|_: &FuturesTickerUpdate| {})
        .expect("register Futures ticker handler");
    let _trade = futures
        .on_trade(|_: &FuturesTradeUpdate| {})
        .expect("register Futures trade handler");
    let _ohlc = futures
        .on_ohlc(|_: &FuturesOhlcUpdate| {})
        .expect("register Futures ohlc handler");
    let _book = futures
        .on_book(|_: &FuturesOrderBookUpdate| {})
        .expect("register Futures maintained-book handler");
    let _book_raw = futures
        .on_book_raw(|_: &FuturesBookDelta| {})
        .expect("register Futures raw-book handler");

    subscriptions
        .subscribe_instrument()
        .expect("subscribe Futures instrument");
    subscriptions
        .subscribe_ticker(symbols.clone(), Some(true))
        .expect("subscribe Futures ticker");
    subscriptions
        .subscribe_trade(symbols.clone(), Some(true))
        .expect("subscribe Futures trade");
    subscriptions
        .subscribe_ohlc(
            symbols.clone(),
            kraken_sdk::OhlcInterval::M1,
            Some(true),
            Some(kraken_sdk::FuturesOhlcTickType::Mark),
        )
        .expect("subscribe Futures ohlc");
    subscriptions
        .unsubscribe_ohlc(
            symbols.clone(),
            kraken_sdk::OhlcInterval::M1,
            Some(kraken_sdk::FuturesOhlcTickType::Mark),
        )
        .expect("unsubscribe Futures ohlc");
    subscriptions
        .subscribe_book(symbols.clone(), kraken_sdk::BookDepth::D10)
        .expect("subscribe Futures maintained book");
    subscriptions
        .unsubscribe_book(symbols.clone())
        .expect("unsubscribe Futures maintained book");
    subscriptions
        .subscribe_book_raw(symbols, kraken_sdk::BookDepth::D10, Some(false))
        .expect("subscribe Futures raw book");
    subscriptions
        .unsubscribe_book_raw(vec!["PI_XBTUSD".to_string()])
        .expect("unsubscribe Futures raw book");

    let active: Vec<FuturesSubscriptionInfo> = subscriptions
        .list_active()
        .expect("read Futures subscriptions");
    let tickers: Vec<FuturesSubscriptionRef> = subscriptions
        .find_by_channel(ChannelName::Ticker)
        .expect("read Futures ticker subscriptions");
    let summary: SubscriptionsSummary = subscriptions
        .status_summary()
        .expect("read Futures subscription summary");
    assert!(
        active.is_empty(),
        "reactor has not consumed queued requests"
    );
    assert!(
        tickers.is_empty(),
        "reactor has not consumed queued requests"
    );
    assert_eq!(summary.total, 0, "no registry rows exist before ready()");

    let _: &FuturesMarketNamespace = client.market().futures();
    let _: Result<kraken_sdk::SubscriptionGuard, kraken_sdk::SubscriptionError> = futures
        .on_ohlc_for(
            vec!["PF_ETHUSD".to_string()],
            kraken_sdk::OhlcInterval::M5,
            None,
            None,
            |_: &FuturesOhlcUpdate| {},
        );
    let _: &FuturesSubscriptionNamespace = client.subscription().futures();
    let _: Result<kraken_sdk::SubscriptionGuard, kraken_sdk::SubscriptionError> = futures
        .on_ticker_for(
            vec!["PI_ETHUSD".to_string()],
            None,
            |_: &FuturesTickerUpdate| {},
        );
    let _: Result<kraken_sdk::SubscriptionGuard, kraken_sdk::SubscriptionError> = futures
        .on_book_for(
            vec!["PI_ETHUSD".to_string()],
            kraken_sdk::BookDepth::D10,
            |_| {},
        );
    let _: Result<kraken_sdk::SubscriptionGuard, kraken_sdk::SubscriptionError> = futures
        .on_book_raw_for(
            vec!["PI_ETHUSD".to_string()],
            kraken_sdk::BookDepth::D10,
            Some(false),
            |_| {},
        );
    let _: Result<(), kraken_sdk::SubscriptionError> = subscriptions.unsubscribe_instrument();
    let _: Result<Vec<FuturesSubscriptionRef>, kraken_sdk::SubscriptionError> =
        subscriptions.find_by_channel(ChannelName::Instrument);
    let _: Result<(), kraken_sdk::SubscriptionError> =
        subscriptions.unsubscribe_channel(ChannelName::Instrument);
    let _: Result<(), kraken_sdk::SubscriptionError> = subscriptions.unsubscribe_all();

    let _asset: Option<FuturesInstrumentAsset> = None;
    let _pair: Option<FuturesInstrumentPair> = None;
    let _perpetual: Option<FuturesPerpetualTickerUpdate> = None;
    let _expiring: Option<FuturesExpiringTickerUpdate> = None;
    let _level: Option<BookLevel> = None;

    let _spot = client
        .market()
        .on_ticker(|_: &TickerUpdate| {})
        .expect("register Spot ticker handler");
}

#[tokio::test]
async fn futures_preferences_surface_is_nameable_through_the_crate_root() {
    use kraken_sdk::{FuturesPreferencesUpdate, FuturesTradingPermission};

    let client = ClientBuilder::new().build().expect("build");
    let account = client.account().futures();
    let subscriptions = client.subscription().futures();

    let _handle = account
        .on_preferences(|update: &FuturesPreferencesUpdate| {
            let _: Option<FuturesTradingPermission> = update.futures_trading;
        })
        .expect("register Futures preferences handler");
    subscriptions
        .subscribe_preferences()
        .expect("subscribe Futures preferences");
    subscriptions
        .unsubscribe_preferences()
        .expect("unsubscribe Futures preferences");
    let _: Result<kraken_sdk::SubscriptionGuard, kraken_sdk::SubscriptionError> =
        account.on_preferences_for(|_: &FuturesPreferencesUpdate| {});
}

#[tokio::test]
async fn futures_account_snapshot_options_exist_only_where_the_venue_honours_them() {
    use kraken_sdk::{
        FuturesBalanceUpdate, FuturesExecutionUpdate, FuturesMarginUpdate, FuturesPositionsUpdate,
        SubscriptionError, SubscriptionGuard,
    };

    let client = ClientBuilder::new().build().expect("build");
    let account = client.account().futures();
    let subscriptions = client.subscription().futures();
    let _: Result<(), SubscriptionError> = subscriptions.subscribe_margin();
    let _: Result<(), SubscriptionError> = subscriptions.subscribe_balances(Some(false));
    let _: Result<(), SubscriptionError> = subscriptions.subscribe_positions(None);
    let _: Result<(), SubscriptionError> =
        subscriptions.subscribe_executions(Some(false), Some(true));
    let _: Result<SubscriptionGuard, SubscriptionError> =
        account.on_margin_for(|_: &FuturesMarginUpdate| {});
    let _: Result<SubscriptionGuard, SubscriptionError> =
        account.on_balances_for(Some(false), |_: &FuturesBalanceUpdate| {});
    let _: Result<SubscriptionGuard, SubscriptionError> =
        account.on_positions_for(None, |_: &FuturesPositionsUpdate| {});
    let _: Result<SubscriptionGuard, SubscriptionError> =
        account.on_executions_for(Some(false), Some(true), |_: &FuturesExecutionUpdate| {});
}

#[tokio::test]
async fn futures_trade_surface_is_nameable_through_the_crate_root() {
    use kraken_sdk::{
        AddOrderBatchResponse, AddOrderResponse, AmendOrderResponse, CancelAllResponse,
        CancelBatchRequest, CancelOrderResponse, ClOrdId, DeadmanRequest, DeadmanResponse,
        FuturesOrder, FuturesOrderAmendRequest, FuturesOrderBatchRequest, FuturesOrderRef,
        FuturesOrderRequest, FuturesTradeNamespace, OrderType, PendingTrade, Side,
    };

    let client = ClientBuilder::new().build().expect("build");
    let futures: &FuturesTradeNamespace = client.trade().futures();
    let order = FuturesOrder::new(Side::Buy, 1.into()).order_type(OrderType::Market);
    let id = ClOrdId::allocate_v4();

    let _: PendingTrade<FuturesOrderRequest, AddOrderResponse> =
        futures.order(FuturesOrderRequest::new("PF_XBTUSD", order.clone()));
    let _: PendingTrade<FuturesOrderAmendRequest, AmendOrderResponse> =
        futures.order_amend(FuturesOrderAmendRequest::by_cl_ord_id(id.clone()));
    let _: PendingTrade<FuturesOrderRef, CancelOrderResponse> = futures.cancel(id.clone());
    let _: PendingTrade<CancelBatchRequest, CancelAllResponse> =
        futures.cancel_batch(vec![id.clone(), id]);
    let _: PendingTrade<kraken_sdk::CancelAllRequest, CancelAllResponse> = futures.cancel_all();
    let _: PendingTrade<DeadmanRequest, DeadmanResponse> = futures.cancel_all_orders_after(0);
    let _: PendingTrade<FuturesOrderBatchRequest, AddOrderBatchResponse> =
        futures.order_batch(FuturesOrderBatchRequest::new("PF_XBTUSD", vec![order; 2]));

    let _spot = client.trade().cancel_all();
}
