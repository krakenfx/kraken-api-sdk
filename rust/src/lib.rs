//! Async Rust SDK for Kraken Spot REST + WebSocket v2. `Client` exposes the
//! market, account, trade, subscription, and events namespaces; see the
//! `README` and `examples/` directory for working programs.

#![warn(missing_docs)]
// deny, not forbid: `build::test_env` scopes an `allow(unsafe_code)` for `env::set_var`.
#![deny(unsafe_code)]

pub mod types;

pub mod error;

pub(crate) mod build;

pub mod api;

pub(crate) mod dispatch;

pub(crate) mod book;
pub(crate) mod conn;
pub(crate) mod rate_limit;
pub(crate) mod rest;

pub(crate) mod auth;
pub(crate) mod clock;
pub(crate) mod jitter;
#[cfg(test)]
mod test_logs;

pub(crate) mod transport;

pub use api::subscription::TerminationCause;
pub use api::{
    AccountError, AccountNamespace, AddOrderBatchRequest, AddOrderBatchResponse, AddOrderDescr,
    AddOrderResponse, AmendId, AmendOrderResponse, AssetMeta, AssetPairMeta, AssetPairs, Assets,
    AwaitError, Balance, BalanceUpdate, BatchOrderEntry, BatchOrderResult, BatchResult,
    CancelAllRequest, CancelAllResponse, CancelBatchRequest, CancelBatchResponse,
    CancelOrderResponse, CancelRequest, CloseOrderType, CloseTime, ClosedOrders,
    ClosedOrdersRequest, ConditionalClose, DeadlineSpec, DeadmanRequest, DeadmanResponse,
    EventSubscription, EventsNamespace, ExecType, ExecutionFee, ExecutionTriggers, ExecutionUpdate,
    ExtendedBalance, ExtendedBalanceEntry, FeeTier, FuturesAccountNamespace, FuturesBalanceUpdate,
    FuturesEmergencyIncident, FuturesExecutionUpdate, FuturesExpiringTickerUpdate,
    FuturesIncidentStep, FuturesInstrumentAsset, FuturesInstrumentPair, FuturesInstrumentSnapshot,
    FuturesIsolatedMargin, FuturesMaintenanceWindow, FuturesMarginUpdate, FuturesMarginValues,
    FuturesMarketNamespace, FuturesMultiCollateralMargin, FuturesOhlcUpdate, FuturesOrder,
    FuturesOrderAmendRequest, FuturesOrderBatchRequest, FuturesOrderRef, FuturesOrderRequest,
    FuturesPerpetualTickerUpdate, FuturesPosition, FuturesPositionsUpdate,
    FuturesPreferencesUpdate, FuturesSingleCollateralMargin, FuturesSingleCollateralWallet,
    FuturesSubscriptionInfo, FuturesSubscriptionNamespace, FuturesSubscriptionRef,
    FuturesSystemStatusUpdate, FuturesTickerUpdate, FuturesTradeNamespace, FuturesTradeUpdate,
    FuturesTradingPermission, KrakenTimestamp, LedgerEntry, LedgerType, LedgerTypeFilter, Ledgers,
    LedgersRequest, LifecyclePosition, MarketError, MarketNamespace, MaskedBalance, OFlag,
    OhlcCandle, OhlcInterval, OhlcRequest, OhlcResult, OhlcUpdate, OpenOrders, OpenOrdersRequest,
    OpenPositionEntry, OpenPositions, OrderAmendRequest, OrderBookLevel, OrderBookSnapshot,
    OrderDescr, OrderError, OrderInfo, OrderRequest, OrderStatus, OrderType, PendingTrade,
    PositionStatus, Price, PriceUnit, RecentTrade, ReconciliationOutcome, ServerTime, Side,
    SpreadEntry, SpreadsResult, StpType, SubscribeFailureCause, SubscriptionError,
    SubscriptionGuard, SubscriptionInfo, SubscriptionNamespace, SubscriptionRef, SubscriptionState,
    SubscriptionsSummary, SystemStatus, SystemStatusUpdate, Ticker, TickerDecodeError,
    TickerResult, TickerUpdate, TimeInForce, TimeSpec, TradeBalance, TradeError, TradeHistoryEntry,
    TradeNamespace, TradeSide, TradeTypeFilter, TradeUpdate, TradeVolume, TradesHistoryRequest,
    TradesHistorySnapshot, TradesRequest, TradesResult, TriggerKind, Wallet, WsOrderStatus,
};
pub use auth::AuthError;
pub use book::{
    BookDelta, BookLevel, FuturesBookDelta, FuturesOrderBookUpdate, OrderBookUpdate, PriceLevel,
};
pub use build::{
    Client, ClientBuilder, CloseError, Completion, ConfigError, ConfigSource, KnobName, KnobValue,
    ReadyError,
};
pub use dispatch::{
    CallbackSource, ClientFailureCause, DeadmanDisarmCause, DispatchEventBus, EventCallback,
    EventEnvelope, EventPayload, EventType, FuturesSubscribeAck, FuturesSubscribeFailureClass,
    HandlerHandle, HandlerId, LoopFailureCause, OrderOp, OrderSubmitStatus, ReactorName, Transport,
    WsFailReason, WsOp,
};
pub use error::{ApiError, ErrorCategory, EventsError};
pub use jitter::{FixedJitter, JitterSource};
pub use rate_limit::{Scope, Tier};
pub use rest::{RestError, RetryReason};
pub use transport::{TransportError, TransportErrorKind};
// Re-exported so `with_headers` callers share this crate's `http` version.
pub use reqwest::header;
pub use types::{
    ApiKey, AssetCode, BookDepth, ChannelName, ClOrdId, ClOrdIdError, ConnectionState,
    FuturesOhlcTickType, RequestHandle, Symbol, SymbolError, TickerTrigger, TxId, WsUrl,
};

/// Internals for in-repo test harnesses; not public API (`test-support` feature).
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod test_support {
    pub use crate::auth::{
        AuthSigner, AuthStack, SpotRestHmacSha512Signer, SystemClockNonceSource,
        TokenLifecycleManager,
    };
    pub use crate::build::Knobs;
    pub use crate::clock::{Clock, SystemClock};
    pub use crate::dispatch::DispatchEventBusConfig;
    pub use crate::rate_limit::{SpotApiRateLimitTracker, SpotTradingRateLimitTracker, Tier};
    pub use crate::rest::RestSurface;
    pub use crate::transport::{HttpTransport, ReqwestHttpTransport};
    pub use crate::types::{ApiSecret, AuthProfile};

    /// Constructs a bare [`crate::DispatchEventBus`] outside a `Client`.
    #[must_use]
    pub fn new_bus(
        cfg: DispatchEventBusConfig,
        clock: std::sync::Arc<dyn Clock>,
    ) -> crate::DispatchEventBus {
        crate::DispatchEventBus::new(cfg, clock)
    }
}
