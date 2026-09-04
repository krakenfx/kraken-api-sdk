//! Async Rust SDK for Kraken Spot REST + WebSocket v2. `Client` exposes the
//! market, account, trade, subscription, and events namespaces; see the
//! `README` and `examples/` directory for working programs.

#![warn(missing_docs)]
// deny, not forbid: test-only `env::{set,remove}_var` (edition 2024) needs a
// scoped `allow(unsafe_code)` in `build::test_env`.
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

pub(crate) mod transport;

pub use api::subscription::TerminationCause;
pub use api::{
    AccountError, AccountNamespace, AddOrderBatchRequest, AddOrderBatchResponse, AddOrderDescr,
    AddOrderResponse, AmendId, AmendOrderResponse, AssetMeta, AssetPairMeta, AssetPairs, Assets,
    AwaitError, Balance, BalanceUpdate, BatchOrderEntry, BatchOrderResult, BatchResult,
    CancelAllRequest, CancelAllResponse, CancelBatchRequest, CancelBatchResponse,
    CancelOrderResponse, CancelRequest, CloseOrderType, CloseTime, ClosedOrders,
    ClosedOrdersRequest, ConditionalClose, DeadlineSpec, DeadmanRequest, DeadmanResponse,
    EventSubscription, EventsNamespace, ExecType, ExecutionFee, ExecutionUpdate, ExtendedBalance,
    ExtendedBalanceEntry, FeeTier, KrakenTimestamp, LedgerEntry, LedgerType, LedgerTypeFilter,
    Ledgers, LedgersRequest, LifecyclePosition, MarketError, MarketNamespace, MaskedBalance, OFlag,
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
pub use book::{BookDelta, BookLevel, OrderBookUpdate, PriceLevel};
pub use build::{
    Client, ClientBuilder, CloseError, Completion, ConfigError, ConfigSource, KnobName, KnobValue,
    ReadyError,
};
pub use dispatch::{
    CallbackSource, ClientFailureCause, DeadmanDisarmCause, DispatchEventBus, EventCallback,
    EventEnvelope, EventPayload, EventType, HandlerHandle, HandlerId, LoopFailureCause, OrderOp,
    OrderSubmitStatus, ReactorName, Transport, WsFailReason, WsOp,
};
pub use error::{ApiError, ErrorCategory, EventsError};
pub use jitter::{FixedJitter, JitterSource};
pub use rate_limit::{Scope, Tier};
pub use rest::{RestError, RetryReason};
pub use transport::{TransportError, TransportErrorKind};
// `reqwest::header` re-exported so `ClientBuilder::with_headers` callers
// never fight a version-mismatched `http` crate for `HeaderMap` / `HeaderValue`.
pub use reqwest::header;
pub use types::{
    ApiKey, AssetCode, BookDepth, ChannelName, ClOrdId, ClOrdIdError, ConnectionState,
    RequestHandle, Symbol, SymbolError, TickerTrigger, TxId, WsUrl,
};

/// Internal types re-exported ONLY for in-repo integration-test harnesses.
/// NOT part of the public API. Gated behind the `test-support` feature.
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

    /// Construct a bare [`crate::DispatchEventBus`] outside a `Client` — test
    /// scaffolding for harnesses that drive SDK internals directly.
    #[must_use]
    pub fn new_bus(
        cfg: DispatchEventBusConfig,
        clock: std::sync::Arc<dyn Clock>,
    ) -> crate::DispatchEventBus {
        crate::DispatchEventBus::new(cfg, clock)
    }
}
