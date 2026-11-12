//! Caller-facing async namespaces; methods are awaited on a tokio runtime.

use std::collections::HashMap;

use serde_json::Value;

pub mod account;
pub mod await_handle;
pub mod events;
mod futures_subscription;
pub mod market;
pub mod subscription;
pub mod subscription_guard;
pub mod subscription_namespace;
pub mod subscription_types;
pub mod trade;
pub(crate) mod ws_decode;
pub(crate) mod ws_surface;

pub use await_handle::{AwaitError, await_request_handle};
pub(crate) use await_handle::{
    CorrelatedArms, TxCell, arm_correlated, resolve_armed, unarm_correlated,
};

pub use account::{
    AccountError, AccountNamespace, Balance, BalanceUpdate, CloseTime, ClosedOrders,
    ClosedOrdersRequest, ExecType, ExecutionFee, ExecutionTriggers, ExecutionUpdate,
    ExtendedBalance, ExtendedBalanceEntry, FuturesAccountNamespace, FuturesBalanceUpdate,
    FuturesExecutionUpdate, FuturesIsolatedMargin, FuturesMarginUpdate, FuturesMarginValues,
    FuturesMultiCollateralMargin, FuturesPosition, FuturesPositionsUpdate,
    FuturesPreferencesUpdate, FuturesSingleCollateralMargin, FuturesSingleCollateralWallet,
    FuturesTradingPermission, KrakenTimestamp, LedgerEntry, LedgerType, LedgerTypeFilter, Ledgers,
    LedgersRequest, LifecyclePosition, MaskedBalance, OpenOrders, OpenOrdersRequest,
    OpenPositionEntry, OpenPositions, OrderDescr, OrderInfo, OrderStatus, PositionStatus,
    ReconciliationOutcome, TradeBalance, TradeHistoryEntry, TradeTypeFilter, TradeVolume,
    TradesHistoryRequest, TradesHistorySnapshot, Wallet, WsOrderStatus,
};
pub use events::{EventSubscription, EventsNamespace};
pub use futures_subscription::FuturesSubscriptionNamespace;
pub use market::{
    AssetMeta, AssetPairMeta, AssetPairs, Assets, FeeTier, FuturesEmergencyIncident,
    FuturesExpiringTickerUpdate, FuturesIncidentStep, FuturesInstrumentAsset,
    FuturesInstrumentPair, FuturesInstrumentSnapshot, FuturesMaintenanceWindow,
    FuturesMarketNamespace, FuturesOhlcUpdate, FuturesPerpetualTickerUpdate,
    FuturesSystemStatusUpdate, FuturesTickerUpdate, FuturesTradeUpdate, MarketError,
    MarketNamespace, OhlcCandle, OhlcInterval, OhlcRequest, OhlcResult, OhlcUpdate, OrderBookLevel,
    OrderBookSnapshot, RecentTrade, ServerTime, SpreadEntry, SpreadsResult, SystemStatus,
    SystemStatusUpdate, Ticker, TickerDecodeError, TickerResult, TickerUpdate, TradeSide,
    TradeUpdate, TradesRequest, TradesResult,
};
pub use subscription_guard::SubscriptionGuard;
pub use subscription_namespace::SubscriptionNamespace;
pub use subscription_types::{
    FuturesSubscriptionInfo, FuturesSubscriptionRef, SubscribeFailureCause, SubscriptionError,
    SubscriptionInfo, SubscriptionRef, SubscriptionState, SubscriptionsSummary,
};
pub use trade::{
    AddOrderBatchRequest, AddOrderBatchResponse, AddOrderDescr, AddOrderResponse, AmendId,
    AmendOrderResponse, BatchOrderEntry, BatchOrderResult, BatchResult, CancelAllRequest,
    CancelAllResponse, CancelBatchRequest, CancelBatchResponse, CancelOrderResponse, CancelRequest,
    CloseOrderType, ConditionalClose, DeadlineSpec, DeadmanRequest, DeadmanResponse, FuturesOrder,
    FuturesOrderAmendRequest, FuturesOrderBatchRequest, FuturesOrderRef, FuturesOrderRequest,
    FuturesTradeNamespace, OFlag, OrderAmendRequest, OrderError, OrderRequest, OrderType,
    PendingTrade, Price, PriceUnit, Side, StpType, TimeInForce, TimeSpec, TradeError,
    TradeNamespace, TriggerKind,
};

/// Lets dispatch helpers stay generic over REST-encoded requests; impls only delegate.
pub(crate) trait WireRequest {
    /// Ordered wire key/value pairs for this request.
    fn wire_params(&self) -> Vec<(String, String)>;
}

impl WireRequest for TradesRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_params()
    }
}

impl WireRequest for OpenOrdersRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_params()
    }
}

impl WireRequest for ClosedOrdersRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_params()
    }
}

impl WireRequest for LedgersRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_params()
    }
}

impl WireRequest for TradesHistoryRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_params()
    }
}

impl WireRequest for OrderRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_form()
    }
}

impl WireRequest for OrderAmendRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_form()
    }
}

impl WireRequest for CancelRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_form()
    }
}

impl WireRequest for CancelAllRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_form()
    }
}

impl WireRequest for DeadmanRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_form()
    }
}

impl WireRequest for AddOrderBatchRequest {
    fn wire_params(&self) -> Vec<(String, String)> {
        self.to_form()
    }
}

/// Decodes a Kraken `result` object into a `HashMap<K, V>`, mapping each entry with `from_entry`.
/// Takes `result` by value so each row moves into `from_value` without a per-entry clone.
pub(crate) fn decode_object_map<Raw, K, V, E>(
    result: Value,
    ctx: &str,
    on_error: impl Fn(String) -> E,
    from_entry: impl Fn(String, Raw) -> Result<(K, V), E>,
) -> Result<HashMap<K, V>, E>
where
    Raw: serde::de::DeserializeOwned,
    K: std::hash::Hash + Eq,
{
    let Value::Object(obj) = result else {
        return Err(on_error(format!("{ctx}: result not an object")));
    };
    let mut map = HashMap::with_capacity(obj.len());
    for (key, raw_value) in obj {
        let raw: Raw = serde_json::from_value(raw_value)
            .map_err(|e| on_error(format!("{ctx} decode {key}: {e}")))?;
        let (k, v) = from_entry(key, raw)?;
        map.insert(k, v);
    }
    Ok(map)
}
