//! Public API — caller-facing async namespaces. Every method here is `async` and
//! must be awaited on a tokio runtime.

use std::collections::HashMap;

use serde_json::Value;

pub mod account;
pub mod await_handle;
pub mod events;
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
    ClosedOrdersRequest, ExecType, ExecutionFee, ExecutionUpdate, ExtendedBalance,
    ExtendedBalanceEntry, KrakenTimestamp, LedgerEntry, LedgerType, LedgerTypeFilter, Ledgers,
    LedgersRequest, LifecyclePosition, MaskedBalance, OpenOrders, OpenOrdersRequest,
    OpenPositionEntry, OpenPositions, OrderDescr, OrderInfo, OrderStatus, PositionStatus,
    ReconciliationOutcome, TradeBalance, TradeHistoryEntry, TradeTypeFilter, TradeVolume,
    TradesHistoryRequest, TradesHistorySnapshot, Wallet, WsOrderStatus,
};
pub use events::{EventSubscription, EventsNamespace};
pub use market::{
    AssetMeta, AssetPairMeta, AssetPairs, Assets, FeeTier, MarketError, MarketNamespace,
    OhlcCandle, OhlcInterval, OhlcRequest, OhlcResult, OhlcUpdate, OrderBookLevel,
    OrderBookSnapshot, RecentTrade, ServerTime, SpreadEntry, SpreadsResult, SystemStatus,
    SystemStatusUpdate, Ticker, TickerDecodeError, TickerResult, TickerUpdate, TradeSide,
    TradeUpdate, TradesRequest, TradesResult,
};
pub use subscription_guard::SubscriptionGuard;
pub use subscription_namespace::SubscriptionNamespace;
pub use subscription_types::{
    SubscribeFailureCause, SubscriptionError, SubscriptionInfo, SubscriptionRef, SubscriptionState,
    SubscriptionsSummary,
};
pub use trade::{
    AddOrderBatchRequest, AddOrderBatchResponse, AddOrderDescr, AddOrderResponse, AmendId,
    AmendOrderResponse, BatchOrderEntry, BatchOrderResult, BatchResult, CancelAllRequest,
    CancelAllResponse, CancelBatchRequest, CancelBatchResponse, CancelOrderResponse, CancelRequest,
    CloseOrderType, ConditionalClose, DeadlineSpec, DeadmanRequest, DeadmanResponse, OFlag,
    OrderAmendRequest, OrderError, OrderRequest, OrderType, PendingTrade, Price, PriceUnit, Side,
    StpType, TimeInForce, TimeSpec, TradeError, TradeNamespace, TriggerKind,
};

/// Crate-internal seam so dispatch helpers stay generic over REST-encoded
/// request types. `to_params`/`to_form` on each struct remain the wire-shape
/// authority (WS JSON is separate); impls here only delegate.
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

/// Decode a Kraken `result` JSON object into a `HashMap<K, V>`, applying
/// `from_entry` to each decoded `(key, Raw)` pair.
///
/// Shared by the market and account REST namespaces: `on_error` adapts a
/// context-tagged message to the caller's error type; `from_entry` performs the
/// per-entry `Raw -> (K, V)` transform. `result` is consumed so each row moves
/// into `from_value` with no per-entry clone.
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
