//! Dispatch table: `(Op, Product, Transport)` → adapter entry (wire target + auth).
//! Frozen at `.build()`; `resolve` is a pure hash lookup.

use std::collections::HashMap;

/// Operation a caller invokes. The `(Op, Product, Transport)` key resolves the
/// wire target + auth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    MarketTicker,
    MarketOrderBook,
    MarketTrades,
    MarketOhlc,
    MarketSpreads,
    MarketAssets,
    MarketAssetPairs,
    MarketServerTime,
    MarketSystemStatus,

    AccountBalance,
    AccountBalanceEx,
    AccountTradeBalance,
    AccountOpenOrders,
    AccountClosedOrders,
    AccountLedgers,
    AccountQueryLedgers,
    AccountTradesHistory,
    AccountOpenPositions,
    AccountTradeVolume,

    TradeOrder,
    TradeOrderCancel,
    TradeOrderAmend,
    TradeOrderCancelAll,
    TradeOrderCancelAllAfter,
    TradeOrderBatch,
    /// Pseudo-op for `cancel_batch` fan-out via N concurrent `TradeOrderCancel`
    /// calls. The seed points at the single-cancel endpoint so resolution succeeds.
    TradeOrderCancelBatch,
}

/// Product family the operation targets. Spot is the only v1 target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Product {
    Spot,
    Futures,
}

/// Wire transport — third leg of `DispatchKey`. Disambiguates multi-transport
/// trade ops (each has both a `Rest` and a `WsV2Auth` entry).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Transport {
    /// Spot REST over HTTPS — the only transport for market/account ops in v1.
    Rest,
    /// Public WS v2 — unauthenticated data channels.
    WsV2Public,
    /// Authenticated WS v2 — token-authenticated trade ops.
    WsV2Auth,
    /// Unreachable in v1 — never seeded.
    WsV2L3,
    /// Futures REST (v2).
    FuturesRest,
    /// Futures WS (v2).
    FuturesWs,
}

/// `(Op, Product, Transport)` key selecting an `AdapterEntry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DispatchKey {
    pub op: Op,
    pub product: Product,
    pub transport: Transport,
}

/// Wire target + auth requirement returned by `resolve`.
#[derive(Debug, Clone)]
pub struct AdapterEntry {
    /// Wire path (REST) or channel/method name (WS).
    pub wire_target: &'static str,
    /// Whether the operation requires authentication.
    pub requires_auth: bool,
}

/// Returned by `resolve` when the `(Op, Product, Transport)` triple is not in
/// the table.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Dispatch key not in table: op={op:?}, product={product:?}, transport={transport:?}.")]
pub struct DispatchError {
    pub op: Op,
    pub product: Product,
    pub transport: Transport,
}

/// Frozen-at-`.build()` table mapping `(Op, Product, Transport)` to `AdapterEntry`.
pub struct DispatchTable {
    table: HashMap<DispatchKey, AdapterEntry>,
}

impl DispatchTable {
    /// Construct with a pre-built table.
    pub fn new(table: HashMap<DispatchKey, AdapterEntry>) -> Self {
        Self { table }
    }

    /// Construct with the v1 Spot table.
    pub fn with_default_spot_table() -> Self {
        use Op::*;
        use Transport::*;
        let mut table = HashMap::new();
        let mut seed = |op: Op, transport: Transport, wire_target, requires_auth| {
            table.insert(
                DispatchKey {
                    op,
                    product: Product::Spot,
                    transport,
                },
                AdapterEntry {
                    wire_target,
                    requires_auth,
                },
            );
        };
        seed(MarketTicker, Rest, "/0/public/Ticker", false);
        seed(MarketOrderBook, Rest, "/0/public/Depth", false);
        seed(MarketTrades, Rest, "/0/public/Trades", false);
        seed(MarketOhlc, Rest, "/0/public/OHLC", false);
        seed(MarketSpreads, Rest, "/0/public/Spread", false);
        seed(MarketAssets, Rest, "/0/public/Assets", false);
        seed(MarketAssetPairs, Rest, "/0/public/AssetPairs", false);
        seed(MarketServerTime, Rest, "/0/public/Time", false);
        seed(MarketSystemStatus, Rest, "/0/public/SystemStatus", false);
        seed(AccountBalance, Rest, "/0/private/Balance", true);
        seed(AccountBalanceEx, Rest, "/0/private/BalanceEx", true);
        seed(AccountTradeBalance, Rest, "/0/private/TradeBalance", true);
        seed(AccountOpenOrders, Rest, "/0/private/OpenOrders", true);
        seed(AccountClosedOrders, Rest, "/0/private/ClosedOrders", true);
        seed(AccountLedgers, Rest, "/0/private/Ledgers", true);
        seed(AccountQueryLedgers, Rest, "/0/private/QueryLedgers", true);
        seed(AccountTradesHistory, Rest, "/0/private/TradesHistory", true);
        seed(AccountOpenPositions, Rest, "/0/private/OpenPositions", true);
        seed(AccountTradeVolume, Rest, "/0/private/TradeVolume", true);
        // Spot trade ops — both Rest and WsV2Auth entries charge the trading tracker.
        seed(TradeOrder, Rest, "/0/private/AddOrder", true);
        seed(TradeOrder, WsV2Auth, "add_order", true);
        seed(TradeOrderCancel, Rest, "/0/private/CancelOrder", true);
        seed(TradeOrderCancel, WsV2Auth, "cancel_order", true);
        seed(TradeOrderAmend, Rest, "/0/private/AmendOrder", true);
        seed(TradeOrderAmend, WsV2Auth, "amend_order", true);
        seed(TradeOrderCancelAll, Rest, "/0/private/CancelAll", true);
        seed(TradeOrderCancelAll, WsV2Auth, "cancel_all", true);
        seed(
            TradeOrderCancelAllAfter,
            Rest,
            "/0/private/CancelAllOrdersAfter",
            true,
        );
        seed(
            TradeOrderCancelAllAfter,
            WsV2Auth,
            "cancel_all_orders_after",
            true,
        );
        seed(TradeOrderBatch, Rest, "/0/private/AddOrderBatch", true);
        seed(TradeOrderBatch, WsV2Auth, "batch_add", true);
        // cancel_batch fans out via TradeOrderCancel; seed points at single-cancel.
        // No WsV2Auth entry — cancel_batch is REST-only.
        seed(TradeOrderCancelBatch, Rest, "/0/private/CancelOrder", true);
        Self::new(table)
    }

    /// Resolve `(op, product, transport)` → `AdapterEntry`.
    pub fn resolve(
        &self,
        op: Op,
        product: Product,
        transport: Transport,
    ) -> Result<&AdapterEntry, DispatchError> {
        self.table
            .get(&DispatchKey {
                op,
                product,
                transport,
            })
            .ok_or(DispatchError {
                op,
                product,
                transport,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_spot_table_resolves_market_ticker_to_canonical_path() {
        let dispatch = DispatchTable::with_default_spot_table();
        let entry = dispatch
            .resolve(Op::MarketTicker, Product::Spot, Transport::Rest)
            .expect("MarketTicker/Spot/Rest should resolve");
        assert_eq!(entry.wire_target, "/0/public/Ticker");
        assert!(!entry.requires_auth);
    }

    #[test]
    fn trade_order_buy_has_both_rest_and_ws_entries() {
        let dispatch = DispatchTable::with_default_spot_table();
        let rest = dispatch
            .resolve(Op::TradeOrder, Product::Spot, Transport::Rest)
            .expect("TradeOrder/Spot/Rest should resolve");
        assert_eq!(rest.wire_target, "/0/private/AddOrder");
        assert!(rest.requires_auth);
        let ws = dispatch
            .resolve(Op::TradeOrder, Product::Spot, Transport::WsV2Auth)
            .expect("TradeOrder/Spot/WsV2Auth should resolve");
        assert_eq!(ws.wire_target, "add_order");
        assert!(ws.requires_auth);
    }

    #[test]
    fn account_read_resolves_to_private_path_with_auth() {
        let dispatch = DispatchTable::with_default_spot_table();
        let entry = dispatch
            .resolve(Op::AccountBalance, Product::Spot, Transport::Rest)
            .expect("AccountBalance/Spot/Rest should resolve");
        assert_eq!(entry.wire_target, "/0/private/Balance");
        assert!(entry.requires_auth);
    }

    #[test]
    fn wrong_transport_for_op_returns_not_in_table() {
        let dispatch = DispatchTable::with_default_spot_table();
        let err = dispatch
            .resolve(Op::MarketTicker, Product::Spot, Transport::WsV2Public)
            .expect_err("ticker has no WsV2Public entry");
        assert_eq!(err.transport, Transport::WsV2Public);
    }

    #[test]
    fn futures_op_returns_not_in_table() {
        let dispatch = DispatchTable::with_default_spot_table();
        let err = dispatch
            .resolve(Op::MarketTicker, Product::Futures, Transport::Rest)
            .expect_err("Futures should be NotInTable in v1");
        assert_eq!(err.op, Op::MarketTicker);
        assert_eq!(err.product, Product::Futures);
    }

    #[test]
    fn empty_table_resolves_nothing() {
        let dispatch = DispatchTable::new(HashMap::new());
        assert!(
            dispatch
                .resolve(Op::MarketTicker, Product::Spot, Transport::Rest)
                .is_err()
        );
    }

    #[test]
    fn every_seeded_op_resolves_to_its_expected_endpoint() {
        use Op::*;
        use Transport::*;
        let d = DispatchTable::with_default_spot_table();
        let expected: &[(Op, Transport, &str, bool)] = &[
            (MarketTicker, Rest, "/0/public/Ticker", false),
            (MarketOrderBook, Rest, "/0/public/Depth", false),
            (MarketTrades, Rest, "/0/public/Trades", false),
            (MarketOhlc, Rest, "/0/public/OHLC", false),
            (MarketSpreads, Rest, "/0/public/Spread", false),
            (MarketAssets, Rest, "/0/public/Assets", false),
            (MarketAssetPairs, Rest, "/0/public/AssetPairs", false),
            (MarketServerTime, Rest, "/0/public/Time", false),
            (MarketSystemStatus, Rest, "/0/public/SystemStatus", false),
            (AccountBalance, Rest, "/0/private/Balance", true),
            (AccountBalanceEx, Rest, "/0/private/BalanceEx", true),
            (AccountTradeBalance, Rest, "/0/private/TradeBalance", true),
            (AccountOpenOrders, Rest, "/0/private/OpenOrders", true),
            (AccountClosedOrders, Rest, "/0/private/ClosedOrders", true),
            (AccountLedgers, Rest, "/0/private/Ledgers", true),
            (AccountQueryLedgers, Rest, "/0/private/QueryLedgers", true),
            (AccountTradesHistory, Rest, "/0/private/TradesHistory", true),
            (AccountOpenPositions, Rest, "/0/private/OpenPositions", true),
            (AccountTradeVolume, Rest, "/0/private/TradeVolume", true),
            (TradeOrder, Rest, "/0/private/AddOrder", true),
            (TradeOrder, Rest, "/0/private/AddOrder", true),
            (TradeOrderCancel, Rest, "/0/private/CancelOrder", true),
            (TradeOrderAmend, Rest, "/0/private/AmendOrder", true),
            (TradeOrderCancelAll, Rest, "/0/private/CancelAll", true),
            (
                TradeOrderCancelAllAfter,
                Rest,
                "/0/private/CancelAllOrdersAfter",
                true,
            ),
            (TradeOrderBatch, Rest, "/0/private/AddOrderBatch", true),
            (TradeOrderCancelBatch, Rest, "/0/private/CancelOrder", true),
            (TradeOrder, WsV2Auth, "add_order", true),
            (TradeOrder, WsV2Auth, "add_order", true),
            (TradeOrderCancel, WsV2Auth, "cancel_order", true),
            (TradeOrderAmend, WsV2Auth, "amend_order", true),
            (TradeOrderCancelAll, WsV2Auth, "cancel_all", true),
            (
                TradeOrderCancelAllAfter,
                WsV2Auth,
                "cancel_all_orders_after",
                true,
            ),
            (TradeOrderBatch, WsV2Auth, "batch_add", true),
        ];
        for (op, transport, target, auth) in expected {
            let e = d
                .resolve(*op, Product::Spot, *transport)
                .unwrap_or_else(|_| panic!("{op:?}/{transport:?} must resolve"));
            assert_eq!(e.wire_target, *target, "{op:?}/{transport:?} endpoint");
            assert_eq!(e.requires_auth, *auth, "{op:?}/{transport:?} auth");
        }
    }
}
