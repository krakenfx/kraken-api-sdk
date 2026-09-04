//! Inert request structs for the paginated/filtered account reads.
//! `to_params` is the only wire-shape authority.

use crate::types::{AssetClass, ClOrdId};

use super::{CloseTime, LedgerTypeFilter, TradeTypeFilter, push_opt};

/// Filters for [`AccountNamespace::open_orders`](super::AccountNamespace::open_orders).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpenOrdersRequest {
    trades: bool,
    userref: Option<i32>,
    cl_ord_id: Option<ClOrdId>,
}

impl OpenOrdersRequest {
    /// Inline each order's fills.
    #[must_use]
    pub fn trades(mut self, trades: bool) -> Self {
        self.trades = trades;
        self
    }

    /// Filter by Kraken's legacy 32-bit user reference.
    #[must_use]
    pub fn userref(mut self, userref: i32) -> Self {
        self.userref = Some(userref);
        self
    }

    /// Narrow to a single client order id, server-side.
    #[must_use]
    pub fn cl_ord_id(mut self, cl_ord_id: ClOrdId) -> Self {
        self.cl_ord_id = Some(cl_ord_id);
        self
    }

    /// Form pairs for `/0/private/OpenOrders`.
    pub(crate) fn to_params(&self) -> Vec<(String, String)> {
        let mut form: Vec<(String, String)> = vec![("trades".to_string(), self.trades.to_string())];
        push_opt(&mut form, "userref", self.userref);
        push_opt(
            &mut form,
            "cl_ord_id",
            self.cl_ord_id.as_ref().map(ClOrdId::as_str),
        );
        form
    }
}

/// Filters for [`AccountNamespace::closed_orders`](super::AccountNamespace::closed_orders).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClosedOrdersRequest {
    trades: bool,
    userref: Option<i32>,
    start: Option<u64>,
    end: Option<u64>,
    ofs: Option<u32>,
    closetime: Option<CloseTime>,
    cl_ord_id: Option<ClOrdId>,
}

impl ClosedOrdersRequest {
    /// Inline each order's fills.
    #[must_use]
    pub fn trades(mut self, trades: bool) -> Self {
        self.trades = trades;
        self
    }

    /// Filter by Kraken's legacy 32-bit user reference.
    #[must_use]
    pub fn userref(mut self, userref: i32) -> Self {
        self.userref = Some(userref);
        self
    }

    /// Lower bound of the query window.
    #[must_use]
    pub fn start(mut self, start: u64) -> Self {
        self.start = Some(start);
        self
    }

    /// Upper bound of the query window.
    #[must_use]
    pub fn end(mut self, end: u64) -> Self {
        self.end = Some(end);
        self
    }

    /// Pagination offset.
    #[must_use]
    pub fn ofs(mut self, ofs: u32) -> Self {
        self.ofs = Some(ofs);
        self
    }

    /// Which timestamp the query window applies to.
    #[must_use]
    pub fn closetime(mut self, closetime: CloseTime) -> Self {
        self.closetime = Some(closetime);
        self
    }

    /// Narrow to a single client order id, server-side.
    #[must_use]
    pub fn cl_ord_id(mut self, cl_ord_id: ClOrdId) -> Self {
        self.cl_ord_id = Some(cl_ord_id);
        self
    }

    /// Form pairs for `/0/private/ClosedOrders`.
    pub(crate) fn to_params(&self) -> Vec<(String, String)> {
        let mut form: Vec<(String, String)> = vec![("trades".to_string(), self.trades.to_string())];
        push_opt(&mut form, "userref", self.userref);
        push_opt(&mut form, "start", self.start);
        push_opt(&mut form, "end", self.end);
        push_opt(&mut form, "ofs", self.ofs);
        push_opt(
            &mut form,
            "closetime",
            self.closetime.as_ref().map(|value| value.as_ref()),
        );
        push_opt(
            &mut form,
            "cl_ord_id",
            self.cl_ord_id.as_ref().map(ClOrdId::as_str),
        );
        form
    }
}

/// Filters for [`AccountNamespace::ledgers`](super::AccountNamespace::ledgers).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct LedgersRequest {
    asset: Option<Vec<String>>,
    aclass: Option<AssetClass>,
    ledger_type: Option<LedgerTypeFilter>,
    start: Option<u64>,
    end: Option<u64>,
    ofs: Option<u32>,
}

impl LedgersRequest {
    /// Restrict to these assets (sent comma-joined).
    #[must_use]
    pub fn asset(mut self, asset: Vec<String>) -> Self {
        self.asset = Some(asset);
        self
    }

    /// Asset class filter.
    #[must_use]
    pub fn aclass(mut self, aclass: AssetClass) -> Self {
        self.aclass = Some(aclass);
        self
    }

    /// Ledger entry type filter (wire `type`).
    #[must_use]
    pub fn ledger_type(mut self, ledger_type: LedgerTypeFilter) -> Self {
        self.ledger_type = Some(ledger_type);
        self
    }

    /// Lower bound of the query window.
    #[must_use]
    pub fn start(mut self, start: u64) -> Self {
        self.start = Some(start);
        self
    }

    /// Upper bound of the query window.
    #[must_use]
    pub fn end(mut self, end: u64) -> Self {
        self.end = Some(end);
        self
    }

    /// Pagination offset.
    #[must_use]
    pub fn ofs(mut self, ofs: u32) -> Self {
        self.ofs = Some(ofs);
        self
    }

    /// Form pairs for `/0/private/Ledgers`.
    pub(crate) fn to_params(&self) -> Vec<(String, String)> {
        let mut form: Vec<(String, String)> = Vec::new();
        if let Some(a) = &self.asset {
            form.push(("asset".to_string(), a.join(",")));
        }
        push_opt(
            &mut form,
            "aclass",
            self.aclass.as_ref().map(|value| match value {
                AssetClass::Forex => "currency",
                other => other.as_ref(),
            }),
        );
        push_opt(
            &mut form,
            "type",
            self.ledger_type.as_ref().map(|value| value.as_ref()),
        );
        push_opt(&mut form, "start", self.start);
        push_opt(&mut form, "end", self.end);
        push_opt(&mut form, "ofs", self.ofs);
        form
    }
}

/// Filters for [`AccountNamespace::trades_history`](super::AccountNamespace::trades_history).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TradesHistoryRequest {
    trade_type: Option<TradeTypeFilter>,
    trades: Option<bool>,
    start: Option<u64>,
    end: Option<u64>,
    ofs: Option<u32>,
    ledgers: bool,
}

impl TradesHistoryRequest {
    /// Trade type filter (wire `type`).
    #[must_use]
    pub fn trade_type(mut self, trade_type: TradeTypeFilter) -> Self {
        self.trade_type = Some(trade_type);
        self
    }

    /// Inline the trades making up each position.
    #[must_use]
    pub fn trades(mut self, trades: bool) -> Self {
        self.trades = Some(trades);
        self
    }

    /// Lower bound of the query window.
    #[must_use]
    pub fn start(mut self, start: u64) -> Self {
        self.start = Some(start);
        self
    }

    /// Upper bound of the query window.
    #[must_use]
    pub fn end(mut self, end: u64) -> Self {
        self.end = Some(end);
        self
    }

    /// Pagination offset.
    #[must_use]
    pub fn ofs(mut self, ofs: u32) -> Self {
        self.ofs = Some(ofs);
        self
    }

    /// Inline the related ledger ids; `false` omits the key entirely.
    #[must_use]
    pub fn ledgers(mut self, ledgers: bool) -> Self {
        self.ledgers = ledgers;
        self
    }

    /// Form pairs for `/0/private/TradesHistory`.
    pub(crate) fn to_params(&self) -> Vec<(String, String)> {
        let mut form: Vec<(String, String)> = Vec::new();
        push_opt(
            &mut form,
            "type",
            self.trade_type.as_ref().map(|value| value.as_ref()),
        );
        push_opt(&mut form, "trades", self.trades);
        push_opt(&mut form, "start", self.start);
        push_opt(&mut form, "end", self.end);
        push_opt(&mut form, "ofs", self.ofs);
        push_opt(&mut form, "ledgers", self.ledgers.then_some(true));
        form
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(k: &str, v: &str) -> (String, String) {
        (k.to_string(), v.to_string())
    }

    #[test]
    fn open_orders_full_emit_and_default_omit() {
        assert_eq!(
            OpenOrdersRequest::default().to_params(),
            [pair("trades", "false")]
        );
        let full = OpenOrdersRequest::default()
            .trades(true)
            .userref(7)
            .cl_ord_id(ClOrdId::new("cl-open-01").unwrap());
        assert_eq!(
            full.to_params(),
            [
                pair("trades", "true"),
                pair("userref", "7"),
                pair("cl_ord_id", "cl-open-01"),
            ]
        );
    }

    #[test]
    fn closed_orders_full_emit_and_default_omit() {
        assert_eq!(
            ClosedOrdersRequest::default().to_params(),
            [pair("trades", "false")]
        );
        let full = ClosedOrdersRequest::default()
            .trades(true)
            .userref(-3)
            .start(1_600_000_000)
            .end(1_700_000_000)
            .ofs(50)
            .closetime(CloseTime::Close)
            .cl_ord_id(ClOrdId::new("cl-closed-1").unwrap());
        assert_eq!(
            full.to_params(),
            [
                pair("trades", "true"),
                pair("userref", "-3"),
                pair("start", "1600000000"),
                pair("end", "1700000000"),
                pair("ofs", "50"),
                pair("closetime", "close"),
                pair("cl_ord_id", "cl-closed-1"),
            ]
        );
    }

    #[test]
    fn ledgers_full_emit_comma_join_and_default_omit() {
        assert!(LedgersRequest::default().to_params().is_empty());
        let full = LedgersRequest::default()
            .asset(vec!["BTC".to_string(), "USD".to_string()])
            .aclass(AssetClass::Forex)
            .ledger_type(LedgerTypeFilter::Trade)
            .start(1_600_000_000)
            .end(1_700_000_000)
            .ofs(10);
        assert_eq!(
            full.to_params(),
            [
                pair("asset", "BTC,USD"),
                pair("aclass", "currency"),
                pair("type", "trade"),
                pair("start", "1600000000"),
                pair("end", "1700000000"),
                pair("ofs", "10"),
            ]
        );
    }

    #[test]
    fn trades_history_full_emit_and_default_omit() {
        assert!(TradesHistoryRequest::default().to_params().is_empty());
        let full = TradesHistoryRequest::default()
            .trade_type(TradeTypeFilter::ClosedPosition)
            .trades(false)
            .start(1_600_000_000)
            .end(1_700_000_000)
            .ofs(25)
            .ledgers(true);
        assert_eq!(
            full.to_params(),
            [
                pair("type", "closed position"),
                pair("trades", "false"),
                pair("start", "1600000000"),
                pair("end", "1700000000"),
                pair("ofs", "25"),
                pair("ledgers", "true"),
            ]
        );
        assert!(
            TradesHistoryRequest::default()
                .ledgers(false)
                .to_params()
                .is_empty()
        );
    }
}
