//! `MarketNamespace` — Spot public REST market data. Needs no credentials and
//! charges no rate-limit budget; transient failures retry automatically.

use std::sync::Arc;

use crate::api::ws_surface::WsSurface;
use crate::dispatch::dispatch_table::{DispatchTable, Op, Product, Transport};
use crate::rest::RestSurface;

mod error;
mod futures;
mod futures_ws_types;
mod requests;
mod rest;
mod types;
pub(crate) mod ws;
pub(crate) mod ws_types;

#[cfg(test)]
mod tests;

pub use error::MarketError;
pub use futures::FuturesMarketNamespace;
pub use futures_ws_types::{
    FuturesEmergencyIncident, FuturesExpiringTickerUpdate, FuturesIncidentStep,
    FuturesInstrumentAsset, FuturesInstrumentPair, FuturesInstrumentSnapshot,
    FuturesMaintenanceWindow, FuturesOhlcUpdate, FuturesPerpetualTickerUpdate,
    FuturesSystemStatusUpdate, FuturesTickerUpdate, FuturesTradeUpdate,
};
#[cfg(test)]
pub(crate) use futures_ws_types::{
    decode_book as decode_futures_book, decode_book_raw as decode_futures_book_raw,
    decode_ohlc as decode_futures_ohlc, decode_ticker as decode_futures_ticker,
    decode_trade as decode_futures_trade,
};
pub use requests::{OhlcRequest, TradesRequest};
pub use types::{
    AssetMeta, AssetPairMeta, AssetPairs, Assets, FeeTier, OhlcCandle, OhlcInterval, OhlcResult,
    OrderBookLevel, OrderBookSnapshot, RecentTrade, ServerTime, SpreadEntry, SpreadsResult,
    SystemStatus, Ticker, TickerResult, TradesResult,
};
pub use ws_types::{
    OhlcUpdate, SystemStatusUpdate, TickerDecodeError, TickerUpdate, TradeSide, TradeUpdate,
};

/// Spot REST public market-data namespace. Accessed via `client.market()`.
pub struct MarketNamespace {
    rest: Arc<RestSurface>,
    ws: Arc<WsSurface>,
    dispatch: Arc<DispatchTable>,
    futures: FuturesMarketNamespace,
    /// Wire-result-key → modern `Symbol` index for the all-pairs ticker snapshot,
    /// lazily fetched from `AssetPairs` once and cached for the client lifetime.
    pairs_key_map: tokio::sync::OnceCell<std::collections::HashMap<String, crate::types::Symbol>>,
}

impl MarketNamespace {
    pub(crate) fn new(
        rest: Arc<RestSurface>,
        ws: Arc<WsSurface>,
        dispatch: Arc<DispatchTable>,
    ) -> Self {
        Self {
            rest,
            futures: FuturesMarketNamespace::new(Arc::clone(&ws)),
            ws,
            dispatch,
            pairs_key_map: tokio::sync::OnceCell::new(),
        }
    }

    /// Access the Futures public market-data namespace.
    pub fn futures(&self) -> &FuturesMarketNamespace {
        &self.futures
    }

    fn endpoint(&self, op: Op) -> Result<&'static str, MarketError> {
        Ok(self
            .dispatch
            .resolve(op, Product::Spot, Transport::Rest)?
            .wire_target)
    }
}
