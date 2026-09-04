//! WS v2 streaming payload types for the market namespace + their wire decoders.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::types::Symbol;

/// Raw WS v2 `trade` data-entry. `price`/`qty` stay `Value` — the wire sends
/// number-or-string (qty may be scientific notation).
#[derive(Debug, Deserialize)]
pub(super) struct RawTradeData {
    pub(super) symbol: String,
    pub(super) side: TradeSide,
    pub(super) price: Value,
    pub(super) qty: Value,
    pub(super) ord_type: String,
    pub(super) trade_id: u64,
    pub(super) timestamp: String,
}

/// Raw WS v2 `ohlc` data-entry. Numeric fields stay `Value` (number-or-string);
/// the deprecated per-candle end-time `timestamp` is ignored.
#[derive(Debug, Deserialize)]
pub(super) struct RawOhlcData {
    pub(super) symbol: String,
    pub(super) open: Value,
    pub(super) high: Value,
    pub(super) low: Value,
    pub(super) close: Value,
    pub(super) trades: u32,
    pub(super) volume: Value,
    pub(super) vwap: Value,
    pub(super) interval_begin: String,
    pub(super) interval: u32,
}

/// Strictly parse a string-or-number wire decimal; `Err` carries the bare cause, callers add field context.
pub(super) fn decimal_from_value(v: &Value) -> Result<Decimal, String> {
    match v {
        Value::String(s) => Decimal::from_str(s).map_err(|e| e.to_string()),
        Value::Number(n) => Decimal::from_str(&n.to_string()).map_err(|e| e.to_string()),
        other => Err(format!("unexpected type {:?}", other)),
    }
}

/// Parse a string-or-number wire decimal; `None` (with a warn) on any non-decimal shape.
pub(crate) fn parse_wire_decimal(v: &Value) -> Option<Decimal> {
    // A wire null is an explicit absence, not a parse failure — no warn.
    if v.is_null() {
        return None;
    }
    let parsed = decimal_from_value(v).ok();
    if parsed.is_none() {
        tracing::warn!(target: "kraken_sdk::decode", value = %v, "money value failed Decimal parse — field dropped");
    }
    parsed
}

/// Aggressor side of a trade print.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Deserialize,
    Serialize,
    strum::Display,
    strum::AsRefStr,
    strum::IntoStaticStr,
)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum TradeSide {
    /// Buyer was the aggressor (taker); wire value `buy`.
    Buy,
    /// Seller was the aggressor (taker); wire value `sell`.
    Sell,
    /// Forward-compat catch-all: an unexpected wire value never drops the frame;
    /// serializes back out as `unknown` — the original wire token is not kept.
    #[serde(other)]
    Unknown,
}

impl TradeSide {
    /// Decode the REST `/0/public/Trades` single-char side token; unrecognised → `Unknown`.
    pub(crate) fn from_trades_char(s: &str) -> Self {
        match s {
            "b" => TradeSide::Buy,
            "s" => TradeSide::Sell,
            _ => TradeSide::Unknown,
        }
    }
}

/// One `trade` channel print per Kraken WS v2 (`update` frames only — no snapshot).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct TradeUpdate {
    /// Pair the trade printed on, e.g. `BTC/USD`.
    pub symbol: Symbol,
    /// Aggressor (taker) side of the print.
    pub side: TradeSide,
    /// Execution price in the quote currency.
    pub price: Decimal,
    /// Executed quantity in the base currency (may be tiny, e.g. `5.1e-05` on the wire).
    pub qty: Decimal,
    /// Taker order type passed through verbatim from wire `ord_type`, e.g. `market` or `limit`.
    pub ord_type: String,
    /// Exchange-assigned trade identifier (wire `trade_id`).
    pub trade_id: u64,
    /// Exchange trade time, RFC 3339 text passed through verbatim.
    pub timestamp: String,
}

/// One `ohlc` channel candle per Kraken WS v2 (`snapshot` + `update` frames);
/// the deprecated per-candle end-time is dropped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct OhlcUpdate {
    /// Pair the candle covers, e.g. `BTC/USD`.
    pub symbol: Symbol,
    /// Opening price of the interval, in the quote currency.
    pub open: Decimal,
    /// Highest trade price within the interval.
    pub high: Decimal,
    /// Lowest trade price within the interval.
    pub low: Decimal,
    /// Latest trade price within the interval; moves on `update` frames until the candle closes.
    pub close: Decimal,
    /// Number of trades aggregated into the candle (wire `trades`).
    pub trades: u32,
    /// Base-currency volume traded within the interval.
    pub volume: Decimal,
    /// Volume-weighted average price over the interval.
    pub vwap: Decimal,
    /// Candle-open time, RFC 3339 text (wire `interval_begin`).
    pub interval_begin: String,
    /// Candle width in minutes (wire `interval`).
    pub interval: u32,
}

/// One `ticker` channel update per Kraken WS v2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct TickerUpdate {
    /// Pair the ticker covers, e.g. `BTC/USD`.
    pub symbol: Symbol,
    /// Best bid price, in the quote currency.
    pub bid: Decimal,
    /// Quantity resting at the best bid, in the base currency (wire `bid_qty`).
    pub bid_qty: Decimal,
    /// Best ask price, in the quote currency.
    pub ask: Decimal,
    /// Quantity resting at the best ask, in the base currency (wire `ask_qty`).
    pub ask_qty: Decimal,
    /// Last trade price.
    pub last: Decimal,
    /// Base-currency volume traded over the trailing 24 hours.
    pub volume: Decimal,
    /// Volume-weighted average price over the trailing 24 hours.
    pub vwap: Decimal,
    /// Lowest trade price over the trailing 24 hours.
    pub low: Decimal,
    /// Highest trade price over the trailing 24 hours.
    pub high: Decimal,
    /// Absolute price change over the trailing 24 hours, in the quote currency.
    pub change: Decimal,
    /// Price change over the trailing 24 hours as a percentage (wire `change_pct`).
    pub change_pct: Decimal,
    /// Exchange wall-clock (RFC 3339) from the frame; `""` when absent.
    pub timestamp: String,
}

impl TickerUpdate {
    /// Decode from the raw wire object. Kraken documents the numeric fields as
    /// decimal-strings but the live endpoint emits JSON numbers; both are accepted.
    pub(crate) fn from_wire(raw: RawTickerUpdate) -> Result<Self, TickerDecodeError> {
        let parse_field =
            |v: &serde_json::Value, field: &'static str| -> Result<Decimal, TickerDecodeError> {
                decimal_from_value(v).map_err(|e| TickerDecodeError(format!("{}: {}", field, e)))
            };
        // Strip the inner `SymbolError`'s terminal period — the `TickerDecodeError`
        // template adds its own, and would double it.
        let symbol = Symbol::new(&raw.symbol).map_err(|e| {
            TickerDecodeError(format!("symbol: {}", e.to_string().trim_end_matches('.')))
        })?;
        Ok(Self {
            symbol,
            bid: parse_field(&raw.bid, "bid")?,
            bid_qty: parse_field(&raw.bid_qty, "bid_qty")?,
            ask: parse_field(&raw.ask, "ask")?,
            ask_qty: parse_field(&raw.ask_qty, "ask_qty")?,
            last: parse_field(&raw.last, "last")?,
            volume: parse_field(&raw.volume, "volume")?,
            vwap: parse_field(&raw.vwap, "vwap")?,
            low: parse_field(&raw.low, "low")?,
            high: parse_field(&raw.high, "high")?,
            change: parse_field(&raw.change, "change")?,
            change_pct: parse_field(&raw.change_pct, "change_pct")?,
            timestamp: raw.timestamp,
        })
    }
}

/// Raw wire shape for a `ticker` channel update; numeric fields stay `Value`
/// (string-or-number — see [`TickerUpdate::from_wire`]).
#[derive(Debug, Deserialize)]
pub(crate) struct RawTickerUpdate {
    pub(crate) symbol: String,
    pub(crate) bid: serde_json::Value,
    pub(crate) bid_qty: serde_json::Value,
    pub(crate) ask: serde_json::Value,
    pub(crate) ask_qty: serde_json::Value,
    pub(crate) last: serde_json::Value,
    pub(crate) volume: serde_json::Value,
    pub(crate) vwap: serde_json::Value,
    pub(crate) low: serde_json::Value,
    pub(crate) high: serde_json::Value,
    pub(crate) change: serde_json::Value,
    pub(crate) change_pct: serde_json::Value,
    #[serde(default)]
    pub(crate) timestamp: String,
}

/// Decode failure for a single ticker update.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
#[error("Ticker decode: {0}.")]
pub struct TickerDecodeError(String);

/// Raw wire shape for a single `status` channel entry.
#[derive(Debug, Deserialize)]
pub(super) struct RawSystemStatusUpdate {
    pub(super) system: Option<String>,
    pub(super) version: Option<String>,
    pub(super) api_version: Option<String>,
    // Raw `Value` so a surprise shape never drops the auto-seed `status` frame.
    pub(super) connection_id: Option<Value>,
}

/// One `status` channel entry per Kraken WS v2; auto-seeded on connection open
/// before any subscribe. Channel-wide — no `symbol`.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct SystemStatusUpdate {
    /// Wire `system`: `online` | `maintenance` | `cancel_only` | `post_only`.
    pub system: Option<String>,
    /// WS protocol version (wire `version`, e.g. `"2.0.10"`).
    pub version: Option<String>,
    /// API family (wire `api_version`, e.g. `"v2"`).
    pub api_version: Option<String>,
    /// Connection id (wire `connection_id`) — uint64, requires `u64` not `i64`.
    pub connection_id: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_wire_invalid_symbol_has_single_terminal_period() {
        let num = || serde_json::Value::String("1".to_string());
        let raw = RawTickerUpdate {
            symbol: String::new(),
            bid: num(),
            bid_qty: num(),
            ask: num(),
            ask_qty: num(),
            last: num(),
            volume: num(),
            vwap: num(),
            low: num(),
            high: num(),
            change: num(),
            change_pct: num(),
            timestamp: String::new(),
        };
        let msg = TickerUpdate::from_wire(raw).unwrap_err().to_string();
        assert!(!msg.ends_with(".."), "double terminal period: {msg:?}");
        assert_eq!(msg, "Ticker decode: symbol: Symbol cannot be empty.");
    }

    #[test]
    fn parse_wire_decimal_out_of_range_returns_none() {
        let v = serde_json::Value::String("9".repeat(40));
        assert!(parse_wire_decimal(&v).is_none());
    }
}
