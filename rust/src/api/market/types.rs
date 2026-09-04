//! REST response data types for the market namespace and their `Raw*` wire decoders.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::MarketError;
use super::ws_types::{TradeSide, decimal_from_value};
use serde_with::skip_serializing_none;

/// Snapshot of a trading pair's ticker. `*_today` aggregates cover the current
/// day; `*_24h` a rolling 24-hour window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Ticker {
    /// Best ask price, in quote currency — wire `a[0]`.
    pub ask_price: Decimal,
    /// Whole-lot volume at the best ask — wire `a[1]`.
    pub ask_whole_lot_volume: Decimal,
    /// Volume at the best ask, in base currency — wire `a[2]`.
    pub ask_lot_volume: Decimal,
    /// Best bid price, in quote currency — wire `b[0]`.
    pub bid_price: Decimal,
    /// Whole-lot volume at the best bid — wire `b[1]`.
    pub bid_whole_lot_volume: Decimal,
    /// Volume at the best bid, in base currency — wire `b[2]`.
    pub bid_lot_volume: Decimal,
    /// Price of the most recent trade — wire `c[0]`.
    pub last_price: Decimal,
    /// Volume of the most recent trade, in base currency — wire `c[1]`.
    pub last_volume: Decimal,
    /// Traded volume for the current day, in base currency — wire `v[0]`.
    pub volume_today: Decimal,
    /// Traded volume over the rolling 24h window, in base currency — wire `v[1]`.
    pub volume_24h: Decimal,
    /// Volume-weighted average price for the current day — wire `p[0]`.
    pub vwap_today: Decimal,
    /// Volume-weighted average price over the rolling 24h window — wire `p[1]`.
    pub vwap_24h: Decimal,
    /// Number of trades for the current day — wire `t[0]`.
    pub trades_today: u64,
    /// Number of trades over the rolling 24h window — wire `t[1]`.
    pub trades_24h: u64,
    /// Lowest trade price for the current day — wire `l[0]`.
    pub low_today: Decimal,
    /// Lowest trade price over the rolling 24h window — wire `l[1]`.
    pub low_24h: Decimal,
    /// Highest trade price for the current day — wire `h[0]`.
    pub high_today: Decimal,
    /// Highest trade price over the rolling 24h window — wire `h[1]`.
    pub high_24h: Decimal,
    /// Opening price of the current day — wire `o`.
    pub open: Decimal,
}

impl Ticker {
    pub(super) fn from_raw(raw: RawTicker) -> Result<Self, MarketError> {
        let ask_price = parse_dec(raw.a.first().ok_or_else(missing("a[0]"))?, "ask price")?;
        let ask_whole_lot_volume = parse_dec(
            raw.a.get(1).ok_or_else(missing("a[1]"))?,
            "ask whole lot volume",
        )?;
        let ask_lot_volume =
            parse_dec(raw.a.get(2).ok_or_else(missing("a[2]"))?, "ask lot volume")?;
        let bid_price = parse_dec(raw.b.first().ok_or_else(missing("b[0]"))?, "bid price")?;
        let bid_whole_lot_volume = parse_dec(
            raw.b.get(1).ok_or_else(missing("b[1]"))?,
            "bid whole lot volume",
        )?;
        let bid_lot_volume =
            parse_dec(raw.b.get(2).ok_or_else(missing("b[2]"))?, "bid lot volume")?;
        let last_price = parse_dec(raw.c.first().ok_or_else(missing("c[0]"))?, "last price")?;
        let last_volume = parse_dec(raw.c.get(1).ok_or_else(missing("c[1]"))?, "last volume")?;
        let volume_today = parse_dec(raw.v.first().ok_or_else(missing("v[0]"))?, "volume today")?;
        let volume_24h = parse_dec(raw.v.get(1).ok_or_else(missing("v[1]"))?, "volume 24h")?;
        let vwap_today = parse_dec(raw.p.first().ok_or_else(missing("p[0]"))?, "vwap today")?;
        let vwap_24h = parse_dec(raw.p.get(1).ok_or_else(missing("p[1]"))?, "vwap 24h")?;
        let high_today = parse_dec(raw.h.first().ok_or_else(missing("h[0]"))?, "high today")?;
        let high_24h = parse_dec(raw.h.get(1).ok_or_else(missing("h[1]"))?, "high 24h")?;
        let low_today = parse_dec(raw.l.first().ok_or_else(missing("l[0]"))?, "low today")?;
        let low_24h = parse_dec(raw.l.get(1).ok_or_else(missing("l[1]"))?, "low 24h")?;
        let open = parse_dec(&raw.o, "open")?;
        let trades_today = *raw.t.first().ok_or_else(missing("t[0]"))?;
        let trades_24h = *raw.t.get(1).ok_or_else(missing("t[1]"))?;

        Ok(Self {
            ask_price,
            ask_whole_lot_volume,
            ask_lot_volume,
            bid_price,
            bid_whole_lot_volume,
            bid_lot_volume,
            last_price,
            last_volume,
            volume_today,
            volume_24h,
            vwap_today,
            vwap_24h,
            trades_today,
            trades_24h,
            low_today,
            low_24h,
            high_today,
            high_24h,
            open,
        })
    }
}

fn missing(field: &'static str) -> impl FnOnce() -> MarketError {
    move || MarketError::malformed(format!("missing {}", field))
}

fn parse_dec(s: &str, field: &'static str) -> Result<Decimal, MarketError> {
    s.parse::<Decimal>()
        .map_err(|e| MarketError::malformed(format!("{}: {}", field, e)))
}

#[derive(Debug, Deserialize)]
pub(super) struct RawTicker {
    a: Vec<String>,
    b: Vec<String>,
    c: Vec<String>,
    v: Vec<String>,
    p: Vec<String>,
    t: Vec<u64>,
    l: Vec<String>,
    h: Vec<String>,
    o: String,
}

/// Tickers returned by `MarketNamespace::ticker`, keyed by the pair string
/// Kraken returns — the modern form for Spot (e.g. `"BTC/USD"`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct TickerResult {
    /// Per-pair tickers keyed by the pair string Kraken returned (e.g. `"BTC/USD"`).
    pub tickers: std::collections::HashMap<String, Ticker>,
}

impl TickerResult {
    /// Ticker for `symbol` by its modern string form.
    pub fn get(&self, symbol: &crate::types::Symbol) -> Option<&Ticker> {
        self.tickers.get(symbol.as_str())
    }
}

/// One price level of an order-book snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct OrderBookLevel {
    /// Price of this level, in quote currency.
    pub price: Decimal,
    /// Resting volume at this level, in base currency.
    pub volume: Decimal,
    /// Kraken-emitted Unix timestamp (seconds).
    pub timestamp: u64,
}

/// Snapshot of an order book returned by `MarketNamespace::orderbook`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct OrderBookSnapshot {
    /// Ask levels, ascending price as returned by Kraken.
    pub asks: Vec<OrderBookLevel>,
    /// Bid levels, descending price as returned by Kraken.
    pub bids: Vec<OrderBookLevel>,
}

impl OrderBookSnapshot {
    pub(super) fn from_raw(raw: RawOrderBook) -> Result<Self, MarketError> {
        let asks = raw
            .asks
            .into_iter()
            .map(decode_level)
            .collect::<Result<Vec<_>, _>>()?;
        let bids = raw
            .bids
            .into_iter()
            .map(decode_level)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { asks, bids })
    }
}

fn decode_level(raw: Vec<Value>) -> Result<OrderBookLevel, MarketError> {
    if raw.len() != 3 {
        return Err(MarketError::malformed(format!(
            "orderbook level expected 3 elements, got {}",
            raw.len()
        )));
    }

    let price_str = raw[0]
        .as_str()
        .ok_or_else(|| MarketError::malformed("orderbook level price not string".into()))?;
    let volume_str = raw[1]
        .as_str()
        .ok_or_else(|| MarketError::malformed("orderbook level volume not string".into()))?;
    let timestamp = decode_level_timestamp(&raw[2])?;

    let price = parse_dec(price_str, "orderbook price")?;
    let volume = parse_dec(volume_str, "orderbook volume")?;

    Ok(OrderBookLevel {
        price,
        volume,
        timestamp,
    })
}

/// Tolerant level-timestamp decode — integer, float (truncated), or numeric string.
fn decode_level_timestamp(v: &Value) -> Result<u64, MarketError> {
    if let Some(n) = v.as_u64() {
        return Ok(n);
    }
    let as_secs = v
        .as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()));
    match as_secs {
        Some(f) if f >= 0.0 && f <= u64::MAX as f64 => Ok(f as u64),
        _ => Err(MarketError::malformed(format!(
            "orderbook level timestamp not a number: {}",
            v
        ))),
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct RawOrderBook {
    asks: Vec<Vec<Value>>,
    bids: Vec<Vec<Value>>,
}

/// Paginated recent-trades response. `last` is Kraken's nanosecond-resolution
/// pagination cursor (decimal string); pass it back as `since` on the next call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct TradesResult {
    /// Pair these trades belong to.
    pub pair: crate::types::Symbol,
    /// Closed trades in the order Kraken returned them (oldest first).
    pub trades: Vec<RecentTrade>,
    /// Nanosecond pagination cursor — decimal string from the wire.
    pub last: String,
}

/// A single closed trade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct RecentTrade {
    /// Execution price, in quote currency.
    pub price: Decimal,
    /// Executed volume, in base currency.
    pub volume: Decimal,
    /// Fractional Unix seconds, kept verbatim as the wire string to avoid `f64`
    /// precision loss.
    pub time: String,
    /// Taker side, decoded from the wire `"b"` (buy) / `"s"` (sell) char.
    pub side: TradeSide,
    /// Raw wire order-type token (`"l"` limit / `"m"` market), kept verbatim so an
    /// unrecognised type never drops the trade.
    pub order_type: String,
    /// Free-text misc field (often empty).
    pub misc: String,
    /// Kraken-assigned numeric trade identifier.
    pub trade_id: u64,
}

impl RecentTrade {
    pub(super) fn from_raw(raw: RawTrade) -> Result<Self, MarketError> {
        let price = raw
            .0
            .parse::<Decimal>()
            .map_err(|e| MarketError::malformed(format!("trade price: {}", e)))?;
        let volume = raw
            .1
            .parse::<Decimal>()
            .map_err(|e| MarketError::malformed(format!("trade volume: {}", e)))?;
        let time = match raw.2 {
            Value::Number(n) => n.to_string(),
            Value::String(s) => s,
            other => {
                return Err(MarketError::malformed(format!(
                    "trade time: expected a JSON number, got {:?}",
                    other
                )));
            }
        };
        let side = TradeSide::from_trades_char(&raw.3);

        Ok(Self {
            price,
            volume,
            time,
            side,
            order_type: raw.4,
            misc: raw.5,
            trade_id: raw.6,
        })
    }
}

/// Raw trade tuple: `[price, volume, time, side, type, misc, trade_id]`.
#[derive(Debug, Deserialize)]
pub(super) struct RawTrade(String, String, Value, String, String, String, u64);

/// Exchange system-status snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct SystemStatus {
    /// `online`, `maintenance`, `cancel_only`, `post_only`.
    pub status: String,
    /// ISO 8601 timestamp from the exchange (e.g., `2026-05-24T12:34:56Z`).
    pub timestamp: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct RawSystemStatus {
    pub(super) status: String,
    pub(super) timestamp: String,
}

/// Exchange server time returned by `MarketNamespace::server_time`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct ServerTime {
    /// Kraken-emitted Unix timestamp (seconds).
    pub unixtime: u64,
    /// The same instant as an RFC 1123 string, verbatim from the wire.
    pub rfc1123: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct RawServerTime {
    pub(super) unixtime: u64,
    pub(super) rfc1123: String,
}

/// Asset metadata keyed by the modern asset code (`BTC`, not `XXBT`);
/// `AssetMeta.altname` retains the legacy short form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Assets {
    /// Per-asset metadata keyed by the modern asset code.
    pub assets: std::collections::HashMap<crate::types::AssetCode, AssetMeta>,
}

/// Metadata for a single asset from `MarketNamespace::assets`.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct AssetMeta {
    /// Kraken asset class (e.g. `"currency"`).
    pub aclass: String,
    /// Kraken's alternate short name, which may be a legacy form (e.g. `XBT`).
    pub altname: String,
    /// Decimal places Kraken records for amounts of this asset.
    pub decimals: u32,
    /// Decimal places Kraken uses when displaying amounts of this asset.
    pub display_decimals: u32,
    /// Asset status (e.g. `"enabled"`); empty string when the wire omits the field.
    pub status: String,
    /// Optional — older asset rows pre-date this field.
    pub collateral_value: Option<Decimal>,
}

impl AssetMeta {
    pub(super) fn from_raw(r: RawAssetMeta) -> Result<Self, MarketError> {
        // collateral_value is string-or-number on the wire.
        let collateral_value = match r.collateral_value {
            None | Some(Value::Null) => None,
            Some(v) => Some(
                decimal_from_value(&v)
                    .map_err(|e| MarketError::malformed(format!("collateral_value: {}", e)))?,
            ),
        };
        Ok(Self {
            aclass: r.aclass,
            altname: r.altname,
            decimals: r.decimals,
            display_decimals: r.display_decimals,
            status: r.status,
            collateral_value,
        })
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct RawAssetMeta {
    aclass: String,
    altname: String,
    decimals: u32,
    display_decimals: u32,
    #[serde(default)]
    status: String,
    collateral_value: Option<Value>,
}

/// Tradeable-pair metadata. Keys are modern pair forms (`BTC/USD`); the
/// legacy-code fields (`altname`, `wsname`, `base`, `quote`) are stripped at decode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct AssetPairs {
    /// Per-pair metadata keyed by the modern pair form (e.g. `"BTC/USD"`).
    pub pairs: std::collections::HashMap<String, AssetPairMeta>,
}

/// Metadata for a single tradeable pair from `MarketNamespace::pairs`.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct AssetPairMeta {
    /// Asset class of the base asset (e.g. `"currency"`).
    pub aclass_base: String,
    /// Asset class of the quote asset (e.g. `"currency"`).
    pub aclass_quote: String,
    /// Minimum price increment, in quote currency.
    pub tick_size: Decimal,
    /// Decimal places of price precision for this pair.
    pub pair_decimals: u32,
    /// Decimal places of precision for the order cost (price × volume).
    pub cost_decimals: u32,
    /// Decimal places of volume precision for this pair.
    pub lot_decimals: u32,
    /// Minimum order volume, in base currency.
    pub ordermin: Decimal,
    /// Minimum order cost, in quote currency.
    pub costmin: Decimal,
    /// Tradeability status (e.g. `"online"`); `None` when Kraken omits the field (many pairs do).
    pub status: Option<String>,
    /// Taker fee schedule by 30-day volume tier.
    pub fees: Vec<FeeTier>,
    /// Maker fee schedule by 30-day volume tier; empty when Kraken omits the field.
    pub fees_maker: Vec<FeeTier>,
    /// Margin-call level in percent; `None` when Kraken omits the field.
    pub margin_call: Option<u32>,
    /// Forced-liquidation margin level in percent; `None` when Kraken omits the field.
    pub margin_stop: Option<u32>,
    /// Buy-side margin leverage tiers; empty when not marginable / omitted.
    pub leverage_buy: Vec<u32>,
    /// Sell-side margin leverage tiers. Empty when not marginable / omitted.
    pub leverage_sell: Vec<u32>,
    /// Multiplier applied to the volume to derive the lot size; `None` when omitted.
    pub lot_multiplier: Option<u32>,
    /// Asset code whose 30-day rolling volume sets this pair's fee tier,
    /// normalised to the modern code; `None` when omitted.
    pub fee_volume_currency: Option<crate::types::AssetCode>,
    /// Maximum aggregate long position size in lots; `None` for non-marginable pairs.
    pub long_position_limit: Option<u64>,
    /// Maximum aggregate short position size in lots; `None` for non-marginable pairs.
    pub short_position_limit: Option<u64>,
    /// Venue this pair executes against (e.g. `"international"`); `None` when omitted.
    pub execution_venue: Option<String>,
}

/// One fee-schedule tier: the fee percentage applied at and above a 30-day
/// volume threshold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FeeTier {
    /// 30-day volume threshold (in `fee_volume_currency`) at which this tier applies.
    pub volume: Decimal,
    /// Fee percentage charged at this tier (e.g. `0.26` = 0.26%).
    pub fee_pct: Decimal,
}

impl AssetPairMeta {
    pub(super) fn from_raw(r: RawAssetPair) -> Result<Self, MarketError> {
        let parse_tier = |v: Vec<Value>| -> Result<FeeTier, MarketError> {
            if v.len() != 2 {
                return Err(MarketError::malformed(format!(
                    "fee tier expected [volume, fee_pct], got {} elements",
                    v.len()
                )));
            }
            let parse_elem = |v: &Value, what: &str| -> Result<Decimal, MarketError> {
                decimal_from_value(v).map_err(|e| {
                    MarketError::malformed(match v {
                        Value::String(_) => format!("fee tier {}: {}", what, e),
                        Value::Number(_) => format!("fee tier {} parse", what),
                        _ => format!("fee tier {} type", what),
                    })
                })
            };
            Ok(FeeTier {
                volume: parse_elem(&v[0], "volume")?,
                fee_pct: parse_elem(&v[1], "fee_pct")?,
            })
        };

        let fees = r
            .fees
            .into_iter()
            .map(parse_tier)
            .collect::<Result<Vec<_>, _>>()?;
        let fees_maker = r
            .fees_maker
            .unwrap_or_default()
            .into_iter()
            .map(parse_tier)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            aclass_base: r.aclass_base,
            aclass_quote: r.aclass_quote,
            tick_size: parse_dec(&r.tick_size, "tick_size")?,
            pair_decimals: r.pair_decimals,
            cost_decimals: r.cost_decimals,
            lot_decimals: r.lot_decimals,
            ordermin: parse_dec(&r.ordermin, "ordermin")?,
            costmin: parse_dec(&r.costmin, "costmin")?,
            status: r.status,
            fees,
            fees_maker,
            margin_call: r.margin_call,
            margin_stop: r.margin_stop,
            leverage_buy: r.leverage_buy,
            leverage_sell: r.leverage_sell,
            lot_multiplier: r.lot_multiplier,
            fee_volume_currency: r
                .fee_volume_currency
                .map(|s| crate::types::AssetCode::from_wire(&s)),
            long_position_limit: r.long_position_limit,
            short_position_limit: r.short_position_limit,
            execution_venue: r.execution_venue,
        })
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct RawAssetPair {
    aclass_base: String,
    aclass_quote: String,
    // Legacy X/Z codes + slashless wire keys are normalised.
    #[serde(default)]
    base: String,
    #[serde(default)]
    quote: String,
    tick_size: String,
    pair_decimals: u32,
    cost_decimals: u32,
    lot_decimals: u32,
    ordermin: String,
    costmin: String,
    // Documented always-present but omitted on many live pairs.
    status: Option<String>,
    fees: Vec<Vec<Value>>,
    fees_maker: Option<Vec<Vec<Value>>>,
    margin_call: Option<u32>,
    margin_stop: Option<u32>,
    #[serde(default)]
    leverage_buy: Vec<u32>,
    #[serde(default)]
    leverage_sell: Vec<u32>,
    // May be omitted despite docs.
    lot_multiplier: Option<u32>,
    // Legacy X/Z code on the wire.
    fee_volume_currency: Option<String>,
    // Absent for non-marginable pairs; can exceed u32.
    long_position_limit: Option<u64>,
    short_position_limit: Option<u64>,
    execution_venue: Option<String>,
}

impl RawAssetPair {
    /// Map key to expose: the wire key verbatim when already slashed (or base/quote
    /// absent), else `{base}/{quote}` rebuilt from legacy codes for a slashless new
    /// listing (e.g. `RENDERUSD`).
    pub(super) fn modern_pair_key(&self, wire_key: String) -> String {
        if wire_key.contains('/') || self.base.is_empty() || self.quote.is_empty() {
            return wire_key;
        }
        format!(
            "{}/{}",
            crate::types::AssetCode::from_wire(&self.base),
            crate::types::AssetCode::from_wire(&self.quote)
        )
    }
}

// `OhlcInterval` lives in `crate::types`; re-exported so `crate::api::market::OhlcInterval` stays.
#[doc(hidden)]
pub use crate::types::OhlcInterval;

/// Paginated OHLC response from `MarketNamespace::ohlc`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct OhlcResult {
    /// Pair these candles belong to.
    pub pair: crate::types::Symbol,
    /// Candles in the order Kraken returned them (ascending time).
    pub candles: Vec<OhlcCandle>,
    /// Pagination cursor — Kraken Unix timestamp (seconds).
    pub last: u64,
}

/// One OHLC candle for the requested interval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct OhlcCandle {
    /// Candle interval start — Kraken-emitted Unix timestamp (seconds).
    pub time: u64,
    /// First trade price of the interval.
    pub open: Decimal,
    /// Highest trade price of the interval.
    pub high: Decimal,
    /// Lowest trade price of the interval.
    pub low: Decimal,
    /// Last trade price of the interval.
    pub close: Decimal,
    /// Volume-weighted average price over the interval.
    pub vwap: Decimal,
    /// Traded volume over the interval, in base currency.
    pub volume: Decimal,
    /// Number of trades in the interval.
    pub count: u32,
}

impl OhlcCandle {
    pub(super) fn from_raw(raw: RawCandle) -> Result<Self, MarketError> {
        Ok(Self {
            time: raw.0,
            open: parse_dec(&raw.1, "ohlc open")?,
            high: parse_dec(&raw.2, "ohlc high")?,
            low: parse_dec(&raw.3, "ohlc low")?,
            close: parse_dec(&raw.4, "ohlc close")?,
            vwap: parse_dec(&raw.5, "ohlc vwap")?,
            volume: parse_dec(&raw.6, "ohlc volume")?,
            count: raw.7,
        })
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct RawCandle(u64, String, String, String, String, String, String, u32);

/// Paginated recent-spreads response. `last` is Kraken's Unix-seconds cursor,
/// stringified for cross-binding consistency (wire `result.last` is an integer).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct SpreadsResult {
    /// Pair these spreads belong to.
    pub pair: crate::types::Symbol,
    /// Spread snapshots in the order Kraken returned them.
    pub spreads: Vec<SpreadEntry>,
    /// Unix-seconds cursor — stringified integer.
    pub last: String,
}

/// A single best-bid/best-ask spread snapshot entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct SpreadEntry {
    /// Snapshot time — Kraken-emitted Unix timestamp (seconds).
    pub time: u64,
    /// Best bid price at the snapshot, in quote currency.
    pub bid: Decimal,
    /// Best ask price at the snapshot, in quote currency.
    pub ask: Decimal,
}

impl SpreadEntry {
    pub(super) fn from_raw(raw: RawSpread) -> Result<Self, MarketError> {
        Ok(Self {
            time: raw.0,
            bid: parse_dec(&raw.1, "spread bid")?,
            ask: parse_dec(&raw.2, "spread ask")?,
        })
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct RawSpread(u64, String, String);
