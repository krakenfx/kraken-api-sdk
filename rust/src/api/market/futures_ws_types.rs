//! Unified Futures public payload types and their wire decoders.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::api::market::ws_types::TradeSide;
use crate::api::ws_decode::{
    extract_data, is_snapshot_frame, lenient_rows, lenient_vec_for, warn_decode_drop,
};
use crate::book::{BookLevel, FuturesBookDelta, FuturesOrderBookUpdate};
use crate::types::{FuturesOhlcTickType, futures_product};

/// One unsolicited Unified Futures `status` update.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesSystemStatusUpdate {
    /// Unified API protocol version.
    pub api_version: String,
    /// Venue-assigned connection identifier when supplied.
    pub connection_id: Option<u64>,
    /// Futures venue availability token.
    pub futures_exchange: String,
    /// Spot venue availability token as reported by Unified.
    pub spot_exchange: String,
    /// Unified service version.
    pub version: String,
    /// Planned maintenance windows; empty when none is announced.
    pub upcoming_maintenance: Vec<FuturesMaintenanceWindow>,
    /// Active incidents; empty when none is open.
    pub emergency: Vec<FuturesEmergencyIncident>,
}

/// One announced maintenance window. Enum-like fields are passed through as the venue
/// sends them (`phase`, `order_submission`, `recommended_action` and the service names).
#[serde_with::apply(
    Option<u64> => #[serde(deserialize_with = "crate::api::ws_decode::lenient_opt")],
    Option<String> => #[serde(deserialize_with = "crate::api::ws_decode::lenient_opt")],
    Vec<String> => #[serde(deserialize_with = "maintenance_affected_services")],
)]
#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct FuturesMaintenanceWindow {
    /// Venue event identifier.
    pub event_id: Option<u64>,
    /// Human-readable title.
    pub title: Option<String>,
    /// RFC 3339 UTC expected start.
    pub expected_start_utc: Option<String>,
    /// RFC 3339 UTC expected end; `None` while not yet known.
    pub expected_end_utc: Option<String>,
    /// Seconds until the start, clamped to 0 after it.
    pub time_to_start_s: Option<u64>,
    /// Announcement phase, for example `announced` or `imminent_5m`.
    pub phase: Option<String>,
    /// Affected services, for example `futures_ws` or `futures_trading`.
    pub affected_services: Vec<String>,
    /// Whether order submission is `allowed`, `discouraged` or `blocked`.
    pub order_submission: Option<String>,
    /// Recommended caller action, for example `cancel_open_orders`.
    pub recommended_action: Option<String>,
    /// RFC 3339 UTC time to cancel open orders by, when applicable.
    pub cancel_before_utc: Option<String>,
    /// Link to the venue's notice; empty when none was provided.
    pub source_url: Option<String>,
}

/// One active incident.
#[serde_with::apply(
    Option<u64> => #[serde(deserialize_with = "crate::api::ws_decode::lenient_opt")],
    Option<String> => #[serde(deserialize_with = "crate::api::ws_decode::lenient_opt")],
    Vec<String> => #[serde(deserialize_with = "incident_affected_services")],
    Vec<FuturesIncidentStep> => #[serde(deserialize_with = "incident_next_steps")],
)]
#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct FuturesEmergencyIncident {
    /// Venue event identifier.
    pub event_id: Option<u64>,
    /// Human-readable title.
    pub title: Option<String>,
    /// Incident state: `investigating`, `identified` or `monitoring`.
    pub incident_status: Option<String>,
    /// Impact: `none`, `minor`, `major` or `critical`.
    pub impact: Option<String>,
    /// Affected services, for example `futures_ws` or `futures_trading`.
    pub affected_services: Vec<String>,
    /// RFC 3339 UTC start of the incident.
    pub started_at_utc: Option<String>,
    /// Expected next steps.
    pub next_steps: Vec<FuturesIncidentStep>,
    /// Link to the venue's notice; empty when none was provided.
    pub source_url: Option<String>,
}

/// One expected next step of an incident.
#[serde_with::apply(
    Option<String> => #[serde(deserialize_with = "crate::api::ws_decode::lenient_opt")],
    Vec<String> => #[serde(deserialize_with = "incident_step_applies_to")],
)]
#[skip_serializing_none]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct FuturesIncidentStep {
    /// Services the step applies to.
    pub applies_to: Vec<String>,
    /// Step kind (wire `type`), for example `expected_restart` or `expected_cancel_only`.
    #[serde(rename(deserialize = "type"))]
    pub step_type: Option<String>,
    /// RFC 3339 UTC time the step is expected.
    pub expected_at_utc: Option<String>,
}

/// Unified Futures `instrument` snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesInstrumentSnapshot {
    /// Available Futures collateral assets.
    pub assets: Vec<FuturesInstrumentAsset>,
    /// Available Futures instruments.
    pub pairs: Vec<FuturesInstrumentPair>,
}

/// One asset in a Unified Futures `instrument` snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesInstrumentAsset {
    /// Venue asset identifier.
    pub id: String,
    /// Venue asset availability token.
    pub status: String,
    /// Maximum ledger precision.
    pub precision: u32,
    /// Recommended display precision.
    pub precision_display: u32,
    /// Whether the asset may be borrowed.
    pub borrowable: bool,
    /// Margin collateral valuation.
    pub collateral_value: Decimal,
    /// Margin rate.
    pub margin_rate: Decimal,
}

/// One pair in a Unified Futures `instrument` snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesInstrumentPair {
    /// Native Futures wire symbol.
    pub symbol: String,
    /// Instrument base asset.
    pub base: String,
    /// Instrument quote asset.
    pub quote: String,
    /// Venue instrument availability token.
    pub status: String,
    /// Price precision.
    pub price_precision: u32,
    /// Quantity precision.
    pub qty_precision: u32,
    /// Minimum order quantity.
    pub qty_min: Decimal,
    /// Permitted quantity increment.
    pub qty_increment: Decimal,
    /// Permitted price increment.
    pub price_increment: Decimal,
}

/// Unified Futures `ticker` update, typed by the native symbol's product family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FuturesTickerUpdate {
    /// Perpetual inverse (`PI_`) or perpetual fixed (`PF_`) contract.
    Perpetual(FuturesPerpetualTickerUpdate),
    /// Fixed-expiry (`FF_`) contract.
    ExpiringFixed(FuturesExpiringTickerUpdate),
    /// Inverse-expiry (`FI_`) contract.
    ExpiringInverse(FuturesExpiringTickerUpdate),
}

impl FuturesTickerUpdate {
    /// Native Futures wire symbol.
    pub fn symbol(&self) -> &str {
        match self {
            Self::Perpetual(update) => &update.symbol,
            Self::ExpiringFixed(update) | Self::ExpiringInverse(update) => &update.symbol,
        }
    }
}

/// Full Unified ticker update for a perpetual (`PI_` or `PF_`) contract.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesPerpetualTickerUpdate {
    /// Native Futures wire symbol.
    pub symbol: String,
    /// Exchange timestamp in RFC3339 form.
    pub timestamp: String,
    /// Next funding-rate timestamp in RFC3339 form.
    pub next_funding_rate_time: String,
    /// Whether the instrument is suspended.
    pub suspended: bool,
    /// Number of trades in the ticker window.
    pub trades: u64,
    /// Best ask price.
    pub ask: Option<Decimal>,
    /// Quantity at the best ask.
    pub ask_qty: Option<Decimal>,
    /// Best bid price.
    pub bid: Option<Decimal>,
    /// Quantity at the best bid.
    pub bid_qty: Option<Decimal>,
    /// Absolute price change over the ticker window.
    pub change: Option<Decimal>,
    /// Percentage price change over the ticker window.
    pub change_pct: Option<Decimal>,
    /// Current funding rate.
    pub funding_rate: Decimal,
    /// Predicted funding rate.
    pub funding_rate_prediction: Decimal,
    /// Highest price in the ticker window.
    pub high: Option<Decimal>,
    /// Underlying index price.
    pub index: Decimal,
    /// Last traded price.
    pub last: Option<Decimal>,
    /// Lowest price in the ticker window.
    pub low: Option<Decimal>,
    /// Mark price.
    pub mark: Decimal,
    /// Opening price for the ticker window.
    pub open: Option<Decimal>,
    /// Open interest.
    pub open_interest: Decimal,
    /// Mark-to-index premium.
    pub premium: Decimal,
    /// Relative funding rate.
    pub relative_funding_rate: Decimal,
    /// Predicted relative funding rate.
    pub relative_funding_rate_prediction: Decimal,
    /// Base trading volume.
    pub volume: Decimal,
    /// Quote trading volume.
    pub volume_quote: Decimal,
    /// Volume-weighted average price.
    pub vwap: Option<Decimal>,
}

/// Unified ticker update for a fixed-expiry (`FF_`) or inverse-expiry (`FI_`) contract.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesExpiringTickerUpdate {
    /// Native Futures wire symbol.
    pub symbol: String,
    /// Exchange timestamp in RFC3339 form.
    pub timestamp: String,
    /// Whether the instrument is suspended.
    pub suspended: bool,
    /// Number of trades in the ticker window.
    pub trades: u64,
    /// Best ask price.
    pub ask: Option<Decimal>,
    /// Quantity at the best ask.
    pub ask_qty: Option<Decimal>,
    /// Best bid price.
    pub bid: Option<Decimal>,
    /// Quantity at the best bid.
    pub bid_qty: Option<Decimal>,
    /// Absolute price change over the ticker window.
    pub change: Option<Decimal>,
    /// Percentage price change over the ticker window.
    pub change_pct: Option<Decimal>,
    /// Highest price in the ticker window.
    pub high: Option<Decimal>,
    /// Underlying index price.
    pub index: Decimal,
    /// Last traded price.
    pub last: Option<Decimal>,
    /// Lowest price in the ticker window.
    pub low: Option<Decimal>,
    /// Mark price.
    pub mark: Decimal,
    /// Opening price for the ticker window.
    pub open: Option<Decimal>,
    /// Open interest.
    pub open_interest: Decimal,
    /// Mark-to-index premium.
    pub premium: Decimal,
    /// Base trading volume.
    pub volume: Decimal,
    /// Quote trading volume.
    pub volume_quote: Decimal,
    /// Volume-weighted average price.
    pub vwap: Option<Decimal>,
}

/// One Unified Futures `trade` update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesTradeUpdate {
    /// Native Futures wire symbol.
    pub symbol: String,
    /// Aggressor side.
    pub side: TradeSide,
    /// Execution price.
    pub price: Decimal,
    /// Executed quantity.
    pub qty: Decimal,
    /// Venue order type token.
    pub ord_type: String,
    /// Venue trade id, sequential per market. `None` when the venue omits it or sends a value that
    /// is not an unsigned integer.
    pub trade_id: Option<u64>,
    /// Exchange timestamp in RFC3339 form.
    pub timestamp: String,
    /// Whether the print came from a snapshot frame rather than a live update.
    pub is_snapshot: bool,
}

/// One Unified Futures `ohlc` candle update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesOhlcUpdate {
    /// Native Futures wire symbol.
    pub symbol: String,
    /// Candle open price.
    pub open: Decimal,
    /// Candle high price.
    pub high: Decimal,
    /// Candle low price.
    pub low: Decimal,
    /// Candle close price.
    pub close: Decimal,
    /// Number of trades in the candle.
    pub trades: u64,
    /// Traded volume in the candle.
    pub volume: Decimal,
    /// Volume-weighted average price.
    pub vwap: Decimal,
    /// Candle start in RFC3339 form.
    pub interval_begin: String,
    /// Candle interval in minutes.
    pub interval: u32,
    /// Price source the candle is built from.
    pub tick_type: FuturesOhlcTickType,
}

/// Decode one raw Unified book entry without maintained state or CRC validation.
pub(crate) fn decode_book_raw(value: Value) -> Option<FuturesBookDelta> {
    let is_snapshot = is_snapshot_frame(&value);
    let parsed = crate::book::parse_book_frame_owned(extract_data(value)?)?;
    Some(FuturesBookDelta {
        symbol: parsed.symbol,
        bids: parsed.bids,
        asks: parsed.asks,
        checksum: parsed.checksum,
        is_snapshot,
        exchange_timestamp: parsed.timestamp,
    })
}

/// Decode a frame only when the maintained fast path is not active.
pub(crate) fn decode_book(value: Value) -> Option<FuturesOrderBookUpdate> {
    let parsed = crate::book::parse_book_frame_owned(extract_data(value)?)?;
    Some(FuturesOrderBookUpdate {
        symbol: parsed.symbol,
        bids: parsed.bids.iter().map(BookLevel::from).collect(),
        asks: parsed.asks.iter().map(BookLevel::from).collect(),
        checksum: parsed.checksum,
        exchange_timestamp: parsed.timestamp,
    })
}

fn maintenance_affected_services<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<String>, D::Error> {
    lenient_vec_for("maintenance.affected_services", d)
}

fn incident_affected_services<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<String>, D::Error> {
    lenient_vec_for("incident.affected_services", d)
}

fn incident_next_steps<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<FuturesIncidentStep>, D::Error> {
    lenient_vec_for("incident.next_steps", d)
}

fn incident_step_applies_to<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<String>, D::Error> {
    lenient_vec_for("incident_step.applies_to", d)
}

/// Run `decode`, logging a throttled drop when it yields `None`.
fn decode_or_warn<T>(
    channel: &'static str,
    reason: &'static str,
    decode: impl FnOnce() -> Option<T>,
) -> Option<T> {
    let decoded = decode();
    if decoded.is_none() {
        warn_decode_drop(channel, reason);
    }
    decoded
}

pub(crate) fn decode_system_status(value: Value) -> Option<FuturesSystemStatusUpdate> {
    let data = extract_data(value)?;
    #[derive(Deserialize)]
    struct RawStatus {
        api_version: String,
        connection_id: Option<u64>,
        futures_exchange: String,
        spot_exchange: String,
        version: String,
        upcoming_maintenance: Option<Value>,
        emergency: Option<Value>,
    }
    let raw = serde_json::from_value::<RawStatus>(data).ok()?;
    Some(FuturesSystemStatusUpdate {
        api_version: raw.api_version,
        connection_id: raw.connection_id,
        futures_exchange: raw.futures_exchange,
        spot_exchange: raw.spot_exchange,
        version: raw.version,
        upcoming_maintenance: lenient_rows("status.upcoming_maintenance", raw.upcoming_maintenance),
        emergency: lenient_rows("status.emergency", raw.emergency),
    })
}

pub(crate) fn decode_instrument_snapshot(value: Value) -> Option<FuturesInstrumentSnapshot> {
    let data = extract_data(value)?;
    let object = data.as_object()?;
    let assets = object.get("assets").and_then(Value::as_array)?;
    let pairs = object.get("pairs").and_then(Value::as_array)?;

    let decoded_assets: Vec<_> = assets.iter().filter_map(decode_instrument_asset).collect();
    let decoded_pairs: Vec<_> = pairs.iter().filter_map(decode_instrument_pair).collect();
    if (!assets.is_empty() && decoded_assets.is_empty())
        || (!pairs.is_empty() && decoded_pairs.is_empty())
    {
        return None;
    }

    Some(FuturesInstrumentSnapshot {
        assets: decoded_assets,
        pairs: decoded_pairs,
    })
}

fn decode_instrument_asset(value: &Value) -> Option<FuturesInstrumentAsset> {
    decode_or_warn("instrument", "malformed_record", || {
        let object = value.as_object()?;
        Some(FuturesInstrumentAsset {
            id: required_string(object, "id")?,
            status: required_string(object, "status")?,
            precision: required_u32(object, "precision")?,
            precision_display: required_u32(object, "precision_display")?,
            borrowable: required_bool(object, "borrowable")?,
            collateral_value: required_decimal(object, "collateral_value")?,
            margin_rate: required_decimal(object, "margin_rate")?,
        })
    })
}

fn decode_instrument_pair(value: &Value) -> Option<FuturesInstrumentPair> {
    decode_or_warn("instrument", "malformed_record", || {
        let object = value.as_object()?;
        Some(FuturesInstrumentPair {
            symbol: required_string(object, "symbol")?,
            base: required_string(object, "base")?,
            quote: required_string(object, "quote")?,
            status: required_string(object, "status")?,
            price_precision: required_u32(object, "price_precision")?,
            qty_precision: required_u32(object, "qty_precision")?,
            qty_min: required_decimal(object, "qty_min")?,
            qty_increment: required_decimal(object, "qty_increment")?,
            price_increment: required_decimal(object, "price_increment")?,
        })
    })
}

fn required_string(object: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    object.get(key)?.as_str().map(ToOwned::to_owned)
}

fn required_u32(object: &serde_json::Map<String, Value>, key: &str) -> Option<u32> {
    object
        .get(key)?
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
}

fn required_bool(object: &serde_json::Map<String, Value>, key: &str) -> Option<bool> {
    object.get(key)?.as_bool()
}

fn required_u64(object: &serde_json::Map<String, Value>, key: &str) -> Option<u64> {
    object.get(key)?.as_u64()
}

fn required_decimal(object: &serde_json::Map<String, Value>, key: &str) -> Option<Decimal> {
    crate::api::market::ws_types::parse_wire_decimal(object.get(key)?)
}

fn optional_decimal(object: &serde_json::Map<String, Value>, key: &str) -> Option<Option<Decimal>> {
    match object.get(key) {
        None | Some(Value::Null) => Some(None),
        Some(value) => crate::api::market::ws_types::parse_wire_decimal(value).map(Some),
    }
}

pub(crate) fn decode_ticker(value: Value) -> Option<FuturesTickerUpdate> {
    let data = extract_data(value)?;
    let object = data.as_object()?;
    match futures_product(object.get("symbol")?.as_str()?)? {
        "PI" | "PF" => decode_perpetual_ticker(object).map(FuturesTickerUpdate::Perpetual),
        "FF" => decode_expiring_ticker(object).map(FuturesTickerUpdate::ExpiringFixed),
        "FI" => decode_expiring_ticker(object).map(FuturesTickerUpdate::ExpiringInverse),
        _ => None,
    }
}

fn decode_perpetual_ticker(
    object: &serde_json::Map<String, Value>,
) -> Option<FuturesPerpetualTickerUpdate> {
    Some(FuturesPerpetualTickerUpdate {
        symbol: required_string(object, "symbol")?,
        timestamp: required_string(object, "timestamp")?,
        next_funding_rate_time: required_string(object, "next_funding_rate_time")?,
        suspended: required_bool(object, "suspended")?,
        trades: required_u64(object, "trades")?,
        ask: optional_decimal(object, "ask")?,
        ask_qty: optional_decimal(object, "ask_qty")?,
        bid: optional_decimal(object, "bid")?,
        bid_qty: optional_decimal(object, "bid_qty")?,
        change: optional_decimal(object, "change")?,
        change_pct: optional_decimal(object, "change_pct")?,
        funding_rate: required_decimal(object, "funding_rate")?,
        funding_rate_prediction: required_decimal(object, "funding_rate_prediction")?,
        high: optional_decimal(object, "high")?,
        index: required_decimal(object, "index")?,
        last: optional_decimal(object, "last")?,
        low: optional_decimal(object, "low")?,
        mark: required_decimal(object, "mark")?,
        open: optional_decimal(object, "open")?,
        open_interest: required_decimal(object, "open_interest")?,
        premium: required_decimal(object, "premium")?,
        relative_funding_rate: required_decimal(object, "relative_funding_rate")?,
        relative_funding_rate_prediction: required_decimal(
            object,
            "relative_funding_rate_prediction",
        )?,
        volume: required_decimal(object, "volume")?,
        volume_quote: required_decimal(object, "volume_quote")?,
        vwap: optional_decimal(object, "vwap")?,
    })
}

fn decode_expiring_ticker(
    object: &serde_json::Map<String, Value>,
) -> Option<FuturesExpiringTickerUpdate> {
    absent_fields(
        object,
        &[
            "next_funding_rate_time",
            "funding_rate",
            "funding_rate_prediction",
            "relative_funding_rate",
            "relative_funding_rate_prediction",
        ],
    )?;
    Some(FuturesExpiringTickerUpdate {
        symbol: required_string(object, "symbol")?,
        timestamp: required_string(object, "timestamp")?,
        suspended: required_bool(object, "suspended")?,
        trades: required_u64(object, "trades")?,
        ask: optional_decimal(object, "ask")?,
        ask_qty: optional_decimal(object, "ask_qty")?,
        bid: optional_decimal(object, "bid")?,
        bid_qty: optional_decimal(object, "bid_qty")?,
        change: optional_decimal(object, "change")?,
        change_pct: optional_decimal(object, "change_pct")?,
        high: optional_decimal(object, "high")?,
        index: required_decimal(object, "index")?,
        last: optional_decimal(object, "last")?,
        low: optional_decimal(object, "low")?,
        mark: required_decimal(object, "mark")?,
        open: optional_decimal(object, "open")?,
        open_interest: required_decimal(object, "open_interest")?,
        premium: required_decimal(object, "premium")?,
        volume: required_decimal(object, "volume")?,
        volume_quote: required_decimal(object, "volume_quote")?,
        vwap: optional_decimal(object, "vwap")?,
    })
}

fn absent_fields(object: &serde_json::Map<String, Value>, fields: &[&str]) -> Option<()> {
    fields
        .iter()
        .all(|field| !object.contains_key(*field))
        .then_some(())
}

pub(crate) fn decode_trade(value: Value) -> Option<FuturesTradeUpdate> {
    let is_snapshot = is_snapshot_frame(&value);
    let data = extract_data(value)?;
    let object = data.as_object()?;
    Some(FuturesTradeUpdate {
        symbol: required_string(object, "symbol")?,
        side: match object.get("side").and_then(Value::as_str) {
            Some("buy" | "b") => TradeSide::Buy,
            Some("sell" | "s") => TradeSide::Sell,
            _ => TradeSide::Unknown,
        },
        price: required_decimal(object, "price")?,
        qty: required_decimal(object, "qty")?,
        ord_type: required_string(object, "ord_type")?,
        trade_id: object.get("trade_id").and_then(Value::as_u64),
        timestamp: required_string(object, "timestamp")?,
        is_snapshot,
    })
}

pub(crate) fn decode_ohlc(value: Value) -> Option<FuturesOhlcUpdate> {
    let data = extract_data(value)?;
    let object = data.as_object()?;
    Some(FuturesOhlcUpdate {
        symbol: required_string(object, "symbol")?,
        open: required_decimal(object, "open")?,
        high: required_decimal(object, "high")?,
        low: required_decimal(object, "low")?,
        close: required_decimal(object, "close")?,
        trades: required_u64(object, "trades")?,
        volume: required_decimal(object, "volume")?,
        vwap: required_decimal(object, "vwap")?,
        interval_begin: required_string(object, "interval_begin")?,
        interval: required_u32(object, "interval")?,
        tick_type: object
            .get("tick_type")
            .and_then(Value::as_str)
            .and_then(FuturesOhlcTickType::from_wire_str)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instrument_snapshot_decodes_typed_fields_and_drops_bad_records() {
        let snapshot = decode_instrument_snapshot(serde_json::json!({
            "data": {
                "assets": [{
                    "id": "BTC", "status": "enabled", "precision": 10,
                    "precision_display": 8, "borrowable": true,
                    "collateral_value": "1.0", "margin_rate": 0.02, "extra": "ignored"
                }, {
                    "id": "USD"
                }],
                "pairs": [{
                    "symbol": "PI_XBTUSD", "base": "BTC", "quote": "USD",
                    "status": "online", "price_precision": 1, "qty_precision": 8,
                    "qty_min": 1, "qty_increment": "0.0001", "price_increment": 0.5
                }]
            }
        }))
        .expect("snapshot with valid records");

        assert_eq!(snapshot.assets.len(), 1);
        assert_eq!(snapshot.pairs.len(), 1);
        assert_eq!(snapshot.assets[0].collateral_value, Decimal::ONE);
        assert_eq!(snapshot.pairs[0].qty_increment, Decimal::new(1, 4));
    }

    #[test]
    fn instrument_snapshot_drops_nonempty_assets_when_all_are_malformed() {
        assert!(
            decode_instrument_snapshot(serde_json::json!({
                "data": {
                    "assets": [{ "id": "BTC" }],
                    "pairs": []
                }
            }))
            .is_none()
        );
    }

    #[test]
    fn instrument_snapshot_drops_nonempty_pairs_when_all_are_malformed() {
        assert!(
            decode_instrument_snapshot(serde_json::json!({
                "data": {
                    "assets": [],
                    "pairs": [{ "symbol": "PI_XBTUSD" }]
                }
            }))
            .is_none()
        );
    }

    #[test]
    fn instrument_snapshot_preserves_a_genuinely_empty_catalog() {
        let snapshot = decode_instrument_snapshot(serde_json::json!({
            "data": { "assets": [], "pairs": [] }
        }))
        .expect("empty source catalog remains a valid snapshot");
        assert!(snapshot.assets.is_empty());
        assert!(snapshot.pairs.is_empty());
    }

    #[test]
    fn status_decodes_connection_id_with_empty_notice_arrays() {
        let status = decode_system_status(serde_json::json!({
            "data": {
                "api_version": "v2", "connection_id": 7,
                "futures_exchange": "online", "spot_exchange": "online",
                "version": "2.0", "emergency": [], "upcoming_maintenance": []
            }
        }))
        .expect("status");
        assert_eq!(status.connection_id, Some(7));
        assert_eq!(status.futures_exchange, "online");
    }

    #[test]
    fn status_hello_fixture_decodes_the_venue_connection_id() {
        let frame: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/public_status_hello.json"
        ))
        .expect("status hello fixture is valid JSON");
        let status = decode_system_status(
            serde_json::json!({ "type": frame["type"], "data": frame["data"][0] }),
        )
        .expect("status hello decodes");
        assert_eq!(status.connection_id, Some(1_788_608_856_875_973_322));
    }

    #[test]
    fn status_serialization_omits_absent_connection_id() {
        let status = FuturesSystemStatusUpdate {
            api_version: "v2".to_owned(),
            connection_id: None,
            futures_exchange: "online".to_owned(),
            spot_exchange: "offline".to_owned(),
            version: "2.0".to_owned(),
            upcoming_maintenance: Vec::new(),
            emergency: Vec::new(),
        };

        let serialized = serde_json::to_value(status).expect("status serializes");
        assert!(serialized.get("connection_id").is_none());
    }

    #[test]
    fn status_carries_maintenance_windows_and_incidents_and_skips_malformed_ones() {
        let status = |extra: serde_json::Value| {
            let mut data = serde_json::json!({
                "api_version": "v2", "futures_exchange": "online",
                "spot_exchange": "online", "version": "1.0"
            });
            data.as_object_mut()
                .expect("object")
                .extend(extra.as_object().expect("object").clone());
            decode_system_status(serde_json::json!({"type": "update", "data": data}))
                .expect("status decodes")
        };

        let full = status(serde_json::json!({
            "upcoming_maintenance": [
                {
                    "event_id": 7, "title": "Scheduled", "expected_start_utc": "2026-11-08T10:00:00Z",
                    "expected_end_utc": null, "time_to_start_s": 3600, "phase": "announced",
                    "affected_services": ["futures_ws", "futures_trading"],
                    "order_submission": "discouraged", "recommended_action": "cancel_open_orders",
                    "cancel_before_utc": "2026-11-08T09:55:00Z", "source_url": ""
                },
                "not an object"
            ],
            "emergency": [{
                "event_id": 9, "title": "Incident", "incident_status": "investigating",
                "impact": "major", "affected_services": ["all"],
                "started_at_utc": "2026-11-07T01:00:00Z",
                "next_steps": [{
                    "applies_to": ["futures_trading"], "type": "expected_cancel_only",
                    "expected_at_utc": "2026-11-07T02:00:00Z"
                }]
            }]
        }));
        assert_eq!(full.upcoming_maintenance.len(), 1);
        let window = &full.upcoming_maintenance[0];
        assert_eq!(window.event_id, Some(7));
        assert_eq!(window.expected_end_utc, None);
        assert_eq!(window.order_submission.as_deref(), Some("discouraged"));
        assert_eq!(
            window.recommended_action.as_deref(),
            Some("cancel_open_orders")
        );
        assert_eq!(window.affected_services, ["futures_ws", "futures_trading"]);
        assert_eq!(full.emergency.len(), 1);
        assert_eq!(full.emergency[0].impact.as_deref(), Some("major"));
        assert_eq!(
            full.emergency[0].next_steps[0].step_type.as_deref(),
            Some("expected_cancel_only")
        );

        for absent in [
            serde_json::json!({}),
            serde_json::json!({"upcoming_maintenance": null, "emergency": null}),
            serde_json::json!({"upcoming_maintenance": [], "emergency": []}),
        ] {
            let quiet = status(absent);
            assert!(quiet.upcoming_maintenance.is_empty() && quiet.emergency.is_empty());
        }
    }

    #[test]
    fn ticker_and_trade_decode_string_or_number_decimals() {
        let ticker = decode_ticker(serde_json::json!({
            "data": {
                "symbol": "PI_XBTUSD", "timestamp": "2026-10-05T00:00:00Z",
                "next_funding_rate_time": "2026-10-05T08:00:00Z", "suspended": false, "trades": 7,
                "ask": "101.5", "ask_qty": 2, "bid": 101, "bid_qty": "3",
                "change": 1, "change_pct": "0.1", "funding_rate": 0, "funding_rate_prediction": "0",
                "high": 102, "index": "100", "last": 101, "low": "99", "mark": 100,
                "open": "100", "open_interest": 12, "premium": "0", "relative_funding_rate": 0,
                "relative_funding_rate_prediction": "0", "volume": 20, "volume_quote": "2000",
                "vwap": 100, "unknown": "ignored"
            }
        }))
        .expect("typed ticker");
        let FuturesTickerUpdate::Perpetual(ticker) = ticker else {
            panic!("PI symbol must decode as a perpetual ticker");
        };
        assert_eq!(ticker.ask, Some(Decimal::new(1015, 1)));
        assert_eq!(ticker.ask_qty, Some(Decimal::new(2, 0)));
        assert_eq!(ticker.funding_rate, Decimal::ZERO);
        assert_eq!(ticker.next_funding_rate_time, "2026-10-05T08:00:00Z");

        let trade = decode_trade(serde_json::json!({
            "data": {
                "symbol": "PI_XBTUSD", "side": "novel-side", "price": "101.5",
                "qty": 2, "ord_type": "market", "timestamp": "2026-10-05T00:00:00Z"
            }
        }))
        .expect("typed trade");
        assert_eq!(trade.side, TradeSide::Unknown);
        assert_eq!(trade.price, Decimal::new(1015, 1));
        assert_eq!(trade.qty, Decimal::new(2, 0));
    }

    macro_rules! optional_ticker_fields {
        ($ticker:expr) => {
            [
                ("ask", $ticker.ask),
                ("ask_qty", $ticker.ask_qty),
                ("bid", $ticker.bid),
                ("bid_qty", $ticker.bid_qty),
                ("change", $ticker.change),
                ("change_pct", $ticker.change_pct),
                ("high", $ticker.high),
                ("last", $ticker.last),
                ("low", $ticker.low),
                ("open", $ticker.open),
                ("vwap", $ticker.vwap),
            ]
        };
    }

    fn optional_fields(ticker: &FuturesTickerUpdate) -> [(&'static str, Option<Decimal>); 11] {
        match ticker {
            FuturesTickerUpdate::Perpetual(ticker) => optional_ticker_fields!(ticker),
            FuturesTickerUpdate::ExpiringFixed(ticker)
            | FuturesTickerUpdate::ExpiringInverse(ticker) => optional_ticker_fields!(ticker),
        }
    }

    fn assert_only_absent_fields(ticker: &FuturesTickerUpdate, absent: &[&str], case: &str) {
        let serialized = serde_json::to_value(ticker).expect("ticker serializes");
        let body = serialized
            .as_object()
            .and_then(|variant| variant.values().next())
            .expect("tagged ticker body");
        for (field, value) in optional_fields(ticker) {
            assert_eq!(
                value.is_none(),
                absent.contains(&field),
                "{case}: {field} decoded as {value:?}"
            );
            assert_eq!(
                body.get(field).is_some(),
                value.is_some(),
                "{case}: {field} serialization"
            );
        }
    }

    #[test]
    fn captured_sparse_tickers_decode_omitted_fields_as_none_and_serialize_without_them() {
        let mut frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_ticker_omitted_fields.json"
        ))
        .expect("sparse ticker fixture is valid JSON");
        frames.push(
            serde_json::from_str(include_str!(
                "../../../tests/fixtures/unified_ws/unified_ticker_fi_xbtusd.json"
            ))
            .expect("FI ticker capture is valid JSON"),
        );
        let trade_window = [
            "change",
            "change_pct",
            "high",
            "last",
            "low",
            "open",
            "vwap",
        ];
        let empty_ask_side = [
            "change",
            "change_pct",
            "high",
            "last",
            "low",
            "open",
            "vwap",
            "ask",
            "ask_qty",
        ];
        let core_only = [
            "change",
            "change_pct",
            "high",
            "last",
            "low",
            "open",
            "vwap",
            "ask",
            "ask_qty",
            "bid",
            "bid_qty",
        ];
        let cases: [(&str, &[&str]); 4] = [
            ("PF_SATSUSD", &trade_window),
            ("PF_GBPUSD", &empty_ask_side),
            ("FF_XBTUSD_270326", &trade_window),
            ("FI_XBTUSD_260925", &core_only),
        ];
        assert_eq!(frames.len(), cases.len());

        for (frame, (symbol, omitted)) in frames.iter().zip(cases) {
            let ticker = decode_ticker(serde_json::json!({ "data": frame["data"][0] }))
                .unwrap_or_else(|| panic!("{symbol} ticker decodes"));
            assert_eq!(ticker.symbol(), symbol);
            assert_only_absent_fields(&ticker, omitted, symbol);
        }
    }

    #[test]
    fn each_optional_ticker_field_omitted_or_null_decodes_as_none_and_is_not_serialized() {
        let parse =
            |fixture: &str| -> Value { serde_json::from_str(fixture).expect("valid ticker JSON") };
        let ff = parse(include_str!(
            "../../../tests/fixtures/unified_ws/unified_ticker_ff_xbtusd.json"
        ));
        let mut fi = ff.clone();
        fi["data"][0]["symbol"] = Value::from("FI_XBTUSD_260925");
        let frames = [
            parse(include_str!(
                "../../../tests/fixtures/unified_ws/unified_ticker_pi_xbtusd.json"
            )),
            parse(include_str!(
                "../../../tests/fixtures/unified_ws/unified_ticker_pf_ethusd.json"
            )),
            ff,
            fi,
        ];
        for frame in frames {
            let full = &frame["data"][0];
            let ticker =
                decode_ticker(serde_json::json!({ "data": full })).expect("ticker decodes");
            for (field, _) in optional_fields(&ticker) {
                for null in [false, true] {
                    let mut data = full.clone();
                    let object = data.as_object_mut().expect("ticker data is an object");
                    if null {
                        object.insert(field.to_owned(), Value::Null);
                    } else {
                        object.remove(field);
                    }
                    let case = format!("{} {field} null={null}", ticker.symbol());
                    let sparse = decode_ticker(serde_json::json!({ "data": data }))
                        .unwrap_or_else(|| panic!("{case}: ticker decodes"));
                    assert_only_absent_fields(&sparse, &[field], &case);
                }
            }
        }
    }

    #[test]
    fn captured_full_tickers_select_the_product_variant_and_serialize_tagged() {
        let pi: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_ticker_pi_xbtusd.json"
        ))
        .expect("PI ticker fixture is valid JSON");
        let pi =
            decode_ticker(serde_json::json!({ "data": pi["data"][0] })).expect("PI ticker decodes");
        let FuturesTickerUpdate::Perpetual(pi) = pi else {
            panic!("PI symbol must decode as a perpetual ticker");
        };
        assert_eq!(pi.symbol, "PI_XBTUSD");
        assert_eq!(pi.funding_rate, Decimal::ZERO);
        assert_eq!(
            serde_json::to_value(FuturesTickerUpdate::Perpetual(pi.clone()))
                .expect("perpetual ticker serializes")["perpetual"]["ask"],
            "101.5"
        );

        let pf: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_ticker_pf_ethusd.json"
        ))
        .expect("PF ticker fixture is valid JSON");
        let pf =
            decode_ticker(serde_json::json!({ "data": pf["data"][0] })).expect("PF ticker decodes");
        let FuturesTickerUpdate::Perpetual(pf) = pf else {
            panic!("PF symbol must decode as a perpetual ticker");
        };
        assert_eq!(pf.symbol, "PF_ETHUSD");
        assert_eq!(
            serde_json::to_value(FuturesTickerUpdate::Perpetual(pf))
                .expect("perpetual ticker serializes")["perpetual"]["symbol"],
            "PF_ETHUSD"
        );

        let ff: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_ticker_ff_xbtusd.json"
        ))
        .expect("FF ticker fixture is valid JSON");
        let ff =
            decode_ticker(serde_json::json!({ "data": ff["data"][0] })).expect("FF ticker decodes");
        let FuturesTickerUpdate::ExpiringFixed(ff) = ff else {
            panic!("FF symbol must decode as a fixed-expiry ticker");
        };
        assert_eq!(ff.symbol, "FF_XBTUSD");
        assert_eq!(ff.ask, Some(Decimal::new(1_012_505, 1)));
        assert_eq!(ff.vwap, Some(Decimal::new(101_230, 0)));
        assert_eq!(
            serde_json::to_value(FuturesTickerUpdate::ExpiringFixed(ff))
                .expect("fixed-expiry ticker serializes")["expiring_fixed"]["symbol"],
            "FF_XBTUSD"
        );
    }

    #[test]
    fn docs_shaped_fi_ticker_decodes_quote_and_required_fields_into_expiring_inverse() {
        let frame: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_ticker_fi_xbtusd_docs_shaped.json"
        ))
        .expect("FI ticker shaped like the REST docs example, core values invented, is valid JSON");
        let ticker = decode_ticker(serde_json::json!({ "data": frame["data"][0] }))
            .expect("FI ticker with quote fields decodes");
        assert_only_absent_fields(
            &ticker,
            &[
                "ask",
                "ask_qty",
                "change",
                "change_pct",
                "high",
                "low",
                "vwap",
            ],
            "FI_XBTUSD_211231",
        );
        let serialized = serde_json::to_value(&ticker).expect("inverse-expiry ticker serializes");
        let FuturesTickerUpdate::ExpiringInverse(fi) = ticker else {
            panic!("FI symbol must decode as an inverse-expiry ticker");
        };
        assert_eq!(fi.symbol, "FI_XBTUSD_211231");
        assert_eq!(fi.timestamp, "2021-12-01T12:00:00.000000000Z");
        assert!(!fi.suspended);
        assert_eq!(fi.trades, 1);
        assert_eq!(fi.volume, Decimal::new(100, 0));
        assert_eq!(fi.volume_quote, Decimal::new(2_800_200, 0));
        assert_eq!(fi.index, Decimal::new(2_800_125, 2));
        assert_eq!(fi.mark, Decimal::new(280_045, 1));
        assert_eq!(fi.open_interest, Decimal::new(1_500, 0));
        assert_eq!(fi.premium, Decimal::new(1, 4));
        assert_eq!(fi.bid, Some(Decimal::new(28_002, 0)));
        assert_eq!(fi.bid_qty, Some(Decimal::new(900, 0)));
        assert_eq!(fi.last, Some(Decimal::new(28_002, 0)));
        assert_eq!(fi.open, Some(Decimal::new(28_002, 0)));
        assert_eq!(serialized["expiring_inverse"]["bid"], "28002");

        let mut data = frame["data"][0].clone();
        data["index"] = Value::from("28001.25");
        let Some(FuturesTickerUpdate::ExpiringInverse(string_index)) =
            decode_ticker(serde_json::json!({ "data": data }))
        else {
            panic!("FI ticker with a string-encoded index must decode as inverse-expiry");
        };
        assert_eq!(string_index.index, Decimal::new(2_800_125, 2));
    }

    #[test]
    fn ticker_prefix_field_mismatches_and_malformed_known_fields_drop() {
        let fi: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_ticker_fi_xbtusd.json"
        ))
        .expect("FI ticker capture is valid JSON");
        let fi_with = |key: &str, value: Value| {
            let mut data = fi["data"][0].clone();
            data[key] = value;
            serde_json::json!({ "data": data })
        };
        for funding in [
            "next_funding_rate_time",
            "funding_rate",
            "funding_rate_prediction",
            "relative_funding_rate",
            "relative_funding_rate_prediction",
        ] {
            assert!(
                decode_ticker(fi_with(funding, Value::from(0))).is_none(),
                "FI ticker with {funding}"
            );
        }
        assert!(decode_ticker(fi_with("ask", serde_json::json!([]))).is_none());

        let ff_with_funding = serde_json::json!({
            "data": {
                "symbol": "FF_XBTUSD", "timestamp": "2026-10-05T00:00:00Z",
                "suspended": false, "trades": 7, "ask": 101, "ask_qty": 2, "bid": 100,
                "bid_qty": 3, "change": 1, "change_pct": 1, "high": 102, "index": 100,
                "last": 101, "low": 99, "mark": 100, "open": 100, "open_interest": 12,
                "premium": 0, "volume": 20, "volume_quote": 2000, "vwap": 100,
                "funding_rate": 0
            }
        });
        assert!(decode_ticker(ff_with_funding).is_none());

        let pi_bad_decimal = serde_json::json!({
            "data": {
                "symbol": "PI_XBTUSD", "timestamp": "2026-10-05T00:00:00Z",
                "next_funding_rate_time": "2026-10-05T08:00:00Z", "suspended": false, "trades": 7,
                "ask": [], "ask_qty": 2, "bid": 101, "bid_qty": 3, "change": 1, "change_pct": 1,
                "funding_rate": 0, "funding_rate_prediction": 0, "high": 102, "index": 100,
                "last": 101, "low": 99, "mark": 100, "open": 100, "open_interest": 12,
                "premium": 0, "relative_funding_rate": 0, "relative_funding_rate_prediction": 0,
                "volume": 20, "volume_quote": 2000, "vwap": 100
            }
        });
        assert!(decode_ticker(pi_bad_decimal).is_none());
    }

    #[test]
    fn trade_decode_reads_is_snapshot_from_envelope_type() {
        let frame = |kind: &str| {
            serde_json::json!({
                "type": kind,
                "data": {
                    "symbol": "PF_XBTUSD", "side": "buy", "price": 80251, "qty": 0.0062,
                    "ord_type": "limit", "timestamp": "2026-11-05T00:00:00Z"
                }
            })
        };
        assert!(
            !decode_trade(frame("update"))
                .expect("update decodes")
                .is_snapshot
        );
        assert!(
            decode_trade(frame("snapshot"))
                .expect("snapshot decodes")
                .is_snapshot
        );
    }

    #[test]
    fn raw_book_decode_preserves_native_symbol_and_snapshot_kind() {
        let raw = decode_book_raw(serde_json::json!({
            "type": "snapshot",
            "data": {
                "symbol": "PI_XBTUSD",
                "bids": [{"price": "100.5", "qty": "2"}],
                "asks": [{"price": "101", "qty": "3"}],
                "checksum": 42,
                "timestamp": "2026-10-05T00:00:00Z"
            }
        }))
        .expect("raw book");
        assert_eq!(raw.symbol, "PI_XBTUSD");
        assert!(raw.is_snapshot);
        assert_eq!(raw.bids[0].price, Decimal::new(1005, 1));
        assert_eq!(raw.asks[0].qty, Decimal::new(3, 0));
    }

    #[test]
    fn raw_book_levels_carry_wire_strings_that_reproduce_the_checksum() {
        let frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_futures_book_crc_sequence.json"
        ))
        .expect("checked-in Unified Futures book fixture is valid JSON");
        let raw = decode_book_raw(serde_json::json!({
            "type": frames[0]["type"],
            "data": frames[0]["data"][0],
        }))
        .expect("captured snapshot decodes");
        assert_eq!(raw.bids[0].price_wire, "83347.0");
        assert_eq!(raw.asks[0].qty_wire, "74");
        let wire = |levels: &[crate::book::PriceLevel]| -> Vec<(String, String)> {
            levels
                .iter()
                .take(10)
                .map(|l| (l.price_wire.clone(), l.qty_wire.clone()))
                .collect()
        };
        let (asks, bids) = (wire(&raw.asks), wire(&raw.bids));
        let crc = crate::book::compute_book_crc32(
            asks.iter().map(|(p, q)| (p.as_str(), q.as_str())),
            bids.iter().map(|(p, q)| (p.as_str(), q.as_str())),
        );
        assert_eq!(crc, raw.checksum);
    }

    #[test]
    fn captured_instrument_and_trade_frames_decode_without_trade_ids() {
        let instrument: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_instrument_snapshot.json"
        ))
        .expect("checked-in instrument fixture is valid JSON");
        let snapshot = decode_instrument_snapshot(instrument).expect("instrument snapshot decodes");
        assert!(!snapshot.assets.is_empty());
        assert!(snapshot.pairs.iter().any(|pair| pair.symbol == "PI_XBTUSD"));

        let frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_trade_updates_without_trade_id.json"
        ))
        .expect("checked-in trade fixture is valid JSON");
        let updates: Vec<_> = frames
            .iter()
            .flat_map(|frame| frame["data"].as_array().expect("trade data array"))
            .map(|entry| {
                assert!(entry.get("trade_id").is_none());
                decode_trade(serde_json::json!({ "data": entry })).expect("trade print decodes")
            })
            .collect();
        assert!(updates.iter().all(|update| update.trade_id.is_none()));

        let numeric_side = decode_trade(serde_json::json!({
            "data": {
                "symbol": "PI_XBTUSD",
                "side": 1,
                "price": 83661.0,
                "qty": 10,
                "ord_type": "limit",
                "timestamp": "2026-09-28T14:00:36.238283000Z"
            }
        }))
        .expect("a non-string side must not drop the print");
        assert_eq!(numeric_side.side, TradeSide::Unknown);

        assert_eq!(updates.len(), 13);
        assert_eq!(
            updates[0], updates[1],
            "identical-looking prints are retained"
        );
    }

    #[test]
    fn trade_id_decodes_from_an_unsigned_integer_and_is_none_otherwise() {
        let print = |trade_id: Value| {
            decode_trade(serde_json::json!({
                "data": {
                    "symbol": "PF_ETHUSD",
                    "side": "buy",
                    "price": 3120.5,
                    "qty": 0.01,
                    "ord_type": "market",
                    "trade_id": trade_id,
                    "timestamp": "2026-11-12T05:40:00.000000000Z"
                }
            }))
            .expect("a print decodes whatever its trade_id")
            .trade_id
        };
        assert_eq!(print(serde_json::json!(43_792_074_u64)), Some(43_792_074));
        assert_eq!(print(serde_json::json!("43792074")), None);
        assert_eq!(print(serde_json::json!(-1)), None);
        assert_eq!(print(Value::Null), None);
    }

    #[test]
    fn captured_ohlc_frames_decode_with_wire_string_symbols() {
        let frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/unified_ohlc_candles.json"
        ))
        .expect("checked-in OHLC fixture is valid JSON");
        let updates: Vec<_> = frames
            .iter()
            .flat_map(|frame| frame["data"].as_array().expect("ohlc data array"))
            .map(|entry| decode_ohlc(serde_json::json!({ "data": entry })).expect("candle decodes"))
            .collect();
        let dec = |text: &str| text.parse::<Decimal>().expect("decimal literal");

        assert_eq!(updates.len(), 3);
        assert_eq!(
            updates[0],
            FuturesOhlcUpdate {
                symbol: "PF_XBTUSD".to_string(),
                open: dec("80157"),
                high: dec("80183"),
                low: dec("80156"),
                close: dec("80183"),
                trades: 108,
                volume: dec("0.0109"),
                vwap: dec("80169.165137614"),
                interval_begin: "2026-10-29T10:59:00.000000000Z".to_string(),
                interval: 1,
                tick_type: FuturesOhlcTickType::Trade,
            }
        );
        assert_eq!(
            updates[1],
            FuturesOhlcUpdate {
                symbol: "PF_ETHUSD".to_string(),
                open: dec("2323.6"),
                high: dec("2326.6"),
                low: dec("2323.5"),
                close: dec("2323.5"),
                trades: 95,
                volume: dec("0.095"),
                vwap: dec("2324.949473684"),
                interval_begin: "2026-10-29T10:59:00.000000000Z".to_string(),
                interval: 1,
                tick_type: FuturesOhlcTickType::Trade,
            }
        );
    }

    #[test]
    fn ohlc_rows_decode_string_decimals_and_mark_tick_type_and_drop_missing_fields() {
        let candle = |open: Value| {
            serde_json::json!({ "data": {
                "symbol": "PF_XBTUSD",
                "open": open,
                "high": "2.5",
                "low": 0.5,
                "close": 1.5,
                "trades": 3,
                "volume": "0.25",
                "vwap": 1.25,
                "interval_begin": "2026-10-29T10:59:00.000000000Z",
                "interval": 5,
                "tick_type": "mark"
            }})
        };
        let dec = |text: &str| text.parse::<Decimal>().expect("decimal literal");
        let decoded = decode_ohlc(candle(serde_json::json!("1"))).expect("string decimal decodes");
        assert_eq!(decoded.open, dec("1"));
        assert_eq!(decoded.high, dec("2.5"));
        assert_eq!(decoded.low, dec("0.5"));
        assert_eq!(decoded.close, dec("1.5"));
        assert_eq!(decoded.volume, dec("0.25"));
        assert_eq!(decoded.vwap, dec("1.25"));
        assert_eq!(decoded.trades, 3);
        assert_eq!(decoded.interval, 5);
        assert_eq!(decoded.tick_type, FuturesOhlcTickType::Mark);

        assert!(decode_ohlc(candle(Value::Null)).is_none());
        for field in ["interval_begin", "tick_type"] {
            let mut missing = candle(serde_json::json!(1));
            missing["data"]
                .as_object_mut()
                .expect("object")
                .remove(field);
            assert!(decode_ohlc(missing).is_none(), "{field}");
        }
    }

    #[test]
    fn status_survives_mistyped_notice_containers_and_fields() {
        let status = |extra: serde_json::Value| {
            let mut data = serde_json::json!({
                "api_version": "v2", "futures_exchange": "online",
                "spot_exchange": "online", "version": "1.0"
            });
            data.as_object_mut()
                .expect("object")
                .extend(extra.as_object().expect("object").clone());
            decode_system_status(serde_json::json!({"type": "update", "data": data}))
                .expect("a mistyped notice must not fail the status frame")
        };

        for bad in [
            serde_json::json!("x"),
            serde_json::json!({"a": 1}),
            serde_json::json!(5),
        ] {
            let quiet = status(serde_json::json!({"upcoming_maintenance": bad, "emergency": bad}));
            assert_eq!(quiet.futures_exchange, "online");
            assert!(quiet.upcoming_maintenance.is_empty() && quiet.emergency.is_empty());
        }

        let mistyped = status(serde_json::json!({
            "upcoming_maintenance": [{
                "event_id": "7", "title": 5, "affected_services": "all",
                "phase": "announced", "time_to_start_s": "soon"
            }],
            "emergency": [{
                "event_id": 9, "impact": "major", "affected_services": ["a", 5],
                "next_steps": [{"type": 5, "applies_to": "x", "expected_at_utc": "t"}]
            }]
        }));
        let window = &mistyped.upcoming_maintenance[0];
        assert_eq!(window.phase.as_deref(), Some("announced"));
        assert_eq!(window.event_id, None);
        assert_eq!(window.title, None);
        assert_eq!(window.time_to_start_s, None);
        assert!(window.affected_services.is_empty());
        let incident = &mistyped.emergency[0];
        assert_eq!(incident.event_id, Some(9));
        assert_eq!(incident.impact.as_deref(), Some("major"));
        assert_eq!(incident.affected_services, ["a"]);
        let step = &incident.next_steps[0];
        assert_eq!(step.expected_at_utc.as_deref(), Some("t"));
        assert_eq!(step.step_type, None);
        assert!(step.applies_to.is_empty());
    }

    #[test]
    fn incident_step_serializes_with_its_rust_field_name_and_no_nulls() {
        let step = FuturesIncidentStep {
            step_type: Some("expected_restart".to_owned()),
            ..FuturesIncidentStep::default()
        };
        let value = serde_json::to_value(&step).expect("serializes");
        assert_eq!(value["step_type"], "expected_restart");
        assert!(value.get("type").is_none());
        assert!(value.get("expected_at_utc").is_none());
    }
}
