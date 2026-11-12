//! Typed payloads and decoders for Unified Futures private account channels.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

use crate::api::ws_decode::{
    decimal, deserialize_optional_decimal, extract_data, is_snapshot_frame, lenient_rows,
    lenient_vec_for,
};

use super::ws_types::{RawWallet, Wallet, wallet_from_raw};

/// One Unified Futures private `executions` entry.
///
/// Futures lifecycle records are sparse. `exec_type` is retained as its wire
/// string so a newly introduced venue transition remains visible to callers.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesExecutionUpdate {
    /// Venue lifecycle transition, for example `"new"`, `"trade"`, or `"canceled"`.
    pub exec_type: String,
    /// Venue order identifier.
    pub order_id: Option<String>,
    /// Client order identifier as the venue reports it: a lowercase UUID is unchanged, an
    /// uppercase UUID is lowercased, 32 hex digits are hyphenated, any other id is packed
    /// into a UUID. Absent on `amended` and `canceled` records; `order_id` joins every record.
    pub cl_ord_id: Option<String>,
    /// Native Futures instrument such as `PF_XBTUSD`.
    pub symbol: Option<String>,
    /// Order side as emitted by the venue.
    pub side: Option<String>,
    /// Order type as emitted by the venue.
    pub order_type: Option<String>,
    /// Order state after this transition.
    pub order_status: Option<String>,
    /// Time in force as emitted by the venue, for example `"GTC"` or `"IOC"`.
    pub time_in_force: Option<String>,
    /// Whether the order only reduces or closes a position; `None` when the venue omits it
    /// or sends a non-boolean.
    pub reduce_only: Option<bool>,
    /// Whether the order is post-only; `None` when the venue omits it or sends a non-boolean.
    /// A post-only order
    /// may instead report `order_type` `"post_only"` with no flag.
    pub post_only: Option<bool>,
    /// Venue trade identifier on fill records.
    pub trade_id: Option<String>,
    /// Venue sequence identifier on fill records.
    pub sequence_id: Option<String>,
    /// RFC 3339 venue timestamp.
    pub timestamp: Option<String>,
    /// Total requested quantity.
    pub order_qty: Option<Decimal>,
    /// Cumulative filled quantity. `None` means the venue sent no figure, not zero:
    /// it omits it on the `new` and `amended` records of stop-loss, take-profit and
    /// trailing-stop orders (and their `-limit` variants) and on `canceled` records.
    /// Fill records and plain orders carry it.
    pub cum_qty: Option<Decimal>,
    /// Most recent fill quantity.
    pub last_qty: Option<Decimal>,
    /// Most recent fill price.
    pub last_price: Option<Decimal>,
    /// Remaining quantity.
    pub remaining_qty: Option<Decimal>,
    /// Limit price when present.
    pub limit_price: Option<Decimal>,
    /// Liquidity indicator on fill records.
    pub liquidity_ind: Option<String>,
    /// Fee charged by the fill.
    pub fee: Option<Decimal>,
    /// Price trigger of a triggered order type; `None` on other orders or when the
    /// venue sends no usable object.
    pub triggers: Option<ExecutionTriggers>,
    /// `true` when the row arrived in the opening snapshot, `false` for a live update.
    pub is_snapshot: bool,
}

/// Price-trigger parameters and state of a triggered order (wire `triggers`).
///
/// Values are passed through as the venue sends them. A number or boolean in a string
/// field is kept as its text; any other mistyped field is `None`.
#[serde_with::apply(Option<String> => #[serde(default, deserialize_with = "deserialize_lenient_string")])]
#[serde_with::apply(Option<Decimal> => #[serde(default, deserialize_with = "deserialize_optional_decimal")])]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ExecutionTriggers {
    /// Reference price tracked for triggering, for example `"last"` or `"index"`.
    pub reference: Option<String>,
    /// Trigger price, read with `price_type`.
    pub price: Option<Decimal>,
    /// Units of `price`: `"static"`, `"pct"` or `"quote"`.
    pub price_type: Option<String>,
    /// Current effective trigger price.
    pub actual_price: Option<Decimal>,
    /// Peak or trough price of a trailing order.
    pub peak_price: Option<Decimal>,
    /// Reference last price that activated the trigger.
    pub last_price: Option<Decimal>,
    /// `"triggered"` or `"untriggered"`.
    pub status: Option<String>,
    /// RFC 3339 time the trigger activated.
    pub timestamp: Option<String>,
}

#[serde_with::apply(Option<String> => #[serde(default, deserialize_with = "deserialize_lenient_string")])]
#[derive(Debug, Deserialize)]
struct RawFuturesExecution {
    exec_type: String,
    order_id: Option<String>,
    cl_ord_id: Option<String>,
    symbol: Option<String>,
    side: Option<String>,
    order_type: Option<String>,
    order_status: Option<String>,
    time_in_force: Option<String>,
    reduce_only: Option<Value>,
    post_only: Option<Value>,
    trade_id: Option<String>,
    sequence_id: Option<Value>,
    timestamp: Option<String>,
    order_qty: Option<Value>,
    cum_qty: Option<Value>,
    last_qty: Option<Value>,
    last_price: Option<Value>,
    remaining_qty: Option<Value>,
    limit_price: Option<Value>,
    liquidity_ind: Option<String>,
    fee: Option<Value>,
    triggers: Option<Value>,
}

pub(crate) fn decode_futures_executions(value: Value) -> Option<FuturesExecutionUpdate> {
    let is_snapshot = is_snapshot_frame(&value);
    let raw: RawFuturesExecution = serde_json::from_value(extract_data(value)?).ok()?;
    Some(FuturesExecutionUpdate {
        exec_type: raw.exec_type,
        order_id: raw.order_id,
        cl_ord_id: raw.cl_ord_id,
        symbol: raw.symbol,
        side: raw.side,
        order_type: raw.order_type,
        order_status: raw.order_status,
        time_in_force: raw.time_in_force,
        reduce_only: raw.reduce_only.as_ref().and_then(Value::as_bool),
        post_only: raw.post_only.as_ref().and_then(Value::as_bool),
        trade_id: raw.trade_id,
        sequence_id: raw.sequence_id.map(|id| match id {
            Value::String(value) => value,
            other => other.to_string(),
        }),
        timestamp: raw.timestamp,
        order_qty: decimal(raw.order_qty),
        cum_qty: decimal(raw.cum_qty),
        last_qty: decimal(raw.last_qty),
        last_price: decimal(raw.last_price),
        remaining_qty: decimal(raw.remaining_qty),
        limit_price: decimal(raw.limit_price),
        liquidity_ind: raw.liquidity_ind,
        fee: decimal(raw.fee),
        triggers: raw
            .triggers
            .and_then(|value| serde_json::from_value(value).ok()),
        is_snapshot,
    })
}

/// A populated Futures position within a Unified private positions snapshot or update.
#[serde_with::apply(Option<String> => #[serde(default, deserialize_with = "deserialize_lenient_string")])]
#[serde_with::apply(Option<Decimal> => #[serde(default, deserialize_with = "deserialize_optional_decimal")])]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FuturesPosition {
    /// Native Futures instrument identifier.
    pub tradeable: Option<String>,
    /// Signed open size.
    pub size: Option<Decimal>,
    /// Average entry price.
    pub entry_price: Option<Decimal>,
    /// RFC 3339 time at which the position was filled.
    pub fill_time: Option<String>,
}

fn deserialize_lenient_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<Value>::deserialize(deserializer)? {
        Some(Value::String(value)) => Some(value),
        Some(value @ (Value::Number(_) | Value::Bool(_))) => Some(value.to_string()),
        _ => None,
    })
}

/// Unified Futures `positions` snapshot or update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesPositionsUpdate {
    /// RFC 3339 venue timestamp.
    pub time: String,
    /// Futures positions.
    pub futures: Vec<FuturesPosition>,
    /// `true` for the opening snapshot, `false` for a live update.
    pub is_snapshot: bool,
}

fn positions_futures<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<FuturesPosition>, D::Error> {
    lenient_vec_for("positions.futures", d)
}

#[derive(Debug, Deserialize)]
struct RawFuturesPositions {
    time: String,
    #[serde(default, deserialize_with = "positions_futures")]
    futures: Vec<FuturesPosition>,
}

pub(crate) fn decode_futures_positions(value: Value) -> Option<FuturesPositionsUpdate> {
    let is_snapshot = is_snapshot_frame(&value);
    let raw: RawFuturesPositions = serde_json::from_value(extract_data(value)?).ok()?;
    Some(FuturesPositionsUpdate {
        time: raw.time,
        futures: raw.futures,
        is_snapshot,
    })
}

/// Common values in the Unified Futures multi-collateral margin payload.
#[serde_with::apply(Option<Decimal> => #[serde(default, deserialize_with = "deserialize_optional_decimal")])]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FuturesMarginValues {
    /// Margin available for new positions.
    pub available_margin: Option<Decimal>,
    /// Account balance valuation.
    pub balance_value: Option<Decimal>,
    /// Collateral valuation.
    pub collateral_value: Option<Decimal>,
    /// Effective leverage as reported by the venue.
    pub effective_leverage: Option<Decimal>,
    /// Initial margin currently reserved.
    pub initial_margin: Option<Decimal>,
    /// Initial margin excluding resting orders.
    pub initial_margin_without_orders: Option<Decimal>,
    /// Maintenance margin currently required.
    pub maintenance_margin: Option<Decimal>,
    /// Current margin equity.
    pub margin_equity: Option<Decimal>,
    /// Profit and loss as reported by the venue.
    pub pnl: Option<Decimal>,
    /// Account portfolio valuation.
    pub portfolio_value: Option<Decimal>,
    /// Total unrealized profit and loss.
    pub total_unrealized: Option<Decimal>,
    /// Unrealized profit and loss usable as margin.
    pub total_unrealized_as_margin: Option<Decimal>,
    /// Unrealized funding amount.
    pub unrealized_funding: Option<Decimal>,
}

/// Multi-collateral Futures margin payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesMultiCollateralMargin {
    /// RFC 3339 venue timestamp.
    pub time: String,
    /// Aggregate account margin values.
    pub values: FuturesMarginValues,
    /// Cross-margin values.
    pub cross: FuturesMarginValues,
    /// Margin figures of each isolated-margin position; empty when none is open.
    pub isolated: Vec<FuturesIsolatedMargin>,
}

/// Margin figures of one isolated-margin position.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesIsolatedMargin {
    /// Instrument the isolated position is on.
    pub tradeable: Option<String>,
    /// The position's margin figures; the venue sends only the initial, maintenance, pnl and unrealized figures here.
    pub values: FuturesMarginValues,
}

/// Single-collateral Futures margin payload: the full current state of every
/// single-collateral wallet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FuturesSingleCollateralMargin {
    /// RFC 3339 venue timestamp.
    pub time: String,
    /// One entry per single-collateral wallet; empty when the account has none.
    #[serde(default, deserialize_with = "single_collateral_wallets")]
    pub wallets: Vec<FuturesSingleCollateralWallet>,
}

/// Margin state of one single-collateral wallet. Figures are denominated in `unit`.
#[serde_with::apply(Option<String> => #[serde(default, deserialize_with = "deserialize_lenient_string")])]
#[serde_with::apply(Option<Decimal> => #[serde(default, deserialize_with = "deserialize_optional_decimal")])]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FuturesSingleCollateralWallet {
    /// Base currency of the wallet's pair, for example `"XBT"`.
    pub base_currency: Option<String>,
    /// Quote currency of the wallet's pair, for example `"USD"`.
    pub quote_currency: Option<String>,
    /// Settlement asset the figures are denominated in.
    pub unit: Option<String>,
    /// Current balance.
    pub balance: Option<Decimal>,
    /// Balance with haircuts and unrealized margin from open positions.
    pub portfolio_value: Option<Decimal>,
    /// Initial margin for open positions and open orders.
    pub initial_margin: Option<Decimal>,
    /// Maintenance margin for open positions.
    pub maintenance_margin: Option<Decimal>,
    /// Portfolio value minus initial margin.
    pub available: Option<Decimal>,
    /// Unrealized funding of open positions.
    pub unrealized_funding: Option<Decimal>,
    /// Profit and loss of open positions.
    pub pnl: Option<Decimal>,
}

fn single_collateral_wallets<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Vec<FuturesSingleCollateralWallet>, D::Error> {
    lenient_vec_for("margin.wallets", d)
}

/// A Unified Futures `margin` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum FuturesMarginUpdate {
    /// Multi-collateral margin data.
    MultiCollateral(Box<FuturesMultiCollateralMargin>),
    /// Single-collateral margin data.
    SingleCollateral(FuturesSingleCollateralMargin),
}

#[derive(Debug, Deserialize)]
struct RawMultiCollateralMargin {
    time: String,
    #[serde(flatten)]
    values: FuturesMarginValues,
    cross: FuturesMarginValues,
    isolated: Option<Value>,
}

#[serde_with::apply(Option<String> => #[serde(default, deserialize_with = "deserialize_lenient_string")])]
#[derive(Debug, Deserialize)]
struct RawIsolatedMargin {
    tradeable: Option<String>,
    #[serde(flatten)]
    values: FuturesMarginValues,
}

pub(crate) fn decode_futures_margin(value: Value) -> Option<FuturesMarginUpdate> {
    let frame_type = value.get("type")?.as_str()?.to_owned();
    let data = extract_data(value)?;
    match frame_type.as_str() {
        "multi_collateral" => {
            let raw: RawMultiCollateralMargin = serde_json::from_value(data).ok()?;
            Some(FuturesMarginUpdate::MultiCollateral(Box::new(
                FuturesMultiCollateralMargin {
                    time: raw.time,
                    values: raw.values,
                    cross: raw.cross,
                    isolated: lenient_rows::<RawIsolatedMargin>("margin.isolated", raw.isolated)
                        .into_iter()
                        .map(|row| FuturesIsolatedMargin {
                            tradeable: row.tradeable,
                            values: row.values,
                        })
                        .collect(),
                },
            )))
        }
        "single_collateral" => serde_json::from_value(data)
            .ok()
            .map(FuturesMarginUpdate::SingleCollateral),
        _ => None,
    }
}

/// One shared Unified private balances entry delivered to the Futures callback.
///
/// The venue accepts no `asset_class` on this channel. A populated
/// derivatives-wallet discriminator has not yet been observed, so wallet rows
/// are preserved rather than filtered.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct FuturesBalanceUpdate {
    /// Asset code as emitted by Unified private.
    pub asset: Option<String>,
    /// Wire asset class.
    pub asset_class: Option<String>,
    /// Total asset balance.
    pub balance: Option<Decimal>,
    /// Wallet detail on snapshots.
    pub wallets: Option<Vec<Wallet>>,
    /// Signed ledger amount applied by this entry; update entries only.
    pub amount: Option<Decimal>,
    /// Fee charged on the ledger event; update entries only.
    pub fee: Option<Decimal>,
    /// Ledger entry identifier; update entries only.
    pub ledger_id: Option<String>,
    /// Reference identifier of the originating event; update entries only.
    pub ref_id: Option<String>,
    /// RFC 3339 timestamp of the ledger event; update entries only.
    pub timestamp: Option<String>,
    /// Ledger type as emitted by the venue (wire `type`); update entries only.
    pub ledger_type: Option<String>,
    /// Wallet type of the ledger event; update entries only.
    pub wallet_type: Option<String>,
    /// Wallet identifier of the ledger event; update entries only.
    pub wallet_id: Option<String>,
    /// `true` when the row arrived in the opening snapshot, `false` for a live update.
    pub is_snapshot: bool,
    /// Frame sequence number; every row of one frame carries the same value.
    pub sequence: Option<u64>,
}

#[serde_with::apply(Option<String> => #[serde(default, deserialize_with = "deserialize_lenient_string")])]
#[derive(Debug, Deserialize)]
struct RawFuturesBalance {
    asset: Option<String>,
    asset_class: Option<String>,
    balance: Option<Value>,
    wallets: Option<Value>,
    amount: Option<Value>,
    fee: Option<Value>,
    ledger_id: Option<String>,
    ref_id: Option<String>,
    timestamp: Option<String>,
    #[serde(rename = "type")]
    ledger_type: Option<String>,
    wallet_type: Option<String>,
    wallet_id: Option<String>,
}

/// The account's futures trading permission from the `preferences` channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum FuturesTradingPermission {
    /// Wire `"enabled"` — the account may place futures orders.
    Enabled,
    /// Wire `"reduce_only"` — only orders that reduce or close existing futures positions.
    ReduceOnly,
    /// Wire `"disabled"` — the account may not place futures orders.
    Disabled,
    /// Unrecognized wire value — forward-compatible catch-all.
    #[serde(other)]
    Unknown,
}

/// Unified Futures `preferences` snapshot or update.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FuturesPreferencesUpdate {
    /// Futures trading permission; `None` when the frame omits the field.
    #[serde(default, deserialize_with = "deserialize_lenient_permission")]
    pub futures_trading: Option<FuturesTradingPermission>,
}

fn deserialize_lenient_permission<'de, D>(
    deserializer: D,
) -> Result<Option<FuturesTradingPermission>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<Value>::deserialize(deserializer)? {
        None | Some(Value::Null) => None,
        Some(value) => {
            Some(serde_json::from_value(value).unwrap_or(FuturesTradingPermission::Unknown))
        }
    })
}

pub(crate) fn decode_futures_preferences(value: Value) -> Option<FuturesPreferencesUpdate> {
    serde_json::from_value(extract_data(value)?).ok()
}

pub(crate) fn decode_futures_balances(value: Value) -> Option<FuturesBalanceUpdate> {
    let is_snapshot = is_snapshot_frame(&value);
    let sequence = value.get("sequence").and_then(Value::as_u64);
    let raw: RawFuturesBalance = serde_json::from_value(extract_data(value)?).ok()?;
    Some(FuturesBalanceUpdate {
        asset: raw.asset,
        asset_class: raw.asset_class,
        balance: decimal(raw.balance),
        wallets: raw.wallets.filter(Value::is_array).map(|wallets| {
            lenient_rows::<RawWallet>("balances.wallets", Some(wallets))
                .into_iter()
                .map(wallet_from_raw)
                .collect()
        }),
        amount: decimal(raw.amount),
        fee: decimal(raw.fee),
        ledger_id: raw.ledger_id,
        ref_id: raw.ref_id,
        timestamp: raw.timestamp,
        ledger_type: raw.ledger_type,
        wallet_type: raw.wallet_type,
        wallet_id: raw.wallet_id,
        is_snapshot,
        sequence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decodes_observed_futures_execution() {
        let update = decode_futures_executions(json!({"data": {
            "exec_type": "trade", "order_status": "filled", "symbol": "PF_XBTUSD",
            "side": "buy", "order_qty": "0.0001", "cum_qty": 0.0001,
            "last_qty": "0.0001", "last_price": "80262", "fee": "0.1",
            "sequence_id": 17
        }}))
        .expect("captured execution must decode");
        assert_eq!(update.exec_type, "trade");
        assert_eq!(update.symbol.as_deref(), Some("PF_XBTUSD"));
        assert_eq!(update.sequence_id.as_deref(), Some("17"));
        assert_eq!(update.cum_qty, Some(Decimal::new(1, 4)));
    }

    #[test]
    fn cum_qty_is_none_when_the_venue_omits_it_and_zero_when_it_sends_zero() {
        let conditional = decode_futures_executions(json!({"data": {
            "exec_type": "new", "order_status": "new", "symbol": "PF_XBTUSD", "side": "sell",
            "order_type": "limit", "order_qty": 0.0001, "limit_price": 71500
        }}))
        .expect("an untriggered stop-loss-limit record must decode");
        let plain = decode_futures_executions(json!({"data": {
            "exec_type": "new", "order_status": "new", "symbol": "PF_XBTUSD", "side": "buy",
            "order_type": "limit", "order_qty": 0.0001, "cum_qty": 0, "limit_price": 1
        }}))
        .expect("a resting limit record must decode");
        assert_eq!(conditional.cum_qty, None);
        assert_eq!(plain.cum_qty, Some(Decimal::ZERO));
    }

    #[test]
    fn execution_row_survives_mistyped_optional_fields_but_not_a_missing_exec_type() {
        let update = decode_futures_executions(json!({"data": {
            "exec_type": "trade", "order_id": 123, "side": true,
            "cl_ord_id": {"nested": 1}, "symbol": "PF_XBTUSD", "last_qty": "0.1"
        }}))
        .expect("a mistyped optional field must not drop the row");
        assert_eq!(update.exec_type, "trade");
        assert_eq!(update.order_id.as_deref(), Some("123"));
        assert_eq!(update.side.as_deref(), Some("true"));
        assert_eq!(update.cl_ord_id, None);
        assert_eq!(update.symbol.as_deref(), Some("PF_XBTUSD"));

        assert!(decode_futures_executions(json!({"data": {"order_id": "x"}})).is_none());
        assert!(decode_futures_executions(json!({"data": [1, 2]})).is_none());
    }

    #[test]
    fn position_and_balance_rows_survive_mistyped_optional_strings() {
        let positions = decode_futures_positions(json!({"data": {
            "time": "t", "futures": [{"tradeable": 7, "size": "1", "fill_time": false}]
        }}))
        .expect("a numeric tradeable must not drop the frame");
        assert_eq!(positions.futures[0].tradeable.as_deref(), Some("7"));
        assert_eq!(positions.futures[0].fill_time.as_deref(), Some("false"));

        let unparseable = decode_futures_positions(json!({"data": {
            "time": "t", "futures": [{"tradeable": "PF_XBTUSD", "size": "n/a", "entry_price": {"x": 1}}]
        }}))
        .expect("an unparseable decimal must not drop the frame");
        assert_eq!(unparseable.futures[0].size, None);
        assert_eq!(unparseable.futures[0].entry_price, None);

        let balance = decode_futures_balances(json!({"data": {
            "asset": "USD", "ledger_id": 99, "wallet_id": ["x"], "balance": 1
        }}))
        .expect("a mistyped ledger id must not drop the row");
        assert_eq!(balance.ledger_id.as_deref(), Some("99"));
        assert_eq!(balance.wallet_id, None);
    }

    #[test]
    fn decodes_observed_positions_and_margin() {
        let positions = decode_futures_positions(json!({"data": {
            "time": "2026-10-27T15:10:15.492860422Z",
            "futures": [{"tradeable": "PF_XBTUSD", "size": "0.0001",
                          "entry_price": "80262", "fill_time": "2026-10-27T15:10:15.492860422Z"}],
            "options": []
        }}))
        .expect("captured positions must decode");
        assert_eq!(positions.futures.len(), 1);
        assert_eq!(positions.futures[0].size, Some(Decimal::new(1, 4)));

        let margin = decode_futures_margin(json!({
            "type": "multi_collateral",
            "data": {"time": "2026-10-27T15:10:13Z", "available_margin": "12.5",
                     "cross": {"effective_leverage": "2.0"}, "isolated": []}
        }))
        .expect("captured margin must decode");
        let FuturesMarginUpdate::MultiCollateral(margin) = margin else {
            panic!("expected multi-collateral margin");
        };
        assert_eq!(margin.values.available_margin, Some(Decimal::new(125, 1)));
        assert_eq!(margin.cross.effective_leverage, Some(Decimal::new(20, 1)));
    }

    #[test]
    fn sparse_position_row_still_decodes() {
        let positions = decode_futures_positions(json!({"data": {
            "time": "2026-10-27T15:10:15Z",
            "futures": [{"tradeable": "PF_XBTUSD"}, {"tradeable": "PF_ETHUSD", "size": "2"}]
        }}))
        .expect("a row missing size and entry_price must not fail the frame");
        assert_eq!(positions.futures.len(), 2);
        assert_eq!(positions.futures[0].size, None);
        assert_eq!(positions.futures[1].size, Some(Decimal::new(2, 0)));
    }

    #[test]
    fn margin_values_accept_numbers_and_tolerate_unparseable_entries() {
        let margin = decode_futures_margin(json!({
            "type": "multi_collateral",
            "data": {"time": "t", "available_margin": 12.5, "pnl": "n/a",
                     "cross": {"effective_leverage": 2}}
        }))
        .expect("numeric and unparseable margin values must not fail the frame");
        let FuturesMarginUpdate::MultiCollateral(margin) = margin else {
            panic!("expected multi-collateral margin");
        };
        assert_eq!(margin.values.available_margin, Some(Decimal::new(125, 1)));
        assert_eq!(margin.values.pnl, None);
        assert_eq!(margin.cross.effective_leverage, Some(Decimal::new(2, 0)));
    }

    #[test]
    fn unmodelled_non_empty_arrays_are_ignored_without_failing_the_frame() {
        let positions = decode_futures_positions(json!({"data": {
            "time": "t", "futures": [], "options": [{"anything": 1}]
        }}))
        .expect("a populated options array must not fail the positions frame");
        assert!(positions.futures.is_empty());
    }

    fn single_collateral(data: Value) -> FuturesSingleCollateralMargin {
        match decode_futures_margin(json!({"type": "single_collateral", "data": data})) {
            Some(FuturesMarginUpdate::SingleCollateral(margin)) => margin,
            other => panic!("expected single-collateral margin, got {other:?}"),
        }
    }

    #[test]
    fn docs_shaped_single_collateral_frame_decodes_every_wallet() {
        let frame: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/private_futures_margin_single_collateral_docs_shaped.json"
        ))
        .expect("fixture JSON");
        let margin = single_collateral(frame["data"].clone());
        assert_eq!(margin.time, "2023-10-16T13:20:01.442310555Z");
        assert_eq!(margin.wallets.len(), 2);
        let xbt = &margin.wallets[0];
        assert_eq!(xbt.base_currency.as_deref(), Some("XBT"));
        assert_eq!(xbt.quote_currency.as_deref(), Some("USD"));
        assert_eq!(xbt.unit.as_deref(), Some("XBT"));
        assert_eq!(xbt.balance, Some(Decimal::new(1_219_368_845, 10)));
        assert_eq!(xbt.portfolio_value, Some(Decimal::new(1_231_447_102, 10)));
        assert_eq!(xbt.initial_margin, Some(Decimal::new(48_100_000, 10)));
        assert_eq!(xbt.maintenance_margin, Some(Decimal::new(24_050_000, 10)));
        assert_eq!(xbt.available, Some(Decimal::new(1_183_347_102, 10)));
        assert_eq!(xbt.unrealized_funding, Some(Decimal::new(418_257, 10)));
        assert_eq!(xbt.pnl, Some(Decimal::new(11_660_000, 10)));
        assert_eq!(margin.wallets[1].quote_currency.as_deref(), Some("EUR"));
    }

    #[test]
    fn single_collateral_skips_malformed_wallets_and_keeps_the_rest() {
        let margin = single_collateral(json!({"time": "t", "wallets": [
            {"unit": "XBT", "balance": "1"},
            "not a wallet",
            {"unit": 5, "balance": "n/a", "pnl": 0.5}
        ]}));
        assert_eq!(margin.wallets.len(), 2);
        assert_eq!(margin.wallets[0].balance, Some(Decimal::ONE));
        assert_eq!(margin.wallets[1].unit.as_deref(), Some("5"));
        assert_eq!(margin.wallets[1].balance, None);
        assert_eq!(margin.wallets[1].pnl, Some(Decimal::new(5, 1)));

        assert!(single_collateral(json!({"time": "t"})).wallets.is_empty());
        assert!(
            single_collateral(json!({"time": "t", "wallets": "x"}))
                .wallets
                .is_empty()
        );
    }

    #[test]
    fn captured_preferences_snapshot_decodes_to_the_permission() {
        let frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/private_preferences_snapshot.json"
        ))
        .expect("checked-in preferences fixture is valid JSON");
        let snapshot = frames
            .into_iter()
            .find(|frame| frame["channel"] == "preferences")
            .expect("snapshot frame");

        let update = decode_futures_preferences(snapshot).expect("snapshot decodes");
        assert_eq!(
            update.futures_trading,
            Some(FuturesTradingPermission::Enabled)
        );
    }

    #[test]
    fn preferences_permission_values_decode_with_a_catch_all() {
        for (wire, want) in [
            ("enabled", FuturesTradingPermission::Enabled),
            ("reduce_only", FuturesTradingPermission::ReduceOnly),
            ("disabled", FuturesTradingPermission::Disabled),
            ("pending_review", FuturesTradingPermission::Unknown),
        ] {
            let update = decode_futures_preferences(json!({
                "type": "update",
                "data": {"futures_trading": wire}
            }))
            .expect("update decodes");
            assert_eq!(update.futures_trading, Some(want), "wire `{wire}`");
        }

        let absent = decode_futures_preferences(json!({"data": {"other_setting": 1}}))
            .expect("a frame without the field still decodes");
        assert_eq!(absent.futures_trading, None);
        assert!(decode_futures_preferences(json!({"data": "enabled"})).is_none());
    }

    #[test]
    fn preferences_with_a_mistyped_permission_survive_as_unknown() {
        for wire in [json!(1), json!(true), json!(["enabled"]), json!({"a": 1})] {
            let update = decode_futures_preferences(json!({"data": {"futures_trading": wire}}))
                .expect("a mistyped permission must not drop the frame");
            assert_eq!(
                update.futures_trading,
                Some(FuturesTradingPermission::Unknown)
            );
        }
        let null = decode_futures_preferences(json!({"data": {"futures_trading": null}}))
            .expect("a null permission decodes");
        assert_eq!(null.futures_trading, None);
    }

    #[test]
    fn preferences_without_a_permission_serialize_without_the_key() {
        let update = FuturesPreferencesUpdate {
            futures_trading: None,
        };
        assert_eq!(serde_json::to_value(update).expect("serializes"), json!({}));
    }

    #[test]
    fn decodes_balance_update_ledger_fields() {
        let update = decode_futures_balances(json!({"data": {
            "ledger_id": "L1", "ref_id": "R1", "timestamp": "2026-10-28T13:44:20.130163Z",
            "type": "derivativesfuturestrade", "asset": "USD", "asset_class": "currency",
            "wallet_type": "spot", "wallet_id": "W1", "amount": 0.0, "fee": 0.001,
            "balance": 100.5
        }}))
        .expect("a ledger-shaped balances update must decode");
        assert_eq!(update.asset.as_deref(), Some("USD"));
        assert_eq!(update.balance, Some(Decimal::new(1005, 1)));
        assert_eq!(update.fee, Some(Decimal::new(1, 3)));
        assert_eq!(update.amount, Some(Decimal::ZERO));
        assert_eq!(
            update.ledger_type.as_deref(),
            Some("derivativesfuturestrade")
        );
        assert_eq!(update.ledger_id.as_deref(), Some("L1"));
        assert_eq!(update.ref_id.as_deref(), Some("R1"));
        assert_eq!(update.wallet_type.as_deref(), Some("spot"));
        assert_eq!(update.wallet_id.as_deref(), Some("W1"));
        assert!(update.wallets.is_none());
    }

    #[test]
    fn decodes_shared_balance_snapshot() {
        let update = decode_futures_balances(json!({"data": {
            "asset": "USD", "asset_class": "currency", "balance": 1.5,
            "wallets": [{"type": "spot", "id": "redacted", "balance": "1.5"}]
        }}))
        .expect("captured balance must decode");
        assert_eq!(update.asset.as_deref(), Some("USD"));
        assert_eq!(update.wallets.as_ref().map(Vec::len), Some(1));
    }

    #[test]
    fn redacted_venue_fixture_decodes_all_account_channels() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/private_futures_account_updates.json"
        ))
        .expect("fixture JSON");
        let execution =
            decode_futures_executions(json!({"data": fixture["executions"]["data"][0]}))
                .expect("execution");
        assert_eq!(execution.exec_type, "trade");
        assert_eq!(execution.symbol.as_deref(), Some("PF_XBTUSD"));

        let positions = decode_futures_positions(fixture["positions"].clone()).expect("positions");
        assert_eq!(positions.futures[0].tradeable.as_deref(), Some("PF_XBTUSD"));

        let margin = decode_futures_margin(fixture["margin"].clone()).expect("margin");
        assert!(matches!(margin, FuturesMarginUpdate::MultiCollateral(_)));

        let balance = decode_futures_balances(json!({"data": fixture["balances"]["data"][0]}))
            .expect("balance");
        assert_eq!(balance.asset.as_deref(), Some("USD"));

        let update =
            decode_futures_balances(json!({"data": fixture["balances_update"]["data"][0]}))
                .expect("balance update");
        assert_eq!(
            update.ledger_type.as_deref(),
            Some("derivativesfuturestrade")
        );
        assert_eq!(update.fee, Some(Decimal::new(1, 3)));
    }

    #[test]
    fn captured_stop_loss_snapshot_and_update_carry_triggers() {
        let frames: Vec<Value> = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/private_futures_execution_triggers.json"
        ))
        .expect("fixture JSON");
        assert_eq!(frames.len(), 2, "snapshot and update frames");
        let expected = ExecutionTriggers {
            reference: Some("last".into()),
            price: Some(Decimal::new(146_552, 0)),
            price_type: Some("static".into()),
            actual_price: None,
            peak_price: None,
            last_price: None,
            status: Some("untriggered".into()),
            timestamp: None,
        };
        for (frame, snapshot) in frames.iter().zip([true, false]) {
            let update =
                decode_futures_executions(json!({"type": frame["type"], "data": frame["data"][0]}))
                    .expect("stop-loss execution decodes");
            assert_eq!(update.is_snapshot, snapshot);
            assert_eq!(update.order_type.as_deref(), Some("stop-loss"));
            assert_eq!(update.triggers.as_ref(), Some(&expected));
        }
    }

    #[test]
    fn triggered_trailing_fields_decode_and_mistyped_triggers_keep_the_row() {
        let triggered = decode_futures_executions(json!({"data": {
            "exec_type": "trade",
            "triggers": {"reference": "index", "price": "5", "price_type": "pct",
                         "actual_price": 101.5, "peak_price": "107", "last_price": 101,
                         "status": "triggered", "timestamp": "2026-11-11T09:00:00Z"}
        }}))
        .expect("triggered row")
        .triggers
        .expect("triggers");
        assert_eq!(triggered.actual_price, Some(Decimal::new(1015, 1)));
        assert_eq!(triggered.peak_price, Some(Decimal::new(107, 0)));
        assert_eq!(triggered.last_price, Some(Decimal::new(101, 0)));
        assert_eq!(triggered.status.as_deref(), Some("triggered"));
        assert_eq!(triggered.timestamp.as_deref(), Some("2026-11-11T09:00:00Z"));

        let not_an_object = decode_futures_executions(json!({"data": {
            "exec_type": "new", "triggers": "last"
        }}))
        .expect("a non-object triggers must not drop the row");
        assert_eq!(not_an_object.triggers, None);

        let mistyped = decode_futures_executions(json!({"data": {
            "exec_type": "new", "triggers": {"price": "n/a", "status": 1, "reference": ["x"]}
        }}))
        .expect("mistyped trigger fields must not drop the row")
        .triggers
        .expect("the object still decodes");
        assert_eq!(mistyped.price, None);
        assert_eq!(mistyped.status.as_deref(), Some("1"));
        assert_eq!(mistyped.reference, None);

        let plain =
            decode_futures_executions(json!({"data": {"exec_type": "new"}})).expect("plain row");
        assert_eq!(plain.triggers, None);
    }

    #[test]
    fn captured_reduce_only_snapshot_row_reports_the_flag() {
        let frame: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/unified_ws/private_futures_execution_reduce_only.json"
        ))
        .expect("fixture JSON");
        let update =
            decode_futures_executions(json!({"type": frame["type"], "data": frame["data"][0]}))
                .expect("reduce-only execution decodes");
        assert!(update.is_snapshot);
        assert_eq!(update.reduce_only, Some(true));
        assert_eq!(update.post_only, None);
        assert_eq!(update.time_in_force, None);
        assert_eq!(update.limit_price, Some(Decimal::new(85_000, 0)));
    }

    #[test]
    fn order_option_fields_decode_and_mistyped_flags_keep_the_row() {
        let update = decode_futures_executions(json!({"data": {
            "exec_type": "new", "order_type": "limit", "time_in_force": "IOC",
            "reduce_only": false, "post_only": true
        }}))
        .expect("documented order options decode");
        assert_eq!(update.time_in_force.as_deref(), Some("IOC"));
        assert_eq!(update.reduce_only, Some(false));
        assert_eq!(update.post_only, Some(true));

        let mistyped = decode_futures_executions(json!({"data": {
            "exec_type": "new", "time_in_force": 7, "reduce_only": "true", "post_only": 1
        }}))
        .expect("mistyped order options must not drop the row");
        assert_eq!(mistyped.time_in_force.as_deref(), Some("7"));
        assert_eq!(mistyped.reduce_only, None);
        assert_eq!(mistyped.post_only, None);
    }

    #[test]
    fn private_rows_carry_the_frame_type_and_balances_the_frame_sequence() {
        let balance = |ty: &str, sequence: Option<u64>| {
            decode_futures_balances(json!({
                "type": ty, "sequence": sequence, "data": {"asset": "USD", "balance": 1}
            }))
            .expect("balance")
        };
        let snapshot = balance("snapshot", Some(1));
        assert!(snapshot.is_snapshot);
        assert_eq!(snapshot.sequence, Some(1));
        let update = balance("update", Some(2));
        assert!(!update.is_snapshot);
        assert_eq!(update.sequence, Some(2));
        assert_eq!(balance("update", None).sequence, None);

        let execution = |ty: &str| {
            decode_futures_executions(json!({"type": ty, "data": {"exec_type": "new"}}))
                .expect("execution")
        };
        assert!(execution("snapshot").is_snapshot);
        assert!(!execution("update").is_snapshot);

        let positions = |ty: &str| {
            decode_futures_positions(json!({"type": ty, "data": {"time": "t", "futures": []}}))
                .expect("positions")
        };
        assert!(positions("snapshot").is_snapshot);
        assert!(!positions("update").is_snapshot);
    }

    #[test]
    fn margin_frame_carries_the_isolated_positions_and_skips_a_malformed_one() {
        let isolated = |rows: Value| {
            let margin = decode_futures_margin(json!({
                "type": "multi_collateral",
                "data": {"time": "t", "cross": {}, "isolated": rows}
            }))
            .expect("margin decodes");
            let FuturesMarginUpdate::MultiCollateral(margin) = margin else {
                panic!("expected multi-collateral margin");
            };
            margin.isolated
        };

        let rows = isolated(json!([
            {
                "tradeable": "PF_XBTUSD",
                "initial_margin": "0.80292",
                "initial_margin_without_orders": "0.80292",
                "maintenance_margin": "0.04014600000000001",
                "pnl": "-0.00805",
                "total_unrealized": "-0.00804999999946847",
                "total_unrealized_as_margin": "-0.00804999999946847",
                "unrealized_funding": "0.000000000000531529789923611"
            },
            "not an object"
        ]));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tradeable.as_deref(), Some("PF_XBTUSD"));
        assert_eq!(rows[0].values.initial_margin, Some(Decimal::new(80292, 5)));
        assert_eq!(rows[0].values.pnl, Some(Decimal::new(-805, 5)));
        assert_eq!(rows[0].values.available_margin, None);

        assert!(isolated(json!([])).is_empty());
    }

    #[test]
    fn positions_frame_survives_a_null_or_malformed_futures_container() {
        let rows = |futures: Value| {
            decode_futures_positions(json!({"data": {"time": "t", "futures": futures}}))
                .expect("a mistyped futures container must not drop the frame")
                .futures
        };

        assert!(rows(json!(null)).is_empty());
        assert!(rows(json!("x")).is_empty());
        let mixed = rows(json!(["not an object", {"tradeable": "PF_XBTUSD", "size": "1"}, 5]));
        assert_eq!(mixed.len(), 1);
        assert_eq!(mixed[0].tradeable.as_deref(), Some("PF_XBTUSD"));
    }

    #[test]
    fn balance_row_survives_a_mistyped_wallet_entry() {
        let update = decode_futures_balances(json!({"data": {
            "asset": "USD", "balance": "1.5",
            "wallets": [
                {"type": "spot", "id": 7, "balance": "1"},
                "not an object",
                {"type": "margin", "id": "w2", "balance": "0.5"}
            ]
        }}))
        .expect("a mistyped wallet must not drop the balance row");
        assert_eq!(update.asset.as_deref(), Some("USD"));
        assert_eq!(update.balance, Some(Decimal::new(15, 1)));
        let wallets = update.wallets.expect("wallets present");
        assert_eq!(wallets.len(), 1);
        assert_eq!(wallets[0].id.as_deref(), Some("w2"));

        let scalar = decode_futures_balances(json!({"data": {
            "asset": "USD", "wallets": 5
        }}))
        .expect("a non-array wallets must not drop the balance row");
        assert_eq!(scalar.wallets, None);
    }

    #[test]
    fn isolated_margin_row_survives_a_mistyped_tradeable() {
        let margin = decode_futures_margin(json!({
            "type": "multi_collateral",
            "data": {"time": "t", "cross": {}, "isolated": [{"tradeable": 7, "pnl": "1"}]}
        }))
        .expect("margin decodes");
        let FuturesMarginUpdate::MultiCollateral(margin) = margin else {
            panic!("expected multi-collateral margin");
        };
        assert_eq!(margin.isolated.len(), 1);
        assert_eq!(margin.isolated[0].tradeable.as_deref(), Some("7"));
    }

    #[test]
    fn margin_frame_survives_a_non_array_isolated() {
        for bad in [json!(null), json!({}), json!("x"), json!(5)] {
            let margin = decode_futures_margin(json!({
                "type": "multi_collateral",
                "data": {"time": "t", "cross": {}, "isolated": bad}
            }))
            .expect("a mistyped isolated must not fail the margin frame");
            let FuturesMarginUpdate::MultiCollateral(margin) = margin else {
                panic!("expected multi-collateral margin");
            };
            assert!(margin.isolated.is_empty());
        }
    }

    #[test]
    fn isolated_margin_serializes_without_nulls() {
        let row = FuturesIsolatedMargin {
            tradeable: None,
            values: serde_json::from_value(json!({})).expect("empty margin values"),
        };
        let value = serde_json::to_value(&row).expect("serializes");
        assert!(value.get("tradeable").is_none());
    }
}
