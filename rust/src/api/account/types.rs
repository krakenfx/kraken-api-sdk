//! Account-namespace response data types and their `Raw*` decoders.

use std::collections::HashMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::error::AccountError;
use crate::api::trade::{OrderType, Side, TimeInForce, TriggerKind};
use crate::types::{AssetCode, ClOrdId, TxId};
use serde_with::skip_serializing_none;

/// Lifecycle bucket an order was found in during the reconciliation walk.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    strum::Display,
    strum::AsRefStr,
    strum::IntoStaticStr,
)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum LifecyclePosition {
    /// OpenOrders leg — wire `"open"`.
    Open,
    /// ClosedOrders leg — wire `"closed"`.
    Closed,
}

/// Result of the `find_order_by_cl_ord_id` reconciliation walk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationOutcome {
    /// Order located — resolved in the OpenOrders or ClosedOrders leg.
    Found {
        /// Exchange transaction id of the matched order.
        txid: TxId,
        /// Order lifecycle status at the time of the match.
        status: OrderStatus,
        /// Which leg (open or closed) the order was found in.
        lifecycle: LifecyclePosition,
    },
    /// Neither OpenOrders nor ClosedOrders matched the client order id.
    NotPlaced,
    /// A leg failed after its retry budget — outcome undetermined; re-walk.
    Unknown,
}

/// Kraken REST timestamp — fractional Unix epoch seconds, kept verbatim as a
/// string to avoid `f64` precision loss. See docs/guides/wire-quirks.md.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct KrakenTimestamp(String);

impl KrakenTimestamp {
    /// The verbatim wire representation (e.g. `"1716800100.5"`).
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Lossy parse to `f64`; `None` unless finite. Prefer [`as_str`](Self::as_str) for exact reconciliation.
    #[must_use]
    pub fn to_f64(&self) -> Option<f64> {
        self.0.parse::<f64>().ok().filter(|f| f.is_finite())
    }
}

impl<'de> Deserialize<'de> for KrakenTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Some timestamps arrive string-wrapped; keep the wire token verbatim.
        match Value::deserialize(deserializer)? {
            Value::Number(n) => Ok(Self(n.to_string())),
            Value::String(s) => Ok(Self(s)),
            _ => Err(<D::Error as serde::de::Error>::custom(
                "KrakenTimestamp: expected a JSON number or string",
            )),
        }
    }
}

/// Order lifecycle status — lowercase wire per OpenOrders/ClosedOrders.
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
pub enum OrderStatus {
    /// Not yet on the book — wire `"pending"`.
    Pending,
    /// Live on the book — wire `"open"`.
    Open,
    /// Fully executed — wire `"closed"`.
    Closed,
    /// Cancelled (possibly after a partial fill) — wire `"canceled"`.
    Canceled,
    /// Expired via its expiration time — wire `"expired"`.
    Expired,
    /// Unrecognized wire value — forward-compatible catch-all.
    #[serde(other)]
    Unknown,
}

/// Margin-position status — lowercase wire.
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
pub enum PositionStatus {
    /// Wire `"open"`.
    Open,
    /// Wire `"closed"`.
    Closed,
}

/// Ledger-entry type — lowercase wire per Ledgers.
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
pub enum LedgerType {
    /// Wire `"staking"`.
    Staking,
    /// Wire `"transfer"`.
    Transfer,
    /// Wire `"trade"`.
    Trade,
    /// Wire `"deposit"`.
    Deposit,
    /// Wire `"withdrawal"`.
    Withdrawal,
    /// Wire `"margin"`.
    Margin,
    /// Wire `"rollover"`.
    Rollover,
    /// Wire `"adjustment"`.
    Adjustment,
    /// Wire `"conversion"`.
    Conversion,
    /// Wire `"reward"`.
    Reward,
    /// Wire `"dividend"`.
    Dividend,
    /// Wire `"sale"`.
    Sale,
    /// Unrecognized wire value — forward-compatible catch-all.
    #[serde(other)]
    Unknown,
}

/// Timestamp used to filter closed orders — wire key `closetime`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
pub enum CloseTime {
    /// Filter by when the order opened — wire `"open"`.
    Open,
    /// Filter by when the order closed — wire `"close"`.
    Close,
    /// Filter by either timestamp — wire `"both"`.
    Both,
}

/// Position-state filter for trade history — wire key `type`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
pub enum TradeTypeFilter {
    /// All trades — wire `"all"`.
    #[strum(serialize = "all")]
    All,
    /// Trades associated with any position — wire `"any position"`.
    #[strum(serialize = "any position")]
    AnyPosition,
    /// Trades associated with closed positions — wire `"closed position"`.
    #[strum(serialize = "closed position")]
    ClosedPosition,
    /// Trades closing a position — wire `"closing position"`.
    #[strum(serialize = "closing position")]
    ClosingPosition,
    /// Trades with no position — wire `"no position"`.
    #[strum(serialize = "no position")]
    NoPosition,
}

/// Outbound ledger-entry filter — wire key `type`.
///
/// This deliberately differs from response-side [`LedgerType`]: outbound
/// filters have no catch-all because an unknown token cannot be serialized.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
pub enum LedgerTypeFilter {
    /// All ledger entry types — wire `"all"`.
    All,
    /// Trade entries — wire `"trade"`.
    Trade,
    /// Deposit entries — wire `"deposit"`.
    Deposit,
    /// Withdrawal entries — wire `"withdrawal"`.
    Withdrawal,
    /// Transfer entries — wire `"transfer"`.
    Transfer,
    /// Margin entries — wire `"margin"`.
    Margin,
    /// Adjustment entries — wire `"adjustment"`.
    Adjustment,
    /// Rollover entries — wire `"rollover"`.
    Rollover,
    /// Credit entries — wire `"credit"`.
    Credit,
    /// Settled entries — wire `"settled"`.
    Settled,
    /// Staking entries — wire `"staking"`.
    Staking,
    /// Dividend entries — wire `"dividend"`.
    Dividend,
    /// Sale entries — wire `"sale"`.
    Sale,
    /// NFT rebate entries — wire `"nft_rebate"`.
    #[strum(serialize = "nft_rebate")]
    NftRebate,
}

/// Per-asset balances keyed by modern asset codes (`BTC`, not `XXBT`) — legacy
/// wire codes are stripped at the SDK boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Balance {
    /// Asset → total balance amount; keys are modern codes (`BTC`, not `XXBT`).
    pub assets: HashMap<AssetCode, Decimal>,
}

impl Balance {
    pub(super) fn from_value(result: Value) -> Result<Self, AccountError> {
        let obj = result
            .as_object()
            .ok_or_else(|| AccountError::malformed("result not an object".into()))?;

        let mut assets = HashMap::with_capacity(obj.len());
        for (asset, raw) in obj {
            let s = raw.as_str().ok_or_else(|| {
                AccountError::malformed(format!("balance value for {} not string", asset))
            })?;
            let amount = s
                .parse::<Decimal>()
                .map_err(|e| AccountError::malformed(format!("balance decode {}: {}", asset, e)))?;
            assets.insert(AssetCode::from_wire(asset), amount);
        }
        Ok(Self { assets })
    }
}

/// Extended per-asset balances, keyed like [`Balance`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct ExtendedBalance {
    /// Asset → extended balance entry; keys are modern codes (`BTC`, not `XXBT`).
    pub assets: HashMap<AssetCode, ExtendedBalanceEntry>,
}

/// Per-asset extended balance. `credit`/`credit_used` appear only for
/// credit-enabled keys — plain Spot keys omit both (absent, not null/zero).
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct ExtendedBalanceEntry {
    /// Total balance of the asset.
    pub balance: Decimal,
    /// Amount on hold for open orders and pending trades.
    pub hold_trade: Decimal,
    /// Present only for credit-enabled keys.
    pub credit: Option<Decimal>,
    /// Present only for credit-enabled keys.
    pub credit_used: Option<Decimal>,
}

impl ExtendedBalanceEntry {
    /// Log-safe view: `Display` renders every amount as `<redacted>` — log this,
    /// never the raw entry.
    ///
    /// ```
    /// # use kraken_sdk::ExtendedBalanceEntry;
    /// # fn ex(entry: &ExtendedBalanceEntry) {
    /// tracing::info!(balance = %entry.masked(), "extended balance");
    /// # }
    /// ```
    pub fn masked(&self) -> MaskedBalance<'_> {
        MaskedBalance(self)
    }
}

/// Log-safe `Display` wrapper over an [`ExtendedBalanceEntry`]; renders each amount as `<redacted>`.
pub struct MaskedBalance<'a>(&'a ExtendedBalanceEntry);

impl std::fmt::Display for MaskedBalance<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "balance=<redacted> hold_trade=<redacted>")?;
        if self.0.credit.is_some() {
            write!(f, " credit=<redacted>")?;
        }
        if self.0.credit_used.is_some() {
            write!(f, " credit_used=<redacted>")?;
        }
        Ok(())
    }
}

impl ExtendedBalance {
    pub(super) fn from_value(result: Value) -> Result<Self, AccountError> {
        let Value::Object(map) = result else {
            return Err(AccountError::malformed(
                "extended_balance: result not an object".into(),
            ));
        };

        let mut assets = HashMap::with_capacity(map.len());
        for (asset, raw) in map {
            let entry: ExtendedBalanceEntry = serde_json::from_value(raw).map_err(|e| {
                AccountError::malformed(format!("extended_balance decode {}: {}", asset, e))
            })?;
            assets.insert(AssetCode::from_wire(&asset), entry);
        }
        Ok(Self { assets })
    }
}

/// Consolidated trade balance. Margin-only wire keys (`ml`/`uv`/`mfo`) are
/// absent when no margin position is open — decoded as `Option<Decimal>`.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct TradeBalance {
    /// Combined balance of all currencies — wire key `eb`.
    #[serde(rename(deserialize = "eb"))]
    pub equivalent_balance: Decimal,
    /// Combined balance of all equity currencies — wire key `tb`.
    #[serde(rename(deserialize = "tb"))]
    pub trade_balance: Decimal,
    /// Margin amount of open positions — wire key `m`.
    #[serde(rename(deserialize = "m"))]
    pub margin_amount: Decimal,
    /// Unrealized net profit/loss of open positions — wire key `n`.
    #[serde(rename(deserialize = "n"))]
    pub unrealized_pnl: Decimal,
    /// Cost basis of open positions — wire key `c`.
    #[serde(rename(deserialize = "c"))]
    pub cost_basis: Decimal,
    /// Current floating valuation of open positions — wire key `v`.
    #[serde(rename(deserialize = "v"))]
    pub floating_valuation: Decimal,
    /// Equity — trade balance plus unrealized profit/loss; wire key `e`.
    #[serde(rename(deserialize = "e"))]
    pub equity: Decimal,
    /// Free margin — equity minus initial margin of open positions; wire key `mf`.
    #[serde(rename(deserialize = "mf"))]
    pub free_margin: Decimal,
    /// Margin level percent — absent when no margin position is open.
    #[serde(rename(deserialize = "ml"))]
    pub margin_level_pct: Option<Decimal>,
    /// Undocumented; absent on non-margin accounts.
    #[serde(rename(deserialize = "uv"))]
    pub unrealized_value: Option<Decimal>,
    /// Undocumented; absent on non-margin accounts.
    #[serde(rename(deserialize = "mfo"))]
    pub free_margin_original: Option<Decimal>,
}

/// Structured `descr` on each OpenOrders/ClosedOrders row.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct OrderDescr {
    /// Wire pair string — the legacy wire form, e.g. `"XBTUSDC"` (NOT `"BTC/USDC"`).
    pub pair: String,
    /// Asset class — wire `"forex"` for crypto/forex.
    pub aclass: String,
    /// Order side. Wire key `type`.
    #[serde(rename(deserialize = "type"))]
    pub side: Side,
    /// Order type (market/limit/stop-loss…) — kebab-case wire.
    pub ordertype: OrderType,
    /// Limit / primary price; Kraken emits `"0"` for market orders.
    #[serde(default)]
    pub price: Decimal,
    /// Secondary price (stop-loss-limit etc.). `"0"` when not applicable.
    #[serde(default)]
    pub price2: Decimal,
    /// Leverage. Stays a `String`: the wire sends `"none"` for non-margin, `"5:1"` for margin.
    pub leverage: String,
    /// Kraken's human-readable order string
    /// (e.g. `"buy 0.5 XBTUSD @ limit 50000"`).
    pub order: String,
    /// Human-readable conditional-close string; the wire always sends this key —
    /// `""` (not absent) when no `close[*]` clause was attached.
    #[serde(default)]
    pub close: String,
}

/// OpenOrders snapshot — currently open orders keyed by txid.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct OpenOrders {
    /// Open orders keyed by txid string — wire key `open`.
    pub open: HashMap<String, OrderInfo>,
}

/// One ClosedOrders page — closed/cancelled orders keyed by txid, plus the
/// total match count for pagination.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct ClosedOrders {
    /// Closed orders keyed by txid string — wire key `closed`.
    pub closed: HashMap<String, OrderInfo>,
    /// Total records matching the filter (across all pages).
    pub count: u64,
}

/// One OpenOrders or ClosedOrders row — one unified type for both;
/// `closetm`/`reason` are ClosedOrders-only (`None` on open rows).
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct OrderInfo {
    /// Referral order transaction id; `None` when the wire sends null.
    pub refid: Option<String>,
    /// Legacy numeric user reference; `None` when the wire sends null.
    pub userref: Option<i32>,
    /// First-class client order id — present on every row.
    pub cl_ord_id: Option<ClOrdId>,
    /// Order lifecycle status — lowercase wire.
    pub status: OrderStatus,
    /// When the order was placed — Unix epoch seconds.
    pub opentm: KrakenTimestamp,
    /// ClosedOrders-only; `None` on open rows.
    pub closetm: Option<KrakenTimestamp>,
    /// Scheduled start time; the wire sends `0` when there is none.
    pub starttm: Option<KrakenTimestamp>,
    /// Expiration time; the wire sends `0` when the order does not expire.
    pub expiretm: Option<KrakenTimestamp>,
    /// Structured order description (pair, side, prices, human string).
    pub descr: OrderDescr,
    /// Order volume in base asset — wire key `vol`.
    #[serde(rename(deserialize = "vol"))]
    pub volume: Decimal,
    /// Executed volume in base asset — wire key `vol_exec`.
    #[serde(rename(deserialize = "vol_exec"))]
    pub volume_executed: Decimal,
    /// Total cost in quote asset; `0` until filled.
    pub cost: Decimal,
    /// Total fee in quote asset; `0` until filled.
    pub fee: Decimal,
    /// Average fill price in quote asset; `0` until filled — the order's own price is `descr.price`.
    pub price: Decimal,
    /// Trigger stop price in quote asset; `None` when absent from the row.
    pub stopprice: Option<Decimal>,
    /// Triggered limit price in quote asset; `None` when absent from the row.
    pub limitprice: Option<Decimal>,
    /// Comma-separated miscellaneous info flags — wire string kept verbatim.
    pub misc: String,
    /// Comma-separated wire form — NOT parsed to a flag set here.
    pub oflags: String,
    /// Present only when `vol_exec > 0`; `None` otherwise.
    pub trades: Option<Vec<TxId>>,
    /// First-class time-in-force; lowercase wire enum (`gtc`/`ioc`/`gtd`/`fok`).
    #[serde(default = "default_time_in_force")]
    pub time_in_force: TimeInForce,
    /// ClosedOrders-only free-text close reason; `None` on open rows.
    pub reason: Option<String>,
    /// Trigger reference price (`last` | `index`) on stop/take-profit rows.
    #[serde(default, deserialize_with = "de_trigger")]
    pub trigger: Option<TriggerKind>,
}

/// An unrecognized or non-string value maps to `None`, never sinking the row.
fn de_trigger<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<TriggerKind>, D::Error> {
    let opt = Option::<serde_json::Value>::deserialize(d)?;
    Ok(opt
        .as_ref()
        .and_then(serde_json::Value::as_str)
        .and_then(TriggerKind::from_wire_str))
}

/// Kraken's documented default when `time_in_force` is absent on a row.
fn default_time_in_force() -> TimeInForce {
    TimeInForce::Gtc
}

/// One Ledgers page — ledger entries keyed by ledger id, plus the total
/// match count for pagination.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Ledgers {
    /// Ledger entries keyed by ledger id string — wire key `ledger`.
    pub ledger: HashMap<String, LedgerEntry>,
    /// Total records matching the filter (across all pages).
    pub count: u64,
}

fn de_asset_from_wire<'de, D>(d: D) -> Result<AssetCode, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    Ok(AssetCode::from_wire(&s))
}

/// One Ledgers row; `asset` is normalised to the modern code on decode.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct LedgerEntry {
    /// Reference id of the event (trade, deposit, …) that produced the entry.
    pub refid: String,
    /// When the entry was posted — Unix epoch seconds.
    pub time: KrakenTimestamp,
    /// Entry type — wire key `type`, decoded to the typed enum.
    #[serde(rename(deserialize = "type"))]
    pub ledger_type: LedgerType,
    /// Free-text subtype qualifier — optional on the wire.
    pub subtype: Option<String>,
    /// Asset class — wire string (`"currency"`), passed through verbatim.
    pub aclass: String,
    /// Asset — normalised to the modern code (`BTC`, not `XXBT`) on decode.
    #[serde(deserialize_with = "de_asset_from_wire")]
    pub asset: AssetCode,
    /// Signed amount of the entry in `asset` — negative for debits.
    pub amount: Decimal,
    /// Fee charged on the entry, in `asset`.
    pub fee: Decimal,
    /// Resulting `asset` balance after this entry.
    pub balance: Decimal,
}

/// One TradesHistory page, keyed by txid. `count` is the total across all pages,
/// not `trades.len()` — paginate via `ofs` when `count > trades.len()`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct TradesHistorySnapshot {
    /// Current page of fills keyed by trade txid.
    pub trades: HashMap<String, TradeHistoryEntry>,
    /// Total records matching the filter (across all pages).
    pub count: u64,
}

/// One TradesHistory row. Carries several wire anomalies vs other endpoints —
/// see field docs and docs/guides/wire-quirks.md.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct TradeHistoryEntry {
    /// Txid of the order this fill executed against.
    pub ordertxid: TxId,
    /// Position txid — always present, even for spot.
    pub postxid: TxId,
    /// Present only on margin trades; wire value `"open"`.
    pub posstatus: Option<PositionStatus>,
    /// Wire pair string — legacy wire form (e.g. `"XBTUSDC"`), not `"BTC/USDC"`.
    pub pair: String,
    /// Asset class — wire key `aclass` on this endpoint, passed through verbatim.
    pub aclass: String,
    /// Fill time — Unix epoch seconds.
    pub time: KrakenTimestamp,
    /// Trade side. Wire key `type`.
    #[serde(rename(deserialize = "type"))]
    pub side: Side,
    /// Order type of the originating order — kebab-case wire.
    pub ordertype: OrderType,
    /// Differs on triggered (stop/take-profit) fills.
    pub tradeordertype: OrderType,
    /// Execution price in quote asset.
    pub price: Decimal,
    /// Total cost of the fill in quote asset.
    pub cost: Decimal,
    /// Fee charged on the fill in quote asset.
    pub fee: Decimal,
    /// Fill volume in base asset.
    pub vol: Decimal,
    /// `0.00000` for spot.
    pub margin: Decimal,
    /// `0` for spot; e.g. `2` for margin. Clean `Decimal` here.
    pub leverage: Decimal,
    /// Comma-separated miscellaneous info flags — wire string kept verbatim.
    pub misc: String,
    /// JSON integer (not string) — wire anomaly on this endpoint.
    pub trade_id: u64,
    /// JSON boolean (not string) — wire anomaly on this endpoint.
    pub maker: bool,
    /// Related ledger-entry ids — present only when requested via
    /// `trades_history(.., ledgers: true)`; two legs per spot fill.
    pub ledgers: Option<Vec<String>>,
}

/// OpenPositions snapshot — a bare `result` map keyed by position id, with no
/// `open` wrapper key (unlike OpenOrders).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct OpenPositions {
    /// Open margin positions keyed by position id (the bare `result` map).
    pub positions: HashMap<String, OpenPositionEntry>,
}

/// One OpenPositions row. Carries wire anomalies unique to this endpoint —
/// see field docs and docs/guides/wire-quirks.md.
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct OpenPositionEntry {
    /// The order that opened the position.
    pub ordertxid: TxId,
    /// Non-optional here (wire `open`).
    pub posstatus: PositionStatus,
    /// Wire pair string — legacy wire form (e.g. `"XBTUSDC"`), not `"BTC/USDC"`.
    pub pair: String,
    /// Wire key is `class`, not `aclass` — anomaly vs every other endpoint.
    #[serde(rename(deserialize = "class"))]
    pub asset_class: String,
    /// When the position was opened — Unix epoch seconds.
    pub time: KrakenTimestamp,
    /// Position side. Wire key `type`.
    #[serde(rename(deserialize = "type"))]
    pub side: Side,
    /// Order type of the opening order — kebab-case wire.
    pub ordertype: OrderType,
    /// Opening cost of the position in quote asset.
    pub cost: Decimal,
    /// Opening fee in quote asset.
    pub fee: Decimal,
    /// Position volume in base asset.
    pub vol: Decimal,
    /// Volume already closed, in base asset.
    pub vol_closed: Decimal,
    /// Initial margin consumed by the position, in quote asset.
    pub margin: Decimal,
    /// Free-text English (non-machine-parseable), e.g. `"0.0100% per 4 hours"`.
    pub terms: String,
    /// JSON string of a unix-seconds integer (not f64) — kept as `String`,
    /// not parsed to a timestamp.
    pub rollovertm: String,
    /// Comma-separated miscellaneous info flags — wire string kept verbatim.
    pub misc: String,
    /// Comma-separated wire form.
    pub oflags: String,
    /// Current market value; `Some` only when `docalcs` was requested.
    pub value: Option<Decimal>,
    /// Unrealised profit/loss; `Some` only when `docalcs` was requested.
    pub net: Option<Decimal>,
}

/// TradeVolume snapshot; only `currency` and `volume` are decoded from the wire response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct TradeVolume {
    /// Valuation currency of `volume`, normalised to the modern code (`USD`).
    pub currency: AssetCode,
    /// Trading volume in `currency`, decoded from the wire string.
    pub volume: Decimal,
}

#[derive(Debug, Deserialize)]
pub(super) struct RawTradeVolume {
    pub(super) currency: String,
    pub(super) volume: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Display comes from strum; every variant must render exactly what serde emits.
    #[test]
    fn strum_display_matches_serde_emission() {
        for v in [LifecyclePosition::Open, LifecyclePosition::Closed] {
            assert_eq!(json!(v), json!(v.to_string()));
        }
        for v in [Side::Buy, Side::Sell] {
            assert_eq!(json!(v), json!(v.to_string()));
        }
        for v in [
            OrderStatus::Pending,
            OrderStatus::Open,
            OrderStatus::Closed,
            OrderStatus::Canceled,
            OrderStatus::Expired,
            OrderStatus::Unknown,
        ] {
            assert_eq!(json!(v), json!(v.to_string()));
        }
        for v in [PositionStatus::Open, PositionStatus::Closed] {
            assert_eq!(json!(v), json!(v.to_string()));
        }
        for v in [
            LedgerType::Staking,
            LedgerType::Transfer,
            LedgerType::Trade,
            LedgerType::Deposit,
            LedgerType::Withdrawal,
            LedgerType::Margin,
            LedgerType::Rollover,
            LedgerType::Adjustment,
            LedgerType::Conversion,
            LedgerType::Reward,
            LedgerType::Dividend,
            LedgerType::Sale,
            LedgerType::Unknown,
        ] {
            assert_eq!(json!(v), json!(v.to_string()));
        }
    }

    #[test]
    fn close_time_wire_tokens_are_pinned() {
        for (value, expected) in [
            (CloseTime::Open, "open"),
            (CloseTime::Close, "close"),
            (CloseTime::Both, "both"),
        ] {
            assert_eq!(value.as_ref(), expected);
            assert_eq!(value.to_string(), expected);
        }
    }

    #[test]
    fn trade_type_filter_wire_tokens_are_pinned() {
        for (value, expected) in [
            (TradeTypeFilter::All, "all"),
            (TradeTypeFilter::AnyPosition, "any position"),
            (TradeTypeFilter::ClosedPosition, "closed position"),
            (TradeTypeFilter::ClosingPosition, "closing position"),
            (TradeTypeFilter::NoPosition, "no position"),
        ] {
            assert_eq!(value.as_ref(), expected);
            assert_eq!(value.to_string(), expected);
        }
    }

    #[test]
    fn ledger_type_filter_wire_tokens_are_pinned() {
        for (value, expected) in [
            (LedgerTypeFilter::All, "all"),
            (LedgerTypeFilter::Trade, "trade"),
            (LedgerTypeFilter::Deposit, "deposit"),
            (LedgerTypeFilter::Withdrawal, "withdrawal"),
            (LedgerTypeFilter::Transfer, "transfer"),
            (LedgerTypeFilter::Margin, "margin"),
            (LedgerTypeFilter::Adjustment, "adjustment"),
            (LedgerTypeFilter::Rollover, "rollover"),
            (LedgerTypeFilter::Credit, "credit"),
            (LedgerTypeFilter::Settled, "settled"),
            (LedgerTypeFilter::Staking, "staking"),
            (LedgerTypeFilter::Dividend, "dividend"),
            (LedgerTypeFilter::Sale, "sale"),
            (LedgerTypeFilter::NftRebate, "nft_rebate"),
        ] {
            assert_eq!(value.as_ref(), expected);
            assert_eq!(value.to_string(), expected);
        }
    }
}
