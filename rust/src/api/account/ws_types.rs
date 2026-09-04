//! Typed payloads and decoders for the auth-WS `executions` and `balances`
//! channels.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::market::ws_types::parse_wire_decimal;
use crate::api::trade::Side;
use crate::types::AssetCode;
use serde_with::skip_serializing_none;

use crate::api::ws_decode::extract_data;

/// Per-entry `exec_type` discriminator for the `executions` channel.
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
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ExecType {
    /// Wire `pending_new` — order accepted by the engine, not yet live in the book.
    PendingNew,
    /// Wire `new` — order is live in the book.
    New,
    /// Wire `trade` — a fill (partial or full) executed on this order.
    Trade,
    /// Wire `filled` — order completely filled (terminal).
    Filled,
    /// Wire `iceberg_refill` — visible tranche of an iceberg order replenished.
    IcebergRefill,
    /// Wire `canceled` — order canceled (terminal).
    Canceled,
    /// Wire `expired` — order expired, e.g. time-in-force lapsed (terminal).
    Expired,
    /// Wire `restated` — order restated by the exchange, not user-initiated.
    Restated,
    /// Wire `status` — status update on the order, e.g. trigger price updated.
    Status,
    /// Wire `amended` — a user-initiated amend was applied to the order.
    Amended,
    /// Catch-all for wire values this SDK version does not know; never a fill.
    /// Serializes back out as `unknown` — the original wire token is not kept.
    #[serde(other)]
    Unknown,
}

/// WS executions `order_status`.
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
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum WsOrderStatus {
    /// Wire `pending_new` — accepted, not yet live in the book.
    PendingNew,
    /// Wire `new` — live in the book, nothing filled yet.
    New,
    /// Wire `partially_filled` — some quantity executed, remainder still live.
    PartiallyFilled,
    /// Wire `filled` — fully executed (terminal).
    Filled,
    /// Wire `canceled` — canceled (terminal).
    Canceled,
    /// Wire `expired` — expired (terminal).
    Expired,
    /// Catch-all for wire values this SDK version does not know.
    /// Serializes back out as `unknown` — the original wire token is not kept.
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
pub(super) struct RawExecution {
    pub(super) exec_type: ExecType,
    pub(super) order_id: Option<String>,
    pub(super) cl_ord_id: Option<String>,
    pub(super) symbol: Option<String>,
    // Lenient: side/trade_id kept as raw String/Value so a surprise token
    // doesn't sink the money-touching entry — mapped in decode_executions.
    pub(super) side: Option<String>,
    pub(super) order_type: Option<String>,
    pub(super) order_status: Option<WsOrderStatus>,
    pub(super) time_in_force: Option<String>,
    pub(super) exec_id: Option<String>,
    pub(super) trade_id: Option<Value>,
    pub(super) timestamp: Option<String>,
    pub(super) cancel_reason: Option<String>,
    pub(super) reason: Option<String>,
    pub(super) liquidity_ind: Option<String>,
    pub(super) order_qty: Option<Value>,
    pub(super) limit_price: Option<Value>,
    pub(super) cum_qty: Option<Value>,
    pub(super) cum_cost: Option<Value>,
    pub(super) avg_price: Option<Value>,
    pub(super) last_qty: Option<Value>,
    pub(super) last_price: Option<Value>,
    pub(super) fee_usd_equiv: Option<Value>,
    pub(super) cost: Option<Value>,
    pub(super) fees: Option<Value>,
}

/// One `executions` channel entry (auth WS v2). Entries are sparse per
/// `exec_type` — only fields relevant to each lifecycle transition are present;
/// every field is `Option` except `exec_type`. Channel-wide (no `symbol` arg).
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct ExecutionUpdate {
    /// Lifecycle transition discriminator; the only field guaranteed present.
    pub exec_type: ExecType,
    /// Kraken-assigned order ID (wire `order_id`).
    pub order_id: Option<String>,
    /// Client order ID (wire `cl_ord_id`); an invalid wire value maps to `None`.
    pub cl_ord_id: Option<crate::types::ClOrdId>,
    /// Trading pair, e.g. `BTC/USD`; an invalid wire value maps to `None`.
    pub symbol: Option<crate::types::Symbol>,
    /// Order side from wire `buy`/`sell`; an unrecognized token maps to `None`.
    pub side: Option<Side>,
    /// Passthrough string — order type as emitted by the wire.
    pub order_type: Option<String>,
    /// Order state after this transition (wire `order_status`).
    pub order_status: Option<WsOrderStatus>,
    /// Passthrough string — time-in-force as emitted by the wire.
    pub time_in_force: Option<String>,
    /// Execution ID for fill entries (wire `exec_id`).
    pub exec_id: Option<String>,
    /// Numeric trade ID; a non-integer wire value maps to `None`.
    pub trade_id: Option<u64>,
    /// Passthrough string — RFC 3339 timestamp as emitted by the wire.
    pub timestamp: Option<String>,
    /// Passthrough string — cancellation reason, present on cancel entries.
    pub cancel_reason: Option<String>,
    /// Passthrough string — event reason when applicable (cancel, amend, restate).
    pub reason: Option<String>,
    /// Passthrough string — maker/taker liquidity indicator on fills.
    pub liquidity_ind: Option<String>,
    /// Total order quantity, in base asset.
    pub order_qty: Option<Decimal>,
    /// Limit price in quote asset, when the order type carries one.
    pub limit_price: Option<Decimal>,
    /// Cumulative filled quantity (base asset) over the order's life.
    pub cum_qty: Option<Decimal>,
    /// Cumulative cost (quote asset) over the order's life.
    pub cum_cost: Option<Decimal>,
    /// Volume-weighted average fill price so far.
    pub avg_price: Option<Decimal>,
    /// Quantity of the most recent fill, in base asset.
    pub last_qty: Option<Decimal>,
    /// Price of the most recent fill, in quote asset.
    pub last_price: Option<Decimal>,
    /// Cumulative fees expressed in USD equivalent (wire `fee_usd_equiv`).
    pub fee_usd_equiv: Option<Decimal>,
    /// Per-fill cost (quote asset); `None` on non-fill entries.
    pub cost: Option<Decimal>,
    /// Per-fill fee(s), one entry per asset; `None` on non-fill entries.
    pub fees: Option<Vec<ExecutionFee>>,
}

/// One per-fill fee: an amount charged in a specific asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct ExecutionFee {
    /// Asset the fee was charged in.
    pub asset: AssetCode,
    /// Fee amount, denominated in `asset`.
    pub qty: Decimal,
}

impl ExecutionUpdate {
    /// `true` iff this entry is an executed fill ([`ExecType::Trade`] or terminal
    /// [`ExecType::Filled`]). Unknown variants return `false`.
    pub fn is_fill(&self) -> bool {
        matches!(self.exec_type, ExecType::Trade | ExecType::Filled)
    }
}

pub(super) fn decode_executions(v: Value) -> Option<ExecutionUpdate> {
    let data = extract_data(v)?;
    let raw: RawExecution = serde_json::from_value(data).ok()?;
    let parse_dec =
        |opt: Option<Value>| -> Option<Decimal> { opt.as_ref().and_then(parse_wire_decimal) };
    Some(ExecutionUpdate {
        exec_type: raw.exec_type,
        order_id: raw.order_id,
        cl_ord_id: raw
            .cl_ord_id
            .and_then(|s| crate::types::ClOrdId::new(s).ok()),
        symbol: raw.symbol.and_then(|s| crate::types::Symbol::new(&s).ok()),
        side: raw.side.and_then(|s| match s.as_str() {
            "buy" => Some(Side::Buy),
            "sell" => Some(Side::Sell),
            _ => None,
        }),
        order_type: raw.order_type,
        order_status: raw.order_status,
        time_in_force: raw.time_in_force,
        exec_id: raw.exec_id,
        trade_id: raw.trade_id.as_ref().and_then(serde_json::Value::as_u64),
        timestamp: raw.timestamp,
        cancel_reason: raw.cancel_reason,
        reason: raw.reason,
        liquidity_ind: raw.liquidity_ind,
        order_qty: parse_dec(raw.order_qty),
        limit_price: parse_dec(raw.limit_price),
        cum_qty: parse_dec(raw.cum_qty),
        cum_cost: parse_dec(raw.cum_cost),
        avg_price: parse_dec(raw.avg_price),
        last_qty: parse_dec(raw.last_qty),
        last_price: parse_dec(raw.last_price),
        fee_usd_equiv: parse_dec(raw.fee_usd_equiv),
        cost: parse_dec(raw.cost),
        // Non-array shape or zero parseable entries -> None; a partially-valid
        // array keeps the entries that parsed.
        fees: raw.fees.as_ref().and_then(Value::as_array).and_then(|arr| {
            let parsed: Vec<ExecutionFee> = arr
                .iter()
                .filter_map(|f| {
                    Some(ExecutionFee {
                        asset: AssetCode::from_wire(f.get("asset")?.as_str()?),
                        qty: parse_wire_decimal(f.get("qty")?)?,
                    })
                })
                .collect();
            (!parsed.is_empty()).then_some(parsed)
        }),
    })
}

#[derive(Debug, Deserialize)]
pub(super) struct RawWallet {
    #[serde(rename = "type")]
    pub(super) wallet_type: Option<String>,
    pub(super) id: Option<String>,
    pub(super) balance: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RawBalance {
    pub(super) asset: Option<String>,
    pub(super) asset_class: Option<String>,
    pub(super) balance: Option<Value>,
    pub(super) wallets: Option<Vec<RawWallet>>,
    pub(super) amount: Option<Value>,
    pub(super) fee: Option<Value>,
    pub(super) ledger_id: Option<String>,
    pub(super) ref_id: Option<String>,
    pub(super) timestamp: Option<String>,
    #[serde(rename = "type")]
    pub(super) ledger_type: Option<String>,
    pub(super) subtype: Option<String>,
    pub(super) category: Option<String>,
    pub(super) wallet_type: Option<String>,
    pub(super) wallet_id: Option<String>,
}

/// A `wallets` sub-entry within a `balances` snapshot. Snapshot entries carry
/// a `wallets` array; update entries do not. `wallet_type` is the wire field
/// `type` (passthrough string).
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Wallet {
    /// Wire field `type` (`"spot"` | `"earn"`).
    pub wallet_type: Option<String>,
    /// Wallet identifier (wire `id`, e.g. `"main"`).
    pub id: Option<String>,
    /// Balance held in this wallet; `None` when absent or unparseable.
    pub balance: Option<Decimal>,
}

/// One `balances` channel entry (auth WS v2). Covers both snapshot entries
/// (`wallets` array) and update entries (ledger fields); the discriminator is
/// the frame `type` (`"snapshot"`/`"update"`). Channel-wide (no `symbol` arg).
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct BalanceUpdate {
    /// Asset code, normalised from legacy wire forms (e.g. `XXBT` → `BTC`).
    pub asset: Option<AssetCode>,
    /// Passthrough string — wire `asset_class` (e.g. `"currency"`).
    pub asset_class: Option<String>,
    /// Total balance for the asset after this entry.
    pub balance: Option<Decimal>,
    /// Present on snapshot entries; `None` on update entries.
    pub wallets: Option<Vec<Wallet>>,
    /// Signed ledger amount applied by this entry; update entries only.
    pub amount: Option<Decimal>,
    /// Fee charged on the ledger event; update entries only.
    pub fee: Option<Decimal>,
    /// Ledger entry ID (wire `ledger_id`); update entries only.
    pub ledger_id: Option<String>,
    /// Reference ID linking to the originating event; update entries only.
    pub ref_id: Option<String>,
    /// Passthrough string — RFC 3339 timestamp as emitted by the wire.
    pub timestamp: Option<String>,
    /// Wire field `type` — ledger/transaction type, passthrough string.
    pub ledger_type: Option<String>,
    /// Passthrough string — ledger subtype as emitted by the wire.
    pub subtype: Option<String>,
    /// Passthrough string — ledger category (e.g. `"trade"`).
    pub category: Option<String>,
    /// Passthrough string — wallet type of the ledger event (e.g. `"spot"`).
    pub wallet_type: Option<String>,
    /// Passthrough string — wallet ID of the ledger event (e.g. `"main"`).
    pub wallet_id: Option<String>,
}

pub(super) fn decode_balances(v: Value) -> Option<BalanceUpdate> {
    let data = extract_data(v)?;
    let raw: RawBalance = serde_json::from_value(data).ok()?;
    let parse_dec =
        |opt: Option<Value>| -> Option<Decimal> { opt.as_ref().and_then(parse_wire_decimal) };
    let wallets = raw.wallets.map(|ws| {
        ws.into_iter()
            .map(|w| Wallet {
                wallet_type: w.wallet_type,
                id: w.id,
                balance: parse_dec(w.balance),
            })
            .collect()
    });
    Some(BalanceUpdate {
        asset: raw.asset.as_deref().map(AssetCode::from_wire),
        asset_class: raw.asset_class,
        balance: parse_dec(raw.balance),
        wallets,
        amount: parse_dec(raw.amount),
        fee: parse_dec(raw.fee),
        ledger_id: raw.ledger_id,
        ref_id: raw.ref_id,
        timestamp: raw.timestamp,
        ledger_type: raw.ledger_type,
        subtype: raw.subtype,
        category: raw.category,
        wallet_type: raw.wallet_type,
        wallet_id: raw.wallet_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn envelope(entry: serde_json::Value) -> Value {
        json!({ "type": "update", "data": entry })
    }

    #[test]
    fn decode_executions_pending_new_rich() {
        let v = envelope(json!({
            "exec_type": "pending_new",
            "order_id": "OABC123",
            "cl_ord_id": "my-cl-ord-id-01",
            "symbol": "BTC/USD",
            "side": "buy",
            "order_type": "limit",
            "order_status": "pending_new",
            "time_in_force": "GTC",
            "timestamp": "2026-06-06T10:00:00Z",
            "order_qty": 0.0001,
            "limit_price": 33000,
            "cum_cost": 0,
            "fee_usd_equiv": 0
        }));
        let u = decode_executions(v).expect("should decode");
        assert_eq!(u.exec_type, ExecType::PendingNew);
        assert_eq!(u.order_status, Some(WsOrderStatus::PendingNew));
        assert_eq!(u.side, Some(Side::Buy));
        assert!(u.symbol.is_some());
        assert_eq!(u.symbol.as_ref().map(|s| s.as_str()), Some("BTC/USD"));
        assert_eq!(u.order_type.as_deref(), Some("limit"));
        assert!(u.order_qty.is_some(), "order_qty should decode from number");
        let qty = u.order_qty.unwrap();
        assert!(qty > Decimal::ZERO, "order_qty > 0");
        assert!(u.limit_price.is_some(), "limit_price should decode");
    }

    #[test]
    fn decode_executions_sparse_new() {
        let v = envelope(json!({
            "exec_type": "new",
            "cl_ord_id": "my-cl-ord-id-01",
            "order_id": "OABC123",
            "order_status": "new",
            "timestamp": "2026-06-06T10:00:01Z"
        }));
        let u = decode_executions(v).expect("sparse new must decode OK");
        assert_eq!(u.exec_type, ExecType::New);
        assert_eq!(u.order_status, Some(WsOrderStatus::New));
        assert!(u.order_qty.is_none(), "order_qty absent on sparse new");
        assert!(u.limit_price.is_none());
        assert!(u.avg_price.is_none());
    }

    #[test]
    fn decode_executions_canceled() {
        let v = envelope(json!({
            "exec_type": "canceled",
            "order_id": "OABC123",
            "cl_ord_id": "my-cl-ord-id-01",
            "order_status": "canceled",
            "cancel_reason": "User requested",
            "reason": "User requested",
            "avg_price": 0,
            "cum_cost": 0,
            "cum_qty": 0,
            "fee_usd_equiv": 0,
            "timestamp": "2026-06-06T10:01:00Z"
        }));
        let u = decode_executions(v).expect("canceled must decode");
        assert_eq!(u.exec_type, ExecType::Canceled);
        assert_eq!(u.order_status, Some(WsOrderStatus::Canceled));
        assert!(u.cancel_reason.is_some(), "cancel_reason should be Some");
        assert!(u.avg_price.is_some(), "avg_price Some even when 0");
        assert!(u.cum_qty.is_some(), "cum_qty Some even when 0");
    }

    #[test]
    fn decode_executions_unknown_exec_type() {
        let v = envelope(json!({
            "exec_type": "some_future_event_type",
            "order_id": "OXYZ"
        }));
        let u = decode_executions(v).expect("unknown exec_type must not fail decode");
        assert_eq!(u.exec_type, ExecType::Unknown);
    }

    #[test]
    fn decode_executions_non_object_data_is_none() {
        let v = json!({ "type": "update", "data": "not_an_object" });
        assert!(decode_executions(v).is_none());
    }

    #[test]
    fn decode_executions_unknown_side_survives() {
        let v = envelope(json!({
            "exec_type": "new",
            "order_id": "OABC123",
            "side": "some_future_side",
            "order_status": "new"
        }));
        let u = decode_executions(v).expect("unknown side must NOT fail the entry");
        assert_eq!(u.exec_type, ExecType::New);
        assert_eq!(u.order_id.as_deref(), Some("OABC123"));
        assert_eq!(
            u.side, None,
            "unrecognized side maps to None, entry preserved"
        );
    }

    #[test]
    fn decode_executions_trade_id_lenient() {
        let v = envelope(json!({
            "exec_type": "trade", "order_id": "O1", "trade_id": "12345"
        }));
        let u = decode_executions(v).expect("string trade_id must not fail entry");
        assert_eq!(u.trade_id, None);
        assert_eq!(u.exec_type, ExecType::Trade);
        let v = envelope(json!({
            "exec_type": "trade", "order_id": "O2", "trade_id": 1.5
        }));
        let u = decode_executions(v).expect("float trade_id must not fail entry");
        assert_eq!(u.trade_id, None);
        let v = envelope(json!({
            "exec_type": "trade", "order_id": "O3", "trade_id": 987654321_u64
        }));
        let u = decode_executions(v).expect("integer trade_id decodes");
        assert_eq!(u.trade_id, Some(987654321));
    }

    #[test]
    fn is_fill_true_for_trade_and_filled() {
        for et in ["trade", "filled"] {
            let v = envelope(json!({ "exec_type": et, "order_id": "O1" }));
            let u = decode_executions(v).expect("must decode");
            assert!(u.is_fill(), "is_fill() should be true for exec_type {et}");
        }
    }

    /// Also locks the `#[non_exhaustive]` fail-closed default: unknown variants
    /// are not fills.
    #[test]
    fn is_fill_false_for_non_fill_exec_types() {
        for et in [
            "pending_new",
            "new",
            "canceled",
            "expired",
            "amended",
            "some_future_event_type",
        ] {
            let v = envelope(json!({ "exec_type": et, "order_id": "O1" }));
            let u = decode_executions(v).expect("must decode");
            assert!(!u.is_fill(), "is_fill() should be false for exec_type {et}");
        }
    }

    #[test]
    fn decode_balances_snapshot_entry() {
        let v = envelope(json!({
            "asset": "USDC",
            "asset_class": "currency",
            "balance": 1.5,
            "wallets": [
                { "type": "spot", "id": "main", "balance": 1.5 }
            ]
        }));
        let u = decode_balances(v).expect("snapshot must decode");
        assert_eq!(u.asset.as_ref().map(|a| a.as_str()), Some("USDC"));
        assert!(u.balance.is_some(), "balance should decode from number");
        let bal = u.balance.unwrap();
        use std::str::FromStr;
        assert_eq!(bal, Decimal::from_str("1.5").unwrap());
        let wallets = u.wallets.as_ref().expect("wallets should be Some");
        assert_eq!(wallets.len(), 1);
        assert_eq!(wallets[0].wallet_type.as_deref(), Some("spot"));
        assert_eq!(wallets[0].id.as_deref(), Some("main"));
        assert!(wallets[0].balance.is_some());
    }

    #[test]
    fn decode_balances_normalises_legacy_asset() {
        let v = envelope(json!({
            "asset": "XXBT",
            "asset_class": "currency",
            "balance": 0.5
        }));
        let u = decode_balances(v).expect("must decode");
        assert_eq!(u.asset.as_ref().map(|a| a.as_str()), Some("BTC"));
    }

    #[test]
    fn decode_balances_update_entry_with_ledger_fields() {
        let v = envelope(json!({
            "asset": "USDC",
            "asset_class": "currency",
            "balance": 2.0,
            "amount": 0.5,
            "fee": 0.0,
            "ledger_id": "LXYZ001",
            "ref_id": "TRADE-001",
            "timestamp": "2026-06-06T10:05:00Z",
            "type": "trade",
            "subtype": null,
            "category": "trade",
            "wallet_type": "spot",
            "wallet_id": "main"
        }));
        let u = decode_balances(v).expect("update entry must decode");
        assert_eq!(u.asset.as_ref().map(|a| a.as_str()), Some("USDC"));
        assert!(u.amount.is_some(), "amount should decode");
        assert_eq!(u.ledger_id.as_deref(), Some("LXYZ001"));
        assert_eq!(u.ledger_type.as_deref(), Some("trade"));
        assert_eq!(u.category.as_deref(), Some("trade"));
        assert_eq!(u.wallet_type.as_deref(), Some("spot"));
        assert_eq!(u.wallet_id.as_deref(), Some("main"));
        assert!(u.wallets.is_none());
    }

    /// Executions are sparse by design: most fields are absent for any given
    /// `exec_type`, so rendering them as `null` would be mostly padding.
    #[test]
    fn execution_update_omits_absent_optional_fields() {
        let u = decode_executions(envelope(json!({
            "exec_type": "new",
            "order_id": "OABC123"
        })))
        .expect("sparse new decodes");
        let v = serde_json::to_value(&u).expect("ExecutionUpdate serializes");
        let obj = v.as_object().expect("serializes to an object");
        assert!(
            !obj.values().any(serde_json::Value::is_null),
            "absent optionals must be omitted, not null: {v}"
        );
        assert_eq!(obj.len(), 2, "only the present fields survive: {v}");
        assert_eq!(v["order_id"], json!("OABC123"));
    }

    /// `rename_all` applies on the way out too, so these variant names are a wire
    /// contract a consumer keys off — not just a decode convenience.
    #[test]
    fn execution_update_serializes_enums_as_snake_case_with_nested_fees() {
        let u = decode_executions(envelope(json!({
            "exec_type": "pending_new",
            "order_id": "OABC123",
            "order_status": "pending_new",
            "fees": [{"asset": "USDC", "qty": "0.10018"}]
        })))
        .expect("pending_new decodes");
        let v = serde_json::to_value(&u).expect("ExecutionUpdate serializes");
        assert_eq!(v["exec_type"], json!("pending_new"));
        assert_eq!(v["order_status"], json!("pending_new"));
        assert_eq!(v["fees"][0]["asset"], json!("USDC"));
        assert_eq!(v["fees"][0]["qty"], json!("0.10018"));
    }

    /// Snapshot and update entries share one type, so a snapshot must not surface
    /// the update-only ledger fields.
    #[test]
    fn balance_update_serializes_wallet_entries() {
        let u = decode_balances(envelope(json!({
            "asset": "BTC",
            "balance": 1.5,
            "wallets": [{"type": "spot", "id": "main", "balance": 1.5}]
        })))
        .expect("snapshot entry must decode");
        let v = serde_json::to_value(&u).expect("BalanceUpdate serializes");
        assert_eq!(v["asset"], json!("BTC"));
        assert_eq!(v["wallets"][0]["wallet_type"], json!("spot"));
        assert_eq!(v["wallets"][0]["id"], json!("main"));
        assert!(
            v.get("ledger_id").is_none(),
            "a snapshot entry carries no ledger fields: {v}"
        );
    }

    /// Display comes from strum; every variant must render exactly what serde emits.
    #[test]
    fn strum_display_matches_serde_emission() {
        for v in [
            ExecType::PendingNew,
            ExecType::New,
            ExecType::Trade,
            ExecType::Filled,
            ExecType::IcebergRefill,
            ExecType::Canceled,
            ExecType::Expired,
            ExecType::Restated,
            ExecType::Status,
            ExecType::Amended,
            ExecType::Unknown,
        ] {
            assert_eq!(json!(v), json!(v.to_string()));
        }
        for v in [
            WsOrderStatus::PendingNew,
            WsOrderStatus::New,
            WsOrderStatus::PartiallyFilled,
            WsOrderStatus::Filled,
            WsOrderStatus::Canceled,
            WsOrderStatus::Expired,
            WsOrderStatus::Unknown,
        ] {
            assert_eq!(json!(v), json!(v.to_string()));
        }
    }
}
