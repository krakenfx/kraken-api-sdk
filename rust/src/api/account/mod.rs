//! `AccountNamespace` — Spot private account data: balances, orders, ledgers,
//! trade history, positions and trade volume, plus the private WS streams.

use std::collections::HashMap;
use std::sync::Arc;

use rust_decimal::Decimal;
use serde_json::Value;

use crate::api::ws_surface::WsSurface;
use crate::dispatch::dispatch_table::{DispatchTable, Op, Product, Transport};
use crate::rest::RestSurface;
use crate::types::{AssetCode, AuthProfile};

mod error;
mod requests;
mod types;
mod ws;
mod ws_types;

#[cfg(test)]
mod tests;

pub use error::AccountError;
pub use requests::{ClosedOrdersRequest, LedgersRequest, OpenOrdersRequest, TradesHistoryRequest};
pub use types::{
    Balance, CloseTime, ClosedOrders, ExtendedBalance, ExtendedBalanceEntry, KrakenTimestamp,
    LedgerEntry, LedgerType, LedgerTypeFilter, Ledgers, LifecyclePosition, MaskedBalance,
    OpenOrders, OpenPositionEntry, OpenPositions, OrderDescr, OrderInfo, OrderStatus,
    PositionStatus, ReconciliationOutcome, TradeBalance, TradeHistoryEntry, TradeTypeFilter,
    TradeVolume, TradesHistorySnapshot,
};
pub use ws_types::{BalanceUpdate, ExecType, ExecutionFee, ExecutionUpdate, Wallet, WsOrderStatus};

use crate::types::ClOrdId;

use types::RawTradeVolume;

fn decode_named_map<T: serde::de::DeserializeOwned>(
    result: serde_json::Value,
    field: &str,
    ctx: &str,
) -> Result<HashMap<String, T>, AccountError> {
    let field_val = match result {
        Value::Object(mut m) => m.remove(field),
        _ => None,
    };
    let Some(Value::Object(field_obj)) = field_val else {
        return Err(AccountError::malformed(format!(
            "{ctx}: missing `{field}` object"
        )));
    };
    decode_bare_map(Value::Object(field_obj), ctx)
}

fn decode_named_map_with_count<T: serde::de::DeserializeOwned>(
    result: serde_json::Value,
    field: &str,
    ctx: &str,
) -> Result<(HashMap<String, T>, u64), AccountError> {
    // `count` is read first (borrow) but the map-decode error must surface first — error precedence.
    let count_opt = result.get("count").and_then(Value::as_u64);
    let map = decode_named_map::<T>(result, field, ctx)?;
    let count =
        count_opt.ok_or_else(|| AccountError::malformed(format!("{ctx}: missing `count`")))?;
    Ok((map, count))
}

fn decode_bare_map<T: serde::de::DeserializeOwned>(
    result: serde_json::Value,
    ctx: &str,
) -> Result<HashMap<String, T>, AccountError> {
    crate::api::decode_object_map(result, ctx, AccountError::malformed, |id, v| Ok((id, v)))
}

fn push_opt<T: ToString>(form: &mut Vec<(String, String)>, name: &'static str, opt: Option<T>) {
    if let Some(v) = opt {
        form.push((name.to_string(), v.to_string()));
    }
}

/// Spot REST private account namespace, accessed via `client.account()`; requires API credentials.
pub struct AccountNamespace {
    rest: Arc<RestSurface>,
    ws: Arc<WsSurface>,
    dispatch: Arc<DispatchTable>,
}

impl AccountNamespace {
    pub(crate) fn new(
        rest: Arc<RestSurface>,
        ws: Arc<WsSurface>,
        dispatch: Arc<DispatchTable>,
    ) -> Self {
        Self { rest, ws, dispatch }
    }

    async fn signed_dispatch(
        &self,
        path: &'static str,
        form: Vec<(String, String)>,
        auth: AuthProfile,
        cost: crate::rest::RateLimitCost,
    ) -> Result<(String, serde_json::Value), AccountError> {
        let request_id = crate::rest::mint_request_id();
        let result = self
            .rest
            .signed_post_costed(
                path,
                form,
                auth,
                cost,
                crate::rest::RetryPolicy::idempotent(),
                &request_id,
            )
            .await;
        match result {
            Ok(value) => Ok((request_id, value)),
            Err(e) => Err(AccountError::from(e).with_request_id(&request_id)),
        }
    }

    async fn signed_request<R: crate::api::WireRequest>(
        &self,
        op: Op,
        req: &R,
        cost: crate::rest::RateLimitCost,
    ) -> Result<(String, serde_json::Value), AccountError> {
        let (path, auth) = self.route(op)?;
        self.signed_dispatch(path, req.wire_params(), auth, cost)
            .await
    }

    fn route(&self, op: Op) -> Result<(&'static str, AuthProfile), AccountError> {
        let entry = self.dispatch.resolve(op, Product::Spot, Transport::Rest)?;
        let auth = if entry.requires_auth {
            AuthProfile::SpotV1
        } else {
            AuthProfile::Public
        };
        Ok((entry.wire_target, auth))
    }

    /// Fetch the account's per-asset balance via `POST /0/private/Balance`.
    ///
    /// # Errors
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected the call.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — bad envelope, or a balance value failed the typed decode.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`, dispatch misses `INTERNAL`.
    pub async fn balance(&self) -> Result<Balance, AccountError> {
        let (path, auth) = self.route(Op::AccountBalance)?;
        let (request_id, result) = self
            .signed_dispatch(
                path,
                Vec::new(),
                auth,
                crate::rest::RateLimitCost::Api { cost: 1.0 },
            )
            .await?;

        Balance::from_value(result).map_err(|e| e.with_request_id(&request_id))
    }

    /// Fetch per-asset extended balance via `POST /0/private/BalanceEx`.
    ///
    /// # Errors
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected the call.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — bad envelope, or a per-asset entry failed the typed decode.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`, dispatch misses `INTERNAL`.
    pub async fn extended_balance(&self) -> Result<ExtendedBalance, AccountError> {
        let (path, auth) = self.route(Op::AccountBalanceEx)?;
        let (request_id, result) = self
            .signed_dispatch(
                path,
                Vec::new(),
                auth,
                crate::rest::RateLimitCost::Api { cost: 1.0 },
            )
            .await?;

        ExtendedBalance::from_value(result).map_err(|e| e.with_request_id(&request_id))
    }

    /// Fetch consolidated trade balance via `POST /0/private/TradeBalance`.
    /// `asset` is the valuation currency (defaults to ZUSD).
    ///
    /// # Errors
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::InvalidArguments`] — Kraken rejected a request parameter (e.g. unknown `asset`).
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected the call.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — bad envelope, or the result failed the typed decode.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`, dispatch misses `INTERNAL`.
    pub async fn trade_balance(&self, asset: Option<String>) -> Result<TradeBalance, AccountError> {
        let mut form: Vec<(String, String)> = Vec::new();
        push_opt(&mut form, "asset", asset);
        let (path, auth) = self.route(Op::AccountTradeBalance)?;
        let (request_id, result) = self
            .signed_dispatch(
                path,
                form,
                auth,
                crate::rest::RateLimitCost::Api { cost: 1.0 },
            )
            .await?;

        serde_json::from_value(result)
            .map_err(|e| AccountError::malformed(format!("trade_balance decode: {}", e)))
            .map_err(|e| e.with_request_id(&request_id))
    }

    /// Fetch currently-open orders via `POST /0/private/OpenOrders`.
    /// Filters live on [`OpenOrdersRequest`]; an unset filter is not sent.
    ///
    /// # Errors
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::InvalidArguments`] — Kraken rejected a request parameter.
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected the call.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — missing `open` object, or an order entry failed the typed decode.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`, dispatch misses `INTERNAL`.
    pub async fn open_orders(&self, req: OpenOrdersRequest) -> Result<OpenOrders, AccountError> {
        let (request_id, result) = self
            .signed_request(
                Op::AccountOpenOrders,
                &req,
                crate::rest::RateLimitCost::Api { cost: 2.0 },
            )
            .await?;

        let open = decode_named_map::<OrderInfo>(result, "open", "open_orders")
            .map_err(|e| e.with_request_id(&request_id))?;
        Ok(OpenOrders { open })
    }

    /// Fetch closed-order history (paginate via `ofs`) via `POST /0/private/ClosedOrders`.
    /// Filters live on [`ClosedOrdersRequest`]; an unset filter is not sent
    /// (`closetime` defaults to `"both"` server-side).
    ///
    /// # Errors
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::InvalidArguments`] — Kraken rejected a request parameter (e.g. bad `closetime`).
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected the call.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — missing `closed` object or `count`, or an order entry failed the typed decode.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`, dispatch misses `INTERNAL`.
    pub async fn closed_orders(
        &self,
        req: ClosedOrdersRequest,
    ) -> Result<ClosedOrders, AccountError> {
        let (request_id, result) = self
            .signed_request(
                Op::AccountClosedOrders,
                &req,
                crate::rest::RateLimitCost::Api { cost: 2.0 },
            )
            .await?;

        let (closed, count) =
            decode_named_map_with_count::<OrderInfo>(result, "closed", "closed_orders")
                .map_err(|e| e.with_request_id(&request_id))?;
        Ok(ClosedOrders { closed, count })
    }

    /// Fetch ledger entries via `POST /0/private/Ledgers`.
    ///
    /// # Errors
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::InvalidArguments`] — Kraken rejected a request parameter (e.g. unknown asset or `type`).
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected the call.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — missing `ledger` object or `count`, or an entry failed the typed decode.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`, dispatch misses `INTERNAL`.
    pub async fn ledgers(&self, req: LedgersRequest) -> Result<Ledgers, AccountError> {
        let (request_id, result) = self
            .signed_request(
                Op::AccountLedgers,
                &req,
                crate::rest::RateLimitCost::Api { cost: 2.0 },
            )
            .await?;

        let (ledger, count) =
            decode_named_map_with_count::<LedgerEntry>(result, "ledger", "ledgers")
                .map_err(|e| e.with_request_id(&request_id))?;
        Ok(Ledgers { ledger, count })
    }

    /// Fetch trade history (executed fills, spot and margin) via
    /// `POST /0/private/TradesHistory`. Filters live on [`TradesHistoryRequest`];
    /// paginate via `ofs`.
    ///
    /// # Errors
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::InvalidArguments`] — Kraken rejected a request parameter (e.g. bad `type`).
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected the call.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — missing `trades` object or `count`, or an entry failed the typed decode.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`, dispatch misses `INTERNAL`.
    pub async fn trades_history(
        &self,
        req: TradesHistoryRequest,
    ) -> Result<TradesHistorySnapshot, AccountError> {
        let (request_id, result) = self
            .signed_request(
                Op::AccountTradesHistory,
                &req,
                crate::rest::RateLimitCost::Api { cost: 2.0 },
            )
            .await?;

        let (trades, count) =
            decode_named_map_with_count::<TradeHistoryEntry>(result, "trades", "trades_history")
                .map_err(|e| e.with_request_id(&request_id))?;
        Ok(TradesHistorySnapshot { trades, count })
    }

    /// Reconcile an order by its client order id; the [`ReconciliationOutcome`]
    /// is returned synchronously and also emitted on the event bus.
    ///
    /// # Errors
    /// Either leg of the walk (OpenOrders, then ClosedOrders) can fail; the error
    /// carries the failing leg's own request id. The walk is idempotent — re-call on error.
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::InvalidArguments`] — Kraken rejected the `cl_ord_id` parameter.
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected a leg.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — a leg's response envelope failed to parse.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`.
    pub async fn find_order_by_cl_ord_id(
        &self,
        cl_ord_id: &ClOrdId,
    ) -> Result<ReconciliationOutcome, AccountError> {
        self.rest
            .find_order_by_cl_ord_id(cl_ord_id)
            .await
            .map_err(|(request_id, e)| AccountError::from(e).with_request_id(&request_id))
    }

    /// Fetch specific ledger entries by ledger id via `POST /0/private/QueryLedgers`.
    ///
    /// # Errors
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::InvalidArguments`] — Kraken rejected a request parameter (e.g. bad ledger id).
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected the call.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — result not an object, or an entry failed the typed decode.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`, dispatch misses `INTERNAL`.
    pub async fn query_ledgers(
        &self,
        ids: Vec<String>,
    ) -> Result<HashMap<String, LedgerEntry>, AccountError> {
        let form = vec![("id".to_string(), ids.join(","))];
        let (path, auth) = self.route(Op::AccountQueryLedgers)?;
        let (request_id, result) = self
            .signed_dispatch(
                path,
                form,
                auth,
                crate::rest::RateLimitCost::Api { cost: 2.0 },
            )
            .await?;
        decode_bare_map::<LedgerEntry>(result, "query_ledgers")
            .map_err(|e| e.with_request_id(&request_id))
    }

    /// Fetch open margin positions via `POST /0/private/OpenPositions`.
    /// `consolidation=market` is not sent (its reply is a different container).
    ///
    /// # Errors
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::InvalidArguments`] — Kraken rejected a request parameter (e.g. unknown `txid`).
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected the call.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — result not an object, or a position entry failed the typed decode.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`, dispatch misses `INTERNAL`.
    pub async fn positions(
        &self,
        txids: Option<Vec<String>>,
        docalcs: bool,
    ) -> Result<OpenPositions, AccountError> {
        let mut form: Vec<(String, String)> = vec![("docalcs".to_string(), docalcs.to_string())];
        if let Some(t) = txids {
            form.push(("txid".to_string(), t.join(",")));
        }
        let (path, auth) = self.route(Op::AccountOpenPositions)?;
        let (request_id, result) = self
            .signed_dispatch(
                path,
                form,
                auth,
                crate::rest::RateLimitCost::Api { cost: 1.0 },
            )
            .await?;
        let positions = decode_bare_map::<OpenPositionEntry>(result, "positions")
            .map_err(|e| e.with_request_id(&request_id))?;
        Ok(OpenPositions { positions })
    }

    /// Fetch the account's 30-day rolling trading volume via `POST /0/private/TradeVolume`.
    /// `pair` and `fee_info` request Kraken's fee schedule, which is not decoded:
    /// [`TradeVolume`] carries the volume and its currency only.
    ///
    /// # Errors
    /// - [`AccountError::PermissionDenied`] — the API key lacks the required query permission.
    /// - [`AccountError::InvalidArguments`] — Kraken rejected a request parameter (e.g. unknown `pair`).
    /// - [`AccountError::RateLimited`] — the pre-flight API counter or Kraken's rate-limit family rejected the call.
    /// - [`AccountError::Transport`] — network-layer failure; retryable iff `transient`.
    /// - [`AccountError::MalformedResponse`] — the result or its `volume` decimal failed the typed decode.
    /// - [`AccountError::Unknown`] — unmapped Kraken strings pass through with their `EClass` code; auth-layer failures carry `AUTH`, dispatch misses `INTERNAL`.
    pub async fn volume(
        &self,
        pair: Option<crate::types::Symbol>,
        fee_info: bool,
    ) -> Result<TradeVolume, AccountError> {
        let mut form: Vec<(String, String)> = vec![("fee-info".to_string(), fee_info.to_string())];
        if let Some(p) = pair {
            form.push(("pair".to_string(), p.as_str().to_string()));
        }
        let (path, auth) = self.route(Op::AccountTradeVolume)?;
        let (request_id, result) = self
            .signed_dispatch(
                path,
                form,
                auth,
                crate::rest::RateLimitCost::Api { cost: 1.0 },
            )
            .await?;
        let raw: RawTradeVolume = serde_json::from_value(result).map_err(|e| {
            AccountError::malformed(format!("volume decode: {}", e)).with_request_id(&request_id)
        })?;
        Ok(TradeVolume {
            currency: AssetCode::from_wire(&raw.currency),
            volume: raw.volume.parse::<Decimal>().map_err(|e| {
                AccountError::malformed(format!("volume.volume: {}", e))
                    .with_request_id(&request_id)
            })?,
        })
    }
}
