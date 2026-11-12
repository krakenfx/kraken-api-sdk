//! Request types for Futures order entry on the Unified private WebSocket.

use rust_decimal::Decimal;

use super::super::error::TradeError;
use super::enums::{OrderType, Side, StpType, TimeInForce, TriggerKind};
use super::requests::{Price, TimeSpec};

fn validate_symbol(symbol: &str) -> Result<(), TradeError> {
    if crate::types::is_futures_wire_symbol(symbol) {
        Ok(())
    } else {
        Err(TradeError::invalid_order(format!(
            "`{symbol}` is not a Futures wire symbol such as `PF_XBTUSD`"
        )))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Leg {
    Unused,
    Required,
}

/// What each supported order type takes: `price`, `price2`, whether a `trigger` applies;
/// `None` for an order type Futures does not support.
const fn price_legs(order_type: OrderType) -> Option<(Leg, Leg, bool)> {
    match order_type {
        OrderType::Iceberg | OrderType::SettlePosition | OrderType::Unknown => None,
        OrderType::Market => Some((Leg::Unused, Leg::Unused, false)),
        OrderType::Limit => Some((Leg::Required, Leg::Unused, false)),
        OrderType::StopLoss | OrderType::TakeProfit | OrderType::TrailingStop => {
            Some((Leg::Required, Leg::Unused, true))
        }
        OrderType::StopLossLimit | OrderType::TakeProfitLimit | OrderType::TrailingStopLimit => {
            Some((Leg::Required, Leg::Required, true))
        }
    }
}

fn check_leg(
    field: &str,
    leg: Leg,
    present: bool,
    order_type: OrderType,
) -> Result<(), TradeError> {
    match (leg, present) {
        (Leg::Required, false) => Err(TradeError::invalid_order(format!(
            "`{field}` is required for a {order_type} order"
        ))),
        (Leg::Unused, true) => Err(TradeError::invalid_order(format!(
            "`{field}` is not used by a {order_type} order"
        ))),
        _ => Ok(()),
    }
}

fn require_absolute(field: &str, price: Option<&Price>) -> Result<(), TradeError> {
    match price {
        Some(Price::Offset { .. }) => Err(TradeError::invalid_order(format!(
            "`{field}` must be an absolute price on Futures"
        ))),
        _ => Ok(()),
    }
}

/// One Futures order, shared by single orders and batch rows; excludes the symbol and dry-run flag.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FuturesOrder {
    /// Order side; wire `side`.
    pub side: Side,
    /// Order quantity in contracts; wire `order_qty`.
    pub volume: Decimal,
    /// Order type; wire `order_type`. Market, limit, stop-loss, take-profit and
    /// trailing-stop (each with a `-limit` variant where it has a limit leg) are accepted.
    pub order_type: OrderType,
    /// Limit price for `Limit`; trigger price for stop-loss, take-profit and
    /// trailing orders (a positive relative offset for trailing).
    pub price: Option<Price>,
    /// Limit price of the `-limit` variants' limit leg; always absolute.
    pub price2: Option<Price>,
    /// Trigger reference price (`last` or `index`); wire `triggers.reference`.
    pub trigger: Option<TriggerKind>,
    /// `Gtc` or `Ioc`; wire `time_in_force`.
    pub time_in_force: Option<TimeInForce>,
    /// Self-trade prevention; only `CancelNewest` is accepted.
    pub stp_type: Option<StpType>,
    /// Only reduce or close an existing position; wire `reduce_only`.
    pub reduce_only: Option<bool>,
    /// Reject rather than cross the book; wire `post_only`.
    pub post_only: bool,
    /// Client order id (UUID v4 allocated when `None`). An id that is not a lowercase UUID comes
    /// back normalised on executions records and, for an uppercase UUID, the `add_order` reply
    /// (see [`crate::FuturesExecutionUpdate::cl_ord_id`]); amend and cancel still take it as sent.
    pub cl_ord_id: Option<crate::types::ClOrdId>,
    /// Broker IIBAN (the partner's Kraken IIBAN); wire `broker`, sent per order (a
    /// batch row carries its own). The venue validates it.
    pub broker: Option<String>,
    /// Wire `deadline` (RFC 3339), sent as given; the SDK never reads the local clock.
    /// A batch row cannot carry one.
    pub deadline: Option<TimeSpec>,
}

impl FuturesOrder {
    /// A market order of `volume` contracts on `side`.
    pub fn new(side: Side, volume: Decimal) -> Self {
        Self {
            side,
            volume,
            order_type: OrderType::Market,
            price: None,
            price2: None,
            trigger: None,
            time_in_force: None,
            stp_type: None,
            reduce_only: None,
            post_only: false,
            cl_ord_id: None,
            broker: None,
            deadline: None,
        }
    }

    /// Set the order type.
    #[must_use]
    pub fn order_type(mut self, order_type: OrderType) -> Self {
        self.order_type = order_type;
        self
    }

    /// Set the limit or trigger price; a `Decimal` converts to an absolute [`Price`].
    #[must_use]
    pub fn price(mut self, price: impl Into<Price>) -> Self {
        self.price = Some(price.into());
        self
    }

    /// Set the limit price of a `-limit` order's limit leg.
    #[must_use]
    pub fn price2(mut self, price2: impl Into<Price>) -> Self {
        self.price2 = Some(price2.into());
        self
    }

    /// Set the trigger reference price.
    #[must_use]
    pub fn trigger(mut self, trigger: TriggerKind) -> Self {
        self.trigger = Some(trigger);
        self
    }

    /// Set the time in force.
    #[must_use]
    pub fn time_in_force(mut self, time_in_force: TimeInForce) -> Self {
        self.time_in_force = Some(time_in_force);
        self
    }

    /// Set self-trade prevention; only `CancelNewest` is accepted.
    #[must_use]
    pub fn stp_type(mut self, stp_type: StpType) -> Self {
        self.stp_type = Some(stp_type);
        self
    }

    /// Only reduce or close an existing position.
    #[must_use]
    pub fn reduce_only(mut self, reduce_only: bool) -> Self {
        self.reduce_only = Some(reduce_only);
        self
    }

    /// Reject rather than cross the book.
    #[must_use]
    pub fn post_only(mut self, post_only: bool) -> Self {
        self.post_only = post_only;
        self
    }

    /// Set the client order id.
    #[must_use]
    pub fn cl_ord_id(mut self, cl_ord_id: crate::types::ClOrdId) -> Self {
        self.cl_ord_id = Some(cl_ord_id);
        self
    }

    /// Set the Broker IIBAN.
    #[must_use]
    pub fn broker(mut self, broker: impl Into<String>) -> Self {
        self.broker = Some(broker.into());
        self
    }

    /// Set the deadline; see [`FuturesOrder::deadline`].
    #[must_use]
    pub fn deadline(mut self, deadline: TimeSpec) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Reject what the venue rejects or would silently drop, before anything is
    /// sent: an unsupported feature, a price the order type needs but lacks, and a
    /// price or trigger the order type does not use. Each is [`TradeError::InvalidOrder`].
    pub fn validate(&self) -> Result<(), TradeError> {
        if self.volume <= Decimal::ZERO {
            return Err(TradeError::invalid_order("order quantity must be positive"));
        }
        let Some((price, price2, trigger)) = price_legs(self.order_type) else {
            return Err(TradeError::invalid_order(format!(
                "order type `{}` is not supported on Futures",
                self.order_type
            )));
        };
        if let Some(tif) = self.time_in_force
            && !matches!(tif, TimeInForce::Gtc | TimeInForce::Ioc)
        {
            return Err(TradeError::invalid_order(format!(
                "time in force `{tif}` is not supported on Futures (gtc or ioc)"
            )));
        }
        if let Some(stp) = self.stp_type
            && stp != StpType::CancelNewest
        {
            return Err(TradeError::invalid_order(format!(
                "self-trade prevention `{stp}` is not supported on Futures (cancel-newest)"
            )));
        }
        check_leg("price", price, self.price.is_some(), self.order_type)?;
        check_leg("price2", price2, self.price2.is_some(), self.order_type)?;
        if self.trigger.is_some() && !trigger {
            return Err(TradeError::invalid_order(format!(
                "`trigger` is not used by a {} order",
                self.order_type
            )));
        }
        match self.order_type {
            OrderType::TrailingStop | OrderType::TrailingStopLimit => {
                if !matches!(&self.price, Some(Price::Offset { value, .. }) if *value > Decimal::ZERO)
                {
                    return Err(TradeError::invalid_order(
                        "trailing-stop price must be a positive relative offset",
                    ));
                }
            }
            _ => require_absolute("price", self.price.as_ref())?,
        }
        require_absolute("price2", self.price2.as_ref())
    }
}

/// Place one Futures order (`add_order` on the Unified private socket).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FuturesOrderRequest {
    /// Futures wire symbol such as `PF_XBTUSD`; wire `symbol`.
    pub symbol: String,
    /// The order.
    pub order: FuturesOrder,
    /// Dry run: the venue validates and places nothing; wire `validate`, sent only when true.
    pub validate: bool,
}

impl FuturesOrderRequest {
    /// Construct from the wire symbol and the order; `validate` off.
    pub fn new(symbol: impl Into<String>, order: FuturesOrder) -> Self {
        Self {
            symbol: symbol.into(),
            order,
            validate: false,
        }
    }

    /// Dry run — the venue validates the order and places nothing.
    #[must_use]
    pub fn validate_only(mut self, validate: bool) -> Self {
        self.validate = validate;
        self
    }

    /// Client-side validator: a Futures wire symbol plus [`FuturesOrder::validate`]; failures
    /// are [`TradeError::InvalidOrder`].
    pub fn validate(&self) -> Result<(), TradeError> {
        validate_symbol(&self.symbol)?;
        self.order.validate()
    }
}

/// Place 2 to 15 Futures orders on one symbol in a single `batch_add`; each row
/// resolves on its own.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FuturesOrderBatchRequest {
    /// Futures wire symbol shared by every row; wire batch-level `symbol`.
    pub symbol: String,
    /// The rows, 2 to 15.
    pub orders: Vec<FuturesOrder>,
    /// Dry run: the venue validates every row and places nothing; wire `validate`, sent only when true.
    pub validate: bool,
}

impl FuturesOrderBatchRequest {
    /// Smallest and largest batch the venue accepts; a larger one makes it drop the socket.
    const SIZE: std::ops::RangeInclusive<usize> = 2..=15;

    /// Construct from the shared symbol and its rows; `validate` off.
    pub fn new(symbol: impl Into<String>, orders: Vec<FuturesOrder>) -> Self {
        Self {
            symbol: symbol.into(),
            orders,
            validate: false,
        }
    }

    /// Dry run — the venue validates every row and places nothing.
    #[must_use]
    pub fn validate_only(mut self, validate: bool) -> Self {
        self.validate = validate;
        self
    }

    /// Client-side validator: the 2 to 15 size bound ([`TradeError::BatchSizeOutOfRange`]),
    /// then a Futures wire symbol, no row `deadline` and every row's [`FuturesOrder::validate`]
    /// ([`TradeError::InvalidOrder`]).
    pub fn validate(&self) -> Result<(), TradeError> {
        TradeError::check_batch_size(self.orders.len(), Self::SIZE)?;
        validate_symbol(&self.symbol)?;
        if self.orders.iter().any(|order| order.deadline.is_some()) {
            return Err(TradeError::invalid_order(
                "`deadline` is not supported on a batch row",
            ));
        }
        self.orders.iter().try_for_each(FuturesOrder::validate)
    }
}

/// Names a Futures order for amend and cancel, by client or venue order id.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum FuturesOrderRef {
    /// The client order id; wire `cl_ord_id`.
    ClOrdId(crate::types::ClOrdId),
    /// The venue's order id; wire `order_id`.
    OrderId(crate::types::TxId),
}

impl FuturesOrderRef {
    pub(crate) fn cl_ord_id(&self) -> Option<&crate::types::ClOrdId> {
        match self {
            Self::ClOrdId(id) => Some(id),
            Self::OrderId(_) => None,
        }
    }

    pub(crate) fn wire(&self) -> (&'static str, &str) {
        match self {
            Self::ClOrdId(id) => ("cl_ord_id", id.as_str()),
            Self::OrderId(id) => ("order_id", id.as_str()),
        }
    }
}

impl From<crate::types::ClOrdId> for FuturesOrderRef {
    fn from(id: crate::types::ClOrdId) -> Self {
        Self::ClOrdId(id)
    }
}

impl From<crate::types::TxId> for FuturesOrderRef {
    fn from(id: crate::types::TxId) -> Self {
        Self::OrderId(id)
    }
}

/// Amend one Futures order. At least one of volume, limit price, post-only and trigger
/// price must be set.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct FuturesOrderAmendRequest {
    /// The order to amend. Not itself amendable.
    pub order: FuturesOrderRef,
    /// New order quantity; wire `order_qty`.
    pub order_volume: Option<Decimal>,
    /// New absolute limit price; wire `limit_price`.
    pub limit_price: Option<Price>,
    /// New post-only flag; wire `post_only`.
    pub post_only: Option<bool>,
    /// New absolute trigger price; wire `trigger_price`.
    pub trigger_price: Option<Price>,
    /// Wire `deadline` (RFC 3339), sent as given; the SDK never reads the local clock.
    pub deadline: Option<TimeSpec>,
}

impl FuturesOrderAmendRequest {
    fn new(order: FuturesOrderRef) -> Self {
        Self {
            order,
            order_volume: None,
            limit_price: None,
            post_only: None,
            trigger_price: None,
            deadline: None,
        }
    }

    /// Amend the order with this client order id.
    pub fn by_cl_ord_id(cl_ord_id: crate::types::ClOrdId) -> Self {
        Self::new(FuturesOrderRef::ClOrdId(cl_ord_id))
    }

    /// Amend the order with this venue order id.
    pub fn by_order_id(order_id: impl Into<crate::types::TxId>) -> Self {
        Self::new(FuturesOrderRef::OrderId(order_id.into()))
    }

    /// New order quantity.
    #[must_use]
    pub fn order_volume(mut self, order_volume: Decimal) -> Self {
        self.order_volume = Some(order_volume);
        self
    }

    /// New limit price; a `Decimal` converts to an absolute [`Price`].
    #[must_use]
    pub fn limit_price(mut self, limit_price: impl Into<Price>) -> Self {
        self.limit_price = Some(limit_price.into());
        self
    }

    /// New post-only flag.
    #[must_use]
    pub fn post_only(mut self, post_only: bool) -> Self {
        self.post_only = Some(post_only);
        self
    }

    /// New trigger price; a `Decimal` converts to an absolute [`Price`].
    #[must_use]
    pub fn trigger_price(mut self, trigger_price: impl Into<Price>) -> Self {
        self.trigger_price = Some(trigger_price.into());
        self
    }

    /// Set the deadline; see [`FuturesOrderAmendRequest::deadline`].
    #[must_use]
    pub fn deadline(mut self, deadline: TimeSpec) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Refuses an empty amend ([`TradeError::EmptyAmendRequest`]; `post_only(false)` alone is
    /// empty), and a non-positive quantity or a relative price ([`TradeError::InvalidOrder`]).
    pub fn validate(&self) -> Result<(), TradeError> {
        if self.order_volume.is_none()
            && self.limit_price.is_none()
            && self.post_only != Some(true)
            && self.trigger_price.is_none()
        {
            return Err(TradeError::EmptyAmendRequest);
        }
        if self.order_volume.is_some_and(|v| v <= Decimal::ZERO) {
            return Err(TradeError::invalid_order(
                "amended quantity must be positive",
            ));
        }
        require_absolute("limit_price", self.limit_price.as_ref())?;
        require_absolute("trigger_price", self.trigger_price.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::api::trade::ws_compose::{
        compose_futures_batch_params, compose_futures_order_params,
    };
    use crate::types::ClOrdId;

    fn qty(n: i64) -> Decimal {
        Decimal::from(n)
    }

    fn offset(unit: super::super::requests::PriceUnit, value: i64) -> Price {
        Price::Offset {
            unit,
            value: qty(value),
        }
    }

    fn base() -> FuturesOrder {
        FuturesOrder::new(Side::Buy, qty(1))
    }

    fn invalid(result: Result<(), TradeError>) -> bool {
        matches!(result, Err(TradeError::InvalidOrder { .. }))
    }

    #[test]
    fn an_order_is_valid_only_for_what_the_venue_accepts() {
        use super::super::requests::PriceUnit::{Percent, Quote};
        let limit = base().order_type(OrderType::Limit).price(qty(100));
        let stop = base().order_type(OrderType::StopLoss).price(qty(90));
        let stop_limit = base()
            .order_type(OrderType::StopLossLimit)
            .price(qty(90))
            .price2(qty(89));
        let trailing = base().order_type(OrderType::TrailingStop);
        let trailing_limit = base().order_type(OrderType::TrailingStopLimit);
        let stp = base().stp_type(StpType::CancelNewest);
        // (label, order, accepted)
        let cases = [
            ("market", base(), true),
            ("limit", limit.clone(), true),
            (
                "ioc limit",
                limit.clone().time_in_force(TimeInForce::Ioc),
                true,
            ),
            ("stop-loss", stop.clone(), true),
            ("stop-loss-limit", stop_limit.clone(), true),
            (
                "trailing pct",
                trailing.clone().price(offset(Percent, 5)),
                true,
            ),
            (
                "trailing-limit",
                trailing_limit
                    .clone()
                    .price(offset(Quote, 500))
                    .price2(qty(70_000)),
                true,
            ),
            ("cancel-newest stp", stp, true),
            (
                "stop-loss with trigger",
                stop.clone().trigger(TriggerKind::Last),
                true,
            ),
            (
                "take-profit-limit",
                base()
                    .order_type(OrderType::TakeProfitLimit)
                    .price(qty(110))
                    .price2(qty(109)),
                true,
            ),
            ("market with price", base().price(qty(100)), false),
            ("market with price2", base().price2(qty(100)), false),
            (
                "market with trigger",
                base().trigger(TriggerKind::Last),
                false,
            ),
            (
                "limit without price",
                base().order_type(OrderType::Limit),
                false,
            ),
            ("limit with price2", limit.clone().price2(qty(99)), false),
            (
                "limit with trigger",
                limit.clone().trigger(TriggerKind::Index),
                false,
            ),
            (
                "stop-loss without price",
                base().order_type(OrderType::StopLoss),
                false,
            ),
            ("stop-loss with price2", stop.clone().price2(qty(89)), false),
            (
                "stop-loss-limit without price2",
                base().order_type(OrderType::StopLossLimit).price(qty(90)),
                false,
            ),
            (
                "take-profit-limit without price2",
                base()
                    .order_type(OrderType::TakeProfitLimit)
                    .price(qty(110)),
                false,
            ),
            (
                "trailing with price2",
                trailing
                    .clone()
                    .price(offset(Percent, 5))
                    .price2(qty(70_000)),
                false,
            ),
            (
                "trailing-limit without price2",
                trailing_limit.clone().price(offset(Quote, 500)),
                false,
            ),
            ("zero quantity", FuturesOrder::new(Side::Buy, qty(0)), false),
            ("iceberg", base().order_type(OrderType::Iceberg), false),
            (
                "settle-position",
                base().order_type(OrderType::SettlePosition),
                false,
            ),
            ("unknown type", base().order_type(OrderType::Unknown), false),
            ("gtd", limit.clone().time_in_force(TimeInForce::Gtd), false),
            ("fok", limit.clone().time_in_force(TimeInForce::Fok), false),
            (
                "limit offset price",
                limit.clone().price(offset(Quote, 5)),
                false,
            ),
            (
                "stop offset trigger",
                stop.clone().price(offset(Percent, 2)),
                false,
            ),
            (
                "limit-leg offset",
                stop_limit.clone().price2(offset(Quote, 5)),
                false,
            ),
            ("trailing absolute", trailing.clone().price(qty(5)), false),
            ("trailing without price", trailing.clone(), false),
            (
                "trailing negative offset",
                trailing.clone().price(offset(Percent, -5)),
                false,
            ),
            (
                "trailing-limit offset limit leg",
                trailing_limit
                    .price(offset(Quote, 500))
                    .price2(offset(Quote, 5)),
                false,
            ),
        ];
        for (label, order, accepted) in cases {
            let result = order.validate();
            assert_eq!(result.is_ok(), accepted, "{label}: {result:?}");
            assert!(accepted || invalid(result), "{label}");
        }
        assert!(
            invalid(base().stp_type(StpType::CancelOldest).validate()),
            "cancel-oldest stp"
        );
    }

    #[test]
    fn a_request_needs_a_futures_wire_symbol_and_a_batch_two_to_fifteen_rows() {
        for (symbol, accepted) in [
            ("PF_XBTUSD", true),
            ("PI_XBTUSD", true),
            ("BTC/USD", false),
            ("OF_XBTUSD_231229", false),
            ("", false),
        ] {
            let request = FuturesOrderRequest::new(symbol, base());
            assert_eq!(request.validate().is_ok(), accepted, "{symbol:?}");
            let batch = FuturesOrderBatchRequest::new(symbol, vec![base(), base()]);
            assert_eq!(batch.validate().is_ok(), accepted, "batch {symbol:?}");
        }
        for (rows, accepted) in [(0, false), (1, false), (2, true), (15, true), (16, false)] {
            let batch = FuturesOrderBatchRequest::new("PF_XBTUSD", vec![base(); rows]);
            let result = batch.validate();
            assert_eq!(result.is_ok(), accepted, "{rows} rows");
            assert!(
                accepted
                    || matches!(
                        result,
                        Err(TradeError::BatchSizeOutOfRange { min: 2, max: 15 })
                    ),
                "{rows} rows"
            );
        }
        let bad_row = FuturesOrderBatchRequest::new(
            "PF_XBTUSD",
            vec![base(), FuturesOrder::new(Side::Buy, qty(0))],
        );
        assert!(invalid(bad_row.validate()));
        let timed_row = FuturesOrderBatchRequest::new(
            "PF_XBTUSD",
            vec![base(), base().deadline(TimeSpec::from_unix_secs(1))],
        );
        assert!(
            invalid(timed_row.validate()),
            "the venue rejects a row deadline"
        );
    }

    #[test]
    fn an_amend_is_refused_locally_for_what_would_cancel_or_be_ignored() {
        use super::super::requests::PriceUnit::Quote;
        let id = ClOrdId::new("test-cl-ord-id-001").unwrap();
        let amend = || FuturesOrderAmendRequest::by_cl_ord_id(id.clone());
        // (label, request, accepted)
        let cases = [
            ("quantity", amend().order_volume(qty(2)), true),
            ("absolute limit price", amend().limit_price(qty(100)), true),
            (
                "absolute trigger price",
                amend().trigger_price(qty(100)),
                true,
            ),
            ("zero quantity", amend().order_volume(qty(0)), false),
            ("negative quantity", amend().order_volume(qty(-1)), false),
            (
                "offset limit price",
                amend().limit_price(offset(Quote, 5)),
                false,
            ),
            (
                "offset trigger price",
                amend().trigger_price(offset(Quote, 5)),
                false,
            ),
        ];
        for (label, request, accepted) in cases {
            let result = request.validate();
            assert_eq!(result.is_ok(), accepted, "{label}: {result:?}");
            assert!(accepted || invalid(result), "{label}");
        }
        assert!(matches!(
            amend().validate(),
            Err(TradeError::EmptyAmendRequest)
        ));
        assert!(
            matches!(
                amend().deadline(TimeSpec::from_unix_secs(1)).validate(),
                Err(TradeError::EmptyAmendRequest)
            ),
            "a deadline alone amends nothing"
        );
        assert!(
            matches!(
                amend().post_only(false).validate(),
                Err(TradeError::EmptyAmendRequest)
            ),
            "the venue reads post_only false alone as no arguments"
        );
        assert!(amend().post_only(true).validate().is_ok());
        assert!(
            amend()
                .post_only(false)
                .order_volume(qty(2))
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn an_amend_names_its_order_by_exactly_one_id() {
        let deadline = TimeSpec::from_unix_secs(1_767_225_600);
        // (request, composed frame)
        let cases = [
            (
                FuturesOrderAmendRequest::by_cl_ord_id(ClOrdId::new("test-cl-ord-id-001").unwrap())
                    .order_volume(qty(2)),
                json!({ "cl_ord_id": "test-cl-ord-id-001", "order_qty": 2 }),
            ),
            (
                FuturesOrderAmendRequest::by_order_id(crate::types::TxId::new("a3270acf-5190"))
                    .order_volume(qty(2))
                    .deadline(deadline),
                json!({
                    "order_id": "a3270acf-5190", "order_qty": 2,
                    "deadline": "2026-01-01T00:00:00Z",
                }),
            ),
        ];
        for (request, expected) in cases {
            assert_eq!(
                crate::api::trade::ws_compose::compose_futures_amend_params(&request),
                expected,
                "{request:?}"
            );
        }
    }

    #[test]
    fn a_cancel_names_its_order_by_exactly_one_id() {
        let compose = crate::api::trade::ws_compose::compose_futures_cancel_params;
        let by_client: FuturesOrderRef = ClOrdId::new("test-cl-ord-id-001").unwrap().into();
        let by_venue: FuturesOrderRef = crate::types::TxId::new("a3270acf-5190").into();
        assert_eq!(
            compose(&by_client),
            json!({ "cl_ord_id": ["test-cl-ord-id-001"] })
        );
        assert_eq!(compose(&by_venue), json!({ "order_id": ["a3270acf-5190"] }));
    }

    #[test]
    fn composed_frames_match_the_probed_wire_shapes() {
        use super::super::requests::PriceUnit::{Percent, Quote};
        let id = ClOrdId::new("test-cl-ord-id-001").unwrap();
        let with_id = |order: FuturesOrder| order.cl_ord_id(id.clone());
        let tail = json!({ "symbol": "PF_XBTUSD", "cl_ord_id": "test-cl-ord-id-001" });
        let merge = |mut head: serde_json::Value| {
            head.as_object_mut()
                .unwrap()
                .extend(tail.as_object().unwrap().clone());
            head
        };
        let mut protected = with_id(
            FuturesOrder::new(Side::Sell, qty(2))
                .order_type(OrderType::Limit)
                .price(qty(90_000))
                .time_in_force(TimeInForce::Ioc)
                .post_only(true)
                .reduce_only(true),
        );
        protected.stp_type = Some(StpType::CancelNewest);
        let iiban = "AAAA BBBB CCCC DDDD";
        // (order, dry run, composed frame)
        let cases = [
            (
                with_id(base()),
                false,
                merge(json!({ "order_type": "market", "side": "buy", "order_qty": 1 })),
            ),
            (
                with_id(base().broker(iiban)),
                false,
                merge(json!({
                    "order_type": "market", "side": "buy", "order_qty": 1, "broker": iiban,
                })),
            ),
            (
                with_id(base().deadline(TimeSpec::from_unix_secs(1_767_225_600))),
                false,
                merge(json!({
                    "order_type": "market", "side": "buy", "order_qty": 1,
                    "deadline": "2026-01-01T00:00:00Z",
                })),
            ),
            (
                protected,
                true,
                merge(json!({
                    "order_type": "limit", "side": "sell", "order_qty": 2,
                    "limit_price": 90000, "limit_price_type": "static",
                    "post_only": true, "reduce_only": true, "stp_type": "cancel_newest",
                    "time_in_force": "ioc", "validate": true,
                })),
            ),
            (
                with_id(
                    FuturesOrder::new(Side::Sell, qty(1))
                        .order_type(OrderType::StopLossLimit)
                        .trigger(TriggerKind::Last)
                        .price(qty(72_000))
                        .price2(qty(71_500)),
                ),
                false,
                merge(json!({
                    "order_type": "stop-loss-limit", "side": "sell", "order_qty": 1,
                    "triggers": { "reference": "last", "price": 72000, "price_type": "static" },
                    "limit_price": 71500, "limit_price_type": "static",
                })),
            ),
            (
                with_id(
                    FuturesOrder::new(Side::Sell, qty(1))
                        .order_type(OrderType::TakeProfit)
                        .trigger(TriggerKind::Index)
                        .price(qty(90_000)),
                ),
                false,
                merge(json!({
                    "order_type": "take-profit", "side": "sell", "order_qty": 1,
                    "triggers": { "reference": "index", "price": 90000, "price_type": "static" },
                })),
            ),
            (
                with_id(
                    FuturesOrder::new(Side::Sell, qty(1))
                        .order_type(OrderType::TrailingStop)
                        .trigger(TriggerKind::Last)
                        .price(offset(Percent, 5)),
                ),
                false,
                merge(json!({
                    "order_type": "trailing-stop", "side": "sell", "order_qty": 1,
                    "triggers": { "reference": "last", "price": 5, "price_type": "pct" },
                })),
            ),
            (
                with_id(
                    FuturesOrder::new(Side::Sell, qty(1))
                        .order_type(OrderType::TrailingStopLimit)
                        .trigger(TriggerKind::Last)
                        .price(offset(Quote, 500))
                        .price2(qty(70_000)),
                ),
                false,
                merge(json!({
                    "order_type": "trailing-stop-limit", "side": "sell", "order_qty": 1,
                    "triggers": { "reference": "last", "price": 500, "price_type": "quote" },
                    "limit_price": 70000, "limit_price_type": "static",
                })),
            ),
        ];
        for (order, validate, expected) in cases {
            assert_eq!(
                compose_futures_order_params("PF_XBTUSD", &order, validate),
                expected,
                "{order:?}"
            );
        }

        let batch = FuturesOrderBatchRequest::new(
            "PF_XBTUSD",
            vec![
                with_id(base()),
                with_id(base().post_only(true).broker(iiban)),
            ],
        )
        .validate_only(true);
        assert_eq!(
            compose_futures_batch_params(&batch),
            json!({
                "symbol": "PF_XBTUSD",
                "validate": true,
                "orders": [
                    { "order_type": "market", "side": "buy", "order_qty": 1,
                      "cl_ord_id": "test-cl-ord-id-001" },
                    { "order_type": "market", "side": "buy", "order_qty": 1,
                      "cl_ord_id": "test-cl-ord-id-001", "post_only": true, "broker": iiban },
                ],
            })
        );
    }
}
