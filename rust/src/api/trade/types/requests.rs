use rust_decimal::Decimal;

use super::super::ws_compose::{rest_unrepresentable_field, ws_unrepresentable_field};
use super::enums::{OFlag, OrderType, Side, StpType, TimeInForce, TriggerKind, oflags_to_wire};

/// Order types that carry a limit price — the only ones FOK is valid on.
fn is_limit_price_bearing(order_type: OrderType) -> bool {
    matches!(
        order_type,
        OrderType::Limit
            | OrderType::Iceberg
            | OrderType::StopLossLimit
            | OrderType::TakeProfitLimit
            | OrderType::TrailingStopLimit
    )
}

/// A `/AddOrder.deadline` offset from "now". The server-clock wire render is
/// deferred, so v1 rejects any set `deadline` at `validate()`. See docs/guides/placing-orders.md.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadlineSpec {
    #[allow(dead_code)]
    offset: std::time::Duration,
}

impl DeadlineSpec {
    /// Construct a deadline `offset` into the future; Kraken requires `0 < offset ≤ 60s`.
    pub fn after(offset: std::time::Duration) -> Self {
        Self { offset }
    }
}

/// An absolute instant as Unix epoch seconds; each transport renders its own
/// wire form. See docs/guides/wire-quirks.md.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeSpec(i64);

impl TimeSpec {
    /// An absolute instant as Unix epoch seconds. A negative value is clamped to
    /// the epoch so no malformed timestamp is emitted; the exchange then rejects it.
    #[must_use]
    pub fn from_unix_secs(secs: i64) -> Self {
        Self(secs.max(0))
    }

    /// An absolute instant from a [`SystemTime`](std::time::SystemTime); errors with
    /// [`SystemTimeError`](std::time::SystemTimeError) when `t` predates the Unix epoch.
    pub fn from_system_time(t: std::time::SystemTime) -> Result<Self, std::time::SystemTimeError> {
        // Saturate rather than wrap: a plain `as i64` would flip a far-future instant to a past date.
        let secs = t.duration_since(std::time::UNIX_EPOCH)?.as_secs();
        Ok(Self(i64::try_from(secs).unwrap_or(i64::MAX)))
    }

    /// REST `starttm` / `expiretm` value — the Unix epoch seconds integer.
    pub(crate) fn to_rest_form(self) -> String {
        self.0.to_string()
    }

    /// WS `effective_time` / `expire_time` value — RFC 3339 with a `Z` designator.
    pub(crate) fn to_ws(self) -> String {
        unix_secs_to_rfc3339_utc(self.0)
    }
}

/// Render Unix epoch seconds as `YYYY-MM-DDThh:mm:ssZ` (UTC) via Howard Hinnant's
/// `civil_from_days` (public domain) — no datetime dependency needed.
fn unix_secs_to_rfc3339_utc(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (hh, mi, ss) = (tod / 3_600, (tod % 3_600) / 60, tod % 60);

    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = yoe + era * 400 + i64::from(m <= 2);

    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mi:02}:{ss:02}Z")
}

/// A price-field value: absolute, or a relative offset from the reference price.
/// Wire encoding is per-transport: docs/guides/wire-quirks.md.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Price {
    /// Absolute price. REST: plain number (`"30000"`); WS: `price_type = "static"`.
    Absolute(Decimal),
    /// Relative offset from the reference price; `value` sign is the direction.
    /// REST: `"+150"` / `"-150"` (quote) or `"+1.0%"` / `"-2.0%"` (percent);
    /// WS: the signed numeric + `price_type = "quote"` | `"pct"`.
    Offset {
        /// Offset unit — quote-currency notional or percentage.
        unit: PriceUnit,
        /// Signed offset magnitude; the sign gives the direction from the reference price.
        value: Decimal,
    },
}

/// Unit for a relative [`Price::Offset`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceUnit {
    /// Notional offset in the quote currency. REST: no suffix; WS `price_type "quote"`.
    Quote,
    /// Percentage offset. REST: `%` suffix; WS `price_type "pct"`.
    Percent,
}

impl From<Decimal> for Price {
    fn from(d: Decimal) -> Self {
        Price::Absolute(d)
    }
}

impl Price {
    /// REST form-encoded value. Kraken requires the explicit `+` on a relative price.
    pub(crate) fn to_rest_form(self) -> String {
        match self {
            Price::Absolute(d) => d.to_string(),
            Price::Offset { unit, value } => {
                let signed = if value.is_sign_negative() {
                    value.to_string()
                } else {
                    format!("+{value}")
                };
                match unit {
                    PriceUnit::Quote => signed,
                    PriceUnit::Percent => format!("{signed}%"),
                }
            }
        }
    }

    /// WS `(numeric value, price_type)`. Direction rides the numeric sign.
    pub(crate) fn to_ws(self) -> (Decimal, &'static str) {
        match self {
            Price::Absolute(d) => (d, "static"),
            Price::Offset {
                unit: PriceUnit::Quote,
                value,
            } => (value, "quote"),
            Price::Offset {
                unit: PriceUnit::Percent,
                value,
            } => (value, "pct"),
        }
    }
}

/// `/0/private/AddOrder` request.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
#[allow(missing_docs)]
pub struct OrderRequest {
    pub side: Side,
    pub pair: crate::types::Symbol,
    pub volume: Decimal,
    pub order_type: OrderType,
    pub price: Option<Price>,
    pub price2: Option<Price>,
    pub leverage: Option<u8>,
    pub margin: bool,
    pub oflags: Vec<OFlag>,
    pub reduce_only: Option<bool>,
    pub stp_type: Option<StpType>,
    pub trigger: Option<TriggerKind>,
    pub time_in_force: Option<TimeInForce>,
    pub display_vol: Option<Decimal>,
    pub start_time: Option<TimeSpec>,
    pub expire_time: Option<TimeSpec>,
    pub deadline: Option<DeadlineSpec>,
    pub userref: Option<i32>,
    pub cl_ord_id: Option<crate::types::ClOrdId>,
    pub conditional_close: Option<ConditionalClose>,
    pub validate: bool,
}

/// Shared `to_form` body for the two AddOrder request types; `side` is supplied
/// by the namespace method. Omits every `None`/empty field.
#[allow(clippy::too_many_arguments)]
fn add_order_to_form(
    pair: &crate::types::Symbol,
    volume: &Decimal,
    order_type: OrderType,
    price: &Option<Price>,
    price2: &Option<Price>,
    leverage: &Option<u8>,
    oflags: &[OFlag],
    reduce_only: &Option<bool>,
    stp_type: &Option<StpType>,
    trigger: &Option<TriggerKind>,
    time_in_force: &Option<TimeInForce>,
    display_vol: &Option<Decimal>,
    start_time: &Option<TimeSpec>,
    expire_time: &Option<TimeSpec>,
    userref: &Option<i32>,
    cl_ord_id: &Option<crate::types::ClOrdId>,
    conditional_close: &Option<ConditionalClose>,
    validate: bool,
) -> Vec<(String, String)> {
    let mut form: Vec<(String, String)> = Vec::new();
    form.push(("pair".into(), pair.as_str().to_string()));
    form.push(("volume".into(), volume.to_string()));
    form.push(("ordertype".into(), order_type.to_string()));
    if let Some(p) = price {
        form.push(("price".into(), p.to_rest_form()));
    }
    if let Some(p) = price2 {
        form.push(("price2".into(), p.to_rest_form()));
    }
    if let Some(l) = leverage {
        form.push(("leverage".into(), l.to_string()));
    }
    if let Some(csv) = oflags_to_wire(oflags) {
        form.push(("oflags".into(), csv));
    }
    if *reduce_only == Some(true) {
        form.push(("reduce_only".into(), "true".into()));
    }
    if let Some(s) = stp_type {
        form.push(("stptype".into(), s.to_string()));
    }
    if let Some(t) = trigger {
        form.push(("trigger".into(), t.to_string()));
    }
    if let Some(tif) = time_in_force {
        form.push(("timeinforce".into(), tif.to_string()));
    }
    if let Some(d) = display_vol {
        form.push(("displayvol".into(), d.to_string()));
    }
    if let Some(s) = start_time {
        form.push(("starttm".into(), s.to_rest_form()));
    }
    if let Some(e) = expire_time {
        form.push(("expiretm".into(), e.to_rest_form()));
    }
    if let Some(u) = userref {
        form.push(("userref".into(), u.to_string()));
    }
    if let Some(c) = cl_ord_id {
        form.push(("cl_ord_id".into(), c.as_str().to_string()));
    }
    if let Some(c) = conditional_close {
        c.push_form("close", &mut form);
    }
    if validate {
        form.push(("validate".into(), "true".into()));
    }
    // deadline → SKIP: server-clock render deferred.
    form
}

/// Toggle [`OFlag::Post`] in an `oflags` set — shared by the buy/sell `post_only` sugar.
fn set_post_only(oflags: &mut Vec<OFlag>, on: bool) {
    if on {
        if !oflags.contains(&OFlag::Post) {
            oflags.push(OFlag::Post);
        }
    } else {
        oflags.retain(|f| *f != OFlag::Post);
    }
}

/// Stamp the chainable setters onto the two field-identical AddOrder requests so
/// the buy/sell surface can't drift. `post_only` (hand-written `oflags` sugar) and
/// `deadline` (rejected by `validate()` in v1) are excluded.
macro_rules! add_order_setters {
    ($t:ty) => {
        impl $t {
            /// Primary price leg — absolute [`Price`] or relative offset; `Decimal` converts.
            #[must_use]
            pub fn price(mut self, price: impl Into<Price>) -> Self {
                self.price = Some(price.into());
                self
            }

            /// Secondary price leg (the limit leg of a triggered-limit order type).
            #[must_use]
            pub fn price2(mut self, price2: impl Into<Price>) -> Self {
                self.price2 = Some(price2.into());
                self
            }

            /// Margin leverage ratio (REST-only; WS uses [`Self::margin`]).
            #[must_use]
            pub fn leverage(mut self, leverage: u8) -> Self {
                self.leverage = Some(leverage);
                self
            }

            /// WS-native margin toggle — funds at the pair's max leverage.
            #[must_use]
            pub fn margin(mut self, margin: bool) -> Self {
                self.margin = margin;
                self
            }

            /// Replace the order-flag set (drops any flag set earlier, `post_only` included).
            #[must_use]
            pub fn oflags(mut self, oflags: Vec<OFlag>) -> Self {
                self.oflags = oflags;
                self
            }

            /// Reduce-only: close against an open margin position, never open one.
            #[must_use]
            pub fn reduce_only(mut self, reduce_only: bool) -> Self {
                self.reduce_only = Some(reduce_only);
                self
            }

            /// Self-trade-prevention behaviour.
            #[must_use]
            pub fn stp_type(mut self, stp_type: StpType) -> Self {
                self.stp_type = Some(stp_type);
                self
            }

            /// Price signal (`last`/`index`) that fires a triggered order type.
            #[must_use]
            pub fn trigger(mut self, trigger: TriggerKind) -> Self {
                self.trigger = Some(trigger);
                self
            }

            /// Time-in-force; FOK is limit-types-only (rejected by `validate()` otherwise).
            #[must_use]
            pub fn time_in_force(mut self, time_in_force: TimeInForce) -> Self {
                self.time_in_force = Some(time_in_force);
                self
            }

            /// Visible iceberg slice in base currency.
            #[must_use]
            pub fn display_vol(mut self, display_vol: Decimal) -> Self {
                self.display_vol = Some(display_vol);
                self
            }

            /// Scheduled activation instant.
            #[must_use]
            pub fn start_time(mut self, start_time: TimeSpec) -> Self {
                self.start_time = Some(start_time);
                self
            }

            /// Expiry instant.
            #[must_use]
            pub fn expire_time(mut self, expire_time: TimeSpec) -> Self {
                self.expire_time = Some(expire_time);
                self
            }

            /// Numeric client tag; mutually exclusive with [`Self::cl_ord_id`].
            #[must_use]
            pub fn userref(mut self, userref: i32) -> Self {
                self.userref = Some(userref);
                self
            }

            /// Client order id; mutually exclusive with [`Self::userref`].
            #[must_use]
            pub fn cl_ord_id(mut self, cl_ord_id: crate::types::ClOrdId) -> Self {
                self.cl_ord_id = Some(cl_ord_id);
                self
            }

            /// Conditional-close (OTO) leg for this order.
            #[must_use]
            pub fn conditional_close(mut self, conditional_close: ConditionalClose) -> Self {
                self.conditional_close = Some(conditional_close);
                self
            }

            /// Dry run — the exchange validates the order and places nothing.
            #[must_use]
            pub fn validate_only(mut self, validate: bool) -> Self {
                self.validate = validate;
                self
            }
        }
    };
}

add_order_setters!(OrderRequest);

#[allow(missing_docs)]
impl OrderRequest {
    pub fn new(pair: crate::types::Symbol, volume: Decimal, side: Side) -> Self {
        Self {
            side,
            pair,
            volume,
            order_type: OrderType::Market,
            price: None,
            price2: None,
            leverage: None,
            margin: false,
            oflags: vec![],
            reduce_only: None,
            stp_type: None,
            trigger: None,
            time_in_force: None,
            display_vol: None,
            start_time: None,
            expire_time: None,
            deadline: None,
            userref: None,
            cl_ord_id: None,
            conditional_close: None,
            validate: false,
        }
    }

    #[must_use]
    pub fn order_type(mut self, order_type: OrderType) -> Self {
        self.order_type = order_type;
        self
    }

    #[must_use]
    pub fn post_only(mut self, on: bool) -> Self {
        set_post_only(&mut self.oflags, on);
        self
    }

    pub fn validate(&self) -> Result<(), super::super::error::TradeError> {
        reject_unsupported_deadline(self.deadline.as_ref())?;
        validate_add_order(
            self.userref.is_some(),
            self.cl_ord_id.is_some(),
            self.order_type,
            self.leverage,
            self.margin,
            self.reduce_only,
            self.time_in_force,
            self.price.as_ref(),
            self.price2.as_ref(),
            &self.conditional_close,
        )
    }

    pub(crate) fn to_form(&self) -> Vec<(String, String)> {
        let mut form = add_order_to_form(
            &self.pair,
            &self.volume,
            self.order_type,
            &self.price,
            &self.price2,
            &self.leverage,
            &self.oflags,
            &self.reduce_only,
            &self.stp_type,
            &self.trigger,
            &self.time_in_force,
            &self.display_vol,
            &self.start_time,
            &self.expire_time,
            &self.userref,
            &self.cl_ord_id,
            &self.conditional_close,
            self.validate,
        );
        form.push(("type".into(), self.side.to_string()));
        form
    }
}

/// Shared trailing-price guard: trailing orders need a positive relative trigger
/// offset (direction is automatic from side). See docs/guides/placing-orders.md.
fn validate_trailing_price(
    order_type: OrderType,
    price: Option<&Price>,
    price2: Option<&Price>,
) -> Result<(), super::super::error::TradeError> {
    use super::super::error::TradeError;
    if !matches!(
        order_type,
        OrderType::TrailingStop | OrderType::TrailingStopLimit
    ) {
        return Ok(());
    }
    match price {
        Some(Price::Offset { value, .. }) if *value > Decimal::ZERO => {}
        Some(Price::Offset { .. }) => {
            return Err(TradeError::InvalidOrder {
                request_id: None,
                detail: "trailing-stop price offset must be positive; direction is automatic \
                         from the order side"
                    .to_string(),
            });
        }
        Some(Price::Absolute(_)) => {
            return Err(TradeError::InvalidOrder {
                request_id: None,
                detail: "trailing-stop price must be a relative offset, not an absolute price"
                    .to_string(),
            });
        }
        None => {
            return Err(TradeError::InvalidOrder {
                request_id: None,
                detail: "trailing-stop requires a relative price offset".to_string(),
            });
        }
    }
    if order_type == OrderType::TrailingStopLimit && price2.is_none() {
        return Err(TradeError::InvalidOrder {
            request_id: None,
            detail: "trailing-stop-limit requires a relative price2 (limit leg)".to_string(),
        });
    }
    if let Some(Price::Absolute(_)) = price2 {
        return Err(TradeError::InvalidOrder {
            request_id: None,
            detail: "trailing-stop-limit price2 must be a relative offset, not an absolute price"
                .to_string(),
        });
    }
    Ok(())
}

/// Reject a caller-set `deadline`: its wire render needs a server-corrected clock
/// that isn't wired yet, so v1 rejects rather than silently dropping.
fn reject_unsupported_deadline(
    deadline: Option<&DeadlineSpec>,
) -> Result<(), super::super::error::TradeError> {
    if deadline.is_some() {
        return Err(super::super::error::TradeError::InvalidOrder {
            request_id: None,
            detail: "the `deadline` order parameter is not yet supported in v1 \
                     (server-clock rendering is deferred); omit it"
                .to_string(),
        });
    }
    Ok(())
}

/// Shared AddOrder validator.
#[allow(clippy::too_many_arguments)]
fn validate_add_order(
    has_userref: bool,
    has_cl_ord_id: bool,
    order_type: OrderType,
    leverage: Option<u8>,
    margin: bool,
    reduce_only: Option<bool>,
    time_in_force: Option<TimeInForce>,
    price: Option<&Price>,
    price2: Option<&Price>,
    conditional_close: &Option<ConditionalClose>,
) -> Result<(), super::super::error::TradeError> {
    use super::super::error::TradeError;
    if has_userref && has_cl_ord_id {
        return Err(TradeError::ConflictingOrderIdentifiers);
    }
    // `margin` is WS-only; `leverage` / relative close price are REST-only —
    // an order needing both has no single transport.
    let (_, close_price, close_price2) = ConditionalClose::flat_legs(conditional_close);
    if let (Some(rf), Some(wf)) = (
        rest_unrepresentable_field(margin),
        ws_unrepresentable_field(
            leverage.is_some(),
            close_price.as_ref(),
            close_price2.as_ref(),
        ),
    ) {
        return Err(TradeError::InvalidOrder {
            request_id: None,
            detail: format!(
                "order is un-placeable: `{rf}` can only be sent on WS while `{wf}` can only be \
                 sent on REST — no single transport supports both. Drop one."
            ),
        });
    }
    validate_order_gates(order_type, leverage, margin, reduce_only, time_in_force)?;
    validate_trailing_price(order_type, price, price2)?;
    if let Some(c) = conditional_close {
        validate_trailing_price(
            c.ordertype.to_order_type(),
            Some(&c.price),
            c.price2.as_ref(),
        )?;
    }
    Ok(())
}

/// Per-order field gates shared by the single-order and batch validators.
fn validate_order_gates(
    order_type: OrderType,
    leverage: Option<u8>,
    margin: bool,
    reduce_only: Option<bool>,
    time_in_force: Option<TimeInForce>,
) -> Result<(), super::super::error::TradeError> {
    use super::super::error::TradeError;
    if matches!(order_type, OrderType::Unknown) {
        return Err(TradeError::InvalidOrder {
            request_id: None,
            detail: "`OrderType::Unknown` is decode-only and cannot be placed".to_string(),
        });
    }
    if order_type == OrderType::SettlePosition && leverage.is_none_or(|l| l == 0) {
        return Err(TradeError::SettlePositionRequiresLeverage);
    }
    // reduce_only needs an open margin position: leverage > 1 (REST) or the WS `margin` toggle.
    if reduce_only == Some(true) && leverage.is_none_or(|l| l <= 1) && !margin {
        return Err(TradeError::ReduceOnlyRequiresLeverage);
    }
    if time_in_force == Some(TimeInForce::Fok) && !is_limit_price_bearing(order_type) {
        return Err(TradeError::InvalidOrder {
            request_id: None,
            detail: "fill-or-kill (fok) time-in-force is only valid on limit order types \
                     (limit, iceberg, stop-loss-limit, take-profit-limit, trailing-stop-limit)"
                .to_string(),
        });
    }
    Ok(())
}

/// `/0/private/AmendOrder` request. Identity is `cl_ord_id`; at least one
/// mutable field must be set. `display_qty` is REST-only.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct OrderAmendRequest {
    /// Identity of the order to amend; wire `cl_ord_id`. Not itself amendable.
    pub cl_ord_id: crate::types::ClOrdId,
    /// New order quantity; wire `order_qty`.
    pub order_volume: Option<Decimal>,
    /// New limit price; wire `limit_price`.
    pub limit_price: Option<Price>,
    /// New post-only flag; wire `post_only`, sent as `true`/`false` when set.
    pub post_only: Option<bool>,
    /// New trigger price; wire `trigger_price`.
    pub trigger_price: Option<Price>,
    /// Iceberg visible-slice size. REST wire `display_qty`; omitted when `None`.
    pub display_qty: Option<Decimal>,
}

impl OrderAmendRequest {
    /// Construct with the identity `cl_ord_id`; all mutable fields empty.
    pub fn new(cl_ord_id: crate::types::ClOrdId) -> Self {
        Self {
            cl_ord_id,
            order_volume: None,
            limit_price: None,
            post_only: None,
            trigger_price: None,
            display_qty: None,
        }
    }

    /// New order quantity.
    #[must_use]
    pub fn order_volume(mut self, order_volume: Decimal) -> Self {
        self.order_volume = Some(order_volume);
        self
    }

    /// New limit price; `Decimal` converts to an absolute [`Price`].
    #[must_use]
    pub fn limit_price(mut self, limit_price: impl Into<Price>) -> Self {
        self.limit_price = Some(limit_price.into());
        self
    }

    /// New post-only flag — sent as `true`/`false`, unlike the AddOrder `oflags` form.
    #[must_use]
    pub fn post_only(mut self, post_only: bool) -> Self {
        self.post_only = Some(post_only);
        self
    }

    /// New trigger price.
    #[must_use]
    pub fn trigger_price(mut self, trigger_price: impl Into<Price>) -> Self {
        self.trigger_price = Some(trigger_price.into());
        self
    }

    /// New iceberg visible-slice size (REST-only).
    #[must_use]
    pub fn display_qty(mut self, display_qty: Decimal) -> Self {
        self.display_qty = Some(display_qty);
        self
    }

    /// Client-side validator — at least one mutable field must be `Some`, else
    /// [`EmptyAmendRequest`](super::super::error::TradeError::EmptyAmendRequest).
    pub fn validate(&self) -> Result<(), super::super::error::TradeError> {
        if self.order_volume.is_none()
            && self.limit_price.is_none()
            && self.post_only.is_none()
            && self.trigger_price.is_none()
            && self.display_qty.is_none()
        {
            return Err(super::super::error::TradeError::EmptyAmendRequest);
        }
        Ok(())
    }

    /// Form-encode for AmendOrder; omits unset mutable fields.
    pub(crate) fn to_form(&self) -> Vec<(String, String)> {
        let mut form: Vec<(String, String)> =
            vec![("cl_ord_id".into(), self.cl_ord_id.as_str().to_string())];
        if let Some(v) = &self.order_volume {
            form.push(("order_qty".into(), v.to_string()));
        }
        if let Some(p) = &self.limit_price {
            form.push(("limit_price".into(), p.to_rest_form()));
        }
        if let Some(po) = self.post_only {
            form.push(("post_only".into(), if po { "true" } else { "false" }.into()));
        }
        if let Some(t) = &self.trigger_price {
            form.push(("trigger_price".into(), t.to_rest_form()));
        }
        if let Some(d) = &self.display_qty {
            form.push(("display_qty".into(), d.to_string()));
        }
        form
    }
}

/// `/0/private/CancelOrder` request by `cl_ord_id`.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CancelRequest {
    /// Client order id of the order to cancel; wire `cl_ord_id`.
    pub cl_ord_id: crate::types::ClOrdId,
}

impl CancelRequest {
    /// Construct from the client order id of the order to cancel.
    pub fn new(cl_ord_id: crate::types::ClOrdId) -> Self {
        Self { cl_ord_id }
    }

    /// Form-encode — single `cl_ord_id` key.
    pub(crate) fn to_form(&self) -> Vec<(String, String)> {
        vec![("cl_ord_id".into(), self.cl_ord_id.as_str().to_string())]
    }
}

/// `/0/private/CancelAll` request — unit-equivalent. No params beyond auth;
/// carried as a struct for `PendingTrade<Req, Resp>` parity.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct CancelAllRequest;

impl CancelAllRequest {
    /// Form-encode — no params (CancelAll takes only auth).
    pub(crate) fn to_form(&self) -> Vec<(String, String)> {
        Vec::new()
    }
}

/// `/0/private/CancelAllOrdersAfter` dead-man's-switch request.
/// `timeout_secs == 0` disarms.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DeadmanRequest {
    /// Countdown in whole seconds; wire `timeout`; `0` disarms the switch.
    pub timeout_secs: u32,
}

impl DeadmanRequest {
    /// Construct with the countdown in whole seconds; `0` disarms the switch.
    pub fn new(timeout_secs: u32) -> Self {
        Self { timeout_secs }
    }

    /// Form-encode — `timeout` = seconds as decimal.
    pub(crate) fn to_form(&self) -> Vec<(String, String)> {
        vec![("timeout".into(), self.timeout_secs.to_string())]
    }
}

/// Order-type subset valid as a conditional-close arm. A trailing close needs a
/// relative price, which the WS path cannot express — place it on REST.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[strum(serialize_all = "kebab-case")]
#[non_exhaustive]
pub enum CloseOrderType {
    /// Wire `limit` — close at a fixed price.
    Limit,
    /// Wire `stop-loss` — market close once the stop trigger is touched.
    StopLoss,
    /// Wire `take-profit` — market close once the profit trigger is touched.
    TakeProfit,
    /// Wire `stop-loss-limit` — limit close (limit leg in `price2`) after the stop trigger.
    StopLossLimit,
    /// Wire `take-profit-limit` — limit close after the profit trigger.
    TakeProfitLimit,
    /// Wire `trailing-stop` — REST-only close; requires a positive relative price.
    TrailingStop,
    /// Wire `trailing-stop-limit` — REST-only close; both price legs relative.
    TrailingStopLimit,
}

impl CloseOrderType {
    /// The corresponding [`OrderType`], for the shared validators and the WS flatten.
    pub(crate) fn to_order_type(self) -> OrderType {
        match self {
            CloseOrderType::Limit => OrderType::Limit,
            CloseOrderType::StopLoss => OrderType::StopLoss,
            CloseOrderType::TakeProfit => OrderType::TakeProfit,
            CloseOrderType::StopLossLimit => OrderType::StopLossLimit,
            CloseOrderType::TakeProfitLimit => OrderType::TakeProfitLimit,
            CloseOrderType::TrailingStop => OrderType::TrailingStop,
            CloseOrderType::TrailingStopLimit => OrderType::TrailingStopLimit,
        }
    }
}

/// Spot OTO/bracket close primitive; renders under the wire `close[...]` bracket.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConditionalClose {
    /// Close-leg order type; wire `close[ordertype]` (batch: `orders[N][close][ordertype]`).
    pub ordertype: CloseOrderType,
    /// Close price — the limit price for a `limit` close, the trigger price otherwise; wire `close[price]`.
    pub price: Price,
    /// Secondary price leg — the limit leg of a `*-limit` close type; omitted otherwise.
    pub price2: Option<Price>,
}

impl ConditionalClose {
    /// The flat `(ordertype, price, price2)` legs the WS composer path speaks.
    pub(crate) fn flat_legs(
        cc: &Option<ConditionalClose>,
    ) -> (Option<OrderType>, Option<Price>, Option<Price>) {
        match cc {
            Some(c) => (Some(c.ordertype.to_order_type()), Some(c.price), c.price2),
            None => (None, None, None),
        }
    }

    /// Construct a close leg from its order type and close price.
    pub fn new(ordertype: CloseOrderType, price: impl Into<Price>) -> Self {
        Self {
            ordertype,
            price: price.into(),
            price2: None,
        }
    }

    /// Secondary price leg (the limit leg of a `*-limit` close type).
    #[must_use]
    pub fn price2(mut self, price2: impl Into<Price>) -> Self {
        self.price2 = Some(price2.into());
        self
    }

    /// Emit the bracket keys under `prefix` — `"close"` for single AddOrder,
    /// `"orders[N][close]"` for a batch entry.
    pub(crate) fn push_form(&self, prefix: &str, form: &mut Vec<(String, String)>) {
        form.push((format!("{prefix}[ordertype]"), self.ordertype.to_string()));
        form.push((format!("{prefix}[price]"), self.price.to_rest_form()));
        if let Some(p2) = &self.price2 {
            form.push((format!("{prefix}[price2]"), p2.to_rest_form()));
        }
    }
}

/// One entry in an order batch. Field docs give the REST `orders[N][field]` bracket
/// names; the WS `batch_add` path uses the WS field names (see [`OrderRequest`]).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BatchOrderEntry {
    /// Wire `orders[N][ordertype]` (e.g. `limit`, `market`, `trailing-stop`).
    pub ordertype: OrderType,
    /// Wire: `orders[N][type]` = `"buy"` / `"sell"`.
    pub side: Side,
    /// Order quantity in base currency; wire `orders[N][volume]`.
    pub volume: Decimal,
    /// Primary price leg; wire `orders[N][price]`.
    pub price: Option<Price>,
    /// Secondary price leg; wire `orders[N][price2]`.
    pub price2: Option<Price>,
    /// Margin leverage ratio; wire `orders[N][leverage]`. REST-only — see `margin` for WS.
    pub leverage: Option<u8>,
    /// WS-native margin toggle — see [`OrderRequest::margin`].
    pub margin: bool,
    /// Sent as `orders[N][reduce_only]=true` only when `Some(true)`; requires a margin position.
    pub reduce_only: Option<bool>,
    /// Self-trade-prevention behaviour; wire `orders[N][stptype]`.
    pub stp_type: Option<StpType>,
    /// Price signal (`last`/`index`) that fires triggered order types; wire `orders[N][trigger]`.
    pub trigger: Option<TriggerKind>,
    /// Visible iceberg volume in base currency; wire `orders[N][displayvol]`.
    pub display_vol: Option<Decimal>,
    /// Scheduled activation; wire `orders[N][starttm]`.
    pub start_time: Option<TimeSpec>,
    /// Expiry; wire `orders[N][expiretm]`.
    pub expire_time: Option<TimeSpec>,
    /// Client order id; wire `orders[N][cl_ord_id]`.
    pub cl_ord_id: Option<crate::types::ClOrdId>,
    /// Numeric client tag; wire `orders[N][userref]`.
    pub userref: Option<i32>,
    /// Order flags CSV; wire `orders[N][oflags]`; `None` or an empty set omits the key.
    pub oflags: Option<Vec<OFlag>>,
    /// Wire `orders[N][timeinforce]`; FOK is only valid on limit order types.
    pub time_in_force: Option<TimeInForce>,
    /// Conditional-close (OTO) leg; renders under `orders[N][close][...]`.
    pub conditional_close: Option<ConditionalClose>,
}

impl BatchOrderEntry {
    /// Construct with the 3 required fields; every optional empty, `margin` off.
    pub fn new(ordertype: OrderType, side: Side, volume: Decimal) -> Self {
        Self {
            ordertype,
            side,
            volume,
            price: None,
            price2: None,
            leverage: None,
            margin: false,
            reduce_only: None,
            stp_type: None,
            trigger: None,
            display_vol: None,
            start_time: None,
            expire_time: None,
            cl_ord_id: None,
            userref: None,
            oflags: None,
            time_in_force: None,
            conditional_close: None,
        }
    }

    /// Primary price leg — absolute [`Price`] or relative offset; `Decimal` converts.
    #[must_use]
    pub fn price(mut self, price: impl Into<Price>) -> Self {
        self.price = Some(price.into());
        self
    }

    /// Secondary price leg (the limit leg of a triggered-limit order type).
    #[must_use]
    pub fn price2(mut self, price2: impl Into<Price>) -> Self {
        self.price2 = Some(price2.into());
        self
    }

    /// Margin leverage ratio (REST-only; WS uses [`Self::margin`]).
    #[must_use]
    pub fn leverage(mut self, leverage: u8) -> Self {
        self.leverage = Some(leverage);
        self
    }

    /// WS-native margin toggle — funds at the pair's max leverage.
    #[must_use]
    pub fn margin(mut self, margin: bool) -> Self {
        self.margin = margin;
        self
    }

    /// Reduce-only: close against an open margin position, never open one.
    #[must_use]
    pub fn reduce_only(mut self, reduce_only: bool) -> Self {
        self.reduce_only = Some(reduce_only);
        self
    }

    /// Self-trade-prevention behaviour.
    #[must_use]
    pub fn stp_type(mut self, stp_type: StpType) -> Self {
        self.stp_type = Some(stp_type);
        self
    }

    /// Price signal (`last`/`index`) that fires a triggered order type.
    #[must_use]
    pub fn trigger(mut self, trigger: TriggerKind) -> Self {
        self.trigger = Some(trigger);
        self
    }

    /// Visible iceberg slice in base currency.
    #[must_use]
    pub fn display_vol(mut self, display_vol: Decimal) -> Self {
        self.display_vol = Some(display_vol);
        self
    }

    /// Scheduled activation instant.
    #[must_use]
    pub fn start_time(mut self, start_time: TimeSpec) -> Self {
        self.start_time = Some(start_time);
        self
    }

    /// Expiry instant.
    #[must_use]
    pub fn expire_time(mut self, expire_time: TimeSpec) -> Self {
        self.expire_time = Some(expire_time);
        self
    }

    /// Client order id for this entry.
    #[must_use]
    pub fn cl_ord_id(mut self, cl_ord_id: crate::types::ClOrdId) -> Self {
        self.cl_ord_id = Some(cl_ord_id);
        self
    }

    /// Numeric client tag for this entry.
    #[must_use]
    pub fn userref(mut self, userref: i32) -> Self {
        self.userref = Some(userref);
        self
    }

    /// Replace the order-flag set; an empty set omits the wire key.
    #[must_use]
    pub fn oflags(mut self, oflags: Vec<OFlag>) -> Self {
        self.oflags = Some(oflags);
        self
    }

    /// Time-in-force; FOK is limit-types-only (rejected by `validate()` otherwise).
    #[must_use]
    pub fn time_in_force(mut self, time_in_force: TimeInForce) -> Self {
        self.time_in_force = Some(time_in_force);
        self
    }

    /// Conditional-close (OTO) leg for this entry.
    #[must_use]
    pub fn conditional_close(mut self, conditional_close: ConditionalClose) -> Self {
        self.conditional_close = Some(conditional_close);
        self
    }
}

/// `POST /0/private/AddOrderBatch` request.
/// Bracket-notation wire, ONE top-level `pair`, 2–15 entries; per-line
/// processing (a rejected entry doesn't fail placed siblings).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct AddOrderBatchRequest {
    /// Single spot pair shared by every entry; wire `pair` (a batch is one-pair-only).
    pub pair: crate::types::Symbol,
    /// Batch entries; 2–15 enforced by [`Self::validate`]; entry `N` renders as `orders[N][...]`.
    pub orders: Vec<BatchOrderEntry>,
    /// Held but rejected by [`Self::validate`] (server-clock render deferred); leave `None`.
    pub deadline: Option<DeadlineSpec>,
    /// When `true` the exchange only validates — nothing is placed; wire `validate`, sent only when true.
    pub validate: bool,
}

impl AddOrderBatchRequest {
    /// Construct from the one shared pair and its entries; `validate` off.
    pub fn new(pair: crate::types::Symbol, orders: Vec<BatchOrderEntry>) -> Self {
        Self {
            pair,
            orders,
            deadline: None,
            validate: false,
        }
    }

    /// Dry run — the exchange validates every entry and places nothing.
    #[must_use]
    pub fn validate_only(mut self, validate: bool) -> Self {
        self.validate = validate;
        self
    }

    /// Client-side batch validator: the 2–15 size bound plus per-entry gates.
    /// Rules: docs/guides/placing-orders.md.
    pub fn validate(&self) -> Result<(), super::super::error::TradeError> {
        use super::super::error::TradeError;
        reject_unsupported_deadline(self.deadline.as_ref())?;
        let n = self.orders.len() as u32;
        if !(2..=15).contains(&n) {
            return Err(TradeError::BatchSizeOutOfRange { min: 2, max: 15 });
        }
        // A batch places on ONE transport: if any entry needs `margin` (WS-only) AND
        // any needs `leverage`/relative-close (REST-only), no transport can send it.
        let rest_blocked = self
            .orders
            .iter()
            .find_map(|e| rest_unrepresentable_field(e.margin));
        let ws_blocked = self.orders.iter().find_map(|e| {
            let (_, cp, cp2) = ConditionalClose::flat_legs(&e.conditional_close);
            ws_unrepresentable_field(e.leverage.is_some(), cp.as_ref(), cp2.as_ref())
        });
        if let (Some(rf), Some(wf)) = (rest_blocked, ws_blocked) {
            return Err(TradeError::InvalidOrder {
                request_id: None,
                detail: format!(
                    "batch is un-placeable: `{rf}` can only be sent on WS while `{wf}` can only \
                     be sent on REST — a batch places on a single transport. Use one across the batch."
                ),
            });
        }
        for e in &self.orders {
            validate_trailing_price(e.ordertype, e.price.as_ref(), e.price2.as_ref())?;
            validate_order_gates(
                e.ordertype,
                e.leverage,
                e.margin,
                e.reduce_only,
                e.time_in_force,
            )?;
            if let Some(c) = &e.conditional_close {
                validate_trailing_price(
                    c.ordertype.to_order_type(),
                    Some(&c.price),
                    c.price2.as_ref(),
                )?;
            }
        }
        Ok(())
    }

    /// Bracket-notation form-encoding: ONE top-level `pair`; entry `N` encodes as `orders[N][field]`.
    pub(crate) fn to_form(&self) -> Vec<(String, String)> {
        let mut form: Vec<(String, String)> = Vec::with_capacity(1 + self.orders.len() * 16);
        form.push(("pair".into(), self.pair.as_str().to_string()));
        for (n, e) in self.orders.iter().enumerate() {
            let p = |k: &str| format!("orders[{n}][{k}]");
            form.push((p("type"), e.side.to_string()));
            form.push((p("ordertype"), e.ordertype.to_string()));
            form.push((p("volume"), e.volume.to_string()));
            if let Some(price) = &e.price {
                form.push((p("price"), price.to_rest_form()));
            }
            if let Some(p2) = &e.price2 {
                form.push((p("price2"), p2.to_rest_form()));
            }
            if let Some(l) = &e.leverage {
                form.push((p("leverage"), l.to_string()));
            }
            if e.reduce_only == Some(true) {
                form.push((p("reduce_only"), "true".into()));
            }
            if let Some(s) = &e.stp_type {
                form.push((p("stptype"), s.to_string()));
            }
            if let Some(t) = &e.trigger {
                form.push((p("trigger"), t.to_string()));
            }
            if let Some(d) = &e.display_vol {
                form.push((p("displayvol"), d.to_string()));
            }
            if let Some(s) = &e.start_time {
                form.push((p("starttm"), s.to_rest_form()));
            }
            if let Some(ex) = &e.expire_time {
                form.push((p("expiretm"), ex.to_rest_form()));
            }
            if let Some(c) = &e.cl_ord_id {
                form.push((p("cl_ord_id"), c.as_str().to_string()));
            }
            if let Some(u) = &e.userref {
                form.push((p("userref"), u.to_string()));
            }
            if let Some(of) = &e.oflags {
                if let Some(csv) = oflags_to_wire(of) {
                    form.push((p("oflags"), csv));
                }
            }
            if let Some(tif) = &e.time_in_force {
                form.push((p("timeinforce"), tif.to_string()));
            }
            if let Some(cc) = &e.conditional_close {
                cc.push_form(&format!("orders[{n}][close]"), &mut form);
            }
        }
        if self.validate {
            form.push(("validate".into(), "true".into()));
        }
        // deadline → SKIP: server-clock render deferred.
        form
    }
}
