use serde::{Deserialize, Serialize};

/// Order type. Wire form is kebab-case per Kraken's `/AddOrder.ordertype` tokens.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    strum::Display,
    strum::AsRefStr,
    strum::IntoStaticStr,
)]
#[non_exhaustive]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum OrderType {
    /// Wire `market`: execute immediately at the best available price.
    Market,
    /// Wire `limit`: execute at the given limit price or better.
    Limit,
    /// Wire `iceberg`: limit order showing only part of its volume in the book.
    Iceberg,
    /// Wire `stop-loss`: fires a market order once the trigger price is reached.
    StopLoss,
    /// Wire `take-profit`: fires a market order once the profit trigger price is reached.
    TakeProfit,
    /// Wire `stop-loss-limit`: fires a limit order once the trigger price is reached.
    StopLossLimit,
    /// Wire `take-profit-limit`: fires a limit order once the profit trigger price is reached.
    TakeProfitLimit,
    /// Wire `trailing-stop`: stop whose trigger trails the market by a relative offset.
    TrailingStop,
    /// Wire `trailing-stop-limit`: trailing trigger that fires a limit order.
    TrailingStopLimit,
    /// Wire `settle-position`: settles an open margin position instead of adding book liquidity.
    SettlePosition,
    /// Unrecognized order-type value from the wire — forward-compatible catch-all.
    #[serde(other)]
    Unknown,
}

/// A single Kraken `oflags` token. Treated as a SET on the wire — Kraken may
/// append an implicit `fciq` on echo, so decoders must compare as a set, not
/// string-equal the CSV. `reduce_only` is NOT an oflag (top-level request bool).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum OFlag {
    /// Wire `post`: post-only — reject the order rather than take liquidity.
    Post,
    /// Wire `fcib`: prefer fees charged in the base currency.
    Fcib,
    /// Wire `fciq`: prefer fees charged in the quote currency (Kraken's implicit echo default).
    Fciq,
    /// Wire `nompp`: disable market-price protection on market orders.
    Nompp,
}

/// Comma-join a set of [`OFlag`]s into the wire `oflags` value.
/// Empty slice → `None` (omit the param entirely); else e.g. `"post,fcib"`.
pub(crate) fn oflags_to_wire(flags: &[OFlag]) -> Option<String> {
    if flags.is_empty() {
        return None;
    }
    Some(
        flags
            .iter()
            .map(|f| f.as_ref())
            .collect::<Vec<&str>>()
            .join(","),
    )
}

/// Self-trade-prevention behaviour. The string form is the REST kebab-case
/// spelling; the WS composer renders its underscore spelling at the call site.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[strum(serialize_all = "kebab-case")]
#[non_exhaustive]
pub enum StpType {
    /// Wire `cancel-newest` (REST): on self-match, cancel the incoming (newer) order.
    CancelNewest,
    /// Wire `cancel-oldest` (REST): on self-match, cancel the resting (older) order.
    CancelOldest,
    /// Wire `cancel-both` (REST): on self-match, cancel both orders.
    CancelBoth,
}

/// WS v2 `stp_type` spelling (UNDERSCORE) — do NOT use the REST `Display` form
/// on the WS path.
pub(crate) fn stp_type_ws(s: StpType) -> &'static str {
    match s {
        StpType::CancelNewest => "cancel_newest",
        StpType::CancelOldest => "cancel_oldest",
        StpType::CancelBoth => "cancel_both",
    }
}

/// Reference price for stop/take-profit triggers.
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
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum TriggerKind {
    /// Wire `last`: trigger off the last traded price.
    Last,
    /// Wire `index`: trigger off the index price.
    Index,
}

impl TriggerKind {
    pub(crate) fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "last" => Some(TriggerKind::Last),
            "index" => Some(TriggerKind::Index),
            _ => None,
        }
    }
}

/// Time-in-force policy. Wire form is lowercase (`gtc`/`ioc`/`gtd`/`fok`).
/// `Fok` (fill-or-kill) is limit-orders-only — setting it on a non-limit type
/// is rejected by `OrderRequest` validation.
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
pub enum TimeInForce {
    /// Wire `gtc`: good-till-cancelled — rests until filled or cancelled.
    Gtc,
    /// Wire `ioc`: immediate-or-cancel — fill what is possible now, cancel the remainder.
    Ioc,
    /// Wire `gtd`: good-till-date — expires at the caller-supplied expiry time.
    Gtd,
    /// Wire `fok`: fill-or-kill — fill entirely or cancel; limit orders only.
    Fok,
}

/// Order side — strict, used both directions (request encode + account decode).
/// Wire form is lowercase `buy` / `sell`. Public-tape fills use tolerant
/// [`crate::api::market::TradeSide`] instead.
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
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum Side {
    /// Wire `buy`: bid side.
    Buy,
    /// Wire `sell`: ask side.
    Sell,
}

/// Opaque amend correlation id from Kraken's `AmendOrder`. Callers MUST NOT
/// parse or interpret the inner string — pass it back verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct AmendId(String);

impl AmendId {
    /// Borrow the raw amend id string exactly as Kraken returned it; treat it
    /// as opaque and pass it back verbatim.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for AmendId {
    fn from(s: String) -> Self {
        AmendId(s)
    }
}
