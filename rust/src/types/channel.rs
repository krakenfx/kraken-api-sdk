//! WS channel / URL / depth / asset-class closed enums.

/// Closed-enum tag for one of the two Spot WS v2 connections. SDK methods take
/// this tag rather than a URL string; [`WsUrl::as_wire_url`] resolves it to the
/// canonical production URL.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WsUrl {
    /// `wss://ws.kraken.com/v2` — public, no auth.
    Public,
    /// `wss://ws-auth.kraken.com/v2` — token-authenticated.
    Auth,
}

impl WsUrl {
    /// Resolve to the canonical production wire URL.
    pub const fn as_wire_url(self) -> &'static str {
        match self {
            WsUrl::Public => "wss://ws.kraken.com/v2",
            WsUrl::Auth => "wss://ws-auth.kraken.com/v2",
        }
    }
}

/// Kraken Spot WS v2 channel name. Closed enum; wire-form names are the
/// lowercase `Display` strings. `Book`/`BookRaw` both map to wire `"book"`.
/// Resubscribe replay ordering: docs/guides/streaming.md.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    strum::Display,
    strum::AsRefStr,
    strum::IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum ChannelName {
    /// Wire `"ticker"` — per-pair ticker updates on the public connection.
    Ticker,
    /// Wire `"book"` — SDK-maintained order book (snapshot + checksummed deltas).
    Book,
    /// Wire `"book"` (shared with `Book`) — opt-in raw delta stream; never parsed from the wire.
    // Collides with `Book` by design: rendered channel strings are always the wire name.
    #[strum(serialize = "book")]
    BookRaw,
    /// Wire `"trade"` — public per-pair trade stream.
    Trade,
    /// Wire `"ohlc"` — candle stream; interval chosen via [`OhlcInterval`].
    Ohlc,
    /// Wire `"status"` — exchange system/connection status frames.
    Status,
    /// Wire `"executions"` — own orders and fills; requires the authenticated connection.
    Executions,
    /// Wire `"balances"` — account balance updates; requires the authenticated connection.
    Balances,
    /// Wire `"heartbeat"` — server liveness frames; carries no market data.
    Heartbeat,
}

impl ChannelName {
    /// `BookRaw` → `Book` (shared wire channel); every other channel maps to itself.
    pub(crate) const fn wire_channel(self) -> ChannelName {
        match self {
            ChannelName::BookRaw => ChannelName::Book,
            other => other,
        }
    }

    /// Parse a wire-form channel-name string. Returns `None` on unknown names;
    /// the caller decides how to handle unrecognised frames.
    pub fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "ticker" => Some(Self::Ticker),
            "book" => Some(Self::Book),
            "trade" => Some(Self::Trade),
            "ohlc" => Some(Self::Ohlc),
            "status" => Some(Self::Status),
            "executions" => Some(Self::Executions),
            "balances" => Some(Self::Balances),
            "heartbeat" => Some(Self::Heartbeat),
            _ => None,
        }
    }
}

/// Order-book subscription depth for the WS v2 `book` channel. Closed enum —
/// Kraken rejects any depth not in this set at subscribe-time, so the SDK
/// surfaces it as a closed enum to push caller errors to compile time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BookDepth {
    /// Wire `depth=10` — 10 price levels per side; the maintained book bumps to 25 for CRC headroom.
    D10,
    /// Wire `depth=25` — 25 price levels per side.
    D25,
    /// Wire `depth=100` — 100 price levels per side.
    D100,
    /// Wire `depth=500` — 500 price levels per side.
    D500,
    /// Wire `depth=1000` — 1000 price levels per side; Kraken's maximum.
    D1000,
}

impl BookDepth {
    /// Wire-form integer used in Kraken's `book` subscribe-frame `depth` field.
    pub const fn as_wire_u32(self) -> u32 {
        match self {
            BookDepth::D10 => 10,
            BookDepth::D25 => 25,
            BookDepth::D100 => 100,
            BookDepth::D500 => 500,
            BookDepth::D1000 => 1000,
        }
    }

    /// Depth subscribed + maintained, bumped for CRC headroom (`D10`→`D25`).
    /// Caller-facing depth is unaffected. See docs/guides/order-book.md.
    pub const fn wire_depth(self) -> BookDepth {
        // Exhaustive: a future BookDepth at/below CHECKSUM_DEPTH must force a headroom decision.
        match self {
            BookDepth::D10 => BookDepth::D25,
            BookDepth::D25 => BookDepth::D25,
            BookDepth::D100 => BookDepth::D100,
            BookDepth::D500 => BookDepth::D500,
            BookDepth::D1000 => BookDepth::D1000,
        }
    }
}

/// OHLC candle interval. Closed enum — the only values Kraken accepts on the
/// `/0/public/OHLC` endpoint and the `ohlc` WS channel. The discriminant is
/// the wire `interval` value in minutes (`u64::from`); the strum string form
/// renders the same minutes (`"60"` for [`OhlcInterval::H1`]).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
pub enum OhlcInterval {
    /// 1-minute candles.
    #[strum(serialize = "1")]
    M1 = 1,
    /// 5-minute candles.
    #[strum(serialize = "5")]
    M5 = 5,
    /// 15-minute candles.
    #[strum(serialize = "15")]
    M15 = 15,
    /// 30-minute candles.
    #[strum(serialize = "30")]
    M30 = 30,
    /// 1-hour candles.
    #[strum(serialize = "60")]
    H1 = 60,
    /// 4-hour candles.
    #[strum(serialize = "240")]
    H4 = 240,
    /// 1-day candles.
    #[strum(serialize = "1440")]
    D1 = 1440,
    /// 7-day candles.
    #[strum(serialize = "10080")]
    D7 = 10080,
    /// 15-day candles — not two weeks.
    #[strum(serialize = "21600")]
    D15 = 21600,
}

impl From<OhlcInterval> for u64 {
    /// The wire `interval` minutes — the enum discriminant.
    fn from(interval: OhlcInterval) -> u64 {
        interval as u64
    }
}

/// Ticker-channel `event_trigger` subscribe option. Closed enum — Kraken WS v2
/// rejects any value outside this set at subscribe-time; default when omitted is
/// `Trades`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum TickerTrigger {
    /// Wire `"trades"` — a ticker update on each trade (Kraken default).
    Trades,
    /// Wire `"bbo"` — a ticker update on each best-bid/offer (top-of-book) change.
    Bbo,
}

/// Kraken asset class. Closed enum; identical variant names and wire
/// serialization across bindings. The SDK never sends `asset_class` on v1
/// endpoints and does not filter by `descr.aclass`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::AsRefStr, strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum AssetClass {
    /// Wire `"forex"` — crypto and fiat pairs. The v1 SDK scope.
    Forex,
    /// Wire `"tokenized_asset"` — xStocks. Reserved for a later release.
    TokenizedAsset,
    /// Wire `"synthetic_pair"`. Reserved for a later release.
    SyntheticPair,
    /// Wire `"futures_contract"`. Reserved for the Futures release.
    FuturesContract,
}

impl AssetClass {
    /// Parse a wire-form `aclass` string. Returns `None` on unknown values.
    pub fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "forex" => Some(Self::Forex),
            "tokenized_asset" => Some(Self::TokenizedAsset),
            "synthetic_pair" => Some(Self::SyntheticPair),
            "futures_contract" => Some(Self::FuturesContract),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ws_url_resolves_to_canonical_production_urls() {
        assert_eq!(WsUrl::Public.as_wire_url(), "wss://ws.kraken.com/v2");
        assert_eq!(WsUrl::Auth.as_wire_url(), "wss://ws-auth.kraken.com/v2");
    }

    #[test]
    fn asset_class_wire_round_trips() {
        for ac in [
            AssetClass::Forex,
            AssetClass::TokenizedAsset,
            AssetClass::SyntheticPair,
            AssetClass::FuturesContract,
        ] {
            assert_eq!(AssetClass::from_wire_str(ac.as_ref()), Some(ac));
        }
        assert_eq!(AssetClass::Forex.to_string(), "forex");
        assert_eq!(AssetClass::from_wire_str("not_a_class"), None);
    }

    /// Discriminant is the wire `interval` minutes; pin each variant and its `Display`.
    #[test]
    fn ohlc_interval_discriminants_are_wire_minutes() {
        for (interval, minutes) in [
            (OhlcInterval::M1, 1u64),
            (OhlcInterval::M5, 5),
            (OhlcInterval::M15, 15),
            (OhlcInterval::M30, 30),
            (OhlcInterval::H1, 60),
            (OhlcInterval::H4, 240),
            (OhlcInterval::D1, 1440),
            (OhlcInterval::D7, 10080),
            (OhlcInterval::D15, 21600),
        ] {
            assert_eq!(u64::from(interval), minutes);
            assert_eq!(interval.to_string(), minutes.to_string());
        }
    }

    /// Every variant renders its wire token (`BookRaw` collides with `Book` on `"book"`).
    #[test]
    fn strum_display_matches_wire_strings() {
        for (ch, wire) in [
            (ChannelName::Ticker, "ticker"),
            (ChannelName::Book, "book"),
            (ChannelName::BookRaw, "book"),
            (ChannelName::Trade, "trade"),
            (ChannelName::Ohlc, "ohlc"),
            (ChannelName::Status, "status"),
            (ChannelName::Executions, "executions"),
            (ChannelName::Balances, "balances"),
            (ChannelName::Heartbeat, "heartbeat"),
        ] {
            assert_eq!(ch.to_string(), wire);
        }
        for (t, wire) in [
            (TickerTrigger::Trades, "trades"),
            (TickerTrigger::Bbo, "bbo"),
        ] {
            assert_eq!(t.to_string(), wire);
        }
        for (ac, wire) in [
            (AssetClass::Forex, "forex"),
            (AssetClass::TokenizedAsset, "tokenized_asset"),
            (AssetClass::SyntheticPair, "synthetic_pair"),
            (AssetClass::FuturesContract, "futures_contract"),
        ] {
            assert_eq!(ac.to_string(), wire);
        }
    }
}
