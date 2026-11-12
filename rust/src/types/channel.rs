//! WS channel / URL / depth / asset-class closed enums.

/// Closed-enum tag for an SDK-managed WebSocket connection. SDK methods take
/// this tag rather than a URL string; [`WsUrl::as_wire_url`] resolves it to the
/// canonical production URL.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WsUrl {
    /// `wss://ws.kraken.com/v2` — public, no auth.
    Public,
    /// `wss://ws-auth.kraken.com/v2` — token-authenticated.
    Auth,
    /// `wss://uws.kraken.com/v2/public` — Unified derivatives public data.
    UnifiedPublic,
    /// `wss://uws.kraken.com/v2/private` — Unified derivatives private data.
    UnifiedPrivate,
}

impl WsUrl {
    /// Every socket identity managed by the client reactor.
    pub(crate) const ALL: [Self; 4] = [
        Self::Public,
        Self::Auth,
        Self::UnifiedPublic,
        Self::UnifiedPrivate,
    ];

    /// Resolve to the canonical production wire URL.
    pub const fn as_wire_url(self) -> &'static str {
        match self {
            WsUrl::Public => "wss://ws.kraken.com/v2",
            WsUrl::Auth => "wss://ws-auth.kraken.com/v2",
            WsUrl::UnifiedPublic => "wss://uws.kraken.com/v2/public",
            WsUrl::UnifiedPrivate => "wss://uws.kraken.com/v2/private",
        }
    }

    /// Whether this is one of the two Unified Futures connections.
    pub(crate) const fn is_unified(self) -> bool {
        match self {
            Self::UnifiedPublic | Self::UnifiedPrivate => true,
            Self::Public | Self::Auth => false,
        }
    }

    /// Whether this connection starts with the shared WebSocket token flow.
    pub(crate) const fn requires_ws_token(self) -> bool {
        match self {
            Self::Auth | Self::UnifiedPrivate => true,
            Self::Public | Self::UnifiedPublic => false,
        }
    }

    pub(crate) const fn sends_korigin(self) -> bool {
        match self {
            Self::Public | Self::Auth | Self::UnifiedPublic => true,
            Self::UnifiedPrivate => false,
        }
    }

    pub(crate) const fn tracks_sequence(self, channel: ChannelName) -> bool {
        match self {
            Self::Auth => matches!(channel, ChannelName::Executions | ChannelName::Balances),
            Self::UnifiedPrivate => matches!(channel, ChannelName::Balances),
            Self::Public | Self::UnifiedPublic => false,
        }
    }

    pub(crate) const fn shares_unified_book_stream(self, channel: ChannelName) -> bool {
        match self {
            Self::UnifiedPublic => channel.book_peer().is_some(),
            Self::Public | Self::Auth | Self::UnifiedPrivate => false,
        }
    }

    /// Asset class stamped on every request frame sent on this connection.
    pub(crate) const fn request_asset_class(self) -> Option<&'static str> {
        match self {
            Self::Public | Self::Auth => None,
            Self::UnifiedPublic | Self::UnifiedPrivate => Some("derivatives"),
        }
    }

    /// Asset class required by this connection's subscribe protocol.
    pub(crate) const fn subscription_asset_class(
        self,
        channel: ChannelName,
    ) -> Option<&'static str> {
        match (self, channel) {
            (Self::UnifiedPrivate, ChannelName::Balances | ChannelName::Preferences) => None,
            (Self::UnifiedPublic | Self::UnifiedPrivate, _) => Some("derivatives"),
            (Self::Public | Self::Auth, _) => None,
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
    /// Wire `"instrument"` — channel-wide market metadata.
    Instrument,
    /// Wire `"executions"` — own orders and fills; requires an authenticated connection (Spot auth, or Unified private for Futures).
    Executions,
    /// Wire `"balances"` — account balance updates; requires an authenticated connection (shared by Spot auth and Unified private).
    Balances,
    /// Wire `"positions"` — Unified Futures position snapshots and updates.
    Positions,
    /// Wire `"margin"` — Unified Futures multi- and single-collateral margin data.
    Margin,
    /// Wire `"preferences"` — Unified Futures account preferences, including the futures trading permission.
    Preferences,
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

    /// The other logical row sharing the wire `book` channel; `None` for any other channel.
    pub(crate) const fn book_peer(self) -> Option<ChannelName> {
        match self {
            ChannelName::Book => Some(ChannelName::BookRaw),
            ChannelName::BookRaw => Some(ChannelName::Book),
            ChannelName::Ticker
            | ChannelName::Trade
            | ChannelName::Ohlc
            | ChannelName::Status
            | ChannelName::Instrument
            | ChannelName::Executions
            | ChannelName::Balances
            | ChannelName::Positions
            | ChannelName::Margin
            | ChannelName::Preferences
            | ChannelName::Heartbeat => None,
        }
    }

    /// Spot socket carrying `self`: `Auth` for the private channels, else `Public`.
    pub(crate) const fn spot_url(self) -> WsUrl {
        match self {
            ChannelName::Executions | ChannelName::Balances => WsUrl::Auth,
            ChannelName::Ticker
            | ChannelName::Book
            | ChannelName::BookRaw
            | ChannelName::Trade
            | ChannelName::Ohlc
            | ChannelName::Status
            | ChannelName::Instrument
            | ChannelName::Positions
            | ChannelName::Margin
            | ChannelName::Preferences
            | ChannelName::Heartbeat => WsUrl::Public,
        }
    }

    /// Registry key for `channel` on `url`. Unified Public keeps `Book` and
    /// `BookRaw` distinct; every other socket folds `BookRaw` into `Book`.
    pub(crate) const fn registry_channel(self, url: WsUrl) -> ChannelName {
        if matches!(url, WsUrl::UnifiedPublic) {
            self
        } else {
            self.wire_channel()
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
            "instrument" => Some(Self::Instrument),
            "executions" => Some(Self::Executions),
            "balances" => Some(Self::Balances),
            "positions" => Some(Self::Positions),
            "margin" => Some(Self::Margin),
            "preferences" => Some(Self::Preferences),
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

impl OhlcInterval {
    pub(crate) const fn from_minutes(minutes: u64) -> Option<Self> {
        match minutes {
            1 => Some(Self::M1),
            5 => Some(Self::M5),
            15 => Some(Self::M15),
            30 => Some(Self::M30),
            60 => Some(Self::H1),
            240 => Some(Self::H4),
            1440 => Some(Self::D1),
            10080 => Some(Self::D7),
            21600 => Some(Self::D15),
            _ => None,
        }
    }
}

/// Price source of a Futures OHLC candle.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    strum::Display,
    strum::AsRefStr,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
#[non_exhaustive]
pub enum FuturesOhlcTickType {
    /// Wire `"trade"`: candles built from trades, the venue default.
    #[default]
    Trade,
    /// Wire `"mark"`: candles built from the mark price.
    Mark,
}

impl FuturesOhlcTickType {
    pub(crate) fn from_wire_str(s: &str) -> Option<Self> {
        match s {
            "trade" => Some(Self::Trade),
            "mark" => Some(Self::Mark),
            _ => None,
        }
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
        assert_eq!(
            WsUrl::UnifiedPublic.as_wire_url(),
            "wss://uws.kraken.com/v2/public"
        );
        assert_eq!(
            WsUrl::UnifiedPrivate.as_wire_url(),
            "wss://uws.kraken.com/v2/private"
        );
    }

    #[test]
    fn unified_private_and_spot_auth_require_a_ws_token() {
        assert!(WsUrl::Auth.requires_ws_token());
        assert!(WsUrl::UnifiedPrivate.requires_ws_token());
        assert!(!WsUrl::Public.requires_ws_token());
        assert!(!WsUrl::UnifiedPublic.requires_ws_token());
    }

    #[test]
    fn only_account_channels_that_carry_a_sequence_are_tracked() {
        for (url, channel, expected) in [
            (WsUrl::Auth, ChannelName::Executions, true),
            (WsUrl::Auth, ChannelName::Balances, true),
            (WsUrl::UnifiedPrivate, ChannelName::Balances, true),
            (WsUrl::UnifiedPrivate, ChannelName::Executions, false),
            (WsUrl::UnifiedPrivate, ChannelName::Positions, false),
            (WsUrl::UnifiedPrivate, ChannelName::Margin, false),
            (WsUrl::UnifiedPrivate, ChannelName::Preferences, false),
            (WsUrl::UnifiedPublic, ChannelName::Ticker, false),
            (WsUrl::Public, ChannelName::Ticker, false),
        ] {
            assert_eq!(
                url.tracks_sequence(channel),
                expected,
                "{url:?}/{channel:?}"
            );
        }
    }

    #[test]
    fn only_unified_public_book_rows_share_one_book_stream() {
        for (url, channel, expected) in [
            (WsUrl::UnifiedPublic, ChannelName::Book, true),
            (WsUrl::UnifiedPublic, ChannelName::BookRaw, true),
            (WsUrl::UnifiedPublic, ChannelName::Ticker, false),
            (WsUrl::Public, ChannelName::Book, false),
            (WsUrl::Public, ChannelName::BookRaw, false),
            (WsUrl::UnifiedPrivate, ChannelName::Book, false),
        ] {
            assert_eq!(
                url.shares_unified_book_stream(channel),
                expected,
                "{url:?}/{channel:?}"
            );
        }
    }

    #[test]
    fn request_asset_class_is_derivatives_only_on_the_unified_urls() {
        for (url, expected) in [
            (WsUrl::Public, None),
            (WsUrl::Auth, None),
            (WsUrl::UnifiedPublic, Some("derivatives")),
            (WsUrl::UnifiedPrivate, Some("derivatives")),
        ] {
            assert_eq!(url.request_asset_class(), expected, "{url:?}");
        }
    }

    #[test]
    fn unified_urls_require_the_derivatives_asset_class() {
        for (url, channel, expected) in [
            (
                WsUrl::UnifiedPublic,
                ChannelName::Ticker,
                Some("derivatives"),
            ),
            (
                WsUrl::UnifiedPrivate,
                ChannelName::Executions,
                Some("derivatives"),
            ),
            (WsUrl::UnifiedPrivate, ChannelName::Balances, None),
            (WsUrl::UnifiedPrivate, ChannelName::Preferences, None),
            (
                WsUrl::UnifiedPrivate,
                ChannelName::Positions,
                Some("derivatives"),
            ),
            (WsUrl::Public, ChannelName::Ticker, None),
            (WsUrl::Auth, ChannelName::Executions, None),
        ] {
            assert_eq!(
                url.subscription_asset_class(channel),
                expected,
                "{url:?}/{channel:?}"
            );
        }
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
            assert_eq!(OhlcInterval::from_minutes(minutes), Some(interval));
        }
        assert_eq!(OhlcInterval::from_minutes(2), None);
    }

    #[test]
    fn futures_ohlc_tick_type_wire_round_trips_and_defaults_to_trade() {
        for (tick_type, wire) in [
            (FuturesOhlcTickType::Trade, "trade"),
            (FuturesOhlcTickType::Mark, "mark"),
        ] {
            assert_eq!(tick_type.to_string(), wire);
            assert_eq!(serde_json::to_value(tick_type).unwrap(), wire);
            assert_eq!(FuturesOhlcTickType::from_wire_str(wire), Some(tick_type));
        }
        assert_eq!(FuturesOhlcTickType::from_wire_str("index"), None);
        assert_eq!(FuturesOhlcTickType::default(), FuturesOhlcTickType::Trade);
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
            (ChannelName::Instrument, "instrument"),
            (ChannelName::Executions, "executions"),
            (ChannelName::Balances, "balances"),
            (ChannelName::Positions, "positions"),
            (ChannelName::Margin, "margin"),
            (ChannelName::Preferences, "preferences"),
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
