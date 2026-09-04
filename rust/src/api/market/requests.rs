//! Inert request structs for multi-argument public market reads.

use crate::types::Symbol;

use super::types::OhlcInterval;

/// Query for [`MarketNamespace::trades`](super::MarketNamespace::trades).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TradesRequest {
    pair: Symbol,
    since: Option<String>,
    count: Option<u32>,
}

impl TradesRequest {
    /// Query recent trades for `pair`.
    #[must_use]
    pub fn new(pair: Symbol) -> Self {
        Self {
            pair,
            since: None,
            count: None,
        }
    }

    /// Nanosecond pagination cursor — a prior response's `last`.
    #[must_use]
    pub fn since(mut self, since: impl Into<String>) -> Self {
        self.since = Some(since.into());
        self
    }

    /// Cap the batch size (max 1000).
    #[must_use]
    pub fn count(mut self, count: u32) -> Self {
        self.count = Some(count);
        self
    }

    /// The queried pair — echoed back on the result.
    pub(crate) fn pair(&self) -> &Symbol {
        &self.pair
    }

    /// Query pairs for `GET /0/public/Trades`.
    pub(crate) fn to_params(&self) -> Vec<(String, String)> {
        let mut query: Vec<(String, String)> =
            vec![("pair".to_string(), self.pair.as_str().to_string())];
        if let Some(s) = &self.since {
            query.push(("since".to_string(), s.clone()));
        }
        if let Some(c) = self.count {
            query.push(("count".to_string(), c.to_string()));
        }
        query
    }
}

/// Query for [`MarketNamespace::ohlc`](super::MarketNamespace::ohlc).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct OhlcRequest {
    pair: Symbol,
    interval: OhlcInterval,
    since: Option<u64>,
}

impl OhlcRequest {
    /// Query OHLCV candles for `pair` at `interval`.
    #[must_use]
    pub fn new(pair: Symbol, interval: OhlcInterval) -> Self {
        Self {
            pair,
            interval,
            since: None,
        }
    }

    /// Unix-seconds pagination cursor — a prior response's `last`.
    #[must_use]
    pub fn since(mut self, since: u64) -> Self {
        self.since = Some(since);
        self
    }

    pub(crate) fn pair(&self) -> &Symbol {
        &self.pair
    }

    pub(crate) fn interval(&self) -> OhlcInterval {
        self.interval
    }

    pub(crate) fn since_opt(&self) -> Option<u64> {
        self.since
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trades_full_emit_and_bare_pair_omit() {
        let pair = crate::types::Symbol::new("BTC/USD").unwrap();
        let bare = TradesRequest::new(pair.clone()).to_params();
        assert_eq!(bare, [("pair".to_string(), "BTC/USD".to_string())]);
        let full = TradesRequest::new(pair)
            .since("17296160477559340")
            .count(25);
        assert_eq!(
            full.to_params(),
            [
                ("pair".to_string(), "BTC/USD".to_string()),
                ("since".to_string(), "17296160477559340".to_string()),
                ("count".to_string(), "25".to_string()),
            ]
        );
    }

    #[test]
    fn ohlc_request_defaults_omit_since() {
        let pair = crate::types::Symbol::new("BTC/USD").unwrap();
        let bare = OhlcRequest::new(pair.clone(), OhlcInterval::M1);
        assert_eq!(bare.pair(), &pair);
        assert_eq!(bare.interval(), OhlcInterval::M1);
        assert_eq!(bare.since_opt(), None);
        let with_since = bare.since(1_700_000_000);
        assert_eq!(with_since.since_opt(), Some(1_700_000_000));
    }
}
