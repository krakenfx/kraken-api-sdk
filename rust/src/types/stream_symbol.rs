//! Internal WebSocket subscription identity.

use super::{FuturesOhlcTickType, OhlcInterval, Symbol};

/// Wire-form symbol scoped to a streaming product; Futures OHLC adds interval and tick type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum StreamSymbol {
    Spot(Symbol),
    Futures(String),
    FuturesOhlc {
        symbol: String,
        interval: OhlcInterval,
        tick_type: FuturesOhlcTickType,
    },
}

impl StreamSymbol {
    pub(crate) fn futures_ohlc(
        symbol: String,
        interval: OhlcInterval,
        tick_type: Option<FuturesOhlcTickType>,
    ) -> Self {
        Self::FuturesOhlc {
            symbol,
            interval,
            tick_type: tick_type.unwrap_or_default(),
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Spot(symbol) => symbol.as_str(),
            Self::Futures(symbol) | Self::FuturesOhlc { symbol, .. } => symbol,
        }
    }

    pub(crate) fn spot(&self) -> Option<&Symbol> {
        match self {
            Self::Spot(symbol) => Some(symbol),
            Self::Futures(_) | Self::FuturesOhlc { .. } => None,
        }
    }

    pub(crate) fn ohlc_interval(&self) -> Option<OhlcInterval> {
        match self {
            Self::FuturesOhlc { interval, .. } => Some(*interval),
            Self::Spot(_) | Self::Futures(_) => None,
        }
    }

    pub(crate) fn ohlc_tick_type(&self) -> Option<FuturesOhlcTickType> {
        match self {
            Self::FuturesOhlc { tick_type, .. } => Some(*tick_type),
            Self::Spot(_) | Self::Futures(_) => None,
        }
    }

    pub(crate) fn futures_identity(
        pair: Option<&Self>,
    ) -> (
        Option<String>,
        Option<OhlcInterval>,
        Option<FuturesOhlcTickType>,
    ) {
        (
            pair.map(|symbol| symbol.as_str().to_string()),
            pair.and_then(Self::ohlc_interval),
            pair.and_then(Self::ohlc_tick_type),
        )
    }
}

impl From<Symbol> for StreamSymbol {
    fn from(symbol: Symbol) -> Self {
        Self::Spot(symbol)
    }
}

/// The `PI` / `PF` / `FI` / `FF` product prefix of a Futures wire symbol, if it has one.
pub(crate) fn futures_product(symbol: &str) -> Option<&str> {
    symbol
        .split_once('_')
        .map(|(product, _)| product)
        .filter(|product| matches!(*product, "PI" | "PF" | "FI" | "FF"))
}

pub(crate) fn is_futures_wire_symbol(symbol: &str) -> bool {
    let Some(product) = futures_product(symbol) else {
        return false;
    };
    let rest = &symbol[product.len() + 1..];
    if rest.is_empty() {
        return false;
    }
    let mut components = rest.split('_');
    let Some(instrument) = components.next() else {
        return false;
    };
    if instrument.is_empty()
        || !instrument
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    {
        return false;
    }
    match product {
        "PI" | "PF" => components.next().is_none(),
        "FI" | "FF" => {
            let Some(expiry) = components.next() else {
                return false;
            };
            components.next().is_none()
                && expiry.len() == 6
                && expiry.bytes().all(|b| b.is_ascii_digit())
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::is_futures_wire_symbol;

    #[test]
    fn accepts_only_native_futures_wire_shapes() {
        for symbol in [
            "PI_XBTUSD",
            "PF_ETHUSD",
            "FI_XBTUSD_251226",
            "FF_SOLUSD_251226",
        ] {
            assert!(is_futures_wire_symbol(symbol), "{symbol}");
        }
        for symbol in [
            "",
            "BTC/USD",
            "OF_XBTUSD",
            "pi_xbtusd",
            "PI_XBT/USD",
            "FI_XBTUSD_2512",
        ] {
            assert!(!is_futures_wire_symbol(symbol), "{symbol}");
        }
    }
}
