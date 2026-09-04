//! Book-frame wire decode. Wire strings kept verbatim for CRC32.

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use super::PriceLevel;

/// A decoded `book` data entry. `bids`/`asks` are in wire order; the builder sorts on apply.
pub(crate) struct ParsedBook {
    pub(crate) symbol: String,
    pub(crate) bids: Vec<PriceLevel>,
    pub(crate) asks: Vec<PriceLevel>,
    pub(crate) checksum: u32,
    /// Exchange wall-clock (RFC 3339); `""` when absent.
    pub(crate) timestamp: String,
}

/// Raw WS v2 `book` data-entry. `price`/`qty` may be JSON string or number; both accepted.
#[derive(Debug, Deserialize)]
struct RawBookData {
    symbol: String,
    #[serde(default)]
    bids: Vec<RawBookLevel>,
    #[serde(default)]
    asks: Vec<RawBookLevel>,
    checksum: u32,
    #[serde(default)]
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct RawBookLevel {
    price: Value,
    qty: Value,
}

impl RawBookLevel {
    fn into_level(self) -> Option<PriceLevel> {
        let (price, price_wire) = parse_decimal(self.price)?;
        let (qty, qty_wire) = parse_decimal(self.qty)?;
        Some(PriceLevel {
            price,
            qty,
            price_wire,
            qty_wire,
        })
    }
}

/// Max price levels per side before a frame is rejected as malformed.
const MAX_BOOK_LEVELS_PER_SIDE: usize = 10_000;

/// Decode a `book` `data` entry. `None` if any level fails or a side exceeds the cap.
pub(crate) fn parse_book_frame(data: &Value) -> Option<ParsedBook> {
    parse_raw(RawBookData::deserialize(data).ok()?)
}

/// Owned-envelope variant: moves strings instead of cloning them.
pub(crate) fn parse_book_frame_owned(data: Value) -> Option<ParsedBook> {
    parse_raw(serde_json::from_value(data).ok()?)
}

fn parse_raw(raw: RawBookData) -> Option<ParsedBook> {
    if raw.bids.len() > MAX_BOOK_LEVELS_PER_SIDE || raw.asks.len() > MAX_BOOK_LEVELS_PER_SIDE {
        return None;
    }
    let bids = raw
        .bids
        .into_iter()
        .map(RawBookLevel::into_level)
        .collect::<Option<Vec<_>>>()?;
    let asks = raw
        .asks
        .into_iter()
        .map(RawBookLevel::into_level)
        .collect::<Option<Vec<_>>>()?;
    Some(ParsedBook {
        symbol: raw.symbol,
        bids,
        asks,
        checksum: raw.checksum,
        timestamp: raw.timestamp,
    })
}

/// Parse a wire decimal (JSON string or number) → typed [`Decimal`] + canonical text for CRC32.
fn parse_decimal(v: Value) -> Option<(Decimal, String)> {
    match v {
        Value::String(s) => Decimal::from_str(&s).ok().map(|d| (d, s)),
        Value::Number(n) => {
            let s = n.to_string();
            Decimal::from_str(&s).ok().map(|d| (d, s))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn level() -> Value {
        json!({ "price": "50000.0", "qty": "1.0" })
    }

    #[test]
    fn parse_book_frame_accepts_normal_frame() {
        let data = json!({
            "symbol": "BTC/USD",
            "bids": [{"price":"50000.0","qty":"1.0"}, {"price":"49999.0","qty":"2.0"}],
            "asks": [{"price":"50001.0","qty":"1.5"}],
            "checksum": 123u32,
        });
        let parsed = parse_book_frame(&data).expect("a normal frame parses");
        assert_eq!(parsed.symbol, "BTC/USD");
        assert_eq!(parsed.bids.len(), 2);
        assert_eq!(parsed.asks.len(), 1);
        assert_eq!(parsed.checksum, 123);
    }

    #[test]
    fn parse_book_frame_exactly_at_cap_is_accepted() {
        let bids: Vec<Value> = (0..MAX_BOOK_LEVELS_PER_SIDE).map(|_| level()).collect();
        let data = json!({ "symbol": "BTC/USD", "bids": bids, "asks": [], "checksum": 0u32 });
        assert!(
            parse_book_frame(&data).is_some(),
            "a side exactly at the cap is accepted"
        );
    }

    #[test]
    fn parse_book_frame_over_cap_is_rejected_as_malformed() {
        // One past the cap → None before per-level parse.
        let over: Vec<Value> = (0..=MAX_BOOK_LEVELS_PER_SIDE).map(|_| level()).collect();
        let bids_over = json!({
            "symbol": "BTC/USD", "bids": over.clone(), "asks": [], "checksum": 0u32
        });
        assert!(
            parse_book_frame(&bids_over).is_none(),
            "oversized bids side is rejected"
        );
        let asks_over = json!({
            "symbol": "BTC/USD", "bids": [], "asks": over, "checksum": 0u32
        });
        assert!(
            parse_book_frame(&asks_over).is_none(),
            "oversized asks side is rejected"
        );
    }
}
