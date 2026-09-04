//! Wire-form trading pair identifiers.

use serde::Serialize;
/// Wire-form trading pair, e.g. `"BTC/USD"` — the modern `BASE/QUOTE` form.
/// Legacy aliases (`XBT`, `XXBT`/`ZUSD` forms) are rejected at construction;
/// `Ord` is lexicographic over the wire string for a stable resubscription sort.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Symbol(String);

impl Symbol {
    /// Construct from a wire-form string, validating shape only (modern
    /// `BASE/QUOTE` with non-legacy legs). Pair *existence* is validated by the
    /// API at first use (surfaced as `MarketError::SymbolNotFound`).
    pub fn new(wire: impl Into<String>) -> Result<Self, SymbolError> {
        let s = wire.into();
        if s.is_empty() {
            return Err(SymbolError::Empty);
        }
        // Reject legacy concatenated X/Z forms (e.g. "XXBTZUSD") before the separator check.
        if s.starts_with("XX") || s.starts_with("ZZ") {
            return Err(SymbolError::LegacyPrefix);
        }
        let (base, quote) = s.split_once('/').ok_or(SymbolError::MissingSeparator)?;
        Self::validate_leg(base)?;
        Self::validate_leg(quote)?;
        // Uppercase to match Kraken's uppercase pair echo (ack routing). See docs/guides/wire-quirks.md.
        Ok(Self(s.to_ascii_uppercase()))
    }

    /// Validate one leg: non-empty, alphanumeric, non-legacy. Digits allowed
    /// (`1INCH`, `AI16Z`); legacy codes rejected via `normalise_asset`.
    fn validate_leg(leg: &str) -> Result<(), SymbolError> {
        if leg.is_empty() {
            return Err(SymbolError::Empty);
        }
        if !leg.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(SymbolError::NonAlphanumeric);
        }
        let upper = leg.to_ascii_uppercase();
        if super::asset::normalise_asset(&upper) != upper {
            return Err(SymbolError::LegacyPrefix);
        }
        Ok(())
    }

    /// Borrow the wire-form string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Symbol {
    /// Bare wire-form pair string (e.g. `BTC/USD`). Ports keep this byte-identical
    /// for error messages: docs/guides/wire-quirks.md.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Errors from constructing a [`Symbol`] from an untrusted string. The
/// "pair doesn't exist on Kraken" case is a separate `MarketError::SymbolNotFound`
/// surfaced at API first-use (not at construction).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SymbolError {
    /// The whole string or one `BASE`/`QUOTE` leg is empty (e.g. `"BTC/"`).
    #[error("Symbol cannot be empty.")]
    Empty,
    /// A legacy alias was passed: `XBT`, an X/Z-heritage code (`ZUSD`), or a concatenated pair (`XXBTZUSD`).
    #[error("Legacy X/Z prefix not accepted; use the modern form (e.g. BTC/USD, not XXBTZUSD).")]
    LegacyPrefix,
    /// A `Symbol` leg contains characters outside ASCII alphanumerics. Digits
    /// are fine (real bases include them, e.g. `1INCH`, `AI16Z`); separators
    /// like `:` are not.
    #[error("Symbol leg must be alphanumeric; got a non-alphanumeric character.")]
    NonAlphanumeric,
    /// An `AssetCode` contains a non-alphabetic character. Asset codes are
    /// alphabetic-only, unlike `Symbol` legs.
    #[error("Asset code must be alphabetic; got a non-alphabetic character.")]
    NonAlphabetic,
    /// No `/` separator found — concatenated forms like `"BTCUSD"` land here, not `LegacyPrefix`.
    #[error("Symbol must use the modern BASE/QUOTE form with a '/' separator (e.g. BTC/USD).")]
    MissingSeparator,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_accepts_canonical_form() {
        let s = Symbol::new("BTC/USD").expect("BTC/USD is canonical");
        assert_eq!(s.as_str(), "BTC/USD");
    }

    #[test]
    fn symbol_rejects_empty() {
        assert_eq!(Symbol::new(""), Err(SymbolError::Empty));
    }

    #[test]
    fn symbol_rejects_legacy_x_z_prefixes() {
        assert_eq!(Symbol::new("XXBTZUSD"), Err(SymbolError::LegacyPrefix));
        assert_eq!(Symbol::new("ZZ-anything"), Err(SymbolError::LegacyPrefix));
    }

    #[test]
    fn symbol_canonicalises_case_to_uppercase() {
        assert_eq!(Symbol::new("btc/usd").unwrap().as_str(), "BTC/USD");
        assert_eq!(Symbol::new("Eth/Usd").unwrap().as_str(), "ETH/USD");
        assert_eq!(Symbol::new("1inch/usd").unwrap().as_str(), "1INCH/USD");
    }

    #[test]
    fn symbol_accepts_digit_bearing_bases() {
        for pair in ["1INCH/USD", "0G/USD", "2Z/USD", "AI16Z/USDC"] {
            let s = Symbol::new(pair).unwrap_or_else(|e| panic!("{pair} should be valid: {e}"));
            assert_eq!(s.as_str(), pair);
        }
    }

    #[test]
    fn symbol_rejects_v2_venue_suffix() {
        assert_eq!(
            Symbol::new("BTC/USD:KDE"),
            Err(SymbolError::NonAlphanumeric)
        );
    }

    #[test]
    fn symbol_rejects_legacy_leg_aliases() {
        assert_eq!(Symbol::new("XBT/USD"), Err(SymbolError::LegacyPrefix));
        assert_eq!(Symbol::new("BTC/ZUSD"), Err(SymbolError::LegacyPrefix));
    }

    #[test]
    fn symbol_rejects_missing_separator() {
        assert_eq!(Symbol::new("BTCUSD"), Err(SymbolError::MissingSeparator));
        assert_eq!(Symbol::new("!!!"), Err(SymbolError::MissingSeparator));
    }

    #[test]
    fn symbol_rejects_non_alphanumeric_leg() {
        assert_eq!(Symbol::new("B!C/USD"), Err(SymbolError::NonAlphanumeric));
        assert_eq!(Symbol::new("BTC/US D"), Err(SymbolError::NonAlphanumeric));
    }

    #[test]
    fn symbol_rejects_empty_leg() {
        assert_eq!(Symbol::new("BTC/"), Err(SymbolError::Empty));
        assert_eq!(Symbol::new("/USD"), Err(SymbolError::Empty));
    }
}
