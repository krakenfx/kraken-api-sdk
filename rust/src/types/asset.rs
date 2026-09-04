//! Asset-code normalisation and the `AssetCode` validating newtype. Legacy
//! X/Z-heritage codes map to modern form on output; modern bare codes pass
//! through. See docs/guides/wire-quirks.md.

use crate::types::SymbolError;
use serde::Serialize;
use std::borrow::Borrow;

/// Modern user-facing asset code (`BTC`, `USD`, ...). Validating newtype with two
/// paths: `new`/`TryFrom`/`FromStr` strictly reject caller input (empty, legacy
/// X/Z forms, non-alpha); `from_wire` normalises server output, infallibly.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct AssetCode(String);

impl AssetCode {
    /// Construct from caller-supplied input (strict validation).
    pub fn new(s: impl Into<String>) -> Result<Self, SymbolError> {
        let s = s.into();
        if s.is_empty() {
            return Err(SymbolError::Empty);
        }
        if !s.chars().all(|c| c.is_ascii_alphabetic()) {
            return Err(SymbolError::NonAlphabetic);
        }
        // Uppercase before the legacy check (table is uppercase-keyed).
        let s = s.to_ascii_uppercase();
        // Table-driven reject — not a leading X/Z strip — so XRP/XLM/ZEC stay valid.
        if normalise_asset(&s) != s {
            return Err(SymbolError::LegacyPrefix);
        }
        Ok(Self(s))
    }

    /// Construct from a server-returned asset code (normalise then wrap). Infallible.
    pub(crate) fn from_wire(raw: &str) -> Self {
        Self(normalise_asset(raw).to_string())
    }

    /// Borrow the stored code as `&str` — construction already normalised or rejected legacy forms.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AssetCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl AsRef<str> for AssetCode {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl Borrow<str> for AssetCode {
    // Hash of inner String == str hash; enables HashMap<AssetCode,_>::get("BTC").
    fn borrow(&self) -> &str {
        &self.0
    }
}
impl TryFrom<&str> for AssetCode {
    type Error = SymbolError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}
impl std::str::FromStr for AssetCode {
    type Err = SymbolError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

/// Normalise a single raw Kraken asset code to its modern user-facing form.
/// Unknown or already-modern codes pass through unchanged. Bounded sync map
/// lookup; no I/O; infallible.
pub(crate) fn normalise_asset(raw: &str) -> &str {
    match raw {
        "XXBT" => "BTC", // altname XBT is deprecated → BTC
        "XBT" => "BTC",
        "XETC" => "ETC",
        "XETH" => "ETH",
        "XLTC" => "LTC",
        "XMLN" => "MLN",
        "XREP" => "REP",
        "XXDG" => "XDG",
        "XXLM" => "XLM",
        "XXMR" => "XMR",
        "XXRP" => "XRP",
        "XZEC" => "ZEC",
        "ZARS" => "ARS",
        "ZAUD" => "AUD",
        "ZCAD" => "CAD",
        "ZCLP" => "CLP",
        "ZCOP" => "COP",
        "ZDKK" => "DKK",
        "ZEUR" => "EUR",
        "ZGBP" => "GBP",
        "ZGEL" => "GEL",
        "ZGHS" => "GHS",
        "ZJPY" => "JPY",
        "ZLKR" => "LKR",
        "ZMXN" => "MXN",
        "ZPLN" => "PLN",
        "ZSEK" => "SEK",
        "ZUGX" => "UGX",
        "ZUSD" => "USD",
        "ZVND" => "VND",
        "ZXOF" => "XOF",
        "KFEE" => "FEE",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xxbt_maps_to_btc_not_xbt() {
        assert_eq!(normalise_asset("XXBT"), "BTC");
        assert_ne!(normalise_asset("XXBT"), "XBT");
    }

    #[test]
    fn zusd_maps_to_usd() {
        assert_eq!(normalise_asset("ZUSD"), "USD");
    }

    #[test]
    fn xbt_deprecated_ticker_maps_to_btc() {
        assert_eq!(normalise_asset("XBT"), "BTC");
        assert_eq!(AssetCode::new("XBT"), Err(SymbolError::LegacyPrefix));
        assert_eq!(AssetCode::from_wire("XBT").as_str(), "BTC");
    }

    #[test]
    fn xeth_maps_to_eth_and_zeur_to_eur() {
        assert_eq!(normalise_asset("XETH"), "ETH");
        assert_eq!(normalise_asset("ZEUR"), "EUR");
    }

    #[test]
    fn kfee_maps_to_fee() {
        assert_eq!(normalise_asset("KFEE"), "FEE");
    }

    #[test]
    fn modern_bare_codes_pass_through_unchanged() {
        assert_eq!(normalise_asset("USDC"), "USDC");
        assert_eq!(normalise_asset("BABY"), "BABY");
        assert_eq!(normalise_asset("UNKNWN"), "UNKNWN");
    }

    #[test]
    fn asset_code_new_btc_ok() {
        let a = AssetCode::new("BTC").expect("BTC is a valid modern asset code");
        assert_eq!(a.as_str(), "BTC");
    }

    #[test]
    fn asset_code_new_legacy_xxbt_rejected() {
        assert_eq!(AssetCode::new("XXBT"), Err(SymbolError::LegacyPrefix));
    }

    #[test]
    fn asset_code_new_legacy_zusd_rejected() {
        assert_eq!(AssetCode::new("ZUSD"), Err(SymbolError::LegacyPrefix));
    }

    #[test]
    fn asset_code_new_modern_x_z_prefix_codes_accepted() {
        assert!(AssetCode::new("XRP").is_ok(), "XRP is a modern code");
        assert!(AssetCode::new("XLM").is_ok(), "XLM is a modern code");
        assert!(AssetCode::new("ZEC").is_ok(), "ZEC is a modern code");
    }

    #[test]
    fn asset_code_new_empty_rejected() {
        assert_eq!(AssetCode::new(""), Err(SymbolError::Empty));
    }

    #[test]
    fn asset_code_new_lowercase_legacy_rejected() {
        assert_eq!(AssetCode::new("xxbt"), Err(SymbolError::LegacyPrefix));
        assert_eq!(AssetCode::new("zusd"), Err(SymbolError::LegacyPrefix));
    }

    #[test]
    fn asset_code_new_lowercase_modern_canonicalised() {
        assert_eq!(AssetCode::new("btc").unwrap().as_str(), "BTC");
        assert_eq!(AssetCode::new("Usdc").unwrap().as_str(), "USDC");
    }

    #[test]
    fn asset_code_new_non_alphabetic_rejected() {
        assert_eq!(AssetCode::new("XBT.B"), Err(SymbolError::NonAlphabetic));
    }

    #[test]
    fn asset_code_from_wire_normalises_legacy() {
        let a = AssetCode::from_wire("XXBT");
        assert_eq!(a.as_str(), "BTC", "from_wire(XXBT) must normalise to BTC");
    }

    #[test]
    fn asset_code_from_wire_passthrough_unknown() {
        let a = AssetCode::from_wire("XBT.B");
        assert_eq!(
            a.as_str(),
            "XBT.B",
            "from_wire unknown passthrough verbatim"
        );
    }

    #[test]
    fn asset_code_borrow_str_works_in_hashmap() {
        use std::collections::HashMap;
        let mut map: HashMap<AssetCode, i32> = HashMap::new();
        map.insert(AssetCode::new("BTC").unwrap(), 42);
        let val = map.get("BTC");
        assert_eq!(
            val,
            Some(&42),
            "HashMap<AssetCode,_>::get(\"BTC\") must work via Borrow<str>"
        );
    }
}
