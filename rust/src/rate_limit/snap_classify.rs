//! Rate-limit snap classifier: wire-code → tracker/scope predicate.
//! Matching rules: docs/guides/error-handling.md.

/// Which tracker and scope to snap, as returned by [`classify_rate_limit_snap`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapTarget {
    /// Trading tracker, per-pair (`EOrder:Rate limit exceeded`).
    TradingPair,
    /// Trading tracker, account/domain (`EOrder:Domain rate limit exceeded`).
    TradingDomain,
    /// API tracker (`EAPI` / `EAuth` / `EGeneral:Too many requests` / `EService:Throttled`).
    Api,
}

/// Maps one Kraken wire error code to the tracker/scope that owns the rejection.
pub(crate) fn classify_rate_limit_snap(code: &str) -> Option<SnapTarget> {
    if code == "EOrder:Rate limit exceeded" {
        return Some(SnapTarget::TradingPair);
    }
    if code == "EOrder:Domain rate limit exceeded" {
        return Some(SnapTarget::TradingDomain);
    }
    if code.starts_with("EAPI:Rate limit exceeded")
        || code.starts_with("EAuth:Rate limit exceeded")
        || code.starts_with("EGeneral:Too many requests")
        || code.starts_with("EService:Throttled")
    {
        return Some(SnapTarget::Api);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eorder_exact_match() {
        assert_eq!(
            classify_rate_limit_snap("EOrder:Rate limit exceeded"),
            Some(SnapTarget::TradingPair)
        );
        assert_eq!(
            classify_rate_limit_snap("EOrder:Domain rate limit exceeded"),
            Some(SnapTarget::TradingDomain)
        );
    }

    #[test]
    fn eapi_starts_with_match() {
        assert_eq!(
            classify_rate_limit_snap("EAPI:Rate limit exceeded"),
            Some(SnapTarget::Api)
        );
        assert_eq!(
            classify_rate_limit_snap("EAPI:Rate limit exceeded: 1234567890"),
            Some(SnapTarget::Api)
        );
    }

    #[test]
    fn eauth_starts_with_match() {
        assert_eq!(
            classify_rate_limit_snap("EAuth:Rate limit exceeded"),
            Some(SnapTarget::Api)
        );
    }

    #[test]
    fn egeneral_too_many_requests_starts_with_match() {
        assert_eq!(
            classify_rate_limit_snap("EGeneral:Too many requests"),
            Some(SnapTarget::Api)
        );
    }

    #[test]
    fn eservice_throttled_starts_with_match() {
        assert_eq!(
            classify_rate_limit_snap("EService:Throttled"),
            Some(SnapTarget::Api)
        );
        assert_eq!(
            classify_rate_limit_snap("EService:Throttled: 1717200000"),
            Some(SnapTarget::Api)
        );
    }

    #[test]
    fn eapi_invalid_key_does_not_snap() {
        assert_eq!(classify_rate_limit_snap("EAPI:Invalid key"), None);
        assert_eq!(classify_rate_limit_snap("EAPI:Invalid nonce"), None);
        assert_eq!(classify_rate_limit_snap("EGeneral:Unknown order"), None);
        assert_eq!(classify_rate_limit_snap(""), None);
    }

    #[test]
    fn eorder_with_suffix_does_not_match() {
        assert_eq!(
            classify_rate_limit_snap("EOrder:Rate limit exceeded: extra"),
            None
        );
    }
}
