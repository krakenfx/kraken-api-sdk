//! Rate-limit snap helpers for the trade namespace: snap the counter that owns
//! a rejection on the REST and WS order paths.

use std::sync::Arc;

use crate::rate_limit::{Scope, SnapTarget, classify_rate_limit_snap};
use crate::rest::RestSurface;
use crate::types::{MonotonicInstant, Symbol};

/// Classify `RestError::Kraken` code strings and snap the TRADING rate-limit
/// counter. `pair` is `None` on amend/cancel index-MISS ⇒ skip the pair snap.
/// EAPI:/EAuth:/EService: are snapped at the `signed_post_costed` chokepoint, not here.
pub(crate) fn snap_on_rest_rejection(
    rest: &Arc<RestSurface>,
    codes: &[String],
    pair: Option<&Symbol>,
    now: MonotonicInstant,
) {
    let Some(api_key) = rest.api_key() else {
        return;
    };
    for code in codes {
        match classify_rate_limit_snap(code) {
            Some(SnapTarget::TradingPair) => {
                // Index MISS → skip the Pair snap.
                if let Some(p) = pair {
                    rest.trading_tracker().snap_to_cap(
                        Scope::Pair(api_key.clone(), p.clone()),
                        code,
                        now,
                    );
                }
                // One snap per rejection — first matching code wins.
                return;
            }
            Some(SnapTarget::TradingDomain) => {
                rest.trading_tracker()
                    .snap_to_cap(Scope::ApiKey(api_key.clone()), code, now);
                return;
            }
            // Api snaps are handled by `signed_post_costed`.
            Some(SnapTarget::Api) | None => {}
        }
    }
}

/// Classify a WS order-response error string (single string, not an array) and
/// snap the owning rate-limit counter. `pair` follows the same source/MISS rules
/// as `snap_on_rest_rejection`.
pub(crate) fn snap_on_ws_rejection(
    rest: &Arc<RestSurface>,
    error: &Option<String>,
    pair: Option<&Symbol>,
    now: MonotonicInstant,
) {
    let Some(api_key) = rest.api_key() else {
        return;
    };
    let Some(msg) = error.as_deref() else { return };
    match classify_rate_limit_snap(msg) {
        Some(SnapTarget::TradingPair) => {
            if let Some(p) = pair {
                rest.trading_tracker().snap_to_cap(
                    Scope::Pair(api_key.clone(), p.clone()),
                    msg,
                    now,
                );
            }
        }
        Some(SnapTarget::TradingDomain) => {
            rest.trading_tracker()
                .snap_to_cap(Scope::ApiKey(api_key.clone()), msg, now);
        }
        Some(SnapTarget::Api) => {
            rest.api_tracker()
                .snap_to_cap(Scope::ApiKey(api_key.clone()), msg, now);
        }
        None => {}
    }
}
