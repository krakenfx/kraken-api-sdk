//! Age-decaying trading-rate cost for amend/cancel: younger orders cost more.

use std::time::Duration;

/// The operation whose age-decaying cost is being computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicCostOp {
    /// `POST /0/private/AmendOrder` (or WS `amend_order`).
    AmendOrder,
    /// `POST /0/private/CancelOrder` (or WS `cancel_order`).
    CancelOrder,
}

/// Per-pair trading-rate cost for amend/cancel, decaying with order age.
/// Brackets: docs/guides/rate-limits.md.
pub fn order_age_cost(op: DynamicCostOp, age: Duration) -> f64 {
    let s = age.as_secs_f64();
    match op {
        DynamicCostOp::AmendOrder => {
            1.0 + if s < 5.0 {
                3.0
            } else if s < 10.0 {
                2.0
            } else if s < 15.0 {
                1.0
            } else {
                0.0
            }
        }
        DynamicCostOp::CancelOrder => {
            if s < 5.0 {
                8.0
            } else if s < 10.0 {
                6.0
            } else if s < 15.0 {
                5.0
            } else if s < 45.0 {
                4.0
            } else if s < 90.0 {
                2.0
            } else if s < 300.0 {
                1.0
            } else {
                0.0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn age_f(secs: f64) -> Duration {
        Duration::from_secs_f64(secs)
    }

    #[test]
    fn amend_cost_brackets() {
        // (age secs, expected cost, label)
        let cases = [
            (4.0, 4.0, "below 5s"),
            (7.0, 3.0, "5s to 9s"),
            (12.0, 2.0, "10s to 14s"),
            (20.0, 1.0, "15s and above"),
            (5.0, 3.0, "boundary exactly 5s"),
            (15.0, 1.0, "boundary exactly 15s"),
        ];
        for (secs, expected, label) in cases {
            let got = order_age_cost(DynamicCostOp::AmendOrder, age_f(secs));
            assert!(
                (got - expected).abs() < 1e-9,
                "{label}: got {got}, want {expected}"
            );
        }
    }

    #[test]
    fn cancel_cost_brackets() {
        // (age secs, expected cost, label)
        let cases = [
            (4.0, 8.0, "below 5s"),
            (7.0, 6.0, "5s to 9s"),
            (12.0, 5.0, "10s to 14s"),
            (30.0, 4.0, "15s to 44s"),
            (60.0, 2.0, "45s to 89s"),
            (200.0, 1.0, "90s to 299s"),
            (400.0, 0.0, "300s and above"),
            (5.0, 6.0, "boundary exactly 5s"),
            (300.0, 0.0, "boundary exactly 300s"),
        ];
        for (secs, expected, label) in cases {
            let got = order_age_cost(DynamicCostOp::CancelOrder, age_f(secs));
            assert!(
                (got - expected).abs() < 1e-9,
                "{label}: got {got}, want {expected}"
            );
        }
    }
}
