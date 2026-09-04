//! `Tier` — Kraken account tier; carries per-tier rate-limit caps and decay rates.

/// Kraken account tier. Determines rate-limit caps and decay rates for both
/// the non-trading REST counter and the per-pair trading counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum Tier {
    /// Base verification level (default): api cap 15 / 0.33/s, trading cap 60 / 1.00/s.
    #[default]
    Starter,
    /// Intermediate: api cap 20 / 0.50/s, trading cap 125 / 2.34/s.
    Intermediate,
    /// Pro: api cap 20 / 1.00/s, trading cap 180 / 3.75/s.
    Pro,
}

impl Tier {
    /// Per-tier cap for the non-trading REST counter (api tracker).
    pub(crate) fn api_cap(self) -> f64 {
        match self {
            Tier::Starter => 15.0,
            Tier::Intermediate => 20.0,
            Tier::Pro => 20.0,
        }
    }

    /// Per-tier decay rate (units per second) for the api counter.
    pub(crate) fn api_decay_per_sec(self) -> f64 {
        match self {
            Tier::Starter => 0.33,
            Tier::Intermediate => 0.50,
            Tier::Pro => 1.00,
        }
    }

    /// Per-tier cap for the trading counter (per-pair).
    pub(crate) fn trading_cap(self) -> f64 {
        match self {
            Tier::Starter => 60.0,
            Tier::Intermediate => 125.0,
            Tier::Pro => 180.0,
        }
    }

    /// Per-tier decay rate (units/s) for the trading counter.
    pub(crate) fn trading_decay_per_sec(self) -> f64 {
        match self {
            Tier::Starter => 1.00,
            Tier::Intermediate => 2.34,
            Tier::Pro => 3.75,
        }
    }
}

// KYC-driven; set via ClientBuilder::with_tier_override — docs/guides/configuration.md.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_tier_is_starter() {
        assert_eq!(Tier::default(), Tier::Starter);
    }

    #[test]
    fn api_cap_matches_wire_facts_per_tier() {
        assert_eq!(Tier::Starter.api_cap(), 15.0);
        assert_eq!(Tier::Intermediate.api_cap(), 20.0);
        assert_eq!(Tier::Pro.api_cap(), 20.0);
    }

    #[test]
    fn trading_cap_matches_wire_facts_per_tier() {
        assert_eq!(Tier::Starter.trading_cap(), 60.0);
        assert_eq!(Tier::Intermediate.trading_cap(), 125.0);
        assert_eq!(Tier::Pro.trading_cap(), 180.0);
    }

    #[test]
    fn trading_decay_matches_wire_facts_per_tier() {
        assert!((Tier::Starter.trading_decay_per_sec() - 1.00).abs() < f64::EPSILON);
        assert!((Tier::Intermediate.trading_decay_per_sec() - 2.34).abs() < f64::EPSILON);
        assert!((Tier::Pro.trading_decay_per_sec() - 3.75).abs() < f64::EPSILON);
    }
}
