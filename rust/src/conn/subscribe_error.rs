//! Subscribe-flow failure discriminator.

/// Discriminator for subscribe-flow failures. One variant suffices in v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeErrorKind {
    /// Wire-level subscribe rejection from Kraken.
    SubscribeRejected {
        kraken_code: String,
        transient: bool,
    },
}
