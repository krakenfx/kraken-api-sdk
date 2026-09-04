//! Offline public-surface proofs, compiled as an external crate: every type a
//! caller must hold is nameable through the crate root. No network, no credentials.

use kraken_sdk::{ChannelName, ClientBuilder, TickerUpdate};

#[tokio::test]
async fn handler_handle_is_nameable_through_the_crate_root() {
    let client = ClientBuilder::new()
        .build()
        .expect("unauthenticated build is infallible");
    let handle: kraken_sdk::HandlerHandle = client.market().on_ticker(|_t: &TickerUpdate| {});
    assert_eq!(handle.channel(), ChannelName::Ticker);
}
