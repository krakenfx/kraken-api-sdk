//! Shared newtypes used across all SDK layers. Monetary values use
//! `rust_decimal::Decimal`, never `f64`.

mod asset;
mod channel;
mod connection;
mod handles;
mod ids;
mod instant;
mod symbol;

pub use asset::AssetCode;
pub use channel::{AssetClass, BookDepth, ChannelName, OhlcInterval, TickerTrigger, WsUrl};
pub use connection::ConnectionState;
pub use handles::{
    CapabilitySnapshot, ConnectionHandle, DispatchKeyStub, ExpectedCompletionEvent, NamespaceName,
    QueueFullError, RequestHandle, SubscriberHandle,
};
pub use ids::{ApiKey, ApiSecretError, ClOrdId, ClOrdIdError, Nonce, TxId};
pub use instant::MonotonicInstant;
pub use symbol::{Symbol, SymbolError};

pub(crate) use handles::CallerEvent;
pub(crate) use ids::SignedRequest;

// Public only under the harness feature, for `test_support`'s gated re-export.
#[cfg(feature = "test-support")]
pub use ids::{ApiSecret, AuthProfile, Otp};
#[cfg(not(feature = "test-support"))]
pub(crate) use ids::{ApiSecret, AuthProfile, Otp};
