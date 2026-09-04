//! Connection layer: Public + Auth managed connections and a host-scoped
//! connection-rate budget. Supervisor methods are sync; completions arrive on
//! the event bus.

pub mod managed_connection;
pub mod rate_budget;
pub mod subscribe_error;
pub mod subscription_registry;
pub mod supervisor;

pub use managed_connection::{AuthErrorKind, ManagedConnection};
pub use subscribe_error::SubscribeErrorKind;
#[cfg(test)]
pub use subscription_registry::SubscriptionRegistry;
pub use subscription_registry::{SubscribeParams, SubscriptionEntry};
pub use supervisor::ConnectionSupervisor;
