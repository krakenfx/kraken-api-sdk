//! REST surface — six-stage pipeline: ApplyRateLimit → Sign → Send →
//! ParseEnvelope → ClassifyError → ApplyRetry. Retry re-enters at stage 1
//! (fresh nonce); order is a cross-binding contract.

pub mod retry;
pub mod surface;

pub(crate) use surface::mint_request_id;
pub use surface::{RateLimitCost, RestError, RestSurface};

pub(crate) use retry::RetryPolicy;
pub use retry::RetryReason;
