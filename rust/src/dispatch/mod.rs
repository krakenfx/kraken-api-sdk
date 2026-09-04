//! Dispatch layer: reactor, event bus, and static dispatch table.
//! I/O→dispatch drops oldest with `QueueFullWarning`; caller→I/O rejects with `QueueFullError`.

pub mod dispatch_table;
pub mod event_bus;
pub mod handler_registry;
pub mod io_reactor;
// `ws_bridge.rs`: deprecated WS shim superseded by `io_reactor`; kept on
// disk but intentionally not wired as a module.

pub use dispatch_table::{Op, Transport};
pub use event_bus::{
    AckSource, CallbackSource, CallerInbound, ClientCloseReason, ClientFailureCause, ClosedReason,
    DeadmanDisarmCause, DispatchEventBus, DispatchEventBusConfig, ErrorClass, EventCallback,
    EventEnvelope, EventPayload, EventType, GapCause, HandlerMutationOp, LoopFailureCause,
    NonTransientClass, OrderOp, OrderSubmitStatus, PostReject, ReactorName, RegistryMutationOp,
    TransientClass, WsFailReason, WsOp,
};
pub(crate) use handler_registry::{
    DataDelivery, DataInvoker, HandlerCallback, HandlerDecodeOutcome, HandlerDecoder,
};
pub use handler_registry::{HandlerHandle, HandlerId, HandlerRegistry, PresenceMirror};
