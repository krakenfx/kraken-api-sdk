//! Shared WS-callback decode helpers used by every `on_*` registration across
//! the market and account namespaces.

use std::sync::Arc;

use serde_json::Value;

use crate::dispatch::{DataInvoker, HandlerDecoder};
use crate::dispatch::{HandlerCallback, HandlerDecodeOutcome};

/// Extract the per-entry `data` object from the reactor's
/// `{ "type", "data": entry }` envelope. Returns `None` on any non-object /
/// missing-`data` shape (→ the caller's decoder signals `DecodeFailed`).
pub(crate) fn extract_data(v: Value) -> Option<Value> {
    match v {
        Value::Object(mut m) => m.remove("data"),
        _ => None,
    }
}

/// Adapt a typed `Fn(&T)` user callback into the type-erased `HandlerCallback`.
/// `decode` parses the wire frame on the I/O reactor (`None` -> DecodeFailed);
/// the type-erased `invoke` is built once here and cloned per frame.
pub(crate) fn wrap_typed<T, F, D>(cb: F, decode: D) -> HandlerCallback
where
    T: Send + Sync + 'static,
    F: Fn(&T) + Send + Sync + 'static,
    D: Fn(Value) -> Option<T> + Send + Sync + 'static,
{
    // The downcast cannot fail: the same `wrap_typed` produced both the shared
    // `T` and this `T`-typed downcast. A mismatch is therefore dropped silently.
    let invoke: DataInvoker = Arc::new(move |payload| {
        if let Some(t) = payload.downcast_ref::<T>() {
            cb(t);
        }
    });
    let decoder: HandlerDecoder = Arc::new(move |v| match decode(v) {
        Some(t) => HandlerDecodeOutcome::Deliver(Arc::new(t)),
        None => HandlerDecodeOutcome::DecodeFailed,
    });
    // Bundle the per-frame decoder with the same typed `invoke` so the reactor's
    // maintained-`Book` fast-path can deliver a typed payload directly via
    // `HandlerEntry::callback.invoke` (no encode→re-decode round-trip).
    HandlerCallback::new(decoder, invoke)
}
