//! Shared WS-callback decode helpers used by every `on_*` registration across
//! the market and account namespaces.

use std::sync::Arc;

use rust_decimal::Decimal;
use serde::Deserialize;
use serde::de::{DeserializeOwned, Deserializer};
use serde_json::Value;

use crate::api::market::ws_types::parse_wire_decimal;
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

/// `true` when the reactor's envelope carries `type: "snapshot"`.
pub(crate) fn is_snapshot_frame(v: &Value) -> bool {
    v.get("type").and_then(Value::as_str) == Some("snapshot")
}

/// Decode each element of a JSON array, skipping any that fail (logged by `field`, never
/// by content); anything that is not an array yields no rows.
pub(crate) fn lenient_rows<T: DeserializeOwned>(
    field: &'static str,
    rows: Option<Value>,
) -> Vec<T> {
    match rows {
        Some(Value::Array(rows)) => {
            let total = rows.len();
            let decoded: Vec<T> = rows
                .into_iter()
                .filter_map(|row| serde_json::from_value(row).ok())
                .collect();
            if decoded.len() < total {
                warn_decode_drop(field, "row skipped");
            }
            decoded
        }
        None | Some(Value::Null) => Vec::new(),
        Some(_) => {
            warn_decode_drop(field, "container not an array");
            Vec::new()
        }
    }
}

/// Throttle drop logs to one per second per (channel, reason). The window is
/// shared by every client in the process and uses `Instant`, not the injected clock.
pub(crate) fn warn_decode_drop(channel: &'static str, reason: &'static str) {
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    const WINDOW: Duration = Duration::from_secs(1);
    static LAST: Mutex<Vec<(&'static str, &'static str, Instant)>> = Mutex::new(Vec::new());

    let now = Instant::now();
    let mut last = LAST.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let emit = match last
        .iter_mut()
        .find(|row| row.0 == channel && row.1 == reason)
    {
        Some(row) => {
            let due = now.saturating_duration_since(row.2) >= WINDOW;
            if due {
                row.2 = now;
            }
            due
        }
        None => {
            last.push((channel, reason, now));
            true
        }
    };
    drop(last);
    if emit {
        tracing::warn!(
            target: "kraken_sdk::decode",
            channel,
            reason,
            "WS record dropped"
        );
    }
}

/// Parse an optional wire money value; a null, absent, or unparseable value is `None`.
pub(crate) fn decimal(value: Option<Value>) -> Option<Decimal> {
    value.as_ref().and_then(parse_wire_decimal)
}

/// Field deserializer for [`decimal`].
pub(crate) fn deserialize_optional_decimal<'de, D>(
    deserializer: D,
) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(decimal(Option::<Value>::deserialize(deserializer)?))
}

/// Field deserializer: a value of the wrong type becomes `None` instead of failing the row.
pub(crate) fn lenient_opt<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    Ok(Option::<Value>::deserialize(deserializer)?.and_then(|v| serde_json::from_value(v).ok()))
}

/// Field deserializer: a non-array value, or an element of the wrong type, is skipped.
pub(crate) fn lenient_vec_for<'de, D, T>(
    field: &'static str,
    deserializer: D,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    Ok(lenient_rows(
        field,
        Option::<Value>::deserialize(deserializer)?,
    ))
}

/// Keep each item's first occurrence, in order. A duplicate would register twice under one
/// guard record and over-release on drop.
pub(crate) fn dedupe_first_seen<T: Eq + std::hash::Hash + Clone>(mut items: Vec<T>) -> Vec<T> {
    let mut seen = std::collections::HashSet::with_capacity(items.len());
    items.retain(|item| seen.insert(item.clone()));
    items
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct SkipProbe {
        n: u32,
    }

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct ContainerProbe {
        n: u32,
    }

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct AbsentProbe {
        n: u32,
    }

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct NullProbe {
        n: u32,
    }

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct CleanProbe {
        n: u32,
    }

    #[test]
    fn a_skipped_row_is_logged_by_field_and_never_by_content() {
        let mut rows = Vec::new();
        let logs = crate::test_logs::capture_logs(|| {
            rows = lenient_rows::<SkipProbe>(
                "probe.skip",
                Some(json!([{"n": 1}, {"n": "SECRET-9"}, {"n": 3}])),
            );
        });
        assert_eq!(rows, vec![SkipProbe { n: 1 }, SkipProbe { n: 3 }]);
        assert_eq!(logs.matches("record dropped").count(), 1, "{logs}");
        assert!(logs.contains("probe.skip"), "{logs}");
        assert!(
            !logs.contains("SECRET"),
            "a wire value reached the log: {logs}"
        );
    }

    #[test]
    fn a_non_array_container_is_logged_by_field() {
        let logs = crate::test_logs::capture_logs(|| {
            assert!(
                lenient_rows::<ContainerProbe>("probe.container", Some(json!({"n": 1}))).is_empty()
            );
        });
        assert_eq!(logs.matches("record dropped").count(), 1, "{logs}");
        assert!(logs.contains("container not an array"), "{logs}");
    }

    #[test]
    fn an_absent_container_logs_nothing() {
        let logs = crate::test_logs::capture_logs(|| {
            assert!(lenient_rows::<AbsentProbe>("probe.absent", None).is_empty());
            assert!(lenient_rows::<NullProbe>("probe.null", Some(Value::Null)).is_empty());
        });
        assert!(logs.is_empty(), "{logs}");
    }

    #[test]
    fn two_fields_of_one_element_type_log_separately() {
        let logs = crate::test_logs::capture_logs(|| {
            let bad = || Some(json!(["ok", 7]));
            assert_eq!(lenient_rows::<String>("probe.first", bad()).len(), 1);
            assert_eq!(lenient_rows::<String>("probe.second", bad()).len(), 1);
        });
        assert_eq!(logs.matches("record dropped").count(), 2, "{logs}");
        assert!(
            logs.contains("probe.first") && logs.contains("probe.second"),
            "{logs}"
        );
    }

    #[test]
    fn a_clean_array_logs_nothing() {
        let logs = crate::test_logs::capture_logs(|| {
            let rows = lenient_rows::<CleanProbe>("probe.clean", Some(json!([{"n": 1}, {"n": 2}])));
            assert_eq!(rows.len(), 2);
        });
        assert!(logs.is_empty(), "{logs}");
    }
}
