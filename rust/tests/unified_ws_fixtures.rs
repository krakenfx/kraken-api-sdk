//! Redacted inbound Unified WS frames: venue captures plus synthetic and docs-shaped frames.
//!
//! They contain no credentials and give Unified-WS tests real venue message
//! shapes without a live network dependency. Content is asserted by the decode
//! tests that consume each fixture; this file only guards the redaction contract.

use serde_json::Value;

fn assert_public_and_redacted(value: &Value, allowed: &[&str]) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                assert!(
                    allowed.contains(&key.as_str())
                        || !matches!(
                            key.as_str(),
                            "connection_id" | "token" | "api_key" | "secret" | "nonce"
                        ),
                    "fixture exposes a sensitive or connection-specific field: {key}"
                );
                assert_public_and_redacted(child, allowed);
            }
        }
        Value::Array(values) => {
            for child in values {
                assert_public_and_redacted(child, allowed);
            }
        }
        _ => {}
    }
}

#[test]
fn every_unified_ws_fixture_is_valid_json_public_and_redacted() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/unified_ws");
    let mut checked = 0;
    for entry in std::fs::read_dir(&dir).expect("fixture directory") {
        let path = entry.expect("fixture entry").path();
        let raw = std::fs::read_to_string(&path).expect("fixture is readable");
        let frame: Value = serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("{} must be valid JSON: {e}", path.display()));
        // The status hello keeps its venue `connection_id`; the status decoder reads it.
        let allowed: &[&str] = if path
            .file_name()
            .is_some_and(|name| name == "public_status_hello.json")
        {
            &["connection_id"]
        } else {
            &[]
        };
        assert_public_and_redacted(&frame, allowed);
        checked += 1;
    }
    assert!(
        checked > 0,
        "no Unified WS fixtures found in {}",
        dir.display()
    );
}
