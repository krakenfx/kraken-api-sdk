//! Shared env-loading helpers for the runnable examples.

/// Reads `name` from repo `.env` (`../.env` then `~/projects/kraken-sdk/.env`).
/// Trims one quote layer, tolerates `export `. Missing `.env` prints lookup paths
/// for credentials-REQUIRED examples.
#[allow(dead_code)] // each example compiles this module; not all pull both forms
pub fn load_env_var(name: &str) -> Option<String> {
    lookup(name, false)
}

/// [`load_env_var`] without the missing-file diagnostic — for credentials-OPTIONAL examples.
#[allow(dead_code)] // pulled in only by the creds-optional examples
pub fn dotenv_lookup(name: &str) -> Option<String> {
    lookup(name, true)
}

fn lookup(name: &str, quiet: bool) -> Option<String> {
    let candidates = [
        concat!(env!("CARGO_MANIFEST_DIR"), "/../.env").to_string(),
        format!(
            "{}/projects/kraken-sdk/.env",
            std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
        ),
    ];
    let mut readable = false;
    for path in &candidates {
        let Ok(contents) = std::fs::read_to_string(path) else {
            continue;
        };
        readable = true;
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let line = line.strip_prefix("export ").unwrap_or(line);
            if let Some((k, v)) = line.split_once('=') {
                if k.trim() == name {
                    return Some(v.trim().trim_matches('"').trim_matches('\'').to_string());
                }
            }
        }
    }
    if !readable && !quiet {
        eprintln!(
            "error: no readable .env at {} or {}",
            candidates[0], candidates[1]
        );
    }
    None
}

/// Real env var, or `default` when unset/empty. Used for runtime knobs.
#[allow(dead_code)] // only the two lifecycle examples pull in the knob form
pub fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.to_string())
}
