# Kraken Unified SDK

An async-native, unified SDK for Kraken's APIs. One interface across products —
the caller writes domain-grouped methods (`market` / `account` / `trade` /
`subscription` / `events`) and never touches transport-specific code; wiring is
locked at `.build()`.

This is a **polyglot mono-repo**: one SDK design, implemented per language under
its own top-level directory.

## Languages

| Language | Path | Status |
|----------|------|--------|
| Rust + tokio | [`rust/`](rust/) | Spot trading + portfolio. |
| C++ · Python · Go · TypeScript/JS | _(coming)_ | Coming soon. |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for general contribution guidelines.
Build, test and code-style rules are per binding and live in that language's own
guide.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Disclaimer

See [DISCLAIMER.md](DISCLAIMER.md).

## Trademark

See [TRADEMARK.md](TRADEMARK.md).
