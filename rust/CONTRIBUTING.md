# Contributing to the Kraken Rust SDK

This file is the day-to-day checklist for the Rust implementation under
[`rust/`](.): what to run before you push, what style a change must follow, and
what to update alongside it.

It has two companions, so nothing is documented twice:

- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — issue reporting, PR etiquette, and
  what is likely to be accepted. Applies to every language binding.
- [`docs/development.md`](docs/development.md) — the contributor's map:
  repository layout, module map, the dual-loop runtime model, where to add a
  method / channel / event / knob, and the conventions a change must respect.

This is an early-stage (pre-1.0) project; the API surface is still settling, so
please open an issue to discuss substantial changes before sending a large PR.

## Prerequisites

Rust **1.85** or later (the `rust-version` in [`Cargo.toml`](Cargo.toml)).
Developers pin a newer toolchain via [`rust-toolchain.toml`](rust-toolchain.toml)
for consistent `rustfmt` / `clippy`.

**Run every command below from the `rust/` directory.** The `ci-*` aliases are
defined in [`.cargo/config.toml`](.cargo/config.toml) and only resolve when that
is your working directory — which is why each CI job does `cd rust` first.

## Building & testing

```sh
cargo build --all-targets   # build the library, examples, and tests
cargo ci-check              # type-check the library, examples, and tests (the CI gate)
cargo ci-clippy             # lints (the CI gate; covers examples and tests too)
cargo ci-fmt                # format check (the CI gate; run `cargo fmt --all` to fix)
cargo ci-test               # full suite (the CI gate; `--lib` is the fast subset)
```

Each `ci-*` command is an alias defined in
[`.cargo/config.toml`](.cargo/config.toml) that runs exactly what the
corresponding CI job runs, so you can reproduce a gate locally with a single
command. Prefer them over the raw `cargo` invocations they wrap — if a gate
changes, the alias changes with it. All of them must pass before you push.

> **The live end-to-end tests spend real money.** They hit the real Kraken
> exchange and are `#[ignore]`d by default; the order-placing tier requires an
> explicit opt-in on top of credentials — do not run it casually. For the exact
> credential and opt-in gating, see
> [Development → Build, test, lint](docs/development.md#build-test-lint) and the
> header of [`tests/e2e_user_flows.rs`](tests/e2e_user_flows.rs).

Where a new test belongs — inline, a sibling file, or [`tests/`](tests) — plus
the `test-support` feature and the two gotchas that cause flakes and hangs, is
covered in [Development → Testing](docs/development.md#testing).

## Code style

- **Formatting is enforced.** `cargo ci-fmt` is a CI gate; `cargo fmt --all`
  fixes it. Settings live in [`rustfmt.toml`](rustfmt.toml).
- **Lints are enforced.** `cargo ci-clippy` runs with `-Dwarnings` and covers
  examples and tests, not just the library.
- **Keep the diff clean.** No unrelated reformatting or refactoring, and follow
  the patterns already in the file you are editing.

The semantic conventions a change must respect — money as
`rust_decimal::Decimal` rather than `f64`, no `.unwrap()` on wire data, modern
`BASE/QUOTE` symbols, credential redaction, and the derived-and-pinned wire
string forms — are documented once in
[Development → Conventions](docs/development.md#conventions). Read them before
your first PR; they are the rules most often missed.

## What to update with your change

- **Tests** — add or update them for behaviour changes, and keep the existing
  tests green.
- **[`CHANGELOG.md`](CHANGELOG.md)** — Keep-a-Changelog; add an entry for any
  public-behaviour change.
- **`docs/`** — update the affected guide when you change public behaviour or
  the public API.
- **[`THIRD_PARTY_NOTICES.html`](THIRD_PARTY_NOTICES.html)** — regenerate it
  whenever dependencies change, with
  [`cargo-about`](https://github.com/EmbarkStudios/cargo-about):

  ```sh
  cargo about generate about.hbs -o THIRD_PARTY_NOTICES.html
  ```

  Attribution covers only what ships — build-time and dev dependencies are
  excluded — and where a crate offers a choice of licences the Apache-2.0 option
  is elected. Both are set in [`about.toml`](about.toml); generation fails if a
  non-permissive licence enters the tree.

Which files a change touches, and whether it counts as breaking under the
`#[non_exhaustive]` and SemVer rules, is set out in
[Development → Where to add things](docs/development.md#where-to-add-things).

If your change alters the **error shape** or the **event envelope shape**, it is
a cross-binding change affecting every language, not a local one — see
[Development → Cross-binding contracts](docs/development.md#cross-binding-contracts)
and open an issue first.

## Related

- [`docs/development.md`](docs/development.md) — repository layout, module map,
  runtime architecture, where to add things, conventions
- [`../CONTRIBUTING.md`](../CONTRIBUTING.md) — issues, PR policy, what gets
  accepted
- [`CLAUDE.md`](CLAUDE.md) — orientation for AI coding agents generating
  *application* code against the SDK
