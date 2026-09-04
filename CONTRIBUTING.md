# Contributing to kraken-api-sdk

Thank you for your interest in contributing to Kraken's API SDK.

This is a **polyglot mono-repo**: one SDK design, implemented per language under
its own top-level directory. This file covers the policy shared by every
language binding — how to file an issue, how to open a pull request, and what is
likely to be accepted. The build, test, and code-style rules for a given
implementation live in that language's own `CONTRIBUTING.md`.

## How to Contribute

1. Fork the repository.
2. Create a branch from `main`.
3. Make your changes, following the guide for the language you are working on.
4. Run that language's checks locally.
5. Open a pull request against `main`.

The maintainers review submissions and merge accepted changes into future
releases. Response times vary. Not every PR will be accepted, and some may
require changes before they can be merged.

## Issues and Feature Requests

Use [GitHub Issues](https://github.com/krakenfx/kraken-api-sdk/issues) for bug
reports and feature requests.

**Bug reports.** Include:

- A minimal reproducer — the smallest program that shows the problem. Redact any
  API keys or secrets.
- Which binding and which version (e.g. for Rust, the `kraken-sdk` version from your
  `Cargo.toml`).
- What you expected to happen, and what happened instead.
- The full error, including its `code` and `category` if one was returned.
- Your OS and architecture (`uname -a`).

**Feature requests.** Describe what you want the SDK to do and why. Include a
usage example if possible. Prefix the title with `Feature request:` so it is easy
to find.

## PR Guidelines

- Keep PRs focused. One logical change per PR.
- Write clear commit messages. Describe what changed and why.
- Add or update tests for new behavior.
- Do not include unrelated formatting or refactoring changes.

### What Makes a Good PR

- Bug fixes with a reproducer or test case.
- New endpoint or channel coverage that extends an existing namespace.
- Documentation improvements.
- Test coverage for untested code paths.

### What Will Likely Be Declined

- Large architectural changes without prior discussion. Open an issue first.
- Changes that break backward compatibility without strong justification.
- Features that require exchange infrastructure changes outside the SDK.
- Changes to a **cross-binding contract** made in one language alone. Two
  contracts are identical across every binding and must not drift
  language-by-language:
  - the **error shape** — every typed error exposes `code`, `category`,
    `retryable`, `request_id`, and `message`;
  - the **event envelope shape** — every event carries `event_type`,
    `event_version`, `timestamp_monotonic`, an optional correlation
    `request_id`, and a typed `payload`.

  Changing either is a cross-binding change, not a local one. Open an issue
  first.
